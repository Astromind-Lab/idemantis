import {
  type AdapterInvocationContext,
  type AdapterOperation,
  defineAdapterOperation,
  type IssueResultEnvelope,
  type ReconciliationResultEnvelope,
} from "@idemantis/adapter-kit"
import {
  type AdapterInstanceId,
  type ConsequenceLevel,
  type EffectClass,
  type JsonObject,
  type JsonValue,
  type OperationCapabilities,
  sha256Digest,
} from "@idemantis/contracts"
import {
  defineOperation,
  type OperationDefinition,
  sequentialMission,
  type ValueExpression,
} from "@idemantis/sdk"
import type { Static, TSchema } from "typebox"
import Schema from "typebox/schema"
import type { ToolLedger } from "./ledger.js"

export class ToolRejected extends Error {
  override readonly name = "ToolRejected"
}

export class ToolUnavailable extends Error {
  override readonly name = "ToolUnavailable"
}

export interface ToolContext {
  run_id: string
  effect_id: string
  attempt_id: string
  idempotency_key: string
  signal: AbortSignal
}

export type ToolLookup<Output> = { found: true; output: Output } | { found: false } | undefined

export interface ToolDefinition<InputSchema extends TSchema, OutputSchema extends TSchema> {
  name: string
  input: InputSchema
  output: OutputSchema
  effect_class?: EffectClass
  consequence?: ConsequenceLevel
  timeout_ms?: number
  run(input: Static<InputSchema>, context: ToolContext): Promise<Static<OutputSchema>>
  lookup?(
    input: Static<InputSchema>,
    context: ToolContext,
  ): Promise<ToolLookup<Static<OutputSchema>>>
}

export type AnyTool = ToolDefinition<TSchema, TSchema>

export interface ToolAdapterOptions {
  adapter: string
  version: string
  tools: readonly AnyTool[]
  ledger: ToolLedger
  instance_id?: AdapterInstanceId
}

export interface ToolAdapter {
  adapter: string
  adapter_version: string
  adapter_package_digest: `sha256:${string}`
  adapter_instance_id: AdapterInstanceId
  operations: AdapterOperation<unknown, unknown, unknown>[]
}

export interface ToolStep {
  name: string
  tool: AnyTool
  input: ValueExpression
}

export interface ToolMissionOptions {
  id: string
  version: string
  adapter: string
  input: TSchema
  steps: readonly ToolStep[]
  attempts?: number
  backoff_ms?: number
}

const toolCapabilities = {
  duplicate_safety: "keyed_uniqueness",
  reconciliation: "authoritative",
  fencing_mode: "none",
  fencing_scope: "operation",
  handoff_mode: "settle_first",
  execution_location: "either",
  cancellation: false,
  compensation: false,
  observation_stream: false,
} satisfies OperationCapabilities

export function defineTool<InputSchema extends TSchema, OutputSchema extends TSchema>(
  definition: ToolDefinition<InputSchema, OutputSchema>,
): ToolDefinition<InputSchema, OutputSchema> {
  return definition
}

function message(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(0, 1_024)
}

function toolContext(context: AdapterInvocationContext): ToolContext {
  return {
    run_id: context.run_id,
    effect_id: context.effect_id,
    attempt_id: context.attempt_id,
    idempotency_key: context.effect_id,
    signal: context.signal,
  }
}

export function toolOperation(adapter: string, tool: AnyTool): OperationDefinition {
  return defineOperation({
    key: tool.name,
    adapter,
    operation: tool.name,
    schemaVersion: 1,
    inputSchema: tool.input,
    outputSchema: tool.output,
    effectClass: tool.effect_class ?? "record_mutation",
    consequence: tool.consequence ?? "reversible",
    capabilities: toolCapabilities,
  })
}

function adapterOperation(
  adapter: string,
  tool: AnyTool,
  ledger: ToolLedger,
  inflight: Set<string>,
): AdapterOperation<unknown, unknown, unknown> {
  const outputValid = Schema.Compile(tool.output)
  const timeout = tool.timeout_ms ?? 30_000

  const resolveUnknown = async (
    effectId: string,
    inputJson: string,
    context: AdapterInvocationContext,
  ): Promise<ReconciliationResultEnvelope<JsonValue>> => {
    if (!tool.lookup) {
      return { result: "unknown", evidence: { reason: "tool call was interrupted" } }
    }
    const found = await tool.lookup(JSON.parse(inputJson) as never, toolContext(context))
    if (found?.found === true) {
      if (!outputValid.Check(found.output)) {
        return { result: "unknown", evidence: { reason: "lookup output did not match schema" } }
      }
      ledger.forget(effectId)
      ledger.begin(effectId, tool.name, JSON.parse(inputJson) as JsonValue)
      ledger.complete(effectId, found.output as JsonValue)
      return { result: "confirmed_completed", output: found.output as JsonValue }
    }
    if (found?.found === false) {
      ledger.forget(effectId)
      return { result: "confirmed_not_applied", evidence: { reason: "lookup found no effect" } }
    }
    return { result: "unknown", evidence: { reason: "lookup could not decide" } }
  }

  return defineAdapterOperation({
    key: tool.name,
    adapter,
    operation: tool.name,
    schemaVersion: 1,
    inputSchema: tool.input,
    outputSchema: tool.output,
    subjectSchema: { type: "string" } as unknown as TSchema,
    effectClass: tool.effect_class ?? "record_mutation",
    consequence: tool.consequence ?? "reversible",
    capabilities: toolCapabilities,
    timeout: { minimumMs: 1, defaultMs: timeout, maximumMs: Math.max(timeout, 300_000) },
    reconciliationLookupFields: ["effect_id"],
    issue: async (context, input): Promise<IssueResultEnvelope<JsonValue>> => {
      const prior = ledger.begin(context.effect_id, tool.name, input as JsonValue)
      if (prior?.status === "completed") {
        return {
          result: "completed",
          output: prior.output ?? null,
          evidence: { deduplicated: true },
        }
      }
      if (prior?.status === "rejected") {
        return { result: "rejected", evidence: { message: prior.message ?? "rejected" } }
      }
      if (prior) {
        return { result: "uncertain", evidence: { reason: `prior call is ${prior.status}` } }
      }
      inflight.add(context.effect_id)
      try {
        const output = await tool.run(input as never, toolContext(context))
        if (!outputValid.Check(output)) {
          ledger.error(context.effect_id, "tool output did not match its schema")
          return { result: "uncertain", evidence: { reason: "output did not match schema" } }
        }
        ledger.complete(context.effect_id, output as JsonValue)
        return { result: "completed", output: output as JsonValue }
      } catch (error) {
        if (error instanceof ToolRejected) {
          ledger.reject(context.effect_id, message(error))
          return { result: "rejected", evidence: { message: message(error) } }
        }
        if (error instanceof ToolUnavailable) {
          ledger.forget(context.effect_id)
          return { result: "failed", evidence: { message: message(error) } }
        }
        ledger.error(context.effect_id, message(error))
        return { result: "uncertain", evidence: { message: message(error) } }
      } finally {
        inflight.delete(context.effect_id)
      }
    },
    reconcile: async (context): Promise<ReconciliationResultEnvelope<JsonValue>> => {
      const entry = ledger.get(context.effect_id)
      if (!entry || entry.status === "rejected") {
        return { result: "confirmed_not_applied" }
      }
      if (entry.status === "completed") {
        return { result: "confirmed_completed", output: entry.output ?? null }
      }
      if (inflight.has(context.effect_id)) {
        return { result: "confirmed_in_progress" }
      }
      return resolveUnknown(context.effect_id, entry.input_json, context)
    },
  }) as unknown as AdapterOperation<unknown, unknown, unknown>
}

export function createToolAdapter(options: ToolAdapterOptions): ToolAdapter {
  const inflight = new Set<string>()
  const operations = options.tools.map((tool) =>
    adapterOperation(options.adapter, tool, options.ledger, inflight),
  )
  const digest = sha256Digest({
    adapter: options.adapter,
    version: options.version,
    operations: operations.map((operation) => operation.descriptor) as unknown as JsonValue[],
  } as JsonObject)
  return {
    adapter: options.adapter,
    adapter_version: options.version,
    adapter_package_digest: digest as `sha256:${string}`,
    adapter_instance_id: options.instance_id ?? "adp_00000000000000000000000000",
    operations,
  }
}

export function toolMission(options: ToolMissionOptions) {
  return sequentialMission({
    id: options.id,
    version: options.version,
    input: options.input,
    ...(options.attempts === undefined ? {} : { attempts: options.attempts }),
    ...(options.backoff_ms === undefined ? {} : { backoff_ms: options.backoff_ms }),
    steps: options.steps.map((step) => ({
      name: step.name,
      operation: toolOperation(options.adapter, step.tool),
      input: step.input,
    })),
  })
}
