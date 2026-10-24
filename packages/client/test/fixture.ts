import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { type AdapterOperation, defineAdapterOperation } from "@idemantis/adapter-kit"
import type { OperationCapabilities } from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import {
  AdapterHost,
  ApiKeyStore,
  type HostedAdapter,
  type StartedLocalRunner,
  startLocalRunner,
} from "@idemantis/runner"
import {
  defineMission,
  defineOperation,
  effectState,
  finalState,
  fromInput,
  mapFields,
  noRetry,
  waitForObservation,
  waitForOperator,
} from "@idemantis/sdk"
import Type, { type TSchema } from "typebox"

const capabilities = {
  duplicate_safety: "native_idempotency",
  reconciliation: "none",
  fencing_mode: "none",
  fencing_scope: "operation",
  handoff_mode: "settle_first",
  execution_location: "either",
  cancellation: false,
  compensation: false,
  observation_stream: false,
} satisfies OperationCapabilities

const ticketSchema = Type.Object(
  { ticket: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)
const draftSchema = Type.Object({ draft: Type.String() }, { additionalProperties: false })

export interface RunnerFixture {
  runner: StartedLocalRunner
  token: string
  digests: { review: string; approval: string }
  calls: string[]
  runtimes: MissionRuntimeDefinition[]
  close(): Promise<void>
}

export async function runnerFixture(): Promise<RunnerFixture> {
  const directory = mkdtempSync(join(tmpdir(), "idemantis-client-"))
  const calls: string[] = []
  const operation = defineAdapterOperation({
    key: "writer.draft",
    adapter: "writer",
    operation: "writer.draft",
    schemaVersion: 1,
    inputSchema: ticketSchema,
    outputSchema: draftSchema,
    effectClass: "record_mutation",
    consequence: "reversible",
    capabilities,
    timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
    issue: async (context, input) => {
      calls.push(context.effect_id)
      return { result: "completed", output: { draft: `Draft for ${input.ticket}` } }
    },
  }) as unknown as AdapterOperation<unknown, unknown, unknown>
  const adapter: HostedAdapter = {
    adapter: "writer",
    adapter_version: "1.0.0",
    adapter_package_digest: `sha256:${"a".repeat(64)}`,
    adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    operations: [operation],
  }
  const descriptor = operation.descriptor
  const draft = defineOperation({
    key: descriptor.key,
    adapter: descriptor.adapter,
    operation: descriptor.operation,
    schemaVersion: descriptor.operation_schema_version,
    inputSchema: descriptor.input_schema as TSchema,
    outputSchema: descriptor.output_schema as TSchema,
    effectClass: descriptor.effect_class,
    consequence: descriptor.consequence,
    capabilities: descriptor.capabilities,
  })
  const draftState = effectState({
    operation: draft,
    effectSlot: "draft",
    input: mapFields({ ticket: fromInput("ticket") }),
    retry: noRetry(),
    onSettled: "review",
    onFailed: "failed",
    onUncertain: "failed",
  })
  const review = defineMission({
    id: "support.reply",
    version: "1.0.0",
    input: ticketSchema,
    initial: "draft",
    states: {
      draft: draftState,
      review: waitForObservation({
        observationType: "review.approved",
        schemaVersion: 1,
        correlation: { kind: "effect", effectState: "draft" },
        timeout: { afterMs: 60_000, onTimeout: "failed" },
        onObserved: "complete",
      }),
      complete: finalState("completed"),
      failed: finalState("failed"),
    },
  })
  const approval = defineMission({
    id: "support.escalation",
    version: "1.0.0",
    input: ticketSchema,
    initial: "draft",
    states: {
      draft: draftState,
      review: waitForOperator({
        reason: "A person must approve the escalation",
        actions: [
          {
            code: "approve",
            consequence: "reversible",
            confirmationRequired: false,
            input: Type.Object(
              { note: Type.String({ maxLength: 512 }) },
              { additionalProperties: false, maxProperties: 1 },
            ),
            expectedOutcomeLabel: "Escalation is approved",
            onAccepted: "complete",
          },
        ],
      }),
      complete: finalState("completed"),
      failed: finalState("failed"),
    },
  })
  const runner = await startLocalRunner({
    port: 0,
    state_directory: directory,
    reference_transition_delay_ms: 0,
    adapters: [adapter],
  })
  const host = new AdapterHost({ source_id: "hosted.runner", adapters: [adapter] })
  const keys = new ApiKeyStore({ path: runner.access_path })
  const token = keys.create("test-agent", ["view", "operate", "define"]).token
  keys.close()
  const runtimes = [review, approval].map((mission) =>
    host.bind({ manifest: mission.manifest, definition_digest: mission.definitionDigest }),
  )
  return {
    runner,
    token,
    calls,
    digests: {
      review: runtimes[0]?.definition_digest ?? "",
      approval: runtimes[1]?.definition_digest ?? "",
    },
    runtimes,
    close: async () => {
      await runner.close()
      rmSync(directory, { recursive: true, force: true })
    },
  }
}
