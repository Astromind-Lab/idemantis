import {
  type AdapterOperation,
  defineAdapterOperation,
  type IssueResultEnvelope,
  type ReconciliationResultEnvelope,
} from "@idemantis/adapter-kit"
import type { JsonObject, OperationCapabilities, RunId } from "@idemantis/contracts"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import {
  defineMission,
  defineOperation,
  effectState,
  finalState,
  fromInput,
  fromOutput,
  mapFields,
  noRetry,
  reconcileState,
} from "@idemantis/sdk"
import Type, { type TSchema } from "typebox"
import { afterEach, describe, expect, it, vi } from "vitest"
import { AdapterHost, type HostedAdapter } from "../src/adapters.js"
import { HostedMissionRunner } from "../src/hosted.js"
import { RunnerStore } from "../src/store.js"

const capabilities = {
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

const topicSchema = Type.Object({ topic: Type.String() }, { additionalProperties: false })
const draftSchema = Type.Object({ draft: Type.String() }, { additionalProperties: false })
const publishSchema = Type.Object(
  { body: Type.String(), topic: Type.String() },
  { additionalProperties: false },
)
const receiptSchema = Type.Object({ url: Type.String() }, { additionalProperties: false })

type Envelope<Output> = Promise<IssueResultEnvelope<Output>>
type Lookup<Output> = Promise<ReconciliationResultEnvelope<Output>>

function operation<Input, Output>(
  key: string,
  inputSchema: TSchema,
  outputSchema: TSchema,
  issue: (input: Input) => Envelope<Output>,
  reconcile: () => Lookup<Output>,
) {
  return defineAdapterOperation({
    key,
    adapter: "writer",
    operation: key,
    schemaVersion: 1,
    inputSchema,
    outputSchema,
    subjectSchema: Type.String(),
    reconciliationLookupFields: ["topic"],
    effectClass: "record_mutation",
    consequence: "reversible",
    capabilities,
    timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
    issue: (_context, input) => issue(input as Input),
    reconcile: () => reconcile(),
  }) as unknown as AdapterOperation<unknown, unknown, unknown>
}

function contract(adapterOperation: AdapterOperation<unknown, unknown, unknown>) {
  const descriptor = adapterOperation.descriptor
  return defineOperation({
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
}

const journals: SqliteJournal[] = []

afterEach(() => {
  for (const journal of journals.splice(0)) {
    journal.close()
  }
})

function setup(
  draftIssue: (input: { topic: string }) => Envelope<{ draft: string }>,
  draftLookup: () => Lookup<{ draft: string }>,
) {
  const publish = vi.fn(async (input: { body: string; topic: string }) => ({
    result: "completed" as const,
    output: { url: `https://posts.test/${input.topic}` },
  }))
  const draftOperation = operation(
    "writer.draft",
    topicSchema,
    draftSchema,
    draftIssue,
    draftLookup,
  )
  const publishOperation = operation(
    "writer.publish",
    publishSchema,
    receiptSchema,
    publish,
    vi.fn(),
  )
  const adapter: HostedAdapter = {
    adapter: "writer",
    adapter_version: "1.0.0",
    adapter_package_digest: `sha256:${"a".repeat(64)}`,
    adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    operations: [draftOperation, publishOperation],
  }
  const draft = contract(draftOperation)
  const mission = defineMission({
    id: "writer.post",
    version: "1.0.0",
    input: topicSchema,
    initial: "draft",
    states: {
      draft: effectState({
        operation: draft,
        effectSlot: "draft",
        input: mapFields({ topic: fromInput("topic") }),
        retry: noRetry(),
        onSettled: "publish",
        onFailed: "failed",
        onUncertain: "reconcile_draft",
      }),
      reconcile_draft: reconcileState({
        effectState: "draft",
        operation: draft,
        subject: fromInput("topic"),
        onConfirmedCompleted: "publish",
        onConfirmedNotApplied: "failed",
        onConfirmedInProgress: "failed",
        onConflictingState: "failed",
        onUnknown: "failed",
      }),
      publish: effectState({
        operation: contract(publishOperation),
        effectSlot: "publish",
        input: mapFields({ body: fromOutput("draft", "draft"), topic: fromInput("topic") }),
        retry: noRetry(),
        onSettled: "complete",
        onFailed: "failed",
        onUncertain: "failed",
      }),
      complete: finalState("completed"),
      failed: finalState("failed"),
    },
  })
  const journal = new SqliteJournal({ path: ":memory:" })
  journals.push(journal)
  const host = new AdapterHost({ source_id: "hosted.runner", adapters: [adapter] })
  const runner = new HostedMissionRunner({ journal, host })
  const store = new RunnerStore({ journal })
  const runtime = host.bind({
    manifest: mission.manifest,
    definition_digest: mission.definitionDigest,
  })
  store.registerDefinition(runtime)
  const runId = store.admitMission({
    definition_digest: runtime.definition_digest,
    input: { topic: "charging" } as JsonObject,
  }).summary.run_id as RunId
  return { journal, runner, runId, publish, store }
}

describe("step outputs", () => {
  it("feeds a completed step's output into the next step and keeps replay exact", async () => {
    const { journal, runner, runId, publish, store } = setup(
      async (input) => ({ result: "completed", output: { draft: `Notes on ${input.topic}` } }),
      vi.fn(),
    )

    await runner.run(runId)
    await runner.close()

    expect(publish).toHaveBeenCalledWith({ body: "Notes on charging", topic: "charging" })
    expect(journal.getMission(runId).state).toMatchObject({
      terminal_outcome: "completed",
      outputs: { draft: { draft: "Notes on charging" } },
    })
    expect(journal.verifyRun(runId).state_digest).toBe(journal.getMission(runId).state_digest)
    expect(store.missionContext(runId).effects.map((effect) => effect.output)).toEqual([
      { draft: "Notes on charging" },
      { url: "https://posts.test/charging" },
    ])
  })

  it("recovers a lost output through reconciliation before the next step uses it", async () => {
    const { journal, runner, runId, publish } = setup(
      async () => ({ result: "uncertain" }),
      async () => ({ result: "confirmed_completed", output: { draft: "Recovered draft" } }),
    )

    await runner.run(runId)
    await runner.close()

    expect(publish).toHaveBeenCalledWith({ body: "Recovered draft", topic: "charging" })
    expect(journal.getMission(runId).state.terminal_outcome).toBe("completed")
    expect(journal.verifyRun(runId).state_digest).toBe(journal.getMission(runId).state_digest)
  })
})
