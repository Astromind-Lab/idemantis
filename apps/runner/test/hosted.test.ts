import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
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
  mapFields,
  reconcileState,
  safeRetry,
} from "@idemantis/sdk"
import Type, { type TSchema } from "typebox"
import { afterEach, describe, expect, it, vi } from "vitest"
import { AdapterHost } from "../src/adapters.js"
import { HostedMissionRunner } from "../src/hosted.js"
import { RunnerStore } from "../src/store.js"

const capabilities = {
  duplicate_safety: "native_idempotency",
  reconciliation: "authoritative",
  fencing_mode: "none",
  fencing_scope: "operation",
  handoff_mode: "settle_first",
  execution_location: "either",
  cancellation: false,
  compensation: false,
  observation_stream: false,
} satisfies OperationCapabilities

const messageSchema = Type.Object(
  { message_id: Type.String({ minLength: 1 }), body: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)
const receiptSchema = Type.Object({ delivered: Type.Boolean() }, { additionalProperties: false })
const subjectSchema = Type.Object(
  { message_id: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)

type Issue = () => Promise<IssueResultEnvelope<{ delivered: boolean }>>
type Reconcile = () => Promise<ReconciliationResultEnvelope<{ delivered: boolean }>>

function sendOperation(issue: Issue, reconcile: Reconcile, adapter = "mailer") {
  return defineAdapterOperation({
    key: `${adapter}.send`,
    adapter,
    operation: `${adapter}.send`,
    schemaVersion: 1,
    inputSchema: messageSchema,
    outputSchema: receiptSchema,
    subjectSchema,
    effectClass: "notification",
    consequence: "irreversible",
    capabilities,
    timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
    reconciliationLookupFields: ["message_id"],
    issue: () => issue(),
    reconcile: () => reconcile(),
  }) as unknown as AdapterOperation<unknown, unknown, unknown>
}

function missionFor(operation: AdapterOperation<unknown, unknown, unknown>) {
  const descriptor = operation.descriptor
  const send = defineOperation({
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
  return defineMission({
    id: "operations.notify",
    version: "1.0.0",
    input: messageSchema,
    initial: "send",
    states: {
      send: effectState({
        operation: send,
        effectSlot: "notice",
        input: mapFields({ message_id: fromInput("message_id"), body: fromInput("body") }),
        retry: safeRetry({ attempts: 2, backoffMs: 40 }),
        onSettled: "complete",
        onFailed: "failed",
        onUncertain: "reconcile",
      }),
      reconcile: reconcileState({
        effectState: "send",
        operation: send,
        subject: mapFields({ message_id: fromInput("message_id") }),
        onConfirmedCompleted: "complete",
        onConfirmedNotApplied: "send",
        onConfirmedInProgress: "reconcile",
        onConflictingState: "unresolved",
        onUnknown: "unresolved",
      }),
      complete: finalState("completed"),
      failed: finalState("failed"),
      unresolved: finalState("unresolved"),
    },
  })
}

const journals: SqliteJournal[] = []
const runners: HostedMissionRunner[] = []
const directories: string[] = []

function hosted(
  journal: SqliteJournal,
  operation: AdapterOperation<unknown, unknown, unknown>,
  leaseDurationMs = 30_000,
) {
  const host = new AdapterHost({
    source_id: "hosted.runner",
    adapters: [
      {
        adapter: "mailer",
        adapter_version: "1.0.0",
        adapter_package_digest: `sha256:${"a".repeat(64)}`,
        adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        operations: [operation],
      },
    ],
  })
  const runner = new HostedMissionRunner({ journal, host, lease_duration_ms: leaseDurationMs })
  runners.push(runner)
  const mission = missionFor(operation)
  const runtime = host.bind({
    manifest: mission.manifest,
    definition_digest: mission.definitionDigest,
  })
  const store = new RunnerStore({ journal })
  store.registerDefinition(runtime)
  return { host, runner, runtime, store }
}

function admit(store: RunnerStore, digest: string): RunId {
  return store.admitMission({
    definition_digest: digest,
    input: { message_id: "msg-1", body: "Dock 3 is ready" } as JsonObject,
  }).summary.run_id
}

function journal(path = ":memory:"): SqliteJournal {
  const opened = new SqliteJournal({ path })
  journals.push(opened)
  return opened
}

afterEach(async () => {
  for (const runner of runners.splice(0)) {
    await Promise.race([runner.close(), new Promise((resolve) => setTimeout(resolve, 50))])
  }
  for (const opened of journals.splice(0)) {
    try {
      opened.close()
    } catch {}
  }
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true })
  }
})

describe("hosted mission runner", () => {
  it("executes a registered mission through hosted adapters", async () => {
    const issue = vi.fn(async () => ({ result: "completed" as const, output: { delivered: true } }))
    const selected = journal()
    const { runner, runtime, store } = hosted(selected, sendOperation(issue, vi.fn()))
    const runId = admit(store, runtime.definition_digest)

    await runner.run(runId)

    expect(selected.getMission(runId).state.terminal_outcome).toBe("completed")
    expect(store.effects(runId).items).toMatchObject([
      { operation: "mailer.send", status: "settled", issue_count: 1 },
    ])
    expect(issue).toHaveBeenCalledOnce()
    expect(runner.executionStatus(runId).activity).toBe("terminal")
  })

  it("accepts only missions bound to its hosted operations", () => {
    const operation = sendOperation(vi.fn(), vi.fn())
    const { host, runner, runtime } = hosted(journal(), operation)
    const other = missionFor(sendOperation(vi.fn(), vi.fn(), "pager"))

    expect(host.operations()).toEqual(["mailer.send"])
    expect(runner.accepts(runtime)).toBe(true)
    expect(
      runner.accepts({ ...runtime, execution_binding_digest: `sha256:${"f".repeat(64)}` }),
    ).toBe(false)
    expect(
      runner.accepts({
        manifest: other.manifest,
        definition_digest: other.definitionDigest,
        execution_binding_digest: runtime.execution_binding_digest,
      }),
    ).toBe(false)
  })

  it("retries a failed effect after its backoff without another start", async () => {
    const issue = vi
      .fn<Issue>()
      .mockResolvedValueOnce({ result: "failed", evidence: { reason: "smtp_busy" } })
      .mockResolvedValueOnce({ result: "completed", output: { delivered: true } })
    const selected = journal()
    const { runner, runtime, store } = hosted(selected, sendOperation(issue, vi.fn()))
    const runId = admit(store, runtime.definition_digest)

    expect(runner.start(runId).disposition).toBe("started")
    await runner.waitForExecution(runId)

    expect(selected.getMission(runId).state.terminal_outcome).toBeNull()
    expect(runner.scheduled(runId)).toBe(true)
    await vi.waitFor(
      () => expect(selected.getMission(runId).state.terminal_outcome).toBe("completed"),
      { timeout: 2_000, interval: 10 },
    )
    expect(issue).toHaveBeenCalledTimes(2)
  })

  it("reconciles a dispatch interrupted by a runner restart instead of resending it", async () => {
    const directory = mkdtempSync(join(tmpdir(), "idemantis-hosted-"))
    directories.push(directory)
    const path = join(directory, "runner.sqlite")
    const crashedIssue = vi.fn<Issue>(() => new Promise(() => {}))
    const first = journal(path)
    const crashed = hosted(first, sendOperation(crashedIssue, vi.fn()), 150)
    const runId = admit(crashed.store, crashed.runtime.definition_digest)

    crashed.runner.start(runId)
    await vi.waitFor(() => expect(first.listAttempts(runId)[0]?.status).toBe("dispatch_started"))
    first.close()

    const restartedIssue = vi.fn<Issue>()
    const reconcile = vi.fn<Reconcile>(async () => ({
      result: "confirmed_completed",
      output: { delivered: true },
    }))
    const second = journal(path)
    const restarted = hosted(second, sendOperation(restartedIssue, reconcile), 150)

    restarted.runner.start(runId)
    await restarted.runner.waitForExecution(runId)
    expect(restarted.runner.scheduled(runId)).toBe(true)
    await vi.waitFor(
      () => expect(second.getMission(runId).state.terminal_outcome).toBe("completed"),
      { timeout: 2_000, interval: 10 },
    )

    expect(crashedIssue).toHaveBeenCalledOnce()
    expect(restartedIssue).not.toHaveBeenCalled()
    expect(reconcile).toHaveBeenCalledOnce()
    expect(restarted.store.effects(runId).items[0]).toMatchObject({ issue_count: 1 })
  })
})
