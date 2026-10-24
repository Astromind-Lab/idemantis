import { mkdtempSync, rmSync } from "node:fs"
import { createServer, type Server } from "node:http"
import type { AddressInfo } from "node:net"
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
  waitForObservation,
} from "@idemantis/sdk"
import { type MissionContext, MissionContextSchema } from "@idemantis/transport"
import Type, { type TSchema } from "typebox"
import Schema from "typebox/schema"
import { afterEach, describe, expect, it, vi } from "vitest"
import { createHttpAdapterHandler } from "@idemantis/adapter-http"
import { AdapterHost, connectRemoteAdapters, type HostedAdapter } from "../src/adapters.js"
import { adapterConnections, leaseDuration } from "../src/cli.js"
import { HostedMissionRunner } from "../src/hosted.js"
import { ApiKeyStore } from "../src/keys.js"
import { type StartedLocalRunner, startLocalRunner } from "../src/local.js"
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

function missionFor(
  operation: AdapterOperation<unknown, unknown, unknown>,
  options: { awaitReceipt?: boolean } = {},
) {
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
        onSettled: options.awaitReceipt ? "await_receipt" : "complete",
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
      ...(options.awaitReceipt
        ? {
            await_receipt: waitForObservation({
              observationType: "mailer.read_receipt",
              schemaVersion: 1,
              correlation: { kind: "effect", effectState: "send" },
              timeout: { afterMs: 60_000, onTimeout: "unresolved" },
              onObserved: "complete",
            }),
          }
        : {}),
      complete: finalState("completed"),
      failed: finalState("failed"),
      unresolved: finalState("unresolved"),
    },
  })
}

const journals: SqliteJournal[] = []
const runners: HostedMissionRunner[] = []
const directories: string[] = []

const localRunners: StartedLocalRunner[] = []
const adapterServers: Server[] = []

function mailer(operation: AdapterOperation<unknown, unknown, unknown>): HostedAdapter {
  return {
    adapter: "mailer",
    adapter_version: "1.0.0",
    adapter_package_digest: `sha256:${"a".repeat(64)}`,
    adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    operations: [operation],
  }
}

function hosted(
  journal: SqliteJournal,
  operation: AdapterOperation<unknown, unknown, unknown>,
  leaseDurationMs = 30_000,
) {
  const host = new AdapterHost({ source_id: "hosted.runner", adapters: [mailer(operation)] })
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
  for (const local of localRunners.splice(0)) {
    await local.close()
  }
  for (const server of adapterServers.splice(0)) {
    server.closeAllConnections()
    await new Promise((resolve) => server.close(resolve))
  }
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

  it("registers, routes, and completes a hosted mission through the local runner", async () => {
    const directory = mkdtempSync(join(tmpdir(), "idemantis-hosted-local-"))
    directories.push(directory)
    const issue = vi.fn(async () => ({ result: "completed" as const, output: { delivered: true } }))
    const operation = sendOperation(issue, vi.fn())
    const local = await startLocalRunner({
      port: 0,
      state_directory: directory,
      reference_transition_delay_ms: 0,
      adapters: [mailer(operation)],
    })
    localRunners.push(local)
    const session = await authenticate(local)
    const mission = missionFor(operation)
    const runtime = new AdapterHost({
      source_id: "hosted.runner",
      adapters: [mailer(operation)],
    }).bind({ manifest: mission.manifest, definition_digest: mission.definitionDigest })
    const post = (path: string, body?: unknown) =>
      fetch(`${local.origin}${path}`, {
        method: "POST",
        headers: body === undefined ? session : { ...session, "content-type": "application/json" },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      })

    const unbound = await post("/api/definitions", {
      runtime: { ...runtime, execution_binding_digest: `sha256:${"e".repeat(64)}` },
    })
    const registered = await post("/api/definitions", { runtime })
    const admitted = await post("/api/missions", {
      definition_digest: runtime.definition_digest,
      input: { message_id: "msg-2", body: "Gate 4 opened" },
    })
    const runId = ((await admitted.json()) as { summary: { run_id: RunId } }).summary.run_id
    const started = await post(`/api/missions/${runId}/start`)

    expect(unbound.status).toBe(422)
    expect(registered.status).toBe(200)
    expect(started.status).toBe(202)
    await vi.waitFor(
      async () => {
        const execution = await fetch(`${local.origin}/api/missions/${runId}/execution`, {
          headers: { cookie: session.cookie },
        })
        expect(((await execution.json()) as { activity: string }).activity).toBe("terminal")
      },
      { timeout: 2_000, interval: 20 },
    )
    expect(issue).toHaveBeenCalledOnce()
  })

  it("executes a mission through an adapter served over HTTP", async () => {
    const directory = mkdtempSync(join(tmpdir(), "idemantis-remote-"))
    directories.push(directory)
    const issue = vi.fn(async () => ({ result: "completed" as const, output: { delivered: true } }))
    const adapter = mailer(sendOperation(issue, vi.fn()))
    const server = createServer(
      createHttpAdapterHandler({
        adapter: adapter.adapter,
        adapter_version: adapter.adapter_version,
        adapter_package_digest: adapter.adapter_package_digest,
        operations: adapter.operations,
        token: "mailer-secret",
      }),
    )
    adapterServers.push(server)
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
    const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
    const connections = adapterConnections(
      [`${adapter.adapter_instance_id}=${url}`],
      "mailer-secret",
    )
    const local = await startLocalRunner({
      port: 0,
      state_directory: directory,
      reference_transition_delay_ms: 0,
      remote_adapters: connections,
    })
    localRunners.push(local)
    const session = await authenticate(local)
    const remote = await connectRemoteAdapters(connections)
    const mission = missionFor(
      remote[0]?.operations[0] as AdapterOperation<unknown, unknown, unknown>,
    )
    const runtime = new AdapterHost({ source_id: "hosted.runner", adapters: remote }).bind({
      manifest: mission.manifest,
      definition_digest: mission.definitionDigest,
    })
    const post = (path: string, body?: unknown) =>
      fetch(`${local.origin}${path}`, {
        method: "POST",
        headers: body === undefined ? session : { ...session, "content-type": "application/json" },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      })

    expect((await post("/api/definitions", { runtime })).status).toBe(200)
    const admitted = await post("/api/missions", {
      definition_digest: runtime.definition_digest,
      input: { message_id: "msg-3", body: "Bay 2 released" },
    })
    const runId = ((await admitted.json()) as { summary: { run_id: RunId } }).summary.run_id
    expect((await post(`/api/missions/${runId}/start`)).status).toBe(202)
    await vi.waitFor(
      async () => {
        const execution = await fetch(`${local.origin}/api/missions/${runId}/execution`, {
          headers: { cookie: session.cookie },
        })
        expect(((await execution.json()) as { activity: string }).activity).toBe("terminal")
      },
      { timeout: 2_000, interval: 20 },
    )
    expect(issue).toHaveBeenCalledOnce()
    expect(runtime.execution_binding_digest).toBe(
      new AdapterHost({ source_id: "hosted.runner", adapters: [adapter] }).bindingDigest({
        manifest: mission.manifest,
        definition_digest: mission.definitionDigest,
      }),
    )
  })

  it("parses remote adapter options and rejects malformed ones", () => {
    expect(adapterConnections(["adp_01ARZ3NDEKTSV4RRFFQ69G5FAV=https://a.test/"], "t")).toEqual([
      {
        adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        url: "https://a.test/",
        token: "t",
      },
    ])
    expect(() => adapterConnections(["https://a.test/"], undefined)).toThrow(TypeError)
    expect(() => adapterConnections(["adp_01ARZ3NDEKTSV4RRFFQ69G5FAV="], undefined)).toThrow(
      TypeError,
    )
  })

  it("advances a waiting mission from an external observation exactly once", async () => {
    const directory = mkdtempSync(join(tmpdir(), "idemantis-observation-"))
    directories.push(directory)
    const issue = vi.fn(async () => ({ result: "completed" as const, output: { delivered: true } }))
    const operation = sendOperation(issue, vi.fn())
    const local = await startLocalRunner({
      port: 0,
      state_directory: directory,
      reference_transition_delay_ms: 0,
      adapters: [mailer(operation)],
    })
    localRunners.push(local)
    const session = await authenticate(local)
    const mission = missionFor(operation, { awaitReceipt: true })
    const runtime = new AdapterHost({
      source_id: "hosted.runner",
      adapters: [mailer(operation)],
    }).bind({ manifest: mission.manifest, definition_digest: mission.definitionDigest })
    const post = (path: string, body?: unknown) =>
      fetch(`${local.origin}${path}`, {
        method: "POST",
        headers: body === undefined ? session : { ...session, "content-type": "application/json" },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      })
    const get = async (path: string) =>
      (await fetch(`${local.origin}${path}`, { headers: { cookie: session.cookie } })).json()

    await post("/api/definitions", { runtime })
    const admitted = await post("/api/missions", {
      definition_digest: runtime.definition_digest,
      input: { message_id: "msg-4", body: "Pickup ready" },
    })
    const runId = ((await admitted.json()) as { summary: { run_id: RunId } }).summary.run_id
    await post(`/api/missions/${runId}/start`)
    await vi.waitFor(
      async () =>
        expect(
          ((await get(`/api/missions/${runId}`)) as { projection: { current_state: string } })
            .projection.current_state,
        ).toBe("await_receipt"),
      { timeout: 2_000, interval: 20 },
    )
    const effectId = (
      (await get(`/api/missions/${runId}/effects`)) as {
        items: { effect_id: string }[]
      }
    ).items[0]?.effect_id
    const observation = {
      observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      observation_type: "mailer.read_receipt",
      effect_id: effectId,
      payload: { reader: "dock-supervisor" },
      observed_at: "2026-10-24T08:00:00.000Z",
    }

    const recorded = await post(`/api/missions/${runId}/observations`, observation)
    const recordedReceipt = (await recorded.json()) as { event_id: string; sequence: number }
    await vi.waitFor(
      async () =>
        expect(
          ((await get(`/api/missions/${runId}/execution`)) as { activity: string }).activity,
        ).toBe("terminal"),
      { timeout: 2_000, interval: 20 },
    )
    const duplicate = await post(`/api/missions/${runId}/observations`, observation)
    const conflicting = await post(`/api/missions/${runId}/observations`, {
      ...observation,
      payload: { reader: "someone-else" },
    })
    const late = await post(`/api/missions/${runId}/observations`, {
      ...observation,
      observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAW",
    })
    const invalid = await post(`/api/missions/${runId}/observations`, {
      ...observation,
      observation_type: "Receipt",
    })

    expect(recorded.status).toBe(201)
    expect(duplicate.status).toBe(200)
    expect(await duplicate.json()).toMatchObject({
      disposition: "duplicate",
      event_id: recordedReceipt.event_id,
      sequence: recordedReceipt.sequence,
    })
    expect(conflicting.status).toBe(409)
    expect(late.status).toBe(409)
    expect(invalid.status).toBe(422)
    expect(
      ((await get(`/api/missions/${runId}`)) as { projection: { terminal_outcome: string } })
        .projection.terminal_outcome,
    ).toBe("completed")
    expect(issue).toHaveBeenCalledOnce()
  })

  it("keeps definitions and resumes unfinished missions after a restart", async () => {
    const directory = mkdtempSync(join(tmpdir(), "idemantis-restart-"))
    directories.push(directory)
    const issue = vi.fn(async () => ({ result: "completed" as const, output: { delivered: true } }))
    const operation = sendOperation(issue, vi.fn())
    const options = {
      port: 0,
      state_directory: directory,
      reference_transition_delay_ms: 0,
      adapters: [mailer(operation)],
    }
    const mission = missionFor(operation)
    const runtime = new AdapterHost({
      source_id: "hosted.runner",
      adapters: [mailer(operation)],
    }).bind({ manifest: mission.manifest, definition_digest: mission.definitionDigest })
    const client = async (local: StartedLocalRunner) => {
      const session = await authenticate(local)
      return {
        post: (path: string, body: unknown) =>
          fetch(`${local.origin}${path}`, {
            method: "POST",
            headers: { ...session, "content-type": "application/json" },
            body: JSON.stringify(body),
          }),
        get: async (path: string) =>
          (await fetch(`${local.origin}${path}`, { headers: { cookie: session.cookie } })).json(),
      }
    }

    const first = await startLocalRunner(options)
    const before = await client(first)
    await before.post("/api/definitions", { runtime })
    const admitted = await before.post("/api/missions", {
      definition_digest: runtime.definition_digest,
      input: { message_id: "msg-6", body: "Queued before restart" },
    })
    const runId = ((await admitted.json()) as { summary: { run_id: RunId } }).summary.run_id
    await first.close()
    expect(issue).not.toHaveBeenCalled()

    const second = await startLocalRunner(options)
    localRunners.push(second)
    const after = await client(second)
    expect(second.resumed).toEqual([runId])
    await vi.waitFor(
      async () =>
        expect(
          ((await after.get(`/api/missions/${runId}`)) as { projection: { status: string } })
            .projection.status,
        ).toBe("completed"),
      { timeout: 2_000, interval: 20 },
    )
    const definitions = (await after.get("/api/definitions")) as {
      items: { definition_digest: string }[]
    }
    const again = await after.post("/api/missions", {
      definition_digest: runtime.definition_digest,
      input: { message_id: "msg-7", body: "Admitted after restart" },
    })
    await second.close()
    localRunners.splice(localRunners.indexOf(second), 1)

    const withoutAdapters = await startLocalRunner({ ...options, adapters: [] })
    localRunners.push(withoutAdapters)
    const unbound = await (await client(withoutAdapters)).post("/api/missions", {
      definition_digest: runtime.definition_digest,
      input: { message_id: "msg-8", body: "No adapter is connected" },
    })

    expect(definitions.items.map((item) => item.definition_digest)).toContain(
      runtime.definition_digest,
    )
    expect(again.status).toBe(201)
    expect(unbound.status).toBe(422)
    expect(withoutAdapters.resumed).toEqual([])
    expect(issue).toHaveBeenCalledOnce()
  })

  it("parses the lease duration option", () => {
    expect(leaseDuration(undefined)).toBeUndefined()
    expect(leaseDuration("5000")).toBe(5_000)
    expect(() => leaseDuration("999")).toThrow(TypeError)
    expect(() => leaseDuration("5s")).toThrow(TypeError)
  })

  it("tells a returning agent what is done, what is pending, and who acts next", async () => {
    const directory = mkdtempSync(join(tmpdir(), "idemantis-context-"))
    directories.push(directory)
    const issue = vi.fn(async () => ({
      result: "completed" as const,
      output: { delivered: true },
      external_reference: "smtp-7",
    }))
    const operation = sendOperation(issue, vi.fn())
    const local = await startLocalRunner({
      port: 0,
      state_directory: directory,
      reference_transition_delay_ms: 0,
      adapters: [mailer(operation)],
    })
    localRunners.push(local)
    const keys = new ApiKeyStore({ path: local.access_path })
    const token = keys.create("resume-agent", ["view", "operate", "define"]).token
    keys.close()
    const call = async (path: string, body?: unknown) => {
      const response = await fetch(`${local.origin}${path}`, {
        method: body === undefined ? "GET" : "POST",
        headers: {
          authorization: `Bearer ${token}`,
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      })
      return { status: response.status, body: (await response.json()) as never }
    }
    const mission = missionFor(operation, { awaitReceipt: true })
    const runtime = new AdapterHost({
      source_id: "hosted.runner",
      adapters: [mailer(operation)],
    }).bind({ manifest: mission.manifest, definition_digest: mission.definitionDigest })

    expect((await call("/api/definitions", { runtime })).status).toBe(200)
    const admitted = await call("/api/missions", {
      definition_digest: runtime.definition_digest,
      input: { message_id: "msg-9", body: "Resume me" },
    })
    const runId = (admitted.body as { summary: { run_id: RunId } }).summary.run_id
    const queued = await call(`/api/missions/${runId}/context`)
    await call(`/api/missions/${runId}/start`, {})
    await vi.waitFor(
      async () =>
        expect(
          ((await call(`/api/missions/${runId}/context`)).body as { state_kind: string })
            .state_kind,
        ).toBe("observation_wait"),
      { timeout: 2_000, interval: 20 },
    )
    const waiting = (await call(`/api/missions/${runId}/context`)).body as unknown as MissionContext
    await call(`/api/missions/${runId}/observations`, {
      observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FB0",
      observation_type: "mailer.read_receipt",
      effect_id: waiting.next.effect_id,
      payload: { reader: "agent" },
    })
    await vi.waitFor(
      async () =>
        expect(
          ((await call(`/api/missions/${runId}/context`)).body as { status: string }).status,
        ).toBe("completed"),
      { timeout: 2_000, interval: 20 },
    )
    const finished = (await call(`/api/missions/${runId}/context`))
      .body as unknown as MissionContext

    expect(Schema.Check(MissionContextSchema, queued.body)).toBe(true)
    expect(queued.body).toMatchObject({ next: { actor: "runner", operation: "mailer.send" } })
    expect(Schema.Check(MissionContextSchema, waiting)).toBe(true)
    expect(waiting.effects).toEqual([
      expect.objectContaining({
        phase: "done",
        output: { delivered: true },
        external_reference: "smtp-7",
      }),
    ])
    expect(waiting.next).toMatchObject({
      actor: "observation",
      observation_type: "mailer.read_receipt",
      effect_id: waiting.effects[0]?.effect_id,
      deadline: expect.any(String),
    })
    expect(Schema.Check(MissionContextSchema, finished)).toBe(true)
    expect(finished).toMatchObject({
      terminal_outcome: "completed",
      next: { actor: "none" },
      execution: { activity: "terminal" },
    })
    expect(issue).toHaveBeenCalledOnce()
  })

  it("compiles, registers, and runs a plan an agent wrote in JSON", async () => {
    const directory = mkdtempSync(join(tmpdir(), "idemantis-plan-"))
    directories.push(directory)
    const issue = vi.fn(async () => ({ result: "completed" as const, output: { delivered: true } }))
    const operation = sendOperation(issue, vi.fn())
    const local = await startLocalRunner({
      port: 0,
      state_directory: directory,
      reference_transition_delay_ms: 0,
      adapters: [mailer(operation)],
    })
    localRunners.push(local)
    const keys = new ApiKeyStore({ path: local.access_path })
    const planner = keys.create("planner", ["view", "operate", "define"]).token
    const worker = keys.create("worker", ["view", "operate"]).token
    keys.close()
    const call = async (token: string, path: string, body?: unknown) => {
      const response = await fetch(`${local.origin}${path}`, {
        method: body === undefined ? "GET" : "POST",
        headers: {
          authorization: `Bearer ${token}`,
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      })
      return { status: response.status, body: (await response.json()) as never }
    }
    const plan = {
      id: "agent.notify",
      version: "1.0.0",
      input_schema: {
        type: "object",
        properties: { message_id: { type: "string" }, body: { type: "string" } },
        required: ["message_id", "body"],
        additionalProperties: false,
      },
      steps: [
        {
          name: "send",
          operation: "mailer.send",
          input: {
            kind: "object",
            fields: {
              message_id: { kind: "input", path: ["message_id"] },
              body: { kind: "input", path: ["body"] },
            },
          },
        },
      ],
    }

    const operations = await call(worker, "/api/operations")
    const denied = await call(worker, "/api/plans", { plan })
    const invalid = await call(planner, "/api/plans", {
      plan: { ...plan, steps: [{ ...plan.steps[0], operation: "shell.exec" }] },
    })
    const registered = await call(planner, "/api/plans", { plan })
    const digest = (registered.body as { definition_digest: string }).definition_digest
    const admitted = await call(worker, "/api/missions", {
      definition_digest: digest,
      input: { message_id: "msg-20", body: "Planned by an agent" },
    })
    const runId = (admitted.body as { summary: { run_id: RunId } }).summary.run_id
    await call(worker, `/api/missions/${runId}/start`, {})
    await vi.waitFor(
      async () =>
        expect(
          ((await call(worker, `/api/missions/${runId}/context`)).body as { status: string })
            .status,
        ).toBe("completed"),
      { timeout: 2_000, interval: 20 },
    )

    expect(operations.body).toMatchObject({
      items: [{ key: "mailer.send", duplicate_safety: "native_idempotency" }],
    })
    expect(denied.status).toBe(403)
    expect(invalid.status).toBe(422)
    expect(invalid.body).toMatchObject({
      error: { message: expect.stringMatching(/not connected/) },
    })
    expect(registered.status).toBe(201)
    expect(registered.body).toMatchObject({
      definition_id: "agent.notify",
      states: ["done", "failed", "send", "send_reconcile", "unresolved"],
    })
    expect(issue).toHaveBeenCalledOnce()
  })
})

async function authenticate(runner: StartedLocalRunner) {
  const bootstrap = new URL(runner.bootstrap_url).hash.slice("#bootstrap=".length)
  const response = await fetch(`${runner.origin}/api/session/bootstrap`, {
    method: "POST",
    headers: {
      origin: runner.origin,
      "x-idemantis-bootstrap": decodeURIComponent(bootstrap),
    },
  })
  const cookie = response.headers.get("set-cookie")?.split(";")[0]
  const csrf = ((await response.json()) as { csrf_token?: unknown }).csrf_token
  if (!cookie || typeof csrf !== "string") {
    throw new Error("Expected local runner authentication")
  }
  return { origin: runner.origin, cookie, "x-idemantis-csrf": csrf }
}
