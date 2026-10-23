import {
  EdgeReconnectUploadSchema,
  canonicalJson,
  sha256Digest,
  type AttemptRecord,
  type CommandRecord,
  type EdgeDelegation,
  type EdgeExecutionScope,
  type JsonObject,
  type ProposedEvent,
} from "@idemantis/contracts"
import { AdapterExecutor } from "@idemantis/adapter-kit"
import { SimulationKernel, VehicleSimulator } from "@idemantis/simulator"
import { chmodSync, mkdtempSync, rmSync, statSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { DatabaseSync } from "node:sqlite"
import Schema from "typebox/schema"
import { afterEach, describe, expect, it } from "vitest"
import {
  EdgeAgent,
  EdgeAgentError,
  EdgeStore,
  edgeDelegationDigest,
  type EdgeClock,
} from "../src/index.js"

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const edgeId = "edge-1" as const
const leaseId = "lse_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const adapterId = "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const attemptId = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const commandId = "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const effectId = "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const digestA = `sha256:${"a".repeat(64)}` as const
const digestB = `sha256:${"b".repeat(64)}` as const
const digestC = `sha256:${"c".repeat(64)}` as const
const dispatchInput = {
  vehicle_id: "vehicle-7",
  route: "north",
  pickup: "dock-a",
  destination: "bay-7",
}
const inputDigest = sha256Digest(dispatchInput)
const directories: string[] = []
const stores: EdgeStore[] = []

class TestClock implements EdgeClock {
  #wall: number
  #monotonic: number

  constructor(wall = "2026-01-01T00:00:00.100Z", monotonic = 1_000) {
    this.#wall = Date.parse(wall)
    this.#monotonic = monotonic
  }

  wallNow(): Date {
    return new Date(this.#wall)
  }

  monotonicNowMs(): number {
    return this.#monotonic
  }

  advance(milliseconds: number): void {
    this.#wall += milliseconds
    this.#monotonic += milliseconds
  }

  advanceWall(milliseconds: number): void {
    this.#wall += milliseconds
  }

  setMonotonic(milliseconds: number): void {
    this.#monotonic = milliseconds
  }
}

function delegation(overrides: Partial<EdgeDelegation> = {}): EdgeDelegation {
  const value: EdgeDelegation = {
    lease: {
      lease_id: leaseId,
      run_id: runId,
      owner_mode: "edge",
      owner_id: edgeId,
      ownership_epoch: 2,
      branch_anchor_sequence: 10,
      not_before: "2026-01-01T00:00:00.000Z",
      not_after: "2026-01-01T00:10:00.000Z",
      maximum_offline_duration_ms: 300_000,
      allowed_clock_uncertainty_ms: 100,
      delegation_digest: digestA,
    },
    definition_digest: digestB,
    execution_binding_digest: digestC,
    allowed_actions: [
      {
        state: "travel",
        effect_class: "physical_actuation",
        operation: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        input_digests: [inputDigest],
      },
    ],
    allowed_event_types: ["effect.dispatch_started", "effect.issue_result_recorded"],
    resource_limits: { maximum_branch_events: 10, maximum_dispatches: 5 },
  }
  const merged = { ...value, ...overrides }
  return {
    ...merged,
    lease: { ...merged.lease, delegation_digest: edgeDelegationDigest(merged) },
  }
}

function event(
  eventId: ProposedEvent["event_id"],
  type: string,
  sourceId = edgeId,
  epoch = 2,
): ProposedEvent {
  return {
    event_id: eventId,
    run_id: runId,
    type,
    schema_version: 1,
    source: { kind: "edge", source_id: sourceId },
    ownership_epoch: epoch,
    payload:
      type === "effect.dispatch_started"
        ? { attempt_id: attemptId, command_id: commandId, effect_id: effectId }
        : {
            attempt_id: attemptId,
            command_id: commandId,
            effect_id: effectId,
            result: "accepted",
          },
  }
}

const execution: EdgeExecutionScope = {
  state: "travel",
  effect_class: "physical_actuation",
  operation: "vehicle.dispatch",
  adapter_instance_id: adapterId,
  input_digest: inputDigest,
}

function command(input = dispatchInput): CommandRecord {
  return {
    command_id: commandId,
    run_id: runId,
    causing_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAA",
    command_slot: "effect.vehicle_dispatch.issue.1",
    kind: "effect.issue",
    status: "claimed",
    ownership_epoch: 2,
    payload: { effect_id: effectId, operation: "vehicle.dispatch", input },
    created_at: "2026-01-01T00:00:00.000Z",
    claim_deadline: "2026-01-01T00:01:00.000Z",
    claimed_by: edgeId,
  }
}

function attempt(): AttemptRecord {
  return {
    attempt_id: attemptId,
    command_id: commandId,
    effect_id: effectId,
    status: "claimed",
    ownership_epoch: 2,
    claimed_by: edgeId,
    claim_deadline: "2026-01-01T00:01:00.000Z",
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
  }
}

function store(path = edgePath()): EdgeStore {
  const value = new EdgeStore({ path, opened_at: "2026-01-01T00:00:00.000Z" })
  stores.push(value)
  return value
}

function edgePath(): string {
  const directory = mkdtempSync(join(tmpdir(), "idemantis-edge-"))
  directories.push(directory)
  return join(directory, "edge.sqlite")
}

function agent(selectedStore: EdgeStore, clock: EdgeClock, bootIdentity = "boot-a"): EdgeAgent {
  return new EdgeAgent({
    edge_id: edgeId,
    boot_identity: bootIdentity,
    clock,
    store: selectedStore,
  })
}

afterEach(() => {
  for (const selectedStore of stores.splice(0)) {
    selectedStore.close()
  }
  for (const directory of directories.splice(0)) {
    chmodSync(directory, 0o700)
    rmSync(directory, { recursive: true, force: true })
  }
})

describe("edge agent", () => {
  it("continues delegated travel through a partition and exports a gapless branch", () => {
    const path = edgePath()
    const clock = new TestClock()
    const firstStore = store(path)
    const first = agent(firstStore, clock)
    first.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    clock.advance(1_000)
    const started = first.append(
      event("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV", "effect.dispatch_started"),
      { execution },
    )
    clock.advance(1_000)
    const result = first.append(
      event("evt_01ARZ3NDEKTSV4RRFFQ69G5FAW", "effect.issue_result_recorded"),
    )

    expect(started).toMatchObject({ duplicate: false, event: { edge_local_sequence: 1 } })
    expect(started.event.execution).toEqual(execution)
    expect(result.event).toMatchObject({
      edge_local_sequence: 2,
      previous_branch_digest: started.event.branch_digest,
    })
    expect(first.state()).toMatchObject({ last_sequence: 2, dispatch_count: 1 })
    expect(statSync(path).mode & 0o777).toBe(0o600)
    firstStore.close()
    stores.splice(stores.indexOf(firstStore), 1)

    const reopened = agent(store(path), clock)
    const upload = reopened.reconnectUpload()
    expect(upload.events.map((item) => item.edge_local_sequence)).toEqual([1, 2])
    expect(upload.final_branch_digest).toBe(result.event.branch_digest)
    expect(Schema.Check(EdgeReconnectUploadSchema, upload)).toBe(true)
  })

  it("upgrades a persisted legacy branch when producing a reconnect upload", () => {
    const path = edgePath()
    const clock = new TestClock()
    const selectedStore = store(path)
    const selectedAgent = agent(selectedStore, clock)
    const selectedDelegation = delegation()
    selectedAgent.activate(selectedDelegation, { measured_clock_uncertainty_ms: 50 })
    const appended = selectedAgent.append(
      event("evt_01ARZ3NDEKTSV4RRFFQ69G5FAV", "effect.dispatch_started"),
      { execution },
    )
    selectedStore.close()
    const legacyEvent = structuredClone(appended.event)
    delete legacyEvent.branch_digest_version
    legacyEvent.branch_digest = sha256Digest({
      edge_local_sequence: legacyEvent.edge_local_sequence,
      previous_branch_digest: selectedDelegation.lease.delegation_digest,
      proposal_digest: legacyEvent.proposal_digest,
    })
    const database = new DatabaseSync(path)
    database
      .prepare(
        `UPDATE edge_branch_events
         SET branch_digest = ?, event_json = ? WHERE edge_local_sequence = 1`,
      )
      .run(legacyEvent.branch_digest, canonicalJson(legacyEvent as unknown as JsonObject))
    database
      .prepare("UPDATE edge_session SET last_branch_digest = ? WHERE singleton = 1")
      .run(legacyEvent.branch_digest)
    database.close()

    const reopened = store(path)
    const upload = reopened.upload("2026-01-01T00:00:12.000Z")

    expect(upload.events[0]).toMatchObject({ branch_digest_version: 2 })
    expect(upload.events[0]?.branch_digest).not.toBe(legacyEvent.branch_digest)
    expect(Schema.Check(EdgeReconnectUploadSchema, upload)).toBe(true)
  })

  it("deduplicates the same event and rejects an event id collision", () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    const proposal = event("evt_01ARZ3NDEKTSV4RRFFQ69G5FAX", "effect.dispatch_started")
    const first = selectedAgent.append(proposal, { execution })
    const duplicate = selectedAgent.append(proposal, { execution })

    expect(duplicate).toEqual({ event: first.event, duplicate: true })
    expect(() =>
      selectedAgent.append(
        {
          ...proposal,
          payload: {
            ...proposal.payload,
            effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAW",
          },
        },
        { execution },
      ),
    ).toThrowError(expect.objectContaining({ code: "EDGE_EVENT_COLLISION" }))
    expect(selectedAgent.state()?.last_sequence).toBe(1)
  })

  it("rejects a branch whose durable event content was altered", () => {
    const path = edgePath()
    const clock = new TestClock()
    const selectedStore = store(path)
    const selectedAgent = agent(selectedStore, clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FC0", "effect.dispatch_started"), {
      execution,
    })
    selectedStore.close()
    stores.splice(stores.indexOf(selectedStore), 1)
    const database = new DatabaseSync(path)
    const row = database.prepare("SELECT event_json FROM edge_branch_events").get() as {
      event_json: string
    }
    const stored = JSON.parse(row.event_json) as {
      proposal: { payload: { effect_id?: unknown; [key: string]: unknown } }
    }
    stored.proposal.payload.effect_id = "eff_01ARZ3NDEKTSV4RRFFQ69G5FAW"
    database.prepare("UPDATE edge_branch_events SET event_json = ?").run(JSON.stringify(stored))
    database.close()

    expect(() => new EdgeStore({ path })).toThrowError(
      expect.objectContaining({ code: "EDGE_STORE_CORRUPT" }),
    )
  })

  it("rejects a reopened store whose delegation no longer matches its digest", () => {
    const path = edgePath()
    const clock = new TestClock()
    const selectedStore = store(path)
    agent(selectedStore, clock).activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    selectedStore.close()
    stores.splice(stores.indexOf(selectedStore), 1)
    const database = new DatabaseSync(path)
    const row = database.prepare("SELECT delegation_json FROM edge_session").get() as {
      delegation_json: string
    }
    const stored = JSON.parse(row.delegation_json) as EdgeDelegation
    stored.allowed_actions = stored.allowed_actions.map((action) => ({
      ...action,
      state: "unlock",
    }))
    database.prepare("UPDATE edge_session SET delegation_json = ?").run(JSON.stringify(stored))
    database.close()

    expect(() => new EdgeStore({ path })).toThrowError(
      expect.objectContaining({ code: "EDGE_STORE_CORRUPT" }),
    )
  })

  it("rejects a reopened store whose event index columns were altered", () => {
    const path = edgePath()
    const clock = new TestClock()
    const selectedStore = store(path)
    const selectedAgent = agent(selectedStore, clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FC1", "effect.dispatch_started"), {
      execution,
    })
    selectedStore.close()
    stores.splice(stores.indexOf(selectedStore), 1)
    const database = new DatabaseSync(path)
    database
      .prepare("UPDATE edge_branch_events SET event_id = ?")
      .run("evt_01ARZ3NDEKTSV4RRFFQ69G5FC2")
    database.close()

    expect(() => new EdgeStore({ path })).toThrowError(
      expect.objectContaining({ code: "EDGE_STORE_CORRUPT" }),
    )
  })

  it("stops before an operation can expand the delegated scope", () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })

    expect(() =>
      selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FAY", "effect.dispatch_started"), {
        execution: { ...execution, operation: "vehicle.unlock" },
      }),
    ).toThrowError(expect.objectContaining({ stop_reason: "scope_violation" }))
    expect(selectedAgent.state()).toMatchObject({ stopped_reason: "scope_violation" })
    expect(selectedAgent.reconnectUpload().events).toEqual([])
  })

  it("waits for the skew-adjusted lease start without stopping", () => {
    const clock = new TestClock("2025-12-31T23:59:59.900Z")
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })

    expect(() =>
      selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FAZ", "effect.dispatch_started"), {
        execution,
      }),
    ).toThrowError(expect.objectContaining({ code: "EDGE_NOT_YET_VALID" }))
    expect(selectedAgent.state()?.stopped_reason).toBeUndefined()
    clock.advance(200)
    expect(
      selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FB0", "effect.dispatch_started"), {
        execution,
      }).event.edge_local_sequence,
    ).toBe(1)
  })

  it("stops at the earlier skew-adjusted lease boundary", () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    const selectedDelegation = delegation({
      lease: { ...delegation().lease, not_after: "2026-01-01T00:00:05.000Z" },
    })
    selectedAgent.activate(selectedDelegation, { measured_clock_uncertainty_ms: 50 })
    clock.advance(4_800)

    expect(() =>
      selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FB1", "effect.dispatch_started"), {
        execution,
      }),
    ).toThrowError(expect.objectContaining({ stop_reason: "lease_expired" }))
    expect(selectedAgent.state()).toMatchObject({ stopped_reason: "lease_expired" })
  })

  it("stops when the maximum offline duration is reached", () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    const selectedDelegation = delegation({
      lease: { ...delegation().lease, maximum_offline_duration_ms: 2_000 },
    })
    selectedAgent.activate(selectedDelegation, { measured_clock_uncertainty_ms: 50 })
    clock.advance(2_000)

    expect(() =>
      selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FB2", "effect.dispatch_started"), {
        execution,
      }),
    ).toThrowError(expect.objectContaining({ stop_reason: "offline_duration_exceeded" }))
  })

  it("persists expiry when state is observed without new work", () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(
      delegation({
        lease: { ...delegation().lease, maximum_offline_duration_ms: 2_000 },
      }),
      { measured_clock_uncertainty_ms: 50 },
    )
    clock.advance(2_000)

    expect(selectedAgent.state()).toMatchObject({
      stopped_reason: "offline_duration_exceeded",
    })
  })

  it("stops after a wall-clock discontinuity", () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    clock.advance(1_000)
    clock.advanceWall(101)

    expect(() =>
      selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FB3", "effect.dispatch_started"), {
        execution,
      }),
    ).toThrowError(expect.objectContaining({ stop_reason: "wall_clock_discontinuity" }))
  })

  it("stops after an unverified offline reboot", () => {
    const path = edgePath()
    const clock = new TestClock()
    const firstStore = store(path)
    const first = agent(firstStore, clock)
    first.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    firstStore.close()
    stores.splice(stores.indexOf(firstStore), 1)

    const rebooted = agent(store(path), clock, "boot-b")
    expect(rebooted.state()).toMatchObject({ stopped_reason: "unverified_reboot" })
    expect(() =>
      rebooted.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FB4", "effect.dispatch_started"), {
        execution,
      }),
    ).toThrowError(expect.objectContaining({ stop_reason: "unverified_reboot" }))
  })

  it("stops when a different edge identity opens the delegated store", () => {
    const path = edgePath()
    const clock = new TestClock()
    const firstStore = store(path)
    agent(firstStore, clock).activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    firstStore.close()
    stores.splice(stores.indexOf(firstStore), 1)

    const reopenedStore = store(path)
    const reopened = new EdgeAgent({
      edge_id: "edge-2",
      boot_identity: "boot-a",
      clock,
      store: reopenedStore,
    })
    expect(reopened.state()).toMatchObject({ stopped_reason: "edge_identity_changed" })
  })

  it("stops when monotonic continuity cannot be proved", () => {
    const path = edgePath()
    const clock = new TestClock()
    const firstStore = store(path)
    agent(firstStore, clock).activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    firstStore.close()
    stores.splice(stores.indexOf(firstStore), 1)
    clock.setMonotonic(999)

    const resumed = agent(store(path), clock)
    expect(resumed.state()).toMatchObject({ stopped_reason: "monotonic_discontinuity" })
  })

  it("reserves result capacity before allowing a dispatch to start", () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(
      delegation({ resource_limits: { maximum_branch_events: 1, maximum_dispatches: 1 } }),
      { measured_clock_uncertainty_ms: 50 },
    )

    expect(() =>
      selectedAgent.append(event("evt_01ARZ3NDEKTSV4RRFFQ69G5FB5", "effect.dispatch_started"), {
        execution,
      }),
    ).toThrowError(expect.objectContaining({ stop_reason: "branch_limit_reached" }))
    expect(selectedAgent.state()).toMatchObject({
      stopped_reason: "branch_limit_reached",
      last_sequence: 0,
    })
  })

  it("rejects activation when clock uncertainty exceeds the lease bound", () => {
    const selectedAgent = agent(store(), new TestClock())
    expect(() =>
      selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 101 }),
    ).toThrowError(EdgeAgentError)
    expect(selectedAgent.state()).toBeUndefined()
  })

  it("rejects a delegation whose bounded content does not match its digest", () => {
    const selectedAgent = agent(store(), new TestClock())
    const valid = delegation()
    const tampered = {
      ...valid,
      allowed_actions: valid.allowed_actions.map((action) => ({ ...action, state: "unlock" })),
    }

    expect(() =>
      selectedAgent.activate(tampered, { measured_clock_uncertainty_ms: 50 }),
    ).toThrowError(expect.objectContaining({ code: "EDGE_DELEGATION_INVALID" }))
    expect(selectedAgent.state()).toBeUndefined()
  })

  it("stops before an adapter can receive an input outside its delegation", async () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    let calls = 0
    const executor = {
      binding: () => ({
        operation_key: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        effect_class: "physical_actuation" as const,
        execution_location: "edge" as const,
      }),
      issue: async () => {
        calls += 1
        return "accepted" as const
      },
    }

    await expect(
      selectedAgent.dispatch({
        state: "travel",
        command: command({ ...dispatchInput, destination: "bay-8" }),
        attempt: attempt(),
        executor,
        dispatch_started_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC3",
        result_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC4",
      }),
    ).rejects.toMatchObject({ stop_reason: "scope_violation" })
    expect(calls).toBe(0)
    expect(selectedAgent.reconnectUpload().events).toEqual([])
  })

  it("does not combine permissions from different delegated actions", async () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    const base = delegation()
    selectedAgent.activate(
      delegation({
        allowed_actions: [
          ...base.allowed_actions,
          {
            state: "unlock",
            effect_class: "access_mutation",
            operation: "vehicle.unlock",
            adapter_instance_id: adapterId,
            input_digests: [digestA],
          },
        ],
      }),
      { measured_clock_uncertainty_ms: 50 },
    )
    let calls = 0
    const executor = {
      binding: () => ({
        operation_key: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        effect_class: "physical_actuation" as const,
        execution_location: "edge" as const,
      }),
      issue: async () => {
        calls += 1
        return "accepted" as const
      },
    }

    await expect(
      selectedAgent.dispatch({
        state: "unlock",
        command: command(),
        attempt: attempt(),
        executor,
        dispatch_started_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC9",
        result_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FCA",
      }),
    ).rejects.toMatchObject({ stop_reason: "scope_violation" })
    expect(calls).toBe(0)
  })

  it("records an in-flight result before stopping at lease expiry", async () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    const selectedDelegation = delegation({
      lease: { ...delegation().lease, not_after: "2026-01-01T00:00:05.000Z" },
    })
    selectedAgent.activate(selectedDelegation, { measured_clock_uncertainty_ms: 50 })
    const executor = {
      binding: () => ({
        operation_key: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        effect_class: "physical_actuation" as const,
        execution_location: "edge" as const,
      }),
      issue: async () => {
        clock.advance(4_800)
        return "accepted" as const
      },
    }

    const outcome = await selectedAgent.dispatch({
      state: "travel",
      command: command(),
      attempt: attempt(),
      executor,
      dispatch_started_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC5",
      result_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC6",
    })

    expect(outcome.result_recorded.event.edge_local_sequence).toBe(2)
    expect(selectedAgent.state()).toMatchObject({
      stopped_reason: "lease_expired",
      last_sequence: 2,
    })
  })

  it("does not reissue an attempt whose dispatch start is already durable", async () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    let calls = 0
    const executor = {
      binding: () => ({
        operation_key: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        effect_class: "physical_actuation" as const,
        execution_location: "edge" as const,
      }),
      issue: async () => {
        calls += 1
        return "accepted" as const
      },
    }
    const request = {
      state: "travel" as const,
      command: command(),
      attempt: attempt(),
      executor,
      dispatch_started_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC7" as const,
      result_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC8" as const,
    }
    await selectedAgent.dispatch(request)

    await expect(selectedAgent.dispatch(request)).rejects.toMatchObject({
      code: "EDGE_DISPATCH_ALREADY_STARTED",
    })
    expect(calls).toBe(1)
    expect(selectedAgent.state()).toMatchObject({ last_sequence: 2, dispatch_count: 1 })
  })

  it("stops before invoking an adapter beyond the dispatch limit", async () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(
      delegation({ resource_limits: { maximum_branch_events: 10, maximum_dispatches: 1 } }),
      { measured_clock_uncertainty_ms: 50 },
    )
    let calls = 0
    const executor = {
      binding: () => ({
        operation_key: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        effect_class: "physical_actuation" as const,
        execution_location: "edge" as const,
      }),
      issue: async () => {
        calls += 1
        return "accepted" as const
      },
    }
    await selectedAgent.dispatch({
      state: "travel",
      command: command(),
      attempt: attempt(),
      executor,
      dispatch_started_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FCB",
      result_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FCC",
    })
    const secondCommand: CommandRecord = {
      ...command(),
      command_id: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAW",
      payload: {
        ...command().payload,
        effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAW",
      },
    }
    const secondAttempt: AttemptRecord = {
      ...attempt(),
      attempt_id: "att_01ARZ3NDEKTSV4RRFFQ69G5FAW",
      command_id: secondCommand.command_id,
      effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAW",
    }

    await expect(
      selectedAgent.dispatch({
        state: "travel",
        command: secondCommand,
        attempt: secondAttempt,
        executor,
        dispatch_started_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FCD",
        result_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FCE",
      }),
    ).rejects.toMatchObject({ stop_reason: "dispatch_limit_reached" })
    expect(calls).toBe(1)
    expect(selectedAgent.state()).toMatchObject({
      stopped_reason: "dispatch_limit_reached",
      last_sequence: 2,
      dispatch_count: 1,
    })
  })

  it("keeps a simulated vehicle moving while the runner is partitioned", async () => {
    const clock = new TestClock()
    const selectedAgent = agent(store(), clock)
    selectedAgent.activate(delegation(), { measured_clock_uncertainty_ms: 50 })
    const kernel = new SimulationKernel({ seed: 17 })
    const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })
    await vehicle.dispatch.activateFence?.({
      adapter_instance_id: adapterId,
      operation_key: "vehicle.dispatch",
      scope_reference: "vehicle-7",
      ownership_epoch: 2,
      requested_at: "2026-01-01T00:00:00.100Z",
      signal: new AbortController().signal,
    })
    const executor = new AdapterExecutor({
      registrations: [{ adapter_instance_id: adapterId, operation: vehicle.dispatch }],
      source_id: edgeId,
      location: "edge",
      clock: { now: () => clock.wallNow() },
    })
    const outcome = await selectedAgent.dispatch({
      state: "travel",
      command: command(),
      attempt: attempt(),
      executor,
      dispatch_started_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FB7",
      result_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FB8",
    })
    await kernel.runUntilIdle()

    expect(outcome).toMatchObject({
      result: "accepted",
      dispatch_started: { event: { edge_local_sequence: 1 } },
      result_recorded: { event: { edge_local_sequence: 2 } },
    })
    expect(vehicle.snapshot().state).toMatchObject({ status: "arrived", active_epoch: 2 })
    expect(selectedAgent.state()).toMatchObject({ dispatch_count: 1, last_sequence: 2 })
  })
})
