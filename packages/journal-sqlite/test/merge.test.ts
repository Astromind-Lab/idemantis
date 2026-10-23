import {
  edgeBranchAnchorDigest,
  edgeBranchDigest,
  edgeDelegationDigest,
  sha256Digest,
  type EdgeDelegation,
  type EdgeMergeRequest,
  type JsonObject,
  type OperationCapabilities,
  type ProposedEvent,
} from "@idemantis/contracts"
import { EdgeAgent, EdgeStore, type EdgeClock } from "@idemantis/edge-agent"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import {
  defineMission,
  defineOperation,
  effectState,
  finalState,
  fromInput,
  mapFields,
  safeRetry,
  waitForObservation,
} from "@idemantis/sdk"
import { chmodSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import Type from "typebox"
import { afterEach, describe, expect, it } from "vitest"
import { JournalError, SqliteJournal } from "../src/index.js"

const capabilities = {
  duplicate_safety: "native_idempotency",
  reconciliation: "authoritative",
  fencing_mode: "application_epoch",
  fencing_scope: "subject",
  handoff_mode: "settle_first",
  execution_location: "either",
  cancellation: false,
  compensation: false,
  observation_stream: true,
} satisfies OperationCapabilities

const dispatch = defineOperation({
  key: "vehicle.dispatch",
  adapter: "vehicle",
  operation: "vehicle.dispatch",
  schemaVersion: 1,
  inputSchema: Type.Object({ route: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ dispatch: Type.String() }, { additionalProperties: false }),
  effectClass: "physical_actuation",
  consequence: "compensatable",
  capabilities,
})

const mission = defineMission({
  id: "merge-delivery",
  version: "1.0.0",
  input: Type.Object({ route: Type.String() }, { additionalProperties: false }),
  initial: "dispatchVehicle",
  states: {
    dispatchVehicle: effectState({
      operation: dispatch,
      effectSlot: "dispatch",
      input: mapFields({ route: fromInput("route") }),
      retry: safeRetry({ attempts: 2, backoffMs: 100 }),
      onAccepted: "waitForArrival",
      onSettled: "complete",
      onFailed: "failed",
      onUncertain: "failed",
    }),
    waitForArrival: waitForObservation({
      observationType: "vehicle.arrived",
      schemaVersion: 1,
      correlation: { kind: "effect", effectState: "dispatchVehicle" },
      timeout: { afterMs: 1_000, onTimeout: "failed" },
      onObserved: "complete",
    }),
    complete: finalState("completed"),
    failed: finalState("failed"),
  },
})

const runtime: MissionRuntimeDefinition = {
  manifest: mission.manifest,
  definition_digest: mission.definitionDigest,
  execution_binding_digest: `sha256:${"b".repeat(64)}`,
}

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const leaseId = "lse_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const adapterId = "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const attemptId = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const mergeId = "mrg_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const edgeId = "edge-merge-1" as const
const eventIds = {
  admission: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  lease: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
  started: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAX",
  result: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAY",
  observation: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAZ",
  intent: "evt_01ARZ3NDEKTSV4RRFFQ69G5FB0",
  timer: "evt_01ARZ3NDEKTSV4RRFFQ69G5FB1",
  conflict: "evt_01ARZ3NDEKTSV4RRFFQ69G5FB2",
} as const
const directories: string[] = []
const journals: SqliteJournal[] = []
const stores: EdgeStore[] = []

class TestClock implements EdgeClock {
  #wall = Date.parse("2026-01-01T00:00:10.000Z")
  #monotonic = 1_000

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
}

function temporaryPath(name: string): string {
  const directory = mkdtempSync(join(tmpdir(), "idemantis-merge-"))
  directories.push(directory)
  return join(directory, name)
}

function journalClock() {
  let tick = 0
  return () => {
    tick += 1
    return new Date(Date.UTC(2026, 0, 1, 0, 0, tick))
  }
}

function proposal(
  eventId: ProposedEvent["event_id"],
  type: string,
  payload: ProposedEvent["payload"],
  source: ProposedEvent["source"],
  causationId?: ProposedEvent["causation_id"],
): ProposedEvent {
  return {
    event_id: eventId,
    run_id: runId,
    type,
    schema_version: 1,
    source,
    ...(causationId ? { causation_id: causationId } : {}),
    ownership_epoch: 1,
    payload,
  }
}

function admission(): ProposedEvent {
  return proposal(
    eventIds.admission,
    "mission.admitted",
    {
      definition_digest: runtime.definition_digest,
      execution_binding_digest: runtime.execution_binding_digest,
      input: { route: "north" },
    },
    { kind: "system", source_id: "merge-test" },
  )
}

function createDelegation(inputDigest: ReturnType<typeof sha256Digest>): EdgeDelegation {
  const value: EdgeDelegation = {
    lease: {
      lease_id: leaseId,
      run_id: runId,
      owner_mode: "edge",
      owner_id: edgeId,
      ownership_epoch: 1,
      branch_anchor_sequence: 1,
      not_before: "2026-01-01T00:00:00.000Z",
      not_after: "2026-01-01T01:00:00.000Z",
      maximum_offline_duration_ms: 3_000_000,
      allowed_clock_uncertainty_ms: 100,
      delegation_digest: `sha256:${"a".repeat(64)}`,
    },
    definition_digest: runtime.definition_digest,
    execution_binding_digest: runtime.execution_binding_digest,
    allowed_actions: [
      {
        state: "dispatchVehicle",
        effect_class: "physical_actuation",
        operation: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        input_digests: [inputDigest],
      },
    ],
    allowed_event_types: [
      "effect.dispatch_started",
      "effect.issue_result_recorded",
      "observation.recorded",
    ],
    resource_limits: { maximum_branch_events: 20, maximum_dispatches: 10 },
  }
  return {
    ...value,
    lease: { ...value.lease, delegation_digest: edgeDelegationDigest(value) },
  }
}

function setup() {
  const runnerPath = temporaryPath("runner.sqlite")
  const journal = new SqliteJournal({ path: runnerPath, clock: journalClock() })
  journals.push(journal)
  journal.admit(runtime, admission())
  const command = journal.listCommands(runId)[0]
  const effect = journal.listEffects(runId)[0]
  if (!command || !effect) {
    throw new Error("Expected admitted effect work")
  }
  const input = Reflect.get(command.payload, "input")
  if (input === undefined) {
    throw new Error("Expected command input")
  }
  const delegation = createDelegation(sha256Digest(input))
  journal.registerOwnershipLease(
    proposal(
      eventIds.lease,
      "ownership.lease_registered",
      { lease: delegation.lease },
      { kind: "edge", source_id: edgeId },
    ),
  )
  const claimed = journal.claimCommand(command.command_id, {
    attempt_id: attemptId,
    claimed_by: edgeId,
    owner_mode: "edge",
    claimed_at: "2026-01-01T00:00:05.000Z",
    lease_duration_ms: 3_000_000,
  })
  const clock = new TestClock()
  const store = new EdgeStore({
    path: temporaryPath("edge.sqlite"),
    opened_at: "2026-01-01T00:00:00.000Z",
  })
  stores.push(store)
  const agent = new EdgeAgent({ edge_id: edgeId, boot_identity: "boot-merge-1", clock, store })
  agent.activate(delegation, { measured_clock_uncertainty_ms: 50 })
  const started = agent.append(
    proposal(
      eventIds.started,
      "effect.dispatch_started",
      {
        attempt_id: attemptId,
        command_id: claimed.command.command_id,
        effect_id: effect.effect_id,
      },
      { kind: "edge", source_id: edgeId },
      claimed.command.causing_event_id,
    ),
    {
      execution: {
        state: "dispatchVehicle",
        effect_class: "physical_actuation",
        operation: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        input_digest: sha256Digest(input),
      },
    },
  )
  clock.advance(1_000)
  agent.append(
    proposal(
      eventIds.result,
      "effect.issue_result_recorded",
      {
        attempt_id: attemptId,
        command_id: claimed.command.command_id,
        effect_id: effect.effect_id,
        result: "accepted",
      },
      { kind: "edge", source_id: edgeId },
      started.event.proposal.event_id,
    ),
  )
  const request: EdgeMergeRequest = { merge_id: mergeId, upload: agent.reconnectUpload() }
  return { journal, effect, request, runnerPath }
}

function observation(eventId: ProposedEvent["event_id"] = eventIds.observation): ProposedEvent {
  return proposal(
    eventId,
    "observation.recorded",
    {
      observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      observation_type: "vehicle.arrived",
      payload: { position: "dock" },
    },
    { kind: "adapter", source_id: "vehicle-simulator" },
  )
}

afterEach(() => {
  for (const store of stores.splice(0)) {
    store.close()
  }
  for (const journal of journals.splice(0)) {
    journal.close()
  }
  for (const directory of directories.splice(0)) {
    chmodSync(directory, 0o700)
    rmSync(directory, { recursive: true, force: true })
  }
})

describe("edge reconciliation merge", () => {
  it("orders edge events before the pending-input watermark and replays the result", () => {
    const { journal, effect, request } = setup()
    const observed = observation()
    observed.payload = { ...observed.payload, effect_id: effect.effect_id }
    const observationReceipt = journal.queuePendingInput(observed, {
      client_request_id: "merge-observation-0001",
    })
    const duplicateObservationReceipt = journal.queuePendingInput(structuredClone(observed), {
      client_request_id: "merge-observation-0001",
    })
    const changedObservation = structuredClone(observed)
    changedObservation.payload = { ...changedObservation.payload, payload: { position: "bay" } }
    expect(() =>
      journal.queuePendingInput(changedObservation, {
        client_request_id: "merge-observation-0001",
      }),
    ).toThrowError(expect.objectContaining({ code: "PENDING_INPUT_COLLISION" }))
    const intent = proposal(
      eventIds.intent,
      "operator.retry",
      {
        client_request_id: "merge-intervention-0001",
        expected_sequence: 2,
        action: "retry",
      },
      { kind: "operator", source_id: "operator-1" },
    )
    const intentReceipt = journal.queuePendingInput(intent, {
      client_request_id: "merge-intervention-0001",
    })
    expect(() => journal.processPendingInputs(runId)).toThrowError(
      expect.objectContaining({ code: "PENDING_INPUT_INVALID" }),
    )

    const merged = journal.mergeEdgeBranch(request)
    const events = journal.listEvents(runId)

    expect(observationReceipt.outcome).toBe("queued")
    expect(duplicateObservationReceipt).toEqual(observationReceipt)
    expect(intentReceipt.outcome).toBe("queued")
    expect(merged).toMatchObject({
      duplicate: false,
      pending_watermark: 2,
      final_sequence: 5,
      edge_events: [
        { edge_local_sequence: 1, canonical_sequence: 3 },
        { edge_local_sequence: 2, canonical_sequence: 4 },
      ],
      pending_outcomes: [
        { client_request_id: "merge-observation-0001", outcome: "accepted" },
        { client_request_id: "merge-intervention-0001", outcome: "rejected_stale" },
      ],
    })
    expect(events.map((event) => event.type)).toEqual([
      "mission.admitted",
      "ownership.lease_registered",
      "effect.dispatch_started",
      "effect.issue_result_recorded",
      "observation.recorded",
    ])
    expect(journal.getMission(runId).state.status).toBe("completed")
    expect(journal.listAttempts(runId)[0]?.status).toBe("result_recorded")
    expect(journal.listCommands(runId)[0]?.status).toBe("completed")
    expect(journal.verifyRun(runId)).toMatchObject({
      event_count: 5,
      state_digest: merged.state_digest,
    })

    const duplicate = journal.mergeEdgeBranch(structuredClone(request))
    expect(duplicate).toEqual({ ...merged, duplicate: true })

    const timer = journal.listTimers(runId)[0]
    if (!timer) {
      throw new Error("Expected a scheduled wait timer")
    }
    const timerEvent = proposal(
      eventIds.timer,
      "timer.fired",
      timer.payload,
      { kind: "timer", source_id: "runner-timer" },
      timer.causing_event_id,
    )
    timerEvent.logical_at = timer.due_at
    const queuedAfterWatermark = journal.queuePendingInput(timerEvent, {
      client_request_id: "merge-timer-input-0001",
    })
    expect(queuedAfterWatermark).toMatchObject({ ingress_sequence: 3, outcome: "queued" })
    expect(journal.processPendingInputs(runId)).toEqual([
      expect.objectContaining({
        client_request_id: "merge-timer-input-0001",
        outcome: "superseded",
      }),
    ])
    expect(journal.listEvents(runId)).toHaveLength(5)
    expect(journal.listTimers(runId)[0]?.status).toBe("superseded")
  })

  it("returns the committed merge result after a journal restart", () => {
    const { journal, request, runnerPath } = setup()
    const merged = journal.mergeEdgeBranch(request)
    journal.close()
    const reopened = new SqliteJournal({ path: runnerPath, clock: journalClock() })
    journals.push(reopened)

    expect(reopened.mergeEdgeBranch(structuredClone(request))).toEqual({
      ...merged,
      duplicate: true,
    })
    expect(reopened.verifyRun(runId)).toMatchObject({
      event_count: 4,
      state_digest: merged.state_digest,
    })
  })

  it("accepts a current timer queued after the merge watermark", () => {
    const { journal, request } = setup()
    const merged = journal.mergeEdgeBranch(request)
    const timer = journal.listTimers(runId)[0]
    if (!timer) {
      throw new Error("Expected a scheduled wait timer")
    }
    const timerEvent = proposal(
      eventIds.timer,
      "timer.fired",
      timer.payload,
      { kind: "timer", source_id: "runner-timer" },
      timer.causing_event_id,
    )
    timerEvent.logical_at = timer.due_at
    const receipt = journal.queuePendingInput(timerEvent, {
      client_request_id: "merge-timer-input-0001",
    })

    expect(merged).toMatchObject({ pending_watermark: 0, final_sequence: 4 })
    expect(receipt).toMatchObject({ ingress_sequence: 1, outcome: "queued" })
    expect(journal.processPendingInputs(runId)).toEqual([
      expect.objectContaining({
        client_request_id: "merge-timer-input-0001",
        outcome: "accepted",
        event_id: eventIds.timer,
      }),
    ])
    expect(journal.listEvents(runId).at(-1)?.type).toBe("timer.fired")
    expect(journal.listTimers(runId)[0]?.status).toBe("fired")
    expect(journal.getMission(runId).state.status).toBe("failed")
    expect(journal.verifyRun(runId).event_count).toBe(5)
  })

  it("rejects changed content under a committed merge identity", () => {
    const { journal, request } = setup()
    journal.mergeEdgeBranch(request)
    const changed = structuredClone(request)
    changed.upload.created_at = "2026-01-01T00:20:00.000Z"

    expect(() => journal.mergeEdgeBranch(changed)).toThrowError(
      expect.objectContaining({ code: "EDGE_MERGE_ID_COLLISION" }),
    )
    const resumed = structuredClone(request)
    resumed.merge_id = "mrg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
    expect(() => journal.mergeEdgeBranch(resumed)).toThrowError(
      expect.objectContaining({ code: "EDGE_MERGE_CONFLICT" }),
    )
    expect(journal.listEvents(runId)).toHaveLength(4)
  })

  it("quarantines a canonical branch conflict without appending an edge prefix", () => {
    const { journal, request } = setup()
    journal.append(observation(eventIds.conflict))

    expect(() => journal.mergeEdgeBranch(request)).toThrowError(
      expect.objectContaining({ code: "EDGE_MERGE_CONFLICT" }),
    )
    expect(journal.listEvents(runId)).toHaveLength(3)
    expect(journal.listQuarantinedEdgeMerges(runId)).toEqual([
      expect.objectContaining({ merge_id: mergeId, reason_code: "EDGE_MERGE_CONFLICT" }),
    ])
  })

  it("rolls back all edge events when a later branch event cannot reconcile", () => {
    const { journal, request } = setup()
    const changed = structuredClone(request)
    const second = changed.upload.events[1]
    if (!second) {
      throw new Error("Expected a second edge event")
    }
    second.proposal.payload = {
      ...second.proposal.payload,
      command_id: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAW",
    }
    second.proposal_digest = sha256Digest(second.proposal as unknown as JsonObject)
    const { branch_digest: _branchDigest, ...unsigned } = second
    second.branch_digest = edgeBranchDigest(
      edgeBranchAnchorDigest(
        changed.upload.delegation.lease.delegation_digest,
        changed.upload.activation,
      ),
      unsigned,
      changed.upload.delegation.lease.delegation_digest,
    )
    changed.upload.final_branch_digest = second.branch_digest

    expect(() => journal.mergeEdgeBranch(changed)).toThrowError(JournalError)
    expect(journal.listEvents(runId)).toHaveLength(2)
    expect(journal.listAttempts(runId)[0]).toMatchObject({ status: "claimed" })
    expect(journal.listQuarantinedEdgeMerges(runId)).toHaveLength(1)
  })

  it("authenticates edge timestamps and execution scope in the branch chain", () => {
    const { journal, request } = setup()
    const changed = structuredClone(request)
    const first = changed.upload.events[0]
    if (!first) {
      throw new Error("Expected a first edge event")
    }
    first.recorded_at = "2026-01-01T00:00:12.000Z"

    expect(() => journal.mergeEdgeBranch(changed)).toThrowError(
      expect.objectContaining({ code: "EDGE_MERGE_INVALID" }),
    )
    expect(journal.listEvents(runId)).toHaveLength(2)
    expect(journal.listQuarantinedEdgeMerges(runId)).toHaveLength(1)
  })
})
