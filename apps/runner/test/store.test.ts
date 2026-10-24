import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import type { OperationCapabilities, RunId, SourceId } from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import {
  defineMission,
  defineOperation,
  effectState,
  finalState,
  fromInput,
  mapFields,
  safeRetry,
} from "@idemantis/sdk"
import {
  EffectDetailSchema,
  EffectPageSchema,
  InterventionReceiptPageSchema,
  InterventionReceiptSchema,
  MissionDetailSchema,
  MissionListPageSchema,
  PreparedInterventionPageSchema,
  PreparedInterventionSchema,
  ReplayResultSchema,
  StreamEnvelopeSchema,
  TimelinePageSchema,
  TransportError,
  verifyGraphArtifact,
} from "@idemantis/transport"
import Type from "typebox"
import Schema from "typebox/schema"
import { afterEach, describe, expect, it } from "vitest"
import { RunnerStore } from "../src/index.js"
import { InterventionRuntime } from "./fixture.js"

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
  key: "delivery.dispatch",
  adapter: "delivery",
  operation: "delivery.dispatch",
  schemaVersion: 1,
  inputSchema: Type.Object({ destination: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ reference: Type.String() }, { additionalProperties: false }),
  effectClass: "physical_actuation",
  consequence: "compensatable",
  capabilities,
})

const delivery = defineMission({
  id: "runner-delivery",
  version: "1.0.0",
  input: Type.Object(
    { destination: Type.String(), shipment: Type.String() },
    { additionalProperties: false },
  ),
  initial: "dispatch",
  states: {
    dispatch: effectState({
      operation: dispatch,
      effectSlot: "dispatch",
      input: mapFields({ destination: fromInput("destination") }),
      retry: safeRetry({ attempts: 2, backoffMs: 100 }),
      onAccepted: "complete",
      onSettled: "complete",
      onFailed: "failed",
      onUncertain: "failed",
    }),
    complete: finalState("completed"),
    failed: finalState("failed"),
  },
})

const runtime: MissionRuntimeDefinition = {
  manifest: delivery.manifest,
  definition_digest: delivery.definitionDigest,
  execution_binding_digest: `sha256:${"b".repeat(64)}`,
}

const runA = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as RunId
const runB = "run_01ARZ3NDEKTSV4RRFFQ69G5FAW" as RunId
const runC = "run_01ARZ3NDEKTSV4RRFFQ69G5FAX" as RunId
const operatorId = "local-operator" as SourceId
const clientRequestId = "operator-request-0001"
const temporaryDirectories: string[] = []
const journals: SqliteJournal[] = []

function openJournal(): SqliteJournal {
  const directory = mkdtempSync(join(tmpdir(), "idemantis-runner-"))
  temporaryDirectories.push(directory)
  let tick = 0
  const journal = new SqliteJournal({
    path: join(directory, "runner.sqlite"),
    clock: () => {
      tick += 1
      return new Date(Date.UTC(2026, 9, 24, 8, 0, tick))
    },
  })
  journals.push(journal)
  return journal
}

function store(): RunnerStore {
  const eventIds = [
    "01ARZ3NDEKTSV4RRFFQ69G5FAY",
    "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
    "01ARZ3NDEKTSV4RRFFQ69G5FB0",
  ]
  let idIndex = 0
  const runner = new RunnerStore({
    journal: openJournal(),
    cursor_secret: new Uint8Array(32).fill(7),
    clock: () => new Date("2026-10-24T08:00:00.000Z"),
    id: () => {
      const id = eventIds[idIndex]
      idIndex += 1
      if (!id) {
        throw new Error("Runner test exhausted deterministic IDs")
      }
      return id
    },
  })
  runner.registerDefinition(runtime, { subject_reference_keys: ["shipment"] })
  runner.registerDefinition(InterventionRuntime, { subject_reference_keys: ["subject"] })
  return runner
}

function admit(runner: RunnerStore, run_id: RunId, shipment: string) {
  return runner.admitMission({
    definition_digest: runtime.definition_digest,
    run_id,
    input: { destination: "north", shipment },
  })
}

function admitIntervention(runner: RunnerStore, run_id: RunId = runA) {
  return runner.admitMission({
    definition_digest: InterventionRuntime.definition_digest,
    run_id,
    input: { subject: "release-47" },
  })
}

function interventionRequest(run_id: RunId = runA) {
  return {
    client_request_id: clientRequestId,
    expected_sequence: 1,
    action_code: "approve_release",
    target: `mission:${run_id}`,
    input: { evidence: "Reviewed deployment record" },
    reason: "Evidence matches the release",
  }
}

afterEach(() => {
  for (const journal of journals.splice(0)) {
    journal.close()
  }
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { recursive: true, force: true })
  }
})

describe("runner store", () => {
  it("projects admitted missions through the runner read models", () => {
    const runner = store()
    const admittedDetail = admit(runner, runA, "shipment-7")
    const journal = journals.at(-1)
    if (!journal) {
      throw new Error("Expected the runner journal")
    }
    journal.registerOwnershipLease({
      event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FB1",
      run_id: runA,
      type: "ownership.lease_registered",
      schema_version: 1,
      source: { kind: "runner", source_id: "local-runner" },
      ownership_epoch: 1,
      payload: {
        lease: {
          lease_id: "lse_01ARZ3NDEKTSV4RRFFQ69G5FB2",
          run_id: runA,
          owner_mode: "runner",
          owner_id: "local-runner",
          ownership_epoch: 1,
          branch_anchor_sequence: 1,
          not_before: "2026-10-24T08:00:00.000Z",
          not_after: "2026-10-24T09:00:00.000Z",
          maximum_offline_duration_ms: 60_000,
          allowed_clock_uncertainty_ms: 1_000,
          delegation_digest: `sha256:${"c".repeat(64)}`,
        },
      },
    })
    const detail = runner.missionDetail(runA)
    const queue = runner.listMissions({ bucket: "active" })
    const timeline = runner.timeline(runA)
    const effects = runner.effects(runA)
    const effect = effects.items[0]

    expect(Schema.Check(MissionDetailSchema, detail)).toBe(true)
    expect(Schema.Check(MissionListPageSchema, queue)).toBe(true)
    expect(Schema.Check(TimelinePageSchema, timeline)).toBe(true)
    expect(Schema.Check(EffectPageSchema, effects)).toBe(true)
    expect(verifyGraphArtifact(detail.graph)).toBe(true)
    expect(queue.list_revision).toBe(3)
    expect(detail.summary.state_entered_at).toBe(admittedDetail.summary.state_entered_at)
    expect(detail.summary.updated_at).not.toBe(admittedDetail.summary.updated_at)
    expect(queue.items).toHaveLength(1)
    expect(queue.total_count).toBe(1)
    expect(queue.items[0]?.subject_references).toEqual([
      {
        kind: "shipment",
        value: { state: "value", classification: "internal", value: "shipment-7" },
      },
    ])
    expect(timeline.items.map((item) => item.sequence)).toEqual([1, 2])
    expect(effect?.operation).toBe("delivery.dispatch")
    if (!effect) {
      throw new Error("Expected the admitted mission to declare an effect")
    }
    expect(Schema.Check(EffectDetailSchema, runner.effectDetail(runA, effect.effect_id))).toBe(true)

    const replay = runner.replay(runA, 2)
    expect(Schema.Check(ReplayResultSchema, replay)).toBe(true)
    expect(replay.verification).toBe("match")
    expect(replay.stored_digest).toBe(replay.rebuilt_digest)
  })

  it("refreshes cached queue summaries when a mission or its definition changes", () => {
    const runner = store()
    admit(runner, runA, "shipment-a")
    const before = runner.listMissions({ bucket: "all" }).items[0]

    runner.recordObservation(
      runA,
      {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FC0",
        observation_type: "delivery.noted",
        payload: {},
      },
      "local-operator",
    )
    const advanced = runner.listMissions({ bucket: "all" }).items[0]
    runner.registerDefinition(runtime, { subject_reference_keys: [] })
    const rekeyed = runner.listMissions({ bucket: "all" }).items[0]

    expect(before?.subject_references).toHaveLength(1)
    expect(advanced?.current_sequence).toBe((before?.current_sequence ?? 0) + 1)
    expect(rekeyed?.subject_references).toEqual([])
  })

  it("binds pagination cursors to the queue revision and query", () => {
    const runner = store()
    admit(runner, runB, "shipment-b")
    admit(runner, runA, "shipment-a")

    const first = runner.listMissions({ bucket: "active", limit: 1 })
    expect(first.items.map((item) => item.run_id)).toEqual([runA])
    expect(first.next_cursor).toBeDefined()
    const cursor = first.next_cursor
    if (!cursor) {
      throw new Error("Expected a cursor for the remaining mission")
    }
    const second = runner.listMissions({
      bucket: "active",
      limit: 1,
      cursor,
    })
    expect(second.items.map((item) => item.run_id)).toEqual([runB])

    admit(runner, runC, "shipment-c")
    expect(() => runner.listMissions({ bucket: "active", limit: 1, cursor })).toThrowError(
      expect.objectContaining({ code: "CURSOR_INVALID" }),
    )
  })

  it("projects resumable mission and operations streams from durable history", () => {
    const runner = store()
    const operationsPosition = runner.operationsStreamPosition(1)
    admit(runner, runA, "shipment-a")

    const missionPosition = runner.missionStreamPosition(runA, 0)
    const mission = runner.missionStreamBatch(runA, missionPosition)
    expect(mission).toHaveLength(1)
    expect(mission.every((envelope) => Schema.Check(StreamEnvelopeSchema, envelope))).toBe(true)
    expect(mission[0]).toMatchObject({
      kind: "mission",
      body: { run_id: runA, sequence: 1 },
    })

    const missionCursor = mission[0]?.resume_cursor
    if (!missionCursor) {
      throw new Error("Expected a mission resume cursor")
    }
    expect(runner.missionStreamPosition(runA, undefined, missionCursor)).toBe(1)
    expect(runner.missionStreamBatch(runA, 1)).toEqual([])

    const operations = runner.operationsStreamBatch(operationsPosition)
    expect(operations).toHaveLength(1)
    expect(operations.every((envelope) => Schema.Check(StreamEnvelopeSchema, envelope))).toBe(true)
    expect(operations[0]).toMatchObject({
      kind: "operations",
      body: { list_revision: 2, changed_run_ids: [runA] },
    })

    expect(() => runner.operationsStreamPosition(undefined, missionCursor)).toThrowError(
      expect.objectContaining({ code: "CURSOR_INVALID" }),
    )
  })

  it("expires stream cursors instead of replaying an unbounded resume window", () => {
    let now = new Date("2026-10-24T08:00:00.000Z")
    const runner = new RunnerStore({
      journal: openJournal(),
      cursor_secret: new Uint8Array(32).fill(9),
      cursor_lifetime_ms: 10,
      clock: () => now,
    })
    const cursor = runner.operationsStreamHeartbeat(1).resume_cursor
    now = new Date("2026-10-24T08:00:00.011Z")

    expect(() => runner.operationsStreamPosition(undefined, cursor)).toThrowError(
      expect.objectContaining({ code: "CURSOR_EXPIRED" }),
    )
  })

  it("rejects invalid boundaries before journal mutation", () => {
    const runner = store()

    expect(() =>
      runner.admitMission({
        definition_digest: runtime.definition_digest,
        run_id: runA,
        input: { destination: "north" },
      }),
    ).toThrowError(expect.objectContaining({ code: "INVALID_REQUEST" }))
    expect(() => runner.listMissions({ limit: 0 })).toThrowError(TransportError)
    expect(runner.listMissions({ bucket: "all" }).items).toEqual([])
  })

  it("prepares, submits, retries, and streams an operator intervention", () => {
    const runner = store()
    const admitted = admitIntervention(runner)
    const descriptor = admitted.valid_interventions[0]

    expect(admitted.summary.queue_bucket).toBe("needs_attention")
    expect(admitted.projection.status).toBe("paused")
    expect(descriptor).toMatchObject({
      action_code: "approve_release",
      target: `mission:${runA}`,
      consequence: "irreversible",
      confirmation_required: true,
      evidence_required: true,
      reason_required: true,
    })

    const request = interventionRequest()
    const prepared = runner.prepareIntervention(runA, operatorId, request)
    const repeatedPreparation = runner.prepareIntervention(
      runA,
      operatorId,
      structuredClone(request),
    )

    expect(Schema.Check(PreparedInterventionSchema, prepared)).toBe(true)
    expect(repeatedPreparation).toEqual(prepared)
    expect(runner.missionDetail(runA).projection.current_sequence).toBe(1)
    expect(() =>
      runner.prepareIntervention(runA, operatorId, {
        ...request,
        reason: "Different content",
      }),
    ).toThrowError(expect.objectContaining({ code: "CLIENT_REQUEST_ID_COLLISION" }))

    const receiptPosition = runner.receiptStreamPosition(runA, operatorId, 0)
    const submission = {
      client_request_id: prepared.client_request_id,
      proposal_digest: prepared.proposal_digest,
    }
    const receipt = runner.submitIntervention(runA, operatorId, submission)
    const repeatedReceipt = runner.submitIntervention(runA, operatorId, submission)

    expect(Schema.Check(InterventionReceiptSchema, receipt)).toBe(true)
    expect(receipt).toMatchObject({ outcome: "accepted", accepted_sequence: 2 })
    expect(repeatedReceipt).toEqual(receipt)
    expect(runner.missionDetail(runA).projection).toMatchObject({
      status: "completed",
      current_state: "complete",
      current_sequence: 2,
    })
    expect(runner.timeline(runA).items[1]).toMatchObject({
      event_type: "operator.intervention_submitted",
      source: { kind: "operator", source_id: operatorId },
    })

    const activity = runner.interventionActivity(runA, operatorId)
    const receipts = runner.interventionReceipts(runA, operatorId)
    expect(Schema.Check(PreparedInterventionPageSchema, activity)).toBe(true)
    expect(Schema.Check(InterventionReceiptPageSchema, receipts)).toBe(true)
    expect(activity.items).toEqual([expect.objectContaining({ state: "submitted" })])
    expect(receipts.items).toEqual([receipt])
    expect(runner.interventionReceiptById(runA, operatorId, clientRequestId)).toEqual(receipt)

    const updates = runner.receiptStreamBatch(runA, operatorId, receiptPosition)
    expect(updates).toHaveLength(1)
    expect(updates[0]).toMatchObject({
      kind: "receipt",
      body: { run_id: runA, client_request_id: clientRequestId, receipt },
    })
    const cursor = updates[0]?.resume_cursor
    if (!cursor) {
      throw new Error("Expected a receipt resume cursor")
    }
    expect(runner.receiptStreamPosition(runA, operatorId, undefined, cursor)).toBe(receipt.revision)
    expect(runner.receiptStreamBatch(runA, operatorId, receipt.revision)).toEqual([])
    const otherOperator = "other-operator" as SourceId
    expect(runner.receiptStreamPosition(runA, otherOperator, 0)).toBe(0)
    expect(runner.receiptStreamBatch(runA, otherOperator, 0)).toEqual([])
  })

  it("rejects malformed actions and preserves discarded preparations", () => {
    const runner = store()
    admitIntervention(runner)
    const request = interventionRequest()

    expect(() =>
      runner.prepareIntervention(runA, operatorId, {
        ...request,
        input: { evidence: "no", unexpected: true },
      }),
    ).toThrowError(expect.objectContaining({ code: "INVALID_REQUEST" }))
    expect(() =>
      runner.prepareIntervention(runA, operatorId, {
        client_request_id: request.client_request_id,
        expected_sequence: request.expected_sequence,
        action_code: request.action_code,
        target: request.target,
        input: request.input,
      }),
    ).toThrowError(expect.objectContaining({ code: "INVALID_REQUEST" }))
    expect(() =>
      runner.prepareIntervention(runA, operatorId, {
        ...request,
        target: `mission:${runB}`,
      }),
    ).toThrowError(expect.objectContaining({ code: "ACTION_FORBIDDEN" }))

    const prepared = runner.prepareIntervention(runA, operatorId, request)
    const submission = {
      client_request_id: prepared.client_request_id,
      proposal_digest: prepared.proposal_digest,
    }
    const discarded = runner.discardIntervention(runA, operatorId, submission)

    expect(discarded.state).toBe("discarded")
    expect(runner.discardIntervention(runA, operatorId, submission)).toEqual(discarded)
    expect(() => runner.submitIntervention(runA, operatorId, submission)).toThrowError(
      expect.objectContaining({ code: "INVALID_REQUEST" }),
    )
    expect(runner.missionDetail(runA).projection.current_sequence).toBe(1)
  })
})
