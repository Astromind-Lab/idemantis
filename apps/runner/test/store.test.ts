import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import type { OperationCapabilities, RunId } from "@idemantis/contracts"
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
  MissionDetailSchema,
  MissionListPageSchema,
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
  return runner
}

function admit(runner: RunnerStore, run_id: RunId, shipment: string) {
  return runner.admitMission({
    definition_digest: runtime.definition_digest,
    run_id,
    input: { destination: "north", shipment },
  })
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
})
