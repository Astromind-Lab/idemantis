import type { OperationCapabilities, ProposedEvent } from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
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
import { chmodSync, mkdtempSync, rmSync, statSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { DatabaseSync } from "node:sqlite"
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

const snapshot = defineOperation({
  key: "vehicle.snapshot",
  adapter: "vehicle",
  operation: "vehicle.snapshot",
  schemaVersion: 1,
  inputSchema: Type.Object({ vehicle: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ status: Type.String() }, { additionalProperties: false }),
  effectClass: "observation",
  consequence: "observational",
  capabilities,
})

const mission = defineMission({
  id: "journal-delivery",
  version: "1.0.0",
  input: Type.Object(
    { route: Type.String(), vehicle: Type.String() },
    { additionalProperties: false },
  ),
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
      onUncertain: "reconcileVehicle",
    }),
    waitForArrival: waitForObservation({
      observationType: "vehicle.arrived",
      schemaVersion: 1,
      correlation: { kind: "effect", effectState: "dispatchVehicle" },
      timeout: { afterMs: 1_000, onTimeout: "reconcileVehicle" },
      onObserved: "complete",
    }),
    reconcileVehicle: reconcileState({
      effectState: "dispatchVehicle",
      operation: snapshot,
      subject: fromInput("vehicle"),
      onConfirmedCompleted: "complete",
      onConfirmedNotApplied: "dispatchVehicle",
      onConfirmedInProgress: "waitForArrival",
      onConflictingState: "unresolved",
      onUnknown: "unresolved",
    }),
    complete: finalState("completed"),
    failed: finalState("failed"),
    unresolved: finalState("unresolved"),
  },
})

const runtime: MissionRuntimeDefinition = {
  manifest: mission.manifest,
  definition_digest: mission.definitionDigest,
  execution_binding_digest: `sha256:${"b".repeat(64)}`,
}

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const eventSuffixes = "VWXYZ0123456789ABCDEFGHJKMNPQRST"
const temporaryDirectories: string[] = []
const journals: SqliteJournal[] = []

function clock() {
  let tick = 0
  return () => {
    tick += 1
    return new Date(Date.UTC(2026, 0, 1, 0, 0, tick))
  }
}

function journalPath(): string {
  const directory = mkdtempSync(join(tmpdir(), "idemantis-journal-"))
  temporaryDirectories.push(directory)
  return join(directory, "journal.sqlite")
}

function open(path: string, journalClock = clock()): SqliteJournal {
  const journal = new SqliteJournal({ path, clock: journalClock })
  journals.push(journal)
  return journal
}

function proposal(
  sequence: number,
  type: string,
  payload: ProposedEvent["payload"],
): ProposedEvent {
  return {
    event_id: `evt_01ARZ3NDEKTSV4RRFFQ69G5FA${eventSuffixes[sequence - 1]}`,
    run_id: runId,
    type,
    schema_version: 1,
    source: { kind: "system", source_id: "journal-test" },
    ownership_epoch: 1,
    payload,
  }
}

function admission(): ProposedEvent {
  return proposal(1, "mission.admitted", {
    definition_digest: runtime.definition_digest,
    execution_binding_digest: runtime.execution_binding_digest,
    input: { route: "north", vehicle: "vehicle-7" },
  })
}

function admitted(journal: SqliteJournal) {
  const result = journal.admit(runtime, admission())
  const effect = journal.listEffects(runId)[0]
  if (!effect) {
    throw new Error("Expected an admitted effect")
  }
  return { result, effect }
}

afterEach(() => {
  for (const journal of journals.splice(0)) {
    journal.close()
  }
  for (const directory of temporaryDirectories.splice(0)) {
    chmodSync(directory, 0o700)
    rmSync(directory, { recursive: true, force: true })
  }
})

describe("SQLite journal", () => {
  it("migrates a private WAL database with foreign keys enabled", () => {
    const path = journalPath()
    const journal = open(path)

    expect(journal.storageInfo()).toEqual({
      schema_version: 1,
      journal_mode: "wal",
      foreign_keys: true,
    })
    expect(statSync(path).mode & 0o777).toBe(0o600)
  })

  it("commits admission, projection, effect, and outbox atomically", () => {
    const journal = open(journalPath())
    const { result, effect } = admitted(journal)
    const stored = journal.getMission(runId)

    expect(result.duplicate).toBe(false)
    expect(result.acceptance.sequence).toBe(1)
    expect(result.command_ids).toHaveLength(1)
    expect(stored.runtime).toEqual(runtime)
    expect(stored.state.status).toBe("running")
    expect(stored.state_digest).toBe(result.state_digest)
    expect(journal.listEvents(runId)).toHaveLength(1)
    expect(journal.listCommands(runId)).toEqual([
      expect.objectContaining({
        command_id: result.command_ids[0],
        kind: "effect.issue",
        payload: expect.objectContaining({ effect_id: effect.effect_id }),
      }),
    ])
    expect(effect.status).toBe("prepared")
    expect(effect.issue_count).toBe(1)
  })

  it("returns prior acceptance for a duplicate and rejects identity collisions", () => {
    const journal = open(journalPath())
    const first = journal.admit(runtime, admission())
    const duplicate = journal.admit(runtime, structuredClone(admission()))
    const changed = admission()
    changed.payload = {
      ...changed.payload,
      input: { route: "south", vehicle: "vehicle-7" },
    }

    expect(duplicate).toEqual({ ...first, duplicate: true })
    expect(() => journal.admit(runtime, changed)).toThrowError(
      expect.objectContaining({ code: "EVENT_ID_COLLISION" }),
    )
    expect(journal.listEvents(runId)).toHaveLength(1)
    expect(journal.listCommands(runId)).toHaveLength(1)
  })

  it("rolls back the event and outbox when reduction fails", () => {
    const journal = open(journalPath())
    const { effect } = admitted(journal)

    expect(() =>
      journal.append(
        proposal(2, "effect.issue_result_recorded", {
          effect_id: effect.effect_id,
          result: "unsupported",
        }),
      ),
    ).toThrow()
    expect(journal.listEvents(runId)).toHaveLength(1)
    expect(journal.listCommands(runId)).toHaveLength(1)

    const accepted = journal.append(
      proposal(2, "effect.issue_result_recorded", {
        effect_id: effect.effect_id,
        result: "accepted",
      }),
    )
    expect(accepted.acceptance.sequence).toBe(2)
    expect(journal.listEvents(runId)).toHaveLength(2)
    expect(journal.listCommands(runId)).toHaveLength(2)
  })

  it("rolls back an accepted event when outbox insertion fails", () => {
    const path = journalPath()
    const journal = open(path)
    const { effect } = admitted(journal)
    const database = new DatabaseSync(path)
    database.exec(`
      CREATE TRIGGER reject_command_insert
      BEFORE INSERT ON commands
      BEGIN
        SELECT RAISE(ABORT, 'forced outbox failure');
      END;
    `)
    database.close()

    expect(() =>
      journal.append(
        proposal(2, "effect.issue_result_recorded", {
          effect_id: effect.effect_id,
          result: "accepted",
        }),
      ),
    ).toThrow()
    expect(journal.listEvents(runId)).toHaveLength(1)
    expect(journal.listCommands(runId)).toHaveLength(1)
    expect(journal.getMission(runId).state.current_sequence).toBe(1)
  })

  it("rebuilds after restart using only the database and latest snapshot", () => {
    const path = journalPath()
    const firstJournal = open(path)
    const { effect } = admitted(firstJournal)
    firstJournal.append(
      proposal(2, "effect.issue_result_recorded", {
        effect_id: effect.effect_id,
        result: "accepted",
      }),
    )
    const snapshotRecord = firstJournal.createSnapshot(runId)
    firstJournal.append(
      proposal(3, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        effect_id: effect.effect_id,
        observation_type: "vehicle.arrived",
        payload: { position: "dock" },
      }),
    )
    firstJournal.close()

    const restarted = open(path)
    const rebuilt = restarted.rebuildProjection(runId)
    const verification = restarted.verifyRun(runId)

    expect(snapshotRecord.sequence).toBe(2)
    expect(rebuilt.state.status).toBe("completed")
    expect(rebuilt.state.current_sequence).toBe(3)
    expect(rebuilt.runtime).toEqual(runtime)
    expect(verification).toEqual({
      run_id: runId,
      event_count: 3,
      command_count: 2,
      snapshots_verified: 1,
      state_digest: rebuilt.state_digest,
    })
  })

  it("keeps one immutable snapshot per mission sequence", () => {
    const journal = open(journalPath())
    admitted(journal)
    const first = journal.createSnapshot(runId)
    const duplicate = journal.createSnapshot(runId)

    expect(duplicate).toEqual(first)
    expect(journal.listSnapshots(runId)).toEqual([first])
  })

  it("detects outbox tampering during full replay verification", () => {
    const path = journalPath()
    const journal = open(path)
    admitted(journal)
    journal.close()

    const database = new DatabaseSync(path)
    database.prepare("UPDATE commands SET command_json = ?").run("{}")
    database.close()

    const reopened = open(path)
    expect(() => reopened.verifyRun(runId)).toThrowError(
      expect.objectContaining({ code: "REPLAY_DIVERGENCE" }),
    )
  })

  it("rejects access after closing the journal", () => {
    const journal = open(journalPath())
    admitted(journal)
    journal.close()

    expect(() => journal.getMission(runId)).toThrowError(JournalError)
    expect(() => journal.getMission(runId)).toThrowError(
      expect.objectContaining({ code: "JOURNAL_CLOSED" }),
    )
  })
})
