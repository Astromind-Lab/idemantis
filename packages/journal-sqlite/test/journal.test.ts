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
  waitForOperator,
  waitForObservation,
} from "@idemantis/sdk"
import { chmodSync, mkdtempSync, rmSync, statSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { DatabaseSync } from "node:sqlite"
import Type from "typebox"
import { afterEach, describe, expect, it } from "vitest"
import { JournalError, SqliteJournal } from "../src/index.js"
import { migrations } from "../src/migrations.js"

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

const operatorMission = defineMission({
  id: "journal-operator-review",
  version: "1.0.0",
  input: Type.Object({ release: Type.String() }, { additionalProperties: false }),
  initial: "review",
  states: {
    review: waitForOperator({
      reason: "Release requires approval",
      actions: [
        {
          code: "approve",
          consequence: "irreversible",
          confirmationRequired: true,
          input: Type.Object(
            { evidence: Type.String({ minLength: 3 }) },
            { additionalProperties: false },
          ),
          presentationHints: [
            { path: "evidence", control: "textarea", label: "Evidence", order: 0 },
          ],
          evidenceRequired: true,
          reasonRequired: true,
          expectedOutcomeLabel: "Release is approved",
          onAccepted: "complete",
        },
      ],
    }),
    complete: finalState("completed"),
  },
})

const operatorRuntime: MissionRuntimeDefinition = {
  manifest: operatorMission.manifest,
  definition_digest: operatorMission.definitionDigest,
  execution_binding_digest: `sha256:${"d".repeat(64)}`,
}

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const attemptId = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const secondAttemptId = "att_01ARZ3NDEKTSV4RRFFQ69G5FAW" as const
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
  sourceId = "journal-test",
  sourceKind: ProposedEvent["source"]["kind"] = "system",
  causationId?: ProposedEvent["causation_id"],
): ProposedEvent {
  return {
    event_id: `evt_01ARZ3NDEKTSV4RRFFQ69G5FA${eventSuffixes[sequence - 1]}`,
    run_id: runId,
    type,
    schema_version: 1,
    source: { kind: sourceKind, source_id: sourceId },
    ...(causationId ? { causation_id: causationId } : {}),
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

function operatorAdmission(): ProposedEvent {
  return proposal(1, "mission.admitted", {
    definition_digest: operatorRuntime.definition_digest,
    execution_binding_digest: operatorRuntime.execution_binding_digest,
    input: { release: "release-47" },
  })
}

function operatorIntervention(): ProposedEvent {
  return proposal(
    2,
    "operator.intervention_submitted",
    {
      client_request_id: "journal-intervention-0001",
      expected_sequence: 1,
      action_code: "approve",
      target: `mission:${runId}`,
      input: { evidence: "Reviewed deployment record" },
      reason: "Evidence matches the release",
    },
    "operator-1",
    "operator",
  )
}

function admitted(journal: SqliteJournal) {
  const result = journal.admit(runtime, admission())
  const effect = journal.listEffects(runId)[0]
  if (!effect) {
    throw new Error("Expected an admitted effect")
  }
  return { result, effect }
}

function claimFirstCommand(
  journal: SqliteJournal,
  selectedAttemptId = attemptId,
  claimedAt = "2026-01-01T00:00:02Z",
  leaseDurationMs = 60_000,
) {
  const command = journal.listCommands(runId)[0]
  if (!command) {
    throw new Error("Expected a pending command")
  }
  return journal.claimCommand(command.command_id, {
    attempt_id: selectedAttemptId,
    claimed_by: "journal-worker",
    owner_mode: "runner",
    claimed_at: claimedAt,
    lease_duration_ms: leaseDurationMs,
  })
}

function startDispatch(journal: SqliteJournal, sequence: number, effectId: string) {
  const command = journal.listCommands(runId)[0]
  if (!command) {
    throw new Error("Expected a claimed command")
  }
  return journal.recordDispatchStarted(
    proposal(
      sequence,
      "effect.dispatch_started",
      {
        attempt_id: attemptId,
        command_id: command.command_id,
        effect_id: effectId,
      },
      "journal-worker",
      "runner",
      command.causing_event_id,
    ),
  )
}

function recordIssueResult(
  journal: SqliteJournal,
  sequence: number,
  effectId: string,
  result: "not_sent" | "accepted" | "completed" | "rejected" | "failed" | "uncertain",
) {
  const command = journal.listCommands(runId)[0]
  if (!command) {
    throw new Error("Expected a claimed command")
  }
  const attempt = journal.listAttempts(runId)[0]
  if (!attempt?.started_event_id) {
    throw new Error("Expected a started attempt")
  }
  return journal.recordDispatchResult(
    proposal(
      sequence,
      "effect.issue_result_recorded",
      {
        attempt_id: attemptId,
        command_id: command.command_id,
        effect_id: effectId,
        result,
      },
      "journal-worker",
      "runner",
      attempt.started_event_id,
    ),
  )
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
      schema_version: 6,
      journal_mode: "wal",
      foreign_keys: true,
    })
    expect(statSync(path).mode & 0o777).toBe(0o600)
  })

  it("upgrades an existing version 1 journal", () => {
    const path = journalPath()
    const database = new DatabaseSync(path)
    const firstMigration = migrations[0]
    database.exec(`
      CREATE TABLE journal_migrations (
        version INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        checksum TEXT NOT NULL,
        applied_at TEXT NOT NULL
      ) STRICT;
    `)
    database.exec(firstMigration.sql)
    database
      .prepare(
        "INSERT INTO journal_migrations(version, name, checksum, applied_at) VALUES (?, ?, ?, ?)",
      )
      .run(
        firstMigration.version,
        firstMigration.name,
        firstMigration.checksum,
        "2026-01-01T00:00:00Z",
      )
    database.exec("PRAGMA user_version = 1")
    database.close()

    const journal = open(path)

    expect(journal.storageInfo().schema_version).toBe(6)
    expect(() => admitted(journal)).not.toThrow()
  })

  it("persists registered definitions across reopen", () => {
    const path = journalPath()
    const journal = open(path)

    const stored = journal.registerDefinition(runtime, {
      subject_reference_keys: ["vehicle", "vehicle", "route"],
    })
    expect(stored.runtime).toEqual(runtime)
    expect(stored.subject_reference_keys).toEqual(["vehicle", "route"])
    const rebound = journal.registerDefinition(
      { ...runtime, execution_binding_digest: `sha256:${"c".repeat(64)}` },
      { subject_reference_keys: ["vehicle"] },
    )
    expect(rebound.registered_at).toBe(stored.registered_at)
    journal.close()

    const reopened = open(path)
    expect(reopened.listDefinitions()).toHaveLength(1)
    expect(reopened.getDefinition(runtime.definition_digest)).toMatchObject({
      runtime: { execution_binding_digest: `sha256:${"c".repeat(64)}` },
      subject_reference_keys: ["vehicle"],
    })
    expect(reopened.getDefinition(`sha256:${"d".repeat(64)}`)).toBeUndefined()
  })

  it("lists lightweight mission heads that track each change", () => {
    const journal = open(journalPath())
    admitted(journal)

    const [head] = journal.listMissionHeads()
    const mission = journal.getMission(runId)

    expect(head).toEqual({
      run_id: runId,
      definition_digest: runtime.definition_digest,
      current_sequence: mission.state.current_sequence,
      state_digest: mission.state_digest,
      updated_at: mission.updated_at,
    })
  })

  it("rejects definitions whose digest does not match the manifest", () => {
    const journal = open(journalPath())

    expect(() =>
      journal.registerDefinition({ ...runtime, definition_digest: `sha256:${"e".repeat(64)}` }),
    ).toThrow(expect.objectContaining({ code: "DEFINITION_INVALID" }))
    expect(() =>
      journal.registerDefinition({
        ...runtime,
        manifest: { ...runtime.manifest, states: "invalid" } as never,
      }),
    ).toThrow(expect.objectContaining({ code: "DEFINITION_INVALID" }))
    expect(journal.listDefinitions()).toEqual([])
  })

  it("commits admission, projection, effect, and outbox atomically", () => {
    const journal = open(journalPath())
    expect(journal.missionListRevision()).toBe(1)
    const { result, effect } = admitted(journal)
    const stored = journal.getMission(runId)

    expect(result.duplicate).toBe(false)
    expect(result.acceptance.sequence).toBe(1)
    expect(result.command_ids).toHaveLength(1)
    expect(stored.runtime).toEqual(runtime)
    expect(stored.state.status).toBe("running")
    expect(stored.state_digest).toBe(result.state_digest)
    expect(journal.listMissions()).toEqual([stored])
    expect(journal.missionListRevision()).toBe(2)
    expect(journal.listMissionChanges(1)).toEqual([{ revision: 2, run_id: runId }])
    expect(journal.listMissionChanges(2)).toEqual([])
    expect(journal.listEvents(runId)).toHaveLength(1)
    expect(journal.listStoredEvents(runId)).toEqual([
      {
        event: journal.listEvents(runId)[0],
        proposal_digest: result.acceptance.proposal_digest,
        state_digest: result.state_digest,
      },
    ])
    expect(journal.listStoredEventsAfter(runId, 0)).toEqual(journal.listStoredEvents(runId))
    expect(journal.listStoredEventsAfter(runId, 1)).toEqual([])
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

  it("persists prepared interventions and accepted receipts across restart", () => {
    const path = journalPath()
    const journalClock = clock()
    const first = open(path, journalClock)
    first.admit(operatorRuntime, operatorAdmission())
    const prepared = first.prepareIntervention(operatorIntervention(), "2026-01-01T00:15:00.000Z")
    expect(() => first.append(operatorIntervention())).toThrowError(
      expect.objectContaining({ code: "DISPATCH_EVENT_INVALID" }),
    )
    const reservedCollision = {
      ...operatorIntervention(),
      type: "observation.recorded",
      source: { kind: "adapter" as const, source_id: "adapter-1" },
      payload: {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        observation_type: "release.reviewed",
        payload: { result: "approved" },
      },
    }
    expect(() => first.append(reservedCollision)).toThrowError(
      expect.objectContaining({ code: "EVENT_ID_COLLISION" }),
    )
    first.close()

    const restarted = open(path, journalClock)
    expect(restarted.getPreparedIntervention(runId, prepared.client_request_id)).toEqual(prepared)
    const result = restarted.submitIntervention(
      runId,
      prepared.client_request_id,
      prepared.proposal_digest,
    )
    if (result.status !== "receipt") {
      throw new Error("Expected a durable intervention receipt")
    }

    expect(result.receipt).toMatchObject({ outcome: "accepted", accepted_sequence: 2 })
    expect(
      restarted.submitIntervention(runId, prepared.client_request_id, prepared.proposal_digest),
    ).toEqual(result)
    expect(restarted.getMission(runId).state).toMatchObject({
      status: "completed",
      current_state: "complete",
      current_sequence: 2,
    })
    expect(restarted.listReceiptUpdatesAfter(runId, "operator-1", 0)).toEqual([
      expect.objectContaining({
        revision: result.receipt.revision,
        receipt: result.receipt,
      }),
    ])
    expect(restarted.interventionReceiptRevision(runId, "operator-2")).toBe(0)
    expect(
      restarted.listInterventionReceipts(runId, "operator-2", result.receipt.revision),
    ).toEqual([])
    expect(restarted.listReceiptUpdatesAfter(runId, "operator-2", 0)).toEqual([])
  })

  it("records a stale intervention without appending its proposal", () => {
    const journal = open(journalPath())
    journal.admit(operatorRuntime, operatorAdmission())
    const prepared = journal.prepareIntervention(operatorIntervention(), "2026-01-01T00:15:00.000Z")
    journal.registerOwnershipLease({
      ...proposal(3, "ownership.lease_registered", {
        lease: {
          lease_id: "lse_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          run_id: runId,
          owner_mode: "runner",
          owner_id: "runner-1",
          ownership_epoch: 1,
          branch_anchor_sequence: 1,
          not_before: "2026-01-01T00:00:00.000Z",
          not_after: "2026-01-01T01:00:00.000Z",
          maximum_offline_duration_ms: 60_000,
          allowed_clock_uncertainty_ms: 1_000,
          delegation_digest: `sha256:${"e".repeat(64)}`,
        },
      }),
      source: { kind: "runner", source_id: "runner-1" },
    })

    const result = journal.submitIntervention(
      runId,
      prepared.client_request_id,
      prepared.proposal_digest,
    )

    expect(result).toMatchObject({ status: "receipt", receipt: { outcome: "rejected_stale" } })
    expect(journal.listEvents(runId).map((event) => event.type)).toEqual([
      "mission.admitted",
      "ownership.lease_registered",
    ])
  })

  it("expires an unsubmitted intervention without producing a receipt", () => {
    let now = new Date("2026-01-01T00:00:00.000Z")
    const journal = open(journalPath(), () => now)
    journal.admit(operatorRuntime, operatorAdmission())
    const prepared = journal.prepareIntervention(operatorIntervention(), "2026-01-01T00:15:00.000Z")
    now = new Date("2026-01-01T00:15:00.000Z")

    expect(journal.getPreparedIntervention(runId, prepared.client_request_id).state).toBe("expired")
    expect(
      journal.submitIntervention(runId, prepared.client_request_id, prepared.proposal_digest),
    ).toMatchObject({ status: "expired", prepared: { state: "expired" } })
    expect(journal.interventionReceiptRevision(runId, "operator-1")).toBe(0)
    expect(journal.listEvents(runId)).toHaveLength(1)
  })

  it("claims a command and records its dispatch lifecycle atomically", () => {
    const journal = open(journalPath())
    const { effect } = admitted(journal)
    const claimed = claimFirstCommand(journal)

    expect(claimed.command).toMatchObject({
      status: "claimed",
      claimed_by: "journal-worker",
      claim_deadline: "2026-01-01T00:01:02.000Z",
    })
    expect(claimed.attempt).toMatchObject({
      attempt_id: attemptId,
      command_id: claimed.command.command_id,
      effect_id: effect.effect_id,
      status: "claimed",
    })

    const started = startDispatch(journal, 2, effect.effect_id)
    const duplicateStart = startDispatch(journal, 2, effect.effect_id)
    expect(started.acceptance.sequence).toBe(2)
    expect(duplicateStart).toEqual({ ...started, duplicate: true })
    expect(journal.listAttempts(runId)[0]).toMatchObject({
      status: "dispatch_started",
      started_event_id: started.acceptance.event_id,
    })
    expect(journal.listEffects(runId)[0]?.status).toBe("dispatching")

    const result = recordIssueResult(journal, 3, effect.effect_id, "accepted")
    const duplicateResult = recordIssueResult(journal, 3, effect.effect_id, "accepted")
    expect(result.acceptance.sequence).toBe(3)
    expect(duplicateResult).toEqual({ ...result, duplicate: true })
    expect(journal.listAttempts(runId)[0]).toMatchObject({
      status: "result_recorded",
      result_event_id: result.acceptance.event_id,
    })
    expect(journal.listCommands(runId)[0]?.status).toBe("completed")
    expect(
      journal.claimNext(runId, {
        attempt_id: secondAttemptId,
        claimed_by: "other-worker",
        owner_mode: "runner",
        claimed_at: "2026-01-01T01:00:00Z",
        lease_duration_ms: 60_000,
      }),
    ).toBeUndefined()
  })

  it("reclaims an expired claim without changing the effect identity", () => {
    const journal = open(journalPath())
    const { effect } = admitted(journal)
    const first = claimFirstCommand(journal, attemptId, "2026-01-01T00:00:02Z", 1_000)
    const second = journal.claimCommand(first.command.command_id, {
      attempt_id: secondAttemptId,
      claimed_by: "other-worker",
      owner_mode: "runner",
      claimed_at: "2026-01-01T00:00:03Z",
      lease_duration_ms: 60_000,
    })

    expect(second.attempt.effect_id).toBe(first.attempt.effect_id)
    expect(second.attempt.effect_id).toBe(effect.effect_id)
    expect(journal.listAttempts(runId)).toHaveLength(2)
    expect(() =>
      journal.recordDispatchStarted(
        proposal(
          2,
          "effect.dispatch_started",
          {
            attempt_id: attemptId,
            command_id: first.command.command_id,
            effect_id: effect.effect_id,
          },
          "journal-worker",
          "runner",
          first.command.causing_event_id,
        ),
      ),
    ).toThrowError(expect.objectContaining({ code: "ATTEMPT_NOT_ACTIVE" }))

    journal.recordDispatchStarted(
      proposal(
        2,
        "effect.dispatch_started",
        {
          attempt_id: secondAttemptId,
          command_id: second.command.command_id,
          effect_id: effect.effect_id,
        },
        "other-worker",
        "runner",
        second.command.causing_event_id,
      ),
    )
    expect(journal.listAttempts(runId)[1]?.status).toBe("dispatch_started")
  })

  it("does not redispatch an expired attempt after dispatch starts", () => {
    const journal = open(journalPath())
    const { effect } = admitted(journal)
    claimFirstCommand(journal, attemptId, "2026-01-01T00:00:02Z", 2_000)
    startDispatch(journal, 2, effect.effect_id)

    expect(
      journal.claimNext(runId, {
        attempt_id: secondAttemptId,
        claimed_by: "other-worker",
        owner_mode: "runner",
        claimed_at: "2026-01-01T01:00:00Z",
        lease_duration_ms: 60_000,
      }),
    ).toBeUndefined()
    expect(journal.listAttempts(runId)).toHaveLength(1)
    expect(journal.listCommands(runId)[0]?.status).toBe("claimed")
  })

  it("materializes, fires, and deduplicates a durable timer", () => {
    const journal = open(journalPath())
    const { effect } = admitted(journal)
    claimFirstCommand(journal)
    startDispatch(journal, 2, effect.effect_id)
    recordIssueResult(journal, 3, effect.effect_id, "accepted")

    const timer = journal.listTimers(runId)[0]
    if (!timer) {
      throw new Error("Expected a durable timer")
    }
    expect(journal.listCommands(runId)[1]?.status).toBe("completed")
    expect(journal.listDueTimers("2026-01-01T00:00:04.999Z")).toEqual([])
    expect(journal.listDueTimers(timer.due_at)).toEqual([timer])

    const fireOptions = {
      timer_id: timer.timer_id,
      event_id: proposal(4, "timer.fired", {}).event_id,
      source_id: "journal-timer",
      fired_at: timer.due_at,
    } as const
    const fired = journal.fireTimer(fireOptions)
    const duplicate = journal.fireTimer(fireOptions)

    expect(fired.status).toBe("fired")
    expect(fired.status === "fired" && fired.append.duplicate).toBe(false)
    expect(duplicate.status).toBe("fired")
    expect(duplicate.status === "fired" && duplicate.append.duplicate).toBe(true)
    expect(journal.listTimers(runId)[0]?.status).toBe("fired")
    expect(journal.listCommands(runId)[2]?.kind).toBe("effect.reconcile")
    expect(journal.listDueTimers(timer.due_at)).toEqual([])
  })

  it("supersedes an obsolete timer without adding a mission event", () => {
    const journal = open(journalPath())
    const { effect } = admitted(journal)
    claimFirstCommand(journal)
    startDispatch(journal, 2, effect.effect_id)
    recordIssueResult(journal, 3, effect.effect_id, "accepted")
    const timer = journal.listTimers(runId)[0]
    if (!timer) {
      throw new Error("Expected a durable timer")
    }
    journal.append(
      proposal(4, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        effect_id: effect.effect_id,
        observation_type: "vehicle.arrived",
        payload: { position: "dock" },
      }),
    )

    const result = journal.fireTimer({
      timer_id: timer.timer_id,
      event_id: proposal(5, "timer.fired", {}).event_id,
      source_id: "journal-timer",
      fired_at: timer.due_at,
    })

    expect(result.status).toBe("superseded")
    expect(journal.listEvents(runId)).toHaveLength(4)
    expect(journal.listTimers(runId)[0]?.status).toBe("superseded")
  })

  it("rejects lifecycle events through the generic append path", () => {
    const journal = open(journalPath())
    const { effect } = admitted(journal)
    const command = journal.listCommands(runId)[0]
    if (!command) {
      throw new Error("Expected a pending command")
    }

    expect(() =>
      journal.append(
        proposal(2, "effect.dispatch_started", {
          attempt_id: attemptId,
          command_id: command.command_id,
          effect_id: effect.effect_id,
        }),
      ),
    ).toThrowError(expect.objectContaining({ code: "DISPATCH_EVENT_INVALID" }))
    expect(journal.listEvents(runId)).toHaveLength(1)
  })

  it("rolls back the event and outbox when reduction fails", () => {
    const journal = open(journalPath())
    const { effect } = admitted(journal)
    const claimed = claimFirstCommand(journal)
    const started = startDispatch(journal, 2, effect.effect_id)

    expect(() =>
      journal.recordDispatchResult(
        proposal(
          3,
          "effect.issue_result_recorded",
          {
            attempt_id: attemptId,
            command_id: claimed.command.command_id,
            effect_id: effect.effect_id,
            result: "unsupported",
          },
          "journal-worker",
          "runner",
          started.acceptance.event_id,
        ),
      ),
    ).toThrow()
    expect(journal.listEvents(runId)).toHaveLength(2)
    expect(journal.listCommands(runId)).toHaveLength(1)

    const accepted = recordIssueResult(journal, 3, effect.effect_id, "accepted")
    expect(accepted.acceptance.sequence).toBe(3)
    expect(journal.listEvents(runId)).toHaveLength(3)
    expect(journal.listCommands(runId)).toHaveLength(2)
  })

  it("rolls back an accepted event when outbox insertion fails", () => {
    const path = journalPath()
    const journal = open(path)
    const { effect } = admitted(journal)
    claimFirstCommand(journal)
    startDispatch(journal, 2, effect.effect_id)
    const database = new DatabaseSync(path)
    database.exec(`
      CREATE TRIGGER reject_command_insert
      BEFORE INSERT ON commands
      BEGIN
        SELECT RAISE(ABORT, 'forced outbox failure');
      END;
    `)
    database.close()

    expect(() => recordIssueResult(journal, 3, effect.effect_id, "accepted")).toThrow()
    expect(journal.listEvents(runId)).toHaveLength(2)
    expect(journal.listCommands(runId)).toHaveLength(1)
    expect(journal.getMission(runId).state.current_sequence).toBe(2)
    expect(journal.listAttempts(runId)[0]?.status).toBe("dispatch_started")
  })

  it("rebuilds after restart using only the database and latest snapshot", () => {
    const path = journalPath()
    const firstJournal = open(path)
    const { effect } = admitted(firstJournal)
    claimFirstCommand(firstJournal)
    startDispatch(firstJournal, 2, effect.effect_id)
    recordIssueResult(firstJournal, 3, effect.effect_id, "accepted")
    const snapshotRecord = firstJournal.createSnapshot(runId)
    firstJournal.append(
      proposal(4, "observation.recorded", {
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

    expect(snapshotRecord.sequence).toBe(3)
    expect(rebuilt.state.status).toBe("completed")
    expect(rebuilt.state.current_sequence).toBe(4)
    expect(rebuilt.runtime).toEqual(runtime)
    expect(verification).toEqual({
      run_id: runId,
      event_count: 4,
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
