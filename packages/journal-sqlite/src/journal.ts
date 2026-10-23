import {
  AttemptIdSchema,
  CommandIdSchema,
  EffectIdSchema,
  EventIdSchema,
  SourceIdSchema,
  TimerIdSchema,
  TimestampSchema,
  canonicalJson,
  sha256Digest,
  type AcceptedEvent,
  type AttemptId,
  type AttemptRecord,
  type CommandId,
  type CommandRecord,
  type Digest,
  type EffectId,
  type EventAcceptance,
  type EventId,
  type JsonObject,
  type ProposedEvent,
  type RunId,
  type TimerId,
  type TimerRecord,
} from "@idemantis/contracts"
import {
  EffectDispatchStartedPayloadSchema,
  EffectIssueResultPayloadSchema,
  EffectReconciliationResultPayloadSchema,
  TimerFiredPayloadSchema,
  deriveTimerId,
  reduceMission,
  type EffectDispatchStartedPayload,
  type EffectIssueResultPayload,
  type EffectProjection,
  type EffectReconciliationResultPayload,
  type MissionRuntimeDefinition,
  type MissionState,
  type ReductionResult,
} from "@idemantis/engine"
import type { DatabaseSync } from "node:sqlite"
import Schema from "typebox/schema"
import { JournalDatabase } from "./database.js"
import { JournalError } from "./errors.js"
import {
  commandJson,
  eventJson,
  manifestJson,
  parseAcceptance,
  parseAcceptedEvent,
  parseAttempt,
  parseCommand,
  parseCommandStatus,
  parseDigest,
  parseEffect,
  parseManifest,
  parseProposedEvent,
  parseState,
  parseTimer,
  proposalDigest,
  stateJson,
} from "./records.js"
import type {
  AppendResult,
  ClaimedCommand,
  CommandClaimOptions,
  JournalOptions,
  MissionSnapshot,
  ProjectionRebuildOptions,
  ReplayVerification,
  StorageInfo,
  StoredMission,
  TimerFireOptions,
  TimerFireResult,
} from "./types.js"

type Row = Record<string, unknown>

interface ReplayOutcome {
  state: MissionState
  state_digest: Digest
  event_count: number
  command_count: number
}

interface PendingAppend {
  accepted: AcceptedEvent
  digest: Digest
  reduction: ReductionResult
}

const guardedEventTypes = new Set([
  "effect.dispatch_started",
  "effect.issue_result_recorded",
  "effect.reconciliation_result_recorded",
  "timer.fired",
])

function text(row: Row, field: string): string {
  const value = row[field]
  if (typeof value !== "string") {
    throw new JournalError("CORRUPT_RECORD", `SQLite returned an invalid ${field}`)
  }

  return value
}

function integer(row: Row, field: string): number {
  const value = row[field]
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new JournalError("CORRUPT_RECORD", `SQLite returned an invalid ${field}`)
  }

  return value
}

function column(row: Row, field: string): unknown {
  return row[field]
}

function nullableText(row: Row, field: string): string | undefined {
  const value = row[field]
  if (value === null || value === undefined) {
    return undefined
  }
  if (typeof value !== "string") {
    throw new JournalError("CORRUPT_RECORD", `SQLite returned an invalid ${field}`)
  }

  return value
}

function asRow(value: unknown, label: string): Row {
  if (value === undefined || value === null || typeof value !== "object") {
    throw new JournalError("CORRUPT_RECORD", `SQLite did not return ${label}`)
  }

  return value as Row
}

function storedJson(textValue: string, label: string): unknown {
  try {
    return JSON.parse(textValue)
  } catch {
    throw new JournalError("CORRUPT_RECORD", `${label} is not valid JSON`)
  }
}

function timestamp(clock: () => Date): string {
  return clock().toISOString()
}

function eventAcceptance(row: Row): EventAcceptance {
  return parseAcceptance({
    event_id: text(row, "event_id"),
    run_id: text(row, "run_id"),
    proposal_digest: text(row, "proposal_digest"),
    sequence: integer(row, "sequence"),
    recorded_at: text(row, "recorded_at"),
  })
}

export class SqliteJournal {
  readonly #database: JournalDatabase
  readonly #clock: () => Date

  constructor(options: JournalOptions) {
    if (
      !Number.isSafeInteger(options.busy_timeout_ms ?? 5_000) ||
      (options.busy_timeout_ms ?? 5_000) < 0
    ) {
      throw new RangeError("busy_timeout_ms must be a non-negative safe integer")
    }

    this.#clock = options.clock ?? (() => new Date())
    const openedAt = timestamp(this.#clock)
    this.#database = new JournalDatabase(options.path, options.busy_timeout_ms ?? 5_000, openedAt)
  }

  admit(definition: MissionRuntimeDefinition, proposal: ProposedEvent): AppendResult {
    const event = parseProposedEvent(proposal)
    if (event.type !== "mission.admitted") {
      throw new JournalError(
        "ADMISSION_REQUIRED",
        "Mission admission requires a mission.admitted proposal",
        { run_id: event.run_id, event_id: event.event_id },
      )
    }

    const digest = proposalDigest(event)

    return this.#database.transaction(() => {
      const duplicate = this.findDuplicate(event.event_id, digest)
      if (duplicate) {
        return duplicate
      }

      if (this.runRow(event.run_id, false)) {
        throw new JournalError("RUN_ALREADY_EXISTS", `Run ${event.run_id} already exists`, {
          run_id: event.run_id,
          event_id: event.event_id,
        })
      }

      const accepted = this.accept(event, 1)
      const reduction = reduceMission(definition, null, accepted)
      this.insertRun(definition, reduction, accepted.recorded_at)
      this.insertEvent(accepted, digest, reduction.state_digest)
      this.insertCommands(reduction.commands)
      this.syncEffects(reduction.state, false)

      return this.appendResult(accepted.event_id, false)
    })
  }

  append(proposal: ProposedEvent): AppendResult {
    const event = parseProposedEvent(proposal)
    if (guardedEventTypes.has(event.type)) {
      throw new JournalError(
        "DISPATCH_EVENT_INVALID",
        `Event ${event.type} requires its atomic journal operation`,
        { run_id: event.run_id, event_id: event.event_id },
      )
    }
    const digest = proposalDigest(event)

    return this.#database.transaction(() => {
      const duplicate = this.findDuplicate(event.event_id, digest)
      if (duplicate) {
        return duplicate
      }

      const pending = this.prepareAppend(event, digest)
      this.storeAppend(pending)
      return this.appendResult(pending.accepted.event_id, false)
    })
  }

  claimCommand(commandId: CommandId, options: CommandClaimOptions): ClaimedCommand {
    this.validateClaimOptions(commandId, options)
    return this.#database.transaction(() => this.claimCommandInTransaction(commandId, options))
  }

  claimNext(runId: RunId, options: CommandClaimOptions): ClaimedCommand | undefined {
    this.validateClaimOptions(undefined, options)
    const claimedAt = new Date(Date.parse(options.claimed_at)).toISOString()

    return this.#database.transaction(() => {
      const run = this.runRow(runId, true)
      const epoch = integer(run, "ownership_epoch")
      this.connection
        .prepare(
          `UPDATE commands
           SET status = 'superseded'
           WHERE run_id = ? AND status IN ('pending', 'claimed') AND ownership_epoch <> ?`,
        )
        .run(runId, epoch)

      const raw = this.connection
        .prepare(
          `SELECT commands.command_id
           FROM commands
           LEFT JOIN effect_attempts
             ON effect_attempts.attempt_id = commands.active_attempt_id
           WHERE commands.run_id = ?
             AND commands.kind IN ('effect.issue', 'effect.reconcile')
             AND commands.ownership_epoch = ?
             AND (
               commands.status = 'pending'
               OR (
                 commands.status = 'claimed'
                 AND commands.claim_deadline <= ?
                 AND effect_attempts.status = 'claimed'
               )
             )
           ORDER BY commands.created_at, commands.ordinal
           LIMIT 1`,
        )
        .get(runId, epoch, claimedAt)

      if (!raw) {
        return undefined
      }

      return this.claimCommandInTransaction(text(raw as Row, "command_id") as CommandId, options)
    })
  }

  recordDispatchStarted(proposal: ProposedEvent): AppendResult {
    const event = parseProposedEvent(proposal)
    if (
      event.type !== "effect.dispatch_started" ||
      !Schema.Check(EffectDispatchStartedPayloadSchema, event.payload)
    ) {
      throw new JournalError(
        "DISPATCH_EVENT_INVALID",
        "Dispatch start requires a valid effect.dispatch_started proposal",
        { run_id: event.run_id, event_id: event.event_id },
      )
    }
    const payload = event.payload as unknown as EffectDispatchStartedPayload
    const digest = proposalDigest(event)

    return this.#database.transaction(() => {
      const duplicate = this.findDuplicate(event.event_id, digest)
      if (duplicate) {
        const attempt = this.attemptRow(payload.attempt_id, true)
        if (nullableText(attempt, "started_event_id") !== event.event_id) {
          throw new JournalError(
            "ATTEMPT_STATE_INVALID",
            `Attempt ${payload.attempt_id} is not linked to event ${event.event_id}`,
            { run_id: event.run_id, event_id: event.event_id },
          )
        }
        return duplicate
      }

      const pending = this.prepareAppend(event, digest)
      this.assertDispatchContext(pending.accepted, payload, "claimed")
      this.storeAppend(pending)
      const result = this.connection
        .prepare(
          `UPDATE effect_attempts
           SET status = 'dispatch_started', started_event_id = ?, updated_at = ?
           WHERE attempt_id = ? AND status = 'claimed'`,
        )
        .run(event.event_id, pending.accepted.recorded_at, payload.attempt_id)

      if (result.changes !== 1) {
        throw new JournalError(
          "TIMER_STATE_INVALID",
          `Attempt ${payload.attempt_id} could not start dispatch`,
          { run_id: event.run_id, event_id: event.event_id },
        )
      }

      return this.appendResult(event.event_id, false)
    })
  }

  recordDispatchResult(proposal: ProposedEvent): AppendResult {
    const event = parseProposedEvent(proposal)
    let payload: EffectIssueResultPayload | EffectReconciliationResultPayload
    let expectedKind: "effect.issue" | "effect.reconcile"

    if (
      event.type === "effect.issue_result_recorded" &&
      Schema.Check(EffectIssueResultPayloadSchema, event.payload)
    ) {
      payload = event.payload as unknown as EffectIssueResultPayload
      expectedKind = "effect.issue"
    } else if (
      event.type === "effect.reconciliation_result_recorded" &&
      Schema.Check(EffectReconciliationResultPayloadSchema, event.payload)
    ) {
      payload = event.payload as unknown as EffectReconciliationResultPayload
      expectedKind = "effect.reconcile"
    } else {
      throw new JournalError(
        "DISPATCH_EVENT_INVALID",
        "Dispatch result requires a valid typed result proposal",
        { run_id: event.run_id, event_id: event.event_id },
      )
    }
    const digest = proposalDigest(event)

    return this.#database.transaction(() => {
      const duplicate = this.findDuplicate(event.event_id, digest)
      if (duplicate) {
        const attempt = this.attemptRow(payload.attempt_id, true)
        if (nullableText(attempt, "result_event_id") !== event.event_id) {
          throw new JournalError(
            "ATTEMPT_STATE_INVALID",
            `Attempt ${payload.attempt_id} is not linked to event ${event.event_id}`,
            { run_id: event.run_id, event_id: event.event_id },
          )
        }
        return duplicate
      }

      const pending = this.prepareAppend(event, digest)
      this.assertDispatchContext(pending.accepted, payload, "dispatch_started")
      const command = this.commandRow(payload.command_id, true)
      if (text(command, "kind") !== expectedKind) {
        throw new JournalError(
          "DISPATCH_EVENT_INVALID",
          `Command ${payload.command_id} cannot accept ${event.type}`,
          { run_id: event.run_id, event_id: event.event_id },
        )
      }

      this.storeAppend(pending)
      const attemptResult = this.connection
        .prepare(
          `UPDATE effect_attempts
           SET status = 'result_recorded', result_event_id = ?, updated_at = ?
           WHERE attempt_id = ? AND status = 'dispatch_started'`,
        )
        .run(event.event_id, pending.accepted.recorded_at, payload.attempt_id)
      const commandResult = this.connection
        .prepare(
          `UPDATE commands
           SET status = 'completed'
           WHERE command_id = ? AND status = 'claimed' AND active_attempt_id = ?`,
        )
        .run(payload.command_id, payload.attempt_id)

      if (attemptResult.changes !== 1 || commandResult.changes !== 1) {
        throw new JournalError(
          "ATTEMPT_STATE_INVALID",
          `Attempt ${payload.attempt_id} could not record its result`,
          { run_id: event.run_id, event_id: event.event_id },
        )
      }

      return this.appendResult(event.event_id, false)
    })
  }

  getMission(runId: RunId): StoredMission {
    this.#database.assertOpen()
    const row = this.runRow(runId, true)
    return this.storedMission(row)
  }

  listEvents(runId: RunId): AcceptedEvent[] {
    this.#database.assertOpen()
    this.runRow(runId, true)
    return this.eventRows(runId, 0).map((row) => parseAcceptedEvent(text(row, "event_json")))
  }

  listCommands(runId: RunId): CommandRecord[] {
    this.#database.assertOpen()
    this.runRow(runId, true)
    const rows = this.connection
      .prepare(
        `SELECT commands.command_json, commands.status, commands.claimed_by,
                commands.claim_deadline
         FROM commands
         JOIN mission_events ON mission_events.event_id = commands.causing_event_id
         WHERE commands.run_id = ?
         ORDER BY mission_events.sequence, commands.ordinal`,
      )
      .all(runId)

    return rows.map((raw) => this.commandRecord(raw as Row))
  }

  listAttempts(runId: RunId): AttemptRecord[] {
    this.#database.assertOpen()
    this.runRow(runId, true)
    return this.connection
      .prepare(
        `SELECT effect_attempts.*
         FROM effect_attempts
         JOIN commands ON commands.command_id = effect_attempts.command_id
         WHERE commands.run_id = ?
         ORDER BY effect_attempts.created_at, effect_attempts.attempt_id`,
      )
      .all(runId)
      .map((raw) => this.attemptRecord(raw as Row))
  }

  listTimers(runId: RunId): TimerRecord[] {
    this.#database.assertOpen()
    this.runRow(runId, true)
    return this.connection
      .prepare("SELECT * FROM timers WHERE run_id = ? ORDER BY due_at, timer_id")
      .all(runId)
      .map((raw) => this.timerRecord(raw as Row))
  }

  listDueTimers(dueAt: string, limit = 100): TimerRecord[] {
    this.#database.assertOpen()
    this.validateTimestamp(dueAt, "dueAt")
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000) {
      throw new RangeError("limit must be a safe integer from 1 through 1000")
    }

    return this.connection
      .prepare(
        `SELECT * FROM timers
         WHERE status = 'scheduled' AND due_at <= ?
         ORDER BY due_at, timer_id
         LIMIT ?`,
      )
      .all(dueAt, limit)
      .map((raw) => this.timerRecord(raw as Row))
  }

  fireTimer(options: TimerFireOptions): TimerFireResult {
    this.validateTimerFireOptions(options)

    return this.#database.transaction(() => {
      const timer = this.timerRow(options.timer_id, true)
      const current = this.timerRecord(timer)
      const fireEventId = nullableText(timer, "fire_event_id")

      if (current.status === "fired" && fireEventId === options.event_id) {
        return {
          status: "fired",
          append: this.appendResult(options.event_id, true),
          timer: current,
        }
      }

      if (current.status !== "scheduled") {
        return { status: "superseded", timer: current }
      }
      if (Date.parse(current.due_at) > Date.parse(options.fired_at)) {
        throw new JournalError("TIMER_NOT_DUE", `Timer ${current.timer_id} is not due`, {
          run_id: current.run_id,
          event_id: options.event_id,
        })
      }

      const run = this.runRow(current.run_id, true)
      const state = parseState(text(run, "state_json"))
      if (!this.timerIsCurrent(current, state)) {
        this.connection
          .prepare("UPDATE timers SET status = 'superseded', updated_at = ? WHERE timer_id = ?")
          .run(options.fired_at, options.timer_id)
        return {
          status: "superseded",
          timer: this.timerRecord(this.timerRow(options.timer_id, true)),
        }
      }

      const event = parseProposedEvent({
        event_id: options.event_id,
        run_id: current.run_id,
        type: "timer.fired",
        schema_version: 1,
        source: { kind: "timer", source_id: options.source_id },
        causation_id: current.causing_event_id,
        ownership_epoch: state.ownership_epoch,
        logical_at: current.due_at,
        payload: current.payload,
      })
      if (!Schema.Check(TimerFiredPayloadSchema, event.payload)) {
        throw new JournalError("CORRUPT_RECORD", `Timer ${current.timer_id} has invalid payload`, {
          run_id: current.run_id,
        })
      }
      const digest = proposalDigest(event)
      const duplicate = this.findDuplicate(event.event_id, digest)
      if (duplicate) {
        throw new JournalError(
          "EVENT_ID_COLLISION",
          `Event ${event.event_id} exists without a timer linkage`,
          { run_id: current.run_id, event_id: event.event_id },
        )
      }

      const pending = this.prepareAppend(event, digest)
      this.storeAppend(pending)
      const result = this.connection
        .prepare(
          `UPDATE timers
           SET status = 'fired', fire_event_id = ?, updated_at = ?
           WHERE timer_id = ? AND status = 'scheduled'`,
        )
        .run(event.event_id, pending.accepted.recorded_at, options.timer_id)

      if (result.changes !== 1) {
        throw new JournalError(
          "ATTEMPT_STATE_INVALID",
          `Timer ${current.timer_id} could not fire`,
          {
            run_id: current.run_id,
            event_id: event.event_id,
          },
        )
      }

      return {
        status: "fired",
        append: this.appendResult(event.event_id, false),
        timer: this.timerRecord(this.timerRow(options.timer_id, true)),
      }
    })
  }

  listEffects(runId: RunId): EffectProjection[] {
    this.#database.assertOpen()
    this.runRow(runId, true)
    return this.connection
      .prepare("SELECT projection_json FROM effects WHERE run_id = ? ORDER BY effect_id")
      .all(runId)
      .map((raw) => parseEffect(text(raw as Row, "projection_json")))
  }

  createSnapshot(runId: RunId): MissionSnapshot {
    return this.#database.transaction(() => {
      const run = this.runRow(runId, true)
      const mission = this.storedMission(run)
      const createdAt = timestamp(this.#clock)
      this.connection
        .prepare(
          `INSERT OR IGNORE INTO mission_snapshots(
            run_id, sequence, definition_digest, state_digest, state_json, created_at
          ) VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(
          runId,
          mission.state.current_sequence,
          mission.runtime.definition_digest,
          mission.state_digest,
          stateJson(mission.state),
          createdAt,
        )

      const snapshot = this.snapshotRow(runId, mission.state.current_sequence)
      if (
        text(snapshot, "definition_digest") !== mission.runtime.definition_digest ||
        text(snapshot, "state_digest") !== mission.state_digest ||
        text(snapshot, "state_json") !== stateJson(mission.state)
      ) {
        throw new JournalError(
          "REPLAY_DIVERGENCE",
          `Snapshot ${runId}:${mission.state.current_sequence} conflicts with mission state`,
          { run_id: runId },
        )
      }

      return this.missionSnapshot(snapshot)
    })
  }

  listSnapshots(runId: RunId): MissionSnapshot[] {
    this.#database.assertOpen()
    this.runRow(runId, true)
    return this.connection
      .prepare("SELECT * FROM mission_snapshots WHERE run_id = ? ORDER BY sequence")
      .all(runId)
      .map((raw) => this.missionSnapshot(raw as Row))
  }

  rebuildProjection(runId: RunId, options: ProjectionRebuildOptions = {}): StoredMission {
    return this.#database.transaction(() => {
      const run = this.runRow(runId, true)
      const definition = this.runtimeFromRow(run)
      const outcome = this.replay(runId, definition, options.use_snapshot ?? true)
      this.syncEffects(outcome.state, true)
      this.updateRun(
        { state: outcome.state, state_digest: outcome.state_digest, commands: [] },
        text(run, "updated_at"),
      )
      return this.getMission(runId)
    })
  }

  verifyRun(runId: RunId): ReplayVerification {
    return this.#database.transaction(() => {
      const run = this.runRow(runId, true)
      const definition = this.runtimeFromRow(run)
      const outcome = this.replay(runId, definition, false)

      if (
        integer(run, "current_sequence") !== outcome.state.current_sequence ||
        text(run, "state_digest") !== outcome.state_digest ||
        text(run, "state_json") !== stateJson(outcome.state)
      ) {
        throw new JournalError(
          "REPLAY_DIVERGENCE",
          `Stored projection for ${runId} differs from full replay`,
          { run_id: runId },
        )
      }

      const storedEffects = this.listEffects(runId).map((effect) =>
        canonicalJson(effect as unknown as JsonObject),
      )
      const derivedEffects = Object.values(outcome.state.effects)
        .sort((left, right) => left.effect_id.localeCompare(right.effect_id))
        .map((effect) => canonicalJson(effect as unknown as JsonObject))
      if (
        storedEffects.length !== derivedEffects.length ||
        storedEffects.some((effect, index) => effect !== derivedEffects[index])
      ) {
        throw new JournalError(
          "REPLAY_DIVERGENCE",
          `Stored effects for ${runId} differ from full replay`,
          { run_id: runId },
        )
      }

      const snapshots = this.listSnapshots(runId)
      const digestBySequence = new Map<number, Digest>()
      let state: MissionState | null = null

      for (const row of this.eventRows(runId, 0)) {
        const event = parseAcceptedEvent(text(row, "event_json"))
        const result = reduceMission(definition, state, event)
        state = result.state
        digestBySequence.set(event.sequence, result.state_digest)
      }

      for (const snapshot of snapshots) {
        if (
          snapshot.definition_digest !== definition.definition_digest ||
          snapshot.state_digest !== digestBySequence.get(snapshot.sequence) ||
          sha256Digest(snapshot.state as unknown as JsonObject) !== snapshot.state_digest
        ) {
          throw new JournalError(
            "REPLAY_DIVERGENCE",
            `Snapshot ${runId}:${snapshot.sequence} differs from full replay`,
            { run_id: runId },
          )
        }
      }

      return {
        run_id: runId,
        event_count: outcome.event_count,
        command_count: outcome.command_count,
        snapshots_verified: snapshots.length,
        state_digest: outcome.state_digest,
      }
    })
  }

  storageInfo(): StorageInfo {
    this.#database.assertOpen()
    const version = asRow(this.connection.prepare("PRAGMA user_version").get(), "schema version")
    const journalMode = asRow(this.connection.prepare("PRAGMA journal_mode").get(), "journal mode")
    const foreignKeys = asRow(
      this.connection.prepare("PRAGMA foreign_keys").get(),
      "foreign key status",
    )

    return {
      schema_version: integer(version, "user_version"),
      journal_mode: text(journalMode, "journal_mode"),
      foreign_keys: integer(foreignKeys, "foreign_keys") === 1,
    }
  }

  close(): void {
    this.#database.close()
  }

  [Symbol.dispose](): void {
    this.close()
  }

  private validateTimestamp(value: string, label: string): number {
    const instant = Date.parse(value)
    if (!Schema.Check(TimestampSchema, value) || !Number.isFinite(instant)) {
      throw new RangeError(`${label} must be a valid UTC RFC 3339 timestamp`)
    }

    return instant
  }

  private validateClaimOptions(
    commandId: CommandId | undefined,
    options: CommandClaimOptions,
  ): void {
    if (commandId !== undefined && !Schema.Check(CommandIdSchema, commandId)) {
      throw new TypeError("commandId must satisfy CommandId")
    }
    if (!Schema.Check(AttemptIdSchema, options.attempt_id)) {
      throw new TypeError("attempt_id must satisfy AttemptId")
    }
    if (!Schema.Check(SourceIdSchema, options.claimed_by)) {
      throw new TypeError("claimed_by must satisfy SourceId")
    }
    const claimedAt = this.validateTimestamp(options.claimed_at, "claimed_at")
    if (!Number.isSafeInteger(options.lease_duration_ms) || options.lease_duration_ms < 1) {
      throw new RangeError("lease_duration_ms must be a positive safe integer")
    }
    if (!Number.isFinite(claimedAt + options.lease_duration_ms)) {
      throw new RangeError("claim deadline exceeds the supported date range")
    }
  }

  private validateTimerFireOptions(options: TimerFireOptions): void {
    if (!Schema.Check(TimerIdSchema, options.timer_id)) {
      throw new TypeError("timer_id must satisfy TimerId")
    }
    if (!Schema.Check(EventIdSchema, options.event_id)) {
      throw new TypeError("event_id must satisfy EventId")
    }
    if (!Schema.Check(SourceIdSchema, options.source_id)) {
      throw new TypeError("source_id must satisfy SourceId")
    }
    this.validateTimestamp(options.fired_at, "fired_at")
  }

  private claimCommandInTransaction(
    commandId: CommandId,
    options: CommandClaimOptions,
  ): ClaimedCommand {
    const command = this.commandRow(commandId, true)
    const runId = text(command, "run_id") as RunId
    const run = this.runRow(runId, true)
    const epoch = integer(run, "ownership_epoch")
    const commandEpoch = integer(command, "ownership_epoch")
    const existingAttempt = this.attemptRow(options.attempt_id, false)

    if (existingAttempt) {
      const attempt = this.attemptRecord(existingAttempt)
      const expectedDeadline = new Date(
        this.validateTimestamp(options.claimed_at, "claimed_at") + options.lease_duration_ms,
      ).toISOString()
      if (
        attempt.command_id !== commandId ||
        attempt.claimed_by !== options.claimed_by ||
        attempt.created_at !== options.claimed_at ||
        attempt.claim_deadline !== expectedDeadline
      ) {
        throw new JournalError(
          "ATTEMPT_ID_COLLISION",
          `Attempt ${options.attempt_id} was already used for a different claim`,
          { run_id: runId },
        )
      }

      return { command: this.commandRecord(this.commandRow(commandId, true)), attempt }
    }

    if (commandEpoch !== epoch) {
      throw new JournalError(
        "COMMAND_OWNERSHIP_STALE",
        `Command ${commandId} belongs to ownership epoch ${commandEpoch}, not ${epoch}`,
        { run_id: runId },
      )
    }

    const kind = text(command, "kind")
    if (kind !== "effect.issue" && kind !== "effect.reconcile") {
      throw new JournalError("COMMAND_NOT_CLAIMABLE", `Command ${commandId} is not dispatchable`, {
        run_id: runId,
      })
    }

    const status = parseCommandStatus(column(command, "status"))
    if (status === "claimed") {
      const activeAttemptId = nullableText(command, "active_attempt_id")
      const activeAttempt = activeAttemptId
        ? this.attemptRow(activeAttemptId as AttemptId, true)
        : undefined
      if (
        !activeAttempt ||
        text(activeAttempt, "status") !== "claimed" ||
        this.validateTimestamp(text(command, "claim_deadline"), "claim_deadline") >
          this.validateTimestamp(options.claimed_at, "claimed_at")
      ) {
        throw new JournalError(
          "COMMAND_NOT_CLAIMABLE",
          `Command ${commandId} has an active or ambiguous attempt`,
          { run_id: runId },
        )
      }
    } else if (status !== "pending") {
      throw new JournalError("COMMAND_NOT_CLAIMABLE", `Command ${commandId} is ${status}`, {
        run_id: runId,
      })
    }

    const parsedCommand = this.commandRecord(command)
    const { effect_id: effectId } = parsedCommand.payload
    if (!Schema.Check(EffectIdSchema, effectId)) {
      throw new JournalError("CORRUPT_RECORD", `Command ${commandId} has no valid effect_id`, {
        run_id: runId,
      })
    }
    const effect = this.connection
      .prepare("SELECT effect_id FROM effects WHERE effect_id = ? AND run_id = ?")
      .get(effectId, runId)
    if (!effect) {
      throw new JournalError("CORRUPT_RECORD", `Command ${commandId} references a missing effect`, {
        run_id: runId,
      })
    }

    const deadline = new Date(
      this.validateTimestamp(options.claimed_at, "claimed_at") + options.lease_duration_ms,
    ).toISOString()
    this.connection
      .prepare(
        `INSERT INTO effect_attempts(
          attempt_id, command_id, effect_id, status, ownership_epoch, claimed_by,
          claim_deadline, created_at, updated_at
        ) VALUES (?, ?, ?, 'claimed', ?, ?, ?, ?, ?)`,
      )
      .run(
        options.attempt_id,
        commandId,
        effectId,
        epoch,
        options.claimed_by,
        deadline,
        options.claimed_at,
        options.claimed_at,
      )
    const update = this.connection
      .prepare(
        `UPDATE commands
         SET status = 'claimed', claimed_by = ?, claim_deadline = ?, active_attempt_id = ?
         WHERE command_id = ? AND status IN ('pending', 'claimed')`,
      )
      .run(options.claimed_by, deadline, options.attempt_id, commandId)

    if (update.changes !== 1) {
      throw new JournalError("COMMAND_NOT_CLAIMABLE", `Command ${commandId} could not be claimed`, {
        run_id: runId,
      })
    }

    return {
      command: this.commandRecord(this.commandRow(commandId, true)),
      attempt: this.attemptRecord(this.attemptRow(options.attempt_id, true)),
    }
  }

  private assertDispatchContext(
    event: AcceptedEvent,
    payload: { attempt_id: AttemptId; command_id: CommandId; effect_id: EffectId },
    expectedStatus: "claimed" | "dispatch_started",
  ): void {
    const attempt = this.attemptRow(payload.attempt_id, true)
    const command = this.commandRow(payload.command_id, true)
    const run = this.runRow(event.run_id, true)
    const expectedCausation =
      expectedStatus === "claimed"
        ? text(command, "causing_event_id")
        : nullableText(attempt, "started_event_id")

    if (
      text(attempt, "command_id") !== payload.command_id ||
      text(attempt, "effect_id") !== payload.effect_id ||
      text(attempt, "status") !== expectedStatus ||
      text(command, "run_id") !== event.run_id ||
      nullableText(command, "active_attempt_id") !== payload.attempt_id ||
      text(command, "status") !== "claimed" ||
      text(command, "claimed_by") !== event.source.source_id ||
      (event.source.kind !== "runner" && event.source.kind !== "edge") ||
      event.causation_id !== expectedCausation ||
      integer(attempt, "ownership_epoch") !== event.ownership_epoch ||
      integer(command, "ownership_epoch") !== event.ownership_epoch ||
      integer(run, "ownership_epoch") !== event.ownership_epoch
    ) {
      throw new JournalError(
        "ATTEMPT_NOT_ACTIVE",
        `Attempt ${payload.attempt_id} does not own command ${payload.command_id}`,
        { run_id: event.run_id, event_id: event.event_id },
      )
    }

    if (
      expectedStatus === "claimed" &&
      Date.parse(text(attempt, "claim_deadline")) <= Date.parse(event.recorded_at)
    ) {
      throw new JournalError(
        "ATTEMPT_NOT_ACTIVE",
        `Attempt ${payload.attempt_id} expired before dispatch started`,
        { run_id: event.run_id, event_id: event.event_id },
      )
    }
  }

  private commandRow(commandId: CommandId, required: true): Row
  private commandRow(commandId: CommandId, required: false): Row | undefined
  private commandRow(commandId: CommandId, required: boolean): Row | undefined {
    const row = this.connection
      .prepare("SELECT * FROM commands WHERE command_id = ?")
      .get(commandId)
    if (!row && required) {
      throw new JournalError("COMMAND_NOT_FOUND", `Command ${commandId} does not exist`)
    }

    return row as Row | undefined
  }

  private attemptRow(attemptId: AttemptId, required: true): Row
  private attemptRow(attemptId: AttemptId, required: false): Row | undefined
  private attemptRow(attemptId: AttemptId, required: boolean): Row | undefined {
    const row = this.connection
      .prepare("SELECT * FROM effect_attempts WHERE attempt_id = ?")
      .get(attemptId)
    if (!row && required) {
      throw new JournalError("ATTEMPT_NOT_FOUND", `Attempt ${attemptId} does not exist`)
    }

    return row as Row | undefined
  }

  private timerRow(timerId: TimerId, required: true): Row
  private timerRow(timerId: TimerId, required: false): Row | undefined
  private timerRow(timerId: TimerId, required: boolean): Row | undefined {
    const row = this.connection.prepare("SELECT * FROM timers WHERE timer_id = ?").get(timerId)
    if (!row && required) {
      throw new JournalError("TIMER_NOT_FOUND", `Timer ${timerId} does not exist`)
    }

    return row as Row | undefined
  }

  private commandRecord(row: Row): CommandRecord {
    const immutable = parseCommand(text(row, "command_json"))
    const claimedBy = nullableText(row, "claimed_by")
    const claimDeadline = nullableText(row, "claim_deadline")
    return parseCommand(
      canonicalJson({
        ...immutable,
        status: parseCommandStatus(column(row, "status")),
        ...(claimedBy ? { claimed_by: claimedBy } : {}),
        ...(claimDeadline ? { claim_deadline: claimDeadline } : {}),
      }),
    )
  }

  private attemptRecord(row: Row): AttemptRecord {
    const startedEventId = nullableText(row, "started_event_id")
    const resultEventId = nullableText(row, "result_event_id")
    return parseAttempt({
      attempt_id: text(row, "attempt_id"),
      command_id: text(row, "command_id"),
      effect_id: text(row, "effect_id"),
      status: text(row, "status"),
      ownership_epoch: integer(row, "ownership_epoch"),
      claimed_by: text(row, "claimed_by"),
      claim_deadline: text(row, "claim_deadline"),
      ...(startedEventId ? { started_event_id: startedEventId } : {}),
      ...(resultEventId ? { result_event_id: resultEventId } : {}),
      created_at: text(row, "created_at"),
      updated_at: text(row, "updated_at"),
    })
  }

  private timerRecord(row: Row): TimerRecord {
    return parseTimer({
      timer_id: text(row, "timer_id"),
      run_id: text(row, "run_id"),
      status: text(row, "status"),
      due_at: text(row, "due_at"),
      purpose: text(row, "purpose"),
      causing_event_id: text(row, "causing_event_id"),
      payload: storedJson(text(row, "payload_json"), "Stored timer payload"),
    })
  }

  private timerIsCurrent(timer: TimerRecord, state: MissionState): boolean {
    if (state.terminal_outcome || !Schema.Check(TimerFiredPayloadSchema, timer.payload)) {
      return false
    }
    if (
      state.current_state_instance_id === timer.payload.state_instance_id &&
      deriveTimerId(state.run_id, state.current_state_instance_id) === timer.timer_id
    ) {
      return true
    }
    if (!state.active_effect_id) {
      return false
    }

    const effect = state.effects[state.active_effect_id]
    return (
      effect?.state_instance_id === timer.payload.state_instance_id &&
      effect.retry_timer_id === timer.timer_id
    )
  }

  private get connection(): DatabaseSync {
    return this.#database.connection
  }

  private accept(proposal: ProposedEvent, sequence: number): AcceptedEvent {
    return {
      ...proposal,
      sequence,
      recorded_at: timestamp(this.#clock),
    }
  }

  private prepareAppend(event: ProposedEvent, digest: Digest): PendingAppend {
    const run = this.runRow(event.run_id, true)
    const definition = this.runtimeFromRow(run)
    const previousState = parseState(text(run, "state_json"))
    const accepted = this.accept(event, integer(run, "current_sequence") + 1)
    const reduction = reduceMission(definition, previousState, accepted)
    return { accepted, digest, reduction }
  }

  private storeAppend(pending: PendingAppend): void {
    this.insertEvent(pending.accepted, pending.digest, pending.reduction.state_digest)
    this.insertCommands(pending.reduction.commands)
    this.syncEffects(pending.reduction.state, false)
    this.updateRun(pending.reduction, pending.accepted.recorded_at)
  }

  private findDuplicate(eventId: EventId, digest: Digest): AppendResult | undefined {
    const raw = this.connection
      .prepare(
        `SELECT event_id, run_id, proposal_digest, sequence, recorded_at, state_digest
         FROM mission_events WHERE event_id = ?`,
      )
      .get(eventId)

    if (!raw) {
      return undefined
    }

    const row = raw as Row
    if (text(row, "proposal_digest") !== digest) {
      throw new JournalError(
        "EVENT_ID_COLLISION",
        `Event ${eventId} was already used for different content`,
        { event_id: eventId },
      )
    }

    return this.appendResult(eventId, true)
  }

  private appendResult(eventId: EventId, duplicate: boolean): AppendResult {
    const row = asRow(
      this.connection
        .prepare(
          `SELECT event_id, run_id, proposal_digest, sequence, recorded_at, state_digest
           FROM mission_events WHERE event_id = ?`,
        )
        .get(eventId),
      `event ${eventId}`,
    )
    const commandRows = this.connection
      .prepare("SELECT command_id FROM commands WHERE causing_event_id = ? ORDER BY ordinal")
      .all(eventId)

    return {
      acceptance: eventAcceptance(row),
      duplicate,
      state_digest: parseDigest(column(row, "state_digest"), "Event state digest"),
      command_ids: commandRows.map(
        (raw) => text(raw as Row, "command_id") as CommandRecord["command_id"],
      ),
    }
  }

  private runRow(runId: RunId, required: true): Row
  private runRow(runId: RunId, required: false): Row | undefined
  private runRow(runId: RunId, required: boolean): Row | undefined {
    const row = this.connection.prepare("SELECT * FROM mission_runs WHERE run_id = ?").get(runId)
    if (!row && required) {
      throw new JournalError("RUN_NOT_FOUND", `Run ${runId} does not exist`, {
        run_id: runId,
      })
    }

    return row as Row | undefined
  }

  private runtimeFromRow(row: Row): MissionRuntimeDefinition {
    return {
      manifest: parseManifest(text(row, "manifest_json")),
      definition_digest: parseDigest(column(row, "definition_digest"), "Definition digest"),
      execution_binding_digest: parseDigest(
        column(row, "execution_binding_digest"),
        "Execution binding digest",
      ),
    }
  }

  private storedMission(row: Row): StoredMission {
    return {
      runtime: this.runtimeFromRow(row),
      state: parseState(text(row, "state_json")),
      state_digest: parseDigest(column(row, "state_digest"), "State digest"),
      created_at: text(row, "created_at"),
      updated_at: text(row, "updated_at"),
    }
  }

  private insertRun(
    definition: MissionRuntimeDefinition,
    reduction: ReductionResult,
    recordedAt: string,
  ): void {
    const manifest = definition.manifest
    this.connection
      .prepare(
        `INSERT INTO mission_runs(
          run_id, definition_id, definition_version, definition_digest,
          manifest_schema_version, minimum_interpreter_version,
          execution_binding_digest, manifest_json, state_json, state_digest,
          status, current_sequence, ownership_epoch, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        reduction.state.run_id,
        manifest.definition_id,
        manifest.definition_version,
        definition.definition_digest,
        manifest.manifest_schema_version,
        manifest.minimum_interpreter_version,
        definition.execution_binding_digest,
        manifestJson(manifest),
        stateJson(reduction.state),
        reduction.state_digest,
        reduction.state.status,
        reduction.state.current_sequence,
        reduction.state.ownership_epoch,
        recordedAt,
        recordedAt,
      )
  }

  private updateRun(reduction: ReductionResult, recordedAt: string): void {
    this.connection
      .prepare(
        `UPDATE mission_runs
         SET state_json = ?, state_digest = ?, status = ?, current_sequence = ?,
             ownership_epoch = ?, updated_at = ?
         WHERE run_id = ?`,
      )
      .run(
        stateJson(reduction.state),
        reduction.state_digest,
        reduction.state.status,
        reduction.state.current_sequence,
        reduction.state.ownership_epoch,
        recordedAt,
        reduction.state.run_id,
      )
  }

  private insertEvent(event: AcceptedEvent, digest: Digest, stateDigest: Digest): void {
    this.connection
      .prepare(
        `INSERT INTO mission_events(
          event_id, run_id, sequence, type, schema_version, ownership_epoch,
          proposal_digest, event_json, state_digest, recorded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        event.event_id,
        event.run_id,
        event.sequence,
        event.type,
        event.schema_version,
        event.ownership_epoch,
        digest,
        eventJson(event),
        stateDigest,
        event.recorded_at,
      )
  }

  private insertCommands(commands: readonly CommandRecord[]): void {
    const statement = this.connection.prepare(
      `INSERT INTO commands(
        command_id, run_id, causing_event_id, ordinal, command_slot, kind,
        status, ownership_epoch, payload_json, command_json, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )

    for (const [ordinal, command] of commands.entries()) {
      const status = command.kind === "timer.schedule" ? "completed" : command.status
      statement.run(
        command.command_id,
        command.run_id,
        command.causing_event_id,
        ordinal,
        command.command_slot,
        command.kind,
        status,
        command.ownership_epoch,
        canonicalJson(command.payload),
        commandJson(command),
        command.created_at,
      )

      if (command.kind === "timer.schedule") {
        this.insertTimer(command)
      }
    }
  }

  private insertTimer(command: CommandRecord): void {
    const {
      timer_id: timerId,
      due_at: dueAt,
      purpose,
      state_instance_id: stateInstanceId,
    } = command.payload
    if (
      !Schema.Check(TimerIdSchema, timerId) ||
      !Schema.Check(TimestampSchema, dueAt) ||
      typeof purpose !== "string" ||
      purpose.length < 1 ||
      purpose.length > 256 ||
      !Schema.Check(TimerFiredPayloadSchema, {
        timer_id: timerId,
        state_instance_id: stateInstanceId,
      })
    ) {
      throw new JournalError(
        "CORRUPT_RECORD",
        `Timer command ${command.command_id} has an invalid payload`,
        { run_id: command.run_id },
      )
    }

    const timer = parseTimer({
      timer_id: timerId,
      run_id: command.run_id,
      status: "scheduled",
      due_at: dueAt,
      purpose,
      causing_event_id: command.causing_event_id,
      payload: {
        timer_id: timerId,
        state_instance_id: stateInstanceId,
      },
    })
    this.connection
      .prepare(
        `INSERT INTO timers(
          timer_id, run_id, status, due_at, purpose, causing_event_id,
          payload_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        timer.timer_id,
        timer.run_id,
        timer.status,
        timer.due_at,
        timer.purpose,
        timer.causing_event_id,
        canonicalJson(timer.payload),
        command.created_at,
        command.created_at,
      )
  }

  private syncEffects(state: MissionState, replace: boolean): void {
    const statement = this.connection.prepare(
      `INSERT INTO effects(
        effect_id, run_id, state_instance_id, state_name, effect_slot, operation,
        status, required, issue_count, retry_timer_id, ownership_epoch,
        projection_json, updated_sequence
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(effect_id) DO UPDATE SET
        status = excluded.status,
        required = excluded.required,
        issue_count = excluded.issue_count,
        retry_timer_id = excluded.retry_timer_id,
        ownership_epoch = excluded.ownership_epoch,
        projection_json = excluded.projection_json,
        updated_sequence = excluded.updated_sequence
      WHERE effects.run_id = excluded.run_id
        AND effects.state_instance_id = excluded.state_instance_id
        AND effects.state_name = excluded.state_name
        AND effects.effect_slot = excluded.effect_slot
        AND effects.operation = excluded.operation`,
    )

    for (const effect of Object.values(state.effects)) {
      const result = statement.run(
        effect.effect_id,
        state.run_id,
        effect.state_instance_id,
        effect.state_name,
        effect.effect_slot,
        effect.operation,
        effect.status,
        effect.required ? 1 : 0,
        effect.issue_count,
        effect.retry_timer_id,
        effect.ownership_epoch,
        canonicalJson(effect as unknown as JsonObject),
        state.current_sequence,
      )

      if (result.changes !== 1) {
        throw new JournalError(
          "CORRUPT_RECORD",
          `Effect ${effect.effect_id} conflicts with another projection`,
          { run_id: state.run_id },
        )
      }
    }

    if (replace) {
      const effectIds = Object.keys(state.effects)
      if (effectIds.length === 0) {
        this.connection.prepare("DELETE FROM effects WHERE run_id = ?").run(state.run_id)
      } else {
        const placeholders = effectIds.map(() => "?").join(", ")
        this.connection
          .prepare(`DELETE FROM effects WHERE run_id = ? AND effect_id NOT IN (${placeholders})`)
          .run(state.run_id, ...effectIds)
      }
    }
  }

  private eventRows(runId: RunId, afterSequence: number): Row[] {
    return this.connection
      .prepare(
        `SELECT event_id, sequence, event_json, state_digest
         FROM mission_events WHERE run_id = ? AND sequence > ? ORDER BY sequence`,
      )
      .all(runId, afterSequence) as Row[]
  }

  private commandsForEvent(eventId: EventId): Row[] {
    return this.connection
      .prepare("SELECT command_json FROM commands WHERE causing_event_id = ? ORDER BY ordinal")
      .all(eventId) as Row[]
  }

  private replay(
    runId: RunId,
    definition: MissionRuntimeDefinition,
    useSnapshot: boolean,
  ): ReplayOutcome {
    let state: MissionState | null = null
    let stateDigest: Digest | undefined
    let startSequence = 0
    let commandCount = 0

    if (useSnapshot) {
      const raw = this.connection
        .prepare("SELECT * FROM mission_snapshots WHERE run_id = ? ORDER BY sequence DESC LIMIT 1")
        .get(runId)

      if (raw) {
        const snapshot = this.missionSnapshot(raw as Row)
        if (
          snapshot.definition_digest !== definition.definition_digest ||
          sha256Digest(snapshot.state as unknown as JsonObject) !== snapshot.state_digest
        ) {
          throw new JournalError(
            "REPLAY_DIVERGENCE",
            `Snapshot ${runId}:${snapshot.sequence} failed validation`,
            { run_id: runId },
          )
        }
        state = snapshot.state
        stateDigest = snapshot.state_digest
        startSequence = snapshot.sequence
      }
    }

    const rows = this.eventRows(runId, startSequence)
    for (const row of rows) {
      const event = parseAcceptedEvent(text(row, "event_json"))
      const reduction = reduceMission(definition, state, event)

      if (
        reduction.state_digest !== parseDigest(column(row, "state_digest"), "Event state digest")
      ) {
        throw new JournalError(
          "REPLAY_DIVERGENCE",
          `Event ${event.event_id} produced a different state digest`,
          { run_id: runId, event_id: event.event_id },
        )
      }

      const storedCommands = this.commandsForEvent(event.event_id)
      const derivedCommands = reduction.commands.map(commandJson)
      const storedCommandJson = storedCommands.map((command) => text(command, "command_json"))
      if (
        derivedCommands.length !== storedCommandJson.length ||
        derivedCommands.some((command, index) => command !== storedCommandJson[index])
      ) {
        throw new JournalError(
          "REPLAY_DIVERGENCE",
          `Event ${event.event_id} produced different outbox commands`,
          { run_id: runId, event_id: event.event_id },
        )
      }

      state = reduction.state
      stateDigest = reduction.state_digest
      commandCount += reduction.commands.length
    }

    if (!state || !stateDigest) {
      throw new JournalError("CORRUPT_RECORD", `Run ${runId} has no replayable events`, {
        run_id: runId,
      })
    }

    return {
      state,
      state_digest: stateDigest,
      event_count: startSequence + rows.length,
      command_count: commandCount,
    }
  }

  private snapshotRow(runId: RunId, sequence: number): Row {
    return asRow(
      this.connection
        .prepare("SELECT * FROM mission_snapshots WHERE run_id = ? AND sequence = ?")
        .get(runId, sequence),
      `snapshot ${runId}:${sequence}`,
    )
  }

  private missionSnapshot(row: Row): MissionSnapshot {
    const state = parseState(text(row, "state_json"))
    const runId = text(row, "run_id") as RunId
    const sequence = integer(row, "sequence")

    if (state.run_id !== runId || state.current_sequence !== sequence) {
      throw new JournalError(
        "CORRUPT_RECORD",
        `Snapshot ${runId}:${sequence} contains the wrong mission state`,
        { run_id: runId },
      )
    }

    return {
      run_id: runId,
      sequence,
      definition_digest: parseDigest(
        column(row, "definition_digest"),
        "Snapshot definition digest",
      ),
      state_digest: parseDigest(column(row, "state_digest"), "Snapshot state digest"),
      state,
      created_at: text(row, "created_at"),
    }
  }
}
