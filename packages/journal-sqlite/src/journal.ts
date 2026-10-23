import {
  canonicalJson,
  sha256Digest,
  type AcceptedEvent,
  type CommandRecord,
  type Digest,
  type EventAcceptance,
  type EventId,
  type JsonObject,
  type ProposedEvent,
  type RunId,
} from "@idemantis/contracts"
import {
  reduceMission,
  type EffectProjection,
  type MissionRuntimeDefinition,
  type MissionState,
  type ReductionResult,
} from "@idemantis/engine"
import type { DatabaseSync } from "node:sqlite"
import { JournalDatabase } from "./database.js"
import { JournalError } from "./errors.js"
import {
  commandJson,
  eventJson,
  manifestJson,
  parseAcceptance,
  parseAcceptedEvent,
  parseCommand,
  parseDigest,
  parseEffect,
  parseManifest,
  parseProposedEvent,
  parseState,
  proposalDigest,
  stateJson,
} from "./records.js"
import type {
  AppendResult,
  JournalOptions,
  MissionSnapshot,
  ProjectionRebuildOptions,
  ReplayVerification,
  StorageInfo,
  StoredMission,
} from "./types.js"

type Row = Record<string, unknown>

interface ReplayOutcome {
  state: MissionState
  state_digest: Digest
  event_count: number
  command_count: number
}

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

function asRow(value: unknown, label: string): Row {
  if (value === undefined || value === null || typeof value !== "object") {
    throw new JournalError("CORRUPT_RECORD", `SQLite did not return ${label}`)
  }

  return value as Row
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
    const digest = proposalDigest(event)

    return this.#database.transaction(() => {
      const duplicate = this.findDuplicate(event.event_id, digest)
      if (duplicate) {
        return duplicate
      }

      const run = this.runRow(event.run_id, true)
      const definition = this.runtimeFromRow(run)
      const previousState = parseState(text(run, "state_json"))
      const accepted = this.accept(event, integer(run, "current_sequence") + 1)
      const reduction = reduceMission(definition, previousState, accepted)
      this.insertEvent(accepted, digest, reduction.state_digest)
      this.insertCommands(reduction.commands)
      this.syncEffects(reduction.state, false)
      this.updateRun(reduction, accepted.recorded_at)

      return this.appendResult(accepted.event_id, false)
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
        `SELECT commands.command_json
         FROM commands
         JOIN mission_events ON mission_events.event_id = commands.causing_event_id
         WHERE commands.run_id = ?
         ORDER BY mission_events.sequence, commands.ordinal`,
      )
      .all(runId)

    return rows.map((raw) => parseCommand(text(raw as Row, "command_json")))
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
      statement.run(
        command.command_id,
        command.run_id,
        command.causing_event_id,
        ordinal,
        command.command_slot,
        command.kind,
        command.status,
        command.ownership_epoch,
        canonicalJson(command.payload),
        commandJson(command),
        command.created_at,
      )
    }
  }

  private syncEffects(state: MissionState, replace: boolean): void {
    if (replace) {
      this.connection.prepare("DELETE FROM effects WHERE run_id = ?").run(state.run_id)
    }

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
