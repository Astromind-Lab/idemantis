import {
  EdgeActivationRecordSchema,
  EdgeBranchEventSchema,
  EdgeDelegationSchema,
  EdgeExecutionScopeSchema,
  EdgeStopReasonSchema,
  AttemptIdSchema,
  CommandIdSchema,
  EffectIdSchema,
  IssueResultSchema,
  ProposedEventSchema,
  ReconciliationResultSchema,
  TimestampSchema,
  canonicalJson,
  sha256Digest,
  type EdgeActivationRecord,
  type EdgeBranchEvent,
  type EdgeDelegation,
  type EdgeReconnectUpload,
  type EdgeStopReason,
  type JsonObject,
} from "@idemantis/contracts"
import { closeSync, constants, fchmodSync, mkdirSync, openSync } from "node:fs"
import { dirname, resolve } from "node:path"
import { DatabaseSync } from "node:sqlite"
import Schema from "typebox/schema"
import { edgeDelegationDigest } from "./digest.js"
import { EdgeAgentError } from "./errors.js"
import type { EdgeAppendInput, EdgeAppendResult, StoredEdgeSession } from "./types.js"

type Row = Record<string, unknown>

interface DispatchIdentity {
  kind: "started" | "result"
  attempt_id: string
  command_id: string
  effect_id: string
}

export interface EdgeStoreOptions {
  path: string
  busy_timeout_ms?: number
  opened_at?: string
}

const migrationSql = `
CREATE TABLE edge_session (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  delegation_json TEXT NOT NULL CHECK (json_valid(delegation_json)),
  activation_json TEXT NOT NULL CHECK (json_valid(activation_json)),
  status TEXT NOT NULL CHECK (status IN ('active', 'stopped')),
  stopped_reason TEXT,
  last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0),
  last_branch_digest TEXT,
  dispatch_count INTEGER NOT NULL CHECK (dispatch_count >= 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (
    (status = 'active' AND stopped_reason IS NULL)
    OR (status = 'stopped' AND stopped_reason IS NOT NULL)
  )
) STRICT;

CREATE TABLE edge_branch_events (
  edge_local_sequence INTEGER PRIMARY KEY CHECK (edge_local_sequence >= 1),
  event_id TEXT NOT NULL UNIQUE,
  proposal_digest TEXT NOT NULL,
  branch_digest TEXT NOT NULL UNIQUE,
  event_json TEXT NOT NULL CHECK (json_valid(event_json)),
  recorded_at TEXT NOT NULL
) STRICT;

CREATE TABLE edge_dispatch_attempts (
  attempt_id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('dispatch_started', 'result_recorded')),
  started_sequence INTEGER NOT NULL UNIQUE,
  result_sequence INTEGER UNIQUE,
  CHECK (
    (status = 'dispatch_started' AND result_sequence IS NULL)
    OR (status = 'result_recorded' AND result_sequence IS NOT NULL)
  ),
  FOREIGN KEY (started_sequence) REFERENCES edge_branch_events(edge_local_sequence),
  FOREIGN KEY (result_sequence) REFERENCES edge_branch_events(edge_local_sequence)
) STRICT;
`

const migrationName = "edge_branch_journal"
const migrationChecksum = sha256Digest({ name: migrationName, sql: migrationSql })

function prepareFile(path: string): string {
  if (path === ":memory:") {
    return path
  }
  const resolved = resolve(path)
  mkdirSync(dirname(resolved), { recursive: true, mode: 0o700 })
  const descriptor = openSync(
    resolved,
    constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW,
    0o600,
  )
  try {
    fchmodSync(descriptor, 0o600)
  } finally {
    closeSync(descriptor)
  }
  return resolved
}

function text(row: Row, field: string): string {
  const value = row[field]
  if (typeof value !== "string") {
    throw new EdgeAgentError("EDGE_STORE_CORRUPT", `SQLite returned an invalid ${field}`)
  }
  return value
}

function optionalText(row: Row, field: string): string | undefined {
  const value = row[field]
  if (value === null || value === undefined) {
    return undefined
  }
  if (typeof value !== "string") {
    throw new EdgeAgentError("EDGE_STORE_CORRUPT", `SQLite returned an invalid ${field}`)
  }
  return value
}

function integer(row: Row, field: string): number {
  const value = row[field]
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new EdgeAgentError("EDGE_STORE_CORRUPT", `SQLite returned an invalid ${field}`)
  }
  return value
}

function optionalInteger(row: Row, field: string): number | undefined {
  const value = row[field]
  if (value === null || value === undefined) {
    return undefined
  }
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new EdgeAgentError("EDGE_STORE_CORRUPT", `SQLite returned an invalid ${field}`)
  }
  return value
}

function json(textValue: string, label: string): unknown {
  try {
    return JSON.parse(textValue) as unknown
  } catch {
    throw new EdgeAgentError("EDGE_STORE_CORRUPT", `${label} is not valid JSON`)
  }
}

function delegation(row: Row): EdgeDelegation {
  const value = json(text(row, "delegation_json"), "Stored edge delegation")
  if (!Schema.Check(EdgeDelegationSchema, value)) {
    throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Stored edge delegation is invalid")
  }
  return value
}

function activation(row: Row): EdgeActivationRecord {
  const value = json(text(row, "activation_json"), "Stored edge activation")
  if (!Schema.Check(EdgeActivationRecordSchema, value)) {
    throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Stored edge activation is invalid")
  }
  return value
}

function branchEvent(row: Row): EdgeBranchEvent {
  const value = json(text(row, "event_json"), "Stored edge event")
  if (!Schema.Check(EdgeBranchEventSchema, value)) {
    throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Stored edge event is invalid")
  }
  return value
}

function dispatchIdentity(event: EdgeBranchEvent): DispatchIdentity | undefined {
  const kind =
    event.proposal.type === "effect.dispatch_started"
      ? "started"
      : event.proposal.type === "effect.issue_result_recorded" ||
          event.proposal.type === "effect.reconciliation_result_recorded"
        ? "result"
        : undefined
  if (!kind) {
    return undefined
  }
  const payload = event.proposal.payload
  const attemptId = Reflect.get(payload, "attempt_id")
  const commandId = Reflect.get(payload, "command_id")
  const effectId = Reflect.get(payload, "effect_id")
  const result = Reflect.get(payload, "result")
  const expectedKeys = kind === "started" ? 3 : 4
  if (
    Object.keys(payload).length !== expectedKeys ||
    !Schema.Check(AttemptIdSchema, attemptId) ||
    !Schema.Check(CommandIdSchema, commandId) ||
    !Schema.Check(EffectIdSchema, effectId) ||
    (event.proposal.type === "effect.issue_result_recorded" &&
      !Schema.Check(IssueResultSchema, result)) ||
    (event.proposal.type === "effect.reconciliation_result_recorded" &&
      !Schema.Check(ReconciliationResultSchema, result))
  ) {
    throw new EdgeAgentError("EDGE_DISPATCH_INVALID", "Edge dispatch event identity is invalid")
  }
  return {
    kind,
    attempt_id: attemptId,
    command_id: commandId,
    effect_id: effectId,
  }
}

function authorized(delegationValue: EdgeDelegation, event: EdgeBranchEvent): boolean {
  const lease = delegationValue.lease
  if (
    event.proposal.run_id !== lease.run_id ||
    event.proposal.source.kind !== "edge" ||
    event.proposal.source.source_id !== lease.owner_id ||
    event.proposal.ownership_epoch !== lease.ownership_epoch ||
    !delegationValue.allowed_event_types.includes(event.proposal.type)
  ) {
    return false
  }
  const execution = event.execution
  if (!execution) {
    return true
  }
  return delegationValue.allowed_actions.some(
    (allowed) =>
      allowed.state === execution.state &&
      allowed.effect_class === execution.effect_class &&
      allowed.operation === execution.operation &&
      allowed.adapter_instance_id === execution.adapter_instance_id &&
      allowed.input_digests.includes(execution.input_digest),
  )
}

export class EdgeStore {
  readonly #database: DatabaseSync
  #closed = false

  constructor(options: EdgeStoreOptions) {
    const busyTimeout = options.busy_timeout_ms ?? 5_000
    if (!Number.isSafeInteger(busyTimeout) || busyTimeout < 0) {
      throw new RangeError("busy_timeout_ms must be a non-negative safe integer")
    }
    this.#database = new DatabaseSync(prepareFile(options.path), {
      allowExtension: false,
      enableDoubleQuotedStringLiterals: false,
      enableForeignKeyConstraints: true,
    })
    this.#database.exec(`
      PRAGMA foreign_keys = ON;
      PRAGMA trusted_schema = OFF;
      PRAGMA secure_delete = ON;
      PRAGMA busy_timeout = ${busyTimeout};
      PRAGMA synchronous = FULL;
    `)
    if (options.path !== ":memory:") {
      this.#database.exec("PRAGMA journal_mode = WAL")
    }
    try {
      this.migrate(options.opened_at ?? new Date().toISOString())
      this.verifyIntegrity()
    } catch (error) {
      this.#closed = true
      this.#database.close()
      throw error
    }
  }

  activate(delegationValue: EdgeDelegation, activationValue: EdgeActivationRecord): boolean {
    if (
      !Schema.Check(EdgeDelegationSchema, delegationValue) ||
      !Schema.Check(EdgeActivationRecordSchema, activationValue) ||
      delegationValue.lease.owner_mode !== "edge" ||
      edgeDelegationDigest(delegationValue) !== delegationValue.lease.delegation_digest ||
      activationValue.lease_id !== delegationValue.lease.lease_id ||
      activationValue.edge_id !== delegationValue.lease.owner_id ||
      activationValue.delegation_digest !== delegationValue.lease.delegation_digest ||
      activationValue.measured_clock_uncertainty_ms >
        delegationValue.lease.allowed_clock_uncertainty_ms
    ) {
      throw new EdgeAgentError("EDGE_DELEGATION_INVALID", "Edge activation is invalid")
    }
    return this.transaction(() => {
      const existing = this.sessionRow(false)
      if (existing) {
        if (
          text(existing, "delegation_json") ===
            canonicalJson(delegationValue as unknown as JsonObject) &&
          text(existing, "activation_json") ===
            canonicalJson(activationValue as unknown as JsonObject)
        ) {
          return false
        }
        throw new EdgeAgentError(
          "EDGE_ALREADY_ACTIVATED",
          "Edge store already contains another delegation",
        )
      }
      this.#database
        .prepare(
          `INSERT INTO edge_session(
            singleton, delegation_json, activation_json, status, stopped_reason,
            last_sequence, last_branch_digest, dispatch_count, created_at, updated_at
          ) VALUES (1, ?, ?, 'active', NULL, 0, NULL, 0, ?, ?)`,
        )
        .run(
          canonicalJson(delegationValue as unknown as JsonObject),
          canonicalJson(activationValue as unknown as JsonObject),
          activationValue.wall_at,
          activationValue.wall_at,
        )
      return true
    })
  }

  state(): StoredEdgeSession | undefined {
    this.assertOpen()
    const row = this.sessionRow(false)
    if (!row) {
      return undefined
    }
    const reason = optionalText(row, "stopped_reason")
    if (reason !== undefined && !Schema.Check(EdgeStopReasonSchema, reason)) {
      throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Stored edge stop reason is invalid")
    }
    const lastBranchDigest = optionalText(row, "last_branch_digest")
    return {
      delegation: delegation(row),
      activation: activation(row),
      ...(reason ? { stopped_reason: reason } : {}),
      last_sequence: integer(row, "last_sequence"),
      dispatch_count: integer(row, "dispatch_count"),
      ...(lastBranchDigest ? { last_branch_digest: lastBranchDigest } : {}),
    }
  }

  stop(reason: EdgeStopReason, at: string): EdgeStopReason {
    if (!Schema.Check(EdgeStopReasonSchema, reason) || !Schema.Check(TimestampSchema, at)) {
      throw new EdgeAgentError("EDGE_EVENT_INVALID", "Edge stop record is invalid")
    }
    return this.transaction(() => {
      const row = this.sessionRow(true)
      const existing = optionalText(row, "stopped_reason")
      if (existing) {
        if (!Schema.Check(EdgeStopReasonSchema, existing)) {
          throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Stored edge stop reason is invalid")
        }
        return existing
      }
      this.#database
        .prepare(
          "UPDATE edge_session SET status = 'stopped', stopped_reason = ?, updated_at = ? WHERE singleton = 1",
        )
        .run(reason, at)
      return reason
    })
  }

  append(input: EdgeAppendInput): EdgeAppendResult {
    if (
      !Schema.Check(ProposedEventSchema, input.proposal) ||
      !Schema.Check(TimestampSchema, input.recorded_at) ||
      (input.execution !== undefined && !Schema.Check(EdgeExecutionScopeSchema, input.execution))
    ) {
      throw new EdgeAgentError("EDGE_EVENT_INVALID", "Edge branch event is invalid")
    }
    const outcome = this.transaction(() => {
      const proposalDigest = sha256Digest(input.proposal as unknown as JsonObject)
      const duplicate = this.#database
        .prepare("SELECT proposal_digest, event_json FROM edge_branch_events WHERE event_id = ?")
        .get(input.proposal.event_id) as Row | undefined
      if (duplicate) {
        if (text(duplicate, "proposal_digest") !== proposalDigest) {
          throw new EdgeAgentError(
            "EDGE_EVENT_COLLISION",
            `Event ${input.proposal.event_id} has conflicting content`,
          )
        }
        return { event: branchEvent(duplicate), duplicate: true }
      }
      const row = this.sessionRow(true)
      const provisionalEvent: EdgeBranchEvent = {
        edge_local_sequence: integer(row, "last_sequence") + 1,
        proposal: structuredClone(input.proposal),
        proposal_digest: proposalDigest,
        branch_digest: delegation(row).lease.delegation_digest,
        recorded_at: input.recorded_at,
        ...(input.execution ? { execution: structuredClone(input.execution) } : {}),
      }
      const identity = dispatchIdentity(provisionalEvent)
      if ((identity?.kind === "started") !== (input.execution !== undefined)) {
        throw new EdgeAgentError(
          "EDGE_DISPATCH_INVALID",
          "Dispatch start and execution scope must be persisted together",
        )
      }
      const existingReason = optionalText(row, "stopped_reason")
      if (existingReason && identity?.kind !== "result") {
        return { stopped: existingReason as EdgeStopReason }
      }
      const delegationValue = delegation(row)
      if (!authorized(delegationValue, provisionalEvent)) {
        throw new EdgeAgentError("EDGE_EVENT_INVALID", "Edge branch event exceeds its delegation")
      }
      const lastSequence = integer(row, "last_sequence")
      const dispatchCount = integer(row, "dispatch_count")
      const openDispatches = integer(
        this.#database
          .prepare(
            "SELECT COUNT(*) AS count FROM edge_dispatch_attempts WHERE status = 'dispatch_started'",
          )
          .get() as Row,
        "count",
      )
      const requiredCapacity =
        identity?.kind === "started" ? 2 : identity?.kind === "result" ? 0 : 1
      if (
        lastSequence + openDispatches + requiredCapacity >
        delegationValue.resource_limits.maximum_branch_events
      ) {
        this.stopInTransaction("branch_limit_reached", input.recorded_at)
        return { stopped: "branch_limit_reached" as const }
      }
      if (
        identity?.kind === "started" &&
        dispatchCount >= delegationValue.resource_limits.maximum_dispatches
      ) {
        this.stopInTransaction("dispatch_limit_reached", input.recorded_at)
        return { stopped: "dispatch_limit_reached" as const }
      }
      if (identity?.kind === "started") {
        const existingAttempt = this.#database
          .prepare("SELECT attempt_id FROM edge_dispatch_attempts WHERE attempt_id = ?")
          .get(identity.attempt_id)
        if (existingAttempt) {
          throw new EdgeAgentError(
            "EDGE_DISPATCH_INVALID",
            `Attempt ${identity.attempt_id} already started under another event`,
          )
        }
      }
      if (identity?.kind === "result") {
        const attempt = this.#database
          .prepare(
            "SELECT command_id, effect_id, status FROM edge_dispatch_attempts WHERE attempt_id = ?",
          )
          .get(identity.attempt_id) as Row | undefined
        if (
          !attempt ||
          text(attempt, "command_id") !== identity.command_id ||
          text(attempt, "effect_id") !== identity.effect_id ||
          text(attempt, "status") !== "dispatch_started"
        ) {
          throw new EdgeAgentError(
            "EDGE_DISPATCH_INVALID",
            `Result does not match open attempt ${identity.attempt_id}`,
          )
        }
      }
      const previousBranchDigest = optionalText(row, "last_branch_digest")
      const edgeLocalSequence = lastSequence + 1
      const branchDigest = sha256Digest({
        edge_local_sequence: edgeLocalSequence,
        previous_branch_digest: previousBranchDigest ?? delegationValue.lease.delegation_digest,
        proposal_digest: proposalDigest,
      })
      const event: EdgeBranchEvent = {
        edge_local_sequence: edgeLocalSequence,
        proposal: structuredClone(input.proposal),
        proposal_digest: proposalDigest,
        ...(previousBranchDigest ? { previous_branch_digest: previousBranchDigest } : {}),
        branch_digest: branchDigest,
        recorded_at: input.recorded_at,
        ...(input.execution ? { execution: structuredClone(input.execution) } : {}),
      }
      if (!Schema.Check(EdgeBranchEventSchema, event)) {
        throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Derived edge event is invalid")
      }
      this.#database
        .prepare(
          `INSERT INTO edge_branch_events(
            edge_local_sequence, event_id, proposal_digest, branch_digest, event_json, recorded_at
          ) VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(
          edgeLocalSequence,
          input.proposal.event_id,
          proposalDigest,
          branchDigest,
          canonicalJson(event as unknown as JsonObject),
          input.recorded_at,
        )
      if (identity?.kind === "started") {
        this.#database
          .prepare(
            `INSERT INTO edge_dispatch_attempts(
              attempt_id, command_id, effect_id, status, started_sequence, result_sequence
            ) VALUES (?, ?, ?, 'dispatch_started', ?, NULL)`,
          )
          .run(identity.attempt_id, identity.command_id, identity.effect_id, edgeLocalSequence)
      }
      if (identity?.kind === "result") {
        this.#database
          .prepare(
            `UPDATE edge_dispatch_attempts
             SET status = 'result_recorded', result_sequence = ?
             WHERE attempt_id = ? AND status = 'dispatch_started'`,
          )
          .run(edgeLocalSequence, identity.attempt_id)
      }
      this.#database
        .prepare(
          `UPDATE edge_session
           SET last_sequence = ?, last_branch_digest = ?, dispatch_count = ?, updated_at = ?
           WHERE singleton = 1`,
        )
        .run(
          edgeLocalSequence,
          branchDigest,
          dispatchCount + (identity?.kind === "started" ? 1 : 0),
          input.recorded_at,
        )
      return { event, duplicate: false }
    })
    if ("stopped" in outcome) {
      throw new EdgeAgentError(
        "EDGE_STOPPED",
        `Edge execution stopped: ${outcome.stopped}`,
        outcome.stopped,
      )
    }
    return outcome
  }

  events(): EdgeBranchEvent[] {
    this.assertOpen()
    return this.#database
      .prepare("SELECT event_json FROM edge_branch_events ORDER BY edge_local_sequence")
      .all()
      .map((row) => branchEvent(row as Row))
  }

  upload(createdAt: string): EdgeReconnectUpload {
    if (!Schema.Check(TimestampSchema, createdAt)) {
      throw new EdgeAgentError("EDGE_EVENT_INVALID", "Reconnect timestamp is invalid")
    }
    this.verifyIntegrity()
    const state = this.state()
    if (!state) {
      throw new EdgeAgentError("EDGE_NOT_ACTIVATED", "Edge store has no delegation")
    }
    return {
      delegation: state.delegation,
      activation: state.activation,
      events: this.events(),
      final_branch_digest: state.last_branch_digest ?? state.delegation.lease.delegation_digest,
      ...(state.stopped_reason ? { stopped_reason: state.stopped_reason } : {}),
      created_at: createdAt,
    }
  }

  close(): void {
    if (!this.#closed) {
      this.#database.close()
      this.#closed = true
    }
  }

  [Symbol.dispose](): void {
    this.close()
  }

  verifyIntegrity(): void {
    this.assertOpen()
    const state = this.state()
    const rows = this.#database
      .prepare(
        `SELECT edge_local_sequence, event_id, proposal_digest, branch_digest, event_json, recorded_at
         FROM edge_branch_events ORDER BY edge_local_sequence`,
      )
      .all() as Row[]
    if (!state) {
      if (rows.length > 0) {
        throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge events exist without a delegation")
      }
      return
    }
    if (
      edgeDelegationDigest(state.delegation) !== state.delegation.lease.delegation_digest ||
      state.delegation.lease.owner_mode !== "edge" ||
      state.activation.lease_id !== state.delegation.lease.lease_id ||
      state.activation.edge_id !== state.delegation.lease.owner_id ||
      state.activation.delegation_digest !== state.delegation.lease.delegation_digest ||
      state.activation.measured_clock_uncertainty_ms >
        state.delegation.lease.allowed_clock_uncertainty_ms ||
      rows.length > state.delegation.resource_limits.maximum_branch_events
    ) {
      throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge session integrity check failed")
    }
    let previous: string | undefined
    let dispatchCount = 0
    const attempts = new Map<
      string,
      {
        command_id: string
        effect_id: string
        status: "dispatch_started" | "result_recorded"
        started_sequence: number
        result_sequence?: number
      }
    >()
    for (const [index, row] of rows.entries()) {
      const event = branchEvent(row)
      const sequence = index + 1
      const proposalDigest = sha256Digest(event.proposal as unknown as JsonObject)
      const expectedBranchDigest = sha256Digest({
        edge_local_sequence: sequence,
        previous_branch_digest: previous ?? state.delegation.lease.delegation_digest,
        proposal_digest: proposalDigest,
      })
      if (
        event.edge_local_sequence !== sequence ||
        event.proposal_digest !== proposalDigest ||
        event.branch_digest !== expectedBranchDigest ||
        integer(row, "edge_local_sequence") !== sequence ||
        text(row, "event_id") !== event.proposal.event_id ||
        text(row, "proposal_digest") !== event.proposal_digest ||
        text(row, "branch_digest") !== event.branch_digest ||
        text(row, "recorded_at") !== event.recorded_at ||
        !authorized(state.delegation, event) ||
        (sequence === 1
          ? event.previous_branch_digest !== undefined
          : event.previous_branch_digest !== previous) ||
        (event.proposal.type === "effect.dispatch_started") !== (event.execution !== undefined)
      ) {
        throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge branch integrity check failed")
      }
      let identity: DispatchIdentity | undefined
      try {
        identity = dispatchIdentity(event)
      } catch {
        throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge dispatch identity is invalid")
      }
      if (identity?.kind === "started") {
        if (attempts.has(identity.attempt_id)) {
          throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge attempt starts more than once")
        }
        attempts.set(identity.attempt_id, {
          command_id: identity.command_id,
          effect_id: identity.effect_id,
          status: "dispatch_started",
          started_sequence: sequence,
        })
        dispatchCount += 1
      }
      if (identity?.kind === "result") {
        const attempt = attempts.get(identity.attempt_id)
        if (
          attempt?.status !== "dispatch_started" ||
          attempt.command_id !== identity.command_id ||
          attempt.effect_id !== identity.effect_id
        ) {
          throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge result has no matching attempt")
        }
        attempt.status = "result_recorded"
        attempt.result_sequence = sequence
      }
      previous = event.branch_digest
    }
    const attemptRows = this.#database
      .prepare(
        `SELECT attempt_id, command_id, effect_id, status, started_sequence, result_sequence
         FROM edge_dispatch_attempts ORDER BY started_sequence`,
      )
      .all() as Row[]
    if (attemptRows.length !== attempts.size) {
      throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge attempt index is incomplete")
    }
    for (const row of attemptRows) {
      const expected = attempts.get(text(row, "attempt_id"))
      const resultSequence = optionalInteger(row, "result_sequence")
      if (
        !expected ||
        text(row, "command_id") !== expected.command_id ||
        text(row, "effect_id") !== expected.effect_id ||
        text(row, "status") !== expected.status ||
        integer(row, "started_sequence") !== expected.started_sequence ||
        resultSequence !== expected.result_sequence
      ) {
        throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge attempt index is inconsistent")
      }
    }
    if (
      state.last_sequence !== rows.length ||
      state.last_branch_digest !== previous ||
      state.dispatch_count !== dispatchCount ||
      dispatchCount > state.delegation.resource_limits.maximum_dispatches
    ) {
      throw new EdgeAgentError(
        "EDGE_STORE_CORRUPT",
        "Edge session counters do not match its branch",
      )
    }
  }

  private migrate(appliedAt: string): void {
    this.transaction(() => {
      this.#database.exec(`
        CREATE TABLE IF NOT EXISTS edge_migrations (
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          checksum TEXT NOT NULL,
          applied_at TEXT NOT NULL
        ) STRICT;
      `)
      const existing = this.#database
        .prepare("SELECT name, checksum FROM edge_migrations WHERE version = 1")
        .get() as Row | undefined
      if (existing) {
        if (
          text(existing, "name") !== migrationName ||
          text(existing, "checksum") !== migrationChecksum
        ) {
          throw new EdgeAgentError(
            "EDGE_MIGRATION_MISMATCH",
            "Applied edge migration does not match this build",
          )
        }
        return
      }
      this.#database.exec(migrationSql)
      this.#database
        .prepare(
          "INSERT INTO edge_migrations(version, name, checksum, applied_at) VALUES (1, ?, ?, ?)",
        )
        .run(migrationName, migrationChecksum, appliedAt)
      this.#database.exec("PRAGMA user_version = 1")
    })
  }

  private transaction<Value>(operation: () => Value): Value {
    this.assertOpen()
    this.#database.exec("BEGIN IMMEDIATE")
    try {
      const value = operation()
      this.#database.exec("COMMIT")
      return value
    } catch (error) {
      try {
        this.#database.exec("ROLLBACK")
      } catch {
        this.#closed = true
        this.#database.close()
      }
      throw error
    }
  }

  private stopInTransaction(reason: EdgeStopReason, at: string): void {
    this.#database
      .prepare(
        "UPDATE edge_session SET status = 'stopped', stopped_reason = ?, updated_at = ? WHERE singleton = 1 AND status = 'active'",
      )
      .run(reason, at)
  }

  private sessionRow(required: true): Row
  private sessionRow(required: false): Row | undefined
  private sessionRow(required: boolean): Row | undefined {
    const row = this.#database.prepare("SELECT * FROM edge_session WHERE singleton = 1").get()
    if (!row && required) {
      throw new EdgeAgentError("EDGE_NOT_ACTIVATED", "Edge store has no delegation")
    }
    return row as Row | undefined
  }

  private assertOpen(): void {
    if (this.#closed) {
      throw new EdgeAgentError("EDGE_STORE_CLOSED", "Edge store is closed")
    }
  }
}
