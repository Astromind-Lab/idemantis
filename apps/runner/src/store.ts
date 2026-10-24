import { randomBytes } from "node:crypto"
import {
  DigestSchema,
  type EffectId,
  type JsonObject,
  type JsonValue,
  type RunId,
  sha256Digest,
} from "@idemantis/contracts"
import { type MissionRuntimeDefinition, reduceMission, replayMission } from "@idemantis/engine"
import {
  JournalError,
  type SqliteJournal,
  type StoredEvent,
  type StoredMission,
} from "@idemantis/journal-sqlite"
import { MissionManifestSchema } from "@idemantis/sdk"
import {
  ApiSchemaVersion,
  type AttentionReason,
  CursorCodec,
  compareMissionPriority,
  type EffectDetail,
  type EffectPage,
  type EffectSummary,
  graphArtifact,
  type MissionDetail,
  type MissionListPage,
  type MissionListQuery,
  MissionListQuerySchema,
  type MissionProjection,
  type MissionSummary,
  type ProtectedValue,
  queueBucketFor,
  type ReplayResult,
  type StreamEnvelope,
  StreamSchemaVersion,
  type TimelineEntry,
  type TimelinePage,
  TransportError,
} from "@idemantis/transport"
import Schema from "typebox/schema"
import { ulid } from "ulid"

const terminalStatuses = new Set(["completed", "cancelled", "compensated", "failed", "unresolved"])
const effectIdField = "effect_id"
const externalReferenceField = "external_reference"

interface DefinitionRegistration {
  runtime: MissionRuntimeDefinition
  subject_reference_keys: string[]
}

export interface RegisteredDefinition {
  definition_id: string
  definition_version: string
  definition_digest: string
  input_schema: JsonObject
}

export interface RunnerStoreOptions {
  journal: SqliteJournal
  cursor_secret?: Uint8Array
  cursor_lifetime_ms?: number
  clock?: () => Date
  id?: () => string
}

export interface RegisterDefinitionOptions {
  subject_reference_keys?: string[]
}

export interface AdmitMissionOptions {
  definition_digest: string
  input: JsonObject
  run_id?: RunId
}

function visible(value: JsonValue): ProtectedValue {
  return { state: "value", classification: "internal", value }
}

function terminal(status: string): boolean {
  return terminalStatuses.has(status)
}

function attentionReason(
  mission: StoredMission,
  effects: ReturnType<SqliteJournal["listEffects"]>,
  ownership: ReturnType<SqliteJournal["getOwnership"]>,
): AttentionReason | undefined {
  if (mission.state.status === "unresolved") {
    return { code: "MISSION_UNRESOLVED", label: "Mission outcome unresolved", severity: "danger" }
  }
  const compensationFailed = effects.find((effect) => effect.status === "compensation_failed")
  if (compensationFailed) {
    return {
      code: "COMPENSATION_FAILED",
      label: `${compensationFailed.operation} compensation failed`,
      severity: "danger",
    }
  }
  const uncertain = effects.find((effect) => effect.status === "uncertain")
  if (uncertain) {
    return {
      code: "EFFECT_UNCERTAIN",
      label: `${uncertain.operation} outcome uncertain`,
      severity: "warning",
    }
  }
  if (ownership.handoff?.status === "blocked") {
    return { code: "HANDOFF_BLOCKED", label: "Ownership handoff blocked", severity: "warning" }
  }
  return undefined
}

function priorityRank(summary: Pick<MissionSummary, "queue_bucket" | "attention_reason">): number {
  if (summary.attention_reason?.severity === "danger") {
    return 100
  }
  if (summary.attention_reason) {
    return 200
  }
  if (summary.queue_bucket === "waiting") {
    return 500
  }
  if (summary.queue_bucket === "active") {
    return 600
  }
  return 700
}

function projection(mission: StoredMission): MissionProjection {
  return {
    run_id: mission.state.run_id,
    status: mission.state.status,
    current_state: mission.state.current_state,
    current_state_instance_id: mission.state.current_state_instance_id,
    current_sequence: mission.state.current_sequence,
    ownership_epoch: mission.state.ownership_epoch,
    active_effect_id: mission.state.active_effect_id,
    terminal_outcome: mission.state.terminal_outcome,
    state_digest: mission.state_digest,
  }
}

function stateEnteredAt(mission: StoredMission, records: StoredEvent[]): string {
  let state: ReturnType<typeof reduceMission>["state"] | null = null
  let enteredAt = mission.created_at
  for (const record of records) {
    const previousInstanceId = state?.current_state_instance_id
    const reduced = reduceMission(mission.runtime, state, record.event)
    if (!state || reduced.state.current_state_instance_id !== previousInstanceId) {
      enteredAt = record.event.recorded_at
    }
    state = reduced.state
  }
  return enteredAt
}

function pagePosition(
  codec: CursorCodec,
  cursor: string | undefined,
  namespace: string,
  revision: number,
  query: JsonObject,
): number {
  if (!cursor) {
    return 0
  }
  return codec.decode(cursor, { namespace, revision, query }).position
}

function streamEventId(value: JsonObject): string {
  return sha256Digest(value)
}

export class RunnerStore {
  readonly #journal: SqliteJournal
  readonly #cursor: CursorCodec
  readonly #clock: () => Date
  readonly #id: () => string
  readonly #definitions = new Map<string, DefinitionRegistration>()

  constructor(options: RunnerStoreOptions) {
    this.#journal = options.journal
    this.#clock = options.clock ?? (() => new Date())
    this.#id = options.id ?? ulid
    this.#cursor = new CursorCodec({
      secret: options.cursor_secret ?? randomBytes(32),
      ...(options.cursor_lifetime_ms !== undefined
        ? { lifetime_ms: options.cursor_lifetime_ms }
        : {}),
      clock: this.#clock,
    })
  }

  get listRevision(): number {
    return this.#journal.missionListRevision()
  }

  missionStreamPosition(runId: RunId, afterSequence: number | undefined, cursor?: string): number {
    const current = this.mission(runId).state.current_sequence
    return this.streamPosition(
      `mission:${runId}`,
      { run_id: runId },
      current,
      afterSequence,
      cursor,
      "Mission sequence",
    )
  }

  operationsStreamPosition(afterRevision: number | undefined, cursor?: string): number {
    return this.streamPosition(
      "operations",
      {},
      this.listRevision,
      afterRevision,
      cursor,
      "Operations revision",
      1,
    )
  }

  missionStreamBatch(runId: RunId, afterSequence: number, limit = 100): StreamEnvelope[] {
    const current = this.mission(runId).state.current_sequence
    this.assertStreamPosition(afterSequence, current, "Mission sequence")
    return this.#journal.listStoredEventsAfter(runId, afterSequence, limit).map((record) => {
      const body = {
        run_id: runId,
        sequence: record.event.sequence,
        event_id: record.event.event_id,
        event_digest: record.proposal_digest,
      }
      return {
        stream_event_id: streamEventId({ kind: "mission", ...body }),
        schema_version: StreamSchemaVersion,
        resume_cursor: this.streamCursor(
          `mission:${runId}`,
          { run_id: runId },
          record.event.sequence,
        ),
        kind: "mission" as const,
        body,
      }
    })
  }

  operationsStreamBatch(afterRevision: number, limit = 1000): StreamEnvelope[] {
    this.assertStreamPosition(afterRevision, this.listRevision, "Operations revision", 1)
    const changes = this.#journal.listMissionChanges(afterRevision, limit)
    const last = changes.at(-1)
    if (!last) {
      return []
    }
    const body = {
      list_revision: last.revision,
      changed_run_ids: [...new Set(changes.map((change) => change.run_id))],
    }
    return [
      {
        stream_event_id: streamEventId({ kind: "operations", ...body }),
        schema_version: StreamSchemaVersion,
        resume_cursor: this.streamCursor("operations", {}, last.revision),
        kind: "operations",
        body,
      },
    ]
  }

  missionStreamHeartbeat(runId: RunId, sequence: number): StreamEnvelope {
    this.assertStreamPosition(
      sequence,
      this.mission(runId).state.current_sequence,
      "Mission sequence",
    )
    return this.heartbeat(`mission:${runId}`, { run_id: runId }, sequence)
  }

  operationsStreamHeartbeat(revision: number): StreamEnvelope {
    this.assertStreamPosition(revision, this.listRevision, "Operations revision", 1)
    return this.heartbeat("operations", {}, revision)
  }

  registerDefinition(
    runtime: MissionRuntimeDefinition,
    options: RegisterDefinitionOptions = {},
  ): void {
    if (
      !Schema.Check(MissionManifestSchema, runtime.manifest) ||
      !Schema.Check(DigestSchema, runtime.definition_digest) ||
      !Schema.Check(DigestSchema, runtime.execution_binding_digest) ||
      sha256Digest(runtime.manifest as unknown as JsonObject) !== runtime.definition_digest
    ) {
      throw new TransportError("INVALID_REQUEST", "Mission definition is invalid", 422)
    }
    const keys = [...new Set(options.subject_reference_keys ?? [])]
    if (keys.length > 16 || keys.some((key) => key.length < 1 || key.length > 64)) {
      throw new TransportError("INVALID_REQUEST", "Subject reference keys are invalid", 422)
    }
    this.#definitions.set(runtime.definition_digest, {
      runtime: structuredClone(runtime),
      subject_reference_keys: keys,
    })
  }

  listDefinitions(): RegisteredDefinition[] {
    return [...this.#definitions.values()]
      .map(({ runtime }) => ({
        definition_id: runtime.manifest.definition_id,
        definition_version: runtime.manifest.definition_version,
        definition_digest: runtime.definition_digest,
        input_schema: structuredClone(runtime.manifest.input_schema as JsonObject),
      }))
      .sort((left, right) => left.definition_id.localeCompare(right.definition_id))
  }

  admitMission(options: AdmitMissionOptions): MissionDetail {
    const registration = this.#definitions.get(options.definition_digest)
    if (!registration) {
      throw new TransportError("INVALID_REQUEST", "Mission definition is not registered", 422)
    }
    if (!Schema.Check(registration.runtime.manifest.input_schema as never, options.input)) {
      throw new TransportError(
        "INVALID_REQUEST",
        "Mission input does not match its definition",
        422,
      )
    }
    const runId = options.run_id ?? (`run_${this.#id()}` as RunId)
    this.#journal.admit(registration.runtime, {
      event_id: `evt_${this.#id()}`,
      run_id: runId,
      type: "mission.admitted",
      schema_version: 1,
      source: { kind: "runner", source_id: "local-runner" },
      ownership_epoch: 1,
      payload: {
        definition_digest: registration.runtime.definition_digest,
        execution_binding_digest: registration.runtime.execution_binding_digest,
        input: structuredClone(options.input),
      },
    })
    return this.missionDetail(runId)
  }

  listMissions(query: MissionListQuery = {}): MissionListPage {
    if (!Schema.Check(MissionListQuerySchema, query)) {
      throw new TransportError("INVALID_REQUEST", "Mission list query is invalid", 422)
    }
    const bucket = query.bucket ?? "needs_attention"
    const search = query.query?.trim().toLocaleLowerCase() ?? ""
    const limit = query.limit ?? 50
    const bindingQuery = { bucket, query: search }
    const snapshot = this.missionSnapshot()
    const position = pagePosition(
      this.#cursor,
      query.cursor,
      "missions",
      snapshot.revision,
      bindingQuery,
    )
    const items = snapshot.missions
      .map((mission) => this.summary(mission))
      .filter((summary) => bucket === "all" || summary.queue_bucket === bucket)
      .filter((summary) => this.matchesSearch(summary, search))
      .sort(compareMissionPriority)
    const selected = items.slice(position, position + limit)
    const nextPosition = position + selected.length
    return {
      schema_version: ApiSchemaVersion,
      list_revision: snapshot.revision,
      filter: bucket,
      query: search,
      total_count: items.length,
      items: selected,
      ...(nextPosition < items.length
        ? {
            next_cursor: this.#cursor.encode(
              { namespace: "missions", revision: snapshot.revision, query: bindingQuery },
              nextPosition,
            ),
          }
        : {}),
    }
  }

  missionDetail(runId: RunId): MissionDetail {
    const mission = this.mission(runId)
    const summary = this.summary(mission)
    const current = mission.runtime.manifest.states[mission.state.current_state]
    const timers = this.#journal
      .listTimers(runId)
      .filter((timer) => timer.status === "scheduled")
      .sort((left, right) => left.due_at.localeCompare(right.due_at))
    return {
      schema_version: ApiSchemaVersion,
      summary,
      input: visible(mission.state.input),
      projection: projection(mission),
      ...(current?.kind === "observation_wait"
        ? { waiting_condition: `Waiting for ${current.observation_type}` }
        : {}),
      ...(timers[0] ? { deadline: timers[0].due_at } : {}),
      definition_digest: mission.runtime.definition_digest,
      execution_binding_digest: mission.runtime.execution_binding_digest,
      valid_interventions: [],
      graph: graphArtifact(mission.runtime.manifest, mission.runtime.definition_digest),
    }
  }

  timeline(runId: RunId, cursor?: string, limit = 100): TimelinePage {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
      throw new TransportError("INVALID_REQUEST", "Timeline limit must be from 1 through 100", 422)
    }
    const mission = this.mission(runId)
    const records = this.#journal.listStoredEvents(runId)
    const query = { run_id: runId }
    const position = pagePosition(
      this.#cursor,
      cursor,
      `timeline:${runId}`,
      mission.state.current_sequence,
      query,
    )
    const entries = this.timelineEntries(mission, records)
    const selected = entries.slice(position, position + limit)
    const nextPosition = position + selected.length
    return {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      items: selected,
      ...(nextPosition < entries.length
        ? {
            next_cursor: this.#cursor.encode(
              {
                namespace: `timeline:${runId}`,
                revision: mission.state.current_sequence,
                query,
              },
              nextPosition,
            ),
          }
        : {}),
    }
  }

  effects(runId: RunId, cursor?: string, limit = 100): EffectPage {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
      throw new TransportError("INVALID_REQUEST", "Effect limit must be from 1 through 100", 422)
    }
    const mission = this.mission(runId)
    const query = { run_id: runId }
    const position = pagePosition(
      this.#cursor,
      cursor,
      `effects:${runId}`,
      mission.state.current_sequence,
      query,
    )
    const items = this.#journal
      .listEffects(runId)
      .map((effect) => this.effectSummary(mission, effect))
    const selected = items.slice(position, position + limit)
    const nextPosition = position + selected.length
    return {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      items: selected,
      ...(nextPosition < items.length
        ? {
            next_cursor: this.#cursor.encode(
              { namespace: `effects:${runId}`, revision: mission.state.current_sequence, query },
              nextPosition,
            ),
          }
        : {}),
    }
  }

  effectDetail(runId: RunId, effectId: EffectId): EffectDetail {
    const mission = this.mission(runId)
    const effect = this.#journal.listEffects(runId).find((item) => item.effect_id === effectId)
    if (!effect) {
      throw new TransportError("RUN_NOT_FOUND", `Effect ${effectId} does not exist`, 404)
    }
    const operation = mission.runtime.manifest.operations[effect.operation]
    const state = mission.runtime.manifest.states[effect.state_name]
    if (!operation) {
      throw new TransportError("INCOMPATIBLE_SCHEMA", "Effect operation is unavailable", 500)
    }
    const relatedEvents = this.#journal
      .listStoredEvents(runId)
      .filter((record) => record.event.payload[effectIdField] === effectId)
    const externalReferences = relatedEvents.flatMap((record) => {
      const reference = record.event.payload[externalReferenceField]
      return reference === undefined ? [] : [visible(reference)]
    })
    return {
      schema_version: ApiSchemaVersion,
      summary: this.effectSummary(mission, effect),
      capabilities: operation.capabilities,
      retry_policy:
        state?.kind === "effect" ? (state.retry as unknown as JsonObject) : { kind: "none" },
      attempts: this.#journal
        .listAttempts(runId)
        .filter((attempt) => attempt.effect_id === effectId)
        .map((attempt) => ({
          attempt_id: attempt.attempt_id,
          status: attempt.status,
          claimed_by: attempt.claimed_by,
          ownership_epoch: attempt.ownership_epoch,
          created_at: attempt.created_at,
          updated_at: attempt.updated_at,
        })),
      evidence: relatedEvents.map((record) => visible(record.event.payload)),
      external_references: externalReferences,
    }
  }

  replay(runId: RunId, sequence: number): ReplayResult {
    const mission = this.mission(runId)
    const records = this.#journal.listStoredEvents(runId)
    if (!Number.isSafeInteger(sequence) || sequence < 1 || sequence > records.length) {
      throw new TransportError("INVALID_REQUEST", "Replay sequence is outside mission history", 422)
    }
    const selected = records.slice(0, sequence)
    const rebuilt = replayMission(
      mission.runtime,
      selected.map((record) => record.event),
    )
    const storedDigest = selected.at(-1)?.state_digest
    if (!storedDigest) {
      throw new TransportError("REPLAY_DIVERGENCE", "Stored replay evidence is unavailable", 500)
    }
    return {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      requested_sequence: sequence,
      projection: projection({
        ...mission,
        state: rebuilt.state,
        state_digest: rebuilt.state_digest,
      }),
      stored_digest: storedDigest,
      rebuilt_digest: rebuilt.state_digest,
      verification: storedDigest === rebuilt.state_digest ? "match" : "diverged",
      events_reduced: selected.length,
      graph: graphArtifact(mission.runtime.manifest, mission.runtime.definition_digest),
    }
  }

  private streamPosition(
    namespace: string,
    query: JsonObject,
    current: number,
    initial: number | undefined,
    cursor: string | undefined,
    label: string,
    minimum = 0,
  ): number {
    if ((initial === undefined) === (cursor === undefined)) {
      throw new TransportError(
        "INVALID_REQUEST",
        "Provide exactly one initial position or resume cursor",
        422,
      )
    }
    const position = cursor
      ? this.#cursor.decode(cursor, { namespace: `stream:${namespace}`, revision: 1, query })
          .position
      : initial
    if (position === undefined) {
      throw new TransportError("INVALID_REQUEST", `${label} is required`, 422)
    }
    this.assertStreamPosition(position, current, label, minimum)
    return position
  }

  private assertStreamPosition(
    position: number,
    current: number,
    label: string,
    minimum = 0,
  ): void {
    if (!Number.isSafeInteger(position) || position < minimum || position > current) {
      throw new TransportError("CURSOR_INVALID", `${label} is outside available history`, 400)
    }
  }

  private streamCursor(namespace: string, query: JsonObject, position: number): string {
    return this.#cursor.encode({ namespace: `stream:${namespace}`, revision: 1, query }, position)
  }

  private heartbeat(namespace: string, query: JsonObject, position: number): StreamEnvelope {
    return {
      stream_event_id: streamEventId({ kind: "heartbeat", namespace, position }),
      schema_version: StreamSchemaVersion,
      resume_cursor: this.streamCursor(namespace, query, position),
      kind: "heartbeat",
    }
  }

  private mission(runId: RunId): StoredMission {
    try {
      return this.#journal.getMission(runId)
    } catch (error) {
      if (error instanceof JournalError && error.code === "RUN_NOT_FOUND") {
        throw new TransportError("RUN_NOT_FOUND", `Mission ${runId} does not exist`, 404)
      }
      throw error
    }
  }

  private missionSnapshot(): { missions: StoredMission[]; revision: number } {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const before = this.#journal.missionListRevision()
      const missions = this.#journal.listMissions()
      const after = this.#journal.missionListRevision()
      if (before === after) {
        return { missions, revision: after }
      }
    }
    throw new TransportError("RUNNER_UNAVAILABLE", "Mission list changed during read", 503)
  }

  private summary(mission: StoredMission): MissionSummary {
    const runId = mission.state.run_id
    const effects = this.#journal.listEffects(runId)
    const ownership = this.#journal.getOwnership(runId)
    const records = this.#journal.listStoredEvents(runId)
    const reason = attentionReason(mission, effects, ownership)
    const current = mission.runtime.manifest.states[mission.state.current_state]
    const queueBucket = queueBucketFor({
      attention: reason !== undefined,
      terminal: terminal(mission.state.status),
      waiting: current?.kind === "observation_wait" || mission.state.status === "waiting",
    })
    const registration = this.#definitions.get(mission.runtime.definition_digest)
    const subjectReferences = (registration?.subject_reference_keys ?? []).flatMap((key) => {
      const value = mission.state.input[key]
      return value === undefined ? [] : [{ kind: key, value: visible(value) }]
    })
    const activeLease = ownership.active_lease?.lease
    const partial: Omit<MissionSummary, "priority_rank"> = {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      definition_id: mission.runtime.manifest.definition_id,
      definition_version: mission.runtime.manifest.definition_version,
      definition_digest: mission.runtime.definition_digest,
      operational_status: mission.state.status,
      ...(terminal(mission.state.status) ? { terminal_status: mission.state.status } : {}),
      current_state: mission.state.current_state,
      state_entered_at: stateEnteredAt(mission, records),
      subject_references: subjectReferences,
      queue_bucket: queueBucket,
      ...(reason ? { attention_reason: reason } : {}),
      owner: activeLease
        ? {
            mode: activeLease.owner_mode,
            owner_id: activeLease.owner_id,
            epoch: activeLease.ownership_epoch,
            connectivity: activeLease.owner_mode === "runner" ? "connected" : "unknown",
          }
        : {
            mode: "runner",
            owner_id: "local-runner",
            epoch: mission.state.ownership_epoch,
            connectivity: "connected",
          },
      current_sequence: mission.state.current_sequence,
      updated_at: mission.updated_at,
    }
    return { ...partial, priority_rank: priorityRank(partial) }
  }

  private matchesSearch(summary: MissionSummary, search: string): boolean {
    if (!search) {
      return true
    }
    const references = summary.subject_references.flatMap((reference) =>
      reference.value.state === "value" ? [String(reference.value.value)] : [],
    )
    return [summary.run_id, summary.definition_id, summary.current_state, ...references].some(
      (value) => value.toLocaleLowerCase().includes(search),
    )
  }

  private timelineEntries(mission: StoredMission, records: StoredEvent[]): TimelineEntry[] {
    let state: ReturnType<typeof reduceMission>["state"] | null = null
    return records.map((record) => {
      const reduced = reduceMission(mission.runtime, state, record.event)
      state = reduced.state
      return {
        sequence: record.event.sequence,
        event_id: record.event.event_id,
        event_digest: record.proposal_digest,
        event_type: record.event.type,
        display_code: `event.${record.event.type}`,
        display_args: { event_type: record.event.type },
        source: record.event.source,
        ...(record.event.causation_id ? { causation_id: record.event.causation_id } : {}),
        ...(record.event.correlation_id ? { correlation_id: record.event.correlation_id } : {}),
        ownership_epoch: record.event.ownership_epoch,
        payload: visible(record.event.payload),
        recorded_at: record.event.recorded_at,
        resulting_state: reduced.state.current_state,
      }
    })
  }

  private effectSummary(
    mission: StoredMission,
    effect: ReturnType<SqliteJournal["listEffects"]>[number],
  ): EffectSummary {
    const operation = mission.runtime.manifest.operations[effect.operation]
    if (!operation) {
      throw new TransportError("INCOMPATIBLE_SCHEMA", "Effect operation is unavailable", 500)
    }
    const related = this.#journal
      .listStoredEvents(mission.state.run_id)
      .filter((record) => record.event.payload[effectIdField] === effect.effect_id)
    return {
      effect_id: effect.effect_id,
      state_instance_id: effect.state_instance_id,
      state_name: effect.state_name,
      effect_slot: effect.effect_slot,
      operation: effect.operation,
      consequence: operation.consequence,
      status: effect.status,
      issue_count: effect.issue_count,
      ownership_epoch: effect.ownership_epoch,
      ...(related.at(-1) ? { last_evidence: visible(related.at(-1)?.event.payload ?? null) } : {}),
    }
  }
}
