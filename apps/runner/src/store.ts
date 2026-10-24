import { randomBytes } from "node:crypto"
import {
  DigestSchema,
  canonicalJson,
  type ClientRequestId,
  type EffectId,
  type JsonObject,
  type JsonValue,
  type RunId,
  type SourceId,
  sha256Digest,
} from "@idemantis/contracts"
import {
  evaluateExpression,
  type MissionRuntimeDefinition,
  reduceMission,
  replayMission,
} from "@idemantis/engine"
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
  type DiscardInterventionRequest,
  DiscardInterventionRequestSchema,
  type EffectDetail,
  type EffectPage,
  type EffectSummary,
  graphArtifact,
  type InterventionDescriptor,
  type InterventionReceipt,
  type InterventionReceiptPage,
  type MissionContext,
  type MissionContextEffect,
  type MissionDetail,
  type MissionNextStep,
  type MissionListPage,
  type MissionListQuery,
  MissionListQuerySchema,
  type MissionProjection,
  type MissionSummary,
  type ObservationReceipt,
  type PrepareInterventionRequest,
  PrepareInterventionRequestSchema,
  type PreparedIntervention,
  type PreparedInterventionPage,
  type ProtectedValue,
  queueBucketFor,
  type RecordObservationRequest,
  RecordObservationRequestSchema,
  type ReplayResult,
  type StreamEnvelope,
  StreamSchemaVersion,
  type TimelineEntry,
  type TimelinePage,
  TransportError,
  type SubmitInterventionRequest,
  SubmitInterventionRequestSchema,
  validateInterventionInput,
  validateInterventionSchema,
} from "@idemantis/transport"
import Schema from "typebox/schema"
import { ulid } from "ulid"

const donePhases = new Set(["settled", "observed", "cancelled", "compensated"])
const uncertainPhases = new Set(["uncertain", "fenced"])
const failedPhases = new Set(["rejected", "failed", "compensation_failed"])
const resultEventTypes = new Set([
  "effect.issue_result_recorded",
  "effect.reconciliation_result_recorded",
])

function effectPhase(status: string): MissionContextEffect["phase"] {
  if (donePhases.has(status)) {
    return "done"
  }
  if (uncertainPhases.has(status)) {
    return "uncertain"
  }
  if (failedPhases.has(status)) {
    return "failed"
  }
  return "in_flight"
}

const terminalStatuses = new Set(["completed", "cancelled", "compensated", "failed", "unresolved"])
const effectIdField = "effect_id"
const externalReferenceField = "external_reference"

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
  intervention_lifetime_ms?: number
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
  const current = mission.runtime.manifest.states[mission.state.current_state]
  if (current?.kind === "operator_wait") {
    return {
      code: "OPERATOR_DECISION_REQUIRED",
      label: current.reason,
      severity: "warning",
    }
  }
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
  readonly #interventionLifetimeMs: number

  constructor(options: RunnerStoreOptions) {
    this.#journal = options.journal
    this.#clock = options.clock ?? (() => new Date())
    this.#id = options.id ?? ulid
    this.#interventionLifetimeMs = options.intervention_lifetime_ms ?? 15 * 60_000
    if (!Number.isSafeInteger(this.#interventionLifetimeMs) || this.#interventionLifetimeMs < 1) {
      throw new RangeError("Intervention lifetime must be a positive safe integer")
    }
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

  receiptStreamPosition(
    runId: RunId,
    actorId: SourceId,
    afterRevision: number | undefined,
    cursor?: string,
  ): number {
    this.mission(runId)
    return this.streamPosition(
      `receipts:${runId}:${actorId}`,
      { run_id: runId, actor_id: actorId },
      this.#journal.interventionReceiptRevision(runId, actorId),
      afterRevision,
      cursor,
      "Receipt revision",
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

  receiptStreamBatch(
    runId: RunId,
    actorId: SourceId,
    afterRevision: number,
    limit = 100,
  ): StreamEnvelope[] {
    const current = this.#journal.interventionReceiptRevision(runId, actorId)
    this.assertStreamPosition(afterRevision, current, "Receipt revision")
    return this.#journal
      .listReceiptUpdatesAfter(runId, actorId, afterRevision, limit)
      .map((update) => {
        const receipt: InterventionReceipt = {
          schema_version: ApiSchemaVersion,
          ...update.receipt,
        }
        const body = {
          run_id: runId,
          client_request_id: update.client_request_id,
          receipt_revision: update.revision,
          receipt,
        }
        return {
          stream_event_id: streamEventId({ kind: "receipt", ...body }),
          schema_version: StreamSchemaVersion,
          resume_cursor: this.streamCursor(
            `receipts:${runId}:${actorId}`,
            { run_id: runId, actor_id: actorId },
            update.revision,
          ),
          kind: "receipt" as const,
          body,
        }
      })
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

  receiptStreamHeartbeat(runId: RunId, actorId: SourceId, revision: number): StreamEnvelope {
    this.assertStreamPosition(
      revision,
      this.#journal.interventionReceiptRevision(runId, actorId),
      "Receipt revision",
    )
    return this.heartbeat(
      `receipts:${runId}:${actorId}`,
      { run_id: runId, actor_id: actorId },
      revision,
    )
  }

  assertDefinition(runtime: MissionRuntimeDefinition): void {
    if (
      !Schema.Check(MissionManifestSchema, runtime.manifest) ||
      !Schema.Check(DigestSchema, runtime.definition_digest) ||
      !Schema.Check(DigestSchema, runtime.execution_binding_digest) ||
      sha256Digest(runtime.manifest as unknown as JsonObject) !== runtime.definition_digest
    ) {
      throw new TransportError("INVALID_REQUEST", "Mission definition is invalid", 422)
    }
    for (const state of Object.values(runtime.manifest.states)) {
      if (state.kind !== "operator_wait") {
        continue
      }
      for (const action of state.actions) {
        validateInterventionSchema(action.input_schema)
      }
    }
  }

  registerDefinition(
    runtime: MissionRuntimeDefinition,
    options: RegisterDefinitionOptions = {},
  ): void {
    this.assertDefinition(runtime)
    const keys = [...new Set(options.subject_reference_keys ?? [])]
    if (keys.length > 16 || keys.some((key) => key.length < 1 || key.length > 64)) {
      throw new TransportError("INVALID_REQUEST", "Subject reference keys are invalid", 422)
    }
    try {
      this.#journal.registerDefinition(runtime, { subject_reference_keys: keys })
    } catch (error) {
      if (error instanceof JournalError && error.code === "DEFINITION_INVALID") {
        throw new TransportError("INVALID_REQUEST", "Mission definition is invalid", 422)
      }
      throw error
    }
  }

  definition(definitionDigest: string): MissionRuntimeDefinition | undefined {
    return this.#journal.getDefinition(definitionDigest)?.runtime
  }

  listDefinitions(): RegisteredDefinition[] {
    return this.#journal
      .listDefinitions()
      .map(({ runtime }) => ({
        definition_id: runtime.manifest.definition_id,
        definition_version: runtime.manifest.definition_version,
        definition_digest: runtime.definition_digest,
        input_schema: structuredClone(runtime.manifest.input_schema as JsonObject),
      }))
      .sort((left, right) => left.definition_id.localeCompare(right.definition_id))
  }

  admitMission(options: AdmitMissionOptions): MissionDetail {
    const registration = this.#journal.getDefinition(options.definition_digest)
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

  recordObservation(
    runId: RunId,
    request: RecordObservationRequest,
    sourceId: SourceId,
  ): ObservationReceipt {
    if (!Schema.Check(RecordObservationRequestSchema, request)) {
      throw new TransportError("INVALID_REQUEST", "Observation request is invalid", 422)
    }
    const mission = this.mission(runId)
    const content = {
      observation_type: request.observation_type,
      payload: request.payload,
      effect_id: request.effect_id ?? null,
      subject: request.subject ?? null,
      observed_at: request.observed_at ?? null,
    }
    const prior = this.#journal.listStoredEvents(runId).find(({ event }) => {
      const { observation_id: observationId } = event.payload
      return event.type === "observation.recorded" && observationId === request.observation_id
    })
    if (prior) {
      const { observation_type, payload, effect_id, subject } = prior.event.payload
      const recorded = {
        observation_type,
        payload,
        effect_id: effect_id ?? null,
        subject: subject ?? null,
        observed_at: prior.event.observed_at ?? null,
      }
      if (canonicalJson(recorded as JsonObject) !== canonicalJson(content as JsonObject)) {
        throw new TransportError(
          "CLIENT_REQUEST_ID_COLLISION",
          "Observation ID is bound to different observation content",
          409,
        )
      }
      return {
        schema_version: ApiSchemaVersion,
        run_id: runId,
        observation_id: request.observation_id,
        event_id: prior.event.event_id,
        sequence: prior.event.sequence,
        disposition: "duplicate",
      }
    }
    if (mission.state.terminal_outcome) {
      throw new TransportError("ACTION_FORBIDDEN", "Mission has already finished", 409)
    }
    const appended = this.#journal.append({
      event_id: `evt_${this.#id()}`,
      run_id: runId,
      type: "observation.recorded",
      schema_version: 1,
      source: { kind: "system", source_id: sourceId },
      ownership_epoch: mission.state.ownership_epoch,
      ...(request.observed_at ? { observed_at: request.observed_at } : {}),
      correlation_id: request.effect_id ?? runId,
      payload: {
        observation_id: request.observation_id,
        observation_type: request.observation_type,
        ...(request.effect_id ? { effect_id: request.effect_id } : {}),
        ...(request.subject !== undefined ? { subject: structuredClone(request.subject) } : {}),
        payload: structuredClone(request.payload),
      },
    })
    return {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      observation_id: request.observation_id,
      event_id: appended.acceptance.event_id,
      sequence: appended.acceptance.sequence,
      disposition: "recorded",
    }
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
      valid_interventions: this.interventionDescriptors(mission),
      graph: graphArtifact(mission.runtime.manifest, mission.runtime.definition_digest),
    }
  }

  missionContext(runId: RunId): MissionContext {
    const mission = this.mission(runId)
    const records = this.#journal.listStoredEvents(runId)
    const results = new Map<string, JsonObject>()
    const firstSeen = new Map<string, number>()
    for (const { event } of records) {
      const { effect_id: effectId } = event.payload
      if (typeof effectId === "string" && !firstSeen.has(effectId)) {
        firstSeen.set(effectId, event.sequence)
      }
      if (resultEventTypes.has(event.type) && typeof effectId === "string") {
        results.set(effectId, event.payload)
      }
    }
    const ordered = this.#journal
      .listEffects(runId)
      .sort(
        (left, right) =>
          (firstSeen.get(left.effect_id) ?? Number.MAX_SAFE_INTEGER) -
            (firstSeen.get(right.effect_id) ?? Number.MAX_SAFE_INTEGER) ||
          left.effect_id.localeCompare(right.effect_id),
      )
    const effects = ordered.map((effect): MissionContextEffect => {
      const recorded = results.get(effect.effect_id) ?? {}
      const { output, external_reference: reference, evidence } = recorded
      return {
        effect_id: effect.effect_id,
        state_name: effect.state_name,
        operation: effect.operation,
        status: effect.status,
        phase: effectPhase(effect.status),
        issue_count: effect.issue_count,
        ...(output === undefined ? {} : { output: structuredClone(output) }),
        ...(typeof reference === "string" ? { external_reference: reference } : {}),
        ...(evidence && typeof evidence === "object" && !Array.isArray(evidence)
          ? { evidence: structuredClone(evidence) }
          : {}),
      }
    })
    const current = mission.runtime.manifest.states[mission.state.current_state]
    if (!current) {
      throw new TransportError("INCOMPATIBLE_SCHEMA", "Mission state is unavailable", 500)
    }
    const summary = this.summary(mission)
    const deadline = this.#journal
      .listTimers(runId)
      .filter((timer) => timer.status === "scheduled")
      .map((timer) => timer.due_at)
      .sort()[0]
    return {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      definition_id: mission.runtime.manifest.definition_id,
      definition_version: mission.runtime.manifest.definition_version,
      status: mission.state.status,
      current_state: mission.state.current_state,
      state_kind: current.kind,
      terminal_outcome: mission.state.terminal_outcome,
      sequence: mission.state.current_sequence,
      state_digest: mission.state_digest,
      input: structuredClone(mission.state.input),
      ...(summary.attention_reason ? { attention_reason: summary.attention_reason } : {}),
      effects,
      next: this.nextStep(mission, deadline),
      recent_events: records.slice(-20).map(({ event }) => ({
        sequence: event.sequence,
        event_type: event.type,
        recorded_at: event.recorded_at,
      })),
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

  prepareIntervention(
    runId: RunId,
    actorId: SourceId,
    request: PrepareInterventionRequest,
  ): PreparedIntervention {
    if (!Schema.Check(PrepareInterventionRequestSchema, request)) {
      throw new TransportError("INVALID_REQUEST", "Intervention request is invalid", 422)
    }
    try {
      const existing = this.#journal.getPreparedIntervention(runId, request.client_request_id)
      if (
        existing.actor_id !== actorId ||
        existing.expected_sequence !== request.expected_sequence ||
        existing.action_code !== request.action_code ||
        existing.target !== request.target ||
        canonicalJson(existing.normalized_input) !== canonicalJson(request.input) ||
        existing.reason !== request.reason
      ) {
        throw new TransportError(
          "CLIENT_REQUEST_ID_COLLISION",
          "Client request ID is bound to different intervention content",
          409,
        )
      }
      return this.preparedIntervention(existing)
    } catch (error) {
      if (error instanceof TransportError) {
        throw error
      }
      if (!(error instanceof JournalError) || error.code !== "INTERVENTION_INVALID") {
        throw this.interventionError(error, runId)
      }
    }
    const mission = this.mission(runId)
    if (request.expected_sequence !== mission.state.current_sequence) {
      throw new TransportError(
        "STALE_SEQUENCE",
        "Mission changed before intervention review",
        409,
        {
          expected_sequence: request.expected_sequence,
          current_sequence: mission.state.current_sequence,
        },
      )
    }
    const descriptor = this.interventionDescriptors(mission).find(
      (candidate) =>
        candidate.action_code === request.action_code && candidate.target === request.target,
    )
    if (!descriptor) {
      throw new TransportError(
        "ACTION_FORBIDDEN",
        "Intervention is not available for the current mission state",
        403,
      )
    }
    validateInterventionInput(descriptor.input_schema, request.input)
    if (descriptor.reason_required && !request.reason) {
      throw new TransportError("INVALID_REQUEST", "Intervention reason is required", 422)
    }
    const event = {
      event_id: `evt_${this.#id()}` as const,
      run_id: runId,
      type: "operator.intervention_submitted" as const,
      schema_version: 1,
      source: { kind: "operator" as const, source_id: actorId },
      ownership_epoch: mission.state.ownership_epoch,
      payload: {
        client_request_id: request.client_request_id,
        expected_sequence: request.expected_sequence,
        action_code: request.action_code,
        target: request.target,
        input: structuredClone(request.input),
        ...(request.reason ? { reason: request.reason } : {}),
      },
    }
    try {
      return this.preparedIntervention(
        this.#journal.prepareIntervention(
          event,
          new Date(this.#clock().getTime() + this.#interventionLifetimeMs).toISOString(),
        ),
      )
    } catch (error) {
      throw this.interventionError(error, runId, request.expected_sequence)
    }
  }

  submitIntervention(
    runId: RunId,
    actorId: SourceId,
    request: SubmitInterventionRequest,
  ): InterventionReceipt {
    if (!Schema.Check(SubmitInterventionRequestSchema, request)) {
      throw new TransportError("INVALID_REQUEST", "Intervention submission is invalid", 422)
    }
    this.assertInterventionActor(runId, request.client_request_id, actorId)
    try {
      const result = this.#journal.submitIntervention(
        runId,
        request.client_request_id,
        request.proposal_digest,
      )
      if (result.status === "expired") {
        throw new TransportError("INVALID_REQUEST", "Prepared intervention has expired", 409)
      }
      return { schema_version: ApiSchemaVersion, ...result.receipt }
    } catch (error) {
      if (error instanceof TransportError) {
        throw error
      }
      throw this.interventionError(error, runId)
    }
  }

  discardIntervention(
    runId: RunId,
    actorId: SourceId,
    request: DiscardInterventionRequest,
  ): PreparedIntervention {
    if (!Schema.Check(DiscardInterventionRequestSchema, request)) {
      throw new TransportError("INVALID_REQUEST", "Intervention discard request is invalid", 422)
    }
    this.assertInterventionActor(runId, request.client_request_id, actorId)
    try {
      return this.preparedIntervention(
        this.#journal.discardIntervention(
          runId,
          request.client_request_id,
          request.proposal_digest,
        ),
      )
    } catch (error) {
      throw this.interventionError(error, runId)
    }
  }

  preparedInterventionById(
    runId: RunId,
    actorId: SourceId,
    clientRequestId: ClientRequestId,
  ): PreparedIntervention {
    this.assertInterventionActor(runId, clientRequestId, actorId)
    try {
      return this.preparedIntervention(
        this.#journal.getPreparedIntervention(runId, clientRequestId),
      )
    } catch (error) {
      throw this.interventionError(error, runId)
    }
  }

  interventionReceiptById(
    runId: RunId,
    actorId: SourceId,
    clientRequestId: ClientRequestId,
  ): InterventionReceipt {
    this.assertInterventionActor(runId, clientRequestId, actorId)
    try {
      return {
        schema_version: ApiSchemaVersion,
        ...this.#journal.getInterventionReceipt(runId, clientRequestId),
      }
    } catch (error) {
      throw this.interventionError(error, runId)
    }
  }

  interventionActivity(
    runId: RunId,
    actorId: SourceId,
    cursor?: string,
    limit = 100,
  ): PreparedInterventionPage {
    this.assertPageLimit(limit)
    this.mission(runId)
    const revision = this.#journal.interventionIntentRevision(runId, actorId)
    const query = { run_id: runId, actor_id: actorId }
    const position = pagePosition(
      this.#cursor,
      cursor,
      `intervention-activity:${runId}:${actorId}`,
      revision,
      query,
    )
    const cutoff = this.#clock().getTime() - 24 * 60 * 60_000
    const items = this.#journal
      .listPreparedInterventions(runId, actorId, revision)
      .filter((record) => Date.parse(record.prepared_at) >= cutoff)
      .map((record) => this.preparedIntervention(record))
    const selected = items.slice(position, position + limit)
    const nextPosition = position + selected.length
    return {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      activity_revision: revision,
      items: selected,
      ...(nextPosition < items.length
        ? {
            next_cursor: this.#cursor.encode(
              {
                namespace: `intervention-activity:${runId}:${actorId}`,
                revision,
                query,
              },
              nextPosition,
            ),
          }
        : {}),
    }
  }

  interventionReceipts(
    runId: RunId,
    actorId: SourceId,
    cursor?: string,
    limit = 100,
  ): InterventionReceiptPage {
    this.assertPageLimit(limit)
    this.mission(runId)
    const revision = this.#journal.interventionReceiptRevision(runId, actorId)
    const query = { run_id: runId, actor_id: actorId }
    const position = pagePosition(
      this.#cursor,
      cursor,
      `intervention-receipts:${runId}:${actorId}`,
      revision,
      query,
    )
    const items = this.#journal
      .listInterventionReceipts(runId, actorId, revision)
      .map((receipt) => ({ schema_version: ApiSchemaVersion, ...receipt }))
    const selected = items.slice(position, position + limit)
    const nextPosition = position + selected.length
    return {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      receipt_revision: revision,
      items: selected,
      ...(nextPosition < items.length
        ? {
            next_cursor: this.#cursor.encode(
              {
                namespace: `intervention-receipts:${runId}:${actorId}`,
                revision,
                query,
              },
              nextPosition,
            ),
          }
        : {}),
    }
  }

  private preparedIntervention(
    record: ReturnType<SqliteJournal["getPreparedIntervention"]>,
  ): PreparedIntervention {
    return {
      schema_version: ApiSchemaVersion,
      client_request_id: record.client_request_id,
      actor_id: record.actor_id,
      run_id: record.run_id,
      expected_sequence: record.expected_sequence,
      action_code: record.action_code,
      target: record.target,
      normalized_input: structuredClone(record.normalized_input),
      ...(record.reason ? { reason: record.reason } : {}),
      proposal_digest: record.proposal_digest,
      prepared_at: record.prepared_at,
      expires_at: record.expires_at,
      state: record.state,
      compatibility: record.compatibility,
    }
  }

  private assertInterventionActor(
    runId: RunId,
    clientRequestId: ClientRequestId,
    actorId: SourceId,
  ): void {
    try {
      const prepared = this.#journal.getPreparedIntervention(runId, clientRequestId)
      if (prepared.actor_id !== actorId) {
        throw new TransportError("ACTION_FORBIDDEN", "Intervention is not available", 403)
      }
    } catch (error) {
      if (error instanceof TransportError) {
        throw error
      }
      throw this.interventionError(error, runId)
    }
  }

  private assertPageLimit(limit: number): void {
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100) {
      throw new TransportError("INVALID_REQUEST", "Page limit must be from 1 through 100", 422)
    }
  }

  private interventionError(
    error: unknown,
    runId: RunId,
    expectedSequence?: number,
  ): TransportError {
    if (error instanceof TransportError) {
      return error
    }
    if (error instanceof JournalError) {
      if (error.code === "RUN_NOT_FOUND") {
        return new TransportError("RUN_NOT_FOUND", `Mission ${runId} does not exist`, 404)
      }
      if (error.code === "INTERVENTION_COLLISION" || error.code === "PENDING_INPUT_COLLISION") {
        return new TransportError(
          "CLIENT_REQUEST_ID_COLLISION",
          "Client request ID is bound to different intervention content",
          409,
        )
      }
      if (error.code === "INTERVENTION_INVALID" && expectedSequence !== undefined) {
        const currentSequence = this.mission(runId).state.current_sequence
        if (currentSequence !== expectedSequence) {
          return new TransportError(
            "STALE_SEQUENCE",
            "Mission changed before intervention review",
            409,
            {
              expected_sequence: expectedSequence,
              current_sequence: currentSequence,
            },
          )
        }
      }
      if (error.code === "INTERVENTION_INVALID" || error.code === "INTERVENTION_STATE_INVALID") {
        return new TransportError("INVALID_REQUEST", error.message, 409)
      }
    }
    return new TransportError("RUNNER_UNAVAILABLE", "Runner could not persist intervention", 500)
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

  private nextStep(mission: StoredMission, deadline: string | undefined): MissionNextStep {
    const current = mission.runtime.manifest.states[mission.state.current_state]
    const activeEffect = mission.state.active_effect_id ?? undefined
    if (mission.state.terminal_outcome || !current || current.kind === "final") {
      return {
        actor: "none",
        summary: `Mission finished as ${mission.state.terminal_outcome ?? mission.state.status}`,
      }
    }
    if (current.kind === "observation_wait") {
      const subject =
        current.correlation.kind === "subject"
          ? evaluateExpression(current.correlation.subject, mission.state.input)
          : undefined
      return {
        actor: "observation",
        summary: `Waiting for a ${current.observation_type} observation`,
        observation_type: current.observation_type,
        ...(activeEffect ? { effect_id: activeEffect } : {}),
        ...(subject === undefined ? {} : { subject }),
        ...(deadline ? { deadline } : {}),
      }
    }
    if (current.kind === "operator_wait") {
      return {
        actor: "operator",
        summary: current.reason,
        actions: current.actions.map((action) => action.action_code),
        ...(deadline ? { deadline } : {}),
      }
    }
    return {
      actor: "runner",
      summary:
        current.kind === "reconcile"
          ? `Runner is reconciling ${current.operation} before deciding the next step`
          : `Runner is executing ${current.operation}`,
      operation: current.operation,
      ...(activeEffect ? { effect_id: activeEffect } : {}),
      ...(deadline ? { deadline } : {}),
    }
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
      waiting:
        current?.kind === "observation_wait" ||
        current?.kind === "operator_wait" ||
        mission.state.status === "waiting",
    })
    const registration = this.#journal.getDefinition(mission.runtime.definition_digest)
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

  private interventionDescriptors(mission: StoredMission): InterventionDescriptor[] {
    const state = mission.runtime.manifest.states[mission.state.current_state]
    if (state?.kind !== "operator_wait") {
      return []
    }
    return state.actions.map((action) => ({
      action_code: action.action_code,
      target: `mission:${mission.state.run_id}`,
      consequence: action.consequence,
      confirmation_required: action.confirmation_required,
      form_schema_version: 1,
      input_schema: structuredClone(action.input_schema),
      presentation_hints: structuredClone(action.presentation_hints),
      evidence_required: action.evidence_required,
      reason_required: action.reason_required,
      expected_outcome_label: action.expected_outcome_label,
    }))
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
