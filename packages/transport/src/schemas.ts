import {
  AttemptIdSchema,
  ClientRequestIdSchema,
  ConsequenceLevelSchema,
  DigestSchema,
  EffectIdSchema,
  EffectStatusSchema,
  EventIdSchema,
  EventSourceSchema,
  EventTypeSchema,
  JsonObjectSchema,
  JsonValueSchema,
  MissionStatusSchema,
  OperationCapabilitiesSchema,
  RunIdSchema,
  SourceIdSchema,
  StateInstanceIdSchema,
  TimestampSchema,
} from "@idemantis/contracts"
import Type from "typebox"

export const ApiSchemaVersion = 1 as const
export const StreamSchemaVersion = 1 as const

const StrictStringSchema = Type.String({ minLength: 1, maxLength: 512 })
const CursorSchema = Type.String({ minLength: 16, maxLength: 4096 })
const RevisionSchema = Type.Integer({ minimum: 1 })

export const TransportErrorCodeSchema = Type.Union([
  Type.Literal("RUNNER_UNAVAILABLE"),
  Type.Literal("RUN_NOT_FOUND"),
  Type.Literal("ACTION_FORBIDDEN"),
  Type.Literal("STALE_SEQUENCE"),
  Type.Literal("INCOMPATIBLE_SCHEMA"),
  Type.Literal("CURSOR_EXPIRED"),
  Type.Literal("CURSOR_INVALID"),
  Type.Literal("CLIENT_REQUEST_ID_COLLISION"),
  Type.Literal("REQUEST_OUTCOME_UNKNOWN"),
  Type.Literal("REDACTED_DATA"),
  Type.Literal("REPLAY_DIVERGENCE"),
  Type.Literal("SESSION_EXPIRED"),
  Type.Literal("DEVELOPMENT_CAPABILITY_DISABLED"),
  Type.Literal("INVALID_REQUEST"),
])

export const ApiErrorSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    error: Type.Object(
      {
        code: TransportErrorCodeSchema,
        message: Type.String({ minLength: 1, maxLength: 1024 }),
        request_id: Type.String({ minLength: 16, maxLength: 128 }),
        details: Type.Optional(JsonObjectSchema),
      },
      { additionalProperties: false },
    ),
  },
  { additionalProperties: false },
)

export const DataClassificationSchema = Type.Union([
  Type.Literal("public"),
  Type.Literal("internal"),
  Type.Literal("sensitive"),
  Type.Literal("secret"),
])

export const VisibleValueSchema = Type.Object(
  {
    state: Type.Literal("value"),
    classification: DataClassificationSchema,
    value: JsonValueSchema,
  },
  { additionalProperties: false },
)

export const RedactedMarkerSchema = Type.Object(
  {
    state: Type.Literal("redacted"),
    classification: DataClassificationSchema,
    reason: Type.String({ minLength: 1, maxLength: 256 }),
  },
  { additionalProperties: false },
)

export const UnavailableMarkerSchema = Type.Object(
  {
    state: Type.Literal("unavailable"),
    reason: Type.String({ minLength: 1, maxLength: 256 }),
  },
  { additionalProperties: false },
)

export const ProtectedValueSchema = Type.Union([
  VisibleValueSchema,
  RedactedMarkerSchema,
  UnavailableMarkerSchema,
])

export const QueueBucketSchema = Type.Union([
  Type.Literal("needs_attention"),
  Type.Literal("active"),
  Type.Literal("waiting"),
  Type.Literal("completed"),
])

export const QueueFilterSchema = Type.Union([QueueBucketSchema, Type.Literal("all")])

export const SeveritySchema = Type.Union([
  Type.Literal("information"),
  Type.Literal("warning"),
  Type.Literal("danger"),
])

export const AttentionReasonSchema = Type.Object(
  {
    code: Type.String({ minLength: 1, maxLength: 128 }),
    label: Type.String({ minLength: 1, maxLength: 256 }),
    severity: SeveritySchema,
  },
  { additionalProperties: false },
)

export const OwnerProjectionSchema = Type.Object(
  {
    mode: Type.Union([Type.Literal("runner"), Type.Literal("edge"), Type.Literal("unowned")]),
    owner_id: Type.Optional(SourceIdSchema),
    epoch: Type.Integer({ minimum: 0 }),
    connectivity: Type.Union([
      Type.Literal("connected"),
      Type.Literal("disconnected"),
      Type.Literal("unknown"),
    ]),
  },
  { additionalProperties: false },
)

export const SubjectReferenceSchema = Type.Object(
  {
    kind: Type.String({ minLength: 1, maxLength: 64 }),
    value: ProtectedValueSchema,
  },
  { additionalProperties: false },
)

export const MissionSummarySchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    run_id: RunIdSchema,
    definition_id: Type.String({ minLength: 1, maxLength: 128 }),
    definition_version: Type.String({ minLength: 1, maxLength: 128 }),
    definition_digest: DigestSchema,
    operational_status: MissionStatusSchema,
    terminal_status: Type.Optional(MissionStatusSchema),
    current_state: Type.String({ minLength: 1, maxLength: 128 }),
    state_entered_at: TimestampSchema,
    subject_references: Type.Array(SubjectReferenceSchema, { maxItems: 16 }),
    queue_bucket: QueueBucketSchema,
    attention_reason: Type.Optional(AttentionReasonSchema),
    priority_rank: Type.Integer({ minimum: 0 }),
    owner: OwnerProjectionSchema,
    current_sequence: Type.Integer({ minimum: 1 }),
    updated_at: TimestampSchema,
  },
  { additionalProperties: false },
)

export const MissionListQuerySchema = Type.Object(
  {
    bucket: Type.Optional(QueueFilterSchema),
    query: Type.Optional(Type.String({ maxLength: 256 })),
    cursor: Type.Optional(CursorSchema),
    limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 50 })),
  },
  { additionalProperties: false },
)

export const MissionListPageSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    list_revision: RevisionSchema,
    filter: QueueFilterSchema,
    query: Type.String({ maxLength: 256 }),
    total_count: Type.Integer({ minimum: 0 }),
    items: Type.Array(MissionSummarySchema, { maxItems: 50 }),
    next_cursor: Type.Optional(CursorSchema),
  },
  { additionalProperties: false },
)

export const GraphOperationSchema = Type.Object(
  {
    key: Type.String({ minLength: 1, maxLength: 128 }),
    adapter: Type.String({ minLength: 1, maxLength: 128 }),
    operation: Type.String({ minLength: 1, maxLength: 128 }),
    consequence: ConsequenceLevelSchema,
  },
  { additionalProperties: false },
)

export const GraphNodeSchema = Type.Object(
  {
    state_id: Type.String({ minLength: 1, maxLength: 128 }),
    kind: Type.Union([
      Type.Literal("effect"),
      Type.Literal("observation_wait"),
      Type.Literal("reconcile"),
      Type.Literal("final"),
    ]),
    display_code: Type.String({ minLength: 1, maxLength: 128 }),
    display_args: JsonObjectSchema,
    operation: Type.Optional(GraphOperationSchema),
    terminal_outcome: Type.Optional(
      Type.Union([
        Type.Literal("completed"),
        Type.Literal("cancelled"),
        Type.Literal("failed"),
        Type.Literal("unresolved"),
        Type.Literal("compensated"),
      ]),
    ),
  },
  { additionalProperties: false },
)

export const GraphTransitionSchema = Type.Object(
  {
    transition_id: Type.String({ pattern: "^[a-f0-9]{24}$" }),
    source_state_id: Type.String({ minLength: 1, maxLength: 128 }),
    target_state_id: Type.String({ minLength: 1, maxLength: 128 }),
    display_code: Type.String({ minLength: 1, maxLength: 128 }),
    display_args: JsonObjectSchema,
    route_kind: Type.Union([
      Type.Literal("normal"),
      Type.Literal("recovery"),
      Type.Literal("cancellation"),
      Type.Literal("compensation"),
    ]),
  },
  { additionalProperties: false },
)

export const GraphArtifactSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    definition_digest: DigestSchema,
    graph_digest: DigestSchema,
    entry_state_id: Type.String({ minLength: 1, maxLength: 128 }),
    nodes: Type.Array(GraphNodeSchema, { maxItems: 4096 }),
    transitions: Type.Array(GraphTransitionSchema, { maxItems: 16384 }),
  },
  { additionalProperties: false },
)

export const MissionProjectionSchema = Type.Object(
  {
    run_id: RunIdSchema,
    status: MissionStatusSchema,
    current_state: Type.String({ minLength: 1, maxLength: 128 }),
    current_state_instance_id: StateInstanceIdSchema,
    current_sequence: Type.Integer({ minimum: 1 }),
    ownership_epoch: Type.Integer({ minimum: 0 }),
    active_effect_id: Type.Union([EffectIdSchema, Type.Null()]),
    terminal_outcome: Type.Union([Type.String({ minLength: 1, maxLength: 64 }), Type.Null()]),
    state_digest: DigestSchema,
  },
  { additionalProperties: false },
)

export const FormPresentationHintSchema = Type.Object(
  {
    path: Type.String({ minLength: 1, maxLength: 512 }),
    control: Type.Union([
      Type.Literal("text"),
      Type.Literal("textarea"),
      Type.Literal("select"),
      Type.Literal("checkbox"),
      Type.Literal("date-time"),
    ]),
    label: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    help: Type.Optional(Type.String({ minLength: 1, maxLength: 512 })),
    order: Type.Optional(Type.Integer({ minimum: 0, maximum: 1024 })),
  },
  { additionalProperties: false },
)

export const InterventionDescriptorSchema = Type.Object(
  {
    action_code: Type.String({ minLength: 1, maxLength: 128 }),
    target: StrictStringSchema,
    consequence: ConsequenceLevelSchema,
    confirmation_required: Type.Boolean(),
    form_schema_version: Type.Literal(1),
    input_schema: JsonObjectSchema,
    presentation_hints: Type.Array(FormPresentationHintSchema, { maxItems: 64 }),
    evidence_required: Type.Boolean(),
    reason_required: Type.Boolean(),
    expected_outcome_label: Type.String({ minLength: 1, maxLength: 256 }),
  },
  { additionalProperties: false },
)

export const MissionDetailSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    summary: MissionSummarySchema,
    input: ProtectedValueSchema,
    projection: MissionProjectionSchema,
    waiting_condition: Type.Optional(StrictStringSchema),
    deadline: Type.Optional(TimestampSchema),
    definition_digest: DigestSchema,
    execution_binding_digest: DigestSchema,
    valid_interventions: Type.Array(InterventionDescriptorSchema, { maxItems: 32 }),
    graph: GraphArtifactSchema,
  },
  { additionalProperties: false },
)

export const TimelineEntrySchema = Type.Object(
  {
    sequence: Type.Integer({ minimum: 1 }),
    event_id: EventIdSchema,
    event_digest: DigestSchema,
    event_type: EventTypeSchema,
    display_code: Type.String({ minLength: 1, maxLength: 128 }),
    display_args: JsonObjectSchema,
    source: EventSourceSchema,
    causation_id: Type.Optional(EventIdSchema),
    correlation_id: Type.Optional(Type.String({ minLength: 1, maxLength: 256 })),
    ownership_epoch: Type.Integer({ minimum: 0 }),
    payload: ProtectedValueSchema,
    recorded_at: TimestampSchema,
    resulting_state: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
  },
  { additionalProperties: false },
)

export const TimelinePageSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    run_id: RunIdSchema,
    items: Type.Array(TimelineEntrySchema, { maxItems: 100 }),
    next_cursor: Type.Optional(CursorSchema),
  },
  { additionalProperties: false },
)

export const EffectSummarySchema = Type.Object(
  {
    effect_id: EffectIdSchema,
    state_instance_id: StateInstanceIdSchema,
    state_name: Type.String({ minLength: 1, maxLength: 128 }),
    effect_slot: Type.String({ minLength: 1, maxLength: 128 }),
    operation: Type.String({ minLength: 1, maxLength: 128 }),
    consequence: ConsequenceLevelSchema,
    status: EffectStatusSchema,
    issue_count: Type.Integer({ minimum: 0 }),
    ownership_epoch: Type.Integer({ minimum: 0 }),
    last_evidence: Type.Optional(ProtectedValueSchema),
  },
  { additionalProperties: false },
)

export const EffectAttemptSchema = Type.Object(
  {
    attempt_id: AttemptIdSchema,
    status: Type.String({ minLength: 1, maxLength: 64 }),
    claimed_by: SourceIdSchema,
    ownership_epoch: Type.Integer({ minimum: 0 }),
    created_at: TimestampSchema,
    updated_at: TimestampSchema,
  },
  { additionalProperties: false },
)

export const EffectDetailSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    summary: EffectSummarySchema,
    capabilities: OperationCapabilitiesSchema,
    retry_policy: JsonObjectSchema,
    attempts: Type.Array(EffectAttemptSchema, { maxItems: 1000 }),
    evidence: Type.Array(ProtectedValueSchema, { maxItems: 1000 }),
    external_references: Type.Array(ProtectedValueSchema, { maxItems: 100 }),
    cancellation_effect_id: Type.Optional(EffectIdSchema),
    compensation_effect_id: Type.Optional(EffectIdSchema),
  },
  { additionalProperties: false },
)

export const EffectPageSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    run_id: RunIdSchema,
    items: Type.Array(EffectSummarySchema, { maxItems: 100 }),
    next_cursor: Type.Optional(CursorSchema),
  },
  { additionalProperties: false },
)

export const PreparedInterventionStateSchema = Type.Union([
  Type.Literal("prepared"),
  Type.Literal("submitted"),
  Type.Literal("discarded"),
  Type.Literal("expired"),
])

export const PreparedInterventionSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    client_request_id: ClientRequestIdSchema,
    actor_id: SourceIdSchema,
    run_id: RunIdSchema,
    expected_sequence: Type.Integer({ minimum: 1 }),
    action_code: Type.String({ minLength: 1, maxLength: 128 }),
    target: StrictStringSchema,
    normalized_input: JsonObjectSchema,
    reason: Type.Optional(Type.String({ minLength: 1, maxLength: 4096 })),
    proposal_digest: DigestSchema,
    prepared_at: TimestampSchema,
    expires_at: TimestampSchema,
    state: PreparedInterventionStateSchema,
    compatibility: Type.Union([Type.Literal("compatible"), Type.Literal("incompatible")]),
  },
  { additionalProperties: false },
)

export const PrepareInterventionRequestSchema = Type.Object(
  {
    client_request_id: ClientRequestIdSchema,
    expected_sequence: Type.Integer({ minimum: 1 }),
    action_code: Type.String({ minLength: 1, maxLength: 128 }),
    target: StrictStringSchema,
    input: JsonObjectSchema,
    reason: Type.Optional(Type.String({ minLength: 1, maxLength: 4096 })),
  },
  { additionalProperties: false },
)

export const SubmitInterventionRequestSchema = Type.Object(
  {
    client_request_id: ClientRequestIdSchema,
    proposal_digest: DigestSchema,
  },
  { additionalProperties: false },
)

export const InterventionOutcomeSchema = Type.Union([
  Type.Literal("queued"),
  Type.Literal("accepted"),
  Type.Literal("rejected_stale"),
  Type.Literal("rejected_invalid"),
])

export const InterventionReceiptSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    client_request_id: ClientRequestIdSchema,
    proposal_digest: DigestSchema,
    run_id: RunIdSchema,
    expected_sequence: Type.Integer({ minimum: 1 }),
    outcome: InterventionOutcomeSchema,
    accepted_sequence: Type.Optional(Type.Integer({ minimum: 1 })),
    received_at: TimestampSchema,
    resolved_at: Type.Optional(TimestampSchema),
    revision: RevisionSchema,
  },
  { additionalProperties: false },
)

export const ReplayResultSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    run_id: RunIdSchema,
    requested_sequence: Type.Integer({ minimum: 1 }),
    projection: MissionProjectionSchema,
    stored_digest: DigestSchema,
    rebuilt_digest: DigestSchema,
    verification: Type.Union([Type.Literal("match"), Type.Literal("diverged")]),
    events_reduced: Type.Integer({ minimum: 1 }),
    graph: GraphArtifactSchema,
  },
  { additionalProperties: false },
)

export const SessionCapabilitySchema = Type.Union([
  Type.Literal("view"),
  Type.Literal("operate"),
  Type.Literal("development_faults"),
])

export const SessionDocumentSchema = Type.Object(
  {
    schema_version: Type.Literal(ApiSchemaVersion),
    actor_id: SourceIdSchema,
    capabilities: Type.Array(SessionCapabilitySchema, { minItems: 1, maxItems: 3 }),
    csrf_token: Type.String({ minLength: 32, maxLength: 256 }),
    csrf_expires_at: TimestampSchema,
    session_expires_at: TimestampSchema,
    api_schema_versions: Type.Array(Type.Integer({ minimum: 1 }), { minItems: 1 }),
    stream_schema_versions: Type.Array(Type.Integer({ minimum: 1 }), { minItems: 1 }),
    renewal: Type.Literal("bootstrap_required"),
  },
  { additionalProperties: false },
)

export const MissionStreamBodySchema = Type.Object(
  {
    run_id: RunIdSchema,
    sequence: Type.Integer({ minimum: 1 }),
    event_id: EventIdSchema,
    event_digest: DigestSchema,
  },
  { additionalProperties: false },
)

export const OperationsStreamBodySchema = Type.Object(
  {
    list_revision: RevisionSchema,
    changed_run_ids: Type.Array(RunIdSchema, { maxItems: 1000 }),
  },
  { additionalProperties: false },
)

export const ReceiptStreamBodySchema = Type.Object(
  {
    run_id: RunIdSchema,
    client_request_id: ClientRequestIdSchema,
    receipt_revision: RevisionSchema,
    receipt: InterventionReceiptSchema,
  },
  { additionalProperties: false },
)

const StreamBaseFields = {
  stream_event_id: Type.String({ minLength: 16, maxLength: 128 }),
  schema_version: Type.Literal(StreamSchemaVersion),
  resume_cursor: CursorSchema,
}

export const StreamEnvelopeSchema = Type.Union([
  Type.Object(
    { ...StreamBaseFields, kind: Type.Literal("heartbeat") },
    { additionalProperties: false },
  ),
  Type.Object(
    { ...StreamBaseFields, kind: Type.Literal("mission"), body: MissionStreamBodySchema },
    { additionalProperties: false },
  ),
  Type.Object(
    { ...StreamBaseFields, kind: Type.Literal("operations"), body: OperationsStreamBodySchema },
    { additionalProperties: false },
  ),
  Type.Object(
    { ...StreamBaseFields, kind: Type.Literal("receipt"), body: ReceiptStreamBodySchema },
    { additionalProperties: false },
  ),
])

export type ApiError = Type.Static<typeof ApiErrorSchema>
export type ProtectedValue = Type.Static<typeof ProtectedValueSchema>
export type AttentionReason = Type.Static<typeof AttentionReasonSchema>
export type MissionSummary = Type.Static<typeof MissionSummarySchema>
export type MissionListQuery = Type.Static<typeof MissionListQuerySchema>
export type MissionListPage = Type.Static<typeof MissionListPageSchema>
export type GraphNode = Type.Static<typeof GraphNodeSchema>
export type GraphTransition = Type.Static<typeof GraphTransitionSchema>
export type GraphArtifact = Type.Static<typeof GraphArtifactSchema>
export type MissionProjection = Type.Static<typeof MissionProjectionSchema>
export type InterventionDescriptor = Type.Static<typeof InterventionDescriptorSchema>
export type MissionDetail = Type.Static<typeof MissionDetailSchema>
export type TimelineEntry = Type.Static<typeof TimelineEntrySchema>
export type TimelinePage = Type.Static<typeof TimelinePageSchema>
export type EffectSummary = Type.Static<typeof EffectSummarySchema>
export type EffectDetail = Type.Static<typeof EffectDetailSchema>
export type EffectPage = Type.Static<typeof EffectPageSchema>
export type PreparedIntervention = Type.Static<typeof PreparedInterventionSchema>
export type PrepareInterventionRequest = Type.Static<typeof PrepareInterventionRequestSchema>
export type SubmitInterventionRequest = Type.Static<typeof SubmitInterventionRequestSchema>
export type InterventionReceipt = Type.Static<typeof InterventionReceiptSchema>
export type ReplayResult = Type.Static<typeof ReplayResultSchema>
export type SessionDocument = Type.Static<typeof SessionDocumentSchema>
export type StreamEnvelope = Type.Static<typeof StreamEnvelopeSchema>
