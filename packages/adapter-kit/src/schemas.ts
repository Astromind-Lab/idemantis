import {
  AdapterInstanceIdSchema,
  AttemptIdSchema,
  CancellationResultSchema,
  CommandIdSchema,
  ConsequenceLevelSchema,
  DefinitionVersionSchema,
  DigestSchema,
  EffectClassSchema,
  EffectIdSchema,
  EventTypeSchema,
  FenceProofSchema,
  IssueResultSchema,
  JsonObjectSchema,
  JsonValueSchema,
  OperationCapabilitiesSchema,
  OperationNameSchema,
  ReconciliationResultSchema,
  RunIdSchema,
  SourceIdSchema,
  StepIdSchema,
  TimestampSchema,
} from "@idemantis/contracts"
import Type from "typebox"

const adapterNamePattern = "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$"
const fieldPathPattern = "^[a-zA-Z0-9_]+(?:\\.[a-zA-Z0-9_]+)*$"

const TimeoutRangeSchema = Type.Object(
  {
    minimum_ms: Type.Integer({ minimum: 1 }),
    default_ms: Type.Integer({ minimum: 1 }),
    maximum_ms: Type.Integer({ minimum: 1 }),
  },
  { additionalProperties: false },
)

const RateLimitSchema = Type.Object(
  {
    maximum_requests: Type.Integer({ minimum: 1 }),
    window_ms: Type.Integer({ minimum: 1 }),
  },
  { additionalProperties: false },
)

const SecretRequirementSchema = Type.Object(
  {
    name: Type.String({ minLength: 1, maxLength: 128, pattern: adapterNamePattern }),
    required: Type.Boolean(),
  },
  { additionalProperties: false },
)

export const AdapterOperationDescriptorSchema = Type.Object(
  {
    key: StepIdSchema,
    adapter: Type.String({ minLength: 1, maxLength: 128, pattern: adapterNamePattern }),
    operation: OperationNameSchema,
    operation_schema_version: Type.Integer({ minimum: 1 }),
    input_schema: JsonObjectSchema,
    output_schema: JsonObjectSchema,
    subject_schema: Type.Optional(JsonObjectSchema),
    effect_class: EffectClassSchema,
    consequence: ConsequenceLevelSchema,
    capabilities: OperationCapabilitiesSchema,
    timeout: TimeoutRangeSchema,
    rate_limit: Type.Optional(RateLimitSchema),
    reconciliation_lookup_fields: Type.Array(
      Type.String({ minLength: 1, maxLength: 128, pattern: fieldPathPattern }),
      { maxItems: 32, uniqueItems: true },
    ),
    observation_types: Type.Array(EventTypeSchema, { maxItems: 64, uniqueItems: true }),
    observation_correlation_fields: Type.Array(
      Type.String({ minLength: 1, maxLength: 128, pattern: fieldPathPattern }),
      { maxItems: 32, uniqueItems: true },
    ),
    secret_requirements: Type.Array(SecretRequirementSchema, {
      maxItems: 32,
      uniqueItems: true,
    }),
    maximum_fence_activation_delay_ms: Type.Optional(Type.Integer({ minimum: 0 })),
  },
  { $id: "AdapterOperationDescriptor", additionalProperties: false },
)

export const AdapterCapabilityManifestSchema = Type.Object(
  {
    adapter: Type.String({ minLength: 1, maxLength: 128, pattern: adapterNamePattern }),
    adapter_version: DefinitionVersionSchema,
    adapter_package_digest: DigestSchema,
    operations: Type.Array(AdapterOperationDescriptorSchema, { minItems: 1 }),
  },
  { $id: "AdapterCapabilityManifest", additionalProperties: false },
)

const EvidenceSchema = Type.Optional(JsonObjectSchema)
const ExternalReferenceSchema = Type.String({ minLength: 1, maxLength: 256 })

export const IssueResultEnvelopeSchema = Type.Union(
  [
    Type.Object(
      { result: Type.Literal("not_sent"), evidence: EvidenceSchema },
      { additionalProperties: false },
    ),
    Type.Object(
      {
        result: Type.Literal("accepted"),
        external_reference: ExternalReferenceSchema,
        evidence: EvidenceSchema,
      },
      { additionalProperties: false },
    ),
    Type.Object(
      {
        result: Type.Literal("completed"),
        output: JsonValueSchema,
        external_reference: Type.Optional(ExternalReferenceSchema),
        evidence: EvidenceSchema,
      },
      { additionalProperties: false },
    ),
    Type.Object(
      { result: Type.Literal("rejected"), evidence: EvidenceSchema },
      { additionalProperties: false },
    ),
    Type.Object(
      { result: Type.Literal("failed"), evidence: EvidenceSchema },
      { additionalProperties: false },
    ),
    Type.Object(
      { result: Type.Literal("uncertain"), evidence: EvidenceSchema },
      { additionalProperties: false },
    ),
  ],
  { $id: "IssueResultEnvelope" },
)

export const ReconciliationResultEnvelopeSchema = Type.Union(
  [
    Type.Object(
      {
        result: Type.Literal("confirmed_completed"),
        output: JsonValueSchema,
        external_reference: Type.Optional(ExternalReferenceSchema),
        evidence: EvidenceSchema,
      },
      { additionalProperties: false },
    ),
    Type.Object(
      { result: Type.Literal("confirmed_not_applied"), evidence: EvidenceSchema },
      { additionalProperties: false },
    ),
    Type.Object(
      {
        result: Type.Literal("confirmed_in_progress"),
        external_reference: Type.Optional(ExternalReferenceSchema),
        evidence: EvidenceSchema,
      },
      { additionalProperties: false },
    ),
    Type.Object(
      { result: Type.Literal("conflicting_state"), evidence: EvidenceSchema },
      { additionalProperties: false },
    ),
    Type.Object(
      { result: Type.Literal("unknown"), evidence: EvidenceSchema },
      { additionalProperties: false },
    ),
  ],
  { $id: "ReconciliationResultEnvelope" },
)

export const CancellationResultEnvelopeSchema = Type.Object(
  {
    result: CancellationResultSchema,
    external_reference: Type.Optional(ExternalReferenceSchema),
    evidence: EvidenceSchema,
  },
  { $id: "CancellationResultEnvelope", additionalProperties: false },
)

export const AdapterInvocationIdentitySchema = Type.Object(
  {
    adapter_instance_id: AdapterInstanceIdSchema,
    run_id: RunIdSchema,
    command_id: CommandIdSchema,
    attempt_id: AttemptIdSchema,
    effect_id: EffectIdSchema,
    ownership_epoch: Type.Integer({ minimum: 0 }),
    deadline: TimestampSchema,
    source_id: SourceIdSchema,
  },
  { $id: "AdapterInvocationIdentity", additionalProperties: false },
)

export { CancellationResultSchema, FenceProofSchema, IssueResultSchema, ReconciliationResultSchema }
