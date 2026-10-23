import Type from "typebox"
import { EffectClassSchema } from "./capabilities.js"
import { ProposedEventSchema } from "./events.js"
import {
  AdapterInstanceIdSchema,
  DigestSchema,
  EventTypeSchema,
  OperationNameSchema,
  SourceIdSchema,
  StepIdSchema,
} from "./ids.js"
import { OwnershipLeaseSchema } from "./execution.js"
import { TimestampSchema } from "./time.js"

export const EdgeDelegatedActionSchema = Type.Object(
  {
    state: StepIdSchema,
    effect_class: EffectClassSchema,
    operation: OperationNameSchema,
    adapter_instance_id: AdapterInstanceIdSchema,
    input_digests: Type.Array(DigestSchema, {
      minItems: 1,
      maxItems: 1_000,
      uniqueItems: true,
    }),
  },
  { $id: "EdgeDelegatedAction", additionalProperties: false },
)

export const EdgeResourceLimitsSchema = Type.Object(
  {
    maximum_branch_events: Type.Integer({ minimum: 1, maximum: 100_000 }),
    maximum_dispatches: Type.Integer({ minimum: 1, maximum: 100_000 }),
  },
  { $id: "EdgeResourceLimits", additionalProperties: false },
)

export const EdgeDelegationSchema = Type.Object(
  {
    lease: OwnershipLeaseSchema,
    definition_digest: DigestSchema,
    execution_binding_digest: DigestSchema,
    allowed_actions: Type.Array(EdgeDelegatedActionSchema, {
      minItems: 1,
      maxItems: 1_000,
      uniqueItems: true,
    }),
    allowed_event_types: Type.Array(EventTypeSchema, {
      minItems: 1,
      maxItems: 1_000,
      uniqueItems: true,
    }),
    resource_limits: EdgeResourceLimitsSchema,
  },
  { $id: "EdgeDelegation", additionalProperties: false },
)

export const EdgeActivationRecordSchema = Type.Object(
  {
    lease_id: OwnershipLeaseSchema.properties.lease_id,
    edge_id: SourceIdSchema,
    boot_identity: SourceIdSchema,
    wall_at: TimestampSchema,
    monotonic_at_ms: Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
    measured_clock_uncertainty_ms: Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
    delegation_digest: DigestSchema,
  },
  { $id: "EdgeActivationRecord", additionalProperties: false },
)

export const EdgeStopReasonSchema = Type.Union(
  [
    Type.Literal("lease_expired"),
    Type.Literal("offline_duration_exceeded"),
    Type.Literal("wall_clock_discontinuity"),
    Type.Literal("unverified_reboot"),
    Type.Literal("edge_identity_changed"),
    Type.Literal("monotonic_discontinuity"),
    Type.Literal("scope_violation"),
    Type.Literal("branch_limit_reached"),
    Type.Literal("dispatch_limit_reached"),
  ],
  { $id: "EdgeStopReason" },
)

export const EdgeExecutionScopeSchema = Type.Object(
  {
    state: StepIdSchema,
    effect_class: EffectClassSchema,
    operation: OperationNameSchema,
    adapter_instance_id: AdapterInstanceIdSchema,
    input_digest: DigestSchema,
  },
  { $id: "EdgeExecutionScope", additionalProperties: false },
)

export const EdgeBranchEventSchema = Type.Object(
  {
    edge_local_sequence: Type.Integer({ minimum: 1 }),
    proposal: ProposedEventSchema,
    proposal_digest: DigestSchema,
    previous_branch_digest: Type.Optional(DigestSchema),
    branch_digest: DigestSchema,
    recorded_at: TimestampSchema,
    execution: Type.Optional(EdgeExecutionScopeSchema),
  },
  { $id: "EdgeBranchEvent", additionalProperties: false },
)

export const EdgeReconnectUploadSchema = Type.Object(
  {
    delegation: EdgeDelegationSchema,
    activation: EdgeActivationRecordSchema,
    events: Type.Array(EdgeBranchEventSchema, { maxItems: 100_000 }),
    final_branch_digest: DigestSchema,
    stopped_reason: Type.Optional(EdgeStopReasonSchema),
    created_at: TimestampSchema,
  },
  { $id: "EdgeReconnectUpload", additionalProperties: false },
)

export type EdgeDelegatedAction = Type.Static<typeof EdgeDelegatedActionSchema>
export type EdgeResourceLimits = Type.Static<typeof EdgeResourceLimitsSchema>
export type EdgeDelegation = Type.Static<typeof EdgeDelegationSchema>
export type EdgeActivationRecord = Type.Static<typeof EdgeActivationRecordSchema>
export type EdgeStopReason = Type.Static<typeof EdgeStopReasonSchema>
export type EdgeExecutionScope = Type.Static<typeof EdgeExecutionScopeSchema>
export type EdgeBranchEvent = Type.Static<typeof EdgeBranchEventSchema>
export type EdgeReconnectUpload = Type.Static<typeof EdgeReconnectUploadSchema>
