import Type from "typebox"
import { FenceProofSchema } from "./capabilities.js"
import {
  AttemptIdSchema,
  ClientRequestIdSchema,
  CommandIdSchema,
  DigestSchema,
  EffectIdSchema,
  EventIdSchema,
  InterventionIdSchema,
  LeaseIdSchema,
  ObservationIdSchema,
  RunIdSchema,
  SourceIdSchema,
  TimerIdSchema,
} from "./ids.js"
import { JsonObjectSchema } from "./json.js"
import { DurationMsSchema, TimestampSchema } from "./time.js"

export const CommandStatusSchema = Type.Union(
  [
    Type.Literal("pending"),
    Type.Literal("claimed"),
    Type.Literal("completed"),
    Type.Literal("superseded"),
  ],
  { $id: "CommandStatus" },
)

export const CommandKindSchema = Type.Union(
  [
    Type.Literal("effect.issue"),
    Type.Literal("effect.reconcile"),
    Type.Literal("effect.cancel"),
    Type.Literal("effect.compensate"),
    Type.Literal("timer.schedule"),
    Type.Literal("ownership.handoff"),
  ],
  { $id: "CommandKind" },
)

/** Describes deterministic work emitted from accepted mission events. */
export const CommandRecordSchema = Type.Object(
  {
    command_id: CommandIdSchema,
    run_id: RunIdSchema,
    causing_event_id: EventIdSchema,
    command_slot: Type.String({ minLength: 1, maxLength: 128 }),
    kind: CommandKindSchema,
    status: CommandStatusSchema,
    ownership_epoch: Type.Integer({ minimum: 0 }),
    payload: JsonObjectSchema,
    created_at: TimestampSchema,
    claim_deadline: Type.Optional(TimestampSchema),
    claimed_by: Type.Optional(SourceIdSchema),
  },
  { $id: "CommandRecord", additionalProperties: false },
)

export const AttemptStatusSchema = Type.Union(
  [Type.Literal("claimed"), Type.Literal("dispatch_started"), Type.Literal("result_recorded")],
  { $id: "AttemptStatus" },
)

/** Tracks one bounded claim and dispatch attempt for an effect command. */
export const AttemptRecordSchema = Type.Object(
  {
    attempt_id: AttemptIdSchema,
    command_id: CommandIdSchema,
    effect_id: EffectIdSchema,
    status: AttemptStatusSchema,
    ownership_epoch: Type.Integer({ minimum: 0 }),
    claimed_by: SourceIdSchema,
    claim_deadline: TimestampSchema,
    started_event_id: Type.Optional(EventIdSchema),
    result_event_id: Type.Optional(EventIdSchema),
    created_at: TimestampSchema,
    updated_at: TimestampSchema,
  },
  { $id: "AttemptRecord", additionalProperties: false },
)

/** Captures evidence reported by a device, service, adapter, or operator. */
export const ObservationRecordSchema = Type.Object(
  {
    observation_id: ObservationIdSchema,
    run_id: RunIdSchema,
    source_id: SourceIdSchema,
    observation_type: Type.String({
      minLength: 3,
      maxLength: 128,
      pattern: "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)+$",
    }),
    schema_version: Type.Integer({ minimum: 1 }),
    subject_reference: Type.String({ minLength: 1, maxLength: 256 }),
    effect_id: Type.Optional(EffectIdSchema),
    ownership_epoch: Type.Optional(Type.Integer({ minimum: 0 })),
    observed_at: TimestampSchema,
    recorded_at: TimestampSchema,
    payload: JsonObjectSchema,
  },
  { $id: "ObservationRecord", additionalProperties: false },
)

export const TimerStatusSchema = Type.Union(
  [
    Type.Literal("scheduled"),
    Type.Literal("fired"),
    Type.Literal("cancelled"),
    Type.Literal("superseded"),
  ],
  { $id: "TimerStatus" },
)

/** Persists a durable deadline that can be fired at most once. */
export const TimerRecordSchema = Type.Object(
  {
    timer_id: TimerIdSchema,
    run_id: RunIdSchema,
    status: TimerStatusSchema,
    due_at: TimestampSchema,
    purpose: Type.String({ minLength: 1, maxLength: 256 }),
    causing_event_id: EventIdSchema,
    payload: JsonObjectSchema,
  },
  { $id: "TimerRecord", additionalProperties: false },
)

export const OwnershipModeSchema = Type.Union(
  [Type.Literal("runner"), Type.Literal("edge"), Type.Literal("none")],
  { $id: "OwnershipMode" },
)

/** Grants time-bounded execution ownership for a mission run. */
export const OwnershipLeaseSchema = Type.Object(
  {
    lease_id: LeaseIdSchema,
    run_id: RunIdSchema,
    owner_mode: OwnershipModeSchema,
    owner_id: SourceIdSchema,
    ownership_epoch: Type.Integer({ minimum: 1 }),
    branch_anchor_sequence: Type.Integer({ minimum: 0 }),
    not_before: TimestampSchema,
    not_after: TimestampSchema,
    maximum_offline_duration_ms: DurationMsSchema,
    allowed_clock_uncertainty_ms: DurationMsSchema,
    delegation_digest: DigestSchema,
    fence_proof: Type.Optional(FenceProofSchema),
  },
  { $id: "OwnershipLease", additionalProperties: false },
)

export const InterventionStatusSchema = Type.Union(
  [
    Type.Literal("queued"),
    Type.Literal("accepted"),
    Type.Literal("rejected_stale"),
    Type.Literal("rejected_invalid"),
  ],
  { $id: "InterventionStatus" },
)

/** Records an operator request and its sequence-aware resolution. */
export const InterventionRecordSchema = Type.Object(
  {
    intervention_id: InterventionIdSchema,
    run_id: RunIdSchema,
    client_request_id: ClientRequestIdSchema,
    proposal_digest: DigestSchema,
    expected_sequence: Type.Integer({ minimum: 0 }),
    actor_id: SourceIdSchema,
    action: Type.String({
      minLength: 1,
      maxLength: 128,
      pattern: "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$",
    }),
    reason: Type.String({ minLength: 1, maxLength: 1024 }),
    status: InterventionStatusSchema,
    payload: JsonObjectSchema,
    submitted_at: TimestampSchema,
    resolved_at: Type.Optional(TimestampSchema),
  },
  { $id: "InterventionRecord", additionalProperties: false },
)

export const IngressOutcomeSchema = Type.Union(
  [
    Type.Literal("queued"),
    Type.Literal("accepted"),
    Type.Literal("rejected_stale"),
    Type.Literal("rejected_invalid"),
    Type.Literal("superseded"),
  ],
  { $id: "IngressOutcome" },
)

/** Acknowledges a client request independently of its eventual effect. */
export const IngressReceiptSchema = Type.Object(
  {
    run_id: RunIdSchema,
    client_request_id: ClientRequestIdSchema,
    proposal_digest: DigestSchema,
    ingress_sequence: Type.Integer({ minimum: 1 }),
    outcome: IngressOutcomeSchema,
    received_at: TimestampSchema,
    event_id: Type.Optional(EventIdSchema),
  },
  { $id: "IngressReceipt", additionalProperties: false },
)

export type CommandStatus = Type.Static<typeof CommandStatusSchema>
export type CommandKind = Type.Static<typeof CommandKindSchema>
export type CommandRecord = Type.Static<typeof CommandRecordSchema>
export type AttemptStatus = Type.Static<typeof AttemptStatusSchema>
export type AttemptRecord = Type.Static<typeof AttemptRecordSchema>
export type ObservationRecord = Type.Static<typeof ObservationRecordSchema>
export type TimerStatus = Type.Static<typeof TimerStatusSchema>
export type TimerRecord = Type.Static<typeof TimerRecordSchema>
export type OwnershipMode = Type.Static<typeof OwnershipModeSchema>
export type OwnershipLease = Type.Static<typeof OwnershipLeaseSchema>
export type InterventionStatus = Type.Static<typeof InterventionStatusSchema>
export type InterventionRecord = Type.Static<typeof InterventionRecordSchema>
export type IngressOutcome = Type.Static<typeof IngressOutcomeSchema>
export type IngressReceipt = Type.Static<typeof IngressReceiptSchema>
