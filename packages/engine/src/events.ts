import {
  AttemptIdSchema,
  CommandIdSchema,
  DigestSchema,
  EffectIdSchema,
  FenceProofSchema,
  HandoffIdSchema,
  IssueResultSchema,
  JsonObjectSchema,
  JsonValueSchema,
  LeaseIdSchema,
  ObservationIdSchema,
  OwnershipHandoffBlockReasonSchema,
  OwnershipLeaseSchema,
  ReconciliationResultSchema,
  SourceIdSchema,
  StateInstanceIdSchema,
  TimerIdSchema,
  type AcceptedEvent,
} from "@idemantis/contracts"
import Type from "typebox"
import Schema from "typebox/schema"
import { ReductionError } from "./errors.js"
import type { ReducerEvent } from "./types.js"

export const MissionAdmittedPayloadSchema = Type.Object(
  {
    definition_digest: DigestSchema,
    execution_binding_digest: DigestSchema,
    input: JsonObjectSchema,
  },
  { additionalProperties: false },
)

export const EffectDispatchStartedPayloadSchema = Type.Object(
  {
    attempt_id: AttemptIdSchema,
    command_id: CommandIdSchema,
    effect_id: EffectIdSchema,
  },
  { additionalProperties: false },
)

export const EffectIssueResultPayloadSchema = Type.Object(
  {
    attempt_id: AttemptIdSchema,
    command_id: CommandIdSchema,
    effect_id: EffectIdSchema,
    result: IssueResultSchema,
  },
  { additionalProperties: false },
)

export const EffectReconciliationResultPayloadSchema = Type.Object(
  {
    attempt_id: AttemptIdSchema,
    command_id: CommandIdSchema,
    effect_id: EffectIdSchema,
    result: ReconciliationResultSchema,
  },
  { additionalProperties: false },
)

export const ObservationRecordedPayloadSchema = Type.Object(
  {
    observation_id: ObservationIdSchema,
    effect_id: Type.Optional(EffectIdSchema),
    observation_type: Type.String({
      minLength: 3,
      maxLength: 128,
      pattern: "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)+$",
    }),
    subject: Type.Optional(JsonValueSchema),
    payload: JsonObjectSchema,
  },
  { additionalProperties: false },
)

export const TimerFiredPayloadSchema = Type.Object(
  {
    timer_id: TimerIdSchema,
    state_instance_id: StateInstanceIdSchema,
  },
  { additionalProperties: false },
)

export const OwnershipLeaseRegisteredPayloadSchema = Type.Object(
  { lease: OwnershipLeaseSchema },
  { additionalProperties: false },
)

export const OwnershipHandoffRequestedPayloadSchema = Type.Object(
  {
    handoff_id: HandoffIdSchema,
    source_lease_id: LeaseIdSchema,
    target_lease_id: LeaseIdSchema,
    target_epoch: Type.Integer({ minimum: 2 }),
    target_owner_mode: Type.Union([Type.Literal("runner"), Type.Literal("edge")]),
    target_owner_id: SourceIdSchema,
  },
  { additionalProperties: false },
)

export const OwnershipHandoffBlockedPayloadSchema = Type.Object(
  {
    handoff_id: HandoffIdSchema,
    reason: OwnershipHandoffBlockReasonSchema,
  },
  { additionalProperties: false },
)

export const OwnershipReleasedPayloadSchema = Type.Object(
  {
    handoff_id: HandoffIdSchema,
    lease_id: LeaseIdSchema,
  },
  { additionalProperties: false },
)

export const OwnershipFenceActivatedPayloadSchema = Type.Object(
  {
    handoff_id: HandoffIdSchema,
    proof: FenceProofSchema,
  },
  { additionalProperties: false },
)

export const OwnershipActivatedPayloadSchema = Type.Object(
  {
    handoff_id: HandoffIdSchema,
    lease: OwnershipLeaseSchema,
  },
  { additionalProperties: false },
)

const validators = {
  "mission.admitted": Schema.Compile(MissionAdmittedPayloadSchema),
  "effect.dispatch_started": Schema.Compile(EffectDispatchStartedPayloadSchema),
  "effect.issue_result_recorded": Schema.Compile(EffectIssueResultPayloadSchema),
  "effect.reconciliation_result_recorded": Schema.Compile(EffectReconciliationResultPayloadSchema),
  "observation.recorded": Schema.Compile(ObservationRecordedPayloadSchema),
  "timer.fired": Schema.Compile(TimerFiredPayloadSchema),
  "ownership.lease_registered": Schema.Compile(OwnershipLeaseRegisteredPayloadSchema),
  "ownership.handoff_requested": Schema.Compile(OwnershipHandoffRequestedPayloadSchema),
  "ownership.handoff_blocked": Schema.Compile(OwnershipHandoffBlockedPayloadSchema),
  "ownership.released": Schema.Compile(OwnershipReleasedPayloadSchema),
  "ownership.fence_activated": Schema.Compile(OwnershipFenceActivatedPayloadSchema),
  "ownership.activated": Schema.Compile(OwnershipActivatedPayloadSchema),
} as const

export function parseReducerEvent(event: AcceptedEvent): ReducerEvent {
  if (!Object.hasOwn(validators, event.type)) {
    throw new ReductionError(
      "UNSUPPORTED_EVENT",
      `Event type ${event.type} is not supported by this interpreter`,
      event.event_id,
    )
  }

  const type = event.type as keyof typeof validators
  const validator = validators[type]

  if (!validator.Check(event.payload)) {
    throw new ReductionError(
      "INVALID_EVENT_PAYLOAD",
      `Event ${event.event_id} has an invalid ${type} payload`,
      event.event_id,
    )
  }

  return { type, payload: event.payload } as ReducerEvent
}
