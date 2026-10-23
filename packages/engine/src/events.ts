import {
  AttemptIdSchema,
  CommandIdSchema,
  DigestSchema,
  EffectIdSchema,
  IssueResultSchema,
  JsonObjectSchema,
  JsonValueSchema,
  ObservationIdSchema,
  ReconciliationResultSchema,
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

const validators = {
  "mission.admitted": Schema.Compile(MissionAdmittedPayloadSchema),
  "effect.dispatch_started": Schema.Compile(EffectDispatchStartedPayloadSchema),
  "effect.issue_result_recorded": Schema.Compile(EffectIssueResultPayloadSchema),
  "effect.reconciliation_result_recorded": Schema.Compile(EffectReconciliationResultPayloadSchema),
  "observation.recorded": Schema.Compile(ObservationRecordedPayloadSchema),
  "timer.fired": Schema.Compile(TimerFiredPayloadSchema),
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
