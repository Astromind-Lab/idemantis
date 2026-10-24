import {
  DigestSchema,
  EffectIdSchema,
  EffectStatusSchema,
  JsonObjectSchema,
  JsonValueSchema,
  MissionStatusSchema,
  ObservationIdSchema,
  RunIdSchema,
  StateInstanceIdSchema,
  StepIdSchema,
  TimerIdSchema,
} from "@idemantis/contracts"
import Type from "typebox"
import type { MissionState } from "./types.js"

export const EffectProjectionSchema = Type.Object(
  {
    effect_id: EffectIdSchema,
    state_instance_id: StateInstanceIdSchema,
    state_name: StepIdSchema,
    effect_slot: StepIdSchema,
    operation: StepIdSchema,
    status: EffectStatusSchema,
    required: Type.Boolean(),
    issue_count: Type.Integer({ minimum: 1 }),
    retry_timer_id: Type.Union([TimerIdSchema, Type.Null()]),
    ownership_epoch: Type.Integer({ minimum: 0 }),
  },
  { $id: "EffectProjection", additionalProperties: false },
)

const FinalOutcomeSchema = Type.Union([
  Type.Literal("completed"),
  Type.Literal("cancelled"),
  Type.Literal("compensated"),
  Type.Literal("failed"),
  Type.Literal("unresolved"),
])

export const MissionStateSchema = Type.Unsafe<MissionState>(
  Type.Object(
    {
      run_id: RunIdSchema,
      definition_digest: DigestSchema,
      execution_binding_digest: DigestSchema,
      status: MissionStatusSchema,
      current_state: StepIdSchema,
      current_state_instance_id: StateInstanceIdSchema,
      current_sequence: Type.Integer({ minimum: 1 }),
      ownership_epoch: Type.Integer({ minimum: 0 }),
      input: JsonObjectSchema,
      active_effect_id: Type.Union([EffectIdSchema, Type.Null()]),
      terminal_outcome: Type.Union([FinalOutcomeSchema, Type.Null()]),
      effects: Type.Record(EffectIdSchema, EffectProjectionSchema),
      observations: Type.Record(ObservationIdSchema, Type.Literal(true)),
      outputs: Type.Optional(Type.Record(StepIdSchema, JsonValueSchema)),
    },
    { $id: "MissionState", additionalProperties: false },
  ),
)
