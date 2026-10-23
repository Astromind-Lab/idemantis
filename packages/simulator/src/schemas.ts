import { JsonObjectSchema, TimestampSchema } from "@idemantis/contracts"
import Type from "typebox"

const stableNamePattern = "^[a-zA-Z0-9](?:[a-zA-Z0-9._:-]*[a-zA-Z0-9])?$"

export const FaultActionSchema = Type.Union([
  Type.Literal("drop_response"),
  Type.Literal("drop_observation"),
  Type.Literal("duplicate_observation"),
  Type.Literal("delay"),
])

export const FaultRuleSchema = Type.Object(
  {
    id: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    target: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    operation: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    stage: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    occurrence: Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
    action: FaultActionSchema,
    delay_ms: Type.Optional(Type.Integer({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER })),
  },
  { additionalProperties: false },
)

export const FaultScheduleSchema = Type.Array(FaultRuleSchema, { maxItems: 256 })

export const SimulationTraceEntrySchema = Type.Object(
  {
    sequence: Type.Integer({ minimum: 1 }),
    at: TimestampSchema,
    target: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    operation: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    stage: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    correlation_id: Type.String({ minLength: 1, maxLength: 256, pattern: stableNamePattern }),
    payload: JsonObjectSchema,
  },
  { additionalProperties: false },
)

export const SimulatedObservationSchema = Type.Object(
  {
    observation_id: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    target: Type.String({ minLength: 1, maxLength: 128, pattern: stableNamePattern }),
    type: Type.String({
      minLength: 3,
      maxLength: 128,
      pattern: "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)+$",
    }),
    subject_reference: Type.String({ minLength: 1, maxLength: 256 }),
    effect_id: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
    ownership_epoch: Type.Optional(Type.Integer({ minimum: 0 })),
    observed_at: TimestampSchema,
    payload: JsonObjectSchema,
  },
  { additionalProperties: false },
)
