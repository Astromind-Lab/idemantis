import Type from "typebox"
import {
  AdapterOperationReferenceSchema,
  ConsequenceLevelSchema,
  EffectClassSchema,
  OperationCapabilitiesSchema,
} from "./capabilities.js"
import { EffectIdSchema, RunIdSchema, StateInstanceIdSchema, StepIdSchema } from "./ids.js"
import { JsonObjectSchema } from "./json.js"
import { TimestampSchema } from "./time.js"

export const EffectStatusSchema = Type.Union(
  [
    Type.Literal("declared"),
    Type.Literal("prepared"),
    Type.Literal("dispatching"),
    Type.Literal("acknowledged"),
    Type.Literal("in_progress"),
    Type.Literal("observed"),
    Type.Literal("settled"),
    Type.Literal("uncertain"),
    Type.Literal("rejected"),
    Type.Literal("failed"),
    Type.Literal("cancel_pending"),
    Type.Literal("cancelling"),
    Type.Literal("cancelled"),
    Type.Literal("compensating"),
    Type.Literal("compensated"),
    Type.Literal("compensation_failed"),
  ],
  { $id: "EffectStatus" },
)

export const EffectRelationSchema = Type.Union(
  [Type.Literal("cancellation"), Type.Literal("compensation"), Type.Literal("follow_up")],
  { $id: "EffectRelation" },
)

/** Records one externally observable effect requested by a mission step. */
export const EffectRecordSchema = Type.Object(
  {
    effect_id: EffectIdSchema,
    run_id: RunIdSchema,
    state_instance_id: StateInstanceIdSchema,
    step_id: StepIdSchema,
    effect_slot: Type.String({
      minLength: 1,
      maxLength: 128,
      pattern: "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$",
    }),
    status: EffectStatusSchema,
    effect_class: EffectClassSchema,
    consequence: ConsequenceLevelSchema,
    required: Type.Boolean(),
    operation: AdapterOperationReferenceSchema,
    capabilities: OperationCapabilitiesSchema,
    input: JsonObjectSchema,
    ownership_epoch: Type.Integer({ minimum: 0 }),
    related_effect_id: Type.Optional(EffectIdSchema),
    relation: Type.Optional(EffectRelationSchema),
    external_reference: Type.Optional(Type.String({ minLength: 1, maxLength: 256 })),
    created_at: TimestampSchema,
    updated_at: TimestampSchema,
  },
  { $id: "EffectRecord", additionalProperties: false },
)

/** Classifies what reconciliation could establish about an uncertain effect. */
export const ReconciliationResultSchema = Type.Union(
  [
    Type.Literal("confirmed_completed"),
    Type.Literal("confirmed_not_applied"),
    Type.Literal("confirmed_in_progress"),
    Type.Literal("conflicting_state"),
    Type.Literal("unknown"),
  ],
  { $id: "ReconciliationResult" },
)

export const IssueResultSchema = Type.Union(
  [
    Type.Literal("not_sent"),
    Type.Literal("accepted"),
    Type.Literal("completed"),
    Type.Literal("rejected"),
    Type.Literal("failed"),
    Type.Literal("uncertain"),
  ],
  { $id: "IssueResult" },
)

export const CancellationResultSchema = Type.Union(
  [
    Type.Literal("accepted"),
    Type.Literal("confirmed_stopped"),
    Type.Literal("rejected_still_active"),
    Type.Literal("failed_not_changed"),
    Type.Literal("uncertain"),
  ],
  { $id: "CancellationResult" },
)

export type EffectStatus = Type.Static<typeof EffectStatusSchema>
export type EffectRelation = Type.Static<typeof EffectRelationSchema>
export type EffectRecord = Type.Static<typeof EffectRecordSchema>
export type ReconciliationResult = Type.Static<typeof ReconciliationResultSchema>
export type IssueResult = Type.Static<typeof IssueResultSchema>
export type CancellationResult = Type.Static<typeof CancellationResultSchema>
