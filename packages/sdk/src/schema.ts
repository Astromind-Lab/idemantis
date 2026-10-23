import {
  ConsequenceLevelSchema,
  DefinitionIdSchema,
  DefinitionVersionSchema,
  EffectClassSchema,
  JsonObjectSchema,
  JsonValueSchema,
  OperationCapabilitiesSchema,
  OperationNameSchema,
} from "@idemantis/contracts"
import Type from "typebox"
import type { MissionManifest, ValueExpression } from "./types.js"

const stateNamePattern = "^[a-z][a-zA-Z0-9]*(?:[._-][a-zA-Z0-9]+)*$"
const adapterNamePattern = "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$"

const StateNameSchema = Type.String({ pattern: stateNamePattern })

/** Runtime schema for the expression language embedded in manifests. */
export const ValueExpressionSchema = Type.Unsafe<ValueExpression>(
  Type.Cyclic(
    {
      ValueExpression: Type.Union([
        Type.Object(
          {
            kind: Type.Literal("input"),
            path: Type.Array(Type.String({ minLength: 1 }), { minItems: 1 }),
          },
          { additionalProperties: false },
        ),
        Type.Object(
          {
            kind: Type.Literal("literal"),
            value: JsonValueSchema,
          },
          { additionalProperties: false },
        ),
        Type.Object(
          {
            kind: Type.Literal("object"),
            fields: Type.Record(Type.String({ minLength: 1 }), Type.Ref("ValueExpression")),
          },
          { additionalProperties: false },
        ),
      ]),
    },
    "ValueExpression",
  ),
)

/** Runtime schema for one logical adapter operation requirement. */
export const OperationManifestSchema = Type.Object(
  {
    key: StateNameSchema,
    adapter: Type.String({ pattern: adapterNamePattern }),
    operation: OperationNameSchema,
    operation_schema_version: Type.Integer({ minimum: 1 }),
    input_schema: JsonObjectSchema,
    output_schema: JsonObjectSchema,
    effect_class: EffectClassSchema,
    consequence: ConsequenceLevelSchema,
    capabilities: OperationCapabilitiesSchema,
  },
  { $id: "OperationManifest", additionalProperties: false },
)

const RetryPolicyManifestSchema = Type.Union([
  Type.Object({ kind: Type.Literal("none") }, { additionalProperties: false }),
  Type.Object(
    {
      kind: Type.Literal("safe"),
      max_attempts: Type.Integer({ minimum: 2 }),
      backoff_ms: Type.Integer({ minimum: 0 }),
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      kind: Type.Literal("reconcile_before_retry"),
      max_attempts: Type.Integer({ minimum: 2 }),
      backoff_ms: Type.Integer({ minimum: 0 }),
    },
    { additionalProperties: false },
  ),
])

const ExecutionOwnerSchema = Type.Union([Type.Literal("runner"), Type.Literal("edge")])

const EffectStateManifestSchema = Type.Object(
  {
    kind: Type.Literal("effect"),
    operation: StateNameSchema,
    effect_slot: StateNameSchema,
    input: ValueExpressionSchema,
    required: Type.Boolean(),
    retry: RetryPolicyManifestSchema,
    execution: ExecutionOwnerSchema,
    automatic_takeover: Type.Boolean(),
    active_handoff: Type.Boolean(),
    cancellation: Type.Optional(
      Type.Object(
        {
          on_cancelled: StateNameSchema,
          on_completion_during_cancel: StateNameSchema,
        },
        { additionalProperties: false },
      ),
    ),
    on_acknowledged: Type.Optional(StateNameSchema),
    on_settled: StateNameSchema,
    on_failed: StateNameSchema,
    on_uncertain: StateNameSchema,
  },
  { additionalProperties: false },
)

const ObservationCorrelationSchema = Type.Union([
  Type.Object(
    {
      kind: Type.Literal("effect"),
      effectState: StateNameSchema,
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      kind: Type.Literal("subject"),
      subject: ValueExpressionSchema,
    },
    { additionalProperties: false },
  ),
])

const ObservationWaitStateManifestSchema = Type.Object(
  {
    kind: Type.Literal("observation_wait"),
    observation_type: Type.String({
      minLength: 3,
      maxLength: 128,
      pattern: "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)+$",
    }),
    schema_version: Type.Integer({ minimum: 1 }),
    correlation: ObservationCorrelationSchema,
    timeout: Type.Object(
      {
        after_ms: Type.Integer({ minimum: 1 }),
        on_timeout: StateNameSchema,
      },
      { additionalProperties: false },
    ),
    on_observed: StateNameSchema,
  },
  { additionalProperties: false },
)

const ReconcileStateManifestSchema = Type.Object(
  {
    kind: Type.Literal("reconcile"),
    effect_state: StateNameSchema,
    operation: StateNameSchema,
    subject: ValueExpressionSchema,
    execution: ExecutionOwnerSchema,
    on_confirmed_completed: StateNameSchema,
    on_confirmed_not_applied: StateNameSchema,
    on_confirmed_in_progress: StateNameSchema,
    on_conflicting_state: StateNameSchema,
    on_unknown: StateNameSchema,
  },
  { additionalProperties: false },
)

const FinalStateManifestSchema = Type.Object(
  {
    kind: Type.Literal("final"),
    outcome: Type.Union([
      Type.Literal("completed"),
      Type.Literal("cancelled"),
      Type.Literal("compensated"),
      Type.Literal("failed"),
      Type.Literal("unresolved"),
    ]),
  },
  { additionalProperties: false },
)

const StateManifestSchema = Type.Union([
  EffectStateManifestSchema,
  ObservationWaitStateManifestSchema,
  ReconcileStateManifestSchema,
  FinalStateManifestSchema,
])

const MissionGraphNodeSchema = Type.Union([
  Type.Object(
    {
      id: StateNameSchema,
      kind: Type.Union([Type.Literal("effect"), Type.Literal("reconcile")]),
      operation: StateNameSchema,
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      id: StateNameSchema,
      kind: Type.Literal("observation_wait"),
    },
    { additionalProperties: false },
  ),
  Type.Object(
    {
      id: StateNameSchema,
      kind: Type.Literal("final"),
      outcome: Type.Union([
        Type.Literal("completed"),
        Type.Literal("cancelled"),
        Type.Literal("compensated"),
        Type.Literal("failed"),
        Type.Literal("unresolved"),
      ]),
    },
    { additionalProperties: false },
  ),
])

const MissionGraphSchema = Type.Object(
  {
    nodes: Type.Array(MissionGraphNodeSchema),
    edges: Type.Array(
      Type.Object(
        {
          from: StateNameSchema,
          to: StateNameSchema,
          trigger: Type.String({ minLength: 1, maxLength: 128 }),
        },
        { additionalProperties: false },
      ),
    ),
  },
  { additionalProperties: false },
)

/** Runtime schema for manifests accepted by the deterministic interpreter. */
export const MissionManifestSchema = Type.Unsafe<MissionManifest>(
  Type.Object(
    {
      manifest_schema_version: Type.Literal(1),
      minimum_interpreter_version: Type.Literal("0.1.0"),
      definition_id: DefinitionIdSchema,
      definition_version: DefinitionVersionSchema,
      input_schema: JsonObjectSchema,
      initial: StateNameSchema,
      operations: Type.Record(StateNameSchema, OperationManifestSchema),
      states: Type.Record(StateNameSchema, StateManifestSchema),
      graph: MissionGraphSchema,
    },
    { $id: "MissionManifest", additionalProperties: false },
  ),
)
