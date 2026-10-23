import Type from "typebox"
import { AdapterInstanceIdSchema, DigestSchema, OperationNameSchema } from "./ids.js"
import { DurationMsSchema, TimestampSchema } from "./time.js"

export const DuplicateSafetySchema = Type.Union(
  [
    Type.Literal("native_idempotency"),
    Type.Literal("keyed_uniqueness"),
    Type.Literal("reconciliation_only"),
    Type.Literal("none"),
  ],
  { $id: "DuplicateSafety" },
)

export const ReconciliationCapabilitySchema = Type.Union(
  [Type.Literal("authoritative"), Type.Literal("best_effort"), Type.Literal("none")],
  { $id: "ReconciliationCapability" },
)

export const FencingModeSchema = Type.Union(
  [
    Type.Literal("application_epoch"),
    Type.Literal("exclusive_session"),
    Type.Literal("expiring_credential"),
    Type.Literal("none"),
  ],
  { $id: "FencingMode" },
)

export const FencingScopeSchema = Type.Union(
  [
    Type.Literal("adapter_instance"),
    Type.Literal("resource"),
    Type.Literal("subject"),
    Type.Literal("operation"),
  ],
  { $id: "FencingScope" },
)

export const HandoffModeSchema = Type.Union(
  [
    Type.Literal("transferable"),
    Type.Literal("settle_first"),
    Type.Literal("cancel_first"),
    Type.Literal("unsupported"),
  ],
  { $id: "HandoffMode" },
)

export const ExecutionLocationSchema = Type.Union(
  [Type.Literal("runner"), Type.Literal("edge"), Type.Literal("either")],
  { $id: "ExecutionLocation" },
)

export const EffectClassSchema = Type.Union(
  [
    Type.Literal("physical_actuation"),
    Type.Literal("resource_reservation"),
    Type.Literal("access_mutation"),
    Type.Literal("financial"),
    Type.Literal("record_mutation"),
    Type.Literal("notification"),
    Type.Literal("observation"),
  ],
  { $id: "EffectClass" },
)

export const ConsequenceLevelSchema = Type.Union(
  [
    Type.Literal("reversible"),
    Type.Literal("compensatable"),
    Type.Literal("irreversible"),
    Type.Literal("observational"),
  ],
  { $id: "ConsequenceLevel" },
)

export const AdapterOperationReferenceSchema = Type.Object(
  {
    adapter_instance_id: AdapterInstanceIdSchema,
    adapter_package_digest: DigestSchema,
    capability_manifest_digest: DigestSchema,
    operation: OperationNameSchema,
    operation_schema_version: Type.Integer({ minimum: 1 }),
  },
  { $id: "AdapterOperationReference", additionalProperties: false },
)

/** Declares the safety guarantees an adapter provides for one operation. */
export const OperationCapabilitiesSchema = Type.Object(
  {
    duplicate_safety: DuplicateSafetySchema,
    reconciliation: ReconciliationCapabilitySchema,
    fencing_mode: FencingModeSchema,
    fencing_scope: FencingScopeSchema,
    handoff_mode: HandoffModeSchema,
    execution_location: ExecutionLocationSchema,
    cancellation: Type.Boolean(),
    compensation: Type.Boolean(),
    observation_stream: Type.Boolean(),
  },
  { $id: "OperationCapabilities", additionalProperties: false },
)

/** Proves that an ownership epoch is active for a bounded adapter scope. */
export const FenceProofSchema = Type.Object(
  {
    adapter_instance_id: AdapterInstanceIdSchema,
    scope: FencingScopeSchema,
    scope_reference: Type.String({ minLength: 1, maxLength: 256 }),
    ownership_epoch: Type.Integer({ minimum: 1 }),
    activation_reference: Type.String({ minLength: 1, maxLength: 256 }),
    effective_at: TimestampSchema,
    maximum_activation_delay_ms: DurationMsSchema,
    evidence_digest: DigestSchema,
  },
  { $id: "FenceProof", additionalProperties: false },
)

export type DuplicateSafety = Type.Static<typeof DuplicateSafetySchema>
export type ReconciliationCapability = Type.Static<typeof ReconciliationCapabilitySchema>
export type FencingMode = Type.Static<typeof FencingModeSchema>
export type FencingScope = Type.Static<typeof FencingScopeSchema>
export type HandoffMode = Type.Static<typeof HandoffModeSchema>
export type ExecutionLocation = Type.Static<typeof ExecutionLocationSchema>
export type EffectClass = Type.Static<typeof EffectClassSchema>
export type ConsequenceLevel = Type.Static<typeof ConsequenceLevelSchema>
export type AdapterOperationReference = Type.Static<typeof AdapterOperationReferenceSchema>
export type OperationCapabilities = Type.Static<typeof OperationCapabilitiesSchema>
export type FenceProof = Type.Static<typeof FenceProofSchema>
