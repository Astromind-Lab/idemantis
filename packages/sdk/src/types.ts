import type {
  AdapterOperationReference,
  ConsequenceLevel,
  Digest,
  EffectClass,
  JsonObject,
  JsonValue,
  OperationCapabilities,
} from "@idemantis/contracts"
import type { TSchema } from "typebox"

export type StateName = string
export type ExecutionOwner = "runner" | "edge"
export type FinalOutcome = "completed" | "cancelled" | "compensated" | "failed" | "unresolved"

/** A serializable data selector or constructor evaluated by the interpreter. */
export type ValueExpression =
  | { kind: "input"; path: string[] }
  | { kind: "literal"; value: JsonValue }
  | { kind: "object"; fields: Record<string, ValueExpression> }

/** The versioned adapter capability contract imported by mission source. */
export interface OperationDefinition {
  key: string
  adapter: string
  operation: string
  schemaVersion: number
  inputSchema: TSchema
  outputSchema: TSchema
  effectClass: EffectClass
  consequence: ConsequenceLevel
  capabilities: OperationCapabilities
}

/** A bounded retry strategy whose safety is checked during compilation. */
export type RetryPolicy =
  | { kind: "none" }
  | { kind: "safe"; maxAttempts: number; backoffMs: number }
  | { kind: "reconcile_before_retry"; maxAttempts: number; backoffMs: number }

export interface CancellationPolicy {
  onCancelled: StateName
  onCompletionDuringCancel?: StateName
}

export interface EffectStateDefinition {
  kind: "effect"
  operation: OperationDefinition
  effectSlot: string
  input: ValueExpression
  required: boolean
  retry: RetryPolicy
  execution: ExecutionOwner
  automaticTakeover: boolean
  activeHandoff: boolean
  cancellation?: CancellationPolicy
  onAccepted?: StateName
  onSettled: StateName
  onFailed: StateName
  onUncertain: StateName
}

export type ObservationCorrelation =
  | { kind: "effect"; effectState: StateName }
  | { kind: "subject"; subject: ValueExpression }

export interface ObservationWaitStateDefinition {
  kind: "observation_wait"
  observationType: string
  schemaVersion: number
  correlation?: ObservationCorrelation
  timeout?: { afterMs: number; onTimeout: StateName }
  onObserved: StateName
}

export interface ReconcileStateDefinition {
  kind: "reconcile"
  effectState: StateName
  operation: OperationDefinition
  subject: ValueExpression
  execution: ExecutionOwner
  onConfirmedCompleted: StateName
  onConfirmedNotApplied: StateName
  onConfirmedInProgress: StateName
  onConflictingState: StateName
  onUnknown: StateName
}

export type InterventionControl = "text" | "textarea" | "select" | "checkbox" | "date-time"

export interface InterventionPresentationHint {
  path: string
  control: InterventionControl
  label?: string
  help?: string
  order?: number
}

export interface OperatorActionDefinition {
  code: string
  consequence: ConsequenceLevel
  confirmationRequired: boolean
  input: TSchema
  presentationHints?: InterventionPresentationHint[]
  evidenceRequired?: boolean
  reasonRequired?: boolean
  expectedOutcomeLabel: string
  onAccepted: StateName
}

export interface OperatorWaitStateDefinition {
  kind: "operator_wait"
  reason: string
  actions: OperatorActionDefinition[]
}

export interface FinalStateDefinition {
  kind: "final"
  outcome: FinalOutcome
}

export type StateDefinition =
  | EffectStateDefinition
  | ObservationWaitStateDefinition
  | ReconcileStateDefinition
  | OperatorWaitStateDefinition
  | FinalStateDefinition

/** The typed authoring input compiled into a callback-free manifest. */
export interface MissionDefinition<Schema extends TSchema = TSchema> {
  id: string
  version: string
  input: Schema
  initial: StateName
  states: Record<StateName, StateDefinition>
}

export interface OperationManifest {
  key: string
  adapter: string
  operation: string
  operation_schema_version: number
  input_schema: JsonObject
  output_schema: JsonObject
  effect_class: EffectClass
  consequence: ConsequenceLevel
  capabilities: OperationCapabilities
}

export type RetryPolicyManifest =
  | { kind: "none" }
  | { kind: "safe"; max_attempts: number; backoff_ms: number }
  | { kind: "reconcile_before_retry"; max_attempts: number; backoff_ms: number }

export interface EffectStateManifest {
  kind: "effect"
  operation: string
  effect_slot: string
  input: ValueExpression
  required: boolean
  retry: RetryPolicyManifest
  execution: ExecutionOwner
  automatic_takeover: boolean
  active_handoff: boolean
  cancellation?: {
    on_cancelled: StateName
    on_completion_during_cancel: StateName
  }
  on_acknowledged?: StateName
  on_settled: StateName
  on_failed: StateName
  on_uncertain: StateName
}

export interface ObservationWaitStateManifest {
  kind: "observation_wait"
  observation_type: string
  schema_version: number
  correlation: ObservationCorrelation
  timeout: { after_ms: number; on_timeout: StateName }
  on_observed: StateName
}

export interface ReconcileStateManifest {
  kind: "reconcile"
  effect_state: StateName
  operation: string
  subject: ValueExpression
  execution: ExecutionOwner
  on_confirmed_completed: StateName
  on_confirmed_not_applied: StateName
  on_confirmed_in_progress: StateName
  on_conflicting_state: StateName
  on_unknown: StateName
}

export interface OperatorActionManifest {
  action_code: string
  consequence: ConsequenceLevel
  confirmation_required: boolean
  input_schema: JsonObject
  presentation_hints: InterventionPresentationHint[]
  evidence_required: boolean
  reason_required: boolean
  expected_outcome_label: string
  on_accepted: StateName
}

export interface OperatorWaitStateManifest {
  kind: "operator_wait"
  reason: string
  actions: OperatorActionManifest[]
}

export interface FinalStateManifest {
  kind: "final"
  outcome: FinalOutcome
}

export type StateManifest =
  | EffectStateManifest
  | ObservationWaitStateManifest
  | ReconcileStateManifest
  | OperatorWaitStateManifest
  | FinalStateManifest

export type MissionGraphNode =
  | { id: StateName; kind: "effect" | "reconcile"; operation: string }
  | { id: StateName; kind: "observation_wait" | "operator_wait" }
  | { id: StateName; kind: "final"; outcome: FinalOutcome }

export interface MissionGraphEdge {
  from: StateName
  to: StateName
  trigger: string
}

/** The deterministic node and transition representation used by inspection tools. */
export interface MissionGraph {
  nodes: MissionGraphNode[]
  edges: MissionGraphEdge[]
}

/** The canonical JSON contract registered for execution and replay. */
export interface MissionManifest {
  manifest_schema_version: 1
  minimum_interpreter_version: "0.1.0"
  definition_id: string
  definition_version: string
  input_schema: JsonObject
  initial: StateName
  operations: Record<string, OperationManifest>
  states: Record<StateName, StateManifest>
  graph: MissionGraph
}

/** A validated manifest and its content identity. */
export interface CompiledMission<Schema extends TSchema = TSchema> {
  inputSchema: Schema
  manifest: MissionManifest
  canonicalManifest: string
  definitionDigest: Digest
}

/** Resolves one logical operation to a concrete adapter instance. */
export interface ExecutionBinding {
  operationKey: string
  reference: AdapterOperationReference
  capabilities: OperationCapabilities
}

export type MissionDefinitionIssueCode =
  | "INVALID_DEFINITION_ID"
  | "INVALID_DEFINITION_VERSION"
  | "INVALID_INPUT_SCHEMA"
  | "INVALID_STATE_NAME"
  | "INVALID_STATE"
  | "INVALID_EXPRESSION"
  | "INVALID_OBSERVATION"
  | "INITIAL_STATE_MISSING"
  | "TRANSITION_TARGET_MISSING"
  | "UNREACHABLE_STATE"
  | "INVALID_OPERATION"
  | "DUPLICATE_OPERATION_KEY"
  | "UNSAFE_RETRY"
  | "RECONCILIATION_UNAVAILABLE"
  | "RECONCILIATION_TARGET_INVALID"
  | "CANCELLATION_UNSUPPORTED"
  | "CANCELLATION_RACE_UNHANDLED"
  | "HANDOFF_UNSUPPORTED"
  | "FENCING_REQUIRED"
  | "EXECUTION_LOCATION_UNSUPPORTED"
  | "OBSERVATION_CORRELATION_REQUIRED"
  | "OBSERVATION_CORRELATION_INVALID"
  | "OBSERVATION_TIMEOUT_REQUIRED"

/** A stable definition diagnostic with a direct remediation. */
export interface MissionDefinitionIssue {
  code: MissionDefinitionIssueCode
  path: string
  message: string
  resolution: string
}

export type ExecutionBindingIssueCode =
  | "INVALID_BINDING"
  | "DUPLICATE_BINDING"
  | "MISSING_BINDING"
  | "UNEXPECTED_BINDING"
  | "BINDING_OPERATION_MISMATCH"
  | "BINDING_SCHEMA_VERSION_MISMATCH"
  | "BINDING_CAPABILITY_MISMATCH"

/** A stable diagnostic for an unsafe or incomplete operation binding. */
export interface ExecutionBindingIssue {
  code: ExecutionBindingIssueCode
  operationKey: string
  message: string
  resolution: string
}
