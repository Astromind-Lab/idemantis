import type {
  AdapterInstanceId,
  AttemptId,
  CancellationResult,
  CommandId,
  Digest,
  EffectId,
  FenceProof,
  JsonObject,
  JsonValue,
  RunId,
  SourceId,
} from "@idemantis/contracts"
import type { Static, TSchema } from "typebox"
import type {
  AdapterCapabilityManifestSchema,
  AdapterOperationDescriptorSchema,
} from "./schemas.js"

export type AdapterOperationDescriptor = Static<typeof AdapterOperationDescriptorSchema>
export type AdapterCapabilityManifest = Static<typeof AdapterCapabilityManifestSchema>

export interface AdapterInvocationContext {
  adapter_instance_id: AdapterInstanceId
  run_id: RunId
  command_id: CommandId
  attempt_id: AttemptId
  effect_id: EffectId
  ownership_epoch: number
  deadline: string
  source_id: SourceId
  signal: AbortSignal
}

export type IssueResultEnvelope<Output = JsonValue> =
  | { result: "not_sent"; evidence?: JsonObject }
  | { result: "accepted"; external_reference: string; evidence?: JsonObject }
  | {
      result: "completed"
      output: Output
      external_reference?: string
      evidence?: JsonObject
    }
  | { result: "rejected"; evidence?: JsonObject }
  | { result: "failed"; evidence?: JsonObject }
  | { result: "uncertain"; evidence?: JsonObject }

export type ReconciliationResultEnvelope<Output = JsonValue> =
  | {
      result: "confirmed_completed"
      output: Output
      external_reference?: string
      evidence?: JsonObject
    }
  | { result: "confirmed_not_applied"; evidence?: JsonObject }
  | { result: "confirmed_in_progress"; external_reference?: string; evidence?: JsonObject }
  | { result: "conflicting_state"; evidence?: JsonObject }
  | { result: "unknown"; evidence?: JsonObject }

export interface CancellationResultEnvelope {
  result: CancellationResult
  external_reference?: string
  evidence?: JsonObject
}

export interface FenceActivationContext {
  adapter_instance_id: AdapterInstanceId
  operation_key: string
  scope_reference: string
  ownership_epoch: number
  requested_at: string
  signal: AbortSignal
}

export interface AdapterOperation<Input = JsonValue, Output = JsonValue, Subject = JsonValue> {
  descriptor: AdapterOperationDescriptor
  issue(context: AdapterInvocationContext, input: Input): Promise<IssueResultEnvelope<Output>>
  reconcile?(
    context: AdapterInvocationContext,
    subject: Subject,
  ): Promise<ReconciliationResultEnvelope<Output>>
  cancel?(context: AdapterInvocationContext, subject: Subject): Promise<CancellationResultEnvelope>
  compensate?(
    context: AdapterInvocationContext,
    subject: Subject,
  ): Promise<IssueResultEnvelope<Output>>
  activateFence?(context: FenceActivationContext): Promise<FenceProof>
}

export interface AdapterOperationDefinition<
  InputSchema extends TSchema,
  OutputSchema extends TSchema,
  SubjectSchema extends TSchema,
> {
  key: string
  adapter: string
  operation: string
  schemaVersion: number
  inputSchema: InputSchema
  outputSchema: OutputSchema
  subjectSchema?: SubjectSchema
  effectClass: AdapterOperationDescriptor["effect_class"]
  consequence: AdapterOperationDescriptor["consequence"]
  capabilities: AdapterOperationDescriptor["capabilities"]
  timeout: { minimumMs: number; defaultMs: number; maximumMs: number }
  rateLimit?: { maximumRequests: number; windowMs: number }
  reconciliationLookupFields?: string[]
  observationTypes?: string[]
  observationCorrelationFields?: string[]
  secretRequirements?: Array<{ name: string; required: boolean }>
  maximumFenceActivationDelayMs?: number
  issue(
    context: AdapterInvocationContext,
    input: Static<InputSchema>,
  ): Promise<IssueResultEnvelope<Static<OutputSchema>>>
  reconcile?(
    context: AdapterInvocationContext,
    subject: Static<SubjectSchema>,
  ): Promise<ReconciliationResultEnvelope<Static<OutputSchema>>>
  cancel?(
    context: AdapterInvocationContext,
    subject: Static<SubjectSchema>,
  ): Promise<CancellationResultEnvelope>
  compensate?(
    context: AdapterInvocationContext,
    subject: Static<SubjectSchema>,
  ): Promise<IssueResultEnvelope<Static<OutputSchema>>>
  activateFence?(context: FenceActivationContext): Promise<FenceProof>
}

export interface AdapterCapabilityBundle {
  manifest: AdapterCapabilityManifest
  canonical_manifest: string
  capability_manifest_digest: Digest
}

export interface AdapterRegistration {
  adapter_instance_id: AdapterInstanceId
  operation: AdapterOperation<unknown, unknown, unknown>
}

export interface AdapterBinding {
  operationKey: string
  reference: {
    adapter_instance_id: AdapterInstanceId
    adapter_package_digest: Digest
    capability_manifest_digest: Digest
    operation: string
    operation_schema_version: number
  }
  capabilities: AdapterOperationDescriptor["capabilities"]
}

export interface RuntimeClock {
  now(): Date
}

export interface AdapterExecutorOptions {
  registrations: readonly AdapterRegistration[]
  source_id: SourceId
  location: "runner" | "edge"
  clock?: RuntimeClock
}

export type UncertainRecoveryDecision = "retry_same_effect" | "reconcile" | "stop_uncertain"
export type ActiveHandoffDecision = "transfer" | "settle" | "cancel" | "block"

export interface AdapterTargetMetrics {
  issue_requests: number
  reconciliation_requests: number
  applied_effects: number
  applied_effect_ids: EffectId[]
  stale_rejections: number
}

export interface DuplicateSafetyConformanceOptions<Input, Output, Subject> {
  operation: AdapterOperation<Input, Output, Subject>
  input: Input
  subject: Subject
  first_context: AdapterInvocationContext
  retry_context: AdapterInvocationContext
  reconciliation_context: AdapterInvocationContext
  inspect(): AdapterTargetMetrics | Promise<AdapterTargetMetrics>
}

export interface DuplicateSafetyConformanceReport {
  operation_key: string
  duplicate_safety: AdapterOperationDescriptor["capabilities"]["duplicate_safety"]
  decision: UncertainRecoveryDecision
  first_result: string
  recovery_result?: string
  metrics: AdapterTargetMetrics
  passed: true
}

export interface FenceConformanceOptions<Input, Output, Subject> {
  operation: AdapterOperation<Input, Output, Subject>
  input: Input
  stale_context: AdapterInvocationContext
  activation_context: FenceActivationContext
  wait_until_delayed(): Promise<void>
  release_delayed(): void
  inspect(): AdapterTargetMetrics | Promise<AdapterTargetMetrics>
}

export interface FenceConformanceReport {
  operation_key: string
  stale_result: string
  proof: FenceProof
  metrics: AdapterTargetMetrics
  passed: true
}
