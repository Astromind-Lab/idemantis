import type {
  AttemptId,
  AttemptRecord,
  AdapterInstanceId,
  CancellationResult,
  CommandRecord,
  EventId,
  FenceProof,
  HandoffId,
  IssueResult,
  ReconciliationResult,
  RunId,
  SourceId,
  OwnershipHandoffBlockReason,
  OwnershipHandoffRecord,
  OwnershipLease,
  OperationCapabilities,
  EffectId,
  EffectStatus,
} from "@idemantis/contracts"
import type { AppendResult, SqliteJournal, TimerFireResult } from "@idemantis/journal-sqlite"

export interface Clock {
  now(): Date
}

export interface IdentityFactory {
  attemptId(): AttemptId
  eventId(): EventId
}

export interface ExecutionReport<Result> {
  result: Result
  output?: unknown
  external_reference?: string
  evidence?: unknown
}

export interface EffectExecutor {
  issue(command: CommandRecord, attempt: AttemptRecord): Promise<IssueResult>
  reconcile(command: CommandRecord, attempt: AttemptRecord): Promise<ReconciliationResult>
  invokeIssue?(
    command: CommandRecord,
    attempt: AttemptRecord,
  ): Promise<ExecutionReport<IssueResult>>
  invokeReconciliation?(
    command: CommandRecord,
    attempt: AttemptRecord,
  ): Promise<ExecutionReport<ReconciliationResult>>
}

export interface HandoffEffect {
  effect_id: EffectId
  operation: string
  status: EffectStatus
}

export interface HandoffFenceRequest {
  run_id: RunId
  effect_id: EffectId
  ownership_epoch: number
  requested_at: string
}

export interface OwnershipHandoffBinding {
  capabilities: OperationCapabilities
  adapter_instance_id?: AdapterInstanceId
  fence_scope_reference?: string
  activate_fence?: (request: HandoffFenceRequest) => Promise<FenceProof>
  cancel?: (effect: HandoffEffect) => Promise<CancellationResult>
}

export interface OwnershipBindingResolver {
  resolve(effect: HandoffEffect): OwnershipHandoffBinding
}

export interface OwnershipCoordinatorOptions {
  journal: SqliteJournal
  source_id: SourceId
  bindings: OwnershipBindingResolver
  clock?: Clock
  identities?: Pick<IdentityFactory, "eventId">
}

export interface OwnershipHandoffRequest {
  handoff_id: HandoffId
  target_lease: OwnershipLease
}

export type OwnershipHandoffOutcome =
  | { status: "active"; handoff: OwnershipHandoffRecord; fence_proofs: FenceProof[] }
  | {
      status: "blocked"
      handoff: OwnershipHandoffRecord
      reason: OwnershipHandoffBlockReason
    }

export interface CommandDispatcherOptions {
  journal: SqliteJournal
  executor: EffectExecutor
  worker_id: SourceId
  owner_mode?: "runner" | "edge"
  lease_duration_ms: number
  clock?: Clock
  identities?: IdentityFactory
  reconcile_retry?: { attempts: number; backoff_ms: number }
}

export interface TimerDispatcherOptions {
  journal: SqliteJournal
  source_id: SourceId
  clock?: Clock
  identities?: IdentityFactory
}

export type DispatchOutcome =
  | { status: "idle" }
  | {
      status: "recorded"
      run_id: RunId
      command: CommandRecord
      attempt: AttemptRecord
      dispatch_started: AppendResult
      result_recorded: AppendResult
      result: IssueResult | ReconciliationResult
    }

export interface InterruptedDispatchRecovery {
  run_id: RunId
  command: CommandRecord
  attempt: AttemptRecord
  result_recorded: AppendResult
  result: IssueResult | ReconciliationResult
}

export interface TimerDispatchOutcome {
  fired_at: string
  results: TimerFireResult[]
}
