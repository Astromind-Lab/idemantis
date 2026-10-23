import type {
  EdgeActivationRecord,
  EdgeBranchEvent,
  EdgeDelegation,
  EdgeExecutionScope,
  EdgeReconnectUpload,
  EdgeStopReason,
  AttemptRecord,
  CommandRecord,
  EventId,
  IssueResult,
  OperationName,
  AdapterInstanceId,
  EffectClass,
  ProposedEvent,
  SourceId,
} from "@idemantis/contracts"
import type { EdgeStore } from "./store.js"

export interface EdgeClock {
  wallNow(): Date
  monotonicNowMs(): number
}

export interface EdgeAgentOptions {
  edge_id: SourceId
  boot_identity: SourceId
  clock: EdgeClock
  store: EdgeStore
}

export interface EdgeActivationOptions {
  measured_clock_uncertainty_ms: number
}

export interface EdgeAppendOptions {
  execution?: EdgeExecutionScope
}

export interface EdgeAppendResult {
  event: EdgeBranchEvent
  duplicate: boolean
}

export interface EdgeAgentState {
  delegation: EdgeDelegation
  activation: EdgeActivationRecord
  stopped_reason?: EdgeStopReason
  last_sequence: number
  dispatch_count: number
}

export interface StoredEdgeSession extends EdgeAgentState {
  last_branch_digest?: string
}

export interface EdgeAppendInput {
  proposal: ProposedEvent
  recorded_at: string
  execution?: EdgeExecutionScope
}

export type EdgeUpload = EdgeReconnectUpload

export interface EdgeEffectExecutor {
  binding(operation: OperationName): {
    operation_key: string
    adapter_instance_id: AdapterInstanceId
    effect_class: EffectClass
    execution_location: "runner" | "edge" | "either"
  }
  issue(command: CommandRecord, attempt: AttemptRecord): Promise<IssueResult>
}

export interface EdgeDispatchRequest {
  state: EdgeExecutionScope["state"]
  command: CommandRecord
  attempt: AttemptRecord
  executor: EdgeEffectExecutor
  dispatch_started_event_id: EventId
  result_event_id: EventId
}

export interface EdgeDispatchOutcome {
  dispatch_started: EdgeAppendResult
  result_recorded: EdgeAppendResult
  result: IssueResult
}
