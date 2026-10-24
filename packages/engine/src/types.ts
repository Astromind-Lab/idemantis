import type {
  AttemptId,
  CommandId,
  CommandRecord,
  ClientRequestId,
  Digest,
  EffectId,
  EffectStatus,
  EventId,
  JsonObject,
  JsonValue,
  MissionStatus,
  ObservationId,
  OwnershipHandoffBlockReason,
  OwnershipLease,
  FenceProof,
  HandoffId,
  RunId,
  StateInstanceId,
  TimerId,
} from "@idemantis/contracts"
import type { FinalOutcome, MissionManifest } from "@idemantis/sdk"

export interface MissionRuntimeDefinition {
  manifest: MissionManifest
  definition_digest: Digest
  execution_binding_digest: Digest
}

export interface EffectProjection {
  effect_id: EffectId
  state_instance_id: StateInstanceId
  state_name: string
  effect_slot: string
  operation: string
  status: EffectStatus
  required: boolean
  issue_count: number
  retry_timer_id: TimerId | null
  ownership_epoch: number
}

export interface MissionState {
  run_id: RunId
  definition_digest: Digest
  execution_binding_digest: Digest
  status: MissionStatus
  current_state: string
  current_state_instance_id: StateInstanceId
  current_sequence: number
  ownership_epoch: number
  input: JsonObject
  active_effect_id: EffectId | null
  terminal_outcome: FinalOutcome | null
  effects: Record<string, EffectProjection>
  observations: Record<string, true>
}

export interface ReductionResult {
  state: MissionState
  commands: CommandRecord[]
  state_digest: Digest
}

export interface ReplayFrame {
  sequence: number
  event_id: EventId
  state_digest: Digest
  command_ids: CommandId[]
}

export interface ReplayResult {
  state: MissionState
  commands: CommandRecord[]
  frames: ReplayFrame[]
  state_digest: Digest
}

export type ReductionErrorCode =
  | "INVALID_DEFINITION"
  | "INVALID_EVENT_PAYLOAD"
  | "INVALID_INPUT"
  | "INVALID_PREVIOUS_STATE"
  | "RUN_MISMATCH"
  | "SEQUENCE_MISMATCH"
  | "OWNERSHIP_EPOCH_MISMATCH"
  | "ADMISSION_REQUIRED"
  | "ALREADY_ADMITTED"
  | "UNSUPPORTED_EVENT"
  | "INVALID_TRANSITION"
  | "INVALID_REDUCTION_OUTPUT"
  | "EMPTY_REPLAY"

export type ReducerEventType =
  | "mission.admitted"
  | "effect.dispatch_started"
  | "effect.issue_result_recorded"
  | "effect.reconciliation_result_recorded"
  | "observation.recorded"
  | "timer.fired"
  | "ownership.lease_registered"
  | "ownership.handoff_requested"
  | "ownership.handoff_blocked"
  | "ownership.released"
  | "ownership.fence_activated"
  | "ownership.effect_cancelled"
  | "ownership.activated"
  | "operator.intervention_submitted"

export interface MissionAdmittedPayload {
  definition_digest: Digest
  execution_binding_digest: Digest
  input: JsonObject
}

export interface EffectDispatchStartedPayload {
  attempt_id: AttemptId
  command_id: CommandId
  effect_id: EffectId
}

export type EffectIssueResult =
  | "not_sent"
  | "accepted"
  | "completed"
  | "rejected"
  | "failed"
  | "uncertain"

export interface EffectIssueResultPayload {
  attempt_id: AttemptId
  command_id: CommandId
  effect_id: EffectId
  result: EffectIssueResult
  output?: JsonValue
  external_reference?: string
  evidence?: JsonObject
}

export type EffectReconciliationResult =
  | "confirmed_completed"
  | "confirmed_not_applied"
  | "confirmed_in_progress"
  | "conflicting_state"
  | "unknown"

export interface EffectReconciliationResultPayload {
  attempt_id: AttemptId
  command_id: CommandId
  effect_id: EffectId
  result: EffectReconciliationResult
  output?: JsonValue
  external_reference?: string
  evidence?: JsonObject
}

export interface ObservationRecordedPayload {
  observation_id: ObservationId
  effect_id?: EffectId
  observation_type: string
  subject?: JsonValue
  payload: JsonObject
}

export interface TimerFiredPayload {
  timer_id: TimerId
  state_instance_id: StateInstanceId
}

export interface OwnershipLeaseRegisteredPayload {
  lease: OwnershipLease
}

export interface OwnershipHandoffRequestedPayload {
  handoff_id: HandoffId
  source_lease_id: OwnershipLease["lease_id"]
  target_lease: OwnershipLease
}

export interface OwnershipHandoffBlockedPayload {
  handoff_id: HandoffId
  reason: OwnershipHandoffBlockReason
}

export interface OwnershipReleasedPayload {
  handoff_id: HandoffId
  lease_id: OwnershipLease["lease_id"]
}

export interface OwnershipFenceActivatedPayload {
  handoff_id: HandoffId
  effect_id: EffectId
  proof: FenceProof
}

export interface OwnershipEffectCancelledPayload {
  handoff_id: HandoffId
  effect_id: EffectId
  result: "confirmed_stopped"
}

export interface OwnershipResumeCommand {
  previous_command_id: CommandId
  kind: "effect.issue" | "effect.reconcile"
  payload: JsonObject
}

export interface OwnershipActivatedPayload {
  handoff_id: HandoffId
  lease: OwnershipLease
  resume_commands: OwnershipResumeCommand[]
}

export interface OperatorInterventionSubmittedPayload {
  client_request_id: ClientRequestId
  expected_sequence: number
  action_code: string
  target: string
  input: JsonObject
  reason?: string
}

export type ReducerEvent =
  | { type: "mission.admitted"; payload: MissionAdmittedPayload }
  | { type: "effect.dispatch_started"; payload: EffectDispatchStartedPayload }
  | { type: "effect.issue_result_recorded"; payload: EffectIssueResultPayload }
  | {
      type: "effect.reconciliation_result_recorded"
      payload: EffectReconciliationResultPayload
    }
  | { type: "observation.recorded"; payload: ObservationRecordedPayload }
  | { type: "timer.fired"; payload: TimerFiredPayload }
  | { type: "ownership.lease_registered"; payload: OwnershipLeaseRegisteredPayload }
  | { type: "ownership.handoff_requested"; payload: OwnershipHandoffRequestedPayload }
  | { type: "ownership.handoff_blocked"; payload: OwnershipHandoffBlockedPayload }
  | { type: "ownership.released"; payload: OwnershipReleasedPayload }
  | { type: "ownership.fence_activated"; payload: OwnershipFenceActivatedPayload }
  | { type: "ownership.effect_cancelled"; payload: OwnershipEffectCancelledPayload }
  | { type: "ownership.activated"; payload: OwnershipActivatedPayload }
  | {
      type: "operator.intervention_submitted"
      payload: OperatorInterventionSubmittedPayload
    }
