import type {
  AcceptedEvent,
  AttemptId,
  AttemptRecord,
  CommandId,
  CommandRecord,
  ClientRequestId,
  Digest,
  EdgeMergeRequest,
  EventAcceptance,
  FenceProof,
  EffectId,
  IngressOutcome,
  JsonObject,
  MergeId,
  OwnershipHandoffRecord,
  OwnershipLease,
  ProposedEvent,
  RunId,
  SourceId,
  TimerId,
  TimerRecord,
} from "@idemantis/contracts"
import type { MissionRuntimeDefinition, MissionState } from "@idemantis/engine"

export type { EdgeMergeEventMapping, EdgeMergeResult } from "@idemantis/contracts"

export interface JournalOptions {
  path: string
  clock?: () => Date
  busy_timeout_ms?: number
}

export type PendingInputKind = "external_fact" | "timer" | "intent"

export interface QueuePendingInputOptions {
  client_request_id: ClientRequestId
}

export interface PendingInputRecord {
  ingress_sequence: number
  run_id: RunId
  client_request_id: ClientRequestId
  input_kind: PendingInputKind
  proposal: ProposedEvent
  proposal_digest: Digest
  expected_sequence?: number
  outcome: IngressOutcome
  accepted_sequence?: number
  received_at: string
  resolved_at?: string
}

export interface QuarantinedEdgeMerge {
  quarantine_id: number
  merge_id: MergeId
  run_id: RunId
  merge_digest: Digest
  reason_code: string
  detail: string
  request: EdgeMergeRequest
  quarantined_at: string
}

export interface AppendResult {
  acceptance: EventAcceptance
  duplicate: boolean
  state_digest: Digest
  command_ids: CommandId[]
}

export interface StoredMission {
  runtime: MissionRuntimeDefinition
  state: MissionState
  state_digest: Digest
  created_at: string
  updated_at: string
}

export interface MissionHead {
  run_id: RunId
  definition_digest: Digest
  current_sequence: number
  state_digest: Digest
  updated_at: string
}

export interface StoredDefinition {
  runtime: MissionRuntimeDefinition
  subject_reference_keys: string[]
  registered_at: string
  updated_at: string
}

export interface RegisterDefinitionOptions {
  subject_reference_keys?: readonly string[]
}

export interface StoredEvent {
  event: AcceptedEvent
  proposal_digest: Digest
  state_digest: Digest
}

export interface MissionListChange {
  revision: number
  run_id: RunId
}

export interface MissionSnapshot {
  run_id: RunId
  sequence: number
  definition_digest: Digest
  state_digest: Digest
  state: MissionState
  created_at: string
}

export interface StorageInfo {
  schema_version: number
  journal_mode: string
  foreign_keys: boolean
}

export type PreparedInterventionState = "prepared" | "submitted" | "discarded" | "expired"

export interface StoredPreparedIntervention {
  intent_sequence: number
  client_request_id: ClientRequestId
  actor_id: SourceId
  run_id: RunId
  expected_sequence: number
  action_code: string
  target: string
  normalized_input: JsonObject
  reason?: string
  proposal_digest: Digest
  proposal: ProposedEvent
  prepared_at: string
  expires_at: string
  state: PreparedInterventionState
  compatibility: "compatible" | "incompatible"
}

export type StoredInterventionOutcome =
  | "queued"
  | "accepted"
  | "rejected_stale"
  | "rejected_invalid"

export interface StoredInterventionReceipt {
  client_request_id: ClientRequestId
  proposal_digest: Digest
  run_id: RunId
  expected_sequence: number
  outcome: StoredInterventionOutcome
  accepted_sequence?: number
  received_at: string
  resolved_at?: string
  revision: number
}

export interface StoredReceiptUpdate {
  revision: number
  run_id: RunId
  client_request_id: ClientRequestId
  receipt: StoredInterventionReceipt
  recorded_at: string
}

export type SubmitInterventionResult =
  | { status: "receipt"; receipt: StoredInterventionReceipt }
  | { status: "expired"; prepared: StoredPreparedIntervention }

export interface ReplayVerification {
  run_id: RunId
  event_count: number
  command_count: number
  snapshots_verified: number
  state_digest: Digest
}

export interface ProjectionRebuildOptions {
  use_snapshot?: boolean
}

export interface CommandClaimOptions {
  attempt_id: AttemptId
  claimed_by: SourceId
  owner_mode: "runner" | "edge"
  claimed_at: string
  lease_duration_ms: number
}

export interface ClaimedCommand {
  command: CommandRecord
  attempt: AttemptRecord
}

export interface StoredOwnershipLease {
  lease: OwnershipLease
  status: "active" | "released"
  activated_event_id: AcceptedEvent["event_id"]
  released_event_id?: AcceptedEvent["event_id"]
  created_at: string
  updated_at: string
}

export interface OwnershipState {
  claims_frozen: boolean
  active_lease?: StoredOwnershipLease
  handoff?: OwnershipHandoffRecord
  leases: StoredOwnershipLease[]
  fences: OwnershipFenceRecord[]
}

export interface OwnershipFenceRecord {
  effect_id: EffectId
  proof: FenceProof
}

export interface TimerFireOptions {
  timer_id: TimerId
  event_id: AcceptedEvent["event_id"]
  source_id: SourceId
  fired_at: string
}

export type TimerFireResult =
  | { status: "fired"; append: AppendResult; timer: TimerRecord }
  | { status: "superseded"; timer: TimerRecord }

export type JournalProposal = ProposedEvent
export type JournalEvent = AcceptedEvent
export type JournalCommand = CommandRecord
