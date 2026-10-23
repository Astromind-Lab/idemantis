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
  IngressOutcome,
  MergeId,
  ProposedEvent,
  FenceProof,
  EffectId,
  OwnershipHandoffRecord,
  OwnershipLease,
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
