import type {
  AcceptedEvent,
  AttemptId,
  AttemptRecord,
  CommandId,
  CommandRecord,
  Digest,
  EventAcceptance,
  ProposedEvent,
  RunId,
  SourceId,
  TimerId,
  TimerRecord,
} from "@idemantis/contracts"
import type { MissionRuntimeDefinition, MissionState } from "@idemantis/engine"

export interface JournalOptions {
  path: string
  clock?: () => Date
  busy_timeout_ms?: number
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
  claimed_at: string
  lease_duration_ms: number
}

export interface ClaimedCommand {
  command: CommandRecord
  attempt: AttemptRecord
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
