import type {
  AttemptId,
  AttemptRecord,
  CommandRecord,
  EventId,
  IssueResult,
  ReconciliationResult,
  RunId,
  SourceId,
} from "@idemantis/contracts"
import type { AppendResult, SqliteJournal, TimerFireResult } from "@idemantis/journal-sqlite"

export interface Clock {
  now(): Date
}

export interface IdentityFactory {
  attemptId(): AttemptId
  eventId(): EventId
}

export interface EffectExecutor {
  issue(command: CommandRecord, attempt: AttemptRecord): Promise<IssueResult>
  reconcile(command: CommandRecord, attempt: AttemptRecord): Promise<ReconciliationResult>
}

export interface CommandDispatcherOptions {
  journal: SqliteJournal
  executor: EffectExecutor
  worker_id: SourceId
  lease_duration_ms: number
  clock?: Clock
  identities?: IdentityFactory
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

export interface TimerDispatchOutcome {
  fired_at: string
  results: TimerFireResult[]
}
