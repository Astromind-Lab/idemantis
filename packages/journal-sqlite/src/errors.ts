import type { EventId, RunId } from "@idemantis/contracts"

export type JournalErrorCode =
  | "JOURNAL_CLOSED"
  | "RUN_NOT_FOUND"
  | "RUN_ALREADY_EXISTS"
  | "ADMISSION_REQUIRED"
  | "EVENT_ID_COLLISION"
  | "COMMAND_NOT_FOUND"
  | "COMMAND_NOT_CLAIMABLE"
  | "COMMAND_OWNERSHIP_STALE"
  | "ATTEMPT_NOT_FOUND"
  | "ATTEMPT_ID_COLLISION"
  | "ATTEMPT_NOT_ACTIVE"
  | "ATTEMPT_STATE_INVALID"
  | "DISPATCH_EVENT_INVALID"
  | "TIMER_NOT_FOUND"
  | "TIMER_NOT_DUE"
  | "TIMER_STATE_INVALID"
  | "CORRUPT_RECORD"
  | "MIGRATION_MISMATCH"
  | "REPLAY_DIVERGENCE"

export class JournalError extends Error {
  override readonly name = "JournalError"

  constructor(
    readonly code: JournalErrorCode,
    message: string,
    readonly context: { run_id?: RunId; event_id?: EventId } = {},
  ) {
    super(message)
  }
}
