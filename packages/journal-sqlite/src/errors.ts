import type { EventId, RunId } from "@idemantis/contracts"

export type JournalErrorCode =
  | "JOURNAL_CLOSED"
  | "RUN_NOT_FOUND"
  | "RUN_ALREADY_EXISTS"
  | "ADMISSION_REQUIRED"
  | "EVENT_ID_COLLISION"
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
