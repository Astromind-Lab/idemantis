export type TransportErrorCode =
  | "RUNNER_UNAVAILABLE"
  | "RUN_NOT_FOUND"
  | "ACTION_FORBIDDEN"
  | "STALE_SEQUENCE"
  | "INCOMPATIBLE_SCHEMA"
  | "CURSOR_EXPIRED"
  | "CURSOR_INVALID"
  | "CLIENT_REQUEST_ID_COLLISION"
  | "REQUEST_OUTCOME_UNKNOWN"
  | "REDACTED_DATA"
  | "REPLAY_DIVERGENCE"
  | "SESSION_EXPIRED"
  | "DEVELOPMENT_CAPABILITY_DISABLED"
  | "INVALID_REQUEST"

export class TransportError extends Error {
  readonly code: TransportErrorCode
  readonly status: number
  readonly details?: Record<string, unknown>

  constructor(
    code: TransportErrorCode,
    message: string,
    status: number,
    details?: Record<string, unknown>,
  ) {
    super(message)
    this.name = "TransportError"
    this.code = code
    this.status = status
    if (details !== undefined) {
      this.details = details
    }
  }
}
