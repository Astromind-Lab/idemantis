import type { EdgeStopReason } from "@idemantis/contracts"

export type EdgeAgentErrorCode =
  | "EDGE_NOT_ACTIVATED"
  | "EDGE_ALREADY_ACTIVATED"
  | "EDGE_DELEGATION_INVALID"
  | "EDGE_NOT_YET_VALID"
  | "EDGE_STOPPED"
  | "EDGE_EVENT_COLLISION"
  | "EDGE_EVENT_INVALID"
  | "EDGE_DISPATCH_INVALID"
  | "EDGE_DISPATCH_ALREADY_STARTED"
  | "EDGE_STORE_CLOSED"
  | "EDGE_STORE_CORRUPT"
  | "EDGE_MIGRATION_MISMATCH"

export class EdgeAgentError extends Error {
  override readonly name = "EdgeAgentError"

  constructor(
    readonly code: EdgeAgentErrorCode,
    message: string,
    readonly stop_reason?: EdgeStopReason,
  ) {
    super(message)
  }
}
