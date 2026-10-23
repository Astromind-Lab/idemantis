import type { EventId } from "@idemantis/contracts"
import type { ReductionErrorCode } from "./types.js"

export class ReductionError extends Error {
  override readonly name = "ReductionError"

  constructor(
    readonly code: ReductionErrorCode,
    message: string,
    readonly eventId?: EventId,
  ) {
    super(message)
  }
}
