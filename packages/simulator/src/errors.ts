export type SimulationErrorCode =
  | "INVALID_SEED"
  | "INVALID_TIME"
  | "INVALID_SCHEDULE"
  | "INVALID_FAULT"
  | "DUPLICATE_FAULT"
  | "QUEUE_LIMIT_EXCEEDED"
  | "TARGET_STATE_INVALID"
  | "SCENARIO_INVARIANT_FAILED"

export class SimulationError extends Error {
  override readonly name = "SimulationError"

  constructor(
    readonly code: SimulationErrorCode,
    message: string,
  ) {
    super(message)
  }
}
