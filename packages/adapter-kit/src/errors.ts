export type AdapterKitErrorCode =
  | "INVALID_DESCRIPTOR"
  | "INVALID_CAPABILITY_COMBINATION"
  | "INVALID_CAPABILITY_MANIFEST"
  | "DUPLICATE_OPERATION"
  | "OPERATION_NOT_FOUND"
  | "COMMAND_INVALID"
  | "INPUT_INVALID"
  | "SUBJECT_INVALID"
  | "RESULT_INVALID"
  | "OUTPUT_INVALID"
  | "RECONCILIATION_UNAVAILABLE"
  | "CANCELLATION_UNAVAILABLE"
  | "COMPENSATION_UNAVAILABLE"
  | "FENCING_UNAVAILABLE"
  | "FENCE_PROOF_INVALID"
  | "CONFORMANCE_FAILED"

export class AdapterKitError extends Error {
  override readonly name = "AdapterKitError"

  constructor(
    readonly code: AdapterKitErrorCode,
    message: string,
    readonly operation_key?: string,
  ) {
    super(message)
  }
}
