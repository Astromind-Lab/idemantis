export type HttpAdapterErrorCode =
  | "CONNECTION_INVALID"
  | "MANIFEST_INVALID"
  | "RESPONSE_REJECTED"
  | "RESPONSE_INVALID"

export class HttpAdapterError extends Error {
  readonly code: HttpAdapterErrorCode

  constructor(code: HttpAdapterErrorCode, message: string) {
    super(message)
    this.name = "HttpAdapterError"
    this.code = code
  }
}
