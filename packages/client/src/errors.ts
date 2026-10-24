export class IdemantisApiError extends Error {
  readonly status: number
  readonly code: string
  readonly request_id: string | undefined

  constructor(status: number, code: string, message: string, requestId?: string) {
    super(message)
    this.name = "IdemantisApiError"
    this.status = status
    this.code = code
    this.request_id = requestId
  }
}
