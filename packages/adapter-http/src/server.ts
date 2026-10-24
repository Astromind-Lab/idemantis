import { timingSafeEqual } from "node:crypto"
import type { IncomingMessage, ServerResponse } from "node:http"
import {
  type AdapterCapabilityBundle,
  AdapterKitError,
  type AdapterOperation,
  buildCapabilityManifest,
} from "@idemantis/adapter-kit"
import Schema from "typebox/schema"
import {
  type HttpAdapterAction,
  HttpAdapterActions,
  HttpAdapterFenceRequestSchema,
  HttpAdapterIssueRequestSchema,
  HttpAdapterProtocolVersion,
  HttpAdapterSubjectRequestSchema,
} from "./protocol.js"

export interface HttpAdapterHandlerOptions {
  adapter: string
  adapter_version: string
  adapter_package_digest: `sha256:${string}`
  operations: readonly AdapterOperation<unknown, unknown, unknown>[]
  token?: string
  maximum_request_bytes?: number
}

export type HttpAdapterHandler = (request: IncomingMessage, response: ServerResponse) => void

class RequestFailure extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.status = status
    this.code = code
  }
}

function send(response: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body)
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(text),
    "cache-control": "no-store",
  })
  response.end(text)
}

function authorized(request: IncomingMessage, token: string | undefined): boolean {
  if (!token) {
    return true
  }
  const supplied = Buffer.from(request.headers.authorization ?? "")
  const expected = Buffer.from(`Bearer ${token}`)
  return supplied.length === expected.length && timingSafeEqual(supplied, expected)
}

async function body(request: IncomingMessage, limit: number): Promise<unknown> {
  if (!request.headers["content-type"]?.startsWith("application/json")) {
    throw new RequestFailure(415, "UNSUPPORTED_MEDIA_TYPE", "Request body must be JSON")
  }
  const chunks: Buffer[] = []
  let size = 0
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string)
    size += buffer.length
    if (size > limit) {
      throw new RequestFailure(413, "REQUEST_TOO_LARGE", "Request body is too large")
    }
    chunks.push(buffer)
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown
  } catch {
    throw new RequestFailure(400, "REQUEST_INVALID", "Request body is not valid JSON")
  }
}

function route(url: string | undefined): { key: string; action: HttpAdapterAction } | "manifest" {
  const path = new URL(url ?? "/", "http://adapter.invalid").pathname
  if (path === "/manifest") {
    return "manifest"
  }
  const match = /^\/operations\/([^/]+)\/([a-z]+)$/.exec(path)
  const action = match?.[2] as HttpAdapterAction | undefined
  if (!match?.[1] || !action || !HttpAdapterActions.includes(action)) {
    throw new RequestFailure(404, "ROUTE_NOT_FOUND", "Adapter route does not exist")
  }
  return { key: decodeURIComponent(match[1]), action }
}

function abortOnClose(response: ServerResponse, deadline?: string): AbortSignal {
  const controller = new AbortController()
  response.once("close", () => {
    if (!response.writableFinished) {
      controller.abort(new DOMException("Client closed the request", "AbortError"))
    }
  })
  if (!deadline) {
    return controller.signal
  }
  const remaining = Date.parse(deadline) - Date.now()
  return AbortSignal.any([
    controller.signal,
    AbortSignal.timeout(Math.min(Math.max(remaining, 0), 2_147_483_647)),
  ])
}

async function invoke(
  operation: AdapterOperation<unknown, unknown, unknown>,
  action: HttpAdapterAction,
  value: unknown,
  response: ServerResponse,
): Promise<unknown> {
  if (action === "fence") {
    if (!Schema.Check(HttpAdapterFenceRequestSchema, value) || !operation.activateFence) {
      throw new RequestFailure(400, "REQUEST_INVALID", "Fence request is invalid")
    }
    return operation.activateFence({ ...value.context, signal: abortOnClose(response) })
  }
  if (action === "issue") {
    if (!Schema.Check(HttpAdapterIssueRequestSchema, value)) {
      throw new RequestFailure(400, "REQUEST_INVALID", "Issue request is invalid")
    }
    return operation.issue(
      { ...value.context, signal: abortOnClose(response, value.context.deadline) },
      value.input,
    )
  }
  if (!Schema.Check(HttpAdapterSubjectRequestSchema, value)) {
    throw new RequestFailure(400, "REQUEST_INVALID", `${action} request is invalid`)
  }
  const handler = operation[action]
  if (!handler) {
    throw new RequestFailure(404, "ACTION_UNSUPPORTED", `Operation does not support ${action}`)
  }
  return handler(
    { ...value.context, signal: abortOnClose(response, value.context.deadline) },
    value.subject,
  )
}

export function createHttpAdapterHandler(options: HttpAdapterHandlerOptions): HttpAdapterHandler {
  const bundle: AdapterCapabilityBundle = buildCapabilityManifest({
    adapter: options.adapter,
    adapter_version: options.adapter_version,
    adapter_package_digest: options.adapter_package_digest,
    operations: options.operations,
  })
  const operations = new Map(
    options.operations.map((operation) => [operation.descriptor.key, operation]),
  )
  const limit = options.maximum_request_bytes ?? 1_048_576

  return (request, response) => {
    void (async () => {
      try {
        if (!authorized(request, options.token)) {
          throw new RequestFailure(
            401,
            "UNAUTHORIZED",
            "Adapter credentials are missing or invalid",
          )
        }
        const selected = route(request.url)
        if (selected === "manifest") {
          if (request.method !== "GET") {
            throw new RequestFailure(405, "METHOD_NOT_ALLOWED", "Manifest requires GET")
          }
          send(response, 200, {
            protocol_version: HttpAdapterProtocolVersion,
            manifest: bundle.manifest,
          })
          return
        }
        if (request.method !== "POST") {
          throw new RequestFailure(405, "METHOD_NOT_ALLOWED", "Operations require POST")
        }
        const operation = operations.get(selected.key)
        if (!operation) {
          throw new RequestFailure(404, "OPERATION_NOT_FOUND", "Adapter operation does not exist")
        }
        const result = await invoke(
          operation,
          selected.action,
          await body(request, limit),
          response,
        )
        send(response, 200, result)
      } catch (error) {
        if (error instanceof RequestFailure) {
          send(response, error.status, { error: { code: error.code, message: error.message } })
          return
        }
        if (error instanceof AdapterKitError) {
          send(response, 422, { error: { code: error.code, message: error.message } })
          return
        }
        send(response, 500, { error: { code: "ADAPTER_FAILED", message: "Adapter failed" } })
      }
    })()
  }
}
