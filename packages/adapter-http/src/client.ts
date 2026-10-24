import {
  type AdapterCapabilityManifest,
  type AdapterInvocationContext,
  type AdapterOperation,
  type AdapterOperationDescriptor,
  defineAdapterOperation,
  type FenceActivationContext,
} from "@idemantis/adapter-kit"
import type { FenceProof, JsonValue } from "@idemantis/contracts"
import type { TSchema } from "typebox"
import Schema from "typebox/schema"
import { HttpAdapterError } from "./errors.js"
import {
  type HttpAdapterAction,
  HttpAdapterManifestResponseSchema,
  HttpAdapterProtocolVersion,
  operationPath,
} from "./protocol.js"

type Fetch = (input: string, init: RequestInit) => Promise<Response>

export interface HttpAdapterConnectionOptions {
  url: string
  token?: string
  fetch?: Fetch
  manifest_timeout_ms?: number
  maximum_response_bytes?: number
}

export interface RemoteAdapter {
  manifest: AdapterCapabilityManifest
  operations: AdapterOperation<unknown, unknown, unknown>[]
}

const loopbackHosts = new Set(["127.0.0.1", "localhost", "[::1]"])

function baseUrl(value: string): URL {
  let parsed: URL
  try {
    parsed = new URL(value)
  } catch {
    throw new HttpAdapterError("CONNECTION_INVALID", "Adapter URL is invalid")
  }
  if (parsed.username || parsed.password || parsed.search || parsed.hash) {
    throw new HttpAdapterError(
      "CONNECTION_INVALID",
      "Adapter URL must not carry credentials, a query, or a fragment",
    )
  }
  if (
    parsed.protocol !== "https:" &&
    !(parsed.protocol === "http:" && loopbackHosts.has(parsed.hostname))
  ) {
    throw new HttpAdapterError(
      "CONNECTION_INVALID",
      "Adapter URL must use HTTPS unless it targets a loopback host",
    )
  }
  if (!parsed.pathname.endsWith("/")) {
    parsed.pathname = `${parsed.pathname}/`
  }
  return parsed
}

function endpoint(base: URL, path: string): string {
  return new URL(path.slice(1), base).toString()
}

async function readJson(response: Response, limit: number, label: string): Promise<unknown> {
  if (!response.ok) {
    throw new HttpAdapterError("RESPONSE_REJECTED", `${label} returned HTTP ${response.status}`)
  }
  const text = await response.text()
  if (Buffer.byteLength(text) > limit) {
    throw new HttpAdapterError("RESPONSE_INVALID", `${label} exceeded the response size limit`)
  }
  try {
    return JSON.parse(text) as unknown
  } catch {
    throw new HttpAdapterError("RESPONSE_INVALID", `${label} did not return JSON`)
  }
}

function invocationIdentity(context: AdapterInvocationContext) {
  const { signal: _signal, ...identity } = context
  return identity
}

function fenceIdentity(context: FenceActivationContext) {
  const { signal: _signal, ...identity } = context
  return identity
}

export async function connectHttpAdapter(
  options: HttpAdapterConnectionOptions,
): Promise<RemoteAdapter> {
  const base = baseUrl(options.url)
  const fetcher: Fetch = options.fetch ?? ((input, init) => fetch(input, init))
  const limit = options.maximum_response_bytes ?? 1_048_576
  const headers: Record<string, string> = {
    accept: "application/json",
    ...(options.token ? { authorization: `Bearer ${options.token}` } : {}),
  }

  const send = async (
    descriptor: AdapterOperationDescriptor,
    action: HttpAdapterAction,
    body: Record<string, JsonValue>,
    signal: AbortSignal,
    idempotencyKey?: string,
  ): Promise<unknown> => {
    const response = await fetcher(endpoint(base, operationPath(descriptor.key, action)), {
      method: "POST",
      headers: {
        ...headers,
        "content-type": "application/json",
        ...(idempotencyKey ? { "idempotency-key": idempotencyKey } : {}),
      },
      body: JSON.stringify({ protocol_version: HttpAdapterProtocolVersion, ...body }),
      signal,
      redirect: "error",
    })
    return readJson(response, limit, `Adapter ${descriptor.key} ${action}`)
  }

  const manifestResponse = await fetcher(endpoint(base, "/manifest"), {
    method: "GET",
    headers,
    signal: AbortSignal.timeout(options.manifest_timeout_ms ?? 5_000),
    redirect: "error",
  })
  const document = await readJson(manifestResponse, limit, "Adapter manifest")
  if (!Schema.Check(HttpAdapterManifestResponseSchema, document)) {
    throw new HttpAdapterError("MANIFEST_INVALID", "Adapter manifest does not match the protocol")
  }
  const manifest = structuredClone(document.manifest) as AdapterCapabilityManifest

  const operations = manifest.operations.map((descriptor) => {
    const capabilities = descriptor.capabilities
    return defineAdapterOperation({
      key: descriptor.key,
      adapter: descriptor.adapter,
      operation: descriptor.operation,
      schemaVersion: descriptor.operation_schema_version,
      inputSchema: descriptor.input_schema as unknown as TSchema,
      outputSchema: descriptor.output_schema as unknown as TSchema,
      ...(descriptor.subject_schema
        ? { subjectSchema: descriptor.subject_schema as unknown as TSchema }
        : {}),
      effectClass: descriptor.effect_class,
      consequence: descriptor.consequence,
      capabilities,
      timeout: {
        minimumMs: descriptor.timeout.minimum_ms,
        defaultMs: descriptor.timeout.default_ms,
        maximumMs: descriptor.timeout.maximum_ms,
      },
      ...(descriptor.rate_limit
        ? {
            rateLimit: {
              maximumRequests: descriptor.rate_limit.maximum_requests,
              windowMs: descriptor.rate_limit.window_ms,
            },
          }
        : {}),
      reconciliationLookupFields: descriptor.reconciliation_lookup_fields,
      observationTypes: descriptor.observation_types,
      observationCorrelationFields: descriptor.observation_correlation_fields,
      secretRequirements: descriptor.secret_requirements,
      ...(descriptor.maximum_fence_activation_delay_ms === undefined
        ? {}
        : { maximumFenceActivationDelayMs: descriptor.maximum_fence_activation_delay_ms }),
      issue: async (context, input) =>
        (await send(
          descriptor,
          "issue",
          { context: invocationIdentity(context), input: input as JsonValue },
          context.signal,
          context.effect_id,
        )) as never,
      ...(capabilities.reconciliation === "none"
        ? {}
        : {
            reconcile: async (context: AdapterInvocationContext, subject: unknown) =>
              (await send(
                descriptor,
                "reconcile",
                { context: invocationIdentity(context), subject: subject as JsonValue },
                context.signal,
              )) as never,
          }),
      ...(capabilities.cancellation
        ? {
            cancel: async (context: AdapterInvocationContext, subject: unknown) =>
              (await send(
                descriptor,
                "cancel",
                { context: invocationIdentity(context), subject: subject as JsonValue },
                context.signal,
              )) as never,
          }
        : {}),
      ...(capabilities.compensation
        ? {
            compensate: async (context: AdapterInvocationContext, subject: unknown) =>
              (await send(
                descriptor,
                "compensate",
                { context: invocationIdentity(context), subject: subject as JsonValue },
                context.signal,
                `${context.effect_id}:compensation`,
              )) as never,
          }
        : {}),
      ...(capabilities.fencing_mode === "none"
        ? {}
        : {
            activateFence: async (context: FenceActivationContext) =>
              (await send(
                descriptor,
                "fence",
                { context: fenceIdentity(context) },
                context.signal,
              )) as FenceProof,
          }),
    }) as unknown as AdapterOperation<unknown, unknown, unknown>
  })

  return { manifest, operations }
}
