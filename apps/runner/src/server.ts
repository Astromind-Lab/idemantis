import { randomUUID } from "node:crypto"
import cookie from "@fastify/cookie"
import helmet from "@fastify/helmet"
import rateLimit from "@fastify/rate-limit"
import type { ClientRequestId, EffectId, JsonObject, RunId, SourceId } from "@idemantis/contracts"
import { ClientRequestIdSchema, EffectIdSchema, RunIdSchema } from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import {
  ApiSchemaVersion,
  type DiscardInterventionRequest,
  type MissionExecutionStart,
  type MissionExecutionStatus,
  type PrepareInterventionRequest,
  type RecordObservationRequest,
  type StreamEnvelope,
  type SubmitInterventionRequest,
  TransportError,
} from "@idemantis/transport"
import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify"
import Schema from "typebox/schema"
import type { SessionAuthority } from "./session.js"
import type { RunnerStore } from "./store.js"

const sessionCookie = "idemantis_session"

export interface RunnerServerOptions {
  store: RunnerStore
  sessions: SessionAuthority
  exact_origin: string
  mission_executor?: MissionExecutor
  secure_cookie?: boolean
  stream_poll_interval_ms?: number
  stream_heartbeat_interval_ms?: number
}

export interface MissionExecutor {
  accepts?(runtime: MissionRuntimeDefinition): boolean
  run(runId: RunId): Promise<void>
  start?(runId: RunId): MissionExecutionStart
  executionStatus?(runId: RunId): MissionExecutionStatus
}

type UnknownObject = Record<string, unknown>

function object(value: unknown): UnknownObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TransportError("INVALID_REQUEST", "Request body must be an object", 422)
  }
  return value as UnknownObject
}

function field(value: UnknownObject, key: string): unknown {
  return Reflect.get(value, key)
}

function header(request: FastifyRequest, name: string): string | undefined {
  const value = request.headers[name]
  return Array.isArray(value) ? value[0] : value
}

function cookieToken(request: FastifyRequest): string | undefined {
  return request.cookies[sessionCookie]
}

function loopbackHostname(hostname: string): boolean {
  return hostname === "127.0.0.1" || hostname === "[::1]" || hostname === "localhost"
}

function loopbackAddress(address: string): boolean {
  return address === "::1" || address.startsWith("127.") || address.startsWith("::ffff:127.")
}

function assertOrigin(request: FastifyRequest, exactOrigin: string): void {
  if (header(request, "origin") !== exactOrigin) {
    throw new TransportError("ACTION_FORBIDDEN", "Request origin is not allowed", 403)
  }
}

function requireMutation(
  request: FastifyRequest,
  sessions: SessionAuthority,
  exactOrigin: string,
): SourceId {
  assertOrigin(request, exactOrigin)
  sessions.assertCsrf(cookieToken(request), header(request, "x-idemantis-csrf"))
  return sessions.assertActive(cookieToken(request))
}

function apiError(request: FastifyRequest, error: TransportError) {
  return {
    schema_version: ApiSchemaVersion,
    error: {
      code: error.code,
      message: error.message,
      request_id: request.id,
      ...(error.details ? { details: error.details as JsonObject } : {}),
    },
  }
}

function clientError(error: unknown): TransportError | undefined {
  if (!error || typeof error !== "object") {
    return undefined
  }
  const status = Reflect.get(error, "statusCode")
  if (typeof status !== "number" || status < 400 || status > 499) {
    return undefined
  }
  return new TransportError("INVALID_REQUEST", "Request could not be accepted", status)
}

function queryValue(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined
}

function numericQuery(value: unknown): number | undefined {
  if (value === undefined) {
    return undefined
  }
  const parsed = Number(value)
  return Number.isSafeInteger(parsed) ? parsed : Number.NaN
}

function runId(value: unknown): RunId {
  if (!Schema.Check(RunIdSchema, value)) {
    throw new TransportError("INVALID_REQUEST", "Run ID is invalid", 422)
  }
  return value as RunId
}

function effectId(value: unknown): EffectId {
  if (!Schema.Check(EffectIdSchema, value)) {
    throw new TransportError("INVALID_REQUEST", "Effect ID is invalid", 422)
  }
  return value as EffectId
}

function clientRequestId(value: unknown): ClientRequestId {
  if (!Schema.Check(ClientRequestIdSchema, value)) {
    throw new TransportError("INVALID_REQUEST", "Client request ID is invalid", 422)
  }
  return value as ClientRequestId
}

function authenticated(request: FastifyRequest, sessions: SessionAuthority): SourceId {
  return sessions.assertActive(cookieToken(request))
}

function streamPosition(envelope: StreamEnvelope): number {
  if (envelope.kind === "mission") {
    return envelope.body.sequence
  }
  if (envelope.kind === "operations") {
    return envelope.body.list_revision
  }
  if (envelope.kind === "receipt") {
    return envelope.body.receipt_revision
  }
  throw new TypeError("A heartbeat does not advance a stream")
}

function streamMessage(envelope: StreamEnvelope): string {
  return `id: ${envelope.resume_cursor}\ndata: ${JSON.stringify(envelope)}\n\n`
}

function pause(duration: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.resolve()
  }
  return new Promise((resolve) => {
    let timeout: NodeJS.Timeout
    const finish = () => {
      clearTimeout(timeout)
      signal.removeEventListener("abort", finish)
      resolve()
    }
    timeout = setTimeout(finish, duration)
    signal.addEventListener("abort", finish, { once: true })
  })
}

async function writeStream(reply: FastifyReply, envelope: StreamEnvelope): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    reply.raw.write(streamMessage(envelope), (error) => (error ? reject(error) : resolve()))
  })
}

async function runEventStream(options: {
  request: FastifyRequest
  reply: FastifyReply
  sessions: SessionAuthority
  initial_position: number
  poll_interval_ms: number
  heartbeat_interval_ms: number
  read: (position: number) => StreamEnvelope[]
  heartbeat: (position: number) => StreamEnvelope
}): Promise<void> {
  const controller = new AbortController()
  const close = () => controller.abort()
  options.request.raw.once("close", close)
  options.reply.raw.once("close", close)
  let position = options.initial_position
  let heartbeatAt = 0
  try {
    while (!controller.signal.aborted) {
      options.sessions.assertActive(cookieToken(options.request))
      const envelopes = options.read(position)
      if (envelopes.length > 0) {
        for (const envelope of envelopes) {
          await writeStream(options.reply, envelope)
          position = streamPosition(envelope)
        }
      } else if (Date.now() >= heartbeatAt) {
        await writeStream(options.reply, options.heartbeat(position))
        heartbeatAt = Date.now() + options.heartbeat_interval_ms
      }
      await pause(options.poll_interval_ms, controller.signal)
    }
  } finally {
    options.request.raw.off("close", close)
    options.reply.raw.off("close", close)
    if (!options.reply.raw.destroyed && !options.reply.raw.writableEnded) {
      options.reply.raw.end()
    }
  }
}

function openEventStream(options: {
  request: FastifyRequest
  reply: FastifyReply
  sessions: SessionAuthority
  initial_position: number
  poll_interval_ms: number
  heartbeat_interval_ms: number
  read: (position: number) => StreamEnvelope[]
  heartbeat: (position: number) => StreamEnvelope
}): FastifyReply {
  options.reply.raw.setHeader("Content-Type", "text/event-stream; charset=utf-8")
  options.reply.raw.setHeader("Cache-Control", "no-store")
  options.reply.raw.setHeader("Connection", "keep-alive")
  options.reply.raw.setHeader("X-Accel-Buffering", "no")
  options.reply.hijack()
  options.reply.raw.flushHeaders()
  void runEventStream(options).catch(() => {
    if (!options.reply.raw.destroyed && !options.reply.raw.writableEnded) {
      options.reply.raw.end()
    }
  })
  return options.reply
}

export async function createRunnerServer(options: RunnerServerOptions): Promise<FastifyInstance> {
  const configuredOrigin = new URL(options.exact_origin)
  if (
    configuredOrigin.origin !== options.exact_origin ||
    !["http:", "https:"].includes(configuredOrigin.protocol) ||
    !loopbackHostname(configuredOrigin.hostname)
  ) {
    throw new RangeError("Runner origin must be an exact loopback HTTP or HTTPS origin")
  }
  const app = Fastify({
    bodyLimit: 128 * 1024,
    genReqId: () => randomUUID(),
    logger: false,
    trustProxy: false,
  })
  const expectedHost = configuredOrigin.host
  const secure = options.secure_cookie ?? configuredOrigin.protocol === "https:"
  const streamPollInterval = options.stream_poll_interval_ms ?? 250
  const streamHeartbeatInterval = options.stream_heartbeat_interval_ms ?? 15_000
  if (
    !Number.isSafeInteger(streamPollInterval) ||
    streamPollInterval < 1 ||
    !Number.isSafeInteger(streamHeartbeatInterval) ||
    streamHeartbeatInterval < 1
  ) {
    throw new RangeError("Stream intervals must be positive safe integers")
  }

  await app.register(cookie)
  await app.register(rateLimit, { max: 240, timeWindow: "1 minute" })
  await app.register(helmet, {
    contentSecurityPolicy: {
      directives: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'"],
        connectSrc: ["'self'"],
        fontSrc: ["'self'"],
        imgSrc: ["'self'", "data:"],
        objectSrc: ["'none'"],
        baseUri: ["'none'"],
        frameAncestors: ["'none'"],
        upgradeInsecureRequests: secure ? [] : null,
      },
    },
    crossOriginEmbedderPolicy: false,
    strictTransportSecurity: secure,
  })

  app.addHook("onRequest", async (request) => {
    if (request.headers.host !== expectedHost) {
      throw new TransportError("ACTION_FORBIDDEN", "Request host is not allowed", 403)
    }
  })

  app.addHook("onSend", async (request, reply, payload) => {
    if (request.url.startsWith("/api/")) {
      reply.header("Cache-Control", "no-store")
      reply.header("Pragma", "no-cache")
    }
    return payload
  })

  app.setErrorHandler((error, request, reply) => {
    const transport =
      error instanceof TransportError
        ? error
        : (clientError(error) ??
          new TransportError("RUNNER_UNAVAILABLE", "Runner could not complete the request", 500))
    reply.status(transport.status).send(apiError(request, transport))
  })

  app.get("/api/health", async () => ({ schema_version: ApiSchemaVersion, status: "ready" }))

  app.post(
    "/api/session/bootstrap",
    { config: { rateLimit: { max: 10, timeWindow: "1 minute" } } },
    async (request, reply) => {
      assertOrigin(request, options.exact_origin)
      if (!loopbackAddress(request.ip)) {
        throw new TransportError("ACTION_FORBIDDEN", "Bootstrap is restricted to loopback", 403)
      }
      const bootstrapSecret = header(request, "x-idemantis-bootstrap")
      if (!bootstrapSecret) {
        throw new TransportError("SESSION_EXPIRED", "Bootstrap secret is required", 401)
      }
      const created = options.sessions.createSession(bootstrapSecret)
      reply.setCookie(sessionCookie, created.cookie_token, {
        httpOnly: true,
        sameSite: "strict",
        secure,
        path: "/",
        maxAge: options.sessions.sessionLifetimeSeconds,
      })
      return created.document
    },
  )

  app.get("/api/session", async (request) => options.sessions.session(cookieToken(request)))

  app.delete("/api/session", async (request, reply) => {
    requireMutation(request, options.sessions, options.exact_origin)
    options.sessions.revoke(cookieToken(request))
    reply.clearCookie(sessionCookie, { path: "/" })
    return { schema_version: ApiSchemaVersion, ended: true }
  })

  app.get("/api/definitions", async (request) => {
    authenticated(request, options.sessions)
    return { schema_version: ApiSchemaVersion, items: options.store.listDefinitions() }
  })

  app.post("/api/definitions", async (request) => {
    requireMutation(request, options.sessions, options.exact_origin)
    const body = object(request.body)
    const runtime = object(field(body, "runtime")) as unknown as MissionRuntimeDefinition
    const subjectKeys = field(body, "subject_reference_keys")
    if (
      subjectKeys !== undefined &&
      (!Array.isArray(subjectKeys) || subjectKeys.some((value) => typeof value !== "string"))
    ) {
      throw new TransportError("INVALID_REQUEST", "Subject reference keys are invalid", 422)
    }
    options.store.assertDefinition(runtime)
    if (options.mission_executor?.accepts && !options.mission_executor.accepts(runtime)) {
      throw new TransportError(
        "INVALID_REQUEST",
        "No configured executor can bind this definition",
        422,
      )
    }
    options.store.registerDefinition(runtime, {
      ...(subjectKeys ? { subject_reference_keys: subjectKeys as string[] } : {}),
    })
    return { schema_version: ApiSchemaVersion, registered: runtime.definition_digest }
  })

  app.post("/api/missions", async (request, reply) => {
    requireMutation(request, options.sessions, options.exact_origin)
    const body = object(request.body)
    const definitionDigest = field(body, "definition_digest")
    if (typeof definitionDigest !== "string") {
      throw new TransportError("INVALID_REQUEST", "Definition digest is required", 422)
    }
    const input = object(field(body, "input")) as JsonObject
    const definition = options.store.definition(definitionDigest)
    if (
      definition &&
      options.mission_executor?.accepts &&
      !options.mission_executor.accepts(definition)
    ) {
      throw new TransportError(
        "INVALID_REQUEST",
        "No configured executor can run this definition",
        422,
      )
    }
    const detail = options.store.admitMission({
      definition_digest: definitionDigest,
      input,
    })
    reply.status(201)
    return detail
  })

  app.post("/api/missions/:runId/run", async (request) => {
    requireMutation(request, options.sessions, options.exact_origin)
    const params = object(request.params)
    const selectedRunId = runId(field(params, "runId"))
    options.store.missionDetail(selectedRunId)
    if (!options.mission_executor) {
      throw new TransportError("RUNNER_UNAVAILABLE", "Mission execution is not configured", 503)
    }
    await options.mission_executor.run(selectedRunId)
    return options.store.missionDetail(selectedRunId)
  })

  app.post("/api/missions/:runId/start", async (request, reply) => {
    requireMutation(request, options.sessions, options.exact_origin)
    const params = object(request.params)
    const selectedRunId = runId(field(params, "runId"))
    options.store.missionDetail(selectedRunId)
    if (!options.mission_executor?.start) {
      throw new TransportError(
        "RUNNER_UNAVAILABLE",
        "Asynchronous execution is not configured",
        503,
      )
    }
    const started = options.mission_executor.start(selectedRunId)
    reply.status(started.disposition === "started" ? 202 : 200)
    return started
  })

  app.post("/api/missions/:runId/observations", async (request, reply) => {
    const sourceId = requireMutation(request, options.sessions, options.exact_origin)
    const params = object(request.params)
    const selectedRunId = runId(field(params, "runId"))
    const receipt = options.store.recordObservation(
      selectedRunId,
      object(request.body) as unknown as RecordObservationRequest,
      sourceId,
    )
    if (receipt.disposition === "recorded" && options.mission_executor?.start) {
      try {
        options.mission_executor.start(selectedRunId)
      } catch (error) {
        if (!(error instanceof TransportError)) {
          throw error
        }
      }
    }
    reply.status(receipt.disposition === "recorded" ? 201 : 200)
    return receipt
  })

  app.get("/api/missions/:runId/execution", async (request) => {
    authenticated(request, options.sessions)
    const params = object(request.params)
    const selectedRunId = runId(field(params, "runId"))
    options.store.missionDetail(selectedRunId)
    if (!options.mission_executor?.executionStatus) {
      throw new TransportError("RUNNER_UNAVAILABLE", "Execution status is not configured", 503)
    }
    return options.mission_executor.executionStatus(selectedRunId)
  })

  app.get("/api/missions", async (request) => {
    authenticated(request, options.sessions)
    const query = object(request.query)
    const bucket = queryValue(field(query, "bucket"))
    const search = queryValue(field(query, "query"))
    const cursor = queryValue(field(query, "cursor"))
    const limit = numericQuery(field(query, "limit"))
    return options.store.listMissions({
      ...(bucket ? { bucket: bucket as "needs_attention" } : {}),
      ...(search ? { query: search } : {}),
      ...(cursor ? { cursor } : {}),
      ...(limit !== undefined ? { limit } : {}),
    })
  })

  app.get("/api/missions/:runId", async (request) => {
    authenticated(request, options.sessions)
    const params = object(request.params)
    return options.store.missionDetail(runId(field(params, "runId")))
  })

  app.get("/api/missions/:runId/timeline", async (request) => {
    authenticated(request, options.sessions)
    const params = object(request.params)
    const query = object(request.query)
    return options.store.timeline(
      runId(field(params, "runId")),
      queryValue(field(query, "cursor")),
      numericQuery(field(query, "limit")) ?? 100,
    )
  })

  app.get("/api/missions/:runId/effects", async (request) => {
    authenticated(request, options.sessions)
    const params = object(request.params)
    const query = object(request.query)
    return options.store.effects(
      runId(field(params, "runId")),
      queryValue(field(query, "cursor")),
      numericQuery(field(query, "limit")) ?? 100,
    )
  })

  app.get("/api/missions/:runId/effects/:effectId", async (request) => {
    authenticated(request, options.sessions)
    const params = object(request.params)
    return options.store.effectDetail(
      runId(field(params, "runId")),
      effectId(field(params, "effectId")),
    )
  })

  app.get("/api/missions/:runId/replay/:sequence", async (request) => {
    authenticated(request, options.sessions)
    const params = object(request.params)
    const sequence = Number(field(params, "sequence"))
    return options.store.replay(runId(field(params, "runId")), sequence)
  })

  app.post("/api/missions/:runId/interventions/prepare", async (request, reply) => {
    const actorId = requireMutation(request, options.sessions, options.exact_origin)
    const params = object(request.params)
    const prepared = options.store.prepareIntervention(
      runId(field(params, "runId")),
      actorId,
      object(request.body) as unknown as PrepareInterventionRequest,
    )
    reply.status(201)
    return prepared
  })

  app.post("/api/missions/:runId/interventions/submit", async (request) => {
    const actorId = requireMutation(request, options.sessions, options.exact_origin)
    const params = object(request.params)
    return options.store.submitIntervention(
      runId(field(params, "runId")),
      actorId,
      object(request.body) as unknown as SubmitInterventionRequest,
    )
  })

  app.post("/api/missions/:runId/interventions/discard", async (request) => {
    const actorId = requireMutation(request, options.sessions, options.exact_origin)
    const params = object(request.params)
    return options.store.discardIntervention(
      runId(field(params, "runId")),
      actorId,
      object(request.body) as unknown as DiscardInterventionRequest,
    )
  })

  app.get("/api/missions/:runId/interventions", async (request) => {
    const actorId = authenticated(request, options.sessions)
    const params = object(request.params)
    const query = object(request.query)
    return options.store.interventionActivity(
      runId(field(params, "runId")),
      actorId,
      queryValue(field(query, "cursor")),
      numericQuery(field(query, "limit")) ?? 100,
    )
  })

  app.get("/api/missions/:runId/interventions/:clientRequestId", async (request) => {
    const actorId = authenticated(request, options.sessions)
    const params = object(request.params)
    return options.store.preparedInterventionById(
      runId(field(params, "runId")),
      actorId,
      clientRequestId(field(params, "clientRequestId")),
    )
  })

  app.get("/api/missions/:runId/interventions/:clientRequestId/receipt", async (request) => {
    const actorId = authenticated(request, options.sessions)
    const params = object(request.params)
    return options.store.interventionReceiptById(
      runId(field(params, "runId")),
      actorId,
      clientRequestId(field(params, "clientRequestId")),
    )
  })

  app.get("/api/missions/:runId/intervention-receipts", async (request) => {
    const actorId = authenticated(request, options.sessions)
    const params = object(request.params)
    const query = object(request.query)
    return options.store.interventionReceipts(
      runId(field(params, "runId")),
      actorId,
      queryValue(field(query, "cursor")),
      numericQuery(field(query, "limit")) ?? 100,
    )
  })

  app.get("/api/streams/operations", async (request, reply) => {
    authenticated(request, options.sessions)
    const query = object(request.query)
    const headerCursor = header(request, "last-event-id")
    const queryCursor = queryValue(field(query, "cursor"))
    const cursor = headerCursor || queryCursor
    const initial = cursor ? undefined : numericQuery(field(query, "after_revision"))
    const position = options.store.operationsStreamPosition(initial, cursor)
    return openEventStream({
      request,
      reply,
      sessions: options.sessions,
      initial_position: position,
      poll_interval_ms: streamPollInterval,
      heartbeat_interval_ms: streamHeartbeatInterval,
      read: (current) => options.store.operationsStreamBatch(current),
      heartbeat: (current) => options.store.operationsStreamHeartbeat(current),
    })
  })

  app.get("/api/missions/:runId/stream", async (request, reply) => {
    authenticated(request, options.sessions)
    const params = object(request.params)
    const selectedRunId = runId(field(params, "runId"))
    const query = object(request.query)
    const headerCursor = header(request, "last-event-id")
    const queryCursor = queryValue(field(query, "cursor"))
    const cursor = headerCursor || queryCursor
    const initial = cursor ? undefined : numericQuery(field(query, "after_sequence"))
    const position = options.store.missionStreamPosition(selectedRunId, initial, cursor)
    return openEventStream({
      request,
      reply,
      sessions: options.sessions,
      initial_position: position,
      poll_interval_ms: streamPollInterval,
      heartbeat_interval_ms: streamHeartbeatInterval,
      read: (current) => options.store.missionStreamBatch(selectedRunId, current),
      heartbeat: (current) => options.store.missionStreamHeartbeat(selectedRunId, current),
    })
  })

  app.get("/api/missions/:runId/receipts/stream", async (request, reply) => {
    const actorId = authenticated(request, options.sessions)
    const params = object(request.params)
    const selectedRunId = runId(field(params, "runId"))
    const query = object(request.query)
    const headerCursor = header(request, "last-event-id")
    const queryCursor = queryValue(field(query, "cursor"))
    const cursor = headerCursor || queryCursor
    const initial = cursor ? undefined : numericQuery(field(query, "after_revision"))
    const position = options.store.receiptStreamPosition(selectedRunId, actorId, initial, cursor)
    return openEventStream({
      request,
      reply,
      sessions: options.sessions,
      initial_position: position,
      poll_interval_ms: streamPollInterval,
      heartbeat_interval_ms: streamHeartbeatInterval,
      read: (current) => options.store.receiptStreamBatch(selectedRunId, actorId, current),
      heartbeat: (current) => options.store.receiptStreamHeartbeat(selectedRunId, actorId, current),
    })
  })

  app.setNotFoundHandler((request, reply) => {
    reply
      .status(404)
      .send(
        apiError(request, new TransportError("INVALID_REQUEST", "API route does not exist", 404)),
      )
  })

  return app
}
