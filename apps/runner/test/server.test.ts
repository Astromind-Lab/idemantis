import { createServer } from "node:net"
import type { RunId } from "@idemantis/contracts"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import {
  InterventionReceiptPageSchema,
  InterventionReceiptSchema,
  MissionDetailSchema,
  MissionExecutionStartSchema,
  MissionExecutionStatusSchema,
  MissionListPageSchema,
  PreparedInterventionPageSchema,
  PreparedInterventionSchema,
  SessionDocumentSchema,
  type StreamEnvelope,
  StreamEnvelopeSchema,
} from "@idemantis/transport"
import Schema from "typebox/schema"
import { afterEach, describe, expect, it, vi } from "vitest"
import { createRunnerServer, type MissionExecutor } from "../src/server.js"
import { SessionAuthority } from "../src/session.js"
import { RunnerStore } from "../src/store.js"
import { InterventionRuntime, VerificationRuntime } from "./fixture.js"

const origin = "http://127.0.0.1:4317"
const host = "127.0.0.1:4317"
const journals: SqliteJournal[] = []
const servers: Awaited<ReturnType<typeof createRunnerServer>>[] = []

async function setup(
  clock?: () => Date,
  configuredOrigin = origin,
  missionExecutor?: MissionExecutor,
) {
  const journal = new SqliteJournal({ path: ":memory:", ...(clock ? { clock } : {}) })
  journals.push(journal)
  const store = new RunnerStore({
    journal,
    cursor_secret: new Uint8Array(32).fill(4),
    ...(clock ? { clock } : {}),
  })
  store.registerDefinition(VerificationRuntime, { subject_reference_keys: ["subject"] })
  store.registerDefinition(InterventionRuntime, { subject_reference_keys: ["subject"] })
  const sessions = new SessionAuthority({ ...(clock ? { clock } : {}) })
  const app = await createRunnerServer({
    store,
    sessions,
    exact_origin: configuredOrigin,
    ...(missionExecutor ? { mission_executor: missionExecutor } : {}),
    stream_poll_interval_ms: 5,
    stream_heartbeat_interval_ms: 25,
  })
  servers.push(app)
  return { app, sessions, store }
}

async function authenticate(
  app: Awaited<ReturnType<typeof createRunnerServer>>,
  sessions: SessionAuthority,
  requestOrigin = origin,
) {
  const bootstrap = sessions.issueBootstrap()
  const response = await app.inject({
    method: "POST",
    url: "/api/session/bootstrap",
    headers: {
      host: new URL(requestOrigin).host,
      origin: requestOrigin,
      "x-idemantis-bootstrap": bootstrap,
    },
  })
  const document = response.json()
  const setCookie = response.headers["set-cookie"]
  const cookie = (Array.isArray(setCookie) ? setCookie[0] : setCookie)?.split(";")[0]
  if (!cookie || typeof document.csrf_token !== "string") {
    throw new Error("Expected an authenticated operator session")
  }
  return {
    bootstrap,
    cookie,
    csrf: document.csrf_token as string,
    document,
    set_cookie: Array.isArray(setCookie) ? setCookie[0] : setCookie,
  }
}

async function availableOrigin(): Promise<{ origin: string; port: number }> {
  const server = createServer()
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", resolve)
  })
  const address = server.address()
  if (!address || typeof address === "string") {
    throw new Error("Expected a TCP address")
  }
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  )
  return { origin: `http://127.0.0.1:${address.port}`, port: address.port }
}

function sseReader(response: Response): () => Promise<StreamEnvelope> {
  const reader = response.body?.getReader()
  if (!reader) {
    throw new Error("Expected an event stream response body")
  }
  const decoder = new TextDecoder()
  let buffered = ""
  return async () => {
    while (true) {
      const boundary = buffered.indexOf("\n\n")
      if (boundary >= 0) {
        const block = buffered.slice(0, boundary)
        buffered = buffered.slice(boundary + 2)
        const data = block
          .split("\n")
          .find((line) => line.startsWith("data: "))
          ?.slice(6)
        if (!data) {
          continue
        }
        const envelope = JSON.parse(data) as unknown
        if (!Schema.Check(StreamEnvelopeSchema, envelope)) {
          throw new Error("Received an invalid stream envelope")
        }
        return envelope as StreamEnvelope
      }
      const chunk = await reader.read()
      if (chunk.done) {
        throw new Error("Event stream ended before the next envelope")
      }
      buffered += decoder.decode(chunk.value, { stream: true })
    }
  }
}

afterEach(async () => {
  for (const app of servers.splice(0)) {
    await app.close()
  }
  for (const journal of journals.splice(0)) {
    journal.close()
  }
})

describe("runner HTTP boundary", () => {
  it("exchanges one bootstrap secret for a protected operator session", async () => {
    const { app, sessions } = await setup()
    const session = await authenticate(app, sessions)

    expect(Schema.Check(SessionDocumentSchema, session.document)).toBe(true)
    expect(session.document.capabilities).toEqual(["view", "operate"])
    expect(session.set_cookie).toContain("HttpOnly")
    expect(session.set_cookie).toContain("SameSite=Strict")
    expect(session.set_cookie).toContain("Path=/")
    expect(session.set_cookie).toContain("Max-Age=28800")
    expect(session.set_cookie).not.toContain("Secure")

    const authenticated = await app.inject({
      method: "GET",
      url: "/api/session",
      headers: { host, cookie: session.cookie },
    })
    expect(authenticated.statusCode).toBe(200)

    const reused = await app.inject({
      method: "POST",
      url: "/api/session/bootstrap",
      headers: {
        host,
        origin,
        "x-idemantis-bootstrap": session.bootstrap,
      },
    })
    expect(reused.statusCode).toBe(401)
    expect(reused.json().error.code).toBe("SESSION_EXPIRED")
  })

  it("refuses non-loopback origins and unexpected hosts", async () => {
    const journal = new SqliteJournal({ path: ":memory:" })
    journals.push(journal)
    const store = new RunnerStore({ journal })
    const sessions = new SessionAuthority()
    await expect(
      createRunnerServer({
        store,
        sessions,
        exact_origin: "https://operator.example.com",
      }),
    ).rejects.toThrow("exact loopback")

    const configured = await setup()
    const response = await configured.app.inject({
      method: "GET",
      url: "/api/health",
      headers: { host: "localhost:4317" },
    })
    expect(response.statusCode).toBe(403)
  })

  it("marks loopback HTTPS session cookies as secure", async () => {
    const secureOrigin = "https://127.0.0.1:4317"
    const { app, sessions } = await setup(undefined, secureOrigin)
    const session = await authenticate(app, sessions, secureOrigin)
    expect(session.set_cookie).toContain("Secure")
  })

  it("rejects wrong-origin bootstrap and mutation requests", async () => {
    const { app, sessions } = await setup()
    const bootstrap = sessions.issueBootstrap()
    const wrongOrigin = await app.inject({
      method: "POST",
      url: "/api/session/bootstrap",
      headers: {
        host,
        origin: "http://attacker.invalid",
        "x-idemantis-bootstrap": bootstrap,
      },
    })
    expect(wrongOrigin.statusCode).toBe(403)

    const remoteBootstrap = await app.inject({
      method: "POST",
      url: "/api/session/bootstrap",
      headers: {
        host,
        origin,
        "x-idemantis-bootstrap": bootstrap,
      },
      remoteAddress: "198.51.100.7",
    })
    expect(remoteBootstrap.statusCode).toBe(403)

    const session = await authenticate(app, sessions)
    const missingCsrf = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers: { host, origin, cookie: session.cookie },
      payload: {
        definition_digest: VerificationRuntime.definition_digest,
        input: { subject: "release-47", summary: "Verify the deployment record" },
      },
    })
    expect(missingCsrf.statusCode).toBe(403)
    expect(missingCsrf.json().error.code).toBe("ACTION_FORBIDDEN")
  })

  it("requires an explicit session refresh after anti-forgery expiry", async () => {
    let now = new Date("2026-10-24T08:00:00.000Z")
    const { app, sessions } = await setup(() => now)
    const session = await authenticate(app, sessions)
    now = new Date("2026-10-24T08:15:00.000Z")

    const rejected = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers: {
        host,
        origin,
        cookie: session.cookie,
        "x-idemantis-csrf": session.csrf,
      },
      payload: {
        definition_digest: VerificationRuntime.definition_digest,
        input: { subject: "release-47", summary: "Verify the deployment record" },
      },
    })
    expect(rejected.statusCode).toBe(403)

    const refreshed = await app.inject({
      method: "GET",
      url: "/api/session",
      headers: { host, cookie: session.cookie },
    })
    const csrf = refreshed.json().csrf_token
    expect(csrf).not.toBe(session.csrf)

    const accepted = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers: {
        host,
        origin,
        cookie: session.cookie,
        "x-idemantis-csrf": csrf,
      },
      payload: {
        definition_digest: VerificationRuntime.definition_digest,
        input: { subject: "release-47", summary: "Verify the deployment record" },
      },
    })
    expect(accepted.statusCode).toBe(201)
  })

  it("creates and inspects a mission through authenticated APIs", async () => {
    const { app, sessions } = await setup()
    const session = await authenticate(app, sessions)
    const created = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers: {
        host,
        origin,
        cookie: session.cookie,
        "x-idemantis-csrf": session.csrf,
      },
      payload: {
        definition_digest: VerificationRuntime.definition_digest,
        input: { subject: "release-47", summary: "Verify the deployment record" },
      },
    })
    expect(created.statusCode).toBe(201)
    expect(Schema.Check(MissionDetailSchema, created.json())).toBe(true)

    const queue = await app.inject({
      method: "GET",
      url: "/api/missions?bucket=completed",
      headers: { host, cookie: session.cookie },
    })
    expect(queue.statusCode).toBe(200)
    expect(Schema.Check(MissionListPageSchema, queue.json())).toBe(true)
    expect(queue.json().total_count).toBe(1)
    expect(queue.json().items[0].subject_references[0].value.value).toBe("release-47")
  })

  it("runs an admitted mission through the configured executor", async () => {
    const run = vi.fn(async () => undefined)
    const { app, sessions } = await setup(undefined, origin, { run })
    const session = await authenticate(app, sessions)
    const headers = {
      host,
      origin,
      cookie: session.cookie,
      "x-idemantis-csrf": session.csrf,
    }
    const created = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers,
      payload: {
        definition_digest: VerificationRuntime.definition_digest,
        input: { subject: "release-47", summary: "Verify the deployment record" },
      },
    })
    const runId = created.json().summary.run_id

    const response = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/run`,
      headers,
    })

    expect(response.statusCode).toBe(200)
    expect(Schema.Check(MissionDetailSchema, response.json())).toBe(true)
    expect(run).toHaveBeenCalledExactlyOnceWith(runId)
  })

  it("rejects execution routes when the server has no mission executor", async () => {
    const { app, sessions } = await setup()
    const session = await authenticate(app, sessions)
    const headers = {
      host,
      origin,
      cookie: session.cookie,
      "x-idemantis-csrf": session.csrf,
    }
    const created = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers,
      payload: {
        definition_digest: VerificationRuntime.definition_digest,
        input: { subject: "release-47", summary: "Verify the deployment record" },
      },
    })

    const runId = created.json().summary.run_id
    const response = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/run`,
      headers,
    })
    const started = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/start`,
      headers,
    })
    const status = await app.inject({
      method: "GET",
      url: `/api/missions/${runId}/execution`,
      headers: { host, cookie: session.cookie },
    })

    expect(response.statusCode).toBe(503)
    expect(response.json().error).toMatchObject({
      code: "RUNNER_UNAVAILABLE",
      message: "Mission execution is not configured",
    })
    expect(started.statusCode).toBe(503)
    expect(started.json().error).toMatchObject({
      code: "RUNNER_UNAVAILABLE",
      message: "Asynchronous execution is not configured",
    })
    expect(status.statusCode).toBe(503)
    expect(status.json().error).toMatchObject({
      code: "RUNNER_UNAVAILABLE",
      message: "Execution status is not configured",
    })
  })

  it("starts execution asynchronously and exposes its status", async () => {
    const statusFor = (runId: RunId) => ({
      schema_version: 1 as const,
      run_id: runId,
      activity: "running" as const,
      updated_at: "2026-10-24T00:00:00.000Z",
      started_at: "2026-10-24T00:00:00.000Z",
    })
    const start = vi.fn((runId: RunId) => ({
      schema_version: 1 as const,
      disposition: "started" as const,
      execution: statusFor(runId),
    }))
    const executionStatus = vi.fn((runId: RunId) => statusFor(runId))
    const { app, sessions } = await setup(undefined, origin, {
      run: vi.fn(async () => undefined),
      start,
      executionStatus,
    })
    const session = await authenticate(app, sessions)
    const headers = {
      host,
      origin,
      cookie: session.cookie,
      "x-idemantis-csrf": session.csrf,
    }
    const created = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers,
      payload: {
        definition_digest: VerificationRuntime.definition_digest,
        input: { subject: "release-47", summary: "Verify the deployment record" },
      },
    })
    const runId = created.json().summary.run_id as string

    const started = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/start`,
      headers,
    })
    const status = await app.inject({
      method: "GET",
      url: `/api/missions/${runId}/execution`,
      headers: { host, cookie: session.cookie },
    })

    expect(started.statusCode).toBe(202)
    expect(Schema.Check(MissionExecutionStartSchema, started.json())).toBe(true)
    expect(status.statusCode).toBe(200)
    expect(Schema.Check(MissionExecutionStatusSchema, status.json())).toBe(true)
    expect(start).toHaveBeenCalledExactlyOnceWith(runId)
    expect(executionStatus).toHaveBeenCalledExactlyOnceWith(runId)
  })

  it("prepares and submits an intervention through the authenticated boundary", async () => {
    const { app, sessions } = await setup()
    const session = await authenticate(app, sessions)
    const headers = {
      host,
      origin,
      cookie: session.cookie,
      "x-idemantis-csrf": session.csrf,
    }
    const created = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers,
      payload: {
        definition_digest: InterventionRuntime.definition_digest,
        input: { subject: "release-47" },
      },
    })
    const runId = created.json().summary.run_id as string
    const request = {
      client_request_id: "http-intervention-0001",
      expected_sequence: 1,
      action_code: "approve_release",
      target: `mission:${runId}`,
      input: { evidence: "Reviewed deployment record" },
      reason: "Evidence matches the release",
    }
    const preparedResponse = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/interventions/prepare`,
      headers,
      payload: request,
    })
    const prepared = preparedResponse.json()

    expect(preparedResponse.statusCode).toBe(201)
    expect(Schema.Check(PreparedInterventionSchema, prepared)).toBe(true)
    const repeatedPreparation = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/interventions/prepare`,
      headers,
      payload: request,
    })
    expect(repeatedPreparation.json()).toEqual(prepared)

    const rejectedBoundary = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/interventions/prepare`,
      headers,
      payload: { ...request, actor_id: "forged-operator" },
    })
    expect(rejectedBoundary.statusCode).toBe(422)

    const submission = {
      client_request_id: prepared.client_request_id,
      proposal_digest: prepared.proposal_digest,
    }
    const submittedResponse = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/interventions/submit`,
      headers,
      payload: submission,
    })
    const receipt = submittedResponse.json()

    expect(submittedResponse.statusCode).toBe(200)
    expect(Schema.Check(InterventionReceiptSchema, receipt)).toBe(true)
    expect(receipt).toMatchObject({ outcome: "accepted", accepted_sequence: 2 })
    const repeatedSubmission = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/interventions/submit`,
      headers,
      payload: submission,
    })
    expect(repeatedSubmission.json()).toEqual(receipt)

    const activity = await app.inject({
      method: "GET",
      url: `/api/missions/${runId}/interventions`,
      headers: { host, cookie: session.cookie },
    })
    const receipts = await app.inject({
      method: "GET",
      url: `/api/missions/${runId}/intervention-receipts`,
      headers: { host, cookie: session.cookie },
    })
    const receiptLookup = await app.inject({
      method: "GET",
      url: `/api/missions/${runId}/interventions/${request.client_request_id}/receipt`,
      headers: { host, cookie: session.cookie },
    })
    expect(Schema.Check(PreparedInterventionPageSchema, activity.json())).toBe(true)
    expect(Schema.Check(InterventionReceiptPageSchema, receipts.json())).toBe(true)
    expect(receiptLookup.json()).toEqual(receipt)
  })

  it("returns bounded error envelopes for invalid routes and oversized requests", async () => {
    const { app } = await setup()
    const missing = await app.inject({
      method: "GET",
      url: "/api/not-a-route",
      headers: { host },
    })
    expect(missing.statusCode).toBe(404)
    expect(missing.json()).toMatchObject({
      schema_version: 1,
      error: { code: "INVALID_REQUEST", message: "API route does not exist" },
    })

    const oversized = await app.inject({
      method: "POST",
      url: "/api/session/bootstrap",
      headers: { host, origin, "content-type": "application/json" },
      payload: JSON.stringify({ value: "x".repeat(129 * 1024) }),
    })
    expect(oversized.statusCode).toBe(413)
    expect(oversized.json()).toMatchObject({
      schema_version: 1,
      error: { code: "INVALID_REQUEST", message: "Request could not be accepted" },
    })
  })

  it("expires sessions without retaining browser authority", async () => {
    let now = new Date("2026-10-24T08:00:00.000Z")
    const { app, sessions } = await setup(() => now)
    const session = await authenticate(app, sessions)
    now = new Date("2026-10-24T16:00:00.000Z")

    const response = await app.inject({
      method: "GET",
      url: "/api/session",
      headers: { host, cookie: session.cookie },
    })
    expect(response.statusCode).toBe(401)
    expect(response.json().error.code).toBe("SESSION_EXPIRED")
  })

  it("revokes the session and clears its cookie", async () => {
    const { app, sessions } = await setup()
    const session = await authenticate(app, sessions)
    const ended = await app.inject({
      method: "DELETE",
      url: "/api/session",
      headers: {
        host,
        origin,
        cookie: session.cookie,
        "x-idemantis-csrf": session.csrf,
      },
    })
    expect(ended.statusCode).toBe(200)
    expect(ended.headers["set-cookie"]).toContain("idemantis_session=;")

    const rejected = await app.inject({
      method: "GET",
      url: "/api/session",
      headers: { host, cookie: session.cookie },
    })
    expect(rejected.statusCode).toBe(401)
  })

  it("streams durable operations and resumes mission events without duplicates", async () => {
    const local = await availableOrigin()
    const { app, sessions, store } = await setup(undefined, local.origin)
    await app.listen({ host: "127.0.0.1", port: local.port })
    const session = await authenticate(app, sessions, local.origin)
    const unauthenticated = await fetch(`${local.origin}/api/streams/operations?after_revision=1`)
    expect(unauthenticated.status).toBe(401)
    const operationsAbort = new AbortController()
    const operationsResponse = await fetch(
      `${local.origin}/api/streams/operations?after_revision=1`,
      {
        headers: { cookie: session.cookie },
        signal: operationsAbort.signal,
      },
    )
    expect(operationsResponse.status).toBe(200)
    expect(operationsResponse.headers.get("content-type")).toBe("text/event-stream; charset=utf-8")
    expect(operationsResponse.headers.get("cache-control")).toBe("no-store")
    expect(operationsResponse.headers.get("x-content-type-options")).toBe("nosniff")
    const nextOperation = sseReader(operationsResponse)
    expect(await nextOperation()).toMatchObject({ kind: "heartbeat" })

    const created = store.admitMission({
      definition_digest: VerificationRuntime.definition_digest,
      input: { subject: "release-47", summary: "Verify the deployment record" },
    })
    const operation = await nextOperation()
    expect(operation).toMatchObject({
      kind: "operations",
      body: { list_revision: 2, changed_run_ids: [created.summary.run_id] },
    })
    operationsAbort.abort()

    const missionAbort = new AbortController()
    const missionResponse = await fetch(
      `${local.origin}/api/missions/${created.summary.run_id}/stream?after_sequence=0`,
      { headers: { cookie: session.cookie }, signal: missionAbort.signal },
    )
    const nextMission = sseReader(missionResponse)
    const mission = await nextMission()
    expect(mission).toMatchObject({
      kind: "mission",
      body: { run_id: created.summary.run_id, sequence: 1 },
    })
    missionAbort.abort()

    const resumedAbort = new AbortController()
    const resumedResponse = await fetch(
      `${local.origin}/api/missions/${created.summary.run_id}/stream?after_sequence=0`,
      {
        headers: { cookie: session.cookie, "last-event-id": mission.resume_cursor },
        signal: resumedAbort.signal,
      },
    )
    expect(await sseReader(resumedResponse)()).toMatchObject({ kind: "heartbeat" })
    resumedAbort.abort()

    const wrongNamespace = await app.inject({
      method: "GET",
      url: `/api/streams/operations?cursor=${encodeURIComponent(mission.resume_cursor)}`,
      headers: { host: new URL(local.origin).host, cookie: session.cookie },
    })
    expect(wrongNamespace.statusCode).toBe(400)
    expect(wrongNamespace.json().error.code).toBe("CURSOR_INVALID")
  })

  it("streams intervention receipts and resumes without duplicates", async () => {
    const local = await availableOrigin()
    const { app, sessions, store } = await setup(undefined, local.origin)
    await app.listen({ host: "127.0.0.1", port: local.port })
    const session = await authenticate(app, sessions, local.origin)
    const created = store.admitMission({
      definition_digest: InterventionRuntime.definition_digest,
      input: { subject: "release-48" },
    })
    const runId = created.summary.run_id
    const abort = new AbortController()
    const response = await fetch(
      `${local.origin}/api/missions/${runId}/receipts/stream?after_revision=0`,
      { headers: { cookie: session.cookie }, signal: abort.signal },
    )
    const nextReceipt = sseReader(response)
    expect(await nextReceipt()).toMatchObject({ kind: "heartbeat" })

    const prepared = store.prepareIntervention(runId, "local-operator", {
      client_request_id: "stream-intervention-0001",
      expected_sequence: 1,
      action_code: "approve_release",
      target: `mission:${runId}`,
      input: { evidence: "Reviewed live stream record" },
      reason: "Evidence matches the release",
    })
    store.submitIntervention(runId, "local-operator", {
      client_request_id: prepared.client_request_id,
      proposal_digest: prepared.proposal_digest,
    })
    const receipt = await nextReceipt()
    expect(receipt).toMatchObject({
      kind: "receipt",
      body: {
        run_id: runId,
        client_request_id: prepared.client_request_id,
        receipt: { outcome: "accepted", accepted_sequence: 2 },
      },
    })
    abort.abort()

    const resumedAbort = new AbortController()
    const resumed = await fetch(`${local.origin}/api/missions/${runId}/receipts/stream`, {
      headers: { cookie: session.cookie, "last-event-id": receipt.resume_cursor },
      signal: resumedAbort.signal,
    })
    expect(await sseReader(resumed)()).toMatchObject({ kind: "heartbeat" })
    resumedAbort.abort()
  })
})
