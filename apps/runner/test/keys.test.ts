import { mkdtempSync, rmSync, statSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterEach, describe, expect, it } from "vitest"
import { runCli } from "../src/cli.js"
import { ApiKeyStore, parseScopes } from "../src/keys.js"
import { type StartedLocalRunner, startLocalRunner } from "../src/local.js"

const directories: string[] = []
const stores: ApiKeyStore[] = []
const runners: StartedLocalRunner[] = []

function directory(): string {
  const created = mkdtempSync(join(tmpdir(), "idemantis-keys-"))
  directories.push(created)
  return created
}

function store(path: string): ApiKeyStore {
  const opened = new ApiKeyStore({ path })
  stores.push(opened)
  return opened
}

afterEach(async () => {
  for (const runner of runners.splice(0)) {
    await runner.close()
  }
  for (const opened of stores.splice(0)) {
    opened.close()
  }
  for (const created of directories.splice(0)) {
    rmSync(created, { recursive: true, force: true })
  }
})

describe("API keys", () => {
  it("stores only a digest and authenticates the issued token", () => {
    const path = join(directory(), "access.sqlite")
    const keys = store(path)

    const created = keys.create("planner-agent", ["view", "operate"])
    const principal = keys.authenticate(created.token)

    expect(statSync(path).mode & 0o777).toBe(0o600)
    expect(created.token).toMatch(/^idm_[0-9A-Z]{26}_[A-Za-z0-9_-]{43}$/)
    expect(principal).toEqual({
      actor_id: "key:planner-agent",
      key_id: created.record.key_id,
      scopes: ["view", "operate"],
    })
    expect(JSON.stringify(keys.list())).not.toContain(created.token.slice(-43))
    expect(keys.list()[0]?.last_used_at).toBeDefined()
    const forged = `${created.token.slice(0, -1)}${created.token.endsWith("A") ? "B" : "A"}`
    expect(() => keys.authenticate(forged)).toThrow(expect.objectContaining({ status: 401 }))
    expect(() => keys.authenticate("idm_short")).toThrow(expect.objectContaining({ status: 401 }))
    expect(() => keys.create("planner-agent", ["view"])).toThrow(TypeError)
    expect(() => keys.create("Planner Agent", ["view"])).toThrow(TypeError)

    expect(keys.revoke("planner-agent").revoked_at).toBeDefined()
    expect(() => keys.authenticate(created.token)).toThrow(expect.objectContaining({ status: 401 }))
    expect(() => keys.revoke("planner-agent")).toThrow(TypeError)
    expect(() => keys.revoke("missing")).toThrow(TypeError)
  })

  it("parses scope lists", () => {
    expect(parseScopes("operate, view")).toEqual(["view", "operate"])
    expect(parseScopes(["define"])).toEqual(["define"])
    expect(() => parseScopes("")).toThrow(TypeError)
    expect(() => parseScopes("view,admin")).toThrow(TypeError)
  })

  it("lets a keyed client call the API without a browser session", async () => {
    const local = await startLocalRunner({
      port: 0,
      state_directory: directory(),
      reference_transition_delay_ms: 0,
      resume: false,
    })
    runners.push(local)
    const keys = store(local.access_path)
    const operator = keys.create("operator-agent", ["view", "operate"]).token
    const viewer = keys.create("viewer-agent", ["view"]).token
    const call = (path: string, token: string | undefined, body?: unknown) =>
      fetch(`${local.origin}${path}`, {
        method: body === undefined ? "GET" : "POST",
        headers: {
          ...(token ? { authorization: token.startsWith("idm_") ? `Bearer ${token}` : token } : {}),
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      })

    const listed = await call("/api/missions", operator)
    const admitted = await call("/api/missions", operator, {
      definition_digest: local.definition_digest,
      input: local.reference_input,
    })
    const viewerAdmit = await call("/api/missions", viewer, {
      definition_digest: local.definition_digest,
      input: local.reference_input,
    })
    const viewerRead = await call("/api/definitions", viewer)
    const defineDenied = await call("/api/definitions", operator, { runtime: {} })
    const anonymous = await call("/api/missions", undefined)
    const malformed = await call("/api/missions", "Bearer")
    const forged = await call(
      "/api/missions",
      `${operator.slice(0, -1)}${operator.endsWith("x") ? "y" : "x"}`,
    )
    keys.revoke("operator-agent")
    const revoked = await call("/api/missions", operator)

    expect(listed.status).toBe(200)
    expect(admitted.status).toBe(201)
    expect(viewerAdmit.status).toBe(403)
    expect(viewerRead.status).toBe(200)
    expect(defineDenied.status).toBe(403)
    expect(anonymous.status).toBe(401)
    expect(malformed.status).toBe(401)
    expect(forged.status).toBe(401)
    expect(revoked.status).toBe(401)
  })

  it("admits a mission once per client request even when an agent retries", async () => {
    const local = await startLocalRunner({
      port: 0,
      state_directory: directory(),
      reference_transition_delay_ms: 0,
      resume: false,
    })
    runners.push(local)
    const keys = store(local.access_path)
    const first = keys.create("agent-one", ["view", "operate"]).token
    const second = keys.create("agent-two", ["view", "operate"]).token
    const admit = async (token: string, body: Record<string, unknown>, headers = {}) => {
      const response = await fetch(`${local.origin}/api/missions`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
          ...headers,
        },
        body: JSON.stringify({
          definition_digest: local.definition_digest,
          input: local.reference_input,
          ...body,
        }),
      })
      return {
        status: response.status,
        run: ((await response.json()) as { summary?: { run_id: string } }).summary?.run_id,
      }
    }

    const original = await admit(first, { client_request_id: "order-1001-attempt" })
    const retried = await admit(first, { client_request_id: "order-1001-attempt" })
    const viaHeader = await admit(first, {}, { "idempotency-key": "order-1001-attempt" })
    const otherAgent = await admit(second, { client_request_id: "order-1001-attempt" })
    const conflicting = await admit(first, {
      client_request_id: "order-1001-attempt",
      input: { ...local.reference_input, mission_id: "different-mission" },
    })
    const short = await admit(first, { client_request_id: "o-7" })
    const malformed = await admit(first, { client_request_id: "has spaces" })

    expect(original.status).toBe(201)
    expect(retried).toEqual({ status: 200, run: original.run })
    expect(viaHeader).toEqual({ status: 200, run: original.run })
    expect(otherAgent.status).toBe(201)
    expect(otherAgent.run).not.toBe(original.run)
    expect(conflicting.status).toBe(409)
    expect(short.status).toBe(201)
    expect(malformed.status).toBe(422)
  })

  it("creates, lists, and revokes keys from the command line", async () => {
    const state = directory()
    const output: string[] = []
    const write = (value: string) => {
      output.push(value)
    }

    expect(
      await runCli(["keys", "create", "--name", "ci-agent", "--state-dir", state], write),
    ).toBe(0)
    const created = JSON.parse(output.pop() ?? "{}") as { token: string; scopes: string[] }
    expect(await runCli(["keys", "list", "--state-dir", state], write)).toBe(0)
    const listed = JSON.parse(output.pop() ?? "[]") as { name: string }[]
    expect(
      await runCli(["keys", "revoke", "--name", "ci-agent", "--state-dir", state], write),
    ).toBe(0)

    expect(created.scopes).toEqual(["view", "operate"])
    expect(created.token).toMatch(/^idm_/)
    expect(listed.map((key) => key.name)).toEqual(["ci-agent"])
    expect(JSON.stringify(listed)).not.toContain(created.token)
    await expect(runCli(["keys", "create", "--state-dir", state], write)).rejects.toThrow(TypeError)
  })
})
