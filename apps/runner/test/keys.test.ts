import { mkdtempSync, rmSync, statSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { afterEach, describe, expect, it } from "vitest"
import { ApiKeyStore, parseScopes } from "../src/keys.js"

const directories: string[] = []
const stores: ApiKeyStore[] = []

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

afterEach(() => {
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
})
