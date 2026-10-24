import { createHash, randomBytes, timingSafeEqual } from "node:crypto"
import { chmodSync, existsSync } from "node:fs"
import { DatabaseSync } from "node:sqlite"
import type { SourceId } from "@idemantis/contracts"
import { TransportError } from "@idemantis/transport"
import { ulid } from "ulid"

export const ApiKeyScopes = ["view", "operate", "define"] as const

export type ApiKeyScope = (typeof ApiKeyScopes)[number]

export interface ApiKeyRecord {
  key_id: string
  name: string
  scopes: ApiKeyScope[]
  created_at: string
  revoked_at?: string
  last_used_at?: string
}

export interface CreatedApiKey {
  record: ApiKeyRecord
  token: string
}

export interface ApiKeyPrincipal {
  actor_id: SourceId
  key_id: string
  scopes: ApiKeyScope[]
}

export interface ApiKeyStoreOptions {
  path: string
  clock?: () => Date
}

type Row = Record<string, unknown>

const tokenPattern = /^idm_([0-9A-HJKMNP-TV-Z]{26})_([A-Za-z0-9_-]{43})$/
const namePattern = /^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?$/

function digest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest()
}

function text(row: Row, field: string): string {
  const value = row[field]
  if (typeof value !== "string") {
    throw new Error(`API key store returned an invalid ${field}`)
  }
  return value
}

function optionalText(row: Row, field: string): string | undefined {
  const value = row[field]
  return typeof value === "string" ? value : undefined
}

export function parseScopes(value: string | readonly string[]): ApiKeyScope[] {
  const items = typeof value === "string" ? value.split(",").map((item) => item.trim()) : value
  const scopes = [...new Set(items)].filter((item) => item.length > 0)
  if (scopes.length === 0 || scopes.some((scope) => !ApiKeyScopes.includes(scope as ApiKeyScope))) {
    throw new TypeError(`API key scopes must be drawn from ${ApiKeyScopes.join(", ")}`)
  }
  return ApiKeyScopes.filter((scope) => scopes.includes(scope))
}

export class ApiKeyStore {
  readonly #database: DatabaseSync
  readonly #clock: () => Date
  readonly #touched = new Map<string, number>()
  #closed = false

  constructor(options: ApiKeyStoreOptions) {
    this.#clock = options.clock ?? (() => new Date())
    const created = options.path !== ":memory:" && !existsSync(options.path)
    this.#database = new DatabaseSync(options.path)
    if (created) {
      chmodSync(options.path, 0o600)
    }
    this.#database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA busy_timeout = 5000;
      CREATE TABLE IF NOT EXISTS api_keys (
        key_id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        secret_digest BLOB NOT NULL,
        scopes_json TEXT NOT NULL CHECK (json_valid(scopes_json)),
        created_at TEXT NOT NULL,
        revoked_at TEXT,
        last_used_at TEXT
      ) STRICT;
    `)
  }

  create(name: string, scopes: readonly ApiKeyScope[]): CreatedApiKey {
    if (!namePattern.test(name)) {
      throw new TypeError(
        "API key names use lowercase letters, digits and hyphens, up to 64 characters",
      )
    }
    const selected = parseScopes(scopes)
    const keyId = ulid()
    const secret = randomBytes(32).toString("base64url")
    const createdAt = this.#clock().toISOString()
    try {
      this.#database
        .prepare(
          "INSERT INTO api_keys(key_id, name, secret_digest, scopes_json, created_at) VALUES (?, ?, ?, ?, ?)",
        )
        .run(keyId, name, digest(secret), JSON.stringify(selected), createdAt)
    } catch (error) {
      if (error instanceof Error && /UNIQUE/.test(error.message)) {
        throw new TypeError(`An API key named ${name} already exists`)
      }
      throw error
    }
    return {
      record: { key_id: keyId, name, scopes: selected, created_at: createdAt },
      token: `idm_${keyId}_${secret}`,
    }
  }

  list(): ApiKeyRecord[] {
    return this.#database
      .prepare("SELECT * FROM api_keys ORDER BY name")
      .all()
      .map((row) => this.record(row as Row))
  }

  revoke(name: string): ApiKeyRecord {
    const revokedAt = this.#clock().toISOString()
    const result = this.#database
      .prepare("UPDATE api_keys SET revoked_at = ? WHERE name = ? AND revoked_at IS NULL")
      .run(revokedAt, name)
    const row = this.#database.prepare("SELECT * FROM api_keys WHERE name = ?").get(name)
    if (!row) {
      throw new TypeError(`No API key is named ${name}`)
    }
    if (result.changes === 0) {
      throw new TypeError(`API key ${name} is already revoked`)
    }
    return this.record(row as Row)
  }

  authenticate(token: string): ApiKeyPrincipal {
    const [, keyId, secret] = tokenPattern.exec(token) ?? []
    const row =
      keyId && secret
        ? (this.#database.prepare("SELECT * FROM api_keys WHERE key_id = ?").get(keyId) as
            | Row
            | undefined)
        : undefined
    const { secret_digest: stored, revoked_at: revokedAt } = row ?? {}
    const presented = digest(secret ?? "")
    if (
      !row ||
      !(stored instanceof Uint8Array) ||
      stored.byteLength !== presented.byteLength ||
      !timingSafeEqual(stored, presented) ||
      revokedAt !== null
    ) {
      throw new TransportError("SESSION_EXPIRED", "API key is invalid or revoked", 401)
    }
    const record = this.record(row)
    const now = this.#clock()
    if ((this.#touched.get(record.key_id) ?? 0) <= now.getTime() - 60_000) {
      this.#touched.set(record.key_id, now.getTime())
      this.#database
        .prepare("UPDATE api_keys SET last_used_at = ? WHERE key_id = ?")
        .run(now.toISOString(), record.key_id)
    }
    return {
      actor_id: `key:${record.name}` as SourceId,
      key_id: record.key_id,
      scopes: record.scopes,
    }
  }

  close(): void {
    if (!this.#closed) {
      this.#closed = true
      this.#database.close()
    }
  }

  private record(row: Row): ApiKeyRecord {
    const revokedAt = optionalText(row, "revoked_at")
    const lastUsedAt = optionalText(row, "last_used_at")
    return {
      key_id: text(row, "key_id"),
      name: text(row, "name"),
      scopes: parseScopes(JSON.parse(text(row, "scopes_json")) as string[]),
      created_at: text(row, "created_at"),
      ...(revokedAt ? { revoked_at: revokedAt } : {}),
      ...(lastUsedAt ? { last_used_at: lastUsedAt } : {}),
    }
  }
}
