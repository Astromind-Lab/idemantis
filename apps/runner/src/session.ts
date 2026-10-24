import { createHash, randomBytes, timingSafeEqual } from "node:crypto"
import {
  ApiSchemaVersion,
  StreamSchemaVersion,
  type SessionDocument,
  TransportError,
} from "@idemantis/transport"

interface BootstrapRecord {
  digest: Buffer
  expires_at: number
  consumed: boolean
}

interface SessionRecord {
  expires_at: number
  csrf_digest: Buffer
  csrf_expires_at: number
  csrf_token: string
}

export interface SessionAuthorityOptions {
  clock?: () => Date
  bootstrap_lifetime_ms?: number
  session_lifetime_ms?: number
  csrf_lifetime_ms?: number
}

export interface CreatedSession {
  cookie_token: string
  document: SessionDocument
}

function secret(): string {
  return randomBytes(32).toString("base64url")
}

function digest(value: string): Buffer {
  return createHash("sha256").update(value, "utf8").digest()
}

function equal(left: Buffer, right: Buffer): boolean {
  return left.byteLength === right.byteLength && timingSafeEqual(left, right)
}

export class SessionAuthority {
  readonly #clock: () => Date
  readonly #bootstrapLifetimeMs: number
  readonly #sessionLifetimeMs: number
  readonly #csrfLifetimeMs: number
  readonly #sessions = new Map<string, SessionRecord>()
  #bootstrap?: BootstrapRecord

  constructor(options: SessionAuthorityOptions = {}) {
    this.#clock = options.clock ?? (() => new Date())
    this.#bootstrapLifetimeMs = options.bootstrap_lifetime_ms ?? 5 * 60_000
    this.#sessionLifetimeMs = options.session_lifetime_ms ?? 8 * 60 * 60_000
    this.#csrfLifetimeMs = options.csrf_lifetime_ms ?? 15 * 60_000
    if (
      !Number.isSafeInteger(this.#bootstrapLifetimeMs) ||
      this.#bootstrapLifetimeMs < 1 ||
      !Number.isSafeInteger(this.#sessionLifetimeMs) ||
      this.#sessionLifetimeMs < 1 ||
      !Number.isSafeInteger(this.#csrfLifetimeMs) ||
      this.#csrfLifetimeMs < 1
    ) {
      throw new RangeError("Session lifetimes must be positive safe integers")
    }
  }

  get sessionLifetimeSeconds(): number {
    return Math.ceil(this.#sessionLifetimeMs / 1000)
  }

  issueBootstrap(): string {
    const value = secret()
    this.#bootstrap = {
      digest: digest(value),
      expires_at: this.#clock().getTime() + this.#bootstrapLifetimeMs,
      consumed: false,
    }
    return value
  }

  createSession(bootstrapSecret: string): CreatedSession {
    const now = this.#clock().getTime()
    if (
      !this.#bootstrap ||
      this.#bootstrap.consumed ||
      this.#bootstrap.expires_at <= now ||
      !equal(this.#bootstrap.digest, digest(bootstrapSecret))
    ) {
      throw new TransportError("SESSION_EXPIRED", "Bootstrap secret is invalid or expired", 401)
    }
    this.#bootstrap.consumed = true
    const cookieToken = secret()
    const csrfToken = secret()
    const record: SessionRecord = {
      expires_at: now + this.#sessionLifetimeMs,
      csrf_digest: digest(csrfToken),
      csrf_expires_at: now + this.#csrfLifetimeMs,
      csrf_token: csrfToken,
    }
    this.#sessions.set(digest(cookieToken).toString("hex"), record)
    return { cookie_token: cookieToken, document: this.document(record) }
  }

  session(cookieToken: string | undefined): SessionDocument {
    const record = this.record(cookieToken)
    const now = this.#clock().getTime()
    if (record.csrf_expires_at <= now) {
      record.csrf_token = secret()
      record.csrf_digest = digest(record.csrf_token)
      record.csrf_expires_at = now + this.#csrfLifetimeMs
    }
    return this.document(record)
  }

  assertActive(cookieToken: string | undefined): void {
    this.record(cookieToken)
  }

  assertCsrf(cookieToken: string | undefined, csrfToken: string | undefined): void {
    const record = this.record(cookieToken)
    if (
      !csrfToken ||
      record.csrf_expires_at <= this.#clock().getTime() ||
      !equal(record.csrf_digest, digest(csrfToken))
    ) {
      throw new TransportError("ACTION_FORBIDDEN", "Anti-forgery token is invalid or expired", 403)
    }
  }

  revoke(cookieToken: string | undefined): void {
    if (cookieToken) {
      this.#sessions.delete(digest(cookieToken).toString("hex"))
    }
  }

  private record(cookieToken: string | undefined): SessionRecord {
    if (!cookieToken) {
      throw new TransportError("SESSION_EXPIRED", "Operator session is required", 401)
    }
    const key = digest(cookieToken).toString("hex")
    const record = this.#sessions.get(key)
    if (!record || record.expires_at <= this.#clock().getTime()) {
      this.#sessions.delete(key)
      throw new TransportError("SESSION_EXPIRED", "Operator session has expired", 401)
    }
    return record
  }

  private document(record: SessionRecord): SessionDocument {
    return {
      schema_version: ApiSchemaVersion,
      actor_id: "local-operator",
      capabilities: ["view", "operate"],
      csrf_token: record.csrf_token,
      csrf_expires_at: new Date(record.csrf_expires_at).toISOString(),
      session_expires_at: new Date(record.expires_at).toISOString(),
      api_schema_versions: [ApiSchemaVersion],
      stream_schema_versions: [StreamSchemaVersion],
      renewal: "bootstrap_required",
    }
  }
}
