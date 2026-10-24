import { createHmac, timingSafeEqual } from "node:crypto"
import { canonicalJson, sha256Digest, type JsonObject } from "@idemantis/contracts"
import { TransportError } from "./errors.js"

interface CursorPayload extends JsonObject {
  namespace: string
  position: number
  revision: number
  query_digest: string
  issued_at: string
  expires_at: string
}

export interface CursorBinding {
  namespace: string
  revision: number
  query: JsonObject
}

export interface CursorState extends CursorBinding {
  position: number
  issued_at: string
  expires_at: string
}

export interface CursorCodecOptions {
  secret: Uint8Array
  lifetime_ms?: number
  clock?: () => Date
}

function payloadSegment(payload: CursorPayload): string {
  return Buffer.from(canonicalJson(payload), "utf8").toString("base64url")
}

function signature(segment: string, secret: Uint8Array): Buffer {
  return createHmac("sha256", secret).update(segment).digest()
}

function invalidCursor(message: string): TransportError {
  return new TransportError("CURSOR_INVALID", message, 400)
}

export class CursorCodec {
  readonly #secret: Uint8Array
  readonly #lifetimeMs: number
  readonly #clock: () => Date

  constructor(options: CursorCodecOptions) {
    if (options.secret.byteLength < 32) {
      throw new RangeError("Cursor secret must contain at least 32 bytes")
    }
    this.#secret = options.secret
    this.#lifetimeMs = options.lifetime_ms ?? 15 * 60_000
    this.#clock = options.clock ?? (() => new Date())
    if (!Number.isSafeInteger(this.#lifetimeMs) || this.#lifetimeMs < 1) {
      throw new RangeError("Cursor lifetime must be a positive safe integer")
    }
  }

  encode(binding: CursorBinding, position: number): string {
    if (!Number.isSafeInteger(position) || position < 0) {
      throw new RangeError("Cursor position must be a non-negative safe integer")
    }
    const issued = this.#clock()
    const payload: CursorPayload = {
      namespace: binding.namespace,
      position,
      revision: binding.revision,
      query_digest: sha256Digest(binding.query),
      issued_at: issued.toISOString(),
      expires_at: new Date(issued.getTime() + this.#lifetimeMs).toISOString(),
    }
    const segment = payloadSegment(payload)
    return `${segment}.${signature(segment, this.#secret).toString("base64url")}`
  }

  decode(cursor: string, binding: CursorBinding): CursorState {
    const [segment, encodedSignature, extra] = cursor.split(".")
    if (!segment || !encodedSignature || extra !== undefined) {
      throw invalidCursor("Cursor encoding is invalid")
    }
    const expected = signature(segment, this.#secret)
    let actual: Buffer
    try {
      actual = Buffer.from(encodedSignature, "base64url")
    } catch {
      throw invalidCursor("Cursor signature is invalid")
    }
    if (actual.byteLength !== expected.byteLength || !timingSafeEqual(actual, expected)) {
      throw invalidCursor("Cursor signature is invalid")
    }
    let payload: CursorPayload
    try {
      const decoded = JSON.parse(Buffer.from(segment, "base64url").toString("utf8")) as unknown
      if (decoded === null || typeof decoded !== "object" || Array.isArray(decoded)) {
        throw new TypeError("Cursor payload must be an object")
      }
      payload = decoded as CursorPayload
    } catch {
      throw invalidCursor("Cursor payload is invalid")
    }
    if (
      payload.namespace !== binding.namespace ||
      payload.revision !== binding.revision ||
      payload.query_digest !== sha256Digest(binding.query) ||
      !Number.isSafeInteger(payload.position) ||
      payload.position < 0 ||
      !Number.isFinite(Date.parse(payload.issued_at)) ||
      !Number.isFinite(Date.parse(payload.expires_at))
    ) {
      throw invalidCursor("Cursor does not match this query")
    }
    if (Date.parse(payload.expires_at) <= this.#clock().getTime()) {
      throw new TransportError("CURSOR_EXPIRED", "Cursor has expired", 410)
    }
    return {
      namespace: binding.namespace,
      revision: binding.revision,
      query: binding.query,
      position: payload.position,
      issued_at: payload.issued_at,
      expires_at: payload.expires_at,
    }
  }
}
