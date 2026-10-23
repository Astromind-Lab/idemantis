import { createHash } from "node:crypto"
import canonicalize from "canonicalize"
import type { Digest } from "./ids.js"
import type { JsonValue } from "./json.js"

/** Raised when a value cannot be represented as canonical JSON. */
export class CanonicalizationError extends Error {
  override readonly name = "CanonicalizationError"
}

/** Serializes a JSON value using RFC 8785 canonicalization. */
export function canonicalJson(value: JsonValue): string {
  const result = canonicalize(value)

  if (typeof result !== "string") {
    throw new CanonicalizationError("Value cannot be represented as canonical JSON")
  }

  return result
}

/** Returns the canonical SHA-256 identity for a JSON value. */
export function sha256Digest(value: JsonValue): Digest {
  const hash = createHash("sha256").update(canonicalJson(value), "utf8").digest("hex")
  return `sha256:${hash}`
}
