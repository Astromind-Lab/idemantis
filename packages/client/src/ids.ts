import { createHash, randomBytes } from "node:crypto"

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

function encode(bytes: Uint8Array): string {
  let value = 0n
  for (const byte of bytes.subarray(0, 17)) {
    value = (value << 8n) | BigInt(byte)
  }
  let text = ""
  for (let index = 0; index < 26; index += 1) {
    text = alphabet[Number(value & 31n)] + text
    value >>= 5n
  }
  return text
}

export function observationIdFor(key: string): string {
  if (key.length < 1 || key.length > 512) {
    throw new TypeError("Observation keys must contain 1 to 512 characters")
  }
  return `obs_${encode(createHash("sha256").update(`observation:${key}`, "utf8").digest())}`
}

export function randomObservationId(): string {
  return `obs_${encode(randomBytes(17))}`
}

export function clientRequestIdFor(key: string): string {
  if (key.length < 1 || key.length > 512) {
    throw new TypeError("Request keys must contain 1 to 512 characters")
  }
  return `req-${encode(createHash("sha256").update(`request:${key}`, "utf8").digest()).toLowerCase()}`
}
