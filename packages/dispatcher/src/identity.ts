import type { AttemptId, EventId } from "@idemantis/contracts"
import { randomBytes } from "node:crypto"
import type { IdentityFactory } from "./types.js"

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

function randomIdentifier(prefix: "att" | "evt"): string {
  const bytes = randomBytes(16)
  let value = 0n
  for (const byte of bytes) {
    value = (value << 8n) | BigInt(byte)
  }

  let encoded = ""
  for (let index = 0; index < 26; index += 1) {
    encoded = `${alphabet[Number(value & 31n)]}${encoded}`
    value >>= 5n
  }

  return `${prefix}_${encoded}`
}

export class SecureIdentityFactory implements IdentityFactory {
  attemptId(): AttemptId {
    return randomIdentifier("att") as AttemptId
  }

  eventId(): EventId {
    return randomIdentifier("evt") as EventId
  }
}
