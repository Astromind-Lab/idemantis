import { createHash } from "node:crypto"
import type {
  CommandId,
  EffectId,
  EventId,
  RunId,
  StateInstanceId,
  TimerId,
} from "@idemantis/contracts"

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

function hashParts(namespace: string, parts: readonly string[]): bigint {
  const hash = createHash("sha256")
  hash.update(`${namespace.length}:${namespace}`)

  for (const part of parts) {
    hash.update(`${part.length}:${part}`)
  }

  return BigInt(`0x${hash.digest("hex").slice(0, 32)}`)
}

function encodeUlid(value: bigint): string {
  let remaining = value
  let encoded = ""

  for (let index = 0; index < 26; index += 1) {
    encoded = `${crockford[Number(remaining & 31n)]}${encoded}`
    remaining >>= 5n
  }

  return encoded
}

function derive(prefix: string, namespace: string, parts: readonly string[]): string {
  return `${prefix}_${encodeUlid(hashParts(namespace, parts))}`
}

export function deriveStateInstanceId(
  runId: RunId,
  stateName: string,
  eventId: EventId,
): StateInstanceId {
  return derive("sti", "state-instance", [runId, stateName, eventId]) as StateInstanceId
}

export function deriveEffectId(
  runId: RunId,
  stateInstanceId: StateInstanceId,
  effectSlot: string,
): EffectId {
  return derive("eff", "effect", [runId, stateInstanceId, effectSlot]) as EffectId
}

export function deriveCommandId(eventId: EventId, commandSlot: string): CommandId {
  return derive("cmd", "command", [eventId, commandSlot]) as CommandId
}

export function deriveTimerId(runId: RunId, stateInstanceId: StateInstanceId): TimerId {
  return derive("tmr", "timer", [runId, stateInstanceId]) as TimerId
}

export function deriveRetryTimerId(
  runId: RunId,
  stateInstanceId: StateInstanceId,
  issueCount: number,
): TimerId {
  return derive("tmr", "effect-retry", [runId, stateInstanceId, issueCount.toString()]) as TimerId
}
