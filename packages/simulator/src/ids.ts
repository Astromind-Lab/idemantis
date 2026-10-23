import type { AdapterInstanceId, AttemptId, CommandId, EffectId, RunId } from "@idemantis/contracts"

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

type ContractIdPrefix = "run" | "cmd" | "att" | "eff" | "adp"
type ContractId<Prefix extends ContractIdPrefix> = Prefix extends "run"
  ? RunId
  : Prefix extends "cmd"
    ? CommandId
    : Prefix extends "att"
      ? AttemptId
      : Prefix extends "eff"
        ? EffectId
        : AdapterInstanceId

function encoded(value: number): string {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError("Simulation identity value must be a non-negative integer")
  }
  let remaining = value
  let result = ""
  do {
    result = `${alphabet[remaining % alphabet.length]}${result}`
    remaining = Math.floor(remaining / alphabet.length)
  } while (remaining > 0)
  return result.padStart(26, "0")
}

export function simulationId<Prefix extends ContractIdPrefix>(
  prefix: Prefix,
  value: number,
): ContractId<Prefix> {
  return `${prefix}_${encoded(value)}` as ContractId<Prefix>
}
