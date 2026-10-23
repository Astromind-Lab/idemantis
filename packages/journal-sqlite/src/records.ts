import {
  AcceptedEventSchema,
  AttemptRecordSchema,
  CommandStatusSchema,
  CommandRecordSchema,
  DigestSchema,
  EventAcceptanceSchema,
  ProposedEventSchema,
  TimerRecordSchema,
  canonicalJson,
  createContractParser,
  sha256Digest,
  type AcceptedEvent,
  type AttemptRecord,
  type CommandStatus,
  type CommandRecord,
  type Digest,
  type EventAcceptance,
  type JsonObject,
  type ProposedEvent,
  type TimerRecord,
} from "@idemantis/contracts"
import {
  EffectProjectionSchema,
  MissionStateSchema,
  type EffectProjection,
  type MissionState,
} from "@idemantis/engine"
import { MissionManifestSchema, type MissionManifest } from "@idemantis/sdk"
import Schema from "typebox/schema"
import { JournalError } from "./errors.js"

const parseAcceptedEventContract = createContractParser(AcceptedEventSchema)
const parseCommandContract = createContractParser(CommandRecordSchema)
const parseAttemptContract = createContractParser(AttemptRecordSchema)
const parseTimerContract = createContractParser(TimerRecordSchema)
const parseEventAcceptanceContract = createContractParser(EventAcceptanceSchema)
export const parseProposedEvent = createContractParser(ProposedEventSchema)
const stateValidator = Schema.Compile(MissionStateSchema)
const effectValidator = Schema.Compile(EffectProjectionSchema)
const manifestValidator = Schema.Compile(MissionManifestSchema)

export function parseJson(text: string, label: string): unknown {
  try {
    return JSON.parse(text) as unknown
  } catch {
    throw new JournalError("CORRUPT_RECORD", `${label} is not valid JSON`)
  }
}

export function parseAcceptedEvent(text: string): AcceptedEvent {
  try {
    return parseAcceptedEventContract(parseJson(text, "Stored event"))
  } catch (error) {
    if (error instanceof JournalError) {
      throw error
    }
    throw new JournalError("CORRUPT_RECORD", "Stored event does not satisfy its contract")
  }
}

export function parseCommand(text: string): CommandRecord {
  try {
    return parseCommandContract(parseJson(text, "Stored command"))
  } catch (error) {
    if (error instanceof JournalError) {
      throw error
    }
    throw new JournalError("CORRUPT_RECORD", "Stored command does not satisfy its contract")
  }
}

export function parseAttempt(value: unknown): AttemptRecord {
  try {
    return parseAttemptContract(value)
  } catch {
    throw new JournalError("CORRUPT_RECORD", "Stored attempt does not satisfy its contract")
  }
}

export function parseTimer(value: unknown): TimerRecord {
  try {
    return parseTimerContract(value)
  } catch {
    throw new JournalError("CORRUPT_RECORD", "Stored timer does not satisfy its contract")
  }
}

export function parseCommandStatus(value: unknown): CommandStatus {
  if (!Schema.Check(CommandStatusSchema, value)) {
    throw new JournalError("CORRUPT_RECORD", "Stored command status is invalid")
  }

  return value
}

export function parseState(text: string): MissionState {
  const value = parseJson(text, "Stored mission state")
  if (!stateValidator.Check(value)) {
    throw new JournalError("CORRUPT_RECORD", "Stored mission state does not satisfy its contract")
  }

  return value
}

export function parseEffect(text: string): EffectProjection {
  const value = parseJson(text, "Stored effect projection")
  if (!effectValidator.Check(value)) {
    throw new JournalError(
      "CORRUPT_RECORD",
      "Stored effect projection does not satisfy its contract",
    )
  }

  return value
}

export function parseManifest(text: string): MissionManifest {
  const value = parseJson(text, "Stored mission manifest")
  if (!manifestValidator.Check(value)) {
    throw new JournalError(
      "CORRUPT_RECORD",
      "Stored mission manifest does not satisfy its contract",
    )
  }

  return value
}

export function parseDigest(value: unknown, label: string): Digest {
  if (!Schema.Check(DigestSchema, value)) {
    throw new JournalError("CORRUPT_RECORD", `${label} is not a valid digest`)
  }

  return value
}

export function proposalDigest(proposal: ProposedEvent): Digest {
  return sha256Digest(proposal as unknown as JsonObject)
}

export function eventJson(event: AcceptedEvent): string {
  return canonicalJson(event as unknown as JsonObject)
}

export function commandJson(command: CommandRecord): string {
  return canonicalJson(command as unknown as JsonObject)
}

export function stateJson(state: MissionState): string {
  return canonicalJson(state as unknown as JsonObject)
}

export function manifestJson(manifest: MissionManifest): string {
  return canonicalJson(manifest as unknown as JsonObject)
}

export function parseAcceptance(value: unknown): EventAcceptance {
  try {
    return parseEventAcceptanceContract(value)
  } catch {
    throw new JournalError("CORRUPT_RECORD", "Stored event acceptance is invalid")
  }
}
