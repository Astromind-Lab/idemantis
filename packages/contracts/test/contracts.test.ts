import fc from "fast-check"
import Schema from "typebox/schema"
import { describe, expect, it } from "vitest"
import {
  AcceptedEventSchema,
  AttemptRecordSchema,
  EffectRecordSchema,
  MissionRunSchema,
  ProposedEventSchema,
  canonicalJson,
  createContractParser,
  sha256Digest,
  type JsonObject,
  type ProposedEvent,
} from "../src/index.js"

const ids = {
  adapter: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  attempt: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  command: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  effect: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  event: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  run: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  state: "sti_01ARZ3NDEKTSV4RRFFQ69G5FAV",
} as const

const digest = `sha256:${"a".repeat(64)}`
const timestamp = "2026-10-23T08:00:00Z"

const proposedEvent: ProposedEvent = {
  event_id: ids.event,
  run_id: ids.run,
  type: "effect.dispatch_started",
  schema_version: 1,
  source: {
    kind: "runner",
    source_id: "runner.local",
  },
  ownership_epoch: 1,
  payload: {
    attempt_id: ids.attempt,
    effect_id: ids.effect,
  },
}

describe("closed contracts", () => {
  it("accepts a valid proposed event", () => {
    const parse = createContractParser(ProposedEventSchema)

    expect(parse(proposedEvent)).toEqual(proposedEvent)
  })

  it("rejects unknown envelope properties", () => {
    const validator = Schema.Compile(ProposedEventSchema)

    expect(validator.Check({ ...proposedEvent, unexpected: true })).toBe(false)
  })

  it("rejects malformed stable identifiers", () => {
    const validator = Schema.Compile(ProposedEventSchema)

    expect(validator.Check({ ...proposedEvent, event_id: "event-1" })).toBe(false)
  })

  it("validates representative mission, effect, attempt, and event records", () => {
    const mission = {
      run_id: ids.run,
      definition: {
        definition_id: "delivery",
        definition_version: "1.0.0",
        definition_digest: digest,
        manifest_schema_version: 1,
        minimum_interpreter_version: "1.0.0",
      },
      execution_binding_digest: digest,
      input: { destination: "bay-7" },
      status: "running",
      current_sequence: 3,
      ownership_epoch: 1,
      created_at: timestamp,
      admitted_at: timestamp,
      outcome_challenged: false,
    }
    const effect = {
      effect_id: ids.effect,
      run_id: ids.run,
      state_instance_id: ids.state,
      step_id: "reserveDestination",
      effect_slot: "reservation",
      status: "dispatching",
      effect_class: "resource_reservation",
      consequence: "compensatable",
      required: true,
      operation: {
        adapter_instance_id: ids.adapter,
        adapter_package_digest: digest,
        capability_manifest_digest: digest,
        operation: "resource.reserve",
        operation_schema_version: 1,
      },
      capabilities: {
        duplicate_safety: "keyed_uniqueness",
        reconciliation: "authoritative",
        fencing_mode: "application_epoch",
        fencing_scope: "resource",
        handoff_mode: "settle_first",
        execution_location: "either",
        cancellation: true,
        compensation: true,
        observation_stream: false,
      },
      input: { destination: "bay-7" },
      ownership_epoch: 1,
      created_at: timestamp,
      updated_at: timestamp,
    }
    const attempt = {
      attempt_id: ids.attempt,
      command_id: ids.command,
      effect_id: ids.effect,
      status: "dispatch_started",
      ownership_epoch: 1,
      claimed_by: "runner.local",
      claim_deadline: timestamp,
      started_event_id: ids.event,
      created_at: timestamp,
      updated_at: timestamp,
    }
    const acceptedEvent = {
      ...proposedEvent,
      sequence: 3,
      recorded_at: timestamp,
    }

    expect(Schema.Compile(MissionRunSchema).Check(mission)).toBe(true)
    expect(Schema.Compile(EffectRecordSchema).Check(effect)).toBe(true)
    expect(Schema.Compile(AttemptRecordSchema).Check(attempt)).toBe(true)
    expect(Schema.Compile(AcceptedEventSchema).Check(acceptedEvent)).toBe(true)
  })
})

describe("canonical identity", () => {
  it("produces the same digest for different object key order", () => {
    const left: JsonObject = { alpha: 1, nested: { beta: true, gamma: "value" } }
    const right: JsonObject = { nested: { gamma: "value", beta: true }, alpha: 1 }

    expect(canonicalJson(left)).toBe(canonicalJson(right))
    expect(sha256Digest(left)).toBe(sha256Digest(right))
  })

  it("changes the digest when event content changes under the same event ID", () => {
    const changed: ProposedEvent = {
      ...proposedEvent,
      payload: { ...proposedEvent.payload, effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAW" },
    }

    expect(sha256Digest(proposedEvent)).not.toBe(sha256Digest(changed))
  })

  it("survives JSON serialization for arbitrary JSON objects", () => {
    fc.assert(
      fc.property(fc.dictionary(fc.string(), fc.jsonValue()), (value) => {
        const original = value as JsonObject
        const roundTrip = JSON.parse(JSON.stringify(value)) as JsonObject

        expect(sha256Digest(original)).toBe(sha256Digest(roundTrip))
      }),
    )
  })
})
