import { randomBytes } from "node:crypto"
import type { JsonObject } from "@idemantis/contracts"
import { defineMission, finalState } from "@idemantis/sdk"
import Type from "typebox"
import Schema from "typebox/schema"
import { describe, expect, it } from "vitest"
import {
  CursorCodec,
  compareMissionPriority,
  GraphArtifactSchema,
  graphArtifact,
  MissionExecutionStartSchema,
  MissionExecutionStatusSchema,
  MissionListPageSchema,
  MissionContextSchema,
  ObservationReceiptSchema,
  queueBucketFor,
  RecordObservationRequestSchema,
  TransportError,
  validateInterventionInput,
  validateInterventionSchema,
  verifyGraphArtifact,
} from "../src/index.js"

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const definitionDigest = `sha256:${"a".repeat(64)}` as const

describe("transport contracts", () => {
  it("assigns every mission to one precedence-ordered queue bucket", () => {
    expect(queueBucketFor({ attention: true, terminal: true, waiting: true })).toBe(
      "needs_attention",
    )
    expect(queueBucketFor({ attention: false, terminal: true, waiting: true })).toBe("completed")
    expect(queueBucketFor({ attention: false, terminal: false, waiting: true })).toBe("waiting")
    expect(queueBucketFor({ attention: false, terminal: false, waiting: false })).toBe("active")
  })

  it("orders equal priority missions by stable run identity", () => {
    const values = [
      { priority_rank: 4, run_id: "run_b" },
      { priority_rank: 2, run_id: "run_c" },
      { priority_rank: 2, run_id: "run_a" },
    ]

    expect(values.sort(compareMissionPriority)).toEqual([
      { priority_rank: 2, run_id: "run_a" },
      { priority_rank: 2, run_id: "run_c" },
      { priority_rank: 4, run_id: "run_b" },
    ])
  })

  it("binds opaque cursors to namespace, query, revision, and expiry", () => {
    let now = new Date("2026-10-24T00:00:00.000Z")
    const codec = new CursorCodec({
      secret: randomBytes(32),
      lifetime_ms: 60_000,
      clock: () => now,
    })
    const binding = {
      namespace: "missions",
      revision: 7,
      query: { bucket: "active", query: "vehicle-7" },
    }
    const cursor = codec.encode(binding, 50)

    expect(codec.decode(cursor, binding).position).toBe(50)
    expect(() => codec.decode(`${cursor}x`, binding)).toThrowError(TransportError)
    expect(() => codec.decode(cursor, { ...binding, revision: 8 })).toThrowError(
      expect.objectContaining({ code: "CURSOR_INVALID" }),
    )
    now = new Date("2026-10-24T00:01:00.000Z")
    expect(() => codec.decode(cursor, binding)).toThrowError(
      expect.objectContaining({ code: "CURSOR_EXPIRED" }),
    )
  })

  it("emits a deterministic graph artifact with a verifiable digest", () => {
    const mission = defineMission({
      id: "transport-proof",
      version: "1.0.0",
      input: Type.Object({}, { additionalProperties: false }),
      initial: "complete",
      states: { complete: finalState("completed") },
    })

    const first = graphArtifact(mission.manifest, mission.definitionDigest)
    const second = graphArtifact(mission.manifest, mission.definitionDigest)

    expect(first).toEqual(second)
    expect(Schema.Check(GraphArtifactSchema, first)).toBe(true)
    expect(verifyGraphArtifact(first)).toBe(true)
    expect(verifyGraphArtifact({ ...first, entry_state_id: "changed" })).toBe(false)
  })

  it("validates asynchronous execution lifecycle responses", () => {
    const execution = {
      schema_version: 1,
      run_id: runId,
      activity: "running",
      updated_at: "2026-10-24T00:00:00.000Z",
      started_at: "2026-10-24T00:00:00.000Z",
    }

    expect(Schema.Check(MissionExecutionStatusSchema, execution)).toBe(true)
    expect(
      Schema.Check(MissionExecutionStartSchema, {
        schema_version: 1,
        disposition: "started",
        execution,
      }),
    ).toBe(true)
    expect(
      Schema.Check(MissionExecutionStatusSchema, {
        ...execution,
        failure_code: "EXECUTION_FAILED",
      }),
    ).toBe(false)
    expect(
      Schema.Check(MissionExecutionStatusSchema, {
        ...execution,
        activity: "failed",
        failure_code: "provider_stack_trace",
      }),
    ).toBe(false)
    expect(
      Schema.Check(MissionExecutionStatusSchema, {
        ...execution,
        activity: "failed",
        finished_at: "2026-10-24T00:00:01.000Z",
        failure_code: "EXECUTION_FAILED",
      }),
    ).toBe(true)
  })

  it("validates external observation requests and receipts", () => {
    const request = {
      observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      observation_type: "delivery.handoff_confirmed",
      payload: { recipient: "payload-42" },
      subject: "delivery-001",
      observed_at: "2026-10-24T00:00:00.000Z",
    }

    expect(Schema.Check(RecordObservationRequestSchema, request)).toBe(true)
    expect(
      Schema.Check(RecordObservationRequestSchema, { ...request, observation_type: "Handoff" }),
    ).toBe(false)
    expect(Schema.Check(RecordObservationRequestSchema, { ...request, source: "operator" })).toBe(
      false,
    )
    expect(
      Schema.Check(ObservationReceiptSchema, {
        schema_version: 1,
        run_id: runId,
        observation_id: request.observation_id,
        event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        sequence: 4,
        disposition: "duplicate",
      }),
    ).toBe(true)
  })

  it("validates the mission resume context", () => {
    const context = {
      schema_version: 1,
      run_id: runId,
      definition_id: "agent.research",
      definition_version: "1.0.0",
      status: "waiting",
      current_state: "await_review",
      state_kind: "observation_wait",
      terminal_outcome: null,
      sequence: 7,
      state_digest: `sha256:${"a".repeat(64)}`,
      input: { topic: "fleet charging" },
      effects: [
        {
          effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          state_name: "draft",
          operation: "llm.draft",
          status: "settled",
          phase: "done",
          issue_count: 1,
          output: { draft: "..." },
        },
      ],
      next: {
        actor: "observation",
        summary: "Waiting for review.approved",
        observation_type: "review.approved",
        effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        deadline: "2026-10-24T01:00:00.000Z",
      },
      recent_events: [
        {
          sequence: 7,
          event_type: "effect.issue_result_recorded",
          recorded_at: "2026-10-24T00:00:00.000Z",
        },
      ],
    }

    expect(Schema.Check(MissionContextSchema, context)).toBe(true)
    expect(Schema.Check(MissionContextSchema, { ...context, state_kind: "sleeping" })).toBe(false)
    expect(
      Schema.Check(MissionContextSchema, { ...context, next: { actor: "agent", summary: "x" } }),
    ).toBe(false)
  })

  it("enforces the bounded intervention form subset", () => {
    const form = {
      type: "object",
      properties: {
        reason: { type: "string", minLength: 3, maxLength: 512 },
        approved: { type: "boolean" },
        evidence: {
          type: "array",
          items: { type: "string", maxLength: 128 },
          maxItems: 5,
        },
      },
      required: ["reason", "approved"],
      additionalProperties: false,
      maxProperties: 3,
    } as unknown as JsonObject

    expect(() => validateInterventionSchema(form)).not.toThrow()
    expect(() =>
      validateInterventionInput(form, {
        reason: "matched operator evidence",
        approved: true,
        evidence: ["receipt-7"],
      }),
    ).not.toThrow()
    expect(() => validateInterventionInput(form, { reason: "no", approved: true })).toThrowError(
      expect.objectContaining({ code: "INVALID_REQUEST" }),
    )
    expect(() =>
      validateInterventionSchema({
        type: "object",
        properties: { value: { $ref: "#/definitions/value" } },
        additionalProperties: false,
        maxProperties: 1,
      }),
    ).toThrowError(expect.objectContaining({ code: "INCOMPATIBLE_SCHEMA" }))
    expect(() =>
      validateInterventionSchema({
        type: "object",
        properties: { value: { type: "string" } },
        additionalProperties: false,
        maxProperties: 1,
      }),
    ).toThrowError(expect.objectContaining({ code: "INCOMPATIBLE_SCHEMA" }))
  })

  it("rejects unknown fields at the public list boundary", () => {
    const page = {
      schema_version: 1,
      list_revision: 1,
      filter: "active",
      query: "",
      total_count: 1,
      items: [
        {
          schema_version: 1,
          run_id: runId,
          definition_id: "reference.delivery",
          definition_version: "1.0.0",
          definition_digest: definitionDigest,
          operational_status: "running",
          current_state: "dispatch_vehicle",
          state_entered_at: "2026-10-24T00:00:00.000Z",
          subject_references: [],
          queue_bucket: "active",
          priority_rank: 6,
          owner: { mode: "runner", owner_id: "local-runner", epoch: 1, connectivity: "connected" },
          current_sequence: 1,
          updated_at: "2026-10-24T00:00:00.000Z",
        },
      ],
      unexpected: true,
    }

    expect(Schema.Check(MissionListPageSchema, page)).toBe(false)
  })
})
