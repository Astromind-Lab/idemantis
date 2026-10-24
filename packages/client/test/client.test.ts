import type { RunId } from "@idemantis/contracts"
import { MissionContextSchema, RecordObservationRequestSchema } from "@idemantis/transport"
import Schema from "typebox/schema"
import { afterEach, describe, expect, it } from "vitest"
import {
  clientRequestIdFor,
  IdemantisApiError,
  IdemantisClient,
  observationIdFor,
  randomObservationId,
} from "../src/index.js"
import { type RunnerFixture, runnerFixture } from "./fixture.js"

const fixtures: RunnerFixture[] = []

async function fixture(): Promise<RunnerFixture> {
  const created = await runnerFixture()
  fixtures.push(created)
  return created
}

afterEach(async () => {
  for (const created of fixtures.splice(0)) {
    await created.close()
  }
})

describe("idemantis client", () => {
  it("registers, launches, waits, and resumes a mission with an API key", async () => {
    const runner = await fixture()
    const client = new IdemantisClient({ url: runner.runner.origin, api_key: runner.token })
    const [review] = runner.runtimes
    if (!review) {
      throw new Error("Expected a review definition")
    }

    expect((await client.health()).status).toBe("ready")
    expect(await client.registerDefinition(review)).toBe(review.definition_digest)
    expect((await client.listDefinitions()).map((item) => item.definition_id)).toContain(
      "support.reply",
    )
    const admitted = await client.launch(review.definition_digest, { ticket: "T-1" })
    const runId = admitted.summary.run_id as RunId
    const waiting = await client.waitFor(runId, { timeout_ms: 5_000, interval_ms: 20 })
    const observation = {
      observation_id: observationIdFor("review:T-1"),
      observation_type: "review.approved",
      effect_id: waiting.next.effect_id ?? "",
      payload: { reviewer: "lead" },
    }
    const first = await client.observe(runId, observation)
    const retried = await client.observe(runId, observation)
    const finished = await client.waitFor(runId, {
      until: (context) => context.terminal_outcome !== null,
      timeout_ms: 5_000,
      interval_ms: 20,
    })

    expect(Schema.Check(MissionContextSchema, waiting)).toBe(true)
    expect(waiting.next.actor).toBe("observation")
    expect(waiting.effects[0]?.output).toEqual({ draft: "Draft for T-1" })
    expect(first.disposition).toBe("recorded")
    expect(retried).toMatchObject({ disposition: "duplicate", event_id: first.event_id })
    expect(finished.terminal_outcome).toBe("completed")
    expect(runner.calls).toHaveLength(1)
    expect((await client.listMissions({ bucket: "all" })).items[0]?.run_id).toBe(runId)
    expect((await client.timeline(runId)).items.length).toBeGreaterThan(3)
    expect((await client.effects(runId)).items).toHaveLength(1)
  })

  it("reports API, transport, and wait failures as typed errors", async () => {
    const runner = await fixture()
    const wrongKey = new IdemantisClient({
      url: runner.runner.origin,
      api_key: `idm_${"0".repeat(26)}_${"a".repeat(43)}`,
    })
    const unreachable = new IdemantisClient({
      url: "http://127.0.0.1:9",
      api_key: runner.token,
      timeout_ms: 1_000,
    })
    const client = new IdemantisClient({ url: runner.runner.origin, api_key: runner.token })
    const [review] = runner.runtimes
    if (!review) {
      throw new Error("Expected a review definition")
    }
    await client.registerDefinition(review)
    const admitted = await client.admit(review.definition_digest, { ticket: "T-2" })

    await expect(wrongKey.listDefinitions()).rejects.toMatchObject({
      status: 401,
      code: "SESSION_EXPIRED",
    })
    await expect(unreachable.health()).rejects.toMatchObject({ code: "RUNNER_UNREACHABLE" })
    await expect(client.admit(review.definition_digest, { wrong: true })).rejects.toBeInstanceOf(
      IdemantisApiError,
    )
    await expect(
      client.waitFor(admitted.summary.run_id as RunId, { timeout_ms: 50, interval_ms: 20 }),
    ).rejects.toMatchObject({ code: "WAIT_TIMEOUT" })
    expect(() => new IdemantisClient({ url: "ftp://runner", api_key: runner.token })).toThrow(
      TypeError,
    )
    expect(() => new IdemantisClient({ url: runner.runner.origin, api_key: "secret" })).toThrow(
      TypeError,
    )
  })

  it("derives stable identifiers that satisfy the API contracts", () => {
    const observation = {
      observation_type: "review.approved",
      payload: {},
    }

    expect(observationIdFor("a")).toBe(observationIdFor("a"))
    expect(observationIdFor("a")).not.toBe(observationIdFor("b"))
    expect(randomObservationId()).not.toBe(randomObservationId())
    expect(
      Schema.Check(RecordObservationRequestSchema, {
        ...observation,
        observation_id: observationIdFor("a"),
      }),
    ).toBe(true)
    expect(
      Schema.Check(RecordObservationRequestSchema, {
        ...observation,
        observation_id: randomObservationId(),
      }),
    ).toBe(true)
    expect(clientRequestIdFor("x")).toMatch(/^req-[0-9a-z]{26}$/)
    expect(() => observationIdFor("")).toThrow(TypeError)
  })
})
