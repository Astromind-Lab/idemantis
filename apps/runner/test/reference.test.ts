import type { JsonObject, RunId } from "@idemantis/contracts"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import { afterEach, describe, expect, it } from "vitest"
import { runCli } from "../src/cli.js"
import { ReferenceMissionRunner } from "../src/reference.js"
import { createRunnerServer } from "../src/server.js"
import { SessionAuthority } from "../src/session.js"
import { RunnerStore } from "../src/store.js"

const journals: SqliteJournal[] = []
const servers: Awaited<ReturnType<typeof createRunnerServer>>[] = []

function setup() {
  const journal = new SqliteJournal({ path: ":memory:" })
  journals.push(journal)
  const store = new RunnerStore({
    journal,
    cursor_secret: new Uint8Array(32).fill(7),
  })
  const runner = new ReferenceMissionRunner({ journal, store, seed: 17 })
  return { journal, store, runner }
}

afterEach(async () => {
  for (const server of servers.splice(0)) {
    await server.close()
  }
  for (const journal of journals.splice(0)) {
    journal.close()
  }
})

describe("reference mission runner", () => {
  it("prints a standalone reference execution proof", async () => {
    let output = ""

    const code = await runCli(["reference"], (value) => {
      output += value
    })
    const proof = JSON.parse(output)

    expect(code).toBe(0)
    expect(proof.outcome).toMatchObject({ status: "completed", terminal_outcome: "completed" })
    expect(proof.graph).toMatchObject({ nodes: 16 })
    expect(proof.effects).toHaveLength(4)
    expect(proof.replay.state_digest).toBe(proof.outcome.state_digest)
  })

  it("admits and executes the reference mission through the operator API", async () => {
    const { journal, store, runner } = setup()
    const sessions = new SessionAuthority()
    const origin = "http://127.0.0.1:4317"
    const host = "127.0.0.1:4317"
    const app = await createRunnerServer({
      store,
      sessions,
      exact_origin: origin,
      mission_executor: runner,
    })
    servers.push(app)
    const bootstrap = sessions.issueBootstrap()
    const authenticated = await app.inject({
      method: "POST",
      url: "/api/session/bootstrap",
      headers: { host, origin, "x-idemantis-bootstrap": bootstrap },
    })
    const setCookie = authenticated.headers["set-cookie"]
    const cookie = (Array.isArray(setCookie) ? setCookie[0] : setCookie)?.split(";")[0]
    const csrf = authenticated.json().csrf_token
    if (!cookie || typeof csrf !== "string") {
      throw new Error("Expected an authenticated operator session")
    }
    const headers = { host, origin, cookie, "x-idemantis-csrf": csrf }
    const admitted = await app.inject({
      method: "POST",
      url: "/api/missions",
      headers,
      payload: {
        definition_digest: runner.runtime.definition_digest,
        input: runner.defaultInput(),
      },
    })
    const runId = admitted.json().summary.run_id

    const executed = await app.inject({
      method: "POST",
      url: `/api/missions/${runId}/run`,
      headers,
    })
    const effects = await app.inject({
      method: "GET",
      url: `/api/missions/${runId}/effects`,
      headers: { host, cookie },
    })

    expect(admitted.statusCode).toBe(201)
    expect(executed.statusCode).toBe(200)
    expect(executed.json().projection).toMatchObject({
      status: "completed",
      current_state: "complete",
      terminal_outcome: "completed",
    })
    expect(executed.json().graph.nodes).toHaveLength(16)
    expect(effects.statusCode).toBe(200)
    expect(effects.json().items).toHaveLength(4)
    expect(journal.verifyRun(runId).state_digest).toBe(executed.json().projection.state_digest)
  })

  it("executes the reference delivery through adapters and verifies its journal", async () => {
    const { journal, store, runner } = setup()
    const admitted = store.admitMission({
      definition_digest: runner.runtime.definition_digest,
      input: runner.defaultInput() as JsonObject,
    })

    await runner.run(admitted.summary.run_id)

    const report = runner.report(admitted.summary.run_id)
    const verification = journal.verifyRun(admitted.summary.run_id)
    const eventTypes = report.timeline.items.map((entry) => entry.event_type)
    const operations = report.effects.items.map((effect) => effect.operation).sort()
    const metrics = report.targets.flatMap((target) => target.metrics)

    expect(report.detail.projection).toMatchObject({
      status: "completed",
      current_state: "complete",
      terminal_outcome: "completed",
    })
    expect(report.detail.graph.nodes).toHaveLength(16)
    expect(report.detail.graph.transitions.length).toBeGreaterThan(15)
    expect(operations).toEqual([
      "access.open",
      "destination.release",
      "destination.reserve",
      "vehicle.dispatch",
    ])
    expect(report.effects.items.every((effect) => effect.issue_count === 1)).toBe(true)
    expect(eventTypes[0]).toBe("mission.admitted")
    expect(eventTypes.filter((type) => type === "effect.dispatch_started")).toHaveLength(4)
    expect(eventTypes.filter((type) => type === "effect.issue_result_recorded")).toHaveLength(4)
    expect(eventTypes.filter((type) => type === "observation.recorded")).toHaveLength(4)
    expect(report.simulation.observations.map((observation) => observation.type).sort()).toEqual([
      "access.opened",
      "vehicle.arrived",
      "vehicle.pickup_confirmed",
    ])
    expect(metrics.reduce((total, metric) => total + metric.applied_effects, 0)).toBe(4)
    expect(verification.event_count).toBe(report.detail.projection.current_sequence)
    expect(verification.state_digest).toBe(report.detail.projection.state_digest)
  })

  it("does not execute a completed mission twice", async () => {
    const { journal, store, runner } = setup()
    const admitted = store.admitMission({
      definition_digest: runner.runtime.definition_digest,
      input: runner.defaultInput() as JsonObject,
    })

    await runner.run(admitted.summary.run_id)
    const runId = admitted.summary.run_id as RunId
    const sequence = journal.getMission(runId).state.current_sequence

    await runner.run(runId)

    expect(journal.getMission(runId).state.current_sequence).toBe(sequence)
  })
})
