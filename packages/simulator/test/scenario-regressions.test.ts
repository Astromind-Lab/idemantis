import { describe, expect, it } from "vitest"
import { runReferenceScenario, SimulationKernel, type TargetSnapshot } from "../src/index.js"

function metric(target: TargetSnapshot | undefined, operation: string) {
  return target?.metrics.find((value) => value.operation === operation)
}

describe("reference scenario regressions", () => {
  it("stops at a declared virtual deadline without draining later work", async () => {
    const kernel = new SimulationKernel({ seed: 12 })
    let early = false
    let late = false
    kernel.schedule(
      {
        target: "service",
        operation: "job.run",
        stage: "job.early",
        correlation_id: "job-1",
        after_ms: 100,
      },
      () => {
        early = true
      },
    )
    kernel.schedule(
      {
        target: "service",
        operation: "job.run",
        stage: "job.late",
        correlation_id: "job-1",
        after_ms: 500,
      },
      () => {
        late = true
      },
    )

    const observed = await kernel.runUntil(() => late, new Date(kernel.now().getTime() + 200))

    expect(observed).toBe(false)
    expect(early).toBe(true)
    expect(late).toBe(false)
    expect(kernel.pendingTasks()).toBe(1)
    expect(kernel.now().toISOString()).toBe("2026-01-01T00:00:00.200Z")
  })

  it("waits for matching access evidence instead of the next queued task", async () => {
    const report = await runReferenceScenario({
      scenario: "happy_path",
      seed: 12,
      fault_schedule: [
        {
          id: "delayed-access-observation",
          target: "access-service",
          operation: "access.open",
          stage: "opened_observation",
          occurrence: 1,
          action: "delay",
          delay_ms: 100,
        },
      ],
    })

    expect(report.outcome).toBe("completed")
    expect(report.terminal_state).toBe("complete")
    expect(report.simulation.faults[0]).toMatchObject({ consumed: true, matches: 1 })
  })

  it("reconciles a dispatch whose response was lost", async () => {
    const report = await runReferenceScenario({
      scenario: "happy_path",
      seed: 12,
      fault_schedule: [
        {
          id: "lost-dispatch-response",
          target: "vehicle",
          operation: "vehicle.dispatch",
          stage: "after_apply",
          occurrence: 1,
          action: "drop_response",
        },
      ],
    })
    const vehicle = report.targets.find((target) => target.target === "vehicle")
    const states = report.simulation.trace
      .filter((entry) => entry.target === "mission" && entry.stage === "state.entered")
      .map((entry) => Reflect.get(entry.payload, "state"))

    expect(report.outcome).toBe("completed")
    expect(states).toContain("reconcile_vehicle")
    expect(states).toContain("await_arrival")
    expect(metric(vehicle, "vehicle.dispatch")).toMatchObject({
      issue_requests: 1,
      reconciliation_requests: 1,
      applied_effects: 1,
    })
  })

  it("recovers target completion after an arrival observation is lost", async () => {
    const report = await runReferenceScenario({
      scenario: "happy_path",
      seed: 12,
      fault_schedule: [
        {
          id: "lost-arrival-observation",
          target: "vehicle",
          operation: "vehicle.dispatch",
          stage: "arrival_observation",
          occurrence: 1,
          action: "drop_observation",
        },
      ],
    })
    const states = report.simulation.trace
      .filter((entry) => entry.target === "mission" && entry.stage === "state.entered")
      .map((entry) => Reflect.get(entry.payload, "state"))

    expect(report.outcome).toBe("completed")
    expect(report.simulation.observations).not.toContainEqual(
      expect.objectContaining({ type: "vehicle.arrived" }),
    )
    expect(states).toContain("reconcile_vehicle")
  })

  it("returns failure evidence for an ordinarily rejected effect", async () => {
    const report = await runReferenceScenario({
      scenario: "happy_path",
      seed: 12,
      input: { reservation_expires_at: "2026-01-01T00:00:00.000Z" },
    })

    expect(report.outcome).toBe("failed")
    expect(report.terminal_state).toBe("failed")
    expect(report.simulation.trace.length).toBeGreaterThan(0)
    expect(report.targets).toHaveLength(3)
    expect(
      report.targets.find((target) => target.target === "destination-resource")?.metrics,
    ).toEqual(expect.arrayContaining([expect.objectContaining({ operation: "resource.reserve" })]))
  })
})
