import type { AdapterInvocationContext } from "@idemantis/adapter-kit"
import { describe, expect, it } from "vitest"
import {
  AccessSimulator,
  FaultSchedule,
  ResourceSimulator,
  type SimulationError,
  SimulationKernel,
  VehicleSimulator,
  createReferenceDeliveryMission,
  runReferenceScenario,
  simulationId,
  type ReferenceScenario,
  type TargetSnapshot,
} from "../src/index.js"

function invocation(
  kernel: SimulationKernel,
  effect: number,
  command = effect,
): AdapterInvocationContext {
  return {
    adapter_instance_id: simulationId("adp", 9),
    run_id: simulationId("run", 9),
    command_id: simulationId("cmd", command),
    attempt_id: simulationId("att", command),
    effect_id: simulationId("eff", effect),
    ownership_epoch: 1,
    deadline: new Date(kernel.now().getTime() + 60_000).toISOString(),
    source_id: "simulator-test",
    signal: new AbortController().signal,
  }
}

async function genericTrace(seed: number) {
  const kernel = new SimulationKernel({ seed })
  kernel.schedule(
    {
      target: "billing-ledger",
      operation: "payment.capture",
      stage: "capture.committed",
      correlation_id: "invoice-42",
      after_ms: 100,
      jitter_ms: 50,
    },
    () => {
      kernel.publishObservation({
        observation_id: "payment.capture-invoice-42",
        target: "billing-ledger",
        type: "payment.captured",
        subject_reference: "invoice-42",
        observed_at: kernel.now().toISOString(),
        payload: { amount_minor: 27_000, currency: "USD" },
      })
    },
  )
  kernel.schedule(
    {
      target: "records-system",
      operation: "record.approve",
      stage: "approval.committed",
      correlation_id: "case-9",
      after_ms: 80,
      jitter_ms: 50,
    },
    () => undefined,
  )
  await kernel.runUntilIdle()
  return kernel.snapshot()
}

function metric(target: TargetSnapshot, operation: string) {
  return target.metrics.find((value) => value.operation === operation)
}

describe("deterministic simulation kernel", () => {
  it("reproduces arbitrary software and service workflows from the same seed", async () => {
    const first = await genericTrace(42)
    const second = await genericTrace(42)
    const changed = await genericTrace(43)

    expect(first).toEqual(second)
    expect(first.trace_digest).toBe(second.trace_digest)
    expect(changed.trace_digest).not.toBe(first.trace_digest)
    expect(first.trace.map((entry) => entry.target)).toContain("billing-ledger")
    expect(first.trace.map((entry) => entry.target)).toContain("records-system")
  })

  it("validates fault rules and consumes only the configured occurrence", () => {
    expect(
      () =>
        new FaultSchedule([
          {
            id: "invalid-delay",
            target: "service",
            operation: "record.update",
            stage: "after_apply",
            occurrence: 1,
            action: "delay",
          },
        ]),
    ).toThrowError(expect.objectContaining({ code: "INVALID_FAULT" }))

    const schedule = new FaultSchedule([
      {
        id: "second-response",
        target: "service",
        operation: "record.update",
        stage: "after_apply",
        occurrence: 2,
        action: "drop_response",
      },
    ])
    const match = { target: "service", operation: "record.update", stage: "after_apply" }

    expect(schedule.take(match)).toBeUndefined()
    expect(schedule.take(match)).toMatchObject({ id: "second-response", action: "drop_response" })
    expect(schedule.take(match)).toBeUndefined()
  })

  it("bounds runaway scheduled work", async () => {
    const kernel = new SimulationKernel({ seed: 1, maximum_tasks: 1 })
    kernel.schedule(
      {
        target: "service",
        operation: "job.run",
        stage: "job.finished",
        correlation_id: "job-1",
        after_ms: 1,
      },
      () => undefined,
    )
    kernel.schedule(
      {
        target: "service",
        operation: "job.run",
        stage: "job.finished",
        correlation_id: "job-2",
        after_ms: 2,
      },
      () => undefined,
    )

    await expect(kernel.runUntilIdle()).rejects.toEqual(
      expect.objectContaining<Partial<SimulationError>>({ code: "QUEUE_LIMIT_EXCEEDED" }),
    )
  })

  it("shares one clock while exposing deterministic source skew", () => {
    const kernel = new SimulationKernel({
      seed: 1,
      clock_skew_ms: { edge: 750, service: -250 },
    })

    expect(kernel.now("edge").getTime() - kernel.now().getTime()).toBe(750)
    expect(kernel.now("service").getTime() - kernel.now().getTime()).toBe(-250)
    expect(
      () => new SimulationKernel({ seed: 1, clock_skew_ms: { edge: Number.NaN } }),
    ).toThrowError(expect.objectContaining({ code: "INVALID_TIME" }))
  })

  it("advances only until requested evidence is available", async () => {
    const kernel = new SimulationKernel({ seed: 1 })
    let ready = false
    let future = false
    kernel.schedule(
      {
        target: "service",
        operation: "job.run",
        stage: "job.ready",
        correlation_id: "job-1",
        after_ms: 10,
      },
      () => {
        ready = true
      },
    )
    kernel.schedule(
      {
        target: "service",
        operation: "job.run",
        stage: "job.expired",
        correlation_id: "job-1",
        after_ms: 1_000,
      },
      () => {
        future = true
      },
    )

    expect(await kernel.runUntil(() => ready)).toBe(true)
    expect(future).toBe(false)
    expect(kernel.pendingTasks()).toBe(1)
  })
})

describe("reference target recovery", () => {
  it("rejects idempotency identities reused with different semantics", async () => {
    const kernel = new SimulationKernel({ seed: 7 })
    const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })
    const resource = new ResourceSimulator(kernel, { resource_id: "bay-3", capacity: 2 })

    await vehicle.dispatch.issue(invocation(kernel, 1), {
      vehicle_id: "vehicle-7",
      route: "route-1",
      pickup: "pickup-a",
      destination: "bay-3",
    })
    const changedDispatch = await vehicle.dispatch.issue(invocation(kernel, 1), {
      vehicle_id: "vehicle-7",
      route: "route-2",
      pickup: "pickup-a",
      destination: "bay-3",
    })

    await resource.reserve.issue(invocation(kernel, 2), {
      resource_id: "bay-3",
      reservation_key: "reservation-1",
      owner: "mission-1",
      expires_at: "2026-01-01T01:00:00.000Z",
    })
    const changedReservation = await resource.reserve.issue(invocation(kernel, 2), {
      resource_id: "bay-3",
      reservation_key: "reservation-1",
      owner: "mission-2",
      expires_at: "2026-01-01T01:00:00.000Z",
    })
    await resource.reserve.issue(invocation(kernel, 3), {
      resource_id: "bay-3",
      reservation_key: "reservation-2",
      owner: "mission-2",
      expires_at: "2026-01-01T01:00:00.000Z",
    })
    await resource.release.issue(invocation(kernel, 4), {
      resource_id: "bay-3",
      reservation_id: "reservation-1",
    })
    const changedRelease = await resource.release.issue(invocation(kernel, 4), {
      resource_id: "bay-3",
      reservation_id: "reservation-2",
    })

    expect(changedDispatch).toMatchObject({
      result: "rejected",
      evidence: { reason: "idempotency_conflict" },
    })
    expect(changedReservation).toMatchObject({
      result: "rejected",
      evidence: { reason: "idempotency_conflict" },
    })
    expect(changedRelease).toMatchObject({
      result: "rejected",
      evidence: { reason: "idempotency_conflict" },
    })
    expect(Reflect.get(resource.snapshot().state, "reservations")).toContainEqual(
      expect.objectContaining({ reservation_id: "reservation-2", released: false }),
    )
  })

  it("attributes cancellation, compensation, and release to their own effects", async () => {
    const kernel = new SimulationKernel({ seed: 7 })
    const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })
    const resource = new ResourceSimulator(kernel, { resource_id: "bay-3" })
    const access = new AccessSimulator(kernel, { access_point_id: "bay-3" })

    await vehicle.dispatch.issue(invocation(kernel, 1), {
      vehicle_id: "vehicle-7",
      route: "route-1",
      pickup: "pickup-a",
      destination: "bay-3",
    })
    await vehicle.dispatch.cancel?.(invocation(kernel, 2), { vehicle_id: "vehicle-7" })

    await resource.reserve.issue(invocation(kernel, 3), {
      resource_id: "bay-3",
      reservation_key: "reservation-1",
      owner: "mission-1",
      expires_at: "2026-01-01T01:00:00.000Z",
    })
    await resource.release.issue(invocation(kernel, 4), {
      resource_id: "bay-3",
      reservation_id: "reservation-1",
    })
    const wrongRelease = await resource.release.reconcile?.(invocation(kernel, 5), {
      resource_id: "bay-3",
      reservation_id: "reservation-1",
    })

    await access.open.issue(invocation(kernel, 6), {
      access_point_id: "bay-3",
      subject: "payload-1",
    })
    await access.open.compensate?.(invocation(kernel, 7), {
      access_point_id: "bay-3",
      subject: "payload-1",
    })
    await kernel.runUntilIdle()

    expect(wrongRelease?.result).toBe("conflicting_state")
    expect(metric(vehicle.snapshot(), "vehicle.dispatch.cancel")?.applied_effect_ids).toEqual([
      simulationId("eff", 2),
    ])
    expect(metric(access.snapshot(), "access.open.compensate")?.applied_effect_ids).toEqual([
      simulationId("eff", 7),
    ])
    expect(Reflect.get(access.snapshot().state, "open")).toBe(false)
    expect(kernel.observations("vehicle.arrived")).toHaveLength(0)
    expect(kernel.observations("access.opened")).toHaveLength(0)
  })

  it("expires reservations on virtual time without consuming real time", async () => {
    const kernel = new SimulationKernel({ seed: 7 })
    const resource = new ResourceSimulator(kernel, { resource_id: "bay-3" })

    await resource.reserve.issue(invocation(kernel, 1), {
      resource_id: "bay-3",
      reservation_key: "reservation-1",
      owner: "mission-1",
      expires_at: "2026-01-01T00:00:01.000Z",
    })
    await kernel.runUntilIdle()

    const reservations = Reflect.get(resource.snapshot().state, "reservations")
    expect(reservations).toEqual([
      expect.objectContaining({ reservation_key: "reservation-1", expired: true }),
    ])
    expect(kernel.now().toISOString()).toBe("2026-01-01T00:00:01.000Z")
  })
})

describe("reference delivery definition", () => {
  it("compiles the cross-system mission independently of simulation seed", () => {
    const mission = (seed: number) => {
      const kernel = new SimulationKernel({ seed })
      const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })
      const resource = new ResourceSimulator(kernel, { resource_id: "bay-3" })
      const access = new AccessSimulator(kernel, { access_point_id: "bay-3" })
      return createReferenceDeliveryMission({
        reserve: resource.reserve,
        release: resource.release,
        dispatch: vehicle.dispatch,
        openAccess: access.open,
      })
    }
    const first = mission(1)
    const second = mission(999)

    expect(first.definitionDigest).toBe(second.definitionDigest)
    expect(first.manifest.graph.nodes.map((node) => node.id)).toEqual(
      expect.arrayContaining([
        "reserve_destination",
        "dispatch_vehicle",
        "open_destination",
        "release_destination",
        "await_operator",
      ]),
    )
    expect(Object.keys(first.manifest.operations)).toEqual([
      "access.open",
      "destination.release",
      "destination.reserve",
      "vehicle.dispatch",
    ])
  })
})

describe("reference scenarios", () => {
  for (const scenario of [
    "happy_path",
    "lost_reservation_response",
    "duplicate_arrival_observation",
  ] satisfies ReferenceScenario[]) {
    it(`completes ${scenario} without violating target invariants`, async () => {
      const report = await runReferenceScenario({ scenario, seed: 2026 })

      expect(report.outcome).toBe("completed")
      expect(report.terminal_state).toBe("complete")
      expect(report.invariants.every((invariant) => invariant.passed)).toBe(true)
      expect(report.simulation.trace_digest).toMatch(/^sha256:[a-f0-9]{64}$/)
      expect(report.target_state_digest).toMatch(/^sha256:[a-f0-9]{64}$/)
    })
  }

  it("reconciles an applied reservation after its response is lost", async () => {
    const report = await runReferenceScenario({
      scenario: "lost_reservation_response",
      seed: 88,
    })
    const resource = report.targets.find((target) => target.target === "destination-resource")
    const reserve = resource ? metric(resource, "resource.reserve") : undefined

    expect(reserve).toMatchObject({
      issue_requests: 1,
      reconciliation_requests: 1,
      applied_effects: 1,
    })
    expect(report.simulation.faults[0]).toMatchObject({ consumed: true, matches: 1 })
  })

  it("deduplicates a repeated arrival at the evidence boundary", async () => {
    const report = await runReferenceScenario({
      scenario: "duplicate_arrival_observation",
      seed: 88,
    })
    const duplicate = report.simulation.trace.filter(
      (entry) => entry.stage === "observation.duplicate" && entry.operation === "vehicle.arrived",
    )

    expect(duplicate).toHaveLength(1)
    expect(
      report.simulation.observations.filter((item) => item.type === "vehicle.arrived"),
    ).toHaveLength(1)
  })

  it("stops an unsafe ambiguous access effect for operator attention", async () => {
    const report = await runReferenceScenario({ scenario: "ambiguous_access", seed: 88 })
    const access = report.targets.find((target) => target.target === "access-service")
    const open = access ? metric(access, "access.open") : undefined

    expect(report.outcome).toBe("awaiting_operator")
    expect(report.terminal_state).toBe("await_operator")
    expect(open).toMatchObject({ issue_requests: 1, applied_effects: 1 })
    expect(report.simulation.observations).not.toContainEqual(
      expect.objectContaining({ type: "access.opened" }),
    )
  })

  it("follows the compiled mission states in order", async () => {
    const report = await runReferenceScenario({ scenario: "happy_path", seed: 88 })
    const states = report.simulation.trace
      .filter((entry) => entry.target === "mission" && entry.stage === "state.entered")
      .map((entry) => Reflect.get(entry.payload, "state"))

    expect(states).toEqual([
      "reserve_destination",
      "dispatch_vehicle",
      "await_pickup",
      "await_arrival",
      "open_destination",
      "await_open",
      "await_handoff",
      "release_destination",
      "complete",
    ])
  })

  it("compiles the reference mission for runner-hosted execution", () => {
    const kernel = new SimulationKernel({ seed: 88 })
    const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })
    const resource = new ResourceSimulator(kernel, { resource_id: "bay-3" })
    const access = new AccessSimulator(kernel, { access_point_id: "bay-3" })
    const mission = createReferenceDeliveryMission(
      {
        reserve: resource.reserve,
        release: resource.release,
        dispatch: vehicle.dispatch,
        openAccess: access.open,
      },
      { vehicleExecution: "runner" },
    )
    const dispatchVehicle = Reflect.get(mission.manifest.states, "dispatch_vehicle")
    const reconcileVehicle = Reflect.get(mission.manifest.states, "reconcile_vehicle")

    expect(dispatchVehicle).toMatchObject({
      kind: "effect",
      execution: "runner",
    })
    expect(reconcileVehicle).toMatchObject({
      kind: "reconcile",
      execution: "runner",
    })
  })

  it("preserves scenario invariants across one hundred seeds", async () => {
    const scenarios: ReferenceScenario[] = [
      "happy_path",
      "lost_reservation_response",
      "duplicate_arrival_observation",
      "ambiguous_access",
    ]

    for (let seed = 0; seed < 100; seed += 1) {
      const scenario = scenarios[seed % scenarios.length]
      if (!scenario) {
        throw new Error("Scenario selection failed")
      }
      const report = await runReferenceScenario({ scenario, seed })
      expect(report.invariants.every((invariant) => invariant.passed)).toBe(true)
      expect(report.outcome).toBe(
        scenario === "ambiguous_access" ? "awaiting_operator" : "completed",
      )
    }
  })

  it("replays identically for the same seed and changes ordering for a new seed", async () => {
    const first = await runReferenceScenario({ scenario: "happy_path", seed: 41 })
    const replay = await runReferenceScenario({ scenario: "happy_path", seed: 41 })
    const changed = await runReferenceScenario({ scenario: "happy_path", seed: 42 })

    expect(replay).toEqual(first)
    expect(changed.simulation.trace_digest).not.toBe(first.simulation.trace_digest)
    expect(changed.target_state_digest).toBe(first.target_state_digest)
  })
})
