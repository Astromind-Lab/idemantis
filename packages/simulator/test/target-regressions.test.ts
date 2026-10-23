import type { AdapterInvocationContext, FenceActivationContext } from "@idemantis/adapter-kit"
import { describe, expect, it } from "vitest"
import {
  ResourceSimulator,
  SimulationKernel,
  VehicleSimulator,
  simulationId,
} from "../src/index.js"

function invocation(
  kernel: SimulationKernel,
  effect: number,
  ownershipEpoch = 1,
): AdapterInvocationContext {
  return {
    adapter_instance_id: simulationId("adp", 1),
    run_id: simulationId("run", 1),
    command_id: simulationId("cmd", effect),
    attempt_id: simulationId("att", effect),
    effect_id: simulationId("eff", effect),
    ownership_epoch: ownershipEpoch,
    deadline: new Date(kernel.now().getTime() + 60_000).toISOString(),
    source_id: "target-regression",
    signal: new AbortController().signal,
  }
}

function fence(
  kernel: SimulationKernel,
  operationKey: string,
  scopeReference: string,
  ownershipEpoch: number,
): FenceActivationContext {
  return {
    adapter_instance_id: simulationId("adp", 1),
    operation_key: operationKey,
    scope_reference: scopeReference,
    ownership_epoch: ownershipEpoch,
    requested_at: kernel.now().toISOString(),
    signal: new AbortController().signal,
  }
}

const dispatchInput = {
  vehicle_id: "vehicle-7",
  route: "route-1",
  pickup: "pickup-a",
  destination: "bay-3",
}

describe("reference target regressions", () => {
  it("rejects stale cancellation and compensation after fence activation", async () => {
    const kernel = new SimulationKernel({ seed: 4 })
    const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })
    const resource = new ResourceSimulator(kernel, { resource_id: "bay-3" })

    await vehicle.dispatch.issue(invocation(kernel, 1), dispatchInput)
    await vehicle.dispatch.activateFence?.(fence(kernel, "vehicle.dispatch", "vehicle-7", 2))
    const vehicleCancel = await vehicle.dispatch.cancel?.(invocation(kernel, 2), {
      vehicle_id: "vehicle-7",
    })

    await resource.reserve.issue(invocation(kernel, 3), {
      resource_id: "bay-3",
      reservation_key: "reservation-1",
      owner: "mission-1",
      expires_at: "2026-01-01T01:00:00.000Z",
    })
    await resource.reserve.activateFence?.(fence(kernel, "destination.reserve", "bay-3", 2))
    const resourceCancel = await resource.reserve.cancel?.(invocation(kernel, 4), {
      resource_id: "bay-3",
      reservation_key: "reservation-1",
    })
    const resourceCompensation = await resource.reserve.compensate?.(invocation(kernel, 5), {
      resource_id: "bay-3",
      reservation_key: "reservation-1",
    })

    expect(vehicleCancel).toMatchObject({
      result: "rejected_still_active",
      evidence: { reason: "stale_epoch" },
    })
    expect(resourceCancel).toMatchObject({
      result: "rejected_still_active",
      evidence: { reason: "stale_epoch" },
    })
    expect(resourceCompensation).toMatchObject({
      result: "rejected",
      evidence: { reason: "stale_epoch" },
    })
    expect(Reflect.get(vehicle.snapshot().state, "status")).toBe("moving")
    expect(Reflect.get(resource.snapshot().state, "reservations")).toContainEqual(
      expect.objectContaining({ reservation_id: "reservation-1", released: false }),
    )
  })

  it("keeps cancelled assignment callbacks from mutating a replacement assignment", async () => {
    const kernel = new SimulationKernel({ seed: 4 })
    const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })

    await vehicle.dispatch.issue(invocation(kernel, 1), dispatchInput)
    await vehicle.dispatch.cancel?.(invocation(kernel, 2), { vehicle_id: "vehicle-7" })
    await vehicle.dispatch.issue(invocation(kernel, 3), { ...dispatchInput, route: "route-2" })
    await kernel.runUntilIdle()
    const oldRetry = await vehicle.dispatch.issue(invocation(kernel, 1), dispatchInput)

    const arrivals = kernel.observations("vehicle.arrived")
    expect(arrivals).toHaveLength(1)
    expect(arrivals[0]?.effect_id).toBe(simulationId("eff", 3))
    expect(Reflect.get(vehicle.snapshot().state, "assignment")).toMatchObject({
      effect_id: simulationId("eff", 3),
      status: "arrived",
    })
    expect(oldRetry).toMatchObject({
      result: "rejected",
      evidence: { reason: "assignment_inactive" },
    })
  })

  it("separates arrival from delivery of its observation", async () => {
    const kernel = new SimulationKernel({
      seed: 4,
      fault_schedule: [
        {
          id: "lost-arrival",
          target: "vehicle",
          operation: "vehicle.dispatch",
          stage: "arrival_observation",
          occurrence: 1,
          action: "drop_observation",
        },
      ],
    })
    const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })

    await vehicle.dispatch.issue(invocation(kernel, 1), dispatchInput)
    await kernel.runUntilIdle()
    const reconciliation = await vehicle.dispatch.reconcile?.(invocation(kernel, 1), {
      vehicle_id: "vehicle-7",
    })

    expect(kernel.observations("vehicle.arrived")).toHaveLength(0)
    expect(reconciliation?.result).toBe("confirmed_completed")
    expect(Reflect.get(vehicle.snapshot().state, "status")).toBe("arrived")
  })

  it("does not report a stopped assignment as active", async () => {
    const kernel = new SimulationKernel({ seed: 4 })
    const vehicle = new VehicleSimulator(kernel, { vehicle_id: "vehicle-7" })

    await vehicle.dispatch.issue(invocation(kernel, 1), dispatchInput)
    await vehicle.dispatch.cancel?.(invocation(kernel, 2), { vehicle_id: "vehicle-7" })
    const reconciliation = await vehicle.dispatch.reconcile?.(invocation(kernel, 1), {
      vehicle_id: "vehicle-7",
    })

    expect(reconciliation?.result).toBe("conflicting_state")
  })
})
