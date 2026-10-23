import type { AdapterInvocationContext, IssueResultEnvelope } from "@idemantis/adapter-kit"
import { sha256Digest, type Digest, type JsonValue } from "@idemantis/contracts"
import type { CompiledMission } from "@idemantis/sdk"
import { AccessSimulator } from "./access.js"
import { SimulationError } from "./errors.js"
import { simulationId } from "./ids.js"
import { SimulationKernel } from "./kernel.js"
import { createReferenceDeliveryMission, type ReferenceDeliveryInput } from "./reference-mission.js"
import { ResourceSimulator } from "./resource.js"
import type { FaultRule, SimulationSnapshot, TargetSnapshot } from "./types.js"
import { VehicleSimulator } from "./vehicle.js"

export type ReferenceScenario =
  | "happy_path"
  | "lost_reservation_response"
  | "duplicate_arrival_observation"
  | "ambiguous_access"

export type ReferenceScenarioOutcome = "completed" | "awaiting_operator" | "failed"

export interface ScenarioInvariant {
  name: string
  passed: boolean
}

export interface ReferenceScenarioOptions {
  scenario: ReferenceScenario
  seed: number
  input?: Partial<ReferenceDeliveryInput>
  fault_schedule?: readonly FaultRule[]
}

export interface ReferenceScenarioReport {
  scenario: ReferenceScenario
  seed: number
  input: ReferenceDeliveryInput
  fault_schedule: FaultRule[]
  mission_definition_digest: Digest
  mission_graph_nodes: number
  outcome: ReferenceScenarioOutcome
  terminal_state: string
  simulation: SimulationSnapshot
  targets: TargetSnapshot[]
  target_state_digest: Digest
  invariants: ScenarioInvariant[]
}

const defaultInput: ReferenceDeliveryInput = {
  mission_id: "delivery-001",
  vehicle_id: "vehicle-7",
  route: "pickup-a-to-bay-3",
  pickup: "pickup-a",
  destination_id: "bay-3",
  reservation_key: "delivery-001-bay-3",
  reservation_expires_at: "2026-01-01T01:00:00.000Z",
  access_subject: "payload-42",
}

function faultsFor(scenario: ReferenceScenario): FaultRule[] {
  switch (scenario) {
    case "happy_path":
      return []
    case "lost_reservation_response":
      return [
        {
          id: "lost-reservation-response",
          target: "destination-resource",
          operation: "resource.reserve",
          stage: "after_apply",
          occurrence: 1,
          action: "drop_response",
        },
      ]
    case "duplicate_arrival_observation":
      return [
        {
          id: "duplicate-arrival",
          target: "vehicle",
          operation: "vehicle.dispatch",
          stage: "arrival_observation",
          occurrence: 1,
          action: "duplicate_observation",
        },
      ]
    case "ambiguous_access":
      return [
        {
          id: "lost-access-response",
          target: "access-service",
          operation: "access.open",
          stage: "after_apply",
          occurrence: 1,
          action: "drop_response",
        },
      ]
  }
}

function context(
  kernel: SimulationKernel,
  adapter: number,
  command: number,
  attempt: number,
  effect: number,
): AdapterInvocationContext {
  return {
    adapter_instance_id: simulationId("adp", adapter),
    run_id: simulationId("run", 1),
    command_id: simulationId("cmd", command),
    attempt_id: simulationId("att", attempt),
    effect_id: simulationId("eff", effect),
    ownership_epoch: 1,
    deadline: new Date(kernel.now().getTime() + 60_000).toISOString(),
    source_id: "reference-runner",
    signal: new AbortController().signal,
  }
}

class MissionProgress {
  readonly #kernel: SimulationKernel
  readonly #mission: CompiledMission
  readonly #correlation: string
  #state: string | undefined

  constructor(kernel: SimulationKernel, mission: CompiledMission, correlation: string) {
    this.#kernel = kernel
    this.#mission = mission
    this.#correlation = correlation
  }

  enter(state: string, trigger?: string): void {
    if (this.#state === undefined) {
      if (state !== this.#mission.manifest.initial || trigger !== undefined) {
        throw new SimulationError(
          "SCENARIO_INVARIANT_FAILED",
          `Scenario must begin at ${this.#mission.manifest.initial}`,
        )
      }
    } else {
      const edge = this.#mission.manifest.graph.edges.find(
        (candidate) =>
          candidate.from === this.#state && candidate.to === state && candidate.trigger === trigger,
      )
      if (!edge) {
        throw new SimulationError(
          "SCENARIO_INVARIANT_FAILED",
          `Transition ${this.#state} -> ${state} via ${trigger ?? "<none>"} is not in the mission`,
        )
      }
    }
    const previous = this.#state
    this.#state = state
    this.#kernel.record({
      target: "mission",
      operation: this.#mission.manifest.definition_id,
      stage: "state.entered",
      correlation_id: this.#correlation,
      payload: {
        state,
        ...(previous ? { previous_state: previous } : {}),
        ...(trigger ? { trigger } : {}),
      },
    })
  }
}

function completedOutput<Output>(envelope: IssueResultEnvelope<Output>): Output | undefined {
  return envelope.result === "completed" ? envelope.output : undefined
}

async function waitForObservation(
  kernel: SimulationKernel,
  mission: CompiledMission,
  state: string,
  type: string,
): Promise<boolean> {
  const definition = mission.manifest.states[state]
  if (definition?.kind !== "observation_wait") {
    throw new SimulationError(
      "SCENARIO_INVARIANT_FAILED",
      `State ${state} is not an observation wait`,
    )
  }
  const deadline = new Date(kernel.now().getTime() + definition.timeout.after_ms)
  return kernel.runUntil(() => kernel.observations(type).length > 0, deadline)
}

function invariants(
  scenario: ReferenceScenario,
  outcome: ReferenceScenarioOutcome,
  simulation: SimulationSnapshot,
  targets: TargetSnapshot[],
): ScenarioInvariant[] {
  const metrics = targets.flatMap((target) => target.metrics)
  const appliedEffectIds = metrics.flatMap((metric) => metric.applied_effect_ids)
  const appliedOnce =
    metrics.every((metric) => metric.applied_effects === metric.applied_effect_ids.length) &&
    new Set(appliedEffectIds).size === appliedEffectIds.length
  const uniqueObservations =
    new Set(simulation.observations.map((observation) => observation.observation_id)).size ===
    simulation.observations.length
  const access = metrics.find((metric) => metric.operation === "access.open")
  const reserve = metrics.find((metric) => metric.operation === "resource.reserve")
  const checks: ScenarioInvariant[] = [
    { name: "target_applied_effects_are_unique", passed: appliedOnce },
    { name: "accepted_observations_are_unique", passed: uniqueObservations },
    {
      name: "unsafe_uncertain_effect_is_not_reissued",
      passed: (access?.issue_requests ?? 0) <= 1,
    },
    {
      name: "lost_reservation_is_reconciled_without_reissue",
      passed:
        scenario !== "lost_reservation_response" ||
        (reserve?.issue_requests === 1 && reserve.reconciliation_requests === 1),
    },
    {
      name: "ambiguous_access_requires_operator",
      passed: scenario !== "ambiguous_access" || outcome === "awaiting_operator",
    },
  ]
  if (checks.some((check) => !check.passed)) {
    throw new SimulationError(
      "SCENARIO_INVARIANT_FAILED",
      `Scenario ${scenario} violated a simulation invariant`,
    )
  }
  return checks
}

function report(
  options: ReferenceScenarioOptions,
  input: ReferenceDeliveryInput,
  faultSchedule: FaultRule[],
  mission: CompiledMission,
  outcome: ReferenceScenarioOutcome,
  terminalState: string,
  kernel: SimulationKernel,
  targets: TargetSnapshot[],
): ReferenceScenarioReport {
  const simulation = kernel.snapshot()
  const targetState = targets.map((target) => ({
    target: target.target,
    state_digest: target.state_digest,
    metrics: target.metrics,
  }))
  return {
    scenario: options.scenario,
    seed: options.seed,
    input,
    fault_schedule: structuredClone(faultSchedule),
    mission_definition_digest: mission.definitionDigest,
    mission_graph_nodes: mission.manifest.graph.nodes.length,
    outcome,
    terminal_state: terminalState,
    simulation,
    targets,
    target_state_digest: sha256Digest(targetState as unknown as JsonValue),
    invariants: invariants(options.scenario, outcome, simulation, targets),
  }
}

export async function runReferenceScenario(
  options: ReferenceScenarioOptions,
): Promise<ReferenceScenarioReport> {
  const input = { ...defaultInput, ...structuredClone(options.input ?? {}) }
  const faultSchedule = [...structuredClone(options.fault_schedule ?? faultsFor(options.scenario))]
  const kernel = new SimulationKernel({ seed: options.seed, fault_schedule: faultSchedule })
  const vehicle = new VehicleSimulator(kernel, { vehicle_id: input.vehicle_id })
  const resource = new ResourceSimulator(kernel, { resource_id: input.destination_id })
  const access = new AccessSimulator(kernel, { access_point_id: input.destination_id })
  const mission = createReferenceDeliveryMission({
    reserve: resource.reserve,
    release: resource.release,
    dispatch: vehicle.dispatch,
    openAccess: access.open,
  })
  const progress = new MissionProgress(kernel, mission, input.mission_id)
  const snapshots = () => [vehicle.snapshot(), resource.snapshot(), access.snapshot()]

  progress.enter("reserve_destination")
  const reserveContext = context(kernel, 1, 1, 1, 1)
  const reserveEnvelope = await resource.reserve.issue(reserveContext, {
    resource_id: input.destination_id,
    reservation_key: input.reservation_key,
    owner: input.mission_id,
    expires_at: input.reservation_expires_at,
  })
  let reservationId = completedOutput(reserveEnvelope)?.reservation_id
  if (reserveEnvelope.result === "uncertain") {
    progress.enter("reconcile_reservation", "effect.uncertain")
    const reconciliation = await resource.reserve.reconcile?.(context(kernel, 1, 2, 2, 1), {
      resource_id: input.destination_id,
      reservation_key: input.reservation_key,
    })
    if (reconciliation?.result === "confirmed_completed") {
      reservationId = reconciliation.output.reservation_id
      progress.enter("dispatch_vehicle", "reconcile.confirmed_completed")
    } else if (reconciliation?.result === "conflicting_state") {
      progress.enter("unresolved", "reconcile.conflicting_state")
      return report(
        options,
        input,
        faultSchedule,
        mission,
        "failed",
        "unresolved",
        kernel,
        snapshots(),
      )
    } else {
      progress.enter("unresolved", "reconcile.unknown")
      return report(
        options,
        input,
        faultSchedule,
        mission,
        "failed",
        "unresolved",
        kernel,
        snapshots(),
      )
    }
  }
  if (!reservationId) {
    progress.enter("failed", "effect.failed")
    return report(options, input, faultSchedule, mission, "failed", "failed", kernel, snapshots())
  }

  if (reserveEnvelope.result !== "uncertain") {
    progress.enter("dispatch_vehicle", "effect.settled")
  }
  type VehicleMissionState =
    | "dispatch_vehicle"
    | "reconcile_vehicle"
    | "await_pickup"
    | "await_arrival"
    | "open_destination"
  let vehicleState: VehicleMissionState = "dispatch_vehicle"
  let dispatchAttempt = 3
  let vehicleTransitions = 0
  while (vehicleState !== "open_destination") {
    vehicleTransitions += 1
    if (vehicleTransitions > 12) {
      throw new SimulationError(
        "SCENARIO_INVARIANT_FAILED",
        "Vehicle recovery exceeded its bounded transition count",
      )
    }
    if (vehicleState === "dispatch_vehicle") {
      const dispatchEnvelope = await vehicle.dispatch.issue(
        context(kernel, 2, 3, dispatchAttempt, 2),
        {
          vehicle_id: input.vehicle_id,
          route: input.route,
          pickup: input.pickup,
          destination: input.destination_id,
        },
      )
      dispatchAttempt += 1
      if (dispatchEnvelope.result === "accepted" || dispatchEnvelope.result === "completed") {
        progress.enter(
          "await_pickup",
          dispatchEnvelope.result === "accepted" ? "effect.acknowledged" : "effect.settled",
        )
        vehicleState = "await_pickup"
      } else if (dispatchEnvelope.result === "uncertain") {
        progress.enter("reconcile_vehicle", "effect.uncertain")
        vehicleState = "reconcile_vehicle"
      } else {
        progress.enter("failed", "effect.failed")
        return report(
          options,
          input,
          faultSchedule,
          mission,
          "failed",
          "failed",
          kernel,
          snapshots(),
        )
      }
      continue
    }
    if (vehicleState === "reconcile_vehicle") {
      const reconciliation = await vehicle.dispatch.reconcile?.(
        context(kernel, 2, 4, 20 + vehicleTransitions, 2),
        { vehicle_id: input.vehicle_id },
      )
      if (reconciliation?.result === "confirmed_completed") {
        progress.enter("open_destination", "reconcile.confirmed_completed")
        vehicleState = "open_destination"
      } else if (reconciliation?.result === "confirmed_not_applied") {
        progress.enter("dispatch_vehicle", "reconcile.confirmed_not_applied")
        vehicleState = "dispatch_vehicle"
      } else if (reconciliation?.result === "confirmed_in_progress") {
        progress.enter("await_arrival", "reconcile.confirmed_in_progress")
        vehicleState = "await_arrival"
      } else {
        const trigger =
          reconciliation?.result === "conflicting_state"
            ? "reconcile.conflicting_state"
            : "reconcile.unknown"
        progress.enter("unresolved", trigger)
        return report(
          options,
          input,
          faultSchedule,
          mission,
          "failed",
          "unresolved",
          kernel,
          snapshots(),
        )
      }
      continue
    }
    if (vehicleState === "await_pickup") {
      const observed = await waitForObservation(
        kernel,
        mission,
        "await_pickup",
        "vehicle.pickup_confirmed",
      )
      if (observed) {
        progress.enter("await_arrival", "observation.received")
        vehicleState = "await_arrival"
      } else {
        progress.enter("reconcile_vehicle", "observation.timeout")
        vehicleState = "reconcile_vehicle"
      }
      continue
    }
    const observed = await waitForObservation(kernel, mission, "await_arrival", "vehicle.arrived")
    if (observed) {
      progress.enter("open_destination", "observation.received")
      vehicleState = "open_destination"
    } else {
      progress.enter("reconcile_vehicle", "observation.timeout")
      vehicleState = "reconcile_vehicle"
    }
  }

  const accessEnvelope = await access.open.issue(context(kernel, 3, 4, 4, 3), {
    access_point_id: input.destination_id,
    subject: input.access_subject,
  })
  if (accessEnvelope.result === "uncertain") {
    progress.enter("await_operator", "effect.uncertain")
    return report(
      options,
      input,
      faultSchedule,
      mission,
      "awaiting_operator",
      "await_operator",
      kernel,
      snapshots(),
    )
  }
  if (accessEnvelope.result !== "completed") {
    progress.enter("failed", "effect.failed")
    return report(options, input, faultSchedule, mission, "failed", "failed", kernel, snapshots())
  }
  progress.enter("await_open", "effect.settled")
  kernel.schedule(
    {
      target: "handoff-service",
      operation: "delivery.confirm_handoff",
      stage: "handoff_observation",
      correlation_id: input.mission_id,
      after_ms: 120,
      jitter_ms: 30,
    },
    () => {
      kernel.publishObservation({
        observation_id: `delivery.handoff-${input.mission_id}`,
        target: "handoff-service",
        type: "delivery.handoff_confirmed",
        subject_reference: input.mission_id,
        observed_at: kernel.now("handoff-service").toISOString(),
        payload: { subject: input.access_subject },
      })
    },
  )
  const accessObserved = await waitForObservation(kernel, mission, "await_open", "access.opened")
  if (!accessObserved) {
    progress.enter("await_operator", "observation.timeout")
    return report(
      options,
      input,
      faultSchedule,
      mission,
      "awaiting_operator",
      "await_operator",
      kernel,
      snapshots(),
    )
  }
  progress.enter("await_handoff", "observation.received")
  const handoffObserved = await waitForObservation(
    kernel,
    mission,
    "await_handoff",
    "delivery.handoff_confirmed",
  )
  if (!handoffObserved) {
    progress.enter("await_operator", "observation.timeout")
    return report(
      options,
      input,
      faultSchedule,
      mission,
      "awaiting_operator",
      "await_operator",
      kernel,
      snapshots(),
    )
  }

  progress.enter("release_destination", "observation.received")
  const releaseEnvelope = await resource.release.issue(context(kernel, 1, 5, 5, 4), {
    resource_id: input.destination_id,
    reservation_id: reservationId,
  })
  if (releaseEnvelope.result !== "completed") {
    progress.enter(
      releaseEnvelope.result === "uncertain" ? "reconcile_release" : "failed",
      releaseEnvelope.result === "uncertain" ? "effect.uncertain" : "effect.failed",
    )
    return report(
      options,
      input,
      faultSchedule,
      mission,
      "failed",
      releaseEnvelope.result === "uncertain" ? "reconcile_release" : "failed",
      kernel,
      snapshots(),
    )
  }
  progress.enter("complete", "effect.settled")
  return report(
    options,
    input,
    faultSchedule,
    mission,
    "completed",
    "complete",
    kernel,
    snapshots(),
  )
}
