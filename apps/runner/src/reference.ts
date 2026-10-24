import {
  AdapterExecutor,
  buildCapabilityManifest,
  createAdapterBinding,
  type AdapterOperation,
  type AdapterRegistration,
} from "@idemantis/adapter-kit"
import {
  sha256Digest,
  type AdapterInstanceId,
  type ObservationId,
  type RunId,
  type SourceId,
} from "@idemantis/contracts"
import { SecureIdentityFactory, SystemClock, type Clock } from "@idemantis/dispatcher"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import type { SqliteJournal } from "@idemantis/journal-sqlite"
import { executionBindingDigest } from "@idemantis/sdk"
import {
  AccessSimulator,
  ReferenceDeliveryInputSchema,
  ResourceSimulator,
  SimulationKernel,
  VehicleSimulator,
  createReferenceDeliveryMission,
  type FaultRule,
  type ReferenceDeliveryInput,
  type SimulatedObservation,
  type SimulationSnapshot,
  type TargetSnapshot,
} from "@idemantis/simulator"
import type { EffectPage, MissionDetail, TimelinePage } from "@idemantis/transport"
import Schema from "typebox/schema"
import { ulid } from "ulid"
import { RunnerExecution } from "./execution.js"
import type { RunnerStore } from "./store.js"

const adapterIds = {
  resource: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  vehicle: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAW",
  access: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAX",
} as const satisfies Record<string, AdapterInstanceId>

interface ReferenceTargets {
  vehicle: VehicleSimulator
  resource: ResourceSimulator
  access: AccessSimulator
  registrations: AdapterRegistration[]
}

interface ReferenceSession {
  kernel: SimulationKernel
  targets: ReferenceTargets
  execution: RunnerExecution
  recorded_observations: number
  handoff_recorded: boolean
}

export interface ReferenceMissionRunnerOptions {
  journal: SqliteJournal
  store: RunnerStore
  seed?: number
  fault_schedule?: readonly FaultRule[]
  clock?: Clock
  maximum_steps?: number
}

export interface ReferenceRunReport {
  detail: MissionDetail
  timeline: TimelinePage
  effects: EffectPage
  simulation: SimulationSnapshot
  targets: TargetSnapshot[]
}

function adapterPackageDigest(
  adapter: string,
  operations: readonly AdapterOperation<unknown, unknown, unknown>[],
): `sha256:${string}` {
  return sha256Digest({
    adapter,
    version: "1.0.0",
    operations: operations.map((operation) => operation.descriptor),
  }) as `sha256:${string}`
}

function sourceId(target: string): SourceId {
  return `${target.replaceAll(/[^a-zA-Z0-9._:]/g, ".")}.simulator` as SourceId
}

function observationId(): ObservationId {
  return `obs_${ulid()}` as ObservationId
}

function targets(kernel: SimulationKernel, input: ReferenceDeliveryInput): ReferenceTargets {
  const vehicle = new VehicleSimulator(kernel, { vehicle_id: input.vehicle_id })
  const resource = new ResourceSimulator(kernel, { resource_id: input.destination_id })
  const access = new AccessSimulator(kernel, { access_point_id: input.destination_id })
  return {
    vehicle,
    resource,
    access,
    registrations: [
      { adapter_instance_id: adapterIds.resource, operation: resource.reserve },
      { adapter_instance_id: adapterIds.resource, operation: resource.release },
      { adapter_instance_id: adapterIds.vehicle, operation: vehicle.dispatch },
      { adapter_instance_id: adapterIds.access, operation: access.open },
    ],
  }
}

function runtimeFor(selected: ReferenceTargets): MissionRuntimeDefinition {
  const mission = createReferenceDeliveryMission(
    {
      reserve: selected.resource.reserve,
      release: selected.resource.release,
      dispatch: selected.vehicle.dispatch,
      openAccess: selected.access.open,
    },
    { vehicleExecution: "runner" },
  )
  const groups = [
    {
      adapter: "resource",
      id: adapterIds.resource,
      operations: [selected.resource.reserve, selected.resource.release],
    },
    { adapter: "vehicle", id: adapterIds.vehicle, operations: [selected.vehicle.dispatch] },
    { adapter: "access", id: adapterIds.access, operations: [selected.access.open] },
  ]
  const bindings = groups.flatMap((group) => {
    const bundle = buildCapabilityManifest({
      adapter: group.adapter,
      adapter_version: "1.0.0",
      adapter_package_digest: adapterPackageDigest(group.adapter, group.operations),
      operations: group.operations,
    })
    return group.operations.map((operation) =>
      createAdapterBinding(bundle, group.id, operation.descriptor.key),
    )
  })
  return {
    manifest: mission.manifest,
    definition_digest: mission.definitionDigest,
    execution_binding_digest: executionBindingDigest(mission, bindings),
  }
}

export class ReferenceMissionRunner {
  readonly #journal: SqliteJournal
  readonly #store: RunnerStore
  readonly #seed: number
  readonly #faultSchedule: readonly FaultRule[]
  readonly #clock: Clock
  readonly #maximumSteps: number
  readonly #runtime: MissionRuntimeDefinition
  readonly #sessions = new Map<RunId, ReferenceSession>()
  readonly #identities = new SecureIdentityFactory()

  constructor(options: ReferenceMissionRunnerOptions) {
    this.#journal = options.journal
    this.#store = options.store
    this.#seed = options.seed ?? 1
    this.#faultSchedule = structuredClone(options.fault_schedule ?? [])
    this.#clock = options.clock ?? new SystemClock()
    this.#maximumSteps = options.maximum_steps ?? 1_000
    if (!Number.isSafeInteger(this.#maximumSteps) || this.#maximumSteps < 1) {
      throw new RangeError("maximum_steps must be a positive safe integer")
    }
    const kernel = new SimulationKernel({
      seed: this.#seed,
      started_at: this.#clock.now().toISOString(),
      fault_schedule: this.#faultSchedule,
    })
    this.#runtime = runtimeFor(targets(kernel, this.defaultInput()))
    this.#store.registerDefinition(this.#runtime, {
      subject_reference_keys: ["mission_id", "vehicle_id", "destination_id"],
    })
  }

  get runtime(): MissionRuntimeDefinition {
    return structuredClone(this.#runtime)
  }

  defaultInput(): ReferenceDeliveryInput {
    return {
      mission_id: "delivery-001",
      vehicle_id: "vehicle-7",
      route: "pickup-a-to-bay-3",
      pickup: "pickup-a",
      destination_id: "bay-3",
      reservation_key: "delivery-001-bay-3",
      reservation_expires_at: new Date(this.#clock.now().getTime() + 3_600_000).toISOString(),
      access_subject: "payload-42",
    }
  }

  async run(runId: RunId): Promise<void> {
    const stored = this.#journal.getMission(runId)
    if (stored.state.terminal_outcome) {
      return
    }
    let session = this.#sessions.get(runId)
    if (!session) {
      if (stored.state.current_sequence !== 1) {
        throw new Error(`Reference simulator state is unavailable for mission ${runId}`)
      }
      if (!Schema.Check(ReferenceDeliveryInputSchema, stored.state.input)) {
        throw new TypeError(`Mission ${runId} does not use the reference delivery input`)
      }
      session = this.createSession(stored.state.input as ReferenceDeliveryInput)
      this.#sessions.set(runId, session)
    }
    for (let step = 0; step < this.#maximumSteps; step += 1) {
      await session.execution.drain(runId, this.#maximumSteps)
      if (this.#journal.getMission(runId).state.terminal_outcome) {
        return
      }
      if (this.recordObservations(runId, session)) {
        continue
      }
      const current = this.#journal.getMission(runId).state.current_state
      if (current === "await_handoff" && !session.handoff_recorded) {
        this.recordHandoff(runId, session)
        continue
      }
      if (await session.kernel.runNext()) {
        continue
      }
      return
    }
    throw new Error(`Mission ${runId} exceeded the reference execution step limit`)
  }

  report(runId: RunId): ReferenceRunReport {
    const session = this.#sessions.get(runId)
    if (!session) {
      throw new Error(`Reference simulator state is unavailable for mission ${runId}`)
    }
    return {
      detail: this.#store.missionDetail(runId),
      timeline: this.#store.timeline(runId),
      effects: this.#store.effects(runId),
      simulation: session.kernel.snapshot(),
      targets: [
        session.targets.vehicle.snapshot(),
        session.targets.resource.snapshot(),
        session.targets.access.snapshot(),
      ],
    }
  }

  private createSession(input: ReferenceDeliveryInput): ReferenceSession {
    const kernel = new SimulationKernel({
      seed: this.#seed,
      started_at: this.#clock.now().toISOString(),
      fault_schedule: this.#faultSchedule,
    })
    const selected = targets(kernel, input)
    const executor = new AdapterExecutor({
      registrations: selected.registrations,
      source_id: "reference.runner",
      location: "runner",
      clock: this.#clock,
    })
    return {
      kernel,
      targets: selected,
      execution: new RunnerExecution({
        journal: this.#journal,
        executor,
        worker_id: "reference.runner",
        timer_source_id: "reference.timer",
        lease_duration_ms: 30_000,
        clock: this.#clock,
      }),
      recorded_observations: 0,
      handoff_recorded: false,
    }
  }

  private recordObservations(runId: RunId, session: ReferenceSession): boolean {
    const observations = session.kernel.observations()
    const pending = observations.slice(session.recorded_observations)
    for (const observation of pending) {
      this.recordObservation(runId, session, observation)
    }
    session.recorded_observations = observations.length
    return pending.length > 0
  }

  private recordObservation(
    runId: RunId,
    session: ReferenceSession,
    observation: SimulatedObservation,
  ): void {
    session.execution.recordObservation(runId, sourceId(observation.target), {
      event_id: this.#identities.eventId(),
      observation_id: observationId(),
      observation_type: observation.type,
      payload: observation.payload,
      ...(observation.effect_id ? { effect_id: observation.effect_id } : {}),
      subject: observation.subject_reference,
      observed_at: observation.observed_at,
      correlation_id: observation.effect_id ?? runId,
    })
  }

  private recordHandoff(runId: RunId, session: ReferenceSession): void {
    const input = this.#journal.getMission(runId).state.input as ReferenceDeliveryInput
    session.execution.recordObservation(runId, "handoff.simulator", {
      event_id: this.#identities.eventId(),
      observation_id: observationId(),
      observation_type: "delivery.handoff_confirmed",
      subject: input.mission_id,
      payload: { mission_id: input.mission_id, recipient: input.access_subject },
      observed_at: session.kernel.now().toISOString(),
      correlation_id: runId,
    })
    session.handoff_recorded = true
  }
}
