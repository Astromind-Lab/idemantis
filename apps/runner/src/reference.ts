import {
  AdapterExecutor,
  type AdapterOperation,
  type AdapterRegistration,
  buildCapabilityManifest,
  createAdapterBinding,
} from "@idemantis/adapter-kit"
import {
  type AdapterInstanceId,
  type AttemptRecord,
  type CommandRecord,
  canonicalJson,
  type JsonObject,
  type JsonValue,
  type ObservationId,
  type RunId,
  type SourceId,
  sha256Digest,
} from "@idemantis/contracts"
import { type Clock, SecureIdentityFactory, SystemClock } from "@idemantis/dispatcher"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import type { SqliteJournal } from "@idemantis/journal-sqlite"
import { executionBindingDigest } from "@idemantis/sdk"
import {
  AccessSimulator,
  createReferenceDeliveryMission,
  type FaultRule,
  type ReferenceDeliveryInput,
  ReferenceDeliveryInputSchema,
  ResourceSimulator,
  type SimulatedObservation,
  SimulationKernel,
  type SimulationSnapshot,
  type TargetSnapshot,
  VehicleSimulator,
} from "@idemantis/simulator"
import type {
  EffectPage,
  MissionDetail,
  MissionExecutionStart,
  MissionExecutionStatus,
  TimelinePage,
} from "@idemantis/transport"
import Schema from "typebox/schema"
import { ulid } from "ulid"
import { RunnerExecution } from "./execution.js"
import { ExecutionJobs } from "./jobs.js"
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
  executor: AdapterExecutor
  execution: RunnerExecution
  clock: RecoveryClock
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
  transition_delay_ms?: number
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

function jsonObject(value: unknown, label: string): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`)
  }
  return value as JsonObject
}

function stringField(value: JsonObject, key: string, label: string): string {
  const selected = value[key]
  if (typeof selected !== "string") {
    throw new TypeError(`${label} must contain ${key}`)
  }
  return selected
}

function valueField(value: JsonObject, key: string): JsonValue | undefined {
  return value[key]
}

function equivalent(left: JsonValue | undefined, right: JsonValue | undefined): boolean {
  return canonicalJson(left ?? null) === canonicalJson(right ?? null)
}

function delay(duration: number): Promise<void> {
  return duration === 0
    ? Promise.resolve()
    : new Promise((resolvePromise) => setTimeout(resolvePromise, duration))
}

class RecoveryClock implements Clock {
  readonly #live: Clock
  #replay: Date | undefined

  constructor(live: Clock) {
    this.#live = live
  }

  now(): Date {
    return this.#replay ? new Date(this.#replay) : this.#live.now()
  }

  replay(at: string): void {
    this.#replay = new Date(at)
  }

  resume(): void {
    this.#replay = undefined
  }
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
  readonly #transitionDelayMs: number
  readonly #runtime: MissionRuntimeDefinition
  readonly #sessions = new Map<RunId, ReferenceSession>()
  readonly #jobs: ExecutionJobs
  readonly #identities = new SecureIdentityFactory()

  constructor(options: ReferenceMissionRunnerOptions) {
    this.#journal = options.journal
    this.#store = options.store
    this.#seed = options.seed ?? 1
    this.#faultSchedule = structuredClone(options.fault_schedule ?? [])
    this.#clock = options.clock ?? new SystemClock()
    this.#maximumSteps = options.maximum_steps ?? 1_000
    this.#transitionDelayMs = options.transition_delay_ms ?? 0
    if (!Number.isSafeInteger(this.#maximumSteps) || this.#maximumSteps < 1) {
      throw new RangeError("maximum_steps must be a positive safe integer")
    }
    if (
      !Number.isSafeInteger(this.#transitionDelayMs) ||
      this.#transitionDelayMs < 0 ||
      this.#transitionDelayMs > 60_000
    ) {
      throw new RangeError("transition_delay_ms must be an integer from 0 through 60000")
    }
    const kernel = new SimulationKernel({
      seed: this.#seed,
      started_at: this.#clock.now().toISOString(),
      fault_schedule: this.#faultSchedule,
    })
    this.#runtime = runtimeFor(targets(kernel, this.defaultInput()))
    this.#jobs = new ExecutionJobs({
      journal: this.#journal,
      clock: this.#clock,
      execute: (runId) => this.execute(runId),
    })
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

  accepts(runtime: MissionRuntimeDefinition): boolean {
    return (
      runtime.definition_digest === this.#runtime.definition_digest &&
      runtime.execution_binding_digest === this.#runtime.execution_binding_digest
    )
  }

  start(runId: RunId): MissionExecutionStart {
    return this.#jobs.start(runId)
  }

  executionStatus(runId: RunId): MissionExecutionStatus {
    return this.#jobs.status(runId)
  }

  async waitForExecution(runId: RunId): Promise<void> {
    await this.#jobs.wait(runId)
  }

  async waitForExecutions(): Promise<void> {
    await this.#jobs.waitAll()
  }

  async run(runId: RunId): Promise<void> {
    return this.#jobs.run(runId)
  }

  private async execute(runId: RunId): Promise<void> {
    const stored = this.#journal.getMission(runId)
    let session = this.#sessions.get(runId)
    if (!session) {
      if (!Schema.Check(ReferenceDeliveryInputSchema, stored.state.input)) {
        throw new TypeError(`Mission ${runId} does not use the reference delivery input`)
      }
      session = this.createSession(
        stored.state.input as ReferenceDeliveryInput,
        this.admittedAt(runId),
      )
      if (stored.state.current_sequence > 1) {
        await this.restoreSession(runId, session)
      }
      this.#sessions.set(runId, session)
    }
    session.clock.resume()
    if (stored.state.terminal_outcome) {
      return
    }
    for (let step = 0; step < this.#maximumSteps; step += 1) {
      const drained = await session.execution.drain(runId, this.#maximumSteps)
      if (this.#journal.getMission(runId).state.terminal_outcome) {
        return
      }
      if (drained.dispatches > 0 || drained.timers > 0) {
        await delay(this.#transitionDelayMs)
      }
      if (this.recordObservations(runId, session)) {
        await delay(this.#transitionDelayMs)
        continue
      }
      const current = this.#journal.getMission(runId).state.current_state
      if (current === "await_handoff" && !session.handoff_recorded) {
        this.recordHandoff(runId, session)
        await delay(this.#transitionDelayMs)
        continue
      }
      if (await session.kernel.runNext()) {
        await delay(this.#transitionDelayMs)
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

  private createSession(input: ReferenceDeliveryInput, startedAt: string): ReferenceSession {
    const kernel = new SimulationKernel({
      seed: this.#seed,
      started_at: startedAt,
      fault_schedule: this.#faultSchedule,
    })
    const selected = targets(kernel, input)
    const clock = new RecoveryClock(this.#clock)
    const executor = new AdapterExecutor({
      registrations: selected.registrations,
      source_id: "reference.runner",
      location: "runner",
      clock,
    })
    return {
      kernel,
      targets: selected,
      executor,
      execution: new RunnerExecution({
        journal: this.#journal,
        executor,
        worker_id: "reference.runner",
        timer_source_id: "reference.timer",
        lease_duration_ms: 30_000,
        clock,
      }),
      clock,
      recorded_observations: 0,
      handoff_recorded: false,
    }
  }

  private admittedAt(runId: RunId): string {
    const admitted = this.#journal.listStoredEvents(runId)[0]?.event
    if (admitted?.type !== "mission.admitted") {
      throw new Error(`Mission ${runId} has no admission event`)
    }
    return admitted.recorded_at
  }

  private async restoreSession(runId: RunId, session: ReferenceSession): Promise<void> {
    const commands = new Map(
      this.#journal.listCommands(runId).map((command) => [command.command_id, command]),
    )
    const attempts = new Map(
      this.#journal.listAttempts(runId).map((attempt) => [attempt.attempt_id, attempt]),
    )
    const events = this.#journal.listStoredEvents(runId).map((record) => record.event)
    for (const event of events.slice(1)) {
      if (
        event.type === "effect.issue_result_recorded" ||
        event.type === "effect.reconciliation_result_recorded"
      ) {
        await this.restoreEffectResult(
          session,
          event.payload,
          event.recorded_at,
          commands,
          attempts,
        )
        continue
      }
      if (event.type === "observation.recorded") {
        const source = event.source.source_id
        if (source === "handoff.simulator") {
          session.handoff_recorded = true
          continue
        }
        await this.restoreObservation(session, event.payload, event.observed_at, source)
      }
    }
    session.recorded_observations = session.kernel.observations().length
    session.clock.resume()
  }

  private async restoreEffectResult(
    session: ReferenceSession,
    payload: JsonObject,
    recordedAt: string,
    commands: Map<string, CommandRecord>,
    attempts: Map<string, AttemptRecord>,
  ): Promise<void> {
    const commandId = stringField(payload, "command_id", "Effect result")
    const attemptId = stringField(payload, "attempt_id", "Effect result")
    const expected = stringField(payload, "result", "Effect result")
    const command = commands.get(commandId)
    const attempt = attempts.get(attemptId)
    if (!command || !attempt) {
      throw new Error(`Reference recovery evidence is incomplete for command ${commandId}`)
    }
    session.clock.replay(recordedAt)
    let actual: string
    try {
      actual =
        command.kind === "effect.issue"
          ? await session.executor.issue(command, attempt)
          : await session.executor.reconcile(command, attempt)
    } catch {
      actual = command.kind === "effect.issue" ? "uncertain" : "unknown"
    }
    if (actual !== expected) {
      throw new Error(
        `Reference recovery diverged for command ${commandId}: expected ${expected}, received ${actual}`,
      )
    }
  }

  private async restoreObservation(
    session: ReferenceSession,
    payload: JsonObject,
    observedAt: string | undefined,
    recordedSource: SourceId,
  ): Promise<void> {
    const expectedType = stringField(payload, "observation_type", "Observation")
    const expectedPayload = jsonObject(valueField(payload, "payload"), "Observation payload")
    const priorCount = session.kernel.observations().length
    let next = session.kernel.observations()[priorCount]
    while (!next && (await session.kernel.runNext())) {
      next = session.kernel.observations()[priorCount]
    }
    if (
      !next ||
      next.type !== expectedType ||
      sourceId(next.target) !== recordedSource ||
      next.effect_id !== valueField(payload, "effect_id") ||
      next.subject_reference !== valueField(payload, "subject") ||
      next.observed_at !== observedAt ||
      !equivalent(next.payload, expectedPayload)
    ) {
      throw new Error(`Reference recovery diverged while restoring ${expectedType}`)
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
