import { TimestampSchema, sha256Digest, type JsonValue } from "@idemantis/contracts"
import Schema from "typebox/schema"
import { SimulationError } from "./errors.js"
import { FaultSchedule } from "./faults.js"
import { SeededRandom } from "./random.js"
import { SimulatedObservationSchema, SimulationTraceEntrySchema } from "./schemas.js"
import type {
  FaultMatch,
  FaultRule,
  ScheduledWork,
  SimulatedObservation,
  SimulationKernelOptions,
  SimulationSnapshot,
  SimulationTraceEntry,
  TraceInput,
} from "./types.js"

interface QueueItem extends ScheduledWork {
  id: number
  due_at: number
  run(): void | Promise<void>
}

const defaultStartedAt = "2026-01-01T00:00:00.000Z"

function instant(value: string): number {
  if (!Schema.Check(TimestampSchema, value)) {
    throw new SimulationError("INVALID_TIME", "Simulation time must be a UTC RFC 3339 timestamp")
  }
  const milliseconds = Date.parse(value)
  if (!Number.isFinite(milliseconds)) {
    throw new SimulationError("INVALID_TIME", "Simulation time must be a valid timestamp")
  }
  return milliseconds
}

function date(milliseconds: number): Date {
  const value = new Date(milliseconds)
  if (!Number.isFinite(value.getTime())) {
    throw new SimulationError("INVALID_TIME", "Simulation time exceeds the supported date range")
  }
  return value
}

function positiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new SimulationError("INVALID_SCHEDULE", `${label} must be a non-negative integer`)
  }
}

export class SimulationKernel {
  readonly #seed: number
  readonly #startedAt: string
  readonly #random: SeededRandom
  readonly #faults: FaultSchedule
  readonly #maximumTasks: number
  readonly #clockSkew: Readonly<Record<string, number>>
  readonly #trace: SimulationTraceEntry[] = []
  readonly #observations = new Map<string, SimulatedObservation>()
  readonly #queue: QueueItem[] = []
  #now: number
  #nextTaskId = 1
  #executedTasks = 0

  constructor(options: SimulationKernelOptions) {
    this.#seed = options.seed
    this.#startedAt = options.started_at ?? defaultStartedAt
    this.#now = instant(this.#startedAt)
    this.#random = new SeededRandom(options.seed)
    this.#faults = new FaultSchedule(options.fault_schedule)
    this.#maximumTasks = options.maximum_tasks ?? 10_000
    if (!Number.isSafeInteger(this.#maximumTasks) || this.#maximumTasks < 1) {
      throw new SimulationError("INVALID_SCHEDULE", "maximum_tasks must be a positive integer")
    }
    this.#clockSkew = structuredClone(options.clock_skew_ms ?? {})
    for (const [source, skew] of Object.entries(this.#clockSkew)) {
      if (source.length === 0 || !Number.isSafeInteger(skew)) {
        throw new SimulationError(
          "INVALID_TIME",
          "Clock skew entries must use a source and integer",
        )
      }
      date(this.#now + skew)
    }
  }

  now(source?: string): Date {
    return date(this.#now + (source ? (this.#clockSkew[source] ?? 0) : 0))
  }

  jitter(maximum: number): number {
    positiveInteger(maximum, "jitter maximum")
    return this.#random.integer(0, maximum)
  }

  takeFault(match: FaultMatch): FaultRule | undefined {
    const fault = this.#faults.take(match)
    if (fault) {
      this.record({
        ...match,
        stage: "fault.applied",
        correlation_id: fault.id,
        payload: { action: fault.action, matched_stage: match.stage },
      })
    }
    return fault
  }

  record(input: TraceInput): SimulationTraceEntry {
    const entry: SimulationTraceEntry = {
      sequence: this.#trace.length + 1,
      at: this.now().toISOString(),
      target: input.target,
      operation: input.operation,
      stage: input.stage,
      correlation_id: input.correlation_id,
      payload: structuredClone(input.payload ?? {}),
    }
    if (!Schema.Check(SimulationTraceEntrySchema, entry)) {
      throw new SimulationError("INVALID_SCHEDULE", "Trace entry does not satisfy its schema")
    }
    this.#trace.push(entry)
    return structuredClone(entry)
  }

  schedule(work: ScheduledWork, run: () => void | Promise<void>): number {
    positiveInteger(work.after_ms, "schedule delay")
    positiveInteger(work.jitter_ms ?? 0, "schedule jitter")
    const preflight: SimulationTraceEntry = {
      sequence: this.#trace.length + 1,
      at: this.now().toISOString(),
      target: work.target,
      operation: work.operation,
      stage: "work.scheduled",
      correlation_id: work.correlation_id,
      payload: structuredClone(work.payload ?? {}),
    }
    if (!Schema.Check(SimulationTraceEntrySchema, preflight)) {
      throw new SimulationError("INVALID_SCHEDULE", "Scheduled work does not satisfy its schema")
    }
    date(this.#now + work.after_ms + (work.jitter_ms ?? 0))
    const dueAt = this.#now + work.after_ms + this.jitter(work.jitter_ms ?? 0)
    const item: QueueItem = {
      ...structuredClone(work),
      id: this.#nextTaskId,
      due_at: dueAt,
      run,
    }
    this.record({
      ...work,
      stage: "work.scheduled",
      payload: {
        ...structuredClone(work.payload ?? {}),
        task_id: item.id,
        due_at: date(dueAt).toISOString(),
      },
    })
    this.#nextTaskId += 1
    this.#queue.push(item)
    this.#queue.sort((left, right) => left.due_at - right.due_at || left.id - right.id)
    return item.id
  }

  async runNext(): Promise<boolean> {
    const item = this.#queue.shift()
    if (!item) {
      return false
    }
    this.#executedTasks += 1
    if (this.#executedTasks > this.#maximumTasks) {
      throw new SimulationError("QUEUE_LIMIT_EXCEEDED", "Simulation task limit was exceeded")
    }
    this.#now = item.due_at
    this.record({
      target: item.target,
      operation: item.operation,
      stage: item.stage,
      correlation_id: item.correlation_id,
      payload: { ...structuredClone(item.payload ?? {}), task_id: item.id },
    })
    await item.run()
    return true
  }

  async runUntilIdle(): Promise<void> {
    while (await this.runNext()) {}
  }

  async runUntil(predicate: () => boolean, deadline?: Date): Promise<boolean> {
    const deadlineAt = deadline?.getTime()
    if (deadlineAt !== undefined && (!Number.isFinite(deadlineAt) || deadlineAt < this.#now)) {
      throw new SimulationError("INVALID_TIME", "Simulation deadline cannot precede current time")
    }
    while (!predicate()) {
      const next = this.#queue[0]
      if (deadlineAt !== undefined && (!next || next.due_at > deadlineAt)) {
        this.#now = deadlineAt
        return false
      }
      if (!(await this.runNext())) {
        return false
      }
    }
    return true
  }

  publishObservation(observation: SimulatedObservation): boolean {
    if (!Schema.Check(SimulatedObservationSchema, observation)) {
      throw new SimulationError("TARGET_STATE_INVALID", "Observation does not satisfy its schema")
    }
    const duplicate = this.#observations.has(observation.observation_id)
    if (!duplicate) {
      this.#observations.set(observation.observation_id, structuredClone(observation))
    }
    this.record({
      target: observation.target,
      operation: observation.type,
      stage: duplicate ? "observation.duplicate" : "observation.accepted",
      correlation_id: observation.observation_id,
      payload: {
        subject_reference: observation.subject_reference,
        ...(observation.effect_id ? { effect_id: observation.effect_id } : {}),
      },
    })
    return !duplicate
  }

  observations(type?: string): SimulatedObservation[] {
    return [...this.#observations.values()]
      .filter((observation) => type === undefined || observation.type === type)
      .map((observation) => structuredClone(observation))
  }

  pendingTasks(): number {
    return this.#queue.length
  }

  snapshot(): SimulationSnapshot {
    const trace = structuredClone(this.#trace)
    return {
      seed: this.#seed,
      started_at: this.#startedAt,
      now: this.now().toISOString(),
      trace,
      trace_digest: sha256Digest(trace as unknown as JsonValue),
      observations: this.observations(),
      faults: this.#faults.snapshot(),
    }
  }
}
