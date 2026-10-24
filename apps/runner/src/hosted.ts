import type { AdapterOperationDescriptor } from "@idemantis/adapter-kit"
import type { RunId, SourceId } from "@idemantis/contracts"
import { type Clock, SystemClock } from "@idemantis/dispatcher"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import type { SqliteJournal } from "@idemantis/journal-sqlite"
import type { MissionExecutionStart, MissionExecutionStatus } from "@idemantis/transport"
import type { AdapterHost } from "./adapters.js"
import { RunnerExecution } from "./execution.js"
import { ExecutionJobs } from "./jobs.js"

const maximumTimeoutMs = 2_147_483_647

export interface HostedMissionRunnerOptions {
  journal: SqliteJournal
  host: AdapterHost
  worker_id?: SourceId
  timer_source_id?: SourceId
  lease_duration_ms?: number
  maximum_steps?: number
  clock?: Clock
}

export class HostedMissionRunner {
  readonly #journal: SqliteJournal
  readonly #host: AdapterHost
  readonly #clock: Clock
  readonly #maximumSteps: number
  readonly #execution: RunnerExecution
  readonly #jobs: ExecutionJobs
  readonly #wakeups = new Map<RunId, ReturnType<typeof setTimeout>>()
  #closed = false

  constructor(options: HostedMissionRunnerOptions) {
    this.#journal = options.journal
    this.#host = options.host
    this.#clock = options.clock ?? new SystemClock()
    this.#maximumSteps = options.maximum_steps ?? 1_000
    if (!Number.isSafeInteger(this.#maximumSteps) || this.#maximumSteps < 1) {
      throw new RangeError("maximum_steps must be a positive safe integer")
    }
    this.#execution = new RunnerExecution({
      journal: options.journal,
      executor: options.host.executor,
      worker_id: options.worker_id ?? "hosted.runner",
      timer_source_id: options.timer_source_id ?? "hosted.timer",
      lease_duration_ms: options.lease_duration_ms ?? 30_000,
      clock: this.#clock,
    })
    this.#jobs = new ExecutionJobs({
      journal: options.journal,
      clock: this.#clock,
      execute: (runId) => this.execute(runId),
    })
  }

  accepts(runtime: MissionRuntimeDefinition): boolean {
    return this.#host.accepts(runtime)
  }

  operations(): AdapterOperationDescriptor[] {
    return this.#host.descriptors()
  }

  compilePlan(plan: unknown): MissionRuntimeDefinition {
    return this.#host.compilePlan(plan)
  }

  start(runId: RunId): MissionExecutionStart {
    return this.#jobs.start(runId)
  }

  executionStatus(runId: RunId): MissionExecutionStatus {
    return this.#jobs.status(runId)
  }

  async run(runId: RunId): Promise<void> {
    return this.#jobs.run(runId)
  }

  async waitForExecution(runId: RunId): Promise<void> {
    await this.#jobs.wait(runId)
  }

  async waitForExecutions(): Promise<void> {
    await this.#jobs.waitAll()
  }

  scheduled(runId: RunId): boolean {
    return this.#wakeups.has(runId)
  }

  async close(): Promise<void> {
    this.#closed = true
    for (const wakeup of this.#wakeups.values()) {
      clearTimeout(wakeup)
    }
    this.#wakeups.clear()
    await this.#jobs.waitAll()
  }

  private async execute(runId: RunId): Promise<void> {
    this.cancelWakeup(runId)
    if (this.#closed) {
      return
    }
    const mission = this.#journal.getMission(runId)
    if (!this.#host.accepts(mission.runtime)) {
      throw new Error(`Mission ${runId} is not bound to the hosted adapters`)
    }
    const drained = await this.#execution.drain(runId, this.#maximumSteps)
    if (drained.status === "idle") {
      this.scheduleWakeup(runId)
    }
  }

  private nextWakeup(runId: RunId, now: number): number | undefined {
    const instants = [
      ...this.#journal
        .listTimers(runId)
        .filter((timer) => timer.status === "scheduled")
        .map((timer) => Date.parse(timer.due_at)),
      ...this.#journal
        .listAttempts(runId)
        .filter((attempt) => attempt.status === "claimed" || attempt.status === "dispatch_started")
        .map((attempt) => Date.parse(attempt.claim_deadline)),
    ].filter((instant) => Number.isFinite(instant) && instant > now)
    return instants.length > 0 ? Math.min(...instants) : undefined
  }

  private scheduleWakeup(runId: RunId): void {
    if (this.#closed) {
      return
    }
    const now = this.#clock.now().getTime()
    const at = this.nextWakeup(runId, now)
    if (at === undefined) {
      return
    }
    const wakeup = setTimeout(
      () => {
        this.#wakeups.delete(runId)
        if (this.#closed || this.#jobs.running(runId)) {
          return
        }
        try {
          this.#jobs.start(runId)
        } catch {}
      },
      Math.min(at - now + 1, maximumTimeoutMs),
    )
    wakeup.unref()
    this.#wakeups.set(runId, wakeup)
  }

  private cancelWakeup(runId: RunId): void {
    const wakeup = this.#wakeups.get(runId)
    if (wakeup) {
      clearTimeout(wakeup)
      this.#wakeups.delete(runId)
    }
  }
}
