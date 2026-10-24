import type { RunId } from "@idemantis/contracts"
import type { Clock } from "@idemantis/dispatcher"
import type { SqliteJournal } from "@idemantis/journal-sqlite"
import {
  ApiSchemaVersion,
  type MissionExecutionStart,
  type MissionExecutionStatus,
} from "@idemantis/transport"

interface ExecutionJob {
  activity: "running" | "idle" | "failed"
  started_at: string
  updated_at: string
  promise: Promise<void>
  finished_at?: string
  failure_code?: "EXECUTION_FAILED"
}

export interface ExecutionJobsOptions {
  journal: SqliteJournal
  clock: Clock
  execute: (runId: RunId) => Promise<void>
}

function defer(): Promise<void> {
  return new Promise((resolvePromise) => setImmediate(resolvePromise))
}

export class ExecutionJobs {
  readonly #journal: SqliteJournal
  readonly #clock: Clock
  readonly #execute: (runId: RunId) => Promise<void>
  readonly #jobs = new Map<RunId, ExecutionJob>()

  constructor(options: ExecutionJobsOptions) {
    this.#journal = options.journal
    this.#clock = options.clock
    this.#execute = options.execute
  }

  start(runId: RunId): MissionExecutionStart {
    const mission = this.#journal.getMission(runId)
    if (mission.state.terminal_outcome) {
      return {
        schema_version: ApiSchemaVersion,
        disposition: "already_terminal",
        execution: this.status(runId),
      }
    }
    if (this.running(runId)) {
      return {
        schema_version: ApiSchemaVersion,
        disposition: "already_running",
        execution: this.status(runId),
      }
    }
    const startedAt = this.#clock.now().toISOString()
    const job: ExecutionJob = {
      activity: "running",
      started_at: startedAt,
      updated_at: startedAt,
      promise: Promise.resolve(),
    }
    const execution = defer().then(() => this.#execute(runId))
    job.promise = execution
    this.#jobs.set(runId, job)
    void execution.then(
      () => {
        const finishedAt = this.#clock.now().toISOString()
        job.activity = "idle"
        job.updated_at = finishedAt
        job.finished_at = finishedAt
      },
      () => {
        const finishedAt = this.#clock.now().toISOString()
        job.activity = "failed"
        job.updated_at = finishedAt
        job.finished_at = finishedAt
        job.failure_code = "EXECUTION_FAILED"
      },
    )
    return {
      schema_version: ApiSchemaVersion,
      disposition: "started",
      execution: this.status(runId),
    }
  }

  status(runId: RunId): MissionExecutionStatus {
    const mission = this.#journal.getMission(runId)
    const job = this.#jobs.get(runId)
    if (mission.state.terminal_outcome) {
      return {
        schema_version: ApiSchemaVersion,
        run_id: runId,
        activity: "terminal",
        updated_at: mission.updated_at,
        ...(job ? { started_at: job.started_at } : {}),
        ...(job?.finished_at ? { finished_at: job.finished_at } : {}),
      }
    }
    if (!job) {
      return {
        schema_version: ApiSchemaVersion,
        run_id: runId,
        activity: "idle",
        updated_at: mission.updated_at,
      }
    }
    if (job.activity === "running") {
      return {
        schema_version: ApiSchemaVersion,
        run_id: runId,
        activity: "running",
        updated_at: job.updated_at,
        started_at: job.started_at,
      }
    }
    if (job.activity === "failed") {
      if (!job.finished_at || !job.failure_code) {
        throw new Error(`Execution state is incomplete for mission ${runId}`)
      }
      return {
        schema_version: ApiSchemaVersion,
        run_id: runId,
        activity: "failed",
        updated_at: job.updated_at,
        started_at: job.started_at,
        finished_at: job.finished_at,
        failure_code: job.failure_code,
      }
    }
    return {
      schema_version: ApiSchemaVersion,
      run_id: runId,
      activity: "idle",
      updated_at: job.updated_at,
      started_at: job.started_at,
      ...(job.finished_at ? { finished_at: job.finished_at } : {}),
    }
  }

  running(runId: RunId): boolean {
    return this.#jobs.get(runId)?.activity === "running"
  }

  async run(runId: RunId): Promise<void> {
    const current = this.#jobs.get(runId)
    if (current?.activity === "running") {
      return current.promise
    }
    this.#jobs.delete(runId)
    return this.#execute(runId)
  }

  async wait(runId: RunId): Promise<void> {
    await this.#jobs.get(runId)?.promise
  }

  async waitAll(): Promise<void> {
    await Promise.all([...this.#jobs.values()].map((job) => job.promise))
  }
}
