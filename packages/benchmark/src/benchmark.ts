import { type ModeRun, runBaseline } from "./baselines.js"
import { runDurable } from "./durable.js"
import { type FaultRates, workflowSteps } from "./world.js"

export const BenchmarkSchemaVersion = 1

export interface BenchmarkOptions {
  seed?: number
  tasks?: number
  rates?: Partial<FaultRates>
  attempts?: number
  restarts?: number
}

export interface ModeMetrics {
  tasks: number
  completed: number
  failed: number
  flagged_for_review: number
  completion_rate: number
  duplicate_side_effects: number
  tasks_with_duplicates: number
  completed_with_hidden_duplicates: number
  failed_after_partial_side_effects: number
  external_calls: number
  restarts: number
}

export interface BenchmarkReport {
  schema_version: number
  workflow: string[]
  seed: number
  tasks: number
  rates: FaultRates
  attempts: number
  restarts: number
  modes: { naive: ModeMetrics; checkpointed: ModeMetrics; idemantis: ModeMetrics }
}

function rate(value: number): number {
  if (!Number.isFinite(value) || value < 0 || value > 1) {
    throw new RangeError("Fault rates must be between 0 and 1")
  }
  return value
}

function metrics(run: ModeRun, tasks: readonly string[]): ModeMetrics {
  let duplicates = 0
  let tasksWithDuplicates = 0
  let hidden = 0
  let partialFailures = 0
  let completed = 0
  let failed = 0
  let flagged = 0
  for (const task of tasks) {
    const extra = workflowSteps.reduce(
      (total, step) => total + Math.max(0, run.world.applied(task, step) - 1),
      0,
    )
    duplicates += extra
    if (extra > 0) {
      tasksWithDuplicates += 1
    }
    const outcome = run.outcomes.get(task)
    const partial = workflowSteps.some((step) => run.world.applied(task, step) > 0)
    if (outcome === "completed") {
      completed += 1
      if (extra > 0) {
        hidden += 1
      }
    } else if (outcome === "flagged") {
      flagged += 1
    } else {
      failed += 1
      if (partial) {
        partialFailures += 1
      }
    }
  }
  return {
    tasks: tasks.length,
    completed,
    failed,
    flagged_for_review: flagged,
    completion_rate: Math.round((completed / tasks.length) * 10_000) / 10_000,
    duplicate_side_effects: duplicates,
    tasks_with_duplicates: tasksWithDuplicates,
    completed_with_hidden_duplicates: hidden,
    failed_after_partial_side_effects: partialFailures,
    external_calls: run.world.totalCalls,
    restarts: run.restarts,
  }
}

export async function runBenchmark(options: BenchmarkOptions = {}): Promise<BenchmarkReport> {
  const seed = options.seed ?? 1
  const count = options.tasks ?? 200
  if (!Number.isSafeInteger(count) || count < 1 || count > 100_000) {
    throw new RangeError("Task count must be from 1 through 100000")
  }
  const rates: FaultRates = {
    transient: rate(options.rates?.transient ?? 0.1),
    lost_response: rate(options.rates?.lost_response ?? 0.08),
    crash: rate(options.rates?.crash ?? 0.04),
  }
  if (rates.transient + rates.lost_response + rates.crash >= 1) {
    throw new RangeError("Fault rates must sum to less than 1")
  }
  const attempts = options.attempts ?? 3
  const restarts = options.restarts ?? 3
  const tasks = Array.from({ length: count }, (_, index) => `task-${index + 1}`)
  const common = { seed, tasks, rates, attempts, restarts }
  return {
    schema_version: BenchmarkSchemaVersion,
    workflow: [...workflowSteps],
    seed,
    tasks: count,
    rates,
    attempts,
    restarts,
    modes: {
      naive: metrics(runBaseline({ ...common, checkpoint: false }), tasks),
      checkpointed: metrics(runBaseline({ ...common, checkpoint: true }), tasks),
      idemantis: metrics(await runDurable(common), tasks),
    },
  }
}
