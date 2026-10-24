import {
  ExternalWorld,
  type FaultRates,
  LostResponse,
  ProcessCrash,
  TransientFailure,
  workflowSteps,
} from "./world.js"

export type TaskOutcome = "completed" | "failed" | "flagged"

export interface ModeRun {
  world: ExternalWorld
  outcomes: Map<string, TaskOutcome>
  restarts: number
}

export interface BaselineOptions {
  seed: number
  tasks: readonly string[]
  rates: FaultRates
  attempts: number
  restarts: number
  checkpoint: boolean
}

export function runBaseline(options: BaselineOptions): ModeRun {
  const world = new ExternalWorld(options.seed, options.rates)
  const outcomes = new Map<string, TaskOutcome>()
  let restarts = 0
  for (const task of options.tasks) {
    let completedSteps = 0
    let lives = 0
    let outcome: TaskOutcome | undefined
    while (!outcome) {
      let crashed = false
      let step = options.checkpoint ? completedSteps : 0
      while (step < workflowSteps.length && !crashed && !outcome) {
        const name = workflowSteps[step] as (typeof workflowSteps)[number]
        let attempt = 0
        let done = false
        while (!done && !crashed) {
          attempt += 1
          try {
            world.call(task, name)
            done = true
          } catch (error) {
            if (error instanceof ProcessCrash) {
              crashed = true
            } else if (
              !(error instanceof TransientFailure || error instanceof LostResponse) ||
              attempt >= options.attempts
            ) {
              outcome = "failed"
              break
            }
          }
        }
        if (done) {
          step += 1
          completedSteps = Math.max(completedSteps, step)
        }
      }
      if (!outcome && !crashed) {
        outcome = "completed"
      }
      if (crashed) {
        lives += 1
        restarts += 1
        if (lives > options.restarts) {
          outcome = "failed"
        }
      }
    }
    outcomes.set(task, outcome)
  }
  return { world, outcomes, restarts }
}
