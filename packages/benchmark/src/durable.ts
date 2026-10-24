import type { JsonObject, RunId } from "@idemantis/contracts"
import { VirtualClock } from "@idemantis/dispatcher"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import { AdapterHost, RunnerExecution, RunnerStore } from "@idemantis/runner"
import { fromInput, fromOutput, mapFields } from "@idemantis/sdk"
import {
  createToolAdapter,
  defineTool,
  ToolLedger,
  ToolUnavailable,
  toolMission,
} from "@idemantis/tools"
import Type from "typebox"
import type { ModeRun, TaskOutcome } from "./baselines.js"
import {
  ExternalWorld,
  type FaultRates,
  ProcessCrash,
  TransientFailure,
  workflowSteps,
} from "./world.js"

export interface DurableOptions {
  seed: number
  tasks: readonly string[]
  rates: FaultRates
  attempts: number
  restarts: number
}

const leaseMs = 30_000
const output = Type.Object({ id: Type.String() }, { additionalProperties: false })

function tools(world: ExternalWorld, crash: () => void) {
  return workflowSteps.map((step) =>
    defineTool({
      name: `bench.${step}`,
      input: Type.Object({ task: Type.String(), after: Type.Optional(Type.String()) }),
      output,
      run: (input, context) => {
        try {
          return Promise.resolve(world.call(input.task, step, context.idempotency_key))
        } catch (error) {
          if (error instanceof TransientFailure) {
            return Promise.reject(new ToolUnavailable(error.message))
          }
          if (error instanceof ProcessCrash) {
            crash()
            return new Promise<never>(() => {})
          }
          return Promise.reject(error)
        }
      },
      lookup: async (_input, context) => {
        const found = world.lookup(context.idempotency_key)
        return found ? { found: true, output: found } : { found: false }
      },
    }),
  )
}

function stepInput(index: number) {
  const previous = workflowSteps[index - 1]
  return previous
    ? mapFields({ task: fromInput("task"), after: fromOutput(previous, "id") })
    : mapFields({ task: fromInput("task") })
}

export async function runDurable(options: DurableOptions): Promise<ModeRun> {
  const world = new ExternalWorld(options.seed, options.rates)
  const clock = new VirtualClock("2026-01-01T00:00:00.000Z")
  const journal = new SqliteJournal({ path: ":memory:", clock: () => clock.now() })
  const ledger = new ToolLedger({ path: ":memory:", clock: () => clock.now() })
  const outcomes = new Map<string, TaskOutcome>()
  let restarts = 0
  let crashed: (() => void) | undefined
  const crash = () => crashed?.()
  const toolset = tools(world, crash)
  const mission = toolMission({
    id: "bench.order",
    version: "1.0.0",
    adapter: "bench-tools",
    input: Type.Object({ task: Type.String() }, { additionalProperties: false }),
    attempts: options.attempts,
    backoff_ms: 1_000,
    steps: workflowSteps.map((step, index) => ({
      name: step,
      tool: toolset[index] as (typeof toolset)[number],
      input: stepInput(index),
    })),
  })
  const host = () =>
    new AdapterHost({
      source_id: "bench.runner",
      clock,
      adapters: [
        createToolAdapter({
          adapter: "bench-tools",
          version: "1.0.0",
          tools: toolset,
          ledger,
          instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        }),
      ],
    })
  const store = new RunnerStore({ journal, clock: () => clock.now() })
  const runtime = host().bind({
    manifest: mission.manifest,
    definition_digest: mission.definitionDigest,
  })
  store.registerDefinition(runtime)
  try {
    for (const task of options.tasks) {
      const runId = store.admitMission({
        definition_digest: runtime.definition_digest,
        input: { task } as JsonObject,
      }).summary.run_id as RunId
      let lives = 0
      for (let pass = 0; pass < 200; pass += 1) {
        if (journal.getMission(runId).state.terminal_outcome) {
          break
        }
        const execution = new RunnerExecution({
          journal,
          executor: host().executor,
          worker_id: "bench.runner",
          timer_source_id: "bench.timer",
          lease_duration_ms: leaseMs,
          clock,
        })
        const died = new Promise<"crashed">((resolvePromise) => {
          crashed = () => resolvePromise("crashed")
        })
        const result = await Promise.race([execution.drain(runId), died])
        crashed = undefined
        if (result === "crashed") {
          lives += 1
          restarts += 1
          clock.advanceBy(leaseMs + 1)
          if (lives > options.restarts) {
            break
          }
          continue
        }
        if (result.status === "terminal") {
          break
        }
        const next = journal
          .listTimers(runId)
          .filter((timer) => timer.status === "scheduled")
          .map((timer) => Date.parse(timer.due_at))
          .sort((left, right) => left - right)[0]
        if (next === undefined) {
          break
        }
        clock.advanceTo(new Date(Math.max(next, clock.now().getTime())))
      }
      const terminal = journal.getMission(runId).state.terminal_outcome
      outcomes.set(
        task,
        terminal === "completed" ? "completed" : terminal === "unresolved" ? "flagged" : "failed",
      )
      journal.verifyRun(runId)
    }
  } finally {
    journal.close()
    ledger.close()
  }
  return { world, outcomes, restarts }
}
