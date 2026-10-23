import { runReferenceScenario } from "@idemantis/simulator"
import { engineCaseFor } from "./catalog.js"
import { InvariantChecker } from "./invariants.js"
import type { AppliedEffectMetric, EngineCaseReport } from "./types.js"

const maximumAppliedEffects = new Map([
  ["destination-resource\u0000resource.reserve", 1],
  ["destination-resource\u0000resource.release", 1],
  ["vehicle\u0000vehicle.dispatch", 1],
  ["access-service\u0000access.open", 1],
])

export async function runEngineCase(seed: number): Promise<EngineCaseReport> {
  const selected = engineCaseFor(seed)
  const options = {
    scenario: selected.scenario,
    seed,
    fault_schedule: selected.fault_schedule,
  } as const
  const result = await runReferenceScenario(options)
  const replay = await runReferenceScenario(options)
  const checker = new InvariantChecker()
  const appliedEffects: AppliedEffectMetric[] = result.targets.flatMap((target) =>
    target.metrics.map((metric) => ({
      target: target.target,
      ...metric,
      maximum_applied_effects:
        maximumAppliedEffects.get(`${target.target}\u0000${metric.operation}`) ?? 1,
    })),
  )
  const traceSequences = result.simulation.trace.map((entry) => entry.sequence)
  const terminalIndex = result.simulation.trace.findLastIndex(
    (entry) =>
      entry.target === "mission" &&
      entry.stage === "state.entered" &&
      Reflect.get(entry.payload, "state") === result.terminal_state,
  )
  const postTerminalEffects =
    terminalIndex < 0
      ? []
      : result.simulation.trace
          .slice(terminalIndex + 1)
          .filter((entry) => entry.stage === "effect.applied")

  checker.check(
    "engine_trace_is_gapless",
    traceSequences.every((sequence, index) => sequence === index + 1),
    "Simulation trace sequence is not gapless",
  )
  checker.check(
    "engine_replay_is_deterministic",
    replay.simulation.trace_digest === result.simulation.trace_digest &&
      replay.target_state_digest === result.target_state_digest,
    "The same seed and schedule produced a different digest",
  )
  checker.check(
    "engine_outcome_matches_case",
    result.outcome === selected.expected_outcome,
    `Expected ${selected.expected_outcome}, received ${result.outcome}`,
  )
  checker.check(
    "engine_fault_schedule_is_consumed",
    result.simulation.faults.every((fault) => fault.consumed),
    "At least one generated fault was not reached",
  )
  checker.check(
    "engine_target_applied_counts_satisfy_policy",
    appliedEffects.every(
      (metric) =>
        metric.applied_effects <= metric.maximum_applied_effects &&
        metric.applied_effects === new Set(metric.applied_effect_ids).size,
    ),
    "A target applied an effect more often than its operation policy allows",
  )
  checker.check(
    "engine_terminal_state_emits_no_effect",
    terminalIndex >= 0 && postTerminalEffects.length === 0,
    "An external effect was applied after the terminal state",
  )
  for (const invariant of result.invariants) {
    checker.check(
      `simulator_${invariant.name}`,
      invariant.passed,
      `Simulator invariant ${invariant.name} failed`,
    )
  }

  return {
    name: selected.name,
    seed,
    fault_schedule: structuredClone(selected.fault_schedule),
    definition_digest: result.mission_definition_digest,
    outcome: result.outcome,
    final_state_digest: result.target_state_digest,
    trace_digest: result.simulation.trace_digest,
    applied_effects: appliedEffects,
    invariants: checker.results(),
  }
}
