import type { Digest } from "@idemantis/contracts"
import type { FaultRule, ReferenceScenario, TargetOperationMetrics } from "@idemantis/simulator"

export interface InvariantResult {
  name: string
  passed: boolean
  detail?: string
}

export interface EdgeFaultStep {
  boundary: string
  action: string
}

export interface EngineCaseDefinition {
  name: string
  scenario: ReferenceScenario
  fault_schedule: FaultRule[]
  expected_outcome: "completed" | "awaiting_operator"
}

export interface EdgeCaseDefinition {
  name: string
  fault_schedule: EdgeFaultStep[]
}

export interface AppliedEffectMetric extends TargetOperationMetrics {
  target: string
  maximum_applied_effects: number
}

export interface EngineCaseReport {
  name: string
  seed: number
  fault_schedule: FaultRule[]
  definition_digest: Digest
  outcome: string
  final_state_digest: Digest
  trace_digest: Digest
  applied_effects: AppliedEffectMetric[]
  invariants: InvariantResult[]
}

export interface EdgeCaseReport {
  name: string
  seed: number
  fault_schedule: EdgeFaultStep[]
  definition_digest: Digest
  stopped_reason?: string
  final_state_digest: Digest
  branch_event_count: number
  applied_effects: AppliedEffectMetric[]
  invariants: InvariantResult[]
}

export interface SeedReport {
  seed: number
  engine: EngineCaseReport
  edge: EdgeCaseReport
  passed: boolean
}

export interface FaultFailure {
  seed: number
  case: string
  invariant: string
  detail: string
  reproduce: string
}

export interface FaultSuiteOptions {
  seed?: number
  count?: number
}

export interface FaultSuiteReport {
  schema_version: 1
  engine_version: string
  seed: number
  count: number
  named_cases: {
    engine: string[]
    edge: string[]
  }
  invariants_checked: string[]
  runs: SeedReport[]
  failures: FaultFailure[]
  passed: boolean
  final_state_digest: Digest
}
