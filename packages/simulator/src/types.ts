import type { AdapterTargetMetrics } from "@idemantis/adapter-kit"
import type { Digest, EffectId, JsonObject } from "@idemantis/contracts"
import type { Static } from "typebox"
import type {
  FaultActionSchema,
  FaultRuleSchema,
  SimulatedObservationSchema,
  SimulationTraceEntrySchema,
} from "./schemas.js"

export type FaultAction = Static<typeof FaultActionSchema>
export type FaultRule = Static<typeof FaultRuleSchema>
export type SimulationTraceEntry = Static<typeof SimulationTraceEntrySchema>
export type SimulatedObservation = Static<typeof SimulatedObservationSchema>

export interface SimulationKernelOptions {
  seed: number
  started_at?: string
  fault_schedule?: readonly FaultRule[]
  maximum_tasks?: number
  clock_skew_ms?: Readonly<Record<string, number>>
}

export interface ScheduledWork {
  target: string
  operation: string
  stage: string
  correlation_id: string
  after_ms: number
  jitter_ms?: number
  payload?: JsonObject
}

export interface TraceInput {
  target: string
  operation: string
  stage: string
  correlation_id: string
  payload?: JsonObject
}

export interface FaultMatch {
  target: string
  operation: string
  stage: string
}

export interface FaultRuleState extends FaultRule {
  consumed: boolean
  matches: number
}

export interface TargetOperationMetrics extends AdapterTargetMetrics {
  operation: string
}

export interface TargetSnapshot {
  target: string
  state: JsonObject
  state_digest: Digest
  metrics: TargetOperationMetrics[]
}

export interface SimulationSnapshot {
  seed: number
  started_at: string
  now: string
  trace: SimulationTraceEntry[]
  trace_digest: Digest
  observations: SimulatedObservation[]
  faults: FaultRuleState[]
}

export interface OperationMetricState {
  issue_requests: number
  reconciliation_requests: number
  applied_effects: number
  applied_effect_ids: EffectId[]
  stale_rejections: number
}
