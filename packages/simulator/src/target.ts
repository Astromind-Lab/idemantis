import { sha256Digest, type EffectId, type JsonObject } from "@idemantis/contracts"
import { SimulationError } from "./errors.js"
import type { SimulationKernel } from "./kernel.js"
import type { OperationMetricState, TargetOperationMetrics, TargetSnapshot } from "./types.js"

function emptyMetrics(): OperationMetricState {
  return {
    issue_requests: 0,
    reconciliation_requests: 0,
    applied_effects: 0,
    applied_effect_ids: [],
    stale_rejections: 0,
  }
}

export class SimulatedTargetProbe {
  readonly #metrics = new Map<string, OperationMetricState>()

  constructor(
    readonly kernel: SimulationKernel,
    readonly target: string,
  ) {}

  issue(operation: string, effectId: EffectId): void {
    this.metric(operation).issue_requests += 1
    this.kernel.record({
      target: this.target,
      operation,
      stage: "request.received",
      correlation_id: effectId,
    })
  }

  reconcile(operation: string, effectId: EffectId): void {
    this.metric(operation).reconciliation_requests += 1
    this.kernel.record({
      target: this.target,
      operation,
      stage: "reconciliation.received",
      correlation_id: effectId,
    })
  }

  apply(operation: string, effectId: EffectId, payload: JsonObject = {}): void {
    const metric = this.metric(operation)
    metric.applied_effects += 1
    metric.applied_effect_ids.push(effectId)
    this.kernel.record({
      target: this.target,
      operation,
      stage: "effect.applied",
      correlation_id: effectId,
      payload,
    })
  }

  rejectStale(operation: string, effectId: EffectId, epoch: number): void {
    this.metric(operation).stale_rejections += 1
    this.kernel.record({
      target: this.target,
      operation,
      stage: "effect.rejected_stale",
      correlation_id: effectId,
      payload: { ownership_epoch: epoch },
    })
  }

  metrics(operation: string): TargetOperationMetrics {
    return { operation, ...structuredClone(this.metric(operation)) }
  }

  snapshot(state: JsonObject): TargetSnapshot {
    const metrics = [...this.#metrics.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([operation, value]) => ({ operation, ...structuredClone(value) }))
    const clonedState = structuredClone(state)
    return {
      target: this.target,
      state: clonedState,
      state_digest: sha256Digest(clonedState),
      metrics,
    }
  }

  private metric(operation: string): OperationMetricState {
    if (operation.length === 0) {
      throw new SimulationError("TARGET_STATE_INVALID", "Operation name cannot be empty")
    }
    let value = this.#metrics.get(operation)
    if (!value) {
      value = emptyMetrics()
      this.#metrics.set(operation, value)
    }
    return value
  }
}
