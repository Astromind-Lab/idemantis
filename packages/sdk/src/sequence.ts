import type { TSchema } from "typebox"
import { effectState, finalState, noRetry, reconcileState, safeRetry } from "./builders.js"
import { defineMission } from "./compiler.js"
import type { OperationDefinition, StateDefinition, ValueExpression } from "./types.js"

export interface SequentialStep {
  name: string
  operation: OperationDefinition
  input: ValueExpression
}

export interface SequentialMissionOptions {
  id: string
  version: string
  input: TSchema
  steps: readonly SequentialStep[]
  attempts?: number
  backoff_ms?: number
}

const reservedStates = new Set(["done", "failed", "unresolved"])

function retryable(operation: OperationDefinition): boolean {
  const safety = operation.capabilities.duplicate_safety
  return safety === "native_idempotency" || safety === "keyed_uniqueness"
}

function reconcilable(operation: OperationDefinition): boolean {
  return operation.capabilities.reconciliation !== "none"
}

export function sequentialMission(options: SequentialMissionOptions) {
  if (options.steps.length === 0) {
    throw new TypeError("A mission plan needs at least one step")
  }
  const names = new Set<string>()
  const states: Record<string, StateDefinition> = {}
  options.steps.forEach((step, index) => {
    if (reservedStates.has(step.name) || names.has(step.name) || step.name.endsWith("_reconcile")) {
      throw new TypeError(`Step name ${step.name} is reserved or repeated`)
    }
    names.add(step.name)
    const next = options.steps[index + 1]?.name ?? "done"
    const reconcile = `${step.name}_reconcile`
    const canReconcile = reconcilable(step.operation)
    states[step.name] = effectState({
      operation: step.operation,
      effectSlot: step.name,
      input: step.input,
      retry: retryable(step.operation)
        ? safeRetry({ attempts: options.attempts ?? 3, backoffMs: options.backoff_ms ?? 1_000 })
        : noRetry(),
      onSettled: next,
      onFailed: "failed",
      onUncertain: canReconcile ? reconcile : "unresolved",
    })
    if (canReconcile) {
      states[reconcile] = reconcileState({
        effectState: step.name,
        operation: step.operation,
        subject: { kind: "literal", value: step.name },
        onConfirmedCompleted: next,
        onConfirmedNotApplied: step.name,
        onConfirmedInProgress: "unresolved",
        onConflictingState: "unresolved",
        onUnknown: "unresolved",
      })
    }
  })
  return defineMission({
    id: options.id,
    version: options.version,
    input: options.input,
    initial: options.steps[0]?.name as string,
    states: {
      ...states,
      done: finalState("completed"),
      failed: finalState("failed"),
      unresolved: finalState("unresolved"),
    },
  })
}
