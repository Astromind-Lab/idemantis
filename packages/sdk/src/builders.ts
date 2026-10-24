import type { JsonValue } from "@idemantis/contracts"
import type {
  EffectStateDefinition,
  FinalOutcome,
  FinalStateDefinition,
  ObservationWaitStateDefinition,
  OperatorWaitStateDefinition,
  OperationDefinition,
  ReconcileStateDefinition,
  RetryPolicy,
  ValueExpression,
} from "./types.js"

/** Declares an adapter operation that a mission may require. */
export function defineOperation(definition: OperationDefinition): OperationDefinition {
  return definition
}

/** Selects a value from the mission input without executing user code. */
export function fromInput(...path: string[]): ValueExpression {
  return { kind: "input", path }
}

/** Embeds a JSON literal in a serializable mission expression. */
export function literal(value: JsonValue): ValueExpression {
  return { kind: "literal", value }
}

/** Builds an object from serializable value expressions. */
export function mapFields(fields: Record<string, ValueExpression>): ValueExpression {
  return { kind: "object", fields }
}

/** Disables automatic retries for an effect. */
export function noRetry(): RetryPolicy {
  return { kind: "none" }
}

/** Retries only through target-enforced duplicate suppression. */
export function safeRetry(options: { attempts: number; backoffMs?: number }): RetryPolicy {
  return {
    kind: "safe",
    maxAttempts: options.attempts,
    backoffMs: options.backoffMs ?? 0,
  }
}

/** Reconciles an ambiguous attempt before another request is issued. */
export function reconcileBeforeRetry(options: {
  attempts: number
  backoffMs?: number
}): RetryPolicy {
  return {
    kind: "reconcile_before_retry",
    maxAttempts: options.attempts,
    backoffMs: options.backoffMs ?? 0,
  }
}

type EffectStateOptions = Omit<
  EffectStateDefinition,
  "kind" | "required" | "execution" | "automaticTakeover" | "activeHandoff"
> &
  Partial<
    Pick<EffectStateDefinition, "required" | "execution" | "automaticTakeover" | "activeHandoff">
  >

/** Declares one external effect and every outcome route it may take. */
export function effectState(options: EffectStateOptions): EffectStateDefinition {
  return {
    ...options,
    kind: "effect",
    required: options.required ?? true,
    execution: options.execution ?? "runner",
    automaticTakeover: options.automaticTakeover ?? false,
    activeHandoff: options.activeHandoff ?? false,
  }
}

/** Waits for correlated evidence with an explicit timeout route. */
export function waitForObservation(
  options: Omit<ObservationWaitStateDefinition, "kind">,
): ObservationWaitStateDefinition {
  return { ...options, kind: "observation_wait" }
}

/** Resolves an uncertain effect through an adapter reconciliation operation. */
export function reconcileState(
  options: Omit<ReconcileStateDefinition, "kind" | "execution"> &
    Partial<Pick<ReconcileStateDefinition, "execution">>,
): ReconcileStateDefinition {
  return { ...options, kind: "reconcile", execution: options.execution ?? "runner" }
}

export function waitForOperator(
  options: Omit<OperatorWaitStateDefinition, "kind">,
): OperatorWaitStateDefinition {
  return { ...options, kind: "operator_wait" }
}

/** Declares an immutable terminal mission outcome. */
export function finalState(outcome: FinalOutcome): FinalStateDefinition {
  return { kind: "final", outcome }
}
