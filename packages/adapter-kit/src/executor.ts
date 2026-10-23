import {
  SourceIdSchema,
  type AttemptRecord,
  type CommandRecord,
  type FenceProof,
  type IssueResult,
  type ReconciliationResult,
} from "@idemantis/contracts"
import Schema from "typebox/schema"
import { AdapterKitError } from "./errors.js"
import { AdapterRegistry } from "./manifest.js"
import type {
  AdapterExecutorOptions,
  AdapterExecutionBinding,
  AdapterInvocationContext,
  FenceActivationContext,
  IssueResultEnvelope,
  ReconciliationResultEnvelope,
  RuntimeClock,
} from "./types.js"

class SystemClock implements RuntimeClock {
  now(): Date {
    return new Date()
  }
}

function deadlineSignal(deadline: number, timeout: number, now: number): AbortSignal {
  const remaining = Math.min(deadline - now, timeout)
  if (remaining <= 0) {
    return AbortSignal.abort(new DOMException("Adapter deadline elapsed", "TimeoutError"))
  }
  return AbortSignal.timeout(Math.min(remaining, 2_147_483_647))
}

export class AdapterExecutor {
  readonly #registry: AdapterRegistry
  readonly #sourceId: AdapterExecutorOptions["source_id"]
  readonly #location: AdapterExecutorOptions["location"]
  readonly #clock: RuntimeClock

  constructor(options: AdapterExecutorOptions) {
    if (!Schema.Check(SourceIdSchema, options.source_id)) {
      throw new AdapterKitError("COMMAND_INVALID", "Adapter executor source identity is invalid")
    }
    this.#registry = new AdapterRegistry(options.registrations)
    this.#sourceId = options.source_id
    this.#location = options.location
    this.#clock = options.clock ?? new SystemClock()
  }

  async issue(command: CommandRecord, attempt: AttemptRecord): Promise<IssueResult> {
    return (await this.invokeIssue(command, attempt)).result
  }

  async reconcile(command: CommandRecord, attempt: AttemptRecord): Promise<ReconciliationResult> {
    return (await this.invokeReconciliation(command, attempt)).result
  }

  binding(operationKey: string): AdapterExecutionBinding {
    const registration = this.#registry.resolve(operationKey)
    return {
      operation_key: operationKey,
      adapter_instance_id: registration.adapter_instance_id,
      effect_class: registration.operation.descriptor.effect_class,
      execution_location: registration.operation.descriptor.capabilities.execution_location,
    }
  }

  async invokeIssue(
    command: CommandRecord,
    attempt: AttemptRecord,
  ): Promise<IssueResultEnvelope<unknown>> {
    const { input, operation: operationKey } = command.payload
    if (command.kind !== "effect.issue" || typeof operationKey !== "string") {
      throw new AdapterKitError(
        "COMMAND_INVALID",
        `Command ${command.command_id} is not an effect issue command`,
      )
    }
    const { context, operation, expired } = this.invocation(command, attempt, operationKey)
    if (expired) {
      return {
        result: "not_sent",
        evidence: { reason: "deadline_elapsed_before_adapter_invocation" },
      }
    }
    return operation.issue(context, input)
  }

  async invokeReconciliation(
    command: CommandRecord,
    attempt: AttemptRecord,
  ): Promise<ReconciliationResultEnvelope<unknown>> {
    const { operation: operationKey, subject } = command.payload
    if (command.kind !== "effect.reconcile" || typeof operationKey !== "string") {
      throw new AdapterKitError(
        "COMMAND_INVALID",
        `Command ${command.command_id} is not an effect reconciliation command`,
      )
    }
    const { context, operation, expired } = this.invocation(command, attempt, operationKey)
    if (expired) {
      return {
        result: "unknown",
        evidence: { reason: "deadline_elapsed_before_adapter_invocation" },
      }
    }
    if (!operation.reconcile) {
      throw new AdapterKitError(
        "RECONCILIATION_UNAVAILABLE",
        `Operation ${operationKey} does not support reconciliation`,
        operationKey,
      )
    }
    return operation.reconcile(context, subject)
  }

  async activateFence(
    operationKey: string,
    context: Omit<FenceActivationContext, "operation_key" | "signal">,
  ): Promise<FenceProof> {
    const { operation } = this.#registry.resolve(operationKey)
    if (!operation.activateFence) {
      throw new AdapterKitError(
        "FENCING_UNAVAILABLE",
        `Operation ${operationKey} does not support fence activation`,
        operationKey,
      )
    }
    const now = this.clockInstant()
    const requestedAt = Date.parse(context.requested_at)
    const maximumDelay = operation.descriptor.maximum_fence_activation_delay_ms
    if (!Number.isFinite(requestedAt) || maximumDelay === undefined) {
      throw new AdapterKitError(
        "COMMAND_INVALID",
        `Fence activation request for ${operationKey} is invalid`,
        operationKey,
      )
    }
    const signal = deadlineSignal(requestedAt + maximumDelay, maximumDelay, now)
    return operation.activateFence({
      ...context,
      operation_key: operationKey,
      signal,
    })
  }

  private invocation(command: CommandRecord, attempt: AttemptRecord, operationKey: string) {
    const { effect_id: commandEffectId } = command.payload
    if (
      command.command_id !== attempt.command_id ||
      command.run_id.length === 0 ||
      attempt.effect_id !== commandEffectId ||
      command.ownership_epoch !== attempt.ownership_epoch ||
      command.claimed_by !== attempt.claimed_by ||
      command.claim_deadline !== attempt.claim_deadline ||
      attempt.claimed_by !== this.#sourceId
    ) {
      throw new AdapterKitError(
        "COMMAND_INVALID",
        `Command ${command.command_id} does not match attempt ${attempt.attempt_id}`,
        operationKey,
      )
    }
    const registration = this.#registry.resolve(operationKey)
    const location = registration.operation.descriptor.capabilities.execution_location
    if (location !== "either" && location !== this.#location) {
      throw new AdapterKitError(
        "COMMAND_INVALID",
        `Operation ${operationKey} cannot execute on ${this.#location}`,
        operationKey,
      )
    }

    const now = this.clockInstant()
    const deadline = Date.parse(attempt.claim_deadline)
    if (!Number.isFinite(deadline)) {
      throw new AdapterKitError(
        "COMMAND_INVALID",
        `Attempt ${attempt.attempt_id} has an invalid deadline`,
        operationKey,
      )
    }
    const context: AdapterInvocationContext = {
      adapter_instance_id: registration.adapter_instance_id,
      run_id: command.run_id,
      command_id: command.command_id,
      attempt_id: attempt.attempt_id,
      effect_id: attempt.effect_id,
      ownership_epoch: attempt.ownership_epoch,
      deadline: attempt.claim_deadline,
      source_id: this.#sourceId,
      signal: deadlineSignal(deadline, registration.operation.descriptor.timeout.default_ms, now),
    }

    return {
      context,
      operation: registration.operation,
      expired: deadline <= now,
    }
  }

  private clockInstant(): number {
    const value = this.#clock.now().getTime()
    if (!Number.isFinite(value)) {
      throw new AdapterKitError("COMMAND_INVALID", "Adapter executor clock is invalid")
    }
    return value
  }
}
