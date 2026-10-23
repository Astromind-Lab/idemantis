import {
  EdgeDelegationSchema,
  EdgeExecutionScopeSchema,
  EdgeReconnectUploadSchema,
  AttemptIdSchema,
  AttemptRecordSchema,
  CommandIdSchema,
  CommandRecordSchema,
  EffectIdSchema,
  IssueResultSchema,
  OperationNameSchema,
  ProposedEventSchema,
  SourceIdSchema,
  TimestampSchema,
  canonicalJson,
  sha256Digest,
  type EdgeActivationRecord,
  type EdgeDelegation,
  type EdgeExecutionScope,
  type EdgeReconnectUpload,
  type EdgeStopReason,
  type JsonObject,
  type JsonValue,
  type ProposedEvent,
} from "@idemantis/contracts"
import Schema from "typebox/schema"
import { EdgeAgentError } from "./errors.js"
import { edgeDelegationDigest } from "./digest.js"
import type {
  EdgeActivationOptions,
  EdgeAgentOptions,
  EdgeAgentState,
  EdgeAppendOptions,
  EdgeAppendResult,
  EdgeDispatchOutcome,
  EdgeDispatchRequest,
  StoredEdgeSession,
} from "./types.js"

function instant(value: string, label: string): number {
  if (!Schema.Check(TimestampSchema, value)) {
    throw new EdgeAgentError("EDGE_DELEGATION_INVALID", `${label} must be a UTC timestamp`)
  }
  const parsed = Date.parse(value)
  if (!Number.isFinite(parsed)) {
    throw new EdgeAgentError("EDGE_DELEGATION_INVALID", `${label} must be a valid timestamp`)
  }
  return parsed
}

function safeMonotonic(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new EdgeAgentError(
      "EDGE_DELEGATION_INVALID",
      "Monotonic time must be a non-negative safe integer",
    )
  }
  return value
}

export class EdgeAgent {
  readonly #edgeId: EdgeAgentOptions["edge_id"]
  readonly #bootIdentity: EdgeAgentOptions["boot_identity"]
  readonly #clock: EdgeAgentOptions["clock"]
  readonly #store: EdgeAgentOptions["store"]

  constructor(options: EdgeAgentOptions) {
    if (
      !Schema.Check(SourceIdSchema, options.edge_id) ||
      !Schema.Check(SourceIdSchema, options.boot_identity)
    ) {
      throw new EdgeAgentError("EDGE_DELEGATION_INVALID", "Edge identity is invalid")
    }
    this.#edgeId = options.edge_id
    this.#bootIdentity = options.boot_identity
    this.#clock = options.clock
    this.#store = options.store
    const state = this.#store.state()
    if (state && !state.stopped_reason) {
      const reason = this.continuityFailure(state) ?? this.clockReading(state).stopReason
      if (reason) {
        this.#store.stop(reason, this.factClock(state).recordedAt)
      }
    }
  }

  activate(delegationValue: EdgeDelegation, options: EdgeActivationOptions): EdgeActivationRecord {
    if (!Schema.Check(EdgeDelegationSchema, delegationValue)) {
      throw new EdgeAgentError(
        "EDGE_DELEGATION_INVALID",
        "Delegation does not satisfy its contract",
      )
    }
    const existing = this.#store.state()
    if (existing) {
      if (
        canonicalJson(existing.delegation as unknown as JsonObject) !==
        canonicalJson(delegationValue as unknown as JsonObject)
      ) {
        throw new EdgeAgentError(
          "EDGE_ALREADY_ACTIVATED",
          "Edge store already contains another delegation",
        )
      }
      this.ensureBootContinuity(existing)
      return structuredClone(existing.activation)
    }
    const lease = delegationValue.lease
    if (edgeDelegationDigest(delegationValue) !== lease.delegation_digest) {
      throw new EdgeAgentError(
        "EDGE_DELEGATION_INVALID",
        "Delegation digest does not match its bounded content",
      )
    }
    if (lease.owner_mode !== "edge" || lease.owner_id !== this.#edgeId) {
      throw new EdgeAgentError(
        "EDGE_DELEGATION_INVALID",
        `Delegation does not authorize edge ${this.#edgeId}`,
      )
    }
    const startsAt = instant(lease.not_before, "Lease not_before")
    const endsAt = instant(lease.not_after, "Lease not_after")
    if (startsAt >= endsAt) {
      throw new EdgeAgentError("EDGE_DELEGATION_INVALID", "Lease window is empty")
    }
    if (
      !Number.isSafeInteger(options.measured_clock_uncertainty_ms) ||
      options.measured_clock_uncertainty_ms < 0 ||
      options.measured_clock_uncertainty_ms > lease.allowed_clock_uncertainty_ms
    ) {
      throw new EdgeAgentError(
        "EDGE_DELEGATION_INVALID",
        "Measured clock uncertainty exceeds the delegation bound",
      )
    }
    const wallInstant = this.#clock.wallNow().getTime()
    if (!Number.isFinite(wallInstant)) {
      throw new EdgeAgentError("EDGE_DELEGATION_INVALID", "Activation wall time is invalid")
    }
    const wallAt = new Date(wallInstant).toISOString()
    if (wallInstant >= endsAt - lease.allowed_clock_uncertainty_ms) {
      throw new EdgeAgentError("EDGE_STOPPED", "Delegation is already expired", "lease_expired")
    }
    const activation: EdgeActivationRecord = {
      lease_id: lease.lease_id,
      edge_id: this.#edgeId,
      boot_identity: this.#bootIdentity,
      wall_at: wallAt,
      monotonic_at_ms: safeMonotonic(this.#clock.monotonicNowMs()),
      measured_clock_uncertainty_ms: options.measured_clock_uncertainty_ms,
      delegation_digest: lease.delegation_digest,
    }
    this.#store.activate(structuredClone(delegationValue), activation)
    return structuredClone(activation)
  }

  append(proposal: ProposedEvent, options: EdgeAppendOptions = {}): EdgeAppendResult {
    if (!Schema.Check(ProposedEventSchema, proposal)) {
      this.stopForViolation("Event does not satisfy its contract")
    }
    const state = this.requiredState()
    const recordedAt = this.effectiveNow(state).toISOString()
    this.assertAuthority(state, proposal)
    const execution = options.execution
    if (proposal.type === "effect.dispatch_started") {
      if (!execution || !Schema.Check(EdgeExecutionScopeSchema, execution)) {
        this.stopForViolation("Dispatch start requires a valid execution scope", recordedAt)
      }
      const attemptId = Reflect.get(proposal.payload, "attempt_id")
      const commandId = Reflect.get(proposal.payload, "command_id")
      const effectId = Reflect.get(proposal.payload, "effect_id")
      if (
        !Schema.Check(AttemptIdSchema, attemptId) ||
        !Schema.Check(CommandIdSchema, commandId) ||
        !Schema.Check(EffectIdSchema, effectId)
      ) {
        this.stopForViolation("Dispatch start does not identify its durable work", recordedAt)
      }
      this.assertExecutionScope(state.delegation, execution, recordedAt)
    } else if (execution) {
      this.stopForViolation("Execution scope is valid only for dispatch start", recordedAt)
    }
    return this.#store.append({
      proposal: structuredClone(proposal),
      recorded_at: recordedAt,
      ...(execution ? { execution: structuredClone(execution) } : {}),
    })
  }

  async dispatch(request: EdgeDispatchRequest): Promise<EdgeDispatchOutcome> {
    const state = this.requiredState()
    const { command, attempt } = request
    if (
      !Schema.Check(CommandRecordSchema, command) ||
      !Schema.Check(AttemptRecordSchema, attempt) ||
      command.kind !== "effect.issue" ||
      command.status !== "claimed" ||
      attempt.status !== "claimed"
    ) {
      this.stopForViolation("Dispatch requires a valid claimed effect command")
    }
    const operation = Reflect.get(command.payload, "operation")
    const effectId = Reflect.get(command.payload, "effect_id")
    const input = Reflect.get(command.payload, "input") as JsonValue | undefined
    if (
      !Schema.Check(OperationNameSchema, operation) ||
      !Schema.Check(EffectIdSchema, effectId) ||
      input === undefined ||
      command.run_id !== state.delegation.lease.run_id ||
      command.ownership_epoch !== state.delegation.lease.ownership_epoch ||
      command.claimed_by !== this.#edgeId ||
      command.claim_deadline === undefined ||
      attempt.command_id !== command.command_id ||
      attempt.effect_id !== effectId ||
      attempt.ownership_epoch !== command.ownership_epoch ||
      attempt.claimed_by !== this.#edgeId ||
      attempt.claim_deadline !== command.claim_deadline
    ) {
      this.stopForViolation("Dispatch command exceeds the delegated claim")
    }
    let binding: ReturnType<EdgeDispatchRequest["executor"]["binding"]>
    try {
      binding = request.executor.binding(operation)
    } catch {
      throw new EdgeAgentError(
        "EDGE_DISPATCH_INVALID",
        `No edge adapter is registered for ${operation}`,
      )
    }
    const execution: EdgeExecutionScope = {
      state: request.state,
      effect_class: binding.effect_class,
      operation,
      adapter_instance_id: binding.adapter_instance_id,
      input_digest: sha256Digest(input),
    }
    if (
      binding.operation_key !== operation ||
      (binding.execution_location !== "edge" && binding.execution_location !== "either")
    ) {
      this.stopForViolation("Adapter binding is outside the delegated edge execution scope")
    }
    this.assertExecutionScope(state.delegation, execution)
    if (
      !state.delegation.allowed_event_types.includes("effect.dispatch_started") ||
      !state.delegation.allowed_event_types.includes("effect.issue_result_recorded")
    ) {
      this.stopForViolation("Delegation cannot record the complete dispatch lifecycle")
    }
    const dispatchStarted = this.append(
      {
        event_id: request.dispatch_started_event_id,
        run_id: command.run_id,
        type: "effect.dispatch_started",
        schema_version: 1,
        source: { kind: "edge", source_id: this.#edgeId },
        causation_id: command.causing_event_id,
        ownership_epoch: command.ownership_epoch,
        payload: {
          attempt_id: attempt.attempt_id,
          command_id: command.command_id,
          effect_id: attempt.effect_id,
        },
      },
      { execution },
    )
    if (dispatchStarted.duplicate) {
      throw new EdgeAgentError(
        "EDGE_DISPATCH_ALREADY_STARTED",
        `Attempt ${attempt.attempt_id} was already persisted and will not be reissued`,
      )
    }
    let result: EdgeDispatchOutcome["result"]
    try {
      const candidate = await request.executor.issue(
        structuredClone(command),
        structuredClone(attempt),
      )
      result = Schema.Check(IssueResultSchema, candidate) ? candidate : "uncertain"
    } catch {
      result = "uncertain"
    }
    const current = this.#store.state()
    if (!current) {
      throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Edge session disappeared during dispatch")
    }
    const clock = this.factClock(current)
    const resultProposal: ProposedEvent = {
      event_id: request.result_event_id,
      run_id: command.run_id,
      type: "effect.issue_result_recorded",
      schema_version: 1,
      source: { kind: "edge", source_id: this.#edgeId },
      causation_id: request.dispatch_started_event_id,
      ownership_epoch: command.ownership_epoch,
      payload: {
        attempt_id: attempt.attempt_id,
        command_id: command.command_id,
        effect_id: attempt.effect_id,
        result,
      },
    }
    this.assertAuthority(current, resultProposal)
    const resultRecorded = this.#store.append({
      proposal: resultProposal,
      recorded_at: clock.recordedAt,
    })
    if (clock.stopReason) {
      this.#store.stop(clock.stopReason, clock.recordedAt)
    }
    return { dispatch_started: dispatchStarted, result_recorded: resultRecorded, result }
  }

  state(): EdgeAgentState | undefined {
    const state = this.#store.state()
    if (!state) {
      return undefined
    }
    if (!state.stopped_reason) {
      const reason = this.continuityFailure(state) ?? this.clockReading(state).stopReason
      if (reason) {
        this.#store.stop(reason, this.factClock(state).recordedAt)
      }
    }
    const current = this.#store.state()
    if (!current) {
      return undefined
    }
    return structuredClone({
      delegation: current.delegation,
      activation: current.activation,
      ...(current.stopped_reason ? { stopped_reason: current.stopped_reason } : {}),
      last_sequence: current.last_sequence,
      dispatch_count: current.dispatch_count,
    })
  }

  reconnectUpload(): EdgeReconnectUpload {
    const state = this.state()
    const createdAt = state ? this.factClock(state).recordedAt : this.#clock.wallNow().toISOString()
    const upload = this.#store.upload(createdAt)
    if (!Schema.Check(EdgeReconnectUploadSchema, upload)) {
      throw new EdgeAgentError("EDGE_STORE_CORRUPT", "Reconnect upload is invalid")
    }
    return structuredClone(upload)
  }

  private requiredState(): StoredEdgeSession {
    const state = this.#store.state()
    if (!state) {
      throw new EdgeAgentError("EDGE_NOT_ACTIVATED", "Edge agent has no delegation")
    }
    if (state.stopped_reason) {
      throw new EdgeAgentError(
        "EDGE_STOPPED",
        `Edge execution stopped: ${state.stopped_reason}`,
        state.stopped_reason,
      )
    }
    this.ensureBootContinuity(state)
    const current = this.#store.state()
    if (!current || current.stopped_reason) {
      const reason = current?.stopped_reason ?? "unverified_reboot"
      throw new EdgeAgentError("EDGE_STOPPED", `Edge execution stopped: ${reason}`, reason)
    }
    return current
  }

  private ensureBootContinuity(state: StoredEdgeSession): void {
    const reason = this.continuityFailure(state)
    if (reason) {
      this.stop(reason, state.activation.wall_at)
    }
  }

  private continuityFailure(state: EdgeAgentState): EdgeStopReason | undefined {
    if (
      state.activation.edge_id !== this.#edgeId ||
      state.delegation.lease.owner_id !== this.#edgeId
    ) {
      return "edge_identity_changed"
    }
    if (state.activation.boot_identity !== this.#bootIdentity) {
      return "unverified_reboot"
    }
    const monotonicNow = this.#clock.monotonicNowMs()
    if (!Number.isSafeInteger(monotonicNow) || monotonicNow < state.activation.monotonic_at_ms) {
      return "monotonic_discontinuity"
    }
    return undefined
  }

  private effectiveNow(state: StoredEdgeSession): Date {
    const reading = this.clockReading(state)
    if (reading.stopReason) {
      this.stop(reading.stopReason, new Date(reading.effectiveWall).toISOString())
    }
    const effectiveWall = reading.effectiveWall
    const uncertainty = state.delegation.lease.allowed_clock_uncertainty_ms
    const earliest = instant(state.delegation.lease.not_before, "Lease not_before") + uncertainty
    if (effectiveWall < earliest) {
      throw new EdgeAgentError("EDGE_NOT_YET_VALID", "Delegation is not valid yet")
    }
    return new Date(effectiveWall)
  }

  private factClock(state: EdgeAgentState): {
    recordedAt: string
    stopReason?: EdgeStopReason
  } {
    const reading = this.clockReading(state)
    return {
      recordedAt: new Date(reading.effectiveWall).toISOString(),
      ...(reading.stopReason ? { stopReason: reading.stopReason } : {}),
    }
  }

  private clockReading(state: EdgeAgentState): {
    effectiveWall: number
    stopReason?: EdgeStopReason
  } {
    const activationWall = instant(state.activation.wall_at, "Stored activation wall time")
    const monotonicNow = this.#clock.monotonicNowMs()
    if (!Number.isSafeInteger(monotonicNow) || monotonicNow < state.activation.monotonic_at_ms) {
      return { effectiveWall: activationWall, stopReason: "monotonic_discontinuity" }
    }
    const elapsed = monotonicNow - state.activation.monotonic_at_ms
    const effectiveWall = activationWall + elapsed
    const actualWall = this.#clock.wallNow().getTime()
    const uncertainty = state.delegation.lease.allowed_clock_uncertainty_ms
    if (!Number.isFinite(actualWall) || Math.abs(actualWall - effectiveWall) > uncertainty) {
      return { effectiveWall, stopReason: "wall_clock_discontinuity" }
    }
    const leaseUpperElapsed =
      instant(state.delegation.lease.not_after, "Lease not_after") - uncertainty - activationWall
    const offlineUpperElapsed = state.delegation.lease.maximum_offline_duration_ms
    if (elapsed >= Math.min(leaseUpperElapsed, offlineUpperElapsed)) {
      return {
        effectiveWall,
        stopReason:
          offlineUpperElapsed <= leaseUpperElapsed ? "offline_duration_exceeded" : "lease_expired",
      }
    }
    return { effectiveWall }
  }

  private assertAuthority(state: StoredEdgeSession, proposal: ProposedEvent): void {
    const lease = state.delegation.lease
    if (
      proposal.run_id !== lease.run_id ||
      proposal.source.kind !== "edge" ||
      proposal.source.source_id !== this.#edgeId ||
      proposal.ownership_epoch !== lease.ownership_epoch ||
      !state.delegation.allowed_event_types.includes(proposal.type)
    ) {
      this.stopForViolation("Event exceeds the delegated authority")
    }
  }

  private assertExecutionScope(
    delegationValue: EdgeDelegation,
    execution: EdgeExecutionScope,
    recordedAt?: string,
  ): void {
    const actionAllowed = delegationValue.allowed_actions.some(
      (allowed) =>
        allowed.state === execution.state &&
        allowed.effect_class === execution.effect_class &&
        allowed.operation === execution.operation &&
        allowed.adapter_instance_id === execution.adapter_instance_id &&
        allowed.input_digests.includes(execution.input_digest),
    )
    if (!actionAllowed) {
      this.stopForViolation("Dispatch exceeds the delegated execution scope", recordedAt)
    }
  }

  private stopForViolation(message: string, at?: string): never {
    const state = this.#store.state()
    if (state) {
      this.#store.stop("scope_violation", at ?? this.factClock(state).recordedAt)
    }
    throw new EdgeAgentError("EDGE_STOPPED", message, "scope_violation")
  }

  private stop(reason: EdgeStopReason, at: string): never {
    this.#store.stop(reason, at)
    throw new EdgeAgentError("EDGE_STOPPED", `Edge execution stopped: ${reason}`, reason)
  }
}
