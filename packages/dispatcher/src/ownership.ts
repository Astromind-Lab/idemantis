import { activeHandoffDecision } from "@idemantis/adapter-kit"
import {
  canonicalJson,
  type CancellationResult,
  type EffectId,
  type EffectStatus,
  type EventSource,
  type JsonObject,
  type OwnershipHandoffBlockReason,
  type ProposedEvent,
} from "@idemantis/contracts"
import type { SqliteJournal } from "@idemantis/journal-sqlite"
import { SystemClock } from "./clock.js"
import { SecureIdentityFactory } from "./identity.js"
import type {
  HandoffEffect,
  OwnershipCoordinatorOptions,
  OwnershipHandoffBinding,
  OwnershipHandoffOutcome,
  OwnershipHandoffRequest,
} from "./types.js"

const terminalEffectStatuses = new Set<EffectStatus>([
  "settled",
  "rejected",
  "failed",
  "cancelled",
  "compensated",
  "compensation_failed",
])

const externalEffectStatuses = new Set<EffectStatus>([
  "dispatching",
  "acknowledged",
  "in_progress",
  "observed",
  "uncertain",
  "fenced",
  "cancel_pending",
  "cancelling",
  "compensating",
])

export class OwnershipCoordinator {
  readonly #journal: SqliteJournal
  readonly #sourceId: OwnershipCoordinatorOptions["source_id"]
  readonly #bindings: OwnershipCoordinatorOptions["bindings"]
  readonly #clock: NonNullable<OwnershipCoordinatorOptions["clock"]>
  readonly #identities: NonNullable<OwnershipCoordinatorOptions["identities"]>

  constructor(options: OwnershipCoordinatorOptions) {
    this.#journal = options.journal
    this.#sourceId = options.source_id
    this.#bindings = options.bindings
    this.#clock = options.clock ?? new SystemClock()
    this.#identities = options.identities ?? new SecureIdentityFactory()
  }

  registerLease(lease: OwnershipHandoffRequest["target_lease"]): void {
    this.#journal.registerOwnershipLease(
      this.event(
        lease.run_id,
        lease.ownership_epoch,
        "ownership.lease_registered",
        { lease },
        { kind: lease.owner_mode, source_id: this.#sourceId },
      ),
    )
  }

  async handoff(request: OwnershipHandoffRequest): Promise<OwnershipHandoffOutcome> {
    const ownership = this.#journal.getOwnership(request.target_lease.run_id)
    const existing = ownership.handoff?.status === "active" ? undefined : ownership.handoff
    const sourceLeaseRecord = existing
      ? ownership.leases.find((entry) => entry.lease.lease_id === existing.source_lease_id)
      : ownership.active_lease
    if (!sourceLeaseRecord) {
      throw new TypeError(`Run ${request.target_lease.run_id} has no active ownership lease`)
    }
    const sourceLease = sourceLeaseRecord.lease
    const targetLease = request.target_lease
    const source = { kind: sourceLease.owner_mode, source_id: this.#sourceId } as const
    if (sourceLease.owner_id !== this.#sourceId) {
      throw new TypeError(
        `Coordinator ${this.#sourceId} does not own lease ${sourceLease.lease_id}`,
      )
    }
    if (existing) {
      if (
        existing.handoff_id !== request.handoff_id ||
        existing.target_lease_id !== targetLease.lease_id ||
        canonicalJson(existing.target_lease as unknown as JsonObject) !==
          canonicalJson(targetLease as unknown as JsonObject) ||
        existing.target_epoch !== targetLease.ownership_epoch ||
        existing.target_owner_mode !== targetLease.owner_mode ||
        existing.target_owner_id !== targetLease.owner_id
      ) {
        throw new TypeError(`Run ${sourceLease.run_id} has a different handoff in progress`)
      }
    } else {
      this.#journal.requestOwnershipHandoff(
        this.event(
          sourceLease.run_id,
          sourceLease.ownership_epoch,
          "ownership.handoff_requested",
          {
            handoff_id: request.handoff_id,
            source_lease_id: sourceLease.lease_id,
            target_lease: targetLease,
          },
          source,
        ),
      )
    }

    const effects = this.handoffEffects(sourceLease.run_id)
    const cancelledEffects = new Set(
      this.#journal
        .listEffects(sourceLease.run_id)
        .filter((effect) => effect.status === "cancelled")
        .map((effect) => effect.effect_id),
    )
    const commands = new Map(
      this.#journal
        .listCommands(sourceLease.run_id)
        .map((command) => [command.command_id, command]),
    )
    const openAttempts = this.#journal
      .listAttempts(sourceLease.run_id)
      .filter(
        (attempt) =>
          attempt.status === "dispatch_started" &&
          attempt.ownership_epoch === sourceLease.ownership_epoch,
      )
    const inFlightEffects = new Set(openAttempts.map((attempt) => attempt.effect_id))
    const recordedFences = new Set(existing ? ownership.fences.map((entry) => entry.effect_id) : [])
    const fences = new Map<string, { effect: HandoffEffect; binding: OwnershipHandoffBinding }>()
    for (const effect of effects.values()) {
      const binding = this.#bindings.resolve(effect)
      const decision = activeHandoffDecision(binding.capabilities)
      if (decision === "block") {
        return this.block(request.handoff_id, sourceLease, source, "unsupported_effect")
      }
      if (decision === "settle" && !terminalEffectStatuses.has(effect.status)) {
        return this.block(request.handoff_id, sourceLease, source, "settle_required")
      }
      if (decision === "cancel" && !terminalEffectStatuses.has(effect.status)) {
        if (!binding.cancel) {
          return this.block(request.handoff_id, sourceLease, source, "cancel_required")
        }
        let result: CancellationResult
        try {
          result = await binding.cancel(effect)
        } catch {
          return this.block(request.handoff_id, sourceLease, source, "cancellation_failed")
        }
        if (result !== "confirmed_stopped") {
          return this.block(request.handoff_id, sourceLease, source, "cancellation_failed")
        }
        this.#journal.recordOwnershipCancellation(
          this.event(
            sourceLease.run_id,
            sourceLease.ownership_epoch,
            "ownership.effect_cancelled",
            {
              handoff_id: request.handoff_id,
              effect_id: effect.effect_id,
              result,
            },
            source,
          ),
        )
        cancelledEffects.add(effect.effect_id)
      }
      if (decision === "transfer") {
        if (
          binding.capabilities.fencing_mode === "none" ||
          !binding.adapter_instance_id ||
          !binding.fence_scope_reference ||
          !binding.activate_fence
        ) {
          return this.block(request.handoff_id, sourceLease, source, "unfenced_attempt_uncertain")
        }
        if (!inFlightEffects.has(effect.effect_id) && !recordedFences.has(effect.effect_id)) {
          fences.set(effect.effect_id, { effect, binding })
        }
      }
    }

    for (const attempt of openAttempts) {
      if (cancelledEffects.has(attempt.effect_id)) {
        continue
      }
      const command = commands.get(attempt.command_id)
      const operation = command ? Reflect.get(command.payload, "operation") : undefined
      if (typeof operation !== "string") {
        return this.block(request.handoff_id, sourceLease, source, "unfenced_attempt_uncertain")
      }
      const projectedEffect = effects.get(attempt.effect_id)
      const effect = projectedEffect ?? {
        effect_id: attempt.effect_id,
        operation,
        status: "dispatching",
      }
      const binding = this.#bindings.resolve(effect)
      if (
        activeHandoffDecision(binding.capabilities) !== "transfer" ||
        binding.capabilities.fencing_mode === "none" ||
        !binding.adapter_instance_id ||
        !binding.fence_scope_reference ||
        !binding.activate_fence
      ) {
        return this.block(request.handoff_id, sourceLease, source, "unfenced_attempt_uncertain")
      }
      if (!recordedFences.has(effect.effect_id)) {
        fences.set(effect.effect_id, { effect, binding })
      }
    }

    if (sourceLeaseRecord.status === "active") {
      this.#journal.releaseOwnership(
        this.event(
          sourceLease.run_id,
          sourceLease.ownership_epoch,
          "ownership.released",
          {
            handoff_id: request.handoff_id,
            lease_id: sourceLease.lease_id,
          },
          source,
        ),
      )
    }

    try {
      for (const { effect, binding } of fences.values()) {
        const fenceRequest = {
          run_id: sourceLease.run_id,
          effect_id: effect.effect_id,
          ownership_epoch: targetLease.ownership_epoch,
          requested_at: this.#clock.now().toISOString(),
        }
        const proof = await binding.activate_fence?.(fenceRequest)
        if (
          !proof ||
          proof.ownership_epoch !== fenceRequest.ownership_epoch ||
          proof.adapter_instance_id !== binding.adapter_instance_id ||
          proof.scope !== binding.capabilities.fencing_scope ||
          proof.scope_reference !== binding.fence_scope_reference ||
          Date.parse(proof.effective_at) < Date.parse(fenceRequest.requested_at) ||
          Date.parse(proof.effective_at) > this.#clock.now().getTime()
        ) {
          return this.block(request.handoff_id, sourceLease, source, "fence_activation_failed")
        }
        this.#journal.recordOwnershipFence(
          this.event(
            sourceLease.run_id,
            sourceLease.ownership_epoch,
            "ownership.fence_activated",
            {
              handoff_id: request.handoff_id,
              effect_id: effect.effect_id,
              proof,
            },
            source,
          ),
        )
      }
    } catch {
      return this.block(request.handoff_id, sourceLease, source, "fence_activation_failed")
    }

    const resumeCommands = this.#journal
      .listCommands(sourceLease.run_id)
      .filter(
        (command) =>
          command.status === "pending" &&
          command.ownership_epoch === sourceLease.ownership_epoch &&
          (command.kind === "effect.issue" || command.kind === "effect.reconcile"),
      )
      .map((command) => ({
        previous_command_id: command.command_id,
        kind: command.kind as "effect.issue" | "effect.reconcile",
        payload: command.payload,
      }))

    this.#journal.activateOwnership(
      this.event(
        sourceLease.run_id,
        targetLease.ownership_epoch,
        "ownership.activated",
        {
          handoff_id: request.handoff_id,
          lease: targetLease,
          resume_commands: resumeCommands,
        },
        {
          kind: targetLease.owner_mode,
          source_id: targetLease.owner_id,
        },
      ),
    )
    const active = this.#journal.getOwnership(sourceLease.run_id)
    if (!active.handoff) {
      throw new TypeError(`Handoff ${request.handoff_id} was not persisted`)
    }
    return {
      status: "active",
      handoff: active.handoff,
      fence_proofs: active.fences.map((entry) => entry.proof),
    }
  }

  private handoffEffects(runId: OwnershipHandoffRequest["target_lease"]["run_id"]) {
    return new Map<EffectId, HandoffEffect>(
      this.#journal
        .listEffects(runId)
        .filter((effect) => externalEffectStatuses.has(effect.status))
        .map((effect) => [
          effect.effect_id,
          {
            effect_id: effect.effect_id,
            operation: effect.operation,
            status: effect.status,
          },
        ]),
    )
  }

  private block(
    handoffId: OwnershipHandoffRequest["handoff_id"],
    sourceLease: OwnershipHandoffRequest["target_lease"],
    source: EventSource,
    reason: OwnershipHandoffBlockReason,
  ): OwnershipHandoffOutcome {
    this.#journal.blockOwnershipHandoff(
      this.event(
        sourceLease.run_id,
        sourceLease.ownership_epoch,
        "ownership.handoff_blocked",
        {
          handoff_id: handoffId,
          reason,
        },
        source,
      ),
    )
    const state = this.#journal.getOwnership(sourceLease.run_id)
    if (!state.handoff) {
      throw new TypeError(`Handoff ${handoffId} was not persisted`)
    }
    return { status: "blocked", handoff: state.handoff, reason }
  }

  private event(
    runId: OwnershipHandoffRequest["target_lease"]["run_id"],
    ownershipEpoch: number,
    type: string,
    payload: ProposedEvent["payload"],
    source: EventSource = { kind: "runner", source_id: this.#sourceId },
  ): ProposedEvent {
    return {
      event_id: this.#identities.eventId(),
      run_id: runId,
      type,
      schema_version: 1,
      source,
      ownership_epoch: ownershipEpoch,
      payload,
    }
  }
}
