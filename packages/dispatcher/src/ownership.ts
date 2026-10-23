import { activeHandoffDecision } from "@idemantis/adapter-kit"
import type {
  CancellationResult,
  EffectId,
  EffectStatus,
  EventSource,
  OwnershipHandoffBlockReason,
  ProposedEvent,
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
      this.event(lease.run_id, lease.ownership_epoch, "ownership.lease_registered", { lease }),
    )
  }

  async handoff(request: OwnershipHandoffRequest): Promise<OwnershipHandoffOutcome> {
    const ownership = this.#journal.getOwnership(request.target_lease.run_id)
    const sourceLease = ownership.active_lease?.lease
    if (!sourceLease) {
      throw new TypeError(`Run ${request.target_lease.run_id} has no active ownership lease`)
    }
    const targetLease = request.target_lease
    this.#journal.requestOwnershipHandoff(
      this.event(sourceLease.run_id, sourceLease.ownership_epoch, "ownership.handoff_requested", {
        handoff_id: request.handoff_id,
        source_lease_id: sourceLease.lease_id,
        target_lease_id: targetLease.lease_id,
        target_epoch: targetLease.ownership_epoch,
        target_owner_mode: targetLease.owner_mode,
        target_owner_id: targetLease.owner_id,
      }),
    )

    const effects = this.handoffEffects(sourceLease.run_id)
    const fences = new Map<string, { effect: HandoffEffect; binding: OwnershipHandoffBinding }>()
    for (const effect of effects.values()) {
      if (effect.status === "dispatching") {
        continue
      }
      const binding = this.#bindings.resolve(effect)
      const decision = activeHandoffDecision(binding.capabilities)
      if (decision === "block") {
        return this.block(request.handoff_id, sourceLease, "unsupported_effect")
      }
      if (decision === "settle" && !terminalEffectStatuses.has(effect.status)) {
        return this.block(request.handoff_id, sourceLease, "settle_required")
      }
      if (decision === "cancel" && !terminalEffectStatuses.has(effect.status)) {
        if (!binding.cancel) {
          return this.block(request.handoff_id, sourceLease, "cancel_required")
        }
        let result: CancellationResult
        try {
          result = await binding.cancel(effect)
        } catch {
          return this.block(request.handoff_id, sourceLease, "cancellation_failed")
        }
        if (result !== "confirmed_stopped") {
          return this.block(request.handoff_id, sourceLease, "cancellation_failed")
        }
      }
      if (decision === "transfer") {
        if (binding.capabilities.fencing_mode === "none" || !binding.activate_fence) {
          return this.block(request.handoff_id, sourceLease, "unfenced_attempt_uncertain")
        }
        fences.set(`${effect.operation}:${effect.effect_id}`, { effect, binding })
      }
    }

    const commands = new Map(
      this.#journal
        .listCommands(sourceLease.run_id)
        .map((command) => [command.command_id, command]),
    )
    for (const attempt of this.#journal.listAttempts(sourceLease.run_id)) {
      if (attempt.status !== "dispatch_started") {
        continue
      }
      const command = commands.get(attempt.command_id)
      const operation = command ? Reflect.get(command.payload, "operation") : undefined
      if (typeof operation !== "string") {
        return this.block(request.handoff_id, sourceLease, "unfenced_attempt_uncertain")
      }
      const effect = effects.get(attempt.effect_id) ?? {
        effect_id: attempt.effect_id,
        operation,
        status: "dispatching",
      }
      const binding = this.#bindings.resolve(effect)
      if (
        activeHandoffDecision(binding.capabilities) !== "transfer" ||
        binding.capabilities.fencing_mode === "none" ||
        !binding.activate_fence
      ) {
        return this.block(request.handoff_id, sourceLease, "unfenced_attempt_uncertain")
      }
      fences.set(`${operation}:${effect.effect_id}`, { effect, binding })
    }

    this.#journal.releaseOwnership(
      this.event(sourceLease.run_id, sourceLease.ownership_epoch, "ownership.released", {
        handoff_id: request.handoff_id,
        lease_id: sourceLease.lease_id,
      }),
    )

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
          proof.scope !== binding.capabilities.fencing_scope
        ) {
          return this.block(request.handoff_id, sourceLease, "fence_activation_failed")
        }
        this.#journal.recordOwnershipFence(
          this.event(sourceLease.run_id, sourceLease.ownership_epoch, "ownership.fence_activated", {
            handoff_id: request.handoff_id,
            proof,
          }),
        )
      }
    } catch {
      return this.block(request.handoff_id, sourceLease, "fence_activation_failed")
    }

    this.#journal.activateOwnership(
      this.event(
        sourceLease.run_id,
        targetLease.ownership_epoch,
        "ownership.activated",
        {
          handoff_id: request.handoff_id,
          lease: targetLease,
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
    return { status: "active", handoff: active.handoff, fence_proofs: active.fence_proofs }
  }

  private handoffEffects(runId: OwnershipHandoffRequest["target_lease"]["run_id"]) {
    return new Map<EffectId, HandoffEffect>(
      this.#journal
        .listEffects(runId)
        .filter((effect) => !terminalEffectStatuses.has(effect.status))
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
    reason: OwnershipHandoffBlockReason,
  ): OwnershipHandoffOutcome {
    this.#journal.blockOwnershipHandoff(
      this.event(sourceLease.run_id, sourceLease.ownership_epoch, "ownership.handoff_blocked", {
        handoff_id: handoffId,
        reason,
      }),
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
