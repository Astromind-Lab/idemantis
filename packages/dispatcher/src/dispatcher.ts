import type {
  EventId,
  IssueResult,
  ProposedEvent,
  ReconciliationResult,
  RunId,
} from "@idemantis/contracts"
import type { ClaimedCommand, SqliteJournal } from "@idemantis/journal-sqlite"
import { SecureIdentityFactory } from "./identity.js"
import { SystemClock } from "./clock.js"
import type {
  Clock,
  CommandDispatcherOptions,
  DispatchOutcome,
  EffectExecutor,
  IdentityFactory,
  TimerDispatcherOptions,
  TimerDispatchOutcome,
} from "./types.js"

export class CommandDispatcher {
  readonly #journal: SqliteJournal
  readonly #executor: EffectExecutor
  readonly #workerId: CommandDispatcherOptions["worker_id"]
  readonly #leaseDurationMs: number
  readonly #clock: Clock
  readonly #identities: IdentityFactory

  constructor(options: CommandDispatcherOptions) {
    if (!Number.isSafeInteger(options.lease_duration_ms) || options.lease_duration_ms < 1) {
      throw new RangeError("lease_duration_ms must be a positive safe integer")
    }
    this.#journal = options.journal
    this.#executor = options.executor
    this.#workerId = options.worker_id
    this.#leaseDurationMs = options.lease_duration_ms
    this.#clock = options.clock ?? new SystemClock()
    this.#identities = options.identities ?? new SecureIdentityFactory()
  }

  async dispatchNext(runId: RunId): Promise<DispatchOutcome> {
    const claim = this.#journal.claimNext(runId, {
      attempt_id: this.#identities.attemptId(),
      claimed_by: this.#workerId,
      claimed_at: this.#clock.now().toISOString(),
      lease_duration_ms: this.#leaseDurationMs,
    })
    if (!claim) {
      return { status: "idle" }
    }

    const dispatchStarted = this.#journal.recordDispatchStarted(
      this.event(
        claim,
        "effect.dispatch_started",
        {
          attempt_id: claim.attempt.attempt_id,
          command_id: claim.command.command_id,
          effect_id: claim.attempt.effect_id,
        },
        claim.command.causing_event_id,
      ),
    )
    const result = await this.execute(claim)
    const resultType =
      claim.command.kind === "effect.issue"
        ? "effect.issue_result_recorded"
        : "effect.reconciliation_result_recorded"
    const resultRecorded = this.#journal.recordDispatchResult(
      this.event(
        claim,
        resultType,
        {
          attempt_id: claim.attempt.attempt_id,
          command_id: claim.command.command_id,
          effect_id: claim.attempt.effect_id,
          result,
        },
        dispatchStarted.acceptance.event_id,
      ),
    )

    return {
      status: "recorded",
      run_id: runId,
      command: claim.command,
      attempt: claim.attempt,
      dispatch_started: dispatchStarted,
      result_recorded: resultRecorded,
      result,
    }
  }

  private async execute(claim: ClaimedCommand): Promise<IssueResult | ReconciliationResult> {
    try {
      if (claim.command.kind === "effect.issue") {
        return await this.#executor.issue(
          structuredClone(claim.command),
          structuredClone(claim.attempt),
        )
      }
      if (claim.command.kind === "effect.reconcile") {
        return await this.#executor.reconcile(
          structuredClone(claim.command),
          structuredClone(claim.attempt),
        )
      }
      throw new TypeError(`Command ${claim.command.command_id} is not dispatchable`)
    } catch {
      return claim.command.kind === "effect.issue" ? "uncertain" : "unknown"
    }
  }

  private event(
    claim: ClaimedCommand,
    type: string,
    payload: ProposedEvent["payload"],
    causationId: EventId,
  ): ProposedEvent {
    return {
      event_id: this.#identities.eventId(),
      run_id: claim.command.run_id,
      type,
      schema_version: 1,
      source: { kind: "runner", source_id: this.#workerId },
      causation_id: causationId,
      ownership_epoch: claim.command.ownership_epoch,
      payload,
    }
  }
}

export class TimerDispatcher {
  readonly #journal: SqliteJournal
  readonly #sourceId: TimerDispatcherOptions["source_id"]
  readonly #clock: Clock
  readonly #identities: IdentityFactory

  constructor(options: TimerDispatcherOptions) {
    this.#journal = options.journal
    this.#sourceId = options.source_id
    this.#clock = options.clock ?? new SystemClock()
    this.#identities = options.identities ?? new SecureIdentityFactory()
  }

  fireDue(limit = 100): TimerDispatchOutcome {
    const firedAt = this.#clock.now().toISOString()
    const timers = this.#journal.listDueTimers(firedAt, limit)
    return {
      fired_at: firedAt,
      results: timers.map((timer) =>
        this.#journal.fireTimer({
          timer_id: timer.timer_id,
          event_id: this.#identities.eventId(),
          source_id: this.#sourceId,
          fired_at: firedAt,
        }),
      ),
    }
  }
}
