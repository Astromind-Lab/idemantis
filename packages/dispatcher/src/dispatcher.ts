import {
  canonicalJson,
  type EventId,
  type IssueResult,
  type JsonObject,
  type JsonValue,
  JsonValueSchema,
  type ProposedEvent,
  type ReconciliationResult,
  type RunId,
} from "@idemantis/contracts"
import Schema from "typebox/schema"
import { JournalError, type ClaimedCommand, type SqliteJournal } from "@idemantis/journal-sqlite"
import { SecureIdentityFactory } from "./identity.js"
import { SystemClock } from "./clock.js"
import type {
  Clock,
  CommandDispatcherOptions,
  DispatchOutcome,
  EffectExecutor,
  ExecutionReport,
  IdentityFactory,
  InterruptedDispatchRecovery,
  TimerDispatcherOptions,
  TimerDispatchOutcome,
} from "./types.js"

const maximumOutputBytes = 65_536
const maximumEvidenceBytes = 16_384

function boundedJson(value: unknown, limit: number): JsonValue | undefined {
  if (!Schema.Check(JsonValueSchema, value)) {
    return undefined
  }
  return Buffer.byteLength(canonicalJson(value as JsonValue)) <= limit
    ? structuredClone(value as JsonValue)
    : undefined
}

function reportDetails(
  report: ExecutionReport<IssueResult | ReconciliationResult>,
): Record<string, JsonValue> {
  const output =
    report.output === undefined ? undefined : boundedJson(report.output, maximumOutputBytes)
  const outputOmitted = report.output !== undefined && output === undefined
  const reference =
    typeof report.external_reference === "string" &&
    report.external_reference.length >= 1 &&
    report.external_reference.length <= 256
      ? report.external_reference
      : undefined
  const evidence =
    report.evidence === undefined ? undefined : boundedJson(report.evidence, maximumEvidenceBytes)
  const evidenceObject: JsonObject | undefined =
    evidence && typeof evidence === "object" && !Array.isArray(evidence) ? evidence : undefined
  return {
    ...(output === undefined ? {} : { output }),
    ...(reference ? { external_reference: reference } : {}),
    ...(evidenceObject || outputOmitted
      ? {
          evidence: {
            ...(evidenceObject ?? {}),
            ...(outputOmitted
              ? { output_omitted: "output exceeded the journal size limit or was not JSON" }
              : {}),
          },
        }
      : {}),
  }
}

export class CommandDispatcher {
  readonly #journal: SqliteJournal
  readonly #executor: EffectExecutor
  readonly #workerId: CommandDispatcherOptions["worker_id"]
  readonly #ownerMode: NonNullable<CommandDispatcherOptions["owner_mode"]>
  readonly #leaseDurationMs: number
  readonly #clock: Clock
  readonly #identities: IdentityFactory
  readonly #reconcileAttempts: number
  readonly #reconcileBackoffMs: number

  constructor(options: CommandDispatcherOptions) {
    if (!Number.isSafeInteger(options.lease_duration_ms) || options.lease_duration_ms < 1) {
      throw new RangeError("lease_duration_ms must be a positive safe integer")
    }
    this.#reconcileAttempts = options.reconcile_retry?.attempts ?? 4
    this.#reconcileBackoffMs = options.reconcile_retry?.backoff_ms ?? 250
    if (
      !Number.isSafeInteger(this.#reconcileAttempts) ||
      this.#reconcileAttempts < 1 ||
      !Number.isSafeInteger(this.#reconcileBackoffMs) ||
      this.#reconcileBackoffMs < 0
    ) {
      throw new RangeError(
        "reconcile_retry needs a positive attempt count and a non-negative backoff",
      )
    }
    this.#journal = options.journal
    this.#executor = options.executor
    this.#workerId = options.worker_id
    this.#ownerMode = options.owner_mode ?? "runner"
    this.#leaseDurationMs = options.lease_duration_ms
    this.#clock = options.clock ?? new SystemClock()
    this.#identities = options.identities ?? new SecureIdentityFactory()
  }

  async dispatchNext(runId: RunId): Promise<DispatchOutcome> {
    const claim = this.#journal.claimNext(runId, {
      attempt_id: this.#identities.attemptId(),
      claimed_by: this.#workerId,
      owner_mode: this.#ownerMode,
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
    let report: ExecutionReport<IssueResult | ReconciliationResult>
    try {
      this.#journal.assertDispatchable(claim.attempt.attempt_id, this.#clock.now().toISOString())
      report = await this.execute(claim)
    } catch (error) {
      if (!(error instanceof JournalError) || error.code !== "COMMAND_OWNERSHIP_STALE") {
        throw error
      }
      report = { result: claim.command.kind === "effect.issue" ? "not_sent" : "unknown" }
    }
    const { result } = report
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
          ...reportDetails(report),
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

  async recoverInterrupted(runId: RunId): Promise<InterruptedDispatchRecovery[]> {
    const now = this.#clock.now().getTime()
    const commands = new Map(
      this.#journal.listCommands(runId).map((command) => [command.command_id, command]),
    )
    const recovered: InterruptedDispatchRecovery[] = []
    for (const attempt of this.#journal.listAttempts(runId)) {
      const command = commands.get(attempt.command_id)
      if (
        !command ||
        !attempt.started_event_id ||
        attempt.status !== "dispatch_started" ||
        attempt.claimed_by !== this.#workerId ||
        command.status !== "claimed" ||
        command.claimed_by !== this.#workerId ||
        Date.parse(attempt.claim_deadline) > now ||
        (command.kind !== "effect.issue" && command.kind !== "effect.reconcile")
      ) {
        continue
      }
      const issue = command.kind === "effect.issue"
      const report: ExecutionReport<IssueResult | ReconciliationResult> = issue
        ? { result: "uncertain", evidence: { interrupted: "dispatch did not report a result" } }
        : await this.reconcileAgain(command, attempt, now)
      const { result } = report
      const resultRecorded = this.#journal.recordDispatchResult(
        this.event(
          { command, attempt },
          issue ? "effect.issue_result_recorded" : "effect.reconciliation_result_recorded",
          {
            attempt_id: attempt.attempt_id,
            command_id: command.command_id,
            effect_id: attempt.effect_id,
            result,
            ...reportDetails(report),
          },
          attempt.started_event_id,
        ),
      )
      recovered.push({
        run_id: runId,
        command,
        attempt,
        result_recorded: resultRecorded,
        result,
      })
    }
    return recovered
  }

  private async reconcileAgain(
    command: ClaimedCommand["command"],
    attempt: ClaimedCommand["attempt"],
    now: number,
  ): Promise<ExecutionReport<IssueResult | ReconciliationResult>> {
    const deadline = new Date(now + this.#leaseDurationMs).toISOString()
    const report = await this.execute({
      command: { ...command, claim_deadline: deadline },
      attempt: { ...attempt, claim_deadline: deadline },
    })
    const evidence =
      report.evidence && typeof report.evidence === "object" && !Array.isArray(report.evidence)
        ? report.evidence
        : {}
    return { ...report, evidence: { ...evidence, repeated_after_interruption: true } }
  }

  private async execute(
    claim: ClaimedCommand,
  ): Promise<ExecutionReport<IssueResult | ReconciliationResult>> {
    const command = structuredClone(claim.command)
    const attempt = structuredClone(claim.attempt)
    try {
      if (claim.command.kind === "effect.issue") {
        return this.#executor.invokeIssue
          ? await this.#executor.invokeIssue(command, attempt)
          : { result: await this.#executor.issue(command, attempt) }
      }
      if (claim.command.kind === "effect.reconcile") {
        return await this.reconcileWithRetry(command, attempt)
      }
      throw new TypeError(`Command ${claim.command.command_id} is not dispatchable`)
    } catch (error) {
      return {
        result: claim.command.kind === "effect.issue" ? "uncertain" : "unknown",
        evidence: {
          executor_error: (error instanceof Error ? error.message : String(error)).slice(0, 512),
        },
      }
    }
  }

  private async reconcileWithRetry(
    command: ClaimedCommand["command"],
    attempt: ClaimedCommand["attempt"],
  ): Promise<ExecutionReport<IssueResult | ReconciliationResult>> {
    let backoff = this.#reconcileBackoffMs
    for (let tried = 1; ; tried += 1) {
      try {
        return this.#executor.invokeReconciliation
          ? await this.#executor.invokeReconciliation(command, attempt)
          : { result: await this.#executor.reconcile(command, attempt) }
      } catch (error) {
        const remaining = Date.parse(attempt.claim_deadline) - this.#clock.now().getTime()
        if (tried >= this.#reconcileAttempts || remaining <= backoff * 2) {
          throw error
        }
        await new Promise((resolvePromise) => setTimeout(resolvePromise, backoff))
        backoff *= 2
      }
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
      source: { kind: this.#ownerMode, source_id: this.#workerId },
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
