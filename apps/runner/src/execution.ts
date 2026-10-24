import type {
  CorrelationId,
  EffectId,
  EventId,
  JsonObject,
  JsonValue,
  ObservationId,
  ProposedEvent,
  RunId,
  SourceId,
} from "@idemantis/contracts"
import {
  CommandDispatcher,
  SecureIdentityFactory,
  TimerDispatcher,
  type Clock,
  type EffectExecutor,
  type IdentityFactory,
} from "@idemantis/dispatcher"
import type { AppendResult, SqliteJournal } from "@idemantis/journal-sqlite"

export interface RunnerObservation {
  event_id: EventId
  observation_id: ObservationId
  observation_type: string
  payload: JsonObject
  effect_id?: EffectId
  subject?: JsonValue
  observed_at?: string
  correlation_id?: CorrelationId
}

export interface RunnerExecutionOptions {
  journal: SqliteJournal
  executor: EffectExecutor
  worker_id: SourceId
  timer_source_id: SourceId
  lease_duration_ms: number
  clock?: Clock
  identities?: IdentityFactory
}

export interface ExecutionDrainResult {
  dispatches: number
  timers: number
  status: "idle" | "terminal"
  current_sequence: number
}

export class RunnerExecution {
  readonly #journal: SqliteJournal
  readonly #commands: CommandDispatcher
  readonly #timers: TimerDispatcher

  constructor(options: RunnerExecutionOptions) {
    const identities = options.identities ?? new SecureIdentityFactory()
    this.#journal = options.journal
    this.#commands = new CommandDispatcher({
      journal: options.journal,
      executor: options.executor,
      worker_id: options.worker_id,
      owner_mode: "runner",
      lease_duration_ms: options.lease_duration_ms,
      ...(options.clock ? { clock: options.clock } : {}),
      identities,
    })
    this.#timers = new TimerDispatcher({
      journal: options.journal,
      source_id: options.timer_source_id,
      ...(options.clock ? { clock: options.clock } : {}),
      identities,
    })
  }

  async drain(runId: RunId, maximumSteps = 1_000): Promise<ExecutionDrainResult> {
    if (!Number.isSafeInteger(maximumSteps) || maximumSteps < 1) {
      throw new RangeError("maximumSteps must be a positive safe integer")
    }
    let dispatches = 0
    let timers = 0
    for (let step = 0; step < maximumSteps; step += 1) {
      const mission = this.#journal.getMission(runId)
      if (mission.state.terminal_outcome) {
        return {
          dispatches,
          timers,
          status: "terminal",
          current_sequence: mission.state.current_sequence,
        }
      }
      const command = await this.#commands.dispatchNext(runId)
      if (command.status === "recorded") {
        dispatches += 1
        continue
      }
      const fired = this.#timers.fireDue()
      if (fired.results.length > 0) {
        timers += fired.results.length
        continue
      }
      return {
        dispatches,
        timers,
        status: "idle",
        current_sequence: mission.state.current_sequence,
      }
    }
    throw new Error(`Mission ${runId} exceeded the execution step limit`)
  }

  recordObservation(
    runId: RunId,
    sourceId: SourceId,
    observation: RunnerObservation,
  ): AppendResult {
    const mission = this.#journal.getMission(runId)
    const proposal: ProposedEvent = {
      event_id: observation.event_id,
      run_id: runId,
      type: "observation.recorded",
      schema_version: 1,
      source: { kind: "adapter", source_id: sourceId },
      ownership_epoch: mission.state.ownership_epoch,
      ...(observation.observed_at ? { observed_at: observation.observed_at } : {}),
      ...(observation.correlation_id ? { correlation_id: observation.correlation_id } : {}),
      payload: {
        observation_id: observation.observation_id,
        observation_type: observation.observation_type,
        ...(observation.effect_id ? { effect_id: observation.effect_id } : {}),
        ...(observation.subject !== undefined ? { subject: observation.subject } : {}),
        payload: structuredClone(observation.payload),
      },
    }
    return this.#journal.append(proposal)
  }
}
