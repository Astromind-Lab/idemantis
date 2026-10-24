import type { RunId } from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import type { SqliteJournal } from "@idemantis/journal-sqlite"
import {
  type MissionExecutionStart,
  type MissionExecutionStatus,
  TransportError,
} from "@idemantis/transport"

export interface RoutableMissionExecutor {
  accepts(runtime: MissionRuntimeDefinition): boolean
  run(runId: RunId): Promise<void>
  start(runId: RunId): MissionExecutionStart
  executionStatus(runId: RunId): MissionExecutionStatus
}

export class MissionRouter {
  readonly #journal: SqliteJournal
  readonly #executors: readonly RoutableMissionExecutor[]

  constructor(journal: SqliteJournal, executors: readonly RoutableMissionExecutor[]) {
    this.#journal = journal
    this.#executors = [...executors]
  }

  accepts(runtime: MissionRuntimeDefinition): boolean {
    return this.#executors.some((executor) => executor.accepts(runtime))
  }

  async run(runId: RunId): Promise<void> {
    return this.select(runId).run(runId)
  }

  start(runId: RunId): MissionExecutionStart {
    return this.select(runId).start(runId)
  }

  executionStatus(runId: RunId): MissionExecutionStatus {
    return this.select(runId).executionStatus(runId)
  }

  resume(): RunId[] {
    const resumed: RunId[] = []
    for (const mission of this.#journal.listMissions()) {
      if (mission.state.terminal_outcome) {
        continue
      }
      const executor = this.#executors.find((candidate) => candidate.accepts(mission.runtime))
      if (executor?.start(mission.state.run_id).disposition === "started") {
        resumed.push(mission.state.run_id)
      }
    }
    return resumed.sort()
  }

  private select(runId: RunId): RoutableMissionExecutor {
    const runtime = this.#journal.getMission(runId).runtime
    const executor = this.#executors.find((candidate) => candidate.accepts(runtime))
    if (!executor) {
      throw new TransportError(
        "RUNNER_UNAVAILABLE",
        "No configured executor is bound to this mission",
        503,
      )
    }
    return executor
  }
}
