import type { AcceptedEvent } from "@idemantis/contracts"
import { ReductionError } from "./errors.js"
import { reduceMission } from "./reducer.js"
import type { MissionRuntimeDefinition, MissionState, ReplayResult } from "./types.js"

export function replayMission(
  definition: MissionRuntimeDefinition,
  events: readonly AcceptedEvent[],
): ReplayResult {
  if (events.length === 0) {
    throw new ReductionError("EMPTY_REPLAY", "Replay requires at least one accepted event")
  }

  let state: MissionState | null = null
  const commands: ReplayResult["commands"] = []
  const frames: ReplayResult["frames"] = []
  let stateDigest: ReplayResult["state_digest"] | undefined

  for (const event of events) {
    const result = reduceMission(definition, state, event)
    state = result.state
    stateDigest = result.state_digest
    commands.push(...result.commands)
    frames.push({
      sequence: event.sequence,
      event_id: event.event_id,
      state_digest: result.state_digest,
      command_ids: result.commands.map((command) => command.command_id),
    })
  }

  if (!state || !stateDigest) {
    throw new ReductionError("EMPTY_REPLAY", "Replay did not produce a mission projection")
  }

  return {
    state,
    commands,
    frames,
    state_digest: stateDigest,
  }
}
