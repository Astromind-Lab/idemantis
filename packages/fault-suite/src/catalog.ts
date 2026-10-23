import { type FaultRule, SeededRandom } from "@idemantis/simulator"
import type { EdgeCaseDefinition, EngineCaseDefinition } from "./types.js"

const engineNames = [
  "clean_delivery",
  "lost_reservation_response",
  "lost_vehicle_response",
  "duplicate_arrival_observation",
  "lost_arrival_observation",
  "reordered_vehicle_observations",
  "ambiguous_access_response",
] as const

const edgeCases: readonly EdgeCaseDefinition[] = [
  {
    name: "before_branch_append",
    fault_schedule: [{ boundary: "before_branch_append", action: "process_restart" }],
  },
  {
    name: "after_dispatch_start_commit",
    fault_schedule: [{ boundary: "after_dispatch_start_commit", action: "process_restart" }],
  },
  {
    name: "after_target_apply_before_result",
    fault_schedule: [{ boundary: "after_target_apply_before_result", action: "process_restart" }],
  },
  {
    name: "after_result_commit",
    fault_schedule: [{ boundary: "after_result_commit", action: "process_restart" }],
  },
  {
    name: "duplicate_delivery",
    fault_schedule: [{ boundary: "branch_delivery", action: "duplicate" }],
  },
  {
    name: "process_restart",
    fault_schedule: [{ boundary: "active_branch", action: "process_restart" }],
  },
  {
    name: "unverified_reboot",
    fault_schedule: [{ boundary: "active_branch", action: "boot_identity_change" }],
  },
  {
    name: "lease_expiry",
    fault_schedule: [{ boundary: "offline_execution", action: "lease_expiry" }],
  },
  {
    name: "wall_clock_discontinuity",
    fault_schedule: [{ boundary: "offline_execution", action: "wall_clock_jump" }],
  },
  {
    name: "monotonic_discontinuity",
    fault_schedule: [{ boundary: "offline_execution", action: "monotonic_reset" }],
  },
  {
    name: "stale_ownership_epoch",
    fault_schedule: [{ boundary: "target_apply", action: "stale_epoch" }],
  },
]

function rule(
  id: string,
  target: string,
  operation: string,
  stage: string,
  action: FaultRule["action"],
  delayMs?: number,
): FaultRule {
  return {
    id,
    target,
    operation,
    stage,
    occurrence: 1,
    action,
    ...(delayMs === undefined ? {} : { delay_ms: delayMs }),
  }
}

export function engineCaseFor(seed: number): EngineCaseDefinition {
  const random = new SeededRandom(seed)
  const name = engineNames[random.integer(0, engineNames.length - 1)]
  if (!name) {
    throw new Error("Engine fault case selection failed")
  }
  switch (name) {
    case "clean_delivery":
      return { name, scenario: "happy_path", fault_schedule: [], expected_outcome: "completed" }
    case "lost_reservation_response":
      return {
        name,
        scenario: "happy_path",
        fault_schedule: [
          rule(
            "lost-reservation-response",
            "destination-resource",
            "resource.reserve",
            "after_apply",
            "drop_response",
          ),
        ],
        expected_outcome: "completed",
      }
    case "lost_vehicle_response":
      return {
        name,
        scenario: "happy_path",
        fault_schedule: [
          rule(
            "lost-vehicle-response",
            "vehicle",
            "vehicle.dispatch",
            "after_apply",
            "drop_response",
          ),
        ],
        expected_outcome: "completed",
      }
    case "duplicate_arrival_observation":
      return {
        name,
        scenario: "happy_path",
        fault_schedule: [
          rule(
            "duplicate-arrival-observation",
            "vehicle",
            "vehicle.dispatch",
            "arrival_observation",
            "duplicate_observation",
          ),
        ],
        expected_outcome: "completed",
      }
    case "lost_arrival_observation":
      return {
        name,
        scenario: "happy_path",
        fault_schedule: [
          rule(
            "lost-arrival-observation",
            "vehicle",
            "vehicle.dispatch",
            "arrival_observation",
            "drop_observation",
          ),
        ],
        expected_outcome: "completed",
      }
    case "reordered_vehicle_observations":
      return {
        name,
        scenario: "happy_path",
        fault_schedule: [
          rule(
            "delayed-pickup-observation",
            "vehicle",
            "vehicle.dispatch",
            "pickup_observation",
            "delay",
            random.integer(500, 1_500),
          ),
        ],
        expected_outcome: "completed",
      }
    case "ambiguous_access_response":
      return {
        name,
        scenario: "happy_path",
        fault_schedule: [
          rule(
            "lost-access-response",
            "access-service",
            "access.open",
            "after_apply",
            "drop_response",
          ),
        ],
        expected_outcome: "awaiting_operator",
      }
  }
}

export function edgeCaseFor(seed: number): EdgeCaseDefinition {
  const random = new SeededRandom((seed ^ 0x9e37_79b9) >>> 0)
  const selected = edgeCases[random.integer(0, edgeCases.length - 1)]
  if (!selected) {
    throw new Error("Edge fault case selection failed")
  }
  return structuredClone(selected)
}

export const namedEngineCases = [...engineNames]
export const namedEdgeCases = edgeCases.map((entry) => entry.name)
