export type QueueBucket = "needs_attention" | "active" | "waiting" | "completed"

export interface QueueFacts {
  attention: boolean
  terminal: boolean
  waiting: boolean
}

export function queueBucketFor(facts: QueueFacts): QueueBucket {
  if (facts.attention) {
    return "needs_attention"
  }
  if (facts.terminal) {
    return "completed"
  }
  if (facts.waiting) {
    return "waiting"
  }
  return "active"
}

export function compareMissionPriority(
  left: { priority_rank: number; run_id: string },
  right: { priority_rank: number; run_id: string },
): number {
  return left.priority_rank - right.priority_rank || left.run_id.localeCompare(right.run_id)
}
