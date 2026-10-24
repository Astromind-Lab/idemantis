import { sha256Digest, type Digest, type JsonObject } from "@idemantis/contracts"
import type { MissionManifest, StateManifest } from "@idemantis/sdk"
import type { GraphArtifact, GraphNode, GraphTransition } from "./schemas.js"

function node(manifest: MissionManifest, stateId: string, state: StateManifest): GraphNode {
  const base = {
    state_id: stateId,
    kind: state.kind,
    display_code: `state.${state.kind}`,
    display_args: { state: stateId },
  }
  if (state.kind === "effect" || state.kind === "reconcile") {
    const operation = manifest.operations[state.operation]
    if (!operation) {
      throw new TypeError(`State ${stateId} references unknown operation ${state.operation}`)
    }
    return {
      ...base,
      operation: {
        key: state.operation,
        adapter: operation.adapter,
        operation: operation.operation,
        consequence: operation.consequence,
      },
    }
  }
  if (state.kind === "final") {
    return { ...base, terminal_outcome: state.outcome }
  }
  return base
}

function routeKind(trigger: string): GraphTransition["route_kind"] {
  if (trigger.includes("cancel")) {
    return "cancellation"
  }
  if (trigger.includes("compensat")) {
    return "compensation"
  }
  if (
    trigger.includes("uncertain") ||
    trigger.includes("failed") ||
    trigger.includes("timeout") ||
    trigger.includes("conflicting") ||
    trigger.includes("unknown")
  ) {
    return "recovery"
  }
  return "normal"
}

function transitionId(from: string, to: string, trigger: string): string {
  return sha256Digest({ from, to, trigger }).slice("sha256:".length, "sha256:".length + 24)
}

export function graphArtifact(manifest: MissionManifest, definitionDigest: Digest): GraphArtifact {
  const nodes = Object.entries(manifest.states)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([stateId, state]) => node(manifest, stateId, state))
  const transitions = manifest.graph.edges
    .map((edge) => ({
      transition_id: transitionId(edge.from, edge.to, edge.trigger),
      source_state_id: edge.from,
      target_state_id: edge.to,
      display_code: `transition.${edge.trigger}`,
      display_args: { trigger: edge.trigger },
      route_kind: routeKind(edge.trigger),
    }))
    .sort((left, right) => left.transition_id.localeCompare(right.transition_id))
  const unsigned = {
    schema_version: 1 as const,
    definition_digest: definitionDigest,
    entry_state_id: manifest.initial,
    nodes,
    transitions,
  }
  return {
    ...unsigned,
    graph_digest: sha256Digest(unsigned as unknown as JsonObject),
  }
}

export function verifyGraphArtifact(value: GraphArtifact): boolean {
  const { graph_digest: graphDigest, ...unsigned } = value
  return sha256Digest(unsigned as unknown as JsonObject) === graphDigest
}
