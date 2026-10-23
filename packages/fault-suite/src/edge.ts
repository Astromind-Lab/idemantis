import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import type { AdapterInvocationContext } from "@idemantis/adapter-kit"
import {
  type AttemptRecord,
  type CommandRecord,
  type EdgeDelegation,
  type EdgeExecutionScope,
  type JsonObject,
  type ProposedEvent,
  sha256Digest,
} from "@idemantis/contracts"
import {
  EdgeAgent,
  EdgeAgentError,
  type EdgeClock,
  EdgeStore,
  edgeDelegationDigest,
} from "@idemantis/edge-agent"
import { SimulationKernel, VehicleSimulator } from "@idemantis/simulator"
import { edgeCaseFor } from "./catalog.js"
import { InvariantChecker } from "./invariants.js"
import type { AppliedEffectMetric, EdgeCaseReport } from "./types.js"

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const edgeId = "edge-fault-suite" as const
const leaseId = "lse_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const adapterId = "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const attemptId = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const commandId = "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const effectId = "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const startedEventId = "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const resultEventId = "evt_01ARZ3NDEKTSV4RRFFQ69G5FAW" as const
const causingEventId = "evt_01ARZ3NDEKTSV4RRFFQ69G5FAX" as const
const definitionDigest = `sha256:${"b".repeat(64)}` as const
const executionBindingDigest = `sha256:${"c".repeat(64)}` as const
const dispatchInput = {
  vehicle_id: "vehicle-7",
  route: "north",
  pickup: "dock-a",
  destination: "bay-7",
}
const inputDigest = sha256Digest(dispatchInput)
const execution: EdgeExecutionScope = {
  state: "travel",
  effect_class: "physical_actuation",
  operation: "vehicle.dispatch",
  adapter_instance_id: adapterId,
  input_digest: inputDigest,
}

class FaultClock implements EdgeClock {
  #wall = Date.parse("2026-01-01T00:00:00.100Z")
  #monotonic = 1_000

  wallNow(): Date {
    return new Date(this.#wall)
  }

  monotonicNowMs(): number {
    return this.#monotonic
  }

  advance(milliseconds: number): void {
    this.#wall += milliseconds
    this.#monotonic += milliseconds
  }

  advanceWall(milliseconds: number): void {
    this.#wall += milliseconds
  }

  setMonotonic(milliseconds: number): void {
    this.#monotonic = milliseconds
  }
}

function delegation(maximumOfflineDurationMs = 300_000): EdgeDelegation {
  const value: EdgeDelegation = {
    lease: {
      lease_id: leaseId,
      run_id: runId,
      owner_mode: "edge",
      owner_id: edgeId,
      ownership_epoch: 2,
      branch_anchor_sequence: 10,
      not_before: "2026-01-01T00:00:00.000Z",
      not_after: "2026-01-01T00:10:00.000Z",
      maximum_offline_duration_ms: maximumOfflineDurationMs,
      allowed_clock_uncertainty_ms: 100,
      delegation_digest: `sha256:${"a".repeat(64)}`,
    },
    definition_digest: definitionDigest,
    execution_binding_digest: executionBindingDigest,
    allowed_actions: [
      {
        state: "travel",
        effect_class: "physical_actuation",
        operation: "vehicle.dispatch",
        adapter_instance_id: adapterId,
        input_digests: [inputDigest],
      },
    ],
    allowed_event_types: ["effect.dispatch_started", "effect.issue_result_recorded"],
    resource_limits: { maximum_branch_events: 10, maximum_dispatches: 5 },
  }
  return { ...value, lease: { ...value.lease, delegation_digest: edgeDelegationDigest(value) } }
}

function event(id: ProposedEvent["event_id"], type: string): ProposedEvent {
  return {
    event_id: id,
    run_id: runId,
    type,
    schema_version: 1,
    source: { kind: "edge", source_id: edgeId },
    causation_id: type === "effect.issue_result_recorded" ? startedEventId : causingEventId,
    ownership_epoch: 2,
    payload: {
      attempt_id: attemptId,
      command_id: commandId,
      effect_id: effectId,
      ...(type === "effect.issue_result_recorded" ? { result: "accepted" } : {}),
    },
  }
}

function command(): CommandRecord {
  return {
    command_id: commandId,
    run_id: runId,
    causing_event_id: causingEventId,
    command_slot: "effect.vehicle_dispatch.issue.1",
    kind: "effect.issue",
    status: "claimed",
    ownership_epoch: 2,
    payload: { effect_id: effectId, operation: "vehicle.dispatch", input: dispatchInput },
    created_at: "2026-01-01T00:00:00.000Z",
    claim_deadline: "2026-01-01T00:01:00.000Z",
    claimed_by: edgeId,
  }
}

function attempt(): AttemptRecord {
  return {
    attempt_id: attemptId,
    command_id: commandId,
    effect_id: effectId,
    status: "claimed",
    ownership_epoch: 2,
    claimed_by: edgeId,
    claim_deadline: "2026-01-01T00:01:00.000Z",
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
  }
}

function invocation(kernel: SimulationKernel, epoch = 2): AdapterInvocationContext {
  return {
    adapter_instance_id: adapterId,
    run_id: runId,
    command_id: commandId,
    attempt_id: attemptId,
    effect_id: effectId,
    ownership_epoch: epoch,
    deadline: new Date(kernel.now().getTime() + 60_000).toISOString(),
    source_id: edgeId,
    signal: new AbortController().signal,
  }
}

export async function runEdgeCase(seed: number): Promise<EdgeCaseReport> {
  const selected = edgeCaseFor(seed)
  const directory = mkdtempSync(join(tmpdir(), "idemantis-fault-suite-"))
  const path = join(directory, "edge.sqlite")
  const stores: EdgeStore[] = []
  const clock = new FaultClock()
  const kernel = new SimulationKernel({ seed })
  const vehicle = new VehicleSimulator(kernel, { vehicle_id: dispatchInput.vehicle_id })
  let currentStore = new EdgeStore({ path, opened_at: "2026-01-01T00:00:00.000Z" })
  stores.push(currentStore)
  let agent = new EdgeAgent({
    edge_id: edgeId,
    boot_identity: "boot-a",
    clock,
    store: currentStore,
  })
  let duplicateAccepted = false
  let replayBlocked = false
  let targetInvocations = 0

  const restart = (bootIdentity = "boot-a") => {
    currentStore.close()
    currentStore = new EdgeStore({ path, opened_at: clock.wallNow().toISOString() })
    stores.push(currentStore)
    agent = new EdgeAgent({
      edge_id: edgeId,
      boot_identity: bootIdentity,
      clock,
      store: currentStore,
    })
  }
  const appendStarted = () =>
    agent.append(event(startedEventId, "effect.dispatch_started"), { execution })
  const appendResult = () => agent.append(event(resultEventId, "effect.issue_result_recorded"))
  const applyTarget = async (epoch = 2) => {
    targetInvocations += 1
    return vehicle.dispatch.issue(invocation(kernel, epoch), dispatchInput)
  }
  const rejectReplay = async () => {
    try {
      await agent.dispatch({
        state: "travel",
        command: command(),
        attempt: attempt(),
        executor: {
          binding: () => ({
            operation_key: "vehicle.dispatch",
            adapter_instance_id: adapterId,
            effect_class: "physical_actuation",
            execution_location: "either",
          }),
          issue: async () => (await applyTarget()).result,
        },
        dispatch_started_event_id: startedEventId,
        result_event_id: resultEventId,
      })
      return false
    } catch (error) {
      return error instanceof EdgeAgentError && error.code === "EDGE_DISPATCH_ALREADY_STARTED"
    }
  }

  try {
    agent.activate(delegation(selected.name === "lease_expiry" ? 900_000 : 300_000), {
      measured_clock_uncertainty_ms: 50,
    })
    switch (selected.name) {
      case "before_branch_append":
        restart()
        break
      case "after_dispatch_start_commit":
        appendStarted()
        restart()
        replayBlocked = await rejectReplay()
        break
      case "after_target_apply_before_result":
        appendStarted()
        await applyTarget()
        restart()
        replayBlocked = await rejectReplay()
        break
      case "after_result_commit":
        appendStarted()
        await applyTarget()
        appendResult()
        restart()
        break
      case "duplicate_delivery": {
        const started = appendStarted()
        const duplicate = agent.append(structuredClone(started.event.proposal), { execution })
        duplicateAccepted = duplicate.duplicate
        await applyTarget()
        appendResult()
        break
      }
      case "process_restart":
        appendStarted()
        restart()
        await applyTarget()
        appendResult()
        break
      case "unverified_reboot":
        appendStarted()
        restart("boot-b")
        break
      case "lease_expiry":
        clock.advance(610_000)
        break
      case "wall_clock_discontinuity":
        clock.advanceWall(1_000)
        break
      case "monotonic_discontinuity":
        clock.setMonotonic(0)
        break
      case "stale_ownership_epoch":
        await vehicle.dispatch.activateFence?.({
          adapter_instance_id: adapterId,
          operation_key: "vehicle.dispatch",
          scope_reference: dispatchInput.vehicle_id,
          ownership_epoch: 3,
          requested_at: kernel.now().toISOString(),
          signal: new AbortController().signal,
        })
        await applyTarget(2)
        break
      default:
        throw new Error(`Unknown edge fault case ${selected.name}`)
    }

    const state = agent.state()
    const upload = agent.reconnectUpload()
    currentStore.verifyIntegrity()
    const target = vehicle.snapshot()
    const appliedEffects: AppliedEffectMetric[] = target.metrics.map((metric) => ({
      target: target.target,
      ...metric,
      maximum_applied_effects: 1,
    }))
    const checker = new InvariantChecker()
    const expectedStop =
      selected.name === "unverified_reboot"
        ? "unverified_reboot"
        : selected.name === "lease_expiry"
          ? "lease_expired"
          : selected.name === "wall_clock_discontinuity"
            ? "wall_clock_discontinuity"
            : selected.name === "monotonic_discontinuity"
              ? "monotonic_discontinuity"
              : undefined
    const expectedEvents =
      selected.name === "before_branch_append" ||
      selected.name === "lease_expiry" ||
      selected.name === "wall_clock_discontinuity" ||
      selected.name === "monotonic_discontinuity" ||
      selected.name === "stale_ownership_epoch"
        ? 0
        : selected.name === "after_dispatch_start_commit" ||
            selected.name === "after_target_apply_before_result" ||
            selected.name === "unverified_reboot"
          ? 1
          : 2

    checker.check(
      "edge_branch_is_gapless",
      upload.events.every((entry, index) => entry.edge_local_sequence === index + 1),
      "Edge local sequence is not gapless",
    )
    checker.check(
      "edge_committed_prefix_survives_restart",
      upload.events.length === expectedEvents,
      `Expected ${expectedEvents} durable events, received ${upload.events.length}`,
    )
    checker.check(
      "edge_target_applied_counts_satisfy_policy",
      appliedEffects.every(
        (metric) =>
          metric.applied_effects <= metric.maximum_applied_effects &&
          metric.applied_effects === new Set(metric.applied_effect_ids).size,
      ),
      "The cooperative target applied an effect more than once",
    )
    checker.check(
      "edge_target_invocation_is_not_replayed_after_restart",
      targetInvocations <= 1 &&
        (!["after_dispatch_start_commit", "after_target_apply_before_result"].includes(
          selected.name,
        ) ||
          replayBlocked),
      `Target received ${targetInvocations} invocations`,
    )
    checker.check(
      "edge_stop_reason_matches_fault",
      state?.stopped_reason === expectedStop,
      `Expected stop reason ${expectedStop ?? "none"}, received ${state?.stopped_reason ?? "none"}`,
    )
    checker.check(
      "edge_duplicate_delivery_is_idempotent",
      selected.name !== "duplicate_delivery" || duplicateAccepted,
      "Duplicate event delivery was not recognized",
    )
    checker.check(
      "edge_stale_epoch_never_applies",
      selected.name !== "stale_ownership_epoch" ||
        (appliedEffects.every((metric) => metric.applied_effects === 0) &&
          appliedEffects.some((metric) => metric.stale_rejections === 1)),
      "A stale ownership epoch reached the target",
    )

    const finalStateDigest = sha256Digest({
      state: state as unknown as JsonObject,
      branch: upload.events.map((entry) => ({
        edge_local_sequence: entry.edge_local_sequence,
        proposal_digest: entry.proposal_digest,
        branch_digest: entry.branch_digest,
      })),
      target_state_digest: target.state_digest,
    })

    return {
      name: selected.name,
      seed,
      fault_schedule: structuredClone(selected.fault_schedule),
      definition_digest: definitionDigest,
      ...(state?.stopped_reason ? { stopped_reason: state.stopped_reason } : {}),
      final_state_digest: finalStateDigest,
      branch_event_count: upload.events.length,
      applied_effects: appliedEffects,
      invariants: checker.results(),
    }
  } finally {
    for (const store of stores) {
      store.close()
    }
    rmSync(directory, { recursive: true, force: true })
  }
}
