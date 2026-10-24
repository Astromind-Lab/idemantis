import { AdapterExecutor, defineAdapterOperation } from "@idemantis/adapter-kit"
import {
  AttemptIdSchema,
  EventIdSchema,
  type AttemptId,
  type EventId,
  type OperationCapabilities,
  type OwnershipLease,
  type ProposedEvent,
} from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import {
  defineMission,
  defineOperation,
  effectState,
  finalState,
  fromInput,
  mapFields,
  reconcileState,
  safeRetry,
  waitForObservation,
} from "@idemantis/sdk"
import { chmodSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import Type from "typebox"
import Schema from "typebox/schema"
import { afterEach, describe, expect, it, vi } from "vitest"
import {
  CommandDispatcher,
  OwnershipCoordinator,
  SecureIdentityFactory,
  TimerDispatcher,
  VirtualClock,
  type IdentityFactory,
} from "../src/index.js"

const capabilities = {
  duplicate_safety: "native_idempotency",
  reconciliation: "authoritative",
  fencing_mode: "application_epoch",
  fencing_scope: "subject",
  handoff_mode: "settle_first",
  execution_location: "either",
  cancellation: false,
  compensation: false,
  observation_stream: true,
} satisfies OperationCapabilities

const transferableCapabilities = {
  ...capabilities,
  handoff_mode: "transferable",
} satisfies OperationCapabilities

const dispatch = defineOperation({
  key: "vehicle.dispatch",
  adapter: "vehicle",
  operation: "vehicle.dispatch",
  schemaVersion: 1,
  inputSchema: Type.Object({ route: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ dispatch: Type.String() }, { additionalProperties: false }),
  effectClass: "physical_actuation",
  consequence: "compensatable",
  capabilities,
})

const snapshot = defineOperation({
  key: "vehicle.snapshot",
  adapter: "vehicle",
  operation: "vehicle.snapshot",
  schemaVersion: 1,
  inputSchema: Type.Object({ vehicle: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ status: Type.String() }, { additionalProperties: false }),
  effectClass: "observation",
  consequence: "observational",
  capabilities,
})

const mission = defineMission({
  id: "dispatcher-delivery",
  version: "1.0.0",
  input: Type.Object(
    { route: Type.String(), vehicle: Type.String() },
    { additionalProperties: false },
  ),
  initial: "dispatchVehicle",
  states: {
    dispatchVehicle: effectState({
      operation: dispatch,
      effectSlot: "dispatch",
      input: mapFields({ route: fromInput("route") }),
      retry: safeRetry({ attempts: 2, backoffMs: 100 }),
      onAccepted: "waitForArrival",
      onSettled: "complete",
      onFailed: "failed",
      onUncertain: "reconcileVehicle",
    }),
    waitForArrival: waitForObservation({
      observationType: "vehicle.arrived",
      schemaVersion: 1,
      correlation: { kind: "effect", effectState: "dispatchVehicle" },
      timeout: { afterMs: 1_000, onTimeout: "reconcileVehicle" },
      onObserved: "complete",
    }),
    reconcileVehicle: reconcileState({
      effectState: "dispatchVehicle",
      operation: snapshot,
      subject: fromInput("vehicle"),
      onConfirmedCompleted: "complete",
      onConfirmedNotApplied: "dispatchVehicle",
      onConfirmedInProgress: "waitForArrival",
      onConflictingState: "unresolved",
      onUnknown: "unresolved",
    }),
    complete: finalState("completed"),
    failed: finalState("failed"),
    unresolved: finalState("unresolved"),
  },
})

const twoStepMission = defineMission({
  id: "two-step-dispatch",
  version: "1.0.0",
  input: Type.Object(
    { route: Type.String(), vehicle: Type.String() },
    { additionalProperties: false },
  ),
  initial: "firstDispatch",
  states: {
    firstDispatch: effectState({
      operation: dispatch,
      effectSlot: "first",
      input: mapFields({ route: fromInput("route") }),
      retry: safeRetry({ attempts: 2, backoffMs: 100 }),
      onSettled: "secondDispatch",
      onFailed: "failed",
      onUncertain: "unresolved",
    }),
    secondDispatch: effectState({
      operation: dispatch,
      effectSlot: "second",
      input: mapFields({ route: fromInput("route") }),
      retry: safeRetry({ attempts: 2, backoffMs: 100 }),
      onSettled: "complete",
      onFailed: "failed",
      onUncertain: "unresolved",
    }),
    complete: finalState("completed"),
    failed: finalState("failed"),
    unresolved: finalState("unresolved"),
  },
})

const runtime: MissionRuntimeDefinition = {
  manifest: mission.manifest,
  definition_digest: mission.definitionDigest,
  execution_binding_digest: `sha256:${"b".repeat(64)}`,
}

const twoStepRuntime: MissionRuntimeDefinition = {
  manifest: twoStepMission.manifest,
  definition_digest: twoStepMission.definitionDigest,
  execution_binding_digest: `sha256:${"c".repeat(64)}`,
}

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const temporaryDirectories: string[] = []
const journals: SqliteJournal[] = []

class FixedIdentities implements IdentityFactory {
  readonly #attempts: AttemptId[]
  readonly #events: EventId[]

  constructor(attempts: AttemptId[], events: EventId[]) {
    this.#attempts = [...attempts]
    this.#events = [...events]
  }

  attemptId(): AttemptId {
    const value = this.#attempts.shift()
    if (!value) {
      throw new Error("No attempt identity remains")
    }
    return value
  }

  eventId(): EventId {
    const value = this.#events.shift()
    if (!value) {
      throw new Error("No event identity remains")
    }
    return value
  }
}

function tickingClock() {
  let tick = 0
  return () => {
    tick += 1
    return new Date(Date.UTC(2026, 0, 1, 0, 0, tick))
  }
}

function open(selectedRuntime = runtime): SqliteJournal {
  const directory = mkdtempSync(join(tmpdir(), "idemantis-dispatcher-"))
  temporaryDirectories.push(directory)
  const journal = new SqliteJournal({
    path: join(directory, "journal.sqlite"),
    clock: tickingClock(),
  })
  journals.push(journal)
  journal.admit(selectedRuntime, admission(selectedRuntime))
  return journal
}

function admission(selectedRuntime = runtime): ProposedEvent {
  return {
    event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    run_id: runId,
    type: "mission.admitted",
    schema_version: 1,
    source: { kind: "system", source_id: "dispatcher-test" },
    ownership_epoch: 1,
    payload: {
      definition_digest: selectedRuntime.definition_digest,
      execution_binding_digest: selectedRuntime.execution_binding_digest,
      input: { route: "north", vehicle: "vehicle-7" },
    },
  }
}

function identities(): FixedIdentities {
  return new FixedIdentities(
    [
      "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      "att_01ARZ3NDEKTSV4RRFFQ69G5FAW",
      "att_01ARZ3NDEKTSV4RRFFQ69G5FAX",
    ],
    [
      "evt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
      "evt_01ARZ3NDEKTSV4RRFFQ69G5FAX",
      "evt_01ARZ3NDEKTSV4RRFFQ69G5FAY",
      "evt_01ARZ3NDEKTSV4RRFFQ69G5FAZ",
    ],
  )
}

function ownershipLease(
  leaseId: OwnershipLease["lease_id"],
  ownerId: string,
  epoch: number,
  ownerMode: "runner" | "edge" = "runner",
): OwnershipLease {
  return {
    lease_id: leaseId,
    run_id: runId,
    owner_mode: ownerMode,
    owner_id: ownerId,
    ownership_epoch: epoch,
    branch_anchor_sequence: 1,
    not_before: "2026-01-01T00:00:00.000Z",
    not_after: "2026-01-02T00:00:00.000Z",
    maximum_offline_duration_ms: 60_000,
    allowed_clock_uncertainty_ms: 100,
    delegation_digest: `sha256:${epoch.toString().repeat(64).slice(0, 64)}`,
  }
}

afterEach(() => {
  for (const journal of journals.splice(0)) {
    journal.close()
  }
  for (const directory of temporaryDirectories.splice(0)) {
    chmodSync(directory, 0o700)
    rmSync(directory, { recursive: true, force: true })
  }
})

describe("command dispatcher", () => {
  it("commits dispatch start before invoking the executor", async () => {
    const journal = open()
    const issue = vi.fn(async () => {
      expect(journal.listAttempts(runId)[0]?.status).toBe("dispatch_started")
      expect(journal.listEffects(runId)[0]?.status).toBe("dispatching")
      return "accepted" as const
    })
    const dispatcher = new CommandDispatcher({
      journal,
      executor: { issue, reconcile: vi.fn() },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:00:02Z"),
      identities: identities(),
    })

    const outcome = await dispatcher.dispatchNext(runId)

    expect(issue).toHaveBeenCalledOnce()
    expect(outcome).toMatchObject({ status: "recorded", result: "accepted" })
    expect(journal.listAttempts(runId)[0]?.status).toBe("result_recorded")
    expect(journal.listCommands(runId).map((command) => command.status)).toEqual([
      "completed",
      "completed",
    ])
  })

  it("records typed uncertainty when an executor throws", async () => {
    const journal = open()
    const issue = vi.fn(async () => {
      throw new Error("connection lost")
    })
    const reconcile = vi.fn(async () => {
      throw new Error("provider unavailable")
    })
    const dispatcher = new CommandDispatcher({
      journal,
      executor: { issue, reconcile },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:00:02Z"),
      identities: identities(),
    })

    const issueOutcome = await dispatcher.dispatchNext(runId)
    const reconciliationOutcome = await dispatcher.dispatchNext(runId)

    expect(issueOutcome).toMatchObject({ status: "recorded", result: "uncertain" })
    expect(reconciliationOutcome).toMatchObject({ status: "recorded", result: "unknown" })
    expect(reconcile).toHaveBeenCalledOnce()
    expect(journal.getMission(runId).state.status).toBe("unresolved")
    expect(await dispatcher.dispatchNext(runId)).toEqual({ status: "idle" })
  })

  it("records an interrupted dispatch as uncertain after its claim expires", async () => {
    const journal = open()
    const issue = vi.fn(() => new Promise<never>(() => {}))
    const crashed = new CommandDispatcher({
      journal,
      executor: { issue, reconcile: vi.fn() },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:00:02Z"),
      identities: identities(),
    })
    void crashed.dispatchNext(runId)
    expect(journal.listAttempts(runId)[0]?.status).toBe("dispatch_started")

    const clock = new VirtualClock("2026-01-01T00:00:30Z")
    const reconcile = vi.fn(async () => "confirmed_completed" as const)
    const restarted = new CommandDispatcher({
      journal,
      executor: { issue: vi.fn(), reconcile },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: new FixedIdentities(
        ["att_01ARZ3NDEKTSV4RRFFQ69G5FB0"],
        [
          "evt_01ARZ3NDEKTSV4RRFFQ69G5FB0",
          "evt_01ARZ3NDEKTSV4RRFFQ69G5FB1",
          "evt_01ARZ3NDEKTSV4RRFFQ69G5FB2",
        ],
      ),
    })
    const stranger = new CommandDispatcher({
      journal,
      executor: { issue: vi.fn(), reconcile: vi.fn() },
      worker_id: "runner-2",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:05:00Z"),
      identities: identities(),
    })

    expect(restarted.recoverInterrupted(runId)).toEqual([])
    expect(stranger.recoverInterrupted(runId)).toEqual([])
    clock.advanceTo("2026-01-01T00:01:02Z")
    const recovered = restarted.recoverInterrupted(runId)

    expect(recovered).toMatchObject([{ run_id: runId, result: "uncertain" }])
    expect(recovered[0]?.result_recorded.acceptance.event_id).toBe("evt_01ARZ3NDEKTSV4RRFFQ69G5FB0")
    expect(restarted.recoverInterrupted(runId)).toEqual([])
    expect(journal.getMission(runId).state.current_state).toBe("reconcileVehicle")
    expect(await restarted.dispatchNext(runId)).toMatchObject({
      status: "recorded",
      result: "confirmed_completed",
    })
    expect(issue).toHaveBeenCalledOnce()
    expect(reconcile).toHaveBeenCalledOnce()
    expect(journal.getMission(runId).state.terminal_outcome).toBe("completed")
  })

  it("executes a validated adapter operation through the durable dispatcher", async () => {
    const journal = open()
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    let receivedAttempt: string | undefined
    let receivedEffect: string | undefined
    const operation = defineAdapterOperation({
      key: "vehicle.dispatch",
      adapter: "vehicle",
      operation: "vehicle.dispatch",
      schemaVersion: 1,
      inputSchema: Type.Object({ route: Type.String() }, { additionalProperties: false }),
      outputSchema: Type.Object({ dispatch: Type.String() }, { additionalProperties: false }),
      subjectSchema: Type.String(),
      effectClass: "physical_actuation",
      consequence: "compensatable",
      capabilities,
      timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
      reconciliationLookupFields: ["vehicle"],
      observationTypes: ["vehicle.arrived"],
      observationCorrelationFields: ["effect_id"],
      maximumFenceActivationDelayMs: 100,
      async issue(context) {
        receivedAttempt = context.attempt_id
        receivedEffect = context.effect_id
        return { result: "completed", output: { dispatch: "dispatch-7" } }
      },
      async reconcile() {
        return {
          result: "confirmed_completed",
          output: { dispatch: "dispatch-7" },
        }
      },
      async activateFence(context) {
        return {
          adapter_instance_id: context.adapter_instance_id,
          scope: "subject",
          scope_reference: context.scope_reference,
          ownership_epoch: context.ownership_epoch,
          activation_reference: "fence-1",
          effective_at: context.requested_at,
          maximum_activation_delay_ms: 100,
          evidence_digest: `sha256:${"e".repeat(64)}`,
        }
      },
    })
    const executor = new AdapterExecutor({
      registrations: [
        {
          adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          operation,
        },
      ],
      source_id: "runner-1",
      location: "runner",
      clock,
    })
    const dispatcher = new CommandDispatcher({
      journal,
      executor,
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })

    const outcome = await dispatcher.dispatchNext(runId)

    expect(outcome).toMatchObject({ status: "recorded", result: "completed" })
    expect(journal.getMission(runId).state.status).toBe("completed")
    expect(receivedAttempt).toBe(journal.listAttempts(runId)[0]?.attempt_id)
    expect(receivedEffect).toBe(journal.listEffects(runId)[0]?.effect_id)
  })
})

describe("ownership handoff", () => {
  it("recreates pending work and lets an edge owner hand it back", async () => {
    const journal = open()
    const resolve = vi.fn(() => ({ capabilities }))
    const runner = new OwnershipCoordinator({
      journal,
      source_id: "runner-1",
      bindings: { resolve },
    })
    runner.registerLease(ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB0", "runner-1", 1))

    const toEdge = await runner.handoff({
      handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAX",
      target_lease: ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB1", "edge-1", 2, "edge"),
    })
    expect(journal.listCommands(runId).at(-1)).toMatchObject({
      status: "pending",
      ownership_epoch: 2,
    })
    expect(journal.listEffects(runId)[0]?.ownership_epoch).toBe(2)
    const edgeDispatcher = new CommandDispatcher({
      journal,
      executor: { issue: async () => "not_sent", reconcile: async () => "unknown" },
      worker_id: "edge-1",
      owner_mode: "edge",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:00:02Z"),
      identities: identities(),
    })
    const edgeDispatch = await edgeDispatcher.dispatchNext(runId)
    const edge = new OwnershipCoordinator({
      journal,
      source_id: "edge-1",
      bindings: { resolve },
    })
    const toRunner = await edge.handoff({
      handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAY",
      target_lease: ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB2", "runner-2", 3),
    })

    expect(toEdge.status).toBe("active")
    expect(edgeDispatch).toMatchObject({ status: "recorded", result: "not_sent" })
    expect(toRunner.status).toBe("active")
    expect(resolve).not.toHaveBeenCalled()
    expect(
      journal.listEvents(runId).filter((event) => event.type === "ownership.handoff_requested"),
    ).toHaveLength(2)
    expect(journal.getOwnership(runId).active_lease?.lease.owner_id).toBe("runner-2")
    expect(journal.listEffects(runId)[0]?.ownership_epoch).toBe(3)
  })

  it("rejects workers outside the active lease identity and time window", async () => {
    const journal = open()
    const coordinator = new OwnershipCoordinator({
      journal,
      source_id: "runner-1",
      bindings: { resolve: () => ({ capabilities }) },
    })
    coordinator.registerLease(ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FAZ", "runner-1", 1))
    const wrongOwner = new CommandDispatcher({
      journal,
      executor: { issue: async () => "completed", reconcile: async () => "unknown" },
      worker_id: "runner-2",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:00:02Z"),
    })
    const expired = new CommandDispatcher({
      journal,
      executor: { issue: async () => "completed", reconcile: async () => "unknown" },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-02T00:00:00Z"),
    })
    const wrongMode = new CommandDispatcher({
      journal,
      executor: { issue: async () => "completed", reconcile: async () => "unknown" },
      worker_id: "runner-1",
      owner_mode: "edge",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:00:02Z"),
    })

    await expect(wrongOwner.dispatchNext(runId)).rejects.toMatchObject({
      code: "COMMAND_OWNERSHIP_STALE",
    })
    await expect(wrongMode.dispatchNext(runId)).rejects.toMatchObject({
      code: "COMMAND_OWNERSHIP_STALE",
    })
    await expect(expired.dispatchNext(runId)).rejects.toMatchObject({
      code: "COMMAND_OWNERSHIP_STALE",
    })
    expect(journal.listAttempts(runId)).toEqual([])
  })

  it("fences a delayed stale request before activating the next epoch", async () => {
    const journal = open()
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    let activeEpoch = 1
    let applied = 0
    let releaseRequest = () => {}
    let markStarted = () => {}
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const delayed = new Promise<void>((resolve) => {
      releaseRequest = resolve
    })
    const operation = defineAdapterOperation({
      key: "vehicle.dispatch",
      adapter: "vehicle",
      operation: "vehicle.dispatch",
      schemaVersion: 1,
      inputSchema: Type.Object({ route: Type.String() }, { additionalProperties: false }),
      outputSchema: Type.Object({ dispatch: Type.String() }, { additionalProperties: false }),
      subjectSchema: Type.String(),
      effectClass: "physical_actuation",
      consequence: "compensatable",
      capabilities: transferableCapabilities,
      timeout: { minimumMs: 10, defaultMs: 60_000, maximumMs: 60_000 },
      reconciliationLookupFields: ["vehicle"],
      observationTypes: ["vehicle.arrived"],
      observationCorrelationFields: ["effect_id"],
      maximumFenceActivationDelayMs: 100,
      async issue(context) {
        markStarted()
        await delayed
        if (context.ownership_epoch < activeEpoch) {
          return { result: "rejected", evidence: { reason: "stale_epoch" } }
        }
        applied += 1
        return { result: "completed", output: { dispatch: "dispatch-7" } }
      },
      async reconcile() {
        return { result: "unknown" }
      },
      async activateFence(context) {
        activeEpoch = context.ownership_epoch
        return {
          adapter_instance_id: context.adapter_instance_id,
          scope: "subject",
          scope_reference: context.scope_reference,
          ownership_epoch: context.ownership_epoch,
          activation_reference: `epoch-${context.ownership_epoch}`,
          effective_at: context.requested_at,
          maximum_activation_delay_ms: 100,
          evidence_digest: `sha256:${"e".repeat(64)}`,
        }
      },
    })
    const executor = new AdapterExecutor({
      registrations: [
        {
          adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          operation,
        },
      ],
      source_id: "runner-1",
      location: "runner",
      clock,
    })
    const coordinator = new OwnershipCoordinator({
      journal,
      source_id: "runner-1",
      clock,
      bindings: {
        resolve: () => ({
          capabilities: transferableCapabilities,
          adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          fence_scope_reference: "vehicle-7",
          activate_fence: async (request) => {
            activeEpoch = request.ownership_epoch
            return {
              adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
              scope: "subject",
              scope_reference: "vehicle-7",
              ownership_epoch: request.ownership_epoch,
              activation_reference: `epoch-${request.ownership_epoch}`,
              effective_at: request.requested_at,
              maximum_activation_delay_ms: 100,
              evidence_digest: `sha256:${"f".repeat(64)}`,
            }
          },
        }),
      },
    })
    coordinator.registerLease(ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FAV", "runner-1", 1))
    const dispatcher = new CommandDispatcher({
      journal,
      executor,
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })

    const dispatch = dispatcher.dispatchNext(runId)
    await started
    const handoff = await coordinator.handoff({
      handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      target_lease: ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FAW", "edge-1", 2, "edge"),
    })
    releaseRequest()
    const outcome = await dispatch

    expect(handoff.status).toBe("active")
    expect(outcome).toMatchObject({ status: "recorded", result: "rejected" })
    expect(applied).toBe(0)
    expect(activeEpoch).toBe(2)
    expect(journal.getMission(runId).state.ownership_epoch).toBe(2)
    expect(journal.getMission(runId).state.current_state).toBe("reconcileVehicle")
    expect(journal.listEffects(runId)[0]).toMatchObject({ status: "uncertain", ownership_epoch: 2 })
    expect(journal.listCommands(runId).at(-1)).toMatchObject({
      kind: "effect.reconcile",
      ownership_epoch: 2,
      status: "pending",
    })
    expect(journal.getOwnership(runId)).toMatchObject({
      claims_frozen: false,
      active_lease: { lease: { owner_id: "edge-1", ownership_epoch: 2 } },
      handoff: { status: "active" },
    })
    expect(journal.verifyRun(runId).event_count).toBe(journal.listEvents(runId).length)
  })

  it("does not accept a fence recorded for a different in-flight effect", async () => {
    const journal = open(twoStepRuntime)
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    const coordinator = new OwnershipCoordinator({
      journal,
      source_id: "runner-1",
      clock,
      bindings: { resolve: () => ({ capabilities }) },
    })
    coordinator.registerLease(ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB8", "runner-1", 1))
    let invocation = 0
    let markStarted = () => {}
    let releaseRequest = () => {}
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const delayed = new Promise<void>((resolve) => {
      releaseRequest = resolve
    })
    const dispatcher = new CommandDispatcher({
      journal,
      executor: {
        issue: async () => {
          invocation += 1
          if (invocation === 1) {
            return "completed"
          }
          markStarted()
          await delayed
          return "rejected"
        },
        reconcile: async () => "unknown",
      },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })
    await dispatcher.dispatchNext(runId)
    const secondDispatch = dispatcher.dispatchNext(runId)
    await started
    const effects = journal.listEffects(runId)
    const settled = effects.find((effect) => effect.status === "settled")
    const dispatching = effects.find((effect) => effect.status === "dispatching")
    if (!settled || !dispatching) {
      throw new Error("Expected settled and dispatching effects")
    }
    const handoffId = "hnd_01ARZ3NDEKTSV4RRFFQ69G5FB1" as const
    const sourceLeaseId = "lse_01ARZ3NDEKTSV4RRFFQ69G5FB8" as const
    const targetLease = ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB9", "edge-1", 2, "edge")
    const ownershipEvent = (
      eventId: EventId,
      type: string,
      payload: ProposedEvent["payload"],
      epoch = 1,
      source: ProposedEvent["source"] = { kind: "runner", source_id: "runner-1" },
    ): ProposedEvent => ({
      event_id: eventId,
      run_id: runId,
      type,
      schema_version: 1,
      source,
      ownership_epoch: epoch,
      payload,
    })

    journal.requestOwnershipHandoff(
      ownershipEvent("evt_01ARZ3NDEKTSV4RRFFQ69G5FC0", "ownership.handoff_requested", {
        handoff_id: handoffId,
        source_lease_id: sourceLeaseId,
        target_lease: targetLease,
      }),
    )
    journal.releaseOwnership(
      ownershipEvent("evt_01ARZ3NDEKTSV4RRFFQ69G5FC1", "ownership.released", {
        handoff_id: handoffId,
        lease_id: sourceLeaseId,
      }),
    )
    journal.recordOwnershipFence(
      ownershipEvent("evt_01ARZ3NDEKTSV4RRFFQ69G5FC2", "ownership.fence_activated", {
        handoff_id: handoffId,
        effect_id: settled.effect_id,
        proof: {
          adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          scope: "subject",
          scope_reference: "settled-effect",
          ownership_epoch: 2,
          activation_reference: "unrelated-fence",
          effective_at: "2026-01-01T00:00:02.000Z",
          maximum_activation_delay_ms: 100,
          evidence_digest: `sha256:${"b".repeat(64)}`,
        },
      }),
    )

    expect(() =>
      journal.activateOwnership(
        ownershipEvent(
          "evt_01ARZ3NDEKTSV4RRFFQ69G5FC3",
          "ownership.activated",
          { handoff_id: handoffId, lease: targetLease, resume_commands: [] },
          2,
          { kind: "edge", source_id: "edge-1" },
        ),
      ),
    ).toThrowError(expect.objectContaining({ code: "OWNERSHIP_HANDOFF_BLOCKED" }))
    expect(journal.getMission(runId).state.ownership_epoch).toBe(1)
    expect(dispatching.status).toBe("dispatching")
    releaseRequest()
    await secondDispatch
  })

  it("blocks takeover while a settle-first effect is still dispatching", async () => {
    const journal = open()
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    let releaseRequest = () => {}
    let markStarted = () => {}
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const delayed = new Promise<void>((resolve) => {
      releaseRequest = resolve
    })
    const coordinator = new OwnershipCoordinator({
      journal,
      source_id: "runner-1",
      clock,
      bindings: { resolve: () => ({ capabilities }) },
    })
    coordinator.registerLease(ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FAX", "runner-1", 1))
    const dispatcher = new CommandDispatcher({
      journal,
      executor: {
        issue: async () => {
          markStarted()
          await delayed
          return "uncertain"
        },
        reconcile: async () => "unknown",
      },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })

    const dispatch = dispatcher.dispatchNext(runId)
    await started
    const handoff = await coordinator.handoff({
      handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAW",
      target_lease: ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FAY", "edge-1", 2, "edge"),
    })

    expect(handoff).toMatchObject({
      status: "blocked",
      reason: "settle_required",
      handoff: { status: "blocked" },
    })
    expect(journal.getOwnership(runId)).toMatchObject({
      claims_frozen: true,
      active_lease: { lease: { owner_id: "runner-1", ownership_epoch: 1 } },
    })
    releaseRequest()
    await dispatch
  })

  it("resumes a released handoff after a future-effective fence is replaced", async () => {
    const journal = open()
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    let releaseRequest = () => {}
    let markStarted = () => {}
    let fenceAttempt = 0
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const delayed = new Promise<void>((resolve) => {
      releaseRequest = resolve
    })
    const coordinator = new OwnershipCoordinator({
      journal,
      source_id: "runner-1",
      clock,
      bindings: {
        resolve: () => ({
          capabilities: transferableCapabilities,
          adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          fence_scope_reference: "vehicle-7",
          activate_fence: async (request) => {
            fenceAttempt += 1
            return {
              adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
              scope: "subject",
              scope_reference: "vehicle-7",
              ownership_epoch: request.ownership_epoch,
              activation_reference: `fence-${fenceAttempt}`,
              effective_at: fenceAttempt === 1 ? "2026-01-01T00:00:03.000Z" : request.requested_at,
              maximum_activation_delay_ms: 1_000,
              evidence_digest: `sha256:${"a".repeat(64)}`,
            }
          },
        }),
      },
    })
    coordinator.registerLease(ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB3", "runner-1", 1))
    const dispatcher = new CommandDispatcher({
      journal,
      executor: {
        issue: async () => {
          markStarted()
          await delayed
          return "rejected"
        },
        reconcile: async () => "unknown",
      },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })
    const request = {
      handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAZ",
      target_lease: ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB4", "edge-1", 2, "edge"),
    } as const

    const dispatch = dispatcher.dispatchNext(runId)
    await started
    const blocked = await coordinator.handoff(request)
    const resumed = await coordinator.handoff(request)
    releaseRequest()
    await dispatch

    expect(blocked).toMatchObject({ status: "blocked", reason: "fence_activation_failed" })
    expect(resumed.status).toBe("active")
    expect(fenceAttempt).toBe(2)
    expect(
      journal.listEvents(runId).filter((event) => event.type === "ownership.handoff_requested"),
    ).toHaveLength(1)
  })

  it("records a confirmed cancel-first stop before activating ownership", async () => {
    const journal = open()
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    let markStarted = () => {}
    let releaseRequest = () => {}
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const delayed = new Promise<void>((resolve) => {
      releaseRequest = resolve
    })
    const cancel = vi.fn(async () => "confirmed_stopped" as const)
    const coordinator = new OwnershipCoordinator({
      journal,
      source_id: "runner-1",
      clock,
      bindings: {
        resolve: () => ({
          capabilities: { ...capabilities, handoff_mode: "cancel_first", cancellation: true },
          cancel,
        }),
      },
    })
    coordinator.registerLease(ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB5", "runner-1", 1))
    const dispatcher = new CommandDispatcher({
      journal,
      executor: {
        issue: async () => {
          markStarted()
          await delayed
          return "rejected"
        },
        reconcile: async () => "unknown",
      },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })
    const dispatch = dispatcher.dispatchNext(runId)
    await started

    const handoff = await coordinator.handoff({
      handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FB0",
      target_lease: ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB6", "edge-1", 2, "edge"),
    })

    expect(handoff.status).toBe("active")
    expect(cancel).toHaveBeenCalledOnce()
    expect(journal.listEffects(runId)[0]?.status).toBe("cancelled")
    expect(
      journal.listEvents(runId).some((event) => event.type === "ownership.effect_cancelled"),
    ).toBe(true)
    expect(journal.listCommands(runId).at(-1)).toMatchObject({
      kind: "effect.reconcile",
      ownership_epoch: 2,
      status: "pending",
    })
    const edge = new OwnershipCoordinator({
      journal,
      source_id: "edge-1",
      clock,
      bindings: { resolve: () => ({ capabilities }) },
    })
    const returned = await edge.handoff({
      handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FB7",
      target_lease: ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB8", "runner-2", 3),
    })
    expect(returned.status).toBe("active")
    expect(journal.listEffects(runId)[0]?.ownership_epoch).toBe(3)
    expect(journal.listCommands(runId).at(-1)).toMatchObject({
      kind: "effect.reconcile",
      ownership_epoch: 3,
      status: "pending",
    })
    releaseRequest()
    await dispatch
    expect(journal.verifyRun(runId).event_count).toBe(journal.listEvents(runId).length)
  })

  it("resumes a handoff after cancellation was persisted before activation", async () => {
    const journal = open()
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    let markStarted = () => {}
    let releaseRequest = () => {}
    const started = new Promise<void>((resolve) => {
      markStarted = resolve
    })
    const delayed = new Promise<void>((resolve) => {
      releaseRequest = resolve
    })
    const cancel = vi.fn(async () => "confirmed_stopped" as const)
    const coordinator = new OwnershipCoordinator({
      journal,
      source_id: "runner-1",
      clock,
      bindings: {
        resolve: () => ({
          capabilities: { ...capabilities, handoff_mode: "cancel_first", cancellation: true },
          cancel,
        }),
      },
    })
    coordinator.registerLease(ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FB9", "runner-1", 1))
    const dispatcher = new CommandDispatcher({
      journal,
      executor: {
        issue: async () => {
          markStarted()
          await delayed
          return "rejected"
        },
        reconcile: async () => "unknown",
      },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })
    const dispatch = dispatcher.dispatchNext(runId)
    await started
    const request = {
      handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FB8",
      target_lease: ownershipLease("lse_01ARZ3NDEKTSV4RRFFQ69G5FC0", "edge-1", 2, "edge"),
    } as const
    journal.requestOwnershipHandoff({
      event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC1",
      run_id: runId,
      type: "ownership.handoff_requested",
      schema_version: 1,
      source: { kind: "runner", source_id: "runner-1" },
      ownership_epoch: 1,
      payload: {
        handoff_id: request.handoff_id,
        source_lease_id: "lse_01ARZ3NDEKTSV4RRFFQ69G5FB9",
        target_lease: request.target_lease,
      },
    })
    await expect(
      coordinator.handoff({
        ...request,
        target_lease: { ...request.target_lease, not_after: "2026-01-03T00:00:00.000Z" },
      }),
    ).rejects.toThrow("different handoff")
    expect(cancel).not.toHaveBeenCalled()
    const effect = journal.listEffects(runId)[0]
    if (!effect) {
      throw new Error("Expected an in-flight effect")
    }
    journal.recordOwnershipCancellation({
      event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FC2",
      run_id: runId,
      type: "ownership.effect_cancelled",
      schema_version: 1,
      source: { kind: "runner", source_id: "runner-1" },
      ownership_epoch: 1,
      payload: {
        handoff_id: request.handoff_id,
        effect_id: effect.effect_id,
        result: "confirmed_stopped",
      },
    })

    const resumed = await coordinator.handoff(request)
    expect(resumed.status).toBe("active")
    expect(cancel).not.toHaveBeenCalled()
    expect(journal.getOwnership(runId).active_lease?.lease.owner_id).toBe("edge-1")
    releaseRequest()
    await dispatch
  })
})

describe("timer dispatcher", () => {
  it("uses virtual time to fire durable timers at their deadline", async () => {
    const journal = open()
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    const commandDispatcher = new CommandDispatcher({
      journal,
      executor: { issue: async () => "accepted", reconcile: async () => "unknown" },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })
    await commandDispatcher.dispatchNext(runId)
    const timer = journal.listTimers(runId)[0]
    if (!timer) {
      throw new Error("Expected a scheduled timer")
    }
    const timerDispatcher = new TimerDispatcher({
      journal,
      source_id: "timer-1",
      clock,
      identities: new FixedIdentities([], ["evt_01ARZ3NDEKTSV4RRFFQ69G5FB0"]),
    })

    expect(timerDispatcher.fireDue().results).toEqual([])
    clock.advanceTo(timer.due_at)
    const outcome = timerDispatcher.fireDue()

    expect(outcome.results).toHaveLength(1)
    expect(outcome.results[0]?.status).toBe("fired")
    expect(journal.listTimers(runId)[0]?.status).toBe("fired")
    expect(journal.listCommands(runId)[2]?.kind).toBe("effect.reconcile")
  })
})

describe("identity factory", () => {
  it("uses cryptographically secure contract-valid identities", () => {
    const factory = new SecureIdentityFactory()
    const attempts = new Set<AttemptId>()
    const events = new Set<EventId>()

    for (let index = 0; index < 64; index += 1) {
      attempts.add(factory.attemptId())
      events.add(factory.eventId())
    }

    expect(attempts.size).toBe(64)
    expect(events.size).toBe(64)
    expect([...attempts].every((value) => Schema.Check(AttemptIdSchema, value))).toBe(true)
    expect([...events].every((value) => Schema.Check(EventIdSchema, value))).toBe(true)
  })
})
