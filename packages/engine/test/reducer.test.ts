import type { AcceptedEvent, JsonObject, OperationCapabilities } from "@idemantis/contracts"
import {
  defineMission,
  defineOperation,
  effectState,
  finalState,
  fromInput,
  mapFields,
  reconcileState,
  safeRetry,
  waitForOperator,
  waitForObservation,
} from "@idemantis/sdk"
import Type from "typebox"
import { describe, expect, it } from "vitest"
import {
  ReductionError,
  evaluateExpression,
  reduceMission,
  replayMission,
  type MissionRuntimeDefinition,
  type ReducerEventType,
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
  id: "vehicle-delivery",
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

const runtime: MissionRuntimeDefinition = {
  manifest: mission.manifest,
  definition_digest: mission.definitionDigest,
  execution_binding_digest: `sha256:${"b".repeat(64)}`,
}

const sequentialObservationMission = defineMission({
  id: "vehicle-pickup-delivery",
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
      onAccepted: "waitForPickup",
      onSettled: "waitForPickup",
      onFailed: "failed",
      onUncertain: "failed",
    }),
    waitForPickup: waitForObservation({
      observationType: "vehicle.pickup_confirmed",
      schemaVersion: 1,
      correlation: { kind: "effect", effectState: "dispatchVehicle" },
      timeout: { afterMs: 1_000, onTimeout: "failed" },
      onObserved: "waitForArrival",
    }),
    waitForArrival: waitForObservation({
      observationType: "vehicle.arrived",
      schemaVersion: 1,
      correlation: { kind: "effect", effectState: "dispatchVehicle" },
      timeout: { afterMs: 1_000, onTimeout: "failed" },
      onObserved: "complete",
    }),
    complete: finalState("completed"),
    failed: finalState("failed"),
  },
})

const sequentialObservationRuntime: MissionRuntimeDefinition = {
  manifest: sequentialObservationMission.manifest,
  definition_digest: sequentialObservationMission.definitionDigest,
  execution_binding_digest: `sha256:${"d".repeat(64)}`,
}

const subjectMission = defineMission({
  id: "subject-wait",
  version: "1.0.0",
  input: Type.Object({ vehicle: Type.String() }, { additionalProperties: false }),
  initial: "waitForSignal",
  states: {
    waitForSignal: waitForObservation({
      observationType: "vehicle.ready",
      schemaVersion: 1,
      correlation: { kind: "subject", subject: fromInput("vehicle") },
      timeout: { afterMs: 1_000, onTimeout: "unresolved" },
      onObserved: "complete",
    }),
    complete: finalState("completed"),
    unresolved: finalState("unresolved"),
  },
})

const subjectRuntime: MissionRuntimeDefinition = {
  manifest: subjectMission.manifest,
  definition_digest: subjectMission.definitionDigest,
  execution_binding_digest: `sha256:${"c".repeat(64)}`,
}

const operatorMission = defineMission({
  id: "operator-review",
  version: "1.0.0",
  input: Type.Object({ release: Type.String() }, { additionalProperties: false }),
  initial: "review",
  states: {
    review: waitForOperator({
      reason: "Release requires approval",
      actions: [
        {
          code: "approve",
          consequence: "irreversible",
          confirmationRequired: true,
          input: Type.Object(
            { evidence: Type.String({ minLength: 3 }) },
            { additionalProperties: false },
          ),
          presentationHints: [
            { path: "evidence", control: "textarea", label: "Evidence", order: 0 },
          ],
          evidenceRequired: true,
          reasonRequired: true,
          expectedOutcomeLabel: "Release is approved",
          onAccepted: "complete",
        },
      ],
    }),
    complete: finalState("completed"),
  },
})

const operatorRuntime: MissionRuntimeDefinition = {
  manifest: operatorMission.manifest,
  definition_digest: operatorMission.definitionDigest,
  execution_binding_digest: `sha256:${"d".repeat(64)}`,
}

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const attemptId = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const commandId = "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const eventSuffixes = "VWXYZ0123456789ABCDEFGHJKMNPQRST"

function acceptedEvent(
  sequence: number,
  type: ReducerEventType,
  payload: JsonObject,
  ownershipEpoch = 1,
): AcceptedEvent {
  return {
    event_id: `evt_01ARZ3NDEKTSV4RRFFQ69G5FA${eventSuffixes[sequence - 1]}`,
    run_id: runId,
    type,
    schema_version: 1,
    source: { kind: "system", source_id: "engine-test" },
    ownership_epoch: ownershipEpoch,
    payload,
    sequence,
    recorded_at: new Date(Date.UTC(2026, 0, 1, 0, 0, sequence)).toISOString(),
  }
}

function admission(sequence = 1): AcceptedEvent {
  return acceptedEvent(sequence, "mission.admitted", {
    definition_digest: runtime.definition_digest,
    execution_binding_digest: runtime.execution_binding_digest,
    input: { route: "north", vehicle: "vehicle-7" },
  })
}

function admitted() {
  return reduceMission(runtime, null, admission())
}

function issueResult(
  sequence: number,
  effectId: string,
  result: "not_sent" | "accepted" | "completed" | "rejected" | "failed" | "uncertain",
): AcceptedEvent {
  return acceptedEvent(sequence, "effect.issue_result_recorded", {
    attempt_id: attemptId,
    command_id: commandId,
    effect_id: effectId,
    result,
  })
}

describe("deterministic reducer", () => {
  it("admits a mission and emits one stable effect command", () => {
    const result = admitted()
    const effectId = result.state.active_effect_id

    expect(effectId).toMatch(/^eff_[0-9A-HJKMNP-TV-Z]{26}$/)
    expect(result.state.status).toBe("running")
    expect(result.state.current_state).toBe("dispatchVehicle")
    expect(result.state.effects[effectId as string]?.issue_count).toBe(1)
    expect(result.commands).toEqual([
      expect.objectContaining({
        kind: "effect.issue",
        command_slot: "effect.dispatch.issue.1",
        payload: {
          effect_id: effectId,
          operation: "vehicle.dispatch",
          input: { route: "north" },
        },
      }),
    ])
    expect(result.state_digest).toMatch(/^sha256:[a-f0-9]{64}$/)
    expect(Object.isFrozen(result)).toBe(true)
    expect(Object.isFrozen(result.state)).toBe(true)
  })

  it("applies only a bound operator intervention at the current sequence", () => {
    const first = reduceMission(operatorRuntime, null, {
      ...admission(),
      payload: {
        definition_digest: operatorRuntime.definition_digest,
        execution_binding_digest: operatorRuntime.execution_binding_digest,
        input: { release: "release-47" },
      },
    })
    expect(first.state).toMatchObject({ status: "paused", current_state: "review" })
    const intervention = {
      ...acceptedEvent(2, "operator.intervention_submitted", {
        client_request_id: "engine-intervention-0001",
        expected_sequence: 1,
        action_code: "approve",
        target: `mission:${runId}`,
        input: { evidence: "Reviewed deployment record" },
        reason: "Evidence matches the release",
      }),
      source: { kind: "operator" as const, source_id: "operator-1" },
    }
    const accepted = reduceMission(operatorRuntime, first.state, intervention)

    expect(accepted.state).toMatchObject({
      status: "completed",
      current_state: "complete",
      current_sequence: 2,
      terminal_outcome: "completed",
    })
    expect(accepted.commands).toEqual([])
    expect(() =>
      reduceMission(operatorRuntime, first.state, {
        ...intervention,
        source: { kind: "system", source_id: "forged-operator" },
      }),
    ).toThrowError(ReductionError)
    expect(() =>
      reduceMission(operatorRuntime, first.state, {
        ...intervention,
        payload: { ...intervention.payload, expected_sequence: 2 },
      }),
    ).toThrowError(ReductionError)
  })

  it("separates acknowledgement from observed completion", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const acknowledged = reduceMission(runtime, first.state, issueResult(2, effectId, "accepted"))

    expect(acknowledged.state.status).toBe("waiting")
    expect(acknowledged.state.current_state).toBe("waitForArrival")
    expect(acknowledged.state.effects[effectId]?.status).toBe("acknowledged")
    expect(acknowledged.commands[0]).toMatchObject({ kind: "timer.schedule" })

    const observed = reduceMission(
      runtime,
      acknowledged.state,
      acceptedEvent(3, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        effect_id: effectId,
        observation_type: "vehicle.arrived",
        payload: { position: "dock" },
      }),
    )

    expect(observed.state.status).toBe("completed")
    expect(observed.state.current_state).toBe("complete")
    expect(observed.state.effects[effectId]?.status).toBe("settled")
    expect(observed.commands).toEqual([])
  })

  it("accepts a correlated observation that arrives before acknowledgement", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const observed = reduceMission(
      runtime,
      first.state,
      acceptedEvent(2, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAW",
        effect_id: effectId,
        observation_type: "vehicle.arrived",
        payload: {},
      }),
    )
    const lateAcknowledgement = reduceMission(
      runtime,
      observed.state,
      issueResult(3, effectId, "accepted"),
    )

    expect(observed.state.status).toBe("completed")
    expect(lateAcknowledgement.state.status).toBe("completed")
    expect(lateAcknowledgement.state.current_sequence).toBe(3)
    expect(lateAcknowledgement.commands).toEqual([])
  })

  it("retains effect correlation across sequential observation waits", () => {
    const first = reduceMission(
      sequentialObservationRuntime,
      null,
      acceptedEvent(1, "mission.admitted", {
        definition_digest: sequentialObservationRuntime.definition_digest,
        execution_binding_digest: sequentialObservationRuntime.execution_binding_digest,
        input: { route: "dock-a", vehicle: "vehicle-7" },
      }),
    )
    const effectId = first.state.active_effect_id as string
    const acknowledged = reduceMission(
      sequentialObservationRuntime,
      first.state,
      issueResult(2, effectId, "accepted"),
    )
    const pickedUp = reduceMission(
      sequentialObservationRuntime,
      acknowledged.state,
      acceptedEvent(3, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FBA",
        effect_id: effectId,
        observation_type: "vehicle.pickup_confirmed",
        payload: {},
      }),
    )
    const arrived = reduceMission(
      sequentialObservationRuntime,
      pickedUp.state,
      acceptedEvent(4, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FBB",
        effect_id: effectId,
        observation_type: "vehicle.arrived",
        payload: {},
      }),
    )

    expect(pickedUp.state.current_state).toBe("waitForArrival")
    expect(pickedUp.state.active_effect_id).toBe(effectId)
    expect(arrived.state.current_state).toBe("complete")
    expect(arrived.state.status).toBe("completed")
  })

  it("retains effect correlation when completion enters an observation wait", () => {
    const first = reduceMission(
      sequentialObservationRuntime,
      null,
      acceptedEvent(1, "mission.admitted", {
        definition_digest: sequentialObservationRuntime.definition_digest,
        execution_binding_digest: sequentialObservationRuntime.execution_binding_digest,
        input: { route: "dock-a", vehicle: "vehicle-7" },
      }),
    )
    const effectId = first.state.active_effect_id as string
    const completed = reduceMission(
      sequentialObservationRuntime,
      first.state,
      issueResult(2, effectId, "completed"),
    )
    const observed = reduceMission(
      sequentialObservationRuntime,
      completed.state,
      acceptedEvent(3, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FBC",
        effect_id: effectId,
        observation_type: "vehicle.pickup_confirmed",
        payload: {},
      }),
    )

    expect(completed.state.current_state).toBe("waitForPickup")
    expect(completed.state.active_effect_id).toBe(effectId)
    expect(observed.state.current_state).toBe("waitForArrival")
  })

  it("settles a subject-correlated wait without an effect identity", () => {
    const admittedWait = reduceMission(
      subjectRuntime,
      null,
      acceptedEvent(1, "mission.admitted", {
        definition_digest: subjectRuntime.definition_digest,
        execution_binding_digest: subjectRuntime.execution_binding_digest,
        input: { vehicle: "vehicle-7" },
      }),
    )
    const observed = reduceMission(
      subjectRuntime,
      admittedWait.state,
      acceptedEvent(2, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAY",
        observation_type: "vehicle.ready",
        subject: "vehicle-7",
        payload: {},
      }),
    )

    expect(admittedWait.state.status).toBe("waiting")
    expect(observed.state.status).toBe("completed")
  })

  it("schedules bounded backoff and retains one logical effect across retries", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const failedAttempt = reduceMission(runtime, first.state, issueResult(2, effectId, "not_sent"))
    const retryTimer = failedAttempt.commands[0]

    expect(retryTimer).toMatchObject({ kind: "timer.schedule" })
    expect(failedAttempt.state.effects[effectId]?.issue_count).toBe(1)

    const { timer_id: timerId, state_instance_id: stateInstanceId } = retryTimer?.payload ?? {}
    if (typeof timerId !== "string" || typeof stateInstanceId !== "string") {
      throw new Error("Expected a retry timer command")
    }

    const retried = reduceMission(
      runtime,
      failedAttempt.state,
      acceptedEvent(3, "timer.fired", {
        timer_id: timerId,
        state_instance_id: stateInstanceId,
      }),
    )

    expect(retried.commands[0]).toMatchObject({
      kind: "effect.issue",
      payload: { effect_id: effectId },
    })
    expect(retried.state.effects[effectId]?.issue_count).toBe(2)

    const exhausted = reduceMission(runtime, retried.state, issueResult(4, effectId, "failed"))
    expect(exhausted.state.status).toBe("failed")
    expect(exhausted.commands).toEqual([])
  })

  it("ignores retry timers that were never scheduled", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const effect = first.state.effects[effectId]
    if (!effect) {
      throw new Error("Expected an admitted effect")
    }

    const ignored = reduceMission(
      runtime,
      first.state,
      acceptedEvent(2, "timer.fired", {
        timer_id: "tmr_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        state_instance_id: effect.state_instance_id,
      }),
    )

    expect(ignored.state.effects[effectId]?.issue_count).toBe(1)
    expect(ignored.commands).toEqual([])
  })

  it("reconciles uncertainty before retrying the same logical effect", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const uncertain = reduceMission(runtime, first.state, issueResult(2, effectId, "uncertain"))

    expect(uncertain.state.status).toBe("reconciling")
    expect(uncertain.commands[0]).toMatchObject({
      kind: "effect.reconcile",
      payload: { effect_id: effectId, subject: "vehicle-7" },
    })

    const confirmedAbsent = reduceMission(
      runtime,
      uncertain.state,
      acceptedEvent(3, "effect.reconciliation_result_recorded", {
        attempt_id: attemptId,
        command_id: commandId,
        effect_id: effectId,
        result: "confirmed_not_applied",
      }),
    )

    expect(confirmedAbsent.state.current_state).toBe("dispatchVehicle")
    expect(confirmedAbsent.state.active_effect_id).toBe(effectId)
    expect(confirmedAbsent.state.effects[effectId]?.issue_count).toBe(1)
    expect(confirmedAbsent.commands[0]).toMatchObject({ kind: "timer.schedule" })
  })

  it("accepts a matching observation while reconciliation is in progress", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const uncertain = reduceMission(runtime, first.state, issueResult(2, effectId, "uncertain"))
    const observed = reduceMission(
      runtime,
      uncertain.state,
      acceptedEvent(3, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAZ",
        effect_id: effectId,
        observation_type: "vehicle.arrived",
        payload: {},
      }),
    )

    expect(observed.state.status).toBe("completed")
    expect(observed.state.effects[effectId]?.status).toBe("settled")
    expect(observed.commands).toEqual([])
  })

  it("replays the same journal into identical state and command history", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const events = [
      admission(),
      issueResult(2, effectId, "accepted"),
      acceptedEvent(3, "observation.recorded", {
        observation_id: "obs_01ARZ3NDEKTSV4RRFFQ69G5FAX",
        effect_id: effectId,
        observation_type: "vehicle.arrived",
        payload: { position: "dock" },
      }),
    ]

    const firstReplay = replayMission(runtime, events)
    const secondReplay = replayMission(runtime, structuredClone(events))

    expect(secondReplay).toEqual(firstReplay)
    expect(secondReplay.state_digest).toBe(firstReplay.state_digest)
    expect(secondReplay.commands.map((command) => command.command_id)).toEqual(
      firstReplay.commands.map((command) => command.command_id),
    )
  })

  it("does not mutate prior state or accepted events", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const previousState = structuredClone(first.state)
    const event = issueResult(2, effectId, "accepted")
    const previousEvent = structuredClone(event)

    reduceMission(runtime, first.state, event)

    expect(first.state).toEqual(previousState)
    expect(event).toEqual(previousEvent)
  })

  it("ignores stale epochs while preserving journal sequence", () => {
    const first = admitted()
    const stale = reduceMission(
      runtime,
      first.state,
      acceptedEvent(
        2,
        "effect.dispatch_started",
        {
          attempt_id: attemptId,
          command_id: commandId,
          effect_id: first.state.active_effect_id,
        },
        0,
      ),
    )

    expect(stale.state.current_sequence).toBe(2)
    expect(stale.state.effects[first.state.active_effect_id as string]?.status).toBe("prepared")
    expect(stale.commands).toEqual([])
  })

  it("does not regress an acknowledged effect on a late dispatch fact", () => {
    const first = admitted()
    const effectId = first.state.active_effect_id as string
    const acknowledged = reduceMission(runtime, first.state, issueResult(2, effectId, "accepted"))
    const lateDispatch = reduceMission(
      runtime,
      acknowledged.state,
      acceptedEvent(3, "effect.dispatch_started", {
        attempt_id: attemptId,
        command_id: commandId,
        effect_id: effectId,
      }),
    )

    expect(lateDispatch.state.effects[effectId]?.status).toBe("acknowledged")
    expect(lateDispatch.state.current_state).toBe("waitForArrival")
  })

  it("rejects malformed sequences, payloads, and empty replay", () => {
    const first = admitted()

    expect(() =>
      reduceMission(
        runtime,
        first.state,
        acceptedEvent(4, "effect.dispatch_started", {
          attempt_id: attemptId,
          command_id: commandId,
          effect_id: first.state.active_effect_id,
        }),
      ),
    ).toThrowError(ReductionError)

    expect(() =>
      reduceMission(
        runtime,
        first.state,
        acceptedEvent(2, "effect.issue_result_recorded", { result: "accepted" }),
      ),
    ).toThrowError(expect.objectContaining({ code: "INVALID_EVENT_PAYLOAD" }))
    expect(() => replayMission(runtime, [])).toThrowError(
      expect.objectContaining({ code: "EMPTY_REPLAY" }),
    )
  })

  it("advances ownership by one epoch and ignores delayed stale results", () => {
    const first = admitted()
    const lease = {
      lease_id: "lse_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      run_id: runId,
      owner_mode: "runner",
      owner_id: "runner-1",
      ownership_epoch: 1,
      branch_anchor_sequence: 1,
      not_before: "2026-01-01T00:00:00.000Z",
      not_after: "2026-01-02T00:00:00.000Z",
      maximum_offline_duration_ms: 60_000,
      allowed_clock_uncertainty_ms: 100,
      delegation_digest: `sha256:${"d".repeat(64)}`,
    } as const
    const registered = reduceMission(
      runtime,
      first.state,
      acceptedEvent(2, "ownership.lease_registered", { lease }),
    )
    expect(() =>
      reduceMission(
        runtime,
        registered.state,
        acceptedEvent(3, "ownership.effect_cancelled", {
          handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          effect_id: registered.state.active_effect_id,
          result: "confirmed_stopped",
        }),
      ),
    ).toThrowError(expect.objectContaining({ code: "INVALID_TRANSITION" }))
    const started = reduceMission(
      runtime,
      registered.state,
      acceptedEvent(3, "effect.dispatch_started", {
        attempt_id: attemptId,
        command_id: commandId,
        effect_id: registered.state.active_effect_id,
      }),
    )
    const cancelled = reduceMission(
      runtime,
      started.state,
      acceptedEvent(4, "ownership.effect_cancelled", {
        handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        effect_id: started.state.active_effect_id,
        result: "confirmed_stopped",
      }),
    )
    expect(cancelled.state.current_state).toBe("reconcileVehicle")
    expect(cancelled.state.effects[started.state.active_effect_id as string]?.status).toBe(
      "cancelled",
    )
    expect(cancelled.commands[0]?.kind).toBe("effect.reconcile")
    const fenced = reduceMission(
      runtime,
      started.state,
      acceptedEvent(4, "ownership.fence_activated", {
        handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAW",
        effect_id: started.state.active_effect_id,
        proof: {
          adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          scope: "subject",
          scope_reference: "vehicle-7",
          ownership_epoch: 2,
          activation_reference: "fence-2",
          effective_at: "2026-01-01T00:00:04.000Z",
          maximum_activation_delay_ms: 100,
          evidence_digest: `sha256:${"f".repeat(64)}`,
        },
      }),
    )
    expect(fenced.state.current_state).toBe("reconcileVehicle")
    expect(fenced.state.effects[started.state.active_effect_id as string]?.status).toBe("fenced")
    expect(fenced.commands[0]?.kind).toBe("effect.reconcile")
    const staleAfterFence = reduceMission(
      runtime,
      fenced.state,
      issueResult(5, started.state.active_effect_id as string, "rejected"),
    )
    expect(staleAfterFence.state.current_state).toBe("reconcileVehicle")
    expect(staleAfterFence.state.effects[started.state.active_effect_id as string]?.status).toBe(
      "fenced",
    )
    expect(staleAfterFence.commands).toEqual([])
    const pendingCommand = first.commands[0]
    if (pendingCommand?.kind !== "effect.issue") {
      throw new Error("Expected pending ownership command")
    }
    const activated = reduceMission(
      runtime,
      registered.state,
      acceptedEvent(
        3,
        "ownership.activated",
        {
          handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          lease: {
            ...lease,
            lease_id: "lse_01ARZ3NDEKTSV4RRFFQ69G5FAW",
            owner_mode: "edge",
            owner_id: "edge-1",
            ownership_epoch: 2,
            branch_anchor_sequence: 2,
          },
          resume_commands: [
            {
              previous_command_id: pendingCommand.command_id,
              kind: pendingCommand.kind,
              payload: pendingCommand.payload,
            },
          ],
        },
        2,
      ),
    )
    const effectId = activated.state.active_effect_id as string
    const delayed = reduceMission(runtime, activated.state, issueResult(4, effectId, "rejected"))

    expect(activated.state.ownership_epoch).toBe(2)
    expect(activated.commands).toEqual([
      expect.objectContaining({
        kind: "effect.issue",
        ownership_epoch: 2,
        payload: pendingCommand.payload,
      }),
    ])
    expect(activated.state.effects[effectId]?.ownership_epoch).toBe(2)
    expect(delayed.state.ownership_epoch).toBe(2)
    expect(delayed.state.current_sequence).toBe(4)
    expect(delayed.state.effects[effectId]?.status).toBe("prepared")
    expect(() =>
      reduceMission(
        runtime,
        delayed.state,
        acceptedEvent(
          5,
          "ownership.activated",
          {
            handoff_id: "hnd_01ARZ3NDEKTSV4RRFFQ69G5FAW",
            lease: { ...lease, ownership_epoch: 4 },
            resume_commands: [],
          },
          4,
        ),
      ),
    ).toThrowError(expect.objectContaining({ code: "OWNERSHIP_EPOCH_MISMATCH" }))
  })

  it("treats prototype-shaped expression keys as JSON data", () => {
    const input = JSON.parse('{"__proto__":{"source":"input"}}') as JsonObject
    const fields = JSON.parse('{"__proto__":{"kind":"literal","value":{"safe":true}}}') as Record<
      string,
      ReturnType<typeof fromInput>
    >
    const value = evaluateExpression({ kind: "object", fields }, input)

    expect(evaluateExpression(fromInput("__proto__"), input)).toEqual({ source: "input" })
    expect(Object.hasOwn(value as JsonObject, "__proto__")).toBe(true)
    expect(Object.getOwnPropertyDescriptor(value, "__proto__")?.value).toEqual({ safe: true })
    expect(Object.getPrototypeOf(value)).toBeNull()
    expect(Object.getPrototypeOf({})).toBe(Object.prototype)
  })
})
