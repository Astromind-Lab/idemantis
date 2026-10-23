import {
  AcceptedEventSchema,
  CommandRecordSchema,
  DigestSchema,
  canonicalJson,
  sha256Digest,
  type AcceptedEvent,
  type CommandKind,
  type CommandRecord,
  type EffectId,
  type JsonObject,
  type JsonValue,
} from "@idemantis/contracts"
import {
  MissionManifestSchema,
  type EffectStateManifest,
  type MissionManifest,
  type ObservationWaitStateManifest,
  type StateManifest,
} from "@idemantis/sdk"
import Schema from "typebox/schema"
import { ReductionError } from "./errors.js"
import { parseReducerEvent } from "./events.js"
import { evaluateExpression } from "./expression.js"
import {
  deriveCommandId,
  deriveEffectId,
  deriveRetryTimerId,
  deriveStateInstanceId,
  deriveTimerId,
} from "./ids.js"
import { MissionStateSchema } from "./schema.js"
import type {
  EffectIssueResultPayload,
  EffectProjection,
  EffectReconciliationResultPayload,
  MissionRuntimeDefinition,
  MissionState,
  ObservationRecordedPayload,
  ReductionResult,
  TimerFiredPayload,
} from "./types.js"

const acceptedEventValidator = Schema.Compile(AcceptedEventSchema)
const missionStateValidator = Schema.Compile(MissionStateSchema)
const commandValidator = Schema.Compile(CommandRecordSchema)
const manifestValidator = Schema.Compile(MissionManifestSchema)

function fail(
  code: ConstructorParameters<typeof ReductionError>[0],
  message: string,
  event?: AcceptedEvent,
): never {
  throw new ReductionError(code, message, event?.event_id)
}

function cloneState(state: MissionState): MissionState {
  return structuredClone(state)
}

function freeze<Value>(value: Value): Value {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) {
      freeze(child)
    }
    Object.freeze(value)
  }

  return value
}

function stateManifest(manifest: MissionManifest, stateName: string): StateManifest {
  const state = Object.hasOwn(manifest.states, stateName) ? manifest.states[stateName] : undefined

  if (!state) {
    fail("INVALID_DEFINITION", `Manifest state ${stateName} does not exist`)
  }

  return state
}

function effectProjection(state: MissionState, effectId: EffectId): EffectProjection {
  const effect = state.effects[effectId]

  if (!effect) {
    fail("INVALID_TRANSITION", `Effect ${effectId} does not exist`)
  }

  return effect
}

function validateDefinition(definition: MissionRuntimeDefinition): void {
  if (
    !manifestValidator.Check(definition.manifest) ||
    !Schema.Check(DigestSchema, definition.definition_digest) ||
    !Schema.Check(DigestSchema, definition.execution_binding_digest)
  ) {
    fail("INVALID_DEFINITION", "Runtime definition does not satisfy its public contracts")
  }

  const actualDigest = sha256Digest(definition.manifest as unknown as JsonObject)
  if (actualDigest !== definition.definition_digest) {
    fail("INVALID_DEFINITION", "Runtime definition digest does not match its manifest")
  }
}

function createCommand(
  event: AcceptedEvent,
  state: MissionState,
  commandSlot: string,
  kind: CommandKind,
  payload: JsonObject,
): CommandRecord {
  return {
    command_id: deriveCommandId(event.event_id, commandSlot),
    run_id: state.run_id,
    causing_event_id: event.event_id,
    command_slot: commandSlot,
    kind,
    status: "pending",
    ownership_epoch: state.ownership_epoch,
    payload,
    created_at: event.recorded_at,
  }
}

function operationInput(
  manifest: MissionManifest,
  operationKey: string,
  expression: EffectStateManifest["input"],
  input: JsonObject,
): JsonValue {
  const operation = Object.hasOwn(manifest.operations, operationKey)
    ? manifest.operations[operationKey]
    : undefined

  if (!operation) {
    fail("INVALID_DEFINITION", `Operation ${operationKey} does not exist`)
  }

  const value = evaluateExpression(expression, input)
  if (!Schema.Check(operation.input_schema as never, value)) {
    fail("INVALID_TRANSITION", `Input for operation ${operationKey} does not satisfy its schema`)
  }

  return value
}

function issueEffect(
  definition: MissionRuntimeDefinition,
  state: MissionState,
  event: AcceptedEvent,
  effectState: EffectStateManifest,
  effect: EffectProjection,
): CommandRecord {
  effect.retry_timer_id = null
  effect.issue_count += 1
  effect.status = "prepared"
  const input = operationInput(
    definition.manifest,
    effectState.operation,
    effectState.input,
    state.input,
  )
  const slot = `effect.${effect.effect_slot}.issue.${effect.issue_count}`

  return createCommand(event, state, slot, "effect.issue", {
    effect_id: effect.effect_id,
    operation: effect.operation,
    input,
  })
}

function dueAt(recordedAt: string, delayMs: number, event: AcceptedEvent): string {
  const instant = Date.parse(recordedAt) + delayMs

  if (!Number.isFinite(instant)) {
    fail("INVALID_TRANSITION", "Timer deadline exceeds the supported date range", event)
  }

  return new Date(instant).toISOString()
}

function enterState(
  definition: MissionRuntimeDefinition,
  state: MissionState,
  target: string,
  event: AcceptedEvent,
  activeEffectId: EffectId | null,
  reuseEffectId?: EffectId,
  deferReusedEffectIssue = false,
): CommandRecord[] {
  const manifestState = stateManifest(definition.manifest, target)
  const stateInstanceId = deriveStateInstanceId(state.run_id, target, event.event_id)
  state.current_state = target
  state.current_state_instance_id = stateInstanceId
  state.terminal_outcome = null

  switch (manifestState.kind) {
    case "effect": {
      let effect: EffectProjection

      if (reuseEffectId) {
        effect = effectProjection(state, reuseEffectId)
        if (effect.state_name !== target) {
          fail(
            "INVALID_TRANSITION",
            `Effect ${reuseEffectId} belongs to ${effect.state_name}, not ${target}`,
            event,
          )
        }
      } else {
        const effectId = deriveEffectId(state.run_id, stateInstanceId, manifestState.effect_slot)
        effect = {
          effect_id: effectId,
          state_instance_id: stateInstanceId,
          state_name: target,
          effect_slot: manifestState.effect_slot,
          operation: manifestState.operation,
          status: "declared",
          required: manifestState.required,
          issue_count: 0,
          retry_timer_id: null,
          ownership_epoch: state.ownership_epoch,
        }
        state.effects[effectId] = effect
      }

      state.status = "running"
      state.active_effect_id = effect.effect_id
      if (reuseEffectId && deferReusedEffectIssue) {
        return []
      }
      return [issueEffect(definition, state, event, manifestState, effect)]
    }
    case "observation_wait": {
      state.status = "waiting"
      state.active_effect_id = activeEffectId
      const timerId = deriveTimerId(state.run_id, stateInstanceId)
      return [
        createCommand(event, state, `state.${target}.timeout`, "timer.schedule", {
          timer_id: timerId,
          state_instance_id: stateInstanceId,
          due_at: dueAt(event.recorded_at, manifestState.timeout.after_ms, event),
          purpose: `state.${target}.timeout`,
        }),
      ]
    }
    case "reconcile": {
      if (!activeEffectId) {
        fail("INVALID_TRANSITION", `Reconciliation state ${target} has no active effect`, event)
      }

      const effect = effectProjection(state, activeEffectId)
      if (effect.state_name !== manifestState.effect_state) {
        fail(
          "INVALID_TRANSITION",
          `Reconciliation state ${target} targets ${manifestState.effect_state}, not ${effect.state_name}`,
          event,
        )
      }

      state.status = "reconciling"
      state.active_effect_id = activeEffectId
      return [
        createCommand(event, state, `state.${target}.reconcile`, "effect.reconcile", {
          effect_id: activeEffectId,
          operation: manifestState.operation,
          subject: evaluateExpression(manifestState.subject, state.input),
        }),
      ]
    }
    case "final":
      state.status = manifestState.outcome
      state.active_effect_id = null
      state.terminal_outcome = manifestState.outcome
      return []
  }
}

function finish(
  state: MissionState,
  commands: CommandRecord[],
  event: AcceptedEvent,
): ReductionResult {
  if (!missionStateValidator.Check(state)) {
    fail("INVALID_REDUCTION_OUTPUT", "Reducer emitted an invalid mission projection", event)
  }

  for (const command of commands) {
    if (!commandValidator.Check(command)) {
      fail("INVALID_REDUCTION_OUTPUT", "Reducer emitted an invalid command", event)
    }
  }

  const result = {
    state,
    commands,
    state_digest: sha256Digest(state as unknown as JsonObject),
  }

  return freeze(result)
}

function retryAvailable(effectState: EffectStateManifest, effect: EffectProjection): boolean {
  return effectState.retry.kind !== "none" && effect.issue_count < effectState.retry.max_attempts
}

function retryEffect(
  definition: MissionRuntimeDefinition,
  state: MissionState,
  event: AcceptedEvent,
  effectState: EffectStateManifest,
  effect: EffectProjection,
): CommandRecord[] {
  if (effectState.retry.kind === "none") {
    return []
  }

  if (effectState.retry.backoff_ms === 0) {
    return [issueEffect(definition, state, event, effectState, effect)]
  }

  const timerId = deriveRetryTimerId(state.run_id, effect.state_instance_id, effect.issue_count)
  effect.status = "prepared"
  effect.retry_timer_id = timerId

  return [
    createCommand(
      event,
      state,
      `effect.${effect.effect_slot}.retry.${effect.issue_count}`,
      "timer.schedule",
      {
        timer_id: timerId,
        state_instance_id: effect.state_instance_id,
        due_at: dueAt(event.recorded_at, effectState.retry.backoff_ms, event),
        purpose: `effect.${effect.effect_slot}.retry`,
      },
    ),
  ]
}

function reduceIssueResult(
  definition: MissionRuntimeDefinition,
  state: MissionState,
  event: AcceptedEvent,
  payload: EffectIssueResultPayload,
): CommandRecord[] {
  if (state.active_effect_id !== payload.effect_id) {
    return []
  }

  const effect = effectProjection(state, payload.effect_id)
  const effectState = stateManifest(definition.manifest, effect.state_name)
  if (effectState.kind !== "effect") {
    fail("INVALID_DEFINITION", `Effect ${effect.effect_id} points to a non-effect state`, event)
  }

  const current = stateManifest(definition.manifest, state.current_state)
  const isOrigin = state.current_state === effect.state_name
  const isAcknowledgedWait =
    current.kind === "observation_wait" && effectState.on_acknowledged === state.current_state
  const isReconciliation =
    current.kind === "reconcile" && current.effect_state === effect.state_name

  if (!isOrigin && !isAcknowledgedWait && !isReconciliation) {
    return []
  }

  switch (payload.result) {
    case "accepted":
      if (!isOrigin) {
        return []
      }
      effect.retry_timer_id = null
      effect.status = "acknowledged"
      return effectState.on_acknowledged
        ? enterState(definition, state, effectState.on_acknowledged, event, effect.effect_id)
        : []
    case "completed":
      effect.retry_timer_id = null
      effect.status = "settled"
      if (effectState.on_settled === state.current_state) {
        return []
      }
      return enterState(definition, state, effectState.on_settled, event, null)
    case "uncertain":
      effect.retry_timer_id = null
      effect.status = "uncertain"
      if (effectState.on_uncertain === state.current_state) {
        return []
      }
      return enterState(definition, state, effectState.on_uncertain, event, effect.effect_id)
    case "not_sent":
    case "rejected":
    case "failed":
      if (isOrigin && retryAvailable(effectState, effect)) {
        return retryEffect(definition, state, event, effectState, effect)
      }
      effect.retry_timer_id = null
      effect.status = payload.result === "rejected" ? "rejected" : "failed"
      return enterState(definition, state, effectState.on_failed, event, null)
  }
}

function observationMatches(
  waitState: ObservationWaitStateManifest,
  effect: EffectProjection | undefined,
  payload: ObservationRecordedPayload,
  input: JsonObject,
): boolean {
  if (payload.observation_type !== waitState.observation_type) {
    return false
  }

  if (waitState.correlation.kind === "effect") {
    return effect?.state_name === waitState.correlation.effectState
  }

  if (payload.subject === undefined) {
    return false
  }

  return (
    canonicalJson(payload.subject) ===
    canonicalJson(evaluateExpression(waitState.correlation.subject, input))
  )
}

function observationWaitForCurrentEffect(
  manifest: MissionManifest,
  state: MissionState,
  effect: EffectProjection | undefined,
): ObservationWaitStateManifest | undefined {
  const current = stateManifest(manifest, state.current_state)

  if (current.kind === "observation_wait") {
    return current
  }

  if (!effect) {
    return undefined
  }

  const effectState = stateManifest(manifest, effect.state_name)
  if (effectState.kind !== "effect" || !effectState.on_acknowledged) {
    return undefined
  }

  const candidate = stateManifest(manifest, effectState.on_acknowledged)
  return candidate.kind === "observation_wait" ? candidate : undefined
}

function reduceObservation(
  definition: MissionRuntimeDefinition,
  state: MissionState,
  event: AcceptedEvent,
  payload: ObservationRecordedPayload,
): CommandRecord[] {
  if (state.observations[payload.observation_id]) {
    return []
  }

  state.observations[payload.observation_id] = true

  if (payload.effect_id && state.active_effect_id !== payload.effect_id) {
    return []
  }

  const effectId = payload.effect_id ?? state.active_effect_id ?? undefined
  const effect = effectId ? state.effects[effectId] : undefined
  const waitState = observationWaitForCurrentEffect(definition.manifest, state, effect)

  if (!waitState || !observationMatches(waitState, effect, payload, state.input)) {
    return []
  }

  if (effect) {
    effect.retry_timer_id = null
    effect.status = "settled"
  }
  return enterState(definition, state, waitState.on_observed, event, null)
}

function reduceTimer(
  definition: MissionRuntimeDefinition,
  state: MissionState,
  event: AcceptedEvent,
  payload: TimerFiredPayload,
): CommandRecord[] {
  const current = stateManifest(definition.manifest, state.current_state)
  if (current.kind !== "observation_wait") {
    if (current.kind !== "effect" || !state.active_effect_id) {
      return []
    }

    const effect = effectProjection(state, state.active_effect_id)
    if (effect.state_name !== state.current_state || !retryAvailable(current, effect)) {
      return []
    }

    const expectedRetryTimerId = effect.retry_timer_id
    if (
      !expectedRetryTimerId ||
      payload.timer_id !== expectedRetryTimerId ||
      payload.state_instance_id !== effect.state_instance_id
    ) {
      return []
    }

    return [issueEffect(definition, state, event, current, effect)]
  }

  const expectedTimerId = deriveTimerId(state.run_id, state.current_state_instance_id)
  if (
    payload.timer_id !== expectedTimerId ||
    payload.state_instance_id !== state.current_state_instance_id
  ) {
    return []
  }

  return enterState(definition, state, current.timeout.on_timeout, event, state.active_effect_id)
}

function reduceReconciliationResult(
  definition: MissionRuntimeDefinition,
  state: MissionState,
  event: AcceptedEvent,
  payload: EffectReconciliationResultPayload,
): CommandRecord[] {
  if (state.active_effect_id !== payload.effect_id) {
    return []
  }

  const current = stateManifest(definition.manifest, state.current_state)
  if (current.kind !== "reconcile") {
    return []
  }

  const effect = effectProjection(state, payload.effect_id)
  const effectState = stateManifest(definition.manifest, effect.state_name)
  if (effectState.kind !== "effect") {
    fail("INVALID_DEFINITION", `Effect ${effect.effect_id} points to a non-effect state`, event)
  }
  let target: string
  let reuseEffectId: EffectId | undefined

  switch (payload.result) {
    case "confirmed_completed":
      effect.retry_timer_id = null
      effect.status = "settled"
      target = current.on_confirmed_completed
      break
    case "confirmed_not_applied":
      if (!retryAvailable(effectState, effect)) {
        effect.retry_timer_id = null
        effect.status = "failed"
        return enterState(definition, state, effectState.on_failed, event, null)
      }
      effect.status = "prepared"
      target = current.on_confirmed_not_applied
      if (target === effect.state_name) {
        reuseEffectId = effect.effect_id
      }
      break
    case "confirmed_in_progress":
      effect.retry_timer_id = null
      effect.status = "in_progress"
      target = current.on_confirmed_in_progress
      break
    case "conflicting_state":
      effect.retry_timer_id = null
      effect.status = "uncertain"
      target = current.on_conflicting_state
      break
    case "unknown":
      effect.retry_timer_id = null
      effect.status = "uncertain"
      target = current.on_unknown
      break
  }

  if (reuseEffectId) {
    const commands = enterState(
      definition,
      state,
      target,
      event,
      effect.effect_id,
      reuseEffectId,
      true,
    )
    return [...commands, ...retryEffect(definition, state, event, effectState, effect)]
  }

  return enterState(definition, state, target, event, effect.effect_id, reuseEffectId)
}

function admit(
  definition: MissionRuntimeDefinition,
  event: AcceptedEvent,
  payload: Extract<ReturnType<typeof parseReducerEvent>, { type: "mission.admitted" }>["payload"],
): ReductionResult {
  if (
    payload.definition_digest !== definition.definition_digest ||
    payload.execution_binding_digest !== definition.execution_binding_digest
  ) {
    fail("INVALID_DEFINITION", "Admission digests do not match the runtime definition", event)
  }

  if (!Schema.Check(definition.manifest.input_schema as never, payload.input)) {
    fail("INVALID_INPUT", "Mission input does not satisfy the registered schema", event)
  }

  const initial = definition.manifest.initial
  const stateInstanceId = deriveStateInstanceId(event.run_id, initial, event.event_id)
  const state: MissionState = {
    run_id: event.run_id,
    definition_digest: definition.definition_digest,
    execution_binding_digest: definition.execution_binding_digest,
    status: "admitted",
    current_state: initial,
    current_state_instance_id: stateInstanceId,
    current_sequence: event.sequence,
    ownership_epoch: event.ownership_epoch,
    input: structuredClone(payload.input),
    active_effect_id: null,
    terminal_outcome: null,
    effects: {},
    observations: {},
  }
  const commands = enterState(definition, state, initial, event, null)
  return finish(state, commands, event)
}

export function reduceMission(
  definition: MissionRuntimeDefinition,
  previousState: MissionState | null,
  event: AcceptedEvent,
): ReductionResult {
  validateDefinition(definition)

  if (!acceptedEventValidator.Check(event)) {
    fail("INVALID_EVENT_PAYLOAD", "Accepted event envelope is invalid", event)
  }

  const expectedSequence = previousState ? previousState.current_sequence + 1 : 1
  if (event.sequence !== expectedSequence) {
    fail(
      "SEQUENCE_MISMATCH",
      `Expected sequence ${expectedSequence}, received ${event.sequence}`,
      event,
    )
  }

  const reducerEvent = parseReducerEvent(event)

  if (!previousState) {
    if (reducerEvent.type !== "mission.admitted") {
      fail("ADMISSION_REQUIRED", "The first mission event must be mission.admitted", event)
    }
    return admit(definition, event, reducerEvent.payload)
  }

  if (!missionStateValidator.Check(previousState)) {
    fail("INVALID_PREVIOUS_STATE", "Previous mission projection is invalid", event)
  }

  if (previousState.run_id !== event.run_id) {
    fail("RUN_MISMATCH", `Event ${event.event_id} belongs to a different run`, event)
  }

  if (reducerEvent.type === "mission.admitted") {
    fail("ALREADY_ADMITTED", "A mission may be admitted only once", event)
  }

  const state = cloneState(previousState)
  state.current_sequence = event.sequence

  if (state.terminal_outcome) {
    return finish(state, [], event)
  }

  if (event.ownership_epoch < state.ownership_epoch) {
    return finish(state, [], event)
  }

  if (event.ownership_epoch > state.ownership_epoch) {
    fail(
      "OWNERSHIP_EPOCH_MISMATCH",
      `Event epoch ${event.ownership_epoch} exceeds active epoch ${state.ownership_epoch}`,
      event,
    )
  }

  let commands: CommandRecord[]

  switch (reducerEvent.type) {
    case "effect.dispatch_started":
      if (state.active_effect_id === reducerEvent.payload.effect_id) {
        const effect = effectProjection(state, reducerEvent.payload.effect_id)
        if (effect.status === "prepared") {
          effect.status = "dispatching"
        }
      }
      commands = []
      break
    case "effect.issue_result_recorded":
      commands = reduceIssueResult(definition, state, event, reducerEvent.payload)
      break
    case "effect.reconciliation_result_recorded":
      commands = reduceReconciliationResult(definition, state, event, reducerEvent.payload)
      break
    case "observation.recorded":
      commands = reduceObservation(definition, state, event, reducerEvent.payload)
      break
    case "timer.fired":
      commands = reduceTimer(definition, state, event, reducerEvent.payload)
      break
  }

  return finish(state, commands, event)
}
