import type { OperationCapabilities } from "@idemantis/contracts"
import Type from "typebox"
import Schema from "typebox/schema"
import { describe, expect, it } from "vitest"
import {
  ExecutionBindingError,
  MissionDefinitionError,
  MissionManifestSchema,
  defineMission,
  defineOperation,
  effectState,
  executionBindingDigest,
  finalState,
  fromInput,
  mapFields,
  noRetry,
  reconcileBeforeRetry,
  reconcileState,
  safeRetry,
  waitForObservation,
  type ExecutionBinding,
  type MissionDefinition,
  type MissionDefinitionIssueCode,
  type StateDefinition,
} from "../src/index.js"

const digest = `sha256:${"a".repeat(64)}` as const

const capabilities = {
  reserve: {
    duplicate_safety: "keyed_uniqueness",
    reconciliation: "authoritative",
    fencing_mode: "application_epoch",
    fencing_scope: "resource",
    handoff_mode: "settle_first",
    execution_location: "either",
    cancellation: true,
    compensation: true,
    observation_stream: false,
  },
  dispatch: {
    duplicate_safety: "native_idempotency",
    reconciliation: "authoritative",
    fencing_mode: "application_epoch",
    fencing_scope: "subject",
    handoff_mode: "transferable",
    execution_location: "either",
    cancellation: true,
    compensation: false,
    observation_stream: true,
  },
  snapshot: {
    duplicate_safety: "native_idempotency",
    reconciliation: "authoritative",
    fencing_mode: "application_epoch",
    fencing_scope: "subject",
    handoff_mode: "settle_first",
    execution_location: "either",
    cancellation: false,
    compensation: false,
    observation_stream: true,
  },
  access: {
    duplicate_safety: "reconciliation_only",
    reconciliation: "authoritative",
    fencing_mode: "exclusive_session",
    fencing_scope: "resource",
    handoff_mode: "settle_first",
    execution_location: "runner",
    cancellation: false,
    compensation: true,
    observation_stream: false,
  },
} satisfies Record<string, OperationCapabilities>

const reserve = defineOperation({
  key: "destination.reserve",
  adapter: "destination",
  operation: "resource.reserve",
  schemaVersion: 1,
  inputSchema: Type.Object({ destination: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ reservation: Type.String() }, { additionalProperties: false }),
  effectClass: "resource_reservation",
  consequence: "compensatable",
  capabilities: capabilities.reserve,
})

const dispatch = defineOperation({
  key: "vehicle.dispatch",
  adapter: "vehicle",
  operation: "vehicle.dispatch",
  schemaVersion: 1,
  inputSchema: Type.Object({ route: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ mission: Type.String() }, { additionalProperties: false }),
  effectClass: "physical_actuation",
  consequence: "compensatable",
  capabilities: capabilities.dispatch,
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
  capabilities: capabilities.snapshot,
})

const openAccess = defineOperation({
  key: "access.open",
  adapter: "access",
  operation: "access.open",
  schemaVersion: 1,
  inputSchema: Type.Object({ destination: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ opened: Type.Boolean() }, { additionalProperties: false }),
  effectClass: "access_mutation",
  consequence: "compensatable",
  capabilities: capabilities.access,
})

function deliveryStates() {
  return {
    reserveDestination: effectState({
      operation: reserve,
      effectSlot: "reservation",
      input: mapFields({ destination: fromInput("destination") }),
      retry: safeRetry({ attempts: 3, backoffMs: 100 }),
      cancellation: {
        onCancelled: "cancelled",
        onCompletionDuringCancel: "unresolved",
      },
      onSettled: "dispatchVehicle",
      onFailed: "failed",
      onUncertain: "reconcileReservation",
    }),
    reconcileReservation: reconcileState({
      operation: reserve,
      subject: fromInput("destination"),
      onConfirmedCompleted: "dispatchVehicle",
      onConfirmedNotApplied: "reserveDestination",
      onConfirmedInProgress: "reconcileReservation",
      onConflictingState: "unresolved",
      onUnknown: "unresolved",
    }),
    dispatchVehicle: effectState({
      operation: dispatch,
      effectSlot: "vehicle_dispatch",
      input: mapFields({ route: fromInput("route") }),
      retry: safeRetry({ attempts: 2 }),
      execution: "edge",
      automaticTakeover: true,
      activeHandoff: true,
      cancellation: {
        onCancelled: "cancelled",
        onCompletionDuringCancel: "unresolved",
      },
      onSettled: "waitForArrival",
      onFailed: "failed",
      onUncertain: "reconcileVehicle",
    }),
    waitForArrival: waitForObservation({
      observationType: "vehicle.arrived",
      schemaVersion: 1,
      correlation: { kind: "effect", effectState: "dispatchVehicle" },
      timeout: { afterMs: 900_000, onTimeout: "reconcileVehicle" },
      onObserved: "openAccess",
    }),
    reconcileVehicle: reconcileState({
      operation: snapshot,
      subject: fromInput("vehicle"),
      onConfirmedCompleted: "openAccess",
      onConfirmedNotApplied: "dispatchVehicle",
      onConfirmedInProgress: "waitForArrival",
      onConflictingState: "unresolved",
      onUnknown: "unresolved",
    }),
    openAccess: effectState({
      operation: openAccess,
      effectSlot: "access",
      input: mapFields({ destination: fromInput("destination") }),
      retry: reconcileBeforeRetry({ attempts: 2, backoffMs: 250 }),
      onSettled: "complete",
      onFailed: "failed",
      onUncertain: "unresolved",
    }),
    complete: finalState("completed"),
    cancelled: finalState("cancelled"),
    failed: finalState("failed"),
    unresolved: finalState("unresolved"),
  }
}

function deliveryDefinition(
  states: Record<string, StateDefinition> = deliveryStates(),
): MissionDefinition {
  return {
    id: "delivery",
    version: "1.0.0",
    input: Type.Object(
      {
        destination: Type.String(),
        route: Type.String(),
        vehicle: Type.String(),
      },
      { additionalProperties: false },
    ),
    initial: "reserveDestination",
    states,
  }
}

function expectDefinitionIssue(
  definition: MissionDefinition,
  code: MissionDefinitionIssueCode,
): void {
  try {
    defineMission(definition)
    throw new Error(`Expected ${code}`)
  } catch (error) {
    expect(error).toBeInstanceOf(MissionDefinitionError)
    expect((error as MissionDefinitionError).issues.map((item) => item.code)).toContain(code)
  }
}

function bindings(): ExecutionBinding[] {
  return [reserve, dispatch, snapshot, openAccess].map((operation, index) => ({
    operationKey: operation.key,
    reference: {
      adapter_instance_id: `adp_01ARZ3NDEKTSV4RRFFQ69G5FA${String.fromCharCode(86 + index)}`,
      adapter_package_digest: digest,
      capability_manifest_digest: digest,
      operation: operation.operation,
      operation_schema_version: operation.schemaVersion,
    },
    capabilities: operation.capabilities,
  }))
}

describe("mission compilation", () => {
  it("emits the same canonical manifest for equivalent source ordering", () => {
    const first = defineMission(deliveryDefinition())
    const reversed = Object.fromEntries(Object.entries(deliveryStates()).reverse())
    const second = defineMission(deliveryDefinition(reversed))

    expect(first.definitionDigest).toBe(second.definitionDigest)
    expect(first.canonicalManifest).toBe(second.canonicalManifest)
    expect(JSON.parse(first.canonicalManifest)).toEqual(first.manifest)
    expect(Object.isFrozen(first.manifest)).toBe(true)
    expect(Schema.Check(MissionManifestSchema, first.manifest)).toBe(true)
    expect(Schema.Check(MissionManifestSchema, { ...first.manifest, unexpected: true })).toBe(false)
  })

  it("emits an inspectable delivery graph", () => {
    const graph = defineMission(deliveryDefinition()).manifest.graph

    expect(graph.nodes).toHaveLength(10)
    expect(graph.edges).toHaveLength(25)
    expect(graph.nodes.map((node) => node.id)).toEqual(
      [...graph.nodes.map((node) => node.id)].sort(),
    )
    expect(graph.nodes).toContainEqual({
      id: "dispatchVehicle",
      kind: "effect",
      operation: "vehicle.dispatch",
    })
    expect(graph.nodes).toContainEqual({ id: "complete", kind: "final", outcome: "completed" })
    expect(graph.edges).toContainEqual({
      from: "dispatchVehicle",
      to: "waitForArrival",
      trigger: "effect.settled",
    })
    expect(graph.edges).toContainEqual({
      from: "waitForArrival",
      to: "reconcileVehicle",
      trigger: "observation.timeout",
    })
    expect(graph.edges.filter((edge) => edge.from === "complete")).toEqual([])
  })

  it("does not freeze the caller's input schema", () => {
    const definition = deliveryDefinition()
    const input = definition.input

    defineMission(definition)

    expect(Object.isFrozen(input)).toBe(false)
  })

  it("binds every operation into an order-independent digest", () => {
    const mission = defineMission(deliveryDefinition())
    const forward = executionBindingDigest(mission, bindings())
    const reverse = executionBindingDigest(mission, bindings().reverse())

    expect(forward).toBe(reverse)
    expect(forward).toMatch(/^sha256:[a-f0-9]{64}$/)
  })

  it("rejects incomplete execution bindings", () => {
    const mission = defineMission(deliveryDefinition())

    expect(() => executionBindingDigest(mission, bindings().slice(1))).toThrowError(
      ExecutionBindingError,
    )
  })

  it("reports malformed bindings without attempting to digest them", () => {
    const mission = defineMission(deliveryDefinition())
    const malformed = bindings()
    malformed[0] = { ...malformed[0], operationKey: undefined } as never

    try {
      executionBindingDigest(mission, malformed)
      throw new Error("Expected INVALID_BINDING")
    } catch (error) {
      expect(error).toBeInstanceOf(ExecutionBindingError)
      expect((error as ExecutionBindingError).issues.map((item) => item.code)).toContain(
        "INVALID_BINDING",
      )
    }
  })
})

describe("definition safety", () => {
  it("rejects unsafe automatic retry", () => {
    const states = deliveryStates()
    const current = states.reserveDestination
    states.reserveDestination = {
      ...current,
      operation: {
        ...current.operation,
        capabilities: {
          ...current.operation.capabilities,
          duplicate_safety: "none",
          reconciliation: "none",
        },
      },
    }

    expectDefinitionIssue(deliveryDefinition(states), "UNSAFE_RETRY")
  })

  it("rejects an unhandled cancellation race", () => {
    const states = deliveryStates()
    const current = states.reserveDestination
    states.reserveDestination = {
      ...current,
      cancellation: { onCancelled: "cancelled" },
    }

    expectDefinitionIssue(deliveryDefinition(states), "CANCELLATION_RACE_UNHANDLED")
  })

  it("rejects active handoff without transferable target support", () => {
    const states = deliveryStates()
    const current = states.dispatchVehicle
    states.dispatchVehicle = {
      ...current,
      operation: {
        ...current.operation,
        capabilities: { ...current.operation.capabilities, handoff_mode: "unsupported" },
      },
    }

    expectDefinitionIssue(deliveryDefinition(states), "HANDOFF_UNSUPPORTED")
  })

  it("rejects automatic takeover without downstream fencing", () => {
    const states = deliveryStates()
    const current = states.dispatchVehicle
    states.dispatchVehicle = {
      ...current,
      operation: {
        ...current.operation,
        capabilities: { ...current.operation.capabilities, fencing_mode: "none" },
      },
    }

    expectDefinitionIssue(deliveryDefinition(states), "FENCING_REQUIRED")
  })

  it("rejects missing transitions and unreachable states", () => {
    const current = deliveryStates()
    const states = {
      ...current,
      openAccess: { ...current.openAccess, onSettled: "missing" },
      orphaned: finalState("failed"),
    }

    expectDefinitionIssue(deliveryDefinition(states), "TRANSITION_TARGET_MISSING")
    expectDefinitionIssue(deliveryDefinition(states), "UNREACHABLE_STATE")
  })

  it("rejects observation waits without correlation and timeout policy", () => {
    const states = deliveryStates()
    const current = states.waitForArrival
    states.waitForArrival = {
      kind: current.kind,
      observationType: current.observationType,
      schemaVersion: current.schemaVersion,
      onObserved: current.onObserved,
    }

    expectDefinitionIssue(deliveryDefinition(states), "OBSERVATION_CORRELATION_REQUIRED")
    expectDefinitionIssue(deliveryDefinition(states), "OBSERVATION_TIMEOUT_REQUIRED")
  })

  it("rejects values outside the serializable expression set", () => {
    const states = deliveryStates()
    const current = states.openAccess
    states.openAccess = {
      ...current,
      input: { kind: "callback", run: () => true } as never,
    }

    expectDefinitionIssue(deliveryDefinition(states), "INVALID_EXPRESSION")
  })

  it("permits effects that explicitly disable retry", () => {
    const states = deliveryStates()
    const current = states.openAccess
    states.openAccess = { ...current, retry: noRetry() }

    expect(defineMission(deliveryDefinition(states)).manifest).toMatchObject({
      states: { openAccess: { kind: "effect" } },
    })
  })
})
