import type { AdapterOperation } from "@idemantis/adapter-kit"
import {
  defineMission,
  defineOperation,
  effectState,
  finalState,
  fromInput,
  mapFields,
  noRetry,
  reconcileState,
  safeRetry,
  waitForObservation,
  type CompiledMission,
  type OperationDefinition,
} from "@idemantis/sdk"
import { TimestampSchema } from "@idemantis/contracts"
import Type, { type TSchema } from "typebox"

export const ReferenceDeliveryInputSchema = Type.Object(
  {
    mission_id: Type.String({ minLength: 1 }),
    vehicle_id: Type.String({ minLength: 1 }),
    route: Type.String({ minLength: 1 }),
    pickup: Type.String({ minLength: 1 }),
    destination_id: Type.String({ minLength: 1 }),
    reservation_key: Type.String({ minLength: 1 }),
    reservation_expires_at: TimestampSchema,
    access_subject: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
)

export type ReferenceDeliveryInput = import("typebox").Static<typeof ReferenceDeliveryInputSchema>

export interface ReferenceDeliveryOperations {
  reserve: AdapterOperation<unknown, unknown, unknown>
  release: AdapterOperation<unknown, unknown, unknown>
  dispatch: AdapterOperation<unknown, unknown, unknown>
  openAccess: AdapterOperation<unknown, unknown, unknown>
}

export interface ReferenceDeliveryMissionOptions {
  vehicleExecution?: "runner" | "edge"
}

function missionOperation(
  operation: AdapterOperation<unknown, unknown, unknown>,
): OperationDefinition {
  const descriptor = operation.descriptor
  return defineOperation({
    key: descriptor.key,
    adapter: descriptor.adapter,
    operation: descriptor.operation,
    schemaVersion: descriptor.operation_schema_version,
    inputSchema: descriptor.input_schema as TSchema,
    outputSchema: descriptor.output_schema as TSchema,
    effectClass: descriptor.effect_class,
    consequence: descriptor.consequence,
    capabilities: descriptor.capabilities,
  })
}

export function createReferenceDeliveryMission(
  operations: ReferenceDeliveryOperations,
  options: ReferenceDeliveryMissionOptions = {},
): CompiledMission<typeof ReferenceDeliveryInputSchema> {
  const reserve = missionOperation(operations.reserve)
  const release = missionOperation(operations.release)
  const dispatch = missionOperation(operations.dispatch)
  const openAccess = missionOperation(operations.openAccess)
  const vehicleExecution = options.vehicleExecution ?? "edge"

  return defineMission({
    id: "reference.delivery",
    version: "1.0.0",
    input: ReferenceDeliveryInputSchema,
    initial: "reserve_destination",
    states: {
      reserve_destination: effectState({
        operation: reserve,
        effectSlot: "destination_reservation",
        input: mapFields({
          resource_id: fromInput("destination_id"),
          reservation_key: fromInput("reservation_key"),
          owner: fromInput("mission_id"),
          expires_at: fromInput("reservation_expires_at"),
        }),
        retry: safeRetry({ attempts: 2, backoffMs: 100 }),
        cancellation: {
          onCancelled: "cancelled",
          onCompletionDuringCancel: "unresolved",
        },
        onSettled: "dispatch_vehicle",
        onFailed: "failed",
        onUncertain: "reconcile_reservation",
      }),
      reconcile_reservation: reconcileState({
        effectState: "reserve_destination",
        operation: reserve,
        subject: mapFields({
          resource_id: fromInput("destination_id"),
          reservation_key: fromInput("reservation_key"),
        }),
        onConfirmedCompleted: "dispatch_vehicle",
        onConfirmedNotApplied: "reserve_destination",
        onConfirmedInProgress: "reconcile_reservation",
        onConflictingState: "unresolved",
        onUnknown: "unresolved",
      }),
      dispatch_vehicle: effectState({
        operation: dispatch,
        effectSlot: "vehicle_dispatch",
        input: mapFields({
          vehicle_id: fromInput("vehicle_id"),
          route: fromInput("route"),
          pickup: fromInput("pickup"),
          destination: fromInput("destination_id"),
        }),
        retry: safeRetry({ attempts: 2, backoffMs: 100 }),
        execution: vehicleExecution,
        ...(vehicleExecution === "edge" ? { automaticTakeover: true, activeHandoff: true } : {}),
        cancellation: {
          onCancelled: "cancelled",
          onCompletionDuringCancel: "unresolved",
        },
        onAccepted: "await_pickup",
        onSettled: "await_pickup",
        onFailed: "failed",
        onUncertain: "reconcile_vehicle",
      }),
      reconcile_vehicle: reconcileState({
        effectState: "dispatch_vehicle",
        operation: dispatch,
        subject: mapFields({ vehicle_id: fromInput("vehicle_id") }),
        execution: vehicleExecution,
        onConfirmedCompleted: "open_destination",
        onConfirmedNotApplied: "dispatch_vehicle",
        onConfirmedInProgress: "await_arrival",
        onConflictingState: "unresolved",
        onUnknown: "unresolved",
      }),
      await_pickup: waitForObservation({
        observationType: "vehicle.pickup_confirmed",
        schemaVersion: 1,
        correlation: { kind: "effect", effectState: "dispatch_vehicle" },
        timeout: { afterMs: 300_000, onTimeout: "reconcile_vehicle" },
        onObserved: "await_arrival",
      }),
      await_arrival: waitForObservation({
        observationType: "vehicle.arrived",
        schemaVersion: 1,
        correlation: { kind: "effect", effectState: "dispatch_vehicle" },
        timeout: { afterMs: 900_000, onTimeout: "reconcile_vehicle" },
        onObserved: "open_destination",
      }),
      open_destination: effectState({
        operation: openAccess,
        effectSlot: "destination_access",
        input: mapFields({
          access_point_id: fromInput("destination_id"),
          subject: fromInput("access_subject"),
        }),
        retry: noRetry(),
        onSettled: "await_open",
        onFailed: "failed",
        onUncertain: "await_operator",
      }),
      await_open: waitForObservation({
        observationType: "access.opened",
        schemaVersion: 1,
        correlation: { kind: "effect", effectState: "open_destination" },
        timeout: { afterMs: 60_000, onTimeout: "await_operator" },
        onObserved: "await_handoff",
      }),
      await_handoff: waitForObservation({
        observationType: "delivery.handoff_confirmed",
        schemaVersion: 1,
        correlation: { kind: "subject", subject: fromInput("mission_id") },
        timeout: { afterMs: 300_000, onTimeout: "await_operator" },
        onObserved: "release_destination",
      }),
      release_destination: effectState({
        operation: release,
        effectSlot: "destination_release",
        input: mapFields({
          resource_id: fromInput("destination_id"),
          reservation_id: fromInput("reservation_key"),
        }),
        retry: safeRetry({ attempts: 3, backoffMs: 100 }),
        onSettled: "complete",
        onFailed: "failed",
        onUncertain: "reconcile_release",
      }),
      reconcile_release: reconcileState({
        effectState: "release_destination",
        operation: release,
        subject: mapFields({
          resource_id: fromInput("destination_id"),
          reservation_id: fromInput("reservation_key"),
        }),
        onConfirmedCompleted: "complete",
        onConfirmedNotApplied: "release_destination",
        onConfirmedInProgress: "reconcile_release",
        onConflictingState: "unresolved",
        onUnknown: "unresolved",
      }),
      complete: finalState("completed"),
      cancelled: finalState("cancelled"),
      failed: finalState("failed"),
      await_operator: finalState("unresolved"),
      unresolved: finalState("unresolved"),
    },
  })
}
