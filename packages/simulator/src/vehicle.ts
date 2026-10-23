import { defineAdapterOperation, type AdapterOperation } from "@idemantis/adapter-kit"
import { sha256Digest, type EffectId } from "@idemantis/contracts"
import Type from "typebox"
import type { SimulationKernel } from "./kernel.js"
import { SimulatedTargetProbe } from "./target.js"
import type { TargetSnapshot } from "./types.js"

const dispatchInputSchema = Type.Object(
  {
    vehicle_id: Type.String({ minLength: 1 }),
    route: Type.String({ minLength: 1 }),
    pickup: Type.String({ minLength: 1 }),
    destination: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
)
const dispatchOutputSchema = Type.Object(
  { assignment_id: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)
const vehicleSubjectSchema = Type.Object(
  { vehicle_id: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)

type DispatchInput = import("typebox").Static<typeof dispatchInputSchema>
type DispatchOutput = import("typebox").Static<typeof dispatchOutputSchema>
type VehicleSubject = import("typebox").Static<typeof vehicleSubjectSchema>
type VehicleStatus = "idle" | "assigned" | "moving" | "arrived" | "stopped" | "faulted"

interface Assignment {
  effect_id: EffectId
  assignment_id: string
  route: string
  pickup: string
  destination: string
}

export interface VehicleSimulatorOptions {
  vehicle_id: string
  target?: string
}

export class VehicleSimulator {
  readonly dispatch: AdapterOperation<DispatchInput, DispatchOutput, VehicleSubject>
  readonly #kernel: SimulationKernel
  readonly #probe: SimulatedTargetProbe
  readonly #vehicleId: string
  #status: VehicleStatus = "idle"
  #assignment: Assignment | undefined
  #activeEpoch = 1

  constructor(kernel: SimulationKernel, options: VehicleSimulatorOptions) {
    this.#kernel = kernel
    this.#vehicleId = options.vehicle_id
    this.#probe = new SimulatedTargetProbe(kernel, options.target ?? "vehicle")
    this.dispatch = defineAdapterOperation({
      key: "vehicle.dispatch",
      adapter: "vehicle",
      operation: "vehicle.dispatch",
      schemaVersion: 1,
      inputSchema: dispatchInputSchema,
      outputSchema: dispatchOutputSchema,
      subjectSchema: vehicleSubjectSchema,
      effectClass: "physical_actuation",
      consequence: "compensatable",
      capabilities: {
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
      timeout: { minimumMs: 10, defaultMs: 2_000, maximumMs: 10_000 },
      reconciliationLookupFields: ["vehicle_id"],
      observationTypes: ["vehicle.arrived", "vehicle.pickup_confirmed"],
      observationCorrelationFields: ["effect_id", "vehicle_id"],
      maximumFenceActivationDelayMs: 100,
      issue: async (context, input) => {
        this.#probe.issue("vehicle.dispatch", context.effect_id)
        if (context.ownership_epoch < this.#activeEpoch) {
          this.#probe.rejectStale("vehicle.dispatch", context.effect_id, context.ownership_epoch)
          return { result: "rejected", evidence: { reason: "stale_epoch" } }
        }
        if (input.vehicle_id !== this.#vehicleId) {
          return { result: "rejected", evidence: { reason: "vehicle_not_found" } }
        }
        if (this.#assignment?.effect_id === context.effect_id) {
          if (
            this.#assignment.route !== input.route ||
            this.#assignment.pickup !== input.pickup ||
            this.#assignment.destination !== input.destination
          ) {
            return { result: "rejected", evidence: { reason: "idempotency_conflict" } }
          }
          if (this.#status === "stopped" || this.#status === "faulted") {
            return { result: "rejected", evidence: { reason: "assignment_inactive" } }
          }
          return this.#status === "arrived"
            ? {
                result: "completed",
                output: { assignment_id: this.#assignment.assignment_id },
              }
            : { result: "accepted", external_reference: this.#assignment.assignment_id }
        }
        if (this.#assignment && !["idle", "stopped"].includes(this.#status)) {
          return { result: "rejected", evidence: { reason: "vehicle_busy" } }
        }

        const assignment: Assignment = {
          effect_id: context.effect_id,
          assignment_id: `assignment-${context.effect_id.slice(-8)}`,
          route: input.route,
          pickup: input.pickup,
          destination: input.destination,
        }
        this.#assignment = assignment
        this.#status = "assigned"
        this.#probe.apply("vehicle.dispatch", context.effect_id, {
          assignment_id: assignment.assignment_id,
          vehicle_id: this.#vehicleId,
        })
        this.#scheduleObservation(
          "pickup_observation",
          100,
          "vehicle.pickup_confirmed",
          input.pickup,
        )
        this.#scheduleObservation("arrival_observation", 500, "vehicle.arrived", input.destination)
        const fault = this.#kernel.takeFault({
          target: this.#probe.target,
          operation: "vehicle.dispatch",
          stage: "after_apply",
        })
        if (fault?.action === "drop_response") {
          return { result: "uncertain", evidence: { reason: "response_lost" } }
        }
        this.#status = "moving"
        return { result: "accepted", external_reference: assignment.assignment_id }
      },
      reconcile: async (context, subject) => {
        this.#probe.reconcile("vehicle.dispatch", context.effect_id)
        if (subject.vehicle_id !== this.#vehicleId || !this.#assignment) {
          return { result: "confirmed_not_applied" }
        }
        if (this.#assignment.effect_id !== context.effect_id) {
          return { result: "conflicting_state" }
        }
        if (this.#status === "arrived") {
          return {
            result: "confirmed_completed",
            output: { assignment_id: this.#assignment.assignment_id },
          }
        }
        return {
          result: "confirmed_in_progress",
          external_reference: this.#assignment.assignment_id,
        }
      },
      cancel: async (context, subject) => {
        this.#probe.issue("vehicle.dispatch.cancel", context.effect_id)
        if (subject.vehicle_id !== this.#vehicleId || !this.#assignment) {
          return { result: "failed_not_changed", evidence: { reason: "assignment_not_found" } }
        }
        if (this.#status === "arrived") {
          return { result: "rejected_still_active", evidence: { reason: "already_arrived" } }
        }
        if (this.#status === "stopped" || this.#status === "faulted") {
          return { result: "confirmed_stopped" }
        }
        this.#status = "stopped"
        this.#probe.apply("vehicle.dispatch.cancel", context.effect_id, {
          assignment_id: this.#assignment.assignment_id,
        })
        return { result: "confirmed_stopped" }
      },
      activateFence: async (context) => {
        this.#activeEpoch = Math.max(this.#activeEpoch, context.ownership_epoch)
        return {
          adapter_instance_id: context.adapter_instance_id,
          scope: "subject",
          scope_reference: context.scope_reference,
          ownership_epoch: context.ownership_epoch,
          activation_reference: `vehicle-epoch-${context.ownership_epoch}`,
          effective_at: context.requested_at,
          maximum_activation_delay_ms: 100,
          evidence_digest: sha256Digest({
            target: this.#probe.target,
            vehicle_id: this.#vehicleId,
            epoch: context.ownership_epoch,
          }),
        }
      },
    })
  }

  snapshot(): TargetSnapshot {
    return this.#probe.snapshot({
      vehicle_id: this.#vehicleId,
      status: this.#status,
      active_epoch: this.#activeEpoch,
      ...(this.#assignment
        ? {
            assignment: {
              effect_id: this.#assignment.effect_id,
              assignment_id: this.#assignment.assignment_id,
              route: this.#assignment.route,
              pickup: this.#assignment.pickup,
              destination: this.#assignment.destination,
            },
          }
        : {}),
    })
  }

  #scheduleObservation(
    stage: string,
    baseDelay: number,
    type: "vehicle.pickup_confirmed" | "vehicle.arrived",
    subject: string,
  ): void {
    const assignment = this.#assignment
    if (!assignment) {
      return
    }
    const fault = this.#kernel.takeFault({
      target: this.#probe.target,
      operation: "vehicle.dispatch",
      stage,
    })
    if (fault?.action === "drop_observation") {
      return
    }
    const delay = baseDelay + (fault?.action === "delay" ? (fault.delay_ms ?? 0) : 0)
    const observationId = `${type}-${assignment.effect_id}`
    this.#kernel.schedule(
      {
        target: this.#probe.target,
        operation: "vehicle.dispatch",
        stage,
        correlation_id: assignment.effect_id,
        after_ms: delay,
        jitter_ms: 40,
        payload: { observation_type: type },
      },
      () => {
        if (this.#status === "stopped" || this.#status === "faulted") {
          this.#kernel.record({
            target: this.#probe.target,
            operation: "vehicle.dispatch",
            stage: "observation.suppressed",
            correlation_id: assignment.effect_id,
            payload: { reason: "assignment_inactive", observation_type: type },
          })
          return
        }
        if (type === "vehicle.arrived") {
          this.#status = "arrived"
        } else if (this.#status === "assigned") {
          this.#status = "moving"
        }
        const observation = {
          observation_id: observationId,
          target: this.#probe.target,
          type,
          subject_reference: subject,
          effect_id: assignment.effect_id,
          ownership_epoch: this.#activeEpoch,
          observed_at: this.#kernel.now(this.#probe.target).toISOString(),
          payload: {
            vehicle_id: this.#vehicleId,
            assignment_id: assignment.assignment_id,
            status: this.#status,
          },
        }
        this.#kernel.publishObservation(observation)
        if (fault?.action === "duplicate_observation") {
          this.#kernel.publishObservation(observation)
        }
      },
    )
  }
}
