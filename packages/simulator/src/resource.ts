import {
  defineAdapterOperation,
  type AdapterOperation,
  type FenceActivationContext,
} from "@idemantis/adapter-kit"
import { TimestampSchema, sha256Digest, type EffectId } from "@idemantis/contracts"
import Type from "typebox"
import type { SimulationKernel } from "./kernel.js"
import { SimulatedTargetProbe } from "./target.js"
import type { TargetSnapshot } from "./types.js"

const reserveInputSchema = Type.Object(
  {
    resource_id: Type.String({ minLength: 1 }),
    reservation_key: Type.String({ minLength: 1 }),
    owner: Type.String({ minLength: 1 }),
    expires_at: TimestampSchema,
  },
  { additionalProperties: false },
)
const reserveOutputSchema = Type.Object(
  { reservation_id: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)
const reservationSubjectSchema = Type.Object(
  {
    resource_id: Type.String({ minLength: 1 }),
    reservation_key: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
)
const releaseInputSchema = Type.Object(
  {
    resource_id: Type.String({ minLength: 1 }),
    reservation_id: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
)
const releaseOutputSchema = Type.Object(
  { released: Type.Boolean() },
  { additionalProperties: false },
)
const releaseSubjectSchema = releaseInputSchema

type ReserveInput = import("typebox").Static<typeof reserveInputSchema>
type ReserveOutput = import("typebox").Static<typeof reserveOutputSchema>
type ReservationSubject = import("typebox").Static<typeof reservationSubjectSchema>
type ReleaseInput = import("typebox").Static<typeof releaseInputSchema>
type ReleaseOutput = import("typebox").Static<typeof releaseOutputSchema>

interface Reservation {
  effect_id: EffectId
  reservation_id: string
  reservation_key: string
  owner: string
  expires_at: string
  released: boolean
  expired: boolean
}

export interface ResourceSimulatorOptions {
  resource_id: string
  capacity?: number
  target?: string
}

export class ResourceSimulator {
  readonly reserve: AdapterOperation<ReserveInput, ReserveOutput, ReservationSubject>
  readonly release: AdapterOperation<ReleaseInput, ReleaseOutput, ReleaseInput>
  readonly #kernel: SimulationKernel
  readonly #probe: SimulatedTargetProbe
  readonly #resourceId: string
  readonly #capacity: number
  readonly #reservations = new Map<string, Reservation>()
  readonly #releaseEffects = new Map<EffectId, string>()
  #activeEpoch = 1

  constructor(kernel: SimulationKernel, options: ResourceSimulatorOptions) {
    this.#kernel = kernel
    this.#resourceId = options.resource_id
    this.#capacity = options.capacity ?? 1
    if (!Number.isSafeInteger(this.#capacity) || this.#capacity < 1) {
      throw new RangeError("Resource capacity must be a positive integer")
    }
    this.#probe = new SimulatedTargetProbe(kernel, options.target ?? "destination-resource")
    this.reserve = defineAdapterOperation({
      key: "destination.reserve",
      adapter: "resource",
      operation: "resource.reserve",
      schemaVersion: 1,
      inputSchema: reserveInputSchema,
      outputSchema: reserveOutputSchema,
      subjectSchema: reservationSubjectSchema,
      effectClass: "resource_reservation",
      consequence: "compensatable",
      capabilities: {
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
      timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
      reconciliationLookupFields: ["reservation_key", "resource_id"],
      maximumFenceActivationDelayMs: 100,
      issue: async (context, input) => {
        this.#probe.issue("resource.reserve", context.effect_id)
        if (context.ownership_epoch < this.#activeEpoch) {
          this.#probe.rejectStale("resource.reserve", context.effect_id, context.ownership_epoch)
          return { result: "rejected", evidence: { reason: "stale_epoch" } }
        }
        if (input.resource_id !== this.#resourceId) {
          return { result: "rejected", evidence: { reason: "resource_not_found" } }
        }
        const existing = this.#reservations.get(input.reservation_key)
        if (existing) {
          if (existing.owner !== input.owner || existing.expires_at !== input.expires_at) {
            return { result: "rejected", evidence: { reason: "idempotency_conflict" } }
          }
          return existing.released || existing.expired
            ? { result: "rejected", evidence: { reason: "reservation_inactive" } }
            : { result: "completed", output: { reservation_id: existing.reservation_id } }
        }
        const active = [...this.#reservations.values()].filter(
          (reservation) => !reservation.released && !reservation.expired,
        )
        if (active.length >= this.#capacity) {
          return { result: "rejected", evidence: { reason: "capacity_exhausted" } }
        }
        const expiresAt = Date.parse(input.expires_at)
        const expiresAfter = expiresAt - this.#kernel.now().getTime()
        if (expiresAfter <= 0) {
          return { result: "rejected", evidence: { reason: "expiry_not_future" } }
        }
        const reservation: Reservation = {
          effect_id: context.effect_id,
          reservation_id: input.reservation_key,
          reservation_key: input.reservation_key,
          owner: input.owner,
          expires_at: input.expires_at,
          released: false,
          expired: false,
        }
        this.#reservations.set(input.reservation_key, reservation)
        this.#probe.apply("resource.reserve", context.effect_id, {
          reservation_id: reservation.reservation_id,
          resource_id: this.#resourceId,
        })
        this.#kernel.schedule(
          {
            target: this.#probe.target,
            operation: "resource.reserve",
            stage: "reservation.expiry",
            correlation_id: context.effect_id,
            after_ms: expiresAfter,
          },
          () => {
            if (reservation.released || reservation.expired) {
              return
            }
            reservation.expired = true
            this.#kernel.record({
              target: this.#probe.target,
              operation: "resource.reserve",
              stage: "reservation.expired",
              correlation_id: context.effect_id,
              payload: { reservation_id: reservation.reservation_id },
            })
          },
        )
        const fault = this.#kernel.takeFault({
          target: this.#probe.target,
          operation: "resource.reserve",
          stage: "after_apply",
        })
        if (fault?.action === "drop_response") {
          return { result: "uncertain", evidence: { reason: "response_lost" } }
        }
        return { result: "completed", output: { reservation_id: reservation.reservation_id } }
      },
      reconcile: async (context, subject) => {
        this.#probe.reconcile("resource.reserve", context.effect_id)
        if (subject.resource_id !== this.#resourceId) {
          return { result: "confirmed_not_applied" }
        }
        const reservation = this.#reservations.get(subject.reservation_key)
        if (!reservation) {
          return { result: "confirmed_not_applied" }
        }
        if (
          reservation.effect_id !== context.effect_id ||
          reservation.released ||
          reservation.expired
        ) {
          return { result: "conflicting_state" }
        }
        return {
          result: "confirmed_completed",
          output: { reservation_id: reservation.reservation_id },
        }
      },
      cancel: async (context, subject) => {
        this.#probe.issue("resource.reserve.cancel", context.effect_id)
        if (context.ownership_epoch < this.#activeEpoch) {
          this.#probe.rejectStale(
            "resource.reserve.cancel",
            context.effect_id,
            context.ownership_epoch,
          )
          return { result: "rejected_still_active", evidence: { reason: "stale_epoch" } }
        }
        const result = this.#stopReservation(subject)
        if (result.result === "confirmed_stopped") {
          this.#probe.apply("resource.reserve.cancel", context.effect_id, {
            reservation_key: subject.reservation_key,
          })
        }
        return result
      },
      compensate: async (context, subject) => {
        this.#probe.issue("resource.reserve.compensate", context.effect_id)
        if (context.ownership_epoch < this.#activeEpoch) {
          this.#probe.rejectStale(
            "resource.reserve.compensate",
            context.effect_id,
            context.ownership_epoch,
          )
          return { result: "rejected", evidence: { reason: "stale_epoch" } }
        }
        const result = this.#stopReservation(subject)
        if (result.result !== "confirmed_stopped") {
          return { result: "failed", evidence: result.evidence }
        }
        this.#probe.apply("resource.reserve.compensate", context.effect_id, {
          reservation_key: subject.reservation_key,
        })
        return {
          result: "completed",
          output: {
            reservation_id:
              this.#reservations.get(subject.reservation_key)?.reservation_id ?? "released",
          },
        }
      },
      activateFence: async (context) => this.#fence(context),
    })
    this.release = defineAdapterOperation({
      key: "destination.release",
      adapter: "resource",
      operation: "resource.release",
      schemaVersion: 1,
      inputSchema: releaseInputSchema,
      outputSchema: releaseOutputSchema,
      subjectSchema: releaseSubjectSchema,
      effectClass: "resource_reservation",
      consequence: "reversible",
      capabilities: {
        duplicate_safety: "native_idempotency",
        reconciliation: "authoritative",
        fencing_mode: "application_epoch",
        fencing_scope: "resource",
        handoff_mode: "settle_first",
        execution_location: "either",
        cancellation: false,
        compensation: false,
        observation_stream: false,
      },
      timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
      reconciliationLookupFields: ["reservation_id", "resource_id"],
      maximumFenceActivationDelayMs: 100,
      issue: async (context, input) => {
        this.#probe.issue("resource.release", context.effect_id)
        if (context.ownership_epoch < this.#activeEpoch) {
          this.#probe.rejectStale("resource.release", context.effect_id, context.ownership_epoch)
          return { result: "rejected", evidence: { reason: "stale_epoch" } }
        }
        const reservation = this.#reservationById(input.reservation_id)
        if (input.resource_id !== this.#resourceId || !reservation) {
          return { result: "rejected", evidence: { reason: "reservation_not_found" } }
        }
        const releasedReservationId = this.#releaseEffects.get(context.effect_id)
        if (releasedReservationId) {
          return releasedReservationId === input.reservation_id
            ? { result: "completed", output: { released: true } }
            : { result: "rejected", evidence: { reason: "idempotency_conflict" } }
        }
        if (reservation.released) {
          return { result: "rejected", evidence: { reason: "released_by_another_effect" } }
        }
        if (reservation.expired) {
          return { result: "rejected", evidence: { reason: "reservation_expired" } }
        }
        reservation.released = true
        this.#releaseEffects.set(context.effect_id, input.reservation_id)
        this.#probe.apply("resource.release", context.effect_id, {
          reservation_id: input.reservation_id,
        })
        return { result: "completed", output: { released: true } }
      },
      reconcile: async (context, subject) => {
        this.#probe.reconcile("resource.release", context.effect_id)
        const reservation = this.#reservationById(subject.reservation_id)
        if (subject.resource_id !== this.#resourceId || !reservation) {
          return { result: "confirmed_not_applied" }
        }
        const releasedReservationId = this.#releaseEffects.get(context.effect_id)
        if (!releasedReservationId) {
          return reservation.released || reservation.expired
            ? { result: "conflicting_state" }
            : { result: "confirmed_not_applied" }
        }
        if (releasedReservationId !== subject.reservation_id) {
          return { result: "conflicting_state" }
        }
        return { result: "confirmed_completed", output: { released: true } }
      },
      activateFence: async (context) => this.#fence(context),
    })
  }

  snapshot(): TargetSnapshot {
    return this.#probe.snapshot({
      resource_id: this.#resourceId,
      capacity: this.#capacity,
      active_epoch: this.#activeEpoch,
      reservations: [...this.#reservations.values()]
        .sort((left, right) => left.reservation_key.localeCompare(right.reservation_key))
        .map((reservation) => ({
          effect_id: reservation.effect_id,
          reservation_id: reservation.reservation_id,
          reservation_key: reservation.reservation_key,
          owner: reservation.owner,
          expires_at: reservation.expires_at,
          released: reservation.released,
          expired: reservation.expired,
        })),
    })
  }

  #stopReservation(subject: ReservationSubject) {
    if (subject.resource_id !== this.#resourceId) {
      return { result: "failed_not_changed" as const, evidence: { reason: "resource_not_found" } }
    }
    const reservation = this.#reservations.get(subject.reservation_key)
    if (!reservation) {
      return { result: "failed_not_changed" as const, evidence: { reason: "not_found" } }
    }
    if (reservation.released || reservation.expired) {
      return { result: "failed_not_changed" as const, evidence: { reason: "not_active" } }
    }
    reservation.released = true
    return { result: "confirmed_stopped" as const }
  }

  #reservationById(reservationId: string): Reservation | undefined {
    return [...this.#reservations.values()].find(
      (reservation) => reservation.reservation_id === reservationId,
    )
  }

  #fence(context: FenceActivationContext) {
    this.#activeEpoch = Math.max(this.#activeEpoch, context.ownership_epoch)
    return {
      adapter_instance_id: context.adapter_instance_id,
      scope: "resource" as const,
      scope_reference: context.scope_reference,
      ownership_epoch: context.ownership_epoch,
      activation_reference: `resource-epoch-${context.ownership_epoch}`,
      effective_at: context.requested_at,
      maximum_activation_delay_ms: 100,
      evidence_digest: sha256Digest({
        target: this.#probe.target,
        resource_id: this.#resourceId,
        epoch: context.ownership_epoch,
      }),
    }
  }
}
