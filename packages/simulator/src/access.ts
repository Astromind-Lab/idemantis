import { defineAdapterOperation, type AdapterOperation } from "@idemantis/adapter-kit"
import type { EffectId } from "@idemantis/contracts"
import Type from "typebox"
import type { SimulationKernel } from "./kernel.js"
import { SimulatedTargetProbe } from "./target.js"
import type { TargetSnapshot } from "./types.js"

const accessInputSchema = Type.Object(
  {
    access_point_id: Type.String({ minLength: 1 }),
    subject: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
)
const accessOutputSchema = Type.Object(
  {
    grant_id: Type.String({ minLength: 1 }),
    opened: Type.Boolean(),
  },
  { additionalProperties: false },
)
const accessSubjectSchema = Type.Object(
  {
    access_point_id: Type.String({ minLength: 1 }),
    subject: Type.String({ minLength: 1 }),
  },
  { additionalProperties: false },
)

type AccessInput = import("typebox").Static<typeof accessInputSchema>
type AccessOutput = import("typebox").Static<typeof accessOutputSchema>
type AccessSubject = import("typebox").Static<typeof accessSubjectSchema>

interface AccessGrant {
  effect_id: EffectId
  grant_id: string
  subject: string
  opened: boolean
  valid: boolean
}

export interface AccessSimulatorOptions {
  access_point_id: string
  target?: string
}

export class AccessSimulator {
  readonly open: AdapterOperation<AccessInput, AccessOutput, AccessSubject>
  readonly #kernel: SimulationKernel
  readonly #probe: SimulatedTargetProbe
  readonly #accessPointId: string
  readonly #grants: AccessGrant[] = []

  constructor(kernel: SimulationKernel, options: AccessSimulatorOptions) {
    this.#kernel = kernel
    this.#accessPointId = options.access_point_id
    this.#probe = new SimulatedTargetProbe(kernel, options.target ?? "access-service")
    this.open = defineAdapterOperation({
      key: "access.open",
      adapter: "access",
      operation: "access.open",
      schemaVersion: 1,
      inputSchema: accessInputSchema,
      outputSchema: accessOutputSchema,
      subjectSchema: accessSubjectSchema,
      effectClass: "access_mutation",
      consequence: "compensatable",
      capabilities: {
        duplicate_safety: "none",
        reconciliation: "none",
        fencing_mode: "none",
        fencing_scope: "resource",
        handoff_mode: "unsupported",
        execution_location: "runner",
        cancellation: false,
        compensation: true,
        observation_stream: true,
      },
      timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
      observationTypes: ["access.opened"],
      observationCorrelationFields: ["effect_id", "access_point_id"],
      issue: async (context, input) => {
        this.#probe.issue("access.open", context.effect_id)
        if (input.access_point_id !== this.#accessPointId) {
          return { result: "rejected", evidence: { reason: "access_point_not_found" } }
        }
        const grant: AccessGrant = {
          effect_id: context.effect_id,
          grant_id: `grant-${this.#grants.length + 1}`,
          subject: input.subject,
          opened: true,
          valid: true,
        }
        this.#grants.push(grant)
        this.#probe.apply("access.open", context.effect_id, {
          access_point_id: this.#accessPointId,
          grant_id: grant.grant_id,
        })
        this.#scheduleObservation(grant)
        const fault = this.#kernel.takeFault({
          target: this.#probe.target,
          operation: "access.open",
          stage: "after_apply",
        })
        if (fault?.action === "drop_response") {
          return { result: "uncertain", evidence: { reason: "response_lost" } }
        }
        return {
          result: "completed",
          output: { grant_id: grant.grant_id, opened: true },
        }
      },
      compensate: async (context, subject) => {
        this.#probe.issue("access.open.compensate", context.effect_id)
        if (subject.access_point_id !== this.#accessPointId) {
          return { result: "failed", evidence: { reason: "access_point_not_found" } }
        }
        const grants = this.#grants.filter(
          (grant) => grant.subject === subject.subject && grant.valid,
        )
        if (grants.length === 0) {
          return { result: "failed", evidence: { reason: "active_grant_not_found" } }
        }
        for (const grant of grants) {
          grant.valid = false
          grant.opened = false
        }
        const grantId = grants.at(-1)?.grant_id
        if (!grantId) {
          return { result: "failed", evidence: { reason: "active_grant_not_found" } }
        }
        this.#probe.apply("access.open.compensate", context.effect_id, {
          access_point_id: this.#accessPointId,
          subject: subject.subject,
        })
        return { result: "completed", output: { grant_id: grantId, opened: false } }
      },
    })
  }

  snapshot(): TargetSnapshot {
    return this.#probe.snapshot({
      access_point_id: this.#accessPointId,
      grants: this.#grants.map((grant) => ({
        effect_id: grant.effect_id,
        grant_id: grant.grant_id,
        subject: grant.subject,
        opened: grant.opened,
        valid: grant.valid,
      })),
      open: this.#grants.some((grant) => grant.opened && grant.valid),
    })
  }

  #scheduleObservation(grant: AccessGrant): void {
    const fault = this.#kernel.takeFault({
      target: this.#probe.target,
      operation: "access.open",
      stage: "opened_observation",
    })
    if (fault?.action === "drop_observation") {
      return
    }
    const delay = 80 + (fault?.action === "delay" ? (fault.delay_ms ?? 0) : 0)
    const observationId = `access.opened-${grant.effect_id}`
    this.#kernel.schedule(
      {
        target: this.#probe.target,
        operation: "access.open",
        stage: "opened_observation",
        correlation_id: grant.effect_id,
        after_ms: delay,
        jitter_ms: 20,
      },
      () => {
        if (!grant.valid || !grant.opened) {
          this.#kernel.record({
            target: this.#probe.target,
            operation: "access.open",
            stage: "observation.suppressed",
            correlation_id: grant.effect_id,
            payload: { reason: "grant_inactive" },
          })
          return
        }
        const observation = {
          observation_id: observationId,
          target: this.#probe.target,
          type: "access.opened",
          subject_reference: this.#accessPointId,
          effect_id: grant.effect_id,
          observed_at: this.#kernel.now(this.#probe.target).toISOString(),
          payload: { grant_id: grant.grant_id, subject: grant.subject },
        }
        this.#kernel.publishObservation(observation)
        if (fault?.action === "duplicate_observation") {
          this.#kernel.publishObservation(observation)
        }
      },
    )
  }
}
