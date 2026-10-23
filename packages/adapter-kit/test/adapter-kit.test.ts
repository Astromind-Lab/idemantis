import {
  sha256Digest,
  type AdapterInstanceId,
  type AttemptId,
  type CommandId,
  type EffectId,
  type OperationCapabilities,
  type RunId,
} from "@idemantis/contracts"
import {
  defineMission,
  defineOperation,
  effectState,
  executionBindingDigest,
  finalState,
  fromInput,
  safeRetry,
} from "@idemantis/sdk"
import Type from "typebox"
import { describe, expect, it } from "vitest"
import {
  AdapterExecutor,
  activeHandoffDecision,
  buildCapabilityManifest,
  createAdapterBinding,
  defineAdapterOperation,
  runDuplicateSafetyConformance,
  runFenceConformance,
  uncertainRecoveryDecision,
  type AdapterInvocationContext,
  type AdapterTargetMetrics,
} from "../src/index.js"

const inputSchema = Type.Object(
  { deduplication_key: Type.String(), payload: Type.String() },
  { additionalProperties: false },
)
const outputSchema = Type.Object({ reference: Type.String() }, { additionalProperties: false })
const subjectSchema = Type.Object(
  { deduplication_key: Type.String() },
  { additionalProperties: false },
)
const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as RunId
const effectId = "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV" as EffectId
const adapterInstanceId = "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV" as AdapterInstanceId
const digest = `sha256:${"d".repeat(64)}` as const

const identity = {
  first: {
    command: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV" as CommandId,
    attempt: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV" as AttemptId,
  },
  retry: {
    command: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAW" as CommandId,
    attempt: "att_01ARZ3NDEKTSV4RRFFQ69G5FAW" as AttemptId,
  },
  reconcile: {
    command: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAX" as CommandId,
    attempt: "att_01ARZ3NDEKTSV4RRFFQ69G5FAX" as AttemptId,
  },
}

function capabilities(
  duplicateSafety: OperationCapabilities["duplicate_safety"],
): OperationCapabilities {
  return {
    duplicate_safety: duplicateSafety,
    reconciliation: duplicateSafety === "reconciliation_only" ? "authoritative" : "none",
    fencing_mode: "none",
    fencing_scope: "subject",
    handoff_mode: "settle_first",
    execution_location: "either",
    cancellation: false,
    compensation: false,
    observation_stream: false,
  }
}

function context(commandId: CommandId, attemptId: AttemptId, epoch = 1): AdapterInvocationContext {
  return {
    adapter_instance_id: adapterInstanceId,
    run_id: runId,
    command_id: commandId,
    attempt_id: attemptId,
    effect_id: effectId,
    ownership_epoch: epoch,
    deadline: "2026-10-23T12:01:00Z",
    source_id: "adapter-test",
    signal: new AbortController().signal,
  }
}

function metricSnapshot(metrics: AdapterTargetMetrics): AdapterTargetMetrics {
  return structuredClone(metrics)
}

function duplicateFixture(
  duplicateSafety: OperationCapabilities["duplicate_safety"],
  key = `target.${duplicateSafety.replace("_", "-")}`,
) {
  const metrics: AdapterTargetMetrics = {
    issue_requests: 0,
    reconciliation_requests: 0,
    applied_effects: 0,
    applied_effect_ids: [],
    stale_rejections: 0,
  }
  const applied = new Set<string>()
  let firstResponse = true
  const duplicateCapabilities = capabilities(duplicateSafety)
  const reconciliation = duplicateSafety === "reconciliation_only"

  const operation = defineAdapterOperation({
    key,
    adapter: "target",
    operation: key,
    schemaVersion: 1,
    inputSchema,
    outputSchema,
    ...(reconciliation ? { subjectSchema } : {}),
    effectClass: "resource_reservation",
    consequence: "compensatable",
    capabilities: duplicateCapabilities,
    timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
    ...(reconciliation ? { reconciliationLookupFields: ["deduplication_key"] } : {}),
    async issue(invocation, input) {
      metrics.issue_requests += 1
      const applicationKey =
        duplicateSafety === "native_idempotency" ? invocation.effect_id : input.deduplication_key
      if (!applied.has(applicationKey)) {
        applied.add(applicationKey)
        metrics.applied_effects += 1
        metrics.applied_effect_ids.push(invocation.effect_id)
      }
      if (firstResponse) {
        firstResponse = false
        return { result: "uncertain", evidence: { reason: "response_lost" } }
      }
      return { result: "completed", output: { reference: applicationKey } }
    },
    ...(reconciliation
      ? {
          async reconcile(
            _invocation: AdapterInvocationContext,
            subject: { deduplication_key: string },
          ) {
            metrics.reconciliation_requests += 1
            return applied.has(subject.deduplication_key)
              ? {
                  result: "confirmed_completed" as const,
                  output: { reference: subject.deduplication_key },
                }
              : { result: "confirmed_not_applied" as const }
          },
        }
      : {}),
  })

  return { operation, inspect: () => metricSnapshot(metrics) }
}

describe("adapter descriptors and manifests", () => {
  it("builds an order-independent canonical capability manifest and binding", () => {
    const first = duplicateFixture("native_idempotency", "target.first").operation
    const second = duplicateFixture("keyed_uniqueness", "target.second").operation
    const forward = buildCapabilityManifest({
      adapter: "target",
      adapter_version: "1.0.0",
      adapter_package_digest: digest,
      operations: [first, second],
    })
    const reverse = buildCapabilityManifest({
      adapter: "target",
      adapter_version: "1.0.0",
      adapter_package_digest: digest,
      operations: [second, first],
    })
    const binding = createAdapterBinding(forward, adapterInstanceId, "target.first")

    expect(forward).toEqual(reverse)
    expect(forward.capability_manifest_digest).toMatch(/^sha256:[a-f0-9]{64}$/)
    expect(binding).toMatchObject({
      operationKey: "target.first",
      reference: {
        adapter_instance_id: adapterInstanceId,
        adapter_package_digest: digest,
        capability_manifest_digest: forward.capability_manifest_digest,
        operation: "target.first",
        operation_schema_version: 1,
      },
      capabilities: { duplicate_safety: "native_idempotency" },
    })
    expect(Object.isFrozen(forward.manifest)).toBe(true)
  })

  it("produces a binding accepted by the mission SDK", () => {
    const adapterOperation = duplicateFixture("native_idempotency", "target.bound").operation
    const descriptor = adapterOperation.descriptor
    const missionOperation = defineOperation({
      key: descriptor.key,
      adapter: descriptor.adapter,
      operation: descriptor.operation,
      schemaVersion: descriptor.operation_schema_version,
      inputSchema,
      outputSchema,
      effectClass: descriptor.effect_class,
      consequence: descriptor.consequence,
      capabilities: descriptor.capabilities,
    })
    const mission = defineMission({
      id: "adapter-binding",
      version: "1.0.0",
      input: inputSchema,
      initial: "apply",
      states: {
        apply: effectState({
          operation: missionOperation,
          effectSlot: "apply",
          input: fromInput("payload"),
          retry: safeRetry({ attempts: 2 }),
          onSettled: "complete",
          onFailed: "failed",
          onUncertain: "failed",
        }),
        complete: finalState("completed"),
        failed: finalState("failed"),
      },
    })
    const bundle = buildCapabilityManifest({
      adapter: "target",
      adapter_version: "1.0.0",
      adapter_package_digest: digest,
      operations: [adapterOperation],
    })
    const binding = createAdapterBinding(bundle, adapterInstanceId, descriptor.key)

    expect(executionBindingDigest(mission, [binding])).toMatch(/^sha256:[a-f0-9]{64}$/)
  })

  it("rejects capability claims that do not match runtime behavior", () => {
    expect(() =>
      defineAdapterOperation({
        key: "target.invalid",
        adapter: "target",
        operation: "target.invalid",
        schemaVersion: 1,
        inputSchema,
        outputSchema,
        effectClass: "record_mutation",
        consequence: "irreversible",
        capabilities: {
          ...capabilities("reconciliation_only"),
          reconciliation: "best_effort",
        },
        timeout: { minimumMs: 10, defaultMs: 100, maximumMs: 1_000 },
        issue: async () => ({ result: "uncertain" }),
      }),
    ).toThrowError(expect.objectContaining({ code: "INVALID_CAPABILITY_COMBINATION" }))
  })

  it("rejects invalid outputs without exposing their contents", async () => {
    const operation = defineAdapterOperation({
      key: "target.invalid-output",
      adapter: "target",
      operation: "target.invalid-output",
      schemaVersion: 1,
      inputSchema,
      outputSchema,
      effectClass: "notification",
      consequence: "compensatable",
      capabilities: capabilities("native_idempotency"),
      timeout: { minimumMs: 10, defaultMs: 100, maximumMs: 1_000 },
      issue: async () => ({ result: "completed", output: { secret: "must-not-leak" } }) as never,
    })

    await expect(
      operation.issue(context(identity.first.command, identity.first.attempt), {
        deduplication_key: "job-1",
        payload: "value",
      }),
    ).rejects.toEqual(
      expect.objectContaining({
        code: "OUTPUT_INVALID",
        message: expect.not.stringContaining("must-not-leak"),
      }),
    )
  })

  it("supports subject operations without requiring reconciliation", async () => {
    const operation = defineAdapterOperation({
      key: "target.cancel",
      adapter: "target",
      operation: "target.cancel",
      schemaVersion: 1,
      inputSchema,
      outputSchema,
      subjectSchema,
      effectClass: "resource_reservation",
      consequence: "compensatable",
      capabilities: {
        ...capabilities("native_idempotency"),
        cancellation: true,
        compensation: true,
        handoff_mode: "cancel_first",
      },
      timeout: { minimumMs: 10, defaultMs: 100, maximumMs: 1_000 },
      issue: async () => ({ result: "accepted", external_reference: "reservation-1" }),
      cancel: async () => ({ result: "confirmed_stopped" }),
      compensate: async (_invocation, subject) => ({
        result: "completed",
        output: { reference: subject.deduplication_key },
      }),
    })
    const invocation = context(identity.first.command, identity.first.attempt)
    const subject = { deduplication_key: "job-1" }

    expect(operation.descriptor.subject_schema).toEqual(subjectSchema)
    await expect(operation.cancel?.(invocation, subject)).resolves.toEqual({
      result: "confirmed_stopped",
    })
    await expect(operation.compensate?.(invocation, subject)).resolves.toEqual({
      result: "completed",
      output: { reference: "job-1" },
    })
  })

  it("prevents best-effort reconciliation from proving non-application", async () => {
    const operation = defineAdapterOperation({
      key: "target.inspect",
      adapter: "target",
      operation: "target.inspect",
      schemaVersion: 1,
      inputSchema,
      outputSchema,
      subjectSchema,
      effectClass: "record_mutation",
      consequence: "irreversible",
      capabilities: {
        ...capabilities("native_idempotency"),
        reconciliation: "best_effort",
      },
      timeout: { minimumMs: 10, defaultMs: 100, maximumMs: 1_000 },
      reconciliationLookupFields: ["deduplication_key"],
      issue: async () => ({ result: "uncertain" }),
      reconcile: async () => ({ result: "confirmed_not_applied" }),
    })

    await expect(
      operation.reconcile?.(context(identity.reconcile.command, identity.reconcile.attempt), {
        deduplication_key: "job-1",
      }),
    ).rejects.toEqual(expect.objectContaining({ code: "RESULT_INVALID" }))
  })
})

describe("adapter executor", () => {
  it("propagates durable identity and refuses expired work before adapter I/O", async () => {
    let calls = 0
    let received: AdapterInvocationContext | undefined
    const operation = defineAdapterOperation({
      key: "target.execute",
      adapter: "target",
      operation: "target.execute",
      schemaVersion: 1,
      inputSchema,
      outputSchema,
      effectClass: "physical_actuation",
      consequence: "compensatable",
      capabilities: capabilities("native_idempotency"),
      timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
      async issue(invocation) {
        calls += 1
        received = invocation
        return { result: "completed", output: { reference: invocation.effect_id } }
      },
    })
    const command = {
      command_id: identity.first.command,
      run_id: runId,
      causing_event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      command_slot: "effect.execute.issue.1",
      kind: "effect.issue",
      status: "claimed",
      ownership_epoch: 1,
      payload: {
        effect_id: effectId,
        operation: "target.execute",
        input: { deduplication_key: "job-1", payload: "value" },
      },
      created_at: "2026-10-23T12:00:00Z",
      claimed_by: "runner-1",
      claim_deadline: "2026-10-23T12:00:01Z",
    } as const
    const attempt = {
      attempt_id: identity.first.attempt,
      command_id: identity.first.command,
      effect_id: effectId,
      status: "claimed",
      ownership_epoch: 1,
      claimed_by: "runner-1",
      claim_deadline: "2026-10-23T12:00:01Z",
      created_at: "2026-10-23T12:00:00Z",
      updated_at: "2026-10-23T12:00:00Z",
    } as const
    let now = new Date("2026-10-23T12:00:00Z")
    const executor = new AdapterExecutor({
      registrations: [{ adapter_instance_id: adapterInstanceId, operation }],
      source_id: "runner-1",
      location: "runner",
      clock: { now: () => now },
    })

    expect(await executor.issue(command, attempt)).toBe("completed")
    expect(received).toMatchObject({
      adapter_instance_id: adapterInstanceId,
      run_id: runId,
      command_id: command.command_id,
      attempt_id: attempt.attempt_id,
      effect_id: effectId,
      ownership_epoch: 1,
      deadline: attempt.claim_deadline,
      source_id: "runner-1",
    })
    expect(received?.signal.aborted).toBe(false)

    now = new Date("2026-10-23T12:00:02Z")
    expect(await executor.issue(command, attempt)).toBe("not_sent")
    expect(calls).toBe(1)
  })
})

describe("duplicate-safety conformance", () => {
  for (const duplicateSafety of [
    "native_idempotency",
    "keyed_uniqueness",
    "reconciliation_only",
    "none",
  ] as const) {
    it(`enforces ${duplicateSafety} recovery against target-side counts`, async () => {
      const fixture = duplicateFixture(duplicateSafety)
      const report = await runDuplicateSafetyConformance({
        operation: fixture.operation,
        input: { deduplication_key: "job-1", payload: "value" },
        subject: { deduplication_key: "job-1" },
        first_context: context(identity.first.command, identity.first.attempt),
        retry_context: context(identity.retry.command, identity.retry.attempt),
        reconciliation_context: context(identity.reconcile.command, identity.reconcile.attempt),
        inspect: fixture.inspect,
      })

      expect(report.passed).toBe(true)
      expect(report.metrics.applied_effects).toBe(1)
      expect(report.metrics.applied_effect_ids).toEqual([effectId])
      expect(report.decision).toBe(
        duplicateSafety === "native_idempotency" || duplicateSafety === "keyed_uniqueness"
          ? "retry_same_effect"
          : duplicateSafety === "reconciliation_only"
            ? "reconcile"
            : "stop_uncertain",
      )
      expect(report.metrics.issue_requests).toBe(report.decision === "retry_same_effect" ? 2 : 1)
      expect(report.metrics.reconciliation_requests).toBe(report.decision === "reconcile" ? 1 : 0)
    })
  }
})

describe("fencing and handoff conformance", () => {
  it("rejects a delayed stale request after fence activation", async () => {
    const metrics: AdapterTargetMetrics = {
      issue_requests: 0,
      reconciliation_requests: 0,
      applied_effects: 0,
      applied_effect_ids: [],
      stale_rejections: 0,
    }
    let activeEpoch = 1
    let release: (() => void) | undefined
    let markDelayed: (() => void) | undefined
    const delayed = new Promise<void>((resolve) => {
      markDelayed = resolve
    })
    const gate = new Promise<void>((resolve) => {
      release = resolve
    })
    const operation = defineAdapterOperation({
      key: "target.fenced",
      adapter: "target",
      operation: "target.fenced",
      schemaVersion: 1,
      inputSchema,
      outputSchema,
      effectClass: "access_mutation",
      consequence: "reversible",
      capabilities: {
        ...capabilities("native_idempotency"),
        fencing_mode: "application_epoch",
        fencing_scope: "subject",
        handoff_mode: "transferable",
      },
      timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
      maximumFenceActivationDelayMs: 100,
      async issue(invocation) {
        metrics.issue_requests += 1
        markDelayed?.()
        await gate
        if (invocation.ownership_epoch < activeEpoch) {
          metrics.stale_rejections += 1
          return { result: "rejected", evidence: { reason: "stale_epoch" } }
        }
        metrics.applied_effects += 1
        metrics.applied_effect_ids.push(invocation.effect_id)
        return { result: "completed", output: { reference: invocation.effect_id } }
      },
      async activateFence(activation) {
        activeEpoch = activation.ownership_epoch
        return {
          adapter_instance_id: activation.adapter_instance_id,
          scope: "subject",
          scope_reference: activation.scope_reference,
          ownership_epoch: activation.ownership_epoch,
          activation_reference: "fence-2",
          effective_at: activation.requested_at,
          maximum_activation_delay_ms: 100,
          evidence_digest: sha256Digest({ epoch: activation.ownership_epoch }),
        }
      },
    })

    const report = await runFenceConformance({
      operation,
      input: { deduplication_key: "job-1", payload: "value" },
      stale_context: context(identity.first.command, identity.first.attempt, 1),
      activation_context: {
        adapter_instance_id: adapterInstanceId,
        operation_key: "target.fenced",
        scope_reference: "job-1",
        ownership_epoch: 2,
        requested_at: "2026-10-23T12:00:00Z",
        signal: new AbortController().signal,
      },
      wait_until_delayed: () => delayed,
      release_delayed: () => release?.(),
      inspect: () => metricSnapshot(metrics),
    })

    expect(report).toMatchObject({
      passed: true,
      stale_result: "rejected",
      proof: { ownership_epoch: 2, scope_reference: "job-1" },
      metrics: { issue_requests: 1, applied_effects: 0, stale_rejections: 1 },
    })
  })

  it("maps every active-effect handoff mode to an explicit decision", () => {
    const base = capabilities("native_idempotency")

    expect(activeHandoffDecision({ ...base, handoff_mode: "transferable" })).toBe("transfer")
    expect(activeHandoffDecision({ ...base, handoff_mode: "settle_first" })).toBe("settle")
    expect(activeHandoffDecision({ ...base, handoff_mode: "cancel_first" })).toBe("cancel")
    expect(activeHandoffDecision({ ...base, handoff_mode: "unsupported" })).toBe("block")
    expect(uncertainRecoveryDecision(base)).toBe("retry_same_effect")
  })
})
