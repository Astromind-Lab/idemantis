import type { FenceProof } from "@idemantis/contracts"
import { AdapterKitError } from "./errors.js"
import { uncertainRecoveryDecision } from "./policy.js"
import type {
  DuplicateSafetyConformanceOptions,
  DuplicateSafetyConformanceReport,
  FenceConformanceOptions,
  FenceConformanceReport,
} from "./types.js"

function fail(message: string, operationKey: string): never {
  throw new AdapterKitError("CONFORMANCE_FAILED", message, operationKey)
}

function validateMetrics(
  operationKey: string,
  effectId: string,
  metrics: Awaited<
    ReturnType<DuplicateSafetyConformanceOptions<unknown, unknown, unknown>["inspect"]>
  >,
  expectedIssueRequests: number,
  expectedReconciliationRequests: number,
): void {
  if (
    !Number.isSafeInteger(metrics.issue_requests) ||
    !Number.isSafeInteger(metrics.reconciliation_requests) ||
    !Number.isSafeInteger(metrics.applied_effects) ||
    !Number.isSafeInteger(metrics.stale_rejections) ||
    metrics.issue_requests !== expectedIssueRequests ||
    metrics.reconciliation_requests !== expectedReconciliationRequests ||
    metrics.applied_effects !== 1 ||
    metrics.applied_effect_ids.length !== metrics.applied_effects ||
    metrics.applied_effect_ids.some((value) => value !== effectId)
  ) {
    fail(`Target metrics for ${operationKey} violate its duplicate-safety policy`, operationKey)
  }
}

export async function runDuplicateSafetyConformance<Input, Output, Subject>(
  options: DuplicateSafetyConformanceOptions<Input, Output, Subject>,
): Promise<DuplicateSafetyConformanceReport> {
  const key = options.operation.descriptor.key
  const first = options.first_context
  const retry = options.retry_context
  const reconciliation = options.reconciliation_context

  if (
    first.effect_id !== retry.effect_id ||
    first.effect_id !== reconciliation.effect_id ||
    new Set([first.attempt_id, retry.attempt_id, reconciliation.attempt_id]).size !== 3 ||
    new Set([first.command_id, retry.command_id, reconciliation.command_id]).size !== 3 ||
    first.run_id !== retry.run_id ||
    first.run_id !== reconciliation.run_id ||
    first.adapter_instance_id !== retry.adapter_instance_id ||
    first.adapter_instance_id !== reconciliation.adapter_instance_id ||
    first.ownership_epoch !== retry.ownership_epoch ||
    first.ownership_epoch !== reconciliation.ownership_epoch
  ) {
    fail(`Conformance contexts for ${key} do not preserve effect and epoch identity`, key)
  }

  const firstEnvelope = await options.operation.issue(first, options.input)
  if (firstEnvelope.result !== "uncertain") {
    fail(`Conformance operation ${key} did not produce an ambiguous first result`, key)
  }

  const decision = uncertainRecoveryDecision(options.operation.descriptor.capabilities)
  let recoveryResult: string | undefined
  let expectedIssueRequests = 1
  let expectedReconciliationRequests = 0

  if (decision === "retry_same_effect") {
    recoveryResult = (await options.operation.issue(retry, options.input)).result
    expectedIssueRequests = 2
  } else if (decision === "reconcile") {
    if (!options.operation.reconcile) {
      fail(`Conformance operation ${key} has no reconciliation implementation`, key)
    }
    recoveryResult = (await options.operation.reconcile(reconciliation, options.subject)).result
    expectedReconciliationRequests = 1
  }

  if (decision === "reconcile" && recoveryResult !== "confirmed_completed") {
    fail(`Authoritative reconciliation for ${key} did not find the applied effect`, key)
  }

  const metrics = await options.inspect()
  validateMetrics(
    key,
    first.effect_id,
    metrics,
    expectedIssueRequests,
    expectedReconciliationRequests,
  )

  return {
    operation_key: key,
    duplicate_safety: options.operation.descriptor.capabilities.duplicate_safety,
    decision,
    first_result: firstEnvelope.result,
    ...(recoveryResult ? { recovery_result: recoveryResult } : {}),
    metrics,
    passed: true,
  }
}

export async function runFenceConformance<Input, Output, Subject>(
  options: FenceConformanceOptions<Input, Output, Subject>,
): Promise<FenceConformanceReport> {
  const key = options.operation.descriptor.key
  if (!options.operation.activateFence) {
    fail(`Conformance operation ${key} has no fence activation implementation`, key)
  }
  if (
    options.operation.descriptor.capabilities.fencing_mode === "none" ||
    options.stale_context.ownership_epoch >= options.activation_context.ownership_epoch ||
    options.stale_context.adapter_instance_id !== options.activation_context.adapter_instance_id
  ) {
    fail(`Fence conformance contexts for ${key} are invalid`, key)
  }

  const stalePromise = options.operation.issue(options.stale_context, options.input)
  await options.wait_until_delayed()

  let proof: FenceProof
  try {
    proof = await options.operation.activateFence(options.activation_context)
  } catch (error) {
    options.release_delayed()
    await stalePromise.catch(() => undefined)
    throw error
  }
  options.release_delayed()
  const staleEnvelope = await stalePromise
  const metrics = await options.inspect()

  if (
    staleEnvelope.result !== "rejected" ||
    metrics.issue_requests !== 1 ||
    metrics.applied_effects !== 0 ||
    metrics.applied_effect_ids.length !== 0 ||
    metrics.stale_rejections !== 1
  ) {
    fail(`Delayed stale request for ${key} was not rejected at the target`, key)
  }

  return {
    operation_key: key,
    stale_result: staleEnvelope.result,
    proof,
    metrics,
    passed: true,
  }
}
