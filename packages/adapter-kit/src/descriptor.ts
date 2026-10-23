import {
  AdapterInstanceIdSchema,
  FenceProofSchema,
  JsonObjectSchema,
  TimestampSchema,
  createContractParser,
  type FenceProof,
  type JsonObject,
} from "@idemantis/contracts"
import Schema from "typebox/schema"
import { AdapterKitError } from "./errors.js"
import {
  AdapterInvocationIdentitySchema,
  AdapterOperationDescriptorSchema,
  CancellationResultEnvelopeSchema,
  IssueResultEnvelopeSchema,
  ReconciliationResultEnvelopeSchema,
} from "./schemas.js"
import type {
  AdapterInvocationContext,
  AdapterOperation,
  AdapterOperationDefinition,
  CancellationResultEnvelope,
  FenceActivationContext,
  IssueResultEnvelope,
  ReconciliationResultEnvelope,
} from "./types.js"
import type { TSchema } from "typebox"

const parseDescriptor = createContractParser(AdapterOperationDescriptorSchema)
const parseIssueEnvelope = createContractParser(IssueResultEnvelopeSchema)
const parseReconciliationEnvelope = createContractParser(ReconciliationResultEnvelopeSchema)
const parseCancellationEnvelope = createContractParser(CancellationResultEnvelopeSchema)
const parseFenceProof = createContractParser(FenceProofSchema)

function fail(
  code: ConstructorParameters<typeof AdapterKitError>[0],
  message: string,
  key?: string,
): never {
  throw new AdapterKitError(code, message, key)
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

function cloneFreeze<Value>(value: Value): Value {
  return freeze(structuredClone(value))
}

function schemaObject(schema: TSchema, key: string, label: string): JsonObject {
  try {
    Schema.Compile(schema)
  } catch {
    fail("INVALID_DESCRIPTOR", `${label} for ${key} cannot be compiled`, key)
  }

  const value = JSON.parse(JSON.stringify(schema)) as unknown
  if (!Schema.Check(JsonObjectSchema, value)) {
    fail("INVALID_DESCRIPTOR", `${label} for ${key} is not a JSON schema object`, key)
  }

  return value as JsonObject
}

function validateDescriptorBehavior<
  InputSchema extends TSchema,
  OutputSchema extends TSchema,
  SubjectSchema extends TSchema,
>(definition: AdapterOperationDefinition<InputSchema, OutputSchema, SubjectSchema>): void {
  const capabilities = definition.capabilities
  const key = definition.key
  const timeout = definition.timeout
  const usesSubject = Boolean(definition.reconcile || definition.cancel || definition.compensate)

  if (
    timeout.minimumMs > timeout.defaultMs ||
    timeout.defaultMs > timeout.maximumMs ||
    !Number.isSafeInteger(timeout.minimumMs) ||
    !Number.isSafeInteger(timeout.defaultMs) ||
    !Number.isSafeInteger(timeout.maximumMs)
  ) {
    fail("INVALID_DESCRIPTOR", `Timeout bounds for ${key} are invalid`, key)
  }

  if (capabilities.duplicate_safety === "reconciliation_only") {
    if (capabilities.reconciliation !== "authoritative" || !definition.reconcile) {
      fail(
        "INVALID_CAPABILITY_COMBINATION",
        `Operation ${key} requires authoritative reconciliation`,
        key,
      )
    }
  }
  if (capabilities.duplicate_safety === "none" && capabilities.reconciliation === "authoritative") {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      `Operation ${key} must declare reconciliation_only when authoritative lookup is its safe path`,
      key,
    )
  }

  if (capabilities.reconciliation === "none") {
    if (definition.reconcile || (definition.reconciliationLookupFields?.length ?? 0) > 0) {
      fail(
        "INVALID_CAPABILITY_COMBINATION",
        `Operation ${key} declares reconciliation behavior without the capability`,
        key,
      )
    }
  } else if (!definition.reconcile || (definition.reconciliationLookupFields?.length ?? 0) === 0) {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      `Operation ${key} must provide reconciliation behavior and lookup fields`,
      key,
    )
  }

  if (usesSubject !== Boolean(definition.subjectSchema)) {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      usesSubject
        ? `Operation ${key} must provide a subject schema`
        : `Operation ${key} declares a subject schema without a subject operation`,
      key,
    )
  }

  if (capabilities.cancellation !== Boolean(definition.cancel)) {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      `Cancellation capability for ${key} does not match its implementation`,
      key,
    )
  }
  if (capabilities.compensation !== Boolean(definition.compensate)) {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      `Compensation capability for ${key} does not match its implementation`,
      key,
    )
  }

  if (capabilities.handoff_mode === "cancel_first" && !capabilities.cancellation) {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      `Cancel-first handoff for ${key} requires cancellation support`,
      key,
    )
  }
  if (capabilities.handoff_mode === "transferable" && capabilities.fencing_mode === "none") {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      `Transferable handoff for ${key} requires target fencing`,
      key,
    )
  }

  if (capabilities.fencing_mode === "none") {
    if (definition.activateFence || definition.maximumFenceActivationDelayMs !== undefined) {
      fail(
        "INVALID_CAPABILITY_COMBINATION",
        `Operation ${key} declares fencing behavior without target fencing`,
        key,
      )
    }
  } else if (
    !definition.activateFence ||
    definition.maximumFenceActivationDelayMs === undefined ||
    !Number.isSafeInteger(definition.maximumFenceActivationDelayMs) ||
    definition.maximumFenceActivationDelayMs < 0
  ) {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      `Operation ${key} must implement bounded fence activation`,
      key,
    )
  }

  const observationTypes = definition.observationTypes ?? []
  const correlationFields = definition.observationCorrelationFields ?? []
  if (
    capabilities.observation_stream !==
    (observationTypes.length > 0 && correlationFields.length > 0)
  ) {
    fail(
      "INVALID_CAPABILITY_COMBINATION",
      `Observation capability for ${key} does not match its declared observation contract`,
      key,
    )
  }
}

function validateInvocationContext(context: AdapterInvocationContext, key: string): void {
  const { signal: _signal, ...identity } = context
  if (
    !Schema.Check(AdapterInvocationIdentitySchema, identity) ||
    typeof context.signal?.aborted !== "boolean"
  ) {
    fail("COMMAND_INVALID", `Invocation context for ${key} is invalid`, key)
  }
}

function validateFenceContext(context: FenceActivationContext, key: string): void {
  if (
    !Schema.Check(AdapterInstanceIdSchema, context.adapter_instance_id) ||
    context.operation_key !== key ||
    typeof context.scope_reference !== "string" ||
    context.scope_reference.length < 1 ||
    context.scope_reference.length > 256 ||
    !Number.isSafeInteger(context.ownership_epoch) ||
    context.ownership_epoch < 1 ||
    !Schema.Check(TimestampSchema, context.requested_at) ||
    !Number.isFinite(Date.parse(context.requested_at)) ||
    typeof context.signal?.aborted !== "boolean"
  ) {
    fail("COMMAND_INVALID", `Fence activation context for ${key} is invalid`, key)
  }
}

function validateOutput(
  envelope: IssueResultEnvelope<unknown> | ReconciliationResultEnvelope<unknown>,
  outputSchema: TSchema,
  key: string,
): void {
  if ("output" in envelope && !Schema.Check(outputSchema, envelope.output)) {
    fail("OUTPUT_INVALID", `Adapter output for ${key} does not satisfy its schema`, key)
  }
}

function issueEnvelope(value: unknown, outputSchema: TSchema, key: string) {
  try {
    const parsed = parseIssueEnvelope(value) as IssueResultEnvelope<unknown>
    validateOutput(parsed, outputSchema, key)
    return cloneFreeze(parsed)
  } catch (error) {
    if (error instanceof AdapterKitError) {
      throw error
    }
    return fail("RESULT_INVALID", `Adapter issue result for ${key} is invalid`, key)
  }
}

function reconciliationEnvelope(
  value: unknown,
  outputSchema: TSchema,
  key: string,
  authoritative: boolean,
) {
  try {
    const parsed = parseReconciliationEnvelope(value) as ReconciliationResultEnvelope<unknown>
    if (parsed.result === "confirmed_not_applied" && !authoritative) {
      fail(
        "RESULT_INVALID",
        `Best-effort reconciliation for ${key} cannot confirm non-application`,
        key,
      )
    }
    validateOutput(parsed, outputSchema, key)
    return cloneFreeze(parsed)
  } catch (error) {
    if (error instanceof AdapterKitError) {
      throw error
    }
    return fail("RESULT_INVALID", `Adapter reconciliation result for ${key} is invalid`, key)
  }
}

function cancellationEnvelope(value: unknown, key: string): CancellationResultEnvelope {
  try {
    return cloneFreeze(parseCancellationEnvelope(value)) as CancellationResultEnvelope
  } catch {
    return fail("RESULT_INVALID", `Adapter cancellation result for ${key} is invalid`, key)
  }
}

function validateProof(
  value: unknown,
  context: FenceActivationContext,
  descriptor: ReturnType<typeof parseDescriptor>,
): FenceProof {
  let proof: FenceProof
  try {
    proof = parseFenceProof(value)
  } catch {
    return fail(
      "FENCE_PROOF_INVALID",
      `Fence proof for ${descriptor.key} is invalid`,
      descriptor.key,
    )
  }

  const requestedAt = Date.parse(context.requested_at)
  const effectiveAt = Date.parse(proof.effective_at)
  const maximumDelay = descriptor.maximum_fence_activation_delay_ms
  if (
    maximumDelay === undefined ||
    proof.adapter_instance_id !== context.adapter_instance_id ||
    proof.scope !== descriptor.capabilities.fencing_scope ||
    proof.scope_reference !== context.scope_reference ||
    proof.ownership_epoch !== context.ownership_epoch ||
    proof.maximum_activation_delay_ms > maximumDelay ||
    effectiveAt < requestedAt ||
    effectiveAt > requestedAt + maximumDelay
  ) {
    return fail(
      "FENCE_PROOF_INVALID",
      `Fence proof for ${descriptor.key} does not match its activation request`,
      descriptor.key,
    )
  }

  return cloneFreeze(proof)
}

export function defineAdapterOperation<
  InputSchema extends TSchema,
  OutputSchema extends TSchema,
  SubjectSchema extends TSchema = TSchema,
>(
  definition: AdapterOperationDefinition<InputSchema, OutputSchema, SubjectSchema>,
): AdapterOperation<
  import("typebox").Static<InputSchema>,
  import("typebox").Static<OutputSchema>,
  import("typebox").Static<SubjectSchema>
> {
  validateDescriptorBehavior(definition)
  const key = definition.key
  const inputSchema = definition.inputSchema
  const outputSchema = definition.outputSchema
  const subjectSchema = definition.subjectSchema
  const descriptorValue = {
    key,
    adapter: definition.adapter,
    operation: definition.operation,
    operation_schema_version: definition.schemaVersion,
    input_schema: schemaObject(inputSchema, key, "Input schema"),
    output_schema: schemaObject(outputSchema, key, "Output schema"),
    ...(subjectSchema
      ? { subject_schema: schemaObject(subjectSchema, key, "Subject schema") }
      : {}),
    effect_class: definition.effectClass,
    consequence: definition.consequence,
    capabilities: structuredClone(definition.capabilities),
    timeout: {
      minimum_ms: definition.timeout.minimumMs,
      default_ms: definition.timeout.defaultMs,
      maximum_ms: definition.timeout.maximumMs,
    },
    ...(definition.rateLimit
      ? {
          rate_limit: {
            maximum_requests: definition.rateLimit.maximumRequests,
            window_ms: definition.rateLimit.windowMs,
          },
        }
      : {}),
    reconciliation_lookup_fields: [...(definition.reconciliationLookupFields ?? [])].sort(),
    observation_types: [...(definition.observationTypes ?? [])].sort(),
    observation_correlation_fields: [...(definition.observationCorrelationFields ?? [])].sort(),
    secret_requirements: [...(definition.secretRequirements ?? [])].sort((left, right) =>
      left.name.localeCompare(right.name),
    ),
    ...(definition.maximumFenceActivationDelayMs === undefined
      ? {}
      : { maximum_fence_activation_delay_ms: definition.maximumFenceActivationDelayMs }),
  }

  let descriptor: ReturnType<typeof parseDescriptor>
  try {
    descriptor = parseDescriptor(descriptorValue)
  } catch {
    return fail("INVALID_DESCRIPTOR", `Operation descriptor ${key} is invalid`, key)
  }

  if (
    new Set(descriptor.secret_requirements.map((item) => item.name)).size !==
    descriptor.secret_requirements.length
  ) {
    return fail("INVALID_DESCRIPTOR", `Operation ${key} repeats a secret requirement`, key)
  }

  const operation: AdapterOperation<unknown, unknown, unknown> = {
    descriptor: cloneFreeze(descriptor),
    async issue(context, input) {
      validateInvocationContext(context, key)
      if (!Schema.Check(inputSchema, input)) {
        return fail("INPUT_INVALID", `Adapter input for ${key} does not satisfy its schema`, key)
      }
      return issueEnvelope(await definition.issue(context, input), outputSchema, key)
    },
    ...(definition.reconcile
      ? {
          async reconcile(context: AdapterInvocationContext, subject: unknown) {
            validateInvocationContext(context, key)
            if (!subjectSchema || !Schema.Check(subjectSchema, subject)) {
              return fail(
                "SUBJECT_INVALID",
                `Reconciliation subject for ${key} does not satisfy its schema`,
                key,
              )
            }
            return reconciliationEnvelope(
              await definition.reconcile?.(context, subject),
              outputSchema,
              key,
              descriptor.capabilities.reconciliation === "authoritative",
            )
          },
        }
      : {}),
    ...(definition.cancel
      ? {
          async cancel(context: AdapterInvocationContext, subject: unknown) {
            validateInvocationContext(context, key)
            if (!subjectSchema || !Schema.Check(subjectSchema, subject)) {
              return fail(
                "SUBJECT_INVALID",
                `Cancellation subject for ${key} does not satisfy its schema`,
                key,
              )
            }
            return cancellationEnvelope(await definition.cancel?.(context, subject), key)
          },
        }
      : {}),
    ...(definition.compensate
      ? {
          async compensate(context: AdapterInvocationContext, subject: unknown) {
            validateInvocationContext(context, key)
            if (!subjectSchema || !Schema.Check(subjectSchema, subject)) {
              return fail(
                "SUBJECT_INVALID",
                `Compensation subject for ${key} does not satisfy its schema`,
                key,
              )
            }
            return issueEnvelope(await definition.compensate?.(context, subject), outputSchema, key)
          },
        }
      : {}),
    ...(definition.activateFence
      ? {
          async activateFence(context: FenceActivationContext) {
            validateFenceContext(context, key)
            return validateProof(await definition.activateFence?.(context), context, descriptor)
          },
        }
      : {}),
  }

  return operation as never
}
