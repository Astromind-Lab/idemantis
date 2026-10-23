import {
  AdapterOperationReferenceSchema,
  OperationCapabilitiesSchema,
  canonicalJson,
  sha256Digest,
  type Digest,
  type JsonObject,
} from "@idemantis/contracts"
import Schema from "typebox/schema"
import { ExecutionBindingError } from "./errors.js"
import type {
  CompiledMission,
  ExecutionBinding,
  ExecutionBindingIssue,
  OperationManifest,
} from "./types.js"

const operationKeyPattern = /^[a-z][a-zA-Z0-9]*(?:[._-][a-zA-Z0-9]+)*$/

function bindingIssue(
  code: ExecutionBindingIssue["code"],
  operationKey: string,
  message: string,
  resolution: string,
): ExecutionBindingIssue {
  return { code, operationKey, message, resolution }
}

function sameCapabilities(
  expected: OperationManifest["capabilities"],
  actual: ExecutionBinding["capabilities"],
): boolean {
  return (
    canonicalJson(expected as unknown as JsonObject) ===
    canonicalJson(actual as unknown as JsonObject)
  )
}

/** Validates resolved operations and returns their canonical binding digest. */
export function executionBindingDigest(
  mission: CompiledMission,
  bindings: readonly ExecutionBinding[],
): Digest {
  const issues: ExecutionBindingIssue[] = []
  const supplied = new Map<string, ExecutionBinding>()

  for (const binding of bindings) {
    const operationKeyValid =
      typeof binding.operationKey === "string" && operationKeyPattern.test(binding.operationKey)
    const operationKey = operationKeyValid ? binding.operationKey : "<invalid>"
    const valid =
      operationKeyValid &&
      Schema.Check(AdapterOperationReferenceSchema, binding.reference) &&
      Schema.Check(OperationCapabilitiesSchema, binding.capabilities)

    if (!valid) {
      issues.push(
        bindingIssue(
          "INVALID_BINDING",
          operationKey,
          `Binding ${operationKey} has an invalid operation key, reference, or capability manifest`,
          "Provide a validated adapter reference and operation capabilities",
        ),
      )
      continue
    }

    if (supplied.has(operationKey)) {
      issues.push(
        bindingIssue(
          "DUPLICATE_BINDING",
          operationKey,
          `Operation ${operationKey} was bound more than once`,
          "Provide exactly one binding for each required operation",
        ),
      )
      continue
    }

    supplied.set(operationKey, binding)
  }

  for (const operationKey of Object.keys(mission.manifest.operations).sort()) {
    const expected = mission.manifest.operations[operationKey] as OperationManifest
    const binding = supplied.get(operationKey)

    if (!binding) {
      issues.push(
        bindingIssue(
          "MISSING_BINDING",
          operationKey,
          `Required operation ${operationKey} has no adapter binding`,
          "Bind every operation before admitting a mission",
        ),
      )
      continue
    }

    if (binding.reference.operation !== expected.operation) {
      issues.push(
        bindingIssue(
          "BINDING_OPERATION_MISMATCH",
          operationKey,
          `Binding resolves ${binding.reference.operation} instead of ${expected.operation}`,
          "Bind the operation named by the compiled manifest",
        ),
      )
    }

    if (binding.reference.operation_schema_version !== expected.operation_schema_version) {
      issues.push(
        bindingIssue(
          "BINDING_SCHEMA_VERSION_MISMATCH",
          operationKey,
          `Binding schema version ${binding.reference.operation_schema_version} does not match ${expected.operation_schema_version}`,
          "Bind the exact operation schema version compiled into the mission",
        ),
      )
    }

    if (!sameCapabilities(expected.capabilities, binding.capabilities)) {
      issues.push(
        bindingIssue(
          "BINDING_CAPABILITY_MISMATCH",
          operationKey,
          `Binding capabilities for ${operationKey} differ from the compiled manifest`,
          "Recompile against the adapter capability manifest being bound",
        ),
      )
    }
  }

  for (const operationKey of [...supplied.keys()].sort()) {
    if (!Object.hasOwn(mission.manifest.operations, operationKey)) {
      issues.push(
        bindingIssue(
          "UNEXPECTED_BINDING",
          operationKey,
          `Operation ${operationKey} is not required by the mission`,
          "Remove the extra binding",
        ),
      )
    }
  }

  if (issues.length > 0) {
    throw new ExecutionBindingError(
      issues.sort((left, right) =>
        left.operationKey === right.operationKey
          ? left.code.localeCompare(right.code)
          : left.operationKey.localeCompare(right.operationKey),
      ),
    )
  }

  const canonicalBindings = [...supplied.values()]
    .sort((left, right) => left.operationKey.localeCompare(right.operationKey))
    .map((binding) => ({
      operation_key: binding.operationKey,
      reference: binding.reference,
      capabilities: binding.capabilities,
    }))

  return sha256Digest({
    definition_digest: mission.definitionDigest,
    bindings: canonicalBindings,
  } as unknown as JsonObject)
}
