import {
  AdapterInstanceIdSchema,
  canonicalJson,
  createContractParser,
  sha256Digest,
  type AdapterInstanceId,
  type JsonObject,
} from "@idemantis/contracts"
import Schema from "typebox/schema"
import { AdapterKitError } from "./errors.js"
import { AdapterCapabilityManifestSchema } from "./schemas.js"
import type {
  AdapterBinding,
  AdapterCapabilityBundle,
  AdapterOperation,
  AdapterRegistration,
} from "./types.js"

const parseManifest = createContractParser(AdapterCapabilityManifestSchema)

function freeze<Value>(value: Value): Value {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) {
      freeze(child)
    }
    Object.freeze(value)
  }
  return value
}

export function buildCapabilityManifest(options: {
  adapter: string
  adapter_version: string
  adapter_package_digest: `sha256:${string}`
  operations: readonly AdapterOperation<unknown, unknown, unknown>[]
}): AdapterCapabilityBundle {
  const operations = options.operations
    .map((operation) => structuredClone(operation.descriptor))
    .sort((left, right) => left.key.localeCompare(right.key))
  const keys = operations.map((operation) => operation.key)

  if (new Set(keys).size !== keys.length) {
    throw new AdapterKitError(
      "DUPLICATE_OPERATION",
      `Capability manifest ${options.adapter} repeats an operation key`,
    )
  }
  if (operations.some((operation) => operation.adapter !== options.adapter)) {
    throw new AdapterKitError(
      "INVALID_CAPABILITY_MANIFEST",
      `Capability manifest ${options.adapter} contains an operation from another adapter`,
    )
  }

  let manifest: ReturnType<typeof parseManifest>
  try {
    manifest = parseManifest({
      adapter: options.adapter,
      adapter_version: options.adapter_version,
      adapter_package_digest: options.adapter_package_digest,
      operations,
    })
  } catch {
    throw new AdapterKitError(
      "INVALID_CAPABILITY_MANIFEST",
      `Capability manifest ${options.adapter} is invalid`,
    )
  }

  const canonicalManifest = canonicalJson(manifest as unknown as JsonObject)
  return freeze({
    manifest: structuredClone(manifest),
    canonical_manifest: canonicalManifest,
    capability_manifest_digest: sha256Digest(manifest as unknown as JsonObject),
  })
}

export function createAdapterBinding(
  bundle: AdapterCapabilityBundle,
  adapterInstanceId: AdapterInstanceId,
  operationKey: string,
): AdapterBinding {
  if (!Schema.Check(AdapterInstanceIdSchema, adapterInstanceId)) {
    throw new AdapterKitError(
      "INVALID_CAPABILITY_MANIFEST",
      "Adapter instance identity is invalid",
      operationKey,
    )
  }
  const descriptor = bundle.manifest.operations.find((operation) => operation.key === operationKey)
  if (!descriptor) {
    throw new AdapterKitError(
      "OPERATION_NOT_FOUND",
      `Operation ${operationKey} is not in the capability manifest`,
      operationKey,
    )
  }

  return freeze({
    operationKey,
    reference: {
      adapter_instance_id: adapterInstanceId,
      adapter_package_digest: bundle.manifest.adapter_package_digest,
      capability_manifest_digest: bundle.capability_manifest_digest,
      operation: descriptor.operation,
      operation_schema_version: descriptor.operation_schema_version,
    },
    capabilities: structuredClone(descriptor.capabilities),
  })
}

export class AdapterRegistry {
  readonly #registrations = new Map<string, AdapterRegistration>()

  constructor(registrations: readonly AdapterRegistration[]) {
    for (const registration of registrations) {
      if (!Schema.Check(AdapterInstanceIdSchema, registration.adapter_instance_id)) {
        throw new AdapterKitError(
          "INVALID_DESCRIPTOR",
          `Adapter instance for ${registration.operation.descriptor.key} is invalid`,
          registration.operation.descriptor.key,
        )
      }
      const key = registration.operation.descriptor.key
      if (this.#registrations.has(key)) {
        throw new AdapterKitError(
          "DUPLICATE_OPERATION",
          `Operation ${key} was registered more than once`,
          key,
        )
      }
      this.#registrations.set(key, registration)
    }
  }

  resolve(operationKey: string): AdapterRegistration {
    const registration = this.#registrations.get(operationKey)
    if (!registration) {
      throw new AdapterKitError(
        "OPERATION_NOT_FOUND",
        `Operation ${operationKey} is not registered`,
        operationKey,
      )
    }
    return registration
  }

  list(): AdapterRegistration[] {
    return [...this.#registrations.values()].sort((left, right) =>
      left.operation.descriptor.key.localeCompare(right.operation.descriptor.key),
    )
  }
}
