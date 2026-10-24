import {
  type AdapterBinding,
  AdapterExecutor,
  type AdapterOperation,
  type AdapterRegistration,
  buildCapabilityManifest,
  createAdapterBinding,
  type RuntimeClock,
} from "@idemantis/adapter-kit"
import type { AdapterInstanceId, Digest, SourceId } from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import { ExecutionBindingError, executionBindingDigest } from "@idemantis/sdk"

export interface HostedAdapter {
  adapter: string
  adapter_version: string
  adapter_package_digest: `sha256:${string}`
  adapter_instance_id: AdapterInstanceId
  operations: readonly AdapterOperation<unknown, unknown, unknown>[]
}

export interface AdapterHostOptions {
  adapters: readonly HostedAdapter[]
  source_id: SourceId
  clock?: RuntimeClock
}

type DefinitionReference = Pick<MissionRuntimeDefinition, "manifest" | "definition_digest">

export class AdapterHost {
  readonly executor: AdapterExecutor
  readonly #bindings = new Map<string, AdapterBinding>()

  constructor(options: AdapterHostOptions) {
    const registrations: AdapterRegistration[] = []
    for (const adapter of options.adapters) {
      const bundle = buildCapabilityManifest({
        adapter: adapter.adapter,
        adapter_version: adapter.adapter_version,
        adapter_package_digest: adapter.adapter_package_digest,
        operations: adapter.operations,
      })
      for (const operation of adapter.operations) {
        registrations.push({ adapter_instance_id: adapter.adapter_instance_id, operation })
        this.#bindings.set(
          operation.descriptor.key,
          createAdapterBinding(bundle, adapter.adapter_instance_id, operation.descriptor.key),
        )
      }
    }
    this.executor = new AdapterExecutor({
      registrations,
      source_id: options.source_id,
      location: "runner",
      ...(options.clock ? { clock: options.clock } : {}),
    })
  }

  operations(): string[] {
    return [...this.#bindings.keys()].sort()
  }

  bindingDigest(definition: DefinitionReference): Digest {
    const bindings = Object.keys(definition.manifest.operations).flatMap((key) => {
      const binding = this.#bindings.get(key)
      return binding ? [binding] : []
    })
    return executionBindingDigest(
      { manifest: definition.manifest, definitionDigest: definition.definition_digest },
      bindings,
    )
  }

  bind(definition: DefinitionReference): MissionRuntimeDefinition {
    return {
      manifest: structuredClone(definition.manifest),
      definition_digest: definition.definition_digest,
      execution_binding_digest: this.bindingDigest(definition),
    }
  }

  accepts(runtime: MissionRuntimeDefinition): boolean {
    try {
      return this.bindingDigest(runtime) === runtime.execution_binding_digest
    } catch (error) {
      if (error instanceof ExecutionBindingError) {
        return false
      }
      throw error
    }
  }
}
