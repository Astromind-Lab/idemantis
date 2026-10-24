import {
  type AdapterBinding,
  type AdapterOperationDescriptor,
  AdapterExecutor,
  type AdapterOperation,
  type AdapterRegistration,
  buildCapabilityManifest,
  createAdapterBinding,
  type RuntimeClock,
} from "@idemantis/adapter-kit"
import { connectHttpAdapter } from "@idemantis/adapter-http"
import {
  type AdapterInstanceId,
  AdapterInstanceIdSchema,
  type Digest,
  type SourceId,
} from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import {
  defineOperation,
  ExecutionBindingError,
  executionBindingDigest,
  type OperationDefinition,
  planMission,
} from "@idemantis/sdk"
import type { TSchema } from "typebox"
import Schema from "typebox/schema"

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

export interface RemoteAdapterConnection {
  url: string
  adapter_instance_id: AdapterInstanceId
  token?: string
}

export async function connectRemoteAdapters(
  connections: readonly RemoteAdapterConnection[],
): Promise<HostedAdapter[]> {
  return Promise.all(
    connections.map(async (connection) => {
      if (!Schema.Check(AdapterInstanceIdSchema, connection.adapter_instance_id)) {
        throw new TypeError(`Adapter instance ${connection.adapter_instance_id} is invalid`)
      }
      const remote = await connectHttpAdapter({
        url: connection.url,
        ...(connection.token ? { token: connection.token } : {}),
      })
      return {
        adapter: remote.manifest.adapter,
        adapter_version: remote.manifest.adapter_version,
        adapter_package_digest: remote.manifest.adapter_package_digest as `sha256:${string}`,
        adapter_instance_id: connection.adapter_instance_id,
        operations: remote.operations,
      }
    }),
  )
}

type DefinitionReference = Pick<MissionRuntimeDefinition, "manifest" | "definition_digest">

export class AdapterHost {
  readonly executor: AdapterExecutor
  readonly #bindings = new Map<string, AdapterBinding>()
  readonly #descriptors = new Map<string, AdapterOperationDescriptor>()

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
        this.#descriptors.set(operation.descriptor.key, structuredClone(operation.descriptor))
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

  descriptors(): AdapterOperationDescriptor[] {
    return [...this.#descriptors.values()]
      .sort((left, right) => left.key.localeCompare(right.key))
      .map((descriptor) => structuredClone(descriptor))
  }

  compilePlan(plan: unknown): MissionRuntimeDefinition {
    const operations: OperationDefinition[] = this.descriptors().map((descriptor) =>
      defineOperation({
        key: descriptor.key,
        adapter: descriptor.adapter,
        operation: descriptor.operation,
        schemaVersion: descriptor.operation_schema_version,
        inputSchema: descriptor.input_schema as unknown as TSchema,
        outputSchema: descriptor.output_schema as unknown as TSchema,
        effectClass: descriptor.effect_class,
        consequence: descriptor.consequence,
        capabilities: descriptor.capabilities,
      }),
    )
    const compiled = planMission(plan, operations)
    return this.bind({ manifest: compiled.manifest, definition_digest: compiled.definitionDigest })
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
