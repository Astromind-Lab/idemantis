import type { JsonObject, RunId } from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import type {
  EffectDetail,
  EffectPage,
  InterventionReceipt,
  MissionContext,
  MissionDetail,
  MissionExecutionStart,
  MissionExecutionStatus,
  MissionListPage,
  ObservationReceipt,
  PreparedIntervention,
  PrepareInterventionRequest,
  RecordObservationRequest,
  SubmitInterventionRequest,
  TimelinePage,
} from "@idemantis/transport"
import { IdemantisApiError } from "./errors.js"

type Fetch = (input: string, init: RequestInit) => Promise<Response>

export interface IdemantisClientOptions {
  url: string
  api_key: string
  fetch?: Fetch
  timeout_ms?: number
}

export interface RegisteredDefinitionSummary {
  definition_id: string
  definition_version: string
  definition_digest: string
  input_schema: JsonObject
}

export interface ConnectedOperation {
  key: string
  adapter: string
  operation: string
  effect_class: string
  consequence: string
  input_schema: JsonObject
  output_schema: JsonObject
  duplicate_safety: string
  reconciliation: string
}

export interface RegisteredPlan {
  definition_digest: string
  definition_id: string
  definition_version: string
  states: string[]
}

export interface MissionListOptions {
  bucket?: "needs_attention" | "active" | "waiting" | "completed" | "all"
  query?: string
  cursor?: string
  limit?: number
}

export interface WaitOptions {
  until?: (context: MissionContext) => boolean
  timeout_ms?: number
  interval_ms?: number
  signal?: AbortSignal
}

function settledForCaller(context: MissionContext): boolean {
  return context.terminal_outcome !== null || context.next.actor !== "runner"
}

function pause(duration: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolvePromise, reject) => {
    if (signal?.aborted) {
      reject(signal.reason)
      return
    }
    const timeout = setTimeout(() => {
      signal?.removeEventListener("abort", abort)
      resolvePromise()
    }, duration)
    const abort = () => {
      clearTimeout(timeout)
      reject(signal?.reason)
    }
    signal?.addEventListener("abort", abort, { once: true })
  })
}

export class IdemantisClient {
  readonly #base: URL
  readonly #apiKey: string
  readonly #fetch: Fetch
  readonly #timeoutMs: number

  constructor(options: IdemantisClientOptions) {
    let base: URL
    try {
      base = new URL(options.url)
    } catch {
      throw new TypeError("Runner URL is invalid")
    }
    if (base.protocol !== "http:" && base.protocol !== "https:") {
      throw new TypeError("Runner URL must use HTTP or HTTPS")
    }
    if (!/^idm_[0-9A-Z]{26}_[A-Za-z0-9_-]{43}$/.test(options.api_key)) {
      throw new TypeError("API key is malformed")
    }
    this.#base = new URL(base.origin)
    this.#apiKey = options.api_key
    this.#fetch = options.fetch ?? ((input, init) => fetch(input, init))
    this.#timeoutMs = options.timeout_ms ?? 30_000
  }

  async health(): Promise<{ status: string }> {
    return this.request("GET", "/api/health")
  }

  async listDefinitions(): Promise<RegisteredDefinitionSummary[]> {
    return (await this.request<{ items: RegisteredDefinitionSummary[] }>("GET", "/api/definitions"))
      .items
  }

  async registerDefinition(
    runtime: MissionRuntimeDefinition,
    options: { subject_reference_keys?: string[] } = {},
  ): Promise<string> {
    return (
      await this.request<{ registered: string }>("POST", "/api/definitions", {
        runtime,
        ...options,
      })
    ).registered
  }

  async operations(): Promise<ConnectedOperation[]> {
    return (await this.request<{ items: ConnectedOperation[] }>("GET", "/api/operations")).items
  }

  async registerPlan(
    plan: JsonObject,
    options: { subject_reference_keys?: string[] } = {},
  ): Promise<RegisteredPlan> {
    return this.request("POST", "/api/plans", { plan, ...options })
  }

  async admit(
    definitionDigest: string,
    input: JsonObject,
    options: { client_request_id?: string } = {},
  ): Promise<MissionDetail> {
    return this.request("POST", "/api/missions", {
      definition_digest: definitionDigest,
      input,
      ...options,
    })
  }

  async start(runId: RunId): Promise<MissionExecutionStart> {
    return this.request("POST", `/api/missions/${this.id(runId)}/start`, {})
  }

  async launch(
    definitionDigest: string,
    input: JsonObject,
    options: { client_request_id?: string } = {},
  ): Promise<MissionDetail> {
    const admitted = await this.admit(definitionDigest, input, options)
    await this.start(admitted.summary.run_id)
    return admitted
  }

  async mission(runId: RunId): Promise<MissionDetail> {
    return this.request("GET", `/api/missions/${this.id(runId)}`)
  }

  async context(runId: RunId): Promise<MissionContext> {
    return this.request("GET", `/api/missions/${this.id(runId)}/context`)
  }

  async execution(runId: RunId): Promise<MissionExecutionStatus> {
    return this.request("GET", `/api/missions/${this.id(runId)}/execution`)
  }

  async listMissions(options: MissionListOptions = {}): Promise<MissionListPage> {
    const query = new URLSearchParams()
    for (const [key, value] of Object.entries(options)) {
      if (value !== undefined) {
        query.set(key, String(value))
      }
    }
    const suffix = query.size > 0 ? `?${query.toString()}` : ""
    return this.request("GET", `/api/missions${suffix}`)
  }

  async timeline(runId: RunId, cursor?: string): Promise<TimelinePage> {
    const suffix = cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""
    return this.request("GET", `/api/missions/${this.id(runId)}/timeline${suffix}`)
  }

  async effects(runId: RunId, cursor?: string): Promise<EffectPage> {
    const suffix = cursor ? `?cursor=${encodeURIComponent(cursor)}` : ""
    return this.request("GET", `/api/missions/${this.id(runId)}/effects${suffix}`)
  }

  async effect(runId: RunId, effectId: string): Promise<EffectDetail> {
    return this.request(
      "GET",
      `/api/missions/${this.id(runId)}/effects/${encodeURIComponent(effectId)}`,
    )
  }

  async observe(runId: RunId, observation: RecordObservationRequest): Promise<ObservationReceipt> {
    return this.request("POST", `/api/missions/${this.id(runId)}/observations`, observation)
  }

  async prepareIntervention(
    runId: RunId,
    request: PrepareInterventionRequest,
  ): Promise<PreparedIntervention> {
    return this.request("POST", `/api/missions/${this.id(runId)}/interventions/prepare`, request)
  }

  async submitIntervention(
    runId: RunId,
    request: SubmitInterventionRequest,
  ): Promise<InterventionReceipt> {
    return this.request("POST", `/api/missions/${this.id(runId)}/interventions/submit`, request)
  }

  async interventionReceipt(runId: RunId, clientRequestId: string): Promise<InterventionReceipt> {
    return this.request(
      "GET",
      `/api/missions/${this.id(runId)}/interventions/${encodeURIComponent(clientRequestId)}/receipt`,
    )
  }

  async waitFor(runId: RunId, options: WaitOptions = {}): Promise<MissionContext> {
    const until = options.until ?? settledForCaller
    const interval = options.interval_ms ?? 250
    const deadline = Date.now() + (options.timeout_ms ?? 60_000)
    for (;;) {
      const context = await this.context(runId)
      if (until(context) || context.terminal_outcome !== null) {
        return context
      }
      if (Date.now() + interval > deadline) {
        throw new IdemantisApiError(
          408,
          "WAIT_TIMEOUT",
          `Mission ${runId} did not reach the requested condition in time`,
        )
      }
      await pause(interval, options.signal)
    }
  }

  private id(runId: RunId): string {
    return encodeURIComponent(runId)
  }

  private async request<Value>(method: "GET" | "POST", path: string, body?: unknown) {
    let response: Response
    try {
      response = await this.#fetch(new URL(path, this.#base).toString(), {
        method,
        headers: {
          accept: "application/json",
          authorization: `Bearer ${this.#apiKey}`,
          ...(body === undefined ? {} : { "content-type": "application/json" }),
        },
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        signal: AbortSignal.timeout(this.#timeoutMs),
        redirect: "error",
      })
    } catch (error) {
      throw new IdemantisApiError(
        0,
        "RUNNER_UNREACHABLE",
        error instanceof Error ? error.message : "Runner is unreachable",
      )
    }
    const text = await response.text()
    let document: unknown
    try {
      document = text.length > 0 ? JSON.parse(text) : {}
    } catch {
      throw new IdemantisApiError(response.status, "RESPONSE_INVALID", "Runner did not return JSON")
    }
    if (!response.ok) {
      const error = (
        document as { error?: { code?: string; message?: string; request_id?: string } }
      ).error
      throw new IdemantisApiError(
        response.status,
        error?.code ?? "REQUEST_FAILED",
        error?.message ?? `Runner returned HTTP ${response.status}`,
        error?.request_id,
      )
    }
    return document as Value
  }
}
