import {
  clientRequestIdFor,
  IdemantisApiError,
  type IdemantisClient,
  observationIdFor,
  randomObservationId,
} from "@idemantis/client"
import type { JsonObject, RunId } from "@idemantis/contracts"

export interface ToolDefinition {
  name: string
  title: string
  description: string
  inputSchema: JsonObject
  annotations: JsonObject
  run(client: IdemantisClient, input: Record<string, unknown>): Promise<unknown>
}

const runIdProperty = {
  type: "string",
  pattern: "^run_[0-9A-HJKMNP-TV-Z]{26}$",
  description: "Mission run ID, for example run_01J...",
}

function text(input: Record<string, unknown>, key: string): string {
  const value = input[key]
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${key} must be a non-empty string`)
  }
  return value
}

function optionalText(input: Record<string, unknown>, key: string): string | undefined {
  const value = input[key]
  if (value === undefined) {
    return undefined
  }
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${key} must be a non-empty string`)
  }
  return value
}

function objectValue(input: Record<string, unknown>, key: string): JsonObject {
  const value = input[key] ?? {}
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${key} must be an object`)
  }
  return value as JsonObject
}

function boundedInteger(
  input: Record<string, unknown>,
  key: string,
  fallback: number,
  maximum: number,
): number {
  const value = input[key] ?? fallback
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new TypeError(`${key} must be an integer from 1 through ${maximum}`)
  }
  return value
}

export const tools: ToolDefinition[] = [
  {
    name: "list_definitions",
    title: "List mission definitions",
    description:
      "Lists the mission definitions this runner can execute, with their digests and input schemas. Call it to find the definition_digest for start_mission.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
    run: (client) => client.listDefinitions().then((items) => ({ items })),
  },
  {
    name: "list_operations",
    title: "List connected operations",
    description:
      "Lists the operations (tools) connected to this runner with their input and output schemas, and whether the runner can safely retry or reconcile them. Use these keys as steps in define_plan.",
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
    run: (client) => client.operations().then((items) => ({ items })),
  },
  {
    name: "define_plan",
    title: "Define a durable plan",
    description:
      'Turns a step-by-step plan into a registered durable mission. Each step names a connected operation and builds its input from the mission input ({"kind":"input","path":["field"]}), an earlier step\'s output ({"kind":"output","state":"step_name","path":["field"]}), literals ({"kind":"literal","value":...}) or objects ({"kind":"object","fields":{...}}). Returns the definition_digest for start_mission.',
    inputSchema: {
      type: "object",
      properties: {
        plan: {
          type: "object",
          properties: {
            id: { type: "string", description: "Lowercase dotted id, for example agent.research" },
            version: { type: "string", description: "Semantic version, for example 1.0.0" },
            input_schema: { type: "object", description: "JSON schema for the mission input" },
            steps: {
              type: "array",
              minItems: 1,
              maxItems: 64,
              items: {
                type: "object",
                properties: {
                  name: { type: "string" },
                  operation: { type: "string" },
                  input: { type: "object" },
                },
                required: ["name", "operation", "input"],
              },
            },
          },
          required: ["id", "version", "input_schema", "steps"],
        },
      },
      required: ["plan"],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true },
    run: (client, input) => client.registerPlan(objectValue(input, "plan")),
  },
  {
    name: "start_mission",
    title: "Start a mission",
    description:
      "Admits a durable mission from a registered definition and starts it. The runner executes each step exactly once, journals every result, and resumes after crashes. Returns the mission's resume context.",
    inputSchema: {
      type: "object",
      properties: {
        definition_digest: { type: "string", pattern: "^sha256:[0-9a-f]{64}$" },
        input: { type: "object", description: "Mission input matching the definition's schema" },
        idempotency_key: {
          type: "string",
          minLength: 1,
          maxLength: 512,
          description: "Stable key for this task so a retried call returns the same mission",
        },
      },
      required: ["definition_digest", "input"],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: false, destructiveHint: false, openWorldHint: true },
    run: async (client, input) => {
      const key = optionalText(input, "idempotency_key")
      const admitted = await client.launch(
        text(input, "definition_digest"),
        objectValue(input, "input"),
        key ? { client_request_id: clientRequestIdFor(`admission:${key}`) } : {},
      )
      return client.context(admitted.summary.run_id)
    },
  },
  {
    name: "get_mission_context",
    title: "Get where a mission stands",
    description:
      "Returns what a mission has already done (with each step's output), what is uncertain, and who must act next: the runner, an outside observation, an operator, or nobody. Call it before redoing any work after a restart or context loss.",
    inputSchema: {
      type: "object",
      properties: { run_id: runIdProperty },
      required: ["run_id"],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
    run: (client, input) => client.context(text(input, "run_id") as RunId),
  },
  {
    name: "list_missions",
    title: "List missions",
    description:
      "Lists missions, most urgent first. Filter by bucket (needs_attention, active, waiting, completed, all) or a text query.",
    inputSchema: {
      type: "object",
      properties: {
        bucket: {
          type: "string",
          enum: ["needs_attention", "active", "waiting", "completed", "all"],
        },
        query: { type: "string", maxLength: 128 },
        limit: { type: "integer", minimum: 1, maximum: 50 },
      },
      additionalProperties: false,
    },
    annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
    run: async (client, input) => {
      const bucket = optionalText(input, "bucket")
      const query = optionalText(input, "query")
      const page = await client.listMissions({
        ...(bucket ? { bucket: bucket as "all" } : {}),
        ...(query ? { query } : {}),
        limit: boundedInteger(input, "limit", 25, 50),
      })
      return {
        total_count: page.total_count,
        items: page.items.map((item) => ({
          run_id: item.run_id,
          definition_id: item.definition_id,
          status: item.operational_status,
          current_state: item.current_state,
          queue_bucket: item.queue_bucket,
          ...(item.attention_reason ? { attention_reason: item.attention_reason } : {}),
          updated_at: item.updated_at,
        })),
      }
    },
  },
  {
    name: "wait_for_mission",
    title: "Wait for a mission",
    description:
      "Waits until the runner has nothing left to do on its own: the mission finished, or it needs an observation or an operator decision. Returns the resume context.",
    inputSchema: {
      type: "object",
      properties: {
        run_id: runIdProperty,
        timeout_ms: { type: "integer", minimum: 1, maximum: 600_000 },
      },
      required: ["run_id"],
      additionalProperties: false,
    },
    annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false },
    run: (client, input) =>
      client.waitFor(text(input, "run_id") as RunId, {
        timeout_ms: boundedInteger(input, "timeout_ms", 60_000, 600_000),
      }),
  },
  {
    name: "record_observation",
    title: "Record an observation",
    description:
      "Reports something that happened outside the runner, such as a webhook, a reply, or a sensor reading. Pass a stable idempotency_key so a retried call is recorded only once.",
    inputSchema: {
      type: "object",
      properties: {
        run_id: runIdProperty,
        observation_type: {
          type: "string",
          pattern: "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)+$",
          description: "Dotted type, for example review.approved",
        },
        payload: { type: "object" },
        effect_id: { type: "string" },
        subject: {},
        idempotency_key: { type: "string", minLength: 1, maxLength: 512 },
      },
      required: ["run_id", "observation_type", "payload"],
      additionalProperties: false,
    },
    annotations: {
      readOnlyHint: false,
      destructiveHint: false,
      idempotentHint: true,
      openWorldHint: false,
    },
    run: (client, input) => {
      const key = optionalText(input, "idempotency_key")
      const effectId = optionalText(input, "effect_id")
      const { subject } = input
      return client.observe(text(input, "run_id") as RunId, {
        observation_id: key ? observationIdFor(key) : randomObservationId(),
        observation_type: text(input, "observation_type"),
        payload: objectValue(input, "payload"),
        ...(effectId ? { effect_id: effectId } : {}),
        ...(subject === undefined ? {} : { subject: subject as never }),
      })
    },
  },
  {
    name: "resolve_operator_wait",
    title: "Answer an operator decision",
    description:
      "Submits one of the actions a waiting mission offers (see next.actions in get_mission_context). The decision is journaled with your API key as the actor. Pass a stable idempotency_key so a retry does not submit twice.",
    inputSchema: {
      type: "object",
      properties: {
        run_id: runIdProperty,
        action_code: { type: "string", minLength: 1, maxLength: 128 },
        input: { type: "object" },
        reason: { type: "string", minLength: 1, maxLength: 4096 },
        idempotency_key: { type: "string", minLength: 1, maxLength: 512 },
      },
      required: ["run_id", "action_code", "idempotency_key"],
      additionalProperties: false,
    },
    annotations: {
      readOnlyHint: false,
      destructiveHint: true,
      idempotentHint: true,
      openWorldHint: false,
    },
    run: async (client, input) => {
      const runId = text(input, "run_id") as RunId
      const actionCode = text(input, "action_code")
      const clientRequestId = clientRequestIdFor(`${runId}:${text(input, "idempotency_key")}`)
      try {
        return await client.interventionReceipt(runId, clientRequestId)
      } catch (error) {
        if (!(error instanceof IdemantisApiError) || error.status !== 404) {
          throw error
        }
      }
      const detail = await client.mission(runId)
      const action = detail.valid_interventions.find(
        (candidate) => candidate.action_code === actionCode,
      )
      if (!action) {
        throw new TypeError(
          `Mission ${runId} does not offer ${actionCode}; offered: ${
            detail.valid_interventions.map((candidate) => candidate.action_code).join(", ") ||
            "none"
          }`,
        )
      }
      const reason = optionalText(input, "reason")
      const prepared = await client.prepareIntervention(runId, {
        client_request_id: clientRequestId,
        expected_sequence: detail.summary.current_sequence,
        action_code: actionCode,
        target: action.target,
        input: objectValue(input, "input"),
        ...(reason ? { reason } : {}),
      })
      return client.submitIntervention(runId, {
        client_request_id: prepared.client_request_id,
        proposal_digest: prepared.proposal_digest,
      })
    },
  },
]
