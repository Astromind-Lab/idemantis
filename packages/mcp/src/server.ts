import { createInterface } from "node:readline"
import type { Readable, Writable } from "node:stream"
import { IdemantisApiError, type IdemantisClient } from "@idemantis/client"
import { type ToolDefinition, tools as defaultTools } from "./tools.js"

export const supportedProtocolVersions = ["2025-06-18", "2025-03-26", "2024-11-05"] as const

type RequestId = string | number

interface JsonRpcRequest {
  jsonrpc: "2.0"
  id?: RequestId
  method: string
  params?: Record<string, unknown>
}

export type JsonRpcResponse =
  | { jsonrpc: "2.0"; id: RequestId | null; result: unknown }
  | {
      jsonrpc: "2.0"
      id: RequestId | null
      error: { code: number; message: string; data?: unknown }
    }

export interface McpServerOptions {
  client: IdemantisClient
  tools?: readonly ToolDefinition[]
  name?: string
  version?: string
}

function failure(id: RequestId | null, code: number, message: string): JsonRpcResponse {
  return { jsonrpc: "2.0", id, error: { code, message } }
}

function request(value: unknown): JsonRpcRequest | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return undefined
  }
  const { jsonrpc, method, id, params } = value as Record<string, unknown>
  if (
    jsonrpc !== "2.0" ||
    typeof method !== "string" ||
    (id !== undefined && typeof id !== "string" && typeof id !== "number") ||
    (params !== undefined && (!params || typeof params !== "object" || Array.isArray(params)))
  ) {
    return undefined
  }
  return value as JsonRpcRequest
}

export class McpServer {
  readonly #client: IdemantisClient
  readonly #tools: Map<string, ToolDefinition>
  readonly #name: string
  readonly #version: string

  constructor(options: McpServerOptions) {
    this.#client = options.client
    this.#tools = new Map((options.tools ?? defaultTools).map((tool) => [tool.name, tool]))
    this.#name = options.name ?? "idemantis"
    this.#version = options.version ?? "0.0.0"
  }

  async handle(message: unknown): Promise<JsonRpcResponse | undefined> {
    const parsed = request(message)
    if (!parsed) {
      return failure(null, -32600, "Invalid JSON-RPC request")
    }
    if (parsed.id === undefined) {
      return undefined
    }
    const id = parsed.id
    try {
      return { jsonrpc: "2.0", id, result: await this.dispatch(parsed) }
    } catch (error) {
      if (error instanceof MethodError) {
        return failure(id, error.code, error.message)
      }
      return failure(id, -32603, error instanceof Error ? error.message : "Internal error")
    }
  }

  private async dispatch(message: JsonRpcRequest): Promise<unknown> {
    switch (message.method) {
      case "initialize": {
        const { protocolVersion: requested } = message.params ?? {}
        const protocolVersion = supportedProtocolVersions.includes(requested as never)
          ? (requested as string)
          : supportedProtocolVersions[0]
        return {
          protocolVersion,
          capabilities: { tools: { listChanged: false } },
          serverInfo: { name: this.#name, version: this.#version },
          instructions:
            "Idemantis runs durable missions. Each step runs exactly once and its result is journaled. After any restart or context loss, call get_mission_context before redoing work.",
        }
      }
      case "ping":
        return {}
      case "tools/list":
        return {
          tools: [...this.#tools.values()].map((tool) => ({
            name: tool.name,
            title: tool.title,
            description: tool.description,
            inputSchema: tool.inputSchema,
            annotations: tool.annotations,
          })),
        }
      case "tools/call":
        return this.call(message.params ?? {})
      default:
        throw new MethodError(-32601, `Method ${message.method} is not supported`)
    }
  }

  private async call(params: Record<string, unknown>): Promise<unknown> {
    const { name, arguments: input } = params
    const tool = typeof name === "string" ? this.#tools.get(name) : undefined
    if (!tool) {
      throw new MethodError(-32602, `Unknown tool ${String(name)}`)
    }
    if (input !== undefined && (!input || typeof input !== "object" || Array.isArray(input))) {
      throw new MethodError(-32602, "Tool arguments must be an object")
    }
    try {
      const result = await tool.run(this.#client, (input ?? {}) as Record<string, unknown>)
      return {
        content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        structuredContent: result,
        isError: false,
      }
    } catch (error) {
      const detail =
        error instanceof IdemantisApiError
          ? { status: error.status, code: error.code, message: error.message }
          : { code: "TOOL_FAILED", message: error instanceof Error ? error.message : "Failed" }
      return {
        content: [{ type: "text", text: JSON.stringify({ error: detail }, null, 2) }],
        structuredContent: { error: detail },
        isError: true,
      }
    }
  }
}

class MethodError extends Error {
  readonly code: number

  constructor(code: number, message: string) {
    super(message)
    this.code = code
  }
}

export async function serveStdio(
  server: McpServer,
  input: Readable = process.stdin,
  output: Writable = process.stdout,
): Promise<void> {
  const lines = createInterface({ input, crlfDelay: Number.POSITIVE_INFINITY })
  const pending = new Set<Promise<void>>()
  const write = (response: JsonRpcResponse) =>
    new Promise<void>((resolvePromise, reject) => {
      output.write(`${JSON.stringify(response)}\n`, (error) =>
        error ? reject(error) : resolvePromise(),
      )
    })
  for await (const line of lines) {
    if (line.trim().length === 0) {
      continue
    }
    let message: unknown
    try {
      message = JSON.parse(line)
    } catch {
      await write(failure(null, -32700, "Message is not valid JSON"))
      continue
    }
    const task = server
      .handle(message)
      .then((response) => (response ? write(response) : undefined))
      .finally(() => pending.delete(task))
    pending.add(task)
  }
  await Promise.all(pending)
}
