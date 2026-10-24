import { PassThrough } from "node:stream"
import { IdemantisClient } from "@idemantis/client"
import { afterEach, describe, expect, it } from "vitest"
import { type RunnerFixture, runnerFixture } from "../../client/test/fixture.js"
import { main } from "../src/cli.js"
import { McpServer, serveStdio } from "../src/index.js"

const fixtures: RunnerFixture[] = []

async function server() {
  const fixture = await runnerFixture()
  fixtures.push(fixture)
  const client = new IdemantisClient({ url: fixture.runner.origin, api_key: fixture.token })
  for (const runtime of fixture.runtimes) {
    await client.registerDefinition(runtime)
  }
  let id = 0
  const mcp = new McpServer({ client })
  const call = async (name: string, input: Record<string, unknown>) => {
    id += 1
    const response = await mcp.handle({
      jsonrpc: "2.0",
      id,
      method: "tools/call",
      params: { name, arguments: input },
    })
    const result = (response as { result: { structuredContent: never; isError: boolean } }).result
    return { body: result.structuredContent as never, error: result.isError }
  }
  return { fixture, mcp, call }
}

afterEach(async () => {
  for (const fixture of fixtures.splice(0)) {
    await fixture.close()
  }
})

describe("idemantis MCP server", () => {
  it("negotiates the protocol and lists its tools", async () => {
    const { mcp } = await server()

    const initialized = await mcp.handle({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: { protocolVersion: "2025-03-26", capabilities: {} },
    })
    const fallback = await mcp.handle({
      jsonrpc: "2.0",
      id: 2,
      method: "initialize",
      params: { protocolVersion: "1999-01-01" },
    })
    const listed = await mcp.handle({ jsonrpc: "2.0", id: 3, method: "tools/list" })
    const notification = await mcp.handle({ jsonrpc: "2.0", method: "notifications/initialized" })

    expect(initialized).toMatchObject({
      id: 1,
      result: { protocolVersion: "2025-03-26", capabilities: { tools: {} } },
    })
    expect(fallback).toMatchObject({ result: { protocolVersion: "2025-06-18" } })
    expect(
      (listed as { result: { tools: { name: string }[] } }).result.tools.map((tool) => tool.name),
    ).toEqual([
      "list_definitions",
      "list_operations",
      "define_plan",
      "start_mission",
      "get_mission_context",
      "list_missions",
      "wait_for_mission",
      "record_observation",
      "resolve_operator_wait",
    ])
    expect(notification).toBeUndefined()
    expect(await mcp.handle({ jsonrpc: "2.0", id: 4, method: "resources/list" })).toMatchObject({
      error: { code: -32601 },
    })
    expect(await mcp.handle({ id: 5, method: "ping" })).toMatchObject({
      error: { code: -32600 },
    })
    expect(
      await mcp.handle({
        jsonrpc: "2.0",
        id: 6,
        method: "tools/call",
        params: { name: "drop_tables" },
      }),
    ).toMatchObject({ error: { code: -32602 } })
  })

  it("lets an agent write its own plan from connected operations and run it", async () => {
    const { fixture, call } = await server()

    const operations = await call("list_operations", {})
    const defined = await call("define_plan", {
      plan: {
        id: "agent.drafts",
        version: "1.0.0",
        input_schema: {
          type: "object",
          properties: { ticket: { type: "string" } },
          required: ["ticket"],
        },
        steps: [
          {
            name: "draft",
            operation: "writer.draft",
            input: { kind: "object", fields: { ticket: { kind: "input", path: ["ticket"] } } },
          },
        ],
      },
    })
    const started = await call("start_mission", {
      definition_digest: (defined.body as { definition_digest: string }).definition_digest,
      input: { ticket: "T-30" },
    })
    const finished = await call("wait_for_mission", {
      run_id: (started.body as { run_id: string }).run_id,
      timeout_ms: 5_000,
    })
    const broken = await call("define_plan", {
      plan: { id: "agent.broken", version: "1.0.0", input_schema: {}, steps: [] },
    })

    expect(operations.body).toMatchObject({ items: [{ key: "writer.draft" }] })
    expect(defined.error).toBe(false)
    expect(finished.body).toMatchObject({
      terminal_outcome: "completed",
      effects: [{ output: { draft: "Draft for T-30" } }],
    })
    expect(broken.error).toBe(true)
    expect(fixture.calls).toHaveLength(1)
  })

  it("lets an agent run, observe, and resume a mission through tools", async () => {
    const { fixture, call } = await server()

    const definitions = await call("list_definitions", {})
    const started = await call("start_mission", {
      definition_digest: fixture.digests.review,
      input: { ticket: "T-10" },
      idempotency_key: "ticket-T-10",
    })
    const restarted = await call("start_mission", {
      definition_digest: fixture.digests.review,
      input: { ticket: "T-10" },
      idempotency_key: "ticket-T-10",
    })
    const runId = (started.body as { run_id: string }).run_id
    expect((restarted.body as { run_id: string }).run_id).toBe(runId)
    const waiting = await call("wait_for_mission", { run_id: runId, timeout_ms: 5_000 })
    const effectId = (waiting.body as { next: { effect_id: string } }).next.effect_id
    const observation = {
      run_id: runId,
      observation_type: "review.approved",
      payload: { reviewer: "lead" },
      effect_id: effectId,
      idempotency_key: "T-10:review",
    }
    const first = await call("record_observation", observation)
    const retried = await call("record_observation", observation)
    const finished = await call("wait_for_mission", { run_id: runId, timeout_ms: 5_000 })
    const listed = await call("list_missions", { bucket: "all" })
    const invalid = await call("start_mission", {
      definition_digest: fixture.digests.review,
      input: { wrong: 1 },
    })

    expect(definitions.error).toBe(false)
    expect(waiting.body).toMatchObject({
      next: { actor: "observation", observation_type: "review.approved" },
      effects: [{ output: { draft: "Draft for T-10" } }],
    })
    expect(first.body).toMatchObject({ disposition: "recorded" })
    expect(retried.body).toMatchObject({ disposition: "duplicate" })
    expect(finished.body).toMatchObject({ terminal_outcome: "completed", next: { actor: "none" } })
    expect((listed.body as { items: { run_id: string }[] }).items[0]?.run_id).toBe(runId)
    expect(invalid.error).toBe(true)
    expect(invalid.body).toMatchObject({ error: { status: 422 } })
    expect(fixture.calls).toHaveLength(1)
  })

  it("answers an operator decision once even when the agent retries", async () => {
    const { fixture, call } = await server()

    const started = await call("start_mission", {
      definition_digest: fixture.digests.approval,
      input: { ticket: "T-11" },
    })
    const runId = (started.body as { run_id: string }).run_id
    const waiting = await call("wait_for_mission", { run_id: runId, timeout_ms: 5_000 })
    const decision = {
      run_id: runId,
      action_code: "approve",
      input: { note: "Looks right" },
      idempotency_key: "T-11:approve",
    }
    const wrong = await call("resolve_operator_wait", { ...decision, action_code: "reject" })
    const approved = await call("resolve_operator_wait", decision)
    const retried = await call("resolve_operator_wait", decision)
    const finished = await call("get_mission_context", { run_id: runId })

    expect(waiting.body).toMatchObject({ next: { actor: "operator", actions: ["approve"] } })
    expect(wrong.error).toBe(true)
    expect(approved.body).toMatchObject({ outcome: "accepted" })
    expect(finished.body).toMatchObject({ terminal_outcome: "completed" })
    expect(retried.error).toBe(false)
    expect(retried.body).toEqual(approved.body)
  })

  it("serves newline-delimited JSON-RPC over stdio", async () => {
    const { mcp } = await server()
    const input = new PassThrough()
    const output = new PassThrough()
    const lines: string[] = []
    output.on("data", (chunk: Buffer) => lines.push(...chunk.toString().trim().split("\n")))

    const serving = serveStdio(mcp, input, output)
    input.write(`${JSON.stringify({ jsonrpc: "2.0", id: 1, method: "ping" })}\n`)
    input.write("not json\n")
    input.write(`${JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" })}\n`)
    input.end()
    await serving

    expect(lines.map((line) => JSON.parse(line))).toEqual([
      { jsonrpc: "2.0", id: null, error: { code: -32700, message: "Message is not valid JSON" } },
      { jsonrpc: "2.0", id: 1, result: {} },
    ])
    await expect(main({})).rejects.toThrow(TypeError)
  })
})
