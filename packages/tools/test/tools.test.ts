import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import type { JsonObject, RunId } from "@idemantis/contracts"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import {
  AdapterHost,
  ApiKeyStore,
  HostedMissionRunner,
  RunnerStore,
  startLocalRunner,
} from "@idemantis/runner"
import { fromInput, fromOutput, mapFields } from "@idemantis/sdk"
import Type from "typebox"
import { afterEach, describe, expect, it } from "vitest"
import {
  createToolAdapter,
  defineTool,
  serveTools,
  ToolLedger,
  ToolRejected,
  ToolUnavailable,
  toolMission,
} from "../src/index.js"

const cleanups: (() => Promise<void> | void)[] = []

afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) {
    await cleanup()
  }
})

interface Crm {
  tickets: Map<string, string>
  notices: string[]
}

function tools(crm: Crm, behavior: { create?: (key: string) => void; lookup?: boolean } = {}) {
  const createTicket = defineTool({
    name: "crm.create_ticket",
    input: Type.Object({ subject: Type.String() }, { additionalProperties: false }),
    output: Type.Object({ ticket_id: Type.String() }, { additionalProperties: false }),
    run: async (_input, context) => {
      const ticketId = `T-${crm.tickets.size + 1}`
      crm.tickets.set(context.idempotency_key, ticketId)
      behavior.create?.(context.idempotency_key)
      return { ticket_id: ticketId }
    },
    ...(behavior.lookup
      ? {
          lookup: async (_input, context) => {
            const ticketId = crm.tickets.get(context.idempotency_key)
            return ticketId ? { found: true, output: { ticket_id: ticketId } } : { found: false }
          },
        }
      : {}),
  })
  const notify = defineTool({
    name: "chat.notify",
    input: Type.Object({ text: Type.String() }, { additionalProperties: false }),
    output: Type.Object({ sent: Type.Boolean() }, { additionalProperties: false }),
    run: async (input) => {
      crm.notices.push(input.text)
      return { sent: true }
    },
  })
  return [createTicket, notify]
}

function mission(toolset: ReturnType<typeof tools>) {
  const [createTicket, notify] = toolset
  if (!createTicket || !notify) {
    throw new Error("Expected tools")
  }
  return toolMission({
    id: "support.open_ticket",
    version: "1.0.0",
    adapter: "agent-tools",
    input: Type.Object({ subject: Type.String() }, { additionalProperties: false }),
    backoff_ms: 5,
    steps: [
      {
        name: "create",
        tool: createTicket,
        input: mapFields({ subject: fromInput("subject") }),
      },
      {
        name: "notify",
        tool: notify,
        input: mapFields({ text: fromOutput("create", "ticket_id") }),
      },
    ],
  })
}

function ledger(path = ":memory:") {
  const opened = new ToolLedger({ path })
  cleanups.push(() => opened.close())
  return opened
}

async function run(toolset: ReturnType<typeof tools>, toolLedger: ToolLedger) {
  const adapter = createToolAdapter({
    adapter: "agent-tools",
    version: "1.0.0",
    tools: toolset,
    ledger: toolLedger,
    instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  })
  const journal = new SqliteJournal({ path: ":memory:" })
  cleanups.push(() => journal.close())
  const host = new AdapterHost({ source_id: "hosted.runner", adapters: [adapter] })
  const runner = new HostedMissionRunner({ journal, host })
  cleanups.push(() => runner.close())
  const store = new RunnerStore({ journal })
  const compiled = mission(toolset)
  const runtime = host.bind({
    manifest: compiled.manifest,
    definition_digest: compiled.definitionDigest,
  })
  store.registerDefinition(runtime)
  const runId = store.admitMission({
    definition_digest: runtime.definition_digest,
    input: { subject: "Charger offline" } as JsonObject,
  }).summary.run_id as RunId
  for (let pass = 0; pass < 100 && !journal.getMission(runId).state.terminal_outcome; pass += 1) {
    await runner.run(runId)
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 10))
  }
  await runner.close()
  return { journal, runId, store }
}

describe("durable agent tools", () => {
  it("runs each tool once and passes its output to the next step", async () => {
    const crm: Crm = { tickets: new Map(), notices: [] }
    const { journal, runId } = await run(tools(crm), ledger())

    expect(journal.getMission(runId).state.terminal_outcome).toBe("completed")
    expect(crm.tickets.size).toBe(1)
    expect(crm.notices).toEqual(["T-1"])
    expect(journal.verifyRun(runId).state_digest).toBe(journal.getMission(runId).state_digest)
  })

  it("recovers a lost response through lookup instead of creating a second ticket", async () => {
    const crm: Crm = { tickets: new Map(), notices: [] }
    const toolset = tools(crm, {
      lookup: true,
      create: () => {
        throw new Error("connection reset after the ticket was created")
      },
    })
    const { journal, runId, store } = await run(toolset, ledger())

    expect(journal.getMission(runId).state.terminal_outcome).toBe("completed")
    expect(crm.tickets.size).toBe(1)
    expect(crm.notices).toEqual(["T-1"])
    expect(store.missionContext(runId).effects.map((effect) => effect.status)).toEqual([
      "settled",
      "settled",
    ])
  })

  it("stops as unresolved when an interrupted call cannot be proven either way", async () => {
    const crm: Crm = { tickets: new Map(), notices: [] }
    const toolset = tools(crm, {
      create: () => {
        throw new Error("timeout")
      },
    })
    const { journal, runId, store } = await run(toolset, ledger())

    expect(journal.getMission(runId).state.terminal_outcome).toBe("unresolved")
    expect(crm.tickets.size).toBe(1)
    expect(crm.notices).toEqual([])
    expect(store.missionContext(runId).attention_reason).toBeDefined()
  })

  it("retries a transient failure and fails fast on a rejection", async () => {
    const transient: Crm = { tickets: new Map(), notices: [] }
    let failures = 0
    const retried = await run(
      tools(transient, {
        create: (key) => {
          if (failures < 1) {
            failures += 1
            transient.tickets.delete(key)
            throw new ToolUnavailable("rate limited")
          }
        },
      }),
      ledger(),
    )
    const rejected: Crm = { tickets: new Map(), notices: [] }
    const refused = await run(
      tools(rejected, {
        create: (key) => {
          rejected.tickets.delete(key)
          throw new ToolRejected("subject is not allowed")
        },
      }),
      ledger(),
    )

    expect(retried.journal.getMission(retried.runId).state.terminal_outcome).toBe("completed")
    expect(transient.tickets.size).toBe(1)
    expect(refused.journal.getMission(refused.runId).state.terminal_outcome).toBe("failed")
    expect(rejected.notices).toEqual([])
  })

  it("answers a repeated call from the ledger without running the tool again", async () => {
    const crm: Crm = { tickets: new Map(), notices: [] }
    const toolLedger = ledger()
    const [createTicket] = tools(crm)
    if (!createTicket) {
      throw new Error("Expected a tool")
    }
    const adapter = createToolAdapter({
      adapter: "agent-tools",
      version: "1.0.0",
      tools: [createTicket],
      ledger: toolLedger,
    })
    const operation = adapter.operations[0]
    if (!operation) {
      throw new Error("Expected an operation")
    }
    const context = {
      adapter_instance_id: adapter.adapter_instance_id,
      run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      command_id: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      attempt_id: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      ownership_epoch: 1,
      deadline: new Date(Date.now() + 60_000).toISOString(),
      source_id: "test",
      signal: new AbortController().signal,
    } as const

    const first = await operation.issue(context as never, { subject: "a" })
    const second = await operation.issue(context as never, { subject: "a" })
    const confirmed = await operation.reconcile?.(context as never, "create")
    const unseen = await operation.reconcile?.(
      { ...context, effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAW" } as never,
      "create",
    )

    expect(first).toMatchObject({ result: "completed", output: { ticket_id: "T-1" } })
    expect(second).toMatchObject({
      result: "completed",
      output: { ticket_id: "T-1" },
      evidence: { deduplicated: true },
    })
    expect(confirmed).toMatchObject({ result: "confirmed_completed" })
    expect(unseen).toMatchObject({ result: "confirmed_not_applied" })
    expect(crm.tickets.size).toBe(1)
    expect(toolLedger.list()).toHaveLength(1)
  })

  it("waits for an in-flight call before answering a reconciliation", async () => {
    let release: () => void = () => {}
    const gate = new Promise<void>((resolvePromise) => {
      release = resolvePromise
    })
    const slow = defineTool({
      name: "crm.slow",
      input: Type.Object({ subject: Type.String() }, { additionalProperties: false }),
      output: Type.Object({ ticket_id: Type.String() }, { additionalProperties: false }),
      run: async () => {
        await gate
        return { ticket_id: "T-9" }
      },
    })
    const operation = createToolAdapter({
      adapter: "agent-tools",
      version: "1.0.0",
      tools: [slow],
      ledger: ledger(),
    }).operations[0]
    if (!operation?.reconcile) {
      throw new Error("Expected a reconcilable operation")
    }
    const context = (effect: string, signal = new AbortController().signal) =>
      ({
        adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        command_id: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        attempt_id: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        effect_id: effect,
        ownership_epoch: 1,
        deadline: new Date(Date.now() + 60_000).toISOString(),
        source_id: "test",
        signal,
      }) as never

    const issued = operation.issue(context("eff_01ARZ3NDEKTSV4RRFFQ69G5FAV"), { subject: "a" })
    const impatient = new AbortController()
    const giveUp = operation.reconcile(
      context("eff_01ARZ3NDEKTSV4RRFFQ69G5FAV", impatient.signal),
      "create",
    )
    impatient.abort()
    const waited = operation.reconcile(context("eff_01ARZ3NDEKTSV4RRFFQ69G5FAV"), "create")
    release()

    expect(await giveUp).toMatchObject({ result: "confirmed_in_progress" })
    expect(await waited).toMatchObject({
      result: "confirmed_completed",
      output: { ticket_id: "T-9" },
    })
    expect(await issued).toMatchObject({ result: "completed" })
  })

  it("serves tools over HTTP to a separately running runner", async () => {
    const directory = mkdtempSync(join(tmpdir(), "idemantis-tools-"))
    cleanups.push(() => rmSync(directory, { recursive: true, force: true }))
    const crm: Crm = { tickets: new Map(), notices: [] }
    const toolset = tools(crm)
    const served = await serveTools(
      createToolAdapter({
        adapter: "agent-tools",
        version: "1.0.0",
        tools: toolset,
        ledger: ledger(join(directory, "tools.sqlite")),
      }),
      { token: "tool-secret" },
    )
    cleanups.push(() => served.close())
    const local = await startLocalRunner({
      port: 0,
      state_directory: join(directory, "runner"),
      reference_transition_delay_ms: 0,
      remote_adapters: [
        {
          adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
          url: served.url,
          token: "tool-secret",
        },
      ],
    })
    cleanups.push(() => local.close())
    const keys = new ApiKeyStore({ path: local.access_path })
    const token = keys.create("tool-agent", ["view", "operate", "define"]).token
    keys.close()
    const compiled = mission(toolset)
    const host = new AdapterHost({
      source_id: "hosted.runner",
      adapters: [
        createToolAdapter({
          adapter: "agent-tools",
          version: "1.0.0",
          tools: toolset,
          ledger: ledger(),
          instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        }),
      ],
    })
    const runtime = host.bind({
      manifest: compiled.manifest,
      definition_digest: compiled.definitionDigest,
    })
    const call = async (path: string, body: unknown) =>
      (await fetch(`${local.origin}${path}`, {
        method: "POST",
        headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
        body: JSON.stringify(body),
      }).then((response) => response.json())) as { summary: { run_id: string } }

    await call("/api/definitions", { runtime })
    const admitted = await call("/api/missions", {
      definition_digest: runtime.definition_digest,
      input: { subject: "Remote" },
    })
    await call(`/api/missions/${admitted.summary.run_id}/start`, {})
    const deadline = Date.now() + 5_000
    while (crm.notices.length === 0 && Date.now() < deadline) {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 20))
    }

    expect(crm.notices).toEqual(["T-1"])
    expect(crm.tickets.size).toBe(1)
  })

  it("rejects step names that collide with generated states", () => {
    const crm: Crm = { tickets: new Map(), notices: [] }
    const [createTicket] = tools(crm)
    if (!createTicket) {
      throw new Error("Expected a tool")
    }
    const build = (name: string) =>
      toolMission({
        id: "bad.names",
        version: "1.0.0",
        adapter: "agent-tools",
        input: Type.Object({ subject: Type.String() }),
        steps: [{ name, tool: createTicket, input: mapFields({ subject: fromInput("subject") }) }],
      })

    expect(() => build("done")).toThrow(TypeError)
    expect(() => build("x_reconcile")).toThrow(TypeError)
    expect(() =>
      toolMission({
        id: "empty",
        version: "1.0.0",
        adapter: "agent-tools",
        input: Type.Object({}),
        steps: [],
      }),
    ).toThrow(TypeError)
  })
})
