import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http"
import type { AddressInfo } from "node:net"
import {
  type AdapterInvocationContext,
  type AdapterOperation,
  AdapterKitError,
  defineAdapterOperation,
} from "@idemantis/adapter-kit"
import type { OperationCapabilities } from "@idemantis/contracts"
import Type from "typebox"
import { afterEach, describe, expect, it, vi } from "vitest"
import { connectHttpAdapter, createHttpAdapterHandler, HttpAdapterError } from "../src/index.js"

const capabilities = {
  duplicate_safety: "native_idempotency",
  reconciliation: "authoritative",
  fencing_mode: "none",
  fencing_scope: "operation",
  handoff_mode: "settle_first",
  execution_location: "either",
  cancellation: false,
  compensation: false,
  observation_stream: false,
} satisfies OperationCapabilities

const inputSchema = Type.Object(
  { ticket: Type.String({ minLength: 1 }), summary: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)
const outputSchema = Type.Object(
  { ticket_url: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)
const subjectSchema = Type.Object(
  { ticket: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
)

function ticketOperation(
  issue: (context: AdapterInvocationContext, input: unknown) => Promise<unknown>,
) {
  return defineAdapterOperation({
    key: "tracker.create_ticket",
    adapter: "tracker",
    operation: "tracker.create_ticket",
    schemaVersion: 1,
    inputSchema,
    outputSchema,
    subjectSchema,
    effectClass: "record_mutation",
    consequence: "reversible",
    capabilities,
    timeout: { minimumMs: 10, defaultMs: 1_000, maximumMs: 5_000 },
    reconciliationLookupFields: ["ticket"],
    issue: issue as never,
    reconcile: async (_context, subject) => ({
      result: "confirmed_completed",
      output: { ticket_url: `https://tracker.test/${subject.ticket}` },
    }),
  }) as unknown as AdapterOperation<unknown, unknown, unknown>
}

const servers: Server[] = []

async function listen(handler: (request: IncomingMessage, response: ServerResponse) => void) {
  const server = createServer(handler)
  servers.push(server)
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve))
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`
}

function context(): AdapterInvocationContext {
  return {
    adapter_instance_id: "adp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    command_id: "cmd_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    attempt_id: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    effect_id: "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    ownership_epoch: 1,
    deadline: new Date(Date.now() + 5_000).toISOString(),
    source_id: "hosted.runner",
    signal: AbortSignal.timeout(5_000),
  }
}

afterEach(async () => {
  for (const server of servers.splice(0)) {
    server.closeAllConnections()
    await new Promise((resolve) => server.close(resolve))
  }
})

describe("http adapter protocol", () => {
  it("connects remote operations whose descriptors match the served adapter", async () => {
    const operation = ticketOperation(vi.fn())
    const url = await listen(
      createHttpAdapterHandler({
        adapter: "tracker",
        adapter_version: "2.1.0",
        adapter_package_digest: `sha256:${"d".repeat(64)}`,
        operations: [operation],
        token: "adapter-secret",
      }),
    )

    const remote = await connectHttpAdapter({ url, token: "adapter-secret" })

    expect(remote.manifest).toMatchObject({ adapter: "tracker", adapter_version: "2.1.0" })
    expect(remote.operations.map((selected) => selected.descriptor)).toEqual([operation.descriptor])
  })

  it("issues and reconciles through HTTP with the effect as idempotency key", async () => {
    const keys = new Map<string, string | undefined>()
    const issue = vi.fn(async (_context: AdapterInvocationContext, input: unknown) => ({
      result: "completed",
      output: { ticket_url: `https://tracker.test/${(input as { ticket: string }).ticket}` },
    }))
    const handler = createHttpAdapterHandler({
      adapter: "tracker",
      adapter_version: "2.1.0",
      adapter_package_digest: `sha256:${"d".repeat(64)}`,
      operations: [ticketOperation(issue)],
    })
    const url = await listen((request, response) => {
      keys.set(request.url ?? "", request.headers["idempotency-key"] as string | undefined)
      handler(request, response)
    })
    const [operation] = (await connectHttpAdapter({ url })).operations

    const issued = await operation?.issue(context(), { ticket: "OPS-7", summary: "Dock jammed" })
    const reconciled = await operation?.reconcile?.(context(), { ticket: "OPS-7" })

    expect(issued).toEqual({
      result: "completed",
      output: { ticket_url: "https://tracker.test/OPS-7" },
    })
    expect(reconciled).toMatchObject({ result: "confirmed_completed" })
    expect(Object.fromEntries(keys)).toEqual({
      "/manifest": undefined,
      "/operations/tracker.create_ticket/issue": "eff_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      "/operations/tracker.create_ticket/reconcile": undefined,
    })
    const [served] = issue.mock.calls
    expect(served?.[0]).toMatchObject({
      run_id: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      ownership_epoch: 1,
    })
  })

  it("reports a refused connection as not sent and leaves reconciliation to retry", async () => {
    const handler = createHttpAdapterHandler({
      adapter: "tracker",
      adapter_version: "2.1.0",
      adapter_package_digest: `sha256:${"d".repeat(64)}`,
      operations: [
        ticketOperation(async () => ({ result: "completed", output: { ticket_url: "x" } })),
      ],
    })
    const url = await listen(handler)
    const [operation] = (await connectHttpAdapter({ url })).operations
    const server = servers.pop()
    await new Promise((resolvePromise) => server?.close(resolvePromise))

    expect(
      await operation?.issue(context(), { ticket: "OPS-9", summary: "Adapter restarting" }),
    ).toMatchObject({ result: "not_sent" })
    await expect(operation?.reconcile?.(context(), { ticket: "OPS-9" })).rejects.toThrow()
  })

  it("fails the invocation when the adapter errors or answers outside its contract", async () => {
    const failing = createHttpAdapterHandler({
      adapter: "tracker",
      adapter_version: "2.1.0",
      adapter_package_digest: `sha256:${"d".repeat(64)}`,
      operations: [
        ticketOperation(async () => {
          throw new Error("tracker database offline")
        }),
      ],
    })
    let manifest = true
    const url = await listen((request, response) => {
      if (manifest || request.url?.endsWith("/reconcile")) {
        manifest = false
        failing(request, response)
        return
      }
      response.writeHead(200, { "content-type": "application/json" })
      response.end(JSON.stringify({ result: "done" }))
    })
    const [operation] = (await connectHttpAdapter({ url })).operations
    const failingUrl = await listen(failing)
    const [failingOperation] = (await connectHttpAdapter({ url: failingUrl })).operations

    await expect(
      failingOperation?.issue(context(), { ticket: "OPS-8", summary: "Gate stuck" }),
    ).rejects.toMatchObject({ code: "RESPONSE_REJECTED" })
    await expect(
      operation?.issue(context(), { ticket: "OPS-8", summary: "Gate stuck" }),
    ).rejects.toBeInstanceOf(AdapterKitError)
  })

  it("refuses insecure remote URLs and unauthenticated manifests", async () => {
    const url = await listen(
      createHttpAdapterHandler({
        adapter: "tracker",
        adapter_version: "2.1.0",
        adapter_package_digest: `sha256:${"d".repeat(64)}`,
        operations: [ticketOperation(vi.fn())],
        token: "adapter-secret",
      }),
    )

    await expect(connectHttpAdapter({ url: "http://tracker.example.com" })).rejects.toThrow(
      HttpAdapterError,
    )
    await expect(connectHttpAdapter({ url, token: "wrong-secret" })).rejects.toMatchObject({
      code: "RESPONSE_REJECTED",
    })
    await expect(connectHttpAdapter({ url })).rejects.toMatchObject({
      code: "RESPONSE_REJECTED",
    })
  })
})
