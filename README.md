# idemantis

Mission control for AI agents.

Idemantis runs the side effects of an agent's work as durable missions. Every tool call is journaled before it is sent and after it returns. A call that may already have happened is never blindly repeated: Idemantis retries only when duplicate suppression is proven, asks the tool what happened when it can, and otherwise stops and flags the outcome as unknown. When an agent crashes or loses its context, it asks one question, "where was I?", and gets back what is done (with each step's output), what is uncertain, and who has to act next.

It works with any agent: over MCP, from TypeScript or Python, or over plain HTTP. Tools can be written in any language that speaks the HTTP adapter protocol.

## Why

Agents retry tools after timeouts without knowing whether the first call landed, so customers get two emails or two charges. Durable-execution engines and framework checkpointers make code resume after a crash, but a step that applied its effect and crashed before saving runs again. Idemantis treats that gap as the product:

- each operation declares its duplicate safety and how it can be reconciled,
- the runner commits `dispatch_started` before every call, so "may have happened" is an explicit state,
- a tool ledger makes plain functions safely retryable and recovers lost responses through a lookup,
- missions resume after a restart without redoing work, and later steps reuse earlier outputs.

## Benchmark

`packages/benchmark` runs the same three-step task (`create_ticket`, `charge_card`, `send_receipt`) for three agents against a seeded external world with transient failures, lost responses, and process crashes.

| Agent | Completed | Duplicate side effects |
| --- | --- | --- |
| Naive retry loop | 97.8% | 292 |
| Checkpoint after each step | 97.8% | 209 |
| Idemantis | 99.2% | 0 |

Default fault rates (10% transient failures, 8% lost responses, 4% crashes), 500 tasks, seed 1: `node packages/benchmark/dist/cli.js --tasks 500 --seed 1`. With 30% lost responses and 10% crashes over 300 tasks, the naive agent completes 75% with 764 duplicates and Idemantis completes 99% with none: `node packages/benchmark/dist/cli.js --tasks 300 --seed 1 --lost 0.3 --crash 0.1`. CI runs `pnpm run proof:agents`, which fails if Idemantis records any duplicate.

## Guarantees

- An accepted event, its state change, and the commands it creates commit in one transaction.
- Effect identities are stable across retries and restarts; tools receive the effect ID as an idempotency key.
- An effect that may have happened is re-issued only when the tool or target enforces duplicate suppression.
- An outcome that cannot be proven stops the mission as `unresolved` instead of guessing.
- Every run can be replayed from the journal to the same state digest.

Idemantis does not make external systems exactly-once on its own. A tool without idempotency or a lookup can still leave an outcome unknown; Idemantis then says so.

## Quickstart

Requirements: Node.js 22.13 or later, pnpm, and Python 3.10 or later for the Python example. Packages are not published yet, so the examples run from this repository.

```sh
pnpm install
pnpm run build
```

### 1. Serve tools

Save as `tools.py`:

```python
from idemantis import ToolLedger, ToolServer

tickets = {}
server = ToolServer(adapter="demo-tools", version="1.0.0", ledger=ToolLedger("tools.sqlite"), token="tool-secret")

@server.tool(
    "crm.create_ticket",
    input_schema={"type": "object", "properties": {"subject": {"type": "string"}}, "required": ["subject"]},
    output_schema={"type": "object", "properties": {"ticket": {"type": "string"}}, "required": ["ticket"]},
)
def create_ticket(value, context):
    ticket = f"T-{len(tickets) + 1}"
    tickets[context.idempotency_key] = ticket
    return {"ticket": ticket}

@server.tool(
    "chat.notify",
    input_schema={"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
    output_schema={"type": "object", "properties": {"sent": {"type": "boolean"}}, "required": ["sent"]},
)
def notify(value, context):
    print("notify:", value["text"], flush=True)
    return {"sent": True}

print("tools ready on", server.serve(port=4400), flush=True)
input()
```

```sh
PYTHONPATH=sdks/python/src python3 tools.py
```

### 2. Start the runner

```sh
IDEMANTIS_ADAPTER_TOKEN=tool-secret node apps/runner/dist/cli.js dev \
  --adapter adp_01JBQ8T4XKZ5Y9V2N3M6R7P8W1=http://127.0.0.1:4400/
```

The runner listens on `http://127.0.0.1:4318`, keeps its journal in `.idemantis/`, and resumes unfinished missions when it restarts.

### 3. Create an API key

```sh
node apps/runner/dist/cli.js keys create --name my-agent --scopes view,operate,define
```

The token is printed once. Keys are stored only as digests and can be revoked with `keys revoke --name my-agent`.

### 4. Define a plan and run it

```python
from idemantis import IdemantisClient, fields, from_input, from_output, plan, step

client = IdemantisClient("http://127.0.0.1:4318", "idm_...")
digest = client.register_plan(plan(
    "support.open_ticket",
    "1.0.0",
    {"type": "object", "properties": {"subject": {"type": "string"}}, "required": ["subject"]},
    [
        step("create", "crm.create_ticket", fields(subject=from_input("subject"))),
        step("notify", "chat.notify", fields(text=from_output("create", "ticket"))),
    ],
))["definition_digest"]

run_id = client.launch(digest, {"subject": "Printer offline"}, idempotency_key="ticket-1042")["summary"]["run_id"]
context = client.wait_for(run_id)
print(context["terminal_outcome"], [effect["output"] for effect in context["effects"]])
```

Calling `launch` again with the same `idempotency_key` returns the same mission. Kill the runner at any point and start it again; the mission continues from the last proven step, and no tool runs twice.

### 5. Connect an MCP agent

```json
{
  "mcpServers": {
    "idemantis": {
      "command": "node",
      "args": ["/path/to/idemantis/packages/mcp/dist/cli.js"],
      "env": {
        "IDEMANTIS_URL": "http://127.0.0.1:4318",
        "IDEMANTIS_API_KEY": "idm_..."
      }
    }
  }
}
```

The agent gets these tools: `list_operations`, `define_plan`, `list_definitions`, `start_mission`, `get_mission_context`, `list_missions`, `wait_for_mission`, `record_observation`, and `resolve_operator_wait`.

## TypeScript

```ts
import { createToolAdapter, defineTool, serveTools, ToolLedger } from "@idemantis/tools"
import { IdemantisClient } from "@idemantis/client"
import Type from "typebox"

const createTicket = defineTool({
  name: "crm.create_ticket",
  input: Type.Object({ subject: Type.String() }),
  output: Type.Object({ ticket: Type.String() }),
  run: async (input, context) => crm.create(input.subject, { reference: context.idempotency_key }),
  lookup: async (_input, context) => {
    const found = await crm.findByReference(context.idempotency_key)
    return found ? { found: true, output: found } : { found: false }
  },
})

await serveTools(
  createToolAdapter({
    adapter: "demo-tools",
    version: "1.0.0",
    tools: [createTicket],
    ledger: new ToolLedger({ path: "tools.sqlite" }),
  }),
  { port: 4400, token: "tool-secret" },
)

const client = new IdemantisClient({ url: "http://127.0.0.1:4318", api_key: process.env.IDEMANTIS_API_KEY ?? "" })
const mission = await client.launch(digest, { subject: "Printer offline" }, { client_request_id: "ticket-1042" })
const context = await client.waitFor(mission.summary.run_id)
```

Tool errors: return a value for success, throw `ToolUnavailable` when nothing happened and a retry is safe, throw `ToolRejected` for a definitive refusal. Any other exception is treated as "may have happened" and is resolved through `lookup` or flagged.

For full control, author missions with `defineMission` from `@idemantis/sdk`: effect states, observation waits, operator decisions, reconciliation states, and `fromOutput` to pass results between steps.

## API

All endpoints accept `Authorization: Bearer <key>`.

| Method and path | Purpose |
| --- | --- |
| `GET /api/operations` | Connected operations and their capabilities |
| `POST /api/plans` | Compile and register a JSON plan (`define` scope) |
| `POST /api/definitions` | Register a compiled definition (`define` scope) |
| `POST /api/missions` | Admit a mission; `client_request_id` makes it idempotent |
| `POST /api/missions/:runId/start` | Start execution |
| `GET /api/missions/:runId/context` | Resume context: done, uncertain, next actor |
| `POST /api/missions/:runId/observations` | Record an outside observation, idempotent per ID |
| `GET /api/missions`, `/api/missions/:runId`, `/timeline`, `/effects` | Read models |
| `POST /api/missions/:runId/interventions/prepare`, `/submit` | Answer an operator decision |
| `GET /api/streams/operations`, `/api/missions/:runId/stream` | Server-sent events |

Tools speak the HTTP adapter protocol: `GET /manifest` and `POST /operations/<key>/issue` or `/reconcile`, with the effect ID in the `idempotency-key` header.

## Repository

| Path | Contents |
| --- | --- |
| `apps/runner` | Runner API, CLI, hosted execution, API keys |
| `packages/sdk` | Mission DSL, compiler, plans |
| `packages/engine` | Pure reducer and replay |
| `packages/journal-sqlite` | Append-only SQLite journal |
| `packages/dispatcher` | Command dispatch, timers, crash recovery |
| `packages/adapter-kit`, `packages/adapter-http` | Operation contract and HTTP adapter protocol |
| `packages/tools` | Exactly-once tool ledger and tool server |
| `packages/client` | TypeScript API client |
| `packages/mcp` | MCP server |
| `packages/benchmark` | Agent reliability benchmark |
| `packages/fault-suite` | Seeded fault and invariant proofs |
| `sdks/python` | Python client, plan builders, tool server |

## Development

```sh
pnpm run check
pnpm run proof:faults
pnpm run proof:agents
PYTHONPATH=sdks/python/src python3 -m unittest discover -s sdks/python/tests
```

## Status

Pre-release. Single-tenant, loopback-only runner with a SQLite journal. See [SECURITY.md](./SECURITY.md) for reporting vulnerabilities.

## License

[Apache 2.0](./LICENSE)
