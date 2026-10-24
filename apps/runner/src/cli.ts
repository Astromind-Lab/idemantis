import { resolve } from "node:path"
import { pathToFileURL } from "node:url"
import { parseArgs } from "node:util"
import type { AdapterInstanceId, JsonObject } from "@idemantis/contracts"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import type { RemoteAdapterConnection } from "./adapters.js"
import { ApiKeyStore, parseScopes } from "./keys.js"
import { accessPath, startLocalRunner, stateDirectory } from "./local.js"
import { ReferenceMissionRunner } from "./reference.js"
import { RunnerStore } from "./store.js"

type Writer = (value: string) => void

export function adapterConnections(
  values: readonly string[] | undefined,
  token: string | undefined,
): RemoteAdapterConnection[] {
  return (values ?? []).map((value) => {
    const separator = value.indexOf("=")
    if (separator < 1 || separator === value.length - 1) {
      throw new TypeError("--adapter requires <adapter-instance-id>=<url>")
    }
    return {
      adapter_instance_id: value.slice(0, separator) as AdapterInstanceId,
      url: value.slice(separator + 1),
      ...(token ? { token } : {}),
    }
  })
}

export function leaseDuration(value: string | undefined): number | undefined {
  if (value === undefined) {
    return undefined
  }
  const parsed = Number(value)
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(parsed) || parsed < 1_000) {
    throw new TypeError("--lease-ms must be an integer of at least 1000")
  }
  return parsed
}

export function rateLimitPerMinute(value: string | undefined): number | undefined {
  if (value === undefined) {
    return undefined
  }
  const parsed = Number(value)
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(parsed) || parsed < 1) {
    throw new TypeError("--rate-limit must be a positive integer of requests per minute")
  }
  return parsed
}

async function referenceProof(): Promise<JsonObject> {
  const journal = new SqliteJournal({ path: ":memory:" })
  try {
    const store = new RunnerStore({ journal })
    const runner = new ReferenceMissionRunner({ journal, store })
    const admitted = store.admitMission({
      definition_digest: runner.runtime.definition_digest,
      input: runner.defaultInput() as JsonObject,
    })
    await runner.run(admitted.summary.run_id)
    const report = runner.report(admitted.summary.run_id)
    const replay = journal.verifyRun(admitted.summary.run_id)
    return {
      run_id: admitted.summary.run_id,
      definition: {
        id: report.detail.summary.definition_id,
        version: report.detail.summary.definition_version,
        digest: report.detail.definition_digest,
        execution_binding_digest: report.detail.execution_binding_digest,
      },
      outcome: {
        status: report.detail.projection.status,
        terminal_outcome: report.detail.projection.terminal_outcome,
        sequence: report.detail.projection.current_sequence,
        state_digest: report.detail.projection.state_digest,
      },
      graph: {
        digest: report.detail.graph.graph_digest,
        nodes: report.detail.graph.nodes.length,
        transitions: report.detail.graph.transitions.length,
      },
      effects: report.effects.items.map((effect) => ({
        operation: effect.operation,
        status: effect.status,
        issue_count: effect.issue_count,
      })),
      evidence: {
        timeline_events: report.timeline.items.length,
        simulator_observations: report.simulation.observations.length,
        simulation_trace_digest: report.simulation.trace_digest,
        target_state_digests: report.targets.map((target) => ({
          target: target.target,
          state_digest: target.state_digest,
        })),
      },
      replay: {
        events: replay.event_count,
        commands: replay.command_count,
        snapshots: replay.snapshots_verified,
        state_digest: replay.state_digest,
      },
    }
  } finally {
    journal.close()
  }
}

function keysCommand(args: readonly string[], write: Writer): number {
  const parsed = parseArgs({
    args: [...args.slice(1)],
    strict: true,
    allowPositionals: false,
    options: {
      name: { type: "string" },
      scopes: { type: "string" },
      "state-dir": { type: "string" },
    },
  })
  const store = new ApiKeyStore({
    path: accessPath(stateDirectory(parsed.values["state-dir"] ?? ".idemantis")),
  })
  try {
    if (args[0] === "list") {
      write(`${JSON.stringify(store.list(), null, 2)}\n`)
      return 0
    }
    const { name } = parsed.values
    if (!name) {
      throw new TypeError(`keys ${args[0]} requires --name <name>`)
    }
    if (args[0] === "create") {
      const created = store.create(name, parseScopes(parsed.values.scopes ?? "view,operate"))
      write(`${JSON.stringify({ ...created.record, token: created.token }, null, 2)}\n`)
      return 0
    }
    if (args[0] === "revoke") {
      write(`${JSON.stringify(store.revoke(name), null, 2)}\n`)
      return 0
    }
    throw new TypeError("keys requires create, list, or revoke")
  } finally {
    store.close()
  }
}

export async function runCli(
  args: readonly string[],
  write: Writer = process.stdout.write.bind(process.stdout),
) {
  if (args[0] === "reference") {
    write(`${JSON.stringify(await referenceProof(), null, 2)}\n`)
    return 0
  }
  if (args[0] === "keys") {
    return keysCommand(args.slice(1), write)
  }
  if (args[0] === "dev") {
    const parsed = parseArgs({
      args: [...args.slice(1)],
      strict: true,
      allowPositionals: false,
      options: {
        port: { type: "string" },
        "state-dir": { type: "string" },
        adapter: { type: "string", multiple: true },
        "lease-ms": { type: "string" },
        "rate-limit": { type: "string" },
      },
    })
    const port = parsed.values.port === undefined ? 4318 : Number(parsed.values.port)
    const lease = leaseDuration(parsed.values["lease-ms"])
    const rateLimit = rateLimitPerMinute(parsed.values["rate-limit"])
    const { IDEMANTIS_ADAPTER_TOKEN: adapterToken } = process.env
    const started = await startLocalRunner({
      port,
      ...(parsed.values["state-dir"] ? { state_directory: parsed.values["state-dir"] } : {}),
      remote_adapters: adapterConnections(parsed.values.adapter, adapterToken),
      ...(lease === undefined ? {} : { lease_duration_ms: lease }),
      ...(rateLimit === undefined ? {} : { rate_limit_per_minute: rateLimit }),
    })
    write(`Runner ready: ${started.origin}\n`)
    write(`Operator bootstrap: ${started.bootstrap_url}\n`)
    write(`Journal: ${started.journal_path}\n`)
    if (started.resumed.length > 0) {
      write(`Resumed missions: ${started.resumed.length}\n`)
    }
    await new Promise<void>((resolvePromise) => {
      process.once("SIGINT", resolvePromise)
      process.once("SIGTERM", resolvePromise)
    })
    await started.close()
    return 0
  }
  write(
    [
      "Usage:",
      "  idemantis-runner reference",
      "  idemantis-runner dev [--port <port>] [--state-dir <dir>] [--adapter <adapter-instance-id>=<url>] [--lease-ms <ms>] [--rate-limit <per-minute>]",
      "  idemantis-runner keys create --name <name> [--scopes view,operate,define] [--state-dir <dir>]",
      "  idemantis-runner keys list [--state-dir <dir>]",
      "  idemantis-runner keys revoke --name <name> [--state-dir <dir>]",
      "",
    ].join("\n"),
  )
  return 1
}

const direct = process.argv[1]
  ? import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  : false

if (direct) {
  runCli(process.argv.slice(2)).then(
    (code) => {
      process.exitCode = code
    },
    (error: unknown) => {
      process.stderr.write(
        `${error instanceof Error ? error.message : "Reference mission failed"}\n`,
      )
      process.exitCode = 1
    },
  )
}
