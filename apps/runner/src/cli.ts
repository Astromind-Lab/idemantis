import { resolve } from "node:path"
import { pathToFileURL } from "node:url"
import { parseArgs } from "node:util"
import type { AdapterInstanceId, JsonObject } from "@idemantis/contracts"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import type { RemoteAdapterConnection } from "./adapters.js"
import { startLocalRunner } from "./local.js"
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

export async function runCli(
  args: readonly string[],
  write: Writer = process.stdout.write.bind(process.stdout),
) {
  if (args[0] === "reference") {
    write(`${JSON.stringify(await referenceProof(), null, 2)}\n`)
    return 0
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
      },
    })
    const port = parsed.values.port === undefined ? 4318 : Number(parsed.values.port)
    const { IDEMANTIS_ADAPTER_TOKEN: adapterToken } = process.env
    const started = await startLocalRunner({
      port,
      ...(parsed.values["state-dir"] ? { state_directory: parsed.values["state-dir"] } : {}),
      remote_adapters: adapterConnections(parsed.values.adapter, adapterToken),
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
  write("Usage: idemantis-runner <reference|dev [--adapter <adapter-instance-id>=<url>]>\n")
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
