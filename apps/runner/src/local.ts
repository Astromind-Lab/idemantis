import { chmodSync, lstatSync, mkdirSync } from "node:fs"
import { createServer } from "node:net"
import { join, resolve } from "node:path"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import type { ReferenceDeliveryInput } from "@idemantis/simulator"
import {
  AdapterHost,
  connectRemoteAdapters,
  type HostedAdapter,
  type RemoteAdapterConnection,
} from "./adapters.js"
import { HostedMissionRunner } from "./hosted.js"
import { ReferenceMissionRunner } from "./reference.js"
import { MissionRouter } from "./router.js"
import { createRunnerServer } from "./server.js"
import { SessionAuthority } from "./session.js"
import { RunnerStore } from "./store.js"

export interface LocalRunnerOptions {
  port?: number
  state_directory?: string
  reference_seed?: number
  reference_maximum_steps?: number
  reference_transition_delay_ms?: number
  adapters?: readonly HostedAdapter[]
  remote_adapters?: readonly RemoteAdapterConnection[]
}

export interface StartedLocalRunner {
  origin: string
  bootstrap_url: string
  journal_path: string
  definition_digest: string
  reference_input: ReferenceDeliveryInput
  close(): Promise<void>
}

function stateDirectory(path: string): string {
  const directory = resolve(path)
  if (directory === resolve(".")) {
    throw new Error("Runner state path must not be the project directory")
  }
  mkdirSync(directory, { recursive: true, mode: 0o700 })
  const metadata = lstatSync(directory)
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) {
    throw new Error("Runner state path must be a directory and cannot be a symbolic link")
  }
  chmodSync(directory, 0o700)
  return directory
}

async function availablePort(): Promise<number> {
  const server = createServer()
  await new Promise<void>((resolvePromise, reject) => {
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => resolvePromise())
  })
  const address = server.address()
  const port = typeof address === "object" && address ? address.port : undefined
  await new Promise<void>((resolvePromise, reject) => {
    server.close((error) => (error ? reject(error) : resolvePromise()))
  })
  if (!port) {
    throw new Error("Runner could not reserve a loopback port")
  }
  return port
}

export async function startLocalRunner(
  options: LocalRunnerOptions = {},
): Promise<StartedLocalRunner> {
  const requestedPort = options.port ?? 4318
  if (!Number.isSafeInteger(requestedPort) || requestedPort < 0 || requestedPort > 65_535) {
    throw new RangeError("Runner port must be an integer from 0 through 65535")
  }
  const port = requestedPort === 0 ? await availablePort() : requestedPort
  const adapters = [
    ...(options.adapters ?? []),
    ...(await connectRemoteAdapters(options.remote_adapters ?? [])),
  ]
  const directory = stateDirectory(options.state_directory ?? ".idemantis")
  const journalPath = join(directory, "runner.sqlite")
  const journal = new SqliteJournal({ path: journalPath })
  let app: Awaited<ReturnType<typeof createRunnerServer>> | undefined
  let hosted: HostedMissionRunner | undefined
  try {
    const store = new RunnerStore({ journal })
    const runner = new ReferenceMissionRunner({
      journal,
      store,
      ...(options.reference_seed !== undefined ? { seed: options.reference_seed } : {}),
      ...(options.reference_maximum_steps !== undefined
        ? { maximum_steps: options.reference_maximum_steps }
        : {}),
      transition_delay_ms: options.reference_transition_delay_ms ?? 350,
    })
    hosted =
      adapters.length > 0
        ? new HostedMissionRunner({
            journal,
            host: new AdapterHost({ adapters, source_id: "hosted.runner" }),
          })
        : undefined
    const router = new MissionRouter(journal, hosted ? [runner, hosted] : [runner])
    const sessions = new SessionAuthority()
    const bootstrap = sessions.issueBootstrap()
    const origin = `http://127.0.0.1:${port}`
    app = await createRunnerServer({
      store,
      sessions,
      exact_origin: origin,
      mission_executor: router,
    })
    await app.listen({ host: "127.0.0.1", port })
    let closed = false
    return {
      origin,
      bootstrap_url: `${origin}/#bootstrap=${encodeURIComponent(bootstrap)}`,
      journal_path: journalPath,
      definition_digest: runner.runtime.definition_digest,
      reference_input: runner.defaultInput(),
      close: async () => {
        if (closed) {
          return
        }
        closed = true
        try {
          await app?.close()
          await runner.waitForExecutions()
          await hosted?.close()
        } finally {
          journal.close()
        }
      },
    }
  } catch (error) {
    try {
      await app?.close()
      await hosted?.close()
    } catch {}
    journal.close()
    throw error
  }
}
