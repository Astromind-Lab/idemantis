import { resolve } from "node:path"
import { pathToFileURL } from "node:url"
import { parseArgs } from "node:util"
import { runBenchmark } from "./benchmark.js"

function numeric(value: string | undefined, label: string): number | undefined {
  if (value === undefined) {
    return undefined
  }
  const parsed = Number(value)
  if (!Number.isFinite(parsed)) {
    throw new TypeError(`--${label} must be a number`)
  }
  return parsed
}

export async function main(args: readonly string[], write: (value: string) => void) {
  const { values } = parseArgs({
    args: [...args],
    strict: true,
    allowPositionals: false,
    options: {
      seed: { type: "string" },
      tasks: { type: "string" },
      transient: { type: "string" },
      lost: { type: "string" },
      crash: { type: "string" },
    },
  })
  const seed = numeric(values.seed, "seed")
  const tasks = numeric(values.tasks, "tasks")
  const transient = numeric(values.transient, "transient")
  const lost = numeric(values.lost, "lost")
  const crash = numeric(values.crash, "crash")
  const report = await runBenchmark({
    ...(seed === undefined ? {} : { seed }),
    ...(tasks === undefined ? {} : { tasks }),
    rates: {
      ...(transient === undefined ? {} : { transient }),
      ...(lost === undefined ? {} : { lost_response: lost }),
      ...(crash === undefined ? {} : { crash }),
    },
  })
  write(`${JSON.stringify(report, null, 2)}\n`)
  return report.modes.idemantis.duplicate_side_effects === 0 &&
    report.modes.idemantis.completed_with_hidden_duplicates === 0
    ? 0
    : 1
}

const direct = process.argv[1]
  ? import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  : false

if (direct) {
  main(process.argv.slice(2), (value) => process.stdout.write(value)).then(
    (code) => {
      process.exitCode = code
    },
    (error: unknown) => {
      process.stderr.write(`${error instanceof Error ? error.message : "Benchmark failed"}\n`)
      process.exitCode = 1
    },
  )
}
