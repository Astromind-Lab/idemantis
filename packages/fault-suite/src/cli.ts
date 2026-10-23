import { pathToFileURL } from "node:url"
import { runFaultSuite } from "./suite.js"

function integer(value: string | undefined, option: string): number {
  if (value === undefined || !/^\d+$/.test(value)) {
    throw new TypeError(`${option} requires a non-negative integer`)
  }
  return Number(value)
}

function options(args: string[]): { seed?: number; count?: number } {
  const parsed: { seed?: number; count?: number } = {}
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index]
    if (argument === "--seed") {
      parsed.seed = integer(args[index + 1], "--seed")
      index += 1
    } else if (argument?.startsWith("--seed=")) {
      parsed.seed = integer(argument.slice("--seed=".length), "--seed")
    } else if (argument === "--count") {
      parsed.count = integer(args[index + 1], "--count")
      index += 1
    } else if (argument?.startsWith("--count=")) {
      parsed.count = integer(argument.slice("--count=".length), "--count")
    } else {
      throw new TypeError(`Unknown option ${argument ?? ""}`)
    }
  }
  return parsed
}

export async function runCli(args: string[]): Promise<number> {
  const report = await runFaultSuite(options(args))
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`)
  return report.passed ? 0 : 1
}

const invokedPath = process.argv[1]
if (invokedPath && import.meta.url === pathToFileURL(invokedPath).href) {
  runCli(process.argv.slice(2)).then(
    (code) => {
      process.exitCode = code
    },
    (error: unknown) => {
      const message = error instanceof Error ? error.message : "Fault suite failed"
      process.stderr.write(`${JSON.stringify({ error: message })}\n`)
      process.exitCode = 1
    },
  )
}
