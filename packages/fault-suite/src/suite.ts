import { sha256Digest } from "@idemantis/contracts"
import { namedEdgeCases, namedEngineCases } from "./catalog.js"
import { runEdgeCase } from "./edge.js"
import { runEngineCase } from "./engine.js"
import type {
  FaultFailure,
  FaultSuiteOptions,
  FaultSuiteReport,
  InvariantResult,
  SeedReport,
} from "./types.js"

const engineVersion = "0.0.0"

function validateOptions(options: FaultSuiteOptions): { seed: number; count: number } {
  const seed = options.seed ?? 0
  const count = options.count ?? 100
  if (!Number.isSafeInteger(seed) || seed < 0 || seed > 0xffff_ffff) {
    throw new RangeError("seed must be a 32-bit unsigned integer")
  }
  if (!Number.isSafeInteger(count) || count < 1 || count > 10_000) {
    throw new RangeError("count must be a safe integer from 1 through 10000")
  }
  if (seed + count - 1 > 0xffff_ffff) {
    throw new RangeError("seed range exceeds 32-bit unsigned integers")
  }
  return { seed, count }
}

function failuresFor(seed: number, caseName: string, values: InvariantResult[]): FaultFailure[] {
  return values
    .filter((value) => !value.passed)
    .map((value) => ({
      seed,
      case: caseName,
      invariant: value.name,
      detail: value.detail ?? "Invariant failed without detail",
      reproduce: `pnpm --silent proof:faults --seed ${seed} --count 1`,
    }))
}

export async function runFaultSuite(options: FaultSuiteOptions = {}): Promise<FaultSuiteReport> {
  const { seed, count } = validateOptions(options)
  const runs: SeedReport[] = []
  const failures: FaultFailure[] = []
  const invariantNames = new Set<string>()

  for (let offset = 0; offset < count; offset += 1) {
    const selectedSeed = seed + offset
    const engine = await runEngineCase(selectedSeed)
    const edge = await runEdgeCase(selectedSeed)
    for (const invariant of [...engine.invariants, ...edge.invariants]) {
      invariantNames.add(invariant.name)
    }
    failures.push(...failuresFor(selectedSeed, engine.name, engine.invariants))
    failures.push(...failuresFor(selectedSeed, edge.name, edge.invariants))
    runs.push({
      seed: selectedSeed,
      engine,
      edge,
      passed:
        engine.invariants.every((invariant) => invariant.passed) &&
        edge.invariants.every((invariant) => invariant.passed),
    })
  }

  return {
    schema_version: 1,
    engine_version: engineVersion,
    seed,
    count,
    named_cases: { engine: [...namedEngineCases], edge: [...namedEdgeCases] },
    invariants_checked: [...invariantNames].sort(),
    runs,
    failures,
    passed: failures.length === 0,
    final_state_digest: sha256Digest(
      runs.map((run) => ({
        seed: run.seed,
        engine: run.engine.final_state_digest,
        edge: run.edge.final_state_digest,
      })),
    ),
  }
}
