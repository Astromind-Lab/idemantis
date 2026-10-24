import { describe, expect, it } from "vitest"
import { main } from "../src/cli.js"
import { ExternalWorld, faultFor, runBenchmark } from "../src/index.js"

describe("agent reliability benchmark", () => {
  it("shows idemantis completing more work without duplicate side effects", async () => {
    const report = await runBenchmark({ seed: 7, tasks: 120 })
    const { naive, checkpointed, idemantis } = report.modes

    expect(idemantis.duplicate_side_effects).toBe(0)
    expect(idemantis.completed_with_hidden_duplicates).toBe(0)
    expect(naive.duplicate_side_effects).toBeGreaterThan(0)
    expect(checkpointed.duplicate_side_effects).toBeGreaterThan(0)
    expect(checkpointed.duplicate_side_effects).toBeLessThan(naive.duplicate_side_effects)
    expect(idemantis.completion_rate).toBeGreaterThanOrEqual(checkpointed.completion_rate)
    expect(idemantis.restarts).toBeGreaterThan(0)
  })

  it("is deterministic for a seed", async () => {
    expect(await runBenchmark({ seed: 3, tasks: 40 })).toEqual(
      await runBenchmark({ seed: 3, tasks: 40 }),
    )
  })

  it("draws faults from the configured rates", () => {
    const rates = { transient: 0.2, lost_response: 0.2, crash: 0.1 }
    const faults = Array.from({ length: 4_000 }, (_, index) => faultFor(1, `k${index}`, rates))
    const share = (kind: string) => faults.filter((fault) => fault === kind).length / 4_000

    expect(share("transient")).toBeCloseTo(0.2, 1)
    expect(share("lost_response")).toBeCloseTo(0.2, 1)
    expect(share("crash")).toBeCloseTo(0.1, 1)
    expect(
      new ExternalWorld(1, { transient: 0, lost_response: 0, crash: 0 }).call("t", "create_ticket"),
    ).toEqual({
      id: "create_ticket-t-1",
    })
  })

  it("validates options and reports through the command line", async () => {
    await expect(runBenchmark({ tasks: 0 })).rejects.toThrow(RangeError)
    await expect(
      runBenchmark({ rates: { transient: 0.5, lost_response: 0.5, crash: 0.1 } }),
    ).rejects.toThrow(RangeError)
    const output: string[] = []

    expect(await main(["--tasks", "20", "--seed", "2"], (value) => output.push(value))).toBe(0)
    expect(JSON.parse(output.join(""))).toMatchObject({ tasks: 20, seed: 2 })
    await expect(main(["--tasks", "many"], () => {})).rejects.toThrow(TypeError)
  })
})
