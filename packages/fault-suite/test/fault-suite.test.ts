import { describe, expect, it } from "vitest"
import { runFaultSuite } from "../src/index.js"

describe("engine and edge fault suite", () => {
  it("passes one hundred deterministic engine and edge seeds", async () => {
    const report = await runFaultSuite({ seed: 0, count: 100 })

    expect(report.passed).toBe(true)
    expect(report.failures).toEqual([])
    expect(report.runs).toHaveLength(100)
    expect(new Set(report.runs.map((run) => run.engine.name))).toEqual(
      new Set(report.named_cases.engine),
    )
    expect(new Set(report.runs.map((run) => run.edge.name))).toEqual(
      new Set(report.named_cases.edge),
    )
    expect(report.runs.every((run) => run.passed)).toBe(true)
    expect(report.final_state_digest).toMatch(/^sha256:[a-f0-9]{64}$/)
  })

  it("reproduces a generated schedule from its seed", async () => {
    const first = await runFaultSuite({ seed: 42, count: 1 })
    const replay = await runFaultSuite({ seed: 42, count: 1 })

    expect(replay).toEqual(first)
    expect(first.runs[0]?.engine.fault_schedule).toEqual(replay.runs[0]?.engine.fault_schedule)
    expect(first.runs[0]?.edge.fault_schedule).toEqual(replay.runs[0]?.edge.fault_schedule)
  })

  it("rejects invalid seed ranges", async () => {
    await expect(runFaultSuite({ seed: 0xffff_ffff, count: 2 })).rejects.toThrow(RangeError)
    await expect(runFaultSuite({ seed: -1, count: 1 })).rejects.toThrow(RangeError)
    await expect(runFaultSuite({ seed: 0, count: 0 })).rejects.toThrow(RangeError)
  })
})
