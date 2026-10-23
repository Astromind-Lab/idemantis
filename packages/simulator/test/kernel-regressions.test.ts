import { describe, expect, it } from "vitest"
import { FaultSchedule, SimulationKernel } from "../src/index.js"

describe("simulation kernel regressions", () => {
  it("counts every matching fault occurrence before returning a fault", () => {
    const schedule = new FaultSchedule([
      {
        id: "first",
        target: "service",
        operation: "record.update",
        stage: "after_apply",
        occurrence: 1,
        action: "drop_response",
      },
      {
        id: "second",
        target: "service",
        operation: "record.update",
        stage: "after_apply",
        occurrence: 2,
        action: "drop_response",
      },
    ])
    const match = { target: "service", operation: "record.update", stage: "after_apply" }

    expect(schedule.take(match)?.id).toBe("first")
    expect(schedule.take(match)?.id).toBe("second")
    expect(schedule.snapshot()).toEqual([
      expect.objectContaining({ id: "first", matches: 1, consumed: true }),
      expect.objectContaining({ id: "second", matches: 2, consumed: true }),
    ])
  })

  it("rejects competing faults for the same occurrence", () => {
    expect(
      () =>
        new FaultSchedule([
          {
            id: "first",
            target: "service",
            operation: "record.update",
            stage: "after_apply",
            occurrence: 1,
            action: "drop_response",
          },
          {
            id: "also-first",
            target: "service",
            operation: "record.update",
            stage: "after_apply",
            occurrence: 1,
            action: "delay",
            delay_ms: 10,
          },
        ]),
    ).toThrowError(expect.objectContaining({ code: "DUPLICATE_FAULT" }))
  })

  it("leaves the queue, trace, task identity, and random state unchanged after rejection", () => {
    const rejected = new SimulationKernel({ seed: 19 })
    const fresh = new SimulationKernel({ seed: 19 })

    expect(() =>
      rejected.schedule(
        {
          target: "invalid target",
          operation: "job.run",
          stage: "job.finished",
          correlation_id: "job-1",
          after_ms: 10,
          jitter_ms: 50,
        },
        () => undefined,
      ),
    ).toThrowError(expect.objectContaining({ code: "INVALID_SCHEDULE" }))

    const work = {
      target: "service",
      operation: "job.run",
      stage: "job.finished",
      correlation_id: "job-1",
      after_ms: 10,
      jitter_ms: 50,
    }
    expect(rejected.schedule(work, () => undefined)).toBe(1)
    expect(fresh.schedule(work, () => undefined)).toBe(1)
    expect(rejected.pendingTasks()).toBe(1)
    expect(rejected.snapshot()).toEqual(fresh.snapshot())
  })
})
