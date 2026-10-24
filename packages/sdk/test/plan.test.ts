import type { OperationCapabilities } from "@idemantis/contracts"
import Type from "typebox"
import { describe, expect, it } from "vitest"
import { defineOperation, type OperationDefinition, PlanError, planMission } from "../src/index.js"

const capabilities = {
  duplicate_safety: "keyed_uniqueness",
  reconciliation: "authoritative",
  fencing_mode: "none",
  fencing_scope: "operation",
  handoff_mode: "settle_first",
  execution_location: "either",
  cancellation: false,
  compensation: false,
  observation_stream: false,
} satisfies OperationCapabilities

const descriptors: OperationDefinition[] = [
  defineOperation({
    key: "web.search",
    adapter: "agent-tools",
    operation: "web.search",
    schemaVersion: 1,
    inputSchema: Type.Object({ query: Type.String() }, { additionalProperties: false }),
    outputSchema: Type.Object({ url: Type.String() }, { additionalProperties: false }),
    effectClass: "observation",
    consequence: "observational",
    capabilities,
  }),
  defineOperation({
    key: "doc.summarize",
    adapter: "agent-tools",
    operation: "doc.summarize",
    schemaVersion: 1,
    inputSchema: Type.Object({ url: Type.String() }, { additionalProperties: false }),
    outputSchema: Type.Object({ summary: Type.String() }, { additionalProperties: false }),
    effectClass: "record_mutation",
    consequence: "reversible",
    capabilities,
  }),
]

const plan = {
  id: "research.brief",
  version: "1.0.0",
  input_schema: {
    type: "object",
    properties: { query: { type: "string" } },
    required: ["query"],
    additionalProperties: false,
  },
  steps: [
    {
      name: "search",
      operation: "web.search",
      input: { kind: "object", fields: { query: { kind: "input", path: ["query"] } } },
    },
    {
      name: "summarize",
      operation: "doc.summarize",
      input: {
        kind: "object",
        fields: { url: { kind: "output", state: "search", path: ["url"] } },
      },
    },
  ],
}

describe("mission plans", () => {
  it("compiles a JSON plan over connected operations into a durable mission", () => {
    const compiled = planMission(plan, descriptors)

    expect(compiled.manifest.initial).toBe("search")
    expect(Object.keys(compiled.manifest.states).sort()).toEqual([
      "done",
      "failed",
      "search",
      "search_reconcile",
      "summarize",
      "summarize_reconcile",
      "unresolved",
    ])
    const { search } = compiled.manifest.states
    expect(search).toMatchObject({
      capture_output: true,
      retry: { kind: "safe" },
    })
    expect(planMission(plan, descriptors).definitionDigest).toBe(compiled.definitionDigest)
  })

  it("skips retries and reconciliation an operation cannot support", () => {
    const unsafe = descriptors.map(
      (descriptor): OperationDefinition => ({
        ...descriptor,
        capabilities: {
          ...descriptor.capabilities,
          duplicate_safety: "none",
          reconciliation: "none",
        },
      }),
    )
    const compiled = planMission(plan, unsafe)

    expect(compiled.manifest.states).not.toHaveProperty("search_reconcile")
    const { search } = compiled.manifest.states
    expect(search).toMatchObject({
      retry: { kind: "none" },
      on_uncertain: "unresolved",
    })
  })

  it("explains why a plan cannot be compiled", () => {
    expect(() => planMission({ ...plan, steps: [] }, descriptors)).toThrow(PlanError)
    expect(() =>
      planMission({ ...plan, steps: [{ ...plan.steps[0], operation: "shell.exec" }] }, descriptors),
    ).toThrow(/not connected/)
    expect(() =>
      planMission({ ...plan, steps: [plan.steps[1], plan.steps[0]] }, descriptors),
    ).toThrow(/can be reached before search produces an output/)
    expect(() =>
      planMission({ ...plan, steps: [plan.steps[0], plan.steps[0]] }, descriptors),
    ).toThrow(/reserved or repeated/)
  })
})
