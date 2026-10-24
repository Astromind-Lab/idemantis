import { DefinitionIdSchema, DefinitionVersionSchema, JsonObjectSchema } from "@idemantis/contracts"
import Type, { type Static, type TSchema } from "typebox"
import Schema from "typebox/schema"
import { MissionDefinitionError } from "./errors.js"
import { ValueExpressionSchema } from "./schema.js"
import { sequentialMission } from "./sequence.js"
import type { OperationDefinition } from "./types.js"

export const MissionPlanSchema = Type.Object(
  {
    id: DefinitionIdSchema,
    version: DefinitionVersionSchema,
    input_schema: JsonObjectSchema,
    steps: Type.Array(
      Type.Object(
        {
          name: Type.String({
            minLength: 1,
            maxLength: 64,
            pattern: "^[a-z][a-zA-Z0-9]*(?:[._-][a-zA-Z0-9]+)*$",
          }),
          operation: Type.String({ minLength: 1, maxLength: 128 }),
          input: ValueExpressionSchema,
        },
        { additionalProperties: false },
      ),
      { minItems: 1, maxItems: 64 },
    ),
    attempts: Type.Optional(Type.Integer({ minimum: 2, maximum: 10 })),
    backoff_ms: Type.Optional(Type.Integer({ minimum: 0, maximum: 3_600_000 })),
  },
  { $id: "MissionPlan", additionalProperties: false },
)

export type MissionPlan = Static<typeof MissionPlanSchema>

export class PlanError extends Error {
  override readonly name = "PlanError"
}

export function planMission(plan: unknown, operations: readonly OperationDefinition[]) {
  if (!Schema.Check(MissionPlanSchema, plan)) {
    throw new PlanError("Mission plan does not match its schema")
  }
  const available = new Map(operations.map((operation) => [operation.key, operation]))
  const steps = plan.steps.map((step) => {
    const operation = available.get(step.operation)
    if (!operation) {
      throw new PlanError(`Operation ${step.operation} is not connected to this runner`)
    }
    return { name: step.name, input: step.input, operation }
  })
  try {
    return sequentialMission({
      id: plan.id,
      version: plan.version,
      input: plan.input_schema as unknown as TSchema,
      steps,
      ...(plan.attempts === undefined ? {} : { attempts: plan.attempts }),
      ...(plan.backoff_ms === undefined ? {} : { backoff_ms: plan.backoff_ms }),
    })
  } catch (error) {
    if (error instanceof MissionDefinitionError) {
      throw new PlanError(error.issues.map((item) => item.message).join("; "))
    }
    if (error instanceof TypeError) {
      throw new PlanError(error.message)
    }
    throw error
  }
}
