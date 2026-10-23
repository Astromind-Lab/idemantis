import type { JsonObject, JsonValue } from "@idemantis/contracts"
import type { ValueExpression } from "@idemantis/sdk"
import { ReductionError } from "./errors.js"

function cloneJson<Value extends JsonValue>(value: Value): Value {
  return JSON.parse(JSON.stringify(value)) as Value
}

export function evaluateExpression(expression: ValueExpression, input: JsonObject): JsonValue {
  switch (expression.kind) {
    case "literal":
      return cloneJson(expression.value)
    case "input": {
      let value: JsonValue = input

      for (const segment of expression.path) {
        if (
          value === null ||
          typeof value !== "object" ||
          Array.isArray(value) ||
          !Object.hasOwn(value, segment)
        ) {
          throw new ReductionError(
            "INVALID_TRANSITION",
            `Input path ${expression.path.join(".")} does not resolve to a value`,
          )
        }

        const next: JsonValue | undefined = value[segment]
        if (next === undefined) {
          throw new ReductionError(
            "INVALID_TRANSITION",
            `Input path ${expression.path.join(".")} does not resolve to a value`,
          )
        }

        value = next
      }

      return cloneJson(value)
    }
    case "object": {
      const value = Object.create(null) as JsonObject

      for (const field of Object.keys(expression.fields).sort()) {
        value[field] = evaluateExpression(expression.fields[field] as ValueExpression, input)
      }

      return value
    }
  }
}
