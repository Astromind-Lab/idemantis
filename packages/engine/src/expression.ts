import type { JsonObject, JsonValue } from "@idemantis/contracts"
import type { ValueExpression } from "@idemantis/sdk"
import { ReductionError } from "./errors.js"

function cloneJson<Value extends JsonValue>(value: Value): Value {
  return JSON.parse(JSON.stringify(value)) as Value
}

function select(root: JsonValue, path: readonly string[], label: string): JsonValue {
  let value: JsonValue = root

  for (const segment of path) {
    if (
      value === null ||
      typeof value !== "object" ||
      Array.isArray(value) ||
      !Object.hasOwn(value, segment)
    ) {
      throw new ReductionError("INVALID_TRANSITION", `${label} does not resolve to a value`)
    }

    const next: JsonValue | undefined = value[segment]
    if (next === undefined) {
      throw new ReductionError("INVALID_TRANSITION", `${label} does not resolve to a value`)
    }

    value = next
  }

  return cloneJson(value)
}

export function evaluateExpression(
  expression: ValueExpression,
  input: JsonObject,
  outputs: Readonly<Record<string, JsonValue>> = {},
): JsonValue {
  switch (expression.kind) {
    case "output": {
      const label = `Output path ${[expression.state, ...expression.path].join(".")}`
      const output = Object.hasOwn(outputs, expression.state)
        ? outputs[expression.state]
        : undefined
      if (output === undefined) {
        throw new ReductionError("INVALID_TRANSITION", `${label} does not resolve to a value`)
      }
      return select(output, expression.path, label)
    }
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
        value[field] = evaluateExpression(
          expression.fields[field] as ValueExpression,
          input,
          outputs,
        )
      }

      return value
    }
  }
}
