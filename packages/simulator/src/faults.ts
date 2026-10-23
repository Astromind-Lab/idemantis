import Schema from "typebox/schema"
import { SimulationError } from "./errors.js"
import { FaultScheduleSchema } from "./schemas.js"
import type { FaultMatch, FaultRule, FaultRuleState } from "./types.js"

function clone<Value>(value: Value): Value {
  return structuredClone(value)
}

export class FaultSchedule {
  readonly #rules: FaultRuleState[]

  constructor(rules: readonly FaultRule[] = []) {
    if (!Schema.Check(FaultScheduleSchema, rules)) {
      throw new SimulationError("INVALID_FAULT", "Fault schedule does not satisfy its schema")
    }
    if (new Set(rules.map((rule) => rule.id)).size !== rules.length) {
      throw new SimulationError("DUPLICATE_FAULT", "Fault schedule repeats a rule identity")
    }
    for (const rule of rules) {
      if ((rule.action === "delay") !== (rule.delay_ms !== undefined)) {
        throw new SimulationError(
          "INVALID_FAULT",
          `Fault ${rule.id} must declare delay_ms only for a delay action`,
        )
      }
    }
    this.#rules = rules.map((rule) => ({ ...clone(rule), consumed: false, matches: 0 }))
  }

  take(match: FaultMatch): FaultRule | undefined {
    for (const rule of this.#rules) {
      if (
        rule.consumed ||
        rule.target !== match.target ||
        rule.operation !== match.operation ||
        rule.stage !== match.stage
      ) {
        continue
      }
      rule.matches += 1
      if (rule.matches === rule.occurrence) {
        rule.consumed = true
        const { consumed: _consumed, matches: _matches, ...value } = rule
        return clone(value)
      }
    }
    return undefined
  }

  snapshot(): FaultRuleState[] {
    return clone(this.#rules)
  }
}
