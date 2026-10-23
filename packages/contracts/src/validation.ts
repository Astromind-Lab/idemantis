import Schema from "typebox/schema"
import type { Static, TSchema } from "typebox"

/** Reports the schema and issues that rejected an external value. */
export class ContractValidationError extends Error {
  override readonly name = "ContractValidationError"

  constructor(
    readonly schemaId: string,
    readonly issues: readonly unknown[],
  ) {
    super(`Value does not satisfy ${schemaId}`)
  }
}

/** Compiles a schema into a typed parser for untrusted input. */
export function createContractParser<T extends TSchema>(schema: T) {
  const validator = Schema.Compile(schema)
  const configuredId = Reflect.get(schema, "$id")
  const schemaId = typeof configuredId === "string" ? configuredId : "anonymous contract"

  return (value: unknown): Static<T> => {
    if (!validator.Check(value)) {
      throw new ContractValidationError(schemaId, [...validator.Errors(value)])
    }

    return value as Static<T>
  }
}
