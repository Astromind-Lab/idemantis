import Type from "typebox"

/** A value that can be represented without loss in JSON. */
export type JsonValue = null | boolean | number | string | JsonValue[] | JsonObject
export type JsonObject = { [key: string]: JsonValue }

/** Runtime validation for recursively nested JSON values. */
export const JsonValueSchema = Type.Unsafe<JsonValue>(
  Type.Cyclic(
    {
      JsonValue: Type.Union([
        Type.Null(),
        Type.Boolean(),
        Type.Number(),
        Type.String(),
        Type.Array(Type.Ref("JsonValue")),
        Type.Record(Type.String(), Type.Ref("JsonValue")),
      ]),
    },
    "JsonValue",
  ),
)

/** Runtime validation for JSON objects at public contract boundaries. */
export const JsonObjectSchema = Type.Unsafe<JsonObject>(
  Type.Record(Type.String(), JsonValueSchema, {
    $id: "JsonObject",
  }),
)
