import Type from "typebox"

/** A UTC RFC 3339 timestamp with optional fractional seconds. */
export const TimestampSchema = Type.String({
  $id: "Timestamp",
  pattern:
    "^[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\\.[0-9]{1,9})?Z$",
})

/** A non-negative millisecond duration within JavaScript's safe integer range. */
export const DurationMsSchema = Type.Integer({
  $id: "DurationMs",
  minimum: 0,
  maximum: Number.MAX_SAFE_INTEGER,
})

export type Timestamp = Type.Static<typeof TimestampSchema>
export type DurationMs = Type.Static<typeof DurationMsSchema>
