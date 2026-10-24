import { canonicalJson, type JsonObject, type JsonValue } from "@idemantis/contracts"
import Schema from "typebox/schema"
import { TransportError } from "./errors.js"

const commonKeywords = new Set(["type", "enum", "title", "description", "default"])
const objectKeywords = new Set([
  ...commonKeywords,
  "properties",
  "required",
  "additionalProperties",
  "minProperties",
  "maxProperties",
])
const stringKeywords = new Set([...commonKeywords, "minLength", "maxLength", "format"])
const numberKeywords = new Set([
  ...commonKeywords,
  "minimum",
  "maximum",
  "exclusiveMinimum",
  "exclusiveMaximum",
])
const arrayKeywords = new Set([...commonKeywords, "items", "minItems", "maxItems"])

interface SchemaCounter {
  properties: number
}

interface InterventionSchemaNode {
  [key: string]: JsonValue | undefined
  type?: JsonValue
  enum?: JsonValue
  properties?: JsonValue
  required?: JsonValue
  additionalProperties?: JsonValue
  minProperties?: JsonValue
  maxProperties?: JsonValue
  items?: JsonValue
  minItems?: JsonValue
  maxItems?: JsonValue
  minLength?: JsonValue
  maxLength?: JsonValue
  format?: JsonValue
}

function incompatible(message: string): never {
  throw new TransportError("INCOMPATIBLE_SCHEMA", message, 422)
}

function assertKnownKeywords(
  value: Record<string, unknown>,
  allowed: Set<string>,
  path: string,
): void {
  for (const keyword of Object.keys(value)) {
    if (!allowed.has(keyword)) {
      incompatible(`${path} contains unsupported keyword ${keyword}`)
    }
  }
}

function assertEnum(value: unknown, path: string): void {
  if (value === undefined) {
    return
  }
  if (!Array.isArray(value) || value.length < 1 || value.length > 50) {
    incompatible(`${path}.enum must contain from 1 through 50 scalar values`)
  }
  if (value.some((entry) => entry !== null && typeof entry === "object")) {
    incompatible(`${path}.enum may contain only scalar values`)
  }
}

function finiteNumber(value: unknown, keyword: string, path: string): void {
  if (value !== undefined && (typeof value !== "number" || !Number.isFinite(value))) {
    incompatible(`${path}.${keyword} must be finite`)
  }
}

function boundedInteger(
  value: unknown,
  keyword: string,
  path: string,
  maximum: number,
  required: boolean,
): void {
  if (value === undefined && !required) {
    return
  }
  if (!Number.isSafeInteger(value) || (value as number) < 0 || (value as number) > maximum) {
    incompatible(`${path}.${keyword} must be a safe integer no greater than ${maximum}`)
  }
}

function validateNode(value: unknown, path: string, depth: number, counter: SchemaCounter): void {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    incompatible(`${path} must be a schema object`)
  }
  const node = value as InterventionSchemaNode
  const type = node.type
  if (typeof type !== "string") {
    incompatible(`${path}.type must name one supported JSON type`)
  }
  assertEnum(node.enum, path)

  if (type === "object") {
    if (depth > 4) {
      incompatible(`${path} exceeds the maximum object depth of four`)
    }
    assertKnownKeywords(node, objectKeywords, path)
    if (node.additionalProperties !== false) {
      incompatible(`${path}.additionalProperties must be false`)
    }
    if (
      node.properties === null ||
      typeof node.properties !== "object" ||
      Array.isArray(node.properties)
    ) {
      incompatible(`${path}.properties must be an object`)
    }
    const properties = node.properties as JsonObject
    const entries = Object.entries(properties)
    if (entries.length > 32) {
      incompatible(`${path} declares more than 32 properties`)
    }
    counter.properties += entries.length
    if (counter.properties > 64) {
      incompatible("Intervention schema declares more than 64 properties")
    }
    const required = node.required
    if (
      required !== undefined &&
      (!Array.isArray(required) ||
        required.some((entry) => typeof entry !== "string" || !(entry in properties)))
    ) {
      incompatible(`${path}.required must reference declared properties`)
    }
    boundedInteger(node.minProperties, "minProperties", path, 32, false)
    boundedInteger(node.maxProperties, "maxProperties", path, 32, true)
    if ((node.maxProperties as number) < entries.length) {
      incompatible(`${path}.maxProperties cannot be smaller than the declared properties`)
    }
    for (const [name, child] of entries) {
      validateNode(child, `${path}.properties.${name}`, depth + 1, counter)
    }
    return
  }

  if (type === "array") {
    assertKnownKeywords(node, arrayKeywords, path)
    boundedInteger(node.maxItems, "maxItems", path, 50, true)
    boundedInteger(node.minItems, "minItems", path, 50, false)
    validateNode(node.items, `${path}.items`, depth, counter)
    return
  }

  if (type === "string") {
    assertKnownKeywords(node, stringKeywords, path)
    if (
      !Number.isSafeInteger(node.maxLength) ||
      (node.maxLength as number) < 0 ||
      (node.maxLength as number) > 4096
    ) {
      incompatible(`${path}.maxLength must be a safe integer no greater than 4096`)
    }
    boundedInteger(node.minLength, "minLength", path, 4096, false)
    if (node.format !== undefined && node.format !== "date-time") {
      incompatible(`${path}.format must be date-time when present`)
    }
    return
  }

  if (type === "integer" || type === "number") {
    assertKnownKeywords(node, numberKeywords, path)
    for (const keyword of ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"]) {
      finiteNumber(node[keyword], keyword, path)
    }
    return
  }

  if (type === "boolean") {
    assertKnownKeywords(node, commonKeywords, path)
    return
  }

  incompatible(`${path}.type ${type} is not supported`)
}

export function validateInterventionSchema(value: JsonObject): void {
  validateNode(value, "schema", 1, { properties: 0 })
  if ((value as InterventionSchemaNode).type !== "object") {
    incompatible("Intervention schema root must be an object")
  }
  try {
    Schema.Compile(value as never)
  } catch {
    incompatible("Intervention schema cannot be compiled")
  }
}

export function validateInterventionInput(schema: JsonObject, input: JsonObject): void {
  validateInterventionSchema(schema)
  if (!Schema.Check(schema as never, input)) {
    throw new TransportError("INVALID_REQUEST", "Intervention input does not match its schema", 422)
  }
  if (Buffer.byteLength(canonicalJson(input), "utf8") > 64 * 1024) {
    throw new TransportError("INVALID_REQUEST", "Intervention input exceeds 64KiB", 413)
  }
}
