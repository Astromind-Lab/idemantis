import {
  DefinitionIdSchema,
  DefinitionVersionSchema,
  EventTypeSchema,
  JsonObjectSchema,
  JsonValueSchema,
  OperationCapabilitiesSchema,
  OperationNameSchema,
  StepIdSchema,
  canonicalJson,
  sha256Digest,
  type JsonObject,
} from "@idemantis/contracts"
import Schema from "typebox/schema"
import { MissionDefinitionError } from "./errors.js"
import { MissionManifestSchema } from "./schema.js"
import type {
  CompiledMission,
  EffectStateDefinition,
  MissionDefinition,
  MissionDefinitionIssue,
  MissionGraph,
  MissionGraphEdge,
  MissionGraphNode,
  MissionManifest,
  ObservationWaitStateDefinition,
  OperationDefinition,
  OperationManifest,
  ReconcileStateDefinition,
  RetryPolicyManifest,
  StateDefinition,
  StateManifest,
  StateName,
  ValueExpression,
} from "./types.js"

const stateNamePattern = /^[a-z][a-zA-Z0-9]*(?:[._-][a-zA-Z0-9]+)*$/
const adapterNamePattern = /^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/
const manifestValidator = Schema.Compile(MissionManifestSchema)

function issue(
  code: MissionDefinitionIssue["code"],
  path: string,
  message: string,
  resolution: string,
): MissionDefinitionIssue {
  return { code, path, message, resolution }
}

function schemaObject(value: unknown): JsonObject {
  if (!Schema.Check(JsonObjectSchema, value)) {
    throw new TypeError("Schema must contain only JSON values")
  }

  const serialized = JSON.stringify(value)

  if (serialized === undefined) {
    throw new TypeError("Schema is not JSON serializable")
  }

  const parsed = JSON.parse(serialized) as unknown

  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new TypeError("Schema must serialize to a JSON object")
  }

  return parsed as JsonObject
}

function hasOnlyKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const allowed = new Set(keys)
  return Object.keys(value).every((key) => allowed.has(key))
}

function validateExpression(
  value: unknown,
  path: string,
  issues: MissionDefinitionIssue[],
): value is ValueExpression {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    issues.push(
      issue(
        "INVALID_EXPRESSION",
        path,
        "Mission expressions must use the serializable expression builders",
        "Use fromInput, literal, or mapFields",
      ),
    )
    return false
  }

  const expression = value as Record<string, unknown>
  const { fields, kind, path: inputPath, value: literalValue } = expression

  if (
    kind === "input" &&
    hasOnlyKeys(expression, ["kind", "path"]) &&
    Array.isArray(inputPath) &&
    inputPath.length > 0 &&
    inputPath.every((segment) => typeof segment === "string" && segment.length > 0)
  ) {
    return true
  }

  if (
    kind === "literal" &&
    hasOnlyKeys(expression, ["kind", "value"]) &&
    Schema.Check(JsonValueSchema, literalValue)
  ) {
    return true
  }

  if (
    kind === "object" &&
    hasOnlyKeys(expression, ["kind", "fields"]) &&
    fields !== null &&
    typeof fields === "object" &&
    !Array.isArray(fields)
  ) {
    const mappedFields = fields as Record<string, unknown>
    let valid = true

    for (const [field, child] of Object.entries(mappedFields)) {
      if (field.length === 0) {
        issues.push(
          issue(
            "INVALID_EXPRESSION",
            `${path}.fields`,
            "Expression field names cannot be empty",
            "Give every mapped field a stable name",
          ),
        )
        valid = false
      } else if (!validateExpression(child, `${path}.fields.${field}`, issues)) {
        valid = false
      }
    }

    return valid
  }

  issues.push(
    issue(
      "INVALID_EXPRESSION",
      path,
      "Mission expressions must contain only a supported kind and its declared fields",
      "Use fromInput, literal, or mapFields without adding runtime values",
    ),
  )
  return false
}

function validateSchema(value: unknown): boolean {
  try {
    Schema.Compile(value as never)
    schemaObject(value)
    return true
  } catch {
    return false
  }
}

function validateOperation(
  operation: OperationDefinition,
  path: string,
  issues: MissionDefinitionIssue[],
): void {
  const fieldsAreValid =
    stateNamePattern.test(operation.key) &&
    adapterNamePattern.test(operation.adapter) &&
    Schema.Check(OperationNameSchema, operation.operation) &&
    Number.isSafeInteger(operation.schemaVersion) &&
    operation.schemaVersion >= 1 &&
    Schema.Check(OperationCapabilitiesSchema, operation.capabilities)

  if (
    !fieldsAreValid ||
    !validateSchema(operation.inputSchema) ||
    !validateSchema(operation.outputSchema)
  ) {
    issues.push(
      issue(
        "INVALID_OPERATION",
        path,
        `Operation ${operation.key} has an invalid descriptor or schema`,
        "Use stable names, a positive schema version, valid capabilities, and JSON schemas",
      ),
    )
  }
}

function validateRetry(
  stateName: string,
  state: EffectStateDefinition,
  issues: MissionDefinitionIssue[],
): void {
  if (state.retry.kind === "none") {
    return
  }

  const path = `states.${stateName}.retry`
  const boundsAreValid =
    Number.isSafeInteger(state.retry.maxAttempts) &&
    state.retry.maxAttempts >= 2 &&
    Number.isSafeInteger(state.retry.backoffMs) &&
    state.retry.backoffMs >= 0

  if (!boundsAreValid) {
    issues.push(
      issue(
        "UNSAFE_RETRY",
        path,
        "Retry attempts and backoff must be bounded non-negative integers",
        "Use at least two attempts and a non-negative millisecond backoff",
      ),
    )
    return
  }

  if (
    state.retry.kind === "safe" &&
    state.operation.capabilities.duplicate_safety !== "native_idempotency" &&
    state.operation.capabilities.duplicate_safety !== "keyed_uniqueness"
  ) {
    issues.push(
      issue(
        "UNSAFE_RETRY",
        path,
        `Operation ${state.operation.key} does not enforce duplicate suppression`,
        "Use native idempotency, keyed uniqueness, or reconcile before retry",
      ),
    )
  }

  if (
    state.retry.kind === "reconcile_before_retry" &&
    state.operation.capabilities.reconciliation !== "authoritative"
  ) {
    issues.push(
      issue(
        "RECONCILIATION_UNAVAILABLE",
        path,
        `Operation ${state.operation.key} cannot prove a prior effect was not applied`,
        "Bind an operation with authoritative reconciliation or disable automatic retry",
      ),
    )
  }
}

function supportsExecution(operation: OperationDefinition, owner: "runner" | "edge"): boolean {
  const location = operation.capabilities.execution_location
  return location === "either" || location === owner
}

function validateEffectState(
  stateName: string,
  state: EffectStateDefinition,
  issues: MissionDefinitionIssue[],
): void {
  const path = `states.${stateName}`
  validateOperation(state.operation, `${path}.operation`, issues)
  validateRetry(stateName, state, issues)
  validateExpression(state.input, `${path}.input`, issues)

  if (
    !Schema.Check(StepIdSchema, state.effectSlot) ||
    typeof state.required !== "boolean" ||
    typeof state.automaticTakeover !== "boolean" ||
    typeof state.activeHandoff !== "boolean"
  ) {
    issues.push(
      issue(
        "INVALID_STATE",
        path,
        "Effect state metadata is invalid",
        "Use a stable effect slot and explicit boolean safety settings",
      ),
    )
  }

  if (!supportsExecution(state.operation, state.execution)) {
    issues.push(
      issue(
        "EXECUTION_LOCATION_UNSUPPORTED",
        `${path}.execution`,
        `Operation ${state.operation.key} cannot execute on ${state.execution}`,
        "Choose a supported owner or bind an operation that can execute there",
      ),
    )
  }

  if (state.automaticTakeover && state.operation.capabilities.fencing_mode === "none") {
    issues.push(
      issue(
        "FENCING_REQUIRED",
        `${path}.automaticTakeover`,
        `Operation ${state.operation.key} cannot reject stale owners`,
        "Provide downstream fencing or require an explicit operator takeover",
      ),
    )
  }

  if (state.activeHandoff && state.operation.capabilities.handoff_mode !== "transferable") {
    issues.push(
      issue(
        "HANDOFF_UNSUPPORTED",
        `${path}.activeHandoff`,
        `Operation ${state.operation.key} cannot transfer an active effect`,
        "Settle or cancel the effect before handoff, or use a transferable operation",
      ),
    )
  }

  if (state.cancellation && !state.operation.capabilities.cancellation) {
    issues.push(
      issue(
        "CANCELLATION_UNSUPPORTED",
        `${path}.cancellation`,
        `Operation ${state.operation.key} does not support cancellation`,
        "Remove the cancellation route or bind an operation with cancellation support",
      ),
    )
  }

  if (state.cancellation && !state.cancellation.onCompletionDuringCancel) {
    issues.push(
      issue(
        "CANCELLATION_RACE_UNHANDLED",
        `${path}.cancellation.onCompletionDuringCancel`,
        "The effect may complete while cancellation is in flight",
        "Route completion during cancellation to acceptance, compensation, or intervention",
      ),
    )
  }
}

function validateObservationWait(
  stateName: string,
  state: ObservationWaitStateDefinition,
  states: Record<StateName, StateDefinition>,
  issues: MissionDefinitionIssue[],
): void {
  const path = `states.${stateName}`

  if (
    !Schema.Check(EventTypeSchema, state.observationType) ||
    !Number.isSafeInteger(state.schemaVersion) ||
    state.schemaVersion < 1
  ) {
    issues.push(
      issue(
        "INVALID_OBSERVATION",
        path,
        "Observation type or schema version is invalid",
        "Use a namespaced observation type and a positive schema version",
      ),
    )
  }

  if (!state.correlation) {
    issues.push(
      issue(
        "OBSERVATION_CORRELATION_REQUIRED",
        `${path}.correlation`,
        "Observation waits require an explicit correlation rule",
        "Correlate by an effect state or a serializable subject expression",
      ),
    )
  } else if (state.correlation.kind === "effect") {
    if (states[state.correlation.effectState]?.kind !== "effect") {
      issues.push(
        issue(
          "OBSERVATION_CORRELATION_INVALID",
          `${path}.correlation.effectState`,
          `State ${state.correlation.effectState} is not an effect state`,
          "Reference the effect whose outcome the observation proves",
        ),
      )
    }
  } else if (state.correlation.kind === "subject") {
    validateExpression(state.correlation.subject, `${path}.correlation.subject`, issues)
  } else {
    issues.push(
      issue(
        "OBSERVATION_CORRELATION_INVALID",
        `${path}.correlation`,
        "Observation correlation uses an unsupported strategy",
        "Correlate by an effect state or a serializable subject expression",
      ),
    )
  }

  if (
    !state.timeout ||
    !Number.isSafeInteger(state.timeout.afterMs) ||
    state.timeout.afterMs <= 0
  ) {
    issues.push(
      issue(
        "OBSERVATION_TIMEOUT_REQUIRED",
        `${path}.timeout`,
        "Observation waits require a positive bounded timeout",
        "Set a timeout and route it to reconciliation or an operator waitpoint",
      ),
    )
  }
}

function validateReconcileState(
  stateName: string,
  state: ReconcileStateDefinition,
  issues: MissionDefinitionIssue[],
): void {
  const path = `states.${stateName}`
  validateOperation(state.operation, `${path}.operation`, issues)
  validateExpression(state.subject, `${path}.subject`, issues)

  if (state.operation.capabilities.reconciliation === "none") {
    issues.push(
      issue(
        "RECONCILIATION_UNAVAILABLE",
        `${path}.operation`,
        `Operation ${state.operation.key} does not support reconciliation`,
        "Use an operation with authoritative or best-effort reconciliation",
      ),
    )
  }

  if (!supportsExecution(state.operation, state.execution)) {
    issues.push(
      issue(
        "EXECUTION_LOCATION_UNSUPPORTED",
        `${path}.execution`,
        `Operation ${state.operation.key} cannot execute on ${state.execution}`,
        "Choose a supported owner or bind an operation that can execute there",
      ),
    )
  }
}

function transitionTargets(state: StateDefinition): Array<[string, StateName]> {
  switch (state.kind) {
    case "effect": {
      const targets: Array<[string, StateName]> = [
        ["onSettled", state.onSettled],
        ["onFailed", state.onFailed],
        ["onUncertain", state.onUncertain],
      ]

      if (state.cancellation) {
        targets.push(["cancellation.onCancelled", state.cancellation.onCancelled])
        if (state.cancellation.onCompletionDuringCancel) {
          targets.push([
            "cancellation.onCompletionDuringCancel",
            state.cancellation.onCompletionDuringCancel,
          ])
        }
      }

      return targets
    }
    case "observation_wait":
      return [
        ["onObserved", state.onObserved],
        ...(state.timeout
          ? [["timeout.onTimeout", state.timeout.onTimeout] as [string, string]]
          : []),
      ]
    case "reconcile":
      return [
        ["onConfirmedCompleted", state.onConfirmedCompleted],
        ["onConfirmedNotApplied", state.onConfirmedNotApplied],
        ["onConfirmedInProgress", state.onConfirmedInProgress],
        ["onConflictingState", state.onConflictingState],
        ["onUnknown", state.onUnknown],
      ]
    case "final":
      return []
  }

  return []
}

function validateGraph(definition: MissionDefinition): MissionDefinitionIssue[] {
  const issues: MissionDefinitionIssue[] = []
  const stateNames = Object.keys(definition.states)

  if (!Schema.Check(DefinitionIdSchema, definition.id)) {
    issues.push(
      issue(
        "INVALID_DEFINITION_ID",
        "id",
        "Mission ID is not a stable definition identifier",
        "Use a lowercase dot, dash, or underscore separated identifier",
      ),
    )
  }

  if (!Schema.Check(DefinitionVersionSchema, definition.version)) {
    issues.push(
      issue(
        "INVALID_DEFINITION_VERSION",
        "version",
        "Mission version is not valid semantic versioning",
        "Use a version such as 1.0.0",
      ),
    )
  }

  if (!validateSchema(definition.input)) {
    issues.push(
      issue(
        "INVALID_INPUT_SCHEMA",
        "input",
        "Mission input is not a compilable JSON schema",
        "Provide a TypeBox schema that serializes to JSON",
      ),
    )
  }

  if (!Object.hasOwn(definition.states, definition.initial)) {
    issues.push(
      issue(
        "INITIAL_STATE_MISSING",
        "initial",
        `Initial state ${definition.initial} does not exist`,
        "Reference one of the declared states",
      ),
    )
  }

  for (const stateName of stateNames) {
    const state = definition.states[stateName]

    if (!stateNamePattern.test(stateName)) {
      issues.push(
        issue(
          "INVALID_STATE_NAME",
          `states.${stateName}`,
          `State name ${stateName} is not stable`,
          "Use a letter-prefixed name containing letters, digits, dots, dashes, or underscores",
        ),
      )
    }

    if (!state) {
      continue
    }

    if (state.kind === "effect") {
      validateEffectState(stateName, state, issues)
    } else if (state.kind === "observation_wait") {
      validateObservationWait(stateName, state, definition.states, issues)
    } else if (state.kind === "reconcile") {
      validateReconcileState(stateName, state, issues)
    } else if (
      state.kind !== "final" ||
      !["completed", "cancelled", "compensated", "failed", "unresolved"].includes(state.outcome)
    ) {
      issues.push(
        issue(
          "INVALID_STATE",
          `states.${stateName}`,
          `State ${stateName} has an invalid kind or terminal outcome`,
          "Use effectState, waitForObservation, reconcileState, or finalState",
        ),
      )
    }

    for (const [field, target] of transitionTargets(state)) {
      if (!Object.hasOwn(definition.states, target)) {
        issues.push(
          issue(
            "TRANSITION_TARGET_MISSING",
            `states.${stateName}.${field}`,
            `Transition target ${target} does not exist`,
            "Reference a declared state",
          ),
        )
      }
    }
  }

  if (Object.hasOwn(definition.states, definition.initial)) {
    const reachable = new Set<string>()
    const pending = [definition.initial]

    while (pending.length > 0) {
      const current = pending.pop()
      if (!current || reachable.has(current)) {
        continue
      }

      reachable.add(current)
      const state = definition.states[current]
      if (!state) {
        continue
      }

      for (const [, target] of transitionTargets(state)) {
        if (Object.hasOwn(definition.states, target)) {
          pending.push(target)
        }
      }
    }

    for (const stateName of stateNames) {
      if (!reachable.has(stateName)) {
        issues.push(
          issue(
            "UNREACHABLE_STATE",
            `states.${stateName}`,
            `State ${stateName} is unreachable from ${definition.initial}`,
            "Add a transition to the state or remove it",
          ),
        )
      }
    }
  }

  return issues.sort((left, right) =>
    left.path === right.path
      ? left.code.localeCompare(right.code)
      : left.path.localeCompare(right.path),
  )
}

function operationManifest(operation: OperationDefinition): OperationManifest {
  return {
    key: operation.key,
    adapter: operation.adapter,
    operation: operation.operation,
    operation_schema_version: operation.schemaVersion,
    input_schema: schemaObject(operation.inputSchema),
    output_schema: schemaObject(operation.outputSchema),
    effect_class: operation.effectClass,
    consequence: operation.consequence,
    capabilities: operation.capabilities,
  }
}

function retryManifest(state: EffectStateDefinition): RetryPolicyManifest {
  if (state.retry.kind === "none") {
    return { kind: "none" }
  }

  return {
    kind: state.retry.kind,
    max_attempts: state.retry.maxAttempts,
    backoff_ms: state.retry.backoffMs,
  }
}

function stateManifest(state: StateDefinition): StateManifest {
  switch (state.kind) {
    case "effect":
      return {
        kind: "effect",
        operation: state.operation.key,
        effect_slot: state.effectSlot,
        input: state.input,
        required: state.required,
        retry: retryManifest(state),
        execution: state.execution,
        automatic_takeover: state.automaticTakeover,
        active_handoff: state.activeHandoff,
        ...(state.cancellation
          ? {
              cancellation: {
                on_cancelled: state.cancellation.onCancelled,
                on_completion_during_cancel: state.cancellation.onCompletionDuringCancel as string,
              },
            }
          : {}),
        on_settled: state.onSettled,
        on_failed: state.onFailed,
        on_uncertain: state.onUncertain,
      }
    case "observation_wait":
      return {
        kind: "observation_wait",
        observation_type: state.observationType,
        schema_version: state.schemaVersion,
        correlation: state.correlation as NonNullable<typeof state.correlation>,
        timeout: {
          after_ms: state.timeout?.afterMs as number,
          on_timeout: state.timeout?.onTimeout as string,
        },
        on_observed: state.onObserved,
      }
    case "reconcile":
      return {
        kind: "reconcile",
        operation: state.operation.key,
        subject: state.subject,
        execution: state.execution,
        on_confirmed_completed: state.onConfirmedCompleted,
        on_confirmed_not_applied: state.onConfirmedNotApplied,
        on_confirmed_in_progress: state.onConfirmedInProgress,
        on_conflicting_state: state.onConflictingState,
        on_unknown: state.onUnknown,
      }
    case "final":
      return { kind: "final", outcome: state.outcome }
  }
}

function graphNode(stateName: StateName, state: StateDefinition): MissionGraphNode {
  switch (state.kind) {
    case "effect":
    case "reconcile":
      return { id: stateName, kind: state.kind, operation: state.operation.key }
    case "observation_wait":
      return { id: stateName, kind: state.kind }
    case "final":
      return { id: stateName, kind: state.kind, outcome: state.outcome }
  }
}

function graphEdges(stateName: StateName, state: StateDefinition): MissionGraphEdge[] {
  switch (state.kind) {
    case "effect": {
      const edges: MissionGraphEdge[] = [
        { from: stateName, to: state.onSettled, trigger: "effect.settled" },
        { from: stateName, to: state.onFailed, trigger: "effect.failed" },
        { from: stateName, to: state.onUncertain, trigger: "effect.uncertain" },
      ]

      if (state.cancellation) {
        edges.push({
          from: stateName,
          to: state.cancellation.onCancelled,
          trigger: "effect.cancelled",
        })
        edges.push({
          from: stateName,
          to: state.cancellation.onCompletionDuringCancel as string,
          trigger: "effect.completed_during_cancel",
        })
      }

      return edges
    }
    case "observation_wait":
      return [
        { from: stateName, to: state.onObserved, trigger: "observation.received" },
        {
          from: stateName,
          to: state.timeout?.onTimeout as string,
          trigger: "observation.timeout",
        },
      ]
    case "reconcile":
      return [
        {
          from: stateName,
          to: state.onConfirmedCompleted,
          trigger: "reconcile.confirmed_completed",
        },
        {
          from: stateName,
          to: state.onConfirmedNotApplied,
          trigger: "reconcile.confirmed_not_applied",
        },
        {
          from: stateName,
          to: state.onConfirmedInProgress,
          trigger: "reconcile.confirmed_in_progress",
        },
        {
          from: stateName,
          to: state.onConflictingState,
          trigger: "reconcile.conflicting_state",
        },
        { from: stateName, to: state.onUnknown, trigger: "reconcile.unknown" },
      ]
    case "final":
      return []
  }
}

function buildGraph(states: Record<StateName, StateDefinition>): MissionGraph {
  const nodes: MissionGraphNode[] = []
  const edges: MissionGraphEdge[] = []

  for (const stateName of Object.keys(states).sort()) {
    const state = states[stateName] as StateDefinition
    nodes.push(graphNode(stateName, state))
    edges.push(...graphEdges(stateName, state))
  }

  edges.sort((left, right) => {
    const leftKey = `${left.from}\u0000${left.trigger}\u0000${left.to}`
    const rightKey = `${right.from}\u0000${right.trigger}\u0000${right.to}`
    return leftKey.localeCompare(rightKey)
  })

  return { nodes, edges }
}

function buildManifest(definition: MissionDefinition): MissionManifest {
  const operations = new Map<string, OperationManifest>()
  const states: Record<string, StateManifest> = {}

  for (const stateName of Object.keys(definition.states).sort()) {
    const state = definition.states[stateName] as StateDefinition
    states[stateName] = stateManifest(state)

    if (state.kind === "effect" || state.kind === "reconcile") {
      const manifest = operationManifest(state.operation)
      const existing = operations.get(manifest.key)

      if (
        existing &&
        canonicalJson(existing as unknown as JsonObject) !==
          canonicalJson(manifest as unknown as JsonObject)
      ) {
        throw new MissionDefinitionError(definition.id, [
          issue(
            "DUPLICATE_OPERATION_KEY",
            `states.${stateName}.operation.key`,
            `Operation key ${manifest.key} identifies different descriptors`,
            "Give distinct operation descriptors distinct stable keys",
          ),
        ])
      }

      operations.set(manifest.key, manifest)
    }
  }

  return {
    manifest_schema_version: 1,
    minimum_interpreter_version: "0.1.0",
    definition_id: definition.id,
    definition_version: definition.version,
    input_schema: schemaObject(definition.input),
    initial: definition.initial,
    operations: Object.fromEntries(
      [...operations.entries()].sort(([left], [right]) => left.localeCompare(right)),
    ),
    states,
    graph: buildGraph(definition.states),
  }
}

function deepFreeze<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) {
      deepFreeze(child)
    }
    Object.freeze(value)
  }

  return value
}

/** Validates a state graph and emits its immutable canonical manifest. */
export function defineMission<const SchemaType extends MissionDefinition["input"]>(
  definition: MissionDefinition<SchemaType>,
): CompiledMission<SchemaType> {
  const issues = validateGraph(definition)

  if (issues.length > 0) {
    throw new MissionDefinitionError(definition.id, issues)
  }

  const manifest = schemaObject(buildManifest(definition)) as unknown as MissionManifest

  if (!manifestValidator.Check(manifest)) {
    throw new TypeError("Compiled mission does not satisfy MissionManifestSchema")
  }

  const canonicalManifest = canonicalJson(manifest as unknown as JsonObject)
  const definitionDigest = sha256Digest(manifest as unknown as JsonObject)
  const inputSchema = schemaObject(definition.input) as unknown as SchemaType

  return deepFreeze({
    inputSchema,
    manifest,
    canonicalManifest,
    definitionDigest,
  })
}
