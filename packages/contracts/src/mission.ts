import Type from "typebox"
import { DefinitionIdSchema, DefinitionVersionSchema, DigestSchema, RunIdSchema } from "./ids.js"
import { JsonObjectSchema } from "./json.js"
import { TimestampSchema } from "./time.js"

export const MissionStatusSchema = Type.Union(
  [
    Type.Literal("created"),
    Type.Literal("admitted"),
    Type.Literal("ready"),
    Type.Literal("running"),
    Type.Literal("waiting"),
    Type.Literal("reconciling"),
    Type.Literal("paused"),
    Type.Literal("compensating"),
    Type.Literal("completed"),
    Type.Literal("cancelled"),
    Type.Literal("compensated"),
    Type.Literal("failed"),
    Type.Literal("unresolved"),
  ],
  { $id: "MissionStatus" },
)

export const TerminalMissionStatusSchema = Type.Union(
  [
    Type.Literal("completed"),
    Type.Literal("cancelled"),
    Type.Literal("compensated"),
    Type.Literal("failed"),
    Type.Literal("unresolved"),
  ],
  { $id: "TerminalMissionStatus" },
)

/** Pins a mission run to one immutable definition and interpreter floor. */
export const MissionDefinitionReferenceSchema = Type.Object(
  {
    definition_id: DefinitionIdSchema,
    definition_version: DefinitionVersionSchema,
    definition_digest: DigestSchema,
    manifest_schema_version: Type.Integer({ minimum: 1 }),
    minimum_interpreter_version: DefinitionVersionSchema,
  },
  { $id: "MissionDefinitionReference", additionalProperties: false },
)

/** Represents the durable, replayable state of one mission execution. */
export const MissionRunSchema = Type.Object(
  {
    run_id: RunIdSchema,
    definition: MissionDefinitionReferenceSchema,
    execution_binding_digest: DigestSchema,
    input: JsonObjectSchema,
    status: MissionStatusSchema,
    current_sequence: Type.Integer({ minimum: 0 }),
    ownership_epoch: Type.Integer({ minimum: 0 }),
    created_at: TimestampSchema,
    admitted_at: Type.Optional(TimestampSchema),
    terminal_at: Type.Optional(TimestampSchema),
    outcome_challenged: Type.Boolean(),
  },
  { $id: "MissionRun", additionalProperties: false },
)

/** Records a challenge to a mission's reported terminal outcome. */
export const OutcomeChallengeSchema = Type.Object(
  {
    run_id: RunIdSchema,
    challenge_id: Type.String({ pattern: "^chl_[0-9A-HJKMNP-TV-Z]{26}$" }),
    terminal_status: TerminalMissionStatusSchema,
    evidence_digest: DigestSchema,
    reason: Type.String({ minLength: 1, maxLength: 512 }),
    opened_at: TimestampSchema,
    resolved_at: Type.Optional(TimestampSchema),
    resolution: Type.Optional(Type.String({ minLength: 1, maxLength: 512 })),
  },
  { $id: "OutcomeChallenge", additionalProperties: false },
)

export type MissionStatus = Type.Static<typeof MissionStatusSchema>
export type TerminalMissionStatus = Type.Static<typeof TerminalMissionStatusSchema>
export type MissionDefinitionReference = Type.Static<typeof MissionDefinitionReferenceSchema>
export type MissionRun = Type.Static<typeof MissionRunSchema>
export type OutcomeChallenge = Type.Static<typeof OutcomeChallengeSchema>
