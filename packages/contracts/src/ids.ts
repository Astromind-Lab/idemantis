import Type from "typebox"

const ulid = "[0-9A-HJKMNP-TV-Z]{26}"

export const DefinitionIdSchema = Type.String({
  $id: "DefinitionId",
  minLength: 1,
  maxLength: 128,
  pattern: "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$",
})

export const DefinitionVersionSchema = Type.String({
  $id: "DefinitionVersion",
  minLength: 5,
  maxLength: 128,
  pattern:
    "^(?:0|[1-9][0-9]*)\\.(?:0|[1-9][0-9]*)\\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?(?:\\+[0-9A-Za-z-]+(?:\\.[0-9A-Za-z-]+)*)?$",
})

/** A lowercase SHA-256 digest with an explicit algorithm prefix. */
export const DigestSchema = Type.String({
  $id: "Digest",
  pattern: "^sha256:[a-f0-9]{64}$",
})

export const RunIdSchema = Type.String({ $id: "RunId", pattern: `^run_${ulid}$` })
export const EventIdSchema = Type.String({ $id: "EventId", pattern: `^evt_${ulid}$` })
export const CommandIdSchema = Type.String({ $id: "CommandId", pattern: `^cmd_${ulid}$` })
export const EffectIdSchema = Type.String({ $id: "EffectId", pattern: `^eff_${ulid}$` })
export const AttemptIdSchema = Type.String({ $id: "AttemptId", pattern: `^att_${ulid}$` })
export const ObservationIdSchema = Type.String({
  $id: "ObservationId",
  pattern: `^obs_${ulid}$`,
})
export const TimerIdSchema = Type.String({ $id: "TimerId", pattern: `^tmr_${ulid}$` })
export const LeaseIdSchema = Type.String({ $id: "LeaseId", pattern: `^lse_${ulid}$` })
export const HandoffIdSchema = Type.String({ $id: "HandoffId", pattern: `^hnd_${ulid}$` })
export const InterventionIdSchema = Type.String({
  $id: "InterventionId",
  pattern: `^int_${ulid}$`,
})
export const AdapterInstanceIdSchema = Type.String({
  $id: "AdapterInstanceId",
  pattern: `^adp_${ulid}$`,
})
export const StateInstanceIdSchema = Type.String({
  $id: "StateInstanceId",
  pattern: `^sti_${ulid}$`,
})
export const MergeIdSchema = Type.String({ $id: "MergeId", pattern: `^mrg_${ulid}$` })

export const StepIdSchema = Type.String({
  $id: "StepId",
  minLength: 1,
  maxLength: 128,
  pattern: "^[a-z][a-zA-Z0-9]*(?:[._-][a-zA-Z0-9]+)*$",
})

export const OperationNameSchema = Type.String({
  $id: "OperationName",
  minLength: 1,
  maxLength: 128,
  pattern: "^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$",
})

export const EventTypeSchema = Type.String({
  $id: "EventType",
  minLength: 3,
  maxLength: 128,
  pattern: "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)+$",
})

export const SourceIdSchema = Type.String({
  $id: "SourceId",
  minLength: 1,
  maxLength: 128,
  pattern: "^[a-zA-Z0-9](?:[a-zA-Z0-9._:-]*[a-zA-Z0-9])?$",
})

export const CorrelationIdSchema = Type.String({
  $id: "CorrelationId",
  minLength: 1,
  maxLength: 256,
  pattern: "^[a-zA-Z0-9](?:[a-zA-Z0-9._:-]*[a-zA-Z0-9])?$",
})

export const ClientRequestIdSchema = Type.String({
  $id: "ClientRequestId",
  minLength: 16,
  maxLength: 128,
  pattern: "^[a-zA-Z0-9](?:[a-zA-Z0-9._:-]*[a-zA-Z0-9])?$",
})

export type DefinitionId = Type.Static<typeof DefinitionIdSchema>
export type DefinitionVersion = Type.Static<typeof DefinitionVersionSchema>
export type Digest = Type.Static<typeof DigestSchema>
export type RunId = Type.Static<typeof RunIdSchema>
export type EventId = Type.Static<typeof EventIdSchema>
export type CommandId = Type.Static<typeof CommandIdSchema>
export type EffectId = Type.Static<typeof EffectIdSchema>
export type AttemptId = Type.Static<typeof AttemptIdSchema>
export type ObservationId = Type.Static<typeof ObservationIdSchema>
export type TimerId = Type.Static<typeof TimerIdSchema>
export type LeaseId = Type.Static<typeof LeaseIdSchema>
export type HandoffId = Type.Static<typeof HandoffIdSchema>
export type InterventionId = Type.Static<typeof InterventionIdSchema>
export type AdapterInstanceId = Type.Static<typeof AdapterInstanceIdSchema>
export type StateInstanceId = Type.Static<typeof StateInstanceIdSchema>
export type MergeId = Type.Static<typeof MergeIdSchema>
export type StepId = Type.Static<typeof StepIdSchema>
export type OperationName = Type.Static<typeof OperationNameSchema>
export type EventType = Type.Static<typeof EventTypeSchema>
export type SourceId = Type.Static<typeof SourceIdSchema>
export type CorrelationId = Type.Static<typeof CorrelationIdSchema>
export type ClientRequestId = Type.Static<typeof ClientRequestIdSchema>
