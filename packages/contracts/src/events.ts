import Type from "typebox"
import {
  CorrelationIdSchema,
  EventIdSchema,
  EventTypeSchema,
  RunIdSchema,
  SourceIdSchema,
} from "./ids.js"
import { JsonObjectSchema } from "./json.js"
import { TimestampSchema } from "./time.js"

export const EventSourceKindSchema = Type.Union(
  [
    Type.Literal("runner"),
    Type.Literal("edge"),
    Type.Literal("adapter"),
    Type.Literal("operator"),
    Type.Literal("timer"),
    Type.Literal("system"),
  ],
  { $id: "EventSourceKind" },
)

export const EventSourceSchema = Type.Object(
  {
    kind: EventSourceKindSchema,
    source_id: SourceIdSchema,
  },
  { $id: "EventSource", additionalProperties: false },
)

const proposalFields = {
  event_id: EventIdSchema,
  run_id: RunIdSchema,
  type: EventTypeSchema,
  schema_version: Type.Integer({ minimum: 1 }),
  source: EventSourceSchema,
  causation_id: Type.Optional(EventIdSchema),
  correlation_id: Type.Optional(CorrelationIdSchema),
  ownership_epoch: Type.Integer({ minimum: 0 }),
  logical_at: Type.Optional(TimestampSchema),
  observed_at: Type.Optional(TimestampSchema),
  payload: JsonObjectSchema,
}

/** Validates an event before the event store assigns ordering and time. */
export const ProposedEventSchema = Type.Object(proposalFields, {
  $id: "ProposedEvent",
  additionalProperties: false,
})

/** Validates an immutable event accepted into a run's ordered history. */
export const AcceptedEventSchema = Type.Object(
  {
    ...proposalFields,
    sequence: Type.Integer({ minimum: 1 }),
    recorded_at: TimestampSchema,
  },
  { $id: "AcceptedEvent", additionalProperties: false },
)

/** Confirms the durable sequence assigned to an accepted proposal. */
export const EventAcceptanceSchema = Type.Object(
  {
    event_id: EventIdSchema,
    run_id: RunIdSchema,
    proposal_digest: Type.String({ pattern: "^sha256:[a-f0-9]{64}$" }),
    sequence: Type.Integer({ minimum: 1 }),
    recorded_at: TimestampSchema,
  },
  { $id: "EventAcceptance", additionalProperties: false },
)

export type EventSourceKind = Type.Static<typeof EventSourceKindSchema>
export type EventSource = Type.Static<typeof EventSourceSchema>
export type ProposedEvent = Type.Static<typeof ProposedEventSchema>
export type AcceptedEvent = Type.Static<typeof AcceptedEventSchema>
export type EventAcceptance = Type.Static<typeof EventAcceptanceSchema>
