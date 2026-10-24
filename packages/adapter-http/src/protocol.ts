import {
  AdapterCapabilityManifestSchema,
  AdapterInvocationIdentitySchema,
} from "@idemantis/adapter-kit"
import {
  AdapterInstanceIdSchema,
  JsonValueSchema,
  StepIdSchema,
  TimestampSchema,
} from "@idemantis/contracts"
import Type, { type Static } from "typebox"

export const HttpAdapterProtocolVersion = 1

export const HttpAdapterActions = ["issue", "reconcile", "cancel", "compensate", "fence"] as const

export type HttpAdapterAction = (typeof HttpAdapterActions)[number]

export const HttpAdapterManifestResponseSchema = Type.Object(
  {
    protocol_version: Type.Literal(HttpAdapterProtocolVersion),
    manifest: AdapterCapabilityManifestSchema,
  },
  { $id: "HttpAdapterManifestResponse", additionalProperties: false },
)

export const HttpAdapterIssueRequestSchema = Type.Object(
  {
    protocol_version: Type.Literal(HttpAdapterProtocolVersion),
    context: AdapterInvocationIdentitySchema,
    input: JsonValueSchema,
  },
  { $id: "HttpAdapterIssueRequest", additionalProperties: false },
)

export const HttpAdapterSubjectRequestSchema = Type.Object(
  {
    protocol_version: Type.Literal(HttpAdapterProtocolVersion),
    context: AdapterInvocationIdentitySchema,
    subject: JsonValueSchema,
  },
  { $id: "HttpAdapterSubjectRequest", additionalProperties: false },
)

export const HttpAdapterFenceRequestSchema = Type.Object(
  {
    protocol_version: Type.Literal(HttpAdapterProtocolVersion),
    context: Type.Object(
      {
        adapter_instance_id: AdapterInstanceIdSchema,
        operation_key: StepIdSchema,
        scope_reference: Type.String({ minLength: 1, maxLength: 256 }),
        ownership_epoch: Type.Integer({ minimum: 0 }),
        requested_at: TimestampSchema,
      },
      { additionalProperties: false },
    ),
  },
  { $id: "HttpAdapterFenceRequest", additionalProperties: false },
)

export type HttpAdapterManifestResponse = Static<typeof HttpAdapterManifestResponseSchema>
export type HttpAdapterIssueRequest = Static<typeof HttpAdapterIssueRequestSchema>
export type HttpAdapterSubjectRequest = Static<typeof HttpAdapterSubjectRequestSchema>
export type HttpAdapterFenceRequest = Static<typeof HttpAdapterFenceRequestSchema>

export function operationPath(operationKey: string, action: HttpAdapterAction): string {
  return `/operations/${encodeURIComponent(operationKey)}/${action}`
}
