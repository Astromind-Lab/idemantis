import type { MissionRuntimeDefinition } from "@idemantis/engine"
import { defineMission, executionBindingDigest, finalState, waitForOperator } from "@idemantis/sdk"
import Type from "typebox"

const verification = defineMission({
  id: "operations.verification",
  version: "1.0.0",
  input: Type.Object(
    {
      subject: Type.String({ minLength: 1, maxLength: 128 }),
      summary: Type.String({ minLength: 1, maxLength: 512 }),
    },
    { additionalProperties: false },
  ),
  initial: "verified",
  states: {
    verified: finalState("completed"),
  },
})

export const VerificationRuntime: MissionRuntimeDefinition = {
  manifest: verification.manifest,
  definition_digest: verification.definitionDigest,
  execution_binding_digest: executionBindingDigest(verification, []),
}

const intervention = defineMission({
  id: "operations.intervention",
  version: "1.0.0",
  input: Type.Object(
    {
      subject: Type.String({ minLength: 1, maxLength: 128 }),
    },
    { additionalProperties: false },
  ),
  initial: "review",
  states: {
    review: waitForOperator({
      reason: "Release requires an operator decision",
      actions: [
        {
          code: "approve_release",
          consequence: "irreversible",
          confirmationRequired: true,
          input: Type.Object(
            { evidence: Type.String({ minLength: 3, maxLength: 512 }) },
            { additionalProperties: false, maxProperties: 1 },
          ),
          presentationHints: [
            { path: "evidence", control: "textarea", label: "Approval evidence", order: 0 },
          ],
          evidenceRequired: true,
          reasonRequired: true,
          expectedOutcomeLabel: "Mission completes with the reviewed release",
          onAccepted: "complete",
        },
      ],
    }),
    complete: finalState("completed"),
  },
})

export const InterventionRuntime: MissionRuntimeDefinition = {
  manifest: intervention.manifest,
  definition_digest: intervention.definitionDigest,
  execution_binding_digest: executionBindingDigest(intervention, []),
}
