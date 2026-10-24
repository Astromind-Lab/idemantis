import type { MissionRuntimeDefinition } from "@idemantis/engine"
import { defineMission, executionBindingDigest, finalState } from "@idemantis/sdk"
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
