import {
  sha256Digest,
  type Digest,
  type EdgeDelegation,
  type JsonObject,
} from "@idemantis/contracts"

export function edgeDelegationDigest(delegation: EdgeDelegation): Digest {
  const { delegation_digest: _delegationDigest, ...lease } = delegation.lease
  return sha256Digest({ ...delegation, lease } as unknown as JsonObject)
}
