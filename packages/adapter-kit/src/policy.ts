import { AdapterKitError } from "./errors.js"
import type {
  ActiveHandoffDecision,
  AdapterOperationDescriptor,
  UncertainRecoveryDecision,
} from "./types.js"

export function uncertainRecoveryDecision(
  capabilities: AdapterOperationDescriptor["capabilities"],
): UncertainRecoveryDecision {
  switch (capabilities.duplicate_safety) {
    case "native_idempotency":
    case "keyed_uniqueness":
      return "retry_same_effect"
    case "reconciliation_only":
      if (capabilities.reconciliation !== "authoritative") {
        throw new AdapterKitError(
          "INVALID_CAPABILITY_COMBINATION",
          "Reconciliation-only safety requires authoritative reconciliation",
        )
      }
      return "reconcile"
    case "none":
      return "stop_uncertain"
  }
}

export function activeHandoffDecision(
  capabilities: AdapterOperationDescriptor["capabilities"],
): ActiveHandoffDecision {
  switch (capabilities.handoff_mode) {
    case "transferable":
      return "transfer"
    case "settle_first":
      return "settle"
    case "cancel_first":
      return "cancel"
    case "unsupported":
      return "block"
  }
}
