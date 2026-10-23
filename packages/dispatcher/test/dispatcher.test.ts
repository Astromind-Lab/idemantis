import {
  AttemptIdSchema,
  EventIdSchema,
  type AttemptId,
  type EventId,
  type OperationCapabilities,
  type ProposedEvent,
} from "@idemantis/contracts"
import type { MissionRuntimeDefinition } from "@idemantis/engine"
import { SqliteJournal } from "@idemantis/journal-sqlite"
import {
  defineMission,
  defineOperation,
  effectState,
  finalState,
  fromInput,
  mapFields,
  reconcileState,
  safeRetry,
  waitForObservation,
} from "@idemantis/sdk"
import { chmodSync, mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import Type from "typebox"
import Schema from "typebox/schema"
import { afterEach, describe, expect, it, vi } from "vitest"
import {
  CommandDispatcher,
  SecureIdentityFactory,
  TimerDispatcher,
  VirtualClock,
  type IdentityFactory,
} from "../src/index.js"

const capabilities = {
  duplicate_safety: "native_idempotency",
  reconciliation: "authoritative",
  fencing_mode: "application_epoch",
  fencing_scope: "subject",
  handoff_mode: "settle_first",
  execution_location: "either",
  cancellation: false,
  compensation: false,
  observation_stream: true,
} satisfies OperationCapabilities

const dispatch = defineOperation({
  key: "vehicle.dispatch",
  adapter: "vehicle",
  operation: "vehicle.dispatch",
  schemaVersion: 1,
  inputSchema: Type.Object({ route: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ dispatch: Type.String() }, { additionalProperties: false }),
  effectClass: "physical_actuation",
  consequence: "compensatable",
  capabilities,
})

const snapshot = defineOperation({
  key: "vehicle.snapshot",
  adapter: "vehicle",
  operation: "vehicle.snapshot",
  schemaVersion: 1,
  inputSchema: Type.Object({ vehicle: Type.String() }, { additionalProperties: false }),
  outputSchema: Type.Object({ status: Type.String() }, { additionalProperties: false }),
  effectClass: "observation",
  consequence: "observational",
  capabilities,
})

const mission = defineMission({
  id: "dispatcher-delivery",
  version: "1.0.0",
  input: Type.Object(
    { route: Type.String(), vehicle: Type.String() },
    { additionalProperties: false },
  ),
  initial: "dispatchVehicle",
  states: {
    dispatchVehicle: effectState({
      operation: dispatch,
      effectSlot: "dispatch",
      input: mapFields({ route: fromInput("route") }),
      retry: safeRetry({ attempts: 2, backoffMs: 100 }),
      onAccepted: "waitForArrival",
      onSettled: "complete",
      onFailed: "failed",
      onUncertain: "reconcileVehicle",
    }),
    waitForArrival: waitForObservation({
      observationType: "vehicle.arrived",
      schemaVersion: 1,
      correlation: { kind: "effect", effectState: "dispatchVehicle" },
      timeout: { afterMs: 1_000, onTimeout: "reconcileVehicle" },
      onObserved: "complete",
    }),
    reconcileVehicle: reconcileState({
      effectState: "dispatchVehicle",
      operation: snapshot,
      subject: fromInput("vehicle"),
      onConfirmedCompleted: "complete",
      onConfirmedNotApplied: "dispatchVehicle",
      onConfirmedInProgress: "waitForArrival",
      onConflictingState: "unresolved",
      onUnknown: "unresolved",
    }),
    complete: finalState("completed"),
    failed: finalState("failed"),
    unresolved: finalState("unresolved"),
  },
})

const runtime: MissionRuntimeDefinition = {
  manifest: mission.manifest,
  definition_digest: mission.definitionDigest,
  execution_binding_digest: `sha256:${"b".repeat(64)}`,
}

const runId = "run_01ARZ3NDEKTSV4RRFFQ69G5FAV" as const
const temporaryDirectories: string[] = []
const journals: SqliteJournal[] = []

class FixedIdentities implements IdentityFactory {
  readonly #attempts: AttemptId[]
  readonly #events: EventId[]

  constructor(attempts: AttemptId[], events: EventId[]) {
    this.#attempts = [...attempts]
    this.#events = [...events]
  }

  attemptId(): AttemptId {
    const value = this.#attempts.shift()
    if (!value) {
      throw new Error("No attempt identity remains")
    }
    return value
  }

  eventId(): EventId {
    const value = this.#events.shift()
    if (!value) {
      throw new Error("No event identity remains")
    }
    return value
  }
}

function tickingClock() {
  let tick = 0
  return () => {
    tick += 1
    return new Date(Date.UTC(2026, 0, 1, 0, 0, tick))
  }
}

function open(): SqliteJournal {
  const directory = mkdtempSync(join(tmpdir(), "idemantis-dispatcher-"))
  temporaryDirectories.push(directory)
  const journal = new SqliteJournal({
    path: join(directory, "journal.sqlite"),
    clock: tickingClock(),
  })
  journals.push(journal)
  journal.admit(runtime, admission())
  return journal
}

function admission(): ProposedEvent {
  return {
    event_id: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
    run_id: runId,
    type: "mission.admitted",
    schema_version: 1,
    source: { kind: "system", source_id: "dispatcher-test" },
    ownership_epoch: 1,
    payload: {
      definition_digest: runtime.definition_digest,
      execution_binding_digest: runtime.execution_binding_digest,
      input: { route: "north", vehicle: "vehicle-7" },
    },
  }
}

function identities(): FixedIdentities {
  return new FixedIdentities(
    [
      "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      "att_01ARZ3NDEKTSV4RRFFQ69G5FAW",
      "att_01ARZ3NDEKTSV4RRFFQ69G5FAX",
    ],
    [
      "evt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
      "evt_01ARZ3NDEKTSV4RRFFQ69G5FAX",
      "evt_01ARZ3NDEKTSV4RRFFQ69G5FAY",
      "evt_01ARZ3NDEKTSV4RRFFQ69G5FAZ",
    ],
  )
}

afterEach(() => {
  for (const journal of journals.splice(0)) {
    journal.close()
  }
  for (const directory of temporaryDirectories.splice(0)) {
    chmodSync(directory, 0o700)
    rmSync(directory, { recursive: true, force: true })
  }
})

describe("command dispatcher", () => {
  it("commits dispatch start before invoking the executor", async () => {
    const journal = open()
    const issue = vi.fn(async () => {
      expect(journal.listAttempts(runId)[0]?.status).toBe("dispatch_started")
      expect(journal.listEffects(runId)[0]?.status).toBe("dispatching")
      return "accepted" as const
    })
    const dispatcher = new CommandDispatcher({
      journal,
      executor: { issue, reconcile: vi.fn() },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:00:02Z"),
      identities: identities(),
    })

    const outcome = await dispatcher.dispatchNext(runId)

    expect(issue).toHaveBeenCalledOnce()
    expect(outcome).toMatchObject({ status: "recorded", result: "accepted" })
    expect(journal.listAttempts(runId)[0]?.status).toBe("result_recorded")
    expect(journal.listCommands(runId).map((command) => command.status)).toEqual([
      "completed",
      "completed",
    ])
  })

  it("records typed uncertainty when an executor throws", async () => {
    const journal = open()
    const issue = vi.fn(async () => {
      throw new Error("connection lost")
    })
    const reconcile = vi.fn(async () => {
      throw new Error("provider unavailable")
    })
    const dispatcher = new CommandDispatcher({
      journal,
      executor: { issue, reconcile },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock: new VirtualClock("2026-01-01T00:00:02Z"),
      identities: identities(),
    })

    const issueOutcome = await dispatcher.dispatchNext(runId)
    const reconciliationOutcome = await dispatcher.dispatchNext(runId)

    expect(issueOutcome).toMatchObject({ status: "recorded", result: "uncertain" })
    expect(reconciliationOutcome).toMatchObject({ status: "recorded", result: "unknown" })
    expect(reconcile).toHaveBeenCalledOnce()
    expect(journal.getMission(runId).state.status).toBe("unresolved")
    expect(await dispatcher.dispatchNext(runId)).toEqual({ status: "idle" })
  })
})

describe("timer dispatcher", () => {
  it("uses virtual time to fire durable timers at their deadline", async () => {
    const journal = open()
    const clock = new VirtualClock("2026-01-01T00:00:02Z")
    const commandDispatcher = new CommandDispatcher({
      journal,
      executor: { issue: async () => "accepted", reconcile: async () => "unknown" },
      worker_id: "runner-1",
      lease_duration_ms: 60_000,
      clock,
      identities: identities(),
    })
    await commandDispatcher.dispatchNext(runId)
    const timer = journal.listTimers(runId)[0]
    if (!timer) {
      throw new Error("Expected a scheduled timer")
    }
    const timerDispatcher = new TimerDispatcher({
      journal,
      source_id: "timer-1",
      clock,
      identities: new FixedIdentities([], ["evt_01ARZ3NDEKTSV4RRFFQ69G5FB0"]),
    })

    expect(timerDispatcher.fireDue().results).toEqual([])
    clock.advanceTo(timer.due_at)
    const outcome = timerDispatcher.fireDue()

    expect(outcome.results).toHaveLength(1)
    expect(outcome.results[0]?.status).toBe("fired")
    expect(journal.listTimers(runId)[0]?.status).toBe("fired")
    expect(journal.listCommands(runId)[2]?.kind).toBe("effect.reconcile")
  })
})

describe("identity factory", () => {
  it("uses cryptographically secure contract-valid identities", () => {
    const factory = new SecureIdentityFactory()
    const attempts = new Set<AttemptId>()
    const events = new Set<EventId>()

    for (let index = 0; index < 64; index += 1) {
      attempts.add(factory.attemptId())
      events.add(factory.eventId())
    }

    expect(attempts.size).toBe(64)
    expect(events.size).toBe(64)
    expect([...attempts].every((value) => Schema.Check(AttemptIdSchema, value))).toBe(true)
    expect([...events].every((value) => Schema.Check(EventIdSchema, value))).toBe(true)
  })
})
