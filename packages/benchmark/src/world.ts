import { createHash } from "node:crypto"

export const workflowSteps = ["create_ticket", "charge_card", "send_receipt"] as const

export type WorkflowStep = (typeof workflowSteps)[number]

export interface FaultRates {
  transient: number
  lost_response: number
  crash: number
}

export interface StepOutput {
  id: string
}

export class TransientFailure extends Error {
  override readonly name = "TransientFailure"
}

export class LostResponse extends Error {
  override readonly name = "LostResponse"
}

export class ProcessCrash extends Error {
  override readonly name = "ProcessCrash"
}

export type Fault = "none" | "transient" | "lost_response" | "crash"

export function faultFor(seed: number, key: string, rates: FaultRates): Fault {
  const digest = createHash("sha256").update(`${seed}:${key}`).digest()
  const roll = digest.readUInt32BE(0) / 0x1_0000_0000
  if (roll < rates.transient) {
    return "transient"
  }
  if (roll < rates.transient + rates.lost_response) {
    return "lost_response"
  }
  if (roll < rates.transient + rates.lost_response + rates.crash) {
    return "crash"
  }
  return "none"
}

export class ExternalWorld {
  readonly #seed: number
  readonly #rates: FaultRates
  readonly #applied = new Map<string, number>()
  readonly #calls = new Map<string, number>()
  readonly #references = new Map<string, StepOutput>()
  #totalCalls = 0

  constructor(seed: number, rates: FaultRates) {
    this.#seed = seed
    this.#rates = rates
  }

  call(task: string, step: WorkflowStep, reference?: string): StepOutput {
    const key = `${task}:${step}`
    const call = (this.#calls.get(key) ?? 0) + 1
    this.#calls.set(key, call)
    this.#totalCalls += 1
    const fault = faultFor(this.#seed, `${key}:${call}`, this.#rates)
    if (fault === "transient") {
      throw new TransientFailure(`${step} is temporarily unavailable`)
    }
    const applied = (this.#applied.get(key) ?? 0) + 1
    this.#applied.set(key, applied)
    const output = { id: `${step}-${task}-${applied}` }
    if (reference) {
      this.#references.set(reference, output)
    }
    if (fault === "lost_response") {
      throw new LostResponse(`${step} response was lost after it was applied`)
    }
    if (fault === "crash") {
      throw new ProcessCrash(`agent process crashed after ${step} was applied`)
    }
    return output
  }

  lookup(reference: string): StepOutput | undefined {
    return this.#references.get(reference)
  }

  applied(task: string, step: WorkflowStep): number {
    return this.#applied.get(`${task}:${step}`) ?? 0
  }

  get totalCalls(): number {
    return this.#totalCalls
  }
}
