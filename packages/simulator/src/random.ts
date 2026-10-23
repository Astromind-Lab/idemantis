import { SimulationError } from "./errors.js"

export class SeededRandom {
  #state: number

  constructor(seed: number) {
    if (!Number.isSafeInteger(seed) || seed < 0 || seed > 0xffff_ffff) {
      throw new SimulationError("INVALID_SEED", "Simulation seed must be a 32-bit unsigned integer")
    }
    this.#state = seed >>> 0
  }

  next(): number {
    this.#state = (this.#state + 0x6d2b_79f5) >>> 0
    let value = this.#state
    value = Math.imul(value ^ (value >>> 15), value | 1)
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61)
    return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296
  }

  integer(minimum: number, maximum: number): number {
    if (!Number.isSafeInteger(minimum) || !Number.isSafeInteger(maximum) || minimum > maximum) {
      throw new SimulationError("INVALID_SEED", "Random integer bounds are invalid")
    }
    return minimum + Math.floor(this.next() * (maximum - minimum + 1))
  }
}
