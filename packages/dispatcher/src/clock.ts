import type { Clock } from "./types.js"

function instant(value: Date | string): number {
  const milliseconds = value instanceof Date ? value.getTime() : Date.parse(value)
  if (!Number.isFinite(milliseconds)) {
    throw new RangeError("Clock instant must be a valid date")
  }

  return milliseconds
}

export class SystemClock implements Clock {
  now(): Date {
    return new Date()
  }
}

export class VirtualClock implements Clock {
  #instant: number

  constructor(initial: Date | string) {
    this.#instant = instant(initial)
  }

  now(): Date {
    return new Date(this.#instant)
  }

  advanceBy(milliseconds: number): Date {
    if (!Number.isSafeInteger(milliseconds) || milliseconds < 0) {
      throw new RangeError("milliseconds must be a non-negative safe integer")
    }
    const next = this.#instant + milliseconds
    if (!Number.isFinite(next)) {
      throw new RangeError("Clock advance exceeds the supported date range")
    }
    this.#instant = next
    return this.now()
  }

  advanceTo(value: Date | string): Date {
    const next = instant(value)
    if (next < this.#instant) {
      throw new RangeError("Virtual clock cannot move backwards")
    }
    this.#instant = next
    return this.now()
  }
}
