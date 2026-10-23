import { uptime } from "node:os"
import type { EdgeClock } from "./types.js"

export class SystemEdgeClock implements EdgeClock {
  wallNow(): Date {
    return new Date()
  }

  monotonicNowMs(): number {
    return Math.floor(uptime() * 1_000)
  }
}
