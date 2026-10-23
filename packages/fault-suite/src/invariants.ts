import type { InvariantResult } from "./types.js"

export class InvariantChecker {
  readonly #results: InvariantResult[] = []

  check(name: string, passed: boolean, detail?: string): void {
    this.#results.push({ name, passed, ...(!passed && detail ? { detail } : {}) })
  }

  results(): InvariantResult[] {
    return structuredClone(this.#results)
  }
}
