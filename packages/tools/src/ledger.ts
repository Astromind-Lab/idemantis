import { chmodSync, existsSync } from "node:fs"
import { DatabaseSync } from "node:sqlite"
import type { JsonValue } from "@idemantis/contracts"

export type LedgerStatus = "running" | "completed" | "rejected" | "errored"

export interface LedgerEntry {
  effect_id: string
  tool: string
  input_json: string
  status: LedgerStatus
  output?: JsonValue
  message?: string
  started_at: string
  finished_at?: string
}

export interface ToolLedgerOptions {
  path: string
  clock?: () => Date
}

type Row = Record<string, unknown>

function text(row: Row, field: string): string {
  const value = row[field]
  if (typeof value !== "string") {
    throw new Error(`Tool ledger returned an invalid ${field}`)
  }
  return value
}

function optional(row: Row, field: string): string | undefined {
  const value = row[field]
  return typeof value === "string" ? value : undefined
}

export class ToolLedger {
  readonly #database: DatabaseSync
  readonly #clock: () => Date
  #closed = false

  constructor(options: ToolLedgerOptions) {
    this.#clock = options.clock ?? (() => new Date())
    const created = options.path !== ":memory:" && !existsSync(options.path)
    this.#database = new DatabaseSync(options.path)
    if (created) {
      chmodSync(options.path, 0o600)
    }
    this.#database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA busy_timeout = 5000;
      CREATE TABLE IF NOT EXISTS tool_calls (
        effect_id TEXT PRIMARY KEY,
        tool TEXT NOT NULL,
        input_json TEXT NOT NULL CHECK (json_valid(input_json)),
        status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'rejected', 'errored')),
        output_json TEXT CHECK (output_json IS NULL OR json_valid(output_json)),
        message TEXT,
        started_at TEXT NOT NULL,
        finished_at TEXT
      ) STRICT;
    `)
  }

  begin(effectId: string, tool: string, input: JsonValue): LedgerEntry | undefined {
    const result = this.#database
      .prepare(
        "INSERT INTO tool_calls(effect_id, tool, input_json, status, started_at) VALUES (?, ?, ?, 'running', ?) ON CONFLICT(effect_id) DO NOTHING",
      )
      .run(effectId, tool, JSON.stringify(input), this.#clock().toISOString())
    return result.changes === 1 ? undefined : this.get(effectId)
  }

  complete(effectId: string, output: JsonValue): void {
    this.finish(effectId, "completed", JSON.stringify(output), null)
  }

  reject(effectId: string, message: string): void {
    this.finish(effectId, "rejected", null, message)
  }

  error(effectId: string, message: string): void {
    this.finish(effectId, "errored", null, message)
  }

  forget(effectId: string): void {
    this.#database
      .prepare("DELETE FROM tool_calls WHERE effect_id = ? AND status != 'completed'")
      .run(effectId)
  }

  get(effectId: string): LedgerEntry | undefined {
    const row = this.#database
      .prepare("SELECT * FROM tool_calls WHERE effect_id = ?")
      .get(effectId) as Row | undefined
    return row ? this.entry(row) : undefined
  }

  list(): LedgerEntry[] {
    return this.#database
      .prepare("SELECT * FROM tool_calls ORDER BY started_at, effect_id")
      .all()
      .map((row) => this.entry(row as Row))
  }

  close(): void {
    if (!this.#closed) {
      this.#closed = true
      this.#database.close()
    }
  }

  private finish(
    effectId: string,
    status: LedgerStatus,
    output: string | null,
    message: string | null,
  ): void {
    this.#database
      .prepare(
        "UPDATE tool_calls SET status = ?, output_json = ?, message = ?, finished_at = ? WHERE effect_id = ? AND status = 'running'",
      )
      .run(status, output, message, this.#clock().toISOString(), effectId)
  }

  private entry(row: Row): LedgerEntry {
    const output = optional(row, "output_json")
    const message = optional(row, "message")
    const finishedAt = optional(row, "finished_at")
    return {
      effect_id: text(row, "effect_id"),
      tool: text(row, "tool"),
      input_json: text(row, "input_json"),
      status: text(row, "status") as LedgerStatus,
      ...(output === undefined ? {} : { output: JSON.parse(output) as JsonValue }),
      ...(message === undefined ? {} : { message }),
      started_at: text(row, "started_at"),
      ...(finishedAt === undefined ? {} : { finished_at: finishedAt }),
    }
  }
}
