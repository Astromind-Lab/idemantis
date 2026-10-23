import { closeSync, constants, fchmodSync, mkdirSync, openSync } from "node:fs"
import { dirname, resolve } from "node:path"
import { DatabaseSync } from "node:sqlite"
import { JournalError } from "./errors.js"
import { migrations } from "./migrations.js"

interface MigrationRow {
  [key: string]: unknown
  version?: unknown
  name?: unknown
  checksum?: unknown
}

function prepareFile(path: string): string {
  if (path === ":memory:") {
    return path
  }

  const resolved = resolve(path)
  mkdirSync(dirname(resolved), { recursive: true, mode: 0o700 })
  const descriptor = openSync(
    resolved,
    constants.O_CREAT | constants.O_RDWR | constants.O_NOFOLLOW,
    0o600,
  )

  try {
    fchmodSync(descriptor, 0o600)
  } finally {
    closeSync(descriptor)
  }

  return resolved
}

function integer(value: unknown, field: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new JournalError("CORRUPT_RECORD", `SQLite returned an invalid ${field}`)
  }

  return value
}

function text(value: unknown, field: string): string {
  if (typeof value !== "string") {
    throw new JournalError("CORRUPT_RECORD", `SQLite returned an invalid ${field}`)
  }

  return value
}

export class JournalDatabase {
  readonly connection: DatabaseSync
  #closed = false

  constructor(path: string, busyTimeoutMs: number, appliedAt: string) {
    const databasePath = prepareFile(path)
    this.connection = new DatabaseSync(databasePath, {
      allowExtension: false,
      enableDoubleQuotedStringLiterals: false,
      enableForeignKeyConstraints: true,
    })
    this.connection.exec(`
      PRAGMA foreign_keys = ON;
      PRAGMA trusted_schema = OFF;
      PRAGMA secure_delete = ON;
      PRAGMA busy_timeout = ${busyTimeoutMs};
      PRAGMA synchronous = FULL;
    `)

    if (path !== ":memory:") {
      this.connection.exec("PRAGMA journal_mode = WAL")
    }

    try {
      this.applyMigrations(appliedAt)
    } catch (error) {
      this.connection.close()
      this.#closed = true
      throw error
    }
  }

  assertOpen(): void {
    if (this.#closed) {
      throw new JournalError("JOURNAL_CLOSED", "The SQLite journal is closed")
    }
  }

  transaction<Value>(operation: () => Value): Value {
    this.assertOpen()
    this.connection.exec("BEGIN IMMEDIATE")

    try {
      const value = operation()
      this.connection.exec("COMMIT")
      return value
    } catch (error) {
      try {
        this.connection.exec("ROLLBACK")
      } catch {
        this.#closed = true
        this.connection.close()
      }
      throw error
    }
  }

  close(): void {
    if (this.#closed) {
      return
    }

    this.connection.close()
    this.#closed = true
  }

  private applyMigrations(appliedAt: string): void {
    this.transaction(() => {
      this.connection.exec(`
        CREATE TABLE IF NOT EXISTS journal_migrations (
          version INTEGER PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          checksum TEXT NOT NULL,
          applied_at TEXT NOT NULL
        ) STRICT;
      `)

      const rows = this.connection
        .prepare("SELECT version, name, checksum FROM journal_migrations ORDER BY version")
        .all()

      for (const raw of rows) {
        const row = raw as MigrationRow
        const version = integer(row.version, "migration version")
        const migration = migrations.find((item) => item.version === version)

        if (
          !migration ||
          text(row.name, "migration name") !== migration.name ||
          text(row.checksum, "migration checksum") !== migration.checksum
        ) {
          throw new JournalError(
            "MIGRATION_MISMATCH",
            `Applied migration ${version} does not match this journal build`,
          )
        }
      }

      for (const migration of migrations) {
        if (
          rows.some(
            (raw) =>
              integer((raw as MigrationRow).version, "migration version") === migration.version,
          )
        ) {
          continue
        }

        this.connection.exec(migration.sql)
        this.connection
          .prepare(
            "INSERT INTO journal_migrations(version, name, checksum, applied_at) VALUES (?, ?, ?, ?)",
          )
          .run(migration.version, migration.name, migration.checksum, appliedAt)
        this.connection.exec(`PRAGMA user_version = ${migration.version}`)
      }
    })
  }
}
