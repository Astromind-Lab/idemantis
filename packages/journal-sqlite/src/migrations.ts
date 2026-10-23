import { sha256Digest, type JsonObject } from "@idemantis/contracts"

export interface Migration {
  version: number
  name: string
  sql: string
  checksum: string
}

const coreJournalSql = `
CREATE TABLE mission_runs (
  run_id TEXT PRIMARY KEY,
  definition_id TEXT NOT NULL,
  definition_version TEXT NOT NULL,
  definition_digest TEXT NOT NULL,
  manifest_schema_version INTEGER NOT NULL CHECK (manifest_schema_version >= 1),
  minimum_interpreter_version TEXT NOT NULL,
  execution_binding_digest TEXT NOT NULL,
  manifest_json TEXT NOT NULL CHECK (json_valid(manifest_json)),
  state_json TEXT NOT NULL CHECK (json_valid(state_json)),
  state_digest TEXT NOT NULL,
  status TEXT NOT NULL,
  current_sequence INTEGER NOT NULL CHECK (current_sequence >= 1),
  ownership_epoch INTEGER NOT NULL CHECK (ownership_epoch >= 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE mission_events (
  event_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES mission_runs(run_id),
  sequence INTEGER NOT NULL CHECK (sequence >= 1),
  type TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
  ownership_epoch INTEGER NOT NULL CHECK (ownership_epoch >= 0),
  proposal_digest TEXT NOT NULL,
  event_json TEXT NOT NULL CHECK (json_valid(event_json)),
  state_digest TEXT NOT NULL,
  recorded_at TEXT NOT NULL,
  UNIQUE (run_id, sequence)
) STRICT;

CREATE INDEX mission_events_run_sequence_idx
ON mission_events(run_id, sequence);

CREATE TABLE commands (
  command_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES mission_runs(run_id),
  causing_event_id TEXT NOT NULL REFERENCES mission_events(event_id),
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  command_slot TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  ownership_epoch INTEGER NOT NULL CHECK (ownership_epoch >= 0),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  command_json TEXT NOT NULL CHECK (json_valid(command_json)),
  created_at TEXT NOT NULL,
  UNIQUE (causing_event_id, ordinal),
  UNIQUE (causing_event_id, command_slot)
) STRICT;

CREATE INDEX commands_run_status_idx
ON commands(run_id, status, created_at);

CREATE TABLE effects (
  effect_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES mission_runs(run_id),
  state_instance_id TEXT NOT NULL,
  state_name TEXT NOT NULL,
  effect_slot TEXT NOT NULL,
  operation TEXT NOT NULL,
  status TEXT NOT NULL,
  required INTEGER NOT NULL CHECK (required IN (0, 1)),
  issue_count INTEGER NOT NULL CHECK (issue_count >= 1),
  retry_timer_id TEXT,
  ownership_epoch INTEGER NOT NULL CHECK (ownership_epoch >= 0),
  projection_json TEXT NOT NULL CHECK (json_valid(projection_json)),
  updated_sequence INTEGER NOT NULL CHECK (updated_sequence >= 1)
) STRICT;

CREATE INDEX effects_run_status_idx
ON effects(run_id, status);

CREATE TABLE mission_snapshots (
  run_id TEXT NOT NULL REFERENCES mission_runs(run_id),
  sequence INTEGER NOT NULL CHECK (sequence >= 1),
  definition_digest TEXT NOT NULL,
  state_digest TEXT NOT NULL,
  state_json TEXT NOT NULL CHECK (json_valid(state_json)),
  created_at TEXT NOT NULL,
  PRIMARY KEY (run_id, sequence)
) STRICT;
`

function migration(version: number, name: string, sql: string): Migration {
  return {
    version,
    name,
    sql,
    checksum: sha256Digest({ name, sql } as JsonObject),
  }
}

export const migrations = [migration(1, "core_journal", coreJournalSql)] as const
