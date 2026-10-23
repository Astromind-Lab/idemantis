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

const dispatchAndTimersSql = `
ALTER TABLE commands ADD COLUMN claimed_by TEXT;
ALTER TABLE commands ADD COLUMN claim_deadline TEXT;

CREATE TABLE effect_attempts (
  attempt_id TEXT PRIMARY KEY,
  command_id TEXT NOT NULL REFERENCES commands(command_id),
  effect_id TEXT NOT NULL REFERENCES effects(effect_id),
  status TEXT NOT NULL CHECK (status IN ('claimed', 'dispatch_started', 'result_recorded')),
  ownership_epoch INTEGER NOT NULL CHECK (ownership_epoch >= 0),
  claimed_by TEXT NOT NULL,
  claim_deadline TEXT NOT NULL,
  started_event_id TEXT REFERENCES mission_events(event_id),
  result_event_id TEXT REFERENCES mission_events(event_id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (
    (status = 'claimed' AND started_event_id IS NULL AND result_event_id IS NULL)
    OR (status = 'dispatch_started' AND started_event_id IS NOT NULL AND result_event_id IS NULL)
    OR (status = 'result_recorded' AND started_event_id IS NOT NULL AND result_event_id IS NOT NULL)
  )
) STRICT;

ALTER TABLE commands
ADD COLUMN active_attempt_id TEXT REFERENCES effect_attempts(attempt_id);

CREATE INDEX effect_attempts_command_idx
ON effect_attempts(command_id, created_at);

CREATE TABLE timers (
  timer_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES mission_runs(run_id),
  status TEXT NOT NULL CHECK (status IN ('scheduled', 'fired', 'cancelled', 'superseded')),
  due_at TEXT NOT NULL,
  purpose TEXT NOT NULL,
  causing_event_id TEXT NOT NULL REFERENCES mission_events(event_id),
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  fire_event_id TEXT REFERENCES mission_events(event_id),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (
    (status = 'fired' AND fire_event_id IS NOT NULL)
    OR (status <> 'fired' AND fire_event_id IS NULL)
  )
) STRICT;

CREATE INDEX timers_due_idx
ON timers(status, due_at, timer_id);

INSERT INTO timers(
  timer_id, run_id, status, due_at, purpose, causing_event_id,
  payload_json, created_at, updated_at
)
SELECT
  json_extract(payload_json, '$.timer_id'),
  run_id,
  'scheduled',
  json_extract(payload_json, '$.due_at'),
  json_extract(payload_json, '$.purpose'),
  causing_event_id,
  json_object(
    'timer_id', json_extract(payload_json, '$.timer_id'),
    'state_instance_id', json_extract(payload_json, '$.state_instance_id')
  ),
  created_at,
  created_at
FROM commands
WHERE kind = 'timer.schedule';

UPDATE commands
SET status = 'completed'
WHERE kind = 'timer.schedule';
`

function migration(version: number, name: string, sql: string): Migration {
  return {
    version,
    name,
    sql,
    checksum: sha256Digest({ name, sql } as JsonObject),
  }
}

export const migrations = [
  migration(1, "core_journal", coreJournalSql),
  migration(2, "dispatch_and_timers", dispatchAndTimersSql),
] as const
