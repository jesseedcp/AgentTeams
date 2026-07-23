"""Initial SQLite schema for durable operations."""

SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS operations (
  operation_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  target_key TEXT NOT NULL,
  status TEXT NOT NULL,
  request_json TEXT NOT NULL,
  result_json TEXT NOT NULL DEFAULT '{}',
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS operations_recovery_idx
  ON operations(status, updated_at);

CREATE TABLE IF NOT EXISTS operation_events (
  operation_id TEXT NOT NULL REFERENCES operations(operation_id),
  sequence INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(operation_id, sequence)
);

CREATE TABLE IF NOT EXISTS processed_matrix_events (
  room_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  PRIMARY KEY(room_id, event_id)
);

CREATE TABLE IF NOT EXISTS key_values (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"""

