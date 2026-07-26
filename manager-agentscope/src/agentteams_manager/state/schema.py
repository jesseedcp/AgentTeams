"""Initial SQLite schema for durable operations."""

SCHEMA_VERSION = 7

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
CREATE UNIQUE INDEX IF NOT EXISTS operation_events_global_sequence_idx
  ON operation_events(sequence);

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

CREATE TABLE IF NOT EXISTS sessions (
  room_id TEXT PRIMARY KEY,
  agent_state_json TEXT NOT NULL,
  policy_revision INTEGER NOT NULL,
  last_event_id TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  task_type TEXT NOT NULL,
  status TEXT NOT NULL,
  title TEXT NOT NULL,
  assigned_to TEXT NOT NULL,
  room_id TEXT NOT NULL,
  project_id TEXT,
  delegated_to_team TEXT,
  schedule TEXT,
  timezone TEXT,
  last_executed_at TEXT,
  next_scheduled_at TEXT,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_due_idx
  ON tasks(status, next_scheduled_at);

CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  room_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  metadata_json TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS projects_status_idx
  ON projects(status, updated_at);

CREATE TABLE IF NOT EXISTS processing_leases (
  task_id TEXT PRIMARY KEY,
  lease_id TEXT NOT NULL UNIQUE,
  processor TEXT NOT NULL,
  operation TEXT NOT NULL,
  started_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  remote_etag TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS processing_leases_expiry_idx
  ON processing_leases(expires_at);

CREATE TABLE IF NOT EXISTS notifications (
  notification_id TEXT PRIMARY KEY,
  source_operation_id TEXT NOT NULL UNIQUE,
  recipient TEXT NOT NULL,
  room_id TEXT NOT NULL,
  text TEXT NOT NULL,
  txn_id TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL,
  event_id TEXT,
  created_at TEXT NOT NULL,
  sent_at TEXT
);
CREATE INDEX IF NOT EXISTS notifications_status_idx
  ON notifications(status, created_at);

CREATE TABLE IF NOT EXISTS topology (
  resource_type TEXT NOT NULL,
  resource_name TEXT NOT NULL,
  room_kind TEXT NOT NULL,
  room_id TEXT NOT NULL,
  matrix_user_id TEXT,
  payload_json TEXT NOT NULL,
  refreshed_at TEXT NOT NULL,
  PRIMARY KEY(resource_type, resource_name, room_kind)
);
CREATE UNIQUE INDEX IF NOT EXISTS topology_room_idx
  ON topology(room_id);

CREATE TABLE IF NOT EXISTS human_access (
  name TEXT PRIMARY KEY,
  matrix_user_id TEXT NOT NULL UNIQUE,
  permission_level INTEGER NOT NULL,
  allowed_rooms_json TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  refreshed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS human_access_matrix_idx
  ON human_access(matrix_user_id);

CREATE TABLE IF NOT EXISTS topology_actors (
  matrix_user_id TEXT PRIMARY KEY,
  actor_kind TEXT NOT NULL,
  resource_name TEXT,
  team_name TEXT,
  payload_json TEXT NOT NULL,
  refreshed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS topology_actors_resource_idx
  ON topology_actors(resource_name, team_name);

CREATE TABLE IF NOT EXISTS channel_relationships (
  relationship_kind TEXT NOT NULL,
  owner_user_id TEXT NOT NULL,
  peer_user_id TEXT NOT NULL DEFAULT '',
  room_id TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(relationship_kind, owner_user_id, peer_user_id)
);
CREATE INDEX IF NOT EXISTS channel_relationships_room_idx
  ON channel_relationships(room_id);
"""
