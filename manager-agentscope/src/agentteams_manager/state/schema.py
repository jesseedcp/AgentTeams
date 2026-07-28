"""Initial SQLite schema for durable operations."""

SCHEMA_VERSION = 12

SESSION_SETTINGS_MIGRATION_COLUMNS = {
    "thinking_effort": "TEXT",
    "reasoning_visibility": "TEXT NOT NULL DEFAULT 'off'",
    "verbose_mode": "TEXT NOT NULL DEFAULT 'off'",
    "elevated_mode": "TEXT NOT NULL DEFAULT 'off'",
    "queue_mode": "TEXT NOT NULL DEFAULT 'followup'",
    "queue_limit": "INTEGER NOT NULL DEFAULT 20",
}

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

CREATE TABLE IF NOT EXISTS session_settings (
  room_id TEXT PRIMARY KEY,
  model_override TEXT,
  thinking_effort TEXT,
  reasoning_visibility TEXT NOT NULL DEFAULT 'off',
  verbose_mode TEXT NOT NULL DEFAULT 'off',
  elevated_mode TEXT NOT NULL DEFAULT 'off',
  queue_mode TEXT NOT NULL DEFAULT 'followup',
  queue_limit INTEGER NOT NULL DEFAULT 20,
  timezone TEXT NOT NULL,
  next_reset_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS session_settings_reset_idx
  ON session_settings(next_reset_at);

CREATE TABLE IF NOT EXISTS daily_memories (
  memory_id TEXT PRIMARY KEY,
  room_id TEXT NOT NULL,
  memory_day TEXT NOT NULL,
  content TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(room_id, source_event_id)
);
CREATE INDEX IF NOT EXISTS daily_memories_room_day_idx
  ON daily_memories(room_id, memory_day, created_at);

CREATE TABLE IF NOT EXISTS long_term_memories (
  memory_id TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  category TEXT NOT NULL,
  content TEXT NOT NULL,
  importance REAL NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS long_term_memories_scope_idx
  ON long_term_memories(scope, importance DESC, updated_at DESC);

CREATE TABLE IF NOT EXISTS project_decisions (
  decision_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  decision TEXT NOT NULL,
  rationale TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS project_decisions_project_idx
  ON project_decisions(project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS worker_capability_assessments (
  worker_name TEXT NOT NULL,
  capability TEXT NOT NULL,
  score REAL NOT NULL,
  evidence TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(worker_name, capability),
  CHECK(score >= 0.0 AND score <= 1.0)
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

CREATE TABLE IF NOT EXISTS project_task_dependencies (
  project_id TEXT NOT NULL,
  task_id TEXT NOT NULL REFERENCES tasks(task_id),
  depends_on_task_id TEXT NOT NULL REFERENCES tasks(task_id),
  created_at TEXT NOT NULL,
  PRIMARY KEY(task_id, depends_on_task_id),
  CHECK(task_id <> depends_on_task_id)
);
CREATE INDEX IF NOT EXISTS project_task_dependencies_project_idx
  ON project_task_dependencies(project_id, task_id);

CREATE TABLE IF NOT EXISTS project_task_transitions (
  task_id TEXT NOT NULL REFERENCES tasks(task_id),
  sequence INTEGER NOT NULL,
  from_status TEXT,
  to_status TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  reason TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY(task_id, sequence)
);

CREATE TABLE IF NOT EXISTS project_participants (
  project_id TEXT NOT NULL,
  worker_name TEXT NOT NULL,
  joined_at TEXT NOT NULL,
  removed_at TEXT,
  PRIMARY KEY(project_id, worker_name)
);
CREATE INDEX IF NOT EXISTS project_participants_active_idx
  ON project_participants(project_id, removed_at);

CREATE TABLE IF NOT EXISTS project_plan_revisions (
  project_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  body TEXT NOT NULL,
  change_kind TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY(project_id, revision)
);

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

CREATE TABLE IF NOT EXISTS confirmation_requests (
  confirmation_id TEXT PRIMARY KEY,
  source_room_id TEXT NOT NULL,
  source_event_id TEXT NOT NULL,
  source_reply_id TEXT NOT NULL,
  requester_id TEXT NOT NULL,
  tool_calls_json TEXT NOT NULL,
  source_policy_json TEXT NOT NULL,
  status TEXT NOT NULL,
  decision INTEGER,
  resolver_id TEXT,
  created_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  resolved_at TEXT,
  UNIQUE(source_room_id, source_event_id, source_reply_id)
);
CREATE INDEX IF NOT EXISTS confirmation_requests_pending_idx
  ON confirmation_requests(status, expires_at);

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

CREATE TABLE IF NOT EXISTS external_channel_contacts (
  provider TEXT NOT NULL,
  external_user_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  destination_id TEXT NOT NULL,
  status TEXT NOT NULL,
  is_primary INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  approved_at TEXT,
  PRIMARY KEY(provider, external_user_id)
);
CREATE INDEX IF NOT EXISTS external_channel_contacts_status_idx
  ON external_channel_contacts(status, updated_at);
CREATE UNIQUE INDEX IF NOT EXISTS external_channel_contacts_primary_idx
  ON external_channel_contacts(is_primary) WHERE is_primary = 1;
"""
