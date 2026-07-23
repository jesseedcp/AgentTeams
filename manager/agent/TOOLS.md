# AgentTeams Manager Typed Tool Guide

All management actions use typed AgentScope tools. Skill documents explain
when to choose them; Python workflows enforce the actual sequence and recovery
rules.

## Resource tools

- `list_workers`, `get_worker`, `create_worker`, `update_worker`
- `start_worker`, `stop_worker`, `reset_worker`, `delete_worker`
- `find_worker_candidates`, `import_worker_package`
- `list_teams`, `get_team`, `create_team`, `update_team`, `delete_team`
- `delegate_to_team_leader`
- `list_humans`, `get_human`, `create_human`, `update_human`,
  `delete_human`
- `inspect_room`, `create_private_room`, `invite_room_member`,
  `remove_room_member`, `ban_room_member`, `unban_room_member`

Resource mutations are Controller-backed and return typed reconciliation
receipts. Room membership tools are Matrix-backed. Never substitute a direct
HTTP request or generated command.

## Task, project, storage, and Git tools

- `create_finite_task`, `create_recurring_task`, `get_task`, `list_tasks`
- `complete_task`, `cancel_task`, `retry_task`
- `create_project`, `get_project`, `update_project_plan`, `close_project`
- `sync_artifacts_down`, `sync_artifacts_up`
- `prepare_git_delegation`, `execute_git_delegation`

Task and project mutations write durable intent before uploads or messages.
Artifact sync returns a checksum manifest. Git writes require confirmation and
an active processing lease.

## Configuration and integration tools

- `inspect_runtime_configuration`
- `switch_manager_model`, `switch_worker_model`
- `list_mcp_servers`, `configure_mcp_server`, `delete_mcp_server`
- `discover_mcp_tools`, `call_mcp_tool`
- `publish_worker_service`, `unpublish_worker_service`

Model changes run a gateway preflight before desired state changes. MCP
configuration is reconciled through Higress and Controller descriptors.
`discover_mcp_tools` and `call_mcp_tool` are AgentScope Toolkit operations;
they do not invoke an external compatibility CLI.

## Retained skill catalog

The Manager ships exactly these skills:

1. `agentteams-find-worker`
2. `channel-management`
3. `file-sync-management`
4. `git-delegation-management`
5. `human-management`
6. `matrix-server-management`
7. `mcp-server-management`
8. `mcporter`
9. `model-switch`
10. `project-management`
11. `service-publishing`
12. `task-coordination`
13. `task-management`
14. `team-management`
15. `worker-management`
16. `worker-model-switch`

The `mcporter` name is retained for capability compatibility. Its execution
path is native AgentScope Toolkit discovery and calls.

## Choosing a workflow

- Ordinary Worker lifecycle request: `worker-management`.
- Market search or package import: `agentteams-find-worker`, then return to
  `worker-management`.
- Multi-Worker work: `team-management`, followed by task delegation to the
  Team Leader.
- Finite or recurring work: `task-management`; use `task-coordination` for
  completion and processing-lease coordination.
- Project DAG or Project Room: `project-management`.
- Manager model versus Worker model: use the matching model-switch skill.
- MCP definition or permissions: `mcp-server-management`; MCP discovery/call:
  `mcporter`.
- Explicit artifact transfer: `file-sync-management`.
- Worker service route: `service-publishing`.
