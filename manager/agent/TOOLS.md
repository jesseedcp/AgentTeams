# AgentTeams Manager Typed Tool Guide

All management actions use typed AgentScope tools. Skill documents explain
when to choose them; Python workflows enforce the actual sequence and recovery
rules.

The following block is generated from the same canonical tool-name registry
used by Matrix authorization. CI rejects a stale or manually divergent block.
At runtime the prompt is rendered again from the concrete AgentScope Toolkit,
so optional MCP tools and room-specific restrictions remain truthful.

<!-- BEGIN GENERATED AGENTSCOPE TOOLS -->
## Registered Manager tools

- `ban_matrix_user`
- `complete_task`
- `configure_mcp`
- `create_channel`
- `create_human`
- `create_project`
- `create_task`
- `create_team`
- `create_worker`
- `delegate_task`
- `delegate_team_task`
- `delete_channel`
- `delete_human`
- `delete_project`
- `delete_task`
- `delete_team`
- `delete_worker`
- `download_matrix_media`
- `find_worker`
- `get_human`
- `get_matrix_room_state`
- `get_project`
- `get_task`
- `get_team`
- `get_worker`
- `git_delegate`
- `git_delegate_high_risk`
- `import_worker`
- `inspect_git_request`
- `invite_matrix_user`
- `kick_matrix_user`
- `list_channels`
- `list_humans`
- `list_matrix_members`
- `list_matrix_rooms`
- `list_mcp_servers`
- `list_projects`
- `list_tasks`
- `list_teams`
- `list_workers`
- `lookup_matrix_user`
- `publish_service`
- `reassign_project_task`
- `remove_mcp`
- `report_project_blocked`
- `request_project_revision`
- `reset_worker`
- `revise_project_plan`
- `revise_project_plan_major`
- `schedule_task`
- `send_notification`
- `sleep_worker`
- `switch_model`
- `switch_worker_model`
- `sync_files`
- `unban_matrix_user`
- `update_channel`
- `update_human`
- `update_manager_identity`
- `update_project`
- `update_project_participants`
- `update_task`
- `update_team`
- `update_worker`
- `upload_matrix_media`
- `wake_worker`

<!-- END GENERATED AGENTSCOPE TOOLS -->

Resource mutations are Controller-backed and return typed reconciliation
receipts. Matrix membership tools are Matrix-backed. Task and project
mutations write durable SQLite intent before storage or message effects.
File synchronization returns a checksum manifest. Git writes require
confirmation and an active processing lease. Model changes run a gateway
preflight before desired state changes. Never substitute a direct HTTP request
or generated shell command for a registered Manager tool.

## Retained skill catalog

The Manager requires these built-in skills. Additional valid local skills are
allowed:

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
