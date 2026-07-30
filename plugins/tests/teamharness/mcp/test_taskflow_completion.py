from __future__ import annotations

import http.server
import json
import os
from pathlib import Path
import socketserver
import sys
import tempfile
import threading
import unittest
from unittest import mock
import urllib.parse


MCP_DIR = Path(__file__).resolve().parents[3] / "teamharness" / "mcp"
sys.path.insert(0, str(MCP_DIR))

import server  # noqa: E402


class _MatrixHandler(http.server.BaseHTTPRequestHandler):
    events: list[dict[str, object]] = []

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_PUT(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        content = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        type(self).events.append(
            {
                "path": urllib.parse.urlparse(self.path).path,
                "authorization": self.headers.get("Authorization"),
                "content": content,
            }
        )
        payload = {"event_id": f"$event-{len(type(self).events)}"}
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(payload).encode("utf-8"))


class _MatrixServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class TaskflowCompletionNotificationTest(unittest.TestCase):
    def setUp(self) -> None:
        _MatrixHandler.events = []
        self.matrix_server = _MatrixServer(("127.0.0.1", 0), _MatrixHandler)
        self.matrix_thread = threading.Thread(target=self.matrix_server.serve_forever, daemon=True)
        self.matrix_thread.start()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp_dir.name) / "workspace"
        self.workspace.mkdir()
        self.env = mock.patch.dict(
            os.environ,
            {
                "AGENTTEAMS_MATRIX_URL": f"http://127.0.0.1:{self.matrix_server.server_address[1]}",
                "AGENTTEAMS_WORKER_MATRIX_TOKEN": "test-token",
                "AGENTTEAMS_MATRIX_USER_ID": "@leader:example.test",
            },
        )
        self.env.start()

    def tearDown(self) -> None:
        self.env.stop()
        self.matrix_server.shutdown()
        self.matrix_server.server_close()
        self.temp_dir.cleanup()

    def test_submit_syncs_then_mentions_persisted_coordinator(self) -> None:
        arguments = {"workspaceDir": str(self.workspace)}
        project_id = "completion-project"
        task_id = "completion-project-01"
        server._write_json(
            server._project_state_path(arguments, project_id),
            {
                "project_id": project_id,
                "title": "Completion project",
                "tasks": [
                    {
                        "task_id": task_id,
                        "title": "Write result",
                        "assigned_to": "@worker:example.test",
                        "status": "planned",
                    }
                ],
            },
        )

        with (
            mock.patch.object(server, "_runtime_team_room_id", return_value="!team:example.test"),
            mock.patch.object(server, "_sync_task", return_value=True),
            mock.patch.object(server, "_publish_task_artifacts", return_value=[]),
        ):
            delegated = server._taskflow(
                {
                    **arguments,
                    "role": "leader",
                    "action": "delegate_task",
                    "payload": {
                        "projectId": project_id,
                        "taskId": task_id,
                        "roomId": "room:!team:example.test",
                        "spec": "Write the result.",
                    },
                }
            )
            self.assertTrue(delegated["ok"])
            self.assertEqual(
                delegated["task"]["coordinator_matrix_user_id"],
                "@leader:example.test",
            )

            os.environ["AGENTTEAMS_MATRIX_USER_ID"] = "@worker:example.test"
            result_path = self.workspace / "shared" / "tasks" / task_id / "result.md"
            result_path.write_text("completed\n", encoding="utf-8")
            submitted = server._taskflow(
                {
                    **arguments,
                    "role": "worker",
                    "action": "submit_task",
                    "payload": {
                        "taskId": task_id,
                        "status": "SUCCESS",
                        "summary": "Result ready",
                        "deliverables": [f"shared/tasks/{task_id}/result.md"],
                    },
                }
            )

        self.assertTrue(submitted["ok"])
        self.assertTrue(submitted["synced"])
        self.assertEqual(submitted["completionNotification"]["status"], "sent")
        self.assertEqual(len(_MatrixHandler.events), 1)
        event = _MatrixHandler.events[0]
        self.assertEqual(event["authorization"], "Bearer test-token")
        content = event["content"]
        self.assertEqual(content["msgtype"], "m.text")
        self.assertIn(
            f"@leader:example.test TASK_COMPLETED: {task_id}",
            content["body"],
        )
        self.assertEqual(
            content["m.mentions"],
            {"user_ids": ["@leader:example.test"]},
        )

    def test_blocked_notification_uses_manager_protocol_marker(self) -> None:
        notification = server._notify_task_submission(
            {
                "room_id": "!project:example.test",
                "coordinator_matrix_user_id": "@manager:example.test",
                "result_path": "shared/tasks/project-task-01/result.md",
            },
            "project-task-01",
            "BLOCKED",
            "Waiting for the administrator's color code",
        )

        self.assertEqual(notification["status"], "sent")
        self.assertEqual(len(_MatrixHandler.events), 1)
        body = _MatrixHandler.events[0]["content"]["body"]
        self.assertTrue(
            body.startswith(
                "@manager:example.test TASK_BLOCKED: project-task-01"
            ),
            body,
        )


if __name__ == "__main__":
    unittest.main()
