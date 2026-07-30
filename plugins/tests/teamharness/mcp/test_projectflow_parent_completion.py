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


class ProjectflowParentCompletionTest(unittest.TestCase):
    def setUp(self) -> None:
        _MatrixHandler.events = []
        self.matrix_server = _MatrixServer(("127.0.0.1", 0), _MatrixHandler)
        self.matrix_thread = threading.Thread(
            target=self.matrix_server.serve_forever,
            daemon=True,
        )
        self.matrix_thread.start()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp_dir.name) / "workspace"
        self.workspace.mkdir()
        self.arguments = {"workspaceDir": str(self.workspace)}
        self.env = mock.patch.dict(
            os.environ,
            {
                "AGENTTEAMS_MATRIX_URL": (
                    f"http://127.0.0.1:{self.matrix_server.server_address[1]}"
                ),
                "AGENTTEAMS_WORKER_MATRIX_TOKEN": "test-token",
            },
        )
        self.env.start()

    def tearDown(self) -> None:
        self.env.stop()
        self.matrix_server.shutdown()
        self.matrix_server.server_close()
        self.temp_dir.cleanup()

    def _write_parent_task(self, task_id: str) -> None:
        task_dir = server._task_dir(self.arguments, task_id)
        server._write_json(
            task_dir / "meta.json",
            {
                "task_id": task_id,
                "room_id": "!leader-room:example.test",
                "assigned_to": "leader",
                "status": "assigned",
            },
        )
        (task_dir / "spec.md").write_text(
            "Do the work.\n\n"
            f"{server.MANAGER_PARENT_TASK_PROTOCOL_HEADING}\n\n"
            "This task was delegated by `@manager:example.test`.\n"
            f"1. Write `shared/tasks/{task_id}/result.md`.\n"
            "2. Reply with "
            f"`@manager:example.test TASK_COMPLETED: {task_id}`.\n",
            encoding="utf-8",
        )

    def _create_parent_project(
        self,
        task_id: str,
        *,
        routed: bool = False,
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "projectId": task_id,
            "title": "Manager parent project",
        }
        if routed:
            payload.update(
                {
                    "source": "matrix",
                    "requester": "@manager:example.test",
                    "sourceRoomId": "!leader-room:example.test",
                    "replyRoute": {
                        "channel": "matrix",
                        "targetUser": "@manager:example.test",
                        "targetSession": "!leader-room:example.test",
                    },
                }
            )
        return server._projectflow(
            {
                **self.arguments,
                "action": "create_project",
                "payload": payload,
            }
        )

    def test_complete_project_mirrors_syncs_and_notifies_parent_task(self) -> None:
        task_id = "task-20260730-010203-abc123"
        self._write_parent_task(task_id)
        created = self._create_parent_project(task_id)
        self.assertTrue(created["ok"])
        self.assertEqual(
            created["project"]["parent_task"]["manager_user_id"],
            "@manager:example.test",
        )
        self.assertEqual(
            created["project"]["source_room_id"],
            "!leader-room:example.test",
        )
        project_result = server._project_dir(self.arguments, task_id) / "result.md"
        project_result.write_text("# Final result\n\nVerified.\n", encoding="utf-8")
        synced: list[dict[str, object]] = []

        def fake_filesync(arguments: dict[str, object]) -> dict[str, object]:
            synced.append(dict(arguments))
            return {"ok": True}

        with mock.patch.object(server, "_filesync", side_effect=fake_filesync):
            completed = server._projectflow(
                {
                    **self.arguments,
                    "action": "complete_project",
                    "payload": {
                        "projectId": task_id,
                        "summary": "All acceptance checks passed",
                    },
                }
            )

        self.assertTrue(completed["ok"])
        self.assertEqual(completed["project"]["status"], "completed")
        parent_result = server._task_dir(self.arguments, task_id) / "result.md"
        self.assertEqual(parent_result.read_bytes(), project_result.read_bytes())
        parent_meta = server._read_json(
            server._task_dir(self.arguments, task_id) / "meta.json"
        )
        self.assertEqual(parent_meta["status"], "submitted")
        self.assertEqual(parent_meta["result_status"], "SUCCESS")
        self.assertEqual(
            parent_meta["result_path"],
            f"shared/tasks/{task_id}/result.md",
        )
        self.assertEqual(
            parent_meta["deliverables"],
            [f"shared/tasks/{task_id}/result.md"],
        )
        self.assertEqual(parent_meta["submitted_by_role"], "team_leader")
        self.assertEqual(len(synced), 2)
        self.assertEqual([call["action"] for call in synced], ["push", "push"])
        self.assertEqual(
            [call["path"] for call in synced],
            [
                f"shared/tasks/{task_id}/result.md",
                f"shared/tasks/{task_id}/meta.json",
            ],
        )
        parent_completion = completed["parentTaskCompletion"]
        self.assertTrue(parent_completion["synced"])
        self.assertEqual(parent_completion["notification"]["status"], "sent")
        self.assertEqual(len(_MatrixHandler.events), 1)
        event = _MatrixHandler.events[0]
        self.assertEqual(event["authorization"], "Bearer test-token")
        content = event["content"]
        self.assertIn(
            f"@manager:example.test TASK_COMPLETED: {task_id}",
            content["body"],
        )
        self.assertEqual(
            content["m.mentions"],
            {"user_ids": ["@manager:example.test"]},
        )

    def test_complete_parent_project_requires_project_result_before_state_change(
        self,
    ) -> None:
        task_id = "task-20260730-010204-def456"
        self._write_parent_task(task_id)
        created = self._create_parent_project(task_id)
        self.assertTrue(created["ok"])

        with mock.patch.object(server, "_filesync") as filesync:
            completed = server._projectflow(
                {
                    **self.arguments,
                    "action": "complete_project",
                    "payload": {"projectId": task_id},
                }
            )

        self.assertFalse(completed["ok"])
        self.assertIn("requires shared/projects", completed["error"])
        filesync.assert_not_called()
        state = server._read_json(server._project_state_path(self.arguments, task_id))
        self.assertEqual(state["status"], "active")
        self.assertEqual(_MatrixHandler.events, [])

    def test_complete_parent_project_uses_report_summary_when_payload_omits_it(
        self,
    ) -> None:
        task_id = "task-20260730-010207-mno345"
        self._write_parent_task(task_id)
        created = self._create_parent_project(task_id)
        self.assertTrue(created["ok"])
        project_result = server._project_dir(self.arguments, task_id) / "result.md"
        project_result.write_text(
            "STATUS: SUCCESS\n"
            "SUMMARY: QWENPAW2-REPORT-PASS - verified by the Leader\n"
            "\n"
            "DELIVERABLES:\n"
            f"- shared/tasks/{task_id}/result.md\n",
            encoding="utf-8",
        )

        with mock.patch.object(
            server,
            "_filesync",
            return_value={"ok": True},
        ):
            completed = server._projectflow(
                {
                    **self.arguments,
                    "action": "complete_project",
                    "payload": {"projectId": task_id},
                }
            )

        self.assertTrue(completed["ok"])
        parent_meta = server._read_json(
            server._task_dir(self.arguments, task_id) / "meta.json"
        )
        self.assertEqual(
            parent_meta["summary"],
            "QWENPAW2-REPORT-PASS - verified by the Leader",
        )
        self.assertEqual(
            completed["parentTaskCompletion"]["summary"],
            "QWENPAW2-REPORT-PASS - verified by the Leader",
        )
        self.assertIn(
            "QWENPAW2-REPORT-PASS - verified by the Leader",
            _MatrixHandler.events[0]["content"]["body"],
        )

    def test_parent_project_uses_route_when_only_spec_was_pulled(self) -> None:
        task_id = "task-20260730-010205-ghi789"
        self._write_parent_task(task_id)
        (server._task_dir(self.arguments, task_id) / "meta.json").unlink()

        created = self._create_parent_project(task_id, routed=True)

        self.assertTrue(created["ok"])
        self.assertEqual(
            created["project"]["parent_task"],
            {
                "task_id": task_id,
                "result_path": f"shared/tasks/{task_id}/result.md",
                "room_id": "!leader-room:example.test",
                "manager_user_id": "@manager:example.test",
            },
        )
        project_result = server._project_dir(self.arguments, task_id) / "result.md"
        project_result.write_text("# Final result\n\nVerified.\n", encoding="utf-8")

        with mock.patch.object(
            server,
            "_filesync",
            return_value={"ok": True},
        ):
            completed = server._projectflow(
                {
                    **self.arguments,
                    "action": "complete_project",
                    "payload": {"projectId": task_id},
                }
            )

        self.assertTrue(completed["ok"])
        self.assertTrue(completed["parentTaskCompletion"]["synced"])
        self.assertEqual(
            completed["parentTaskCompletion"]["notification"]["status"],
            "sent",
        )

    def test_parent_project_without_meta_requires_matching_manager_route(self) -> None:
        task_id = "task-20260730-010206-jkl012"
        self._write_parent_task(task_id)
        (server._task_dir(self.arguments, task_id) / "meta.json").unlink()

        created = self._create_parent_project(task_id)

        self.assertTrue(created["ok"])
        self.assertNotIn("parent_task", created["project"])


if __name__ == "__main__":
    unittest.main()
