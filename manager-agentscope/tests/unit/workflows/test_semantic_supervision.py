from datetime import UTC, datetime, timedelta

import pytest

from agentteams_manager.domain.models import TaskRecord, WorkerResource
from agentteams_manager.workflows.heartbeat import SemanticSupervisor


class Tasks:
    def __init__(self, items):
        self.items = tuple(items)

    async def list_all(self):
        return self.items


class Workers:
    def __init__(self, items):
        self.items = tuple(items)

    async def list_workers(self):
        return self.items


class Notifications:
    def __init__(self):
        self.sent = []

    async def send_once(self, *, source_operation_id, text):
        self.sent.append((source_operation_id, text))


@pytest.mark.asyncio
async def test_semantic_supervision_detects_threshold_breaches() -> None:
    now = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    tasks = [
        TaskRecord(
            task_id="task-overdue",
            task_type="finite",
            status="in_progress",
            title="Overdue",
            assigned_to="alice",
            room_id="!alice:local",
            created_at=now - timedelta(hours=4),
            updated_at=now - timedelta(hours=3),
        ),
        TaskRecord(
            task_id="task-blocked",
            task_type="finite",
            status="blocked",
            title="Blocked",
            assigned_to="alice",
            room_id="!alice:local",
            created_at=now - timedelta(hours=2),
            updated_at=now - timedelta(hours=1),
        ),
        TaskRecord(
            task_id="task-ready",
            task_type="finite",
            status="ready",
            title="Needs capacity",
            assigned_to="alice",
            room_id="!alice:local",
            created_at=now,
            updated_at=now,
        ),
    ]
    workers = [
        WorkerResource(
            name="alice",
            runtime="qwenpaw",
            phase="Running",
            status={
                "lastHeartbeatAt": (
                    now - timedelta(hours=2)
                ).isoformat(),
                "containerState": "running",
            },
        ),
    ]
    notifications = Notifications()
    supervisor = SemanticSupervisor(
        tasks=Tasks(tasks),
        workers=Workers(workers),
        notifications=notifications,
        overdue_after=timedelta(hours=2),
        blocked_after=timedelta(minutes=30),
        worker_silence_after=timedelta(minutes=45),
    )

    report = await supervisor.inspect(now)

    assert {alert.kind for alert in report.alerts} == {
        "task_overdue",
        "project_blocker",
        "worker_nonresponsive",
        "capacity_shortage",
    }
    assert report.notified == 4
    assert all(key.startswith("supervision:") for key, _ in notifications.sent)
