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


class SupervisionState:
    def __init__(self):
        self.rows = {}

    async def record_ping(self, *, subject_key, observed_token, pinged_at):
        del pinged_at
        previous = self.rows.get(subject_key)
        missed = (
            int(previous["missed"]) + 1
            if previous is not None
            and previous["observed_token"] == observed_token
            else 0
        )
        self.rows[subject_key] = {
            "observed_token": observed_token,
            "missed": missed,
        }
        return missed


class Lifecycle:
    def __init__(self):
        self.woken = []

    async def wake_worker(self, name, *, context):
        self.woken.append((name, context))
        return WorkerResource(
            name=name,
            runtime="qwenpaw",
            phase="Running",
            room_id="!alice:local",
            matrix_user_id="@alice:local",
            status={"containerState": "running"},
        )


class Matrix:
    def __init__(self):
        self.sent = []

    async def send_text(
        self,
        room_id,
        text,
        *,
        txn_id,
        thread_id=None,
        mentions=(),
    ):
        del thread_id
        self.sent.append((room_id, text, txn_id, mentions))
        return "$heartbeat"


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


@pytest.mark.asyncio
async def test_semantic_supervision_wakes_and_pings_then_escalates() -> None:
    now = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
    task = TaskRecord(
        task_id="task-stalled",
        task_type="finite",
        status="in_progress",
        title="Stalled",
        assigned_to="alice",
        room_id="!alice:local",
        metadata={"matrix_user_id": "@alice:local"},
        created_at=now - timedelta(hours=4),
        updated_at=now - timedelta(hours=3),
    )
    worker = WorkerResource(
        name="alice",
        runtime="qwenpaw",
        phase="Stopped",
        room_id="!alice:local",
        matrix_user_id="@alice:local",
        status={
            "lastHeartbeatAt": (
                now - timedelta(hours=3)
            ).isoformat(),
            "containerState": "stopped",
        },
    )
    notifications = Notifications()
    state = SupervisionState()
    lifecycle = Lifecycle()
    matrix = Matrix()
    supervisor = SemanticSupervisor(
        tasks=Tasks([task]),
        workers=Workers([worker]),
        notifications=notifications,
        state=state,
        lifecycle=lifecycle,
        matrix=matrix,
        overdue_after=timedelta(hours=2),
        worker_silence_after=timedelta(minutes=45),
    )

    first = await supervisor.inspect(now)
    second = await supervisor.inspect(now + timedelta(minutes=30))

    assert first.woken == 1
    assert first.pinged == 1
    assert second.pinged == 1
    assert lifecycle.woken[0][0] == "alice"
    assert matrix.sent[0][0] == "!alice:local"
    assert matrix.sent[0][3] == ("@alice:local",)
    assert any(
        alert.kind == "task_unresponsive"
        for alert in second.alerts
    )
    assert any(
        "reassign_project_task" in text or "重新分配" in text
        for _, text in notifications.sent
    )
