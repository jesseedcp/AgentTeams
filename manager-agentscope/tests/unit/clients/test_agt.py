from __future__ import annotations

import pytest
from pydantic import ValidationError

from agentteams_manager.clients.agt import (
    AgtClient,
    HumanCreateRequest,
    HumanUpdateRequest,
    TeamCreateRequest,
    WorkerCreateRequest,
    WorkerUpdateRequest,
)
from tests.fixtures.fake_agt import FakeProcess


def _worker(name: str = "alice", runtime: str = "qwenpaw") -> dict:
    return {
        "name": name,
        "phase": "Running",
        "model": "qwen3.6-plus",
        "runtime": runtime,
        "image": f"agentteams-worker:{runtime}",
        "containerState": "running",
        "matrixUserID": f"@worker-{name}:matrix.local",
        "roomID": f"!{name}:matrix.local",
        "message": "",
        "team": "",
        "role": "",
        "identity": "Release specialist",
        "soul": "Ship carefully.",
        "skills": ["git", "review"],
        "package": "oss://workers/release.zip",
        "expose": [{"port": 8080}],
    }


@pytest.mark.asyncio
async def test_get_worker_uses_json_and_parses_runtime() -> None:
    process = FakeProcess()
    process.queue_json(_worker())
    client = AgtClient(process)

    worker = await client.get_worker("alice")

    assert worker is not None
    assert worker.runtime == "qwenpaw"
    assert worker.skills == ("git", "review")
    assert worker.spec["identity"] == "Release specialist"
    assert worker.spec["package"] == "oss://workers/release.zip"
    assert worker.spec["expose"] == [8080]
    assert process.argv == (
        "agt",
        "get",
        "workers",
        "alice",
        "-o",
        "json",
    )


@pytest.mark.asyncio
async def test_get_worker_normalizes_not_found() -> None:
    process = FakeProcess()
    process.queue_error("get worker: HTTP 404: worker not found")

    assert await AgtClient(process).get_worker("missing") is None


@pytest.mark.asyncio
async def test_list_workers_parses_only_typed_resources() -> None:
    process = FakeProcess()
    process.queue_json({"workers": [_worker(), _worker("bob", "hermes")], "total": 2})

    workers = await AgtClient(process).list_workers()

    assert [worker.name for worker in workers] == ["alice", "bob"]
    assert process.argv == ("agt", "get", "workers", "-o", "json")


@pytest.mark.asyncio
async def test_create_and_update_worker_map_only_known_flags() -> None:
    process = FakeProcess()
    process.queue_json({"name": "alice", "accepted": True})
    process.queue_error("", returncode=0)
    process.queue_json(_worker(runtime="copaw"))
    client = AgtClient(process)

    await client.create_worker(
        WorkerCreateRequest(
            name="alice",
            runtime="copaw",
            model="qwen3.6-plus",
            skills=("git", "review"),
        ),
    )
    assert process.argv == (
        "agt",
        "create",
        "worker",
        "--name",
        "alice",
        "--model",
        "qwen3.6-plus",
        "--runtime",
        "copaw",
        "--skills",
        "git,review",
        "--no-wait",
        "-o",
        "json",
    )

    worker = await client.update_worker(
        WorkerUpdateRequest(name="alice", model="qwen3.6-plus"),
    )
    assert worker.runtime == "copaw"
    assert process.calls[-2][0] == (
        "agt",
        "update",
        "worker",
        "--name",
        "alice",
        "--model",
        "qwen3.6-plus",
    )


@pytest.mark.asyncio
async def test_update_worker_preserves_explicit_empty_arrays() -> None:
    process = FakeProcess()
    process.queue_error("", returncode=0)
    updated = _worker(runtime="copaw")
    updated["skills"] = []
    updated["expose"] = []
    process.queue_json(updated)

    worker = await AgtClient(process).update_worker(
        WorkerUpdateRequest(
            name="alice",
            skills=(),
            expose=(),
        ),
    )

    assert worker.skills == ()
    assert worker.spec["expose"] == []
    assert process.calls[-2][0] == (
        "agt",
        "update",
        "worker",
        "--name",
        "alice",
        "--skills",
        "",
        "--expose",
        "",
    )


@pytest.mark.parametrize(
    "package_uri",
    (
        "nacos://admin:password@registry.example/public/coder/v1",
        "https://packages.example/coder.zip?X-Amz-Signature=secret",
    ),
)
def test_worker_requests_reject_credential_bearing_package_uris(
    package_uri: str,
) -> None:
    with pytest.raises(ValidationError, match="credentials"):
        WorkerCreateRequest(
            name="alice",
            runtime="copaw",
            model="qwen3.6-plus",
            package_uri=package_uri,
        )


@pytest.mark.asyncio
async def test_apply_worker_package_binds_expected_digest_to_uri() -> None:
    process = FakeProcess()
    process.queue_error("", returncode=0)
    process.queue_json(_worker(name="imported", runtime="hermes"))
    client = AgtClient(process)
    digest = "sha256:" + ("a" * 64)

    worker = await client.apply_worker_package(
        name="imported",
        package_uri=(
            "nacos://registry.example:8848/public/remote-coder/1.4.0"
        ),
        expected_digest=digest,
        runtime="hermes",
    )

    assert worker.name == "imported"
    assert process.calls[-2][0] == (
        "agt",
        "apply",
        "worker",
        "--name",
        "imported",
        "--package",
        (
            "nacos://registry.example:8848/public/remote-coder/1.4.0"
            "?expectedDigest=sha256%3A"
            + ("a" * 64)
        ),
        "--runtime",
        "hermes",
    )


@pytest.mark.asyncio
async def test_team_and_human_outputs_map_controller_fields() -> None:
    process = FakeProcess()
    process.queue_json(
        {
            "name": "alpha",
            "phase": "Running",
            "leaderName": "alpha-lead",
            "workerNames": ["alpha-dev"],
            "teamRoomID": "!team:local",
            "leaderDMRoomID": "!leader-dm:local",
            "leaderReady": True,
            "readyWorkers": 1,
            "totalWorkers": 1,
        },
    )
    process.queue_json(
        {
            "name": "reviewer",
            "phase": "Running",
            "displayName": "Reviewer",
            "email": "reviewer@example.com",
            "matrixUserID": "@reviewer:local",
            "rooms": ["!alpha-lead:local"],
            "permissionLevel": 2,
            "accessibleTeams": ["alpha"],
            "accessibleWorkers": ["alpha-dev"],
            "note": "release reviewer",
        },
    )
    client = AgtClient(process)

    team = await client.get_team("alpha")
    human = await client.get_human("reviewer")

    assert team is not None
    assert team.leader == "alpha-lead"
    assert team.spec["teamRoomID"] == "!team:local"
    assert human is not None
    assert human.permission_level == 2
    assert human.allowed_rooms == ("!alpha-lead:local",)
    assert human.spec["email"] == "reviewer@example.com"
    assert human.spec["accessibleWorkers"] == ["alpha-dev"]


@pytest.mark.asyncio
async def test_create_team_and_human_use_typed_argv() -> None:
    process = FakeProcess()
    process.queue_json({"name": "alpha"})
    process.queue_error("", returncode=0)
    client = AgtClient(process)

    await client.create_team(
        TeamCreateRequest(
            name="alpha",
            leader_name="alpha-lead",
            workers=("dev", "qa"),
        ),
    )
    assert process.argv[:5] == (
        "agt",
        "create",
        "team",
        "--name",
        "alpha",
    )

    await client.create_human(
        HumanCreateRequest(
            name="reviewer",
            display_name="Reviewer",
            email="reviewer@example.com",
            permission_level=2,
            accessible_teams=("alpha",),
        ),
    )
    assert process.argv == (
        "agt",
        "create",
        "human",
        "--name",
        "reviewer",
        "--display-name",
        "Reviewer",
        "--email",
        "reviewer@example.com",
        "--permission-level",
        "2",
        "--accessible-teams",
        "alpha",
    )


@pytest.mark.asyncio
async def test_update_human_supports_scope_changes_and_explicit_clears() -> None:
    process = FakeProcess()
    process.queue_error("", returncode=0)
    process.queue_json(
        {
            "name": "reviewer",
            "phase": "Active",
            "displayName": "Reviewer",
            "email": "",
            "matrixUserID": "@reviewer:local",
            "rooms": [],
            "permissionLevel": 3,
            "accessibleTeams": [],
            "accessibleWorkers": ["alpha-dev"],
            "note": "",
        },
    )

    human = await AgtClient(process).update_human(
        HumanUpdateRequest(
            name="reviewer",
            email="",
            permission_level=3,
            accessible_teams=(),
            accessible_workers=("alpha-dev",),
            note="",
        ),
    )

    assert human.permission_level == 3
    assert process.calls[-2][0] == (
        "agt",
        "update",
        "human",
        "--name",
        "reviewer",
        "--email",
        "",
        "--permission-level",
        "3",
        "--accessible-teams",
        "",
        "--accessible-workers",
        "alpha-dev",
        "--note",
        "",
    )
