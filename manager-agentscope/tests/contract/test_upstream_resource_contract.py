"""Differential guard pinned to the latest audited official AgentTeams HEAD."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(".")
FIXTURE = Path(__file__).parent / "fixtures" / "upstream-agentteams.json"


def test_pinned_upstream_resource_contract_matches_local_sources() -> None:
    contract = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert contract["commit"] == (
        "8de237da736a542766e132836b29c0a2a9c48740"
    )

    crd = (
        ROOT
        / "agentteams-controller"
        / "config"
        / "crd"
        / "teams.agentteams.io.yaml"
    ).read_text(encoding="utf-8")
    types = (
        ROOT
        / "agentteams-controller"
        / "api"
        / "v1beta1"
        / "types.go"
    ).read_text(encoding="utf-8")
    reconciler = (
        ROOT
        / "agentteams-controller"
        / "internal"
        / "controller"
        / "team_controller.go"
    ).read_text(encoding="utf-8")

    assert "required: [workerMembers]" in crd
    assert 'WorkerMembers []TeamWorkerRef `json:"workerMembers,omitempty"`' in (
        types
    )
    for legacy in contract["team"]["legacyEmbeddedFields"]:
        assert f"{legacy}:" not in _team_spec_schema(crd)
    assert (
        "handleDeleteTeam removes Team-owned state while preserving "
        "referenced Workers"
    ) in reconciler
    assert "DeleteWorker(" not in _delete_handler(reconciler)


def test_cli_rejects_legacy_embedded_team_runtime_shape() -> None:
    create = (
        ROOT / "agentteams-controller" / "cmd" / "agt" / "create.go"
    ).read_text(encoding="utf-8")
    update_test = (
        ROOT / "agentteams-controller" / "cmd" / "agt" / "update_test.go"
    ).read_text(encoding="utf-8")
    assert '"workerMembers": workerMembers' in create
    assert "TestUpdateTeamRejectsLegacyEmbeddedRuntimeFlags" in update_test


def test_upstream_and_local_runtime_sets_are_distinguished() -> None:
    contract = json.loads(FIXTURE.read_text(encoding="utf-8"))
    backend = (
        ROOT
        / "agentteams-controller"
        / "internal"
        / "backend"
        / "interface.go"
    ).read_text(encoding="utf-8")
    assert contract["manager"]["upstreamRuntimes"] == [
        "openclaw",
        "copaw",
        "hermes",
    ]
    assert contract["manager"]["localRuntime"] == "agentscope"
    assert contract["manager"]["replacement"] == "intentional"
    assert contract["worker"]["upstreamDistributions"] == [
        "openclaw",
        "copaw",
        "hermes",
        "qwenpaw",
        "openhuman",
    ]
    assert contract["worker"]["intentionallyRemoved"] == ["openhuman"]
    for runtime in contract["worker"]["localRuntimes"]:
        assert f'= "{runtime}"' in backend
    for runtime in contract["worker"]["intentionallyRemoved"]:
        assert runtime not in backend.casefold()


def test_intentional_replacements_are_explicit() -> None:
    contract = json.loads(FIXTURE.read_text(encoding="utf-8"))
    differences = contract["intentionalDifferences"]
    assert set(differences) == {
        "managerRuntime",
        "webClient",
        "stateStore",
        "removedWorker",
    }
    assert "AgentScope 2.0" in differences["managerRuntime"]
    assert "Cinny" in differences["webClient"]
    assert "SQLite" in differences["stateStore"]
    assert "OpenHuman" in differences["removedWorker"]


def test_latest_dashboard_delta_is_classified_as_replaced() -> None:
    contract = json.loads(FIXTURE.read_text(encoding="utf-8"))
    delta = contract["latestUpstreamDelta"]
    assert delta["commit"] == contract["commit"]
    assert set(delta["scope"]) == set(delta["localDisposition"])
    assert "built-in Manager admin console" in (
        delta["localDisposition"]["dashboardQuickStart"]
    )
    assert "Worker CRDs and Kubernetes Secrets" in (
        delta["localDisposition"]["legacyWorkerCredentialExtraction"]
    )


def _team_spec_schema(crd: str) -> str:
    start = crd.index("              required: [workerMembers]")
    end = crd.index("          status:", start)
    return crd[start:end]


def _delete_handler(source: str) -> str:
    start = source.index("func (r *TeamReconciler) handleDeleteTeam")
    end = source.index(
        "func (r *TeamReconciler) teamLeaderRuntimeName",
        start,
    )
    return source[start:end]
