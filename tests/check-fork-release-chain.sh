#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD=(python)
elif command -v py >/dev/null 2>&1; then
    PYTHON_CMD=(py -3.12)
else
    echo "Python 3 is required" >&2
    exit 1
fi

PROJECT_ROOT="${PROJECT_ROOT}" "${PYTHON_CMD[@]}" - <<'PY'
from __future__ import annotations

import json
import os
import re
from pathlib import Path

root = Path(os.environ["PROJECT_ROOT"])

workflow_paths = [
    root / ".github/workflows/build.yml",
    root / ".github/workflows/build-rc.yml",
    root / ".github/workflows/release.yml",
    root / ".github/workflows/test-integration.yml",
]
workflow_text = "\n".join(
    path.read_text(encoding="utf-8") for path in workflow_paths
)
folded_workflows = workflow_text.casefold()

assert "openhuman" not in folded_workflows, (
    "release/build/integration workflows still reference the removed "
    "OpenHuman image"
)
assert re.search(r"(?m)^\s*REGISTRY:\s*ghcr\.io\s*$", workflow_text)
assert re.search(r"(?m)^\s*REPO:\s*jesseedcp\s*$", workflow_text)
assert re.search(r"(?m)^\s*packages:\s*write\s*$", workflow_text)
assert "${{ github.actor }}" in workflow_text
assert "${{ secrets.GITHUB_TOKEN }}" in workflow_text
assert not re.search(
    r"higress-registry\.[^\s'\"]+/agentteams/(?:agentteams-)?"
    r"(?:manager|controller|worker)",
    workflow_text,
)

makefile = (root / "Makefile").read_text(encoding="utf-8")
assert re.search(r"(?m)^REGISTRY\s*\?=\s*ghcr\.io\s*$", makefile)
assert re.search(r"(?m)^REPO\s*\?=\s*jesseedcp\s*$", makefile)
assert "openhuman" not in makefile.casefold()

defined_targets = set(
    re.findall(r"(?m)^([a-zA-Z0-9_.-]+)\s*:(?!=)", makefile)
)
called_push_targets = set(
    re.findall(r"\bmake\s+(push-[a-z0-9-]+)\b", workflow_text)
)
for target in sorted(called_push_targets):
    assert target in defined_targets, f"workflow calls missing Make target: {target}"

bash_installer = (
    root / "install/agentteams-install.sh"
).read_text(encoding="utf-8")
ps_installer = (
    root / "install/agentteams-install.ps1"
).read_text(encoding="utf-8")
for installer in (bash_installer, ps_installer):
    assert "AGENTTEAMS_RELEASE_REPOSITORY" in installer
    assert "jesseedcp/AgentTeams" in installer
    assert "AGENTTEAMS_IMAGE_REPOSITORY" in installer
    assert "ghcr.io" in installer

assert (
    '${AGENTTEAMS_REGISTRY}/${AGENTTEAMS_IMAGE_REPOSITORY}'
    in bash_installer
)
assert (
    '$($script:AGENTTEAMS_REGISTRY)/'
    '$($script:AGENTTEAMS_IMAGE_REPOSITORY)/agentteams-manager'
    in ps_installer
)
assert (
    "api.github.com/repos/$($script:AGENTTEAMS_RELEASE_REPOSITORY)"
    "/releases/latest"
    in ps_installer
)
assert "Resolve-AgentTeamsVersion" in ps_installer

assert (
    "api.github.com/repos/agentscope-ai/AgentTeams/releases/latest"
    not in bash_installer
)
assert (
    "api.github.com/repos/agentscope-ai/AgentTeams/releases/latest"
    not in ps_installer
)

helm_values = (root / "helm/agentteams/values.yaml").read_text(
    encoding="utf-8"
)
for image in (
    "agentteams-controller",
    "agentteams-manager",
    "agentteams-worker",
    "agentteams-copaw-worker",
    "agentteams-hermes-worker",
    "agentteams-qwenpaw-worker",
):
    assert f"repository: ghcr.io/jesseedcp/{image}" in helm_values

contract = json.loads(
    (
        root
        / "manager-agentscope/tests/contract/fixtures/upstream-agentteams.json"
    ).read_text(encoding="utf-8")
)
assert contract["manager"]["upstreamRuntimes"] == [
    "openclaw",
    "copaw",
    "hermes",
]
assert contract["manager"]["localRuntime"] == "agentscope"
assert contract["worker"]["upstreamDistributions"] == [
    "openclaw",
    "copaw",
    "hermes",
    "qwenpaw",
    "openhuman",
]
assert contract["worker"]["localRuntimes"] == [
    "openclaw",
    "copaw",
    "hermes",
    "qwenpaw",
]
assert contract["worker"]["intentionallyRemoved"] == ["openhuman"]

print("fork release chain contract passed")
PY
