"""QwenPaw 与仓库级集成 Workflow 的静态接线契约。

测试把 YAML 当数据读取，确认路径触发器、镜像矩阵和测试分片都包含 QwenPaw。它不会真正构建镜像；
价值在于提前发现“代码存在，但 CI 从不构建/验收”的发布漏洞，真实运行由 test-26/27 补充。
"""

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "test-integration.yml"


def test_integration_workflow_runs_qwenpaw_like_copaw() -> None:
    workflow = yaml.load(
        WORKFLOW.read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )

    pull_request_paths = workflow["on"]["pull_request"]["paths"]
    push_paths = workflow["on"]["push"]["paths"]
    assert "qwenpaw/**" in pull_request_paths
    assert "qwenpaw/**" in push_paths

    build_targets = workflow["jobs"]["build-images"]["strategy"]["matrix"]["target"]
    assert "qwenpaw-worker" in build_targets

    matrix_step = next(
        step
        for step in workflow["jobs"]["detect-changes"]["steps"]
        if step.get("id") == "test-matrix"
    )
    matrix_script = matrix_step["run"]
    assert matrix_script.count('"worker_runtime":"qwenpaw"') == 3
    assert '"filter_env":"SHARD_B_TESTS"' not in matrix_script
    assert "SHARD_B_TESTS" not in workflow["env"]
    assert "14" not in workflow["env"]["NON_GITHUB_TESTS"].split()
    assert (
        '"shard":"qwenpaw-teamharness","filter_env":"SHARD_QWENPAW_TESTS",'
        '"manager_runtime":"agentscope","worker_runtime":"qwenpaw",'
        '"requires_secret":true'
    ) in matrix_script
    assert workflow["jobs"]["integration-tests"]["strategy"]["matrix"] == (
        "${{ fromJSON(needs.detect-changes.outputs.test_matrix) }}"
    )
