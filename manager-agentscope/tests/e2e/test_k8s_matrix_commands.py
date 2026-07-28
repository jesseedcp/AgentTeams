"""Live Matrix command and optional confirmed tool-call acceptance."""

from __future__ import annotations

import os
import re
import uuid
from pathlib import Path

from conftest import K8sHarness

ROOT = Path(__file__).resolve().parents[3]


def test_matrix_session_commands_reply_without_model_turn(
    k8s_harness: K8sHarness,
) -> None:
    if not k8s_harness.enabled:
        _assert_static_matrix_contract()
        return

    token, room_id, manager_user_id = k8s_harness.matrix_context()
    cases = (
        ("/help", lambda body: "会话命令" in body and "/commands" in body),
        ("/status", lambda body: "会话状态：" in body),
        ("/model status", lambda body: "当前会话模型：" in body),
        (
            "/stop",
            lambda body: (
                "当前没有正在运行的任务" in body
                or "已停止当前任务" in body
            ),
        ),
    )
    for command, predicate in cases:
        started_at = k8s_harness.matrix_send(token, room_id, command)
        reply = k8s_harness.matrix_wait_for_reply(
            token,
            room_id,
            manager_user_id,
            started_at,
            predicate,
        )
        assert predicate(reply)


def test_confirmed_matrix_tool_call_changes_controller_state_when_enabled(
    k8s_harness: K8sHarness,
) -> None:
    if not k8s_harness.enabled:
        _assert_static_matrix_contract()
        return
    if os.environ.get("AGENTTEAMS_E2E_LLM") != "1":
        return

    token, room_id, manager_user_id = k8s_harness.matrix_context()
    suffix = uuid.uuid4().hex[:8]
    worker_name = f"e2e-matrix-{suffix}"
    model = str(
        k8s_harness.kubectl_json("get", "manager", "default")["spec"][
            "model"
        ],
    )
    try:
        elevated_at = k8s_harness.matrix_send(
            token,
            room_id,
            "/elevated ask",
        )
        k8s_harness.matrix_wait_for_reply(
            token,
            room_id,
            manager_user_id,
            elevated_at,
            lambda body: "elevated 确认策略已设置为：ask" in body,
        )

        prompt_at = k8s_harness.matrix_send(
            token,
            room_id,
            (
                "请立即调用 create_worker 工具创建 Worker，"
                f"name={worker_name}，runtime=qwenpaw，model={model}。"
                "不要只给说明，必须执行工具。"
            ),
        )
        approval = k8s_harness.matrix_wait_for_reply(
            token,
            room_id,
            manager_user_id,
            prompt_at,
            lambda body: (
                "需要管理员批准" in body
                and worker_name in body
                and "/confirm " in body
            ),
            timeout=300,
        )
        match = re.search(r"/confirm ([0-9a-f]{32})", approval)
        assert match is not None, approval
        confirmation_id = match.group(1)

        confirm_at = k8s_harness.matrix_send(
            token,
            room_id,
            f"/confirm {confirmation_id}",
        )
        k8s_harness.matrix_wait_for_reply(
            token,
            room_id,
            manager_user_id,
            confirm_at,
            lambda body: "管理员已批准该操作" in body,
            timeout=300,
        )
        created = k8s_harness.wait(
            lambda: k8s_harness.try_kubectl_json(
                "get",
                "worker",
                worker_name,
            ),
            timeout=300,
            description=f"confirmed Matrix tool creation of worker/{worker_name}",
        )
        assert created["spec"]["runtime"] == "qwenpaw"
    finally:
        k8s_harness.kubectl(
            "delete",
            "worker",
            worker_name,
            "--ignore-not-found=true",
            check=False,
        )


def _assert_static_matrix_contract() -> None:
    commands = (
        ROOT
        / "manager-agentscope"
        / "src"
        / "agentteams_manager"
        / "matrix"
        / "commands.py"
    ).read_text(encoding="utf-8")
    runner = (
        ROOT
        / "manager-agentscope"
        / "src"
        / "agentteams_manager"
        / "matrix"
        / "session_runner.py"
    ).read_text(encoding="utf-8")
    for command in (
        '"/model"',
        '"/models"',
        '"/help"',
        '"/commands"',
        '"/stop"',
        '"/think"',
        '"/reasoning"',
        '"/queue"',
    ):
        assert command in commands
    assert '"/confirm": "confirm"' in runner
    assert "UserConfirmResultEvent" in runner
