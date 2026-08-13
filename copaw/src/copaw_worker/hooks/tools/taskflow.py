"""CoPaw-native taskflow tool for AgentTeams task state."""

# 初学者导读：taskflow 把自然语言工作过程落成确定的 Task 状态转换。Leader 派工、
# Worker ack、完成或报告阻塞都需满足前置条件，状态和结果写入工作区后可随 MinIO
# 恢复。工具只操作当前任务协议，不能改变 Controller 中的 Team 成员或 Worker CR。

from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any

from agentscope.message import TextBlock
from agentscope.tool import ToolResponse

from copaw_worker.hooks.tools.filesync import create_sync
from copaw_worker.task import (
    RESULT_STATUSES,
    FileSystemTaskStore,
    TaskflowError,
    TaskMeta,
    TaskResult,
    ack_task,
    canonical_worker_id,
    check_task,
    delegate_task,
    is_effective_result,
    submit_task,
    validate_task_result,
)


def _response(payload: dict[str, Any]) -> ToolResponse:
    # 逻辑说明：`_response` 接收 payload，执行 Task 状态工具 中的“响应”步骤，返回 ToolResponse；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；
    # 本函数不额外重试，避免掩盖持续故障。
    return ToolResponse(
        content=[
            TextBlock(
                type="text",
                text=json.dumps(payload, ensure_ascii=False),
            ),
        ],
    )


def _ok(**payload: Any) -> ToolResponse:
    return _response({"ok": True, **payload})


def _error(message: str, **payload: Any) -> ToolResponse:
    return _response({"ok": False, "error": message, **payload})


def _workspace_dir() -> Path:
    # 逻辑说明：`_workspace_dir` 接收 当前对象/进程状态，执行 Task 状态工具 中的“workspace dir”步骤，返回 Path；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    configured = os.getenv("COPAW_WORKING_DIR")
    if configured:
        return Path(configured) / "workspaces" / "default"

    cwd = Path.cwd()
    if cwd.name == "default" and cwd.parent.name == "workspaces":
        return cwd
    if cwd.name == ".copaw":
        return cwd / "workspaces" / "default"
    return cwd


def _store() -> FileSystemTaskStore:
    # 逻辑说明：`_store` 接收 当前对象/进程状态，执行 Task 状态工具 中的“store”步骤，返回 FileSystemTaskStore；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    return FileSystemTaskStore(_workspace_dir())


def _runtime_root() -> Path:
    # 逻辑说明：`_runtime_root` 接收 当前对象/进程状态，执行 Task 状态工具 中的“runtime root”步骤，返回 Path；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    configured = os.getenv("COPAW_WORKING_DIR")
    if configured:
        return Path(configured).expanduser().resolve().parent

    workspace = _workspace_dir().resolve()
    if workspace.name == "default" and workspace.parent.name == "workspaces":
        copaw_dir = workspace.parent.parent
        if copaw_dir.name == ".copaw":
            return copaw_dir.parent
    return workspace


def _strip_yaml_string(value: str) -> str:
    # 逻辑说明：`_strip_yaml_string` 接收 value，执行 Task 状态工具 中的“strip yaml string”步骤，返回 str；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    text = value.strip()
    if not text or text in {"null", "~"}:
        return ""
    if "#" in text:
        text = text.split("#", 1)[0].strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _runtime_config_field(section: str, key: str) -> str:
    # 逻辑说明：`_runtime_config_field` 接收 section、key，执行 Task 状态工具 中的“runtime 配置 字段”步骤，返回 str；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    path = _runtime_root() / "runtime" / "runtime.yaml"
    if not path.exists():
        return ""

    in_section = False
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for raw_line in lines:
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        if not raw_line.startswith((" ", "\t")):
            in_section = raw_line.strip() == f"{section}:"
            continue
        if not in_section:
            continue
        stripped = raw_line.strip()
        if ":" not in stripped:
            continue
        field, value = stripped.split(":", 1)
        if field.strip() == key:
            return _strip_yaml_string(value)
    return ""


def _normalize_room_id(room_id: str) -> str:
    # 逻辑说明：`_normalize_room_id` 接收 room_id，规范化房间 ID，返回 str；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    text = (room_id or "").strip()
    if text.startswith("room:"):
        text = text[len("room:") :].strip()
    return text


def _room_target(room_id: str) -> str:
    # 逻辑说明：`_room_target` 接收 room_id，执行 Task 状态工具 中的“房间 target”步骤，返回 str；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；
    # 本函数不额外重试，避免掩盖持续故障。
    text = (room_id or "").strip()
    if text.startswith("room:"):
        return text
    return f"room:{text}"


def _require_team_leader_assignment_room(room_id: str) -> None:
    # 逻辑说明：`_require_team_leader_assignment_room` 接收 room_id，验证必需条件团队 Leader 派工 房间，返回 None；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    role = _runtime_config_field("member", "role")
    team_room_id = _runtime_config_field("team", "teamRoomId")
    if role != "team_leader" or not team_room_id:
        return

    if _normalize_room_id(room_id) != _normalize_room_id(team_room_id):
        raise TaskflowError(
            "team leader task assignments must use the Team Room "
            f"{_room_target(team_room_id)}, not {room_id}",
        )


def _current_actor() -> str | None:
    # 逻辑说明：`_current_actor` 接收 当前对象/进程状态，执行 Task 状态工具 中的“当前 actor”步骤，返回 str | None；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    configured = (
        os.getenv("AGENTTEAMS_MATRIX_USER_ID")
        or os.getenv("COPAW_MATRIX_USER_ID")
    )
    if configured:
        return configured.strip()

    try:
        from copaw.config.config import load_agent_config

        agent_config = load_agent_config("default")
        channels = _read_config_value(agent_config, "channels") or {}
        matrix_cfg = _read_config_value(channels, "matrix") or {}
        user_id = _read_config_value(matrix_cfg, "user_id", "userId")
        return str(user_id).strip() if user_id else None
    except Exception:
        return None


def _read_config_value(obj: Any, *names: str) -> Any:
    # 逻辑说明：按候选名称顺序从字典键或配置对象属性中读取首个值，用于兼容 Matrix user_id/userId 两种命名并确定任务操作者。
    # 所有名称都不存在时返回 None；只读取现有配置，不修改配置对象，也不自行加载磁盘配置。
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj.get(name)
        if hasattr(obj, name):
            return getattr(obj, name)
    return None


def _coerce_payload(payload: dict[str, Any] | str | None) -> dict[str, Any]:
    # 逻辑说明：把 Task 状态工具的可选 payload 统一为字典，允许调用方传对象、JSON 对象字符串或 None。
    # JSON 语法错误及数组等非对象顶层值会抛出 TaskflowError，避免后续任务字段读取产生含糊错误；转换过程没有持久化副作用。
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise TaskflowError(f"payload must be a JSON object: {exc.msg}") from exc
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise TaskflowError("payload must be an object")
    return payload


def _required_str(payload: dict[str, Any], key: str) -> str:
    # 逻辑说明：`_required_str` 接收 payload、key，执行 Task 状态工具 中的“必填 str”步骤，返回 str；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TaskflowError(f"payload.{key} is required")
    return value.strip()


def _optional_str(payload: dict[str, Any], key: str) -> str | None:
    # 逻辑说明：`_optional_str` 接收 payload、key，执行 Task 状态工具 中的“可选 str”步骤，返回 str | None；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TaskflowError(f"payload.{key} must be a string")
    return value


def _coerce_str_list(payload: dict[str, Any], key: str) -> list[str]:
    # 逻辑说明：`_coerce_str_list` 接收 payload、key，把输入转换为str list，返回 list[str]；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    value = payload.get(key)
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise TaskflowError(f"payload.{key} must be a JSON array: {exc.msg}") from exc
    if not isinstance(value, list):
        raise TaskflowError(f"payload.{key} must be a list")
    normalized = [str(item).strip() for item in value if str(item).strip()]
    return normalized


def _task_result_from_payload(payload: dict[str, Any]) -> TaskResult | None:
    # 逻辑说明：`_task_result_from_payload` 接收 payload，执行 Task 状态工具 中的“Task 结果 from 载荷”步骤，返回 TaskResult | None；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    result_keys = {"status", "summary", "deliverables", "notes"}
    if not any(key in payload for key in result_keys):
        return None

    status = _required_str(payload, "status")
    if status not in RESULT_STATUSES:
        raise TaskflowError(f"invalid result status: {status}")
    return TaskResult(
        status=status,
        summary=_required_str(payload, "summary"),
        deliverables=_coerce_str_list(payload, "deliverables"),
        notes=_coerce_str_list(payload, "notes"),
    )


def _require_ack_preconditions(meta: TaskMeta, actor: str | None) -> None:
    # 逻辑说明：`_require_ack_preconditions` 接收 meta、actor，验证必需条件ack preconditions，返回 None；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    current = canonical_worker_id(actor)
    if not current:
        raise TaskflowError("current worker identity is required")
    assigned = canonical_worker_id(meta.assigned_to)
    if current != assigned:
        raise TaskflowError(
            f"task {meta.task_id} is assigned to {meta.assigned_to}, not {current}",
        )
    if not (meta.room_id or "").strip():
        raise TaskflowError(f"task {meta.task_id} is missing room_id")


async def taskflow(
    action: str,
    payload: dict[str, Any] | str | None = None,
    dryRun: bool = False,
) -> ToolResponse:
    """Manage AgentTeams task state with action-specific payload fields."""
    # 逻辑说明：`taskflow` 接收 action、payload、dryRun，解析 action 与 payload，执行 Task 接收、检查或提交并统一返回 ToolResponse，返回 ToolResponse；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    payload_data: dict[str, Any] = {}
    try:
        store = _store()
        payload_data = _coerce_payload(payload)

        if action == "delegate_task":
            project_id = _required_str(payload_data, "projectId")
            task_id = _required_str(payload_data, "taskId")
            room_id = _required_str(payload_data, "roomId")
            spec = _required_str(payload_data, "spec")
            _require_team_leader_assignment_room(room_id)
            if dryRun:
                return _ok(
                    dryRun=True,
                    action=action,
                    projectId=project_id,
                    taskId=task_id,
                )
            meta = delegate_task(
                store,
                project_id=project_id,
                task_id=task_id,
                spec=spec,
                room_id=room_id,
            )
            task_path = f"shared/tasks/{task_id}/"
            sync = create_sync()
            sync.push_shared_path(task_path)
            return _ok(action=action, task=asdict(meta), synced=True)

        if action == "check_task":
            task_id = _required_str(payload_data, "taskId")
            if dryRun:
                return _ok(dryRun=True, action=action, taskId=task_id)
            task_path = f"shared/tasks/{task_id}/"
            sync = create_sync()
            sync.pull_shared_path(task_path)
            meta = store.read_task_meta(task_id)
            result = check_task(store, task_id=task_id)
            return _ok(
                action=action,
                task=asdict(meta),
                result=asdict(result),
                effective=is_effective_result(result),
            )

        if action == "ack_task":
            task_id = _required_str(payload_data, "taskId")
            if dryRun:
                return _ok(dryRun=True, action=action, taskId=task_id)
            task_path = f"shared/tasks/{task_id}/"
            sync = create_sync()
            sync.pull_shared_path(task_path)
            actor = _current_actor()
            _require_ack_preconditions(store.read_task_meta(task_id), actor)
            spec = store.read_task_spec(task_id)
            meta = ack_task(store, task_id=task_id, actor=actor)
            sync.push_shared_path(task_path, exclude=["spec.md", "base/"])
            return _ok(action=action, task=asdict(meta), spec=spec)

        if action == "submit_task":
            task_id = _required_str(payload_data, "taskId")
            result = _task_result_from_payload(payload_data)
            if result is not None:
                validate_task_result(task_id, result)
            if dryRun:
                dry_run_payload: dict[str, Any] = {
                    "dryRun": True,
                    "action": action,
                    "taskId": task_id,
                }
                if result is not None:
                    dry_run_payload["result"] = asdict(result)
                return _ok(**dry_run_payload)
            meta = submit_task(store, task_id=task_id, result=result, actor=_current_actor())
            task_path = f"shared/tasks/{task_id}/"
            result_path = f"shared/tasks/{task_id}/result.md"
            sync = create_sync()
            sync.push_shared_path(task_path, exclude=["spec.md", "base/"])
            sync.stat_shared_path(result_path)
            response_payload: dict[str, Any] = {
                "action": action,
                "task": asdict(meta),
                "synced": True,
                "verified": True,
            }
            if result is not None:
                response_payload["result"] = asdict(result)
            return _ok(**response_payload)

        raise TaskflowError(
            "action must be one of: delegate_task, check_task, ack_task, submit_task",
        )
    except TaskflowError as exc:
        return _error(
            str(exc),
            action=action,
            projectId=payload_data.get("projectId"),
            taskId=payload_data.get("taskId"),
        )
    except Exception as exc:  # pragma: no cover - defensive runtime boundary
        return _error(
            f"taskflow failed: {exc}",
            action=action,
            projectId=payload_data.get("projectId"),
            taskId=payload_data.get("taskId"),
        )
