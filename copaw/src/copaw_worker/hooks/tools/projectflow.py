"""CoPaw-native projectflow tool for AgentTeams project/DAG execution."""

# 初学者导读：DAG 是带依赖关系的任务图，例如 B 只有在 A 完成后才可执行。本工具
# 在 Worker/Leader 工作区推进该图，并检查仍在运行的子任务，避免父任务提前完成。
# Manager 仍负责创建与治理正式 Project；这里是成员侧的执行与汇报状态。

from __future__ import annotations

import asyncio
import json
import os
import urllib.error
import urllib.request
from dataclasses import asdict
from pathlib import Path
from typing import Any

from agentscope.message import TextBlock
from agentscope.tool import ToolResponse

from copaw_worker.task import (
    FileSystemTaskStore,
    TaskflowError,
    canonical_worker_id,
    complete_project,
    create_project,
    parse_dag_tasks,
    parse_loop_plan,
    parse_loop_tasks,
    parse_plan_type,
    pause_project,
    plan_dag,
    plan_loop,
    ready_loop_nodes,
    ready_nodes,
    record_loop_iteration,
    resume_project,
)


def _response(payload: dict[str, Any]) -> ToolResponse:
    # 逻辑说明：`_response` 接收 payload，执行 Project/DAG 工具 中的“响应”步骤，返回 ToolResponse；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；
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
    # 逻辑说明：`_workspace_dir` 接收 当前对象/进程状态，执行 Project/DAG 工具 中的“workspace dir”步骤，返回 Path；
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
    # 逻辑说明：`_store` 接收 当前对象/进程状态，执行 Project/DAG 工具 中的“store”步骤，返回 FileSystemTaskStore；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    return FileSystemTaskStore(_workspace_dir())


def _coerce_payload(payload: dict[str, Any] | str | None) -> dict[str, Any]:
    # 逻辑说明：规范化 Project/DAG 工具调用的 payload：JSON 字符串先解码，None 变为空字典，已有字典原样返回。
    # 解码失败或顶层不是对象时抛出 TaskflowError，阻止错误形状的数据进入项目状态存储；这里不产生文件副作用。
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
    # 逻辑说明：`_required_str` 接收 payload、key，执行 Project/DAG 工具 中的“必填 str”步骤，返回 str；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TaskflowError(f"payload.{key} is required")
    return value.strip()


def _optional_str(payload: dict[str, Any], key: str) -> str | None:
    # 逻辑说明：`_optional_str` 接收 payload、key，执行 Project/DAG 工具 中的“可选 str”步骤，返回 str | None；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TaskflowError(f"payload.{key} must be a string")
    return value


def _coerce_tasks(tasks: list[dict[str, Any]] | str | None) -> list[dict[str, Any]]:
    # 逻辑说明：`_coerce_tasks` 接收 tasks，把输入转换为tasks，返回 list[dict[str, Any]]；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    if isinstance(tasks, str):
        try:
            tasks = json.loads(tasks)
        except json.JSONDecodeError as exc:
            raise TaskflowError(f"tasks must be a JSON array: {exc.msg}") from exc
    if not isinstance(tasks, list) or not tasks:
        raise TaskflowError("tasks must be a non-empty list")
    if not all(isinstance(task, dict) for task in tasks):
        raise TaskflowError("tasks must be a list of objects")
    return tasks


def _coerce_optional_tasks(tasks: list[dict[str, Any]] | str | None) -> list[dict[str, Any]]:
    # 逻辑说明：`_coerce_optional_tasks` 接收 tasks，把输入转换为可选 tasks，返回 list[dict[str, Any]]；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    if tasks is None:
        return []
    return _coerce_tasks(tasks)


def _required_int(payload: dict[str, Any], key: str) -> int:
    # 逻辑说明：`_required_int` 接收 payload、key，执行 Project/DAG 工具 中的“必填 int”步骤，返回 int；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TaskflowError(f"payload.{key} must be an integer")
    return value


def _optional_int(payload: dict[str, Any], key: str, default: int) -> int:
    # 逻辑说明：`_optional_int` 接收 payload、key、default，执行 Project/DAG 工具 中的“可选 int”步骤，返回 int；
    # 不修改外部状态。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    value = payload.get(key)
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, int):
        raise TaskflowError(f"payload.{key} must be an integer")
    return value


def _list_project_ids(store: FileSystemTaskStore) -> list[str]:
    # 逻辑说明：`_list_project_ids` 接收 store，列出Project ids，返回 list[str]；不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    projects_dir = store.shared_dir / "projects"
    if not projects_dir.exists():
        return []
    return sorted(
        path.name
        for path in projects_dir.iterdir()
        if path.is_dir() and (path / "meta.json").exists()
    )


def _task_payload(task: Any) -> dict[str, Any]:
    return {
        "taskId": task.task_id,
        "title": task.title,
        "assignedTo": task.assigned_to,
        "dependsOn": task.depends_on,
        "planStatus": task.status,
    }


async def _fetch_worker_runtime_status(
    worker_name: str,
    *,
    timeout_seconds: int,
) -> dict[str, Any]:
    # 逻辑说明：`_fetch_worker_runtime_status` 接收 worker_name、timeout_seconds，在线程中请求目标 Worker /api/chats，统计运行会话并返回状态，
    # 返回 dict[str, Any]；
    #
    # 会访问网络服务。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    safe_worker = canonical_worker_id(worker_name)
    if not safe_worker:
        return {
            "runtimeStatus": "unknown",
            "runtimeStatusSource": "unconfigured",
            "error": "worker name is empty",
        }

    url = f"http://agentteams-worker-{safe_worker}:8088/api/chats"

    def _fetch() -> dict[str, Any]:
        # 逻辑说明：`_fetch` 接收 当前对象/进程状态，执行 Project/DAG 工具 中的“fetch”步骤，返回 dict[str, Any]；
        # 会访问网络服务。失败策略：非法输入或状态会立即抛错，其他底层异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        request = urllib.request.Request(url, headers={"X-Agent-Id": "default"})
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            data = json.loads(response.read().decode("utf-8"))
        if not isinstance(data, list):
            raise TaskflowError("worker /api/chats response must be a list")
        running = any(
            isinstance(item, dict) and item.get("status") == "running"
            for item in data
        )
        running_count = sum(
            1
            for item in data
            if isinstance(item, dict) and item.get("status") == "running"
        )
        return {
            "runtimeStatus": "running" if running else "idle",
            "runtimeStatusSource": url,
            "runningSessionCount": running_count,
            "sessionCount": len(data),
        }

    try:
        return await asyncio.to_thread(_fetch)
    except (
        OSError,
        TimeoutError,
        urllib.error.URLError,
        json.JSONDecodeError,
        TaskflowError,
    ) as exc:
        return {
            "runtimeStatus": "unknown",
            "runtimeStatusSource": url,
            "error": str(exc),
        }


async def _check_active_tasks(
    store: FileSystemTaskStore,
    *,
    project_id: str | None = None,
    timeout_seconds: int = 3,
) -> dict[str, Any]:
    # 逻辑说明：`_check_active_tasks` 接收 store、project_id、timeout_seconds，遍历活动 Project 与 delegated Task，
    # 汇总运行状态、结果缺失和阻塞问题，返回 dict[str, Any]；
    #
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    project_ids = [project_id] if project_id else _list_project_ids(store)
    issues: list[dict[str, Any]] = []
    checked_projects = 0

    for current_project_id in project_ids:
        if not current_project_id:
            continue
        meta = store.read_project_meta(current_project_id)
        if meta.status != "active":
            continue
        checked_projects += 1

        plan = store.read_project_plan(current_project_id)
        plan_type = parse_plan_type(plan)
        tasks = parse_loop_tasks(plan) if plan_type == "loop" else parse_dag_tasks(plan)
        project_issue_start = len(issues)
        for task in tasks:
            if task.status != "delegated":
                continue

            base = {
                "projectId": meta.project_id,
                "projectTitle": meta.title,
                "projectStatus": meta.status,
                "planType": plan_type,
                "taskId": task.task_id,
                "taskTitle": task.title,
                "assignedTo": task.assigned_to,
                "planStatus": task.status,
                "taskPath": f"shared/tasks/{task.task_id}/",
            }

            try:
                task_meta = store.read_task_meta(task.task_id)
                task_status = task_meta.status
            except TaskflowError:
                issues.append(
                    {
                        **base,
                        "taskStatus": "missing",
                        "issueType": "missing_task_meta",
                        "recommendation": "leader_repair_or_delegate_again",
                    }
                )
                continue

            try:
                result = store.read_task_result(task.task_id)
                issues.append(
                    {
                        **base,
                        "taskStatus": task_status,
                        "resultStatus": result.status,
                        "issueType": "task_result_pending_check",
                        "recommendation": "leader_check_submitted_result",
                    }
                )
                continue
            except TaskflowError as exc:
                task_dir = store.shared_dir / "tasks" / task.task_id
                if (task_dir / "result.md").exists():
                    issues.append(
                        {
                            **base,
                            "taskStatus": task_status,
                            "issueType": "invalid_task_result",
                            "error": str(exc),
                            "recommendation": "ask_worker_fix_result_protocol",
                        }
                    )
                    continue

            runtime = await _fetch_worker_runtime_status(
                task.assigned_to,
                timeout_seconds=timeout_seconds,
            )
            runtime_status = runtime.get("runtimeStatus")
            if runtime_status == "running":
                continue
            if runtime_status == "idle":
                issues.append(
                    {
                        **base,
                        "taskStatus": task_status,
                        **runtime,
                        "issueType": "task_not_running",
                        "recommendation": "ask_worker_continue_task",
                    }
                )
            else:
                issues.append(
                    {
                        **base,
                        "taskStatus": task_status,
                        **runtime,
                        "issueType": "worker_runtime_unknown",
                        "recommendation": "inspect_worker_runtime",
                    }
                )

        if len(issues) > project_issue_start:
            continue

        ready = (
            ready_loop_nodes(store, project_id=current_project_id)
            if plan_type == "loop"
            else ready_nodes(store, project_id=current_project_id)
        )
        if ready:
            issues.append(
                {
                    "projectId": meta.project_id,
                    "projectTitle": meta.title,
                    "projectStatus": meta.status,
                    "planType": plan_type,
                    "issueType": "ready_tasks_pending",
                    "readyTasks": [_task_payload(task) for task in ready],
                    "recommendation": "normal_leader_schedule_ready_tasks",
                }
            )
            continue

        if not tasks or not all(task.status == "completed" for task in tasks):
            continue

        if plan_type == "loop":
            loop = parse_loop_plan(plan)
            if loop is not None and loop.status == "running":
                issues.append(
                    {
                        "projectId": meta.project_id,
                        "projectTitle": meta.title,
                        "projectStatus": meta.status,
                        "planType": plan_type,
                        "issueType": "loop_iteration_decision_pending",
                        "iteration": loop.current_iteration,
                        "maxIterations": loop.max_iterations,
                        "recommendation": "normal_leader_record_loop_iteration",
                    }
                )
            continue

        issues.append(
            {
                "projectId": meta.project_id,
                "projectTitle": meta.title,
                "projectStatus": meta.status,
                "planType": plan_type,
                "issueType": "project_completion_pending",
                "recommendation": "normal_leader_aggregate_and_complete_project",
            }
        )

    return {
        "checkedProjects": checked_projects,
        "issues": issues,
    }


async def projectflow(
    action: str,
    payload: dict[str, Any] | str | None = None,
    dryRun: bool = False,
) -> ToolResponse:
    """Manage AgentTeams project execution plans with action-specific payload fields."""
    # 逻辑说明：`projectflow` 接收 action、payload、dryRun，解析 action 与 payload，执行 Project/DAG 操作并统一返回 ToolResponse，
    # 返回 ToolResponse；
    #
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    payload_data: dict[str, Any] = {}
    try:
        store = _store()
        payload_data = _coerce_payload(payload)

        if action == "create_project":
            project_id = _required_str(payload_data, "projectId")
            title = _required_str(payload_data, "title")
            if dryRun:
                return _ok(
                    dryRun=True,
                    action=action,
                    projectId=project_id,
                    title=title,
                )
            meta = create_project(
                store,
                project_id=project_id,
                title=title,
                source=_optional_str(payload_data, "source"),
                requester=_optional_str(payload_data, "requester"),
                parent_task_id=_optional_str(payload_data, "parentTaskId"),
            )
            return _ok(action=action, project=asdict(meta))

        if action == "plan_dag":
            project_id = _required_str(payload_data, "projectId")
            tasks_payload = _coerce_tasks(payload_data.get("tasks"))
            if dryRun:
                return _ok(dryRun=True, action=action, projectId=project_id, tasks=tasks_payload)
            graph = plan_dag(store, project_id=project_id, tasks=tasks_payload)
            ready = ready_nodes(store, project_id=project_id)
            return _ok(
                action=action,
                tasks=[asdict(task) for task in graph],
                readyNodes=[asdict(task) for task in ready],
            )

        if action == "plan_loop":
            project_id = _required_str(payload_data, "projectId")
            goal = _required_str(payload_data, "goal")
            stop_condition = _required_str(payload_data, "stopCondition")
            iteration_template = _required_str(payload_data, "iterationTemplate")
            max_iterations = _required_int(payload_data, "maxIterations")
            current_iteration = _optional_int(payload_data, "currentIteration", 0)
            status = str(payload_data.get("status") or "running")
            tasks_payload = _coerce_optional_tasks(payload_data.get("tasks"))
            if dryRun:
                return _ok(
                    dryRun=True,
                    action=action,
                    projectId=project_id,
                    goal=goal,
                    stopCondition=stop_condition,
                    iterationTemplate=iteration_template,
                    maxIterations=max_iterations,
                    currentIteration=current_iteration,
                    status=status,
                    tasks=tasks_payload,
                )
            loop = plan_loop(
                store,
                project_id=project_id,
                goal=goal,
                stop_condition=stop_condition,
                iteration_template=iteration_template,
                max_iterations=max_iterations,
                current_iteration=current_iteration,
                status=status,
                tasks=tasks_payload,
            )
            ready = ready_loop_nodes(store, project_id=project_id)
            return _ok(
                action=action,
                loop=asdict(loop),
                readyNodes=[asdict(task) for task in ready],
            )

        if action == "ready_nodes":
            project_id = _required_str(payload_data, "projectId")
            ready = ready_nodes(store, project_id=project_id)
            return _ok(action=action, readyNodes=[asdict(task) for task in ready])

        if action == "ready_loop_nodes":
            project_id = _required_str(payload_data, "projectId")
            ready = ready_loop_nodes(store, project_id=project_id)
            return _ok(action=action, readyNodes=[asdict(task) for task in ready])

        if action == "check_active_tasks":
            project_id = _optional_str(payload_data, "projectId")
            timeout_seconds = _optional_int(payload_data, "timeoutSeconds", 3)
            if timeout_seconds < 1:
                raise TaskflowError("payload.timeoutSeconds must be greater than zero")
            if dryRun:
                return _ok(
                    dryRun=True,
                    action=action,
                    projectId=project_id,
                    timeoutSeconds=timeout_seconds,
                )
            result = await _check_active_tasks(
                store,
                project_id=project_id,
                timeout_seconds=timeout_seconds,
            )
            return _ok(action=action, **result)

        if action == "record_loop_iteration":
            project_id = _required_str(payload_data, "projectId")
            iteration = _required_int(payload_data, "iteration")
            decision = _required_str(payload_data, "decision")
            summary = _required_str(payload_data, "summary")
            next_action = _optional_str(payload_data, "nextAction")
            if dryRun:
                return _ok(
                    dryRun=True,
                    action=action,
                    projectId=project_id,
                    iteration=iteration,
                    decision=decision,
                    summary=summary,
                    nextAction=next_action,
                )
            loop = record_loop_iteration(
                store,
                project_id=project_id,
                iteration=iteration,
                decision=decision,
                summary=summary,
                next_action=next_action,
            )
            return _ok(action=action, loop=asdict(loop))

        if action == "pause_project":
            project_id = _required_str(payload_data, "projectId")
            if dryRun:
                return _ok(
                    dryRun=True,
                    action=action,
                    projectId=project_id,
                )
            meta = pause_project(store, project_id=project_id)
            return _ok(action=action, project=asdict(meta))

        if action == "resume_project":
            project_id = _required_str(payload_data, "projectId")
            if dryRun:
                return _ok(
                    dryRun=True,
                    action=action,
                    projectId=project_id,
                )
            meta = resume_project(store, project_id=project_id)
            return _ok(action=action, project=asdict(meta))

        if action == "complete_project":
            project_id = _required_str(payload_data, "projectId")
            if dryRun:
                return _ok(
                    dryRun=True,
                    action=action,
                    projectId=project_id,
                )
            meta = complete_project(store, project_id=project_id)
            return _ok(action=action, project=asdict(meta))

        raise TaskflowError(
            "action must be one of: create_project, plan_dag, ready_nodes, "
            "plan_loop, ready_loop_nodes, record_loop_iteration, "
            "check_active_tasks, pause_project, resume_project, complete_project",
        )
    except TaskflowError as exc:
        return _error(
            str(exc),
            action=action,
            projectId=payload_data.get("projectId"),
        )
    except Exception as exc:  # pragma: no cover - defensive tool boundary
        return _error(
            f"projectflow failed: {exc}",
            action=action,
            projectId=payload_data.get("projectId"),
        )
