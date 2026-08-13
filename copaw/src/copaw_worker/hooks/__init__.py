"""用于把上游 CoPaw 行为约束到 AgentTeams Worker 边界的运行时 hooks。

这些 hooks 处理凭据、消息和工具结果等跨边界数据；它们不改变 Manager 的审批或
编排策略。集中安装能确保每种 CoPaw 启动模式得到相同保护。

Runtime hooks for adapting upstream CoPaw behavior to AgentTeams.
"""

from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any

logger = logging.getLogger(__name__)

_TOOL_HOOK_INSTALLED = False
_MESSAGE_FILTER_HOOK_INSTALLED = False


def _builtin_tool_config(agent: Any, name: str) -> Any | None:
    # 逻辑说明：`_builtin_tool_config` 接收 agent、name，执行 CoPaw hook 与工具注册 中的“builtin 工具 配置”步骤，返回 Any | None；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    try:
        builtin_tools = agent._agent_config.tools.builtin_tools
        return builtin_tools.get(name)
    except Exception:
        return None


def _builtin_tool_enabled(agent: Any, name: str) -> bool:
    # 逻辑说明：`_builtin_tool_enabled` 接收 agent、name，执行 CoPaw hook 与工具注册 中的“builtin 工具 enabled”步骤，返回 bool；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    config = _builtin_tool_config(agent, name)
    return bool(getattr(config, "enabled", True))


def _builtin_tool_async_execution(agent: Any, name: str) -> bool:
    # 逻辑说明：`_builtin_tool_async_execution` 接收 agent、name，执行 CoPaw hook 与工具注册 中的“builtin 工具 async execution”步骤，返回 bool；
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    config = _builtin_tool_config(agent, name)
    return bool(getattr(config, "async_execution", False))


def _register_tool_function(toolkit: Any, func: Any, **kwargs: Any) -> None:
    # 逻辑说明：`_register_tool_function` 接收 toolkit、func、**kwargs，执行 CoPaw hook 与工具注册 中的“register 工具 function”步骤，返回 None；
    # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
    try:
        toolkit.register_tool_function(func, **kwargs)
    except TypeError:
        fallback = dict(kwargs)
        fallback.pop("async_execution", None)
        toolkit.register_tool_function(func, **fallback)


def _existing_tool_schema(toolkit: Any, name: str) -> dict[str, Any] | None:
    # 逻辑说明：`_existing_tool_schema` 接收 toolkit、name，执行 CoPaw hook 与工具注册 中的“existing 工具 schema”步骤，
    # 返回 dict[str, Any] | None；
    #
    # 不修改外部状态。失败策略：底层异常继续上抛，由调用链统一处理；本函数不额外重试，避免掩盖持续故障。
    tool = getattr(toolkit, "tools", {}).get(name)
    schema = getattr(tool, "json_schema", None)
    return deepcopy(schema) if isinstance(schema, dict) else None


def install_message_filter_hooks() -> None:
    """Leave Matrix reply filtering to final send/tool boundaries."""
    # 逻辑说明：把模块级标志设为已处理并记录 query-handler 过滤已禁用；重复调用幂等返回，因为 Matrix 回复过滤由最终发送与工具边界负责。
    global _MESSAGE_FILTER_HOOK_INSTALLED
    if _MESSAGE_FILTER_HOOK_INSTALLED:
        return

    _MESSAGE_FILTER_HOOK_INSTALLED = True
    logger.info("AgentTeams CoPaw query-handler message filter hook is disabled")


def install_tool_hooks() -> None:
    """Install AgentTeams-owned CoPaw tool hooks.

    CoPaw creates a temporary CoPawAgent for every query, and each agent
    builds a fresh toolkit. Hooking _create_toolkit lets AgentTeams inject tools
    without modifying upstream CoPaw files.
    """
    # 逻辑说明：一次性替换 CoPawAgent 的 toolkit 工厂，使每个临时 Agent 都注册 AgentTeams message/filesync/projectflow/taskflow 工具、输出脱敏中间件与凭据拦截。
    # 各工具或中间件注册失败只记录日志并保留原 toolkit；补丁带 marker 且模块有安装标志，避免重复包装上游方法。
    global _TOOL_HOOK_INSTALLED
    install_message_filter_hooks()

    if _TOOL_HOOK_INSTALLED:
        return

    from copaw.agents.react_agent import CoPawAgent

    from copaw_worker.hooks.credential_guard import install_credential_guard_hook
    from copaw_worker.hooks.output_sanitizer import create_sanitizer_middleware
    from copaw_worker.hooks.tools.filesync import filesync
    from copaw_worker.hooks.tools.message import message
    from copaw_worker.hooks.tools.projectflow import projectflow
    from copaw_worker.hooks.tools.taskflow import taskflow

    original_create_toolkit = CoPawAgent._create_toolkit
    if getattr(original_create_toolkit, "_agentteams_message_hook", False):
        _TOOL_HOOK_INSTALLED = True
        return

    def create_toolkit_with_agentteams_tools(self: Any, *args: Any, **kwargs: Any):
        # 逻辑说明：`create_toolkit_with_agentteams_tools` 接收 *args、**kwargs，创建toolkit with agentteams tools，返回 约定结果；
        # 不修改外部状态。失败策略：已知失败按现有 except 转为错误结果或日志，未覆盖异常继续上抛；本函数不额外重试，避免掩盖持续故障。
        toolkit = original_create_toolkit(self, *args, **kwargs)
        try:
            _register_tool_function(
                toolkit,
                message,
                namesake_strategy="override",
            )
            logger.debug("Registered AgentTeams CoPaw message tool")
            _register_tool_function(
                toolkit,
                filesync,
                namesake_strategy="override",
            )
            logger.debug("Registered AgentTeams CoPaw filesync tool")
            _register_tool_function(
                toolkit,
                projectflow,
                namesake_strategy="override",
            )
            logger.debug("Registered AgentTeams CoPaw projectflow tool")
            _register_tool_function(
                toolkit,
                taskflow,
                namesake_strategy="override",
            )
            logger.debug("Registered AgentTeams CoPaw taskflow tool")
        except Exception:
            logger.exception("Failed to register AgentTeams CoPaw tool hooks")
        try:
            toolkit.register_middleware(create_sanitizer_middleware())
        except Exception:
            logger.exception("Failed to register output sanitizer middleware")
        return toolkit

    create_toolkit_with_agentteams_tools._agentteams_message_hook = True  # type: ignore[attr-defined]
    CoPawAgent._create_toolkit = create_toolkit_with_agentteams_tools
    _TOOL_HOOK_INSTALLED = True
    logger.info("Installed AgentTeams CoPaw tool hooks")

    install_credential_guard_hook()
