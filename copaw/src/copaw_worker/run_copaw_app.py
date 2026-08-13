"""Run the upstream CoPaw app with AgentTeams runtime hooks installed."""

# 初学者导读：先安装凭据保护、消息过滤和输出脱敏等第一方 hook，再进入上游
# CoPaw 应用。顺序不能倒置，否则上游可能在保护层注册前就开始处理第一条消息。
# hook 只收紧 Worker 边界，不把 CoPaw 变成 Manager。

from __future__ import annotations

import runpy

from copaw_worker.hooks import install_tool_hooks


def main() -> None:
    # 逻辑说明：在以 `python -m copaw` 启动 Web 应用前安装 AgentTeams 工具、脱敏与凭据保护 hooks，再用 `__main__` 语义转交上游 CoPaw 入口；任一步失败直接抛出。
    install_tool_hooks()
    runpy.run_module("copaw", run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
