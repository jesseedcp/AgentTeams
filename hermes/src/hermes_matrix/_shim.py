"""Shim installed as ``gateway.platforms.matrix`` inside hermes-agent.

The image build renames hermes-agent's original Matrix module to
``gateway.platforms._matrix_native``.  This shim keeps the module path stable
for ``gateway.run._create_adapter`` while swapping in AgentTeams's subclassed
``MatrixAdapter``.
"""

# 初学者导读：shim（薄转接层）保留上游 import 路径，使 Hermes 无需知道自己正在
# AgentTeams 中运行；除 MatrixAdapter 替换为带策略的子类外，其余名称全部转发给
# 上游模块。这比修改 vendor 源码稳定，升级时也容易看清我们真正改变了什么。
from __future__ import annotations

from gateway.platforms import _matrix_native as _native
from gateway.platforms._matrix_native import *  # noqa: F401,F403

from hermes_matrix.adapter import MatrixAdapter as MatrixAdapter

check_matrix_requirements = _native.check_matrix_requirements


def __getattr__(name: str):
    return getattr(_native, name)


def __dir__() -> list[str]:
    # 逻辑说明：合并 shim 与原生扩展的符号名并排序，使交互检查看到完整 API。
    return sorted(set(globals()) | set(dir(_native)))
