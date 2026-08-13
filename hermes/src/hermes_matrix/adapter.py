"""AgentTeams Hermes Matrix overlay 的稳定导入入口。

其他代码只依赖这里导出的 MatrixAdapter；真正实现位于 ``overlay_adapter``。这一层
让模块改名不影响上游 shim，也明确本项目替换的是策略适配器而非完整 Matrix SDK。

Compatibility wrapper for the AgentTeams Matrix overlay adapter.
"""
from hermes_matrix.overlay_adapter import MatrixAdapter

__all__ = ["MatrixAdapter"]
