"""Generate and validate the checked-in typed-tool catalog.

根据实际注册的 typed tools 生成并校验工具目录文档。

工具说明如果与代码漂移，模型可能请求不存在的参数或误判能力边界。本模块从 toolkit
读取名称与 schema，渲染到受标记区域并检查文档中的集合；它只同步说明，不授予权限，
最终可调用集合仍由当前 room policy 决定。
"""

from __future__ import annotations

START_MARKER = "<!-- BEGIN GENERATED AGENTSCOPE TOOLS -->"
END_MARKER = "<!-- END GENERATED AGENTSCOPE TOOLS -->"


def render_tool_catalog(tool_names: frozenset[str] | set[str]) -> str:
    # 逻辑说明：把工具名称去重集合按字典序渲染成两个固定 marker 包围的 Markdown 列表；输入为空仍生成合法空区块，函数不修改任何文档文件。
    lines = [START_MARKER, "## Registered Manager tools", ""]
    lines.extend(f"- `{name}`" for name in sorted(tool_names))
    lines.extend(("", END_MARKER))
    return "\n".join(lines)


def replace_tool_catalog(document: str, tool_names: frozenset[str]) -> str:
    # 逻辑说明：在 document 中定位首个生成区块的起止 marker，并只用 render_tool_catalog 的新内容替换该闭区间；任一 marker 缺失时由 str.index 抛错且原字符串不变。
    start = document.index(START_MARKER)
    end = document.index(END_MARKER, start) + len(END_MARKER)
    return (
        document[:start]
        + render_tool_catalog(tool_names)
        + document[end:]
    )


def documented_tool_names(document: str) -> frozenset[str]:
    # 逻辑说明：截取 document 两个生成 marker 之间的文本，只解析完整的 Markdown 反引号列表项并返回去重 frozenset；marker 缺失时明确抛出 ValueError，不猜测其他段落。
    start = document.index(START_MARKER)
    end = document.index(END_MARKER, start)
    block = document[start:end]
    return frozenset(
        line[3:-1]
        for line in block.splitlines()
        if line.startswith("- `") and line.endswith("`")
    )
