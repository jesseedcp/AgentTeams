"""Generate and validate the checked-in typed-tool catalog."""

from __future__ import annotations

START_MARKER = "<!-- BEGIN GENERATED AGENTSCOPE TOOLS -->"
END_MARKER = "<!-- END GENERATED AGENTSCOPE TOOLS -->"


def render_tool_catalog(tool_names: frozenset[str] | set[str]) -> str:
    lines = [START_MARKER, "## Registered Manager tools", ""]
    lines.extend(f"- `{name}`" for name in sorted(tool_names))
    lines.extend(("", END_MARKER))
    return "\n".join(lines)


def replace_tool_catalog(document: str, tool_names: frozenset[str]) -> str:
    start = document.index(START_MARKER)
    end = document.index(END_MARKER, start) + len(END_MARKER)
    return (
        document[:start]
        + render_tool_catalog(tool_names)
        + document[end:]
    )


def documented_tool_names(document: str) -> frozenset[str]:
    start = document.index(START_MARKER)
    end = document.index(END_MARKER, start)
    block = document[start:end]
    return frozenset(
        line[3:-1]
        for line in block.splitlines()
        if line.startswith("- `") and line.endswith("`")
    )
