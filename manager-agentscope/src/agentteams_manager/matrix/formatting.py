"""Small allowlist Markdown renderer for Matrix formatted bodies.

把有限 Markdown 转成 Matrix 可显示且经过转义的 HTML。

Agent 回复可能包含列表、代码或链接，但 Matrix formatted_body 是 HTML，直接插入模型
文本会造成标签注入。这里先转义，再只恢复允许的结构和安全链接；纯文本 body 始终
保留作为兼容回退，客户端不支持 HTML 时仍能看到完整内容。
"""

from __future__ import annotations

import html
import re
from urllib.parse import urlparse

_LINK = re.compile(r"\[([^\]\n]+)\]\(([^)\s]+)\)")
_CODE = re.compile(r"`([^`\n]+)`")
_STRONG = re.compile(r"\*\*([^*\n]+)\*\*")
_EMPHASIS = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)")


def markdown_to_matrix_html(text: str) -> str:
    """Render a deliberately small Markdown subset after HTML escaping."""
    # 逻辑说明：先转义全部模型文本，再依次恢复代码块、安全链接、行内样式和列表；因此返回值可放进 Matrix formatted_body，且不执行任何远程 I/O。
    escaped = html.escape(text, quote=True)
    fenced: list[str] = []

    def store_fence(match: re.Match[str]) -> str:
        # 逻辑说明：把已转义的围栏代码暂存为编号占位符，避免后续 Markdown 正则误改代码内容，最终由外层函数按编号放回 HTML。
        language = html.escape(match.group(1).strip(), quote=True)
        body = match.group(2).strip("\n")
        class_name = f' class="language-{language}"' if language else ""
        fenced.append(f"<pre><code{class_name}>{body}</code></pre>")
        return f"\x00FENCE{len(fenced) - 1}\x00"

    escaped = re.sub(
        r"```([A-Za-z0-9_+-]*)\n(.*?)```",
        store_fence,
        escaped,
        flags=re.DOTALL,
    )
    escaped = _LINK.sub(_safe_link, escaped)
    escaped = _CODE.sub(r"<code>\1</code>", escaped)
    escaped = _STRONG.sub(r"<strong>\1</strong>", escaped)
    escaped = _EMPHASIS.sub(r"<em>\1</em>", escaped)
    lines = escaped.splitlines()
    rendered: list[str] = []
    in_list = False
    for line in lines:
        if line.startswith(("- ", "* ")):
            if not in_list:
                rendered.append("<ul>")
                in_list = True
            rendered.append(f"<li>{line[2:]}</li>")
            continue
        if in_list:
            rendered.append("</ul>")
            in_list = False
        if not line:
            rendered.append("<br>")
        elif line.startswith("\x00FENCE"):
            rendered.append(line)
        else:
            rendered.append(f"<p>{line}</p>")
    if in_list:
        rendered.append("</ul>")
    result = "".join(rendered)
    for index, block in enumerate(fenced):
        result = result.replace(f"\x00FENCE{index}\x00", block)
    return result or "<p></p>"


def _safe_link(match: re.Match[str]) -> str:
    # 逻辑说明：仅允许同时具有主机名的 http/https 链接生成锚点；javascript、相对地址等输入降级为可见纯文本，防止点击时执行危险协议。
    label, raw_url = match.groups()
    parsed = urlparse(html.unescape(raw_url))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return f"{label} ({raw_url})"
    url = html.escape(html.unescape(raw_url), quote=True)
    return f'<a href="{url}" rel="noopener noreferrer">{label}</a>'
