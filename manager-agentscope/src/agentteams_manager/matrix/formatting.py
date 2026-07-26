"""Small allowlist Markdown renderer for Matrix formatted bodies."""

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
    escaped = html.escape(text, quote=True)
    fenced: list[str] = []

    def store_fence(match: re.Match[str]) -> str:
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
    label, raw_url = match.groups()
    parsed = urlparse(html.unescape(raw_url))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return f"{label} ({raw_url})"
    url = html.escape(html.unescape(raw_url), quote=True)
    return f'<a href="{url}" rel="noopener noreferrer">{label}</a>'
