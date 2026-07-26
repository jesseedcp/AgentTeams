from agentteams_manager.matrix.formatting import markdown_to_matrix_html


def test_markdown_html_is_useful_and_sanitized() -> None:
    rendered = markdown_to_matrix_html(
        "**Ready** <script>alert(1)</script>\n"
        "[docs](https://example.com/a?q=1)\n"
        "[bad](javascript:alert(1))",
    )

    assert "<strong>Ready</strong>" in rendered
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in rendered
    assert '<a href="https://example.com/a?q=1"' in rendered
    assert 'href="javascript:' not in rendered
