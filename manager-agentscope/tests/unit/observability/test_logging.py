from agentteams_manager.observability.logging import redact_fields


def test_log_redaction_is_recursive() -> None:
    redacted = redact_fields(
        {
            "event": "request",
            "headers": {"Authorization": "Bearer secret"},
            "nested": {"api_key": "secret", "safe": "visible"},
        },
    )

    assert redacted["headers"]["Authorization"] == "[REDACTED]"
    assert redacted["nested"]["api_key"] == "[REDACTED]"
    assert redacted["nested"]["safe"] == "visible"

