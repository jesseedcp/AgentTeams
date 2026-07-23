from agentteams_manager.observability.tracing import build_tracer_from_env


def test_cms_disabled_uses_noop_tracer(monkeypatch) -> None:
    monkeypatch.delenv("AGENTTEAMS_CMS_TRACES_ENABLED", raising=False)

    tracer = build_tracer_from_env()

    assert tracer.is_noop

