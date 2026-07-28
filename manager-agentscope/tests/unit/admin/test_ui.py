from agentteams_manager.admin.ui import ADMIN_HTML


def test_admin_ui_supports_safe_resource_mutations() -> None:
    assert "api/v1/" in ADMIN_HTML
    assert "Idempotency-Key" in ADMIN_HTML
    assert 'method:"POST"' in ADMIN_HTML
    assert 'openEditor("PATCH"' in ADMIN_HTML
    assert 'openEditor("DELETE"' in ADMIN_HTML
    assert "<dialog" in ADMIN_HTML
    assert "sessionStorage" not in ADMIN_HTML
    assert "localStorage" not in ADMIN_HTML
