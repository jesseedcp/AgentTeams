from agentteams_manager.admin.ui import ADMIN_HTML


def test_admin_ui_supports_safe_resource_mutations() -> None:
    assert "api/v1/" in ADMIN_HTML
    assert "Idempotency-Key" in ADMIN_HTML
    assert 'method:"POST"' in ADMIN_HTML
    assert 'openEditor("PATCH"' in ADMIN_HTML
    assert 'openEditor("DELETE"' in ADMIN_HTML
    assert "async function mutate" in ADMIN_HTML
    assert "result.status!==202" in ADMIN_HTML
    assert '"Idempotency-Key":idempotencyKey' in ADMIN_HTML
    assert "setTimeout(resolve,2000)" in ADMIN_HTML
    assert "<dialog" in ADMIN_HTML
    assert "sessionStorage" not in ADMIN_HTML
    assert "localStorage" not in ADMIN_HTML


def test_admin_ui_uses_project_id_and_requires_manual_confirmation() -> None:
    assert "function resourceIdentifier(resource,item)" in ADMIN_HTML
    assert 'resource==="projects"?item.project_id:item.name' in ADMIN_HTML
    assert 'document.querySelector("#confirmed").checked=false' in ADMIN_HTML
    assert 'item.name||item.project_id' not in ADMIN_HTML
