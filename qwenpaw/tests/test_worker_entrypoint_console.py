from pathlib import Path


def test_qwenpaw_entrypoint_only_passes_console_port_when_enabled() -> None:
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "qwenpaw-worker-entrypoint.sh"
    ).read_text()

    assert 'CONSOLE_PORT="${AGENTTEAMS_CONSOLE_PORT:-}"' in script
    assert 'if [ -n "${CONSOLE_PORT}" ]; then' in script
    assert 'CMD_ARGS+=(--console-port "${CONSOLE_PORT}")' in script
    assert '--console-port "${CONSOLE_PORT}"\n)' not in script
