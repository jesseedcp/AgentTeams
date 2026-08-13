from pathlib import Path


def test_qwenpaw_entrypoint_only_passes_console_port_when_enabled() -> None:
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "qwenpaw-worker-entrypoint.sh"
    # 仓库中的 Shell 脚本统一采用 UTF-8；显式指定编码，避免 Windows
    # 默认 GBK 在脚本含中文维护注释时把“读取源码”误判成运行逻辑失败。
    ).read_text(encoding="utf-8")

    assert 'CONSOLE_PORT="${AGENTTEAMS_CONSOLE_PORT:-}"' in script
    assert 'if [ -n "${CONSOLE_PORT}" ]; then' in script
    assert 'CMD_ARGS+=(--console-port "${CONSOLE_PORT}")' in script
    assert '--console-port "${CONSOLE_PORT}"\n)' not in script
