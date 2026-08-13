#!/usr/bin/env python3
from __future__ import annotations
"""
Export AgentTeams debug logs: Matrix messages + agent session logs.

Usage:
    # Export last 1 hour (default)
    python scripts/export-debug-log.py --range 1h

    # Export last 1 day
    python scripts/export-debug-log.py --range 1d

    # Filter by container or room
    python scripts/export-debug-log.py --range 1h --container agentteams-manager --room Worker

    # Disable PII redaction
    python scripts/export-debug-log.py --range 1h --no-redact

Output structure:
    debug-log/20260319-153000/
    ├── summary.txt
    ├── matrix-messages/
    │   └── RoomName_!roomid.jsonl
    ├── agent-sessions/
        ├── agentteams-manager/
        │   └── {session-id}.jsonl
        └── agentteams-worker-xxx/
            └── {session-key}.jsonl
    └── container-logs/
        ├── agentteams-worker-xxx.log
        └── agentteams-worker-xxx.state.json
"""
# 初学者说明：此脚本只读采集 Matrix、Agent session 和容器日志，再生成便于排障的
# 快照；它不会修复或重放状态。默认脱敏是安全边界，因为原始事件可能包含 API key、
# Matrix token、用户消息和路径。只有在明确控制输出目录和访问权限时才可使用
# --no-redact，生成物不得直接提交到 Git 或公开 issue。

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# PII redaction
# ---------------------------------------------------------------------------

_PII_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("ID_CARD",       re.compile(r'\b[1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx]\b')),
    ("PHONE",         re.compile(r'(?<!\d)(?:\+?86[-\s]?)?1[3-9]\d{9}(?!\d)')),
    ("EMAIL",         re.compile(r'\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b')),
    ("BANK_CARD",     re.compile(r'\b(?:6[0-9]{15,18}|4[0-9]{15}|5[1-5][0-9]{14}|3[47][0-9]{13}|62[0-9]{14,17})\b')),
    ("IP",            re.compile(r'\b(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\b')),
    ("ALIYUN_AK",     re.compile(r'\bLTAI[A-Za-z0-9]{12,30}\b')),
    ("ALIYUN_SK",     re.compile(r'(?i)(?:access_?key_?secret|secret_?access_?key)\s*[:=]\s*(\S{20,})')),
    ("AWS_AK",        re.compile(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b')),
    ("OPENAI_KEY",    re.compile(r'\bsk-[A-Za-z0-9]{20,}\b')),
    ("ANTHROPIC_KEY", re.compile(r'\bsk-ant-[A-Za-z0-9\-]{20,}\b')),
    ("DASHSCOPE_KEY", re.compile(r'\bsk-sp-[A-Za-z0-9]{20,}\b')),
    ("DEEPSEEK_KEY",  re.compile(r'\bsk-[a-f0-9]{32,}\b')),
    ("BEARER",        re.compile(r'(?i)(Bearer\s+)([A-Za-z0-9\-_.]{20,})')),
    ("SECRET_KV",     re.compile(
        r'(?i)((?:password|passwd|pwd|secret|token|api_?key|access_?key|secret_?key'
        r'|private_?key|credential|appkey|app_?secret|auth_?token|signing_?key'
        r'|client_?secret|master_?key)\s*[:=]\s*)'
        r'(\S+)'
    )),
    ("MATRIX_TOKEN",  re.compile(r'\bsyt_[A-Za-z0-9_\-]{10,}\b')),
    ("HEX_SECRET",    re.compile(r'\b[A-Fa-f0-9]{32,}\b')),
    ("PASSPORT",      re.compile(r'\b[EeGg]\d{8}\b')),
    ("SSN",           re.compile(r'\b\d{3}-\d{2}-\d{4}\b')),
]

_SECRET_FIELD_PATTERN = re.compile(
    r'(?i)^(?:password|passwd|pwd|secret|token|api[_-]?key'
    r'|access[_-]?key(?:[_-]?secret)?|secret[_-]?access[_-]?key'
    r'|secret[_-]?key|private[_-]?key|credential|app[_-]?key'
    r'|app[_-]?secret|auth[_-]?token|signing[_-]?key'
    r'|client[_-]?secret|master[_-]?key)$'
)


def redact_pii(text: str) -> str:
    # 逻辑说明：依次应用凭据和个人信息模式，把日志中的敏感值替换为固定掩码；空文本原样返回，避免采集阶段制造新内容。
    if not text:
        return text
    for name, pattern in _PII_PATTERNS:
        if name in ("SECRET_KV", "ALIYUN_SK", "BEARER"):
            text = pattern.sub(r'\1****', text)
        else:
            text = pattern.sub('****', text)
    return text


def redact_json_strings(obj):
    # 逻辑说明：递归遍历 JSON 兼容结构；敏感字段按键名整值遮盖，其余字符串再走模式脱敏，并保留列表/字典形状供排障读取。
    if isinstance(obj, str):
        return redact_pii(obj)
    if isinstance(obj, list):
        return [redact_json_strings(v) for v in obj]
    if isinstance(obj, dict):
        redacted = {}
        for key, value in obj.items():
            if isinstance(key, str) and _SECRET_FIELD_PATTERN.fullmatch(key):
                redacted[key] = "****"
            else:
                redacted[key] = redact_json_strings(value)
        return redacted
    return obj


# ---------------------------------------------------------------------------
# Common helpers
# ---------------------------------------------------------------------------

def parse_range(range_str: str) -> int:
    # 逻辑说明：把用户输入的分钟、小时或天数转换为秒；格式不在白名单内就明确失败，防止导出错误时间范围。
    m = re.fullmatch(r"(\d+)\s*(m|min|h|hr|hour|d|day)s?", range_str.strip(), re.IGNORECASE)
    if not m:
        raise ValueError(f"Invalid range format: '{range_str}'. Use e.g. 10m, 1h, 1d")
    value, unit = int(m.group(1)), m.group(2)[0].lower()
    return value * {"m": 60, "h": 3600, "d": 86400}[unit]


def parse_ts(ts_str: str) -> float:
    # 逻辑说明：把容器日志的 ISO 时间统一换算为 UTC epoch；缺失或损坏时间返回零，让调用方保守保留而不是终止整个导出。
    if not ts_str:
        return 0
    ts_str = ts_str.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return 0


def sanitize_filename(name: str) -> str:
    # 逻辑说明：把外部房间名限制为短小安全的文件名片段，避免路径分隔符或超长名称逃出导出目录。
    return re.sub(r'[^\w\-. ]', '_', name).strip()[:80]


def docker_exec(container: str, cmd: str) -> str:
    # 逻辑说明：在指定容器内执行只读采集命令并返回标准输出；调用方按空输出处理缺失数据，命令有 30 秒硬超时。
    result = subprocess.run(
        ["docker", "exec", container, "sh", "-c", cmd],
        capture_output=True, text=True, timeout=30,
    )
    return result.stdout


def list_agentteams_containers() -> list[str]:
    # 逻辑说明：从 Docker 查询 AgentTeams 命名空间内的运行容器并保持首次出现顺序去重，供各 runtime 会话探测复用。
    names: list[str] = []
    for prefix in ("agentteams-",):
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}", "--filter", f"name={prefix}"],
            capture_output=True, text=True, timeout=10,
        )
        names.extend(n.strip() for n in result.stdout.splitlines() if n.strip())
    return list(dict.fromkeys(names))


# ---------------------------------------------------------------------------
# Container diagnostics
# ---------------------------------------------------------------------------

def _docker_run(*args: str) -> subprocess.CompletedProcess[str]:
    # 逻辑说明：统一执行 Docker 诊断子命令；CLI 缺失、启动失败或超时时转成非零 CompletedProcess，使一次采集失败不会崩溃整个导出。
    try:
        return subprocess.run(
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return subprocess.CompletedProcess(
            ["docker", *args],
            1,
            "",
            f"{type(exc).__name__}: {exc}",
        )


def export_container_logs(out_dir: Path, since_epoch: float, redact: bool,
                          container_filter: str | None) -> int:
    """Export Docker state and logs for all AgentTeams containers."""
    # 逻辑说明：筛选目标容器后分别写入 inspect/state/log 文件；默认先脱敏，单容器失败记录诊断并继续，返回实际导出的容器数。
    result = _docker_run("ps", "-a", "--format", "{{.Names}}", "--filter", "name=agentteams-")
    containers = [name.strip() for name in result.stdout.splitlines() if name.strip()]
    if container_filter:
        containers = [name for name in containers if container_filter in name]
    if not containers:
        print("  [containers] No matching AgentTeams containers found")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    since = datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()

    for container in containers:
        state_result = _docker_run("inspect", "--format={{json .State}}", container)
        image_result = _docker_run("inspect", "--format={{json .Config.Image}}", container)
        restart_result = _docker_run("inspect", "--format={{.RestartCount}}", container)
        try:
            state = json.loads(state_result.stdout)
        except (json.JSONDecodeError, TypeError):
            state = {"inspect_error": state_result.stderr.strip() or state_result.stdout.strip()}
        try:
            image = json.loads(image_result.stdout)
        except (json.JSONDecodeError, TypeError):
            image = image_result.stdout.strip()
        try:
            restart_count = int(restart_result.stdout.strip())
        except ValueError:
            restart_count = None

        diagnostic = {
            "container": container,
            "image": image,
            "restart_count": restart_count,
            "state": state,
        }
        if redact:
            diagnostic = redact_json_strings(diagnostic)
        filename = sanitize_filename(container)
        (out_dir / f"{filename}.state.json").write_text(
            json.dumps(diagnostic, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        logs_result = _docker_run("logs", "--timestamps", "--since", since, container)
        logs = logs_result.stdout + logs_result.stderr
        if redact:
            logs = redact_pii(logs)
        (out_dir / f"{filename}.log").write_text(logs, encoding="utf-8")
        print(f"  {container}: state + {len(logs.splitlines())} log lines")

    return len(containers)


# ---------------------------------------------------------------------------
# Matrix messages export
# ---------------------------------------------------------------------------

def load_env_file(path: str) -> dict[str, str]:
    # 逻辑说明：只按 KEY=VALUE 读取安装器 env 文件用于定位服务，不执行其中的 Shell；文件不存在视为无法自动发现而非致命错误。
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    env[key.strip()] = value.strip()
    except FileNotFoundError:
        pass
    return env


def matrix_login(homeserver: str, user: str, password: str) -> str:
    # 逻辑说明：向指定 homeserver 发起最小密码登录并只返回 access token；网络、认证或响应格式错误直接交给上层决定是否跳过 Matrix 导出。
    url = f"{homeserver.rstrip('/')}/_matrix/client/v3/login"
    payload = json.dumps({
        "type": "m.login.password",
        "identifier": {"type": "m.id.user", "user": user},
        "password": password,
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    return data["access_token"]


def matrix_api(homeserver: str, token: str, endpoint: str, params: dict | None = None) -> dict:
    # 逻辑说明：构造带 Bearer 的 Matrix GET 请求并解析 JSON；HTTP 错误只打印状态与响应诊断后重新抛出，不在日志中输出 token。
    url = f"{homeserver.rstrip('/')}/_matrix/client/v3/{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"  API error {e.code} on {endpoint}: {body}", file=sys.stderr)
        raise


def fetch_room_messages(homeserver: str, token: str, room_id: str, since_ts: int) -> list[dict]:
    # 逻辑说明：从房间最新端反向分页，达到时间边界或无下一页即停止，最后恢复为时间正序供 JSONL 阅读。
    encoded = urllib.parse.quote(room_id)
    messages = []
    from_token = ""
    while True:
        params = {"dir": "b", "limit": "100"}
        if from_token:
            params["from"] = from_token
        data = matrix_api(homeserver, token, f"rooms/{encoded}/messages", params)
        chunk = data.get("chunk", [])
        if not chunk:
            break
        hit_boundary = False
        for event in chunk:
            if event.get("origin_server_ts", 0) < since_ts:
                hit_boundary = True
                break
            messages.append(event)
        if hit_boundary:
            break
        next_token = data.get("end")
        if not next_token or next_token == from_token:
            break
        from_token = next_token
    messages.reverse()
    return messages


def format_event(event: dict, redact: bool) -> dict:
    # 逻辑说明：把 Matrix 原始事件压缩成稳定诊断字段，并按事件类型保留必要内容；启用安全默认值时对整个结果递归脱敏。
    content = event.get("content", {})
    ts = event.get("origin_server_ts", 0)
    record = {
        "event_id": event.get("event_id"),
        "type": event.get("type"),
        "sender": event.get("sender"),
        "timestamp": ts,
        "time": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat(),
    }
    if event.get("type") == "m.room.message":
        record["msgtype"] = content.get("msgtype")
        record["body"] = content.get("body")
        if content.get("format"):
            record["format"] = content["format"]
        if content.get("url"):
            record["url"] = content["url"]
        if content.get("m.relates_to"):
            record["relates_to"] = content["m.relates_to"]
    else:
        record["content"] = content
    return redact_json_strings(record) if redact else record


def export_matrix_messages(out_dir: Path, since_epoch: float, redact: bool,
                           room_filter: str | None, env_file: str | None,
                           homeserver: str, token: str,
                           messages_only: bool) -> tuple[int, int]:
    """Export Matrix messages. Returns (rooms_exported, message_count)."""
    # 逻辑说明：从显式参数或安装 env 解析 homeserver/凭据，筛选已加入房间并分页写 JSONL；单房间失败不丢弃其他房间，返回房间与消息计数。
    env_path = env_file or os.path.expanduser("~/agentteams-manager.env")
    agentteams_env = load_env_file(env_path)

    if not homeserver:
        if not agentteams_env:
            print(f"  [matrix] Cannot find {env_path}, skipping Matrix export")
            return 0, 0
        port = agentteams_env.get("AGENTTEAMS_PORT_GATEWAY", "18080")
        homeserver = f"http://127.0.0.1:{port}"

    if not token:
        # Use Manager token — Manager is in every room (DM, Worker, Project)
        manager_password = agentteams_env.get("AGENTTEAMS_MANAGER_PASSWORD", "")
        if manager_password:
            try:
                token = matrix_login(homeserver, "manager", manager_password)
            except Exception:
                pass

        # Fallback to admin token if Manager login failed
        if not token:
            admin_user = agentteams_env.get("AGENTTEAMS_ADMIN_USER", "admin")
            admin_password = agentteams_env.get("AGENTTEAMS_ADMIN_PASSWORD", "")
            if not admin_password:
                print(f"  [matrix] No usable credentials found, skipping Matrix export")
                return 0, 0
            try:
                token = matrix_login(homeserver, admin_user, admin_password)
            except Exception as e:
                print(f"  [matrix] Login failed: {e}, skipping Matrix export")
                return 0, 0

    since_ts = int(since_epoch * 1000)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        rooms = matrix_api(homeserver, token, "joined_rooms").get("joined_rooms", [])
    except Exception as e:
        print(f"  [matrix] Failed to list rooms: {e}")
        return 0, 0

    total_messages = 0
    total_rooms = 0

    for room_id in rooms:
        encoded = urllib.parse.quote(room_id)
        try:
            room_name = matrix_api(homeserver, token, f"rooms/{encoded}/state/m.room.name").get("name", "")
        except Exception:
            room_name = ""

        if room_filter:
            if room_filter not in room_id and room_filter not in (room_name or ""):
                continue

        display = f"{room_name} ({room_id})" if room_name else room_id
        print(f"  {display} ... ", end="", flush=True)

        messages = fetch_room_messages(homeserver, token, room_id, since_ts)
        if messages_only:
            messages = [e for e in messages if e.get("type") == "m.room.message"]

        if not messages:
            print("0 messages, skipped")
            continue

        name_part = sanitize_filename(room_name) if room_name else ""
        id_part = sanitize_filename(room_id)
        filename = f"{name_part}_{id_part}.jsonl" if name_part else f"{id_part}.jsonl"

        with open(out_dir / filename, "w", encoding="utf-8") as f:
            for event in messages:
                record = format_event(event, redact=redact)
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        print(f"{len(messages)} messages -> {filename}")
        total_messages += len(messages)
        total_rooms += 1

    return total_rooms, total_messages


# ---------------------------------------------------------------------------
# Agent sessions export (AgentScope Manager + Worker runtimes)
# ---------------------------------------------------------------------------

def detect_runtime(container: str) -> tuple[str, str]:
    """Probe known absolute session-dir locations across runtimes & container types.

    Returns (runtime_name, absolute_sessions_dir). Empty tuple if none found.

    Layout reference (verified against worker/scripts/worker-entrypoint.sh,
    copaw/scripts/copaw-worker-entrypoint.sh, copaw/AGENTS.md and
    tests/lib/agent-metrics.sh):

      AgentScope Manager:
        sqlite -> /var/lib/agentteams-manager/state/manager.db

      OpenClaw / Hermes Worker (HOME=/root/agentteams-fs/agents/<name>):
        openclaw -> /root/agentteams-fs/agents/<name>/.openclaw/agents/main/sessions
        hermes   -> /root/agentteams-fs/agents/<name>/.hermes/sessions

      CoPaw Worker (HOME=/root/.agentteams-worker/<name>, also reachable via
      /root/agentteams-fs symlink that points to that same dir):
        copaw    -> /root/.agentteams-worker/<name>/.copaw/workspaces/default/sessions
        copaw    -> /root/agentteams-fs/.copaw/workspaces/default/sessions  (alt)
    """
    # 逻辑说明：按 AgentScope、OpenClaw、CoPaw、Hermes 的权威路径顺序在容器内探测会话存储；首个命中决定后续解析器，均未命中返回空元组。
    manager_database = "/var/lib/agentteams-manager/state/manager.db"
    if (
        docker_exec(
            container,
            f"test -f '{manager_database}' && echo yes || echo no",
        ).strip()
        == "yes"
    ):
        return "agentscope", manager_database

    candidates: list[tuple[str, str]] = []

    worker_name = docker_exec(container, "echo $AGENTTEAMS_WORKER_NAME").strip()
    if worker_name:
        candidates.extend([
            ("openclaw", f"/root/agentteams-fs/agents/{worker_name}/.openclaw/agents/main/sessions"),
            ("hermes",   f"/root/agentteams-fs/agents/{worker_name}/.hermes/sessions"),
            ("copaw",    f"/root/.agentteams-worker/{worker_name}/.copaw/workspaces/default/sessions"),
            ("copaw",    "/root/agentteams-fs/.copaw/workspaces/default/sessions"),
        ])

    for runtime, path in candidates:
        if docker_exec(container, f"test -d '{path}' && echo yes || echo no").strip() == "yes":
            return runtime, path

    # Last-resort: scan from / for any known session dir layout.
    found = docker_exec(
        container,
        "find / -maxdepth 7 \\( "
        "-path '*/.openclaw/agents/main/sessions' "
        "-o -path '*/.hermes/sessions' "
        "-o -path '*/.copaw/workspaces/default/sessions' "
        "\\) -type d 2>/dev/null | head -1",
    ).strip()
    if found:
        if "/.openclaw/" in found:
            return "openclaw", found
        if "/.hermes/" in found:
            return "hermes", found
        return "copaw", found

    return "", ""


def export_agentscope_sessions(
    container: str,
    database_path: str,
    since_epoch: float,
    out_dir: Path,
    redact: bool,
) -> tuple[int, int]:
    """Export redacted AgentScope state through a read-only SQLite snapshot."""
    # 逻辑说明：在 Manager 容器内以只读 SQLite 查询时间范围内的会话和事件，再把 JSON 结果复制到宿主导出目录；失败返回零计数且不写数据库。
    extraction = f"""
import datetime
import json
import sqlite3

database_path = {json.dumps(database_path)}
since_epoch = {since_epoch!r}
connection = sqlite3.connect(
    "file:" + database_path + "?mode=ro",
    uri=True,
    timeout=5,
)
connection.row_factory = sqlite3.Row
rows = connection.execute(
    "SELECT room_id, agent_state_json, policy_revision, "
    "last_event_id, updated_at FROM sessions ORDER BY updated_at DESC"
).fetchall()
result = []
for row in rows:
    timestamp = str(row["updated_at"]).replace("Z", "+00:00")
    try:
        updated_epoch = datetime.datetime.fromisoformat(timestamp).timestamp()
    except (TypeError, ValueError):
        updated_epoch = 0
    if updated_epoch and updated_epoch < since_epoch:
        continue
    try:
        state = json.loads(row["agent_state_json"])
    except (TypeError, json.JSONDecodeError):
        continue
    result.append({{
        "room_id": row["room_id"],
        "policy_revision": row["policy_revision"],
        "last_event_id": row["last_event_id"],
        "updated_at": row["updated_at"],
        "state": state,
    }})
print(json.dumps(result, ensure_ascii=False))
"""
    raw = docker_exec(
        container,
        "python3 - <<'PY'\n" + extraction + "\nPY",
    )
    if not raw.strip():
        return 0, 0
    try:
        rows = json.loads(raw)
    except json.JSONDecodeError:
        return 0, 0
    if not isinstance(rows, list):
        return 0, 0

    total_sessions = 0
    total_events = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        state = row.get("state")
        if not isinstance(state, dict):
            continue
        context = state.get("context", [])
        if not isinstance(context, list):
            context = []
        session_id = str(state.get("session_id", "session"))
        room_id = str(row.get("room_id", "room"))
        metadata = {
            key: value
            for key, value in state.items()
            if key != "context"
        }
        header = {
            "type": "session",
            "runtime": "agentscope",
            "session_id": session_id,
            "room_id": room_id,
            "policy_revision": row.get("policy_revision"),
            "last_event_id": row.get("last_event_id"),
            "updated_at": row.get("updated_at"),
            "state": metadata,
        }
        events = []
        for message in context:
            if not isinstance(message, dict):
                continue
            timestamp = str(message.get("created_at", ""))
            event_epoch = parse_ts(timestamp)
            if event_epoch and event_epoch < since_epoch:
                continue
            events.append({"type": "message", **message})
        if redact:
            header = redact_json_strings(header)
            events = [
                redact_json_strings(event)
                for event in events
            ]
        output = [json.dumps(header, ensure_ascii=False)]
        output.extend(
            json.dumps(event, ensure_ascii=False)
            for event in events
        )
        filename = (
            f"{sanitize_filename(room_id) or 'room'}_"
            f"{sanitize_filename(session_id) or 'session'}.jsonl"
        )
        (out_dir / filename).write_text(
            "\n".join(output) + "\n",
            encoding="utf-8",
        )
        print(
            f"  {container}/{filename} (agentscope): "
            f"{len(events)} events",
        )
        total_sessions += 1
        total_events += len(events)

    return total_sessions, total_events


def export_openclaw_sessions(container: str, sessions_dir: str, since_epoch: float,
                             out_dir: Path, redact: bool) -> tuple[int, int]:
    # 逻辑说明：逐个读取 OpenClaw JSONL 会话，按事件时间过滤、可选递归脱敏并写独立文件；损坏行跳过，返回有效会话和事件总数。
    ls_output = docker_exec(container, f"ls '{sessions_dir}'/*.jsonl 2>/dev/null").strip()
    if not ls_output:
        return 0, 0

    total_sessions = 0
    total_events = 0

    for session_path in [f.strip() for f in ls_output.splitlines() if f.strip()]:
        filename = os.path.basename(session_path)

        first_line = docker_exec(container, f"head -1 '{session_path}'").strip()
        if not first_line:
            continue
        try:
            header = json.loads(first_line)
        except json.JSONDecodeError:
            continue

        last_line = docker_exec(container, f"tail -1 '{session_path}'").strip()
        try:
            last_ts = parse_ts(json.loads(last_line).get("timestamp", ""))
        except Exception:
            last_ts = parse_ts(header.get("timestamp", ""))

        if last_ts < since_epoch and last_ts > 0:
            continue

        raw = docker_exec(container, f"cat '{session_path}'")
        if not raw.strip():
            continue

        output_lines = []
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            event_ts = parse_ts(event.get("timestamp", ""))
            if event.get("type") != "session" and event_ts < since_epoch and event_ts > 0:
                continue
            if redact:
                event = redact_json_strings(event)
            output_lines.append(json.dumps(event, ensure_ascii=False))

        if len(output_lines) <= 1:
            continue

        with open(out_dir / filename, "w", encoding="utf-8") as f:
            f.write("\n".join(output_lines) + "\n")

        event_count = len(output_lines) - 1
        print(f"  {container}/{filename} (openclaw): {event_count} events")
        total_sessions += 1
        total_events += event_count

    # sessions.json index
    try:
        raw = docker_exec(container, f"cat '{sessions_dir}/sessions.json'")
        if raw.strip():
            index = json.loads(raw)
            if redact:
                index = redact_json_strings(index)
            with open(out_dir / "sessions.json", "w", encoding="utf-8") as f:
                json.dump(index, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return total_sessions, total_events


def export_copaw_sessions(container: str, sessions_dir: str, since_epoch: float,
                          out_dir: Path, redact: bool) -> tuple[int, int]:
    # 逻辑说明：发现 CoPaw JSON 会话文件并兼容其对象/消息布局，过滤时间后写统一 JSONL；单文件无法解析时继续采集其他会话。
    ls_output = docker_exec(container, f"find '{sessions_dir}' -name '*.json' -type f 2>/dev/null").strip()
    if not ls_output:
        return 0, 0

    total_sessions = 0
    total_events = 0

    for session_path in [f.strip() for f in ls_output.splitlines() if f.strip()]:
        raw = docker_exec(container, f"cat '{session_path}'")
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue

        agent = data.get("agent", {})
        memory = agent.get("memory", {})
        content = memory.get("content", [])
        if not content:
            continue

        basename = os.path.basename(session_path).replace(".json", "")
        header = {
            "type": "session",
            "runtime": "copaw",
            "agent_name": agent.get("name", ""),
            "session_key": basename,
            "compressed_summary": memory.get("_compressed_summary", ""),
        }

        messages_in_range = []
        for turn_idx, turn in enumerate(content):
            if not isinstance(turn, list):
                continue
            for msg in turn:
                if not isinstance(msg, dict):
                    continue
                msg_ts = parse_ts(msg.get("timestamp", ""))
                if msg_ts >= since_epoch or msg_ts == 0:
                    event = {
                        "type": "message",
                        "turn": turn_idx,
                        "id": msg.get("id", ""),
                        "role": msg.get("role", ""),
                        "name": msg.get("name", ""),
                        "timestamp": msg.get("timestamp", ""),
                        "content": msg.get("content", []),
                    }
                    if msg.get("metadata"):
                        event["metadata"] = msg["metadata"]
                    messages_in_range.append(event)

        if not messages_in_range:
            continue

        output_lines = []
        if redact:
            header = redact_json_strings(header)
        output_lines.append(json.dumps(header, ensure_ascii=False))
        for event in messages_in_range:
            if redact:
                event = redact_json_strings(event)
            output_lines.append(json.dumps(event, ensure_ascii=False))

        out_filename = basename + ".jsonl"
        with open(out_dir / out_filename, "w", encoding="utf-8") as f:
            f.write("\n".join(output_lines) + "\n")

        event_count = len(output_lines) - 1
        print(f"  {container}/{out_filename} (copaw): {event_count} events")
        total_sessions += 1
        total_events += event_count

    return total_sessions, total_events


def export_hermes_sessions(container: str, sessions_dir: str, since_epoch: float,
                           out_dir: Path, redact: bool) -> tuple[int, int]:
    # 逻辑说明：读取 Hermes JSONL 会话，兼容时间字段并只保留目标区间；可选脱敏后按会话落盘，坏事件不会阻断整批导出。
    ls_output = docker_exec(container, f"ls '{sessions_dir}'/*.jsonl 2>/dev/null").strip()
    if not ls_output:
        return 0, 0

    total_sessions = 0
    total_events = 0

    for session_path in [f.strip() for f in ls_output.splitlines() if f.strip()]:
        raw = docker_exec(container, f"cat '{session_path}'")
        if not raw.strip():
            continue

        output_lines = []
        last_ts = 0.0
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            event_ts = parse_ts(event.get("timestamp", ""))
            if event_ts:
                last_ts = max(last_ts, event_ts)

            role = event.get("role", "")
            if role != "session_meta" and event_ts < since_epoch and event_ts > 0:
                continue

            if redact:
                event = redact_json_strings(event)
            output_lines.append(json.dumps(event, ensure_ascii=False))

        if not output_lines:
            continue

        if last_ts < since_epoch and last_ts > 0:
            continue

        filename = os.path.basename(session_path)
        with open(out_dir / filename, "w", encoding="utf-8") as f:
            f.write("\n".join(output_lines) + "\n")

        event_count = len(output_lines) - 1 if output_lines and '"role": "session_meta"' in output_lines[0] else len(output_lines)
        print(f"  {container}/{filename} (hermes): {event_count} events")
        total_sessions += 1
        total_events += max(event_count, 0)

    hermes_home = str(Path(sessions_dir).parent)
    state_db = f"{hermes_home}/state.db"
    has_state_db = docker_exec(container, f"test -f '{state_db}' && echo yes || echo no").strip() == "yes"
    if has_state_db:
        raw = docker_exec(container, f"python3 - <<'PY'\nimport json, sqlite3\nconn = sqlite3.connect('{state_db}')\nconn.row_factory = sqlite3.Row\nrows = conn.execute('SELECT * FROM sessions ORDER BY started_at DESC LIMIT 200').fetchall()\nprint(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))\nPY")
        if raw.strip():
            try:
                data = json.loads(raw)
                if redact:
                    data = redact_json_strings(data)
                with open(out_dir / "sessions-db.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            except json.JSONDecodeError:
                pass

    logs_dir = f"{hermes_home}/logs"
    has_logs_dir = docker_exec(container, f"test -d '{logs_dir}' && echo yes || echo no").strip() == "yes"
    if has_logs_dir:
        for log_name in ("agent.log", "errors.log", "gateway.log"):
            raw = docker_exec(container, f"test -f '{logs_dir}/{log_name}' && cat '{logs_dir}/{log_name}' || true")
            if raw.strip():
                content = redact_pii(raw) if redact else raw
                (out_dir / log_name).write_text(content, encoding="utf-8")

    return total_sessions, total_events


def export_agent_sessions(out_dir: Path, since_epoch: float, redact: bool,
                          container_filter: str | None) -> tuple[int, int]:
    """Export agent sessions from all containers. Returns (session_count, event_count)."""
    # 逻辑说明：遍历目标容器、探测其 runtime 并分派到对应只读解析器；未知 runtime 或单容器错误仅告警，最后汇总所有成功计数。
    containers = list_agentteams_containers()
    if container_filter:
        containers = [c for c in containers if container_filter in c]

    if not containers:
        print("  [sessions] No matching AgentTeams containers found")
        return 0, 0

    total_sessions = 0
    total_events = 0

    for container in containers:
        runtime, sessions_dir = detect_runtime(container)
        if not runtime:
            print(f"  {container}: no sessions directory, skipped")
            continue

        container_dir = out_dir / container
        container_dir.mkdir(parents=True, exist_ok=True)

        if runtime == "agentscope":
            s, e = export_agentscope_sessions(
                container,
                sessions_dir,
                since_epoch,
                container_dir,
                redact,
            )
        elif runtime == "openclaw":
            s, e = export_openclaw_sessions(container, sessions_dir, since_epoch,
                                            container_dir, redact)
        elif runtime == "hermes":
            s, e = export_hermes_sessions(container, sessions_dir, since_epoch,
                                          container_dir, redact)
        else:
            s, e = export_copaw_sessions(container, sessions_dir, since_epoch,
                                         container_dir, redact)

        if s == 0:
            if not any(container_dir.iterdir()):
                container_dir.rmdir()
                print(f"  {container} ({runtime}): no sessions in range, skipped")
            else:
                print(f"  {container} ({runtime}): exported auxiliary debug artifacts without session events")
        else:
            total_sessions += s
            total_events += e

    return total_sessions, total_events


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # 逻辑说明：解析导出范围和安全选项，依次采集容器、Matrix 与 Agent 会话，生成摘要后以计数反映结果；默认始终启用脱敏。
    parser = argparse.ArgumentParser(
        description="Export AgentTeams debug logs (Matrix messages + agent sessions)"
    )
    parser.add_argument("--range", "-r", required=True, dest="time_range",
                        help="Time range to export, e.g. 10m, 1h, 1d")
    parser.add_argument("--container", "-c", default=None,
                        help="Only export sessions from containers matching this substring")
    parser.add_argument("--room", default=None,
                        help="Only export Matrix rooms matching this substring")
    parser.add_argument("--homeserver", "-s", default="",
                        help="Matrix homeserver URL (auto-detected from env file)")
    parser.add_argument("--token", "-t", default="",
                        help="Matrix access token (auto-detected from env file)")
    parser.add_argument("--env-file", default=None,
                        help="Path to agentteams-manager.env (default: ~/agentteams-manager.env)")
    parser.add_argument("--messages-only", action="store_true",
                        help="Only export m.room.message events (skip state events)")
    parser.add_argument("--no-redact", action="store_true",
                        help="Disable PII redaction")
    args = parser.parse_args()

    range_seconds = parse_range(args.time_range)
    since_epoch = time.time() - range_seconds
    since_human = datetime.fromtimestamp(since_epoch, tz=timezone.utc).isoformat()
    now_str = datetime.now().strftime("%Y%m%d-%H%M%S")

    run_dir = Path("debug-log") / now_str
    run_dir.mkdir(parents=True, exist_ok=True)
    redact = not args.no_redact

    print(f"AgentTeams Debug Log Export")
    print(f"  Range: last {args.time_range} (since {since_human})")
    print(f"  Output: {run_dir.resolve()}")
    print(f"  PII redaction: {'on' if redact else 'off'}")
    print()

    # --- Matrix messages ---
    print("=== Matrix Messages ===")
    matrix_dir = run_dir / "matrix-messages"
    rooms, messages = export_matrix_messages(
        matrix_dir, since_epoch, redact,
        room_filter=args.room, env_file=args.env_file,
        homeserver=args.homeserver, token=args.token,
        messages_only=args.messages_only,
    )
    if rooms == 0 and matrix_dir.exists() and not any(matrix_dir.iterdir()):
        matrix_dir.rmdir()
    print()

    # --- Agent sessions ---
    print("=== Agent Sessions ===")
    sessions_dir = run_dir / "agent-sessions"
    sessions, events = export_agent_sessions(
        sessions_dir, since_epoch, redact,
        container_filter=args.container,
    )
    if sessions == 0 and sessions_dir.exists() and not any(sessions_dir.iterdir()):
        sessions_dir.rmdir()
    print()

    # --- Container diagnostics ---
    print("=== Container Diagnostics ===")
    container_logs_dir = run_dir / "container-logs"
    containers = export_container_logs(
        container_logs_dir, since_epoch, redact,
        container_filter=args.container,
    )
    if containers == 0 and container_logs_dir.exists() and not any(container_logs_dir.iterdir()):
        container_logs_dir.rmdir()
    print()

    # --- Summary ---
    summary = (
        f"AgentTeams Debug Log\n"
        f"Exported at: {now_str}\n"
        f"Range: last {args.time_range} (since {since_human})\n"
        f"PII redaction: {'on' if redact else 'off'}\n"
        f"\n"
        f"Matrix messages: {messages} messages from {rooms} rooms\n"
        f"Agent sessions: {events} events from {sessions} sessions\n"
        f"Container diagnostics: {containers} containers\n"
    )
    (run_dir / "summary.txt").write_text(summary)

    print(
        f"Done. {messages} messages from {rooms} rooms, "
        f"{events} events from {sessions} sessions, "
        f"{containers} container diagnostics"
    )
    print(f"Output: {run_dir.resolve()}")


if __name__ == "__main__":
    main()
