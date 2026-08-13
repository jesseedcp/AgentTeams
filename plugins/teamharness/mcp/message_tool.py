#!/usr/bin/env python3
"""TeamHarness message MCP tool implementation."""

# 初学者导读：Worker/Leader 通过此工具向 Matrix 房间或队友发送确定的协调消息。
# 工具先选择当前 channel 的回复路由，执行目标与 @mention 校验，再把消息交给
# QwenPaw channel 或 Matrix API；成功后补写 session，避免下一轮 Agent 不知道自己
# 已经发送过。它不是任意 HTTP 客户端，也不授予 Worker 管理资源的权限。

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import time
from typing import Any, Callable
import urllib.error
import urllib.parse
import urllib.request


SELF_TRIGGER_MESSAGE_TYPES = {"PROJECT_REQUESTED"}
TEAMHARNESS_TRIGGER_CONTENT_KEY = "m.teamharness.trigger"


@dataclass(frozen=True)
class MessageToolDeps:
    """把网络/路由函数作为依赖注入，便于在单元测试中替换为无副作用假实现。"""
    reply_route: Callable[[dict[str, Any]], dict[str, Any]]
    qwenpaw_message: Callable[[dict[str, Any], dict[str, Any], str, str], dict[str, Any]]
    matrix_target: Callable[[str], tuple[str, str]]
    mentions: Callable[[str, str], list[str]]
    ping_pong_error: Callable[[str, list[str]], str | None]
    matrix_content: Callable[[str, list[str]], dict[str, Any]]
    record_matrix_outbound_to_session: Callable[[str, str, str | None, str], bool]


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    # 逻辑说明：`_text` 把可选输入转换并去除首尾空白，空值返回空字符串，供后续路由和消息校验统一使用；不产生外部副作用。
    return str(value).strip() if value is not None else ""


def _message_object(arguments: dict[str, Any]) -> dict[str, Any]:
    # 逻辑说明：`_message_object` 接受 dict 或 JSON 字符串消息；解析失败返回空对象而不执行发送。
    message = arguments.get("message")
    if isinstance(message, dict):
        return message
    if isinstance(message, str):
        raw = message.strip()
        if raw.startswith("{"):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
    return {}


def _message_text(arguments: dict[str, Any]) -> str:
    # 逻辑说明：`_message_text` 按兼容字段优先级提取正文，并统一转换成字符串交给路由校验。
    message = arguments.get("message")
    message_obj = _message_object(arguments)
    if message_obj:
        for key in ("text", "body", "message", "content"):
            value = message_obj.get(key)
            if value is not None:
                return str(value)
        return ""
    if message is not None:
        return str(message)
    return str(arguments.get("text") or arguments.get("body") or "")


def _message_type(arguments: dict[str, Any]) -> str:
    # 逻辑说明：`_message_type` 从顶层或嵌套 message 读取类型；缺失时返回空值供普通消息路径处理。
    for key in ("type", "messageType", "message_type"):
        value = arguments.get(key)
        if value is not None:
            return _text(value)
    message = _message_object(arguments)
    if message:
        for key in ("type", "messageType", "message_type"):
            value = message.get(key)
            if value is not None:
                return _text(value)
    return ""


def _agent_from(value: Any) -> str:
    # 逻辑说明：`_agent_from` 从多版本 agent 身份字段中选择首个非空值，避免协议别名导致身份丢失。
    data = _dict(value)
    for key in ("agent", "agentId", "agent_id", "accountId", "account_id", "runtimeName", "runtime_name", "name"):
        text = _text(data.get(key))
        if text:
            return text
    return ""


def _current_agent(arguments: dict[str, Any]) -> str:
    # 逻辑说明：`_current_agent` 优先使用请求身份，再回退 runtime 环境和 default；只返回规范字符串。
    for key in ("agentId", "agent_id", "accountId", "account_id"):
        value = _text(arguments.get(key))
        if value:
            return value
    return _text(os.getenv("AGENTTEAMS_WORKER_NAME")) or "default"


def _is_matrix_user_id(value: str) -> bool:
    # 逻辑说明：`_is_matrix_user_id` 做最小 MXID 形状判断，供后续目标路由拒绝明显非法身份。
    text = value.strip()
    return text.startswith("@") and ":" in text


def _session_id_from(value: Any) -> str:
    # 逻辑说明：`_session_id_from` 从字符串或多版本嵌套 session 字段提取稳定会话标识。
    if isinstance(value, str):
        return value.strip()
    data = _dict(value)
    session = data.get("session")
    if isinstance(session, str):
        return session.strip()
    if isinstance(session, dict):
        for key in (
            "id",
            "sessionId",
            "session_id",
            "targetSession",
            "target_session",
            "sourceSession",
            "source_session",
            "roomId",
            "room_id",
        ):
            text = _text(session.get(key))
            if text:
                return text
    for key in (
        "id",
        "sessionId",
        "session_id",
        "targetSession",
        "target_session",
        "sourceSession",
        "source_session",
        "roomId",
        "room_id",
        "target",
    ):
        text = _text(data.get(key))
        if text:
            return text
    return ""


def _target_value(arguments: dict[str, Any], route: dict[str, Any]) -> str:
    # 逻辑说明：`_target_value` 按显式参数优先于 reply route 的顺序选择目标，首个有效 session 即返回。
    for value in (
        arguments.get("target"),
        route.get("target"),
        arguments.get("room_id"),
        arguments.get("roomId"),
        route.get("room_id"),
        route.get("roomId"),
        route.get("targetRoom"),
        route.get("target_room"),
        route.get("targetSession"),
        route.get("target_session"),
    ):
        target = _session_id_from(value)
        if target:
            return target
    return ""


def _source_value(arguments: dict[str, Any], route: dict[str, Any]) -> str:
    # 逻辑说明：`_source_value` 汇总 requester/sender 的兼容字段，避免 self-trigger 校验猜测来源。
    for value in (
        arguments.get("sourceSession"),
        arguments.get("source_session"),
        arguments.get("senderSession"),
        arguments.get("sender_session"),
        arguments.get("currentSession"),
        arguments.get("current_session"),
        route.get("sourceSession"),
        route.get("source_session"),
        arguments.get("sender"),
        arguments.get("source"),
    ):
        source = _session_id_from(value)
        if source:
            return source
    return ""


def _channel_from(value: Any) -> str:
    # 逻辑说明：`_channel_from` 从直接或嵌套 session 中提取 channel，并统一为小写。
    data = _dict(value)
    for key in ("channel", "channelId", "channel_id"):
        text = _text(data.get(key)).lower()
        if text:
            return text
    session = data.get("session")
    if isinstance(session, dict):
        for key in ("channel", "channelId", "channel_id"):
            text = _text(session.get(key)).lower()
            if text:
                return text
    return ""


def _source_channel(arguments: dict[str, Any], route: dict[str, Any], source_session: str) -> str:
    # 逻辑说明：`_source_channel` 先读显式来源渠道，再从 session 前缀推导；不修改 route。
    for value in (
        arguments.get("sourceChannel"),
        arguments.get("source_channel"),
        arguments.get("senderChannel"),
        arguments.get("sender_channel"),
        route.get("sourceChannel"),
        route.get("source_channel"),
        arguments.get("sender"),
        arguments.get("source"),
    ):
        channel = _channel_from(value) if isinstance(value, dict) else _text(value).lower()
        if channel:
            return channel
    if _matrix_room_id_from_session(source_session):
        return "matrix"
    raw = source_session.strip()
    if ":" in raw:
        prefix = raw.split(":", 1)[0].strip().lower()
        if prefix and not prefix.startswith("!"):
            return prefix
    return ""


def _matrix_room_id_from_session(value: str) -> str:
    # 逻辑说明：`_matrix_room_id_from_session` 去除兼容前缀并仅接受 `!` 开头的稳定 Matrix room ID。
    raw = (value or "").strip()
    if raw.startswith("matrix:"):
        raw = raw[len("matrix:") :]
    if raw.startswith("room:"):
        raw = raw[len("room:") :]
    return raw if raw.startswith("!") else ""


def _canonical_session(channel: str, session: str) -> str:
    # 逻辑说明：`_canonical_session` 补齐 channel 前缀；Matrix 特别规范为 `matrix:<room-id>`。
    raw = session.strip()
    source_channel = channel.strip().lower()
    if source_channel == "matrix":
        room_id = _matrix_room_id_from_session(raw)
        return f"matrix:{room_id}" if room_id else raw
    if source_channel and raw.startswith(f"{source_channel}:"):
        return raw
    return f"{source_channel}:{raw}" if source_channel else raw


def _route_bool(route: dict[str, Any], *names: str) -> bool | None:
    # 逻辑说明：`_route_bool` 按字段顺序解析常见布尔写法；缺失返回 None 以保留默认策略。
    for name in names:
        if name not in route:
            continue
        value = route.get(name)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"1", "true", "yes", "y", "on"}:
                return True
            if text in {"0", "false", "no", "n", "off"}:
                return False
        if value is not None:
            return bool(value)
    return None


def _normalize_reply_route(route: dict[str, Any]) -> dict[str, Any]:
    # 逻辑说明：`_normalize_reply_route` 合并 reply route 的兼容别名，并只输出统一字段供跨渠道回复。
    channel = _text(route.get("channel")).lower()
    target_user = _text(
        route.get("targetUser")
        or route.get("target_user")
        or route.get("userId")
        or route.get("user_id"),
    )
    target_session = _text(
        route.get("targetSession")
        or route.get("target_session")
        or route.get("sessionId")
        or route.get("session_id"),
    )
    if channel == "matrix":
        target = _text(
            route.get("target")
            or route.get("roomId")
            or route.get("room_id")
            or route.get("targetRoom")
            or route.get("target_room")
            or target_session,
        )
        room_id = _matrix_room_id_from_session(target)
        if not room_id:
            return {}
        normalized: dict[str, Any] = {
            "channel": "matrix",
            "targetSession": room_id,
        }
        if target_user:
            normalized["targetUser"] = target_user
    else:
        if not (channel and target_user and target_session):
            return {}
        normalized = {
            "channel": channel,
            "targetUser": target_user,
            "targetSession": target_session,
        }
    mention_sender = _route_bool(route, "mentionSender", "mention_sender", "atSender", "at_sender")
    if mention_sender is not None:
        normalized["mentionSender"] = mention_sender
    return normalized


def _visible_reply_route_error(message_text: str, reply_route: dict[str, Any]) -> str:
    # 逻辑说明：`_visible_reply_route_error` 要求外部渠道回路信息在可见正文中出现，防止 Leader 丢失回复路径。
    channel = _text(reply_route.get("channel")).lower()
    if channel and channel != "matrix" and channel not in message_text:
        return "PROJECT_REQUESTED message text must include replyRoute.channel so the task-room Leader can pass it to projectflow"
    target_user = _text(reply_route.get("targetUser") or reply_route.get("target_user"))
    if channel and channel != "matrix" and target_user and target_user not in message_text:
        return "PROJECT_REQUESTED message text must include replyRoute.targetUser so the task-room Leader can pass it to projectflow"
    target_session = _text(reply_route.get("targetSession") or reply_route.get("target_session"))
    if target_session and target_session not in message_text:
        return "PROJECT_REQUESTED message text must include replyRoute.targetSession so the task-room Leader can pass it to projectflow"
    return ""


def _self_trigger_intent(
    arguments: dict[str, Any],
    route: dict[str, Any],
    *,
    channel: str,
    target_id: str,
) -> dict[str, Any] | None:
    # 逻辑说明：`_self_trigger_intent` 只为允许的项目请求生成自触发 metadata，并拒绝来源/目标不一致。
    message_type = _message_type(arguments)
    if message_type not in SELF_TRIGGER_MESSAGE_TYPES:
        return None
    source_session = _source_value(arguments, route)
    if not source_session:
        return None
    source_channel = _source_channel(arguments, route, source_session)
    if not source_channel or channel.strip().lower() != "matrix":
        return None
    target_session = f"matrix:{target_id}"
    canonical_source_session = _canonical_session(source_channel, source_session)
    if canonical_source_session == target_session:
        return None

    current_agent = _current_agent(arguments)
    source_agent = _agent_from(arguments.get("sender")) or _agent_from(arguments.get("source")) or current_agent
    target_agent = _agent_from(arguments.get("target")) or current_agent
    if source_agent != target_agent:
        return None
    if not _is_matrix_user_id(source_agent):
        return {
            "status": "invalid",
            "kind": "self_cross_session",
            "type": message_type,
            "error": "PROJECT_REQUESTED sender.agent and agentId must be the current runtime Matrix user id, not a role or workspace name",
        }
    reply_route = _normalize_reply_route(route)
    if not reply_route:
        return {
            "status": "invalid",
            "kind": "self_cross_session",
            "type": message_type,
            "error": "PROJECT_REQUESTED requires structured replyRoute with channel and targetSession",
        }

    return {
        "status": "requested",
        "kind": "self_cross_session",
        "type": message_type,
        "agentId": target_agent,
        "sourceChannel": source_channel,
        "targetChannel": "matrix",
        "sourceSession": canonical_source_session,
        "targetSession": target_session,
        "sourceRoomId": source_session.strip(),
        "targetRoomId": target_id,
        "replyRoute": reply_route,
    }


def message(arguments: dict[str, Any], deps: MessageToolDeps) -> dict[str, Any]:
    """发送一条受策略约束的协作消息，并返回可序列化的工具结果。

    对 Matrix 先规范 route/room，再阻止 ping-pong 与非法 self-trigger；只有发送成功
    才记录到 session。其他 channel 委托 QwenPaw 的原生消息实现，保持同一 MCP
    schema 而不在这里复制每个平台协议。
    """
    # 逻辑说明：`message` 解析 route 与正文、执行目标/mention/ping-pong 校验，发送成功后才补写 session。
    action = arguments.get("action") or "send"
    route = deps.reply_route(arguments)
    channel = str(arguments.get("channel") or route.get("channel") or "matrix")
    if action != "send":
        return {"ok": False, "tool": "message", "error": f"unsupported action: {action}"}
    message_text = _message_text(arguments)
    if channel != "matrix":
        return deps.qwenpaw_message(arguments, route, channel, message_text)

    try:
        target_kind, target_id = deps.matrix_target(_target_value(arguments, route))
    except ValueError as exc:
        return {"ok": False, "tool": "message", "error": str(exc)}
    if target_kind == "user":
        return {"ok": False, "tool": "message", "error": "Matrix user targets are not supported yet"}

    mentions = deps.mentions(message_text, target_id)
    blocked = deps.ping_pong_error(message_text, mentions)
    if blocked:
        return {"ok": False, "tool": "message", "error": blocked}

    content = deps.matrix_content(message_text, mentions)
    trigger = _self_trigger_intent(arguments, route, channel=channel, target_id=target_id)
    if _message_type(arguments) in SELF_TRIGGER_MESSAGE_TYPES and trigger is None:
        return {
            "ok": False,
            "tool": "message",
            "action": "send",
            "channel": "matrix",
            "target": f"room:{target_id}",
            "error": "PROJECT_REQUESTED must be sent as a same-agent Matrix self-trigger with sender.session, target room, matching sender.agent/agentId, and structured replyRoute",
        }
    if trigger is not None and trigger.get("status") == "invalid":
        return {
            "ok": False,
            "tool": "message",
            "action": "send",
            "channel": "matrix",
            "target": f"room:{target_id}",
            "trigger": trigger,
            "error": str(trigger.get("error") or "invalid PROJECT_REQUESTED trigger"),
        }
    if trigger is not None:
        visible_route_error = _visible_reply_route_error(message_text, _dict(trigger.get("replyRoute")))
        if visible_route_error:
            return {
                "ok": False,
                "tool": "message",
                "action": "send",
                "channel": "matrix",
                "target": f"room:{target_id}",
                "trigger": trigger,
                "error": visible_route_error,
            }
    if trigger is not None:
        content = dict(content)
        content[TEAMHARNESS_TRIGGER_CONTENT_KEY] = dict(trigger)
    base: dict[str, Any] = {
        "ok": True,
        "tool": "message",
        "action": "send",
        "channel": "matrix",
        "target": f"room:{target_id}",
        "targetKind": "room",
        "mentions": mentions,
        "content": content,
    }
    if trigger is not None:
        base["trigger"] = trigger
    if arguments.get("dryRun"):
        base["dryRun"] = True
        return base

    homeserver = os.getenv("AGENTTEAMS_MATRIX_URL", "").rstrip("/")
    token = os.getenv("AGENTTEAMS_WORKER_MATRIX_TOKEN", "")
    if not homeserver or not token:
        return {"ok": False, "tool": "message", "error": "AGENTTEAMS_MATRIX_URL and AGENTTEAMS_WORKER_MATRIX_TOKEN are required"}

    room_id = urllib.parse.quote(target_id, safe="")
    txn_id = f"teamharness-{os.getpid()}-{int(time.time() * 1000)}"
    url = f"{homeserver}/_matrix/client/v3/rooms/{room_id}/send/m.room.message/{txn_id}"
    request = urllib.request.Request(
        url,
        data=json.dumps(content).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="PUT",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8") or "{}")
        base["messageId"] = data.get("event_id")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:200]
        return {"ok": False, "tool": "message", "error": f"Matrix API error: HTTP {exc.code}: {body}"}
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return {"ok": False, "tool": "message", "error": f"Matrix API error: {exc}"}
    if trigger is not None:
        trigger["status"] = "sent"
        trigger["targetCurrentEvent"] = base.get("messageId")
        base["delivery"] = {"sent": "matrix_self_trigger"}
        base["context"] = {"via": "matrix_current_event"}
        return base
    account_id = str(arguments.get("agentId") or arguments.get("agent_id") or arguments.get("accountId") or arguments.get("account_id") or "default").strip() or "default"
    try:
        base["sessionRecorded"] = deps.record_matrix_outbound_to_session(
            target_id,
            message_text,
            base.get("messageId"),
            account_id,
        )
    except Exception:
        base["sessionRecorded"] = False
        base["warning"] = "message sent, but local session record failed"
    return base
