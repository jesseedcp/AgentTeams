"""Small synchronous client for QwenPaw's localhost management API."""

# 初学者导读：这个模块不是直接调用大模型，而是管理同一容器里的 QwenPaw
# 进程。Controller 发布“应该使用哪个模型、开放哪些 MCP 工具”等期望状态后，
# RuntimeUpdater 会通过这里的 localhost HTTP API 把配置写进正在运行的 QwenPaw。
# 之所以不直接改 QwenPaw 的文件，是因为 API 可以校验写入结果，也能避免运行中
# 的内存状态与磁盘状态互相矛盾。这里是 Worker 内部边界，不是 Manager 管理 API。

from __future__ import annotations

import json
import time
from typing import Any, Iterable
import urllib.error
import urllib.parse
import urllib.request


class QwenPawApiError(RuntimeError):
    """Raised when QwenPaw rejects or fails to persist desired state."""


class QwenPawApiClient:
    """把底层 HTTP 请求封装成可验证的 QwenPaw 配置操作。

    ``base_url`` 通常指向当前 Pod 内的 ``127.0.0.1``。调用方仍需负责判断
    哪一代 Controller 配置应该生效；本类只负责发送、检查和把网络错误转换成
    ``QwenPawApiError``，避免上层误把一次超时当成成功。
    """
    def __init__(self, base_url: str, timeout: float = 10) -> None:
        # 逻辑说明：规范化本机 API 地址并保存超时；这里只记录连接参数，不会立即发起网络请求。
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        payload: Any | None = None,
    ) -> Any:
        # 逻辑说明：把可选 JSON 编码为 HTTP 请求并统一解析响应；网络、状态码或 JSON 错误都转换成可识别异常。
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=body,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raise QwenPawApiError(
                f"QwenPaw API {method} {path} failed with HTTP {exc.code}",
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise QwenPawApiError(
                f"QwenPaw API {method} {path} unavailable: {type(exc).__name__}",
            ) from exc
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise QwenPawApiError(
                f"QwenPaw API {method} {path} returned invalid JSON",
            ) from exc

    def get_version(self) -> str:
        # 逻辑说明：读取本机 QwenPaw 版本并拒绝空值，避免把“API 有响应”误判为“版本正确”。
        payload = self._request("GET", "/api/version")
        version = str((payload or {}).get("version") or "").strip()
        if not version:
            raise QwenPawApiError("QwenPaw API did not return a version")
        return version

    def require_version(self, expected: str) -> None:
        # 逻辑说明：比较实际与期望版本；不一致立即失败，避免向不兼容的管理 API 写配置。
        actual = self.get_version()
        if actual != expected:
            raise QwenPawApiError(
                f"expected QwenPaw {expected}, API reported {actual}",
            )

    def get_channel(self, channel: str) -> dict[str, Any]:
        # 逻辑说明：读取指定 channel；404 表示尚未配置并返回空字典，其他错误继续交给上层处理。
        try:
            return self._request(
                "GET",
                f"/api/config/channels/{urllib.parse.quote(channel, safe='')}",
            )
        except QwenPawApiError as exc:
            if "HTTP 404" in str(exc):
                return {}
            raise

    def put_channel(
        self,
        channel: str,
        desired: dict[str, Any],
        *,
        secret_fields: Iterable[str] = (),
    ) -> dict[str, Any]:
        """写入 channel 后读回核对，并在请求未携带秘密时保留已有秘密字段。

        API 的成功状态只说明请求被接受，不代表持久状态完全一致；读回检查能让
        RuntimeUpdater 在重启后重试，而不是把部分应用误报为完成。
        """
        # 逻辑说明：合并需保留的秘密字段后 PUT，再 GET 比较非秘密字段；不一致时不会提交当前 generation。
        current = self.get_channel(channel)
        payload = dict(desired)
        secret_fields = set(secret_fields)
        for field in secret_fields:
            if not payload.get(field) and current.get(field):
                payload[field] = current[field]
        path = f"/api/config/channels/{urllib.parse.quote(channel, safe='')}"
        self._request("PUT", path, payload)
        actual = self.get_channel(channel)
        mismatched = sorted(
            key
            for key, value in payload.items()
            if key not in secret_fields and actual.get(key) != value
        )
        if mismatched:
            raise QwenPawApiError(
                f"QwenPaw channel {channel} readback mismatch: {', '.join(mismatched)}",
            )
        return actual

    def get_acl(self, channel: str) -> dict[str, Any]:
        # 逻辑说明：读取 channel 当前白名单和黑名单，供差异更新以及最终校验使用。
        return self._request(
            "GET",
            f"/api/access-control/{urllib.parse.quote(channel, safe='')}",
        )

    def reconcile_acl(
        self,
        channel: str,
        whitelist: Iterable[str],
        blacklist: Iterable[str],
    ) -> dict[str, Any]:
        """按集合差异增删 allow/deny 项，再读回验证最终 ACL。"""
        # 逻辑说明：计算期望集合与现状的增删差异，逐批调用 ACL API 后读回；最终集合不同即报错。
        desired_white = set(whitelist)
        desired_black = set(blacklist)
        current = self.get_acl(channel)
        current_white = set((current.get("whitelist") or {}).keys())
        current_black = set((current.get("blacklist") or {}).keys())

        self._acl_action(
            "/api/access-control/whitelist/remove",
            channel,
            current_white - desired_white,
        )
        self._acl_action(
            "/api/access-control/blacklist/remove",
            channel,
            current_black - desired_black,
        )
        self._acl_action(
            "/api/access-control/whitelist/add",
            channel,
            desired_white - current_white,
        )
        self._acl_action(
            "/api/access-control/blacklist/add",
            channel,
            desired_black - current_black,
        )

        actual = self.get_acl(channel)
        if set((actual.get("whitelist") or {}).keys()) != desired_white:
            raise QwenPawApiError(
                f"QwenPaw ACL {channel} whitelist readback mismatch",
            )
        if set((actual.get("blacklist") or {}).keys()) != desired_black:
            raise QwenPawApiError(
                f"QwenPaw ACL {channel} blacklist readback mismatch",
            )
        return actual

    def _acl_action(
        self,
        path: str,
        channel: str,
        user_ids: Iterable[str],
    ) -> None:
        # 逻辑说明：将用户 ID 排序为 API 条目后批量发送；空集合跳过，避免无意义请求。
        entries = [
            {"channel": channel, "user_id": user_id}
            for user_id in sorted(user_ids)
        ]
        if entries:
            self._request("POST", path, {"entries": entries})

    def list_mcp(self) -> list[dict[str, Any]]:
        return self._request("GET", "/api/mcp")

    def get_mcp(self, client_key: str) -> dict[str, Any]:
        # 逻辑说明：按经过 URL 转义的 client key 读取单个 MCP 客户端的持久配置。
        return self._request(
            "GET",
            f"/api/mcp/{urllib.parse.quote(client_key, safe='')}",
        )

    def create_mcp(
        self,
        client_key: str,
        client: dict[str, Any],
    ) -> dict[str, Any]:
        # 逻辑说明：创建 MCP 客户端后调用统一读回校验，确保服务实际保存了期望字段。
        self._request(
            "POST",
            "/api/mcp",
            {"client_key": client_key, "client": client},
        )
        return self._verify_mcp(client_key, client)

    def update_mcp(
        self,
        client_key: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        # 逻辑说明：对既有 MCP 客户端提交局部更新，再读取完整对象验证本次更新字段。
        self._request(
            "PUT",
            f"/api/mcp/{urllib.parse.quote(client_key, safe='')}",
            updates,
        )
        return self._verify_mcp(client_key, updates)

    def _verify_mcp(
        self,
        client_key: str,
        desired: dict[str, Any],
    ) -> dict[str, Any]:
        # 逻辑说明：读取服务端 MCP 对象并逐字段比较；缺失或值不同会阻止上层把配置视为已应用。
        actual = self.get_mcp(client_key)
        observable = {
            "name",
            "description",
            "enabled",
            "transport",
            "url",
            "command",
            "args",
            "cwd",
            "tools",
        }
        mismatched = sorted(
            key
            for key in observable & desired.keys()
            if actual.get(key) != desired.get(key)
        )
        if mismatched:
            raise QwenPawApiError(
                f"QwenPaw MCP {client_key} readback mismatch: {', '.join(mismatched)}",
            )
        return actual

    def delete_mcp(self, client_key: str) -> None:
        # 逻辑说明：删除指定 MCP 客户端后检查列表；对象仍存在就明确报告持久化失败。
        self._request(
            "DELETE",
            f"/api/mcp/{urllib.parse.quote(client_key, safe='')}",
        )
        if any(item.get("key") == client_key for item in self.list_mcp()):
            raise QwenPawApiError(
                f"QwenPaw MCP {client_key} delete readback mismatch",
            )

    def get_mcp_policy(self, client_key: str) -> dict[str, Any]:
        # 逻辑说明：读取 MCP 工具授权策略，供更新后的读回校验使用。
        return self._request(
            "GET",
            f"/api/mcp/policy/{urllib.parse.quote(client_key, safe='')}",
        )

    def list_mcp_tools(self, client_key: str) -> list[dict[str, Any]]:
        # 逻辑说明：查询 MCP 服务已发现的工具；结果用于判断服务是否完成异步启动。
        return self._request(
            "GET",
            f"/api/mcp/tools/{urllib.parse.quote(client_key, safe='')}",
        )

    def wait_for_mcp_tools(
        self,
        client_key: str,
        *,
        timeout: float = 30,
        interval: float = 0.5,
    ) -> list[dict[str, Any]]:
        # 逻辑说明：在截止时间内轮询工具列表；临时 API 错误会重试，超时则携带最后错误失败。
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                tools = self.list_mcp_tools(client_key)
                if tools:
                    return tools
            except QwenPawApiError as exc:
                last_error = exc
            time.sleep(interval)
        raise QwenPawApiError(
            f"QwenPaw MCP client {client_key} did not become callable: "
            f"{last_error or 'no tools returned'}",
        )

    def put_mcp_policy(
        self,
        client_key: str,
        desired: dict[str, Any],
    ) -> dict[str, Any]:
        # 逻辑说明：写入 MCP 授权策略后读回逐字段验证，防止权限配置仅返回成功却未持久化。
        self._request(
            "PUT",
            f"/api/mcp/policy/{urllib.parse.quote(client_key, safe='')}",
            desired,
        )
        actual = self.get_mcp_policy(client_key)
        mismatched = sorted(
            key for key, value in desired.items() if actual.get(key) != value
        )
        if mismatched:
            raise QwenPawApiError(
                f"QwenPaw MCP policy {client_key} readback mismatch: "
                f"{', '.join(mismatched)}",
            )
        return actual

    def configure_active_model(
        self,
        provider_id: str,
        model: str,
        *,
        base_url: str = "",
        api_key: str = "",
        provider_name: str = "",
        chat_model: str = "OpenAIChatModel",
    ) -> dict[str, Any]:
        """确保 provider/model 存在并切换默认 Agent，最后逐层读回验证。

        Worker 实际请求经过 Higress 模型网关；此处配置的是 QwenPaw 如何找到该
        provider，而不是把云厂商 key 暴露给 Matrix 或 Agent 提示词。
        """
        # 逻辑说明：补建或更新 provider/model，切换默认 Agent 后逐层读回；任一步不一致都中止配置提交。
        providers = self._request("GET", "/api/models")
        provider = next(
            (item for item in providers if item.get("id") == provider_id),
            None,
        )
        if provider is None:
            self._request(
                "POST",
                "/api/models/custom-providers",
                {
                    "id": provider_id,
                    "name": provider_name or provider_id,
                    "default_base_url": base_url,
                    "chat_model": chat_model,
                    "models": [{"id": model, "name": model}],
                },
            )
        else:
            known_models = {
                str(item.get("id"))
                for item in list(provider.get("models") or [])
                + list(provider.get("extra_models") or [])
            }
            if model not in known_models:
                self._request(
                    "POST",
                    f"/api/models/{urllib.parse.quote(provider_id, safe='')}/models",
                    {"id": model, "name": model},
                )
        config_payload: dict[str, Any] = {"chat_model": chat_model}
        if api_key:
            config_payload["api_key"] = api_key
        if base_url:
            config_payload["base_url"] = base_url
        self._request(
            "PUT",
            f"/api/models/{urllib.parse.quote(provider_id, safe='')}/config",
            config_payload,
        )
        self._request(
            "PUT",
            "/api/models/active",
            {
                "provider_id": provider_id,
                "model": model,
                "scope": "agent",
                "agent_id": "default",
            },
        )
        actual = self._request(
            "GET",
            "/api/models/active?scope=agent&agent_id=default",
        )
        active = (actual or {}).get("active_llm") or {}
        if active.get("provider_id") != provider_id or active.get("model") != model:
            raise QwenPawApiError("QwenPaw active model readback mismatch")
        providers = self._request("GET", "/api/models")
        provider = next(
            (item for item in providers if item.get("id") == provider_id),
            None,
        )
        if provider is None:
            raise QwenPawApiError("QwenPaw model provider readback mismatch")
        known_models = {
            str(item.get("id"))
            for item in list(provider.get("models") or [])
            + list(provider.get("extra_models") or [])
        }
        if model not in known_models:
            raise QwenPawApiError("QwenPaw provider model readback mismatch")
        if base_url and str(provider.get("base_url") or "").rstrip("/") != base_url.rstrip("/"):
            raise QwenPawApiError("QwenPaw provider base URL readback mismatch")
        return actual

    def configure_agent(
        self,
        agent_id: str,
        updates: dict[str, Any],
    ) -> dict[str, Any]:
        # 逻辑说明：合并目标 Agent 的原配置与更新，写入后逐字段读回；不一致时向上报告失败。
        encoded = urllib.parse.quote(agent_id, safe="")
        current = self._request("GET", f"/api/agents/{encoded}")
        payload = {**current, **updates, "id": agent_id}
        self._request("PUT", f"/api/agents/{encoded}", payload)
        actual = self._request("GET", f"/api/agents/{encoded}")
        mismatched = [
            key for key, value in updates.items() if actual.get(key) != value
        ]
        if mismatched:
            raise QwenPawApiError(
                f"QwenPaw agent {agent_id} readback mismatch: "
                f"{', '.join(sorted(mismatched))}",
            )
        return actual

    def disable_agent_if_present(
        self,
        agent_id: str,
        *,
        retries: int = 120,
        retry_delay: float = 1.0,
    ) -> bool:
        """幂等停用临时 Agent；HTTP 409 表示它仍忙，短暂等待后再试。"""
        # 逻辑说明：仅对存在且启用的 Agent 停用；占用冲突按次数退避，最终读回保证确已停用。
        agents = self._request("GET", "/api/agents").get("agents") or []
        current = next(
            (agent for agent in agents if agent.get("id") == agent_id),
            None,
        )
        if current is None:
            return False
        if current.get("enabled", True):
            encoded = urllib.parse.quote(agent_id, safe="")
            for attempt in range(retries + 1):
                try:
                    self._request(
                        "PATCH",
                        f"/api/agents/{encoded}/toggle",
                        {"enabled": False},
                    )
                    break
                except QwenPawApiError as exc:
                    if "HTTP 409" not in str(exc) or attempt == retries:
                        raise
                    time.sleep(retry_delay)
        agents = self._request("GET", "/api/agents").get("agents") or []
        actual = next(
            (agent for agent in agents if agent.get("id") == agent_id),
            None,
        )
        if actual is None or actual.get("enabled", True):
            raise QwenPawApiError(
                f"QwenPaw agent {agent_id} disable readback mismatch",
            )
        return True

    def sync_plugin(self, plugin_id: str) -> dict[str, Any]:
        # 逻辑说明：触发指定内置插件从磁盘同步运行状态，并把 API 的同步结果返回调用方。
        encoded = urllib.parse.quote(plugin_id, safe="")
        return self._request("POST", f"/api/{encoded}/sync", {})

    def refresh_and_enable_skills(
        self,
        skill_names: Iterable[str],
    ) -> dict[str, Any]:
        # 逻辑说明：先刷新技能索引，再批量启用去重后的技能；任一失败会汇总为明确异常。
        self._request("POST", "/api/skills/refresh", {})
        names = sorted(set(skill_names))
        if not names:
            return {"results": {}}
        result = self._request("POST", "/api/skills/batch-enable", names)
        failed = sorted(
            name
            for name, value in (result.get("results") or {}).items()
            if not isinstance(value, dict) or not value.get("success")
        )
        if failed:
            raise QwenPawApiError(
                f"QwenPaw skill enable failed: {', '.join(failed)}",
            )
        return result
