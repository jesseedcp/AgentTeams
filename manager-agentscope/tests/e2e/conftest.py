"""Shared opt-in harness for live Kubernetes acceptance tests."""

from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import (
    ProxyHandler,
    Request,
    build_opener,
)

import pytest


@dataclass(frozen=True)
class HTTPResult:
    status: int
    payload: dict[str, Any]


class K8sHarness:
    """Small, secret-safe client for the existing local Kind deployment."""

    def __init__(self) -> None:
        self.namespace = os.environ.get(
            "AGENTTEAMS_E2E_NAMESPACE",
            "agentteams-k8s-b35deb9",
        )
        self.gateway_url = os.environ.get(
            "AGENTTEAMS_E2E_GATEWAY_URL",
            "http://127.0.0.1:18388",
        ).rstrip("/")
        self._opener = build_opener(ProxyHandler({}))

    @property
    def enabled(self) -> bool:
        if os.environ.get("AGENTTEAMS_E2E_K8S") != "1":
            return False
        if shutil.which("kubectl") is None:
            return False
        result = subprocess.run(
            ["kubectl", "get", "namespace", self.namespace],
            capture_output=True,
            check=False,
            encoding="utf-8",
        )
        return result.returncode == 0

    def kubectl(
        self,
        *arguments: str,
        check: bool = True,
        timeout: float = 180,
    ) -> str:
        return subprocess.run(
            ["kubectl", "-n", self.namespace, *arguments],
            capture_output=True,
            check=check,
            encoding="utf-8",
            timeout=timeout,
        ).stdout

    def kubectl_json(self, *arguments: str) -> dict[str, Any]:
        return json.loads(self.kubectl(*arguments, "-o", "json"))

    def try_kubectl_json(
        self,
        *arguments: str,
    ) -> dict[str, Any] | None:
        result = subprocess.run(
            [
                "kubectl",
                "-n",
                self.namespace,
                *arguments,
                "-o",
                "json",
            ],
            capture_output=True,
            check=False,
            encoding="utf-8",
        )
        return json.loads(result.stdout) if result.returncode == 0 else None

    def secret_value(self, name: str, key: str) -> str:
        secret = self.kubectl_json("get", "secret", name)
        encoded = str(secret["data"][key])
        return base64.b64decode(encoded).decode("utf-8")

    def pod_env(self, name: str, variable: str) -> str:
        return self.kubectl(
            "exec",
            name,
            "--",
            "printenv",
            variable,
        ).strip()

    def wait(
        self,
        predicate: Callable[[], Any],
        *,
        timeout: float = 240,
        interval: float = 2,
        description: str,
    ) -> Any:
        deadline = time.monotonic() + timeout
        last_error: BaseException | None = None
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
            except (
                HTTPError,
                json.JSONDecodeError,
                subprocess.CalledProcessError,
                TimeoutError,
            ) as error:
                last_error = error
            time.sleep(interval)
        detail = f": {last_error}" if last_error is not None else ""
        raise TimeoutError(f"timed out waiting for {description}{detail}")

    def admin_request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        token: str | None = None,
        idempotency_key: str | None = None,
    ) -> HTTPResult:
        headers = {"Accept": "application/json"}
        if token is None:
            token = self.pod_env(
                "agentteams-manager",
                "AGENTTEAMS_MANAGER_ADMIN_TOKEN",
            )
        if token:
            headers["Authorization"] = f"Bearer {token}"
        data = None
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        if method != "GET":
            headers["Idempotency-Key"] = (
                idempotency_key or f"e2e-{uuid.uuid4().hex}"
            )
            if data is None:
                data = b"{}"
                headers["Content-Type"] = "application/json"
        request = Request(
            f"{self.gateway_url}/manager-admin/api/v1/{path}",
            data=data,
            headers=headers,
            method=method,
        )
        return self._json_request(request)

    def admin_wait_for_success(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None,
        *,
        expected_status: int,
        timeout: float = 300,
    ) -> HTTPResult:
        """Resume one idempotent admin mutation until effects converge."""
        idempotency_key = f"e2e-{uuid.uuid4().hex}"

        def completed() -> HTTPResult | None:
            result = self.admin_request(
                method,
                path,
                payload,
                idempotency_key=idempotency_key,
            )
            if (
                result.status == 202
                and result.payload.get("error", {}).get("code")
                == "effect_pending"
            ):
                return None
            if result.status != expected_status:
                raise AssertionError(
                    f"admin mutation returned HTTP {result.status}: "
                    f"{result.payload}",
                )
            return result

        return self.wait(
            completed,
            timeout=timeout,
            interval=2,
            description=f"{method} {path} to converge",
        )

    def matrix_context(self) -> tuple[str, str, str]:
        username = self.pod_env(
            "agentteams-manager",
            "AGENTTEAMS_ADMIN_USER",
        )
        password = self.secret_value(
            "agentteams-runtime-env",
            "AGENTTEAMS_ADMIN_PASSWORD",
        )
        login = self.matrix_request(
            "POST",
            "/_matrix/client/v3/login",
            payload={
                "type": "m.login.password",
                "identifier": {
                    "type": "m.id.user",
                    "user": username,
                },
                "password": password,
                "initial_device_display_name": "AgentTeams K8s E2E",
            },
        )
        manager = self.kubectl_json("get", "manager", "default")
        return (
            str(login["access_token"]),
            str(manager["status"]["roomID"]),
            str(manager["status"]["matrixUserID"]),
        )

    def matrix_request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        payload: dict[str, Any] | None = None,
        query: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        suffix = f"?{urlencode(query)}" if query else ""
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        data = None
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        result = self._json_request(
            Request(
                f"{self.gateway_url}{path}{suffix}",
                data=data,
                headers=headers,
                method=method,
            ),
        )
        if result.status >= 400:
            raise RuntimeError(
                f"Matrix request failed with HTTP {result.status}: "
                f"{result.payload}",
            )
        return result.payload

    def matrix_send(
        self,
        token: str,
        room_id: str,
        body: str,
    ) -> int:
        started_at = int(time.time() * 1000)
        self.matrix_request(
            "PUT",
            (
                f"/_matrix/client/v3/rooms/{quote(room_id, safe='')}"
                f"/send/m.room.message/e2e-{uuid.uuid4().hex}"
            ),
            token=token,
            payload={"msgtype": "m.text", "body": body},
        )
        return started_at

    def matrix_wait_for_reply(
        self,
        token: str,
        room_id: str,
        manager_user_id: str,
        started_at: int,
        predicate: Callable[[str], bool],
        *,
        timeout: float = 180,
    ) -> str:
        encoded_room = quote(room_id, safe="")

        def find_reply() -> str | None:
            response = self.matrix_request(
                "GET",
                (
                    f"/_matrix/client/v3/rooms/{encoded_room}/messages"
                ),
                token=token,
                query={"dir": "b", "limit": "100"},
            )
            for event in response.get("chunk", ()):
                if event.get("sender") != manager_user_id:
                    continue
                if int(event.get("origin_server_ts", 0)) < started_at - 500:
                    continue
                content = event.get("content") or {}
                edited = content.get("m.new_content") or {}
                body = str(edited.get("body") or content.get("body") or "")
                if predicate(body):
                    return body
            return None

        return str(
            self.wait(
                find_reply,
                timeout=timeout,
                interval=1,
                description="a matching Matrix Manager reply",
            ),
        )

    def _json_request(self, request: Request) -> HTTPResult:
        try:
            with self._opener.open(request, timeout=30) as response:
                raw = response.read()
                return HTTPResult(
                    response.status,
                    json.loads(raw) if raw else {},
                )
        except HTTPError as error:
            raw = error.read()
            return HTTPResult(
                error.code,
                json.loads(raw) if raw else {},
            )


@pytest.fixture(scope="session")
def k8s_harness() -> K8sHarness:
    return K8sHarness()
