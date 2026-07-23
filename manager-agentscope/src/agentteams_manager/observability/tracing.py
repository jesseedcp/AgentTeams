"""Optional OpenTelemetry tracing compatible with CMS deployments."""

from __future__ import annotations

import os
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class TracerHandle:
    tracer: Any | None
    provider: Any | None
    is_noop: bool

    def start_as_current_span(self, name: str, **kwargs: Any):
        if self.is_noop or self.tracer is None:
            return nullcontext()
        return self.tracer.start_as_current_span(name, **kwargs)

    def shutdown(self) -> None:
        if self.provider is not None:
            self.provider.shutdown()


def build_tracer_from_env() -> TracerHandle:
    enabled = os.environ.get(
        "AGENTTEAMS_CMS_TRACES_ENABLED",
        "",
    ).casefold() in {"1", "true", "yes", "on"}
    if not enabled:
        return TracerHandle(
            tracer=None,
            provider=None,
            is_noop=True,
        )

    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(
        resource=Resource.create(
            {"service.name": "agentteams-manager"},
        ),
    )
    endpoint = os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ) or os.environ.get("AGENTTEAMS_CMS_OTLP_ENDPOINT")
    if endpoint:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )

        provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(endpoint=endpoint.rstrip("/") + "/v1/traces"),
            ),
        )
    trace.set_tracer_provider(provider)
    return TracerHandle(
        tracer=provider.get_tracer("agentteams-manager"),
        provider=provider,
        is_noop=False,
    )

