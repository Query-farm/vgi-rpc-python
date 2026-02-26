# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""OpenTelemetry server-side instrumentation for vgi-rpc.

Provides ``OtelConfig`` and ``instrument_server()`` for adding distributed
tracing (spans) and metrics (counters, histograms) to ``RpcServer`` dispatch.

Requires ``pip install vgi-rpc[otel]`` (opentelemetry-api + opentelemetry-sdk).

Usage::

    from vgi_rpc.otel import OtelConfig, instrument_server

    server = RpcServer(MyProtocol, MyImpl())
    instrument_server(server)  # uses global TracerProvider / MeterProvider
"""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping
from contextvars import Token
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from opentelemetry import context as otel_context
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.metrics import Counter, Histogram, Meter, MeterProvider, get_meter_provider
from opentelemetry.trace import SpanKind, StatusCode, Tracer, TracerProvider, get_tracer_provider

from vgi_rpc.rpc._common import (
    AuthContext,
    CallStatistics,
    HookToken,
    _current_trace_headers,
    _register_dispatch_hook,
)

if TYPE_CHECKING:
    import falcon

    from vgi_rpc.rpc._server import RpcServer
    from vgi_rpc.rpc._types import RpcMethodInfo

_logger = logging.getLogger("vgi_rpc.otel")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OtelConfig:
    """Configuration for OpenTelemetry instrumentation.

    Attributes:
        tracer_provider: Custom ``TracerProvider``; uses the global provider when ``None``.
        meter_provider: Custom ``MeterProvider``; uses the global provider when ``None``.
        enable_tracing: Enable span creation (default ``True``).
        enable_metrics: Enable counter/histogram recording (default ``True``).
        record_exceptions: Record exceptions on error spans (default ``True``).
        custom_attributes: Extra span/metric attributes merged into every dispatch.

    """

    tracer_provider: TracerProvider | None = None
    meter_provider: MeterProvider | None = None
    enable_tracing: bool = True
    enable_metrics: bool = True
    record_exceptions: bool = True
    custom_attributes: Mapping[str, str] = field(default_factory=dict)


def instrument_server(server: RpcServer, config: OtelConfig | None = None) -> RpcServer:
    """Attach OpenTelemetry tracing and metrics to a server.

    Must be called before ``serve()`` — not thread-safe during dispatch.

    Args:
        server: The ``RpcServer`` to instrument.
        config: Optional configuration; uses global providers and defaults when ``None``.

    Returns:
        The same *server* instance (for chaining).

    """
    if config is None:
        config = OtelConfig()
    hook = _OtelDispatchHook(config, server.protocol_name, server.server_id)
    server._dispatch_hook = _register_dispatch_hook(server._dispatch_hook, hook)
    return server


# ---------------------------------------------------------------------------
# Client-side trace injection helper (called from _wire.py)
# ---------------------------------------------------------------------------


class _DictCarrier(dict[str, str]):
    """Carrier for propagate.inject() — plain dict."""


def _inject_trace_context() -> dict[bytes, bytes] | None:
    """Return trace context metadata keys if an active span exists, else ``None``.

    Best-effort: returns ``None`` if there is no current span or if injection
    produces no headers.
    """
    span = trace.get_current_span()
    if not span.get_span_context().is_valid:
        return None
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    if not carrier:
        return None
    return {k.encode(): v.encode() for k, v in carrier.items()}


# ---------------------------------------------------------------------------
# Falcon middleware for HTTP W3C trace propagation
# ---------------------------------------------------------------------------


class _FalconHeaderGetter:
    """Adapts falcon.Request headers to the OTel textmap Getter interface."""

    def get(self, carrier: Any, key: str) -> list[str] | None:
        """Get header values for a key."""
        value = carrier.get_header(key)
        if value is None:
            return None
        return [value]

    def keys(self, carrier: Any) -> list[str]:
        """Return available header names."""
        return []


_FALCON_GETTER = _FalconHeaderGetter()


class _OtelFalconMiddleware:
    """Falcon middleware that extracts W3C traceparent/tracestate from HTTP headers.

    Inserted before ``_AuthMiddleware`` so that authentication runs inside
    the trace context.
    """

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Extract trace context from HTTP headers and attach to OTel context."""
        ctx = propagate.extract(req, getter=_FALCON_GETTER)  # type: ignore[arg-type]
        token = otel_context.attach(ctx)
        req.context.otel_context_token = token

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Detach the OTel context after the request."""
        token = getattr(req.context, "otel_context_token", None)
        if token is not None:
            otel_context.detach(token)


# ---------------------------------------------------------------------------
# Internal dispatch hook
# ---------------------------------------------------------------------------


@dataclass
class _OtelHookToken:
    """Internal token carrying span + timing for on_dispatch_end."""

    span: trace.Span | None
    otel_token: Token[Context] | None
    start_time: float
    method_name: str
    method_type: str
    service: str


class _OtelDispatchHook:
    """Implements ``_DispatchHook`` with OpenTelemetry spans and metrics."""

    __slots__ = (
        "_config",
        "_counter",
        "_histogram",
        "_meter",
        "_protocol_name",
        "_server_id",
        "_tracer",
    )

    def __init__(self, config: OtelConfig, protocol_name: str, server_id: str) -> None:
        self._config = config
        self._protocol_name = protocol_name
        self._server_id = server_id

        # Tracer
        tp = config.tracer_provider or get_tracer_provider()
        self._tracer: Tracer = tp.get_tracer("vgi_rpc", "0.1.0")

        # Meter + instruments
        mp: MeterProvider = config.meter_provider or get_meter_provider()
        self._meter: Meter = mp.get_meter("vgi_rpc", "0.1.0")
        self._counter: Counter = self._meter.create_counter(
            "rpc.server.requests",
            unit="{request}",
            description="Number of RPC requests handled",
        )
        self._histogram: Histogram = self._meter.create_histogram(
            "rpc.server.duration",
            unit="s",
            description="Duration of RPC requests",
        )

    def on_dispatch_start(
        self,
        info: RpcMethodInfo,
        auth: AuthContext,
        transport_metadata: Mapping[str, Any],
    ) -> HookToken:
        """Start a span and record the start time."""
        start_time = time.monotonic()
        span: trace.Span | None = None
        otel_token: Token[Context] | None = None

        if self._config.enable_tracing:
            # Check for pipe-transport trace context in contextvar
            trace_headers = _current_trace_headers.get()
            parent_ctx: Context | None = None
            if trace_headers:
                parent_ctx = propagate.extract(trace_headers)

            attrs: dict[str, str] = {
                "rpc.system": "vgi_rpc",
                "rpc.service": self._protocol_name,
                "rpc.method": info.name,
                "rpc.vgi_rpc.method_type": info.method_type.value,
                "rpc.vgi_rpc.server_id": self._server_id,
            }
            # Auth attributes
            if auth.principal:
                attrs["enduser.id"] = auth.principal
            if auth.domain:
                attrs["rpc.vgi_rpc.auth.domain"] = auth.domain
            attrs["rpc.vgi_rpc.auth.authenticated"] = str(auth.authenticated).lower()
            # Transport attributes (HTTP)
            remote_addr = transport_metadata.get("remote_addr")
            if remote_addr:
                attrs["net.peer.ip"] = str(remote_addr)
            user_agent = transport_metadata.get("user_agent")
            if user_agent:
                attrs["user_agent.original"] = str(user_agent)
            # Custom attributes
            attrs.update(self._config.custom_attributes)

            if parent_ctx is not None:
                span = self._tracer.start_span(
                    f"vgi_rpc/{info.name}",
                    kind=SpanKind.SERVER,
                    attributes=attrs,
                    context=parent_ctx,
                )
            else:
                span = self._tracer.start_span(
                    f"vgi_rpc/{info.name}",
                    kind=SpanKind.SERVER,
                    attributes=attrs,
                )
            otel_token = otel_context.attach(trace.set_span_in_context(span))

        return _OtelHookToken(
            span=span,
            otel_token=otel_token,
            start_time=start_time,
            method_name=info.name,
            method_type=info.method_type.value,
            service=self._protocol_name,
        )

    def on_dispatch_end(
        self,
        token: HookToken,
        info: RpcMethodInfo,
        error: BaseException | None,
        *,
        stats: CallStatistics | None = None,
    ) -> None:
        """End the span and record metrics."""
        if not isinstance(token, _OtelHookToken):
            return

        duration = time.monotonic() - token.start_time
        status = "error" if error is not None else "ok"

        # Finalize span
        if token.span is not None:
            if error is not None:
                token.span.set_status(StatusCode.ERROR, str(error))
                token.span.set_attribute("rpc.vgi_rpc.error_type", type(error).__name__)
                if self._config.record_exceptions:
                    token.span.record_exception(error)
            else:
                token.span.set_status(StatusCode.OK)
            if stats is not None:
                token.span.set_attribute("rpc.vgi_rpc.input_batches", stats.input_batches)
                token.span.set_attribute("rpc.vgi_rpc.output_batches", stats.output_batches)
                token.span.set_attribute("rpc.vgi_rpc.input_rows", stats.input_rows)
                token.span.set_attribute("rpc.vgi_rpc.output_rows", stats.output_rows)
                token.span.set_attribute("rpc.vgi_rpc.input_bytes", stats.input_bytes)
                token.span.set_attribute("rpc.vgi_rpc.output_bytes", stats.output_bytes)
            token.span.end()

        if token.otel_token is not None:
            otel_context.detach(token.otel_token)

        # Record metrics
        if self._config.enable_metrics:
            metric_attrs: dict[str, str] = {
                "rpc.system": "vgi_rpc",
                "rpc.service": token.service,
                "rpc.method": token.method_name,
                "rpc.vgi_rpc.method_type": token.method_type,
                "status": status,
            }
            self._counter.add(1, metric_attrs)
            self._histogram.record(duration, metric_attrs)
