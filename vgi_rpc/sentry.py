# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Sentry error reporting and optional performance monitoring for vgi-rpc.

Provides ``SentryConfig`` and ``instrument_server_sentry()`` for reporting
unhandled server exceptions to Sentry with rich RPC context (method name,
auth principal, server ID, etc.).

Requires ``pip install vgi-rpc[sentry]`` (sentry-sdk>=2.0).

Users must initialize Sentry separately via ``sentry_sdk.init()`` — this
module does **not** manage the DSN or SDK lifecycle.

Usage::

    import sentry_sdk
    from vgi_rpc.sentry import SentryConfig, instrument_server_sentry

    sentry_sdk.init(dsn="https://...")
    server = RpcServer(MyProtocol, MyImpl())
    instrument_server_sentry(server)  # uses default config
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import sentry_sdk
import sentry_sdk.tracing

from vgi_rpc.rpc._common import (
    AuthContext,
    CallStatistics,
    HookToken,
    _register_dispatch_hook,
)

if TYPE_CHECKING:
    from vgi_rpc.rpc._server import RpcServer
    from vgi_rpc.rpc._types import RpcMethodInfo

_logger = logging.getLogger("vgi_rpc.sentry")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SentryConfig:
    """Configuration for Sentry error reporting and optional performance monitoring.

    Attributes:
        enable_error_capture: Capture exceptions via ``sentry_sdk.capture_exception`` (default ``True``).
        enable_performance: Start a Sentry transaction per dispatch.  **Opt-in** to avoid
            duplicate tracing when used alongside OpenTelemetry and to conserve Sentry quota.
        record_request_context: Set Sentry scope context with method name, server ID,
            and authentication info (default ``True``).
        custom_tags: Extra tags applied to every Sentry event.
        ignored_exceptions: Exception types to skip when reporting (e.g. ``PermissionError``).
        op_name: Sentry transaction operation name (default ``"rpc.server"``).

    """

    enable_error_capture: bool = True
    enable_performance: bool = False
    record_request_context: bool = True
    custom_tags: Mapping[str, str] = field(default_factory=dict)
    ignored_exceptions: tuple[type[BaseException], ...] = ()
    op_name: str = "rpc.server"


def instrument_server_sentry(server: RpcServer, config: SentryConfig | None = None) -> RpcServer:
    """Attach Sentry error reporting to a server.

    Must be called before ``serve()`` — not thread-safe during dispatch.

    Args:
        server: The ``RpcServer`` to instrument.
        config: Optional configuration; uses defaults when ``None``.

    Returns:
        The same *server* instance (for chaining).

    """
    if config is None:
        config = SentryConfig()
    hook = _SentryDispatchHook(config, server.protocol_name, server.server_id)
    server._dispatch_hook = _register_dispatch_hook(server._dispatch_hook, hook)
    return server


# ---------------------------------------------------------------------------
# Internal dispatch hook
# ---------------------------------------------------------------------------


@dataclass
class _SentryHookToken:
    """Internal token carrying transaction reference for on_dispatch_end."""

    transaction: sentry_sdk.tracing.Transaction | sentry_sdk.tracing.NoOpSpan | None
    method_name: str
    service: str


class _SentryDispatchHook:
    """Implements ``_DispatchHook`` with Sentry error capture and optional transactions."""

    __slots__ = ("_config", "_protocol_name", "_server_id")

    def __init__(self, config: SentryConfig, protocol_name: str, server_id: str) -> None:
        self._config = config
        self._protocol_name = protocol_name
        self._server_id = server_id

    def on_dispatch_start(
        self,
        info: RpcMethodInfo,
        auth: AuthContext,
        transport_metadata: Mapping[str, Any],
    ) -> HookToken:
        """Set Sentry scope context and optionally start a transaction."""
        scope = sentry_sdk.get_current_scope()

        if self._config.record_request_context:
            scope.set_context(
                "rpc",
                {
                    "method": info.name,
                    "method_type": info.method_type.value,
                    "service": self._protocol_name,
                    "server_id": self._server_id,
                },
            )
            if auth.principal:
                scope.set_user({"username": auth.principal})
            if auth.domain:
                scope.set_tag("auth.domain", auth.domain)

        for key, value in self._config.custom_tags.items():
            scope.set_tag(key, value)

        transaction: sentry_sdk.tracing.Transaction | sentry_sdk.tracing.NoOpSpan | None = None
        if self._config.enable_performance:
            transaction = sentry_sdk.start_transaction(
                op=self._config.op_name,
                name=f"vgi_rpc/{info.name}",
            )

        return _SentryHookToken(
            transaction=transaction,
            method_name=info.name,
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
        """Capture exceptions and finalize any active transaction."""
        if not isinstance(token, _SentryHookToken):
            return

        if (
            error is not None
            and self._config.enable_error_capture
            and not isinstance(error, self._config.ignored_exceptions)
        ):
            sentry_sdk.capture_exception(error)

        if token.transaction is not None:
            if error is not None:
                token.transaction.set_status("internal_error")
            else:
                token.transaction.set_status("ok")
            token.transaction.finish()
