# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Sentry error reporting and optional performance monitoring for vgi-rpc.

Provides ``SentryConfig`` and ``instrument_server_sentry()`` for reporting
unhandled server exceptions to Sentry with rich RPC context (method name,
auth principal, server ID, etc.).

Requires ``pip install vgi-rpc[sentry]`` (sentry-sdk>=2.0).

Users must initialize Sentry separately via ``sentry_sdk.init()`` — this
module does **not** manage the DSN or SDK lifecycle.  Setting ``SENTRY_DSN``
alone is **not** enough: the SDK does not auto-initialise on import.  You
still have to call ``sentry_sdk.init()``; if you don't pass ``dsn=`` it
falls back to reading ``SENTRY_DSN`` from the environment.

Usage::

    import sentry_sdk
    from vgi_rpc.sentry import SentryConfig, instrument_server_sentry

    sentry_sdk.init(dsn="https://...")
    server = RpcServer(MyProtocol, MyImpl())
    instrument_server_sentry(server)  # uses default config

Automatic instrumentation
-------------------------

If ``sentry_sdk`` is already imported and ``sentry_sdk.is_initialized()``
returns ``True`` when ``RpcServer`` is constructed, the framework attaches
:func:`instrument_server_sentry` with default config automatically.  No
flag, no extra env var — if ``sentry_sdk.init()`` has run, vgi-rpc reports
RPC errors with full context.

To customise (``custom_tags``, ``claim_tags``, ``enable_performance``,
``ignored_exceptions``), call :func:`instrument_server_sentry` explicitly
or pass ``sentry_config=`` to ``make_wsgi_app`` / ``serve_http``.  The
explicit call always wins: it **replaces** any auto-attached default hook
in the dispatch chain regardless of call order.
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
    _CompositeDispatchHook,
    _DispatchHook,
    _format_claim_value,
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
        claim_tags: Maps claim keys to Sentry tag names, e.g.
            ``{"tenant_id": "auth.tenant_id"}``.

    """

    enable_error_capture: bool = True
    enable_performance: bool = False
    record_request_context: bool = True
    custom_tags: Mapping[str, str] = field(default_factory=dict)
    ignored_exceptions: tuple[type[BaseException], ...] = ()
    op_name: str = "rpc.server"
    claim_tags: Mapping[str, str] = field(default_factory=dict)


def instrument_server_sentry(server: RpcServer, config: SentryConfig | None = None) -> RpcServer:
    """Attach Sentry error reporting to a server, replacing any prior Sentry hook.

    Must be called before ``serve()`` — not thread-safe during dispatch.
    If a ``_SentryDispatchHook`` is already registered on *server* (e.g. an
    auto-attached default), it is removed and replaced with one configured
    from *config*.  This guarantees explicit calls always win regardless of
    whether they happen before or after auto-attach.

    Args:
        server: The ``RpcServer`` to instrument.
        config: Optional configuration; uses defaults when ``None``.

    Returns:
        The same *server* instance (for chaining).

    """
    if config is None:
        config = SentryConfig()
    server._dispatch_hook = _strip_sentry_hook(server._dispatch_hook)
    hook = _SentryDispatchHook(config, server.protocol_name, server.server_id)
    server._dispatch_hook = _register_dispatch_hook(server._dispatch_hook, hook)
    return server


def _has_sentry_hook(hook: _DispatchHook | None) -> bool:
    """Return True when *hook* (or any composite child) is a ``_SentryDispatchHook``."""
    if hook is None:
        return False
    if isinstance(hook, _SentryDispatchHook):
        return True
    if isinstance(hook, _CompositeDispatchHook):
        return any(isinstance(inner, _SentryDispatchHook) for inner in hook._hooks)
    return False


def _strip_sentry_hook(hook: _DispatchHook | None) -> _DispatchHook | None:
    """Return *hook* with any ``_SentryDispatchHook`` removed from the chain."""
    if hook is None or isinstance(hook, _SentryDispatchHook):
        return None
    if isinstance(hook, _CompositeDispatchHook):
        kept = [inner for inner in hook._hooks if not isinstance(inner, _SentryDispatchHook)]
        if len(kept) == len(hook._hooks):
            return hook
        if not kept:
            return None
        if len(kept) == 1:
            return kept[0]
        return _CompositeDispatchHook(kept)
    return hook


def _maybe_auto_instrument(server: RpcServer) -> bool:
    """Attach default Sentry instrumentation when ``sentry_sdk`` has been initialised.

    Called by ``RpcServer.__init__`` whenever ``sentry_sdk`` is already
    importable in the current process.  No env-var gate: the user's call to
    ``sentry_sdk.init()`` is the signal of intent.  (``SENTRY_DSN`` alone
    is not enough — the SDK does not auto-init on import; ``init()`` reads
    the env var only if no explicit ``dsn=`` is passed.)  Idempotent — if
    the server already carries a Sentry hook (e.g. via explicit
    ``instrument_server_sentry`` or ``sentry_config=`` on
    ``make_wsgi_app``), this is a no-op.

    Args:
        server: The ``RpcServer`` to potentially instrument.

    Returns:
        ``True`` when instrumentation was attached on this call, ``False``
        otherwise (SDK not initialised or already instrumented).

    """
    if not sentry_sdk.is_initialized():
        return False
    if _has_sentry_hook(server._dispatch_hook):
        return False
    hook = _SentryDispatchHook(SentryConfig(), server.protocol_name, server.server_id)
    server._dispatch_hook = _register_dispatch_hook(server._dispatch_hook, hook)
    _logger.debug("Auto-attached Sentry instrumentation to server %s", server.server_id)
    return True


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
            scope.set_tag("auth.authenticated", str(auth.authenticated).lower())
            for claim_key, tag_name in self._config.claim_tags.items():
                formatted = _format_claim_value(auth.claims.get(claim_key))
                if formatted is not None:
                    scope.set_tag(tag_name, str(formatted))

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
