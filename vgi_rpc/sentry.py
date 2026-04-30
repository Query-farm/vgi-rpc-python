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

Trace Explorer recipe
---------------------

Every dispatch attaches ``rpc.system``, ``rpc.service``, ``rpc.method``,
and ``rpc.method_type`` as span attributes on the WSGI-created transaction's
root span — searchable in Sentry's Trace Explorer / Insights.  Streams
also carry ``rpc.stream_id`` (one uuid shared across all HTTP turns of one
logical stream call).

Opt in to per-call argument recording with ``SentryConfig(record_params=True)``
to expose kwargs as ``rpc.param.<k>`` span attributes.  After enabling it,
queries like the following work in Trace Explorer::

    span.op:rpc.server rpc.method:executeScan rpc.param.table:orders
    -> chart p99(span.duration) GROUP BY rpc.param.predicate

This is the same pattern Sentry's own ``sqlalchemy`` integration uses for
SQL spans (``db.system``, ``db.name``, etc.).  Tags (low-cardinality, used
for Issues-side filtering of error events) and span data (high-cardinality,
indexed for Trace Explorer) are independent surfaces; the operator-curated
``tag_params`` whitelist controls which params are duplicated as scope tags.

Caveats:

* Subsequent ``/exchange`` HTTP turns of a stream do **not** carry kwargs
  — params live only on the ``/init`` transaction.  Filter Trace Explorer
  by ``rpc.stream_id`` to find sibling turns of one logical call.
* Sentry's default scrubber matches kwarg **key names** only (e.g.
  ``password``, ``token``).  Free-text values such as predicates pass
  through; supply ``param_redactor`` or use Sentry Advanced Data Scrubbing
  if your kwargs may contain PII.
* Long string values are truncated at ``max_param_value_bytes`` (default
  1024 — Relay's practical per-attribute cap).
* Span attributes accept only primitives (``str``/``bool``/``int``/``float``)
  and homogeneous lists of one of those types; other values are silently
  dropped to avoid Sentry ingestion errors.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import sentry_sdk
import sentry_sdk.tracing

from vgi_rpc.rpc._common import (
    AuthContext,
    CallStatistics,
    HookToken,
    _CompositeDispatchHook,
    _current_stream_id,
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


_DEFAULT_REDACT_KEY_RE = re.compile(r"password|token|secret|key|authorization", re.IGNORECASE)
"""Default redactor matches kwarg keys whose names suggest credentials."""

_PARAM_PRIMITIVE = (str, bool, int, float)
"""Primitive types accepted as span-attribute values by Sentry's EAP indexer."""

_TAG_VALUE_MAX_LEN = 200
"""Sentry tag value cap (chars).  Longer values are clipped."""


def short_hash(value: bytes | str | None, *, length: int = 12) -> str | None:
    """Return a stable hex prefix of ``sha256(value)`` suitable for a Sentry tag.

    For tagging high-cardinality opaque IDs (``attach_id``, ``transaction_id``,
    UUIDs, JWT subs) without exhausting Sentry's tag-value distribution UI.
    Same input → same prefix; collisions are negligible at ``length>=8`` for
    any realistic per-process volume.

    ``bytes`` are normalised to their hex string before hashing so that
    ``short_hash(id_bytes)`` and ``short_hash(id_bytes.hex())`` produce the
    same tag value — call sites that already hold a hex string don't need
    to round-trip through bytes.

    Returns ``None`` when *value* is ``None``, so callers can pass through
    optional fields without an extra branch.
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.hex()
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


_WELL_KNOWN_ID_FIELDS: Mapping[str, str] = {
    "attach_id": "vgi.attach_id",
    "transaction_id": "vgi.transaction_id",
}
"""VGI base-protocol identifiers that get hashed and tagged automatically.

These are the only VGI-specific names baked into vgi-rpc.  Justified because
vgi-rpc was built for VGI; consumers whose RPC methods don't carry these
names pay nothing (extraction returns ``None``, no tag is set).
"""


def _extract_well_known(kwargs: Mapping[str, Any], name: str) -> bytes | str | None:
    """Pull *name* out of *kwargs* — top-level, nested attr, or InitRequest.bind_call.

    Returns the first matching ``bytes``/``str`` value, or ``None``.  Walks
    one level deep: top-level kwarg → ``getattr(kwarg, name)`` →
    ``getattr(kwarg.bind_call, name)``.  Not a general tree walk; only the
    shapes the VGI protocol actually uses (direct kwargs, request
    dataclasses, ``InitRequest`` wrapping ``BindRequest``).
    """
    if name in kwargs:
        v = kwargs[name]
        if isinstance(v, (bytes, str)):
            return v
    for v in kwargs.values():
        attr = getattr(v, name, None)
        if isinstance(attr, (bytes, str)):
            return attr
        bind_call = getattr(v, "bind_call", None)
        if bind_call is not None:
            nested = getattr(bind_call, name, None)
            if isinstance(nested, (bytes, str)):
                return nested
    return None


def noop_redactor(kwargs: Mapping[str, Any]) -> Mapping[str, Any]:
    """Pass-through redactor — emit kwargs verbatim.  Use only when you trust them."""
    return kwargs


def _default_redactor(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """Drop kwargs whose key name matches the default credential pattern.

    Key-based, not value-based: ``predicate="email='alice@x.com'"`` is NOT
    matched.  Operators with PII in free-text params must supply a custom
    ``param_redactor`` or use Sentry's Advanced Data Scrubbing rules.
    """
    return {k: v for k, v in kwargs.items() if not _DEFAULT_REDACT_KEY_RE.search(k)}


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
        user_claim_map: Maps Sentry user-object fields to JWT claim names, used
            to populate ``user.username`` / ``user.email`` / ``user.name`` from
            the decoded JWT.  Defaults to standard OIDC claims
            (``preferred_username`` → ``username``, ``email`` → ``email``,
            ``name`` → ``name``).  ``user.id`` is always set from
            ``auth.principal`` (typically the ``sub`` claim) and is not
            configurable here.  Override per-key for non-standard IdPs (e.g.
            Auth0 namespaced claims).  Values absent from the JWT are skipped.
        set_transaction_name: Override Sentry's WSGI-derived transaction name with
            ``rpc {method}``.  Default ``True``.  Disable to keep the route-template
            grouping (rarely useful — the default ``/{method}`` placeholder loses
            the actual method name).
        record_params: Attach RPC kwargs as ``rpc.param.<k>`` span attributes for
            Trace Explorer / Insights.  Default ``False``: kwargs may contain user
            data, and Sentry's default scrubber matches key names only — free-text
            values pass through.  Operators must opt in explicitly.
        tag_params: Short whitelist of param names to *also* duplicate as scope
            tags so they are filterable in Issues view (errors).  Keep this list
            for low-cardinality identifiers (table names, format enums) — never
            predicates, ids, or row counts.  Values are stringified and clipped
            to 200 chars.
        param_redactor: Optional sanitizer applied to kwargs before recording.
            Default redacts keys matching ``password|token|secret|key|authorization``
            (case-insensitive).  Pass an explicit callable to replace the default,
            or :func:`noop_redactor` to disable filtering.
        max_param_value_bytes: Truncate string param values longer than this
            (Relay's practical per-attribute cap).  Default 1024.

    """

    enable_error_capture: bool = True
    enable_performance: bool = False
    record_request_context: bool = True
    custom_tags: Mapping[str, str] = field(default_factory=dict)
    ignored_exceptions: tuple[type[BaseException], ...] = ()
    op_name: str = "rpc.server"
    claim_tags: Mapping[str, str] = field(default_factory=dict)
    user_claim_map: Mapping[str, str] = field(
        default_factory=lambda: {
            "username": "preferred_username",
            "email": "email",
            "name": "name",
        }
    )
    set_transaction_name: bool = True
    record_params: bool = False
    tag_params: Sequence[str] = ()
    param_redactor: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None
    max_param_value_bytes: int = 1024


def _safe_params(
    kwargs: Mapping[str, Any], config: SentryConfig
) -> Iterator[tuple[str, str | bool | int | float | list[Any]]]:
    """Yield ``(key, value)`` pairs safe to attach as span attributes.

    Applies *config*'s redactor, drops values that are not primitives or
    homogeneous lists of primitives (Sentry's EAP indexer rejects them),
    and truncates strings exceeding ``config.max_param_value_bytes``.
    """
    redactor = config.param_redactor or _default_redactor
    for k, v in redactor(kwargs).items():
        if isinstance(v, str):
            encoded = v.encode("utf-8", errors="replace")
            if len(encoded) > config.max_param_value_bytes:
                _logger.debug("Truncating rpc.param.%s (%d > %d bytes)", k, len(encoded), config.max_param_value_bytes)
                yield k, encoded[: config.max_param_value_bytes].decode("utf-8", errors="replace")
            else:
                yield k, v
        elif isinstance(v, _PARAM_PRIMITIVE):
            yield k, v
        elif isinstance(v, (list, tuple)) and _is_homogeneous_primitive_seq(v):
            yield k, list(v)
        # else: silently drop — EAP rejects non-primitive types (dicts, bytes,
        # dataclasses, etc.); attaching them would surface as ingestion errors.


def _is_homogeneous_primitive_seq(seq: list[Any] | tuple[Any, ...]) -> bool:
    """Return True when *seq* is non-empty and every element shares one EAP-safe type.

    Sentry's EAP indexer accepts ``list[str] | list[bool] | list[int] | list[float]``
    only — heterogeneous element types are rejected.  Empty lists are also rejected
    here because they carry no type information for the indexer.
    """
    if not seq:
        return False
    first = seq[0]
    if isinstance(first, bool):
        target: type | tuple[type, ...] = bool
    elif isinstance(first, int):
        target = int
    elif isinstance(first, float):
        target = float
    elif isinstance(first, str):
        target = str
    else:
        return False
    return all(type(x) is target for x in seq)


def _stringify_tag(value: str | bool | int | float) -> str:
    """Render a primitive as a Sentry tag value, clipped to 200 chars."""
    s = str(value)
    if len(s) > _TAG_VALUE_MAX_LEN:
        return s[:_TAG_VALUE_MAX_LEN]
    return s


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
        kwargs: Mapping[str, Any],
    ) -> HookToken:
        """Set Sentry scope/span context and optionally start a transaction.

        Always sets:
            - Transaction name ``rpc {method}`` (when ``set_transaction_name``)
            - Span attributes ``rpc.system``, ``rpc.service``, ``rpc.method``,
              ``rpc.method_type`` on the active transaction's root span
            - ``rpc.stream_id`` span attribute when running inside a stream call
            - ``rpc.method`` and ``rpc.method_type`` scope tags for Issues filtering

        Opt-in:
            - ``rpc.param.<k>`` span attributes when ``record_params`` is set
            - ``rpc.param.<k>`` scope tags for the keys listed in ``tag_params``
        """
        scope = sentry_sdk.get_current_scope()

        if self._config.set_transaction_name:
            scope.set_transaction_name(f"rpc {info.name}", source="custom")

        # Span attributes — the searchable surface in Trace Explorer / Insights.
        # ``get_current_span()`` returns ``None`` when tracing is sampled out
        # (e.g. ``traces_sample_rate=0``); skip the writes silently in that case.
        span = sentry_sdk.get_current_span()
        if span is not None:
            span.set_data("rpc.system", "vgi_rpc")
            span.set_data("rpc.service", self._protocol_name)
            span.set_data("rpc.method", info.name)
            span.set_data("rpc.method_type", info.method_type.value)
            stream_id = _current_stream_id.get()
            if stream_id:
                # Span data only — never a scope tag.  uuid4 per stream means
                # unbounded distinct values; tagging would pollute Sentry's
                # tag distribution UI.
                span.set_data("rpc.stream_id", stream_id)
            if self._config.record_params:
                for k, v in _safe_params(kwargs, self._config):
                    span.set_data(f"rpc.param.{k}", v)

        # Scope tags — Issues-side filtering of error events.  Method name and
        # type are bounded; the operator-curated whitelist handles params.
        scope.set_tag("rpc.method", info.name)
        scope.set_tag("rpc.method_type", info.method_type.value)
        for k in self._config.tag_params:
            if k in kwargs:
                v = kwargs[k]
                if isinstance(v, _PARAM_PRIMITIVE):
                    scope.set_tag(f"rpc.param.{k}", _stringify_tag(v))

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
            user: dict[str, Any] = {}
            if auth.principal:
                # auth.principal is typically the JWT ``sub`` claim — an
                # opaque, stable IdP-issued identifier.  Sentry's ``user.id``
                # is the right field for that; ``user.username`` is meant
                # for human-readable handles.
                user["id"] = auth.principal
            for user_field, claim_key in self._config.user_claim_map.items():
                value = auth.claims.get(claim_key)
                if isinstance(value, str) and value:
                    user[user_field] = value
            if user:
                scope.set_user(user)
            if auth.domain:
                scope.set_tag("auth.domain", auth.domain)
            scope.set_tag("auth.authenticated", str(auth.authenticated).lower())
            for claim_key, tag_name in self._config.claim_tags.items():
                formatted = _format_claim_value(auth.claims.get(claim_key))
                if formatted is not None:
                    scope.set_tag(tag_name, str(formatted))

        for key, value in self._config.custom_tags.items():
            scope.set_tag(key, value)

        # Auto-tag VGI well-known protocol IDs (attach_id, transaction_id) on
        # *every* dispatch so subsequent operations on the same client session
        # share a queryable tag value, regardless of which RPC method carried
        # the ID.  Hashed to keep tag-value cardinality bounded.
        for field_name, tag_name in _WELL_KNOWN_ID_FIELDS.items():
            raw = _extract_well_known(kwargs, field_name)
            hashed = short_hash(raw)
            if hashed is not None:
                scope.set_tag(tag_name, hashed)
                if span is not None:
                    span.set_data(tag_name, hashed)

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
