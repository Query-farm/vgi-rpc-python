# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Constants, errors, authentication, and call context for the RPC framework."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Mapping, MutableMapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, Final, Protocol, cast

import pyarrow as pa

from vgi_rpc.log import Level, Message

if TYPE_CHECKING:
    from vgi_rpc.rpc._types import RpcMethodInfo

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_EMPTY_SCHEMA = pa.schema([])
_logger = logging.getLogger("vgi_rpc.rpc")
_access_logger = logging.getLogger("vgi_rpc.access")

ClientLog = Callable[[Message], None]
"""Callback type for emitting client-directed log messages from RPC method implementations."""


# ---------------------------------------------------------------------------
# TransportKind
# ---------------------------------------------------------------------------


class TransportKind(StrEnum):
    """Coarse identifier of the transport binding an :class:`RpcServer`.

    Workers (RPC implementations) read this via :attr:`RpcServer.transport_kind`
    or the ``on_serve_start`` lifecycle hook to tailor startup behaviour
    (skip HTTP-only caching, enable transport-specific metrics, etc.).
    Members are :class:`StrEnum` so the value is wire/log-friendly.

    Members:
        PIPE: Stdio / ``PipeTransport`` / ``ShmPipeTransport``.  Subprocess
            workers also report ``PIPE`` because they speak Arrow IPC over
            the parent's stdin/stdout.
        HTTP: WSGI / ``serve_http`` / ``make_wsgi_app``.
        UNIX: ``UnixTransport`` (AF_UNIX socket).
    """

    PIPE = "pipe"
    HTTP = "http"
    UNIX = "unix"


class ServeStartHook(Protocol):
    """Optional lifecycle hook on the RPC implementation, fired once at startup.

    If an implementation defines an ``on_serve_start(self, kind)`` method,
    the framework calls it once per process before the first request is
    dispatched.  The hook is duck-typed — defining this Protocol is **not**
    required; it exists purely for users who want to type-hint their impl.

    For HTTP, the hook fires on the first request handled in the current
    process (lazy / fork-safe).  For pipe / unix transports, it fires
    when ``RpcServer.serve(transport)`` begins.

    A hook that raises propagates out of the serve path.  The exception
    is logged via ``logging.getLogger("vgi_rpc.rpc").exception`` first so
    the trace is visible even when stderr is captured.
    """

    def on_serve_start(self, kind: TransportKind) -> None:
        """Run worker-side setup that depends on the transport binding."""
        ...


# ---------------------------------------------------------------------------
# Authentication & Call Context
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AuthContext:
    """Authentication context for the current request.

    Populated by transport-specific ``authenticate`` callbacks (e.g. JWT
    validation on HTTP) and injected into method implementations via
    :class:`CallContext`.

    Attributes:
        domain: Authentication scheme that produced this context, or
            ``None`` for unauthenticated requests.
        authenticated: Whether the caller was successfully authenticated.
        principal: Identity of the caller (e.g. username, service account).
        claims: Arbitrary claims from the authentication token.

    """

    domain: str | None
    authenticated: bool
    principal: str | None = None
    claims: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def anonymous(cls) -> AuthContext:
        """Unauthenticated context (default for pipe transport)."""
        return _ANONYMOUS

    def require_authenticated(self) -> None:
        """Raise if not authenticated.

        Raises:
            PermissionError: If ``authenticated`` is ``False``.

        """
        if not self.authenticated:
            raise PermissionError("Authentication required")


_ANONYMOUS: Final[AuthContext] = AuthContext(domain=None, authenticated=False)
_EMPTY_TRANSPORT_METADATA: Final[Mapping[str, Any]] = MappingProxyType({})
_EMPTY_COOKIES: Final[Mapping[str, str]] = MappingProxyType({})


def _format_claim_value(value: object) -> str | int | float | bool | None:
    """Format a claim value for use as an OTel attribute or Sentry tag.

    Scalars (str, int, float, bool) pass through natively.
    Returns ``None`` for unsupported types (dict, list, None).
    """
    if isinstance(value, (str, int, float, bool)):
        return value
    return None


class _ContextLoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """LoggerAdapter that preserves framework-bound extra fields.

    User-supplied ``extra`` in individual log calls is merged, but
    framework fields take precedence on key conflicts.  Fields always
    present: ``server_id``, ``method``.  Conditionally present when
    available: ``principal``, ``auth_domain``, ``remote_addr``.
    """

    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, MutableMapping[str, Any]]:
        """Merge user extra with framework extra, framework wins on conflict."""
        user_extra = kwargs.get("extra", {})
        kwargs["extra"] = {**user_extra, **(self.extra or {})}
        return msg, kwargs


class CallContext:
    """Request-scoped context injected into methods that declare a ``ctx`` parameter.

    Provides authentication, logging, and transport metadata in a single
    injection point.
    """

    __slots__ = (
        "_logger",
        "_method_name",
        "_protocol_name",
        "_request_id",
        "_server_id",
        "auth",
        "emit_client_log",
        "implementation",
        "kind",
        "transport_metadata",
    )

    def __init__(
        self,
        auth: AuthContext,
        emit_client_log: ClientLog,
        transport_metadata: Mapping[str, Any] | None = None,
        *,
        server_id: str = "",
        method_name: str = "",
        protocol_name: str = "",
        kind: TransportKind | None = None,
        implementation: Any = None,
    ) -> None:
        """Initialize with auth context, client-log callback, and optional server context fields.

        Args:
            auth: Authentication context for the current request.
            emit_client_log: Callback for emitting client-directed log messages.
            transport_metadata: Transport-specific metadata (e.g. ``remote_addr``).
            server_id: Stable identifier for the server instance.
            method_name: Name of the RPC method being dispatched.
            protocol_name: Name of the Protocol class being served.
            kind: Active transport binding for this dispatch.
            implementation: The protocol implementation object the
                RpcServer was constructed with. Surfaced on the context so
                producer-mode stream states (and other framework-driven
                callbacks that lack direct access to the implementation
                instance) can dispatch helper calls back through the
                public protocol surface — including any meta-worker
                dispatching the top-level RpcServer is fronting.

        """
        self.auth = auth
        self.emit_client_log = emit_client_log
        self.transport_metadata: Mapping[str, Any] = transport_metadata or {}
        self.kind: TransportKind | None = kind
        self.implementation: Any = implementation
        self._server_id = server_id
        self._method_name = method_name
        self._protocol_name = protocol_name
        self._request_id = _current_request_id.get()
        self._logger: _ContextLoggerAdapter | None = None

    @property
    def request_id(self) -> str:
        """Per-request correlation ID (empty string if not set)."""
        return self._request_id

    @property
    def logger(self) -> logging.LoggerAdapter[logging.Logger]:
        """Server-side logger with request context pre-bound.

        Returns:
            A ``LoggerAdapter`` with logger name
            ``vgi_rpc.service.<ProtocolName>``.  Always includes
            ``server_id`` and ``method``; conditionally includes
            ``request_id``, ``principal``, ``auth_domain``, and
            ``remote_addr`` when available.

        """
        if self._logger is None:
            base = logging.getLogger(f"vgi_rpc.service.{self._protocol_name}")
            extra: dict[str, object] = {
                "server_id": self._server_id,
                "method": self._method_name,
            }
            if self._request_id:
                extra["request_id"] = self._request_id
            if self.auth.principal:
                extra["principal"] = self.auth.principal
            if self.auth.domain:
                extra["auth_domain"] = self.auth.domain
            remote = self.transport_metadata.get("remote_addr")
            if remote:
                extra["remote_addr"] = remote
            self._logger = _ContextLoggerAdapter(base, extra)
        return self._logger

    def client_log(self, level: Level, message: str, **extra: str) -> None:
        """Emit a client-directed log message (convenience wrapper)."""
        self.emit_client_log(Message(level, message, **extra))

    @property
    def cookies(self) -> Mapping[str, str]:
        """Cookies from the incoming HTTP request.

        Returns a read-only mapping of cookie name to value.  Empty for
        non-HTTP transports (pipe, subprocess, shared memory) because
        those transports have no concept of HTTP cookies.
        """
        cookies = self.transport_metadata.get("cookies")
        if cookies is None:
            return _EMPTY_COOKIES
        return cast("Mapping[str, str]", cookies)

    def set_cookie(
        self,
        name: str,
        value: str,
        *,
        expires: datetime | None = None,
        max_age: int | None = None,
        domain: str | None = None,
        path: str | None = None,
        secure: bool | None = None,
        http_only: bool = True,
        same_site: str | None = None,
        partitioned: bool = False,
    ) -> None:
        """Queue a ``Set-Cookie`` header for the HTTP response.

        Only valid inside a unary RPC method served over HTTP.  Streaming
        responses are chunked with state carried in Arrow metadata and
        cannot cleanly attach a single ``Set-Cookie``.

        Args:
            name: Cookie name.
            value: Cookie value.
            expires: Absolute expiry time.  Mutually useful with ``max_age``.
            max_age: Seconds until expiry.  ``0`` deletes the cookie.
            domain: ``Domain`` attribute.
            path: ``Path`` attribute.
            secure: ``Secure`` attribute.  ``None`` lets the transport decide.
            http_only: ``HttpOnly`` attribute.  Defaults to ``True``.
            same_site: ``SameSite`` attribute (``"Strict"``, ``"Lax"``, ``"None"``).
            partitioned: ``Partitioned`` attribute (CHIPS).

        Raises:
            RuntimeError: If the call is not a unary HTTP request.

        """
        sink = _current_response_cookies.get()
        if sink is None:
            raise RuntimeError(
                "set_cookie() is only supported inside unary RPC methods served over HTTP",
            )
        sink.append(
            CookieSpec(
                name=name,
                value=value,
                expires=expires,
                max_age=max_age,
                domain=domain,
                path=path,
                secure=secure,
                http_only=http_only,
                same_site=same_site,
                partitioned=partitioned,
            )
        )

    def delete_cookie(
        self,
        name: str,
        *,
        path: str | None = None,
        domain: str | None = None,
    ) -> None:
        """Queue a cookie deletion on the HTTP response.

        Args:
            name: Cookie name to unset.
            path: ``Path`` attribute of the cookie to match.
            domain: ``Domain`` attribute of the cookie to match.

        Raises:
            RuntimeError: If the call is not a unary HTTP request.

        """
        sink = _current_response_cookies.get()
        if sink is None:
            raise RuntimeError(
                "delete_cookie() is only supported inside unary RPC methods served over HTTP",
            )
        sink.append(CookieSpec(name=name, delete=True, path=path, domain=domain))

    # --- Sticky sessions (HTTP-only) -----------------------------------

    @property
    def session(self) -> object | None:
        """The live session state object, or ``None`` if no session is bound to this request.

        Sticky sessions are HTTP-only. On other transports this is always
        ``None`` because the middleware that populates the contextvar is
        only installed on the HTTP WSGI app.
        """
        sc = _current_session_context.get()
        return sc.state if sc is not None else None

    @property
    def session_id(self) -> str | None:
        """The opaque 12-char session ID, or ``None`` if no session is bound to this request."""
        sc = _current_session_context.get()
        return sc.session_id if sc is not None else None

    def open_session(self, state: object, ttl: float | None = None) -> None:
        """Register a sticky session holding *state* for subsequent requests.

        The framework mints a signed ``VGI-Session`` token and attaches it
        to the response header; the client (inside a
        ``with_session_token()`` block) echoes the header on subsequent
        requests, and the framework restores *state* as ``ctx.session``.

        The ``state`` object stays in the per-worker registry until the
        method explicitly calls :meth:`close_session`, the session's TTL
        elapses, or the server drains. If *state* has a ``close()`` method
        it is invoked on eviction so cursors / file handles / model handles
        get released promptly. The framework serializes concurrent calls
        on the same session via a per-session RLock; different sessions
        run in parallel.

        Args:
            state: The Python object to keep alive for the session.
            ttl: Override the server's default session TTL in seconds.
                ``None`` (the default) uses ``sticky_default_ttl`` from
                ``make_wsgi_app``.

        Raises:
            RuntimeError: If sticky sessions are not available on the
                current transport (anything other than HTTP, or HTTP
                without ``enable_sticky=True``); if the client did not
                opt in with ``VGI-Session-Accept: true`` (the client is
                outside a ``with_session_token()`` block and would lose
                the minted token); or if a session is already bound to
                this request.

        """
        sink = _current_sticky_sink.get()
        if sink is None:
            raise RuntimeError("sticky sessions not available on this transport")
        if not sink.accept_opens:
            raise RuntimeError(
                "client did not opt in to sticky sessions "
                "(missing VGI-Session-Accept: true header — open the call inside "
                "an HttpConnection.with_session_token() block)"
            )
        if _current_session_context.get() is not None:
            raise RuntimeError("a sticky session is already active for this request")
        sink.open(state, ttl)
        # Override middleware-set action with "open" for the access log.
        # If close_session was already called this request (unusual but possible
        # via reopens), it'll be overridden again to "close" below.
        _current_sticky_action.set("open")

    def close_session(self) -> None:
        """Invalidate the sticky session bound to this request.

        Invokes ``state.close()`` if defined, removes the registry entry,
        and asks the response middleware to emit ``VGI-Session-Close:
        true`` so the client drops its stored token. Idempotent — calling
        twice within the same request is a no-op on the second call.

        Raises:
            RuntimeError: If sticky sessions are not available on the
                current transport.

        """
        sink = _current_sticky_sink.get()
        if sink is None:
            raise RuntimeError("sticky sessions not available on this transport")
        sink.close()
        _current_sticky_action.set("close")


@dataclass(frozen=True)
class CookieSpec:
    """Queued cookie mutation to apply to an HTTP response.

    Created by :meth:`CallContext.set_cookie` / :meth:`CallContext.delete_cookie`
    and consumed by the HTTP transport after method dispatch completes.
    Internal — callers interact only through the ``CallContext`` helpers.
    """

    name: str
    value: str = ""
    delete: bool = False
    expires: datetime | None = None
    max_age: int | None = None
    domain: str | None = None
    path: str | None = None
    secure: bool | None = None
    http_only: bool = True
    same_site: str | None = None
    partitioned: bool = False


@dataclass(frozen=True)
class _TransportContext:
    """Internal: auth + transport metadata set by the transport layer."""

    auth: AuthContext
    transport_metadata: Mapping[str, Any] = field(default_factory=dict)


_current_transport: ContextVar[_TransportContext | None] = ContextVar("vgi_rpc_transport", default=None)

# Per-request sink for outgoing HTTP response cookies.  Installed by the
# HTTP unary resource before dispatch and drained after.  ``None`` on
# non-unary or non-HTTP code paths — ``CallContext.set_cookie`` raises
# in that case.
_current_response_cookies: ContextVar[list[CookieSpec] | None] = ContextVar("vgi_rpc_response_cookies", default=None)


# ---------------------------------------------------------------------------
# Sticky session contextvars (HTTP-only)
# ---------------------------------------------------------------------------


class _StickySinkProtocol(Protocol):
    """Internal interface bridging ``CallContext.open_session`` to the HTTP middleware.

    Installed by ``_StickyMiddleware.process_request`` when sticky sessions
    are enabled on the WSGI app. The middleware's concrete implementation
    owns the per-worker registry, the AEAD token key, and the per-response
    mint/close state. ``CallContext`` only sees this Protocol so the core
    has no compile-time dependency on the HTTP module.
    """

    accept_opens: bool
    """Whether the client opted in via the ``VGI-Session-Accept`` header."""

    def open(self, state: object, ttl: float | None) -> str:
        """Register a session, return the sealed token. Caller stores nothing."""
        ...

    def close(self) -> None:
        """Close the session currently bound to this request, if any."""
        ...


@dataclass(frozen=True)
class _SessionContext:
    """Internal: per-request handle on the live sticky session.

    Set by ``_StickyMiddleware`` when a valid ``VGI-Session`` token is
    presented; read by ``CallContext.session`` / ``CallContext.session_id``.
    """

    state: object
    session_id: str


_current_sticky_sink: ContextVar[_StickySinkProtocol | None] = ContextVar(
    "vgi_rpc_sticky_sink",
    default=None,
)
_current_session_context: ContextVar[_SessionContext | None] = ContextVar(
    "vgi_rpc_session_context",
    default=None,
)

# Sticky-session lifecycle action observed during dispatch — surfaced on
# the access-log record. Set by the sticky middleware (``none`` /
# ``resume``) and overridden by ``CallContext.open_session`` / ``close_session``
# (``open`` / ``close``). Read by ``_emit_access_log``; ``None`` (default)
# means "no sticky machinery touched this request" and the field is
# omitted from the access-log line. Middleware-short-circuit cases
# (lost / expired) currently do not produce access-log records — they
# short-circuit before dispatch and are documented as a gap in
# ``docs/access-log-spec.md``.
_current_sticky_action: ContextVar[str | None] = ContextVar(
    "vgi_rpc_sticky_action",
    default=None,
)

# Session ID separately tracked so it survives ``close_session()`` clearing
# the session-context contextvar. The access log reads this so a "close"
# record still carries the id of the just-closed session.
_current_session_id: ContextVar[str | None] = ContextVar(
    "vgi_rpc_session_id",
    default=None,
)


def _get_auth_and_metadata() -> tuple[AuthContext, Mapping[str, Any]]:
    """Read auth and transport metadata from the current transport contextvar."""
    tc = _current_transport.get()
    if tc is not None:
        return tc.auth, tc.transport_metadata
    return _ANONYMOUS, _EMPTY_TRANSPORT_METADATA


def current_auth() -> AuthContext:
    """Return the :class:`AuthContext` for the request currently being dispatched.

    Reads the transport contextvar set by the transport layer before method
    dispatch. Lets code that is not a direct RPC method handler (and therefore
    has no ``CallContext`` parameter) still reach the caller's identity —
    e.g. catalog opaque-data sealing in VGI. Returns the anonymous context
    when called outside a dispatch (pipe transport, or no transport active).
    """
    return _get_auth_and_metadata()[0]


# ---------------------------------------------------------------------------
# Per-request correlation ID
# ---------------------------------------------------------------------------


def _generate_request_id() -> str:
    """Generate a 16-char hex request ID for correlation."""
    return uuid.uuid4().hex[:16]


_current_request_id: ContextVar[str] = ContextVar("vgi_rpc_request_id", default="")

# Request batch custom_metadata — set by _read_request(), read by serve_one()
# for dynamic SHM segment attachment without changing _read_request()'s return type.
_current_request_metadata: ContextVar[pa.KeyValueMetadata | None] = ContextVar("vgi_rpc_request_metadata", default=None)

# Raw request batch bytes — set by _read_request() before deserialization,
# included in the access log as base64 for full call-parameter capture.
_current_request_batch: ContextVar[bytes | None] = ContextVar("vgi_rpc_request_batch", default=None)

# Stream correlation ID — set by _serve_stream() / HTTP init path,
# carried in the HTTP state token for exchange/produce correlation.
_current_stream_id: ContextVar[str] = ContextVar("vgi_rpc_stream_id", default="")


# ---------------------------------------------------------------------------
# MethodType enum
# ---------------------------------------------------------------------------


class MethodType(Enum):
    """Classification of RPC method patterns."""

    UNARY = "unary"
    STREAM = "stream"


# ---------------------------------------------------------------------------
# RpcError
# ---------------------------------------------------------------------------


class RpcError(Exception):
    """Raised on the client side when the server reports an error."""

    def __init__(self, error_type: str, error_message: str, remote_traceback: str, *, request_id: str = "") -> None:
        """Initialize with error details from the remote side."""
        self.error_type = error_type
        self.error_message = error_message
        self.remote_traceback = remote_traceback
        self.request_id = request_id
        super().__init__(f"{error_type}: {error_message}")


class VersionError(Exception):
    """Raised when a request has a missing or incompatible protocol version."""


class MethodNotImplementedError(AttributeError):
    """Raised server-side when no handler is registered for the requested RPC method.

    Subclass of ``AttributeError`` so existing ``except AttributeError`` callers
    keep working. Carries a stable ``error_kind`` class attribute that the
    wire serializer surfaces as a top-level ``vgi_rpc.error_kind`` metadata
    key on the EXCEPTION-level error batch, so clients can pattern-match on
    the kind rather than substring-searching the message text.

    Used by callers that want to detect "old server doesn't know this method"
    cleanly (e.g. capability detection + fallback to a legacy RPC method).
    """

    error_kind: ClassVar[str] = "method_not_implemented"


class SessionLostError(Exception):
    """Raised server-side when a sticky session token cannot be honoured.

    Surfaced over the wire with ``error_kind="session_lost"`` so clients can
    pattern-match the kind without substring-searching the message. Causes
    include: token presented to a different worker than the one that minted
    it (server_id mismatch), the registry entry aged out via TTL eviction,
    AAD mismatch (cross-principal replay), or any other validation failure.

    Sticky session machinery is HTTP-only; this error never originates from
    pipe/unix transports.
    """

    error_kind: ClassVar[str] = "session_lost"


class ServerDrainingError(Exception):
    """Raised server-side when a sticky-enabled worker is draining and refuses new sessions.

    Surfaced over the wire with ``error_kind="server_draining"``. Existing
    sessions continue to serve through TTL or explicit close; only new
    ``ctx.open_session`` calls are rejected. Operators trigger drain via
    ``RpcServer.drain()`` (typically from a SIGTERM handler) ahead of
    deploy-time worker rotation.
    """

    error_kind: ClassVar[str] = "server_draining"


# ---------------------------------------------------------------------------
# Trace context propagation (pipe/subprocess transport)
# ---------------------------------------------------------------------------

_current_trace_headers: ContextVar[dict[str, str] | None] = ContextVar("_current_trace_headers", default=None)


# ---------------------------------------------------------------------------
# Per-call I/O statistics
# ---------------------------------------------------------------------------


@dataclass
class CallStatistics:
    """Mutable accumulator of per-call I/O counters for usage accounting.

    Created at dispatch start and populated as batches flow through the
    server.  Surfaced through the access log and OTel dispatch hook.

    **Byte measurement**: uses ``pa.RecordBatch.get_total_buffer_size()``
    which reports logical Arrow buffer sizes (O(columns), negligible cost).
    This is an *approximation* — it does **not** include IPC framing
    overhead (padding, schema messages, EOS markers).

    Attributes:
        input_batches: Number of input batches read by the server.
        output_batches: Number of output batches written by the server.
        input_rows: Total rows across all input batches.
        output_rows: Total rows across all output batches.
        input_bytes: Approximate logical bytes across all input batches.
        output_bytes: Approximate logical bytes across all output batches.

    """

    input_batches: int = 0
    output_batches: int = 0
    input_rows: int = 0
    output_rows: int = 0
    input_bytes: int = 0
    output_bytes: int = 0

    def record_input(self, batch: pa.RecordBatch) -> None:
        """Record an input batch's row count and buffer size."""
        self.input_batches += 1
        self.input_rows += batch.num_rows
        self.input_bytes += batch.get_total_buffer_size()

    def record_output(self, batch: pa.RecordBatch) -> None:
        """Record an output batch's row count and buffer size."""
        self.output_batches += 1
        self.output_rows += batch.num_rows
        self.output_bytes += batch.get_total_buffer_size()


_current_call_stats: ContextVar[CallStatistics | None] = ContextVar("vgi_rpc_call_stats", default=None)


def _record_input(batch: pa.RecordBatch) -> None:
    """Record an input batch on the current call's statistics (if any)."""
    stats = _current_call_stats.get()
    if stats is not None:
        stats.record_input(batch)


def _record_output(batch: pa.RecordBatch) -> None:
    """Record an output batch on the current call's statistics (if any)."""
    stats = _current_call_stats.get()
    if stats is not None:
        stats.record_output(batch)


# ---------------------------------------------------------------------------
# Dispatch hook protocol
# ---------------------------------------------------------------------------

type HookToken = object
"""Opaque token returned by ``_DispatchHook.on_dispatch_start``."""


class _DispatchHook(Protocol):
    """Internal protocol for observability hooks called around RPC dispatch."""

    def on_dispatch_start(
        self,
        info: RpcMethodInfo,
        auth: AuthContext,
        transport_metadata: Mapping[str, Any],
        kwargs: Mapping[str, Any],
    ) -> HookToken:
        """Start observability for a dispatch and return an opaque token."""
        ...

    def on_dispatch_end(
        self,
        token: HookToken,
        info: RpcMethodInfo,
        error: BaseException | None,
        *,
        stats: CallStatistics | None = None,
    ) -> None:
        """Finalize observability after method dispatch (success or failure)."""
        ...


class _CompositeDispatchHook:
    """Dispatch hook that delegates to multiple inner hooks in order.

    ``on_dispatch_start`` collects ``(hook, token)`` pairs;
    ``on_dispatch_end`` calls them in reverse (finally-stack semantics).
    Individual hook failures are logged and swallowed so one failing
    hook does not prevent the others from running.
    """

    __slots__ = ("_hooks",)

    def __init__(self, hooks: list[_DispatchHook]) -> None:
        self._hooks = hooks

    def on_dispatch_start(
        self,
        info: RpcMethodInfo,
        auth: AuthContext,
        transport_metadata: Mapping[str, Any],
        kwargs: Mapping[str, Any],
    ) -> HookToken:
        """Delegate to all inner hooks and collect tokens."""
        tokens: list[tuple[_DispatchHook, HookToken]] = []
        for hook in self._hooks:
            try:
                token = hook.on_dispatch_start(info, auth, transport_metadata, kwargs)
                tokens.append((hook, token))
            except Exception:
                _logger.exception("Dispatch hook %r failed on start", hook)
        return tokens

    def on_dispatch_end(
        self,
        token: HookToken,
        info: RpcMethodInfo,
        error: BaseException | None,
        *,
        stats: CallStatistics | None = None,
    ) -> None:
        """Delegate to all inner hooks in reverse order."""
        if not isinstance(token, list):
            return
        pairs = cast(list[tuple["_DispatchHook", HookToken]], token)
        for hook, inner_token in reversed(pairs):
            try:
                hook.on_dispatch_end(inner_token, info, error, stats=stats)
            except Exception:
                _logger.exception("Dispatch hook %r failed on end", hook)


def _register_dispatch_hook(existing: _DispatchHook | None, new_hook: _DispatchHook) -> _DispatchHook:
    """Register a dispatch hook, composing with any existing hook.

    Returns *new_hook* directly when there is no existing hook, wraps
    both in a ``_CompositeDispatchHook`` otherwise, or appends to an
    existing composite.

    Args:
        existing: The current hook on the server (may be ``None``).
        new_hook: The hook to add.

    Returns:
        A hook that delegates to all registered hooks.

    """
    if existing is None:
        return new_hook
    if isinstance(existing, _CompositeDispatchHook):
        existing._hooks.append(new_hook)
        return existing
    return _CompositeDispatchHook([existing, new_hook])
