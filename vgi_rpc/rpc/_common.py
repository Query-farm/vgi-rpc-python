"""Constants, errors, authentication, and call context for the RPC framework."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Mapping, MutableMapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, Protocol

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
    ) -> None:
        """Initialize with auth context, client-log callback, and optional server context fields."""
        self.auth = auth
        self.emit_client_log = emit_client_log
        self.transport_metadata: Mapping[str, Any] = transport_metadata or {}
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


@dataclass(frozen=True)
class _TransportContext:
    """Internal: auth + transport metadata set by the transport layer."""

    auth: AuthContext
    transport_metadata: Mapping[str, Any] = field(default_factory=dict)


_current_transport: ContextVar[_TransportContext | None] = ContextVar("vgi_rpc_transport", default=None)


def _get_auth_and_metadata() -> tuple[AuthContext, Mapping[str, Any]]:
    """Read auth and transport metadata from the current transport contextvar."""
    tc = _current_transport.get()
    if tc is not None:
        return tc.auth, tc.transport_metadata
    return _ANONYMOUS, _EMPTY_TRANSPORT_METADATA


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
