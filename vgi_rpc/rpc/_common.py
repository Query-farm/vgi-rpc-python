"""Constants, errors, authentication, and call context for the RPC framework."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, MutableMapping
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Final

import pyarrow as pa

from vgi_rpc.log import Level, Message

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
        self._logger: _ContextLoggerAdapter | None = None

    @property
    def logger(self) -> logging.LoggerAdapter[logging.Logger]:
        """Server-side logger with request context pre-bound.

        Returns:
            A ``LoggerAdapter`` with logger name
            ``vgi_rpc.service.<ProtocolName>``.  Always includes
            ``server_id`` and ``method``; conditionally includes
            ``principal``, ``auth_domain``, and ``remote_addr``
            when available.

        """
        if self._logger is None:
            base = logging.getLogger(f"vgi_rpc.service.{self._protocol_name}")
            extra: dict[str, object] = {
                "server_id": self._server_id,
                "method": self._method_name,
            }
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

    def __init__(self, error_type: str, error_message: str, remote_traceback: str) -> None:
        """Initialize with error details from the remote side."""
        self.error_type = error_type
        self.error_message = error_message
        self.remote_traceback = remote_traceback
        super().__init__(f"{error_type}: {error_message}")


class VersionError(Exception):
    """Raised when a request has a missing or incompatible protocol version."""
