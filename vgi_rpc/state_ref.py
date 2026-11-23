# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

r"""``StateRef``, argument references and the turn budget (``STATE_REF_V1.md``).

On HTTP every stream continuation echoes the stream's state back to the
server.  A :class:`StateRef` is how a server says "this part of my stream
state does not change": store it once, carry a small sealed tag in the state,
read it back on any later turn on any replica::

    def scan(self, query: bytes, ctx: CallContext) -> Stream[ScanState]:
        ref = ctx.state_ref(query)                 # only while handling init
        return Stream(output_schema=..., state=ScanState(query=ref))

    class ScanState(ProducerState):
        query: StateRef

        def produce(self, out, ctx):
            query = ctx.resolve(self.query)        # bytes; any turn, any replica

The client never parses a ``StateRef``: it echoes the opaque state token as it
always has.  Worker code never branches on transport or storage either -- the
same calls work on pipe, stdio, Unix and TCP (where the state never leaves the
process, so the value is simply held in memory) and on HTTP with or without
storage configured.

Byte layouts (``STATE_REF_V1.md`` §16)::

    StateRef (inline)  = 0x01 || codec || body
    StateRef (stored)  = 0x02 || 0x01 || nonce24 || AEAD(tag_plaintext, STATEREF_TAG_AAD)
    tag_plaintext      = 0x01 || created_at u64le || size u64le || sha256[32]
                         || id_len u16le || object_id
    stored object      = 0x01 || nonce24 || AEAD(codec || body, STATEREF_OBJECT_AAD)
    argument ref       = 0x01 || nonce24 || AEAD(0x01 || size u64le || sha256[32]
                         || id_len u16le || object_id, ARGREF_AAD)

Sealing scopes, all under the server's one ``token_key``::

    STATEREF_TAG_AAD    = b"vgi_rpc.stateref.v1\x00"        || BASE_AAD(auth, protocol) || call_id
    STATEREF_OBJECT_AAD = b"vgi_rpc.stateref.object.v1\x00" || BASE_AAD(auth, protocol) || call_id
    ARGREF_AAD          = b"vgi_rpc.argref.v1\x00"          || BASE_AAD(auth, SERVER_SCOPE)

``BASE_AAD`` is exactly the cursor token's AAD (:func:`base_aad`).
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import os
import struct
import time
from collections.abc import Callable, Generator, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeVar

import pyarrow as pa

from vgi_rpc import _token_codec
from vgi_rpc.errors import Code
from vgi_rpc.object_cache import ObjectCache, default_object_cache

if TYPE_CHECKING:
    from vgi_rpc.external import ObjectStore
    from vgi_rpc.rpc._common import AuthContext
    from vgi_rpc.utils import IpcValidation

__all__ = [
    "ARGREF_AAD_PREFIX",
    "CURSOR_RULE_FLOOR_BYTES",
    "DEFAULT_STATE_REF_INLINE_BYTES",
    "DEFAULT_TOKEN_TTL_SECONDS",
    "SERVER_SCOPE",
    "STATEREF_OBJECT_AAD_PREFIX",
    "STATEREF_TAG_AAD_PREFIX",
    "STATE_REF_INLINE_BYTES_ENV",
    "TURN_BUDGET_HEADROOM_BYTES",
    "ArgumentRef",
    "ArgumentsRefIntegrityError",
    "ArgumentsRefMissingError",
    "CursorRule",
    "CursorTooLargeError",
    "InvalidArgumentsRefError",
    "InvalidStateRefError",
    "StateRef",
    "StateRefError",
    "StateRefMissingError",
    "StateRefMisuseError",
    "StateRefRuntime",
    "StateRefScope",
    "StateRefTooLargeError",
    "StorageUnavailableError",
    "argument_ref_aad",
    "base_aad",
    "mint_argument_ref",
    "open_argument_ref",
    "resolve_argument_ref",
    "state_ref_inline_bytes_from_env",
    "turn_budget",
]

_logger = logging.getLogger("vgi_rpc.state_ref")

R = TypeVar("R")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Scope for tokens that belong to the server rather than to any one protocol.
#: Spelled with a leading NUL so it cannot collide with a protocol name.
SERVER_SCOPE = "\x00server"

STATEREF_TAG_AAD_PREFIX = b"vgi_rpc.stateref.v1\x00"
"""Domain prefix of a stored ``StateRef``'s tag AAD."""
STATEREF_OBJECT_AAD_PREFIX = b"vgi_rpc.stateref.object.v1\x00"
"""Domain prefix of a stored ``StateRef``'s object AAD."""
ARGREF_AAD_PREFIX = b"vgi_rpc.argref.v1\x00"
"""Domain prefix of an argument reference's AAD."""

DEFAULT_STATE_REF_INLINE_BYTES = 16384
"""Default ``VGI_RPC_STATE_REF_INLINE_BYTES``."""
STATE_REF_INLINE_BYTES_ENV = "VGI_RPC_STATE_REF_INLINE_BYTES"
"""Environment variable capping the inline payload size."""
TURN_BUDGET_HEADROOM_BYTES = 4096
"""Room left in a continuation request for everything but the two tokens."""
CURSOR_RULE_FLOOR_BYTES = 16384
"""A request cap below this disables the cursor rule (with a startup warning)."""
DEFAULT_TOKEN_TTL_SECONDS = 21600
"""Default maximum age of cursor tokens, call tokens and stored tags (6 h)."""

_FORM_INLINE = 0x01
_FORM_STORED = 0x02
_SEAL_VERSION = 0x01
_TAG_VERSION = 0x01
_ARGREF_VERSION = 0x01
_SHA256_LEN = 32
_MAX_OBJECT_ID_BYTES = 1024
_TAG_FIXED_LEN = 1 + 8 + 8 + _SHA256_LEN + 2
_ARGREF_FIXED_LEN = 1 + 8 + _SHA256_LEN + 2

CursorRule = Literal["warn", "error"]
"""What the server does with a cursor over its turn budget."""

# ---------------------------------------------------------------------------
# Errors (STATE_REF_V1.md §6, vgi large-values-3.4.md §5a)
# ---------------------------------------------------------------------------


class StateRefError(Exception):
    """Base of every ``StateRef`` failure; carries the canonical code and kind."""

    error_code: ClassVar[Code] = Code.INTERNAL
    error_kind: ClassVar[str] = ""


class InvalidStateRefError(StateRefError):
    """Unknown form, tag unseal or scope failure, bad layout, expired tag, digest mismatch.

    Answered with HTTP 400 like any other token failure.
    """

    error_code: ClassVar[Code] = Code.INVALID_ARGUMENT
    error_kind: ClassVar[str] = "invalid_state_ref"


class StateRefMissingError(StateRefError):
    """Storage reports a stored ``StateRef``'s object absent."""

    error_code: ClassVar[Code] = Code.NOT_FOUND
    error_kind: ClassVar[str] = "state_ref_missing"

    def __init__(self) -> None:
        """Create the error with the normative message."""
        super().__init__("stream state object is gone (StateRef object expired or deleted); restart the query")


class StateRefTooLargeError(StateRefError):
    """A stored object exceeds the server's ``max_fetch_bytes`` (at store or resolve)."""

    error_code: ClassVar[Code] = Code.RESOURCE_EXHAUSTED
    error_kind: ClassVar[str] = "state_ref_too_large"


class StateRefMisuseError(StateRefError):
    """``state_ref()`` was called outside the handling of a stream's init request."""

    error_code: ClassVar[Code] = Code.FAILED_PRECONDITION
    error_kind: ClassVar[str] = "state_ref_misuse"


class CursorTooLargeError(StateRefError):
    """A cursor token exceeds the server's turn budget under the ``error`` cursor rule."""

    error_code: ClassVar[Code] = Code.RESOURCE_EXHAUSTED
    error_kind: ClassVar[str] = "cursor_too_large"


class StorageUnavailableError(StateRefError):
    """A transient storage failure (5xx, timeout) while fetching an object."""

    error_code: ClassVar[Code] = Code.UNAVAILABLE
    error_kind: ClassVar[str] = "storage_unavailable"


class InvalidArgumentsRefError(StateRefError):
    """An argument reference does not open, does not match its location, or cannot be resolved here."""

    error_code: ClassVar[Code] = Code.INVALID_ARGUMENT
    error_kind: ClassVar[str] = "invalid_arguments_ref"


class ArgumentsRefIntegrityError(StateRefError):
    """The fetched argument object's length or SHA-256 differs from the sealed values."""

    error_code: ClassVar[Code] = Code.DATA_LOSS
    error_kind: ClassVar[str] = "arguments_ref_integrity"


class ArgumentsRefMissingError(StateRefError):
    """Storage reports an argument object absent (deleted by lifecycle, never uploaded)."""

    error_code: ClassVar[Code] = Code.NOT_FOUND
    error_kind: ClassVar[str] = "arguments_ref_missing"


# ---------------------------------------------------------------------------
# AAD
# ---------------------------------------------------------------------------


def base_aad(auth: AuthContext | None, protocol: str) -> bytes:
    r"""Return the cursor-token AAD for *auth* and *protocol* (``BASE_AAD``).

    Wire format::

        prefix := b"vgi_rpc.state.v7\x00" if a peer-evidence binding digest is present
                  else b"vgi_rpc.state.v6\x00"
        tail   := b"\x00anonymous"                                  (unauthenticated)
                | b"\x01" || domain || b"\x00" || principal          (authenticated)
                  then, if binding present: || b"\x00" || binding
        BASE_AAD := prefix || tail || b"\x00" || protocol

    Args:
        auth: The request's authentication context.
        protocol: Wire name of the owning protocol, or :data:`SERVER_SCOPE`.

    Returns:
        The AAD bytes.

    """
    binding = auth.claims.get("peer_evidence_binding") if auth is not None else None
    has_binding = isinstance(binding, str) and bool(binding)
    prefix = b"vgi_rpc.state.v7\x00" if has_binding else b"vgi_rpc.state.v6\x00"
    scope = b"\x00" + protocol.encode()
    if auth is None or not auth.authenticated:
        aad = prefix + b"\x00anonymous"
    else:
        aad = prefix + b"\x01" + (auth.domain or "").encode() + b"\x00" + (auth.principal or "").encode()
    if has_binding:
        assert isinstance(binding, str)
        aad += b"\x00" + binding.encode()
    return aad + scope


def _tag_aad(base: bytes, call_id: bytes) -> bytes:
    return STATEREF_TAG_AAD_PREFIX + base + call_id


def _object_aad(base: bytes, call_id: bytes) -> bytes:
    return STATEREF_OBJECT_AAD_PREFIX + base + call_id


def argument_ref_aad(auth: AuthContext | None) -> bytes:
    """Return ``ARGREF_AAD`` for *auth*: bound to the principal, not to a protocol.

    Args:
        auth: The request's authentication context.

    Returns:
        The AAD bytes.

    """
    return ARGREF_AAD_PREFIX + base_aad(auth, SERVER_SCOPE)


def _crypto() -> Any:
    from vgi_rpc import crypto

    return crypto


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def state_ref_inline_bytes_from_env(env: Mapping[str, str] | None = None) -> int:
    """Read ``VGI_RPC_STATE_REF_INLINE_BYTES`` (default 16384).

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        The configured inline cap.

    Raises:
        ValueError: If the variable is set to anything but a positive integer.

    """
    source = os.environ if env is None else env
    raw = source.get(STATE_REF_INLINE_BYTES_ENV)
    if raw is None or not raw.strip():
        return DEFAULT_STATE_REF_INLINE_BYTES
    try:
        value = int(raw.strip())
    except ValueError:
        value = 0
    if value <= 0:
        raise ValueError(f"{STATE_REF_INLINE_BYTES_ENV} must be a positive integer, got {raw!r}")
    return value


def turn_budget(max_request_bytes: int | None, call_token_bytes: int) -> int | None:
    """Return the sealed bytes a cursor may take on a continuation, or ``None`` for unbounded.

    ``floor((max_request_bytes - 4096) / 1.34) - call_token_bytes``; the
    ``1.34`` converts the request's base64 metadata size to sealed bytes.

    Args:
        max_request_bytes: The advertised ``VGI-Max-Request-Bytes``; ``None``
            or ``<= 0`` means unbounded.
        call_token_bytes: Sealed length of the stream's call token (``0``
            when sizing the call token's own ``schemas_ref``).

    Returns:
        The budget in sealed bytes (may be negative under a tiny cap), or
        ``None`` when the request size is unbounded.

    """
    if max_request_bytes is None or max_request_bytes <= 0:
        return None
    return ((max_request_bytes - TURN_BUDGET_HEADROOM_BYTES) * 100) // 134 - call_token_bytes


class StateRefRuntime:
    """One server's ``StateRef`` machinery: key, storage, budgets and cache.

    Built by the HTTP app (:func:`~vgi_rpc.http.make_wsgi_app`); the byte-stream
    transports never need one, because their state never leaves the process.
    """

    __slots__ = (
        "_cursor_rule_disabled",
        "_storage_source",
        "cache",
        "clock",
        "cursor_rule",
        "inline_bytes_cap",
        "max_fetch_bytes",
        "max_request_bytes",
        "token_key",
        "token_ttl",
    )

    def __init__(
        self,
        *,
        token_key: bytes,
        token_ttl: int = DEFAULT_TOKEN_TTL_SECONDS,
        storage: Callable[[], object | None] | None = None,
        max_fetch_bytes: int,
        max_request_bytes: int | None = None,
        inline_bytes_cap: int = DEFAULT_STATE_REF_INLINE_BYTES,
        cursor_rule: CursorRule = "warn",
        cache: ObjectCache | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Create the runtime.

        Args:
            token_key: The server's 32-byte AEAD key (shared by every replica).
            token_ttl: Maximum age of a stored tag in seconds; ``0`` disables.
            storage: Returns the configured storage (or ``None``); read per
                use so a server whose storage is swapped picks it up.  A
                storage that is not an :class:`~vgi_rpc.external.ObjectStore`
                counts as none: refs stay inline.
            max_fetch_bytes: The server's inbound fetch cap.
            max_request_bytes: The advertised request cap, or ``None``.
            inline_bytes_cap: ``VGI_RPC_STATE_REF_INLINE_BYTES``.
            cursor_rule: ``"warn"`` logs ``cursor_large``; ``"error"`` raises
                ``cursor_too_large`` (only when a request cap is set).
            cache: The object cache; defaults to the process-wide one.
            clock: Wall-clock source, injectable for tests.

        Raises:
            ValueError: If *inline_bytes_cap* is not positive or *cursor_rule*
                is not ``"warn"``/``"error"``.

        """
        if type(inline_bytes_cap) is not int or inline_bytes_cap <= 0:
            raise ValueError(f"state_ref_inline_bytes must be a positive integer, got {inline_bytes_cap!r}")
        if cursor_rule not in ("warn", "error"):
            raise ValueError(f"cursor_rule must be 'warn' or 'error', got {cursor_rule!r}")
        self.token_key = token_key
        self.token_ttl = token_ttl
        self._storage_source = storage
        self.max_fetch_bytes = max_fetch_bytes
        self.max_request_bytes = max_request_bytes if max_request_bytes is not None and max_request_bytes > 0 else None
        self.inline_bytes_cap = inline_bytes_cap
        self.cursor_rule: CursorRule = cursor_rule
        self.cache = cache if cache is not None else default_object_cache()
        self.clock = clock
        self._cursor_rule_disabled = (
            self.max_request_bytes is not None and self.max_request_bytes < CURSOR_RULE_FLOOR_BYTES
        )
        if self._cursor_rule_disabled:
            _logger.warning(
                "max_request_bytes (%d) is below %d: the cursor rule is disabled and stream continuations may not fit",
                self.max_request_bytes,
                CURSOR_RULE_FLOOR_BYTES,
            )

    @property
    def storage(self) -> ObjectStore | None:
        """The configured object store, or ``None`` when there is none."""
        from vgi_rpc.external import ObjectStore

        if self._storage_source is None:
            return None
        candidate = self._storage_source()
        return candidate if isinstance(candidate, ObjectStore) else None

    @property
    def cursor_rule_enabled(self) -> bool:
        """Whether over-budget cursors warn or error at all (off below the 16 KiB floor)."""
        return not self._cursor_rule_disabled

    @property
    def decompress_cap(self) -> int:
        """Largest decompressed ``StateRef`` value: 64 MiB or ``max_fetch_bytes``, whichever is larger."""
        return max(_token_codec.MAX_TOKEN_PLAINTEXT_BYTES, self.max_fetch_bytes)

    def turn_budget(self, call_token_bytes: int) -> int | None:
        """Return :func:`turn_budget` for this server's request cap.

        Args:
            call_token_bytes: Sealed length of the stream's call token.

        Returns:
            The budget, or ``None`` when unbounded.

        """
        return turn_budget(self.max_request_bytes, call_token_bytes)

    def inline_bytes(self, call_token_bytes: int) -> int:
        """Return ``state_ref_inline_bytes``: ``min(cap, floor(turn_budget / 4))``, floored at 0.

        Args:
            call_token_bytes: Sealed length of the stream's call token.

        Returns:
            The largest payload kept inline.

        """
        budget = self.turn_budget(call_token_bytes)
        if budget is None:
            return self.inline_bytes_cap
        return max(0, min(self.inline_bytes_cap, budget // 4))

    def check_cursor(self, *, cursor_bytes: int, call_token_bytes: int, protocol: str, method: str) -> str | None:
        """Apply the cursor rule to one sealed cursor.

        Args:
            cursor_bytes: Sealed (base64-decoded) length of the cursor token.
            call_token_bytes: Sealed length of the stream's call token.
            protocol: Owning protocol, for the message.
            method: Method name, for the message.

        Returns:
            ``None`` when the cursor fits (or the rule is off), else the
            ``cursor_large`` warning text (``"warn"`` rule).

        Raises:
            CursorTooLargeError: The cursor is over budget under the
                ``"error"`` rule with a request cap set.

        """
        if self._cursor_rule_disabled:
            return None
        budget = self.turn_budget(call_token_bytes)
        if budget is None or cursor_bytes <= budget:
            return None
        detail = (
            f"stream state for {protocol}.{method} is {cursor_bytes} bytes, over this server's turn budget of "
            f"{budget} bytes (VGI-Max-Request-Bytes {self.max_request_bytes}). Wrap data that does not change "
            f"across turns in ctx.state_ref()."
        )
        extra = {
            "event": "cursor_large",
            "protocol": protocol,
            "method": method,
            "cursor_bytes": cursor_bytes,
            "budget": budget,
        }
        if self.cursor_rule == "error":
            extra["event"] = "cursor_too_large"
            _logger.warning("cursor_too_large: %s", detail, extra=extra)
            raise CursorTooLargeError(f"cursor_too_large: {detail}")
        _logger.warning("cursor_large: %s", detail, extra=extra)
        return f"cursor_large: {detail}"


# ---------------------------------------------------------------------------
# StateRef
# ---------------------------------------------------------------------------


class _Encoder:
    """Everything needed to decide a ref's form and seal it, for one cursor mint."""

    __slots__ = ("base_aad", "call_id", "inline_bytes", "method", "protocol", "runtime")

    def __init__(
        self,
        runtime: StateRefRuntime,
        *,
        base: bytes,
        call_id: bytes,
        inline_bytes: int,
        protocol: str,
        method: str,
    ) -> None:
        self.runtime = runtime
        self.base_aad = base
        self.call_id = call_id
        self.inline_bytes = inline_bytes
        self.protocol = protocol
        self.method = method


_current_encoder: ContextVar[_Encoder | None] = ContextVar("vgi_rpc_state_ref_encoder", default=None)


class StateRef:
    """An immutable value carried in stream state by reference.

    Create one with :meth:`CallContext.state_ref` while handling a stream's
    init request; store it in a field of your state annotated ``StateRef``;
    read it back with :meth:`CallContext.resolve` on any turn.

    The value is held in memory until the state is first serialized into an
    HTTP state token.  That serialization decides the form -- inline (a
    compressed payload inside the token) or stored (uploaded once, a sealed
    tag in the token) -- and caches the encoded bytes, so later
    serializations re-emit them exactly.  A ref read back from a token holds
    only those bytes.  On pipe, stdio, Unix and TCP nothing is ever encoded.
    """

    __slots__ = ("_encoded", "_record", "_value")

    #: Arrow type of a ``StateRef`` field (read by the serialization layer).
    __vgi_arrow_type__: ClassVar[pa.DataType] = pa.binary()

    _value: bytes | None
    _record: object | None
    _encoded: bytes | None

    def __init__(self) -> None:
        """Refuse direct construction.

        Raises:
            TypeError: Always; use :meth:`CallContext.state_ref`.

        """
        raise TypeError("StateRef is created with ctx.state_ref(value)")

    @classmethod
    def _from_value(cls, value: bytes, record: object | None = None) -> StateRef:
        ref = object.__new__(cls)
        ref._value = value
        ref._record = record
        ref._encoded = None
        return ref

    @classmethod
    def _from_encoded(cls, encoded: bytes) -> StateRef:
        ref = object.__new__(cls)
        ref._value = None
        ref._record = None
        ref._encoded = encoded
        return ref

    @property
    def form(self) -> Literal["memory", "inline", "stored", "invalid"]:
        """Where the value lives: ``memory`` (not yet encoded), ``inline`` or ``stored``."""
        if self._encoded is None:
            return "memory"
        head = self._encoded[:1]
        if head == bytes((_FORM_INLINE,)):
            return "inline"
        if head == bytes((_FORM_STORED,)):
            return "stored"
        return "invalid"

    @property
    def encoded(self) -> bytes | None:
        """The encoded bytes once the form is decided, else ``None``."""
        return self._encoded

    def serialize_to_bytes(self) -> bytes:
        """Return the field's wire bytes, deciding the form on first token serialization.

        Inside a state-token mint (the HTTP server sets an encoder) the form
        is decided, any store performed, and the result cached.  Outside one
        -- a caller serializing a state object for its own reasons -- an
        un-encoded ref renders inline without caching, so the next token
        mint still decides properly.

        Returns:
            The encoded ``StateRef`` bytes.

        """
        if self._encoded is not None:
            return self._encoded
        encoder = _current_encoder.get()
        value = self._value
        assert value is not None
        if encoder is None:
            return bytes((_FORM_INLINE,)) + _token_codec.pack(value)
        # No lock around the encode: it may upload, and a ref is only ever
        # un-encoded during its own stream's init turn, which one thread runs.
        encoded = _encode_value(value, encoder)
        if self._encoded is None:
            self._encoded = encoded
        return self._encoded

    @classmethod
    def deserialize_from_bytes(cls, data: bytes, ipc_validation: IpcValidation | None = None) -> StateRef:
        """Rebuild a ref from its wire bytes (validated only when resolved).

        Args:
            data: The encoded bytes.
            ipc_validation: Unused; accepted for the serialization layer's
                ``deserialize_from_bytes`` calling convention.

        Returns:
            A ref holding exactly *data*.

        """
        del ipc_validation
        return cls._from_encoded(bytes(data))

    def _identity(self) -> bytes:
        if self._encoded is not None:
            return self._encoded
        assert self._value is not None
        return b"\xff" + self._value

    def __eq__(self, other: object) -> bool:
        """Compare by encoded bytes, or by value when neither is encoded."""
        if not isinstance(other, StateRef):
            return NotImplemented
        return self is other or self._identity() == other._identity()

    def __hash__(self) -> int:
        """Hash consistently with :meth:`__eq__`."""
        return hash(self._identity())

    def __repr__(self) -> str:
        """Describe the form and size, never the value."""
        if self._encoded is None:
            assert self._value is not None
            return f"StateRef(form='memory', bytes={len(self._value)})"
        return f"StateRef(form={self.form!r}, encoded_bytes={len(self._encoded)})"


def _encode_value(value: bytes, encoder: _Encoder) -> bytes:
    """Decide a value's form and encode it (§5.2), storing it when it is stored."""
    payload = _token_codec.pack(value)
    runtime = encoder.runtime
    storage = runtime.storage
    if storage is None or len(payload) <= encoder.inline_bytes:
        _logger.debug(
            "state_ref.store",
            extra={
                "event": "state_ref.store",
                "protocol": encoder.protocol,
                "method": encoder.method,
                "payload_bytes": len(payload),
                "object_bytes": 0,
                "form": "inline",
            },
        )
        return bytes((_FORM_INLINE,)) + payload
    crypto = _crypto()
    object_bytes: bytes = crypto.seal_bytes(
        payload,
        runtime.token_key,
        aad=_object_aad(encoder.base_aad, encoder.call_id),
        version=_SEAL_VERSION,
    )
    if len(object_bytes) > runtime.max_fetch_bytes:
        raise StateRefTooLargeError(
            f"StateRef object for {encoder.protocol}.{encoder.method} is {len(object_bytes)} bytes, over this "
            f"server's max_fetch_bytes ({runtime.max_fetch_bytes}); no replica could fetch it back"
        )
    object_id = storage.put_object(object_bytes)
    id_bytes = object_id.encode()
    if not 1 <= len(id_bytes) <= _MAX_OBJECT_ID_BYTES:
        raise RuntimeError(f"storage returned an object_id of {len(id_bytes)} bytes (must be 1-1024)")
    tag_plaintext = (
        bytes((_TAG_VERSION,))
        + struct.pack("<QQ", int(runtime.clock()), len(object_bytes))
        + hashlib.sha256(object_bytes).digest()
        + struct.pack("<H", len(id_bytes))
        + id_bytes
    )
    tag: bytes = crypto.seal_bytes(
        tag_plaintext,
        runtime.token_key,
        aad=_tag_aad(encoder.base_aad, encoder.call_id),
        version=_SEAL_VERSION,
    )
    _logger.debug(
        "state_ref.store",
        extra={
            "event": "state_ref.store",
            "protocol": encoder.protocol,
            "method": encoder.method,
            "payload_bytes": len(payload),
            "object_bytes": len(object_bytes),
            "form": "stored",
        },
    )
    return bytes((_FORM_STORED,)) + tag


@contextlib.contextmanager
def _encoding(encoder: _Encoder | None) -> Generator[None]:
    """Install *encoder* for :meth:`StateRef.serialize_to_bytes` for the block."""
    token = _current_encoder.set(encoder)
    try:
        yield
    finally:
        _current_encoder.reset(token)


def _unpack_value(payload: bytes, cap: int) -> bytes:
    try:
        return _token_codec.unpack(payload, cap)
    except _token_codec.TokenCodecError as exc:
        raise InvalidStateRefError(f"StateRef payload is malformed: {exc}") from None


def _resolve_stored(
    tag_envelope: bytes,
    *,
    runtime: StateRefRuntime,
    base: bytes,
    call_id: bytes,
) -> bytes:
    """Resolve a stored ref's tag envelope (§5.3 step 2), through the object cache."""
    crypto = _crypto()
    try:
        tag_plaintext: bytes = crypto.open_bytes(
            tag_envelope, runtime.token_key, aad=_tag_aad(base, call_id), version=_SEAL_VERSION
        )
    except crypto.SealError:
        raise InvalidStateRefError("StateRef tag does not open (tampered, or minted for another stream)") from None
    if len(tag_plaintext) < _TAG_FIXED_LEN or tag_plaintext[0] != _TAG_VERSION:
        raise InvalidStateRefError("StateRef tag has an unknown version or a malformed layout")
    created_at, size = struct.unpack_from("<QQ", tag_plaintext, 1)
    digest = tag_plaintext[17 : 17 + _SHA256_LEN]
    (id_len,) = struct.unpack_from("<H", tag_plaintext, 17 + _SHA256_LEN)
    if not 1 <= id_len <= _MAX_OBJECT_ID_BYTES or _TAG_FIXED_LEN + id_len != len(tag_plaintext):
        raise InvalidStateRefError("StateRef tag has a malformed layout")
    try:
        object_id = tag_plaintext[_TAG_FIXED_LEN:].decode()
    except UnicodeDecodeError:
        raise InvalidStateRefError("StateRef tag has a malformed layout") from None
    if runtime.token_ttl > 0 and int(runtime.clock()) - created_at > runtime.token_ttl:
        raise InvalidStateRefError("StateRef expired")
    if size > runtime.max_fetch_bytes:
        raise StateRefTooLargeError(
            f"StateRef object is {size} bytes, over this server's max_fetch_bytes ({runtime.max_fetch_bytes})"
        )
    storage = runtime.storage
    if storage is None:
        raise InvalidStateRefError("a stored StateRef cannot be resolved: this server has no object storage")

    def fetch() -> bytes:
        object_bytes = _fetch_object(storage, object_id, runtime.max_fetch_bytes, missing=StateRefMissingError)
        if len(object_bytes) != size:
            raise InvalidStateRefError("StateRef object size mismatch")
        if hashlib.sha256(object_bytes).digest() != digest:
            raise InvalidStateRefError("StateRef object digest mismatch")
        try:
            payload: bytes = crypto.open_bytes(
                object_bytes, runtime.token_key, aad=_object_aad(base, call_id), version=_SEAL_VERSION
            )
        except crypto.SealError:
            raise InvalidStateRefError("StateRef object does not open") from None
        return _unpack_value(payload, runtime.decompress_cap)

    value, hit = runtime.cache.get_or_fetch(object_id, fetch)
    _logger.debug(
        "state_ref.resolve",
        extra={"event": "state_ref.resolve", "form": "stored", "cache": "hit" if hit else "miss", "bytes": len(value)},
    )
    return value


def _fetch_object(
    storage: ObjectStore,
    object_id: str,
    max_bytes: int,
    *,
    missing: Callable[[], Exception],
) -> bytes:
    """``storage.get`` with the error mapping both resolvers share."""
    from vgi_rpc.external import ObjectMissingError

    try:
        return storage.get(object_id, max_bytes=max_bytes)
    except ObjectMissingError:
        raise missing() from None
    except StateRefError:
        raise
    except Exception as exc:
        raise StorageUnavailableError(f"object storage fetch failed ({type(exc).__name__}): {exc}") from None


def _resolve_encoded(
    encoded: bytes,
    *,
    runtime: StateRefRuntime | None,
    base: bytes | None,
    call_id: bytes | None,
) -> bytes:
    """Resolve encoded ``StateRef`` bytes in the given stream scope."""
    if not encoded:
        raise InvalidStateRefError("StateRef is empty")
    form = encoded[0]
    if form == _FORM_INLINE:
        cap = runtime.decompress_cap if runtime is not None else _token_codec.MAX_TOKEN_PLAINTEXT_BYTES
        value = _unpack_value(encoded[1:], cap)
        _logger.debug(
            "state_ref.resolve",
            extra={"event": "state_ref.resolve", "form": "inline", "cache": "miss", "bytes": len(value)},
        )
        return value
    if form == _FORM_STORED:
        if runtime is None or base is None or call_id is None:
            raise InvalidStateRefError("a stored StateRef cannot be resolved outside an HTTP stream turn")
        return _resolve_stored(encoded[1:], runtime=runtime, base=base, call_id=call_id)
    raise InvalidStateRefError(f"StateRef has unknown form 0x{form:02x}")


class StateRefScope:
    """The ``StateRef`` view of one request: what :class:`CallContext` delegates to.

    The framework attaches one to every :class:`CallContext` it builds.  On
    HTTP it carries the server runtime and the stream's ``call_id``; on the
    byte-stream transports there is no runtime (values never leave memory).
    ``init`` is ``True`` only for the context handed to the stream method
    itself -- the one place ``state_ref()`` is allowed.
    """

    __slots__ = ("_memo_bytes", "_memo_ids", "auth", "call_id", "init", "method", "protocol", "runtime")

    def __init__(
        self,
        *,
        runtime: StateRefRuntime | None = None,
        auth: AuthContext | None = None,
        protocol: str = "",
        method: str = "",
        call_id: bytes | None = None,
        init: bool = False,
    ) -> None:
        """Create a scope.

        Args:
            runtime: The server's runtime (HTTP), or ``None``.
            auth: The request's authentication context.
            protocol: Owning protocol's wire name.
            method: Method name.
            call_id: The stream's call id, known on continuations.
            init: Whether this is the stream method's own context.

        """
        self.runtime = runtime
        self.auth = auth
        self.protocol = protocol
        self.method = method
        self.call_id = call_id
        self.init = init
        self._memo_bytes: dict[bytes, StateRef] = {}
        self._memo_ids: dict[int, tuple[object, StateRef]] = {}

    def state_ref(self, value: object) -> StateRef:
        """Wrap *value* in a ``StateRef`` (see :meth:`CallContext.state_ref`).

        Args:
            value: ``bytes`` or an ``ArrowSerializableDataclass``.

        Returns:
            The ref; byte-equal or identical values within one init share one.

        Raises:
            StateRefMisuseError: Not handling a stream's init request.
            TypeError: *value* is neither bytes nor a serializable record.

        """
        if not self.init:
            raise StateRefMisuseError(
                f"state_ref() may only be called while handling a stream's init request ({self.protocol}.{self.method})"
            )
        remembered = self._memo_ids.get(id(value))
        if remembered is not None and remembered[0] is value:
            return remembered[1]
        record: object | None = None
        if isinstance(value, bytes | bytearray | memoryview):
            data = bytes(value)
        else:
            serialize = getattr(value, "serialize_to_bytes", None)
            if serialize is None or not callable(serialize):
                raise TypeError(f"state_ref() takes bytes or an ArrowSerializableDataclass, got {type(value).__name__}")
            data = serialize()
            if not isinstance(data, bytes):
                raise TypeError(f"{type(value).__name__}.serialize_to_bytes() did not return bytes")
            record = value
        ref = self._memo_bytes.get(data)
        if ref is None:
            ref = StateRef._from_value(data, record)
            self._memo_bytes[data] = ref
        self._memo_ids[id(value)] = (value, ref)
        return ref

    def resolve_bytes(self, ref: StateRef) -> bytes:
        """Return the value bytes *ref* names (see :meth:`CallContext.resolve`).

        Args:
            ref: The ref to resolve.

        Returns:
            The value's serialized bytes.

        Raises:
            TypeError: *ref* is not a ``StateRef``.

        """
        if not isinstance(ref, StateRef):
            raise TypeError(f"resolve() takes a StateRef, got {type(ref).__name__}")
        if ref._value is not None:
            return ref._value
        assert ref._encoded is not None
        base = base_aad(self.auth, self.protocol) if self.runtime is not None else None
        return _resolve_encoded(ref._encoded, runtime=self.runtime, base=base, call_id=self.call_id)

    def resolve(self, ref: StateRef, record_type: type[R] | None = None) -> bytes | R:
        """Resolve *ref*, optionally deserializing into *record_type*.

        Args:
            ref: The ref to resolve.
            record_type: An ``ArrowSerializableDataclass`` subclass, or ``None``
                for raw bytes.

        Returns:
            Bytes, or an instance of *record_type* (the original instance
            when the ref still holds it in memory).

        """
        if record_type is None:
            return self.resolve_bytes(ref)
        if ref._value is not None and isinstance(ref._record, record_type):
            return ref._record
        data = self.resolve_bytes(ref)
        deserialize = getattr(record_type, "deserialize_from_bytes", None)
        if deserialize is None:
            raise TypeError(f"{record_type.__name__} has no deserialize_from_bytes()")
        result: R = deserialize(data)
        return result

    def resolve_argument_ref(self, ref: bytes, *, size: int, sha256: str) -> bytes:
        """Resolve an argument reference (see :meth:`CallContext.resolve_argument_ref`).

        Args:
            ref: The sealed reference from the grant's ``ref`` column.
            size: The size the caller claims alongside it.
            sha256: The SHA-256 hex the caller claims alongside it.

        Returns:
            The verified object bytes.

        Raises:
            InvalidArgumentsRefError: No HTTP runtime (byte-stream transports
                vend no grants), or any failure :func:`resolve_argument_ref`
                maps to it.

        """
        if self.runtime is None:
            raise InvalidArgumentsRefError("argument references can only be resolved by an HTTP server")
        return resolve_argument_ref(
            ref,
            size=size,
            sha256=sha256,
            token_key=self.runtime.token_key,
            auth=self.auth,
            storage=self.runtime.storage,
            max_fetch_bytes=self.runtime.max_fetch_bytes,
            cache=self.runtime.cache,
        )


# ---------------------------------------------------------------------------
# Framework helpers (HTTP)
# ---------------------------------------------------------------------------


def _make_encoder(
    runtime: StateRefRuntime,
    *,
    auth: AuthContext | None,
    protocol: str,
    method: str,
    call_id: bytes,
    call_token_bytes: int,
) -> _Encoder:
    return _Encoder(
        runtime,
        base=base_aad(auth, protocol),
        call_id=call_id,
        inline_bytes=runtime.inline_bytes(call_token_bytes),
        protocol=protocol,
        method=method,
    )


def encode_value(
    runtime: StateRefRuntime,
    value: bytes,
    *,
    auth: AuthContext | None,
    protocol: str,
    method: str,
    call_id: bytes,
    call_token_bytes: int,
) -> bytes:
    """Encode *value* as a ``StateRef`` directly (the call token's ``schemas_ref``).

    Args:
        runtime: The server runtime.
        value: The value bytes.
        auth: The request's authentication context.
        protocol: Owning protocol.
        method: Method name.
        call_id: The stream's call id.
        call_token_bytes: Sealed call-token length for the inline limit
            (``0`` for the call token's own ``schemas_ref``).

    Returns:
        The encoded ref bytes.

    """
    encoder = _make_encoder(
        runtime, auth=auth, protocol=protocol, method=method, call_id=call_id, call_token_bytes=call_token_bytes
    )
    return _encode_value(value, encoder)


def resolve_encoded(
    runtime: StateRefRuntime,
    encoded: bytes,
    *,
    auth: AuthContext | None,
    protocol: str,
    call_id: bytes,
) -> bytes:
    """Resolve encoded ``StateRef`` bytes for the stream *call_id* (the call token's ``schemas_ref``).

    Args:
        runtime: The server runtime.
        encoded: The encoded ref.
        auth: The request's authentication context.
        protocol: Owning protocol.
        call_id: The stream's call id.

    Returns:
        The value bytes.

    """
    return _resolve_encoded(encoded, runtime=runtime, base=base_aad(auth, protocol), call_id=call_id)


@contextlib.contextmanager
def encoding_for_stream(
    runtime: StateRefRuntime,
    *,
    auth: AuthContext | None,
    protocol: str,
    method: str,
    call_id: bytes,
    call_token_bytes: int,
) -> Generator[None]:
    """Install the encoder that decides un-encoded refs' forms during a cursor mint.

    Args:
        runtime: The server runtime.
        auth: The request's authentication context.
        protocol: Owning protocol.
        method: Method name.
        call_id: The stream's call id.
        call_token_bytes: Sealed call-token length (enters the inline limit).

    Yields:
        Nothing; serialize the state inside the block.

    """
    encoder = _make_encoder(
        runtime, auth=auth, protocol=protocol, method=method, call_id=call_id, call_token_bytes=call_token_bytes
    )
    with _encoding(encoder):
        yield


# ---------------------------------------------------------------------------
# Argument references (STATE_REF_V1.md §10.2)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ArgumentRef:
    """The opened contents of an argument reference.

    Attributes:
        size: The sealed byte length.
        sha256: The sealed SHA-256, 64 lowercase hex.
        object_id: The storage id.

    """

    size: int
    sha256: str
    object_id: str


def mint_argument_ref(token_key: bytes, auth: AuthContext | None, *, size: int, sha256: str, object_id: str) -> bytes:
    """Seal an argument reference for one granted upload.

    Minted from the *claimed* ``(size, sha256)`` without seeing the bytes:
    storage enforces them on the PUT where it can, and the resolver verifies
    them on the GET.  There is no expiry; on an unauthenticated server a
    reference is a bearer capability for as long as the object lives.

    Args:
        token_key: The server's AEAD key.
        auth: The ``__upload_url__/init`` request's authentication context.
        size: The entry's declared size (``>= 1``).
        sha256: The entry's declared SHA-256, 64 lowercase hex.
        object_id: The storage id the provider returned.

    Returns:
        The sealed reference (``0x01 || nonce || ciphertext+tag``).

    Raises:
        ValueError: On a bad size, digest or object id.

    """
    from vgi_rpc.external import is_sha256_hex

    if type(size) is not int or size < 1:
        raise ValueError(f"argument ref size must be an integer >= 1, got {size!r}")
    if not is_sha256_hex(sha256):
        raise ValueError("argument ref sha256 must be 64 lowercase hex characters")
    id_bytes = object_id.encode()
    if not 1 <= len(id_bytes) <= _MAX_OBJECT_ID_BYTES:
        raise ValueError(f"object_id must be 1-{_MAX_OBJECT_ID_BYTES} bytes of UTF-8, got {len(id_bytes)}")
    plaintext = (
        bytes((_ARGREF_VERSION,))
        + struct.pack("<Q", size)
        + bytes.fromhex(sha256)
        + struct.pack("<H", len(id_bytes))
        + id_bytes
    )
    sealed: bytes = _crypto().seal_bytes(plaintext, token_key, aad=argument_ref_aad(auth), version=_SEAL_VERSION)
    _logger.debug("arguments_ref.mint", extra={"event": "arguments_ref.mint", "size": size})
    return sealed


def open_argument_ref(token_key: bytes, auth: AuthContext | None, ref: bytes) -> ArgumentRef:
    """Open an argument reference sealed for *auth*.

    Args:
        token_key: The server's AEAD key.
        auth: The presenting request's authentication context.
        ref: The sealed reference.

    Returns:
        The opened :class:`ArgumentRef`.

    Raises:
        InvalidArgumentsRefError: Wrong key, principal or scope, tampering,
            unknown version, or a malformed layout.

    """
    crypto = _crypto()
    try:
        plaintext: bytes = crypto.open_bytes(ref, token_key, aad=argument_ref_aad(auth), version=_SEAL_VERSION)
    except crypto.SealError:
        raise InvalidArgumentsRefError(
            "argument reference does not open (tampered, or granted to another principal or server)"
        ) from None
    if len(plaintext) < _ARGREF_FIXED_LEN or plaintext[0] != _ARGREF_VERSION:
        raise InvalidArgumentsRefError("argument reference has an unknown version or a malformed layout")
    (size,) = struct.unpack_from("<Q", plaintext, 1)
    digest = plaintext[9 : 9 + _SHA256_LEN]
    (id_len,) = struct.unpack_from("<H", plaintext, 9 + _SHA256_LEN)
    if not 1 <= id_len <= _MAX_OBJECT_ID_BYTES or _ARGREF_FIXED_LEN + id_len != len(plaintext):
        raise InvalidArgumentsRefError("argument reference has a malformed layout")
    try:
        object_id = plaintext[_ARGREF_FIXED_LEN:].decode()
    except UnicodeDecodeError:
        raise InvalidArgumentsRefError("argument reference has a malformed layout") from None
    return ArgumentRef(size=size, sha256=digest.hex(), object_id=object_id)


def resolve_argument_ref(
    ref: bytes,
    *,
    size: int,
    sha256: str,
    token_key: bytes,
    auth: AuthContext | None,
    storage: ObjectStore | None,
    max_fetch_bytes: int,
    cache: ObjectCache | None = None,
) -> bytes:
    """Open, fetch (through the object cache) and verify an argument reference.

    Order: validate the claimed location; open the ref with ``ARGREF_AAD``;
    check the sealed size and SHA-256 equal the claimed ones; look the
    ``object_id`` up in the object cache; on a miss, single-flight
    ``storage.get``; verify length and SHA-256; return.

    Args:
        ref: The sealed reference.
        size: The claimed byte length (``ArgumentsLocation.size``).
        sha256: The claimed SHA-256, 64 lowercase hex.
        token_key: The server's AEAD key.
        auth: The presenting request's authentication context.
        storage: The server's object store, or ``None``.
        max_fetch_bytes: The server's inbound fetch cap.
        cache: The object cache; defaults to the process-wide one.

    Returns:
        The verified object bytes.

    Raises:
        InvalidArgumentsRefError: Malformed location, no storage, a ref that
            does not open for this caller, or sealed values that differ from
            the claimed ones.
        ArgumentsRefIntegrityError: The fetched bytes' length or digest
            differs from the sealed values.
        ArgumentsRefMissingError: Storage reports the object absent.
        StorageUnavailableError: A transient storage failure.

    """
    from vgi_rpc.external import is_sha256_hex

    if type(size) is not int or size < 1:
        raise InvalidArgumentsRefError(f"argument location size must be an integer >= 1, got {size!r}")
    if not is_sha256_hex(sha256):
        raise InvalidArgumentsRefError("argument location sha256 must be 64 lowercase hex characters")
    if storage is None:
        raise InvalidArgumentsRefError("this server has no object storage configured to resolve argument references")
    opened = open_argument_ref(token_key, auth, ref)
    if opened.size != size or opened.sha256 != sha256:
        raise InvalidArgumentsRefError("argument reference does not match the size and sha256 it was sent with")
    if size > max_fetch_bytes:
        raise InvalidArgumentsRefError(
            f"argument object is {size} bytes, over this server's max_fetch_bytes ({max_fetch_bytes})"
        )
    the_cache = cache if cache is not None else default_object_cache()
    expected_digest = bytes.fromhex(sha256)

    def fetch() -> bytes:
        data = _fetch_object(
            storage,
            opened.object_id,
            max_fetch_bytes,
            missing=lambda: ArgumentsRefMissingError("the uploaded argument object is gone (expired or deleted)"),
        )
        if len(data) != size:
            raise ArgumentsRefIntegrityError(f"argument object size mismatch: expected {size}, got {len(data)}")
        if hashlib.sha256(data).digest() != expected_digest:
            raise ArgumentsRefIntegrityError("argument object sha256 mismatch")
        return data

    value, hit = the_cache.get_or_fetch(opened.object_id, fetch)
    if len(value) != size:
        # A cached value always passed the checks above for this object id;
        # a length change means the id was reused, which storage never does.
        raise ArgumentsRefIntegrityError(f"argument object size mismatch: expected {size}, got {len(value)}")
    _logger.debug(
        "arguments_ref.resolve",
        extra={"event": "arguments_ref.resolve", "cache": "hit" if hit else "miss", "bytes": len(value)},
    )
    return value
