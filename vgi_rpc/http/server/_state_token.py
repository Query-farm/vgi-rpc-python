# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Encrypted state token helpers for the HTTP streaming protocol.

Streaming is stateless on the wire: each exchange carries the serialized
``StreamState`` inside an authenticated-encrypted (AEAD) token in Arrow
custom metadata. Tokens are sealed with XChaCha20-Poly1305 — confidential
(state bytes are opaque to anything between client and server) and
authenticated (tampering or cross-principal replay fails decryption).

The single master ``token_key`` is paired with per-token random nonces;
``(domain, principal)`` is carried in AAD so a token minted for one user
cannot be replayed by another.  Plaintext payload framing lives entirely
inside the ciphertext, including the creation timestamp.

TWO TOKENS, NOT ONE
-------------------
A stream's state divides into a part that is fixed for the life of the call
and a part that advances per turn.  Carrying both in one token means every
continuation re-serializes, re-seals, re-opens and re-parses the fixed part —
which for a typical stream is the overwhelming majority of the payload.

So the two travel separately:

``CALL_STATE_KEY`` — the **call token**.  Minted once by ``/init``, carries
the call state plus the stream's frozen output/input schemas and a random
``call_id``.  The client echoes it on every request; the server never
re-issues it.  Servers keep a :class:`_CallStateCache` so a warm process
skips opening and parsing it altogether.

``STATE_KEY`` — the **cursor token**.  Re-minted every turn, carries only the
advancing ``StreamState`` plus the ``call_id`` it belongs to.  This is the
only token a response returns.

The cursor token is what binds the pair: ``call_id`` lives inside its
ciphertext, so it is authenticated before it is trusted, and a cache lookup
keyed on it cannot be steered by anything the client controls.
"""

from __future__ import annotations

import base64
import os
import struct
import threading
import time
import types as _types
from collections import OrderedDict
from http import HTTPStatus
from typing import get_args, get_origin, get_type_hints

import pyarrow as pa
import zstandard

from vgi_rpc import crypto
from vgi_rpc.rpc import AuthContext, MethodType, RpcServer, Stream, StreamState
from vgi_rpc.utils import (
    COMPACT_MARKER,
    ArrowSerializableDataclass,
    IpcValidation,
    deserialize_compact,
    serialize_compact,
)

from .._common import _RpcHttpError

# The AEAD envelope (version byte, nonce, ciphertext+tag) is handled by
# ``vgi_rpc.crypto``. The constants here describe only the *plaintext*
# payload framing that lives inside the ciphertext.
_HEADER_LEN = 4  # uint32 LE prefix for each plaintext segment
_CURSOR_TOKEN_VERSION = 5  # v5: cursor-only token, call state moved to its own token
_CALL_TOKEN_VERSION = 1  # call token format (independent version line)
_TIMESTAMP_LEN = 8  # uint64 LE, seconds since epoch (inside ciphertext)
_CALL_ID_LEN = 16  # random per-stream id, minted at /init, echoed by the cursor
_MIN_CURSOR_PLAINTEXT_LEN = _TIMESTAMP_LEN + _CALL_ID_LEN + _HEADER_LEN

# Token payloads are compressed *before* sealing. It has to be that way round:
# once sealed, a token is ciphertext, so the HTTP body codec can no longer find
# any redundancy in it — measured, zstd over a sealed token recovers only the
# slack base64 added (to ~76-80%), never the state's own structure. Compressing
# inside the seal reaches the actual redundancy: a 7,800-byte call state packs
# to 1,872, turning a 10,820-byte token into 2,552.
#
# Level 3 for both token kinds. Measured against level 1 it is the same speed
# on these payload sizes and slightly smaller; the levels that compress
# materially better (9, 19) cost 8x and 84x the CPU for a few hundred bytes.
_TOKEN_ZSTD_LEVEL = 3

#: Per-thread zstd codecs for token payloads.
#:
#: The level is fixed at import, so a compressor is reusable — and building
#: one per call is not free next to the compression itself: measured 1.04us
#: to construct-and-compress a token payload against 0.67us reusing the
#: instance, on a path that runs once per stream turn.
#:
#: Per *thread* rather than one shared instance because python-zstandard does
#: not promise that two threads may call into one codec object at the same
#: time, and this runs on a WSGI thread pool. A thread-local costs one dict
#: lookup and removes the question.
_codecs = threading.local()


def _compressor() -> zstandard.ZstdCompressor:
    """Return this thread's token compressor, building it on first use."""
    codec = getattr(_codecs, "compressor", None)
    if codec is None:
        codec = zstandard.ZstdCompressor(level=_TOKEN_ZSTD_LEVEL)
        _codecs.compressor = codec
    return codec


def _decompressor() -> zstandard.ZstdDecompressor:
    """Return this thread's token decompressor, building it on first use."""
    codec = getattr(_codecs, "decompressor", None)
    if codec is None:
        codec = zstandard.ZstdDecompressor()
        _codecs.decompressor = codec
    return codec


# Guard against a decompression bomb. The plaintext is authenticated before we
# ever decompress it, so this is defence against a framework bug rather than an
# attacker — but an unbounded decompress in a request path is not worth having.
_MAX_TOKEN_PLAINTEXT_BYTES = 64 << 20

_CODEC_RAW = b"\x00"
_CODEC_ZSTD = b"\x01"


def _pack_plaintext(plaintext: bytes) -> bytes:
    """Compress a token payload, tagging which codec was used.

    Compression is skipped when it does not pay — small payloads can come
    out larger, and the flag byte means the reader does not have to guess.

    Args:
        plaintext: The framed token payload.

    Returns:
        A one-byte codec tag followed by the (possibly compressed) payload.

    """
    packed = _compressor().compress(plaintext)
    if len(packed) < len(plaintext):
        return _CODEC_ZSTD + packed
    return _CODEC_RAW + plaintext


def _unpack_plaintext(data: bytes) -> bytes:
    """Reverse :func:`_pack_plaintext`.

    Args:
        data: Decrypted token payload, codec tag first.

    Returns:
        The framed token payload.

    Raises:
        _RpcHttpError: If the codec tag is unknown or decompression fails —
            both mean a token this server did not mint, so they surface as
            the same uniform 400 as every other token failure.

    """
    if not data:
        raise _RpcHttpError(RuntimeError("Malformed token payload"), status_code=HTTPStatus.BAD_REQUEST)
    tag, body = data[:1], data[1:]
    if tag == _CODEC_RAW:
        return body
    if tag != _CODEC_ZSTD:
        raise _RpcHttpError(RuntimeError("Malformed token payload"), status_code=HTTPStatus.BAD_REQUEST)

    try:
        return _decompressor().decompress(body, max_output_size=_MAX_TOKEN_PLAINTEXT_BYTES)
    except zstandard.ZstdError as exc:
        raise _RpcHttpError(
            RuntimeError("Malformed token payload"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc


def _compute_call_aad(auth: AuthContext | None) -> bytes:
    r"""Build the AAD that binds a *call* token to its issuing principal.

    Identical in shape to :func:`_compute_aad` but with a distinct
    version-tagged prefix, so a call token and a cursor token are not
    interchangeable even for the same principal: presenting one where the
    other is expected fails the AEAD tag check rather than decoding into a
    payload the reader will misinterpret.

    Args:
        auth: The authentication context for the current request.

    Returns:
        Associated-data bytes for the AEAD seal/open call.

    """
    prefix = b"vgi_rpc.call.v1\x00"
    if auth is None or not auth.authenticated:
        return prefix + b"\x00anonymous"
    domain = (auth.domain or "").encode()
    principal = (auth.principal or "").encode()
    return prefix + b"\x01" + domain + b"\x00" + principal


def _compute_aad(auth: AuthContext | None) -> bytes:
    r"""Build the AAD that binds a state token to its issuing principal.

    Wire format::

        b"vgi_rpc.state.v4\x00" || domain_bytes || b"\x00" || principal_bytes

    For anonymous requests, the identity tail is the literal
    ``b"\x00anonymous"`` — matching the convention used elsewhere in the
    framework.  Including the version-tagged prefix prevents AAD reuse if
    the token format ever changes; the leading prefix is fixed-length and
    therefore prefix-unambiguous with respect to the variable-length
    identity tail.

    Args:
        auth: The authentication context for the current request.

    Returns:
        Associated-data bytes for the AEAD seal/open call.

    """
    prefix = b"vgi_rpc.state.v4\x00"
    if auth is None or not auth.authenticated:
        return prefix + b"\x00anonymous"
    domain = (auth.domain or "").encode()
    principal = (auth.principal or "").encode()
    return prefix + b"\x01" + domain + b"\x00" + principal


# ---------------------------------------------------------------------------
# Call token — minted once per stream, echoed by the client, never re-issued
# ---------------------------------------------------------------------------


def _seal_call_token(
    call_state_bytes: bytes,
    call_state_type: str,
    schema_bytes: bytes,
    input_schema_bytes: bytes,
    call_id: bytes,
    stream_id: str,
    token_key: bytes,
    aad: bytes,
    created_at: int,
) -> bytes:
    """Seal the immutable half of a stream's state into a call token.

    Plaintext (encrypted before transport)::

        [ 8 bytes : created_at  (uint64 LE, seconds since epoch)]
        [16 bytes : call_id     (random, minted at /init)]
        [ 4 bytes : call_len    (uint32 LE)] [call_state_bytes]
        [ 4 bytes : type_len    (uint32 LE)] [call_state_type (UTF-8)]
        [ 4 bytes : schema_len  (uint32 LE)] [schema_bytes]
        [ 4 bytes : input_len   (uint32 LE)] [input_schema_bytes]
        [ 4 bytes : sid_len     (uint32 LE)] [stream_id_bytes (UTF-8)]

    The type name is carried because a stream method may return a *union*
    of state classes whose members declare different call-state types (VGI's
    ``init`` does exactly this: table/scalar states carry a call state,
    finalize states do not).  The reader resolves the name against the set
    the method declares — it never looks up a class by client-supplied name.

    Args:
        call_state_bytes: Serialized call state; empty when the stream
            declares none (the schemas alone still justify the token).
        call_state_type: Class name of the call state, or ``""`` when there
            is none.
        schema_bytes: Serialized output ``pa.Schema``.
        input_schema_bytes: Serialized input ``pa.Schema``.
        call_id: The stream's random 16-byte call id.
        stream_id: Chain-correlation id (hex UUID).
        token_key: 32-byte master AEAD key.
        aad: Associated data from :func:`_compute_call_aad`.
        created_at: Token creation time as seconds since epoch.

    Returns:
        The opaque sealed token, base64-encoded for UTF-8 safe metadata.

    """
    stream_id_bytes = stream_id.encode()
    type_bytes = call_state_type.encode()
    plaintext = (
        struct.pack("<Q", created_at)
        + call_id
        + struct.pack("<I", len(call_state_bytes))
        + call_state_bytes
        + struct.pack("<I", len(type_bytes))
        + type_bytes
        + struct.pack("<I", len(schema_bytes))
        + schema_bytes
        + struct.pack("<I", len(input_schema_bytes))
        + input_schema_bytes
        + struct.pack("<I", len(stream_id_bytes))
        + stream_id_bytes
    )
    sealed = crypto.seal_bytes(_pack_plaintext(plaintext), token_key, aad=aad, version=_CALL_TOKEN_VERSION)
    return base64.b64encode(sealed)


def _open_call_token(
    token: bytes,
    token_key: bytes,
    aad: bytes,
    token_ttl: int = 0,
) -> tuple[bytes, str, bytes, bytes, bytes, str]:
    """Open and verify a call token.

    Args:
        token: The opaque token produced by :func:`_seal_call_token`.
        token_key: 32-byte master AEAD key.
        aad: Associated data — must match the AAD used at seal time.
        token_ttl: Maximum token age in seconds; ``0`` disables expiry.

    Returns:
        ``(call_state_bytes, call_state_type, schema_bytes, input_schema_bytes,
        call_id, stream_id)``

    Raises:
        _RpcHttpError: On malformed, tampered, expired, or cross-principal
            tokens (HTTP 400), uniformly — as with the cursor token, the
            failure modes are deliberately indistinguishable to the caller.

    """
    try:
        raw = base64.b64decode(token, validate=True)
    except Exception as exc:
        raise _RpcHttpError(
            RuntimeError("Malformed call token"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc

    try:
        sealed_plaintext: bytes = crypto.open_bytes(raw, token_key, aad=aad, version=_CALL_TOKEN_VERSION)
    except crypto.SealError as exc:
        raise _RpcHttpError(
            RuntimeError("Call token signature verification failed"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc
    plaintext = _unpack_plaintext(sealed_plaintext)

    if len(plaintext) < _TIMESTAMP_LEN + _CALL_ID_LEN + _HEADER_LEN * 5:
        raise _RpcHttpError(RuntimeError("Malformed call token"), status_code=HTTPStatus.BAD_REQUEST)

    call_id = plaintext[_TIMESTAMP_LEN : _TIMESTAMP_LEN + _CALL_ID_LEN]
    pos = _TIMESTAMP_LEN + _CALL_ID_LEN
    call_state_bytes, pos = _read_segment(plaintext, pos, "Malformed call token")
    type_bytes, pos = _read_segment(plaintext, pos, "Malformed call token")
    schema_bytes, pos = _read_segment(plaintext, pos, "Malformed call token")
    input_schema_bytes, pos = _read_segment(plaintext, pos, "Malformed call token")
    stream_id_bytes, payload_end = _read_segment(plaintext, pos, "Malformed call token")

    if payload_end != len(plaintext):
        raise _RpcHttpError(RuntimeError("Malformed call token"), status_code=HTTPStatus.BAD_REQUEST)

    if token_ttl > 0:
        created_at = struct.unpack_from("<Q", plaintext, 0)[0]
        if int(time.time()) - created_at > token_ttl:
            raise _RpcHttpError(RuntimeError("Call token expired"), status_code=HTTPStatus.BAD_REQUEST)

    return (
        call_state_bytes,
        type_bytes.decode(),
        schema_bytes,
        input_schema_bytes,
        call_id,
        stream_id_bytes.decode(),
    )


def _read_segment(data: bytes, pos: int, message: str) -> tuple[bytes, int]:
    """Read one uint32-LE length-prefixed segment starting at ``pos``."""
    if pos + _HEADER_LEN > len(data):
        raise _RpcHttpError(RuntimeError(message), status_code=HTTPStatus.BAD_REQUEST)
    seg_len = struct.unpack_from("<I", data, pos)[0]
    seg_end = pos + _HEADER_LEN + seg_len
    if seg_end > len(data):
        raise _RpcHttpError(RuntimeError(message), status_code=HTTPStatus.BAD_REQUEST)
    return data[pos + _HEADER_LEN : seg_end], seg_end


class _ResolvedCall:
    """A call token's contents, parsed once and reusable across turns.

    Instances are shared between concurrent requests that present the same
    ``call_id``, so everything reachable from here must be treated as
    immutable.  The schemas are ``pa.Schema`` (immutable in Arrow); the
    call-state object's immutability is the contract
    :meth:`StreamState.bind_call_state` documents.
    """

    __slots__ = ("call_state", "input_schema", "output_schema", "stream_id")

    def __init__(
        self,
        call_state: ArrowSerializableDataclass | None,
        output_schema: pa.Schema,
        input_schema: pa.Schema,
        stream_id: str,
    ) -> None:
        self.call_state = call_state
        self.output_schema = output_schema
        self.input_schema = input_schema
        self.stream_id = stream_id


class _CallStateCache:
    """Bounded, thread-safe LRU of ``call_id`` → :class:`_ResolvedCall`.

    A pure accelerator: a miss (cold process, evicted entry, request landing
    on a different node) falls back to opening the call token the client
    supplied, so statelessness is preserved and no request depends on a
    prior request having warmed anything.

    The key is the ``call_id`` recovered from *inside* the cursor token's
    ciphertext, paired with the caller's identity.  Both parts are
    authenticated before the lookup happens: the cursor token's AEAD tag
    covers the call id, and its AAD covers the principal.  A client
    therefore cannot steer a lookup toward another principal's entry, and
    cannot present a call id the server did not mint.
    """

    __slots__ = ("_entries", "_lock", "_max_entries", "_ttl")

    def __init__(self, max_entries: int = 4096, ttl: float = 3600.0) -> None:
        self._entries: OrderedDict[tuple[bytes, str], tuple[float, _ResolvedCall]] = OrderedDict()
        self._lock = threading.Lock()
        self._max_entries = max_entries
        self._ttl = ttl

    @staticmethod
    def _identity(auth: AuthContext | None) -> str:
        if auth is None or not auth.authenticated:
            return "\0anonymous"
        return f"{auth.domain or ''}\0{auth.principal or ''}"

    def get(self, call_id: bytes, auth: AuthContext | None, now: float) -> _ResolvedCall | None:
        """Return the cached call for ``call_id``, or ``None`` on miss/expiry."""
        key = (call_id, self._identity(auth))
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            expires_at, resolved = entry
            if expires_at <= now:
                del self._entries[key]
                return None
            self._entries.move_to_end(key)
            return resolved

    def put(self, call_id: bytes, auth: AuthContext | None, resolved: _ResolvedCall, now: float) -> None:
        """Record ``resolved`` under ``call_id``, evicting the oldest if full."""
        key = (call_id, self._identity(auth))
        with self._lock:
            self._entries[key] = (now + self._ttl, resolved)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    def clear(self) -> None:
        """Drop every entry (tests, and explicit operator reset)."""
        with self._lock:
            self._entries.clear()


def _mint_call_token(
    call_state: ArrowSerializableDataclass | None,
    output_schema: pa.Schema,
    input_schema: pa.Schema,
    token_key: bytes,
    auth: AuthContext | None,
    stream_id: str,
    *,
    now: int | None = None,
) -> tuple[bytes, bytes, bytes]:
    """Serialize and seal a stream's call token.  Called once, by ``/init``.

    Args:
        call_state: The stream's call state, or ``None``.
        output_schema: Per-stream output schema (frozen at init).
        input_schema: Per-stream input schema (frozen at init).
        token_key: Master AEAD key from the server config.
        auth: Authenticated identity for AAD binding.
        stream_id: Chain-correlation id.
        now: Override for the baked-in timestamp; default ``time.time()``.

    Returns:
        ``(token, call_id, call_state_bytes)``.  ``call_id`` must be threaded
        into every cursor token minted for this stream; ``call_state_bytes``
        is surfaced to the access log alongside the cursor state.

    """
    call_id = os.urandom(_CALL_ID_LEN)
    call_state_bytes = b"" if call_state is None else call_state.serialize_to_bytes()
    token = _seal_call_token(
        call_state_bytes,
        "" if call_state is None else type(call_state).__name__,
        output_schema.serialize().to_pybytes(),
        input_schema.serialize().to_pybytes(),
        call_id,
        stream_id,
        token_key,
        _compute_call_aad(auth),
        int(time.time()) if now is None else now,
    )
    return token, call_id, call_state_bytes


# Type alias: a single concrete class or an ordered tuple for unions.
_StateInfo = type[StreamState] | tuple[type[StreamState], ...]

# Arrow IPC streams always start with 0xFF (continuation indicator).
# We use 0x00 as a discriminator byte for union-tagged state envelopes.
_UNION_STATE_MARKER = b"\x00"


def _seal_cursor_token(
    state_bytes: bytes,
    call_id: bytes,
    token_key: bytes,
    aad: bytes,
    created_at: int,
) -> bytes:
    """Seal the advancing half of a stream's state into a cursor token.

    Plaintext (encrypted before transport)::

        [ 8 bytes : created_at (uint64 LE, seconds since epoch)]
        [16 bytes : call_id    (the call token this cursor belongs to)]
        [ 4 bytes : state_len  (uint32 LE)] [state_bytes]

    Everything the v4 token carried besides the state — both schemas and the
    stream id — has moved to the call token, since none of it can change
    while the stream lives.  What remains is small enough that the AEAD's
    fixed cost dominates.

    Args:
        state_bytes: Serialized cursor state (Arrow IPC or tagged union
            envelope).
        call_id: The call id this cursor is bound to.
        token_key: 32-byte master AEAD key.
        aad: Associated data binding the token to its principal.
        created_at: Token creation time as seconds since epoch.

    Returns:
        The opaque sealed token, base64-encoded for UTF-8 safe metadata.

    """
    plaintext = struct.pack("<Q", created_at) + call_id + struct.pack("<I", len(state_bytes)) + state_bytes
    sealed = crypto.seal_bytes(_pack_plaintext(plaintext), token_key, aad=aad, version=_CURSOR_TOKEN_VERSION)
    return base64.b64encode(sealed)


def _open_cursor_token(
    token: bytes,
    token_key: bytes,
    aad: bytes,
    token_ttl: int = 0,
) -> tuple[bytes, bytes]:
    """Open and verify a cursor token.

    Args:
        token: The opaque token produced by :func:`_seal_cursor_token`.
        token_key: 32-byte master AEAD key.
        aad: Associated data — must match the AAD used at seal time.
        token_ttl: Maximum token age in seconds; ``0`` disables expiry.

    Returns:
        ``(state_bytes, call_id)``.  The call id is authenticated by the
        AEAD tag, which is what makes it safe to use as a cache key.

    Raises:
        _RpcHttpError: On malformed, tampered, expired, or cross-principal
            tokens (HTTP 400).

    """
    try:
        raw = base64.b64decode(token, validate=True)
    except Exception as exc:
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc

    try:
        sealed_plaintext: bytes = crypto.open_bytes(raw, token_key, aad=aad, version=_CURSOR_TOKEN_VERSION)
    except crypto.SealError as exc:
        raise _RpcHttpError(
            RuntimeError("State token signature verification failed"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc
    plaintext = _unpack_plaintext(sealed_plaintext)

    if len(plaintext) < _MIN_CURSOR_PLAINTEXT_LEN:
        raise _RpcHttpError(RuntimeError("Malformed state token"), status_code=HTTPStatus.BAD_REQUEST)

    call_id = plaintext[_TIMESTAMP_LEN : _TIMESTAMP_LEN + _CALL_ID_LEN]
    state_bytes, payload_end = _read_segment(plaintext, _TIMESTAMP_LEN + _CALL_ID_LEN, "Malformed state token")
    if payload_end != len(plaintext):
        raise _RpcHttpError(RuntimeError("Malformed state token"), status_code=HTTPStatus.BAD_REQUEST)

    if token_ttl > 0:
        created_at = struct.unpack_from("<Q", plaintext, 0)[0]
        if int(time.time()) - created_at > token_ttl:
            raise _RpcHttpError(RuntimeError("State token expired"), status_code=HTTPStatus.BAD_REQUEST)

    return state_bytes, call_id


def _mint_cursor_token(
    state: StreamState,
    state_info: _StateInfo,
    call_id: bytes,
    token_key: bytes,
    auth: AuthContext | None,
    *,
    now: int | None = None,
) -> tuple[bytes, bytes]:
    """Serialize the cursor state and seal it into a continuation token.

    This is the whole per-turn token cost under the split: one small
    serialize and one small seal.  Both schemas and the call state stay
    where ``/init`` put them.

    Args:
        state: The current ``StreamState`` instance (cursor only).
        state_info: Concrete state class or union tuple, used by
            :func:`_serialize_state_bytes` to pick the wire format.
        call_id: The call id this stream's cursor is bound to.
        token_key: Master AEAD key from the server config.
        auth: Authenticated identity for AAD binding.
        now: Override for the baked-in timestamp; default ``time.time()``.

    Returns:
        ``(token, state_bytes)`` — the sealed token for the
        ``vgi_rpc.stream_state#b64`` metadata key, and the raw plaintext
        state bytes for the access log's ``response_state`` field.

    """
    state_bytes = _serialize_state_bytes(state, state_info)
    token = _seal_cursor_token(
        state_bytes,
        call_id,
        token_key,
        _compute_aad(auth),
        int(time.time()) if now is None else now,
    )
    return token, state_bytes


def _serialize_state_bytes(state: StreamState, state_info: _StateInfo) -> bytes:
    r"""Serialize state bytes for state token payload.

    Cursor states are records, not tables: a counter, an offset, an opaque
    blob. Arrow IPC charges a schema message, a batch message, an
    end-of-stream marker and alignment padding for every one of them, which
    for a two-int state measured 416 bytes and 36us against 16 bytes and
    0.21us for the same integers packed directly. So a flat state takes the
    compact codec and anything Arrow is genuinely needed for -- a state
    holding a ``RecordBatch``, say -- keeps the Arrow path.

    Which encoding was used is recoverable from the first byte, so the reader
    needs no side channel: ``\x01`` compact, ``\xff`` Arrow IPC.

    Single-state methods store those bytes directly.
    Union-state methods store: ``\x00`` + uint16-LE tag + those bytes.
    """
    state_bytes = serialize_compact(state)
    if state_bytes is None:
        state_bytes = state.serialize_to_bytes()
    if isinstance(state_info, tuple):
        try:
            tag = state_info.index(type(state))
        except ValueError as exc:
            msg = (
                f"State type {type(state).__name__!r} is not valid for union method; "
                f"expected one of {[t.__name__ for t in state_info]}"
            )
            raise RuntimeError(msg) from exc
        return _UNION_STATE_MARKER + struct.pack("<H", tag) + state_bytes
    return state_bytes


def _deserialize_state_bytes(
    state_cls: type[StreamState],
    raw: bytes,
    ipc_validation: IpcValidation,
) -> StreamState:
    """Rebuild a state object, dispatching on how it was encoded.

    Args:
        state_cls: The concrete state class, already resolved.
        raw: The state payload, encoding marker included.
        ipc_validation: Validation level for the Arrow path.

    Returns:
        The deserialized state.

    """
    if raw[:1] == COMPACT_MARKER:
        state: StreamState = deserialize_compact(state_cls, raw)
        return state
    return state_cls.deserialize_from_bytes(raw, ipc_validation)


def _resolve_state_cls(
    data: bytes,
    state_info: _StateInfo,
) -> tuple[type[StreamState], bytes]:
    """Resolve the concrete state class from token state bytes.

    Args:
        data: Raw token state bytes.
        state_info: Metadata describing the stream's state class.

    Returns:
        ``(state_cls, raw_state_bytes)``

    """
    if isinstance(state_info, tuple):
        if data[:1] != _UNION_STATE_MARKER or len(data) < 3:
            msg = "Cannot deserialize union state from untagged token"
            raise RuntimeError(msg)
        tag = struct.unpack("<H", data[1:3])[0]
        if tag >= len(state_info):
            msg = f"Unknown union state tag {tag}; expected 0..{len(state_info) - 1}"
            raise RuntimeError(msg)
        return state_info[tag], data[3:]
    return state_info, data


def _resolve_state_types(
    server: RpcServer,
) -> dict[str, _StateInfo]:
    """Introspect server implementation to map method names to concrete state types.

    Examines the return type hints of each stream method on the
    implementation (not the protocol) to extract the concrete
    ``StreamState`` subclass.

    For union return types (``Stream[A | B, ...]``), stores an
    ordered tuple of classes so token state can carry a compact
    numeric tag instead of class names.

    Args:
        server: The ``RpcServer`` whose implementation to introspect.

    Returns:
        Mapping of method name to state info (single class or union dict).

    """
    result: dict[str, _StateInfo] = {}
    for name, info in server.methods.items():
        if info.method_type != MethodType.STREAM:
            continue
        impl_method = getattr(server.implementation, name, None)
        if impl_method is None:
            continue
        try:
            hints = get_type_hints(impl_method)
        except (NameError, AttributeError) as exc:
            msg = f"Cannot resolve type hints for stream method {name!r}: {exc}"
            raise TypeError(msg) from exc
        return_hint = hints.get("return")
        if return_hint is None:
            continue
        origin = get_origin(return_hint)
        if origin is Stream:
            args = get_args(return_hint)
            if not args:
                continue
            state_arg = args[0]
            if isinstance(state_arg, type) and issubclass(state_arg, StreamState):
                result[name] = state_arg
            elif isinstance(state_arg, _types.UnionType):
                members = tuple(t for t in get_args(state_arg) if isinstance(t, type) and issubclass(t, StreamState))
                if len(members) == 1:
                    result[name] = members[0]
                elif members:
                    result[name] = members
    return result
