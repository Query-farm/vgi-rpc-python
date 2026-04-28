# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Signed state token helpers for the HTTP streaming protocol.

Streaming is stateless on the wire: each exchange carries the serialized
``StreamState`` inside an HMAC-signed token in Arrow custom metadata.
This module handles packing, unpacking, and resolving the concrete
``StreamState`` subclass from server introspection.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import struct
import time
import types as _types
from http import HTTPStatus
from typing import get_args, get_origin, get_type_hints

import pyarrow as pa

from vgi_rpc.rpc import AuthContext, MethodType, RpcServer, Stream, StreamState

from .._common import _RpcHttpError


def _derive_signing_key(signing_key: bytes, auth: AuthContext | None) -> bytes:
    """Derive a per-principal signing key for stream state tokens.

    Binds the effective HMAC key to the authenticated identity so a state
    token issued to one user cannot be replayed by another user who has
    obtained a copy of the opaque token. Unauthenticated requests share a
    common ``anonymous`` derivation — there is no identity to cross, so
    this preserves the pre-existing behavior for the pipe/anonymous path.

    Args:
        signing_key: The master HMAC key configured on the server.
        auth: The authentication context for the current request.

    Returns:
        A 32-byte derived key specific to ``(domain, principal)``.

    """
    if auth is None or not auth.authenticated:
        identity = b"\x00anonymous"
    else:
        domain = (auth.domain or "").encode()
        principal = (auth.principal or "").encode()
        identity = b"\x01" + domain + b"\x00" + principal
    return hmac.new(signing_key, b"vgi_rpc.stream_state:" + identity, hashlib.sha256).digest()


_HMAC_LEN = 32  # SHA-256 digest size
_HEADER_LEN = 4  # uint32 LE prefix for each segment
_TOKEN_VERSION = 3  # bump when the token wire format changes (v3: added stream_id)
_TOKEN_VERSION_LEN = 1  # single byte
_TIMESTAMP_LEN = 8  # uint64 LE, seconds since epoch
_MIN_TOKEN_LEN = _TOKEN_VERSION_LEN + _TIMESTAMP_LEN + _HEADER_LEN * 4 + _HMAC_LEN


def _pack_state_token(
    state_bytes: bytes,
    schema_bytes: bytes,
    input_schema_bytes: bytes,
    signing_key: bytes,
    created_at: int,
    stream_id: str = "",
) -> bytes:
    """Pack state, output schema, input schema, and stream_id into a signed token.

    Wire format (v3)::

        [1 byte:  version=3          (uint8)]
        [8 bytes: created_at         (uint64 LE, seconds since epoch)]
        [4 bytes: state_len          (uint32 LE)]
        [state_len bytes: state_bytes]
        [4 bytes: schema_len         (uint32 LE)]
        [schema_len bytes: schema_bytes]
        [4 bytes: input_schema_len   (uint32 LE)]
        [input_schema_len bytes: input_schema_bytes]
        [4 bytes: stream_id_len      (uint32 LE)]
        [stream_id_len bytes: stream_id_bytes (UTF-8)]
        [32 bytes: HMAC-SHA256(key, all above)]

    Args:
        state_bytes: Serialized state (Arrow IPC).
        schema_bytes: Serialized output ``pa.Schema``.
        input_schema_bytes: Serialized input ``pa.Schema``.
        signing_key: HMAC signing key.
        created_at: Token creation time as seconds since epoch.
        stream_id: Stream correlation ID (hex UUID).

    Returns:
        The opaque signed token, base64-encoded for UTF-8 safe metadata.

    """
    stream_id_bytes = stream_id.encode()
    payload = (
        struct.pack("B", _TOKEN_VERSION)
        + struct.pack("<Q", created_at)
        + struct.pack("<I", len(state_bytes))
        + state_bytes
        + struct.pack("<I", len(schema_bytes))
        + schema_bytes
        + struct.pack("<I", len(input_schema_bytes))
        + input_schema_bytes
        + struct.pack("<I", len(stream_id_bytes))
        + stream_id_bytes
    )
    mac = hmac.new(signing_key, payload, hashlib.sha256).digest()
    return base64.b64encode(payload + mac)


def _unpack_state_token(token: bytes, signing_key: bytes, token_ttl: int = 0) -> tuple[bytes, bytes, bytes, str]:
    """Unpack and verify a signed state token.

    Args:
        token: The opaque token produced by ``_pack_state_token``.
        signing_key: HMAC signing key (must match the one used to pack).
        token_ttl: Maximum token age in seconds.  ``0`` disables expiry
            checking.

    Returns:
        ``(state_bytes, schema_bytes, input_schema_bytes, stream_id)``

    Raises:
        _RpcHttpError: On malformed, tampered, or expired tokens (HTTP 400).

    """
    try:
        token = base64.b64decode(token, validate=True)
    except Exception as exc:
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc

    if len(token) < _MIN_TOKEN_LEN:
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    def _read_segment(data: bytes, pos: int) -> tuple[bytes, int]:
        if pos + _HEADER_LEN > len(data):
            raise _RpcHttpError(RuntimeError("Malformed state token"), status_code=HTTPStatus.BAD_REQUEST)
        seg_len = struct.unpack_from("<I", data, pos)[0]
        seg_end = pos + _HEADER_LEN + seg_len
        if seg_end > len(data):
            raise _RpcHttpError(RuntimeError("Malformed state token"), status_code=HTTPStatus.BAD_REQUEST)
        return data[pos + _HEADER_LEN : seg_end], seg_end

    segments_start = _TOKEN_VERSION_LEN + _TIMESTAMP_LEN
    state_bytes, pos = _read_segment(token, segments_start)
    schema_bytes, pos = _read_segment(token, pos)
    input_schema_bytes, pos = _read_segment(token, pos)
    stream_id_bytes, payload_end = _read_segment(token, pos)

    if payload_end + _HMAC_LEN != len(token):
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    # Verify HMAC before inspecting any payload fields (including version)
    # to avoid leaking information about the token format to unauthenticated callers.
    payload = token[:payload_end]
    received_mac = token[payload_end:]
    expected_mac = hmac.new(signing_key, payload, hashlib.sha256).digest()
    if not hmac.compare_digest(received_mac, expected_mac):
        raise _RpcHttpError(
            RuntimeError("State token signature verification failed"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    version = token[0]
    if version != _TOKEN_VERSION:
        raise _RpcHttpError(
            RuntimeError(f"Unsupported state token version {version} (expected {_TOKEN_VERSION})"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    # Enforce token TTL when configured
    if token_ttl > 0:
        created_at = struct.unpack_from("<Q", token, _TOKEN_VERSION_LEN)[0]
        if int(time.time()) - created_at > token_ttl:
            raise _RpcHttpError(
                RuntimeError("State token expired"),
                status_code=HTTPStatus.BAD_REQUEST,
            )

    return state_bytes, schema_bytes, input_schema_bytes, stream_id_bytes.decode()


# Type alias: a single concrete class or an ordered tuple for unions.
_StateInfo = type[StreamState] | tuple[type[StreamState], ...]

# Arrow IPC streams always start with 0xFF (continuation indicator).
# We use 0x00 as a discriminator byte for union-tagged state envelopes.
_UNION_STATE_MARKER = b"\x00"


def _mint_continuation_token(
    state: StreamState,
    state_info: _StateInfo,
    output_schema: pa.Schema,
    input_schema: pa.Schema,
    signing_key: bytes,
    auth: AuthContext | None,
    stream_id: str,
    *,
    now: int | None = None,
) -> tuple[bytes, bytes]:
    """Serialize state + schemas + stream_id into a signed continuation token.

    Centralises the per-turn token packaging shared by:

    - ``_run_stream_init_sync`` (exchange-stream init: mint the first token).
    - ``_run_http_producer_turn`` (producer continuation: mint the next token
      when the wire body cap is reached).
    - ``_run_http_exchange_turn`` (exchange continuation: mint a refreshed
      token after each ``state.process()`` call).

    Args:
        state: The current ``StreamState`` instance.
        state_info: Concrete state class or union tuple, used by
            :func:`_serialize_state_bytes` to pick the wire format.
        output_schema: Per-stream output schema (frozen at init).
        input_schema: Per-stream input schema (frozen at init).
        signing_key: Master HMAC key from the server config.
        auth: Authenticated identity for per-principal key derivation.
        stream_id: Chain-correlation id; threaded through every turn so
            access-log and tracing observers can correlate continuations.
        now: Override for the timestamp baked into the token; default is
            ``int(time.time())``.  Useful for tests that need a fixed
            reference point.

    Returns:
        ``(token, state_bytes)`` — the signed token (suitable for the
        ``vgi_rpc.stream_state#b64`` metadata key) and the raw state
        bytes (passed separately to the access log's ``response_state``
        field; the token itself is opaque/HMAC-protected).

    """
    state_bytes = _serialize_state_bytes(state, state_info)
    schema_bytes = output_schema.serialize().to_pybytes()
    input_schema_bytes = input_schema.serialize().to_pybytes()
    token = _pack_state_token(
        state_bytes,
        schema_bytes,
        input_schema_bytes,
        _derive_signing_key(signing_key, auth),
        int(time.time()) if now is None else now,
        stream_id=stream_id,
    )
    return token, state_bytes


def _serialize_state_bytes(state: StreamState, state_info: _StateInfo) -> bytes:
    r"""Serialize state bytes for state token payload.

    Single-state methods store raw serialized state bytes.
    Union-state methods store: ``\x00`` + uint16-LE tag + raw bytes.
    """
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


def _resolve_state_cls(
    data: bytes,
    state_info: _StateInfo,
) -> tuple[type[StreamState], bytes]:
    """Resolve the concrete state class from token state bytes.

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
