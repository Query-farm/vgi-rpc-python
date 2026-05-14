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
"""

from __future__ import annotations

import base64
import struct
import time
import types as _types
from http import HTTPStatus
from typing import get_args, get_origin, get_type_hints

import pyarrow as pa

from vgi_rpc import crypto
from vgi_rpc.rpc import AuthContext, MethodType, RpcServer, Stream, StreamState

from .._common import _RpcHttpError

# The AEAD envelope (version byte, nonce, ciphertext+tag) is handled by
# ``vgi_rpc.crypto``. The constants here describe only the *plaintext*
# payload framing that lives inside the ciphertext.
_HEADER_LEN = 4  # uint32 LE prefix for each plaintext segment
_TOKEN_VERSION = 4  # bump when the token wire format changes (v4: AEAD encrypted)
_TIMESTAMP_LEN = 8  # uint64 LE, seconds since epoch (inside ciphertext)
_MIN_PLAINTEXT_LEN = _TIMESTAMP_LEN + _HEADER_LEN * 4


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


def _seal_state_token(
    state_bytes: bytes,
    schema_bytes: bytes,
    input_schema_bytes: bytes,
    token_key: bytes,
    aad: bytes,
    created_at: int,
    stream_id: str = "",
) -> bytes:
    """Seal state, schemas, and stream_id into an AEAD-encrypted token.

    Wire format (v4)::

        version=4 (1B) || nonce (24B) || ciphertext+tag

    Plaintext (encrypted before transport)::

        [8 bytes : created_at  (uint64 LE, seconds since epoch)]
        [4 bytes : state_len   (uint32 LE)] [state_bytes]
        [4 bytes : schema_len  (uint32 LE)] [schema_bytes]
        [4 bytes : input_len   (uint32 LE)] [input_schema_bytes]
        [4 bytes : sid_len     (uint32 LE)] [stream_id_bytes  (UTF-8)]

    The token's ``version`` byte is not authenticated as AAD — it acts
    purely as a format selector; a tampered version byte still fails
    decryption because we use the matching algorithm constant on the
    decrypt path.

    Args:
        state_bytes: Serialized state (Arrow IPC or tagged union envelope).
        schema_bytes: Serialized output ``pa.Schema``.
        input_schema_bytes: Serialized input ``pa.Schema``.
        token_key: 32-byte master AEAD key.
        aad: Associated data binding the token to its principal.
        created_at: Token creation time as seconds since epoch.
        stream_id: Stream correlation ID (hex UUID), empty for legacy paths.

    Returns:
        The opaque sealed token, base64-encoded for UTF-8 safe metadata.

    """
    stream_id_bytes = stream_id.encode()
    plaintext = (
        struct.pack("<Q", created_at)
        + struct.pack("<I", len(state_bytes))
        + state_bytes
        + struct.pack("<I", len(schema_bytes))
        + schema_bytes
        + struct.pack("<I", len(input_schema_bytes))
        + input_schema_bytes
        + struct.pack("<I", len(stream_id_bytes))
        + stream_id_bytes
    )
    sealed = crypto.seal_bytes(plaintext, token_key, aad=aad, version=_TOKEN_VERSION)
    return base64.b64encode(sealed)


def _open_state_token(
    token: bytes,
    token_key: bytes,
    aad: bytes,
    token_ttl: int = 0,
) -> tuple[bytes, bytes, bytes, str]:
    """Open and verify an AEAD state token.

    Args:
        token: The opaque token produced by ``_seal_state_token``.
        token_key: 32-byte master AEAD key (must match the one used to seal).
        aad: Associated data — must match the AAD used at seal time.
        token_ttl: Maximum token age in seconds.  ``0`` disables expiry
            checking.

    Returns:
        ``(state_bytes, schema_bytes, input_schema_bytes, stream_id)``

    Raises:
        _RpcHttpError: On malformed, tampered, expired, or cross-principal
            tokens (HTTP 400).  The underlying ``crypto.SealError`` is caught
            and re-raised so we do not leak crypto exception types to the
            wire.

    """
    try:
        raw = base64.b64decode(token, validate=True)
    except Exception as exc:
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc

    try:
        plaintext: bytes = crypto.open_bytes(raw, token_key, aad=aad, version=_TOKEN_VERSION)
    except crypto.SealError as exc:
        # Map every failure mode — malformed, wrong version, bad tag, wrong
        # key, wrong AAD (cross-principal replay) — to a single uniform 400
        # so callers cannot distinguish them via timing or message.
        raise _RpcHttpError(
            RuntimeError("State token signature verification failed"),
            status_code=HTTPStatus.BAD_REQUEST,
        ) from exc

    if len(plaintext) < _MIN_PLAINTEXT_LEN:
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

    state_bytes, pos = _read_segment(plaintext, _TIMESTAMP_LEN)
    schema_bytes, pos = _read_segment(plaintext, pos)
    input_schema_bytes, pos = _read_segment(plaintext, pos)
    stream_id_bytes, payload_end = _read_segment(plaintext, pos)

    if payload_end != len(plaintext):
        raise _RpcHttpError(
            RuntimeError("Malformed state token"),
            status_code=HTTPStatus.BAD_REQUEST,
        )

    # TTL check runs after authenticity is established — the timestamp is
    # inside the ciphertext so it cannot be tampered with independently.
    if token_ttl > 0:
        created_at = struct.unpack_from("<Q", plaintext, 0)[0]
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
    token_key: bytes,
    auth: AuthContext | None,
    stream_id: str,
    *,
    now: int | None = None,
) -> tuple[bytes, bytes]:
    """Serialize state + schemas + stream_id into an encrypted continuation token.

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
        token_key: Master AEAD key from the server config.
        auth: Authenticated identity for AAD binding.
        stream_id: Chain-correlation id; threaded through every turn so
            access-log and tracing observers can correlate continuations.
        now: Override for the timestamp baked into the token; default is
            ``int(time.time())``.  Useful for tests that need a fixed
            reference point.

    Returns:
        ``(token, state_bytes)`` — the sealed token (suitable for the
        ``vgi_rpc.stream_state#b64`` metadata key) and the raw plaintext
        state bytes (passed separately to the access log's
        ``response_state`` field so the on-disk record stores decrypted
        state regardless of how the wire envelope evolves).

    """
    state_bytes = _serialize_state_bytes(state, state_info)
    schema_bytes = output_schema.serialize().to_pybytes()
    input_schema_bytes = input_schema.serialize().to_pybytes()
    token = _seal_state_token(
        state_bytes,
        schema_bytes,
        input_schema_bytes,
        token_key,
        _compute_aad(auth),
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
