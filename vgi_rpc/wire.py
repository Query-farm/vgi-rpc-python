"""Public wire-framing helpers for VGI-RPC intermediaries.

vgi-rpc has two first-class roles: *client* (:func:`vgi_rpc.connect`) and
*server* (:class:`vgi_rpc.RpcServer` / :func:`vgi_rpc.run_server`). A third role —
an **intermediary** (proxy, router, gateway, test harness) — needs to read a
request off the wire, rewrite it, re-frame it for forwarding, and synthesize
in-band error responses, without standing up a full client or server.

This module is the stable public surface for that role, so intermediaries don't
reach into the internal ``vgi_rpc.rpc._wire`` codec (private, and free to change).
"""

from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.ipc as ipc

if TYPE_CHECKING:
    from vgi_rpc.external import ServerExternalConfig

__all__ = [
    "build_error_stream",
    "find_protocol_version",
    "find_state_token",
    "read_request",
    "read_unary_result",
    "write_request",
    "write_unary_result",
]


def read_request(
    data: bytes,
    *,
    external_config: ServerExternalConfig | None = None,
) -> tuple[str, dict[str, Any]]:
    """Parse a request IPC body into ``(method_name, kwargs)``.

    Args:
        data: The complete request IPC stream bytes.
        external_config: Resolves externalized (``vgi_rpc.location``) pointer
            requests by fetching the referenced bytes; ``None`` disables
            resolution (a pointer request then fails to parse — callers that
            require resolution should treat that as a fail-closed denial).

    Returns:
        The dispatched method name and its keyword-argument mapping.

    """
    from vgi_rpc.rpc._wire import _read_request

    method_name, kwargs = _read_request(BytesIO(data), external_config=external_config)
    return method_name, kwargs


def write_request(
    method_name: str,
    params_schema: pa.Schema,
    kwargs: dict[str, Any],
    *,
    protocol_version: str | None = None,
) -> bytes:
    """Frame a request as a complete IPC stream body for forwarding.

    Args:
        method_name: The RPC method name (e.g. ``"bind"``).
        params_schema: The method's parameter schema.
        kwargs: The parameter values, keyed by field name.
        protocol_version: The application ``protocol_version`` to stamp on the
            request, so a versioned server's dispatch-boundary check still sees
            the originating client's version. Leave ``None`` to emit a request
            that is structurally exempt from that check (the codec omits the key).

    Returns:
        The framed request IPC stream bytes.

    """
    from vgi_rpc.rpc._wire import _write_request

    buf = BytesIO()
    _write_request(buf, method_name, params_schema, kwargs, protocol_version=protocol_version)
    return buf.getvalue()


def build_error_stream(
    exc: BaseException,
    *,
    schema: pa.Schema | None = None,
    server_id: str | None = None,
) -> bytes:
    """Build a complete IPC stream carrying a single error batch.

    This is the wire shape an intermediary returns to deny or abort a call
    in-band — the client decodes it back into a raised exception.

    Args:
        exc: The exception to encode; its type and message reach the client.
        schema: The stream schema; defaults to an empty schema.
        server_id: Optional server id to stamp on the error batch.

    Returns:
        The error IPC stream bytes.

    """
    from vgi_rpc.rpc._wire import _write_error_stream

    buf = BytesIO()
    _write_error_stream(buf, schema if schema is not None else pa.schema([]), exc, server_id=server_id)
    return buf.getvalue()


def find_state_token(data: bytes) -> bytes | None:
    """Return the stream-state continuation token carried in a request/response body.

    The token (key :data:`vgi_rpc.metadata.STATE_KEY`) rides in a record batch's
    ``custom_metadata`` — not a header. Stream continuations recover their state
    from this token, never from headers, so an intermediary that wants to route
    or correlate a stream by it must read the batch metadata.

    Two body shapes are handled by one walk:

    * an **exchange request** is a single IPC stream whose first batch carries
      the token;
    * a **producer init/exchange response** may be *several concatenated IPC
      streams* (a header stream followed by the producer's data stream — the
      header lives in its own stream with a different schema), so the token can
      be in a later stream.

    Returns the first token found across all concatenated streams (so the
    single-stream request case returns after its first batch), or ``None`` if
    absent or the body is unparseable.

    Note:
        For a response that rotates the token across multiple data batches, the
        *last* token is the continuation the peer will send next; this returns
        the first. Single-token responses (the common case) make them identical.

    Args:
        data: The (decompressed) request or response IPC body bytes.

    Returns:
        The state token bytes, or ``None``.

    """
    from vgi_rpc.metadata import STATE_KEY

    try:
        buf = BytesIO(data)
        total = len(data)
        while buf.tell() < total:
            start = buf.tell()
            try:
                reader = ipc.open_stream(buf)
                while True:
                    try:
                        rb = reader.read_next_batch_with_custom_metadata()
                    except StopIteration:
                        break
                    md = rb.custom_metadata
                    if md is not None:
                        token = md.get(STATE_KEY)
                        if token:
                            return token
            except Exception:
                break
            if buf.tell() == start:  # no forward progress — avoid an infinite loop
                break
    except Exception:
        return None
    return None


def find_protocol_version(data: bytes) -> str | None:
    """Return the application ``protocol_version`` stamped on a request body.

    Scans the request batch's ``custom_metadata`` for
    :data:`vgi_rpc.metadata.PROTOCOL_VERSION_KEY`. An intermediary that rewrites
    a request must recover and re-stamp this (see ``write_request``) so the
    backend's dispatch-boundary version check still sees the originating client's
    version. Returns ``None`` when absent or unparseable — a request that never
    carried one is structurally exempt from the check.

    Args:
        data: The (decompressed) request IPC body bytes.

    Returns:
        The protocol-version string, or ``None``.

    """
    from vgi_rpc.metadata import PROTOCOL_VERSION_KEY

    try:
        reader = ipc.open_stream(BytesIO(data))
        while True:
            try:
                rb = reader.read_next_batch_with_custom_metadata()
            except StopIteration:
                break
            md = rb.custom_metadata
            if md is not None:
                version = md.get(PROTOCOL_VERSION_KEY)
                if version:
                    return version.decode()
    except Exception:
        return None
    return None


def read_unary_result(data: bytes) -> tuple[pa.Schema, bytes] | None:
    """Unwrap a unary-RPC response to ``(envelope_schema, raw_result_bytes)``.

    A unary response is an IPC stream of zero or more leading **log batches**
    (0-row, carrying :data:`vgi_rpc.metadata.LOG_LEVEL_KEY` in schema metadata)
    followed by one data batch whose ``result`` column holds the serialized
    response object. This returns the envelope schema plus the **raw** result
    bytes — no typed decode — for an intermediary that inspects or rewrites the
    response and re-wraps it via :func:`write_unary_result`.

    Lenient: returns ``None`` for an error/empty/non-``result`` stream, so the
    caller can forward it unchanged.

    Args:
        data: The (resolved, decompressed) response IPC body bytes.

    Returns:
        ``(envelope_schema, result_bytes)``, or ``None``.

    """
    from vgi_rpc.metadata import LOG_LEVEL_KEY

    reader = ipc.open_stream(BytesIO(data))
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            return None
        if batch.num_rows > 0:
            if "result" not in batch.schema.names:
                return None
            idx = batch.schema.get_field_index("result")
            return batch.schema, batch.column(idx)[0].as_py()
        md = batch.schema.metadata
        if md and LOG_LEVEL_KEY in md:
            continue
        return None


def write_unary_result(envelope_schema: pa.Schema, result_bytes: bytes) -> bytes:
    """Build a unary-RPC response IPC stream wrapping ``result_bytes``.

    The inverse of :func:`read_unary_result`: emit a single data batch whose
    ``result`` column carries ``result_bytes`` under ``envelope_schema``.

    Args:
        envelope_schema: The response envelope schema (a single ``result`` field).
        result_bytes: The serialized response object.

    Returns:
        The response IPC stream bytes.

    """
    batch = pa.record_batch([pa.array([result_bytes], type=pa.binary())], schema=envelope_schema)
    buf = BytesIO()
    with ipc.new_stream(buf, envelope_schema) as writer:
        writer.write_batch(batch)
    return buf.getvalue()
