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

if TYPE_CHECKING:
    from vgi_rpc.external import ServerExternalConfig

__all__ = ["build_error_stream", "read_request", "write_request"]


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
