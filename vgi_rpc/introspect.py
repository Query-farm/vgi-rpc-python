# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Introspection support for vgi-rpc servers.

Provides a built-in ``__describe__`` RPC method that returns machine-readable
metadata about all methods exposed by an ``RpcServer``.  The response is a
standard Arrow IPC batch with one row per method, plus batch ``custom_metadata``
for protocol name, versions, and server identity.

Server side
-----------
``build_describe_batch()`` builds the cached response batch at server init
time.  ``RpcServer`` (with ``enable_describe=True``) handles ``__describe__``
requests by writing this pre-built batch directly.

Client side
-----------
``introspect()`` sends a ``__describe__`` request over a pipe/subprocess
transport and returns a ``ServiceDescription``.  ``http_introspect()`` does
the same over HTTP (see ``http.py``).

``parse_describe_batch()`` converts the raw Arrow batch into a typed
``ServiceDescription`` for programmatic use.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.metadata import (
    DESCRIBE_VERSION_KEY,
    PROTOCOL_HASH_KEY,
    PROTOCOL_NAME_KEY,
    REQUEST_VERSION,
    REQUEST_VERSION_KEY,
    RPC_METHOD_KEY,
    SERVER_ID_KEY,
)
from vgi_rpc.rpc import (
    _EMPTY_SCHEMA,
    MethodType,
    RpcMethodInfo,
    RpcTransport,
    _dispatch_log_or_error,
    _drain_stream,
)
from vgi_rpc.utils import IpcValidation, ValidatedReader

__all__ = [
    "DESCRIBE_METHOD_NAME",
    "DESCRIBE_VERSION",
    "MethodDescription",
    "ServiceDescription",
    "build_describe_batch",
    "compute_protocol_hash",
    "introspect",
    "parse_describe_batch",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DESCRIBE_METHOD_NAME = "__describe__"
"""Well-known method name for introspection requests."""

DESCRIBE_VERSION = "4"
"""Introspection format version for forward compatibility.

History:
  - v4: dropped Python-flavoured fields (``doc``, ``param_types_json``,
    ``param_defaults_json``, ``param_docs_json``); introduced
    ``protocol_hash`` (SHA-256 over the canonical describe payload).
  - v3: added ``param_docs_json``.
  - v2: added ``has_header``, ``header_schema_ipc``, ``is_exchange``.
"""

_DESCRIBE_FIELDS: list[pa.Field[pa.DataType]] = [
    pa.field("name", pa.utf8()),
    pa.field("method_type", pa.utf8()),
    pa.field("has_return", pa.bool_()),
    pa.field("params_schema_ipc", pa.binary()),
    pa.field("result_schema_ipc", pa.binary()),
    pa.field("has_header", pa.bool_()),
    pa.field("header_schema_ipc", pa.binary(), nullable=True),
    pa.field("is_exchange", pa.bool_(), nullable=True),
]
_DESCRIBE_SCHEMA = pa.schema(_DESCRIBE_FIELDS)


# ---------------------------------------------------------------------------
# Result dataclasses (NOT ArrowSerializableDataclass)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MethodDescription:
    """Description of a single RPC method from introspection.

    The wire format carries only language-neutral data: name, method
    type, schemas, and stream flags.  Rich Python-flavoured metadata
    (parameter type names, defaults, docstrings) lives in the Protocol
    source class — consumers needing that information import the
    Protocol directly rather than reconstructing it from the wire.

    For ``STREAM`` methods, ``result_schema`` reflects the Protocol-level
    return type (always empty).  The actual stream output schema is
    determined at runtime by the implementation and cannot be reported
    statically.

    Attributes:
        name: Method name as it appears on the Protocol.
        method_type: Whether this is UNARY or STREAM.
        has_return: ``True`` for unary methods that return a value.
        params_schema: Arrow schema for request parameters.
        result_schema: Arrow schema for the response (unary) or empty (streams).
        has_header: ``True`` for stream methods that declare a header type.
        header_schema: Arrow schema for the header, or ``None`` if no header.
        is_exchange: For streams, ``True`` if exchange (bidi), ``False`` if
            producer, ``None`` if unknown.  Always ``None`` for unary.

    """

    name: str
    method_type: MethodType
    has_return: bool
    params_schema: pa.Schema
    result_schema: pa.Schema
    has_header: bool = False
    header_schema: pa.Schema | None = None
    is_exchange: bool | None = None


@dataclass(frozen=True)
class ServiceDescription:
    """Complete description of an RPC service from introspection.

    Attributes:
        protocol_name: Name of the Protocol class.
        request_version: Wire protocol version.
        describe_version: Introspection format version.
        protocol_hash: SHA-256 hex digest of the canonical describe payload.
            Stable across server processes that expose the same Protocol;
            changes when any wire-relevant detail changes.  Use this as the
            schema-registry key when decoding archived access-log records.
        server_id: Server instance identifier.
        methods: Mapping of method name to ``MethodDescription``.

    """

    protocol_name: str
    request_version: str
    describe_version: str
    protocol_hash: str
    server_id: str
    methods: Mapping[str, MethodDescription]

    def __str__(self) -> str:
        """Return a human-readable summary of the service."""
        lines: list[str] = [
            f"RPC Service: {self.protocol_name}",
            f"  server_id: {self.server_id}",
            f"  request_version: {self.request_version}",
            f"  describe_version: {self.describe_version}",
            f"  protocol_hash: {self.protocol_hash}",
            "",
        ]
        for name, md in sorted(self.methods.items()):
            lines.append(f"  {name}({md.method_type.value})")
            if md.params_schema.names:
                lines.append(f"    params: {md.params_schema}")
            if md.has_return:
                lines.append(f"    returns: {md.result_schema}")
            lines.append("")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Server-side: build the cached batch
# ---------------------------------------------------------------------------


def build_describe_batch(
    protocol_name: str,
    methods: Mapping[str, RpcMethodInfo],
    server_id: str,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata]:
    """Build the ``__describe__`` response batch.

    One row per method, sorted by name.  The returned
    ``pa.KeyValueMetadata`` carries protocol name, wire protocol
    version, describe format version, ``protocol_hash``, and server
    identity — callers pass it as ``custom_metadata`` when writing.

    Args:
        protocol_name: Name of the Protocol class.
        methods: Method metadata from ``rpc_methods()``.
        server_id: The server's identity string.

    Returns:
        A ``(pa.RecordBatch, pa.KeyValueMetadata)`` tuple.  The batch
        uses the plain ``_DESCRIBE_SCHEMA`` (no schema-level metadata);
        the metadata dict carries the five introspection keys.

    """
    names: list[str] = []
    method_types: list[str] = []
    has_returns: list[bool] = []
    params_schemas: list[bytes] = []
    result_schemas: list[bytes] = []
    has_headers: list[bool] = []
    header_schemas: list[bytes | None] = []
    is_exchanges: list[bool | None] = []

    for name, info in sorted(methods.items()):
        names.append(name)
        method_types.append(info.method_type.value)
        has_returns.append(info.has_return)
        params_schemas.append(info.params_schema.serialize().to_pybytes())
        result_schemas.append(info.result_schema.serialize().to_pybytes())
        has_headers.append(info.header_type is not None)
        header_schemas.append(
            info.header_type.ARROW_SCHEMA.serialize().to_pybytes() if info.header_type is not None else None
        )
        is_exchanges.append(info.is_exchange)

    batch = pa.RecordBatch.from_pydict(
        {
            "name": names,
            "method_type": method_types,
            "has_return": has_returns,
            "params_schema_ipc": params_schemas,
            "result_schema_ipc": result_schemas,
            "has_header": has_headers,
            "header_schema_ipc": header_schemas,
            "is_exchange": is_exchanges,
        },
        schema=_DESCRIBE_SCHEMA,
    )

    protocol_hash = compute_protocol_hash(protocol_name, batch)

    custom_metadata = pa.KeyValueMetadata(
        {
            PROTOCOL_NAME_KEY: protocol_name.encode(),
            REQUEST_VERSION_KEY: REQUEST_VERSION,
            DESCRIBE_VERSION_KEY: DESCRIBE_VERSION.encode(),
            PROTOCOL_HASH_KEY: protocol_hash.encode(),
            SERVER_ID_KEY: server_id.encode(),
        }
    )
    return batch, custom_metadata


# ---------------------------------------------------------------------------
# Protocol hash
# ---------------------------------------------------------------------------


def compute_protocol_hash(protocol_name: str, batch: pa.RecordBatch) -> str:
    """Return the SHA-256 hex digest of the canonical describe payload.

    The hash covers only language-neutral wire fields: ``protocol_name``,
    ``request_version``, ``describe_version``, and for each method (sorted
    by name) ``name``, ``method_type``, ``has_return``, ``has_header``,
    ``is_exchange``, ``params_schema_ipc``, ``result_schema_ipc``, and
    ``header_schema_ipc``.  Server identity, docstrings, parameter type
    names, parameter defaults, and parameter docstrings are *not* part of
    the hash — they vary across processes/builds without changing protocol
    semantics.

    The hash is reproducible across language ports that produce the same
    Arrow schema bytes for the same Protocol.

    Args:
        protocol_name: Name of the Protocol class.
        batch: A ``__describe__`` response batch built by
            :func:`build_describe_batch` (rows sorted by name).

    Returns:
        Lowercase 64-character hex SHA-256 digest.

    """
    import hashlib

    h = hashlib.sha256()
    h.update(b"vgi_rpc.describe.v")
    h.update(DESCRIBE_VERSION.encode())
    h.update(b"|")
    h.update(REQUEST_VERSION)
    h.update(b"|")
    h.update(protocol_name.encode())
    h.update(b"|")
    n = batch.num_rows
    name_col = batch.column("name")
    method_type_col = batch.column("method_type")
    has_return_col = batch.column("has_return")
    params_col = batch.column("params_schema_ipc")
    result_col = batch.column("result_schema_ipc")
    has_header_col = batch.column("has_header")
    header_col = batch.column("header_schema_ipc")
    is_exchange_col = batch.column("is_exchange")
    for i in range(n):
        h.update(b"\x1f")
        h.update(name_col[i].as_py().encode())
        h.update(b"\x1e")
        h.update(method_type_col[i].as_py().encode())
        h.update(b"\x1e")
        h.update(b"1" if has_return_col[i].as_py() else b"0")
        h.update(b"\x1e")
        h.update(b"1" if has_header_col[i].as_py() else b"0")
        h.update(b"\x1e")
        is_exchange = is_exchange_col[i].as_py()
        h.update(b"-" if is_exchange is None else (b"1" if is_exchange else b"0"))
        h.update(b"\x1e")
        h.update(params_col[i].as_py())
        h.update(b"\x1e")
        h.update(result_col[i].as_py())
        h.update(b"\x1e")
        header_bytes = header_col[i].as_py()
        if header_bytes is not None:
            h.update(header_bytes)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Client-side: parse the response batch
# ---------------------------------------------------------------------------


def parse_describe_batch(
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None = None,
) -> ServiceDescription:
    """Parse a ``__describe__`` response batch into a ``ServiceDescription``.

    Reads batch ``custom_metadata`` for protocol name, versions, and server
    identity.  Each row is converted to a ``MethodDescription``.

    Args:
        batch: The response ``RecordBatch`` from a ``__describe__`` call.
        custom_metadata: The batch custom metadata carrying protocol name,
            versions, and server identity.

    Returns:
        A ``ServiceDescription`` with all method metadata.

    Raises:
        ValueError: If required metadata is missing.

    """
    md: dict[bytes, bytes] = dict(custom_metadata) if custom_metadata is not None else {}

    protocol_name: str = md.get(PROTOCOL_NAME_KEY, b"").decode()
    request_version: str = md.get(REQUEST_VERSION_KEY, b"").decode()
    describe_version: str = md.get(DESCRIBE_VERSION_KEY, b"").decode()
    protocol_hash: str = md.get(PROTOCOL_HASH_KEY, b"").decode()
    server_id: str = md.get(SERVER_ID_KEY, b"").decode()

    method_map: dict[str, MethodDescription] = {}
    for i in range(batch.num_rows):
        name: str = batch.column("name")[i].as_py()
        method_type = MethodType(batch.column("method_type")[i].as_py())
        has_return: bool = batch.column("has_return")[i].as_py()

        params_schema_bytes: bytes = batch.column("params_schema_ipc")[i].as_py()
        result_schema_bytes: bytes = batch.column("result_schema_ipc")[i].as_py()
        params_schema = pa.ipc.read_schema(pa.py_buffer(params_schema_bytes))
        result_schema = pa.ipc.read_schema(pa.py_buffer(result_schema_bytes))

        has_header: bool = batch.column("has_header")[i].as_py()
        header_schema: pa.Schema | None = None
        header_schema_bytes: bytes | None = batch.column("header_schema_ipc")[i].as_py()
        if header_schema_bytes is not None:
            header_schema = pa.ipc.read_schema(pa.py_buffer(header_schema_bytes))

        is_exchange: bool | None = batch.column("is_exchange")[i].as_py()

        method_map[name] = MethodDescription(
            name=name,
            method_type=method_type,
            has_return=has_return,
            params_schema=params_schema,
            result_schema=result_schema,
            has_header=has_header,
            header_schema=header_schema,
            is_exchange=is_exchange,
        )

    return ServiceDescription(
        protocol_name=protocol_name,
        request_version=request_version,
        describe_version=describe_version,
        protocol_hash=protocol_hash,
        server_id=server_id,
        methods=method_map,
    )


# ---------------------------------------------------------------------------
# Client-side: pipe/subprocess introspection
# ---------------------------------------------------------------------------


def introspect(
    transport: RpcTransport,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> ServiceDescription:
    """Send a ``__describe__`` request over any ``RpcTransport``.

    Args:
        transport: An open ``RpcTransport``.
        ipc_validation: Validation level for incoming IPC batches.

    Returns:
        A ``ServiceDescription`` with all method metadata.

    Raises:
        RpcError: If the server does not support introspection or returns
            an error.

    """
    # Write a minimal request: empty params, method = __describe__
    request_metadata = pa.KeyValueMetadata(
        {RPC_METHOD_KEY: DESCRIBE_METHOD_NAME.encode(), REQUEST_VERSION_KEY: REQUEST_VERSION}
    )
    with ipc.new_stream(transport.writer, _EMPTY_SCHEMA) as writer:
        empty_batch = pa.RecordBatch.from_pydict({}, schema=_EMPTY_SCHEMA)
        writer.write_batch(empty_batch, custom_metadata=request_metadata)

    # Read response
    reader = ValidatedReader(ipc.open_stream(transport.reader), ipc_validation)
    # Skip log batches, collect the data batch
    while True:
        batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
        if not _dispatch_log_or_error(batch, custom_metadata):
            break
    _drain_stream(reader)

    return parse_describe_batch(batch, custom_metadata)
