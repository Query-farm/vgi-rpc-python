"""Introspection support for vgi-rpc servers.

Provides a built-in ``__describe__`` RPC method that returns machine-readable
metadata about all methods exposed by an ``RpcServer``.  The response is a
standard Arrow IPC batch with one row per method, plus schema-level metadata
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

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated, Any, get_args, get_origin

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.metadata import REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY, SERVER_ID_KEY
from vgi_rpc.rpc import (
    _EMPTY_SCHEMA,
    MethodType,
    RpcMethodInfo,
    RpcTransport,
    _dispatch_log_or_error,
    _drain_stream,
)
from vgi_rpc.utils import IpcValidation, ValidatedReader, _is_optional_type

__all__ = [
    "DESCRIBE_METHOD_NAME",
    "DESCRIBE_VERSION",
    "MethodDescription",
    "ServiceDescription",
    "build_describe_batch",
    "introspect",
    "parse_describe_batch",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DESCRIBE_METHOD_NAME = "__describe__"
"""Well-known method name for introspection requests."""

DESCRIBE_VERSION = "1"
"""Introspection format version for forward compatibility."""

PROTOCOL_NAME_KEY = b"vgi_rpc.protocol_name"
"""Schema-level metadata key for the protocol class name."""

DESCRIBE_VERSION_KEY = b"vgi_rpc.describe_version"
"""Schema-level metadata key for the introspection format version."""

_DESCRIBE_FIELDS: list[pa.Field[pa.DataType]] = [
    pa.field("name", pa.utf8()),
    pa.field("method_type", pa.utf8()),
    pa.field("doc", pa.utf8(), nullable=True),
    pa.field("has_return", pa.bool_()),
    pa.field("params_schema_ipc", pa.binary()),
    pa.field("result_schema_ipc", pa.binary()),
    pa.field("param_types_json", pa.utf8(), nullable=True),
    pa.field("param_defaults_json", pa.utf8(), nullable=True),
]
_DESCRIBE_SCHEMA = pa.schema(_DESCRIBE_FIELDS)


# ---------------------------------------------------------------------------
# Type-name helper
# ---------------------------------------------------------------------------


def _type_name(python_type: Any) -> str:
    """Convert a Python type annotation to a human-readable string.

    Handles basic types, Optional, generics, Enum subclasses,
    and ArrowSerializableDataclass subclasses.

    Args:
        python_type: A Python type annotation.

    Returns:
        A concise, human-readable type name string.

    """
    if python_type is type(None):
        return "None"

    inner, is_nullable = _is_optional_type(python_type)
    if is_nullable:
        return f"{_type_name(inner)} | None"

    origin = get_origin(python_type)

    # Annotated[T, ...] → unwrap to T
    if origin is Annotated:
        return _type_name(get_args(python_type)[0])

    if isinstance(python_type, type):
        return python_type.__name__

    # Generic types: list[T], dict[K, V], frozenset[T]
    args = get_args(python_type)
    if origin is list:
        return f"list[{_type_name(args[0])}]" if args else "list"
    if origin is dict:
        return f"dict[{_type_name(args[0])}, {_type_name(args[1])}]" if len(args) >= 2 else "dict"
    if origin is frozenset:
        return f"frozenset[{_type_name(args[0])}]" if args else "frozenset"

    return str(python_type)


# ---------------------------------------------------------------------------
# Defaults serialization
# ---------------------------------------------------------------------------


def _safe_defaults_json(defaults: dict[str, Any]) -> str | None:
    """JSON-serialize parameter defaults, skipping non-serializable values.

    Args:
        defaults: Mapping of parameter name to default value.

    Returns:
        JSON string of serializable defaults, or ``None`` if empty.

    """
    if not defaults:
        return None
    safe: dict[str, object] = {}
    for name, value in defaults.items():
        if isinstance(value, Enum):
            safe[name] = value.name
        elif isinstance(value, (str, int, float, bool)):
            safe[name] = value
        elif value is None:
            safe[name] = None
        elif isinstance(value, (list, tuple, dict)):
            try:
                json.dumps(value)
                safe[name] = value
            except (TypeError, ValueError):
                pass
        # Skip non-serializable values silently
    return json.dumps(safe) if safe else None


# ---------------------------------------------------------------------------
# Result dataclasses (NOT ArrowSerializableDataclass)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MethodDescription:
    """Description of a single RPC method from introspection.

    For ``SERVER_STREAM`` and ``BIDI_STREAM`` methods, ``result_schema``
    reflects the Protocol-level return type (typically empty).  The actual
    stream output schema is determined at runtime by the implementation and
    cannot be reported statically.

    Attributes:
        name: Method name as it appears on the Protocol.
        method_type: Whether this is UNARY, SERVER_STREAM, or BIDI_STREAM.
        doc: The method's docstring, or ``None``.
        has_return: ``True`` for unary methods that return a value.
        params_schema: Arrow schema for request parameters.
        result_schema: Arrow schema for the response (unary) or empty (streams).
        param_types: Human-readable type names keyed by parameter name.
        param_defaults: Parsed default values keyed by parameter name.

    """

    name: str
    method_type: MethodType
    doc: str | None
    has_return: bool
    params_schema: pa.Schema
    result_schema: pa.Schema
    param_types: dict[str, str] = field(default_factory=dict)
    param_defaults: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ServiceDescription:
    """Complete description of an RPC service from introspection.

    Attributes:
        protocol_name: Name of the Protocol class.
        request_version: Wire protocol version.
        describe_version: Introspection format version.
        server_id: Server instance identifier.
        methods: Mapping of method name to ``MethodDescription``.

    """

    protocol_name: str
    request_version: str
    describe_version: str
    server_id: str
    methods: Mapping[str, MethodDescription]

    def __str__(self) -> str:
        """Return a human-readable summary of the service."""
        lines: list[str] = [
            f"RPC Service: {self.protocol_name}",
            f"  server_id: {self.server_id}",
            f"  request_version: {self.request_version}",
            f"  describe_version: {self.describe_version}",
            "",
        ]
        for name, md in sorted(self.methods.items()):
            lines.append(f"  {name}({md.method_type.value})")
            if md.param_types:
                params_str = ", ".join(f"{k}: {v}" for k, v in md.param_types.items())
                lines.append(f"    params: {params_str}")
            if md.has_return:
                lines.append(f"    returns: {md.result_schema}")
            if md.doc:
                lines.append(f"    doc: {md.doc.strip()}")
            lines.append("")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Server-side: build the cached batch
# ---------------------------------------------------------------------------


def build_describe_batch(
    protocol_name: str,
    methods: Mapping[str, RpcMethodInfo],
    server_id: str,
) -> pa.RecordBatch:
    """Build the ``__describe__`` response batch.

    One row per method.  Schema-level metadata carries protocol name,
    wire protocol version, describe format version, and server identity.

    Args:
        protocol_name: Name of the Protocol class.
        methods: Method metadata from ``rpc_methods()``.
        server_id: The server's identity string.

    Returns:
        A ``pa.RecordBatch`` with ``_DESCRIBE_SCHEMA`` plus schema-level metadata.

    """
    names: list[str] = []
    method_types: list[str] = []
    docs: list[str | None] = []
    has_returns: list[bool] = []
    params_schemas: list[bytes] = []
    result_schemas: list[bytes] = []
    param_types_jsons: list[str | None] = []
    param_defaults_jsons: list[str | None] = []

    for name, info in sorted(methods.items()):
        names.append(name)
        method_types.append(info.method_type.value)
        docs.append(info.doc)
        has_returns.append(info.has_return)
        params_schemas.append(info.params_schema.serialize().to_pybytes())
        result_schemas.append(info.result_schema.serialize().to_pybytes())

        # Build param_types_json
        if info.param_types:
            pt = {k: _type_name(v) for k, v in info.param_types.items()}
            param_types_jsons.append(json.dumps(pt))
        else:
            param_types_jsons.append(None)

        # Build param_defaults_json
        param_defaults_jsons.append(_safe_defaults_json(info.param_defaults))

    # Schema-level metadata
    schema_metadata = {
        PROTOCOL_NAME_KEY: protocol_name.encode(),
        REQUEST_VERSION_KEY: REQUEST_VERSION,
        DESCRIBE_VERSION_KEY: DESCRIBE_VERSION.encode(),
        SERVER_ID_KEY: server_id.encode(),
    }
    schema_with_metadata = _DESCRIBE_SCHEMA.with_metadata(schema_metadata)

    batch = pa.RecordBatch.from_pydict(
        {
            "name": names,
            "method_type": method_types,
            "doc": docs,
            "has_return": has_returns,
            "params_schema_ipc": params_schemas,
            "result_schema_ipc": result_schemas,
            "param_types_json": param_types_jsons,
            "param_defaults_json": param_defaults_jsons,
        },
        schema=schema_with_metadata,
    )
    return batch


# ---------------------------------------------------------------------------
# Client-side: parse the response batch
# ---------------------------------------------------------------------------


def parse_describe_batch(batch: pa.RecordBatch) -> ServiceDescription:
    """Parse a ``__describe__`` response batch into a ``ServiceDescription``.

    Reads schema-level metadata for protocol name, versions, and server
    identity.  Each row is converted to a ``MethodDescription``.

    Args:
        batch: The response ``RecordBatch`` from a ``__describe__`` call.

    Returns:
        A ``ServiceDescription`` with all method metadata.

    Raises:
        ValueError: If required schema metadata is missing.

    """
    schema_md = batch.schema.metadata or {}

    protocol_name = schema_md.get(PROTOCOL_NAME_KEY, b"").decode()
    request_version = schema_md.get(REQUEST_VERSION_KEY, b"").decode()
    describe_version = schema_md.get(DESCRIBE_VERSION_KEY, b"").decode()
    server_id = schema_md.get(SERVER_ID_KEY, b"").decode()

    method_map: dict[str, MethodDescription] = {}
    for i in range(batch.num_rows):
        name = batch.column("name")[i].as_py()
        method_type = MethodType(batch.column("method_type")[i].as_py())
        doc = batch.column("doc")[i].as_py()
        has_return = batch.column("has_return")[i].as_py()

        params_schema_bytes: bytes = batch.column("params_schema_ipc")[i].as_py()
        result_schema_bytes: bytes = batch.column("result_schema_ipc")[i].as_py()
        params_schema = pa.ipc.read_schema(pa.py_buffer(params_schema_bytes))
        result_schema = pa.ipc.read_schema(pa.py_buffer(result_schema_bytes))

        pt_json: str | None = batch.column("param_types_json")[i].as_py()
        param_types: dict[str, str] = json.loads(pt_json) if pt_json else {}

        pd_json: str | None = batch.column("param_defaults_json")[i].as_py()
        param_defaults: dict[str, object] = json.loads(pd_json) if pd_json else {}

        method_map[name] = MethodDescription(
            name=name,
            method_type=method_type,
            doc=doc,
            has_return=has_return,
            params_schema=params_schema,
            result_schema=result_schema,
            param_types=param_types,
            param_defaults=param_defaults,
        )

    return ServiceDescription(
        protocol_name=protocol_name,
        request_version=request_version,
        describe_version=describe_version,
        server_id=server_id,
        methods=method_map,
    )


# ---------------------------------------------------------------------------
# Client-side: pipe/subprocess introspection
# ---------------------------------------------------------------------------


def introspect(
    transport: RpcTransport,
    ipc_validation: IpcValidation = IpcValidation.NONE,
) -> ServiceDescription:
    """Send a ``__describe__`` request over a pipe/subprocess transport.

    Args:
        transport: An open ``RpcTransport`` (pipe or subprocess).
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

    # Reconstruct the schema metadata from the reader's schema
    # The batch itself doesn't carry schema-level metadata; the IPC stream schema does
    schema_with_md = reader.schema.with_metadata(reader.schema.metadata or {})
    batch_with_md = pa.RecordBatch.from_arrays(
        [batch.column(i) for i in range(batch.num_columns)],
        schema=schema_with_md,
    )

    return parse_describe_batch(batch_with_md)
