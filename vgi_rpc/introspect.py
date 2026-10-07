"""Client-side introspection over ``vgi_rpc.Reflection.v1``.

Introspection used to be ``__describe__``, a hardcoded method name answered
from a pre-built batch in a bespoke format.  It is now an ordinary co-hosted
protocol (see ``rpc/_reflection.py``), which is what lets six ports generate it
rather than hand-maintain it.

What survives here is the *client-side view*: ``ServiceDescription`` and
``MethodDescription`` are convenience dataclasses for Python callers, not a
wire format -- they were only ever the latter by accident of there having been
a single encoding.  ``introspect()`` and ``http_introspect()`` speak reflection
and present the result in this shape, so the CLI, the describe page and the
conformance runner did not have to change.

``list_protocols()`` and ``describe_protocol()`` are the connection-reusing
entry points: they take a proxy a caller already holds -- on any transport --
and ask reflection over that same connection, where ``introspect()`` needs a
raw transport and ``http_introspect()`` builds its own HTTP calls.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.metadata import (
    REQUEST_VERSION,
    REQUEST_VERSION_KEY,
    RPC_METHOD_KEY,
)
from vgi_rpc.rpc import (
    _EMPTY_SCHEMA,
    MethodType,
    RpcError,
    RpcTransport,
    _dispatch_log_or_error,
    _drain_stream,
)
from vgi_rpc.rpc._protocol_hash import compute_protocol_hash as _compute_protocol_hash
from vgi_rpc.utils import IpcValidation, ValidatedReader, new_ipc_stream

if TYPE_CHECKING:
    # Imported for annotations only: the wire types live under ``vgi_rpc.rpc``,
    # which imports this module at runtime.  Naming them here costs nothing at
    # import time and lets both type checkers see through ``_adapt_description``
    # instead of being told to look away.
    from vgi_rpc.rpc._reflection import ProtocolList as _WireProtocolList
    from vgi_rpc.rpc._reflection import ServiceDescription as _WireDescription

__all__ = [
    "DESCRIBE_VERSION",
    "HostedProtocol",
    "MethodDescription",
    "ReflectionNotSupportedError",
    "ServiceDescription",
    "compute_protocol_hash",
    "describe_protocol",
    "introspect",
    "list_protocols",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Re-exported so ``vgi_rpc.introspect.compute_protocol_hash`` keeps working.
#: The definition lives in ``rpc/_protocol_hash.py`` because it is taken over
#: the decoded description rather than over any particular encoding of it.
compute_protocol_hash = _compute_protocol_hash

DESCRIBE_VERSION = "5"
"""Introspection format version.

Vestigial since v5: introspection is ``vgi_rpc.Reflection.v1`` now, a protocol
whose major version is part of its own name, so there is no separate format
number to negotiate.  Reported for the benefit of readers who still look for
it, and it will not move again.

History:
  - v5: introspection became a protocol.  ``__describe__``, ``_DESCRIBE_SCHEMA``,
    ``build_describe_batch`` and ``parse_describe_batch`` are gone; the payload
    is a generated schema and the hash is taken over the decoded description.
  - v4: dropped Python-flavoured fields; introduced ``protocol_hash``.
  - v3: added ``param_docs_json``.
  - v2: added ``has_header``, ``header_schema_ipc``, ``is_exchange``.
"""


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
        protocol_version: Application protocol surface version declared by
            the Protocol class (canonical semver MAJOR.MINOR.PATCH), or empty
            string when the Protocol opts out. Diagnostic — actual enforcement
            happens via the per-request ``vgi_rpc.protocol_version`` metadata
            key at the server's dispatch boundary.

    """

    protocol_name: str
    request_version: str
    describe_version: str
    protocol_hash: str
    server_id: str
    methods: Mapping[str, MethodDescription]
    protocol_version: str = ""

    def __str__(self) -> str:
        """Return a human-readable summary of the service."""
        lines: list[str] = [
            f"RPC Service: {self.protocol_name}",
            f"  server_id: {self.server_id}",
            f"  request_version: {self.request_version}",
            f"  describe_version: {self.describe_version}",
            f"  protocol_hash: {self.protocol_hash}",
        ]
        if self.protocol_version:
            lines.append(f"  protocol_version: {self.protocol_version}")
        lines.append("")
        for name, md in sorted(self.methods.items()):
            lines.append(f"  {name}({md.method_type.value})")
            if md.params_schema.names:
                lines.append(f"    params: {md.params_schema}")
            if md.has_return:
                lines.append(f"    returns: {md.result_schema}")
            lines.append("")
        return "\n".join(lines)


@dataclass(frozen=True)
class HostedProtocol:
    """One protocol a server hosts, as ``vgi_rpc.Reflection.v1`` lists it.

    A client-side view of the wire ``ProtocolSummary``, returned by
    :func:`list_protocols` in the server's order: application protocols in
    registration order (the primary first), then the framework's own
    (``vgi_rpc.Reflection.v1``, and ``vgi_rpc.Identity.v1`` on an HTTP server
    that hosts it).

    Attributes:
        name: The protocol's wire name -- its routing key, carrying its major
            version, e.g. ``"vgi_rpc.Reflection.v1"``.
        version: Its declared semver, or ``""`` when it declares none.
        hash: SHA-256 of its canonical description, as 64 lowercase hex
            characters.  Equal hashes mean an identical wire surface, in any
            port, so a caller holding a cached description for this hash can
            skip :func:`describe_protocol`.
        deprecated: Whether callers should migrate off this protocol.
        deprecation_message: What to migrate to; empty unless ``deprecated``.
        features: Capability tokens the protocol announces.

    """

    name: str
    version: str
    hash: str
    deprecated: bool = False
    deprecation_message: str = ""
    features: tuple[str, ...] = ()


class ReflectionNotSupportedError(RpcError):
    """The server does not host ``vgi_rpc.Reflection.v1``.

    Raised by :func:`list_protocols` and :func:`describe_protocol` when the
    server answers the reflection call with "not hosted" rather than with a
    listing: a server built without ``enable_describe=True`` (the default),
    or one that predates reflection.  Such a server still serves its own protocol, so this
    is a statement about discovery, not about the connection -- the
    connection remains usable.

    A subclass of :class:`~vgi_rpc.rpc.RpcError` carrying the server's
    original error fields, so code that already catches ``RpcError`` keeps
    working; catch this class to branch on "cannot discover" specifically.
    """

    @classmethod
    def from_rpc_error(cls, error: RpcError) -> ReflectionNotSupportedError:
        """Wrap the server's "not hosted" answer, keeping every field.

        Args:
            error: The error the reflection call raised.

        Returns:
            An instance carrying *error*'s type, message, traceback, request
            id, code, kind and details.

        """
        return cls(
            error.error_type,
            error.error_message,
            error.remote_traceback,
            request_id=error.request_id,
            error_code=error.error_code,
            error_kind=error.error_kind,
            error_details=error.error_details,
        )


#: ``error_kind`` values meaning "this server does not answer reflection".
_NOT_HOSTED_KINDS = frozenset({"protocol_not_supported", "method_not_implemented"})

#: Remote exception names for the same, from servers that send no error kind.
_NOT_HOSTED_TYPES = frozenset({"ProtocolNotSupportedError", "MethodNotImplementedError"})


def _reflection_not_hosted(error: RpcError) -> bool:
    """Whether *error* says the server does not host reflection at all.

    Only meaningful for ``list_protocols``, which is always hosted when
    reflection is: a "not supported" answer to it can only be about the
    protocol.  (``describe`` answers ``protocol_not_supported`` for an
    unknown *argument*, which is why :func:`describe_protocol` lists first.)

    A current server without reflection answers ``protocol_not_supported``;
    one older than multi-protocol hosting ignores the protocol key and answers
    an unknown method; both carry ``UNIMPLEMENTED`` when the server sends a
    code at all.  An HTTP server older than protocol-scoped routes answers a
    bare 404, which the HTTP client reports as a non-Arrow ``HttpError``.
    """
    if error.error_kind in _NOT_HOSTED_KINDS or error.error_code == "UNIMPLEMENTED":
        return True
    if error.error_type in _NOT_HOSTED_TYPES:
        return True
    return error.error_type == "HttpError" and error.error_message.startswith("HTTP 404")


def _reflection_proxy(target: object) -> Any:
    """Return a ``vgi_rpc.Reflection.v1`` proxy on *target*'s connection.

    Typed ``Any`` because the proxy is the dynamic one every connect function
    returns; the two methods called on it are fixed by ``Reflection``.
    """
    from vgi_rpc.rpc._client import _RpcProxy
    from vgi_rpc.rpc._reflection import Reflection

    rebind = getattr(target, "_rebind_protocol", None)
    if callable(rebind):
        return rebind(Reflection)
    if isinstance(target, RpcTransport):
        return _RpcProxy(Reflection, target)
    raise TypeError(
        f"cannot reach reflection through a {type(target).__name__}: pass a proxy returned by "
        f"connect(), serve_pipe(), unix_connect(), tcp_connect(), iroh_connect(), http_connect(), "
        f"httpi_connect(), WorkerPool.connect() or RpcConnection, or an RpcTransport"
    )


def _list(proxy: Any) -> _WireProtocolList:
    """Call ``list_protocols`` on a reflection proxy, classifying "not hosted"."""
    try:
        listing: _WireProtocolList = proxy.list_protocols()
    except ReflectionNotSupportedError:
        raise
    except RpcError as exc:
        if _reflection_not_hosted(exc):
            raise ReflectionNotSupportedError.from_rpc_error(exc) from exc
        raise
    return listing


def list_protocols(target: object) -> list[HostedProtocol]:
    """List the protocols a server hosts, over a connection the caller holds.

    One round trip -- ``vgi_rpc.Reflection.v1.list_protocols`` -- on *target*'s
    own connection; nothing new is opened and nothing is closed.  Over HTTP
    the call shares the proxy's ``httpx2.Client``, prefix, auth, retry and
    response-budget settings; over every other transport it shares the
    proxy's byte stream, which the server demultiplexes by each request's
    protocol key.  On a pipe-like transport, do not call it while a stream is
    open on the same connection: the two would interleave on one channel.

    Args:
        target: A proxy from any connect function -- ``connect``,
            ``serve_pipe``, ``unix_connect``, ``tcp_connect``,
            ``iroh_connect``, ``http_connect``, ``httpi_connect``,
            ``WorkerPool.connect`` or ``RpcConnection`` -- bound to any
            protocol the server hosts, or a raw ``RpcTransport``.

    Returns:
        One :class:`HostedProtocol` per hosted protocol, in the server's
        order: application protocols first, primary leading, then the
        framework's own.

    Raises:
        ReflectionNotSupportedError: The server does not host reflection
            (built without ``enable_describe=True``, or older than it).
            The connection is still usable.
        RpcError: The server answered with any other error, or the
            transport failed.
        TypeError: *target* is neither a proxy nor an ``RpcTransport``.

    """
    listing = _list(_reflection_proxy(target))
    return [
        HostedProtocol(
            name=p.protocol,
            version=p.protocol_version,
            hash=p.protocol_hash,
            deprecated=p.deprecated,
            deprecation_message=p.deprecation_message,
            features=tuple(p.features),
        )
        for p in listing.protocols
    ]


def describe_protocol(target: object, name: str) -> ServiceDescription:
    """Describe one hosted protocol, over a connection the caller holds.

    Two round trips on *target*'s connection: ``list_protocols`` (for the
    server identity the description carries, and to tell "no reflection"
    apart from "no such protocol") then ``describe(name)``.  The connection
    rules are those of :func:`list_protocols`.

    Args:
        target: A proxy from any connect function, or a raw
            ``RpcTransport``; see :func:`list_protocols`.
        name: The protocol's wire name, as :func:`list_protocols` reports it.

    Returns:
        A :class:`ServiceDescription` with each method's type and schemas.

    Raises:
        ReflectionNotSupportedError: The server does not host reflection.
        RpcError: The server does not host *name* (``error_kind``
            ``"protocol_not_supported"``), answered with another error, or
            the transport failed.

    """
    proxy = _reflection_proxy(target)
    listing = _list(proxy)
    described: _WireDescription = proxy.describe(protocol=name)
    return _adapt_description(described, listing)


# ---------------------------------------------------------------------------
# Server-side: build the cached batch
# ---------------------------------------------------------------------------


def _reflection_call(
    transport: RpcTransport,
    method: str,
    params_schema: pa.Schema,
    params: dict[str, object],
    ipc_validation: IpcValidation,
) -> bytes:
    """Make one unary call to ``vgi_rpc.Reflection.v1`` over a raw transport.

    Written by hand rather than through the generated client because
    introspection is what a caller does *before* it has a client bound to a
    protocol -- that is the whole point of it.
    """
    from vgi_rpc.metadata import PROTOCOL_KEY
    from vgi_rpc.rpc._reflection import Reflection

    request_metadata = pa.KeyValueMetadata(
        {
            RPC_METHOD_KEY: method.encode(),
            PROTOCOL_KEY: Reflection.protocol_name.encode(),
            REQUEST_VERSION_KEY: REQUEST_VERSION,
        }
    )
    with new_ipc_stream(transport.writer, params_schema) as writer:
        writer.write_batch(
            pa.RecordBatch.from_pydict({k: [v] for k, v in params.items()}, schema=params_schema),
            custom_metadata=request_metadata,
        )

    reader = ValidatedReader(ipc.open_stream(transport.reader), ipc_validation)
    while True:
        batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
        if not _dispatch_log_or_error(batch, custom_metadata):
            break
    _drain_stream(reader)
    del custom_metadata
    # A dataclass return rides as serialized bytes in a single ``result``
    # column -- the framework's ordinary unary convention.  Reflection is an
    # ordinary protocol now, so it is subject to it like everything else.
    result: bytes = batch.column("result")[0].as_py()
    return result


def introspect(
    transport: RpcTransport,
    ipc_validation: IpcValidation = IpcValidation.FULL,
    protocol: str | None = None,
) -> ServiceDescription:
    """Describe a server's protocol over any ``RpcTransport``.

    Two round trips: ``list_protocols`` to learn what is hosted, then
    ``describe`` on one of them.  The first is unavoidable now that a server
    may host several protocols -- there is no longer a single "the" protocol to
    ask about without asking.

    Args:
        transport: An open ``RpcTransport``.
        ipc_validation: Validation level for incoming IPC batches.
        protocol: Which protocol to describe.  Defaults to the first hosted
            one that is not reflection itself, which is the primary.

    Returns:
        A ``ServiceDescription`` with all method metadata.

    Raises:
        RpcError: If the server does not support reflection or returns an
            error.
        ValueError: If the server hosts no application protocol.

    """
    from vgi_rpc.rpc._reflection import ProtocolList
    from vgi_rpc.rpc._reflection import ServiceDescription as WireDescription

    listing = ProtocolList.deserialize_from_bytes(
        _reflection_call(transport, "list_protocols", _EMPTY_SCHEMA, {}, ipc_validation)
    )

    if protocol is None:
        application = [p for p in listing.protocols if not p.protocol.startswith("vgi_rpc.")]
        if not application:
            raise ValueError(f"Server {listing.server_id} hosts no application protocol.")
        protocol = application[0].protocol

    params_schema = pa.schema([pa.field("protocol", pa.utf8(), nullable=False)])
    described = WireDescription.deserialize_from_bytes(
        _reflection_call(transport, "describe", params_schema, {"protocol": protocol}, ipc_validation)
    )

    return _adapt_description(described, listing)


def _adapt_description(wire: _WireDescription, listing: _WireProtocolList) -> ServiceDescription:
    """Present a reflection reply in this module's client-side shape.

    ``introspect.ServiceDescription`` is a *client-side view*, not a wire
    format -- it was only ever the latter by accident of there being one
    encoding.  Keeping it lets the CLI, the describe page and the conformance
    runner move to reflection without changing a line.
    """
    methods: dict[str, MethodDescription] = {}
    for m in wire.methods:
        methods[m.name] = MethodDescription(
            name=m.name,
            method_type=MethodType(m.method_type),
            has_return=m.has_return,
            params_schema=_read_schema(m.params_schema_ipc),
            result_schema=_read_schema(m.result_schema_ipc),
            has_header=m.has_header,
            header_schema=_read_schema(m.header_schema_ipc) if m.has_header else None,
            is_exchange=_is_exchange(m.stream_kind),
        )
    return ServiceDescription(
        protocol_name=wire.protocol,
        request_version=listing.request_version,
        describe_version=DESCRIBE_VERSION,
        protocol_hash=wire.protocol_hash,
        server_id=listing.server_id,
        methods=methods,
        protocol_version=wire.protocol_version,
    )


def _read_schema(ipc_bytes: bytes) -> pa.Schema:
    """Read a schema from IPC bytes, treating empty as the empty schema."""
    if not ipc_bytes:
        return _EMPTY_SCHEMA
    return ipc.read_schema(pa.py_buffer(ipc_bytes))


def _is_exchange(stream_kind: str) -> bool | None:
    """Map a reflection stream kind back to this module's tri-state bool."""
    if stream_kind == "exchange":
        return True
    if stream_kind == "producer":
        return False
    return None
