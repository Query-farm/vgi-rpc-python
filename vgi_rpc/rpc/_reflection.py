# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``vgi_rpc.Reflection.v1`` — discovery as an ordinary co-hosted protocol.

Introspection used to be a hardcoded method name, ``__describe__``, answered
from a pre-built batch before dispatch.  That made it a thing every port had to
hand-implement, in a bespoke format, outside the codegen that produces every
other method — which is how the recorded describe drift happened.  Here it is a
protocol like any other: its methods are generated, its payload is a generated
schema, and the ports get it from the same pipeline as everything else.

Following gRPC's reflection service and D-Bus's ``org.freedesktop.DBus``, it is
*co-hosted* rather than special-cased.  Its own major version sits in its name,
so an incompatible reflection is a 404 a client can act on rather than a
mis-parse.

**Exempt from the protocol_version gate.**  This is the protocol a
version-mismatched client calls to learn *what* mismatched; gating it would deny
the client the diagnosis it came for.  The exemption is a property of the
binding, not a hardcoded method name.

**Minor skew must be survivable**, which the generated decoder gives: it reads
by field name, ignores columns it does not know, and defaults columns that are
absent.  A v1.1 server answering a v1.0 client therefore decodes, and a v1.0
server answering a v1.1 client decodes with defaults.  That property is load
bearing -- a strict decoder would fail at exactly the moment a client most
needs a good answer -- so it is asserted in ``tests/test_reflection.py``
against a deliberately extended schema, not merely relied upon.

The decoder defaults what *has* a default and raises for what does not, which
is right: silently zero-filling a required field hands a client a description
that is wrong rather than absent.  It also makes one rule normative for every
port -- **any field added in a minor version must carry a default**, or the
addition is a breaking change wearing a minor version number.
"""

from __future__ import annotations

import dataclasses
from typing import ClassVar, Protocol

import pyarrow as pa

from vgi_rpc.utils import ArrowSerializableDataclass

__all__ = [
    "IDEMPOTENCY_LEVELS",
    "STREAM_KINDS",
    "MethodInfo",
    "ProtocolList",
    "ProtocolSummary",
    "ServiceDescription",
]

#: How safe a method is to retry, borrowed from gRPC's ``idempotency_level``.
#: With an HTTP transport and a policy proxy in the path, retries *will* happen;
#: nothing on the wire said what was safe to retry.
#:
#: - ``unknown``        -- the default; a caller must assume the worst.
#: - ``no_side_effects``-- a read; safe to retry, and safe to issue twice in
#:                         parallel and take whichever answers first.
#: - ``idempotent``     -- has side effects, but repeating it is equivalent to
#:                         performing it once.
IDEMPOTENCY_LEVELS = ("unknown", "no_side_effects", "idempotent")

#: What a stream method does, when that is knowable.  Whether a stream is an
#: exchange is decided by the implementation's return type, not by the
#: Protocol, so a server describing its own Protocol often cannot say --
#: ``unknown`` is the honest answer and is spelled rather than left null.
STREAM_KINDS = ("unknown", "producer", "exchange")


@dataclasses.dataclass(frozen=True)
class MethodInfo(ArrowSerializableDataclass):
    """One method's wire surface.

    Schemas travel as serialized Arrow IPC rather than as a structural
    description: a client's whole purpose in asking is to get a schema it can
    hand to its own Arrow implementation, and IPC is the one representation
    every port already reads.  The *hash* is what compares across ports, and it
    is taken over the decoded structure precisely so these bytes need not
    match.

    Attributes:
        name: Method name.
        method_type: ``"unary"`` or ``"stream"``.
        has_return: Whether a unary method returns a value.
        has_header: Whether a stream declares a header type.
        stream_kind: For streams, one of :data:`STREAM_KINDS`; empty for
            unary.  A string rather than a nullable bool because the state is
            genuinely three-valued -- whether a stream is an exchange is an
            *implementation* property, not visible on the Protocol -- and
            "unknown" should be said rather than encoded as absence.
        params_schema_ipc: Request parameter schema, as Arrow IPC.
        result_schema_ipc: Response schema, as Arrow IPC; empty when
            ``has_return`` is false.
        header_schema_ipc: Stream header schema, as Arrow IPC; empty when
            ``has_header`` is false.
        idempotency: One of :data:`IDEMPOTENCY_LEVELS`.
        deprecated: Whether callers should migrate off this method.
        deprecation_message: What to migrate to.  Empty unless ``deprecated``.

    """

    name: str
    method_type: str
    has_return: bool
    has_header: bool
    stream_kind: str
    params_schema_ipc: bytes
    result_schema_ipc: bytes
    header_schema_ipc: bytes
    idempotency: str = "unknown"
    deprecated: bool = False
    deprecation_message: str = ""


@dataclasses.dataclass(frozen=True)
class ServiceDescription(ArrowSerializableDataclass):
    """One protocol's full description.

    Carries no server identity: two processes serving the same protocol must
    describe it identically, or the description is not a property of the
    protocol.  Server identity lives on :class:`ProtocolList`, which is a
    statement about a server rather than about a protocol.

    Attributes:
        protocol: Wire name -- the routing key, carrying the major version.
        protocol_version: Declared semver, or empty when the protocol opts out.
        protocol_hash: Digest of the canonical description; identical in every
            port for the same protocol.
        deprecated: Whether callers should migrate off this protocol.
        deprecation_message: What to migrate to.
        features: Open set of capability tokens.  Additive capabilities
            announce themselves here instead of consuming version numbers.
        methods: Every method, sorted by name.

    """

    protocol: str
    protocol_version: str
    protocol_hash: str
    deprecated: bool
    deprecation_message: str
    features: list[str]
    methods: list[MethodInfo]


@dataclasses.dataclass(frozen=True)
class ProtocolSummary(ArrowSerializableDataclass):
    """One hosted protocol, without its methods.

    Enough to decide whether to fetch the full description: a client that
    already knows a hash can skip the round trip entirely.

    Attributes:
        protocol: Wire name.
        protocol_version: Declared semver, or empty.
        protocol_hash: Digest of the canonical description.
        deprecated: Whether callers should migrate off this protocol.
        deprecation_message: What to migrate to.
        features: Open set of capability tokens.

    """

    protocol: str
    protocol_version: str
    protocol_hash: str
    deprecated: bool
    deprecation_message: str
    features: list[str]


@dataclasses.dataclass(frozen=True)
class ProtocolList(ArrowSerializableDataclass):
    """What this server hosts.

    A statement about a server, so this is where server identity lives.

    Attributes:
        server_id: Server instance identifier.
        server_version: Build version string.
        request_version: The framework's wire envelope version.
        protocols: Every hosted protocol, including reflection itself.

    """

    server_id: str
    server_version: str
    request_version: str
    protocols: list[ProtocolSummary]


class Reflection(Protocol):
    """Discovery, versioned by its own name.

    Kept to two methods on purpose.  ``list_protocols`` is the cheap question
    -- what is here, and has it changed -- and it is the only one a client
    needs on a warm path, since the hash answers "has it changed" without
    transferring any schema.  ``describe`` is the expensive one, asked once.
    """

    protocol_name: ClassVar[str] = "vgi_rpc.Reflection.v1"

    def list_protocols(self) -> ProtocolList:
        """Return every protocol this server hosts, with versions and hashes."""
        ...

    def describe(self, protocol: str) -> ServiceDescription:
        """Return one protocol's full description.

        Args:
            protocol: The wire name to describe.

        Returns:
            Its description.

        Raises:
            ProtocolNotSupportedError: This server does not host it.

        """
        ...


def _schema_ipc(schema: pa.Schema | None) -> bytes:
    """Serialize a schema, or return empty bytes when there is none.

    Empty rather than null: a nullable column costs every port a null check on
    a value it will never do anything with except treat as absent.
    """
    return b"" if schema is None else schema.serialize().to_pybytes()


class ReflectionImpl:
    """Answers reflection from the server's own bindings.

    Holds the server rather than a snapshot so it cannot drift from what
    dispatch actually routes to -- a description that disagrees with the
    dispatcher is worse than no description, because a client acts on it.

    Self-description is not special-cased: reflection's own binding is
    registered like any other and appears in its own output.  A client can
    therefore discover reflection the same way it discovers everything else,
    rather than having to know a priori what to ask.
    """

    __slots__ = ("_cache", "_server")

    def __init__(self, server: object) -> None:
        self._server = server
        # Descriptions are immutable for a server's life, so build each once.
        # This is the pre-built-batch optimisation the old __describe__ had,
        # kept -- but as a cache inside an ordinary implementation rather than
        # a fast path that skipped access logging and telemetry.
        self._cache: dict[str, ServiceDescription] = {}

    def list_protocols(self) -> ProtocolList:
        """Return every protocol this server hosts, with versions and hashes."""
        server = self._server
        return ProtocolList(
            server_id=server.server_id,  # type: ignore[attr-defined]
            server_version=server.server_version,  # type: ignore[attr-defined]
            request_version=_request_version(),
            protocols=[
                ProtocolSummary(
                    protocol=binding.name,
                    protocol_version=binding.version or "",
                    protocol_hash=binding.protocol_hash,
                    deprecated=False,
                    deprecation_message="",
                    features=[],
                )
                for binding in server.bindings.values()  # type: ignore[attr-defined]
            ],
        )

    def describe(self, protocol: str) -> ServiceDescription:
        """Return one protocol's full description.

        Args:
            protocol: The wire name to describe.

        Returns:
            Its description.

        Raises:
            ProtocolNotSupportedError: This server does not host it.

        """
        cached = self._cache.get(protocol)
        if cached is not None:
            return cached

        from ._common import ProtocolNotSupportedError

        binding = self._server.bindings.get(protocol)  # type: ignore[attr-defined]
        if binding is None:
            hosted = sorted(self._server.bindings)  # type: ignore[attr-defined]
            raise ProtocolNotSupportedError(f"This server does not host protocol {protocol!r}. Hosted: {hosted}.")

        description = ServiceDescription(
            protocol=binding.name,
            protocol_version=binding.version or "",
            protocol_hash=binding.protocol_hash,
            deprecated=False,
            deprecation_message="",
            features=[],
            methods=[
                MethodInfo(
                    name=info.name,
                    method_type=info.method_type.value,
                    has_return=bool(info.has_return),
                    has_header=info.header_type is not None,
                    stream_kind=_stream_kind(info),
                    params_schema_ipc=_schema_ipc(info.params_schema),
                    result_schema_ipc=_schema_ipc(info.result_schema if info.has_return else None),
                    header_schema_ipc=_schema_ipc(
                        info.header_type.ARROW_SCHEMA if info.header_type is not None else None
                    ),
                    idempotency=getattr(info, "idempotency", "unknown"),
                    deprecated=False,
                    deprecation_message="",
                )
                # Sorted so two ports iterating differently-ordered maps still
                # produce the same output.
                for _, info in sorted(binding.methods.items())
            ],
        )
        self._cache[protocol] = description
        return description


def _request_version() -> str:
    """Return the framework's wire envelope version as text."""
    from vgi_rpc.metadata import REQUEST_VERSION

    return REQUEST_VERSION.decode()


def _stream_kind(info: object) -> str:
    """Return the stream kind for *info*, or empty for a unary method."""
    from vgi_rpc.rpc import MethodType

    if info.method_type is not MethodType.STREAM:  # type: ignore[attr-defined]
        return ""
    is_exchange = info.is_exchange  # type: ignore[attr-defined]
    if is_exchange is None:
        return "unknown"
    return "exchange" if is_exchange else "producer"
