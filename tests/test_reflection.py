# Copyright 2025, 2026 Query Farm LLC - https://query.farm

"""``vgi_rpc.Reflection.v1`` — discovery as an ordinary co-hosted protocol."""

from __future__ import annotations

import dataclasses
from typing import ClassVar, Protocol

import pytest

from vgi_rpc.rpc import RpcServer
from vgi_rpc.rpc._common import ProtocolNotSupportedError
from vgi_rpc.rpc._reflection import (
    IDEMPOTENCY_LEVELS,
    MethodInfo,
    ProtocolList,
    ProtocolSummary,
    Reflection,
    ReflectionImpl,
    ServiceDescription,
)
from vgi_rpc.utils import ArrowSerializableDataclass


class _App(Protocol):
    """An application protocol for reflection to describe."""

    protocol_name: ClassVar[str] = "demo.App.v1"
    protocol_version: ClassVar[str] = "2.1.0"

    def echo(self, value: str) -> str:
        """Return the value."""
        ...

    def count(self) -> int:
        """Return a count."""
        ...


class _AppImpl:
    def echo(self, value: str) -> str:
        return value

    def count(self) -> int:
        return 0


def _server() -> RpcServer:
    return RpcServer(_App, _AppImpl(), enable_describe=True)


def _reflection(srv: RpcServer) -> ReflectionImpl:
    impl = srv.bindings["vgi_rpc.Reflection.v1"].impl
    assert isinstance(impl, ReflectionImpl)
    return impl


class TestRegistration:
    """Reflection is a binding, not a hardcoded method name."""

    def test_is_registered_as_a_protocol(self) -> None:
        """It routes on (protocol, method) like everything else."""
        assert "vgi_rpc.Reflection.v1" in _server().bindings

    def test_carries_its_major_version_in_its_name(self) -> None:
        """So an incompatible reflection is a 404, not a mis-parse."""
        assert Reflection.protocol_name.endswith(".v1")

    def test_is_exempt_from_the_version_gate(self) -> None:
        """The protocol a mismatched client calls to learn what mismatched.

        Gating it would deny the client the diagnosis it came for.
        """
        assert _server().bindings["vgi_rpc.Reflection.v1"].version_exempt

    def test_the_primary_protocol_is_still_the_application_one(self) -> None:
        """Reflection registers after the application protocols, never before.

        ``protocol_name``, ``protocol_hash`` and the landing-page title all
        report the primary, and none of them should start saying "Reflection".
        """
        srv = _server()
        assert srv.protocol_name == "demo.App.v1"

    def test_claims_the_reserved_prefix_that_applications_cannot(self) -> None:
        """Only the framework may register under ``vgi_rpc.``."""
        assert Reflection.protocol_name.startswith("vgi_rpc.")

    def test_absent_when_describe_is_disabled(self) -> None:
        """``enable_describe`` is now binding registration."""
        assert "vgi_rpc.Reflection.v1" not in RpcServer(_App, _AppImpl()).bindings


class TestListProtocols:
    """The cheap question: what is here, and has it changed."""

    def test_lists_every_hosted_protocol(self) -> None:
        """Including reflection itself -- self-description is not special-cased."""
        listed = {p.protocol for p in _reflection(_server()).list_protocols().protocols}
        assert listed == {"demo.App.v1", "vgi_rpc.Reflection.v1"}

    def test_reports_versions_and_hashes(self) -> None:
        """A client that already knows a hash can skip the describe round trip."""
        srv = _server()
        summary = next(p for p in _reflection(srv).list_protocols().protocols if p.protocol == "demo.App.v1")
        assert summary.protocol_version == "2.1.0"
        assert len(summary.protocol_hash) == 64

    def test_reports_server_identity(self) -> None:
        """Server identity lives here, not in a protocol's description.

        Two processes serving one protocol must describe it identically, or
        the description is not a property of the protocol.
        """
        srv = _server()
        assert _reflection(srv).list_protocols().server_id == srv.server_id

    def test_features_is_an_open_set(self) -> None:
        """Additive capabilities announce here instead of consuming versions."""
        for summary in _reflection(_server()).list_protocols().protocols:
            assert isinstance(summary.features, list)


class TestDescribe:
    """The expensive question, asked once."""

    def test_describes_an_application_protocol(self) -> None:
        """Methods are sorted so two ports iterating maps differently agree."""
        desc = _reflection(_server()).describe("demo.App.v1")
        assert [m.name for m in desc.methods] == ["count", "echo"]

    def test_describes_itself(self) -> None:
        """A client discovers reflection the way it discovers everything else."""
        desc = _reflection(_server()).describe("vgi_rpc.Reflection.v1")
        assert sorted(m.name for m in desc.methods) == ["describe", "list_protocols"]

    def test_unknown_protocol_is_not_supported(self) -> None:
        """Named, not silently empty: an empty description reads as "no methods"."""
        with pytest.raises(ProtocolNotSupportedError, match="does not host"):
            _reflection(_server()).describe("demo.Nope.v1")

    def test_carries_no_server_identity(self) -> None:
        """A protocol's description must not vary between processes."""
        assert not any(f.name in {"server_id", "server_version"} for f in dataclasses.fields(ServiceDescription))

    def test_schemas_travel_as_arrow_ipc(self) -> None:
        """The client's purpose in asking is a schema it can hand to Arrow."""
        import pyarrow as pa

        desc = _reflection(_server()).describe("demo.App.v1")
        echo = next(m for m in desc.methods if m.name == "echo")
        schema = pa.ipc.read_schema(pa.py_buffer(echo.params_schema_ipc))
        assert schema.names == ["value"]

    def test_absent_schemas_are_empty_not_null(self) -> None:
        """A nullable column costs every port a null check on a value it ignores."""
        desc = _reflection(_server()).describe("demo.App.v1")
        assert all(m.header_schema_ipc == b"" for m in desc.methods)

    def test_repeated_calls_return_the_cached_description(self) -> None:
        """The pre-built-batch optimisation survives, as an ordinary cache.

        The old ``__describe__`` fast path skipped dispatch entirely, and with
        it access logging and telemetry.  This keeps the saving without the
        blind spot.
        """
        impl = _reflection(_server())
        assert impl.describe("demo.App.v1") is impl.describe("demo.App.v1")

    def test_idempotency_defaults_to_unknown(self) -> None:
        """A caller must assume the worst until a protocol says otherwise."""
        desc = _reflection(_server()).describe("demo.App.v1")
        assert all(m.idempotency == "unknown" for m in desc.methods)
        assert IDEMPOTENCY_LEVELS[0] == "unknown"


class TestTolerantDecoding:
    """Minor skew must be survivable -- the load-bearing property.

    A strict decoder would fail at exactly the moment a client most needs a
    good answer: when it is talking to a server it does not fully understand.
    These assert the behaviour rather than relying on it, because it is a
    requirement every port has to meet, not an accident of one codec.
    """

    def test_a_newer_server_decodes_against_an_older_client(self) -> None:
        """Extra columns are ignored, so v1.1 -> v1.0 works."""

        @dataclasses.dataclass(frozen=True)
        class _Extended(ArrowSerializableDataclass):
            protocol: str
            protocol_version: str
            protocol_hash: str
            deprecated: bool
            deprecation_message: str
            features: list[str]
            methods: list[MethodInfo]
            # A field a future minor might add.
            documentation_url: str = ""

        wire = _Extended(
            protocol="demo.App.v1",
            protocol_version="2.1.0",
            protocol_hash="0" * 64,
            deprecated=False,
            deprecation_message="",
            features=["streaming"],
            methods=[],
            documentation_url="https://example.invalid",
        ).serialize_to_bytes()

        decoded = ServiceDescription.deserialize_from_bytes(wire)
        assert decoded.protocol == "demo.App.v1"
        assert decoded.features == ["streaming"]

    def test_an_older_server_decodes_against_a_newer_client(self) -> None:
        """Absent columns take their defaults, so v1.0 -> v1.1 works."""

        @dataclasses.dataclass(frozen=True)
        class _Trimmed(ArrowSerializableDataclass):
            name: str
            method_type: str
            has_return: bool
            has_header: bool
            stream_kind: str
            params_schema_ipc: bytes
            result_schema_ipc: bytes
            header_schema_ipc: bytes

        wire = _Trimmed(
            name="echo",
            method_type="unary",
            has_return=True,
            has_header=False,
            stream_kind="",
            params_schema_ipc=b"",
            result_schema_ipc=b"",
            header_schema_ipc=b"",
        ).serialize_to_bytes()

        decoded = MethodInfo.deserialize_from_bytes(wire)
        assert decoded.name == "echo"
        assert decoded.idempotency == "unknown"
        assert decoded.deprecated is False

    def test_a_field_added_without_a_default_is_not_survivable(self) -> None:
        """Which is exactly why every field added in a minor must carry one.

        The decoder defaults what has a default and raises for what does not.
        That is the right behaviour -- silently zero-filling a required field
        would hand a client a description that is wrong rather than absent --
        and it makes "new fields get defaults" a rule the ports must follow,
        not a style preference.
        """
        import io

        import pyarrow as pa

        schema = pa.schema([pa.field("protocol", pa.string(), nullable=False)])
        batch = pa.RecordBatch.from_pydict({"protocol": ["demo.App.v1"]}, schema=schema)
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, schema) as writer:
            writer.write_batch(batch)

        with pytest.raises(ValueError, match="Missing fields"):
            ServiceDescription.deserialize_from_bytes(buf.getvalue())

    def test_decoding_is_by_field_name_not_position(self) -> None:
        """A port emitting fields in another order must still be readable."""
        import io

        import pyarrow as pa

        # Declaration order deliberately scrambled relative to the dataclass.
        fields = list(ServiceDescription.ARROW_SCHEMA)
        schema = pa.schema(list(reversed(fields)))
        batch = pa.RecordBatch.from_pydict(
            {
                "protocol_hash": ["a" * 64],
                "deprecation_message": [""],
                "protocol": ["demo.App.v1"],
                "features": [[]],
                "deprecated": [False],
                "methods": [[]],
                "protocol_version": ["2.1.0"],
            },
            schema=schema,
        )
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, schema) as writer:
            writer.write_batch(batch)

        decoded = ServiceDescription.deserialize_from_bytes(buf.getvalue())
        assert decoded.protocol == "demo.App.v1"
        assert decoded.protocol_hash == "a" * 64


class TestGeneratedTypes:
    """The payload is a generated schema, not a hand-built batch.

    That is the whole point: describe stops being a bespoke format
    hand-maintained in six languages and rides the same codegen as every other
    method.
    """

    @pytest.mark.parametrize("cls", [MethodInfo, ServiceDescription, ProtocolSummary, ProtocolList])
    def test_has_a_generated_schema(self, cls: type[ArrowSerializableDataclass]) -> None:
        """Each type carries an ARROW_SCHEMA the ports generate from."""
        assert cls.ARROW_SCHEMA is not None

    @pytest.mark.parametrize("cls", [MethodInfo, ServiceDescription, ProtocolSummary, ProtocolList])
    def test_round_trips(self, cls: type[ArrowSerializableDataclass]) -> None:
        """Serialize and deserialize agree, which the ports must reproduce."""
        srv = _server()
        impl = _reflection(srv)
        value: ArrowSerializableDataclass = (
            impl.list_protocols() if cls in (ProtocolList,) else impl.describe("demo.App.v1")
        )
        if cls is ProtocolSummary:
            value = impl.list_protocols().protocols[0]
        elif cls is MethodInfo:
            value = impl.describe("demo.App.v1").methods[0]
        assert type(value).deserialize_from_bytes(value.serialize_to_bytes()) == value
