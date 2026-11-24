# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Protocol version negotiation (``tools/cross-port/specs/PROTOCOL_VERSIONS_V1.md``).

The cross-language contract is in ``vgi_rpc/conformance/_protocol_versions_pytest.py``;
these cover the reference's own pieces: parsing, the server's window and
override, and the pipe and HTTP clients' negotiation state.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import Generator
from typing import Any, ClassVar, Protocol, cast

import pyarrow as pa
import pytest

from vgi_rpc.metadata import PROTOCOL_VERSION_KEY
from vgi_rpc.protocol_versions import (
    PROTOCOL_VERSION_UNSUPPORTED,
    ClientVersioning,
    EndpointNegotiation,
    NoCommonVersionError,
    VersionRange,
    format_versions,
    highest_common,
    is_retry_safe,
    is_version_refusal,
    legacy_server_version,
    parse_versions,
)
from vgi_rpc.rpc import ProtocolVersionError, RpcConnection, RpcError, RpcServer, make_pipe_pair
from vgi_rpc.rpc._common import _current_request_metadata

# ---------------------------------------------------------------------------
# Protocols
# ---------------------------------------------------------------------------


class _Server34(Protocol):
    """The server side: speaks exactly 3.4."""

    protocol_name: ClassVar[str] = "demo.Versioned.v3"
    protocol_version: ClassVar[str] = "3.4.1"

    def ping(self) -> str:
        """Return the version the server received."""
        ...


class _Client35(Protocol):
    """A newer client that can still speak 3.4."""

    protocol_name: ClassVar[str] = "demo.Versioned.v3"
    protocol_version: ClassVar[str] = "3.5.2"
    protocol_version_range: ClassVar[str] = "3.4-3.5"

    def ping(self) -> str:
        """Return the version the server received."""
        ...


class _Client36(Protocol):
    """A client with no overlap with 3.4."""

    protocol_name: ClassVar[str] = "demo.Versioned.v3"
    protocol_version: ClassVar[str] = "3.6.0"
    protocol_version_range: ClassVar[str] = "3.5-3.6"

    def ping(self) -> str:
        """Return the version the server received."""
        ...


class _Unversioned(Protocol):
    """Declares no version: never gated, never advertised."""

    protocol_name: ClassVar[str] = "demo.Free.v1"

    def hello(self) -> str:
        """Say hello."""
        ...


class _Impl:
    def ping(self) -> str:
        md = _current_request_metadata.get()
        raw = md.get(PROTOCOL_VERSION_KEY) if md is not None else None
        return raw.decode() if raw is not None else ""

    def hello(self) -> str:
        return "hi"


# ---------------------------------------------------------------------------
# Syntax
# ---------------------------------------------------------------------------


class TestSyntax:
    """``name=MIN-MAX[, ...]``."""

    def test_round_trip(self) -> None:
        """Format then parse is the identity, in order."""
        ranges = {"vgi.v3": VersionRange((3, 4), (3, 4)), "vgi.secret.v1": VersionRange((1, 0), (1, 2))}
        text = format_versions(ranges)
        assert text == "vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.2"
        assert parse_versions(text) == ranges

    def test_tolerant(self) -> None:
        """Malformed entries are skipped, whitespace is free, a single version means V-V."""
        parsed = parse_versions(b" a=1.0-1.1 ,bad, b=2.0 , c=3.x-3.4, d=2.0-1.0, a=9.9-9.9, =1.0")
        assert parsed == {"a": VersionRange((1, 0), (1, 1)), "b": VersionRange((2, 0), (2, 0))}
        assert parse_versions(None) == {}
        assert parse_versions("") == {}

    def test_semver_ends_are_accepted(self) -> None:
        """A full semver end drops its patch."""
        assert VersionRange.parse("3.4.7-3.5.0") == VersionRange((3, 4), (3, 5))
        assert VersionRange.exactly("3.4.9") == VersionRange((3, 4), (3, 4))

    def test_inverted_is_rejected(self) -> None:
        """``min`` above ``max`` is not a window."""
        with pytest.raises(ValueError, match="min above max"):
            VersionRange.parse("3.5-3.4")

    def test_membership_and_overlap(self) -> None:
        """``in`` compares major.minor; overlap picks the highest common version."""
        rng = VersionRange((3, 3), (3, 4))
        assert (3, 3) in rng and (3, 4, 9) in rng and (3, 5) not in rng and "3.4" not in rng
        assert highest_common(VersionRange((3, 4), (3, 5)), VersionRange((3, 3), (3, 4))) == (3, 4)
        assert highest_common(VersionRange((3, 5), (3, 6)), VersionRange((3, 4), (3, 4))) is None
        assert str(rng) == "3.3-3.4"


# ---------------------------------------------------------------------------
# Declarations
# ---------------------------------------------------------------------------


class TestDeclarations:
    """``protocol_version`` plus the optional ``protocol_version_range``."""

    def test_default_window_is_the_version(self) -> None:
        """No range: exactly the declared major.minor."""
        v = ClientVersioning.from_protocol(_Server34, "demo.Versioned.v3")
        assert v is not None and v.range == VersionRange((3, 4), (3, 4)) and v.patch == 1
        assert v.advertised() == "demo.Versioned.v3=3.4-3.4"
        assert v.wire_version((3, 4)) == "3.4.1"

    def test_declared_window(self) -> None:
        """A range widens downward."""
        v = ClientVersioning.from_protocol(_Client35, "demo.Versioned.v3")
        assert v is not None and v.range == VersionRange((3, 4), (3, 5))

    def test_unversioned(self) -> None:
        """No version: nothing to negotiate."""
        assert ClientVersioning.from_protocol(_Unversioned, "demo.Free.v1") is None

    def test_range_must_end_at_the_version(self) -> None:
        """``protocol_version`` is the newest spoken; a range above it is a contradiction."""

        class _Bad(Protocol):
            protocol_version: ClassVar[str] = "3.4.0"
            protocol_version_range: ClassVar[str] = "3.4-3.5"

            def ping(self) -> str: ...

        with pytest.raises(ValueError, match="must end at protocol_version"):
            ClientVersioning.from_protocol(_Bad, "x")
        with pytest.raises(ValueError, match="must end at protocol_version"):
            RpcServer(_Bad, _Impl())

    def test_range_must_be_text(self) -> None:
        """A non-string range is a type error."""

        class _Bad(Protocol):
            protocol_version: ClassVar[str] = "3.4.0"
            protocol_version_range: ClassVar[object] = 3.4

            def ping(self) -> str: ...

        with pytest.raises(TypeError, match="protocol_version_range must be a str"):
            ClientVersioning.from_protocol(_Bad, "x")

    def test_version_must_be_text(self) -> None:
        """A non-string version is a type error."""

        class _Bad(Protocol):
            protocol_version: ClassVar[object] = 3

            def ping(self) -> str: ...

        with pytest.raises(TypeError, match="protocol_version must be a str"):
            ClientVersioning.from_protocol(_Bad, "x")


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


class TestServerWindow:
    """What the server accepts and advertises."""

    def test_advertisement_lists_only_gated_protocols(self) -> None:
        """Primary first; unversioned and reflection absent."""
        srv = RpcServer(_Server34, _Impl(), extra_protocols=[(_Unversioned, _Impl())], enable_describe=True)
        assert srv.protocol_versions == "demo.Versioned.v3=3.4-3.4"

    def test_nothing_versioned_is_empty(self) -> None:
        """Present-but-empty, not absent."""
        assert RpcServer(_Unversioned, _Impl()).protocol_versions == ""

    def test_override_widens_and_gates(self) -> None:
        """``protocol_version_ranges`` replaces the window, for the gate and the advertisement."""
        srv = RpcServer(_Server34, _Impl(), protocol_version_ranges={"demo.Versioned.v3": "3.3-3.4"})
        assert srv.protocol_versions == "demo.Versioned.v3=3.3-3.4"
        for ok in (b"3.3.0", b"3.4.9"):
            self._gate(srv, ok)
        with pytest.raises(ProtocolVersionError) as info:
            self._gate(srv, b"3.2.0")
        assert "too old" in str(info.value)
        assert info.value.error_kind == PROTOCOL_VERSION_UNSUPPORTED

    def test_override_accepts_a_range_object(self) -> None:
        """A :class:`VersionRange` is accepted as well as text."""
        srv = RpcServer(_Server34, _Impl(), protocol_version_ranges={"demo.Versioned.v3": VersionRange((3, 3), (3, 3))})
        assert srv.protocol_versions == "demo.Versioned.v3=3.3-3.3"

    @pytest.mark.parametrize("name", ["demo.Free.v1", "nope", "vgi_rpc.Reflection.v1"])
    def test_override_of_an_ungated_protocol_raises(self, name: str) -> None:
        """Only a hosted, versioned, gated protocol has a window to override."""
        with pytest.raises(ValueError, match="not a hosted protocol that declares a protocol_version"):
            RpcServer(
                _Server34,
                _Impl(),
                extra_protocols=[(_Unversioned, _Impl())],
                enable_describe=True,
                protocol_version_ranges={name: "1.0-1.0"},
            )

    def test_refusal_carries_both_windows(self) -> None:
        """The message quotes the client's advertised window; the details carry the server's."""
        srv = RpcServer(_Server34, _Impl())
        with pytest.raises(ProtocolVersionError) as info:
            self._gate(srv, b"3.6.0", client=b"demo.Versioned.v3=3.5-3.6")
        exc = info.value
        assert "client speaks demo.Versioned.v3 3.6 (supports 3.5-3.6)" in str(exc)
        assert "this server supports demo.Versioned.v3 3.4-3.4" in str(exc)
        assert "server is too old" in str(exc) and "too new" in str(exc)
        assert exc.error_metadata == {b"vgi_rpc.protocol_versions": b"demo.Versioned.v3=3.4-3.4"}
        kinds = [type(d).__name__ for d in exc.error_details]
        assert kinds == ["PreconditionFailure", "ErrorInfo"]

    def test_bare_error_has_no_details(self) -> None:
        """An error built without context carries nothing machine-readable."""
        exc = ProtocolVersionError("x")
        assert exc.error_details == [] and exc.error_metadata == {}

    @staticmethod
    def _gate(srv: RpcServer, version: bytes, client: bytes | None = None) -> None:
        pairs = {PROTOCOL_VERSION_KEY: version}
        if client is not None:
            pairs[b"vgi_rpc.protocol_versions_client"] = client
        token = _current_request_metadata.set(pa.KeyValueMetadata(pairs))
        try:
            srv.gate_version(srv.methods["ping"])
        finally:
            _current_request_metadata.reset(token)


# ---------------------------------------------------------------------------
# Negotiation state
# ---------------------------------------------------------------------------


def _client35() -> ClientVersioning:
    v = ClientVersioning.from_protocol(_Client35, "demo.Versioned.v3")
    assert v is not None
    return v


class TestEndpointNegotiation:
    """The §6 decision."""

    def test_nothing_seen_sends_the_max(self) -> None:
        """Before any response: the client's newest."""
        n = EndpointNegotiation()
        assert n.wire_version(_client35()) == "3.5.2"
        assert not n.has_advertisement

    def test_absent_value_is_ignored(self) -> None:
        """A response with no header teaches nothing."""
        n = EndpointNegotiation()
        n.observe(None)
        assert not n.has_advertisement and n.advertised == {}

    def test_advertised_window_picks_the_highest_common(self) -> None:
        """Server 3.3-3.4 against client 3.4-3.5: 3.4."""
        n = EndpointNegotiation("http://w")
        n.observe("demo.Versioned.v3=3.3-3.4, other=1.0-1.0")
        assert n.wire_version(_client35()) == "3.4.2"
        assert n.endpoint == "http://w"

    def test_unlisted_protocol_sends_the_max(self) -> None:
        """A server that does not gate this protocol: today's behaviour."""
        n = EndpointNegotiation()
        n.observe("other=1.0-1.0")
        assert n.wire_version(_client35()) == "3.5.2"

    @pytest.mark.parametrize(
        ("server", "direction"),
        [("demo.Versioned.v3=3.2-3.3", "server is too old"), ("demo.Versioned.v3=3.6-3.7", "client is too old")],
    )
    def test_disjoint_raises_naming_both(self, server: str, direction: str) -> None:
        """No common version: the §6 message, before anything is sent."""
        n = EndpointNegotiation("http://w")
        n.observe(server)
        with pytest.raises(NoCommonVersionError) as info:
            n.wire_version(_client35())
        text = str(info.value)
        assert "3.4-3.5" in text and server.split("=")[1] in text and "at http://w" in text
        assert direction in text.lower()
        assert info.value.error_kind == PROTOCOL_VERSION_UNSUPPORTED
        assert isinstance(info.value, ProtocolVersionError)

    def test_refusal_with_windows_is_learned(self) -> None:
        """A refusal's ErrorInfo replaces what was known."""
        n = EndpointNegotiation()
        n.observe_refusal("demo.Versioned.v3", self._refusal(info="demo.Versioned.v3=3.4-3.4"))
        assert n.wire_version(_client35()) == "3.4.2"

    def test_legacy_refusal_names_the_version(self) -> None:
        """A 3.4.0-era refusal: exactly the version it names."""
        n = EndpointNegotiation()
        err = self._refusal(kind="protocol_version_mismatch", message="mismatch\n  Client: 3.5.2\n  Server: 3.4.0\n")
        assert legacy_server_version(err) == (3, 4)
        n.observe_refusal("demo.Versioned.v3", err)
        assert n.wire_version(_client35()) == "3.4.2"

    def test_legacy_refusal_outside_the_window(self) -> None:
        """A legacy version the client cannot speak is no common version."""
        n = EndpointNegotiation()
        n.observe_refusal("demo.Versioned.v3", self._refusal(kind="protocol_version_mismatch", message="Server: 2.0.0"))
        with pytest.raises(NoCommonVersionError):
            n.wire_version(_client35())

    def test_legacy_refusal_via_precondition_text(self) -> None:
        """The pre-spec precondition description is a source too."""
        details = (
            '[{"@type": "vgi_rpc.PreconditionFailure", "violations": [{"type": "protocol_version", '
            '"subject": "demo.Versioned.v3", "description": "client declares 3.5.2, server requires 3.4.0; '
            'major and minor must match"}]}]'
        )
        err = self._refusal(kind="protocol_version_mismatch", message="nope", details=details)
        assert legacy_server_version(err) == (3, 4)

    def test_unrelated_errors_teach_nothing(self) -> None:
        """Only version refusals are read."""
        err = self._refusal(kind="widget_missing", message="Server: 3.4.0")
        assert legacy_server_version(err) is None
        assert not is_version_refusal(err)
        n = EndpointNegotiation()
        n.observe_refusal("demo.Versioned.v3", err)
        assert n.wire_version(_client35()) == "3.5.2"

    def test_retry_safety(self) -> None:
        """Reflection and declared-idempotent methods only."""
        assert is_retry_safe("vgi_rpc.Reflection.v1", "unknown")
        assert is_retry_safe("x", "no_side_effects") and is_retry_safe("x", "idempotent")
        assert not is_retry_safe("x", "unknown")

    @staticmethod
    def _refusal(
        *, kind: str = PROTOCOL_VERSION_UNSUPPORTED, message: str = "refused", info: str = "", details: str = ""
    ) -> RpcError:
        import json

        if info:
            details = json.dumps([{"@type": "vgi_rpc.ErrorInfo", "metadata": {"vgi_rpc.protocol_versions": info}}])
        return RpcError(
            "ProtocolVersionError",
            message,
            "",
            error_kind=kind,
            error_code="FAILED_PRECONDITION",
            error_details=json.loads(details) if details else [],
        )


# ---------------------------------------------------------------------------
# Clients end to end
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def _pipe(client_protocol: type, server: RpcServer) -> Generator[Any]:
    """Serve *server* on an in-process pipe; yield a proxy bound to *client_protocol*."""
    client_transport, server_transport = make_pipe_pair()
    thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
    thread.start()
    try:
        connection: RpcConnection[Any] = RpcConnection(client_protocol, client_transport)
        with connection as proxy:
            yield proxy
    finally:
        client_transport.close()
        thread.join(timeout=5)
        server_transport.close()


class TestPipeClient:
    """Byte-stream transports learn the windows from reflection before the first call."""

    def test_wide_client_reads_describe_first(self) -> None:
        """A 3.4-3.5 client against a 3.4 server speaks 3.4 on its first call."""
        with _pipe(_Client35, RpcServer(_Server34, _Impl(), enable_describe=True)) as proxy:
            assert proxy.ping() == "3.4.2"

    def test_without_reflection_the_refusal_teaches(self) -> None:
        """No reflection to probe: the first call is refused, the second speaks 3.4."""
        with _pipe(_Client35, RpcServer(_Server34, _Impl())) as proxy:
            with pytest.raises(RpcError) as info:
                proxy.ping()
            assert info.value.error_kind == PROTOCOL_VERSION_UNSUPPORTED
            assert proxy.ping() == "3.4.2"

    def test_disjoint_surfaces_the_server_refusal_then_fails_before_sending(self) -> None:
        """First the server's refusal (both windows named); then a client-side failure."""
        with _pipe(_Client36, RpcServer(_Server34, _Impl(), enable_describe=True)) as proxy:
            with pytest.raises(NoCommonVersionError) as info:
                proxy.ping()
            assert "3.5-3.6" in str(info.value) and "3.4-3.4" in str(info.value)

    def test_single_version_client_does_not_probe(self) -> None:
        """Nothing to choose: no extra round trip, the declared version goes out."""
        with _pipe(_Server34, RpcServer(_Server34, _Impl(), enable_describe=True)) as proxy:
            assert proxy.ping() == "3.4.1"
            assert not proxy._negotiation.has_advertisement


class TestHttpClient:
    """HTTP learns from every response's header."""

    def test_header_teaches_the_endpoint(self) -> None:
        """First call refused (not idempotent, so surfaced), second at the common version."""
        from vgi_rpc.http import http_connect, make_sync_client

        client = make_sync_client(RpcServer(_Server34, _Impl()), token_key=b"k" * 32)
        with http_connect(_Client35, client=client) as proxy:
            with pytest.raises(RpcError) as info:
                proxy.ping()
            assert info.value.error_kind == PROTOCOL_VERSION_UNSUPPORTED
            assert proxy.ping() == "3.4.2"

    def test_idempotent_call_is_retried_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A call declared safe is re-sent once at the renegotiated version."""
        import vgi_rpc.http._client as client_mod
        from vgi_rpc.http import http_connect, make_sync_client

        monkeypatch.setattr(client_mod, "is_retry_safe", lambda protocol, idempotency: True)
        client = make_sync_client(RpcServer(_Server34, _Impl()), token_key=b"k" * 32)
        with http_connect(_Client35, client=client) as proxy:
            assert proxy.ping() == "3.4.2"

    def test_disjoint_fails_before_sending_once_learned(self) -> None:
        """The server's refusal first, then the client-side §6 failure."""
        from vgi_rpc.http import http_connect, make_sync_client

        client = make_sync_client(RpcServer(_Server34, _Impl()), token_key=b"k" * 32)
        with http_connect(_Client36, client=client) as proxy:
            with pytest.raises(RpcError) as first:
                proxy.ping()
            assert "3.5-3.6" in first.value.error_message and "3.4-3.4" in first.value.error_message
            with pytest.raises(NoCommonVersionError):
                proxy.ping()

    def test_client_advertises_its_window(self) -> None:
        """``vgi_rpc.protocol_versions_client`` rides on every request (the header too, on HTTP)."""
        from vgi_rpc.http import http_connect, make_sync_client

        class _Echo:
            def ping(self) -> str:
                md = _current_request_metadata.get()
                raw = md.get(b"vgi_rpc.protocol_versions_client") if md is not None else None
                return raw.decode() if raw is not None else ""

        client = make_sync_client(RpcServer(_Client35, _Echo()), token_key=b"k" * 32)
        with http_connect(_Client35, client=client) as proxy:
            assert proxy.ping() == "demo.Versioned.v3=3.4-3.5"
            headers = cast("Any", proxy)._build_headers()
            assert headers["VGI-Client-Protocol-Versions"] == "demo.Versioned.v3=3.4-3.5"
        with _pipe(_Client35, RpcServer(_Client35, _Echo())) as pipe_proxy:
            assert pipe_proxy.ping() == "demo.Versioned.v3=3.4-3.5"
