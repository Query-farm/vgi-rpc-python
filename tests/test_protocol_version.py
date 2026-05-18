"""Tests for the per-protocol version mechanism.

Covers:
- ``parse_version`` regex + edge cases.
- ``RpcServer._check_protocol_version`` comparison matrix (exact major+minor,
  patch ignored; directional error messages on mismatch).
- ``vars()`` vs ``getattr`` inheritance semantics — subclasses must not
  silently inherit a parent Protocol's ``protocol_version``.
- ``RpcServer.__init__`` and ``_RpcProxy.__init__`` validate semver at
  construction time, not first RPC.
- The dispatch-boundary check raises ``ProtocolVersionError`` (a subclass of
  ``VersionError``) on mismatch; ``__describe__`` is exempt.
"""

from __future__ import annotations

import contextlib
import io
import threading
from typing import ClassVar, Protocol

import pytest

from vgi_rpc.metadata import SEMVER_REGEX, parse_version
from vgi_rpc.rpc import ProtocolVersionError, RpcError, RpcServer, VersionError
from vgi_rpc.rpc._client import _RpcProxy
from vgi_rpc.rpc._transport import make_pipe_pair


class _BaseProto(Protocol):
    """Test Protocol that declares protocol_version=1.2.3 — the inheritance baseline."""

    protocol_version: ClassVar[str] = "1.2.3"

    def greet(self, name: str) -> str: ...


class _Impl:
    """Trivial implementation: greet returns 'hi {name}'."""

    def greet(self, name: str) -> str:
        """Return a greeting."""
        return f"hi {name}"


class _UndeclaredProto(Protocol):
    """Test Protocol with no protocol_version — opt-out path."""

    def greet(self, name: str) -> str: ...


class _SubProto(_BaseProto, Protocol):
    """Subclass that does NOT redeclare protocol_version.

    vars()-not-getattr inheritance check: a server constructed against
    _SubProto must see protocol_version as None (opt-out), even though
    ``getattr(_SubProto, 'protocol_version')`` returns ``'1.2.3'``.
    """


class TestParseVersion:
    """Tests for parse_version() and the canonical semver regex."""

    def test_valid_canonical_forms(self) -> None:
        """Standard MAJOR.MINOR.PATCH triples parse to (major, minor, patch)."""
        assert parse_version("1.0.0") == (1, 0, 0)
        assert parse_version("0.0.0") == (0, 0, 0)
        assert parse_version("12.345.6789") == (12, 345, 6789)

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "1",
            "1.0",
            "1.0.0.0",
            "1.0.0-rc1",
            "1.0.0+build",
            "v1.0.0",
            "banana",
            "01.0.0",  # leading zero
            "1.02.0",
            "1.0.03",
            "-1.0.0",
            "1.-1.0",
            " 1.0.0",
            "1.0.0 ",
        ],
    )
    def test_invalid_forms_rejected(self, value: str) -> None:
        """Anything that isn't canonical semver raises ValueError."""
        with pytest.raises(ValueError):
            parse_version(value)

    def test_regex_matches_parse(self) -> None:
        """SEMVER_REGEX is the source of truth — matches parse_version's accept set."""
        assert SEMVER_REGEX.match("1.0.0") is not None
        assert SEMVER_REGEX.match("1.0.0-rc1") is None


class TestServerInheritance:
    """vars() vs getattr inheritance semantics for protocol_version."""

    def test_base_protocol_version_visible(self) -> None:
        """A Protocol declaring protocol_version makes it visible on RpcServer."""
        server = RpcServer(_BaseProto, _Impl())
        assert server.protocol_version == "1.2.3"

    def test_subclass_does_not_inherit(self) -> None:
        """Subclasses that don't redeclare protocol_version do not inherit it.

        getattr would surface the parent's value; vars() does not.
        """
        # Sanity: getattr leaks, vars does not. The literal-attribute call here
        # is deliberate — replacing it with attribute access would obscure the
        # contrast against vars() that this test exists to prove.
        assert getattr(_SubProto, "protocol_version") == "1.2.3"  # noqa: B009
        assert vars(_SubProto).get("protocol_version") is None
        # The server reads via vars() — opt-out is silent.
        server = RpcServer(_SubProto, _Impl())
        assert server.protocol_version is None

    def test_undeclared_protocol_is_opt_out(self) -> None:
        """A Protocol with no protocol_version yields server.protocol_version == None."""
        server = RpcServer(_UndeclaredProto, _Impl())
        assert server.protocol_version is None

    def test_proxy_inheritance_semantics_match(self) -> None:
        """_RpcProxy must use vars(), not getattr — symmetric with server."""

        class _FakeTransport:
            """Stand-in transport: _RpcProxy reads from it but never writes here."""

            writer = io.BytesIO()
            reader = io.BytesIO()
            shm = None

            def close(self) -> None:
                """No-op close — pytest-fixture lifecycle, not real I/O."""

        sub_proxy = _RpcProxy(_SubProto, _FakeTransport())
        assert sub_proxy._protocol_version is None
        base_proxy = _RpcProxy(_BaseProto, _FakeTransport())
        assert base_proxy._protocol_version == "1.2.3"


class TestConstructionValidation:
    """Both client and server validate semver format at construction, not first RPC."""

    def test_server_rejects_malformed_at_construction(self) -> None:
        """RpcServer raises ValueError on a Protocol with a non-semver protocol_version."""

        class _BadProto(Protocol):
            protocol_version: ClassVar[str] = "banana"

            def greet(self, name: str) -> str: ...

        with pytest.raises(ValueError, match="Invalid protocol version"):
            RpcServer(_BadProto, _Impl())

    def test_server_rejects_non_string(self) -> None:
        """RpcServer raises TypeError when protocol_version is declared as a non-str."""

        class _BadType(Protocol):
            protocol_version: ClassVar[int] = 1

            def greet(self, name: str) -> str: ...

        with pytest.raises(TypeError, match="must be a str"):
            RpcServer(_BadType, _Impl())

    def test_client_rejects_malformed_at_construction(self) -> None:
        """_RpcProxy raises ValueError on a Protocol with a non-semver protocol_version."""

        class _BadProto(Protocol):
            protocol_version: ClassVar[str] = "1.0.0-rc1"

            def greet(self, name: str) -> str: ...

        class _FakeTransport:
            """Stand-in transport — proxy construction fails before any I/O."""

            writer = io.BytesIO()
            reader = io.BytesIO()
            shm = None

            def close(self) -> None:
                """No-op close."""

        with pytest.raises(ValueError, match="Invalid protocol version"):
            _RpcProxy(_BadProto, _FakeTransport())


class TestComparisonRule:
    """RpcServer._check_protocol_version: exact major+minor, patch ignored."""

    def _server(self, version: str) -> RpcServer:
        """Build an RpcServer whose Protocol declares protocol_version=*version*."""

        class _Proto(Protocol):
            protocol_version: ClassVar[str] = version

            def greet(self, name: str) -> str: ...

        return RpcServer(_Proto, _Impl())

    def test_exact_match_passes(self) -> None:
        """Identical client/server semver strings -> no raise."""
        srv = self._server("1.2.3")
        srv._check_protocol_version(b"1.2.3")

    def test_patch_ignored(self) -> None:
        """Patch component is ignored — any matching major+minor passes."""
        srv = self._server("1.2.3")
        srv._check_protocol_version(b"1.2.0")
        srv._check_protocol_version(b"1.2.99")

    def test_minor_mismatch_rejected_with_direction_client_too_old(self) -> None:
        """Client minor < server minor -> 'upgrade the client' directive."""
        srv = self._server("1.5.0")
        with pytest.raises(ProtocolVersionError) as exc_info:
            srv._check_protocol_version(b"1.2.0")
        msg = str(exc_info.value)
        assert "Client: 1.2.0" in msg
        assert "Server: 1.5.0" in msg
        assert "upgrade the VGI extension/client" in msg

    def test_minor_mismatch_rejected_with_direction_server_too_old(self) -> None:
        """Client minor > server minor -> 'upgrade the worker' directive."""
        srv = self._server("1.2.0")
        with pytest.raises(ProtocolVersionError) as exc_info:
            srv._check_protocol_version(b"1.5.0")
        msg = str(exc_info.value)
        assert "Client: 1.5.0" in msg
        assert "Server: 1.2.0" in msg
        assert "upgrade the VGI worker" in msg

    def test_major_mismatch_client_too_old(self) -> None:
        """Client major < server major -> 'upgrade the client'."""
        srv = self._server("2.0.0")
        with pytest.raises(ProtocolVersionError) as exc_info:
            srv._check_protocol_version(b"1.9.9")
        assert "upgrade the VGI extension/client" in str(exc_info.value)

    def test_major_mismatch_server_too_old(self) -> None:
        """Client major > server major -> 'upgrade the worker'."""
        srv = self._server("1.2.0")
        with pytest.raises(ProtocolVersionError) as exc_info:
            srv._check_protocol_version(b"2.0.0")
        assert "upgrade the VGI worker" in str(exc_info.value)

    def test_malformed_client_version_rejected(self) -> None:
        """Non-semver client metadata raises ProtocolVersionError with a 'malformed' hint."""
        srv = self._server("1.2.0")
        with pytest.raises(ProtocolVersionError, match="malformed protocol_version"):
            srv._check_protocol_version(b"banana")

    def test_missing_client_version_rejected(self) -> None:
        """A versioned server rejects requests with no protocol_version metadata key."""
        srv = self._server("1.2.0")
        with pytest.raises(ProtocolVersionError, match="did not send"):
            srv._check_protocol_version(None)

    def test_undecodable_bytes_rejected(self) -> None:
        """Non-UTF-8 client metadata raises ProtocolVersionError with a clear hint."""
        srv = self._server("1.2.0")
        with pytest.raises(ProtocolVersionError, match="non-UTF-8"):
            srv._check_protocol_version(b"\xff\xfe")

    def test_protocol_version_error_is_version_error_subclass(self) -> None:
        """Existing catch sites for VersionError must keep working (subclass relationship)."""
        srv = self._server("1.2.0")
        with pytest.raises(VersionError):
            srv._check_protocol_version(None)


# --- Wire-level smoke tests --------------------------------------------------
# Module-level Protocols (typing.Protocol doesn't compose well with type()).


class _ProtoV100(Protocol):
    """Test Protocol declaring protocol_version=1.0.0."""

    protocol_version: ClassVar[str] = "1.0.0"

    def greet(self, name: str) -> str: ...


class _ProtoV200(Protocol):
    """Test Protocol declaring protocol_version=2.0.0."""

    protocol_version: ClassVar[str] = "2.0.0"

    def greet(self, name: str) -> str: ...


class _ProtoNoVersion(Protocol):
    """Test Protocol that opts out of protocol_version entirely."""

    def greet(self, name: str) -> str: ...


class TestServerWireBehaviour:
    """End-to-end smoke tests for the dispatch-boundary check over a pipe."""

    def _run_call(
        self,
        server_proto: type,
        client_proto: type,
    ) -> tuple[Exception | None, object | None]:
        """Run greet(name='x') once and return (raised, result). Closes both transports."""
        client_t, server_t = make_pipe_pair()
        server = RpcServer(server_proto, _Impl())
        proxy = _RpcProxy(client_proto, client_t)
        try:

            def serve() -> None:
                """Serve one request and swallow any raised exception (visible via the wire instead)."""
                with contextlib.suppress(BaseException):
                    server.serve_one(server_t)

            t = threading.Thread(target=serve, daemon=True)
            t.start()
            result: object | None = None
            caught: Exception | None = None
            try:
                result = proxy.greet(name="x")
            except RpcError as e:
                caught = e
            t.join(timeout=5)
            return caught, result
        finally:
            client_t.close()
            server_t.close()

    def test_matched_versions_succeed(self) -> None:
        """Both sides at 1.0.0 -> call succeeds."""
        err, result = self._run_call(_ProtoV100, _ProtoV100)
        assert err is None
        assert result == "hi x"

    def test_mismatched_versions_raise_with_direction(self) -> None:
        """Client 1.0.0, server 2.0.0 -> RpcError with directional message text."""
        err, _ = self._run_call(_ProtoV200, _ProtoV100)
        assert err is not None
        assert isinstance(err, RpcError)
        err_text = str(err) + " " + err.error_message
        assert "Client: 1.0.0" in err_text
        assert "Server: 2.0.0" in err_text
        assert "upgrade the VGI extension/client" in err_text

    def test_undeclared_server_does_not_check(self) -> None:
        """Server without protocol_version -> no check fires, even when client sends one."""
        err, result = self._run_call(_ProtoNoVersion, _ProtoV200)
        assert err is None
        assert result == "hi x"

    def test_describe_bypass(self) -> None:
        """__describe__ must succeed even when client and server protocol_versions mismatch.

        Describe is the diagnostic path a mismatched client uses to learn the
        server's expected version. If the server gated __describe__ on
        protocol_version, version-mismatched clients would have no way to
        recover beyond reading the (also-truncated) framework error.
        """
        from vgi_rpc.introspect import introspect

        client_t, server_t = make_pipe_pair()
        # Server speaks 2.0.0; client introspect call is framework-internal and
        # carries no client-side version (introspect.introspect doesn't go
        # through _RpcProxy). The server must serve __describe__ regardless.
        server = RpcServer(_ProtoV200, _Impl(), enable_describe=True)
        try:

            def serve() -> None:
                """Serve one request; describe must dispatch normally."""
                with contextlib.suppress(BaseException):
                    server.serve_one(server_t)

            t = threading.Thread(target=serve, daemon=True)
            t.start()
            desc = introspect(client_t)
            t.join(timeout=5)
            assert desc.protocol_name == "_ProtoV200"
            # Server advertised its version in describe-response metadata.
            assert desc.protocol_version == "2.0.0"
        finally:
            client_t.close()
            server_t.close()


# ---------------------------------------------------------------------------
# HTTP transport — the path external clients actually use.
# ---------------------------------------------------------------------------


class TestHttpTransportVersionCheck:
    """HTTP unary and stream init must enforce the dispatch-boundary version check.

    HTTP doesn't route through ``RpcServer.serve_one`` — it has its own
    dispatch loop in ``vgi_rpc/http/server/_app_unary.py`` and ``_app_stream.py``.
    Without an HTTP-specific check, a mismatched HTTP client would dispatch
    straight to the worker handler with the wrong schemas, and the only
    defense would be the Arrow column-count check (silent corruption in some
    cases). These tests gate that HTTP-path regression.
    """

    def test_matched_versions_succeed(self) -> None:
        """HTTP unary RPC succeeds when client and server speak the same protocol_version."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_ProtoV100, _Impl())
        client = make_sync_client(server, token_key=b"test")
        try:
            with http_connect(_ProtoV100, client=client) as proxy:
                assert proxy.greet(name="x") == "hi x"
        finally:
            client.close()

    def test_mismatched_versions_raise_with_direction(self) -> None:
        """HTTP unary RPC fails with directional message when versions mismatch.

        The C++ extension's user-visible error path runs through this: a
        DuckDB user issuing ``ATTACH`` against a worker on the wrong version
        sees the directional text inline, not a generic 'request failed'.
        """
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_ProtoV200, _Impl())
        # Client built against the 1.0.0 Protocol talks to a 2.0.0 server.
        client = make_sync_client(server, token_key=b"test")
        try:
            with http_connect(_ProtoV100, client=client) as proxy, pytest.raises(RpcError) as exc_info:
                proxy.greet(name="x")
            err_text = str(exc_info.value) + " " + exc_info.value.error_message
            assert "Client: 1.0.0" in err_text
            assert "Server: 2.0.0" in err_text
            assert "upgrade the VGI extension/client" in err_text
        finally:
            client.close()

    def test_describe_bypass_over_http(self) -> None:
        """HTTP __describe__ must dispatch regardless of client/server version mismatch."""
        from vgi_rpc.http import http_introspect, make_sync_client

        server = RpcServer(_ProtoV200, _Impl(), enable_describe=True)
        client = make_sync_client(server, token_key=b"test")
        try:
            # http_introspect is the framework-internal discovery path; like
            # the pipe-transport introspect(), it doesn't carry a client
            # protocol_version. The server must serve describe anyway so
            # mismatched clients can introspect the expected version.
            desc = http_introspect(client=client)
            assert desc.protocol_name == "_ProtoV200"
            assert desc.protocol_version == "2.0.0"
        finally:
            client.close()

    def test_undeclared_server_does_not_check_over_http(self) -> None:
        """Server without protocol_version -> HTTP path is opt-out too.

        Symmetry with the pipe path. A Protocol that hasn't opted into
        versioning must not gain the check just because the transport
        happens to be HTTP.
        """
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_ProtoNoVersion, _Impl())
        # Client declares 2.0.0; server opts out → check doesn't fire.
        client = make_sync_client(server, token_key=b"test")
        try:
            with http_connect(_ProtoV200, client=client) as proxy:
                assert proxy.greet(name="x") == "hi x"
        finally:
            client.close()
