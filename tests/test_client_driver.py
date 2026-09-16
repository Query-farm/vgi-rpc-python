# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the port-agnostic client-driver shim.

These drive ``vgi_rpc.conformance.client_driver`` against ``tests.fake_client_driver``,
a scripted driver that answers the control protocol without a server behind it.
The point is to pin the *control protocol* behaviour that four ports will
implement against — the two error channels, log relay, stream termination,
session ops — in the reference repo, where it is cheap to run.

The contract these support is written down in
``tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md``.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from vgi_rpc._codec import Encoding
from vgi_rpc.conformance import ConformanceHeader
from vgi_rpc.conformance.client_driver import (
    DRIVER_COMMAND_ENV_VAR,
    ClientDriver,
    ClientDriverProxy,
    DriverStreamSession,
)
from vgi_rpc.log import Message
from vgi_rpc.rpc import AnnotatedBatch, MethodType, RpcError


@pytest.fixture
def transcript(tmp_path: Path) -> Path:
    """Path the fake driver writes its request transcript to."""
    return tmp_path / "ops.json"


@pytest.fixture
def driver(transcript: Path) -> ClientDriver:
    """Return a ``ClientDriver`` pointed at the scripted fake driver."""
    return ClientDriver([sys.executable, "-m", "tests.fake_client_driver", str(transcript)])


@pytest.fixture
def logs() -> list[Message]:
    """Collector for log records the driver relays."""
    return []


@pytest.fixture
def proxy(driver: ClientDriver, logs: list[Message]) -> Iterator[ClientDriverProxy]:
    """Yield a connected proxy over the fake driver, closed on teardown."""
    connection = driver.connect("stdio", ["fake-worker"], logs.append, headers={"X-Principal": "alice"})
    try:
        yield connection
    finally:
        connection.close()


def _ops(transcript: Path) -> list[dict[str, Any]]:
    """Read the fake driver's request transcript."""
    lines = transcript.read_text(encoding="utf-8").splitlines()
    return [cast("dict[str, Any]", json.loads(line)) for line in lines if line.strip()]


def _stream(proxy: ClientDriverProxy, method: str, **kwargs: object) -> DriverStreamSession:
    """Open a stream by method name, typed as the session the shim returns."""
    return cast("DriverStreamSession", getattr(proxy, method)(**kwargs))


class TestConnect:
    """The ``connect`` op carries the routing key and the caller's headers."""

    def test_connect_names_the_protocol(self, proxy: ClientDriverProxy, transcript: Path) -> None:
        """Every connection is bound to a routing key -- never left unrouted."""
        connect = _ops(transcript)[0]
        assert connect["op"] == "connect"
        assert connect["protocol"] == "ConformanceService"
        assert connect["transport"] == "stdio"
        assert connect["target"] == ["fake-worker"]

    def test_connect_forwards_caller_headers(self, proxy: ClientDriverProxy, transcript: Path) -> None:
        """Headers the caller pinned reach the driver as connection defaults."""
        assert _ops(transcript)[0]["headers"] == {"X-Principal": "alice"}

    def test_compression_level_is_tri_state(self, driver: ClientDriver, transcript: Path) -> None:
        """``None`` means disabled, and must survive as JSON null, not as absence."""
        connection = driver.connect("http", "http://example.invalid", compression_level=None)
        connection.close()
        connect = _ops(transcript)[0]
        assert "compression_level" in connect
        assert connect["compression_level"] is None

    def test_shm_carries_a_segment_size(self, driver: ClientDriver, transcript: Path) -> None:
        """The shm transport needs a segment size; the harness supplies a default."""
        connection = driver.connect("shm", ["fake-worker"])
        connection.close()
        assert _ops(transcript)[0]["shm_size"] > 0

    def test_refused_connection_raises(self, driver: ClientDriver) -> None:
        """A driver-level refusal is a transport error, not a remote error."""
        with pytest.raises(RpcError) as excinfo:
            driver.connect("stdio", "refuse")
        assert excinfo.value.error_type == "TransportError"
        assert "connection refused" in str(excinfo.value)


class TestUnary:
    """Unary calls, their results, their logs and their errors."""

    def test_round_trip(self, proxy: ClientDriverProxy) -> None:
        """A value encoded by the canonical writer comes back decoded."""
        assert proxy.echo_string(value="hello") == "hello"

    def test_logs_are_relayed(self, proxy: ClientDriverProxy, logs: list[Message]) -> None:
        """Log records attached to a response reach the ``on_log`` callback."""
        proxy.echo_string(value="hello")
        assert [message.message for message in logs] == ["echoed"]
        assert logs[0].extra == {"origin": "fake"}

    def test_void_return(self, proxy: ClientDriverProxy) -> None:
        """A response with no payload slot is ``None``, not an error."""
        assert proxy.void_noop() is None

    def test_remote_error_keeps_its_type(self, proxy: ClientDriverProxy) -> None:
        """``error_type`` is asserted verbatim by the suite, so it must survive."""
        with pytest.raises(RpcError) as excinfo:
            proxy.raise_value_error(message="boom")
        assert excinfo.value.error_type == "ValueError"
        assert excinfo.value.error_message == "boom"
        assert excinfo.value.remote_traceback == "remote traceback"

    def test_legacy_message_key_is_accepted(self, proxy: ClientDriverProxy) -> None:
        """A driver writing ``message`` instead of ``error_message`` still reports it.

        The contract says ``error_message``; silently dropping the text of an
        error is a worse outcome than accepting the older spelling.
        """
        with pytest.raises(RpcError) as excinfo:
            proxy.raise_runtime_error(message="x")
        assert excinfo.value.error_message == "legacy"

    def test_driver_refusal_is_a_transport_error(self, proxy: ClientDriverProxy) -> None:
        """``ok: false`` means the driver did not execute the op at all."""
        with pytest.raises(RpcError) as excinfo:
            proxy.echo_int(value=1)
        assert excinfo.value.error_type == "TransportError"

    def test_unknown_method_is_an_attribute_error(self, proxy: ClientDriverProxy) -> None:
        """The proxy exposes the service's methods and nothing else."""
        with pytest.raises(AttributeError):
            _ = proxy.not_a_method


class TestProducerStream:
    """Producer streams, their headers, tokens and terminal events."""

    def test_iterates_to_completion(self, proxy: ClientDriverProxy) -> None:
        """``done: true`` ends iteration rather than raising."""
        batches = list(_stream(proxy, "produce_n", count=3))
        assert [item.batch.column("index")[0].as_py() for item in batches] == [2, 1, 0]

    def test_custom_metadata_survives(self, proxy: ClientDriverProxy) -> None:
        """A batch's Arrow custom metadata crosses the control boundary intact."""
        first = next(iter(_stream(proxy, "produce_n", count=1)))
        assert first.custom_metadata is not None
        assert first.custom_metadata[b"fake.index"] == b"0"

    def test_tick_metadata_is_sent_as_an_empty_batch(self, proxy: ClientDriverProxy, transcript: Path) -> None:
        """Per-tick metadata rides an empty batch; the batch itself is ignored."""
        session = _stream(proxy, "produce_tick_metadata", count=1)
        session.tick(custom_metadata={b"probe": b"1"})
        session.close()
        assert any(op["op"] == "tick" and "input_b64" in op for op in _ops(transcript))

    def test_header_is_decoded(self, proxy: ClientDriverProxy) -> None:
        """A declared header is decoded with the canonical header reader."""
        session = _stream(proxy, "produce_with_header", count=3)
        header = session.typed_header(ConformanceHeader)
        assert header.total_expected == 3
        assert header.description == "fake"
        session.close()

    def test_next_with_token(self, proxy: ClientDriverProxy) -> None:
        """The resume token rides alongside the batch it follows."""
        session = _stream(proxy, "produce_n", count=2)
        _, token = session.next_with_token()
        assert token == "token-1"
        session.close()

    def test_error_on_init_surfaces_the_remote_type(self, proxy: ClientDriverProxy) -> None:
        """Even spelled the non-conforming way (``ok: false`` + object)."""
        with pytest.raises(RpcError) as excinfo:
            proxy.produce_error_on_init()
        assert excinfo.value.error_type == "ValueError"

    def test_cancel_is_terminal(self, proxy: ClientDriverProxy) -> None:
        """After cancelling, no further stream op may be sent."""
        session = _stream(proxy, "produce_n", count=5)
        session.cancel()
        with pytest.raises(RpcError):
            session.tick()

    def test_close_is_idempotent(self, proxy: ClientDriverProxy, transcript: Path) -> None:
        """A second close must not put another op on the wire."""
        session = _stream(proxy, "produce_n", count=5)
        session.close()
        session.close()
        assert sum(1 for op in _ops(transcript) if op["op"] == "close") == 1

    def test_close_releases_the_connection_for_reuse(self, proxy: ClientDriverProxy) -> None:
        """Ending a stream returns the connection to ordinary calls."""
        with _stream(proxy, "produce_n", count=1) as session:
            next(iter(session))
        assert proxy.echo_string(value="after") == "after"


class TestExchangeStream:
    """Exchange streams send a batch per turn."""

    def test_round_trip(self, proxy: ClientDriverProxy) -> None:
        """Input batch in, output batch out."""
        with _stream(proxy, "exchange_scale", factor=2.0) as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"value": [4]}))
        assert out.batch.column("value")[0].as_py() == 40

    def test_open_is_flagged_as_exchange(self, proxy: ClientDriverProxy, transcript: Path) -> None:
        """``is_exchange`` is declared by the harness, never guessed from the name."""
        with _stream(proxy, "exchange_scale", factor=2.0) as session:
            session.exchange(AnnotatedBatch.from_pydict({"value": [1]}))
        opened = next(op for op in _ops(transcript) if op["op"] == "stream_open")
        assert opened["is_exchange"] is True

    def test_missing_reply_is_a_protocol_error(self, proxy: ClientDriverProxy) -> None:
        """An exchange that answers ``done`` has violated its own contract."""
        with _stream(proxy, "exchange_scale", factor=2.0) as session, pytest.raises(RpcError) as excinfo:
            session.exchange(AnnotatedBatch.from_pydict({"value": [-1]}))
        assert excinfo.value.error_type == "ProtocolError"

    def test_error_on_init(self, proxy: ClientDriverProxy) -> None:
        """A refused open raises the remote type, with no stream left behind."""
        with pytest.raises(RpcError) as excinfo:
            proxy.exchange_error_on_init()
        assert excinfo.value.error_type == "ValueError"


class TestDescribe:
    """``describe`` relays an already-decoded description."""

    def test_service_description(self, proxy: ClientDriverProxy) -> None:
        """Fields land where ``ServiceDescription`` expects them."""
        described = proxy.describe()
        assert described.protocol_name == "ConformanceService"
        assert described.describe_version == "5"
        assert described.server_id == "fake-server"
        assert described.protocol_version == "2.0.0"

    def test_method_shapes(self, proxy: ClientDriverProxy) -> None:
        """Method type, stream kind and schemas decode from the relayed JSON."""
        methods = proxy.describe().methods
        assert methods["echo_string"].method_type is MethodType.UNARY
        assert methods["echo_string"].is_exchange is None
        assert methods["echo_string"].params_schema.names == ["value"]
        assert methods["produce_n"].method_type is MethodType.STREAM
        assert methods["produce_n"].is_exchange is False
        # An absent schema is the empty schema, not an error.
        assert methods["produce_n"].params_schema.names == []


class TestHttpStandIns:
    """The ``vgi_rpc.http`` module-level entry points, routed through a driver."""

    def test_capabilities(self, driver: ClientDriver) -> None:
        """Capabilities decode into the real dataclass, dropping unknown codecs."""
        caps = driver.http_capabilities("http://example.invalid")
        assert caps.sticky_enabled is True
        assert caps.sticky_default_ttl == 300
        assert caps.sticky_echo_headers == ("Backend",)
        assert caps.max_request_bytes == 1048576
        assert caps.max_response_bytes is None
        assert caps.supported_encodings == (Encoding.ZSTD, Encoding.GZIP)

    def test_upload_urls(self, driver: ClientDriver) -> None:
        """Upload URLs decode into the real dataclass, with a real timestamp."""
        urls = driver.request_upload_urls("http://example.invalid", count=2)
        assert [url.upload_url for url in urls] == ["http://up/0", "http://up/1"]
        assert urls[0].expires_at == datetime.fromtimestamp(1767225600, tz=UTC)

    def test_http_connect_is_a_context_manager(self, driver: ClientDriver) -> None:
        """The stand-in has ``http_connect``'s shape, including its scope."""
        from vgi_rpc.conformance import ConformanceService

        with driver.http_connect(ConformanceService, "http://example.invalid") as connection:
            assert connection.echo_string(value="via http") == "via http"

    def test_http_connect_requires_a_target(self, driver: ClientDriver) -> None:
        """Neither a URL nor a client is not a connection."""
        from vgi_rpc.conformance import ConformanceService

        with pytest.raises(ValueError, match="base_url is required"), driver.http_connect(ConformanceService):
            pass

    def test_install_http_overrides(self, driver: ClientDriver) -> None:
        """The overrides replace the module-level entry points in place."""
        import vgi_rpc.http as http_module

        saved = (http_module.http_connect, http_module.http_capabilities, http_module.request_upload_urls)
        try:
            driver.install_http_overrides()
            assert http_module.http_capabilities("http://example.invalid").sticky_enabled is True
        finally:
            (
                http_module.http_connect,
                http_module.http_capabilities,
                http_module.request_upload_urls,
            ) = saved


class TestSessions:
    """Sticky-session scope and its accessors."""

    def test_token_and_detach(self, proxy: ClientDriverProxy) -> None:
        """A minted token is readable, and detaching hands it over."""
        with proxy.with_session_token() as session:
            assert session.current_session_token() == "minted-token"
            assert session.detach() == "minted-token"
            assert session.current_session_token() is None

    def test_resume_presents_the_token(self, proxy: ClientDriverProxy, transcript: Path) -> None:
        """Resuming presents an existing token rather than minting a new one."""
        with proxy.with_session_token(token="carried") as session:
            assert session.current_session_token() == "carried"
        begin = next(op for op in _ops(transcript) if op["op"] == "session_begin")
        assert begin["token"] == "carried"

    def test_echo_headers_drop_non_strings(self, proxy: ClientDriverProxy) -> None:
        """A header value that is not a string is dropped, not coerced."""
        with proxy.with_session_token() as session:
            assert session.current_echo_headers() == {"Backend": "b7"}

    def test_rpc_calls_delegate_through_the_view(self, proxy: ClientDriverProxy) -> None:
        """The view is transparent for RPC method names."""
        with proxy.with_session_token() as session:
            echo = cast("Callable[..., object]", session.echo_string)
            assert echo(value="in session") == "in session"

    def test_scope_always_ends(self, proxy: ClientDriverProxy, transcript: Path) -> None:
        """An exception inside the scope still tears the session down."""
        with pytest.raises(RpcError), proxy.with_session_token():
            proxy.raise_value_error(message="inside")
        assert any(op["op"] == "session_end" for op in _ops(transcript))


class TestDriverConfiguration:
    """How a port names its driver."""

    def test_from_env_splits_a_command_line(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An interpreted driver needs no wrapper script."""
        monkeypatch.setenv(DRIVER_COMMAND_ENV_VAR, "bun run driver.ts")
        assert ClientDriver.from_env().command == ("bun", "run", "driver.ts")

    def test_from_env_falls_back(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unset, the port's own default applies."""
        monkeypatch.delenv(DRIVER_COMMAND_ENV_VAR, raising=False)
        assert ClientDriver.from_env(default=["./driver"]).command == ("./driver",)

    def test_from_env_requires_something(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No variable and no default is a configuration error, not a hang."""
        monkeypatch.delenv(DRIVER_COMMAND_ENV_VAR, raising=False)
        with pytest.raises(RuntimeError, match=DRIVER_COMMAND_ENV_VAR):
            ClientDriver.from_env()

    def test_empty_command_is_rejected(self) -> None:
        """An empty argv would spawn nothing and block forever."""
        with pytest.raises(ValueError, match="empty"):
            ClientDriver([])

    def test_close_is_safe_twice(self, driver: ClientDriver) -> None:
        """Teardown must not turn a dead driver into a second failure."""
        connection = driver.connect("stdio", ["fake-worker"])
        connection.close()
        connection.close()
