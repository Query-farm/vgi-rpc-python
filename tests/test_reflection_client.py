# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""``list_protocols`` / ``describe_protocol``: reflection over a held connection.

Every transport, because the point of the API is that it reuses whatever
connection the caller already has: a proxy on a byte stream is rebound to
``vgi_rpc.Reflection.v1`` over that stream, an HTTP proxy over its client.
The conformance workers are the targets because they host reflection and a
secondary protocol on every transport.  Raw Iroh is covered in
``test_iroh_transport.py``, where its loopback lives.
"""

from __future__ import annotations

import contextlib
import re
import sys
import threading
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from vgi_rpc import (
    HostedProtocol,
    ReflectionNotSupportedError,
    RpcError,
    ServiceDescription,
    describe_protocol,
    list_protocols,
)
from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.conformance.secondary import SECONDARY_PROTOCOL_NAME, conformance_extra_protocols
from vgi_rpc.http import http_connect, make_sync_client
from vgi_rpc.introspect import _reflection_not_hosted
from vgi_rpc.pool import WorkerPool
from vgi_rpc.rpc import (
    MethodType,
    RpcServer,
    ShmPipeTransport,
    make_pipe_pair,
    rpc_methods,
)
from vgi_rpc.rpc._client import _RpcProxy
from vgi_rpc.shm import ShmSegment

from ._fixture_service import RpcFixtureService, RpcFixtureServiceImpl
from .conftest import _CONFORMANCE_PIPE, _SKIP_UNIX, _SKIP_WIN_EXTERNALIZE

_PRIMARY = "ConformanceService"
_REFLECTION = "vgi_rpc.Reflection.v1"
_HEX64 = re.compile(r"[0-9a-f]{64}")

#: Transports the conformance connector reaches, plus the two it does not.
_TRANSPORTS = [
    "pipe",
    "shm_pipe",
    "subprocess",
    "pool",
    "http",
    pytest.param("http_externalize_always", marks=_SKIP_WIN_EXTERNALIZE),
    pytest.param("unix", marks=_SKIP_UNIX),
    pytest.param("unix_threaded", marks=_SKIP_UNIX),
    pytest.param("unix_launcher", marks=_SKIP_UNIX),
    "tcp",
]

ConnFactory = Callable[[], contextlib.AbstractContextManager[Any]]


def _server() -> RpcServer:
    """Build an in-process conformance server that hosts reflection."""
    return RpcServer(
        ConformanceService,
        ConformanceServiceImpl(),
        enable_describe=True,
        extra_protocols=conformance_extra_protocols(),
    )


@contextlib.contextmanager
def _shm_conn() -> Iterator[Any]:
    """Yield a conformance proxy over a shared-memory pipe."""
    shm = ShmSegment.create(4 * 1024 * 1024)
    try:
        client_pipe, server_pipe = make_pipe_pair()
        client = ShmPipeTransport(client_pipe, shm)
        thread = threading.Thread(target=_server().serve, args=(ShmPipeTransport(server_pipe, shm),), daemon=True)
        thread.start()
        try:
            yield _RpcProxy(ConformanceService, client)
        finally:
            client.close()
            thread.join(timeout=5)
    finally:
        shm.unlink()
        with contextlib.suppress(BufferError):
            shm.close()


@pytest.fixture(scope="module")
def conformance_pool() -> Iterator[WorkerPool]:
    """Yield a worker pool of conformance pipe workers."""
    with WorkerPool(max_idle=1) as pool:
        yield pool


@pytest.fixture(params=_TRANSPORTS)
def any_conn(
    request: pytest.FixtureRequest,
    conformance_protocol_connector: Callable[..., contextlib.AbstractContextManager[Any]],
) -> ConnFactory:
    """Return a factory for a ``ConformanceService`` proxy on one transport."""
    transport: str = request.param
    if transport == "shm_pipe":
        return _shm_conn
    if transport == "pool":
        pool: WorkerPool = request.getfixturevalue("conformance_pool")
        return lambda: pool.connect(ConformanceService, [sys.executable, _CONFORMANCE_PIPE])
    return lambda: conformance_protocol_connector(transport, ConformanceService)


def _expected_hash() -> str:
    """Return the primary's hash, as an in-process server computes it."""
    return _server().bindings[_PRIMARY].protocol_hash


class TestEveryTransport:
    """The listing and description, on each transport's own connection."""

    def test_list_protocols(self, any_conn: ConnFactory) -> None:
        """Application protocols lead in order, reflection follows, hashes are canonical."""
        with any_conn() as proxy:
            hosted = list_protocols(proxy)
        names = [p.name for p in hosted]
        assert names[:2] == [_PRIMARY, SECONDARY_PROTOCOL_NAME]
        assert _REFLECTION in names[2:]
        assert all(isinstance(p, HostedProtocol) for p in hosted)
        assert all(_HEX64.fullmatch(p.hash) for p in hosted)
        assert hosted[0].hash == _expected_hash()
        assert hosted[0].deprecated is False
        assert hosted[0].features == ()

    def test_describe_protocol(self, any_conn: ConnFactory) -> None:
        """A description names every method, with the hash the listing reported."""
        with any_conn() as proxy:
            hosted = {p.name: p for p in list_protocols(proxy)}
            desc = describe_protocol(proxy, _PRIMARY)
            reflection = describe_protocol(proxy, _REFLECTION)
        assert isinstance(desc, ServiceDescription)
        assert desc.protocol_name == _PRIMARY
        assert desc.protocol_hash == hosted[_PRIMARY].hash
        assert set(desc.methods) == set(rpc_methods(ConformanceService))
        assert desc.methods["echo_string"].method_type is MethodType.UNARY
        assert desc.methods["produce_n"].is_exchange is False
        assert desc.server_id
        assert set(reflection.methods) == {"describe", "list_protocols"}

    def test_connection_stays_usable(self, any_conn: ConnFactory) -> None:
        """Reflection neither closes nor desynchronises the primary connection."""
        with any_conn() as proxy:
            assert proxy.echo_string(value="a") == "a"
            list_protocols(proxy)
            describe_protocol(proxy, _PRIMARY)
            assert proxy.echo_string(value="b") == "b"
            assert len(list(proxy.produce_n(count=3))) == 3
            assert list_protocols(proxy)[0].name == _PRIMARY
            assert proxy.echo_int(value=7) == 7

    def test_unknown_protocol(self, any_conn: ConnFactory) -> None:
        """An unhosted name is the server's protocol_not_supported, not "no reflection"."""
        with any_conn() as proxy:
            with pytest.raises(RpcError) as info:
                describe_protocol(proxy, "nope.v1")
            assert not isinstance(info.value, ReflectionNotSupportedError)
            assert info.value.error_kind == "protocol_not_supported"
            assert proxy.echo_string(value="c") == "c"


class TestTargets:
    """What may be passed as the target."""

    def test_raw_transport(self) -> None:
        """A bare RpcTransport works, as introspect() always has."""
        client, srv = make_pipe_pair()
        thread = threading.Thread(target=_server().serve, args=(srv,), daemon=True)
        thread.start()
        try:
            assert list_protocols(client)[0].name == _PRIMARY
            assert "echo_string" in describe_protocol(client, _PRIMARY).methods
        finally:
            client.close()
            thread.join(timeout=5)
            srv.close()

    def test_proxy_bound_to_any_protocol(self) -> None:
        """The target's own protocol does not matter; only its connection does."""
        from vgi_rpc.conformance.secondary import Secondary

        client, srv = make_pipe_pair()
        thread = threading.Thread(target=_server().serve, args=(srv,), daemon=True)
        thread.start()
        try:
            secondary = _RpcProxy(Secondary, client)
            assert [p.name for p in list_protocols(secondary)][:2] == [_PRIMARY, SECONDARY_PROTOCOL_NAME]
        finally:
            client.close()
            thread.join(timeout=5)
            srv.close()

    def test_rejects_other_objects(self) -> None:
        """Anything that is neither a proxy nor a transport is a TypeError naming the accepted kinds."""
        with pytest.raises(TypeError, match="http_connect"):
            list_protocols(object())


@contextlib.contextmanager
def _no_reflection_pipe() -> Iterator[Any]:
    """Yield a proxy on a pipe server built with ``enable_describe=False``."""
    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=False)
    client, srv = make_pipe_pair()
    thread = threading.Thread(target=server.serve, args=(srv,), daemon=True)
    thread.start()
    try:
        yield _RpcProxy(RpcFixtureService, client)
    finally:
        client.close()
        thread.join(timeout=5)
        srv.close()


@contextlib.contextmanager
def _no_reflection_http() -> Iterator[Any]:
    """Yield an HTTP proxy on a server built with ``enable_describe=False``."""
    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=False)
    client = make_sync_client(server, token_key=b"test-key")
    try:
        with http_connect(RpcFixtureService, client=client) as proxy:
            yield proxy
    finally:
        client.close()


_NO_REFLECTION: dict[str, Callable[[], contextlib.AbstractContextManager[Any]]] = {
    "pipe": _no_reflection_pipe,
    "http": _no_reflection_http,
}


class TestServerWithoutReflection:
    """A server that does not host reflection raises ReflectionNotSupportedError."""

    @pytest.mark.parametrize("transport", sorted(_NO_REFLECTION))
    def test_list_raises_and_connection_survives(self, transport: str) -> None:
        """The error is specific, keeps the server's fields, and leaves the connection usable."""
        with _NO_REFLECTION[transport]() as proxy:
            with pytest.raises(ReflectionNotSupportedError) as info:
                list_protocols(proxy)
            assert isinstance(info.value, RpcError)
            assert info.value.error_kind == "protocol_not_supported"
            assert info.value.error_code == "UNIMPLEMENTED"
            assert proxy.add(a=2.0, b=2.0) == pytest.approx(4.0)

    @pytest.mark.parametrize("transport", sorted(_NO_REFLECTION))
    def test_describe_raises(self, transport: str) -> None:
        """describe_protocol lists first, so it reports "no reflection", not "no such protocol"."""
        with _NO_REFLECTION[transport]() as proxy, pytest.raises(ReflectionNotSupportedError):
            describe_protocol(proxy, "RpcFixtureService")

    @pytest.mark.parametrize(
        "error",
        [
            RpcError("MethodNotImplementedError", "no list_protocols", ""),
            RpcError("AttributeError", "x", "", error_kind="method_not_implemented"),
            RpcError("Whatever", "x", "", error_code="UNIMPLEMENTED"),
            RpcError("HttpError", "HTTP 404: response is not a valid Arrow IPC stream", ""),
        ],
        ids=["old-type", "kind", "code", "http-404"],
    )
    def test_older_servers_classified(self, error: RpcError) -> None:
        """The answers older servers give are all read as "no reflection"."""
        assert _reflection_not_hosted(error)

    @pytest.mark.parametrize(
        "error",
        [
            RpcError("ValueError", "boom", ""),
            RpcError("HttpError", "HTTP 500: response is not a valid Arrow IPC stream", ""),
            RpcError("TransportError", "pipe closed", "", error_code="UNAVAILABLE"),
        ],
        ids=["app-error", "http-500", "unavailable"],
    )
    def test_other_errors_not_classified(self, error: RpcError) -> None:
        """Any other failure propagates as itself."""
        assert not _reflection_not_hosted(error)
