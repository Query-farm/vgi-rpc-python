# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Native Iroh client contract and loopback tests."""

from __future__ import annotations

import asyncio
import importlib
import json
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from io import BufferedReader, IOBase
from pathlib import Path
from typing import Any, cast

import httpx2
import pytest

from vgi_rpc.iroh import (
    IROH_ARROW_MUX_ALPN,
    IROH_HTTP_ALPN,
    DispatchCertainty,
    IrohErrorCategory,
    IrohErrorStage,
    IrohHttpTransport,
    IrohTransportError,
    IrohUriError,
    _decode_secret_key,
    _IrohReader,
    _IrohWriter,
    _load_iroh,
    _NativeSession,
    _process_ephemeral_key,
    _runtime,
    iroh_connect,
    parse_iroh_uri,
)
from vgi_rpc.rpc import RpcServer

from ._fixture_service import RpcFixtureService, RpcFixtureServiceImpl

_VECTORS = Path(__file__).parents[1] / "vgi_rpc" / "conformance" / "iroh_transport_vectors.json"


def test_uri_parser_consumes_canonical_vectors() -> None:
    """Every valid and adversarial URI agrees with the registry fixture."""
    contract = json.loads(_VECTORS.read_text())
    assert contract["alpns"] == {
        "iroh": IROH_ARROW_MUX_ALPN.decode(),
        "httpi": IROH_HTTP_ALPN.decode(),
    }
    for case in contract["uri_cases"]:
        if case["valid"]:
            target = parse_iroh_uri(case["uri"])
            assert target.scheme == case["scheme"]
            assert target.base_path == case["base_path"]
            assert len(target.endpoint_id) == 32
        else:
            with pytest.raises(IrohUriError) as caught:
                parse_iroh_uri(case["uri"])
            assert caught.value.stage == IrohErrorStage.PARSE
            assert caught.value.category == IrohErrorCategory.INVALID_INPUT
            assert caught.value.dispatch_certainty == DispatchCertainty.NOT_SENT


def test_error_vectors_use_public_closed_sets() -> None:
    """Golden error fields cannot drift outside the portable enums."""
    contract = json.loads(_VECTORS.read_text())
    for case in contract["error_cases"]:
        assert IrohErrorStage(case["stage"])
        assert IrohErrorCategory(case["category"])
        assert DispatchCertainty(case["dispatch_certainty"])


def test_secret_key_formats_and_redacted_validation() -> None:
    """Bytes, hex, and z-base-32 produce the same private key bytes."""
    raw = bytes(32)
    assert _decode_secret_key(raw) == raw
    assert _decode_secret_key(raw.hex()) == raw
    assert _decode_secret_key("y" * 52) == raw
    with pytest.raises(ValueError, match="Iroh secret key") as caught:
        _decode_secret_key("private-value-that-must-not-appear")
    assert "private-value" not in str(caught.value)


def test_default_identity_is_stable_for_the_process() -> None:
    """Raw and HTTP connectors can share one process-lifetime ephemeral identity."""
    first = _process_ephemeral_key()
    assert len(first) == 32
    assert _process_ephemeral_key() is first


def test_missing_binding_is_structured_and_never_downloaded(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unavailable optional binding fails closed without a connector fallback."""

    def missing(_name: str) -> Any:
        raise ImportError("not installed")

    monkeypatch.setattr(importlib, "import_module", missing)
    with pytest.raises(IrohTransportError) as caught:
        _load_iroh()
    assert caught.value.stage == IrohErrorStage.BIND
    assert caught.value.category == IrohErrorCategory.UNSUPPORTED
    assert caught.value.dispatch_certainty == DispatchCertainty.NOT_SENT


class _AcceptedTransport:
    def __init__(self, session: _NativeSession) -> None:
        self._session = session
        self._reader = cast("IOBase", BufferedReader(_IrohReader(session), buffer_size=64 * 1024))
        self._writer = cast("IOBase", _IrohWriter(session))

    @property
    def reader(self) -> IOBase:
        return self._reader

    @property
    def writer(self) -> IOBase:
        return self._writer

    def close(self) -> None:
        self._writer.close()
        self._reader.close()


@contextmanager
def _native_server(alpn: bytes) -> Iterator[tuple[Any, Any, Any]]:
    api = _load_iroh()
    runtime = _runtime(api)

    async def bind() -> Any:
        return await api.Endpoint.bind(
            api.EndpointOptions(
                preset=api.preset_minimal(),
                alpns=[alpn],
                relay_mode=api.RelayMode.disabled(),
            )
        )

    endpoint = runtime.call(
        bind(),
        timeout=10,
        cancellation=None,
        stage=IrohErrorStage.BIND,
        certainty=DispatchCertainty.NOT_SENT,
    )
    try:
        yield api, runtime, endpoint
    finally:
        runtime.call(
            endpoint.close(),
            timeout=10,
            cancellation=None,
            stage=IrohErrorStage.CLOSE,
            certainty=DispatchCertainty.SENT,
        )


def test_native_raw_iroh_rpc_loopback() -> None:
    """The official binding carries a real typed VGI unary call end to end."""
    with _native_server(IROH_ARROW_MUX_ALPN) as (api, runtime, endpoint):

        async def accept_stream() -> tuple[Any, Any]:
            incoming = await endpoint.accept_next()
            assert incoming is not None
            accepting = await incoming.accept()
            connection = await accepting.connect()
            assert connection.alpn() == IROH_ARROW_MUX_ALPN
            return connection, await connection.accept_bi()

        accepted = asyncio.run_coroutine_threadsafe(accept_stream(), runtime._loop)
        server_errors: list[BaseException] = []

        def serve() -> None:
            try:
                connection, stream = accepted.result(timeout=10)
                session = _NativeSession(
                    api,
                    runtime,
                    endpoint,
                    connection,
                    stream.send(),
                    stream.recv(),
                    10,
                    None,
                )
                transport = _AcceptedTransport(session)
                try:
                    RpcServer(RpcFixtureService, RpcFixtureServiceImpl()).serve(transport)
                finally:
                    transport.close()
            except BaseException as exc:
                server_errors.append(exc)

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()
        uri = f"iroh://{endpoint.id().to_bytes().hex()}"
        with iroh_connect(
            RpcFixtureService,
            uri,
            no_relay=True,
            direct_addresses=endpoint.bound_sockets(),
            connect_timeout=10,
            io_timeout=10,
        ) as service:
            assert service.add(a=3.0, b=4.0) == pytest.approx(7.0)
        thread.join(timeout=10)
        assert not thread.is_alive()
        assert server_errors == []


def test_native_httpi_loopback_uses_http11_on_one_bistream() -> None:
    """The official binding carries a standard HTTP/1.1 exchange over iroh-http/2."""
    fixture = Path(__file__).with_name("serve_fixture_iroh.py")
    process = subprocess.Popen(
        [sys.executable, str(fixture)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    assert process.stderr is not None
    try:
        discovery = json.loads(process.stdout.readline())
        uri = f"httpi://{discovery['endpoint_id']}/vgi"
        transport = IrohHttpTransport(
            uri,
            no_relay=True,
            direct_addresses=discovery["addresses"],
            connect_timeout=10,
            io_timeout=10,
        )
        with httpx2.Client(base_url="http://iroh.test", transport=cast("httpx2.BaseTransport", transport)) as client:
            response = client.post("/vgi/echo?mode=one", content=b"payload")
        request = bytes.fromhex(json.loads(process.stdout.readline())["request_hex"])
        assert process.wait(timeout=10) == 0, process.stderr.read()
        assert response.status_code == 200
        assert response.text == "hello"
        assert request.startswith(b"POST /vgi/echo?mode=one HTTP/1.1\r\n")
        assert request.endswith(b"\r\npayload")
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)
