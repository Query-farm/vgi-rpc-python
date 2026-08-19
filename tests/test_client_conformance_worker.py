# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Self-tests for the native-client conformance reference worker."""

from __future__ import annotations

import datetime as dt
import ssl
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any, cast
from wsgiref.types import StartResponse, WSGIEnvironment

import pyarrow as pa
import pytest

from vgi_rpc.conformance.client_worker import (
    PRODUCER_SCHEMA,
    TYPED_EXCHANGE_SCHEMA,
    ClientConformanceService,
    ClientConformanceServiceImpl,
    StrictExchangeSchemaMiddleware,
)
from vgi_rpc.external import ClientExternalConfig
from vgi_rpc.http import http_capabilities, http_connect, http_introspect
from vgi_rpc.introspect import introspect
from vgi_rpc.metadata import (
    CALL_STATE_KEY,
    LOCATION_KEY,
    LOCATION_SHA256_KEY,
    REQUEST_VERSION,
    REQUEST_VERSION_KEY,
    RPC_METHOD_KEY,
    SERVER_ID_KEY,
    STATE_KEY,
    TRANSPORT_SHM_KEY,
)
from vgi_rpc.rpc import (
    AnnotatedBatch,
    RpcConnection,
    RpcError,
    ShmPipeTransport,
    StderrMode,
    SubprocessTransport,
    rpc_methods,
    serve_pipe,
    tcp_connect,
    unix_connect,
)
from vgi_rpc.shm import ShmSegment
from vgi_rpc.transport_options import TRANSPORT_OPTIONS_METHOD_NAME, shm_available


@contextmanager
def _running_worker(*args: str, tls: bool = False) -> Iterator[tuple[str, str | None]]:
    """Start the standalone worker and yield its base URL and optional CA path."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi_rpc.conformance.client_worker", "--http", "0", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), line
        port = int(line.split(":", 1)[1])
        ca_path: str | None = None
        if tls:
            ca_line = proc.stdout.readline().decode().strip()
            assert ca_line.startswith("TLS-CA:"), ca_line
            ca_path = ca_line.removeprefix("TLS-CA:")
        yield f"{'https' if tls else 'http'}://127.0.0.1:{port}", ca_path
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@contextmanager
def _running_raw_socket_worker(*args: str) -> Iterator[str]:
    """Start a Unix/TCP raw worker and yield its discovery line."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi_rpc.conformance.client_worker", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        discovery = proc.stdout.readline().decode().strip()
        assert discovery.startswith(("UNIX:", "TCP:")), discovery
        yield discovery
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def _request_body(method: str, batch: pa.RecordBatch, extra: dict[bytes, bytes] | None = None) -> bytes:
    """Serialize one native-client request batch with mandatory wire metadata."""
    metadata = {
        RPC_METHOD_KEY: method.encode(),
        REQUEST_VERSION_KEY: REQUEST_VERSION,
        **(extra or {}),
    }
    body = BytesIO()
    with pa.ipc.new_stream(body, batch.schema) as writer:
        writer.write_batch(batch, custom_metadata=pa.KeyValueMetadata(metadata))
    return body.getvalue()


def _read_batches(body: bytes) -> list[tuple[pa.RecordBatch, pa.KeyValueMetadata | None]]:
    """Read all batches and custom metadata from one Arrow IPC response stream."""
    reader = pa.ipc.open_stream(BytesIO(body))
    batches: list[tuple[pa.RecordBatch, pa.KeyValueMetadata | None]] = []
    while True:
        try:
            batches.append(reader.read_next_batch_with_custom_metadata())
        except StopIteration:
            return batches


def _transport_options(transport: Any) -> pa.KeyValueMetadata:
    """Call the built-in raw transport negotiation method on an open connection."""
    request = pa.RecordBatch.from_pylist([{}], schema=pa.schema([]))
    metadata = pa.KeyValueMetadata(
        {
            RPC_METHOD_KEY: TRANSPORT_OPTIONS_METHOD_NAME.encode(),
            REQUEST_VERSION_KEY: REQUEST_VERSION,
            TRANSPORT_SHM_KEY: b"true",
        }
    )
    with pa.ipc.new_stream(transport.writer, request.schema) as writer:
        writer.write_batch(request, custom_metadata=metadata)
    reader = pa.ipc.open_stream(transport.reader)
    response, response_metadata = reader.read_next_batch_with_custom_metadata()
    assert response.num_columns == 0
    assert response.num_rows == 0
    with pytest.raises(StopIteration):
        reader.read_next_batch()
    assert response_metadata is not None
    return response_metadata


def _params(method: str, **values: object) -> pa.RecordBatch:
    """Build an exact parameter batch from the worker's declared method schema."""
    schema = rpc_methods(ClientConformanceService)[method].params_schema
    return pa.RecordBatch.from_pylist([values], schema=schema)


def _wait_ready(base_url: str, *, prefix: str = "", verify: ssl.SSLContext | bool = True) -> None:
    """Wait until a just-spawned HTTP or HTTPS worker accepts requests."""
    import httpx2

    deadline = time.monotonic() + 5
    with httpx2.Client(verify=verify, trust_env=False) as client:
        while True:
            try:
                response = client.get(f"{base_url}{prefix}/health", timeout=0.5)
                response.raise_for_status()
                return
            except httpx2.TransportError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)


def _exchange(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Round-trip one batch through the reference worker."""
    with (
        serve_pipe(ClientConformanceService, ClientConformanceServiceImpl()) as proxy,
        proxy.typed_exchange() as session,
    ):
        return session.exchange(AnnotatedBatch(batch=batch)).batch


def test_all_null_batch_preserves_declared_schema() -> None:
    """All-null runtime values cannot erase the client's declared types."""
    arrays = [pa.array([None], type=field.type) for field in TYPED_EXCHANGE_SCHEMA]
    batch = pa.RecordBatch.from_arrays(arrays, schema=TYPED_EXCHANGE_SCHEMA)
    result = _exchange(batch)
    assert result.schema == TYPED_EXCHANGE_SCHEMA
    assert result.to_pylist() == batch.to_pylist()


def test_zero_row_batch_preserves_declared_schema() -> None:
    """An empty exchange still carries every declared field and logical type."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in TYPED_EXCHANGE_SCHEMA],
        schema=TYPED_EXCHANGE_SCHEMA,
    )
    result = _exchange(batch)
    assert result.schema == TYPED_EXCHANGE_SCHEMA
    assert result.num_rows == 0


def test_populated_batch_round_trips_nested_logical_types() -> None:
    """Dictionary, temporal, decimal, list, and struct values survive intact."""
    batch = pa.RecordBatch.from_pylist(
        [
            {
                "nullable_float": 1.5,
                "tags": ["alpha", None, "omega"],
                "category": "blue",
                "event_time": dt.datetime(2026, 8, 18, 12, 34, 56, tzinfo=dt.UTC),
                "amount": Decimal("1234.5000"),
                "nested": {"name": "sample", "scores": [1, None, 3]},
            }
        ],
        schema=TYPED_EXCHANGE_SCHEMA,
    )
    result = _exchange(batch)
    assert result.schema == TYPED_EXCHANGE_SCHEMA
    assert result.to_pylist() == batch.to_pylist()


def test_strict_http_worker_rejects_inferred_all_null_schema() -> None:
    """The HTTP worker inspects the pre-cast wire schema and rejects inference."""
    inferred = pa.RecordBatch.from_pylist(
        [
            {
                "nullable_float": None,
                "tags": None,
                "category": None,
                "event_time": None,
                "amount": None,
                "nested": None,
            }
        ]
    )
    body = BytesIO()
    with pa.ipc.new_stream(body, inferred.schema) as writer:
        writer.write_batch(inferred)

    delegated = False

    def delegate(environ: WSGIEnvironment, start_response: StartResponse) -> list[bytes]:
        nonlocal delegated
        delegated = True
        return [b""]

    status = ""

    def start_response(
        value: str,
        headers: list[tuple[str, str]],
        exc_info: object = None,
    ) -> Callable[[bytes], object]:
        nonlocal status
        status = value

        def write(data: bytes) -> object:
            return None

        return write

    environ = cast(
        "WSGIEnvironment",
        {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/typed_exchange/exchange",
            "CONTENT_LENGTH": str(len(body.getvalue())),
            "wsgi.input": BytesIO(body.getvalue()),
        },
    )
    result = StrictExchangeSchemaMiddleware(delegate)(environ, start_response)
    assert not delegated
    assert status == "400 Bad Request"
    assert b"schema mismatch" in b"".join(cast("list[bytes]", result))


def test_http_worker_enforces_wire_schema_end_to_end() -> None:
    """The documented module entry point accepts exact and rejects inferred IPC."""
    import httpx2

    proc = subprocess.Popen(
        [sys.executable, "-m", "vgi_rpc.conformance.client_worker", "--http", "0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdout is not None
        line = proc.stdout.readline().decode().strip()
        assert line.startswith("PORT:"), line
        base_url = f"http://127.0.0.1:{int(line.split(':', 1)[1])}"
        deadline = time.monotonic() + 5
        while True:
            try:
                httpx2.get(f"{base_url}/health", timeout=0.5)
                break
            except httpx2.TransportError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

        exact = pa.RecordBatch.from_arrays(
            [pa.array([None], type=field.type) for field in TYPED_EXCHANGE_SCHEMA],
            schema=TYPED_EXCHANGE_SCHEMA,
        )
        inferred = pa.RecordBatch.from_pylist([{field.name: None for field in TYPED_EXCHANGE_SCHEMA}])
        with http_connect(ClientConformanceService, base_url) as proxy:
            with proxy.typed_exchange() as session:
                assert session.exchange(AnnotatedBatch(exact)).batch.schema == TYPED_EXCHANGE_SCHEMA
            with proxy.typed_exchange() as session, pytest.raises(RpcError):
                session.exchange(AnnotatedBatch(inferred))
    finally:
        proc.terminate()
        proc.wait(timeout=5)


def test_worker_describe_contract_covers_native_client_methods() -> None:
    """The standalone worker exposes describe v4 without changing the canonical server."""
    with _running_worker("--prefix", "/vgi") as (base_url, _ca):
        _wait_ready(base_url, prefix="/vgi")
        description = http_introspect(base_url, prefix="/vgi")

    assert description.protocol_name == "ClientConformanceService"
    assert description.describe_version == "4"
    assert len(description.protocol_hash) == 64
    expected = {
        "typed_exchange",
        "producer_sequence",
        "producer_zero_row_then_value",
        "producer_emit_and_finish",
        "producer_empty",
        "open_client_session",
        "increment_client_session",
        "close_client_session",
        "large_response",
        "echo_bytes",
    }
    assert set(description.methods) == expected
    assert description.methods["typed_exchange"].is_exchange is True
    for name in (
        "producer_sequence",
        "producer_zero_row_then_value",
        "producer_emit_and_finish",
        "producer_empty",
    ):
        assert description.methods[name].is_exchange is False


def test_producer_cursor_contract_init_resume_zero_row_and_terminal() -> None:
    """Producer turns distinguish data, continuation sentinels, and terminal streams."""
    import httpx2

    headers = {"Content-Type": "application/vnd.apache.arrow.stream"}
    with _running_worker("--prefix", "/vgi") as (base_url, _ca):
        _wait_ready(base_url, prefix="/vgi")
        with httpx2.Client(base_url=base_url, trust_env=False) as client:
            init = client.post(
                "/vgi/producer_sequence/init",
                content=_request_body(
                    "producer_sequence",
                    _params("producer_sequence", count=2, payload_bytes=4),
                ),
                headers=headers,
            )
            init.raise_for_status()
            init_batches = _read_batches(init.content)
            assert len(init_batches) == 2
            first, first_md = init_batches[0]
            sentinel, sentinel_md = init_batches[1]
            assert first.schema == PRODUCER_SCHEMA
            assert first.column("index").to_pylist() == [0]
            assert first_md is None or first_md.get(STATE_KEY) is None
            assert sentinel.num_rows == 0
            assert sentinel_md is not None
            cursor = sentinel_md.get(STATE_KEY)
            call_state = sentinel_md.get(CALL_STATE_KEY)
            assert cursor is not None
            assert call_state is not None

            tick = pa.RecordBatch.from_pylist([], schema=pa.schema([]))
            resumed = client.post(
                "/vgi/producer_sequence/exchange",
                content=_request_body(
                    "producer_sequence",
                    tick,
                    {STATE_KEY: cursor, CALL_STATE_KEY: call_state},
                ),
                headers=headers,
            )
            resumed.raise_for_status()
            resumed_batches = _read_batches(resumed.content)
            assert len(resumed_batches) == 2
            assert resumed_batches[0][0].column("index").to_pylist() == [1]
            next_md = resumed_batches[1][1]
            assert next_md is not None
            next_cursor = next_md.get(STATE_KEY)
            assert next_cursor is not None
            assert next_md.get(CALL_STATE_KEY) is None, "call-state token is issued only on init"

            terminal = client.post(
                "/vgi/producer_sequence/exchange",
                content=_request_body(
                    "producer_sequence",
                    tick,
                    {STATE_KEY: next_cursor, CALL_STATE_KEY: call_state},
                ),
                headers=headers,
            )
            terminal.raise_for_status()
            assert _read_batches(terminal.content) == []

            zero_init = client.post(
                "/vgi/producer_zero_row_then_value/init",
                content=_request_body(
                    "producer_zero_row_then_value",
                    _params("producer_zero_row_then_value"),
                ),
                headers=headers,
            )
            zero_init.raise_for_status()
            zero_batches = _read_batches(zero_init.content)
            assert len(zero_batches) == 2
            assert zero_batches[0][0].num_rows == 0
            assert zero_batches[0][1] is None or zero_batches[0][1].get(STATE_KEY) is None
            assert zero_batches[1][0].num_rows == 0
            assert zero_batches[1][1] is not None
            zero_cursor = zero_batches[1][1].get(STATE_KEY)
            zero_call_state = zero_batches[1][1].get(CALL_STATE_KEY)
            assert zero_cursor is not None
            assert zero_call_state is not None

            zero_resumed = client.post(
                "/vgi/producer_zero_row_then_value/exchange",
                content=_request_body(
                    "producer_zero_row_then_value",
                    tick,
                    {STATE_KEY: zero_cursor, CALL_STATE_KEY: zero_call_state},
                ),
                headers=headers,
            )
            zero_resumed.raise_for_status()
            zero_resumed_batches = _read_batches(zero_resumed.content)
            assert len(zero_resumed_batches) == 2
            assert zero_resumed_batches[0][0].column("index").to_pylist() == [7]
            zero_next_md = zero_resumed_batches[1][1]
            assert zero_next_md is not None
            zero_next_cursor = zero_next_md.get(STATE_KEY)
            assert zero_next_cursor is not None

            zero_terminal = client.post(
                "/vgi/producer_zero_row_then_value/exchange",
                content=_request_body(
                    "producer_zero_row_then_value",
                    tick,
                    {STATE_KEY: zero_next_cursor, CALL_STATE_KEY: zero_call_state},
                ),
                headers=headers,
            )
            zero_terminal.raise_for_status()
            assert _read_batches(zero_terminal.content) == []

            finished = client.post(
                "/vgi/producer_emit_and_finish/init",
                content=_request_body("producer_emit_and_finish", _params("producer_emit_and_finish")),
                headers=headers,
            )
            finished.raise_for_status()
            finished_batches = _read_batches(finished.content)
            assert len(finished_batches) == 1
            assert finished_batches[0][0].column("index").to_pylist() == [99]
            assert finished_batches[0][1] is None or finished_batches[0][1].get(STATE_KEY) is None

            empty = client.post(
                "/vgi/producer_empty/init",
                content=_request_body("producer_empty", _params("producer_empty")),
                headers=headers,
            )
            empty.raise_for_status()
            assert _read_batches(empty.content) == []


def test_buffered_producer_init_returns_multiple_pending_batches_and_cursor() -> None:
    """A native producer client must queue every init data batch before the cursor."""
    import httpx2

    with _running_worker("--prefix", "/vgi", "--producer-turn-bytes", "16384") as (base_url, _ca):
        _wait_ready(base_url, prefix="/vgi")
        response = httpx2.post(
            f"{base_url}/vgi/producer_sequence/init",
            content=_request_body(
                "producer_sequence",
                _params("producer_sequence", count=100, payload_bytes=1024),
            ),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        response.raise_for_status()
        batches = _read_batches(response.content)

    assert len(batches) >= 3
    data = [(batch, metadata) for batch, metadata in batches if metadata is None or metadata.get(STATE_KEY) is None]
    assert len(data) >= 2
    assert [batch.column("index")[0].as_py() for batch, _metadata in data] == list(range(len(data)))
    sentinel, metadata = batches[-1]
    assert sentinel.num_rows == 0
    assert metadata is not None
    assert metadata.get(STATE_KEY) is not None
    assert metadata.get(CALL_STATE_KEY) is not None


def test_sticky_worker_mode_exercises_open_resume_close_and_stale_token() -> None:
    """The optional worker mode exposes the complete native sticky-token lifecycle."""
    with _running_worker("--prefix", "/vgi", "--sticky") as (base_url, _ca):
        _wait_ready(base_url, prefix="/vgi")
        capabilities = http_capabilities(base_url, prefix="/vgi")
        assert capabilities.sticky_enabled is True
        assert capabilities.sticky_default_ttl == 60
        assert capabilities.sticky_echo_headers == ("X-VGI-Worker-Affinity",)

        with http_connect(ClientConformanceService, base_url, prefix="/vgi") as typed_proxy:
            proxy = cast("Any", typed_proxy)
            with proxy.with_session_token() as session:
                assert session.open_client_session(initial=10) == 10
                token = session.current_session_token()
                assert token is not None
                assert session.increment_client_session(by=5) == 15
                assert session.increment_client_session(by=-2) == 13
                assert session.close_client_session() == 13
                assert session.current_session_token() is None

            with proxy.with_session_token(token=token) as stale, pytest.raises(RpcError) as excinfo:
                stale.increment_client_session(by=1)
            assert excinfo.value.error_type == "SessionLostError"


def test_external_worker_mode_covers_response_pointer_and_client_upload_flow() -> None:
    """External mode proves pointer fetches and upload-URL request externalization."""
    import httpx2

    expected = bytes(i % 251 for i in range(32 * 1024))
    external = ClientExternalConfig(url_validator=None)
    with _running_worker("--prefix", "/vgi", "--external", "--external-threshold", "4096") as (
        base_url,
        _ca,
    ):
        _wait_ready(base_url, prefix="/vgi")
        capabilities = http_capabilities(base_url, prefix="/vgi")
        assert capabilities.externalization_enabled is True
        assert capabilities.upload_url_support is True
        assert capabilities.max_request_bytes == 4096

        raw = httpx2.post(
            f"{base_url}/vgi/large_response",
            content=_request_body("large_response", _params("large_response", size=len(expected))),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        raw.raise_for_status()
        raw_batches = _read_batches(raw.content)
        assert len(raw_batches) == 1
        pointer, pointer_md = raw_batches[0]
        assert pointer.num_rows == 0
        assert pointer_md is not None
        assert pointer_md.get(LOCATION_KEY) is not None
        assert pointer_md.get(LOCATION_SHA256_KEY) is not None

        with http_connect(
            ClientConformanceService,
            base_url,
            prefix="/vgi",
            external_location=external,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        ) as proxy:
            assert proxy.large_response(size=len(expected)) == expected
            assert proxy.echo_bytes(value=expected) == expected


def test_tls_worker_mode_publishes_ca_and_requires_trust() -> None:
    """TLS mode gives native clients a real verified HTTPS acceptance fixture."""
    import httpx2

    with _running_worker("--prefix", "/vgi", "--tls", tls=True) as (base_url, ca_path):
        assert ca_path is not None
        trusted_context = ssl.create_default_context(cafile=ca_path)
        _wait_ready(base_url, prefix="/vgi", verify=trusted_context)
        with httpx2.Client(trust_env=False) as untrusted, pytest.raises(httpx2.TransportError):
            untrusted.get(f"{base_url}/vgi/health", timeout=1)
        with httpx2.Client(base_url=base_url, verify=trusted_context, trust_env=False) as trusted:
            description = http_introspect(client=trusted, prefix="/vgi")
            assert description.protocol_name == "ClientConformanceService"


def test_stdio_worker_exposes_raw_client_surface_and_dynamic_shm(monkeypatch: pytest.MonkeyPatch) -> None:
    """The spawnable raw worker covers negotiation, describe, and every RPC shape."""
    monkeypatch.setenv("VGI_RPC_SHM_MIN_BATCH_BYTES", "1")
    monkeypatch.setattr("vgi_rpc.shm.SHM_MIN_BATCH_BYTES", 1)
    command = [sys.executable, "-m", "vgi_rpc.conformance.client_worker", "--stdio"]
    transport = SubprocessTransport(command, stderr=StderrMode.DEVNULL)
    shm = ShmSegment.create(2 * 1024 * 1024)
    wrapped = ShmPipeTransport(cast("Any", transport), shm)
    try:
        description = introspect(transport)
        assert description.protocol_name == "ClientConformanceService"
        assert description.describe_version == "4"
        assert TRANSPORT_OPTIONS_METHOD_NAME not in description.methods
        options = _transport_options(transport)
        assert options.get(TRANSPORT_SHM_KEY) == (b"true" if shm_available() else b"false")
        assert options.get(REQUEST_VERSION_KEY) == REQUEST_VERSION
        assert options.get(SERVER_ID_KEY) == b"client-worker"

        payload = bytes(i % 251 for i in range(512 * 1024))
        with RpcConnection(ClientConformanceService, wrapped) as proxy:
            assert proxy.echo_bytes(value=payload) == payload

            produced = list(proxy.producer_sequence(count=3, payload_bytes=4))
            assert [item.batch.column("index")[0].as_py() for item in produced] == [0, 1, 2]

            batch = pa.RecordBatch.from_arrays(
                [pa.array([None], type=field.type) for field in TYPED_EXCHANGE_SCHEMA],
                schema=TYPED_EXCHANGE_SCHEMA,
            )
            with proxy.typed_exchange() as session:
                echoed = session.exchange(AnnotatedBatch(batch)).batch
                empty = pa.RecordBatch.from_arrays(
                    [pa.array([], type=field.type) for field in TYPED_EXCHANGE_SCHEMA],
                    schema=TYPED_EXCHANGE_SCHEMA,
                )
                echoed_empty = session.exchange(AnnotatedBatch(empty)).batch
            assert echoed.schema == TYPED_EXCHANGE_SCHEMA
            assert echoed.to_pylist() == batch.to_pylist()
            assert echoed_empty.schema == TYPED_EXCHANGE_SCHEMA
            assert echoed_empty.num_rows == 0
    finally:
        transport.close()
        shm.unlink()
        with suppress(BufferError):
            shm.close()


@pytest.mark.skipif(sys.platform == "win32", reason="AF_UNIX is unavailable on Windows")
def test_unix_worker_launch_discovery_and_round_trip() -> None:
    """The Unix launch mode announces its bound path and accepts raw RPCs."""
    with tempfile.TemporaryDirectory(prefix="vgi-rpc-client-unix-") as directory:
        socket_path = Path(directory) / "worker.sock"
        with _running_raw_socket_worker("--unix", str(socket_path)) as discovery:
            assert discovery == f"UNIX:{socket_path.resolve()}"
            with unix_connect(ClientConformanceService, str(socket_path)) as proxy:
                assert proxy.echo_bytes(value=b"unix") == b"unix"
                batches = list(proxy.producer_sequence(count=1, payload_bytes=2))
                assert batches[0].batch.to_pylist() == [{"index": 0, "payload": b"\x00\x00"}]
        assert not socket_path.exists()


def test_tcp_worker_launch_discovery_and_round_trip() -> None:
    """The TCP launch mode reports its ephemeral loopback port and accepts raw RPCs."""
    with _running_raw_socket_worker("--tcp", "127.0.0.1:0") as discovery:
        advertised = discovery.removeprefix("TCP:")
        host, separator, raw_port = advertised.rpartition(":")
        assert separator == ":"
        assert host == "127.0.0.1"
        port = int(raw_port)
        assert port > 0
        with tcp_connect(ClientConformanceService, host, port) as proxy:
            assert proxy.echo_bytes(value=b"tcp") == b"tcp"
