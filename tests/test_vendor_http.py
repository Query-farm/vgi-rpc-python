# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""End-to-end: an HTTP server configured from the blob-vendor environment.

A real waitress server runs ``external_config_from_env()`` against the
in-process fake vendor.  Large results are externalized through the vendor's
sized grants and resolved by the Python client; a client request over
``max_request_bytes`` goes through the vendor's ``/v1/upload_urls`` and comes
back to the server as a host-pinned pointer.  This is the focused equivalent
of the conformance external suites, which are wired to the fake-storage
service's own ``/alloc`` API and cannot take a pluggable backend.
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol

import httpx2
import pyarrow as pa
import pytest

from vgi_rpc.conformance.fake_vendor import FakeVendorServer, serve_fake_vendor
from vgi_rpc.http import http_connect, make_wsgi_app
from vgi_rpc.metadata import LOCATION_SOURCE_KEY
from vgi_rpc.rpc import CallContext, OutputCollector, RpcError, RpcServer, Stream, StreamState
from vgi_rpc.vendor import VendorStorage, client_external_config_from_env, external_config_from_env

_TOKEN = "e2e-token"
_THRESHOLD = 4096
_MAX_REQUEST = 64 * 1024


@dataclass
class _BigState(StreamState):
    """Emit one large batch, then finish."""

    size: int
    done: bool = False

    def process(self, input: object, out: OutputCollector, ctx: CallContext) -> None:
        """Emit or finish."""
        if self.done:
            out.finish()
            return
        self.done = True
        out.emit_pydict({"value": list(range(self.size))})


class _Svc(Protocol):
    def big(self, n: int) -> str: ...

    def length(self, data: str) -> int: ...

    def big_stream(self, size: int) -> Stream[StreamState]: ...


class _SvcImpl:
    def big(self, n: int) -> str:
        return "v" * n

    def length(self, data: str) -> int:
        return len(data)

    def big_stream(self, size: int) -> Stream[_BigState]:
        return Stream(output_schema=pa.schema([pa.field("value", pa.int64())]), state=_BigState(size=size))


@dataclass
class _Deployment:
    fake: FakeVendorServer
    base_url: str


@pytest.fixture
def deployment(monkeypatch: pytest.MonkeyPatch) -> Iterator[_Deployment]:
    """Run a waitress server configured purely from the vendor environment."""
    import waitress

    with serve_fake_vendor({_TOKEN: "tenant-e2e"}) as fake:
        monkeypatch.setenv("VGI_BLOB_VENDOR_URL", fake.url)
        monkeypatch.delenv("VGI_BLOB_VENDOR_TOKEN", raising=False)
        monkeypatch.setenv("VGI_CF_DO_TOKEN", _TOKEN)
        monkeypatch.setenv("VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", str(_THRESHOLD))
        monkeypatch.setenv("VGI_RPC_EXTERNAL_COMPRESSION", "zstd")
        config = external_config_from_env()
        assert config is not None and isinstance(config.storage, VendorStorage)
        app = make_wsgi_app(
            RpcServer(_Svc, _SvcImpl(), external_location=config),
            token_key=b"vendor-e2e",
            max_request_bytes=_MAX_REQUEST,
            upload_url_provider=config.storage,
            max_upload_bytes=16 * 1024 * 1024,
        )
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = int(s.getsockname()[1])
        server = waitress.create_server(app, host="127.0.0.1", port=port)

        def run() -> None:
            with contextlib.suppress(OSError):
                server.run()

        threading.Thread(target=run, daemon=True).start()
        base_url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 5.0
        while True:
            try:
                httpx2.get(base_url, timeout=0.5)
                break
            except httpx2.TransportError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)
        try:
            yield _Deployment(fake, base_url)
        finally:
            server.close()
            config.storage.close()


def test_large_unary_result_is_externalized_and_resolved(deployment: _Deployment) -> None:
    """A result over the threshold goes through a sized vendor grant; the client resolves it."""
    client_cfg = client_external_config_from_env()
    with http_connect(_Svc, deployment.base_url, external_location=client_cfg) as proxy:  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        assert proxy.big(n=50_000) == "v" * 50_000
        assert proxy.big(n=10) == "v" * 10
    stats = deployment.fake.vendor.stats()
    assert stats["/v1/blobs/put"] == 1  # only the large result was vendored
    assert stats["r2_put"] == 1
    assert stats["r2_get"] >= 1


def test_large_stream_output_is_externalized(deployment: _Deployment) -> None:
    """A producer-stream batch over the threshold is externalized too."""
    client_cfg = client_external_config_from_env()
    with http_connect(_Svc, deployment.base_url, external_location=client_cfg) as proxy:  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        batches = list(proxy.big_stream(size=10_000))
    assert sum(ab.batch.num_rows for ab in batches) == 10_000
    assert any(
        ab.custom_metadata is not None and ab.custom_metadata.get(LOCATION_SOURCE_KEY) is not None for ab in batches
    )
    assert deployment.fake.vendor.stats()["/v1/blobs/put"] == 1


def test_large_client_request_goes_through_vendor_upload_urls(deployment: _Deployment) -> None:
    """A request over max_request_bytes is PUT to a vendor upload URL and resolved by the server."""
    client_cfg = client_external_config_from_env()
    payload = "q" * (_MAX_REQUEST * 2)
    with http_connect(_Svc, deployment.base_url, external_location=client_cfg, compression_level=None) as proxy:  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
        assert proxy.length(data=payload) == len(payload)
    vendor = deployment.fake.vendor
    assert vendor.stats()["/v1/upload_urls"] == 1
    puts = [headers for method, path, headers in vendor.requests if method == "PUT"]
    assert len(puts) == 1
    assert "HTTP_AUTHORIZATION" not in puts[0]


def test_client_upload_put_is_octet_stream(deployment: _Deployment, monkeypatch: pytest.MonkeyPatch) -> None:
    """The client's PUT to a vended upload URL declares application/octet-stream."""
    seen: list[str] = []
    original_put = httpx2.Client.put

    def spy(self: httpx2.Client, url: str, **kwargs: object) -> httpx2.Response:
        headers = kwargs.get("headers")
        if isinstance(headers, dict):
            seen.append(str(headers.get("Content-Type")))
        return original_put(self, url, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx2.Client, "put", spy)
    with http_connect(_Svc, deployment.base_url, compression_level=None) as proxy:
        assert proxy.length(data="z" * (_MAX_REQUEST * 2)) == _MAX_REQUEST * 2
    assert seen == ["application/octet-stream"]


def test_server_rejects_client_pointer_to_foreign_host(deployment: _Deployment) -> None:
    """A client pointer outside the vendor's hosts is refused by the server's pinned validator."""
    from io import BytesIO

    from pyarrow import ipc

    from vgi_rpc.external import make_external_location_batch
    from vgi_rpc.metadata import merge_metadata
    from vgi_rpc.rpc import rpc_methods
    from vgi_rpc.rpc._wire import _write_request
    from vgi_rpc.utils import new_ipc_stream

    info = rpc_methods(_Svc)["length"]
    buf = BytesIO()
    _write_request(buf, "length", info.params_schema, {"data": "x"}, protocol="_Svc")
    batch, md = ipc.open_stream(BytesIO(buf.getvalue())).read_next_batch_with_custom_metadata()
    port = deployment.fake.url.rsplit(":", 1)[1]
    pointer, loc = make_external_location_batch(batch.schema, f"http://localhost:{port}/r2/get/nope")
    out = BytesIO()
    with new_ipc_stream(out, batch.schema) as writer:
        writer.write_batch(pointer, custom_metadata=merge_metadata(md, loc))
    resp = httpx2.post(
        f"{deployment.base_url}/_Svc/length",
        content=out.getvalue(),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert b"pinned" in resp.content or b"rejected" in resp.content
    assert deployment.fake.vendor.stats().get("r2_get", 0) == 0


def test_capabilities_advertise_vendor_uploads(deployment: _Deployment) -> None:
    """The env-configured server advertises externalization and upload URLs."""
    from vgi_rpc.http import http_capabilities

    caps = http_capabilities(deployment.base_url)
    assert caps.upload_url_support
    assert caps.max_upload_bytes == 16 * 1024 * 1024
    assert caps.max_request_bytes == _MAX_REQUEST


def test_vendor_outage_surfaces_as_rpc_error(deployment: _Deployment) -> None:
    """When the vendor is down, a large result fails as an RPC error without leaking the token."""
    deployment.fake.vendor.fail_next("/v1/blobs/put", status=503, count=10)
    with (
        http_connect(_Svc, deployment.base_url) as proxy,
        pytest.raises(RpcError) as exc_info,
    ):
        proxy.big(n=50_000)
    assert _TOKEN not in str(exc_info.value) + (exc_info.value.remote_traceback or "")


def test_run_server_http_applies_vendor_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """run_server --http builds the vendor config and vends upload URLs from it."""
    from vgi_rpc.rpc import run_server

    calls: list[tuple[RpcServer, dict[str, object]]] = []

    def fake_serve_http(server: RpcServer, **kwargs: object) -> None:
        calls.append((server, kwargs))

    monkeypatch.setattr("vgi_rpc.http.serve_http", fake_serve_http)
    monkeypatch.setattr("sys.argv", ["worker", "--http"])
    monkeypatch.setenv("VGI_BLOB_VENDOR_URL", "https://blobs.example")
    monkeypatch.setenv("VGI_BLOB_VENDOR_TOKEN", "t")
    monkeypatch.setenv("VGI_RPC_MAX_UPLOAD_BYTES", "1234")
    run_server(_Svc, _SvcImpl())
    ((server, kwargs),) = calls
    assert server.external_config is not None
    assert isinstance(server.external_config.storage, VendorStorage)
    assert kwargs["upload_url_provider"] is server.external_config.storage
    assert kwargs["max_upload_bytes"] == 1234
    server.external_config.storage.close()


def test_run_server_without_vendor_env_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without VGI_BLOB_VENDOR_URL, run_server --http passes no upload provider."""
    from vgi_rpc.rpc import run_server

    calls: list[tuple[RpcServer, dict[str, object]]] = []

    def fake_serve_http(server: RpcServer, **kwargs: object) -> None:
        calls.append((server, kwargs))

    monkeypatch.setattr("vgi_rpc.http.serve_http", fake_serve_http)
    monkeypatch.setattr("sys.argv", ["worker", "--http"])
    monkeypatch.delenv("VGI_BLOB_VENDOR_URL", raising=False)
    run_server(_Svc, _SvcImpl())
    ((server, kwargs),) = calls
    assert server.external_config is None
    assert "upload_url_provider" not in kwargs
