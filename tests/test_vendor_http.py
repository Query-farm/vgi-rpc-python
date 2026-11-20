# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""End-to-end: an HTTP server configured from the blob-vendor environment.

A real waitress server runs ``external_config_from_env()`` against the
in-process fake vendor.  Large results are externalized through the vendor's
sized grants and resolved by the Python client; a client request over
``max_request_bytes`` goes through a sized vendor grant (upload-URL protocol
v2, ``/v1/blobs/put``) and comes back to the server as a host-pinned pointer
carrying its SHA-256.  This is the focused equivalent
of the conformance external suites, which are wired to the fake-storage
service's own ``/alloc`` API and cannot take a pluggable backend.
"""

from __future__ import annotations

import contextlib
import socket
import threading
import time
from collections.abc import Generator, Iterator
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
    with http_connect(_Svc, deployment.base_url, external_location=client_cfg) as proxy:
        assert proxy.big(n=50_000) == "v" * 50_000
        assert proxy.big(n=10) == "v" * 10
    stats = deployment.fake.vendor.stats()
    assert stats["/v1/blobs/put"] == 1  # only the large result was vendored
    assert stats["r2_put"] == 1
    assert stats["r2_get"] >= 1


def test_large_stream_output_is_externalized(deployment: _Deployment) -> None:
    """A producer-stream batch over the threshold is externalized too."""
    client_cfg = client_external_config_from_env()
    with http_connect(_Svc, deployment.base_url, external_location=client_cfg) as proxy:
        batches = list(proxy.big_stream(size=10_000))
    assert sum(ab.batch.num_rows for ab in batches) == 10_000
    assert any(
        ab.custom_metadata is not None and ab.custom_metadata.get(LOCATION_SOURCE_KEY) is not None for ab in batches
    )
    assert deployment.fake.vendor.stats()["/v1/blobs/put"] == 1


def test_large_client_request_goes_through_sized_vendor_grant(deployment: _Deployment) -> None:
    """A request over max_request_bytes is PUT to a sized, checksummed vendor URL and resolved by the server."""
    client_cfg = client_external_config_from_env()
    payload = "q" * (_MAX_REQUEST * 2)
    with http_connect(_Svc, deployment.base_url, external_location=client_cfg, compression_level=None) as proxy:
        assert proxy.length(data=payload) == len(payload)
    vendor = deployment.fake.vendor
    stats = vendor.test_stats()
    assert stats["puts"] == 1 and stats["upload_urls"] == 0  # the unsized path is no longer used
    assert stats["upload_refusals"] == 0
    # The server marked the grant as a client upload (origin=client_upload).
    assert stats["puts_upload"] == 1 and stats["puts_rpc"] == 0
    assert stats["client_uploads"] == 1 and stats["client_upload_bytes"] == stats["bytes_uploaded"]
    puts = [headers for method, path, headers in vendor.requests if method == "PUT"]
    assert len(puts) == 1
    assert "HTTP_AUTHORIZATION" not in puts[0]
    assert "HTTP_X_AMZ_CHECKSUM_SHA256" in puts[0]


def test_corrupted_client_upload_is_refused_by_storage(deployment: _Deployment) -> None:
    """The upload fault damages the PUT in transit; the store refuses it (BadDigest) and the call fails."""
    vendor = deployment.fake.vendor
    vendor.set_upload_fault("corrupt", 1)
    client_cfg = client_external_config_from_env()
    payload = "k" * (_MAX_REQUEST * 2)
    with (
        http_connect(_Svc, deployment.base_url, external_location=client_cfg, compression_level=None) as proxy,
        pytest.raises(RpcError, match="BadDigest"),
    ):
        proxy.length(data=payload)
    stats = vendor.test_stats()
    assert stats["uploads_corrupted"] == 1
    assert stats["upload_bad_digest"] == 1 and stats["upload_refusals"] == 1
    assert stats["client_upload_refusals"] == 1 and stats["client_uploads"] == 0
    # The fault is spent: the next call goes through.
    with http_connect(_Svc, deployment.base_url, external_location=client_cfg, compression_level=None) as proxy:
        assert proxy.length(data=payload) == len(payload)


def test_client_upload_put_is_octet_stream(deployment: _Deployment, monkeypatch: pytest.MonkeyPatch) -> None:
    """The client's PUT to a vended upload URL declares application/octet-stream."""
    seen: list[str] = []
    original_send = httpx2.Client.send

    def spy(self: httpx2.Client, request: httpx2.Request, **kwargs: object) -> httpx2.Response:
        if request.method == "PUT":
            seen.append(str(request.headers.get("Content-Type")))
        return original_send(self, request, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(httpx2.Client, "send", spy)
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
    # A well-formed digest, so the refusal is the host pin and not the missing sha256.
    pointer, loc = make_external_location_batch(batch.schema, f"http://localhost:{port}/r2/get/nope", sha256="e" * 64)
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
    assert caps.upload_url_version == 2
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
    monkeypatch.setenv("VGI_RPC_MAX_REQUEST_BYTES", "65536")
    run_server(_Svc, _SvcImpl())
    ((server, kwargs),) = calls
    assert server.external_config is not None
    assert isinstance(server.external_config.storage, VendorStorage)
    assert kwargs["upload_url_provider"] is server.external_config.storage
    assert kwargs["max_upload_bytes"] == 1234
    assert kwargs["max_request_bytes"] == 65536
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
    assert kwargs["upload_url_provider"] is None


class _InjectAuth(httpx2.Auth):
    """An auth flow that stamps a credential on every request it sees."""

    def auth_flow(self, request: httpx2.Request) -> Generator[httpx2.Request, httpx2.Response]:
        request.headers["Authorization"] = "Bearer rpc-auth-flow-secret"
        request.headers["X-Auth-Flow"] = "1"
        yield request


def _leaky_client(base_url: str | None = None) -> httpx2.Client:
    """Build an httpx2 client carrying every kind of caller credential."""

    def hook(request: httpx2.Request) -> None:
        request.headers["X-Hook"] = "rpc-hook-secret"

    kwargs: dict[str, object] = {}
    if base_url is not None:
        kwargs["base_url"] = base_url
    return httpx2.Client(
        headers={"Authorization": "Bearer rpc-default-secret", "X-Custom": "rpc-custom-secret"},
        cookies={"session": "rpc-cookie-secret"},
        auth=_InjectAuth(),
        event_hooks={"request": [hook]},
        timeout=30.0,
        **kwargs,  # type: ignore[arg-type]
    )


_CALLER_HEADERS = frozenset({"HTTP_AUTHORIZATION", "HTTP_COOKIE", "HTTP_X_CUSTOM", "HTTP_X_HOOK", "HTTP_X_AUTH_FLOW"})


def test_client_upload_put_never_carries_caller_credentials(deployment: _Deployment) -> None:
    """A client upload PUT carries none of the RPC client's headers, cookies, auth flow or hooks."""
    client_cfg = client_external_config_from_env()
    with (
        _leaky_client(deployment.base_url) as rpc_client,
        http_connect(_Svc, client=rpc_client, external_location=client_cfg, compression_level=None) as proxy,
    ):
        assert proxy.length(data="c" * (_MAX_REQUEST * 2)) == _MAX_REQUEST * 2
    vendor = deployment.fake.vendor
    puts = [headers for method, path, headers in vendor.requests if method == "PUT"]
    assert len(puts) == 1
    assert not puts[0] & _CALLER_HEADERS, sorted(puts[0] & _CALLER_HEADERS)


def test_vendor_clients_never_send_session_credentials(deployment: _Deployment) -> None:
    """VendorStorage / VendorBlobClient storage PUT/GET carry no session credentials.

    Vendor API requests carry the vendor token (not the session's auth flow)
    and none of the session's default headers or cookies; the session's event
    hooks, which the caller installed on purpose, do run for them.
    """
    from vgi_rpc.vendor import VendorBlobClient

    fake = deployment.fake
    with _leaky_client() as session:
        with VendorStorage(fake.url, _TOKEN, session=session) as storage:
            url = storage.upload(b"payload" * 100, pa.schema([]))
            pair = storage.generate_upload_url(10, "a" * 64)
        with VendorBlobClient(fake.url, _TOKEN, session=session) as blobs:
            ref = blobs.put_bytes(b"blob" * 100)
            assert blobs.get_bytes(ref) == b"blob" * 100
    assert url and pair.upload_url
    storage_requests = [h for m, p, h in fake.vendor.requests if p.startswith("/r2/")]
    assert len(storage_requests) >= 4  # 2 PUTs + probe/GET for the blob
    for headers in storage_requests:
        assert not headers & _CALLER_HEADERS, sorted(headers & _CALLER_HEADERS)
    api_requests = [h for m, p, h in fake.vendor.requests if p.startswith("/v1/")]
    assert api_requests
    for headers in api_requests:
        assert "HTTP_AUTHORIZATION" in headers  # the vendor token; a wrong one would 401
        assert not headers & (_CALLER_HEADERS - {"HTTP_AUTHORIZATION", "HTTP_X_HOOK"})
