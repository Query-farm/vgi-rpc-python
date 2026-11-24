# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Storage transfers on slow and stalled links.

Every storage HTTP request is bounded by a connect timeout and a *stall*
timeout (no bytes moving), never by a whole-request deadline.  These tests run
a local TLS server that trickles bytes at a fixed rate (or stops moving them)
and check that:

* a transfer much longer than the stall timeout succeeds with exactly one
  request (nothing killed and retried), and
* a transfer that stops moving bytes fails within the stall timeout plus
  slack.

TLS matters: CPython's ``SSLSocket.send`` writes a whole buffer under one
timeout, so a body handed to the socket in one piece gets a deadline for all
of it.  Over plain HTTP that bug is invisible.
"""

from __future__ import annotations

import contextlib
import datetime
import ipaddress
import os
import socket
import ssl
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx2
import pytest

from vgi_rpc.external_fetch import (
    DEFAULT_STORAGE_STALL_TIMEOUT_SECONDS,
    ENV_STORAGE_STALL_TIMEOUT,
    FetchConfig,
    fetch_url,
    iter_upload_chunks,
    storage_stall_timeout,
)
from vgi_rpc.http import storage_put
from vgi_rpc.vendor import PutGrant, VendorError, _download_capped, _VendorApi

#: Stall timeout used by every test here (seconds).
STALL = 2.0
#: Trickle rate (bytes/second) and body size: each transfer takes ~7 s, over
#: three times the stall timeout, so a whole-request deadline of STALL fails.
#: The rate keeps the end-of-upload drain of the kernel send buffer (Linux
#: autotunes it up to 4 MiB) under STALL: after its last write the client
#: waits for the response under the read (stall) timeout while the server
#: is still reading buffered bytes.
RATE = 4_000_000
SIZE = 28_000_000
#: Allowed lateness for a stalled transfer to fail.
SLACK = 3.0


# ---------------------------------------------------------------------------
# A slow TLS storage server
# ---------------------------------------------------------------------------


def _self_signed(tmp: Path) -> tuple[Path, Path]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp / "cert.pem", tmp / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return cert_path, key_path


@dataclass
class SlowStorage:
    """Request counters and the base URL of a running :func:`slow_storage`."""

    url: str
    requests: dict[str, int] = field(default_factory=dict)
    received: dict[str, int] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def count(self, path: str) -> int:
        """Return the number of requests seen for *path*."""
        with self.lock:
            return self.requests.get(path, 0)


def _pace(start: float, moved: int, rate: int) -> None:
    ahead = moved / rate - (time.monotonic() - start)
    if ahead > 0:
        time.sleep(ahead)


def _serve_one(conn: ssl.SSLSocket, state: SlowStorage, stop: threading.Event) -> None:
    with conn:
        conn.settimeout(30)
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = conn.recv(65536)
            if not chunk:
                return
            buf += chunk
        head, body = buf.split(b"\r\n\r\n", 1)
        lines = head.decode("latin-1").split("\r\n")
        method, target, _ = lines[0].split(" ", 2)
        headers = {k.strip().lower(): v.strip() for k, v in (line.split(":", 1) for line in lines[1:])}
        parts = urlsplit(target)
        path, query = parts.path, {k: int(v[0]) for k, v in parse_qs(parts.query).items()}
        with state.lock:
            state.requests[path] = state.requests.get(path, 0) + 1
        rate = query.get("rate", RATE)
        stall_after = query.get("stall_after", -1)

        if method == "PUT":
            assert "transfer-encoding" not in headers, "upload must be Content-Length framed"
            length = int(headers["content-length"])
            got, start = len(body), time.monotonic()
            while got < length:
                if 0 <= stall_after <= got:
                    # Stop reading: the client's writes back up and stall.
                    stop.wait(60)
                    return
                chunk = conn.recv(min(16384, length - got))
                if not chunk:
                    return
                got += len(chunk)
                _pace(start, got, rate)
            with state.lock:
                state.received[path] = got
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            return

        # GET: a body of `size` bytes trickled at `rate`.  A Range request gets
        # a 206 for its range (the fetch probe sends Range: bytes=0-0).
        size = query.get("size", SIZE)
        start_byte, end_byte = 0, size - 1
        status = "200 OK"
        extra = ""
        if "range" in headers:
            lo, hi = headers["range"].removeprefix("bytes=").split("-")
            start_byte, end_byte = int(lo), min(int(hi), size - 1)
            status = "206 Partial Content"
            extra = f"Content-Range: bytes {start_byte}-{end_byte}/{size}\r\n"
        length = end_byte - start_byte + 1
        conn.sendall(
            f"HTTP/1.1 {status}\r\nContent-Length: {length}\r\nAccept-Ranges: bytes\r\n{extra}"
            "Connection: close\r\n\r\n".encode()
        )
        if method == "HEAD":
            return
        sent, start, block = 0, time.monotonic(), b"\x5a" * 16384
        while sent < length:
            if 0 <= stall_after <= sent:
                stop.wait(60)
                return
            n = min(len(block), length - sent)
            conn.sendall(block[:n])
            sent += n
            _pace(start, sent, rate)


@pytest.fixture
def slow_storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[SlowStorage]:
    """Run a TLS server on 127.0.0.1 that trickles or stalls request and response bodies.

    ``PUT <path>?rate=R[&stall_after=N]`` reads the body at R bytes/s (or stops
    reading after N bytes); ``GET <path>?size=S&rate=R[&stall_after=N]`` sends S
    bytes at R bytes/s (or stops after N).  The certificate is trusted through
    ``SSL_CERT_FILE``, which httpx2 and aiohttp both honour.
    """
    import aiohttp.connector

    cert, key = _self_signed(tmp_path)
    monkeypatch.setenv("SSL_CERT_FILE", str(cert))
    # aiohttp builds its verifying context at import, before SSL_CERT_FILE is set.
    client_ctx = ssl.create_default_context(cafile=str(cert))
    monkeypatch.setattr(aiohttp.connector, "_SSL_CONTEXT_VERIFIED", client_ctx)
    monkeypatch.setenv(ENV_STORAGE_STALL_TIMEOUT, str(STALL))
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert, key)
    listener = socket.socket()
    # A small receive buffer makes a slow reader push back on the client fast.
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    state = SlowStorage(url=f"https://127.0.0.1:{listener.getsockname()[1]}")
    stop = threading.Event()

    def accept_loop() -> None:
        while not stop.is_set():
            try:
                raw, _ = listener.accept()
            except OSError:
                return
            try:
                conn = ctx.wrap_socket(raw, server_side=True)
            except (OSError, ssl.SSLError):
                raw.close()
                continue
            threading.Thread(target=_serve_one, args=(conn, state, stop), daemon=True).start()

    thread = threading.Thread(target=accept_loop, daemon=True)
    thread.start()
    try:
        yield state
    finally:
        stop.set()
        # shutdown() wakes the blocked accept(); close() alone does not.
        with contextlib.suppress(OSError):
            listener.shutdown(socket.SHUT_RDWR)
        listener.close()
        thread.join(timeout=5)


def _api(max_attempts: int = 3) -> _VendorApi:
    return _VendorApi(
        "https://vendor.invalid", "token", timeout=STALL, session=None, max_attempts=max_attempts, backoff_seconds=0.0
    )


def _grant(url: str, size: int) -> PutGrant:
    return PutGrant(
        object_id="obj",
        upload_url=url,
        required_headers={"content-length": str(size)},
        download_url=None,
        expires_at=datetime.datetime(2099, 1, 1, tzinfo=datetime.UTC),
        region=None,
        download_expires_at=None,
    )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class TestStallTimeoutSetting:
    """``VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS`` parsing."""

    def test_default(self) -> None:
        """Unset means 30 s."""
        assert storage_stall_timeout({}) == DEFAULT_STORAGE_STALL_TIMEOUT_SECONDS == 30.0

    def test_override(self) -> None:
        """A positive number is used as-is."""
        assert storage_stall_timeout({ENV_STORAGE_STALL_TIMEOUT: "7.5"}) == 7.5

    @pytest.mark.parametrize("raw", ["", "  ", "abc", "0", "-3", "nan", "inf"])
    def test_invalid_falls_back(self, raw: str) -> None:
        """Empty, non-numeric, non-positive and non-finite values give the default."""
        assert storage_stall_timeout({ENV_STORAGE_STALL_TIMEOUT: raw}) == DEFAULT_STORAGE_STALL_TIMEOUT_SECONDS

    def test_upload_chunks(self) -> None:
        """Bodies are split into 64 KiB pieces that rejoin exactly."""
        data = bytes(range(256)) * 1000
        pieces = list(iter_upload_chunks(data))
        assert b"".join(pieces) == data
        assert max(map(len, pieces)) == 64 * 1024
        assert list(iter_upload_chunks(b"")) == []

    def test_fetch_config_has_no_total_deadline(self) -> None:
        """FetchConfig bounds connect and stalls, not the whole request."""
        from vgi_rpc.external_fetch import _client_timeout

        timeout = _client_timeout(FetchConfig(stall_timeout_seconds=12.0))
        assert timeout.total is None
        assert timeout.sock_connect == 30.0
        assert timeout.sock_read == 12.0


# ---------------------------------------------------------------------------
# Slow but progressing: one attempt, success
# ---------------------------------------------------------------------------


class TestSlowTransfersSucceed:
    """Transfers lasting ~4x the stall timeout complete in one request."""

    def test_vendor_signed_put(self, slow_storage: SlowStorage) -> None:
        """A signed-URL upload (externalized result, StateRef) is not cut off."""
        api = _api()
        url = f"{slow_storage.url}/put?rate={RATE}"
        t0 = time.monotonic()
        api.put_object(_grant(url, SIZE), b"\x01" * SIZE)
        elapsed = time.monotonic() - t0
        assert elapsed > 3 * STALL
        assert slow_storage.count("/put") == 1
        assert slow_storage.received["/put"] == SIZE
        api.close()

    def test_vendor_signed_get(self, slow_storage: SlowStorage) -> None:
        """A signed-URL download (argument / StateRef fetch by id) is not cut off."""
        api = _api()
        t0 = time.monotonic()
        data = _download_capped(api, f"{slow_storage.url}/get?size={SIZE}&rate={RATE}", SIZE)
        assert len(data) == SIZE
        assert time.monotonic() - t0 > 3 * STALL
        assert slow_storage.count("/get") == 1
        api.close()

    def test_client_upload_url_put(self, slow_storage: SlowStorage) -> None:
        """The client's PUT to a server-vended upload URL is not cut off."""
        t0 = time.monotonic()
        resp = storage_put(f"{slow_storage.url}/upload?rate={RATE}", b"\x02" * SIZE)
        assert resp.status_code == 200
        assert time.monotonic() - t0 > 3 * STALL
        assert slow_storage.count("/upload") == 1
        assert slow_storage.received["/upload"] == SIZE

    def test_fetch_url_get(self, slow_storage: SlowStorage) -> None:
        """An ExternalLocation fetch (plain GET) is not cut off."""
        with FetchConfig() as config:
            t0 = time.monotonic()
            data = fetch_url(f"{slow_storage.url}/fetch?size={SIZE}&rate={RATE}", config)
        assert len(data) == SIZE
        assert time.monotonic() - t0 > 3 * STALL
        # A HEAD probe, then one GET (below the parallel Range threshold).
        assert slow_storage.count("/fetch") == 2


# ---------------------------------------------------------------------------
# Stalled: fail within the stall timeout plus slack
# ---------------------------------------------------------------------------


class TestStalledTransfersFail:
    """A transfer that stops moving bytes fails after about one stall timeout."""

    def test_vendor_signed_put_stall(self, slow_storage: SlowStorage) -> None:
        """The storage stops reading the upload body."""
        api = _api(max_attempts=1)
        url = f"{slow_storage.url}/put-stall?rate={RATE}&stall_after=4000000"
        t0 = time.monotonic()
        with pytest.raises(VendorError, match=r"WriteTimeout|ReadTimeout"):
            api.put_object(_grant(url, SIZE), b"\x01" * SIZE)
        # ~1 s of trickling before the stall, then STALL; socket buffers add a
        # little, never the whole body's worth.
        assert time.monotonic() - t0 < 1.0 + STALL + SLACK
        api.close()

    def test_vendor_signed_put_stall_retries(self, slow_storage: SlowStorage) -> None:
        """A stall is transient: the upload is retried, up to max_attempts."""
        api = _api(max_attempts=3)
        url = f"{slow_storage.url}/put-stall3?rate=4000000&stall_after=200000"
        with pytest.raises(VendorError, match="after 3 attempts"):
            api.put_object(_grant(url, SIZE), b"\x01" * SIZE)
        assert slow_storage.count("/put-stall3") == 3
        api.close()

    def test_vendor_signed_get_stall(self, slow_storage: SlowStorage) -> None:
        """The storage stops sending the download body."""
        api = _api(max_attempts=1)
        t0 = time.monotonic()
        with pytest.raises(VendorError, match="ReadTimeout"):
            _download_capped(api, f"{slow_storage.url}/get-stall?size={SIZE}&rate={RATE}&stall_after=2000000", SIZE)
        assert time.monotonic() - t0 < 0.5 + STALL + SLACK
        api.close()

    def test_client_upload_url_put_stall(self, slow_storage: SlowStorage, monkeypatch: pytest.MonkeyPatch) -> None:
        """The client's upload-URL PUT stalls: a transport error after the retries."""
        monkeypatch.setattr("vgi_rpc.http._client._STORAGE_PUT_BACKOFF_SECONDS", 0.0)
        t0 = time.monotonic()
        with pytest.raises(httpx2.TimeoutException):
            storage_put(f"{slow_storage.url}/upload-stall?rate=4000000&stall_after=200000", b"\x02" * SIZE)
        assert slow_storage.count("/upload-stall") == 3
        assert time.monotonic() - t0 < 3 * (STALL + SLACK)

    def test_fetch_url_stall(self, slow_storage: SlowStorage) -> None:
        """An ExternalLocation fetch whose body stops arriving."""
        with FetchConfig() as config:
            t0 = time.monotonic()
            with pytest.raises((TimeoutError, OSError)):
                fetch_url(f"{slow_storage.url}/fetch-stall?size={SIZE}&rate={RATE}&stall_after=2000000", config)
        assert time.monotonic() - t0 < 2 * (0.5 + STALL) + SLACK


# ---------------------------------------------------------------------------
# S3 / GCS backend settings
# ---------------------------------------------------------------------------


class TestBackendTimeouts:
    """The S3 and GCS backends use connect + stall timeouts and bounded retries."""

    def test_s3_client_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Botocore gets connect 30 s, read (stall) from the env, 3 standard attempts, and the addressing style."""
        pytest.importorskip("boto3")
        import boto3

        from vgi_rpc.s3 import S3Storage

        # boto3's default session caches credentials: start from a fresh one.
        monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
        monkeypatch.setenv(ENV_STORAGE_STALL_TIMEOUT, "45")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        storage = S3Storage(
            bucket="b", endpoint_url="http://minio.internal:9000", region_name="auto", addressing_style="path"
        )
        config = storage._get_client().meta.config
        assert config.connect_timeout == 30.0
        assert config.read_timeout == 45.0
        assert config.retries == {"total_max_attempts": 3, "mode": "standard"}
        assert config.s3 == {"addressing_style": "path"}
        url = storage.generate_upload_url(10, "0" * 64).upload_url
        assert url.startswith("http://minio.internal:9000/b/vgi-rpc/")
        assert S3Storage(bucket="b", stall_timeout_seconds=5.0)._get_client().meta.config.read_timeout == 5.0

    def test_s3_upload_body_is_a_stream(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Uploads hand botocore a file object, so the body is written in small timed blocks."""
        pytest.importorskip("boto3")
        from unittest.mock import MagicMock

        from vgi_rpc.s3 import S3Storage

        storage = S3Storage(bucket="b")
        client = MagicMock()
        client.generate_presigned_url.return_value = "https://s3.example/k"
        storage._pool.client = client
        storage.upload(b"abc", None)  # type: ignore[arg-type]
        storage.put_object(b"abcd")
        bodies = [call.kwargs["Body"] for call in client.put_object.call_args_list]
        assert [b.read() for b in bodies] == [b"abc", b"abcd"]

    def test_gcs_timeouts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """GCS: downloads get (30, stall); an upload's read timeout covers one 8 MiB request at 64 KiB/s."""
        pytest.importorskip("google.cloud.storage")
        from vgi_rpc.gcs import GCSStorage

        monkeypatch.delenv(ENV_STORAGE_STALL_TIMEOUT, raising=False)
        storage = GCSStorage(bucket="b")
        assert storage._read_timeout() == (30.0, 30.0)
        assert storage._upload_timeout(1000) == (30.0, 30.0)
        assert storage._upload_timeout(100 * 1024 * 1024) == (30.0, 128.0)
        slow = GCSStorage(bucket="b", upload_floor_bytes_per_second=16 * 1024, stall_timeout_seconds=10.0)
        assert slow._read_timeout() == (30.0, 10.0)
        assert slow._upload_timeout(1024 * 1024) == (30.0, 64.0)


# ---------------------------------------------------------------------------
# Custom S3 endpoint (MinIO), opt-in
# ---------------------------------------------------------------------------

_MINIO = os.environ.get("VGI_TEST_MINIO_ENDPOINT", "")


@pytest.mark.skipif(not _MINIO, reason="set VGI_TEST_MINIO_ENDPOINT (+ _ACCESS_KEY, _SECRET_KEY) to run")
class TestMinioEndpoint:
    """S3Storage against a real S3-compatible server on a custom (http) endpoint."""

    @pytest.fixture
    def minio(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[object]:
        """S3Storage on MinIO, path-style, with a fresh bucket."""
        boto3 = pytest.importorskip("boto3")
        import uuid

        from vgi_rpc.s3 import S3Storage

        monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.environ["VGI_TEST_MINIO_ACCESS_KEY"])
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.environ["VGI_TEST_MINIO_SECRET_KEY"])
        bucket = f"vgi-rpc-test-{uuid.uuid4().hex[:12]}"
        boto3.client("s3", endpoint_url=_MINIO, region_name="us-east-1").create_bucket(Bucket=bucket)
        yield S3Storage(bucket=bucket, endpoint_url=_MINIO, region_name="us-east-1", addressing_style="path")

    def test_put_get_and_presigned_urls(self, minio: object) -> None:
        """Objects round-trip; upload URLs use the custom host and enforce the signed SHA-256."""
        import hashlib

        from vgi_rpc.s3 import S3Storage

        assert isinstance(minio, S3Storage)
        key = minio.put_object(b"hello minio")
        assert minio.get(key) == b"hello minio"

        body = b"x" * 200_000
        vended = minio.generate_upload_url(len(body), hashlib.sha256(body).hexdigest())
        host = urlsplit(_MINIO).netloc
        assert urlsplit(vended.upload_url).netloc == host
        assert urlsplit(vended.download_url).netloc == host
        assert urlsplit(vended.upload_url).path.startswith(f"/{minio.bucket}/")
        # Same length, wrong bytes: the signed x-amz-checksum-sha256 rejects it.
        bad = storage_put(vended.upload_url, b"y" * len(body), headers=dict(vended.headers))
        assert bad.status_code == 400
        good = storage_put(vended.upload_url, body, headers=dict(vended.headers))
        assert good.status_code == 200
        assert httpx2.get(vended.download_url).content == body
