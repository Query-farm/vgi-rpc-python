# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for vgi_rpc.vendor against the in-process fake vendor."""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterator
from datetime import UTC, datetime

import httpx2
import pyarrow as pa
import pytest

from vgi_rpc.conformance.fake_vendor import FakeVendorServer, serve_fake_vendor
from vgi_rpc.external import Compression, ExternalStorage, UploadUrlProvider
from vgi_rpc.external_fetch import FetchConfig, fetch_url
from vgi_rpc.vendor import (
    BlobRef,
    BlobScope,
    VendorBlobClient,
    VendorError,
    VendorStorage,
    client_external_config_from_env,
    external_config_from_env,
    max_upload_bytes_from_env,
    pinned_hosts_validator,
    vendor_url_validator,
)

_TOKEN = "test-token-s3cr3t"
_SCHEMA = pa.schema([pa.field("x", pa.int64())])


@pytest.fixture
def fake() -> Iterator[FakeVendorServer]:
    """Run a fake vendor with two tenants."""
    with serve_fake_vendor({_TOKEN: "tenant-a", "other-token": "tenant-b"}) as server:
        yield server


@pytest.fixture
def storage(fake: FakeVendorServer) -> Iterator[VendorStorage]:
    """VendorStorage bound to the fake vendor, no backoff."""
    with VendorStorage(fake.url, _TOKEN, backoff_seconds=0.0) as s:
        yield s


@pytest.fixture
def blobs(fake: FakeVendorServer) -> Iterator[VendorBlobClient]:
    """VendorBlobClient bound to the fake vendor, no backoff."""
    with VendorBlobClient(fake.url, _TOKEN, backoff_seconds=0.0) as client:
        yield client


def _fetch(url: str, validator: object) -> bytes:
    with FetchConfig() as cfg:
        return fetch_url(url, cfg, url_validator=validator)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# VendorStorage
# ---------------------------------------------------------------------------


class TestVendorStorage:
    """ExternalStorage / UploadUrlProvider behaviour."""

    def test_implements_both_protocols(self, storage: VendorStorage) -> None:
        """VendorStorage satisfies both runtime-checkable protocols."""
        assert isinstance(storage, ExternalStorage)
        assert isinstance(storage, UploadUrlProvider)

    def test_upload_round_trip(self, fake: FakeVendorServer, storage: VendorStorage) -> None:
        """upload() returns a download URL that serves the exact bytes."""
        data = b"arrow-ipc-bytes" * 1000
        url = storage.upload(data, _SCHEMA)
        assert _fetch(url, storage.url_validator) == data
        assert fake.vendor.stats()["/v1/blobs/put"] == 1

    def test_upload_sends_exactly_required_headers(self, fake: FakeVendorServer, storage: VendorStorage) -> None:
        """The signed PUT carries the checksum header and no Authorization or Content-Type."""
        storage.upload(b"abc", _SCHEMA)
        puts = [h for m, p, h in fake.vendor.requests if m == "PUT"]
        assert len(puts) == 1
        assert "HTTP_X_AMZ_CHECKSUM_SHA256" in puts[0]
        assert "HTTP_AUTHORIZATION" not in puts[0]
        api = [h for m, p, h in fake.vendor.requests if p.startswith("/v1/")]
        assert all("HTTP_AUTHORIZATION" in h for h in api)

    def test_signed_put_headers_are_exactly_the_grant(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The PUT to storage carries the grant's headers and no Content-Type or Authorization."""
        seen: dict[str, httpx2.Headers] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen[request.method] = request.headers
            if request.url.path == "/v1/info":
                return httpx2.Response(200, json={"upload_hosts": ["r2.example"], "download_hosts": ["r2.example"]})
            if request.method == "PUT":
                return httpx2.Response(200)
            return httpx2.Response(
                200,
                json={
                    "object_id": "rpc/t/1/x",
                    "upload_url": "https://r2.example/k?X-Amz-Signature=s",
                    "required_headers": {"content-length": "2", "x-amz-checksum-sha256": "c3VtCg=="},
                    "download_url": "https://r2.example/k?X-Amz-Signature=g",
                    "expires_at": "2099-01-01T00:00:00Z",
                },
            )

        transport = httpx2.MockTransport(handler)
        monkeypatch.setattr("vgi_rpc.vendor._new_storage_client", lambda timeout: httpx2.Client(transport=transport))
        with httpx2.Client(transport=transport) as session:
            VendorStorage("https://vendor.example", _TOKEN, session=session).upload(
                b"hi", _SCHEMA, content_encoding="zstd"
            )
        put = seen["PUT"]
        assert put["content-length"] == "2"
        assert put["x-amz-checksum-sha256"] == "c3VtCg=="
        assert put["content-encoding"] == "zstd"
        assert "content-type" not in put
        assert "authorization" not in put
        assert seen["POST"]["authorization"] == f"Bearer {_TOKEN}"

    def test_grant_size_disagreeing_with_data_is_refused(self) -> None:
        """A grant whose content-length differs from the data is refused before any PUT."""

        def handler(request: httpx2.Request) -> httpx2.Response:
            assert request.method != "PUT"
            return httpx2.Response(
                200,
                json={
                    "object_id": "o",
                    "upload_url": "https://r2.example/k",
                    "required_headers": {"content-length": "999"},
                    "expires_at": 1_900_000_000,
                },
            )

        with httpx2.Client(transport=httpx2.MockTransport(handler)) as session:
            s = VendorStorage("https://vendor.example", _TOKEN, session=session, pin_hosts=False)
            with pytest.raises(VendorError, match="Content-Length 999"):
                s.upload(b"hi", _SCHEMA)

    def test_upload_with_content_encoding(self, fake: FakeVendorServer, storage: VendorStorage) -> None:
        """A zstd-encoded upload is stored with its encoding and decoded on fetch."""
        import zstandard

        raw = b"z" * 50_000
        url = storage.upload(zstandard.compress(raw), _SCHEMA, content_encoding="zstd")
        assert _fetch(url, storage.url_validator) == raw

    def test_generate_upload_url(self, storage: VendorStorage) -> None:
        """generate_upload_url() vends an unsized PUT/GET pair that round-trips."""
        pair = storage.generate_upload_url(_SCHEMA)
        assert pair.expires_at > datetime.now(UTC)
        resp = httpx2.put(
            pair.upload_url, content=b"client bytes", headers={"Content-Type": "application/octet-stream"}
        )
        assert resp.status_code == 200
        assert _fetch(pair.download_url, storage.url_validator) == b"client bytes"

    def test_retries_5xx_on_put_and_upload(self, fake: FakeVendorServer, storage: VendorStorage) -> None:
        """A transient 503 from the vendor and from storage is retried."""
        fake.vendor.fail_next("/v1/blobs/put", status=503)
        fake.vendor.fail_next("/r2/put/", status=500)
        url = storage.upload(b"retry me", _SCHEMA)
        assert _fetch(url, storage.url_validator) == b"retry me"
        assert fake.vendor.stats()["r2_put"] == 1  # the faulted attempt never reached the store

    def test_gives_up_after_max_attempts(self, fake: FakeVendorServer, storage: VendorStorage) -> None:
        """Persistent 5xx surfaces as VendorError carrying the status."""
        fake.vendor.fail_next("/v1/blobs/put", status=503, count=10)
        with pytest.raises(VendorError, match="after 3 attempts") as exc_info:
            storage.upload(b"x", _SCHEMA)
        assert exc_info.value.status == 503

    def test_4xx_is_not_retried(self, fake: FakeVendorServer) -> None:
        """A bad token fails once with 401, no retries."""
        with VendorStorage(fake.url, "wrong", backoff_seconds=0.0) as s, pytest.raises(VendorError) as exc_info:
            s.upload(b"x", _SCHEMA)
        assert exc_info.value.status == 401
        assert sum(1 for _, p, _ in fake.vendor.requests if p == "/v1/blobs/put") == 1

    def test_retries_transport_errors(self) -> None:
        """A connection error is retried; the next attempt succeeds."""
        calls: list[str] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls.append(str(request.url))
            if len(calls) == 1:
                raise httpx2.ConnectError("boom", request=request)
            return httpx2.Response(
                200,
                json={
                    "urls": [{"upload_url": "https://h/u", "download_url": "https://h/d", "expires_at": 1_900_000_000}]
                },
            )

        with httpx2.Client(transport=httpx2.MockTransport(handler)) as session:
            s = VendorStorage("https://vendor.example", _TOKEN, session=session, backoff_seconds=0.0, pin_hosts=False)
            pair = s.generate_upload_url(_SCHEMA)
        assert pair.upload_url == "https://h/u"
        assert len(calls) == 2

    def test_rejects_vended_url_off_pinned_hosts(self) -> None:
        """A vendor answer pointing outside /v1/info's hosts is refused before use."""

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path == "/v1/info":
                return httpx2.Response(200, json={"region": "eu", "upload_hosts": ["r2.example"], "download_hosts": []})
            return httpx2.Response(
                200,
                json={
                    "urls": [
                        {
                            "upload_url": "https://evil.example/u",
                            "download_url": "https://r2.example/d",
                            "expires_at": "2099-01-01T00:00:00Z",
                        }
                    ]
                },
            )

        with httpx2.Client(transport=httpx2.MockTransport(handler)) as session:
            s = VendorStorage("https://vendor.example", _TOKEN, session=session)
            with pytest.raises(VendorError, match="pinned"):
                s.generate_upload_url(_SCHEMA)

    def test_token_and_signatures_never_logged(
        self, fake: FakeVendorServer, storage: VendorStorage, caplog: pytest.LogCaptureFixture
    ) -> None:
        """No log record (vgi_rpc's or httpx2's) and no error carries the token or a signed query."""
        caplog.set_level(logging.DEBUG)
        fake.vendor.fail_next("/r2/put/", status=503, count=3)
        with pytest.raises(VendorError) as exc_info:
            storage.upload(b"secret payload", _SCHEMA)
        fake.vendor.fail_next("/v1/blobs/put", status=503)
        storage.upload(b"ok", _SCHEMA)
        messages = [r.getMessage() for r in caplog.records]
        assert any(r.name == "httpx2" for r in caplog.records)  # httpx2 did log the requests...
        text = "\n".join(messages) + str(exc_info.value)  # ...with their URLs redacted
        assert _TOKEN not in text
        assert "X-Amz-Signature" not in text
        assert "/r2/put/" in text  # redacted URL path is still there for diagnosis

    def test_requires_https_for_remote_vendor(self) -> None:
        """A plain-http vendor URL is accepted only for loopback."""
        with pytest.raises(ValueError, match="https"):
            VendorStorage("http://vendor.example", _TOKEN)
        VendorStorage("http://localhost:1", _TOKEN).close()

    def test_empty_token_rejected(self) -> None:
        """An empty token is a configuration error."""
        with pytest.raises(ValueError, match="token"):
            VendorStorage("https://vendor.example", "")


# ---------------------------------------------------------------------------
# VendorBlobClient
# ---------------------------------------------------------------------------


class TestVendorBlobClient:
    """Raw put/get/delete."""

    def test_round_trip(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """put_bytes/get_bytes round-trip; the ref names size and hex sha256."""
        data = bytes(range(256)) * 400
        ref = blobs.put_bytes(data)
        assert ref.size == len(data)
        assert ref.sha256 == hashlib.sha256(data).hexdigest()
        assert re.fullmatch(r"[0-9a-f]{32}", ref.object_id)
        assert blobs.get_bytes(ref) == data

    def test_empty_blob(self, blobs: VendorBlobClient) -> None:
        """A zero-byte blob round-trips."""
        ref = blobs.put_bytes(b"")
        assert blobs.get_bytes(ref) == b""

    def test_checksum_mismatch_rejected_on_fetch(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """Bytes that changed after upload fail the sha256 check."""
        ref = blobs.put_bytes(b"original-bytes")
        fake.vendor.corrupt(ref.object_id, b"tampered-bytes")
        with pytest.raises(VendorError, match="SHA-256 mismatch"):
            blobs.get_bytes(ref)

    def test_size_mismatch_rejected_on_fetch(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """A blob that grew after upload fails the size check."""
        ref = blobs.put_bytes(b"short")
        fake.vendor.corrupt(ref.object_id, b"short-and-longer")
        with pytest.raises(VendorError, match="size mismatch"):
            blobs.get_bytes(ref)

    def test_forged_ref_rejected(self, blobs: VendorBlobClient) -> None:
        """A ref whose sha256 does not match the stored bytes is refused."""
        ref = blobs.put_bytes(b"abc")
        forged = BlobRef(ref.object_id, ref.size, hashlib.sha256(b"xyz").hexdigest())
        with pytest.raises(VendorError, match="SHA-256 mismatch"):
            blobs.get_bytes(forged)

    def test_size_limit_enforced_before_fetch(self, fake: FakeVendorServer) -> None:
        """A ref larger than max_fetch_bytes is refused without asking the vendor."""
        with (
            FetchConfig(max_fetch_bytes=1024) as cfg,
            VendorBlobClient(fake.url, _TOKEN, fetch_config=cfg) as client,
        ):
            ref = client.put_bytes(b"x" * 2048)
            before = fake.vendor.stats().get("/v1/blobs/get", 0)
            with pytest.raises(VendorError, match="max_fetch_bytes"):
                client.get_bytes(ref)
            assert fake.vendor.stats().get("/v1/blobs/get", 0) == before

    def test_size_limit_enforced_during_fetch(self, fake: FakeVendorServer) -> None:
        """Storage serving more than max_fetch_bytes is cut off even when the ref claims less."""
        with (
            FetchConfig(max_fetch_bytes=1024) as cfg,
            VendorBlobClient(fake.url, _TOKEN, fetch_config=cfg) as client,
        ):
            ref = client.put_bytes(b"x" * 100)
            fake.vendor.corrupt(ref.object_id, b"x" * 4096)
            with pytest.raises(VendorError, match="download failed"):
                client.get_bytes(ref)

    def test_storage_enforces_declared_size_and_checksum(self, fake: FakeVendorServer) -> None:
        """The fake store, like R2, rejects a PUT whose body does not match the grant."""
        resp = httpx2.post(
            f"{fake.url}/v1/blobs/put",
            json={"size": 3, "sha256_b64": "A" * 43 + "=", "purpose": "fs"},
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        grant = resp.json()
        bad = httpx2.put(grant["upload_url"], content=b"abc", headers=grant["required_headers"])
        assert bad.status_code == 400  # BadDigest: body does not hash to the signed checksum
        wrong_len = httpx2.put(grant["upload_url"], content=b"abcd", headers={"x-amz-checksum-sha256": "A" * 43 + "="})
        assert wrong_len.status_code == 403

    def test_missing_object_is_clean_error(self, blobs: VendorBlobClient) -> None:
        """An unknown object id surfaces as a 404 VendorError."""
        with pytest.raises(VendorError) as exc_info:
            blobs.get_bytes(BlobRef("0" * 32, 1, "0" * 64))
        assert exc_info.value.status == 404
        assert exc_info.value.code == "not_found"

    def test_never_uploaded_object_is_clean_error(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """A put grant whose upload never happened is a 404 ``not_uploaded``, not garbage."""
        resp = httpx2.post(
            f"{fake.url}/v1/blobs/put",
            json={"size": 3, "sha256_b64": "A" * 43 + "=", "purpose": "fs"},
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        ref = BlobRef(resp.json()["object_id"], 3, "0" * 64)
        with pytest.raises(VendorError) as exc_info:
            blobs.get_bytes(ref)
        assert (exc_info.value.status, exc_info.value.code) == (404, "not_uploaded")

    def test_cross_tenant_get_refused(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """Another tenant cannot get a download URL for this tenant's object."""
        ref = blobs.put_bytes(b"mine")
        with VendorBlobClient(fake.url, "other-token") as other, pytest.raises(VendorError) as exc_info:
            other.get_bytes(ref)
        assert (exc_info.value.status, exc_info.value.code) == (404, "not_found")

    def test_idempotency_key_reuses_object(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """A retried write with the same key gets the same object; different bytes conflict."""
        first = blobs.put_bytes(b"value", idempotency_key="attempt-1:0")
        again = blobs.put_bytes(b"value", idempotency_key="attempt-1:0")
        assert again == first
        assert blobs.get_bytes(again) == b"value"
        with pytest.raises(VendorError) as exc_info:
            blobs.put_bytes(b"other", idempotency_key="attempt-1:0")
        assert (exc_info.value.status, exc_info.value.code) == (409, "idempotency_conflict")
        assert blobs.put_bytes(b"value", idempotency_key="attempt-1:1") != first

    def test_object_too_large_and_quota(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """Vendor 413s surface with their codes and are not retried."""
        fake.vendor.max_object_bytes = 4
        with pytest.raises(VendorError) as exc_info:
            blobs.put_bytes(b"12345")
        assert (exc_info.value.status, exc_info.value.code) == (413, "object_too_large")
        fake.vendor.max_object_bytes = 1 << 30
        fake.vendor.quota_bytes = 6
        blobs.put_bytes(b"1234")
        with pytest.raises(VendorError) as exc_info:
            blobs.put_bytes(b"1234")
        assert exc_info.value.code == "quota_exceeded"
        assert sum(1 for _, p, _ in fake.vendor.requests if p == "/v1/blobs/put") == 3

    def test_bad_digest_code_surfaces(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """A storage BadDigest on the signed PUT is reported with its XML code."""
        from vgi_rpc.vendor import _b64_sha256

        grant = blobs._api.put(size=3, sha256_b64=_b64_sha256(b"abc"), purpose="fs")
        with pytest.raises(VendorError) as exc_info:
            blobs._api.put_object(grant, b"xyz")
        assert (exc_info.value.status, exc_info.value.code) == (400, "BadDigest")

    def test_info_shape(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """/v1/info parses tenant, region, limits and TTLs."""
        info = blobs.info()
        assert info.tenant == "tenant-a"
        assert info.region == "auto-enam"
        assert info.limits["max_delete_ids"] == 1000
        assert info.ttl_seconds["put"] == 300
        assert info.url_prefixes == (f"{fake.url}/r2/",)

    def test_delete_and_delete_scope(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """delete() removes named objects; delete_scope() removes a scope's objects only."""
        scope = BlobScope(shard="s1", execution_id="657865632d31")
        a = blobs.put_bytes(b"a", scope=scope)
        b = blobs.put_bytes(b"b", scope=scope)
        c = blobs.put_bytes(b"c", scope=BlobScope(shard="s1", execution_id="6f74686572"))
        d = blobs.put_bytes(b"d")
        blobs.delete([d, "fs/tenant-a/unknown"])
        assert fake.vendor.object_data(d.object_id) is None
        blobs.delete_scope("s1", "657865632d31")
        assert fake.vendor.object_data(a.object_id) is None
        assert fake.vendor.object_data(b.object_id) is None
        assert fake.vendor.object_data(c.object_id) == b"c"

    def test_scope_charset_enforced(self, blobs: VendorBlobClient) -> None:
        """Scope fields outside [A-Za-z0-9._:-] (e.g. base64 padding) are refused, as on staging."""
        with pytest.raises(VendorError) as exc_info:
            blobs.put_bytes(b"x", scope=BlobScope(shard="s1", execution_id="ZXhlYw=="))
        assert (exc_info.value.status, exc_info.value.code) == (400, "bad_request")

    def test_retries_on_get(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """A transient 502 on get is retried."""
        ref = blobs.put_bytes(b"payload")
        fake.vendor.fail_next("/v1/blobs/get", status=502)
        assert blobs.get_bytes(ref) == b"payload"


# ---------------------------------------------------------------------------
# Pinning
# ---------------------------------------------------------------------------


class TestPinnedHostsValidator:
    """pinned_hosts_validator semantics."""

    def test_exact_host_only(self) -> None:
        """Only the listed host on the default https port passes; subdomains and look-alikes fail."""
        validate = pinned_hosts_validator(["acct.eu.r2.cloudflarestorage.com"])
        validate("https://acct.eu.r2.cloudflarestorage.com/bucket/key?X-Amz-Signature=abc")
        validate("https://ACCT.eu.r2.cloudflarestorage.com:443/k")
        for bad in (
            "https://evil.acct.eu.r2.cloudflarestorage.com/k",
            "https://acct.eu.r2.cloudflarestorage.com.evil.com/k",
            "https://acct.eu.r2.cloudflarestorage.com:8443/k",
            "http://acct.eu.r2.cloudflarestorage.com/k",
            "https://user:pw@acct.eu.r2.cloudflarestorage.com/k",
            "ftp://acct.eu.r2.cloudflarestorage.com/k",
        ):
            with pytest.raises(ValueError):
                validate(bad)

    def test_rejection_message_is_redacted(self) -> None:
        """A rejected URL's signed query does not appear in the error."""
        validate = pinned_hosts_validator(["good.example"])
        with pytest.raises(ValueError) as exc_info:
            validate("https://bad.example/k?X-Amz-Signature=supersecret")
        assert "supersecret" not in str(exc_info.value)

    def test_explicit_port_and_http(self) -> None:
        """host:port entries pin the port; http only with allow_http."""
        validate = pinned_hosts_validator(["127.0.0.1:9000"], allow_http=True)
        validate("http://127.0.0.1:9000/x")
        with pytest.raises(ValueError):
            validate("http://127.0.0.1:9001/x")
        with pytest.raises(ValueError):
            pinned_hosts_validator(["127.0.0.1:9000"])("http://127.0.0.1:9000/x")

    def test_prefixes(self) -> None:
        """With prefixes, only URLs under them pass; dot-segments are refused."""
        validate = pinned_hosts_validator(["r2.example"], prefixes=["https://r2.example/bucket-enam/"])
        validate("https://r2.example/bucket-enam/rpc/t/1/x?X-Amz-Signature=s")
        for bad in (
            "https://r2.example/bucket-weur/rpc/t/1/x",
            "https://r2.example/bucket-enam-evil/x",
            "https://r2.example/bucket-enam/../bucket-weur/x",
        ):
            with pytest.raises(ValueError):
                validate(bad)

    def test_vendor_validator_uses_url_prefixes(self, fake: FakeVendorServer) -> None:
        """The /v1/info validator also enforces url_prefixes."""
        validate = vendor_url_validator(fake.url, _TOKEN)
        validate(f"{fake.url}/r2/get/x")
        with pytest.raises(ValueError, match="prefixes"):
            validate(f"{fake.url}/v1/info")

    def test_empty_or_bad_list(self) -> None:
        """An empty list or a URL-shaped entry is a configuration error."""
        with pytest.raises(ValueError):
            pinned_hosts_validator([])
        with pytest.raises(ValueError):
            pinned_hosts_validator(["host/path"])

    def test_vendor_validator_rejects_foreign_host(self, fake: FakeVendorServer) -> None:
        """The /v1/info-backed validator accepts the vendor's store and rejects anything else."""
        validate = vendor_url_validator(fake.url, _TOKEN)
        validate(f"{fake.url}/r2/get/abc")
        port = fake.url.rsplit(":", 1)[1]
        with pytest.raises(ValueError, match="pinned"):
            validate(f"http://localhost:{port}/r2/get/abc")
        with pytest.raises(ValueError):
            validate("https://example.com/x")

    def test_vendor_validator_caches_info(self, fake: FakeVendorServer) -> None:
        """/v1/info is fetched once within the TTL, and a failed refresh keeps the last list."""
        cached = vendor_url_validator(fake.url, _TOKEN)
        for i in range(3):
            cached(f"{fake.url}/r2/get/{i}")
        assert fake.vendor.stats()["/v1/info"] == 1
        refreshing = vendor_url_validator(fake.url, _TOKEN, ttl_seconds=0.0)
        refreshing(f"{fake.url}/r2/get/a")
        fake.vendor.fail_next("/v1/info", status=503, count=10)
        refreshing(f"{fake.url}/r2/get/b")  # refresh fails, stale list kept

    def test_vendor_validator_fails_closed_without_info(self, fake: FakeVendorServer) -> None:
        """With no host list ever fetched, every URL is rejected."""
        validate = vendor_url_validator(fake.url, "wrong-token")
        with pytest.raises(ValueError, match="Cannot pin"):
            validate(f"{fake.url}/r2/get/a")

    def test_pinned_fetch_rejects_foreign_host(self, fake: FakeVendorServer, blobs: VendorBlobClient) -> None:
        """fetch_url with the vendor validator never contacts a foreign host."""
        port = fake.url.rsplit(":", 1)[1]
        with pytest.raises(ValueError):
            _fetch(f"http://localhost:{port}/r2/get/x", blobs.url_validator)


# ---------------------------------------------------------------------------
# Env configs
# ---------------------------------------------------------------------------


class TestEnvConfig:
    """external_config_from_env and friends."""

    def test_unset_returns_none(self) -> None:
        """No VGI_BLOB_VENDOR_URL, no config."""
        assert external_config_from_env({}) is None
        assert client_external_config_from_env({}) is None
        assert max_upload_bytes_from_env({}) is None

    def test_token_fallback(self, fake: FakeVendorServer) -> None:
        """VGI_CF_DO_TOKEN is used when VGI_BLOB_VENDOR_TOKEN is unset."""
        cfg = external_config_from_env({"VGI_BLOB_VENDOR_URL": fake.url, "VGI_CF_DO_TOKEN": _TOKEN})
        assert cfg is not None
        assert isinstance(cfg.storage, VendorStorage)
        url = cfg.storage.upload(b"hello", _SCHEMA)
        assert cfg.url_validator is not None
        cfg.url_validator(url)

    def test_missing_token_is_error(self) -> None:
        """A URL without any token is a configuration error, not a silent None."""
        with pytest.raises(ValueError, match="VGI_BLOB_VENDOR_TOKEN"):
            external_config_from_env({"VGI_BLOB_VENDOR_URL": "https://v.example"})

    def test_threshold_and_compression(self) -> None:
        """Threshold and compression parse; defaults are 1 MiB and none."""
        base = {"VGI_BLOB_VENDOR_URL": "https://v.example", "VGI_BLOB_VENDOR_TOKEN": "t"}
        cfg = external_config_from_env(base)
        assert cfg is not None
        assert cfg.externalize_threshold_bytes == 1_048_576
        assert cfg.compression is None
        cfg = external_config_from_env(
            {**base, "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES": "2048", "VGI_RPC_EXTERNAL_COMPRESSION": "zstd:5"}
        )
        assert cfg is not None
        assert cfg.externalize_threshold_bytes == 2048
        assert cfg.compression == Compression("zstd", 5)
        cfg = external_config_from_env({**base, "VGI_RPC_EXTERNAL_COMPRESSION": "gzip"})
        assert cfg is not None and cfg.compression == Compression("gzip")
        for bad in ({"VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES": "0"}, {"VGI_RPC_EXTERNAL_COMPRESSION": "lz4"}):
            with pytest.raises(ValueError):
                external_config_from_env({**base, **bad})

    def test_from_env_constructors(self, fake: FakeVendorServer) -> None:
        """from_env builds working clients; the dedicated token wins over the fallback."""
        env = {"VGI_BLOB_VENDOR_URL": fake.url, "VGI_BLOB_VENDOR_TOKEN": _TOKEN, "VGI_CF_DO_TOKEN": "other-token"}
        with VendorBlobClient.from_env(env=env) as client:
            client.put_bytes(b"env")
            assert client.info().tenant == "tenant-a"
        with VendorStorage.from_env(env=env) as s:
            s.upload(b"env", _SCHEMA)
        assert max_upload_bytes_from_env({"VGI_RPC_MAX_UPLOAD_BYTES": "1000"}) == 1000
        client_cfg = client_external_config_from_env(env)
        assert client_cfg is not None and client_cfg.url_validator is not None
        client_cfg.url_validator(f"{fake.url}/r2/get/x")
