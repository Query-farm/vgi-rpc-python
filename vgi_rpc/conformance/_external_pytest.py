# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""External-location pytest conformance cases.

Kept separate from the main suite because these cases need a small raw-HTTP
driver: the ordinary proxy deliberately hides pointer batches, while these
tests must place one on each inbound HTTP route explicitly.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable
from io import BytesIO
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.external import ClientExternalConfig, make_external_location_batch
from vgi_rpc.metadata import CALL_STATE_KEY, STATE_KEY, merge_metadata
from vgi_rpc.rpc import AnnotatedBatch, RpcError, _dispatch_log_or_error, rpc_methods
from vgi_rpc.rpc._wire import _write_request
from vgi_rpc.utils import new_ipc_stream

if TYPE_CHECKING:
    import httpx2

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
_IDENTITY_HEADERS = {
    "Content-Type": _ARROW_CONTENT_TYPE,
    "Accept-Encoding": "identity",
    "X-VGI-Accept-Encoding": "identity",
}


def _request_body(method_name: str, **kwargs: object) -> bytes:
    """Serialize one standard unary/stream-init request."""
    info = rpc_methods(ConformanceService)[method_name]
    version = vars(ConformanceService).get("protocol_version")
    buf = BytesIO()
    _write_request(
        buf,
        method_name,
        info.params_schema,
        kwargs,
        protocol_version=version if isinstance(version, str) else None,
    )
    return buf.getvalue()


def _pointer_body(original_body: bytes, location: str, *, sha256: str | None = None) -> bytes:
    """Replace the request body batch with an external-location pointer."""
    reader = ipc.open_stream(BytesIO(original_body))
    batch, request_metadata = reader.read_next_batch_with_custom_metadata()
    pointer, location_metadata = make_external_location_batch(batch.schema, location, sha256=sha256)
    outer_metadata = merge_metadata(request_metadata, location_metadata)
    out = BytesIO()
    with new_ipc_stream(out, batch.schema) as writer:
        writer.write_batch(pointer, custom_metadata=outer_metadata)
    return out.getvalue()


def _upload_body(
    storage_url: str,
    body: bytes,
    *,
    content_encoding: str | None = None,
) -> tuple[str, str]:
    """Upload an IPC body and return ``(download_url, raw_sha256)``."""
    import httpx2

    allocation_response = httpx2.post(f"{storage_url}/alloc", json={}, timeout=5.0)
    allocation_response.raise_for_status()
    allocation = allocation_response.json()
    legacy_url = str(allocation["object_url"])
    upload_url = str(allocation.get("upload_url", legacy_url))
    download_url = str(allocation.get("download_url", legacy_url))
    headers = {"Content-Type": "application/octet-stream"}
    if content_encoding is not None:
        headers["Content-Encoding"] = content_encoding
    put_response = httpx2.put(
        upload_url,
        content=body,
        headers=headers,
        timeout=5.0,
    )
    put_response.raise_for_status()
    return download_url, hashlib.sha256(body).hexdigest()


def _storage_stats(storage_url: str) -> dict[str, int]:
    """Read fake-storage counters."""
    import httpx2

    response = httpx2.get(f"{storage_url}/_stats", timeout=5.0)
    response.raise_for_status()
    return {str(key): int(value) for key, value in response.json().items()}


def _redirect_url(download_url: str, route: str) -> str:
    """Replace the method-bound download route with a redirect fixture route."""
    marker = "/download/"
    assert marker in download_url
    return download_url.replace(marker, f"/{route}/", 1)


def _external_pointer_body(storage_url: str, original_body: bytes) -> bytes:
    """Upload *original_body* and return its checksummed pointer body."""
    download_url, checksum = _upload_body(storage_url, original_body)
    return _pointer_body(original_body, download_url, sha256=checksum)


def _response_batches(response: httpx2.Response) -> list[tuple[pa.RecordBatch, pa.KeyValueMetadata | None]]:
    """Decode one uncompressed Arrow response stream and raise its RPC error."""
    assert response.status_code == 200, f"{response.status_code}: {response.content[:200]!r}"
    reader = ipc.open_stream(BytesIO(response.content))
    batches: list[tuple[pa.RecordBatch, pa.KeyValueMetadata | None]] = []
    while True:
        try:
            batch, metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            break
        _dispatch_log_or_error(batch, metadata)
        batches.append((batch, metadata))
    return batches


def _assert_rpc_error_response(response: httpx2.Response, *, match: str | None = None) -> None:
    """Require the HTTP error discriminator and a typed Arrow RPC error."""
    assert response.status_code == 200
    assert response.headers.get("X-VGI-RPC-Error") == "true"
    with pytest.raises(RpcError, match=match):
        _response_batches(response)


def _post(client: httpx2.Client, path: str, body: bytes) -> httpx2.Response:
    """POST an uncompressed Arrow request using the supplied reusable client."""
    return client.post(path, content=body, headers=_IDENTITY_HEADERS)


def _result_value(response: httpx2.Response) -> object:
    """Return the first unary ``result`` value in *response*."""
    for batch, _metadata in _response_batches(response):
        if batch.num_rows == 1 and "result" in batch.schema.names:
            return batch.column("result")[0].as_py()
    raise AssertionError("response carried no unary result")


def _state_tokens(response: httpx2.Response) -> tuple[bytes, bytes]:
    """Extract the cursor and call tokens from a stream-init response."""
    for batch, metadata in _response_batches(response):
        if batch.num_rows != 0 or metadata is None:
            continue
        cursor = metadata.get(STATE_KEY)
        call = metadata.get(CALL_STATE_KEY)
        if cursor is not None and call is not None:
            return cursor, call
    raise AssertionError("stream init response carried no cursor/call token pair")


def _exchange_body(batch: pa.RecordBatch, cursor: bytes, call: bytes) -> bytes:
    """Serialize an inline exchange turn carrying both state tokens."""
    out = BytesIO()
    metadata = pa.KeyValueMetadata({STATE_KEY: cursor, CALL_STATE_KEY: call})
    with new_ipc_stream(out, batch.schema) as writer:
        writer.write_batch(batch, custom_metadata=metadata)
    return out.getvalue()


class TestExternalInputRoutes:
    """Every inbound HTTP data route must resolve external pointer batches."""

    def test_unary_resolves_external_input(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """Unary parameters may arrive through an external pointer."""
        import httpx2

        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        original = _request_body("echo_string", value="external unary input")
        pointer = _external_pointer_body(conformance_fake_storage, original)
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            assert _result_value(_post(client, "/echo_string", pointer)) == "external unary input"

    def test_stream_init_resolves_external_input(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """Stream initialization parameters may arrive through a pointer."""
        import httpx2

        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        original = _request_body("produce_n", count=2)
        pointer = _external_pointer_body(conformance_fake_storage, original)
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            batches = _response_batches(_post(client, "/produce_n/init", pointer))
        assert any(batch.num_rows == 1 and batch.column("value")[0].as_py() == 0 for batch, _ in batches)
        assert any(metadata is not None and metadata.get(STATE_KEY) is not None for _, metadata in batches)

    def test_exchange_resolves_external_input(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """Exchange data batches may arrive through an external pointer."""
        import httpx2

        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            cursor, call = _state_tokens(
                _post(client, "/exchange_scale/init", _request_body("exchange_scale", factor=3.0))
            )
            input_batch = pa.RecordBatch.from_pydict(
                {"value": [1.5, 2.0]},
                schema=pa.schema([pa.field("value", pa.float64())]),
            )
            inline_exchange = _exchange_body(input_batch, cursor, call)
            pointer_exchange = _external_pointer_body(conformance_fake_storage, inline_exchange)
            batches = _response_batches(_post(client, "/exchange_scale/exchange", pointer_exchange))
        data = [batch for batch, _ in batches if batch.num_rows > 0]
        assert len(data) == 1
        assert data[0].column("value").to_pylist() == pytest.approx([4.5, 6.0])

    def test_exchange_client_auto_externalizes_large_input(
        self,
        conformance_http_with_storage_port: int,
    ) -> None:
        """A 413 exchange retry uses the server-vended upload/download pair."""
        from vgi_rpc.http import http_connect

        values = [float(i) for i in range(2_000)]
        config = ClientExternalConfig(url_validator=None)
        with (
            http_connect(
                ConformanceService,  # type: ignore[type-abstract]
                f"http://127.0.0.1:{conformance_http_with_storage_port}",
                external_location=config,  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]
                compression_level=None,
            ) as proxy,
            proxy.exchange_scale(factor=2.0) as session,
        ):
            result = session.exchange(AnnotatedBatch.from_pydict({"value": values}))
        assert result.batch.column("value").to_pylist() == pytest.approx([value * 2.0 for value in values])


class TestExternalFetchFailures:
    """Fetch failures are RPC errors and never fabricated empty input."""

    def test_404_is_rpc_error_and_server_remains_reusable(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """A missing object fails the call; a later call still succeeds."""
        import httpx2

        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        original = _request_body("echo_int", value=7)
        pointer = _pointer_body(original, f"{conformance_fake_storage}/download/conformance-missing")
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            _assert_rpc_error_response(_post(client, "/echo_int", pointer))
            assert _result_value(_post(client, "/echo_int", original)) == 7

    def test_exchange_404_is_error_not_empty_dispatch(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """A failed exchange fetch must not dispatch a fabricated empty batch."""
        import httpx2

        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            cursor, call = _state_tokens(
                _post(client, "/exchange_scale/init", _request_body("exchange_scale", factor=2.0))
            )
            input_batch = pa.RecordBatch.from_pydict(
                {"value": [3.0]},
                schema=pa.schema([pa.field("value", pa.float64())]),
            )
            inline_exchange = _exchange_body(input_batch, cursor, call)
            pointer = _pointer_body(
                inline_exchange,
                f"{conformance_fake_storage}/download/conformance-missing-exchange",
            )
            _assert_rpc_error_response(_post(client, "/exchange_scale/exchange", pointer))
            assert _result_value(_post(client, "/echo_int", _request_body("echo_int", value=13))) == 13

    def test_checksum_mismatch_is_rpc_error_and_server_remains_reusable(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """A wrong advertised digest is rejected before method dispatch."""
        import httpx2

        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        original = _request_body("echo_int", value=11)
        download_url, _checksum = _upload_body(conformance_fake_storage, original)
        pointer = _pointer_body(original, download_url, sha256="0" * 64)
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            _assert_rpc_error_response(_post(client, "/echo_int", pointer), match="checksum")
            assert _result_value(_post(client, "/echo_int", original)) == 11


class TestExternalFetchSecurity:
    """External fetches validate redirects, enforce both caps, and redact credentials."""

    def test_same_host_redirect_succeeds(
        self,
        conformance_http_external_security_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """An allowed redirect remains usable after per-hop validation."""
        import httpx2

        original = _request_body("echo_int", value=23)
        download_url, checksum = _upload_body(conformance_fake_storage, original)
        pointer = _pointer_body(original, _redirect_url(download_url, "redirect"), sha256=checksum)
        base_url = f"http://127.0.0.1:{conformance_http_external_security_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            assert _result_value(_post(client, "/echo_int", pointer)) == 23

    def test_disallowed_redirect_hop_is_not_fetched(
        self,
        conformance_http_external_security_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """A redirect target is validated before the target receives HEAD or GET."""
        import httpx2

        original = _request_body("echo_int", value=29)
        download_url, checksum = _upload_body(conformance_fake_storage, original)
        pointer = _pointer_body(original, _redirect_url(download_url, "redirect-localhost"), sha256=checksum)
        before = _storage_stats(conformance_fake_storage).get("download_requests", 0)
        base_url = f"http://127.0.0.1:{conformance_http_external_security_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            _assert_rpc_error_response(_post(client, "/echo_int", pointer), match="URL rejected")
            assert _result_value(_post(client, "/echo_int", original)) == 29
        after = _storage_stats(conformance_fake_storage).get("download_requests", 0)
        assert after == before, "the rejected localhost redirect target was fetched"

    def test_redirect_loop_is_bounded(
        self,
        conformance_http_external_security_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """A redirect cycle fails instead of consuming the call deadline."""
        import httpx2

        original = _request_body("echo_int", value=31)
        download_url, checksum = _upload_body(conformance_fake_storage, original)
        pointer = _pointer_body(original, _redirect_url(download_url, "redirect-loop"), sha256=checksum)
        base_url = f"http://127.0.0.1:{conformance_http_external_security_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            _assert_rpc_error_response(_post(client, "/echo_int", pointer), match="redirect limit")

    def test_encoded_body_cap_is_independent(
        self,
        conformance_http_external_security_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """More than 4 KiB on the storage wire is rejected before dispatch."""
        import httpx2

        value = os.urandom(5_000).hex()
        original = _request_body("echo_string", value=value)
        assert len(original) > 4096
        download_url, checksum = _upload_body(conformance_fake_storage, original)
        pointer = _pointer_body(original, download_url, sha256=checksum)
        base_url = f"http://127.0.0.1:{conformance_http_external_security_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            _assert_rpc_error_response(_post(client, "/echo_string", pointer), match="max_fetch_bytes")

    def test_decoded_zstd_cap_is_independent(
        self,
        conformance_http_external_security_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """A small encoded body cannot inflate beyond the 8 KiB decoded cap."""
        import httpx2
        import zstandard

        original = _request_body("echo_string", value="x" * 20_000)
        encoded = zstandard.ZstdCompressor().compress(original)
        assert len(encoded) < 4096
        assert len(original) > 8192
        download_url, _encoded_checksum = _upload_body(
            conformance_fake_storage,
            encoded,
            content_encoding="zstd",
        )
        pointer = _pointer_body(original, download_url, sha256=hashlib.sha256(original).hexdigest())
        base_url = f"http://127.0.0.1:{conformance_http_external_security_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            _assert_rpc_error_response(_post(client, "/echo_string", pointer), match="max_decompressed_bytes")

    def test_signed_query_is_redacted_from_rpc_error(
        self,
        conformance_http_external_security_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """Neither the error message nor remote traceback echoes signed credentials."""
        import httpx2

        secret = "conformance-secret-signature"
        original = _request_body("echo_int", value=37)
        location = (
            f"{conformance_fake_storage}/download/conformance-missing-signed"
            f"?X-Amz-Credential=credential&X-Amz-Signature={secret}"
        )
        pointer = _pointer_body(original, location)
        base_url = f"http://127.0.0.1:{conformance_http_external_security_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            response = _post(client, "/echo_int", pointer)
        assert secret.encode() not in response.content
        assert b"X-Amz-Credential" not in response.content
        _assert_rpc_error_response(response)


class TestExternalStorageUrlPair:
    """Upload URL providers must vend method-correct URL pairs."""

    def test_upload_url_control_route_honors_request_cap(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """An oversized control request is rejected before storage allocation."""
        import httpx2

        from vgi_rpc.http import http_capabilities, request_upload_urls

        rpc_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        capabilities = http_capabilities(rpc_url)
        assert capabilities.max_request_bytes is not None
        before = httpx2.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
        with httpx2.Client(base_url=rpc_url, timeout=5.0) as client:
            response = client.post(
                "/__upload_url__/init",
                content=b"x" * (capabilities.max_request_bytes + 1),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )
            assert response.status_code == 413
            after = httpx2.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
            assert after == before, "an oversized control request must not allocate storage"
            # The rejected body must be framed cleanly enough to reuse the
            # same persistent HTTP connection for the next request.
            assert len(request_upload_urls(count=1, client=client)) == 1

    def test_chunked_upload_url_control_route_honors_request_cap(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """A chunked body cannot bypass the upload-control allocation guard."""
        import httpx2

        from vgi_rpc.http import http_capabilities, request_upload_urls

        rpc_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        capabilities = http_capabilities(rpc_url)
        max_request_bytes = capabilities.max_request_bytes
        assert max_request_bytes is not None
        before = httpx2.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]

        def chunks() -> Iterable[bytes]:
            remaining = max_request_bytes + 1
            while remaining:
                chunk = b"x" * min(1024, remaining)
                remaining -= len(chunk)
                yield chunk

        with httpx2.Client(base_url=rpc_url, timeout=5.0) as client:
            response = client.post(
                "/__upload_url__/init",
                content=chunks(),
                headers={"Content-Type": _ARROW_CONTENT_TYPE},
            )
            assert response.status_code == 413
            after = httpx2.get(f"{conformance_fake_storage}/_stats", timeout=5.0).json()["object_count"]
            assert after == before, "an oversized chunked request must not allocate storage"
            assert len(request_upload_urls(count=1, client=client)) == 1

    def test_upload_and_download_urls_are_method_bound(
        self,
        conformance_http_with_storage_port: int,
    ) -> None:
        """Wrong-method use fails while PUT followed by GET round-trips."""
        import httpx2

        from vgi_rpc.http import request_upload_urls

        urls = request_upload_urls(f"http://127.0.0.1:{conformance_http_with_storage_port}", count=1)
        assert len(urls) == 1
        pair = urls[0]
        assert httpx2.get(pair.upload_url, timeout=5.0).status_code == 403
        assert httpx2.put(pair.download_url, content=b"wrong method", timeout=5.0).status_code == 403
        payload = b"method-bound external storage conformance"
        assert httpx2.put(pair.upload_url, content=payload, timeout=5.0).status_code == 204
        response = httpx2.get(pair.download_url, timeout=5.0)
        assert response.status_code == 200
        assert response.content == payload


__all__ = [
    "TestExternalFetchFailures",
    "TestExternalFetchSecurity",
    "TestExternalInputRoutes",
    "TestExternalStorageUrlPair",
]
