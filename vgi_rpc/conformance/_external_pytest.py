# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""External-location pytest conformance cases.

Kept separate from the main suite because these cases need a small raw-HTTP
driver: the ordinary proxy deliberately hides pointer batches, while these
tests must place one on each inbound HTTP route explicitly.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
from collections.abc import Iterable, Iterator
from io import BytesIO
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.conformance._types import INPUT_METADATA_KEY
from vgi_rpc.external import ClientExternalConfig, make_external_location_batch
from vgi_rpc.metadata import (
    CALL_STATE_KEY,
    LOCATION_FETCH_MS_KEY,
    LOCATION_KEY,
    LOCATION_SHA256_KEY,
    LOCATION_SOURCE_KEY,
    STATE_KEY,
    merge_metadata,
)
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
        protocol=vars(ConformanceService).get("protocol_name") or ConformanceService.__name__,
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
            assert _result_value(_post(client, "/ConformanceService/echo_string", pointer)) == "external unary input"

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
            batches = _response_batches(_post(client, "/ConformanceService/produce_n/init", pointer))
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
                _post(client, "/ConformanceService/exchange_scale/init", _request_body("exchange_scale", factor=3.0))
            )
            input_batch = pa.RecordBatch.from_pydict(
                {"value": [1.5, 2.0]},
                schema=pa.schema([pa.field("value", pa.float64())]),
            )
            inline_exchange = _exchange_body(input_batch, cursor, call)
            pointer_exchange = _external_pointer_body(conformance_fake_storage, inline_exchange)
            batches = _response_batches(_post(client, "/ConformanceService/exchange_scale/exchange", pointer_exchange))
        data = [batch for batch, _ in batches if batch.num_rows > 0]
        assert len(data) == 1
        assert data[0].column("value").to_pylist() == pytest.approx([4.5, 6.0])

    def test_exchange_external_input_carries_the_payloads_metadata(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """An externalized exchange input reaches ``exchange`` with the payload's metadata.

        Resolved metadata is the fetched batch's, never the pointer's, plus the
        reader's provenance stamp (WIRE_PROTOCOL.md §12) -- and on an exchange
        it is application data the method reads. The pointer and the payload
        disagree on the application key here, so a server that hands over the
        pointer's metadata, or none, is told which.
        """
        import httpx2

        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            cursor, call = _state_tokens(
                _post(
                    client,
                    "/ConformanceService/exchange_input_metadata/init",
                    _request_body("exchange_input_metadata"),
                )
            )
            input_batch = pa.RecordBatch.from_pydict(
                {"value": [1.0]},
                schema=pa.schema([pa.field("value", pa.float64())]),
            )
            payload = BytesIO()
            with new_ipc_stream(payload, input_batch.schema) as writer:
                writer.write_batch(
                    input_batch,
                    custom_metadata=pa.KeyValueMetadata(
                        {STATE_KEY: cursor, CALL_STATE_KEY: call, INPUT_METADATA_KEY: b"from-payload"}
                    ),
                )
            download_url, checksum = _upload_body(conformance_fake_storage, payload.getvalue())
            pointer, location_metadata = make_external_location_batch(input_batch.schema, download_url, sha256=checksum)
            pointer_body = BytesIO()
            with new_ipc_stream(pointer_body, input_batch.schema) as writer:
                writer.write_batch(
                    pointer,
                    custom_metadata=merge_metadata(
                        pa.KeyValueMetadata(
                            {STATE_KEY: cursor, CALL_STATE_KEY: call, INPUT_METADATA_KEY: b"from-pointer"}
                        ),
                        location_metadata,
                    ),
                )
            batches = _response_batches(
                _post(client, "/ConformanceService/exchange_input_metadata/exchange", pointer_body.getvalue())
            )
        data = [batch for batch, _ in batches if batch.num_rows > 0]
        assert len(data) == 1
        seen = data[0].column("seen")[0].as_py()
        keys = data[0].column("keys")[0].as_py().split(",")
        assert seen == "from-payload", (
            f"resolved metadata must be the fetched payload's, not the pointer's; got {seen!r}"
        )
        for stamped in (LOCATION_SOURCE_KEY, LOCATION_FETCH_MS_KEY):
            assert stamped.decode() in keys, (
                f"the reader must stamp {stamped.decode()} on a resolved input; got {keys!r}"
            )
        for absent in (LOCATION_KEY, LOCATION_SHA256_KEY, STATE_KEY, CALL_STATE_KEY):
            assert absent.decode() not in keys, f"{absent.decode()} must not reach application code; got {keys!r}"

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
            _assert_rpc_error_response(_post(client, "/ConformanceService/echo_int", pointer))
            assert _result_value(_post(client, "/ConformanceService/echo_int", original)) == 7

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
                _post(client, "/ConformanceService/exchange_scale/init", _request_body("exchange_scale", factor=2.0))
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
            _assert_rpc_error_response(_post(client, "/ConformanceService/exchange_scale/exchange", pointer))
            assert (
                _result_value(_post(client, "/ConformanceService/echo_int", _request_body("echo_int", value=13))) == 13
            )

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
            _assert_rpc_error_response(_post(client, "/ConformanceService/echo_int", pointer), match="checksum")
            assert _result_value(_post(client, "/ConformanceService/echo_int", original)) == 11


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
            assert _result_value(_post(client, "/ConformanceService/echo_int", pointer)) == 23

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
            _assert_rpc_error_response(_post(client, "/ConformanceService/echo_int", pointer), match="URL rejected")
            assert _result_value(_post(client, "/ConformanceService/echo_int", original)) == 29
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
            _assert_rpc_error_response(_post(client, "/ConformanceService/echo_int", pointer), match="redirect limit")

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
            _assert_rpc_error_response(
                _post(client, "/ConformanceService/echo_string", pointer), match="max_fetch_bytes"
            )

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
            _assert_rpc_error_response(
                _post(client, "/ConformanceService/echo_string", pointer), match="max_decompressed_bytes"
            )

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
            response = _post(client, "/ConformanceService/echo_int", pointer)
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


@contextlib.contextmanager
def _external_proxy(port: int) -> Iterator[ConformanceService]:
    """Connect an ordinary external-config client with an owned fetch pool."""
    from vgi_rpc.external import ExternalLocationConfig
    from vgi_rpc.http import http_connect

    config = ExternalLocationConfig(url_validator=None)
    try:
        with http_connect(
            ConformanceService,  # type: ignore[type-abstract]
            f"http://127.0.0.1:{port}",
            external_location=config,
        ) as proxy:
            yield proxy
    finally:
        config.fetch_config.close()


def _published_string_pointer(client: httpx2.Client, value: str, *, include_sha256: bool) -> tuple[str, str | None]:
    """Call ``published_string`` raw; return its pointer's ``(location, sha256)``."""
    response = _post(
        client,
        "/ConformanceService/published_string",
        _request_body("published_string", value=value, include_sha256=include_sha256),
    )
    data = [
        (batch, metadata)
        for batch, metadata in _response_batches(response)
        if metadata is None or metadata.get(b"vgi_rpc.log_level") is None
    ]
    assert len(data) == 1, f"expected exactly one data batch, got {len(data)}"
    batch, metadata = data[0]
    assert batch.num_rows == 0, f"a published ref must answer with a zero-row pointer, got {batch.num_rows} rows"
    assert metadata is not None and metadata.get(LOCATION_KEY) is not None, (
        f"published_string response carried no {LOCATION_KEY.decode()}"
    )
    expected_schema = rpc_methods(ConformanceService)["published_string"].result_schema
    assert batch.schema.equals(expected_schema), f"pointer schema {batch.schema} != result schema {expected_schema}"
    location = metadata.get(LOCATION_KEY)
    assert location is not None
    digest = metadata.get(LOCATION_SHA256_KEY)
    return location.decode(), None if digest is None else digest.decode()


def _fetch_published(url: str) -> bytes:
    """Fetch (and decode) a published object the way a client resolver does."""
    from vgi_rpc.external_fetch import FetchConfig, fetch_url

    config = FetchConfig()
    try:
        return fetch_url(url, config)
    finally:
        config.close()


def _published_value() -> str:
    """Return a value no earlier call has published, so the cache starts cold."""
    return f"published-{os.urandom(8).hex()}"


class TestExternalRef:
    """Pre-published ``ExternalRef`` results: publish once, answer with the same pointer.

    ``published_string(value, include_sha256)`` publishes ``{result: [value]}``
    through the worker's storage on its first call and returns the cached ref
    thereafter.  The answer is always a pointer batch -- even for a two-byte
    value, because a ref is never inlined -- and the client resolves it like
    any other.  Uses the same fixtures as :class:`TestExternalInputRoutes`.
    """

    def test_tiny_value_round_trips_through_pointer(self, conformance_http_with_storage_port: int) -> None:
        """A tiny value still arrives through the pointer: the threshold does not apply."""
        with _external_proxy(conformance_http_with_storage_port) as proxy:
            assert proxy.published_string(value="hi", include_sha256=True) == "hi"

    @pytest.mark.parametrize("include_sha256", [True, False])
    def test_pointer_shape_and_digest(
        self,
        conformance_http_with_storage_port: int,
        include_sha256: bool,
    ) -> None:
        """The response is a zero-row pointer; the digest is present iff requested and correct."""
        import httpx2

        value = _published_value()
        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            url, digest = _published_string_pointer(client, value, include_sha256=include_sha256)
        raw = _fetch_published(url)
        if include_sha256:
            assert digest is not None, f"include_sha256=True but no {LOCATION_SHA256_KEY.decode()} on the pointer"
            assert digest == hashlib.sha256(raw).hexdigest(), "pointer digest does not match the object"
        else:
            assert digest is None, f"include_sha256=False but the pointer carries {LOCATION_SHA256_KEY.decode()}"
        # The object is the result stream: result schema + exactly one 1-row batch.
        reader = ipc.open_stream(BytesIO(raw))
        batches = list(reader)
        assert len(batches) == 1 and batches[0].num_rows == 1
        assert batches[0].column("result")[0].as_py() == value

    def test_published_once(
        self,
        conformance_http_with_storage_port: int,
        conformance_fake_storage: str,
    ) -> None:
        """Two calls name the same location and upload at most one object between them."""
        import httpx2

        value = _published_value()
        base_url = f"http://127.0.0.1:{conformance_http_with_storage_port}"
        before = _storage_stats(conformance_fake_storage)["object_count"]
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            first, _ = _published_string_pointer(client, value, include_sha256=True)
            middle = _storage_stats(conformance_fake_storage)["object_count"]
            second, _ = _published_string_pointer(client, value, include_sha256=True)
        after = _storage_stats(conformance_fake_storage)["object_count"]
        assert first == second, "a published ref must be reused, not re-uploaded"
        assert middle - before <= 1, f"first call uploaded {middle - before} objects (expected at most 1)"
        assert after == middle, f"second call uploaded {after - middle} objects (expected 0)"

    def test_without_digest_round_trips(self, conformance_http_with_storage_port: int) -> None:
        """``include_sha256=False``: the client skips the content check and still resolves."""
        value = _published_value()
        with _external_proxy(conformance_http_with_storage_port) as proxy:
            assert proxy.published_string(value=value, include_sha256=False) == value

    def test_publish_uses_worker_compression(self, conformance_http_with_zstd_storage_port: int) -> None:
        """A worker with zstd externalisation publishes compressed; the digest covers the raw bytes."""
        import httpx2

        value = _published_value()
        base_url = f"http://127.0.0.1:{conformance_http_with_zstd_storage_port}"
        with httpx2.Client(base_url=base_url, timeout=5.0) as client:
            url, digest = _published_string_pointer(client, value, include_sha256=True)
            head = client.head(url)
        assert head.status_code == 200
        assert head.headers.get("Content-Encoding") == "zstd", "published object should use the worker's compression"
        raw = _fetch_published(url)
        assert digest == hashlib.sha256(raw).hexdigest()
        with _external_proxy(conformance_http_with_zstd_storage_port) as proxy:
            assert proxy.published_string(value=value, include_sha256=True) == value
