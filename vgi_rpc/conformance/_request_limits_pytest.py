# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Portable HTTP request-cap conformance cases."""

from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.rpc import rpc_methods
from vgi_rpc.rpc._wire import _write_request

if TYPE_CHECKING:
    import httpx2

pytestmark = pytest.mark.timeout(10)

_ARROW_CONTENT_TYPE = "application/vnd.apache.arrow.stream"
_CAP_HEADER = "VGI-Max-Request-Bytes"
_CODECS_HEADER = "VGI-Supported-Encodings"


def _request_body(method_name: str, **kwargs: object) -> bytes:
    """Serialize one canonical unary request."""
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


def _result_value(response: httpx2.Response) -> object:
    """Read the unary result scalar from an identity-encoded response."""
    reader = pa.ipc.open_stream(BytesIO(response.content))
    for batch in reader:
        if "result" in batch.schema.names:
            return batch.column("result")[0].as_py()
    raise AssertionError("response carried no result batch")


class TestCompressedHttpRequestCap:
    """Encoded and decoded request bytes independently obey the advertised cap."""

    @pytest.mark.parametrize("codec", ("zstd", "gzip"))
    def test_decoded_expansion_is_413_and_connection_recovers(
        self,
        request: pytest.FixtureRequest,
        codec: str,
    ) -> None:
        """A small compressed body cannot expand past ``max_request_bytes``."""
        try:
            port: int = request.getfixturevalue("conformance_http_small_request_cap_port")
        except pytest.FixtureLookupError:
            pytest.skip("runner provides no small request-cap HTTP worker")

        import httpx2

        from vgi_rpc._codec import Encoding, compress

        base_url = f"http://127.0.0.1:{port}"
        with httpx2.Client(base_url=base_url, timeout=3.0) as client:
            options = client.options("/health")
            raw_cap = options.headers.get(_CAP_HEADER)
            if raw_cap is None:
                pytest.skip("fixture worker does not advertise max_request_bytes")
            cap = int(raw_cap)
            assert 512 <= cap <= 64 * 1024, f"fixture must advertise a deliberately small request cap, got {cap}"

            advertised_raw = options.headers.get(_CODECS_HEADER)
            assert advertised_raw is not None, "server must advertise request codecs"
            advertised = {item.strip().lower() for item in advertised_raw.split(",") if item.strip()}
            assert advertised == {"zstd", "gzip"}, (
                f"the conformance worker must support both zstd and gzip, got {sorted(advertised)}"
            )

            encoding = Encoding(codec)
            expanded = _request_body("echo_string", value="x" * max(32 * 1024, cap * 8))
            encoded = compress(encoding, expanded)
            assert len(expanded) > cap
            assert len(encoded) <= cap, (
                f"fixture cap {cap} is too small for the {codec} control body ({len(encoded)} encoded bytes)"
            )

            headers = {
                "Content-Type": _ARROW_CONTENT_TYPE,
                "Content-Encoding": codec,
                "Accept-Encoding": "identity",
                "X-VGI-Accept-Encoding": "identity",
            }
            rejected = client.post("/echo_string", content=encoded, headers=headers)
            assert rejected.status_code == 413, (
                f"{codec} decoded expansion must be 413, got {rejected.status_code}: {rejected.content[:200]!r}"
            )

            control = client.post(
                "/echo_int",
                content=_request_body("echo_int", value=41),
                headers={
                    "Content-Type": _ARROW_CONTENT_TYPE,
                    "Accept-Encoding": "identity",
                    "X-VGI-Accept-Encoding": "identity",
                },
            )
            assert control.status_code == 200, control.content[:200]
            assert _result_value(control) == 41


__all__ = ["TestCompressedHttpRequestCap"]
