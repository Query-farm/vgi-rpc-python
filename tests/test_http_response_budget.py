# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Canonical HTTP response-budget contract tests."""

from __future__ import annotations

import json
import random
import zlib
from collections.abc import Iterator
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Protocol, cast

import falcon
import falcon.testing
import httpx2
import pyarrow as pa
import pytest

from vgi_rpc.http import http_capabilities, http_connect, make_sync_client, make_wsgi_app
from vgi_rpc.http._common import (
    _ARROW_CONTENT_TYPE,
    ACCEPT_MAX_RESPONSE_BYTES_HEADER,
    ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER,
)
from vgi_rpc.http._retry import _post_bounded
from vgi_rpc.http._testing import _SyncTestResponse
from vgi_rpc.http.server._responses import _parse_accepted_response_bytes
from vgi_rpc.rpc import (
    AuthContext,
    CallContext,
    OutputCollector,
    ProducerState,
    RpcError,
    RpcServer,
    Stream,
    _write_request,
)

_VECTORS = json.loads((Path(__file__).parents[1] / "vgi_rpc/conformance/http_response_budget_vectors.json").read_text())


class _BudgetProtocol(Protocol):
    def observe(self) -> str: ...

    def blob(self, size: int) -> bytes: ...

    def producer(self, large_on_turn: int, size: int) -> Stream[ProducerState]: ...


_SEEN_OUTPUT_BUDGETS: list[tuple[int | None, int | None, int | None]] = []


@dataclass
class _BudgetProducer(ProducerState):
    large_on_turn: int
    size: int
    turn: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        self.turn += 1
        _SEEN_OUTPUT_BUDGETS.append(
            (out.response_limit_bytes, out.preferred_response_bytes, out.remaining_response_bytes)
        )
        payload = random.Random(7).randbytes(self.size) if self.turn == self.large_on_turn else b"ok"
        out.emit_pydict({"payload": [payload]})
        if self.turn >= self.large_on_turn:
            out.finish()


class _BudgetImpl:
    calls = 0

    def observe(self, ctx: CallContext) -> str:
        self.calls += 1
        return f"{ctx.response_limit_bytes}:{ctx.preferred_response_bytes}"

    def blob(self, size: int) -> bytes:
        self.calls += 1
        return b"x" * size

    def producer(self, large_on_turn: int, size: int, ctx: CallContext) -> Stream[_BudgetProducer]:
        self.calls += 1
        assert (
            ctx.preferred_response_bytes is None
            or ctx.response_limit_bytes is None
            or (ctx.preferred_response_bytes <= ctx.response_limit_bytes)
        )
        return Stream(
            output_schema=pa.schema([pa.field("payload", pa.binary())]),
            state=_BudgetProducer(large_on_turn=large_on_turn, size=size),
        )


def _request_body(method: str, schema: pa.Schema, values: dict[str, object]) -> bytes:
    buf = BytesIO()
    _write_request(buf, method, schema, values)
    return buf.getvalue()


def test_capability_is_on_options_errors_and_cors_preflight() -> None:
    """Support is universal, discoverable, and permitted by CORS."""
    app = make_wsgi_app(
        RpcServer(_BudgetProtocol, _BudgetImpl()),
        token_key=b"budget-key",
        cors_origins="https://caller.example",
    )
    client = falcon.testing.TestClient(app)

    options = client.simulate_options("/health")
    assert options.headers[ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER.lower()] == "true"

    missing = client.simulate_get("/missing")
    assert missing.headers[ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER.lower()] == "true"

    preflight = client.simulate_options(
        "/observe",
        headers={
            "Origin": "https://caller.example",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": ACCEPT_MAX_RESPONSE_BYTES_HEADER,
        },
    )
    assert ACCEPT_MAX_RESPONSE_BYTES_HEADER.lower() in preflight.headers["access-control-allow-headers"].lower()


def test_capability_discovery_and_native_default_header() -> None:
    """Discovery parses support and native clients send the 256 MiB default."""
    seen: list[str | None] = []

    def authenticate(req: falcon.Request) -> AuthContext:
        seen.append(req.get_header(ACCEPT_MAX_RESPONSE_BYTES_HEADER))
        return AuthContext.anonymous()

    client = make_sync_client(
        RpcServer(_BudgetProtocol, _BudgetImpl()), token_key=b"budget-key", authenticate=authenticate
    )

    class _ObservingClient:
        prefix = ""

        def options(self, url: str, *, headers: dict[str, str] | None = None) -> Any:
            seen.append(None if headers is None else headers.get(ACCEPT_MAX_RESPONSE_BYTES_HEADER))
            return client.options(url, headers=headers)

        def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> Any:
            return client.post(url, content=content, headers=headers)

    observing = _ObservingClient()
    caps = http_capabilities(client=cast("Any", observing), accepted_max_response_bytes=256 * 1024 * 1024)
    assert caps.accept_max_response_bytes_support is True
    assert seen[-1] == str(256 * 1024 * 1024)
    with http_connect(_BudgetProtocol, client=cast("Any", observing)) as proxy:
        proxy.observe()
    assert seen[-1] == str(256 * 1024 * 1024)


def test_capability_discovery_accepts_204_and_rejects_error_with_support() -> None:
    """Discovery requires a successful response, not only a support field."""

    class _StatusClient:
        prefix = ""

        def __init__(self, status: int) -> None:
            self.status = status

        def options(self, url: str, *, headers: dict[str, str] | None = None) -> _SyncTestResponse:
            assert headers == {ACCEPT_MAX_RESPONSE_BYTES_HEADER: "65536"}
            return _SyncTestResponse(
                self.status,
                b"",
                headers={ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER: "true"},
            )

    caps = http_capabilities(
        client=cast("Any", _StatusClient(204)),
        accepted_max_response_bytes=65536,
    )
    assert caps.accept_max_response_bytes_support is True
    with pytest.raises(RpcError, match="Capability discovery failed with HTTP 500"):
        http_capabilities(
            client=cast("Any", _StatusClient(500)),
            accepted_max_response_bytes=65536,
        )


def test_configured_client_fails_closed_before_post_when_support_is_absent() -> None:
    """A client never advertises a hard limit without first discovering support."""
    inner = make_sync_client(RpcServer(_BudgetProtocol, _BudgetImpl()), token_key=b"budget-key")

    class _LegacyClient:
        prefix = ""
        posts = 0

        def options(self, url: str, *, headers: dict[str, str] | None = None) -> Any:
            response = inner.options(url, headers=headers)
            response.headers.pop(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER.lower(), None)
            return response

        def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> Any:
            self.posts += 1
            return inner.post(url, content=content, headers=headers)

    legacy = _LegacyClient()
    with (
        http_connect(_BudgetProtocol, client=cast("Any", legacy)) as proxy,
        pytest.raises(RpcError, match="does not advertise"),
    ):
        proxy.observe()
    assert legacy.posts == 0


def test_client_rejects_a_nonconforming_oversize_decoded_body() -> None:
    """The advertised client bound remains hard if the server violates it."""
    inner = make_sync_client(RpcServer(_BudgetProtocol, _BudgetImpl()), token_key=b"budget-key")

    class _ViolatingClient:
        prefix = ""

        def options(self, url: str, *, headers: dict[str, str] | None = None) -> Any:
            return inner.options(url, headers=headers)

        def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> Any:
            response = inner.post(url, content=content, headers=headers)
            response.content += b"x" * 65536
            return response

    with (
        http_connect(
            _BudgetProtocol,
            client=cast("Any", _ViolatingClient()),
            accepted_max_response_bytes=65536,
        ) as proxy,
        pytest.raises(RpcError) as caught,
    ):
        proxy.observe()
    assert caught.value.error_type == "ResponseTooLargeError"


@pytest.mark.parametrize("bad_value", [None, "TRUE", "true, true"])
def test_every_capped_post_requires_one_exact_support_header(bad_value: str | None) -> None:
    """Capability discovery cannot substitute for per-response acknowledgement."""
    inner = make_sync_client(RpcServer(_BudgetProtocol, _BudgetImpl()), token_key=b"budget-key")

    class _BadAcknowledgementClient:
        prefix = ""

        def options(self, url: str, *, headers: dict[str, str] | None = None) -> Any:
            return inner.options(url, headers=headers)

        def post(self, url: str, *, content: bytes, headers: dict[str, str]) -> Any:
            response = inner.post(url, content=content, headers=headers)
            response.headers.pop(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, None)
            response.headers.pop(ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER.lower(), None)
            if bad_value is not None:
                response.headers[ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER] = bad_value
            return response

    with (
        http_connect(_BudgetProtocol, client=cast("Any", _BadAcknowledgementClient())) as proxy,
        pytest.raises(RpcError) as caught,
    ):
        proxy.observe()
    assert caught.value.error_type == "ProtocolError"
    assert "exactly one" in caught.value.error_message


def test_real_client_stops_decoded_stream_at_limit_plus_one() -> None:
    """Compressed responses are decoded incrementally and never fully buffered."""
    decoded = b"x" * (4 * 65536)
    compressed = zlib.compress(decoded, wbits=31)

    class _Body(httpx2.SyncByteStream):
        closed = False

        def __iter__(self) -> Iterator[bytes]:
            yield compressed

        def close(self) -> None:
            self.closed = True

    body = _Body()

    def handler(_request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            headers={
                "Content-Encoding": "gzip",
                ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER: "true",
            },
            stream=body,
        )

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client, pytest.raises(RpcError) as caught:
        _post_bounded(
            client,
            "https://worker.example/rpc",
            content=b"request",
            headers={},
            response_limit_bytes=65536,
        )
    assert caught.value.error_type == "ResponseTooLargeError"
    assert "65537 > 65536" in caught.value.error_message
    assert body.closed is True


def test_real_client_rejects_duplicate_support_fields_before_body() -> None:
    """Separate duplicate support fields are rejected before streaming bytes."""
    headers = [
        (ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true"),
        (ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER, "true"),
    ]

    def handler(_request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, headers=headers, content=b"not-arrow")

    with httpx2.Client(transport=httpx2.MockTransport(handler)) as client, pytest.raises(RpcError) as caught:
        _post_bounded(
            client,
            "https://worker.example/rpc",
            content=b"request",
            headers={},
            response_limit_bytes=65536,
        )
    assert caught.value.error_type == "ProtocolError"
    assert "exactly one" in caught.value.error_message


@pytest.mark.parametrize(
    "bad",
    _VECTORS["invalid_values"],
)
def test_malformed_client_limit_is_400_before_dispatch(bad: str) -> None:
    """Non-canonical or out-of-range limits fail before worker dispatch."""
    impl = _BudgetImpl()
    impl.calls = 0
    client = make_sync_client(RpcServer(_BudgetProtocol, impl), token_key=b"budget-key")
    body = _request_body("observe", pa.schema([]), {})
    response = client.post(
        "/observe",
        content=body,
        headers={"Content-Type": _ARROW_CONTENT_TYPE, ACCEPT_MAX_RESPONSE_BYTES_HEADER: bad},
    )
    assert response.status_code == 400
    assert impl.calls == 0


def test_authentication_precedes_budget_header_validation() -> None:
    """Do not let malformed budget fields bypass the normal auth boundary."""
    impl = _BudgetImpl()
    impl.calls = 0

    def reject(_req: falcon.Request) -> AuthContext:
        raise ValueError("bad credential")

    client = make_sync_client(RpcServer(_BudgetProtocol, impl), token_key=b"budget-key", authenticate=reject)
    response = client.post(
        "/observe",
        content=_request_body("observe", pa.schema([]), {}),
        headers={"Content-Type": _ARROW_CONTENT_TYPE, ACCEPT_MAX_RESPONSE_BYTES_HEADER: "invalid"},
    )
    assert response.status_code == 401
    assert response.headers[ACCEPT_MAX_RESPONSE_BYTES_SUPPORT_HEADER.lower()] == "true"
    assert impl.calls == 0


@pytest.mark.parametrize("vector", _VECTORS["valid_values"])
def test_canonical_valid_header_vectors(vector: dict[str, object]) -> None:
    """Every language consumes the same canonical positive-decimal vectors."""

    class _Request:
        def get_header(self, name: str) -> str:
            assert name == ACCEPT_MAX_RESPONSE_BYTES_HEADER
            return str(vector["raw"])

    assert _parse_accepted_response_bytes(_Request()) == vector["value"]  # type: ignore[arg-type]


def test_app_host_client_precedence_and_worker_visible_budgets() -> None:
    """All hard caps take the minimum and preferred is clamped to it."""
    client = make_sync_client(
        RpcServer(_BudgetProtocol, _BudgetImpl()),
        token_key=b"budget-key",
        max_request_bytes=90_000,
        hosting_max_request_bytes=80_000,
        max_response_bytes=100_000,
        hosting_max_response_bytes=80_000,
        preferred_response_bytes=90_000,
    )
    caps = http_capabilities(client=client)
    assert caps.max_request_bytes == 80_000
    assert caps.max_response_bytes == 80_000

    with http_connect(_BudgetProtocol, client=client, accepted_max_response_bytes=70_000) as proxy:
        assert proxy.observe() == "70000:70000"
        list(proxy.producer(large_on_turn=1, size=2))
    assert _SEEN_OUTPUT_BUDGETS[-1][0:2] == (70_000, 70_000)
    assert _SEEN_OUTPUT_BUDGETS[-1][2] is not None
    assert _SEEN_OUTPUT_BUDGETS[-1][2] <= 70_000
    with http_connect(_BudgetProtocol, client=client, accepted_max_response_bytes=None) as proxy:
        assert proxy.observe() == "80000:80000"


def test_unary_client_limit_is_strict_structured_error() -> None:
    """A unary overshoot returns the canonical structured error."""
    client = make_sync_client(RpcServer(_BudgetProtocol, _BudgetImpl()), token_key=b"budget-key")
    with (
        http_connect(_BudgetProtocol, client=client, accepted_max_response_bytes=65536) as proxy,
        pytest.raises(RpcError) as caught,
    ):
        proxy.blob(size=256 * 1024)
    assert caught.value.error_type == "ResponseTooLargeError"
    assert "max_response_bytes (" in caught.value.error_message
    assert "> 65536)" in caught.value.error_message
    assert "blob" in caught.value.error_message


def test_producer_init_and_continuation_are_strict() -> None:
    """Both producer response shapes strict-fail rather than overshooting."""
    client = make_sync_client(RpcServer(_BudgetProtocol, _BudgetImpl()), token_key=b"budget-key")

    with (
        http_connect(_BudgetProtocol, client=client, accepted_max_response_bytes=65536) as proxy,
        pytest.raises(RpcError) as init_error,
    ):
        proxy.producer(large_on_turn=1, size=256 * 1024)
    assert init_error.value.error_type == "ResponseTooLargeError"

    with http_connect(_BudgetProtocol, client=client, accepted_max_response_bytes=65536) as proxy:
        stream = cast("Any", proxy.producer(large_on_turn=2, size=256 * 1024))
        batches = iter(stream)
        assert next(batches).batch.column("payload")[0].as_py() == b"ok"
        with pytest.raises(RpcError) as continuation_error:
            next(batches)
    assert continuation_error.value.error_type == "ResponseTooLargeError"


def test_resumed_stream_cannot_raise_init_response_limit() -> None:
    """A continuation may tighten, but cannot raise, the init-time hard limit."""
    client = make_sync_client(
        RpcServer(_BudgetProtocol, _BudgetImpl()),
        token_key=b"budget-key",
        call_state_cache_entries=0,
    )
    with http_connect(_BudgetProtocol, client=client, accepted_max_response_bytes=65536) as proxy:
        original = cast("Any", proxy.producer(large_on_turn=2, size=128 * 1024))
        first, token = original.next_with_token()
        assert first is not None
        assert token is not None
    with http_connect(_BudgetProtocol, client=client, accepted_max_response_bytes=256 * 1024) as proxy:
        resumed = cast("Any", proxy).resume_stream("producer", token)
        with pytest.raises(RpcError) as caught:
            resumed.next_with_token()
    assert caught.value.error_type == "ResponseTooLargeError"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_response_bytes": 0},
        {"max_response_bytes": 65535},
        {"hosting_max_response_bytes": True},
        {"preferred_response_bytes": 1 << 53},
        {"hosting_max_request_bytes": -1},
    ],
)
def test_server_budget_configuration_is_portably_bounded(kwargs: dict[str, object]) -> None:
    """Configuration accepts only portable integer limits of at least 64 KiB."""
    with pytest.raises(ValueError):
        make_wsgi_app(RpcServer(_BudgetProtocol, _BudgetImpl()), token_key=b"budget-key", **kwargs)  # type: ignore[arg-type]
