# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Shared conformance cases for ``StateRef``, the object cache and argument refs.

``tools/cross-port/specs/STATE_REF_V1.md`` §15, tests 1-14.  Every SDK's
runner collects these through :mod:`vgi_rpc.conformance._pytest_suite`.  They
drive the ``ConformanceService`` methods ``state_ref_exchange``,
``object_cache_stats``, ``object_cache_age`` and ``resolve_argument_ref`` over
raw HTTP (the proxy deliberately hides state tokens, which several cases must
see or tamper with), plus the ordinary connection factory for the
every-transport cases.

Runner fixtures
---------------

``conformance_state_ref_replicas`` -> :class:`StateRefReplicas`
    Two HTTP workers sharing one ``token_key`` and one fake storage
    (``vgi-rpc-fake-storage``, :mod:`vgi_rpc.conformance.fake_storage`) as
    both external storage and upload-URL provider, configured with:

    * **no request cap** (no ``VGI-Max-Request-Bytes``);
    * the cursor rule set to **error** (``cursor_too_large``);
    * an object cache bound of :data:`OBJECT_CACHE_TEST_BYTES`
      (``VGI_RPC_OBJECT_CACHE_BYTES``);
    * the default ``StateRef`` inline limit (16384) and ``token_ttl``;
    * authentication that **requires** ``X-Conformance-Principal: <name>``
      (the principal is the header value, domain ``conformance``) and
      answers 401 without it.

``conformance_state_ref_short_ttl_port`` -> ``int``
    One HTTP worker with the fake storage, no request cap, no
    authentication, and ``token_ttl`` of **2** seconds.

``conformance_cursor_budget_ports`` -> :class:`CursorBudgetPorts`
    Three HTTP workers without storage: ``warn`` (request cap 32768, cursor
    rule warn), ``error`` (request cap 32768, cursor rule error) and
    ``floor`` (request cap 9000, cursor rule error: below the 16 KiB floor,
    so the rule is disabled).

``conformance_state_ref_capabilities`` -> ``frozenset[str]`` (optional)
    ``{"serialized_state"}`` when HTTP stream state is serialized into tokens
    (the default when the fixture is absent).  The single-replica SDKs of
    ``STATE_REF_V1.md`` §5.4 (C#, C, C++) return ``frozenset()``: the cases
    that need stored tags, cross-replica turns, the cursor rule, codec bytes
    or tag expiry then skip with :data:`SINGLE_REPLICA_SKIP_REASON`, which
    those lanes list in ``EXPECTED_SKIP_REASONS``.

A runner without one of the port fixtures skips the cases that need it with a
reason naming the fixture; lanes fail on unlisted skips, so a port cannot
miss them silently.
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from io import BytesIO
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pytest
from pyarrow import ipc

from vgi_rpc.conformance._external_pytest import _request_body
from vgi_rpc.conformance._types import STATE_REF_INPUT_SCHEMA, ObjectCacheStatsResult, state_ref_payload
from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import CALL_STATE_KEY, STATE_KEY
from vgi_rpc.rpc import AnnotatedBatch, RpcError, _dispatch_log_or_error
from vgi_rpc.rpc._wire import _write_request
from vgi_rpc.utils import new_ipc_stream

if TYPE_CHECKING:
    from collections.abc import Callable

    import httpx2

__all__ = [
    "OBJECT_CACHE_TEST_BYTES",
    "SINGLE_REPLICA_SKIP_REASON",
    "STORED_PAYLOAD_BYTES",
    "CursorBudgetPorts",
    "StateRefReplicas",
    "TestArgumentRefs",
    "TestCursorBudget",
    "TestObjectCache",
    "TestStateRef",
]

#: Object-cache bound of the replica workers: one stored payload fits, two do not.
OBJECT_CACHE_TEST_BYTES = 98304
#: Size of an incompressible payload that is always stored (over the 16 KiB inline limit).
STORED_PAYLOAD_BYTES = 65536
#: Cursor padding well over the 32768-byte cap's turn budget (~20 KB).
OVER_BUDGET_PADDING = 40000
#: Skip reason for the single-replica SDKs (STATE_REF_V1.md §5.4).
SINGLE_REPLICA_SKIP_REASON = (
    "single-replica SDK: HTTP stream state is never serialized, so StateRefs are in-memory and inline only "
    "(STATE_REF_V1.md §5.4)"
)

_PRINCIPAL_HEADER = "X-Conformance-Principal"
_SERVICE = "ConformanceService"
_ARROW = "application/vnd.apache.arrow.stream"
_IDENTITY_HEADERS = {"Content-Type": _ARROW, "Accept-Encoding": "identity", "X-VGI-Accept-Encoding": "identity"}


@dataclass(frozen=True)
class StateRefReplicas:
    """Two HTTP workers sharing a token key and a fake storage (see module docstring).

    Attributes:
        ports: ``(a, b)``.
        storage_url: The fake storage both use, e.g. ``http://127.0.0.1:<port>``.

    """

    ports: tuple[int, int]
    storage_url: str


@dataclass(frozen=True)
class CursorBudgetPorts:
    """The cursor-rule workers (see module docstring).

    Attributes:
        warn: Request cap 32768, rule ``warn``.
        error: Request cap 32768, rule ``error``.
        floor: Request cap 9000, rule ``error`` (disabled by the floor).

    """

    warn: int
    error: int
    floor: int


# ---------------------------------------------------------------------------
# Fixture access
# ---------------------------------------------------------------------------


def _capabilities(request: pytest.FixtureRequest) -> frozenset[str]:
    try:
        caps = request.getfixturevalue("conformance_state_ref_capabilities")
    except pytest.FixtureLookupError:
        return frozenset({"serialized_state"})
    return frozenset(caps)


def _require_serialized(request: pytest.FixtureRequest) -> None:
    if "serialized_state" not in _capabilities(request):
        pytest.skip(SINGLE_REPLICA_SKIP_REASON)


def _fixture(request: pytest.FixtureRequest, name: str) -> Any:
    try:
        return request.getfixturevalue(name)
    except pytest.FixtureLookupError:
        pytest.skip(f"runner provides no {name!r} fixture (STATE_REF_V1.md §15; see _state_ref_pytest)")


def _replicas(request: pytest.FixtureRequest) -> StateRefReplicas:
    replicas = _fixture(request, "conformance_state_ref_replicas")
    assert isinstance(replicas, StateRefReplicas)
    return replicas


def _seed() -> str:
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Raw HTTP driver
# ---------------------------------------------------------------------------


@dataclass
class _Reply:
    """One decoded HTTP response."""

    status: int
    headers: Any
    rows: list[dict[str, Any]] = field(default_factory=list)
    cursor: bytes | None = None
    call: bytes | None = None
    logs: list[Message] = field(default_factory=list)
    error: RpcError | None = None

    @property
    def row(self) -> dict[str, Any]:
        assert self.error is None, f"unexpected error: {self.error}"
        assert len(self.rows) == 1, f"expected one row, got {self.rows!r}"
        return self.rows[0]


def _decode(response: httpx2.Response) -> _Reply:
    reply = _Reply(status=response.status_code, headers=response.headers)
    if not response.content:
        return reply
    try:
        reader = ipc.open_stream(BytesIO(response.content))
    except pa.ArrowInvalid:
        return reply
    while True:
        try:
            batch, metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            break
        try:
            if _dispatch_log_or_error(batch, metadata, reply.logs.append):
                continue
        except RpcError as exc:
            reply.error = exc
            continue
        if metadata is not None:
            reply.cursor = metadata.get(STATE_KEY) or reply.cursor
            reply.call = metadata.get(CALL_STATE_KEY) or reply.call
        reply.rows.extend(batch.to_pylist())
    return reply


def _headers(principal: str | None) -> dict[str, str]:
    headers = dict(_IDENTITY_HEADERS)
    if principal is not None:
        headers[_PRINCIPAL_HEADER] = principal
    return headers


def _client(port: int) -> httpx2.Client:
    import httpx2

    return httpx2.Client(base_url=f"http://127.0.0.1:{port}", timeout=15.0)


def _init(
    client: httpx2.Client,
    *,
    size: int,
    compressible: bool = False,
    seed: str,
    misuse: bool = False,
    cursor_padding: int = 0,
    principal: str | None = None,
) -> _Reply:
    body = _request_body(
        "state_ref_exchange",
        size=size,
        compressible=compressible,
        seed=seed,
        misuse=misuse,
        cursor_padding=cursor_padding,
    )
    return _decode(client.post(f"/{_SERVICE}/state_ref_exchange/init", content=body, headers=_headers(principal)))


def _exchange(
    client: httpx2.Client,
    cursor: bytes,
    call: bytes,
    *,
    override: bytes | None = None,
    principal: str | None = None,
) -> _Reply:
    batch = pa.RecordBatch.from_pydict({"override": [override]}, schema=STATE_REF_INPUT_SCHEMA)
    out = BytesIO()
    with new_ipc_stream(out, batch.schema) as writer:
        writer.write_batch(batch, custom_metadata=pa.KeyValueMetadata({STATE_KEY: cursor, CALL_STATE_KEY: call}))
    return _decode(
        client.post(f"/{_SERVICE}/state_ref_exchange/exchange", content=out.getvalue(), headers=_headers(principal))
    )


def _unary(client: httpx2.Client, method: str, *, principal: str | None = None, **kwargs: object) -> _Reply:
    return _decode(
        client.post(f"/{_SERVICE}/{method}", content=_request_body(method, **kwargs), headers=_headers(principal))
    )


def _result(reply: _Reply) -> Any:
    assert reply.error is None, f"unexpected error: {reply.error}"
    assert reply.rows, "unary response carried no result"
    return reply.rows[0]["result"]


def _cache_stats(client: httpx2.Client) -> ObjectCacheStatsResult:
    """Call ``object_cache_stats`` (a record result, so nested IPC bytes on the wire)."""
    raw = _result(_unary(client, "object_cache_stats", principal="alice"))
    return raw if isinstance(raw, ObjectCacheStatsResult) else ObjectCacheStatsResult.deserialize_from_bytes(raw)


def _storage_stats(storage_url: str) -> dict[str, int]:
    import httpx2

    response = httpx2.get(f"{storage_url}/_stats", timeout=5.0)
    response.raise_for_status()
    return {str(k): int(v) for k, v in response.json().items()}


def _stream(client: httpx2.Client, *, principal: str | None = None, **kwargs: Any) -> tuple[bytes, bytes]:
    """Open a ``state_ref_exchange`` stream and return its ``(cursor, call)`` tokens."""
    reply = _init(client, principal=principal, **kwargs)
    assert reply.error is None, f"init failed: {reply.error}"
    assert reply.cursor is not None and reply.call is not None, "init returned no state tokens"
    return reply.cursor, reply.call


def _assert_kind(reply: _Reply, kind: str, *, status: int | None = None) -> None:
    assert reply.error is not None, f"expected {kind}, got rows {reply.rows!r} (HTTP {reply.status})"
    assert reply.error.error_kind == kind, f"expected kind {kind!r}, got {reply.error.error_kind!r}: {reply.error}"
    if status is not None:
        assert reply.status == status, f"expected HTTP {status} for {kind}, got {reply.status}"


def _grant(client: httpx2.Client, payload: bytes, *, principal: str | None) -> tuple[_Reply, httpx2.Response]:
    """Request one upload URL for *payload*; return the decoded reply and the raw response."""
    from vgi_rpc.http import UPLOAD_URL_PARAMS_SCHEMA

    buf = BytesIO()
    _write_request(
        buf,
        "__upload_url__",
        UPLOAD_URL_PARAMS_SCHEMA,
        {"uploads": [{"size": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}]},
    )
    response = client.post("/__upload_url__/init", content=buf.getvalue(), headers=_headers(principal))
    return _decode(response), response


def _upload(client: httpx2.Client, payload: bytes, *, principal: str | None) -> dict[str, Any]:
    """Grant, PUT and return the grant row (``object_id``, ``ref``, ...)."""
    import httpx2

    reply, _ = _grant(client, payload, principal=principal)
    row = reply.row
    headers = {str(k): str(v) for k, v in (row["upload_headers"] or [])}
    if not any(k.lower() == "content-type" for k in headers):
        headers["Content-Type"] = "application/octet-stream"
    put = httpx2.put(row["upload_url"], content=payload, headers=headers, timeout=10.0)
    assert 200 <= put.status_code < 300, f"PUT refused: {put.status_code}"
    return row


def _resolve_arg(
    client: httpx2.Client,
    ref: bytes,
    payload: bytes,
    *,
    principal: str | None,
    size: int | None = None,
    sha256: str | None = None,
) -> _Reply:
    return _unary(
        client,
        "resolve_argument_ref",
        principal=principal,
        ref=ref,
        size=len(payload) if size is None else size,
        sha256=hashlib.sha256(payload).hexdigest() if sha256 is None else sha256,
    )


# ---------------------------------------------------------------------------
# StateRef (tests 1-4, 9, 10)
# ---------------------------------------------------------------------------


class TestStateRef:
    """``StateRef``: round trips, cross-replica, scope refusals, codec, misuse, expiry."""

    def test_inline_round_trip_on_every_transport(self, conformance_conn: Callable[..., Any]) -> None:
        """Test 1 (inline): with no storage the value round-trips on every turn of every transport."""
        seed = _seed()
        payload = state_ref_payload(4096, False, seed)
        empty = AnnotatedBatch.from_pydict({"override": [None]}, schema=STATE_REF_INPUT_SCHEMA)
        with (
            conformance_conn() as proxy,
            proxy.state_ref_exchange(
                size=4096, compressible=False, seed=seed, misuse=False, cursor_padding=0
            ) as session,
        ):
            for _ in range(3):
                out = session.exchange(empty)
                assert out.batch.column("size")[0].as_py() == len(payload)
                assert out.batch.column("sha256")[0].as_py() == hashlib.sha256(payload).hexdigest()
                assert out.batch.column("form")[0].as_py() in ("memory", "inline")

    def test_state_ref_on_a_continuation_is_misuse(self, conformance_conn: Callable[..., Any]) -> None:
        """Test 9: ``state_ref()`` while handling a continuation raises ``state_ref_misuse``."""
        empty = AnnotatedBatch.from_pydict({"override": [None]}, schema=STATE_REF_INPUT_SCHEMA)
        with (
            conformance_conn() as proxy,
            proxy.state_ref_exchange(
                size=16, compressible=True, seed=_seed(), misuse=True, cursor_padding=0
            ) as session,
            pytest.raises(RpcError) as raised,
        ):
            session.exchange(empty)
        assert raised.value.error_kind == "state_ref_misuse"
        assert raised.value.error_code == "FAILED_PRECONDITION"
        assert "state_ref() may only be called while handling a stream's init request" in raised.value.error_message

    def test_stored_round_trip(self, request: pytest.FixtureRequest) -> None:
        """Test 1 (stored): an over-limit value is stored exactly once and resolves on every turn."""
        _require_serialized(request)
        replicas = _replicas(request)
        seed = _seed()
        payload = state_ref_payload(STORED_PAYLOAD_BYTES, False, seed)
        before = _storage_stats(replicas.storage_url)
        with _client(replicas.ports[0]) as client:
            cursor, call = _stream(client, size=STORED_PAYLOAD_BYTES, seed=seed, principal="alice")
            for _ in range(3):
                reply = _exchange(client, cursor, call, principal="alice")
                assert reply.row["form"] == "stored"
                assert reply.row["sha256"] == hashlib.sha256(payload).hexdigest()
                assert reply.row["size"] == len(payload)
                assert reply.cursor is not None
                cursor = reply.cursor
        after = _storage_stats(replicas.storage_url)
        assert after["object_puts"] - before["object_puts"] == 1, "a stored StateRef is put exactly once"

    def test_cross_replica_resolve(self, request: pytest.FixtureRequest) -> None:
        """Test 2: init on A, continue on B; B fetches the object exactly once."""
        _require_serialized(request)
        replicas = _replicas(request)
        seed = _seed()
        payload = state_ref_payload(STORED_PAYLOAD_BYTES, False, seed)
        with _client(replicas.ports[0]) as a, _client(replicas.ports[1]) as b:
            cursor, call = _stream(a, size=STORED_PAYLOAD_BYTES, seed=seed, principal="alice")
            before = _storage_stats(replicas.storage_url)
            first = _exchange(b, cursor, call, principal="alice")
            assert first.row["sha256"] == hashlib.sha256(payload).hexdigest()
            assert first.cursor is not None
            second = _exchange(b, first.cursor, call, principal="alice")
            assert second.row["sha256"] == hashlib.sha256(payload).hexdigest()
        after = _storage_stats(replicas.storage_url)
        assert after["object_gets"] - before["object_gets"] == 1, "replica B must get the object once, then cache it"

    def test_foreign_and_tampered_tags_are_refused(self, request: pytest.FixtureRequest) -> None:
        """Test 3: tampered, other-stream, other-principal, cursor-as-tag and argref-as-tag are refused."""
        import base64

        _require_serialized(request)
        replicas = _replicas(request)
        with _client(replicas.ports[0]) as client:
            x_cursor, x_call = _stream(client, size=STORED_PAYLOAD_BYTES, seed=_seed(), principal="alice")
            x = _exchange(client, x_cursor, x_call, principal="alice")
            x_ref = x.row["ref"]
            assert x.row["form"] == "stored" and isinstance(x_ref, bytes)
            y_cursor, y_call = _stream(client, size=STORED_PAYLOAD_BYTES, seed=_seed(), principal="alice")
            bob_cursor, bob_call = _stream(client, size=STORED_PAYLOAD_BYTES, seed=_seed(), principal="bob")
            grant_row = _upload(client, b"an argument", principal="alice")

            tampered = bytearray(x_ref)
            tampered[len(tampered) // 2] ^= 0x01
            presented = {
                "tampered tag": bytes(tampered),
                "another stream's tag": x_ref,
                "a cursor token presented as a tag": b"\x02" + base64.b64decode(x_cursor),
                "an argument ref presented as a tag": b"\x02" + grant_row["ref"],
                "an unknown form byte": b"\x07" + x_ref[1:],
            }
            for label, override in presented.items():
                cursor, call = (x_cursor, x_call) if label == "tampered tag" else (y_cursor, y_call)
                reply = _exchange(client, cursor, call, override=override, principal="alice")
                assert reply.error is not None, f"{label} was accepted"
                _assert_kind(reply, "invalid_state_ref", status=400)
            reply = _exchange(client, bob_cursor, bob_call, override=x_ref, principal="bob")
            _assert_kind(reply, "invalid_state_ref", status=400)

    def test_codec(self, conformance_http_port: int, request: pytest.FixtureRequest) -> None:
        """Test 4: a large payload compresses, a tiny one stays raw, an unknown codec is refused."""
        _require_serialized(request)
        with _client(conformance_http_port) as client:
            cursor, call = _stream(client, size=8192, compressible=True, seed=_seed())
            large = _exchange(client, cursor, call)
            assert large.row["form"] == "inline"
            assert large.row["ref"][0] == 0x01 and large.row["ref"][1] in (0x01, 0x02), "large payload not compressed"
            assert len(large.row["ref"]) < 8192 // 4

            cursor, call = _stream(client, size=3, compressible=False, seed=_seed())
            tiny = _exchange(client, cursor, call)
            assert tiny.row["ref"][:2] == b"\x01\x00", "a tiny payload must stay raw (codec 0x00)"

            refused = _exchange(client, cursor, call, override=b"\x01\x7f" + b"payload")
            _assert_kind(refused, "invalid_state_ref", status=400)

    def test_expired_tag_is_refused_before_any_get(self, request: pytest.FixtureRequest) -> None:
        """Test 10: once ``token_ttl`` has passed the turn is refused, and nothing is fetched."""
        _require_serialized(request)
        port = int(_fixture(request, "conformance_state_ref_short_ttl_port"))
        storage_url = str(_fixture(request, "conformance_fake_storage"))
        with _client(port) as client:
            cursor, call = _stream(client, size=STORED_PAYLOAD_BYTES, seed=_seed())
            before = _storage_stats(storage_url)
            deadline = time.monotonic() + 8.0
            refused: _Reply | None = None
            while time.monotonic() < deadline:
                time.sleep(1.0)
                reply = _exchange(client, cursor, call)
                if reply.error is not None:
                    refused = reply
                    break
                assert reply.cursor is not None
                cursor = reply.cursor
            assert refused is not None, "a stream older than token_ttl was still served"
            assert refused.status == 400, f"expiry must be a 400, got {refused.status}"
            gets = _storage_stats(storage_url)["object_gets"] - before["object_gets"]
        # The worker that minted the stream holds the value from init and may
        # resolve live turns from its cache; what matters is that the refusal
        # itself fetched nothing.
        assert gets <= 1, f"an expired stream fetched its object ({gets} gets)"


# ---------------------------------------------------------------------------
# Cursor rule (tests 5, 12)
# ---------------------------------------------------------------------------


def _cursor_warnings(reply: _Reply) -> list[Message]:
    return [m for m in reply.logs if m.level == Level.WARN and m.message.startswith("cursor_large")]


class TestCursorBudget:
    """The turn budget's cursor rule and its 16 KiB floor."""

    def test_warn_rule_warns_and_continues(self, request: pytest.FixtureRequest) -> None:
        """Test 5 (before the port): an over-budget cursor logs ``cursor_large`` and the stream continues."""
        _require_serialized(request)
        ports = _fixture(request, "conformance_cursor_budget_ports")
        with _client(ports.warn) as client:
            reply = _init(client, size=64, seed=_seed(), cursor_padding=OVER_BUDGET_PADDING)
        assert reply.error is None, f"warn rule must not refuse: {reply.error}"
        assert reply.cursor is not None
        assert _cursor_warnings(reply), f"no cursor_large warning; logs were {[m.message for m in reply.logs]!r}"

    def test_error_rule_refuses(self, request: pytest.FixtureRequest) -> None:
        """Test 5 (after the port): an over-budget cursor is ``cursor_too_large``."""
        _require_serialized(request)
        ports = _fixture(request, "conformance_cursor_budget_ports")
        with _client(ports.error) as client:
            reply = _init(client, size=64, seed=_seed(), cursor_padding=OVER_BUDGET_PADDING)
            _assert_kind(reply, "cursor_too_large")
            assert reply.error is not None and reply.error.error_code == "RESOURCE_EXHAUSTED"
            assert "ctx.state_ref()" in reply.error.error_message
            within = _init(client, size=64, seed=_seed(), cursor_padding=1024)
            assert within.error is None and within.cursor is not None and not _cursor_warnings(within)

    def test_no_error_without_a_request_cap(self, request: pytest.FixtureRequest) -> None:
        """Test 5: with ``max_request_bytes`` unset the error rule never fires."""
        _require_serialized(request)
        replicas = _replicas(request)
        with _client(replicas.ports[0]) as client:
            reply = _init(client, size=64, seed=_seed(), cursor_padding=OVER_BUDGET_PADDING, principal="alice")
            assert reply.error is None and reply.cursor is not None and not _cursor_warnings(reply)

    def test_below_the_floor_the_rule_is_off(self, request: pytest.FixtureRequest) -> None:
        """Test 12: a cap below 16384 starts, and never emits ``cursor_large`` / ``cursor_too_large``."""
        _require_serialized(request)
        ports = _fixture(request, "conformance_cursor_budget_ports")
        with _client(ports.floor) as client:
            reply = _init(client, size=64, seed=_seed(), cursor_padding=OVER_BUDGET_PADDING)
        assert reply.error is None, f"the floor disables the rule, but the init was refused: {reply.error}"
        assert reply.cursor is not None and not _cursor_warnings(reply)


# ---------------------------------------------------------------------------
# Object cache (tests 6, 7, 8, 14) -- via argument refs, so every SDK runs them
# ---------------------------------------------------------------------------


class TestObjectCache:
    """The per-process object cache: single-flight, byte bound, no pinning, one-day age."""

    def test_concurrent_resolves_share_one_get(self, request: pytest.FixtureRequest) -> None:
        """Test 6: N concurrent resolves of one object make one ``get``."""
        replicas = _replicas(request)
        payload = state_ref_payload(32768, False, _seed())
        with _client(replicas.ports[1]) as client:
            row = _upload(client, payload, principal="alice")
        before = _storage_stats(replicas.storage_url)
        results: list[_Reply] = []
        lock = threading.Lock()

        def resolve() -> None:
            with _client(replicas.ports[1]) as client:
                reply = _resolve_arg(client, row["ref"], payload, principal="alice")
            with lock:
                results.append(reply)

        threads = [threading.Thread(target=resolve) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert len(results) == 8
        for reply in results:
            assert _result(reply) == hashlib.sha256(payload).hexdigest()
        assert _storage_stats(replicas.storage_url)["object_gets"] - before["object_gets"] == 1

    def test_byte_bound_evicts_and_memos_do_not_pin(self, request: pytest.FixtureRequest) -> None:
        """Tests 7 and 8: two values over the bound evict the first; resident bytes stay under the bound."""
        replicas = _replicas(request)
        x = state_ref_payload(STORED_PAYLOAD_BYTES, False, _seed())
        y = state_ref_payload(STORED_PAYLOAD_BYTES, False, _seed())
        with _client(replicas.ports[1]) as client:
            x_row = _upload(client, x, principal="alice")
            y_row = _upload(client, y, principal="alice")
            before = _storage_stats(replicas.storage_url)
            for ref, payload in ((x_row["ref"], x), (y_row["ref"], y), (x_row["ref"], x)):
                assert (
                    _result(_resolve_arg(client, ref, payload, principal="alice"))
                    == hashlib.sha256(payload).hexdigest()
                )
            gets = _storage_stats(replicas.storage_url)["object_gets"] - before["object_gets"]
            stats = _cache_stats(client)
        assert gets == 3, f"the second resolve of the first value must miss after eviction (gets={gets})"
        assert stats.max_bytes == OBJECT_CACHE_TEST_BYTES
        assert stats.resident_bytes <= OBJECT_CACHE_TEST_BYTES
        assert stats.resident_bytes < 2 * STORED_PAYLOAD_BYTES, "evicted values must not stay resident"
        assert stats.evictions >= 1

    def test_entries_age_out_after_a_day(self, request: pytest.FixtureRequest) -> None:
        """Test 14: an entry fetched over a day ago is re-fetched; a deleted object then reports missing."""
        import httpx2

        replicas = _replicas(request)
        payload = state_ref_payload(4096, False, _seed())
        with _client(replicas.ports[1]) as client:
            row = _upload(client, payload, principal="alice")
            before = _storage_stats(replicas.storage_url)
            assert _result(_resolve_arg(client, row["ref"], payload, principal="alice"))
            assert _result(_resolve_arg(client, row["ref"], payload, principal="alice"))
            assert _storage_stats(replicas.storage_url)["object_gets"] - before["object_gets"] == 1
            assert _result(_unary(client, "object_cache_age", principal="alice", seconds=86401)) >= 1
            assert _result(_resolve_arg(client, row["ref"], payload, principal="alice"))
            assert _storage_stats(replicas.storage_url)["object_gets"] - before["object_gets"] == 2

            assert httpx2.delete(f"{replicas.storage_url}/objects/{row['object_id']}", timeout=5.0).status_code == 204
            _result(_unary(client, "object_cache_age", principal="alice", seconds=86401))
            _assert_kind(_resolve_arg(client, row["ref"], payload, principal="alice"), "arguments_ref_missing")


# ---------------------------------------------------------------------------
# Grants and argument references (tests 11, 13)
# ---------------------------------------------------------------------------


class TestArgumentRefs:
    """``__upload_url__/init`` grant columns and argument-reference resolution."""

    def test_grant_columns_resolve(self, request: pytest.FixtureRequest) -> None:
        """Test 11: the grant's ``object_id`` and ``ref`` resolve for the uploaded bytes only."""
        import httpx2

        replicas = _replicas(request)
        payload = b"a large argument " * 64
        with _client(replicas.ports[0]) as client:
            row = _upload(client, payload, principal="alice")
            assert isinstance(row["object_id"], str) and row["object_id"]
            assert isinstance(row["ref"], bytes) and row["ref"]
            assert (
                _result(_resolve_arg(client, row["ref"], payload, principal="alice"))
                == hashlib.sha256(payload).hexdigest()
            )
            _assert_kind(
                _resolve_arg(client, row["ref"], payload, principal="alice", size=len(payload) + 1),
                "invalid_arguments_ref",
            )
            _assert_kind(
                _resolve_arg(client, row["ref"], payload, principal="alice", sha256="0" * 64),
                "invalid_arguments_ref",
            )
            tampered = bytearray(row["ref"])
            tampered[-1] ^= 0x01
            _assert_kind(_resolve_arg(client, bytes(tampered), payload, principal="alice"), "invalid_arguments_ref")

            gone = b"an argument whose object is deleted " * 8
            gone_row = _upload(client, gone, principal="alice")
            assert (
                httpx2.delete(f"{replicas.storage_url}/objects/{gone_row['object_id']}", timeout=5.0).status_code == 204
            )
            _assert_kind(_resolve_arg(client, gone_row["ref"], gone, principal="alice"), "arguments_ref_missing")

    def test_grant_is_authenticated_like_a_method(self, request: pytest.FixtureRequest) -> None:
        """Test 13: a grant's ref opens for the same principal only; an unauthenticated grant is refused."""
        replicas = _replicas(request)
        payload = b"principal-bound argument " * 16
        with _client(replicas.ports[0]) as client:
            row = _upload(client, payload, principal="alice")
            assert _result(_resolve_arg(client, row["ref"], payload, principal="alice"))
            _assert_kind(_resolve_arg(client, row["ref"], payload, principal="bob"), "invalid_arguments_ref")
            _, raw = _grant(client, payload, principal=None)
            assert raw.status_code == 401, (
                f"an unauthenticated grant must be refused like any call, got {raw.status_code}"
            )
            # Replica B shares the key, so the ref opens there for the same principal.
        with _client(replicas.ports[1]) as other:
            assert _result(_resolve_arg(other, row["ref"], payload, principal="alice"))
