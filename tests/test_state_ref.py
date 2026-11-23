# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Language-local tests for StateRef, the object cache, argument refs and the turn budget.

The shared, cross-SDK cases live in ``vgi_rpc/conformance/_state_ref_pytest.py``;
these cover what the wire cannot show (STATE_REF_V1.md §15, last paragraph):
codec tags on the seal path, that a stored ref re-serializes to identical bytes
without a second ``put_object``, memoization, and each error path.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
import threading
import zlib
from dataclasses import dataclass
from typing import Any, Protocol

import pyarrow as pa
import pytest

from vgi_rpc import _token_codec
from vgi_rpc.external import ObjectMissingError, ObjectStore, ServerExternalConfig
from vgi_rpc.http import http_connect, make_sync_client
from vgi_rpc.object_cache import ObjectCache, default_object_cache_bytes
from vgi_rpc.rpc import (
    AnnotatedBatch,
    AuthContext,
    CallContext,
    ExchangeState,
    OutputCollector,
    RpcError,
    RpcServer,
    Stream,
    serve_pipe,
)
from vgi_rpc.state_ref import (
    ArgumentsRefIntegrityError,
    ArgumentsRefMissingError,
    CursorTooLargeError,
    InvalidArgumentsRefError,
    InvalidStateRefError,
    StateRef,
    StateRefMissingError,
    StateRefMisuseError,
    StateRefRuntime,
    StateRefScope,
    StateRefTooLargeError,
    StorageUnavailableError,
    argument_ref_aad,
    base_aad,
    encoding_for_stream,
    mint_argument_ref,
    open_argument_ref,
    resolve_argument_ref,
    state_ref_inline_bytes_from_env,
    turn_budget,
)
from vgi_rpc.utils import ArrowSerializableDataclass

_KEY = b"\x07" * 32
_CALL_ID = b"\x01" * 16
_ALICE = AuthContext(domain="test", authenticated=True, principal="alice")
_BOB = AuthContext(domain="test", authenticated=True, principal="bob")


class MemoryStore:
    """In-memory ``ExternalStorage`` + ``ObjectStore`` with call counters."""

    def __init__(self) -> None:
        """Start empty."""
        self.objects: dict[str, bytes] = {}
        self.puts = 0
        self.gets = 0
        self.fail_gets = False
        self.lock = threading.Lock()

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Unused: StateRef never externalizes."""
        raise AssertionError("not used")

    def put_object(self, data: bytes) -> str:
        """Put object."""
        with self.lock:
            self.puts += 1
            # Unique across instances: the process-wide object cache is keyed by
            # object_id, exactly as real storage ids are unique.
            object_id = f"obj-{os.urandom(8).hex()}"
            self.objects[object_id] = data
            return object_id

    def get(self, object_id: str, *, max_bytes: int | None = None) -> bytes:
        """Return an object by id, counting the call."""
        with self.lock:
            self.gets += 1
            if self.fail_gets:
                raise ConnectionError("storage 503")
            if object_id not in self.objects:
                raise ObjectMissingError("gone")
            return self.objects[object_id]


def _runtime(store: MemoryStore | None, **kwargs: Any) -> StateRefRuntime:
    return StateRefRuntime(
        token_key=_KEY,
        storage=(lambda: store),
        max_fetch_bytes=kwargs.pop("max_fetch_bytes", 1 << 20),
        cache=kwargs.pop("cache", ObjectCache(1 << 20)),
        **kwargs,
    )


def _encode(runtime: StateRefRuntime, ref: StateRef, *, auth: AuthContext | None = _ALICE) -> bytes:
    with encoding_for_stream(runtime, auth=auth, protocol="P", method="m", call_id=_CALL_ID, call_token_bytes=0):
        return ref.serialize_to_bytes()


def _scope(runtime: StateRefRuntime | None, *, auth: AuthContext | None = _ALICE, init: bool = False) -> StateRefScope:
    return StateRefScope(runtime=runtime, auth=auth, protocol="P", method="m", call_id=_CALL_ID, init=init)


# ---------------------------------------------------------------------------
# The token codec
# ---------------------------------------------------------------------------


class TestTokenCodec:
    """The standard token codec (STATE_REF_V1.md §7)."""

    def test_compressible_payload_takes_zstd(self) -> None:
        """Compressible payload takes zstd."""
        packed = _token_codec.pack(b"a" * 4096)
        assert packed[0] == _token_codec.CODEC_ZSTD and len(packed) < 100
        assert _token_codec.unpack(packed) == b"a" * 4096

    def test_without_zstandard_the_writer_uses_deflate(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A bare install (no zstandard) packs with DEFLATE and refuses zstd payloads clearly."""
        zstd_packed = _token_codec.pack(b"c" * 4096)
        monkeypatch.setattr(_token_codec, "_zstandard", lambda: None)
        packed = _token_codec.pack(b"c" * 4096)
        assert packed[0] == _token_codec.CODEC_DEFLATE and len(packed) < 100
        assert _token_codec.unpack(packed) == b"c" * 4096
        with pytest.raises(_token_codec.TokenCodecError, match=r"vgi-rpc\[http\]"):
            _token_codec.unpack(zstd_packed)

    def test_tiny_payload_stays_raw(self) -> None:
        """Tiny payload stays raw."""
        assert _token_codec.pack(b"xy") == b"\x00xy"

    def test_deflate_is_accepted(self) -> None:
        """Deflate is accepted."""
        deflater = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        body = deflater.compress(b"b" * 5000) + deflater.flush()
        assert _token_codec.unpack(b"\x02" + body) == b"b" * 5000

    @pytest.mark.parametrize("data", [b"", b"\x09abc", b"\x01not-zstd", b"\x02\xff\xff"])
    def test_bad_payloads_are_refused(self, data: bytes) -> None:
        """Bad payloads are refused."""
        with pytest.raises(_token_codec.TokenCodecError):
            _token_codec.unpack(data)

    def test_decompression_is_bounded(self) -> None:
        """Decompression is bounded."""
        with pytest.raises(_token_codec.TokenCodecError):
            _token_codec.unpack(_token_codec.pack(b"z" * 10_000), max_output_bytes=100)
        deflater = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        body = deflater.compress(b"z" * 10_000) + deflater.flush()
        with pytest.raises(_token_codec.TokenCodecError):
            _token_codec.unpack(b"\x02" + body, max_output_bytes=100)


# ---------------------------------------------------------------------------
# StateRef encoding and resolution
# ---------------------------------------------------------------------------


class TestStateRefForms:
    """Inline vs stored, decided once at first token serialization (§5)."""

    def test_small_value_is_inline_and_compressed_inside(self) -> None:
        """Small value is inline and compressed inside."""
        runtime = _runtime(MemoryStore())
        ref = StateRef._from_value(b"q" * 2000)
        encoded = _encode(runtime, ref)
        assert encoded[0] == 0x01 and encoded[1] == _token_codec.CODEC_ZSTD
        assert _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(encoded)) == b"q" * 2000

    def test_no_storage_keeps_large_values_inline(self) -> None:
        """No storage keeps large values inline."""
        runtime = _runtime(None)
        value = os.urandom(50_000)
        encoded = _encode(runtime, StateRef._from_value(value))
        assert encoded[0] == 0x01
        assert _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(encoded)) == value

    def test_large_value_is_stored_once_and_reserialized_identically(self) -> None:
        """Large value is stored once and reserialized identically."""
        store = MemoryStore()
        runtime = _runtime(store)
        value = os.urandom(50_000)
        ref = StateRef._from_value(value)
        first = _encode(runtime, ref)
        second = _encode(runtime, ref)
        assert first == second and first[0] == 0x02
        assert store.puts == 1, "re-serializing a stored ref must not upload again"
        assert len(first) < 200
        reread = StateRef.deserialize_from_bytes(first)
        assert _encode(runtime, reread) == first and store.puts == 1
        assert _scope(runtime).resolve_bytes(reread) == value
        assert _scope(runtime).resolve_bytes(reread) == value
        assert store.gets == 1, "the second resolve is served by the object cache"

    def test_stored_object_is_ciphertext(self) -> None:
        """Stored object is ciphertext."""
        store = MemoryStore()
        value = b"secret-value " * 4000
        _encode(_runtime(store, inline_bytes_cap=1), StateRef._from_value(value))
        (stored,) = store.objects.values()
        assert b"secret-value" not in stored

    def test_inline_limit_uses_the_compressed_size(self) -> None:
        """Inline limit uses the compressed size."""
        store = MemoryStore()
        runtime = _runtime(store)
        compressible = b"r" * 200_000  # compresses far below 16 KiB
        assert _encode(runtime, StateRef._from_value(compressible))[0] == 0x01
        assert store.puts == 0

    def test_outside_a_mint_an_unencoded_ref_renders_inline_without_caching(self) -> None:
        """Outside a mint an unencoded ref renders inline without caching."""
        store = MemoryStore()
        ref = StateRef._from_value(os.urandom(50_000))
        assert ref.serialize_to_bytes()[0] == 0x01
        assert ref.form == "memory"
        assert _encode(_runtime(store), ref)[0] == 0x02

    def test_direct_construction_is_refused(self) -> None:
        """Direct construction is refused."""
        with pytest.raises(TypeError):
            StateRef()

    def test_repr_never_shows_the_value(self) -> None:
        """Repr never shows the value."""
        assert "secret" not in repr(StateRef._from_value(b"secret"))


class TestStateRefRefusals:
    """Every resolve refusal of §5.3 / §6."""

    def _stored(self, store: MemoryStore, runtime: StateRefRuntime) -> tuple[bytes, bytes]:
        value = os.urandom(40_000)
        return _encode(runtime, StateRef._from_value(value)), value

    def test_wrong_principal_stream_or_protocol_is_invalid(self) -> None:
        """Wrong principal stream or protocol is invalid."""
        store = MemoryStore()
        runtime = _runtime(store)
        encoded, _ = self._stored(store, runtime)
        ref = StateRef.deserialize_from_bytes(encoded)
        for scope in (
            _scope(runtime, auth=_BOB),
            _scope(runtime, auth=None),
            StateRefScope(runtime=runtime, auth=_ALICE, protocol="P", method="m", call_id=b"\x02" * 16),
            StateRefScope(runtime=runtime, auth=_ALICE, protocol="Q", method="m", call_id=_CALL_ID),
        ):
            with pytest.raises(InvalidStateRefError):
                scope.resolve_bytes(ref)
        assert store.gets == 0, "scope failures are refused before any fetch"

    def test_tampered_or_unknown_form_is_invalid(self) -> None:
        """Tampered or unknown form is invalid."""
        store = MemoryStore()
        runtime = _runtime(store)
        encoded, _ = self._stored(store, runtime)
        tampered = bytearray(encoded)
        tampered[30] ^= 0x01
        for bad in (bytes(tampered), b"\x03" + encoded[1:], b"", b"\x01\x05abc"):
            with pytest.raises(InvalidStateRefError):
                _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(bad))

    def test_a_cursor_token_or_argument_ref_is_not_a_tag(self) -> None:
        """A cursor token or argument ref is not a tag."""
        from vgi_rpc.http.server._state_token import _compute_aad, _seal_cursor_token

        runtime = _runtime(MemoryStore())
        cursor = base64.b64decode(_seal_cursor_token(b"s", _CALL_ID, _KEY, _compute_aad(_ALICE, protocol="P"), 1))
        argref = mint_argument_ref(_KEY, _ALICE, size=3, sha256=hashlib.sha256(b"abc").hexdigest(), object_id="x")
        for envelope in (cursor, argref):
            with pytest.raises(InvalidStateRefError):
                _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(b"\x02" + envelope))

    def test_expired_tag_is_refused_before_any_get(self) -> None:
        """Expired tag is refused before any get."""
        store = MemoryStore()
        now = [1_000_000.0]
        runtime = _runtime(store, token_ttl=60, clock=lambda: now[0])
        encoded, _ = self._stored(store, runtime)
        now[0] += 61
        with pytest.raises(InvalidStateRefError, match="StateRef expired"):
            _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(encoded))
        assert store.gets == 0

    def test_zero_ttl_never_expires(self) -> None:
        """Zero ttl never expires."""
        store = MemoryStore()
        now = [1_000_000.0]
        runtime = _runtime(store, token_ttl=0, clock=lambda: now[0])
        encoded, value = self._stored(store, runtime)
        now[0] += 10**8
        assert _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(encoded)) == value

    def test_digest_and_size_mismatch_are_invalid(self) -> None:
        """Digest and size mismatch are invalid."""
        store = MemoryStore()
        runtime = _runtime(store)
        encoded, _ = self._stored(store, runtime)
        (object_id,) = store.objects
        original = store.objects[object_id]
        store.objects[object_id] = original[:-1] + bytes([original[-1] ^ 1])
        with pytest.raises(InvalidStateRefError, match="digest mismatch"):
            _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(encoded))
        store.objects[object_id] = original + b"x"
        with pytest.raises(InvalidStateRefError, match="size mismatch"):
            _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(encoded))

    def test_missing_and_transient_storage_errors(self) -> None:
        """Missing and transient storage errors."""
        store = MemoryStore()
        runtime = _runtime(store)
        encoded, _ = self._stored(store, runtime)
        store.fail_gets = True
        with pytest.raises(StorageUnavailableError) as transient:
            _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(encoded))
        assert transient.value.error_code == "UNAVAILABLE"
        store.fail_gets = False
        store.objects.clear()
        with pytest.raises(StateRefMissingError) as missing:
            _scope(runtime).resolve_bytes(StateRef.deserialize_from_bytes(encoded))
        assert missing.value.error_kind == "state_ref_missing" and missing.value.error_code == "NOT_FOUND"
        assert "restart the query" in str(missing.value)

    def test_too_large_at_store_and_at_resolve(self) -> None:
        """Too large at store and at resolve."""
        store = MemoryStore()
        with pytest.raises(StateRefTooLargeError):
            _encode(_runtime(store, max_fetch_bytes=20_000), StateRef._from_value(os.urandom(50_000)))
        assert store.puts == 0
        encoded = _encode(_runtime(store), StateRef._from_value(os.urandom(50_000)))
        with pytest.raises(StateRefTooLargeError):
            _scope(_runtime(store, max_fetch_bytes=20_000)).resolve_bytes(StateRef.deserialize_from_bytes(encoded))

    def test_stored_ref_needs_an_http_scope(self) -> None:
        """Stored ref needs an http scope."""
        store = MemoryStore()
        encoded = _encode(_runtime(store), StateRef._from_value(os.urandom(40_000)))
        with pytest.raises(InvalidStateRefError):
            StateRefScope(protocol="P").resolve_bytes(StateRef.deserialize_from_bytes(encoded))


@dataclass(frozen=True)
class _Record(ArrowSerializableDataclass):
    """A small record for typed resolve."""

    name: str
    count: int


class TestStateRefScope:
    """``state_ref()`` / ``resolve()`` through a scope: misuse, memo, typed resolve."""

    def test_state_ref_outside_init_is_misuse(self) -> None:
        """State ref outside init is misuse."""
        with pytest.raises(StateRefMisuseError) as raised:
            _scope(None).state_ref(b"x")
        assert raised.value.error_code == "FAILED_PRECONDITION"
        assert str(raised.value) == "state_ref() may only be called while handling a stream's init request (P.m)"

    def test_a_bare_call_context_refuses_state_ref(self) -> None:
        """A bare call context refuses state ref."""
        ctx = CallContext(auth=_ALICE, emit_client_log=lambda _m: None, protocol_name="P", method_name="m")
        with pytest.raises(StateRefMisuseError):
            ctx.state_ref(b"x")

    def test_memo_shares_refs_by_identity_and_by_bytes(self) -> None:
        """Memo shares refs by identity and by bytes."""
        scope = _scope(None, init=True)
        record = _Record(name="a", count=1)
        assert scope.state_ref(record) is scope.state_ref(record)
        assert scope.state_ref(b"same") is scope.state_ref(bytearray(b"same"))
        assert scope.state_ref(_Record(name="a", count=1)) is scope.state_ref(record)
        assert scope.state_ref(b"one") is not scope.state_ref(b"two")

    def test_memo_stores_one_object(self) -> None:
        """Memo stores one object."""
        store = MemoryStore()
        runtime = _runtime(store)
        scope = _scope(runtime, init=True)
        value = os.urandom(40_000)
        a, b = scope.state_ref(value), scope.state_ref(bytes(value))
        assert _encode(runtime, a) == _encode(runtime, b) and store.puts == 1

    def test_typed_resolve_returns_the_instance_in_memory_and_deserializes_otherwise(self) -> None:
        """Typed resolve returns the instance in memory and deserializes otherwise."""
        scope = _scope(None, init=True)
        record = _Record(name="r", count=3)
        ref = scope.state_ref(record)
        assert scope.resolve(ref, _Record) is record
        reread = StateRef.deserialize_from_bytes(ref.serialize_to_bytes())
        assert scope.resolve(reread, _Record) == record

    def test_state_ref_rejects_unserializable_values(self) -> None:
        """State ref rejects unserializable values."""
        with pytest.raises(TypeError):
            _scope(None, init=True).state_ref(12)


# ---------------------------------------------------------------------------
# AAD and argument refs
# ---------------------------------------------------------------------------


class TestAadAndArgumentRefs:
    """Sealing scopes (§4) and argument references (§10.2)."""

    def test_base_aad_matches_the_cursor_token_aad(self) -> None:
        """Base aad matches the cursor token aad."""
        from vgi_rpc.http.server._state_token import _compute_aad

        bound = AuthContext(domain="d", authenticated=True, principal="p", claims={"peer_evidence_binding": "abc"})
        for auth in (None, _ALICE, bound, AuthContext(domain=None, authenticated=False, principal=None)):
            assert base_aad(auth, "P") == _compute_aad(auth, protocol="P")

    def test_argument_ref_aad_is_principal_and_binding_scoped(self) -> None:
        """Argument ref aad is principal and binding scoped."""
        bound_a = AuthContext(domain="d", authenticated=True, principal="p", claims={"peer_evidence_binding": "A"})
        bound_b = AuthContext(domain="d", authenticated=True, principal="p", claims={"peer_evidence_binding": "B"})
        assert argument_ref_aad(bound_a) != argument_ref_aad(bound_b)
        assert argument_ref_aad(bound_a).startswith(b"vgi_rpc.argref.v1\x00vgi_rpc.state.v7\x00")
        ref = mint_argument_ref(_KEY, bound_a, size=1, sha256="a" * 64, object_id="o")
        assert open_argument_ref(_KEY, bound_a, ref).object_id == "o"
        with pytest.raises(InvalidArgumentsRefError):
            open_argument_ref(_KEY, bound_b, ref)

    def test_mint_open_round_trip_and_refusals(self) -> None:
        """Mint open round trip and refusals."""
        sha = hashlib.sha256(b"payload").hexdigest()
        ref = mint_argument_ref(_KEY, _ALICE, size=7, sha256=sha, object_id="obj")
        opened = open_argument_ref(_KEY, _ALICE, ref)
        assert (opened.size, opened.sha256, opened.object_id) == (7, sha, "obj")
        for auth in (_BOB, None):
            with pytest.raises(InvalidArgumentsRefError):
                open_argument_ref(_KEY, auth, ref)
        with pytest.raises(InvalidArgumentsRefError):
            open_argument_ref(b"\x08" * 32, _ALICE, ref)
        for bad in ({"size": 0}, {"sha256": "A" * 64}, {"object_id": ""}):
            kwargs: dict[str, Any] = {"size": 7, "sha256": sha, "object_id": "obj", **bad}
            with pytest.raises(ValueError):
                mint_argument_ref(_KEY, _ALICE, **kwargs)

    def _resolve(self, store: MemoryStore, ref: bytes, payload: bytes, **kwargs: Any) -> bytes:
        return resolve_argument_ref(
            ref,
            size=kwargs.pop("size", len(payload)),
            sha256=kwargs.pop("sha256", hashlib.sha256(payload).hexdigest()),
            token_key=_KEY,
            auth=kwargs.pop("auth", _ALICE),
            storage=kwargs.pop("storage", store),
            max_fetch_bytes=1 << 20,
            cache=kwargs.pop("cache", ObjectCache(1 << 20)),
        )

    def test_resolve_verifies_and_caches(self) -> None:
        """Resolve verifies and caches."""
        store = MemoryStore()
        payload = b"argument bytes"
        object_id = store.put_object(payload)
        ref = mint_argument_ref(
            _KEY, _ALICE, size=len(payload), sha256=hashlib.sha256(payload).hexdigest(), object_id=object_id
        )
        cache = ObjectCache(1 << 20)
        assert self._resolve(store, ref, payload, cache=cache) == payload
        assert self._resolve(store, ref, payload, cache=cache) == payload
        assert store.gets == 1

    def test_resolve_refusals(self) -> None:
        """Resolve refusals."""
        store = MemoryStore()
        payload = b"argument bytes"
        sha = hashlib.sha256(payload).hexdigest()
        object_id = store.put_object(b"different bytes")
        ref = mint_argument_ref(_KEY, _ALICE, size=len(payload), sha256=sha, object_id=object_id)
        with pytest.raises(InvalidArgumentsRefError):
            self._resolve(store, ref, payload, size=len(payload) + 1)
        with pytest.raises(InvalidArgumentsRefError):
            self._resolve(store, ref, payload, sha256="f" * 64)
        with pytest.raises(InvalidArgumentsRefError):
            self._resolve(store, ref, payload, sha256="not-hex")
        with pytest.raises(InvalidArgumentsRefError):
            self._resolve(store, ref, payload, storage=None)
        with pytest.raises(InvalidArgumentsRefError):
            self._resolve(store, ref, payload, auth=_BOB)
        with pytest.raises(ArgumentsRefIntegrityError) as integrity:
            self._resolve(store, ref, payload)
        assert integrity.value.error_code == "DATA_LOSS"
        store.objects.clear()
        with pytest.raises(ArgumentsRefMissingError):
            self._resolve(store, ref, payload)
        assert store.gets == 2, "every refusal before the fetch costs no get"

    def test_byte_stream_scope_cannot_resolve_argument_refs(self) -> None:
        """Byte stream scope cannot resolve argument refs."""
        with pytest.raises(InvalidArgumentsRefError):
            StateRefScope().resolve_argument_ref(b"x", size=1, sha256="a" * 64)


# ---------------------------------------------------------------------------
# Object cache
# ---------------------------------------------------------------------------


class TestObjectCache:
    """The object cache (§11)."""

    def test_single_flight(self) -> None:
        """Single flight."""
        cache = ObjectCache(1 << 20)
        calls = []
        gate = threading.Event()

        def fetch() -> bytes:
            """Block until released, counting calls."""
            calls.append(1)
            gate.wait(5)
            return b"value"

        results: list[bytes] = []
        threads = [
            threading.Thread(target=lambda: results.append(cache.get_or_fetch("id", fetch)[0])) for _ in range(8)
        ]
        for thread in threads:
            thread.start()
        gate.set()
        for thread in threads:
            thread.join(5)
        assert results == [b"value"] * 8 and len(calls) == 1
        assert cache.stats().fetches == 1

    def test_failed_fetch_is_shared_and_not_cached(self) -> None:
        """Failed fetch is shared and not cached."""
        cache = ObjectCache(1 << 20)

        def boom() -> bytes:
            """Fail."""
            raise ValueError("nope")

        with pytest.raises(ValueError):
            cache.get_or_fetch("id", boom)
        assert cache.get_or_fetch("id", lambda: b"ok") == (b"ok", False)

    def test_byte_bound_evicts_lru(self) -> None:
        """Byte bound evicts lru."""
        cache = ObjectCache(100)
        cache.get_or_fetch("a", lambda: b"a" * 60)
        cache.get_or_fetch("b", lambda: b"b" * 60)
        assert not cache.contains("a") and cache.contains("b")
        stats = cache.stats()
        assert stats.resident_bytes == 60 and stats.evictions == 1 and stats.entries == 1

    def test_oversized_value_is_returned_but_not_retained(self, caplog: pytest.LogCaptureFixture) -> None:
        """Oversized value is returned but not retained."""
        cache = ObjectCache(10)
        with caplog.at_level(logging.WARNING, logger="vgi_rpc.object_cache"):
            assert cache.get_or_fetch("big", lambda: b"x" * 50) == (b"x" * 50, False)
        assert not cache.contains("big") and "not retained" in caplog.text

    def test_entries_age_out_after_a_day(self) -> None:
        """Entries age out after a day."""
        now = [0.0]
        cache = ObjectCache(1 << 20, clock=lambda: now[0])
        cache.get_or_fetch("id", lambda: b"v1")
        now[0] += 86_000
        assert cache.get_or_fetch("id", lambda: b"v2") == (b"v1", True)
        now[0] += 500
        assert cache.get_or_fetch("id", lambda: b"v2") == (b"v2", False)
        assert cache.age_entries(86_401) == 1
        assert cache.get_or_fetch("id", lambda: b"v3") == (b"v3", False)

    def test_default_bound(self) -> None:
        """Default bound."""
        assert default_object_cache_bytes({"VGI_RPC_OBJECT_CACHE_BYTES": "12345"}) == 12345
        assert 0 < default_object_cache_bytes({}) <= 1 << 30
        for bad in ("-1", "lots"):
            with pytest.raises(ValueError):
                default_object_cache_bytes({"VGI_RPC_OBJECT_CACHE_BYTES": bad})


# ---------------------------------------------------------------------------
# Turn budget and the cursor rule
# ---------------------------------------------------------------------------


class TestTurnBudget:
    """The turn budget, inline limit, cursor rule and floor (§9)."""

    def test_formula(self) -> None:
        """``floor((max - 4096) / 1.34) - call_token_bytes``; unbounded without a cap."""
        assert turn_budget(None, 500) is None
        assert turn_budget(0, 500) is None
        assert turn_budget(32768, 0) == (32768 - 4096) * 100 // 134
        assert turn_budget(32768, 600) == (32768 - 4096) * 100 // 134 - 600

    def test_inline_bytes(self) -> None:
        """Inline bytes."""
        assert _runtime(None).inline_bytes(0) == 16384
        capped = _runtime(None, max_request_bytes=32768)
        assert capped.inline_bytes(600) == ((32768 - 4096) * 100 // 134 - 600) // 4
        assert _runtime(None, max_request_bytes=4000).inline_bytes(0) == 0
        assert _runtime(None, inline_bytes_cap=1000).inline_bytes(0) == 1000

    def test_env_knob(self) -> None:
        """Env knob."""
        assert state_ref_inline_bytes_from_env({}) == 16384
        assert state_ref_inline_bytes_from_env({"VGI_RPC_STATE_REF_INLINE_BYTES": "2048"}) == 2048
        for bad in ("0", "-5", "big"):
            with pytest.raises(ValueError):
                state_ref_inline_bytes_from_env({"VGI_RPC_STATE_REF_INLINE_BYTES": bad})
        with pytest.raises(ValueError):
            _runtime(None, inline_bytes_cap=0)
        with pytest.raises(ValueError):
            _runtime(None, cursor_rule="strict")

    def test_cursor_rule(self) -> None:
        """Cursor rule."""
        warn = _runtime(None, max_request_bytes=32768)
        assert warn.check_cursor(cursor_bytes=1000, call_token_bytes=300, protocol="P", method="m") is None
        message = warn.check_cursor(cursor_bytes=40_000, call_token_bytes=300, protocol="P", method="m")
        assert message is not None and message.startswith("cursor_large: stream state for P.m is 40000 bytes")
        error = _runtime(None, max_request_bytes=32768, cursor_rule="error")
        with pytest.raises(CursorTooLargeError, match=r"Wrap data that does not change across turns in ctx.state_ref"):
            error.check_cursor(cursor_bytes=40_000, call_token_bytes=300, protocol="P", method="m")
        unbounded = _runtime(None, cursor_rule="error")
        assert unbounded.check_cursor(cursor_bytes=10**9, call_token_bytes=0, protocol="P", method="m") is None

    def test_floor_warns_and_disables_the_rule(self, caplog: pytest.LogCaptureFixture) -> None:
        """Floor warns and disables the rule."""
        with caplog.at_level(logging.WARNING, logger="vgi_rpc.state_ref"):
            runtime = _runtime(None, max_request_bytes=9000, cursor_rule="error")
        assert (
            "max_request_bytes (9000) is below 16384: the cursor rule is disabled and stream continuations may not fit"
            in caplog.text
        )
        assert not runtime.cursor_rule_enabled
        assert runtime.check_cursor(cursor_bytes=10**6, call_token_bytes=0, protocol="P", method="m") is None


# ---------------------------------------------------------------------------
# End to end: pipe and in-process HTTP
# ---------------------------------------------------------------------------

_IN = pa.schema([pa.field("x", pa.int64())])
_OUT = pa.schema([pa.field("sha", pa.utf8())])


@dataclass
class _HoldState(ExchangeState):
    """Resolve the held ref on every exchange."""

    value: StateRef

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Emit the SHA-256 of the resolved value."""
        out.emit_pydict({"sha": [hashlib.sha256(ctx.resolve(self.value)).hexdigest()]})


@dataclass
class _WideState(ExchangeState):
    """Emit one all-null row of a very wide schema."""

    value: StateRef
    columns: int

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Resolve, then emit nulls."""
        assert ctx.resolve(self.value) == b"v"
        out.emit_pydict({f"column_with_a_long_name_{i}": [None] for i in range(self.columns)})


class _HoldService(Protocol):
    """A one-ref exchange service and a wide-schema variant."""

    def hold(self, size: int) -> Stream[ExchangeState]:
        """Hold a ref of the first ``size`` payload bytes."""
        ...

    def wide(self, columns: int) -> Stream[ExchangeState]:
        """Hold a one-byte ref under an output schema of ``columns`` columns."""
        ...


class _HoldImpl:
    """Implementation of :class:`_HoldService`."""

    def __init__(self, payload: bytes) -> None:
        """Hold the payload the stream wraps."""
        self.payload = payload

    def hold(self, size: int, ctx: CallContext) -> Stream[_HoldState]:
        """Wrap the first ``size`` payload bytes."""
        return Stream(output_schema=_OUT, state=_HoldState(value=ctx.state_ref(self.payload[:size])), input_schema=_IN)

    def wide(self, columns: int, ctx: CallContext) -> Stream[_WideState]:
        """Wrap one byte under a very wide output schema."""
        schema = pa.schema([pa.field(f"column_with_a_long_name_{i}", pa.utf8()) for i in range(columns)])
        return Stream(
            output_schema=schema, state=_WideState(value=ctx.state_ref(b"v"), columns=columns), input_schema=_IN
        )


class TestEndToEnd:
    """StateRef through real dispatch: pipe and in-process HTTP."""

    def test_pipe_keeps_values_in_memory(self) -> None:
        """Pipe keeps values in memory."""
        payload = os.urandom(100_000)
        with serve_pipe(_HoldService, _HoldImpl(payload)) as proxy, proxy.hold(size=100_000) as session:
            for _ in range(3):
                out = session.exchange(AnnotatedBatch.from_pydict({"x": [1]}, schema=_IN))
                assert out.batch.column("sha")[0].as_py() == hashlib.sha256(payload).hexdigest()

    def _client(self, store: MemoryStore, **kwargs: Any) -> Any:
        server = RpcServer(
            _HoldService, _HoldImpl(os.urandom(100_000)), external_location=ServerExternalConfig(storage=store)
        )
        return make_sync_client(server, token_key=_KEY, **kwargs)

    def test_http_stores_once_across_turns_and_cold_replicas(self) -> None:
        """Http stores once across turns and cold replicas."""
        store = MemoryStore()
        assert isinstance(store, ObjectStore)
        payload = os.urandom(100_000)
        server = RpcServer(_HoldService, _HoldImpl(payload), external_location=ServerExternalConfig(storage=store))
        client = make_sync_client(server, token_key=_KEY, call_state_cache_entries=0)
        with http_connect(_HoldService, client=client) as proxy, proxy.hold(size=100_000) as session:
            for _ in range(3):
                out = session.exchange(AnnotatedBatch.from_pydict({"x": [1]}, schema=_IN))
                assert out.batch.column("sha")[0].as_py() == hashlib.sha256(payload).hexdigest()
        assert store.puts == 1

    def test_wide_schemas_go_behind_a_stored_schemas_ref(self) -> None:
        """Wide schemas go behind a stored schemas ref."""
        store = MemoryStore()
        client = self._client(store, call_state_cache_entries=0, state_ref_inline_bytes=256)
        with http_connect(_HoldService, client=client) as proxy, proxy.wide(columns=3000) as session:
            out = session.exchange(AnnotatedBatch.from_pydict({"x": [1]}, schema=_IN))
            assert out.batch.num_rows == 1
        assert store.puts == 1, "the schemas_ref is stored once (the stream's own one-byte ref stays inline)"

    def test_cursor_too_large_is_an_rpc_error(self) -> None:
        """Cursor too large is an rpc error."""
        server = RpcServer(_HoldService, _HoldImpl(os.urandom(100_000)))
        client = make_sync_client(server, token_key=_KEY, max_request_bytes=32768, cursor_rule="error")
        with (
            pytest.raises(RpcError) as raised,
            http_connect(_HoldService, client=client) as proxy,
            proxy.hold(size=60_000) as session,
        ):
            session.exchange(AnnotatedBatch.from_pydict({"x": [1]}, schema=_IN))
        assert raised.value.error_kind == "cursor_too_large"
