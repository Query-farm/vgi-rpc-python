# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for pre-published external references (``ExternalRef`` / ``publish_external``)."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterator
from io import BytesIO
from typing import Protocol

import pyarrow as pa
import pytest
import zstandard
from pyarrow import ipc

from vgi_rpc import ExternalRef, publish_external
from vgi_rpc.conformance.fake_storage import FakeStorageBackend, serve_in_thread
from vgi_rpc.external import Compression, ExternalLocationConfig
from vgi_rpc.metadata import LOCATION_KEY, LOCATION_SHA256_KEY
from vgi_rpc.rpc import RpcError, RpcServer, rpc_methods, serve_pipe
from vgi_rpc.rpc._wire import _write_external_ref
from vgi_rpc.utils import new_ipc_stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _RecordingStorage:
    """In-memory ``ExternalStorage`` that records every upload."""

    def __init__(self) -> None:
        self.uploads: list[tuple[bytes, pa.Schema, str | None]] = []

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Record the upload and return a synthetic URL."""
        self.uploads.append((data, schema, content_encoding))
        return f"https://storage.invalid/obj/{len(self.uploads)}"


_RESULT_SCHEMA = pa.schema([pa.field("result", pa.utf8(), nullable=False)])


def _result_batch(value: str) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"result": [value]}, schema=_RESULT_SCHEMA)


class _RefService(Protocol):
    """Service whose methods may answer with a pre-published ref."""

    def catalog(self, version: int) -> str | ExternalRef:
        """Return the catalog for *version* (published once per version)."""
        ...

    def maybe(self, value: str | None) -> str | ExternalRef | None:
        """Return an optional result, possibly through a ref."""
        ...

    def plain(self, value: str) -> str:
        """Return *value*; declared ``-> str`` though the impl returns a ref."""
        ...


class _RefServiceImpl:
    def __init__(self, publish_storage: FakeStorageBackend) -> None:
        self._publish_storage = publish_storage
        self._refs: dict[int, ExternalRef] = {}
        self.publishes = 0

    def catalog(self, version: int) -> str | ExternalRef:
        ref = self._refs.get(version)
        if ref is None:
            schema = rpc_methods(_RefService)["catalog"].result_schema
            batch = pa.RecordBatch.from_pydict({"result": [f"catalog-v{version}"]}, schema=schema)
            ref = publish_external(batch, self._publish_storage)
            self._refs[version] = ref
            self.publishes += 1
        return ref

    def maybe(self, value: str | None) -> str | ExternalRef | None:
        if value is None:
            return None
        schema = rpc_methods(_RefService)["maybe"].result_schema
        return publish_external(pa.RecordBatch.from_pydict({"result": [value]}, schema=schema), self._publish_storage)

    def plain(self, value: str) -> str:
        schema = rpc_methods(_RefService)["plain"].result_schema
        return publish_external(  # type: ignore[return-value]  # ty: ignore[invalid-return-type]
            pa.RecordBatch.from_pydict({"result": [value]}, schema=schema),
            self._publish_storage,
            include_sha256=False,
        )


@pytest.fixture(scope="module")
def fake_storage_url() -> Iterator[str]:
    """Run the fake storage service for the module."""
    base_url, shutdown = serve_in_thread()
    try:
        yield base_url
    finally:
        shutdown()


def _object_count(base_url: str) -> int:
    import httpx2

    return int(httpx2.get(f"{base_url}/_stats", timeout=5.0).json()["object_count"])


# ---------------------------------------------------------------------------
# ExternalRef value
# ---------------------------------------------------------------------------


class TestExternalRefValue:
    """Construction-time validation."""

    def test_valid(self) -> None:
        """A URL with or without a lowercase hex digest is accepted."""
        assert ExternalRef("https://x/y").sha256 is None
        digest = hashlib.sha256(b"x").hexdigest()
        assert ExternalRef("https://x/y", digest).sha256 == digest

    @pytest.mark.parametrize("sha", ["", "abc", "A" * 64, "g" * 64])
    def test_bad_digest_rejected(self, sha: str) -> None:
        """Anything but 64 lowercase hex characters is refused."""
        with pytest.raises(ValueError, match="sha256"):
            ExternalRef("https://x/y", sha)

    def test_empty_url_rejected(self) -> None:
        """An empty URL is refused."""
        with pytest.raises(ValueError, match="url"):
            ExternalRef("")

    def test_pointer_batch_omits_digest_when_none(self) -> None:
        """No digest on the ref means no ``vgi_rpc.location.sha256`` key on the pointer."""
        batch, cm = ExternalRef("https://x/y").pointer_batch(_RESULT_SCHEMA)
        assert batch.num_rows == 0
        assert batch.schema == _RESULT_SCHEMA
        assert cm.get(LOCATION_KEY) == b"https://x/y"
        assert cm.get(LOCATION_SHA256_KEY) is None


# ---------------------------------------------------------------------------
# publish_external
# ---------------------------------------------------------------------------


class TestPublishExternal:
    """``publish_external`` produces exactly what the per-call externalizer would."""

    def test_uploads_once_and_hashes_raw_stream(self) -> None:
        """One upload of a schema + 1-row IPC stream; the digest covers those bytes."""
        storage = _RecordingStorage()
        ref = publish_external(_result_batch("hello"), storage)
        assert len(storage.uploads) == 1
        data, schema, encoding = storage.uploads[0]
        assert encoding is None
        assert schema == _RESULT_SCHEMA
        assert ref.url == "https://storage.invalid/obj/1"
        assert ref.sha256 == hashlib.sha256(data).hexdigest()
        batches = list(ipc.open_stream(BytesIO(data)))
        assert len(batches) == 1
        assert batches[0].to_pydict() == {"result": ["hello"]}

    def test_matches_maybe_externalize_batch_bytes(self) -> None:
        """The published object is byte-identical to the threshold externalizer's upload."""
        from vgi_rpc.external import maybe_externalize_batch

        published = _RecordingStorage()
        externalized = _RecordingStorage()
        publish_external(_result_batch("same"), published)
        maybe_externalize_batch(
            _result_batch("same"),
            None,
            ExternalLocationConfig(storage=externalized, externalize_threshold_bytes=0),
        )
        assert published.uploads[0][0] == externalized.uploads[0][0]

    def test_without_digest(self) -> None:
        """``include_sha256=False`` yields a ref with no digest."""
        assert publish_external(_result_batch("x"), _RecordingStorage(), include_sha256=False).sha256 is None

    def test_compression(self) -> None:
        """Compression is applied before upload; the digest is of the raw bytes."""
        storage = _RecordingStorage()
        ref = publish_external(_result_batch("compress me" * 50), storage, Compression())
        data, _schema, encoding = storage.uploads[0]
        assert encoding == "zstd"
        raw = zstandard.ZstdDecompressor().decompress(data)
        assert ref.sha256 == hashlib.sha256(raw).hexdigest()
        assert next(iter(ipc.open_stream(BytesIO(raw)))).to_pydict() == {"result": ["compress me" * 50]}

    @pytest.mark.parametrize("rows", [0, 2])
    def test_rejects_non_single_row_batch(self, rows: int) -> None:
        """Only a 1-row result batch can be published."""
        batch = pa.RecordBatch.from_pydict({"result": ["v"] * rows}, schema=_RESULT_SCHEMA)
        with pytest.raises(ValueError, match="1-row"):
            publish_external(batch, _RecordingStorage())


# ---------------------------------------------------------------------------
# Return-annotation stripping
# ---------------------------------------------------------------------------


class TestReturnAnnotation:
    """``X | ExternalRef`` derives the schema of ``X``."""

    def test_union_strips_ref(self) -> None:
        """``-> str | ExternalRef`` is a non-nullable utf8 result."""
        info = rpc_methods(_RefService)["catalog"]
        assert info.result_schema == _RESULT_SCHEMA
        assert info.result_type is str
        assert info.has_return

    def test_optional_union_strips_ref(self) -> None:
        """``-> str | None | ExternalRef`` stays nullable."""
        info = rpc_methods(_RefService)["maybe"]
        assert info.result_schema.field("result").nullable
        assert info.result_schema.field("result").type == pa.utf8()

    def test_bare_ref_rejected(self) -> None:
        """A bare ``-> ExternalRef`` has no result type to derive."""

        class _Bad(Protocol):
            def m(self) -> ExternalRef: ...

        with pytest.raises(TypeError, match="ExternalRef"):
            rpc_methods(_Bad)

    def test_protocol_hash_ignores_ref(self) -> None:
        """Adding ``| ExternalRef`` does not change the wire contract."""
        from vgi_rpc.rpc._protocol_hash import compute_protocol_hash

        class _A(Protocol):
            def catalog(self, version: int) -> str: ...

        class _B(Protocol):
            def catalog(self, version: int) -> str | ExternalRef: ...

        assert compute_protocol_hash("P", rpc_methods(_A)) == compute_protocol_hash("P", rpc_methods(_B))


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


class TestWriteExternalRef:
    """The wire helper writes a pointer and logs the ``external_ref`` route."""

    def test_writes_pointer(self, caplog: pytest.LogCaptureFixture) -> None:
        """One zero-row pointer batch with the ref's URL and digest."""
        digest = hashlib.sha256(b"x").hexdigest()
        buf = BytesIO()
        with (
            caplog.at_level(logging.DEBUG, logger="vgi_rpc.wire.response"),
            new_ipc_stream(buf, _RESULT_SCHEMA) as writer,
        ):
            _write_external_ref(writer, _RESULT_SCHEMA, ExternalRef("https://x/y", digest))
        batch, cm = ipc.open_stream(BytesIO(buf.getvalue())).read_next_batch_with_custom_metadata()
        assert batch.num_rows == 0
        assert cm.get(LOCATION_KEY) == b"https://x/y"
        assert cm.get(LOCATION_SHA256_KEY) == digest.encode()
        assert "route=external_ref" in caplog.text


class TestPipeDispatch:
    """A ref returned over the pipe transport resolves on the client."""

    def test_ref_without_server_storage(self, fake_storage_url: str) -> None:
        """No server storage, default threshold: the ref still goes as a pointer and is published once."""
        publish = FakeStorageBackend(fake_storage_url)
        impl = _RefServiceImpl(publish)
        config = ExternalLocationConfig(url_validator=None)
        try:
            before = _object_count(fake_storage_url)
            with serve_pipe(_RefService, impl, external_location=config) as proxy:
                assert proxy.catalog(version=1) == "catalog-v1"
                assert proxy.catalog(version=1) == "catalog-v1"
                assert proxy.catalog(version=2) == "catalog-v2"
                assert proxy.maybe(value="m") == "m"
                assert proxy.maybe(value=None) is None
                assert proxy.plain(value="p") == "p"
            assert impl.publishes == 2
            # catalog v1 + v2, one maybe, one plain -- and nothing from the server's own externalizer.
            assert _object_count(fake_storage_url) - before == 4
        finally:
            config.fetch_config.close()

    def test_ref_without_client_config_fails_cleanly(self, fake_storage_url: str) -> None:
        """A client that cannot resolve pointers reports an error rather than a wrong value."""
        impl = _RefServiceImpl(FakeStorageBackend(fake_storage_url))
        with serve_pipe(_RefService, impl) as proxy:
            with pytest.raises((RpcError, RuntimeError, TypeError, ValueError, IndexError, pa.ArrowInvalid)):
                proxy.catalog(version=3)
            # The transport stays usable for an ordinary call afterwards.
            assert proxy.maybe(value=None) is None


class TestHttpDispatch:
    """A ref returned over HTTP bypasses the external-channel cap and resolves."""

    def test_ref_bypasses_externalized_cap(self, fake_storage_url: str) -> None:
        """A 1-byte ``max_externalized_response_bytes`` does not block a ref."""
        from vgi_rpc.http import http_connect, make_sync_client

        publish = FakeStorageBackend(fake_storage_url)
        impl = _RefServiceImpl(publish)
        server = RpcServer(
            _RefService,
            impl,
            external_location=ExternalLocationConfig(
                storage=publish, externalize_threshold_bytes=1, url_validator=None
            ),
        )
        client = make_sync_client(server, max_externalized_response_bytes=1)
        config = ExternalLocationConfig(url_validator=None)
        try:
            with http_connect(_RefService, client=client, external_location=config) as proxy:
                assert proxy.catalog(version=1) == "catalog-v1"
                assert proxy.catalog(version=1) == "catalog-v1"
                assert proxy.plain(value="no digest") == "no digest"
        finally:
            config.fetch_config.close()
        assert impl.publishes == 1
