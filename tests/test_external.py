"""Tests for ExternalLocation batch support."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Protocol
from unittest.mock import MagicMock, patch

import aiohttp
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import zstandard
from aioresponses import CallbackResult
from aioresponses import aioresponses as aioresponses_ctx
from pyarrow import ipc

from vgi_rpc.external import (
    Compression,
    ExternalLocationConfig,
    ExternalStorage,
    is_external_location_batch,
    make_external_location_batch,
    maybe_externalize_batch,
    maybe_externalize_collector,
    resolve_external_location,
)
from vgi_rpc.external_fetch import FetchConfig
from vgi_rpc.log import Level, Message
from vgi_rpc.metadata import (
    LOCATION_FETCH_MS_KEY,
    LOCATION_KEY,
    LOCATION_SOURCE_KEY,
    LOG_LEVEL_KEY,
    encode_metadata,
)
from vgi_rpc.rpc import (
    AnnotatedBatch,
    BidiStream,
    BidiStreamState,
    CallContext,
    OutputCollector,
    ServerStream,
    ServerStreamState,
    serve_pipe,
)
from vgi_rpc.utils import empty_batch

# ---------------------------------------------------------------------------
# MockStorage — in-memory ExternalStorage
# ---------------------------------------------------------------------------

_SCHEMA = pa.schema([pa.field("value", pa.int64())])


class MockStorage(ExternalStorage):
    """In-memory ExternalStorage for testing.

    Uses ``https://`` URLs so aioresponses can intercept them.
    """

    def __init__(self) -> None:
        """Initialize with empty data store."""
        self.data: dict[str, bytes] = {}
        self._counter = 0
        self.last_content_encoding: str | None = None

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Store data and return a mock URL."""
        self._counter += 1
        self.last_content_encoding = content_encoding
        url = f"https://mock.storage/{self._counter}"
        self.data[url] = data
        return url


@contextmanager
def _mock_aio(storage: MockStorage, *, content_encoding: str | None = None) -> Iterator[aioresponses_ctx]:
    """Context manager that registers all MockStorage URLs in aioresponses."""
    enc = content_encoding or storage.last_content_encoding
    with aioresponses_ctx() as mock:
        for url, body in storage.data.items():
            head_headers: dict[str, str] = {"Content-Length": str(len(body))}
            get_headers: dict[str, str] = {"Content-Length": str(len(body))}
            if enc:
                head_headers["Content-Encoding"] = enc
                get_headers["Content-Encoding"] = enc
            mock.head(url, headers=head_headers)
            mock.get(url, body=body, headers=get_headers)
        yield mock


# ---------------------------------------------------------------------------
# Helper to serialize batches into IPC bytes
# ---------------------------------------------------------------------------


def _serialize_ipc(
    schema: pa.Schema,
    batches: list[tuple[pa.RecordBatch, pa.KeyValueMetadata | None]],
) -> bytes:
    """Serialize a list of (batch, custom_metadata) into IPC stream bytes."""
    buf = BytesIO()
    with ipc.new_stream(buf, schema) as writer:
        for batch, cm in batches:
            if cm is not None:
                writer.write_batch(batch, custom_metadata=cm)
            else:
                writer.write_batch(batch)
    return buf.getvalue()


# ===========================================================================
# Unit tests — detection
# ===========================================================================


class TestIsExternalLocation:
    """Tests for is_external_location_batch detection."""

    def test_positive(self) -> None:
        """Zero-row batch with LOCATION_KEY is detected."""
        batch, cm = make_external_location_batch(_SCHEMA, "mock://test")
        assert is_external_location_batch(batch, cm) is True

    def test_non_zero_rows(self) -> None:
        """Batches with rows are not pointers."""
        batch = pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA)
        cm = pa.KeyValueMetadata({LOCATION_KEY: b"mock://test"})
        assert is_external_location_batch(batch, cm) is False

    def test_no_metadata(self) -> None:
        """No metadata means not a pointer."""
        batch = empty_batch(_SCHEMA)
        assert is_external_location_batch(batch, None) is False

    def test_log_batch(self) -> None:
        """Log batches are not pointers even with LOCATION_KEY."""
        batch = empty_batch(_SCHEMA)
        cm = pa.KeyValueMetadata(
            {
                LOCATION_KEY: b"mock://test",
                LOG_LEVEL_KEY: b"INFO",
            }
        )
        assert is_external_location_batch(batch, cm) is False

    def test_no_location_key(self) -> None:
        """Zero-row batch without LOCATION_KEY is not a pointer."""
        batch = empty_batch(_SCHEMA)
        cm = pa.KeyValueMetadata({b"other": b"value"})
        assert is_external_location_batch(batch, cm) is False


# ===========================================================================
# Unit tests — creation
# ===========================================================================


class TestMakeExternalLocationBatch:
    """Tests for make_external_location_batch."""

    def test_produces_zero_row_batch(self) -> None:
        """Created batch is zero-row with correct schema."""
        batch, _cm = make_external_location_batch(_SCHEMA, "mock://test")
        assert batch.num_rows == 0
        assert batch.schema == _SCHEMA

    def test_has_location_metadata(self) -> None:
        """Created batch has LOCATION_KEY in metadata."""
        _batch, cm = make_external_location_batch(_SCHEMA, "mock://test")
        assert cm.get(LOCATION_KEY) == b"mock://test"

    def test_is_detected(self) -> None:
        """Created batch is detected by is_external_location_batch."""
        batch, cm = make_external_location_batch(_SCHEMA, "mock://test")
        assert is_external_location_batch(batch, cm) is True


# ===========================================================================
# Unit tests — resolution
# ===========================================================================


class TestResolveExternalLocation:
    """Tests for resolve_external_location."""

    def test_basic_resolution(self) -> None:
        """Fetch a stream with one data batch."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage)

        data_batch = pa.RecordBatch.from_pydict({"value": [42]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        url = "https://mock.storage/basic"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with _mock_aio(storage):
            resolved, _resolved_cm = resolve_external_location(pointer, cm, config)

        assert resolved.num_rows == 1
        assert resolved.column("value")[0].as_py() == 42

    def test_with_logs(self) -> None:
        """Fetch a stream with log batches + data batch; logs dispatched via on_log."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage)

        log_cm = encode_metadata({"vgi_rpc.log_level": "INFO", "vgi_rpc.log_message": "hello"})
        log_batch = empty_batch(_SCHEMA)
        data_batch = pa.RecordBatch.from_pydict({"value": [99]}, schema=_SCHEMA)

        ipc_bytes = _serialize_ipc(_SCHEMA, [(log_batch, log_cm), (data_batch, None)])
        url = "https://mock.storage/logs"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)
        received_logs: list[Message] = []

        with _mock_aio(storage):
            resolved, _ = resolve_external_location(pointer, cm, config, on_log=received_logs.append)

        assert resolved.column("value")[0].as_py() == 99
        assert len(received_logs) == 1
        assert received_logs[0].level == Level.INFO

    def test_no_on_log_callback(self) -> None:
        """Logs in fetched stream are silently discarded if on_log is None."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage)

        log_cm = encode_metadata({"vgi_rpc.log_level": "INFO", "vgi_rpc.log_message": "hello"})
        log_batch = empty_batch(_SCHEMA)
        data_batch = pa.RecordBatch.from_pydict({"value": [77]}, schema=_SCHEMA)

        ipc_bytes = _serialize_ipc(_SCHEMA, [(log_batch, log_cm), (data_batch, None)])
        url = "https://mock.storage/nolog"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with _mock_aio(storage):
            resolved, _ = resolve_external_location(pointer, cm, config, on_log=None)

        assert resolved.column("value")[0].as_py() == 77

    def test_redirect_loop(self) -> None:
        """Fetched batch with LOCATION_KEY raises RuntimeError."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, max_retries=0)

        inner_batch = empty_batch(_SCHEMA)
        inner_cm = pa.KeyValueMetadata({LOCATION_KEY: b"https://mock.storage/loop"})
        ipc_bytes = _serialize_ipc(_SCHEMA, [(inner_batch, inner_cm)])
        url = "https://mock.storage/loop"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with (
            _mock_aio(storage),
            pytest.raises(RuntimeError, match="Redirect loop"),
        ):
            resolve_external_location(pointer, cm, config)

    def test_schema_mismatch(self) -> None:
        """Schema mismatch raises ValueError."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, max_retries=0)

        other_schema = pa.schema([pa.field("other", pa.string())])
        data_batch = pa.RecordBatch.from_pydict({"other": ["x"]}, schema=other_schema)
        ipc_bytes = _serialize_ipc(other_schema, [(data_batch, None)])
        url = "https://mock.storage/mismatch"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with (
            _mock_aio(storage),
            pytest.raises(ValueError, match="Schema mismatch"),
        ):
            resolve_external_location(pointer, cm, config)

    def test_multiple_data_batches(self) -> None:
        """Multiple data batches in fetched stream raises RuntimeError."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, max_retries=0)

        b1 = pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA)
        b2 = pa.RecordBatch.from_pydict({"value": [2]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(b1, None), (b2, None)])
        url = "https://mock.storage/multi"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with (
            _mock_aio(storage),
            pytest.raises(RuntimeError, match="Multiple data batches"),
        ):
            resolve_external_location(pointer, cm, config)

    def test_empty_stream(self) -> None:
        """Empty IPC stream (no batches) raises RuntimeError."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, max_retries=0)

        ipc_bytes = _serialize_ipc(_SCHEMA, [])
        url = "https://mock.storage/empty"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with (
            _mock_aio(storage),
            pytest.raises(RuntimeError, match="No data batch"),
        ):
            resolve_external_location(pointer, cm, config)

    def test_retry_success(self) -> None:
        """First attempt fails, second succeeds."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, max_retries=1, retry_delay_seconds=0.0)

        data_batch = pa.RecordBatch.from_pydict({"value": [55]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        url = "https://mock.storage/retry"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with aioresponses_ctx() as mock:
            # First attempt: HEAD ok, GET fails; second attempt: HEAD ok, GET succeeds
            mock.head(url, headers={"Content-Length": str(len(ipc_bytes))})
            mock.get(url, exception=aiohttp.ClientConnectionError("transient failure"))
            mock.head(url, headers={"Content-Length": str(len(ipc_bytes))})
            mock.get(url, body=ipc_bytes, headers={"Content-Length": str(len(ipc_bytes))})

            resolved, _ = resolve_external_location(pointer, cm, config)

        assert resolved.column("value")[0].as_py() == 55

    def test_all_retries_fail(self) -> None:
        """All retries exhausted raises RuntimeError."""
        config = ExternalLocationConfig(max_retries=1, retry_delay_seconds=0.0)

        url = "https://mock.storage/fail"
        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with (
            aioresponses_ctx() as mock,
            pytest.raises(RuntimeError, match="Failed to resolve"),
        ):
            # Both attempts: HEAD fails with connection error
            mock.head(url, exception=aiohttp.ClientConnectionError("permanent failure"))
            mock.head(url, exception=aiohttp.ClientConnectionError("permanent failure"))
            resolve_external_location(pointer, cm, config)

    def test_fetch_metadata(self) -> None:
        """Resolved batch has fetch_ms and source metadata."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage)

        data_batch = pa.RecordBatch.from_pydict({"value": [10]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        url = "https://mock.storage/meta"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with _mock_aio(storage):
            _, resolved_cm = resolve_external_location(pointer, cm, config)

        assert resolved_cm is not None
        fetch_ms_bytes = resolved_cm.get(LOCATION_FETCH_MS_KEY)
        assert fetch_ms_bytes is not None
        fetch_ms = float(fetch_ms_bytes.decode() if isinstance(fetch_ms_bytes, bytes) else fetch_ms_bytes)
        assert fetch_ms >= 0
        source = resolved_cm.get(LOCATION_SOURCE_KEY)
        assert source is not None
        source_str = source.decode() if isinstance(source, bytes) else source
        assert source_str == url

    def test_max_fetch_bytes_exceeded(self) -> None:
        """Response larger than max_fetch_bytes raises RuntimeError."""
        storage = MockStorage()
        fetch_cfg = FetchConfig(max_fetch_bytes=100)
        config = ExternalLocationConfig(storage=storage, max_retries=0, fetch_config=fetch_cfg)

        data_batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        assert len(ipc_bytes) > 100  # sanity check
        url = "https://mock.storage/big"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with (
            _mock_aio(storage),
            pytest.raises(RuntimeError, match="max_fetch_bytes"),
        ):
            resolve_external_location(pointer, cm, config)

    def test_max_fetch_bytes_at_limit(self) -> None:
        """Response exactly at max_fetch_bytes succeeds."""
        storage = MockStorage()

        data_batch = pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        fetch_cfg = FetchConfig(max_fetch_bytes=len(ipc_bytes))
        config = ExternalLocationConfig(storage=storage, max_retries=0, fetch_config=fetch_cfg)

        url = "https://mock.storage/exact"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with _mock_aio(storage):
            resolved, _ = resolve_external_location(pointer, cm, config)

        assert resolved.num_rows == 1

    def test_defaults_allow_normal_batches(self) -> None:
        """Default 256 MB limits do not interfere with normal data."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, max_retries=0)

        data_batch = pa.RecordBatch.from_pydict({"value": list(range(1000))}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        url = "https://mock.storage/normal"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with _mock_aio(storage):
            resolved, _ = resolve_external_location(pointer, cm, config)

        assert resolved.num_rows == 1000


# ===========================================================================
# Unit tests — production (collector)
# ===========================================================================


class TestMaybeExternalizeCollector:
    """Tests for maybe_externalize_collector."""

    def test_above_threshold(self) -> None:
        """Data batch above threshold is externalized."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)

        out = OutputCollector(_SCHEMA)
        out.emit_pydict({"value": list(range(100))})

        result = maybe_externalize_collector(out, config)
        assert len(result) == 1
        batch, cm = result[0]
        assert batch.num_rows == 0
        assert cm is not None
        assert cm.get(LOCATION_KEY) is not None
        assert len(storage.data) == 1

    def test_below_threshold(self) -> None:
        """Data batch below threshold is not externalized."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10_000_000)

        out = OutputCollector(_SCHEMA)
        out.emit_pydict({"value": [1]})

        result = maybe_externalize_collector(out, config)
        assert len(result) == 1
        batch, _ = result[0]
        assert batch.num_rows == 1

    def test_no_storage(self) -> None:
        """No storage configured passes through."""
        config = ExternalLocationConfig(storage=None)

        out = OutputCollector(_SCHEMA)
        out.emit_pydict({"value": list(range(100))})

        result = maybe_externalize_collector(out, config)
        assert len(result) == 1
        batch, _ = result[0]
        assert batch.num_rows == 100

    def test_no_data_batch(self) -> None:
        """Finished-only collector passes through."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)

        out = OutputCollector(_SCHEMA)
        out.finish()

        result = maybe_externalize_collector(out, config)
        assert len(result) == 0
        assert len(storage.data) == 0

    def test_log_only_collector(self) -> None:
        """Log-only collector with no data batch passes through."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)

        out = OutputCollector(_SCHEMA)
        out.client_log(Level.INFO, "hello")
        # Don't emit a data batch — this is a log-only collector that also finishes
        out.finish()

        result = maybe_externalize_collector(out, config)
        assert len(result) == 1
        batch, cm = result[0]
        assert batch.num_rows == 0
        assert cm is not None
        assert cm.get(LOG_LEVEL_KEY) is not None
        assert len(storage.data) == 0

    def test_upload_failure_propagates(self) -> None:
        """Upload failure raises."""

        class FailStorage:
            """Storage that always fails."""

            def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
                """Raise OSError unconditionally."""
                raise OSError("storage down")

        config = ExternalLocationConfig(storage=FailStorage(), externalize_threshold_bytes=10)

        out = OutputCollector(_SCHEMA)
        out.emit_pydict({"value": list(range(100))})

        with pytest.raises(OSError, match="storage down"):
            maybe_externalize_collector(out, config)

    def test_externalized_includes_logs(self) -> None:
        """When externalized, the IPC stream includes log batches + data batch."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)

        out = OutputCollector(_SCHEMA)
        out.client_log(Level.INFO, "pre-data log")
        out.emit_pydict({"value": list(range(100))})

        result = maybe_externalize_collector(out, config)
        assert len(result) == 1
        assert len(storage.data) == 1

        # Read back the uploaded IPC stream
        uploaded_bytes = next(iter(storage.data.values()))
        reader = ipc.open_stream(BytesIO(uploaded_bytes))
        fetched_batches = []
        while True:
            try:
                b, cm = reader.read_next_batch_with_custom_metadata()
                fetched_batches.append((b, cm))
            except StopIteration:
                break

        # Should have 2 batches: log + data
        assert len(fetched_batches) == 2
        log_b, log_cm = fetched_batches[0]
        assert log_b.num_rows == 0
        assert log_cm is not None
        assert log_cm.get(LOG_LEVEL_KEY) is not None
        data_b, _ = fetched_batches[1]
        assert data_b.num_rows == 100


# ===========================================================================
# Unit tests — production (single batch)
# ===========================================================================


class TestMaybeExternalizeBatch:
    """Tests for maybe_externalize_batch."""

    def test_above_threshold(self) -> None:
        """Above threshold is externalized."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)

        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        result_batch, result_cm = maybe_externalize_batch(batch, None, config)

        assert result_batch.num_rows == 0
        assert result_cm is not None
        assert result_cm.get(LOCATION_KEY) is not None

    def test_below_threshold(self) -> None:
        """Below threshold passes through."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10_000_000)

        batch = pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA)
        result_batch, result_cm = maybe_externalize_batch(batch, None, config)

        assert result_batch.num_rows == 1
        assert result_cm is None

    def test_no_storage(self) -> None:
        """No storage passes through."""
        config = ExternalLocationConfig(storage=None)

        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        result_batch, _ = maybe_externalize_batch(batch, None, config)

        assert result_batch.num_rows == 100

    def test_zero_row_passthrough(self) -> None:
        """Zero-row batches are never externalized."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=0)

        batch = empty_batch(_SCHEMA)
        result_batch, _ = maybe_externalize_batch(batch, None, config)

        assert result_batch.num_rows == 0
        assert len(storage.data) == 0

    def test_preserves_existing_metadata(self) -> None:
        """When externalized, the original batch metadata is included in the IPC stream."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)

        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        user_cm = pa.KeyValueMetadata({b"user.key": b"user.value"})
        result_batch, result_cm = maybe_externalize_batch(batch, user_cm, config)

        assert result_batch.num_rows == 0
        assert result_cm is not None

        # Read back the uploaded IPC stream and check user metadata is present
        uploaded_bytes = next(iter(storage.data.values()))
        reader = ipc.open_stream(BytesIO(uploaded_bytes))
        _b, cm = reader.read_next_batch_with_custom_metadata()
        assert cm is not None
        assert cm.get(b"user.key") == b"user.value"

    def test_metadata_survives_roundtrip(self) -> None:
        """User metadata on data batches survives externalization round-trip."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10, max_retries=0)

        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        user_cm = pa.KeyValueMetadata({b"user.key": b"user.value"})

        ext_batch, ext_cm = maybe_externalize_batch(batch, user_cm, config)
        assert ext_batch.num_rows == 0
        assert ext_cm is not None

        with _mock_aio(storage):
            resolved, resolved_cm = resolve_external_location(ext_batch, ext_cm, config)

        assert resolved.num_rows == 100
        assert resolved_cm is not None
        assert resolved_cm.get(b"user.key") == b"user.value"


# ===========================================================================
# Integration tests — pipe transport with MockStorage
# ===========================================================================


@dataclass
class _LargeStreamState(ServerStreamState):
    """Server stream that produces large batches with logs."""

    count: int
    size: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Produce a large batch with a log message."""
        if self.current >= self.count:
            out.finish()
            return
        out.client_log(Level.INFO, f"producing batch {self.current}")
        out.emit_pydict({"value": list(range(self.size))})
        self.current += 1


@dataclass
class _LargeBidiState(BidiStreamState):
    """Bidi stream that produces large output."""

    factor: float

    def process(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Process input and produce large output."""
        scaled = pc.multiply(input.batch.column("value"), self.factor)
        out.emit_arrays([scaled])


class _ExternalService(Protocol):
    """Protocol for ExternalLocation integration tests."""

    def echo_large(self, data: str) -> str:
        """Return the data back (may be externalized if large)."""
        ...

    def stream_large(self, count: int, size: int) -> ServerStream[ServerStreamState]:
        """Stream large batches."""
        ...

    def bidi_large(self, factor: float) -> BidiStream[BidiStreamState]:
        """Bidi with potentially large input/output."""
        ...


class _ExternalServiceImpl:
    """Implementation for ExternalLocation integration tests."""

    def echo_large(self, data: str) -> str:
        """Return the data back."""
        return data

    def stream_large(self, count: int, size: int) -> ServerStream[_LargeStreamState]:
        """Stream large batches."""
        schema = pa.schema([pa.field("value", pa.int64())])
        return ServerStream(output_schema=schema, state=_LargeStreamState(count=count, size=size))

    def bidi_large(self, factor: float) -> BidiStream[_LargeBidiState]:
        """Bidi with potentially large input/output."""
        schema = pa.schema([pa.field("value", pa.float64())])
        return BidiStream(output_schema=schema, state=_LargeBidiState(factor=factor))


def _mock_aio_dynamic(storage: MockStorage, mock: aioresponses_ctx, *, content_encoding: str | None = None) -> None:
    """Register pattern-based HEAD + GET callbacks that serve from MockStorage dynamically."""
    pattern = re.compile(r"^https://mock\.storage/.*$")
    enc = content_encoding or storage.last_content_encoding

    def _head_callback(url_: Any, **kwargs: Any) -> CallbackResult:
        url_str = str(url_)
        if url_str not in storage.data:
            return CallbackResult(status=404)
        body = storage.data[url_str]
        headers: dict[str, str] = {"Content-Length": str(len(body))}
        if enc:
            headers["Content-Encoding"] = enc
        return CallbackResult(status=200, headers=headers)

    def _get_callback(url_: Any, **kwargs: Any) -> CallbackResult:
        url_str = str(url_)
        if url_str not in storage.data:
            return CallbackResult(status=404)
        body = storage.data[url_str]
        headers: dict[str, str] = {"Content-Length": str(len(body))}
        if enc:
            headers["Content-Encoding"] = enc
        return CallbackResult(status=200, body=body, headers=headers)

    # Register enough pattern matches for all expected requests
    for _ in range(50):
        mock.head(pattern, callback=_head_callback)
        mock.get(pattern, callback=_get_callback)


class TestPipeIntegration:
    """Integration tests using pipe transport with mocked external storage."""

    def _make_config(self, storage: MockStorage, threshold: int = 100) -> ExternalLocationConfig:
        """Create an ExternalLocationConfig with low threshold for testing."""
        return ExternalLocationConfig(
            storage=storage,
            externalize_threshold_bytes=threshold,
            max_retries=0,
            retry_delay_seconds=0.0,
        )

    def test_unary_large_externalized(self) -> None:
        """Large unary result is externalized and resolved transparently."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=10)

        with aioresponses_ctx() as mock:
            _mock_aio_dynamic(storage, mock)
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                external_location=config,
            ) as svc:
                result = svc.echo_large(data="x" * 200)

        assert result == "x" * 200
        assert len(storage.data) >= 1

    def test_unary_small_inline(self) -> None:
        """Small unary result stays inline."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=10_000_000)

        with serve_pipe(
            _ExternalService,
            _ExternalServiceImpl(),
            external_location=config,
        ) as svc:
            result = svc.echo_large(data="hello")

        assert result == "hello"
        assert len(storage.data) == 0

    def test_server_stream_large_externalized(self) -> None:
        """Large server-stream batches are externalized with logs."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)

        received_logs: list[Message] = []

        with aioresponses_ctx() as mock:
            _mock_aio_dynamic(storage, mock)
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                on_log=received_logs.append,
                external_location=config,
            ) as svc:
                batches = list(svc.stream_large(count=3, size=50))

        assert len(batches) == 3
        for ab in batches:
            assert ab.batch.num_rows == 50
        # Log messages should be received from the externalized streams
        assert len(received_logs) == 3
        assert all(m.level == Level.INFO for m in received_logs)

    def test_server_stream_logs_from_external(self) -> None:
        """Verify logs embedded in externalized stream are dispatched to on_log."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)

        received_logs: list[Message] = []

        with aioresponses_ctx() as mock:
            _mock_aio_dynamic(storage, mock)
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                on_log=received_logs.append,
                external_location=config,
            ) as svc:
                batches = list(svc.stream_large(count=2, size=50))

        assert len(batches) == 2
        assert len(received_logs) == 2
        assert "producing batch 0" in received_logs[0].message
        assert "producing batch 1" in received_logs[1].message

    def test_bidi_large_output_externalized(self) -> None:
        """Large bidi output is externalized and resolved."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)

        with aioresponses_ctx() as mock:
            _mock_aio_dynamic(storage, mock)
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                external_location=config,
            ) as svc:
                bidi = svc.bidi_large(factor=2.0)
                with bidi:
                    input_batch = AnnotatedBatch.from_pydict(
                        {"value": [float(i) for i in range(50)]},
                        schema=pa.schema([pa.field("value", pa.float64())]),
                    )
                    result = bidi.exchange(input_batch)

        assert result.batch.num_rows == 50
        assert result.batch.column("value")[0].as_py() == 0.0
        assert result.batch.column("value")[1].as_py() == 2.0

    def test_bidi_large_input_externalized(self) -> None:
        """Large bidi input is externalized by client and resolved by server."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)

        with aioresponses_ctx() as mock:
            _mock_aio_dynamic(storage, mock)
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                external_location=config,
            ) as svc:
                bidi = svc.bidi_large(factor=3.0)
                with bidi:
                    input_batch = AnnotatedBatch.from_pydict(
                        {"value": [float(i) for i in range(50)]},
                        schema=pa.schema([pa.field("value", pa.float64())]),
                    )
                    result = bidi.exchange(input_batch)

        assert result.batch.num_rows == 50
        assert result.batch.column("value")[2].as_py() == 6.0

    def test_small_batches_inline(self) -> None:
        """Small batches remain inline (not externalized)."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=10_000_000)

        with serve_pipe(
            _ExternalService,
            _ExternalServiceImpl(),
            external_location=config,
        ) as svc:
            batches = list(svc.stream_large(count=2, size=2))

        assert len(batches) == 2
        for ab in batches:
            assert ab.batch.num_rows == 2
        assert len(storage.data) == 0


# ===========================================================================
# Integration tests — external storage disabled
# ===========================================================================


class TestExternalDisabled:
    """Integration tests verifying all method types work when external storage is not configured."""

    def test_unary_no_config(self) -> None:
        """Unary call works with no ExternalLocationConfig at all."""
        with serve_pipe(_ExternalService, _ExternalServiceImpl()) as svc:
            result = svc.echo_large(data="hello")

        assert result == "hello"

    def test_unary_storage_none(self) -> None:
        """Unary call works with ExternalLocationConfig(storage=None)."""
        config = ExternalLocationConfig(storage=None)

        with serve_pipe(
            _ExternalService,
            _ExternalServiceImpl(),
            external_location=config,
        ) as svc:
            result = svc.echo_large(data="x" * 200)

        assert result == "x" * 200

    def test_server_stream_no_config(self) -> None:
        """Server streaming works with no ExternalLocationConfig."""
        received_logs: list[Message] = []

        with serve_pipe(
            _ExternalService,
            _ExternalServiceImpl(),
            on_log=received_logs.append,
        ) as svc:
            batches = list(svc.stream_large(count=3, size=50))

        assert len(batches) == 3
        for ab in batches:
            assert ab.batch.num_rows == 50
        assert len(received_logs) == 3

    def test_server_stream_storage_none(self) -> None:
        """Server streaming works with ExternalLocationConfig(storage=None)."""
        config = ExternalLocationConfig(storage=None)

        with serve_pipe(
            _ExternalService,
            _ExternalServiceImpl(),
            external_location=config,
        ) as svc:
            batches = list(svc.stream_large(count=2, size=50))

        assert len(batches) == 2
        for ab in batches:
            assert ab.batch.num_rows == 50

    def test_bidi_no_config(self) -> None:
        """Bidi streaming works with no ExternalLocationConfig."""
        with serve_pipe(_ExternalService, _ExternalServiceImpl()) as svc:
            bidi = svc.bidi_large(factor=2.0)
            with bidi:
                input_batch = AnnotatedBatch.from_pydict(
                    {"value": [1.0, 2.0, 3.0]},
                    schema=pa.schema([pa.field("value", pa.float64())]),
                )
                result = bidi.exchange(input_batch)

        assert result.batch.num_rows == 3
        assert result.batch.column("value")[0].as_py() == 2.0
        assert result.batch.column("value")[2].as_py() == 6.0

    def test_bidi_storage_none(self) -> None:
        """Bidi streaming works with ExternalLocationConfig(storage=None)."""
        config = ExternalLocationConfig(storage=None)

        with serve_pipe(
            _ExternalService,
            _ExternalServiceImpl(),
            external_location=config,
        ) as svc:
            bidi = svc.bidi_large(factor=3.0)
            with bidi:
                input_batch = AnnotatedBatch.from_pydict(
                    {"value": [1.0, 2.0, 3.0]},
                    schema=pa.schema([pa.field("value", pa.float64())]),
                )
                result = bidi.exchange(input_batch)

        assert result.batch.num_rows == 3
        assert result.batch.column("value")[1].as_py() == 6.0


# ===========================================================================
# S3 tests (moto mock)
# ===========================================================================


class TestS3Storage:
    """Tests for S3Storage with moto mock.

    Uses moto's mock_aws for S3 operations and aioresponses to intercept
    fetches, reading back from moto's mock S3.
    """

    @pytest.fixture()
    def _s3_env(self) -> Iterator[tuple[Any, Any]]:
        """Create S3Storage and a boto3 client for reading back from mock S3."""
        pytest.importorskip("moto")
        boto3 = pytest.importorskip("boto3")

        from moto import mock_aws

        from vgi_rpc.s3 import S3Storage

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")

            storage = S3Storage(
                bucket="test-bucket",
                region_name="us-east-1",
            )

            yield storage, client

    def test_upload_and_presign(self, _s3_env: tuple[Any, Any]) -> None:
        """Upload IPC data, verify presigned URL is generated."""
        s3_storage, _ = _s3_env
        data_batch = pa.RecordBatch.from_pydict({"value": [42]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])

        url = s3_storage.upload(ipc_bytes, _SCHEMA)
        assert "test-bucket" in url
        assert ".arrow" in url

    def test_full_roundtrip(self, _s3_env: tuple[Any, Any]) -> None:
        """Full pipe-transport round-trip with S3Storage + moto."""
        from urllib.parse import urlparse

        s3_storage, s3_client = _s3_env
        config = ExternalLocationConfig(
            storage=s3_storage,
            externalize_threshold_bytes=10,
        )

        # Helper to read objects from moto's mock S3 for aioresponses
        def _s3_callback(url_: Any, **kwargs: Any) -> CallbackResult:
            parsed = urlparse(str(url_))
            key = parsed.path.lstrip("/")
            resp = s3_client.get_object(Bucket="test-bucket", Key=key)
            body: bytes = resp["Body"].read()
            return CallbackResult(status=200, body=body, headers={"Content-Length": str(len(body))})

        def _s3_head_callback(url_: Any, **kwargs: Any) -> CallbackResult:
            parsed = urlparse(str(url_))
            key = parsed.path.lstrip("/")
            resp = s3_client.get_object(Bucket="test-bucket", Key=key)
            body: bytes = resp["Body"].read()
            return CallbackResult(status=200, headers={"Content-Length": str(len(body))})

        with aioresponses_ctx() as mock:
            pattern = re.compile(r"^https://.*test-bucket.*$")
            for _ in range(10):
                mock.head(pattern, callback=_s3_head_callback)
                mock.get(pattern, callback=_s3_callback)
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                external_location=config,
            ) as svc:
                result = svc.echo_large(data="x" * 200)

        assert result == "x" * 200

    def test_upload_with_content_encoding(self, _s3_env: tuple[Any, Any]) -> None:
        """S3 put_object receives ContentEncoding kwarg when passed to upload()."""
        _s3_storage, s3_client = _s3_env
        s3_storage = _s3_storage

        data_batch = pa.RecordBatch.from_pydict({"value": [42]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        url = s3_storage.upload(ipc_bytes, _SCHEMA, content_encoding="zstd")

        assert ".arrow.zst" in url

        # Verify the object has ContentEncoding set via head_object
        from urllib.parse import urlparse

        parsed = urlparse(url)
        key = parsed.path.lstrip("/")
        head_resp = s3_client.head_object(Bucket="test-bucket", Key=key)
        assert head_resp["ContentEncoding"] == "zstd"

    def test_upload_without_content_encoding(self, _s3_env: tuple[Any, Any]) -> None:
        """No ContentEncoding when not passed to upload()."""
        s3_storage, _s3_client = _s3_env

        data_batch = pa.RecordBatch.from_pydict({"value": [42]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        url = s3_storage.upload(ipc_bytes, _SCHEMA)

        assert ".arrow.zst" not in url
        assert ".arrow" in url

    def test_file_extension_zst(self, _s3_env: tuple[Any, Any]) -> None:
        """Key ends in .arrow.zst when content_encoding="zstd" passed to upload()."""
        s3_storage, _ = _s3_env

        data_batch = pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        url = s3_storage.upload(ipc_bytes, _SCHEMA, content_encoding="zstd")

        assert url.split("?")[0].endswith(".arrow.zst")


# ===========================================================================
# GCS tests (mock)
# ===========================================================================


class TestGCSStorage:
    """Tests for GCSStorage with mocked google-cloud-storage."""

    @pytest.fixture()
    def _gcs_mocks(self) -> Iterator[tuple[MagicMock, MagicMock, MagicMock, MagicMock]]:
        """Create mocked GCS client hierarchy: (cls, client, bucket, blob)."""
        pytest.importorskip("google.cloud.storage")
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_client = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        with patch("google.cloud.storage.Client", return_value=mock_client) as mock_cls:
            yield mock_cls, mock_client, mock_bucket, mock_blob

    def test_upload_and_signed_url(self, _gcs_mocks: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
        """Upload IPC data, verify signed URL is generated."""
        from vgi_rpc.gcs import GCSStorage

        _mock_cls, mock_client, _mock_bucket, mock_blob = _gcs_mocks
        mock_blob.generate_signed_url.return_value = "https://storage.googleapis.com/signed-url"

        data_batch = pa.RecordBatch.from_pydict({"value": [42]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])

        storage = GCSStorage(bucket="test-bucket")
        url = storage.upload(ipc_bytes, _SCHEMA)

        assert url == "https://storage.googleapis.com/signed-url"
        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_blob.upload_from_string.assert_called_once_with(ipc_bytes, content_type="application/octet-stream")
        mock_blob.generate_signed_url.assert_called_once()
        call_kwargs = mock_blob.generate_signed_url.call_args
        assert call_kwargs.kwargs["version"] == "v4"
        assert call_kwargs.kwargs["method"] == "GET"

    def test_full_roundtrip(self, _gcs_mocks: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
        """Full round-trip with mocked GCS + aioresponses."""
        from vgi_rpc.gcs import GCSStorage

        _, _, _, mock_blob = _gcs_mocks
        uploaded: dict[str, bytes] = {}

        def capture_upload(data: bytes, content_type: str = "") -> None:
            uploaded["data"] = data

        mock_blob.upload_from_string.side_effect = capture_upload
        mock_blob.generate_signed_url.return_value = "https://storage.googleapis.com/test"

        storage = GCSStorage(bucket="test-bucket")
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)

        data_batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)

        ext_batch, ext_cm = maybe_externalize_batch(data_batch, None, config)

        assert ext_batch.num_rows == 0
        assert ext_cm is not None

        with aioresponses_ctx() as mock:
            body = uploaded["data"]
            mock.head(
                "https://storage.googleapis.com/test",
                headers={"Content-Length": str(len(body))},
            )
            mock.get(
                "https://storage.googleapis.com/test",
                body=body,
                headers={"Content-Length": str(len(body))},
            )
            resolved, _ = resolve_external_location(ext_batch, ext_cm, config)

        assert resolved.num_rows == 100

    def test_custom_project(self, _gcs_mocks: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
        """Verify project kwarg is passed to Client()."""
        from vgi_rpc.gcs import GCSStorage

        mock_cls, _, _, mock_blob = _gcs_mocks
        mock_blob.generate_signed_url.return_value = "https://example.com/signed"

        storage = GCSStorage(bucket="test-bucket", project="my-project")
        ipc_bytes = _serialize_ipc(_SCHEMA, [(pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA), None)])
        storage.upload(ipc_bytes, _SCHEMA)

        mock_cls.assert_called_once_with(project="my-project")

    def test_custom_prefix(self, _gcs_mocks: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
        """Verify blob name starts with custom prefix."""
        from vgi_rpc.gcs import GCSStorage

        _, _, mock_bucket, mock_blob = _gcs_mocks
        mock_blob.generate_signed_url.return_value = "https://example.com/signed"

        storage = GCSStorage(bucket="test-bucket", prefix="custom/prefix/")
        ipc_bytes = _serialize_ipc(_SCHEMA, [(pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA), None)])
        storage.upload(ipc_bytes, _SCHEMA)

        blob_name: str = mock_bucket.blob.call_args[0][0]
        assert blob_name.startswith("custom/prefix/")
        assert blob_name.endswith(".arrow")

    def test_upload_with_content_encoding(self, _gcs_mocks: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
        """Blob content_encoding set before upload when passed to upload()."""
        from vgi_rpc.gcs import GCSStorage

        _, _, _mock_bucket, mock_blob = _gcs_mocks
        mock_blob.generate_signed_url.return_value = "https://example.com/signed"

        storage = GCSStorage(bucket="test-bucket")
        ipc_bytes = _serialize_ipc(_SCHEMA, [(pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA), None)])
        storage.upload(ipc_bytes, _SCHEMA, content_encoding="zstd")

        # Verify content_encoding was set on the blob
        assert mock_blob.content_encoding == "zstd"

    def test_upload_without_content_encoding(
        self, _gcs_mocks: tuple[MagicMock, MagicMock, MagicMock, MagicMock]
    ) -> None:
        """content_encoding not set when not passed to upload()."""
        from vgi_rpc.gcs import GCSStorage

        _, _, mock_bucket, mock_blob = _gcs_mocks
        mock_blob.generate_signed_url.return_value = "https://example.com/signed"
        # Reset any mock tracking
        mock_blob.reset_mock()

        storage = GCSStorage(bucket="test-bucket")
        ipc_bytes = _serialize_ipc(_SCHEMA, [(pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA), None)])
        storage.upload(ipc_bytes, _SCHEMA)

        # content_encoding should not have been set (it's a MagicMock attribute)
        blob_name: str = mock_bucket.blob.call_args[0][0]
        assert blob_name.endswith(".arrow")
        assert not blob_name.endswith(".arrow.zst")

    def test_file_extension_zst(self, _gcs_mocks: tuple[MagicMock, MagicMock, MagicMock, MagicMock]) -> None:
        """Blob name ends in .arrow.zst when content_encoding="zstd" passed to upload()."""
        from vgi_rpc.gcs import GCSStorage

        _, _, mock_bucket, mock_blob = _gcs_mocks
        mock_blob.generate_signed_url.return_value = "https://example.com/signed"

        storage = GCSStorage(bucket="test-bucket")
        ipc_bytes = _serialize_ipc(_SCHEMA, [(pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA), None)])
        storage.upload(ipc_bytes, _SCHEMA, content_encoding="zstd")

        blob_name: str = mock_bucket.blob.call_args[0][0]
        assert blob_name.endswith(".arrow.zst")


# ===========================================================================
# Integration tests — FetchConfig (parallel fetch path)
# ===========================================================================


class TestFetchConfigIntegration:
    """Integration tests for resolve_external_location with FetchConfig."""

    def test_basic_resolution_with_fetch_config(self) -> None:
        """FetchConfig-based resolution succeeds for basic case."""
        storage = MockStorage()
        fetch_cfg = FetchConfig(parallel_threshold_bytes=10_000_000)  # above data size → simple GET
        config = ExternalLocationConfig(storage=storage, fetch_config=fetch_cfg, max_retries=0)

        data_batch = pa.RecordBatch.from_pydict({"value": [42]}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])
        url = "https://mock.storage/basic"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        with aioresponses_ctx() as mock:
            mock.head(url, headers={"Content-Length": str(len(ipc_bytes))})
            mock.get(url, body=ipc_bytes, headers={"Content-Length": str(len(ipc_bytes))})

            resolved, _resolved_cm = resolve_external_location(pointer, cm, config)

        assert resolved.num_rows == 1
        assert resolved.column("value")[0].as_py() == 42

    def test_parallel_path_with_fetch_config(self) -> None:
        """FetchConfig triggers parallel Range fetching above threshold."""
        storage = MockStorage()
        data_batch = pa.RecordBatch.from_pydict({"value": list(range(500))}, schema=_SCHEMA)
        ipc_bytes = _serialize_ipc(_SCHEMA, [(data_batch, None)])

        # Use tiny chunk/threshold to force parallel path
        fetch_cfg = FetchConfig(
            parallel_threshold_bytes=100,
            chunk_size_bytes=512,
            max_parallel_requests=4,
        )
        config = ExternalLocationConfig(storage=storage, fetch_config=fetch_cfg, max_retries=0)

        url = "https://mock.storage/parallel"
        storage.data[url] = ipc_bytes

        pointer, cm = make_external_location_batch(_SCHEMA, url)

        range_requests: list[str] = []

        def _range_callback(url_: Any, **kwargs: Any) -> CallbackResult:
            headers = kwargs.get("headers", {})
            range_header = headers.get("Range", "")
            range_requests.append(range_header)
            if range_header:
                match = re.match(r"bytes=(\d+)-(\d+)", range_header)
                if match:
                    start, end = int(match.group(1)), int(match.group(2))
                    chunk = ipc_bytes[start : end + 1]
                    return CallbackResult(
                        status=206,
                        body=chunk,
                        headers={
                            "Content-Range": f"bytes {start}-{end}/{len(ipc_bytes)}",
                            "Content-Length": str(len(chunk)),
                        },
                    )
            return CallbackResult(status=200, body=ipc_bytes)

        with aioresponses_ctx() as mock:
            mock.head(
                url,
                headers={"Content-Length": str(len(ipc_bytes)), "Accept-Ranges": "bytes"},
            )
            for _ in range(50):
                mock.get(url, callback=_range_callback)

            resolved, _ = resolve_external_location(pointer, cm, config)

        assert resolved.num_rows == 500
        # Verify that Range requests were actually made
        assert any("bytes=" in r for r in range_requests)


# ===========================================================================
# Unit tests — compression
# ===========================================================================


class TestCompression:
    """Tests for zstandard compression in ExternalLocation."""

    def test_compress_decompress_roundtrip(self) -> None:
        """IPC bytes survive compress → upload → fetch (with Content-Encoding: zstd) → decompress → parse."""
        storage = MockStorage()
        config = ExternalLocationConfig(
            storage=storage,
            externalize_threshold_bytes=10,
            compression=Compression(),
            max_retries=0,
        )

        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        ext_batch, ext_cm = maybe_externalize_batch(batch, None, config)

        assert ext_batch.num_rows == 0
        assert ext_cm is not None
        assert len(storage.data) == 1

        with _mock_aio(storage):
            resolved, _ = resolve_external_location(ext_batch, ext_cm, config)

        assert resolved.num_rows == 100
        assert resolved.column("value")[0].as_py() == 0
        assert resolved.column("value")[99].as_py() == 99

    def test_compression_disabled_by_default(self) -> None:
        """Default ExternalLocationConfig does not compress; uploaded data is raw IPC."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)

        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        maybe_externalize_batch(batch, None, config)

        uploaded_bytes = next(iter(storage.data.values()))
        # Raw IPC stream — NOT zstd-compressed (no zstd magic 0x28B52FFD)
        assert uploaded_bytes[:4] != b"\x28\xb5\x2f\xfd"
        # Verify it parses as valid IPC
        reader = ipc.open_stream(BytesIO(uploaded_bytes))
        b = reader.read_next_batch()
        assert b.num_rows == 100

    def test_compression_enabled_externalize_batch(self) -> None:
        """Compressed data is uploaded (verify uploaded bytes are zstd-compressed)."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10, compression=Compression())

        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        maybe_externalize_batch(batch, None, config)

        uploaded_bytes = next(iter(storage.data.values()))
        # Zstd magic number is 0xFD2FB528 (little-endian)
        assert uploaded_bytes[:4] == b"\x28\xb5\x2f\xfd"

    def test_compression_enabled_externalize_collector(self) -> None:
        """Collector output compressed when enabled."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10, compression=Compression())

        out = OutputCollector(_SCHEMA)
        out.emit_pydict({"value": list(range(100))})

        result = maybe_externalize_collector(out, config)
        assert len(result) == 1

        uploaded_bytes = next(iter(storage.data.values()))
        # Verify zstd magic
        assert uploaded_bytes[:4] == b"\x28\xb5\x2f\xfd"

    def test_compressed_resolve(self) -> None:
        """Resolve round-trip: externalize with compression → mock HTTP with Content-Encoding → resolve."""
        storage = MockStorage()
        config = ExternalLocationConfig(
            storage=storage, externalize_threshold_bytes=10, compression=Compression(), max_retries=0
        )

        batch = pa.RecordBatch.from_pydict({"value": [42, 43, 44]}, schema=_SCHEMA)
        ext_batch, ext_cm = maybe_externalize_batch(batch, None, config)

        with _mock_aio(storage):
            resolved, _ = resolve_external_location(ext_batch, ext_cm, config)

        assert resolved.num_rows == 3
        assert resolved.column("value")[1].as_py() == 43

    def test_compression_level_configurable(self) -> None:
        """Non-default compression level (e.g. 10) produces valid compressed data."""
        storage = MockStorage()
        config = ExternalLocationConfig(
            storage=storage,
            externalize_threshold_bytes=10,
            compression=Compression(level=10),
            max_retries=0,
        )

        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        ext_batch, ext_cm = maybe_externalize_batch(batch, None, config)

        uploaded_bytes = next(iter(storage.data.values()))
        # Verify it's valid zstd
        decompressed = zstandard.ZstdDecompressor().decompress(uploaded_bytes)
        # Decompressed data should be valid IPC
        reader = ipc.open_stream(BytesIO(decompressed))
        b = reader.read_next_batch()
        assert b.num_rows == 100

        with _mock_aio(storage):
            resolved, _ = resolve_external_location(ext_batch, ext_cm, config)

        assert resolved.num_rows == 100

    def test_no_content_encoding_header_no_decompression(self) -> None:
        """Fetch without Content-Encoding header returns raw bytes (backward compat)."""
        storage = MockStorage()  # No content_encoding
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10, max_retries=0)

        batch = pa.RecordBatch.from_pydict({"value": [1]}, schema=_SCHEMA)
        ext_batch, ext_cm = maybe_externalize_batch(batch, None, config)

        # Serve raw IPC without Content-Encoding — should work fine
        with _mock_aio(storage):
            resolved, _ = resolve_external_location(ext_batch, ext_cm, config)

        assert resolved.num_rows == 1
        assert resolved.column("value")[0].as_py() == 1

    def test_corrupt_compressed_data(self) -> None:
        """Corrupted bytes with Content-Encoding: zstd raises RuntimeError."""
        storage = MockStorage()
        config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10, max_retries=0)

        # Upload raw (non-compressed) data but serve with Content-Encoding: zstd
        batch = pa.RecordBatch.from_pydict({"value": list(range(100))}, schema=_SCHEMA)
        # Externalize WITHOUT compression — raw IPC uploaded
        no_compress_config = ExternalLocationConfig(storage=storage, externalize_threshold_bytes=10)
        ext_batch, ext_cm = maybe_externalize_batch(batch, None, no_compress_config)

        # Now try to resolve with the zstd mock — raw IPC isn't valid zstd
        with (
            _mock_aio(storage, content_encoding="zstd"),
            pytest.raises(RuntimeError, match="Failed to decompress zstd data"),
        ):
            resolve_external_location(ext_batch, ext_cm, config)


# ===========================================================================
# Integration tests — pipe transport with compression
# ===========================================================================


class TestPipeIntegrationCompressed:
    """Integration tests using pipe transport with compressed external storage."""

    def _make_config(self, storage: MockStorage, threshold: int = 100) -> ExternalLocationConfig:
        """Create compressed ExternalLocationConfig for testing."""
        return ExternalLocationConfig(
            storage=storage,
            externalize_threshold_bytes=threshold,
            max_retries=0,
            retry_delay_seconds=0.0,
            compression=Compression(),
        )

    def test_unary_large_compressed(self) -> None:
        """Large unary response with compression round-trips correctly."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=10)

        with aioresponses_ctx() as mock:
            _mock_aio_dynamic(storage, mock, content_encoding="zstd")
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                external_location=config,
            ) as svc:
                result = svc.echo_large(data="x" * 200)

        assert result == "x" * 200
        assert len(storage.data) >= 1
        # Verify uploaded data is zstd-compressed
        for data in storage.data.values():
            assert data[:4] == b"\x28\xb5\x2f\xfd"

    def test_server_stream_compressed(self) -> None:
        """Server stream + compression + logs works end-to-end."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)

        received_logs: list[Message] = []

        with aioresponses_ctx() as mock:
            _mock_aio_dynamic(storage, mock, content_encoding="zstd")
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                on_log=received_logs.append,
                external_location=config,
            ) as svc:
                batches = list(svc.stream_large(count=3, size=50))

        assert len(batches) == 3
        for ab in batches:
            assert ab.batch.num_rows == 50
        assert len(received_logs) == 3

    def test_bidi_compressed(self) -> None:
        """Bidi with compression works end-to-end."""
        storage = MockStorage()
        config = self._make_config(storage, threshold=100)

        with aioresponses_ctx() as mock:
            _mock_aio_dynamic(storage, mock, content_encoding="zstd")
            with serve_pipe(
                _ExternalService,
                _ExternalServiceImpl(),
                external_location=config,
            ) as svc:
                bidi = svc.bidi_large(factor=2.0)
                with bidi:
                    input_batch = AnnotatedBatch.from_pydict(
                        {"value": [float(i) for i in range(50)]},
                        schema=pa.schema([pa.field("value", pa.float64())]),
                    )
                    result = bidi.exchange(input_batch)

        assert result.batch.num_rows == 50
        assert result.batch.column("value")[0].as_py() == 0.0
        assert result.batch.column("value")[1].as_py() == 2.0
