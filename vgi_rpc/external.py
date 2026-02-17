"""ExternalLocation batch support for large data batches.

When record batches exceed a configurable size threshold, they can be
externalized to remote storage (e.g. S3) and replaced with zero-row
pointer batches containing a ``vgi_rpc.location`` metadata key.
Readers resolve the pointers transparently; writers externalize batches
above the threshold.

Key design point: when externalizing stream output, the
external IPC stream contains ALL batches from one ``OutputCollector``
cycle (log batches + the single data batch). The pointer batch replaces
the entire cycle in the response stream.  On resolution, the client reads
back all batches from the external stream, dispatches log batches via
``on_log``, and returns just the data batch.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

import aiohttp
import pyarrow as pa
import zstandard
from pyarrow import ipc
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from vgi_rpc.external_fetch import FetchConfig, fetch_url
from vgi_rpc.log import Message
from vgi_rpc.metadata import (
    LOCATION_FETCH_MS_KEY,
    LOCATION_KEY,
    LOCATION_SOURCE_KEY,
    LOG_LEVEL_KEY,
    merge_metadata,
)
from vgi_rpc.utils import IpcValidation, ValidatedReader

if TYPE_CHECKING:
    from vgi_rpc.rpc import OutputCollector

_logger = logging.getLogger(__name__)

__all__ = [
    "Compression",
    "ExternalLocationConfig",
    "ExternalStorage",
    "FetchConfig",
    "UploadUrl",
    "UploadUrlProvider",
    "https_only_validator",
]


# ---------------------------------------------------------------------------
# ExternalStorage protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ExternalStorage(Protocol):
    """Pluggable storage interface for externalizing large batches.

    Implementations must be thread-safe — ``upload()`` may be called
    concurrently from different server threads.

    .. important:: **Object lifecycle** — vgi-rpc does not manage the
       lifecycle of uploaded objects.  Data uploaded via ``upload()``
       or ``generate_upload_url()`` persists indefinitely unless the
       server operator configures cleanup.  Use storage-level lifecycle
       rules (e.g. S3 Lifecycle Policies, GCS Object Lifecycle
       Management) or bucket-level TTLs to automatically expire and
       delete stale objects.
    """

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Upload serialized IPC data and return a URL for retrieval.

        The uploaded object is not automatically deleted — server
        operators are responsible for configuring object cleanup via
        storage lifecycle rules or TTLs.

        Args:
            data: Complete Arrow IPC stream bytes.
            schema: The schema of the data being uploaded.
            content_encoding: Optional encoding applied to *data*
                (e.g. ``"zstd"``).  Backends should store this so that
                fetchers can decompress correctly.

        Returns:
            A URL (typically pre-signed) that can be fetched to retrieve
            the uploaded data.

        """
        ...


# ---------------------------------------------------------------------------
# UploadUrl + UploadUrlProvider
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UploadUrl:
    """Pre-signed URL pair for client-side data upload.

    S3/GCS pre-signed URLs are signed per HTTP method, so a PUT URL
    cannot be used for GET — hence two URLs for the same storage object.

    Attributes:
        upload_url: Pre-signed PUT URL for uploading data.
        download_url: Pre-signed GET URL for downloading the uploaded data.
        expires_at: Expiration time for the pre-signed URLs (UTC).

    """

    upload_url: str
    download_url: str
    expires_at: datetime


@runtime_checkable
class UploadUrlProvider(Protocol):
    """Generates pre-signed upload URL pairs.

    Implementations must be thread-safe — ``generate_upload_url()``
    may be called concurrently from different server threads.

    .. important:: **Object lifecycle** — vgi-rpc does not manage the
       lifecycle of uploaded objects.  Data uploaded via these URLs
       persists indefinitely unless the server operator configures
       cleanup.  Use storage-level lifecycle rules (e.g. S3 Lifecycle
       Policies, GCS Object Lifecycle Management) or bucket-level TTLs
       to automatically expire and delete stale objects.
    """

    def generate_upload_url(self, schema: pa.Schema) -> UploadUrl:
        """Generate a pre-signed upload/download URL pair.

        The caller receives time-limited PUT and GET URLs for a new
        storage object.  **The uploaded object is not automatically
        deleted** — server operators are responsible for configuring
        object cleanup via storage lifecycle rules or TTLs.

        Args:
            schema: The Arrow schema of the data to be uploaded.
                Backends may use this for content-type or metadata hints.

        Returns:
            An ``UploadUrl`` with PUT and GET URLs for the same object.

        """
        ...


# ---------------------------------------------------------------------------
# URL validation
# ---------------------------------------------------------------------------


def https_only_validator(url: str) -> None:
    """Reject URLs that do not use the ``https`` scheme.

    This is the default ``url_validator`` for ``ExternalLocationConfig``.
    It prevents the client from issuing requests over plain HTTP or other
    schemes (``ftp``, ``file``, etc.) when resolving external-location
    pointers.

    Args:
        url: The URL to validate.

    Raises:
        ValueError: If the URL scheme is not ``https``.

    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"URL scheme '{parsed.scheme}' not allowed (only 'https' is permitted)")


# ---------------------------------------------------------------------------
# ExternalLocationConfig
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Compression:
    """Compression settings for externalized data.

    Attributes:
        algorithm: Compression algorithm.  Currently only ``"zstd"``
            is supported.
        level: Compression level (1-22 for zstd).

    """

    algorithm: Literal["zstd"] = "zstd"
    level: int = 3


@dataclass(frozen=True)
class ExternalLocationConfig:
    """Configuration for ExternalLocation batch support.

    .. note:: **Trust boundary** — When resolution is enabled, the
       client will fetch URLs embedded in server responses.  The
       default ``url_validator`` (``https_only_validator``) rejects
       non-HTTPS URLs, but callers should still only connect to
       **trusted** RPC servers.

    Attributes:
        storage: Storage backend for uploading externalized data.
            Required for production (writing); not needed for
            resolution-only (reading) scenarios.
        externalize_threshold_bytes: Data batch buffer size above
            which to externalize.  Uses
            ``batch.get_total_buffer_size()`` as a fast O(1) estimate.
        max_retries: Number of fetch retries (total attempts =
            ``max_retries + 1``, capped at 3).
        retry_delay_seconds: Delay between retry attempts.
        fetch_config: Fetch configuration controlling parallelism,
            timeouts, and size limits.  Defaults to sensible values.
        compression: Compression settings for externalized data.
            ``None`` disables compression (default).
        url_validator: Callback invoked before fetching external
            URLs.  Should raise ``ValueError`` to reject.  Defaults
            to ``https_only_validator`` (HTTPS-only).  Set to
            ``None`` to disable validation.

    """

    storage: ExternalStorage | None = None
    externalize_threshold_bytes: int = 1_048_576
    max_retries: int = field(default=2)
    retry_delay_seconds: float = 0.5
    fetch_config: FetchConfig = field(default_factory=FetchConfig)
    compression: Compression | None = None
    url_validator: Callable[[str], None] | None = field(default=https_only_validator)


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def is_external_location_batch(batch: pa.RecordBatch, custom_metadata: pa.KeyValueMetadata | None) -> bool:
    """Check whether a batch is an ExternalLocation pointer.

    A pointer batch is a zero-row batch whose custom metadata contains
    ``vgi_rpc.location`` and does NOT contain ``vgi_rpc.log_level``
    (which would make it a log batch).

    Args:
        batch: The record batch to check.
        custom_metadata: Custom metadata from the batch.

    Returns:
        ``True`` if the batch is an ExternalLocation pointer.

    """
    if batch.num_rows != 0:
        return False
    if custom_metadata is None:
        return False
    if custom_metadata.get(LOCATION_KEY) is None:
        return False
    return custom_metadata.get(LOG_LEVEL_KEY) is None


# ---------------------------------------------------------------------------
# Creation
# ---------------------------------------------------------------------------


def make_external_location_batch(
    schema: pa.Schema,
    url: str,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata]:
    """Create a zero-row pointer batch for an externalized location.

    Args:
        schema: The schema the pointer batch should conform to.
        url: The URL where the actual data resides.

    Returns:
        A ``(batch, custom_metadata)`` tuple where *batch* is zero-row
        and *custom_metadata* contains ``vgi_rpc.location``.

    """
    batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=f.type) for f in schema],
        schema=schema,
    )
    custom_metadata = pa.KeyValueMetadata({LOCATION_KEY: url.encode()})
    return batch, custom_metadata


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


# Sentinel for "no on_log callback" — allows callers to pass None
_OnLog = Callable[[Message], None] | None


def resolve_external_location(
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None,
    config: ExternalLocationConfig | None,
    on_log: _OnLog = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Resolve an ExternalLocation pointer batch, or return it unchanged.

    Safe to call on any batch — non-pointer batches and ``config=None``
    are returned as-is.  Detection via ``is_external_location_batch`` and
    URL extraction are handled internally.

    .. note:: **Trust boundary** — The URLs fetched by this function
       originate from the server / storage layer (pre-signed S3/GCS
       URLs generated by ``ExternalStorage.upload()``).  The
       ``url_validator`` on ``ExternalLocationConfig`` (default:
       ``https_only_validator``) is called before each fetch to
       reject disallowed schemes or hosts.

    Args:
        batch: A record batch (pointer or regular data).
        custom_metadata: Custom metadata from the batch.
        config: ExternalLocation configuration, or ``None`` to skip.
        on_log: Optional callback for log messages found in the
            fetched stream.  If ``None``, log batches are silently
            discarded.
        ipc_validation: Validation level for batches in the fetched stream.

    Returns:
        ``(batch, custom_metadata)`` unchanged if not a pointer,
        otherwise ``(resolved_data_batch, resolved_metadata)``.

    Raises:
        RuntimeError: If all retries are exhausted, a redirect loop is
            detected, the fetched stream contains no data batch or
            multiple data batches, or the fetch exceeds
            ``config.fetch_config.max_fetch_bytes``.
        ValueError: If the URL fails validation (e.g. non-HTTPS when using
            ``https_only_validator``) or the fetched data has a schema
            mismatch.

    """
    if config is None or not is_external_location_batch(batch, custom_metadata):
        return batch, custom_metadata

    # is_external_location_batch guarantees custom_metadata is not None
    # and contains LOCATION_KEY
    assert custom_metadata is not None
    url_bytes = custom_metadata.get(LOCATION_KEY)
    assert url_bytes is not None
    url = url_bytes.decode() if isinstance(url_bytes, bytes) else str(url_bytes)

    if config.url_validator is not None:
        config.url_validator(url)

    max_retries = min(config.max_retries, 2)  # cap at 3 total attempts

    retry_types: tuple[type[BaseException], ...] = (OSError, pa.ArrowInvalid, aiohttp.ClientError)

    try:
        retryer = Retrying(
            stop=stop_after_attempt(max_retries + 1),
            wait=wait_fixed(config.retry_delay_seconds),
            retry=retry_if_exception_type(retry_types),
            reraise=True,
        )
        return retryer(_fetch_and_resolve, batch.schema, url, config, on_log, ipc_validation)
    except (*retry_types,) as exc:
        _logger.error(
            "ExternalLocation resolution failed: %s (attempts=%d)",
            url,
            max_retries + 1,
            extra={"url": url, "attempts": max_retries + 1, "error_type": type(exc).__name__},
        )
        raise RuntimeError(f"Failed to resolve ExternalLocation after {max_retries + 1} attempts: {url}") from exc


def _fetch_and_resolve(
    expected_schema: pa.Schema,
    url: str,
    config: ExternalLocationConfig,
    on_log: _OnLog,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Fetch URL and extract the data batch from the IPC stream."""
    from vgi_rpc.rpc import _dispatch_log_or_error

    t0 = time.monotonic()
    data = fetch_url(url, config.fetch_config)
    elapsed_ms = (time.monotonic() - t0) * 1000

    reader = ValidatedReader(ipc.open_stream(BytesIO(data)), ipc_validation)
    data_batches: list[tuple[pa.RecordBatch, pa.KeyValueMetadata | None]] = []

    while True:
        try:
            fetched_batch, fetched_cm = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            break

        # Prevent redirect loops
        if fetched_cm is not None and fetched_cm.get(LOCATION_KEY) is not None:
            raise RuntimeError(f"Redirect loop detected: fetched batch from {url} contains vgi_rpc.location")

        # Dispatch log batches
        if _dispatch_log_or_error(fetched_batch, fetched_cm, on_log):
            continue

        data_batches.append((fetched_batch, fetched_cm))

    if len(data_batches) == 0:
        raise RuntimeError(f"No data batch found in ExternalLocation stream from {url}")
    if len(data_batches) > 1:
        raise RuntimeError(f"Multiple data batches ({len(data_batches)}) found in ExternalLocation stream from {url}")

    resolved_batch, resolved_cm = data_batches[0]

    # Validate schema compatibility
    if resolved_batch.schema != expected_schema:
        raise ValueError(
            f"Schema mismatch in ExternalLocation: expected {expected_schema}, got {resolved_batch.schema}"
        )

    # Attach fetch metadata
    fetch_metadata = pa.KeyValueMetadata(
        {
            LOCATION_FETCH_MS_KEY: f"{elapsed_ms:.1f}".encode(),
            LOCATION_SOURCE_KEY: url.encode(),
        }
    )
    final_cm = merge_metadata(resolved_cm, fetch_metadata)

    return resolved_batch, final_cm


# ---------------------------------------------------------------------------
# Production — OutputCollector
# ---------------------------------------------------------------------------


def maybe_externalize_collector(
    out: OutputCollector,
    config: ExternalLocationConfig,
) -> list[tuple[pa.RecordBatch, pa.KeyValueMetadata | None]]:
    """Possibly externalize an entire OutputCollector cycle.

    If the data batch exceeds ``config.externalize_threshold_bytes`` and storage is
    configured, serializes ALL batches (logs + data) as a single IPC
    stream, uploads via ``storage.upload()``, and returns a single-element
    list containing the ExternalLocation pointer batch.

    Otherwise returns the original batches unchanged.

    Args:
        out: The ``OutputCollector`` to check.
        config: ExternalLocation configuration.

    Returns:
        List of ``(batch, custom_metadata)`` tuples ready for writing.

    """
    if config.storage is None:
        return [(ab.batch, ab.custom_metadata) for ab in out.batches]

    # Check if there is a data batch to externalize
    try:
        data_ab = out.data_batch
    except RuntimeError:
        # No data batch — finished-only or log-only collector
        return [(ab.batch, ab.custom_metadata) for ab in out.batches]

    # Fast O(1) size check
    if data_ab.batch.get_total_buffer_size() < config.externalize_threshold_bytes:
        return [(ab.batch, ab.custom_metadata) for ab in out.batches]

    # Serialize all batches into one IPC stream
    buf = BytesIO()
    with ipc.new_stream(buf, out.output_schema) as writer:
        for ab in out.batches:
            if ab.custom_metadata is not None:
                writer.write_batch(ab.batch, custom_metadata=ab.custom_metadata)
            else:
                writer.write_batch(ab.batch)

    ipc_bytes = buf.getvalue()

    content_encoding: str | None = None
    if config.compression is not None:
        ipc_bytes = zstandard.ZstdCompressor(level=config.compression.level).compress(ipc_bytes)
        content_encoding = config.compression.algorithm

    url = config.storage.upload(ipc_bytes, out.output_schema, content_encoding=content_encoding)
    _logger.debug(
        "Batch externalized: %s (%d bytes, compressed=%s)",
        url,
        len(ipc_bytes),
        content_encoding is not None,
        extra={"url": url, "size_bytes": len(ipc_bytes), "compressed": content_encoding is not None},
    )

    pointer_batch, pointer_cm = make_external_location_batch(out.output_schema, url)
    return [(pointer_batch, pointer_cm)]


# ---------------------------------------------------------------------------
# Production — single batch
# ---------------------------------------------------------------------------


def maybe_externalize_batch(
    batch: pa.RecordBatch,
    custom_metadata: pa.KeyValueMetadata | None,
    config: ExternalLocationConfig,
) -> tuple[pa.RecordBatch, pa.KeyValueMetadata | None]:
    """Possibly externalize a single batch.

    For non-OutputCollector write points (unary results, bidi inputs).

    Args:
        batch: The batch to possibly externalize.
        custom_metadata: Custom metadata for the batch.
        config: ExternalLocation configuration.

    Returns:
        Original ``(batch, custom_metadata)`` if below threshold or no
        storage configured; otherwise a pointer batch.

    """
    if config.storage is None:
        return batch, custom_metadata

    # Never externalize zero-row batches (logs, errors, finish markers)
    if batch.num_rows == 0:
        return batch, custom_metadata

    # Fast O(1) size check
    if batch.get_total_buffer_size() < config.externalize_threshold_bytes:
        return batch, custom_metadata

    # Serialize the single batch as IPC stream
    buf = BytesIO()
    with ipc.new_stream(buf, batch.schema) as writer:
        if custom_metadata is not None:
            writer.write_batch(batch, custom_metadata=custom_metadata)
        else:
            writer.write_batch(batch)

    ipc_bytes = buf.getvalue()

    content_encoding: str | None = None
    if config.compression is not None:
        ipc_bytes = zstandard.ZstdCompressor(level=config.compression.level).compress(ipc_bytes)
        content_encoding = config.compression.algorithm

    url = config.storage.upload(ipc_bytes, batch.schema, content_encoding=content_encoding)
    _logger.debug(
        "Batch externalized: %s (%d bytes, compressed=%s)",
        url,
        len(ipc_bytes),
        content_encoding is not None,
        extra={"url": url, "size_bytes": len(ipc_bytes), "compressed": content_encoding is not None},
    )

    return make_external_location_batch(batch.schema, url)
