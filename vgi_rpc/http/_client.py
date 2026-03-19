# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""HTTP client implementation using httpx.

Provides ``http_connect`` context manager, ``HttpStreamSession``,
``http_introspect``, ``http_capabilities``, and ``request_upload_urls``.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from http import HTTPStatus
from io import BytesIO
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast

import httpx
import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import (
    ExternalLocationConfig,
    UploadUrl,
    maybe_externalize_batch,
    resolve_external_location,
)
from vgi_rpc.log import Message
from vgi_rpc.metadata import STATE_KEY, merge_metadata, strip_keys
from vgi_rpc.rpc import (
    _EMPTY_SCHEMA,
    AnnotatedBatch,
    MethodType,
    RpcError,
    RpcMethodInfo,
    _dispatch_log_or_error,
    _drain_stream,
    _read_batch_with_log_check,
    _read_unary_response,
    _send_request,
    _write_request,
    rpc_methods,
)
from vgi_rpc.rpc._debug import fmt_batch, wire_http_logger
from vgi_rpc.rpc._wire import _read_stream_header
from vgi_rpc.utils import ArrowSerializableDataclass, IpcValidation, ValidatedReader, empty_batch

from ._common import (
    _ARROW_CONTENT_TYPE,
    _UPLOAD_URL_METHOD,
    _UPLOAD_URL_PARAMS_SCHEMA,
    MAX_REQUEST_BYTES_HEADER,
    MAX_UPLOAD_BYTES_HEADER,
    UPLOAD_URL_HEADER,
    _compress_body,
)
from ._retry import HttpRetryConfig, _options_with_retry, _post_with_retry

if TYPE_CHECKING:
    from vgi_rpc.introspect import ServiceDescription

    from ._testing import _SyncTestClient, _SyncTestResponse


def _open_response_stream(
    content: bytes,
    status_code: int,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> ValidatedReader:
    """Open an Arrow IPC stream from HTTP response bytes.

    Args:
        content: Response body bytes.
        status_code: HTTP status code (used in error messages).
        ipc_validation: Validation level for batches read from the stream.

    Returns:
        A ``ValidatedReader`` wrapping the IPC stream.

    Raises:
        RpcError: If the server returns 401 (``AuthenticationError``) or
            the response is not a valid Arrow IPC stream.

    """
    if wire_http_logger.isEnabledFor(logging.DEBUG):
        wire_http_logger.debug(
            "Open response stream: status=%d, body_size=%d",
            status_code,
            len(content),
        )
    # 401 responses are plain text (not Arrow IPC) because they are produced
    # by _AuthMiddleware before any method is resolved, so no output schema
    # is available.  We surface them as RpcError("AuthenticationError").
    if status_code == HTTPStatus.UNAUTHORIZED:
        raise RpcError("AuthenticationError", content.decode(errors="replace"), "")
    try:
        return ValidatedReader(ipc.open_stream(BytesIO(content)), ipc_validation)
    except pa.ArrowInvalid:
        body_preview = content[:200].decode(errors="replace") if content else ""
        raise RpcError(
            "HttpError",
            f"HTTP {status_code}: response is not a valid Arrow IPC stream (first 200 bytes: {body_preview!r})",
            "",
        ) from None


# ---------------------------------------------------------------------------
# Client — http_connect + HttpStreamSession
# ---------------------------------------------------------------------------


class HttpStreamSession:
    """Client-side handle for a stream over HTTP (both producer and exchange patterns).

    For producer streams, use ``__iter__()`` — yields batches from batched
    responses and follows continuation tokens transparently.
    For exchange streams, use ``exchange()`` — sends an input batch and
    receives an output batch.

    Supports context manager protocol for convenience.
    """

    __slots__ = (
        "_client",
        "_compression_level",
        "_external_config",
        "_finished",
        "_header",
        "_ipc_validation",
        "_method",
        "_on_log",
        "_output_schema",
        "_pending_batches",
        "_retry_config",
        "_state_bytes",
        "_url_prefix",
    )

    def __init__(
        self,
        client: httpx.Client | _SyncTestClient,
        url_prefix: str,
        method: str,
        state_bytes: bytes | None,
        output_schema: pa.Schema,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
        ipc_validation: IpcValidation = IpcValidation.FULL,
        pending_batches: list[AnnotatedBatch] | None = None,
        finished: bool = False,
        header: object | None = None,
        retry_config: HttpRetryConfig | None = None,
        compression_level: int | None = None,
    ) -> None:
        """Initialize with HTTP client, method details, and initial state."""
        self._client = client
        self._url_prefix = url_prefix
        self._method = method
        self._state_bytes = state_bytes
        self._output_schema = output_schema
        self._on_log = on_log
        self._external_config = external_config
        self._ipc_validation = ipc_validation
        self._pending_batches: list[AnnotatedBatch] = pending_batches or []
        self._finished = finished
        self._header = header
        self._retry_config = retry_config
        self._compression_level = compression_level

    @property
    def header(self) -> object | None:
        """The stream header, or ``None`` if the stream has no header."""
        return self._header

    def typed_header[H: ArrowSerializableDataclass](self, header_type: type[H]) -> H:
        """Return the stream header narrowed to the expected type.

        Args:
            header_type: The expected header dataclass type.

        Returns:
            The header, typed as *header_type*.

        Raises:
            TypeError: If the header is ``None`` or not an instance of
                *header_type*.

        """
        if self._header is None:
            raise TypeError(f"Stream has no header (expected {header_type.__name__})")
        if not isinstance(self._header, header_type):
            raise TypeError(f"Header type mismatch: expected {header_type.__name__}, got {type(self._header).__name__}")
        return self._header

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers, adding ``Content-Encoding`` and ``Accept-Encoding`` when compression is enabled."""
        headers: dict[str, str] = {"Content-Type": _ARROW_CONTENT_TYPE}
        if self._compression_level is not None:
            headers["Content-Encoding"] = "zstd"
            headers["Accept-Encoding"] = "zstd"
        return headers

    def _prepare_body(self, content: bytes) -> bytes:
        """Compress *content* when compression is enabled."""
        if self._compression_level is not None:
            return _compress_body(content, self._compression_level)
        return content

    def exchange(self, input_batch: AnnotatedBatch) -> AnnotatedBatch:
        """Send an input batch and receive the output batch.

        Args:
            input_batch: The input batch to send.

        Returns:
            The output batch from the server.

        Raises:
            RpcError: If the server reports an error or the stream has finished.

        """
        if self._state_bytes is None:
            raise RpcError("ProtocolError", "Stream has finished — no state token available", "")

        batch_to_write = input_batch.batch
        cm_to_write = input_batch.custom_metadata

        # Client-side externalization for large inputs
        if self._external_config is not None:
            batch_to_write, cm_to_write = maybe_externalize_batch(batch_to_write, cm_to_write, self._external_config)

        # Write input batch with state in metadata
        req_buf = BytesIO()
        state_md = pa.KeyValueMetadata({STATE_KEY: self._state_bytes})
        merged = merge_metadata(cm_to_write, state_md)
        with ipc.new_stream(req_buf, batch_to_write.schema) as writer:
            writer.write_batch(batch_to_write, custom_metadata=merged)

        if wire_http_logger.isEnabledFor(logging.DEBUG):
            wire_http_logger.debug(
                "HTTP stream exchange: method=%s, input=%s",
                self._method,
                fmt_batch(batch_to_write),
            )
        # Exchange calls are NOT retried: the server's process() method may
        # have side effects, and a proxy 502 after server processing would
        # cause duplicate execution.  Only init/unary/continuation are retried.
        resp = self._client.post(
            f"{self._url_prefix}/{self._method}/exchange",
            content=self._prepare_body(req_buf.getvalue()),
            headers=self._build_headers(),
        )
        if wire_http_logger.isEnabledFor(logging.DEBUG):
            wire_http_logger.debug(
                "HTTP stream exchange response: method=%s, status=%d, size=%d",
                self._method,
                resp.status_code,
                len(resp.content),
            )

        # Read response — log batches + data batch with state
        reader = _open_response_stream(resp.content, resp.status_code, self._ipc_validation)
        try:
            ab = _read_batch_with_log_check(reader, self._on_log, self._external_config)
        except RpcError:
            _drain_stream(reader)
            raise

        # Extract updated state from metadata
        if ab.custom_metadata is not None:
            new_state = ab.custom_metadata.get(STATE_KEY)
            if new_state is not None:
                self._state_bytes = new_state

        # Strip state token from user-visible metadata
        user_cm = strip_keys(ab.custom_metadata, STATE_KEY)

        _drain_stream(reader)
        return AnnotatedBatch(batch=ab.batch, custom_metadata=user_cm)

    def _send_continuation(self, token: bytes) -> ValidatedReader:
        """Send a continuation request and return the new response reader."""
        req_buf = BytesIO()
        state_md = pa.KeyValueMetadata({STATE_KEY: token})
        with ipc.new_stream(req_buf, _EMPTY_SCHEMA) as writer:
            writer.write_batch(empty_batch(_EMPTY_SCHEMA), custom_metadata=state_md)

        resp = _post_with_retry(
            self._client,
            f"{self._url_prefix}/{self._method}/exchange",
            content=self._prepare_body(req_buf.getvalue()),
            headers=self._build_headers(),
            config=self._retry_config,
        )
        return _open_response_stream(resp.content, resp.status_code, self._ipc_validation)

    def __iter__(self) -> Iterator[AnnotatedBatch]:
        """Iterate over output batches from a producer stream.

        Yields pre-loaded batches from init, then follows continuation tokens.
        """
        # Yield pre-loaded batches from init response
        yield from self._pending_batches
        self._pending_batches.clear()

        if self._finished:
            return

        # Follow continuation tokens
        if self._state_bytes is None:
            return

        reader: ValidatedReader | None = None
        try:
            reader = self._send_continuation(self._state_bytes)
            while True:
                try:
                    batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
                except StopIteration:
                    break

                # Check for continuation token (zero-row batch with STATE_KEY)
                if batch.num_rows == 0 and custom_metadata is not None:
                    token = custom_metadata.get(STATE_KEY)
                    if token is not None:
                        if not isinstance(token, bytes):
                            raise TypeError(f"Expected bytes for state token, got {type(token).__name__}")
                        _drain_stream(reader)
                        reader = self._send_continuation(token)
                        continue

                # Dispatch log/error batches
                if _dispatch_log_or_error(batch, custom_metadata, self._on_log):
                    continue

                resolved_batch, resolved_cm = resolve_external_location(
                    batch, custom_metadata, self._external_config, self._on_log, reader.ipc_validation
                )
                yield AnnotatedBatch(batch=resolved_batch, custom_metadata=resolved_cm)
        except RpcError:
            if reader is not None:
                _drain_stream(reader)
            raise

    def close(self) -> None:
        """Close the session (no-op for HTTP — stateless)."""

    def __enter__(self) -> HttpStreamSession:
        """Enter the context."""
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_val: BaseException | None,
        _exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context."""
        self.close()


@contextlib.contextmanager
def http_connect[P](
    protocol: type[P],
    base_url: str | None = None,
    *,
    prefix: str | None = None,
    on_log: Callable[[Message], None] | None = None,
    client: httpx.Client | _SyncTestClient | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
    retry: HttpRetryConfig | None = None,
    compression_level: int | None = 3,
) -> Iterator[P]:
    """Connect to an HTTP RPC server and yield a typed proxy.

    Args:
        protocol: The Protocol class defining the RPC interface.
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``; ignored when a pre-built
            *client* is provided.  The internally-created client follows
            redirects transparently.
        prefix: URL prefix matching the server's prefix.  When ``None``
            (the default), auto-detected from a ``_SyncTestClient``'s
            ``.prefix`` attribute, or ``""`` for other clients.
        on_log: Optional callback for log messages from the server.
        client: Optional HTTP client — ``httpx.Client`` for production,
            or a ``_SyncTestClient`` from ``make_sync_client()`` for testing.
        external_location: Optional ExternalLocationConfig for
            resolving and producing externalized batches.
        ipc_validation: Validation level for incoming IPC batches.
        retry: Optional retry configuration for transient HTTP failures.
            When ``None`` (the default), no retries are attempted.
        compression_level: Zstandard compression level for request bodies.
            ``3`` (the default) compresses requests and adds
            ``Content-Encoding: zstd``.  ``None`` disables request
            compression (httpx still auto-decompresses server responses).

    Yields:
        A typed RPC proxy supporting all methods defined on *protocol*.

    Raises:
        ValueError: If *base_url* is ``None`` and *client* is ``None``.

    """
    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)

    # Auto-detect prefix from _SyncTestClient when not explicitly provided
    if prefix is None:
        url_prefix = getattr(client, "prefix", "")
    else:
        url_prefix = prefix
    try:
        yield cast(
            P,
            _HttpProxy(
                protocol,
                client,
                url_prefix,
                on_log,
                external_config=external_location,
                ipc_validation=ipc_validation,
                retry_config=retry,
                compression_level=compression_level,
            ),
        )
    finally:
        if own_client:
            client.close()


def http_introspect(
    base_url: str | None = None,
    *,
    prefix: str | None = None,
    client: httpx.Client | _SyncTestClient | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
    retry: HttpRetryConfig | None = None,
) -> ServiceDescription:
    """Send a ``__describe__`` request over HTTP and return a ``ServiceDescription``.

    Args:
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``.
        prefix: URL prefix matching the server's prefix.  ``None``
            auto-detects from ``_SyncTestClient``.
        client: Optional HTTP client (``httpx.Client`` or ``_SyncTestClient``).
        ipc_validation: Validation level for incoming IPC batches.
        retry: Optional retry configuration for transient HTTP failures.

    Returns:
        A ``ServiceDescription`` with all method metadata.

    Raises:
        RpcError: If the server does not support introspection or returns
            an error.
        ValueError: If *base_url* is ``None`` and *client* is ``None``.

    """
    from vgi_rpc.introspect import DESCRIBE_METHOD_NAME, parse_describe_batch

    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)
    if prefix is None:
        prefix = getattr(client, "prefix", "")

    try:
        # Build a minimal request: empty params with __describe__ method name
        req_buf = BytesIO()
        request_metadata = pa.KeyValueMetadata(
            {
                b"vgi_rpc.method": DESCRIBE_METHOD_NAME.encode(),
                b"vgi_rpc.request_version": b"1",
            }
        )
        with ipc.new_stream(req_buf, _EMPTY_SCHEMA) as writer:
            writer.write_batch(
                pa.RecordBatch.from_pydict({}, schema=_EMPTY_SCHEMA),
                custom_metadata=request_metadata,
            )

        resp = _post_with_retry(
            client,
            f"{prefix}/{DESCRIBE_METHOD_NAME}",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
            config=retry,
        )

        reader = _open_response_stream(resp.content, resp.status_code, ipc_validation)
        # Skip log batches
        while True:
            batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
            if not _dispatch_log_or_error(batch, custom_metadata):
                break
        _drain_stream(reader)

        return parse_describe_batch(batch, custom_metadata)
    finally:
        if own_client:
            client.close()


def _init_http_stream_session(
    client: httpx.Client | _SyncTestClient,
    url_prefix: str,
    method_name: str,
    reader: ValidatedReader,
    on_log: Callable[[Message], None] | None = None,
    *,
    external_config: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
    header: object | None = None,
    retry_config: HttpRetryConfig | None = None,
    compression_level: int | None = None,
) -> HttpStreamSession:
    """Parse an init response and return an ``HttpStreamSession``.

    Reads the IPC stream from the init response, collecting state tokens,
    pending data batches, and log/error batches.  This is shared between
    ``_HttpProxy._make_stream_caller`` and the CLI.

    Args:
        client: HTTP client for subsequent exchange requests.
        url_prefix: URL path prefix (e.g. ``/vgi``).
        method_name: RPC method name.
        reader: ``ValidatedReader`` opened from the init response.
        on_log: Optional log callback.
        external_config: Optional external location config.
        ipc_validation: Validation level for IPC batches.
        header: Optional pre-read stream header.
        retry_config: Optional retry configuration for transient failures.
        compression_level: Zstandard compression level for exchange requests.

    Returns:
        A configured ``HttpStreamSession`` ready for iteration or exchange.

    Raises:
        RpcError: If the server reports an error in the init response.

    """
    output_schema = reader.schema
    state_bytes: bytes | None = None
    pending_batches: list[AnnotatedBatch] = []
    finished = False

    try:
        while True:
            try:
                batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
            except StopIteration:
                finished = True
                break

            # Check for state token (zero-row batch with STATE_KEY)
            if batch.num_rows == 0 and custom_metadata is not None:
                token = custom_metadata.get(STATE_KEY)
                if token is not None:
                    if isinstance(token, bytes):
                        state_bytes = token
                    break

            # Dispatch log/error batches
            if _dispatch_log_or_error(batch, custom_metadata, on_log):
                continue

            # Data batch from producer init
            resolved_batch, resolved_cm = resolve_external_location(
                batch, custom_metadata, external_config, on_log, reader.ipc_validation
            )
            pending_batches.append(AnnotatedBatch(batch=resolved_batch, custom_metadata=resolved_cm))
    except RpcError:
        _drain_stream(reader)
        raise

    _drain_stream(reader)

    return HttpStreamSession(
        client=client,
        url_prefix=url_prefix,
        method=method_name,
        state_bytes=state_bytes,
        output_schema=output_schema,
        on_log=on_log,
        external_config=external_config,
        ipc_validation=ipc_validation,
        pending_batches=pending_batches,
        finished=finished,
        header=header,
        retry_config=retry_config,
        compression_level=compression_level,
    )


class _HttpProxy:
    """Dynamic proxy that implements RPC method calls over HTTP."""

    def __init__(
        self,
        protocol: type,
        client: httpx.Client | _SyncTestClient,
        url_prefix: str,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
        ipc_validation: IpcValidation = IpcValidation.FULL,
        retry_config: HttpRetryConfig | None = None,
        compression_level: int | None = None,
    ) -> None:
        self._protocol = protocol
        self._client = client
        self._url_prefix = url_prefix
        self._methods = rpc_methods(protocol)
        self._on_log = on_log
        self._external_config = external_config
        self._ipc_validation = ipc_validation
        self._retry_config = retry_config
        self._compression_level = compression_level

    def _build_headers(self) -> dict[str, str]:
        """Build HTTP headers, adding ``Content-Encoding`` and ``Accept-Encoding`` when compression is enabled."""
        headers: dict[str, str] = {"Content-Type": _ARROW_CONTENT_TYPE}
        if self._compression_level is not None:
            headers["Content-Encoding"] = "zstd"
            headers["Accept-Encoding"] = "zstd"
        return headers

    def _prepare_body(self, content: bytes) -> bytes:
        """Compress *content* when compression is enabled."""
        if self._compression_level is not None:
            return _compress_body(content, self._compression_level)
        return content

    def __getattr__(self, name: str) -> Any:
        """Resolve RPC method names to callable proxies, caching on first access.

        Returns ``Any`` because each method name maps to a different callable
        signature (unary or stream), so no single static return type can
        represent all of them.
        """
        info = self._methods.get(name)
        if info is None:
            raise AttributeError(f"{self._protocol.__name__} has no RPC method '{name}'")

        if info.method_type == MethodType.UNARY:
            caller = self._make_unary_caller(info)
        elif info.method_type == MethodType.STREAM:
            caller = self._make_stream_caller(info)
        else:
            raise AttributeError(f"Unknown method type for '{name}'")

        self.__dict__[name] = caller
        return caller

    def _make_unary_caller(self, info: RpcMethodInfo) -> Callable[..., object]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log
        ext_cfg = self._external_config
        ipc_validation = self._ipc_validation
        retry_cfg = self._retry_config
        build_headers = self._build_headers
        prepare_body = self._prepare_body

        def caller(**kwargs: object) -> object:
            if wire_http_logger.isEnabledFor(logging.DEBUG):
                wire_http_logger.debug("HTTP unary call: %s/%s", url_prefix, info.name)
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = _post_with_retry(
                client,
                f"{url_prefix}/{info.name}",
                content=prepare_body(req_buf.getvalue()),
                headers=build_headers(),
                config=retry_cfg,
            )
            if wire_http_logger.isEnabledFor(logging.DEBUG):
                wire_http_logger.debug(
                    "HTTP unary response: method=%s, status=%d, size=%d",
                    info.name,
                    resp.status_code,
                    len(resp.content),
                )

            reader = _open_response_stream(resp.content, resp.status_code, ipc_validation)
            return _read_unary_response(reader, info, on_log, ext_cfg)

        return caller

    def _make_stream_caller(self, info: RpcMethodInfo) -> Callable[..., HttpStreamSession]:
        client = self._client
        url_prefix = self._url_prefix
        on_log = self._on_log
        ext_cfg = self._external_config
        ipc_validation = self._ipc_validation
        retry_cfg = self._retry_config
        build_headers = self._build_headers
        prepare_body = self._prepare_body
        compression_level = self._compression_level

        def caller(**kwargs: object) -> HttpStreamSession:
            if wire_http_logger.isEnabledFor(logging.DEBUG):
                wire_http_logger.debug("HTTP stream init: %s/%s/init", url_prefix, info.name)
            # Send init request
            req_buf = BytesIO()
            _send_request(req_buf, info, kwargs)

            resp = _post_with_retry(
                client,
                f"{url_prefix}/{info.name}/init",
                content=prepare_body(req_buf.getvalue()),
                headers=build_headers(),
                config=retry_cfg,
            )
            if wire_http_logger.isEnabledFor(logging.DEBUG):
                wire_http_logger.debug(
                    "HTTP stream init response: method=%s, status=%d, size=%d",
                    info.name,
                    resp.status_code,
                    len(resp.content),
                )

            # Read header from response before main IPC stream (if method declares one).
            # For non-401 errors (e.g. 500), _read_stream_header still works correctly
            # because _set_error_response writes a proper IPC error stream with error
            # metadata, which _read_stream_header detects via _dispatch_log_or_error.
            header = None
            resp_stream = BytesIO(resp.content)
            if info.header_type is not None:
                # Check for auth errors first (plain text, not Arrow IPC)
                if resp.status_code == 401:
                    _open_response_stream(resp.content, resp.status_code, ipc_validation)
                header = _read_stream_header(resp_stream, info.header_type, ipc_validation, on_log, ext_cfg)

            reader = _open_response_stream(resp_stream.read(), resp.status_code, ipc_validation)
            return _init_http_stream_session(
                client=client,
                url_prefix=url_prefix,
                method_name=info.name,
                reader=reader,
                on_log=on_log,
                external_config=ext_cfg,
                ipc_validation=ipc_validation,
                header=header,
                retry_config=retry_cfg,
                compression_level=compression_level,
            )

        return caller


# ---------------------------------------------------------------------------
# Server capabilities discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HttpServerCapabilities:
    """Capabilities advertised by an HTTP RPC server.

    Attributes:
        max_request_bytes: Maximum request body size the server advertises,
            or ``None`` if the server does not advertise a limit.
            Advertisement only -- no server-side enforcement.
        upload_url_support: Whether the server supports the
            ``__upload_url__`` endpoint for client-side uploads.
        max_upload_bytes: Maximum upload size the server advertises for
            client-vended URLs, or ``None`` if not advertised.
            Advertisement only -- no server-side enforcement.

    """

    max_request_bytes: int | None = None
    upload_url_support: bool = False
    max_upload_bytes: int | None = None


def http_capabilities(
    base_url: str | None = None,
    *,
    prefix: str | None = None,
    client: httpx.Client | _SyncTestClient | None = None,
    retry: HttpRetryConfig | None = None,
) -> HttpServerCapabilities:
    """Discover server capabilities via an OPTIONS request.

    Sends ``OPTIONS {prefix}/__capabilities__`` and reads capability
    headers (``VGI-Max-Request-Bytes``, ``VGI-Upload-URL-Support``,
    ``VGI-Max-Upload-Bytes``) from the response.

    Args:
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``.
        prefix: URL prefix matching the server's prefix.  ``None``
            auto-detects from ``_SyncTestClient``.
        client: Optional HTTP client (``httpx.Client`` or ``_SyncTestClient``).
        retry: Optional retry configuration for transient HTTP failures.

    Returns:
        An ``HttpServerCapabilities`` with discovered values.

    Raises:
        ValueError: If *base_url* is ``None`` and *client* is ``None``.

    """
    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)
    if prefix is None:
        prefix = getattr(client, "prefix", "")

    try:
        url = f"{prefix}/__capabilities__"
        resp = _options_with_retry(client, url, config=retry)
        headers = resp.headers

        max_req: int | None = None
        raw = headers.get(MAX_REQUEST_BYTES_HEADER) or headers.get(MAX_REQUEST_BYTES_HEADER.lower())
        if raw is not None:
            with contextlib.suppress(ValueError):
                max_req = int(raw)

        upload_raw = headers.get(UPLOAD_URL_HEADER) or headers.get(UPLOAD_URL_HEADER.lower())
        upload_support = upload_raw == "true" if upload_raw is not None else False

        max_upload: int | None = None
        upload_bytes_raw = headers.get(MAX_UPLOAD_BYTES_HEADER) or headers.get(MAX_UPLOAD_BYTES_HEADER.lower())
        if upload_bytes_raw is not None:
            with contextlib.suppress(ValueError):
                max_upload = int(upload_bytes_raw)

        return HttpServerCapabilities(
            max_request_bytes=max_req,
            upload_url_support=upload_support,
            max_upload_bytes=max_upload,
        )
    finally:
        if own_client:
            client.close()


def request_upload_urls(
    base_url: str | None = None,
    *,
    count: int = 1,
    prefix: str | None = None,
    client: httpx.Client | _SyncTestClient | None = None,
    retry: HttpRetryConfig | None = None,
) -> list[UploadUrl]:
    """Request pre-signed upload URLs from the server's ``__upload_url__`` endpoint.

    The server must have been configured with an ``upload_url_provider``
    in ``make_wsgi_app()``.

    Args:
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``.
        count: Number of upload URLs to request (default 1, max 100).
        prefix: URL prefix matching the server's prefix.  ``None``
            auto-detects from ``_SyncTestClient``.
        client: Optional HTTP client (``httpx.Client`` or ``_SyncTestClient``).
        retry: Optional retry configuration for transient HTTP failures.

    Returns:
        A list of ``UploadUrl`` objects with pre-signed PUT and GET URLs.

    Raises:
        RpcError: If the server does not support upload URLs (404) or
            returns an error.
        ValueError: If *base_url* is ``None`` and *client* is ``None``.

    """
    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)
    if prefix is None:
        prefix = getattr(client, "prefix", "")

    try:
        # Build request IPC with standard wire protocol metadata
        req_buf = BytesIO()
        _write_request(req_buf, _UPLOAD_URL_METHOD, _UPLOAD_URL_PARAMS_SCHEMA, {"count": count})

        resp = _post_with_retry(
            client,
            f"{prefix}/__upload_url__/init",
            content=req_buf.getvalue(),
            headers={"Content-Type": _ARROW_CONTENT_TYPE},
            config=retry,
        )

        # Without an upload_url_provider the route doesn't exist and the
        # request falls through to _StreamInitResource → 404.
        if resp.status_code == HTTPStatus.NOT_FOUND:
            raise RpcError("NotSupported", "Server does not support upload URLs", "")

        reader = _open_response_stream(resp.content, resp.status_code)
        urls: list[UploadUrl] = []
        try:
            while True:
                try:
                    batch, custom_metadata = reader.read_next_batch_with_custom_metadata()
                except StopIteration:
                    break

                if _dispatch_log_or_error(batch, custom_metadata):
                    continue

                for i in range(batch.num_rows):
                    upload_url = batch.column("upload_url")[i].as_py()
                    download_url = batch.column("download_url")[i].as_py()
                    expires_at = batch.column("expires_at")[i].as_py()
                    urls.append(UploadUrl(upload_url=upload_url, download_url=download_url, expires_at=expires_at))
        except RpcError:
            _drain_stream(reader)
            raise
        _drain_stream(reader)
        return urls
    finally:
        if own_client:
            client.close()


# ---------------------------------------------------------------------------
# OAuth Protected Resource Metadata discovery (RFC 9728)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OAuthResourceMetadataResponse:
    """Parsed RFC 9728 metadata received from a server (client-side).

    Returned by ``http_oauth_metadata()`` and ``fetch_oauth_metadata()``.
    For the server-side configuration class, see ``OAuthResourceMetadata``
    in ``vgi_rpc.http._oauth``.

    Attributes:
        resource: Canonical URL of the protected resource.
        authorization_servers: Authorization server issuer URLs.
        scopes_supported: OAuth scopes the resource understands.
        bearer_methods_supported: Token delivery methods supported.
        resource_signing_alg_values_supported: JWS algorithms for
            resource-signed responses.
        resource_name: Human-readable name of the resource.
        resource_documentation: URL to developer documentation.
        resource_policy_uri: URL to the resource's privacy policy.
        resource_tos_uri: URL to the resource's terms of service.
        client_id: OAuth client_id to use when authenticating with the
            authorization server.  Custom extension (not in RFC 9728).
        client_secret: OAuth client_secret to use when authenticating with the
            authorization server.  Custom extension (not in RFC 9728).
        use_id_token_as_bearer: When ``True``, the client should use the
            OIDC ``id_token`` as the Bearer token instead of the
            ``access_token``.  Custom extension (not in RFC 9728).
        device_code_client_id: OAuth client_id to use specifically for the
            device code grant flow.  Custom extension (not in RFC 9728).
        device_code_client_secret: OAuth client_secret to use specifically
            for the device code grant flow.  Custom extension (not in
            RFC 9728).

    """

    resource: str
    authorization_servers: tuple[str, ...]
    scopes_supported: tuple[str, ...] = ()
    bearer_methods_supported: tuple[str, ...] = ("header",)
    resource_signing_alg_values_supported: tuple[str, ...] = ()
    resource_name: str | None = None
    resource_documentation: str | None = None
    resource_policy_uri: str | None = None
    resource_tos_uri: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    use_id_token_as_bearer: bool = False
    device_code_client_id: str | None = None
    device_code_client_secret: str | None = None


def http_oauth_metadata(
    base_url: str | None = None,
    *,
    prefix: str | None = None,
    client: httpx.Client | _SyncTestClient | None = None,
) -> OAuthResourceMetadataResponse | None:
    """Discover OAuth Protected Resource Metadata (RFC 9728).

    Sends ``GET /.well-known/oauth-protected-resource{prefix}`` and parses
    the JSON response into an ``OAuthResourceMetadataResponse``.

    Args:
        base_url: Base URL of the server (e.g. ``http://localhost:8000``).
            Required when *client* is ``None``.
        prefix: URL prefix matching the server's prefix.  ``None``
            auto-detects from ``_SyncTestClient``.
            Must match the ``prefix`` passed to ``make_wsgi_app()`` on the
            server side — a mismatch will result in a 404 (``None`` return).
        client: Optional HTTP client (``httpx.Client`` or ``_SyncTestClient``).

    Returns:
        An ``OAuthResourceMetadataResponse`` with discovered metadata, or
        ``None`` if the server does not advertise metadata (404).

    Raises:
        ValueError: If *base_url* is ``None`` and *client* is ``None``,
            or if the server returns an unexpected HTTP error.

    """
    own_client = client is None
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        client = httpx.Client(base_url=base_url, follow_redirects=True)
    if prefix is None:
        prefix = getattr(client, "prefix", "")

    try:
        url = f"/.well-known/oauth-protected-resource{prefix}"
        resp: httpx.Response | _SyncTestResponse = client.get(url)
        if resp.status_code == HTTPStatus.NOT_FOUND:
            return None
        if resp.status_code != HTTPStatus.OK:
            raise ValueError(f"Failed to fetch OAuth metadata from {url}: HTTP {resp.status_code}")
        body: dict[str, Any] = json.loads(resp.content)
        return _parse_metadata_json(body)
    finally:
        if own_client:
            client.close()


_RESOURCE_METADATA_RE = re.compile(r'resource_metadata="([^"]+)"')
_CLIENT_ID_RE = re.compile(r'client_id="([^"]+)"')
_CLIENT_SECRET_RE = re.compile(r'client_secret="([^"]+)"')
_USE_ID_TOKEN_RE = re.compile(r'use_id_token_as_bearer="([^"]+)"')
_DEVICE_CODE_CLIENT_ID_RE = re.compile(r'device_code_client_id="([^"]+)"')
_DEVICE_CODE_CLIENT_SECRET_RE = re.compile(r'device_code_client_secret="([^"]+)"')


def parse_resource_metadata_url(www_authenticate: str) -> str | None:
    """Extract the ``resource_metadata`` URL from a ``WWW-Authenticate`` header.

    Parses a ``Bearer`` challenge per RFC 9728 Section 5.1 and returns
    the ``resource_metadata`` parameter value, or ``None`` if not present.

    Args:
        www_authenticate: The ``WWW-Authenticate`` header value
            (e.g. ``'Bearer resource_metadata="https://..."'``).

    Returns:
        The metadata URL string, or ``None`` if the header does not
        contain a ``resource_metadata`` parameter.

    """
    match = _RESOURCE_METADATA_RE.search(www_authenticate)
    return match.group(1) if match else None


def parse_client_id(www_authenticate: str) -> str | None:
    """Extract the ``client_id`` from a ``WWW-Authenticate`` header.

    Parses a ``Bearer`` challenge and returns the ``client_id`` parameter
    value, or ``None`` if not present.  This is a custom extension (not
    defined in RFC 9728).

    Args:
        www_authenticate: The ``WWW-Authenticate`` header value.

    Returns:
        The client_id string, or ``None`` if not present.

    """
    match = _CLIENT_ID_RE.search(www_authenticate)
    return match.group(1) if match else None


def parse_client_secret(www_authenticate: str) -> str | None:
    """Extract the ``client_secret`` from a ``WWW-Authenticate`` header.

    Parses a ``Bearer`` challenge and returns the ``client_secret`` parameter
    value, or ``None`` if not present.  This is a custom extension (not
    defined in RFC 9728).

    Args:
        www_authenticate: The ``WWW-Authenticate`` header value.

    Returns:
        The client_secret string, or ``None`` if not present.

    """
    match = _CLIENT_SECRET_RE.search(www_authenticate)
    return match.group(1) if match else None


def parse_use_id_token_as_bearer(www_authenticate: str) -> bool:
    """Extract the ``use_id_token_as_bearer`` flag from a ``WWW-Authenticate`` header.

    Parses a ``Bearer`` challenge and returns ``True`` if the
    ``use_id_token_as_bearer`` parameter is present with value ``"true"``.
    This is a custom extension (not defined in RFC 9728).

    Args:
        www_authenticate: The ``WWW-Authenticate`` header value.

    Returns:
        ``True`` if the header contains ``use_id_token_as_bearer="true"``,
        ``False`` otherwise.

    """
    match = _USE_ID_TOKEN_RE.search(www_authenticate)
    return match.group(1) == "true" if match else False


def parse_device_code_client_id(www_authenticate: str) -> str | None:
    """Extract the ``device_code_client_id`` from a ``WWW-Authenticate`` header.

    Parses a ``Bearer`` challenge and returns the ``device_code_client_id``
    parameter value, or ``None`` if not present.  This is a custom extension
    (not defined in RFC 9728).

    Args:
        www_authenticate: The ``WWW-Authenticate`` header value.

    Returns:
        The device_code_client_id string, or ``None`` if not present.

    """
    match = _DEVICE_CODE_CLIENT_ID_RE.search(www_authenticate)
    return match.group(1) if match else None


def parse_device_code_client_secret(www_authenticate: str) -> str | None:
    """Extract the ``device_code_client_secret`` from a ``WWW-Authenticate`` header.

    Parses a ``Bearer`` challenge and returns the ``device_code_client_secret``
    parameter value, or ``None`` if not present.  This is a custom extension
    (not defined in RFC 9728).

    Args:
        www_authenticate: The ``WWW-Authenticate`` header value.

    Returns:
        The device_code_client_secret string, or ``None`` if not present.

    """
    match = _DEVICE_CODE_CLIENT_SECRET_RE.search(www_authenticate)
    return match.group(1) if match else None


def _parse_metadata_json(body: dict[str, Any]) -> OAuthResourceMetadataResponse:
    """Parse a JSON dict into an ``OAuthResourceMetadataResponse``.

    Raises:
        ValueError: If required fields (``resource``, ``authorization_servers``)
            are missing from the JSON.

    """
    try:
        resource = body["resource"]
        authorization_servers = body["authorization_servers"]
    except KeyError as exc:
        raise ValueError(f"OAuth metadata response missing required field {exc}") from exc
    return OAuthResourceMetadataResponse(
        resource=resource,
        authorization_servers=tuple(authorization_servers),
        scopes_supported=tuple(body.get("scopes_supported", ())),
        bearer_methods_supported=tuple(body.get("bearer_methods_supported", ("header",))),
        resource_signing_alg_values_supported=tuple(body.get("resource_signing_alg_values_supported", ())),
        resource_name=body.get("resource_name"),
        resource_documentation=body.get("resource_documentation"),
        resource_policy_uri=body.get("resource_policy_uri"),
        resource_tos_uri=body.get("resource_tos_uri"),
        client_id=body.get("client_id"),
        client_secret=body.get("client_secret"),
        use_id_token_as_bearer=body.get("use_id_token_as_bearer", False),
        device_code_client_id=body.get("device_code_client_id"),
        device_code_client_secret=body.get("device_code_client_secret"),
    )


def fetch_oauth_metadata(
    metadata_url: str,
    *,
    client: httpx.Client | _SyncTestClient | None = None,
) -> OAuthResourceMetadataResponse:
    """Fetch OAuth metadata from a URL extracted from a 401 ``WWW-Authenticate`` header.

    Unlike ``http_oauth_metadata()`` which constructs the well-known URL
    from a base URL + prefix, this function fetches from an explicit URL
    — typically parsed from a 401 response via
    ``parse_resource_metadata_url()``.

    Args:
        metadata_url: Full URL to the metadata document (e.g.
            ``https://api.example.com/.well-known/oauth-protected-resource/vgi``).
        client: Optional HTTP client (``httpx.Client`` or ``_SyncTestClient``).

    Returns:
        An ``OAuthResourceMetadataResponse`` with the server's OAuth metadata.

    Raises:
        ValueError: If the server returns a non-200 status code or the
            response is missing required fields.

    """
    own_client = client is None
    if client is None:
        client = httpx.Client(follow_redirects=True)

    try:
        resp: httpx.Response | _SyncTestResponse = client.get(metadata_url)
        if resp.status_code != HTTPStatus.OK:
            raise ValueError(f"Failed to fetch OAuth metadata from {metadata_url}: HTTP {resp.status_code}")
        body: dict[str, Any] = json.loads(resp.content)
        return _parse_metadata_json(body)
    finally:
        if own_client:
            client.close()
