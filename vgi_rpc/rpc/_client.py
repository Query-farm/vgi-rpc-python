# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Client proxy and connection."""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Iterator
from io import IOBase
from types import TracebackType
from typing import Any, cast

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.external import ExternalLocationConfig, maybe_externalize_batch
from vgi_rpc.log import Message
from vgi_rpc.rpc._common import _EMPTY_SCHEMA, MethodType, RpcError
from vgi_rpc.rpc._debug import fmt_batch, wire_request_logger, wire_stream_logger, wire_transport_logger
from vgi_rpc.rpc._transport import RpcTransport
from vgi_rpc.rpc._types import _TICK_BATCH, AnnotatedBatch, RpcMethodInfo, rpc_methods
from vgi_rpc.rpc._wire import _read_batch_with_log_check, _read_stream_header, _read_unary_response, _send_request
from vgi_rpc.shm import ShmSegment, maybe_write_to_shm
from vgi_rpc.utils import ArrowSerializableDataclass, IpcValidation, ValidatedReader

# Exceptions that indicate the transport peer has disconnected or the IPC
# data is truncated/corrupt.  Caught on the client side and wrapped into
# ``RpcError("TransportError", ...)``.  We intentionally list specific
# ``ConnectionError`` subclasses rather than the broad ``OSError`` to avoid
# swallowing unrelated OS-level errors (e.g. ``PermissionError``).
_TRANSPORT_ERRORS = (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, EOFError, pa.ArrowInvalid)


class StreamSession:
    """Client-side handle for a stream call (both producer and exchange patterns).

    For producer streams, use ``__iter__()`` or ``tick()``.
    For exchange streams, use ``exchange()`` with context manager.
    Log batches are delivered to the ``on_log`` callback.

    Not thread-safe: do not share a session across threads.
    """

    __slots__ = (
        "_closed",
        "_external_config",
        "_header",
        "_input_writer",
        "_ipc_validation",
        "_on_log",
        "_output_reader",
        "_reader_stream",
        "_shm",
        "_writer_stream",
    )

    def __init__(
        self,
        writer_stream: IOBase,
        reader_stream: IOBase,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
        ipc_validation: IpcValidation = IpcValidation.FULL,
        shm: ShmSegment | None = None,
        header: object | None = None,
    ) -> None:
        """Initialize with writer/reader streams and optional log callback."""
        self._writer_stream = writer_stream
        self._reader_stream = reader_stream
        self._on_log = on_log
        self._input_writer: ipc.RecordBatchStreamWriter | None = None
        self._output_reader: ValidatedReader | None = None
        self._closed = False
        self._external_config = external_config
        self._ipc_validation = ipc_validation
        self._shm = shm
        self._header = header

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

    def _write_batch(self, input: AnnotatedBatch) -> None:
        """Write a batch to the input stream, opening it on first use."""
        batch_to_write = input.batch
        cm_to_write = input.custom_metadata

        # Client-side SHM for large inputs (preferred over external)
        if self._shm is not None:
            batch_to_write, cm_to_write = maybe_write_to_shm(batch_to_write, cm_to_write, self._shm)
        elif self._external_config is not None:
            batch_to_write, cm_to_write = maybe_externalize_batch(batch_to_write, cm_to_write, self._external_config)

        first_write = self._input_writer is None
        if self._input_writer is None:
            self._input_writer = ipc.new_stream(self._writer_stream, batch_to_write.schema)
        if wire_stream_logger.isEnabledFor(logging.DEBUG):
            wire_stream_logger.debug(
                "Stream write input: %s, first_write=%s",
                fmt_batch(batch_to_write),
                first_write,
            )
        if cm_to_write is not None:
            self._input_writer.write_batch(batch_to_write, custom_metadata=cm_to_write)
        else:
            self._input_writer.write_batch(batch_to_write)

    def _read_response(self) -> AnnotatedBatch:
        """Read the next data batch from the output stream, skipping log batches."""
        if self._output_reader is None:
            self._output_reader = ValidatedReader(ipc.open_stream(self._reader_stream), self._ipc_validation)
        ab = _read_batch_with_log_check(self._output_reader, self._on_log, self._external_config, shm=self._shm)
        if wire_stream_logger.isEnabledFor(logging.DEBUG):
            wire_stream_logger.debug("Stream read output: %s", fmt_batch(ab.batch))
        return ab

    def exchange(self, input: AnnotatedBatch) -> AnnotatedBatch:
        """Send an input batch, receive the output batch.

        Returns an ``AnnotatedBatch``. Log batches are delivered to
        the ``on_log`` callback before returning. On ``RpcError``, the
        session is automatically closed so the transport is clean.

        Raises:
            StopIteration: When the stream has finished.
            RpcError: On server-side errors or transport failures.

        """
        if wire_stream_logger.isEnabledFor(logging.DEBUG):
            wire_stream_logger.debug("Stream exchange: sending input")
        try:
            self._write_batch(input)
        except _TRANSPORT_ERRORS as exc:
            # Set _closed directly — calling close() would attempt I/O on the broken transport.
            self._closed = True
            raise RpcError("TransportError", f"Transport failed during stream exchange (write): {exc}", "") from exc
        try:
            return self._read_response()
        except RpcError:
            self.close()
            raise
        except _TRANSPORT_ERRORS as exc:
            self._closed = True  # Bypass close() — transport is broken.
            raise RpcError("TransportError", f"Transport failed during stream exchange (read): {exc}", "") from exc

    def tick(self) -> AnnotatedBatch:
        """Send a tick batch (producer streams) and receive the output batch.

        Returns:
            The next output batch.

        Raises:
            StopIteration: When the producer stream has finished.
            RpcError: On server-side errors or transport failures.

        """
        if wire_stream_logger.isEnabledFor(logging.DEBUG):
            wire_stream_logger.debug("Stream tick")
        try:
            self._write_batch(_TICK_BATCH)
        except _TRANSPORT_ERRORS as exc:
            # Set _closed directly — calling close() would attempt I/O on the broken transport.
            self._closed = True
            raise RpcError("TransportError", f"Transport failed during stream tick (write): {exc}", "") from exc
        try:
            return self._read_response()
        except StopIteration:
            self.close()
            raise
        except RpcError:
            self.close()
            raise
        except _TRANSPORT_ERRORS as exc:
            self._closed = True  # Bypass close() — transport is broken.
            raise RpcError("TransportError", f"Transport failed during stream tick (read): {exc}", "") from exc

    def __iter__(self) -> Iterator[AnnotatedBatch]:
        """Iterate over output batches from a producer stream.

        Sends tick batches and yields output batches until the server
        signals stream completion.
        """
        while True:
            try:
                yield self.tick()
            except StopIteration:
                break

    def close(self) -> None:
        """Close input stream (signals EOS) and drain remaining output."""
        if self._closed:
            return
        if wire_stream_logger.isEnabledFor(logging.DEBUG):
            wire_stream_logger.debug("Stream close")
        self._closed = True
        if self._input_writer is not None:
            self._input_writer.close()
        else:
            with ipc.new_stream(self._writer_stream, _EMPTY_SCHEMA):
                pass
        if self._output_reader is None:
            try:
                self._output_reader = ValidatedReader(ipc.open_stream(self._reader_stream), self._ipc_validation)
            except (pa.ArrowInvalid, OSError, StopIteration):
                return
        _MAX_DRAIN = 10_000
        with contextlib.suppress(StopIteration, RpcError, pa.ArrowInvalid, OSError):
            for _ in range(_MAX_DRAIN):
                _read_batch_with_log_check(self._output_reader, self._on_log, self._external_config, shm=self._shm)

    def __enter__(self) -> StreamSession:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager."""
        self.close()


class _RpcProxy:
    """Dynamic proxy that implements RPC method calls through a transport.

    Not thread-safe: each proxy serialises calls over a single transport.
    Do not share a proxy across threads; create one connection per thread instead.
    """

    def __init__(
        self,
        protocol: type,
        transport: RpcTransport,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> None:
        self._protocol = protocol
        self._transport = transport
        self._methods = rpc_methods(protocol)
        self._on_log = on_log
        self._external_config = external_config
        self._ipc_validation = ipc_validation
        # ShmPipeTransport and _PooledTransport expose a .shm property;
        # plain RpcTransport does not.  Duck-type to avoid coupling to concrete classes.
        self._shm: ShmSegment | None = getattr(transport, "shm", None)

    def __getattr__(self, name: str) -> Any:
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
        transport = self._transport
        on_log = self._on_log
        ext_cfg = self._external_config
        ipc_validation = self._ipc_validation
        shm = self._shm

        def caller(**kwargs: object) -> object:
            if wire_request_logger.isEnabledFor(logging.DEBUG):
                wire_request_logger.debug("Unary call: method=%s", info.name)
            try:
                _send_request(transport.writer, info, kwargs, shm=shm)
                reader = ValidatedReader(ipc.open_stream(transport.reader), ipc_validation)
                return _read_unary_response(reader, info, on_log, ext_cfg, shm=shm)
            except RpcError:
                raise
            except _TRANSPORT_ERRORS as exc:
                raise RpcError(
                    "TransportError", f"Transport failed during unary call to '{info.name}': {exc}", ""
                ) from exc

        return caller

    def _make_stream_caller(self, info: RpcMethodInfo) -> Callable[..., StreamSession]:
        transport = self._transport
        on_log = self._on_log
        ext_cfg = self._external_config
        ipc_validation = self._ipc_validation
        shm = self._shm

        def caller(**kwargs: object) -> StreamSession:
            if wire_stream_logger.isEnabledFor(logging.DEBUG):
                wire_stream_logger.debug("Stream init: method=%s", info.name)
            try:
                _send_request(transport.writer, info, kwargs, shm=shm)
                # _PooledTransport (pool.py) uses __slots__ and tracks stream
                # lifecycle to detect abandoned streams.  object.__setattr__ is
                # needed because __slots__ classes don't have __dict__.
                # Note: _stream_opened is set here (before session construction)
                # because the request has been sent — even if session creation
                # fails, the transport state is tainted and the pool must
                # discard the worker rather than reuse it.
                if hasattr(transport, "_stream_opened"):
                    object.__setattr__(transport, "_stream_opened", True)
                header = None
                if info.header_type is not None:
                    header = _read_stream_header(transport.reader, info.header_type, ipc_validation, on_log, ext_cfg)
                session = StreamSession(
                    transport.writer,
                    transport.reader,
                    on_log,
                    external_config=ext_cfg,
                    ipc_validation=ipc_validation,
                    shm=shm,
                    header=header,
                )
                if hasattr(transport, "_last_stream_session"):
                    object.__setattr__(transport, "_last_stream_session", session)  # __slots__
                return session
            except RpcError:
                raise
            except _TRANSPORT_ERRORS as exc:
                raise RpcError(
                    "TransportError", f"Transport failed during stream init for '{info.name}': {exc}", ""
                ) from exc

        return caller


# ---------------------------------------------------------------------------
# RpcConnection — typed context manager
# ---------------------------------------------------------------------------


class RpcConnection[P]:
    """Context manager that provides a typed RPC proxy over a transport.

    The type parameter ``P`` is the Protocol class, enabling IDE
    autocompletion for all methods defined on the protocol::

        with RpcConnection(MyProtocol, transport) as svc:
            result = svc.add(a=1, b=2)   # IDE sees MyProtocol methods

    """

    __slots__ = ("_external_config", "_ipc_validation", "_on_log", "_protocol", "_transport")

    def __init__(
        self,
        protocol: type[P],
        transport: RpcTransport,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_location: ExternalLocationConfig | None = None,
        ipc_validation: IpcValidation = IpcValidation.FULL,
    ) -> None:
        """Initialize with a protocol type and transport."""
        self._protocol = protocol
        self._transport = transport
        self._on_log = on_log
        self._external_config = external_location
        self._ipc_validation = ipc_validation

    def __enter__(self) -> P:
        """Enter the context and return a typed proxy."""
        if wire_transport_logger.isEnabledFor(logging.DEBUG):
            wire_transport_logger.debug("RpcConnection open: protocol=%s", self._protocol.__name__)
        return cast(
            P,
            _RpcProxy(
                self._protocol,
                self._transport,
                self._on_log,
                external_config=self._external_config,
                ipc_validation=self._ipc_validation,
            ),
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the transport."""
        if wire_transport_logger.isEnabledFor(logging.DEBUG):
            wire_transport_logger.debug("RpcConnection close: protocol=%s", self._protocol.__name__)
        self._transport.close()
