# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Native Iroh client transports backed by the official ``iroh`` package.

The binding is optional and loaded only when an Iroh endpoint is used.  No
connector executable is downloaded or launched.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import importlib
import os
import re
import secrets
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum
from io import BufferedReader, IOBase, RawIOBase
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import unquote_to_bytes, urlsplit

from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.log import Message
from vgi_rpc.rpc import RpcConnection
from vgi_rpc.utils import IpcValidation

if TYPE_CHECKING:
    from collections.abc import Coroutine

    import httpx2

    from vgi_rpc.http import HttpRetryConfig


IROH_ARROW_MUX_ALPN = b"vgi-rpc/arrow-mux/1"
IROH_HTTP_ALPN = b"iroh-http/2"

_ENDPOINT_RE = re.compile(r"[0-9a-f]{64}\Z")
_PERCENT_SEPARATOR_RE = re.compile(r"%(?:2f|2F|5c|5C)")
_ZBASE32_ALPHABET = "ybndrfg8ejkmcpqxot1uwisza345h769"
_ZBASE32_VALUES = {character: value for value, character in enumerate(_ZBASE32_ALPHABET)}
_IO_CHUNK = 1024 * 1024
_CANCEL_POLL_SECONDS = 0.05
_MAX_HTTP_HEAD = 64 * 1024
_HTTP_TOKEN_RE = re.compile(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")


class IrohErrorStage(StrEnum):
    """Stage at which an Iroh transport operation failed."""

    PARSE = "parse"
    BIND = "bind"
    RESOLVE = "resolve"
    CONNECT = "connect"
    ALPN = "alpn"
    OPEN_STREAM = "open_stream"
    WRITE = "write"
    READ = "read"
    CANCEL = "cancel"
    CLOSE = "close"


class IrohErrorCategory(StrEnum):
    """Portable category for an Iroh transport failure."""

    INVALID_INPUT = "invalid_input"
    UNSUPPORTED = "unsupported"
    UNAVAILABLE = "unavailable"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    AUTHENTICATION = "authentication"
    PROTOCOL = "protocol"
    CONNECTION_RESET = "connection_reset"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    INTERNAL = "internal"


class DispatchCertainty(StrEnum):
    """Whether application request bytes may have reached the worker."""

    NOT_SENT = "not_sent"
    UNKNOWN = "unknown"
    SENT = "sent"


class IrohTransportError(ConnectionResetError):
    """Structured failure raised by the native Iroh byte transport."""

    stage: IrohErrorStage
    category: IrohErrorCategory
    dispatch_certainty: DispatchCertainty

    def __init__(
        self,
        message: str,
        *,
        stage: IrohErrorStage,
        category: IrohErrorCategory,
        dispatch_certainty: DispatchCertainty,
    ) -> None:
        """Initialize a transport failure without including private key material."""
        super().__init__(message)
        self.stage = stage
        self.category = category
        self.dispatch_certainty = dispatch_certainty


class IrohUriError(ValueError):
    """Canonical URI parse failure with the portable error dimensions."""

    stage = IrohErrorStage.PARSE
    category = IrohErrorCategory.INVALID_INPUT
    dispatch_certainty = DispatchCertainty.NOT_SENT


@dataclass(frozen=True, slots=True)
class IrohTarget:
    """Validated Iroh URI components."""

    scheme: str
    endpoint_id: bytes
    base_path: str = ""

    @property
    def endpoint_hex(self) -> str:
        """Return the canonical lowercase hexadecimal EndpointId."""
        return self.endpoint_id.hex()


def parse_iroh_uri(uri: str) -> IrohTarget:
    """Parse a canonical ``iroh://`` or ``httpi://`` endpoint URI.

    Args:
        uri: Endpoint URI to validate.

    Returns:
        Canonical endpoint components.

    Raises:
        IrohUriError: If the URI is not canonical or contains unsupported parts.

    """
    if (
        not isinstance(uri, str)
        or not uri
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in uri)
    ):
        raise IrohUriError("Iroh endpoint URI must be non-empty text without whitespace or controls")
    if not uri.startswith(("iroh://", "httpi://")):
        raise IrohUriError("Iroh endpoint URI scheme must be exactly 'iroh' or 'httpi'")
    parsed = urlsplit(uri)
    if parsed.scheme not in {"iroh", "httpi"} or parsed.query or parsed.fragment:
        raise IrohUriError("Iroh endpoint URI must not contain a query or fragment")
    if parsed.username is not None or parsed.password is not None:
        raise IrohUriError("Iroh endpoint URI must not contain user information")
    try:
        port = parsed.port
    except ValueError as exc:
        raise IrohUriError("Iroh endpoint URI must not contain a port") from exc
    if port is not None or parsed.netloc != parsed.hostname:
        raise IrohUriError("Iroh endpoint URI authority must contain only an EndpointId")
    endpoint = parsed.hostname or ""
    if _ENDPOINT_RE.fullmatch(endpoint) is None:
        raise IrohUriError("Iroh EndpointId must be exactly 64 lowercase hexadecimal characters")

    if parsed.scheme == "iroh":
        if parsed.path:
            raise IrohUriError("iroh:// endpoints must not contain a path")
        return IrohTarget("iroh", bytes.fromhex(endpoint))

    path = parsed.path
    if path == "/":
        path = ""
    if "\\" in path or _PERCENT_SEPARATOR_RE.search(path):
        raise IrohUriError("httpi:// base paths must not contain encoded separators or backslashes")
    if path:
        segments = path[1:].split("/")
        if not path.startswith("/") or any(segment in {"", ".", ".."} for segment in segments):
            raise IrohUriError("httpi:// base path must be canonical and contain no empty or dot segments")
        for segment in segments:
            if "%" in re.sub(r"%[0-9A-Fa-f]{2}", "", segment):
                raise IrohUriError("httpi:// base path contains an invalid percent escape")
            decoded = unquote_to_bytes(segment)
            if decoded in {b".", b".."} or b"/" in decoded or b"\\" in decoded:
                raise IrohUriError("httpi:// base path contains an encoded dot or separator segment")
    return IrohTarget("httpi", bytes.fromhex(endpoint), path)


def _decode_secret_key(value: bytes | str | None) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        if len(value) != 32:
            raise ValueError("Iroh secret key must contain exactly 32 bytes")
        return value
    if not isinstance(value, str):
        raise TypeError("Iroh secret key must be bytes, hexadecimal text, or z-base-32 text")
    if re.fullmatch(r"[0-9a-f]{64}", value):
        return bytes.fromhex(value)
    if len(value) != 52 or any(character not in _ZBASE32_VALUES for character in value):
        raise ValueError("Iroh secret key must be 64 lowercase hex or 52-character z-base-32")
    accumulator = 0
    bit_count = 0
    output = bytearray()
    for character in value:
        accumulator = (accumulator << 5) | _ZBASE32_VALUES[character]
        bit_count += 5
        while bit_count >= 8:
            bit_count -= 8
            output.append((accumulator >> bit_count) & 0xFF)
    if len(output) != 32 or (bit_count and accumulator & ((1 << bit_count) - 1)):
        raise ValueError("Iroh z-base-32 secret key has non-zero padding bits")
    return bytes(output)


def _load_iroh() -> ModuleType:
    try:
        return importlib.import_module("iroh")
    except ImportError as exc:
        raise IrohTransportError(
            "Native Iroh support requires the optional 'iroh' package; install vgi-rpc[iroh]",
            stage=IrohErrorStage.BIND,
            category=IrohErrorCategory.UNSUPPORTED,
            dispatch_certainty=DispatchCertainty.NOT_SENT,
        ) from exc


class _IrohRuntime:
    """One process-wide asyncio loop for UniFFI's asynchronous API."""

    def __init__(self, api: ModuleType) -> None:
        self._api = api
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, name="vgi-iroh", daemon=True)
        self._thread.start()
        self._ready.wait()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        ffi = getattr(self._api, "iroh_ffi", None)
        set_loop = getattr(ffi, "uniffi_set_event_loop", None)
        if callable(set_loop):
            set_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()

    def call[T](
        self,
        awaitable: Coroutine[Any, Any, T],
        *,
        timeout: float | None,
        cancellation: threading.Event | None,
        stage: IrohErrorStage,
        certainty: DispatchCertainty,
    ) -> T:
        future = asyncio.run_coroutine_threadsafe(awaitable, self._loop)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if cancellation is not None and cancellation.is_set():
                future.cancel()
                raise IrohTransportError(
                    f"Iroh operation cancelled during {stage.value}",
                    stage=IrohErrorStage.CANCEL,
                    category=IrohErrorCategory.CANCELLED,
                    dispatch_certainty=certainty,
                )
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                future.cancel()
                raise IrohTransportError(
                    f"Iroh {stage.value} timed out",
                    stage=stage,
                    category=IrohErrorCategory.TIMEOUT,
                    dispatch_certainty=certainty,
                )
            wait_for = _CANCEL_POLL_SECONDS if remaining is None else min(_CANCEL_POLL_SECONDS, remaining)
            try:
                return future.result(timeout=wait_for)
            except concurrent.futures.TimeoutError:
                continue
            except IrohTransportError:
                raise
            except BaseException as exc:
                raise IrohTransportError(
                    f"Iroh {stage.value} failed: {type(exc).__name__}",
                    stage=stage,
                    category=_categorize_exception(exc),
                    dispatch_certainty=certainty,
                ) from exc


_RUNTIME_LOCK = threading.Lock()
_RUNTIME: _IrohRuntime | None = None
_EPHEMERAL_KEY_LOCK = threading.Lock()
_EPHEMERAL_KEY: bytes | None = None
_EPHEMERAL_KEY_PID: int | None = None


def _runtime(api: ModuleType) -> _IrohRuntime:
    global _RUNTIME
    with _RUNTIME_LOCK:
        if _RUNTIME is None:
            _RUNTIME = _IrohRuntime(api)
        return _RUNTIME


def _process_ephemeral_key() -> bytes:
    """Return one ephemeral identity per process, regenerating after fork."""
    global _EPHEMERAL_KEY, _EPHEMERAL_KEY_PID
    process_id = os.getpid()
    with _EPHEMERAL_KEY_LOCK:
        if _EPHEMERAL_KEY is None or process_id != _EPHEMERAL_KEY_PID:
            _EPHEMERAL_KEY = secrets.token_bytes(32)
            _EPHEMERAL_KEY_PID = process_id
        return _EPHEMERAL_KEY


def _categorize_exception(exc: BaseException) -> IrohErrorCategory:
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    if "cancel" in name or "cancel" in text:
        return IrohErrorCategory.CANCELLED
    if "timeout" in name or "timed out" in text:
        return IrohErrorCategory.TIMEOUT
    if "alpn" in text or "protocol" in text:
        return IrohErrorCategory.PROTOCOL
    if "authentication" in text or "certificate" in text or "key mismatch" in text:
        return IrohErrorCategory.AUTHENTICATION
    if "reset" in text or "closed" in text or "connection lost" in text:
        return IrohErrorCategory.CONNECTION_RESET
    if "capacity" in text or "resource" in text:
        return IrohErrorCategory.RESOURCE_EXHAUSTED
    return IrohErrorCategory.UNAVAILABLE


@dataclass(slots=True)
class _NativeSession:
    api: ModuleType
    runtime: _IrohRuntime
    endpoint: Any
    connection: Any
    send: Any
    recv: Any
    io_timeout: float | None
    cancellation: threading.Event | None
    closed: bool = False

    def call[T](self, awaitable: Coroutine[Any, Any, T], stage: IrohErrorStage, certainty: DispatchCertainty) -> T:
        return self.runtime.call(
            awaitable,
            timeout=self.io_timeout,
            cancellation=self.cancellation,
            stage=stage,
            certainty=certainty,
        )


class _IrohReader(RawIOBase):
    def __init__(self, session: _NativeSession) -> None:
        super().__init__()
        self._session = session

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Any, /) -> int:
        if self.closed:
            return 0
        view = memoryview(buffer).cast("B")
        if not view:
            return 0
        size = min(len(view), _IO_CHUNK)
        data = self._session.call(
            self._session.recv.read(size),
            IrohErrorStage.READ,
            DispatchCertainty.SENT,
        )
        view[: len(data)] = data
        return len(data)


class _IrohWriter(RawIOBase):
    def __init__(self, session: _NativeSession) -> None:
        super().__init__()
        self._session = session
        self._finished = False

    def writable(self) -> bool:
        return True

    def write(self, buffer: Any, /) -> int:
        if self.closed or self._finished:
            raise BrokenPipeError("Iroh send stream is closed")
        data = bytes(memoryview(buffer).cast("B"))
        if data:
            self._session.call(
                self._session.send.write_all(data),
                IrohErrorStage.WRITE,
                DispatchCertainty.UNKNOWN,
            )
        return len(data)

    def finish(self) -> None:
        if not self._finished:
            self._session.call(
                self._session.send.finish(),
                IrohErrorStage.CLOSE,
                DispatchCertainty.SENT,
            )
            self._finished = True

    def close(self) -> None:
        if not self.closed:
            with contextlib.suppress(IrohTransportError):
                self.finish()
        super().close()


def _validate_options(
    *,
    relay_urls: Sequence[str] | None,
    no_relay: bool,
    connect_timeout: float | None,
    io_timeout: float | None,
) -> None:
    if relay_urls and no_relay:
        raise ValueError("relay_urls and no_relay are mutually exclusive")
    if relay_urls is not None and (not relay_urls or any(not isinstance(url, str) or not url for url in relay_urls)):
        raise ValueError("relay_urls must be a non-empty sequence of non-empty URLs")
    if connect_timeout is not None and connect_timeout <= 0:
        raise ValueError("connect_timeout must be positive or None")
    if io_timeout is not None and io_timeout <= 0:
        raise ValueError("io_timeout must be positive or None")


def _bind_endpoint(
    target: IrohTarget,
    *,
    secret_key: bytes | str | None,
    relay_urls: Sequence[str] | None,
    no_relay: bool,
    direct_addresses: Sequence[str],
    remote_relay_url: str | None,
    connect_timeout: float | None,
    io_timeout: float | None,
    cancellation: threading.Event | None,
    alpn: bytes,
    open_stream: bool = True,
) -> _NativeSession:
    _validate_options(
        relay_urls=relay_urls,
        no_relay=no_relay,
        connect_timeout=connect_timeout,
        io_timeout=io_timeout,
    )
    api = _load_iroh()
    runtime = _runtime(api)
    key = _decode_secret_key(secret_key) or _process_ephemeral_key()
    relay_mode = None
    if no_relay:
        relay_mode = api.RelayMode.disabled()
    elif relay_urls is not None:
        relay_mode = api.RelayMode.custom_from_urls(list(relay_urls))
    options = api.EndpointOptions(secret_key=key, relay_mode=relay_mode)
    endpoint = runtime.call(
        api.Endpoint.bind(options),
        timeout=connect_timeout,
        cancellation=cancellation,
        stage=IrohErrorStage.BIND,
        certainty=DispatchCertainty.NOT_SENT,
    )
    try:
        endpoint_id = api.EndpointId.from_bytes(target.endpoint_id)
        address = api.EndpointAddr(endpoint_id, remote_relay_url, list(direct_addresses))
        connection = runtime.call(
            endpoint.connect(address, alpn),
            timeout=connect_timeout,
            cancellation=cancellation,
            stage=IrohErrorStage.CONNECT,
            certainty=DispatchCertainty.NOT_SENT,
        )
        if bytes(connection.alpn()) != alpn:
            raise IrohTransportError(
                "Iroh peer negotiated an unexpected ALPN",
                stage=IrohErrorStage.ALPN,
                category=IrohErrorCategory.PROTOCOL,
                dispatch_certainty=DispatchCertainty.NOT_SENT,
            )
        stream = None
        if open_stream:
            stream = runtime.call(
                connection.open_bi(),
                timeout=connect_timeout,
                cancellation=cancellation,
                stage=IrohErrorStage.OPEN_STREAM,
                certainty=DispatchCertainty.NOT_SENT,
            )
        return _NativeSession(
            api,
            runtime,
            endpoint,
            connection,
            None if stream is None else stream.send(),
            None if stream is None else stream.recv(),
            io_timeout,
            cancellation,
        )
    except BaseException:
        with contextlib.suppress(BaseException):
            runtime.call(
                endpoint.close(),
                timeout=connect_timeout,
                cancellation=None,
                stage=IrohErrorStage.CLOSE,
                certainty=DispatchCertainty.NOT_SENT,
            )
        raise


class IrohTransport:
    """Synchronous VGI byte stream carried by one native Iroh bi-stream."""

    def __init__(
        self,
        endpoint: str,
        *,
        secret_key: bytes | str | None = None,
        relay_urls: Sequence[str] | None = None,
        no_relay: bool = False,
        direct_addresses: Sequence[str] = (),
        remote_relay_url: str | None = None,
        connect_timeout: float | None = 30.0,
        io_timeout: float | None = 300.0,
        cancellation: threading.Event | None = None,
    ) -> None:
        """Connect a raw VGI client using the official Iroh Python binding."""
        target = parse_iroh_uri(endpoint)
        if target.scheme != "iroh":
            raise ValueError("IrohTransport requires an iroh:// endpoint")
        self._session = _bind_endpoint(
            target,
            secret_key=secret_key,
            relay_urls=relay_urls,
            no_relay=no_relay,
            direct_addresses=direct_addresses,
            remote_relay_url=remote_relay_url,
            connect_timeout=connect_timeout,
            io_timeout=io_timeout,
            cancellation=cancellation,
            alpn=IROH_ARROW_MUX_ALPN,
        )
        self._reader = cast("IOBase", BufferedReader(_IrohReader(self._session), buffer_size=64 * 1024))
        self._writer = cast("IOBase", _IrohWriter(self._session))

    @property
    def reader(self) -> IOBase:
        """Readable blocking byte stream."""
        return self._reader

    @property
    def writer(self) -> IOBase:
        """Writable blocking byte stream."""
        return self._writer

    @property
    def local_endpoint_id(self) -> str:
        """Return the public local EndpointId; private key material is never exposed."""
        endpoint_bytes = cast("bytes", self._session.endpoint.id().to_bytes())
        return endpoint_bytes.hex()

    def close(self) -> None:
        """Finish the stream and close the Iroh endpoint."""
        if self._session.closed:
            return
        self._session.closed = True
        with contextlib.suppress(BaseException):
            self._writer.close()
        with contextlib.suppress(BaseException):
            self._reader.close()
        with contextlib.suppress(BaseException):
            self._session.connection.close(0, b"vgi client closed")
        with contextlib.suppress(BaseException):
            self._session.runtime.call(
                self._session.endpoint.close(),
                timeout=self._session.io_timeout,
                cancellation=None,
                stage=IrohErrorStage.CLOSE,
                certainty=DispatchCertainty.SENT,
            )


@contextlib.contextmanager
def iroh_connect[P](
    protocol: type[P],
    endpoint: str,
    *,
    secret_key: bytes | str | None = None,
    relay_urls: Sequence[str] | None = None,
    no_relay: bool = False,
    direct_addresses: Sequence[str] = (),
    remote_relay_url: str | None = None,
    connect_timeout: float | None = 30.0,
    io_timeout: float | None = 300.0,
    cancellation: threading.Event | None = None,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
) -> Iterator[P]:
    """Connect a typed VGI proxy to an ``iroh://`` worker."""
    transport = IrohTransport(
        endpoint,
        secret_key=secret_key,
        relay_urls=relay_urls,
        no_relay=no_relay,
        direct_addresses=direct_addresses,
        remote_relay_url=remote_relay_url,
        connect_timeout=connect_timeout,
        io_timeout=io_timeout,
        cancellation=cancellation,
    )
    try:
        with RpcConnection(
            protocol,
            transport,
            on_log=on_log,
            external_location=external_location,
            ipc_validation=ipc_validation,
        ) as proxy:
            yield proxy
    finally:
        transport.close()


def _read_http_response(
    session: _NativeSession,
    recv: Any,
    request_method: str,
) -> tuple[int, list[tuple[bytes, bytes]], bytes]:
    """Read one bounded-head HTTP/1.1 response without waiting for stream EOF."""
    buffered = bytearray()

    def receive() -> None:
        chunk = session.call(recv.read(_IO_CHUNK), IrohErrorStage.READ, DispatchCertainty.SENT)
        if not chunk:
            raise IrohTransportError(
                "Iroh HTTP response ended before its declared framing was complete",
                stage=IrohErrorStage.READ,
                category=IrohErrorCategory.PROTOCOL,
                dispatch_certainty=DispatchCertainty.SENT,
            )
        buffered.extend(chunk)

    while b"\r\n\r\n" not in buffered:
        if len(buffered) > _MAX_HTTP_HEAD:
            raise IrohTransportError(
                "Iroh HTTP response head exceeds 65536 bytes",
                stage=IrohErrorStage.READ,
                category=IrohErrorCategory.RESOURCE_EXHAUSTED,
                dispatch_certainty=DispatchCertainty.SENT,
            )
        receive()
    head, initial_body = bytes(buffered).split(b"\r\n\r\n", 1)
    buffered[:] = initial_body
    lines = head.split(b"\r\n")
    status_parts = lines[0].split(b" ", 2)
    if len(status_parts) < 2 or status_parts[0] not in {b"HTTP/1.0", b"HTTP/1.1"}:
        raise IrohTransportError(
            "Iroh HTTP response has an invalid status line",
            stage=IrohErrorStage.READ,
            category=IrohErrorCategory.PROTOCOL,
            dispatch_certainty=DispatchCertainty.SENT,
        )
    try:
        status = int(status_parts[1])
    except ValueError as exc:
        raise IrohTransportError(
            "Iroh HTTP response has an invalid status code",
            stage=IrohErrorStage.READ,
            category=IrohErrorCategory.PROTOCOL,
            dispatch_certainty=DispatchCertainty.SENT,
        ) from exc
    if not 100 <= status <= 999 or len(status_parts[1]) != 3:
        raise IrohTransportError(
            "Iroh HTTP response has an invalid status code",
            stage=IrohErrorStage.READ,
            category=IrohErrorCategory.PROTOCOL,
            dispatch_certainty=DispatchCertainty.SENT,
        )

    headers: list[tuple[bytes, bytes]] = []
    for line in lines[1:]:
        if line[:1] in {b" ", b"\t"} or b":" not in line:
            raise IrohTransportError(
                "Iroh HTTP response contains malformed or folded headers",
                stage=IrohErrorStage.READ,
                category=IrohErrorCategory.PROTOCOL,
                dispatch_certainty=DispatchCertainty.SENT,
            )
        name, value = line.split(b":", 1)
        if _HTTP_TOKEN_RE.fullmatch(name) is None or any(byte < 0x20 and byte != 0x09 for byte in value):
            raise IrohTransportError(
                "Iroh HTTP response contains an invalid header",
                stage=IrohErrorStage.READ,
                category=IrohErrorCategory.PROTOCOL,
                dispatch_certainty=DispatchCertainty.SENT,
            )
        headers.append((name, value.strip()))

    transfer_values = [value.lower() for name, value in headers if name.lower() == b"transfer-encoding"]
    length_values = [value for name, value in headers if name.lower() == b"content-length"]
    no_body = request_method == "HEAD" or status in {204, 304} or 100 <= status < 200

    def take_line() -> bytes:
        while b"\r\n" not in buffered:
            receive()
        line, remainder = bytes(buffered).split(b"\r\n", 1)
        buffered[:] = remainder
        return line

    if no_body:
        body = b""
    elif transfer_values:
        combined = b",".join(transfer_values).replace(b" ", b"")
        if combined != b"chunked" or length_values:
            raise IrohTransportError(
                "Iroh HTTP response has ambiguous transfer framing",
                stage=IrohErrorStage.READ,
                category=IrohErrorCategory.PROTOCOL,
                dispatch_certainty=DispatchCertainty.SENT,
            )
        decoded = bytearray()
        while True:
            size_text = take_line().split(b";", 1)[0]
            try:
                size = int(size_text, 16)
            except ValueError as exc:
                raise IrohTransportError(
                    "Iroh HTTP response has an invalid chunk size",
                    stage=IrohErrorStage.READ,
                    category=IrohErrorCategory.PROTOCOL,
                    dispatch_certainty=DispatchCertainty.SENT,
                ) from exc
            if size < 0:
                raise IrohTransportError(
                    "Iroh HTTP response has a negative chunk size",
                    stage=IrohErrorStage.READ,
                    category=IrohErrorCategory.PROTOCOL,
                    dispatch_certainty=DispatchCertainty.SENT,
                )
            if size == 0:
                while take_line():
                    pass
                break
            while len(buffered) < size + 2:
                receive()
            decoded.extend(buffered[:size])
            if buffered[size : size + 2] != b"\r\n":
                raise IrohTransportError(
                    "Iroh HTTP response chunk is missing its terminator",
                    stage=IrohErrorStage.READ,
                    category=IrohErrorCategory.PROTOCOL,
                    dispatch_certainty=DispatchCertainty.SENT,
                )
            del buffered[: size + 2]
        body = bytes(decoded)
    elif length_values:
        if len(set(length_values)) != 1:
            raise IrohTransportError(
                "Iroh HTTP response has conflicting Content-Length fields",
                stage=IrohErrorStage.READ,
                category=IrohErrorCategory.PROTOCOL,
                dispatch_certainty=DispatchCertainty.SENT,
            )
        try:
            length = int(length_values[0])
        except ValueError as exc:
            raise IrohTransportError(
                "Iroh HTTP response has an invalid Content-Length",
                stage=IrohErrorStage.READ,
                category=IrohErrorCategory.PROTOCOL,
                dispatch_certainty=DispatchCertainty.SENT,
            ) from exc
        if length < 0:
            raise IrohTransportError(
                "Iroh HTTP response has a negative Content-Length",
                stage=IrohErrorStage.READ,
                category=IrohErrorCategory.PROTOCOL,
                dispatch_certainty=DispatchCertainty.SENT,
            )
        while len(buffered) < length:
            receive()
        body = bytes(buffered[:length])
    else:
        while True:
            chunk = session.call(recv.read(_IO_CHUNK), IrohErrorStage.READ, DispatchCertainty.SENT)
            if not chunk:
                break
            buffered.extend(chunk)
        body = bytes(buffered)

    normalized_headers = [
        (name, value)
        for name, value in headers
        if name.lower() not in {b"transfer-encoding", b"content-length", b"connection"}
    ]
    normalized_headers.append((b"Content-Length", str(len(body)).encode("ascii")))
    return status, normalized_headers, body


class IrohHttpTransport:
    """``httpx2`` transport implementing HTTP/1.1 over native Iroh streams."""

    def __init__(
        self,
        endpoint: str,
        *,
        secret_key: bytes | str | None = None,
        relay_urls: Sequence[str] | None = None,
        no_relay: bool = False,
        direct_addresses: Sequence[str] = (),
        remote_relay_url: str | None = None,
        connect_timeout: float | None = 30.0,
        io_timeout: float | None = 300.0,
        cancellation: threading.Event | None = None,
    ) -> None:
        """Connect an HTTP-over-Iroh client and retain the negotiated connection."""
        import httpx2

        target = parse_iroh_uri(endpoint)
        if target.scheme != "httpi":
            raise ValueError("IrohHttpTransport requires an httpi:// endpoint")
        self._httpx = httpx2
        self._target = target
        self._session = _bind_endpoint(
            target,
            secret_key=secret_key,
            relay_urls=relay_urls,
            no_relay=no_relay,
            direct_addresses=direct_addresses,
            remote_relay_url=remote_relay_url,
            connect_timeout=connect_timeout,
            io_timeout=io_timeout,
            cancellation=cancellation,
            alpn=IROH_HTTP_ALPN,
            open_stream=False,
        )

    def __enter__(self) -> IrohHttpTransport:
        """Open the transport for ``httpx2.Client`` context management."""
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: object | None,
    ) -> None:
        """Close the transport for ``httpx2.Client`` context management."""
        self.close()

    def handle_request(self, request: httpx2.Request) -> httpx2.Response:
        """Execute one HTTP exchange on a fresh stream of the pooled connection."""
        certainty = DispatchCertainty.NOT_SENT
        try:
            stream = self._session.call(
                self._session.connection.open_bi(),
                IrohErrorStage.OPEN_STREAM,
                DispatchCertainty.NOT_SENT,
            )
            send = stream.send()
            recv = stream.recv()
            body = request.read()
            headers = [(bytes(name), bytes(value)) for name, value in request.headers.raw]
            headers = [
                (name, value)
                for name, value in headers
                if name.lower() not in {b"content-length", b"transfer-encoding", b"connection"}
            ]
            headers.extend(((b"Content-Length", str(len(body)).encode("ascii")), (b"Connection", b"close")))
            target = request.url.raw_path
            head = request.method.encode("ascii") + b" " + target + b" HTTP/1.1\r\n"
            head += b"".join(name + b": " + value + b"\r\n" for name, value in headers) + b"\r\n"
            certainty = DispatchCertainty.UNKNOWN
            self._session.call(send.write_all(head + body), IrohErrorStage.WRITE, certainty)
            self._session.call(send.finish(), IrohErrorStage.WRITE, certainty)
            certainty = DispatchCertainty.SENT
            status, response_headers, response_body = _read_http_response(
                self._session,
                recv,
                request.method,
            )
            return self._httpx.Response(
                status,
                headers=response_headers,
                content=response_body,
                request=request,
            )
        except IrohTransportError as exc:
            if exc.category == IrohErrorCategory.TIMEOUT:
                if certainty == DispatchCertainty.NOT_SENT:
                    raise self._httpx.ConnectTimeout(str(exc), request=request) from exc
                raise self._httpx.ReadTimeout(str(exc), request=request) from exc
            if certainty == DispatchCertainty.NOT_SENT:
                raise self._httpx.ConnectError(str(exc), request=request) from exc
            raise self._httpx.ReadError(str(exc), request=request) from exc
        except BaseException as exc:
            raise self._httpx.ReadError(
                f"Iroh HTTP exchange failed during {certainty.value}: {type(exc).__name__}",
                request=request,
            ) from exc

    def close(self) -> None:
        """Close the retained Iroh connection and local endpoint."""
        if self._session.closed:
            return
        self._session.closed = True
        with contextlib.suppress(BaseException):
            self._session.connection.close(0, b"vgi http client closed")
        with contextlib.suppress(BaseException):
            self._session.runtime.call(
                self._session.endpoint.close(),
                timeout=self._session.io_timeout,
                cancellation=None,
                stage=IrohErrorStage.CLOSE,
                certainty=DispatchCertainty.SENT,
            )


@contextlib.contextmanager
def httpi_connect[P](
    protocol: type[P],
    endpoint: str,
    *,
    secret_key: bytes | str | None = None,
    relay_urls: Sequence[str] | None = None,
    no_relay: bool = False,
    direct_addresses: Sequence[str] = (),
    remote_relay_url: str | None = None,
    connect_timeout: float | None = 30.0,
    io_timeout: float | None = 300.0,
    cancellation: threading.Event | None = None,
    on_log: Callable[[Message], None] | None = None,
    external_location: ExternalLocationConfig | None = None,
    ipc_validation: IpcValidation = IpcValidation.FULL,
    retry: HttpRetryConfig | None = None,
    compression_level: int | None = 1,
    accepted_max_response_bytes: int | None = 256 * 1024 * 1024,
) -> Iterator[P]:
    """Connect a typed VGI HTTP proxy through ``iroh-http/2``."""
    import httpx2

    from vgi_rpc.http import http_connect

    target = parse_iroh_uri(endpoint)
    if target.scheme != "httpi":
        raise ValueError("httpi_connect requires an httpi:// endpoint")
    transport = IrohHttpTransport(
        endpoint,
        secret_key=secret_key,
        relay_urls=relay_urls,
        no_relay=no_relay,
        direct_addresses=direct_addresses,
        remote_relay_url=remote_relay_url,
        connect_timeout=connect_timeout,
        io_timeout=io_timeout,
        cancellation=cancellation,
    )
    client = httpx2.Client(base_url=f"http://{target.endpoint_hex}", transport=cast("httpx2.BaseTransport", transport))
    try:
        with http_connect(
            protocol,
            client=client,
            prefix=target.base_path,
            on_log=on_log,
            external_location=external_location,
            ipc_validation=ipc_validation,
            retry=retry,
            compression_level=compression_level,
            accepted_max_response_bytes=accepted_max_response_bytes,
        ) as proxy:
            yield proxy
    finally:
        client.close()


@contextlib.contextmanager
def endpoint_connect[P](protocol: type[P], endpoint: str, **options: Any) -> Iterator[P]:
    """Dispatch an Iroh endpoint URI to its native raw or HTTP client."""
    target = parse_iroh_uri(endpoint)
    connector = iroh_connect if target.scheme == "iroh" else httpi_connect
    with connector(protocol, endpoint, **options) as proxy:
        yield proxy


__all__ = [
    "DispatchCertainty",
    "IROH_ARROW_MUX_ALPN",
    "IROH_HTTP_ALPN",
    "IrohErrorCategory",
    "IrohErrorStage",
    "IrohHttpTransport",
    "IrohTarget",
    "IrohTransport",
    "IrohTransportError",
    "IrohUriError",
    "endpoint_connect",
    "httpi_connect",
    "iroh_connect",
    "parse_iroh_uri",
]
