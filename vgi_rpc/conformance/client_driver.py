# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Drive a foreign vgi-rpc *client* through a JSONL driver subprocess.

The conformance suite normally points the reference Python client at a foreign
*server*.  This module is the other direction.  It reconstructs the proxy /
stream-session / session-view surface the imported conformance test bodies
expect and forwards every call to a driver subprocess written in the port under
test, over a small newline-delimited JSON control protocol.

It reuses the canonical Python value encoders and decoders
(``_send_request``, ``_read_unary_response``, ``_read_stream_header``,
:class:`~vgi_rpc.rpc.AnnotatedBatch`), so **no value marshaling happens here**:
the bytes crossing the control boundary are Arrow IPC streams plus a method
name, and the foreign client does all the real wire framing, transport I/O,
stream lockstep and log/error envelope parsing.  That is what makes the run a
test of the foreign client rather than of this shim.

The control protocol is specified in
``tools/cross-port/specs/CLIENT_DRIVER_PROTOCOL.md``.  That document, not this
module, is the contract a port implements against; this module is its reference
consumer.

Typical use from a port's own conformance harness::

    from vgi_rpc.conformance.client_driver import ClientDriver

    driver = ClientDriver.from_env(default=["./target/debug/my-driver"])
    driver.install_http_overrides()          # route http_connect & friends
    proxy = driver.connect("stdio", ["./my-conformance-worker"])
"""

from __future__ import annotations

import base64
import contextlib
import io
import json
import os
import shlex
import subprocess
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Self, cast

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc._codec import Encoding
from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.external import ExternalLocationConfig, UploadUrl
from vgi_rpc.introspect import MethodDescription, ServiceDescription
from vgi_rpc.log import Level, Message
from vgi_rpc.rpc import AnnotatedBatch, MethodType, RpcError
from vgi_rpc.rpc._types import RpcMethodInfo, _protocol_wire_name, rpc_methods
from vgi_rpc.rpc._wire import _read_stream_header, _read_unary_response, _send_request
from vgi_rpc.utils import ArrowSerializableDataclass, IpcValidation, ValidatedReader

if TYPE_CHECKING:  # pragma: no cover - import cycle / optional extra
    from vgi_rpc.http import HttpServerCapabilities

__all__ = [
    "DEFAULT_SHM_SIZE",
    "DRIVER_COMMAND_ENV_VAR",
    "ClientDriver",
    "ClientDriverProxy",
    "DriverSessionView",
    "DriverStreamSession",
]

#: Environment variable naming the driver executable (or full command line).
#: Every port's CI already sets this; ``shlex.split`` means an interpreted
#: driver (``bun run driver.ts``, ``java -jar driver.jar``) needs no wrapper
#: script.
DRIVER_COMMAND_ENV_VAR = "VGI_CLIENT_DRIVER"

#: Default size of the POSIX shared-memory side-channel, in bytes, when the
#: harness opens a ``shm`` connection without naming one.
DEFAULT_SHM_SIZE = 4 * 1024 * 1024

#: Request headers an HTTP client library installs for itself.  A driver sets
#: its own, so only headers the *caller* added are forwarded on ``connect``.
_FALLBACK_STOCK_HEADERS = frozenset({"accept", "accept-encoding", "connection", "host", "user-agent"})

type JsonObject = dict[str, object]


# ---------------------------------------------------------------------------
# JSON accessors.  The control channel is untyped by construction (a foreign
# process wrote it), so every read narrows explicitly rather than trusting it.
# ---------------------------------------------------------------------------


def _as_dict(value: object) -> JsonObject | None:
    """Return *value* as a JSON object, or ``None`` if it is not one.

    Args:
        value: A decoded JSON value.

    Returns:
        The value as a ``dict``, or ``None``.

    """
    if isinstance(value, dict):
        return cast("JsonObject", value)
    return None


def _as_list(value: object) -> list[object] | None:
    """Return *value* as a JSON array, or ``None`` if it is not one.

    Args:
        value: A decoded JSON value.

    Returns:
        The value as a ``list``, or ``None``.

    """
    if isinstance(value, list):
        return cast("list[object]", value)
    return None


def _as_str(value: object) -> str | None:
    """Return *value* as a string, or ``None`` if it is not one.

    Args:
        value: A decoded JSON value.

    Returns:
        The value as a ``str``, or ``None``.

    """
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    """Return *value* as an integer, or ``None`` if it is not one.

    ``bool`` is excluded: JSON has no integer/boolean overlap worth honouring
    here, and a driver that sent ``true`` for a byte cap has a bug.

    Args:
        value: A decoded JSON value.

    Returns:
        The value as an ``int``, or ``None``.

    """
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


def _as_str_map(value: object) -> dict[str, str]:
    """Return *value* as a flat string-to-string mapping.

    Non-string entries are dropped rather than coerced: a driver that put a
    number where the protocol says string should not have the harness paper
    over it.

    Args:
        value: A decoded JSON value.

    Returns:
        A ``dict[str, str]``; empty when *value* is not an object.

    """
    obj = _as_dict(value)
    if obj is None:
        return {}
    return {key: item for key, item in obj.items() if isinstance(item, str)}


def _b64e(data: bytes) -> str:
    """Base64-encode *data* for the control channel.

    Args:
        data: Raw bytes.

    Returns:
        Standard (padded) base64 ASCII text.

    """
    return base64.standard_b64encode(data).decode("ascii")


def _b64d(text: str) -> bytes:
    """Decode standard base64 text from the control channel.

    Args:
        text: Standard (padded) base64 ASCII text.

    Returns:
        The decoded bytes.

    """
    return base64.standard_b64decode(text.encode("ascii"))


def _serialize_batch(batch: pa.RecordBatch, custom_metadata: Mapping[bytes, bytes] | None) -> bytes:
    """Serialize one batch, plus optional custom metadata, as an IPC stream.

    Args:
        batch: The record batch to write.
        custom_metadata: Arrow custom metadata to attach, or ``None``.

    Returns:
        A complete Arrow IPC stream holding exactly one batch.

    """
    buf = io.BytesIO()
    with ipc.new_stream(buf, batch.schema) as writer:
        if custom_metadata:
            writer.write_batch(batch, custom_metadata=custom_metadata)
        else:
            writer.write_batch(batch)
    return buf.getvalue()


def _reader(data: bytes) -> ValidatedReader:
    """Open an Arrow IPC stream over *data* with full validation.

    Args:
        data: A complete Arrow IPC stream.

    Returns:
        A validating reader positioned at the first message.

    """
    return ValidatedReader(ipc.open_stream(io.BytesIO(data)), IpcValidation.FULL)


def _schema_from_ipc(encoded: object) -> pa.Schema:
    """Decode a base64 Arrow IPC stream down to its schema.

    Batches in the stream, if any, are ignored; only the schema message is
    read.  An absent or empty value yields the empty schema, so a driver need
    not distinguish "no schema" from "schema with no fields".

    Args:
        encoded: Base64 IPC stream text, or ``None``.

    Returns:
        The decoded Arrow schema.

    """
    text = _as_str(encoded)
    if not text:
        return pa.schema([])
    return ipc.open_stream(pa.py_buffer(_b64d(text))).schema


def _stock_header_names(client: object) -> frozenset[str]:
    """Return the header names *client*'s own library installs by default.

    Built by constructing a bare instance of the same client class, so this
    tracks whichever HTTP library the caller used rather than hard-coding one.
    Falls back to a known set when the class cannot be default-constructed.

    Args:
        client: A pre-built HTTP client object.

    Returns:
        Lower-cased header names to treat as library defaults.

    """
    try:
        probe = type(client)()
    except Exception:
        return _FALLBACK_STOCK_HEADERS
    try:
        headers = getattr(probe, "headers", None)
        names = frozenset(str(name).lower() for name in headers) if headers is not None else frozenset()
    finally:
        closer = getattr(probe, "close", None)
        if callable(closer):
            closer()
    return names or _FALLBACK_STOCK_HEADERS


def _target_and_headers(base_url: str | None, client: object) -> tuple[str, dict[str, str]]:
    """Resolve an ``http_connect``-style ``base_url`` / ``client`` pair.

    ``http_connect`` accepts either a URL or a pre-built HTTP client; the
    sticky cross-principal tests use the latter to pin an identity header.  A
    driver takes a URL plus default headers, so unwrap the client into that
    shape, forwarding only headers the caller actually added.

    Args:
        base_url: Base URL, used when *client* is ``None``.
        client: A pre-built HTTP client, or ``None``.

    Returns:
        The target URL and the caller-supplied default headers.

    Raises:
        ValueError: If neither *base_url* nor *client* was given.

    """
    if client is None:
        if base_url is None:
            raise ValueError("base_url is required when client is not provided")
        return base_url, {}
    stock = _stock_header_names(client)
    headers: dict[str, str] = {}
    client_headers = getattr(client, "headers", None)
    if client_headers is not None:
        for name, value in dict(client_headers).items():
            if str(name).lower() not in stock:
                headers[str(name)] = str(value)
    client_base = getattr(client, "base_url", None)
    target = str(client_base) if client_base is not None else base_url
    if target is None:
        raise ValueError("base_url is required when the client carries no base_url")
    return target, headers


class ClientDriver:
    """A foreign vgi-rpc client under test, reached through a driver subprocess.

    One instance describes *how to start* a driver; it holds no process of its
    own.  Every :meth:`connect` spawns a fresh driver, because the control
    protocol binds exactly one connection per driver process.
    """

    def __init__(
        self,
        command: Sequence[str],
        *,
        service: type = ConformanceService,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
    ) -> None:
        """Describe a driver command.

        Args:
            command: Argv of the driver executable.  A single-element list is
                the common case; an interpreted driver passes its interpreter
                and script.
            service: The Protocol class whose methods the proxy exposes and
                whose wire name becomes the routing key.
            env: Extra environment variables for the driver process, merged
                over the parent environment.
            cwd: Working directory for the driver process.

        Raises:
            ValueError: If *command* is empty.

        """
        if not tuple(command):
            raise ValueError("client driver command is empty")
        self.command: tuple[str, ...] = tuple(command)
        self.service = service
        self.env = dict(env) if env else None
        self.cwd = cwd

    @classmethod
    def from_env(
        cls,
        *,
        default: Sequence[str] | None = None,
        service: type = ConformanceService,
        env: Mapping[str, str] | None = None,
        cwd: str | None = None,
    ) -> Self:
        """Build a driver from ``VGI_CLIENT_DRIVER``, falling back to *default*.

        The variable is split with :func:`shlex.split`, so it may name a bare
        executable or a whole command line.

        Args:
            default: Argv to use when the variable is unset.
            service: The Protocol class the proxy is bound to.
            env: Extra environment variables for the driver process.
            cwd: Working directory for the driver process.

        Returns:
            A configured :class:`ClientDriver`.

        Raises:
            RuntimeError: If the variable is unset and no *default* was given.

        """
        raw = os.environ.get(DRIVER_COMMAND_ENV_VAR)
        if raw:
            return cls(shlex.split(raw), service=service, env=env, cwd=cwd)
        if default is None:
            raise RuntimeError(f"{DRIVER_COMMAND_ENV_VAR} is not set and no default driver command was given")
        return cls(default, service=service, env=env, cwd=cwd)

    def connect(
        self,
        transport: str,
        target: object,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
        compression_level: int | None = 1,
        headers: Mapping[str, str] | None = None,
        shm_size: int | None = None,
    ) -> ClientDriverProxy:
        """Spawn a driver and open one connection through it.

        Args:
            transport: One of ``stdio``, ``shm``, ``unix``, ``tcp``, ``http``.
            target: The transport's target — argv list, socket path,
                ``host:port``, or base URL.
            on_log: Callback for server log messages relayed by the driver.
            external_config: When not ``None``, the *driver's* client resolves
                external-location pointers.  Only its presence crosses the
                control boundary; resolving Python-side would mask the client
                under test.
            compression_level: Zstandard level for HTTP request bodies;
                ``None`` disables request compression.
            headers: Default request headers for every HTTP request.
            shm_size: Size of the shared-memory segment for ``shm``.

        Returns:
            A live proxy over the driver's connection.

        """
        return ClientDriverProxy(
            self,
            transport,
            target,
            on_log,
            external_config=external_config,
            compression_level=compression_level,
            headers=headers,
            shm_size=shm_size,
        )

    def spawn(self) -> subprocess.Popen[bytes]:
        """Start the driver subprocess with its control channel piped.

        Returns:
            The running process, with binary ``stdin``/``stdout`` pipes.

        """
        env = None
        if self.env is not None:
            env = {**os.environ, **self.env}
        return subprocess.Popen(
            list(self.command),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            bufsize=0,
            env=env,
            cwd=self.cwd,
        )

    # -- ``vgi_rpc.http`` stand-ins -----------------------------------------

    @contextlib.contextmanager
    def http_connect(
        self,
        protocol: type,
        base_url: str | None = None,
        *,
        on_log: Callable[[Message], None] | None = None,
        external_location: ExternalLocationConfig | None = None,
        compression_level: int | None = 1,
        prefix: str | None = None,
        client: object = None,
        **_ignored: object,
    ) -> Iterator[ClientDriverProxy]:
        """Stand in for :func:`vgi_rpc.http.http_connect`, routed through the driver.

        Args:
            protocol: The Protocol class — accepted for signature parity; the
                driver is bound to the service this :class:`ClientDriver`
                carries.
            base_url: Base URL of the server.
            on_log: Callback for relayed server log messages.
            external_location: When not ``None``, the driver's client resolves
                external-location pointers.
            compression_level: Zstandard level for request bodies.
            prefix: Accepted for signature parity and ignored — the control
                protocol carries a whole URL, not a mount prefix.
            client: A pre-built HTTP client whose base URL and caller-set
                headers are unwrapped for the driver.
            **_ignored: Remaining ``http_connect`` keywords, accepted and
                ignored; the conformance suite passes none of them.

        Yields:
            A proxy over the driver's HTTP connection.

        """
        del protocol, prefix
        target, headers = _target_and_headers(base_url, client)
        proxy = self.connect(
            "http",
            target,
            on_log,
            external_config=external_location,
            compression_level=compression_level,
            headers=headers,
        )
        try:
            yield proxy
        finally:
            proxy.close()

    def http_capabilities(
        self,
        base_url: str | None = None,
        *,
        prefix: str | None = None,
        client: object = None,
        **_ignored: object,
    ) -> HttpServerCapabilities:
        """Stand in for :func:`vgi_rpc.http.http_capabilities`.

        Args:
            base_url: Base URL of the server.
            prefix: Accepted for signature parity and ignored.
            client: A pre-built HTTP client to unwrap.
            **_ignored: Remaining keywords, accepted and ignored.

        Returns:
            The capabilities the driver's client discovered.

        """
        from vgi_rpc.http import HttpServerCapabilities

        del prefix
        target, headers = _target_and_headers(base_url, client)
        proxy = self.connect("http", target, headers=headers)
        try:
            caps = _as_dict(proxy.admin("capabilities").get("caps")) or {}
        finally:
            proxy.close()
        encodings: list[Encoding] = []
        for token in _as_list(caps.get("supported_encodings")) or []:
            name = _as_str(token)
            if name is not None:
                with contextlib.suppress(ValueError):
                    encodings.append(Encoding(name))
        echo_headers = tuple(
            name for name in (_as_str(item) for item in _as_list(caps.get("sticky_echo_headers")) or []) if name
        )
        return HttpServerCapabilities(
            max_request_bytes=_as_int(caps.get("max_request_bytes")),
            max_response_bytes=_as_int(caps.get("max_response_bytes")),
            max_externalized_response_bytes=_as_int(caps.get("max_externalized_response_bytes")),
            externalization_enabled=bool(caps.get("externalization_enabled")),
            upload_url_support=bool(caps.get("upload_url_support")),
            max_upload_bytes=_as_int(caps.get("max_upload_bytes")),
            supported_encodings=tuple(encodings),
            sticky_enabled=bool(caps.get("sticky_enabled")),
            sticky_default_ttl=_as_int(caps.get("sticky_default_ttl")),
            sticky_echo_headers=echo_headers,
        )

    def request_upload_urls(
        self,
        base_url: str | None = None,
        *,
        count: int = 1,
        prefix: str | None = None,
        client: object = None,
        **_ignored: object,
    ) -> list[UploadUrl]:
        """Stand in for :func:`vgi_rpc.http.request_upload_urls`.

        Args:
            base_url: Base URL of the server.
            count: Number of upload URL pairs to request.
            prefix: Accepted for signature parity and ignored.
            client: A pre-built HTTP client to unwrap.
            **_ignored: Remaining keywords, accepted and ignored.

        Returns:
            The URL pairs the driver's client obtained.

        """
        del prefix
        target, headers = _target_and_headers(base_url, client)
        proxy = self.connect("http", target, headers=headers)
        try:
            raw = _as_list(proxy.admin("request_upload_urls", count=count).get("urls")) or []
        finally:
            proxy.close()
        urls: list[UploadUrl] = []
        for item in raw:
            entry = _as_dict(item)
            if entry is None:
                continue
            urls.append(
                UploadUrl(
                    upload_url=_as_str(entry.get("upload_url")) or "",
                    download_url=_as_str(entry.get("download_url")) or "",
                    expires_at=_expires_at(entry.get("expires_at")),
                )
            )
        return urls

    def http_introspect(
        self,
        base_url: str | None = None,
        *,
        prefix: str | None = None,
        client: object = None,
        **_ignored: object,
    ) -> ServiceDescription:
        """Stand in for :func:`vgi_rpc.http.http_introspect`.

        Args:
            base_url: Base URL of the server.
            prefix: Accepted for signature parity and ignored.
            client: A pre-built HTTP client to unwrap.
            **_ignored: Remaining keywords, accepted and ignored.

        Returns:
            The service description the driver's client decoded.

        """
        del prefix
        target, headers = _target_and_headers(base_url, client)
        proxy = self.connect("http", target, headers=headers)
        try:
            return proxy.describe()
        finally:
            proxy.close()

    def install_http_overrides(self) -> None:
        """Route ``vgi_rpc.http``'s module-level entry points through this driver.

        The HTTP feature tests (external location, sticky sessions, response
        caps) import ``http_connect`` / ``http_capabilities`` /
        ``request_upload_urls`` inside the test body.  Without this they would
        quietly exercise the *Python* client and prove nothing about the port
        under test.
        """
        import vgi_rpc.http

        # Deliberately re-bound through an ``Any`` view of the module: these
        # stand-ins accept the keywords the conformance suite actually passes,
        # not every keyword the originals declare, so a structural check here
        # would only be satisfied by re-declaring parameters no test uses.
        patched: Any = vgi_rpc.http
        patched.http_connect = self.http_connect
        patched.http_capabilities = self.http_capabilities
        patched.request_upload_urls = self.request_upload_urls


def _expires_at(value: object) -> datetime:
    """Interpret an upload URL's ``expires_at`` field.

    The protocol asks for integer Unix seconds.  A missing or unparseable
    value becomes the epoch rather than an error: the conformance suite checks
    that URLs were vended, not when they lapse.

    Args:
        value: The decoded JSON value.

    Returns:
        A timezone-aware UTC timestamp.

    """
    seconds = _as_int(value)
    if seconds is not None:
        with contextlib.suppress(OverflowError, OSError, ValueError):
            return datetime.fromtimestamp(seconds, tz=UTC)
    text = _as_str(value)
    if text:
        with contextlib.suppress(ValueError):
            return datetime.fromisoformat(text)
    return datetime.fromtimestamp(0, tz=UTC)


class ClientDriverProxy:
    """Mimics the ``_RpcProxy`` surface, forwarding to a driver subprocess."""

    def __init__(
        self,
        driver: ClientDriver,
        transport: str,
        target: object,
        on_log: Callable[[Message], None] | None = None,
        *,
        external_config: ExternalLocationConfig | None = None,
        compression_level: int | None = 1,
        headers: Mapping[str, str] | None = None,
        shm_size: int | None = None,
    ) -> None:
        """Spawn the driver and issue ``connect``.

        Args:
            driver: The driver command to spawn.
            transport: One of ``stdio``, ``shm``, ``unix``, ``tcp``, ``http``.
            target: The transport's target.
            on_log: Callback for relayed server log messages.
            external_config: Presence enables external-location resolution in
                the driver's client.
            compression_level: Zstandard level for HTTP request bodies;
                ``None`` disables request compression.
            headers: Default request headers for every HTTP request.
            shm_size: Size of the shared-memory segment for ``shm``.

        """
        self._driver = driver
        self._methods = rpc_methods(driver.service)
        self._on_log = on_log
        self._external = external_config is not None
        self._compression_level = compression_level
        # The routing key.  Every request names the protocol it addresses -- a
        # single-protocol server is not an exemption, it refuses an unrouted
        # call like any other.  ``_send_request`` only stamps it when handed
        # ``protocol=``, so deriving it here is what keeps this shim speaking
        # the same wire as the canonical ``_RpcProxy``.
        self._protocol = _protocol_wire_name(driver.service)
        self._protocol_version = _as_str(vars(driver.service).get("protocol_version"))
        self._headers = dict(headers) if headers else {}
        self._proc = driver.spawn()
        self._connect(transport, target, shm_size)

    # -- control channel ----------------------------------------------------

    def _send(self, message: JsonObject) -> None:
        """Write one control request.

        Args:
            message: The request object.

        Raises:
            RpcError: If the driver's stdin is already gone.

        """
        stdin = self._proc.stdin
        if stdin is None:
            raise RpcError("TransportError", "client driver has no control channel", "")
        stdin.write((json.dumps(message) + "\n").encode("utf-8"))
        stdin.flush()

    def _recv(self) -> JsonObject:
        """Read one control response.

        Returns:
            The decoded response object.

        Raises:
            RpcError: If the driver closed the channel or wrote a non-object.

        """
        stdout = self._proc.stdout
        line = stdout.readline() if stdout is not None else b""
        if not line:
            raise RpcError("TransportError", "client driver closed the control channel", "")
        decoded = _as_dict(json.loads(line.decode("utf-8")))
        if decoded is None:
            raise RpcError("TransportError", f"client driver wrote a non-object response: {line!r}", "")
        return decoded

    def _connect(self, transport: str, target: object, shm_size: int | None) -> None:
        """Issue the ``connect`` op and check its acknowledgement.

        Args:
            transport: The transport name.
            target: The transport's target.
            shm_size: Shared-memory segment size for ``shm``.

        Raises:
            RpcError: If the driver could not connect.

        """
        request: JsonObject = {
            "op": "connect",
            "transport": transport,
            "target": target,
            # Bound once.  Over HTTP the reference server routes only
            # ``{protocol}/{method}``, so the driver needs this to build a
            # path that exists, not just a metadata key.
            "protocol": self._protocol,
            "external": self._external,
            "compression_level": self._compression_level,
            "headers": self._headers,
        }
        if transport == "shm":
            request["shm_size"] = shm_size if shm_size is not None else DEFAULT_SHM_SIZE
        self._send(request)
        resp = self._recv()
        if not resp.get("ok"):
            raise RpcError("TransportError", f"driver connect failed: {_driver_error_text(resp)}", "")

    def _replay_logs(self, logs: object) -> None:
        """Deliver relayed log records to the ``on_log`` callback.

        Args:
            logs: The response's ``logs`` array, or ``None``.

        """
        entries = _as_list(logs)
        if not entries or self._on_log is None:
            return
        for item in entries:
            entry = _as_dict(item)
            if entry is None:
                continue
            level = _as_str(entry.get("level")) or "INFO"
            message = _as_str(entry.get("message")) or ""
            self._on_log(Message(Level[level], message, **_as_str_map(entry.get("extra"))))

    @staticmethod
    def _raise_if_error(resp: JsonObject) -> None:
        """Re-raise a remote error the driver's client reported.

        Args:
            resp: A control response.

        Raises:
            RpcError: When the response carries an ``error``.

        """
        err = resp.get("error")
        if not err:
            return
        detail = _as_dict(err)
        if detail is None:
            # A driver that spelled a remote failure as a bare string.  Not the
            # contract, but losing the message would be worse than accepting it.
            raise RpcError("TransportError", _as_str(err) or repr(err), "")
        raise RpcError(
            _as_str(detail.get("error_type")) or "RpcError",
            # ``error_message`` is the contract; ``message`` is accepted because
            # one driver generation wrote that instead and an empty message is
            # the worst possible failure mode for a conformance run.
            _as_str(detail.get("error_message")) or _as_str(detail.get("message")) or "",
            _as_str(detail.get("traceback")) or "",
        )

    def _check_ok(self, resp: JsonObject) -> None:
        """Fail on a driver-level refusal.

        Args:
            resp: A control response.

        Raises:
            RpcError: When ``ok`` is false.

        """
        if not resp.get("ok"):
            # Some drivers spell a *remote* error this way too; prefer the
            # structured error when one is present so the test sees the real
            # ``error_type`` rather than a blanket ``TransportError``.
            self._raise_if_error(resp)
            raise RpcError("TransportError", _driver_error_text(resp), "")

    # -- proxy surface ------------------------------------------------------

    def __getattr__(self, name: str) -> Callable[..., object]:
        """Resolve an RPC method name to a caller bound to this connection.

        Args:
            name: The attribute being looked up.

        Returns:
            A callable issuing that RPC method.

        Raises:
            AttributeError: If the service declares no such method.

        """
        info = self._methods.get(name)
        if info is None:
            raise AttributeError(f"{self._driver.service.__name__} has no RPC method '{name}'")
        caller = self._make_unary(info) if info.method_type == MethodType.UNARY else self._make_stream(info)
        self.__dict__[name] = caller
        return caller

    def _request_bytes(self, info: RpcMethodInfo, kwargs: dict[str, object]) -> bytes:
        """Encode one request as an Arrow IPC stream using the canonical writer.

        Args:
            info: The method being called.
            kwargs: The call's keyword arguments.

        Returns:
            A complete request IPC stream.

        """
        buf = io.BytesIO()
        _send_request(
            buf,
            info,
            kwargs,
            protocol=self._protocol,
            protocol_version=self._protocol_version,
        )
        return buf.getvalue()

    def _make_unary(self, info: RpcMethodInfo) -> Callable[..., object]:
        """Build the caller for a unary method.

        Args:
            info: The method being bound.

        Returns:
            A callable issuing the ``unary`` op.

        """

        def caller(**kwargs: object) -> object:
            self._send({"op": "unary", "request_b64": _b64e(self._request_bytes(info, kwargs))})
            resp = self._recv()
            self._check_ok(resp)
            self._replay_logs(resp.get("logs"))
            self._raise_if_error(resp)
            result = _as_str(resp.get("result_b64"))
            if result is None:
                return None
            # ``external_config=None``: the driver's client already resolved any
            # external pointer, and resolving here would mask it.
            return _read_unary_response(_reader(_b64d(result)), info, None, None)

        return caller

    def _make_stream(self, info: RpcMethodInfo) -> Callable[..., DriverStreamSession]:
        """Build the caller for a streaming method.

        Args:
            info: The method being bound.

        Returns:
            A callable issuing the ``stream_open`` op.

        """

        def caller(**kwargs: object) -> DriverStreamSession:
            self._send(
                {
                    "op": "stream_open",
                    "request_b64": _b64e(self._request_bytes(info, kwargs)),
                    "is_exchange": bool(info.is_exchange),
                    "has_header": info.header_type is not None,
                }
            )
            resp = self._recv()
            self._check_ok(resp)
            self._replay_logs(resp.get("logs"))
            self._raise_if_error(resp)
            header: ArrowSerializableDataclass | None = None
            encoded = _as_str(resp.get("header_b64"))
            if encoded and info.header_type is not None:
                header = _read_stream_header(
                    io.BytesIO(_b64d(encoded)),
                    info.header_type,
                    IpcValidation.FULL,
                    self._on_log,
                    None,
                )
            return DriverStreamSession(self, header)

        return caller

    def describe(self) -> ServiceDescription:
        """Return the driver client's own introspection, relayed as JSON.

        Introspection is ``vgi_rpc.Reflection.v1``, whose reply is two nested
        payloads rather than one flat batch.  Relaying raw Arrow the way every
        other op does would make this shim re-implement the reflection schema;
        the driver's client already decoded it, so the driver hands over what
        it got and this rebuilds the client-side view.

        Returns:
            The described service.

        Raises:
            RpcError: If the driver refused, or the description is missing.

        """
        self._send({"op": "describe"})
        resp = self._recv()
        self._check_ok(resp)
        self._replay_logs(resp.get("logs"))
        self._raise_if_error(resp)
        described = _as_dict(resp.get("describe"))
        if described is None:
            raise RpcError("ProtocolError", "driver returned no description", "")
        methods: dict[str, MethodDescription] = {}
        for item in _as_list(described.get("methods")) or []:
            entry = _as_dict(item)
            if entry is None:
                continue
            name = _as_str(entry.get("name")) or ""
            has_header = bool(entry.get("has_header"))
            is_exchange = entry.get("is_exchange")
            methods[name] = MethodDescription(
                name=name,
                method_type=MethodType(_as_str(entry.get("method_type")) or "unary"),
                has_return=bool(entry.get("has_return")),
                params_schema=_schema_from_ipc(entry.get("params_schema_b64")),
                result_schema=_schema_from_ipc(entry.get("result_schema_b64")),
                has_header=has_header,
                header_schema=_schema_from_ipc(entry.get("header_schema_b64")) if has_header else None,
                is_exchange=bool(is_exchange) if isinstance(is_exchange, bool) else None,
            )
        return ServiceDescription(
            protocol_name=_as_str(described.get("protocol_name")) or "",
            request_version=_as_str(described.get("request_version")) or "",
            describe_version=_as_str(described.get("describe_version")) or "",
            protocol_hash=_as_str(described.get("protocol_hash")) or "",
            server_id=_as_str(described.get("server_id")) or "",
            methods=methods,
            protocol_version=_as_str(described.get("protocol_version")) or "",
        )

    def with_session_token(self, token: str | None = None) -> contextlib.AbstractContextManager[DriverSessionView]:
        """Scope a sticky HTTP session around a block of calls.

        Calls made on the yielded view carry ``VGI-Session`` headers; on exit
        the session is torn down (a best-effort ``DELETE`` unless detached).

        Args:
            token: An existing session token to resume, or ``None`` to let the
                server mint one.

        Returns:
            A context manager yielding the session view.

        """

        @contextlib.contextmanager
        def _scope() -> Iterator[DriverSessionView]:
            self.admin("session_begin", token=token)
            try:
                yield DriverSessionView(self)
            finally:
                self.admin("session_end")

        return _scope()

    def admin(self, op: str, **extra: object) -> JsonObject:
        """Issue one out-of-band control op and return its response.

        Args:
            op: The op name.
            **extra: Additional request fields.

        Returns:
            The decoded response.

        """
        self._send({"op": op, **extra})
        resp = self._recv()
        self._check_ok(resp)
        return resp

    def close(self) -> None:
        """Shut the driver down, then reap it.

        Best-effort throughout: a driver that has already died must not turn
        test teardown into a second failure.
        """
        with contextlib.suppress(Exception):
            self._send({"op": "shutdown"})
            self._recv()
        with contextlib.suppress(Exception):
            if self._proc.stdin is not None:
                self._proc.stdin.close()
        try:
            self._proc.wait(timeout=5)
        except Exception:
            self._proc.kill()

    def __enter__(self) -> Self:
        """Enter the connection scope.

        Returns:
            This proxy.

        """
        return self

    def __exit__(self, *exc: object) -> None:
        """Close the connection.

        Args:
            *exc: The pending exception triple, unused.

        """
        self.close()


def _driver_error_text(resp: JsonObject) -> str:
    """Render a driver-level (``ok: false``) refusal as text.

    Args:
        resp: A control response.

    Returns:
        A human-readable description of the refusal.

    """
    err = resp.get("error")
    detail = _as_dict(err)
    if detail is not None:
        return _as_str(detail.get("error_message")) or _as_str(detail.get("message")) or repr(detail)
    return _as_str(err) or repr(err)


class DriverStreamSession:
    """Mimics ``StreamSession`` over the driver control channel."""

    def __init__(self, proxy: ClientDriverProxy, header: ArrowSerializableDataclass | None) -> None:
        """Bind a freshly opened stream.

        Args:
            proxy: The connection the stream belongs to.
            header: The decoded stream header, or ``None``.

        """
        self._proxy = proxy
        self._header = header
        # A driver may implement streams as a nested read loop that exits the
        # moment the stream terminates (EOS, error, cancel or close).
        # ``_active`` tracks that: once false the shim must never send another
        # stream op, because a nested driver would no longer be listening for
        # one.  Honouring it is what lets flat and nested drivers both conform.
        self._active = True
        self._cancelled = False
        self._finished = False
        self._closed = False

    @property
    def header(self) -> ArrowSerializableDataclass | None:
        """The stream header, or ``None`` when the method declares none."""
        return self._header

    def typed_header[H: ArrowSerializableDataclass](self, header_type: type[H]) -> H:
        """Return the header narrowed to its expected type.

        Args:
            header_type: The expected header dataclass type.

        Returns:
            The header, typed as *header_type*.

        Raises:
            TypeError: If the header is absent or of another type.

        """
        if not isinstance(self._header, header_type):
            raise TypeError(f"header is {type(self._header).__name__}, expected {header_type.__name__}")
        return self._header

    def _decode_batch(self, resp: JsonObject) -> AnnotatedBatch:
        """Decode one relayed stream item.

        Args:
            resp: A control response carrying ``batch_b64``.

        Returns:
            The batch with its custom metadata.

        Raises:
            RpcError: If the response carries no batch.

        """
        encoded = _as_str(resp.get("batch_b64"))
        if encoded is None:
            raise RpcError("ProtocolError", "driver reported a stream item with no batch", "")
        batch, custom_metadata = _reader(_b64d(encoded)).read_next_batch_with_custom_metadata()
        return AnnotatedBatch(batch=batch, custom_metadata=custom_metadata)

    def _stream_op(self, request: JsonObject) -> JsonObject:
        """Send one stream op and classify the response.

        Args:
            request: The op to send.

        Returns:
            The decoded response.

        Raises:
            RpcError: On a driver refusal or a relayed remote error.

        """
        self._proxy._send(request)
        try:
            resp = self._proxy._recv()
        except RpcError:
            self._active = False
            raise
        if not resp.get("ok"):
            self._active = False
            self._finished = True
            self._proxy._check_ok(resp)
        self._proxy._replay_logs(resp.get("logs"))
        if resp.get("error"):
            self._active = False
            self._finished = True
            self._proxy._raise_if_error(resp)
        return resp

    def tick(self, custom_metadata: Mapping[bytes, bytes] | None = None) -> AnnotatedBatch:
        """Pull the next batch from a producer stream.

        Args:
            custom_metadata: Per-tick Arrow custom metadata to send upstream.

        Returns:
            The next batch.

        Raises:
            RpcError: If the stream was cancelled, or the server errored.
            StopIteration: At end of stream.

        """
        if self._cancelled:
            raise RpcError("ProtocolError", "stream cancelled", "")
        if self._finished or not self._active:
            raise StopIteration
        request: JsonObject = {"op": "tick"}
        if custom_metadata is not None:
            empty = pa.RecordBatch.from_arrays([], schema=pa.schema([]))
            request["input_b64"] = _b64e(_serialize_batch(empty, custom_metadata))
        resp = self._stream_op(request)
        if resp.get("done"):
            self._active = False
            self._finished = True
            raise StopIteration
        return self._decode_batch(resp)

    def __iter__(self) -> DriverStreamSession:
        """Iterate the producer stream.

        Returns:
            This session.

        """
        return self

    def __next__(self) -> AnnotatedBatch:
        """Pull the next batch.

        Returns:
            The next batch.

        """
        return self.tick()

    def next_with_token(self) -> tuple[AnnotatedBatch, str | None]:
        """Pull the next batch together with its opaque resume token.

        Returns:
            The batch and the continuation token, which is ``None`` on
            transports that carry no resumable state.

        Raises:
            RpcError: If the stream was cancelled, or the server errored.
            StopIteration: At end of stream.

        """
        if self._cancelled:
            raise RpcError("ProtocolError", "stream cancelled", "")
        if self._finished or not self._active:
            raise StopIteration
        resp = self._stream_op({"op": "next_with_token"})
        if resp.get("done") or resp.get("batch_b64") is None:
            self._active = False
            self._finished = True
            raise StopIteration
        return self._decode_batch(resp), _as_str(resp.get("token"))

    def exchange(self, input: AnnotatedBatch) -> AnnotatedBatch:
        """Send one batch and receive its reply.

        Args:
            input: The batch to send.

        Returns:
            The reply batch.

        Raises:
            RpcError: If the stream is closed, finished, or the server errored.

        """
        if self._cancelled or self._closed:
            raise RpcError("ProtocolError", "stream closed", "")
        if self._finished or not self._active:
            raise RpcError("ProtocolError", "stream finished", "")
        payload = _serialize_batch(input.batch, input.custom_metadata)
        resp = self._stream_op({"op": "exchange", "input_b64": _b64e(payload)})
        if resp.get("done") or resp.get("batch_b64") is None:
            self._active = False
            self._finished = True
            raise RpcError("ProtocolError", "exchange returned no batch", "")
        return self._decode_batch(resp)

    def cancel(self) -> None:
        """Cancel the stream early, releasing the server's work."""
        if self._cancelled or not self._active:
            self._cancelled = True
            return
        self._proxy._send({"op": "cancel"})
        resp = self._proxy._recv()
        self._proxy._replay_logs(resp.get("logs"))
        self._cancelled = True
        self._active = False

    def close(self) -> None:
        """Release the stream, ending any nested driver loop."""
        if self._closed:
            return
        self._closed = True
        if self._active:
            self._proxy._send({"op": "close"})
            self._proxy._recv()
            self._active = False

    def __enter__(self) -> Self:
        """Enter the stream scope.

        Returns:
            This session.

        """
        return self

    def __exit__(self, *exc: object) -> None:
        """Close the stream.

        Args:
            *exc: The pending exception triple, unused.

        """
        self.close()


class DriverSessionView:
    """Mimics the HTTP ``_SessionView``.

    RPC calls delegate to the proxy — the driver's client has the sticky
    session bound, so its requests already carry the session headers — and the
    session-token / echo-header / detach accessors map to control ops.
    """

    def __init__(self, proxy: ClientDriverProxy) -> None:
        """Bind a view to an active session.

        Args:
            proxy: The connection whose session is active.

        """
        object.__setattr__(self, "_proxy", proxy)

    def __getattr__(self, name: str) -> object:
        """Delegate RPC method names to the underlying proxy.

        Args:
            name: The attribute being looked up.

        Returns:
            The proxy's bound attribute.

        """
        return getattr(object.__getattribute__(self, "_proxy"), name)

    @property
    def _connection(self) -> ClientDriverProxy:
        """The proxy this view delegates to."""
        return cast("ClientDriverProxy", object.__getattribute__(self, "_proxy"))

    def current_session_token(self) -> str | None:
        """Return the session token the server minted, if any.

        Returns:
            The token, or ``None`` when no session is bound.

        """
        return _as_str(self._connection.admin("session_token").get("token"))

    def current_echo_headers(self) -> dict[str, str]:
        """Return the ``VGI-Echo-*`` headers captured when the session opened.

        Returns:
            Header name to value.

        """
        return _as_str_map(self._connection.admin("session_echo_headers").get("headers"))

    def detach(self) -> str | None:
        """Detach the session so it outlives this scope.

        Returns:
            The detached session token, or ``None``.

        """
        return _as_str(self._connection.admin("session_detach").get("token"))
