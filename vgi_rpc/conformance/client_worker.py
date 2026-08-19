# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Python reference worker for native-client serialization conformance.

The main shared suite drives foreign servers with the Python client, so it
cannot detect bugs in another SDK's batch construction.  This deliberately
small worker reverses that direction: each native SDK connects to Python and
must exchange batches using :data:`TYPED_EXCHANGE_SCHEMA` exactly.

Run it with the normal worker flags, for example::

    python -m vgi_rpc.conformance.client_worker --http 0
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import ipaddress
import signal
import socket
import ssl
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from types import FrameType
from typing import Protocol, cast
from wsgiref.simple_server import WSGIRequestHandler, make_server
from wsgiref.types import StartResponse, WSGIApplication, WSGIEnvironment

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc._codec import Encoding, decompress
from vgi_rpc.external import ServerExternalConfig
from vgi_rpc.rpc import (
    AnnotatedBatch,
    CallContext,
    ExchangeState,
    OutputCollector,
    ProducerState,
    RpcServer,
    Stream,
)

TYPED_EXCHANGE_SCHEMA = pa.schema(
    [
        pa.field("nullable_float", pa.float64(), nullable=True),
        pa.field("tags", pa.list_(pa.field("item", pa.utf8(), nullable=True)), nullable=True),
        pa.field("category", pa.dictionary(pa.int16(), pa.utf8()), nullable=True),
        pa.field("event_time", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("amount", pa.decimal128(18, 4), nullable=True),
        pa.field(
            "nested",
            pa.struct(
                [
                    pa.field("name", pa.utf8(), nullable=True),
                    pa.field("scores", pa.list_(pa.field("item", pa.int32(), nullable=True)), nullable=True),
                ]
            ),
            nullable=True,
        ),
    ]
)
"""Canonical native-client exchange schema; field order and nullability are exact."""

_producer_fields: list[pa.Field[pa.DataType]] = [
    pa.field("index", pa.int64(), nullable=False),
    pa.field("payload", pa.binary(), nullable=False),
]
PRODUCER_SCHEMA = pa.schema(_producer_fields)
"""Canonical producer schema used to exercise HTTP continuation handling."""


class ClientConformanceService(Protocol):
    """Service used only to validate a language SDK's native client."""

    def typed_exchange(self) -> Stream[TypedEchoState]:
        """Echo batches that use :data:`TYPED_EXCHANGE_SCHEMA` exactly."""
        ...

    def producer_sequence(self, count: int, payload_bytes: int) -> Stream[SequenceProducerState]:
        """Emit deterministic producer batches, one process cycle per index."""
        ...

    def producer_zero_row_then_value(self) -> Stream[ZeroRowProducerState]:
        """Emit a real zero-row data batch before one populated batch."""
        ...

    def producer_emit_and_finish(self) -> Stream[EmitAndFinishProducerState]:
        """Emit a data batch and mark the producer terminal in the same tick."""
        ...

    def producer_empty(self) -> Stream[EmptyClientProducerState]:
        """Finish on the init turn without data or a continuation token."""
        ...

    def open_client_session(self, initial: int) -> int:
        """Open a sticky counter session when the worker runs with ``--sticky``."""
        ...

    def increment_client_session(self, by: int) -> int:
        """Resume and increment the sticky counter session."""
        ...

    def close_client_session(self) -> int:
        """Return the sticky counter and close its server-side session."""
        ...

    def large_response(self, size: int) -> bytes:
        """Return deterministic bytes large enough to force response externalization."""
        ...

    def echo_bytes(self, value: bytes) -> bytes:
        """Echo bytes so a client can prove request externalization."""
        ...


@dataclass
class TypedEchoState(ExchangeState):
    """Stateless exchange that echoes the already-validated input batch."""

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Emit the input batch without rebuilding or inferring its schema."""
        out.emit(input.batch)


@dataclass
class SequenceProducerState(ProducerState):
    """Produce ``count`` deterministic batches before a terminal empty turn."""

    count: int
    payload_bytes: int
    current: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit the next index or finish without a continuation token."""
        if self.current >= self.count:
            out.finish()
            return
        payload = bytes([self.current % 251]) * self.payload_bytes
        out.emit(
            pa.RecordBatch.from_arrays(
                [pa.array([self.current], type=pa.int64()), pa.array([payload], type=pa.binary())],
                schema=PRODUCER_SCHEMA,
            )
        )
        self.current += 1


@dataclass
class ZeroRowProducerState(ProducerState):
    """Emit a metadata-free zero-row data batch, one value, then terminate."""

    stage: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Advance the zero-row/value/terminal sequence by one tick."""
        if self.stage == 0:
            out.emit(
                pa.RecordBatch.from_arrays(
                    [pa.array([], pa.int64()), pa.array([], pa.binary())],
                    schema=PRODUCER_SCHEMA,
                )
            )
        elif self.stage == 1:
            out.emit(
                pa.RecordBatch.from_arrays(
                    [pa.array([7], pa.int64()), pa.array([b"after-zero"], pa.binary())],
                    schema=PRODUCER_SCHEMA,
                )
            )
        else:
            out.finish()
        self.stage += 1


@dataclass
class EmitAndFinishProducerState(ProducerState):
    """Emit one value and finish during the same producer process call."""

    emitted: bool = False

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Emit one terminal data batch."""
        if self.emitted:
            out.finish()
            return
        out.emit(
            pa.RecordBatch.from_arrays(
                [pa.array([99], pa.int64()), pa.array([b"terminal"], pa.binary())],
                schema=PRODUCER_SCHEMA,
            )
        )
        out.finish()
        self.emitted = True


@dataclass
class EmptyClientProducerState(ProducerState):
    """Finish immediately on producer init."""

    placeholder: int = 0

    def produce(self, out: OutputCollector, ctx: CallContext) -> None:
        """Mark the producer finished without emitting a batch."""
        out.finish()


@dataclass
class _ClientSessionCounter:
    """Mutable object retained by the optional sticky-session registry."""

    value: int


class ClientConformanceServiceImpl:
    """Reference implementation of :class:`ClientConformanceService`."""

    def typed_exchange(self) -> Stream[TypedEchoState]:
        """Create one typed echo exchange session."""
        return Stream(
            output_schema=TYPED_EXCHANGE_SCHEMA,
            state=TypedEchoState(),
            input_schema=TYPED_EXCHANGE_SCHEMA,
        )

    def producer_sequence(self, count: int, payload_bytes: int) -> Stream[SequenceProducerState]:
        """Create the deterministic multi-turn producer."""
        if count < 0 or payload_bytes < 0:
            raise ValueError("count and payload_bytes must be non-negative")
        return Stream(
            output_schema=PRODUCER_SCHEMA,
            state=SequenceProducerState(count=count, payload_bytes=payload_bytes),
        )

    def producer_zero_row_then_value(self) -> Stream[ZeroRowProducerState]:
        """Create a producer whose first application batch has zero rows."""
        return Stream(output_schema=PRODUCER_SCHEMA, state=ZeroRowProducerState())

    def producer_emit_and_finish(self) -> Stream[EmitAndFinishProducerState]:
        """Create a producer that emits and finishes in its init tick."""
        return Stream(output_schema=PRODUCER_SCHEMA, state=EmitAndFinishProducerState())

    def producer_empty(self) -> Stream[EmptyClientProducerState]:
        """Create a producer that terminates immediately."""
        return Stream(output_schema=PRODUCER_SCHEMA, state=EmptyClientProducerState())

    def open_client_session(self, initial: int, ctx: CallContext) -> int:
        """Open a sticky counter and return its initial value."""
        ctx.open_session(_ClientSessionCounter(initial))
        return initial

    def increment_client_session(self, by: int, ctx: CallContext) -> int:
        """Increment the counter bound to the request's sticky token."""
        counter = ctx.session
        if not isinstance(counter, _ClientSessionCounter):
            raise RuntimeError("no client conformance session is bound")
        counter.value += by
        return counter.value

    def close_client_session(self, ctx: CallContext) -> int:
        """Close the bound counter and return its terminal value."""
        counter = ctx.session
        if not isinstance(counter, _ClientSessionCounter):
            raise RuntimeError("no client conformance session is bound")
        value = counter.value
        ctx.close_session()
        return value

    def large_response(self, size: int) -> bytes:
        """Build a deterministic byte sequence of exactly ``size`` bytes."""
        if size < 0 or size > 16 * 1024 * 1024:
            raise ValueError("size must be between 0 and 16777216")
        return bytes(i % 251 for i in range(size))

    def echo_bytes(self, value: bytes) -> bytes:
        """Echo a client payload byte-for-byte."""
        return value


class StrictExchangeSchemaMiddleware:
    """Reject exchange bodies whose on-wire Arrow schema was inferred incorrectly."""

    def __init__(self, app: WSGIApplication) -> None:
        """Wrap a vgi-rpc WSGI application."""
        self._app = app

    def __call__(self, environ: WSGIEnvironment, start_response: StartResponse) -> object:
        """Inspect typed-exchange input before the framework performs safe casts."""
        path = str(environ.get("PATH_INFO", ""))
        if str(environ.get("REQUEST_METHOD", "")).upper() != "POST" or not path.endswith("/typed_exchange/exchange"):
            return self._app(environ, start_response)

        try:
            length = int(str(environ.get("CONTENT_LENGTH") or "0"))
            stream = environ["wsgi.input"]
            body = stream.read(length)
            encoded_as = str(environ.get("HTTP_CONTENT_ENCODING") or "identity").strip().lower()
            encoding = next(item for item in Encoding if item.value == encoded_as)
            decoded = decompress(encoding, body, max_output_size=64 * 1024 * 1024)
            actual = ipc.open_stream(BytesIO(decoded)).schema
        except Exception:
            return self._reject(start_response, "typed exchange body is not valid Arrow IPC")

        environ["wsgi.input"] = BytesIO(body)
        environ["CONTENT_LENGTH"] = str(len(body))
        if actual != TYPED_EXCHANGE_SCHEMA:
            return self._reject(
                start_response,
                f"typed exchange schema mismatch: expected {TYPED_EXCHANGE_SCHEMA}, got {actual}",
            )
        return self._app(environ, start_response)

    @staticmethod
    def _reject(start_response: StartResponse, message: str) -> list[bytes]:
        """Return a deterministic HTTP 400 without invoking the RPC handler."""
        body = message.encode()
        start_response(
            "400 Bad Request",
            [("Content-Type", "text/plain; charset=utf-8"), ("Content-Length", str(len(body)))],
        )
        return [body]


def make_client_conformance_server(
    *,
    external_location: ServerExternalConfig | None = None,
) -> RpcServer:
    """Build the transport-independent native-client reference server.

    Args:
        external_location: Optional HTTP external-storage configuration. Raw
            transports normally leave this unset and negotiate shared memory
            through the built-in ``__transport_options__`` method instead.

    Returns:
        An introspectable server exposing only :class:`ClientConformanceService`.

    """
    return RpcServer(
        ClientConformanceService,
        ClientConformanceServiceImpl(),
        external_location=external_location,
        enable_describe=True,
        server_id="client-worker",
    )


def make_client_conformance_app(
    *,
    prefix: str = "",
    enable_sticky: bool = False,
    enable_external: bool = False,
    external_threshold: int = 4096,
    producer_turn_bytes: int | None = None,
) -> tuple[WSGIApplication, Callable[[], None]]:
    """Build the standalone native-client app and its resource cleanup callback.

    Args:
        prefix: HTTP route prefix, such as ``"/vgi"``.
        enable_sticky: Install the sticky-session middleware and capability headers.
        enable_external: Start an embedded fake object store and enable both
            response pointers and the ``__upload_url__`` request flow.
        external_threshold: Response-externalization threshold and advertised
            direct-request limit when external mode is enabled.
        producer_turn_bytes: Optional HTTP response cap used to buffer multiple
            producer batches in one turn before issuing a continuation token.

    Returns:
        ``(app, close)``. The idempotent ``close`` callback releases the
        embedded storage client and server when external mode was enabled.

    """
    if external_threshold <= 0:
        raise ValueError("external_threshold must be positive")
    if producer_turn_bytes is not None and producer_turn_bytes <= 0:
        raise ValueError("producer_turn_bytes must be positive")

    external_config = None
    upload_url_provider = None
    cleanups: list[Callable[[], None]] = []
    if enable_external:
        from vgi_rpc.conformance.fake_storage import FakeStorageBackend, serve_in_thread

        storage_url, shutdown_storage = serve_in_thread()
        backend = FakeStorageBackend(storage_url)
        upload_url_provider = backend
        external_config = ServerExternalConfig(
            storage=backend,
            externalize_threshold_bytes=external_threshold,
            url_validator=None,
        )
        cleanups.extend((backend.close, shutdown_storage))

    server = make_client_conformance_server(external_location=external_config)

    from vgi_rpc.http import make_wsgi_app

    app = StrictExchangeSchemaMiddleware(
        make_wsgi_app(
            server,
            prefix=prefix,
            token_key=b"c" * 32,
            max_response_bytes=producer_turn_bytes,
            max_request_bytes=external_threshold if enable_external else None,
            upload_url_provider=upload_url_provider,
            max_upload_bytes=16 * 1024 * 1024 if enable_external else None,
            compression_level=None,
            enable_sticky=enable_sticky,
            sticky_default_ttl=60.0,
            sticky_echo_headers={"X-VGI-Worker-Affinity": "client-worker"} if enable_sticky else None,
        )
    )
    closed = False

    def close() -> None:
        """Release optional embedded-storage resources exactly once."""
        nonlocal closed
        if closed:
            return
        closed = True
        for cleanup in cleanups:
            cleanup()

    return cast("WSGIApplication", app), close


class _QuietRequestHandler(WSGIRequestHandler):
    """Suppress access-log noise from the stdlib TLS fixture server."""

    def log_message(self, format: str, *args: object) -> None:
        """Discard the default stderr access log."""


def _write_tls_material(directory: Path, host: str) -> tuple[Path, Path]:
    """Generate a self-signed localhost certificate for the optional TLS lane."""
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError as exc:  # pragma: no cover - exercised by minimal installations
        raise RuntimeError("--tls requires the vgi-rpc mtls extra") from exc

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = dt.datetime.now(dt.UTC)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "vgi-rpc client conformance")])
    sans: list[x509.GeneralName] = [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        sans.append(x509.DNSName(host))
    else:
        if address not in {ipaddress.ip_address("127.0.0.1")}:
            sans.append(x509.IPAddress(address))
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_path = directory / "client-worker-ca.pem"
    key_path = directory / "client-worker-key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)
    return cert_path, key_path


def _interrupt_server(_signum: int, _frame: FrameType | None) -> None:
    """Turn SIGTERM into normal unwinding so socket and TLS fixtures clean up."""
    raise KeyboardInterrupt


def _serve_raw_socket(server: RpcServer, *, unix: str | None, tcp: str | None) -> None:
    """Serve one discoverable Unix or TCP raw-framing endpoint."""
    from vgi_rpc.rpc import serve_tcp, serve_unix

    previous_sigterm = signal.signal(signal.SIGTERM, _interrupt_server)
    try:
        with contextlib.suppress(KeyboardInterrupt):
            if unix is not None:
                path = str(Path(unix).resolve())
                serve_unix(
                    server,
                    path,
                    threaded=True,
                    on_bound=lambda bound: print(f"UNIX:{bound}", flush=True),
                )
                return

            assert tcp is not None
            if ":" in tcp:
                host, _, raw_port = tcp.rpartition(":")
                host = host or "127.0.0.1"
            else:
                host = "127.0.0.1"
                raw_port = tcp
            try:
                port = int(raw_port)
            except ValueError as exc:
                raise ValueError(f"--tcp expects [HOST:]PORT, got {tcp!r}") from exc
            if not 0 <= port <= 65535:
                raise ValueError(f"--tcp port must be between 0 and 65535, got {port}")
            serve_tcp(
                server,
                host,
                port,
                threaded=True,
                on_bound=lambda bound_host, bound_port: print(
                    f"TCP:{bound_host}:{bound_port}",
                    flush=True,
                ),
            )
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)


def main() -> None:
    """Serve the native-client worker over HTTP or raw Arrow IPC framing."""
    parser = argparse.ArgumentParser(description="vgi-rpc native-client conformance worker")
    transport = parser.add_mutually_exclusive_group(required=True)
    transport.add_argument("--http", nargs="?", type=int, const=0, metavar="PORT")
    transport.add_argument("--stdio", action="store_true", help="serve raw Arrow IPC on stdin/stdout")
    transport.add_argument("--unix", metavar="PATH", help="serve raw Arrow IPC on a Unix domain socket")
    transport.add_argument(
        "--tcp",
        metavar="[HOST:]PORT",
        help="serve unauthenticated raw Arrow IPC on loopback TCP; PORT may be 0",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--prefix", default="", help="HTTP route prefix, for example /vgi")
    parser.add_argument("--sticky", action="store_true", help="enable sticky-session lifecycle fixtures")
    parser.add_argument(
        "--external",
        action="store_true",
        help="enable embedded object storage for request/response externalization",
    )
    parser.add_argument(
        "--external-threshold",
        type=int,
        default=4096,
        metavar="BYTES",
        help="externalize responses and advertise request upload URLs above this size",
    )
    parser.add_argument(
        "--producer-turn-bytes",
        type=int,
        default=None,
        metavar="BYTES",
        help="buffer producer batches up to this HTTP response size before continuation",
    )
    parser.add_argument("--tls", action="store_true", help="serve HTTPS with an ephemeral localhost CA")
    args = parser.parse_args()

    raw_mode = args.stdio or args.unix is not None or args.tcp is not None
    raw_incompatible = [
        name
        for enabled, name in (
            (bool(args.prefix), "--prefix"),
            (args.sticky, "--sticky"),
            (args.external, "--external"),
            (args.producer_turn_bytes is not None, "--producer-turn-bytes"),
            (args.tls, "--tls"),
        )
        if raw_mode and enabled
    ]
    if raw_incompatible:
        parser.error(f"{', '.join(raw_incompatible)} cannot be combined with a raw transport")

    if raw_mode:
        server = make_client_conformance_server()
        if args.stdio:
            from vgi_rpc.rpc import serve_stdio

            serve_stdio(server)
            return
        try:
            _serve_raw_socket(server, unix=args.unix, tcp=args.tcp)
        except ValueError as exc:
            parser.error(str(exc))
        return

    assert args.http is not None
    try:
        app, close_app = make_client_conformance_app(
            prefix=args.prefix,
            enable_sticky=args.sticky,
            enable_external=args.external,
            external_threshold=args.external_threshold,
            producer_turn_bytes=args.producer_turn_bytes,
        )
    except (RuntimeError, ValueError) as exc:
        parser.error(str(exc))

    try:
        if args.tls:
            with tempfile.TemporaryDirectory(prefix="vgi-rpc-client-tls-") as tls_dir:
                cert_path, key_path = _write_tls_material(Path(tls_dir), args.host)
                httpd = make_server(args.host, args.http, app, handler_class=_QuietRequestHandler)
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                context.load_cert_chain(certfile=cert_path, keyfile=key_path)
                httpd.socket = context.wrap_socket(httpd.socket, server_side=True)
                port = int(httpd.server_address[1])
                print(f"PORT:{port}", flush=True)
                print(f"TLS-CA:{cert_path}", flush=True)
                previous_sigterm = signal.signal(signal.SIGTERM, _interrupt_server)
                try:
                    with contextlib.suppress(KeyboardInterrupt):
                        httpd.serve_forever()
                finally:
                    signal.signal(signal.SIGTERM, previous_sigterm)
                    httpd.server_close()
            return

        try:
            import waitress
        except ImportError:
            parser.error("HTTP support requires the vgi-rpc http extra")

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((args.host, args.http))
        port = int(listener.getsockname()[1])
        print(f"PORT:{port}", flush=True)
        waitress.serve(app, sockets=[listener], _quiet=True)
    finally:
        close_app()


if __name__ == "__main__":  # pragma: no cover - exercised by SDK integration tests
    main()


__all__ = [
    "ClientConformanceService",
    "ClientConformanceServiceImpl",
    "EmptyClientProducerState",
    "EmitAndFinishProducerState",
    "PRODUCER_SCHEMA",
    "SequenceProducerState",
    "StrictExchangeSchemaMiddleware",
    "TYPED_EXCHANGE_SCHEMA",
    "TypedEchoState",
    "ZeroRowProducerState",
    "make_client_conformance_app",
    "make_client_conformance_server",
]
