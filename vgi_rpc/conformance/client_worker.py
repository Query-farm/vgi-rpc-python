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
import socket
from dataclasses import dataclass
from io import BytesIO
from typing import Protocol
from wsgiref.types import StartResponse, WSGIApplication, WSGIEnvironment

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc._codec import Encoding, decompress
from vgi_rpc.rpc import AnnotatedBatch, CallContext, ExchangeState, OutputCollector, RpcServer, Stream

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


class ClientConformanceService(Protocol):
    """Service used only to validate a language SDK's native client."""

    def typed_exchange(self) -> Stream[TypedEchoState]:
        """Echo batches that use :data:`TYPED_EXCHANGE_SCHEMA` exactly."""
        ...


@dataclass
class TypedEchoState(ExchangeState):
    """Stateless exchange that echoes the already-validated input batch."""

    def exchange(self, input: AnnotatedBatch, out: OutputCollector, ctx: CallContext) -> None:
        """Emit the input batch without rebuilding or inferring its schema."""
        out.emit(input.batch)


class ClientConformanceServiceImpl:
    """Reference implementation of :class:`ClientConformanceService`."""

    def typed_exchange(self) -> Stream[TypedEchoState]:
        """Create one typed echo exchange session."""
        return Stream(
            output_schema=TYPED_EXCHANGE_SCHEMA,
            state=TypedEchoState(),
            input_schema=TYPED_EXCHANGE_SCHEMA,
        )


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


def main() -> None:
    """Serve the strict native-client conformance worker over HTTP."""
    parser = argparse.ArgumentParser(description="vgi-rpc native-client conformance worker")
    parser.add_argument("--http", nargs="?", type=int, const=0, required=True, metavar="PORT")
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    try:
        import waitress
    except ImportError:
        parser.error("HTTP support requires the vgi-rpc http extra")

    from vgi_rpc.http import make_wsgi_app

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((args.host, args.http))
    port = int(listener.getsockname()[1])
    server = RpcServer(ClientConformanceService, ClientConformanceServiceImpl())
    app = StrictExchangeSchemaMiddleware(make_wsgi_app(server, compression_level=None))
    print(f"PORT:{port}", flush=True)
    waitress.serve(app, sockets=[listener], _quiet=True)


if __name__ == "__main__":  # pragma: no cover - exercised by SDK integration tests
    main()


__all__ = [
    "ClientConformanceService",
    "ClientConformanceServiceImpl",
    "StrictExchangeSchemaMiddleware",
    "TYPED_EXCHANGE_SCHEMA",
    "TypedEchoState",
]
