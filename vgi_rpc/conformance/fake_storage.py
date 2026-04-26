# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""In-memory fake object-storage service for external-location conformance tests.

The vgi-rpc external-location feature (``vgi_rpc/external.py``) lets the
server upload large batches to an object store and embed a ``vgi_rpc.location``
URL in a zero-row pointer batch; the client transparently fetches the URL
to reconstruct the data.  Real-world deployments use S3 or GCS for this,
which is too heavyweight to require for cross-implementation conformance
testing.

This module provides a minimal HTTP service that is functionally
equivalent for testing purposes, plus a ready-made
:class:`ExternalStorage` adapter (:class:`FakeStorageBackend`) so the
Python conformance worker can be wired up against it.

Wire contract — four endpoints, each implementation can run this exact
service or implement the contract independently:

* ``POST /alloc`` with optional JSON ``{"content_encoding": "zstd"}``
  body returns ``{"object_url": "http://host:port/blob/{id}"}``.  The
  returned URL is the same path used for both ``PUT`` (upload) and
  ``GET`` (download).
* ``PUT /blob/{id}`` accepts the raw bytes (and an optional
  ``Content-Encoding`` request header which the service echoes back on
  ``HEAD``/``GET``).  Returns ``204``.
* ``HEAD /blob/{id}`` returns ``200`` with ``Content-Length``,
  ``Accept-Ranges: bytes``, and (if applicable) ``Content-Encoding``.
* ``GET /blob/{id}`` returns ``200`` (or ``206`` for ``Range`` requests)
  with the stored bytes.

The service is in-memory only — every restart loses its objects, which
is exactly what tests want.

Usage
-----

In-process (preferred for Python tests)::

    from vgi_rpc.conformance.fake_storage import serve_in_thread

    base_url, shutdown = serve_in_thread()
    try:
        # base_url is "http://127.0.0.1:<port>"
        ...
    finally:
        shutdown()

Standalone (for non-Python conformance runners)::

    python -m vgi_rpc.conformance.fake_storage --port 0

Prints ``PORT:<n>`` to stdout (machine-readable port discovery).
"""

from __future__ import annotations

import argparse
import json
import logging
import threading
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC
from typing import IO, TYPE_CHECKING, cast
from wsgiref.simple_server import WSGIRequestHandler, make_server
from wsgiref.types import StartResponse, WSGIApplication, WSGIEnvironment

if TYPE_CHECKING:
    import pyarrow as pa

    from vgi_rpc.external import UploadUrl

_logger = logging.getLogger(__name__)


class _BlobStore:
    """Thread-safe in-memory blob store keyed by blob ID."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._blobs: dict[str, tuple[bytes, str | None]] = {}

    def allocate(self) -> str:
        return uuid.uuid4().hex

    def put(self, blob_id: str, data: bytes, content_encoding: str | None) -> None:
        with self._lock:
            self._blobs[blob_id] = (data, content_encoding)

    def get(self, blob_id: str) -> tuple[bytes, str | None] | None:
        with self._lock:
            return self._blobs.get(blob_id)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"object_count": len(self._blobs)}


def _parse_range(header: str, total: int) -> tuple[int, int] | None:
    """Parse an HTTP ``Range: bytes=N-M`` header into an inclusive ``(start, end)``."""
    if not header.startswith("bytes="):
        return None
    spec = header[len("bytes=") :]
    if "," in spec:
        return None
    if "-" not in spec:
        return None
    start_str, end_str = spec.split("-", 1)
    try:
        start = int(start_str) if start_str else 0
        end = int(end_str) if end_str else total - 1
    except ValueError:
        return None
    if start < 0 or end < start or end >= total:
        return None
    return start, end


def make_app(base_url: str, store: _BlobStore | None = None) -> WSGIApplication:
    """Construct the fake-storage WSGI app.

    Args:
        base_url: External base URL the service is reachable at, e.g.
            ``"http://127.0.0.1:8123"``.  Embedded in ``object_url``
            responses from ``POST /alloc``.
        store: Optional pre-built blob store (for tests that want to
            inspect contents directly).  A fresh one is created if not
            provided.

    Returns:
        A WSGI callable suitable for ``wsgiref.simple_server`` or
        ``waitress``.

    """
    if store is None:
        store = _BlobStore()

    def app(environ: WSGIEnvironment, start_response: StartResponse) -> Iterable[bytes]:
        method = str(environ["REQUEST_METHOD"]).upper()
        path = str(environ["PATH_INFO"])

        if method == "POST" and path == "/alloc":
            return _handle_alloc(environ, start_response, store, base_url)
        if path.startswith("/blob/"):
            blob_id = path[len("/blob/") :]
            if not blob_id or "/" in blob_id:
                return _respond(start_response, "404 Not Found", b"unknown blob")
            if method == "PUT":
                return _handle_put(environ, start_response, store, blob_id)
            if method == "HEAD":
                return _handle_head(start_response, store, blob_id)
            if method == "GET":
                return _handle_get(environ, start_response, store, blob_id)
            return _respond(start_response, "405 Method Not Allowed", b"")
        if method == "GET" and path == "/_stats":
            body = json.dumps(store.stats()).encode()
            return _respond(start_response, "200 OK", body, content_type="application/json")
        return _respond(start_response, "404 Not Found", b"")

    # Attach the store so tests can inspect / reset it.
    app.store = store  # type: ignore[attr-defined]
    return app


def _handle_alloc(
    environ: WSGIEnvironment,
    start_response: StartResponse,
    store: _BlobStore,
    base_url: str,
) -> list[bytes]:
    blob_id = store.allocate()
    # Pre-allocate an empty entry so HEAD/GET before PUT returns 404 cleanly
    # (we only insert on PUT).
    body = json.dumps({"object_url": f"{base_url}/blob/{blob_id}"}).encode()
    return _respond(start_response, "200 OK", body, content_type="application/json")


def _handle_put(
    environ: WSGIEnvironment,
    start_response: StartResponse,
    store: _BlobStore,
    blob_id: str,
) -> list[bytes]:
    try:
        length = int(str(environ.get("CONTENT_LENGTH") or "0"))
    except ValueError:
        return _respond(start_response, "400 Bad Request", b"invalid Content-Length")
    stream = cast("IO[bytes] | None", environ.get("wsgi.input"))
    data = stream.read(length) if stream is not None and length > 0 else b""
    encoding = environ.get("HTTP_CONTENT_ENCODING")
    store.put(blob_id, data, str(encoding) if encoding else None)
    return _respond(start_response, "204 No Content", b"")


def _handle_head(
    start_response: StartResponse,
    store: _BlobStore,
    blob_id: str,
) -> list[bytes]:
    entry = store.get(blob_id)
    if entry is None:
        return _respond(start_response, "404 Not Found", b"")
    data, encoding = entry
    headers = [
        ("Content-Length", str(len(data))),
        ("Accept-Ranges", "bytes"),
        ("Content-Type", "application/octet-stream"),
    ]
    if encoding:
        headers.append(("Content-Encoding", encoding))
    start_response("200 OK", headers)
    return [b""]


def _handle_get(
    environ: WSGIEnvironment,
    start_response: StartResponse,
    store: _BlobStore,
    blob_id: str,
) -> list[bytes]:
    entry = store.get(blob_id)
    if entry is None:
        return _respond(start_response, "404 Not Found", b"")
    data, encoding = entry
    range_header = str(environ.get("HTTP_RANGE", ""))
    headers: list[tuple[str, str]] = [("Content-Type", "application/octet-stream"), ("Accept-Ranges", "bytes")]
    if encoding:
        headers.append(("Content-Encoding", encoding))
    if range_header:
        rng = _parse_range(range_header, len(data))
        if rng is None:
            start_response("416 Range Not Satisfiable", [("Content-Range", f"bytes */{len(data)}")])
            return [b""]
        start, end = rng
        chunk = data[start : end + 1]
        headers.append(("Content-Range", f"bytes {start}-{end}/{len(data)}"))
        headers.append(("Content-Length", str(len(chunk))))
        start_response("206 Partial Content", headers)
        return [chunk]
    headers.append(("Content-Length", str(len(data))))
    start_response("200 OK", headers)
    return [data]


def _respond(
    start_response: StartResponse,
    status: str,
    body: bytes,
    *,
    content_type: str = "text/plain",
) -> list[bytes]:
    start_response(status, [("Content-Type", content_type), ("Content-Length", str(len(body)))])
    return [body]


# ---------------------------------------------------------------------------
# In-process server helpers
# ---------------------------------------------------------------------------


class _SilentHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return


def serve_in_thread(host: str = "127.0.0.1", port: int = 0) -> tuple[str, Callable[[], None]]:
    """Start the fake storage on a background thread and return ``(base_url, shutdown)``.

    Args:
        host: Bind host.  Defaults to ``"127.0.0.1"``.
        port: Bind port.  ``0`` (default) selects a free port.

    Returns:
        A ``(base_url, shutdown)`` pair.  Call ``shutdown()`` to stop
        the server and join its thread.

    """
    # Bind first to discover the chosen port, then build the app with the
    # full base URL so allocated object URLs are externally reachable.
    placeholder_app = cast("WSGIApplication", lambda environ, start_response: [b""])
    server = make_server(host, port, placeholder_app, handler_class=_SilentHandler)
    bound_port = server.server_address[1]
    base_url = f"http://{host}:{bound_port}"
    server.set_app(make_app(base_url))

    thread = threading.Thread(target=server.serve_forever, name="fake-storage", daemon=True)
    thread.start()

    def shutdown() -> None:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    return base_url, shutdown


# ---------------------------------------------------------------------------
# ExternalStorage adapter
# ---------------------------------------------------------------------------


class FakeStorageBackend:
    """Adapter implementing both ``ExternalStorage`` and ``UploadUrlProvider``.

    Used by the Python conformance HTTP worker when run with
    ``--fake-storage URL``.  ``upload()`` covers the server-to-client
    externalization path (server uploads, embeds GET URL in pointer
    batch).  ``generate_upload_url()`` covers the client-to-server
    upload-URL path (server vends a presigned URL pair so the client
    can PUT, then send a pointer batch back).

    Other-language ports can implement the same POST-then-PUT pattern
    against the same service to satisfy either or both protocols.
    """

    def __init__(self, base_url: str) -> None:
        """Bind to a running fake-storage service at ``base_url``."""
        import httpx

        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=10.0)

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Allocate a URL, PUT bytes, return the GET URL (``ExternalStorage``)."""
        alloc_body = {"content_encoding": content_encoding} if content_encoding else {}
        alloc_resp = self._client.post(f"{self._base_url}/alloc", json=alloc_body)
        alloc_resp.raise_for_status()
        object_url = alloc_resp.json()["object_url"]

        put_headers = {"Content-Type": "application/octet-stream"}
        if content_encoding:
            put_headers["Content-Encoding"] = content_encoding
        put_resp = self._client.put(object_url, content=data, headers=put_headers)
        put_resp.raise_for_status()
        return str(object_url)

    def generate_upload_url(self, schema: pa.Schema) -> UploadUrl:
        """Allocate a fresh blob and return an ``UploadUrl`` (``UploadUrlProvider``)."""
        from datetime import datetime, timedelta

        from vgi_rpc.external import UploadUrl

        alloc_resp = self._client.post(f"{self._base_url}/alloc", json={})
        alloc_resp.raise_for_status()
        object_url = alloc_resp.json()["object_url"]
        # The fake storage uses the same URL path for PUT and GET; HTTP
        # method disambiguation rather than presigning per method.
        return UploadUrl(
            upload_url=object_url,
            download_url=object_url,
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    def close(self) -> None:
        """Release the underlying HTTP client."""
        self._client.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the fake storage as a standalone HTTP server.

    Prints ``PORT:<n>`` to stdout once bound (machine-readable
    discovery) and then serves requests until interrupted.
    """
    parser = argparse.ArgumentParser(prog="vgi-rpc-fake-storage")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0, help="0 selects a free port")
    args = parser.parse_args()

    placeholder_app = cast("WSGIApplication", lambda environ, start_response: [b""])
    server = make_server(args.host, args.port, placeholder_app, handler_class=_SilentHandler)
    bound_port = server.server_address[1]
    base_url = f"http://{args.host}:{bound_port}"
    server.set_app(make_app(base_url))
    print(f"PORT:{bound_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
