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

Wire contract — the legacy four endpoints plus method-bound URL aliases,
each implementation can run this exact
service or implement the contract independently:

* ``POST /alloc`` with optional JSON ``{"content_encoding": "zstd"}``
  body returns ``object_url`` (the legacy dual-method URL), ``upload_url``
  (PUT-only), and ``download_url`` (GET/HEAD-only).  Keeping ``object_url``
  makes the fixture backward compatible while the method-bound pair catches
  production backends that accidentally vend a PUT signature for later GET.
  A *sized* allocation (upload-URL protocol v2) adds ``"size": N`` and
  ``"sha256_b64": "<base64 of the 32 digest bytes>"`` to the body; the
  response then also carries ``upload_headers``
  (``{"x-amz-checksum-sha256": <sha256_b64>}``) and its ``upload_url``
  verifies the PUT like S3/R2: a body whose length is not ``size`` is
  ``400 IncompleteBody``, an ``x-amz-checksum-sha256`` header missing or
  different from the allocation is ``403 SignatureDoesNotMatch``, and a body
  that does not hash to it is ``400 BadDigest`` (S3-style XML error bodies).
  Nothing is stored on a refusal.
* ``PUT /blob/{id}`` accepts the raw bytes (and an optional
  ``Content-Encoding`` request header which the service echoes back on
  ``HEAD``/``GET``).  Returns ``204``.
* ``HEAD /blob/{id}`` returns ``200`` with ``Content-Length``,
  ``Accept-Ranges: bytes``, and (if applicable) ``Content-Encoding``.
* ``GET /blob/{id}`` returns ``200`` (or ``206`` for ``Range`` requests)
  with the stored bytes.
* ``GET|HEAD /redirect/{id}`` redirects to the same-host download URL.
* ``GET|HEAD /redirect-localhost/{id}`` redirects from ``127.0.0.1`` to
  ``localhost``.  Security tests configure a validator that rejects that hop
  and assert the download route is never reached.
* ``GET|HEAD /redirect-loop/{id}`` redirects to itself.
* Every ``/alloc`` response carries ``object_id`` (the blob id), the id the
  object API and protocol-3.4 grants name it by.  An allocation whose body
  says ``"purpose": "object"`` is an object-API write (``put_object``) and
  counts toward ``object_puts``.
* ``GET /objects/{id}`` returns the stored bytes (``404`` when absent) and
  counts ``object_gets``: the object API's ``get(object_id)``.
* ``DELETE /objects/{id}`` deletes a blob (``204``, ``404`` when absent):
  a test route standing in for the bucket lifecycle.
* ``GET /_stats`` includes per-route request counters in addition to the
  legacy ``object_count``; ``upload_refusals`` counts sized PUTs refused, and
  ``allocations``, ``object_puts``, ``object_gets`` and ``object_deletes``
  are always present (``0`` until used).

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
import base64
import hashlib
import json
import logging
import socketserver
import threading
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC
from typing import IO, TYPE_CHECKING, cast
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server
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
        self._hits: dict[str, int] = {}
        self._sized: dict[str, tuple[int, str]] = {}

    def allocate(self, sized: tuple[int, str] | None = None) -> str:
        blob_id = uuid.uuid4().hex
        if sized is not None:
            with self._lock:
                self._sized[blob_id] = sized
        return blob_id

    def sized(self, blob_id: str) -> tuple[int, str] | None:
        """Return the ``(size, sha256_b64)`` a sized allocation declared, if any."""
        with self._lock:
            return self._sized.get(blob_id)

    def put(self, blob_id: str, data: bytes, content_encoding: str | None) -> None:
        with self._lock:
            self._blobs[blob_id] = (data, content_encoding)

    def get(self, blob_id: str) -> tuple[bytes, str | None] | None:
        with self._lock:
            return self._blobs.get(blob_id)

    def delete(self, blob_id: str) -> bool:
        """Delete a blob; return whether it existed."""
        with self._lock:
            return self._blobs.pop(blob_id, None) is not None

    def hit(self, name: str) -> None:
        """Increment one observable route counter."""
        with self._lock:
            self._hits[name] = self._hits.get(name, 0) + 1

    def stats(self) -> dict[str, int]:
        with self._lock:
            always = dict.fromkeys(_ALWAYS_REPORTED, 0)
            return {"object_count": len(self._blobs), **always, **self._hits}


#: Counters ``/_stats`` reports even before their first hit, so a test can read
#: a before-value without special-casing absence.
_ALWAYS_REPORTED = ("allocations", "object_puts", "object_gets", "object_deletes", "upload_refusals")


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

        # Read the whole request body up front, on every route, whether or not
        # the handler below wants it.  ``wsgiref`` answers HTTP/1.0 and closes
        # the connection after each response; closing a socket that still has
        # unread bytes in its receive buffer makes the kernel send RST instead
        # of FIN, and a client with a read in flight then loses the response it
        # had already been sent.  POSIX hands the application the bytes it has
        # buffered regardless, which is why ``POST /alloc`` -- whose JSON body
        # nothing here consumes -- worked everywhere except Windows, where the
        # abort surfaces as WSAECONNABORTED (10053) in whichever
        # external-location test happened to be making the call.
        body = _read_body(environ)

        if method == "POST" and path == "/alloc":
            return _handle_alloc(environ, start_response, store, base_url, body)
        redirect_prefix = next(
            (prefix for prefix in ("/redirect/", "/redirect-localhost/", "/redirect-loop/") if path.startswith(prefix)),
            None,
        )
        if redirect_prefix is not None and method in ("GET", "HEAD"):
            blob_id = path[len(redirect_prefix) :]
            if not blob_id or "/" in blob_id:
                return _respond(start_response, "404 Not Found", b"unknown blob")
            store.hit(redirect_prefix.strip("/").replace("-", "_") + "_requests")
            if redirect_prefix == "/redirect-loop/":
                target = f"{base_url}{redirect_prefix}{blob_id}"
            elif redirect_prefix == "/redirect-localhost/":
                target = f"{base_url.replace('127.0.0.1', 'localhost')}/download/{blob_id}"
            else:
                target = f"{base_url}/download/{blob_id}"
            start_response("302 Found", [("Location", target), ("Content-Length", "0")])
            return [b""]
        route_prefix = next(
            (prefix for prefix in ("/blob/", "/upload/", "/download/") if path.startswith(prefix)),
            None,
        )
        if route_prefix is not None:
            blob_id = path[len(route_prefix) :]
            if not blob_id or "/" in blob_id:
                return _respond(start_response, "404 Not Found", b"unknown blob")
            if route_prefix == "/upload/" and method != "PUT":
                return _respond(start_response, "403 Forbidden", b"upload URL only permits PUT")
            if route_prefix == "/download/" and method not in ("GET", "HEAD"):
                return _respond(start_response, "403 Forbidden", b"download URL only permits GET or HEAD")
            if route_prefix == "/download/":
                store.hit("download_requests")
            if method == "PUT":
                return _handle_put(environ, start_response, store, blob_id, body)
            if method == "HEAD":
                return _handle_head(start_response, store, blob_id)
            if method == "GET":
                return _handle_get(environ, start_response, store, blob_id)
            return _respond(start_response, "405 Method Not Allowed", b"")
        if path.startswith("/objects/"):
            blob_id = path[len("/objects/") :]
            if not blob_id or "/" in blob_id:
                return _respond(start_response, "404 Not Found", b"unknown blob")
            if method == "GET":
                store.hit("object_gets")
                entry = store.get(blob_id)
                if entry is None:
                    return _respond(start_response, "404 Not Found", b"no such object")
                return _respond(start_response, "200 OK", entry[0], content_type="application/octet-stream")
            if method == "DELETE":
                store.hit("object_deletes")
                if not store.delete(blob_id):
                    return _respond(start_response, "404 Not Found", b"no such object")
                return _respond(start_response, "204 No Content", b"")
            return _respond(start_response, "405 Method Not Allowed", b"")
        if method == "GET" and path == "/_stats":
            body = json.dumps(store.stats()).encode()
            return _respond(start_response, "200 OK", body, content_type="application/json")
        return _respond(start_response, "404 Not Found", b"")

    # Attach the store so tests can inspect / reset it.
    app.store = store  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
    return app


def _handle_alloc(
    environ: WSGIEnvironment,
    start_response: StartResponse,
    store: _BlobStore,
    base_url: str,
    request_body: bytes = b"",
) -> list[bytes]:
    try:
        request = json.loads(request_body or b"{}")
    except ValueError:
        request = None
    if not isinstance(request, dict):
        return _respond(start_response, "400 Bad Request", b"alloc body must be a JSON object")
    sized: tuple[int, str] | None = None
    if "size" in request or "sha256_b64" in request:
        size = request.get("size")
        sha256_b64 = request.get("sha256_b64")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            return _respond(start_response, "400 Bad Request", b"size must be a non-negative integer")
        try:
            digest_ok = isinstance(sha256_b64, str) and len(base64.b64decode(sha256_b64, validate=True)) == 32
        except ValueError:
            digest_ok = False
        if not digest_ok or not isinstance(sha256_b64, str):
            return _respond(start_response, "400 Bad Request", b"sha256_b64 must be a base64 SHA-256")
        sized = (size, sha256_b64)
    blob_id = store.allocate(sized)
    store.hit("allocations")
    if request.get("purpose") == "object":
        store.hit("object_puts")
    # Pre-allocate an empty entry so HEAD/GET before PUT returns 404 cleanly
    # (we only insert on PUT).
    response: dict[str, object] = {
        # Retained for older conformance adapters that have not migrated
        # to the explicit method-bound pair yet.
        "object_url": f"{base_url}/blob/{blob_id}",
        "upload_url": f"{base_url}/upload/{blob_id}",
        "download_url": f"{base_url}/download/{blob_id}",
        "object_id": blob_id,
    }
    if sized is not None:
        response["upload_headers"] = {"x-amz-checksum-sha256": sized[1]}
    return _respond(start_response, "200 OK", json.dumps(response).encode(), content_type="application/json")


def _handle_put(
    environ: WSGIEnvironment,
    start_response: StartResponse,
    store: _BlobStore,
    blob_id: str,
    data: bytes,
) -> list[bytes]:
    if data is _MALFORMED_LENGTH:
        return _respond(start_response, "400 Bad Request", b"invalid Content-Length")
    sized = store.sized(blob_id)
    if sized is not None:
        refusal = _sized_put_refusal(environ, data, *sized)
        if refusal is not None:
            status, code = refusal
            store.hit("upload_refusals")
            store.hit(f"upload_refusals_{code}")
            payload = f"<Error><Code>{code}</Code></Error>".encode()
            return _respond(start_response, status, payload, content_type="application/xml")
    encoding = environ.get("HTTP_CONTENT_ENCODING")
    store.put(blob_id, data, str(encoding) if encoding else None)
    return _respond(start_response, "204 No Content", b"")


def _sized_put_refusal(environ: WSGIEnvironment, data: bytes, size: int, sha256_b64: str) -> tuple[str, str] | None:
    """Return ``(status, S3 error code)`` when a sized PUT must be refused, else ``None``.

    Mirrors S3/R2 for a URL signed over ``Content-Length`` and
    ``x-amz-checksum-sha256``: a short or long body is ``IncompleteBody``,
    a checksum header that is absent or not the signed value fails the
    signature, and a body that does not hash to the signed value is
    ``BadDigest``.
    """
    if len(data) != size:
        return "400 Bad Request", "IncompleteBody"
    if environ.get("HTTP_X_AMZ_CHECKSUM_SHA256") != sha256_b64:
        return "403 Forbidden", "SignatureDoesNotMatch"
    if base64.b64encode(hashlib.sha256(data).digest()).decode() != sha256_b64:
        return "400 Bad Request", "BadDigest"
    return None


#: Sentinel distinguishing "the client sent a Content-Length we cannot parse"
#: from "the client sent no body".  Returned rather than raised so the read
#: still happens on every route -- see the note in ``make_app``.
_MALFORMED_LENGTH = b"\x00__malformed_content_length__"


def _read_body(environ: WSGIEnvironment) -> bytes:
    """Read exactly ``CONTENT_LENGTH`` bytes of the request body.

    Bounded by the declared length rather than read to EOF: ``wsgi.input`` is
    the connection itself under ``wsgiref``, so an unbounded read would block
    waiting for a client that has already said everything it intends to.

    Args:
        environ: The WSGI environment for the request.

    Returns:
        The body bytes, empty when there is no body, or
        :data:`_MALFORMED_LENGTH` when ``CONTENT_LENGTH`` will not parse.

    """
    try:
        length = int(str(environ.get("CONTENT_LENGTH") or "0"))
    except ValueError:
        return _MALFORMED_LENGTH
    stream = cast("IO[bytes] | None", environ.get("wsgi.input"))
    if stream is None or length <= 0:
        return b""
    chunks: list[bytes] = []
    remaining = length
    while remaining > 0:
        chunk = stream.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


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


class _ThreadingWSGIServer(socketserver.ThreadingMixIn, WSGIServer):
    """One thread per connection, rather than one connection at a time.

    ``wsgiref``'s default server handles a single request to completion before
    accepting the next, and this fixture has two concurrent clients by
    construction: the test process, and the worker under test reaching for
    ``/alloc`` while that test waits on ``/_stats`` or a ``GET``.  Serialised,
    the second connection sits in the listen backlog for as long as the first
    request takes.  On Windows a connection the local stack gives up on is a
    hard ``WSAECONNABORTED`` where POSIX quietly delivers the response, so the
    fixture's serialisation surfaces as an intermittent ``ReadError`` in
    whichever external-location test happened to be running.

    :class:`_BlobStore` was already written for this -- every accessor takes a
    lock -- so the only thing that was single-threaded is the accept loop.
    """

    daemon_threads = True
    #: The default of 5 is the other half of the same problem: a runner under
    #: load can have more than five connections in flight against one fixture.
    request_queue_size = 64

    def server_bind(self) -> None:
        """Bind without ``HTTPServer``'s reverse DNS lookup of the host.

        ``HTTPServer.server_bind`` calls ``socket.getfqdn(host)`` only to fill
        in ``SERVER_NAME``, and this fixture has no use for the answer.  That
        lookup took 35 seconds for ``127.0.0.1`` on GitHub's macOS runners, and
        the port is announced after it, so a client waiting for the port gave
        up first.
        """
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = str(host)
        self.server_port = int(port)
        self.setup_environ()


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
    server = make_server(host, port, placeholder_app, server_class=_ThreadingWSGIServer, handler_class=_SilentHandler)
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
        self._base_url = base_url.rstrip("/")

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Allocate a URL, PUT bytes, return the GET URL (``ExternalStorage``)."""
        import httpx2

        alloc_body = {"content_encoding": content_encoding} if content_encoding else {}
        alloc_resp = httpx2.post(f"{self._base_url}/alloc", json=alloc_body, timeout=10.0)
        alloc_resp.raise_for_status()
        allocation = alloc_resp.json()
        upload_url = allocation.get("upload_url", allocation["object_url"])
        download_url = allocation.get("download_url", allocation["object_url"])

        put_headers = {"Content-Type": "application/octet-stream"}
        if content_encoding:
            put_headers["Content-Encoding"] = content_encoding
        put_resp = httpx2.put(upload_url, content=data, headers=put_headers, timeout=10.0)
        put_resp.raise_for_status()
        return str(download_url)

    def generate_upload_url(self, size: int, sha256_hex: str) -> UploadUrl:
        """Allocate a sized blob and return an ``UploadUrl`` (``UploadUrlProvider``).

        The allocation declares *size* and the digest, so the fake storage
        refuses any other PUT body the way S3/R2 would.
        """
        from datetime import datetime, timedelta

        import httpx2

        from vgi_rpc.external import UploadUrl, sha256_b64_from_hex

        alloc_resp = httpx2.post(
            f"{self._base_url}/alloc",
            json={"size": size, "sha256_b64": sha256_b64_from_hex(sha256_hex)},
            timeout=10.0,
        )
        alloc_resp.raise_for_status()
        allocation = alloc_resp.json()
        object_url = allocation["object_url"]
        headers = allocation.get("upload_headers") or {}
        return UploadUrl(
            upload_url=allocation.get("upload_url", object_url),
            download_url=allocation.get("download_url", object_url),
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            headers={str(k): str(v) for k, v in headers.items()},
            object_id=str(allocation["object_id"]),
        )

    def put_object(self, data: bytes) -> str:
        """Store *data* through a sized object allocation and return its id (``ObjectStore``).

        ``POST /alloc`` with ``purpose: "object"`` (counted as ``object_puts``)
        plus the size and digest, then a verified PUT -- the same shape as a
        vendor sized grant.
        """
        import httpx2

        sha256_b64 = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
        alloc_resp = httpx2.post(
            f"{self._base_url}/alloc",
            json={"size": len(data), "sha256_b64": sha256_b64, "purpose": "object"},
            timeout=10.0,
        )
        alloc_resp.raise_for_status()
        allocation = alloc_resp.json()
        headers = {"Content-Type": "application/octet-stream", **(allocation.get("upload_headers") or {})}
        put_resp = httpx2.put(allocation["upload_url"], content=data, headers=headers, timeout=10.0)
        put_resp.raise_for_status()
        return str(allocation["object_id"])

    def get(self, object_id: str, *, max_bytes: int | None = None) -> bytes:
        """Fetch an object by id via ``GET /objects/{id}`` (``ObjectStore``; counted as ``object_gets``).

        Args:
            object_id: The blob id.
            max_bytes: Largest object accepted, or ``None``.

        Returns:
            The object bytes.

        Raises:
            ObjectMissingError: The fake storage answers 404.
            ValueError: The object is larger than *max_bytes*.

        """
        import httpx2

        from vgi_rpc.external import ObjectMissingError

        resp = httpx2.get(f"{self._base_url}/objects/{object_id}", timeout=10.0)
        if resp.status_code == 404:
            raise ObjectMissingError("fake storage object is gone")
        resp.raise_for_status()
        data = resp.content
        if max_bytes is not None and len(data) > max_bytes:
            raise ValueError(f"object is {len(data)} bytes, over max_fetch_bytes ({max_bytes})")
        return data

    def delete_object(self, object_id: str) -> bool:
        """Delete an object (``DELETE /objects/{id}``); return whether it existed.  Test helper."""
        import httpx2

        resp = httpx2.delete(f"{self._base_url}/objects/{object_id}", timeout=10.0)
        return resp.status_code == 204

    def close(self) -> None:
        """Retain the adapter cleanup protocol; requests own their clients."""


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
    server = make_server(
        args.host, args.port, placeholder_app, server_class=_ThreadingWSGIServer, handler_class=_SilentHandler
    )
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
