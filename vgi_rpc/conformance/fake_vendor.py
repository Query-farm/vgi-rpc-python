# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""In-memory fake blob vendor (and fake R2) for :mod:`vgi_rpc.vendor` tests.

Implements the vendor's HTTP API and, on the same port, an object store whose
"pre-signed" URLs behave like R2's:

Mirrors the staging Worker (``blobs.staging.queryfarm.cloud``): errors are
``{error, message, ...}``, times ISO-8601 UTC, object ids 32 lowercase hex.

* ``POST /v1/blobs/put`` ``{size, sha256_b64, purpose, scope?, content_encoding?,
  idempotency_key?}`` returns ``{object_id, region, upload_url, upload_method,
  required_headers, expires_at, download_url?, download_expires_at?}``.  The
  same ``idempotency_key`` with the same bytes returns the same object; with
  other bytes, 409 ``idempotency_conflict``.  Over ``max_object_bytes`` or the
  tenant quota: 413 ``object_too_large`` / ``quota_exceeded``.
  The PUT URL enforces the declared ``Content-Length`` and
  ``x-amz-checksum-sha256`` header *and* that the body really hashes to it
  (R2 answers ``400 BadDigest``), plus ``Content-Encoding`` when declared.
  ``download_url`` is returned only for purpose ``rpc``.
* ``POST /v1/blobs/get`` ``{object_id}`` returns
  ``{object_id, download_url, size, sha256_b64, content_encoding, expires_at}``
  for the calling tenant's objects; never uploaded is 404 ``not_uploaded``,
  anyone else's (or an unknown id) 404 ``not_found``.
* ``POST /v1/blobs/delete`` ``{object_ids}`` and
  ``POST /v1/blobs/delete_scope`` ``{shard, execution_id}`` return ``{deleted}``.
* ``POST /v1/upload_urls`` ``{count <= 100}`` returns unsized
  ``{region, urls: [{object_id, upload_url, download_url, expires_at, download_expires_at}]}``.
* ``GET /v1/info`` returns ``{tenant, region, jurisdiction, upload_hosts,
  download_hosts, url_prefixes, limits, ttl_seconds}``.

Every ``/v1`` route needs ``Authorization: Bearer <token>`` for a token given
to the constructor.  Signed URLs carry an ``X-Amz-Signature`` query parameter
the store checks, so a URL with its query stripped fails as on R2.  Download
URLs support ``HEAD`` and single ``Range`` requests.

Test hooks on :class:`FakeVendor`: :meth:`~FakeVendor.fail_next` (inject 5xx),
:meth:`~FakeVendor.corrupt` (tamper with a stored object),
:attr:`~FakeVendor.requests` (method, path, header names) and
:meth:`~FakeVendor.stats`.

Usage::

    with serve_fake_vendor({"tok": "tenant-a"}) as fake:
        storage = VendorStorage(fake.url, "tok")
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import secrets
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import cast
from urllib.parse import parse_qs
from wsgiref.simple_server import make_server
from wsgiref.types import StartResponse, WSGIApplication, WSGIEnvironment

from vgi_rpc.conformance.fake_storage import (
    _MALFORMED_LENGTH,
    _parse_range,
    _read_body,
    _SilentHandler,
    _ThreadingWSGIServer,
)

__all__ = ["FakeVendor", "FakeVendorServer", "serve_fake_vendor"]

#: Scope fields the staging Worker accepts.
_SCOPE_RE = re.compile(r"[A-Za-z0-9._:-]{1,128}")


@dataclass
class _Object:
    tenant: str
    purpose: str
    size: int | None  # None = unsized (/v1/upload_urls)
    sha256_b64: str | None
    content_encoding: str | None
    scope: tuple[str, str] | None
    data: bytes | None = None
    stored_encoding: str | None = None
    idempotency_key: str | None = None


@dataclass
class _Grant:
    object_id: str
    method: str  # "PUT" or "GET"
    signature: str
    expires: datetime


@dataclass
class FakeVendor:
    """State and request handling of the fake vendor.

    Attributes:
        tokens: Bearer token to tenant name.
        region: Region reported by ``/v1/info``.
        url_ttl: Lifetime of PUT and ``get`` download URLs (300 s, as staging).
        rpc_download_ttl: Lifetime of ``rpc`` download URLs from ``put``.
        max_object_bytes: Largest declarable object (413 ``object_too_large``).
        quota_bytes: Per-tenant bytes charged at ``put`` (413 ``quota_exceeded``).
        base_url: Set by :func:`serve_fake_vendor` once bound.
        requests: ``(method, path, header names)`` for every request.

    """

    tokens: dict[str, str]
    region: str = "auto-enam"
    url_ttl: timedelta = timedelta(seconds=300)
    rpc_download_ttl: timedelta = timedelta(seconds=3600)
    max_object_bytes: int = 4 * 1024**3
    quota_bytes: int = 10 * 1024**3
    base_url: str = ""
    requests: list[tuple[str, str, frozenset[str]]] = field(default_factory=list)
    _objects: dict[str, _Object] = field(default_factory=dict)
    _grants: dict[str, _Grant] = field(default_factory=dict)
    _faults: list[list[object]] = field(default_factory=list)
    _hits: dict[str, int] = field(default_factory=dict)
    _used: dict[str, int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    # -- test hooks -------------------------------------------------------

    def fail_next(self, path_prefix: str, *, status: int = 503, count: int = 1) -> None:
        """Answer the next *count* requests whose path starts with *path_prefix* with *status*.

        Args:
            path_prefix: e.g. ``"/v1/blobs/put"`` or ``"/r2/put/"``.
            status: HTTP status to return.
            count: How many requests to fail.

        """
        with self._lock:
            self._faults.append([path_prefix, status, count])

    def corrupt(self, object_id: str, data: bytes) -> None:
        """Replace a stored object's bytes, bypassing the checksum (simulates tampering).

        Args:
            object_id: The object to change.
            data: Its new bytes.

        """
        with self._lock:
            self._objects[object_id].data = data

    def object_data(self, object_id: str) -> bytes | None:
        """Return an object's stored bytes, or ``None`` if never uploaded or deleted.

        Args:
            object_id: The object id.

        Returns:
            The bytes, or ``None``.

        """
        with self._lock:
            obj = self._objects.get(object_id)
            return None if obj is None else obj.data

    def object_ids(self) -> list[str]:
        """Return all live object ids.

        Returns:
            The ids.

        """
        with self._lock:
            return list(self._objects)

    def stats(self) -> dict[str, int]:
        """Return route hit counters plus ``object_count``.

        Returns:
            The counters.

        """
        with self._lock:
            return {"object_count": len(self._objects), **self._hits}

    @property
    def host(self) -> str:
        """``host:port`` the signed URLs point at."""
        return self.base_url.split("://", 1)[1]

    # -- internals --------------------------------------------------------

    def _hit(self, name: str) -> None:
        self._hits[name] = self._hits.get(name, 0) + 1

    def _take_fault(self, path: str) -> int | None:
        for fault in self._faults:
            prefix, status, remaining = fault
            if path.startswith(cast("str", prefix)) and cast("int", remaining) > 0:
                fault[2] = cast("int", remaining) - 1
                return cast("int", status)
        return None

    def _sign(self, object_id: str, method: str, ttl: timedelta | None = None) -> tuple[str, str]:
        grant_id = secrets.token_hex(16)
        expires = datetime.now(UTC) + (ttl or self.url_ttl)
        signature = secrets.token_hex(32)
        self._grants[grant_id] = _Grant(object_id, method, signature, expires)
        route = "put" if method == "PUT" else "get"
        url = (
            f"{self.base_url}/r2/{route}/{grant_id}"
            f"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=FAKE%2F{grant_id[:8]}&X-Amz-Signature={signature}"
        )
        return url, _iso(expires)

    def app(self) -> WSGIApplication:
        """Build the WSGI app.

        Returns:
            The app.

        """

        def application(environ: WSGIEnvironment, start_response: StartResponse) -> Iterable[bytes]:
            method = str(environ["REQUEST_METHOD"]).upper()
            path = str(environ["PATH_INFO"])
            # Always drain the body first; see fake_storage.make_app.
            body = _read_body(environ)
            header_names = frozenset(k for k in environ if k.startswith("HTTP_") or k in ("CONTENT_TYPE",))
            with self._lock:
                self.requests.append((method, path, header_names))
                fault = self._take_fault(path)
                if fault is not None:
                    return _error(start_response, fault, "injected_fault", "injected by FakeVendor.fail_next")
                if path.startswith("/v1/"):
                    return self._api(environ, start_response, method, path, body)
                if path.startswith("/r2/"):
                    return self._store(environ, start_response, method, path, body)
            return _error(start_response, 404, "not_found", "no such route")

        return application

    # -- vendor API -------------------------------------------------------

    def _api(
        self, environ: WSGIEnvironment, start_response: StartResponse, method: str, path: str, body: bytes
    ) -> list[bytes]:
        auth = str(environ.get("HTTP_AUTHORIZATION", ""))
        tenant = self.tokens.get(auth[len("Bearer ") :]) if auth.startswith("Bearer ") else None
        if tenant is None:
            return _error(start_response, 401, "unauthorized", "missing or unknown bearer token")
        self._hit(path)
        if method == "GET" and path == "/v1/info":
            return _json(start_response, 200, self._info(tenant))
        if method != "POST":
            return _error(start_response, 405, "bad_request", "method not allowed")
        try:
            req = json.loads(body or b"{}")
            if not isinstance(req, dict):
                raise ValueError
        except ValueError:
            return _error(start_response, 400, "bad_request", "body must be a JSON object")
        if path == "/v1/blobs/put":
            return self._put(start_response, tenant, req)
        if path == "/v1/blobs/get":
            object_id = str(req.get("object_id", ""))
            obj = self._objects.get(object_id)
            if obj is None or obj.tenant != tenant or obj.size is None:
                return _error(start_response, 404, "not_found", "no such object")
            if obj.data is None:
                return _error(start_response, 404, "not_uploaded", "object was never uploaded")
            url, expires = self._sign(object_id, "GET")
            return _json(
                start_response,
                200,
                {
                    "object_id": object_id,
                    "download_url": url,
                    "size": obj.size,
                    "sha256_b64": obj.sha256_b64,
                    "content_encoding": obj.content_encoding,
                    "expires_at": expires,
                },
            )
        if path == "/v1/blobs/delete":
            ids = req.get("object_ids")
            if not isinstance(ids, list) or len(ids) > 1000:
                return _error(start_response, 400, "bad_request", "object_ids must be a list of at most 1000")
            deleted = 0
            for object_id in ids:
                obj = self._objects.get(str(object_id))
                if obj is not None and obj.tenant == tenant:
                    del self._objects[str(object_id)]
                    deleted += 1
            return _json(start_response, 200, {"deleted": deleted})
        if path == "/v1/blobs/delete_scope":
            scope = (str(req.get("shard", "")), str(req.get("execution_id", "")))
            doomed = [k for k, o in self._objects.items() if o.tenant == tenant and o.scope == scope]
            for key in doomed:
                del self._objects[key]
            return _json(start_response, 200, {"deleted": len(doomed)})
        if path == "/v1/upload_urls":
            count = req.get("count", 1)
            if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 100:
                return _error(start_response, 400, "bad_request", "count must be 1..100")
            urls = []
            for _ in range(count):
                object_id = secrets.token_hex(16)
                self._objects[object_id] = _Object(tenant, "rpc", None, None, None, None)
                put_url, expires = self._sign(object_id, "PUT")
                get_url, get_expires = self._sign(object_id, "GET", self.rpc_download_ttl)
                urls.append(
                    {
                        "object_id": object_id,
                        "upload_url": put_url,
                        "download_url": get_url,
                        "expires_at": expires,
                        "download_expires_at": get_expires,
                    }
                )
            return _json(start_response, 200, {"region": self.region, "urls": urls})
        return _error(start_response, 404, "not_found", "no such route")

    def _info(self, tenant: str) -> dict[str, object]:
        jurisdiction = self.region if self.region in ("eu", "us") else None
        return {
            "tenant": tenant,
            "region": self.region,
            "jurisdiction": jurisdiction,
            "upload_hosts": [self.host],
            "download_hosts": [self.host],
            "url_prefixes": [f"{self.base_url}/r2/"],
            "limits": {
                "max_object_bytes": self.max_object_bytes,
                "quota_bytes": self.quota_bytes,
                "used_bytes": self._used.get(tenant, 0),
                "max_upload_urls": 100,
                "max_delete_ids": 1000,
            },
            "ttl_seconds": {
                "put": int(self.url_ttl.total_seconds()),
                "rpc_download": int(self.rpc_download_ttl.total_seconds()),
                "get": int(self.url_ttl.total_seconds()),
            },
        }

    def _put(self, start_response: StartResponse, tenant: str, req: Mapping[str, object]) -> list[bytes]:
        size = req.get("size")
        sha = req.get("sha256_b64")
        purpose = req.get("purpose")
        encoding = req.get("content_encoding")
        scope = req.get("scope")
        key = req.get("idempotency_key")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            return _error(start_response, 400, "bad_request", "size must be a non-negative integer")
        try:
            sha_ok = isinstance(sha, str) and len(base64.b64decode(sha, validate=True)) == 32
        except ValueError:
            sha_ok = False
        if not sha_ok or not isinstance(sha, str):
            return _error(start_response, 400, "bad_request", "sha256_b64 must be a base64 SHA-256")
        if purpose not in ("rpc", "fs"):
            return _error(start_response, 400, "bad_request", "purpose must be rpc or fs")
        if encoding is not None and not isinstance(encoding, str):
            return _error(start_response, 400, "bad_request", "content_encoding must be a string")
        if key is not None and (not isinstance(key, str) or not key):
            return _error(start_response, 400, "bad_request", "idempotency_key must be a non-empty string")
        scope_key: tuple[str, str] | None = None
        if scope is not None:
            if not isinstance(scope, dict) or "shard" not in scope or "execution_id" not in scope:
                return _error(start_response, 400, "bad_request", "scope needs shard and execution_id")
            scope_key = (str(scope["shard"]), str(scope["execution_id"]))
            for name, value in zip(("shard", "execution_id"), scope_key, strict=True):
                if not _SCOPE_RE.fullmatch(value):
                    return _error(start_response, 400, "bad_request", f"scope.{name}: 1-128 chars [A-Za-z0-9._:-]")
        if size > self.max_object_bytes:
            return _error(
                start_response,
                413,
                "object_too_large",
                "size exceeds max_object_bytes",
                max_bytes=self.max_object_bytes,
            )
        existing = next(
            (
                (oid, o)
                for oid, o in self._objects.items()
                if key is not None and o.tenant == tenant and o.idempotency_key == key
            ),
            None,
        )
        if existing is not None:
            object_id, obj = existing
            if (obj.size, obj.sha256_b64, obj.purpose, obj.content_encoding) != (size, sha, purpose, encoding):
                return _error(start_response, 409, "idempotency_conflict", "idempotency_key reused for other bytes")
        else:
            if self._used.get(tenant, 0) + size > self.quota_bytes:
                return _error(start_response, 413, "quota_exceeded", "tenant quota exhausted")
            self._used[tenant] = self._used.get(tenant, 0) + size
            object_id = secrets.token_hex(16)
            self._objects[object_id] = _Object(
                tenant,
                str(purpose),
                size,
                sha,
                encoding,
                scope_key,
                idempotency_key=key if isinstance(key, str) else None,
            )
        upload_url, expires = self._sign(object_id, "PUT")
        required = {"content-length": str(size), "x-amz-checksum-sha256": sha}
        if encoding:
            required["content-encoding"] = encoding
        resp: dict[str, object] = {
            "object_id": object_id,
            "region": self.region,
            "upload_url": upload_url,
            "upload_method": "PUT",
            "required_headers": required,
            "expires_at": expires,
        }
        if purpose == "rpc":
            resp["download_url"], resp["download_expires_at"] = self._sign(object_id, "GET", self.rpc_download_ttl)
        return _json(start_response, 200, resp)

    # -- fake R2 ----------------------------------------------------------

    def _store(
        self, environ: WSGIEnvironment, start_response: StartResponse, method: str, path: str, body: bytes
    ) -> list[bytes]:
        parts = path.split("/")
        if len(parts) != 4:
            return _xml(start_response, 404, "NoSuchKey")
        _, _, route, grant_id = parts
        grant = self._grants.get(grant_id)
        query = parse_qs(str(environ.get("QUERY_STRING", "")))
        if grant is None or query.get("X-Amz-Signature", [""])[0] != grant.signature:
            return _xml(start_response, 403, "SignatureDoesNotMatch")
        if datetime.now(UTC) > grant.expires:
            return _xml(start_response, 403, "AccessDenied")
        allowed = ("PUT",) if grant.method == "PUT" else ("GET", "HEAD")
        if method not in allowed or route != ("put" if grant.method == "PUT" else "get"):
            return _xml(start_response, 403, "SignatureDoesNotMatch")
        obj = self._objects.get(grant.object_id)
        if method == "PUT":
            self._hit("r2_put")
            if obj is None:
                return _xml(start_response, 404, "NoSuchKey")
            return self._store_put(environ, start_response, obj, body)
        self._hit("r2_get")
        if obj is None or obj.data is None:
            return _xml(start_response, 404, "NoSuchKey")
        return _serve_object(environ, start_response, method, obj.data, obj.stored_encoding)

    def _store_put(
        self, environ: WSGIEnvironment, start_response: StartResponse, obj: _Object, body: bytes
    ) -> list[bytes]:
        if body is _MALFORMED_LENGTH:
            return _xml(start_response, 400, "InvalidArgument")
        encoding = environ.get("HTTP_CONTENT_ENCODING")
        if obj.size is not None:
            # Sized grant: the signed headers must be present and true.
            if str(environ.get("CONTENT_LENGTH", "")) != str(obj.size) or len(body) != obj.size:
                return _xml(start_response, 403, "SignatureDoesNotMatch")
            if environ.get("HTTP_X_AMZ_CHECKSUM_SHA256") != obj.sha256_b64:
                return _xml(start_response, 403, "SignatureDoesNotMatch")
            if base64.b64encode(hashlib.sha256(body).digest()).decode() != obj.sha256_b64:
                return _xml(start_response, 400, "BadDigest")
            if obj.content_encoding and encoding != obj.content_encoding:
                return _xml(start_response, 403, "SignatureDoesNotMatch")
        obj.data = body
        obj.stored_encoding = str(encoding) if encoding else None
        start_response("200 OK", [("Content-Length", "0"), ("ETag", '"fake"')])
        return [b""]


def _serve_object(
    environ: WSGIEnvironment, start_response: StartResponse, method: str, data: bytes, encoding: str | None
) -> list[bytes]:
    headers = [("Content-Type", "application/octet-stream"), ("Accept-Ranges", "bytes")]
    if encoding:
        headers.append(("Content-Encoding", encoding))
    range_header = str(environ.get("HTTP_RANGE", ""))
    if range_header and method == "GET":
        rng = _parse_range(range_header, len(data))
        if rng is None:
            start_response("416 Range Not Satisfiable", [("Content-Range", f"bytes */{len(data)}")])
            return [b""]
        start, end = rng
        chunk = data[start : end + 1]
        headers += [("Content-Range", f"bytes {start}-{end}/{len(data)}"), ("Content-Length", str(len(chunk)))]
        start_response("206 Partial Content", headers)
        return [chunk]
    headers.append(("Content-Length", str(len(data))))
    start_response("200 OK", headers)
    return [b""] if method == "HEAD" else [data]


_REASONS = {
    200: "OK",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    409: "Conflict",
    413: "Content Too Large",
}


def _status(code: int) -> str:
    return f"{code} {_REASONS.get(code, 'Service Unavailable' if code >= 500 else 'Error')}"


def _iso(moment: datetime) -> str:
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _error(start_response: StartResponse, code: int, error: str, message: str, **extra: object) -> list[bytes]:
    return _json(start_response, code, {"error": error, "message": message, **extra})


def _json(start_response: StartResponse, code: int, body: Mapping[str, object]) -> list[bytes]:
    payload = json.dumps(body).encode()
    start_response(_status(code), [("Content-Type", "application/json"), ("Content-Length", str(len(payload)))])
    return [payload]


def _xml(start_response: StartResponse, code: int, error: str) -> list[bytes]:
    payload = f"<Error><Code>{error}</Code></Error>".encode()
    start_response(_status(code), [("Content-Type", "application/xml"), ("Content-Length", str(len(payload)))])
    return [payload]


class FakeVendorServer:
    """A running fake vendor; a context manager that shuts it down on exit."""

    def __init__(self, vendor: FakeVendor, server: _ThreadingWSGIServer) -> None:
        """Wrap a started server.

        Args:
            vendor: The vendor state.
            server: The bound WSGI server.

        """
        self.vendor = vendor
        self._server = server
        self._stopped = False

    @property
    def url(self) -> str:
        """The vendor's base URL."""
        return self.vendor.base_url

    def close(self) -> None:
        """Stop the server."""
        if not self._stopped:
            self._stopped = True
            self._server.shutdown()
            self._server.server_close()

    def __enter__(self) -> FakeVendorServer:
        """Enter the context manager.

        Returns:
            This server.

        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Stop the server."""
        self.close()


def serve_fake_vendor(
    tokens: Mapping[str, str] | None = None, *, host: str = "127.0.0.1", port: int = 0, region: str = "auto-enam"
) -> FakeVendorServer:
    """Start a fake vendor on a background thread.

    Args:
        tokens: Bearer token to tenant; defaults to ``{"test-token": "tenant-a"}``.
        host: Bind host.
        port: Bind port (``0`` picks a free one).
        region: Region reported by ``/v1/info``.

    Returns:
        The running :class:`FakeVendorServer`.

    """
    vendor = FakeVendor(tokens=dict(tokens or {"test-token": "tenant-a"}), region=region)
    placeholder = cast("WSGIApplication", lambda environ, start_response: [b""])
    server = make_server(host, port, placeholder, server_class=_ThreadingWSGIServer, handler_class=_SilentHandler)
    vendor.base_url = f"http://{host}:{server.server_address[1]}"
    server.set_app(vendor.app())
    threading.Thread(target=server.serve_forever, name="fake-vendor", daemon=True).start()
    return FakeVendorServer(vendor, server)


def main() -> None:
    """Run the fake vendor standalone; prints ``PORT:<n>``."""
    parser = argparse.ArgumentParser(prog="vgi-rpc-fake-vendor")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--token", action="append", default=[], help="TOKEN=TENANT (repeatable)")
    args = parser.parse_args()
    tokens = dict(t.split("=", 1) for t in args.token) if args.token else None
    fake = serve_fake_vendor(tokens, host=args.host, port=args.port)
    print(f"PORT:{fake.url.rsplit(':', 1)[1]}", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        fake.close()


if __name__ == "__main__":
    main()
