# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""In-memory fake blob vendor (and fake R2) for :mod:`vgi_rpc.vendor` tests.

Implements the vendor's HTTP API and, on the same port, an object store whose
"pre-signed" URLs behave like R2's:

Mirrors the staging Worker (``blobs.staging.queryfarm.cloud``): errors are
``{error, message, ...}``, times ISO-8601 UTC, object ids 32 lowercase hex.

* ``POST /v1/blobs/put`` ``{size, sha256_b64, purpose, scope?, content_encoding?,
  idempotency_key?, origin?}`` returns ``{object_id, region, upload_url, upload_method,
  required_headers, expires_at, download_url?, download_expires_at?}``.  The
  same ``idempotency_key`` with the same bytes returns the same object; with
  other bytes, 409 ``idempotency_conflict``.  Over ``max_object_bytes`` or the
  tenant quota: 413 ``object_too_large`` / ``quota_exceeded``.
  The PUT URL enforces the declared size and ``x-amz-checksum-sha256`` like
  R2: a body of any other length is ``400 IncompleteBody``, a checksum header
  absent or different from the signed value (or a ``Content-Encoding`` other
  than the declared one) is ``403 SignatureDoesNotMatch``, and a body that
  does not hash to the signed value is ``400 BadDigest``.
  ``download_url`` is returned only for purpose ``rpc``.
  ``origin: "client_upload"`` marks a grant a server mints for a *client*
  to upload a request (``UploadUrlProvider.generate_upload_url``); the real
  vendor ignores the field, the fake counts and faults those grants apart
  from the server's own writes. ``origin: "state_ref"`` marks a server write
  of a stored ``StateRef`` object (``ObjectStore.put_object``), counted as
  ``puts_state_ref`` (and, being purpose ``rpc``, in ``puts_rpc``). Any other
  value, or none, is a plain server write.
* ``POST /v1/blobs/get`` ``{object_id}`` returns
  ``{object_id, download_url, size, sha256_b64, content_encoding, expires_at}``
  for the calling tenant's objects; never uploaded is 404 ``not_uploaded``,
  anyone else's (or an unknown id) 404 ``not_found``.
* ``POST /v1/blobs/delete`` ``{object_ids}`` and
  ``POST /v1/blobs/delete_scope`` ``{shard, execution_id}`` return ``{deleted}``.
* ``POST /v1/upload_urls`` is gone (protocol 3.4): the vendor dropped its
  unsized upload path, so the route answers 404 like any unknown path.
* ``GET /v1/info`` returns ``{tenant, region, jurisdiction, upload_hosts,
  download_hosts, url_prefixes, limits, ttl_seconds}``.

Every ``/v1`` route needs ``Authorization: Bearer <token>`` for a token given
to the constructor.  Signed URLs carry an ``X-Amz-Signature`` query parameter
the store checks, so a URL with its query stripped fails as on R2.  Download
URLs support ``HEAD`` and single ``Range`` requests.

Test hooks on :class:`FakeVendor`: :meth:`~FakeVendor.fail_next` (inject 5xx),
:meth:`~FakeVendor.corrupt` (tamper with a stored object),
:meth:`~FakeVendor.set_download_fault` (corrupt or lose downloads by purpose),
:attr:`~FakeVendor.requests` (method, path, header names),
:meth:`~FakeVendor.stats` and :meth:`~FakeVendor.test_stats`.

Test routes (``--test-routes`` / ``test_routes=True``), unauthenticated, for
test lanes in any language (e.g. read from SQL with ``read_json_auto``):

* ``GET /__stats__`` returns :meth:`~FakeVendor.test_stats` as JSON.
* ``GET /__admin__/fault?purpose=fs|rpc&action=corrupt|missing&count=N``
  arms a download fault (see :meth:`~FakeVendor.set_download_fault`); it
  *replaces* any fault armed for that purpose, so repeating the request is
  harmless.
* ``GET /__admin__/fault?purpose=upload&action=corrupt&count=N`` corrupts the
  next *N* signed ``PUT`` bodies of client uploads in transit (see
  :meth:`~FakeVendor.set_upload_fault`), so the store's checksum check
  refuses them with ``400 BadDigest``; server writes (results, function
  storage) are never touched. It also replaces any armed upload fault.
* ``GET /__admin__/delete?origin=client_upload|state_ref`` deletes every
  object granted with that ``origin`` (client uploads / argument objects, or
  stored ``StateRef`` objects) and returns ``{"ok": true, "deleted": N}``:
  a stand-in for the bucket lifecycle, so a test can make a reference
  "missing" (``arguments_ref_missing`` / ``state_ref_missing``).
* ``GET /__admin__/reset`` clears every fault (armed, applied and
  :meth:`~FakeVendor.fail_next`).

Only ``GET`` has side effects; ``HEAD`` answers ``200`` with the same
headers and no effect (HTTP clients such as DuckDB's httpfs may ``HEAD``
first). The JSON bodies are padded with spaces to a fixed
:data:`TEST_ROUTE_BODY_BYTES` so a ``HEAD``'s ``Content-Length`` stays true
for the following ``GET``, ranged reads work, and responses carry
``Cache-Control: no-store``.

Usage::

    with serve_fake_vendor({"tok": "tenant-a"}) as fake:
        storage = VendorStorage(fake.url, "tok")
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import secrets
import signal
import sys
import threading
import time
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

__all__ = [
    "CLIENT_UPLOAD_ORIGIN",
    "STATS_KEYS",
    "TEST_ROUTE_BODY_BYTES",
    "FakeVendor",
    "FakeVendorServer",
    "serve_fake_vendor",
]

#: Scope fields the staging Worker accepts.
_SCOPE_RE = re.compile(r"[A-Za-z0-9._:-]{1,128}")

#: Keys of :meth:`FakeVendor.test_stats` (and ``GET /__stats__``), in order.
STATS_KEYS = (
    "puts",
    "gets",
    "deletes",
    "delete_scopes",
    "upload_urls",
    "storage_puts",
    "storage_gets",
    "objects",
    "bytes",
    "bytes_uploaded",
    "bytes_downloaded",
    "upload_refusals",
    "upload_bad_digest",
    "upload_incomplete_body",
    "upload_signature_mismatch",
    "uploads_corrupted",
    "puts_rpc",
    "puts_fs",
    "puts_upload",
    "client_uploads",
    "client_upload_bytes",
    "client_upload_refusals",
    "puts_state_ref",
    "gets_state_ref",
    "gets_upload",
)

#: Every test-route JSON body is padded with spaces to exactly this many bytes.
TEST_ROUTE_BODY_BYTES = 1024

_FAULT_ACTIONS = ("corrupt", "missing")
_PURPOSES = ("rpc", "fs")
#: Pseudo-purpose of ``/__admin__/fault`` that targets signed PUTs, not downloads.
_UPLOAD_FAULT_PURPOSE = "upload"
#: ``origin`` of a ``/v1/blobs/put`` grant minted for a client upload.
CLIENT_UPLOAD_ORIGIN = "client_upload"
#: ``origin`` of a ``/v1/blobs/put`` grant for a stored ``StateRef`` object.
STATE_REF_ORIGIN = "state_ref"
#: Origins ``/__admin__/delete`` accepts.
_DELETE_ORIGINS = (CLIENT_UPLOAD_ORIGIN, STATE_REF_ORIGIN)


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
    client_upload: bool = False
    state_ref: bool = False


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
        test_routes: Serve the unauthenticated ``/__stats__`` and
            ``/__admin__/*`` routes (see the module docstring).
        requests: ``(method, path, header names)`` for every request.

    """

    tokens: dict[str, str]
    region: str = "auto-enam"
    url_ttl: timedelta = timedelta(seconds=300)
    rpc_download_ttl: timedelta = timedelta(seconds=3600)
    max_object_bytes: int = 4 * 1024**3
    quota_bytes: int = 10 * 1024**3
    base_url: str = ""
    test_routes: bool = False
    requests: list[tuple[str, str, frozenset[str]]] = field(default_factory=list)
    _objects: dict[str, _Object] = field(default_factory=dict)
    _grants: dict[str, _Grant] = field(default_factory=dict)
    _faults: list[list[object]] = field(default_factory=list)
    _hits: dict[str, int] = field(default_factory=dict)
    _used: dict[str, int] = field(default_factory=dict)
    _armed: dict[str, tuple[str, int]] = field(default_factory=dict)
    _faulted: dict[str, str] = field(default_factory=dict)
    _upload_corruptions: int = 0
    _bytes_uploaded: int = 0
    _bytes_downloaded: int = 0
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

    def set_download_fault(self, purpose: str, action: str, count: int) -> None:
        """Fault the next *count* objects of *purpose* that are downloaded.

        The first signed-URL ``GET`` (ranged or not) of an object with that
        purpose, while the fault has objects left, takes one and marks the
        object; from then on *every* ``GET`` of it is faulted, until
        :meth:`reset_faults`. So a client that probes with a ranged ``GET``,
        fetches in ranges or retries with a fresh URL sees one consistent
        fault, as with an object damaged or expired at rest:

        * ``corrupt`` serves the object with every byte XORed with ``0xFF``
          (same length, different content);
        * ``missing`` answers ``404 NoSuchKey``.

        ``HEAD`` is never faulted. Arming *replaces* any fault already armed
        for *purpose*; objects already marked stay marked.

        Args:
            purpose: ``"fs"`` or ``"rpc"``.
            action: ``"corrupt"`` or ``"missing"``.
            count: How many objects to fault (``0`` disarms).

        Raises:
            ValueError: On an unknown purpose or action, or a negative count.

        """
        if purpose not in _PURPOSES:
            raise ValueError(f"purpose must be one of {_PURPOSES}, got {purpose!r}")
        if action not in _FAULT_ACTIONS:
            raise ValueError(f"action must be one of {_FAULT_ACTIONS}, got {action!r}")
        if count < 0:
            raise ValueError(f"count must be >= 0, got {count}")
        with self._lock:
            if count:
                self._armed[purpose] = (action, count)
            else:
                self._armed.pop(purpose, None)

    def set_upload_fault(self, action: str, count: int) -> None:
        """Corrupt the next *count* client-upload ``PUT`` bodies in transit.

        Only PUTs to client-upload objects count: grants from
        ``/v1/blobs/put`` with ``origin: "client_upload"`` and the unsized
        ``/v1/upload_urls`` (only ever handed to clients).  The server's own
        writes (results, function storage) are never corrupted, so a test
        may run large-result queries while the fault is armed.  Every byte
        of the body is XORed with ``0xFF`` before the store verifies it, as
        if damaged on the wire, so a sized (checksummed) PUT is refused with
        ``400 BadDigest`` and nothing is stored.  An unsized PUT would store
        the damaged bytes.  Arming *replaces* any armed upload fault.

        Args:
            action: ``"corrupt"`` (the only upload action).
            count: How many client-upload PUTs to corrupt (``0`` disarms).

        Raises:
            ValueError: On another action or a negative count.

        """
        if action != "corrupt":
            raise ValueError(f"upload faults support only action 'corrupt', got {action!r}")
        if count < 0:
            raise ValueError(f"count must be >= 0, got {count}")
        with self._lock:
            self._upload_corruptions = count

    def delete_by_origin(self, origin: str) -> int:
        """Delete every object granted with *origin* (stands in for bucket lifecycle).

        Args:
            origin: ``"client_upload"`` (argument / request uploads) or
                ``"state_ref"`` (stored ``StateRef`` objects).

        Returns:
            How many objects were deleted.

        Raises:
            ValueError: On another origin.

        """
        if origin not in _DELETE_ORIGINS:
            raise ValueError(f"origin must be one of {_DELETE_ORIGINS}, got {origin!r}")
        with self._lock:
            doomed = [
                key
                for key, obj in self._objects.items()
                if (obj.client_upload if origin == CLIENT_UPLOAD_ORIGIN else obj.state_ref)
            ]
            for key in doomed:
                del self._objects[key]
        return len(doomed)

    def reset_faults(self) -> None:
        """Clear armed and applied download and upload faults and :meth:`fail_next` faults."""
        with self._lock:
            self._armed.clear()
            self._faulted.clear()
            self._faults.clear()
            self._upload_corruptions = 0

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

    def test_stats(self) -> dict[str, int]:
        """Return the counters served at ``GET /__stats__``.

        Keys (:data:`STATS_KEYS`): ``puts`` / ``gets`` / ``deletes`` /
        ``delete_scopes`` / ``upload_urls`` (authenticated calls to
        ``/v1/blobs/put``, ``/v1/blobs/get``, ``/v1/blobs/delete``,
        ``/v1/blobs/delete_scope`` and ``/v1/upload_urls``, either purpose),
        ``storage_puts`` / ``storage_gets`` (signed-URL ``PUT`` and
        ``GET``/``HEAD`` requests with a valid signature), ``objects`` and
        ``bytes`` (live uploaded objects and their total size) and
        ``bytes_uploaded`` / ``bytes_downloaded`` (cumulative bytes accepted
        by signed ``PUT`` and served by signed ``GET``), ``upload_refusals``
        (signed ``PUT`` bodies the store refused, the sum of
        ``upload_bad_digest``, ``upload_incomplete_body`` and
        ``upload_signature_mismatch``) and ``uploads_corrupted`` (PUT bodies
        damaged by an upload fault, which only client uploads take).

        By purpose, for ``/v1/blobs/put`` grants issued (``200``; ``puts``
        also counts refused calls): ``puts_rpc`` (purpose ``rpc``, server
        writes such as externalized results), ``puts_fs`` (purpose ``fs``,
        function storage) and ``puts_upload`` (``origin: "client_upload"``,
        minted for a client to upload a request; not in the other two).
        Signed ``PUT`` requests to client-upload objects (those grants and
        the unsized ``/v1/upload_urls``): ``client_uploads`` /
        ``client_upload_bytes`` (accepted, and their bytes) and
        ``client_upload_refusals`` (refused by the store).

        Protocol 3.4 (``STATE_REF_V1.md`` §10.3): ``puts_state_ref`` (grants
        with ``origin: "state_ref"``, a subset of ``puts_rpc``),
        ``gets_state_ref`` and ``gets_upload`` (``/v1/blobs/get`` calls on
        objects granted with ``origin`` ``state_ref`` and ``client_upload``).
        ``upload_urls`` stays in the keys and is always ``0``: the unsized
        ``/v1/upload_urls`` route no longer exists.

        All but ``objects`` and ``bytes`` only grow, so tests compare
        before/after values.

        Returns:
            The counters.

        """
        with self._lock:
            live = [o.data for o in self._objects.values() if o.data is not None]
            hits = self._hits
            return {
                "puts": hits.get("/v1/blobs/put", 0),
                "gets": hits.get("/v1/blobs/get", 0),
                "deletes": hits.get("/v1/blobs/delete", 0),
                "delete_scopes": hits.get("/v1/blobs/delete_scope", 0),
                "upload_urls": 0,
                "storage_puts": hits.get("r2_put", 0),
                "storage_gets": hits.get("r2_get", 0),
                "objects": len(live),
                "bytes": sum(len(d) for d in live),
                "bytes_uploaded": self._bytes_uploaded,
                "bytes_downloaded": self._bytes_downloaded,
                "upload_refusals": sum(
                    hits.get(k, 0) for k in ("upload_bad_digest", "upload_incomplete_body", "upload_signature_mismatch")
                ),
                "upload_bad_digest": hits.get("upload_bad_digest", 0),
                "upload_incomplete_body": hits.get("upload_incomplete_body", 0),
                "upload_signature_mismatch": hits.get("upload_signature_mismatch", 0),
                "uploads_corrupted": hits.get("uploads_corrupted", 0),
                "puts_rpc": hits.get("puts_rpc", 0),
                "puts_fs": hits.get("puts_fs", 0),
                "puts_upload": hits.get("puts_upload", 0),
                "client_uploads": hits.get("client_uploads", 0),
                "client_upload_bytes": hits.get("client_upload_bytes", 0),
                "client_upload_refusals": hits.get("client_upload_refusals", 0),
                "puts_state_ref": hits.get("puts_state_ref", 0),
                "gets_state_ref": hits.get("gets_state_ref", 0),
                "gets_upload": hits.get("gets_upload", 0),
            }

    @property
    def host(self) -> str:
        """``host:port`` the signed URLs point at."""
        return self.base_url.split("://", 1)[1]

    # -- internals --------------------------------------------------------

    def _hit(self, name: str, n: int = 1) -> None:
        self._hits[name] = self._hits.get(name, 0) + n

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
            if self.test_routes and path.startswith(("/__stats__", "/__admin__/")):
                return self._test_route(environ, start_response, method, path)
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

    # -- test routes ------------------------------------------------------

    def _test_route(
        self, environ: WSGIEnvironment, start_response: StartResponse, method: str, path: str
    ) -> list[bytes]:
        if method not in ("GET", "HEAD"):
            return _error(start_response, 405, "bad_request", "method not allowed")
        effect = method == "GET"
        body: dict[str, object]
        if path == "/__stats__":
            body = dict(self.test_stats())
        elif path == "/__admin__/reset":
            if effect:
                self.reset_faults()
            body = {"ok": True}
        elif path == "/__admin__/fault":
            query = parse_qs(str(environ.get("QUERY_STRING", "")))
            purpose = query.get("purpose", [""])[0]
            action = query.get("action", [""])[0]
            raw_count = query.get("count", ["1"])[0]
            try:
                count = int(raw_count)
                if count < 0:
                    raise ValueError
                if purpose == _UPLOAD_FAULT_PURPOSE:
                    if action != "corrupt":
                        raise ValueError
                elif purpose not in _PURPOSES or action not in _FAULT_ACTIONS:
                    raise ValueError
            except ValueError:
                return _error(
                    start_response,
                    400,
                    "bad_request",
                    "need purpose=fs|rpc with action=corrupt|missing, or purpose=upload with action=corrupt; count>=0",
                )
            if effect:
                if purpose == _UPLOAD_FAULT_PURPOSE:
                    self.set_upload_fault(action, count)
                else:
                    self.set_download_fault(purpose, action, count)
            body = {"ok": True, "purpose": purpose, "action": action, "count": count}
        elif path == "/__admin__/delete":
            origin = parse_qs(str(environ.get("QUERY_STRING", ""))).get("origin", [""])[0]
            if origin not in _DELETE_ORIGINS:
                return _error(start_response, 400, "bad_request", "need origin=client_upload|state_ref")
            deleted = self.delete_by_origin(origin) if effect else 0
            body = {"ok": True, "deleted": deleted}
        else:
            return _error(start_response, 404, "not_found", "no such route")
        payload = json.dumps(body).encode()
        payload += b" " * max(0, TEST_ROUTE_BODY_BYTES - 1 - len(payload)) + b"\n"
        return _serve_object(
            environ, start_response, method, payload, None, content_type="application/json", no_store=True
        )

    def _download_fault(self, object_id: str, obj: _Object) -> str | None:
        """Return the fault for a signed ``GET`` of *object_id*, marking it if one is armed."""
        applied = self._faulted.get(object_id)
        if applied is not None:
            return applied
        armed = self._armed.get(obj.purpose)
        if armed is None or obj.data is None:
            return None
        action, remaining = armed
        if remaining <= 1:
            del self._armed[obj.purpose]
        else:
            self._armed[obj.purpose] = (action, remaining - 1)
        self._faulted[object_id] = action
        return action

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
            if obj.state_ref:
                self._hit("gets_state_ref")
            elif obj.client_upload:
                self._hit("gets_upload")
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
        # Advisory, like the real vendor (which ignores it): anything but the
        # client-upload marker is a server write.
        client_upload = req.get("origin") == CLIENT_UPLOAD_ORIGIN
        state_ref = req.get("origin") == STATE_REF_ORIGIN
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
            obj.client_upload = obj.client_upload or client_upload
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
                client_upload=client_upload,
                state_ref=state_ref,
            )
        self._hit("puts_upload" if client_upload else f"puts_{purpose}")
        if state_ref:
            self._hit("puts_state_ref")
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
        data = obj.data
        if method == "GET":
            fault = self._download_fault(grant.object_id, obj)
            if fault == "missing":
                return _xml(start_response, 404, "NoSuchKey")
            if fault == "corrupt":
                data = bytes(b ^ 0xFF for b in data)
        chunks = _serve_object(environ, start_response, method, data, obj.stored_encoding)
        self._bytes_downloaded += sum(len(c) for c in chunks)
        return chunks

    def _store_put(
        self, environ: WSGIEnvironment, start_response: StartResponse, obj: _Object, body: bytes
    ) -> list[bytes]:
        result = self._store_put_checked(environ, start_response, obj, body)
        if obj.client_upload:
            # Only an accepted PUT answers an empty body; refusals carry S3 XML.
            self._hit("client_uploads" if result == [b""] else "client_upload_refusals")
        return result

    def _store_put_checked(
        self, environ: WSGIEnvironment, start_response: StartResponse, obj: _Object, body: bytes
    ) -> list[bytes]:
        if body is _MALFORMED_LENGTH:
            return _xml(start_response, 400, "InvalidArgument")
        if obj.client_upload and self._upload_corruptions > 0:
            # Damaged in transit: the store sees other bytes than were sent.
            self._upload_corruptions -= 1
            self._hit("uploads_corrupted")
            body = bytes(b ^ 0xFF for b in body)
        encoding = environ.get("HTTP_CONTENT_ENCODING")
        if obj.size is not None:
            # Sized grant: verify like R2 (length, then signed checksum
            # header, then the body's real digest).
            if len(body) != obj.size:
                self._hit("upload_incomplete_body")
                return _xml(start_response, 400, "IncompleteBody")
            if environ.get("HTTP_X_AMZ_CHECKSUM_SHA256") != obj.sha256_b64 or (
                obj.content_encoding and encoding != obj.content_encoding
            ):
                self._hit("upload_signature_mismatch")
                return _xml(start_response, 403, "SignatureDoesNotMatch")
            if base64.b64encode(hashlib.sha256(body).digest()).decode() != obj.sha256_b64:
                self._hit("upload_bad_digest")
                return _xml(start_response, 400, "BadDigest")
        obj.data = body
        obj.stored_encoding = str(encoding) if encoding else None
        self._bytes_uploaded += len(body)
        if obj.client_upload:
            self._hit("client_upload_bytes", len(body))
        start_response("200 OK", [("Content-Length", "0"), ("ETag", '"fake"')])
        return [b""]


def _serve_object(
    environ: WSGIEnvironment,
    start_response: StartResponse,
    method: str,
    data: bytes,
    encoding: str | None,
    *,
    content_type: str = "application/octet-stream",
    no_store: bool = False,
) -> list[bytes]:
    headers = [("Content-Type", content_type), ("Accept-Ranges", "bytes")]
    if no_store:
        headers.append(("Cache-Control", "no-store"))
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
    tokens: Mapping[str, str] | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    region: str = "auto-enam",
    test_routes: bool = False,
) -> FakeVendorServer:
    """Start a fake vendor on a background thread.

    Args:
        tokens: Bearer token to tenant; defaults to ``{"test-token": "tenant-a"}``.
        host: Bind host.
        port: Bind port (``0`` picks a free one).
        region: Region reported by ``/v1/info``.
        test_routes: Serve ``/__stats__`` and ``/__admin__/*`` (module docstring).

    Returns:
        The running :class:`FakeVendorServer`.

    """
    vendor = FakeVendor(tokens=dict(tokens or {"test-token": "tenant-a"}), region=region, test_routes=test_routes)
    placeholder = cast("WSGIApplication", lambda environ, start_response: [b""])
    server = make_server(host, port, placeholder, server_class=_ThreadingWSGIServer, handler_class=_SilentHandler)
    vendor.base_url = f"http://{host}:{server.server_address[1]}"
    server.set_app(vendor.app())
    threading.Thread(target=server.serve_forever, name="fake-vendor", daemon=True).start()
    return FakeVendorServer(vendor, server)


def main(argv: list[str] | None = None) -> None:
    """Run the fake vendor standalone (the ``vgi-rpc-fake-vendor`` command).

    Prints ``PORT:<n>`` and then ``URL:<base url>`` on stdout and serves
    until killed. On ``SIGTERM``, ``SIGINT`` or ``SIGHUP`` it logs to stderr
    which signal arrived and (on Linux) the sending pid and its command
    line, then its final counters, so a lane can tell why its vendor died.
    ``--token TOKEN[=TENANT]`` (repeatable; tenant defaults to ``tenant-a``,
    token to ``test-token``), ``--test-routes`` serves ``/__stats__`` and
    ``/__admin__/*``.

    Args:
        argv: Arguments (default ``sys.argv[1:]``).

    """
    parser = argparse.ArgumentParser(prog="vgi-rpc-fake-vendor", description="In-memory fake blob vendor for tests")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--token", action="append", default=[], help="TOKEN[=TENANT] (repeatable)")
    parser.add_argument("--region", default="auto-enam", help="Region reported by /v1/info")
    parser.add_argument(
        "--test-routes", action="store_true", help="Serve unauthenticated GET /__stats__ and /__admin__/{fault,reset}"
    )
    args = parser.parse_args(argv)
    tokens: dict[str, str] | None = None
    if args.token:
        tokens = {}
        for entry in args.token:
            token, _, tenant = entry.partition("=")
            tokens[token] = tenant or "tenant-a"
    # Block the stop signals before any thread starts (threads inherit the
    # mask), so they stay pending for the main thread to take with
    # sigwaitinfo and log who sent them.
    can_wait = hasattr(signal, "pthread_sigmask") and hasattr(signal, "sigwait")
    if can_wait:
        signal.pthread_sigmask(signal.SIG_BLOCK, _STOP_SIGNALS)
    fake = serve_fake_vendor(tokens, host=args.host, port=args.port, region=args.region, test_routes=args.test_routes)
    started = time.monotonic()
    print(f"PORT:{fake.url.rsplit(':', 1)[1]}", flush=True)
    print(f"URL:{fake.url}", flush=True)
    _log(f"pid {os.getpid()} serving {fake.url}")
    try:
        if can_wait:
            _log(f"received {_wait_stop_signal()}; shutting down after {time.monotonic() - started:.1f}s")
        else:
            threading.Event().wait()
    except KeyboardInterrupt:
        _log(f"received KeyboardInterrupt; shutting down after {time.monotonic() - started:.1f}s")
    finally:
        _log(f"final counters {json.dumps(fake.vendor.test_stats(), separators=(',', ':'))}")
        fake.close()


#: Signals that stop the standalone fake vendor (each is logged with its sender).
_STOP_SIGNALS = frozenset({signal.SIGTERM, signal.SIGINT} | ({signal.SIGHUP} if hasattr(signal, "SIGHUP") else set()))


def _log(message: str) -> None:
    """Write a timestamped diagnostic line to stderr (test lanes keep it in a log file)."""
    stamp = datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    print(f"vgi-rpc-fake-vendor: {stamp} {message}", file=sys.stderr, flush=True)


def _wait_stop_signal() -> str:
    """Block until a stop signal arrives; describe it and, where the OS says, its sender."""
    if sys.platform != "linux":  # no sigwaitinfo (macOS): the signal, not its sender
        return f"{signal.Signals(signal.sigwait(_STOP_SIGNALS)).name} (sender unknown on {sys.platform})"
    info = signal.sigwaitinfo(_STOP_SIGNALS)
    return f"{signal.Signals(info.si_signo).name} from pid {info.si_pid}{_describe_pid(info.si_pid)}"


def _describe_pid(pid: int) -> str:
    """Return `` (command line)`` for *pid* from ``/proc``, or `` (exited)`` once it is gone."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read(400)
    except OSError:
        return " (exited)"
    cmdline = raw.replace(b"\0", b" ").decode(errors="replace").strip()
    return f" ({cmdline[:300]})" if cmdline else " (exited)"


if __name__ == "__main__":
    main()
