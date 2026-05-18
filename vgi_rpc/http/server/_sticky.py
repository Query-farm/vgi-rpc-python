# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Sticky-session machinery for the HTTP transport.

Sticky sessions let an RPC method bind a Python object (a DB cursor, a
loaded model, an open file handle) to the worker process that opened it,
keyed by a short-lived AEAD-sealed token. Subsequent requests carrying
the same ``VGI-Session`` header restore the object as ``ctx.session``;
requests that miss (wrong worker, expired, evicted) raise the typed
:class:`~vgi_rpc.rpc.SessionLostError`.

The feature is **HTTP-only and opt-in**: it ships off by default and the
non-sticky wire path is byte-identical to the pre-sticky framework. The
session token rides exclusively in HTTP headers — no cookies — so that
multiple concurrent sticky sessions to one host from a single client
multiplex correctly (each call carries its own header, not an ambient
jar entry).

This module owns four concerns:

* :class:`_SessionEntry` / :class:`_SessionRegistry` — the per-worker
  in-process dict of live sessions plus a daemon reaper that evicts on
  TTL and invokes ``state.close()`` for handle-bearing values.
* :func:`_seal_session_token` / :func:`_open_session_token` — the AEAD
  envelope for the session token, layered on top of
  :mod:`vgi_rpc.crypto` and binding the token to the request principal
  via the same ``_compute_aad`` helper used by stream tokens.
* :class:`_StickyMiddleware` — Falcon middleware that resolves an
  incoming token to a registry entry, holds the per-session RLock for
  the duration of dispatch, installs the contextvars that
  :class:`~vgi_rpc.rpc.CallContext` reads, and emits the response
  header on session open / close.
* :class:`_SessionResource` — the ``DELETE /vgi/__session__`` endpoint
  the client fires on context-manager exit, idempotent and
  principal-bound via AAD.
"""

from __future__ import annotations

import base64
import contextlib
import logging
import secrets
import struct
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING

import falcon

from vgi_rpc import crypto
from vgi_rpc.rpc import (
    SessionLostError,
    _current_session_context,
    _current_session_id,
    _current_sticky_action,
    _current_sticky_sink,
    _get_auth_and_metadata,
    _SessionContext,
)
from vgi_rpc.rpc._common import ServerDrainingError

from .._common import (
    ECHO_HEADER_PREFIX,
    SESSION_ACCEPT_HEADER,
    SESSION_CLOSE_HEADER,
    SESSION_HEADER,
)
from ._responses import _set_error_response
from ._state_token import _compute_aad

if TYPE_CHECKING:
    from collections.abc import Mapping

_logger = logging.getLogger("vgi_rpc.sticky")

# Token format: 1-byte version || nonce || ciphertext+tag
# Plaintext frame: created_at:u64 | server_id_len:u8 | server_id | session_id:bytes(SESSION_ID_LEN) | expires_at:u64
# We use length-prefix on server_id since it can vary (RpcServer permits
# operator-supplied IDs); session_id is framework-minted and fixed-length.
_TOKEN_VERSION = 1
_SESSION_ID_LEN = 12  # bytes — matches RpcServer.server_id format (24 hex chars / 12 bytes)
_PLAINTEXT_PREFIX = struct.Struct("<Q B")  # created_at, server_id_len
_PLAINTEXT_SUFFIX = struct.Struct("<Q")  # expires_at


# ---------------------------------------------------------------------------
# Token sealing
# ---------------------------------------------------------------------------


def _seal_session_token(
    server_id: str,
    session_id: bytes,
    expires_at: int,
    token_key: bytes,
    aad: bytes,
    *,
    now: int | None = None,
) -> str:
    """Seal a session token. Returns the base64url-encoded value for the header."""
    if len(session_id) != _SESSION_ID_LEN:
        msg = f"session_id must be {_SESSION_ID_LEN} bytes, got {len(session_id)}"
        raise ValueError(msg)
    server_id_bytes = server_id.encode()
    if len(server_id_bytes) > 255:
        msg = f"server_id too long ({len(server_id_bytes)} bytes); max 255"
        raise ValueError(msg)
    plaintext = (
        _PLAINTEXT_PREFIX.pack(int(time.time()) if now is None else now, len(server_id_bytes))
        + server_id_bytes
        + session_id
        + _PLAINTEXT_SUFFIX.pack(expires_at)
    )
    sealed = crypto.seal_bytes(plaintext, token_key, aad=aad, version=_TOKEN_VERSION)
    # Base64url, no padding — header-safe and roundtrips losslessly.
    return base64.urlsafe_b64encode(sealed).rstrip(b"=").decode("ascii")


def _open_session_token(
    token: str,
    token_key: bytes,
    aad: bytes,
) -> tuple[str, bytes, int]:
    """Open a session token. Returns ``(server_id, session_id, expires_at)``.

    Raises:
        SessionLostError: For any malformed, tampered, expired, wrong-key,
            or wrong-principal token. All failure modes are indistinguishable
            from the caller's perspective — wrong-AAD (cross-principal replay)
            is reported the same way as garbage input.

    """
    try:
        # Re-pad to a multiple of 4 for urlsafe_b64decode.
        padded = token + "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
    except Exception as exc:
        raise SessionLostError("malformed session token") from exc

    try:
        plaintext = crypto.open_bytes(raw, token_key, aad=aad, version=_TOKEN_VERSION)
    except crypto.SealError as exc:
        raise SessionLostError("session token verification failed") from exc

    prefix_len = _PLAINTEXT_PREFIX.size
    if len(plaintext) < prefix_len:
        raise SessionLostError("malformed session token")
    _created_at, server_id_len = _PLAINTEXT_PREFIX.unpack_from(plaintext, 0)
    sid_pos = prefix_len + server_id_len
    end_pos = sid_pos + _SESSION_ID_LEN + _PLAINTEXT_SUFFIX.size
    if len(plaintext) != end_pos:
        raise SessionLostError("malformed session token")
    server_id = plaintext[prefix_len:sid_pos].decode("ascii", errors="replace")
    session_id = plaintext[sid_pos : sid_pos + _SESSION_ID_LEN]
    (expires_at,) = _PLAINTEXT_SUFFIX.unpack_from(plaintext, sid_pos + _SESSION_ID_LEN)
    return server_id, session_id, expires_at


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@dataclass
class _SessionEntry:
    """A single live session in the per-worker registry."""

    state: object
    expires_at: float
    principal_key: str
    lock: threading.RLock


class _SessionRegistry:
    """Per-worker in-process dict of live sessions.

    The registry's own lock protects the dict's identity (insertion,
    deletion, iteration); per-entry RLocks (held only during dispatch)
    serialize concurrent calls on the same session. The registry lock
    is never held during dispatch — it's a fast-path mutex for
    metadata, not a serialization point.
    """

    def __init__(self, default_ttl: float) -> None:
        self._default_ttl = default_ttl
        self._lock = threading.Lock()
        self._entries: dict[bytes, _SessionEntry] = {}
        self._draining = False

    @property
    def default_ttl(self) -> float:
        """Default session TTL in seconds when ``open`` is called with ``ttl=None``."""
        return self._default_ttl

    @property
    def draining(self) -> bool:
        """Whether the server is draining; new opens raise ``ServerDrainingError``."""
        return self._draining

    def set_draining(self, value: bool) -> None:
        """Flip the draining flag. Existing sessions continue to serve."""
        self._draining = value

    def open(
        self,
        state: object,
        ttl: float | None,
        principal_key: str,
    ) -> tuple[bytes, float]:
        """Register a session. Returns ``(session_id, expires_at)``.

        Raises:
            ServerDrainingError: If the server is in drain mode.

        """
        if self._draining:
            raise ServerDrainingError("server is draining — new sessions are rejected")
        effective_ttl = self._default_ttl if ttl is None else ttl
        expires_at = time.time() + effective_ttl
        entry = _SessionEntry(
            state=state,
            expires_at=expires_at,
            principal_key=principal_key,
            lock=threading.RLock(),
        )
        session_id = secrets.token_bytes(_SESSION_ID_LEN)
        with self._lock:
            self._entries[session_id] = entry
        return session_id, expires_at

    def get(self, session_id: bytes, principal_key: str) -> _SessionEntry | None:
        """Look up an entry. Returns ``None`` on miss, expiry, or principal mismatch.

        Expired entries are evicted in-line (and their ``state.close()`` is
        invoked) so the caller's miss is observationally identical whether
        the entry never existed or aged out between the reaper ticks.

        Note that AAD already binds the token to its principal at the
        crypto layer, so this principal_key check is defense-in-depth: a
        forged token could not have valid AAD in the first place.
        """
        now = time.time()
        with self._lock:
            entry = self._entries.get(session_id)
            if entry is None:
                return None
            if entry.expires_at < now:
                del self._entries[session_id]
                self._close_state_suppressed(entry.state)
                return None
            if entry.principal_key != principal_key:
                return None
        return entry

    def close(self, session_id: bytes) -> bool:
        """Remove a session and invoke ``state.close()``. Returns ``True`` on hit."""
        with self._lock:
            entry = self._entries.pop(session_id, None)
        if entry is None:
            return False
        self._close_state_suppressed(entry.state)
        return True

    def drain_expired(self, now: float | None = None) -> int:
        """Evict any sessions past their TTL. Returns the eviction count."""
        if now is None:
            now = time.time()
        with self._lock:
            expired_sids = [sid for sid, e in self._entries.items() if e.expires_at < now]
            expired = [self._entries.pop(sid) for sid in expired_sids]
        for entry in expired:
            self._close_state_suppressed(entry.state)
        return len(expired)

    def shutdown(self) -> None:
        """Invoke ``state.close()`` on every live session and clear the registry.

        Called on WSGI app teardown so handle-bearing values get released
        cleanly. Does NOT fire on SIGKILL / process crash — see
        ``docs/sticky-sessions-spec.md`` for the documented crash contract.
        """
        with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            self._close_state_suppressed(entry.state)

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def __iter__(self) -> Iterator[bytes]:
        with self._lock:
            return iter(list(self._entries.keys()))

    @staticmethod
    def _close_state_suppressed(state: object) -> None:
        """Invoke ``state.close()`` if defined; suppress and log exceptions.

        Cleanup is the context-manager contract — DB cursors, file handles,
        and model handles typically expose ``close()``. We don't require it
        (the state may just be a plain dataclass) so the check is duck-typed.
        """
        closer = getattr(state, "close", None)
        if not callable(closer):
            return
        try:
            closer()
        except Exception:
            _logger.exception(
                "Suppressed exception from session state.close() during eviction",
            )


# ---------------------------------------------------------------------------
# Reaper thread
# ---------------------------------------------------------------------------


class _ReaperThread(threading.Thread):
    """Daemon thread that ticks the registry's expiry sweep.

    Started lazily by ``_StickyMiddleware.process_request`` under a
    one-shot lock so it spawns inside the worker child (gunicorn /
    uwsgi pre-fork) rather than the master.
    """

    def __init__(self, registry: _SessionRegistry, tick_seconds: float = 1.0) -> None:
        super().__init__(name="vgi-rpc-sticky-reaper", daemon=True)
        self._registry = registry
        self._tick = tick_seconds
        self._stop = threading.Event()

    def stop(self) -> None:
        """Signal the reaper to exit at the next tick boundary."""
        self._stop.set()

    def run(self) -> None:
        """Tick the registry until ``stop()`` is set."""
        while not self._stop.wait(self._tick):
            try:
                self._registry.drain_expired()
            except Exception:
                _logger.exception("Sticky session reaper tick failed")


# ---------------------------------------------------------------------------
# Per-request sink (concrete implementation of _StickySinkProtocol)
# ---------------------------------------------------------------------------


@dataclass
class _StickySink:
    """Per-request sink bridging ``CallContext.open_session`` to this middleware.

    Mutated during dispatch by the runtime API and read by
    ``_StickyMiddleware.process_response`` to decide which session
    headers to emit. Implements ``_StickySinkProtocol``.

    ``session_id`` is kept here (in addition to the contextvar) so it
    survives ``close_session()`` clearing the session contextvar — the
    access log needs the id of the session this request touched, even
    when the method that closed it has already returned.
    """

    accept_opens: bool
    _open_callback: Callable[[object, float | None], str]
    _close_callback: Callable[[], bool]
    mint_token: str | None = None
    closed: bool = False
    session_id: str | None = None

    def open(self, state: object, ttl: float | None) -> str:
        """Register a session via the callback; stash the minted token for the response."""
        token = self._open_callback(state, ttl)
        self.mint_token = token
        # _open_callback set _current_session_context — capture the new id
        # from there. We could equally have _open_callback return it, but
        # the contextvar is the single source of truth right after open.
        sc = _current_session_context.get()
        if sc is not None:
            self.session_id = sc.session_id
        return token

    def close(self) -> None:
        """Close the bound session via the callback; signal the response middleware."""
        self._close_callback()
        self.closed = True


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


class _StickyMiddleware:
    """Falcon middleware that wires the session header to the registry.

    Inserted after :class:`~vgi_rpc.http.server._middleware._AuthMiddleware`
    so authenticated principals are already on the transport contextvar
    by the time we read them for AAD binding. Inserted before
    :class:`~vgi_rpc.http.server._middleware._CapabilitiesMiddleware`
    so capability headers continue to land on every response regardless
    of sticky outcomes.

    All session-state mutation lives in :class:`_SessionRegistry`; this
    class is purely the request/response shim.
    """

    __slots__ = (
        "_echo_headers",
        "_exempt_prefixes",
        "_reaper",
        "_reaper_lock",
        "_registry",
        "_token_key",
    )

    def __init__(
        self,
        registry: _SessionRegistry,
        token_key: bytes,
        *,
        exempt_prefixes: tuple[str, ...] = (),
        echo_headers: Mapping[str, str] | None = None,
    ) -> None:
        self._registry = registry
        self._token_key = token_key
        self._exempt_prefixes = exempt_prefixes
        # Frozen snapshot so per-response emission doesn't re-read a mutable
        # operator dict mid-request. Empty dict ⇒ no echo headers emitted.
        self._echo_headers: tuple[tuple[str, str], ...] = tuple(echo_headers.items()) if echo_headers else ()
        self._reaper: _ReaperThread | None = None
        self._reaper_lock = threading.Lock()

    def _ensure_reaper(self) -> None:
        """Start the reaper thread on first request (fork-safe under pre-fork servers)."""
        if self._reaper is not None:
            return
        with self._reaper_lock:
            if self._reaper is None:
                self._reaper = _ReaperThread(self._registry)
                self._reaper.start()

    def stop_reaper(self) -> None:
        """Signal the reaper to exit. Idempotent."""
        with self._reaper_lock:
            if self._reaper is not None:
                self._reaper.stop()
                self._reaper = None

    @staticmethod
    def _principal_key(req: falcon.Request) -> str:
        """Derive a stable principal key for registry partitioning.

        AAD already binds the token to the issuing principal at the crypto
        layer; the registry's principal_key check is defense-in-depth so
        a bug in token sealing can't accidentally let user A's request
        bind to user B's session. We mirror the same anonymous handling
        as :func:`vgi_rpc.http.server._state_token._compute_aad`.
        """
        auth, _ = _get_auth_and_metadata()
        if auth is None or not auth.authenticated:
            return "\x00anonymous"
        return f"{auth.domain or ''}\x00{auth.principal or ''}"

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Resolve the session header (if any) and install the sticky contextvars."""
        for prefix in self._exempt_prefixes:
            if req.path == prefix or req.path.startswith(prefix + "/"):
                return
        self._ensure_reaper()

        accept_opens = (req.get_header(SESSION_ACCEPT_HEADER) or "").strip().lower() == "true"
        principal_key = self._principal_key(req)

        # Resolve any presented session token. AAD is the same shape as
        # stream tokens — bound to the request's authenticated identity —
        # so cross-principal replay fails decryption at the crypto layer.
        token_header = req.get_header(SESSION_HEADER)
        if token_header:
            try:
                auth, _ = _get_auth_and_metadata()
                aad = _compute_aad(auth)
                server_id, session_id, _expires_at = _open_session_token(
                    token_header.strip(),
                    self._token_key,
                    aad,
                )
                if server_id != _expected_server_id(req):
                    # Wrong worker — without a replay mechanism (deferred to a
                    # later PR), we can only tell the client the session is
                    # gone. The token might be perfectly valid; it just doesn't
                    # belong to this process.
                    raise SessionLostError(
                        "session token was issued by a different worker (server_id mismatch)",
                    )
                entry = self._registry.get(session_id, principal_key)
                if entry is None:
                    raise SessionLostError(
                        "session not found, expired, or principal mismatch",
                    )
            except SessionLostError as exc:
                # Convert middleware-time SessionLostError into the same
                # Arrow EXCEPTION-batch response shape that in-dispatch errors
                # produce, so clients see a clean RpcError(error_type='SessionLostError')
                # with vgi_rpc.error_kind = "session_lost" in the metadata.
                # resp.complete signals Falcon to skip route dispatch.
                _set_error_response(resp, exc, status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
                resp.complete = True
                return
            # Acquire the per-session RLock for the duration of dispatch.
            # Released in process_response. Same-session concurrent calls
            # serialize here; different-session calls run in parallel.
            entry.lock.acquire()
            req.context.sticky_entry = entry
            req.context.sticky_entry_lock_acquired = True
            session_id_hex = session_id.hex()
            sc_token = _current_session_context.set(
                _SessionContext(state=entry.state, session_id=session_id_hex),
            )
            req.context.sticky_session_token = sc_token
            sid_token = _current_session_id.set(session_id_hex)
            req.context.sticky_session_id_token = sid_token
            # Mark the access log: valid token, entry found, dispatch will run.
            action_token = _current_sticky_action.set("resume")
        else:
            # No token presented — record the absence so the access log shows
            # the request flowed through the sticky middleware without touching
            # a session. ``CallContext.open_session`` will override this to
            # "open" if the method opts in.
            action_token = _current_sticky_action.set("none")
        req.context.sticky_action_token = action_token

        sink = _StickySink(
            accept_opens=accept_opens,
            _open_callback=lambda state, ttl: self._open_session(req, principal_key, state, ttl),
            _close_callback=lambda: self._close_session(req),
        )
        # On resume, carry the session_id eagerly so the access log can read
        # it even after close_session() clears the session contextvar.
        resume_ctx = _current_session_context.get()
        if resume_ctx is not None:
            sink.session_id = resume_ctx.session_id
        req.context.sticky_sink = sink
        sink_token = _current_sticky_sink.set(sink)
        req.context.sticky_sink_token = sink_token

    def _open_session(
        self,
        req: falcon.Request,
        principal_key: str,
        state: object,
        ttl: float | None,
    ) -> str:
        """Register *state* in the registry and seal a token bound to the principal."""
        session_id, expires_at = self._registry.open(state, ttl, principal_key)
        auth, _ = _get_auth_and_metadata()
        aad = _compute_aad(auth)
        token = _seal_session_token(
            server_id=_expected_server_id(req),
            session_id=session_id,
            expires_at=int(expires_at),
            token_key=self._token_key,
            aad=aad,
        )
        # Install the session context for the remainder of this request
        # so ctx.session / ctx.session_id work in the same call that opened.
        session_id_hex = session_id.hex()
        sc_token = _current_session_context.set(
            _SessionContext(state=state, session_id=session_id_hex),
        )
        req.context.sticky_session_token = sc_token
        # Track session_id separately so it survives ctx.close_session()
        # clearing the session-context contextvar — the access log still
        # carries the id on close records.
        sid_token = _current_session_id.set(session_id_hex)
        req.context.sticky_session_id_token = sid_token
        return token

    def _close_session(self, req: falcon.Request) -> bool:
        """Remove the session bound to this request from the registry."""
        sc = _current_session_context.get()
        if sc is None:
            return False
        try:
            session_id = bytes.fromhex(sc.session_id)
        except ValueError:
            return False
        # Release the per-session RLock before removal so process_response's
        # release doesn't double-unlock.
        entry = getattr(req.context, "sticky_entry", None)
        if entry is not None and getattr(req.context, "sticky_entry_lock_acquired", False):
            with contextlib.suppress(RuntimeError):
                entry.lock.release()
            req.context.sticky_entry_lock_acquired = False
        hit = self._registry.close(session_id)
        # Clear the contextvar so subsequent ctx.session reads return None.
        sc_token = getattr(req.context, "sticky_session_token", None)
        if sc_token is not None:
            _current_session_context.reset(sc_token)
            req.context.sticky_session_token = None
        return hit

    def process_response(
        self,
        req: falcon.Request,
        resp: falcon.Response,
        resource: object,
        req_succeeded: bool,
    ) -> None:
        """Emit session response headers and release contextvars / per-session lock."""
        sink: _StickySink | None = getattr(req.context, "sticky_sink", None)
        if sink is not None:
            if sink.mint_token is not None:
                resp.set_header(SESSION_HEADER, sink.mint_token)
                # Tell the client to echo these headers on every subsequent
                # request in this session. Used for client-driven routing
                # (fly-force-instance-id on Fly.io, similar mechanisms
                # elsewhere). Emitted only on session-opening responses;
                # the client captures and replays them for the lifetime of
                # the session view, no need to repeat.
                for name, value in self._echo_headers:
                    resp.set_header(f"{ECHO_HEADER_PREFIX}{name}", value)
            if sink.closed:
                resp.set_header(SESSION_CLOSE_HEADER, "true")
        # Release the per-session lock if dispatch held it and close_session
        # did not already release it.
        entry: _SessionEntry | None = getattr(req.context, "sticky_entry", None)
        if entry is not None and getattr(req.context, "sticky_entry_lock_acquired", False):
            with contextlib.suppress(RuntimeError):
                entry.lock.release()
            req.context.sticky_entry_lock_acquired = False
        # Reset contextvars.
        sc_token = getattr(req.context, "sticky_session_token", None)
        if sc_token is not None:
            # ValueError ⇒ already reset by close_session — fine.
            with contextlib.suppress(ValueError):
                _current_session_context.reset(sc_token)
            req.context.sticky_session_token = None
        sink_token = getattr(req.context, "sticky_sink_token", None)
        if sink_token is not None:
            _current_sticky_sink.reset(sink_token)
            req.context.sticky_sink_token = None
        action_token = getattr(req.context, "sticky_action_token", None)
        if action_token is not None:
            _current_sticky_action.reset(action_token)
            req.context.sticky_action_token = None
        sid_token = getattr(req.context, "sticky_session_id_token", None)
        if sid_token is not None:
            _current_session_id.reset(sid_token)
            req.context.sticky_session_id_token = None


# ---------------------------------------------------------------------------
# DELETE /vgi/__session__ resource
# ---------------------------------------------------------------------------


class _SessionResource:
    """Falcon resource for ``DELETE {prefix}/__session__``.

    Idempotent best-effort teardown: a valid token whose entry is found
    returns 204 after invoking ``state.close()``; everything else (missing
    header, malformed token, AAD mismatch, server_id mismatch, registry
    miss) returns 200 — so a stale or stolen token cannot be used to probe
    whether a session exists.

    The principal binding is already enforced by AAD at the crypto layer
    and re-checked against the registry's stored ``principal_key``.
    """

    __slots__ = ("_registry", "_token_key")

    def __init__(self, registry: _SessionRegistry, token_key: bytes) -> None:
        self._registry = registry
        self._token_key = token_key

    def on_delete(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Close the session referenced by ``VGI-Session``; idempotent on any failure."""
        token_header = req.get_header(SESSION_HEADER)
        if not token_header:
            resp.status = HTTPStatus.OK
            return
        auth, _ = _get_auth_and_metadata()
        aad = _compute_aad(auth)
        try:
            server_id, session_id, _expires_at = _open_session_token(
                token_header.strip(),
                self._token_key,
                aad,
            )
        except SessionLostError:
            # Stale / forged / wrong-principal — return idempotent success
            # so callers can't probe whether a session exists.
            resp.status = HTTPStatus.OK
            return
        if server_id != _expected_server_id(req):
            resp.status = HTTPStatus.OK
            return
        principal_key = _StickyMiddleware._principal_key(req)
        entry = self._registry.get(session_id, principal_key)
        if entry is None:
            resp.status = HTTPStatus.OK
            return
        # Acquire the per-session RLock so we serialize with any in-flight
        # call on this session — matches the contract documented for
        # concurrent dispatch.
        with entry.lock:
            self._registry.close(session_id)
        resp.set_header(SESSION_CLOSE_HEADER, "true")
        resp.status = HTTPStatus.NO_CONTENT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _expected_server_id(req: falcon.Request) -> str:
    """Read the server_id from the WSGI app's _HttpRpcApp context.

    Threaded through ``req.env`` so token validation can compare against
    the active worker without needing a back-reference to the RpcServer.
    """
    # Falcon stashes the original WSGI environ; we attach server_id at
    # app construction time via _StickyMiddleware's installation point.
    server_id = req.env.get("vgi_rpc.server_id")
    if isinstance(server_id, str):
        return server_id
    # Defensive: shouldn't happen in normal operation — middleware install
    # always attaches the server_id. Fall back to empty so tokens
    # cryptographically can't match anything.
    return ""


def install_server_id_middleware(server_id: str) -> _ServerIdEnvMiddleware:
    """Build a tiny Falcon middleware that pins server_id into ``req.env``.

    The sticky middleware needs to know "which worker am I?" to validate
    that an incoming session token was minted here. Passing it via
    ``req.env`` instead of a back-reference keeps :class:`_StickyMiddleware`
    construction symmetric with :class:`_AuthMiddleware`.
    """
    return _ServerIdEnvMiddleware(server_id)


class _ServerIdEnvMiddleware:
    """Pin the active worker's server_id into ``req.env`` for sticky lookups."""

    __slots__ = ("_server_id",)

    def __init__(self, server_id: str) -> None:
        self._server_id = server_id

    def process_request(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Attach the worker server_id so sticky middleware can read it from req.env."""
        req.env["vgi_rpc.server_id"] = self._server_id


# ---------------------------------------------------------------------------
# Drain handle for operator-facing graceful shutdown
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DrainHandle:
    """Operator-facing handle for triggering graceful drain on a sticky-enabled WSGI app.

    Returned by :func:`drain_handle` when called against an app built by
    :func:`vgi_rpc.http.make_wsgi_app` with ``enable_sticky=True``. Provides
    the two operations operators need to wire up SIGTERM handlers, pre-fork
    worker-exit hooks (gunicorn ``worker_exit``), or custom shutdown logic:

    * :meth:`drain` — flip the registry's drain flag so subsequent
      ``ctx.open_session`` calls raise :class:`~vgi_rpc.rpc.ServerDrainingError`.
      Existing-session calls continue to serve until TTL or explicit close.
    * :meth:`shutdown` — invoke ``state.close()`` on every live session and
      clear the registry. Use after the operator-controlled grace period.

    Both methods are idempotent and thread-safe (they delegate to
    :class:`_SessionRegistry`'s lock-guarded methods).
    """

    drain: Callable[[], None]
    """Set the registry's drain flag; new ``ctx.open_session`` calls raise ``ServerDrainingError``."""

    shutdown: Callable[[], None]
    """Invoke ``state.close()`` on every live session and clear the registry."""

    is_draining: Callable[[], bool]
    """Return whether ``drain()`` has been invoked."""


def drain_handle(app: falcon.App[falcon.Request, falcon.Response]) -> DrainHandle | None:
    """Return a :class:`DrainHandle` for *app*, or ``None`` if sticky is not enabled.

    Inspects the Falcon app's middleware tuple to find the
    :class:`_StickyMiddleware` instance, then constructs closures over its
    registry. Returns ``None`` cleanly for non-sticky apps so operator code
    can branch with ``if (handle := drain_handle(app)) is not None: ...``.

    Used by :func:`vgi_rpc.http.serve_http` for its SIGTERM wiring, and
    exposed publicly so operators running under gunicorn / uwsgi / their
    own WSGI launcher can wire equivalent shutdown hooks. See the spec at
    ``docs/sticky-sessions-spec.md`` for the pre-fork worker-exit recipe.
    """
    # Falcon stores middleware as a tuple of three tuples:
    # (request-handlers, request-handlers-async, response-handlers).
    # Each handler is a bound method on a middleware instance; we walk
    # them all to find a _StickyMiddleware. Iteration order is stable
    # within Falcon's implementation but we don't rely on it — we just
    # find the first sticky instance and stop.
    middleware_groups: tuple[tuple[object, ...], ...] = getattr(app, "_middleware", ())
    for group in middleware_groups:
        for bound_method in group:
            owner = getattr(bound_method, "__self__", None)
            if isinstance(owner, _StickyMiddleware):
                return _build_drain_handle(owner._registry)
    return None


def _build_drain_handle(registry: _SessionRegistry) -> DrainHandle:
    """Build a :class:`DrainHandle` whose closures bind *registry* explicitly.

    Lives outside the for-loop in :func:`drain_handle` so the lambdas
    capture *registry* via the function parameter, not via the enclosing
    loop variable (which would be a ruff B023 footgun even though only
    one iteration ever runs).
    """
    return DrainHandle(
        drain=lambda: registry.set_draining(True),
        shutdown=registry.shutdown,
        is_draining=lambda: registry.draining,
    )


# Re-exports for the factory module.
__all__ = [
    "DrainHandle",
    "_ReaperThread",
    "_ServerIdEnvMiddleware",
    "_SessionEntry",
    "_SessionRegistry",
    "_SessionResource",
    "_StickyMiddleware",
    "_StickySink",
    "drain_handle",
    "_open_session_token",
    "_seal_session_token",
    "install_server_id_middleware",
]
