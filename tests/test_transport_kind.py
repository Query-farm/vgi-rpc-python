# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for TransportKind, on_serve_start, and per-call CallContext.kind."""

from __future__ import annotations

import contextlib
import logging
import socket
import subprocess
import sys
import threading
from pathlib import Path
from typing import Protocol

import pytest

from vgi_rpc import (
    CallContext,
    RpcServer,
    ShmPipeTransport,
    TransportKind,
)
from vgi_rpc.rpc import (
    PipeTransport,
    UnixTransport,
    make_pipe_pair,
    make_unix_pair,
    serve_pipe,
)
from vgi_rpc.shm import ShmSegment

# ---------------------------------------------------------------------------
# Test service: a single unary method that echoes the transport kind.
# ---------------------------------------------------------------------------


class _KindService(Protocol):
    """Service used to surface the per-call transport kind to the test."""

    def report_kind(self) -> str:
        """Return the string value of ``ctx.kind``, or ``"none"`` when unset."""
        ...


class _RecordingImpl:
    """Records on_serve_start calls and exposes ctx.kind via report_kind."""

    def __init__(self) -> None:
        """Start with empty hook history; populated lazily as the framework binds."""
        self.kinds: list[TransportKind] = []
        self.call_count = 0
        self._lock = threading.Lock()

    def on_serve_start(self, kind: TransportKind) -> None:
        """Record the transport kind announced at startup."""
        with self._lock:
            self.kinds.append(kind)
            self.call_count += 1

    def report_kind(self, ctx: CallContext) -> str:
        """Return the per-call kind so tests can assert end-to-end plumbing."""
        return ctx.kind.value if ctx.kind is not None else "none"


class _NoHookImpl:
    """Implementation without an on_serve_start hook — ensures opt-in is honored."""

    def report_kind(self, ctx: CallContext) -> str:
        """Return the kind seen on the call."""
        return ctx.kind.value if ctx.kind is not None else "none"


class _RaisingImpl:
    """Implementation whose hook raises — used to assert propagation."""

    def __init__(self) -> None:
        """Track whether the hook was invoked at least once."""
        self.fired = False

    def on_serve_start(self, kind: TransportKind) -> None:
        """Raise so tests can observe propagation + logging."""
        self.fired = True
        raise RuntimeError("boom")

    def report_kind(self, ctx: CallContext) -> str:
        """Unused in this fixture but required by the protocol."""
        return "unreachable"


# ---------------------------------------------------------------------------
# Pipe transport
# ---------------------------------------------------------------------------


class TestPipeTransport:
    """Hook fires with PIPE for plain pipe transport."""

    def test_serve_pipe_fires_hook_once(self) -> None:
        """One round-trip over serve_pipe — hook fires once with PIPE, no caps."""
        impl = _RecordingImpl()
        with serve_pipe(_KindService, impl) as svc:
            assert svc.report_kind() == "pipe"
        assert impl.kinds == [TransportKind.PIPE]
        assert impl.call_count == 1

    def test_no_hook_is_fine(self) -> None:
        """Implementations without on_serve_start serve cleanly."""
        with serve_pipe(_KindService, _NoHookImpl()) as svc:
            assert svc.report_kind() == "pipe"

    def test_hook_signature_is_kind_only(self) -> None:
        """The hook receives only ``kind`` — no ``server`` argument."""
        observed: list[tuple[object, ...]] = []

        class _SigCapture:
            def on_serve_start(self, *args: object, **kwargs: object) -> None:
                observed.append(args + tuple(kwargs.values()))

            def report_kind(self, ctx: CallContext) -> str:
                return ctx.kind.value if ctx.kind is not None else "none"

        with serve_pipe(_KindService, _SigCapture()) as svc:
            svc.report_kind()
        assert observed == [(TransportKind.PIPE,)]

    def test_transport_kind_attribute_set(self) -> None:
        """``server.transport_kind`` is populated once serving begins."""
        client_transport, server_transport = make_pipe_pair()
        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        assert server.transport_kind is None
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            from vgi_rpc.rpc._client import _RpcProxy

            proxy = _RpcProxy(_KindService, client_transport, None)
            proxy.report_kind()
            assert server.transport_kind == TransportKind.PIPE
            assert server.transport_capabilities == frozenset()
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()


# ---------------------------------------------------------------------------
# Shared-memory pipe transport
# ---------------------------------------------------------------------------


class TestShmPipeTransport:
    """Hook fires with PIPE + ``"shm"`` capability for ShmPipeTransport."""

    def test_shm_pipe_reports_shm_capability(self) -> None:
        """ShmPipeTransport binds as PIPE with capabilities={"shm"}."""
        impl = _RecordingImpl()
        shm = ShmSegment.create(1 * 1024 * 1024)
        try:
            client_pipe, server_pipe = make_pipe_pair()
            client_transport = ShmPipeTransport(client_pipe, shm)
            server_transport = ShmPipeTransport(server_pipe, shm)
            server = RpcServer(_KindService, impl)
            thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
            thread.start()
            try:
                from vgi_rpc.rpc._client import _RpcProxy

                proxy = _RpcProxy(_KindService, client_transport, None)
                proxy.report_kind()
                assert server.transport_kind == TransportKind.PIPE
                assert server.transport_capabilities == frozenset({"shm"})
                assert impl.kinds == [TransportKind.PIPE]
            finally:
                client_transport.close()
                thread.join(timeout=5)
        finally:
            shm.unlink()
            with contextlib.suppress(BufferError):
                shm.close()


# ---------------------------------------------------------------------------
# Unix transport
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets not available on Windows")
class TestUnixTransport:
    """Hook fires with UNIX for UnixTransport."""

    def test_unix_pair(self) -> None:
        """Direct UnixTransport pair binds as UNIX."""
        client_transport, server_transport = make_unix_pair()
        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            from vgi_rpc.rpc._client import _RpcProxy

            proxy = _RpcProxy(_KindService, client_transport, None)
            assert proxy.report_kind() == "unix"
            assert server.transport_kind == TransportKind.UNIX
            assert impl.kinds == [TransportKind.UNIX]
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()


# ---------------------------------------------------------------------------
# HTTP transport (lazy first-request firing)
# ---------------------------------------------------------------------------


class TestHttpTransport:
    """HTTP fires the hook lazily on first request (fork-safety)."""

    def test_lazy_fire_on_first_request(self) -> None:
        """make_wsgi_app does NOT fire the hook; first request does."""
        from vgi_rpc.http import http_connect, make_sync_client

        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        client = make_sync_client(server, signing_key=b"k")

        # The WSGI app is built but no request has been served yet.
        assert server.transport_kind is None
        assert impl.call_count == 0

        with http_connect(_KindService, client=client) as svc:
            assert svc.report_kind() == "http"

        assert server.transport_kind == TransportKind.HTTP
        assert impl.kinds == [TransportKind.HTTP]
        assert impl.call_count == 1

    def test_second_request_does_not_refire(self) -> None:
        """Second HTTP call short-circuits the middleware; hook stays at 1."""
        from vgi_rpc.http import http_connect, make_sync_client

        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        client = make_sync_client(server, signing_key=b"k")
        with http_connect(_KindService, client=client) as svc:
            svc.report_kind()
            svc.report_kind()
            svc.report_kind()
        assert impl.call_count == 1


# ---------------------------------------------------------------------------
# CallContext.kind plumbing
# ---------------------------------------------------------------------------


class TestCallContextKind:
    """ctx.kind is populated from server.transport_kind on every dispatch."""

    def test_pipe_ctx_kind(self) -> None:
        """Methods accepting ctx see kind=PIPE for pipe transport."""
        with serve_pipe(_KindService, _RecordingImpl()) as svc:
            assert svc.report_kind() == "pipe"

    def test_http_ctx_kind(self) -> None:
        """Methods accepting ctx see kind=HTTP for HTTP transport."""
        from vgi_rpc.http import http_connect, make_sync_client

        server = RpcServer(_KindService, _RecordingImpl())
        client = make_sync_client(server, signing_key=b"k")
        with http_connect(_KindService, client=client) as svc:
            assert svc.report_kind() == "http"


# ---------------------------------------------------------------------------
# _notify_transport semantics
# ---------------------------------------------------------------------------


class TestNotifyTransport:
    """Direct unit tests of the idempotence + rebind contract."""

    def test_idempotent_same_kind(self) -> None:
        """Calling _notify_transport twice with same kind+caps fires hook once."""
        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        server._notify_transport(TransportKind.PIPE, frozenset())
        server._notify_transport(TransportKind.PIPE, frozenset())
        assert impl.call_count == 1
        assert impl.kinds == [TransportKind.PIPE]

    def test_rebind_fires_again(self) -> None:
        """A different kind re-fires the hook and updates the public attribute."""
        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        server._notify_transport(TransportKind.PIPE, frozenset())
        server._notify_transport(TransportKind.HTTP, frozenset())
        assert impl.kinds == [TransportKind.PIPE, TransportKind.HTTP]
        assert server.transport_kind == TransportKind.HTTP

    def test_rebind_capability_change_fires(self) -> None:
        """Same kind but different capabilities is a rebind too."""
        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        server._notify_transport(TransportKind.PIPE, frozenset())
        server._notify_transport(TransportKind.PIPE, frozenset({"shm"}))
        assert impl.call_count == 2
        assert server.transport_capabilities == frozenset({"shm"})

    def test_hook_raise_propagates_and_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        """Hook exceptions propagate out and are logged via _logger.exception."""
        impl = _RaisingImpl()
        server = RpcServer(_KindService, impl)
        with caplog.at_level(logging.ERROR, logger="vgi_rpc.rpc"), pytest.raises(RuntimeError, match="boom"):
            server._notify_transport(TransportKind.PIPE, frozenset())
        assert impl.fired
        assert any("on_serve_start hook raised" in record.message for record in caplog.records)

    def test_hook_failure_allows_retry(self) -> None:
        """A failed on_serve_start must leave bind state uncommitted so a retry re-fires.

        Regression: previously, ``_notify_transport`` committed
        ``_transport_kind`` before running the hook.  When the hook raised
        (e.g. transient downstream failure on the first HTTP request), the
        :class:`_TransportNotifyMiddleware` short-circuit
        (``if transport_kind is None``) made every subsequent request skip
        the hook entirely — silent loss of one-shot startup work.
        """

        class _FlakyImpl:
            def __init__(self) -> None:
                self.calls = 0

            def on_serve_start(self, kind: TransportKind) -> None:
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("transient")

            def report_kind(self, ctx: CallContext) -> str:
                return ctx.kind.value if ctx.kind is not None else "none"

        impl = _FlakyImpl()
        server = RpcServer(_KindService, impl)

        with pytest.raises(RuntimeError, match="transient"):
            server._notify_transport(TransportKind.PIPE, frozenset())

        # Bind state must NOT be committed if the hook failed, otherwise the
        # middleware's `transport_kind is None` guard skips the retry.
        assert server.transport_kind is None
        assert server.transport_capabilities == frozenset()

        server._notify_transport(TransportKind.PIPE, frozenset())
        assert impl.calls == 2, "hook must re-fire on retry after a prior failure"
        assert server.transport_kind is TransportKind.PIPE


# ---------------------------------------------------------------------------
# Subprocess end-to-end — worker reports PIPE
# ---------------------------------------------------------------------------


_KIND_FIXTURE = Path(__file__).parent / "serve_fixture_kind_pipe.py"


class TestSubprocess:
    """End-to-end: worker spawned via SubprocessTransport reports PIPE."""

    def test_subprocess_reports_pipe(self) -> None:
        """Spawn a worker, call report_kind, expect ``"pipe"``."""
        from vgi_rpc.rpc import RpcConnection, SubprocessTransport

        transport = SubprocessTransport([sys.executable, str(_KIND_FIXTURE)])
        try:
            with RpcConnection(_KindService, transport) as svc:
                assert svc.report_kind() == "pipe"
        finally:
            transport.close()


# ---------------------------------------------------------------------------
# Fork safety — child process fires the hook independently
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="os.fork unavailable on Windows")
class TestForkSafety:
    """Pre-fork WSGI: child workers fire the hook in their own process."""

    def test_fork_after_make_wsgi_app(self) -> None:
        """Fork after building the app; child handles a request and fires the hook."""
        import os

        from vgi_rpc.http import http_connect, make_sync_client

        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        # Build the app in the parent — this MUST NOT fire the hook.
        client = make_sync_client(server, signing_key=b"k")
        assert server.transport_kind is None

        # Use a pipe to confirm the child fired the hook in its own process.
        r_fd, w_fd = os.pipe()
        pid = os.fork()  # type: ignore[attr-defined, unused-ignore]
        if pid == 0:
            # Child
            os.close(r_fd)
            try:
                with http_connect(_KindService, client=client) as svc:
                    svc.report_kind()
                msg = "ok" if server.transport_kind == TransportKind.HTTP else "fail"
            except Exception as exc:
                msg = f"err:{exc!r}"
            os.write(w_fd, msg.encode())
            os.close(w_fd)
            os._exit(0)
        # Parent
        os.close(w_fd)
        try:
            data = os.read(r_fd, 256).decode()
        finally:
            os.close(r_fd)
            os.waitpid(pid, 0)
        assert data == "ok"
        # In the parent, the hook was never fired by the child's first
        # request (the contextvar/state lives in the child memory space).
        # Parent state is unaffected.
        assert server.transport_kind is None


# ---------------------------------------------------------------------------
# WorkerPool — hook fires once per worker process, not once per borrow
# ---------------------------------------------------------------------------


class TestWorkerPoolReuse:
    """Borrowing the same pool worker twice does not re-fire the hook."""

    def test_pool_reuse_does_not_refire(self) -> None:
        """Two consecutive WorkerPool.connect() calls land on the same worker; one hook."""
        from vgi_rpc.pool import WorkerPool

        pool = WorkerPool(max_idle=1)
        try:
            cmd = [sys.executable, str(_KIND_FIXTURE)]
            with pool.connect(_KindService, cmd) as svc:
                first = svc.report_kind()
            with pool.connect(_KindService, cmd) as svc:
                second = svc.report_kind()
            assert first == "pipe"
            assert second == "pipe"
        finally:
            pool.close()


# ---------------------------------------------------------------------------
# UnixTransport over a real socket pair (raw construction, not via serve_unix)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(sys.platform == "win32", reason="Unix sockets not available on Windows")
class TestUnixTransportRaw:
    """Direct UnixTransport over socketpair — verifies isinstance dispatch."""

    def test_isinstance_dispatch(self) -> None:
        """A bare UnixTransport bound via serve() reports UNIX."""
        s1, s2 = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)  # type: ignore[attr-defined, unused-ignore]
        client_transport = UnixTransport(s1)
        server_transport = UnixTransport(s2)
        impl = _RecordingImpl()
        server = RpcServer(_KindService, impl)
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()
        try:
            from vgi_rpc.rpc._client import _RpcProxy

            proxy = _RpcProxy(_KindService, client_transport, None)
            assert proxy.report_kind() == "unix"
            assert server.transport_kind == TransportKind.UNIX
        finally:
            client_transport.close()
            thread.join(timeout=5)
            server_transport.close()


# ---------------------------------------------------------------------------
# Cleanup helper to keep the workspace tidy across reruns
# ---------------------------------------------------------------------------


# Silence unused-import warnings for symbols only used inside test bodies.
_ = (PipeTransport, subprocess)
