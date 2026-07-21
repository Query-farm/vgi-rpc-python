# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""``serve_http`` — convenience wrapper that runs the WSGI app under waitress."""

from __future__ import annotations

import logging
import os
import signal
import socket
import sys
import threading
import warnings
from collections.abc import Mapping

import falcon

from vgi_rpc.rpc import RpcServer

from ._factory import make_wsgi_app
from ._sticky import drain_handle

_logger = logging.getLogger("vgi_rpc.http")

# waitress's defaults are tuned for small web payloads and are actively
# hostile to Arrow bodies:
#
#   inbuf_overflow = 512 KiB — any larger request body is spooled to a
#     *temp file on disk* and read back before the app ever sees it.  An
#     8 MiB Arrow batch round-trips through the filesystem every request.
#   recv_bytes = 8 KiB — an 8 MiB body is assembled from ~1000 recv()
#     calls plus ~1000 buffer appends.
#   send_bytes = 1 — one byte per socket write scheduling decision.
#
# Measured on an 8 MiB body with a trivial echo app (no VGI in the loop):
# 19.8 ms/request of pure server overhead at the defaults, 4.5 ms with
# these settings.  The spill buys no memory safety here anyway — the
# compression middleware already decompresses the whole body into a
# BytesIO — so trade the disk round-trip for RAM and bound the exposure
# with max_request_bytes.
_WAITRESS_IO_CHUNK = 1 << 20  # 1 MiB socket read/write chunks
_WAITRESS_MIN_BUFFER = 16 << 20  # floor for the in-memory body buffer
_WAITRESS_DEFAULT_BUFFER = 64 << 20  # used when no request cap is advertised


def serve_http(
    server: RpcServer,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    max_response_bytes: int | None = None,
    max_externalized_response_bytes: int | None = None,
    max_stream_response_bytes: int | None = None,
    max_request_bytes: int | None = None,
    compression_level: int | None = 1,
    enable_sticky: bool = False,
    sticky_default_ttl: float = 300.0,
    sticky_echo_headers: Mapping[str, str] | None = None,
    drain_grace_seconds: float = 30.0,
    install_signal_handlers: bool = True,
) -> None:
    """Serve an ``RpcServer`` over HTTP using waitress.

    This is a convenience wrapper that combines :func:`make_wsgi_app` with
    automatic port selection and ``waitress.serve``.

    The selected port is printed to stdout as ``PORT:<port>`` for
    machine-readable discovery (e.g. by test harnesses or process managers).

    When ``enable_sticky=True`` (and ``install_signal_handlers=True``, the
    default), this wrapper installs SIGTERM / SIGINT handlers that perform
    a graceful drain:

    1. First signal: flip the registry's drain flag so subsequent
       ``ctx.open_session`` calls raise :class:`~vgi_rpc.rpc.ServerDrainingError`.
       Existing sessions continue to serve.
    2. After ``drain_grace_seconds`` (in a daemon timer thread): invoke
       ``state.close()`` on every live session and ``os._exit(0)``.
    3. Second signal: skip the grace period and exit immediately.

    For pre-fork servers (gunicorn, uwsgi) operators wire their own
    ``worker_exit`` hooks. See :func:`vgi_rpc.http.drain_handle` and the
    spec at ``docs/sticky-sessions-spec.md`` for the operator recipe.

    Args:
        server: The ``RpcServer`` to expose.
        host: Bind address (default ``127.0.0.1``).
        port: TCP port.  ``0`` (the default) auto-selects a free port.
        max_response_bytes: HTTP body cap; applies to every method.  See
            :func:`make_wsgi_app` for full semantics.
        max_externalized_response_bytes: Cap on bytes uploaded to external
            storage per HTTP response.  See :func:`make_wsgi_app`.
        max_stream_response_bytes: **Deprecated** alias for
            ``max_response_bytes``.
        max_request_bytes: Advertised via ``VGI-Max-Request-Bytes`` (see
            :func:`make_wsgi_app`), and used here to size waitress's
            in-memory body buffers.  When ``None``, buffers default to
            64 MiB — large enough that Arrow bodies never spill to a
            temp file, which is waitress's behaviour above 512 KiB.
        compression_level: zstd level for request/response bodies, or
            ``None`` to disable compression entirely.  See
            :func:`make_wsgi_app`.
        enable_sticky: See :func:`make_wsgi_app`.
        sticky_default_ttl: See :func:`make_wsgi_app`.
        sticky_echo_headers: See :func:`make_wsgi_app`.
        drain_grace_seconds: Seconds to wait between flipping the drain
            flag and forcibly exiting on SIGTERM.  Existing sessions get
            this long to complete in-flight work.  Default ``30.0``.
            Ignored when sticky is disabled.
        install_signal_handlers: When ``True`` (the default), install the
            SIGTERM / SIGINT handlers described above.  Set to ``False``
            when embedding ``serve_http`` inside a larger process that
            already owns signal handling (rare; the default is correct
            for the standard "one process, serve until killed" deployment).

    """
    if max_stream_response_bytes is not None:
        if max_response_bytes is not None:
            raise TypeError("Pass either max_response_bytes or max_stream_response_bytes, not both")
        warnings.warn(
            "max_stream_response_bytes is deprecated; use max_response_bytes instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        max_response_bytes = max_stream_response_bytes

    try:
        import waitress as _waitress
    except ImportError:
        print("HTTP transport requires waitress: pip install vgi-rpc[http]", file=sys.stderr)
        sys.exit(1)

    if port == 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, 0))
            port = int(s.getsockname()[1])

    app = make_wsgi_app(
        server,
        max_response_bytes=max_response_bytes,
        max_request_bytes=max_request_bytes,
        compression_level=compression_level,
        max_externalized_response_bytes=max_externalized_response_bytes,
        enable_sticky=enable_sticky,
        sticky_default_ttl=sticky_default_ttl,
        sticky_echo_headers=sticky_echo_headers,
    )

    if install_signal_handlers and enable_sticky:
        _install_drain_signal_handlers(app, drain_grace_seconds)

    print(f"PORT:{port}", flush=True)
    print(f"Serving on http://{host}:{port}/", file=sys.stderr, flush=True)
    body_buffer = max(_WAITRESS_MIN_BUFFER, max_request_bytes or _WAITRESS_DEFAULT_BUFFER)
    _waitress.serve(
        app,
        host=host,
        port=port,
        _quiet=True,
        inbuf_overflow=body_buffer,
        outbuf_overflow=body_buffer,
        recv_bytes=_WAITRESS_IO_CHUNK,
        send_bytes=_WAITRESS_IO_CHUNK,
    )


def _install_drain_signal_handlers(
    app: falcon.App[falcon.Request, falcon.Response],
    drain_grace_seconds: float,
) -> None:
    """Install SIGTERM / SIGINT handlers that drain sticky sessions before exit.

    Operates on the sticky registry through :func:`drain_handle`; a no-op
    if the app isn't sticky-enabled (defensive — :func:`serve_http` only
    calls this when sticky is on, but the check makes the helper safe to
    reuse in other contexts).

    The exit path uses ``os._exit`` to avoid Python's normal interpreter
    shutdown, which would join non-daemon threads and undo the grace-
    period semantics. ``state.close()`` has already been invoked on
    every live session by the time we exit, so the cleanup contract is
    upheld.
    """
    handle = drain_handle(app)
    if handle is None:
        return

    fired = threading.Event()

    def _drain_then_exit(signum: int, _frame: object) -> None:
        signal_name = signal.Signals(signum).name
        if fired.is_set():
            # Second signal: skip grace, exit now. Operators sending a
            # second signal are explicit about wanting immediate shutdown.
            _logger.warning(
                "Received second %s; exiting immediately without grace period",
                signal_name,
                extra={"signal": signal_name, "grace_skipped": True},
            )
            os._exit(1)
        fired.set()
        _logger.info(
            "Received %s; flipping drain flag (grace %.1fs before forced exit)",
            signal_name,
            drain_grace_seconds,
            extra={
                "signal": signal_name,
                "drain_grace_seconds": drain_grace_seconds,
            },
        )
        handle.drain()

        def _grace_expired() -> None:
            _logger.info(
                "Drain grace period elapsed; closing live sessions and exiting",
                extra={"drain_grace_seconds": drain_grace_seconds},
            )
            handle.shutdown()
            os._exit(0)

        # Daemon timer so it doesn't block process exit if the operator
        # double-signals while it's pending.
        timer = threading.Timer(drain_grace_seconds, _grace_expired)
        timer.daemon = True
        timer.start()

    for sig in (signal.SIGTERM, signal.SIGINT):
        # Skip signals the platform doesn't support (Windows lacks SIGTERM
        # in some configurations).
        try:
            signal.signal(sig, _drain_then_exit)
        except (OSError, ValueError):
            _logger.debug("Could not install handler for %s; skipping", sig)
