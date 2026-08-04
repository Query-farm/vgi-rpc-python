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
from collections.abc import Callable, Mapping

import falcon

from vgi_rpc.rpc import AuthContext, RpcServer

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

# waitress's own default is 4. A VGI request can hold its thread for the length
# of a scan rather than the milliseconds a web request takes, so 4 concurrent
# clients is enough to make every further one queue — measured as a worker
# serializing six parallel clients that should have overlapped. 16 keeps the
# thread cost trivial while removing that ceiling for ordinary fan-out.
_DEFAULT_THREADS = 4  # waitress's own default; see _resolve_threads


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
    authenticate: Callable[[falcon.Request], AuthContext] | None = None,
    proxy_proof_required: bool = False,
    token_key: bytes | None = None,
    enable_sticky: bool = False,
    sticky_default_ttl: float = 300.0,
    sticky_echo_headers: Mapping[str, str] | None = None,
    drain_grace_seconds: float = 30.0,
    install_signal_handlers: bool = True,
    threads: int | None = None,
    call_state_cache_entries: int = 4096,
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
        authenticate: Per-request authenticate callback.  See
            :func:`make_wsgi_app`.  Without this parameter the only way to
            serve an authenticated worker is to call ``make_wsgi_app`` and
            run waitress by hand, which is how workers end up shipping with
            no authentication at all.
        proxy_proof_required: Advertise ``VGI-Proxy-Proof-Required``.  See
            :func:`make_wsgi_app`.
        token_key: Stable AEAD key for sealed state tokens.  See
            :func:`make_wsgi_app`.  When ``None`` a random per-process key is
            generated, so tokens do not survive a restart or work across
            processes.
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
        threads: Waitress worker threads, i.e. how many requests the server
            handles concurrently before the rest queue.  ``None`` (the default)
            resolves ``VGI_HTTP_THREADS``, then falls back to
            ``_DEFAULT_THREADS``.  See :func:`_resolve_threads` for why
            waitress's own default of 4 is too low here.
        call_state_cache_entries: Size of the per-process call-state cache;
            ``0`` disables it.  See :func:`make_wsgi_app`.

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
        authenticate=authenticate,
        proxy_proof_required=proxy_proof_required,
        token_key=token_key,
        enable_sticky=enable_sticky,
        sticky_default_ttl=sticky_default_ttl,
        sticky_echo_headers=sticky_echo_headers,
        call_state_cache_entries=call_state_cache_entries,
    )

    if install_signal_handlers and enable_sticky:
        _install_drain_signal_handlers(app, drain_grace_seconds)

    _tame_queue_depth_logger()

    print(f"PORT:{port}", flush=True)
    print(f"Serving on http://{host}:{port}/", file=sys.stderr, flush=True)
    _waitress.serve(
        app,
        host=host,
        port=port,
        _quiet=True,
        threads=_resolve_threads(threads),
        **waitress_arrow_tuning(max_request_bytes),
    )


def waitress_arrow_tuning(max_request_bytes: int | None = None) -> dict[str, int]:
    """Waitress buffer settings sized for Arrow bodies rather than web pages.

    Waitress's defaults assume small payloads and are actively hostile to
    this workload: ``inbuf_overflow`` (512 KiB) spools a larger request body
    to a temp file before the app sees it, ``outbuf_overflow`` (1 MiB) does
    the same for responses, and ``recv_bytes`` (8 KiB) assembles a multi-MiB
    body from hundreds of ``recv()`` calls. A single emitted batch is
    routinely megabytes, so all three sit on the hot path.

    Exposed as a helper because there are three places that start waitress
    for this workload -- ``serve_http``, ``vgi-serve``, and
    ``Worker.serve_http`` -- and they had drifted, with the CLI everyone
    actually runs being the untuned one. One definition, no drift.

    Args:
        max_request_bytes: The advertised request cap, when there is one.
            Buffers are sized to hold a whole body in memory, floored so a
            small cap does not reintroduce spooling.

    Returns:
        Keyword arguments to splat into ``waitress.serve``.

    """
    body_buffer = max(_WAITRESS_MIN_BUFFER, max_request_bytes or _WAITRESS_DEFAULT_BUFFER)
    return {
        "inbuf_overflow": body_buffer,
        "outbuf_overflow": body_buffer,
        "recv_bytes": _WAITRESS_IO_CHUNK,
        "send_bytes": _WAITRESS_IO_CHUNK,
    }


def _tame_queue_depth_logger() -> None:
    """Stop waitress warning once per request that its queue is non-empty.

    ``ThreadedTaskDispatcher.add_task`` logs at WARNING whenever
    ``queue_size > idle_threads``. For a server whose requests are longer
    than a web page load that is not an exceptional condition, it is the
    normal state: with more concurrent clients than threads the queue is
    non-empty essentially always, so the warning fires on **every request**.

    That is not free. Because it is WARNING it passes the default level, so
    a ``LogRecord`` is built and formatted per request -- measured at 5.7% of
    a loaded server's GIL-held time, more than this framework spends on Arrow
    serialization. And it interacts badly with the configuration that is
    actually fastest for CPU-bound workers: fewer threads means a deeper
    queue means more warnings.

    The signal itself is real, so this raises the bar to ERROR rather than
    disabling the logger -- saturation still surfaces through request
    latency in the access log, which is where an operator can see it in
    context. ``VGI_RPC_WAITRESS_QUEUE_LOG=1`` restores waitress's own
    warning for anyone who wants it.
    """
    if (os.environ.get("VGI_RPC_WAITRESS_QUEUE_LOG") or "").strip().lower() in ("1", "true", "yes"):
        return
    # Level, not a filter: a filter runs *after* the record is constructed,
    # which is the part that costs.
    logging.getLogger("waitress.queue").setLevel(logging.ERROR)


def _resolve_threads(threads: int | None) -> int:
    """Decide waitress's worker-thread count.

    These are real Python threads sharing one GIL, so for a request path that
    is CPU-bound Python they cannot add throughput -- they can only contend.
    Measured on a pure-Arrow scan, aggregate turns/s across 24 clients:

        1 thread   1311      4 threads   855      16 threads   889

    A function that blocks on external I/O releases the GIL while blocked, and
    there the picture inverts -- same harness against a fixture that sleeps
    5ms per tick:

        1 thread    161      4 threads   556      16 threads   301

    So the choice is genuinely workload-dependent between 1 and 4, but **16 is
    dominated**: worse than 4 for blocking functions and worse than 1 for
    computing ones. This previously defaulted to 16 on the reasoning that
    waitress's 4 was "far too few" because a VGI request can occupy its thread
    for a whole scan. That reasoning holds for a blocking workload and is
    backwards for a computing one, and the measurements above say 4 already
    covers the blocking case.

    Defaulting to 4 is therefore just declining to override waitress. The
    asymmetry supports it: choosing 1 costs a blocking function 3.4x, while
    choosing 4 costs a computing one 1.5x.

    Deployments that know their shape should say so -- ``--http-threads 1``
    for Arrow-crunching functions, higher for ones that call out to a
    database or an API.

    Resolution order: the explicit argument, then ``VGI_HTTP_THREADS``, then
    :data:`_DEFAULT_THREADS`. The env var exists because a worker is usually
    launched by something that does not own its argv (a container, a test
    harness), the same reason ``VGI_WORKER_LOG_*`` exists.

    Args:
        threads: The caller's explicit value, or None to resolve from the
            environment.

    Returns:
        The thread count to hand waitress.

    Raises:
        SystemExit: ``VGI_HTTP_THREADS`` is set to something that is not a
            positive integer -- silently falling back would hide the misconfig.

    """
    if threads is not None:
        return threads
    raw = os.environ.get("VGI_HTTP_THREADS")
    if not raw:
        return _DEFAULT_THREADS
    try:
        value = int(raw)
    except ValueError:
        sys.exit(f"VGI_HTTP_THREADS={raw!r} is not an integer")
    if value < 1:
        sys.exit(f"VGI_HTTP_THREADS={raw!r} must be >= 1")
    return value


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
