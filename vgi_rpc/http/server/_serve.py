# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""``serve_http`` — convenience wrapper that runs the WSGI app under waitress."""

from __future__ import annotations

import warnings

from vgi_rpc.rpc import RpcServer

from ._factory import make_wsgi_app


def serve_http(
    server: RpcServer,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    max_response_bytes: int | None = None,
    max_externalized_response_bytes: int | None = None,
    max_stream_response_bytes: int | None = None,
) -> None:
    """Serve an ``RpcServer`` over HTTP using waitress.

    This is a convenience wrapper that combines ``make_wsgi_app`` with
    automatic port selection and ``waitress.serve``.

    The selected port is printed to stdout as ``PORT:<port>`` for
    machine-readable discovery (e.g. by test harnesses or process managers).

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

    """
    import socket
    import sys

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
        max_externalized_response_bytes=max_externalized_response_bytes,
    )
    print(f"PORT:{port}", flush=True)
    print(f"Serving on http://{host}:{port}/", file=sys.stderr, flush=True)
    _waitress.serve(app, host=host, port=port, _quiet=True)
