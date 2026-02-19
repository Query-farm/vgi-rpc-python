"""CLI entry point for the conformance test server.

Uses argparse (stdlib) to avoid requiring optional dependencies.

Usage::

    vgi-rpc-conformance --pipe              # stdio pipe transport (default)
    vgi-rpc-conformance --http [PORT]       # HTTP via waitress
    vgi-rpc-conformance --describe          # Enable __describe__ introspection

"""

from __future__ import annotations

import argparse
import sys

from vgi_rpc.conformance._impl import ConformanceServiceImpl
from vgi_rpc.conformance._protocol import ConformanceService
from vgi_rpc.rpc import RpcServer, run_server


def main() -> None:
    """Run the conformance test server."""
    parser = argparse.ArgumentParser(description="vgi-rpc conformance test server")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--pipe", action="store_true", default=True, help="Serve over stdin/stdout pipe (default)")
    group.add_argument("--http", nargs="?", type=int, const=0, default=None, metavar="PORT", help="Serve over HTTP")
    parser.add_argument("--describe", action="store_true", help="Enable __describe__ introspection")
    args = parser.parse_args()

    impl = ConformanceServiceImpl()
    server = RpcServer(ConformanceService, impl, enable_describe=args.describe)

    if args.http is not None:
        _serve_http(server, args.http)
    else:
        run_server(server)


def _serve_http(server: RpcServer, port: int) -> None:
    """Start an HTTP server with the given RpcServer."""
    try:
        from vgi_rpc.http import make_wsgi_app
    except ImportError:
        print("HTTP transport requires vgi-rpc[http]: pip install vgi-rpc[http]", file=sys.stderr)
        sys.exit(1)

    try:
        import waitress
    except ImportError:
        print("HTTP transport requires waitress: pip install vgi-rpc[http]", file=sys.stderr)
        sys.exit(1)

    import socket

    if port == 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = int(s.getsockname()[1])

    app = make_wsgi_app(server)
    print(f"PORT:{port}", flush=True)
    waitress.serve(app, host="127.0.0.1", port=port, _quiet=True)
