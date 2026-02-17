"""Subprocess server entry point for CLI describe tests (HTTP transport).

Starts a waitress HTTP server exposing the test RPC fixture service with
``enable_describe=True``.  Prints ``PORT:<n>`` to stdout so the parent
process can discover the port.
"""

import socket
import sys

import waitress

from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.http import make_wsgi_app
from vgi_rpc.rpc import RpcServer


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def main() -> None:
    """Start a waitress HTTP server for the RPC fixture service with describe."""
    port = int(sys.argv[1]) if len(sys.argv) > 1 else _find_free_port()

    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=True)
    app = make_wsgi_app(server)

    # Signal the port to the parent process
    print(f"PORT:{port}", flush=True)

    waitress.serve(app, host="127.0.0.1", port=port, _quiet=True)


if __name__ == "__main__":
    main()
