"""Subprocess server entry point for HTTP-based RPC fixture tests.

Installed as ``vgi-rpc-test-http-worker`` via pyproject.toml ``[project.scripts]``.
Can also be run directly: ``python -m tests.serve_fixture_http``

Starts a uvicorn HTTP server exposing the test RPC service, prints
``PORT:<n>`` to stdout so the parent process can discover the port.
"""

import socket
import sys

import uvicorn

from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.http import make_asgi_app
from vgi_rpc.rpc import RpcServer


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def main() -> None:
    """Start a uvicorn HTTP server for the RPC fixture service."""
    port = int(sys.argv[1]) if len(sys.argv) > 1 else _find_free_port()

    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
    app = make_asgi_app(server)

    # Signal the port to the parent process
    print(f"PORT:{port}", flush=True)

    uvicorn.run(app, host="127.0.0.1", port=port, log_level="error")


if __name__ == "__main__":
    main()
