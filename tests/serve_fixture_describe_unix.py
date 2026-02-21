"""Unix socket server entry point for CLI describe tests (fixture service).

Serves the test RPC fixture service with ``enable_describe=True`` over
a Unix domain socket for introspection via the CLI.
"""

import sys

from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.rpc import RpcServer, serve_unix


def main() -> None:
    """Serve the RPC fixture service with describe enabled over a Unix socket."""
    path = sys.argv[1]
    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=True)
    print(f"UNIX:{path}", flush=True)
    serve_unix(server, path)


if __name__ == "__main__":
    main()
