"""Unix socket server entry point for conformance tests.

Can be run directly: ``python tests/serve_conformance_unix.py /path/to/socket``

Imports the conformance service Protocol and implementation, then serves
RPC requests over a Unix domain socket.
"""

import sys

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.rpc import RpcServer, serve_unix


def main() -> None:
    """Serve the conformance service over a Unix domain socket."""
    path = sys.argv[1]
    server = RpcServer(ConformanceService, ConformanceServiceImpl())
    print(f"UNIX:{path}", flush=True)
    serve_unix(server, path)


if __name__ == "__main__":
    main()
