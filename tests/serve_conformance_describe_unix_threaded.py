"""Threaded Unix socket server entry point for CLI describe tests (conformance service).

Serves the conformance RPC service with ``enable_describe=True`` over
a threaded Unix domain socket for introspection via the CLI.
"""

import sys

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.rpc import RpcServer, serve_unix


def main() -> None:
    """Serve the conformance service with describe enabled over a threaded Unix socket."""
    path = sys.argv[1]
    server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
    print(f"UNIX:{path}", flush=True)
    serve_unix(server, path, threaded=True)


if __name__ == "__main__":
    main()
