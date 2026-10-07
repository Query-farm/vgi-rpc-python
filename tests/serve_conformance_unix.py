# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Unix socket server entry point for conformance tests.

Can be run directly: ``python tests/serve_conformance_unix.py /path/to/socket``

Imports the conformance service Protocol and implementation, then serves
RPC requests over a Unix domain socket.
"""

import sys

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.conformance.secondary import conformance_extra_protocols
from vgi_rpc.rpc import RpcServer, serve_unix


def main() -> None:
    """Serve the conformance service over a Unix domain socket."""
    path = sys.argv[1]
    server = RpcServer(
        ConformanceService,
        ConformanceServiceImpl(),
        enable_describe=True,
        extra_protocols=conformance_extra_protocols(),
    )
    # Announce from on_bound: only once the socket is listening can a reader connect.
    serve_unix(server, path, on_bound=lambda bound: print(f"UNIX:{bound}", flush=True))


if __name__ == "__main__":
    main()
