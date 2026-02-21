# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Threaded Unix socket server entry point for RPC fixture tests.

Can be run directly: ``python tests/serve_fixture_unix_threaded.py /path/to/socket``

Imports the test service Protocol and implementation, then serves
RPC requests over a threaded Unix domain socket.
"""

import sys

from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.rpc import RpcServer, serve_unix


def main() -> None:
    """Serve the RPC fixture service over a threaded Unix domain socket."""
    path = sys.argv[1]
    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
    print(f"UNIX:{path}", flush=True)
    serve_unix(server, path, threaded=True)


if __name__ == "__main__":
    main()
