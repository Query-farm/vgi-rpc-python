# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for CLI describe tests (pipe transport).

Serves the test RPC fixture service with ``enable_describe=True`` over
stdin/stdout for introspection via the CLI.
"""

from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.rpc import RpcServer, serve_stdio


def main() -> None:
    """Serve the RPC fixture service with describe enabled over stdin/stdout."""
    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl(), enable_describe=True)
    serve_stdio(server)


if __name__ == "__main__":
    main()
