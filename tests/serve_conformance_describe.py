# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for CLI describe tests (conformance, pipe transport).

Serves the conformance RPC service with ``enable_describe=True`` over
stdin/stdout for introspection via the CLI.
"""

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.rpc import RpcServer, serve_stdio


def main() -> None:
    """Serve the conformance service with describe enabled over stdin/stdout."""
    server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
    serve_stdio(server)


if __name__ == "__main__":
    main()
