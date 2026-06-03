# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for pipe-based conformance tests.

Can be run directly: ``python -m tests.serve_conformance_pipe``

Imports the conformance service Protocol and implementation, then serves
RPC requests over stdin/stdout.
"""

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.rpc import RpcServer, run_server


def main() -> None:
    """Serve the conformance service over stdin/stdout.

    Pre-builds the ``RpcServer`` with ``enable_describe=True`` so the
    ``TestDescribeConformance`` suite can probe ``__describe__`` against this
    real subprocess worker (not just an in-process Python server).
    """
    server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
    run_server(server)


if __name__ == "__main__":
    main()
