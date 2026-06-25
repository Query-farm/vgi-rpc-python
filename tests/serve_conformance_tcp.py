# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Threaded TCP socket server entry point for conformance tests.

Can be run directly: ``python tests/serve_conformance_tcp.py [HOST] [PORT]``

Binds a loopback TCP socket (host defaults to ``127.0.0.1``, port to ``0`` for
OS auto-select), then serves the conformance service over raw Arrow-IPC framing.
Emits a ``TCP:<host>:<port>`` discovery line on stdout once bound so the test
harness can learn the auto-selected port.
"""

import sys

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.rpc import RpcServer, serve_tcp


def main() -> None:
    """Serve the conformance service over a threaded TCP socket."""
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)

    def _emit(bound_host: str, bound_port: int) -> None:
        print(f"TCP:{bound_host}:{bound_port}", flush=True)

    serve_tcp(server, host, port, threaded=True, on_bound=_emit)


if __name__ == "__main__":
    main()
