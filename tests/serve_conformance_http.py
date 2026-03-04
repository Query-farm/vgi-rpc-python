# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for HTTP-based conformance tests.

Can be run directly: ``python -m tests.serve_conformance_http``

Starts an HTTP server exposing the conformance RPC service via ``run_server --http``.
"""

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.rpc import run_server


def main() -> None:
    """Start an HTTP server for the conformance service."""
    run_server(ConformanceService, ConformanceServiceImpl())


if __name__ == "__main__":
    main()
