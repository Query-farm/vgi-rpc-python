# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for pool-based RPC fixture tests.

Run directly via ``python tests/serve_fixture_pool.py``.

Imports the pool test service Protocol and implementation, then serves
RPC requests over stdin/stdout.
"""

from tests.test_pool import PoolTestService, PoolTestServiceImpl
from vgi_rpc.rpc import run_server


def main() -> None:
    """Serve the pool test service over stdin/stdout."""
    run_server(PoolTestService, PoolTestServiceImpl())


if __name__ == "__main__":
    main()
