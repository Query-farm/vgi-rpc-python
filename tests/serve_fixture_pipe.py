# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for pipe-based RPC fixture tests.

Installed as ``vgi-rpc-test-worker`` via pyproject.toml ``[project.scripts]``.
Can also be run directly: ``python -m tests.serve_fixture_pipe``

Imports the test service Protocol and implementation, then serves
RPC requests over stdin/stdout.
"""

from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.rpc import run_server


def main() -> None:
    """Serve the RPC fixture service over stdin/stdout."""
    run_server(RpcFixtureService, RpcFixtureServiceImpl())


if __name__ == "__main__":
    main()
