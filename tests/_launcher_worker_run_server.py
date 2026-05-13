# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Thin worker shim used by ``test_launcher.py``.

Calls :func:`vgi_rpc.rpc.run_server` so tests exercise the new ``--unix`` /
``--idle-timeout`` argparse path end-to-end.
"""

from tests._fixture_service import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.rpc import run_server


def main() -> None:
    """Entry point — argparse flags come from ``sys.argv``."""
    run_server(RpcFixtureService, RpcFixtureServiceImpl())


if __name__ == "__main__":
    main()
