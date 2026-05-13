# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Thin worker shim used by ``test_launcher.py``.

Calls :func:`vgi_rpc.rpc.run_server` so tests exercise the new ``--unix`` /
``--idle-timeout`` argparse path end-to-end.
"""

import faulthandler
import sys

from tests._fixture_service import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.rpc import run_server


def main() -> None:
    """Entry point — argparse flags come from ``sys.argv``."""
    # If something hangs, dump every thread stack to stderr so the test can
    # surface it.  Safe in production-style tests because the worker is
    # expected to exit well before this fires.
    faulthandler.enable()
    # Dump every 4 seconds while alive — once the test starts waiting for
    # idle-shutdown we should see at most one or two dumps before the worker
    # exits cleanly, but several dumps if it hangs.
    faulthandler.dump_traceback_later(4.0, repeat=True, file=sys.stderr, exit=False)
    run_server(RpcFixtureService, RpcFixtureServiceImpl())


if __name__ == "__main__":
    main()
