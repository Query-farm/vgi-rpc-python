# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance worker shim that goes through ``run_server()``.

Used to verify the conformance suite runs end-to-end against a worker spawned
via ``vgi-rpc launch``.
"""

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.rpc import RpcServer, run_server


def main() -> None:
    """Entry point — argparse flags come from ``sys.argv``.

    Pre-builds the ``RpcServer`` with ``enable_describe=True`` so the
    conformance suite (which probes ``__describe__``) works without the
    caller needing to remember ``--describe``.
    """
    server = RpcServer(ConformanceService, ConformanceServiceImpl(), enable_describe=True)
    run_server(server)


if __name__ == "__main__":
    main()
