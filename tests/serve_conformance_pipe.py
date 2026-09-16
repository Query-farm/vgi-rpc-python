# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for pipe-based conformance tests.

Can be run directly: ``python -m tests.serve_conformance_pipe``

Delegates to :func:`vgi_rpc.conformance._cli.main` with ``--pipe`` and
``--describe`` pre-selected, forwarding any further arguments unchanged.

This used to build its own ``RpcServer`` and take no arguments at all, which
made it a second, flag-less definition of "the reference byte-stream peer"
sitting beside the CLI. Ports split between the two: some spawn
``vgi-rpc-conformance --pipe``, others spawn this script. When
``--fake-storage``/``--externalize-threshold`` were added to the CLI so the
byte-stream externalization group could reach a reference peer, the ports
pointed here silently did not get them -- and a flag passed to a script with
no argument parser is discarded without complaint, so the failure is a hang
rather than an error.

Delegating keeps one definition. A flag added to the CLI reaches every port
on its next run, whichever spelling it uses.
"""

import sys

from vgi_rpc.conformance._cli import main as _cli_main


def main() -> None:
    """Serve the conformance service over stdin/stdout via the shared CLI."""
    # ``--describe`` preserves this entry point's long-standing behaviour:
    # it hardcoded ``enable_describe=True`` so ``TestDescribeConformance``
    # could probe a real subprocess worker, while the CLI defaults it off.
    sys.argv = [sys.argv[0], "--pipe", "--describe", *sys.argv[1:]]
    _cli_main()


if __name__ == "__main__":
    main()
