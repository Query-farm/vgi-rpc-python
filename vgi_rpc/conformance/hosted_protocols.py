# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""``vgi-rpc-test-hosted`` -- the hosted-protocols conformance group, for SDK workers.

Usage::

    vgi-rpc-test-hosted --cmd "vgi-fixture-worker" --expect vgi.v2,conformance.Secondary.v1
    vgi-rpc-test-hosted --unix /tmp/w.sock --expect vgi.v2,conformance.Secondary.v1
    vgi-rpc-test-hosted --url http://127.0.0.1:8123 --expect vgi.v2,conformance.Secondary.v1 --identity
    vgi-rpc-test-hosted --cmd "..." --expect ... -- -k Retry -x     # extra pytest arguments

Runs ``vgi_rpc.conformance._hosted_pytest`` under pytest against one worker on
one transport.  Needs ``pytest`` and ``pytest-timeout`` installed beside
``vgi-rpc[http,conformance]`` -- the same set the ports' CI installs.  The exit
code is pytest's: 0 when everything passed, non-zero otherwise.  See
``tools/cross-port/specs/MULTI_PROTOCOL_HOSTING.md``.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

__all__ = ["main"]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vgi-rpc-test-hosted",
        description="Hosted-protocols conformance group for workers hosting conformance.Secondary.v1.",
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--cmd", "-c", metavar="CMD", help="Worker command line (stdio transport)")
    target.add_argument("--unix", metavar="PATH", help="Unix domain socket the worker listens on")
    target.add_argument("--url", "-u", metavar="URL", help="HTTP base URL of the worker")
    parser.add_argument("--prefix", default="", help="HTTP route prefix (default: none)")
    parser.add_argument(
        "--expect",
        required=True,
        metavar="P1,P2,...",
        help="Application protocols the worker hosts, in registration order (e.g. vgi.v2,conformance.Secondary.v1)",
    )
    parser.add_argument(
        "--identity",
        action="store_true",
        help="The worker hosts vgi_rpc.Identity.v1 with the IDENTITY_CONFORMANCE_FIXTURE.md policy (HTTP only)",
    )
    parser.add_argument("pytest_args", nargs="*", help="Extra pytest arguments, after --")
    return parser


def main(argv: list[str] | None = None) -> None:
    """Run the hosted-protocols group and exit with pytest's status."""
    args = _build_parser().parse_args(argv)
    if args.identity and not args.url:
        sys.stderr.write("--identity requires --url: Identity is hosted on transports that authenticate callers\n")
        sys.exit(2)
    try:
        import pytest
    except ImportError:
        sys.stderr.write("vgi-rpc-test-hosted needs pytest and pytest-timeout: pip install pytest pytest-timeout\n")
        sys.exit(2)

    if args.cmd:
        transport, target = "pipe", args.cmd
    elif args.unix:
        transport, target = "unix", args.unix
    else:
        transport, target = "http", args.url.rstrip("/")

    os.environ["VGI_HOSTED_TRANSPORT"] = transport
    os.environ["VGI_HOSTED_TARGET"] = target
    os.environ["VGI_HOSTED_PREFIX"] = args.prefix
    os.environ["VGI_HOSTED_EXPECT"] = args.expect
    os.environ["VGI_HOSTED_IDENTITY"] = "1" if args.identity else ""

    module = Path(__file__).with_name("_hosted_pytest.py")
    # An explicit ini file, so whatever pytest configuration the *calling*
    # repository has (addopts like ``-n auto --mypy``) cannot leak into this
    # run: the group must behave identically in every SDK's CI.
    with tempfile.TemporaryDirectory(prefix="vgi-hosted-") as scratch:
        ini = Path(scratch) / "pytest.ini"
        ini.write_text("[pytest]\ntimeout_func_only = true\n")
        status = pytest.main(
            [
                str(module),
                "-c",
                str(ini),
                "--rootdir",
                str(module.parent),
                "-p",
                "no:cacheprovider",
                "-q",
                *args.pytest_args,
            ]
        )
    sys.exit(int(status))


if __name__ == "__main__":  # pragma: no cover - console-script entry point
    main()
