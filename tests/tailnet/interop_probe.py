# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Call a foreign worker whose handler enforces the live identity contract."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

import httpx2

from vgi_rpc.conformance import ConformanceService
from vgi_rpc.http import http_connect
from vgi_rpc.rpc import tcp_connect


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="transport", required=True)
    tcp = subparsers.add_parser("tcp")
    tcp.add_argument("--host", required=True)
    tcp.add_argument("--port", required=True, type=int)
    http = subparsers.add_parser("http")
    http.add_argument("--url", required=True)
    http.add_argument("--spoof-login")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Prove two authenticated calls reach the foreign-language handler."""
    args = _parser().parse_args(argv)
    if args.transport == "tcp":
        with tcp_connect(ConformanceService, args.host, args.port, connect_timeout=20) as client:
            assert client.echo_string(value="python-to-foreign-1") == "python-to-foreign-1"
            assert client.echo_string(value="python-to-foreign-2") == "python-to-foreign-2"
    else:
        headers = {"Tailscale-User-Login": args.spoof_login} if args.spoof_login else None
        with (
            httpx2.Client(
                base_url=args.url,
                headers=headers,
                follow_redirects=True,
                timeout=20,
                trust_env=False,
            ) as http,
            http_connect(ConformanceService, client=http) as client,
        ):
            assert client.echo_string(value="python-to-foreign-1") == "python-to-foreign-1"
            assert client.echo_string(value="python-to-foreign-2") == "python-to-foreign-2"
    print(f"Python -> foreign {args.transport} Tailnet probe passed", flush=True)


if __name__ == "__main__":
    main()
