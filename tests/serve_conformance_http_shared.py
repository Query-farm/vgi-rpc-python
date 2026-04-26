# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance HTTP worker with an externally-supplied HMAC signing key.

Used by two-server round-robin conformance fixtures: both workers are
launched with the same ``--key`` so state tokens minted by one server
verify on the other.  This exercises the protocol's "state lives in the
signed token" contract and exposes any server-side resident stream state.
"""

import argparse

import waitress

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.http import make_wsgi_app
from vgi_rpc.rpc import RpcServer


def main() -> None:
    """Start a waitress HTTP server with a caller-supplied signing key."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--key", required=True, help="hex-encoded 32-byte HMAC signing key")
    args = parser.parse_args()

    server = RpcServer(ConformanceService, ConformanceServiceImpl())
    app = make_wsgi_app(server, signing_key=bytes.fromhex(args.key))

    print(f"PORT:{args.port}", flush=True)
    waitress.serve(app, host="127.0.0.1", port=args.port, _quiet=True)


if __name__ == "__main__":
    main()
