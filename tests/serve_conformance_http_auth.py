# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance HTTP worker with a reject-all authenticate callback.

Used by conformance tests that assert ``GET /health`` is exempt from
authentication: every RPC endpoint on this server returns 401, but the
health probe must still succeed for orchestrators / load balancers.
"""

import argparse

import falcon
import waitress

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.http import make_wsgi_app
from vgi_rpc.rpc import AuthContext, RpcServer


def _reject_all(_req: falcon.Request) -> AuthContext:
    raise ValueError("authentication required")


def main() -> None:
    """Start a waitress HTTP server that rejects every authenticated route."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    server = RpcServer(ConformanceService, ConformanceServiceImpl())
    app = make_wsgi_app(server, authenticate=_reject_all)

    print(f"PORT:{args.port}", flush=True)
    waitress.serve(app, host="127.0.0.1", port=args.port, _quiet=True)


if __name__ == "__main__":
    main()
