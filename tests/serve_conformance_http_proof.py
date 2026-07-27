# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance HTTP worker gated on proxy proof.

The reference implementation of the worker half of the proxy-proof contract.
Other-language runners provide an equivalent binary so the shared
``TestProxyProof`` group can be run against them; the CLI surface here is the
one they mirror.
"""

import argparse

import waitress

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.http import ProxyProofConfig, make_wsgi_app, parse_secrets, proxy_proof_gate, require_all
from vgi_rpc.rpc import RpcServer


def main() -> None:
    """Start a waitress HTTP server that gates RPC routes on a proxy proof."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--proof-mode", default="require", choices=("off", "allow", "require"))
    parser.add_argument("--proof-origin-id", default="conformance-origin")
    parser.add_argument("--proof-secrets", default="", help="kid:hex,kid:hex")
    parser.add_argument("--proof-skew", type=int, default=30)
    parser.add_argument(
        "--proof-no-replay-cache",
        action="store_true",
        help="Disable nonce replay rejection so replay-window behaviour can be observed.",
    )
    parser.add_argument("--prefix", default="")
    args = parser.parse_args()

    server = RpcServer(ConformanceService, ConformanceServiceImpl())

    authenticate = None
    if args.proof_mode != "off":
        config = ProxyProofConfig(
            mode=args.proof_mode,
            origin_id=args.proof_origin_id,
            secrets=parse_secrets(args.proof_secrets),
            skew_seconds=args.proof_skew,
            enable_replay_cache=not args.proof_no_replay_cache,
        )
        authenticate = require_all(proxy_proof_gate(config))

    app = make_wsgi_app(server, authenticate=authenticate, prefix=args.prefix)

    print(f"PORT:{args.port}", flush=True)
    waitress.serve(app, host="127.0.0.1", port=args.port, _quiet=True)


if __name__ == "__main__":
    main()
