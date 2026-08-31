# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Run identity-aware VGI workers inside the Tailnet test topology."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

import waitress

from tests.tailnet.service import TailnetEvidenceImpl, TailnetEvidenceService
from vgi_rpc import RpcServer
from vgi_rpc.http import make_wsgi_app, tailscale_localapi_provider, tailscale_serve_header_provider
from vgi_rpc.rpc import peer_identity_primary, require_peer_identity, serve_tcp


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issuer", required=True)
    subparsers = parser.add_subparsers(dest="transport", required=True)

    tcp = subparsers.add_parser("tcp")
    tcp.add_argument("--host", default="127.0.0.1")
    tcp.add_argument("--port", type=int, required=True)
    tcp.add_argument("--localapi-socket", required=True)
    tcp.add_argument("--service-name")
    tcp.add_argument("--proxy-protocol", choices=("off", "required"), default="off")
    tcp.add_argument("--trusted-proxy-address", action="append", default=[])

    http = subparsers.add_parser("http")
    http.add_argument("--host", default="127.0.0.1")
    http.add_argument("--port", type=int, required=True)
    http.add_argument("--trusted-proxy-address", action="append", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    """Start the configured raw-TCP or HTTP integration worker."""
    args = _parser().parse_args(argv)
    server = RpcServer(TailnetEvidenceService, TailnetEvidenceImpl())
    if args.transport == "tcp":
        provider = tailscale_localapi_provider(issuer=args.issuer, unix_socket=args.localapi_socket)
        serve_tcp(
            server,
            args.host,
            args.port,
            threaded=True,
            proxy_protocol=args.proxy_protocol,
            trusted_proxy_addresses=tuple(args.trusted_proxy_address),
            service_name=args.service_name,
            peer_identity_providers=(provider,),
            peer_authentication_policy=peer_identity_primary("tailscale"),
        )
        return

    provider = tailscale_serve_header_provider(
        issuer=args.issuer,
        trusted_proxy_addresses=args.trusted_proxy_address,
    )
    app = make_wsgi_app(
        server,
        peer_identity_providers=(provider,),
        peer_authentication_policy=require_peer_identity("tailscale"),
        enable_landing_page=False,
        enable_describe_page=False,
    )
    waitress.serve(app, host=args.host, port=args.port, threads=8, _quiet=True)


if __name__ == "__main__":
    main()
