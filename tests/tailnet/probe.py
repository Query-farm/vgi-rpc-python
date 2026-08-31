# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Assert peer identity evidence over a real Tailnet connection."""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
from collections.abc import Sequence
from typing import Any, cast

import httpx2

from tests.tailnet.service import TailnetEvidenceService
from vgi_rpc.http import http_connect
from vgi_rpc.rpc import tcp_connect


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="transport", required=True)
    tcp = subparsers.add_parser("tcp")
    tcp.add_argument("--host", required=True)
    tcp.add_argument("--port", type=int, required=True)
    tcp.add_argument("--proxy")
    tcp.add_argument("--require-local-dns-failure", action="store_true")
    http = subparsers.add_parser("http")
    http.add_argument("--url", required=True)
    http.add_argument("--spoof-login")

    for child in (tcp, http):
        child.add_argument("--expected-evidence-source", required=True)
        child.add_argument("--expected-assurance", required=True)
        child.add_argument("--expected-issuer", required=True)
        child.add_argument("--expected-subject-kind", required=True)
        child.add_argument("--expected-subject-stability", required=True)
        child.add_argument("--expected-capability", required=True)
        child.add_argument("--expected-tag")
        child.add_argument("--expected-target-kind")
        child.add_argument("--expected-target-value")
        child.add_argument("--expect-proxy", action="store_true")
        child.add_argument("--expect-authenticated", action="store_true")
        child.add_argument("--expect-principal-match", action="store_true")
        child.add_argument("--expect-evidence-binding", action="store_true")
    return parser


def _assert_snapshot(raw: str, args: argparse.Namespace) -> dict[str, Any]:
    payload = cast("dict[str, Any]", json.loads(raw))
    assert payload["provider_status"] == {"tailscale": "available"}, payload
    assert len(payload["identities"]) == 1, payload
    identity = payload["identities"][0]
    assert identity["provider"] == "tailscale", identity
    assert identity["evidence_source"] == args.expected_evidence_source, identity
    assert identity["assurance"] == args.expected_assurance, identity
    assert identity["issuer"] == args.expected_issuer, identity
    assert identity["subject_kind"] == args.expected_subject_kind, identity
    assert identity["subject_stability"] == args.expected_subject_stability, identity
    assert identity["subject_verified"] is (args.expected_subject_stability != "none"), identity
    assert (identity["subject_fingerprint"] is not None) is (args.expected_subject_stability != "none"), identity
    assert identity["capabilities_verified"] is True, identity
    assert args.expected_capability in identity["capability_names"], identity
    assert identity["proxy_present"] is args.expect_proxy, identity
    if args.expected_tag:
        assert args.expected_tag in identity["tags"], identity
    if args.expected_target_kind:
        actual_target = identity["capability_target"]
        assert actual_target["kind"] == args.expected_target_kind, identity
        if args.expected_target_value:
            assert actual_target["value"] == args.expected_target_value, identity
    if getattr(args, "spoof_login", None):
        spoofed = hashlib.sha256(f"login:{args.spoof_login}".encode()).hexdigest()
        assert identity["subject_fingerprint"] != spoofed, "Serve trusted a client-supplied identity header"
    assert payload["auth"]["authenticated"] is args.expect_authenticated, payload["auth"]
    assert payload["auth"]["principal_matches_identity"] is args.expect_principal_match, payload["auth"]
    assert payload["auth"]["peer_evidence_binding_present"] is args.expect_evidence_binding, payload["auth"]
    assert payload["auth"]["domain"] == ("tailscale" if args.expect_authenticated else None), payload["auth"]
    return payload


def _assert_no_local_dns(host: str, port: int) -> None:
    try:
        socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return
    raise AssertionError(f"{host!r} unexpectedly resolved outside the Tailnet SOCKS proxy")


def main(argv: Sequence[str] | None = None) -> None:
    """Run one live transport probe and assert its identity evidence."""
    args = _parser().parse_args(argv)
    if args.transport == "tcp":
        if args.require_local_dns_failure:
            _assert_no_local_dns(args.host, args.port)
        with tcp_connect(
            TailnetEvidenceService,
            args.host,
            args.port,
            proxy=args.proxy,
            connect_timeout=15,
        ) as client:
            first = _assert_snapshot(client.snapshot(), args)
            second = _assert_snapshot(client.snapshot(), args)
    else:
        headers = {"Tailscale-User-Login": args.spoof_login} if args.spoof_login else None
        with (
            httpx2.Client(
                base_url=args.url,
                headers=headers,
                follow_redirects=True,
                timeout=15,
                trust_env=False,
            ) as http_client,
            http_connect(TailnetEvidenceService, client=http_client) as client,
        ):
            first = _assert_snapshot(client.snapshot(), args)
            second = _assert_snapshot(client.snapshot(), args)
    assert first == second, "identity evidence changed within a stable test peer"
    print(json.dumps(first, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
