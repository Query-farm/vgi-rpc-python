# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Execute shared provider-neutral transport identity adapter vectors."""

from __future__ import annotations

import json
from importlib.resources import files
from typing import Any, cast

import pytest

from vgi_rpc import PeerResolutionContext
from vgi_rpc.http import (
    envoy_xfcc_spiffe_provider,
    gcp_load_balancer_spiffe_provider,
    tailscale_serve_header_provider,
)
from vgi_rpc.http._spiffe import validate_spiffe_id


@pytest.fixture(scope="module")
def vectors() -> dict[str, Any]:
    """Load the canonical adapter fixture shipped to every active SDK."""
    return cast(
        "dict[str, Any]",
        json.loads(
            files("vgi_rpc.conformance").joinpath("transport_identity_vectors.json").read_text(encoding="utf-8")
        ),
    )


def _headers(raw: dict[str, str]) -> dict[str, tuple[str, ...]]:
    return {name: (value,) for name, value in raw.items()}


def test_shared_spiffe_id_vectors(vectors: dict[str, Any]) -> None:
    """Pin canonical SPIFFE ID validation independent of certificate syntax."""
    for case in vectors["spiffe_id_cases"]:
        if case["expected"] == "valid":
            assert validate_spiffe_id(case["value"], frozenset({"example.org"}))[0] == case["value"]
        else:
            with pytest.raises(ValueError):
                validate_spiffe_id(case["value"], frozenset({"example.org"}))


def test_shared_envoy_xfcc_vectors(vectors: dict[str, Any]) -> None:
    """Pin strict adjacent Envoy SANITIZE_SET behavior."""
    provider = envoy_xfcc_spiffe_provider(trust_domains={"example.org"}, trusted_proxy_addresses={"127.0.0.1"})
    for case in vectors["envoy_xfcc_cases"]:
        result = provider(
            PeerResolutionContext(
                transport="http",
                immediate_peer="127.0.0.1",
                headers={"X-Forwarded-Client-Cert": (case["value"],)},
            )
        )
        assert result.status.value == case["expected"], case["name"]


def test_shared_gcp_vectors(vectors: dict[str, Any]) -> None:
    """Pin the complete GCP frontend mTLS signal combination."""
    provider = gcp_load_balancer_spiffe_provider(trust_domains={"example.org"}, trusted_proxy_addresses={"127.0.0.1"})
    for case in vectors["gcp_cases"]:
        result = provider(
            PeerResolutionContext(
                transport="http",
                immediate_peer="127.0.0.1",
                headers=_headers(case["headers"]),
            )
        )
        assert result.status.value == case["expected"], case["name"]


def test_shared_tailscale_serve_vectors(vectors: dict[str, Any]) -> None:
    """Pin login, subjectless capability, Funnel, and malformed evidence cases."""
    provider = tailscale_serve_header_provider(issuer="tailnet:test", trusted_proxy_addresses={"127.0.0.1"})
    for case in vectors["tailscale_serve_cases"]:
        result = provider(
            PeerResolutionContext(
                transport="http",
                immediate_peer="127.0.0.1",
                headers=_headers(case["headers"]),
            )
        )
        assert result.status.value == case["expected"], case["name"]
        if "subject_stability" in case:
            assert result.identities[0].subject_stability.value == case["subject_stability"]
