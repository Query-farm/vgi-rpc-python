# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Trusted Iroh bridge identity provider tests."""

from __future__ import annotations

import pytest

from vgi_rpc import (
    IROH_ENDPOINT_HEADER,
    AuthContext,
    IdentityAssurance,
    PeerEvidenceSet,
    PeerIdentityStatus,
    PeerResolutionContext,
    iroh_forwarded_header_provider,
)
from vgi_rpc.rpc import peer_identity_primary

_ENDPOINT = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"


def _context(*, peer: str = "127.0.0.1", values: tuple[str, ...] = (_ENDPOINT,)) -> PeerResolutionContext:
    return PeerResolutionContext(
        transport="http",
        immediate_peer=peer,
        headers={IROH_ENDPOINT_HEADER: values},
    )


def test_forwarded_iroh_identity_is_stable_and_namespaced_locally() -> None:
    """Trusted canonical evidence becomes a stable locally namespaced subject."""
    provider = iroh_forwarded_header_provider(
        issuer="production-mesh",
        trusted_proxy_addresses={"127.0.0.1"},
    )
    result = provider(_context())
    identity = result.identities[0]
    assert result.status is PeerIdentityStatus.AVAILABLE
    assert identity.subject_key == _ENDPOINT
    assert identity.issuer == "production-mesh"
    assert identity.assurance is IdentityAssurance.CONFIGURED_PROXY
    assert identity.attributes["original_assurance"] == "cryptographic_peer"
    auth = peer_identity_primary("iroh")(
        PeerEvidenceSet.from_results((result,)),
        AuthContext.anonymous(),
    )
    assert auth.authenticated
    assert "/production-mesh/" in (auth.principal or "")


@pytest.mark.parametrize(
    ("context", "status"),
    [
        (_context(peer="192.0.2.1"), PeerIdentityStatus.UNTRUSTED_PROXY),
        (_context(values=()), PeerIdentityStatus.NO_MATCH),
        (_context(values=(_ENDPOINT, _ENDPOINT)), PeerIdentityStatus.INVALID),
        (_context(values=(_ENDPOINT.upper(),)), PeerIdentityStatus.INVALID),
        (_context(values=("00",)), PeerIdentityStatus.INVALID),
    ],
)
def test_forwarded_iroh_identity_fails_closed(
    context: PeerResolutionContext,
    status: PeerIdentityStatus,
) -> None:
    """Untrusted, missing, duplicate, or non-canonical evidence is unavailable."""
    provider = iroh_forwarded_header_provider(
        issuer="production-mesh",
        trusted_proxy_addresses={"127.0.0.1"},
    )
    assert provider(context).status is status


def test_forwarded_iroh_headers_reject_controls_and_case_duplicates() -> None:
    """Header normalization cannot turn ambiguous or control-bearing evidence into identity."""
    provider = iroh_forwarded_header_provider(
        issuer="production-mesh",
        trusted_proxy_addresses=("127.0.0.1",),
    )
    assert provider(_context(values=(f"{_ENDPOINT}\t",))).status is PeerIdentityStatus.INVALID
    with pytest.raises(PermissionError):
        PeerResolutionContext(
            transport="http",
            immediate_peer="127.0.0.1",
            headers={
                IROH_ENDPOINT_HEADER: (_ENDPOINT,),
                IROH_ENDPOINT_HEADER.lower(): (_ENDPOINT,),
            },
        )


def test_forwarded_iroh_provider_requires_explicit_trust() -> None:
    """No implicit loopback or empty proxy trust boundary is permitted."""
    with pytest.raises(ValueError):
        iroh_forwarded_header_provider(issuer="production-mesh", trusted_proxy_addresses=())
    with pytest.raises(ValueError):
        iroh_forwarded_header_provider(
            issuer="production-mesh",
            trusted_proxy_addresses=("localhost",),
        )
    with pytest.raises(ValueError):
        iroh_forwarded_header_provider(
            issuer="production-mesh",
            trusted_proxy_addresses=("127.0.0.1", "::ffff:127.0.0.1"),
        )
    with pytest.raises(ValueError):
        iroh_forwarded_header_provider(
            issuer="production\tmesh",
            trusted_proxy_addresses=("127.0.0.1",),
        )


def test_forwarded_iroh_provider_normalizes_mapped_ipv4() -> None:
    """Equivalent IPv4-mapped bridge addresses share one exact trust identity."""
    provider = iroh_forwarded_header_provider(
        issuer="production-mesh",
        trusted_proxy_addresses=("::ffff:127.0.0.1",),
    )
    result = provider(_context(peer="127.0.0.1"))
    assert result.status is PeerIdentityStatus.AVAILABLE
    assert result.identities[0].proxy_address == "127.0.0.1"
