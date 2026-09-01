# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Trusted HTTP forwarding of bridge-verified Iroh EndpointIds."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final

from vgi_rpc.rpc import (
    IdentityAssurance,
    PeerIdentity,
    PeerIdentityProvider,
    PeerIdentityResult,
    PeerIdentityStatus,
    PeerResolutionContext,
    SubjectKind,
    SubjectStability,
)

IROH_ENDPOINT_HEADER: Final = "VGI-Forwarded-Iroh-Endpoint"
_PROVIDER: Final = "iroh"
_CANONICAL_ENDPOINT = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True)
class _IrohForwardedHeaderProvider:
    issuer: str
    trusted_proxy_addresses: frozenset[str]
    provider: str = _PROVIDER

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        if context.immediate_peer not in self.trusted_proxy_addresses:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.UNTRUSTED_PROXY)
        try:
            endpoint_id = context.header(IROH_ENDPOINT_HEADER)
        except PermissionError:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        if endpoint_id is None:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.NO_MATCH)
        if _CANONICAL_ENDPOINT.fullmatch(endpoint_id) is None:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        identity = PeerIdentity(
            provider=self.provider,
            evidence_source="http_proxy",
            assurance=IdentityAssurance.CONFIGURED_PROXY,
            issuer=self.issuer,
            transport="http",
            subject_kind=SubjectKind.ENDPOINT,
            subject_key=endpoint_id,
            subject_stability=SubjectStability.STABLE,
            subject_verified=True,
            attributes={"original_assurance": IdentityAssurance.CRYPTOGRAPHIC_PEER.value},
            source_address=endpoint_id,
            proxy_address=context.immediate_peer,
        )
        return PeerIdentityResult.available(identity)


def iroh_forwarded_header_provider(
    *,
    issuer: str,
    trusted_proxy_addresses: Iterable[str],
) -> PeerIdentityProvider:
    """Resolve a sanitized Iroh EndpointId from an exact trusted HTTP proxy."""
    proxies = frozenset(trusted_proxy_addresses)
    if not isinstance(issuer, str) or not issuer or not proxies:
        raise ValueError("issuer and trusted_proxy_addresses are required")
    if any(
        not isinstance(address, str) or not address or any(character in address for character in "\r\n\x00")
        for address in proxies
    ):
        raise ValueError("trusted proxy addresses must be exact non-empty values")
    try:
        issuer.encode()
        for address in proxies:
            address.encode()
    except UnicodeEncodeError as exc:
        raise ValueError("issuer and trusted proxy addresses must contain Unicode scalar values") from exc
    return _IrohForwardedHeaderProvider(issuer, proxies)
