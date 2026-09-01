# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Trusted HTTP forwarding of bridge-verified Iroh EndpointIds."""

from __future__ import annotations

import ipaddress
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
        immediate = _normalize_exact_ip(context.immediate_peer)
        if immediate is None or immediate not in self.trusted_proxy_addresses:
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
            proxy_address=immediate,
        )
        return PeerIdentityResult.available(identity)


def iroh_forwarded_header_provider(
    *,
    issuer: str,
    trusted_proxy_addresses: Iterable[str],
) -> PeerIdentityProvider:
    """Resolve a sanitized Iroh EndpointId from an exact trusted HTTP proxy."""
    if not isinstance(issuer, str) or not issuer or _contains_control(issuer):
        raise ValueError("issuer must be non-empty text without controls")
    try:
        issuer.encode()
    except UnicodeEncodeError as exc:
        raise ValueError("issuer must contain Unicode scalar values") from exc
    proxies: set[str] = set()
    for configured in trusted_proxy_addresses:
        normalized = _normalize_exact_ip(configured)
        if normalized is None:
            raise ValueError("trusted proxy addresses must be exact IP literals")
        if normalized in proxies:
            raise ValueError("duplicate normalized trusted proxy address")
        proxies.add(normalized)
    if not proxies:
        raise ValueError("issuer and trusted_proxy_addresses are required")
    return _IrohForwardedHeaderProvider(issuer, frozenset(proxies))


def _normalize_exact_ip(value: object) -> str | None:
    if not isinstance(value, str) or not value or "%" in value:
        return None
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        return None
    mapped = parsed.ipv4_mapped if isinstance(parsed, ipaddress.IPv6Address) else None
    return str(mapped or parsed)


def _contains_control(value: str) -> bool:
    return any(ord(character) <= 0x1F or ord(character) == 0x7F for character in value)
