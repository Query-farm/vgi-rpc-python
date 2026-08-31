# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""SPIFFE X.509-SVID evidence from a trusted HTTP proxy."""

from __future__ import annotations

import datetime
import re
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from cryptography import x509
from cryptography.x509.oid import ExtendedKeyUsageOID

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

_SPIFFE_PROVIDER = "spiffe"
_TRUST_DOMAIN_RE = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,253}[a-z0-9])?$")
_PATH_RE = re.compile(r"/(?:[A-Za-z0-9._-]+)(?:/[A-Za-z0-9._-]+)*$")
_XFCC_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
_SHA256_RE = re.compile(r"^[0-9A-Fa-f]{64}$")
_PERCENT_ESCAPE_RE = re.compile(r"%(?:[0-9A-Fa-f]{2})")


def validate_spiffe_id(value: str, trust_domains: frozenset[str]) -> tuple[str, str]:
    """Validate a workload SPIFFE ID and return its ID and trust domain."""
    if not value or not value.isascii() or len(value.encode()) > 2048 or "%" in value:
        raise ValueError("SPIFFE ID is empty or exceeds 2048 bytes")
    parsed = urlsplit(value)
    if parsed.scheme != "spiffe" or parsed.query or parsed.fragment:
        raise ValueError("invalid SPIFFE ID scheme, query, or fragment")
    if parsed.username is not None or parsed.password is not None or parsed.port is not None:
        raise ValueError("SPIFFE ID authority cannot contain userinfo or a port")
    trust_domain = parsed.hostname or ""
    if parsed.netloc != trust_domain or not _TRUST_DOMAIN_RE.fullmatch(trust_domain):
        raise ValueError("invalid SPIFFE trust domain")
    if not _PATH_RE.fullmatch(parsed.path):
        raise ValueError("leaf X.509-SVID path is not canonical")
    if any(segment in (".", "..") for segment in parsed.path.split("/")):
        raise ValueError("SPIFFE ID path cannot contain dot segments")
    if trust_domain not in trust_domains:
        raise ValueError("SPIFFE trust domain is not allowed")
    return value, trust_domain


def _identity_from_verified_x509_svid(
    cert: x509.Certificate,
    *,
    trust_domains: frozenset[str],
    source_address: str | None,
    proxy_address: str | None,
    evidence_source: str = "verified_certificate_header",
    assurance: IdentityAssurance = IdentityAssurance.CONFIGURED_PROXY,
    transport: str = "http",
) -> PeerIdentity:
    """Validate a leaf SVID after the transport boundary validates its chain."""
    now = datetime.datetime.now(datetime.UTC)
    if now < cert.not_valid_before_utc or now > cert.not_valid_after_utc:
        raise ValueError("X.509-SVID is outside its validity period")

    san_extension = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
    if not cert.subject and not san_extension.critical:
        raise ValueError("subjectless X.509-SVID requires a critical SAN extension")
    sans = san_extension.value
    uri_sans = sans.get_values_for_type(x509.UniformResourceIdentifier)
    if len(uri_sans) != 1:
        raise ValueError("X.509-SVID must contain exactly one URI SAN")
    spiffe_id, trust_domain = validate_spiffe_id(uri_sans[0], trust_domains)

    basic = cert.extensions.get_extension_for_class(x509.BasicConstraints)
    if basic.value.ca:
        raise ValueError("X.509-SVID leaf cannot be a CA")
    key_usage = cert.extensions.get_extension_for_class(x509.KeyUsage)
    if not key_usage.critical or not key_usage.value.digital_signature:
        raise ValueError("X.509-SVID key usage must be critical and allow digitalSignature")
    if key_usage.value.key_cert_sign or key_usage.value.crl_sign:
        raise ValueError("X.509-SVID leaf cannot sign certificates or CRLs")
    try:
        extended = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    except x509.ExtensionNotFound:
        pass
    else:
        if ExtendedKeyUsageOID.CLIENT_AUTH not in extended or ExtendedKeyUsageOID.SERVER_AUTH not in extended:
            raise ValueError("X.509-SVID extended key usage must include clientAuth and serverAuth")

    return PeerIdentity(
        provider=_SPIFFE_PROVIDER,
        evidence_source=evidence_source,
        assurance=assurance,
        issuer=f"spiffe://{trust_domain}",
        transport=transport,
        subject_kind=SubjectKind.WORKLOAD,
        subject_key=spiffe_id,
        subject_stability=SubjectStability.STABLE,
        subject_verified=True,
        source_address=source_address,
        proxy_address=proxy_address,
    )


@dataclass(frozen=True)
class _SpiffeX509HeaderProvider:
    trust_domains: frozenset[str]
    trusted_proxy_addresses: frozenset[str]
    header: str
    chain_verified_header: str | None
    chain_verified_value: str
    max_header_bytes: int
    evidence_source: str = "verified_certificate_header"
    provider: str = _SPIFFE_PROVIDER

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        if context.immediate_peer not in self.trusted_proxy_addresses:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.UNTRUSTED_PROXY)
        raw = context.header(self.header)
        if not raw:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.NO_MATCH)
        if not raw.isascii() or len(raw.encode()) > self.max_header_bytes:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        if self.chain_verified_header is not None:
            verified = context.header(self.chain_verified_header)
            if verified is None or len(verified.encode()) > 64 or verified != self.chain_verified_value:
                return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        try:
            decoded = unquote(raw)
            if (
                not decoded.isascii()
                or len(decoded.encode()) > self.max_header_bytes
                or decoded.count("-----BEGIN CERTIFICATE-----") != 1
                or decoded.count("-----END CERTIFICATE-----") != 1
                or not decoded.strip().endswith("-----END CERTIFICATE-----")
            ):
                return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
            cert = x509.load_pem_x509_certificate(decoded.encode())
            identity = _identity_from_verified_x509_svid(
                cert,
                trust_domains=self.trust_domains,
                source_address=context.asserted_peer,
                proxy_address=context.immediate_peer,
                evidence_source=self.evidence_source,
            )
        except (TypeError, ValueError, x509.ExtensionNotFound):
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        return PeerIdentityResult.available(identity)


def spiffe_x509_header_provider(
    *,
    trust_domains: Iterable[str],
    trusted_proxy_addresses: Iterable[str],
    header: str = "X-SSL-Client-Cert",
    chain_verified_header: str,
    chain_verified_value: str = "true",
    max_header_bytes: int = 16_384,
) -> PeerIdentityProvider:
    """Create a strict X.509-SVID provider for a trusted proxy boundary.

    The proxy must validate the certificate chain against the appropriate
    SPIFFE bundle and strip client-supplied copies of all configured headers.
    A positive chain-verification header is mandatory. The proxy must strip
    client-supplied copies of both configured headers.
    """
    domains = frozenset(trust_domains)
    proxies = frozenset(trusted_proxy_addresses)
    if not domains or not proxies:
        raise ValueError("trust_domains and trusted_proxy_addresses must not be empty")
    if (
        not header
        or not chain_verified_header
        or header.lower() == chain_verified_header.lower()
        or any(character in header + chain_verified_header + chain_verified_value for character in "\r\n\x00")
        or max_header_bytes <= 0
    ):
        raise ValueError("chain_verified_header and a positive max_header_bytes are required")
    for domain in domains:
        if not _TRUST_DOMAIN_RE.fullmatch(domain):
            raise ValueError(f"invalid SPIFFE trust domain: {domain!r}")
    return _SpiffeX509HeaderProvider(
        domains,
        proxies,
        header,
        chain_verified_header,
        chain_verified_value,
        max_header_bytes,
    )


def aws_alb_spiffe_provider(
    *,
    trust_domains: Iterable[str],
    trusted_proxy_addresses: Iterable[str],
    leaf_header: str = "X-Amzn-Mtls-Clientcert-Leaf",
    max_header_bytes: int = 16_384,
) -> PeerIdentityProvider:
    """Consume an X.509-SVID from an AWS ALB mTLS *verify-mode* listener.

    AWS ALB does not emit a per-request ``verified=true`` header in verify
    mode.  Consequently this adapter is safe only when the operator guarantees
    all of the following: the listener is in verify mode, ALB-generated mTLS
    headers overwrite/strip caller copies, the backend is unreachable except
    through the listed ALB peers, and the ALB trust store contains the intended
    SPIFFE bundle.  The evidence assurance is therefore ``configured_proxy``.
    """
    domains = _validated_domains_and_proxies(trust_domains, trusted_proxy_addresses)
    if not leaf_header or any(character in leaf_header for character in "\r\n\x00") or max_header_bytes <= 0:
        raise ValueError("leaf_header and a positive max_header_bytes are required")
    return _SpiffeX509HeaderProvider(
        domains[0],
        domains[1],
        leaf_header,
        None,
        "",
        max_header_bytes,
        "aws_alb_mtls_verify",
    )


def nginx_spiffe_provider(
    *,
    trust_domains: Iterable[str],
    trusted_proxy_addresses: Iterable[str],
    certificate_header: str = "X-SSL-Client-Cert",
    verification_header: str = "X-SSL-Client-Verify",
    max_header_bytes: int = 16_384,
) -> PeerIdentityProvider:
    """Consume an nginx-verified X.509-SVID from explicitly trusted peers."""
    return _named_verified_certificate_provider(
        trust_domains=trust_domains,
        trusted_proxy_addresses=trusted_proxy_addresses,
        certificate_header=certificate_header,
        verification_header=verification_header,
        verification_value="SUCCESS",
        evidence_source="nginx_mtls",
        max_header_bytes=max_header_bytes,
    )


def azure_application_gateway_spiffe_provider(
    *,
    trust_domains: Iterable[str],
    trusted_proxy_addresses: Iterable[str],
    certificate_header: str = "X-Client-Certificate",
    verification_header: str = "X-Client-Certificate-Verification",
    max_header_bytes: int = 16_384,
) -> PeerIdentityProvider:
    """Consume strict-mode Azure Application Gateway mTLS server variables.

    Configure a rewrite rule mapping ``client_certificate`` and
    ``client_certificate_verification`` to the two named headers, and ensure
    client-supplied copies are replaced before forwarding.
    """
    return _named_verified_certificate_provider(
        trust_domains=trust_domains,
        trusted_proxy_addresses=trusted_proxy_addresses,
        certificate_header=certificate_header,
        verification_header=verification_header,
        verification_value="SUCCESS",
        evidence_source="azure_application_gateway_mtls_strict",
        max_header_bytes=max_header_bytes,
    )


@dataclass(frozen=True)
class _GcpLoadBalancerSpiffeProvider:
    trust_domains: frozenset[str]
    trusted_proxy_addresses: frozenset[str]
    spiffe_id_header: str
    present_header: str
    chain_verified_header: str
    error_header: str
    provider: str = _SPIFFE_PROVIDER

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        if context.immediate_peer not in self.trusted_proxy_addresses:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.UNTRUSTED_PROXY)
        try:
            present = context.header(self.present_header)
            verified = context.header(self.chain_verified_header)
            spiffe_id = context.header(self.spiffe_id_header)
            error = context.header(self.error_header)
        except PermissionError:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        if present == "false" and verified in (None, "false") and spiffe_id is None:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.NO_MATCH)
        if present != "true" or verified != "true" or error not in (None, "") or not spiffe_id:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        try:
            canonical, trust_domain = validate_spiffe_id(spiffe_id, self.trust_domains)
            identity = PeerIdentity(
                provider=self.provider,
                evidence_source="gcp_load_balancer_mtls",
                assurance=IdentityAssurance.CONFIGURED_PROXY,
                issuer=f"spiffe://{trust_domain}",
                transport="http",
                subject_kind=SubjectKind.WORKLOAD,
                subject_key=canonical,
                subject_stability=SubjectStability.STABLE,
                subject_verified=True,
                attributes={"client_certificate_present": True, "client_certificate_chain_verified": True},
                source_address=context.asserted_peer,
                proxy_address=context.immediate_peer,
            )
        except (TypeError, ValueError):
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        return PeerIdentityResult.available(identity)


def gcp_load_balancer_spiffe_provider(
    *,
    trust_domains: Iterable[str],
    trusted_proxy_addresses: Iterable[str],
    spiffe_id_header: str = "X-Client-Cert-Spiffe-Id",
    present_header: str = "X-Client-Cert-Present",
    chain_verified_header: str = "X-Client-Cert-Chain-Verified",
    error_header: str = "X-Client-Cert-Error",
) -> PeerIdentityProvider:
    """Consume GCP frontend-mTLS custom headers from trusted LB peers."""
    domains, proxies = _validated_domains_and_proxies(trust_domains, trusted_proxy_addresses)
    headers = (spiffe_id_header, present_header, chain_verified_header, error_header)
    if any(not header or any(character in header for character in "\r\n\x00") for header in headers):
        raise ValueError("GCP mTLS header names must be non-empty and contain no controls")
    if len({header.lower() for header in headers}) != len(headers):
        raise ValueError("GCP mTLS header names must be distinct")
    return _GcpLoadBalancerSpiffeProvider(domains, proxies, *headers)


@dataclass(frozen=True)
class _EnvoyXfccSpiffeProvider:
    trust_domains: frozenset[str]
    trusted_proxy_addresses: frozenset[str]
    header: str
    max_header_bytes: int
    provider: str = _SPIFFE_PROVIDER

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        if context.immediate_peer not in self.trusted_proxy_addresses:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.UNTRUSTED_PROXY)
        try:
            raw = context.header(self.header)
        except PermissionError:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        if raw is None:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.NO_MATCH)
        try:
            fields = _parse_single_envoy_xfcc(raw, self.max_header_bytes)
            uris = fields.get("uri", ())
            hashes = fields.get("hash", ())
            if len(uris) != 1 or len(hashes) != 1 or _SHA256_RE.fullmatch(hashes[0]) is None:
                raise ValueError("Envoy XFCC requires exactly one URI and one SHA-256 Hash")
            spiffe_id, trust_domain = validate_spiffe_id(uris[0], self.trust_domains)
            attributes: dict[str, object] = {"certificate_sha256": hashes[0].lower()}
            by = fields.get("by", ())
            if by:
                attributes["proxy_identities"] = by
            identity = PeerIdentity(
                provider=self.provider,
                evidence_source="envoy_xfcc_sanitize_set",
                assurance=IdentityAssurance.CONFIGURED_PROXY,
                issuer=f"spiffe://{trust_domain}",
                transport="http",
                subject_kind=SubjectKind.WORKLOAD,
                subject_key=spiffe_id,
                subject_stability=SubjectStability.STABLE,
                subject_verified=True,
                attributes=attributes,
                source_address=context.asserted_peer,
                proxy_address=context.immediate_peer,
            )
        except (TypeError, ValueError):
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        return PeerIdentityResult.available(identity)


def envoy_xfcc_spiffe_provider(
    *,
    trust_domains: Iterable[str],
    trusted_proxy_addresses: Iterable[str],
    header: str = "X-Forwarded-Client-Cert",
    max_header_bytes: int = 16_384,
) -> PeerIdentityProvider:
    """Consume one strict text-format XFCC element from Envoy SANITIZE_SET.

    The adjacent Envoy must use mTLS, ``forward_client_cert_details``
    ``SANITIZE_SET``, text format, and include URI details. This adapter rejects
    append/forward chains so a client-controlled earlier element can never be
    selected accidentally.
    """
    domains, proxies = _validated_domains_and_proxies(trust_domains, trusted_proxy_addresses)
    if not header or any(character in header for character in "\r\n\x00") or max_header_bytes <= 0:
        raise ValueError("header and a positive max_header_bytes are required")
    return _EnvoyXfccSpiffeProvider(domains, proxies, header, max_header_bytes)


def _parse_single_envoy_xfcc(raw: str, max_header_bytes: int) -> dict[str, tuple[str, ...]]:
    if (
        not raw.isascii()
        or len(raw.encode()) > max_header_bytes
        or any(ord(value) < 0x20 or ord(value) == 0x7F for value in raw)
    ):
        raise ValueError("Envoy XFCC is non-ASCII, contains controls, or is oversized")
    elements = _split_xfcc(raw, ",")
    if len(elements) != 1 or not elements[0].strip():
        raise ValueError("Envoy SANITIZE_SET XFCC must contain exactly one element")
    fields: dict[str, list[str]] = {}
    for raw_pair in _split_xfcc(elements[0], ";"):
        pair = raw_pair.strip()
        if not pair or "=" not in pair:
            raise ValueError("malformed Envoy XFCC field")
        raw_key, raw_value = pair.split("=", 1)
        key = raw_key.strip().lower()
        if _XFCC_KEY_RE.fullmatch(raw_key.strip()) is None or key not in {
            "by",
            "hash",
            "cert",
            "chain",
            "subject",
            "uri",
            "dns",
            "issuer",
        }:
            raise ValueError("unsupported Envoy XFCC field")
        value = _xfcc_value(raw_value.strip())
        if key in ("by", "uri", "cert", "chain"):
            value = _strict_percent_decode(value)
        if key not in ("by", "uri", "dns") and key in fields:
            raise ValueError("duplicate Envoy XFCC singleton field")
        fields.setdefault(key, []).append(value)
    return {key: tuple(values) for key, values in fields.items()}


def _split_xfcc(value: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    escaped = False
    for character in value:
        if escaped:
            if character not in ('"', "\\"):
                raise ValueError("unsupported Envoy XFCC quoted escape")
            current.append(character)
            escaped = False
        elif quoted and character == "\\":
            escaped = True
        elif character == '"':
            quoted = not quoted
            current.append(character)
        elif character == delimiter and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(character)
    if quoted or escaped:
        raise ValueError("unterminated Envoy XFCC quoted value")
    parts.append("".join(current))
    return parts


def _xfcc_value(value: str) -> str:
    if value.startswith('"') or value.endswith('"'):
        if len(value) < 2 or value[0] != '"' or value[-1] != '"':
            raise ValueError("malformed Envoy XFCC quoted value")
        return value[1:-1]
    if any(character in value for character in ",;="):
        raise ValueError("unquoted Envoy XFCC delimiter")
    if not value:
        raise ValueError("empty Envoy XFCC value")
    return value


def _strict_percent_decode(value: str) -> str:
    residue = _PERCENT_ESCAPE_RE.sub("", value)
    if "%" in residue:
        raise ValueError("invalid Envoy XFCC percent escape")
    decoded = unquote(value, encoding="utf-8", errors="strict")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in decoded):
        raise ValueError("decoded Envoy XFCC value contains controls")
    return decoded


def _named_verified_certificate_provider(
    *,
    trust_domains: Iterable[str],
    trusted_proxy_addresses: Iterable[str],
    certificate_header: str,
    verification_header: str,
    verification_value: str,
    evidence_source: str,
    max_header_bytes: int,
) -> PeerIdentityProvider:
    domains, proxies = _validated_domains_and_proxies(trust_domains, trusted_proxy_addresses)
    if (
        not certificate_header
        or not verification_header
        or certificate_header.lower() == verification_header.lower()
        or any(character in certificate_header + verification_header + verification_value for character in "\r\n\x00")
        or max_header_bytes <= 0
    ):
        raise ValueError("distinct certificate/verification headers and a positive size limit are required")
    return _SpiffeX509HeaderProvider(
        domains,
        proxies,
        certificate_header,
        verification_header,
        verification_value,
        max_header_bytes,
        evidence_source,
    )


def _validated_domains_and_proxies(
    trust_domains: Iterable[str], trusted_proxy_addresses: Iterable[str]
) -> tuple[frozenset[str], frozenset[str]]:
    domains = frozenset(trust_domains)
    proxies = frozenset(trusted_proxy_addresses)
    if not domains or not proxies:
        raise ValueError("trust_domains and trusted_proxy_addresses must not be empty")
    for domain in domains:
        if not _TRUST_DOMAIN_RE.fullmatch(domain):
            raise ValueError(f"invalid SPIFFE trust domain: {domain!r}")
    if any(not proxy or any(character in proxy for character in "\r\n\x00") for proxy in proxies):
        raise ValueError("trusted proxy addresses must be exact non-empty values")
    return domains, proxies
