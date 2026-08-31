# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Peer evidence, policy composition, and SPIFFE provider tests."""

from __future__ import annotations

import datetime
import json
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from importlib.resources import files
from typing import Protocol, cast
from urllib.parse import quote

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from vgi_rpc import (
    AuthContext,
    CallContext,
    IdentityAssurance,
    PeerEvidenceSet,
    PeerIdentity,
    PeerIdentityResult,
    PeerIdentityStatus,
    PeerIdentityUnavailableError,
    PeerResolutionContext,
    RpcError,
    RpcServer,
    SubjectKind,
    SubjectStability,
)
from vgi_rpc.http import (
    aws_alb_spiffe_provider,
    azure_application_gateway_spiffe_provider,
    bearer_authenticate_static,
    envoy_xfcc_spiffe_provider,
    gcp_load_balancer_spiffe_provider,
    http_connect,
    make_sync_client,
    nginx_spiffe_provider,
    spiffe_x509_header_provider,
)
from vgi_rpc.http.server._state_token import _compute_aad, _compute_call_aad
from vgi_rpc.rpc import (
    all_of_peer_identities,
    any_of_peer_identities,
    observe_peer_identity,
    peer_identity_primary,
    require_peer_identity,
)


def _identity(provider: str = "spiffe") -> PeerIdentity:
    return PeerIdentity(
        provider=provider,
        evidence_source="test",
        assurance=IdentityAssurance.CRYPTOGRAPHIC_PEER,
        issuer="spiffe://example.org",
        transport="tcp",
        subject_kind=SubjectKind.WORKLOAD,
        subject_key="spiffe://example.org/workload",
        subject_stability=SubjectStability.STABLE,
        subject_verified=True,
    )


def _svid(*uris: str, ca: bool = False) -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.datetime.now(datetime.UTC)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.SubjectAlternativeName([x509.UniformResourceIdentifier(uri) for uri in uris]), False)
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=not ca,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=ca,
                crl_sign=ca,
                encipher_only=False,
                decipher_only=False,
            ),
            True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]),
            False,
        )
        .sign(key, hashes.SHA256())
    )
    return quote(cert.public_bytes(serialization.Encoding.PEM).decode())


def test_evidence_set_rejects_duplicate_provider() -> None:
    """Duplicate results cannot make provider status ambiguous."""
    result = PeerIdentityResult.available(_identity())
    with pytest.raises(ValueError, match="duplicate"):
        PeerEvidenceSet.from_results((result, result))


def test_primary_policy_builds_auth_context() -> None:
    """A primary provider promotes its stable subject into AuthContext."""
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(_identity()),))
    auth = peer_identity_primary("spiffe")(evidence, AuthContext.anonymous())
    assert auth.authenticated
    assert auth.principal == ("peer/spiffe/spiffe%3A%2F%2Fexample.org/spiffe%3A%2F%2Fexample.org%2Fworkload")
    assert auth.claims["subject"] == "spiffe://example.org/workload"
    assert auth.claims["assurance"] == "cryptographic_peer"
    assert auth.claims["peer_evidence_binding"] == "948ce118ddd5f212e7bfd62e13ffdba0675397c56a43060e98656965389e5367"


def test_shared_peer_identity_golden_vector() -> None:
    """The packaged vector pins portable principal and binding behavior."""
    vector = json.loads(files("vgi_rpc.conformance").joinpath("peer_identity_vectors.json").read_text(encoding="utf-8"))
    raw = vector["identity"]
    identity = PeerIdentity(
        provider=raw["provider"],
        evidence_source=raw["evidence_source"],
        assurance=IdentityAssurance(raw["assurance"]),
        issuer=raw["issuer"],
        transport=raw["transport"],
        subject_kind=SubjectKind(raw["subject_kind"]),
        subject_key=raw["subject_key"],
        subject_stability=SubjectStability(raw["subject_stability"]),
        subject_verified=raw["subject_verified"],
        attributes=raw["attributes"],
        capabilities=raw["capabilities"],
        capabilities_verified=raw["capabilities_verified"],
        source_address=raw["source_address"],
        proxy_address=raw["proxy_address"],
    )
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(identity),))
    assert identity.canonical_principal == vector["expected"]["canonical_principal"]
    assert evidence.binding_digest((identity.provider,)) == vector["expected"]["binding_digest"]


def test_binding_ignores_routing_topology_but_not_authorization_evidence() -> None:
    """Trusted proxy replicas and new source ports do not invalidate state."""
    first = replace(_identity(), source_address="100.64.0.1:40001", proxy_address="10.0.0.10")
    second = replace(_identity(), source_address="100.64.0.1:49999", proxy_address="10.0.0.11")
    first_evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(first),))
    second_evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(second),))
    assert first_evidence.binding_digest((first.provider,)) == second_evidence.binding_digest((second.provider,))

    changed = replace(second, capabilities={"query.farm/run": ({"queue": "red"},)})
    changed_evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(changed),))
    assert first_evidence.binding_digest((first.provider,)) != changed_evidence.binding_digest((changed.provider,))


def test_canonical_principal_namespaces_issuer() -> None:
    """Equal provider subjects from different issuers cannot collide."""
    first = _identity()
    second = PeerIdentity(
        provider=first.provider,
        evidence_source=first.evidence_source,
        assurance=first.assurance,
        issuer="spiffe://other.example",
        transport=first.transport,
        subject_kind=first.subject_kind,
        subject_key=first.subject_key,
        subject_stability=first.subject_stability,
        subject_verified=True,
    )
    assert first.canonical_principal != second.canonical_principal


def test_evidence_is_deeply_immutable() -> None:
    """Nested capability values are snapshotted into immutable tuples/maps."""
    source = {"roles": ["reader", {"scope": "one"}]}
    identity = PeerIdentity(
        provider="test",
        evidence_source="test",
        assurance=IdentityAssurance.LOCAL_DAEMON,
        issuer="test://issuer",
        transport="tcp",
        capabilities=source,
    )
    source["roles"].append("writer")
    assert identity.capabilities["roles"] == ("reader", {"scope": "one"})


def test_all_of_requires_application_auth_and_binds_every_factor() -> None:
    """All-of includes application auth and every required peer factor."""
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(_identity()),))
    policy = all_of_peer_identities("spiffe", identity_linker=lambda _auth, _identities: None)
    with pytest.raises(PermissionError):
        policy(evidence, AuthContext.anonymous())
    auth = policy(evidence, AuthContext(domain="bearer", authenticated=True, principal="alice"))
    assert auth.claims["application_principal"] == "alice"
    assert auth.claims["application_domain"] == "bearer"
    assert len(auth.claims["peer_evidence_binding"]) == 64


def test_all_of_rejects_identity_link_conflicts() -> None:
    """All-of cannot silently compose identities the application says conflict."""
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(_identity()),))

    def reject(_auth: AuthContext, _identities: object) -> None:
        raise PermissionError("identity conflict")

    policy = all_of_peer_identities("spiffe", identity_linker=reject)
    with pytest.raises(PermissionError, match="conflict"):
        policy(evidence, AuthContext(domain="bearer", authenticated=True, principal="alice"))


def test_all_of_binds_application_principal_into_state_tokens() -> None:
    """Two users on one peer cannot replay cursor, call, or session state."""
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(_identity()),))
    policy = all_of_peer_identities("spiffe", identity_linker=lambda _auth, _identities: None)
    alice = policy(evidence, AuthContext(domain="bearer", authenticated=True, principal="alice"))
    bob = policy(evidence, AuthContext(domain="bearer", authenticated=True, principal="bob"))
    assert _compute_aad(alice) != _compute_aad(bob)
    assert _compute_call_aad(alice) != _compute_call_aad(bob)


def test_all_of_requires_an_identity_linker() -> None:
    """Construction fails closed when no cross-authority mapping is supplied."""
    with pytest.raises(ValueError, match="identity_linker"):
        all_of_peer_identities("spiffe")


def test_required_peer_evidence_binds_anonymous_state_tokens() -> None:
    """Required peer identity binds every state token without application auth."""
    first = PeerEvidenceSet.from_results((PeerIdentityResult.available(_identity()),))
    changed = replace(_identity(), issuer="spiffe://other.example")
    second = PeerEvidenceSet.from_results((PeerIdentityResult.available(changed),))
    policy = require_peer_identity("spiffe")
    first_auth = policy(first, AuthContext.anonymous())
    second_auth = policy(second, AuthContext.anonymous())
    assert _compute_aad(first_auth) != _compute_aad(second_auth)
    assert _compute_call_aad(first_auth) != _compute_call_aad(second_auth)


def test_required_peer_evidence_accepts_capability_only_evidence() -> None:
    """Require validates evidence; only primary needs a stable subject."""
    capability_only = replace(
        _identity(),
        subject_key=None,
        subject_stability=SubjectStability.NONE,
        subject_verified=False,
        capabilities={"query.farm/can-run": [{"worker": "analytics"}]},
        capabilities_verified=True,
    )
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(capability_only),))
    auth = AuthContext(domain="bearer", authenticated=True, principal="alice")
    required = require_peer_identity("spiffe")(evidence, auth)
    assert required.authenticated
    assert required.principal == "alice"
    with pytest.raises(PermissionError, match="stable subject"):
        peer_identity_primary("spiffe")(evidence, AuthContext.anonymous())


def test_resolution_context_rejects_duplicate_and_unsafe_headers() -> None:
    """Identity header ambiguity and control characters fail at the adapter boundary."""
    context = PeerResolutionContext(transport="http", headers={"X-ID": ("one", "two")})
    with pytest.raises(PermissionError, match="duplicate"):
        context.header("x-id")
    with pytest.raises(PermissionError, match="case-varied"):
        PeerResolutionContext(transport="http", headers={"X-ID": ("one",), "x-id": ("two",)})
    with pytest.raises(ValueError, match="header value"):
        PeerResolutionContext(transport="http", headers={"X-ID": ("one\r\ntwo",)})


def test_any_of_fails_closed_when_provider_is_unavailable() -> None:
    """An outage is transient when no other mechanism authenticated."""
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult("spiffe", PeerIdentityStatus.UNAVAILABLE),))
    with pytest.raises(PeerIdentityUnavailableError):
        any_of_peer_identities("spiffe")(evidence, AuthContext.anonymous())
    auth = AuthContext(domain="bearer", authenticated=True, principal="alice")
    assert any_of_peer_identities("spiffe")(evidence, auth) is auth


def test_any_of_rejects_ambiguous_available_evidence_before_fallback() -> None:
    """Multiple stable subjects are invalid even when bearer auth succeeded."""
    first = _identity()
    second = replace(_identity(), subject_key="spiffe://example.org/other")
    evidence = PeerEvidenceSet.from_results(
        (PeerIdentityResult("spiffe", PeerIdentityStatus.AVAILABLE, (first, second)),)
    )
    with pytest.raises(PermissionError, match="ambiguous"):
        any_of_peer_identities("spiffe")(
            evidence,
            AuthContext(domain="bearer", authenticated=True, principal="alice"),
        )


def test_any_of_skips_unavailable_provider_when_later_provider_is_usable() -> None:
    """Alternative providers are evaluated independent of declaration order."""
    second = replace(_identity(), provider="second")
    evidence = PeerEvidenceSet.from_results(
        (
            PeerIdentityResult("first", PeerIdentityStatus.UNAVAILABLE),
            PeerIdentityResult.available(second),
        )
    )
    auth = any_of_peer_identities("first", "second")(evidence, AuthContext.anonymous())
    assert auth.authenticated
    assert auth.domain == "second"


def test_any_of_skips_subjectless_available_provider_when_later_provider_is_usable() -> None:
    """Capability-only evidence cannot block a later authenticatable identity."""
    subjectless = replace(
        _identity(provider="first"),
        subject_kind=SubjectKind.UNKNOWN,
        subject_key=None,
        subject_stability=SubjectStability.NONE,
        subject_verified=False,
    )
    second = replace(_identity(), provider="second")
    evidence = PeerEvidenceSet.from_results(
        (
            PeerIdentityResult.available(subjectless),
            PeerIdentityResult.available(second),
        )
    )
    auth = any_of_peer_identities("first", "second")(evidence, AuthContext.anonymous())
    assert auth.authenticated
    assert auth.domain == "second"


class _PeerService(Protocol):
    def whoami(self) -> str: ...


class _PeerImpl:
    def whoami(self, ctx: CallContext) -> str:
        status = ctx.peer_evidence.provider_status["spiffe"]
        return f"{status}:{ctx.auth.principal}"


def test_spiffe_provider_populates_call_context_and_authenticates() -> None:
    """Verified SVID evidence reaches the worker and can authenticate it."""
    provider = spiffe_x509_header_provider(
        trust_domains={"example.org"},
        trusted_proxy_addresses={"127.0.0.1"},
        chain_verified_header="X-Client-Cert-Verified",
    )
    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        peer_identity_providers=(provider,),
        peer_authentication_policy=peer_identity_primary("spiffe"),
        default_headers={
            "X-SSL-Client-Cert": _svid("spiffe://example.org/ns/default/sa/worker"),
            "X-Client-Cert-Verified": "true",
        },
    )
    with http_connect(_PeerService, client=client) as service:
        assert service.whoami() == (
            "available:peer/spiffe/spiffe%3A%2F%2Fexample.org/spiffe%3A%2F%2Fexample.org%2Fns%2Fdefault%2Fsa%2Fworker"
        )


@pytest.mark.parametrize(
    "case",
    ["wrong-trust-domain", "multiple-uri-sans", "ca-leaf"],
)
def test_spiffe_provider_rejects_invalid_svid_profiles(case: str) -> None:
    """Trust-domain, URI-count, and leaf constraints fail closed."""
    if case == "wrong-trust-domain":
        certificate = _svid("spiffe://other.org/workload")
    elif case == "multiple-uri-sans":
        certificate = _svid("spiffe://example.org/a", "spiffe://example.org/b")
    else:
        certificate = _svid("spiffe://example.org/workload", ca=True)
    provider = spiffe_x509_header_provider(
        trust_domains={"example.org"},
        trusted_proxy_addresses={"127.0.0.1"},
        chain_verified_header="X-Client-Cert-Verified",
    )
    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        peer_identity_providers=(provider,),
        peer_authentication_policy=peer_identity_primary("spiffe"),
        default_headers={"X-SSL-Client-Cert": certificate, "X-Client-Cert-Verified": "true"},
    )
    with http_connect(_PeerService, client=client) as service, pytest.raises(RpcError):
        service.whoami()


def test_spiffe_provider_reports_untrusted_proxy() -> None:
    """Headers from an untrusted immediate peer are never accepted."""
    provider = spiffe_x509_header_provider(
        trust_domains={"example.org"},
        trusted_proxy_addresses={"192.0.2.1"},
        chain_verified_header="X-Client-Cert-Verified",
    )
    result = provider(PeerResolutionContext(transport="http", immediate_peer="127.0.0.1"))
    assert result.status is PeerIdentityStatus.UNTRUSTED_PROXY


def test_spiffe_provider_requires_positive_chain_verification() -> None:
    """A trusted delivery peer alone is insufficient to verify an SVID."""
    provider = spiffe_x509_header_provider(
        trust_domains={"example.org"},
        trusted_proxy_addresses={"127.0.0.1"},
        chain_verified_header="X-Client-Cert-Verified",
    )
    context = PeerResolutionContext(
        transport="http",
        immediate_peer="127.0.0.1",
        headers={"X-SSL-Client-Cert": (_svid("spiffe://example.org/workload"),)},
    )
    assert provider(context).status is PeerIdentityStatus.INVALID


@pytest.mark.parametrize(
    ("factory", "certificate_header", "verification_header", "verification_value", "evidence_source"),
    [
        (nginx_spiffe_provider, "X-SSL-Client-Cert", "X-SSL-Client-Verify", "SUCCESS", "nginx_mtls"),
        (
            azure_application_gateway_spiffe_provider,
            "X-Client-Certificate",
            "X-Client-Certificate-Verification",
            "SUCCESS",
            "azure_application_gateway_mtls_strict",
        ),
    ],
)
def test_named_proxy_spiffe_providers_require_verified_chain(
    factory: Callable[..., object],
    certificate_header: str,
    verification_header: str,
    verification_value: str,
    evidence_source: str,
) -> None:
    """Nginx and Azure wrappers retain the explicit per-request verification gate."""
    provider = cast(
        "Callable[[PeerResolutionContext], PeerIdentityResult]",
        factory(trust_domains={"example.org"}, trusted_proxy_addresses={"proxy"}),
    )
    base_headers = {certificate_header: (_svid("spiffe://example.org/workload"),)}
    missing = provider(PeerResolutionContext(transport="http", immediate_peer="proxy", headers=base_headers))
    assert missing.status is PeerIdentityStatus.INVALID
    available = provider(
        PeerResolutionContext(
            transport="http",
            immediate_peer="proxy",
            headers={**base_headers, verification_header: (verification_value,)},
        )
    )
    assert available.status is PeerIdentityStatus.AVAILABLE
    assert available.identities[0].evidence_source == evidence_source


def test_aws_alb_spiffe_provider_models_verify_mode_as_configured_proxy() -> None:
    """AWS verify mode is an operator-configured boundary, not direct verification."""
    provider = aws_alb_spiffe_provider(trust_domains={"example.org"}, trusted_proxy_addresses={"10.0.0.8"})
    context = PeerResolutionContext(
        transport="http",
        immediate_peer="10.0.0.8",
        headers={"X-Amzn-Mtls-Clientcert-Leaf": (_svid("spiffe://example.org/workload"),)},
    )
    result = provider(context)
    assert result.status is PeerIdentityStatus.AVAILABLE
    assert result.identities[0].evidence_source == "aws_alb_mtls_verify"
    assert result.identities[0].assurance is IdentityAssurance.CONFIGURED_PROXY
    assert provider(replace(context, immediate_peer="10.0.0.9")).status is PeerIdentityStatus.UNTRUSTED_PROXY


def test_gcp_load_balancer_spiffe_provider_requires_all_validation_signals() -> None:
    """GCP custom headers authenticate only a present, chain-verified SVID."""
    provider = gcp_load_balancer_spiffe_provider(trust_domains={"example.org"}, trusted_proxy_addresses={"proxy"})
    headers = {
        "X-Client-Cert-Present": ("true",),
        "X-Client-Cert-Chain-Verified": ("true",),
        "X-Client-Cert-Spiffe-Id": ("spiffe://example.org/ns/default/sa/client",),
    }
    available = provider(PeerResolutionContext(transport="http", immediate_peer="proxy", headers=headers))
    assert available.status is PeerIdentityStatus.AVAILABLE
    assert available.identities[0].subject_key == "spiffe://example.org/ns/default/sa/client"
    assert available.identities[0].evidence_source == "gcp_load_balancer_mtls"

    invalid = provider(
        PeerResolutionContext(
            transport="http",
            immediate_peer="proxy",
            headers={**headers, "X-Client-Cert-Chain-Verified": ("false",)},
        )
    )
    assert invalid.status is PeerIdentityStatus.INVALID
    no_certificate = provider(
        PeerResolutionContext(
            transport="http",
            immediate_peer="proxy",
            headers={"X-Client-Cert-Present": ("false",)},
        )
    )
    assert no_certificate.status is PeerIdentityStatus.NO_MATCH


def test_envoy_xfcc_spiffe_provider_requires_sanitize_set_single_element() -> None:
    """Strict XFCC accepts one verified client element and rejects forwarded chains."""
    provider = envoy_xfcc_spiffe_provider(trust_domains={"example.org"}, trusted_proxy_addresses={"127.0.0.1"})
    valid = "By=spiffe://mesh.example/proxy;Hash=" + "a" * 64 + ';URI="spiffe://example.org/ns/default/sa/client"'
    result = provider(
        PeerResolutionContext(
            transport="http",
            immediate_peer="127.0.0.1",
            headers={"X-Forwarded-Client-Cert": (valid,)},
        )
    )
    assert result.status is PeerIdentityStatus.AVAILABLE
    assert result.identities[0].subject_key == "spiffe://example.org/ns/default/sa/client"
    assert result.identities[0].evidence_source == "envoy_xfcc_sanitize_set"

    appended = valid + ",By=spiffe://mesh.example/second;Hash=" + "b" * 64 + ";URI=spiffe://example.org/other"
    assert (
        provider(
            PeerResolutionContext(
                transport="http",
                immediate_peer="127.0.0.1",
                headers={"X-Forwarded-Client-Cert": (appended,)},
            )
        ).status
        is PeerIdentityStatus.INVALID
    )


@pytest.mark.parametrize(
    "header",
    [
        "URI=spiffe://example.org/client",  # no hash
        "Hash=abc;URI=spiffe://example.org/client",
        "Hash=" + "a" * 64 + ";URI=spiffe://example.org/one;URI=spiffe://example.org/two",
        "Hash=" + "a" * 64 + ";Hash=" + "b" * 64 + ";URI=spiffe://example.org/client",
        "Hash=" + "a" * 64 + ";URI=spiffe://other.org/client",
        "Hash=" + "a" * 64 + ";URI=spiffe://example.org/client%ZZ",
        "Hash=" + "a" * 64 + ';URI="spiffe://example.org/client',
        "Unknown=value;Hash=" + "a" * 64 + ";URI=spiffe://example.org/client",
    ],
)
def test_envoy_xfcc_spiffe_provider_rejects_ambiguous_or_invalid_fields(header: str) -> None:
    """Malformed, duplicate, unknown, and out-of-domain XFCC fields fail closed."""
    provider = envoy_xfcc_spiffe_provider(trust_domains={"example.org"}, trusted_proxy_addresses={"proxy"})
    result = provider(
        PeerResolutionContext(
            transport="http",
            immediate_peer="proxy",
            headers={"X-Forwarded-Client-Cert": (header,)},
        )
    )
    assert result.status is PeerIdentityStatus.INVALID


@pytest.mark.parametrize(
    "case",
    ["oversized", "combined", "multiple"],
)
def test_spiffe_provider_bounds_and_rejects_ambiguous_certificate_headers(case: str) -> None:
    """Oversized, combined, and multi-certificate header values fail closed."""
    if case == "oversized":
        certificate, max_header_bytes = "x" * 128, 64
    elif case == "combined":
        certificate, max_header_bytes = _svid("spiffe://example.org/workload") + ",duplicate", 16_384
    else:
        certificate = _svid("spiffe://example.org/workload") + quote("\n-----BEGIN CERTIFICATE-----\nextra")
        max_header_bytes = 16_384
    provider = spiffe_x509_header_provider(
        trust_domains={"example.org"},
        trusted_proxy_addresses={"127.0.0.1"},
        chain_verified_header="X-Client-Cert-Verified",
        max_header_bytes=max_header_bytes,
    )
    context = PeerResolutionContext(
        transport="http",
        immediate_peer="127.0.0.1",
        headers={
            "X-SSL-Client-Cert": (certificate,),
            "X-Client-Cert-Verified": ("true",),
        },
    )
    assert provider(context).status is PeerIdentityStatus.INVALID


def test_any_of_accepts_spiffe_when_bearer_is_missing_but_rejects_invalid_bearer() -> None:
    """Missing credentials may fall through; presented invalid credentials may not."""
    provider = spiffe_x509_header_provider(
        trust_domains={"example.org"},
        trusted_proxy_addresses={"127.0.0.1"},
        chain_verified_header="X-Client-Cert-Verified",
    )
    headers = {
        "X-SSL-Client-Cert": _svid("spiffe://example.org/workload"),
        "X-Client-Cert-Verified": "true",
    }
    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        authenticate=bearer_authenticate_static(
            tokens={"valid": AuthContext(domain="bearer", authenticated=True, principal="alice")}
        ),
        peer_identity_providers=(provider,),
        peer_authentication_policy=any_of_peer_identities("spiffe"),
        default_headers=headers,
    )
    with http_connect(_PeerService, client=client) as service:
        assert "peer/spiffe/" in service.whoami()
    client._default_headers["Authorization"] = "Bearer invalid"
    with http_connect(_PeerService, client=client) as service, pytest.raises(RpcError):
        service.whoami()


def test_observation_policy_cannot_bypass_required_bearer_authentication() -> None:
    """Observation alone must not consume an authenticator's missing-credential failure."""
    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        authenticate=bearer_authenticate_static(
            tokens={"valid": AuthContext(domain="bearer", authenticated=True, principal="alice")}
        ),
        peer_authentication_policy=observe_peer_identity,
    )
    with http_connect(_PeerService, client=client) as service, pytest.raises(RpcError):
        service.whoami()


@pytest.mark.parametrize(
    "spiffe_id",
    [
        "spiffe://example.org/a%2Fb",
        "spiffe://example.org/a//b",
        "spiffe://example.org/a/../b",
        "spiffe://example.org/a/",
        "spiffe://example.org/a:b",
    ],
)
def test_spiffe_provider_rejects_noncanonical_paths(spiffe_id: str) -> None:
    """Noncanonical paths cannot create cross-implementation aliases."""
    provider = spiffe_x509_header_provider(
        trust_domains={"example.org"},
        trusted_proxy_addresses={"127.0.0.1"},
        chain_verified_header="X-Client-Cert-Verified",
    )
    context = PeerResolutionContext(
        transport="http",
        immediate_peer="127.0.0.1",
        headers={
            "X-SSL-Client-Cert": (_svid(spiffe_id),),
            "X-Client-Cert-Verified": ("true",),
        },
    )
    assert provider(context).status is PeerIdentityStatus.INVALID


class _CallbackPeerProvider:
    def __init__(self, provider: str, callback: Callable[[PeerResolutionContext], PeerIdentityResult]) -> None:
        self.provider = provider
        self._callback = callback

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        return self._callback(context)


def test_http_resolution_separates_proxy_ip_from_whois_socket_endpoint() -> None:
    """HTTP proxy trust receives an IP while LocalAPI keeps the source port."""
    snapshots: list[PeerResolutionContext] = []

    def capture(context: PeerResolutionContext) -> PeerIdentityResult:
        snapshots.append(context)
        return PeerIdentityResult("capture", PeerIdentityStatus.NO_MATCH)

    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        peer_identity_providers=(_CallbackPeerProvider("capture", capture),),
    )
    response = client.get("/")
    assert response.status_code == 200
    assert len(snapshots) == 1
    assert snapshots[0].immediate_peer == "127.0.0.1"
    assert snapshots[0].source_endpoint is not None
    source_host, source_port = snapshots[0].source_endpoint.rsplit(":", 1)
    assert source_host == "127.0.0.1"
    assert int(source_port) > 0


def test_http_peer_providers_run_concurrently_with_one_deadline() -> None:
    """Independent providers start together and observe one total deadline."""
    barrier = threading.Barrier(2)
    deadlines: list[float | None] = []

    # Give each callback a stable result name independent of completion order.
    def provider(name: str) -> _CallbackPeerProvider:
        def named(context: PeerResolutionContext) -> PeerIdentityResult:
            deadlines.append(context.deadline)
            barrier.wait(timeout=0.5)
            return PeerIdentityResult(name, PeerIdentityStatus.NO_MATCH)

        return _CallbackPeerProvider(name, named)

    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        peer_identity_providers=(provider("first"), provider("second")),
        peer_resolution_timeout=1.0,
    )
    response = client.get("/")
    assert response.status_code == 200
    assert len(deadlines) == 2 and deadlines[0] == deadlines[1]


def test_http_noncooperative_peer_provider_has_hard_bounded_capacity() -> None:
    """Unavailable evidence is policy input while hung callbacks stay bounded."""
    release = threading.Event()

    def blocked(context: PeerResolutionContext) -> PeerIdentityResult:
        del context
        release.wait(timeout=5)
        return PeerIdentityResult("blocked", PeerIdentityStatus.NO_MATCH)

    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        peer_identity_providers=(_CallbackPeerProvider("blocked", blocked),),
        peer_resolution_timeout=0.05,
        peer_provider_concurrency=1,
    )
    started = time.monotonic()
    first = client.get("/")
    elapsed = time.monotonic() - started
    second = client.get("/")
    assert first.status_code == 200
    assert second.status_code == 200
    assert elapsed < 0.5

    any_of = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        authenticate=lambda _request: AuthContext("bearer", True, "alice"),
        peer_identity_providers=(_CallbackPeerProvider("blocked", blocked),),
        peer_authentication_policy=any_of_peer_identities("blocked"),
        peer_resolution_timeout=0.05,
        peer_provider_concurrency=1,
    )
    assert any_of.get("/").status_code == 200

    required = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        peer_identity_providers=(_CallbackPeerProvider("blocked", blocked),),
        peer_authentication_policy=require_peer_identity("blocked"),
        peer_resolution_timeout=0.05,
        peer_provider_concurrency=1,
    )
    assert required.get("/").status_code == 503
    release.set()


def test_http_completed_invalid_evidence_is_not_downgraded_behind_slow_provider() -> None:
    """A completed rejection wins even when another provider consumes the budget."""
    release = threading.Event()
    invalid_done = threading.Event()

    def slow(context: PeerResolutionContext) -> PeerIdentityResult:
        del context
        invalid_done.wait(timeout=1)
        release.wait(timeout=5)
        return PeerIdentityResult("slow", PeerIdentityStatus.NO_MATCH)

    def invalid(context: PeerResolutionContext) -> PeerIdentityResult:
        del context
        invalid_done.set()
        return PeerIdentityResult("invalid", PeerIdentityStatus.INVALID)

    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        authenticate=lambda _request: AuthContext("bearer", True, "alice"),
        peer_identity_providers=(
            _CallbackPeerProvider("slow", slow),
            _CallbackPeerProvider("invalid", invalid),
        ),
        peer_authentication_policy=any_of_peer_identities("slow", "invalid"),
        peer_resolution_timeout=0.05,
        peer_provider_concurrency=2,
    )
    try:
        assert client.get("/").status_code == 401
    finally:
        release.set()


def test_http_peer_provider_exception_text_is_not_exported(caplog: pytest.LogCaptureFixture) -> None:
    """Extension-provider secrets never reach the response or auth log fields."""
    secret = "raw-capability-and-token-secret"

    def rejected(context: PeerResolutionContext) -> PeerIdentityResult:
        del context
        raise PermissionError(secret)

    client = make_sync_client(
        RpcServer(_PeerService, _PeerImpl()),
        token_key=b"k",
        peer_identity_providers=(_CallbackPeerProvider("rejected", rejected),),
    )
    with caplog.at_level(logging.WARNING, logger="vgi_rpc.http"):
        response = client.get("/")
    assert response.status_code == 401
    assert secret.encode() not in response.content
    assert all(secret not in record.getMessage() for record in caplog.records)
    assert all(getattr(record, "auth_error", None) != secret for record in caplog.records)


def test_peer_resolution_header_multiplicity_is_preserved_and_rejected() -> None:
    """The provider boundary never silently picks one duplicate header value."""
    context = PeerResolutionContext(transport="http", headers={"X-Identity": ("one", "two")})
    with pytest.raises(PermissionError, match="duplicate identity header"):
        context.header("x-identity")


@pytest.mark.parametrize(
    ("factory", "expected"),
    [
        (
            lambda: PeerIdentityResult("spiffe", cast("PeerIdentityStatus", "future_status")),
            "status must be a PeerIdentityStatus",
        ),
        (
            lambda: replace(_identity(), assurance=cast("IdentityAssurance", "future_assurance")),
            "assurance must be an IdentityAssurance",
        ),
        (
            lambda: replace(_identity(), subject_kind=cast("SubjectKind", "future_kind")),
            "subject_kind must be a SubjectKind",
        ),
    ],
)
def test_peer_identity_rejects_unknown_enum_values(factory: Callable[[], object], expected: str) -> None:
    """Unknown wire-like enum values fail at construction, not later during policy use."""
    with pytest.raises(TypeError, match=expected):
        factory()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: replace(_identity(), subject_key="bad\ud800subject"),
        lambda: replace(_identity(), attributes={"bad\ud800key": True}),
        lambda: replace(_identity(), capabilities={"nested": ["bad\ud800value"]}),
        lambda: PeerResolutionContext(transport="http", headers={"X-Test": ("bad\ud800value",)}),
    ],
)
def test_peer_identity_rejects_non_scalar_unicode(factory: Callable[[], object]) -> None:
    """Unpaired surrogates cannot create divergent principals or JSON digests."""
    with pytest.raises(ValueError, match="unpaired surrogate"):
        factory()


def test_peer_identity_rejects_nonfinite_deadline_and_json_number() -> None:
    """Deadlines and JSON numbers reject non-finite values."""
    with pytest.raises(ValueError, match="finite monotonic"):
        PeerResolutionContext(transport="http", deadline=float("inf"))
    with pytest.raises(TypeError, match="finite"):
        replace(_identity(), attributes={"score": float("nan")})
