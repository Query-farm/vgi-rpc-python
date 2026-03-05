# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for mTLS authentication factories and XFCC parser."""

from __future__ import annotations

import datetime
from typing import Protocol
from urllib.parse import quote

import falcon
import falcon.testing.helpers
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from vgi_rpc import AuthContext, CallContext, RpcServer
from vgi_rpc.http import (
    bearer_authenticate_static,
    chain_authenticate,
    http_connect,
    make_sync_client,
)
from vgi_rpc.http._mtls import (
    XfccElement,
    _parse_xfcc,
    mtls_authenticate,
    mtls_authenticate_fingerprint,
    mtls_authenticate_subject,
    mtls_authenticate_xfcc,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class _IdentityService(Protocol):
    def whoami(self) -> str: ...


class _IdentityImpl:
    def whoami(self, ctx: CallContext) -> str:
        ctx.auth.require_authenticated()
        return f"{ctx.auth.domain}:{ctx.auth.principal}"


def _make_test_cert(
    cn: str = "test-client",
    *,
    days_valid: int = 365,
    not_before_offset: datetime.timedelta | None = None,
) -> tuple[x509.Certificate, rsa.RSAPrivateKey]:
    """Generate a self-signed certificate for testing."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.datetime.now(datetime.UTC)
    not_before = now + not_before_offset if not_before_offset else now - datetime.timedelta(hours=1)
    not_after = not_before + datetime.timedelta(days=days_valid)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .sign(key, hashes.SHA256())
    )
    return cert, key


def _cert_to_header(cert: x509.Certificate) -> str:
    """PEM-encode and URL-encode a certificate for header injection."""
    pem = cert.public_bytes(serialization.Encoding.PEM).decode()
    return quote(pem)


def _make_req(
    *,
    cert_header: str | None = None,
    header_name: str = "X-SSL-Client-Cert",
    xfcc: str | None = None,
    authorization: str | None = None,
) -> falcon.Request:
    """Create a Falcon test request with optional mTLS headers."""
    headers: dict[str, str] = {}
    if cert_header is not None:
        headers[header_name] = cert_header
    if xfcc is not None:
        headers["x-forwarded-client-cert"] = xfcc
    if authorization is not None:
        headers["Authorization"] = authorization
    return falcon.testing.helpers.create_req(headers=headers)


# ---------------------------------------------------------------------------
# TestMtlsAuthenticate
# ---------------------------------------------------------------------------


class TestMtlsAuthenticate:
    """Tests for mtls_authenticate factory."""

    def test_valid_cert(self) -> None:
        """A valid certificate calls validate and returns its AuthContext."""
        cert, _key = _make_test_cert("alice")

        def validate(c: x509.Certificate) -> AuthContext:
            cn_attrs = c.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            return AuthContext(domain="mtls", authenticated=True, principal=str(cn_attrs[0].value), claims={})

        auth_fn = mtls_authenticate(validate=validate)
        req = _make_req(cert_header=_cert_to_header(cert))
        auth = auth_fn(req)
        assert auth.authenticated is True
        assert auth.principal == "alice"

    def test_invalid_pem_raises(self) -> None:
        """Non-PEM header value raises ValueError."""
        _noop_ctx = AuthContext(domain="", authenticated=True, principal="", claims={})

        def noop(c: x509.Certificate) -> AuthContext:
            return _noop_ctx

        auth_fn = mtls_authenticate(validate=noop)
        req = _make_req(cert_header=quote("not a certificate"))
        with pytest.raises(ValueError, match="not a PEM certificate"):
            auth_fn(req)

    def test_missing_header_raises(self) -> None:
        """Missing certificate header raises ValueError."""
        _noop_ctx = AuthContext(domain="", authenticated=True, principal="", claims={})

        def noop(c: x509.Certificate) -> AuthContext:
            return _noop_ctx

        auth_fn = mtls_authenticate(validate=noop)
        req = _make_req()
        with pytest.raises(ValueError, match="Missing"):
            auth_fn(req)

    def test_custom_header_name(self) -> None:
        """Custom header name is respected."""
        cert, _key = _make_test_cert("bob")

        def validate(c: x509.Certificate) -> AuthContext:
            return AuthContext(domain="mtls", authenticated=True, principal="bob", claims={})

        auth_fn = mtls_authenticate(validate=validate, header="X-Amzn-Mtls-Clientcert")
        req = _make_req(cert_header=_cert_to_header(cert), header_name="X-Amzn-Mtls-Clientcert")
        auth = auth_fn(req)
        assert auth.principal == "bob"

    def test_validate_rejection(self) -> None:
        """Validate callback can reject a certificate."""
        cert, _key = _make_test_cert("evil")

        def validate(c: x509.Certificate) -> AuthContext:
            raise ValueError("certificate revoked")

        auth_fn = mtls_authenticate(validate=validate)
        req = _make_req(cert_header=_cert_to_header(cert))
        with pytest.raises(ValueError, match="certificate revoked"):
            auth_fn(req)

    def test_check_expiry_expired(self) -> None:
        """Expired certificate is rejected when check_expiry=True."""
        cert, _key = _make_test_cert("expired", days_valid=0, not_before_offset=datetime.timedelta(days=-2))

        def validate(c: x509.Certificate) -> AuthContext:
            return AuthContext(domain="mtls", authenticated=True, principal="x", claims={})

        auth_fn = mtls_authenticate(validate=validate, check_expiry=True)
        req = _make_req(cert_header=_cert_to_header(cert))
        with pytest.raises(ValueError, match="expired"):
            auth_fn(req)

    def test_check_expiry_not_yet_valid(self) -> None:
        """Future certificate is rejected when check_expiry=True."""
        cert, _key = _make_test_cert("future", not_before_offset=datetime.timedelta(days=30))

        def validate(c: x509.Certificate) -> AuthContext:
            return AuthContext(domain="mtls", authenticated=True, principal="x", claims={})

        auth_fn = mtls_authenticate(validate=validate, check_expiry=True)
        req = _make_req(cert_header=_cert_to_header(cert))
        with pytest.raises(ValueError, match="not yet valid"):
            auth_fn(req)

    def test_end_to_end_rpc(self) -> None:
        """Full round-trip: mTLS auth -> RPC call -> identity returned."""
        cert, _key = _make_test_cert("rpc-client")

        def validate(c: x509.Certificate) -> AuthContext:
            cn_attrs = c.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            return AuthContext(domain="mtls", authenticated=True, principal=str(cn_attrs[0].value), claims={})

        auth_fn = mtls_authenticate(validate=validate)
        server = RpcServer(_IdentityService, _IdentityImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=auth_fn,
            default_headers={"X-SSL-Client-Cert": _cert_to_header(cert)},
        )
        with http_connect(_IdentityService, client=client) as svc:
            result = svc.whoami()
            assert result == "mtls:rpc-client"


# ---------------------------------------------------------------------------
# TestMtlsAuthenticateFingerprint
# ---------------------------------------------------------------------------


class TestMtlsAuthenticateFingerprint:
    """Tests for mtls_authenticate_fingerprint convenience factory."""

    def test_known_fingerprint(self) -> None:
        """A known fingerprint returns the mapped AuthContext."""
        cert, _key = _make_test_cert("known")
        fp = cert.fingerprint(hashes.SHA256()).hex()
        ctx = AuthContext(domain="mtls", authenticated=True, principal="known-client", claims={})
        auth_fn = mtls_authenticate_fingerprint(fingerprints={fp: ctx})
        req = _make_req(cert_header=_cert_to_header(cert))
        auth = auth_fn(req)
        assert auth.principal == "known-client"

    def test_unknown_fingerprint_raises(self) -> None:
        """An unknown fingerprint raises ValueError."""
        cert, _key = _make_test_cert("unknown")
        ctx = AuthContext(domain="mtls", authenticated=True, principal="x", claims={})
        auth_fn = mtls_authenticate_fingerprint(fingerprints={"deadbeef": ctx})
        req = _make_req(cert_header=_cert_to_header(cert))
        with pytest.raises(ValueError, match="Unknown certificate fingerprint"):
            auth_fn(req)

    def test_custom_algorithm_sha1(self) -> None:
        """SHA-1 algorithm works for fingerprinting."""
        cert, _key = _make_test_cert("sha1-client")
        fp = cert.fingerprint(hashes.SHA1()).hex()
        ctx = AuthContext(domain="mtls", authenticated=True, principal="sha1-ok", claims={})
        auth_fn = mtls_authenticate_fingerprint(fingerprints={fp: ctx}, algorithm="sha1")
        req = _make_req(cert_header=_cert_to_header(cert))
        auth = auth_fn(req)
        assert auth.principal == "sha1-ok"

    def test_normalized_lowercase_hex(self) -> None:
        """Fingerprint is always lowercase hex without colons."""
        cert, _key = _make_test_cert("norm")
        fp = cert.fingerprint(hashes.SHA256()).hex()
        assert fp == fp.lower()
        assert ":" not in fp
        ctx = AuthContext(domain="mtls", authenticated=True, principal="norm-ok", claims={})
        auth_fn = mtls_authenticate_fingerprint(fingerprints={fp: ctx})
        req = _make_req(cert_header=_cert_to_header(cert))
        auth = auth_fn(req)
        assert auth.principal == "norm-ok"

    def test_unsupported_algorithm_raises(self) -> None:
        """Unsupported algorithm raises ValueError at construction time."""
        ctx = AuthContext(domain="mtls", authenticated=True, principal="x", claims={})
        with pytest.raises(ValueError, match="Unsupported hash algorithm"):
            mtls_authenticate_fingerprint(fingerprints={"abc": ctx}, algorithm="md5")


# ---------------------------------------------------------------------------
# TestMtlsAuthenticateSubject
# ---------------------------------------------------------------------------


class TestMtlsAuthenticateSubject:
    """Tests for mtls_authenticate_subject factory."""

    def test_cn_extraction(self) -> None:
        """Subject CN is used as principal."""
        cert, _key = _make_test_cert("my-service")
        auth_fn = mtls_authenticate_subject()
        req = _make_req(cert_header=_cert_to_header(cert))
        auth = auth_fn(req)
        assert auth.principal == "my-service"
        assert auth.domain == "mtls"
        assert auth.authenticated is True

    def test_allowed_subjects_pass(self) -> None:
        """Certificate with CN in allowed_subjects is accepted."""
        cert, _key = _make_test_cert("allowed")
        auth_fn = mtls_authenticate_subject(allowed_subjects=frozenset({"allowed", "also-ok"}))
        req = _make_req(cert_header=_cert_to_header(cert))
        auth = auth_fn(req)
        assert auth.principal == "allowed"

    def test_allowed_subjects_reject(self) -> None:
        """Certificate with CN not in allowed_subjects is rejected."""
        cert, _key = _make_test_cert("forbidden")
        auth_fn = mtls_authenticate_subject(allowed_subjects=frozenset({"allowed"}))
        req = _make_req(cert_header=_cert_to_header(cert))
        with pytest.raises(ValueError, match="not in allowed subjects"):
            auth_fn(req)

    def test_claims_contain_serial_and_validity(self) -> None:
        """Claims include subject_dn, serial (hex), and not_valid_after."""
        cert, _key = _make_test_cert("claims-test")
        auth_fn = mtls_authenticate_subject()
        req = _make_req(cert_header=_cert_to_header(cert))
        auth = auth_fn(req)
        assert "subject_dn" in auth.claims
        assert "serial" in auth.claims
        assert "not_valid_after" in auth.claims
        # Serial should be hex string
        int(str(auth.claims["serial"]), 16)

    def test_check_expiry(self) -> None:
        """Expired certificate is rejected when check_expiry=True."""
        cert, _key = _make_test_cert("expired-subj", days_valid=0, not_before_offset=datetime.timedelta(days=-2))
        auth_fn = mtls_authenticate_subject(check_expiry=True)
        req = _make_req(cert_header=_cert_to_header(cert))
        with pytest.raises(ValueError, match="expired"):
            auth_fn(req)


# ---------------------------------------------------------------------------
# TestParseXfcc
# ---------------------------------------------------------------------------


class TestParseXfcc:
    """Tests for the XFCC parser."""

    def test_simple_single_element(self) -> None:
        """Single element with hash and subject."""
        result = _parse_xfcc('Hash=abc123;Subject="CN=client1"')
        assert len(result) == 1
        assert result[0].hash == "abc123"
        assert result[0].subject == "CN=client1"

    def test_multiple_elements(self) -> None:
        """Comma-separated elements are parsed correctly."""
        result = _parse_xfcc('Hash=a;Subject="CN=first",Hash=b;Subject="CN=second"')
        assert len(result) == 2
        assert result[0].subject == "CN=first"
        assert result[1].subject == "CN=second"

    def test_quoted_subject_with_commas(self) -> None:
        """Commas inside quoted values do not split elements."""
        result = _parse_xfcc('Subject="CN=test,O=Acme\\, Inc."')
        assert len(result) == 1
        assert result[0].subject == "CN=test,O=Acme, Inc."

    def test_quoted_subject_with_semicolons(self) -> None:
        """Semicolons inside quoted values do not split key-value pairs."""
        result = _parse_xfcc('Subject="CN=test;extra=val";Hash=abc')
        assert len(result) == 1
        assert result[0].subject == "CN=test;extra=val"
        assert result[0].hash == "abc"

    def test_url_encoded_cert_field(self) -> None:
        """Cert field is URL-decoded."""
        encoded = quote("-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----\n")
        result = _parse_xfcc(f"Cert={encoded}")
        assert len(result) == 1
        assert result[0].cert is not None
        assert result[0].cert.startswith("-----BEGIN CERTIFICATE-----")

    def test_empty_header(self) -> None:
        """Empty header returns empty list."""
        assert _parse_xfcc("") == []

    def test_dns_multiple(self) -> None:
        """Multiple DNS fields are collected into a tuple."""
        result = _parse_xfcc("DNS=a.example.com;DNS=b.example.com")
        assert len(result) == 1
        assert result[0].dns == ("a.example.com", "b.example.com")

    def test_uri_field(self) -> None:
        """URI field is URL-decoded."""
        encoded = quote("spiffe://cluster.local/ns/default/sa/client")
        result = _parse_xfcc(f"URI={encoded}")
        assert len(result) == 1
        assert result[0].uri == "spiffe://cluster.local/ns/default/sa/client"

    def test_by_field(self) -> None:
        """By field is URL-decoded."""
        encoded = quote("spiffe://cluster.local/ns/default/sa/server")
        result = _parse_xfcc(f"By={encoded}")
        assert len(result) == 1
        assert result[0].by == "spiffe://cluster.local/ns/default/sa/server"


# ---------------------------------------------------------------------------
# TestMtlsAuthenticateXfcc
# ---------------------------------------------------------------------------


class TestMtlsAuthenticateXfcc:
    """Tests for mtls_authenticate_xfcc factory."""

    def test_valid_xfcc(self) -> None:
        """Valid XFCC header extracts principal from Subject CN."""
        auth_fn = mtls_authenticate_xfcc()
        req = _make_req(xfcc='Hash=abc;Subject="CN=client1,O=Acme"')
        auth = auth_fn(req)
        assert auth.principal == "client1"
        assert auth.domain == "mtls"
        assert auth.authenticated is True

    def test_missing_header_raises(self) -> None:
        """Missing x-forwarded-client-cert header raises ValueError."""
        auth_fn = mtls_authenticate_xfcc()
        req = _make_req()
        with pytest.raises(ValueError, match="Missing"):
            auth_fn(req)

    def test_custom_validate(self) -> None:
        """Custom validate callback is used when provided."""

        def validate(elem: XfccElement) -> AuthContext:
            if elem.hash == "trusted":
                return AuthContext(domain="xfcc", authenticated=True, principal="validated", claims={})
            raise ValueError("untrusted")

        auth_fn = mtls_authenticate_xfcc(validate=validate)
        req = _make_req(xfcc="Hash=trusted")
        auth = auth_fn(req)
        assert auth.principal == "validated"

    def test_custom_validate_rejection(self) -> None:
        """Custom validate callback can reject."""

        def validate(elem: XfccElement) -> AuthContext:
            raise ValueError("nope")

        auth_fn = mtls_authenticate_xfcc(validate=validate)
        req = _make_req(xfcc="Hash=whatever")
        with pytest.raises(ValueError, match="nope"):
            auth_fn(req)

    def test_default_principal_extraction(self) -> None:
        """Default mode extracts CN from Subject field."""
        auth_fn = mtls_authenticate_xfcc()
        req = _make_req(xfcc='Subject="CN=my-service,OU=Engineering"')
        auth = auth_fn(req)
        assert auth.principal == "my-service"

    def test_select_element_first(self) -> None:
        """select_element='first' uses the first (original client) element."""
        auth_fn = mtls_authenticate_xfcc(select_element="first")
        req = _make_req(xfcc='Subject="CN=original",Subject="CN=proxy"')
        auth = auth_fn(req)
        assert auth.principal == "original"

    def test_select_element_last(self) -> None:
        """select_element='last' uses the last (nearest proxy) element."""
        auth_fn = mtls_authenticate_xfcc(select_element="last")
        req = _make_req(xfcc='Subject="CN=original",Subject="CN=proxy"')
        auth = auth_fn(req)
        assert auth.principal == "proxy"

    def test_claims_populated(self) -> None:
        """Default mode populates claims from XFCC fields."""
        encoded_uri = quote("spiffe://cluster/ns/default")
        auth_fn = mtls_authenticate_xfcc()
        req = _make_req(xfcc=f'Hash=deadbeef;Subject="CN=svc";URI={encoded_uri}')
        auth = auth_fn(req)
        assert auth.claims["hash"] == "deadbeef"
        assert auth.claims["subject"] == "CN=svc"
        assert auth.claims["uri"] == "spiffe://cluster/ns/default"


# ---------------------------------------------------------------------------
# TestMtlsWithChain
# ---------------------------------------------------------------------------


class TestMtlsWithChain:
    """Tests for chaining mTLS with other authenticators."""

    def test_chain_mtls_plus_bearer(self) -> None:
        """Chain mTLS + bearer: mTLS succeeds, bearer not tried."""
        cert, _key = _make_test_cert("chain-client")

        def validate(c: x509.Certificate) -> AuthContext:
            return AuthContext(domain="mtls", authenticated=True, principal="chain-client", claims={})

        mtls_auth = mtls_authenticate(validate=validate)
        bearer_ctx = AuthContext(domain="bearer", authenticated=True, principal="bearer-user", claims={})
        bearer_auth = bearer_authenticate_static(tokens={"token-x": bearer_ctx})
        chain = chain_authenticate(mtls_auth, bearer_auth)

        req = _make_req(cert_header=_cert_to_header(cert))
        auth = chain(req)
        assert auth.domain == "mtls"
        assert auth.principal == "chain-client"

    def test_chain_mtls_fallback_to_bearer(self) -> None:
        """Chain mTLS + bearer: mTLS fails (no header), falls back to bearer."""

        def validate(c: x509.Certificate) -> AuthContext:
            return AuthContext(domain="mtls", authenticated=True, principal="x", claims={})

        mtls_auth = mtls_authenticate(validate=validate)
        bearer_ctx = AuthContext(domain="bearer", authenticated=True, principal="bearer-user", claims={})
        bearer_auth = bearer_authenticate_static(tokens={"my-token": bearer_ctx})
        chain = chain_authenticate(mtls_auth, bearer_auth)

        req = _make_req(authorization="Bearer my-token")
        auth = chain(req)
        assert auth.domain == "bearer"
        assert auth.principal == "bearer-user"

    def test_chain_end_to_end_rpc(self) -> None:
        """Full round-trip: chain mTLS + bearer over RPC."""
        cert, _key = _make_test_cert("e2e-chain")

        def validate(c: x509.Certificate) -> AuthContext:
            cn_attrs = c.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            return AuthContext(domain="mtls", authenticated=True, principal=str(cn_attrs[0].value), claims={})

        mtls_auth = mtls_authenticate(validate=validate)
        bearer_ctx = AuthContext(domain="bearer", authenticated=True, principal="b", claims={})
        bearer_auth = bearer_authenticate_static(tokens={"tok": bearer_ctx})
        chain = chain_authenticate(mtls_auth, bearer_auth)

        server = RpcServer(_IdentityService, _IdentityImpl())
        client = make_sync_client(
            server,
            signing_key=b"k",
            authenticate=chain,
            default_headers={"X-SSL-Client-Cert": _cert_to_header(cert)},
        )
        with http_connect(_IdentityService, client=client) as svc:
            assert svc.whoami() == "mtls:e2e-chain"
