# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Adversarial PROXY protocol v2 parser and exact-read tests."""

from __future__ import annotations

import datetime
import ipaddress
import socket
import ssl
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, cast

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
    PeerResolutionContext,
    RpcServer,
    SubjectKind,
    SubjectStability,
)
from vgi_rpc.rpc import TcpTransport, make_tcp_pair, serve_tcp, tcp_connect
from vgi_rpc.rpc._client import _RpcProxy
from vgi_rpc.rpc._identity import peer_identity_primary
from vgi_rpc.rpc._proxy_protocol_v2 import (
    VGI_IROH_ENDPOINT_TLV,
    ProxyProtocolV2Error,
    parse_proxy_protocol_v2,
    read_proxy_protocol_v2,
)
from vgi_rpc.rpc._transport import _tcp_identity_resolver

_SIGNATURE = b"\r\n\r\n\0\r\nQUIT\n"


def _write_tls_material(directory: Path) -> tuple[Path, Path, Path, Path, Path]:
    """Create one CA and valid client/server X.509-SVID leaves."""
    now = datetime.datetime.now(datetime.UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "VGI test CA")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )

    def leaf(name: str, spiffe_id: str) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)]))
            .issuer_name(ca.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=1))
            .not_valid_after(now + datetime.timedelta(hours=1))
            .add_extension(
                x509.SubjectAlternativeName([x509.UniformResourceIdentifier(spiffe_id)]),
                critical=False,
            )
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH, ExtendedKeyUsageOID.SERVER_AUTH]),
                critical=False,
            )
            .sign(ca_key, hashes.SHA256())
        )
        return key, cert

    client_key, client = leaf("client", "spiffe://example.org/client")
    server_key, server = leaf("server", "spiffe://example.org/server")
    paths = tuple(directory / name for name in ("ca.pem", "client.pem", "client.key", "server.pem", "server.key"))
    paths[0].write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    paths[1].write_bytes(client.public_bytes(serialization.Encoding.PEM))
    paths[2].write_bytes(
        client_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    paths[3].write_bytes(server.public_bytes(serialization.Encoding.PEM))
    paths[4].write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cast("tuple[Path, Path, Path, Path, Path]", paths)


def _preamble(
    source: str = "192.0.2.10",
    destination: str = "198.51.100.20",
    source_port: int = 4242,
    destination_port: int = 9400,
    *,
    tlvs: bytes = b"",
) -> bytes:
    source_ip = ipaddress.ip_address(source)
    destination_ip = ipaddress.ip_address(destination)
    if source_ip.version != destination_ip.version:
        raise ValueError("address families must match")
    family = 0x11 if source_ip.version == 4 else 0x21
    body = source_ip.packed + destination_ip.packed
    body += source_port.to_bytes(2, "big") + destination_port.to_bytes(2, "big")
    body += tlvs
    return _SIGNATURE + bytes((0x21, family)) + len(body).to_bytes(2, "big") + body


def _iroh_preamble(endpoint_id: bytes = bytes(range(32)), *, tlvs: bytes = b"") -> bytes:
    identity = bytes((VGI_IROH_ENDPOINT_TLV,)) + (33).to_bytes(2, "big") + b"\x01" + endpoint_id
    body = identity + tlvs
    return _SIGNATURE + b"\x21\x00" + len(body).to_bytes(2, "big") + body


@pytest.mark.parametrize(
    ("source", "destination", "expected_source"),
    [
        ("192.0.2.10", "198.51.100.20", "192.0.2.10"),
        ("2001:db8::10", "2001:db8::20", "2001:db8::10"),
        ("::ffff:192.0.2.10", "::ffff:198.51.100.20", "192.0.2.10"),
    ],
)
def test_proxy_v2_parses_tcp_addresses_and_bounded_unknown_tlvs(
    source: str, destination: str, expected_source: str
) -> None:
    """IPv4, IPv6, mapped IPv4, and structurally valid unknown TLVs work."""
    parsed = parse_proxy_protocol_v2(_preamble(source, destination, tlvs=b"\xee\x00\x03abc"))
    assert parsed.source_address == expected_source
    assert parsed.source_port == 4242
    assert parsed.destination_port == 9400


def test_proxy_v2_iroh_identity_requires_explicit_unspec_opt_in() -> None:
    """A non-IP Iroh subject is accepted only on the dedicated trusted path."""
    preamble = _iroh_preamble()
    with pytest.raises(ProxyProtocolV2Error, match="IPv4 or IPv6"):
        parse_proxy_protocol_v2(preamble)

    parsed = parse_proxy_protocol_v2(preamble, allow_iroh_identity=True)
    assert parsed.source_address is None
    assert parsed.destination_address is None
    assert parsed.iroh_endpoint_id == bytes(range(32))


@pytest.mark.parametrize(
    "preamble",
    [
        _iroh_preamble(tlvs=bytes((VGI_IROH_ENDPOINT_TLV, 0, 33)) + b"\x01" + bytes(range(32))),
        _SIGNATURE + b"\x21\x00\x00\x00",
        _SIGNATURE + b"\x21\x00\x00\x04" + bytes((VGI_IROH_ENDPOINT_TLV, 0, 1, 2)),
    ],
)
def test_proxy_v2_rejects_ambiguous_or_invalid_iroh_identity(preamble: bytes) -> None:
    """Missing, duplicate, wrong-sized, or wrong-version identity fails closed."""
    with pytest.raises(ProxyProtocolV2Error):
        parse_proxy_protocol_v2(preamble, allow_iroh_identity=True)


def test_proxy_v2_rejects_iroh_identity_on_ip_family_when_enabled() -> None:
    """A bridge cannot combine an asserted IP source with an Iroh subject."""
    tlv = bytes((VGI_IROH_ENDPOINT_TLV, 0, 33)) + b"\x01" + bytes(range(32))
    with pytest.raises(ProxyProtocolV2Error, match="requires PROXY/UNSPEC"):
        parse_proxy_protocol_v2(_preamble(tlvs=tlv), allow_iroh_identity=True)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: b"bad-signatur" + value[12:],
        lambda value: value[:12] + b"\x11" + value[13:],  # version 1
        lambda value: value[:12] + b"\x20" + value[13:],  # LOCAL
        lambda value: value[:13] + b"\x12" + value[14:],  # UDP/IPv4
        lambda value: value[:-1],
        lambda value: value + b"x",
        lambda value: value[:-3] + b"\xee\x00\x04x",
    ],
)
def test_proxy_v2_rejects_unsafe_or_malformed_forms(mutate: Callable[[bytes], bytes]) -> None:
    """Bad signature/version/command/protocol/length/TLV forms fail closed."""
    preamble = _preamble(tlvs=b"\xee\x00\x01x")
    with pytest.raises(ProxyProtocolV2Error):
        parse_proxy_protocol_v2(mutate(preamble))


def test_proxy_v2_reader_preserves_following_vgi_bytes_and_deadline() -> None:
    """The exact reader consumes no Arrow byte and applies one total timeout."""
    reader, writer = socket.socketpair()
    preamble = _preamble()

    def send_fragments() -> None:
        for byte in preamble + b"VGI":
            writer.sendall(bytes((byte,)))
        writer.close()

    thread = threading.Thread(target=send_fragments)
    thread.start()
    parsed = read_proxy_protocol_v2(reader, timeout=1.0)
    assert parsed.source_address == "192.0.2.10"
    following = bytearray()
    while len(following) < 3:
        following.extend(reader.recv(3 - len(following)))
    assert bytes(following) == b"VGI"
    reader.close()
    thread.join(timeout=1)

    reader, writer = socket.socketpair()
    writer.sendall(preamble[:4])
    started = time.monotonic()
    with pytest.raises(ProxyProtocolV2Error, match="timed out"):
        read_proxy_protocol_v2(reader, timeout=0.03)
    assert time.monotonic() - started < 0.2
    reader.close()
    writer.close()


class _Provider:
    provider = "spiffe"

    def __init__(self) -> None:
        self.context: PeerResolutionContext | None = None

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        self.context = context
        return PeerIdentityResult.available(
            PeerIdentity(
                provider=self.provider,
                evidence_source="test",
                assurance=IdentityAssurance.CONFIGURED_PROXY,
                issuer="spiffe://example.org",
                transport="tcp",
                subject_kind=SubjectKind.WORKLOAD,
                subject_key="spiffe://example.org/client",
                subject_stability=SubjectStability.STABLE,
                subject_verified=True,
                source_address=context.asserted_peer,
                proxy_address=context.immediate_peer,
            )
        )


def _connected_tcp_pair() -> tuple[socket.socket, socket.socket]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    client = socket.create_connection(listener.getsockname())
    server, _ = listener.accept()
    listener.close()
    return client, server


def test_tcp_resolver_trusts_immediate_peer_before_parsing_and_snapshots_scope() -> None:
    """Trusted PROXY source/destination and service reach providers exactly once."""
    provider = _Provider()
    resolver = _tcp_identity_resolver(
        proxy_protocol="required",
        trusted_proxy_addresses=("127.0.0.1",),
        proxy_preamble_timeout=0.5,
        maximum_proxy_preamble_bytes=256,
        service_name="svc:analytics",
        peer_identity_providers=(provider,),
        peer_authentication_policy=peer_identity_primary("spiffe"),
        peer_resolution_timeout=0.5,
        peer_provider_concurrency=1,
    )
    assert resolver is not None
    client, server = _connected_tcp_pair()
    client.sendall(_preamble() + b"VGI")
    resolved_server, (auth, evidence, metadata) = resolver(server)
    assert resolved_server is server
    assert auth.authenticated
    assert auth.principal and auth.principal.endswith("spiffe%3A%2F%2Fexample.org%2Fclient")
    assert evidence.provider_status["spiffe"].value == "available"
    assert provider.context is not None
    assert provider.context.immediate_peer == "127.0.0.1"
    assert provider.context.source_endpoint is not None
    assert provider.context.source_endpoint.startswith("127.0.0.1:")
    assert provider.context.asserted_peer == "192.0.2.10:4242"
    assert provider.context.destination_address == "198.51.100.20:9400"
    assert provider.context.service_name == "svc:analytics"
    assert metadata["immediate_peer"].startswith("127.0.0.1:")
    assert server.recv(3) == b"VGI"
    client.close()
    server.close()


def test_tcp_resolver_promotes_forwarded_iroh_identity() -> None:
    """The trusted bridge supplies only the subject; issuer remains local."""
    resolver = _tcp_identity_resolver(
        proxy_protocol="required",
        trusted_proxy_addresses=("127.0.0.1",),
        proxy_preamble_timeout=0.5,
        maximum_proxy_preamble_bytes=256,
        service_name=None,
        peer_identity_providers=(),
        peer_authentication_policy=peer_identity_primary("iroh"),
        peer_resolution_timeout=0.5,
        peer_provider_concurrency=1,
        iroh_proxy_issuer="production-mesh",
    )
    assert resolver is not None
    client, server = _connected_tcp_pair()
    client.sendall(_iroh_preamble() + b"VGI")
    resolved_server, (auth, evidence, metadata) = resolver(server)
    identity = evidence.identities[0]
    assert resolved_server is server
    assert auth.authenticated
    assert identity.provider == "iroh"
    assert identity.issuer == "production-mesh"
    assert identity.subject_key == bytes(range(32)).hex()
    assert identity.assurance is IdentityAssurance.CONFIGURED_PROXY
    assert identity.attributes["original_assurance"] == "cryptographic_peer"
    assert metadata["remote_addr"].startswith("127.0.0.1:")
    assert server.recv(3) == b"VGI"
    client.close()
    server.close()


def test_tcp_resolver_rejects_untrusted_proxy_before_reading() -> None:
    """An untrusted immediate peer cannot make the server parse a preamble."""
    untrusted = _tcp_identity_resolver(
        proxy_protocol="required",
        trusted_proxy_addresses=("192.0.2.1",),
        proxy_preamble_timeout=1.0,
        maximum_proxy_preamble_bytes=256,
        service_name=None,
        peer_identity_providers=(),
        peer_authentication_policy=None,
        peer_resolution_timeout=1.0,
        peer_provider_concurrency=1,
    )
    assert untrusted is not None
    client, server = _connected_tcp_pair()
    started = time.monotonic()
    with pytest.raises(ProxyProtocolV2Error, match="trusted"):
        untrusted(server)
    assert time.monotonic() - started < 0.1
    client.close()
    server.close()


class _IdentityService(Protocol):
    def identity(self) -> str:
        """Return the connection identity observed by the worker."""
        ...


class _IdentityImpl:
    def identity(self, ctx: CallContext) -> str:
        return f"{ctx.auth.principal}:{ctx.peer_evidence.provider_status['spiffe']}"


def test_rpc_server_connection_identity_reaches_raw_call_context() -> None:
    """One raw connection snapshot reaches calls without entering the wire format."""
    client_transport, server_transport = make_tcp_pair()
    identity = PeerIdentity(
        provider="spiffe",
        evidence_source="test",
        assurance=IdentityAssurance.CRYPTOGRAPHIC_PEER,
        issuer="spiffe://example.org",
        transport="tcp",
        subject_kind=SubjectKind.WORKLOAD,
        subject_key="spiffe://example.org/client",
        subject_stability=SubjectStability.STABLE,
        subject_verified=True,
    )
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(identity),))
    auth = AuthContext(domain="spiffe", authenticated=True, principal="worker-client", claims={"nested": {"v": 1}})
    server = RpcServer(_IdentityService, _IdentityImpl())
    thread = threading.Thread(
        target=server.serve,
        args=(server_transport,),
        kwargs={"auth": auth, "peer_evidence": evidence},
        daemon=True,
    )
    thread.start()
    try:
        proxy = _RpcProxy(_IdentityService, client_transport, None)
        assert cast("_IdentityService", proxy).identity() == "worker-client:available"
    finally:
        client_transport.close()
        thread.join(timeout=2)
        server_transport.close()


def test_serve_tcp_proxy_v2_resolves_identity_before_vgi_framing() -> None:
    """The public listener strips PROXY v2 and serves the following Arrow request."""
    provider = _Provider()
    server = RpcServer(_IdentityService, _IdentityImpl())
    ready = threading.Event()
    bound_port = 0

    def bound(_host: str, port: int) -> None:
        nonlocal bound_port
        bound_port = port
        ready.set()

    thread = threading.Thread(
        target=serve_tcp,
        args=(server, "127.0.0.1", 0),
        kwargs={
            "threaded": True,
            "on_bound": bound,
            "proxy_protocol": "required",
            "trusted_proxy_addresses": ("127.0.0.1",),
            "peer_identity_providers": (provider,),
            "peer_authentication_policy": peer_identity_primary("spiffe"),
        },
        daemon=True,
    )
    thread.start()
    assert ready.wait(2)
    raw = socket.create_connection(("127.0.0.1", bound_port))
    raw.sendall(_preamble())
    transport = TcpTransport(raw)
    try:
        proxy = _RpcProxy(_IdentityService, transport, None)
        assert cast("_IdentityService", proxy).identity().endswith(":available")
    finally:
        transport.close()


def test_direct_tls_spiffe_is_verified_and_snapshotted_for_connection(tmp_path: Path) -> None:
    """Mutual TLS creates direct cryptographic SPIFFE evidence for every call."""
    ca, client_cert, client_key, server_cert, server_key = _write_tls_material(tmp_path)
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(server_cert, server_key)
    server_tls.load_verify_locations(cafile=ca)
    server_tls.verify_mode = ssl.CERT_REQUIRED

    client_tls = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
    client_tls.check_hostname = False
    client_tls.load_cert_chain(client_cert, client_key)

    server = RpcServer(_IdentityService, _IdentityImpl())
    ready = threading.Event()
    bound_port = 0

    def bound(_host: str, port: int) -> None:
        nonlocal bound_port
        bound_port = port
        ready.set()

    thread = threading.Thread(
        target=serve_tcp,
        args=(server, "127.0.0.1", 0),
        kwargs={
            "threaded": True,
            "max_connections": 1,
            "idle_timeout": 0.05,
            "on_bound": bound,
            "tls_context": server_tls,
            "spiffe_trust_domains": ("example.org",),
            "peer_authentication_policy": peer_identity_primary("spiffe"),
        },
        daemon=True,
    )
    thread.start()
    assert ready.wait(2)
    with tcp_connect(
        _IdentityService,
        "127.0.0.1",
        bound_port,
        tls_context=client_tls,
        server_spiffe_trust_domains=("example.org",),
    ) as client:
        observed = client.identity()
        assert observed.endswith(":available")
        assert "spiffe%3A%2F%2Fexample.org%2Fclient" in observed
    thread.join(timeout=2)
    assert not thread.is_alive()


def test_direct_spiffe_requires_mutual_tls_verification() -> None:
    """A TLS context that does not verify client chains cannot assert SPIFFE."""
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    with pytest.raises(ValueError, match="CERT_REQUIRED"):
        _tcp_identity_resolver(
            proxy_protocol="off",
            trusted_proxy_addresses=(),
            proxy_preamble_timeout=1,
            maximum_proxy_preamble_bytes=256,
            service_name=None,
            peer_identity_providers=(),
            peer_authentication_policy=None,
            peer_resolution_timeout=1,
            peer_provider_concurrency=1,
            tls_context=context,
            spiffe_trust_domains=("example.org",),
        )
