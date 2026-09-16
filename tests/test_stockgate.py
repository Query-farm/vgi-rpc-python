# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Stockgate worker registration client tests."""

from __future__ import annotations

import base64
import json
import os
import stat
import struct
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from vgi_rpc.iroh import _load_iroh
from vgi_rpc.stockgate import (
    StockgateAddress,
    StockgateClient,
    StockgateError,
    StockgateHttpResponse,
    StockgateRegistrationIntent,
    _pop_message,
    load_or_create_iroh_secret_key,
)


class _StockgateFixture:
    """Stateful fake transport that validates the registration protocol."""

    def __init__(self, secret_key: bytes) -> None:
        self.secret_key = secret_key
        self.resolver_key = _load_iroh().SecretKey.from_bytes(bytes(reversed(range(32))))
        self.calls: list[tuple[str, str, Mapping[str, str], bytes | None]] = []
        self.endpoint_id = ""
        self.packet = b""

    def __call__(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> StockgateHttpResponse:
        """Return deterministic API responses and verify submitted proofs."""
        assert timeout == 15.0
        self.calls.append((method, url, headers, body))
        payload = json.loads(body) if body else {}
        if url.endswith("/endpoints/challenge"):
            self.endpoint_id = payload["endpoint_id"]
            assert payload["tags"] == ["client-requested"]
            assert payload["ephemeral"] is False
            return self._json(
                200,
                {
                    "challenge_id": "pop_test",
                    "nonce": "nonce-value",
                    "org_id": "org_test",
                    "kind": "service",
                    "service_id": "svc_test",
                    "endpoint_id": self.endpoint_id,
                    "tags": ["token-authoritative"],
                    "label": "test worker",
                    "ephemeral": True,
                    "expires_in": 120,
                },
            )
        if url.endswith("/endpoints") and method == "POST":
            self.packet = base64.b64decode(payload["signed_packet"], validate=True)
            self._verify_address_packet(self.packet)
            signature = base64.b64decode(payload["signature"], validate=True)
            api = _load_iroh()
            public = api.SecretKey.from_bytes(self.secret_key).public()
            expected = _pop_message(
                nonce="nonce-value",
                org_id="org_test",
                intent=StockgateRegistrationIntent(
                    org="o/test-org",
                    service="echo",
                    label="test worker",
                    tags=("token-authoritative",),
                    ephemeral=True,
                ),
                service_id="svc_test",
                endpoint_id=self.endpoint_id,
                signed_packet=self.packet,
            )
            public.verify(expected, api.Signature.from_bytes(signature))
            return self._json(
                201,
                {
                    "endpoint_id": self.endpoint_id,
                    "service_id": "svc_test",
                    "credential": "ec_original.secret",
                    "credential_expires_in": 86_400,
                },
            )
        if url.endswith("/heartbeat"):
            assert payload == {"config_version": 0, "endpoint_id": self.endpoint_id}
            return self._json(
                200,
                {
                    "credential": "ec_rotated.secret",
                    "config_version": 3,
                    "next_heartbeat_s": 45,
                    "require_attest": True,
                    "accepts_from": ["peer-id"],
                    "revoked_jti": {"cursor": 4, "items": []},
                },
            )
        if "/resolve/o/test-org/echo" in url:
            now = int(time.time())
            resolved = {
                "endpoint_id": self.endpoint_id,
                "service_id": "svc_test",
                "name": "stockgate://o/test-org/echo",
                "org": "o/test-org",
                "resolved_at": now,
                "expires_at": now + 120,
            }
            return self._json(
                200,
                {
                    **resolved,
                    "signed_packet": base64.b64encode(self.packet).decode(),
                    "liveness": "live",
                    "require_attest": True,
                    "sig": self._resolver_signature(resolved),
                },
            )
        if url.endswith("/.well-known/jwks.json"):
            public = self.resolver_key.public().to_bytes()
            return self._json(
                200,
                {
                    "keys": [
                        {
                            "kty": "OKP",
                            "crv": "Ed25519",
                            "kid": "resolver-test",
                            "alg": "EdDSA",
                            "use": "sig",
                            "x": self._base64url(public),
                        }
                    ]
                },
            )
        if method == "PUT" and url.endswith("/address-record"):
            self.packet = base64.b64decode(payload["signed_packet"], validate=True)
            self._verify_address_packet(self.packet)
            return StockgateHttpResponse(204, {}, b"")
        if method == "DELETE":
            return StockgateHttpResponse(204, {}, b"")
        raise AssertionError(f"Unexpected request: {method} {url}")

    def _verify_address_packet(self, packet: bytes) -> None:
        """Verify the relay-format Pkarr packet with the endpoint key."""
        assert len(packet) >= 84
        signature = packet[:64]
        timestamp = struct.unpack(">Q", packet[64:72])[0]
        dns = packet[72:]
        signable = b"3:seqi" + str(timestamp).encode() + b"e1:v" + str(len(dns)).encode() + b":" + dns
        api = _load_iroh()
        public = api.SecretKey.from_bytes(self.secret_key).public()
        public.verify(signable, api.Signature.from_bytes(signature))
        assert public.to_bytes().hex() not in packet[:32].hex()

    @staticmethod
    def _json(status: int, payload: dict[str, Any]) -> StockgateHttpResponse:
        """Encode a fixture response."""
        return StockgateHttpResponse(status, {"content-type": "application/json"}, json.dumps(payload).encode())

    @staticmethod
    def _base64url(value: bytes) -> str:
        """Encode unpadded base64url."""
        return base64.urlsafe_b64encode(value).decode().rstrip("=")

    def _resolver_signature(self, payload: dict[str, Any]) -> str:
        """Create the detached JWS used by resolver responses."""
        header = self._base64url(
            json.dumps(
                {"alg": "EdDSA", "kid": "resolver-test", "typ": "sg-resolve+jws"},
                separators=(",", ":"),
            ).encode()
        )
        encoded_payload = self._base64url(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode())
        signature = self.resolver_key.sign(f"{header}.{encoded_payload}".encode())
        return f"{header}..{self._base64url(signature.to_bytes())}"


class _ManagementFixture(_StockgateFixture):
    """Fake the human-management calls that precede registration."""

    def __call__(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> StockgateHttpResponse:
        """Create a service and exact registration grant before PoP."""
        payload = json.loads(body) if body else {}
        if method == "GET" and url.endswith("/orgs/test-org/services"):
            self.calls.append((method, url, headers, body))
            return self._json(200, {"services": []})
        if method == "POST" and url.endswith("/orgs/test-org/services"):
            self.calls.append((method, url, headers, body))
            assert payload == {
                "display_name": None,
                "name": "echo",
                "reach_mode": "direct",
                "require_attest": False,
                "visibility": "org",
            }
            return self._json(201, {"name": "echo", "service_id": "svc_test"})
        if method == "POST" and url.endswith("/orgs/test-org/registration-tokens"):
            self.calls.append((method, url, headers, body))
            assert payload["service_id"] == "svc_test"
            assert payload["expires_in"] == 600
            assert payload["ephemeral"] is False
            assert isinstance(payload["endpoint_id"], str)
            return self._json(
                201,
                {
                    "credential": "rt_exact.secret",
                    "service_id": "svc_test",
                    "endpoint_id": payload["endpoint_id"],
                    "ephemeral": False,
                    "expires_at": int(time.time()) + 600,
                },
            )
        return super().__call__(method, url, headers, body, timeout)


def test_registration_heartbeat_resolution_and_cleanup() -> None:
    """A worker completes registration and its ongoing lifecycle."""
    secret_key = bytes(range(32))
    fixture = _StockgateFixture(secret_key)
    client = StockgateClient("https://stockgate.example", transport=fixture)
    endpoint = client.register(
        "rt_token-id.bootstrap-secret",
        secret_key,
        StockgateAddress(direct_addresses=("127.0.0.1:9400",)),
        StockgateRegistrationIntent(
            org="o/test-org",
            service="echo",
            label="test worker",
            tags=("client-requested",),
        ),
    )

    assert endpoint.tags == ("token-authoritative",)
    assert endpoint.ephemeral is True
    assert endpoint.endpoint_hex == _load_iroh().SecretKey.from_bytes(secret_key).public().to_bytes().hex()
    challenge_call, registration_call = fixture.calls
    assert challenge_call[2]["Authorization"] == "Bearer rt_token-id.bootstrap-secret"
    assert registration_call[2]["Authorization"] == "Bearer rt_token-id.bootstrap-secret"
    assert "bootstrap-secret" not in challenge_call[1]
    assert b"bootstrap-secret" not in (challenge_call[3] or b"")

    heartbeat = client.heartbeat(endpoint)
    assert heartbeat.endpoint.credential == "ec_rotated.secret"
    assert heartbeat.endpoint.config_version == 3
    assert heartbeat.next_heartbeat_s == 45
    assert heartbeat.require_attest is True
    assert heartbeat.accepts_from == ("peer-id",)
    assert heartbeat.revoked_jti == {"cursor": 4, "items": []}

    resolution = client.resolve("test-org", "echo", heartbeat.endpoint.credential)
    assert resolution.endpoint_id == endpoint.endpoint_id
    assert resolution.endpoint_hex == endpoint.endpoint_hex
    assert resolution.signed_packet == endpoint.signed_packet
    assert resolution.address == StockgateAddress(direct_addresses=("127.0.0.1:9400",))
    updated_address = client.update_address(
        heartbeat.endpoint,
        secret_key,
        StockgateAddress(direct_addresses=("127.0.0.1:9500",)),
    )
    assert updated_address.signed_packet == fixture.packet
    client.delete_endpoint(heartbeat.endpoint)
    assert fixture.calls[-1][0] == "DELETE"
    assert fixture.calls[-1][2]["Authorization"] == "Bearer ec_rotated.secret"


def test_human_registration_workflow_creates_service_and_exact_grant() -> None:
    """The high-level workflow binds a single-use grant before registering."""
    secret_key = bytes(range(32))
    fixture = _ManagementFixture(secret_key)
    client = StockgateClient("https://stockgate.example", transport=fixture)

    service, endpoint = client.register_service(
        "dc_human.secret",
        secret_key,
        StockgateAddress(direct_addresses=("127.0.0.1:9400",)),
        StockgateRegistrationIntent(
            org="test-org",
            service="echo",
            label="test worker",
            tags=("client-requested",),
        ),
    )

    assert service.created is True
    assert service.service_id == endpoint.service_id == "svc_test"
    assert [call[0] for call in fixture.calls[:5]] == ["GET", "POST", "POST", "POST", "POST"]
    for call in fixture.calls[:3]:
        assert call[2]["Authorization"] == "Bearer dc_human.secret"
    for call in fixture.calls[3:5]:
        assert call[2]["Authorization"] == "Bearer rt_exact.secret"


def test_device_authorization_polls_without_exposing_code() -> None:
    """Device authorization waits for approval and returns the human credential."""
    calls: list[tuple[str, str, bytes | None]] = []
    authorization: list[tuple[str, str]] = []
    polls = 0

    def device_transport(
        method: str,
        url: str,
        _headers: Mapping[str, str],
        body: bytes | None,
        _timeout: float,
    ) -> StockgateHttpResponse:
        nonlocal polls
        calls.append((method, url, body))
        if url.endswith("/device/code"):
            return _StockgateFixture._json(
                201,
                {
                    "device_code": "ABCD-EFGH.device-secret",
                    "user_code": "ABCD-EFGH",
                    "verification_uri": "https://stockgate.example/device/verify",
                    "verification_uri_complete": "https://stockgate.example/device/verify?user_code=ABCD-EFGH",
                    "expires_in": 600,
                    "interval": 5,
                },
            )
        polls += 1
        if polls == 1:
            return _StockgateFixture._json(
                400,
                {"title": "authorization_pending", "detail": "Device authorization is pending."},
            )
        return _StockgateFixture._json(
            200,
            {"access_token": "dc_human.secret", "token_type": "Bearer", "expires_in": 3600},
        )

    credential = StockgateClient("https://stockgate.example", transport=device_transport).authenticate_device(
        on_authorization=lambda url, code: authorization.append((url, code)),
        sleep=lambda _seconds: None,
    )

    assert credential.access_token == "dc_human.secret"
    assert authorization == [("https://stockgate.example/device/verify?user_code=ABCD-EFGH", "ABCD-EFGH")]
    assert len(calls) == 3


def test_key_file_is_persistent_and_private(tmp_path: Path) -> None:
    """A generated Iroh identity survives restarts and rejects loose modes."""
    path = tmp_path / "worker" / "iroh.key"
    created = load_or_create_iroh_secret_key(path)
    assert len(created) == 32
    assert load_or_create_iroh_secret_key(path) == created
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        path.chmod(0o644)
        with pytest.raises(ValueError, match="must not be accessible"):
            load_or_create_iroh_secret_key(path)


def test_response_mismatch_fails_closed() -> None:
    """A challenge that changes endpoint identity is never signed."""

    def mismatched(
        _method: str,
        _url: str,
        _headers: Mapping[str, str],
        _body: bytes | None,
        _timeout: float,
    ) -> StockgateHttpResponse:
        return _StockgateFixture._json(
            200,
            {
                "challenge_id": "pop_test",
                "nonce": "nonce",
                "org_id": "org_test",
                "kind": "service",
                "service_id": "svc_test",
                "endpoint_id": "ybndrfg8ejkmcpqxot1uwisza345h769ybndrfg8ejkmcpqxot1u",
                "tags": [],
                "label": None,
                "ephemeral": False,
            },
        )

    client = StockgateClient("https://stockgate.example", transport=mismatched)
    with pytest.raises(StockgateError, match="different endpoint ID"):
        client.register(
            "rt_token.secret",
            bytes(32),
            StockgateAddress(direct_addresses=("127.0.0.1:9400",)),
            StockgateRegistrationIntent(org="o/test", service="echo"),
        )


def test_structured_error_does_not_expose_credential() -> None:
    """Transport failures retain API diagnostics without secret material."""

    def denied(
        _method: str,
        _url: str,
        _headers: Mapping[str, str],
        _body: bytes | None,
        _timeout: float,
    ) -> StockgateHttpResponse:
        return _StockgateFixture._json(401, {"title": "invalid_credential", "detail": "Credential was rejected."})

    client = StockgateClient("https://stockgate.example", transport=denied)
    with pytest.raises(StockgateError) as caught:
        client.register(
            "rt_token.super-secret-value",
            bytes(32),
            StockgateAddress(direct_addresses=("127.0.0.1:9400",)),
            StockgateRegistrationIntent(org="o/test", service="echo"),
        )
    assert caught.value.status == 401
    assert caught.value.code == "invalid_credential"
    assert "super-secret-value" not in str(caught.value)
