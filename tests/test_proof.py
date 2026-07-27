# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for proxy-proof minting and verification.

These mirror the ``TestProxyProof`` conformance group: any behaviour asserted
here is part of the cross-language contract in ``docs/proxy-proof-spec.md``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac

import falcon.testing.helpers
import pytest

from vgi_rpc.http._proof import (
    PROOF_HEADER,
    ProofError,
    ProxyProofConfig,
    canonical_string,
    derive_secret,
    mint_proof,
    parse_secrets,
    proxy_proof_gate,
    verify_proof,
)
from vgi_rpc.http._replay import NonceCache

_BASE_KEY = bytes(range(32))
_SECRET = b"\x11" * 32
_OTHER_SECRET = b"\x22" * 32
_ORIGIN = "worker-a.example.com"
_SECRETS = {"prod-use1": (_SECRET, "prod-use1")}


def _tamper(token: str, index: int, value: str) -> str:
    parts = token.split(".")
    parts[index] = value
    return ".".join(parts)


class TestDerivation:
    """Tests for per-(proxy, origin) secret derivation."""

    def test_is_deterministic(self) -> None:
        """The same inputs always yield the same secret."""
        a = derive_secret(_BASE_KEY, "proxy-1", _ORIGIN)
        b = derive_secret(_BASE_KEY, "proxy-1", _ORIGIN)
        assert a == b
        assert len(a) == 32

    def test_differs_per_origin(self) -> None:
        """Audience binding: a different worker gets a different secret."""
        a = derive_secret(_BASE_KEY, "proxy-1", "worker-a")
        b = derive_secret(_BASE_KEY, "proxy-1", "worker-b")
        assert a != b

    def test_differs_per_proxy(self) -> None:
        """A different proxy gets a different secret at the same worker."""
        a = derive_secret(_BASE_KEY, "proxy-1", _ORIGIN)
        b = derive_secret(_BASE_KEY, "proxy-2", _ORIGIN)
        assert a != b

    def test_separator_is_unambiguous(self) -> None:
        """Component boundaries cannot be shifted between the two ids.

        A concatenation without a separator that no component may contain
        would make ("ab", "c") and ("a", "bc") collide.
        """
        a = derive_secret(_BASE_KEY, "ab", "c.d")
        b = derive_secret(_BASE_KEY, "a", "b.c.d")
        assert a != b

    def test_rejects_wrong_key_length(self) -> None:
        """A base key that is not 32 bytes is refused."""
        with pytest.raises(ValueError, match="32 bytes"):
            derive_secret(b"short", "proxy-1", _ORIGIN)


class TestMintVerifyRoundTrip:
    """Tests for the happy path and token shape."""

    def test_round_trip(self) -> None:
        """A freshly minted proof verifies and reports its proxy label."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN)
        claims = verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN)
        assert claims["verified"] == "true"
        assert claims["proxy"] == "prod-use1"
        assert claims["reason"] == "ok"

    def test_token_shape(self) -> None:
        """The wire format matches the specification."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN)
        version, kid, ts, nonce, mac = token.split(".")
        assert version == "v1"
        assert kid == "prod-use1"
        assert ts.isdigit()
        assert len(nonce) == 22
        assert len(mac) == 43

    def test_nonce_is_fresh_per_mint(self) -> None:
        """Each mint draws a new nonce, or the receiver's cache would reject it."""
        tokens = {mint_proof(_SECRET, "prod-use1", _ORIGIN).split(".")[3] for _ in range(50)}
        assert len(tokens) == 50

    def test_claims_are_all_strings(self) -> None:
        """Claim values are strings so they round-trip in every port's map type."""
        claims = verify_proof(mint_proof(_SECRET, "prod-use1", _ORIGIN), secrets=_SECRETS, origin_id=_ORIGIN)
        assert all(isinstance(v, str) for v in claims.values())


class TestMalformed:
    """Tests that malformed input is rejected cheaply and uniformly."""

    @pytest.mark.parametrize(
        ("token", "why"),
        [
            ("", "empty"),
            ("garbage", "not dotted"),
            ("v1.a.b.c", "four fields"),
            ("v1.a.b.c.d.e", "six fields"),
            ("v2.prod-use1.100.n.m", "wrong version"),
            ("v1.bad!kid.100.AAAAAAAAAAAAAAAAAAAAAA.m", "kid charset"),
            ("v1.prod-use1.notanumber.AAAAAAAAAAAAAAAAAAAAAA.m", "ts charset"),
            ("v1.prod-use1.100.short.m", "nonce charset"),
        ],
    )
    def test_rejected_as_malformed(self, token: str, why: str) -> None:
        """Structurally invalid tokens fail before any MAC is computed."""
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN)
        assert exc.value.reason == "malformed", why

    def test_oversized_header_rejected(self) -> None:
        """A huge header is refused before parsing, bounding the work done."""
        with pytest.raises(ProofError) as exc:
            verify_proof("v1." + "x" * 600, secrets=_SECRETS, origin_id=_ORIGIN)
        assert exc.value.reason == "malformed"

    def test_non_base64_mac_rejected(self) -> None:
        """A MAC field that is not base64url is malformed, not a bad MAC."""
        token = _tamper(mint_proof(_SECRET, "prod-use1", _ORIGIN), 4, "!!!!")
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN)
        assert exc.value.reason == "malformed"


class TestSignature:
    """Tests for MAC verification and its framing."""

    def test_unknown_kid(self) -> None:
        """A kid with no configured secret is rejected as such."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN)
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets={"other": (_SECRET, "other")}, origin_id=_ORIGIN)
        assert exc.value.reason == "unknown_kid"

    def test_wrong_secret(self) -> None:
        """A proof signed with the wrong secret fails the MAC check."""
        token = mint_proof(_OTHER_SECRET, "prod-use1", _ORIGIN)
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN)
        assert exc.value.reason == "bad_mac"

    def test_claimed_kid_is_not_trusted(self) -> None:
        """Naming a proxy in `kid` does not grant that proxy's identity.

        The label must come from the secret that verified, never from the
        transmitted field, which the caller fully controls.
        """
        secrets = {"prod-use1": (_SECRET, "prod-use1"), "staging": (_OTHER_SECRET, "staging")}
        # Claims to be prod-use1 but is signed with staging's secret.
        ts, nonce = "1000", "AAAAAAAAAAAAAAAAAAAAAA"
        mac = hmac.new(_OTHER_SECRET, canonical_string("prod-use1", ts, nonce, _ORIGIN), hashlib.sha256).digest()
        forged = f"v1.prod-use1.{ts}.{nonce}.{base64.urlsafe_b64encode(mac).rstrip(b'=').decode()}"
        with pytest.raises(ProofError) as exc:
            verify_proof(forged, secrets=secrets, origin_id=_ORIGIN, now=1000)
        assert exc.value.reason == "bad_mac"

    @pytest.mark.parametrize("index", [1, 2, 3, 4])
    def test_any_tampered_field_fails(self, index: int) -> None:
        """Mutating any signed field invalidates the proof."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN, now=1000, nonce="AAAAAAAAAAAAAAAAAAAAAA")
        replacement = {1: "prod-use1x", 2: "1001", 3: "BBBBBBBBBBBBBBBBBBBBBB", 4: "A" * 43}[index]
        with pytest.raises(ProofError):
            verify_proof(_tamper(token, index, replacement), secrets=_SECRETS, origin_id=_ORIGIN, now=1000)

    def test_mac_framing_must_be_separated(self) -> None:
        """A MAC over concatenated-without-separators fields must not verify.

        This catches a port that implemented the crypto correctly but framed
        the canonical string wrongly — the failure mode a plain round-trip
        test inside one implementation cannot see.
        """
        ts, nonce, kid = "1000", "AAAAAAAAAAAAAAAAAAAAAA", "prod-use1"
        bad_input = b"vgi.proxy.proof.v1" + kid.encode() + ts.encode() + nonce.encode() + _ORIGIN.encode()
        mac = hmac.new(_SECRET, bad_input, hashlib.sha256).digest()
        token = f"v1.{kid}.{ts}.{nonce}.{base64.urlsafe_b64encode(mac).rstrip(b'=').decode()}"
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, now=1000)
        assert exc.value.reason == "bad_mac"


class TestAudienceBinding:
    """Tests that a proof is confined to the worker it was minted for."""

    def test_rejected_at_a_different_origin(self) -> None:
        """A proof for worker A does not verify at worker B, and vice versa.

        Both positives are asserted in the same test so this cannot pass
        because both workers reject everything.
        """
        token_a = mint_proof(_SECRET, "prod-use1", "worker-a")
        token_b = mint_proof(_SECRET, "prod-use1", "worker-b")

        assert verify_proof(token_a, secrets=_SECRETS, origin_id="worker-a")["verified"] == "true"
        assert verify_proof(token_b, secrets=_SECRETS, origin_id="worker-b")["verified"] == "true"

        for token, origin in ((token_a, "worker-b"), (token_b, "worker-a")):
            with pytest.raises(ProofError) as exc:
                verify_proof(token, secrets=_SECRETS, origin_id=origin)
            assert exc.value.reason == "bad_mac"


class TestTimeWindow:
    """Tests for the two-sided timestamp window."""

    def test_accepts_within_window(self) -> None:
        """A proof inside the window verifies."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN, now=1000)
        assert verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, skew_seconds=30, now=1020)

    def test_rejects_expired(self) -> None:
        """A proof older than the window is refused."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN, now=1000)
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, skew_seconds=30, now=1031)
        assert exc.value.reason == "expired"

    def test_rejects_far_future(self) -> None:
        """A far-future timestamp is refused.

        Checking only the upper bound would let a future-dated proof pass
        indefinitely — the defect present in one sibling implementation's
        signed-cookie helper.
        """
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN, now=2000)
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, skew_seconds=30, now=1000)
        assert exc.value.reason == "not_yet_valid"

    def test_tolerates_modest_forward_skew(self) -> None:
        """Small clock drift in either direction is accepted."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN, now=1015)
        assert verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, skew_seconds=30, now=1000)


class TestReplay:
    """Tests for nonce replay rejection."""

    def test_replayed_nonce_rejected(self) -> None:
        """The same proof presented twice is refused the second time."""
        cache = NonceCache(ttl_seconds=30)
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN, now=1000)
        assert verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, nonce_cache=cache, now=1000)
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, nonce_cache=cache, now=1000)
        assert exc.value.reason == "replayed"

    def test_distinct_nonce_same_timestamp_accepted(self) -> None:
        """Concurrent requests in the same second are not replays."""
        cache = NonceCache(ttl_seconds=30)
        for nonce in ("A" * 22, "B" * 22):
            token = mint_proof(_SECRET, "prod-use1", _ORIGIN, now=1000, nonce=nonce)
            assert verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, nonce_cache=cache, now=1000)

    def test_without_cache_replay_is_possible(self) -> None:
        """With the cache disabled only the timestamp bounds replay."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN, now=1000)
        for _ in range(3):
            assert verify_proof(token, secrets=_SECRETS, origin_id=_ORIGIN, nonce_cache=None, now=1000)


class TestConfig:
    """Tests for configuration validation."""

    def test_off_mode_needs_nothing(self) -> None:
        """An unconfigured worker validates trivially."""
        assert ProxyProofConfig().mode == "off"

    @pytest.mark.parametrize("mode", ["allow", "require"])
    def test_requires_origin_and_secrets(self, mode: str) -> None:
        """An enabled worker without an origin id or secrets refuses to start."""
        with pytest.raises(ValueError, match="origin_id"):
            ProxyProofConfig(mode=mode, secrets=_SECRETS)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="secret"):
            ProxyProofConfig(mode=mode, origin_id=_ORIGIN)  # type: ignore[arg-type]

    def test_rejects_wrong_secret_length(self) -> None:
        """A short secret aborts construction rather than weakening the MAC."""
        with pytest.raises(ValueError, match="32 bytes"):
            ProxyProofConfig(mode="require", origin_id=_ORIGIN, secrets={"k": (b"short", "k")})

    def test_rejects_unknown_mode(self) -> None:
        """A typo'd mode is refused instead of silently disabling the gate."""
        with pytest.raises(ValueError, match="mode must be"):
            ProxyProofConfig(mode="requrie")  # type: ignore[arg-type]

    def test_parse_secrets(self) -> None:
        """The kid:hex form parses and the kid doubles as the label."""
        parsed = parse_secrets(f"prod-use1:{'11' * 32},staging:{'22' * 32}")
        assert parsed["prod-use1"] == (_SECRET, "prod-use1")
        assert parsed["staging"] == (_OTHER_SECRET, "staging")

    @pytest.mark.parametrize(
        "raw",
        ["prod-use1", "prod-use1:zz", f"prod-use1:{'11' * 16}", f"bad!kid:{'11' * 32}", ""],
    )
    def test_parse_secrets_rejects_bad_input(self, raw: str) -> None:
        """Any malformed entry fails the whole parse, never a partial one."""
        with pytest.raises(ValueError):
            parse_secrets(raw)


class TestGate:
    """Tests for the request gate and its modes."""

    def _req(self, token: str | None) -> falcon.Request:
        headers = {PROOF_HEADER: token} if token is not None else {}
        return falcon.testing.helpers.create_req(headers=headers)

    def test_require_accepts_valid(self) -> None:
        """A valid proof passes the gate with attribution."""
        gate = proxy_proof_gate(ProxyProofConfig(mode="require", origin_id=_ORIGIN, secrets=_SECRETS))
        claims = gate(self._req(mint_proof(_SECRET, "prod-use1", _ORIGIN)))
        assert claims["verified"] == "true"
        assert claims["proxy"] == "prod-use1"

    def test_require_rejects_missing(self) -> None:
        """A missing header is refused in require mode."""
        gate = proxy_proof_gate(ProxyProofConfig(mode="require", origin_id=_ORIGIN, secrets=_SECRETS))
        with pytest.raises(ProofError) as exc:
            gate(self._req(None))
        assert exc.value.reason == "no_proof"

    def test_require_message_does_not_echo_input(self) -> None:
        """The refusal message never reflects caller-supplied text."""
        gate = proxy_proof_gate(ProxyProofConfig(mode="require", origin_id=_ORIGIN, secrets=_SECRETS))
        with pytest.raises(ProofError) as exc:
            gate(self._req("v1.attacker-controlled.100.AAAAAAAAAAAAAAAAAAAAAA.AAAA"))
        assert "attacker-controlled" not in str(exc.value)

    def test_multiple_headers_rejected(self) -> None:
        """A comma-joined repeat of the header is refused."""
        gate = proxy_proof_gate(ProxyProofConfig(mode="require", origin_id=_ORIGIN, secrets=_SECRETS))
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN)
        with pytest.raises(ProofError) as exc:
            gate(self._req(f"{token}, {token}"))
        assert exc.value.reason == "malformed"

    def test_allow_mode_passes_unverified(self) -> None:
        """Allow mode records the failure but does not deny."""
        gate = proxy_proof_gate(ProxyProofConfig(mode="allow", origin_id=_ORIGIN, secrets=_SECRETS))
        claims = gate(self._req(None))
        assert claims["verified"] == "false"
        assert claims["reason"] == "no_proof"

    def test_allow_mode_still_attributes_a_valid_proof(self) -> None:
        """Allow mode is a rollout lever, so success must be observable."""
        gate = proxy_proof_gate(ProxyProofConfig(mode="allow", origin_id=_ORIGIN, secrets=_SECRETS))
        claims = gate(self._req(mint_proof(_SECRET, "prod-use1", _ORIGIN)))
        assert claims["verified"] == "true"
        assert claims["proxy"] == "prod-use1"

    def test_off_mode_installs_no_gate(self) -> None:
        """Off mode is the absence of a gate, not a gate that always passes."""
        with pytest.raises(ValueError, match="install no gate"):
            proxy_proof_gate(ProxyProofConfig(mode="off"))

    def test_replay_rejected_through_gate(self) -> None:
        """The gate's cache is shared across requests."""
        gate = proxy_proof_gate(ProxyProofConfig(mode="require", origin_id=_ORIGIN, secrets=_SECRETS))
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN)
        assert gate(self._req(token))["verified"] == "true"
        with pytest.raises(ProofError) as exc:
            gate(self._req(token))
        assert exc.value.reason == "replayed"


class TestRotation:
    """Tests for key rotation via multiple kids."""

    def test_both_kids_accepted_during_overlap(self) -> None:
        """Old and new secrets verify simultaneously, so no request is lost."""
        secrets = {"prod-use1": (_SECRET, "prod-use1"), "prod-use1-v2": (_OTHER_SECRET, "prod-use1")}
        for kid, secret in (("prod-use1", _SECRET), ("prod-use1-v2", _OTHER_SECRET)):
            claims = verify_proof(mint_proof(secret, kid, _ORIGIN), secrets=secrets, origin_id=_ORIGIN)
            # Both carry the same operator-facing label, so attribution is
            # undisturbed by an overlap window.
            assert claims["proxy"] == "prod-use1"

    def test_retired_kid_rejected(self) -> None:
        """After removal the old secret no longer verifies."""
        token = mint_proof(_SECRET, "prod-use1", _ORIGIN)
        with pytest.raises(ProofError) as exc:
            verify_proof(token, secrets={"prod-use1-v2": (_OTHER_SECRET, "prod-use1")}, origin_id=_ORIGIN)
        assert exc.value.reason == "unknown_kid"
