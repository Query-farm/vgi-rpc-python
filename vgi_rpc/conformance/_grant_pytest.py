# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Cross-language conformance for closing the identity loop over HTTP.

``issue_grant`` mints a credential for unattended automation to present later
as an ordinary bearer, and ``resolve_token`` resolves opaque credentials -- but
until WIRE_PROTOCOL.md §16 "Accepting identity credentials" nothing fed either
back into authentication.  These groups assert, against a port's *grant
worker* (``IDENTITY_CONFORMANCE_FIXTURE.md`` §10):

- a grant the worker mints is accepted back as a bearer, as the grant's
  principal, with ``domain="grant"``, the grant's claims and **no**
  ``auth_time``;
- a grant-authenticated caller cannot ``issue_grant`` (grants never mint grants);
- the worker's minter matches the format: its token opens under the published
  fixture key with the reference verifier;
- its verifier matches too: grants the *reference* mints with the fixture key
  are accepted, including under the previous (rotated) key;
- tampered, expired, wrong-key, wrong-audience and non-canonical grants are
  refused 401 **without** falling through to ``resolve_token`` -- which here
  resolves almost anything, so a fall-through is a loud success, not a quiet
  401 (the resolvable-probe pattern);
- a token without the ``vgig1.`` prefix never reaches the grant verifier;
- ``resolve_token`` backs bearer authentication: resolved => accepted,
  ``None`` => 401, unavailable => 503 with the hook's ``Retry-After``.
"""

from __future__ import annotations

import json
import time
from io import BytesIO
from typing import TYPE_CHECKING, Any

import pytest
from pyarrow import ipc

from vgi_rpc.conformance.identity_fixture import (
    AUTH_TIME_HEADER,
    AUTH_UNAVAILABLE_RETRY_AFTER,
    GRANT_AUDIENCE,
    GRANT_KEY_CURRENT,
    GRANT_KEY_PREVIOUS,
    GRANT_KEY_STRANGER,
    GRANT_MAX_TTL,
    MINTER_PRINCIPAL,
    PRINCIPAL_HEADER,
    SUBJECT_PRINCIPAL,
    SUBJECT_TOKEN,
    SUBJECT_TOKEN_NAME,
    TOKEN_AUTH_UNAVAILABLE,
    TOKEN_JWS_TRAP,
    TOKEN_UNAVAILABLE,
    TOKEN_UNKNOWN,
    UNAVAILABLE_RETRY_AFTER,
    WHOAMI_PROTOCOL_NAME,
    Whoami,
    conformance_grant_keys,
)
from vgi_rpc.grants import GRANT_TOKEN_PREFIX, GrantKeys, grant_key_id, mint_grant_token, verify_grant_token
from vgi_rpc.metadata import ERROR_KIND_KEY, LOG_LEVEL_KEY
from vgi_rpc.rpc._token_identity import Identity, IssuedGrant

if TYPE_CHECKING:
    import httpx2

pytestmark = pytest.mark.timeout(30)

_FIXTURE = "conformance_http_grant_port"
_ARROW = "application/vnd.apache.arrow.stream"


def _port(request: pytest.FixtureRequest) -> int:
    """Return the grant worker's port, or skip loudly naming the fixture."""
    try:
        return int(request.getfixturevalue(_FIXTURE))
    except pytest.FixtureLookupError:
        pytest.skip(
            f"runner provides no {_FIXTURE!r}: an HTTP worker hosting vgi_rpc.Identity.v1 with the fixture "
            f"resolver and grant keys, no mint hook, and {WHOAMI_PROTOCOL_NAME} "
            f"(IDENTITY_CONFORMANCE_FIXTURE.md §10). A skip here is an open deliverable, not a pass."
        )


def _body(protocol: type, method: str, kwargs: dict[str, Any]) -> bytes:
    from vgi_rpc.rpc import rpc_methods
    from vgi_rpc.rpc._wire import _write_request

    info = rpc_methods(protocol)[method]
    buf = BytesIO()
    _write_request(buf, method, info.params_schema, kwargs, protocol=str(vars(protocol)["protocol_name"]))
    return buf.getvalue()


def _post(port: int, protocol: str, method: str, body: bytes, headers: dict[str, str]) -> httpx2.Response:
    import httpx2

    response = None
    for prefix in ("", "/vgi"):
        response = httpx2.post(
            f"http://127.0.0.1:{port}{prefix}/{protocol}/{method}",
            content=body,
            headers={"content-type": _ARROW, **headers},
            timeout=10.0,
        )
        if response.status_code != 404:
            return response
    assert response is not None
    return response


def _result(response: httpx2.Response) -> tuple[Any, str | None]:
    """Return ``(result value, error_kind)`` from a 200 Arrow body."""
    assert response.status_code == 200, f"HTTP {response.status_code}: {response.content[:200]!r}"
    reader = ipc.open_stream(BytesIO(response.content))
    value: Any = None
    while True:
        try:
            batch, metadata = reader.read_next_batch_with_custom_metadata()
        except StopIteration:
            break
        meta = {k.decode(): v.decode() for k, v in metadata.to_dict().items()} if metadata else {}
        if meta.get(LOG_LEVEL_KEY.decode()) == "EXCEPTION":
            return None, meta.get(ERROR_KIND_KEY.decode(), "")
        if batch.num_rows and batch.schema.names == ["result"]:
            value = batch.column(0)[0].as_py()
    return value, None


def _whoami(port: int, headers: dict[str, str]) -> httpx2.Response:
    return _post(port, WHOAMI_PROTOCOL_NAME, "whoami", _body(Whoami, "whoami", {}), headers)


def _who(port: int, token: str) -> dict[str, Any]:
    """Authenticate with *token* as a bearer and return the decoded whoami."""
    value, kind = _result(_whoami(port, {"Authorization": f"Bearer {token}"}))
    assert kind is None, f"whoami raised {kind!r}"
    decoded: dict[str, Any] = json.loads(value)
    return decoded


def _mint(port: int, *, scopes: list[str], purpose: str, ttl: int) -> IssuedGrant:
    """Ask the worker to mint, as a freshly authenticated minter."""
    body = _body(Identity, "issue_grant", {"purpose": purpose, "scopes": scopes, "ttl_seconds": ttl})
    headers = {PRINCIPAL_HEADER: MINTER_PRINCIPAL, AUTH_TIME_HEADER: str(time.time() - 60)}
    value, kind = _result(_post(port, Identity.protocol_name, "issue_grant", body, headers))
    assert kind is None, f"issue_grant refused with {kind!r}; the grant worker must mint sealed grants"
    grant = IssuedGrant.deserialize_from_bytes(value)
    assert isinstance(grant, IssuedGrant)
    return grant


def _reference_token(key: bytes, *, audience: str = GRANT_AUDIENCE, ttl: int = 600, now: int | None = None) -> str:
    keys = GrantKeys(keys=(key,), audience=audience, max_ttl_seconds=GRANT_MAX_TTL)
    token, _ = mint_grant_token(
        keys, principal=SUBJECT_PRINCIPAL + "-grant", scopes=["read"], purpose="reference", ttl_seconds=ttl, now=now
    )
    return token


def _assert_refused(port: int, token: str, label: str, reason: str = "invalid_credential") -> None:
    response = _whoami(port, {"Authorization": f"Bearer {token}"})
    assert response.status_code == 401, (
        f"a {label} grant was answered HTTP {response.status_code}. It must be 401. The fixture resolver "
        f"resolves almost anything, so a 200 means the grant fell through to resolve_token -- a grant-shaped "
        f"token must stop at the grant verifier. Body: {response.content[:200]!r}"
    )
    got = response.headers.get("VGI-Auth-Reason")
    assert got == reason, f"a {label} grant carried VGI-Auth-Reason={got!r}, expected {reason!r}"


class TestSealedGrants:
    """The worker mints sealed grants and accepts them back as bearers."""

    def test_a_minted_grant_is_accepted_as_a_bearer(self, request: pytest.FixtureRequest) -> None:
        """The loop closes: issue_grant's token authenticates, as the grant."""
        port = _port(request)
        grant = _mint(port, scopes=["read", "write"], purpose="nightly", ttl=600)
        assert grant.token.startswith(GRANT_TOKEN_PREFIX), f"not a sealed grant: {grant.token[:20]!r}"
        who = _who(port, grant.token)
        assert who == {
            "authenticated": True,
            "claims": {"grant_id": grant.grant_id, "purpose": "nightly", "scopes": ["read", "write"]},
            "domain": "grant",
            "principal": MINTER_PRINCIPAL,
        }, f"grant-authenticated AuthContext was {who}"

    def test_the_minted_grant_matches_the_format(self, request: pytest.FixtureRequest) -> None:
        """The port's token opens under the published key with the reference verifier.

        Also pins the key id (the current key mints) and the lifetime cap.
        """
        port = _port(request)
        grant = _mint(port, scopes=[], purpose="cap", ttl=10_000_000)
        claims = verify_grant_token(conformance_grant_keys(), grant.token)
        assert claims.principal == MINTER_PRINCIPAL
        assert claims.expires_at - claims.issued_at == GRANT_MAX_TTL, "the requested lifetime must be capped"
        assert float(claims.expires_at) == grant.expires_at
        assert claims.grant_id == grant.grant_id
        from vgi_rpc.grants import _b64url_strict

        assert _b64url_strict(grant.token[len(GRANT_TOKEN_PREFIX) :])[:8] == grant_key_id(GRANT_KEY_CURRENT), (
            "the first configured key mints"
        )

    def test_a_grant_cannot_mint_a_grant(self, request: pytest.FixtureRequest) -> None:
        """No auth_time in a grant-authenticated context, so issue_grant refuses."""
        port = _port(request)
        grant = _mint(port, scopes=[], purpose="parent", ttl=600)
        body = _body(Identity, "issue_grant", {"purpose": "child", "scopes": [], "ttl_seconds": 60})
        _, kind = _result(
            _post(port, Identity.protocol_name, "issue_grant", body, {"Authorization": f"Bearer {grant.token}"})
        )
        assert kind == "stale_auth", (
            f"a grant-authenticated caller asked for a grant and got kind={kind!r}. It must be stale_auth: a "
            f"grant carries no auth_time, which is the one rule that stops a grant lineage escaping the IdP."
        )

    @pytest.mark.parametrize("key", [GRANT_KEY_CURRENT, GRANT_KEY_PREVIOUS], ids=["current", "previous"])
    def test_reference_minted_grants_are_accepted(self, request: pytest.FixtureRequest, key: bytes) -> None:
        """The verifier agrees with the reference minter, under both configured keys."""
        who = _who(_port(request), _reference_token(key))
        assert (who["domain"], who["principal"]) == ("grant", SUBJECT_PRINCIPAL + "-grant"), who

    def test_within_clock_skew_is_accepted(self, request: pytest.FixtureRequest) -> None:
        """Expired 30 seconds ago, inside the 60-second skew allowance."""
        token = _reference_token(GRANT_KEY_CURRENT, ttl=60, now=int(time.time()) - 90)
        assert _who(_port(request), token)["domain"] == "grant"


class TestSealedGrantRejections:
    """Every bad grant is 401 and never reaches resolve_token."""

    def test_tampered(self, request: pytest.FixtureRequest) -> None:
        """One character inside the ciphertext changed: the tag check refuses it."""
        token = _reference_token(GRANT_KEY_CURRENT)
        mid = len(token) - 10
        tampered = token[:mid] + ("A" if token[mid] != "A" else "B") + token[mid + 1 :]
        _assert_refused(_port(request), tampered, "tampered")

    def test_expired(self, request: pytest.FixtureRequest) -> None:
        """Authentic and past expiry beyond the skew: expired_credential."""
        token = _reference_token(GRANT_KEY_CURRENT, ttl=60, now=int(time.time()) - 3000)
        _assert_refused(_port(request), token, "expired", reason="expired_credential")

    def test_wrong_key(self, request: pytest.FixtureRequest) -> None:
        """Sealed with a key the worker does not hold."""
        _assert_refused(_port(request), _reference_token(GRANT_KEY_STRANGER), "wrong-key")

    def test_wrong_audience(self, request: pytest.FixtureRequest) -> None:
        """The right key, another deployment's audience."""
        _assert_refused(_port(request), _reference_token(GRANT_KEY_CURRENT, audience="elsewhere"), "wrong-audience")

    def test_padded(self, request: pytest.FixtureRequest) -> None:
        """Base64url with padding is not the canonical spelling."""
        _assert_refused(_port(request), _reference_token(GRANT_KEY_CURRENT) + "=", "padded")


class TestGrantPrefixRouting:
    """Only ``vgig1.`` reaches the grant verifier; nothing else does."""

    def test_an_unknown_prefix_never_reaches_the_grant_verifier(self, request: pytest.FixtureRequest) -> None:
        """``vgig2.`` and a stripped grant are opaque bearers -- resolve_token answers them."""
        port = _port(request)
        body = _reference_token(GRANT_KEY_CURRENT)[len(GRANT_TOKEN_PREFIX) :]
        for token in ("vgig2." + body, body):
            who = _who(port, token)
            assert who["domain"] == "token", (
                f"{token[:10]!r}... was authenticated with domain {who['domain']!r}. Only the exact "
                f"'{GRANT_TOKEN_PREFIX}' prefix routes to the grant verifier."
            )


class TestResolveTokenBearer:
    """A worker's resolve_token backs bearer authentication."""

    def test_a_resolved_credential_authenticates(self, request: pytest.FixtureRequest) -> None:
        """Resolved: authenticated as the identity, domain "token"."""
        who = _who(_port(request), SUBJECT_TOKEN)
        assert who == {
            "authenticated": True,
            "claims": {"token_name": SUBJECT_TOKEN_NAME},
            "domain": "token",
            "principal": SUBJECT_PRINCIPAL,
        }, who

    def test_an_unresolved_credential_is_401(self, request: pytest.FixtureRequest) -> None:
        """None from the hook falls through, and nothing else accepts it."""
        response = _whoami(_port(request), {"Authorization": f"Bearer {TOKEN_UNKNOWN}"})
        assert response.status_code == 401, f"HTTP {response.status_code}"

    @pytest.mark.parametrize(
        ("token", "retry_after"),
        [(TOKEN_UNAVAILABLE, UNAVAILABLE_RETRY_AFTER), (TOKEN_AUTH_UNAVAILABLE, AUTH_UNAVAILABLE_RETRY_AFTER)],
        ids=["identity-unavailable", "auth-unavailable"],
    )
    def test_an_outage_is_503_never_401(self, request: pytest.FixtureRequest, token: str, retry_after: int) -> None:
        """Not knowable is not a rejection: 503 with the hook's Retry-After."""
        response = _whoami(_port(request), {"Authorization": f"Bearer {token}"})
        assert response.status_code == 503, (
            f"a resolver outage answered HTTP {response.status_code}. A 401 here makes every caller "
            f"re-authenticate against a service that is merely down."
        )
        assert response.headers.get("Retry-After") == str(retry_after), response.headers.get("Retry-After")

    def test_a_jws_is_not_resolved(self, request: pytest.FixtureRequest) -> None:
        """The resolver would answer for it; a JWS must not be routed onward."""
        response = _whoami(_port(request), {"Authorization": f"Bearer {TOKEN_JWS_TRAP}"})
        assert response.status_code == 401, f"a JWS-shaped bearer reached resolve_token (HTTP {response.status_code})"

    def test_no_credential_stays_anonymous(self, request: pytest.FixtureRequest) -> None:
        """Adding bearer acceptance does not make an unauthenticated probe fail."""
        value, kind = _result(_whoami(_port(request), {}))
        assert kind is None
        assert json.loads(value)["authenticated"] is False


__all__ = [
    "TestGrantPrefixRouting",
    "TestResolveTokenBearer",
    "TestSealedGrantRejections",
    "TestSealedGrants",
]
