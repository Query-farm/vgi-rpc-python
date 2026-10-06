# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Sealed grants (IDENTITY_V1_SPEC.md §9) in the reference implementation.

The vector file is what every port's tests consume; these tests are what keep
the reference itself pinned to it.
"""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any

import falcon
import falcon.testing
import pytest

from vgi_rpc.errors import AuthUnavailableError
from vgi_rpc.grants import (
    GRANT_TOKEN_PREFIX,
    GrantInvalidError,
    GrantKeys,
    mint_grant_token,
    sealed_mint_grant,
    verify_grant_token,
)
from vgi_rpc.http import (
    compose_identity_authenticate,
    grant_authenticate,
    resolve_token_authenticate,
)
from vgi_rpc.http._unauthorized import declare_proxy_headers
from vgi_rpc.rpc import AuthContext, CallContext, RpcServer
from vgi_rpc.rpc._token_identity import IdentityImpl, IdentityUnavailableError, StaleAuthError, TokenIdentity

VECTORS = json.loads(
    (Path(__file__).resolve().parents[1] / "vgi_rpc" / "conformance" / "grant_token_vectors.json").read_text()
)
KEY = bytes(range(32))


def _keys(case: dict[str, Any]) -> GrantKeys:
    d = VECTORS["defaults"]
    return GrantKeys(
        keys=tuple(base64.b64decode(k) for k in case.get("verify_keys_b64", d["verify_keys_b64"])),
        audience=case.get("audience", d["audience"]),
        max_ttl_seconds=case.get("max_ttl_seconds", d["max_ttl_seconds"]),
        clock_skew_seconds=d["clock_skew_seconds"],
    )


@pytest.mark.parametrize("case", VECTORS["mint"], ids=lambda c: c["name"])
def test_the_reference_mints_each_vector_exactly(case: dict[str, Any]) -> None:
    """Fixed key, nonce, clock and grant id produce exactly the pinned token."""
    keys = GrantKeys(
        keys=(base64.b64decode(case["minting_key_b64"]),),
        audience=case["audience"],
        max_ttl_seconds=case["max_ttl_seconds"],
    )
    req = case["request"]
    token, claims = mint_grant_token(
        keys,
        principal=req["principal"],
        scopes=req["scopes"],
        purpose=req["purpose"],
        ttl_seconds=req["ttl_seconds"],
        grant_id=req["grant_id"],
        now=case["now"],
        nonce=bytes.fromhex(case["nonce_hex"]),
    )
    assert token == case["token"]
    assert claims.expires_at == case["claims"]["expires_at"]
    assert verify_grant_token(keys, token, now=case["now"]).principal == req["principal"]


@pytest.mark.parametrize("case", VECTORS["accept"], ids=lambda c: c["name"])
def test_accept_vectors(case: dict[str, Any]) -> None:
    """Rotation and the skew allowance."""
    claims = verify_grant_token(_keys(case), case["token"], now=case.get("now", VECTORS["defaults"]["now"]))
    assert claims.principal


@pytest.mark.parametrize("case", VECTORS["reject"], ids=lambda c: c["name"])
def test_reject_vectors(case: dict[str, Any]) -> None:
    """Every rejection is one type; ``expired`` only once authenticity is proven."""
    with pytest.raises(GrantInvalidError) as excinfo:
        verify_grant_token(_keys(case), case["token"], now=case.get("now", VECTORS["defaults"]["now"]))
    assert excinfo.value.expired is case["expired"]


@pytest.mark.parametrize(
    "keys",
    [[], ["not base64!"], [base64.b64encode(b"short").decode()], [base64.b64encode(KEY).decode()] * 2],
    ids=["none", "garbage", "short", "duplicate"],
)
def test_a_malformed_key_refuses_to_start(keys: list[str]) -> None:
    """Parse errors are construction errors, never a server running with a misread key."""
    with pytest.raises(ValueError):
        GrantKeys.parse(keys)


def test_env_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset means off; set means on; malformed refuses to construct a server."""
    from vgi_rpc.conformance.secondary import Secondary, SecondaryImpl

    monkeypatch.delenv("VGI_RPC_GRANT_KEYS", raising=False)
    assert RpcServer(Secondary, SecondaryImpl()).identity is None
    monkeypatch.setenv("VGI_RPC_GRANT_KEYS", base64.b64encode(KEY).decode())
    monkeypatch.setenv("VGI_RPC_GRANT_MAX_TTL_SECONDS", "120")
    server = RpcServer(Secondary, SecondaryImpl())
    assert server.grant_keys is not None and server.grant_keys.max_ttl_seconds == 120
    assert server.identity is not None and server.identity.offered_methods() == frozenset({"issue_grant"})
    monkeypatch.setenv("VGI_RPC_GRANT_KEYS", "bm90LTMyLWJ5dGVz")
    with pytest.raises(ValueError, match="32"):
        RpcServer(Secondary, SecondaryImpl())


def _ctx(auth: AuthContext) -> CallContext:
    return CallContext(auth=auth, implementation=None, transport_metadata={}, emit_client_log=lambda *a, **k: None)


def test_sealed_minting_and_ttl_cap() -> None:
    """The built-in minter caps the lifetime and refuses a non-positive one."""
    keys = GrantKeys(keys=(KEY,), max_ttl_seconds=100)
    grant = sealed_mint_grant(keys)("alice", "p", ["s"], 10_000)
    claims = verify_grant_token(keys, grant.token)
    assert claims.expires_at - claims.issued_at == 100
    from vgi_rpc.rpc._token_identity import GrantRefusedError

    with pytest.raises(GrantRefusedError):
        sealed_mint_grant(keys)("alice", "p", [], 0)


def _app(authenticate: Any) -> falcon.testing.TestClient:
    class _Who:
        def on_get(self, req: falcon.Request, resp: falcon.Response) -> None:
            auth = authenticate(req)
            resp.media = {"domain": auth.domain, "principal": auth.principal, "claims": dict(auth.claims)}

    app = falcon.App()
    app.add_route("/who", _Who())
    return falcon.testing.TestClient(app)


def test_a_grant_authenticated_caller_cannot_mint() -> None:
    """Grant AuthContext has no auth_time, so IdentityImpl.issue_grant refuses: grants never mint grants."""
    keys = GrantKeys(keys=(KEY,))
    token, _ = mint_grant_token(keys, principal="alice", scopes=["r"], purpose="p", ttl_seconds=60)
    req = falcon.testing.create_req(headers={"Authorization": f"Bearer {token}"})
    auth = grant_authenticate(keys)(req)
    assert (auth.domain, auth.principal) == ("grant", "alice")
    assert "auth_time" not in auth.claims
    identity = IdentityImpl(grant_keys=keys)
    with pytest.raises(StaleAuthError):
        identity.issue_grant("child", [], 60, _ctx(auth))
    # The positive control: a fresh human credential does mint.
    fresh = AuthContext(domain="jwt", authenticated=True, principal="alice", claims={"auth_time": time.time()})
    assert identity.issue_grant("child", [], 60, _ctx(fresh)).token.startswith(GRANT_TOKEN_PREFIX)


def test_prefix_routing_in_both_directions() -> None:
    """Non-prefix falls through (ValueError); a bad grant stops the chain (PermissionError)."""
    keys = GrantKeys(keys=(KEY,))
    calls: list[str] = []

    def resolver(token: str) -> TokenIdentity | None:
        calls.append(token)
        return TokenIdentity(principal="resolved")

    auth = compose_identity_authenticate(None, grant_keys=keys, resolve_token=resolver)
    assert auth is not None
    good, _ = mint_grant_token(keys, principal="alice", scopes=[], purpose="", ttl_seconds=60)
    bad = good[:-12] + ("A" if good[-12] != "A" else "B") + good[-11:]
    assert auth(falcon.testing.create_req(headers={"Authorization": f"Bearer {good}"})).domain == "grant"
    with pytest.raises(PermissionError):
        auth(falcon.testing.create_req(headers={"Authorization": f"Bearer {bad}"}))
    assert auth(falcon.testing.create_req(headers={"Authorization": "Bearer opaque"})).principal == "resolved"
    assert calls == ["opaque"], "neither grant reached the resolver"
    assert auth(falcon.testing.create_req()).authenticated is False


def test_resolve_token_outage_maps_to_auth_unavailable() -> None:
    """Both outage spellings become AuthUnavailableError (503), keeping the hint."""

    def identity_down(token: str) -> TokenIdentity | None:
        raise IdentityUnavailableError("down", retry_after=5)

    def auth_down(token: str) -> TokenIdentity | None:
        raise AuthUnavailableError("down", retry_after=7)

    req = falcon.testing.create_req(headers={"Authorization": "Bearer x"})
    for hook, hint in ((identity_down, 5), (auth_down, 7)):
        with pytest.raises(AuthUnavailableError) as excinfo:
            resolve_token_authenticate(hook)(req)
        assert excinfo.value.retry_after == hint


def test_a_proxy_dependent_authenticator_refuses_composition() -> None:
    """OR-ing a grant beside a proxy-proof requirement would bypass it: refuse to start."""

    def gated(req: falcon.Request) -> AuthContext:
        raise ValueError("no")

    declare_proxy_headers(gated, "X-Proxy-Proof")
    with pytest.raises(ValueError, match="proxy"):
        compose_identity_authenticate(gated, grant_keys=GrantKeys(keys=(KEY,)), resolve_token=None)


def test_make_wsgi_app_accepts_a_minted_grant_end_to_end() -> None:
    """Mint through Identity.v1 over HTTP, then present it as a bearer."""
    from vgi_rpc.conformance.identity_fixture import Whoami, WhoamiImpl
    from vgi_rpc.http import http_connect, make_sync_client
    from vgi_rpc.rpc._token_identity import Identity

    keys = GrantKeys(keys=(KEY,))

    def principal_header(req: falcon.Request) -> AuthContext:
        principal = req.get_header("X-P")
        if not principal:
            raise ValueError("no principal header")
        return AuthContext(domain="test", authenticated=True, principal=principal, claims={"auth_time": time.time()})

    server = RpcServer(Whoami, WhoamiImpl(), identity=IdentityImpl(grant_keys=keys), grant_keys=None)
    client = make_sync_client(server, authenticate=principal_header, default_headers={"X-P": "alice"})
    with http_connect(Identity, client=client) as ident:
        grant = ident.issue_grant(purpose="p", scopes=["r"], ttl_seconds=60)
    bearer = make_sync_client(
        server, authenticate=principal_header, default_headers={"Authorization": f"Bearer {grant.token}"}
    )
    with http_connect(Whoami, client=bearer) as who:
        decoded = json.loads(who.whoami())
    assert (decoded["domain"], decoded["principal"]) == ("grant", "alice")


def test_whoami_hash_is_pinned() -> None:
    """The grant worker's probe protocol hashes to the spec's digest."""
    from vgi_rpc.conformance.identity_fixture import WHOAMI_PROTOCOL_HASH, WHOAMI_PROTOCOL_NAME, Whoami
    from vgi_rpc.rpc import rpc_methods
    from vgi_rpc.rpc._protocol_hash import compute_protocol_hash

    assert compute_protocol_hash(WHOAMI_PROTOCOL_NAME, rpc_methods(Whoami)) == WHOAMI_PROTOCOL_HASH
