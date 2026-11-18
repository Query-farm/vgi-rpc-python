"""Location delegation in the Python OAuth client (the "Pass Holder").

A mock Desk Address, Front Desk and Field Worker behind one
``httpx2.MockTransport``: the client is given the Desk Address, refreshes with
RFC 8707 ``resource``, follows ``vgi_location`` to the Field Address, and
answers a Field Worker 401 with one refresh.
"""

from __future__ import annotations

from urllib.parse import parse_qs

import httpx2
import pytest

from vgi_rpc.http._oauth_client import OAuthTokenError, VgiOAuthAuth
from vgi_rpc.http._oauth_delegation import (
    DelegationError,
    canonical_resource,
    check_field_address,
    field_address_patterns,
    rewrite_to_field,
)

ISSUER = "https://auth.queryfarm.cloud"
DESK = "https://desk.queryfarm.cloud/d/dep_mvt"
FIELD = "https://k7f2q9xw.field.queryfarm.cloud"
METADATA = "https://desk.queryfarm.cloud/.well-known/oauth-protected-resource/d/dep_mvt"


class FrontDesk:
    """Mock Desk Address + authorization server + Field Worker."""

    def __init__(
        self, *, location: str = FIELD, issuer: str = ISSUER, resource: str = DESK, patterns: object = None
    ) -> None:
        """Configure what the mock server answers."""
        self.location = location
        self.patterns = patterns
        self.issuer = issuer
        self.resource = resource
        self.refreshes: list[dict[str, list[str]]] = []
        self.field_calls: list[str] = []
        self.current_pass = ""
        self.generation = 0

    def handler(self, request: httpx2.Request) -> httpx2.Response:
        """Answer one request as the Desk Address, Front Desk or Field Worker."""
        url = str(request.url)
        if url.startswith(DESK):
            return httpx2.Response(401, headers={"WWW-Authenticate": f'Bearer resource_metadata="{METADATA}"'})
        if url == METADATA:
            return httpx2.Response(200, json={"resource": self.resource, "authorization_servers": [ISSUER]})
        if url == f"{ISSUER}/.well-known/openid-configuration":
            return httpx2.Response(
                200,
                json={
                    "issuer": self.issuer,
                    "authorization_endpoint": f"{ISSUER}/authorize",
                    "token_endpoint": f"{ISSUER}/token",
                    "resource_indicators_supported": True,
                    **({} if self.patterns is None else {"vgi_field_address_patterns": self.patterns}),
                },
            )
        if url == f"{ISSUER}/token":
            form = parse_qs(request.content.decode())
            self.refreshes.append(form)
            if form.get("resource") != [DESK] or "scope" in form or form.get("client_id") != ["vgi-duckdb"]:
                return httpx2.Response(400, json={"error": "invalid_target", "error_description": "bad request"})
            self.generation += 1
            self.current_pass = f"pass-{self.generation}"
            return httpx2.Response(
                200,
                json={
                    "access_token": self.current_pass,
                    "token_type": "Bearer",
                    "expires_in": 900,
                    "refresh_token": f"ort1.r{self.generation}",
                    "vgi_location": self.location,
                },
            )
        if url.startswith(self.location):
            self.field_calls.append(url)
            if request.headers.get("Authorization") != f"Bearer {self.current_pass}":
                # A hostile challenge pointing elsewhere must be ignored.
                return httpx2.Response(
                    401, headers={"WWW-Authenticate": 'Bearer resource_metadata="https://evil.example/md"'}
                )
            return httpx2.Response(200, json={"ok": True, "host": request.headers.get("Host")})
        return httpx2.Response(404)


def _client(desk: FrontDesk, refresh_token: str = "ort1.seed") -> tuple[httpx2.Client, VgiOAuthAuth]:
    transport = httpx2.MockTransport(desk.handler)
    auth = VgiOAuthAuth(base_url=DESK, refresh_token=refresh_token, transport=transport)
    return httpx2.Client(auth=auth, transport=transport), auth


def test_refresh_routes_to_the_field_address_with_resource() -> None:
    """A refresh sends resource, and requests go to the Field Address."""
    desk = FrontDesk()
    client, _ = _client(desk)
    r = client.post(f"{DESK}/vgi.v3/catalog_attach")
    assert r.status_code == 200
    assert desk.field_calls == [f"{FIELD}/vgi.v3/catalog_attach"]
    assert r.json()["host"] == "k7f2q9xw.field.queryfarm.cloud"
    assert desk.refreshes[0]["grant_type"] == ["refresh_token"]
    assert desk.refreshes[0]["refresh_token"] == ["ort1.seed"]
    # Later calls go straight to the Field Address with the Pass.
    client.post(f"{DESK}/vgi.v3/catalog_contents")
    assert desk.field_calls[-1] == f"{FIELD}/vgi.v3/catalog_contents"
    assert len(desk.refreshes) == 1


def test_a_field_401_refreshes_once_and_ignores_its_challenge() -> None:
    """A Field Worker 401 triggers one refresh; its challenge is ignored."""
    desk = FrontDesk()
    client, _ = _client(desk)
    client.post(f"{DESK}/vgi.v3/catalog_attach")
    desk.current_pass = "rotated-elsewhere"  # the worker no longer accepts our Pass
    desk.generation = 5
    r = client.post(f"{DESK}/vgi.v3/catalog_contents")
    assert r.status_code == 200
    assert len(desk.refreshes) == 2
    assert desk.refreshes[1]["refresh_token"] == ["ort1.r1"]


def test_a_location_outside_the_allowed_hosts_is_refused() -> None:
    """A vgi_location outside the allowed hosts fails the sign-in."""
    desk = FrontDesk(location="https://evil.example")
    client, _ = _client(desk)
    with pytest.raises(OAuthTokenError, match="not an allowed delegation host"):
        client.post(f"{DESK}/vgi.v3/catalog_attach")


def test_an_unknown_issuer_never_receives_the_refresh_token() -> None:
    """Issuer pinning: an unknown issuer gets no credential."""
    desk = FrontDesk(issuer="https://auth.evil.example")
    client, _ = _client(desk)
    with pytest.raises(DelegationError):
        client.post(f"{DESK}/vgi.v3/catalog_attach")
    assert desk.refreshes == []


def test_metadata_for_another_resource_doesnt_delegate() -> None:
    """Metadata naming another resource leaves delegation off."""
    desk = FrontDesk(resource="https://desk.queryfarm.cloud/d/someone_else")
    client, _ = _client(desk)
    # Not delegated: the plain refresh path runs, without resource (which this mock refuses).
    with pytest.raises(OAuthTokenError):
        client.post(f"{DESK}/vgi.v3/catalog_attach")
    assert "resource" not in desk.refreshes[0]
    assert desk.field_calls == []


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://DESK.queryfarm.cloud:443/d/x/", "https://desk.queryfarm.cloud/d/x"),
        ("https://desk.queryfarm.cloud/d/x?y=1", None),
        ("https://desk.queryfarm.cloud/d/../x", None),
        ("http://desk.queryfarm.cloud/d/x", None),
    ],
)
def test_canonical_resource(raw: str, expected: str | None) -> None:
    """The spec's canonical resource form."""
    assert canonical_resource(raw) == expected


@pytest.mark.parametrize(
    "location",
    [
        "http://k.field.queryfarm.cloud",
        "https://k.field.queryfarm.cloud/x",
        "https://10.0.0.1",
        "https://a@k.field.queryfarm.cloud",
    ],
)
def test_field_address_rules(location: str) -> None:
    """Field Addresses must be bare https origins on allowed hosts."""
    with pytest.raises(DelegationError):
        check_field_address(ISSUER, location)


@pytest.mark.parametrize(
    ("location", "ok"),
    [
        ("https://k7f2q9xw.field.queryfarm.cloud", True),
        ("https://abc.wt12.field.queryfarm.cloud:8443", True),
        ("https://o-3to7u3vuka9q2xw1mzp4.fly.dev", True),
        ("https://fly.dev", False),
        ("https://a.b.fly.dev", False),
        ("https://evil-fly.dev", False),
        ("https://app.fly.dev.evil.com", False),
        ("https://field.queryfarm.cloud", False),
        ("https://a..field.queryfarm.cloud", False),
        ("https://evil.example.com", False),
    ],
)
def test_orchard_field_hosts(location: str, ok: bool) -> None:
    """Orchard may hand out its own field hosts or exactly one label under fly.dev."""
    for issuer in (ISSUER, "https://auth.staging.queryfarm.cloud"):
        if ok:
            assert check_field_address(issuer, location) == location
        else:
            with pytest.raises(DelegationError):
                check_field_address(issuer, location)


def test_published_patterns_admit_a_new_provider() -> None:
    """An issuer's published patterns admit a host the built-in list doesn't know."""
    desk = FrontDesk(location="https://w-abc123.run.app", patterns=["*.run.app", "*.fly.dev"])
    client, _ = _client(desk)
    assert client.post(f"{DESK}/vgi.v3/catalog_attach").status_code == 200
    assert desk.field_calls == ["https://w-abc123.run.app/vgi.v3/catalog_attach"]


def test_published_patterns_replace_the_defaults() -> None:
    """Published patterns are authoritative: a default host they omit is refused."""
    desk = FrontDesk(patterns=["*.fly.dev"])
    client, _ = _client(desk)
    with pytest.raises(OAuthTokenError, match="not an allowed delegation host"):
        client.post(f"{DESK}/vgi.v3/catalog_attach")


@pytest.mark.parametrize(
    "patterns",
    [
        ["*.com"],
        ["**.dev"],
        ["*"],
        ["evil.example.com"],
        ["*.run.app", "*.com"],
        ["*.Run.app"],
        ["*.-bad.example"],
        "*.run.app",
        [],
        [1],
        ["*.x.example"] * 17,
    ],
)
def test_unusable_published_patterns_fall_back_to_the_defaults(patterns: object) -> None:
    """Any invalid entry rejects the whole list; the built-in defaults then apply."""
    assert field_address_patterns(patterns) is None
    ok = FrontDesk(patterns=patterns)
    client, _ = _client(ok)
    assert client.post(f"{DESK}/vgi.v3/catalog_attach").status_code == 200
    evil = FrontDesk(location="https://w-abc123.run.app", patterns=patterns)
    client, _ = _client(evil)
    with pytest.raises(OAuthTokenError, match="not an allowed delegation host"):
        client.post(f"{DESK}/vgi.v3/catalog_attach")


def test_field_address_patterns_shape() -> None:
    """Valid lists come back as tuples, one or two kinds of wildcard."""
    assert field_address_patterns(["**.field.queryfarm.cloud", "*.fly.dev"]) == (
        "**.field.queryfarm.cloud",
        "*.fly.dev",
    )


def test_loopback_only_with_the_test_escape_hatch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Loopback delegation needs the test-only environment switch."""
    with pytest.raises(DelegationError):
        check_field_address("http://127.0.0.1:9000", "http://127.0.0.1:9001")
    monkeypatch.setenv("VGI_TEST_ALLOW_LOOPBACK_DELEGATION", "1")
    assert check_field_address("http://127.0.0.1:9000", "http://127.0.0.1:9001/") == "http://127.0.0.1:9001"


def test_rewrite_to_field() -> None:
    """Desk URLs map onto the Field Address; others are left alone."""
    assert rewrite_to_field(f"{DESK}/vgi.v3/m", DESK, FIELD) == f"{FIELD}/vgi.v3/m"
    assert rewrite_to_field("https://other.example/x", DESK, FIELD) is None
