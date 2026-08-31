# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Fast safety checks for the opt-in real-Tailnet harness."""

from __future__ import annotations

import json

from tests.tailnet.service import TailnetEvidenceImpl
from vgi_rpc import (
    AuthContext,
    CallContext,
    IdentityAssurance,
    PeerEvidenceSet,
    PeerIdentity,
    PeerIdentityResult,
    SubjectKind,
    SubjectStability,
)


def test_tailnet_snapshot_is_deterministic_and_redacted() -> None:
    """The live probe cannot leak profiles, principals, addresses, or capability bodies."""
    identity = PeerIdentity(
        provider="tailscale",
        evidence_source="localapi",
        assurance=IdentityAssurance.LOCAL_DAEMON,
        issuer="tailnet:test",
        transport="tcp",
        subject_kind=SubjectKind.TAGGED_NODE,
        subject_key="node:stable-secret-node-id",
        subject_stability=SubjectStability.STABLE,
        subject_verified=True,
        attributes={
            "tags": ["tag:vgi-ci-client"],
            "node_name": "private-device.example.ts.net",
            "user_login": "private-user@example.com",
            "capability_target": {"kind": "destination_ip", "value": "100.64.0.1"},
        },
        capabilities={"query.farm/cap/vgi-test": [{"secret": "capability-body"}]},
        capabilities_verified=True,
        source_address="100.64.0.2:4242",
    )
    evidence = PeerEvidenceSet.from_results((PeerIdentityResult.available(identity),))
    context = CallContext(
        AuthContext("tailscale", True, "canonical-private-principal"),
        lambda *_args, **_kwargs: None,
        peer_evidence=evidence,
    )

    first = TailnetEvidenceImpl().snapshot(context)
    second = TailnetEvidenceImpl().snapshot(context)

    assert first == second
    assert "stable-secret-node-id" not in first
    assert "canonical-private-principal" not in first
    assert "private-device" not in first
    assert "private-user" not in first
    assert "100.64.0.2" not in first
    assert "capability-body" not in first
    payload = json.loads(first)
    assert payload["provider_status"] == {"tailscale": "available"}
    assert payload["identities"][0]["tags"] == ["tag:vgi-ci-client"]
    assert payload["identities"][0]["capability_names"] == ["query.farm/cap/vgi-test"]
