# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Small RPC surface used by the real-Tailnet integration gate."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any, Protocol

from vgi_rpc import CallContext


class TailnetEvidenceService(Protocol):
    """Return a redacted view of the connection's peer evidence."""

    def snapshot(self) -> str:
        """Return the redacted identity snapshot as deterministic JSON."""
        ...


def _fingerprint(value: str | None) -> str | None:
    """Make stable comparison possible without logging a principal."""
    return hashlib.sha256(value.encode()).hexdigest() if value else None


class TailnetEvidenceImpl:
    """Expose only fields needed to assert the integration contract."""

    def snapshot(self, ctx: CallContext) -> str:
        """Build a redacted, deterministic view of the active call context."""
        identities: list[dict[str, Any]] = []
        for identity in ctx.peer_evidence.identities:
            target = identity.attributes.get("capability_target")
            safe_target = dict(target) if isinstance(target, Mapping) else target
            if isinstance(safe_target, dict) and safe_target.get("kind") == "destination_ip":
                safe_target.pop("value", None)
            tags = identity.attributes.get("tags", ())
            identities.append(
                {
                    "provider": identity.provider,
                    "evidence_source": identity.evidence_source,
                    "assurance": identity.assurance.value,
                    "transport": identity.transport,
                    "subject_kind": identity.subject_kind.value,
                    "subject_stability": identity.subject_stability.value,
                    "subject_verified": identity.subject_verified,
                    "subject_fingerprint": _fingerprint(identity.subject_key),
                    "tags": sorted(str(tag) for tag in tags),
                    "capability_names": sorted(identity.capabilities),
                    "capabilities_verified": identity.capabilities_verified,
                    "capability_target": safe_target,
                    "source_present": identity.source_address is not None,
                    "proxy_present": identity.proxy_address is not None,
                }
            )
        payload = {
            "provider_status": {
                provider: status.value for provider, status in sorted(ctx.peer_evidence.provider_status.items())
            },
            "identities": identities,
            "auth": {
                "authenticated": ctx.auth.authenticated,
                "domain": ctx.auth.domain,
                "principal_fingerprint": _fingerprint(ctx.auth.principal),
            },
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))
