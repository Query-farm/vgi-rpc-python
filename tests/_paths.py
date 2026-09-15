# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Route-shape helpers for tests that build HTTP paths by hand.

Many tests deliberately bypass the typed client — they probe malformed bodies,
wrong content types, unknown methods, compression caps and auth headers, none of
which a typed client would ever produce. They therefore construct URLs directly.

Before this module the route shape was duplicated across roughly a hundred such
sites, so moving it (``{prefix}/{method}`` → ``{prefix}/{protocol}/{method}``)
meant an edit at every one. These wrap the production builders in
``vgi_rpc.http._common`` so tests and the client agree by construction, and the
next change to the shape is one edit.
"""

from __future__ import annotations

from vgi_rpc.http._common import reserved_path, rpc_path

#: Fixture protocols used across the HTTP tests. Routing keys are the Protocol
#: class names, since none of these declare an explicit ``protocol_name``.
FIXTURE = "RpcFixtureService"
CONFORMANCE = "ConformanceService"
CLIENT_CONFORMANCE = "ClientConformanceService"


def fixture_path(method: str, *, prefix: str = "", suffix: str = "") -> str:
    """Path to a method on the shared ``RpcFixtureService`` test worker."""
    return rpc_path(FIXTURE, method, prefix=prefix, suffix=suffix)


def conformance_path(method: str, *, prefix: str = "", suffix: str = "") -> str:
    """Path to a method on the conformance service."""
    return rpc_path(CONFORMANCE, method, prefix=prefix, suffix=suffix)


__all__ = [
    "CLIENT_CONFORMANCE",
    "CONFORMANCE",
    "FIXTURE",
    "conformance_path",
    "fixture_path",
    "reserved_path",
    "rpc_path",
]
