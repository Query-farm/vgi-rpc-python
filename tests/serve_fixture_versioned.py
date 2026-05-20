# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for CLI protocol_version tests (pipe transport).

Serves the test RPC fixture service via a Protocol subclass that declares
``protocol_version`` so the server enforces the dispatch-boundary check.
A CLI that doesn't forward ``vgi_rpc.protocol_version`` would be rejected;
these workers exist to prove the CLI propagates the discovered version.
"""

from typing import ClassVar

from tests._fixture_service import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.rpc import RpcServer, serve_stdio


class VersionedFixtureService(RpcFixtureService):
    """RpcFixtureService that opts into application-protocol versioning."""

    protocol_version: ClassVar[str] = "1.0.0"


def main() -> None:
    """Serve the versioned fixture service over stdin/stdout."""
    server = RpcServer(VersionedFixtureService, RpcFixtureServiceImpl(), enable_describe=True)
    serve_stdio(server)


if __name__ == "__main__":
    main()
