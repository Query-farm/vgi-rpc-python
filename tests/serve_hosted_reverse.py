# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""A stdio worker whose application protocols are registered in reverse-alphabetical order.

``ConformanceService`` happens to sort before ``conformance.Secondary.v1`` in
ASCII, so the port suite cannot tell "listed in registration order" from
"listed sorted by name" -- TypeScript shipped exactly that bug.  This worker
registers ``zeta.Primary.v1``, ``conformance.Secondary.v1``, ``alpha.Extra.v1``
in that order, which no sort produces, and is what the hosted-protocols group
is pointed at to prove it catches a sorted listing.
"""

from __future__ import annotations

from typing import ClassVar, Protocol

from vgi_rpc.conformance.secondary import Secondary, SecondaryImpl
from vgi_rpc.rpc import RpcServer, serve_stdio

#: The order the worker registers, and so the order reflection must list.
REVERSE_ORDER = ("zeta.Primary.v1", "conformance.Secondary.v1", "alpha.Extra.v1")


class ZetaPrimary(Protocol):
    """Primary; sorts last."""

    protocol_name: ClassVar[str] = "zeta.Primary.v1"

    def ping(self) -> str:
        """Answer ``pong``."""
        ...


class AlphaExtra(Protocol):
    """Registered last; sorts first."""

    protocol_name: ClassVar[str] = "alpha.Extra.v1"

    def ping(self) -> str:
        """Answer ``pong``."""
        ...


class _Ping:
    def ping(self) -> str:
        return "pong"


def main() -> None:
    """Serve over stdio with reflection on."""
    server = RpcServer(
        ZetaPrimary,
        _Ping(),
        extra_protocols=[(Secondary, SecondaryImpl()), (AlphaExtra, _Ping())],
        enable_describe=True,
    )
    serve_stdio(server)


if __name__ == "__main__":
    main()
