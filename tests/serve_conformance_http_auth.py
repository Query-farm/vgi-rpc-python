# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance HTTP worker with a reject-all authenticate callback.

Used by conformance tests that assert ``GET /health`` is exempt from
authentication: every RPC endpoint on this server returns 401, but the
health probe must still succeed for orchestrators / load balancers.

It also backs ``TestUnauthorized``'s reason-code tests. A request may name
the reason it wants refused with, via ``X-Conformance-Auth-Reason``, which
is what lets the suite prove a server *discriminates* between reason codes
rather than stamping one constant on every 401. Without the header the
worker raises a bare ``ValueError`` — the unclassified path, which must
land on ``unauthorized``.
"""

import argparse

import falcon
import waitress

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.http import AuthFailure, AuthReason, make_wsgi_app
from vgi_rpc.rpc import AuthContext, RpcServer

_REASON_HEADER = "X-Conformance-Auth-Reason"

#: The reasons a *request* may ask to be refused with.  ``proxy_required`` is
#: deliberately absent: docs/unauthorized-spec.md §5 derives it from server
#: configuration, never from the request, so a worker that let a caller
#: summon it would be modelling the contract wrong.  Anything not in this map
#: — including ``proxy_required`` and any typo — falls through to the
#: unclassified path, so a test asking for a reason it cannot get fails
#: rather than quietly passing.
_REQUESTABLE = {
    "missing_credential": AuthReason.MISSING_CREDENTIAL,
    "invalid_credential": AuthReason.INVALID_CREDENTIAL,
    "expired_credential": AuthReason.EXPIRED_CREDENTIAL,
    "insufficient_scope": AuthReason.INSUFFICIENT_SCOPE,
}


def _reject_all(req: falcon.Request) -> AuthContext:
    requested = req.get_header(_REASON_HEADER)
    reason = _REQUESTABLE.get(requested or "")
    if reason is not None:
        # The detail is the reason code itself so the suite can assert the
        # header and the JSON body agree without pinning prose.
        raise AuthFailure(reason, reason.value)
    raise ValueError("authentication required")


def main() -> None:
    """Start a waitress HTTP server that rejects every authenticated route."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    server = RpcServer(ConformanceService, ConformanceServiceImpl())
    app = make_wsgi_app(server, authenticate=_reject_all)

    print(f"PORT:{args.port}", flush=True)
    waitress.serve(app, host="127.0.0.1", port=args.port, _quiet=True)


if __name__ == "__main__":
    main()
