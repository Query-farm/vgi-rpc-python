# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Fly.io quickstart helpers for HTTP sticky sessions.

Fly.io routes traffic via Anycast to whatever Machine the edge proxy
(fly-proxy) picks; a request opening a sticky session may land on a
different Machine on the next call. ``fly-force-instance-id`` is the
proactive routing header fly-proxy honours: the client supplies the
target Machine ID, fly-proxy routes directly there.

vgi-rpc's :ref:`sticky session echo-header mechanism <sticky-sessions>`
fits this exactly: at session-open time the server tells the client to
echo a given header on subsequent calls. On Fly, that header is
``fly-force-instance-id`` and the value is ``$FLY_MACHINE_ID``.

Two helpers are provided:

* :func:`auto_server_id` — read ``$FLY_MACHINE_ID`` and use it as the
  RpcServer's ``server_id``. Aligns the session-token's stamped server
  identity with the Fly Machine identity so operators reading logs see
  the same string in both places.
* :func:`fly_sticky_echo_headers` — return the
  ``{"fly-force-instance-id": $FLY_MACHINE_ID}`` mapping ready to pass
  as ``sticky_echo_headers=`` to :func:`vgi_rpc.http.make_wsgi_app`.

Both return ``None`` in non-Fly environments (when ``FLY_MACHINE_ID`` is
unset) so a single codebase deployed to Fly and elsewhere reads the same
configuration without conditional branches:

.. code-block:: python

    from vgi_rpc import RpcServer
    from vgi_rpc.http import make_wsgi_app
    from vgi_rpc.http.fly import auto_server_id, fly_sticky_echo_headers

    server = RpcServer(
        MyService, MyServiceImpl(),
        server_id=auto_server_id(),  # ⇒ FLY_MACHINE_ID on Fly, random elsewhere
    )
    app = make_wsgi_app(
        server,
        enable_sticky=True,
        sticky_echo_headers=fly_sticky_echo_headers(),  # ⇒ Fly hint, or None
    )

Off Fly, ``sticky_echo_headers=None`` is a no-op (no headers emitted);
the deployment falls back to whatever sticky-routing mechanism the LB
provides (or no sticky routing at all if there's no LB).
"""

from __future__ import annotations

import os

__all__ = ["FLY_MACHINE_ID", "auto_server_id", "fly_sticky_echo_headers"]


FLY_MACHINE_ID: str | None = os.environ.get("FLY_MACHINE_ID")
"""The current Fly Machine ID, or ``None`` outside Fly.

Read once at module import.  Fly Machines have stable IDs that persist
across restarts of the same Machine, so caching at import time is safe.
"""


def auto_server_id() -> str | None:
    """Return ``FLY_MACHINE_ID`` if running on Fly, else ``None``.

    Use as ``RpcServer(server_id=auto_server_id())`` to make the session
    token's stamped server identity match the Fly Machine ID. The
    framework's session-token format embeds ``server_id`` length-prefixed,
    so this works for any length of identifier — Fly Machine IDs are
    14 hex characters today but the contract doesn't depend on that.

    Returns ``None`` outside Fly so RpcServer falls back to its
    default random 12-char hex ``server_id``.
    """
    return FLY_MACHINE_ID


def fly_sticky_echo_headers() -> dict[str, str] | None:
    """Return ``{"fly-force-instance-id": FLY_MACHINE_ID}`` on Fly, else ``None``.

    Use as ``make_wsgi_app(..., sticky_echo_headers=fly_sticky_echo_headers())``.
    When a method opens a session via ``ctx.open_session(...)`` on Fly, the
    server emits ``VGI-Echo-fly-force-instance-id: <machine-id>`` on the
    response; the client captures and replays it as ``fly-force-instance-id``
    on every subsequent request in the same session, and fly-proxy routes
    directly to the owning Machine.

    Returns ``None`` outside Fly so passing this through unchanged is a
    no-op in non-Fly environments — operators don't need a conditional.
    """
    if FLY_MACHINE_ID is None:
        return None
    return {"fly-force-instance-id": FLY_MACHINE_ID}
