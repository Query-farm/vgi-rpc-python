# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""HTTP server subpackage.

The Falcon/WSGI server is split across focused modules:

* :mod:`_state_token` — signed state token pack/unpack + state type resolution.
* :mod:`_responses` — response/error helpers.
* :mod:`_errors` — Falcon error serializer + 404 sink.
* :mod:`_pages` — landing + describe HTML pages.
* :mod:`_resources` — Falcon resources (unary, stream init/exchange, health, upload-url).
* :mod:`_middleware` — Falcon middlewares (drain, request-id, access-log, auth, compression, CORS, capabilities).
* :mod:`_app` — ``_HttpRpcApp`` config/state holder.
* :mod:`_app_unary` — unary dispatch path.
* :mod:`_app_stream` — stream init/exchange/producer dispatch paths.
* :mod:`_factory` — ``make_wsgi_app``.
* :mod:`_serve` — ``serve_http`` waitress wrapper.

* :mod:`_introspect` — the token-introspection endpoint and its resolver types.

``make_wsgi_app``, ``serve_http`` and the two introspection types a worker needs
to write a resolver are part of the public API; everything else is considered
internal.
"""

from ._factory import make_wsgi_app
from ._introspect import TokenIdentity, TokenResolver
from ._serve import serve_http

__all__ = ["TokenIdentity", "TokenResolver", "make_wsgi_app", "serve_http"]
