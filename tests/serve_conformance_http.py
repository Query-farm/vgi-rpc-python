# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess server entry point for HTTP-based conformance tests.

Starts an HTTP server exposing the conformance RPC service.  Accepts an
optional ``--fake-storage URL`` flag that wires the conformance worker
against a :class:`vgi_rpc.conformance.fake_storage.FakeStorageBackend`
so external-location tests can run end-to-end without S3/GCS.

When ``--fake-storage`` is set the worker also installs the same
backend as an ``upload_url_provider`` and advertises
``--externalize-threshold`` as ``max_request_bytes`` via the
``OPTIONS {prefix}/__capabilities__`` discovery endpoint.
"""

from __future__ import annotations

import argparse
import socket
import sys
from urllib.parse import urlparse

import falcon

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.external import Compression, ExternalLocationConfig, FetchConfig
from vgi_rpc.http import DrainHandle, drain_handle, make_wsgi_app, serve_http
from vgi_rpc.http.server._introspect import TokenIdentity
from vgi_rpc.rpc import AuthContext, RpcServer

# Header the sticky principal-binding conformance fixture reads to decide
# which principal a request belongs to.  Documented in
# ``docs/sticky-sessions-spec.md`` §9 so cross-language ports can boot an
# equivalent fixture; the value is arbitrary, only the convention matters.
_PRINCIPAL_HEADER = "X-Conformance-Principal"


def _principal_from_header(req: falcon.Request) -> AuthContext:
    """Map ``X-Conformance-Principal`` to an authenticated principal.

    Requests without the header stay anonymous so unauthenticated probes
    (``GET /health``, ``OPTIONS /__capabilities__``) keep working.
    """
    principal = req.get_header(_PRINCIPAL_HEADER)
    if not principal:
        return AuthContext(domain=None, authenticated=False, principal=None)
    return AuthContext(domain="conformance", authenticated=True, principal=principal)


#: Fixed conformance values for the token-introspection group.  A port
#: supplying ``conformance_http_introspect_port`` MUST configure exactly these,
#: since the shared tests post the subject credential and assert the principal.
CONFORMANCE_INTROSPECTOR = "conformance-introspector"
CONFORMANCE_SUBJECT_TOKEN = "conformance-opaque-subject-token"
CONFORMANCE_SUBJECT_PRINCIPAL = "subject@conformance.example"
CONFORMANCE_SUBJECT_TOKEN_NAME = "conformance-subject"
CONFORMANCE_SUBJECT_TTL = 300
#: A JWS-shaped credential the resolver *would* resolve.  Deliberately
#: resolvable: if the fixture only offered an unknown JWS, a port with no shape
#: guard would reject it as unknown and pass the test for the wrong reason.
#: Made resolvable, the guard becomes observable -- a port that fails to reject
#: JWS shapes answers 200 and fails.
CONFORMANCE_JWS_TRAP_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"

#: The credential the resolver reports as *unknowable* rather than unknown. The
#: shared suite posts it to check that a backing-store outage surfaces as a
#: transient 503 and not as the endpoint's own definitive 404 — which a caller
#: may negative-cache, turning a brief outage into a remembered bad credential.
CONFORMANCE_UNAVAILABLE_TOKEN = "conformance-unavailable-token"


def _maybe_access_log(server: RpcServer, path: str | None) -> None:
    """Wire the access log, if the runner asked for one.

    Backs the shared ``TestRequestId`` correlation case: asserting that the
    ``X-Request-ID`` on a response equals the ``request_id`` in the record
    means reading back what the server logged for a request the suite made.
    """
    if not path:
        return
    from vgi_rpc.rpc import _configure_access_log

    _configure_access_log(
        path=path,
        max_bytes=0,
        backup_count=0,
        when=None,
        max_record_bytes=1_048_576,
        server_id=server.server_id,
    )


def _conformance_resolver(token: str) -> TokenIdentity | None:
    """Resolve the fixed credentials the shared tests post.

    Three answers, deliberately: an identity, ``None`` for "does not resolve",
    and ``AuthUnavailableError`` for "I could not find out". The third is not a
    flavour of the second — a caller may negative-cache ``None``'s 404, so an
    outage reported that way is remembered as a bad credential.

    Args:
        token: The opaque credential posted to the endpoint.

    Returns:
        The identity, or ``None`` when the credential does not resolve.

    Raises:
        AuthUnavailableError: For ``CONFORMANCE_UNAVAILABLE_TOKEN``, standing in
            for a backing store that cannot be reached.

    """
    if token == CONFORMANCE_UNAVAILABLE_TOKEN:
        from vgi_rpc.http import AuthUnavailableError

        raise AuthUnavailableError("conformance: mapping store unreachable")
    if token in (CONFORMANCE_SUBJECT_TOKEN, CONFORMANCE_JWS_TRAP_TOKEN):
        return TokenIdentity(
            principal=CONFORMANCE_SUBJECT_PRINCIPAL,
            token_name=CONFORMANCE_SUBJECT_TOKEN_NAME,
            ttl_seconds=CONFORMANCE_SUBJECT_TTL,
        )
    return None


class _TestDrainResource:
    """Test-only admin endpoint at ``/__test_drain__``.

    Lets the canonical ``TestSticky::test_drain_rejects_new_opens`` test
    flip the sticky registry's drain flag over the wire (without sending
    SIGTERM, which would kill the subprocess fixture). NOT installed in
    production :func:`vgi_rpc.http.make_wsgi_app` — this is purely a
    conformance-fixture concern.

    Supports:
    * ``POST`` — set the drain flag (subsequent ``open_session`` calls raise ``ServerDrainingError``).
    * ``DELETE`` — clear the drain flag (so subsequent tests in the same fixture session aren't poisoned).

    Both are idempotent and return 204.
    No-op if the server isn't sticky-enabled.
    """

    __slots__ = ("_handle",)

    def __init__(self, handle: DrainHandle | None) -> None:
        self._handle = handle

    def on_post(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Set the drain flag; idempotent."""
        if self._handle is not None:
            self._handle.drain()
        resp.status = falcon.HTTP_204

    def on_delete(self, req: falcon.Request, resp: falcon.Response) -> None:
        """Clear the drain flag; idempotent."""
        # Reach through the handle's drain closure to find the registry.
        # The DrainHandle dataclass exposes is_draining + drain + shutdown
        # but not set_draining(False) — that's a deliberate operator-API
        # choice (production deployments only ever drain, never undrain).
        # Tests need the reverse so they don't poison the fixture.
        if self._handle is not None:
            # Walk to the underlying _SessionRegistry via the bound shutdown method.
            registry = self._handle.shutdown.__self__  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            registry.set_draining(False)
        resp.status = falcon.HTTP_204


def _bind_listener(host: str, port: int) -> socket.socket:
    """Bind a listening socket and keep it, so the port cannot be stolen.

    Selecting a port by binding a probe socket, closing it, and letting the
    server bind the number afterwards leaves a window in which another worker
    starting concurrently can take that port. The loser's bind fails and its
    process dies, but the fixture that spawned it still sees something
    listening — the *winner* — and hands out a port served by the wrong
    worker. That surfaces far away as an unrelated conformance failure (two
    "distinct" sticky peers turning out to be one server, say). Binding once
    and handing the live socket to waitress closes the window.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    return sock


def main() -> None:
    """Start an HTTP server for the conformance service."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", default=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    # Default on so TestDescribeConformance can probe __describe__ against this
    # real HTTP worker.  Use --no-describe semantics by omitting the flag is not
    # possible with store_true; tests that need it off build their own server.
    parser.add_argument("--describe", action="store_true", default=True)
    parser.add_argument(
        "--fake-storage",
        default=None,
        help="Base URL of a fake-storage service (enables external-location uploads).",
    )
    parser.add_argument(
        "--externalize-threshold",
        type=int,
        default=4096,
        help="Bytes threshold above which server-produced batches are externalized.",
    )
    parser.add_argument(
        "--max-request-bytes",
        type=int,
        default=None,
        help=(
            "Bytes cap on inline request bodies (advertised + enforced). "
            "Defaults to --externalize-threshold when unset."
        ),
    )
    parser.add_argument(
        "--compression",
        choices=("none", "zstd"),
        default="none",
        help="Compression for externalized batches.",
    )
    parser.add_argument(
        "--max-fetch-bytes",
        type=int,
        default=None,
        help="Encoded/on-wire cap for resolving external request pointers.",
    )
    parser.add_argument(
        "--max-decompressed-fetch-bytes",
        type=int,
        default=None,
        help="Decoded cap for resolving external request pointers.",
    )
    parser.add_argument(
        "--reject-localhost-redirects",
        action="store_true",
        help=(
            "Allow the fixture's 127.0.0.1 storage URL but reject a redirect "
            "to localhost. Test-only redirect-hop validation affordance."
        ),
    )
    parser.add_argument(
        "--no-sticky",
        action="store_true",
        default=False,
        help="Disable sticky sessions. Default: enabled, so TestSticky conformance group runs.",
    )
    parser.add_argument(
        "--no-sticky-echo",
        action="store_true",
        default=False,
        help=(
            "Disable the canonical sticky-echo-header advertisement. "
            "Default: enabled with a fixed marker header so TestSticky::"
            "test_echo_header_round_trip exercises the contract."
        ),
    )
    parser.add_argument(
        "--sticky-ttl",
        type=float,
        default=None,
        help=(
            "Default sticky session TTL in seconds. A short value (~1s) backs "
            "TestSticky::test_expired_session_surfaces_session_lost; omit for the "
            "framework default."
        ),
    )
    parser.add_argument(
        "--token-key",
        default=None,
        help=(
            "Hex-encoded AEAD token key. Two workers booted with the same key back "
            "TestSticky::test_token_from_other_worker_rejected — the token decrypts "
            "on the peer but must still be refused on server_id mismatch."
        ),
    )
    parser.add_argument(
        "--sticky-auth",
        action="store_true",
        default=False,
        help=(
            f"Install an authenticate callback mapping the {_PRINCIPAL_HEADER} header to "
            "the request principal, so TestSticky::test_cross_principal_replay_rejected "
            "can present one worker's session token as two different principals."
        ),
    )
    parser.add_argument(
        "--no-compression",
        action="store_true",
        help=(
            "Serve with response compression disabled, so the server advertises "
            "a present-but-empty VGI-Supported-Encodings. Backs the shared "
            "conformance case that a server which says it speaks no compression "
            "never compresses, whatever the client asks for."
        ),
    )
    parser.add_argument(
        "--no-call-state-cache",
        action="store_true",
        help=(
            "Serve with the call-state cache disabled, so every stream "
            "continuation takes the cache-miss path and must resolve the "
            "client's echoed call token. Backs the shared conformance case "
            "that a client echoes vgi_rpc.call_state#b64 on every request — "
            "which a warm cache would otherwise hide until production."
        ),
    )
    parser.add_argument(
        "--access-log",
        default=None,
        help=(
            "Append JSONL access-log records to this path. Backs the shared "
            "TestRequestId correlation case, which needs to read back what the "
            "server logged for a request it made."
        ),
    )
    parser.add_argument(
        "--introspect",
        action="store_true",
        help=(
            "Enable POST /__introspect_token__ with the fixed conformance "
            "resolver and a single-principal introspector allowlist. Backs the "
            "shared TestTokenIntrospection group; implies principal-header auth "
            "so the allowlist has something to check."
        ),
    )
    parser.add_argument(
        "--cors-origin",
        default=None,
        help=(
            "Serve with CORS enabled for this origin. Backs the shared "
            "conformance case that every capability header a server "
            "advertises is also named in Access-Control-Expose-Headers — "
            "without which a browser client can see none of them, a failure "
            "the suite's own HTTP client cannot otherwise detect."
        ),
    )
    args = parser.parse_args()

    call_state_cache_entries = 0 if args.no_call_state_cache else 4096
    introspect_resolver = _conformance_resolver if args.introspect else None
    introspect_principals = [CONFORMANCE_INTROSPECTOR] if args.introspect else None
    enable_sticky = not args.no_sticky
    # Fixed marker the canonical TestSticky::test_echo_header_round_trip
    # captures + replays. Operators wiring up real deployments use
    # vgi_rpc.http.fly.fly_sticky_echo_headers() or their own mapping —
    # this constant exists only to give the conformance group a stable
    # contract to exercise.
    sticky_echo_headers: dict[str, str] | None = (
        None if args.no_sticky_echo or not enable_sticky else {"x-vgi-conformance-echo": "conformance-fixed-marker"}
    )

    compression_level = None if args.no_compression else 1
    token_key = bytes.fromhex(args.token_key) if args.token_key else None
    authenticate = _principal_from_header if (args.sticky_auth or args.introspect) else None
    # Mirror make_wsgi_app's own default so the plain worker keeps shipping
    # the documented value in VGI-Sticky-Default-TTL.
    sticky_default_ttl: float = 300.0 if args.sticky_ttl is None else float(args.sticky_ttl)

    if not args.fake_storage:
        # Plain HTTP server, no external storage.
        server = RpcServer(
            ConformanceService,
            ConformanceServiceImpl(),
            enable_describe=args.describe,
        )
        _maybe_access_log(server, args.access_log)
        if not enable_sticky:
            # serve_http() doesn't expose enable_sticky directly; fall through
            # to the make_wsgi_app + waitress path below when sticky is on so
            # the conformance default exercises the feature.
            serve_http(
                server,
                host=args.host,
                port=args.port,
                compression_level=compression_level,
                call_state_cache_entries=call_state_cache_entries,
                cors_origins=args.cors_origin,
            )
            return
        # Sticky-enabled default path. Mirrors the externalisation branch
        # below but without storage, so the canonical TestSticky group has
        # a server to talk to in every conformance run.
        listener = _bind_listener(args.host, args.port)
        port = int(listener.getsockname()[1])
        app = make_wsgi_app(
            server,
            compression_level=compression_level,
            enable_sticky=True,
            sticky_echo_headers=sticky_echo_headers,
            token_key=token_key,
            authenticate=authenticate,
            sticky_default_ttl=sticky_default_ttl,
            call_state_cache_entries=call_state_cache_entries,
            cors_origins=args.cors_origin,
            introspect_resolver=introspect_resolver,
            introspect_principals=introspect_principals,
        )
        # Test-only admin endpoint so canonical conformance tests can
        # trigger drain over the wire without sending SIGTERM.
        app.add_route("/__test_drain__", _TestDrainResource(drain_handle(app)))
        try:
            import waitress
        except ImportError:
            print("HTTP transport requires waitress: pip install vgi-rpc[http]", file=sys.stderr)
            sys.exit(1)
        print(f"PORT:{port}", flush=True)
        waitress.serve(app, sockets=[listener], _quiet=True)
        return

    # Externalization-enabled mode: wire both server-side externalization
    # (ExternalLocationConfig.storage) and client-vended upload URLs
    # (upload_url_provider) so the OPTIONS capabilities response
    # advertises the full external-location protocol.
    from vgi_rpc.conformance.fake_storage import FakeStorageBackend

    backend = FakeStorageBackend(args.fake_storage)

    def external_url_validator(url: str) -> None:
        """Apply the external-security fixture's host policy."""
        if not args.reject_localhost_redirects:
            return
        if urlparse(url).hostname != "127.0.0.1":
            raise ValueError("external-security fixture permits only 127.0.0.1")

    fetch_defaults = FetchConfig()
    fetch_config = FetchConfig(
        max_fetch_bytes=(fetch_defaults.max_fetch_bytes if args.max_fetch_bytes is None else args.max_fetch_bytes),
        max_decompressed_bytes=args.max_decompressed_fetch_bytes,
    )
    external_location = ExternalLocationConfig(
        storage=backend,
        externalize_threshold_bytes=args.externalize_threshold,
        compression=Compression() if args.compression == "zstd" else None,
        fetch_config=fetch_config,
        url_validator=external_url_validator if args.reject_localhost_redirects else None,
    )
    server = RpcServer(
        ConformanceService,
        ConformanceServiceImpl(),
        enable_describe=args.describe,
        external_location=external_location,
    )
    _maybe_access_log(server, args.access_log)

    listener = _bind_listener(args.host, args.port)
    port = int(listener.getsockname()[1])

    max_request_bytes = args.max_request_bytes if args.max_request_bytes is not None else args.externalize_threshold
    app = make_wsgi_app(
        server,
        upload_url_provider=backend,
        compression_level=compression_level,
        max_request_bytes=max_request_bytes,
        max_upload_bytes=64 * 1024 * 1024,
        enable_sticky=enable_sticky,
        sticky_echo_headers=sticky_echo_headers,
        cors_origins=args.cors_origin,
        introspect_resolver=introspect_resolver,
        introspect_principals=introspect_principals,
    )
    # Test-only admin endpoint (see _TestDrainResource for rationale).
    app.add_route("/__test_drain__", _TestDrainResource(drain_handle(app)))

    try:
        import waitress
    except ImportError:
        print("HTTP transport requires waitress: pip install vgi-rpc[http]", file=sys.stderr)
        sys.exit(1)

    print(f"PORT:{port}", flush=True)
    waitress.serve(app, sockets=[listener], _quiet=True)


if __name__ == "__main__":
    main()
