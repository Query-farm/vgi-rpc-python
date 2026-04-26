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

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.external import Compression, ExternalLocationConfig
from vgi_rpc.http import make_wsgi_app, serve_http
from vgi_rpc.rpc import RpcServer


def main() -> None:
    """Start an HTTP server for the conformance service."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", default=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--describe", action="store_true", default=False)
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
    args = parser.parse_args()

    if not args.fake_storage:
        # Plain HTTP server, no external storage.
        server = RpcServer(
            ConformanceService,
            ConformanceServiceImpl(),
            enable_describe=args.describe,
        )
        serve_http(server, host=args.host, port=args.port)
        return

    # Externalization-enabled mode: wire both server-side externalization
    # (ExternalLocationConfig.storage) and client-vended upload URLs
    # (upload_url_provider) so the OPTIONS capabilities response
    # advertises the full external-location protocol.
    from vgi_rpc.conformance.fake_storage import FakeStorageBackend

    backend = FakeStorageBackend(args.fake_storage)
    external_location = ExternalLocationConfig(
        storage=backend,
        externalize_threshold_bytes=args.externalize_threshold,
        compression=Compression() if args.compression == "zstd" else None,
        url_validator=None,
    )
    server = RpcServer(
        ConformanceService,
        ConformanceServiceImpl(),
        enable_describe=args.describe,
        external_location=external_location,
    )

    port = args.port
    if port == 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((args.host, 0))
            port = int(s.getsockname()[1])

    max_request_bytes = args.max_request_bytes if args.max_request_bytes is not None else args.externalize_threshold
    app = make_wsgi_app(
        server,
        upload_url_provider=backend,
        max_request_bytes=max_request_bytes,
        max_upload_bytes=64 * 1024 * 1024,
    )

    try:
        import waitress
    except ImportError:
        print("HTTP transport requires waitress: pip install vgi-rpc[http]", file=sys.stderr)
        sys.exit(1)

    print(f"PORT:{port}", flush=True)
    waitress.serve(app, host=args.host, port=port, _quiet=True)


if __name__ == "__main__":
    main()
