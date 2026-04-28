# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Conformance HTTP worker booted with strict response caps.

Used by the strict-fail conformance tests that need a server with a small
``max_response_bytes`` and ``max_externalized_response_bytes`` so the
``produce_oversized_batch`` / ``oversized_unary`` / ``exchange_oversized``
methods can deliberately overshoot the cap and trigger Phase B's
strict-fail behaviour.

Defaults to ``max_response_bytes = max_externalized_response_bytes = 65_536``.
The conformance tests read these values via ``http_capabilities()`` and
compute their target overshoot size as a multiple of the advertised cap,
so the exact byte value here is not load-bearing — only that the worker
implementations can produce payloads larger than it.

A ``--fake-storage URL`` flag is accepted so the externalised-channel
strict-fail variant can be exercised end-to-end without a real S3/GCS
backend.
"""

from __future__ import annotations

import argparse

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.external import ExternalLocationConfig
from vgi_rpc.http import serve_http
from vgi_rpc.rpc import RpcServer


def main() -> None:
    """Start a strict-cap HTTP conformance worker."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--describe", action="store_true", default=True)
    parser.add_argument(
        "--max-response-bytes",
        type=int,
        # 1 MiB is large enough that incidental tests (e.g.
        # ``cancellable_producer`` running for ~1s before being cancelled)
        # don't trip the cap, while still being small enough that the
        # ``http_response_cap.*`` tests' 4x target overshoots provably
        # (4 MiB > 1 MiB).
        default=1024 * 1024,
        help="HTTP body cap (default: 1 MiB).",
    )
    parser.add_argument(
        "--max-externalized-response-bytes",
        type=int,
        default=1024 * 1024,
        help="External-channel cap per HTTP response (default: 1 MiB).",
    )
    parser.add_argument(
        "--fake-storage",
        default=None,
        help=(
            "Base URL of a fake-storage backend.  When set, the server "
            "wires externalisation so the externalised-strict-fail variant "
            "can be exercised."
        ),
    )
    parser.add_argument(
        "--externalize-threshold",
        type=int,
        default=4096,
        help="Bytes threshold above which server-produced batches are externalized.",
    )
    args = parser.parse_args()

    external_location: ExternalLocationConfig | None = None
    if args.fake_storage:
        from vgi_rpc.conformance.fake_storage import FakeStorageBackend

        external_location = ExternalLocationConfig(
            storage=FakeStorageBackend(args.fake_storage),
            externalize_threshold_bytes=args.externalize_threshold,
            url_validator=None,
        )

    server = RpcServer(
        ConformanceService,
        ConformanceServiceImpl(),
        enable_describe=args.describe,
        external_location=external_location,
    )
    serve_http(
        server,
        host=args.host,
        port=args.port,
        max_response_bytes=args.max_response_bytes,
        max_externalized_response_bytes=args.max_externalized_response_bytes,
    )


if __name__ == "__main__":
    main()
