# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Subprocess worker for ``test_transport_kind`` end-to-end tests.

Serves a tiny ``_KindService`` over stdin/stdout pipes via ``run_server``
so the parent test can confirm the worker's ``on_serve_start`` sees
``TransportKind.PIPE`` even when launched as a child process.
"""

from __future__ import annotations

from tests.test_transport_kind import _KindService, _RecordingImpl
from vgi_rpc.rpc import run_server


def main() -> None:
    """Serve the kind-reporting fixture over stdin/stdout."""
    run_server(_KindService, _RecordingImpl())


if __name__ == "__main__":
    main()
