"""Subprocess server entry point for pipe-based conformance tests.

Can be run directly: ``python -m tests.serve_conformance_pipe``

Imports the conformance service Protocol and implementation, then serves
RPC requests over stdin/stdout.
"""

from vgi_rpc.conformance import ConformanceService, ConformanceServiceImpl
from vgi_rpc.rpc import run_server


def main() -> None:
    """Serve the conformance service over stdin/stdout."""
    run_server(ConformanceService, ConformanceServiceImpl())


if __name__ == "__main__":
    main()
