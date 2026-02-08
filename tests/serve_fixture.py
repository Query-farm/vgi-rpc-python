"""Subprocess server entry point for RPC fixture tests.

Installed as ``vgi-rpc-test-worker`` via pyproject.toml ``[project.scripts]``.
Can also be run directly: ``python -m tests.serve_fixture``

Imports the test service Protocol and implementation, then serves
RPC requests over stdin/stdout.
"""

from tests.test_rpc import RpcFixtureService, RpcFixtureServiceImpl
from vgi_rpc.rpc import RpcServer, serve_stdio


def main() -> None:
    """Serve the RPC fixture service over stdin/stdout."""
    server = RpcServer(RpcFixtureService, RpcFixtureServiceImpl())
    serve_stdio(server)


if __name__ == "__main__":
    main()
