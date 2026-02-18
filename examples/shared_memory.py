"""Zero-copy shared memory transport with ``ShmPipeTransport``.

Demonstrates how to set up a shared memory segment and wrap pipe
transports in ``ShmPipeTransport`` for zero-copy Arrow batch transfer
between co-located processes (or threads).

Run::

    python examples/shared_memory.py
"""

from __future__ import annotations

import contextlib
import threading
from typing import Protocol, cast

from vgi_rpc import RpcServer, ShmPipeTransport, make_pipe_pair
from vgi_rpc.rpc import _RpcProxy
from vgi_rpc.shm import ShmSegment

# ---------------------------------------------------------------------------
# 1. Define a Protocol and implementation
# ---------------------------------------------------------------------------


class MathService(Protocol):
    """A simple math service."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def multiply(self, a: float, b: float) -> float:
        """Multiply two numbers."""
        ...


class MathServiceImpl:
    """Concrete implementation of MathService."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def multiply(self, a: float, b: float) -> float:
        """Multiply two numbers."""
        return a * b


# ---------------------------------------------------------------------------
# 2. Run the example with shared memory transport
# ---------------------------------------------------------------------------


def main() -> None:
    """Create a shared memory segment, wrap pipes, and call methods."""
    # Allocate a 4 MB shared memory segment
    shm = ShmSegment.create(4 * 1024 * 1024)
    try:
        # Create pipe pair and wrap in ShmPipeTransport
        client_pipe, server_pipe = make_pipe_pair()
        client_transport = ShmPipeTransport(client_pipe, shm)
        server_transport = ShmPipeTransport(server_pipe, shm)

        # Start server on a daemon thread
        server = RpcServer(MathService, MathServiceImpl())
        thread = threading.Thread(target=server.serve, args=(server_transport,), daemon=True)
        thread.start()

        try:
            # Create a typed proxy and call methods
            svc = cast(MathService, _RpcProxy(MathService, client_transport))

            result_add = svc.add(a=2.5, b=3.5)
            print(f"add(2.5, 3.5)      = {result_add}")

            result_mul = svc.multiply(a=4.0, b=5.0)
            print(f"multiply(4.0, 5.0) = {result_mul}")
        finally:
            client_transport.close()
            thread.join(timeout=5)
    finally:
        shm.unlink()
        with contextlib.suppress(BufferError):
            shm.close()


if __name__ == "__main__":
    main()
