"""Client that spawns a subprocess server and calls methods on it.

Uses ``vgi_rpc.connect()`` which launches the worker as a child process
and communicates over stdin/stdout pipes.

Run::

    python examples/subprocess_client.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Protocol

from vgi_rpc import RpcError, connect

_HERE = Path(__file__).resolve().parent


class Calculator(Protocol):
    """Simple calculator service.

    This Protocol is duplicated from subprocess_worker.py so each file is
    self-contained.  In a real project you'd define the Protocol once in a
    shared module and import it from both sides.
    """

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def multiply(self, a: float, b: float) -> float:
        """Multiply two numbers."""
        ...

    def divide(self, a: float, b: float) -> float:
        """Divide a by b."""
        ...


def main() -> None:
    """Spawn the worker and make RPC calls."""
    cmd = [sys.executable, str(_HERE / "subprocess_worker.py")]

    with connect(Calculator, cmd) as calc:
        print(f"add(2, 3)      = {calc.add(a=2, b=3)}")
        print(f"multiply(4, 5) = {calc.multiply(a=4, b=5)}")
        print(f"divide(10, 3)  = {calc.divide(a=10, b=3):.4f}")

        # Server-side exceptions are propagated as RpcError
        try:
            calc.divide(a=1, b=0)
        except RpcError as e:
            print(f"\nCaught remote error: {e.error_type}: {e.error_message}")


if __name__ == "__main__":
    main()
