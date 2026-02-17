"""Subprocess server entry point.

This script serves an RPC service over stdin/stdout, designed to be spawned
as a child process by a client using ``vgi_rpc.connect()``.

The client example is in ``subprocess_client.py``.

Run the client (which spawns this automatically)::

    python examples/subprocess_client.py
"""

from __future__ import annotations

from typing import Protocol

from vgi_rpc import run_server


class Calculator(Protocol):
    """Simple calculator service."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...

    def multiply(self, a: float, b: float) -> float:
        """Multiply two numbers."""
        ...

    def divide(self, a: float, b: float) -> float:
        """Divide a by b."""
        ...


class CalculatorImpl:
    """Concrete implementation of Calculator."""

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b

    def multiply(self, a: float, b: float) -> float:
        """Multiply two numbers."""
        return a * b

    def divide(self, a: float, b: float) -> float:
        """Divide a by b."""
        if b == 0.0:
            raise ValueError("Division by zero")
        return a / b


def main() -> None:
    """Serve over stdin/stdout."""
    run_server(Calculator, CalculatorImpl())


if __name__ == "__main__":
    main()
