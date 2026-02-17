"""Minimal vgi-rpc example: define a service and call it in-process.

This is the quickest way to get started. The service runs in a background
thread and communicates over an in-process pipe — no subprocess or network
needed.

Run::

    python examples/hello_world.py
"""

from __future__ import annotations

from typing import Protocol

from vgi_rpc import serve_pipe


# 1. Define the service interface as a Protocol class.
#    Return types determine the method type (unary here).
class Greeter(Protocol):
    """A simple greeting service."""

    def greet(self, name: str) -> str:
        """Return a greeting for *name*."""
        ...

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        ...


# 2. Implement the interface.
class GreeterImpl:
    """Concrete implementation of the Greeter service."""

    def greet(self, name: str) -> str:
        """Return a greeting for *name*."""
        return f"Hello, {name}!"

    def add(self, a: float, b: float) -> float:
        """Add two numbers."""
        return a + b


# 3. Start the server in-process and call methods through a typed proxy.
def main() -> None:
    """Run the example."""
    with serve_pipe(Greeter, GreeterImpl()) as svc:
        print(svc.greet(name="World"))  # Hello, World!
        print(svc.add(a=2.5, b=3.5))  # 6.0


if __name__ == "__main__":
    main()
