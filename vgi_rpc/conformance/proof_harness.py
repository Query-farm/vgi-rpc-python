# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Shared harness for the proxy-proof conformance group.

Other-language runners import these types to describe the worker they can
spawn, so the matrix in ``_pytest_suite.py`` stays language-agnostic. A runner
supplies exactly one fixture — ``proof_worker_factory`` — and the suite drives
everything else from here.

Keeping the token *minting* here rather than in the port under test is the
point: a port that frames its canonical string incorrectly still round-trips
against itself, and only fails when checked against an independent minter.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field

__all__ = [
    "CONFORMANCE_KID",
    "CONFORMANCE_ORIGIN",
    "CONFORMANCE_SECRET_HEX",
    "ProofUnsupported",
    "ProofWorker",
    "ProofWorkerConfig",
    "ProofWorkerFactory",
    "secret_bytes",
]

#: Fixed test vectors. Constant rather than random so a failure is
#: reproducible from the test name alone, and so a port can hard-code the
#: same values in its own unit tests and compare byte for byte.
CONFORMANCE_KID = "conformance-proxy"
CONFORMANCE_ORIGIN = "conformance-origin"
CONFORMANCE_SECRET_HEX = "11" * 32
_SECOND_SECRET_HEX = "22" * 32


def secret_bytes(hex_value: str = CONFORMANCE_SECRET_HEX) -> bytes:
    """Decode a configured hex secret."""
    return bytes.fromhex(hex_value)


class ProofUnsupported(Exception):
    """Raised by a runner's factory for a configuration it cannot express.

    The suite converts this into a per-test skip, so a port that supports
    ``require`` mode but not, say, a configurable skew still runs everything
    else rather than reporting a blanket failure.
    """


@dataclass(frozen=True)
class ProofWorkerConfig:
    """A worker configuration the runner must be able to spawn.

    Attributes:
        mode: ``off``, ``allow`` or ``require``.
        origin_id: Identifier the worker folds into its MAC input.
        secrets: ``kid:hex`` pairs, comma-separated.
        skew_seconds: Acceptance half-window.
        replay_cache: Whether nonce replay is rejected. Disabling it is how
            the suite observes window behaviour independently of replay.

    """

    mode: str = "require"
    origin_id: str = CONFORMANCE_ORIGIN
    secrets: str = f"{CONFORMANCE_KID}:{CONFORMANCE_SECRET_HEX}"
    skew_seconds: int = 30
    replay_cache: bool = True

    def with_second_key(self, kid: str = "conformance-proxy-v2") -> ProofWorkerConfig:
        """Return a copy that also accepts a second key, as during a rotation."""
        return ProofWorkerConfig(
            mode=self.mode,
            origin_id=self.origin_id,
            secrets=f"{self.secrets},{kid}:{_SECOND_SECRET_HEX}",
            skew_seconds=self.skew_seconds,
            replay_cache=self.replay_cache,
        )


@dataclass(frozen=True)
class ProofWorker:
    """A spawned worker the suite can address.

    Attributes:
        port: Loopback port the worker listens on.
        prefix: URL prefix for RPC routes, ``""`` for root. Mandatory because
            it is the only way a shared test can address a real endpoint on
            runners that mount at different paths — and addressing a path that
            does not exist is how a 401 assertion passes for the wrong reason.
        config: The configuration this worker was spawned with, echoed back so
            tests can derive window offsets from the real skew.

    """

    port: int
    prefix: str = ""
    config: ProofWorkerConfig = field(default_factory=ProofWorkerConfig)

    @property
    def base_url(self) -> str:
        """Root URL of the worker."""
        return f"http://127.0.0.1:{self.port}"

    def rpc_url(self, method: str) -> str:
        """URL of one RPC method on this worker."""
        return f"{self.base_url}{self.prefix}/{method}"

    @property
    def health_url(self) -> str:
        """URL of the health endpoint, which is mounted under the prefix."""
        return f"{self.base_url}{self.prefix}/health"


#: What a runner's ``proof_worker_factory`` fixture must return.
ProofWorkerFactory = Callable[[ProofWorkerConfig], AbstractContextManager[ProofWorker]]
