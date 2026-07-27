# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Bounded replay cache for proxy-proof nonces.

A verified proof is replayable for the width of its timestamp window unless
nonces are remembered. This module provides that memory with two independent
bounds:

* a **TTL** equal to the acceptance window — a nonce older than the window can
  no longer verify, so remembering it buys nothing; and
* a **hard capacity cap** — without one, an attacker sending distinct nonces at
  line rate grows the process without limit. That is a trivially remote
  memory-exhaustion vector, and a TTL alone does not close it: the TTL bounds
  how *long* an entry lives, never how many arrive inside that window.

Overflow evicts the oldest entry rather than rejecting the request. A traffic
burst should not become an outage, and the timestamp window still bounds how
long an evicted nonce could be replayed. The eviction is counted so the
condition is visible rather than silent.

No dependencies beyond the standard library.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable

__all__ = ["NonceCache"]

DEFAULT_CAPACITY = 100_000


class NonceCache:
    """Thread-safe, bounded set of recently-seen nonces.

    Entries expire after ``ttl_seconds`` and the total is capped at
    ``capacity``. Because every entry shares the same TTL, insertion order is
    also expiry order — so expired entries are always a prefix of the
    :class:`~collections.OrderedDict`, and sweeping from the front until the
    first live entry is both exact and amortized O(1) per insertion.

    Attributes:
        capacity: Maximum number of retained nonces.
        ttl_seconds: How long a nonce is remembered.

    """

    __slots__ = ("_clock", "_entries", "_evicted", "_lock", "_replays", "capacity", "ttl_seconds")

    def __init__(
        self,
        *,
        ttl_seconds: float,
        capacity: int = DEFAULT_CAPACITY,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Create a nonce cache.

        Args:
            ttl_seconds: Retention window, normally the proof acceptance skew.
            capacity: Hard upper bound on retained entries.
            clock: Monotonic time source, injectable for tests. Defaults to
                :func:`time.monotonic` — a wall clock would let an NTP step
                expire or resurrect entries.

        Raises:
            ValueError: If ``ttl_seconds`` or ``capacity`` is not positive.

        """
        if ttl_seconds <= 0:
            raise ValueError(f"ttl_seconds must be positive, got {ttl_seconds}")
        if capacity <= 0:
            raise ValueError(f"capacity must be positive, got {capacity}")
        self.ttl_seconds = float(ttl_seconds)
        self.capacity = int(capacity)
        self._clock = clock if clock is not None else time.monotonic
        self._entries: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()
        self._evicted = 0
        self._replays = 0

    def check_and_add(self, nonce: str) -> bool:
        """Atomically test whether a nonce is fresh, remembering it if so.

        Test and insert are a single locked operation deliberately: a separate
        ``contains`` then ``add`` would let two concurrent replays of the same
        nonce both observe "not seen" and both be accepted.

        Args:
            nonce: The nonce field from a proof token.

        Returns:
            ``True`` if the nonce had not been seen (and is now remembered),
            ``False`` if it is a replay.

        """
        now = self._clock()
        with self._lock:
            self._sweep(now)
            if nonce in self._entries:
                self._replays += 1
                return False
            # Evict oldest rather than refuse: a burst past capacity is an
            # availability problem, not an authentication one, and the
            # timestamp window still bounds the evicted nonce's usefulness.
            while len(self._entries) >= self.capacity:
                self._entries.popitem(last=False)
                self._evicted += 1
            self._entries[nonce] = now + self.ttl_seconds
            return True

    def _sweep(self, now: float) -> None:
        """Drop expired entries. Caller holds the lock."""
        entries = self._entries
        while entries:
            nonce, expires_at = next(iter(entries.items()))
            if expires_at > now:
                # Uniform TTL means insertion order is expiry order, so the
                # first live entry ends the sweep.
                break
            del entries[nonce]

    def stats(self) -> dict[str, int]:
        """Return counters for observability.

        ``overflow_evictions`` rising means ``capacity`` is undersized for the
        offered rate — or that someone is deliberately flooding distinct
        nonces. Either way it should be alerted on, because past that point
        replay protection is degraded.
        """
        with self._lock:
            return {
                "size": len(self._entries),
                "capacity": self.capacity,
                "replays_rejected": self._replays,
                "overflow_evictions": self._evicted,
            }

    def __len__(self) -> int:
        """Return the number of retained nonces, including any not yet swept."""
        with self._lock:
            return len(self._entries)
