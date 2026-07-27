# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the bounded proxy-proof nonce replay cache."""

from __future__ import annotations

import threading

import pytest

from vgi_rpc.http._replay import NonceCache


class _FakeClock:
    """Injectable monotonic clock so TTL behaviour needs no sleeping."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        """Return the current fake time."""
        return self.now

    def advance(self, seconds: float) -> None:
        """Move the clock forward."""
        self.now += seconds


class TestFreshness:
    """Tests for first-use acceptance and replay rejection."""

    def test_first_use_is_fresh(self) -> None:
        """An unseen nonce is accepted."""
        cache = NonceCache(ttl_seconds=30)
        assert cache.check_and_add("n1") is True

    def test_immediate_replay_is_rejected(self) -> None:
        """Re-presenting the same nonce inside the window is refused."""
        cache = NonceCache(ttl_seconds=30)
        assert cache.check_and_add("n1") is True
        assert cache.check_and_add("n1") is False
        assert cache.stats()["replays_rejected"] == 1

    def test_distinct_nonces_are_independent(self) -> None:
        """Different nonces do not interfere."""
        cache = NonceCache(ttl_seconds=30)
        assert cache.check_and_add("n1") is True
        assert cache.check_and_add("n2") is True
        assert len(cache) == 2


class TestTtl:
    """Tests for TTL-based expiry using an injected clock."""

    def test_entry_expires_after_ttl(self) -> None:
        """Past the window a nonce is forgotten.

        Safe because a proof that old can no longer pass the timestamp
        check anyway, and forgetting is what keeps the cache bounded.
        """
        clock = _FakeClock()
        cache = NonceCache(ttl_seconds=30, clock=clock)
        assert cache.check_and_add("n1") is True
        clock.advance(31)
        assert cache.check_and_add("n1") is True

    def test_entry_survives_until_ttl(self) -> None:
        """Inside the window the nonce is still remembered."""
        clock = _FakeClock()
        cache = NonceCache(ttl_seconds=30, clock=clock)
        cache.check_and_add("n1")
        clock.advance(29)
        assert cache.check_and_add("n1") is False

    def test_sweep_drops_only_expired_entries(self) -> None:
        """Sweeping removes expired entries without touching live ones."""
        clock = _FakeClock()
        cache = NonceCache(ttl_seconds=30, clock=clock)
        cache.check_and_add("old")
        clock.advance(20)
        cache.check_and_add("new")
        clock.advance(11)  # "old" is now 31s, "new" is 11s
        cache.check_and_add("trigger")
        assert cache.check_and_add("old") is True  # forgotten
        assert cache.check_and_add("new") is False  # retained


class TestCapacity:
    """Tests for the hard capacity cap and overflow behaviour."""

    def test_hard_cap_is_enforced(self) -> None:
        """Size never exceeds capacity, however many nonces arrive.

        Without this an attacker sending distinct nonces at line rate grows
        the process without limit — the TTL alone does not bound how many
        arrive inside the window.
        """
        cache = NonceCache(ttl_seconds=3600, capacity=10)
        for i in range(100):
            cache.check_and_add(f"n{i}")
        assert len(cache) <= 10

    def test_overflow_evicts_oldest_and_counts(self) -> None:
        """Overflow drops the oldest entry and increments the counter."""
        cache = NonceCache(ttl_seconds=3600, capacity=3)
        for name in ("a", "b", "c"):
            cache.check_and_add(name)
        cache.check_and_add("d")  # evicts "a"
        assert cache.stats()["overflow_evictions"] == 1
        assert cache.check_and_add("a") is True  # evicted, so looks fresh
        assert cache.check_and_add("c") is False  # still retained

    def test_overflow_does_not_reject_the_request(self) -> None:
        """A burst past capacity degrades replay protection, not availability."""
        cache = NonceCache(ttl_seconds=3600, capacity=2)
        assert all(cache.check_and_add(f"n{i}") for i in range(50))

    @pytest.mark.parametrize(("ttl", "capacity"), [(0, 10), (-1, 10), (30, 0), (30, -5)])
    def test_rejects_nonpositive_bounds(self, ttl: float, capacity: int) -> None:
        """Construction refuses a TTL or capacity that disables a bound."""
        with pytest.raises(ValueError):
            NonceCache(ttl_seconds=ttl, capacity=capacity)


class TestConcurrency:
    """Tests that test-and-set is atomic under contention."""

    def test_concurrent_replay_admits_exactly_one(self) -> None:
        """Racing replays of one nonce yield exactly one acceptance.

        A separate contains-then-add would let two threads both observe
        "not seen" and both be served.
        """
        cache = NonceCache(ttl_seconds=30)
        results: list[bool] = []
        results_lock = threading.Lock()
        start = threading.Barrier(16)

        def attempt() -> None:
            start.wait()
            ok = cache.check_and_add("contested")
            with results_lock:
                results.append(ok)

        threads = [threading.Thread(target=attempt) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(results) == 1, f"expected exactly one acceptance, got {sum(results)}"
