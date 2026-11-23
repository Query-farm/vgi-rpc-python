# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The per-process object cache (``STATE_REF_V1.md`` §11).

Stored :class:`~vgi_rpc.state_ref.StateRef` values and argument references
both name an object in storage by its ``object_id``.  Resolving one fetches
the object, verifies it and opens it; this cache keeps the result so the next
turn -- or the next scan thread of the same query -- does not fetch again.

The rules, all required:

1. **Keyed by ``object_id``, never by a digest.**  Grants mint objects for
   claimed hashes without seeing the bytes, so a digest-keyed cache would
   hand one principal's value to another who merely knows its hash.  An
   ``object_id`` only reaches a server inside a tag or reference sealed for
   the caller.
2. **Single-flight.**  Concurrent resolves of one ``object_id`` share one
   fetch.
3. **Byte-bounded LRU.**  ``VGI_RPC_OBJECT_CACHE_BYTES``, default
   ``min(1 GiB, 40% of the cgroup memory limit)`` (or of physical memory when
   no cgroup limit is found).  A value larger than the bound is returned but
   not retained, with a warning.
4. **Memos hold the ``object_id``, never the value**, and look it up again on
   use, so resident bytes stay bounded by this cache alone.
5. **``fetched_at``.**  A hit on an entry fetched more than one day ago is a
   miss: an object may be deleted by the bucket lifecycle after a day, and the
   cache must not outlive it by more than that.

Nothing here is visible on the wire.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

__all__ = [
    "OBJECT_CACHE_BYTES_ENV",
    "OBJECT_CACHE_MAX_AGE_SECONDS",
    "ObjectCache",
    "ObjectCacheStats",
    "default_object_cache",
    "default_object_cache_bytes",
]

_logger = logging.getLogger("vgi_rpc.object_cache")

OBJECT_CACHE_BYTES_ENV = "VGI_RPC_OBJECT_CACHE_BYTES"
"""Environment variable overriding the cache's byte bound."""

OBJECT_CACHE_MAX_AGE_SECONDS = 86400
"""An entry fetched longer ago than this is treated as a miss (one day)."""

_ONE_GIB = 1 << 30


def _cgroup_memory_limit() -> int | None:
    """Return the cgroup memory limit in bytes, or ``None`` when unlimited/unknown."""
    candidates = (
        Path("/sys/fs/cgroup/memory.max"),  # cgroup v2
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),  # cgroup v1
    )
    for path in candidates:
        try:
            raw = path.read_text().strip()
        except OSError:
            continue
        if not raw or raw == "max":
            return None
        try:
            value = int(raw)
        except ValueError:
            continue
        # cgroup v1 reports "unlimited" as a huge page-aligned number.
        if value <= 0 or value >= (1 << 60):
            return None
        return value
    return None


def _physical_memory() -> int | None:
    """Return physical memory in bytes, or ``None`` when it cannot be read."""
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, ValueError, OSError):
        return None
    if pages <= 0 or page_size <= 0:
        return None
    return int(pages) * int(page_size)


def default_object_cache_bytes(env: Mapping[str, str] | None = None) -> int:
    """Return the cache bound: the env override, else ``min(1 GiB, 40% of memory)``.

    Args:
        env: Environment mapping (defaults to ``os.environ``).

    Returns:
        The bound in bytes.

    Raises:
        ValueError: If ``VGI_RPC_OBJECT_CACHE_BYTES`` is set but is not a
            non-negative integer.

    """
    source = os.environ if env is None else env
    raw = source.get(OBJECT_CACHE_BYTES_ENV)
    if raw is not None and raw.strip():
        try:
            value = int(raw.strip())
        except ValueError:
            raise ValueError(f"{OBJECT_CACHE_BYTES_ENV} must be a non-negative integer, got {raw!r}") from None
        if value < 0:
            raise ValueError(f"{OBJECT_CACHE_BYTES_ENV} must be a non-negative integer, got {raw!r}")
        return value
    memory = _cgroup_memory_limit() or _physical_memory()
    if memory is None:
        return _ONE_GIB
    return min(_ONE_GIB, (memory * 40) // 100)


@dataclass(frozen=True)
class ObjectCacheStats:
    """A snapshot of the cache's counters.

    Attributes:
        resident_bytes: Bytes of values currently retained.
        entries: Number of retained values.
        max_bytes: The byte bound.
        hits: Lookups answered from the cache.
        misses: Lookups that fetched (including aged-out entries).
        evictions: Entries dropped to stay under the bound or aged out.
        fetches: Fetches actually performed (single-flight joins excluded).

    """

    resident_bytes: int
    entries: int
    max_bytes: int
    hits: int
    misses: int
    evictions: int
    fetches: int


class _Flight:
    """One in-progress fetch that concurrent callers wait on."""

    __slots__ = ("done", "error", "value")

    def __init__(self) -> None:
        self.done = threading.Event()
        self.value: bytes | None = None
        self.error: BaseException | None = None


class ObjectCache:
    """Byte-bounded, single-flight LRU of verified object values keyed by ``object_id``.

    Thread-safe.  Values are the verified, opened, decompressed bytes the
    caller's ``fetch`` returns; the cache never inspects them.
    """

    def __init__(
        self,
        max_bytes: int,
        *,
        max_age_seconds: float = OBJECT_CACHE_MAX_AGE_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Create a cache.

        Args:
            max_bytes: Byte bound on retained values.  ``0`` retains nothing
                (every lookup fetches, still single-flight).
            max_age_seconds: Entries fetched longer ago than this are misses.
            clock: Wall-clock source, injectable for tests.

        """
        self._max_bytes = max_bytes
        self._max_age = max_age_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, tuple[bytes, float]] = OrderedDict()
        self._resident = 0
        self._flights: dict[str, _Flight] = {}
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._fetches = 0

    @property
    def max_bytes(self) -> int:
        """The byte bound."""
        return self._max_bytes

    def stats(self) -> ObjectCacheStats:
        """Return a snapshot of the counters.

        Returns:
            The current :class:`ObjectCacheStats`.

        """
        with self._lock:
            return ObjectCacheStats(
                resident_bytes=self._resident,
                entries=len(self._entries),
                max_bytes=self._max_bytes,
                hits=self._hits,
                misses=self._misses,
                evictions=self._evictions,
                fetches=self._fetches,
            )

    def clear(self) -> None:
        """Drop every entry (tests, and explicit operator reset)."""
        with self._lock:
            self._entries.clear()
            self._resident = 0

    def age_entries(self, seconds: float) -> int:
        """Shift every entry's ``fetched_at`` back by *seconds* (test hook).

        Lets a test prove the one-day age rule without waiting a day.

        Args:
            seconds: How far to move each entry into the past.

        Returns:
            The number of entries shifted.

        """
        with self._lock:
            for key, (value, fetched_at) in list(self._entries.items()):
                self._entries[key] = (value, fetched_at - seconds)
            return len(self._entries)

    def contains(self, object_id: str) -> bool:
        """Return whether a fresh entry for *object_id* is retained (no counters change).

        Args:
            object_id: The object id.

        Returns:
            ``True`` when a lookup would hit.

        """
        with self._lock:
            entry = self._entries.get(object_id)
            return entry is not None and self._clock() - entry[1] <= self._max_age

    def get_or_fetch(self, object_id: str, fetch: Callable[[], bytes]) -> tuple[bytes, bool]:
        """Return the value for *object_id*, fetching it at most once across threads.

        Args:
            object_id: The storage id (the cache key).
            fetch: Called on a miss to produce the verified value.  Exactly
                one caller runs it per miss; concurrent callers wait for and
                share its result (or its exception).

        Returns:
            ``(value, hit)``.  ``hit`` is ``True`` when the value came from
            the cache rather than from this call's fetch or a joined one.

        """
        with self._lock:
            entry = self._entries.get(object_id)
            if entry is not None:
                value, fetched_at = entry
                if self._clock() - fetched_at <= self._max_age:
                    self._entries.move_to_end(object_id)
                    self._hits += 1
                    _logger.debug("object_cache hit", extra={"event": "hit", "resident_bytes": self._resident})
                    return value, True
                # Aged out: the object may already be gone; never serve it.
                del self._entries[object_id]
                self._resident -= len(value)
                self._evictions += 1
            self._misses += 1
            flight = self._flights.get(object_id)
            leader = flight is None
            if flight is None:
                flight = _Flight()
                self._flights[object_id] = flight
                self._fetches += 1
        if not leader:
            flight.done.wait()
            if flight.error is not None:
                raise flight.error
            assert flight.value is not None
            return flight.value, False
        _logger.debug("object_cache miss", extra={"event": "miss", "resident_bytes": self._resident})
        try:
            value = fetch()
        except BaseException as exc:
            flight.error = exc
            with self._lock:
                self._flights.pop(object_id, None)
            flight.done.set()
            raise
        flight.value = value
        with self._lock:
            self._flights.pop(object_id, None)
            self._insert_locked(object_id, value)
        flight.done.set()
        return value, False

    def _insert_locked(self, object_id: str, value: bytes) -> None:
        size = len(value)
        if size > self._max_bytes:
            _logger.warning(
                "object_cache: a %d-byte value exceeds the cache bound (%d bytes, %s); resolved but not retained",
                size,
                self._max_bytes,
                OBJECT_CACHE_BYTES_ENV,
                extra={"event": "not_retained", "bytes": size, "max_bytes": self._max_bytes},
            )
            return
        previous = self._entries.pop(object_id, None)
        if previous is not None:
            self._resident -= len(previous[0])
        self._entries[object_id] = (value, self._clock())
        self._resident += size
        while self._resident > self._max_bytes and self._entries:
            _evicted_id, (evicted, _at) = self._entries.popitem(last=False)
            self._resident -= len(evicted)
            self._evictions += 1
            _logger.debug("object_cache evict", extra={"event": "evict", "resident_bytes": self._resident})


_default_lock = threading.Lock()
_default_cache: ObjectCache | None = None


def default_object_cache() -> ObjectCache:
    """Return the process-wide object cache, creating it on first use.

    Its bound is read from ``VGI_RPC_OBJECT_CACHE_BYTES`` (or the memory
    default) when it is created.

    Returns:
        The shared :class:`ObjectCache`.

    """
    global _default_cache
    with _default_lock:
        if _default_cache is None:
            _default_cache = ObjectCache(default_object_cache_bytes())
        return _default_cache
