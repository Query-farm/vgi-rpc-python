# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tests for the transport read/write chunking wrappers.

These pin the two properties that let a >2 GiB Arrow IPC body survive a
single ``write(2)`` / ``recv(2)``: every call to the underlying stream is
clamped below ``INT_MAX``, and the wrapper loops until the whole buffer is
accounted for.  Both are needed -- clamping without looping turns an
intermittent ``EINVAL`` into a deterministic short transfer, and looping
without clamping still fails outright on a socket.

The real sizes involved are multiple gigabytes, so the chunk limits are
monkeypatched down to a handful of bytes instead.  The arithmetic is the
same; only the constant changes.
"""

from __future__ import annotations

import io
from typing import Any

import pytest

from vgi_rpc.rpc import _transport


class _RecordingRaw(io.RawIOBase):
    """A stream that records the size of every read/write it is offered.

    Short-transfers on purpose: it accepts or returns at most ``limit``
    bytes per call, which is what a real pipe or socket does under load.
    """

    def __init__(self, data: bytes = b"", limit: int = 3) -> None:
        super().__init__()
        self.offered: list[int] = []
        self.written = bytearray()
        self._data = data
        self._pos = 0
        self._limit = limit

    def write(self, b: Any, /) -> int:
        view = memoryview(b)
        self.offered.append(len(view))
        take = min(len(view), self._limit)
        self.written.extend(view[:take])
        return take

    def readinto(self, b: Any, /) -> int:
        view = memoryview(b).cast("B")
        self.offered.append(len(view))
        remaining = len(self._data) - self._pos
        take = min(len(view), self._limit, remaining)
        view[:take] = self._data[self._pos : self._pos + take]
        self._pos += take
        return take

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return True


class TestExactWriter:
    """A large payload must reach the peer whole."""

    def test_loops_until_everything_is_written(self) -> None:
        """A stream that accepts 3 bytes a time must still receive it all."""
        raw = _RecordingRaw(limit=3)
        writer = _transport._ExactWriter(raw)
        payload = bytes(range(20))
        assert writer.write(payload) == len(payload)
        assert bytes(raw.written) == payload

    def test_clamps_each_underlying_call(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No single write may exceed the chunk limit, whatever it is set to."""
        monkeypatch.setattr(_transport, "_MAX_WRITE_CHUNK", 4)
        raw = _RecordingRaw(limit=4)
        writer = _transport._ExactWriter(raw)
        assert writer.write(bytes(20)) == 20
        assert max(raw.offered) <= 4

    def test_refuses_to_spin_on_a_stalled_peer(self) -> None:
        """A stream accepting nothing is an error, not an infinite loop."""
        raw = _RecordingRaw(limit=0)
        writer = _transport._ExactWriter(raw)
        with pytest.raises(OSError, match="not consuming"):
            writer.write(b"abc")


class TestClampedRaw:
    """A large message body must be delivered whole.

    The clamp sits under the buffering, so ``BufferedReader`` supplies the
    refill loop in C. That matters for correctness as well as speed: pyarrow
    does not retry a short read -- it raises ``Expected to be able to read N
    bytes for message body, got M`` -- so a clamp with nothing looping above
    it turns an intermittent EINVAL into a deterministic corrupt stream.
    """

    def test_clamps_each_underlying_call(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """No single read may exceed the chunk limit -- this is the EINVAL guard."""
        monkeypatch.setattr(_transport, "_MAX_READ_CHUNK", 4)
        raw = _RecordingRaw(data=bytes(range(20)), limit=99)
        clamped = _transport._ClampedRaw(raw)
        assert clamped.readinto(bytearray(20)) == 4
        assert max(raw.offered) <= 4

    def test_buffering_above_refills_across_the_clamp(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The whole request must still arrive, assembled by the layer above."""
        monkeypatch.setattr(_transport, "_MAX_READ_CHUNK", 4)
        payload = bytes(range(20))
        raw = _RecordingRaw(data=payload, limit=99)
        reader = io.BufferedReader(_transport._ClampedRaw(raw))
        assert reader.read(len(payload)) == payload
        assert max(raw.offered) <= 4

    def test_short_at_eof(self) -> None:
        """EOF returns what arrived; the caller reports the truncation."""
        raw = _RecordingRaw(data=b"abc", limit=3)
        reader = io.BufferedReader(_transport._ClampedRaw(raw))
        assert reader.read(10) == b"abc"

    def test_clamped_closes_the_stream_it_replaced(self) -> None:
        """Closing the replacement must release what the original owned."""
        # The socket transports hand us ``sock.makefile("rb")``, whose close
        # decrements the socket's io refcount. Dropping it on the floor would
        # leak that.
        original = io.BufferedReader(_RecordingRaw(data=b"hello", limit=5))
        replacement = _transport._clamped(original)
        assert replacement.read(5) == b"hello"
        replacement.close()
        assert original.closed

    def test_clamped_passes_through_a_stream_with_no_raw(self) -> None:
        """A stream exposing no raw layer is left alone rather than broken."""
        plain = _RecordingRaw(data=b"xyz", limit=3)
        assert _transport._clamped(plain) is plain
