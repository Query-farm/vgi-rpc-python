"""The ``__transport_options__`` RPC method — transport capability negotiation.

A framework-level handshake, parallel to ``__describe__`` (see
:mod:`vgi_rpc.introspect`). The client calls it once per worker, before
``init``, to discover which transport features the worker supports; the
shared-memory side-channel (and, later, compression / AEAD) is used only when
both peers advertise support.

Capabilities ride as request/response *metadata* under the
``vgi_rpc.transport.*`` namespace (not as batch rows), consistent with how the
SHM segment itself is advertised and decode-safe (the params batch stays empty).
Each capability is one ``vgi_rpc.transport.<name>`` key with a string value;
unknown keys are ignored, so the set is open-ended.
"""

from __future__ import annotations

from vgi_rpc.metadata import TRANSPORT_SHM_KEY

__all__ = [
    "TRANSPORT_OPTIONS_METHOD_NAME",
    "shm_available",
    "worker_transport_metadata",
]

TRANSPORT_OPTIONS_METHOD_NAME = "__transport_options__"


def shm_available() -> bool:
    """Whether this worker can use the POSIX shared-memory side-channel.

    The VGI shm channel is POSIX named shared memory (``shm_open``/``mmap``),
    which interoperates across the C++/Java/Go/Python peers on Linux and macOS.
    Windows' ``multiprocessing.shared_memory`` uses a different, non-interoperable
    backing, so shm is offered only on POSIX.
    """
    return True  # POSIX shm_open + Windows CreateFileMapping (multiprocessing.shared_memory)


def worker_transport_metadata() -> dict[bytes, bytes]:
    """Return this worker's transport capabilities as ``__transport_options__`` response metadata."""
    return {TRANSPORT_SHM_KEY: b"true" if shm_available() else b"false"}
