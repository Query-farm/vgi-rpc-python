# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The standard token codec: compress, tag, then seal.

Every sealed payload vgi-rpc mints -- the HTTP cursor and call tokens, and the
inline and stored :class:`~vgi_rpc.state_ref.StateRef` payloads -- is
compressed *before* it is sealed (``STATE_REF_V1.md`` §7).  Once sealed a
token is ciphertext, and no outer codec can find the redundancy in it.

The layout is one codec byte followed by the body::

    sealed_payload := codec (1 byte) || body

==========  ==================================  ====================================
``codec``   Body                                Notes
==========  ==================================  ====================================
``0x00``    raw framed plaintext                used when compression does not shrink
``0x01``    zstd frame                          the default; level 3
``0x02``    raw DEFLATE (RFC 1951)              for runtimes without zstd
other       --                                  reserved; refused
==========  ==================================  ====================================

A reader must accept every codec its own writer can emit.  This writer emits
``0x00`` and ``0x01``; the reader also accepts ``0x02`` so payloads from a
DEFLATE-only writer sharing this process's key (a test, a future port of the
same server) still open.
"""

from __future__ import annotations

import threading
import zlib

import zstandard

__all__ = [
    "CODEC_DEFLATE",
    "CODEC_RAW",
    "CODEC_ZSTD",
    "MAX_TOKEN_PLAINTEXT_BYTES",
    "TokenCodecError",
    "pack",
    "unpack",
]

CODEC_RAW = 0x00
"""Body is the plaintext as-is."""
CODEC_ZSTD = 0x01
"""Body is one zstd frame."""
CODEC_DEFLATE = 0x02
"""Body is a raw DEFLATE stream (RFC 1951, no zlib header)."""

MAX_TOKEN_PLAINTEXT_BYTES = 64 << 20
"""Decompression bound for tokens (64 MiB)."""

# Level 3: measured the same speed as level 1 on token-sized payloads and
# slightly smaller; levels that compress materially better (9, 19) cost 8x
# and 84x the CPU for a few hundred bytes.
_ZSTD_LEVEL = 3

#: Per-thread codecs.  python-zstandard does not promise that two threads may
#: call into one codec object at once, and this runs on a WSGI thread pool.
#: Reusing a compressor measured 0.67us per token against 1.04us to build one.
_codecs = threading.local()


class TokenCodecError(ValueError):
    """A payload carries an unknown codec tag or does not decompress."""


def _compressor() -> zstandard.ZstdCompressor:
    codec = getattr(_codecs, "compressor", None)
    if codec is None:
        codec = zstandard.ZstdCompressor(level=_ZSTD_LEVEL)
        _codecs.compressor = codec
    return codec


def _decompressor() -> zstandard.ZstdDecompressor:
    codec = getattr(_codecs, "decompressor", None)
    if codec is None:
        codec = zstandard.ZstdDecompressor()
        _codecs.decompressor = codec
    return codec


def pack(plaintext: bytes) -> bytes:
    """Compress *plaintext* when that makes it smaller and tag the codec used.

    Args:
        plaintext: The bytes to pack.

    Returns:
        ``0x01 || zstd(plaintext)`` when that is shorter than *plaintext*,
        else ``0x00 || plaintext`` -- so a small payload never grows by more
        than the tag byte.

    """
    packed = _compressor().compress(plaintext)
    if len(packed) < len(plaintext):
        return bytes((CODEC_ZSTD,)) + packed
    return bytes((CODEC_RAW,)) + plaintext


def unpack(data: bytes, max_output_bytes: int = MAX_TOKEN_PLAINTEXT_BYTES) -> bytes:
    """Reverse :func:`pack`, bounding the decompressed size.

    Args:
        data: A packed payload, codec tag first.
        max_output_bytes: Largest decompressed size accepted.

    Returns:
        The plaintext.

    Raises:
        TokenCodecError: The payload is empty, its codec tag is unknown, it
            does not decompress, or it decompresses past *max_output_bytes*.

    """
    if not data:
        raise TokenCodecError("empty payload")
    tag, body = data[0], data[1:]
    if tag == CODEC_RAW:
        if len(body) > max_output_bytes:
            raise TokenCodecError("payload exceeds the decompression bound")
        return bytes(body)
    if tag == CODEC_ZSTD:
        # ``max_output_size`` bounds only frames that do not declare their
        # content size; a frame that declares one is decompressed to that size
        # whatever the bound.  So check the declared size first.
        try:
            declared = zstandard.frame_content_size(body)
        except zstandard.ZstdError as exc:
            raise TokenCodecError("zstd body does not decompress") from exc
        if declared > max_output_bytes:
            raise TokenCodecError("payload exceeds the decompression bound")
        try:
            out = _decompressor().decompress(body, max_output_size=max_output_bytes)
        except zstandard.ZstdError as exc:
            raise TokenCodecError("zstd body does not decompress") from exc
        if len(out) > max_output_bytes:
            raise TokenCodecError("payload exceeds the decompression bound")
        return out
    if tag == CODEC_DEFLATE:
        inflater = zlib.decompressobj(-zlib.MAX_WBITS)
        try:
            out = inflater.decompress(body, max_output_bytes)
        except zlib.error as exc:
            raise TokenCodecError("deflate body does not decompress") from exc
        if inflater.unconsumed_tail or not inflater.eof:
            raise TokenCodecError("deflate body is truncated or exceeds the decompression bound")
        return out
    raise TokenCodecError(f"unknown codec tag 0x{tag:02x}")
