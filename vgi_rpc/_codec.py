# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Content-encoding codecs shared by the HTTP transport and external storage.

Kept in a module without ``vgi_rpc.rpc`` / ``vgi_rpc.http`` dependencies
so the storage layer (which is imported before ``vgi_rpc.http``) can
reach the codec dispatchers without circular imports.
"""

from __future__ import annotations

import enum
import zlib

__all__ = [
    "DecompressionError",
    "Encoding",
    "available_encodings",
    "compress",
    "decompress",
    "parse_encoding_list",
]


_GZIP_WBITS = 31  # 15-bit window + 16 = gzip wrapper around raw deflate
_DEFAULT_GZIP_LEVEL = 6
_DEFAULT_ZSTD_LEVEL = 3
_DECOMPRESS_CHUNK_BYTES = 65536


class Encoding(enum.Enum):
    """Content-encoding codecs supported by the HTTP transport.

    Wire names (``value``) match the tokens used in ``Content-Encoding`` /
    ``Accept-Encoding`` headers and in ``VGI-Supported-Encodings``.

    ``IDENTITY`` is the no-op transform, not a compressor.  It exists so a
    client can *explicitly* ask for an uncompressed response by listing it
    in an accept header — otherwise "no compression" is only reachable by
    accident, when nothing the client offers happens to be producible.
    It is deliberately excluded from :func:`available_encodings` (and so
    from the advertised ``VGI-Supported-Encodings`` set), since every
    implementation can always do it and saying so carries no information.
    """

    ZSTD = "zstd"
    GZIP = "gzip"
    IDENTITY = "identity"


class DecompressionError(Exception):
    """Raised when a codec-agnostic decompression fails or exceeds the cap."""


def _zstd_available() -> bool:
    try:
        import zstandard  # noqa: F401
    except ImportError:
        return False
    return True


def available_encodings() -> tuple[Encoding, ...]:
    """Return the encodings the runtime can actually compress/decompress.

    gzip is always available (stdlib ``zlib``).  zstd is optional —
    if ``zstandard`` isn't importable we drop it silently so a server
    without the package can still start and advertise gzip only.
    """
    if _zstd_available():
        return (Encoding.ZSTD, Encoding.GZIP)
    return (Encoding.GZIP,)


def parse_encoding_list(header_value: str) -> list[Encoding]:
    """Parse a comma-separated ``Accept-Encoding``-style header.

    Tokens not in :class:`Encoding` are skipped silently — the caller
    intersects with what *we* can do anyway.  Order is preserved so
    callers can honour the client's preference order.
    """
    out: list[Encoding] = []
    seen: set[Encoding] = set()
    for raw in header_value.split(","):
        token = raw.strip().lower()
        if not token:
            continue
        if ";" in token:
            token = token.split(";", 1)[0].strip()
        for enc in Encoding:
            if enc.value == token and enc not in seen:
                out.append(enc)
                seen.add(enc)
                break
    return out


def _compress_body_zstd(data: bytes, level: int) -> bytes:
    """Compress *data* using zstd at the given level."""
    import zstandard

    return zstandard.ZstdCompressor(level=level).compress(data)


def _decompress_body_zstd(data: bytes, *, max_output_size: int | None = None) -> bytes:
    """Decompress zstd-compressed *data* with optional output cap.

    See :func:`decompress` for the cap semantics.
    """
    import zstandard

    if max_output_size is None:
        return zstandard.ZstdDecompressor().decompress(data)

    # Refuse the frame up-front when the header claims more than allowed.
    params = zstandard.get_frame_parameters(data)
    if params.content_size != -1 and params.content_size > max_output_size:
        raise zstandard.ZstdError(
            f"Compressed frame declares decompressed size {params.content_size} bytes, "
            f"which exceeds max_output_size={max_output_size}"
        )

    if params.content_size != -1:
        return zstandard.ZstdDecompressor().decompress(data)

    decompressor = zstandard.ZstdDecompressor()
    chunks: list[bytes] = []
    total = 0
    with decompressor.stream_reader(data) as reader:
        while True:
            chunk = reader.read(min(_DECOMPRESS_CHUNK_BYTES, max_output_size - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > max_output_size:
                raise zstandard.ZstdError(f"Decompressed output exceeds max_output_size={max_output_size}")
            chunks.append(chunk)
    return b"".join(chunks)


def _compress_body_gzip(data: bytes, level: int) -> bytes:
    """Compress *data* as a gzip stream at the given level.

    Uses ``zlib.compressobj(wbits=31)`` which emits the gzip wrapper
    around raw deflate — cheaper than ``gzip.GzipFile`` (no file-object
    glue) and gives explicit streaming control.
    """
    co = zlib.compressobj(level, zlib.DEFLATED, _GZIP_WBITS)
    return co.compress(data) + co.flush(zlib.Z_FINISH)


def _decompress_body_gzip(data: bytes, *, max_output_size: int | None = None) -> bytes:
    """Decompress gzip-encoded *data* with optional output cap.

    gzip's ISIZE footer carries the uncompressed length mod 2^32, so it
    cannot be trusted for a bomb-cap precheck.  Defence-in-depth is a
    bounded streaming loop: feed input through ``decompressobj`` and
    bail the moment ``max_output_size`` is exceeded.
    """
    do = zlib.decompressobj(_GZIP_WBITS)
    if max_output_size is None:
        return do.decompress(data) + do.flush()

    chunks: list[bytes] = []
    total = 0
    remaining = data
    while remaining or do.unconsumed_tail:
        if do.unconsumed_tail:
            inbuf = do.unconsumed_tail
        else:
            inbuf, remaining = remaining, b""
        chunk = do.decompress(inbuf, _DECOMPRESS_CHUNK_BYTES)
        if chunk:
            total += len(chunk)
            if total > max_output_size:
                raise DecompressionError(f"Decompressed gzip output exceeds max_output_size={max_output_size}")
            chunks.append(chunk)
        if not chunk and not do.unconsumed_tail:
            break
    tail = do.flush()
    if tail:
        total += len(tail)
        if total > max_output_size:
            raise DecompressionError(f"Decompressed gzip output exceeds max_output_size={max_output_size}")
        chunks.append(tail)
    return b"".join(chunks)


def compress(encoding: Encoding, data: bytes, *, level: int | None = None) -> bytes:
    """Compress *data* with the given codec.

    ``level`` defaults are codec-specific: zstd ``3``, gzip ``6``.
    ``IDENTITY`` is the no-op transform and returns *data* unchanged.
    """
    if encoding is Encoding.IDENTITY:
        return data
    if encoding is Encoding.ZSTD:
        return _compress_body_zstd(data, _DEFAULT_ZSTD_LEVEL if level is None else level)
    if encoding is Encoding.GZIP:
        return _compress_body_gzip(data, _DEFAULT_GZIP_LEVEL if level is None else level)
    raise ValueError(f"Unsupported encoding: {encoding!r}")


def decompress(encoding: Encoding, data: bytes, *, max_output_size: int | None = None) -> bytes:
    """Decompress *data* with the given codec, bounded by *max_output_size*.

    ``IDENTITY`` returns *data* unchanged — ``Content-Encoding: identity``
    is a valid request header meaning "no transform applied", so it must
    pass through rather than 415.
    """
    if encoding is Encoding.IDENTITY:
        return data
    if encoding is Encoding.ZSTD:
        return _decompress_body_zstd(data, max_output_size=max_output_size)
    if encoding is Encoding.GZIP:
        return _decompress_body_gzip(data, max_output_size=max_output_size)
    raise ValueError(f"Unsupported encoding: {encoding!r}")
