# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Protocol version negotiation, v1 (``tools/cross-port/specs/PROTOCOL_VERSIONS_V1.md``).

Every server advertises, per protocol it gates, the inclusive window of
``major.minor`` versions it accepts::

    VGI-Protocol-Versions: vgi.v3=3.4-3.4, vgi.secret.v1=1.0-1.0

on every HTTP response, and as the ``vgi_rpc.protocol_versions`` metadata
value on reflection responses, ``__transport_options__`` replies and
``protocol_version_unsupported`` refusals on every transport.  A client keeps
an :class:`EndpointNegotiation` per endpoint, learns the server's windows from
whatever it sees first, and speaks the highest version both sides support.

This module is pure: parsing, formatting and the negotiation state.  The
server and client wiring live with the transports.
"""

from __future__ import annotations

import dataclasses
import re
import threading
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from vgi_rpc.metadata import parse_version
from vgi_rpc.rpc._common import ProtocolVersionError

if TYPE_CHECKING:
    from vgi_rpc.rpc._common import RpcError

__all__ = [
    "CLIENT_PROTOCOL_VERSIONS_HEADER",
    "CLIENT_PROTOCOL_VERSIONS_KEY",
    "PROTOCOL_VERSIONS_HEADER",
    "PROTOCOL_VERSIONS_KEY",
    "PROTOCOL_VERSION_UNSUPPORTED",
    "ClientVersioning",
    "EndpointNegotiation",
    "NoCommonVersionError",
    "VersionRange",
    "format_versions",
    "highest_common",
    "is_retry_safe",
    "is_version_refusal",
    "parse_versions",
]

#: HTTP response header carrying the server's windows.  On every response.
PROTOCOL_VERSIONS_HEADER = "VGI-Protocol-Versions"
#: HTTP request header carrying the client's own windows.  Informational.
CLIENT_PROTOCOL_VERSIONS_HEADER = "VGI-Client-Protocol-Versions"
#: Metadata key carrying the server's windows (describe, refusals, transport options).
PROTOCOL_VERSIONS_KEY = b"vgi_rpc.protocol_versions"
#: Request metadata key carrying the client's own windows.  Informational.
CLIENT_PROTOCOL_VERSIONS_KEY = b"vgi_rpc.protocol_versions_client"
#: The ``error_kind`` of a refusal.
PROTOCOL_VERSION_UNSUPPORTED = "protocol_version_unsupported"

_MAJOR_MINOR = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)$")
#: A protocol name as it may appear on the left of ``=``: the routing-key
#: alphabet, which never contains ``=``, ``,`` or whitespace.
_NAME = re.compile(r"^[A-Za-z0-9_.:/-]+$")


def _parse_major_minor(text: str) -> tuple[int, int]:
    """Parse ``MAJOR.MINOR``; a full ``MAJOR.MINOR.PATCH`` is accepted and its patch dropped.

    Args:
        text: The version text.

    Returns:
        ``(major, minor)``.

    Raises:
        ValueError: Not a canonical ``MAJOR.MINOR`` (or semver).

    """
    text = text.strip()
    m = _MAJOR_MINOR.match(text)
    if m is not None:
        return int(m.group(1)), int(m.group(2))
    major, minor, _ = parse_version(text)
    return major, minor


@dataclasses.dataclass(frozen=True, order=True)
class VersionRange:
    """An inclusive ``min..max`` window of ``(major, minor)`` versions.

    Attributes:
        min: The oldest version accepted.
        max: The newest version accepted.

    """

    min: tuple[int, int]
    max: tuple[int, int]

    def __post_init__(self) -> None:
        """Reject an inverted window."""
        if self.min > self.max:
            raise ValueError(f"protocol version range {self} has min above max")

    @classmethod
    def parse(cls, text: str) -> VersionRange:
        """Parse ``MIN-MAX`` (each ``MAJOR.MINOR``), or a single ``MAJOR.MINOR`` meaning ``V-V``.

        Args:
            text: The range text, e.g. ``"3.4-3.4"``.

        Returns:
            The parsed range.

        Raises:
            ValueError: The text is not a range.

        """
        lo, sep, hi = text.strip().partition("-")
        low = _parse_major_minor(lo)
        high = _parse_major_minor(hi) if sep else low
        return cls(low, high)

    @classmethod
    def exactly(cls, version: str) -> VersionRange:
        """Return the single-version window of a semver's ``major.minor``.

        Args:
            version: Canonical semver ``MAJOR.MINOR.PATCH`` (or ``MAJOR.MINOR``).

        Returns:
            ``V-V``.

        """
        mm = _parse_major_minor(version)
        return cls(mm, mm)

    def __contains__(self, version: object) -> bool:
        """Whether a ``(major, minor)`` (or longer tuple) lies inside the window."""
        if not isinstance(version, tuple) or len(version) < 2:
            return False
        mm = (int(version[0]), int(version[1]))
        return self.min <= mm <= self.max

    def __str__(self) -> str:
        """Wire form: ``MIN-MAX``, always both ends."""
        return f"{self.min[0]}.{self.min[1]}-{self.max[0]}.{self.max[1]}"


def _mm(version: tuple[int, int]) -> str:
    return f"{version[0]}.{version[1]}"


def format_versions(ranges: Mapping[str, VersionRange] | Iterable[tuple[str, VersionRange]]) -> str:
    """Render ``name=MIN-MAX[, name=MIN-MAX]...`` in the given order.

    Args:
        ranges: Protocol name to window, in the order to advertise them.

    Returns:
        The header / metadata value.

    """
    items = ranges.items() if isinstance(ranges, Mapping) else ranges
    return ", ".join(f"{name}={rng}" for name, rng in items)


def parse_versions(text: str | bytes | None) -> dict[str, VersionRange]:
    """Parse a ``VGI-Protocol-Versions`` value.  Tolerant: a malformed entry is skipped.

    Entries are separated by commas and may be surrounded by whitespace; order
    carries no meaning.  An entry this parser cannot read is dropped rather than
    failing the whole value, so a later syntax extension degrades to "that one
    protocol is unadvertised" -- the pre-spec fallback -- instead of an error.
    A name that repeats keeps its first window.

    Args:
        text: The header or metadata value, or ``None``.

    Returns:
        Protocol name to window; empty when absent or wholly unreadable.

    """
    if text is None:
        return {}
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="replace")
    out: dict[str, VersionRange] = {}
    for entry in text.split(","):
        name, sep, rng = entry.strip().partition("=")
        name = name.strip()
        if not sep or not _NAME.match(name) or name in out:
            continue
        try:
            out[name] = VersionRange.parse(rng)
        except ValueError:
            continue
    return out


def highest_common(a: VersionRange, b: VersionRange) -> tuple[int, int] | None:
    """Return the highest ``(major, minor)`` inside both windows, or ``None`` when they are disjoint.

    Args:
        a: One window.
        b: The other.

    Returns:
        ``min(a.max, b.max)`` when it is at least both minimums, else ``None``.

    """
    top = min(a.max, b.max)
    if top < a.min or top < b.min:
        return None
    return top


@dataclasses.dataclass(frozen=True)
class ClientVersioning:
    """What a client bound to one Protocol speaks.

    Attributes:
        protocol: The wire name.
        version: The declared ``protocol_version`` (the newest the client speaks).
        range: The window the client supports; its ``max`` is ``version``'s major.minor.

    """

    protocol: str
    version: str
    range: VersionRange

    @property
    def patch(self) -> int:
        """The client's patch level, sent with whatever major.minor is negotiated."""
        return parse_version(self.version)[2]

    def advertised(self) -> str:
        """Return the client's ``VGI-Client-Protocol-Versions`` / metadata value."""
        return format_versions({self.protocol: self.range})

    def wire_version(self, major_minor: tuple[int, int]) -> str:
        """``MAJOR.MINOR.PATCH`` to send for a negotiated major.minor: the client's patch rides along."""
        return f"{major_minor[0]}.{major_minor[1]}.{self.patch}"

    @classmethod
    def from_protocol(cls, protocol: type, wire_name: str) -> ClientVersioning | None:
        """Read the Protocol's declaration; ``None`` when it declares no version.

        ``protocol_version`` is the newest version the client speaks.  An
        optional ``protocol_version_range: ClassVar[str]`` (``"MIN-MAX"``)
        widens the window downward; its ``MAX`` must equal ``protocol_version``'s
        major.minor.  Both are read with ``vars()`` so a subclass that does not
        redeclare them claims nothing.

        Args:
            protocol: The Protocol class.
            wire_name: Its routing key.

        Returns:
            The client's versioning, or ``None``.

        Raises:
            TypeError: A declaration is not a string.
            ValueError: A declaration is malformed or inconsistent.

        """
        raw_version = vars(protocol).get("protocol_version")
        if raw_version is None:
            return None
        if not isinstance(raw_version, str):
            raise TypeError(f"{protocol.__name__}.protocol_version must be a str, got {type(raw_version).__name__}")
        rng = declared_range(protocol, raw_version)
        return cls(protocol=wire_name, version=raw_version, range=rng)


def declared_range(protocol: type, version: str) -> VersionRange:
    """Return the window a Protocol declares: ``protocol_version_range``, or exactly ``protocol_version``.

    Args:
        protocol: The Protocol class.
        version: Its validated ``protocol_version``.

    Returns:
        The declared window.

    Raises:
        TypeError: ``protocol_version_range`` is not a string.
        ValueError: It is malformed, or its ``MAX`` is not ``version``'s major.minor.

    """
    parts = parse_version(version)
    raw_range = vars(protocol).get("protocol_version_range")
    if raw_range is None:
        return VersionRange((parts[0], parts[1]), (parts[0], parts[1]))
    if not isinstance(raw_range, str):
        raise TypeError(f"{protocol.__name__}.protocol_version_range must be a str, got {type(raw_range).__name__}")
    rng = VersionRange.parse(raw_range)
    if rng.max != (parts[0], parts[1]):
        raise ValueError(
            f"{protocol.__name__}.protocol_version_range {raw_range!r} must end at protocol_version "
            f"{version}'s major.minor ({parts[0]}.{parts[1]}): protocol_version is the newest version "
            f"spoken, and the range widens it downward only."
        )
    return rng


class NoCommonVersionError(ProtocolVersionError):
    """The client and server windows for a protocol do not overlap (raised client-side, before sending).

    A :class:`~vgi_rpc.rpc.ProtocolVersionError`, so it carries the
    ``protocol_version_unsupported`` kind a server refusal does, and a caller
    handles "the server refused my version" and "I know the server will refuse
    every version I speak" in one place.  ``client_range`` and ``server_range``
    hold the two windows.
    """

    def __init__(
        self, protocol: str, client_range: VersionRange, server_range: VersionRange, endpoint: str = ""
    ) -> None:
        """Build the §6 message naming both windows and which side to upgrade.

        Args:
            protocol: The protocol.
            client_range: What the client supports.
            server_range: What the server advertised.
            endpoint: Where the server is, for the message; may be empty.

        """
        self.client_range = client_range
        self.server_range = server_range
        where = f" at {endpoint}" if endpoint else ""
        if server_range.max < client_range.min:
            advice = (
                f"The server is too old (this client is too new for it): upgrade the worker to one supporting "
                f"{protocol} {_mm(client_range.min)} or newer, or use a client that supports {protocol} "
                f"{_mm(server_range.max)}."
            )
        else:
            advice = (
                f"The client is too old (the server is too new for it): upgrade the client to one supporting "
                f"{protocol} {_mm(server_range.min)} or newer, or use a worker that supports {protocol} "
                f"{_mm(client_range.max)}."
            )
        super().__init__(
            f"{PROTOCOL_VERSION_UNSUPPORTED}: no common {protocol} version. This client supports "
            f"{protocol} {client_range}; the server{where} supports {protocol} {server_range}. {advice}",
            protocol=protocol,
            server_version=str(server_range),
            server_versions=format_versions({protocol: server_range}),
            client_versions=format_versions({protocol: client_range}),
        )


#: Version texts a pre-spec server's refusal carries ("Server: 3.4.0", "server requires 3.4.0").
_LEGACY_SERVER_VERSION = re.compile(r"(?:Server:\s*|server requires\s+)(\d+\.\d+)(?:\.\d+)?")


def ranges_from_error(err: RpcError) -> dict[str, VersionRange]:
    """Return the server windows a ``protocol_version_unsupported`` refusal carries (``ErrorInfo`` metadata).

    Args:
        err: The refusal.

    Returns:
        The windows; empty when the refusal carries none.

    """
    info = err.error_info()
    if info is None:
        return {}
    return parse_versions(info.metadata.get(PROTOCOL_VERSIONS_KEY.decode()))


def legacy_server_version(err: RpcError) -> tuple[int, int] | None:
    """Return the single server version a pre-spec refusal names, when one is present.

    Servers released before this spec (the 3.4.0 SDKs) refuse with
    ``protocol_version_mismatch`` and a message naming ``Server: X.Y.Z``.  Per
    §6 the client then treats the server as speaking exactly that version.

    Args:
        err: The refusal.

    Returns:
        ``(major, minor)`` or ``None``.

    """
    if err.error_kind not in ("protocol_version_mismatch", PROTOCOL_VERSION_UNSUPPORTED):
        return None
    texts = [err.error_message]
    failure = err.precondition_failure()
    if failure is not None:
        texts.extend(v.description for v in failure.violations)
    for text in texts:
        m = _LEGACY_SERVER_VERSION.search(text)
        if m is not None:
            return _parse_major_minor(m.group(1))
    return None


class EndpointNegotiation:
    """Negotiation state for one endpoint (one worker location / connection pool).

    Holds the server windows learned from any response, and answers "which
    version do I send for this protocol".  Thread-safe: an HTTP client pool is
    shared across threads.

    The decision, per protocol (§6):

    1. The server advertised a window for it: the highest common version, or
       :class:`NoCommonVersionError` when there is none.
    2. A pre-spec refusal named a single server version: that one.
    3. Otherwise -- nothing seen yet, or a server that does not gate the
       protocol -- the client's own newest version, which is today's behaviour.
    """

    __slots__ = ("_advertised", "_endpoint", "_legacy", "_lock", "_seen")

    def __init__(self, endpoint: str = "") -> None:
        """Start empty: nothing learned.

        Args:
            endpoint: Where the server is; used only in error messages.

        """
        self._endpoint = endpoint
        self._lock = threading.Lock()
        self._advertised: dict[str, VersionRange] = {}
        self._legacy: dict[str, tuple[int, int]] = {}
        self._seen = False

    @property
    def endpoint(self) -> str:
        """Where the server is."""
        return self._endpoint

    @property
    def advertised(self) -> dict[str, VersionRange]:
        """A copy of the windows learned so far."""
        with self._lock:
            return dict(self._advertised)

    @property
    def has_advertisement(self) -> bool:
        """Whether any response so far carried the advertisement."""
        with self._lock:
            return self._seen

    def observe(self, value: str | bytes | None) -> None:
        """Record an advertisement read from a response; absent is ignored.

        A present value replaces what was learned: the newest response is the
        server's current statement (a redeploy behind one URL changes it).

        Args:
            value: The header / metadata value, or ``None`` when the response had none.

        """
        if value is None:
            return
        parsed = parse_versions(value)
        with self._lock:
            self._advertised = parsed
            self._seen = True
            self._legacy.clear()

    def observe_refusal(self, protocol: str, err: RpcError) -> None:
        """Learn from a ``protocol_version_unsupported`` (or pre-spec mismatch) refusal.

        Args:
            protocol: The protocol the refused request was addressed to.
            err: The refusal.

        """
        ranges = ranges_from_error(err)
        if ranges:
            with self._lock:
                self._advertised = ranges
                self._seen = True
                self._legacy.clear()
            return
        legacy = legacy_server_version(err)
        if legacy is not None:
            with self._lock:
                if not self._seen:
                    self._legacy[protocol] = legacy

    def choose(self, client: ClientVersioning) -> tuple[int, int]:
        """Return the ``(major, minor)`` to speak for *client*'s protocol.

        Args:
            client: The client's versioning for the protocol.

        Returns:
            The version to send.

        Raises:
            NoCommonVersionError: The server's window and the client's do not overlap.

        """
        with self._lock:
            server = self._advertised.get(client.protocol)
            legacy = self._legacy.get(client.protocol)
        if server is not None:
            common = highest_common(client.range, server)
            if common is None:
                raise NoCommonVersionError(client.protocol, client.range, server, self._endpoint)
            return common
        if legacy is not None:
            if legacy not in client.range:
                raise NoCommonVersionError(client.protocol, client.range, VersionRange(legacy, legacy), self._endpoint)
            return legacy
        return client.range.max

    def wire_version(self, client: ClientVersioning) -> str:
        """Return the ``vgi_rpc.protocol_version`` value to send for *client*'s protocol.

        Args:
            client: The client's versioning for the protocol.

        Returns:
            ``MAJOR.MINOR.PATCH``.

        """
        return client.wire_version(self.choose(client))


#: The reflection protocol: describe / list_protocols, idempotent by definition.
_REFLECTION = "vgi_rpc.Reflection.v1"
_SAFE_IDEMPOTENCY = ("no_side_effects", "idempotent")


def is_version_refusal(err: BaseException) -> bool:
    """Whether *err* is a server's version refusal (this spec's kind, or the pre-spec one).

    Args:
        err: An error raised by a call.

    Returns:
        ``True`` for ``protocol_version_unsupported`` and ``protocol_version_mismatch``.

    """
    return getattr(err, "error_kind", None) in (PROTOCOL_VERSION_UNSUPPORTED, "protocol_version_mismatch")


def is_retry_safe(protocol_name: str, idempotency: str) -> bool:
    """Whether a refused call may be re-sent once at a renegotiated version (§6).

    Only calls with no side effects to repeat: reflection, and methods whose
    description declares ``no_side_effects`` or ``idempotent``.  A refusal is
    raised before the method runs, so even an unsafe method has not executed --
    but a client cannot tell a refusal from a gateway that lost a success, so
    the rule stays the spec's: never retry what is not declared idempotent.

    Args:
        protocol_name: The protocol the call was addressed to.
        idempotency: The method's declared idempotency level.

    Returns:
        Whether a single automatic retry is allowed.

    """
    return protocol_name == _REFLECTION or idempotency in _SAFE_IDEMPOTENCY
