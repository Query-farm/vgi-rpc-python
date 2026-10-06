# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""``conformance.Secondary.v1`` -- the second application protocol every conformance worker hosts.

Normative in ``tools/cross-port/specs/MULTI_PROTOCOL_HOSTING.md``; this module is
the reference implementation and the constants the shared suite asserts.

It exists to make three things observable that a single-protocol worker hides.

**Routing by pair.**  ``echo_string`` deliberately repeats the name *and* the
signature of ``ConformanceService.echo_string``.  A server that keyed dispatch
on the bare method name would answer one with the other; because this one
prefixes its reply, the mistake is a wrong value rather than a coincidentally
right one.  The plan named this method ``echo``; the primary has no ``echo``,
and a collision is the point, so it takes the primary's ``echo_string``.

**A per-binding version gate.**  The secondary declares *no*
``protocol_version`` while the primary declares ``2.0.0``.  A client therefore
sends no version on secondary calls, and a server that gates every call against
the primary's version (rather than the resolved binding's) refuses them.

**The error model.**  ``fail`` raises whatever code and kind the caller names,
with a fixed set of details -- one catalog type, one ``RetryInfo`` when asked,
and one type no client knows -- so a client's handling of each can be asserted
exactly.  ``fail_oversized`` raises details over the 4 KiB cap, built so that
dropping only the large element (rather than the whole array) is detectable.
"""

from __future__ import annotations

from typing import Any, ClassVar, Protocol

from vgi_rpc.errors import (
    BadRequest,
    Code,
    ErrorInfo,
    FieldViolation,
    RetryInfo,
    StatusError,
)

__all__ = [
    "FAIL_ERROR_INFO",
    "INVALID_CODE_KIND",
    "OVERSIZED_KIND",
    "OVERSIZED_PADDING_BYTES",
    "PROBE_DETAIL",
    "SECONDARY_ECHO_PREFIX",
    "SECONDARY_PROTOCOL_HASH",
    "SECONDARY_PROTOCOL_NAME",
    "Secondary",
    "SecondaryImpl",
    "conformance_extra_protocols",
    "expected_fail_details",
]

#: Routing key of the fixture protocol.
SECONDARY_PROTOCOL_NAME = "conformance.Secondary.v1"

#: Pinned digest of the secondary's description (WIRE_PROTOCOL.md §14).  Read
#: back off a running worker through reflection, never compared against a
#: locally computed copy -- that would prove only that the copy was made.
SECONDARY_PROTOCOL_HASH = "58557cf1611546ad22d1c379bc3ce1b04166082f78375e9fc959f0086347eab6"

#: What ``echo_string`` prepends.  The primary echoes verbatim; this is what
#: makes a mis-route a wrong answer rather than a right one by accident.
SECONDARY_ECHO_PREFIX = "secondary:"

#: The ``ErrorInfo`` every ``fail`` carries.  Fixed, so the metadata map's round
#: trip is asserted exactly.
FAIL_ERROR_INFO: dict[str, Any] = {"@type": "vgi_rpc.ErrorInfo", "metadata": {"fixture": SECONDARY_PROTOCOL_NAME}}

#: A detail type no client knows, legitimately named under this protocol.  A
#: client must keep the error and ignore the detail; one that rejects the batch,
#: or crashes decoding it, fails.
PROBE_DETAIL: dict[str, Any] = {
    "@type": f"{SECONDARY_PROTOCOL_NAME}.Probe",
    "note": "clients ignore detail types they do not know",
}

#: Kind ``fail`` answers with when asked for a code outside the closed set.
INVALID_CODE_KIND = "invalid_code"

#: Kind ``fail_oversized`` raises.
OVERSIZED_KIND = "details_oversized"

#: Size of the ``ErrorInfo`` padding ``fail_oversized`` carries -- over the cap
#: on its own, so the array cannot fit however compactly a port serializes it.
OVERSIZED_PADDING_BYTES = 5000


def expected_fail_details(retry_delay_seconds: float) -> list[dict[str, Any]]:
    """Return the detail array ``fail`` sends, in wire order.

    Args:
        retry_delay_seconds: The delay the caller passed.

    Returns:
        ``ErrorInfo``, then ``RetryInfo`` when the delay is positive, then the
        probe type.

    """
    details: list[dict[str, Any]] = [FAIL_ERROR_INFO]
    if retry_delay_seconds > 0:
        details.append(RetryInfo(retry_delay_seconds=retry_delay_seconds).to_json())
    details.append(PROBE_DETAIL)
    return details


class Secondary(Protocol):
    """The fixture protocol hosted beside ``ConformanceService``.

    No ``protocol_version`` on purpose -- see the module docstring.
    """

    protocol_name: ClassVar[str] = SECONDARY_PROTOCOL_NAME

    def echo_string(self, value: str) -> str:
        """Return ``SECONDARY_ECHO_PREFIX + value``; collides with the primary's method."""
        ...

    def fail(self, code: str, kind: str, retry_delay_seconds: float) -> None:
        """Raise an error with *code*, *kind* (absent when empty) and the fixed details."""
        ...

    def fail_oversized(self) -> None:
        """Raise an error whose details exceed the 4 KiB cap."""
        ...


class SecondaryImpl:
    """Reference implementation of :class:`Secondary`."""

    def echo_string(self, value: str) -> str:
        """Echo with the prefix that makes a mis-route visible."""
        return SECONDARY_ECHO_PREFIX + value

    def fail(self, code: str, kind: str, retry_delay_seconds: float) -> None:
        """Raise the requested error.

        A *code* outside the closed set is itself refused, with
        ``INVALID_ARGUMENT`` / ``invalid_code`` and a ``BadRequest`` naming the
        field -- which doubles as the ``BadRequest`` round trip.

        Args:
            code: The canonical code name to raise.
            kind: The reason; empty means none.
            retry_delay_seconds: ``RetryInfo`` delay; none is sent unless positive.

        Raises:
            StatusError: Always.

        """
        if code not in Code.__members__:
            raise StatusError(
                f"{code!r} is not a canonical error code",
                code=Code.INVALID_ARGUMENT,
                kind=INVALID_CODE_KIND,
                details=[
                    BadRequest(
                        field_violations=(FieldViolation(field="code", description="must be a canonical code name"),)
                    )
                ],
            )
        raise StatusError(
            f"conformance.Secondary.v1 fail: {code} {kind}".rstrip(),
            code=Code(code),
            kind=kind or None,
            details=expected_fail_details(retry_delay_seconds),
        )

    def fail_oversized(self) -> None:
        """Raise an error whose detail array cannot fit the cap.

        ``RetryInfo`` comes first and is small: a server that drops only the
        element that does not fit -- rather than the whole array -- keeps it,
        and then the error also turns retryable, which is the observable harm
        of partial delivery.

        Raises:
            StatusError: Always.

        """
        raise StatusError(
            "conformance.Secondary.v1 fail_oversized: details exceed 4 KiB",
            code=Code.RESOURCE_EXHAUSTED,
            kind=OVERSIZED_KIND,
            details=[
                RetryInfo(retry_delay_seconds=1),
                ErrorInfo(metadata={"padding": "x" * OVERSIZED_PADDING_BYTES}),
            ],
        )


def conformance_extra_protocols() -> list[tuple[type, object]]:
    """Return the ``extra_protocols`` every conformance worker registers.

    Returns:
        ``[(Secondary, SecondaryImpl())]`` -- registered after the primary, so
        reflection lists it second.

    """
    return [(Secondary, SecondaryImpl())]
