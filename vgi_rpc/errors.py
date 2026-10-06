# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""The error model: a canonical code, an open reason, and typed details.

Every EXCEPTION batch carries three layers, adopted from gRPC's
``google.rpc.Status`` (WIRE_PROTOCOL.md §8):

======== ============================ ===================================================
Layer    Wire key                     Set
======== ============================ ===================================================
Code     ``vgi_rpc.error_code``       **Closed**: gRPC's sixteen codes minus ``OK``,
                                      sent as the code's *name* (``UNAVAILABLE``).
Reason   ``vgi_rpc.error_kind``       Open, unique within the protocol that raised it.
Details  ``vgi_rpc.error_details``    A JSON array of typed objects from a fixed catalog.
======== ============================ ===================================================

The code is what generic handling keys on -- retry or not, how to show it, and
later the HTTP status a proxy maps it to.  The kind is what a client branches
on.  The details carry machine-readable specifics: how long to wait, which
field was wrong, which resource the error concerns.

What this module fixes relative to gRPC is mostly *visibility*.  gRPC put its
details in a binary trailer that many clients, proxies and logs never decoded,
and its proxies silently truncate oversized trailers.  Here the three layers
are plain top-level metadata, mirrored into ``log_extra`` and the access log,
and the details array is capped at 4 KiB -- dropped *whole* when over, never
truncated, because a half-delivered detail list reads as a complete one.

Servers raise a :class:`StatusError` (or any exception that carries
``error_code`` / ``error_kind`` / ``error_details`` attributes); clients read
:attr:`vgi_rpc.rpc.RpcError.error_code` and friends, plus the typed accessors
:meth:`~vgi_rpc.rpc.RpcError.retry_info` and so on.
"""

from __future__ import annotations

import dataclasses
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from enum import StrEnum
from typing import Any, ClassVar, Self

__all__ = [
    "MAX_ERROR_DETAILS_BYTES",
    "AuthUnavailableError",
    "BadRequest",
    "Code",
    "ErrorDetail",
    "ErrorInfo",
    "FieldViolation",
    "Help",
    "HelpLink",
    "LocalizedMessage",
    "PreconditionFailure",
    "PreconditionViolation",
    "QuotaFailure",
    "QuotaViolation",
    "ResourceInfo",
    "RetryInfo",
    "StatusError",
    "decode_error_details",
    "encode_error_details",
    "error_code_of",
    "error_details_of",
    "error_kind_of",
    "is_retryable",
    "parse_error_detail",
]

#: Cap on the serialized ``vgi_rpc.error_details`` value, in UTF-8 bytes.  A
#: server whose array would exceed it omits the array entirely.
MAX_ERROR_DETAILS_BYTES = 4096


class Code(StrEnum):
    """The closed set of canonical error codes: gRPC's sixteen, minus ``OK``.

    The wire value is the member's *name* -- ``"UNAVAILABLE"``, not ``14`` --
    so a log line, a proxy rule and a client switch all read the same string.
    The set is closed: a client that receives a value it does not recognise
    treats it as :attr:`UNKNOWN`.
    """

    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    NOT_FOUND = "NOT_FOUND"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    FAILED_PRECONDITION = "FAILED_PRECONDITION"
    ABORTED = "ABORTED"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    UNIMPLEMENTED = "UNIMPLEMENTED"
    INTERNAL = "INTERNAL"
    UNAVAILABLE = "UNAVAILABLE"
    DATA_LOSS = "DATA_LOSS"
    UNAUTHENTICATED = "UNAUTHENTICATED"

    @classmethod
    def parse(cls, value: object) -> Code:
        """Read a wire value, mapping anything unrecognised to :attr:`UNKNOWN`.

        Args:
            value: The ``vgi_rpc.error_code`` value, or ``None`` when absent.

        Returns:
            The matching member, or :attr:`UNKNOWN`.

        """
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            try:
                return cls(value)
            except ValueError:
                return cls.UNKNOWN
        return cls.UNKNOWN


# ---------------------------------------------------------------------------
# The detail catalog
# ---------------------------------------------------------------------------


def _str(obj: Mapping[str, Any], key: str) -> str:
    value = obj.get(key, "")
    if not isinstance(value, str):
        raise ValueError(f"{key!r} must be a string")
    return value


def _objects(obj: Mapping[str, Any], key: str) -> list[Mapping[str, Any]]:
    value = obj.get(key, [])
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"{key!r} must be an array of objects")
    return value


@dataclasses.dataclass(frozen=True)
class ErrorInfo:
    """Extra context for the reason.

    The reason and its domain are already ``error_kind`` and the protocol, so,
    unlike gRPC's ``ErrorInfo``, neither is repeated here.

    Attributes:
        TYPE: The ``@type`` naming this detail on the wire.
        metadata: String-to-string context.  Never credentials or user data.

    """

    TYPE: ClassVar[str] = "vgi_rpc.ErrorInfo"

    metadata: Mapping[str, str] = dataclasses.field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        """Return the JSON object form, ``@type`` included."""
        return {"@type": self.TYPE, "metadata": dict(self.metadata)}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Self:
        """Build from the JSON object form.

        Args:
            obj: One decoded detail object.

        Returns:
            The typed detail.

        Raises:
            ValueError: If a field has the wrong shape.

        """
        raw = obj.get("metadata", {})
        if not isinstance(raw, Mapping) or not all(isinstance(v, str) for v in raw.values()):
            raise ValueError("'metadata' must be an object of strings")
        return cls(metadata={str(k): v for k, v in raw.items()})


@dataclasses.dataclass(frozen=True)
class RetryInfo:
    """How long to wait before retrying.

    Attributes:
        TYPE: The ``@type`` naming this detail on the wire.
        retry_delay_seconds: Seconds; a retry waits at least this long.

    """

    TYPE: ClassVar[str] = "vgi_rpc.RetryInfo"

    retry_delay_seconds: float

    def to_json(self) -> dict[str, Any]:
        """Return the JSON object form, ``@type`` included."""
        delay = self.retry_delay_seconds
        # A whole number travels as an integer so the common case reads the
        # same in every language's JSON encoder ("7", not "7.0").
        value: float | int = int(delay) if float(delay).is_integer() else float(delay)
        return {"@type": self.TYPE, "retry_delay_seconds": value}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Self:
        """Build from the JSON object form.

        Args:
            obj: One decoded detail object.

        Returns:
            The typed detail.

        Raises:
            ValueError: If the delay is missing, not a number, negative or not finite.

        """
        raw = obj.get("retry_delay_seconds")
        if isinstance(raw, bool) or not isinstance(raw, int | float):
            raise ValueError("'retry_delay_seconds' must be a number")
        if not math.isfinite(raw) or raw < 0:
            raise ValueError("'retry_delay_seconds' must be a finite, non-negative number")
        return cls(retry_delay_seconds=float(raw))


@dataclasses.dataclass(frozen=True)
class FieldViolation:
    """One wrong input."""

    field: str
    description: str = ""


@dataclasses.dataclass(frozen=True)
class BadRequest:
    """Which inputs were wrong.

    Attributes:
        TYPE: The ``@type`` naming this detail on the wire.
        field_violations: One entry per wrong input.

    """

    TYPE: ClassVar[str] = "vgi_rpc.BadRequest"

    field_violations: Sequence[FieldViolation] = ()

    def to_json(self) -> dict[str, Any]:
        """Return the JSON object form, ``@type`` included."""
        return {
            "@type": self.TYPE,
            "field_violations": [{"field": v.field, "description": v.description} for v in self.field_violations],
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Self:
        """Build from the JSON object form.

        Args:
            obj: One decoded detail object.

        Returns:
            The typed detail.

        Raises:
            ValueError: If a field has the wrong shape.

        """
        return cls(
            field_violations=tuple(
                FieldViolation(field=_str(v, "field"), description=_str(v, "description"))
                for v in _objects(obj, "field_violations")
            )
        )


@dataclasses.dataclass(frozen=True)
class PreconditionViolation:
    """One unmet precondition."""

    type: str
    subject: str = ""
    description: str = ""


@dataclasses.dataclass(frozen=True)
class PreconditionFailure:
    """What state must change before the call can succeed.

    Attributes:
        TYPE: The ``@type`` naming this detail on the wire.
        violations: One entry per unmet precondition.

    """

    TYPE: ClassVar[str] = "vgi_rpc.PreconditionFailure"

    violations: Sequence[PreconditionViolation] = ()

    def to_json(self) -> dict[str, Any]:
        """Return the JSON object form, ``@type`` included."""
        return {
            "@type": self.TYPE,
            "violations": [
                {"type": v.type, "subject": v.subject, "description": v.description} for v in self.violations
            ],
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Self:
        """Build from the JSON object form.

        Args:
            obj: One decoded detail object.

        Returns:
            The typed detail.

        Raises:
            ValueError: If a field has the wrong shape.

        """
        return cls(
            violations=tuple(
                PreconditionViolation(
                    type=_str(v, "type"), subject=_str(v, "subject"), description=_str(v, "description")
                )
                for v in _objects(obj, "violations")
            )
        )


@dataclasses.dataclass(frozen=True)
class QuotaViolation:
    """One exhausted limit."""

    subject: str
    description: str = ""


@dataclasses.dataclass(frozen=True)
class QuotaFailure:
    """Which limit was hit.

    Attributes:
        TYPE: The ``@type`` naming this detail on the wire.
        violations: One entry per exhausted limit.

    """

    TYPE: ClassVar[str] = "vgi_rpc.QuotaFailure"

    violations: Sequence[QuotaViolation] = ()

    def to_json(self) -> dict[str, Any]:
        """Return the JSON object form, ``@type`` included."""
        return {
            "@type": self.TYPE,
            "violations": [{"subject": v.subject, "description": v.description} for v in self.violations],
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Self:
        """Build from the JSON object form.

        Args:
            obj: One decoded detail object.

        Returns:
            The typed detail.

        Raises:
            ValueError: If a field has the wrong shape.

        """
        return cls(
            violations=tuple(
                QuotaViolation(subject=_str(v, "subject"), description=_str(v, "description"))
                for v in _objects(obj, "violations")
            )
        )


@dataclasses.dataclass(frozen=True)
class ResourceInfo:
    """Which object the error concerns.

    Attributes:
        TYPE: The ``@type`` naming this detail on the wire.
        resource_type: The kind of resource, e.g. ``"report"``.
        resource_name: Its name or identifier.
        owner: Its owner, when meaningful.
        description: What went wrong with it.

    """

    TYPE: ClassVar[str] = "vgi_rpc.ResourceInfo"

    resource_type: str = ""
    resource_name: str = ""
    owner: str = ""
    description: str = ""

    def to_json(self) -> dict[str, Any]:
        """Return the JSON object form, ``@type`` included."""
        return {
            "@type": self.TYPE,
            "resource_type": self.resource_type,
            "resource_name": self.resource_name,
            "owner": self.owner,
            "description": self.description,
        }

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Self:
        """Build from the JSON object form.

        Args:
            obj: One decoded detail object.

        Returns:
            The typed detail.

        Raises:
            ValueError: If a field has the wrong shape.

        """
        return cls(
            resource_type=_str(obj, "resource_type"),
            resource_name=_str(obj, "resource_name"),
            owner=_str(obj, "owner"),
            description=_str(obj, "description"),
        )


@dataclasses.dataclass(frozen=True)
class HelpLink:
    """One pointer to documentation."""

    description: str
    url: str


@dataclasses.dataclass(frozen=True)
class Help:
    """Where to read more.

    Attributes:
        TYPE: The ``@type`` naming this detail on the wire.
        links: Documentation pointers.

    """

    TYPE: ClassVar[str] = "vgi_rpc.Help"

    links: Sequence[HelpLink] = ()

    def to_json(self) -> dict[str, Any]:
        """Return the JSON object form, ``@type`` included."""
        return {"@type": self.TYPE, "links": [{"description": v.description, "url": v.url} for v in self.links]}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Self:
        """Build from the JSON object form.

        Args:
            obj: One decoded detail object.

        Returns:
            The typed detail.

        Raises:
            ValueError: If a field has the wrong shape.

        """
        return cls(
            links=tuple(
                HelpLink(description=_str(v, "description"), url=_str(v, "url")) for v in _objects(obj, "links")
            )
        )


@dataclasses.dataclass(frozen=True)
class LocalizedMessage:
    """Text that is safe to show an end user.

    ``error_message`` stays developer-facing English, as in gRPC; this is the
    one place user-facing text belongs.

    Attributes:
        TYPE: The ``@type`` naming this detail on the wire.
        locale: BCP 47 tag, e.g. ``"en-US"``.
        message: The localized text.

    """

    TYPE: ClassVar[str] = "vgi_rpc.LocalizedMessage"

    locale: str
    message: str

    def to_json(self) -> dict[str, Any]:
        """Return the JSON object form, ``@type`` included."""
        return {"@type": self.TYPE, "locale": self.locale, "message": self.message}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> Self:
        """Build from the JSON object form.

        Args:
            obj: One decoded detail object.

        Returns:
            The typed detail.

        Raises:
            ValueError: If a field has the wrong shape.

        """
        return cls(locale=_str(obj, "locale"), message=_str(obj, "message"))


type ErrorDetail = (
    ErrorInfo | RetryInfo | BadRequest | PreconditionFailure | QuotaFailure | ResourceInfo | Help | LocalizedMessage
)
"""Any member of the fixed detail catalog."""

_CATALOG: dict[str, type[ErrorDetail]] = {
    cls.TYPE: cls
    for cls in (
        ErrorInfo,
        RetryInfo,
        BadRequest,
        PreconditionFailure,
        QuotaFailure,
        ResourceInfo,
        Help,
        LocalizedMessage,
    )
}

_RESERVED_PREFIX = "vgi_rpc."


def parse_error_detail(obj: object) -> ErrorDetail | None:
    """Decode one detail object, or ``None`` when it is unknown or malformed.

    Clients ignore detail types they do not know, and a malformed known type
    is treated as absent rather than failing the error it rides on: the error
    is the news, the detail is commentary.

    Args:
        obj: One element of the decoded ``vgi_rpc.error_details`` array.

    Returns:
        The typed detail, or ``None``.

    """
    if not isinstance(obj, Mapping):
        return None
    cls = _CATALOG.get(str(obj.get("@type", "")))
    if cls is None:
        return None
    try:
        return cls.from_json(obj)
    except (ValueError, TypeError):
        return None


def _detail_json(detail: ErrorDetail | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(detail, Mapping):
        return dict(detail)
    return detail.to_json()


def _validate_details(objs: Sequence[Mapping[str, Any]]) -> None:
    """Apply the catalog rules to a detail list.

    Args:
        objs: The detail objects, in order.

    Raises:
        ValueError: A type is missing, repeated, or claims the reserved
            prefix without being in the catalog.

    """
    seen: set[str] = set()
    for obj in objs:
        type_name = obj.get("@type")
        if not isinstance(type_name, str) or not type_name:
            raise ValueError("every error detail must name its type in '@type'")
        if type_name in seen:
            raise ValueError(f"error detail type {type_name!r} appears more than once")
        seen.add(type_name)
        if type_name.startswith(_RESERVED_PREFIX) and type_name not in _CATALOG:
            raise ValueError(
                f"{type_name!r} claims the reserved 'vgi_rpc.' prefix but is not in the catalog. "
                f"A protocol-defined detail type must live under its own protocol's name."
            )
        if "." not in type_name:
            raise ValueError(f"{type_name!r} is not qualified; protocol-defined types live under the protocol's name")


def encode_error_details(details: Iterable[ErrorDetail | Mapping[str, Any]]) -> str | None:
    """Serialize a detail list for ``vgi_rpc.error_details``, enforcing the rules.

    Returns ``None`` -- meaning *omit the key* -- for an empty list, for a list
    that breaks a catalog rule, and for one whose serialized form exceeds
    :data:`MAX_ERROR_DETAILS_BYTES`.  The array is dropped whole, never
    trimmed: a client cannot tell a truncated list from a complete one, so a
    partial list is worse than none.

    Args:
        details: Typed details, or already-built JSON objects.

    Returns:
        Compact JSON text, or ``None``.

    """
    objs = [_detail_json(d) for d in details]
    if not objs:
        return None
    try:
        _validate_details(objs)
        text = json.dumps(objs, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError):
        return None
    if len(text.encode("utf-8")) > MAX_ERROR_DETAILS_BYTES:
        return None
    return text


def decode_error_details(raw: str | bytes | None) -> list[dict[str, Any]]:
    """Decode a ``vgi_rpc.error_details`` value into its JSON objects.

    Tolerant by design: anything that is not a JSON array decodes as empty,
    and non-object elements are skipped.  Unknown ``@type`` values are kept --
    filtering to the catalog is what the typed accessors do.

    Args:
        raw: The metadata value, or ``None`` when absent.

    Returns:
        The detail objects, in wire order.

    """
    if raw is None:
        return []
    try:
        decoded = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return []
    if not isinstance(decoded, list):
        return []
    return [dict(item) for item in decoded if isinstance(item, dict)]


def is_retryable(code: Code | str, details: Iterable[Mapping[str, Any]] = ()) -> bool:
    """Whether the rule in WIRE_PROTOCOL.md §8 calls an error retryable.

    ``UNAVAILABLE`` is retryable; ``RESOURCE_EXHAUSTED`` is retryable only when
    it carries ``RetryInfo``.  Everything else is final -- ``ABORTED`` included,
    which means "retry the whole operation at a higher level", not this call.

    This is a classification, not a policy.  Automatic retry is opt-in, as in
    gRPC, because a method may not be idempotent.

    Args:
        code: The canonical code.
        details: The detail objects.

    Returns:
        Whether a retry of this call is warranted.

    """
    parsed = Code.parse(code)
    if parsed is Code.UNAVAILABLE:
        return True
    if parsed is Code.RESOURCE_EXHAUSTED:
        return any(isinstance(parse_error_detail(d), RetryInfo) for d in details)
    return False


# ---------------------------------------------------------------------------
# Reading the model off an exception
# ---------------------------------------------------------------------------


def error_code_of(exc: BaseException) -> Code:
    """Return the canonical code an exception declares, or ``UNKNOWN``.

    Read from an ``error_code`` attribute (class or instance).  An error with
    no classification is ``UNKNOWN``, which is honest: gRPC's lesson is not that
    ``UNKNOWN`` is wrong but that services reached for it for errors they *had*
    classified, so every ``error_kind`` this package defines names its code.

    Args:
        exc: The exception being reported.

    Returns:
        Its code.

    """
    return Code.parse(getattr(exc, "error_code", None))


def error_kind_of(exc: BaseException) -> str | None:
    """Return the ``error_kind`` an exception declares, or ``None``.

    Args:
        exc: The exception being reported.

    Returns:
        A non-empty kind, or ``None``.

    """
    kind = getattr(exc, "error_kind", None)
    return kind if isinstance(kind, str) and kind else None


def error_details_of(exc: BaseException) -> list[dict[str, Any]]:
    """Return the detail objects an exception declares, as JSON objects.

    Args:
        exc: The exception being reported.

    Returns:
        Its details, or an empty list.  Never raises: an exception whose
        ``error_details`` is broken still has to be reported.

    """
    try:
        raw = getattr(exc, "error_details", None)
        if raw is None or isinstance(raw, str | bytes | Mapping):
            return []
        return [_detail_json(d) for d in raw]
    except Exception:
        return []


class StatusError(Exception):
    """An application error carrying the full error model.

    Raise it from a method body to choose the code, the reason and the details
    a client sees::

        raise StatusError(
            "report is being rebuilt",
            code=Code.UNAVAILABLE,
            kind="report_rebuilding",
            details=[RetryInfo(retry_delay_seconds=30)],
        )

    Any exception class may instead declare ``error_code`` / ``error_kind`` /
    ``error_details`` attributes; this is the convenience for the case where a
    dedicated class would add nothing.

    The instance carries ``error_code``, ``error_kind`` and ``error_details``,
    the attributes :meth:`vgi_rpc.log.Message.from_exception` reads.  The
    details are validated eagerly, so a rule violation fails in the code that
    made it rather than being silently dropped on the way out.
    """

    def __init__(
        self,
        message: str,
        *,
        code: Code | str,
        kind: str | None = None,
        details: Iterable[ErrorDetail | Mapping[str, Any]] = (),
    ) -> None:
        """Build the error.

        Args:
            message: Developer-facing text.
            code: One of the sixteen canonical codes.
            kind: The reason a client branches on, unique within the raising protocol.
            details: Catalog details, at most one of each type.

        Raises:
            ValueError: If *code* is not a canonical code, or *details* breaks
                a catalog rule.

        """
        super().__init__(message)
        if isinstance(code, str) and not isinstance(code, Code) and code not in Code.__members__:
            raise ValueError(f"{code!r} is not a canonical error code")
        self.error_code: Code = Code(code)
        self.error_kind: str | None = kind or None
        objs = [_detail_json(d) for d in details]
        _validate_details(objs)
        self.error_details: list[dict[str, Any]] = objs


class AuthUnavailableError(Exception):
    """An authenticator could not answer. Not a rejection.

    "The credential is bad" and "I could not find out whether the credential
    is bad" are different answers, and collapsing them is expensive in both
    directions. A sidecar restart that surfaces as 401 makes every caller
    re-authenticate at once; the DuckDB extension treats a second 401 after a
    refresh as fatal, so a thirty-second blip becomes a fleet-wide re-login
    storm. And a caller that negative-caches a rejection will cache an outage.

    Deliberately **not** a :class:`ValueError`, which is the whole point:
    :func:`~vgi_rpc.http.chain_authenticate` advances to the next
    authenticator on ``ValueError``, so an outage raised as one is read as
    "this credential isn't mine, try the next" and ends up as a 401 from the
    end of the chain. Raised as this type it propagates, and
    ``_AuthMiddleware`` renders ``503`` with ``Retry-After`` instead.

    Raised from an identity hook (``resolve_token`` / ``mint_grant``) it is
    translated to ``identity_unavailable`` with the same retry hint
    (WIRE_PROTOCOL.md §16), which is why it lives here rather than in the HTTP
    package: the translation happens on every transport.

    Raise it for transport failures, timeouts, and 5xx from a remote
    authority. Do not raise it for a credential the authority answered about.

    Attributes:
        error_code: Always ``UNAVAILABLE``; the hint rides as ``RetryInfo``.

    """

    error_code: ClassVar[Code] = Code.UNAVAILABLE

    def __init__(self, detail: str = "", *, retry_after: int = 5) -> None:
        """Create a transient-failure signal.

        Args:
            detail: Operator-facing text. Must not contain the credential.
            retry_after: Seconds to advertise in ``Retry-After``. Keep it
                short: it is a hint to retry, not a backoff schedule.

        """
        super().__init__(detail or "authentication service unavailable")
        self.detail = detail
        self.retry_after = retry_after

    @property
    def error_details(self) -> list[RetryInfo]:
        """The retry hint, as the one detail this error carries."""
        return [RetryInfo(retry_delay_seconds=float(self.retry_after))]
