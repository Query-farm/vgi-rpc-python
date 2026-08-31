# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Transport-neutral peer identity evidence and authentication policies."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Final, Protocol
from urllib.parse import quote

from vgi_rpc.rpc._common import AuthContext


class PeerIdentityStatus(StrEnum):
    """Outcome of resolving evidence from one identity provider."""

    OFF = "off"
    NOT_APPLICABLE = "not_applicable"
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    PERMISSION_DENIED = "permission_denied"
    NO_MATCH = "no_match"
    INVALID = "invalid"
    UNTRUSTED_PROXY = "untrusted_proxy"


class IdentityAssurance(StrEnum):
    """How peer evidence was verified."""

    CRYPTOGRAPHIC_PEER = "cryptographic_peer"
    LOCAL_DAEMON = "local_daemon"
    CONFIGURED_PROXY = "configured_proxy"


class SubjectKind(StrEnum):
    """Kind of principal named by peer evidence."""

    USER = "user"
    TAGGED_NODE = "tagged_node"
    WORKLOAD = "workload"
    ENDPOINT = "endpoint"
    UNKNOWN = "unknown"


class SubjectStability(StrEnum):
    """Stability of a provider subject identifier."""

    STABLE = "stable"
    LOGIN = "login"
    NONE = "none"


class PeerIdentityUnavailableError(Exception):
    """A configured peer identity authority could not answer."""


class PeerIdentityRejectedError(PermissionError):
    """Peer evidence was present but invalid or outside the trust boundary."""


_MAX_JSON_BYTES: Final = 65_536
_MAX_JSON_DEPTH: Final = 16
_MAX_JSON_VALUES: Final = 4_096
_MAX_HEADER_BYTES: Final = 65_536
_MAX_HEADER_VALUES: Final = 256


def _require_unicode_scalar(value: str, field: str) -> None:
    try:
        value.encode()
    except UnicodeEncodeError as exc:
        raise ValueError(f"{field} contains an unpaired surrogate") from exc


def _deep_freeze(value: Any, *, _depth: int = 0, _count: list[int] | None = None) -> Any:
    """Recursively snapshot JSON-like evidence into immutable values."""
    if _depth > _MAX_JSON_DEPTH:
        raise ValueError("peer evidence exceeds maximum JSON depth")
    if _count is None:
        _count = [0]
    _count[0] += 1
    if _count[0] > _MAX_JSON_VALUES:
        raise ValueError("peer evidence exceeds maximum JSON value count")
    if isinstance(value, float) and not math.isfinite(value):
        raise TypeError("peer evidence floats must be finite")
    if isinstance(value, str):
        _require_unicode_scalar(value, "peer evidence string")
        return value
    if value is None or isinstance(value, int | float | bool):
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("peer evidence object keys must be strings")
        for key in value:
            _require_unicode_scalar(key, "peer evidence object key")
        return MappingProxyType(
            {key: _deep_freeze(item, _depth=_depth + 1, _count=_count) for key, item in value.items()}
        )
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return tuple(_deep_freeze(item, _depth=_depth + 1, _count=_count) for item in value)
    raise TypeError(f"peer evidence value is not JSON-compatible: {type(value).__name__}")


def _canonical_json(value: Any) -> str:
    """Serialize frozen JSON evidence deterministically within an SDK."""
    if isinstance(value, Mapping):
        value = {key: _canonical_json_value(item) for key, item in value.items()}
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _bounded_snapshot(value: Any) -> Any:
    frozen = _deep_freeze(value)
    if len(_canonical_json(frozen).encode()) > _MAX_JSON_BYTES:
        raise ValueError("peer evidence exceeds maximum JSON byte size")
    return frozen


def _canonical_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _canonical_json_value(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_canonical_json_value(item) for item in value]
    return value


@dataclass(frozen=True)
class PeerResolutionContext:
    """Provider-neutral snapshot of a transport peer and destination."""

    transport: str
    immediate_peer: str | None = None
    source_endpoint: str | None = None
    asserted_peer: str | None = None
    destination_address: str | None = None
    authority: str | None = None
    service_name: str | None = None
    headers: Mapping[str, tuple[str, ...]] = field(default_factory=dict, repr=False)
    metadata: Mapping[str, Any] = field(default_factory=dict, repr=False)
    deadline: float | None = None

    def __post_init__(self) -> None:
        """Normalize headers and deeply snapshot metadata."""
        if not isinstance(self.headers, Mapping):
            raise TypeError("peer-resolution headers must be a mapping")
        if not isinstance(self.metadata, Mapping):
            raise TypeError("peer-resolution metadata must be a mapping")
        for name in (
            "transport",
            "immediate_peer",
            "source_endpoint",
            "asserted_peer",
            "destination_address",
            "authority",
            "service_name",
        ):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, str):
                    raise TypeError(f"{name} must be a string or None")
                _require_unicode_scalar(value, name)
        if not self.transport:
            raise ValueError("transport is required")
        if self.deadline is not None and not math.isfinite(self.deadline):
            raise ValueError("deadline must be a finite monotonic timestamp")
        normalized: dict[str, tuple[str, ...]] = {}
        header_bytes = 0
        header_values = 0
        for name, values in self.headers.items():
            if not isinstance(name, str):
                raise TypeError("peer-resolution header names must be strings")
            if not name or any(character in name for character in "\r\n\x00"):
                raise ValueError("invalid peer-resolution header name")
            _require_unicode_scalar(name, "peer-resolution header name")
            raw_values: Any = values
            if isinstance(raw_values, str):
                raise TypeError("peer-resolution header values must be tuples")
            snapshot = tuple(raw_values)
            header_values += len(snapshot)
            if any(
                not isinstance(value, str) or any(character in value for character in "\r\n\x00") for value in snapshot
            ):
                raise ValueError(f"invalid peer-resolution header value: {name}")
            for value in snapshot:
                _require_unicode_scalar(value, f"peer-resolution header value: {name}")
                header_bytes += len(name.encode()) + len(value.encode())
            if header_values > _MAX_HEADER_VALUES or header_bytes > _MAX_HEADER_BYTES:
                raise ValueError("peer-resolution headers exceed configured safety limits")
            key = name.lower()
            if key in normalized:
                raise PeerIdentityRejectedError(f"case-varied duplicate identity header: {name}")
            normalized[key] = snapshot
        object.__setattr__(self, "headers", MappingProxyType(normalized))
        object.__setattr__(self, "metadata", _bounded_snapshot(self.metadata))

    def header(self, name: str) -> str | None:
        """Return one header value, rejecting ambiguous duplicates."""
        values = self.headers.get(name.lower(), ())
        if len(values) > 1:
            raise PeerIdentityRejectedError(f"duplicate identity header: {name}")
        return values[0] if values else None


@dataclass(frozen=True)
class PeerIdentity:
    """Verified or observed identity evidence for a transport peer."""

    provider: str
    evidence_source: str
    assurance: IdentityAssurance
    issuer: str
    transport: str
    subject_kind: SubjectKind = SubjectKind.UNKNOWN
    subject_key: str | None = None
    subject_stability: SubjectStability = SubjectStability.NONE
    subject_verified: bool = False
    attributes: Mapping[str, Any] = field(default_factory=dict, repr=False)
    capabilities: Mapping[str, Any] = field(default_factory=dict, repr=False)
    capabilities_verified: bool = False
    source_address: str | None = None
    proxy_address: str | None = None

    def __post_init__(self) -> None:
        """Validate invariants and snapshot caller-owned mappings."""
        if not all(
            isinstance(value, str) for value in (self.provider, self.evidence_source, self.issuer, self.transport)
        ):
            raise TypeError("provider, evidence_source, issuer, and transport must be strings")
        if self.subject_key is not None and not isinstance(self.subject_key, str):
            raise TypeError("subject_key must be a string or None")
        if not self.provider or not self.evidence_source or not self.issuer:
            raise ValueError("provider, evidence_source, and issuer are required")
        for name in ("provider", "evidence_source", "issuer", "transport", "subject_key"):
            value = getattr(self, name)
            if value is not None:
                _require_unicode_scalar(value, name)
        if not isinstance(self.assurance, IdentityAssurance):
            raise TypeError("assurance must be an IdentityAssurance")
        if not isinstance(self.subject_kind, SubjectKind):
            raise TypeError("subject_kind must be a SubjectKind")
        if not isinstance(self.subject_stability, SubjectStability):
            raise TypeError("subject_stability must be a SubjectStability")
        if type(self.subject_verified) is not bool or type(self.capabilities_verified) is not bool:
            raise TypeError("verification fields must be bool values")
        if self.subject_verified and not self.subject_key:
            raise ValueError("verified identity requires subject_key")
        if self.subject_key is None and self.subject_stability is not SubjectStability.NONE:
            raise ValueError("subjectless identity must use subject_stability='none'")
        object.__setattr__(self, "attributes", _bounded_snapshot(self.attributes))
        object.__setattr__(self, "capabilities", _bounded_snapshot(self.capabilities))

    @property
    def canonical_principal(self) -> str:
        """Return an unambiguous provider/issuer/subject principal key."""
        if not self.subject_key:
            raise ValueError("subjectless evidence has no canonical principal")
        parts = (self.provider, self.issuer, self.subject_key)
        return "peer/" + "/".join(quote(part, safe="") for part in parts)


@dataclass(frozen=True)
class PeerIdentityResult:
    """Resolution result from one peer identity provider."""

    provider: str
    status: PeerIdentityStatus
    identities: tuple[PeerIdentity, ...] = ()

    def __post_init__(self) -> None:
        """Validate status and identity correspondence."""
        if not isinstance(self.provider, str):
            raise TypeError("provider must be a string")
        if not isinstance(self.identities, tuple) or any(
            not isinstance(identity, PeerIdentity) for identity in self.identities
        ):
            raise TypeError("identities must be a tuple of PeerIdentity values")
        if not self.provider:
            raise ValueError("provider is required")
        _require_unicode_scalar(self.provider, "provider")
        if not isinstance(self.status, PeerIdentityStatus):
            raise TypeError("status must be a PeerIdentityStatus")
        if (self.status is PeerIdentityStatus.AVAILABLE) != bool(self.identities):
            raise ValueError("only an available result may carry identities")
        if any(identity.provider != self.provider for identity in self.identities):
            raise ValueError("result provider must match every identity provider")

    @classmethod
    def available(cls, identity: PeerIdentity) -> PeerIdentityResult:
        """Build an available result containing one identity."""
        return cls(identity.provider, PeerIdentityStatus.AVAILABLE, (identity,))


@dataclass(frozen=True)
class PeerEvidenceSet:
    """Immutable evidence snapshot for one request or connection."""

    identities: tuple[PeerIdentity, ...] = ()
    provider_status: Mapping[str, PeerIdentityStatus] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Snapshot statuses and validate available entries."""
        if not isinstance(self.identities, tuple) or any(
            not isinstance(identity, PeerIdentity) for identity in self.identities
        ):
            raise TypeError("identities must be a tuple of PeerIdentity values")
        if not isinstance(self.provider_status, Mapping):
            raise TypeError("provider_status must be a mapping")
        statuses = dict(self.provider_status)
        if any(
            not isinstance(provider, str) or not isinstance(status, PeerIdentityStatus)
            for provider, status in statuses.items()
        ):
            raise TypeError("provider_status requires string keys and PeerIdentityStatus values")
        if any(statuses.get(identity.provider) is not PeerIdentityStatus.AVAILABLE for identity in self.identities):
            raise ValueError("identity provider is not marked available")
        object.__setattr__(self, "provider_status", MappingProxyType(statuses))

    @classmethod
    def empty(cls) -> PeerEvidenceSet:
        """Return the shared empty evidence snapshot."""
        return _EMPTY_PEER_EVIDENCE

    @classmethod
    def from_results(cls, results: Sequence[PeerIdentityResult]) -> PeerEvidenceSet:
        """Combine provider results, rejecting duplicate providers."""
        identities: list[PeerIdentity] = []
        statuses: dict[str, PeerIdentityStatus] = {}
        for result in results:
            if result.provider in statuses:
                raise ValueError(f"duplicate peer identity provider: {result.provider}")
            statuses[result.provider] = result.status
            identities.extend(result.identities)
        return cls(tuple(identities), statuses) if results else cls.empty()

    def for_provider(self, provider: str) -> tuple[PeerIdentity, ...]:
        """Return all identities supplied by a provider."""
        return tuple(identity for identity in self.identities if identity.provider == provider)

    def unique_verified_subject(self, provider: str) -> PeerIdentity:
        """Return one verified, stable subject or raise ``PermissionError``."""
        matches = tuple(
            identity
            for identity in self.for_provider(provider)
            if identity.subject_verified
            and identity.subject_key
            and identity.subject_stability is SubjectStability.STABLE
        )
        if len(matches) != 1:
            raise PermissionError(f"provider {provider!r} did not produce one verified stable subject")
        return matches[0]

    def reject_ambiguous_provider(self, provider: str) -> None:
        """Reject multiple eligible subjects even when another factor succeeds."""
        matches = tuple(
            identity
            for identity in self.for_provider(provider)
            if identity.subject_verified
            and identity.subject_key
            and identity.subject_stability is SubjectStability.STABLE
        )
        if len(matches) > 1:
            raise PeerIdentityRejectedError(f"provider {provider!r} produced ambiguous verified subjects")

    def require_usable_provider(self, provider: str) -> PeerIdentity:
        """Require a usable provider outcome with status-aware failure."""
        status = self.provider_status.get(provider, PeerIdentityStatus.OFF)
        if status in (PeerIdentityStatus.UNAVAILABLE, PeerIdentityStatus.PERMISSION_DENIED):
            raise PeerIdentityUnavailableError(f"peer identity provider {provider!r} is unavailable")
        if status in (PeerIdentityStatus.INVALID, PeerIdentityStatus.UNTRUSTED_PROXY):
            raise PeerIdentityRejectedError(f"peer identity provider {provider!r} rejected evidence")
        return self.unique_verified_subject(provider)

    def require_available_provider(self, provider: str) -> tuple[PeerIdentity, ...]:
        """Require valid available evidence without requiring a stable subject."""
        status = self.provider_status.get(provider, PeerIdentityStatus.OFF)
        if status in (PeerIdentityStatus.UNAVAILABLE, PeerIdentityStatus.PERMISSION_DENIED):
            raise PeerIdentityUnavailableError(f"peer identity provider {provider!r} is unavailable")
        if status in (PeerIdentityStatus.INVALID, PeerIdentityStatus.UNTRUSTED_PROXY):
            raise PeerIdentityRejectedError(f"peer identity provider {provider!r} rejected evidence")
        if status is not PeerIdentityStatus.AVAILABLE:
            raise PermissionError(f"peer identity provider {provider!r} did not produce evidence")
        identities = self.for_provider(provider)
        if not identities:
            raise PermissionError(f"peer identity provider {provider!r} did not produce evidence")
        return identities

    def binding_digest(
        self,
        providers: Sequence[str],
        application_auth: AuthContext | None = None,
    ) -> str:
        """Hash every authorization-relevant field for state-token binding."""
        digest = hashlib.sha256()

        def add(value: str) -> None:
            encoded = value.encode()
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)

        for provider in sorted(set(providers)):
            status = self.provider_status.get(provider, PeerIdentityStatus.OFF)
            add(provider)
            add(status.value)
            identities = sorted(
                (
                    identity.provider,
                    identity.issuer,
                    identity.subject_key or "",
                    identity.assurance.value,
                    identity.evidence_source,
                    identity.transport,
                    identity.subject_kind.value,
                    identity.subject_stability.value,
                    "true" if identity.subject_verified else "false",
                    "true" if identity.capabilities_verified else "false",
                    # Topology is audit/routing evidence, not authorization
                    # evidence. Keep the two empty framing fields for digest
                    # compatibility with the original null-address vector,
                    # but never bind a session to an ephemeral source port or
                    # one member of a trusted proxy fleet.
                    "",
                    "",
                    _canonical_json(identity.attributes),
                    _canonical_json(identity.capabilities),
                )
                for identity in self.for_provider(provider)
            )
            for identity in identities:
                for identity_field in identity:
                    add(identity_field)
        if application_auth is not None:
            add("application_auth")
            add(application_auth.domain or "")
            add(application_auth.principal or "")
        return digest.hexdigest()


_EMPTY_PEER_EVIDENCE: Final[PeerEvidenceSet] = PeerEvidenceSet()


class PeerIdentityProvider(Protocol):
    """Transport adapter that resolves peer identity evidence."""

    @property
    def provider(self) -> str:
        """Stable provider name used in status maps and policy configuration."""
        ...

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        """Resolve evidence without exceeding ``context.deadline``.

        Networked providers must apply the remaining monotonic deadline to
        every connect/read operation and return ``UNAVAILABLE`` on timeout.
        """
        ...


PeerAuthenticationPolicy = Callable[[PeerEvidenceSet, AuthContext], AuthContext]
PeerIdentityLinker = Callable[[AuthContext, Mapping[str, PeerIdentity]], None]


def observe_peer_identity(evidence: PeerEvidenceSet, existing_auth: AuthContext) -> AuthContext:
    """Expose evidence without changing existing authentication."""
    del evidence
    return existing_auth


def require_peer_identity(provider: str) -> PeerAuthenticationPolicy:
    """Require verified evidence while preserving existing authentication."""

    def evaluate(evidence: PeerEvidenceSet, existing_auth: AuthContext) -> AuthContext:
        evidence.require_available_provider(provider)
        return _with_evidence_binding(existing_auth, evidence, (provider,))

    return evaluate


def peer_identity_primary(provider: str) -> PeerAuthenticationPolicy:
    """Authenticate from a provider's unique verified stable subject."""

    def evaluate(evidence: PeerEvidenceSet, existing_auth: AuthContext) -> AuthContext:
        del existing_auth
        identity = evidence.require_usable_provider(provider)
        assert identity.subject_key is not None
        return AuthContext(
            domain=provider,
            authenticated=True,
            principal=identity.canonical_principal,
            claims={
                "issuer": identity.issuer,
                "subject_kind": identity.subject_kind.value,
                "assurance": identity.assurance.value,
                "evidence_source": identity.evidence_source,
                "subject": identity.subject_key,
                "peer_evidence_binding": evidence.binding_digest((provider,)),
            },
        )

    return evaluate


def any_of_peer_identities(*providers: str) -> PeerAuthenticationPolicy:
    """Accept existing authentication or the first available peer provider."""
    if not providers:
        raise ValueError("at least one provider is required")

    def evaluate(evidence: PeerEvidenceSet, existing_auth: AuthContext) -> AuthContext:
        for provider in providers:
            status = evidence.provider_status.get(provider, PeerIdentityStatus.OFF)
            if status in (PeerIdentityStatus.INVALID, PeerIdentityStatus.UNTRUSTED_PROXY):
                raise PeerIdentityRejectedError(f"peer identity provider {provider!r} rejected evidence")
            if status is PeerIdentityStatus.AVAILABLE:
                evidence.reject_ambiguous_provider(provider)
        if existing_auth.authenticated:
            return existing_auth
        for provider in providers:
            if evidence.provider_status.get(provider) is PeerIdentityStatus.AVAILABLE:
                try:
                    evidence.unique_verified_subject(provider)
                except PermissionError:
                    continue
                return peer_identity_primary(provider)(evidence, existing_auth)
        if any(
            evidence.provider_status.get(provider)
            in (PeerIdentityStatus.UNAVAILABLE, PeerIdentityStatus.PERMISSION_DENIED)
            for provider in providers
        ):
            raise PeerIdentityUnavailableError("no usable authentication factor; a peer provider is unavailable")
        raise PermissionError("no configured provider produced a verified subject")

    return evaluate


def all_of_peer_identities(
    *providers: str,
    principal_provider: str | None = None,
    identity_linker: PeerIdentityLinker | None = None,
) -> PeerAuthenticationPolicy:
    """Require every factor and an application-defined identity link."""
    if not providers:
        raise ValueError("at least one provider is required")
    selected = principal_provider or providers[0]
    if selected not in providers:
        raise ValueError("principal_provider must be one of providers")
    if identity_linker is None:
        raise ValueError("all_of requires identity_linker to reject conflicting identities")

    def evaluate(evidence: PeerEvidenceSet, existing_auth: AuthContext) -> AuthContext:
        if not existing_auth.authenticated:
            raise PeerIdentityRejectedError("all_of requires existing application authentication")
        identities = MappingProxyType({provider: evidence.require_usable_provider(provider) for provider in providers})
        identity_linker(existing_auth, identities)
        primary = peer_identity_primary(selected)(evidence, existing_auth)
        claims = dict(primary.claims)
        claims["peer_evidence_binding"] = evidence.binding_digest(providers, existing_auth)
        claims["application_domain"] = existing_auth.domain
        claims["application_principal"] = existing_auth.principal
        return AuthContext(
            domain=primary.domain,
            authenticated=True,
            principal=primary.principal,
            claims=claims,
        )

    return evaluate


def _with_evidence_binding(
    auth: AuthContext,
    evidence: PeerEvidenceSet,
    providers: Sequence[str],
) -> AuthContext:
    """Clone an authentication result with peer-factor binding metadata."""
    claims = dict(auth.claims)
    claims["peer_evidence_binding"] = evidence.binding_digest(providers)
    return AuthContext(domain=auth.domain, authenticated=auth.authenticated, principal=auth.principal, claims=claims)
