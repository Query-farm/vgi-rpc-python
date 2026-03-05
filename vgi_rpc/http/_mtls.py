# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Mutual TLS (mTLS) authentication factories for vgi-rpc HTTP transport.

Provides factories for extracting client identity from proxy-terminated mTLS
connections.  Two header conventions are supported:

- **PEM-in-header**: ``mtls_authenticate``, ``mtls_authenticate_fingerprint``,
  and ``mtls_authenticate_subject`` parse URL-encoded PEM certificates from
  headers like ``X-SSL-Client-Cert`` (nginx) or ``X-Amzn-Mtls-Clientcert``
  (AWS ALB).  These require ``pip install vgi-rpc[mtls]`` (cryptography).
- **XFCC**: ``mtls_authenticate_xfcc`` parses the Envoy
  ``x-forwarded-client-cert`` structured header.  No extra dependencies.

.. warning::

    **Header spoofing risk.** The reverse proxy **MUST** strip
    client-supplied ``X-SSL-Client-Cert`` / ``x-forwarded-client-cert``
    headers before forwarding.  Failure to do so allows clients to forge
    certificate identity.  These factories trust the header unconditionally.

No certificate chain validation is performed — that is the proxy's
responsibility.  These factories only extract identity from the forwarded
certificate information.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal
from urllib.parse import unquote

import falcon

from vgi_rpc.rpc import AuthContext

# ---------------------------------------------------------------------------
# XFCC types and parser (no cryptography needed)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class XfccElement:
    """A single element from an ``x-forwarded-client-cert`` header.

    Fields correspond to Envoy XFCC key names.  ``cert`` is URL-decoded
    PEM when present.
    """

    hash: str | None = None
    cert: str | None = None
    subject: str | None = None
    uri: str | None = None
    dns: tuple[str, ...] = ()
    by: str | None = None


def _parse_xfcc(header_value: str) -> list[XfccElement]:
    """Parse an ``x-forwarded-client-cert`` header value.

    Handles comma-separated elements (respecting quoted values),
    semicolon-separated key=value pairs within each element, and
    URL-encoded ``Cert``/``URI``/``By`` fields.

    Args:
        header_value: Raw header value string.

    Returns:
        List of parsed :class:`XfccElement` instances.

    """
    elements: list[XfccElement] = []
    for raw_element in _split_respecting_quotes(header_value, ","):
        raw_element = raw_element.strip()
        if not raw_element:
            continue
        pairs = _split_respecting_quotes(raw_element, ";")
        fields: dict[str, str | list[str]] = {}
        for pair in pairs:
            pair = pair.strip()
            if not pair:
                continue
            eq_idx = pair.find("=")
            if eq_idx < 0:
                continue
            key = pair[:eq_idx].strip().lower()
            value = pair[eq_idx + 1 :].strip()
            # Strip surrounding quotes
            if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
                value = _unescape_quoted(value[1:-1])
            if key in ("cert", "uri", "by"):
                value = unquote(value)
            if key == "dns":
                dns_list = fields.get("dns")
                if isinstance(dns_list, list):
                    dns_list.append(value)
                else:
                    fields["dns"] = [value]
            else:
                fields[key] = value
        dns_val = fields.get("dns")
        dns_tuple: tuple[str, ...] = ()
        if isinstance(dns_val, list):
            dns_tuple = tuple(dns_val)
        hash_val = fields.get("hash")
        cert_val = fields.get("cert")
        subject_val = fields.get("subject")
        uri_val = fields.get("uri")
        by_val = fields.get("by")
        elements.append(
            XfccElement(
                hash=hash_val if isinstance(hash_val, str) else None,
                cert=cert_val if isinstance(cert_val, str) else None,
                subject=subject_val if isinstance(subject_val, str) else None,
                uri=uri_val if isinstance(uri_val, str) else None,
                dns=dns_tuple,
                by=by_val if isinstance(by_val, str) else None,
            )
        )
    return elements


def _split_respecting_quotes(text: str, delimiter: str) -> list[str]:
    """Split *text* on *delimiter*, respecting double-quoted segments."""
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == '"':
            in_quotes = not in_quotes
            current.append(ch)
        elif ch == "\\" and in_quotes and i + 1 < len(text):
            current.append(ch)
            current.append(text[i + 1])
            i += 1
        elif ch == delimiter and not in_quotes:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
        i += 1
    parts.append("".join(current))
    return parts


def _unescape_quoted(text: str) -> str:
    """Remove backslash escapes inside a quoted string."""
    return re.sub(r"\\(.)", r"\1", text)


def mtls_authenticate_xfcc(
    *,
    validate: Callable[[XfccElement], AuthContext] | None = None,
    domain: str = "mtls",
    select_element: Literal["first", "last"] = "first",
) -> Callable[[falcon.Request], AuthContext]:
    """Create an authenticate callback from Envoy ``x-forwarded-client-cert``.

    Parses the ``x-forwarded-client-cert`` header and extracts client identity.
    Does **not** require the ``cryptography`` library.

    .. warning::

        The reverse proxy **MUST** strip client-supplied
        ``x-forwarded-client-cert`` headers before forwarding.  Failure to do
        so allows clients to forge certificate identity.

    Args:
        validate: Optional callback that receives the selected
            :class:`XfccElement` and returns an ``AuthContext``.  When
            ``None``, the Subject field's CN is used as ``principal``.
        domain: Domain string for the returned ``AuthContext``
            (default ``"mtls"``).
        select_element: Which element to use when multiple are present:
            ``"first"`` (original client, default) or ``"last"``
            (nearest proxy).

    Returns:
        A callback ``(falcon.Request) -> AuthContext`` suitable for
        ``make_wsgi_app(authenticate=...)``.

    Raises:
        ValueError: From the returned callback when the header is missing
            or validation fails.

    """

    def authenticate(req: falcon.Request) -> AuthContext:
        header_value = req.get_header("x-forwarded-client-cert")
        if not header_value:
            raise ValueError("Missing x-forwarded-client-cert header")
        elements = _parse_xfcc(header_value)
        if not elements:
            raise ValueError("Empty x-forwarded-client-cert header")
        element = elements[0] if select_element == "first" else elements[-1]
        if validate is not None:
            return validate(element)
        # Default: extract CN from Subject
        principal = _extract_cn(element.subject) if element.subject else ""
        claims: dict[str, object] = {}
        if element.hash:
            claims["hash"] = element.hash
        if element.subject:
            claims["subject"] = element.subject
        if element.uri:
            claims["uri"] = element.uri
        if element.dns:
            claims["dns"] = list(element.dns)
        if element.by:
            claims["by"] = element.by
        return AuthContext(
            domain=domain,
            authenticated=True,
            principal=principal,
            claims=claims,
        )

    return authenticate


def _extract_cn(subject: str) -> str:
    """Extract the CN value from an RFC 4514 or similar DN string."""
    for part in re.split(r"(?<!\\),", subject):
        part = part.strip()
        if part.upper().startswith("CN="):
            return part[3:]
    return ""


# ---------------------------------------------------------------------------
# PEM-based factories (require cryptography)
# ---------------------------------------------------------------------------

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes as _hashes

    _HASH_ALGORITHMS: dict[str, _hashes.HashAlgorithm] = {
        "sha256": _hashes.SHA256(),
        "sha1": _hashes.SHA1(),
        "sha384": _hashes.SHA384(),
        "sha512": _hashes.SHA512(),
    }

    def _parse_cert_from_header(req: falcon.Request, header: str) -> x509.Certificate:
        """Extract and parse a PEM certificate from a request header.

        Args:
            req: Falcon request.
            header: Header name to read.

        Returns:
            Parsed X.509 certificate.

        Raises:
            ValueError: If the header is missing, not valid PEM, or unparseable.

        """
        raw = req.get_header(header)
        if not raw:
            raise ValueError(f"Missing {header} header")
        pem_str = unquote(raw)
        if not pem_str.startswith("-----BEGIN CERTIFICATE-----"):
            raise ValueError("Header value is not a PEM certificate")
        try:
            return x509.load_pem_x509_certificate(pem_str.encode())
        except Exception as exc:
            raise ValueError(f"Failed to parse PEM certificate: {exc}") from exc

    def _check_cert_expiry(cert: x509.Certificate) -> None:
        """Validate certificate is within its validity period.

        Args:
            cert: Certificate to check.

        Raises:
            ValueError: If the certificate is expired or not yet valid.

        """
        import datetime

        now = datetime.datetime.now(datetime.UTC)
        if now < cert.not_valid_before_utc:
            raise ValueError("Certificate is not yet valid")
        if now > cert.not_valid_after_utc:
            raise ValueError("Certificate has expired")

    def mtls_authenticate(
        *,
        validate: Callable[[x509.Certificate], AuthContext],
        header: str = "X-SSL-Client-Cert",
        check_expiry: bool = False,
    ) -> Callable[[falcon.Request], AuthContext]:
        """Create an mTLS authenticate callback with custom certificate validation.

        Generic factory that parses the client certificate from a proxy header
        and delegates identity extraction to a user-supplied ``validate``
        callback.

        .. warning::

            The reverse proxy **MUST** strip client-supplied
            ``X-SSL-Client-Cert`` / ``x-forwarded-client-cert`` headers
            before forwarding.  Failure to do so allows clients to forge
            certificate identity.

        No certificate chain validation is performed — that is the proxy's
        responsibility.

        Args:
            validate: Callable that receives the parsed ``x509.Certificate``
                and returns an ``AuthContext``.  Must raise ``ValueError``
                when the certificate is not acceptable.
            header: HTTP header containing the URL-encoded PEM certificate
                (default ``"X-SSL-Client-Cert"``).
            check_expiry: When ``True``, verify the certificate is within its
                validity period before calling ``validate`` (default ``False``).

        Returns:
            A callback ``(falcon.Request) -> AuthContext`` suitable for
            ``make_wsgi_app(authenticate=...)``.

        """

        def authenticate(req: falcon.Request) -> AuthContext:
            cert = _parse_cert_from_header(req, header)
            if check_expiry:
                _check_cert_expiry(cert)
            return validate(cert)

        return authenticate

    def mtls_authenticate_fingerprint(
        *,
        fingerprints: Mapping[str, AuthContext],
        header: str = "X-SSL-Client-Cert",
        algorithm: str = "sha256",
        domain: str = "mtls",
        check_expiry: bool = False,
    ) -> Callable[[falcon.Request], AuthContext]:
        """Create an mTLS authenticate callback using certificate fingerprint lookup.

        Computes the certificate fingerprint and looks it up in the provided
        mapping.  Fingerprints must be lowercase hex without colons.

        .. warning::

            The reverse proxy **MUST** strip client-supplied
            ``X-SSL-Client-Cert`` headers before forwarding.  Failure to do
            so allows clients to forge certificate identity.

        Args:
            fingerprints: Mapping from lowercase hex fingerprint strings to
                ``AuthContext`` values.
            header: HTTP header containing the URL-encoded PEM certificate
                (default ``"X-SSL-Client-Cert"``).
            algorithm: Hash algorithm for fingerprinting — ``"sha256"``
                (default), ``"sha1"``, ``"sha384"``, or ``"sha512"``.
            domain: Domain string for the returned ``AuthContext``
                (default ``"mtls"``).
            check_expiry: When ``True``, verify the certificate is within its
                validity period (default ``False``).

        Returns:
            A callback ``(falcon.Request) -> AuthContext`` suitable for
            ``make_wsgi_app(authenticate=...)``.

        Raises:
            ValueError: At construction time if *algorithm* is unsupported.
                From the returned callback when the fingerprint is not found.

        """
        hash_algo = _HASH_ALGORITHMS.get(algorithm)
        if hash_algo is None:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")

        def validate(cert: x509.Certificate) -> AuthContext:
            fp = cert.fingerprint(hash_algo).hex()
            ctx = fingerprints.get(fp)
            if ctx is None:
                raise ValueError(f"Unknown certificate fingerprint: {fp}")
            return ctx

        return mtls_authenticate(validate=validate, header=header, check_expiry=check_expiry)

    def mtls_authenticate_subject(
        *,
        header: str = "X-SSL-Client-Cert",
        domain: str = "mtls",
        allowed_subjects: frozenset[str] | None = None,
        check_expiry: bool = False,
    ) -> Callable[[falcon.Request], AuthContext]:
        """Create an mTLS authenticate callback using certificate subject CN.

        Extracts the Subject Common Name as ``principal`` and populates
        ``claims`` with the full RFC 4514 DN, serial number (hex), and
        ``not_valid_after``.

        .. warning::

            The reverse proxy **MUST** strip client-supplied
            ``X-SSL-Client-Cert`` headers before forwarding.  Failure to do
            so allows clients to forge certificate identity.

        Args:
            header: HTTP header containing the URL-encoded PEM certificate
                (default ``"X-SSL-Client-Cert"``).
            domain: Domain string for the returned ``AuthContext``
                (default ``"mtls"``).
            allowed_subjects: When set, only accept certificates whose
                Subject CN is in this set.  ``None`` accepts any valid
                certificate — this is a deliberate security decision that
                should be made explicitly.
            check_expiry: When ``True``, verify the certificate is within its
                validity period (default ``False``).

        Returns:
            A callback ``(falcon.Request) -> AuthContext`` suitable for
            ``make_wsgi_app(authenticate=...)``.

        """

        def validate(cert: x509.Certificate) -> AuthContext:
            subject = cert.subject
            cn_attrs = subject.get_attributes_for_oid(x509.oid.NameOID.COMMON_NAME)
            cn = str(cn_attrs[0].value) if cn_attrs else ""
            if allowed_subjects is not None and cn not in allowed_subjects:
                raise ValueError(f"Subject CN {cn!r} not in allowed subjects")
            rfc4514 = subject.rfc4514_string()
            serial_hex = format(cert.serial_number, "x")
            not_valid_after = cert.not_valid_after_utc.isoformat()
            return AuthContext(
                domain=domain,
                authenticated=True,
                principal=cn,
                claims={
                    "subject_dn": rfc4514,
                    "serial": serial_hex,
                    "not_valid_after": not_valid_after,
                },
            )

        return mtls_authenticate(validate=validate, header=header, check_expiry=check_expiry)

except ImportError:
    pass
