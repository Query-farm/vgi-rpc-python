# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Tailscale Serve and LocalAPI peer-identity evidence providers."""

from __future__ import annotations

import base64
import ipaddress
import json
import math
import re
import socket
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from email.errors import HeaderParseError
from email.header import decode_header
from typing import Any, Final
from urllib.parse import urlencode, urlsplit

from vgi_rpc.rpc import (
    IdentityAssurance,
    PeerIdentity,
    PeerIdentityProvider,
    PeerIdentityResult,
    PeerIdentityStatus,
    PeerResolutionContext,
    SubjectKind,
    SubjectStability,
)

_PROVIDER: Final = "tailscale"
_LOCALAPI_HOST: Final = "local-tailscaled.sock"
_SERVE_LOGIN: Final = "Tailscale-User-Login"
_SERVE_NAME: Final = "Tailscale-User-Name"
_SERVE_PROFILE: Final = "Tailscale-User-Profile-Pic"
_SERVE_CAPABILITIES: Final = "Tailscale-App-Capabilities"
_FUNNEL_REQUEST: Final = "Tailscale-Funnel-Request"
_Q_WORDS = re.compile(r"=\?utf-8\?[qQ]\?[^?]*\?=(?:[ \t]+=\?utf-8\?[qQ]\?[^?]*\?=)*", re.IGNORECASE)
_SERVICE_NAME = re.compile(r"svc:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\Z")
_HTTP_TOKEN = re.compile(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")


class _DuplicateJSONKey(ValueError):
    pass


def _contains_ascii_control(value: str) -> bool:
    return any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJSONKey(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise ValueError(f"non-finite JSON constant: {value}")


def _load_json(raw: bytes | str) -> Any:
    return json.loads(raw, object_pairs_hook=_strict_object, parse_constant=_reject_constant)


def _decode_serve_value(value: str, *, max_bytes: int) -> str:
    if not value.isascii() or len(value.encode()) > max_bytes:
        raise ValueError("Tailscale Serve header is non-ASCII or oversized")
    if _contains_ascii_control(value):
        raise ValueError("Tailscale Serve header contains a control character")
    if not value.startswith("=?"):
        return value
    if _Q_WORDS.fullmatch(value) is None:
        raise ValueError("Tailscale Serve header is not strict RFC 2047 UTF-8 Q encoding")
    try:
        pieces = decode_header(value)
        decoded = "".join(
            piece.decode(charset or "ascii", errors="strict") if isinstance(piece, bytes) else piece
            for piece, charset in pieces
        )
    except (HeaderParseError, LookupError, UnicodeError) as exc:
        raise ValueError("invalid RFC 2047 Tailscale Serve header") from exc
    if len(decoded.encode()) > max_bytes or _contains_ascii_control(decoded):
        raise ValueError("decoded Tailscale Serve header is invalid or oversized")
    return decoded


def _capabilities(value: str, *, max_bytes: int) -> Mapping[str, Any]:
    decoded = _decode_serve_value(value, max_bytes=max_bytes)
    parsed = _load_json(decoded)
    if not isinstance(parsed, dict):
        raise ValueError("Tailscale app capabilities must be a JSON object")
    for name, entries in parsed.items():
        if not name or len(name.encode()) > 512 or "/" not in name or _contains_ascii_control(name):
            raise ValueError("invalid Tailscale application capability name")
        if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
            raise ValueError("Tailscale application capability values must be arrays of objects")
    return parsed


@dataclass(frozen=True)
class _TailscaleServeHeaderProvider:
    issuer: str
    trusted_proxy_addresses: frozenset[str]
    max_header_bytes: int
    provider: str = _PROVIDER

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        if context.immediate_peer not in self.trusted_proxy_addresses:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.UNTRUSTED_PROXY)
        try:
            funnel = context.header(_FUNNEL_REQUEST)
            login_raw = context.header(_SERVE_LOGIN)
            name_raw = context.header(_SERVE_NAME)
            profile_raw = context.header(_SERVE_PROFILE)
            capabilities_raw = context.header(_SERVE_CAPABILITIES)
        except PermissionError:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        if funnel is not None:
            return PeerIdentityResult(
                self.provider,
                PeerIdentityStatus.NOT_APPLICABLE if funnel == "?1" else PeerIdentityStatus.INVALID,
            )
        try:
            login = _decode_serve_value(login_raw, max_bytes=self.max_header_bytes) if login_raw is not None else None
            display_name = (
                _decode_serve_value(name_raw, max_bytes=self.max_header_bytes) if name_raw is not None else None
            )
            if profile_raw is not None:
                _decode_serve_value(profile_raw, max_bytes=self.max_header_bytes)
            capabilities = (
                _capabilities(capabilities_raw, max_bytes=self.max_header_bytes) if capabilities_raw is not None else {}
            )
        except (TypeError, ValueError, RecursionError):
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)

        if login_raw is not None and not login:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        if (name_raw is not None or profile_raw is not None) and not login:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        if not login and not capabilities:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.NO_MATCH)

        attributes: dict[str, Any] = {}
        if login:
            attributes["user_login"] = login
        if display_name:
            attributes["user_display_name"] = display_name
        identity = PeerIdentity(
            provider=self.provider,
            evidence_source="serve_proxy",
            assurance=IdentityAssurance.CONFIGURED_PROXY,
            issuer=self.issuer,
            transport="http",
            subject_kind=SubjectKind.USER if login else SubjectKind.UNKNOWN,
            subject_key=f"login:{login}" if login else None,
            subject_stability=SubjectStability.LOGIN if login else SubjectStability.NONE,
            subject_verified=bool(login),
            attributes=attributes,
            capabilities=capabilities,
            capabilities_verified=capabilities_raw is not None,
            source_address=context.asserted_peer,
            proxy_address=context.immediate_peer,
        )
        return PeerIdentityResult.available(identity)


def tailscale_serve_header_provider(
    *,
    issuer: str,
    trusted_proxy_addresses: Iterable[str],
    max_header_bytes: int = 16_384,
) -> PeerIdentityProvider:
    """Trust strict Tailscale Serve identity headers from exact proxy peers."""
    proxies = frozenset(trusted_proxy_addresses)
    if (
        not isinstance(issuer, str)
        or not issuer
        or not proxies
        or type(max_header_bytes) is not int
        or max_header_bytes <= 0
    ):
        raise ValueError("issuer, trusted_proxy_addresses, and a positive max_header_bytes are required")
    if any(
        not isinstance(address, str) or not address or any(character in address for character in "\r\n\x00")
        for address in proxies
    ):
        raise ValueError("trusted proxy addresses must be exact non-empty values")
    try:
        issuer.encode()
        for address in proxies:
            address.encode()
    except UnicodeEncodeError as exc:
        raise ValueError("issuer and trusted proxy addresses must contain Unicode scalar values") from exc
    return _TailscaleServeHeaderProvider(issuer, proxies, max_header_bytes)


class _HTTPProtocolError(ValueError):
    pass


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("LocalAPI deadline exceeded")
    return remaining


def _recv(sock: socket.socket, deadline: float, size: int = 8192) -> bytes:
    sock.settimeout(_remaining(deadline))
    return sock.recv(size)


def _read_http_response(
    sock: socket.socket,
    deadline: float,
    max_body_bytes: int,
) -> tuple[int, Mapping[str, tuple[str, ...]], bytes]:
    buffer = bytearray()
    while b"\r\n\r\n" not in buffer:
        if len(buffer) > 32_768:
            raise _HTTPProtocolError("LocalAPI response headers are oversized")
        chunk = _recv(sock, deadline)
        if not chunk:
            raise _HTTPProtocolError("truncated LocalAPI response headers")
        buffer.extend(chunk)
    raw_headers, initial_body = bytes(buffer).split(b"\r\n\r\n", 1)
    lines = raw_headers.split(b"\r\n")
    try:
        protocol, raw_status, _reason = lines[0].decode("ascii").split(" ", 2)
        status = int(raw_status)
    except (UnicodeError, ValueError) as exc:
        raise _HTTPProtocolError("invalid LocalAPI HTTP status line") from exc
    if protocol not in ("HTTP/1.0", "HTTP/1.1") or not 100 <= status <= 599:
        raise _HTTPProtocolError("invalid LocalAPI HTTP status")
    headers: dict[str, list[str]] = {}
    for line in lines[1:]:
        if not line or line[:1] in b" \t" or b":" not in line:
            raise _HTTPProtocolError("invalid LocalAPI response header")
        name, raw_value = line.split(b":", 1)
        if _HTTP_TOKEN.fullmatch(name) is None:
            raise _HTTPProtocolError("invalid LocalAPI response header name")
        try:
            key = name.decode("ascii").lower()
            value = raw_value.decode("ascii").strip()
        except UnicodeError as exc:
            raise _HTTPProtocolError("non-ASCII LocalAPI response header") from exc
        if not key or any(character in key + value for character in "\r\n\x00"):
            raise _HTTPProtocolError("invalid LocalAPI response header")
        headers.setdefault(key, []).append(value)

    body = bytearray(initial_body)
    lengths = headers.get("content-length", [])
    encodings = [value.lower() for value in headers.get("transfer-encoding", [])]
    if lengths and encodings:
        raise _HTTPProtocolError("ambiguous LocalAPI response framing")
    if lengths:
        if len(lengths) != 1:
            raise _HTTPProtocolError("duplicate LocalAPI Content-Length")
        try:
            length = int(lengths[0])
        except ValueError as exc:
            raise _HTTPProtocolError("invalid LocalAPI Content-Length") from exc
        if length < 0 or length > max_body_bytes:
            raise _HTTPProtocolError("LocalAPI response is oversized")
        while len(body) < length:
            chunk = _recv(sock, deadline, min(8192, length - len(body)))
            if not chunk:
                raise _HTTPProtocolError("truncated LocalAPI response body")
            body.extend(chunk)
        body = body[:length]
    elif encodings:
        if encodings != ["chunked"]:
            raise _HTTPProtocolError("unsupported LocalAPI transfer encoding")
        body = bytearray(_decode_chunked(sock, deadline, bytes(body), max_body_bytes))
    else:
        while True:
            if len(body) > max_body_bytes:
                raise _HTTPProtocolError("LocalAPI response is oversized")
            chunk = _recv(sock, deadline)
            if not chunk:
                break
            body.extend(chunk)
    if len(body) > max_body_bytes:
        raise _HTTPProtocolError("LocalAPI response is oversized")
    return status, {name: tuple(values) for name, values in headers.items()}, bytes(body)


def _decode_chunked(sock: socket.socket, deadline: float, initial: bytes, max_body_bytes: int) -> bytes:
    pending = bytearray(initial)
    decoded = bytearray()

    def line() -> bytes:
        while b"\r\n" not in pending:
            if len(pending) > 8192:
                raise _HTTPProtocolError("oversized LocalAPI chunk line")
            chunk = _recv(sock, deadline)
            if not chunk:
                raise _HTTPProtocolError("truncated LocalAPI chunked response")
            pending.extend(chunk)
        value, rest = bytes(pending).split(b"\r\n", 1)
        pending[:] = rest
        return value

    while True:
        raw_size = line().split(b";", 1)[0]
        try:
            size = int(raw_size, 16)
        except ValueError as exc:
            raise _HTTPProtocolError("invalid LocalAPI chunk size") from exc
        if size < 0 or len(decoded) + size > max_body_bytes:
            raise _HTTPProtocolError("LocalAPI response is oversized")
        if size == 0:
            while line():
                pass
            return bytes(decoded)
        while len(pending) < size + 2:
            chunk = _recv(sock, deadline)
            if not chunk:
                raise _HTTPProtocolError("truncated LocalAPI chunk")
            pending.extend(chunk)
        if pending[size : size + 2] != b"\r\n":
            raise _HTTPProtocolError("invalid LocalAPI chunk terminator")
        decoded.extend(pending[:size])
        del pending[: size + 2]


def _destination_ip(value: str) -> str:
    try:
        return str(ipaddress.ip_address(value))
    except ValueError as direct_error:
        parsed = urlsplit(f"//{value}")
        if parsed.hostname is None:
            raise ValueError("destination_address must contain an IP address") from direct_error
        return str(ipaddress.ip_address(parsed.hostname))


@dataclass(frozen=True)
class _TailscaleLocalAPIProvider:
    issuer: str
    unix_socket: str | None
    endpoint_host: str | None
    endpoint_port: int | None
    password: str | None
    timeout: float
    max_response_bytes: int
    provider: str = _PROVIDER

    def __call__(self, context: PeerResolutionContext) -> PeerIdentityResult:
        source = context.asserted_peer or context.source_endpoint or context.immediate_peer
        if not source:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.NOT_APPLICABLE)
        if any(character in source for character in "\r\n\x00") or len(source.encode()) > 4096:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        params = {"addr": source, "proto": "tcp"}
        target: dict[str, str] = {"kind": "node"}
        if context.service_name:
            if _SERVICE_NAME.fullmatch(context.service_name) is None:
                return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
            params["svc_name"] = context.service_name
            target = {"kind": "service", "value": context.service_name}
        elif context.destination_address:
            try:
                destination = _destination_ip(context.destination_address)
            except ValueError:
                return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
            params["dst_ip"] = destination
            target = {"kind": "destination_ip", "value": destination}

        deadline = min(context.deadline or float("inf"), time.monotonic() + self.timeout)
        try:
            status, headers, body = self._request(f"/localapi/v0/whois?{urlencode(params)}", deadline)
        except (OSError, TimeoutError):
            return PeerIdentityResult(self.provider, PeerIdentityStatus.UNAVAILABLE)
        except _HTTPProtocolError:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        if status in (401, 403):
            return PeerIdentityResult(self.provider, PeerIdentityStatus.PERMISSION_DENIED)
        if status == 404:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.NO_MATCH)
        if 500 <= status <= 599:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.UNAVAILABLE)
        if status != 200:
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        content_types = headers.get("content-type", ())
        if len(content_types) != 1 or content_types[0].split(";", 1)[0].strip().lower() != "application/json":
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        try:
            payload = _load_json(body)
            identity = self._identity(payload, context, target)
        except (KeyError, TypeError, ValueError, UnicodeError, RecursionError):
            return PeerIdentityResult(self.provider, PeerIdentityStatus.INVALID)
        return PeerIdentityResult.available(identity)

    def _request(self, path: str, deadline: float) -> tuple[int, Mapping[str, tuple[str, ...]], bytes]:
        if self.unix_socket is not None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            address: str | tuple[str, int] = self.unix_socket
        else:
            family = socket.AF_INET6 if ":" in (self.endpoint_host or "") else socket.AF_INET
            sock = socket.socket(family, socket.SOCK_STREAM)
            address = (self.endpoint_host or "", self.endpoint_port or 0)
        try:
            sock.settimeout(_remaining(deadline))
            sock.connect(address)
            headers = [f"GET {path} HTTP/1.1", f"Host: {_LOCALAPI_HOST}", "Connection: close"]
            if self.password is not None:
                credential = base64.b64encode(f":{self.password}".encode()).decode("ascii")
                headers.append(f"Authorization: Basic {credential}")
            request = ("\r\n".join(headers) + "\r\n\r\n").encode("ascii")
            sock.settimeout(_remaining(deadline))
            sock.sendall(request)
            return _read_http_response(sock, deadline, self.max_response_bytes)
        finally:
            sock.close()

    def _identity(self, payload: Any, context: PeerResolutionContext, target: Mapping[str, str]) -> PeerIdentity:
        if not isinstance(payload, dict) or not isinstance(payload.get("Node"), dict):
            raise ValueError("LocalAPI WhoIs response is missing Node")
        node = payload["Node"]
        profile = payload.get("UserProfile")
        capabilities = payload.get("CapMap", {})
        if capabilities is None:
            capabilities = {}
        if not isinstance(capabilities, dict):
            raise ValueError("LocalAPI CapMap must be an object")
        for name, entries in capabilities.items():
            if not isinstance(name, str) or not isinstance(entries, list):
                raise ValueError("LocalAPI CapMap values must be arrays")
        tags = node.get("Tags") or []
        if not isinstance(tags, list) or any(not isinstance(tag, str) or not tag.startswith("tag:") for tag in tags):
            raise ValueError("LocalAPI node tags are invalid")
        stable_node_id = node.get("StableID")
        node_name = node.get("Name")
        if stable_node_id is not None and not isinstance(stable_node_id, str):
            raise ValueError("LocalAPI StableID is invalid")
        if node_name is not None and not isinstance(node_name, str):
            raise ValueError("LocalAPI node name is invalid")
        attributes: dict[str, Any] = {
            "node_id": stable_node_id,
            "node_name": node_name,
            "tags": tags,
            "capability_target": dict(target),
        }
        if tags:
            if not stable_node_id:
                raise ValueError("tagged LocalAPI node lacks StableID")
            subject_kind = SubjectKind.TAGGED_NODE
            subject_key = f"node:{stable_node_id}"
        else:
            if not isinstance(profile, dict) or type(profile.get("ID")) is not int or profile["ID"] <= 0:
                raise ValueError("untagged LocalAPI node lacks a stable user ID")
            subject_kind = SubjectKind.USER
            subject_key = f"user:{profile['ID']}"
            attributes["user_id"] = str(profile["ID"])
            for source_name, target_name in (("LoginName", "user_login"), ("DisplayName", "user_display_name")):
                value = profile.get(source_name)
                if value is not None and not isinstance(value, str):
                    raise ValueError("LocalAPI user profile is invalid")
                if value:
                    attributes[target_name] = value
        return PeerIdentity(
            provider=self.provider,
            evidence_source="localapi",
            assurance=IdentityAssurance.LOCAL_DAEMON,
            issuer=self.issuer,
            transport=context.transport,
            subject_kind=subject_kind,
            subject_key=subject_key,
            subject_stability=SubjectStability.STABLE,
            subject_verified=True,
            attributes=attributes,
            capabilities=capabilities,
            capabilities_verified=True,
            source_address=context.asserted_peer or context.source_endpoint or context.immediate_peer,
        )


def tailscale_localapi_provider(
    *,
    issuer: str,
    unix_socket: str | None = None,
    endpoint: str | None = None,
    password: str | None = None,
    timeout: float = 5.0,
    max_response_bytes: int = 65_536,
) -> PeerIdentityProvider:
    """Create a no-cache LocalAPI WhoIs provider over Unix socket or HTTP."""
    if (
        not isinstance(issuer, str)
        or not issuer
        or not isinstance(timeout, int | float)
        or isinstance(timeout, bool)
        or not math.isfinite(timeout)
        or timeout <= 0
        or type(max_response_bytes) is not int
        or max_response_bytes <= 0
    ):
        raise ValueError("issuer, a positive timeout, and a positive max_response_bytes are required")
    try:
        issuer.encode()
    except UnicodeEncodeError as exc:
        raise ValueError("issuer must contain Unicode scalar values") from exc
    if unix_socket is not None and endpoint is not None:
        raise ValueError("configure exactly one of unix_socket or endpoint")
    endpoint_host: str | None = None
    endpoint_port: int | None = None
    if endpoint is not None:
        if not isinstance(endpoint, str):
            raise TypeError("endpoint must be a string")
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "http"
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.path not in ("", "/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("endpoint must be an HTTP origin without userinfo, path, query, or fragment")
        endpoint_host = parsed.hostname
        endpoint_port = parsed.port or 80
    else:
        unix_socket = unix_socket or "/var/run/tailscale/tailscaled.sock"
        if not isinstance(unix_socket, str) or not unix_socket or "\x00" in unix_socket:
            raise ValueError("unix_socket must be a valid path")
        if password is not None:
            raise ValueError("password authentication is only valid for an HTTP endpoint")
    if password is not None and (
        not isinstance(password, str) or any(character in password for character in "\r\n\x00")
    ):
        raise ValueError("LocalAPI password must be a string without control characters")
    return _TailscaleLocalAPIProvider(
        issuer,
        unix_socket,
        endpoint_host,
        endpoint_port,
        password,
        float(timeout),
        max_response_bytes,
    )
