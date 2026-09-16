# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Stockgate registration and liveness client for native VGI workers.

The client deliberately shares the worker's persistent Iroh secret key.  The
same key identifies the endpoint, signs its pkarr address packet, and proves
possession during registration.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import stat
import struct
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from http.client import HTTPMessage
from pathlib import Path
from typing import IO, Protocol, cast
from urllib.parse import quote, urlsplit

from vgi_rpc.iroh import _decode_secret_key, _load_iroh

type JsonValue = bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None
type JsonObject = dict[str, JsonValue]

_ZBASE32_ALPHABET = "ybndrfg8ejkmcpqxot1uwisza345h769"
_MAX_RESPONSE_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class StockgateHttpResponse:
    """Bounded response returned by a Stockgate HTTP transport."""

    status: int
    headers: Mapping[str, str]
    body: bytes


type StockgateHttpTransport = Callable[
    [str, str, Mapping[str, str], bytes | None, float],
    StockgateHttpResponse,
]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: HTTPMessage,
        newurl: str,
    ) -> urllib.request.Request | None:
        return None


def _urllib_transport(
    method: str,
    url: str,
    headers: Mapping[str, str],
    body: bytes | None,
    timeout: float,
) -> StockgateHttpResponse:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        response = opener.open(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read(_MAX_RESPONSE_BYTES + 1)
        if len(payload) > _MAX_RESPONSE_BYTES:
            raise StockgateError(502, "response_too_large", "Stockgate response exceeded 1 MiB.")
        status = response.getcode()
        if status is None:
            raise StockgateError(502, "invalid_response", "Stockgate returned no HTTP status.")
        return StockgateHttpResponse(status, dict(response.headers.items()), payload)


class _IrohSignature(Protocol):
    def to_bytes(self) -> bytes:
        """Return the 64-byte Ed25519 signature."""
        ...


class _IrohEndpointId(Protocol):
    def to_bytes(self) -> bytes:
        """Return the 32-byte public key."""
        ...


class _IrohSecretKey(Protocol):
    def public(self) -> _IrohEndpointId:
        """Return this key's public endpoint identity."""
        ...

    def sign(self, message: bytes) -> _IrohSignature:
        """Sign an arbitrary message."""
        ...


class _IrohSecretKeyType(Protocol):
    def from_bytes(self, value: bytes) -> _IrohSecretKey:
        """Construct a secret key from 32 raw bytes."""
        ...


class StockgateError(RuntimeError):
    """A structured Stockgate API or transport failure."""

    status: int
    code: str
    request_id: str | None

    def __init__(self, status: int, code: str, detail: str, request_id: str | None = None) -> None:
        """Initialize a failure without including any presented credential."""
        super().__init__(detail)
        self.status = status
        self.code = code
        self.request_id = request_id


@dataclass(frozen=True, slots=True)
class StockgateAddress:
    """Network addresses published for one Iroh endpoint."""

    relay_url: str | None = None
    direct_addresses: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Validate that the address is useful and safe to publish."""
        if not self.relay_url and not self.direct_addresses:
            raise ValueError("A Stockgate address requires a relay URL or direct address")
        if self.relay_url:
            parsed = urlsplit(self.relay_url)
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
                raise ValueError("Stockgate relay URL must be a public HTTPS URL")
        if any(not value or any(ord(character) <= 0x20 for character in value) for value in self.direct_addresses):
            raise ValueError("Stockgate direct addresses must be non-empty and contain no whitespace or controls")


@dataclass(frozen=True, slots=True)
class StockgateRegistrationIntent:
    """Complete service registration intent committed by the PoP signature."""

    org: str
    service: str
    label: str | None = None
    tags: tuple[str, ...] = ()
    ephemeral: bool = False


@dataclass(frozen=True, slots=True)
class StockgateIdentity:
    """Persistent Iroh identity in Stockgate and URI representations."""

    secret_key: bytes
    endpoint_id: str
    endpoint_hex: str


@dataclass(frozen=True, slots=True)
class StockgateDeviceCredential:
    """Expiring human CLI credential obtained through device authorization."""

    access_token: str
    expires_in: int


@dataclass(frozen=True, slots=True)
class StockgateService:
    """Existing or newly created Stockgate service selected for registration."""

    service_id: str
    name: str
    created: bool


@dataclass(frozen=True, slots=True)
class StockgateRegistrationGrant:
    """Short-lived single-use grant bound to one service and EndpointId."""

    credential: str
    service_id: str
    endpoint_id: str
    expires_at: int
    ephemeral: bool


@dataclass(frozen=True, slots=True)
class StockgateEndpoint:
    """Registered endpoint state held by the worker."""

    endpoint_id: str
    endpoint_hex: str
    service_id: str
    credential: str
    credential_expires_in: int
    signed_packet: bytes
    tags: tuple[str, ...]
    ephemeral: bool
    config_version: int = 0


@dataclass(frozen=True, slots=True)
class StockgateHeartbeat:
    """Worker configuration returned by a successful heartbeat."""

    endpoint: StockgateEndpoint
    next_heartbeat_s: int
    require_attest: bool | None
    accepts_from: tuple[str, ...] | None
    revoked_jti: JsonObject | None


@dataclass(frozen=True, slots=True)
class StockgateResolution:
    """Resolved service metadata returned by Stockgate."""

    endpoint_id: str
    endpoint_hex: str
    service_id: str
    name: str
    signed_packet: bytes
    address: StockgateAddress
    liveness: str
    require_attest: bool
    raw: JsonObject


def _zbase32(value: bytes) -> str:
    accumulator = 0
    bit_count = 0
    output: list[str] = []
    for byte in value:
        accumulator = (accumulator << 8) | byte
        bit_count += 8
        while bit_count >= 5:
            bit_count -= 5
            output.append(_ZBASE32_ALPHABET[(accumulator >> bit_count) & 31])
            accumulator &= (1 << bit_count) - 1
    if bit_count:
        output.append(_ZBASE32_ALPHABET[(accumulator << (5 - bit_count)) & 31])
    return "".join(output)


def load_or_create_iroh_secret_key(path: str | os.PathLike[str]) -> bytes:
    """Load a 32-byte worker identity, creating a mode-0600 hex file if absent.

    Args:
        path: Persistent key file. Parent directories are created mode 0700.

    Returns:
        The raw 32-byte Iroh secret key.

    Raises:
        ValueError: If an existing key is malformed or accessible by group/other.

    """
    key_path = Path(path)
    key_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        encoded = key_path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        key = secrets.token_bytes(32)
        try:
            descriptor = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            encoded = key_path.read_text(encoding="ascii").strip()
        else:
            with os.fdopen(descriptor, "w", encoding="ascii") as output:
                output.write(f"{key.hex()}\n")
                output.flush()
                os.fsync(output.fileno())
            return key
    if os.name != "nt" and stat.S_IMODE(key_path.stat().st_mode) & 0o077:
        raise ValueError("Iroh secret key file must not be accessible by group or other users")
    decoded_key = _decode_secret_key(encoded)
    if decoded_key is None:
        raise ValueError("Iroh secret key file is empty")
    return decoded_key


def load_or_create_stockgate_identity(path: str | os.PathLike[str]) -> StockgateIdentity:
    """Load a persistent secret and derive its public Stockgate/Iroh identifiers."""
    return stockgate_identity(load_or_create_iroh_secret_key(path))


def stockgate_identity(secret_key: bytes | str) -> StockgateIdentity:
    """Derive public Stockgate/Iroh identifiers from an in-memory secret key."""
    decoded_key = _decode_secret_key(secret_key)
    if decoded_key is None:
        raise ValueError("A 32-byte Iroh secret key is required")
    public = _load_iroh().SecretKey.from_bytes(decoded_key).public().to_bytes()
    return StockgateIdentity(decoded_key, _zbase32(public), public.hex())


def _dns_name(value: str) -> bytes:
    output = bytearray()
    for label in value.rstrip(".").split("."):
        encoded = label.encode("ascii")
        if not encoded or len(encoded) > 63:
            raise ValueError("Invalid DNS label in signed address packet")
        output.append(len(encoded))
        output.extend(encoded)
    output.append(0)
    return bytes(output)


def _txt_data(value: str) -> bytes:
    encoded = value.encode()
    if not encoded:
        return b"\x00"
    output = bytearray()
    for offset in range(0, len(encoded), 255):
        chunk = encoded[offset : offset + 255]
        output.append(len(chunk))
        output.extend(chunk)
    return bytes(output)


def _read_dns_name(packet: bytes, start: int) -> tuple[str, int]:
    labels: list[str] = []
    offset = start
    next_offset: int | None = None
    visited: set[int] = set()
    while True:
        if offset >= len(packet) or offset in visited:
            raise ValueError("Malformed DNS name in signed address packet")
        visited.add(offset)
        length = packet[offset]
        if length == 0:
            return ".".join(labels), next_offset if next_offset is not None else offset + 1
        if length & 0xC0 == 0xC0:
            if offset + 1 >= len(packet):
                raise ValueError("Truncated DNS pointer in signed address packet")
            if next_offset is None:
                next_offset = offset + 2
            offset = ((length & 0x3F) << 8) | packet[offset + 1]
            continue
        if length > 63 or offset + 1 + length > len(packet):
            raise ValueError("Malformed DNS label in signed address packet")
        labels.append(packet[offset + 1 : offset + 1 + length].decode("ascii"))
        offset += length + 1


def _decode_txt(data: bytes) -> str:
    output = bytearray()
    offset = 0
    while offset < len(data):
        length = data[offset]
        offset += 1
        if offset + length > len(data):
            raise ValueError("Malformed TXT record in signed address packet")
        output.extend(data[offset : offset + length])
        offset += length
    return output.decode()


def _address_from_signed_packet(endpoint_id: str, packet: bytes) -> StockgateAddress:
    if not 72 <= len(packet) <= 1072:
        raise ValueError("Iroh signed address packet must be between 72 and 1072 bytes")
    public_bytes = _decode_secret_key(endpoint_id)
    if public_bytes is None:
        raise ValueError("A Stockgate endpoint ID is required")
    timestamp = struct.unpack(">Q", packet[64:72])[0]
    dns = packet[72:]
    signable = b"3:seqi" + str(timestamp).encode("ascii") + b"e1:v" + str(len(dns)).encode("ascii") + b":" + dns
    api = _load_iroh()
    public = api.EndpointId.from_bytes(public_bytes)
    try:
        public.verify(signable, api.Signature.from_bytes(packet[:64]))
    except Exception as error:
        raise ValueError("Iroh signed address packet signature does not match the endpoint ID") from error
    if len(dns) < 12:
        raise ValueError("Truncated DNS signed address packet")
    questions, answers = struct.unpack(">HH", dns[4:8])
    offset = 12
    for _ in range(questions):
        _, offset = _read_dns_name(dns, offset)
        offset += 4
        if offset > len(dns):
            raise ValueError("Truncated DNS question in signed address packet")
    relay_url: str | None = None
    direct_addresses: list[str] = []
    expected_owner = f"_iroh.{endpoint_id}"
    for _ in range(answers):
        owner, offset = _read_dns_name(dns, offset)
        if offset + 10 > len(dns):
            raise ValueError("Truncated DNS answer in signed address packet")
        record_type, record_class, _ttl, data_length = struct.unpack(">HHIH", dns[offset : offset + 10])
        offset += 10
        data_end = offset + data_length
        if data_end > len(dns):
            raise ValueError("Truncated DNS record data in signed address packet")
        if owner == expected_owner and record_type == 16 and record_class == 1:
            value = _decode_txt(dns[offset:data_end])
            if value.startswith("relay="):
                if relay_url is not None:
                    raise ValueError("Signed address packet contains duplicate relay records")
                relay_url = value.removeprefix("relay=")
            elif value.startswith("addr="):
                direct_addresses.extend(value.removeprefix("addr=").split())
        offset = data_end
    if offset != len(dns):
        raise ValueError("Signed address packet contains trailing DNS data")
    return StockgateAddress(relay_url, tuple(direct_addresses))


def _signed_packet(
    secret_key: bytes, address: StockgateAddress, timestamp_us: int | None = None
) -> tuple[str, str, bytes]:
    api = _load_iroh()
    key_type = cast("_IrohSecretKeyType", api.SecretKey)
    key = key_type.from_bytes(secret_key)
    public = key.public().to_bytes()
    endpoint_id = _zbase32(public)
    values: list[str] = []
    if address.relay_url:
        values.append(f"relay={address.relay_url}")
    if address.direct_addresses:
        values.append(f"addr={' '.join(address.direct_addresses)}")
    owner = _dns_name(f"_iroh.{endpoint_id}")
    records = bytearray()
    for value in values:
        data = _txt_data(value)
        records.extend(owner)
        records.extend(struct.pack(">HHIH", 16, 1, 30, len(data)))
        records.extend(data)
    dns = struct.pack(">HHHHHH", 0, 0x8400, 0, len(values), 0, 0) + bytes(records)
    if len(dns) > 1000:
        raise ValueError("Iroh signed address packet DNS payload exceeds 1000 bytes")
    timestamp = timestamp_us if timestamp_us is not None else time.time_ns() // 1000
    signable = b"3:seqi" + str(timestamp).encode("ascii") + b"e1:v" + str(len(dns)).encode("ascii") + b":" + dns
    relay_payload = key.sign(signable).to_bytes() + struct.pack(">Q", timestamp) + dns
    return endpoint_id, public.hex(), relay_payload


def _pop_message(
    *,
    nonce: str,
    org_id: str,
    intent: StockgateRegistrationIntent,
    service_id: str,
    endpoint_id: str,
    signed_packet: bytes,
) -> bytes:
    tag_bytes = "\0".join(sorted(intent.tags)).encode()
    fields = (
        b"stockgate-pop-v1",
        nonce.encode(),
        org_id.encode(),
        b"service",
        service_id.encode(),
        endpoint_id.encode(),
        (intent.label or "").encode(),
        b"1" if intent.ephemeral else b"0",
    )
    return b"\0".join(fields) + b"\0" + hashlib.sha256(tag_bytes).digest() + hashlib.sha256(signed_packet).digest()


def _required_string(value: JsonObject, key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise StockgateError(502, "invalid_response", f"Stockgate response omitted {key}.")
    return item


def _required_int(value: JsonObject, key: str) -> int:
    item = value.get(key)
    if not isinstance(item, int) or isinstance(item, bool):
        raise StockgateError(502, "invalid_response", f"Stockgate response omitted {key}.")
    return item


def _base64url_decode(value: str) -> bytes:
    if not value or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_" for character in value
    ):
        raise ValueError("Invalid base64url encoding")
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _verify_resolver_signature(response: JsonObject, jwks: JsonObject) -> None:
    compact = _required_string(response, "sig")
    parts = compact.split(".")
    if len(parts) != 3 or parts[1]:
        raise StockgateError(502, "invalid_response", "Stockgate returned an invalid detached resolver signature.")
    try:
        header_value: object = json.loads(_base64url_decode(parts[0]))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise StockgateError(
            502, "invalid_response", "Stockgate returned an invalid resolver signature header."
        ) from error
    if not isinstance(header_value, dict) or set(header_value) != {"alg", "kid", "typ"}:
        raise StockgateError(502, "invalid_response", "Stockgate returned an unsupported resolver signature header.")
    header = cast("dict[str, object]", header_value)
    kid = header.get("kid")
    if header.get("alg") != "EdDSA" or header.get("typ") != "sg-resolve+jws" or not isinstance(kid, str):
        raise StockgateError(502, "invalid_response", "Stockgate returned an unsupported resolver signature.")
    keys = jwks.get("keys")
    if not isinstance(keys, list):
        raise StockgateError(502, "invalid_response", "Stockgate JWKS omitted its keys array.")
    candidates = [key for key in keys if isinstance(key, dict) and key.get("kid") == kid]
    if len(candidates) != 1:
        raise StockgateError(502, "unknown_resolver_key", "Stockgate resolver signing key is unknown.")
    key = candidates[0]
    public_value = key.get("x")
    if (
        key.get("kty") != "OKP"
        or key.get("crv") != "Ed25519"
        or key.get("alg") != "EdDSA"
        or key.get("use") != "sig"
        or "d" in key
        or not isinstance(public_value, str)
    ):
        raise StockgateError(502, "invalid_response", "Stockgate resolver signing key is invalid.")
    signed_payload = {
        name: response.get(name) for name in ("endpoint_id", "expires_at", "name", "org", "resolved_at", "service_id")
    }
    if any(value is None for value in signed_payload.values()):
        raise StockgateError(502, "invalid_response", "Stockgate resolver response omitted signed fields.")
    payload = json.dumps(signed_payload, separators=(",", ":"), sort_keys=True).encode()
    signing_input = f"{parts[0]}.{base64.urlsafe_b64encode(payload).decode().rstrip('=')}".encode("ascii")
    try:
        public = _load_iroh().EndpointId.from_bytes(_base64url_decode(public_value))
        public.verify(signing_input, _load_iroh().Signature.from_bytes(_base64url_decode(parts[2])))
    except Exception as error:
        raise StockgateError(502, "invalid_response", "Stockgate resolver signature verification failed.") from error


class StockgateClient:
    """Synchronous registration, heartbeat, and resolution client."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 15.0,
        transport: StockgateHttpTransport | None = None,
        allow_insecure_localhost: bool = False,
    ) -> None:
        """Create a client pinned to one Stockgate origin.

        Args:
            base_url: Stockgate issuer origin.
            timeout: Per-request deadline in seconds.
            transport: Optional test/application HTTP transport.
            allow_insecure_localhost: Permit HTTP only for loopback tests.

        Raises:
            ValueError: If the origin or timeout is unsafe.

        """
        parsed = urlsplit(base_url)
        local_http = (
            allow_insecure_localhost
            and parsed.scheme == "http"
            and parsed.hostname in {"127.0.0.1", "::1", "localhost"}
        )
        if (parsed.scheme != "https" and not local_http) or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Stockgate base URL must be an HTTPS origin")
        if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise ValueError("Stockgate base URL must not contain a path, query, or fragment")
        if timeout <= 0:
            raise ValueError("Stockgate timeout must be positive")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._transport = transport or _urllib_transport
        self._jwks: JsonObject | None = None
        self._jwks_expires_at = 0.0
        self._last_unknown_kid_refresh = 0.0

    def _request(self, method: str, path: str, credential: str | None, payload: JsonObject | None = None) -> JsonObject:
        headers = {"Accept": "application/json", "User-Agent": "vgi-rpc-stockgate"}
        body = None
        if credential:
            headers["Authorization"] = f"Bearer {credential}"
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        try:
            response = self._transport(method, f"{self._base_url}{path}", headers, body, self._timeout)
        except StockgateError:
            raise
        except Exception as error:
            raise StockgateError(0, "transport_error", f"Stockgate request failed: {type(error).__name__}") from error
        request_id = next((value for key, value in response.headers.items() if key.lower() == "x-request-id"), None)
        try:
            decoded: object = json.loads(response.body) if response.body else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StockgateError(
                response.status, "invalid_response", "Stockgate returned invalid JSON.", request_id
            ) from error
        if not isinstance(decoded, dict) or any(not isinstance(key, str) for key in decoded):
            raise StockgateError(
                response.status, "invalid_response", "Stockgate returned a non-object JSON response.", request_id
            )
        result = cast("JsonObject", decoded)
        if not 200 <= response.status < 300:
            code = result.get("title")
            detail = result.get("detail")
            raise StockgateError(
                response.status,
                code if isinstance(code, str) else "request_failed",
                detail if isinstance(detail, str) else f"Stockgate returned HTTP {response.status}.",
                request_id,
            )
        return result

    def authenticate_device(
        self,
        *,
        client_name: str = "Query Farm CLI",
        on_authorization: Callable[[str, str], None] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> StockgateDeviceCredential:
        """Authenticate a human through Stockgate's OAuth device flow."""
        issued = self._request("POST", "/device/code", None, {"client_name": client_name})
        device_code = _required_string(issued, "device_code")
        user_code = _required_string(issued, "user_code")
        verification_url = issued.get("verification_uri_complete") or issued.get("verification_uri")
        if not isinstance(verification_url, str) or not verification_url:
            raise StockgateError(502, "invalid_response", "Stockgate omitted the device verification URL.")
        expires_in = _required_int(issued, "expires_in")
        interval = _required_int(issued, "interval")
        if expires_in <= 0 or interval <= 0:
            raise StockgateError(502, "invalid_response", "Stockgate returned invalid device-flow timing.")
        if on_authorization is not None:
            on_authorization(verification_url, user_code)
        deadline = time.monotonic() + expires_in
        while time.monotonic() < deadline:
            sleep(interval)
            try:
                token = self._request(
                    "POST",
                    "/token",
                    None,
                    {
                        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                        "device_code": device_code,
                    },
                )
            except StockgateError as error:
                if error.code == "authorization_pending":
                    continue
                if error.code == "slow_down":
                    interval += 5
                    continue
                raise
            access_token = _required_string(token, "access_token")
            if not access_token.startswith("dc_"):
                raise StockgateError(502, "invalid_response", "Stockgate returned an invalid device credential.")
            credential_expires_in = _required_int(token, "expires_in")
            if credential_expires_in <= 0:
                raise StockgateError(502, "invalid_response", "Stockgate returned invalid credential timing.")
            return StockgateDeviceCredential(access_token, credential_expires_in)
        raise StockgateError(400, "expired_token", "Device authorization expired before it was approved.")

    def ensure_service(
        self,
        human_credential: str,
        org: str,
        service: str,
        *,
        display_name: str | None = None,
        visibility: str = "org",
        reach_mode: str = "direct",
        require_attest: bool = False,
    ) -> StockgateService:
        """Find a service by name or create it with conservative defaults."""
        handle = org.removeprefix("o/")
        path = f"/orgs/{quote(handle, safe='')}/services"
        catalog = self._request("GET", path, human_credential)
        services = catalog.get("services")
        if not isinstance(services, list):
            raise StockgateError(502, "invalid_response", "Stockgate service catalog omitted its services array.")
        for candidate in services:
            if isinstance(candidate, dict) and candidate.get("name") == service:
                service_id = candidate.get("service_id")
                if not isinstance(service_id, str) or not service_id:
                    raise StockgateError(502, "invalid_response", "Stockgate service omitted its ID.")
                return StockgateService(service_id, service, False)
        created = self._request(
            "POST",
            path,
            human_credential,
            {
                "name": service,
                "display_name": display_name,
                "visibility": visibility,
                "reach_mode": reach_mode,
                "require_attest": require_attest,
            },
        )
        if _required_string(created, "name") != service:
            raise StockgateError(502, "invalid_response", "Stockgate created a differently named service.")
        return StockgateService(_required_string(created, "service_id"), service, True)

    def create_registration_grant(
        self,
        human_credential: str,
        org: str,
        service_id: str,
        endpoint_id: str,
        *,
        ephemeral: bool = False,
        expires_in: int = 600,
    ) -> StockgateRegistrationGrant:
        """Issue one short-lived, single-use grant for an exact service endpoint."""
        if not 60 <= expires_in <= 3600:
            raise ValueError("Registration grant lifetime must be between 60 and 3600 seconds")
        handle = org.removeprefix("o/")
        response = self._request(
            "POST",
            f"/orgs/{quote(handle, safe='')}/registration-tokens",
            human_credential,
            {
                "service_id": service_id,
                "endpoint_id": endpoint_id,
                "ephemeral": ephemeral,
                "expires_in": expires_in,
            },
        )
        if _required_string(response, "service_id") != service_id:
            raise StockgateError(502, "invalid_response", "Stockgate bound the grant to a different service.")
        if _required_string(response, "endpoint_id") != endpoint_id:
            raise StockgateError(502, "invalid_response", "Stockgate bound the grant to a different endpoint.")
        credential = _required_string(response, "credential")
        if not credential.startswith("rt_"):
            raise StockgateError(502, "invalid_response", "Stockgate returned an invalid registration grant.")
        returned_ephemeral = response.get("ephemeral")
        if returned_ephemeral is not ephemeral:
            raise StockgateError(502, "invalid_response", "Stockgate returned different ephemeral state.")
        return StockgateRegistrationGrant(
            credential=credential,
            service_id=service_id,
            endpoint_id=endpoint_id,
            expires_at=_required_int(response, "expires_at"),
            ephemeral=ephemeral,
        )

    def register_service(
        self,
        human_credential: str,
        secret_key: bytes | str,
        address: StockgateAddress,
        intent: StockgateRegistrationIntent,
        *,
        grant_expires_in: int = 600,
    ) -> tuple[StockgateService, StockgateEndpoint]:
        """Create or find a service, issue its exact grant, and register immediately."""
        key_bytes = _decode_secret_key(secret_key)
        if key_bytes is None:
            raise ValueError("A persistent Iroh secret key is required")
        public = _load_iroh().SecretKey.from_bytes(key_bytes).public().to_bytes()
        endpoint_id = _zbase32(public)
        service = self.ensure_service(human_credential, intent.org, intent.service)
        grant = self.create_registration_grant(
            human_credential,
            intent.org,
            service.service_id,
            endpoint_id,
            ephemeral=intent.ephemeral,
            expires_in=grant_expires_in,
        )
        endpoint = self.register(grant.credential, key_bytes, address, intent)
        if endpoint.service_id != service.service_id:
            raise StockgateError(502, "invalid_response", "Registration returned a different service.")
        return service, endpoint

    def register(
        self,
        registration_credential: str,
        secret_key: bytes | str,
        address: StockgateAddress,
        intent: StockgateRegistrationIntent,
    ) -> StockgateEndpoint:
        """Register a service endpoint using a registration credential.

        Args:
            registration_credential: Short-lived ``rt_…`` bootstrap credential.
            secret_key: Persistent 32-byte, hex, or z-base-32 Iroh secret key.
            address: Relay and/or direct endpoint addresses to publish.
            intent: Complete service registration intent.

        Returns:
            Endpoint state containing the one-time ``ec_…`` credential.

        Raises:
            StockgateError: If challenge issuance or registration fails.
            ValueError: If key or address material is invalid.

        """
        key_bytes = _decode_secret_key(secret_key)
        if key_bytes is None:
            raise ValueError("A persistent Iroh secret key is required")
        endpoint_id, endpoint_hex, packet = _signed_packet(key_bytes, address)
        tags = tuple(sorted(set(intent.tags)))
        committed_intent = replace(intent, tags=tags)
        challenge = self._request(
            "POST",
            "/endpoints/challenge",
            registration_credential,
            {
                "endpoint_id": endpoint_id,
                "org": committed_intent.org,
                "kind": "service",
                "service": committed_intent.service,
                "label": committed_intent.label,
                "tags": list(tags),
                "ephemeral": committed_intent.ephemeral,
                "signed_packet_sha256": hashlib.sha256(packet).hexdigest(),
            },
        )
        challenge_id = _required_string(challenge, "challenge_id")
        nonce = _required_string(challenge, "nonce")
        org_id = _required_string(challenge, "org_id")
        service_id = _required_string(challenge, "service_id")
        if _required_string(challenge, "kind") != "service":
            raise StockgateError(502, "invalid_response", "Stockgate returned a different endpoint kind.")
        if _required_string(challenge, "endpoint_id") != endpoint_id:
            raise StockgateError(502, "invalid_response", "Stockgate returned a different endpoint ID.")
        normalized_label = challenge.get("label")
        if normalized_label != committed_intent.label:
            raise StockgateError(502, "invalid_response", "Stockgate returned a different endpoint label.")
        normalized_tags = challenge.get("tags")
        if not isinstance(normalized_tags, list) or any(not isinstance(tag, str) or not tag for tag in normalized_tags):
            raise StockgateError(502, "invalid_response", "Stockgate returned invalid normalized tags.")
        typed_tags = cast("list[str]", normalized_tags)
        if typed_tags != sorted(set(typed_tags)):
            raise StockgateError(502, "invalid_response", "Stockgate returned unsorted or duplicate normalized tags.")
        normalized_ephemeral = challenge.get("ephemeral")
        if not isinstance(normalized_ephemeral, bool):
            raise StockgateError(502, "invalid_response", "Stockgate returned invalid ephemeral state.")
        effective_intent = replace(
            committed_intent,
            tags=tuple(typed_tags),
            ephemeral=normalized_ephemeral,
        )
        api = _load_iroh()
        key_type = cast("_IrohSecretKeyType", api.SecretKey)
        signature = key_type.from_bytes(key_bytes).sign(
            _pop_message(
                nonce=nonce,
                org_id=org_id,
                intent=effective_intent,
                service_id=service_id,
                endpoint_id=endpoint_id,
                signed_packet=packet,
            )
        )
        registered = self._request(
            "POST",
            "/endpoints",
            registration_credential,
            {
                "challenge_id": challenge_id,
                "signature": base64.b64encode(signature.to_bytes()).decode("ascii"),
                "signed_packet": base64.b64encode(packet).decode("ascii"),
            },
        )
        if _required_string(registered, "endpoint_id") != endpoint_id:
            raise StockgateError(502, "invalid_response", "Stockgate returned a different endpoint ID.")
        registered_service = _required_string(registered, "service_id")
        if registered_service != service_id:
            raise StockgateError(502, "invalid_response", "Stockgate returned a different service ID.")
        credential = _required_string(registered, "credential")
        if not credential.startswith("ec_"):
            raise StockgateError(502, "invalid_response", "Stockgate returned an invalid endpoint credential.")
        return StockgateEndpoint(
            endpoint_id=endpoint_id,
            endpoint_hex=endpoint_hex,
            service_id=service_id,
            credential=credential,
            credential_expires_in=_required_int(registered, "credential_expires_in"),
            signed_packet=packet,
            tags=effective_intent.tags,
            ephemeral=effective_intent.ephemeral,
        )

    def heartbeat(
        self,
        endpoint: StockgateEndpoint,
        *,
        address: StockgateAddress | None = None,
        secret_key: bytes | str | None = None,
    ) -> StockgateHeartbeat:
        """Publish liveness, optionally refresh addresses, and apply credential rotation."""
        if (address is None) != (secret_key is None):
            raise ValueError("address and secret_key must be provided together")
        prepared = endpoint
        if address is not None and secret_key is not None:
            key_bytes = _decode_secret_key(secret_key)
            if key_bytes is None:
                raise ValueError("A persistent Iroh secret key is required")
            endpoint_id, endpoint_hex, packet = _signed_packet(key_bytes, address)
            if endpoint_id != endpoint.endpoint_id or endpoint_hex != endpoint.endpoint_hex:
                raise ValueError("Iroh secret key does not belong to this Stockgate endpoint")
            prepared = replace(endpoint, signed_packet=packet)
        payload: JsonObject = {"endpoint_id": endpoint.endpoint_id, "config_version": endpoint.config_version}
        if prepared is not endpoint:
            payload["signed_packet"] = base64.b64encode(prepared.signed_packet).decode("ascii")
        response = self._request("POST", "/heartbeat", endpoint.credential, payload)
        credential = response.get("credential")
        if credential is not None and (not isinstance(credential, str) or not credential.startswith("ec_")):
            raise StockgateError(502, "invalid_response", "Stockgate returned an invalid rotated credential.")
        config_version = response.get("config_version", endpoint.config_version)
        if not isinstance(config_version, int) or isinstance(config_version, bool):
            raise StockgateError(502, "invalid_response", "Stockgate returned an invalid config version.")
        updated = replace(prepared, credential=credential or endpoint.credential, config_version=config_version)
        accepts = response.get("accepts_from")
        if accepts is not None and (
            not isinstance(accepts, list) or any(not isinstance(item, str) for item in accepts)
        ):
            raise StockgateError(502, "invalid_response", "Stockgate returned invalid accepts_from policy.")
        revoked = response.get("revoked_jti")
        if revoked is not None and not isinstance(revoked, dict):
            raise StockgateError(502, "invalid_response", "Stockgate returned an invalid revocation feed.")
        require_attest = response.get("require_attest")
        if require_attest is not None and not isinstance(require_attest, bool):
            raise StockgateError(502, "invalid_response", "Stockgate returned invalid attestation policy.")
        return StockgateHeartbeat(
            endpoint=updated,
            next_heartbeat_s=_required_int(response, "next_heartbeat_s"),
            require_attest=require_attest,
            accepts_from=None if accepts is None else tuple(cast("list[str]", accepts)),
            revoked_jti=revoked,
        )

    def update_address(
        self,
        endpoint: StockgateEndpoint,
        secret_key: bytes | str,
        address: StockgateAddress,
    ) -> StockgateEndpoint:
        """Publish a newer signed address packet and return updated endpoint state."""
        key_bytes = _decode_secret_key(secret_key)
        if key_bytes is None:
            raise ValueError("A persistent Iroh secret key is required")
        endpoint_id, endpoint_hex, packet = _signed_packet(key_bytes, address)
        if endpoint_id != endpoint.endpoint_id or endpoint_hex != endpoint.endpoint_hex:
            raise ValueError("Iroh secret key does not belong to this Stockgate endpoint")
        self._request(
            "PUT",
            f"/endpoints/{quote(endpoint.endpoint_id, safe='')}/address-record",
            endpoint.credential,
            {"signed_packet": base64.b64encode(packet).decode("ascii")},
        )
        return replace(endpoint, signed_packet=packet)

    def resolve(self, org: str, service: str, credential: str | None = None) -> StockgateResolution:
        """Resolve one service name using an optional bearer credential."""
        response = self._request("GET", f"/resolve/o/{quote(org, safe='')}/{quote(service, safe='')}", credential)
        monotonic_now = time.monotonic()
        if self._jwks is None or monotonic_now >= self._jwks_expires_at:
            self._jwks = self._request("GET", "/.well-known/jwks.json", None)
            self._jwks_expires_at = monotonic_now + 300
        try:
            _verify_resolver_signature(response, self._jwks)
        except StockgateError as error:
            if error.code != "unknown_resolver_key":
                raise
            if monotonic_now - self._last_unknown_kid_refresh < 60:
                raise
            self._last_unknown_kid_refresh = monotonic_now
            self._jwks = self._request("GET", "/.well-known/jwks.json", None)
            self._jwks_expires_at = monotonic_now + 300
            _verify_resolver_signature(response, self._jwks)
        resolved_at = _required_int(response, "resolved_at")
        expires_at = _required_int(response, "expires_at")
        now = int(time.time())
        if resolved_at > now + 60 or expires_at <= now or expires_at - resolved_at > 120:
            raise StockgateError(
                502, "invalid_response", "Stockgate resolver response is expired or has invalid timing."
            )
        expected_org = f"o/{org}"
        resolved_org = _required_string(response, "org")
        resolved_name = _required_string(response, "name")
        name_prefix = f"stockgate://{expected_org}/"
        if resolved_org != expected_org or (
            resolved_name != f"{name_prefix}{service}"
            and not (service.startswith("@") and resolved_name.startswith(name_prefix))
        ):
            raise StockgateError(502, "invalid_response", "Stockgate resolver response names a different service.")
        try:
            packet = base64.b64decode(_required_string(response, "signed_packet"), validate=True)
        except ValueError as error:
            raise StockgateError(
                502, "invalid_response", "Stockgate returned invalid signed-packet encoding."
            ) from error
        endpoint_id = _required_string(response, "endpoint_id")
        try:
            address = _address_from_signed_packet(endpoint_id, packet)
            endpoint_bytes = _decode_secret_key(endpoint_id)
        except ValueError as error:
            raise StockgateError(502, "invalid_response", str(error)) from error
        if endpoint_bytes is None:
            raise StockgateError(502, "invalid_response", "Stockgate response omitted endpoint identity.")
        require_attest = response.get("require_attest")
        if not isinstance(require_attest, bool):
            raise StockgateError(502, "invalid_response", "Stockgate returned invalid attestation policy.")
        return StockgateResolution(
            endpoint_id=endpoint_id,
            endpoint_hex=endpoint_bytes.hex(),
            service_id=_required_string(response, "service_id"),
            name=resolved_name,
            signed_packet=packet,
            address=address,
            liveness=_required_string(response, "liveness"),
            require_attest=require_attest,
            raw=response,
        )

    def delete_endpoint(self, endpoint: StockgateEndpoint) -> None:
        """Revoke this endpoint and remove it from liveness immediately."""
        self._request("DELETE", f"/endpoints/{quote(endpoint.endpoint_id, safe='')}", endpoint.credential)


__all__ = [
    "StockgateAddress",
    "StockgateClient",
    "StockgateEndpoint",
    "StockgateDeviceCredential",
    "StockgateError",
    "StockgateHeartbeat",
    "StockgateHttpResponse",
    "StockgateHttpTransport",
    "StockgateIdentity",
    "StockgateRegistrationGrant",
    "StockgateRegistrationIntent",
    "StockgateResolution",
    "StockgateService",
    "load_or_create_iroh_secret_key",
    "load_or_create_stockgate_identity",
    "stockgate_identity",
]
