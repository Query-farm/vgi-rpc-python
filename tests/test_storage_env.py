# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""Direct object storage from the environment (direct-storage contract v1).

Unit tests run everywhere.  ``TestS3Compat*`` run against a real S3-compatible
server (CI: RustFS, which validates SigV4 and issues STS AssumeRole session
tokens; MinIO works too) when
``VGI_TEST_S3_ENDPOINT`` (plain http) and ``VGI_TEST_S3_ACCESS_KEY`` /
``VGI_TEST_S3_SECRET_KEY`` (root credentials) are set; the TLS cases also
need ``VGI_TEST_S3_TLS_ENDPOINT`` and ``VGI_TEST_S3_CA_FILE``.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx2
import pytest

from vgi_rpc.storage_env import (
    ObjectStoreBlobClient,
    direct_storage_from_env,
    gcs_storage_from_env,
    s3_storage_from_env,
    storage_backend_from_env,
    storage_url_validator,
)

pytest.importorskip("boto3")

from vgi_rpc.s3 import S3Storage, StorageConfigError

_SHA = "0" * 64


# ---------------------------------------------------------------------------
# Backend selection (§1)
# ---------------------------------------------------------------------------


class TestBackendSelection:
    """``VGI_STORAGE_BACKEND`` and its inference."""

    @pytest.mark.parametrize(
        ("env", "expected"),
        [
            ({}, "none"),
            ({"VGI_BLOB_VENDOR_URL": "https://v.example"}, "vendor"),
            ({"VGI_STORAGE_S3_BUCKET": "b"}, "s3"),
            ({"VGI_STORAGE_GCS_BUCKET": "b"}, "gcs"),
            ({"VGI_STORAGE_BACKEND": "S3", "VGI_BLOB_VENDOR_URL": "x", "VGI_STORAGE_S3_BUCKET": "b"}, "s3"),
            ({"VGI_STORAGE_BACKEND": "none", "VGI_STORAGE_S3_BUCKET": "b"}, "none"),
        ],
    )
    def test_selection(self, env: dict[str, str], expected: str) -> None:
        """Explicit wins; otherwise vendor, then s3, then gcs, else none."""
        assert storage_backend_from_env(env) == expected

    def test_conflict_names_the_variables(self) -> None:
        """Two backends configured without an explicit choice refuse to start."""
        with pytest.raises(ValueError, match=r"VGI_BLOB_VENDOR_URL, VGI_STORAGE_S3_BUCKET.*VGI_STORAGE_BACKEND"):
            storage_backend_from_env({"VGI_BLOB_VENDOR_URL": "https://v", "VGI_STORAGE_S3_BUCKET": "b"})

    def test_unknown_backend(self) -> None:
        """An unknown value is an error."""
        with pytest.raises(ValueError, match="vendor, s3, gcs or none"):
            storage_backend_from_env({"VGI_STORAGE_BACKEND": "azure"})


# ---------------------------------------------------------------------------
# S3 settings (§2-§3)
# ---------------------------------------------------------------------------


class TestS3FromEnv:
    """``VGI_STORAGE_S3_*`` parsing."""

    def test_defaults_aws(self) -> None:
        """No endpoint: AWS, virtual-hosted, sha256, region chain default us-east-1, empty prefix."""
        s = s3_storage_from_env({"VGI_STORAGE_S3_BUCKET": "b"})
        assert (s.bucket, s.prefix, s.region_name, s.endpoint_url) == ("b", "", "us-east-1", None)
        assert s.addressing_style == "virtual"
        assert s.checksum == "sha256"
        assert s.presign_expiry_seconds == 3600
        assert s.request_checksum_calculation is None
        assert s.verify_bucket

    def test_region_chain(self) -> None:
        """VGI_STORAGE_S3_REGION, then AWS_REGION, then AWS_DEFAULT_REGION."""
        base = {"VGI_STORAGE_S3_BUCKET": "b", "AWS_DEFAULT_REGION": "eu-west-1"}
        assert s3_storage_from_env(base).region_name == "eu-west-1"
        assert s3_storage_from_env({**base, "AWS_REGION": "eu-central-1"}).region_name == "eu-central-1"
        assert s3_storage_from_env({**base, "AWS_REGION": "x", "VGI_STORAGE_S3_REGION": "auto"}).region_name == "auto"

    def test_custom_endpoint(self) -> None:
        """An endpoint defaults to path-style and turns botocore's automatic CRC32 off."""
        env = {
            "VGI_STORAGE_S3_BUCKET": "b",
            "VGI_STORAGE_S3_ENDPOINT": "http://minio:9000/",
            "VGI_STORAGE_S3_PUBLIC_ENDPOINT": "https://s3.example.com",
            "VGI_STORAGE_S3_PREFIX": "vgi/",
            "VGI_STORAGE_URL_TTL_SECONDS": "900",
        }
        s = s3_storage_from_env(env)
        assert s.endpoint_url == "http://minio:9000"
        assert s.public_endpoint_url == "https://s3.example.com"
        assert s.addressing_style == "path"
        assert s.request_checksum_calculation == "when_required"
        assert (s.prefix, s.presign_expiry_seconds) == ("vgi/", 900)
        assert s3_storage_from_env({**env, "VGI_STORAGE_S3_FORCE_PATH_STYLE": "false"}).addressing_style == "virtual"
        chosen = s3_storage_from_env({**env, "AWS_REQUEST_CHECKSUM_CALCULATION": "when_supported"})
        assert chosen.request_checksum_calculation is None

    def test_explicit_keys(self) -> None:
        """Explicit keys and session token are used and never repr'd."""
        s = s3_storage_from_env(
            {
                "VGI_STORAGE_S3_BUCKET": "b",
                "VGI_STORAGE_S3_ACCESS_KEY_ID": "AKIDEXAMPLE",
                "VGI_STORAGE_S3_SECRET_ACCESS_KEY": "s3cr3t-value",
                "VGI_STORAGE_S3_SESSION_TOKEN": "t0ken-value",
            }
        )
        assert (s.access_key_id, s.secret_access_key, s.session_token) == ("AKIDEXAMPLE", "s3cr3t-value", "t0ken-value")
        text = repr(s)
        assert "s3cr3t" not in text and "t0ken" not in text and "AKIDEXAMPLE" not in text
        creds = s._ensure_clients().session.get_credentials().get_frozen_credentials()
        assert (creds.access_key, creds.token) == ("AKIDEXAMPLE", "t0ken-value")

    @pytest.mark.parametrize(
        ("extra", "message"),
        [
            ({"VGI_STORAGE_S3_ACCESS_KEY_ID": "a"}, "set together"),
            ({"VGI_STORAGE_S3_SESSION_TOKEN": "t"}, "needs VGI_STORAGE_S3_ACCESS_KEY_ID"),
            ({"VGI_STORAGE_S3_CHECKSUM": "md5"}, "sha256 or none"),
            ({"VGI_STORAGE_S3_FORCE_PATH_STYLE": "maybe"}, "true or false"),
            ({"VGI_STORAGE_S3_ENDPOINT": "minio:9000"}, "absolute http"),
            ({"VGI_STORAGE_URL_TTL_SECONDS": "0"}, "between 1 and"),
            ({"VGI_STORAGE_URL_TTL_SECONDS": "999999999"}, "between 1 and"),
        ],
    )
    def test_invalid(self, extra: dict[str, str], message: str) -> None:
        """Malformed settings fail at startup with the variable's name."""
        with pytest.raises(ValueError, match=message):
            s3_storage_from_env({"VGI_STORAGE_S3_BUCKET": "b", **extra})

    def test_bucket_required(self) -> None:
        """``s3`` without a bucket is an error."""
        with pytest.raises(ValueError, match="VGI_STORAGE_S3_BUCKET"):
            s3_storage_from_env({})


class TestDirectStorage:
    """``direct_storage_from_env``: the server wiring."""

    def test_vendor_and_none_return_none(self) -> None:
        """Only s3 / gcs produce a direct configuration."""
        assert direct_storage_from_env({}) is None
        assert direct_storage_from_env({"VGI_STORAGE_BACKEND": "vendor"}) is None

    def test_plain_http_needs_opt_in(self) -> None:
        """http:// pre-signed URLs are refused unless VGI_STORAGE_ALLOW_PLAIN_HTTP=1."""
        env = {"VGI_STORAGE_S3_BUCKET": "b", "VGI_STORAGE_S3_ENDPOINT": "http://127.0.0.1:9000"}
        with pytest.raises(ValueError, match="VGI_STORAGE_ALLOW_PLAIN_HTTP=1"):
            direct_storage_from_env(env)
        cfg = direct_storage_from_env({**env, "VGI_STORAGE_ALLOW_PLAIN_HTTP": "1"})
        assert cfg is not None and cfg.backend == "s3"

    def test_internal_http_public_https_is_fine(self) -> None:
        """Only the URLs handed to clients matter: an http internal endpoint behind an https public one is allowed."""
        cfg = direct_storage_from_env(
            {
                "VGI_STORAGE_S3_BUCKET": "b",
                "VGI_STORAGE_S3_ENDPOINT": "http://minio:9000",
                "VGI_STORAGE_S3_PUBLIC_ENDPOINT": "https://s3.example.com",
            }
        )
        assert cfg is not None
        assert cfg.upload_url_provider is cfg.storage
        assert "s3.example.com" in cfg.describe()

    def test_checksum_none_disables_grants(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """checksum=none: results still externalize, but no upload-URL provider (no grants advertised)."""
        cfg = direct_storage_from_env({"VGI_STORAGE_S3_BUCKET": "b", "VGI_STORAGE_S3_CHECKSUM": "none"})
        assert cfg is not None
        assert cfg.upload_url_provider is None
        assert cfg.external_location.storage is cfg.storage
        assert "checksum=none" in cfg.describe()
        storage = cfg.storage
        assert isinstance(storage, S3Storage)
        with pytest.raises(StorageConfigError, match="checksum is 'none'"):
            storage.generate_upload_url(10, _SHA)

    def test_url_validator_pins_presign_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Inbound pointer URLs must be on the host the storage signs for, over https unless opted in."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "a")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "b")
        storage = S3Storage(
            bucket="b", endpoint_url="http://minio:9000", public_endpoint_url="https://s3.example.com", region_name="x"
        )
        validate = storage_url_validator(storage)
        validate("https://s3.example.com/b/k?X-Amz-Signature=1")
        with pytest.raises(ValueError, match="not this worker's storage host"):
            validate("https://evil.example.com/b/k")
        with pytest.raises(ValueError, match="VGI_STORAGE_ALLOW_PLAIN_HTTP"):
            validate("http://s3.example.com/b/k")
        storage_url_validator(storage, allow_plain_http=True)("http://s3.example.com/b/k")

    def test_public_endpoint_used_for_presigned_urls_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Grants and result URLs carry the public host; requests go to the internal endpoint."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "a")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "b")
        storage = S3Storage(
            bucket="b",
            prefix="p/",
            endpoint_url="http://minio:9000",
            public_endpoint_url="https://s3.example.com",
            region_name="us-east-1",
            addressing_style="path",
        )
        grant = storage.generate_upload_url(5, _SHA)
        assert grant.upload_url.startswith("https://s3.example.com/b/p/")
        assert grant.download_url.startswith("https://s3.example.com/b/p/")
        assert storage._get_client().meta.endpoint_url == "http://minio:9000"

    def test_gcs_from_env(self) -> None:
        """GCS settings: bucket, prefix, endpoint, credentials file, TTL."""
        pytest.importorskip("google.cloud.storage")
        g = gcs_storage_from_env(
            {
                "VGI_STORAGE_GCS_BUCKET": "gb",
                "VGI_STORAGE_GCS_PREFIX": "x/",
                "VGI_STORAGE_GCS_ENDPOINT": "http://127.0.0.1:4443",
                "VGI_STORAGE_GCS_CREDENTIALS_FILE": "/etc/sa.json",
                "VGI_STORAGE_URL_TTL_SECONDS": "600",
            }
        )
        assert (g.bucket, g.prefix, g.endpoint_url, g.credentials_file, g.presign_expiry_seconds) == (
            "gb",
            "x/",
            "http://127.0.0.1:4443",
            "/etc/sa.json",
            600,
        )
        assert g.presign_hosts() == frozenset({"127.0.0.1:4443"})
        with pytest.raises(ValueError, match="VGI_STORAGE_GCS_BUCKET"):
            gcs_storage_from_env({"VGI_STORAGE_BACKEND": "gcs"})


# ---------------------------------------------------------------------------
# URL lifetime under temporary credentials (§4)
# ---------------------------------------------------------------------------


def _storage_with_credentials(lifetimes: list[int], refreshes: list[int]) -> S3Storage:
    """Build an S3Storage signing with refreshable credentials that live ``lifetimes[i]`` seconds after refresh i."""
    import boto3
    import botocore.session
    from botocore.credentials import RefreshableCredentials

    def metadata(seconds: int) -> dict[str, str]:
        return {
            "access_key": "AKIDTEMP",
            "secret_key": "secret",
            "token": f"token-{len(refreshes)}",
            "expiry_time": (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat(),
        }

    def refresh() -> dict[str, str]:
        refreshes.append(1)
        return metadata(lifetimes[min(len(refreshes), len(lifetimes) - 1)])

    creds = RefreshableCredentials.create_from_metadata(metadata(lifetimes[0]), refresh, "test-temporary")
    core = botocore.session.Session()
    core._credentials = creds
    storage = S3Storage(bucket="b", region_name="us-east-1", endpoint_url="https://s3.example.com")
    pool = storage._pool
    pool.session = boto3.session.Session(botocore_session=core)
    pool.client = pool.session.client("s3", endpoint_url="https://s3.example.com", config=storage._config())
    pool.presign_client = pool.client
    return storage


def _expires(url: str) -> int:
    return int(parse_qs(urlsplit(url).query)["X-Amz-Expires"][0])


class TestUrlLifetime:
    """A URL never outlives the credentials that signed it."""

    def test_static_credentials_use_the_ttl(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Static keys have no expiry: X-Amz-Expires is the TTL."""
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "a")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "b")
        storage = S3Storage(bucket="b", region_name="us-east-1", presign_expiry_seconds=1234)
        assert _expires(storage.generate_upload_url(1, _SHA).upload_url) == 1234

    def test_capped_by_credential_expiry(self) -> None:
        """Credentials with 30 min left: URLs live 30 min - 60 s, not the 1 h TTL; no refresh needed."""
        refreshes: list[int] = []
        storage = _storage_with_credentials([1800], refreshes)
        grant = storage.generate_upload_url(1, _SHA)
        assert 1800 - 60 - 5 <= _expires(grant.upload_url) <= 1800 - 60
        assert _expires(grant.download_url) == _expires(grant.upload_url)
        assert grant.expires_at <= datetime.now(UTC) + timedelta(seconds=1740)
        assert refreshes == []

    def test_short_credentials_refreshed_before_a_grant(self) -> None:
        """Under 15 min (+60 s) left: refreshed before signing an upload grant."""
        refreshes: list[int] = []
        # 15.5 min: outside botocore's own 15-min advisory refresh window, but
        # short of the 15 min + 60 s margin a grant needs.
        storage = _storage_with_credentials([930, 3000], refreshes)
        grant = storage.generate_upload_url(1, _SHA)
        assert refreshes, "credentials should have been refreshed"
        assert _expires(grant.upload_url) > 15 * 60
        assert "token-1" in grant.upload_url

    def test_unrefreshable_short_credentials_sign_for_what_is_left(self, caplog: pytest.LogCaptureFixture) -> None:
        """A credential source that keeps returning short credentials: sign for the remaining lifetime, warn."""
        refreshes: list[int] = []
        storage = _storage_with_credentials([300, 300], refreshes)
        grant = storage.generate_upload_url(1, _SHA)
        assert _expires(grant.upload_url) <= 300 - 60
        assert any("expire in" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Function-storage adapter
# ---------------------------------------------------------------------------


class _MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, data: bytes) -> str:
        key = uuid.uuid4().hex
        self.objects[key] = data
        return key

    def get(self, object_id: str, *, max_bytes: int | None = None) -> bytes:
        return self.objects[object_id]


def test_object_store_blob_client() -> None:
    """put_bytes/get_bytes round-trip and verify; deletes are no-ops."""
    from vgi_rpc.vendor import VendorError

    store = _MemoryStore()
    with ObjectStoreBlobClient(store) as blobs:
        ref = blobs.put_bytes(b"hello", scope=("s", "e"), idempotency_key="k")
        assert ref.size == 5 and ref.sha256 == hashlib.sha256(b"hello").hexdigest()
        assert blobs.get_bytes(ref) == b"hello"
        store.objects[ref.object_id] = b"HELLO"
        with pytest.raises(VendorError, match="SHA-256"):
            blobs.get_bytes(ref)
        assert blobs.delete([ref]) == 0 and blobs.delete_scope("s", "e") == 0
        assert ref.object_id in store.objects


# ---------------------------------------------------------------------------
# S3-compatible server (opt-in)
# ---------------------------------------------------------------------------

_S3 = os.environ.get("VGI_TEST_S3_ENDPOINT", "")
_S3_TLS = os.environ.get("VGI_TEST_S3_TLS_ENDPOINT", "")
_needs_s3 = pytest.mark.skipif(not _S3, reason="set VGI_TEST_S3_ENDPOINT (+ _ACCESS_KEY, _SECRET_KEY)")


def _root() -> tuple[str, str]:
    return os.environ["VGI_TEST_S3_ACCESS_KEY"], os.environ["VGI_TEST_S3_SECRET_KEY"]


def _make_bucket(endpoint: str, verify: Any = None) -> str:
    import boto3

    key, secret = _root()
    bucket = f"vgi-env-{uuid.uuid4().hex[:12]}"
    boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        verify=verify,
    ).create_bucket(Bucket=bucket)
    return bucket


def _sts_credentials(endpoint: str, seconds: int = 900) -> dict[str, Any]:
    """Temporary credentials (with a session token) from the server STS AssumeRole."""
    import boto3

    key, secret = _root()
    sts = boto3.client(
        "sts", endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id=key, aws_secret_access_key=secret
    )
    creds: dict[str, Any] = sts.assume_role(
        RoleArn="arn:aws:iam::000000000000:role/vgi-test", RoleSessionName="vgi-test", DurationSeconds=seconds
    )["Credentials"]
    return creds


def _env(endpoint: str, bucket: str, **extra: str) -> dict[str, str]:
    key, secret = _root()
    return {
        "VGI_STORAGE_S3_BUCKET": bucket,
        "VGI_STORAGE_S3_ENDPOINT": endpoint,
        "VGI_STORAGE_S3_PREFIX": "vgi/",
        "VGI_STORAGE_S3_ACCESS_KEY_ID": key,
        "VGI_STORAGE_S3_SECRET_ACCESS_KEY": secret,
        "VGI_STORAGE_ALLOW_PLAIN_HTTP": "1",
        **extra,
    }


def _exercise(storage: S3Storage, http: httpx2.Client) -> None:
    """All four roles: object store, result externalization, upload grant (sha256 enforced)."""
    key = storage.put_object(b"state-ref object")
    assert storage.get(key) == b"state-ref object"
    url = storage.upload(b"result bytes", None)  # type: ignore[arg-type]
    assert http.get(url).content == b"result bytes"
    body = os.urandom(70_000)
    grant = storage.generate_upload_url(len(body), hashlib.sha256(body).hexdigest())
    assert http.put(grant.upload_url, content=b"x" * len(body), headers=dict(grant.headers)).status_code == 400
    assert http.put(grant.upload_url, content=body, headers=dict(grant.headers)).status_code == 200
    assert http.get(grant.download_url).content == body
    assert storage.get(grant.object_id or "") == body


@_needs_s3
class TestS3CompatHttp:
    """S3-compatible server over plain http, path-style."""

    @pytest.fixture
    def bucket(self) -> str:
        """Create a fresh bucket."""
        return _make_bucket(_S3)

    def test_static_keys(self, bucket: str) -> None:
        """Explicit static keys, all roles."""
        cfg = direct_storage_from_env(_env(_S3, bucket, VGI_STORAGE_S3_VERIFY="1"))
        assert cfg is not None and cfg.verify_at_startup
        storage = cfg.storage
        assert isinstance(storage, S3Storage)
        storage.verify()
        with httpx2.Client() as http:
            _exercise(storage, http)
        assert urlsplit(storage.generate_upload_url(1, _SHA).upload_url).path.startswith(f"/{bucket}/vgi/")

    def test_session_token_from_sts(self, bucket: str) -> None:
        """Temporary keys + session token from STS AssumeRole, all roles."""
        creds = _sts_credentials(_S3)
        env = _env(
            _S3,
            bucket,
            VGI_STORAGE_S3_ACCESS_KEY_ID=creds["AccessKeyId"],
            VGI_STORAGE_S3_SECRET_ACCESS_KEY=creds["SecretAccessKey"],
            VGI_STORAGE_S3_SESSION_TOKEN=creds["SessionToken"],
        )
        storage = s3_storage_from_env(env)
        with httpx2.Client() as http:
            _exercise(storage, http)
        assert "X-Amz-Security-Token" in storage.upload(b"x", None)  # type: ignore[arg-type]

    def test_session_token_from_aws_env_chain(self, bucket: str, monkeypatch: pytest.MonkeyPatch) -> None:
        """No VGI_STORAGE_S3_* keys: the standard AWS_* variables (with AWS_SESSION_TOKEN) are used."""
        creds = _sts_credentials(_S3)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", creds["AccessKeyId"])
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", creds["SecretAccessKey"])
        monkeypatch.setenv("AWS_SESSION_TOKEN", creds["SessionToken"])
        env = {k: v for k, v in _env(_S3, bucket).items() if "ACCESS_KEY" not in k}
        storage = s3_storage_from_env(env)
        with httpx2.Client() as http:
            _exercise(storage, http)

    def test_credential_process_caps_url_lifetime(
        self, bucket: str, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A profile with credential_process (STS, 15 min): URLs live at most expiry - 60 s, and work."""
        import sys

        script = tmp_path / "creds.py"
        key, secret = _root()
        script.write_text(
            "import boto3, json\n"
            f"c = boto3.client('sts', endpoint_url={_S3!r}, region_name='us-east-1', "
            f"aws_access_key_id={key!r}, aws_secret_access_key={secret!r}).assume_role("
            "RoleArn='arn:aws:iam::000000000000:role/vgi-cp', RoleSessionName='vgi-cp', "
            "DurationSeconds=900)['Credentials']\n"
            "print(json.dumps({'Version': 1, 'AccessKeyId': c['AccessKeyId'], "
            "'SecretAccessKey': c['SecretAccessKey'], 'SessionToken': c['SessionToken'], "
            "'Expiration': c['Expiration'].isoformat()}))\n"
        )
        config = tmp_path / "config"
        config.write_text(f"[profile vgi]\ncredential_process = {sys.executable} {script}\n")
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("AWS_CONFIG_FILE", str(config))
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "none"))
        monkeypatch.setenv("AWS_PROFILE", "vgi")
        env = {k: v for k, v in _env(_S3, bucket).items() if "ACCESS_KEY" not in k}
        storage = s3_storage_from_env(env)
        url = storage.upload(b"short-lived", None)  # type: ignore[arg-type]
        assert _expires(url) <= 900 - 60
        assert httpx2.get(url).content == b"short-lived"

    def test_checksum_none(self, bucket: str) -> None:
        """checksum=none: objects and results work without checksum headers; no grants."""
        cfg = direct_storage_from_env(_env(_S3, bucket, VGI_STORAGE_S3_CHECKSUM="none"))
        assert cfg is not None and cfg.upload_url_provider is None
        storage = cfg.storage
        assert isinstance(storage, S3Storage)
        key = storage.put_object(b"no checksum")
        assert storage.get(key) == b"no checksum"
        assert httpx2.get(storage.upload(b"r", None)).content == b"r"  # type: ignore[arg-type]

    def test_public_endpoint(self, bucket: str) -> None:
        """Requests go to 127.0.0.1, pre-signed URLs name localhost (both reach the server)."""
        public = _S3.replace("127.0.0.1", "localhost")
        storage = s3_storage_from_env(_env(_S3, bucket, VGI_STORAGE_S3_PUBLIC_ENDPOINT=public))
        grant = storage.generate_upload_url(3, hashlib.sha256(b"abc").hexdigest())
        assert urlsplit(grant.upload_url).hostname == "localhost"
        assert httpx2.put(grant.upload_url, content=b"abc", headers=dict(grant.headers)).status_code == 200
        assert storage.get(grant.object_id or "") == b"abc"

    def test_verify_messages(self, bucket: str) -> None:
        """The startup check names the problem: missing bucket, bad credentials."""
        missing = s3_storage_from_env(_env(_S3, f"{bucket}-missing"))
        with pytest.raises(StorageConfigError, match="does not exist"):
            missing.verify()
        with pytest.raises(StorageConfigError, match="does not exist"):
            missing.put_object(b"x")  # first use runs the same check
        bad = s3_storage_from_env(_env(_S3, bucket, VGI_STORAGE_S3_SECRET_ACCESS_KEY="wrong-secret"))
        with pytest.raises(StorageConfigError, match=r"Access denied.*explicit keys") as info:
            bad.verify()
        assert "wrong-secret" not in str(info.value)


@pytest.mark.skipif(not (_S3 and _S3_TLS), reason="set VGI_TEST_S3_TLS_ENDPOINT and VGI_TEST_S3_CA_FILE")
class TestS3CompatTls:
    """S3-compatible server over https with a private CA."""

    @pytest.fixture
    def ca(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
        """Trust the test CA for boto3 (AWS_CA_BUNDLE) and httpx (SSL_CERT_FILE)."""
        path = os.environ["VGI_TEST_S3_CA_FILE"]
        monkeypatch.setenv("AWS_CA_BUNDLE", path)
        monkeypatch.setenv("SSL_CERT_FILE", path)
        yield path

    def test_static_and_session_token(self, ca: str) -> None:
        """Static keys, then STS session-token keys, over TLS; no plain-http opt-in needed."""
        bucket = _make_bucket(_S3_TLS, verify=ca)
        env = _env(_S3_TLS, bucket)
        del env["VGI_STORAGE_ALLOW_PLAIN_HTTP"]
        cfg = direct_storage_from_env(env)
        assert cfg is not None
        assert isinstance(cfg.storage, S3Storage)
        with httpx2.Client(verify=ca) as http:
            _exercise(cfg.storage, http)
            creds = _sts_credentials(_S3_TLS)
            tok = s3_storage_from_env(
                {
                    **env,
                    "VGI_STORAGE_S3_ACCESS_KEY_ID": creds["AccessKeyId"],
                    "VGI_STORAGE_S3_SECRET_ACCESS_KEY": creds["SecretAccessKey"],
                    "VGI_STORAGE_S3_SESSION_TOKEN": creds["SessionToken"],
                }
            )
            _exercise(tok, http)
