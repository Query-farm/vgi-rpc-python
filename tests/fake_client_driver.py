# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""A scripted client driver, used to test the driver-protocol shim itself.

Every real driver wraps a client and talks to a server.  This one talks to
nobody: it decodes the control requests, records them, and answers with canned
Arrow payloads.  That makes it possible to exercise the parts of
``vgi_rpc.conformance.client_driver`` that are about the *control protocol* —
the two error channels, log relay, stream termination, session ops — without
standing up a server and without a second language toolchain.

It deliberately answers a few ops the way a *non-conforming* driver would
(``fail_stream_open`` replies ``ok: false`` with a structured error), because
tolerating both spellings is part of the shim's job.

Run as ``python -m tests.fake_client_driver <record-path>``.
"""

from __future__ import annotations

import base64
import io
import json
import sys
from collections.abc import Callable
from typing import Any

import pyarrow as pa
from pyarrow import ipc

from vgi_rpc.conformance import ConformanceHeader
from vgi_rpc.metadata import RPC_METHOD_KEY


def _b64(data: bytes) -> str:
    """Base64-encode driver output."""
    return base64.standard_b64encode(data).decode("ascii")


def _one_batch(batch: pa.RecordBatch, metadata: dict[bytes, bytes] | None = None) -> str:
    """Serialize one batch (plus metadata) as a base64 IPC stream."""
    buf = io.BytesIO()
    with ipc.new_stream(buf, batch.schema) as writer:
        if metadata:
            writer.write_batch(batch, custom_metadata=metadata)
        else:
            writer.write_batch(batch)
    return _b64(buf.getvalue())


def _read_request(encoded: str) -> tuple[str, pa.RecordBatch, dict[bytes, bytes]]:
    """Decode a request IPC stream into (method, batch, metadata)."""
    reader = ipc.open_stream(io.BytesIO(base64.standard_b64decode(encoded)))
    batch, custom = reader.read_next_batch_with_custom_metadata()
    metadata = dict(custom.to_dict()) if custom is not None else {}
    return metadata.get(RPC_METHOD_KEY, b"").decode("utf-8"), batch, metadata


def _log(message: str) -> dict[str, Any]:
    """One relayed log record."""
    return {"level": "INFO", "message": message, "extra": {"origin": "fake"}}


class FakeDriver:
    """Answers the control protocol from a script rather than from a server."""

    def __init__(self, record_path: str | None) -> None:
        """Start with no connection, no stream and no session."""
        self.record_path = record_path
        self.ops: list[dict[str, Any]] = []
        self.session_token: str | None = None
        self.remaining = 0
        self.stream_kind = ""

    def record(self, request: dict[str, Any]) -> None:
        """Append one request to the on-disk transcript.

        JSONL rather than one rewritten array: the harness reads the file while
        this process is still appending to it, and a half-written array is not
        parseable while a half-written line simply is not there yet.
        """
        self.ops.append(request)
        if self.record_path is not None:
            with open(self.record_path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(request) + "\n")

    def handle(self, request: dict[str, Any]) -> dict[str, Any] | None:
        """Answer one control request, or return ``None`` to exit."""
        self.record(request)
        op = request.get("op")
        handler: Callable[[dict[str, Any]], dict[str, Any] | None] | None = getattr(self, f"_op_{op}", None)
        if handler is None:
            return {"ok": False, "error": f"unknown op: {op}"}
        return handler(request)

    # -- connection ------------------------------------------------------

    def _op_connect(self, request: dict[str, Any]) -> dict[str, Any]:
        if request.get("target") == "refuse":
            return {"ok": False, "error": "connection refused"}
        return {"ok": True}

    def _op_shutdown(self, request: dict[str, Any]) -> None:
        del request
        return

    def _op_describe(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        params = _one_batch(pa.RecordBatch.from_pydict({"value": pa.array([], pa.string())}))
        return {
            "ok": True,
            "logs": [],
            "error": None,
            "describe": {
                "protocol_name": "ConformanceService",
                "request_version": "1",
                "describe_version": "5",
                "protocol_hash": "deadbeef",
                "server_id": "fake-server",
                "protocol_version": "2.0.0",
                "methods": [
                    {
                        "name": "echo_string",
                        "method_type": "unary",
                        "has_return": True,
                        "has_header": False,
                        "is_exchange": None,
                        "params_schema_b64": params,
                        "result_schema_b64": None,
                        "header_schema_b64": None,
                    },
                    {
                        "name": "produce_n",
                        "method_type": "stream",
                        "has_return": False,
                        "has_header": False,
                        "is_exchange": False,
                        "params_schema_b64": None,
                        "result_schema_b64": None,
                        "header_schema_b64": None,
                    },
                ],
            },
        }

    # -- calls -----------------------------------------------------------

    def _op_unary(self, request: dict[str, Any]) -> dict[str, Any]:
        method, batch, _ = _read_request(request["request_b64"])
        if method == "raise_value_error":
            return {
                "ok": True,
                "result_b64": None,
                "logs": [_log("about to fail")],
                "error": {
                    "error_type": "ValueError",
                    "error_message": batch.column("message")[0].as_py(),
                    "traceback": "remote traceback",
                },
            }
        if method == "raise_runtime_error":
            # A driver generation that wrote `message` instead of `error_message`.
            return {"ok": True, "result_b64": None, "error": {"error_type": "RuntimeError", "message": "legacy"}}
        if method == "void_noop":
            return {"ok": True, "result_b64": None, "logs": [], "error": None}
        if method == "echo_string":
            value = batch.column("value")[0].as_py()
            return {
                "ok": True,
                "result_b64": _one_batch(pa.RecordBatch.from_pydict({"result": [value]})),
                "logs": [_log("echoed")],
                "error": None,
            }
        return {"ok": False, "error": f"fake driver has no method {method}"}

    def _op_stream_open(self, request: dict[str, Any]) -> dict[str, Any]:
        method, batch, _ = _read_request(request["request_b64"])
        if method == "produce_error_on_init":
            # Deliberately the non-conforming spelling: ok:false with a
            # structured error.  The shim must still raise the remote type.
            return {"ok": False, "error": {"error_type": "ValueError", "error_message": "init failed"}}
        if method == "exchange_error_on_init":
            return {"ok": True, "error": {"error_type": "ValueError", "error_message": "init failed"}, "logs": []}
        self.stream_kind = "exchange" if request.get("is_exchange") else "producer"
        header = None
        if request.get("has_header"):
            header = _b64(ConformanceHeader(total_expected=3, description="fake").serialize_to_bytes())
        self.remaining = int(batch.column("count")[0].as_py()) if "count" in batch.schema.names else 2
        return {"ok": True, "header_b64": header, "logs": [_log("stream opened")]}

    # -- stream ----------------------------------------------------------

    def _item(self, index: int, token: str | None = None) -> dict[str, Any]:
        batch = pa.RecordBatch.from_pydict({"index": [index], "value": [index * 10]})
        return {
            "ok": True,
            "done": False,
            "batch_b64": _one_batch(batch, {b"fake.index": str(index).encode()}),
            "token": token,
            "logs": [],
            "error": None,
        }

    def _end(self) -> dict[str, Any]:
        return {"ok": True, "done": True, "batch_b64": None, "token": None, "logs": [], "error": None}

    def _op_tick(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        if self.remaining <= 0:
            return self._end()
        self.remaining -= 1
        return self._item(self.remaining)

    def _op_next_with_token(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        if self.remaining <= 0:
            return self._end()
        self.remaining -= 1
        return self._item(self.remaining, token=f"token-{self.remaining}")

    def _op_exchange(self, request: dict[str, Any]) -> dict[str, Any]:
        reader = ipc.open_stream(io.BytesIO(base64.standard_b64decode(request["input_b64"])))
        batch, _ = reader.read_next_batch_with_custom_metadata()
        if batch.num_rows and "value" in batch.schema.names and batch.column("value")[0].as_py() == -1:
            return {"ok": True, "done": True, "batch_b64": None, "logs": [], "error": None}
        return self._item(batch.column("value")[0].as_py() if batch.num_rows else 0)

    def _op_cancel(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        self.remaining = 0
        return {"ok": True, "logs": [_log("cancelled")]}

    def _op_close(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        self.remaining = 0
        return {"ok": True}

    # -- http-only -------------------------------------------------------

    def _op_capabilities(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        return {
            "ok": True,
            "caps": {
                "sticky_enabled": True,
                "sticky_default_ttl": 300,
                "sticky_echo_headers": ["Backend"],
                "upload_url_support": True,
                "max_request_bytes": 1048576,
                "max_response_bytes": None,
                "max_externalized_response_bytes": None,
                "externalization_enabled": True,
                "max_upload_bytes": 16777216,
                "supported_encodings": ["zstd", "gzip", "bogus"],
            },
        }

    def _op_request_upload_urls(self, request: dict[str, Any]) -> dict[str, Any]:
        count = int(request.get("count", 1))
        return {
            "ok": True,
            "urls": [
                {"upload_url": f"http://up/{i}", "download_url": f"http://down/{i}", "expires_at": 1767225600}
                for i in range(count)
            ],
        }

    def _op_session_begin(self, request: dict[str, Any]) -> dict[str, Any]:
        self.session_token = request.get("token") or "minted-token"
        return {"ok": True}

    def _op_session_token(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        return {"ok": True, "token": self.session_token}

    def _op_session_echo_headers(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        return {"ok": True, "headers": {"Backend": "b7", "Dropped": 7}}

    def _op_session_detach(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        token, self.session_token = self.session_token, None
        return {"ok": True, "token": token}

    def _op_session_end(self, request: dict[str, Any]) -> dict[str, Any]:
        del request
        self.session_token = None
        return {"ok": True}


def main() -> None:
    """Run the control loop on stdin/stdout."""
    driver = FakeDriver(sys.argv[1] if len(sys.argv) > 1 else None)
    for line in sys.stdin:
        if not line.strip():
            continue
        response = driver.handle(json.loads(line))
        if response is None:
            sys.stdout.write(json.dumps({"ok": True}) + "\n")
            sys.stdout.flush()
            return
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
