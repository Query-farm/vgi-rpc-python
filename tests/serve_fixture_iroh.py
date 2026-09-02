# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

"""One-shot native Iroh HTTP server used by the client qualification test."""

from __future__ import annotations

import asyncio
import json
from typing import cast

import iroh

from vgi_rpc.iroh import IROH_HTTP_ALPN


async def _main() -> None:
    iroh.iroh_ffi.uniffi_set_event_loop(cast("asyncio.BaseEventLoop", asyncio.get_running_loop()))
    endpoint = await iroh.Endpoint.bind(
        iroh.EndpointOptions(
            preset=iroh.preset_minimal(),
            alpns=[IROH_HTTP_ALPN],
            relay_mode=iroh.RelayMode.disabled(),
        )
    )
    addresses = [
        address.replace("0.0.0.0:", "127.0.0.1:").replace("[::]:", "[::1]:") for address in endpoint.bound_sockets()
    ]
    print(json.dumps({"endpoint_id": endpoint.id().to_bytes().hex(), "addresses": addresses}), flush=True)
    try:
        incoming = await endpoint.accept_next()
        assert incoming is not None
        accepting = await incoming.accept()
        connection = await accepting.connect()
        assert connection.alpn() == IROH_HTTP_ALPN
        stream = await connection.accept_bi()
        request_bytes = bytearray()
        while b"\r\n\r\n" not in request_bytes:
            request_bytes.extend(await stream.recv().read(4096))
        head, body = bytes(request_bytes).split(b"\r\n\r\n", 1)
        content_length = next(
            int(line.split(b":", 1)[1].strip())
            for line in head.split(b"\r\n")[1:]
            if line.lower().startswith(b"content-length:")
        )
        while len(body) < content_length:
            body += await stream.recv().read(content_length - len(body))
        print(json.dumps({"request_hex": (head + b"\r\n\r\n" + body).hex()}), flush=True)
        response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Type: text/plain\r\n\r\nhello"
        await stream.send().write_all(response)
        await stream.send().finish()
        # Do not close the endpoint immediately after FIN: on slower CI runners
        # that can race the client's final body read and turn a valid response
        # into a connection-level ReadError. Wait until the peer acknowledges the
        # finished send side (or its own close) first.
        await asyncio.wait_for(stream.send().stopped(), timeout=10)
    finally:
        await endpoint.close()


if __name__ == "__main__":
    asyncio.run(_main())
