# Native Iroh client

Install the official n0 binding alongside VGI RPC:

```console
pip install 'vgi-rpc[iroh]'
```

Raw Arrow-mux uses a stateful Iroh stream:

```python
from vgi_rpc import iroh_connect

with iroh_connect(MyProtocol, "iroh://<64-lowercase-hex-endpoint-id>") as worker:
    print(worker.some_method(value=1))
```

HTTP-over-Iroh retains VGI's stateless HTTP behavior and existing request,
response, continuation, and authentication headers:

```python
from vgi_rpc import httpi_connect

with httpi_connect(MyProtocol, "httpi://<64-lowercase-hex-endpoint-id>/vgi") as worker:
    print(worker.some_method(value=1))
```

Both connectors accept `secret_key` (32 bytes, 64 lowercase hex, or
52-character z-base-32), `relay_urls`, `no_relay`, `connect_timeout`,
`io_timeout`, and a `threading.Event` as `cancellation`.  `relay_urls` and
`no_relay` are mutually exclusive.  `direct_addresses` and
`remote_relay_url` can supply already-discovered peer addressing, which is
useful for isolated tests and private discovery systems.

The binding runs in-process.  VGI does not download a connector, write an
executable to a cache, or start a child process.  If the optional package is
absent, using an Iroh endpoint fails with a structured `unsupported` error.
