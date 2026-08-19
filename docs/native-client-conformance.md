# Native-client conformance

The shared pytest suite proves server behavior by driving every worker with
the Python client. It cannot prove that a foreign SDK builds request batches
from its declared schema instead of inferring types from runtime samples.

Each SDK must therefore run complementary native-client tests against the
Python reference worker. The original typed-exchange lane remains available
at the root prefix:

```bash
python -m vgi_rpc.conformance.client_worker --http 0
```

The worker prints `PORT:<n>` and exposes one exchange method,
`typed_exchange`. Its exact input and output schema is exported as
`vgi_rpc.conformance.client_worker.TYPED_EXCHANGE_SCHEMA`:

- `nullable_float: float64`, nullable
- `tags: list<utf8>`, nullable, with nullable items
- `category: dictionary<int16, utf8>`, nullable
- `event_time: timestamp[us, tz=UTC]`, nullable
- `amount: decimal128(18, 4)`, nullable
- `nested: struct<name: utf8, scores: list<int32>>`, nullable; both children
  and list items are nullable

The native test must send and verify at least:

1. One row in which every field is null.
2. A zero-row batch that still carries the exact declared schema.
3. A populated row covering dictionary, timestamp, decimal, list, and nested
   struct values.

Clients must construct these batches from the declared schema. Inferring from
the all-null or empty samples is non-conformant and is intentionally rejected
by the Python worker before dispatch.

## Raw native-client lane

The same additive `ClientConformanceService` can be launched over the raw
Arrow-IPC transports used by a native `RpcClient`. No separate fixture service
or canonical `ConformanceService` changes are required.

For a child process whose stdin/stdout are owned by the client:

```bash
python -m vgi_rpc.conformance.client_worker --stdio
```

Stdout is exclusively the binary RPC channel in this mode; the worker emits no
text discovery line. A launcher must not inherit that stdout as a terminal or
attempt to parse it as logs.

For a discoverable local socket:

```bash
python -m vgi_rpc.conformance.client_worker --unix /tmp/vgi-client-worker.sock
```

The first stdout line is `UNIX:<absolute-path>`. On Windows, use the platform's
named-pipe launcher contract rather than this AF_UNIX fixture.

For raw loopback TCP with an OS-selected port:

```bash
python -m vgi_rpc.conformance.client_worker --tcp 127.0.0.1:0
```

The first stdout line is `TCP:<host>:<bound-port>`. Raw TCP has no TLS or
authentication and is suitable only for a trusted or loopback network.

All three modes expose the same methods and built-ins on one persistent raw
connection. A native client must prove:

- `echo_bytes(value)` is a unary byte-for-byte round trip;
- `producer_sequence(count=3, payload_bytes=4)` is consumed by sending
  producer tick batches until the server closes the response stream, yielding
  indices 0, 1, and 2 exactly once;
- `typed_exchange()` accepts multiple application batches on its open exchange
  stream and echoes the exact declared schema and values;
- `__describe__` returns describe v4 for `ClientConformanceService`, including
  the unary, producer, and exchange shapes; and
- `__transport_options__` is synthetic (absent from describe) but returns an
  empty response batch whose metadata includes `vgi_rpc.transport.shm`.

Before using shared memory, the client sends an empty-schema
`__transport_options__` request with its own
`vgi_rpc.transport.shm=true`. It may use SHM only if the response also says
`true`. The client then owns a wire-compatible segment, advertises
`vgi_rpc.shm_segment_name` and `vgi_rpc.shm_segment_size`, and replaces eligible
large batches with `vgi_rpc.shm_offset`/`vgi_rpc.shm_length` pointer batches.
The worker attaches through the existing server transport path and may return
the response through the same segment. The client must resolve and release the
allocation before closing and unlinking its segment. A missing method, `false`
response, or unavailable local SHM must fall back to inline framing. SHM is
only safe when both processes are known to share the same host; the TCP fixture
defaults to loopback for that reason.

## Full native HTTP-client lane

The worker also exposes additive fixtures for the HTTP-client functionality
that cannot be proved by driving a foreign server with Python. These methods
belong only to `ClientConformanceService`; they do not modify the canonical
`ConformanceService` or its server conformance contract.

Use `/vgi` for the full lane so clients can keep their production prefix:

```bash
python -m vgi_rpc.conformance.client_worker --http 0 --prefix /vgi
```

The first stdout line is `PORT:<n>`. Tests should wait for
`GET /vgi/health` before issuing RPCs.

### Describe acceptance contract

The worker enables describe v4. A native client must call
`POST /vgi/__describe__` with an empty-schema, zero-row parameter batch
carrying `vgi_rpc.method=__describe__` and `vgi_rpc.request_version=1`.
It must:

- accept the canonical eight-column describe-v4 schema;
- preserve the response custom metadata, including the 64-character
  `vgi_rpc.protocol_hash`, `vgi_rpc.protocol_name=ClientConformanceService`,
  `vgi_rpc.describe_version=4`, and `vgi_rpc.server_id`;
- deserialize every `params_schema_ipc`, `result_schema_ipc`, and present
  `header_schema_ipc` value as an Arrow schema;
- report `typed_exchange` as an exchange and the four `producer_*` methods as
  producers rather than inferring stream shape from a runtime batch.

### Producer acceptance contract

The output schema for every producer fixture is exact:

- `index: int64`, non-nullable
- `payload: binary`, non-nullable

The native producer client must implement the following state machine:

1. `POST /vgi/{method}/init` can return application data immediately, before
   the client has sent a separate tick.
2. A response can contain one or more application batches followed by a
   zero-row continuation sentinel. Queue every application batch.
3. Identify the continuation sentinel by
   `vgi_rpc.stream_state#b64`, never merely by `num_rows == 0`.
4. Retain `vgi_rpc.call_state#b64` from init for the whole call. Continuation
   responses rotate only `vgi_rpc.stream_state#b64` and do not reissue the
   call-state token.
5. Resume with `POST /vgi/{method}/exchange`, sending an empty-schema,
   zero-row tick carrying both the latest cursor and the retained call token.
6. A valid IPC stream with no application or continuation batch is terminal.
   A data batch without a following continuation is also terminal.
7. Once terminal, further iteration returns end-of-stream locally rather than
   replaying the last cursor.

Required cases:

- `producer_sequence(count=2, payload_bytes=4)` yields indices 0 and 1 over
  init plus resumptions, followed by an empty terminal response.
- `producer_zero_row_then_value()` yields a metadata-free zero-row
  *application* batch, then index 7 on the next turn. The zero-row data must
  be delivered rather than swallowed as control.
- `producer_emit_and_finish()` yields index 99 in init with no continuation.
- `producer_empty()` terminates in init with no data.

Clients must also run a buffered-init case:

```bash
python -m vgi_rpc.conformance.client_worker \
  --http 0 --prefix /vgi --producer-turn-bytes 16384
```

Calling `producer_sequence(count=100, payload_bytes=1024)` produces multiple
pending application batches followed by a cursor in the init response. This
is the case that rejects clients which remember only the last decoded batch.

### Sticky-session acceptance contract

Start the focused mode with:

```bash
python -m vgi_rpc.conformance.client_worker --http 0 --prefix /vgi --sticky
```

`OPTIONS /vgi/health` advertises sticky support, a 60-second default TTL, and
`X-VGI-Worker-Affinity` in `VGI-Sticky-Echo-Headers`. A conforming client must:

- send `VGI-Session-Accept: true` while opening a session;
- capture `VGI-Session` and each `VGI-Echo-*` response header;
- resend the token and the stripped echo header on subsequent unary and
  stream requests made through that session view;
- clear its token after `close_client_session()` returns with
  `VGI-Session-Close: true`;
- make local close/destruction idempotent and use
  `DELETE /vgi/__session__` for best-effort teardown of a still-live token;
- surface reuse of the explicitly closed token as the typed remote
  `SessionLostError`, not as an Arrow parse error or an anonymous HTTP error.

The required value sequence is open at 10, increment by 5 to 15, increment by
-2 to 13, close at 13, then prove the saved token is stale.

### External-location acceptance contract

Start the worker and its embedded credential-free object store with:

```bash
python -m vgi_rpc.conformance.client_worker \
  --http 0 --prefix /vgi --external --external-threshold 4096
```

Capabilities advertise externalization, a 4096-byte direct-request limit,
the synthetic upload-URL endpoint, and a 16 MiB upload limit.

For responses, `large_response(size=32768)` returns a zero-row pointer batch
with `vgi_rpc.location` and `vgi_rpc.location.sha256`. The client must fetch
the URL without storage credentials, verify the SHA-256 over decoded bytes,
open the fetched Arrow IPC stream, and return the 32 KiB value. A pointer is
not an empty application result.

For requests, call `echo_bytes` with the same 32 KiB value. After discovering
that the serialized request exceeds `VGI-Max-Request-Bytes`, the client must:

1. request a method-bound pair from `POST /vgi/__upload_url__/init`;
2. PUT the complete original Arrow IPC request to `upload_url`;
3. send a zero-row pointer batch naming `download_url` to `echo_bytes`;
4. resolve the externalized response and obtain the original bytes.

Do not reuse the PUT URL as the pointer GET URL. The fixture deliberately
rejects the wrong HTTP method so a one-URL implementation cannot pass.

### TLS acceptance contract

TLS is an independent transport fixture (the embedded external store uses
plain loopback HTTP, so test externalization separately):

```bash
python -m vgi_rpc.conformance.client_worker --http 0 --prefix /vgi --tls
```

The worker prints `PORT:<n>` followed by `TLS-CA:<path>`. The certificate has
SANs for `localhost` and `127.0.0.1`, is valid for one day, and the server
requires TLS 1.2 or newer. A native client must prove:

- the connection fails under default system trust;
- it succeeds when `TLS-CA` is installed as the trust anchor;
- hostname/IP verification remains enabled; and
- a trusted HTTPS call such as `__describe__` completes normally.

Disabling certificate verification is not a conformant way to pass this lane.
