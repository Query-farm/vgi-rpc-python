# Native-client conformance

The shared pytest suite proves server behavior by driving every worker with
the Python client. It cannot prove that a foreign SDK builds request batches
from its declared schema instead of inferring types from runtime samples.

Each SDK must therefore run a complementary native-client test against the
Python reference worker:

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
