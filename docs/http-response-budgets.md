# HTTP response budgets

VGI HTTP clients can place a hard bound on each RPC response by sending:

```http
VGI-Accept-Max-Response-Bytes: 268435456
```

The value is one ASCII positive decimal integer in the portable range
`65536..9007199254740991` (`2^53-1`). Combined/duplicate fields, commas, signs,
leading zeroes, non-ASCII digits, values below 65536, and larger values are invalid. HTTP
stacks may remove field-value optional whitespace before VGI sees it; the
grammar applies to the normalized field value delivered to the application.

Servers advertise support on OPTIONS and every other response:

```http
VGI-Accept-Max-Response-Bytes-Support: true
```

The support response header is CORS-exposed. A browser preflight requesting
the request header is accepted when CORS is enabled.

Clients configured with a hard limit must discover this capability before the
first RPC request and fail closed when support is absent. They then send the
configured hard limit on that first request and every continuation. Passing an
explicit unbounded value disables both the preflight and the request header.

## Precedence

Application and hosting limits are independent. The effective hard limit is
the minimum of every value that is present:

```text
min(max_response_bytes,
    hosting_max_response_bytes,
    VGI-Accept-Max-Response-Bytes)
```

The same minimum rule combines `max_request_bytes` and
`hosting_max_request_bytes`. A hosting setting can tighten an application's
setting but cannot weaken it. Server limits remain optional. Python's native
HTTP client sends a 256 MiB accepted-response limit by default; pass
`accepted_max_response_bytes=None` only when unbounded legacy behavior is
deliberate.

`preferred_response_bytes` is a server-side batching target, not another wire
header. It is clamped to the effective hard limit. Worker code sees the final
values as `CallContext.response_limit_bytes`,
`CallContext.preferred_response_bytes`, `OutputCollector.response_limit_bytes`,
and `OutputCollector.preferred_response_bytes`. The older
`OutputCollector.remaining_response_bytes` continues to report the current
turn's remainder after framing already written.

## Enforcement

The hard response limit counts decoded Arrow IPC bytes, including framing but
before gzip/zstd HTTP content coding. It covers unary responses, stream initialization,
producer continuations, and exchange responses. It is checked after ordinary
externalization, so a large result may still be rescued by replacing inline
data with an external-location pointer. `max_externalized_response_bytes`
remains a separate hard cap on uploaded payload bytes.

An otherwise-successful response that still exceeds the effective limit is
replaced by an Arrow EXCEPTION envelope with error type
`ResponseTooLargeError`, HTTP 200, and `X-VGI-RPC-Error: true`. Its message
contains `max_response_bytes (actual > limit)` and the method name. A producer
oversize response contains no continuation cursor; clients must not advance
from a result they were not allowed to accept.

The init-time effective hard limit is sealed into immutable continuation
state. Later continuation requests may tighten it, but cannot raise it.
Clients also enforce their advertised limit locally on the decoded body so a
misconfigured or non-conforming intermediary cannot make the bound advisory.

The normative language-neutral cases live in
`vgi_rpc/conformance/http_response_budget_vectors.json`.
