# TypeScript: Missing OpenTelemetry instrumentation

**Repo:** ~/Development/vgi-rpc-typescript
**Priority:** Feature gap — Python and Go both have OpenTelemetry support

## Problem

The Python and Go implementations both have OpenTelemetry instrumentation for distributed tracing and metrics. The TypeScript implementation has no OpenTelemetry support.

## What Exists in Other Implementations

### Python (`vgi_rpc/otel.py`)
- `OtelMiddleware` — server middleware that creates spans for each RPC call
- Extracts W3C `traceparent`/`tracestate` from request metadata for distributed tracing
- Records span attributes: method name, method type, server ID, request ID
- Records call statistics: input/output batch counts, row counts, byte counts
- Records errors as span exceptions
- Configurable tracer and meter providers

### Go (`vgirpc/otel/`)
- Implements `DispatchHook` interface for automatic instrumentation
- Extracts W3C trace context from transport metadata
- Creates spans with method/server attributes
- Records request counter and duration histogram metrics
- Separate Go module (`vgirpc/otel`) with its own `go.mod`

### Wire Protocol Support (already in TypeScript)
The TypeScript implementation already reads/writes `traceparent` and `tracestate` metadata keys (defined in `src/constants.ts`). The wire protocol support is there — the instrumentation layer is missing.

## Implementation Approach

Add OpenTelemetry support as an optional module. The TypeScript implementation should:

1. Provide a dispatch hook or middleware that wraps method handlers
2. Extract `traceparent`/`tracestate` from request metadata
3. Create spans for each RPC call with appropriate attributes
4. Record basic metrics (request count, duration)
5. Use the `@opentelemetry/api` package (peer dependency)

## Python Reference

- `~/Development/vgi-rpc/vgi_rpc/otel.py` — full implementation (~150 lines)
- `~/Development/vgi-rpc/tests/test_otel.py` — tests

## Go Reference

- `~/Development/vgi-rpc-go/vgirpc/otel/` — Go implementation as separate module

## Verification

```bash
make test-unit
make test-conformance
```
