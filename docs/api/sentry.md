# Sentry

Server-side Sentry instrumentation for unhandled exception capture and optional performance monitoring. Requires `pip install vgi-rpc[sentry]`.

vgi-rpc does **not** manage Sentry's DSN or SDK lifecycle — you must call `sentry_sdk.init()` yourself. Setting `SENTRY_DSN` alone is **not** enough: the Sentry SDK does not auto-initialise on import. `sentry_sdk.init()` will fall back to reading `SENTRY_DSN` from the environment only if you don't pass an explicit `dsn=`.

## Automatic instrumentation

If `sentry_sdk` is initialised in the worker process, vgi-rpc attaches default-config Sentry instrumentation to every `RpcServer` automatically. No flag, no extra env var — `sentry_sdk.is_initialized()` is the signal of intent.

```python
import sentry_sdk
sentry_sdk.init()                         # reads SENTRY_DSN if no dsn= passed

from vgi_rpc import RpcServer
server = RpcServer(MyService, MyServiceImpl())   # auto-attached
```

The check is gated on `sentry_sdk` already being importable in the process, so workers that have not opted into Sentry pay nothing.

**Order matters.** `sentry_sdk.init()` must run before `RpcServer(...)`. If the SDK is not initialised when the constructor runs, auto-attach is a no-op (logged at `DEBUG` on `vgi_rpc.sentry`); attach explicitly later if needed.

## Customising the configuration

Call `instrument_server_sentry()` explicitly with a `SentryConfig` to override the auto-attached default. The explicit call **replaces** any prior Sentry hook in the dispatch chain — explicit config wins regardless of order:

```python
from vgi_rpc.sentry import SentryConfig, instrument_server_sentry

server = RpcServer(MyService, MyServiceImpl())   # auto-attached default
instrument_server_sentry(
    server,
    SentryConfig(custom_tags={"env": "prod"}, enable_performance=True),
)  # replaces the default-config hook
```

For HTTP, pass `sentry_config=` directly to `make_wsgi_app()`/`serve_http()` — same replace semantics:

```python
from vgi_rpc.http import make_wsgi_app

app = make_wsgi_app(server, sentry_config=SentryConfig(custom_tags={"env": "prod"}))
```

## Captured Context

When `record_request_context=True` (the default), each Sentry event carries:

| Sentry field | Source |
|---|---|
| `contexts.rpc.method` | RPC method name |
| `contexts.rpc.method_type` | `"unary"` or `"stream"` |
| `contexts.rpc.service` | Protocol class name |
| `contexts.rpc.server_id` | `RpcServer.server_id` |
| `user.username` | `auth.principal` (when authenticated) |
| `tags["rpc.method"]` | RPC method name (always — for Issues filtering) |
| `tags["rpc.method_type"]` | `"unary"` or `"stream"` (always) |
| `tags["auth.domain"]` | `auth.domain` (when set) |
| `tags["auth.authenticated"]` | `"true"` or `"false"` |
| `tags[<claim_tag>]` | Mapped via `SentryConfig.claim_tags` |
| `tags[<custom_tag>]` | From `SentryConfig.custom_tags` |
| span data `rpc.system` | Always `"vgi_rpc"` |
| span data `rpc.service` | Protocol class name |
| span data `rpc.method` | RPC method name |
| span data `rpc.method_type` | `"unary"` / `"stream"` |
| span data `rpc.stream_id` | uuid shared across all HTTP turns of one stream call (streams only) |

Unhandled exceptions are sent via `sentry_sdk.capture_exception()` unless their type appears in `SentryConfig.ignored_exceptions` (e.g. ignore `PermissionError` for noisy auth-failure events).

## Transaction naming

By default vgi-rpc replaces Sentry's WSGI-derived transaction name (a literal route template like `/{method}` from Falcon) with `rpc {method}`, so transactions group by RPC method in Sentry's Performance dashboard. Disable with `SentryConfig(set_transaction_name=False)` if you have alerts pinned to the route-template names.

## RPC parameters and Trace Explorer

Opt in to per-call argument recording with `SentryConfig(record_params=True)`. Kwargs become `rpc.param.<k>` span attributes on the transaction's root span — searchable in [Sentry Trace Explorer / Insights](https://docs.sentry.io/product/explore/trace-explorer/).

```python
config = SentryConfig(
    record_params=True,
    tag_params=("table", "format"),     # also duplicate as scope tags for Issues
)
```

After enabling, queries like the following work in Trace Explorer:

```
span.op:rpc.server rpc.method:executeScan rpc.param.table:orders
-> chart p99(span.duration) GROUP BY rpc.param.predicate
```

This is the same pattern Sentry's own SQLAlchemy integration uses (`db.system`, `db.name`, etc.).

**Tags vs. span data.** `rpc.param.<k>` lands in two places:

- **Span data (always when `record_params=True`)**: high-cardinality, indexed for Trace Explorer and Insights, supports aggregations (`p99 GROUP BY`).
- **Scope tags (only for keys listed in `tag_params`)**: low-cardinality, used for filtering error events in the Issues view. Operator-curated whitelist — never auto-promoted.

**Caveats:**

- **Stream `/exchange` turns don't carry kwargs.** Params live on the `/init` transaction only. Filter Trace Explorer by `rpc.stream_id` to find sibling turns of one logical call.
- **Sentry's default scrubber matches kwarg key names only.** Free-text values such as predicates pass through. Supply a `param_redactor` callable, or use Sentry [Advanced Data Scrubbing](https://docs.sentry.io/security-legal-pii/scrubbing/server-side-scrubbing/), if your kwargs may contain PII. `send_default_pii=False` does **not** save you here.
- **Long string values are truncated** at `max_param_value_bytes` (default 1024 — Relay's practical per-attribute cap). A debug log line records the truncation.
- **Span attributes accept primitives only**: `str`, `bool`, `int`, `float`, and homogeneous lists of one of those types. Dicts, bytes, dataclasses, and mixed-type lists are silently dropped to avoid Sentry ingestion errors.
- **`tag_params` values are clipped to 200 chars** (Sentry's tag value cap). Keep this whitelist for short identifiers.

## Performance Monitoring

Performance is **opt-in** to avoid duplicate tracing when used alongside [OpenTelemetry](otel.md) and to conserve Sentry quota:

```python
config = SentryConfig(enable_performance=True, op_name="rpc.server")
instrument_server_sentry(server, config)
```

Each dispatch then starts a Sentry transaction named `vgi_rpc/<method>` with operation `op_name` (default `"rpc.server"`), finished with status `ok` or `internal_error` based on dispatch outcome.

## Coexistence with OpenTelemetry

Sentry and OTel hooks compose into a `_CompositeDispatchHook` and run independently — register either order:

```python
from vgi_rpc.otel import OtelConfig, instrument_server
from vgi_rpc.sentry import SentryConfig, instrument_server_sentry

instrument_server(server, OtelConfig(...))
instrument_server_sentry(server, SentryConfig())
```

Hook failures are isolated — one hook raising does not stop the others.

## API Reference

### SentryConfig

::: vgi_rpc.sentry.SentryConfig

### instrument_server_sentry

::: vgi_rpc.sentry.instrument_server_sentry
