# Sentry

Server-side Sentry instrumentation for unhandled exception capture and optional performance monitoring. Requires `pip install vgi-rpc[sentry]`.

vgi-rpc does **not** manage Sentry's DSN or SDK lifecycle — call `sentry_sdk.init()` yourself (or rely on the SDK's own auto-init from `SENTRY_DSN`).

## Automatic instrumentation

If `sentry_sdk` is initialised in the worker process, vgi-rpc attaches default-config Sentry instrumentation to every `RpcServer` automatically. No flag, no extra env var — `sentry_sdk.is_initialized()` is the signal of intent.

```python
import sentry_sdk
sentry_sdk.init(dsn="https://...")        # or just set SENTRY_DSN

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
| `tags["auth.domain"]` | `auth.domain` (when set) |
| `tags["auth.authenticated"]` | `"true"` or `"false"` |
| `tags[<claim_tag>]` | Mapped via `SentryConfig.claim_tags` |
| `tags[<custom_tag>]` | From `SentryConfig.custom_tags` |

Unhandled exceptions are sent via `sentry_sdk.capture_exception()` unless their type appears in `SentryConfig.ignored_exceptions` (e.g. ignore `PermissionError` for noisy auth-failure events).

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
