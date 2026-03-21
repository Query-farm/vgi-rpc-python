# TypeScript: Missing dispatch hooks for observability

**Status:** RESOLVED — commit `e6bc16a` in vgi-rpc-typescript
**Repo:** ~/Development/vgi-rpc-typescript
**Priority:** Feature gap — Python and Go both have dispatch hooks

## Problem

Python and Go both provide a dispatch hook mechanism that fires before and after each RPC method call, enabling observability (logging, metrics, tracing) without modifying handler code. TypeScript has no equivalent.

## What Exists in Other Implementations

### Python (`vgi_rpc/rpc/_server.py`)
- `Middleware` protocol with `on_dispatch_start()` and `on_dispatch_end()` methods
- `RpcServer` accepts a `middleware` list
- Each middleware receives method name, call context, and call statistics (input/output batches, rows, bytes)
- Used by `OtelMiddleware` and `SentryMiddleware`

### Go (`vgirpc/hooks.go`)
```go
type DispatchHook interface {
    OnDispatchStart(ctx context.Context, info DispatchInfo) (context.Context, HookToken)
    OnDispatchEnd(ctx context.Context, token HookToken, info DispatchInfo,
        stats *CallStatistics, err error)
}
```
- `DispatchInfo` carries method name, type, server ID, request ID, auth context
- `CallStatistics` carries input/output batch counts, row counts, byte counts
- Used by the OpenTelemetry integration

## Implementation Approach

Add a hook/middleware interface to the TypeScript `VgiRpcServer`:

```typescript
interface DispatchHook {
  onDispatchStart(info: DispatchInfo): HookToken;
  onDispatchEnd(token: HookToken, info: DispatchInfo, stats: CallStatistics, error?: Error): void;
}
```

This is a prerequisite for the OpenTelemetry feature (ts-003).

## Key Files

- `src/server.ts` — `VgiRpcServer` class, where hooks would be called
- `src/types.ts` — where `DispatchHook`, `DispatchInfo`, `CallStatistics` types would be defined

## Python Reference

- `~/Development/vgi-rpc/vgi_rpc/rpc/_server.py` — middleware implementation
- `~/Development/vgi-rpc/vgi_rpc/rpc/_common.py` — `CallStatistics` dataclass

## Go Reference

- `~/Development/vgi-rpc-go/vgirpc/hooks.go` — `DispatchHook` interface definition
- `~/Development/vgi-rpc-go/vgirpc/context.go` — `CallStatistics` struct

## Verification

```bash
make test-unit
```
