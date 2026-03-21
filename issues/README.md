# Issues

Functional deficiencies across the three vgi-rpc implementations, written as actionable tasks for coding agents.

## Naming Convention

`{lang}-{number}-{short-description}.md` where lang is `go`, `ts`, or `py`.

## Current Issues

### Bugs (fix first)

| Issue | Repo | Summary |
|-------|------|---------|
| [go-001](go-001-http-conformance-broken.md) | Go | HTTP conformance tests broken — prefix mismatch + catch-all 404 handler |
| [ts-002](ts-002-node-http-conformance-404.md) | TypeScript | node-http conformance transport returns 404 |

### Schema Drift

| Issue | Repo | Summary |
|-------|------|---------|
| [go-002](go-002-upgrade-describe-v3.md) | Go | Upgrade `__describe__` schema from v2 to v3 |
| [ts-001](ts-001-upgrade-describe-v3.md) | TypeScript | Upgrade `__describe__` schema from v2 to v3 |

### Feature Gaps

| Issue | Repo | Summary |
|-------|------|---------|
| [ts-004](ts-004-missing-dispatch-hooks.md) | TypeScript | Missing dispatch hooks for observability |
| [ts-003](ts-003-missing-opentelemetry.md) | TypeScript | Missing OpenTelemetry instrumentation |
| [go-003](go-003-missing-http-client.md) | Go | No HTTP client (may be intentional) |
