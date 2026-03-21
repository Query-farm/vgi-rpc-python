# Go: Upgrade __describe__ schema from v2 to v3

**Repo:** ~/Development/vgi-rpc-go
**Reference:** Python commit `d8c9984` — added is_exchange and param_docs_json fields

## Problem

The Python implementation upgraded `DESCRIBE_VERSION` from `"2"` to `"3"`, adding two new fields to the `__describe__` response schema. The Go implementation is still at v2 and is missing these fields.

## What v3 Adds

Two new nullable fields appended to the describe schema:

### 1. `is_exchange` (boolean, nullable)

For stream methods, indicates the stream pattern:
- `true` — exchange (bidirectional: client sends input, server sends output)
- `false` — producer (server-initiated, client sends empty ticks)
- `nil` — unknown

For unary methods: always `nil`.

This information is already available in the Go server — each registered stream method knows whether it was registered via `Producer()`, `Exchange()`, or `DynamicStreamWithHeader()`. The field just needs to be populated in the describe response.

### 2. `param_docs_json` (utf8/string, nullable)

JSON object mapping parameter names to their documentation strings. For example:
```json
{"count": "Number of items to produce", "prefix": "String prefix for each item"}
```

In Python, these are extracted from Google-style docstring `Args:` sections. In Go, there is no equivalent automatic source — this field can be set to `nil` for now, or populated from struct field comments if desired.

## Changes Required

In `vgirpc/describe.go`:

1. Update the constant:
   ```go
   DescribeVersion = "3"
   ```

2. Add two fields to the describe schema (after `header_schema_ipc`):
   ```go
   {Name: "is_exchange", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
   {Name: "param_docs_json", Type: arrow.BinaryTypes.String, Nullable: true},
   ```

3. Populate `is_exchange` in the describe batch builder:
   - For `Producer` / `ProducerWithHeader` methods: `false`
   - For `Exchange` / `ExchangeWithHeader` methods: `true`
   - For `DynamicStreamWithHeader` methods: `nil` (unknown at registration time)
   - For `Unary` / `UnaryVoid` methods: `nil`

4. Populate `param_docs_json` as `nil` for all methods (Go doesn't have docstring parsing).

## Key Files

- `vgirpc/describe.go` — lines 20-41: schema definition, constant, and batch builder
- `vgirpc/server.go` — `methodInfo` struct: check how method type (producer vs exchange) is tracked

## Python Reference

- `~/Development/vgi-rpc/vgi_rpc/introspect.py` — lines 86-100: `_DESCRIBE_SCHEMA` with all 12 fields
- `~/Development/vgi-rpc/vgi_rpc/introspect.py` — lines 264-340: `build_describe_batch()` showing how fields are populated

## Verification

```bash
# Rebuild and run conformance
make test

# Also verify with describe_diff.py from vgi-rpc-sync:
cd ~/Development/vgi-rpc-sync && uv run python describe_diff.py
# Should show: go 48 methods, describe v3
```
