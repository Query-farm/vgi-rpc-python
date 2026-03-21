# TypeScript: Upgrade __describe__ schema from v2 to v3

**Repo:** ~/Development/vgi-rpc-typescript
**Reference:** Python commit `d8c9984` — added is_exchange and param_docs_json fields

## Problem

The Python implementation upgraded `DESCRIBE_VERSION` from `"2"` to `"3"`, adding two new fields to the `__describe__` response schema. The TypeScript implementation is still at v2 and is missing these fields.

## What v3 Adds

Two new nullable fields appended to the describe schema:

### 1. `is_exchange` (boolean, nullable)

For stream methods, indicates the stream pattern:
- `true` — exchange (bidirectional: client sends input, server sends output)
- `false` — producer (server-initiated, client sends empty ticks)
- `null` — unknown

For unary methods: always `null`.

The TypeScript `Protocol` builder already tracks whether a method was registered via `.producer()` or `.exchange()`. This information just needs to be included in the describe output.

### 2. `param_docs_json` (utf8/string, nullable)

JSON object mapping parameter names to their documentation strings. For example:
```json
{"count": "Number of items to produce", "prefix": "String prefix for each item"}
```

In Python, these are extracted from docstrings. In TypeScript, the `doc` field on method definitions could be parsed, or this field can be set to `null` for now.

## Changes Required

In `src/constants.ts`:
```typescript
export const DESCRIBE_VERSION = "3";
```

In `src/dispatch/describe.ts`:

1. Add two fields to the describe schema (after `header_schema_ipc`):
   ```typescript
   new Field("is_exchange", new Bool(), true),
   new Field("param_docs_json", new Utf8(), true),
   ```

2. Populate `is_exchange` in the describe batch builder:
   - For `.producer()` methods: `false`
   - For `.exchange()` methods: `true`
   - For `.unary()` methods: `null`

3. Populate `param_docs_json` as `null` for all methods (or extract from `doc` if desired).

## Key Files

- `src/constants.ts` — line 18: `DESCRIBE_VERSION`
- `src/dispatch/describe.ts` — lines 29-40: schema definition, lines 50+: batch builder
- `src/protocol.ts` — method registration: check how method type (producer vs exchange) is stored

## Python Reference

- `~/Development/vgi-rpc/vgi_rpc/introspect.py` — lines 86-100: `_DESCRIBE_SCHEMA` with all 12 fields
- `~/Development/vgi-rpc/vgi_rpc/introspect.py` — lines 264-340: `build_describe_batch()` showing how fields are populated

## Verification

```bash
# Run unit tests
make test-unit

# Run conformance (requires Python CLI)
make test-conformance

# Also verify with describe_diff.py from vgi-rpc-sync:
cd ~/Development/vgi-rpc-sync && uv run python describe_diff.py
# Should show: typescript 48 methods, describe v3
```
