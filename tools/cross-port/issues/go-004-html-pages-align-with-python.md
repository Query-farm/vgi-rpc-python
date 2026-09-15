# Go: Align HTML describe page with Python reference

**Status:** RESOLVED — commit `6eae8bc` in vgi-rpc-go
**Repo:** ~/Development/vgi-rpc-go
**Priority:** Consistency — HTML pages should match the Python reference implementation

## Problem

The Go HTTP describe page has three differences from the Python reference:

### 1. Extra badges: `producer` and `exchange`

Python uses only `unary`, `stream`, and `header` badges. Go adds `producer` and `exchange` badges for stream methods. TypeScript was aligned to match Python.

**Python:** `echo_scale → [stream]`
**Go:** `echo_scale → [exchange, stream]`

### 2. Missing Description column

Python's parameter table has 4 columns: Name, Type, Default, Description.
Go has only 3: Name, Type, Default.

### 3. Type name: `string` vs `str`

Python uses `str` for UTF-8 string types. Go uses `string`. TypeScript was aligned to `str`.

## Changes Required

In `vgirpc/http_pages.go`:

1. **Remove producer/exchange badges** from `buildMethodCard()`. Only render `UNARY`/`STREAM` and `HEADER` badges.

2. **Add Description column** to the parameters table. For now, use `&mdash;` as Go doesn't have docstring-based parameter descriptions.

3. **Change type name** in `arrowTypeToString()`: return `"str"` instead of `"string"` for Arrow String type.

## Key Files

- `vgirpc/http_pages.go` — method card builder, badge rendering, type name mapping

## Verification

```bash
make lint
go test ./vgirpc/
make test

# Cross-impl test:
cd ~/Development/vgi-rpc-sync && uv run pytest test_html_pages.py -v
# Go xfail tests should now pass
```
