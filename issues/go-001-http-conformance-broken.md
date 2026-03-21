# Go: HTTP conformance tests broken — URL prefix mismatch + catch-all 404 handler

**Repo:** ~/Development/vgi-rpc-go
**Severity:** All HTTP transport conformance tests fail (pipe/subprocess/unix all pass)
**Introduced by:** commit `85caef7` — "Add HTML landing page, API describe page, and 404 page"

## Problem

The Go HTTP conformance tests all fail because of a URL prefix mismatch combined with a catch-all HTML 404 handler.

**The mismatch:**
- The Python test harness (`test_go_conformance.py`, line 136-140) calls `http_connect(ConformanceService, f"http://127.0.0.1:{port}")` with no prefix — the Python client defaults to empty prefix `""`, so it sends requests to `POST /method_name`
- The Go HTTP server (`vgirpc/http.go`, line 102) defaults to prefix `/vgi`, so it registers routes at `POST /vgi/{method}`, `POST /vgi/{method}/init`, etc.
- Commit `85caef7` added a catch-all `h.mux.HandleFunc("/", h.handleNotFound)` that returns HTML for any unmatched route
- Requests to `POST /method_name` don't match `/vgi/{method}`, fall through to the catch-all, and return HTML instead of Arrow IPC

## Root Cause

The Go server's default HTTP prefix (`/vgi`) doesn't match what the Python HTTP client expects (empty prefix `""`). The Python client was recently updated to default to `""` (no prefix), but the Go server still defaults to `/vgi`.

## Fix

Either:

**Option A** (recommended): Change the Go HTTP server default prefix from `/vgi` to `""` to match the Python client's new default. This aligns with what the TypeScript implementation already did in commit `f496cbd`.

In `vgirpc/http.go`, change:
```go
prefix: "/vgi",
```
to:
```go
prefix: "",
```

**Option B**: Update the conformance worker to explicitly set prefix to `""`, and update `test_go_conformance.py` to pass the correct prefix. This is a narrower fix but doesn't address the default mismatch.

## Key Files

- `vgirpc/http.go` — line 102: default prefix, lines 164-170: route registration, lines 206-218: catch-all handler
- `vgirpc/http_pages.go` — lines 347-351: handleNotFound returns HTML
- `test_go_conformance.py` — lines 136-140: HTTP fixture uses base URL without prefix
- `conformance/cmd/vgi-rpc-conformance-go/main.go` — conformance worker HTTP mode setup

## Verification

After fixing, run:
```bash
make test
```

All 426 tests should pass (currently ~106 HTTP tests fail, ~320 pipe/subprocess/unix tests pass).
