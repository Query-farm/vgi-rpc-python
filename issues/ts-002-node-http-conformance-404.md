# TypeScript: node-http conformance transport returns 404

**Repo:** ~/Development/vgi-rpc-typescript
**Severity:** node-http transport conformance tests fail with 404 HTML responses

## Problem

The `node-http` transport variant in the TypeScript conformance tests returns 404 HTML pages instead of Arrow IPC responses. The pipe transport works fine, and the prefix configuration appears correct after commit `f496cbd` (which changed the default prefix from `/vgi` to `""`).

## Symptoms

```
FAILED test_ts_conformance.py::TestUnaryScalarEcho::test_echo_string[node-http]
E  vgi_rpc.rpc._common.RpcError: HttpError: HTTP 404: response is not a valid
   Arrow IPC stream (first 200 bytes: '<!doctype html>...<title>404: Not Found</title>...')
```

The error shows a 404 HTML page, suggesting the request hits the server but the route isn't matched.

## Investigation Notes

The prefix change in `f496cbd` correctly updated:
- `src/http/handler.ts` — default prefix is now `""`
- All conformance HTTP servers (`conformance-http.ts`, `conformance-http-node.ts`, etc.) — removed explicit `prefix: "/vgi"`
- The Python client also defaults to empty prefix

So the prefix alignment appears correct. The issue may be:
1. The Node.js HTTP adapter (`conformance-http-node.ts`) isn't properly translating incoming requests to the fetch handler
2. The bundled build for Node.js may be stale or have a different configuration
3. The Node.js `http.createServer` adapter may not correctly forward the URL path to `createHttpHandler`

## Key Files

- `examples/conformance-http-node.ts` — Node.js HTTP server for conformance
- `test_ts_conformance.py` — lines 186-192: HTTP fixture setup, look at `node-http` variant
- `src/http/handler.ts` — `createHttpHandler()` route matching logic

## Verification

```bash
# Run just the node-http conformance tests
PYTHON=~/Development/vgi-rpc/.venv/bin/python make test-conformance

# Or run all tests
make test
```
