# Go: No HTTP client implementation

**Repo:** ~/Development/vgi-rpc-go
**Priority:** Feature gap — Python and TypeScript both have HTTP clients

## Problem

The Go implementation has an HTTP server (`HttpServer` implementing `http.Handler`) but no HTTP client. Both Python and TypeScript provide HTTP client functions for connecting to vgi-rpc HTTP servers.

## What Exists in Other Implementations

### Python (`vgi_rpc/http/_client.py`)
- `http_connect(protocol, url, ...)` — returns a typed RPC proxy over HTTP
- Uses `httpx` for HTTP requests
- Handles stateful streaming via state tokens (init/exchange endpoints)
- Supports authentication, timeouts, connection pooling
- `http_introspect(protocol, url)` — introspection over HTTP

### TypeScript (`src/client/connect.ts`)
- `httpConnect(baseUrl, options?)` — returns an `RpcClient`
- Uses `fetch` API
- Handles streaming via init/exchange endpoints with state tokens
- Supports zstd compression, authorization headers

## Scope

This is a significant feature. An HTTP client for Go would need:

1. Send Arrow IPC over HTTP POST to the correct endpoints
2. Handle unary calls: `POST {prefix}/{method}`
3. Handle stream init: `POST {prefix}/{method}/init`
4. Handle stream exchange: `POST {prefix}/{method}/exchange` with state token round-tripping
5. Parse Arrow IPC responses
6. Support authentication (bearer tokens, custom auth)
7. Support optional zstd compression

## Note

This may be intentionally omitted — Go servers are typically consumed by Python or TypeScript clients, not other Go processes. Mark as `n/a` if there's no use case for Go-to-Go HTTP RPC.

## Python Reference

- `~/Development/vgi-rpc/vgi_rpc/http/_client.py` — full HTTP client (~200 lines)

## TypeScript Reference

- `~/Development/vgi-rpc-typescript/src/client/connect.ts` — HTTP client implementation
