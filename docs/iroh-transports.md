# Iroh transport contract

This document is the normative registry for VGI RPC transports carried by
Iroh.  Breaking changes require a new ALPN and a new contract-version fixture.

## Registered endpoints

| URI form | ALPN | Semantics |
| --- | --- | --- |
| `iroh://<endpoint-id>` | `vgi-rpc/arrow-mux/1` | One connection-oriented VGI byte stream per client connection. |
| `httpi://<endpoint-id>[/base-path]` | `iroh-http/2` | HTTP/1.1 request/response semantics, one request per QUIC bidirectional stream. |

`endpoint-id` is exactly the 32-byte Ed25519 public key encoded as 64 lowercase
hexadecimal characters.  User information, ports, query strings, fragments,
uppercase hexadecimal, percent escapes, and Unicode hostnames are invalid.
`iroh://` has no path.  `httpi://` permits an absolute base path; an omitted
path and `/` both mean the empty base path.  Empty path segments, `.` and `..`
segments (including a trailing slash on a non-root path), percent-encoded path
separators, backslashes, and control characters are rejected so every
implementation sends the same request target.

The two ALPNs deliberately have different connection semantics.  Raw
Arrow-mux keeps call and stream state on a connection.  `iroh-http/2` retains
the existing stateless HTTP VGI behavior, including OPTIONS discovery,
continuations, authentication headers, and request/response budgets.

## Identity and configuration

The QUIC handshake authenticates the remote EndpointId.  Relay forwarding does
not weaken or replace this identity.  A client may use a caller-supplied
32-byte secret key or an ephemeral process identity.  Implementations must
never log, serialize into diagnostics, or place the private key in an error.

The standard Iroh relay/discovery preset is the default.  A custom relay set
and relay-disabled mode are mutually exclusive.  A missing optional native
binding produces `unsupported`, never a connector download or direct-network
fallback.

## Errors

Iroh transport failures expose three independent fields:

- `stage`: `parse`, `bind`, `resolve`, `connect`, `alpn`, `open_stream`,
  `write`, `read`, `cancel`, or `close`.
- `category`: `invalid_input`, `unsupported`, `unavailable`, `timeout`,
  `cancelled`, `authentication`, `protocol`, `connection_reset`,
  `resource_exhausted`, or `internal`.
- `dispatch_certainty`: `not_sent`, `unknown`, or `sent`.

`dispatch_certainty` describes whether application request bytes may have
reached the peer.  Parse, bind, resolve, connect, ALPN, and stream-open failures
are `not_sent`; a write failure is `unknown`; failures after the request has
been completely written are `sent`.  Implementations must not automatically
retry an RPC when certainty is `unknown` or `sent` unless the application has
an independent idempotency contract.

The machine-readable normative cases are in
`vgi_rpc/conformance/iroh_transport_vectors.json`.
