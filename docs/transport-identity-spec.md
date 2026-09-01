# Transport Identity Evidence Contract

Status: experimental, normative for active VGI RPC SDKs.

Transport identity is an off-wire input to worker authentication. It does not
change Arrow/VGI framing. A transport adapter resolves evidence once per HTTP
request or once per stateful connection and supplies the immutable snapshot to
the worker's `CallContext`.

## Resolution context

Providers receive a transport-neutral snapshot with:

- `transport`: `http`, `tcp`, `iroh`, or another registered transport name.
- `immediate_peer`: the normalized directly connected peer IP used for exact
  proxy trust decisions.
- `source_endpoint`: the directly connected source socket endpoint (`IP:port`)
  when the transport exposes it; LocalAPI WhoIs requires this full value.
- `asserted_peer`: a proxy-asserted source, only after proxy trust succeeds.
- `destination_address`: the actual local destination address when known.
- `authority`: the HTTP authority/Host. It is routing input, not a trusted
  destination address.
- `service_name`: an operator-configured logical service identity.
- multi-valued `headers`, structured `metadata`, cancellation, and a remaining
  budget measured from a monotonic clock. Wall-clock deadline timestamps may
  be exposed for diagnostics but are not enforcement inputs.

Header names are case-insensitive. Duplicate identity headers, case-varied
duplicates, control characters, and over-limit provider inputs fail closed.
Network providers must bound every connect/read operation by cancellation and
the supplied remaining budget.

The Python WSGI profile cannot recover physical header-line multiplicity after
the WSGI server has normalized the request. Its trusted adjacent proxy/server
MUST therefore replace each identity header and reject duplicate input before
WSGI. Provider grammars still reject ambiguous coalesced certificate, JSON,
XFCC, and verification values. A server adapter that exposes raw header lines
is required to claim end-to-end duplicate-line detection at the VGI boundary.

## Provider result

Each configured provider emits exactly one result with a unique provider name
and one of:

`off`, `not_applicable`, `available`, `unavailable`, `permission_denied`,
`no_match`, `invalid`, or `untrusted_proxy`.

Only `available` may carry identities, and it must carry at least one.
`unavailable` and `permission_denied` are transient authority failures;
`invalid` and `untrusted_proxy` are authentication rejections.

An identity contains:

- provider, evidence source, assurance, issuer, and transport;
- subject kind/key/stability/verification;
- structured JSON attributes and opaque structured JSON capabilities;
- capability verification and optional source/proxy addresses.

The stable principal is namespaced as:

`peer/<percent(provider)>/<percent(issuer)>/<percent(subject_key)>`

Tags, display names, login names, HTTP authority, and IP addresses are
attributes, never stable principals. A primary authenticator requires exactly
one verified subject with `stable` stability. Capability-only or login-only
evidence is observable but cannot become a primary principal.

## Authentication composition

- `observe`: expose evidence and preserve application authentication.
- `require`: require usable evidence and preserve application authentication.
- `primary`: authenticate as one provider's unique stable verified subject.
- `any_of`: accept valid application auth or one usable provider. Invalid,
  untrusted, or ambiguous available evidence never downgrades to another
  mechanism. An unavailable alternative does not defeat an already valid
  factor. When application authentication wins, peer evidence is observation
  only: applications MUST NOT authorize from it unless a `require`, `all_of`,
  or custom policy also places its binding in `AuthContext`.
- `all_of`: require valid application auth and every configured provider. The
  application must supply an identity linker that rejects conflicting
  identities.

A presented invalid application credential never falls back to peer identity.
For `require`, an `available` provider result with valid capability-only or
otherwise subjectless evidence is usable. Only `primary`, `any_of` when the
provider is the authenticating factor, and `all_of` require a unique stable
verified subject.
Observation cannot consume a required authenticator's missing-credential
failure.

## Stateful binding

Every cursor, call-state token, sticky-session token, registry/cache key, and
resumable stream identity must bind the complete authorization evidence. The
binding includes provider status and every authorization-relevant identity
field. `all_of` additionally includes the application authentication domain and
principal. This prevents two users sharing one transport peer from replaying
each other's state.

`source_address` and `proxy_address` are routing/audit topology, not built-in
authorization evidence, and are excluded from the built-in digest. This keeps
state valid across new source ports and members of an explicitly trusted proxy
fleet. An application that intentionally authorizes topology must normalize it
and add an explicit claim/binding in a custom policy.

Capabilities are validated opaque JSON. Each attributes, capabilities, or
provider-metadata object is limited to 65,536 UTF-8 bytes in canonical compact
JSON, 16 container levels (the root object is level 1), and 4,096 aggregate
JSON values including containers and scalars. Implementations reject duplicate
JSON keys and may impose smaller transport-specific limits. They must not base
authorization on cross-language JSON byte equality. Golden vectors cover the
portable scalar/object subset used by the shared digest test.

## Adapter boundaries

- Tailscale: LocalAPI WhoIs, trusted Serve headers, or trusted PROXY v2 source.
- SPIFFE: directly verified SVIDs or explicitly trusted proxy verification.
- Iroh: cryptographic endpoint/node identity captured for the connection.
- Envoy/nginx/cloud L7: headers only from an explicitly trusted immediate
  proxy, with direct backend access prevented.
- Cloud L4: socket/PROXY evidence plus provider APIs only where the platform
  actually supplies a verifiable workload/client identity.

Adapters provide evidence. Worker/application policy alone decides
authorization.

### Iroh bridge forwarding

A bridge forwarding an authenticated Iroh connection to a raw VGI TCP worker
uses PROXY protocol v2 with command `PROXY`, family/protocol `UNSPEC` (`0x00`),
and exactly one VGI Iroh TLV (`0xE0`). The TLV payload is version byte `1`
followed by the 32-byte EndpointId. The stable subject encoding is the
lowercase hexadecimal encoding of those bytes. No issuer, capability, address,
or credential is accepted from this TLV.

`PROXY/UNSPEC` remains invalid on ordinary listeners. A worker accepts it only
when Iroh proxy identity is explicitly enabled and the immediate sender is in
the existing exact trusted-proxy boundary. It obtains the issuer locally,
marks delivery assurance as `configured_proxy`, and records that the bridge
cryptographically verified the original Iroh peer. Missing, duplicate,
wrong-version, wrong-sized, or IP-family Iroh TLVs fail closed.

For HTTP forwarding, the bridge removes any client-supplied
`VGI-Forwarded-Iroh-Endpoint` field and sets exactly one lowercase 64-digit
hexadecimal EndpointId. The worker accepts it only from the same explicit
trusted-proxy boundary, obtains the issuer locally, and otherwise applies the
same identity and assurance rules as the raw TLV.
