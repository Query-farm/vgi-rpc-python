# Real Tailnet integration testing

The real-Tailnet suite is an opt-in release gate. It creates ephemeral Tailscale
nodes and exercises VGI through Tailscale's data plane and identity surfaces.
It does not replace the fast adversarial fixtures in the ordinary CI workflow.

## What the matrix proves

| Profile | Path | Client identity | Evidence and assertions |
| --- | --- | --- | --- |
| `core` | MagicDNS to raw TCP | tagged node | LocalAPI WhoIs, stable node subject, tag, destination-IP capability, authenticated RPC |
| `core` | `socks5h` to raw TCP | tagged userspace node | proxy-side MagicDNS, LocalAPI WhoIs, no direct fallback, same evidence contract |
| `core` | HTTPS through Tailscale Serve | tagged node | capability-only Serve evidence, subjectless request, configured-proxy assurance |
| `core` | HTTPS through Tailscale Serve | tagged node | a forged client `Tailscale-User-Login` is stripped instead of becoming identity |
| `full` | MagicDNS to raw TCP | untagged test user | LocalAPI stable numeric user subject and primary authentication |
| `full` | HTTPS through Tailscale Serve | untagged test user | verified login-scoped Serve subject and application capability |
| `full` | Tailscale Service to PROXY v2 raw TCP | tagged node | exact proxy trust, asserted source, service-scoped WhoIs capability, stable primary authentication |

The worker returns only a redacted view of its `CallContext`: principal values
are SHA-256 fingerprints, capability values are omitted, user profile fields and
addresses are omitted, and only expected tag and capability names are exposed.
The workflow does not print auth keys or Tailscale container environments.

## Tailnet setup

Use a dedicated integration Tailnet or an equally isolated policy scope. Start
from the
[`tests/tailnet/policy.hujson`](https://github.com/Query-farm/vgi-rpc-python/blob/main/tests/tailnet/policy.hujson)
template, replace the test user, and merge the fragment into the Tailnet policy.

Create a GitHub Environment named `tailnet-integration`. Environment protection
rules are recommended because the workflow joins a real Tailnet. Configure:

| Kind | Name | Required by | Value |
| --- | --- | --- | --- |
| Secret | `TS_SERVER_OAUTH_CLIENT_ID` | all | OAuth client ID with writable `auth_keys` scope, restricted to `tag:vgi-ci-server` |
| Secret | `TS_SERVER_OAUTH_SECRET` | all | OAuth client secret restricted to `tag:vgi-ci-server` |
| Secret | `TS_OAUTH_CLIENT_ID` | all | OAuth client ID with writable `auth_keys` scope, restricted to `tag:vgi-ci-client` |
| Secret | `TS_OAUTH_SECRET` | all | OAuth client secret restricted to `tag:vgi-ci-client` |
| Secret | `TAILNET_USER_AUTHKEY` | `full` | reusable, ephemeral, pre-approved key owned by a dedicated untagged test user |
| Variable | `TAILNET_ISSUER` | all | stable namespace such as `tailnet:vgi-integration` |
| Variable | `TAILNET_EXPECTED_CAPABILITY` | all | `query.farm/cap/vgi-test` for the supplied policy |
| Variable | `TAILNET_EXPECTED_CLIENT_TAG` | all | `tag:vgi-ci-client` |
| Variable | `TAILNET_SERVICE_NAME` | `full` | `svc:vgi-ci` for the supplied policy |
| Variable | `TAILNET_SERVICE_HOST` | `full` | the Service's MagicDNS name, without a scheme or port |

For `full`, define the Service in the Tailscale admin console with TCP port
`19400` and approve the ephemeral server as a host if approval is not automated.
The harness configures that host with `tailscale serve`, raw TCP forwarding, and
PROXY protocol v2. The VGI backend binds only to loopback and trusts only
loopback as the immediate proxy peer.

Run the workflow manually with `core` first. `full` intentionally fails if the
test user, Service, grants, capability, or host approval is missing; it does not
downgrade to a less meaningful test. Enable a scheduled `core` run only after
the GitHub Environment and credentials are installed, so a missing integration
environment cannot create a misleading release signal.

## Running from a controlled Linux host

Docker, Docker Compose v2, and `/dev/net/tun` are required. Export the same
secrets and variables, then run:

```console
tests/tailnet/run.sh core
tests/tailnet/run.sh full
```

Every node receives a run-specific hostname. The exit trap removes containers,
state volumes, sockets, and the ephemeral node state even after a failed probe.
Tagged nodes enroll as ephemeral, preauthorized resources through the scoped
OAuth credential and advertise only their configured server or client tag.

## Cross-language rollout

The Python worker is the reference oracle because the identity contract and
golden fixtures live in this repository. Each active client SDK should run the
same cases against this topology, using its native direct-TCP and SOCKS5h
dialers. Each server SDK should then replace the Python worker for the paths it
implements. A port is not marked real-Tailnet complete until both directions
pass.

| SDK | Native client against reference worker | Native server as identity oracle |
| --- | --- | --- |
| Python | implemented by this workflow | implemented by this workflow |
| Go | next gate | next gate |
| Rust | next gate | next gate |
| TypeScript | after raw-socket identity adapter | after raw-socket identity adapter |
| Java/Kotlin | next gate; HTTP SOCKS remains separately tracked | next gate |
| C# | next gate | next gate |
| C++ | next gate | next gate |

Windows named-pipe LocalAPI discovery and both macOS Tailscale variants require
native runners and are separate from this Linux container topology. Likewise,
policy revocation and multi-host Service drain require a control-plane mutation
credential and two approved service hosts. Those belong in a protected
destructive/HA workflow; the current workflow makes no claim that they pass.
