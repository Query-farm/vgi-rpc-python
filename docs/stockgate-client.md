# Stockgate worker client

Install the CLI and native Iroh extras:

```console
pip install 'vgi-rpc[cli,iroh]'
```

## Register a worker

The normal registration path is a single command:

```console
qf service register o/example/weather --relay-url https://relay.example
```

`qf` loads or creates the worker's Iroh key, opens Stockgate's device-login
page, creates or finds the service, obtains an endpoint-bound 10-minute grant,
and immediately completes proof-of-possession registration. It never prints or
persists that one-time grant. The cached human credential, worker key, and
rotating endpoint credential are stored in private files under
`~/.config/qf/` by default. Use `--help` to select explicit key and state paths,
advertise repeatable direct addresses, target staging, or suppress automatic
browser opening.

For an intentionally process-lifetime identity, run the worker under the
supervisor instead:

```console
qf service run o/example/weather \
  --raw-upstream tcp://127.0.0.1:9400 \
  -- python weather_worker.py --listen 127.0.0.1:9400
```

This form generates the key in memory and transfers it to
`vgi-iroh-bridge` once through inherited standard input. It never writes the
Iroh key or endpoint credential to disk. Registration is always ephemeral;
`qf` heartbeats while the child runs and deletes the endpoint during normal or
interrupted shutdown. The cached human device credential remains persistent
unless an explicit credential path points at temporary storage.

The Python API below is the lower-level worker lifecycle interface.

The Stockgate client uses the worker's persistent Iroh secret key for all three
identity operations: the EndpointId, the signed Pkarr address packet, and the
registration proof of possession. Keep the key and endpoint credential in
durable secret storage. A replacement credential returned by a heartbeat must
be persisted before the next heartbeat.

```python
from vgi_rpc import (
    StockgateAddress,
    StockgateClient,
    StockgateRegistrationIntent,
    iroh_connect,
    load_or_create_iroh_secret_key,
)

stockgate = StockgateClient("https://stockgate.query-farm.services")
secret_key = load_or_create_iroh_secret_key("/var/lib/my-worker/iroh.key")

# A lower-level caller first creates/finds the service and requests a short-lived
# grant bound to this key's EndpointId. Never put it in logs or command lines.
endpoint = stockgate.register(
    registration_credential,
    secret_key,
    StockgateAddress(relay_url="https://relay.example"),
    StockgateRegistrationIntent(
        org="o/example",
        service="weather",
        label="production worker",
    ),
)

heartbeat = stockgate.heartbeat(endpoint)
endpoint = heartbeat.endpoint  # persist this: it may contain a rotated credential
```

Send heartbeats at `heartbeat.next_heartbeat_s`. When the bridge's advertised
addresses change, either include a freshly signed packet in the next heartbeat
or update it independently:

```python
heartbeat = stockgate.heartbeat(
    endpoint,
    secret_key=secret_key,
    address=StockgateAddress(direct_addresses=("203.0.113.20:443",)),
)
endpoint = heartbeat.endpoint

# Equivalent independent update:
endpoint = stockgate.update_address(
    endpoint,
    secret_key,
    StockgateAddress(relay_url="https://new-relay.example"),
)
```

Resolution verifies both the detached Stockgate resolver signature and the
endpoint's Pkarr signature before exposing connection hints:

```python
resolved = stockgate.resolve("example", "weather", caller_credential)
with iroh_connect(
    WeatherService,
    f"iroh://{resolved.endpoint_hex}",
    direct_addresses=resolved.address.direct_addresses,
    remote_relay_url=resolved.address.relay_url,
) as worker:
    forecast = worker.forecast(postcode="10001")
```

`resolved.endpoint_id` is Stockgate's z-base-32 representation;
`resolved.endpoint_hex` is the same public key formatted for an `iroh://` URI.

Call `delete_endpoint(endpoint)` during intentional decommissioning. Crashes
are handled by Stockgate liveness expiry, but explicit deletion removes the
endpoint from resolution immediately.

## Low-level staging end-to-end smoke test

The repository also retains a protocol-level harness for testing a real TCP
worker and Iroh bridge. Its service must already exist, and its grant must be
bound to the temporary key used by the harness. Reading that grant silently
keeps it out of shell history and process arguments:

```console
read -s STOCKGATE_REGISTRATION_TOKEN
export STOCKGATE_REGISTRATION_TOKEN
uv sync --extra iroh --extra conformance
uv run python scripts/stockgate_worker_e2e.py \
  --org example \
  --service vgi-e2e-001 \
  --bridge ../vgi-rpc-rust/target/release/vgi-iroh-bridge
unset STOCKGATE_REGISTRATION_TOKEN
```

The harness verifies registration, heartbeat, resolver JWS, endpoint Pkarr,
one typed VGI call, and endpoint deletion. It uses a direct loopback Iroh path,
so run the bridge on the same machine. The organization service row remains
after endpoint cleanup; use a disposable service name and remove it through
the staging console afterward.
