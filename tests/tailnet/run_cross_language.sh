#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

TAILNET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASE_COMPOSE="$TAILNET_ROOT/tests/tailnet/compose.yaml"
INTEROP_COMPOSE="$TAILNET_ROOT/tests/tailnet/cross-language.compose.yaml"
REVISION_MANIFEST="$TAILNET_ROOT/tests/tailnet/revisions.json"
COMPOSE=(docker compose -f "$BASE_COMPOSE" -f "$INTEROP_COMPOSE" --profile interop)

require_variable() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "required cross-language Tailnet variable is unset: $name" >&2
    exit 2
  fi
}

for name in \
  TS_OAUTH_CLIENT_ID \
  TS_OAUTH_SECRET \
  TAILNET_ISSUER \
  TAILNET_EXPECTED_CAPABILITY \
  TAILNET_EXPECTED_CLIENT_TAG \
  TAILNET_GO_SOURCE \
  TAILNET_RUST_SOURCE; do
  require_variable "$name"
done

TAILNET_GO_REVISION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["go"])' "$REVISION_MANIFEST")"
TAILNET_RUST_REVISION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["rust"])' "$REVISION_MANIFEST")"
for revision in "$TAILNET_GO_REVISION" "$TAILNET_RUST_REVISION"; do
  if [[ ! "$revision" =~ ^[0-9a-f]{40}$ ]]; then
    echo "cross-language revision manifest contains an unresolved revision: $revision" >&2
    exit 2
  fi
done
GO_RESOLVED_REVISION="$(git -C "$TAILNET_GO_SOURCE" rev-parse HEAD)"
RUST_RESOLVED_REVISION="$(git -C "$TAILNET_RUST_SOURCE" rev-parse HEAD)"
if [[ "$GO_RESOLVED_REVISION" != "$TAILNET_GO_REVISION" ]]; then
  echo "Go source revision $GO_RESOLVED_REVISION does not match pinned $TAILNET_GO_REVISION" >&2
  exit 2
fi
if [[ "$RUST_RESOLVED_REVISION" != "$TAILNET_RUST_REVISION" ]]; then
  echo "Rust source revision $RUST_RESOLVED_REVISION does not match pinned $TAILNET_RUST_REVISION" >&2
  exit 2
fi
echo "cross-language revisions: go=$GO_RESOLVED_REVISION rust=$RUST_RESOLVED_REVISION"

export TAILNET_SERVER_HOSTNAME="${TAILNET_SERVER_HOSTNAME:-vgi-interop-server-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_CLIENT_HOSTNAME="${TAILNET_CLIENT_HOSTNAME:-vgi-interop-client-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_SOCKS_HOSTNAME="${TAILNET_SOCKS_HOSTNAME:-vgi-interop-socks-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"

cleanup() {
  local status="$?"
  if [[ "$status" -ne 0 ]]; then
    "${COMPOSE[@]}" ps >&2 || true
    "${COMPOSE[@]}" logs --no-color --tail=200 \
      tailscale-server tailscale-client tailscale-socks \
      worker-direct worker-http \
      go-worker-direct go-worker-http rust-worker-direct rust-worker-http >&2 || true
  fi
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

docker build --tag "${VGI_TAILNET_IMAGE:-vgi-rpc-tailnet:local}" \
  --file "$TAILNET_ROOT/tests/tailnet/Dockerfile" "$TAILNET_ROOT"
docker build --tag "${VGI_TAILNET_GO_IMAGE:-vgi-rpc-tailnet-go:local}" \
  --file "$TAILNET_GO_SOURCE/conformance/tailnet.Dockerfile" "$TAILNET_GO_SOURCE"
docker build --tag "${VGI_TAILNET_RUST_IMAGE:-vgi-rpc-tailnet-rust:local}" \
  --file "$TAILNET_RUST_SOURCE/tailnet-integration/Dockerfile" "$TAILNET_RUST_SOURCE"

"${COMPOSE[@]}" up --detach --wait --wait-timeout 120 \
  tailscale-server tailscale-client tailscale-socks worker-direct worker-http

SERVER_DNS="$("${COMPOSE[@]}" exec -T tailscale-server \
  tailscale --socket=/var/run/tailscale/tailscaled.sock status --json | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["Self"]["DNSName"].rstrip("."))')"
if [[ -z "$SERVER_DNS" ]]; then
  echo "Tailscale did not report a server MagicDNS name" >&2
  exit 1
fi

"${COMPOSE[@]}" exec -T tailscale-server tailscale --socket=/var/run/tailscale/tailscaled.sock serve \
  --yes --bg --accept-app-caps="$TAILNET_EXPECTED_CAPABILITY" \
  --https=443 http://127.0.0.1:18080
"${COMPOSE[@]}" exec -T tailscale-server tailscale --socket=/var/run/tailscale/tailscaled.sock cert \
  --cert-file=/tmp/vgi-tailnet-cert.pem \
  --key-file=/tmp/vgi-tailnet-key.pem \
  "$SERVER_DNS"

common_http_expectations=(
  --expected-evidence-source serve_proxy
  --expected-assurance configured_proxy
  --expected-issuer "$TAILNET_ISSUER"
  --expected-subject-kind unknown
  --expected-subject-stability none
  --expected-capability "$TAILNET_EXPECTED_CAPABILITY"
  --expect-proxy
)
common_tcp_expectations=(
  --expected-evidence-source localapi
  --expected-assurance local_daemon
  --expected-issuer "$TAILNET_ISSUER"
  --expected-subject-kind tagged_node
  --expected-subject-stability stable
  --expected-capability "$TAILNET_EXPECTED_CAPABILITY"
  --expected-tag "$TAILNET_EXPECTED_CLIENT_TAG"
  --expected-target-kind destination_ip
  --expect-authenticated
)

"${COMPOSE[@]}" run --rm go-client-direct client-http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm go-client-socks client-http \
  --url "https://$SERVER_DNS" \
  --proxy socks5h://tailscale-socks:1055 \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm rust-client-direct client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  "${common_tcp_expectations[@]}"

SOCKS_CONTAINER_ID="$("${COMPOSE[@]}" ps -q tailscale-socks)"
SOCKS_BRIDGE_IP="$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SOCKS_CONTAINER_ID")"
if [[ -z "$SOCKS_BRIDGE_IP" ]]; then
  echo "could not resolve the userspace SOCKS container bridge address" >&2
  exit 1
fi
"${COMPOSE[@]}" run --rm rust-client-socks client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  --proxy "socks5h://$SOCKS_BRIDGE_IP:1055" \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm rust-client-direct client-http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

wait_for_server_port() {
  local port="$1"
  "${COMPOSE[@]}" run --rm server-tool python -c \
    "import socket,time; deadline=time.monotonic()+30; last=None
while time.monotonic()<deadline:
 try:
  socket.create_connection(('127.0.0.1',$port),1).close(); raise SystemExit(0)
 except OSError as exc:
  last=exc; time.sleep(.25)
raise SystemExit(f'port $port did not become ready: {last}')"
}

probe_foreign_tcp() {
  "${COMPOSE[@]}" run --rm probe-direct python -m tests.tailnet.interop_probe tcp \
    --host "$SERVER_DNS" --port 19400
}

probe_foreign_http() {
  "${COMPOSE[@]}" run --rm probe-direct python -m tests.tailnet.interop_probe http \
    --url "https://$SERVER_DNS" \
    --spoof-login attacker@example.invalid
}

"${COMPOSE[@]}" stop worker-direct worker-http

for implementation in go rust; do
  "${COMPOSE[@]}" up --detach "${implementation}-worker-direct"
  wait_for_server_port 19400
  probe_foreign_tcp
  "${COMPOSE[@]}" stop "${implementation}-worker-direct"

  "${COMPOSE[@]}" up --detach "${implementation}-worker-http"
  wait_for_server_port 18080
  probe_foreign_http
  "${COMPOSE[@]}" stop "${implementation}-worker-http"
done

echo "cross-language real Tailnet integration passed for Go and Rust"
