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
  TAILNET_CPP_SOURCE \
  TAILNET_CSHARP_SOURCE \
  TAILNET_GO_SOURCE \
  TAILNET_JAVA_SOURCE \
  TAILNET_RUST_SOURCE \
  TAILNET_TYPESCRIPT_SOURCE; do
  require_variable "$name"
done

TAILNET_CPP_REVISION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["cpp"])' "$REVISION_MANIFEST")"
TAILNET_CSHARP_REVISION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["csharp"])' "$REVISION_MANIFEST")"
TAILNET_GO_REVISION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["go"])' "$REVISION_MANIFEST")"
TAILNET_JAVA_REVISION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["java"])' "$REVISION_MANIFEST")"
TAILNET_RUST_REVISION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["rust"])' "$REVISION_MANIFEST")"
TAILNET_TYPESCRIPT_REVISION="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["typescript"])' "$REVISION_MANIFEST")"
for revision in "$TAILNET_CPP_REVISION" "$TAILNET_CSHARP_REVISION" "$TAILNET_GO_REVISION" "$TAILNET_JAVA_REVISION" "$TAILNET_RUST_REVISION" "$TAILNET_TYPESCRIPT_REVISION"; do
  if [[ ! "$revision" =~ ^[0-9a-f]{40}$ ]]; then
    echo "cross-language revision manifest contains an unresolved revision: $revision" >&2
    exit 2
  fi
done
CPP_RESOLVED_REVISION="$(git -C "$TAILNET_CPP_SOURCE" rev-parse HEAD)"
CSHARP_RESOLVED_REVISION="$(git -C "$TAILNET_CSHARP_SOURCE" rev-parse HEAD)"
GO_RESOLVED_REVISION="$(git -C "$TAILNET_GO_SOURCE" rev-parse HEAD)"
JAVA_RESOLVED_REVISION="$(git -C "$TAILNET_JAVA_SOURCE" rev-parse HEAD)"
RUST_RESOLVED_REVISION="$(git -C "$TAILNET_RUST_SOURCE" rev-parse HEAD)"
TYPESCRIPT_RESOLVED_REVISION="$(git -C "$TAILNET_TYPESCRIPT_SOURCE" rev-parse HEAD)"
if [[ "$CPP_RESOLVED_REVISION" != "$TAILNET_CPP_REVISION" ]]; then
  echo "C++ source revision $CPP_RESOLVED_REVISION does not match pinned $TAILNET_CPP_REVISION" >&2
  exit 2
fi
if [[ "$CSHARP_RESOLVED_REVISION" != "$TAILNET_CSHARP_REVISION" ]]; then
  echo "C# source revision $CSHARP_RESOLVED_REVISION does not match pinned $TAILNET_CSHARP_REVISION" >&2
  exit 2
fi
if [[ "$GO_RESOLVED_REVISION" != "$TAILNET_GO_REVISION" ]]; then
  echo "Go source revision $GO_RESOLVED_REVISION does not match pinned $TAILNET_GO_REVISION" >&2
  exit 2
fi
if [[ "$JAVA_RESOLVED_REVISION" != "$TAILNET_JAVA_REVISION" ]]; then
  echo "Java source revision $JAVA_RESOLVED_REVISION does not match pinned $TAILNET_JAVA_REVISION" >&2
  exit 2
fi
if [[ "$RUST_RESOLVED_REVISION" != "$TAILNET_RUST_REVISION" ]]; then
  echo "Rust source revision $RUST_RESOLVED_REVISION does not match pinned $TAILNET_RUST_REVISION" >&2
  exit 2
fi
if [[ "$TYPESCRIPT_RESOLVED_REVISION" != "$TAILNET_TYPESCRIPT_REVISION" ]]; then
  echo "TypeScript source revision $TYPESCRIPT_RESOLVED_REVISION does not match pinned $TAILNET_TYPESCRIPT_REVISION" >&2
  exit 2
fi
echo "cross-language revisions: cpp=$CPP_RESOLVED_REVISION csharp=$CSHARP_RESOLVED_REVISION go=$GO_RESOLVED_REVISION java=$JAVA_RESOLVED_REVISION rust=$RUST_RESOLVED_REVISION typescript=$TYPESCRIPT_RESOLVED_REVISION"

export TAILNET_SERVER_HOSTNAME="${TAILNET_SERVER_HOSTNAME:-vgi-interop-server-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_CLIENT_HOSTNAME="${TAILNET_CLIENT_HOSTNAME:-vgi-interop-client-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_SOCKS_HOSTNAME="${TAILNET_SOCKS_HOSTNAME:-vgi-interop-socks-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
CPP_BUILD_PID=""

cleanup() {
  local status="$?"
  if [[ -n "$CPP_BUILD_PID" ]] && kill -0 "$CPP_BUILD_PID" 2>/dev/null; then
    kill "$CPP_BUILD_PID" 2>/dev/null || true
    wait "$CPP_BUILD_PID" 2>/dev/null || true
  fi
  if [[ "$status" -ne 0 ]]; then
    "${COMPOSE[@]}" ps >&2 || true
    "${COMPOSE[@]}" logs --no-color --tail=200 \
      tailscale-server tailscale-client tailscale-socks \
      worker-direct worker-http \
      cpp-worker-direct cpp-worker-http cpp-worker-proxy-v2 \
      csharp-worker-direct csharp-worker-http \
      go-worker-direct go-worker-http go-worker-proxy-v2 \
      java-worker-direct java-worker-http \
      rust-worker-direct rust-worker-http rust-worker-proxy-v2 rust-proxy-v2-relay \
      rust-iroh-server rust-iroh-client \
      typescript-worker-direct typescript-worker-http >&2 || true
  fi
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

docker build --tag "${VGI_TAILNET_CPP_IMAGE:-vgi-rpc-tailnet-cpp:local}" \
  --file "$TAILNET_CPP_SOURCE/tailnet-integration/Dockerfile" "$TAILNET_CPP_SOURCE" &
CPP_BUILD_PID="$!"

docker build --tag "${VGI_TAILNET_IMAGE:-vgi-rpc-tailnet:local}" \
  --file "$TAILNET_ROOT/tests/tailnet/Dockerfile" "$TAILNET_ROOT"
docker build --tag "${VGI_TAILNET_CSHARP_IMAGE:-vgi-rpc-tailnet-csharp:local}" \
  --file "$TAILNET_CSHARP_SOURCE/conformance/tailnet.Dockerfile" "$TAILNET_CSHARP_SOURCE"
docker build --tag "${VGI_TAILNET_GO_IMAGE:-vgi-rpc-tailnet-go:local}" \
  --file "$TAILNET_GO_SOURCE/conformance/tailnet.Dockerfile" "$TAILNET_GO_SOURCE"
docker build --tag "${VGI_TAILNET_JAVA_IMAGE:-vgi-rpc-tailnet-java:local}" \
  --file "$TAILNET_JAVA_SOURCE/tailnet-integration/Dockerfile" "$TAILNET_JAVA_SOURCE"
docker build --tag "${VGI_TAILNET_RUST_IMAGE:-vgi-rpc-tailnet-rust:local}" \
  --file "$TAILNET_RUST_SOURCE/tailnet-integration/Dockerfile" "$TAILNET_RUST_SOURCE"
docker build --tag "${VGI_TAILNET_TYPESCRIPT_IMAGE:-vgi-rpc-tailnet-typescript:local}" \
  --file "$TAILNET_TYPESCRIPT_SOURCE/conformance/tailnet.Dockerfile" "$TAILNET_TYPESCRIPT_SOURCE"
if wait "$CPP_BUILD_PID"; then
  CPP_BUILD_PID=""
else
  CPP_BUILD_PID=""
  echo "C++ Tailnet adapter image build failed" >&2
  exit 1
fi

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

"${COMPOSE[@]}" run --rm cpp-client-direct client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm csharp-client-direct client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  "${common_tcp_expectations[@]}"

SOCKS_CONTAINER_ID="$("${COMPOSE[@]}" ps -q tailscale-socks)"
SOCKS_BRIDGE_IP="$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$SOCKS_CONTAINER_ID")"
if [[ -z "$SOCKS_BRIDGE_IP" ]]; then
  echo "could not resolve the userspace SOCKS container bridge address" >&2
  exit 1
fi
"${COMPOSE[@]}" run --rm cpp-client-socks client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  --proxy "socks5h://$SOCKS_BRIDGE_IP:1055" \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm csharp-client-socks client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  --proxy "socks5h://$SOCKS_BRIDGE_IP:1055" \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm csharp-client-direct client-http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm csharp-client-socks client-http \
  --url "https://$SERVER_DNS" \
  --proxy socks5h://tailscale-socks:1055 \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm cpp-client-direct client-http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm cpp-client-socks client-http \
  --url "https://$SERVER_DNS" \
  --proxy socks5h://tailscale-socks:1055 \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm go-client-direct client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm go-client-socks client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  --proxy "socks5h://$SOCKS_BRIDGE_IP:1055" \
  "${common_tcp_expectations[@]}"

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

"${COMPOSE[@]}" run --rm rust-client-socks client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  --proxy "socks5h://$SOCKS_BRIDGE_IP:1055" \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm rust-client-direct client-http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm java-client-direct client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm java-client-socks client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  --proxy "socks5h://$SOCKS_BRIDGE_IP:1055" \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm java-client-direct client-http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm java-client-socks client-http \
  --url "https://$SERVER_DNS" \
  --proxy socks5h://tailscale-socks:1055 \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm typescript-client-direct client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm typescript-client-socks client-tcp \
  --host "$SERVER_DNS" --port 19400 \
  --proxy "socks5h://$SOCKS_BRIDGE_IP:1055" \
  "${common_tcp_expectations[@]}"

"${COMPOSE[@]}" run --rm typescript-client-direct client-http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  "${common_http_expectations[@]}"

"${COMPOSE[@]}" run --rm typescript-client-socks client-http \
  --url "https://$SERVER_DNS" \
  --proxy "socks5h://tailscale-socks:1055" \
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

wait_for_service_file() {
  local service="$1"
  local path="$2"
  local container_id
  container_id="$("${COMPOSE[@]}" ps -q "$service")"
  if [[ -z "$container_id" ]]; then
    echo "service $service has no running container" >&2
    return 1
  fi
  local _
  for _ in {1..120}; do
    if docker exec "$container_id" test -s "$path"; then
      return 0
    fi
    sleep 0.25
  done
  echo "service $service did not publish $path" >&2
  return 1
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

"${COMPOSE[@]}" up --detach cpp-worker-direct
wait_for_server_port 19400
probe_foreign_tcp
"${COMPOSE[@]}" stop cpp-worker-direct

"${COMPOSE[@]}" up --detach cpp-worker-http
wait_for_server_port 18080
probe_foreign_http
"${COMPOSE[@]}" stop cpp-worker-http

for implementation in typescript java csharp; do
  "${COMPOSE[@]}" up --detach "${implementation}-worker-direct"
  wait_for_server_port 19400
  probe_foreign_tcp
  "${COMPOSE[@]}" stop "${implementation}-worker-direct"
done

"${COMPOSE[@]}" up --detach typescript-worker-http
wait_for_server_port 18080
probe_foreign_http
"${COMPOSE[@]}" stop typescript-worker-http

"${COMPOSE[@]}" up --detach java-worker-http
wait_for_server_port 18080
probe_foreign_http
"${COMPOSE[@]}" stop java-worker-http

"${COMPOSE[@]}" up --detach csharp-worker-http
wait_for_server_port 18080
probe_foreign_http
"${COMPOSE[@]}" stop csharp-worker-http

for implementation in rust go cpp; do
  "${COMPOSE[@]}" up --detach "${implementation}-worker-proxy-v2"
  wait_for_server_port 19401
  "${COMPOSE[@]}" up --detach rust-proxy-v2-relay
  wait_for_server_port 19400
  probe_foreign_tcp
  "${COMPOSE[@]}" stop rust-proxy-v2-relay "${implementation}-worker-proxy-v2"
done

"${COMPOSE[@]}" up --detach rust-iroh-server
wait_for_service_file rust-iroh-server /interop/server.json
"${COMPOSE[@]}" run --rm rust-iroh-client
"${COMPOSE[@]}" stop rust-iroh-server

echo "cross-language real Tailnet integration passed for Go, Rust, TypeScript, Java, C#, and C++"
