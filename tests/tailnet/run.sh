#!/usr/bin/env bash
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

TAILNET_PROFILE="${1:-core}"
if [[ "$TAILNET_PROFILE" != "core" && "$TAILNET_PROFILE" != "full" ]]; then
  echo "usage: $0 [core|full]" >&2
  exit 2
fi

TAILNET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$TAILNET_ROOT/tests/tailnet/compose.yaml"
COMPOSE=(docker compose -f "$COMPOSE_FILE")
if [[ "$TAILNET_PROFILE" == "full" ]]; then
  COMPOSE+=(--profile full)
fi

require_variable() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "required Tailnet test variable is unset: $name" >&2
    exit 2
  fi
}

for name in \
  TS_OAUTH_CLIENT_ID \
  TS_OAUTH_SECRET \
  TAILNET_ISSUER \
  TAILNET_EXPECTED_CAPABILITY \
  TAILNET_EXPECTED_CLIENT_TAG; do
  require_variable "$name"
done
if [[ "$TAILNET_PROFILE" == "full" ]]; then
  for name in TAILNET_USER_AUTHKEY TAILNET_SERVICE_NAME TAILNET_SERVICE_HOST; do
    require_variable "$name"
  done
fi

export TAILNET_SERVER_HOSTNAME="${TAILNET_SERVER_HOSTNAME:-vgi-ci-server-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_CLIENT_HOSTNAME="${TAILNET_CLIENT_HOSTNAME:-vgi-ci-client-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_SOCKS_HOSTNAME="${TAILNET_SOCKS_HOSTNAME:-vgi-ci-socks-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"
export TAILNET_USER_HOSTNAME="${TAILNET_USER_HOSTNAME:-vgi-ci-user-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}}"

cleanup() {
  local status="$?"
  if [[ "$status" -ne 0 ]]; then
    "${COMPOSE[@]}" ps >&2 || true
    "${COMPOSE[@]}" logs --no-color --tail=200 \
      tailscale-server tailscale-client tailscale-socks tailscale-user \
      worker-direct worker-http worker-service >&2 || true
  fi
  "${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT

docker build --tag "${VGI_TAILNET_IMAGE:-vgi-rpc-tailnet:local}" --file "$TAILNET_ROOT/tests/tailnet/Dockerfile" "$TAILNET_ROOT"

core_services=(tailscale-server tailscale-client tailscale-socks worker-direct worker-http)
"${COMPOSE[@]}" up --detach --wait --wait-timeout 120 "${core_services[@]}"
if [[ "$TAILNET_PROFILE" == "full" ]]; then
  "${COMPOSE[@]}" up --detach --wait --wait-timeout 120 tailscale-user worker-service
fi

SERVER_DNS="$("${COMPOSE[@]}" exec -T tailscale-server \
  tailscale --socket=/var/run/tailscale/tailscaled.sock status --json | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["Self"]["DNSName"].rstrip("."))')"
if [[ -z "$SERVER_DNS" ]]; then
  echo "Tailscale did not report a server MagicDNS name" >&2
  exit 1
fi

"${COMPOSE[@]}" exec -T tailscale-server tailscale --socket=/var/run/tailscale/tailscaled.sock serve \
  --yes \
  --bg \
  --accept-app-caps="$TAILNET_EXPECTED_CAPABILITY" \
  --https=443 \
  http://127.0.0.1:18080

# `tailscale serve` returns before its first ACME certificate is necessarily
# available. Force the certificate fetch to complete so the HTTPS assertion
# tests Serve rather than racing certificate issuance.
"${COMPOSE[@]}" exec -T tailscale-server tailscale --socket=/var/run/tailscale/tailscaled.sock cert \
  --cert-file=/tmp/vgi-tailnet-cert.pem \
  --key-file=/tmp/vgi-tailnet-key.pem \
  "$SERVER_DNS"

"${COMPOSE[@]}" run --rm probe-direct python -m tests.tailnet.probe tcp \
  --host "$SERVER_DNS" \
  --port 19400 \
  --expected-evidence-source localapi \
  --expected-assurance local_daemon \
  --expected-subject-kind tagged_node \
  --expected-subject-stability stable \
  --expected-capability "$TAILNET_EXPECTED_CAPABILITY" \
  --expected-tag "$TAILNET_EXPECTED_CLIENT_TAG" \
  --expected-target-kind destination_ip \
  --expect-authenticated

"${COMPOSE[@]}" run --rm probe-socks python -m tests.tailnet.probe tcp \
  --host "$SERVER_DNS" \
  --port 19400 \
  --proxy socks5h://tailscale-socks:1055 \
  --require-local-dns-failure \
  --expected-evidence-source localapi \
  --expected-assurance local_daemon \
  --expected-subject-kind tagged_node \
  --expected-subject-stability stable \
  --expected-capability "$TAILNET_EXPECTED_CAPABILITY" \
  --expected-tag "$TAILNET_EXPECTED_CLIENT_TAG" \
  --expected-target-kind destination_ip \
  --expect-authenticated

"${COMPOSE[@]}" run --rm probe-direct python -m tests.tailnet.probe http \
  --url "https://$SERVER_DNS" \
  --spoof-login attacker@example.invalid \
  --expected-evidence-source serve_proxy \
  --expected-assurance configured_proxy \
  --expected-subject-kind unknown \
  --expected-subject-stability none \
  --expected-capability "$TAILNET_EXPECTED_CAPABILITY" \
  --expect-proxy

if [[ "$TAILNET_PROFILE" == "full" ]]; then
  "${COMPOSE[@]}" run --rm probe-user python -m tests.tailnet.probe tcp \
    --host "$SERVER_DNS" \
    --port 19400 \
    --expected-evidence-source localapi \
    --expected-assurance local_daemon \
    --expected-subject-kind user \
    --expected-subject-stability stable \
    --expected-capability "$TAILNET_EXPECTED_CAPABILITY" \
    --expected-target-kind destination_ip \
    --expect-authenticated

  "${COMPOSE[@]}" run --rm probe-user python -m tests.tailnet.probe http \
    --url "https://$SERVER_DNS" \
    --spoof-login attacker@example.invalid \
    --expected-evidence-source serve_proxy \
    --expected-assurance configured_proxy \
    --expected-subject-kind user \
    --expected-subject-stability login \
    --expected-capability "$TAILNET_EXPECTED_CAPABILITY" \
    --expect-proxy

  "${COMPOSE[@]}" exec -T tailscale-server tailscale --socket=/var/run/tailscale/tailscaled.sock serve \
    --yes \
    --bg \
    --service="$TAILNET_SERVICE_NAME" \
    --proxy-protocol=2 \
    --tcp=19400 \
    tcp://127.0.0.1:19401

  "${COMPOSE[@]}" run --rm probe-direct python -m tests.tailnet.probe tcp \
    --host "$TAILNET_SERVICE_HOST" \
    --port 19400 \
    --expected-evidence-source localapi \
    --expected-assurance local_daemon \
    --expected-subject-kind tagged_node \
    --expected-subject-stability stable \
    --expected-capability "$TAILNET_EXPECTED_CAPABILITY" \
    --expected-tag "$TAILNET_EXPECTED_CLIENT_TAG" \
    --expected-target-kind service \
    --expected-target-value "$TAILNET_SERVICE_NAME" \
    --expect-authenticated
fi

echo "real Tailnet integration profile '$TAILNET_PROFILE' passed"
