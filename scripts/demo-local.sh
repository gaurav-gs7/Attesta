#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export ATTESTA_API_TOKEN="${ATTESTA_API_TOKEN:-local-dev-token}"
export ATTESTA_ADDR="${ATTESTA_ADDR:-127.0.0.1:8080}"
export ATTESTA_API_URL="${ATTESTA_API_URL:-http://127.0.0.1:8080}"
export GOCACHE="${GOCACHE:-$ROOT_DIR/.gocache}"
export GOMODCACHE="${GOMODCACHE:-$ROOT_DIR/.gomodcache}"

go run ./cmd/attesta-api &
api_pid=$!
trap 'kill "$api_pid" 2>/dev/null || true' EXIT

for _ in $(seq 1 30); do
  if curl -fsS "$ATTESTA_API_URL/readyz" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

go run ./cmd/attesta service init payments-api \
  --language go \
  --team platform \
  --owner payments-platform \
  --tier critical \
  --pager payments-oncall \
  --env staging \
  --slo-availability 99.9 \
  --slo-latency-p95 300ms \
  --deployment canary

go run ./cmd/attesta check payments-api
go run ./cmd/attesta slo status payments-api
echo "The API reads RolloutGuards only with ATTESTA_CLUSTER_READ=true; without it this says so"
go run ./cmd/attesta guards payments-api || true
echo "SIMULATED: the next call opens an incident from a hand-written alert; its error rate and latency are stored as sender-reported values"
go run ./cmd/attesta incident create --service payments-api --alert HighErrorRate --error-rate 8.2 --latency-p95-ms 850
go run ./cmd/attesta incident list
go run ./cmd/attesta workflows list

curl -fsS "$ATTESTA_API_URL/metrics" | sed -n '1,20p'
