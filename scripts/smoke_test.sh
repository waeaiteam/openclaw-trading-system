#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:18091}"

echo "[1/3] GET ${BASE_URL}/api/status"
curl -fsS "${BASE_URL}/api/status" >/dev/null
echo "OK"

echo "[2/3] GET ${BASE_URL}/api/instances/summary"
curl -fsS "${BASE_URL}/api/instances/summary" >/dev/null
echo "OK"

echo "[3/3] GET ${BASE_URL}/instance/okx/raw"
curl -fsS "${BASE_URL}/instance/okx/raw" >/dev/null
echo "OK"

echo "Smoke test passed."
