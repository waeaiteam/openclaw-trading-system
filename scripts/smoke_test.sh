#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR/src"

echo "[1/3] Python syntax check"
python3 -m py_compile ai_trader_v3.py ai_trader_v3_1.py orderbook_tracker.py liquidation_heatmap_factor.py stock_market_pro_factors.py
echo "OK"

echo "[2/3] One-shot trading decision dry run"
python3 ai_trader_v3_1.py --once >/tmp/okx_trader_smoke.log 2>&1 || {
  echo "Run failed. log:"
  tail -n 80 /tmp/okx_trader_smoke.log || true
  exit 1
}
echo "OK"

echo "[3/3] Check status file"
test -f /root/.okx-paper/ai_trader_status.json
echo "OK"

echo "Trading smoke test passed."
