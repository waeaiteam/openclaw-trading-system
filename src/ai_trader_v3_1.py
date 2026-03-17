#!/usr/bin/env python3
"""
AI Trader v3.2 extension layer on top of ai_trader_v3.py
- Adds kline pattern + technical kline factors
- Adds strategy historical drawdown snapshot
- v3.1.4: Adds Stock Market Pro integration (cross-market, macro risk, enhanced technical)
- v3.2.0: Fixes critical bugs (cooldown, peakEquity, RSI, liquidity quality gate), adds 5-layer evolution
"""

import argparse
from datetime import datetime, timezone
import math
import os
import time
from statistics import mean
from typing import Any, Dict, List

from ai_trader_v3 import (
    CFG,
    ACCOUNT_FILE,
    HISTORY_FILE,
    STATE_FILE,
    DataFeed,
    Executor,
    FactorEngine,
    RiskEngine,
    TraderV3,
    clamp,
    iso_now,
    read_json,
    sfloat,
    utc_now,
    write_json,
)

# Orderbook tracker for spoofing detection
try:
    from orderbook_tracker import get_tracker, OrderTracker
    ORDERBOOK_TRACKER_AVAILABLE = True
except ImportError:
    ORDERBOOK_TRACKER_AVAILABLE = False

# Stock Market Pro integration
try:
    from stock_market_pro_factors import (
        compute_cross_market_factor,
        compute_macro_risk_factor,
        compute_enhanced_technical_factor,
        clear_cache as smp_clear_cache
    )
    STOCK_MARKET_PRO_AVAILABLE = True
except ImportError:
    STOCK_MARKET_PRO_AVAILABLE = False

# Liquidation heatmap factor
try:
    from liquidation_heatmap_factor import compute_liquidation_heatmap_factor
    LIQUIDATION_HEATMAP_AVAILABLE = True
except ImportError:
    LIQUIDATION_HEATMAP_AVAILABLE = False

WHALE_FILE = "/root/.okx-paper/data/whale_events.json"

# Extend weight map
# 新权重配置：按相对占比加权，最终会按 active 权重归一化
CFG["weights"] = {
    # 新因子（高权重）
    "order_flow_persistence": 0.08,
    "liquidity_depth": 0.10,
    "liquidation_pressure": 0.15,
    "funding_basis_divergence": 0.15,
    # 原有因子
    "oi_delta": 0.10,
    "taker_volume": 0.10,
    "breakout": 0.08,
    "mean_reversion": 0.06,
    "technical_kline": 0.04,
    "momentum": 0.03,
    # 其他因子（权重=0，不参与决策但保留数据）
    "kline_pattern": 0.0,
    "strategy_history": 0.0,
    "asr_vc": 0.0,
    "cross_market_correlation": 0.0,
    "macro_risk_sentiment": 0.0,
    "enhanced_technical": 0.0,
}
CFG["whale_large_usd"] = float(os.getenv("WHALE_LARGE_USD", "1000000"))
CFG["whale_api_url"] = os.getenv("WHALE_API_URL", "").strip()
CFG["whale_api_key"] = os.getenv("WHALE_API_KEY", "").strip()
CFG["blockchair_api_key"] = os.getenv("BLOCKCHAIR_API_KEY", "").strip()
CFG["blockchair_min_btc"] = float(os.getenv("BLOCKCHAIR_MIN_BTC", "10"))

# === 交易成本参数 ===
CFG["fee_taker"] = 0.0005  # 0.05% taker手续费
CFG["fee_maker"] = 0.0002  # 0.02% maker手续费
CFG["slippage_bps_base"] = 1.5  # 基础滑点 1.5 bps
CFG["slippage_bps_high_vol"] = 4.0  # 高波动滑点 4 bps
CFG["funding_interval_hours"] = 8  # 资金费率结算间隔
CFG["funding_fee_multiplier"] = 1.0  # 资金费率乘数

# === ?????????v3.2.2? ===
CFG["intraday_max_hold_hours"] = float(os.getenv("INTRADAY_MAX_HOLD_HOURS", "4"))
CFG["sl_atr_mult"] = float(os.getenv("SL_ATR_MULT", "1.0"))
CFG["sl_min_pct"] = float(os.getenv("SL_MIN_PCT", "0.005"))
CFG["sl_max_pct"] = float(os.getenv("SL_MAX_PCT", "0.015"))
CFG["tp1_atr_mult"] = float(os.getenv("TP1_ATR_MULT", "1.3"))
CFG["tp1_min_pct"] = float(os.getenv("TP1_MIN_PCT", "0.010"))
CFG["tp2_atr_mult"] = float(os.getenv("TP2_ATR_MULT", "2.3"))
CFG["tp2_min_pct"] = float(os.getenv("TP2_MIN_PCT", "0.020"))

DATA_DIR_LOCAL = os.path.dirname(ACCOUNT_FILE)
TRADES_FILE = f"{DATA_DIR_LOCAL}/trades.json"
DECISIONS_FILE = f"{DATA_DIR_LOCAL}/decisions.json"
EVOLUTION_FILE = f"{DATA_DIR_LOCAL}/evolution_history.json"
ADAPTIVE_FILE = f"{DATA_DIR_LOCAL}/adaptive_params.json"
POSITIONS_FILE = f"{DATA_DIR_LOCAL}/positions.json"
PENDING_ANALYSIS_FILE = f"{DATA_DIR_LOCAL}/pending_analysis.json"
COST_LEDGER_FILE = f"{DATA_DIR_LOCAL}/cost_ledger.json"  # 成本台账
FACTOR_WEIGHTS_FILE = f"{DATA_DIR_LOCAL}/factor_weights.json"  # 因子权重持久化
BASE_THRESHOLDS = {
    "min_trade_score": 55,  # 提高阈值，减少低质量噪音开仓
    "min_confidence": 50,   # 降低信心阈值
}


# === 成本台账辅助函数 ===
def generate_cost_id():
    """生成成本记录ID"""
    import uuid
    return f"cost_{uuid.uuid4().hex[:12]}"


def calculate_slippage_bps(atr_pct: float, liquidity_quality: float, funding_rate: float) -> float:
    """
    计算滑点（bps）
    
    规则：
    - ATR% >= 1.2% 或 liquidity_quality < 0.70 或 abs(funding_rate) >= 0.03% → 4.0 bps
    - 仅一项轻微触发 → 2.5 bps
    - 默认 → 1.5 bps
    """
    triggers = 0
    
    if atr_pct >= 0.012:
        triggers += 1
    if liquidity_quality < 0.70:
        triggers += 1
    if funding_rate is not None and abs(funding_rate) >= 0.0003:
        triggers += 1
    
    if triggers >= 2:
        return CFG["slippage_bps_high_vol"]  # 4.0
    elif triggers == 1:
        return 2.5
    else:
        return CFG["slippage_bps_base"]  # 1.5


def calculate_fill_price(mark_price: float, side: str, slippage_bps: float) -> float:
    """
    计算模拟成交价
    
    long: fill_price = mark_price * (1 + slippage_bps/10000)
    short: fill_price = mark_price * (1 - slippage_bps/10000)
    """
    slippage_ratio = slippage_bps / 10000
    if side == "long":
        return mark_price * (1 + slippage_ratio)
    else:
        return mark_price * (1 - slippage_ratio)


def record_cost_entry(
    event_type: str,
    position_id: str,
    symbol: str,
    side: str,
    action: str,
    size_btc: float,
    leverage: int,
    mark_price: float,
    fill_price: float,
    funding_rate: float = None,
    fee_rate: float = 0.0005,
    slippage_bps: float = 1.5,
    fee_cost: float = 0.0,
    slippage_cost: float = 0.0,
    funding_cost: float = None,
    gross_pnl: float = None,
    net_pnl: float = None,
    equity_before: float = 0.0,
    equity_after: float = 0.0,
    realized_pnl_after: float = 0.0,
    reason: str = "",
    strategy: str = "intraday",
    notes: str = ""
) -> Dict[str, Any]:
    """
    记录成本台账条目
    """
    total_cost = fee_cost + slippage_cost
    if funding_cost is not None:
        total_cost += funding_cost
    
    entry = {
        "id": generate_cost_id(),
        "ts": iso_now(),
        "event_type": event_type,
        "position_id": position_id,
        "order_id": None,
        "symbol": symbol,
        "side": side,
        "action": action,
        "size_btc": round(size_btc, 4),
        "leverage": leverage,
        "mark_price": round(mark_price, 2),
        "fill_price": round(fill_price, 2),
        "funding_rate": funding_rate,
        "fee_rate": fee_rate,
        "slippage_bps": slippage_bps,
        "fee_cost": round(fee_cost, 4),
        "slippage_cost": round(slippage_cost, 4),
        "funding_cost": round(funding_cost, 4) if funding_cost is not None else None,
        "total_cost": round(total_cost, 4),
        "cost_currency": "USDT",
        "gross_pnl": round(gross_pnl, 4) if gross_pnl is not None else None,
        "net_pnl": round(net_pnl, 4) if net_pnl is not None else None,
        "equity_before": round(equity_before, 2),
        "equity_after": round(equity_after, 2),
        "realized_pnl_after": round(realized_pnl_after, 4),
        "reason": reason,
        "strategy": strategy,
        "notes": notes
    }
    
    # 写入台账
    ledger = read_json(COST_LEDGER_FILE, [])
    if not isinstance(ledger, list):
        ledger = []
    ledger.append(entry)
    # 保留最近1000条
    ledger = ledger[-1000:]
    write_json(COST_LEDGER_FILE, ledger)
    
    return entry


def ema(values: List[float], period: int) -> List[float]:
    if not values:
        return []
    if len(values) < period:
        return [mean(values)] * len(values)
    k = 2 / (period + 1)
    out = [mean(values[:period])]
    for v in values[period:]:
        out.append((v - out[-1]) * k + out[-1])
    return [out[0]] * (period - 1) + out


def rsi(values: List[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    deltas = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(0.0, d) for d in deltas[-period:]]
    losses = [max(0.0, -d) for d in deltas[-period:]]
    avg_gain = mean(gains) if gains else 0.0
    avg_loss = mean(losses) if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def calc_max_drawdown(equity: List[float]) -> float:
    if not equity:
        return 0.0
    peak = equity[0]
    max_dd = 0.0
    for x in equity:
        if x > peak:
            peak = x
        dd = (peak - x) / peak if peak > 0 else 0.0
        max_dd = max(max_dd, dd)
    return max_dd



def append_json_list(path: str, item: Dict[str, Any], keep: int = 1200) -> None:
    arr = read_json(path, [])
    if not isinstance(arr, list):
        arr = []
    arr.append(item)
    arr = arr[-keep:]
    write_json(path, arr)


def load_adaptive() -> Dict[str, Any]:
    d = read_json(ADAPTIVE_FILE, {})
    if not isinstance(d, dict):
        d = {}
    d.setdefault("min_trade_score_adj", 0)
    d.setdefault("min_confidence_adj", 0)
    d.setdefault("intraday_leverage_cap", 10)
    d.setdefault("swing_leverage_cap", 3)
    d.setdefault("version", "3.2.0")
    d.setdefault("updated_at", iso_now())
    return d


def save_adaptive(d: Dict[str, Any]) -> None:
    d["updated_at"] = iso_now()
    write_json(ADAPTIVE_FILE, d)


def load_factor_weights() -> Dict[str, float]:
    """加载持久化的因子权重"""
    d = read_json(FACTOR_WEIGHTS_FILE, {})
    if not isinstance(d, dict):
        d = {}
    return d


def save_factor_weights(weights: Dict[str, float]) -> None:
    """持久化因子权重"""
    write_json(FACTOR_WEIGHTS_FILE, weights)


def normalize_account(account: Dict[str, Any]) -> Dict[str, Any]:
    """
    统一账户字段规范，确保字段一致性
    - 迁移旧字段（snake_case）到新字段（camelCase）
    - 设置默认值
    - 删除废弃字段
    """
    if not isinstance(account, dict):
        account = {}
    
    # 兼容迁移：旧字段 -> 新字段
    if "realized_pnl" in account and "realizedPnl" not in account:
        account["realizedPnl"] = account["realized_pnl"]
    if "unrealized_pnl" in account and "unrealizedPnl" not in account:
        account["unrealizedPnl"] = account["unrealized_pnl"]
    if "daily_pnl" in account and "dailyPnl" not in account:
        account["dailyPnl"] = account["daily_pnl"]
    if "initial_balance" in account and "initialBalance" not in account:
        account["initialBalance"] = account["initial_balance"]
    if "margin_used" in account and "marginUsed" not in account:
        account["marginUsed"] = account["margin_used"]
    if "available_margin" in account and "availableMargin" not in account:
        account["availableMargin"] = account["available_margin"]
    if "max_drawdown" in account and "maxDrawdown" not in account:
        account["maxDrawdown"] = account["max_drawdown"]
    if "peak_equity" in account and "peakEquity" not in account:
        account["peakEquity"] = account["peak_equity"]
    if "daily_trades" in account and "dailyTrades" not in account:
        account["dailyTrades"] = account["daily_trades"]
    if "defense_mode" in account and "defenseMode" not in account:
        account["defenseMode"] = account["defense_mode"]
    
    # 删除旧字段
    old_fields = [
        "realized_pnl", "unrealized_pnl", "daily_pnl", "initial_balance",
        "margin_used", "available_margin", "max_drawdown", "peak_equity",
        "daily_trades", "defense_mode"
    ]
    for field in old_fields:
        account.pop(field, None)
    
    # 设置默认值
    account.setdefault("initialBalance", 1000)
    account.setdefault("balance", 1000)
    account.setdefault("equity", 1000)
    account.setdefault("marginUsed", 0)
    account.setdefault("availableMargin", 1000)
    account.setdefault("realizedPnl", 0)
    account.setdefault("unrealizedPnl", 0)
    account.setdefault("dailyPnl", 0)
    account.setdefault("maxDrawdown", 0)
    account.setdefault("peakEquity", 1000)
    account.setdefault("dailyTrades", 0)
    account.setdefault("defenseMode", False)
    account.setdefault("costDay", "")  # 成本统计日期，用于跨天清零
    account.setdefault("feesPaidToday", 0)
    account.setdefault("fundingPaidToday", 0)
    account.setdefault("updated_at", iso_now())
    
    return account


def load_account() -> Dict[str, Any]:
    """读取并规范化账户数据"""
    account = read_json(ACCOUNT_FILE, {})
    account = normalize_account(account)
    return account


def save_account(account: Dict[str, Any]) -> None:
    """保存账户数据（自动规范化）"""
    account = normalize_account(account)
    account["updated_at"] = iso_now()
    write_json(ACCOUNT_FILE, account)


class DataFeedV31(DataFeed):
    def _read_env_file(self, path: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    out[k.strip()] = v.strip().strip('"').strip("'")
        except Exception:
            return out
        return out

    def _get_json(self, url: str, timeout: int = 12, headers: Dict[str, str] | None = None):
        try:
            r = self.session.get(url, timeout=timeout, headers=headers or None)
            r.raise_for_status()
            return r.json(), None
        except Exception as e:
            return None, str(e)

    def _parse_okx_candles(self, data: Dict[str, Any]) -> List[Dict[str, float]]:
        rows = (data or {}).get("data") or []
        out: List[Dict[str, float]] = []
        for r in rows:
            if isinstance(r, list) and len(r) >= 6:
                out.append(
                    {
                        "ts": sfloat(r[0]),
                        "o": sfloat(r[1]),
                        "h": sfloat(r[2]),
                        "l": sfloat(r[3]),
                        "c": sfloat(r[4]),
                        "v": sfloat(r[5]),
                    }
                )
        out.reverse()
        return out

    def _merge_candles(self, base: List[Dict[str, float]], extra: List[Dict[str, float]]) -> List[Dict[str, float]]:
        merged: Dict[int, Dict[str, float]] = {}
        for row in base:
            ts = int(sfloat(row.get("ts"), 0))
            if ts > 0:
                merged[ts] = row
        for row in extra:
            ts = int(sfloat(row.get("ts"), 0))
            if ts > 0:
                merged[ts] = row
        return [merged[k] for k in sorted(merged.keys())]

    def _bar_seconds(self, bar: str) -> int:
        return {"5m": 300, "1H": 3600, "1D": 86400}.get(bar, 3600)

    def _fetch_candles_from_local(self, bar: str, limit: int = 240) -> List[Dict[str, float]]:
        # Rebuild synthetic OHLC from stored minute snapshots when upstream kline is short.
        history = read_json(HISTORY_FILE, [])
        if not isinstance(history, list) or not history:
            return []
        sec = self._bar_seconds(bar)
        buckets: Dict[int, Dict[str, float]] = {}
        for row in history[-5000:]:
            if not isinstance(row, dict):
                continue
            ts_raw = row.get("timestamp")
            spot = row.get("spot") if isinstance(row.get("spot"), dict) else {}
            price = sfloat(spot.get("price"), 0.0)
            if not ts_raw or price <= 0:
                continue
            try:
                dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                ts = int(dt.timestamp())
            except Exception:
                continue
            key = int((ts // sec) * sec * 1000)
            cur = buckets.get(key)
            if cur is None:
                buckets[key] = {"ts": float(key), "o": price, "h": price, "l": price, "c": price, "v": 0.0}
            else:
                prev_c = cur["c"]
                cur["h"] = max(cur["h"], price)
                cur["l"] = min(cur["l"], price)
                cur["c"] = price
                cur["v"] += abs(price - prev_c)
        if not buckets:
            return []
        out = [buckets[k] for k in sorted(buckets.keys())]
        return out[-limit:]

    def _fetch_whale_supabase(self) -> Dict[str, Any]:
        # Auto-discover existing monitor credentials from workspace .env
        env = self._read_env_file("/root/.openclaw/workspace/.env")
        url = env.get("SUPABASE_URL", "").rstrip("/")
        key = env.get("SUPABASE_KEY", "")
        if not url or not key:
            return {"source": "supabase", "events": []}
        headers = {"apikey": key, "Authorization": f"Bearer {key}"}
        # Try several likely table names from existing monitor conventions.
        tables = [
            "whale_events",
            "large_transfers",
            "onchain_transfers",
            "transfers",
            "alerts",
        ]
        for t in tables:
            q = f"{url}/rest/v1/{t}?select=*&order=timestamp.desc&limit=200"
            try:
                r = self.session.get(q, headers=headers, timeout=10)
                r.raise_for_status()
                data = r.json()
            except Exception:
                data = None
            if not isinstance(data, list):
                continue
            if data:
                return {"source": f"supabase:{t}", "events": data}
        return {"source": "supabase", "events": []}

    def _fetch_whale_blockchair(self) -> Dict[str, Any]:
        """
        Blockchair public/free API whale feed.
        We query large BTC transactions in the last 24h and convert to USD by
        using blockchair context market_price_usd.
        """
        # Configurable raw BTC threshold to avoid huge-query + no-signal extremes.
        sat_threshold = int(max(1.0, CFG.get("blockchair_min_btc", 10.0)) * 100_000_000)
        key = CFG.get("blockchair_api_key", "")
        base = "https://api.blockchair.com/bitcoin/transactions"
        q = f"time(>NOW-24h),output_total({sat_threshold}..)"
        url = f"{base}?q={q}&s=time(desc)&limit=120"
        if key:
            url += f"&key={key}"
        data, err = self._get_json(url, timeout=20)
        if err or not isinstance(data, dict):
            return {"source": "blockchair", "events": [], "error": err or "invalid_response"}

        # Blockchair may return non-200 with error payload in context
        context = data.get("context") or {}
        if context.get("code") and int(sfloat(context.get("code"))) >= 400:
            return {
                "source": "blockchair",
                "events": [],
                "error": context.get("error") or f"code_{context.get('code')}",
            }

        market_price = sfloat(context.get("market_price_usd"), 0.0)
        rows = data.get("data") or []
        events: List[Dict[str, Any]] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            sat = sfloat(r.get("output_total"), 0.0)
            if sat <= 0:
                continue
            btc = sat / 100_000_000.0
            usd = btc * market_price if market_price > 0 else 0.0
            # If price unavailable, keep sat threshold filter.
            if usd > 0 and usd < CFG["whale_large_usd"]:
                continue
            ts = r.get("time") or r.get("date") or ""
            events.append(
                {
                    "timestamp": ts,
                    "amount_usd": usd,
                    "amount_btc": btc,
                    "direction": "unknown",
                    "from_type": "unknown",
                    "to_type": "unknown",
                    "tx_hash": r.get("hash"),
                    "block_id": r.get("block_id"),
                }
            )
        return {"source": "blockchair", "events": events}

    def _fetch_candles(self, bar: str, limit: int = 180) -> List[Dict[str, float]]:
        candles: List[Dict[str, float]] = []
        # Try live endpoint first, then historical endpoint.
        for endpoint in ("candles", "history-candles"):
            url = f"https://www.okx.com/api/v5/market/{endpoint}?instId=BTC-USDT-SWAP&bar={bar}&limit={limit}"
            data, err = self._get_json(url, timeout=15)
            if err or not data:
                continue
            parsed = self._parse_okx_candles(data)
            if parsed:
                candles = self._merge_candles(candles, parsed)
            if len(candles) >= limit:
                break
        # Local historical fallback when upstream returns too few candles.
        min_required = min(limit, 80 if bar == "1H" else 60)
        if len(candles) < min_required:
            local = self._fetch_candles_from_local(bar, max(limit, min_required))
            candles = self._merge_candles(local, candles)  # keep API values when both exist
        return candles[-limit:]

    def _fetch_whale(self) -> Dict[str, Any]:
        # Preferred source per user requirement
        bc = self._fetch_whale_blockchair()
        if bc.get("events"):
            return bc
        if CFG.get("whale_api_url"):
            headers = {}
            key = CFG.get("whale_api_key", "")
            if key:
                headers["Authorization"] = f"Bearer {key}"
                headers["X-API-Key"] = key
            data, err = self._get_json(CFG["whale_api_url"], headers=headers)
            if not err and data is not None:
                if isinstance(data, dict) and isinstance(data.get("events"), list):
                    return {"source": "whale_api", "events": data["events"]}
                if isinstance(data, list):
                    return {"source": "whale_api", "events": data}
        supa = self._fetch_whale_supabase()
        if supa.get("events"):
            return supa
        local = read_json(WHALE_FILE, [])
        if isinstance(local, list) and local:
            return {"source": "whale_file", "events": local}
        # Preserve upstream error visibility for monitoring
        if bc.get("error"):
            return {"source": "blockchair", "events": [], "error": bc.get("error")}
        return {"source": "none", "events": []}

    def _long_term_metrics(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        d1 = ((snap.get("kline") or {}).get("d1") or [])
        if len(d1) < 210:
            return {"ready": False}
        closes = [sfloat(x.get("c"), 0.0) for x in d1 if sfloat(x.get("c"), 0.0) > 0]
        if len(closes) < 210:
            return {"ready": False}
        price = closes[-1]
        last200 = closes[-200:]
        ma200 = sum(last200) / len(last200)
        gma = math.exp(sum(math.log(max(1e-9, x)) for x in last200) / len(last200))
        day_idx = max(1.0, (utc_now() - datetime(2009, 1, 3, tzinfo=timezone.utc)).days)
        fair = math.exp(-17.0 + 5.8 * math.log(day_idx))
        ahr999_proxy = (price / max(1e-9, gma)) * (price / max(1e-9, fair))
        fg = sfloat(((snap.get("fear_greed") or {}).get("value")), 50)
        bottom_signal = bool(fg <= 20 and (ahr999_proxy <= 0.45 or price <= ma200 * 0.9))
        return {
            "ready": True,
            "price": round(price, 2),
            "ma200": round(ma200, 2),
            "gma200": round(gma, 2),
            "fair_value_proxy": round(fair, 2),
            "ahr999_proxy": round(ahr999_proxy, 4),
            "fear_greed": round(fg, 2),
            "bottom_signal": bottom_signal,
        }

    def fetch(self) -> Dict[str, Any]:
        out = super().fetch()
        out["kline"] = {
            "m5": self._fetch_candles("5m", 180),
            "h1": self._fetch_candles("1H", 240),
            "d1": self._fetch_candles("1D", 260),
        }
        out["taker_volume"] = self._fetch_taker_volume()
        out["long_term"] = self._long_term_metrics(out)
        # expose data quality in logs/panel when kline is still short
        h1n = len((out.get("kline") or {}).get("h1") or [])
        d1n = len((out.get("kline") or {}).get("d1") or [])
        if h1n < 80 or d1n < 210:
            out.setdefault("errors", []).append(f"kline_insufficient:h1={h1n},d1={d1n}")
        return out

    def _fetch_taker_volume(self) -> Dict[str, Any]:
        """Fetch taker buy/sell volume from OKX public API"""
        url = "https://www.okx.com/api/v5/rubik/stat/taker-volume?ccy=BTC&instType=CONTRACTS&period=5m"
        data, err = self._get_json(url, timeout=8)
        if err or not data or data.get("code") != "0":
            return {"error": err or data.get("msg", "unknown"), "data": []}
        rows = data.get("data", [])
        return {"data": rows[:24], "source": "okx"}


class FactorEngineV31(FactorEngine):
    def _factor(self, name: str, score: float, direction: str, confidence: float, details: Dict[str, Any]) -> Dict[str, Any]:
        return super()._factor(name, score, direction, confidence, details)

    def _direction(self, score: float, up: float = 54, down: float = 46) -> str:
        if score > up:
            return "bullish"
        if score < down:
            return "bearish"
        return "neutral"

    def oi_delta(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """Override: More sensitive to small OI changes"""
        oi = sfloat((snap.get("oi") or {}).get("contracts"), 0)
        spot = snap.get("spot") or {}
        price = sfloat(spot.get("price"), 0)
        
        if oi <= 0 or price <= 0:
            return self._factor("oi_delta", 50, "neutral", 20, {"missing": True})
        
        prices = self._hist_values(("spot", "price"), 20) if hasattr(self, '_hist_values') else [price] * 20
        ois = self._hist_values(("oi", "contracts"), 20) if hasattr(self, '_hist_values') else [oi] * 20
        
        score = 50.0
        details = {}
        
        if len(ois) >= 6 and len(prices) >= 6:
            oi_chg = (oi / max(1e-9, ois[-6]) - 1.0) * 100
            px_chg = (price / max(1e-9, prices[-6]) - 1.0) * 100
            
            # Strong signals (original logic)
            if px_chg > 0 and oi_chg > 0:
                score += clamp(min(px_chg, oi_chg) * 3.0, 0, 18)
            elif px_chg < 0 and oi_chg > 0:
                score -= clamp(min(abs(px_chg), oi_chg) * 3.0, 0, 18)
            elif px_chg > 0 and oi_chg < 0:
                score -= clamp(min(px_chg, abs(oi_chg)) * 2.5, 0, 14)
            # NEW: Add sensitivity to small changes
            elif abs(px_chg) > 0.05 or abs(oi_chg) > 0.05:
                # Small price up, OI stable = slight bullish
                if px_chg > 0.05 and abs(oi_chg) < 0.1:
                    score += min(3, px_chg * 2)
                # Small price down, OI stable = slight bearish
                elif px_chg < -0.05 and abs(oi_chg) < 0.1:
                    score -= min(3, abs(px_chg) * 2)
                # OI increasing with stable price = accumulation
                elif oi_chg > 0.05 and abs(px_chg) < 0.1:
                    score += min(2, oi_chg * 2)
                # OI decreasing with stable price = distribution
                elif oi_chg < -0.05 and abs(px_chg) < 0.1:
                    score -= min(2, abs(oi_chg) * 2)
            
            details = {"oi_change_6": round(oi_chg, 3), "price_change_6": round(px_chg, 3)}
        
        direction = "bullish" if score > 53 else "bearish" if score < 47 else "neutral"
        conf = 50 + min(30, abs(score - 50))
        return self._factor("oi_delta", score, direction, conf, details)

    def long_short(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """Override: More sensitive to ratio changes (threshold 1.05/0.95 instead of 1.45/0.75)"""
        ratio = sfloat((snap.get("long_short") or {}).get("ratio"), 1.0)
        score = 50.0
        
        # Strong signals (keep original thresholds)
        if ratio > 1.45:
            score -= clamp((ratio - 1.45) * 35.0, 0, 20)
        elif ratio < 0.75:
            score += clamp((0.75 - ratio) * 35.0, 0, 20)
        # NEW: Medium signals
        elif ratio > 1.20:
            score -= clamp((ratio - 1.20) * 15.0, 0, 8)
        elif ratio < 0.85:
            score += clamp((0.85 - ratio) * 15.0, 0, 8)
        # NEW: Weak signals (more sensitive)
        elif ratio > 1.05:
            score -= clamp((ratio - 1.05) * 8.0, 0, 4)
        elif ratio < 0.95:
            score += clamp((0.95 - ratio) * 8.0, 0, 4)
        
        direction = "bullish" if score > 52 else "bearish" if score < 48 else "neutral"
        conf = 48 + min(30, abs(score - 50) * 1.2)
        return self._factor("long_short", score, direction, conf, {"ratio": round(ratio, 4)})

    def market_regime(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """Override: More sensitive to market changes"""
        market = snap.get("market") or {}
        dom = sfloat(market.get("btc_dominance"), 50)
        chg = sfloat(market.get("mcap_change_24h"), 0)
        score = 50.0
        
        # Strong signals (keep original)
        if chg > 4:
            score += clamp(chg * 1.8, 0, 15)
        elif chg < -4:
            score -= clamp(abs(chg) * 1.8, 0, 15)
        # NEW: Medium signals
        elif chg > 2:
            score += clamp((chg - 2) * 1.2, 0, 6)
        elif chg < -2:
            score -= clamp((abs(chg) - 2) * 1.2, 0, 6)
        # NEW: Weak signals
        elif chg > 0.5:
            score += clamp(chg * 0.8, 0, 3)
        elif chg < -0.5:
            score -= clamp(abs(chg) * 0.8, 0, 3)
        
        # Dominance effect
        if dom > 60:
            score -= clamp((dom - 60) * 1.0, 0, 8)
        elif dom < 50:
            score += clamp((50 - dom) * 0.8, 0, 6)
        
        direction = "bullish" if score > 52 else "bearish" if score < 48 else "neutral"
        conf = 48 + min(26, abs(chg) * 1.5)
        return self._factor("market_regime", score, direction, conf, {"btc_dom": dom, "mcap_change_24h": chg})

    def whale_flow(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        whale = snap.get("whale") or {}
        events = whale.get("events") if isinstance(whale, dict) else []
        events = events if isinstance(events, list) else []
        large = CFG["whale_large_usd"]
        now_ms = int(utc_now().timestamp() * 1000)
        buy_flow = 0.0
        sell_flow = 0.0
        total = 0.0
        used = 0
        for ev in events[-300:]:
            if not isinstance(ev, dict):
                continue
            ts = sfloat(ev.get("timestamp") or ev.get("ts") or ev.get("time") or now_ms)
            if ts < 1e12:
                ts *= 1000
            if now_ms - ts > 24 * 3600 * 1000:
                continue
            usd = sfloat(ev.get("amount_usd") or ev.get("value_usd") or ev.get("usd") or ev.get("size_usd") or ev.get("amount"), 0)
            if usd < large:
                continue
            frm = str(ev.get("from_type") or ev.get("from") or "").lower()
            to = str(ev.get("to_type") or ev.get("to") or "").lower()
            direction = str(ev.get("direction") or "").lower()
            weight = min(3.0, usd / max(1.0, large))
            if "exchange" in to or direction in {"to_exchange", "inflow", "deposit"}:
                sell_flow += weight
            elif "exchange" in frm or direction in {"from_exchange", "outflow", "withdraw"}:
                buy_flow += weight
            total += weight
            used += 1
        score = 50.0
        if total > 0:
            score += clamp(((buy_flow - sell_flow) / max(1.0, total)) * 30.0, -24, 24)
        conf = 30 + min(55, total * 4)
        details = {"source": whale.get("source", "none") if isinstance(whale, dict) else "none", "events_used": used, "buy_flow": round(buy_flow, 3), "sell_flow": round(sell_flow, 3), "large_usd_threshold": large}
        if isinstance(whale, dict) and whale.get("error"):
            details["upstream_error"] = str(whale.get("error"))[:220]
        if used == 0:
            details["missing_reason"] = "no_large_events_24h"
            conf = 20
        return self._factor("whale_flow", score, self._direction(score), conf, details)

    def kline_pattern(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        candles = (snap.get("kline") or {}).get("m5") or []
        score = 50.0
        patterns: List[str] = []
        if len(candles) >= 6:
            c1, c2 = candles[-2], candles[-1]
            o1, h1, l1, cl1 = c1["o"], c1["h"], c1["l"], c1["c"]
            o2, h2, l2, cl2 = c2["o"], c2["h"], c2["l"], c2["c"]
            body2 = abs(cl2 - o2)
            rng2 = max(1e-9, h2 - l2)
            if cl1 < o1 and cl2 > o2 and o2 <= cl1 and cl2 >= o1:
                score += 10
                patterns.append("bullish_engulfing")
            if cl1 > o1 and cl2 < o2 and o2 >= cl1 and cl2 <= o1:
                score -= 10
                patterns.append("bearish_engulfing")
            lw = min(o2, cl2) - l2
            uw = h2 - max(o2, cl2)
            if body2 / rng2 < 0.35 and lw > body2 * 2.2 and uw < body2 * 1.2:
                score += 8
                patterns.append("hammer")
            if body2 / rng2 < 0.35 and uw > body2 * 2.2 and lw < body2 * 1.2:
                score -= 8
                patterns.append("shooting_star")
        return self._factor("kline_pattern", score, self._direction(score), 45 + min(35, len(patterns) * 12), {"patterns": patterns})

    def technical_kline(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        candles = (snap.get("kline") or {}).get("h1") or []
        score = 50.0
        details: Dict[str, Any] = {}
        if len(candles) >= 50:
            closes = [x["c"] for x in candles]
            highs = [x["h"] for x in candles]
            lows = [x["l"] for x in candles]
            rv = rsi(closes, 14)
            ef = ema(closes, 12)
            es = ema(closes, 26)
            atr = mean([highs[i] - lows[i] for i in range(max(1, len(highs) - 20), len(highs))])
            score += clamp((ef[-1] - es[-1]) / max(1e-9, closes[-1]) * 8000, -15, 15)
            if rv < 30:
                score += 10
            elif rv > 70:
                score -= 10
            details = {"rsi14": round(rv, 3), "ema12": round(ef[-1], 2), "ema26": round(es[-1], 2), "atr20": round(atr, 2)}
        return self._factor("technical_kline", score, self._direction(score), 52 + min(25, abs(score - 50)), details)

    def strategy_history(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        candles = (snap.get("kline") or {}).get("h1") or []
        if len(candles) < 80:
            return self._factor("strategy_history", 50, "neutral", 20, {"missing": True})
        closes = [x["c"] for x in candles]
        ef = ema(closes, 10)
        es = ema(closes, 30)
        rets = [closes[i] / closes[i - 1] - 1.0 for i in range(1, len(closes))]
        strat = []
        for i in range(1, len(closes)):
            sig = 1 if ef[i - 1] > es[i - 1] else -1
            if abs((ef[i - 1] - es[i - 1]) / max(1e-9, closes[i - 1])) < 0.0008:
                sig = 0
            strat.append(sig * rets[i - 1])
        eq = [1.0]
        for r in strat:
            eq.append(eq[-1] * (1 + r))
        total_ret = eq[-1] - 1.0
        dd = calc_max_drawdown(eq)
        win = sum(1 for x in strat if x > 0) / max(1, len(strat))
        score = 50.0 + clamp(total_ret * 220, -20, 24) - clamp(dd * 180, 0, 24) + clamp((win - 0.5) * 40, -8, 8)
        details = {"sim_return_pct": round(total_ret * 100, 3), "sim_max_drawdown_pct": round(dd * 100, 3), "sim_win_rate": round(win, 4), "bars": len(candles)}
        return self._factor("strategy_history", score, self._direction(score), 45 + min(35, len(candles) / 8), details)

    def asr_vc(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """
        ASR-VC (inference):
        ASR = Adaptive Support/Resistance positioning (rolling channel)
        VC  = Volume Confirmation (z-score of volume)
        """
        candles = (snap.get("kline") or {}).get("h1") or []
        if len(candles) < 50:
            return self._factor("asr_vc", 50, "neutral", 20, {"missing": True})

        closes = [x["c"] for x in candles]
        highs = [x["h"] for x in candles]
        lows = [x["l"] for x in candles]
        vols = [x.get("v", 0.0) for x in candles]

        window_h = highs[-49:-1]
        window_l = lows[-49:-1]
        if not window_h or not window_l:
            return self._factor("asr_vc", 50, "neutral", 20, {"missing": True})

        resistance = max(window_h)
        support = min(window_l)
        close = closes[-1]
        span = max(1e-9, resistance - support)
        pos = (close - support) / span  # 0~1

        # VC: how strong current volume is vs recent
        vol_win = vols[-49:-1]
        mu = mean(vol_win) if vol_win else 0.0
        sd = math.sqrt(mean([(v - mu) ** 2 for v in vol_win])) if vol_win else 0.0
        z = (vols[-1] - mu) / sd if sd > 1e-9 else 0.0

        score = 50.0
        # Reversion tendency in neutral volume
        if pos < 0.25:
            score += 8
        elif pos > 0.75:
            score -= 8

        # Breakout / breakdown with volume confirmation
        if close > resistance:
            score += 12 if z > 1.0 else 4
        elif close < support:
            score -= 12 if z > 1.0 else 4

        # Penalize weak moves on thin volume
        if abs(z) < 0.2:
            score -= 2

        details = {
            "support": round(support, 2),
            "resistance": round(resistance, 2),
            "asr_pos": round(pos, 4),
            "volume_z": round(z, 3),
        }
        return self._factor("asr_vc", score, self._direction(score), 50 + min(28, abs(z) * 10), details)

    def taker_volume(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """
        Taker Volume Factor - analyze buy/sell pressure from OKX taker volume data
        Data format: [timestamp, buy_volume_usd, sell_volume_usd]
        """
        tv_data = snap.get("taker_volume") or {}
        rows = tv_data.get("data", [])
        
        if not rows or len(rows) < 3:
            return self._factor("taker_volume", 50, "neutral", 20, {"missing": True, "reason": "insufficient_data"})
        
        # Aggregate last N periods
        total_buy = 0.0
        total_sell = 0.0
        for row in rows[:12]:  # Last 1 hour (12 * 5min)
            if len(row) >= 3:
                total_buy += sfloat(row[1], 0)
                total_sell += sfloat(row[2], 0)
        
        if total_buy + total_sell < 1e6:  # Less than 1M USD total volume
            return self._factor("taker_volume", 50, "neutral", 25, {"missing": True, "reason": "low_volume"})
        
        # Calculate buy/sell ratio
        ratio = total_buy / max(1e-9, total_sell)
        total = total_buy + total_sell
        
        # Score based on ratio
        # ratio > 1.2 = bullish, ratio < 0.8 = bearish
        score = 50.0
        if ratio > 1.3:
            score += min(20, (ratio - 1.0) * 30)
        elif ratio > 1.1:
            score += min(12, (ratio - 1.0) * 40)
        elif ratio < 0.7:
            score -= min(20, (1.0 - ratio) * 30)
        elif ratio < 0.9:
            score -= min(12, (1.0 - ratio) * 40)
        
        # Confidence based on volume magnitude and ratio strength
        vol_conf = min(30, (total / 1e7) * 5)  # More volume = more confidence
        ratio_conf = min(25, abs(ratio - 1.0) * 40)  # Stronger ratio = more confidence
        conf = 35 + vol_conf + ratio_conf
        
        details = {
            "buy_vol_usd": round(total_buy, 0),
            "sell_vol_usd": round(total_sell, 0),
            "buy_sell_ratio": round(ratio, 3),
            "periods": len(rows[:12]),
        }
        
        return self._factor("taker_volume", score, self._direction(score), conf, details)

    def breakout(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """
        Breakout Strategy Factor - detect and score breakout opportunities
        Combines support/resistance breakout with volume confirmation and trend alignment
        """
        # Get candle data
        candles = (snap.get("kline") or {}).get("h1") or []
        if len(candles) < 50:
            return self._factor("breakout", 50, "neutral", 20, {"missing": True, "reason": "insufficient_candles"})
        
        # Get current price and volume data
        closes = [x["c"] for x in candles]
        highs = [x["h"] for x in candles]
        lows = [x["l"] for x in candles]
        vols = [x.get("v", 0.0) for x in candles]
        
        current_price = closes[-1]
        current_vol = vols[-1]
        
        # Calculate support/resistance levels (last 48 hours)
        window_highs = highs[-49:-1]
        window_lows = lows[-49:-1]
        
        resistance = max(window_highs) if window_highs else current_price
        support = min(window_lows) if window_lows else current_price
        
        # Calculate ATR for breakout threshold
        tr_list = []
        for i in range(-20, -1):
            if i == -20:
                tr = highs[i] - lows[i]
            else:
                tr = max(highs[i] - lows[i], 
                        abs(highs[i] - closes[i-1]), 
                        abs(lows[i] - closes[i-1]))
            tr_list.append(tr)
        atr = mean(tr_list) if tr_list else 0
        
        # Volume analysis
        vol_window = vols[-49:-1]
        avg_vol = mean(vol_window) if vol_window else current_vol
        vol_std = math.sqrt(mean([(v - avg_vol)**2 for v in vol_window])) if vol_window else 0
        vol_z = (current_vol - avg_vol) / max(1e-9, vol_std) if vol_std > 1e-9 else 0
        
        # Breakout detection
        breakout_threshold = atr * 0.5  # Minimum breakout distance
        
        upward_breakout = current_price > resistance + breakout_threshold
        downward_breakout = current_price < support - breakout_threshold
        
        # Volume confirmation
        volume_confirmed = vol_z > 1.0  # Volume 1 std above average
        strong_volume = vol_z > 2.0  # Volume 2 std above average
        
        # Trend alignment using MA
        ma20 = mean(closes[-21:-1]) if len(closes) >= 21 else current_price
        ma50 = mean(closes[-51:-1]) if len(closes) >= 51 else current_price
        
        uptrend = current_price > ma20 > ma50
        downtrend = current_price < ma20 < ma50
        
        # Calculate breakout score
        score = 50.0
        confidence = 30.0
        breakout_type = "none"
        details = {
            "resistance": round(resistance, 2),
            "support": round(support, 2),
            "current_price": round(current_price, 2),
            "atr": round(atr, 2),
            "volume_z": round(vol_z, 2),
            "breakout_dist": round(breakout_threshold, 2),
        }
        
        if upward_breakout:
            breakout_type = "upward"
            score = 58.0  # Base bullish score
            
            # Add points for confirmation
            if volume_confirmed:
                score += 8
                confidence += 10
            if strong_volume:
                score += 5
                confidence += 5
            if uptrend:
                score += 6
                confidence += 8
            
            # Distance from resistance
            dist_pct = (current_price - resistance) / max(1e-9, atr)
            score += min(8, dist_pct * 2)
            
            details["breakout_type"] = "upward"
            details["dist_from_resistance"] = round(dist_pct, 2)
            
        elif downward_breakout:
            breakout_type = "downward"
            score = 42.0  # Base bearish score
            
            # Add points for confirmation
            if volume_confirmed:
                score -= 8
                confidence += 10
            if strong_volume:
                score -= 5
                confidence += 5
            if downtrend:
                score -= 6
                confidence += 8
            
            # Distance from support
            dist_pct = (support - current_price) / max(1e-9, atr)
            score -= min(8, dist_pct * 2)
            
            details["breakout_type"] = "downward"
            details["dist_from_support"] = round(dist_pct, 2)
            
        else:
            # Check for near-breakout (potential setup)
            dist_to_resistance = (resistance - current_price) / max(1e-9, atr)
            dist_to_support = (current_price - support) / max(1e-9, atr)
            
            details["dist_to_resistance_atr"] = round(dist_to_resistance, 2)
            details["dist_to_support_atr"] = round(dist_to_support, 2)
            details["breakout_type"] = "none"
            
            # Near resistance - potential upward breakout setup
            if dist_to_resistance < 1.0 and volume_confirmed:
                score += 3
                confidence += 5
                details["setup"] = "near_resistance_with_volume"
            
            # Near support - potential downward breakout setup
            elif dist_to_support < 1.0 and volume_confirmed:
                score -= 3
                confidence += 5
                details["setup"] = "near_support_with_volume"
        
        details["trend"] = "uptrend" if uptrend else "downtrend" if downtrend else "neutral"
        details["volume_status"] = "strong" if strong_volume else "confirmed" if volume_confirmed else "normal"
        
        confidence = min(80, confidence + 20)
        
        return self._factor("breakout", score, self._direction(score), confidence, details)

    def mean_reversion(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mean Reversion Strategy Factor - identify overbought/oversold conditions
        With trend filtering to avoid catching falling knives
        RSI thresholds: < 35 oversold (long), > 65 overbought (short)
        """
        # Get candle data
        candles = (snap.get("kline") or {}).get("h1") or []
        if len(candles) < 60:
            return self._factor("mean_reversion", 50, "neutral", 20, {"missing": True, "reason": "insufficient_candles"})
        
        closes = [x["c"] for x in candles]
        highs = [x["h"] for x in candles]
        lows = [x["l"] for x in candles]
        vols = [x.get("v", 0.0) for x in candles]
        
        current_price = closes[-1]
        
        # Calculate RSI (14-period)
        def calc_rsi(prices, period=14):
            if len(prices) < period + 1:
                return 50.0
            deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
            gains = [max(0.0, d) for d in deltas[-period:]]
            losses = [max(0.0, -d) for d in deltas[-period:]]
            avg_gain = mean(gains) if gains else 0.0
            avg_loss = mean(losses) if losses else 0.0
            if avg_loss == 0:
                return 100.0
            rs = avg_gain / avg_loss
            return 100.0 - (100.0 / (1.0 + rs))
        
        rsi_14 = calc_rsi(closes, 14)
        
        # Calculate moving averages for trend detection
        ma20 = mean(closes[-21:-1]) if len(closes) >= 21 else current_price
        ma50 = mean(closes[-51:-1]) if len(closes) >= 51 else current_price
        
        # Trend determination
        uptrend = current_price > ma20 > ma50
        downtrend = current_price < ma20 < ma50
        sideways = not uptrend and not downtrend
        
        # Calculate Bollinger Bands
        bb_period = 20
        bb_closes = closes[-(bb_period+1):-1]
        bb_mid = mean(bb_closes) if bb_closes else current_price
        bb_std = math.sqrt(mean([(c - bb_mid)**2 for c in bb_closes])) if bb_closes else 0
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std
        bb_position = (current_price - bb_lower) / max(1e-9, bb_upper - bb_lower) if bb_upper > bb_lower else 0.5
        
        # Price deviation from MA20
        ma_deviation_pct = (current_price - ma20) / max(1e-9, ma20) * 100
        
        # ATR for volatility context
        tr_list = []
        for i in range(-20, -1):
            tr = max(highs[i] - lows[i], 
                    abs(highs[i] - closes[i-1]), 
                    abs(lows[i] - closes[i-1]))
            tr_list.append(tr)
        atr = mean(tr_list) if tr_list else 0
        
        # Volume analysis
        vol_window = vols[-20:-1]
        avg_vol = mean(vol_window) if vol_window else vols[-1]
        vol_std = math.sqrt(mean([(v - avg_vol)**2 for v in vol_window])) if vol_window else 0
        vol_z = (vols[-1] - avg_vol) / max(1e-9, vol_std) if vol_std > 1e-9 else 0
        
        # Mean reversion scoring
        score = 50.0
        confidence = 30.0
        
        details = {
            "rsi": round(rsi_14, 2),
            "bb_position_pct": round(bb_position * 100, 1),
            "ma_deviation_pct": round(ma_deviation_pct, 2),
            "ma20": round(ma20, 2),
            "bb_upper": round(bb_upper, 2),
            "bb_lower": round(bb_lower, 2),
            "trend": "uptrend" if uptrend else "downtrend" if downtrend else "sideways",
            "atr": round(atr, 2),
        }
        
        # Overbought condition (potential short)
        if rsi_14 > 65:
            base_score = 42.0  # Bearish bias
            
            # Trend filter: only short in downtrend or sideways
            if downtrend or sideways:
                # Stronger signal with higher RSI
                rsi_bonus = min(10, (rsi_14 - 65) * 0.5)
                score = base_score - rsi_bonus
                
                # BB confirmation
                if bb_position > 0.85:
                    score -= 5
                    confidence += 8
                
                # Volume confirmation (low vol = weak momentum)
                if vol_z < 0:
                    confidence += 5
                
                # Price deviation confirmation
                if ma_deviation_pct > 2:
                    score -= 3
                    confidence += 5
                
                details["signal"] = "overbought_short"
                details["trend_filter"] = "passed"
            else:
                # Uptrend - filter out short signals
                score = 50.0
                details["signal"] = "overbought_but_uptrend"
                details["trend_filter"] = "blocked"
        
        # Oversold condition (potential long)
        elif rsi_14 < 35:
            base_score = 58.0  # Bullish bias
            
            # Trend filter: only long in uptrend or sideways
            if uptrend or sideways:
                # Stronger signal with lower RSI
                rsi_bonus = min(10, (35 - rsi_14) * 0.5)
                score = base_score + rsi_bonus
                
                # BB confirmation
                if bb_position < 0.15:
                    score += 5
                    confidence += 8
                
                # Volume confirmation
                if vol_z < 0:
                    confidence += 5
                
                # Price deviation confirmation
                if ma_deviation_pct < -2:
                    score += 3
                    confidence += 5
                
                details["signal"] = "oversold_long"
                details["trend_filter"] = "passed"
            else:
                # Downtrend - filter out long signals
                score = 50.0
                details["signal"] = "oversold_but_downtrend"
                details["trend_filter"] = "blocked"
        
        # Neutral RSI - check for extreme BB positions
        else:
            if bb_position > 0.9 and (downtrend or sideways):
                score = 46.0
                details["signal"] = "near_bb_upper"
                confidence += 5
            elif bb_position < 0.1 and (uptrend or sideways):
                score = 54.0
                details["signal"] = "near_bb_lower"
                confidence += 5
            else:
                details["signal"] = "neutral"
        
        # Adjust confidence based on signal clarity
        if abs(rsi_14 - 50) > 20:
            confidence += 10
        elif abs(rsi_14 - 50) > 10:
            confidence += 5
        
        # Sideways market increases confidence for mean reversion
        if sideways:
            confidence += 8
            details["market_regime"] = "sideways_favorable"
        
        confidence = min(80, confidence + 20)
        
        return self._factor("mean_reversion", score, self._direction(score), confidence, details)

    def liquidity_depth(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """
        Liquidity Depth Factor - analyze order book depth and pressure
        With spoofing detection and credibility-weighted analysis
        
        双层计算架构：
        1. raw_score/raw_conf - 原始因子评分
        2. quality (0~1) - 数据质量评估
        3. gated_score/gated_conf - 质量门控后的最终评分
        """
        import requests
        import time
        
        # Get order book data
        try:
            url = "https://www.okx.com/api/v5/market/books"
            params = {"instId": "BTC-USDT-SWAP", "sz": "50"}
            r = requests.get(url, params=params, timeout=5)
            data = r.json()
            
            if data.get("code") != "0":
                return self._factor("liquidity_depth", 50, "neutral", 20, {
                    "error": data.get("msg", "API error"), 
                    "quality": 0,
                    "hard_hold_candidate": True,
                    "gate_reason": "orderbook_unavailable"
                })
            
            books = data.get("data", [{}])[0]
            bids = books.get("bids", [])
            asks = books.get("asks", [])
            
            if not bids or not asks:
                return self._factor("liquidity_depth", 50, "neutral", 20, {
                    "error": "empty orderbook", 
                    "quality": 0,
                    "hard_hold_candidate": True,
                    "gate_reason": "orderbook_unavailable"
                })
            
            # Process bids and asks
            bid_list = [(float(b[0]), float(b[1])) for b in bids]
            ask_list = [(float(a[0]), float(a[1])) for a in asks]
            
            # === 质量指标初始化 ===
            quality_metrics = {
                "spoof_risk": 0.0,      # 欺骗行为风险 (0~1)
                "spread_risk": 0.0,     # 价差风险 (0~1)
                "impact_risk": 0.0,     # 冲击成本风险 (0~1)
                "depth_risk": 0.0,      # 深度风险 (0~1)
            }
            
            # === Order Tracking for Spoofing Detection ===
            spoofing_analysis = {"detected": False, "signals": [], "credible_bid_vol": 0, "credible_ask_vol": 0}
            
            if ORDERBOOK_TRACKER_AVAILABLE:
                try:
                    tracker = get_tracker()
                    analysis = tracker.update_orders(bid_list, ask_list, time.time())
                    
                    # Get credibility-weighted volumes
                    _, credible_bid_vol = tracker.get_adjusted_volume(
                        [{"side": "bid", "volume": v, "credibility": c} 
                         for v, c in [(b["volume"], b["credibility"]) for b in analysis.get("large_bids", [])]], 
                        "bid"
                    )
                    _, credible_ask_vol = tracker.get_adjusted_volume(
                        [{"side": "ask", "volume": v, "credibility": c} 
                         for v, c in [(a["volume"], a["credibility"]) for a in analysis.get("large_asks", [])]], 
                        "ask"
                    )
                    
                    spoofing_analysis = {
                        "detected": len(analysis.get("spoofing_signals", [])) > 0,
                        "signals": analysis.get("spoofing_signals", [])[:3],
                        "credible_bid_vol": credible_bid_vol,
                        "credible_ask_vol": credible_ask_vol,
                        "suspicious_bids": sum(1 for b in analysis.get("large_bids", []) if b.get("is_suspicious")),
                        "suspicious_asks": sum(1 for a in analysis.get("large_asks", []) if a.get("is_suspicious")),
                        "avg_credibility": sum(
                            b.get("credibility", 1) for b in analysis.get("large_bids", [])
                        ) / max(1, len(analysis.get("large_bids", []))) if analysis.get("large_bids") else 1.0
                    }
                    
                    # 计算 spoof_risk
                    if spoofing_analysis["detected"]:
                        suspicious_count = spoofing_analysis["suspicious_bids"] + spoofing_analysis["suspicious_asks"]
                        quality_metrics["spoof_risk"] = min(1.0, suspicious_count * 0.2)
                    
                except Exception as e:
                    spoofing_analysis["error"] = str(e)[:50]
            
            # Calculate volumes
            bid_volume = sum(float(b[1]) for b in bids)
            ask_volume = sum(float(a[1]) for a in asks)
            total_volume = bid_volume + ask_volume
            
            # Order book imbalance (raw)
            raw_imbalance = (bid_volume - ask_volume) / max(1e-9, total_volume) if total_volume > 0 else 0
            
            # Find large orders (> 10 BTC)
            large_threshold = 10.0
            large_bids = [(float(b[0]), float(b[1])) for b in bids if float(b[1]) > large_threshold]
            large_asks = [(float(a[0]), float(a[1])) for a in asks if float(a[1]) > large_threshold]
            
            # Use credibility-weighted volumes if available
            if spoofing_analysis.get("credible_bid_vol", 0) > 0 or spoofing_analysis.get("credible_ask_vol", 0) > 0:
                large_bid_volume = spoofing_analysis["credible_bid_vol"]
                large_ask_volume = spoofing_analysis["credible_ask_vol"]
            else:
                large_bid_volume = sum(v for _, v in large_bids)
                large_ask_volume = sum(v for _, v in large_asks)
            
            large_imbalance = (large_bid_volume - large_ask_volume) / max(1e-9, large_bid_volume + large_ask_volume) if (large_bid_volume + large_ask_volume) > 0 else 0
            
            # Spread analysis
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            spread = best_ask - best_bid
            spread_pct = (spread / best_bid * 100) if best_bid > 0 else 0
            mid_price = (best_bid + best_ask) / 2
            
            # === 计算 spread_risk ===
            # 价差 > 0.1% 认为有风险
            if spread_pct > 0.1:
                quality_metrics["spread_risk"] = min(1.0, spread_pct / 0.2)
            
            # Price pressure (weighted by proximity to mid price)
            bid_pressure = 0.0
            ask_pressure = 0.0
            for b in bids[:20]:
                price, vol = float(b[0]), float(b[1])
                dist = (mid_price - price) / mid_price
                bid_pressure += vol * (1 - dist)
            
            for a in asks[:20]:
                price, vol = float(a[0]), float(a[1])
                dist = (price - mid_price) / mid_price
                ask_pressure += vol * (1 - dist)
            
            pressure_imbalance = (bid_pressure - ask_pressure) / max(1e-9, bid_pressure + ask_pressure)
            
            # === Price Impact Estimation ===
            impact_sell = 0
            impact_buy = 0
            remaining = 100.0
            
            for a in asks:
                if remaining <= 0:
                    break
                filled = min(remaining, float(a[1]))
                impact_buy += filled * (float(a[0]) - mid_price) / mid_price
                remaining -= filled
            
            remaining = 100.0
            for b in bids:
                if remaining <= 0:
                    break
                filled = min(remaining, float(b[1]))
                impact_sell += filled * (mid_price - float(b[0])) / mid_price
                remaining -= filled
            
            impact_asymmetry = (impact_sell - impact_buy) / max(1e-9, impact_sell + impact_buy) if (impact_sell + impact_buy) > 0 else 0
            
            # === 计算 impact_risk ===
            avg_impact = (impact_sell + impact_buy) / 2
            # 冲击成本 > 0.5% 认为有风险
            if avg_impact > 0.005:
                quality_metrics["impact_risk"] = min(1.0, avg_impact / 0.01)
            
            # === 计算 depth_risk ===
            # 总深度 < 500 BTC 认为有风险
            if total_volume < 500:
                quality_metrics["depth_risk"] = max(0, 1 - total_volume / 500)
            
            # Order book slope analysis
            bid_cumsum = []
            ask_cumsum = []
            cum_bid = 0
            cum_ask = 0
            
            for b in bids[:20]:
                cum_bid += float(b[1])
                bid_cumsum.append((float(b[0]), cum_bid))
            
            for a in asks[:20]:
                cum_ask += float(a[1])
                ask_cumsum.append((float(a[0]), cum_ask))
            
            bid_slope = (bid_cumsum[-1][1] - bid_cumsum[0][1]) / max(1, len(bid_cumsum)) if len(bid_cumsum) > 1 else 0
            ask_slope = (ask_cumsum[-1][1] - ask_cumsum[0][1]) / max(1, len(ask_cumsum)) if len(ask_cumsum) > 1 else 0
            slope_imbalance = (bid_slope - ask_slope) / max(1e-9, bid_slope + ask_slope) if (bid_slope + ask_slope) > 0 else 0
            
            # Effective liquidity
            bid_effective = sum(float(b[1]) * (1 / (1 + abs(float(b[0]) - mid_price) / mid_price * 100)) for b in bids[:20])
            ask_effective = sum(float(a[1]) * (1 / (1 + abs(float(a[0]) - mid_price) / mid_price * 100)) for a in asks[:20])
            effective_imbalance = (bid_effective - ask_effective) / max(1e-9, bid_effective + ask_effective) if (bid_effective + ask_effective) > 0 else 0
            
            # Top 5 ratio
            top5_bid_vol = sum(float(b[1]) for b in bids[:5])
            top5_ask_vol = sum(float(a[1]) for a in asks[:5])
            top5_ratio = top5_bid_vol / max(1e-9, top5_ask_vol) if top5_ask_vol > 0 else 1.0
            
            # Average order size
            avg_bid_size = bid_volume / len(bids) if bids else 0
            avg_ask_size = ask_volume / len(asks) if asks else 0
            size_imbalance = (avg_bid_size - avg_ask_size) / max(1e-9, avg_bid_size + avg_ask_size) if (avg_bid_size + avg_ask_size) > 0 else 0
            
            # ========================================
            # 第一层：计算 raw_score / raw_conf
            # ========================================
            raw_score = 50.0
            raw_conf = 30.0
            
            # Use credibility-adjusted imbalance
            imbalance = large_imbalance
            
            # Overall imbalance effect
            if imbalance > 0.3:
                raw_score += clamp(imbalance * 20, 0, 15)
                raw_conf += min(15, abs(imbalance) * 25)
            elif imbalance < -0.3:
                raw_score -= clamp(abs(imbalance) * 20, 0, 15)
                raw_conf += min(15, abs(imbalance) * 25)
            elif abs(imbalance) > 0.15:
                raw_score += imbalance * 10
                raw_conf += min(8, abs(imbalance) * 15)
            
            # Large order imbalance
            avg_cred = spoofing_analysis.get("avg_credibility", 1.0)
            if large_imbalance > 0.5:
                raw_score += 8 * avg_cred
                raw_conf += 10 * avg_cred
            elif large_imbalance < -0.5:
                raw_score -= 8 * avg_cred
                raw_conf += 10 * avg_cred
            
            # Pressure imbalance
            if pressure_imbalance > 0.2:
                raw_score += 5
            elif pressure_imbalance < -0.2:
                raw_score -= 5
            
            # Slope
            if slope_imbalance > 0.3:
                raw_score += 4
                raw_conf += 5
            elif slope_imbalance < -0.3:
                raw_score -= 4
                raw_conf += 5
            
            # Impact asymmetry
            if impact_asymmetry > 0.2:
                raw_score -= 3
            elif impact_asymmetry < -0.2:
                raw_score += 3
            
            # Effective imbalance
            if abs(effective_imbalance) > 0.3:
                raw_score += effective_imbalance * 5
                raw_conf += 3
            
            # Top 5 ratio
            if top5_ratio > 1.5:
                raw_score += 3
            elif top5_ratio < 0.67:
                raw_score -= 3
            
            # Size imbalance
            if abs(size_imbalance) > 0.3:
                raw_score += size_imbalance * 3
            
            # Spread tightness
            if spread_pct < 0.01:
                raw_conf += 5
            elif spread_pct > 0.05:
                raw_conf -= 5
            
            # Liquidation cascade risk
            avg_long_leverage = 10.0
            avg_short_leverage = 8.0
            long_liq_price = mid_price * (1 - 0.9 / avg_long_leverage)
            short_liq_price = mid_price * (1 + 0.9 / avg_short_leverage)
            
            cascade_risk = 0.0
            near_long_liq = [p for p, v in large_bids if abs(p - long_liq_price) / long_liq_price < 0.02]
            near_short_liq = [p for p, v in large_asks if abs(p - short_liq_price) / short_liq_price < 0.02]
            
            if near_long_liq and large_bid_volume > 50:
                cascade_risk += 5
            if near_short_liq and large_ask_volume > 50:
                cascade_risk -= 5
            
            raw_score += cascade_risk
            
            # === Spoofing penalty（修复死代码） ===
            spoofing_penalty = 0.0
            if spoofing_analysis.get("detected"):
                spoofing_penalty = -5
                raw_score += spoofing_penalty  # 作用到分数
                raw_conf -= 10
            
            raw_conf = min(80, raw_conf + 20)
            
            # ========================================
            # 第二层：计算 quality (加权风险)
            # ========================================
            # 权重：spoof_risk=0.45, spread_risk=0.2, impact_risk=0.2, depth_risk=0.15
            quality_weights = {
                "spoof_risk": 0.45,
                "spread_risk": 0.20,
                "impact_risk": 0.20,
                "depth_risk": 0.15,
            }
            
            weighted_risk = sum(
                quality_metrics[k] * quality_weights[k] 
                for k in quality_metrics
            )
            quality = max(0.0, min(1.0, 1.0 - weighted_risk))
            
            # ========================================
            # 第三层：质量门控
            # ========================================
            # gated_score = 50 + (raw_score - 50) * quality
            # gated_conf = raw_conf * (0.4 + 0.6 * quality)
            gated_score = 50 + (raw_score - 50) * quality
            gated_conf = raw_conf * (0.4 + 0.6 * quality)
            
            # === 硬门槛判断 ===
            hard_hold_candidate = False
            gate_reason = None
            # 交易成本硬门槛：价差或冲击过高时直接禁开，避免成本吞噬信号
            slippage_proxy_pct = max(spread_pct, avg_impact * 100)
            
            if quality < 0.55:
                hard_hold_candidate = True
                gate_reason = f"quality={quality:.2f}<0.55"
            elif slippage_proxy_pct > 0.10:
                hard_hold_candidate = True
                gate_reason = f"slippage_proxy={slippage_proxy_pct:.3f}%>0.10%"
            
            # Direction（受硬门槛影响）
            if hard_hold_candidate:
                direction = "neutral"
            else:
                direction = self._direction(gated_score)
            
            # Details
            details = {
                # 原始指标
                "bid_volume": round(bid_volume, 2),
                "ask_volume": round(ask_volume, 2),
                "imbalance_pct": round(imbalance * 100, 2),
                "large_bids": len(large_bids),
                "large_asks": len(large_asks),
                "large_imbalance_pct": round(large_imbalance * 100, 2),
                "spread_usd": round(spread, 2),
                "slippage_proxy_pct": round(slippage_proxy_pct, 4),
                "pressure_imbalance_pct": round(pressure_imbalance * 100, 2),
                "slope_imbalance_pct": round(slope_imbalance * 100, 1),
                "impact_asymmetry_pct": round(impact_asymmetry * 100, 1),
                "effective_imbalance_pct": round(effective_imbalance * 100, 1),
                "top5_ratio": round(top5_ratio, 2),
                "size_imbalance_pct": round(size_imbalance * 100, 1),
                # Spoofing
                "spoofing_detected": spoofing_analysis.get("detected", False),
                "spoofing_penalty": spoofing_penalty,
                "suspicious_bids": spoofing_analysis.get("suspicious_bids", 0),
                "suspicious_asks": spoofing_analysis.get("suspicious_asks", 0),
                "avg_credibility": round(spoofing_analysis.get("avg_credibility", 1.0), 2),
                # 质量指标
                "quality": round(quality, 3),
                "quality_metrics": {k: round(v, 3) for k, v in quality_metrics.items()},
                "raw_score": round(raw_score, 2),
                "gated_score": round(gated_score, 2),
                "raw_conf": round(raw_conf, 2),
                "gated_conf": round(gated_conf, 2),
                # 硬门槛
                "hard_hold_candidate": hard_hold_candidate,
                "gate_reason": gate_reason,
                # Liquidation
                "long_liq_price": round(long_liq_price, 2),
                "short_liq_price": round(short_liq_price, 2),
                "cascade_risk": round(cascade_risk, 2),
            }
            
            return self._factor("liquidity_depth", gated_score, direction, gated_conf, details)
            
        except Exception as e:
            return self._factor("liquidity_depth", 50, "neutral", 20, {
                "error": str(e)[:100], 
                "quality": 0,
                "hard_hold_candidate": True,
                "gate_reason": "orderbook_unavailable"
            })

    def order_flow_persistence(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """
        Order Flow Persistence Factor - 分析买卖压力持续性
        
        数据源: snap.taker_volume.data (5m周期)
        
        计算:
        - 15m/30m/60m 维度的买卖力量持续性和一致性
        - 检测趋势性和反转信号
        - 量价背离检测
        
        输出: 
        - raw_score: 原始因子分
        - raw_conf: 原始置信度
        - quality: 数据质量(0~1)
        - details: 详细指标
        """
        tv_data = snap.get("taker_volume") or {}
        rows = tv_data.get("data", [])
        
        if not rows or len(rows) < 12:  # 至少需要12个5m周期(1小时)
            return self._factor("order_flow_persistence", 50, "neutral", 20, {
                "missing": True,
                "quality": 0,
                "reason": "insufficient_taker_volume_data",
                "hard_hold_candidate": True,
                "gate_reason": "data_unavailable"
            })
        
        try:
            # 解析数据: [timestamp, buy_vol_usd, sell_vol_usd]
            parsed = []
            for row in rows[:24]:  # 最近2小时
                if len(row) >= 3:
                    parsed.append({
                        "ts": sfloat(row[0], 0),
                        "buy": sfloat(row[1], 0),
                        "sell": sfloat(row[2], 0),
                    })
            
            if len(parsed) < 12:
                return self._factor("order_flow_persistence", 50, "neutral", 20, {
                    "missing": True,
                    "quality": 0,
                    "hard_hold_candidate": True,
                    "gate_reason": "insufficient_parsed_data"
                })
            
            # === 多时间框架分析 ===
            def analyze_period(data, periods, name):
                """分析指定时间窗口的买卖压力"""
                subset = data[:periods]
                total_buy = sum(d["buy"] for d in subset)
                total_sell = sum(d["sell"] for d in subset)
                total = total_buy + total_sell
                
                if total < 1e6:  # 小于100万USD
                    return None

                # 强度门槛：机构订单流通常要求显著失衡，避免拆单噪音误判
                intensity = abs(total_buy - total_sell) / max(total, 1e-9)
                if intensity < 0.20:
                    return None
                
                # 买卖比
                ratio = total_buy / max(1e-9, total_sell)
                
                # 持续性：连续同方向的比例
                directions = []
                for d in subset:
                    if d["buy"] > d["sell"] * 1.2:
                        directions.append(1)  # 买方优势
                    elif d["sell"] > d["buy"] * 1.2:
                        directions.append(-1)  # 卖方优势
                    else:
                        directions.append(0)  # 均衡
                
                # 计算连续性和一致性
                consecutive = 0
                max_consecutive = 0
                for i in range(len(directions)):
                    if directions[i] == directions[i-1] if i > 0 else False:
                        consecutive += 1
                        max_consecutive = max(max_consecutive, consecutive)
                    else:
                        consecutive = 1
                
                consistency = sum(1 for d in directions if d != 0) / len(directions)
                
                # 量价背离：买卖比与连续性的方向是否一致
                expected_dir = 1 if ratio > 1 else -1 if ratio < 1 else 0
                actual_dir = directions[0] if directions else 0
                divergence = (expected_dir != actual_dir) and (abs(ratio - 1) > 0.1)
                
                return {
                    "period": name,
                    "ratio": ratio,
                    "total_vol": total,
                    "max_consecutive": max_consecutive,
                    "consistency": consistency,
                    "divergence": divergence,
                    "direction": expected_dir,
                    "intensity": intensity,
                }
            
            # 分析多个时间框架
            periods_analysis = {}
            for periods, name in [(3, "15m"), (6, "30m"), (12, "60m")]:
                result = analyze_period(parsed, periods, name)
                if result:
                    periods_analysis[name] = result
            
            if not periods_analysis:
                return self._factor("order_flow_persistence", 50, "neutral", 25, {
                    "quality": 0.5,
                    "hard_hold_candidate": True,
                    "gate_reason": "no_valid_period_analysis"
                })
            
            # === 综合评分 ===
            raw_score = 50.0
            raw_conf = 30.0
            signals = []
            
            # 检查多时间框架一致性
            directions_15m = periods_analysis.get("15m", {}).get("direction", 0)
            directions_30m = periods_analysis.get("30m", {}).get("direction", 0)
            directions_60m = periods_analysis.get("60m", {}).get("direction", 0)
            
            # 三个时间框架方向一致
            if directions_15m == directions_30m == directions_60m and directions_15m != 0:
                raw_score += directions_15m * 15
                raw_conf += 20
                signals.append(f"triple_alignment_{directions_15m}")
            
            # 两个时间框架方向一致
            elif (directions_15m == directions_30m != 0) or (directions_30m == directions_60m != 0):
                raw_score += directions_30m * 10
                raw_conf += 12
                signals.append(f"double_alignment")
            
            # 持续性信号
            for name, analysis in periods_analysis.items():
                if analysis["max_consecutive"] >= 4:
                    raw_score += analysis["direction"] * 8
                    raw_conf += 10
                    signals.append(f"{name}_persistent")
                
                # 量价背离
                if analysis["divergence"]:
                    raw_score -= analysis["direction"] * 5  # 反向调整
                    raw_conf -= 5
                    signals.append(f"{name}_divergence")
            
            # 计算质量
            quality = 0.7
            if len(periods_analysis) == 3:
                quality = 1.0
            elif len(periods_analysis) == 2:
                quality = 0.85
            
            # 应用质量门控
            gated_score = 50 + (raw_score - 50) * quality
            gated_conf = raw_conf * (0.5 + 0.5 * quality)
            
            # 硬门槛
            hard_hold_candidate = False
            gate_reason = None
            if quality < 0.6:
                hard_hold_candidate = True
                gate_reason = f"quality={quality:.2f}<0.6"
            
            direction = self._direction(gated_score) if not hard_hold_candidate else "neutral"
            
            details = {
                "periods_analysis": {k: {kk: round(vv, 3) if isinstance(vv, float) else vv 
                                        for kk, vv in v.items()} 
                                    for k, v in periods_analysis.items()},
                "signals": signals,
                "quality": round(quality, 3),
                "raw_score": round(raw_score, 2),
                "gated_score": round(gated_score, 2),
                "hard_hold_candidate": hard_hold_candidate,
                "gate_reason": gate_reason,
            }
            
            return self._factor("order_flow_persistence", gated_score, direction, gated_conf, details)
            
        except Exception as e:
            return self._factor("order_flow_persistence", 50, "neutral", 20, {
                "error": str(e)[:100],
                "quality": 0,
                "hard_hold_candidate": True,
                "gate_reason": "exception"
            })

    def liquidation_pressure(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """
        Liquidation Pressure Factor - 分析清算压力
        
        数据源:
        - liquidity_depth.details.long_liq_price/short_liq_price
        - 估算的清算订单簿
        
        计算:
        - 多空清算价位距离当前价格的百分比
        - 清算量估算（基于杠杆分布假设）
        - 级联清算风险评估
        
        输出:
        - raw_score: 原始因子分
        - raw_conf: 原始置信度
        - quality: 数据质量(0~1)
        - cascade_risk: 级联清算风险等级
        """
        # 获取流动性深度因子数据
        liq_depth = snap.get("factors", {}).get("liquidity_depth", {}) if isinstance(snap.get("factors"), dict) else {}
        
        # 如果没有预先计算的因子，尝试从snap直接计算
        if not liq_depth or not isinstance(liq_depth, dict):
            # 从流动性深度因子获取数据
            liq_factor = self.liquidity_depth(snap) if hasattr(self, 'liquidity_depth') else {}
            liq_details = liq_factor.get("details", {})
        else:
            liq_details = liq_depth.get("details", {})
        
        # 获取当前价格
        spot = snap.get("spot") or {}
        current_price = sfloat(spot.get("price"), 0)
        
        if current_price <= 0:
            return self._factor("liquidation_pressure", 50, "neutral", 20, {
                "missing": True,
                "quality": 0,
                "hard_hold_candidate": True,
                "gate_reason": "price_unavailable"
            })
        
        try:
            # 获取清算价位
            long_liq_price = sfloat(liq_details.get("long_liq_price"), current_price * 0.9)
            short_liq_price = sfloat(liq_details.get("short_liq_price"), current_price * 1.1)
            
            # 计算清算价位距离
            long_liq_dist_pct = (current_price - long_liq_price) / current_price * 100 if long_liq_price > 0 else 0
            short_liq_dist_pct = (short_liq_price - current_price) / current_price * 100 if short_liq_price > 0 else 0
            
            # 获取订单簿不平衡
            imbalance = sfloat(liq_details.get("imbalance_pct"), 0) / 100
            
            # 估算清算量（假设杠杆分布）
            # 假设平均杠杆为10x，清算阈值为90%保证金率
            # 清算价位 ≈ 入场价 * (1 - 0.9/杠杆)
            avg_leverage_long = 10
            avg_leverage_short = 8
            
            # 估算：距离清算价位越近，潜在清算量越大
            # 使用sigmoid函数估算清算压力
            def sigmoid(x):
                return 1 / (1 + math.exp(-x))
            
            # 清算压力：距离越近，压力越大
            # 阈值：5%为高压，10%为中等
            long_liq_pressure = sigmoid((10 - long_liq_dist_pct) / 3) if long_liq_dist_pct > 0 else 0
            short_liq_pressure = sigmoid((10 - short_liq_dist_pct) / 3) if short_liq_dist_pct > 0 else 0
            
            # 级联清算风险
            # 当价格接近清算价位且订单簿不平衡时，级联风险高
            cascade_long = long_liq_pressure * (1 - imbalance) if imbalance < 0 else long_liq_pressure * 0.5
            cascade_short = short_liq_pressure * (1 + imbalance) if imbalance > 0 else short_liq_pressure * 0.5

            # 近清算区放大：距离<3%时，级联效应通常非线性增强
            if 0 < long_liq_dist_pct < 3:
                cascade_long *= 1.3
            if 0 < short_liq_dist_pct < 3:
                cascade_short *= 1.3
            
            cascade_risk = max(cascade_long, cascade_short)
            
            # === 评分逻辑 ===
            raw_score = 50.0
            raw_conf = 30.0
            
            # 多头清算压力大 -> 做空信号（价格下跌压力大）
            if long_liq_pressure > 0.6:
                raw_score -= 12
                raw_conf += 15
            elif long_liq_pressure > 0.4:
                raw_score -= 6
                raw_conf += 8
            
            # 空头清算压力大 -> 做多信号（价格上涨压力大，逼空）
            if short_liq_pressure > 0.6:
                raw_score += 12
                raw_conf += 15
            elif short_liq_pressure > 0.4:
                raw_score += 6
                raw_conf += 8
            
            # 级联清算风险调整
            if cascade_risk > 0.7:
                raw_conf += 10  # 高置信度
                # 级联方向：取决于哪边更可能被清算
                if cascade_long > cascade_short:
                    raw_score -= 8  # 多头爆仓 -> 价格进一步下跌
                else:
                    raw_score += 8  # 空头爆仓 -> 价格进一步上涨
            
            # 计算质量
            quality = 1.0 if long_liq_price > 0 and short_liq_price > 0 else 0.6
            
            # 应用质量门控
            gated_score = 50 + (raw_score - 50) * quality
            gated_conf = raw_conf * (0.5 + 0.5 * quality)
            
            # 硬门槛
            hard_hold_candidate = False
            gate_reason = None
            if quality < 0.5:
                hard_hold_candidate = True
                gate_reason = f"quality={quality:.2f}<0.5"
            
            # 级联风险等级
            if cascade_risk > 0.7:
                cascade_level = "extreme"
            elif cascade_risk > 0.5:
                cascade_level = "high"
            elif cascade_risk > 0.3:
                cascade_level = "medium"
            else:
                cascade_level = "low"
            
            direction = self._direction(gated_score) if not hard_hold_candidate else "neutral"
            
            details = {
                "long_liq_price": round(long_liq_price, 2),
                "short_liq_price": round(short_liq_price, 2),
                "long_liq_dist_pct": round(long_liq_dist_pct, 2),
                "short_liq_dist_pct": round(short_liq_dist_pct, 2),
                "long_liq_pressure": round(long_liq_pressure, 3),
                "short_liq_pressure": round(short_liq_pressure, 3),
                "cascade_risk": round(cascade_risk, 3),
                "cascade_level": cascade_level,
                "quality": round(quality, 3),
                "raw_score": round(raw_score, 2),
                "gated_score": round(gated_score, 2),
                "hard_hold_candidate": hard_hold_candidate,
                "gate_reason": gate_reason,
            }
            
            return self._factor("liquidation_pressure", gated_score, direction, gated_conf, details)
            
        except Exception as e:
            return self._factor("liquidation_pressure", 50, "neutral", 20, {
                "error": str(e)[:100],
                "quality": 0,
                "hard_hold_candidate": True,
                "gate_reason": "exception"
            })

    def funding_basis_divergence(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        """
        Funding Basis Divergence Factor - 分析资金费率与基差背离
        
        数据源:
        - funding_rate: snap.funding.funding_rate
        - basis: 现货与合约价差
        
        计算:
        - funding_bps: 资金费率(基点)
        - basis_bps: 基差(基点)
        - funding_z: 资金费率z-score
        - basis_z: 基差z-score
        - same_sign: 是否同向（背离信号）
        
        逻辑:
        - funding_rate上升 + basis下降 = 背离（多头过度拥挤 -> 做空）
        - funding_rate下降 + basis上升 = 背离（空头过度拥挤 -> 做多）
        - 同向 = 趋势确认
        
        输出:
        - raw_score: 原始因子分
        - raw_conf: 原始置信度
        - quality: 数据质量(0~1)
        - divergence_type: 背离类型
        """
        # 获取资金费率
        funding_data = snap.get("funding") or {}
        funding_rate = sfloat(funding_data.get("funding_rate") or funding_data.get("rate"), 0)
        
        # 获取现货和合约价格
        spot = snap.get("spot") or {}
        current_price = sfloat(spot.get("price"), 0)
        swap = snap.get("swap") or {}
        swap_price = sfloat(swap.get("price"), 0)
        premium_data = snap.get("premium_index") or {}
        premium_index = sfloat(premium_data.get("premium") or premium_data.get("premiumIndex"), 0)
        
        # 获取合约价格（从OI或其他来源）
        oi_data = snap.get("oi") or {}
        # 优先使用真实swap-spot价差；若缺失则回退到premium index
        basis_source = "swap_spot"
        
        if current_price <= 0:
            return self._factor("funding_basis_divergence", 50, "neutral", 20, {
                "missing": True,
                "quality": 0,
                "hard_hold_candidate": True,
                "gate_reason": "price_unavailable"
            })
        
        try:
            if swap_price > 0 and current_price > 0:
                basis_pct = (swap_price - current_price) / current_price
            elif abs(premium_index) > 0:
                basis_pct = premium_index
                basis_source = "premium_index_fallback"
            else:
                basis_pct = 0.0
                basis_source = "missing"
            
            # 转换为基点
            funding_bps = funding_rate * 10000  # 例如 0.01% = 1 bps
            basis_bps = basis_pct * 10000
            
            # 计算z-score（相对于历史均值）
            # 假设历史均值：funding ≈ 0.01%, basis ≈ 0
            funding_mean = 0.01 / 100  # 0.01%
            funding_std = 0.02 / 100   # 0.02%
            
            basis_mean = 0
            basis_std = 0.0001  # 0.01%
            
            funding_z = (funding_rate - funding_mean) / max(funding_std, 1e-9)
            basis_z = (basis_pct - basis_mean) / max(basis_std, 1e-9)
            
            # 判断同向/背离
            # 同向：funding>0且basis>0，或funding<0且basis<0
            # 背离：funding>0但basis<0，或funding<0但basis>0
            same_sign = (funding_rate * basis_pct) > 0
            
            # === 评分逻辑 ===
            raw_score = 50.0
            raw_conf = 30.0
            divergence_type = "none"
            
            # 高资金费率（多头付费）
            if funding_rate > 0.0005:  # > 0.05%
                # 多头过度拥挤
                raw_score -= 10
                raw_conf += 10
                divergence_type = "high_funding_long_crowded"
            
            elif funding_rate > 0.0003:  # > 0.03%
                raw_score -= 5
                raw_conf += 5
                divergence_type = "moderate_funding_long"
            
            # 负资金费率（空头付费）
            elif funding_rate < -0.0005:  # < -0.05%
                # 空头过度拥挤
                raw_score += 10
                raw_conf += 10
                divergence_type = "low_funding_short_crowded"
            
            elif funding_rate < -0.0003:  # < -0.03%
                raw_score += 5
                raw_conf += 5
                divergence_type = "moderate_funding_short"
            
            # 背离信号（如果有真实基差数据）
            if not same_sign and abs(basis_pct) > 0.0001:
                # funding_rate 与 basis 背离
                if funding_rate > 0 and basis_pct < 0:
                    # 资金费率高但基差为负 -> 多头更激进 -> 潜在反转
                    raw_score -= 8
                    raw_conf += 12
                    divergence_type = "funding_basis_divergence_bearish"
                elif funding_rate < 0 and basis_pct > 0:
                    # 资金费率低但基差为正 -> 空头更激进 -> 潜在反转
                    raw_score += 8
                    raw_conf += 12
                    divergence_type = "funding_basis_divergence_bullish"
            
            # 极端资金费率
            if abs(funding_z) > 2:
                raw_conf += 15
                if funding_z > 2:
                    raw_score -= 5
                else:
                    raw_score += 5
            
            # 计算质量
            quality = 1.0 if (abs(funding_rate) > 0 and basis_source != "missing") else 0.6
            
            # 应用质量门控
            gated_score = 50 + (raw_score - 50) * quality
            gated_conf = raw_conf * (0.5 + 0.5 * quality)
            
            # 硬门槛
            hard_hold_candidate = False
            gate_reason = None
            if quality < 0.4:
                hard_hold_candidate = True
                gate_reason = f"quality={quality:.2f}<0.4"
            
            direction = self._direction(gated_score) if not hard_hold_candidate else "neutral"
            
            details = {
                "funding_rate_pct": round(funding_rate * 100, 4),
                "funding_bps": round(funding_bps, 2),
                "basis_pct": round(basis_pct * 100, 4),
                "basis_bps": round(basis_bps, 2),
                "basis_source": basis_source,
                "swap_price": round(swap_price, 2) if swap_price > 0 else None,
                "funding_z": round(funding_z, 3),
                "basis_z": round(basis_z, 3),
                "same_sign": same_sign,
                "divergence_type": divergence_type,
                "quality": round(quality, 3),
                "raw_score": round(raw_score, 2),
                "gated_score": round(gated_score, 2),
                "hard_hold_candidate": hard_hold_candidate,
                "gate_reason": gate_reason,
            }
            
            return self._factor("funding_basis_divergence", gated_score, direction, gated_conf, details)
            
        except Exception as e:
            return self._factor("funding_basis_divergence", 50, "neutral", 20, {
                "error": str(e)[:100],
                "quality": 0,
                "hard_hold_candidate": True,
                "gate_reason": "exception"
            })

    def evaluate(self, snap: Dict[str, Any], weights: Dict[str, float]) -> Dict[str, Any]:
        base = super().evaluate(snap, weights)
        
        # 先计算流动性深度，因为liquidation_pressure依赖它
        liquidity_depth_result = self.liquidity_depth(snap)
        
        extra = {
            # 新因子（高权重）
            "order_flow_persistence": self.order_flow_persistence(snap),
            "liquidity_depth": liquidity_depth_result,
            "liquidation_pressure": self.liquidation_pressure(snap),
            "funding_basis_divergence": self.funding_basis_divergence(snap),
            # 原有因子
            "oi_delta": self.oi_delta(snap),
            "taker_volume": self.taker_volume(snap),
            "breakout": self.breakout(snap),
            "mean_reversion": self.mean_reversion(snap),
            "technical_kline": self.technical_kline(snap),
            "momentum": self.momentum(snap),
            # 保留但权重=0的因子
            "kline_pattern": self.kline_pattern(snap),
            "strategy_history": self.strategy_history(snap),
            "asr_vc": self.asr_vc(snap),
        }
        
        # Add Stock Market Pro factors if available
        if STOCK_MARKET_PRO_AVAILABLE:
            try:
                extra["cross_market_correlation"] = compute_cross_market_factor()
                extra["macro_risk_sentiment"] = compute_macro_risk_factor()
                extra["enhanced_technical"] = compute_enhanced_technical_factor()
            except Exception as e:
                # If Stock Market Pro fails, use neutral factors
                extra["cross_market_correlation"] = {"name": "cross_market_correlation", "score": 50, "direction": "neutral", "confidence": 20, "details": {"error": str(e)[:100]}}
                extra["macro_risk_sentiment"] = {"name": "macro_risk_sentiment", "score": 50, "direction": "neutral", "confidence": 20, "details": {"error": str(e)[:100]}}
                extra["enhanced_technical"] = {"name": "enhanced_technical", "score": 50, "direction": "neutral", "confidence": 20, "details": {"error": str(e)[:100]}}
        
        # Add liquidation heatmap factor
        try:
            current_price = snap.get("lastPrice", 85000)
            extra["liquidation_heatmap"] = compute_liquidation_heatmap_factor(current_price)
        except Exception as e:
            extra["liquidation_heatmap"] = {"name": "liquidation_heatmap", "score": 50, "direction": "neutral", "confidence": 20, "details": {"error": str(e)[:100]}}
        
        factors = dict(base.get("factors", {}))
        factors.update(extra)

        # --- 结构化叠加层 ---
        # 目标不是“更多因子一起投票”，而是先判断 BTC 大环境，再看趋势/订单流/拥挤度是否允许出手。
        def ema_last(values: List[float], period: int) -> float:
            vals = [sfloat(x, 0.0) for x in values if sfloat(x, 0.0) > 0]
            if not vals:
                return 0.0
            alpha = 2.0 / (period + 1.0)
            prev = vals[0]
            for v in vals[1:]:
                prev = alpha * v + (1.0 - alpha) * prev
            return prev

        def combine_scores(mapping: Dict[str, float]) -> tuple[float, float]:
            tw = 0.0
            sc = 0.0
            cf = 0.0
            for fname, w in mapping.items():
                f = factors.get(fname)
                if not isinstance(f, dict):
                    continue
                tw += abs(w)
                sc += sfloat(f.get("score"), 50.0) * w
                cf += sfloat(f.get("confidence"), 45.0) * abs(w)
            if tw <= 1e-9:
                return 50.0, 35.0
            return sc / tw, cf / tw

        price = sfloat((snap.get("spot") or {}).get("price"), 0.0)
        h1 = (snap.get("kline") or {}).get("h1") or []
        d1 = (snap.get("kline") or {}).get("d1") or []
        h1_closes = [sfloat(x.get("c"), 0.0) for x in h1 if sfloat(x.get("c"), 0.0) > 0]
        d1_closes = [sfloat(x.get("c"), 0.0) for x in d1 if sfloat(x.get("c"), 0.0) > 0]
        benchmark_score = 50.0
        benchmark_conf = 42.0
        ema20_h1 = ema_last(h1_closes[-160:], 20) if h1_closes else 0.0
        ema60_h1 = ema_last(h1_closes[-240:], 60) if h1_closes else 0.0
        ema30_d1 = ema_last(d1_closes[-200:], 30) if d1_closes else 0.0
        ema120_d1 = ema_last(d1_closes[-260:], 120) if d1_closes else 0.0
        d1_ret_5 = 0.0
        if len(d1_closes) >= 6 and d1_closes[-6] > 0:
            d1_ret_5 = (d1_closes[-1] / d1_closes[-6] - 1.0) * 100.0

        if price > 0 and ema20_h1 > 0:
            benchmark_score += 4 if price >= ema20_h1 else -4
        if ema20_h1 > 0 and ema60_h1 > 0:
            benchmark_score += 7 if ema20_h1 >= ema60_h1 else -7
            benchmark_conf += 5
        if ema30_d1 > 0 and ema120_d1 > 0:
            benchmark_score += 8 if ema30_d1 >= ema120_d1 else -8
            benchmark_conf += 8
        if price > 0 and ema120_d1 > 0:
            benchmark_score += 7 if price >= ema120_d1 else -7
        if d1_ret_5 >= 2.5:
            benchmark_score += 6
        elif d1_ret_5 >= 0.8:
            benchmark_score += 3
        elif d1_ret_5 <= -2.5:
            benchmark_score -= 6
        elif d1_ret_5 <= -0.8:
            benchmark_score -= 3
        benchmark_score = clamp(benchmark_score, 0, 100)
        benchmark_conf = clamp(benchmark_conf, 0, 100)

        trend_score, trend_conf = combine_scores({
            "momentum": 0.12,
            "breakout": 0.26,
            "technical_kline": 0.18,
            "kline_pattern": 0.08,
            "enhanced_technical": 0.18,
            "asr_vc": 0.18,
        })
        flow_score, flow_conf = combine_scores({
            "order_flow_persistence": 0.24,
            "liquidity_depth": 0.14,
            "liquidation_pressure": 0.22,
            "oi_delta": 0.18,
            "taker_volume": 0.22,
        })
        carry_score, carry_conf = combine_scores({
            "funding_basis_divergence": 0.36,
            "funding": 0.18,
            "basis": 0.18,
            "long_short": 0.14,
            "market_regime": 0.14,
        })
        risk_score, risk_conf = combine_scores({
            "volatility_regime": 0.20,
            "cross_market_correlation": 0.20,
            "macro_risk_sentiment": 0.28,
            "fear_greed": 0.14,
            "strategy_history": 0.18,
        })

        synth = {
            "benchmark_regime": self._factor(
                "benchmark_regime",
                benchmark_score,
                self._direction(benchmark_score, up=54, down=46),
                benchmark_conf,
                {
                    "price": round(price, 2),
                    "ema20_h1": round(ema20_h1, 2),
                    "ema60_h1": round(ema60_h1, 2),
                    "ema30_d1": round(ema30_d1, 2),
                    "ema120_d1": round(ema120_d1, 2),
                    "d1_ret_5_pct": round(d1_ret_5, 2),
                    "allow_long": benchmark_score >= 58,
                    "allow_short": benchmark_score <= 40,
                    "regime": "bull" if benchmark_score >= 58 else "bear" if benchmark_score <= 40 else "neutral",
                },
            ),
            "trend_stack": self._factor(
                "trend_stack",
                trend_score,
                self._direction(trend_score, up=54, down=46),
                trend_conf,
                {
                    "explanation": "把动量、突破、K线结构、增强技术面合并为一个趋势簇，避免单一指标误触发。",
                    "components": ["momentum", "breakout", "technical_kline", "kline_pattern", "enhanced_technical", "asr_vc"],
                },
            ),
            "flow_impulse": self._factor(
                "flow_impulse",
                flow_score,
                self._direction(flow_score, up=54, down=46),
                flow_conf,
                {
                    "explanation": "把订单流持续性、主动买卖、OI变化、爆仓压力和盘口深度合并，判断是否真的有资金推动。",
                    "components": ["order_flow_persistence", "liquidity_depth", "liquidation_pressure", "oi_delta", "taker_volume"],
                },
            ),
            "carry_regime": self._factor(
                "carry_regime",
                carry_score,
                self._direction(carry_score, up=53, down=47),
                carry_conf,
                {
                    "explanation": "把资金费率、基差、多空比与拥挤度合并，判断做多/做空是否已经过热。",
                    "components": ["funding_basis_divergence", "funding", "basis", "long_short", "market_regime"],
                },
            ),
            "risk_pressure": self._factor(
                "risk_pressure",
                risk_score,
                self._direction(risk_score, up=53, down=47),
                risk_conf,
                {
                    "explanation": "把宏观风险、跨市场联动、波动率与策略自身历史表现合并，用来决定风险预算。",
                    "components": ["volatility_regime", "cross_market_correlation", "macro_risk_sentiment", "fear_greed", "strategy_history"],
                },
            ),
        }
        factors.update(synth)
        
        # 筛选出有效权重的因子（active factors）
        active_factors = {name: f for name, f in factors.items() if sfloat(weights.get(name), 0) > 0}
        
        # 按 active factors 计算 score/confidence
        tw = 0.0
        sc = 0.0
        cf = 0.0
        for name, f in active_factors.items():
            w = sfloat(weights.get(name), 0)
            tw += w
            sc += f["score"] * w
            cf += f["confidence"] * w
        raw_score = sc / tw if tw else 50.0
        raw_conf = cf / tw if tw else 50.0

        synth_weights = {
            "benchmark_regime": 0.32,
            "trend_stack": 0.25,
            "flow_impulse": 0.21,
            "carry_regime": 0.14,
            "risk_pressure": 0.08,
        }
        synth_score, synth_conf = combine_scores(synth_weights)
        score = clamp(raw_score * 0.42 + synth_score * 0.58, 0, 100)
        conf = clamp(raw_conf * 0.45 + synth_conf * 0.55, 0, 100)
        
        # bullish/bearish 也只用 active factors
        bullish = sum(1 for f in active_factors.values() if f["direction"] == "bullish")
        bearish = sum(1 for f in active_factors.values() if f["direction"] == "bearish")
        align = abs(bullish - bearish) / max(1, len(active_factors))
        conf = clamp(conf + align * 10, 0, 100)
        
        benchmark_context = {
            "score": round(benchmark_score, 2),
            "regime": "bull" if benchmark_score >= 58 else "bear" if benchmark_score <= 40 else "neutral",
            "allow_long": benchmark_score >= 58 and bool(price > 0 and ema20_h1 > 0 and price >= ema20_h1),
            "allow_short": benchmark_score <= 40 and bool(price > 0 and ema20_h1 > 0 and price <= ema20_h1),
            "prefer_long_only": benchmark_score >= 62,
            "price_above_h1_trend": bool(price > 0 and ema20_h1 > 0 and price >= ema20_h1),
            "price_above_d1_trend": bool(price > 0 and ema120_d1 > 0 and price >= ema120_d1),
            "d1_ret_5_pct": round(d1_ret_5, 2),
        }

        active_mode = "neutral_cash"
        preferred_decision = "hold"
        if (
            benchmark_score >= 58
            and trend_score >= 54
            and flow_score >= 51
            and benchmark_context["price_above_h1_trend"]
            and benchmark_context["price_above_d1_trend"]
        ):
            active_mode = "bull_breakout"
            preferred_decision = "long"
        elif (
            benchmark_score <= 40
            and trend_score <= 46
            and flow_score <= 45
            and not benchmark_context["price_above_h1_trend"]
            and carry_score <= 48
        ):
            active_mode = "bear_tactical"
            preferred_decision = "short"

        if active_mode == "bull_breakout":
            stance = "当前进入牛市顺势模式，只做顺趋势做多，优先保护已经形成的盈利。"
        elif active_mode == "bear_tactical":
            stance = "当前进入熊市战术空头模式，只做更短、更克制的空单，不把空头当作长期主线。"
        else:
            stance = "当前进入中性现金模式，宁可等待，也不为了完成交易次数去硬开仓。"

        risk_budget = (
            "进攻"
            if active_mode == "bull_breakout" and benchmark_score >= 62 and risk_score >= 52
            else "标准"
            if active_mode == "bear_tactical"
            else "防守"
        )

        factor_groups = {
            "main": [
                "benchmark_regime",
                "trend_stack",
                "flow_impulse",
                "carry_regime",
                "order_flow_persistence",
                "liquidity_depth",
                "liquidation_pressure",
                "funding_basis_divergence",
                "oi_delta",
                "taker_volume",
                "breakout",
            ],
            "aux": [
                "risk_pressure",
                "momentum",
                "mean_reversion",
                "technical_kline",
                "kline_pattern",
                "asr_vc",
                "strategy_history",
                "cross_market_correlation",
                "macro_risk_sentiment",
                "enhanced_technical",
                "funding",
                "basis",
                "long_short",
                "fear_greed",
                "market_regime",
                "volatility_regime",
            ],
        }

        strategy_advisor = {
            "profile": "regime_balance_v4",
            "version": "v4.1.0-regime-balance-calibrated",
            "active_mode": active_mode,
            "preferred_decision": preferred_decision,
            "risk_budget": risk_budget,
            "stance": stance,
            "cluster_scores": {
                "benchmark_regime": round(benchmark_score, 2),
                "trend_stack": round(trend_score, 2),
                "flow_impulse": round(flow_score, 2),
                "carry_regime": round(carry_score, 2),
                "risk_pressure": round(risk_score, 2),
            },
            "advice": [
                "先判定当前属于牛市顺势、中性观望，还是熊市战术空头。",
                "只有 BTC 基准、趋势簇、订单流簇同时站到同一边，才允许开仓。",
                "v4.1 会更克制地过滤掉强趋势衰减后的追单，先保护曲线，再追求更高频率。",
                "空头只作为战术工具，不把做空当作长期收益来源。",
                "最近结构不清晰时，空仓本身就是一种决策。",
            ],
        }

        return {
            "score": round(score, 2), 
            "confidence": round(conf, 2), 
            "bullish_factors": bullish, 
            "bearish_factors": bearish, 
            "factor_count": len(factors),  # 总因子数（兼容面板）
            "active_factor_count": len(active_factors),  # 真实参与决策的因子数
            "raw_score": round(raw_score, 2),
            "raw_confidence": round(raw_conf, 2),
            "factors": factors,
            "benchmark_context": benchmark_context,
            "factor_groups": factor_groups,
            "strategy_advisor": strategy_advisor,
        }


class TraderV31(TraderV3):
    def __init__(self):
        super().__init__()
        self.feed = DataFeedV31(self.logger)
        self.risk = RiskEngine(CFG)
        self.exec = Executor(self.logger)
        self.adaptive = load_adaptive()
        self._apply_adaptive()
        # 加载持久化的因子权重
        saved_weights = load_factor_weights()
        if saved_weights:
            CFG["weights"].update(saved_weights)
            self.logger.log(f"[WEIGHTS_LOADED] Loaded {len(saved_weights)} persisted factor weights")

    def _apply_adaptive(self) -> None:
        CFG["min_trade_score"] = int(BASE_THRESHOLDS["min_trade_score"] + sfloat(self.adaptive.get("min_trade_score_adj"), 0))
        CFG["min_confidence"] = int(BASE_THRESHOLDS["min_confidence"] + sfloat(self.adaptive.get("min_confidence_adj"), 0))

    def _update_perf(self, state: Dict[str, Any], equity: float) -> Dict[str, Any]:
        perf = state.get("strategy_performance") or {}
        curve = perf.get("equity_curve") or []
        curve.append({"ts": iso_now(), "equity": equity})
        curve = curve[-720:]
        vals = [sfloat(x.get("equity"), equity) for x in curve]
        return {"equity_curve": curve, "max_drawdown": round(calc_max_drawdown(vals), 5), "points": len(curve)}

    

    def _compute_tracks(self, snap: Dict[str, Any], factors: Dict[str, Any]) -> Dict[str, Any]:
        fs = factors.get("factors", {}) if isinstance(factors, dict) else {}
        asr = (fs.get("asr_vc") or {}).get("details") or {}
        patt = fs.get("kline_pattern") or {}
        momentum = fs.get("momentum") or {}
        market_regime = fs.get("market_regime") or {}
    
        pscore = sfloat(patt.get("score"), 50)
        ascore = sfloat((fs.get("asr_vc") or {}).get("score"), 50)
        tscore = sfloat((fs.get("technical_kline") or {}).get("score"), 50)
        _ = sfloat(momentum.get("score"), 50)
        _ = sfloat(market_regime.get("score"), 50)
    
        vol_z = sfloat(asr.get("volume_z"), 0.0)
        support = sfloat(asr.get("support"), 0.0)
        resistance = sfloat(asr.get("resistance"), 0.0)
        price = sfloat((snap.get("spot") or {}).get("price"), 0.0)
    
        liq_risk = 0.0
        if support > 0 and resistance > support and price > 0:
            pos = (price - support) / max(1e-9, resistance - support)
            if pos > 0.9 or pos < 0.1:
                liq_risk += 8
            liq_risk += clamp(abs(vol_z) * 2.5, 0, 10)
    
        intraday_score = 0.38 * pscore + 0.34 * ascore + 0.28 * tscore - liq_risk
        intraday_dir = "long" if intraday_score >= 52 else "short" if intraday_score <= 48 else "hold"
    
        lt = snap.get("long_term") or {}
        ahr = sfloat(lt.get("ahr999_proxy"), 9.9)
        fg = sfloat(lt.get("fear_greed"), 50)
        ma200 = sfloat(lt.get("ma200"), 0)
        long_bottom = bool(lt.get("bottom_signal", False))
    
        if long_bottom:
            swing_score = 72.0 + clamp((0.45 - ahr) * 30.0, -6, 12)
        else:
            swing_score = 42.0 + clamp((50 - fg) * 0.2, -6, 6) + (2.0 if ma200 > 0 and price < ma200 else -1.0)
        swing_dir = "long" if long_bottom else "hold"
    
        # Default to intraday always; swing is for reference only
        selected = "intraday"
        chosen_dir = intraday_dir
        chosen_edge = intraday_score
    
        return {
            "intraday": {
                "name": "short_term_intraday",
                "score": round(intraday_score, 2),
                "direction": intraday_dir,
                "risk": "high",
                "logic": "volume+pattern+support_resistance+liquidation_risk",
            },
            "swing": {
                "name": "long_term_swing",
                "score": round(swing_score, 2),
                "direction": swing_dir,
                "risk": "medium",
                "logic": "bottom_only: fear_greed + ahr999_proxy + ma200",
            },
            "selected": {
                "name": selected,
                "direction": chosen_dir,
                "score": round(chosen_edge, 2),
            },
            "inputs": {
                "volume_z": round(vol_z, 3),
                "support": support,
                "resistance": resistance,
                "price": price,
                "fear_greed": round(fg, 2),
                "ahr999_proxy": round(ahr, 4),
                "ma200": round(ma200, 2),
                "long_term_bottom_signal": long_bottom,
            },
        }
    
    def _analyze_trade_result(self, trade_snapshot: Dict[str, Any], pnl: float, pnl_pct: float) -> Dict[str, Any]:
        """深度分析交易结果，生成总结"""
        analysis = {
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "reasons": [],
            "factor_errors": [],
            "suggestions": [],
        }
        
        direction = trade_snapshot.get("direction", "")
        factors_snap = trade_snapshot.get("factors_snapshot", {})
        
        # 1. 分析因子信号正确性
        for name, f in factors_snap.items():
            f_dir = f.get("direction", "neutral")
            f_score = f.get("score", 50)
            f_conf = f.get("confidence", 50)
            
            # 判断因子信号是否正确
            if direction == "long":
                if f_dir == "bullish" and pnl < 0:
                    analysis["factor_errors"].append({
                        "factor": name,
                        "signal": "bullish",
                        "score": f_score,
                        "error": "做多信号错误",
                    })
                elif f_dir == "bearish" and pnl > 0:
                    analysis["factor_errors"].append({
                        "factor": name,
                        "signal": "bearish",
                        "score": f_score,
                        "error": "看空信号但实际盈利，被忽略",
                    })
            elif direction == "short":
                if f_dir == "bearish" and pnl < 0:
                    analysis["factor_errors"].append({
                        "factor": name,
                        "signal": "bearish",
                        "score": f_score,
                        "error": "做空信号错误",
                    })
                elif f_dir == "bullish" and pnl > 0:
                    analysis["factor_errors"].append({
                        "factor": name,
                        "signal": "bullish",
                        "score": f_score,
                        "error": "看多信号但实际做空盈利，被忽略",
                    })
        
        # 2. 分析入场时机
        score = trade_snapshot.get("score", 50)
        confidence = trade_snapshot.get("confidence", 50)
        
        if score < 55:
            analysis["reasons"].append(f"入场评分偏低({score:.1f})，信号不够强")
        if confidence < 55:
            analysis["reasons"].append(f"置信度偏低({confidence:.1f})，市场不确定性高")
        
        # 3. 分析ATR/波动率
        atr_pct = trade_snapshot.get("market_snapshot", {}).get("atr_pct", 0)
        if atr_pct < 0.005:
            analysis["reasons"].append(f"ATR过低({atr_pct*100:.2f}%)，市场波动不足")
        elif atr_pct > 0.015:
            analysis["reasons"].append(f"ATR过高({atr_pct*100:.2f}%)，市场波动剧烈")
        
        # 4. 生成建议
        if len(analysis["factor_errors"]) > 3:
            analysis["suggestions"].append("因子信号分歧较大，考虑提高一致性阈值")
        
        error_factors = [e["factor"] for e in analysis["factor_errors"][:3]]
        if error_factors:
            analysis["suggestions"].append(f"需要重新评估因子: {', '.join(error_factors)}")
        
        # 5. 生成总结文本
        direction_cn = "做多" if direction == "long" else "做空"
        result_cn = "盈利" if pnl > 0 else "亏损"
        
        summary = f"【{direction_cn}交易{result_cn}】盈亏: {pnl:.2f}U ({pnl_pct:.2f}%)。"
        
        if analysis["factor_errors"]:
            error_names = [e["factor"] for e in analysis["factor_errors"][:3]]
            summary += f"信号错误因子: {', '.join(error_names)}。"
        
        if analysis["reasons"]:
            summary += "原因分析: " + "; ".join(analysis["reasons"][:2]) + "。"
        
        if analysis["suggestions"]:
            summary += "改进建议: " + "; ".join(analysis["suggestions"][:2]) + "。"
        
        analysis["summary"] = summary
        return analysis

    def _calculate_fitness(self, state: Dict[str, Any], account: Dict[str, Any]) -> Dict[str, Any]:
        """计算系统适应度评分"""
        trades = read_json(TRADES_FILE, [])
        if not isinstance(trades, list) or len(trades) < 10:
            return {"score": 50, "components": {}, "level": "insufficient_data"}
        
        # 1. 净收益率 (35%)
        initial = sfloat(state.get("initial_capital", 1000), 1000)
        current = sfloat(account.get("equity", 1000), 1000)
        net_return = (current - initial) / initial * 100
        net_return_score = 100 / (1 + max(0, 2.718 ** (-(net_return - 10) / 20)))  # sigmoid centered at 10%
        
        # 统一使用最近30笔交易
        recent_trades = trades[-30:] if trades else []
        
        # 2. Sharpe比率 (20%)
        returns = []
        for t in recent_trades:
            if "pnl" in t:
                returns.append(t["pnl"] / initial * 100)
        if returns:
            avg_ret = sum(returns) / len(returns)
            std_ret = (sum((r - avg_ret) ** 2 for r in returns) / len(returns)) ** 0.5 if len(returns) > 1 else 1
            sharpe = avg_ret / max(std_ret, 0.1)
            sharpe_score = max(0, min(100, (sharpe + 1) / 4 * 100))
        else:
            sharpe_score = 50
        
        # 3. 胜率质量 (20%)
        wins = sum(1 for t in recent_trades if t.get("pnl", 0) > 0)
        win_rate = wins / len(recent_trades) if recent_trades else 0.5
        sample_factor = min(1, len(recent_trades) / 30)
        win_rate_score = win_rate * sample_factor * 100
        
        # 4. 盈亏比 (15%)
        win_trades = [t for t in recent_trades if t.get("pnl", 0) > 0]
        loss_trades = [t for t in recent_trades if t.get("pnl", 0) < 0]
        if win_trades and loss_trades:
            avg_win = sum(t["pnl"] for t in win_trades) / len(win_trades)
            avg_loss = abs(sum(t["pnl"] for t in loss_trades) / len(loss_trades))
            pl_ratio = avg_win / max(avg_loss, 0.1)
            pl_ratio_score = max(0, min(100, (pl_ratio - 0.5) / 2.5 * 100))
        else:
            pl_ratio_score = 50
        
        # 5. 因子稳定性 (10%)
        factors = state.get("factors", {})
        if factors:
            factor_stds = []
            for name, f in factors.items():
                if isinstance(f, dict):
                    factor_stds.append(abs(f.get("score", 50) - 50) / 50)
            avg_std = sum(factor_stds) / len(factor_stds) if factor_stds else 0.5
            stability_score = (1 - avg_std) * 100
        else:
            stability_score = 50
        
        # 惩罚项
        # 6. 最大回撤惩罚 (25%)
        perf = state.get("strategy_performance", {})
        max_dd = sfloat(perf.get("max_drawdown", 0), 0)
        dd_penalty = max_dd * 100 * 2.5
        
        # 7. 过度交易惩罚 (15%)
        daily_trades = state.get("daily", {}).get("trade_count", 0)
        overtrade_penalty = max(0, (daily_trades - 5) * 3)
        
        # 8. 复杂度惩罚 (10%)
        active_factors = sum(1 for w in CFG["weights"].values() if w > 0.03)
        complexity_penalty = max(0, (active_factors - 10) * 0.5)
        
        # 9. 成本惩罚 (10%)
        fee_rate = 0.0005
        slippage = 0.0002
        total_cost = len(trades) * (fee_rate + slippage) * 2
        gross_profit = sum(max(0, t.get("pnl", 0)) for t in trades)
        cost_impact = total_cost / max(gross_profit, 1) * 100 if gross_profit > 0 else 0
        cost_penalty = cost_impact * 0.5
        
        # 计算总分
        fitness = (
            0.35 * net_return_score +
            0.20 * sharpe_score +
            0.20 * win_rate_score +
            0.15 * pl_ratio_score +
            0.10 * stability_score -
            0.25 * dd_penalty -
            0.15 * overtrade_penalty -
            0.10 * complexity_penalty -
            0.10 * cost_penalty
        )
        fitness = max(0, min(100, fitness))
        
        # 确定等级
        if fitness >= 80:
            level = "excellent"
        elif fitness >= 60:
            level = "good"
        elif fitness >= 40:
            level = "acceptable"
        elif fitness >= 20:
            level = "poor"
        else:
            level = "critical"
        
        return {
            "score": round(fitness, 2),
            "level": level,
            "components": {
                "net_return": round(net_return_score, 2),
                "sharpe": round(sharpe_score, 2),
                "win_rate": round(win_rate_score, 2),
                "pl_ratio": round(pl_ratio_score, 2),
                "stability": round(stability_score, 2),
            },
            "penalties": {
                "drawdown": round(dd_penalty, 2),
                "overtrading": round(overtrade_penalty, 2),
                "complexity": round(complexity_penalty, 2),
                "cost": round(cost_penalty, 2),
            },
            "raw_metrics": {
                "net_return_pct": round(net_return, 2),
                "sharpe_ratio": round(sharpe, 2) if 'sharpe' in dir() else 0,
                "win_rate_pct": round(win_rate * 100, 2),
                "pl_ratio": round(pl_ratio, 2) if 'pl_ratio' in dir() else 0,
                "max_drawdown_pct": round(max_dd * 100, 2),
            }
        }

    def _evolution_tick(self, state: Dict[str, Any], account_after: Dict[str, Any], factors: Dict[str, Any], execution: Dict[str, Any], tracks: Dict[str, Any]) -> None:
        events = read_json(EVOLUTION_FILE, [])
        if not isinstance(events, list):
            events = []

        today = utc_now().strftime("%Y-%m-%d")
        
        # === 每日进化 ===
        if state.get("_last_evolution_date") != today:
            event = {
                "ts": iso_now(),
                "type": "daily_evolution",
                "date": today,
                "summary": "每日自适应检查",
                "selected_track": (tracks.get("selected") or {}).get("name"),
                "signal_score": sfloat((factors or {}).get("score"), 50),
                "drawdown": sfloat(((state.get("strategy_performance") or {}).get("max_drawdown")), 0),
                "adaptive_before": dict(self.adaptive),
            }
            
            if event["drawdown"] > 0.08:
                self.adaptive["min_trade_score_adj"] = min(12, int(self.adaptive.get("min_trade_score_adj", 0)) + 1)
                self.adaptive["intraday_leverage_cap"] = max(3, int(self.adaptive.get("intraday_leverage_cap", 6)) - 1)
                event["action"] = "收紧风控：回撤过大"
                event["summary"] = f"检测到回撤{event['drawdown']*100:.1f}%超过8%阈值，收紧开仓条件和杠杆上限"
            else:
                self.adaptive["min_trade_score_adj"] = max(-2, int(self.adaptive.get("min_trade_score_adj", 0)) - 1)
                event["action"] = "正常放宽"
                event["summary"] = "系统运行正常，适度放宽开仓条件"
            
            self._apply_adaptive()
            event["adaptive_after"] = dict(self.adaptive)
            events.append(event)
            state["_last_evolution_date"] = today

        # === 交易后进化（深度分析） ===
        eq_now = sfloat(account_after.get("equity", account_after.get("balance", 1000)), 1000)
        prev_eq = sfloat(state.get("_last_equity", eq_now), eq_now)
        delta = eq_now - prev_eq
        
        if execution.get("attempted"):
            # 获取最近交易记录进行分析
            trades = read_json(TRADES_FILE, [])
            last_trade = trades[-1] if trades else {}
            
            if delta < -0.5:
                # 亏损超过0.5U，进行深度分析
                pnl = delta
                pnl_pct = (delta / prev_eq * 100) if prev_eq > 0 else 0
                
                # 深度分析
                analysis = self._analyze_trade_result(last_trade, pnl, pnl_pct)
                
                # 调整参数
                self.adaptive["min_trade_score_adj"] = min(15, int(self.adaptive.get("min_trade_score_adj", 0)) + 1)
                self.adaptive["min_confidence_adj"] = min(12, int(self.adaptive.get("min_confidence_adj", 0)) + 1)
                self.adaptive["intraday_leverage_cap"] = max(2, int(self.adaptive.get("intraday_leverage_cap", 6)) - 1)
                
                # 根据错误因子调整权重
                weight_adjustments = []
                if analysis["factor_errors"]:
                    for err in analysis["factor_errors"][:2]:
                        factor_name = err["factor"]
                        if factor_name in CFG["weights"]:
                            # 降低错误因子的权重
                            old_weight = CFG["weights"][factor_name]
                            new_weight = max(0.01, old_weight * 0.9)
                            CFG["weights"][factor_name] = new_weight
                            weight_adjustments.append(f"{factor_name}: {old_weight:.2f}→{new_weight:.2f}")
                
                self._apply_adaptive()
                
                evolution_event = {
                    "ts": iso_now(),
                    "type": "loss_deep_analysis",
                    "equity_delta": round(delta, 4),
                    "pnl_pct": round(pnl_pct, 2),
                    "summary": analysis["summary"],
                    "factor_errors": analysis["factor_errors"],
                    "reasons": analysis["reasons"],
                    "suggestions": analysis["suggestions"],
                    "action": "亏损后深度复盘",
                    "adaptive_after": dict(self.adaptive),
                    "weight_adjustments": weight_adjustments,
                }
                events.append(evolution_event)
                
            elif delta > 0.5:
                # 盈利超过0.5U
                pnl = delta
                pnl_pct = (delta / prev_eq * 100) if prev_eq > 0 else 0
                
                analysis = self._analyze_trade_result(last_trade, pnl, pnl_pct)
                
                self.adaptive["min_trade_score_adj"] = max(-2, int(self.adaptive.get("min_trade_score_adj", 0)) - 1)
                self.adaptive["min_confidence_adj"] = max(-2, int(self.adaptive.get("min_confidence_adj", 0)) - 1)
                
                # 增加正确因子权重
                weight_adjustments = []
                correct_factors = [f for f in last_trade.get("factors_snapshot", {}).items() 
                                   if (last_trade.get("direction") == "long" and f[1].get("direction") == "bullish" and pnl > 0) or
                                      (last_trade.get("direction") == "short" and f[1].get("direction") == "bearish" and pnl > 0)]
                
                for name, f in correct_factors[:2]:
                    if name in CFG["weights"]:
                        old_weight = CFG["weights"][name]
                        new_weight = min(0.20, old_weight * 1.05)
                        CFG["weights"][name] = new_weight
                        weight_adjustments.append(f"{name}: {old_weight:.2f}→{new_weight:.2f}")
                
                self._apply_adaptive()
                
                events.append({
                    "ts": iso_now(),
                    "type": "win_analysis",
                    "equity_delta": round(delta, 4),
                    "pnl_pct": round(pnl_pct, 2),
                    "summary": analysis["summary"],
                    "action": "盈利后适度放宽",
                    "adaptive_after": dict(self.adaptive),
                    "weight_adjustments": weight_adjustments,
                })

        state["_last_equity"] = eq_now
        events = events[-1000:]
        write_json(EVOLUTION_FILE, events)
        save_adaptive(self.adaptive)
        # 持久化因子权重（修复P1：权重进化持久化）
        save_factor_weights(CFG["weights"])

    def step(self) -> Dict[str, Any]:
        state = self._load_state()
        history = self._load_history()
        account = load_account()  # 使用统一的账户加载函数

        # === 跨天清零日统计字段 ===
        today = utc_now().strftime("%Y-%m-%d")
        if account.get("costDay", "") != today:
            # 跨天了，清零日统计
            account["feesPaidToday"] = 0
            account["fundingPaidToday"] = 0
            account["dailyPnl"] = 0
            account["dailyTrades"] = 0
            account["costDay"] = today
            save_account(account)
            self.logger.log(f"[DAILY_RESET] New day {today}, daily stats cleared")
            
            self.adaptive["force_trade_today"] = False
            save_adaptive(self.adaptive)
            self.logger.log("[DAILY_RESET] v4.1 keeps neutral-cash behavior by default; force_trade_today remains disabled")

        # === 消费待分析队列（修复P1-2：闭环进化） ===
        pending = read_json(PENDING_ANALYSIS_FILE, [])
        if isinstance(pending, list) and len(pending) > 0:
            # 取出最早的待分析记录
            to_analyze = pending.pop(0)
            write_json(PENDING_ANALYSIS_FILE, pending)
            
            # 执行分析
            self.logger.log(f"[AI_ANALYSIS] Processing pending analysis: {to_analyze.get('close_type', 'unknown')}")
            # 分析逻辑会在后续的evolution中处理，这里只是消费队列
            # 如果需要立即分析，可以调用 _analyze_trade_result

        snap = self.feed.fetch()
        latest_price = sfloat((snap.get("spot") or {}).get("price"), 0.0)
        state["latest_price"] = latest_price

        # === 实时更新peakEquity（每轮都检查，包含持仓浮盈） ===
        positions = read_json(POSITIONS_FILE, [])
        equity = sfloat(account.get("equity", 1000), 1000)
        peak = sfloat(account.get("peakEquity", equity), equity)
        
        # 计算持仓浮盈
        unrealized_pnl = 0.0
        if positions and isinstance(positions, list):
            for pos in positions:
                if pos.get("side") and pos.get("size", 0) > 0:
                    entry_price = sfloat(pos.get("entryPrice", 0), 0)
                    size = sfloat(pos.get("size", 0), 0)
                    side = pos.get("side", "")
                    if entry_price > 0 and latest_price > 0:
                        if side == "long":
                            unrealized_pnl += (latest_price - entry_price) * size
                        elif side == "short":
                            unrealized_pnl += (entry_price - latest_price) * size
        
        # 实时权益 = 已实现权益 + 浮盈
        realtime_equity = equity + unrealized_pnl
        
        if realtime_equity > peak:
            account["peakEquity"] = round(realtime_equity, 2)
            account["unrealizedPnl"] = round(unrealized_pnl, 2)
            save_account(account)
            self.logger.log(f"[PEAK_UPDATED] peakEquity: {peak:.2f} -> {realtime_equity:.2f} (浮盈: {unrealized_pnl:.2f})")

        if snap.get("errors"):
            self.logger.log("data_errors=" + "; ".join(snap["errors"]))

        # Keep historical file compact; avoid persisting full kline arrays each cycle.
        hist_row = dict(snap)
        hist_row.pop("kline", None)
        hist_row.pop("long_term", None)
        hist_row.pop("whale", None)
        history.append(hist_row)
        self._save_history(history)

        factors = FactorEngineV31(history).evaluate(snap, CFG["weights"])
        tracks = self._compute_tracks(snap, factors)

        strat = factors.get("factors", {}).get("strategy_history", {})
        strat_dd = sfloat((strat.get("details") or {}).get("sim_max_drawdown_pct"), 0) / 100.0

        risk = self.risk.assess(account, state, sfloat(snap.get("data_quality", 0)), factors)
        if strat_dd > 0.22:
            risk["veto_reasons"].append(f"strategy_drawdown_high:{strat_dd:.3f}")
            risk["decision"] = "hold"
        benchmark_context = factors.get("benchmark_context", {}) if isinstance(factors, dict) else {}
        strategy_advisor = factors.get("strategy_advisor", {}) if isinstance(factors, dict) else {}

        # === 持仓超时检查 ===
        # positions已在上面的peakEquity更新时读取，这里复用
        position_timeout_close = None
        position_sl_tp_close = None  # 止损止盈平仓
        
        if positions and isinstance(positions, list):
            for pos in positions:
                if pos.get("side") and pos.get("size", 0) > 0:
                    try:
                        # === 资金费率扣减（每8小时结算一次） ===
                        funding_rate = snap.get("funding", {}).get("funding_rate", 0)
                        if funding_rate is not None and abs(funding_rate) > 0:
                            open_time_str = pos.get("openTime", "")
                            if open_time_str:
                                open_time = datetime.fromisoformat(open_time_str.replace('Z', '+00:00'))
                                now = utc_now()
                                
                                # 计算已跨过的8小时窗口数
                                hours_held = (now - open_time).total_seconds() / 3600
                                funding_interval = CFG["funding_interval_hours"]  # 8小时
                                
                                # 检查是否跨过新的资金费率窗口
                                open_funding_crossed = pos.get("open_funding_crossed", 0)
                                current_funding_crossed = int(hours_held / funding_interval)
                                
                                if current_funding_crossed > open_funding_crossed:
                                    # 跨过了新的窗口，需要扣减资金费率
                                    windows_crossed = current_funding_crossed - open_funding_crossed
                                    notional = pos.get("notional", 0)
                                    side = pos.get("side", "")
                                    
                                    # 资金费率成本
                                    # 正费率时：多头付钱（成本为正），空头收钱（成本为负）
                                    # 负费率时：多头收钱（成本为负），空头付钱（成本为正）
                                    side_sign = 1 if side == "long" else -1
                                    funding_cost = notional * funding_rate * side_sign * windows_crossed * CFG["funding_fee_multiplier"]
                                    # 注意：funding_cost 为正表示付钱，为负表示收钱
                                    # 所以 equity 应减少 funding_cost（付钱时减少，收钱时增加）
                                    
                                    # 扣减账户权益
                                    account = load_account()
                                    old_equity = account.get("equity", 1000)
                                    new_equity = old_equity - funding_cost  # 修复：付钱减少，收钱增加
                                    
                                    account["equity"] = round(new_equity, 2)
                                    account["balance"] = round(new_equity, 2)
                                    account["realizedPnl"] = round(account.get("realizedPnl", 0) - funding_cost, 4)  # 修复：付钱时funding_cost>0，PnL减少
                                    account["fundingPaidTotal"] = round(account.get("fundingPaidTotal", 0) - funding_cost, 4)  # 修复：记录付出去的资金费
                                    account["fundingPaidToday"] = round(account.get("fundingPaidToday", 0) - funding_cost, 4)  # 修复
                                    save_account(account)
                                    
                                    # 更新持仓的已跨窗口数
                                    pos["open_funding_crossed"] = current_funding_crossed
                                    write_json(POSITIONS_FILE, positions)
                                    
                                    # 记录成本台账
                                    record_cost_entry(
                                        event_type="funding",
                                        position_id=pos.get("position_id", "unknown"),
                                        symbol="BTC-USDT-SWAP",
                                        side=side,
                                        action="funding_settle",
                                        size_btc=pos.get("size", 0),
                                        leverage=pos.get("lever", 1),
                                        mark_price=latest_price,
                                        fill_price=latest_price,
                                        funding_rate=funding_rate,
                                        fee_rate=0,
                                        slippage_bps=0,
                                        fee_cost=0,
                                        slippage_cost=0,
                                        funding_cost=funding_cost,
                                        gross_pnl=None,
                                        net_pnl=None,
                                        equity_before=old_equity,
                                        equity_after=new_equity,
                                        realized_pnl_after=account["realizedPnl"],
                                        reason=f"funding_{int(windows_crossed)}x8h_settle",
                                        strategy=pos.get("strategy", "intraday"),
                                        notes=f"windows_crossed={windows_crossed}"
                                    )
                                    
                                    self.logger.log(f"[FUNDING_SETTLE] {side} funding: {funding_cost:.4f} USDT (rate: {funding_rate*100:.4f}%, windows: {windows_crossed})")
                        
                        # === 移动止盈止损检查 ===
                        sl_price = pos.get("stopLoss", 0)
                        tp1_price = pos.get("takeProfit1", 0)
                        tp2_price = pos.get("takeProfit2", 0)
                        side = pos.get("side", "")
                        entry_price = pos.get("entryPrice", 0)
                        current_size = pos.get("size", 0)
                        tp1_triggered = pos.get("tp1Triggered", False)
                        
                        # 多头止盈止损逻辑
                        if side == "long":
                            # 止损检查
                            if sl_price > 0 and latest_price <= sl_price:
                                position_sl_tp_close = {
                                    "action": "close",
                                    "side": side,
                                    "size": current_size,
                                    "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                    "reason": f"stop_loss: {latest_price} <= {sl_price}",
                                    "type": "stop_loss",
                                    "entry_price": entry_price,
                                }
                                self.logger.log(f"[STOP_LOSS] LONG hit SL at {latest_price} (SL: {sl_price})")
                            
                            # TP2检查（剩余仓位全平）
                            elif tp1_triggered and tp2_price > 0 and latest_price >= tp2_price:
                                position_sl_tp_close = {
                                    "action": "close",
                                    "side": side,
                                    "size": current_size,
                                    "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                    "reason": f"take_profit_2: {latest_price} >= {tp2_price}",
                                    "type": "take_profit_2",
                                    "entry_price": entry_price,
                                }
                                self.logger.log(f"[TAKE_PROFIT_2] LONG hit TP2 at {latest_price} (TP2: {tp2_price})")
                            
                            # TP1检查（减仓50%，止损移到入场价）
                            elif not tp1_triggered and tp1_price > 0 and latest_price >= tp1_price:
                                # 减仓50%（检查最小下单量）
                                MIN_SIZE = 0.001  # OKX BTC-USDT-SWAP最小下单量
                                half_size = round(current_size * 0.5, 4)
                                remaining_size = round(current_size - half_size, 4)
                                
                                # 如果减仓量太小，直接全平
                                if half_size < MIN_SIZE:
                                    position_sl_tp_close = {
                                        "action": "close",
                                        "side": side,
                                        "size": current_size,
                                        "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                        "reason": f"take_profit_1_full: size too small for partial ({half_size} < {MIN_SIZE})",
                                        "type": "take_profit_1_full",
                                        "entry_price": entry_price,
                                        "position_data": pos.copy(),  # 保存完整持仓数据
                                    }
                                    self.logger.log(f"[TAKE_PROFIT_1] LONG hit TP1, but size {half_size} < min {MIN_SIZE}, closing all")
                                else:
                                    # 记录部分平仓请求（下单成功后再更新持仓）
                                    partial_close = {
                                        "action": "partial_close",
                                        "side": side,
                                        "size": half_size,
                                        "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                        "reason": f"take_profit_1: {latest_price} >= {tp1_price}",
                                        "type": "take_profit_1",
                                        "entry_price": entry_price,
                                        "remaining_size": remaining_size,
                                        "new_stop_loss": entry_price,  # 保本止损
                                        "position_data": pos.copy(),  # 保存完整持仓数据
                                        "original_size": current_size,  # 修复：记录原始持仓大小
                                    }
                                    position_sl_tp_close = partial_close
                                    self.logger.log(f"[TAKE_PROFIT_1_PENDING] LONG hit TP1 at {latest_price}, will close {half_size} and move SL to {entry_price}")
                        
                        # 空头止盈止损逻辑
                        elif side == "short":
                            # 止损检查
                            if sl_price > 0 and latest_price >= sl_price:
                                position_sl_tp_close = {
                                    "action": "close",
                                    "side": side,
                                    "size": current_size,
                                    "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                    "reason": f"stop_loss: {latest_price} >= {sl_price}",
                                    "type": "stop_loss",
                                    "entry_price": entry_price,
                                }
                                self.logger.log(f"[STOP_LOSS] SHORT hit SL at {latest_price} (SL: {sl_price})")
                            
                            # TP2检查（剩余仓位全平）
                            elif tp1_triggered and tp2_price > 0 and latest_price <= tp2_price:
                                position_sl_tp_close = {
                                    "action": "close",
                                    "side": side,
                                    "size": current_size,
                                    "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                    "reason": f"take_profit_2: {latest_price} <= {tp2_price}",
                                    "type": "take_profit_2",
                                    "entry_price": entry_price,
                                }
                                self.logger.log(f"[TAKE_PROFIT_2] SHORT hit TP2 at {latest_price} (TP2: {tp2_price})")
                            
                            # TP1检查（减仓50%，止损移到入场价）
                            elif not tp1_triggered and tp1_price > 0 and latest_price <= tp1_price:
                                # 减仓50%（检查最小下单量）
                                MIN_SIZE = 0.001  # OKX BTC-USDT-SWAP最小下单量
                                half_size = round(current_size * 0.5, 4)
                                remaining_size = round(current_size - half_size, 4)
                                
                                # 如果减仓量太小，直接全平
                                if half_size < MIN_SIZE:
                                    position_sl_tp_close = {
                                        "action": "close",
                                        "side": side,
                                        "size": current_size,
                                        "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                        "reason": f"take_profit_1_full: size too small for partial ({half_size} < {MIN_SIZE})",
                                        "type": "take_profit_1_full",
                                        "entry_price": entry_price,
                                        "position_data": pos.copy(),  # 保存完整持仓数据
                                    }
                                    self.logger.log(f"[TAKE_PROFIT_1] SHORT hit TP1, but size {half_size} < min {MIN_SIZE}, closing all")
                                else:
                                    # 记录部分平仓请求（下单成功后再更新持仓）
                                    partial_close = {
                                        "action": "partial_close",
                                        "side": side,
                                        "size": half_size,
                                        "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                        "reason": f"take_profit_1: {latest_price} <= {tp1_price}",
                                        "type": "take_profit_1",
                                        "entry_price": entry_price,
                                        "remaining_size": remaining_size,
                                        "new_stop_loss": entry_price,  # 保本止损
                                        "position_data": pos.copy(),  # 保存完整持仓数据
                                        "original_size": current_size,  # 修复：记录原始持仓大小
                                    }
                                    position_sl_tp_close = partial_close
                                    self.logger.log(f"[TAKE_PROFIT_1_PENDING] SHORT hit TP1 at {latest_price}, will close {half_size} and move SL to {entry_price}")
                        
                        # === 超时检查 ===
                        open_time_str = pos.get("openTime", "")
                        if open_time_str:
                            from datetime import datetime as dt
                            open_time = dt.fromisoformat(open_time_str.replace("Z", "+00:00"))
                            now_utc = utc_now()
                            if open_time.tzinfo is None:
                                open_time = open_time.replace(tzinfo=timezone.utc)
                            if now_utc.tzinfo is None:
                                now_utc = now_utc.replace(tzinfo=timezone.utc)
                            hold_hours = (now_utc - open_time).total_seconds() / 3600
                            max_hold = float(pos.get("maxHoldHours", 4))
                            strategy = pos.get("strategy", "intraday")
                            
                            state["position_status"] = {
                                "has_position": True,
                                "side": pos["side"],
                                "size": pos["size"],
                                "entry_price": pos.get("entryPrice", 0),
                                "hold_hours": round(hold_hours, 2),
                                "max_hold_hours": max_hold,
                                "strategy": strategy,
                                "timeout": hold_hours > max_hold,
                                "stop_loss": sl_price,
                                "take_profit": tp1_price,
                            }
                            
                            # 超时平仓（优先级低于止损止盈）
                            if hold_hours > max_hold and not position_sl_tp_close:
                                position_timeout_close = {
                                    "action": "close",
                                    "side": pos["side"],
                                    "size": pos["size"],
                                    "instId": pos.get("instId", "BTC-USDT-SWAP"),
                                    "reason": f"timeout: {hold_hours:.1f}h > {max_hold}h",
                                    "entry_price": pos.get("entryPrice", 0),
                                }
                                self.logger.log(f"[POSITION_TIMEOUT] {pos['side']} {pos['size']} BTC held {hold_hours:.1f}h > {max_hold}h")
                    except Exception as e:
                        self.logger.log(f"position_check_error: {e}")
        else:
            state["position_status"] = {"has_position": False}

        decision = risk["decision"]
        lev = int(risk["leverage"])
        size = sfloat(risk["size_btc"])
        
        # === 硬门槛：评分偏离度检查（abs(score-50)>=5才交易）===
        raw_score = factors.get("score", 50)
        score_deviation = abs(raw_score - 50)
        if decision in {"long", "short"} and score_deviation < 5:
            risk["veto_reasons"].append(f"score_deviation_low:{score_deviation:.1f}<5")
            decision = "hold"
            self.logger.log(f"[HARD_GATE] Blocked: score deviation {score_deviation:.1f} < 5 (score={raw_score:.1f})")

        # === 结构性门槛：先过 BTC 基准环境，再允许下单 ===
        if decision == "long" and not bool(benchmark_context.get("allow_long", False)):
            risk["veto_reasons"].append("benchmark_gate_long_blocked")
            decision = "hold"
            self.logger.log("[BENCHMARK_GATE] Blocked long: benchmark regime does not allow long risk")
        if decision == "short" and not bool(benchmark_context.get("allow_short", False)):
            risk["veto_reasons"].append("benchmark_gate_short_blocked")
            decision = "hold"
            self.logger.log("[BENCHMARK_GATE] Blocked short: benchmark regime does not allow short risk")

        preferred_decision = str(strategy_advisor.get("preferred_decision") or "hold")
        if decision in {"long", "short"} and preferred_decision in {"long", "short"} and decision != preferred_decision:
            mismatch_decision = decision
            decision = "hold"
            risk["veto_reasons"].append(f"structural_mismatch:{mismatch_decision}!={preferred_decision}")
            self.logger.log(f"[STRUCTURE_GATE] Blocked: {mismatch_decision} mismatches structural preference {preferred_decision}")

        # === 硬门槛：关键因子对齐检查（至少3个关键因子方向一致）===
        KEY_FACTORS = {"liquidation_pressure", "funding_basis_divergence", "liquidity_depth", "oi_delta", "taker_volume"}
        
        if decision in {"long", "short"}:
            factors_data = factors.get("factors", {})
            key_factor_directions = {"long": 0, "short": 0, "neutral": 0}
            hard_hold_all = True  # 假设所有关键因子都是hard_hold
            
            for fname in KEY_FACTORS:
                fdata = factors_data.get(fname, {})
                if not isinstance(fdata, dict):
                    continue
                fdir = fdata.get("direction", "neutral")
                fhard = fdata.get("details", {}).get("hard_hold_candidate", False)
                
                if fdir in key_factor_directions:
                    key_factor_directions[fdir] += 1
                
                # 如果有任何一个关键因子不是hard_hold，则取消全局hard_hold
                if not fhard:
                    hard_hold_all = False
            
            # 统计同向因子数
            long_count = key_factor_directions["long"]
            short_count = key_factor_directions["short"]
            max_aligned = max(long_count, short_count)
            aligned_direction = "long" if long_count >= short_count else "short"
            
            # 检查对齐数
            if max_aligned < 3:
                risk["veto_reasons"].append(f"key_factor_alignment_lt3:{max_aligned}")
                decision = "hold"
                self.logger.log(f"[KEY_FACTOR_ALIGN] Blocked: only {max_aligned} key factors aligned (need >=3)")
            elif decision != aligned_direction:
                # 如果决策方向与关键因子主要方向不一致，也拦截
                risk["veto_reasons"].append(f"key_factor_mismatch:decision={decision}_aligned={aligned_direction}")
                decision = "hold"
                self.logger.log(f"[KEY_FACTOR_ALIGN] Blocked: decision {decision} != aligned {aligned_direction}")
            
            # 如果所有关键因子都是hard_hold_candidate，强制hold
            if hard_hold_all and decision in {"long", "short"}:
                risk["veto_reasons"].append(f"all_key_factors_hard_hold")
                decision = "hold"
                self.logger.log(f"[KEY_FACTOR_ALIGN] Blocked: all key factors are hard_hold_candidate")

        # === 流动性质量门控（第一道：硬门槛拦截） ===
        liquidity_guard = {}
        liquidity_factor = factors.get("factors", {}).get("liquidity_depth", {})
        liquidity_details = liquidity_factor.get("details", {})
        liquidity_quality = liquidity_details.get("quality", 1.0)
        hard_hold_candidate = liquidity_details.get("hard_hold_candidate", False)
        
        liquidity_guard = {
            "quality": liquidity_quality,
            "raw_score": liquidity_details.get("raw_score", 50),
            "gated_score": liquidity_details.get("gated_score", 50),
            "spoofing_detected": liquidity_details.get("spoofing_detected", False),
            "gate_reason": liquidity_details.get("gate_reason"),
        }
        state["liquidity_guard"] = liquidity_guard
        
        # 硬门槛拦截（quality<0.55 或 orderbook不可用）
        if decision in {"long", "short"} and hard_hold_candidate:
            gate_reason = liquidity_details.get("gate_reason", "quality_low")
            risk["veto_reasons"].append(f"liquidity_quality_low:{liquidity_quality:.2f}")
            decision = "hold"
            self.logger.log(f"[LIQUIDITY_GUARD] Blocked: quality={liquidity_quality:.2f}, reason={gate_reason}")

        active_mode = str(strategy_advisor.get("active_mode") or "neutral_cash")
        if decision in {"long", "short"} and active_mode == "neutral_cash":
            risk["veto_reasons"].append("regime_neutral_cash")
            decision = "hold"
            self.logger.log("[REGIME_GATE] Blocked: neutral_cash mode prefers waiting")
        elif decision == "short" and active_mode == "bear_tactical":
            lev = min(lev, 3)
            size = round(clamp(size * 0.75, 0.001, 0.04), 4)
        elif decision == "long" and active_mode == "bull_breakout":
            lev = min(max(lev, 2), 5)

        # apply track preference only when base risk engine allows trading
        # 质量低时禁用track加杠杆
        sel = tracks.get("selected") or {}
        if decision != "hold" and sel.get("direction") in {"long", "short"}:
            decision = sel.get("direction")
            # 质量低于0.70时禁用track加杠杆
            if liquidity_quality >= 0.70:
                if sel.get("name") == "intraday":
                    cap = int(self.adaptive.get("intraday_leverage_cap", 6))
                    lev = min(max(3, lev + 2), cap)
                    size = round(clamp(size * 1.35, 0.001, 0.06), 4)
                else:
                    cap = int(self.adaptive.get("swing_leverage_cap", 3))
                    lev = min(max(1, lev), cap)
                    size = round(clamp(size * 0.95, 0.001, 0.03), 4)
            else:
                self.logger.log(f"[LIQUIDITY_GUARD] Disabled track leverage: quality={liquidity_quality:.2f}<0.70")

        # ATR-aware scaling
        tech = (factors.get("factors", {}).get("technical_kline") or {}).get("details", {})
        atr = sfloat(tech.get("atr20"), 0.0)
        atr_pct = atr / max(1e-9, latest_price) if latest_price > 0 else 0.0
        if atr_pct > 0:
            vol_scale = clamp(0.012 / atr_pct, 0.45, 1.15)
            size = round(clamp(size * vol_scale, 0.001, 0.06 if sel.get("name") == "intraday" else 0.03), 4)
            if vol_scale < 0.7:
                lev = max(1, lev - 1)

        # === 流动性质量门控（第二道：最终封顶降风险） ===
        # 在所有加仓逻辑之后，对中等质量做最终降风险
        if decision in {"long", "short"} and liquidity_quality < 0.70:
            lev = max(1, int(lev * 0.6))
            size = round(size * 0.75, 4)
            
            # 检查最小下单量（OKX BTC-USDT-SWAP最小0.001 BTC）
            MIN_SIZE = 0.001
            if size < MIN_SIZE:
                risk["veto_reasons"].append(f"min_size_after_cap:{size:.4f}<0.001")
                decision = "hold"
                self.logger.log(f"[LIQUIDITY_GUARD] Blocked: size={size:.4f}<0.001 after cap, changed to hold")
            else:
                # 确保 size 在合法范围内
                size = round(clamp(size, MIN_SIZE, 0.06 if sel.get("name") == "intraday" else 0.03), 4)
                self.logger.log(f"[LIQUIDITY_GUARD] Final cap: quality={liquidity_quality:.2f}, lev={lev}, size={size}")

        perf = state.get("strategy_performance") or {}
        live_dd = sfloat(perf.get("max_drawdown"), 0.0)
        dynamic_interval = CFG["loop_seconds"]
        if live_dd >= 0.08:
            dynamic_interval = 120
        if live_dd >= 0.12:
            dynamic_interval = 180

        # === 平仓执行（优先级：止损止盈 > 超时） ===
        if position_sl_tp_close:
            close_type = position_sl_tp_close.get("type", "unknown")
            is_partial_close = close_type == "take_profit_1"  # TP1是部分平仓
            
            # 止损止盈平仓（最高优先级）
            close_side = "short" if position_sl_tp_close["side"] == "long" else "long"
            close_size = position_sl_tp_close["size"]
            execution = self.exec.execute_paper(close_side, 1, close_size, latest_price)
            execution["sl_tp_close"] = True
            execution["close_reason"] = position_sl_tp_close["reason"]
            execution["close_type"] = close_type
            execution["is_partial_close"] = is_partial_close
            decision = f"{close_side}_sl_tp_close"
            
            if is_partial_close:
                self.logger.log(f"[PARTIAL_CLOSE] TP1 executed {close_side} {close_size} BTC @ {latest_price}")
            else:
                self.logger.log(f"[FULL_CLOSE] {close_type} executed {close_side} {close_size} BTC @ {latest_price}")
        elif position_timeout_close:
            # 超时平仓
            close_side = "short" if position_timeout_close["side"] == "long" else "long"
            execution = self.exec.execute_paper(close_side, 1, position_timeout_close["size"], latest_price)
            execution["timeout_close"] = True
            execution["timeout_reason"] = position_timeout_close["reason"]
            decision = f"{close_side}_timeout_close"
            self.logger.log(f"[TIMEOUT_CLOSE] executed {close_side} {position_timeout_close['size']} BTC @ {latest_price}")
        else:
            execution = self.exec.execute_paper(decision, lev, size, latest_price)
        now_iso = iso_now()

        state["decision_count"] = int(state.get("decision_count", 0)) + 1
        state["last_decision"] = {
            "timestamp": now_iso,
            "direction": decision,
            "score": factors["score"],
            "confidence": factors["confidence"],
            "bullish_factors": factors["bullish_factors"],
            "bearish_factors": factors["bearish_factors"],
            "benchmark_score": benchmark_context.get("score"),
            "preferred_decision": preferred_decision,
            "active_mode": strategy_advisor.get("active_mode"),
            "veto_reasons": risk["veto_reasons"],
            "price": latest_price,
            "strategy_track": sel.get("name"),
        }
        # 只在真实成交时更新last_execution（用于冷却判断）
        if execution.get("success"):
            state["last_execution"] = {
                "timestamp": now_iso,
                "ts_unix": utc_now().timestamp(),
                "attempted": execution.get("attempted", False),
                "success": execution.get("success", False),
                "message": execution.get("message", ""),
                "payload": execution.get("payload"),
                "status_code": execution.get("status_code"),
            }

        daily = risk["daily"]
        if execution.get("success"):
            state["trade_count"] = int(state.get("trade_count", 0)) + 1
            daily["trade_count"] = int(daily.get("trade_count", 0)) + 1
            
            # === 更新持仓文件 ===
            if decision in {"long", "short"} and not execution.get("timeout_close"):
                # === 计算交易成本 ===
                # 1. 计算滑点
                funding_rate = snap.get("funding", {}).get("funding_rate", 0)
                liquidity_quality = liquidity_guard.get("quality", 1.0)
                slippage_bps = calculate_slippage_bps(atr_pct, liquidity_quality, funding_rate)
                
                # 2. 计算成交价
                fill_price = calculate_fill_price(latest_price, decision, slippage_bps)
                slippage_cost = abs(fill_price - latest_price) * size
                
                # 3. 计算开仓手续费
                fee_rate = CFG["fee_taker"]
                open_fee = fill_price * size * fee_rate
                
                # 4. 扣减账户权益
                account = load_account()
                equity_before = account.get("equity", 1000)
                equity_after = equity_before - open_fee
                realized_pnl_after = account.get("realizedPnl", 0) - open_fee
                
                account["equity"] = round(equity_after, 2)
                account["balance"] = round(equity_after, 2)
                account["realizedPnl"] = round(realized_pnl_after, 4)
                account["feesPaidTotal"] = round(account.get("feesPaidTotal", 0) + open_fee, 4)
                account["feesPaidToday"] = round(account.get("feesPaidToday", 0) + open_fee, 4)
                # 更新日统计（修复P3）
                account["dailyTrades"] = account.get("dailyTrades", 0) + 1
                save_account(account)
                
                # 5. ?????????v3.2.2??????????????
                sl_move = max(CFG["sl_min_pct"], min(CFG["sl_max_pct"], atr_pct * CFG["sl_atr_mult"]))
                tp1_move = max(CFG["tp1_min_pct"], atr_pct * CFG["tp1_atr_mult"])
                tp2_move = max(CFG["tp2_min_pct"], atr_pct * CFG["tp2_atr_mult"])
                if decision == "long":
                    sl_ratio = 1.0 - sl_move
                    tp1_ratio = 1.0 + tp1_move
                    tp2_ratio = 1.0 + tp2_move
                else:
                    sl_ratio = 1.0 + sl_move
                    tp1_ratio = 1.0 - tp1_move
                    tp2_ratio = 1.0 - tp2_move
                
                # 6. 生成仓位ID
                position_id = f"pos_{utc_now().strftime('%Y%m%d%H%M%S')}"
                
                # 新开仓（含止损止盈）
                new_pos = {
                    "instId": "BTC-USDT-SWAP",
                    "side": decision,
                    "size": size,
                    "entryPrice": fill_price,  # 使用成交价
                    "markPx": latest_price,
                    "notional": round(size * fill_price, 2),
                    "unrealizedPnl": 0.0,
                    "lever": lev,
                    "openTime": now_iso,
                    "strategy": sel.get("name", "intraday"),
                    "maxHoldHours": float(CFG.get("intraday_max_hold_hours", 4)),
                    "stopLoss": round(fill_price * sl_ratio, 2),  # 基于成交价计算
                    "takeProfit1": round(fill_price * tp1_ratio, 2),
                    "takeProfit2": round(fill_price * tp2_ratio, 2),
                    "tp1Triggered": False,
                    "position_id": position_id,
                    "open_fee": round(open_fee, 4),  # 记录开仓手续费
                    "slippage_cost": round(slippage_cost, 4),  # 记录滑点成本
                    "open_funding_crossed": 0,  # 已跨资金费率窗口数
                }
                write_json(POSITIONS_FILE, [new_pos])
                
                # 7. 记录成本台账
                record_cost_entry(
                    event_type="open_fee",
                    position_id=position_id,
                    symbol="BTC-USDT-SWAP",
                    side=decision,
                    action="open",
                    size_btc=size,
                    leverage=lev,
                    mark_price=latest_price,
                    fill_price=fill_price,
                    funding_rate=funding_rate,
                    fee_rate=fee_rate,
                    slippage_bps=slippage_bps,
                    fee_cost=open_fee,
                    slippage_cost=slippage_cost,
                    equity_before=equity_before,
                    equity_after=equity_after,
                    realized_pnl_after=realized_pnl_after,
                    reason="new_position",
                    strategy=sel.get("name", "intraday"),
                    notes=""
                )
                
                self.logger.log(f"[POSITION_OPENED] {decision} {size} BTC @ {fill_price:.2f} (slip: {slippage_bps:.1f}bps) | SL: {new_pos['stopLoss']} | TP1: {new_pos['takeProfit1']} | Fee: {open_fee:.4f} USDT")
            elif execution.get("sl_tp_close") or execution.get("timeout_close") or "sl_tp_close" in decision or "timeout_close" in decision:
                # 止损止盈或超时平仓
                close_type = execution.get("close_type", "timeout")
                is_partial_close = execution.get("is_partial_close", False)
                
                # === 统一获取平仓请求（支持 SL/TP 和 timeout） ===
                close_request = position_sl_tp_close or position_timeout_close
                
                # === 计算平仓成本 ===
                # 获取持仓信息
                positions = read_json(POSITIONS_FILE, [])
                if positions and isinstance(positions, list) and len(positions) > 0:
                    pos = positions[0]
                    entry_fill_price = pos.get("entryPrice", latest_price)
                    open_fee_paid = pos.get("open_fee", 0)
                    position_id = pos.get("position_id", "unknown")
                else:
                    entry_fill_price = latest_price
                    open_fee_paid = 0
                    position_id = "unknown"
                
                # 1. 计算平仓滑点
                funding_rate = snap.get("funding", {}).get("funding_rate", 0)
                liquidity_quality = liquidity_guard.get("quality", 1.0)
                close_slippage_bps = calculate_slippage_bps(atr_pct, liquidity_quality, funding_rate)
                
                # 2. 计算平仓成交价（与开仓方向相反）
                # long平仓是卖出，short平仓是买入
                open_side = close_request.get("side", "") if close_request else ""
                close_side = "short" if open_side == "long" else "long"
                exit_fill_price = calculate_fill_price(latest_price, close_side, close_slippage_bps)
                close_size = close_request.get("size", 0) if close_request else 0
                close_slippage_cost = abs(exit_fill_price - latest_price) * close_size
                
                # 3. 计算平仓手续费
                close_fee_rate = CFG["fee_taker"]
                close_fee = exit_fill_price * close_size * close_fee_rate
                
                # 4. 计算毛盈亏和净盈亏
                if open_side == "long":
                    # 多头平仓：卖出价 - 买入价
                    gross_pnl = (exit_fill_price - entry_fill_price) * close_size
                else:
                    # 空头平仓：买入价 - 卖出价
                    gross_pnl = (entry_fill_price - exit_fill_price) * close_size
                
                # 净盈亏 = 毛盈亏 - 开仓手续费分摊 - 平仓手续费
                # 如果是部分平仓，按比例分摊开仓手续费
                if is_partial_close and close_request:
                    original_size = close_request.get("original_size", close_size)
                    open_fee_alloc = open_fee_paid * (close_size / original_size) if original_size > 0 else 0
                else:
                    open_fee_alloc = open_fee_paid
                
                net_pnl = gross_pnl - open_fee_alloc - close_fee
                
                # 计算盈亏百分比
                notional = entry_fill_price * close_size if close_size > 0 else 1
                pnl_pct = (net_pnl / notional) * 100 if notional > 0 else 0
                
                # 滑点成本
                total_slippage = close_slippage_cost
                
                # 5. 更新账户权益
                account = load_account()
                old_equity = sfloat(account.get("equity", 1000), 1000)
                new_equity = old_equity + net_pnl
                
                # 维护高水位和最大回撤
                old_peak = sfloat(account.get("peakEquity", 1000), 1000)
                new_peak = max(old_peak, new_equity)
                current_dd = (new_peak - new_equity) / new_peak if new_peak > 0 else 0.0
                old_max_dd = sfloat(account.get("maxDrawdown", 0), 0)
                new_max_dd = max(old_max_dd, current_dd)
                
                account["equity"] = round(new_equity, 2)
                account["balance"] = round(new_equity, 2)
                account["peakEquity"] = round(new_peak, 2)
                account["maxDrawdown"] = round(new_max_dd, 4)
                account["realizedPnl"] = round(sfloat(account.get("realizedPnl", 0), 0) + net_pnl, 4)
                account["feesPaidTotal"] = round(account.get("feesPaidTotal", 0) + close_fee, 4)
                account["feesPaidToday"] = round(account.get("feesPaidToday", 0) + close_fee, 4)
                # 更新日统计（修复P3）
                account["dailyPnl"] = round(account.get("dailyPnl", 0) + net_pnl, 4)
                save_account(account)
                
                # 6. 记录成本台账（平仓手续费）
                record_cost_entry(
                    event_type="close_fee",
                    position_id=position_id,
                    symbol="BTC-USDT-SWAP",
                    side=open_side,
                    action="partial_close" if is_partial_close else "close",
                    size_btc=close_size,
                    leverage=pos.get("lever", 1) if positions else 1,
                    mark_price=latest_price,
                    fill_price=exit_fill_price,
                    funding_rate=funding_rate,
                    fee_rate=close_fee_rate,
                    slippage_bps=close_slippage_bps,
                    fee_cost=close_fee,
                    slippage_cost=close_slippage_cost,
                    gross_pnl=gross_pnl,
                    net_pnl=net_pnl,
                    equity_before=old_equity,
                    equity_after=new_equity,
                    realized_pnl_after=account["realizedPnl"],
                    reason=close_type,
                    strategy=pos.get("strategy", "intraday") if positions else "intraday",
                    notes=""
                )
                
                self.logger.log(f"[ACCOUNT_UPDATED] equity: {old_equity:.2f} -> {new_equity:.2f}, Net PnL: {net_pnl:.4f} (Gross: {gross_pnl:.4f}, Fee: {close_fee:.4f})")
                
                # 部分平仓不清空持仓（下单成功后才更新持仓）
                if is_partial_close:
                    # TP1部分平仓成功，更新持仓
                    if position_sl_tp_close and execution.get("success"):
                        remaining_size = position_sl_tp_close.get("remaining_size", 0)
                        new_stop_loss = position_sl_tp_close.get("new_stop_loss", entry_price)
                        original_pos = position_sl_tp_close.get("position_data", {})
                        
                        # 更新持仓：止损移到入场价（保本）
                        if original_pos:
                            updated_pos = original_pos.copy()
                            updated_pos["size"] = remaining_size
                            updated_pos["stopLoss"] = new_stop_loss  # 保本止损
                            updated_pos["tp1Triggered"] = True
                            updated_pos["notional"] = round(remaining_size * latest_price, 2)
                            # 修复：更新剩余开仓费，避免后续重复扣费
                            remaining_open_fee = updated_pos.get("open_fee", 0) - open_fee_alloc
                            updated_pos["open_fee"] = round(max(0, remaining_open_fee), 4)
                            write_json(POSITIONS_FILE, [updated_pos])
                        
                    self.logger.log(f"[PARTIAL_CLOSE_DONE] TP1 partial close, PnL: {net_pnl:.2f} ({pnl_pct:.2f}%), remaining position kept")
                else:
                    # 全平：清空持仓
                    write_json(POSITIONS_FILE, [])
                    self.logger.log(f"[POSITION_CLOSED] {close_type} close, positions cleared, PnL: {net_pnl:.2f} ({pnl_pct:.2f}%)")
                
                # 全平时写入待分析队列，触发AI分析（部分平仓不触发）
                if not is_partial_close:
                    analysis_request = {
                        "ts": iso_now(),
                        "trigger": "position_close",
                        "close_type": close_type,
                        "entry_price": entry_fill_price,
                        "close_price": latest_price,
                        "size": close_size,
                        "side": open_side,
                        "pnl": net_pnl,
                        "pnl_pct": pnl_pct,
                        "decision_score": factors["score"],
                        "decision_confidence": factors["confidence"],
                        "factors_snapshot": {
                            name: {
                                "score": f.get("score", 50),
                                "direction": f.get("direction", "neutral"),
                            }
                            for name, f in factors.get("factors", {}).items()
                            if isinstance(f, dict)
                        },
                        "atr_pct": atr_pct,
                        "strategy": sel.get("name", "intraday"),
                        "status": "pending",
                    }
                    
                    pending = read_json(PENDING_ANALYSIS_FILE, [])
                    if not isinstance(pending, list):
                        pending = []
                    pending.append(analysis_request)
                    write_json(PENDING_ANALYSIS_FILE, pending[-20:])  # 保留最近20条
                    self.logger.log(f"[AI_ANALYSIS_QUEUED] {close_type} close, PnL: {net_pnl:.2f}, waiting for AI analysis")
                
                # 更新状态中的权益记录
                state["_last_equity"] = new_equity if 'new_equity' in dir() else old_equity
        state["daily"] = daily

        state["risk"] = risk["risk_snapshot"]
        state["risk"]["atr_pct"] = round(atr_pct, 5)
        state["signal"] = {
            "score": factors["score"],
            "confidence": factors["confidence"],
            "factor_count": factors["factor_count"],
            "bullish_factors": factors["bullish_factors"],
            "bearish_factors": factors["bearish_factors"],
            "benchmark_score": benchmark_context.get("score"),
            "preferred_decision": preferred_decision,
            "active_mode": strategy_advisor.get("active_mode"),
        }
        state["decision"] = decision
        state["score"] = factors["score"]
        state["confidence"] = factors["confidence"]
        state["strategy"] = sel.get("name", "intraday")
        state["timestamp"] = now_iso
        state["updated_at"] = now_iso
        state["factors"] = factors["factors"]
        state["strategy_tracks"] = tracks
        state["active_strategy"] = sel.get("name", "intraday")
        state["adaptive"] = self.adaptive
        state["benchmark_context"] = benchmark_context
        state["factor_groups"] = factors.get("factor_groups", {})
        state["strategy_advisor"] = strategy_advisor
        state["strategy_performance"] = self._update_perf(state, sfloat(account.get("equity", 1000), 1000))
        
        # 计算系统适应度
        state["fitness"] = self._calculate_fitness(state, account)

        if decision in {"long", "short"} and latest_price > 0:
            sl = 1.0 - max(0.006, min(0.02, atr_pct * 1.2)) if decision == "long" else 1.0 + max(0.006, min(0.02, atr_pct * 1.2))
            tp1 = 1.0 + max(0.008, atr_pct * 1.0) if decision == "long" else 1.0 - max(0.008, atr_pct * 1.0)
            tp2 = 1.0 + max(0.015, atr_pct * 1.8) if decision == "long" else 1.0 - max(0.015, atr_pct * 1.8)
            state["trade_plan"] = {
                "direction": decision,
                "entry": round(latest_price, 2),
                "stop_loss": round(latest_price * sl, 2),
                "take_profit_1": round(latest_price * tp1, 2),
                "take_profit_2": round(latest_price * tp2, 2),
                "size_btc": size,
                "leverage": lev,
                "strategy": sel.get("name", "intraday"),
                "benchmark_score": benchmark_context.get("score"),
                "risk_budget": strategy_advisor.get("risk_budget"),
                "active_mode": strategy_advisor.get("active_mode"),
            }
        else:
            state["trade_plan"] = {"direction": "hold"}

        state["runtime"] = {"next_interval_seconds": dynamic_interval}
        state["next_check"] = (
            "hold: " + (", ".join(risk["veto_reasons"]) if risk["veto_reasons"] else "insufficient edge")
            if decision == "hold"
            else f"{decision} candidate {lev}x size={size} ({sel.get('name','intraday')})"
        )

        # persist decision/trade history for dashboard
        append_json_list(
            DECISIONS_FILE,
            {
                "ts": iso_now(),
                "decision": decision,
                "score": factors["score"],
                "confidence": factors["confidence"],
                "price": latest_price,
                "strategy": sel.get("name", "intraday"),
                "veto": risk.get("veto_reasons", []),
                "execution": execution.get("message", ""),
            },
            3000,
        )
        if execution.get("success"):
            # 记录完整交易快照，用于后续进化分析
            trade_snapshot = {
                "ts": now_iso,
                "direction": decision,
                "price": latest_price,
                "size_btc": size,
                "leverage": lev,
                "strategy": sel.get("name", "intraday"),
                "score": factors["score"],
                "confidence": factors["confidence"],
                "status": "filled",
                # 因子快照
                "factors_snapshot": {
                    name: {
                        "score": f.get("score", 50),
                        "direction": f.get("direction", "neutral"),
                        "confidence": f.get("confidence", 50),
                    }
                    for name, f in factors.get("factors", {}).items()
                    if isinstance(f, dict)
                },
                # 市场快照
                "market_snapshot": {
                    "atr_pct": round(atr_pct, 4),
                    "track": sel.get("name", "intraday"),
                    "track_direction": sel.get("direction", "hold"),
                    "track_score": sel.get("score", 50),
                },
                # 拦截原因
                "veto_reasons": risk.get("veto_reasons", []),
            }
            append_json_list(TRADES_FILE, trade_snapshot, 3000)

        trades_history = read_json(TRADES_FILE, [])
        if isinstance(trades_history, list):
            state["trade_count"] = len(trades_history)

        # evolution engine (daily + post-loss)
        account_after = load_account()
        self._evolution_tick(state, account_after, factors, execution, tracks)

        write_json(STATE_FILE, state)
        self.logger.log(
            "v4.1.0 score={:.1f} conf={:.1f} decision={} lev={} size={} atr_pct={:.4f} track={} next={} veto={} exec={}".format(
                factors["score"],
                factors["confidence"],
                decision,
                lev,
                size,
                atr_pct,
                sel.get("name", "intraday"),
                dynamic_interval,
                "|".join(risk["veto_reasons"]) if risk["veto_reasons"] else "none",
                execution.get("message", ""),
            )
        )
        return {
            "decision": decision,
            "score": factors["score"],
            "confidence": factors["confidence"],
            "execution": execution,
            "next_interval": dynamic_interval,
            "track": sel.get("name", "intraday"),
        }

def main() -> None:
    parser = argparse.ArgumentParser(description="ai_trader_v3_1")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=int, default=CFG["loop_seconds"])
    args = parser.parse_args()
    t = TraderV31()
    if args.once:
        t.step()
        return
    while True:
        try:
            out = t.step()
            sleep_s = int(max(5, args.interval, sfloat((out or {}).get("next_interval"), CFG["loop_seconds"])))
            time.sleep(sleep_s)
        except KeyboardInterrupt:
            t.logger.log("ai_trader_v3_1 stopped")
            break
        except Exception as e:
            t.logger.log(f"v3.1 fatal_loop_error={e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
