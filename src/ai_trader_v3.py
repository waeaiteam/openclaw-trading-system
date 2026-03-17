#!/usr/bin/env python3
"""
Institutional-style multi-factor paper trader (v3).
- Dynamic factor scoring (no fixed placeholder scores)
- Hard risk gates and structured state output
- Compatible with existing /root/.okx-paper layout
"""

import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from statistics import pstdev
from typing import Any, Dict, List, Optional, Tuple

import requests

DATA_DIR = "/root/.okx-paper/data"
STATE_FILE = "/root/.okx-paper/ai_trader_status.json"
LOG_FILE = "/root/.okx-paper/ai_trader.log"
HISTORY_FILE = f"{DATA_DIR}/market_history.json"
ACCOUNT_FILE = f"{DATA_DIR}/account.json"

API_BASE = "https://wae.asia/api/okx"
SPOT_INST = "BTC-USDT"
SWAP_INST = "BTC-USDT-SWAP"

CFG = {
    "loop_seconds": 60,
    "max_daily_trades": 5,  # 增加日内交易上限
    "max_position_risk": 0.008,
    "max_total_risk": 0.015,
    "max_drawdown": 0.10,
    "cooldown_seconds": 60 * 60,  # 1小时冷却
    "defense_loss_streak": 3,
    "min_data_quality": 0.60,
    "min_trade_score": 55,  # 降低开仓阈值
    "high_quality_score": 72,
    "min_confidence": 52,  # 降低置信度阈值
    "take_profit_pct": 0.015,  # 1.5%止盈
    "stop_loss_pct": 0.01,  # 1%止损
    "weights": {
        "momentum": 0.08,
        "volatility_regime": 0.05,
        "funding": 0.07,
        "oi_delta": 0.08,
        "long_short": 0.07,
        "fear_greed": 0.07,
        "basis": 0.07,
        "market_regime": 0.06,
    },
    "history_max_points": 1500,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def sfloat(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def read_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # unique tmp filename to avoid collisions with concurrent writers
    tmp = f"{path}.tmp.{os.getpid()}.{int(time.time() * 1000)}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


class Logger:
    def log(self, msg: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")


class DataFeed:
    def __init__(self, logger: Logger):
        self.logger = logger
        self.session = requests.Session()

    def _get_json(self, url: str, timeout: int = 12) -> Tuple[Optional[dict], Optional[str]]:
        try:
            r = self.session.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json(), None
        except Exception as e:
            return None, str(e)

    def fetch(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "timestamp": iso_now(),
            "errors": [],
        }

        spot, err = self._get_json(f"https://www.okx.com/api/v5/market/ticker?instId={SPOT_INST}")
        if err:
            out["errors"].append(f"spot_ticker: {err}")
        else:
            row = (spot.get("data") or [{}])[0]
            last = sfloat(row.get("last"))
            open24 = sfloat(row.get("open24h"), last if last else 1)
            high24 = sfloat(row.get("high24h"), last)
            low24 = sfloat(row.get("low24h"), last)
            out["spot"] = {
                "price": last,
                "open24h": open24,
                "high24h": high24,
                "low24h": low24,
                "change24h_pct": ((last - open24) / open24 * 100.0) if open24 else 0.0,
            }

        swap, err = self._get_json(f"https://www.okx.com/api/v5/market/ticker?instId={SWAP_INST}")
        if err:
            out["errors"].append(f"swap_ticker: {err}")
        else:
            row = (swap.get("data") or [{}])[0]
            out["swap"] = {"price": sfloat(row.get("last"))}

        funding, err = self._get_json(f"https://www.okx.com/api/v5/public/funding-rate?instId={SWAP_INST}")
        if err:
            out["errors"].append(f"funding: {err}")
        else:
            row = (funding.get("data") or [{}])[0]
            out["funding"] = {
                "funding_rate": sfloat(row.get("fundingRate")),
                "next_funding_rate": sfloat(row.get("nextFundingRate")),
            }

        oi, err = self._get_json(f"https://www.okx.com/api/v5/public/open-interest?instId={SWAP_INST}")
        if err:
            out["errors"].append(f"open_interest: {err}")
        else:
            data = oi.get("data") or []
            row = data[0] if data else {}
            out["oi"] = {
                "contracts": sfloat(row.get("oi")),
                "oi_usd": sfloat(row.get("oiUsd")),
            }

        # OKX rubik ratio endpoint currently requires ccy and returns [ts, ratio] rows.
        ls, err = self._get_json(
            "https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=BTC&period=5m"
        )
        if err:
            out["errors"].append(f"long_short: {err}")
        else:
            data = ls.get("data") or []
            ratio = 1.0
            if data:
                row = data[0]
                if isinstance(row, list) and len(row) >= 2:
                    ratio = sfloat(row[1], 1.0)
                elif isinstance(row, dict):
                    long_acc = sfloat(row.get("longAccount"), 0.5)
                    short_acc = sfloat(row.get("shortAccount"), 0.5)
                    ratio = long_acc / max(0.01, short_acc)
            out["long_short"] = {
                "ratio": ratio,
            }

        fg, err = self._get_json("https://api.alternative.me/fng/?limit=2")
        if err:
            out["errors"].append(f"fear_greed: {err}")
        else:
            data = fg.get("data") or []
            cur = data[0] if data else {}
            prev = data[1] if len(data) > 1 else cur
            out["fear_greed"] = {
                "value": int(sfloat(cur.get("value"), 50)),
                "prev_value": int(sfloat(prev.get("value"), 50)),
            }

        cg, err = self._get_json("https://api.coingecko.com/api/v3/global")
        if err:
            out["errors"].append(f"coingecko_global: {err}")
        else:
            data = cg.get("data") or {}
            mcap_pct = data.get("market_cap_percentage") or {}
            out["market"] = {
                "btc_dominance": sfloat(mcap_pct.get("btc"), 50),
                "mcap_change_24h": sfloat(data.get("market_cap_change_percentage_24h_usd"), 0),
            }

        required = ["spot", "swap", "funding", "oi", "long_short", "fear_greed", "market"]
        out["data_quality"] = sum(1 for k in required if k in out) / float(len(required))
        return out


class FactorEngine:
    def __init__(self, history: List[Dict[str, Any]]):
        self.history = history

    def _hist_values(self, path: Tuple[str, ...], limit: int = 300) -> List[float]:
        vals: List[float] = []
        for row in self.history[-limit:]:
            cur = row
            ok = True
            for p in path:
                if not isinstance(cur, dict) or p not in cur:
                    ok = False
                    break
                cur = cur[p]
            if ok:
                vals.append(sfloat(cur))
        return vals

    def _factor(self, name: str, score: float, direction: str, confidence: float, details: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": name,
            "score": round(clamp(score, 0, 100), 2),
            "direction": direction,
            "confidence": round(clamp(confidence, 0, 100), 2),
            "details": details,
        }

    def momentum(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        px = sfloat(snap["spot"]["price"])
        past = self._hist_values(("spot", "price"), 120)
        score = 50.0
        details: Dict[str, Any] = {}
        if len(past) >= 15 and px > 0:
            r5 = (px / past[-5] - 1.0) * 100 if past[-5] else 0.0
            r15 = (px / past[-15] - 1.0) * 100 if past[-15] else 0.0
            combo = 0.6 * r5 + 0.4 * r15
            score += clamp(combo * 6.0, -25, 25)
            details = {"ret_5": round(r5, 3), "ret_15": round(r15, 3), "combo": round(combo, 3)}
        direction = "bullish" if score > 55 else "bearish" if score < 45 else "neutral"
        conf = 55 + min(30, abs(score - 50))
        return self._factor("momentum", score, direction, conf, details)

    def volatility_regime(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        prices = self._hist_values(("spot", "price"), 100)
        score = 50.0
        details: Dict[str, Any] = {}
        if len(prices) >= 20:
            rets = []
            for i in range(1, len(prices)):
                if prices[i - 1] > 0:
                    rets.append((prices[i] / prices[i - 1]) - 1.0)
            vol = pstdev(rets[-30:]) * math.sqrt(288) * 100 if len(rets) >= 30 else pstdev(rets) * 100
            score -= clamp((vol - 2.0) * 8.0, -20, 20)
            details["realized_vol_pct"] = round(vol, 3)
        hl = (sfloat(snap["spot"]["high24h"]) - sfloat(snap["spot"]["low24h"])) / max(1e-9, sfloat(snap["spot"]["price"])) * 100
        score -= clamp((hl - 5.0) * 2.0, -10, 10)
        details["hl_range_pct"] = round(hl, 3)
        direction = "bullish" if score > 53 else "bearish" if score < 47 else "neutral"
        conf = 50 + min(25, abs(score - 50))
        return self._factor("volatility_regime", score, direction, conf, details)

    def funding(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        fr = sfloat(snap["funding"]["funding_rate"]) * 100
        hist = self._hist_values(("funding", "funding_rate"), 240)
        score = 50.0
        z = 0.0
        if len(hist) >= 20:
            vals = [v * 100 for v in hist]
            mu = sum(vals) / len(vals)
            sd = pstdev(vals) if len(vals) > 1 else 0.0
            z = (fr - mu) / sd if sd > 1e-9 else 0.0
            score -= clamp(z * 9.0, -22, 22)
        else:
            score -= clamp(fr * 120.0, -15, 15)
        direction = "bullish" if score > 53 else "bearish" if score < 47 else "neutral"
        conf = 55 + min(25, abs(z) * 8)
        return self._factor("funding", score, direction, conf, {"funding_pct": round(fr, 5), "zscore": round(z, 3)})

    def oi_delta(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        oi = sfloat(snap["oi"]["contracts"])
        prices = self._hist_values(("spot", "price"), 20)
        ois = self._hist_values(("oi", "contracts"), 20)
        score = 50.0
        details = {}
        if len(ois) >= 6 and len(prices) >= 6:
            oi_chg = (oi / max(1e-9, ois[-6]) - 1.0) * 100
            px_chg = (sfloat(snap["spot"]["price"]) / max(1e-9, prices[-6]) - 1.0) * 100
            if px_chg > 0 and oi_chg > 0:
                score += clamp(min(px_chg, oi_chg) * 3.0, 0, 18)
            elif px_chg < 0 and oi_chg > 0:
                score -= clamp(min(abs(px_chg), oi_chg) * 3.0, 0, 18)
            elif px_chg > 0 and oi_chg < 0:
                score -= clamp(min(px_chg, abs(oi_chg)) * 2.5, 0, 14)
            details = {"oi_change_6": round(oi_chg, 3), "price_change_6": round(px_chg, 3)}
        direction = "bullish" if score > 54 else "bearish" if score < 46 else "neutral"
        conf = 50 + min(30, abs(score - 50))
        return self._factor("oi_delta", score, direction, conf, details)

    def long_short(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        ratio = sfloat((snap.get("long_short") or {}).get("ratio"), 1.0)
        score = 50.0
        if ratio > 1.45:
            score -= clamp((ratio - 1.45) * 35.0, 0, 20)
        elif ratio < 0.75:
            score += clamp((0.75 - ratio) * 35.0, 0, 20)
        direction = "bullish" if score > 53 else "bearish" if score < 47 else "neutral"
        conf = 48 + min(30, abs(score - 50) * 1.2)
        return self._factor("long_short", score, direction, conf, {"ratio": round(ratio, 4)})

    def fear_greed(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        cur = sfloat(snap["fear_greed"]["value"], 50)
        prev = sfloat(snap["fear_greed"]["prev_value"], cur)
        delta = cur - prev
        score = 50.0
        if cur <= 20:
            score += 16
        elif cur <= 30:
            score += 10
        elif cur >= 80:
            score -= 16
        elif cur >= 70:
            score -= 10
        score += clamp(delta * 0.6, -8, 8)
        direction = "bullish" if score > 53 else "bearish" if score < 47 else "neutral"
        conf = 52 + min(22, abs(cur - 50) * 0.4)
        return self._factor("fear_greed", score, direction, conf, {"value": cur, "delta": delta})

    def basis(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        spot = sfloat(snap["spot"]["price"])
        swap = sfloat(snap["swap"]["price"])
        bps = ((swap - spot) / max(1e-9, spot)) * 10000
        score = 50.0
        score -= clamp(bps * 0.08, -18, 18)
        direction = "bullish" if score > 53 else "bearish" if score < 47 else "neutral"
        conf = 50 + min(25, abs(bps) * 0.2)
        return self._factor("basis", score, direction, conf, {"basis_bps": round(bps, 3)})

    def market_regime(self, snap: Dict[str, Any]) -> Dict[str, Any]:
        dom = sfloat(snap["market"]["btc_dominance"], 50)
        chg = sfloat(snap["market"]["mcap_change_24h"], 0)
        score = 50.0
        if chg > 4:
            score += clamp(chg * 1.8, 0, 15)
        elif chg < -4:
            score -= clamp(abs(chg) * 1.8, 0, 15)
        if dom > 60:
            score -= clamp((dom - 60) * 1.0, 0, 8)
        elif dom < 50:
            score += clamp((50 - dom) * 0.8, 0, 6)
        direction = "bullish" if score > 53 else "bearish" if score < 47 else "neutral"
        conf = 48 + min(26, abs(chg) * 1.5)
        return self._factor("market_regime", score, direction, conf, {"btc_dom": dom, "mcap_change_24h": chg})

    def evaluate(self, snap: Dict[str, Any], weights: Dict[str, float]) -> Dict[str, Any]:
        factors = {
            "momentum": self.momentum(snap) if "spot" in snap else self._factor("momentum", 50, "neutral", 20, {"missing": True}),
            "volatility_regime": self.volatility_regime(snap) if "spot" in snap else self._factor("volatility_regime", 50, "neutral", 20, {"missing": True}),
            "funding": self.funding(snap) if "funding" in snap else self._factor("funding", 50, "neutral", 20, {"missing": True}),
            "oi_delta": self.oi_delta(snap) if "oi" in snap and "spot" in snap else self._factor("oi_delta", 50, "neutral", 20, {"missing": True}),
            "long_short": self.long_short(snap),
            "fear_greed": self.fear_greed(snap) if "fear_greed" in snap else self._factor("fear_greed", 50, "neutral", 20, {"missing": True}),
            "basis": self.basis(snap) if "swap" in snap and "spot" in snap else self._factor("basis", 50, "neutral", 20, {"missing": True}),
            "market_regime": self.market_regime(snap) if "market" in snap else self._factor("market_regime", 50, "neutral", 20, {"missing": True}),
        }

        used = []
        weighted_score = 0.0
        weighted_conf = 0.0
        total_w = 0.0
        for name, f in factors.items():
            w = sfloat(weights.get(name), 0)
            if w <= 0:
                continue
            total_w += w
            weighted_score += f["score"] * w
            weighted_conf += f["confidence"] * w
            used.append(name)

        final_score = weighted_score / total_w if total_w > 0 else 50.0
        final_conf = weighted_conf / total_w if total_w > 0 else 50.0

        bullish = sum(1 for f in factors.values() if f["direction"] == "bullish")
        bearish = sum(1 for f in factors.values() if f["direction"] == "bearish")
        alignment = abs(bullish - bearish) / max(1, len(factors))
        final_conf = clamp(final_conf + alignment * 12.0, 0, 100)

        return {
            "score": round(final_score, 2),
            "confidence": round(final_conf, 2),
            "bullish_factors": bullish,
            "bearish_factors": bearish,
            "factor_count": len(factors),
            "used_weights": used,
            "factors": factors,
        }


class RiskEngine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg

    def _today(self) -> str:
        return utc_now().strftime("%Y-%m-%d")

    def assess(self, account: Dict[str, Any], state: Dict[str, Any], quality: float, signal: Dict[str, Any]) -> Dict[str, Any]:
        veto: List[str] = []
        now_ts = utc_now().timestamp()
        score = sfloat(signal["score"])
        conf = sfloat(signal["confidence"])

        equity = sfloat(account.get("equity", 1000), 1000)
        peak = max(equity, sfloat(account.get("peakEquity", equity), equity))
        dd = (peak - equity) / peak if peak > 0 else 0.0

        daily = state.get("daily", {})
        if daily.get("date") != self._today():
            daily = {"date": self._today(), "trade_count": 0}

        if bool(account.get("defenseMode", False)):
            veto.append("account_defense_mode")
        if dd >= self.cfg["max_drawdown"]:
            veto.append(f"max_drawdown_reached:{dd:.3f}")
        if sfloat(quality) < self.cfg["min_data_quality"]:
            veto.append(f"low_data_quality:{quality:.2f}")
        if int(daily.get("trade_count", 0)) >= int(self.cfg["max_daily_trades"]):
            veto.append("max_daily_trades")

        last_exec = state.get("last_execution", {})
        last_exec_ts = sfloat(last_exec.get("ts_unix", 0), 0)
        if last_exec_ts > 0 and (now_ts - last_exec_ts) < self.cfg["cooldown_seconds"]:
            veto.append("cooldown")

        if score < self.cfg["min_trade_score"]:
            veto.append("score_below_threshold")
        if conf < self.cfg["min_confidence"]:
            veto.append("confidence_below_threshold")

        direction = "hold"
        lev = 1
        if not veto:
            if signal["bullish_factors"] >= signal["bearish_factors"] + 2:
                direction = "long"
            elif signal["bearish_factors"] >= signal["bullish_factors"] + 2:
                direction = "short"
            else:
                veto.append("factor_alignment_weak")

        if direction != "hold":
            if score >= self.cfg["high_quality_score"] and conf >= 72:
                lev = 3
            elif score >= self.cfg["min_trade_score"] + 6 and conf >= 64:
                lev = 2
            else:
                lev = 1

        # size in BTC for paper endpoint; dynamic by equity and confidence
        notional = equity * self.cfg["max_position_risk"] * (0.7 + 0.6 * (conf / 100.0)) * lev
        btc_price = max(1e-9, sfloat(state.get("latest_price", 0), 0))
        size_btc = clamp(notional / btc_price if btc_price > 0 else 0.005, 0.001, 0.03)

        return {
            "veto_reasons": veto,
            "decision": direction if not veto else "hold",
            "leverage": lev,
            "size_btc": round(size_btc, 4),
            "risk_snapshot": {
                "equity": equity,
                "peak_equity": peak,
                "drawdown": round(dd, 4),
                "daily_trade_count": int(daily.get("trade_count", 0)),
                "data_quality": round(quality, 3),
            },
            "daily": daily,
        }


class Executor:
    def __init__(self, logger: Logger):
        self.logger = logger
        self.session = requests.Session()

    def execute_paper(self, direction: str, leverage: int, size_btc: float, price: float) -> Dict[str, Any]:
        if direction not in {"long", "short"}:
            return {"attempted": False, "success": False, "message": "hold"}

        payloads = [
            {
                "action": "trade",
                "direction": direction,
                "price": price,
                "size": size_btc,
                "leverage": leverage,
            },
            {
                "action": "decide",
                "direction": direction,
                "price": price,
                "size": size_btc,
                "leverage": leverage,
            },
        ]

        for p in payloads:
            try:
                r = self.session.post(f"{API_BASE}/paper", json=p, timeout=15)
                text = r.text[:1200]
                try:
                    data = r.json()
                except Exception:
                    data = {"raw": text}

                ok = bool(data.get("success", False)) and not data.get("error")
                if ok:
                    return {
                        "attempted": True,
                        "success": True,
                        "status_code": r.status_code,
                        "payload": p,
                        "response": data,
                        "message": data.get("message", "ok"),
                    }

                # Continue trying fallback payload if this one is not accepted.
                last_err = data.get("error") or data.get("message") or text
            except Exception as e:
                last_err = str(e)

        return {
            "attempted": True,
            "success": False,
            "message": f"paper_api_failed: {last_err}",
        }


class TraderV3:
    def __init__(self):
        self.logger = Logger()
        self.feed = DataFeed(self.logger)
        self.risk = RiskEngine(CFG)
        self.exec = Executor(self.logger)
        self.started_at = iso_now()

    def _load_state(self) -> Dict[str, Any]:
        s = read_json(STATE_FILE, {})
        if not isinstance(s, dict):
            s = {}
        s.setdefault("trader_status", "active")
        s.setdefault("account_type", "paper")
        s.setdefault("start_time", self.started_at)
        s.setdefault("decision_count", 0)
        s.setdefault("trade_count", 0)
        s.setdefault("daily", {"date": utc_now().strftime("%Y-%m-%d"), "trade_count": 0})
        return s

    def _save_state(self, state: Dict[str, Any]) -> None:
        write_json(STATE_FILE, state)

    def _load_history(self) -> List[Dict[str, Any]]:
        h = read_json(HISTORY_FILE, [])
        return h if isinstance(h, list) else []

    def _save_history(self, history: List[Dict[str, Any]]) -> None:
        keep = history[-CFG["history_max_points"] :]
        write_json(HISTORY_FILE, keep)

    def _load_account(self) -> Dict[str, Any]:
        d = read_json(ACCOUNT_FILE, {})
        return d if isinstance(d, dict) else {}

    def step(self) -> Dict[str, Any]:
        state = self._load_state()
        history = self._load_history()
        account = self._load_account()

        snap = self.feed.fetch()
        latest_price = sfloat((snap.get("spot") or {}).get("price"), 0.0)
        state["latest_price"] = latest_price

        if snap.get("errors"):
            self.logger.log("data_errors=" + "; ".join(snap["errors"]))

        history.append(snap)
        self._save_history(history)

        factors = FactorEngine(history).evaluate(snap, CFG["weights"])
        risk = self.risk.assess(account, state, sfloat(snap.get("data_quality", 0)), factors)

        decision = risk["decision"]
        lev = int(risk["leverage"])
        size = sfloat(risk["size_btc"])
        px = latest_price

        execution = self.exec.execute_paper(decision, lev, size, px)

        # Update state counters only on confirmed success.
        state["decision_count"] = int(state.get("decision_count", 0)) + 1
        state["last_decision"] = {
            "timestamp": iso_now(),
            "direction": decision,
            "score": factors["score"],
            "confidence": factors["confidence"],
            "bullish_factors": factors["bullish_factors"],
            "bearish_factors": factors["bearish_factors"],
            "veto_reasons": risk["veto_reasons"],
            "price": px,
        }

        # 只在真实成交时更新last_execution（用于冷却判断）
        if execution.get("success"):
            state["last_execution"] = {
                "timestamp": iso_now(),
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
            state["last_trade_date"] = utc_now().strftime("%Y-%m-%d")
            state["last_trade"] = {
                "timestamp": iso_now(),
                "direction": decision,
                "price": px,
                "size_btc": size,
                "leverage": lev,
                "score": factors["score"],
                "confidence": factors["confidence"],
            }

        state["daily"] = daily
        state["risk"] = risk["risk_snapshot"]
        state["signal"] = {
            "score": factors["score"],
            "confidence": factors["confidence"],
            "factor_count": factors["factor_count"],
            "bullish_factors": factors["bullish_factors"],
            "bearish_factors": factors["bearish_factors"],
        }
        state["factors"] = factors["factors"]
        state["rules"] = {
            "risk_per_trade": f"{CFG['max_position_risk']*100:.2f}%",
            "max_total_risk": f"{CFG['max_total_risk']*100:.2f}%",
            "max_daily_trades": CFG["max_daily_trades"],
            "cooldown_seconds": CFG["cooldown_seconds"],
            "min_trade_score": CFG["min_trade_score"],
            "high_quality_score": CFG["high_quality_score"],
        }

        if decision == "hold":
            next_hint = "hold: " + (", ".join(risk["veto_reasons"]) if risk["veto_reasons"] else "insufficient edge")
        else:
            next_hint = f"{decision} candidate {lev}x size={size}"
        state["next_check"] = next_hint

        self._save_state(state)

        self.logger.log(
            "score={:.1f} conf={:.1f} decision={} lev={} size={} veto={} exec={}".format(
                factors["score"],
                factors["confidence"],
                decision,
                lev,
                size,
                "|".join(risk["veto_reasons"]) if risk["veto_reasons"] else "none",
                execution.get("message", ""),
            )
        )

        return {
            "decision": decision,
            "score": factors["score"],
            "confidence": factors["confidence"],
            "execution": execution,
            "veto": risk["veto_reasons"],
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="ai_trader_v3")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--interval", type=int, default=CFG["loop_seconds"], help="loop interval seconds")
    args = parser.parse_args()

    trader = TraderV3()
    if args.once:
        trader.step()
        return

    while True:
        try:
            trader.step()
            time.sleep(max(5, args.interval))
        except KeyboardInterrupt:
            trader.logger.log("ai_trader_v3 stopped by keyboard interrupt")
            break
        except Exception as e:
            trader.logger.log(f"fatal_loop_error={e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
