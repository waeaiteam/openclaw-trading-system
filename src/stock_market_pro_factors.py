#!/usr/bin/env python3
"""
Stock Market Pro Integration Module
Provides real market data from Yahoo Finance for enhanced trading factors.

Dependencies: yfinance, pandas, numpy (must be installed)
"""

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import math
import signal

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

# Cache to avoid excessive API calls
_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_TTL = 300  # 5 minutes cache

# Timeout for yfinance calls (修复P2-6)
YFINANCE_TIMEOUT = 10  # seconds


class TimeoutError(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutError("yfinance call timed out")


def clear_cache() -> None:
    """Clear all cached data."""
    global _CACHE
    _CACHE = {}


def _cache_get(key: str) -> Optional[Any]:
    """Get cached data if still valid."""
    entry = _CACHE.get(key)
    if entry and time.time() - entry.get("ts", 0) < _CACHE_TTL:
        return entry.get("data")
    return None


def _cache_set(key: str, data: Any) -> None:
    """Set cache with timestamp."""
    _CACHE[key] = {"ts": time.time(), "data": data}


def fetch_yf_prices(tickers: List[str], period: str = "5d") -> Dict[str, Dict[str, Any]]:
    """
    Fetch real prices from Yahoo Finance.
    
    Args:
        tickers: List of ticker symbols (e.g., ['BTC-USD', 'SPY', '^VIX'])
        period: Time period (e.g., '5d', '1mo')
    
    Returns:
        Dict of {ticker: {price, change_pct, data}}
    """
    if not YFINANCE_AVAILABLE:
        return {t: {"error": "yfinance not installed"} for t in tickers}
    
    cache_key = f"yf_prices_{'_'.join(tickers)}_{period}"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    
    results = {}
    try:
        # 添加超时保护（修复P2-6）
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(YFINANCE_TIMEOUT)
        
        try:
            data = yf.download(tickers, period=period, progress=False, group_by='ticker')
            signal.alarm(0)  # 取消闹钟
        except TimeoutError:
            signal.alarm(0)
            return {t: {"error": "yfinance timeout"} for t in tickers}
        
        for t in tickers:
            try:
                # Handle MultiIndex columns from yfinance (even for single ticker)
                # Note: yfinance returns columns as [('SPY', 'Close'), ...] not [('Close', 'SPY'), ...]
                if isinstance(data.columns, pd.MultiIndex):
                    close = data[(t, 'Close')].dropna().values.flatten()
                elif len(tickers) == 1:
                    close = data['Close'].dropna().values.flatten()
                else:
                    close = data[t]['Close'].dropna().values.flatten()
                
                if len(close) >= 2:
                    latest = float(close[-1])
                    prev = float(close[0])
                    change_pct = (latest - prev) / prev * 100
                    results[t] = {
                        "price": latest,
                        "change_pct": round(change_pct, 2),
                        "high_5d": float(np.max(close)),
                        "low_5d": float(np.min(close)),
                        "error": None
                    }
                else:
                    results[t] = {"error": "insufficient_data"}
            except Exception as e:
                results[t] = {"error": str(e)[:100]}
    except Exception as e:
        results = {t: {"error": str(e)[:100]} for t in tickers}
    
    _cache_set(cache_key, results)
    return results


def fetch_btc_technicals(period: str = "1mo") -> Dict[str, Any]:
    """
    Fetch BTC technical indicators using Yahoo Finance data.
    
    Returns:
        Dict with RSI, MACD, BB, ATR values
    """
    if not YFINANCE_AVAILABLE:
        return {"error": "yfinance not installed"}
    
    cache_key = f"btc_technicals_{period}"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    
    result = {"error": None}
    try:
        btc = yf.download('BTC-USD', period=period, progress=False)
        
        # Handle MultiIndex columns from yfinance
        if isinstance(btc.columns, pd.MultiIndex):
            closes = btc[('Close', 'BTC-USD')].values.flatten()
            highs = btc[('High', 'BTC-USD')].values.flatten()
            lows = btc[('Low', 'BTC-USD')].values.flatten()
        else:
            closes = btc['Close'].values.flatten()
            highs = btc['High'].values.flatten()
            lows = btc['Low'].values.flatten()
        
        if len(closes) < 20:
            return {"error": "insufficient_data"}
        
        # RSI(14)
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = float(np.mean(gains[-14:])) if len(gains) >= 14 else 0.0
        avg_loss = float(np.mean(losses[-14:])) if len(losses) >= 14 else 0.0
        
        # RSI计算（正确处理极端情况）
        if avg_loss == 0 and avg_gain > 0:
            # 只有上涨没有下跌 -> RSI = 100（极度超买）
            result["rsi"] = 100.0
        elif avg_gain == 0 and avg_loss > 0:
            # 只有下跌没有上涨 -> RSI = 0（极度超卖）
            result["rsi"] = 0.0
        elif avg_gain == 0 and avg_loss == 0:
            # 无波动 -> RSI = 50（中性）
            result["rsi"] = 50.0
        else:
            # 正常计算
            rs = avg_gain / avg_loss
            result["rsi"] = round(float(100 - (100 / (1 + rs))), 2)
        
        # EMA/MACD
        closes_series = pd.Series(closes)
        ema12 = closes_series.ewm(span=12).mean().values
        ema26 = closes_series.ewm(span=26).mean().values
        macd_line = ema12[-1] - ema26[-1]
        signal_line = pd.Series(ema12 - ema26).ewm(span=9).mean().values[-1]
        result["macd"] = round(float(macd_line), 2)
        result["macd_signal"] = round(float(signal_line), 2)
        result["macd_hist"] = round(float(macd_line - signal_line), 2)
        result["macd_bullish"] = bool(macd_line > signal_line)
        
        # Bollinger Bands
        sma20 = np.mean(closes[-20:])
        std20 = np.std(closes[-20:])
        bb_upper = sma20 + 2 * std20
        bb_lower = sma20 - 2 * std20
        result["bb_upper"] = round(float(bb_upper), 2)
        result["bb_lower"] = round(float(bb_lower), 2)
        result["bb_sma"] = round(float(sma20), 2)
        result["bb_position"] = round(float((closes[-1] - bb_lower) / (bb_upper - bb_lower) * 100), 1)
        
        # ATR(14)
        tr = np.maximum(
            highs[-14:] - lows[-14:],
            np.maximum(
                np.abs(highs[-14:] - closes[-15:-1]),
                np.abs(lows[-14:] - closes[-15:-1])
            )
        )
        result["atr"] = round(float(np.mean(tr)), 2)
        result["atr_pct"] = round(float(np.mean(tr) / closes[-1] * 100), 2)
        
        # Price
        result["price"] = round(float(closes[-1]), 2)
        
    except Exception as e:
        result["error"] = str(e)[:200]
    
    _cache_set(cache_key, result)
    return result


def fetch_cross_market_correlation() -> Dict[str, Any]:
    """
    Calculate correlation between BTC and SPY.
    
    Returns:
        Dict with correlation value and direction signal
    """
    if not YFINANCE_AVAILABLE:
        return {"error": "yfinance not installed", "correlation": 0}
    
    cache_key = "cross_market_corr"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    
    result = {"error": None}
    try:
        spy = yf.download('SPY', period='1mo', progress=False)['Close'].values.flatten()
        btc = yf.download('BTC-USD', period='1mo', progress=False)['Close'].values.flatten()
        
        min_len = min(len(spy), len(btc))
        if min_len < 10:
            return {"error": "insufficient_data", "correlation": 0}
        
        spy_rets = np.diff(spy[-min_len:]) / spy[-min_len:-1]
        btc_rets = np.diff(btc[-min_len:]) / btc[-min_len:-1]
        
        corr = float(np.corrcoef(spy_rets, btc_rets)[0, 1])
        result["correlation"] = round(corr, 3)
        result["correlation_strength"] = "strong" if abs(corr) > 0.7 else "moderate" if abs(corr) > 0.4 else "weak"
        
        # Get SPY direction for signal
        spy_chg = (spy[-1] - spy[-5]) / spy[-5] * 100 if len(spy) >= 5 else 0
        result["spy_5d_change"] = round(spy_chg, 2)
        
    except Exception as e:
        result["error"] = str(e)[:100]
        result["correlation"] = 0
    
    _cache_set(cache_key, result)
    return result


def fetch_macro_risk() -> Dict[str, Any]:
    """
    Fetch macro risk indicators: VIX and DXY.
    
    Returns:
        Dict with VIX and DXY values and risk assessment
    """
    if not YFINANCE_AVAILABLE:
        return {"error": "yfinance not installed"}
    
    cache_key = "macro_risk"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    
    result = {"error": None}
    try:
        # VIX
        vix_data = yf.download('^VIX', period='5d', progress=False)
        vix_close = vix_data['Close'].values.flatten()
        if len(vix_close) > 0:
            vix_val = float(vix_close[-1])
            result["vix"] = round(vix_val, 2)
            
            if vix_val > 30:
                result["vix_risk_level"] = "high_panic"
            elif vix_val > 25:
                result["vix_risk_level"] = "elevated"
            elif vix_val > 20:
                result["vix_risk_level"] = "moderate"
            elif vix_val > 15:
                result["vix_risk_level"] = "low"
            else:
                result["vix_risk_level"] = "complacent"
        
        # DXY
        dxy_data = yf.download('DX-Y.NYB', period='5d', progress=False)
        dxy_close = dxy_data['Close'].values.flatten()
        if len(dxy_close) >= 2:
            dxy_val = float(dxy_close[-1])
            dxy_prev = float(dxy_close[0])
            dxy_chg = (dxy_val - dxy_prev) / dxy_prev * 100
            result["dxy"] = round(dxy_val, 2)
            result["dxy_5d_change"] = round(dxy_chg, 2)
        
    except Exception as e:
        result["error"] = str(e)[:100]
    
    _cache_set(cache_key, result)
    return result


def compute_cross_market_factor() -> Dict[str, Any]:
    """
    Compute cross market correlation factor score.
    
    Returns:
        Factor dict with score, direction, confidence
    """
    data = fetch_cross_market_correlation()
    
    if data.get("error"):
        return {
            "name": "cross_market_correlation",
            "score": 50.0,
            "direction": "neutral",
            "confidence": 20.0,
            "details": {"error": data["error"]}
        }
    
    score = 50.0
    corr = data.get("correlation", 0)
    spy_chg = data.get("spy_5d_change", 0)
    
    # Correlation-based scoring
    if abs(corr) > 0.6:
        # Strong correlation - follow SPY direction
        if spy_chg > 2:
            score += min(15, spy_chg * 3)
        elif spy_chg < -2:
            score -= min(15, abs(spy_chg) * 3)
    else:
        # Weak correlation - SPY impact is muted
        if spy_chg > 3:
            score += 5
        elif spy_chg < -3:
            score -= 5
    
    direction = "bullish" if score > 55 else "bearish" if score < 45 else "neutral"
    confidence = 30 + min(30, abs(corr) * 40) if data.get("error") is None else 20
    
    return {
        "name": "cross_market_correlation",
        "score": round(score, 2),
        "direction": direction,
        "confidence": round(confidence, 1),
        "details": {
            "correlation": corr,
            "spy_5d_change": spy_chg,
            "correlation_strength": data.get("correlation_strength", "unknown")
        }
    }


def compute_macro_risk_factor() -> Dict[str, Any]:
    """
    Compute macro risk sentiment factor score.
    
    Returns:
        Factor dict with score, direction, confidence
    """
    data = fetch_macro_risk()
    
    if data.get("error"):
        return {
            "name": "macro_risk_sentiment",
            "score": 50.0,
            "direction": "neutral",
            "confidence": 20.0,
            "details": {"error": data["error"]}
        }
    
    score = 50.0
    vix = data.get("vix", 20)
    dxy_chg = data.get("dxy_5d_change", 0)
    
    # VIX contribution (inverse - low VIX is bullish)
    if vix < 15:
        score += 10  # Complacency = risk on
    elif vix < 20:
        score += 5
    elif vix > 30:
        score -= 12  # Panic = risk off
    elif vix > 25:
        score -= 6
    
    # DXY contribution (inverse - strong dollar hurts BTC)
    if dxy_chg > 1:
        score -= 8
    elif dxy_chg > 0.5:
        score -= 4
    elif dxy_chg < -1:
        score += 8
    elif dxy_chg < -0.5:
        score += 4
    
    direction = "bullish" if score > 55 else "bearish" if score < 45 else "neutral"
    confidence = 40 if vix > 15 else 30  # More confidence in non-extreme VIX
    
    return {
        "name": "macro_risk_sentiment",
        "score": round(score, 2),
        "direction": direction,
        "confidence": round(confidence, 1),
        "details": {
            "vix": vix,
            "vix_risk_level": data.get("vix_risk_level", "unknown"),
            "dxy_5d_change": dxy_chg
        }
    }


def compute_enhanced_technical_factor() -> Dict[str, Any]:
    """
    Compute enhanced technical factor using professional indicators.
    
    Returns:
        Factor dict with score, direction, confidence
    """
    data = fetch_btc_technicals()
    
    if data.get("error"):
        return {
            "name": "enhanced_technical",
            "score": 50.0,
            "direction": "neutral",
            "confidence": 20.0,
            "details": {"error": data["error"]}
        }
    
    score = 50.0
    rsi = data.get("rsi", 50)
    macd_bullish = data.get("macd_bullish", False)
    macd_hist = data.get("macd_hist", 0)
    bb_pos = data.get("bb_position", 50)
    atr_pct = data.get("atr_pct", 2)
    
    # RSI contribution
    if rsi < 30:
        score += 12  # Oversold
    elif rsi < 40:
        score += 5
    elif rsi > 70:
        score -= 12  # Overbought
    elif rsi > 60:
        score -= 5
    
    # MACD contribution
    if macd_bullish:
        score += min(10, abs(macd_hist) / 50)
    else:
        score -= min(10, abs(macd_hist) / 50)
    
    # BB position contribution
    if bb_pos < 20:
        score += 6  # Near lower band
    elif bb_pos > 80:
        score -= 6  # Near upper band
    
    # ATR contribution (volatility adjustment)
    atr_conf_penalty = 0.0
    if atr_pct >= 7:
        score -= 8
        atr_conf_penalty = 10
    elif atr_pct >= 5:
        score -= 5
        atr_conf_penalty = 6
    elif atr_pct <= 1.5:
        score += 2
        atr_conf_penalty = -2

    direction = "bullish" if score > 55 else "bearish" if score < 45 else "neutral"
    confidence = 35 + min(25, (100 - abs(rsi - 50)) * 0.3) - atr_conf_penalty
    confidence = max(20.0, min(90.0, confidence))
    
    return {
        "name": "enhanced_technical",
        "score": round(score, 2),
        "direction": direction,
        "confidence": round(confidence, 1),
        "details": {
            "rsi": rsi,
            "macd": data.get("macd"),
            "macd_signal": data.get("macd_signal"),
            "macd_bullish": macd_bullish,
            "bb_position": bb_pos,
            "atr_pct": atr_pct
        }
    }


def get_all_new_factors() -> Dict[str, Dict[str, Any]]:
    """
    Compute all new factors from stock-market-pro integration.
    
    Returns:
        Dict of factor_name -> factor_data
    """
    return {
        "cross_market_correlation": compute_cross_market_factor(),
        "macro_risk_sentiment": compute_macro_risk_factor(),
        "enhanced_technical": compute_enhanced_technical_factor()
    }


if __name__ == "__main__":
    # Test the module
    print("=== Stock Market Pro Integration Test ===\n")
    
    print("1. BTC Technicals:")
    tech = fetch_btc_technicals()
    for k, v in tech.items():
        print(f"   {k}: {v}")
    
    print("\n2. Cross Market Correlation:")
    corr = fetch_cross_market_correlation()
    for k, v in corr.items():
        print(f"   {k}: {v}")
    
    print("\n3. Macro Risk:")
    macro = fetch_macro_risk()
    for k, v in macro.items():
        print(f"   {k}: {v}")
    
    print("\n4. All Factors:")
    factors = get_all_new_factors()
    for name, f in factors.items():
        print(f"   {name}: score={f['score']}, dir={f['direction']}, conf={f['confidence']}")
