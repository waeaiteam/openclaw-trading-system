#!/usr/bin/env python3
"""
清算热力图因子 - 最终版

核心逻辑（简化）：
1. 获取清算分布
2. 计算上方 vs 下方清算量比例
3. 上方清算量 << 下方 → 价格涨过密集区 → 做空
4. 下方清算量 << 上方 → 价格跌过密集区 → 做多

关键：看清算量的上下分布比例
"""
import os
import json
import time
import subprocess
from typing import Dict, Optional

CLAW402_SCRIPT = "/root/.openclaw/workspace-trader/skills/claw402/scripts/query.mjs"
WALLET_KEY = os.environ.get("WALLET_PRIVATE_KEY", "0xec50fb901409082680e4487079141c909484c7f0336ac9bd5a61880327128017")
CACHE_FILE = "/root/.okx-paper/data/liquidation_heatmap_cache.json"
CACHE_TTL = 3600

# 参数
PARAMS = {
    # 稀疏区判定：上方/下方清算量占比 < 这个值
    'sparse_threshold': 0.15,  # 15%
    
    # 信号强度阈值
    'min_imbalance': 3.0,  # 上方/下方比例 > 3:1 或 < 1:3
}


def fetch_heatmap(symbol: str = "BTCUSDT") -> Optional[Dict]:
    """获取清算热力图"""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            cache = json.load(f)
        if time.time() - cache.get("timestamp", 0) < CACHE_TTL:
            return cache.get("data")
    
    env = os.environ.copy()
    env["WALLET_PRIVATE_KEY"] = WALLET_KEY
    
    cmd = ["node", CLAW402_SCRIPT, "/api/v1/coinank/liquidation/heat-map",
           "exchange=Binance", f"symbol={symbol}", "interval=1d"]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, env=env,
                                cwd="/root/.openclaw/workspace-trader/skills/claw402", timeout=30)
        if result.returncode != 0:
            return None
        
        raw = json.loads(result.stdout)
        if raw.get("data", {}).get("success"):
            data = raw["data"]["data"]
            normalized = {
                "tickSize": data.get("tickSize", 50),
                "liqData": data.get("liqHeatMap", {}).get("data", []),
            }
            with open(CACHE_FILE, 'w') as f:
                json.dump({"data": normalized, "timestamp": time.time()}, f)
            return normalized
    except:
        pass
    return None


def compute_liquidation_heatmap_factor(current_price: float, symbol: str = "BTCUSDT") -> Dict:
    """
    计算清算热力图因子
    
    核心逻辑：
    - 上方清算量 << 下方 → 价格涨过密集区，上方稀疏 → 做空
    - 下方清算量 << 上方 → 价格跌过密集区，下方稀疏 → 做多
    """
    default = {
        "name": "liquidation_heatmap",
        "score": 50,
        "direction": "neutral",
        "confidence": 20,
        "details": {"error": "no_data"}
    }
    
    heatmap = fetch_heatmap(symbol)
    if not heatmap:
        return default
    
    liq_data = heatmap.get("liqData", [])
    tick_size = heatmap.get("tickSize", 50)
    
    # 聚合价格区间的清算量
    price_volumes = {}
    for item in liq_data:
        if len(item) >= 3:
            try:
                idx = int(item[0])
                vol = float(item[2])
                if vol > 0:
                    if idx not in price_volumes:
                        price_volumes[idx] = 0
                    price_volumes[idx] += vol
            except:
                continue
    
    if not price_volumes:
        return default
    
    # 找到价格中心（最大清算量的索引 ≈ 当前价格附近）
    max_idx = max(price_volumes.items(), key=lambda x: x[1])[0]
    
    # 计算上方和下方的清算量
    above_total = sum(vol for idx, vol in price_volumes.items() if idx > max_idx)
    below_total = sum(vol for idx, vol in price_volumes.items() if idx < max_idx)
    total = above_total + below_total
    
    if total == 0:
        return default
    
    # 计算占比
    above_pct = above_total / total
    below_pct = below_total / total
    
    # 计算信号
    score = 50.0
    direction = "neutral"
    confidence = 30.0
    
    # 判断稀疏区和信号
    # 上方清算量很少 → 价格涨过了密集区 → 上方没空头了 → 做空
    if above_pct < PARAMS['sparse_threshold']:
        # 上方稀疏
        imbalance = below_total / above_total if above_total > 0 else float('inf')
        
        if imbalance >= PARAMS['min_imbalance']:
            # 强烈不平衡，做空信号
            signal_strength = min(imbalance / 10, 1.0)  # 归一化到 0-1
            score = 25 + signal_strength * 20  # 25-45，偏向空
            direction = "short"
            confidence = 50 + signal_strength * 40  # 50-90
    
    # 下方清算量很少 → 价格跌过了密集区 → 下方没多头了 → 做多
    elif below_pct < PARAMS['sparse_threshold']:
        # 下方稀疏
        imbalance = above_total / below_total if below_total > 0 else float('inf')
        
        if imbalance >= PARAMS['min_imbalance']:
            signal_strength = min(imbalance / 10, 1.0)
            score = 55 + signal_strength * 20  # 55-75，偏向多
            direction = "long"
            confidence = 50 + signal_strength * 40
    
    return {
        "name": "liquidation_heatmap",
        "score": round(score, 2),
        "direction": direction,
        "confidence": round(confidence, 2),
        "details": {
            "current_idx": max_idx,
            "above_total_m": round(above_total / 1e6, 1),
            "below_total_m": round(below_total / 1e6, 1),
            "above_pct": round(above_pct * 100, 2),
            "below_pct": round(below_pct * 100, 2),
            "imbalance": round(below_total / above_total, 2) if above_total > 0 else float('inf'),
            "above_sparse": above_pct < PARAMS['sparse_threshold'],
            "below_sparse": below_pct < PARAMS['sparse_threshold'],
            "tick_size": tick_size
        }
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--price", type=float, default=84500)
    args = parser.parse_args()
    
    print(f"\n清算热力图因子（最终版）: @ {args.price}")
    print("=" * 60)
    
    result = compute_liquidation_heatmap_factor(args.price)
    print(json.dumps(result, indent=2))
