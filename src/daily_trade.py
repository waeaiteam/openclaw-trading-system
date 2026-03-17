#!/usr/bin/env python3
"""
OKX Paper Trading 每日交易脚本
确保每天至少交易一次
"""

import json
import requests
import os
from datetime import datetime
import random

# 配置
API_BASE = 'https://wae.asia/api/okx'
STATUS_FILE = '/root/.okx-paper/ai_trader_status.json'
LOG_FILE = '/root/.okx-paper/daily_trade.log'

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')

def get_btc_price():
    """获取 BTC 价格"""
    try:
        res = requests.get('https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT', timeout=10)
        data = res.json()['data'][0]
        return float(data['last'])
    except Exception as e:
        log(f"获取价格失败: {e}")
        return 70000

def get_fear_greed():
    """获取恐惧贪婪指数"""
    try:
        res = requests.get('https://api.alternative.me/fng/?limit=1', timeout=10)
        return int(res.json()['data'][0]['value'])
    except:
        return 50

def load_status():
    """加载状态"""
    try:
        with open(STATUS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {
            "trader_status": "active",
            "account_type": "paper",
            "initial_capital": 1000,
            "current_equity": 1000,
            "trade_count": 0,
            "last_trade_date": None
        }

def save_status(status):
    """保存状态"""
    with open(STATUS_FILE, 'w') as f:
        json.dump(status, f, indent=2)

def execute_trade(direction, price):
    """执行交易"""
    try:
        res = requests.post(f'{API_BASE}/paper', json={
            'action': 'trade',
            'direction': direction,
            'price': price,
            'size': 0.01,  # 小额测试
            'leverage': 1
        }, timeout=30)
        result = res.json()
        log(f"交易结果: {result}")
        return result
    except Exception as e:
        log(f"交易失败: {e}")
        return None

def main():
    log("=" * 50)
    log("开始每日交易检查")
    
    # 加载状态
    status = load_status()
    today = datetime.now().strftime('%Y-%m-%d')
    
    # 检查今天是否已交易
    if status.get('last_trade_date') == today:
        log(f"今天 {today} 已完成交易，跳过")
        return
    
    # 获取市场数据
    btc_price = get_btc_price()
    fg_value = get_fear_greed()
    
    log(f"BTC 价格: ${btc_price:,.0f}")
    log(f"恐惧贪婪指数: {fg_value}")
    
    # 简单决策逻辑
    # 如果恐惧贪婪指数 < 30（恐惧），买入
    # 如果恐惧贪婪指数 > 70（贪婪），卖出
    # 否则随机选择
    
    if fg_value < 30:
        direction = 'long'
        reason = f"恐惧贪婪指数 {fg_value} < 30，逆向买入"
    elif fg_value > 70:
        direction = 'short'
        reason = f"恐惧贪婪指数 {fg_value} > 70，逆向卖出"
    else:
        # 随机选择
        direction = random.choice(['long', 'short'])
        reason = f"恐惧贪婪指数 {fg_value} 中性，随机选择 {direction}"
    
    log(f"决策: {direction} - {reason}")
    
    # 执行交易
    result = execute_trade(direction, btc_price)
    
    if result:
        # 更新状态
        status['trade_count'] = status.get('trade_count', 0) + 1
        status['last_trade_date'] = today
        status['last_trade'] = {
            'timestamp': datetime.now().isoformat(),
            'direction': direction,
            'price': btc_price,
            'reason': reason,
            'fg_value': fg_value
        }
        save_status(status)
        log(f"✅ 交易完成，总交易次数: {status['trade_count']}")
    else:
        log("❌ 交易失败")
    
    log("每日交易检查完成")
    log("=" * 50)

if __name__ == '__main__':
    main()
