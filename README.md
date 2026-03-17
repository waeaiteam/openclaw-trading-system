# OpenClaw OKX Trading System (Pure)

纯交易系统仓库，只包含 OKX 交易引擎与因子模块，不包含 xhs/main/world/airdrop 等其他实例代码。

## 包含内容

- `src/ai_trader_v3.py`：基础交易引擎
- `src/ai_trader_v3_1.py`：主策略引擎（建议生产使用）
- `src/orderbook_tracker.py`：订单簿追踪
- `src/liquidation_heatmap_factor.py`：清算热力图因子
- `src/stock_market_pro_factors.py`：跨市场因子（可选增强）

## 项目结构

```text
openclaw-trading-system/
  src/
    ai_trader_v3.py
    ai_trader_v3_1.py
    daily_trade.py
    orderbook_tracker.py
    liquidation_heatmap_factor.py
    stock_market_pro_factors.py
  config/
    .env.example
  deploy/systemd/
    okx-trader.service
    README.md
  scripts/
    start_trader.sh
    run_once.sh
    smoke_test.sh
  requirements.txt
```

## 运行环境

- Python 3.10+
- Linux (systemd)
- 网络可访问 OKX API

安装依赖：

```bash
pip install -r requirements.txt
```

## 快速运行

一次决策测试：

```bash
bash scripts/run_once.sh
```

持续运行：

```bash
bash scripts/start_trader.sh
```

## 关键路径约定

当前代码按 VPS 生产路径设计：
- 运行目录：`/root/.okx-paper`
- 状态文件：`/root/.okx-paper/ai_trader_status.json`
- 数据目录：`/root/.okx-paper/data`

## systemd 部署

```bash
sudo cp deploy/systemd/okx-trader.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now okx-trader.service
sudo systemctl status okx-trader.service
```

## 安全说明

- 不要提交任何密钥、账户配置、真实交易凭据
- `.gitignore` 已排除日志和常见敏感文件
