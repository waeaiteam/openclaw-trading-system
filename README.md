# OpenClaw Trading System

一个可独立部署的交易系统项目封装，核心包含：
- `coevo_dashboard.py`：多实例监控与交易面板（OKX/主 Agent/社媒等）
- `site_platform_ai_monitor.py`：站点状态快照与 AI 分析脚本

## 项目结构

```text
openclaw-trading-system/
  src/
    coevo_dashboard.py
    site_platform_ai_monitor.py
  config/
    .env.example
  deploy/systemd/
    coevo-dashboard.service
    site-ai-monitor.service
  scripts/
    start_dashboard.sh
    run_site_ai_monitor.sh
    smoke_test.sh
  requirements.txt
```

## 环境要求

- Python 3.10+
- Linux（建议 Ubuntu + systemd）
- 网络可访问 OKX/OpenAI/子实例接口

安装依赖：

```bash
pip install -r requirements.txt
```

## 快速启动

```bash
cd src
python3 coevo_dashboard.py
```

默认监听 `127.0.0.1:18091`。

## 配置说明

脚本内部当前使用固定路径（如 `/root/.okx-paper`、`/root/.openclaw`）。  
生产部署时建议：
- 保持与现网一致目录
- 或在二次改造时统一改为环境变量配置

## systemd 部署

1. 复制项目到服务器，例如：`/root/openclaw-trading-system`
2. 根据实际路径修改 `deploy/systemd/*.service`
3. 安装并启动：

```bash
sudo cp deploy/systemd/coevo-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now coevo-dashboard.service
sudo systemctl status coevo-dashboard.service
```

## 健康检查

```bash
bash scripts/smoke_test.sh
```

## 安全建议

- 不要把 API Key、私钥、密码提交到仓库
- 使用 `.env` 或系统密钥管理器保存敏感配置
- 当前仓库已通过 `.gitignore` 屏蔽常见敏感文件
