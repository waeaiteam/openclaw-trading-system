# systemd 部署说明

## 1) 复制服务文件

```bash
sudo cp coevo-dashboard.service /etc/systemd/system/
sudo cp site-ai-monitor.service /etc/systemd/system/
```

## 2) 按实际路径修改

默认路径是：
- `/root/openclaw-trading-system/src/coevo_dashboard.py`
- `/root/openclaw-trading-system/src/site_platform_ai_monitor.py`

如果你部署路径不同，先编辑 service 文件中的 `WorkingDirectory` 和 `ExecStart`。

## 3) 启动 Dashboard

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now coevo-dashboard.service
sudo systemctl status coevo-dashboard.service
```

## 4) 手动触发一次 AI Monitor

```bash
sudo systemctl start site-ai-monitor.service
sudo systemctl status site-ai-monitor.service
```
