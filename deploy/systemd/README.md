# systemd 部署说明（纯交易系统）

## 1) 复制服务文件

```bash
sudo cp okx-trader.service /etc/systemd/system/
```

## 2) 检查路径

默认路径：
- `/root/openclaw-trading-system/src/ai_trader_v3_1.py`

如果实际路径不同，请先修改 service 文件中的 `WorkingDirectory` 与 `ExecStart`。

## 3) 启动服务

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now okx-trader.service
sudo systemctl status okx-trader.service
```

## 4) 查看日志

```bash
journalctl -u okx-trader.service -f
```
