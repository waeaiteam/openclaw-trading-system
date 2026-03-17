#!/usr/bin/env python3
import base64
import json
import math
import statistics
import os
import re
import select
import shlex
import subprocess
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from hmac import new as hmac_new
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse, urlencode

import requests

ROOT = '/root/.okx-paper'
DATA_DIR = f'{ROOT}/data'
STATUS_FILE = f'{ROOT}/ai_trader_status.json'
ACCOUNT_FILE = f'{DATA_DIR}/account.json'
PAPER_POS_FILE = f'{DATA_DIR}/positions.json'
TRADES_FILE = f'{DATA_DIR}/trades.json'
DECISIONS_FILE = f'{DATA_DIR}/decisions.json'
EVOLUTION_FILE = f'{DATA_DIR}/evolution_history.json'
COST_LEDGER_FILE = f'{DATA_DIR}/cost_ledger.json'
MODE_FILE = f'{DATA_DIR}/dashboard_mode.json'
OKX_CONFIG = '/root/.okx/config.toml'
AI_LOG_FILE = '/root/.okx-paper/ai_trader.log'
AI_STDERR_FILE = '/root/ai_trader_output.log'
TRADER_OPENCLAW_ROOT = '/root/.openclaw'
TRADER_SESSIONS_JSON = f'{TRADER_OPENCLAW_ROOT}/agents/trader/sessions/sessions.json'
TRADER_SESSIONS_DIR = f'{TRADER_OPENCLAW_ROOT}/agents/trader/sessions'
TRADER_MEMORY_FILE = f'{TRADER_OPENCLAW_ROOT}/workspace-trader/MEMORY.md'
XHS_PROXY_BASE = 'http://127.0.0.1:18092'
MAIN_PROXY_BASE = 'http://127.0.0.1:18093'
WORLD_PROXY_BASE = 'http://127.0.0.1:18094'
AIRDROP_PROXY_BASE = 'http://127.0.0.1:18095'
TZ_CN = timezone(timedelta(hours=8))

HOST = '127.0.0.1'
PORT = 18091
OKX_BASE = 'https://www.okx.com'
OPENCLAW_LOG_DIR = '/tmp/openclaw'
CHAT_ALLOWED_AGENTS = {'main', 'trader', 'zishu'}
BACKTEST_CACHE_FILE = f'{DATA_DIR}/backtest_ui_cache.json'
MAX_BACKTEST_BARS = 12000
OKX_PUBLIC_TIMEOUT = 12
CROSS_MARKET_CACHE_FILE = f'{DATA_DIR}/cross_market_corr_daily.json'
CROSS_MARKET_ROLL_DAYS = 30
CROSS_MARKET_WARM_DAYS = 45
CROSS_MARKET_CACHE_MAX_POINTS = 6000
MACRO_RISK_CACHE_FILE = f'{DATA_DIR}/macro_risk_daily.json'
MACRO_RISK_WARM_DAYS = 15
MACRO_RISK_CACHE_MAX_POINTS = 6000
FEAR_GREED_CACHE_FILE = f'{DATA_DIR}/fear_greed_daily.json'
FEAR_GREED_WARM_DAYS = 35
FEAR_GREED_CACHE_MAX_POINTS = 8000

CLAW402_SKILL_DIR = '/root/.openclaw/workspace/skills/claw402'
CLAW402_SKILL_MD = f'{CLAW402_SKILL_DIR}/SKILL.md'
CLAW402_QUERY_SCRIPT = f'{CLAW402_SKILL_DIR}/scripts/query.mjs'
CLAW402_GATEWAY_CONF = '/root/.config/systemd/user/openclaw-gateway.service.d/40-claw402.conf'
CLAW402_USAGE_FILE = f'{DATA_DIR}/claw402_usage_stats.json'
CLAW402_BASE_RPC = 'https://mainnet.base.org'
CLAW402_BASE_USDC = '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913'
CLAW402_USAGE_LOCK = threading.Lock()
CLAW402_CATALOG_LOCK = threading.Lock()
CLAW402_CATALOG_CACHE: Dict[str, Any] = {'mtime': 0.0, 'payload': {'ok': True, 'categories': [], 'endpoints': []}}
SITE_AI_MONITOR_SCRIPT = '/root/site_platform_ai_monitor.py'
SITE_AI_REPORT_FILE = f'{DATA_DIR}/site_ai_analysis.json'
SITE_AI_LOCK = threading.Lock()
SITE_AI_CACHE_TTL_MS = 5 * 60 * 1000

RELEASE_VERSION = 'v4.1.0-regime-balance-calibrated'
RELEASE_DATE = '2026-03-15'
RELEASE_TITLE_ZH = '多状态稳健增强版 v4.1'
RELEASE_TITLE_EN = 'Multi-Regime Stability Upgrade v4.1'
RELEASE_NOTES_ZH = [
    '把默认画像重新标定为 v4.1 参数集：更高的牛市门槛、更短的持仓预算、更紧的追踪止盈，优先追求更平滑的净值曲线。',
    '最新 3000 根 1H 参考样本下，默认回测参数落在目标区间附近：年化约 38.1%，最大回撤约 2.82%。',
    '最近 720 根窗口从负收益修正为正收益，虽然仍可能阶段性跑输 BTC 单边拉升，但系统会尽量保持净值曲线向上。',
    'Dashboard、默认回测参数、trader 固定文档与线上交易脚本同步升级到 v4.1 口径。',
    '继续坚持真实披露：不承诺盈利，不隐藏回撤，不为了凑交易频率而强行开仓。',
]
RELEASE_NOTES_EN = [
    'Retuned the default v4 profile with a higher bull gate, shorter hold budget, and tighter trailing logic to stabilize the equity curve.',
    'On the latest 3000-bar 1H reference sample, the calibrated default runs near the target band: about 38.1% annualized with 2.82% max drawdown.',
    'The latest 720-bar window is back to positive absolute return, even if BTC can still outperform during sharp one-way rallies.',
    'Dashboard defaults, trader docs, and the VPS-side trading script are now aligned to the v4.1 calibrated system.',
    'Truthfulness remains mandatory: no profit guarantees, no hidden drawdown, and no forced trades for activity.',
]
RELEASE_FILES = [
    '/root/.okx-paper/ai_trader_v3_1.py',
    '/root/.openclaw/agents/trader/agent/system.md',
    '/root/.openclaw/workspace-trader/README.md',
    '/root/.openclaw/workspace-trader/HEARTBEAT.md',
    '/root/.okx-paper/data/factor_weights.json',
]

INSTANCE_REGISTRY_FILE = f'{DATA_DIR}/coevo_instances.json'
INSTANCE_REGISTRY_LOCK = threading.Lock()
INSTANCE_REGISTRY_CACHE: Dict[str, Any] = {'mtime': -1.0, 'instances': []}
LEGACY_INSTANCE_IDS = {'okx', 'xhs', 'main', 'world', 'airdrop'}
INSTANCE_STATUS_TIMEOUT_DEFAULT = 15
INSTANCE_HTTP_TIMEOUT_DEFAULT = 20

DEFAULT_INSTANCE_REGISTRY: List[Dict[str, Any]] = [
    {
        'id': 'okx',
        'name_zh': 'OKX策略交易实例',
        'name_en': 'OKX Trading Instance',
        'description_zh': 'OpenClaw Trader 子Agent实例，负责交易策略执行与风控监控。',
        'description_en': 'OpenClaw Trader sub-agent for strategy execution and risk monitoring.',
        'category_zh': '交易实例',
        'category_en': 'Trading',
        'order': 10,
        'enabled': True,
        'base_url': '',
        'raw_mode': 'local_okx',
        'status_mode': 'local_okx',
        'status_path': '/api/status',
        'status_timeout': 8,
        'chat_agent': 'trader',
    },
    {
        'id': 'xhs',
        'name_zh': '小红书实例',
        'name_en': 'Xiaohongshu Instance',
        'description_zh': 'zishu 子Agent实例，负责内容发布与运营节奏监控。',
        'description_en': 'zishu sub-agent for social content publishing and cadence monitoring.',
        'category_zh': '社媒实例',
        'category_en': 'Social',
        'order': 20,
        'enabled': True,
        'base_url': XHS_PROXY_BASE,
        'raw_mode': 'proxy',
        'status_mode': 'proxy_json',
        'status_path': '/api/xhs-status',
        'status_timeout': 25,
        'chat_agent': 'zishu',
    },
    {
        'id': 'main',
        'name_zh': '主Agent实例',
        'name_en': 'Main Agent Instance',
        'description_zh': 'main 子Agent实例，负责主流程协作与跨模块调度。',
        'description_en': 'main sub-agent for orchestration and cross-module coordination.',
        'category_zh': '核心实例',
        'category_en': 'Core',
        'order': 30,
        'enabled': True,
        'base_url': MAIN_PROXY_BASE,
        'raw_mode': 'proxy',
        'status_mode': 'proxy_json',
        'status_path': '/api/main-status',
        'status_timeout': 12,
        'chat_agent': 'main',
    },
    {
        'id': 'world',
        'name_zh': 'WorldMonitor实例',
        'name_en': 'WorldMonitor Instance',
        'description_zh': '全球监控子实例，用于世界状态与外部看板展示。',
        'description_en': 'Global monitoring sub-instance for external world state dashboards.',
        'category_zh': '监控实例',
        'category_en': 'Monitoring',
        'order': 40,
        'enabled': True,
        'base_url': WORLD_PROXY_BASE,
        'raw_mode': 'proxy',
        'status_mode': 'proxy_world',
        'status_path': '/',
        'status_timeout': 12,
        'chat_agent': '',
    },
    {
        'id': 'airdrop',
        'name_zh': '空投管理实例',
        'name_en': 'Airdrop Instance',
        'description_zh': '空投管理子实例，负责多钱包与任务管理。',
        'description_en': 'Airdrop management sub-instance for wallet and task operations.',
        'category_zh': '运营实例',
        'category_en': 'Operations',
        'order': 50,
        'enabled': True,
        'base_url': AIRDROP_PROXY_BASE,
        'raw_mode': 'proxy',
        'status_mode': 'local_airdrop',
        'status_path': '/api/airdrop-status',
        'status_timeout': 10,
        'chat_agent': '',
    },
]


def _to_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v if v is not None else '').strip().lower()
    if s in {'1', 'true', 'yes', 'on', 'y'}:
        return True
    if s in {'0', 'false', 'no', 'off', 'n'}:
        return False
    return default


def _normalize_instance_item(raw: Any, base: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    out = dict(base or {})
    for key in [
        'id',
        'name_zh',
        'name_en',
        'description_zh',
        'description_en',
        'category_zh',
        'category_en',
        'order',
        'enabled',
        'base_url',
        'raw_mode',
        'status_mode',
        'status_path',
        'status_timeout',
        'chat_agent',
    ]:
        if key in raw:
            out[key] = raw.get(key)

    iid = str(out.get('id') or '').strip().lower()
    if not iid:
        return None
    out['id'] = iid
    out['name_zh'] = str(out.get('name_zh') or iid.upper())
    out['name_en'] = str(out.get('name_en') or out['name_zh'])
    out['description_zh'] = str(out.get('description_zh') or '')
    out['description_en'] = str(out.get('description_en') or out['description_zh'])
    out['category_zh'] = str(out.get('category_zh') or '实例')
    out['category_en'] = str(out.get('category_en') or 'Instance')
    out['order'] = int(out.get('order') or 1000)
    out['enabled'] = _to_bool(out.get('enabled'), True)
    out['base_url'] = str(out.get('base_url') or '').strip()
    out['raw_mode'] = str(out.get('raw_mode') or 'proxy').strip().lower()
    out['status_mode'] = str(out.get('status_mode') or 'proxy_json').strip().lower()
    out['status_path'] = str(out.get('status_path') or '/').strip() or '/'
    out['status_timeout'] = max(3, min(int(out.get('status_timeout') or INSTANCE_STATUS_TIMEOUT_DEFAULT), 60))
    out['chat_agent'] = str(out.get('chat_agent') or '').strip().lower()
    return out


def get_instance_registry() -> List[Dict[str, Any]]:
    mtime = -1.0
    if os.path.exists(INSTANCE_REGISTRY_FILE):
        try:
            mtime = os.path.getmtime(INSTANCE_REGISTRY_FILE)
        except Exception:
            mtime = -1.0
    with INSTANCE_REGISTRY_LOCK:
        if INSTANCE_REGISTRY_CACHE.get('mtime') == mtime and INSTANCE_REGISTRY_CACHE.get('instances'):
            return [dict(x) for x in INSTANCE_REGISTRY_CACHE.get('instances', [])]

        defaults = [_normalize_instance_item(x) for x in DEFAULT_INSTANCE_REGISTRY]
        default_map = {x['id']: x for x in defaults if isinstance(x, dict) and x.get('id')}
        payload = read_json(INSTANCE_REGISTRY_FILE, [])
        items = payload.get('instances') if isinstance(payload, dict) else payload
        if not isinstance(items, list):
            items = []

        merged: Dict[str, Dict[str, Any]] = {}
        for iid, item in default_map.items():
            merged[iid] = dict(item)

        for raw in items:
            if not isinstance(raw, dict):
                continue
            rid = str(raw.get('id') or '').strip().lower()
            if not rid:
                continue
            norm = _normalize_instance_item(raw, merged.get(rid))
            if not norm:
                continue
            merged[rid] = norm

        rows = [x for x in merged.values() if x.get('enabled')]
        rows.sort(key=lambda x: (int(x.get('order') or 1000), str(x.get('id') or '')))
        INSTANCE_REGISTRY_CACHE['mtime'] = mtime
        INSTANCE_REGISTRY_CACHE['instances'] = [dict(x) for x in rows]
        return [dict(x) for x in rows]


def get_instance_meta(instance_id: str) -> Optional[Dict[str, Any]]:
    iid = str(instance_id or '').strip().lower()
    if not iid:
        return None
    for item in get_instance_registry():
        if str(item.get('id') or '') == iid:
            return item
    return None


def instance_public_meta(item: Dict[str, Any]) -> Dict[str, Any]:
    iid = str(item.get('id') or '')
    if iid in LEGACY_INSTANCE_IDS:
        legacy = f'/{iid}'
    else:
        legacy = ''
    return {
        'id': iid,
        'name_zh': str(item.get('name_zh') or iid),
        'name_en': str(item.get('name_en') or item.get('name_zh') or iid),
        'description_zh': str(item.get('description_zh') or ''),
        'description_en': str(item.get('description_en') or item.get('description_zh') or ''),
        'category_zh': str(item.get('category_zh') or '实例'),
        'category_en': str(item.get('category_en') or 'Instance'),
        'chat_enabled': bool(item.get('chat_agent')),
        'chat_agent': str(item.get('chat_agent') or ''),
        'instance_path': f'/instance/{iid}',
        'raw_path': f'/instance/{iid}/raw',
        'legacy_path': legacy,
    }


def list_instance_public_meta() -> List[Dict[str, Any]]:
    return [instance_public_meta(x) for x in get_instance_registry()]


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def read_json(path: str, default: Any) -> Any:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def write_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def to_num(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def tail_file(path: str, lines: int = 40) -> List[str]:
    try:
        rows = open(path, 'r', encoding='utf-8', errors='replace').read().splitlines()
        rows = [x for x in rows if x.strip()]
        return rows[-lines:]
    except Exception:
        return []


def read_text(path: str, default: str = '') -> str:
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read()
    except Exception:
        return default


def list_files(dir_path: str, suffix: str) -> List[str]:
    try:
        rows = []
        for name in os.listdir(dir_path):
            if not name.endswith(suffix):
                continue
            full = os.path.join(dir_path, name)
            if os.path.isfile(full):
                rows.append(full)
        rows.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return rows
    except Exception:
        return []


def fmt_ms(ms: Any) -> str:
    try:
        ts = int(ms) / 1000.0
        return datetime.fromtimestamp(ts, timezone.utc).astimezone(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return '-'


def parse_time_to_ms(raw: Any) -> int | None:
    s = str(raw or '').strip()
    if not s:
        return None
    if s.isdigit():
        try:
            v = int(s)
            return v if v > 10_000_000_000 else v * 1000
        except Exception:
            return None
    s2 = s.replace('Z', '+00:00')
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        pass
    # fallback: common format yyyy-mm-dd HH:MM:SS
    try:
        dt = datetime.strptime(s[:19], '%Y-%m-%d %H:%M:%S').replace(tzinfo=TZ_CN)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _extract_text_from_content(content: Any) -> str:
    if not isinstance(content, list):
        return ''
    parts: List[str] = []
    for c in content:
        if not isinstance(c, dict):
            continue
        typ = str(c.get('type', ''))
        if typ in {'text', 'input_text', 'output_text'}:
            txt = str(c.get('text', ''))
            if txt.strip():
                parts.append(txt.strip())
        elif typ == 'toolCall':
            nm = str(c.get('name', 'tool'))
            parts.append(f'[toolCall:{nm}]')
        elif typ == 'toolResult':
            parts.append('[toolResult]')
    return '\n'.join(parts).strip()


def parse_trader_session_index() -> Dict[str, Any]:
    data = read_json(TRADER_SESSIONS_JSON, {})
    item = (data or {}).get('agent:trader:main') or {}
    updated = int(item.get('updatedAt') or 0)
    age_min = None
    if updated > 0:
        age_min = round((now_ms() - updated) / 60000, 2)
    return {
        'session_id': item.get('sessionId', ''),
        'session_file': item.get('sessionFile', ''),
        'updated_at_ms': updated,
        'updated_at': fmt_ms(updated) if updated else '-',
        'minutes_since_update': age_min,
        'channel': (item.get('deliveryContext') or {}).get('channel', item.get('channel', '')),
        'account_id': (item.get('deliveryContext') or {}).get('accountId', item.get('lastAccountId', '')),
        'to': (item.get('deliveryContext') or {}).get('to', item.get('lastTo', '')),
    }


def get_latest_trader_session_file(session_idx: Dict[str, Any]) -> str:
    sf = str(session_idx.get('session_file', '')).strip()
    if sf and os.path.isfile(sf):
        return sf
    sid = str(session_idx.get('session_id', '')).strip()
    if sid:
        p = os.path.join(TRADER_SESSIONS_DIR, f'{sid}.jsonl')
        if os.path.isfile(p):
            return p
    files = list_files(TRADER_SESSIONS_DIR, '.jsonl')
    return files[0] if files else ''


def parse_trader_session_events(session_path: str) -> Dict[str, Any]:
    events: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    if not session_path or not os.path.isfile(session_path):
        return {'events': events, 'errors': errors, 'latest_user': {}, 'latest_assistant': {}}
    lines = tail_file(session_path, 1200)
    err_kw = ['error', 'failed', 'timeout', 'timed out', 'exception', 'traceback', 'invalid data', '10012', '400', '500', '429', '失败', '错误']
    for raw in lines:
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        role = ''
        text = ''
        ts = str(obj.get('timestamp', ''))
        typ = str(obj.get('type', ''))
        if typ == 'message' and isinstance(obj.get('message'), dict):
            msg = obj.get('message') or {}
            role = str(msg.get('role', ''))
            text = _extract_text_from_content(msg.get('content'))
            if not text:
                err = str(msg.get('errorMessage', '')).strip()
                if err:
                    text = err
        elif typ == 'item' and isinstance(obj.get('message'), dict):
            msg = obj.get('message') or {}
            role = str(msg.get('role', ''))
            text = _extract_text_from_content(msg.get('content'))
        elif typ == 'response_item' and isinstance(obj.get('item'), dict):
            it = obj.get('item') or {}
            role = str(it.get('role', 'assistant'))
            text = _extract_text_from_content(it.get('content'))
        if not text:
            continue
        summary = text.strip().replace('\n', ' ')
        if len(summary) > 260:
            summary = summary[:260] + '...'
        ev = {'timestamp': ts, 'role': role, 'summary': summary}
        events.append(ev)
        low = summary.lower()
        if any(k in low for k in err_kw):
            errors.append(ev)

    latest_user = {}
    latest_assistant = {}
    for ev in reversed(events):
        if not latest_user and ev.get('role') == 'user':
            latest_user = ev
        if not latest_assistant and ev.get('role') == 'assistant':
            latest_assistant = ev
        if latest_user and latest_assistant:
            break
    return {
        'events': events[-120:],
        'errors': errors[-60:],
        'latest_user': latest_user,
        'latest_assistant': latest_assistant,
    }


def load_trader_memory_head(lines: int = 80) -> str:
    text = read_text(TRADER_MEMORY_FILE, '')
    if not text:
        return '-'
    return '\n'.join(text.splitlines()[:lines])


def _is_trade_today(ts_raw: Any) -> bool:
    ms = parse_time_to_ms(ts_raw)
    if ms is None:
        return False
    dt = datetime.fromtimestamp(ms / 1000.0, timezone.utc).astimezone(TZ_CN)
    today = datetime.now(TZ_CN).date()
    return dt.date() == today


def filter_today_trades(trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in trades:
        if not isinstance(t, dict):
            continue
        if _is_trade_today(t.get('ts')):
            out.append(t)
    if out:
        return out[-80:]
    return trades[-30:]


def build_trader_monitor(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    runtime = parse_trader_session_index()
    session_file = get_latest_trader_session_file(runtime)
    session_data = parse_trader_session_events(session_file)
    return {
        'runtime': {**runtime, 'session_file': session_file},
        'events': session_data.get('events', []),
        'errors': session_data.get('errors', []),
        'latest_dialogue': {
            'user': session_data.get('latest_user', {}),
            'assistant': session_data.get('latest_assistant', {}),
        },
        'memory_head': load_trader_memory_head(80),
        'today_trades': filter_today_trades(trades),
    }


def fetch_xhs_status() -> Dict[str, Any]:
    try:
        r = requests.get(f'{XHS_PROXY_BASE}/api/xhs-status', timeout=25)
        if r.status_code != 200:
            return {'ok': False, 'error': f'http_{r.status_code}'}
        data = r.json()
        data['ok'] = True
        return data
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def fetch_xhs_image(path: str) -> Dict[str, Any]:
    try:
        r = requests.get(f'{XHS_PROXY_BASE}/api/xhs-image', params={'path': path}, timeout=12)
        if r.status_code != 200:
            return {'ok': False, 'status': r.status_code, 'content': b'', 'ctype': 'application/octet-stream'}
        return {
            'ok': True,
            'status': 200,
            'content': r.content,
            'ctype': r.headers.get('Content-Type', 'application/octet-stream'),
        }
    except Exception:
        return {'ok': False, 'status': 502, 'content': b'', 'ctype': 'application/octet-stream'}


def fetch_main_status() -> Dict[str, Any]:
    try:
        r = requests.get(f'{MAIN_PROXY_BASE}/api/main-status', timeout=10)
        if r.status_code != 200:
            return {'ok': False, 'error': f'http_{r.status_code}'}
        data = r.json()
        data['ok'] = True
        return data
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def fetch_world_status() -> Dict[str, Any]:
    try:
        r = requests.get(f'{WORLD_PROXY_BASE}/', timeout=10)
        text = r.text if r.status_code == 200 else ''
        title = '-'
        m = re.search(r'<title>(.*?)</title>', text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            title = _clip_text(m.group(1).strip(), 120)
        return {
            'ok': r.status_code == 200,
            'status_code': r.status_code,
            'title': title,
            'reachable': r.status_code == 200,
        }
    except Exception as e:
        return {'ok': False, 'status_code': 502, 'title': '-', 'reachable': False, 'error': str(e)}


def fetch_airdrop_status() -> Dict[str, Any]:
    """获取空投管理系统状态"""
    try:
        import sqlite3
        db_path = '/root/airdrop-farmer/airdrop-manager/data/airdrop.db'
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 钱包数量
        cursor.execute("SELECT COUNT(*) FROM wallets WHERE status='active'")
        wallet_count = cursor.fetchone()[0]
        
        # 项目数量
        cursor.execute("SELECT COUNT(*) FROM projects WHERE status='active'")
        project_count = cursor.fetchone()[0]
        
        # 任务数量
        cursor.execute("SELECT COUNT(*) FROM tasks WHERE status='pending'")
        pending_tasks = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tasks WHERE status='completed'")
        completed_tasks = cursor.fetchone()[0]
        
        # 项目列表
        cursor.execute("SELECT id, name, chain, status, expected_value FROM projects WHERE status='active'")
        projects = []
        for row in cursor.fetchall():
            projects.append({
                'id': row[0],
                'name': row[1],
                'chain': row[2],
                'status': row[3],
                'expected_value': row[4]
            })
        
        # 钱包列表
        cursor.execute("SELECT id, address, balance, status FROM wallets WHERE status='active'")
        wallets = []
        for row in cursor.fetchall():
            wallets.append({
                'id': row[0],
                'address': row[1],
                'balance': row[2],
                'status': row[3]
            })
        
        # 机会列表（最新的10个）
        cursor.execute("SELECT id, name, chain, status FROM opportunities ORDER BY id DESC LIMIT 10")
        opportunities = []
        for row in cursor.fetchall():
            opportunities.append({
                'id': row[0],
                'name': row[1],
                'chain': row[2],
                'status': row[3]
            })
        
        # 每日统计
        cursor.execute("SELECT date, total_claimed, tasks_completed FROM daily_stats ORDER BY date DESC LIMIT 7")
        daily_stats = []
        for row in cursor.fetchall():
            daily_stats.append({
                'date': row[0],
                'total_claimed': row[1],
                'tasks_completed': row[2]
            })
        
        conn.close()
        
        return {
            'ok': True,
            'health': {
                'status': '正常' if wallet_count >= 6 else '警告',
                'wallets': wallet_count,
                'projects': project_count,
            },
            'stats': {
                'wallet_count': wallet_count,
                'project_count': project_count,
                'pending_tasks': pending_tasks,
                'completed_tasks': completed_tasks,
            },
            'projects': projects,
            'wallets': wallets,
            'opportunities': opportunities,
            'daily_stats': daily_stats,
        }
    except Exception as e:
        return {'ok': False, 'error': str(e), 'health': {'status': '异常'}, 'stats': {}}


def fetch_proxy_json(base_url: str, status_path: str, timeout_sec: int) -> Dict[str, Any]:
    target = str(base_url or '').rstrip('/') + '/' + str(status_path or '/').lstrip('/')
    try:
        r = requests.get(target, timeout=max(3, min(int(timeout_sec or INSTANCE_STATUS_TIMEOUT_DEFAULT), 60)))
        if r.status_code != 200:
            return {'ok': False, 'error': f'http_{r.status_code}', 'status_code': r.status_code}
        data = r.json()
        if isinstance(data, dict):
            data = dict(data)
            data['ok'] = bool(data.get('ok', True))
            return data
        return {'ok': True, 'data': data}
    except Exception as e:
        return {'ok': False, 'error': str(e), 'status_code': 502}


def _health_level_from_text(text: str, ok: bool = True) -> str:
    s = str(text or '').strip().lower()
    if not ok:
        return 'down'
    if s in {'normal', 'healthy', 'up', 'running', 'active', 'ok'}:
        return 'up'
    if any(x in s for x in ['警告', 'warning', 'degraded', 'risk']):
        return 'degraded'
    if any(x in s for x in ['异常', 'down', 'error', 'failed', 'offline']):
        return 'down'
    return 'up'


def _normalize_okx_instance_status(item: Dict[str, Any], raw: Dict[str, Any], ok: bool, latency_ms: int, err: str) -> Dict[str, Any]:
    trader_status = str(raw.get('trader_status') or '')
    proc = str(raw.get('processing_state') or '')
    score = (raw.get('signal') or {}).get('score') if isinstance(raw.get('signal'), dict) else None
    strategy = str(raw.get('active_strategy') or '-')
    mode = str(raw.get('mode') or 'paper')
    level = _health_level_from_text(trader_status, ok)
    if ok and trader_status.lower() in {'active', 'running'}:
        level = 'up'
    elif ok and trader_status:
        level = 'degraded'
    return {
        'id': item.get('id'),
        'name_zh': item.get('name_zh'),
        'name_en': item.get('name_en'),
        'category_zh': item.get('category_zh'),
        'category_en': item.get('category_en'),
        'ok': bool(ok),
        'status_level': level,
        'status_text_zh': '正常' if level == 'up' else ('警告' if level == 'degraded' else '异常'),
        'status_text_en': 'Up' if level == 'up' else ('Degraded' if level == 'degraded' else 'Down'),
        'summary_zh': f"策略:{strategy} | 评分:{score if score is not None else '-'} | 模式:{'实盘' if mode == 'live' else '模拟'}",
        'summary_en': f"Strategy:{strategy} | Score:{score if score is not None else '-'} | Mode:{'Live' if mode == 'live' else 'Paper'}",
        'latency_ms': latency_ms,
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'chat_agent': str(item.get('chat_agent') or ''),
        'instance_path': f"/instance/{item.get('id')}",
        'legacy_path': f"/{item.get('id')}" if item.get('id') in LEGACY_INSTANCE_IDS else '',
        'error': err or '',
        'metrics': {
            'trader_status': trader_status or '-',
            'processing_state': proc or '-',
            'score': score,
            'strategy': strategy,
            'mode': mode,
        },
    }


def _normalize_xhs_instance_status(item: Dict[str, Any], raw: Dict[str, Any], ok: bool, latency_ms: int, err: str) -> Dict[str, Any]:
    health = raw.get('health') if isinstance(raw.get('health'), dict) else {}
    publish = raw.get('publish') if isinstance(raw.get('publish'), dict) else {}
    queue = raw.get('queue') if isinstance(raw.get('queue'), dict) else {}
    st = str(health.get('status') or '')
    level = _health_level_from_text(st, ok)
    suc = int(publish.get('success_count') or 0)
    tot = int(publish.get('today_count') or 0)
    fail = int(queue.get('failed_count_zishu') or 0)
    return {
        'id': item.get('id'),
        'name_zh': item.get('name_zh'),
        'name_en': item.get('name_en'),
        'category_zh': item.get('category_zh'),
        'category_en': item.get('category_en'),
        'ok': bool(ok),
        'status_level': level,
        'status_text_zh': '正常' if level == 'up' else ('警告' if level == 'degraded' else '异常'),
        'status_text_en': 'Up' if level == 'up' else ('Degraded' if level == 'degraded' else 'Down'),
        'summary_zh': f"今日发布:{suc}/{tot} | 失败队列:{fail}",
        'summary_en': f"Today Published:{suc}/{tot} | Failed Queue:{fail}",
        'latency_ms': latency_ms,
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'chat_agent': str(item.get('chat_agent') or ''),
        'instance_path': f"/instance/{item.get('id')}",
        'legacy_path': f"/{item.get('id')}" if item.get('id') in LEGACY_INSTANCE_IDS else '',
        'error': err or '',
        'metrics': {'health_status': st or '-', 'today_success': suc, 'today_total': tot, 'queue_failed': fail},
    }


def _normalize_main_instance_status(item: Dict[str, Any], raw: Dict[str, Any], ok: bool, latency_ms: int, err: str) -> Dict[str, Any]:
    health = raw.get('health') if isinstance(raw.get('health'), dict) else {}
    runtime = raw.get('runtime') if isinstance(raw.get('runtime'), dict) else {}
    queue = raw.get('queue') if isinstance(raw.get('queue'), dict) else {}
    st = str(health.get('status') or runtime.get('run_state') or '')
    run_state = str(runtime.get('run_state') or '-')
    failed = int(queue.get('failed_count_main') or 0)
    level = _health_level_from_text(st, ok)
    return {
        'id': item.get('id'),
        'name_zh': item.get('name_zh'),
        'name_en': item.get('name_en'),
        'category_zh': item.get('category_zh'),
        'category_en': item.get('category_en'),
        'ok': bool(ok),
        'status_level': level,
        'status_text_zh': '正常' if level == 'up' else ('警告' if level == 'degraded' else '异常'),
        'status_text_en': 'Up' if level == 'up' else ('Degraded' if level == 'degraded' else 'Down'),
        'summary_zh': f"运行状态:{run_state} | 失败队列:{failed}",
        'summary_en': f"Run State:{run_state} | Failed Queue:{failed}",
        'latency_ms': latency_ms,
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'chat_agent': str(item.get('chat_agent') or ''),
        'instance_path': f"/instance/{item.get('id')}",
        'legacy_path': f"/{item.get('id')}" if item.get('id') in LEGACY_INSTANCE_IDS else '',
        'error': err or '',
        'metrics': {'health_status': st or '-', 'run_state': run_state, 'queue_failed': failed},
    }


def _normalize_world_instance_status(item: Dict[str, Any], raw: Dict[str, Any], ok: bool, latency_ms: int, err: str) -> Dict[str, Any]:
    code = int(raw.get('status_code') or 0)
    title = str(raw.get('title') or '-')
    reachable = bool(raw.get('reachable'))
    level = 'up' if ok and reachable and code == 200 else ('degraded' if ok else 'down')
    return {
        'id': item.get('id'),
        'name_zh': item.get('name_zh'),
        'name_en': item.get('name_en'),
        'category_zh': item.get('category_zh'),
        'category_en': item.get('category_en'),
        'ok': bool(ok),
        'status_level': level,
        'status_text_zh': '正常' if level == 'up' else ('警告' if level == 'degraded' else '异常'),
        'status_text_en': 'Up' if level == 'up' else ('Degraded' if level == 'degraded' else 'Down'),
        'summary_zh': f"可达:{'是' if reachable else '否'} | HTTP:{code} | 标题:{title}",
        'summary_en': f"Reachable:{'Yes' if reachable else 'No'} | HTTP:{code} | Title:{title}",
        'latency_ms': latency_ms,
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'chat_agent': '',
        'instance_path': f"/instance/{item.get('id')}",
        'legacy_path': f"/{item.get('id')}" if item.get('id') in LEGACY_INSTANCE_IDS else '',
        'error': err or '',
        'metrics': {'status_code': code, 'reachable': reachable, 'title': title},
    }


def _normalize_airdrop_instance_status(item: Dict[str, Any], raw: Dict[str, Any], ok: bool, latency_ms: int, err: str) -> Dict[str, Any]:
    health = raw.get('health') if isinstance(raw.get('health'), dict) else {}
    stats = raw.get('stats') if isinstance(raw.get('stats'), dict) else {}
    st = str(health.get('status') or '')
    wallets = int(stats.get('wallet_count') or 0)
    projects = int(stats.get('project_count') or 0)
    pending = int(stats.get('pending_tasks') or 0)
    level = _health_level_from_text(st, ok)
    return {
        'id': item.get('id'),
        'name_zh': item.get('name_zh'),
        'name_en': item.get('name_en'),
        'category_zh': item.get('category_zh'),
        'category_en': item.get('category_en'),
        'ok': bool(ok),
        'status_level': level,
        'status_text_zh': '正常' if level == 'up' else ('警告' if level == 'degraded' else '异常'),
        'status_text_en': 'Up' if level == 'up' else ('Degraded' if level == 'degraded' else 'Down'),
        'summary_zh': f"钱包:{wallets} | 项目:{projects} | 待处理任务:{pending}",
        'summary_en': f"Wallets:{wallets} | Projects:{projects} | Pending Tasks:{pending}",
        'latency_ms': latency_ms,
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'chat_agent': '',
        'instance_path': f"/instance/{item.get('id')}",
        'legacy_path': f"/{item.get('id')}" if item.get('id') in LEGACY_INSTANCE_IDS else '',
        'error': err or '',
        'metrics': {'health_status': st or '-', 'wallet_count': wallets, 'project_count': projects, 'pending_tasks': pending},
    }


def _normalize_generic_instance_status(item: Dict[str, Any], raw: Dict[str, Any], ok: bool, latency_ms: int, err: str) -> Dict[str, Any]:
    status_hint = ''
    if isinstance(raw, dict):
        status_hint = str(raw.get('status') or raw.get('state') or raw.get('health') or '')
    level = _health_level_from_text(status_hint, ok)
    return {
        'id': item.get('id'),
        'name_zh': item.get('name_zh'),
        'name_en': item.get('name_en'),
        'category_zh': item.get('category_zh'),
        'category_en': item.get('category_en'),
        'ok': bool(ok),
        'status_level': level,
        'status_text_zh': '正常' if level == 'up' else ('警告' if level == 'degraded' else '异常'),
        'status_text_en': 'Up' if level == 'up' else ('Degraded' if level == 'degraded' else 'Down'),
        'summary_zh': f"实例状态:{status_hint or ('可用' if ok else '不可用')}",
        'summary_en': f"Instance Status:{status_hint or ('Available' if ok else 'Unavailable')}",
        'latency_ms': latency_ms,
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'chat_agent': str(item.get('chat_agent') or ''),
        'instance_path': f"/instance/{item.get('id')}",
        'legacy_path': f"/{item.get('id')}" if item.get('id') in LEGACY_INSTANCE_IDS else '',
        'error': err or '',
        'metrics': {'status_hint': status_hint},
    }


def fetch_instance_status(instance_id: str) -> Dict[str, Any]:
    item = get_instance_meta(instance_id)
    if not item:
        return {'ok': False, 'error': 'instance_not_found', 'id': str(instance_id or '')}
    iid = str(item.get('id') or '')
    st_mode = str(item.get('status_mode') or 'proxy_json').lower()
    t0 = time.time()
    raw: Dict[str, Any] = {}
    ok = False
    err = ''

    try:
        if st_mode == 'local_okx' or iid == 'okx':
            raw = build_status()
            ok = True
        elif st_mode == 'local_airdrop' or iid == 'airdrop':
            raw = fetch_airdrop_status()
            ok = bool(raw.get('ok'))
            err = str(raw.get('error') or '')
        elif st_mode == 'proxy_world' or iid == 'world':
            raw = fetch_world_status()
            ok = bool(raw.get('ok'))
            err = str(raw.get('error') or '')
        elif iid == 'xhs':
            raw = fetch_xhs_status()
            ok = bool(raw.get('ok'))
            err = str(raw.get('error') or '')
        elif iid == 'main':
            raw = fetch_main_status()
            ok = bool(raw.get('ok'))
            err = str(raw.get('error') or '')
        else:
            raw = fetch_proxy_json(str(item.get('base_url') or ''), str(item.get('status_path') or '/'), int(item.get('status_timeout') or INSTANCE_STATUS_TIMEOUT_DEFAULT))
            ok = bool(raw.get('ok'))
            err = str(raw.get('error') or '')
    except Exception as e:
        raw = {'ok': False, 'error': str(e)}
        ok = False
        err = str(e)

    latency_ms = int((time.time() - t0) * 1000)
    if iid == 'okx':
        return _normalize_okx_instance_status(item, raw, ok, latency_ms, err)
    if iid == 'xhs':
        return _normalize_xhs_instance_status(item, raw, ok, latency_ms, err)
    if iid == 'main':
        return _normalize_main_instance_status(item, raw, ok, latency_ms, err)
    if iid == 'world':
        return _normalize_world_instance_status(item, raw, ok, latency_ms, err)
    if iid == 'airdrop':
        return _normalize_airdrop_instance_status(item, raw, ok, latency_ms, err)
    return _normalize_generic_instance_status(item, raw, ok, latency_ms, err)


def fetch_instances_summary() -> Dict[str, Any]:
    items = get_instance_registry()
    if not items:
        return {'ok': True, 'instances': [], 'counts': {'total': 0, 'up': 0, 'degraded': 0, 'down': 0}, 'updated_at': datetime.now(timezone.utc).isoformat()}
    rows: List[Dict[str, Any]] = []
    max_workers = max(1, min(len(items), 8))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(fetch_instance_status, str(x.get('id') or '')): str(x.get('id') or '') for x in items}
        for fut in as_completed(fut_map):
            iid = fut_map.get(fut, '')
            try:
                row = fut.result()
            except Exception as e:
                meta = get_instance_meta(iid) or {'id': iid, 'name_zh': iid, 'name_en': iid}
                row = _normalize_generic_instance_status(meta, {}, False, 0, str(e))
            rows.append(row)
    rank = {str(x.get('id') or ''): idx for idx, x in enumerate(items)}
    rows.sort(key=lambda x: rank.get(str(x.get('id') or ''), 9999))
    counts = {'total': len(rows), 'up': 0, 'degraded': 0, 'down': 0}
    for x in rows:
        lvl = str(x.get('status_level') or 'down')
        if lvl not in counts:
            lvl = 'down'
        counts[lvl] += 1
    return {'ok': True, 'instances': rows, 'counts': counts, 'updated_at': datetime.now(timezone.utc).isoformat()}


def proxy_world(path: str, query: str, raw_prefix: str = '/world/raw/') -> Dict[str, Any]:
    p = str(path or '/').strip()
    if not p.startswith('/'):
        p = '/' + p
    target = f'{WORLD_PROXY_BASE}{p}'
    if query:
        target = target + '?' + query
    try:
        r = requests.get(target, timeout=20)
        ctype = r.headers.get('Content-Type', 'application/octet-stream')
        body = r.content
        if p in {'', '/'} and ('text/html' in ctype):
            text = r.text
            pref = str(raw_prefix or '/world/raw/')
            text = re.sub(r'(src|href)=([\'"])\/', r'\1=\2' + pref, text)
            body = text.encode('utf-8', errors='replace')
            ctype = 'text/html; charset=utf-8'
        return {'ok': True, 'status': r.status_code, 'content': body, 'ctype': ctype}
    except Exception as e:
        return {'ok': False, 'status': 502, 'content': f'world raw proxy failed: {e}'.encode('utf-8'), 'ctype': 'text/plain; charset=utf-8'}


def proxy_world_api(method: str, path: str, query: str, body: bytes = b'', req_headers: Any = None) -> Dict[str, Any]:
    p = str(path or '').strip()
    if not p.startswith('/'):
        p = '/' + p
    target = f'{WORLD_PROXY_BASE}{p}'
    if query:
        target = target + '?' + query
    headers: Dict[str, str] = {}
    if req_headers is not None:
        for k in ['Content-Type', 'Accept', 'Authorization', 'X-WorldMonitor-Key', 'User-Agent']:
            v = req_headers.get(k)
            if v:
                headers[k] = str(v)
    try:
        r = requests.request(method.upper(), target, data=body or None, headers=headers, timeout=30)
        return {
            'ok': True,
            'status': r.status_code,
            'content': r.content,
            'ctype': r.headers.get('Content-Type', 'application/octet-stream'),
        }
    except Exception as e:
        return {
            'ok': False,
            'status': 502,
            'content': f'world api proxy failed: {e}'.encode('utf-8'),
            'ctype': 'text/plain; charset=utf-8',
        }


def _proxy_passthrough_headers(req_headers: Any) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if req_headers is None:
        return headers
    for key in ['Content-Type', 'Accept', 'Authorization', 'User-Agent', 'X-Requested-With']:
        val = req_headers.get(key)
        if val:
            headers[key] = str(val)
    return headers


def _rewrite_instance_raw_html(instance_id: str, html_text: str) -> str:
    iid = str(instance_id or '').strip().lower()
    prefix = f'/instance/{iid}/raw/'
    text = str(html_text or '')
    # Keep frontend self-contained under /instance/<id>/raw.
    text = re.sub(r'(src|href|action)=([\'"])/(?!/)', r'\1=\2' + prefix, text)
    text = re.sub(r'([\'"])/api/', r'\1' + prefix + 'api/', text)
    inject = '<style>.wrap{max-width:none!important;width:100%!important;margin:0!important;padding:14px 16px!important} body{background:transparent!important}</style>'
    if '</head>' in text:
        text = text.replace('</head>', inject + '</head>', 1)
    return text


def proxy_instance_raw_get(instance_id: str, rel_path: str, query: str, req_headers: Any = None) -> Dict[str, Any]:
    item = get_instance_meta(instance_id)
    if not item:
        return {'ok': False, 'status': 404, 'content': b'instance_not_found', 'ctype': 'text/plain; charset=utf-8'}
    iid = str(item.get('id') or '')
    raw_mode = str(item.get('raw_mode') or 'proxy').lower()
    path = str(rel_path or '/').strip()
    if not path.startswith('/'):
        path = '/' + path

    if raw_mode == 'local_okx':
        if path not in {'/', ''}:
            return {'ok': False, 'status': 404, 'content': b'not_found', 'ctype': 'text/plain; charset=utf-8'}
        text = HTML
        inject = '<style>.wrap{max-width:none!important;width:100%!important;margin:0!important;padding:14px 16px!important} .wrap > .row:first-child{display:none!important} body{background:transparent!important}</style>'
        if '</head>' in text:
            text = text.replace('</head>', inject + '</head>', 1)
        return {'ok': True, 'status': 200, 'content': text.encode('utf-8', errors='replace'), 'ctype': 'text/html; charset=utf-8'}

    if iid == 'world':
        return proxy_world(path, query, f'/instance/{iid}/raw/')

    base_url = str(item.get('base_url') or '').rstrip('/')
    if not base_url:
        return {'ok': False, 'status': 502, 'content': b'base_url_missing', 'ctype': 'text/plain; charset=utf-8'}
    target = base_url + path
    if query:
        target = target + '?' + query

    try:
        r = requests.get(target, headers=_proxy_passthrough_headers(req_headers), timeout=max(5, min(int(item.get('status_timeout') or INSTANCE_HTTP_TIMEOUT_DEFAULT), 60)))
        ctype = r.headers.get('Content-Type', 'application/octet-stream')
        body = r.content
        if path in {'', '/'} and ('text/html' in ctype):
            text = _rewrite_instance_raw_html(iid, r.text)
            body = text.encode('utf-8', errors='replace')
            ctype = 'text/html; charset=utf-8'
        return {'ok': True, 'status': r.status_code, 'content': body, 'ctype': ctype}
    except Exception as e:
        return {'ok': False, 'status': 502, 'content': f'instance raw proxy failed: {e}'.encode('utf-8'), 'ctype': 'text/plain; charset=utf-8'}


def proxy_instance_api(instance_id: str, method: str, rel_path: str, query: str, body: bytes = b'', req_headers: Any = None) -> Dict[str, Any]:
    item = get_instance_meta(instance_id)
    if not item:
        return {'ok': False, 'status': 404, 'content': b'instance_not_found', 'ctype': 'text/plain; charset=utf-8'}
    iid = str(item.get('id') or '')
    path = str(rel_path or '/').strip()
    if not path.startswith('/'):
        path = '/' + path
    if iid == 'world':
        return proxy_world_api(method, path, query, body, req_headers)
    base_url = str(item.get('base_url') or '').rstrip('/')
    if not base_url:
        return {'ok': False, 'status': 502, 'content': b'base_url_missing', 'ctype': 'text/plain; charset=utf-8'}
    target = base_url + path
    if query:
        target = target + '?' + query
    try:
        r = requests.request(method.upper(), target, data=body or None, headers=_proxy_passthrough_headers(req_headers), timeout=max(5, min(int(item.get('status_timeout') or INSTANCE_HTTP_TIMEOUT_DEFAULT), 60)))
        return {'ok': True, 'status': r.status_code, 'content': r.content, 'ctype': r.headers.get('Content-Type', 'application/octet-stream')}
    except Exception as e:
        return {'ok': False, 'status': 502, 'content': f'instance api proxy failed: {e}'.encode('utf-8'), 'ctype': 'text/plain; charset=utf-8'}


def _parse_json_from_text(raw_text: str) -> Dict[str, Any]:
    text = str(raw_text or '').strip()
    if not text:
        return {}
    if text.startswith('{'):
        try:
            obj = json.loads(text)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    pos = text.find('{')
    if pos >= 0:
        tail = text[pos:]
        try:
            obj = json.loads(tail)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _extract_site_ai_options(raw: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    api_key = str(raw.get('api_key') or '').strip()
    if api_key:
        out['api_key'] = api_key[:400]
    api_base = str(raw.get('api_base') or raw.get('api_url') or '').strip()
    if api_base:
        if api_base.startswith('http://') or api_base.startswith('https://'):
            out['api_base'] = api_base[:260].rstrip('/')
    model = str(raw.get('model') or '').strip()
    if model:
        model = re.sub(r'[^A-Za-z0-9._:-]', '', model)
        if model:
            out['model'] = model[:80]
    return out


def run_site_ai_analysis(force: bool = False, options: Optional[Dict[str, str]] = None, test_model: bool = False) -> Dict[str, Any]:
    opts = dict(options or {})
    using_request_options = bool(opts)
    with SITE_AI_LOCK:
        now = now_ms()
        cached = read_json(SITE_AI_REPORT_FILE, {})
        if not isinstance(cached, dict):
            cached = {}
        cached_ts = int(cached.get('updated_at_ms') or 0)
        if (not force) and (not test_model) and (not using_request_options) and cached_ts > 0 and (now - cached_ts) <= SITE_AI_CACHE_TTL_MS:
            out = dict(cached)
            out['from_cache'] = True
            return out

        if not os.path.exists(SITE_AI_MONITOR_SCRIPT):
            if cached and (not using_request_options) and (not test_model):
                out = dict(cached)
                out['from_cache'] = True
                out['warning'] = 'site_ai_monitor_script_missing'
                return out
            return {
                'ok': False,
                'error': 'site_ai_monitor_script_missing',
                'path': SITE_AI_MONITOR_SCRIPT,
                'updated_at_ms': now,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }

        cmd = [
            'python3',
            SITE_AI_MONITOR_SCRIPT,
            '--base-url',
            f'http://{HOST}:{PORT}',
            '--timeout',
            '12',
        ]
        if test_model:
            cmd.append('--test-model')
        env = os.environ.copy()
        if opts.get('api_key'):
            env['SITE_AI_API_KEY'] = str(opts.get('api_key') or '')
        if opts.get('api_base'):
            env['SITE_AI_API_BASE'] = str(opts.get('api_base') or '')
        if opts.get('model'):
            env['SITE_AI_MODEL'] = str(opts.get('model') or '')
        try:
            cp = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=140,
                env=env,
            )
        except Exception as e:
            if cached and (not using_request_options) and (not test_model):
                out = dict(cached)
                out['from_cache'] = True
                out['warning'] = f'run_site_ai_monitor_exception: {e}'
                return out
            return {
                'ok': False,
                'error': f'run_site_ai_monitor_exception: {e}',
                'updated_at_ms': now,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }

        if cp.returncode != 0:
            detail = (cp.stderr or cp.stdout or 'site_ai_monitor_failed').strip()
            if cached and (not using_request_options) and (not test_model):
                out = dict(cached)
                out['from_cache'] = True
                out['warning'] = _clip_text(detail, 320)
                return out
            return {
                'ok': False,
                'error': 'site_ai_monitor_failed',
                'detail': _clip_text(detail, 320),
                'updated_at_ms': now,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }

        payload = _parse_json_from_text(cp.stdout or '')
        if not payload:
            detail = (cp.stdout or cp.stderr or '').strip()
            if cached and (not using_request_options) and (not test_model):
                out = dict(cached)
                out['from_cache'] = True
                out['warning'] = 'site_ai_output_invalid_json'
                if detail:
                    out['warning_detail'] = _clip_text(detail, 320)
                return out
            return {
                'ok': False,
                'error': 'site_ai_output_invalid_json',
                'detail': _clip_text(detail, 320),
                'updated_at_ms': now,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }

        payload['updated_at_ms'] = int(payload.get('updated_at_ms') or now_ms())
        payload['updated_at'] = str(payload.get('updated_at') or datetime.now(timezone.utc).isoformat())
        payload['from_cache'] = False
        payload['runtime_source'] = 'request' if using_request_options else 'server_env'
        if opts.get('model'):
            payload['runtime_model'] = str(opts.get('model') or '')
        if opts.get('api_base'):
            payload['runtime_api_base'] = str(opts.get('api_base') or '')
        if payload.get('ok') and (not using_request_options) and (not test_model):
            write_json(SITE_AI_REPORT_FILE, payload)
        return payload


_INSTANCE_ROUTE_RE = re.compile(r'^/instance/([A-Za-z0-9_-]+)(?:/(.*))?$')
_LEGACY_RAW_ROUTE_RE = re.compile(r'^/(okx|xhs|main|world|airdrop)/raw(?:/(.*))?$')


def _normalize_rel_path(tail: str) -> str:
    raw = str(tail or '')
    if not raw:
        return '/'
    if raw.startswith('/'):
        return raw or '/'
    return '/' + raw


def _parse_instance_route(path: str) -> Optional[Dict[str, str]]:
    m = _INSTANCE_ROUTE_RE.match(str(path or ''))
    if not m:
        return None
    iid = str(m.group(1) or '').strip().lower()
    tail = str(m.group(2) or '').strip()
    tail = tail.strip('/')
    return {'id': iid, 'tail': tail}


def _parse_legacy_raw_route(path: str) -> Optional[Dict[str, str]]:
    m = _LEGACY_RAW_ROUTE_RE.match(str(path or ''))
    if not m:
        return None
    iid = str(m.group(1) or '').strip().lower()
    tail = str(m.group(2) or '').strip()
    return {'id': iid, 'rel_path': _normalize_rel_path(tail)}

def agent_sessions_json(agent: str) -> str:
    return f'{TRADER_OPENCLAW_ROOT}/agents/{agent}/sessions/sessions.json'


def agent_sessions_dir(agent: str) -> str:
    return f'{TRADER_OPENCLAW_ROOT}/agents/{agent}/sessions'


def get_agent_session_file(agent: str) -> str:
    idx = read_json(agent_sessions_json(agent), {})
    item = (idx or {}).get(f'agent:{agent}:main') or {}
    sf = str(item.get('sessionFile', '')).strip()
    if sf and os.path.isfile(sf):
        return sf
    sid = str(item.get('sessionId', '')).strip()
    if sid:
        p = os.path.join(agent_sessions_dir(agent), f'{sid}.jsonl')
        if os.path.isfile(p):
            return p
    files = list_files(agent_sessions_dir(agent), '.jsonl')
    return files[0] if files else ''


def _clip_text(text: str, max_len: int = 320) -> str:
    s = str(text or '').strip().replace('\r', ' ').replace('\n', ' ')
    if len(s) <= max_len:
        return s
    return s[:max_len] + '...'


def _extract_stream_chunks_from_content(content: Any) -> List[Dict[str, str]]:
    chunks: List[Dict[str, str]] = []
    if not isinstance(content, list):
        return chunks
    for c in content:
        if not isinstance(c, dict):
            continue
        typ = str(c.get('type', '')).strip()
        if typ in {'text', 'input_text', 'output_text'}:
            txt = _clip_text(str(c.get('text', '')).strip(), 900)
            if txt:
                chunks.append({'kind': 'text', 'text': txt})
        elif typ == 'thinking':
            txt = _clip_text(str(c.get('thinking') or c.get('text') or '').strip(), 900)
            if txt:
                chunks.append({'kind': 'thinking', 'text': txt})
        elif typ == 'toolCall':
            nm = str(c.get('name', 'tool')).strip() or 'tool'
            args = c.get('arguments')
            if isinstance(args, (dict, list)):
                arg_text = _clip_text(json.dumps(args, ensure_ascii=False), 280)
            else:
                arg_text = _clip_text(str(args or ''), 280)
            text = nm if not arg_text else f'{nm} {arg_text}'
            chunks.append({'kind': 'tool_call', 'text': text})
        elif typ == 'toolResult':
            txt = _clip_text(str(c.get('text') or '').strip(), 320)
            chunks.append({'kind': 'tool_result', 'text': txt or '[toolResult]'})
    return chunks


def read_agent_session_events_since(session_file: str, offset: int) -> tuple[int, List[Dict[str, str]]]:
    if not session_file or not os.path.isfile(session_file):
        return offset, []
    events: List[Dict[str, str]] = []
    try:
        with open(session_file, 'r', encoding='utf-8', errors='replace') as f:
            f.seek(max(offset, 0))
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                typ = str(obj.get('type', ''))
                ts = str(obj.get('timestamp', ''))
                role = ''
                chunks: List[Dict[str, str]] = []
                if typ == 'message' and isinstance(obj.get('message'), dict):
                    msg = obj.get('message') or {}
                    role = str(msg.get('role', ''))
                    chunks = _extract_stream_chunks_from_content(msg.get('content'))
                    err = str(msg.get('errorMessage', '')).strip()
                    if err:
                        chunks.append({'kind': 'error', 'text': _clip_text(err, 320)})
                elif typ == 'item' and isinstance(obj.get('message'), dict):
                    msg = obj.get('message') or {}
                    role = str(msg.get('role', ''))
                    chunks = _extract_stream_chunks_from_content(msg.get('content'))
                elif typ == 'response_item' and isinstance(obj.get('item'), dict):
                    msg = obj.get('item') or {}
                    role = str(msg.get('role', 'assistant'))
                    chunks = _extract_stream_chunks_from_content(msg.get('content'))
                if not chunks:
                    continue
                for ch in chunks:
                    events.append(
                        {
                            'timestamp': ts,
                            'role': role or 'assistant',
                            'kind': ch.get('kind', 'text'),
                            'text': ch.get('text', ''),
                        }
                    )
            new_offset = f.tell()
        return new_offset, events
    except Exception:
        return offset, []


def parse_agent_session_errors(agent: str, limit: int = 12) -> List[Dict[str, str]]:
    session_file = get_agent_session_file(agent)
    if not session_file or not os.path.isfile(session_file):
        return []
    err_kw = ['error', 'failed', 'timeout', 'timed out', 'exception', 'traceback', 'invalid data', '10012', '400', '500', '429', '230099', '11310', '失败', '错误']
    rows: List[Dict[str, str]] = []
    for raw in tail_file(session_file, 1800):
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        typ = str(obj.get('type', ''))
        ts = str(obj.get('timestamp', ''))
        role = ''
        txt = ''
        if typ == 'message' and isinstance(obj.get('message'), dict):
            msg = obj.get('message') or {}
            role = str(msg.get('role', ''))
            txt = _extract_text_from_content(msg.get('content')) or str(msg.get('errorMessage', '')).strip()
        elif typ == 'item' and isinstance(obj.get('message'), dict):
            msg = obj.get('message') or {}
            role = str(msg.get('role', ''))
            txt = _extract_text_from_content(msg.get('content'))
        elif typ == 'response_item' and isinstance(obj.get('item'), dict):
            msg = obj.get('item') or {}
            role = str(msg.get('role', 'assistant'))
            txt = _extract_text_from_content(msg.get('content'))
        txt = _clip_text(txt, 320)
        if not txt:
            continue
        low = txt.lower()
        if any(k in low for k in err_kw):
            rows.append({'timestamp': ts, 'role': role, 'summary': txt})
    return rows[-limit:]


def parse_gateway_errors(limit: int = 80) -> Dict[str, List[Dict[str, str]]]:
    out: Dict[str, List[Dict[str, str]]] = {'main': [], 'trader': [], 'zishu': []}
    files = list_files(OPENCLAW_LOG_DIR, '.log')
    if not files:
        return out
    log_file = files[0]
    lines = tail_file(log_file, 6000)
    for raw in lines:
        raw_low = raw.lower()
        if not any(k in raw_low for k in ['final reply failed', 'streaming start failed', 'invalid data', 'request failed with status code 400', '230099', '11310']):
            continue
        agent = ''
        if 'feishu[default]' in raw:
            agent = 'main'
        elif 'feishu[trader]' in raw:
            agent = 'trader'
        elif 'feishu[zishu]' in raw:
            agent = 'zishu'
        if not agent:
            continue
        ts = '-'
        msg = ''
        mt = re.search(r'"time":"([^"]+)"', raw)
        if mt:
            ts = mt.group(1)
        mm = re.search(r'"1":"((?:\\\\"|[^"])*)"', raw)
        if mm:
            try:
                msg = bytes(mm.group(1), 'utf-8').decode('unicode_escape', 'ignore')
            except Exception:
                msg = mm.group(1)
        if not msg:
            msg = _clip_text(raw, 280)
        out[agent].append({'timestamp': ts, 'summary': _clip_text(msg, 320)})
    for key in list(out.keys()):
        out[key] = out[key][-limit:]
    return out


def build_agent_error_report() -> Dict[str, Any]:
    gateway = parse_gateway_errors(30)
    agents: Dict[str, Any] = {}
    for agent in sorted(CHAT_ALLOWED_AGENTS):
        sess = parse_agent_session_errors(agent, 20)
        gw = gateway.get(agent, [])
        agents[agent] = {
            'session_errors': sess,
            'gateway_errors': gw,
            'session_error_count': len(sess),
            'gateway_error_count': len(gw),
            'latest_session_error': sess[-1] if sess else {},
            'latest_gateway_error': gw[-1] if gw else {},
        }
    return {'timestamp': now_ms(), 'agents': agents}


def recommendation(direction: str) -> str:
    if direction == 'bullish':
        return '\u5efa\u8bae\u5f00\u591a'
    if direction == 'bearish':
        return '\u5efa\u8bae\u5f00\u7a7a'
    return '\u5efa\u8bae\u89c2\u671b'


def describe_factor_detail(key: str, value: Any) -> str:
    labels = {
        'ret_5': '\u8fd15\u4e2a\u91c7\u6837\u5468\u671f\u6536\u76ca\u7387(%)',
        'ret_15': '\u8fd115\u4e2a\u91c7\u6837\u5468\u671f\u6536\u76ca\u7387(%)',
        'combo': '\u52a8\u91cf\u7ec4\u5408\u503c(\u77ed\u4e2d\u5468\u671f\u52a0\u6743)',
        'realized_vol_pct': '\u5b9e\u73b0\u6ce2\u52a8\u7387(%)',
        'hl_range_pct': '24H\u632f\u5e45(%)',
        'funding_pct': '\u8d44\u91d1\u8d39\u7387(%)',
        'zscore': '\u6807\u51c6\u5206(z-score,\u79bb\u5747\u503c\u7a0b\u5ea6)',
        'oi_change_6': '\u8fd16\u4e2a\u5468\u671fOI\u53d8\u5316(%)',
        'price_change_6': '\u8fd16\u4e2a\u5468\u671f\u4ef7\u683c\u53d8\u5316(%)',
        'ratio': '\u591a\u7a7a\u8d26\u6237\u6bd4',
        'value': '\u6050\u60e7\u8d2a\u5a6a\u6307\u6570(\u5f53\u524d\u503c)',
        'delta': '\u6050\u60e7\u8d2a\u5a6a\u6307\u6570\u53d8\u5316\u91cf',
        'basis_bps': '\u6c38\u7eed-\u73b0\u8d27\u57fa\u5dee(bps)',
        'btc_dom': 'BTC\u5e02\u503c\u5360\u6bd4(%)',
        'mcap_change_24h': '\u5168\u5e02\u573a24H\u5e02\u503c\u53d8\u5316(%)',
        'patterns': 'K\u7ebf\u5f62\u6001\u8bc6\u522b\u7ed3\u679c',
        'rsi14': 'RSI(14)',
        'ema12': 'EMA12',
        'ema26': 'EMA26',
        'atr20': 'ATR20',
        'sim_return_pct': '\u5386\u53f2\u6a21\u62df\u6536\u76ca\u7387(%)',
        'sim_max_drawdown_pct': '\u5386\u53f2\u6a21\u62df\u6700\u5927\u56de\u64a4(%)',
        'sim_win_rate': '\u5386\u53f2\u6a21\u62df\u80dc\u7387',
        'support': '\u652f\u6491\u4f4d',
        'resistance': '\u963b\u529b\u4f4d',
        'asr_pos': '\u4ef7\u683c\u5728\u652f\u6491-\u963b\u529b\u533a\u95f4\u4f4d\u7f6e',
        'volume_z': '\u6210\u4ea4\u91cfz-score(\u5f02\u5e38\u7a0b\u5ea6)',
        'buy_vol_usd': '\u4e3b\u52a8\u4e70\u5165\u6210\u4ea4\u989d(USD)',
        'sell_vol_usd': '\u4e3b\u52a8\u5356\u51fa\u6210\u4ea4\u989d(USD)',
        'buy_sell_ratio': '\u4e3b\u52a8\u4e70/\u5356\u6bd4',
        'breakout_type': '\u7a81\u7834\u7c7b\u578b',
        'trend': '\u8d8b\u52bf\u65b9\u5411',
        'rsi': 'RSI',
        'bb_position_pct': '\u4ef7\u683c\u5728\u5e03\u6797\u5e26\u4f4d\u7f6e(%)',
        'signal': '\u5f53\u524d\u4fe1\u53f7',
        'quality': '\u6d41\u52a8\u6027\u8d28\u91cf\u7cfb\u6570(0-1)',
        'gated_score': '\u95e8\u63a7\u540e\u5f97\u5206',
        'gate_reason': '\u95e8\u63a7\u539f\u56e0',
        'correlation': '\u8de8\u5e02\u573a\u76f8\u5173\u7cfb\u6570',
        'spy_5d_change': 'SPY 5\u65e5\u6da8\u8dcc(%)',
        'correlation_strength': '\u76f8\u5173\u5f3a\u5ea6',
        'vix': 'VIX\u6307\u6570',
        'dxy_5d_change': 'DXY 5\u65e5\u53d8\u5316(%)',
        'vix_risk_level': 'VIX\u98ce\u9669\u5206\u7ea7',
        'macd_bullish': 'MACD\u662f\u5426\u91d1\u53c9',
        'bb_position': '\u5e03\u6797\u5e26\u4f4d\u7f6e',
        'atr_pct': 'ATR\u5360\u4ef7\u683c\u6bd4\u4f8b(%)',
        'atr': 'ATR\u503c',
        'avg_credibility': '\u7efc\u5408\u4fe1\u53f7\u53ef\u4fe1\u5ea6',
        'bars': '\u53c2\u4e0e\u8ba1\u7b97K\u7ebf\u6570\u91cf',
        'bb_lower': '\u5e03\u6797\u4e0b\u8f68',
        'bb_upper': '\u5e03\u6797\u4e0a\u8f68',
        'bid_volume': '\u4e70\u4e00\u4fa7\u6302\u5355\u91cf',
        'ask_volume': '\u5356\u4e00\u4fa7\u6302\u5355\u91cf',
        'breakout_dist': '\u8ddd\u79bb\u7a81\u7834\u8fb9\u754c\u7684\u5dee\u503c',
        'cascade_risk': '\u94fe\u5f0f\u7206\u4ed3\u98ce\u9669\u7cfb\u6570',
        'current_price': '\u5f53\u524d\u6700\u65b0\u4ef7',
        'dist_to_resistance_atr': '\u8ddd\u963b\u529b\u4f4d(ATR\u500d\u6570)',
        'dist_to_support_atr': '\u8ddd\u652f\u6491\u4f4d(ATR\u500d\u6570)',
        'effective_imbalance_pct': '\u6709\u6548\u4e70\u5356\u76d8\u4e0d\u5e73\u8861(%)',
        'gated_conf': '\u95e8\u63a7\u540e\u7f6e\u4fe1\u5ea6',
        'hard_hold_candidate': '\u662f\u5426\u89e6\u53d1\u5f3a\u5236\u89c2\u671b\u5019\u9009',
        'imbalance_pct': '\u4e70\u5356\u76d8\u4e0d\u5e73\u8861(%)',
        'impact_asymmetry_pct': '\u51b2\u51fb\u6210\u672c\u4e0d\u5bf9\u79f0(%)',
        'large_asks': '\u5927\u989d\u5356\u5355\u6570\u91cf',
        'large_bids': '\u5927\u989d\u4e70\u5355\u6570\u91cf',
        'large_imbalance_pct': '\u5927\u5355\u5931\u8861\u6bd4(%)',
        'long_liq_price': '\u591a\u5934\u9884\u4f30\u7206\u4ed3\u4ef7',
        'ma20': 'MA20',
        'ma_deviation_pct': '\u4ef7\u683c\u76f8\u5bf9MA\u504f\u79bb(%)',
        'macd': 'MACD\u503c',
        'macd_signal': 'MACD Signal',
        'market_regime': '\u5e02\u573a\u98ce\u683c\u6807\u7b7e',
        'periods': '\u7edf\u8ba1\u5468\u671f\u6570',
        'pressure_imbalance_pct': '\u4e70\u5356\u538b\u529b\u4e0d\u5e73\u8861(%)',
        'quality_metrics': '\u6d41\u52a8\u6027\u8d28\u91cf\u5b50\u9879',
        'raw_conf': '\u539f\u59cb\u7f6e\u4fe1\u5ea6',
        'raw_score': '\u539f\u59cb\u5f97\u5206(\u95e8\u63a7\u524d)',
        'short_liq_price': '\u7a7a\u5934\u9884\u4f30\u7206\u4ed3\u4ef7',
        'size_imbalance_pct': '\u6309\u59d4\u6258\u91cf\u7684\u4e0d\u5e73\u8861(%)',
        'slope_imbalance_pct': '\u4ef7\u9636\u659c\u7387\u4e0d\u5e73\u8861(%)',
        'spoofing_detected': '\u662f\u5426\u68c0\u6d4b\u5230\u8bf1\u9a97\u6302\u5355',
        'spoofing_penalty': '\u8bf1\u9a97\u60e9\u7f5a\u5206',
        'spread_usd': '\u70b9\u5dee(USD)',
        'suspicious_asks': '\u53ef\u7591\u5356\u5355\u6570\u91cf',
        'suspicious_bids': '\u53ef\u7591\u4e70\u5355\u6570\u91cf',
        'top5_ratio': '\u4e94\u6863\u6df1\u5ea6\u6bd4',
        'trend_filter': '\u8d8b\u52bf\u8fc7\u6ee4\u7ed3\u679c',
        'volume_status': '\u6210\u4ea4\u91cf\u72b6\u6001',
    }
    label = labels.get(key, key)
    return f'{label}: {value}'


def _detail_val(details: Dict[str, Any], *keys: str) -> str:
    for k in keys:
        if k in details:
            v = details.get(k)
            if v is None:
                continue
            s = str(v)
            if s.strip():
                return s
    return '-'


def explain(name: str, details: Dict[str, Any] | None = None) -> Dict[str, Any]:
    d = details if isinstance(details, dict) else {}
    common_fallback = {
        'logic': '\u8be5\u56e0\u5b50\u5df2\u53c2\u4e0e\u7efc\u5408\u8bc4\u5206\u4e0e\u65b9\u5411\u51b3\u7b56\uff0c\u53ef\u5728\u811a\u672c\u4e2d\u67e5\u770b\u5bf9\u5e94\u5b9e\u73b0\u3002',
        'guide': [
            '\u8c03\u7528: FactorEngineV31.evaluate -> \u5bf9\u5e94\u56e0\u5b50\u51fd\u6570',
            '\u6570\u636e: \u5f53\u524d\u5e02\u573a\u5feb\u7167 snap + \u5386\u53f2 history',
            '\u5f53\u524d\u4f9d\u636e: ' + ', '.join([f'{k}={v}' for k, v in list(d.items())[:3]]) if d else '\u5f53\u524d\u4f9d\u636e: -',
            '\u591a\u7a7a\u5224\u5b9a: score>50 \u504f\u591a, score<50 \u504f\u7a7a',
            '\u65b9\u5411\u95e8\u69db: \u4e00\u822c\u7531 _direction(up/down \u9608\u503c) \u51b3\u5b9a',
            '\u6587\u4ef6: /root/.okx-paper/ai_trader_v3_1.py',
        ],
    }

    if name == 'benchmark_regime':
        return {
            'logic': 'BTC基准环境不是拿来直接开仓的，而是先判断现在是否值得承担方向性风险。只有大环境允许，系统才会开放做多或做空权限。',
            'guide': [
                '调用: FactorEngineV31.evaluate 结构化叠加层',
                '数据: h1 / d1 K线、价格相对均线位置、近5日日线收益',
                '核心: 价格是否站上 h1 趋势线、d1 慢均线是否向上、日线斜率是否支持当前方向',
                '判定: score>=58 视为多头环境, score<=42 视为空头环境, 中间区域优先等待',
                f"当前依据: price={_detail_val(d, 'price')}, ema20_h1={_detail_val(d, 'ema20_h1')}, ema120_d1={_detail_val(d, 'ema120_d1')}, d1_ret_5_pct={_detail_val(d, 'd1_ret_5_pct')}",
            ],
        }
    if name == 'trend_stack':
        return {
            'logic': '趋势结构簇把动量、突破、技术K线、K线形态和增强技术面合并。目的不是让每个指标都投票，而是只在它们大致同向时承认“趋势成立”。',
            'guide': [
                '调用: FactorEngineV31.evaluate 结构化叠加层',
                '数据: momentum / breakout / technical_kline / kline_pattern / enhanced_technical / asr_vc',
                '用途: 判断趋势是否真实存在，而不是短期噪声或单点拉盘',
                '判定: score>54 说明趋势簇支持做多, score<46 说明趋势簇支持做空',
                f"当前依据: components={_detail_val(d, 'components')}",
            ],
        }
    if name == 'flow_impulse':
        return {
            'logic': '订单流推进簇关注的是“有没有真实资金在推”。只有订单流持续、主动成交、OI变化和清算压力同向，系统才把这个方向视为可交易。',
            'guide': [
                '调用: FactorEngineV31.evaluate 结构化叠加层',
                '数据: order_flow_persistence / liquidity_depth / liquidation_pressure / oi_delta / taker_volume',
                '用途: 避免只因价格涨跌就追单，而忽略背后并没有新增资金推动',
                '判定: score>54 代表买盘推进更真实, score<46 代表卖盘推进更真实',
                f"当前依据: components={_detail_val(d, 'components')}",
            ],
        }
    if name == 'carry_regime':
        return {
            'logic': '资金费率与基差簇主要拿来识别拥挤。市场越过热，越要降低追单意愿；市场越不拥挤，趋势延续空间通常更健康。',
            'guide': [
                '调用: FactorEngineV31.evaluate 结构化叠加层',
                '数据: funding_basis_divergence / funding / basis / long_short / market_regime',
                '用途: 判断多空两边是否已经过热，避免在拥挤末端接最后一棒',
                '判定: score高通常表示拥挤度较低或逆向更安全, score低表示一侧已过热',
                f"当前依据: components={_detail_val(d, 'components')}",
            ],
        }
    if name == 'risk_pressure':
        return {
            'logic': '风险压力簇不直接告诉你该做多还是做空，它负责回答另一个问题: 现在该不该把仓位和杠杆放大。',
            'guide': [
                '调用: FactorEngineV31.evaluate 结构化叠加层',
                '数据: volatility_regime / cross_market_correlation / macro_risk_sentiment / fear_greed / strategy_history',
                '用途: 当宏观和策略自身状态都不好时，系统自动收缩风险预算',
                '判定: score高表示可承受风险更高, score低表示优先防守',
                f"当前依据: components={_detail_val(d, 'components')}",
            ],
        }
    if name == 'momentum':
        return {
            'logic': '\u52a8\u91cf\u56e0\u5b50\u4f7f\u7528\u77ed\u4e2d\u671f\u6536\u76ca\u878d\u5408\u8bc4\u4f30\u8d8b\u52bf\u5ef6\u7eed\u6027\uff0c\u516c\u5f0f\u4e0e\u9608\u503c\u6765\u81ea ai_trader_v3.py::FactorEngine.momentum\u3002',
            'guide': [
                '\u8c03\u7528: DataFeed.fetch(spot.price) -> FactorEngine.momentum(ai_trader_v3.py:240)',
                '\u6570\u636e: spot.price + history(spot.price, 120)',
                '\u516c\u5f0f: r5=(px/past[-5]-1)*100, r15=(px/past[-15]-1)*100, combo=0.6*r5+0.4*r15',
                '\u8bc4\u5206: score=50+clamp(combo*6,-25,25), \u591a>55, \u7a7a<45',
                '\u7f6e\u4fe1\u5ea6: 55 + min(30, abs(score-50))',
                f"\u5f53\u524d\u4f9d\u636e: ret_5={_detail_val(d, 'ret_5')}, ret_15={_detail_val(d, 'ret_15')}, combo={_detail_val(d, 'combo')}",
            ],
        }
    if name == 'volatility_regime':
        return {
            'logic': '\u6ce2\u52a8\u533a\u95f4\u56e0\u5b50\u5c06\u5b9e\u73b0\u6ce2\u52a8\u7387\u4e0e24h\u6307\u6807\u6ce2\u52a8\u878d\u5165\u6253\u5206\uff0c\u8fc7\u9ad8\u6ce2\u52a8\u4f1a\u538b\u4f4e\u53ef\u4ea4\u6613\u6027\u8bc4\u5206\u3002',
            'guide': [
                '\u8c03\u7528: FactorEngine.volatility_regime(ai_trader_v3.py:255)',
                '\u6570\u636e: history(spot.price,100) + spot.high24h/low24h/price',
                '\u516c\u5f0f: vol=pstdev(returns)*sqrt(288)*100, hl=(high24-low24)/price*100',
                '\u8bc4\u5206: score-=clamp((vol-2)*8,-20,20); score-=clamp((hl-5)*2,-10,10)',
                '\u9608\u503c: \u591a>53, \u7a7a<47; \u7f6e\u4fe1\u5ea6=50+min(25,abs(score-50))',
                f"\u5f53\u524d\u4f9d\u636e: realized_vol_pct={_detail_val(d, 'realized_vol_pct')}, hl_range_pct={_detail_val(d, 'hl_range_pct')}",
            ],
        }
    if name == 'funding':
        return {
            'logic': '\u8d44\u91d1\u8d39\u7387\u56e0\u5b50\u6839\u636e\u8d44\u91d1\u8d39\u7387z-score\u8bc4\u4f30\u62e5\u6324\u5ea6\uff0cz-score\u8d8a\u9ad8\u8d8a\u504f\u7a7a\u3002',
            'guide': [
                '\u8c03\u7528: DataFeed.fetch(funding-rate) -> FactorEngine.funding(ai_trader_v3.py:274)',
                '\u6570\u636e: funding.funding_rate + history(funding_rate,240)',
                '\u516c\u5f0f: fr=funding_rate*100; z=(fr-mu)/sd (\u6837\u672c\u4e0d\u8db320\u65f6\u7528fr\u76f4\u63a5\u56de\u9000)',
                '\u8bc4\u5206: score-=clamp(z*9,-22,22) \u6216 score-=clamp(fr*120,-15,15)',
                '\u9608\u503c: \u591a>53, \u7a7a<47; \u7f6e\u4fe1\u5ea6=55+min(25,abs(z)*8)',
                f"\u5f53\u524d\u4f9d\u636e: funding_pct={_detail_val(d, 'funding_pct')}, zscore={_detail_val(d, 'zscore')}",
            ],
        }
    if name == 'oi_delta':
        return {
            'logic': 'OI\u4e0e\u4ef7\u683c\u8054\u52a8\u56e0\u5b50\u5728v3.1\u4e2d\u5df2\u589e\u5f3a\u7075\u654f\u5ea6\uff0c\u4e0d\u4ec5\u770b\u5f3a\u4fe1\u53f7\uff0c\u8fd8\u5bf9\u5c0f\u53d8\u5316\u7ed9\u4e88\u5f31\u4fe1\u53f7\u52a0\u51cf\u5206\u3002',
            'guide': [
                '\u8c03\u7528: FactorEngineV31.oi_delta(ai_trader_v3_1.py:646)',
                '\u6570\u636e: spot.price + oi.contracts + history(20)',
                '\u516c\u5f0f: oi_chg/px_chg(\u76f8\u5bf96\u4e2abar)\u3002\u540c\u5411\u4e0a\u6da8+\u5206\uff0c\u4ef7\u8dccOI\u6da8-\u5206\uff0c\u4ef7\u6da8OI\u964d-\u5206',
                '\u5f31\u4fe1\u53f7: |px_chg|>0.05% \u6216 |oi_chg|>0.05% \u65f6\u89e6\u53d1\u5c0f\u5e45\u52a0\u51cf\u5206',
                '\u9608\u503c: \u591a>53, \u7a7a<47; \u7f6e\u4fe1\u5ea6=50+min(30,abs(score-50))',
                f"\u5f53\u524d\u4f9d\u636e: oi_change_6={_detail_val(d, 'oi_change_6')}, price_change_6={_detail_val(d, 'price_change_6')}",
            ],
        }
    if name == 'long_short':
        return {
            'logic': '\u591a\u7a7a\u8d26\u6237\u6bd4\u5728v3.1\u4f7f\u7528\u4e09\u6863\u7075\u654f\u5ea6\uff08\u5f3a/\u4e2d/\u5f31\uff09\uff0c\u907f\u514d\u53ea\u6709\u6781\u7aef\u6570\u503c\u624d\u6709\u4fe1\u53f7\u3002',
            'guide': [
                '\u8c03\u7528: DataFeed.fetch(long-short ratio) -> FactorEngineV31.long_short(ai_trader_v3_1.py:693)',
                '\u6570\u636e: OKX rubik long-short-account-ratio',
                '\u5206\u6863: \u5f3a(>1.45/<0.75)\u3001\u4e2d(>1.20/<0.85)\u3001\u5f31(>1.05/<0.95)',
                '\u8bc4\u5206: ratio\u8fc7\u9ad8\u89c6\u4e3a\u62e5\u6324\u504f\u7a7a\uff0cratio\u8fc7\u4f4e\u89c6\u4e3a\u53cd\u5411\u504f\u591a',
                '\u9608\u503c: \u591a>52, \u7a7a<48; \u7f6e\u4fe1\u5ea6=48+min(30,abs(score-50)*1.2)',
                f"\u5f53\u524d\u4f9d\u636e: ratio={_detail_val(d, 'ratio')}",
            ],
        }
    if name == 'fear_greed':
        return {
            'logic': '\u60c5\u7eea\u56e0\u5b50\u4f7f\u7528 Alternative.me \u6050\u60e7\u6307\u6570\uff0c\u7ed3\u5408\u5f53\u524d\u503c\u4e0e\u53d8\u5316\u91cf\u8fdb\u884c\u9006\u5411\u52a0\u51cf\u5206\u3002',
            'guide': [
                '\u8c03\u7528: DataFeed.fetch(fng limit=2) -> FactorEngine.fear_greed(ai_trader_v3.py:322)',
                '\u6570\u636e: value\u4e0eprev_value',
                '\u89c4\u5219: <=20 +16, <=30 +10, >=80 -16, >=70 -10',
                '\u52a8\u6001\u9879: score += clamp((value-prev)*0.6,-8,8)',
                '\u9608\u503c: \u591a>53, \u7a7a<47; \u7f6e\u4fe1\u5ea6=52+min(22,abs(value-50)*0.4)',
                f"\u5f53\u524d\u4f9d\u636e: value={_detail_val(d, 'value')}, delta={_detail_val(d, 'delta')}",
            ],
        }
    if name == 'basis':
        return {
            'logic': '\u57fa\u5dee\u56e0\u5b50\u7528swap\u4e0espot\u4ef7\u5dee\uff08bps\uff09\u8bc4\u4f30\u8fc7\u70ed/\u8fc7\u51b7\uff0c\u57fa\u5dee\u8d8a\u9ad8\u8d8a\u6291\u5236\u591a\u5934\u8bc4\u5206\u3002',
            'guide': [
                '\u8c03\u7528: DataFeed.fetch(spot/swap ticker) -> FactorEngine.basis(ai_trader_v3.py:340)',
                '\u6570\u636e: spot.price, swap.price',
                '\u516c\u5f0f: bps=((swap-spot)/spot)*10000',
                '\u8bc4\u5206: score -= clamp(bps*0.08,-18,18)',
                '\u9608\u503c: \u591a>53, \u7a7a<47; \u7f6e\u4fe1\u5ea6=50+min(25,abs(bps)*0.2)',
                f"\u5f53\u524d\u4f9d\u636e: basis_bps={_detail_val(d, 'basis_bps')}",
            ],
        }
    if name == 'market_regime':
        return {
            'logic': '\u5e02\u573a\u98ce\u683c\u56e0\u5b50\u5728v3.1\u589e\u52a0\u4e86\u5f31/\u4e2d\u53d8\u5316\u654f\u611f\u5ea6\uff0c\u5e76\u53e0\u52a0BTC\u5e02\u503c\u5360\u6bd4\u5f71\u54cd\u3002',
            'guide': [
                '\u8c03\u7528: DataFeed.fetch(coingecko global) -> FactorEngineV31.market_regime(ai_trader_v3_1.py:718)',
                '\u6570\u636e: market_cap_change_24h + btc_dominance',
                '\u5206\u6863: mcap\u5f3a(>|4|)\u3001\u4e2d(>|2|)\u3001\u5f31(>|0.5|)\u5206\u522b\u52a0\u51cf\u5206',
                '\u5360\u6bd4\u4fee\u6b63: dom>60 \u6263\u5206, dom<50 \u52a0\u5206',
                '\u9608\u503c: \u591a>52, \u7a7a<48; \u7f6e\u4fe1\u5ea6=48+min(26,abs(chg)*1.5)',
                f"\u5f53\u524d\u4f9d\u636e: btc_dom={_detail_val(d, 'btc_dom')}, mcap_change_24h={_detail_val(d, 'mcap_change_24h')}",
            ],
        }
    if name == 'kline_pattern':
        return {
            'logic': 'K\u7ebf\u5f62\u6001\u56e0\u5b50\u4f7f\u75285m K\u7ebf\u8bc6\u522b\u542f\u53d1\u5f0f\u5f62\u6001\uff08\u541e\u6ca1/\u9524\u5934/\u6d41\u661f\uff09\u5e76\u76f4\u63a5\u8f6c\u6362\u4e3a\u5206\u6570\u3002',
            'guide': [
                '\u8c03\u7528: DataFeedV31._fetch_candles(5m) -> FactorEngineV31.kline_pattern(ai_trader_v3_1.py:794)',
                '\u6570\u636e: \u6700\u540e2\u68395m K\u7ebf\u7684O/H/L/C',
                '\u89c4\u5219: bullish_engulfing +10, bearish_engulfing -10, hammer +8, shooting_star -8',
                '\u65b9\u5411: _direction(score, up=54, down=46)',
                '\u7f6e\u4fe1\u5ea6: 45 + min(35, patterns_count*12)',
                f"\u5f53\u524d\u4f9d\u636e: patterns={_detail_val(d, 'patterns')}",
            ],
        }
    if name == 'technical_kline':
        return {
            'logic': '\u6280\u672fK\u7ebf\u56e0\u5b50\u7528h1\u5468\u671f\u7684EMA\u5dee\u3001RSI\u3001ATR\u8054\u5408\u6253\u5206\u3002',
            'guide': [
                '\u8c03\u7528: DataFeedV31._fetch_candles(1H) -> FactorEngineV31.technical_kline(ai_trader_v3_1.py:820)',
                '\u6570\u636e: h1 candles(>=50)',
                '\u516c\u5f0f: score += clamp((ema12-ema26)/close*8000,-15,15)',
                '\u89c4\u5219: RSI<30 +10, RSI>70 -10; \u65b9\u5411\u9608\u503c up54/down46',
                '\u7f6e\u4fe1\u5ea6: 52 + min(25, abs(score-50))',
                f"\u5f53\u524d\u4f9d\u636e: rsi14={_detail_val(d, 'rsi14')}, ema12={_detail_val(d, 'ema12')}, ema26={_detail_val(d, 'ema26')}, atr20={_detail_val(d, 'atr20')}",
            ],
        }
    if name == 'strategy_history':
        return {
            'logic': '\u7b56\u7565\u5386\u53f2\u56e0\u5b50\u5bf9h1\u5386\u53f2\u8fdb\u884cEMA10/30\u7b80\u5316\u56de\u6d4b\uff0c\u7528\u6536\u76ca/\u56de\u64a4/\u80dc\u7387\u751f\u6210\u5065\u5eb7\u5ea6\u5206\u3002',
            'guide': [
                '\u8c03\u7528: FactorEngineV31.strategy_history(ai_trader_v3_1.py:840)',
                '\u6570\u636e: h1 candles(>=80), EMA10/EMA30 \u4fe1\u53f7\u6a21\u62df\u6743\u76ca\u66f2\u7ebf',
                '\u516c\u5f0f: score=50+clamp(ret*220,-20,24)-clamp(dd*180,0,24)+clamp((win-0.5)*40,-8,8)',
                '\u65b9\u5411: _direction(score, up=54, down=46)',
                '\u7f6e\u4fe1\u5ea6: 45 + min(35, bars/8)',
                f"\u5f53\u524d\u4f9d\u636e: ret%={_detail_val(d, 'sim_return_pct')}, maxDD%={_detail_val(d, 'sim_max_drawdown_pct')}, win={_detail_val(d, 'sim_win_rate')}",
            ],
        }
    if name == 'asr_vc':
        return {
            'logic': 'ASR-VC\u56e0\u5b50\u57fa\u4e8eh1\u6eda\u52a8\u652f\u6491/\u963b\u529b\u4f4d\u7f6e+\u6210\u4ea4\u91cfz-score\u8054\u5408\u6253\u5206\uff0c\u540c\u65f6\u652f\u6301\u7a81\u7834\u4e0e\u5747\u503c\u56de\u5f52\u573a\u666f\u3002',
            'guide': [
                '\u8c03\u7528: FactorEngineV31.asr_vc(ai_trader_v3_1.py:864)',
                '\u6570\u636e: h1 candles(>=50), support/resistance=\u8fd148\u6839\u9ad8\u4f4e\u70b9',
                '\u516c\u5f0f: pos=(close-support)/(resistance-support), volume_z=(v-mu)/sd',
                '\u89c4\u5219: pos<0.25 +8, pos>0.75 -8; \u5411\u4e0a/\u5411\u4e0b\u7a81\u7834\u4f34z>1 \u589e\u52a0\u52a0\u51cf\u5206',
                '\u9608\u503c: _direction up54/down46; conf=50+min(28,abs(z)*10)',
                f"\u5f53\u524d\u4f9d\u636e: support={_detail_val(d, 'support')}, resistance={_detail_val(d, 'resistance')}, asr_pos={_detail_val(d, 'asr_pos')}, volume_z={_detail_val(d, 'volume_z')}",
            ],
        }
    if name == 'taker_volume':
        return {
            'logic': 'Taker\u6210\u4ea4\u91cf\u56e0\u5b50\u4f7f\u7528OKX Rubik 5m\u8d2d/\u5356\u4e3b\u52a8\u6210\u4ea4\u91d1\u989d\uff0c\u7edf\u8ba1\u8fd11\u5c0f\u65f6\u4e70\u5356\u538b\u529b\u3002',
            'guide': [
                '\u8c03\u7528: DataFeedV31._fetch_taker_volume -> FactorEngineV31.taker_volume(ai_trader_v3_1.py:921)',
                '\u6570\u636e: OKX /api/v5/rubik/stat/taker-volume?ccy=BTC&period=5m, \u53d6\u6700\u8fd112\u6761',
                '\u516c\u5f0f: ratio=buy_vol/sell_vol; total<1M USD\u76f4\u63a5\u4e2d\u6027',
                '\u89c4\u5219: ratio>1.3/\u003e1.1 \u52a0\u5206, ratio<0.7/\u003c0.9 \u51cf\u5206',
                '\u7f6e\u4fe1\u5ea6: 35 + vol_conf(total) + ratio_conf(abs(ratio-1))',
                f"\u5f53\u524d\u4f9d\u636e: buy={_detail_val(d, 'buy_vol_usd')}, sell={_detail_val(d, 'sell_vol_usd')}, ratio={_detail_val(d, 'buy_sell_ratio')}",
            ],
        }
    if name == 'breakout':
        return {
            'logic': '\u7a81\u7834\u56e0\u5b50\u57fa\u4e8eh1\u533a\u95f4\u7a81\u7834+\u6210\u4ea4\u91cf\u786e\u8ba4+\u8d8b\u52bf\u540c\u5411\u6027\u7efc\u5408\u6253\u5206\u3002',
            'guide': [
                '\u8c03\u7528: FactorEngineV31.breakout(ai_trader_v3_1.py:973)',
                '\u6570\u636e: h1 candles(>=50), support/resistance(\u8fd148h), ATR20, volume_z, MA20/MA50',
                '\u7a81\u7834\u5224\u5b9a: price>resistance+0.5*ATR \u6216 price<support-0.5*ATR',
                '\u52a0\u5206\u9879: volume_z>1/\u003e2 \u4e0e\u8d8b\u52bf\u540c\u5411(uptrend/downtrend)',
                '\u65b9\u5411: \u6700\u7ec8score\u8d70 _direction(up54/down46), \u975e\u7a81\u7834\u65f6\u4f1a\u8bc4\u4f30near setup',
                f"\u5f53\u524d\u4f9d\u636e: breakout_type={_detail_val(d, 'breakout_type')}, volume_z={_detail_val(d, 'volume_z')}, trend={_detail_val(d, 'trend')}",
            ],
        }
    if name == 'mean_reversion':
        return {
            'logic': '\u5747\u503c\u56de\u5f52\u56e0\u5b50\u7528RSI+BB+\u8d8b\u52bf\u8fc7\u6ee4\uff0c\u53ea\u5728\u8d8b\u52bf\u5141\u8bb8\u7684\u65b9\u5411\u51fa\u4fe1\u53f7\uff0c\u907f\u514d\u9006\u52bf\u6284\u5e95/\u6478\u9876\u3002',
            'guide': [
                '\u8c03\u7528: FactorEngineV31.mean_reversion(ai_trader_v3_1.py:1119)',
                '\u6570\u636e: h1 candles(>=60), RSI14, BB(20,2), MA20/MA50, vol_z',
                '\u89c4\u5219: RSI>65 \u8003\u8651\u7a7a, RSI<35 \u8003\u8651\u591a; \u9700\u901a\u8fc7\u8d8b\u52bf\u8fc7\u6ee4',
                '\u8fc7\u6ee4: \u4e0b\u884c\u8d8b\u52bf\u7981\u591a, \u4e0a\u884c\u8d8b\u52bf\u7981\u7a7a; \u6a2a\u76d8\u4f1a\u63d0\u9ad8\u7f6e\u4fe1\u5ea6',
                '\u65b9\u5411: _direction(up54/down46), \u4e2d\u6027\u533a\u95f4\u4f1a\u505aBB\u8fb9\u754c\u8865\u5145\u5224\u65ad',
                f"\u5f53\u524d\u4f9d\u636e: rsi={_detail_val(d, 'rsi')}, bb_pos%={_detail_val(d, 'bb_position_pct')}, trend={_detail_val(d, 'trend')}, signal={_detail_val(d, 'signal')}",
            ],
        }
    if name == 'liquidity_depth':
        return {
            'logic': '\u6d41\u52a8\u6027\u6df1\u5ea6\u56e0\u5b50\u4f7f\u7528OKX orderbook\u5b9e\u65f6\u5206\u6790+\u53cd\u6b3a\u9a97\u8bc6\u522b\uff0c\u901a\u8fc7quality gate\u5bf9raw score\u505a\u98ce\u9669\u95e8\u63a7\u3002',
            'guide': [
                '\u8c03\u7528: FactorEngineV31.liquidity_depth(ai_trader_v3_1.py:1296)',
                '\u6570\u636e: OKX /api/v5/market/books?instId=BTC-USDT-SWAP&sz=50 (+orderbook_tracker\u53ef\u9009)',
                '\u516c\u5f0f: raw_score\u7531imbalance/spread/impact/slope/top5\u7b49\u6784\u6210',
                '\u95e8\u63a7: quality=1-weighted_risk(spoof0.45+spread0.20+impact0.20+depth0.15)',
                '\u6700\u7ec8: gated_score=50+(raw-50)*quality; quality<0.55 \u89e6\u53d1hard_hold(\u65b9\u5411\u4e2d\u6027)',
                f"\u5f53\u524d\u4f9d\u636e: quality={_detail_val(d, 'quality')}, gated_score={_detail_val(d, 'gated_score')}, gate_reason={_detail_val(d, 'gate_reason')}",
            ],
        }
    if name == 'cross_market_correlation':
        return {
            'logic': '\u8de8\u5e02\u573a\u76f8\u5173\u56e0\u5b50\u6765\u81eastock_market_pro_factors.py\uff0c\u57fa\u4e8eBTC\u4e0eSPY\u76f8\u5173\u5ea6\u51b3\u5b9a\u662f\u5426\u8ddf\u968f\u7f8e\u80a1\u65b9\u5411\u3002',
            'guide': [
                '\u8c03\u7528: compute_cross_market_factor(stock_market_pro_factors.py:293)',
                '\u6570\u636e: yfinance BTC-USD & SPY (1mo\u76f8\u5173 + SPY 5d\u6da8\u8dcc)',
                '\u89c4\u5219: |corr|>0.6 \u65f6\u8ddf\u968fSPY(>2% \u52a0\u5206, <-2% \u51cf\u5206), \u5426\u5219\u5f31\u5316\u5f71\u54cd',
                '\u65b9\u5411: \u591a>55, \u7a7a<45',
                '\u7f6e\u4fe1\u5ea6: 30 + min(30, |corr|*40)',
                f"\u5f53\u524d\u4f9d\u636e: corr={_detail_val(d, 'correlation')}, spy_5d_change={_detail_val(d, 'spy_5d_change')}, strength={_detail_val(d, 'correlation_strength')}",
            ],
        }
    if name == 'macro_risk_sentiment':
        return {
            'logic': '\u5b8f\u89c2\u98ce\u9669\u56e0\u5b50\u6765\u81eastock_market_pro_factors.py\uff0c\u7528VIX\u4e0eDXY\u53d8\u5316\u8bc4\u4f30risk-on/risk-off\u3002',
            'guide': [
                '\u8c03\u7528: compute_macro_risk_factor(stock_market_pro_factors.py:345)',
                '\u6570\u636e: yfinance ^VIX \u548c DX-Y.NYB (5d)',
                '\u89c4\u5219: VIX\u4f4e(\u003c15/\u003c20)\u504f\u591a, VIX\u9ad8(\u003e25/\u003e30)\u504f\u7a7a',
                '\u89c4\u5219: DXY\u4e0a\u884c(>0.5/1%)\u504f\u7a7a, DXY\u4e0b\u884c(<-0.5/-1%)\u504f\u591a',
                '\u65b9\u5411: \u591a>55, \u7a7a<45; \u7f6e\u4fe1\u5ea6\u57fa\u4e8eVIX\u72b6\u6001',
                f"\u5f53\u524d\u4f9d\u636e: vix={_detail_val(d, 'vix')}, dxy_5d_change={_detail_val(d, 'dxy_5d_change')}, vix_level={_detail_val(d, 'vix_risk_level')}",
            ],
        }
    if name == 'enhanced_technical':
        return {
            'logic': '\u589e\u5f3a\u6280\u672f\u56e0\u5b50\u6765\u81eastock_market_pro_factors.py\uff0c\u7528yfinance\u6570\u636e\u8ba1\u7b97RSI/MACD/BB/ATR\u7684\u7ec4\u5408\u8bc4\u5206\u3002',
            'guide': [
                '\u8c03\u7528: compute_enhanced_technical_factor(stock_market_pro_factors.py:403)',
                '\u6570\u636e: yfinance BTC-USD(1mo)\u6280\u672f\u6307\u6807',
                '\u89c4\u5219: RSI<30 \u52a0\u5206, RSI>70 \u51cf\u5206; MACD\u91d1/\u6b7b\u53c9\u8c03\u6574\u5206\u503c',
                '\u89c4\u5219: BB\u4f4d\u7f6e<20 \u52a0\u5206, >80 \u51cf\u5206; ATR\u8fc7\u9ad8\u6263\u5206\u5e76\u4e0b\u8c03\u7f6e\u4fe1\u5ea6',
                '\u65b9\u5411: \u591a>55, \u7a7a<45; conf=35+\u6307\u6807\u4e00\u81f4\u6027-\u6ce2\u52a8\u60e9\u7f5a',
                f"\u5f53\u524d\u4f9d\u636e: rsi={_detail_val(d, 'rsi')}, macd_bullish={_detail_val(d, 'macd_bullish')}, bb_position={_detail_val(d, 'bb_position')}, atr_pct={_detail_val(d, 'atr_pct')}",
            ],
        }
    return common_fallback


FACTOR_ROLE_META: Dict[str, Dict[str, Any]] = {
    'benchmark_regime': {'label': 'BTC基准环境', 'group': 'main', 'thesis': '先判断大环境是否值得承担方向性风险。'},
    'trend_stack': {'label': '趋势结构簇', 'group': 'main', 'thesis': '把趋势、突破、K线结构与增强技术面合并，减少单指标误判。'},
    'flow_impulse': {'label': '订单流推进簇', 'group': 'main', 'thesis': '只有真实资金推进，趋势才更有延续性。'},
    'carry_regime': {'label': '基差资金费率簇', 'group': 'main', 'thesis': '判断市场是否拥挤、是否适合追单。'},
    'risk_pressure': {'label': '风险压力簇', 'group': 'aux', 'thesis': '宏观、波动率和策略历史表现用于决定风险预算。'},
    'order_flow_persistence': {'label': '订单流持续性', 'group': 'main', 'thesis': '不是看某一刻谁买得多，而是看 3-5 个窗口是否持续同向。'},
    'liquidity_depth': {'label': '盘口深度', 'group': 'main', 'thesis': '盘口深度不足时，信号再好也容易被滑点和假挂单吃掉。'},
    'liquidation_pressure': {'label': '清算压力', 'group': 'main', 'thesis': '价格靠近拥挤清算区时，容易出现加速波动。'},
    'funding_basis_divergence': {'label': '资金费率-基差背离', 'group': 'main', 'thesis': '资金费率与基差越同向过热，越不适合追涨杀跌。'},
    'oi_delta': {'label': 'OI变化', 'group': 'main', 'thesis': '判断价格变化背后是否有新增杠杆资金参与。'},
    'taker_volume': {'label': '主动成交量', 'group': 'main', 'thesis': '看主动买卖盘谁在抢着成交。'},
    'breakout': {'label': '突破因子', 'group': 'main', 'thesis': '识别价格是否真的脱离区间，而不是区间内噪声。'},
    'momentum': {'label': '动量', 'group': 'aux', 'thesis': '确认近中期价格惯性。'},
    'mean_reversion': {'label': '均值回归', 'group': 'aux', 'thesis': '只在震荡时有价值，趋势行情里容易逆势。'},
    'technical_kline': {'label': '技术K线', 'group': 'aux', 'thesis': 'EMA、RSI、ATR 提供技术面结构。'},
    'kline_pattern': {'label': 'K线形态', 'group': 'aux', 'thesis': '适合做入场微调，不适合单独做主信号。'},
    'asr_vc': {'label': '支撑阻力-量能确认', 'group': 'aux', 'thesis': '看当前位置接近支撑还是阻力，并用量能确认。'},
    'strategy_history': {'label': '策略历史状态', 'group': 'aux', 'thesis': '策略自身近期回撤和失效率会反过来约束风险。'},
    'cross_market_correlation': {'label': '跨市场相关性', 'group': 'aux', 'thesis': 'BTC 与美股/风险资产联动增强时，单看链上和合约容易失真。'},
    'macro_risk_sentiment': {'label': '宏观风险情绪', 'group': 'aux', 'thesis': '高波动、高恐慌时，先缩风险预算。'},
    'enhanced_technical': {'label': '增强技术面', 'group': 'aux', 'thesis': 'MACD、EMA 结构、RSI 的综合确认。'},
    'funding': {'label': '资金费率', 'group': 'aux', 'thesis': '更适合用来识别拥挤，而不是直接追单。'},
    'basis': {'label': '基差', 'group': 'aux', 'thesis': '基差高说明永续或期货相对现货过热。'},
    'long_short': {'label': '多空账户比', 'group': 'aux', 'thesis': '判断大众账户是否过度一边倒。'},
    'fear_greed': {'label': '情绪指标', 'group': 'aux', 'thesis': '更适合作为环境过滤，而不是日内单独开仓。'},
    'market_regime': {'label': '市场风格', 'group': 'aux', 'thesis': '风险偏好阶段会改变同一因子的有效性。'},
    'volatility_regime': {'label': '波动率环境', 'group': 'aux', 'thesis': '高波动阶段应减少反复交易。'},
    'trend_ema': {'label': 'EMA趋势', 'group': 'aux', 'thesis': '短中期均线堆叠，用来衡量趋势是否顺畅。'},
    'rsi_reversion': {'label': 'RSI回归', 'group': 'aux', 'thesis': '偏向震荡市过滤，不适合单独做趋势信号。'},
    'breakout_20': {'label': '20周期突破', 'group': 'main', 'thesis': '用固定通道突破判断是否摆脱盘整。'},
    'volume_flow': {'label': '量价流', 'group': 'aux', 'thesis': '成交量是否支持当前价格方向。'},
    'basis_spread': {'label': '期现基差', 'group': 'aux', 'thesis': '永续与现货价差代表短期拥挤程度。'},
    'funding_rate': {'label': '历史资金费率', 'group': 'aux', 'thesis': '历史资金费率持续偏一侧时，拥挤风险会上升。'},
    'oi_momentum': {'label': 'OI动量', 'group': 'main', 'thesis': '新增持仓是否在配合价格变化。'},
    'long_short_ratio': {'label': '多空持仓比', 'group': 'aux', 'thesis': '合约参与者是否过度偏多或偏空。'},
    'taker_imbalance': {'label': '主动买卖失衡', 'group': 'main', 'thesis': '短线最直接的买卖盘推进信号之一。'},
}


def factor_meta(key: str) -> Dict[str, Any]:
    base = FACTOR_ROLE_META.get(str(key) or '', {})
    return {
        'label': str(base.get('label') or key),
        'group': str(base.get('group') or 'aux'),
        'thesis': str(base.get('thesis') or '该因子参与环境过滤或方向确认。'),
    }


def default_factor_groups() -> Dict[str, List[str]]:
    main = [k for k, v in FACTOR_ROLE_META.items() if str(v.get('group')) == 'main']
    aux = [k for k, v in FACTOR_ROLE_META.items() if str(v.get('group')) != 'main']
    return {'main': main, 'aux': aux}


def factor_story(key: str, score: float, confidence: float, direction: str, details: Dict[str, Any] | None = None) -> str:
    meta = factor_meta(key)
    dir_txt = '偏多' if direction == 'bullish' else '偏空' if direction == 'bearish' else '观望'
    strength = '强' if abs(score - 50) >= 15 else '中' if abs(score - 50) >= 8 else '弱'
    d = details if isinstance(details, dict) else {}
    snippets = []
    for k, v in list(d.items())[:3]:
        snippets.append(f'{k}={v}')
    snippet_txt = '；'.join(snippets) if snippets else '暂无额外实时值'
    return f"{meta['label']}当前{dir_txt}，信号强度{strength}，置信度{round(confidence, 2)}。{meta['thesis']} 当前关键读数：{snippet_txt}。"


def load_mode() -> str:
    d = read_json(MODE_FILE, {'mode': 'paper'})
    m = str((d or {}).get('mode', 'paper')).lower()
    return m if m in {'paper', 'live'} else 'paper'


def save_mode(mode: str) -> None:
    write_json(MODE_FILE, {'mode': mode, 'updated_at': datetime.now(timezone.utc).isoformat()})


def parse_okx_config() -> Dict[str, Any]:
    if not os.path.exists(OKX_CONFIG):
        return {'default_profile': '', 'profiles': {}}
    lines = open(OKX_CONFIG, 'r', encoding='utf-8').read().splitlines()
    default_profile = ''
    profiles: Dict[str, Dict[str, Any]] = {}
    cur = None
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('default_profile'):
            parts = line.split('=', 1)
            if len(parts) == 2:
                default_profile = parts[1].strip().strip('"').strip("'")
            continue
        if line.startswith('[profiles.') and line.endswith(']'):
            name = line[len('[profiles.'):-1]
            cur = name
            profiles.setdefault(cur, {})
            continue
        if cur and '=' in line:
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k == 'demo':
                profiles[cur][k] = (v.lower() == 'true')
            else:
                profiles[cur][k] = v
    return {'default_profile': default_profile, 'profiles': profiles}


def pick_profile(mode: str) -> Dict[str, Any]:
    cfg = parse_okx_config()
    profiles = cfg.get('profiles', {})
    if mode == 'paper':
        if 'okx-demo' in profiles:
            p = dict(profiles['okx-demo'])
            p['name'] = 'okx-demo'
            p['demo'] = True
            return p
    if mode == 'live':
        if 'okx-live' in profiles:
            p = dict(profiles['okx-live'])
            p['name'] = 'okx-live'
            p['demo'] = False
            return p
    dp = cfg.get('default_profile')
    if dp and dp in profiles:
        p = dict(profiles[dp])
        p['name'] = dp
        p.setdefault('demo', mode == 'paper')
        return p
    if profiles:
        name = list(profiles.keys())[0]
        p = dict(profiles[name])
        p['name'] = name
        p.setdefault('demo', mode == 'paper')
        return p
    return {}


def okx_request(profile: Dict[str, Any], method: str, endpoint: str, body: Dict[str, Any] | None = None) -> Dict[str, Any]:
    api_key = str(profile.get('api_key', ''))
    secret = str(profile.get('secret_key', ''))
    passphrase = str(profile.get('passphrase', ''))
    if not api_key or not secret or not passphrase:
        return {'code': '-1', 'msg': 'missing_api_credentials', 'data': []}
    ts = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    body_str = json.dumps(body, separators=(',', ':')) if body else ''
    prehash = f"{ts}{method.upper()}/api/v5{endpoint}{body_str}"
    sign = base64.b64encode(hmac_new(secret.encode(), prehash.encode(), sha256).digest()).decode()
    headers = {
        'OK-ACCESS-KEY': api_key,
        'OK-ACCESS-SIGN': sign,
        'OK-ACCESS-TIMESTAMP': ts,
        'OK-ACCESS-PASSPHRASE': passphrase,
        'Content-Type': 'application/json',
    }
    if profile.get('demo'):
        headers['x-simulated-trading'] = '1'
    url = f"{OKX_BASE}/api/v5{endpoint}"
    try:
        r = requests.request(method.upper(), url, headers=headers, data=body_str if body else None, timeout=12)
        return r.json()
    except Exception as e:
        return {'code': '-1', 'msg': str(e), 'data': []}



def get_okx_account(mode: str) -> Dict[str, Any]:
    profile = pick_profile(mode)
    if not profile:
        return {
            'mode': mode,
            'profile': '',
            'configured': False,
            'error': 'okx_config_missing',
            'trading_balances': [],
            'funding_balances': [],
            'positions': [],
            'trading_equity_usdt': 0,
            'funding_equity_usdt': 0,
            'total_equity_usdt': 0,
            'position_value_usdt': 0,
        }

    bal_res = okx_request(profile, 'GET', '/account/balance')
    fund_bal_res = okx_request(profile, 'GET', '/asset/balances')
    fund_val_res = okx_request(profile, 'GET', '/asset/asset-valuation?ccy=USDT')
    pos_res = okx_request(profile, 'GET', '/account/positions?instType=SWAP')

    trading_balances: List[Dict[str, Any]] = []
    funding_balances: List[Dict[str, Any]] = []
    positions: List[Dict[str, Any]] = []
    trading_eq = 0.0
    funding_eq = 0.0
    pos_value = 0.0

    try:
        details = ((bal_res.get('data') or [{}])[0].get('details') or [])
        for b in details:
            eq = to_num(b.get('eq'))
            if eq <= 0:
                continue
            usd = to_num(b.get('eqUsd'))
            trading_eq += usd if usd > 0 else 0.0
            trading_balances.append({
                'ccy': b.get('ccy'),
                'equity': eq,
                'avail': to_num(b.get('availBal')),
                'frozen': to_num(b.get('frozenBal')),
                'eq_usd': usd,
            })
    except Exception:
        pass

    try:
        rows = fund_val_res.get('data') or []
        if rows:
            funding_eq = to_num(rows[0].get('totalBal'), 0)
    except Exception:
        funding_eq = 0.0

    try:
        for b in (fund_bal_res.get('data') or []):
            bal = to_num(b.get('bal'))
            if bal <= 0:
                continue
            funding_balances.append({
                'ccy': b.get('ccy'),
                'equity': bal,
                'avail': to_num(b.get('availBal')),
                'frozen': to_num(b.get('frozenBal')),
            })
    except Exception:
        pass

    try:
        for p in (pos_res.get('data') or []):
            mark = to_num(p.get('markPx'))
            sz = abs(to_num(p.get('pos')))
            ct_val = to_num(p.get('ctVal'), 1.0)
            notional = abs(to_num(p.get('notionalUsd')))
            if notional <= 0:
                notional = abs(to_num(p.get('notionalCcy'))) * mark
            if notional <= 0:
                notional = abs(sz * ct_val * mark)
            pos_value += notional
            positions.append({
                'instId': p.get('instId'),
                'side': p.get('posSide') or p.get('side') or 'net',
                'size': sz,
                'markPx': mark,
                'notional': notional,
                'upl': to_num(p.get('upl')),
                'uplRatioPct': to_num(p.get('uplRatio')) * 100,
                'lever': to_num(p.get('lever'), 1),
                'liqPx': to_num(p.get('liqPx')),
                'mgnMode': p.get('mgnMode'),
                'entryPx': to_num(p.get('avgPx') or p.get('fillPx') or p.get('openAvgPx')),
            })
    except Exception:
        pass

    trading_balances.sort(key=lambda x: x.get('eq_usd', 0), reverse=True)
    funding_balances.sort(key=lambda x: x.get('equity', 0), reverse=True)
    positions.sort(key=lambda x: x.get('notional', 0), reverse=True)

    return {
        'mode': mode,
        'profile': profile.get('name', ''),
        'configured': True,
        'demo': bool(profile.get('demo', False)),
        'trading_balances': trading_balances,
        'funding_balances': funding_balances,
        'positions': positions,
        'trading_equity_usdt': round(trading_eq, 4),
        'funding_equity_usdt': round(funding_eq, 4),
        'total_equity_usdt': round(trading_eq + funding_eq, 4),
        'position_value_usdt': round(pos_value, 4),
        'raw_error': bal_res.get('msg') if str(bal_res.get('code')) != '0' else '',
    }

def get_paper_account() -> Dict[str, Any]:
    acc = read_json(ACCOUNT_FILE, {})
    pos = read_json(PAPER_POS_FILE, [])
    if not isinstance(acc, dict):
        acc = {}
    if not isinstance(pos, list):
        pos = []

    current_price = 0.0
    try:
        resp = requests.get('https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP', timeout=5)
        if resp.status_code == 200:
            data = resp.json().get('data', [])
            if data:
                current_price = float(data[0].get('last', 0) or 0)
    except Exception:
        pass

    from datetime import datetime as _dt, timezone as _tz
    now = _dt.now(_tz.utc)
    total_unrealized_pnl = 0.0
    enhanced_positions: List[Dict[str, Any]] = []

    for p in pos:
        ep = dict(p)
        entry_price = float(p.get('entryPrice', 0) or 0)
        size = float(p.get('size', 0) or 0)
        lever = float(p.get('lever', 1) or 1)
        side = str(p.get('side', '') or '')

        mark_price = float(p.get('markPx', entry_price) or entry_price)
        if current_price > 0:
            mark_price = current_price

        pnl = float(p.get('unrealizedPnl', 0) or 0)
        pnl_pct = float(p.get('unrealizedPnlPct', 0) or 0)
        if mark_price > 0 and entry_price > 0 and size > 0:
            if side == 'long':
                pnl = (mark_price - entry_price) * size
                pnl_pct = ((mark_price - entry_price) / entry_price) * 100.0 * max(lever, 1)
            elif side == 'short':
                pnl = (entry_price - mark_price) * size
                pnl_pct = ((entry_price - mark_price) / entry_price) * 100.0 * max(lever, 1)

        ep['markPx'] = round(mark_price, 4)
        ep['unrealizedPnl'] = round(pnl, 4)
        ep['unrealizedPnlPct'] = round(pnl_pct, 4)
        ep['notional'] = round(abs(size * mark_price), 4)
        total_unrealized_pnl += pnl

        try:
            open_time_str = str(p.get('openTime', '') or '').strip()
            if open_time_str:
                open_time_str = open_time_str.replace('Z', '+00:00')
                if '+' not in open_time_str and '-' not in open_time_str[-6:]:
                    open_time_str += '+00:00'
                open_time = _dt.fromisoformat(open_time_str)
                if open_time.tzinfo is None:
                    open_time = open_time.replace(tzinfo=_tz.utc)
                hold_hours = (now - open_time).total_seconds() / 3600.0
                max_hold = float(p.get('maxHoldHours', 4) or 4)
                ep['hold_hours'] = round(hold_hours, 2)
                ep['max_hold_hours'] = max_hold
                ep['timeout'] = hold_hours > max_hold
            else:
                ep['hold_hours'] = 0.0
                ep['max_hold_hours'] = 4.0
                ep['timeout'] = False
        except Exception:
            ep['hold_hours'] = 0.0
            ep['max_hold_hours'] = 4.0
            ep['timeout'] = False

        enhanced_positions.append(ep)

    return {
        'initial_balance': to_num(acc.get('initialBalance', 1000), 1000),
        'balance': to_num(acc.get('balance', 1000), 1000),
        'equity': to_num(acc.get('equity', 1000), 1000),
        'realized_pnl': to_num(acc.get('realizedPnl', 0), 0),
        'unrealized_pnl': round(total_unrealized_pnl, 4),
        'daily_pnl': to_num(acc.get('dailyPnl', 0), 0),
        'positions': enhanced_positions,
        'current_price': current_price,
    }


def _pick_open_signal_snapshot(trades: List[Dict[str, Any]], side: str, open_ms: int | None, used: set[int]) -> Dict[str, Any]:
    if not trades:
        return {}
    best_idx = -1
    best_delta = 10 * 60 * 1000 + 1
    for idx, row in enumerate(trades):
        if idx in used or not isinstance(row, dict):
            continue
        if str(row.get('direction', '')).lower() != side.lower():
            continue
        ts_ms = parse_time_to_ms(row.get('ts'))
        if ts_ms is None:
            continue
        if open_ms is None:
            best_idx = idx
            break
        delta = abs(ts_ms - open_ms)
        if delta <= 10 * 60 * 1000 and delta < best_delta:
            best_delta = delta
            best_idx = idx
    if best_idx >= 0:
        used.add(best_idx)
        return trades[best_idx]
    return {}


def _infer_close_position_side(direction: str, strategy: str) -> str:
    d = str(direction or '').lower()
    s = str(strategy or '').lower()
    if 'close' not in d and 'close' not in s:
        return ''

    order_side = ''
    if 'long' in d:
        order_side = 'long'
    elif 'short' in d:
        order_side = 'short'
    elif d in {'long', 'short'}:
        order_side = d
    elif 'long' in s:
        order_side = 'long'
    elif 'short' in s:
        order_side = 'short'

    if order_side == 'long':
        return 'short'
    if order_side == 'short':
        return 'long'
    return ''


def _infer_close_reason(row: Dict[str, Any]) -> str:
    reason = str(row.get('reason') or row.get('event_type') or '').strip()
    if reason:
        return reason
    direction = str(row.get('direction') or '').lower()
    strategy = str(row.get('strategy') or '').lower()
    if 'manual' in strategy:
        return '手动平仓'
    if 'sl_tp' in direction or 'sl' in direction or 'tp' in direction:
        return '止盈止损触发'
    if 'close' in strategy or 'close' in direction:
        return '策略平仓'
    return '平仓'


def _fmt_trade_num(v: Any, digits: int = 4) -> str:
    try:
        n = float(v)
    except Exception:
        return '--'
    if not math.isfinite(n):
        return '--'
    s = f"{n:.{max(0, int(digits))}f}".rstrip('0').rstrip('.')
    return s if s else '0'


def _side_zh(side: str) -> str:
    s = str(side or '').strip().lower()
    if s == 'long':
        return '做多'
    if s == 'short':
        return '做空'
    return s or '-'


def _factor_key_zh(key: str) -> str:
    k = str(key or '').strip()
    if not k:
        return '-'
    alias = {
        'benchmark_regime': 'BTC基准环境',
        'trend_stack': '趋势结构簇',
        'flow_impulse': '订单流推进簇',
        'carry_regime': '拥挤度簇',
        'risk_pressure': '风险压力簇',
        'system_score': '系统综合分',
        'score': '综合评分',
        'confidence': '置信度',
    }
    if k in alias:
        return alias[k]
    meta = factor_meta(k)
    label = str(meta.get('label') or '').strip()
    if label:
        return label
    return k


def _extract_factor_items(signal: Dict[str, Any]) -> List[tuple[str, float]]:
    if not isinstance(signal, dict):
        return []
    numeric: Dict[str, float] = {}

    def push(name: Any, val: Any) -> None:
        k = str(name or '').strip()
        if not k:
            return
        if k in {
            'ts', 'timestamp', 'time', 'price', 'entry_price', 'open_price', 'close_price',
            'score', 'confidence', 'leverage', 'size', 'size_btc', 'notional',
            'pnl', 'net_pnl', 'gross_pnl', 'pnl_pct', 'hold_minutes',
            'max_hold_hours', 'max_hold_minutes', 'stop_loss', 'take_profit',
        }:
            return
        if k.startswith('_'):
            return
        try:
            n = float(val)
        except Exception:
            return
        if not math.isfinite(n):
            return
        numeric[k] = n

    def scan_dict(obj: Dict[str, Any]) -> None:
        for k, v in obj.items():
            if isinstance(v, dict):
                hit = False
                for vk in ['score', 'value', 'weight', 'signal', 'strength', 'zscore']:
                    if vk in v:
                        push(k, v.get(vk))
                        hit = True
                        break
                if not hit:
                    continue
            elif isinstance(v, (int, float)):
                push(k, v)
            elif isinstance(v, str):
                sv = v.strip()
                if re.fullmatch(r'[-+]?\d+(\.\d+)?', sv):
                    push(k, sv)

    def scan_list(arr: List[Any]) -> None:
        for row in arr:
            if not isinstance(row, dict):
                continue
            name = row.get('key') or row.get('name') or row.get('factor') or row.get('id')
            val = None
            for vk in ['score', 'value', 'weight', 'signal', 'strength', 'zscore']:
                if vk in row:
                    val = row.get(vk)
                    break
            if name is not None and val is not None:
                push(name, val)

    for k, v in (signal.items() if isinstance(signal, dict) else []):
        if k in FACTOR_ROLE_META or k in {'benchmark_regime', 'trend_stack', 'flow_impulse', 'carry_regime', 'risk_pressure', 'liquidation_event'}:
            push(k, v)
    for key in [
        'factors', 'factor_scores', 'cluster_scores', 'score_breakdown', 'components',
        'factor_breakdown', 'decision_basis', 'signal_detail', 'weights',
    ]:
        obj = signal.get(key)
        if isinstance(obj, dict):
            scan_dict(obj)
        elif isinstance(obj, list):
            scan_list(obj)

    rows = [(k, v) for k, v in numeric.items() if abs(v) > 1e-9]
    rows.sort(key=lambda x: abs(x[1]), reverse=True)
    return rows[:4]


def _entry_factor_basis_zh(signal: Dict[str, Any]) -> str:
    items = _extract_factor_items(signal if isinstance(signal, dict) else {})
    if items:
        return '、'.join([f"{_factor_key_zh(k)}({v:+.2f})" for k, v in items[:3]])
    rs = str((signal or {}).get('reason') or (signal or {}).get('rationale') or (signal or {}).get('analysis') or '').strip()
    if rs:
        return rs[:120]
    return '无明确因子快照，按策略阈值开仓'


def _pick_plan_value(open_src: Dict[str, Any], close_src: Dict[str, Any], keys: List[str]) -> Any:
    for obj in [open_src, close_src]:
        if not isinstance(obj, dict):
            continue
        candidates = [obj]
        nested_open = obj.get('open_event')
        if isinstance(nested_open, dict):
            candidates.append(nested_open)
        for c in candidates:
            for k in keys:
                if k in c and c.get(k) not in [None, '', 'null', 'None']:
                    return c.get(k)
    return None


def _position_detail_zh(
    side: str,
    open_price: float,
    size_btc: float,
    leverage: float,
    notional: float,
    open_src: Dict[str, Any],
    close_src: Dict[str, Any],
) -> str:
    lev = max(0.0, to_num(leverage, 1.0))
    parts = [
        f"方向{_side_zh(side)}",
        f"仓位{_fmt_trade_num(size_btc, 8)} BTC",
        f"杠杆{_fmt_trade_num(lev, 2)}x",
        f"名义{_fmt_trade_num(notional, 2)}U",
        f"开仓价{_fmt_trade_num(open_price, 2)}",
    ]
    sl = _pick_plan_value(open_src, close_src, ['stop_loss', 'stop_loss_price', 'stop_price', 'stopPx', 'sl_price', 'sl'])
    tp = _pick_plan_value(open_src, close_src, ['take_profit', 'take_profit_price', 'take_profit_px', 'tp_price', 'tp'])
    mm = _pick_plan_value(open_src, close_src, ['mgn_mode', 'margin_mode', 'mgnMode'])
    hh = _pick_plan_value(open_src, close_src, ['max_hold_hours', 'maxHoldHours'])
    hm = _pick_plan_value(open_src, close_src, ['max_hold_minutes', 'maxHoldMinutes', 'hold_limit_minutes', 'holding_limit_minutes'])
    if sl is not None:
        parts.append(f"止损{_fmt_trade_num(sl, 2)}")
    if tp is not None:
        parts.append(f"止盈{_fmt_trade_num(tp, 2)}")
    if hh is not None:
        parts.append(f"最长持仓{_fmt_trade_num(hh, 2)}h")
    elif hm is not None:
        parts.append(f"最长持仓{_fmt_trade_num(hm, 0)}分钟")
    if mm is not None:
        parts.append(f"保证金模式{str(mm)}")
    return '；'.join(parts)


def _entry_logic_zh(strategy: str, side: str, score: Any, confidence: Any, signal: Dict[str, Any]) -> str:
    st = str(strategy or '').strip() or '默认策略'
    score_txt = '--'
    conf_txt = '--'
    try:
        score_n = float(score)
        if math.isfinite(score_n):
            score_txt = _fmt_trade_num(score_n, 2)
    except Exception:
        pass
    try:
        conf_n = float(confidence)
        if math.isfinite(conf_n):
            conf_txt = _fmt_trade_num(conf_n, 2)
    except Exception:
        pass
    factors = _entry_factor_basis_zh(signal if isinstance(signal, dict) else {})
    return f"策略:{st}；方向:{_side_zh(side)}；评分/置信度:{score_txt}/{conf_txt}；主因子:{factors}"


def build_trade_lifecycle_from_trades(trades: List[Dict[str, Any]], keep: int = 220) -> List[Dict[str, Any]]:
    if not isinstance(trades, list):
        return []

    ordered = sorted(
        [x for x in trades if isinstance(x, dict)],
        key=lambda x: parse_time_to_ms(x.get('ts')) or 0,
    )
    open_stacks: Dict[str, List[Dict[str, Any]]] = {'long': [], 'short': []}
    rows: List[Dict[str, Any]] = []

    def make_closed_row(open_ev: Dict[str, Any], close_ev: Dict[str, Any]) -> Dict[str, Any]:
        open_ts = str(open_ev.get('open_ts') or '')
        close_ts = str(close_ev.get('ts') or '')
        open_ts_ms = parse_time_to_ms(open_ts)
        close_ts_ms = parse_time_to_ms(close_ts)
        side = str(open_ev.get('side') or '').lower()
        open_price = to_num(open_ev.get('open_price'))
        close_price = to_num(close_ev.get('price'))
        size = to_num(close_ev.get('size_btc') or open_ev.get('size_btc'))
        notional = abs(open_price * size)
        pnl = close_ev.get('pnl')
        if pnl is None:
            if side == 'long':
                pnl = (close_price - open_price) * size
            elif side == 'short':
                pnl = (open_price - close_price) * size
            else:
                pnl = 0.0
        pnl = to_num(pnl)
        hold_minutes = None
        if open_ts_ms is not None and close_ts_ms is not None:
            hold_minutes = round(max(0.0, (close_ts_ms - open_ts_ms) / 60000.0), 2)
        strategy = str(open_ev.get('strategy') or close_ev.get('strategy') or '')
        lev = to_num(open_ev.get('leverage') or close_ev.get('leverage'), 1)
        signal_snapshot = open_ev.get('signal_snapshot') if isinstance(open_ev.get('signal_snapshot'), dict) else {}
        return {
            'position_id': str(open_ev.get('position_id') or f"trades-{int(close_ts_ms or 0)}"),
            'open_ts': open_ts,
            'close_ts': close_ts,
            'side': side,
            'strategy': strategy,
            'status': 'close',
            'close_reason': _infer_close_reason(close_ev),
            'open_price': round(open_price, 6),
            'close_price': round(close_price, 6),
            'size_btc': round(size, 8),
            'leverage': lev,
            'notional': round(notional, 4),
            'gross_pnl': round(pnl, 4),
            'net_pnl': round(pnl, 4),
            'total_cost': 0.0,
            'pnl_pct': round((pnl / notional * 100.0) if notional > 0 else 0.0, 4),
            'hold_minutes': hold_minutes,
            'signal_score': open_ev.get('signal_score'),
            'signal_confidence': open_ev.get('signal_confidence'),
            'factor_basis_zh': _entry_factor_basis_zh(signal_snapshot),
            'entry_logic_zh': _entry_logic_zh(strategy, side, open_ev.get('signal_score'), open_ev.get('signal_confidence'), signal_snapshot),
            'position_detail_zh': _position_detail_zh(side, open_price, size, lev, notional, open_ev if isinstance(open_ev, dict) else {}, close_ev if isinstance(close_ev, dict) else {}),
        }

    for t in ordered:
        direction = str(t.get('direction') or '').lower()
        strategy = str(t.get('strategy') or '')
        ts = str(t.get('ts') or '')
        ts_ms = parse_time_to_ms(ts)
        size = to_num(t.get('size_btc'))
        price = to_num(t.get('price'))
        lev = to_num(t.get('leverage'), 1)
        is_close = ('close' in direction) or ('close' in strategy.lower())

        if not is_close and direction in {'long', 'short'}:
            # Some old test snapshots are already "flat" records with realized pnl only.
            if t.get('pnl') is not None and str(strategy).lower().startswith('test_'):
                notional = abs(price * size)
                pnl = to_num(t.get('pnl'))
                rows.append({
                    'position_id': f"legacy-{int(ts_ms or 0)}",
                    'open_ts': ts,
                    'close_ts': ts,
                    'side': direction,
                    'strategy': strategy,
                    'status': 'close',
                    'close_reason': '历史快照(无开平仓明细)',
                    'open_price': round(price, 6),
                    'close_price': round(price, 6),
                    'size_btc': round(size, 8),
                    'leverage': lev,
                    'notional': round(notional, 4),
                    'gross_pnl': round(pnl, 4),
                    'net_pnl': round(pnl, 4),
                    'total_cost': 0.0,
                    'pnl_pct': round((pnl / notional * 100.0) if notional > 0 else 0.0, 4),
                    'hold_minutes': 0.0,
                    'signal_score': t.get('score'),
                    'signal_confidence': t.get('confidence'),
                    'factor_basis_zh': _entry_factor_basis_zh(t),
                    'entry_logic_zh': _entry_logic_zh(strategy, direction, t.get('score'), t.get('confidence'), t),
                    'position_detail_zh': _position_detail_zh(direction, price, size, lev, notional, t, t),
                })
                continue
            open_stacks[direction].append({
                'position_id': f"open-{int(ts_ms or 0)}-{len(open_stacks[direction])}",
                'open_ts': ts,
                'open_ts_ms': ts_ms,
                'side': direction,
                'strategy': strategy,
                'open_price': price,
                'size_btc': size,
                'leverage': lev,
                'signal_score': t.get('score'),
                'signal_confidence': t.get('confidence'),
                'signal_snapshot': t,
            })
            continue

        if not is_close:
            continue

        close_target_side = _infer_close_position_side(direction, strategy)
        if close_target_side not in {'long', 'short'}:
            close_target_side = 'long' if direction == 'short' else 'short' if direction == 'long' else ''
        stack = open_stacks.get(close_target_side, [])
        if not stack:
            # Unknown opener: still produce a close row for visibility.
            pseudo_open = {
                'position_id': f"unknown-open-{int(ts_ms or 0)}",
                'open_ts': ts,
                'open_ts_ms': ts_ms,
                'side': close_target_side or ('long' if direction == 'short' else 'short' if direction == 'long' else 'long'),
                'strategy': strategy,
                'open_price': price,
                'size_btc': size,
                'leverage': lev,
                'signal_score': t.get('score'),
                'signal_confidence': t.get('confidence'),
                'signal_snapshot': t,
            }
            rows.append(make_closed_row(pseudo_open, t))
            continue

        best_idx = len(stack) - 1
        if size > 0:
            best_gap = None
            for idx in range(len(stack) - 1, -1, -1):
                gap = abs(to_num(stack[idx].get('size_btc')) - size)
                if best_gap is None or gap < best_gap:
                    best_gap = gap
                    best_idx = idx
        open_ev = stack.pop(best_idx)
        rows.append(make_closed_row(open_ev, t))

    now_ts = now_ms()
    for side, stack in open_stacks.items():
        for open_ev in stack:
            open_price = to_num(open_ev.get('open_price'))
            open_size = to_num(open_ev.get('size_btc'))
            notional = abs(open_price * open_size)
            hold_minutes = None
            open_ts_ms = open_ev.get('open_ts_ms')
            if open_ts_ms is not None:
                hold_minutes = round(max(0.0, (now_ts - int(open_ts_ms)) / 60000.0), 2)
            strategy = str(open_ev.get('strategy') or '')
            signal_snapshot = open_ev.get('signal_snapshot') if isinstance(open_ev.get('signal_snapshot'), dict) else {}
            lev = to_num(open_ev.get('leverage'), 1)
            rows.append({
                'position_id': str(open_ev.get('position_id') or ''),
                'open_ts': str(open_ev.get('open_ts') or ''),
                'close_ts': '',
                'side': side,
                'strategy': strategy,
                'status': 'open',
                'close_reason': '持仓中',
                'open_price': round(open_price, 6),
                'close_price': 0.0,
                'size_btc': round(open_size, 8),
                'leverage': lev,
                'notional': round(notional, 4),
                'gross_pnl': 0.0,
                'net_pnl': 0.0,
                'total_cost': 0.0,
                'pnl_pct': 0.0,
                'hold_minutes': hold_minutes,
                'signal_score': open_ev.get('signal_score'),
                'signal_confidence': open_ev.get('signal_confidence'),
                'factor_basis_zh': _entry_factor_basis_zh(signal_snapshot),
                'entry_logic_zh': _entry_logic_zh(strategy, side, open_ev.get('signal_score'), open_ev.get('signal_confidence'), signal_snapshot),
                'position_detail_zh': _position_detail_zh(side, open_price, open_size, lev, notional, open_ev if isinstance(open_ev, dict) else {}, {}),
            })

    rows.sort(key=lambda x: parse_time_to_ms(x.get('close_ts')) or parse_time_to_ms(x.get('open_ts')) or 0, reverse=True)
    return rows[:keep]


def build_trade_lifecycle(trades: List[Dict[str, Any]], cost_ledger: List[Dict[str, Any]], keep: int = 220) -> List[Dict[str, Any]]:
    if not isinstance(trades, list):
        trades = []
    if not isinstance(cost_ledger, list):
        cost_ledger = []

    # We only use explicit open signals from trade snapshots for score/confidence enrichment.
    signal_candidates = []
    for t in trades:
        if not isinstance(t, dict):
            continue
        d = str(t.get('direction', '')).lower()
        if d in {'long', 'short'}:
            signal_candidates.append(t)

    used_signal_idx: set[int] = set()
    open_map: Dict[str, Dict[str, Any]] = {}
    rows: List[Dict[str, Any]] = []

    ordered_ledger = sorted(
        [x for x in cost_ledger if isinstance(x, dict)],
        key=lambda x: parse_time_to_ms(x.get('ts')) or 0,
    )

    for ev in ordered_ledger:
        action = str(ev.get('action', '')).strip().lower()
        if action not in {'open', 'close', 'partial_close'}:
            continue

        pid = str(ev.get('position_id') or '').strip() or f"unknown-{str(ev.get('id') or '')[:12]}"
        ts = str(ev.get('ts') or '')
        ts_ms = parse_time_to_ms(ts)
        side = str(ev.get('side') or '').strip().lower()

        if action == 'open':
            signal = _pick_open_signal_snapshot(signal_candidates, side, ts_ms, used_signal_idx)
            open_map[pid] = {
                'position_id': pid,
                'open_ts': ts,
                'open_ts_ms': ts_ms,
                'side': side,
                'strategy': str(ev.get('strategy') or signal.get('strategy') or ''),
                'open_price': to_num(ev.get('fill_price') or ev.get('mark_price')),
                'open_size_btc': to_num(ev.get('size_btc')),
                'leverage': to_num(ev.get('leverage'), 1),
                'signal_score': signal.get('score'),
                'signal_confidence': signal.get('confidence'),
                'signal_snapshot': signal if isinstance(signal, dict) else {},
                'open_event': ev if isinstance(ev, dict) else {},
            }
            continue

        # close / partial_close
        base = open_map.get(pid) or {
            'position_id': pid,
            'open_ts': '',
            'open_ts_ms': None,
            'side': side,
            'strategy': str(ev.get('strategy') or ''),
            'open_price': 0.0,
            'open_size_btc': to_num(ev.get('size_btc')),
            'leverage': to_num(ev.get('leverage'), 1),
            'signal_score': None,
            'signal_confidence': None,
            'signal_snapshot': {},
            'open_event': {},
        }

        close_size = to_num(ev.get('size_btc') or base.get('open_size_btc'))
        open_price = to_num(base.get('open_price'))
        close_price = to_num(ev.get('fill_price') or ev.get('mark_price'))
        lev = to_num(base.get('leverage') or ev.get('leverage'), 1)
        strategy = str(base.get('strategy') or ev.get('strategy') or '')
        signal_snapshot = base.get('signal_snapshot') if isinstance(base.get('signal_snapshot'), dict) else {}
        notional = abs(open_price * close_size)
        net_pnl = to_num(ev.get('net_pnl'))
        hold_minutes = None
        if base.get('open_ts_ms') is not None and ts_ms is not None:
            hold_minutes = round(max(0.0, (ts_ms - int(base['open_ts_ms'])) / 60000.0), 2)

        rows.append({
            'position_id': pid,
            'open_ts': str(base.get('open_ts') or ''),
            'close_ts': ts,
            'side': str(base.get('side') or side or ''),
            'strategy': strategy,
            'status': action,
            'close_reason': str(ev.get('reason') or ev.get('event_type') or ''),
            'open_price': round(open_price, 6),
            'close_price': round(close_price, 6),
            'size_btc': round(close_size, 8),
            'leverage': lev,
            'notional': round(notional, 4),
            'gross_pnl': round(to_num(ev.get('gross_pnl')), 4),
            'net_pnl': round(net_pnl, 4),
            'total_cost': round(to_num(ev.get('total_cost')), 4),
            'pnl_pct': round((net_pnl / notional * 100.0) if notional > 0 else 0.0, 4),
            'hold_minutes': hold_minutes,
            'signal_score': base.get('signal_score'),
            'signal_confidence': base.get('signal_confidence'),
            'factor_basis_zh': _entry_factor_basis_zh(signal_snapshot),
            'entry_logic_zh': _entry_logic_zh(strategy, str(base.get('side') or side or ''), base.get('signal_score'), base.get('signal_confidence'), signal_snapshot),
            'position_detail_zh': _position_detail_zh(str(base.get('side') or side or ''), open_price, close_size, lev, notional, base if isinstance(base, dict) else {}, ev if isinstance(ev, dict) else {}),
        })

        if action == 'partial_close':
            remain = max(0.0, to_num(base.get('open_size_btc')) - close_size)
            base['open_size_btc'] = round(remain, 8)
            open_map[pid] = base
        else:
            open_map.pop(pid, None)

    now_ts = now_ms()
    for pid, base in open_map.items():
        open_price = to_num(base.get('open_price'))
        open_size = to_num(base.get('open_size_btc'))
        lev = to_num(base.get('leverage'), 1)
        strategy = str(base.get('strategy') or '')
        signal_snapshot = base.get('signal_snapshot') if isinstance(base.get('signal_snapshot'), dict) else {}
        notional = abs(open_price * open_size)
        hold_minutes = None
        open_ts_ms = base.get('open_ts_ms')
        if open_ts_ms is not None:
            hold_minutes = round(max(0.0, (now_ts - int(open_ts_ms)) / 60000.0), 2)

        rows.append({
            'position_id': pid,
            'open_ts': str(base.get('open_ts') or ''),
            'close_ts': '',
            'side': str(base.get('side') or ''),
            'strategy': str(base.get('strategy') or ''),
            'status': 'open',
            'close_reason': '持仓中',
            'open_price': round(open_price, 6),
            'close_price': 0.0,
            'size_btc': round(open_size, 8),
            'leverage': lev,
            'notional': round(notional, 4),
            'gross_pnl': 0.0,
            'net_pnl': 0.0,
            'total_cost': 0.0,
            'pnl_pct': 0.0,
            'hold_minutes': hold_minutes,
            'signal_score': base.get('signal_score'),
            'signal_confidence': base.get('signal_confidence'),
            'factor_basis_zh': _entry_factor_basis_zh(signal_snapshot),
            'entry_logic_zh': _entry_logic_zh(strategy, str(base.get('side') or ''), base.get('signal_score'), base.get('signal_confidence'), signal_snapshot),
            'position_detail_zh': _position_detail_zh(str(base.get('side') or ''), open_price, open_size, lev, notional, base if isinstance(base, dict) else {}, {}),
        })

    rows.sort(key=lambda x: parse_time_to_ms(x.get('close_ts')) or parse_time_to_ms(x.get('open_ts')) or 0, reverse=True)
    return rows[:keep]


def filter_today_trade_lifecycle(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for x in rows:
        if not isinstance(x, dict):
            continue
        if _is_trade_today(x.get('close_ts')) or _is_trade_today(x.get('open_ts')):
            out.append(x)
    return out




def _parse_user_time_to_ms(raw: Any) -> int | None:
    s = str(raw or '').strip()
    if not s:
        return None
    if s.isdigit():
        try:
            v = int(s)
            return v if v > 10_000_000_000 else v * 1000
        except Exception:
            return None
    s2 = s.replace('Z', '+00:00')
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=TZ_CN)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _bar_to_ms(bar: str) -> int:
    b = str(bar or '1H').strip()
    m = re.match(r'^(\d+)([mHDW])$', b, flags=re.I)
    if not m:
        return 60 * 60 * 1000
    n = int(m.group(1))
    unit = m.group(2).upper()
    if unit == 'M':
        return n * 60 * 1000
    if unit == 'H':
        return n * 60 * 60 * 1000
    if unit == 'D':
        return n * 24 * 60 * 60 * 1000
    if unit == 'W':
        return n * 7 * 24 * 60 * 60 * 1000
    return 60 * 60 * 1000


def _safe_ratio(a: float, b: float) -> float:
    try:
        if b == 0:
            return 0.0
        return a / b
    except Exception:
        return 0.0


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _ema(series: List[float], period: int) -> List[float]:
    out: List[float] = [0.0] * len(series)
    if not series:
        return out
    alpha = 2.0 / (period + 1.0)
    prev = series[0]
    out[0] = prev
    for i in range(1, len(series)):
        prev = alpha * series[i] + (1.0 - alpha) * prev
        out[i] = prev
    return out


def _rsi(series: List[float], period: int = 14) -> List[float]:
    n = len(series)
    out = [50.0] * n
    if n < period + 2:
        return out
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        d = series[i] - series[i - 1]
        gains[i] = max(d, 0.0)
        losses[i] = max(-d, 0.0)
    avg_gain = sum(gains[1:period + 1]) / period
    avg_loss = sum(losses[1:period + 1]) / period
    rs = avg_gain / avg_loss if avg_loss > 0 else 999.0
    out[period] = 100.0 - (100.0 / (1.0 + rs))
    for i in range(period + 1, n):
        avg_gain = ((period - 1) * avg_gain + gains[i]) / period
        avg_loss = ((period - 1) * avg_loss + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 0 else 999.0
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return out


def _rolling_mean_std(series: List[float], window: int) -> tuple[List[float], List[float]]:
    n = len(series)
    means = [0.0] * n
    stds = [0.0] * n
    if n == 0:
        return means, stds
    for i in range(n):
        s = max(0, i - window + 1)
        w = series[s:i + 1]
        if not w:
            continue
        m = sum(w) / len(w)
        means[i] = m
        if len(w) >= 2:
            v = sum((x - m) ** 2 for x in w) / len(w)
            stds[i] = v ** 0.5
    return means, stds


def _rolling_hh_ll(highs: List[float], lows: List[float], window: int) -> tuple[List[float], List[float]]:
    n = len(highs)
    hh = [0.0] * n
    ll = [0.0] * n
    for i in range(n):
        s = max(0, i - window + 1)
        hh[i] = max(highs[s:i + 1])
        ll[i] = min(lows[s:i + 1])
    return hh, ll


def fetch_okx_real_kline(inst_id: str, bar: str, start_ms: int | None, end_ms: int | None, max_bars: int) -> Dict[str, Any]:
    max_bars = max(200, min(max_bars, MAX_BACKTEST_BARS))
    base_url = f'{OKX_BASE}/api/v5/market/history-candles'
    all_rows: List[List[Any]] = []
    seen_ts: set[int] = set()
    after = str(end_ms) if end_ms else str(now_ms())
    stop = False

    max_rounds = max(80, min(420, int(max_bars / 80) + 30))
    for _ in range(max_rounds):
        left = max_bars - len(all_rows)
        if left <= 0 or stop:
            break
        params = {
            'instId': inst_id,
            'bar': bar,
            'limit': str(min(100, left)),
        }
        if after:
            params['after'] = after
        try:
            r = requests.get(base_url, params=params, timeout=OKX_PUBLIC_TIMEOUT)
            if r.status_code != 200:
                return {'ok': False, 'error': f'kline_http_{r.status_code}', 'detail': (r.text or '')[:220]}
            obj = r.json()
        except Exception as e:
            return {'ok': False, 'error': f'kline_request_failed: {e}'}

        arr = (obj or {}).get('data') or []
        if not isinstance(arr, list) or not arr:
            break

        oldest_ts = None
        for row in arr:
            if not isinstance(row, list) or len(row) < 6:
                continue
            ts = int(to_num(row[0], 0))
            if ts <= 0:
                continue
            if end_ms and ts > end_ms:
                continue
            if start_ms and ts < start_ms:
                stop = True
                continue
            if ts in seen_ts:
                continue
            seen_ts.add(ts)
            all_rows.append(row)
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts
        if oldest_ts is None:
            break
        after = str(oldest_ts)
        if start_ms and oldest_ts <= start_ms:
            break
        time.sleep(0.04)

    if not all_rows:
        return {'ok': False, 'error': 'empty_kline_data'}

    candles: List[Dict[str, Any]] = []
    for row in all_rows:
        try:
            ts = int(to_num(row[0], 0))
            o = to_num(row[1], 0.0)
            h = to_num(row[2], 0.0)
            l = to_num(row[3], 0.0)
            c = to_num(row[4], 0.0)
            v = to_num(row[5], 0.0)
            if ts <= 0 or c <= 0:
                continue
            candles.append({'ts': ts, 'open': o, 'high': h, 'low': l, 'close': c, 'volume': v})
        except Exception:
            continue

    candles.sort(key=lambda x: x['ts'])
    if len(candles) > max_bars:
        candles = candles[-max_bars:]

    return {
        'ok': True,
        'source': {
            'exchange': 'OKX',
            'endpoint': '/api/v5/market/history-candles',
            'instId': inst_id,
            'bar': bar,
        },
        'candles': candles,
    }


def _downsample_points(points: List[Dict[str, Any]], keep: int = 360) -> List[Dict[str, Any]]:
    n = len(points)
    if n <= keep:
        return points
    step = max(1, n // keep)
    out = [points[i] for i in range(0, n, step)]
    if out and out[-1] != points[-1]:
        out.append(points[-1])
    return out[-keep:]


def _equity_backtest(candles: List[Dict[str, Any]], signal: List[int], leverage: float, fee_bps: float, slippage_bps: float, dd_window: int) -> Dict[str, Any]:
    n = min(len(candles), len(signal))
    if n < 20:
        return {'ok': False, 'error': 'insufficient_bars'}

    closes = [to_num(c['close'], 0.0) for c in candles[:n]]
    times = [int(to_num(c['ts'], 0)) for c in candles[:n]]
    fee_rate = max(0.0, fee_bps) / 10000.0
    slip_rate = max(0.0, slippage_bps) / 10000.0
    unit_cost = fee_rate + slip_rate

    eq = 1.0
    pos = 0
    peak = 1.0
    peak_i = 0
    max_dd = 0.0
    dd_start_i = 0
    dd_trough_i = 0
    curve: List[Dict[str, Any]] = []

    trades = 0
    wins = 0
    losses = 0
    trade_entry_eq = None
    trade_entry_i = None
    gross_profit = 0.0
    gross_loss = 0.0
    sum_win_ret = 0.0
    sum_loss_ret = 0.0

    total_fee = 0.0
    total_slip = 0.0

    for i in range(1, n):
        target = int(signal[i - 1])
        target = 1 if target > 0 else (-1 if target < 0 else 0)

        if target != pos:
            turnover = abs(target - pos)
            if turnover > 0:
                c_rate = turnover * unit_cost
                eq *= max(0.000001, 1.0 - c_rate)
                total_fee += turnover * fee_rate
                total_slip += turnover * slip_rate

            # close previous trade
            if pos != 0 and trade_entry_eq is not None:
                tr = _safe_ratio(eq, trade_entry_eq) - 1.0
                trades += 1
                if tr > 0:
                    wins += 1
                    gross_profit += tr
                    sum_win_ret += tr
                elif tr < 0:
                    losses += 1
                    gross_loss += abs(tr)
                    sum_loss_ret += abs(tr)
                trade_entry_eq = None
                trade_entry_i = None

            # open new trade
            if target != 0:
                trade_entry_eq = eq
                trade_entry_i = i

            pos = target

        prev = closes[i - 1]
        cur = closes[i]
        bar_ret = _safe_ratio(cur - prev, prev)
        eq *= max(0.000001, 1.0 + pos * leverage * bar_ret)

        if eq > peak:
            peak = eq
            peak_i = i
        dd = _safe_ratio(peak - eq, peak)
        if dd > max_dd:
            max_dd = dd
            dd_start_i = peak_i
            dd_trough_i = i

        curve.append({'ts': times[i], 'equity': eq, 'drawdown_pct': dd * 100.0})

    if pos != 0 and trade_entry_eq is not None:
        tr = _safe_ratio(eq, trade_entry_eq) - 1.0
        trades += 1
        if tr > 0:
            wins += 1
            gross_profit += tr
            sum_win_ret += tr
        elif tr < 0:
            losses += 1
            gross_loss += abs(tr)
            sum_loss_ret += abs(tr)

    # rolling drawdown window
    w = max(5, int(dd_window or 96))
    worst_w = 0.0
    latest_w = 0.0
    if curve:
        eqs = [x['equity'] for x in curve]
        for i in range(len(eqs)):
            s = max(0, i - w + 1)
            local_peak = max(eqs[s:i + 1])
            local_dd = _safe_ratio(local_peak - eqs[i], local_peak)
            if local_dd > worst_w:
                worst_w = local_dd
            latest_w = local_dd

    total_return = (eq - 1.0) * 100.0
    span_days = max((times[-1] - times[0]) / 86400000.0, 1.0) if times else 1.0
    annualized_return_pct = (((eq / 1.0) ** (365.0 / span_days)) - 1.0) * 100.0 if eq > 0 else -100.0
    win_rate = (_safe_ratio(wins, trades) * 100.0) if trades > 0 else 0.0
    avg_win_pct = ((sum_win_ret / wins) * 100.0) if wins > 0 else 0.0
    avg_loss_pct = ((sum_loss_ret / losses) * 100.0) if losses > 0 else 0.0
    reward_risk_ratio = (_safe_ratio(avg_win_pct, avg_loss_pct)) if avg_loss_pct > 1e-9 else None
    profit_factor = (_safe_ratio(gross_profit, gross_loss)) if gross_loss > 1e-12 else None
    expectancy_pct = (win_rate / 100.0) * avg_win_pct - (1.0 - win_rate / 100.0) * avg_loss_pct

    # bar-level sharpe-like
    bar_rets: List[float] = []
    for i in range(1, len(curve)):
        a = to_num(curve[i - 1].get('equity'), 1.0)
        b = to_num(curve[i].get('equity'), a)
        bar_rets.append(_safe_ratio(b - a, a))
    sharpe_like = 0.0
    if len(bar_rets) >= 8:
        sd = statistics.pstdev(bar_rets)
        if sd > 1e-9:
            sharpe_like = (statistics.mean(bar_rets) / sd) * math.sqrt(len(bar_rets))
    calmar_like = _safe_ratio(annualized_return_pct, max_dd * 100.0) if max_dd > 1e-9 else None

    return {
        'ok': True,
        'initial_equity': 1.0,
        'final_equity': round(eq, 6),
        'return_pct': round(total_return, 4),
        'annualized_return_pct': round(annualized_return_pct, 4),
        'max_drawdown_pct': round(max_dd * 100.0, 4),
        'rolling_drawdown': {
            'window_bars': w,
            'latest_pct': round(latest_w * 100.0, 4),
            'worst_pct': round(worst_w * 100.0, 4),
        },
        'drawdown_period': {
            'peak_ts': fmt_ms(times[dd_start_i]) if dd_start_i < len(times) else '-',
            'trough_ts': fmt_ms(times[dd_trough_i]) if dd_trough_i < len(times) else '-',
        },
        'trades': int(trades),
        'wins': int(wins),
        'losses': int(losses),
        'win_rate_pct': round(win_rate, 2),
        'avg_win_pct': round(avg_win_pct, 4),
        'avg_loss_pct': round(avg_loss_pct, 4),
        'reward_risk_ratio': round(reward_risk_ratio, 4) if reward_risk_ratio is not None else None,
        'profit_factor': round(profit_factor, 4) if profit_factor is not None else None,
        'expectancy_pct': round(expectancy_pct, 4),
        'sharpe_like': round(sharpe_like, 4),
        'calmar_like': round(calmar_like, 4) if calmar_like is not None else None,
        'costs': {
            'fee_pct_equity': round(total_fee * 100.0, 4),
            'slippage_pct_equity': round(total_slip * 100.0, 4),
            'total_pct_equity': round((total_fee + total_slip) * 100.0, 4),
        },
        'curve': _downsample_points(curve, 360),
    }


def _full_equity_curve(candles: List[Dict[str, Any]], signal: List[int], leverage: float, fee_bps: float, slippage_bps: float) -> List[Dict[str, Any]]:
    n = min(len(candles), len(signal))
    if n < 2:
        return []
    closes = [to_num(c.get('close'), 0.0) for c in candles[:n]]
    times = [int(to_num(c.get('ts'), 0)) for c in candles[:n]]
    fee_rate = max(0.0, fee_bps) / 10000.0
    slip_rate = max(0.0, slippage_bps) / 10000.0
    unit_cost = fee_rate + slip_rate
    eq = 1.0
    pos = 0
    out: List[Dict[str, Any]] = []
    for i in range(1, n):
        target = int(signal[i - 1])
        target = 1 if target > 0 else (-1 if target < 0 else 0)
        if target != pos:
            turnover = abs(target - pos)
            if turnover > 0:
                eq *= max(0.000001, 1.0 - turnover * unit_cost)
            pos = target
        prev = closes[i - 1]
        cur = closes[i]
        bar_ret = _safe_ratio(cur - prev, prev)
        eq *= max(0.000001, 1.0 + pos * leverage * bar_ret)
        out.append({'ts': times[i], 'equity': eq})
    return out


def _virtual_trade_rows_from_signal(
    candles: List[Dict[str, Any]],
    signal: List[int],
    leverage: float,
    fee_bps: float,
    slippage_bps: float,
    bar_ms: int,
    strategy_name: str = 'liquidation_reversal_v1',
    factor_series: Dict[str, Any] | None = None,
    liq_event_series: List[float] | None = None,
    keep: int = 120,
) -> List[Dict[str, Any]]:
    n = min(len(candles), len(signal))
    if n < 2:
        return []
    closes = [to_num(c.get('close'), 0.0) for c in candles[:n]]
    times = [int(to_num(c.get('ts'), 0)) for c in candles[:n]]
    fee_rate = max(0.0, fee_bps) / 10000.0
    slip_rate = max(0.0, slippage_bps) / 10000.0
    unit_cost = fee_rate + slip_rate

    fac = factor_series if isinstance(factor_series, dict) else {}

    def _arr(key: str, default: float = 0.0) -> List[float]:
        arr = list(fac.get(key) or [])
        if len(arr) < n:
            arr.extend([arr[-1] if arr else default] * (n - len(arr)))
        elif len(arr) > n:
            arr = arr[:n]
        return [to_num(v, default) for v in arr]

    liq = _arr('liquidation_pressure', 0.0)
    flow = _arr('order_flow_persistence', 0.0)
    taker = _arr('taker_imbalance', 0.0)
    trend = _arr('trend_ema', 0.0)
    evt = list(liq_event_series or [])
    if len(evt) < n:
        evt.extend([0.0] * (n - len(evt)))
    elif len(evt) > n:
        evt = evt[:n]
    est_cost_pct = (2.0 * unit_cost) * 100.0
    lev = max(0.2, to_num(leverage, 1.0))

    def _entry_logic(entry_idx: int, side_sign: int) -> tuple[str, str, str]:
        side = 'long' if side_sign > 0 else 'short'
        lp = to_num(liq[entry_idx] if entry_idx < len(liq) else 0.0, 0.0)
        fl = to_num(flow[entry_idx] if entry_idx < len(flow) else 0.0, 0.0)
        tk = to_num(taker[entry_idx] if entry_idx < len(taker) else 0.0, 0.0)
        tr = to_num(trend[entry_idx] if entry_idx < len(trend) else 0.0, 0.0)
        evv = to_num(evt[entry_idx] if entry_idx < len(evt) else 0.0, 0.0)
        factor_basis = (
            f"{_factor_key_zh('liquidation_pressure')}({lp:+.2f})、"
            f"{_factor_key_zh('order_flow_persistence')}({fl:+.2f})、"
            f"{_factor_key_zh('taker_imbalance')}({tk:+.2f})"
        )
        if side_sign > 0 and evv > 0:
            core = '价格下砸触发多头清算，执行反向做多'
        elif side_sign < 0 and evv < 0:
            core = '价格上冲触发空头清算，执行反向做空'
        elif abs(lp) >= 0.36:
            core = '清算压力达到阈值，执行反向开仓'
        else:
            core = '清算反向信号触发开仓'
        entry_logic = f"策略:{strategy_name}；方向:{_side_zh(side)}；主因子:{factor_basis}；触发:{core}"
        pos_detail = (
            f"信号仓位(回测虚拟, 非真实下单)；杠杆{_fmt_trade_num(lev, 2)}x；"
            f"入场价{_fmt_trade_num(closes[entry_idx], 2)}；"
            f"趋势{tr:+.2f}；估算单笔成本{_fmt_trade_num(est_cost_pct, 4)}%"
        )
        return entry_logic, factor_basis, pos_detail

    rows: List[Dict[str, Any]] = []
    pos = 0
    entry_i: int | None = None
    entry_px = 0.0

    for i in range(1, n):
        target = int(signal[i - 1])
        target = 1 if target > 0 else (-1 if target < 0 else 0)
        if target != pos:
            if pos != 0 and entry_i is not None:
                close_px = closes[i]
                open_px = entry_px if entry_px > 1e-9 else close_px
                move = _safe_ratio(close_px - open_px, open_px)
                gross_pct = (move * lev * 100.0) if pos > 0 else (-move * lev * 100.0)
                net_pct = gross_pct - est_cost_pct
                hold_minutes = round(max(0.0, (times[i] - times[entry_i]) / 60000.0), 2)
                close_reason = '反向信号翻仓' if target == -pos else '信号回落平仓'
                entry_logic, factor_basis, pos_detail = _entry_logic(entry_i, pos)
                rows.append({
                    'open_ts': fmt_ms(times[entry_i]),
                    'close_ts': fmt_ms(times[i]),
                    'strategy': strategy_name,
                    'side': 'long' if pos > 0 else 'short',
                    'status': 'close',
                    'open_price': round(open_px, 6),
                    'close_price': round(close_px, 6),
                    'leverage': lev,
                    'hold_minutes': hold_minutes,
                    'close_reason': close_reason,
                    'entry_logic_zh': entry_logic,
                    'factor_basis_zh': factor_basis,
                    'position_detail_zh': pos_detail,
                    'gross_pnl_pct': round(gross_pct, 4),
                    'net_pnl_pct': round(net_pct, 4),
                    'estimated_cost_pct': round(est_cost_pct, 4),
                })
            if target != 0:
                entry_i = i
                entry_px = closes[i]
            else:
                entry_i = None
                entry_px = 0.0
            pos = target

    if pos != 0 and entry_i is not None:
        latest_i = n - 1
        close_px = closes[latest_i]
        open_px = entry_px if entry_px > 1e-9 else close_px
        move = _safe_ratio(close_px - open_px, open_px)
        gross_pct = (move * lev * 100.0) if pos > 0 else (-move * lev * 100.0)
        net_pct = gross_pct - est_cost_pct
        hold_minutes = round(max(0.0, (times[latest_i] - times[entry_i]) / 60000.0), 2)
        entry_logic, factor_basis, pos_detail = _entry_logic(entry_i, pos)
        rows.append({
            'open_ts': fmt_ms(times[entry_i]),
            'close_ts': '',
            'strategy': strategy_name,
            'side': 'long' if pos > 0 else 'short',
            'status': 'open',
            'open_price': round(open_px, 6),
            'close_price': round(close_px, 6),
            'leverage': lev,
            'hold_minutes': hold_minutes,
            'close_reason': '持仓中(回测信号)',
            'entry_logic_zh': entry_logic,
            'factor_basis_zh': factor_basis,
            'position_detail_zh': pos_detail,
            'gross_pnl_pct': round(gross_pct, 4),
            'net_pnl_pct': round(net_pct, 4),
            'estimated_cost_pct': round(est_cost_pct, 4),
        })

    rows.sort(key=lambda x: parse_time_to_ms(x.get('close_ts')) or parse_time_to_ms(x.get('open_ts')) or 0, reverse=True)
    return rows[:keep]


def _factor_series_from_kline(candles: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [to_num(x.get('close'), 0.0) for x in candles]
    highs = [to_num(x.get('high'), 0.0) for x in candles]
    lows = [to_num(x.get('low'), 0.0) for x in candles]
    vols = [to_num(x.get('volume'), 0.0) for x in candles]
    n = len(candles)

    ema12 = _ema(closes, 12)
    ema20 = _ema(closes, 20)
    ema26 = _ema(closes, 26)
    ema50 = _ema(closes, 50)
    ema100 = _ema(closes, 100)
    macd = [ema12[i] - ema26[i] for i in range(n)]
    macd_sig = _ema(macd, 9)
    rsi14 = _rsi(closes, 14)
    hh20, ll20 = _rolling_hh_ll(highs, lows, 20)
    vol_ma20, vol_std20 = _rolling_mean_std(vols, 20)

    momentum = [0.0] * n
    trend = [0.0] * n
    mean_rev = [0.0] * n
    breakout = [0.0] * n
    volume_flow = [0.0] * n
    volatility_regime = [0.0] * n
    asr_vc = [0.0] * n
    enhanced_technical = [0.0] * n

    for i in range(n):
        c = closes[i]
        if c <= 0:
            continue

        # 1) momentum (6/24 bars)
        r6 = _safe_ratio(c - closes[max(0, i - 6)], closes[max(0, i - 6)])
        r24 = _safe_ratio(c - closes[max(0, i - 24)], closes[max(0, i - 24)])
        momentum[i] = _clamp((r6 * 0.65 + r24 * 0.35) * 42.0, -1.0, 1.0)

        # 2) trend EMA
        trend[i] = _clamp(_safe_ratio(ema20[i] - ema50[i], c) * 180.0, -1.0, 1.0)

        # 3) mean reversion by RSI
        rsi = rsi14[i]
        if rsi < 50:
            mean_rev[i] = _clamp((50.0 - rsi) / 22.0, -1.0, 1.0)
        else:
            mean_rev[i] = _clamp(-(rsi - 50.0) / 22.0, -1.0, 1.0)

        # 4) breakout (20-bar channel)
        up = hh20[i]
        dn = ll20[i]
        if up > 0 and c > up * 1.0005:
            breakout[i] = _clamp((c / up - 1.0) * 220.0, 0.0, 1.0)
        elif dn > 0 and c < dn * 0.9995:
            breakout[i] = -_clamp((dn / c - 1.0) * 220.0, 0.0, 1.0)
        else:
            breakout[i] = 0.0

        # 5) volume-price flow
        vstd = vol_std20[i]
        vz = _safe_ratio(vols[i] - vol_ma20[i], vstd if vstd > 1e-9 else 1.0)
        r1 = _safe_ratio(c - closes[max(0, i - 1)], closes[max(0, i - 1)])
        volume_flow[i] = _clamp((1.0 if r1 >= 0 else -1.0) * _clamp(vz / 3.0, 0.0, 1.0), -1.0, 1.0)

        # 6) volatility regime (directional volatility)
        tr = _safe_ratio(highs[i] - lows[i], c)
        dir_sign = 1.0 if (ema20[i] >= ema50[i]) else -1.0
        volatility_regime[i] = _clamp(dir_sign * tr * 25.0, -1.0, 1.0)

        # 7) ASR-VC proxy (support/resistance location + volume confirmation)
        rng = max(1e-9, up - dn)
        pos = _safe_ratio(c - dn, rng)  # 0 near support, 1 near resistance
        asr_bias = (0.5 - pos) * 2.0
        vol_confirm = _clamp(vz / 2.5, -1.0, 1.0)
        asr_vc[i] = _clamp(asr_bias * 0.7 + vol_confirm * 0.3, -1.0, 1.0)

        # 8) enhanced technical (MACD + EMA stack + RSI)
        macd_part = _clamp((macd[i] - macd_sig[i]) / max(c * 0.0012, 1e-9), -1.0, 1.0)
        ema_part = _clamp(_safe_ratio(ema20[i] - ema100[i], c) * 260.0, -1.0, 1.0)
        rsi_part = _clamp((50.0 - rsi) / 25.0, -1.0, 1.0)
        enhanced_technical[i] = _clamp(macd_part * 0.45 + ema_part * 0.35 + rsi_part * 0.20, -1.0, 1.0)

    return {
        'momentum': momentum,
        'trend_ema': trend,
        'rsi_reversion': mean_rev,
        'breakout_20': breakout,
        'volume_flow': volume_flow,
        'volatility_regime': volatility_regime,
        'asr_vc': asr_vc,
        'enhanced_technical': enhanced_technical,
    }


def _rubik_period_from_bar(bar: str) -> str:
    ms = _bar_to_ms(bar)
    if ms <= 15 * 60 * 1000:
        return '5m'
    if ms <= 4 * 60 * 60 * 1000:
        return '1H'
    return '1D'


def _fetch_okx_rubik_rows(endpoint: str, base_params: Dict[str, Any], start_ms: int | None, end_ms: int | None, max_points: int = 3000) -> List[List[Any]]:
    out: List[List[Any]] = []
    seen: set[int] = set()
    stop = False
    end_cursor = int(end_ms or now_ms())

    for _ in range(48):
        left = max_points - len(out)
        if left <= 0 or stop:
            break
        params = dict(base_params)
        params['limit'] = str(min(100, left))
        params['end'] = str(end_cursor)
        try:
            r = requests.get(f'{OKX_BASE}{endpoint}', params=params, timeout=OKX_PUBLIC_TIMEOUT)
            if r.status_code != 200:
                break
            obj = r.json()
            arr = (obj or {}).get('data') or []
            if not isinstance(arr, list) or not arr:
                break
        except Exception:
            break

        min_ts = None
        for row in arr:
            if not isinstance(row, list) or not row:
                continue
            ts = int(to_num(row[0], 0))
            if ts <= 0:
                continue
            if end_ms and ts > end_ms:
                continue
            if start_ms and ts < start_ms:
                stop = True
                continue
            if ts in seen:
                continue
            seen.add(ts)
            out.append(row)
            if min_ts is None or ts < min_ts:
                min_ts = ts
        if min_ts is None:
            break
        end_cursor = int(min_ts - 1)
        if start_ms and min_ts <= start_ms:
            break
        time.sleep(0.03)

    out.sort(key=lambda x: int(to_num(x[0], 0)))
    return out


def _fetch_okx_funding_rows(inst_id: str, start_ms: int | None, end_ms: int | None, max_points: int = 1200) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[int] = set()
    stop = False
    after_cursor: int | None = None

    for _ in range(40):
        left = max_points - len(out)
        if left <= 0 or stop:
            break
        params: Dict[str, Any] = {'instId': inst_id, 'limit': str(min(100, left))}
        if after_cursor:
            params['after'] = str(after_cursor)
        try:
            r = requests.get(f'{OKX_BASE}/api/v5/public/funding-rate-history', params=params, timeout=OKX_PUBLIC_TIMEOUT)
            if r.status_code != 200:
                break
            obj = r.json()
            arr = (obj or {}).get('data') or []
            if not isinstance(arr, list) or not arr:
                break
        except Exception:
            break

        min_ts = None
        for item in arr:
            if not isinstance(item, dict):
                continue
            ts = int(to_num(item.get('fundingTime'), 0))
            if ts <= 0:
                continue
            if end_ms and ts > end_ms:
                continue
            if start_ms and ts < start_ms:
                stop = True
                continue
            if ts in seen:
                continue
            seen.add(ts)
            out.append({'ts': ts, 'rate': to_num(item.get('realizedRate') or item.get('fundingRate'), 0.0)})
            if min_ts is None or ts < min_ts:
                min_ts = ts
        if min_ts is None:
            break
        after_cursor = int(min_ts)
        if start_ms and min_ts <= start_ms:
            break
        time.sleep(0.03)

    out.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
    return out


def _align_scalar_series(candles: List[Dict[str, Any]], points: List[Dict[str, Any]], key: str, default: float = 0.0) -> List[float]:
    out: List[float] = []
    if not candles:
        return out
    if not points:
        return [default for _ in candles]
    j = 0
    cur = default
    for c in candles:
        ts = int(to_num(c.get('ts'), 0))
        while j < len(points) and int(to_num(points[j].get('ts'), 0)) <= ts:
            cur = to_num(points[j].get(key), cur)
            j += 1
        out.append(cur)
    return out


def _align_taker_series(candles: List[Dict[str, Any]], points: List[Dict[str, Any]]) -> tuple[List[float], List[float]]:
    buy: List[float] = []
    sell: List[float] = []
    if not candles:
        return buy, sell
    if not points:
        return ([0.0 for _ in candles], [0.0 for _ in candles])
    j = 0
    cur_buy = 0.0
    cur_sell = 0.0
    for c in candles:
        ts = int(to_num(c.get('ts'), 0))
        while j < len(points) and int(to_num(points[j].get('ts'), 0)) <= ts:
            cur_buy = to_num(points[j].get('buy'), cur_buy)
            cur_sell = to_num(points[j].get('sell'), cur_sell)
            j += 1
        buy.append(cur_buy)
        sell.append(cur_sell)
    return buy, sell



def _corr_coef(xs: List[float], ys: List[float]) -> float:
    n = min(len(xs), len(ys))
    if n <= 2:
        return 0.0
    mx = sum(xs[:n]) / n
    my = sum(ys[:n]) / n
    cov = 0.0
    vx = 0.0
    vy = 0.0
    for i in range(n):
        dx = xs[i] - mx
        dy = ys[i] - my
        cov += dx * dy
        vx += dx * dx
        vy += dy * dy
    den = math.sqrt(max(vx * vy, 0.0))
    if den <= 1e-12:
        return 0.0
    return _clamp(cov / den, -1.0, 1.0)


def _extract_close_points_from_df(df: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if df is None:
        return out
    try:
        if getattr(df, 'empty', True):
            return out
    except Exception:
        return out

    ser = None
    try:
        cols = getattr(df, 'columns', [])
        if hasattr(cols, 'levels'):
            # yfinance often returns MultiIndex columns: (Price, Ticker)
            try:
                tmp = df.xs('Close', axis=1, level=0)
                # tmp may be DataFrame if multiple tickers; pick first column
                if hasattr(tmp, 'columns'):
                    if len(tmp.columns) >= 1:
                        ser = tmp[tmp.columns[0]]
                else:
                    ser = tmp
            except Exception:
                ser = None
            if ser is None:
                for c in list(cols):
                    name = '/'.join([str(x) for x in c]) if isinstance(c, tuple) else str(c)
                    if 'close' in name.lower() and 'adj' not in name.lower():
                        ser = df[c]
                        break
                if ser is None:
                    for c in list(cols):
                        name = '/'.join([str(x) for x in c]) if isinstance(c, tuple) else str(c)
                        if 'close' in name.lower():
                            ser = df[c]
                            break
        elif 'Close' in cols:
            ser = df['Close']
    except Exception:
        ser = None

    if ser is None:
        return out

    try:
        ser = ser.dropna()
    except Exception:
        pass

    try:
        it = ser.items()
    except Exception:
        return out

    for idx, v in it:
        c = to_num(v, 0.0)
        if c <= 0:
            continue
        try:
            dt = idx.to_pydatetime()
        except Exception:
            try:
                dt = datetime.strptime(str(idx)[:10], '%Y-%m-%d')
            except Exception:
                continue
        dt_utc = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
        ts = int(dt_utc.timestamp() * 1000) + 86399999
        out.append({'ts': ts, 'close': c})

    out.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
    return out


def _fetch_yf_daily_close(ticker: str, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    try:
        import yfinance as yf
    except Exception:
        return []
    try:
        df = yf.download(
            ticker,
            start=start_dt.strftime('%Y-%m-%d'),
            end=end_dt.strftime('%Y-%m-%d'),
            interval='1d',
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return _extract_close_points_from_df(df)
    except Exception:
        return []


def _compute_cross_market_daily_points(start_ms: int, end_ms: int, roll_days: int = 30) -> List[Dict[str, Any]]:
    if end_ms <= start_ms:
        return []

    start_dt = datetime.fromtimestamp(max(0, start_ms) / 1000.0, timezone.utc)
    end_dt = datetime.fromtimestamp(max(0, end_ms) / 1000.0, timezone.utc) + timedelta(days=2)

    btc_pts = _fetch_yf_daily_close('BTC-USD', start_dt, end_dt)
    spy_pts = _fetch_yf_daily_close('SPY', start_dt, end_dt)
    if not btc_pts or not spy_pts:
        return []

    def _dkey(ts: int) -> str:
        return datetime.fromtimestamp(ts / 1000.0, timezone.utc).strftime('%Y-%m-%d')

    btc_map = {_dkey(int(to_num(x.get('ts'), 0))): to_num(x.get('close'), 0.0) for x in btc_pts}
    spy_map = {_dkey(int(to_num(x.get('ts'), 0))): to_num(x.get('close'), 0.0) for x in spy_pts}

    dates = sorted([d for d in btc_map.keys() if d in spy_map])
    if len(dates) < max(roll_days + 2, 12):
        return []

    btc_close = [to_num(btc_map.get(d), 0.0) for d in dates]
    spy_close = [to_num(spy_map.get(d), 0.0) for d in dates]

    btc_ret = [0.0]
    spy_ret = [0.0]
    for i in range(1, len(dates)):
        b0 = btc_close[i - 1]
        s0 = spy_close[i - 1]
        btc_ret.append(_safe_ratio(btc_close[i] - b0, b0 if b0 > 1e-12 else 1.0))
        spy_ret.append(_safe_ratio(spy_close[i] - s0, s0 if s0 > 1e-12 else 1.0))

    out: List[Dict[str, Any]] = []
    for i, ds in enumerate(dates):
        try:
            dtu = datetime.strptime(ds, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            ts = int(dtu.timestamp() * 1000) + 86399999
        except Exception:
            continue

        corr = 0.0
        if i >= roll_days:
            j0 = i - roll_days + 1
            xw = btc_ret[j0:i + 1]
            yw = spy_ret[j0:i + 1]
            if len(xw) >= 8 and len(yw) >= 8:
                corr = _corr_coef(xw, yw)

        spy_chg = 0.0
        if i >= 5 and spy_close[i - 5] > 0:
            spy_chg = (spy_close[i] - spy_close[i - 5]) / spy_close[i - 5] * 100.0

        score = 50.0
        if abs(corr) > 0.6:
            if spy_chg > 2:
                score += min(15.0, spy_chg * 3.0)
            elif spy_chg < -2:
                score -= min(15.0, abs(spy_chg) * 3.0)
        else:
            if spy_chg > 3:
                score += 5.0
            elif spy_chg < -3:
                score -= 5.0

        score = _clamp(score, 0.0, 100.0)
        factor = _clamp((score - 50.0) / 25.0, -1.0, 1.0)
        confidence = 20.0 if i < roll_days else (30.0 + min(30.0, abs(corr) * 40.0))

        out.append({
            'ts': ts,
            'btc_close': round(to_num(btc_close[i], 0.0), 6),
            'spy_close': round(to_num(spy_close[i], 0.0), 6),
            'corr': round(corr, 6),
            'spy_5d_change': round(spy_chg, 4),
            'score': round(score, 4),
            'factor': round(factor, 6),
            'confidence': round(confidence, 2),
            'source': 'real',
        })

    out = [p for p in out if int(to_num(p.get('ts'), 0)) >= int(start_ms) and int(to_num(p.get('ts'), 0)) <= int(end_ms + 86400000)]
    return out


def _load_cross_market_cache_points() -> List[Dict[str, Any]]:
    raw = read_json(CROSS_MARKET_CACHE_FILE, {})
    pts = (raw or {}).get('points') if isinstance(raw, dict) else []
    if not isinstance(pts, list):
        return []
    out: List[Dict[str, Any]] = []
    for p in pts:
        if not isinstance(p, dict):
            continue
        ts = int(to_num(p.get('ts'), 0))
        if ts <= 0:
            continue
        out.append({
            'ts': ts,
            'btc_close': to_num(p.get('btc_close'), 0.0),
            'spy_close': to_num(p.get('spy_close'), 0.0),
            'corr': to_num(p.get('corr'), 0.0),
            'spy_5d_change': to_num(p.get('spy_5d_change'), 0.0),
            'score': to_num(p.get('score'), 50.0),
            'factor': to_num(p.get('factor'), 0.0),
            'confidence': to_num(p.get('confidence'), 20.0),
            'source': str(p.get('source') or 'cache'),
        })
    out.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
    return out


def _save_cross_market_cache_points(points: List[Dict[str, Any]]) -> None:
    if not isinstance(points, list):
        return
    clean = [x for x in points if isinstance(x, dict) and int(to_num(x.get('ts'), 0)) > 0]
    clean.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
    if len(clean) > CROSS_MARKET_CACHE_MAX_POINTS:
        clean = clean[-CROSS_MARKET_CACHE_MAX_POINTS:]
    write_json(CROSS_MARKET_CACHE_FILE, {'updated_at': now_ms(), 'points': clean})


def _get_cross_market_points(start_ms: int, end_ms: int) -> Dict[str, Any]:
    warm_ms = CROSS_MARKET_WARM_DAYS * 86400000
    req_start = max(0, int(start_ms) - warm_ms)
    req_end = int(end_ms)

    cached = _load_cross_market_cache_points()
    need_fetch = True
    if cached:
        c0 = int(to_num(cached[0].get('ts'), 0))
        c1 = int(to_num(cached[-1].get('ts'), 0))
        if c0 <= req_start + 86400000 and c1 >= req_end - 86400000:
            need_fetch = False

    fetched = []
    if need_fetch:
        fetched = _compute_cross_market_daily_points(req_start, req_end + 86400000, CROSS_MARKET_ROLL_DAYS)
        if fetched:
            m: Dict[int, Dict[str, Any]] = {}
            for p in cached:
                m[int(to_num(p.get('ts'), 0))] = p
            for p in fetched:
                m[int(to_num(p.get('ts'), 0))] = p
            cached = list(m.values())
            cached.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
            _save_cross_market_cache_points(cached)

    points = [p for p in cached if int(to_num(p.get('ts'), 0)) >= req_start and int(to_num(p.get('ts'), 0)) <= req_end + 86400000]
    meta = {
        'cross_market_points': len(points),
        'cross_market_cache_total': len(cached),
        'cross_market_roll_days': CROSS_MARKET_ROLL_DAYS,
        'cross_market_warm_days': CROSS_MARKET_WARM_DAYS,
        'cross_market_fetched': len(fetched),
    }
    return {'points': points, 'meta': meta}



def _macro_risk_level(vix: float) -> str:
    if vix > 30:
        return 'high_panic'
    if vix > 25:
        return 'elevated'
    if vix > 20:
        return 'moderate'
    if vix > 15:
        return 'low'
    return 'complacent'


def _compute_macro_risk_daily_points(start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    if end_ms <= start_ms:
        return []

    start_dt = datetime.fromtimestamp(max(0, start_ms) / 1000.0, timezone.utc)
    end_dt = datetime.fromtimestamp(max(0, end_ms) / 1000.0, timezone.utc) + timedelta(days=2)

    vix_pts = _fetch_yf_daily_close('^VIX', start_dt, end_dt)
    dxy_pts = _fetch_yf_daily_close('DX-Y.NYB', start_dt, end_dt)
    if not vix_pts or not dxy_pts:
        return []

    def _dkey(ts: int) -> str:
        return datetime.fromtimestamp(ts / 1000.0, timezone.utc).strftime('%Y-%m-%d')

    vix_map = {_dkey(int(to_num(x.get('ts'), 0))): to_num(x.get('close'), 0.0) for x in vix_pts}
    dxy_map = {_dkey(int(to_num(x.get('ts'), 0))): to_num(x.get('close'), 0.0) for x in dxy_pts}
    dates = sorted([d for d in dxy_map.keys() if d in vix_map])
    if len(dates) < 8:
        return []

    vix_close = [to_num(vix_map.get(d), 0.0) for d in dates]
    dxy_close = [to_num(dxy_map.get(d), 0.0) for d in dates]

    out: List[Dict[str, Any]] = []
    for i, ds in enumerate(dates):
        try:
            dtu = datetime.strptime(ds, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            ts = int(dtu.timestamp() * 1000) + 86399999
        except Exception:
            continue

        vix = to_num(vix_close[i], 20.0)
        dxy_chg = 0.0
        if i >= 5 and dxy_close[i - 5] > 0:
            dxy_chg = (dxy_close[i] - dxy_close[i - 5]) / dxy_close[i - 5] * 100.0

        score = 50.0
        # same logic as stock_market_pro_factors.compute_macro_risk_factor
        if vix < 15:
            score += 10.0
        elif vix < 20:
            score += 5.0
        elif vix > 30:
            score -= 12.0
        elif vix > 25:
            score -= 6.0

        if dxy_chg > 1:
            score -= 8.0
        elif dxy_chg > 0.5:
            score -= 4.0
        elif dxy_chg < -1:
            score += 8.0
        elif dxy_chg < -0.5:
            score += 4.0

        score = _clamp(score, 0.0, 100.0)
        factor = _clamp((score - 50.0) / 25.0, -1.0, 1.0)
        confidence = 40.0 if vix > 15 else 30.0

        out.append({
            'ts': ts,
            'vix': round(vix, 6),
            'vix_risk_level': _macro_risk_level(vix),
            'dxy_close': round(to_num(dxy_close[i], 0.0), 6),
            'dxy_5d_change': round(dxy_chg, 6),
            'score': round(score, 4),
            'factor': round(factor, 6),
            'confidence': round(confidence, 2),
            'source': 'real',
        })

    out = [p for p in out if int(to_num(p.get('ts'), 0)) >= int(start_ms) and int(to_num(p.get('ts'), 0)) <= int(end_ms + 86400000)]
    return out


def _load_macro_risk_cache_points() -> List[Dict[str, Any]]:
    raw = read_json(MACRO_RISK_CACHE_FILE, {})
    pts = (raw or {}).get('points') if isinstance(raw, dict) else []
    if not isinstance(pts, list):
        return []
    out: List[Dict[str, Any]] = []
    for p in pts:
        if not isinstance(p, dict):
            continue
        ts = int(to_num(p.get('ts'), 0))
        if ts <= 0:
            continue
        out.append({
            'ts': ts,
            'vix': to_num(p.get('vix'), 20.0),
            'vix_risk_level': str(p.get('vix_risk_level') or 'unknown'),
            'dxy_close': to_num(p.get('dxy_close'), 0.0),
            'dxy_5d_change': to_num(p.get('dxy_5d_change'), 0.0),
            'score': to_num(p.get('score'), 50.0),
            'factor': to_num(p.get('factor'), 0.0),
            'confidence': to_num(p.get('confidence'), 30.0),
            'source': str(p.get('source') or 'cache'),
        })
    out.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
    return out


def _save_macro_risk_cache_points(points: List[Dict[str, Any]]) -> None:
    if not isinstance(points, list):
        return
    clean = [x for x in points if isinstance(x, dict) and int(to_num(x.get('ts'), 0)) > 0]
    clean.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
    if len(clean) > MACRO_RISK_CACHE_MAX_POINTS:
        clean = clean[-MACRO_RISK_CACHE_MAX_POINTS:]
    write_json(MACRO_RISK_CACHE_FILE, {'updated_at': now_ms(), 'points': clean})


def _get_macro_risk_points(start_ms: int, end_ms: int) -> Dict[str, Any]:
    warm_ms = MACRO_RISK_WARM_DAYS * 86400000
    req_start = max(0, int(start_ms) - warm_ms)
    req_end = int(end_ms)

    cached = _load_macro_risk_cache_points()
    need_fetch = True
    if cached:
        c0 = int(to_num(cached[0].get('ts'), 0))
        c1 = int(to_num(cached[-1].get('ts'), 0))
        if c0 <= req_start + 86400000 and c1 >= req_end - 86400000:
            need_fetch = False

    fetched: List[Dict[str, Any]] = []
    if need_fetch:
        fetched = _compute_macro_risk_daily_points(req_start, req_end + 86400000)
        if fetched:
            m: Dict[int, Dict[str, Any]] = {}
            for p in cached:
                m[int(to_num(p.get('ts'), 0))] = p
            for p in fetched:
                m[int(to_num(p.get('ts'), 0))] = p
            cached = list(m.values())
            cached.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
            _save_macro_risk_cache_points(cached)

    points = [p for p in cached if int(to_num(p.get('ts'), 0)) >= req_start and int(to_num(p.get('ts'), 0)) <= int(req_end + 86400000)]
    meta = {
        'macro_risk_points': len(points),
        'macro_risk_cache_total': len(cached),
        'macro_risk_warm_days': MACRO_RISK_WARM_DAYS,
        'macro_risk_fetched': len(fetched),
    }
    return {'points': points, 'meta': meta}


def _fear_greed_level(v: float) -> str:
    x = to_num(v, 50.0)
    if x <= 20:
        return 'extreme_fear'
    if x <= 30:
        return 'fear'
    if x >= 80:
        return 'extreme_greed'
    if x >= 70:
        return 'greed'
    return 'neutral'


def _compute_fear_greed_daily_points(start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    if end_ms <= start_ms:
        return []
    try:
        r = requests.get(
            'https://api.alternative.me/fng/',
            params={'limit': 0, 'format': 'json'},
            timeout=OKX_PUBLIC_TIMEOUT,
        )
        if r.status_code != 200:
            return []
        obj = r.json()
    except Exception:
        return []

    rows = (obj or {}).get('data') if isinstance(obj, dict) else []
    if not isinstance(rows, list) or not rows:
        return []

    daily: List[Dict[str, Any]] = []
    for x in rows:
        if not isinstance(x, dict):
            continue
        ts_raw = x.get('timestamp')
        try:
            ts_ms = int(float(ts_raw)) * 1000
        except Exception:
            continue
        if ts_ms <= 0:
            continue
        dt = datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc)
        ts_day = int(datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc).timestamp() * 1000) + 86399999
        val = _clamp(to_num(x.get('value'), 50.0), 0.0, 100.0)
        cls = str(x.get('value_classification') or '').strip() or _fear_greed_level(val)
        daily.append({'ts': ts_day, 'value': val, 'classification': cls})

    if not daily:
        return []

    daily.sort(key=lambda z: int(to_num(z.get('ts'), 0)))
    out: List[Dict[str, Any]] = []
    prev_val = None
    for p in daily:
        value = to_num(p.get('value'), 50.0)
        delta = 0.0 if prev_val is None else (value - to_num(prev_val, value))
        score = 50.0
        if value <= 20:
            score += 16.0
        elif value <= 30:
            score += 10.0
        elif value >= 80:
            score -= 16.0
        elif value >= 70:
            score -= 10.0
        score += _clamp(delta * 0.6, -8.0, 8.0)
        score = _clamp(score, 0.0, 100.0)
        factor = _clamp((score - 50.0) / 25.0, -1.0, 1.0)
        confidence = _clamp(52.0 + min(22.0, abs(value - 50.0) * 0.4), 20.0, 95.0)
        out.append({
            'ts': int(to_num(p.get('ts'), 0)),
            'value': round(value, 4),
            'delta': round(delta, 4),
            'classification': str(p.get('classification') or _fear_greed_level(value)),
            'score': round(score, 4),
            'factor': round(factor, 6),
            'confidence': round(confidence, 2),
            'source': 'real',
        })
        prev_val = value

    out = [p for p in out if int(to_num(p.get('ts'), 0)) >= int(start_ms) and int(to_num(p.get('ts'), 0)) <= int(end_ms + 86400000)]
    return out


def _load_fear_greed_cache_points() -> List[Dict[str, Any]]:
    raw = read_json(FEAR_GREED_CACHE_FILE, {})
    pts = (raw or {}).get('points') if isinstance(raw, dict) else []
    if not isinstance(pts, list):
        return []
    out: List[Dict[str, Any]] = []
    for p in pts:
        if not isinstance(p, dict):
            continue
        ts = int(to_num(p.get('ts'), 0))
        if ts <= 0:
            continue
        value = _clamp(to_num(p.get('value'), 50.0), 0.0, 100.0)
        out.append({
            'ts': ts,
            'value': value,
            'delta': to_num(p.get('delta'), 0.0),
            'classification': str(p.get('classification') or _fear_greed_level(value)),
            'score': to_num(p.get('score'), 50.0),
            'factor': to_num(p.get('factor'), 0.0),
            'confidence': to_num(p.get('confidence'), 50.0),
            'source': str(p.get('source') or 'cache'),
        })
    out.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
    return out


def _save_fear_greed_cache_points(points: List[Dict[str, Any]]) -> None:
    if not isinstance(points, list):
        return
    clean = [x for x in points if isinstance(x, dict) and int(to_num(x.get('ts'), 0)) > 0]
    clean.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
    if len(clean) > FEAR_GREED_CACHE_MAX_POINTS:
        clean = clean[-FEAR_GREED_CACHE_MAX_POINTS:]
    write_json(FEAR_GREED_CACHE_FILE, {'updated_at': now_ms(), 'points': clean})


def _get_fear_greed_points(start_ms: int, end_ms: int) -> Dict[str, Any]:
    warm_ms = FEAR_GREED_WARM_DAYS * 86400000
    req_start = max(0, int(start_ms) - warm_ms)
    req_end = int(end_ms)

    cached = _load_fear_greed_cache_points()
    need_fetch = True
    if cached:
        c0 = int(to_num(cached[0].get('ts'), 0))
        c1 = int(to_num(cached[-1].get('ts'), 0))
        if c0 <= req_start + 86400000 and c1 >= req_end - 86400000:
            need_fetch = False

    fetched: List[Dict[str, Any]] = []
    if need_fetch:
        fetched = _compute_fear_greed_daily_points(req_start, req_end + 86400000)
        if fetched:
            m: Dict[int, Dict[str, Any]] = {}
            for p in cached:
                m[int(to_num(p.get('ts'), 0))] = p
            for p in fetched:
                m[int(to_num(p.get('ts'), 0))] = p
            cached = list(m.values())
            cached.sort(key=lambda x: int(to_num(x.get('ts'), 0)))
            _save_fear_greed_cache_points(cached)

    points = [p for p in cached if int(to_num(p.get('ts'), 0)) >= req_start and int(to_num(p.get('ts'), 0)) <= int(req_end + 86400000)]
    meta = {
        'fear_greed_points': len(points),
        'fear_greed_cache_total': len(cached),
        'fear_greed_warm_days': FEAR_GREED_WARM_DAYS,
        'fear_greed_fetched': len(fetched),
    }
    return {'points': points, 'meta': meta}


def _build_aux_series(candles: List[Dict[str, Any]], inst_id: str, bar: str, start_ms: int | None, end_ms: int | None) -> Dict[str, Any]:
    if not candles:
        return {'series': {}, 'source_meta': {}}

    ts_start = int(to_num(candles[0].get('ts'), 0))
    ts_end = int(to_num(candles[-1].get('ts'), 0))
    s_ms = int(start_ms or ts_start)
    e_ms = int(end_ms or ts_end)

    period = _rubik_period_from_bar(bar)
    max_points = max(300, min(MAX_BACKTEST_BARS, len(candles) + 120))

    # spot for basis
    spot_inst = inst_id.replace('-SWAP', '') if inst_id.endswith('-SWAP') else 'BTC-USDT'
    spot_k = fetch_okx_real_kline(spot_inst, bar, s_ms, e_ms, len(candles) + 20)
    spot_points: List[Dict[str, Any]] = []
    if spot_k.get('ok'):
        for c in (spot_k.get('candles') or []):
            spot_points.append({'ts': int(to_num(c.get('ts'), 0)), 'close': to_num(c.get('close'), 0.0)})

    oi_rows = _fetch_okx_rubik_rows(
        '/api/v5/rubik/stat/contracts/open-interest-history',
        {'instId': inst_id, 'period': period},
        s_ms,
        e_ms,
        max_points=max_points,
    )
    ls_rows = _fetch_okx_rubik_rows(
        '/api/v5/rubik/stat/contracts/long-short-account-ratio-contract',
        {'instId': inst_id, 'period': period},
        s_ms,
        e_ms,
        max_points=max_points,
    )
    tv_rows = _fetch_okx_rubik_rows(
        '/api/v5/rubik/stat/taker-volume-contract',
        {'instId': inst_id, 'period': period},
        s_ms,
        e_ms,
        max_points=max_points,
    )
    fr_rows = _fetch_okx_funding_rows(inst_id, s_ms, e_ms, max_points=max_points)

    oi_points = [{'ts': int(to_num(x[0], 0)), 'oi': to_num(x[1], 0.0)} for x in oi_rows if isinstance(x, list) and len(x) >= 2]
    ls_points = [{'ts': int(to_num(x[0], 0)), 'ratio': to_num(x[1], 1.0)} for x in ls_rows if isinstance(x, list) and len(x) >= 2]
    tv_points = [{'ts': int(to_num(x[0], 0)), 'buy': to_num(x[1], 0.0), 'sell': to_num(x[2], 0.0)} for x in tv_rows if isinstance(x, list) and len(x) >= 3]

    series = {
        'spot_close': _align_scalar_series(candles, spot_points, 'close', 0.0),
        'funding_rate': _align_scalar_series(candles, fr_rows, 'rate', 0.0),
        'open_interest': _align_scalar_series(candles, oi_points, 'oi', 0.0),
        'long_short_ratio': _align_scalar_series(candles, ls_points, 'ratio', 1.0),
    }
    buy, sell = _align_taker_series(candles, tv_points)
    series['taker_buy'] = buy
    series['taker_sell'] = sell

    xmc = _get_cross_market_points(s_ms, e_ms)
    xmc_points = list((xmc or {}).get('points') or [])
    series['cross_market_factor'] = _align_scalar_series(candles, xmc_points, 'factor', 0.0)
    series['cross_market_corr'] = _align_scalar_series(candles, xmc_points, 'corr', 0.0)

    mkr = _get_macro_risk_points(s_ms, e_ms)
    mkr_points = list((mkr or {}).get('points') or [])
    series['macro_risk_factor'] = _align_scalar_series(candles, mkr_points, 'factor', 0.0)
    series['macro_vix'] = _align_scalar_series(candles, mkr_points, 'vix', 20.0)
    series['macro_dxy_5d_change'] = _align_scalar_series(candles, mkr_points, 'dxy_5d_change', 0.0)

    fng = _get_fear_greed_points(s_ms, e_ms)
    fng_points = list((fng or {}).get('points') or [])
    series['fear_greed_factor'] = _align_scalar_series(candles, fng_points, 'factor', 0.0)
    series['fear_greed_value'] = _align_scalar_series(candles, fng_points, 'value', 50.0)
    series['fear_greed_delta'] = _align_scalar_series(candles, fng_points, 'delta', 0.0)

    source_meta = {
        'rubik_period': period,
        'spot_points': len(spot_points),
        'funding_points': len(fr_rows),
        'oi_points': len(oi_points),
        'long_short_points': len(ls_points),
        'taker_points': len(tv_points),
    }
    source_meta.update((xmc or {}).get('meta') or {})
    source_meta.update((mkr or {}).get('meta') or {})
    source_meta.update((fng or {}).get('meta') or {})

    return {
        'series': series,
        'source_meta': source_meta,
        'cross_market_daily': xmc_points,
        'macro_risk_daily': mkr_points,
        'fear_greed_daily': fng_points,
    }


def _factor_series_from_aux(candles: List[Dict[str, Any]], aux_series: Dict[str, Any]) -> Dict[str, Any]:
    n = len(candles)
    closes = [to_num(x.get('close'), 0.0) for x in candles]

    def _fit_arr(arr: Any, default: float = 0.0) -> List[float]:
        a = list(arr or [])
        if len(a) < n:
            a.extend([a[-1] if a else default] * (n - len(a)))
        elif len(a) > n:
            a = a[:n]
        return a

    spot = _fit_arr(aux_series.get('spot_close'), 0.0)
    fr = _fit_arr(aux_series.get('funding_rate'), 0.0)
    oi = _fit_arr(aux_series.get('open_interest'), 0.0)
    ratio = _fit_arr(aux_series.get('long_short_ratio'), 1.0)
    taker_buy = _fit_arr(aux_series.get('taker_buy'), 0.0)
    taker_sell = _fit_arr(aux_series.get('taker_sell'), 0.0)
    cross_market = _fit_arr(aux_series.get('cross_market_factor'), 0.0)
    macro_risk = _fit_arr(aux_series.get('macro_risk_factor'), 0.0)
    fear_greed = _fit_arr(aux_series.get('fear_greed_factor'), 0.0)

    basis_spread = [0.0] * n
    funding_rate = [0.0] * n
    oi_momentum = [0.0] * n
    long_short_ratio = [0.0] * n
    taker_imbalance = [0.0] * n
    order_flow_persistence = [0.0] * n
    funding_basis_divergence = [0.0] * n
    liquidation_pressure = [0.0] * n

    imb = [0.0] * n
    for i in range(n):
        c = closes[i]
        s = spot[i] if i < len(spot) else 0.0
        f = fr[i] if i < len(fr) else 0.0
        r = ratio[i] if i < len(ratio) else 1.0
        b = taker_buy[i] if i < len(taker_buy) else 0.0
        sl = taker_sell[i] if i < len(taker_sell) else 0.0

        # basis (swap-spot) contrarian
        if c > 0 and s > 0:
            basis = _safe_ratio(c - s, s)
            basis_spread[i] = _clamp(-basis * 140.0, -1.0, 1.0)
        else:
            basis = 0.0

        # funding contrarian
        funding_rate[i] = _clamp(-f * 9000.0, -1.0, 1.0)

        # long-short crowding contrarian
        if r >= 1.2:
            long_short_ratio[i] = -_clamp((r - 1.2) / 0.45, 0.0, 1.0)
        elif r <= 0.82:
            long_short_ratio[i] = _clamp((0.82 - r) / 0.32, 0.0, 1.0)
        else:
            long_short_ratio[i] = 0.0

        # taker imbalance
        denom = b + sl
        im = _safe_ratio(b - sl, denom if denom > 1e-9 else 1.0)
        imb[i] = _clamp(im, -1.0, 1.0)
        taker_imbalance[i] = _clamp(im * 2.4, -1.0, 1.0)

        # funding-basis divergence/consensus
        fr_sign = 1 if f > 0 else (-1 if f < 0 else 0)
        basis_sign = 1 if basis > 0 else (-1 if basis < 0 else 0)
        mag = _clamp(abs(f) * 7000.0 + abs(basis) * 90.0, 0.0, 1.0)
        if fr_sign != 0 and fr_sign == basis_sign:
            # same sign means crowded, contrarian opposite
            funding_basis_divergence[i] = -fr_sign * mag
        elif fr_sign != 0 and basis_sign != 0 and fr_sign != basis_sign:
            funding_basis_divergence[i] = -fr_sign * 0.22
        else:
            funding_basis_divergence[i] = 0.0

        # liquidation pressure proxy (crowding + imbalance)
        long_crowded = _clamp((r - 1.18) / 0.42, 0.0, 1.0)
        short_crowded = _clamp((0.84 - r) / 0.30, 0.0, 1.0)
        liquidation_pressure[i] = _clamp((short_crowded - long_crowded) * 0.65 + im * 0.35, -1.0, 1.0)

    for i in range(n):
        j0 = max(0, i - 3)
        w = imb[j0:i + 1]
        if not w:
            continue
        signs = [1 if x > 0 else (-1 if x < 0 else 0) for x in w]
        consistency = abs(sum(signs)) / len(signs)
        mean_imb = sum(w) / len(w)
        order_flow_persistence[i] = _clamp(mean_imb * (0.55 + 0.45 * consistency) * 2.0, -1.0, 1.0)

        # OI momentum with price coupling
        j = max(0, i - 6)
        oi0 = oi[j] if j < len(oi) else 0.0
        oid = _safe_ratio((oi[i] if i < len(oi) else 0.0) - oi0, oi0 if oi0 > 1e-9 else 1.0)
        p0 = closes[j] if j < len(closes) else 0.0
        pd = _safe_ratio(closes[i] - p0, p0 if p0 > 1e-9 else 1.0)
        if abs(pd) < 1e-9:
            oi_momentum[i] = _clamp(oid * 20.0, -1.0, 1.0)
        else:
            oi_momentum[i] = _clamp((1.0 if pd > 0 else -1.0) * abs(oid) * 18.0, -1.0, 1.0)

    return {
        'basis_spread': basis_spread,
        'funding_rate': funding_rate,
        'oi_momentum': oi_momentum,
        'long_short_ratio': long_short_ratio,
        'taker_imbalance': taker_imbalance,
        'order_flow_persistence': order_flow_persistence,
        'funding_basis_divergence': funding_basis_divergence,
        'liquidation_pressure': liquidation_pressure,
        'cross_market_correlation': cross_market,
        'macro_risk_sentiment': macro_risk,
        'fear_greed': fear_greed,
    }


def _score_to_signal(scores: List[float], threshold: float) -> List[int]:
    out: List[int] = []
    t = abs(float(threshold))
    for s in scores:
        if s >= t:
            out.append(1)
        elif s <= -t:
            out.append(-1)
        else:
            out.append(0)
    return out


BACKTEST_PROFILES: Dict[str, Dict[str, Any]] = {
    'regime_balance_v4': {
        'label': '稳健多状态 v4.1',
        'allow_short': True,
        'smooth': 5,
        'breakout_window': 16,
        'breakout_buffer': 0.0,
        'bull_gate': 0.086,
        'bear_gate': -0.483,
        'trend_gate': 0.081,
        'trend_gate_short': 0.045,
        'flow_gate': 0.051,
        'flow_gate_short': 0.115,
        'carry_long_floor': -0.066,
        'carry_short_cap': 0.239,
        'risk_floor': -0.135,
        'pull_extra': 0.052,
        'hold_hours_long': 4.58,
        'hold_hours_short': 4.55,
        'trail_long': 0.0124,
        'trail_short': 0.0052,
        'long_bail_bench': -0.027,
        'short_bail_bench': 0.107,
        'trend_exit_long': 0.018,
        'trend_exit_short': 0.047,
        'flow_exit_long': -0.052,
        'flow_exit_short': 0.045,
        'carry_exit_long': -0.262,
        'carry_exit_short': 0.106,
    },
    'balanced_alpha': {
        'label': '平衡阿尔法',
        'allow_short': True,
        'smooth': 4,
        'breakout_window': 20,
        'breakout_buffer': 0.001,
        'bull_gate': 0.12,
        'bear_gate': -0.36,
        'trend_gate': 0.06,
        'trend_gate_short': 0.10,
        'flow_gate': 0.04,
        'flow_gate_short': 0.09,
        'carry_long_floor': -0.06,
        'carry_short_cap': 0.18,
        'risk_floor': -0.08,
        'pull_extra': 0.06,
        'hold_hours_long': 8.0,
        'hold_hours_short': 10.0,
        'trail_long': 0.010,
        'trail_short': 0.015,
        'long_bail_bench': -0.02,
        'short_bail_bench': 0.02,
        'trend_exit_long': 0.02,
        'trend_exit_short': 0.06,
        'flow_exit_long': -0.06,
        'flow_exit_short': 0.06,
        'carry_exit_long': -0.15,
        'carry_exit_short': 0.16,
    },
    'defensive_cash': {
        'label': '防守现金',
        'allow_short': False,
        'smooth': 4,
        'breakout_window': 16,
        'breakout_buffer': 0.001,
        'bull_gate': 0.16,
        'bear_gate': -0.36,
        'trend_gate': 0.10,
        'trend_gate_short': 0.08,
        'flow_gate': 0.04,
        'flow_gate_short': 0.11,
        'carry_long_floor': -0.06,
        'carry_short_cap': 0.99,
        'risk_floor': -0.08,
        'pull_extra': 0.02,
        'hold_hours_long': 16.0,
        'hold_hours_short': 8.0,
        'trail_long': 0.008,
        'trail_short': 0.012,
        'long_bail_bench': 0.00,
        'short_bail_bench': 0.04,
        'trend_exit_long': -0.02,
        'trend_exit_short': 0.04,
        'flow_exit_long': -0.03,
        'flow_exit_short': 0.06,
        'carry_exit_long': -0.30,
        'carry_exit_short': 0.16,
    },
    'liquidation_reversal_v1': {
        'label': '清算反向 v1',
        'allow_short': True,
        'smooth': 3,
        'hold_hours_long': 6.0,
        'hold_hours_short': 6.0,
        'min_hold_bars': 2,
        'trail_long': 0.0072,
        'trail_short': 0.0072,
        'liq_trigger': 0.44,
        'liq_release': 0.14,
        'impulse_bars': 3,
        'impulse_gate': 0.0045,
        'extension_gate': 0.0035,
        'flow_gate': 0.09,
        'take_profit': 0.0065,
        'stop_loss_long': 0.0105,
        'stop_loss_short': 0.0105,
    },
}


def _series_mix(fac: Dict[str, Any], n: int, mapping: Dict[str, float]) -> List[float]:
    out = [0.0] * n
    tw = 0.0
    for key, w in mapping.items():
        arr = list(fac.get(key) or [])
        if len(arr) < n:
            arr.extend([arr[-1] if arr else 0.0] * (n - len(arr)))
        elif len(arr) > n:
            arr = arr[:n]
        if not arr:
            continue
        tw += abs(w)
        for i in range(n):
            out[i] += to_num(arr[i], 0.0) * w
    if tw <= 1e-9:
        return out
    return [_clamp(v / tw, -1.0, 1.0) for v in out]


def _liquidation_reversal_signal(
    candles: List[Dict[str, Any]],
    fac: Dict[str, Any],
    profile: Dict[str, Any],
    bar_ms: int,
    benchmark: List[float],
    trend_stack: List[float],
    flow_impulse: List[float],
    carry_regime: List[float],
    risk_pressure: List[float],
) -> Dict[str, Any]:
    n = len(candles)
    closes = [to_num(x.get('close'), 0.0) for x in candles]
    ema20 = _ema(closes, 20)
    ema50 = _ema(closes, 50)
    bar_hours = max(bar_ms / 3600000.0, 1.0 / 12.0)
    max_hold_long = max(3, int(round(to_num(profile.get('hold_hours_long'), 6.0) / bar_hours)))
    max_hold_short = max(3, int(round(to_num(profile.get('hold_hours_short'), 6.0) / bar_hours)))
    min_hold_bars = max(1, int(to_num(profile.get('min_hold_bars'), 2)))
    smooth = max(2, int(to_num(profile.get('smooth'), 3)))
    impulse_bars = max(1, int(to_num(profile.get('impulse_bars'), 3)))
    liq_trigger = max(0.08, min(0.95, to_num(profile.get('liq_trigger'), 0.44)))
    liq_release = max(0.02, min(liq_trigger * 0.95, to_num(profile.get('liq_release'), 0.14)))
    impulse_gate = max(0.0005, min(0.03, to_num(profile.get('impulse_gate'), 0.0045)))
    extension_gate = max(0.0005, min(0.06, to_num(profile.get('extension_gate'), 0.0035)))
    flow_gate = max(0.01, min(0.95, to_num(profile.get('flow_gate'), 0.09)))
    trail_long = max(0.001, min(0.08, to_num(profile.get('trail_long'), 0.0072)))
    trail_short = max(0.001, min(0.08, to_num(profile.get('trail_short'), 0.0072)))
    take_profit = max(0.001, min(0.2, to_num(profile.get('take_profit'), 0.0065)))
    stop_loss_long = max(0.001, min(0.2, to_num(profile.get('stop_loss_long'), 0.0105)))
    stop_loss_short = max(0.001, min(0.2, to_num(profile.get('stop_loss_short'), 0.0105)))
    allow_short = bool(profile.get('allow_short', True))

    def extend_series(key: str, fill: float = 0.0) -> List[float]:
        arr = list(fac.get(key) or [])
        if len(arr) < n:
            arr.extend([arr[-1] if arr else fill] * (n - len(arr)))
        elif len(arr) > n:
            arr = arr[:n]
        return [_clamp(to_num(v, fill), -1.0, 1.0) for v in arr]

    liq = extend_series('liquidation_pressure', 0.0)
    flow = extend_series('order_flow_persistence', 0.0)
    taker = extend_series('taker_imbalance', 0.0)
    rsi_reversion = extend_series('rsi_reversion', 0.0)

    def smoothed(arr: List[float], idx: int, win: int) -> float:
        s = max(0, idx - win + 1)
        seg = arr[s:idx + 1]
        return sum(seg) / len(seg) if seg else 0.0

    signal = [0] * n
    system_score = [0.0] * n
    liq_event = [0.0] * n

    pos = 0
    entry_i: int | None = None
    entry_px = 0.0
    high_water = 0.0
    low_water = 0.0

    for i in range(n):
        c = closes[i]
        if c <= 0:
            continue
        j = max(0, i - impulse_bars)
        base = closes[j] if closes[j] > 1e-9 else c
        move = _safe_ratio(c - base, base)
        lp = smoothed(liq, i, smooth)
        fl = smoothed(flow, i, smooth)
        tk = smoothed(taker, i, smooth)
        rv = smoothed(rsi_reversion, i, smooth)
        ext_up = _safe_ratio(c - ema20[i], ema20[i] if ema20[i] > 1e-9 else c)
        ext_down = _safe_ratio(ema20[i] - c, ema20[i] if ema20[i] > 1e-9 else c)

        squeeze_up = lp >= liq_trigger and move >= impulse_gate and ext_up >= extension_gate and (fl >= flow_gate or tk >= flow_gate * 0.9)
        squeeze_down = lp <= -liq_trigger and move <= -impulse_gate and ext_down >= extension_gate and (fl <= -flow_gate or tk <= -flow_gate * 0.9)
        if squeeze_up:
            liq_event[i] = -1.0
        elif squeeze_down:
            liq_event[i] = 1.0

        move_scaled = _clamp(move / max(impulse_gate, 1e-6), -2.0, 2.0) / 2.0
        system_score[i] = _clamp((-lp) * 0.66 + (-move_scaled) * 0.24 + rv * 0.10, -1.0, 1.0)

        long_mean_revert = c >= ema20[i] and c >= ema50[i] and rv >= -0.12
        short_mean_revert = c <= ema20[i] and c <= ema50[i] and rv <= 0.12

        if pos == 0:
            if squeeze_down:
                pos = 1
                entry_i = i
                entry_px = c
                high_water = c
                low_water = c
            elif squeeze_up and allow_short:
                pos = -1
                entry_i = i
                entry_px = c
                high_water = c
                low_water = c
        elif pos == 1:
            high_water = max(high_water, c)
            hold_bars = i - (entry_i if entry_i is not None else i)
            pnl = _safe_ratio(c - entry_px, entry_px if entry_px > 1e-9 else c)
            trail_hit = c <= high_water * (1.0 - trail_long)
            neutralized = abs(lp) <= liq_release and hold_bars >= min_hold_bars
            exit_long = hold_bars >= max_hold_long or pnl <= -stop_loss_long or pnl >= take_profit or long_mean_revert or trail_hit or neutralized
            if squeeze_up and allow_short:
                pos = -1
                entry_i = i
                entry_px = c
                high_water = c
                low_water = c
            elif exit_long:
                pos = 0
                entry_i = None
                entry_px = 0.0
        elif pos == -1:
            low_water = min(low_water, c)
            hold_bars = i - (entry_i if entry_i is not None else i)
            pnl = _safe_ratio(entry_px - c, entry_px if entry_px > 1e-9 else c)
            trail_hit = c >= low_water * (1.0 + trail_short)
            neutralized = abs(lp) <= liq_release and hold_bars >= min_hold_bars
            exit_short = hold_bars >= max_hold_short or pnl <= -stop_loss_short or pnl >= take_profit or short_mean_revert or trail_hit or neutralized
            if squeeze_down:
                pos = 1
                entry_i = i
                entry_px = c
                high_water = c
                low_water = c
            elif exit_short:
                pos = 0
                entry_i = None
                entry_px = 0.0

        signal[i] = pos

    return {
        'signal': signal,
        'system_score': system_score,
        'clusters': {
            'benchmark_regime': benchmark,
            'trend_stack': trend_stack,
            'flow_impulse': flow_impulse,
            'carry_regime': carry_regime,
            'risk_pressure': risk_pressure,
            'liquidation_event': liq_event,
        },
        'profile': profile,
        'max_hold_bars': {'long': max_hold_long, 'short': max_hold_short},
    }


def _stable_alpha_signal(candles: List[Dict[str, Any]], fac: Dict[str, Any], profile_name: str, bar_ms: int) -> Dict[str, Any]:
    n = len(candles)
    closes = [to_num(x.get('close'), 0.0) for x in candles]
    ema20 = _ema(closes, 20)
    ema50 = _ema(closes, 50)
    ema120 = _ema(closes, 120)
    profile = dict(BACKTEST_PROFILES.get(profile_name) or BACKTEST_PROFILES['regime_balance_v4'])
    bar_hours = max(bar_ms / 3600000.0, 1.0 / 12.0)
    max_hold_long = max(4, int(round(to_num(profile.get('hold_hours_long'), 8.0) / bar_hours)))
    max_hold_short = max(3, int(round(to_num(profile.get('hold_hours_short'), 6.0) / bar_hours)))
    smooth = max(2, int(to_num(profile.get('smooth'), 4)))
    breakout_w = max(6, int(to_num(profile.get('breakout_window'), 16)))
    breakout_buf = to_num(profile.get('breakout_buffer'), 0.0)

    benchmark = _series_mix(fac, n, {
        'trend_ema': 0.30,
        'momentum': 0.16,
        'breakout_20': 0.18,
        'enhanced_technical': 0.18,
        'asr_vc': 0.10,
        'cross_market_correlation': 0.08,
    })
    trend_stack = _series_mix(fac, n, {
        'trend_ema': 0.26,
        'momentum': 0.20,
        'breakout_20': 0.18,
        'enhanced_technical': 0.16,
        'asr_vc': 0.12,
        'technical_kline': 0.08,
    })
    flow_impulse = _series_mix(fac, n, {
        'oi_momentum': 0.24,
        'taker_imbalance': 0.24,
        'order_flow_persistence': 0.22,
        'liquidation_pressure': 0.18,
        'volume_flow': 0.12,
    })
    carry_regime = _series_mix(fac, n, {
        'funding_basis_divergence': 0.34,
        'funding_rate': 0.20,
        'basis_spread': 0.18,
        'long_short_ratio': 0.16,
        'fear_greed': 0.12,
    })
    risk_pressure = _series_mix(fac, n, {
        'macro_risk_sentiment': 0.28,
        'cross_market_correlation': 0.22,
        'volatility_regime': 0.22,
        'fear_greed': 0.14,
        'rsi_reversion': 0.14,
    })
    if profile_name == 'liquidation_reversal_v1':
        return _liquidation_reversal_signal(
            candles,
            fac,
            profile,
            bar_ms,
            benchmark,
            trend_stack,
            flow_impulse,
            carry_regime,
            risk_pressure,
        )

    def smoothed(arr: List[float], idx: int, win: int) -> float:
        s = max(0, idx - win + 1)
        seg = arr[s:idx + 1]
        return sum(seg) / len(seg) if seg else 0.0

    signal = [0] * n
    system_score = [0.0] * n
    pos = 0
    entry_i: int | None = None
    high_water = 0.0
    low_water = 0.0

    for i in range(n):
        c = closes[i]
        if c <= 0:
            continue
        b = smoothed(benchmark, i, smooth)
        t = smoothed(trend_stack, i, smooth)
        f = smoothed(flow_impulse, i, smooth)
        cg = smoothed(carry_regime, i, max(2, smooth - 1))
        r = smoothed(risk_pressure, i, smooth)
        up_stack = c >= ema20[i] >= ema50[i] >= ema120[i]
        down_stack = c <= ema20[i] <= ema50[i] <= ema120[i]
        hh = max(closes[max(0, i - breakout_w):i] or [c])
        ll = min(closes[max(0, i - breakout_w):i] or [c])
        broke_up = c >= hh * (1.0 + breakout_buf)
        broke_down = c <= ll * (1.0 - breakout_buf)
        long_power = _clamp(b * 0.40 + t * 0.28 + f * 0.22 + cg * 0.10, -1.0, 1.0)
        short_power = _clamp((-b) * 0.44 + (-t) * 0.24 + (-f) * 0.22 + (-cg) * 0.10, -1.0, 1.0)
        system_score[i] = long_power if abs(long_power) >= abs(short_power) else -short_power

        bull_state = b >= to_num(profile.get('bull_gate'), 0.08) and t >= to_num(profile.get('trend_gate'), 0.08) and up_stack and r >= to_num(profile.get('risk_floor'), -0.15)
        bear_state = bool(profile.get('allow_short')) and b <= to_num(profile.get('bear_gate'), -0.48) and t <= -to_num(profile.get('trend_gate_short'), 0.04) and down_stack
        long_impulse = bull_state and f >= to_num(profile.get('flow_gate'), 0.04) and cg >= to_num(profile.get('carry_long_floor'), -0.06) and (broke_up or (c > ema20[i] and f >= to_num(profile.get('flow_gate'), 0.04) + to_num(profile.get('pull_extra'), 0.06)))
        short_impulse = bear_state and f <= -to_num(profile.get('flow_gate_short'), 0.11) and cg <= to_num(profile.get('carry_short_cap'), 0.24) and (broke_down or (c < ema20[i] and f <= -(to_num(profile.get('flow_gate_short'), 0.11) + to_num(profile.get('pull_extra'), 0.06))))

        if pos == 0:
            if long_impulse:
                pos = 1
                entry_i = i
                high_water = c
                low_water = c
            elif short_impulse:
                pos = -1
                entry_i = i
                high_water = c
                low_water = c
        elif pos == 1:
            high_water = max(high_water, c)
            hold_bars = i - (entry_i or i)
            trail_hit = c <= high_water * (1.0 - to_num(profile.get('trail_long'), 0.012))
            if hold_bars >= max_hold_long or b <= to_num(profile.get('long_bail_bench'), -0.04) or t <= to_num(profile.get('trend_exit_long'), 0.02) or f <= to_num(profile.get('flow_exit_long'), -0.06) or cg <= to_num(profile.get('carry_exit_long'), -0.22) or trail_hit or c < ema20[i]:
                pos = 0
                entry_i = None
        elif pos == -1:
            low_water = min(low_water, c)
            hold_bars = i - (entry_i or i)
            trail_hit = c >= low_water * (1.0 + to_num(profile.get('trail_short'), 0.006))
            if hold_bars >= max_hold_short or b >= to_num(profile.get('short_bail_bench'), 0.08) or t >= -to_num(profile.get('trend_exit_short'), 0.06) or f >= -to_num(profile.get('flow_exit_short'), 0.06) or cg >= to_num(profile.get('carry_exit_short'), 0.10) or trail_hit or c > ema20[i]:
                pos = 0
                entry_i = None

        signal[i] = pos

    return {
        'signal': signal,
        'system_score': system_score,
        'clusters': {
            'benchmark_regime': benchmark,
            'trend_stack': trend_stack,
            'flow_impulse': flow_impulse,
            'carry_regime': carry_regime,
            'risk_pressure': risk_pressure,
        },
        'profile': profile,
        'max_hold_bars': {'long': max_hold_long, 'short': max_hold_short},
    }


def _rolling_outperformance_rate(sys_curve: List[Dict[str, Any]], bench_curve: List[Dict[str, Any]], window_bars: int) -> Dict[str, Any]:
    if not sys_curve or not bench_curve:
        return {'window_bars': window_bars, 'rate_pct': 0.0}
    sys_eq = [to_num(x.get('equity'), 1.0) for x in sys_curve]
    bench_eq = [to_num(x.get('equity'), 1.0) for x in bench_curve]
    n = min(len(sys_eq), len(bench_eq))
    if n <= window_bars:
        return {'window_bars': window_bars, 'rate_pct': 0.0}
    wins = 0
    total = 0
    for i in range(window_bars, n):
        sr = _safe_ratio(sys_eq[i] - sys_eq[i - window_bars], sys_eq[i - window_bars])
        br = _safe_ratio(bench_eq[i] - bench_eq[i - window_bars], bench_eq[i - window_bars])
        wins += 1 if sr > br else 0
        total += 1
    return {'window_bars': window_bars, 'rate_pct': round((wins / total) * 100.0, 2) if total else 0.0}


def _window_scan(candles: List[Dict[str, Any]], fac: Dict[str, Any], profile_name: str, leverage: float, fee_bps: float, slippage_bps: float, dd_window: int, bar_ms: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for w in [720, 960, 1200, 1600, 2000, 2400, 2800, 3000]:
        if len(candles) < w:
            continue
        sub = candles[-w:]
        fac_sub = {k: (list(v[-w:]) if isinstance(v, list) else v) for k, v in fac.items()}
        pack = _stable_alpha_signal(sub, fac_sub, profile_name, bar_ms)
        sig = list(pack.get('signal') or [])
        bt = _equity_backtest(sub, sig, leverage, fee_bps, slippage_bps, dd_window)
        bm = _equity_backtest(sub, [1] * len(sub), 1.0, 0.0, 0.0, dd_window)
        if not bt.get('ok') or not bm.get('ok'):
            continue
        rows.append({
            'bars': w,
            'return_pct': bt.get('return_pct', 0.0),
            'annualized_return_pct': bt.get('annualized_return_pct', 0.0),
            'max_drawdown_pct': bt.get('max_drawdown_pct', 0.0),
            'profit_factor': bt.get('profit_factor'),
            'trades': bt.get('trades', 0),
            'benchmark_return_pct': bm.get('return_pct', 0.0),
            'excess_return_pct': round(to_num(bt.get('return_pct'), 0.0) - to_num(bm.get('return_pct'), 0.0), 4),
        })
    return rows


def run_backtest_real_kline(payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    p = payload if isinstance(payload, dict) else {}
    inst_id = str(p.get('instId') or 'BTC-USDT-SWAP').strip().upper()
    bar = str(p.get('bar') or '1H').strip()

    start_ms = _parse_user_time_to_ms(p.get('start'))
    end_ms = _parse_user_time_to_ms(p.get('end'))
    if end_ms is None:
        end_ms = now_ms()
    if start_ms and end_ms and start_ms >= end_ms:
        return {'ok': False, 'error': 'invalid_range_start_ge_end'}

    bars = int(to_num(p.get('bars'), 1500))
    bars = max(300, min(bars, MAX_BACKTEST_BARS))
    dd_window = int(to_num(p.get('drawdownWindowBars'), 96))
    dd_window = max(5, min(dd_window, 2000))

    leverage = to_num(p.get('leverage'), 1.0)
    leverage = max(0.2, min(leverage, 8.0))
    fee_bps = to_num(p.get('feeBps'), 5.0)
    slippage_bps = to_num(p.get('slippageBps'), 3.0)
    system_threshold = to_num(p.get('systemThreshold'), 0.21)
    system_threshold = max(0.08, min(system_threshold, 0.35))
    profile_name = str(p.get('profile') or 'regime_balance_v4').strip().lower()
    if profile_name not in BACKTEST_PROFILES:
        profile_name = 'regime_balance_v4'

    kline = fetch_okx_real_kline(inst_id, bar, start_ms, end_ms, bars)
    if not kline.get('ok'):
        return {'ok': False, 'error': kline.get('error', 'kline_fetch_failed'), 'detail': kline.get('detail', '')}
    candles = kline.get('candles') or []
    if len(candles) < 120:
        return {'ok': False, 'error': 'kline_not_enough', 'bars': len(candles)}

    fac = _factor_series_from_kline(candles)
    aux = _build_aux_series(candles, inst_id, bar, start_ms, end_ms)
    fac_aux = _factor_series_from_aux(candles, aux.get('series') or {})
    fac.update(fac_aux)
    fac_for_scan = {k: (list(v) if isinstance(v, list) else v) for k, v in fac.items()}

    n = len(candles)
    bar_ms = _bar_to_ms(bar)
    stable_pack = _stable_alpha_signal(candles, fac, profile_name, bar_ms)
    fac.update(stable_pack.get('clusters') or {})
    fac['system_score'] = list(stable_pack.get('system_score') or [])

    weights = {
        'benchmark_regime': 0.16,
        'trend_stack': 0.14,
        'flow_impulse': 0.13,
        'carry_regime': 0.10,
        'risk_pressure': 0.06,
        'order_flow_persistence': 0.06,
        'liquidation_pressure': 0.06,
        'funding_basis_divergence': 0.05,
        'oi_momentum': 0.05,
        'taker_imbalance': 0.05,
        'breakout_20': 0.04,
        'enhanced_technical': 0.03,
        'asr_vc': 0.03,
        'macro_risk_sentiment': 0.02,
        'cross_market_correlation': 0.02,
        'fear_greed': 0.02,
        'momentum': 0.02,
        'trend_ema': 0.02,
        'long_short_ratio': 0.02,
        'volume_flow': 0.01,
        'basis_spread': 0.01,
        'funding_rate': 0.01,
        'volatility_regime': 0.01,
    }

    factor_threshold = {
        'benchmark_regime': 0.10,
        'trend_stack': 0.12,
        'flow_impulse': 0.12,
        'carry_regime': 0.10,
        'risk_pressure': 0.12,
        'breakout_20': 0.10,
        'asr_vc': 0.12,
        'funding_basis_divergence': 0.12,
        'order_flow_persistence': 0.12,
        'cross_market_correlation': 0.12,
        'macro_risk_sentiment': 0.10,
        'fear_greed': 0.10,
    }

    active_keys = [k for k in weights.keys() if isinstance(fac.get(k), list) and len(fac.get(k) or []) >= n]
    if not active_keys:
        return {'ok': False, 'error': 'no_factor_series'}

    # factor-level backtests
    factor_results: List[Dict[str, Any]] = []
    for key in active_keys:
        arr = fac.get(key) or [0.0] * n
        sig = _score_to_signal(arr, factor_threshold.get(key, 0.14))
        bt = _equity_backtest(candles, sig, leverage, fee_bps, slippage_bps, dd_window)
        if not bt.get('ok'):
            continue
        factor_results.append({
            'key': key,
            'name': factor_meta(key).get('label'),
            'weight': round(to_num(weights.get(key), 0.0), 4),
            'group': factor_meta(key).get('group'),
            'thesis': factor_meta(key).get('thesis'),
            'return_pct': bt.get('return_pct', 0.0),
            'max_drawdown_pct': bt.get('max_drawdown_pct', 0.0),
            'rolling_drawdown': bt.get('rolling_drawdown', {}),
            'drawdown_period': bt.get('drawdown_period', {}),
            'trades': bt.get('trades', 0),
            'win_rate_pct': bt.get('win_rate_pct', 0.0),
            'sharpe_like': bt.get('sharpe_like', 0.0),
            'costs': bt.get('costs', {}),
            'curve': bt.get('curve', []),
        })

    factor_results.sort(key=lambda x: to_num(x.get('max_drawdown_pct'), 0.0), reverse=True)

    # full factor time-series package for unified multi-factor chart
    factor_series: Dict[str, List[Dict[str, Any]]] = {}
    factor_meta_rows: List[Dict[str, Any]] = []
    for key in active_keys:
        arr = list(fac.get(key) or [])
        if len(arr) < n:
            arr.extend([arr[-1] if arr else 0.0] * (n - len(arr)))
        elif len(arr) > n:
            arr = arr[:n]
        pts: List[Dict[str, Any]] = []
        for i, c in enumerate(candles):
            pts.append({
                'ts': int(to_num(c.get('ts'), 0)),
                'value': round(to_num(arr[i], 0.0), 6),
            })
        factor_series[key] = _downsample_points(pts, 1200)
        factor_meta_rows.append({
            'key': key,
            'label': factor_meta(key).get('label'),
            'group': factor_meta(key).get('group'),
            'weight': round(to_num(weights.get(key), 0.0), 4),
            'threshold': round(to_num(factor_threshold.get(key, 0.14), 0.14), 4),
            'latest': round(to_num(arr[-1] if arr else 0.0, 0.0), 6),
        })

    # system backtest: 使用基准增强型状态机，而不是简单分数翻仓
    system_signal = list(stable_pack.get('signal') or [])
    system_bt = _equity_backtest(candles, system_signal, leverage, fee_bps, slippage_bps, dd_window)
    if not system_bt.get('ok'):
        return {'ok': False, 'error': system_bt.get('error', 'system_backtest_failed')}
    benchmark_bt = _equity_backtest(candles, [1] * n, 1.0, 0.0, 0.0, dd_window)
    if not benchmark_bt.get('ok'):
        return {'ok': False, 'error': benchmark_bt.get('error', 'benchmark_backtest_failed')}
    liq_pack = _stable_alpha_signal(candles, fac_for_scan, 'liquidation_reversal_v1', bar_ms)
    liq_signal = list(liq_pack.get('signal') or [])
    liq_bt = _equity_backtest(candles, liq_signal, leverage, fee_bps, slippage_bps, dd_window)
    liq_trade_rows = _virtual_trade_rows_from_signal(
        candles=candles,
        signal=liq_signal,
        leverage=leverage,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        bar_ms=bar_ms,
        strategy_name='liquidation_reversal_v1',
        factor_series=fac_for_scan,
        liq_event_series=((liq_pack.get('clusters') or {}).get('liquidation_event') or []),
        keep=160,
    )

    start_ts = candles[0].get('ts') if candles else 0
    end_ts = candles[-1].get('ts') if candles else 0
    system_curve_full = _full_equity_curve(candles, system_signal, leverage, fee_bps, slippage_bps)
    benchmark_curve_full = _full_equity_curve(candles, [1] * n, 1.0, 0.0, 0.0)
    rolling_7d = _rolling_outperformance_rate(system_curve_full, benchmark_curve_full, max(5, int(round((7 * 24 * 3600000) / max(bar_ms, 1)))))
    rolling_30d = _rolling_outperformance_rate(system_curve_full, benchmark_curve_full, max(5, int(round((30 * 24 * 3600000) / max(bar_ms, 1)))))
    window_scan = _window_scan(candles, fac_for_scan, profile_name, leverage, fee_bps, slippage_bps, dd_window, bar_ms)
    profile_descriptions = {
        'regime_balance_v4': 'v4.1 在多状态状态机上做了参数标定：提高牛市门槛、缩短持仓预算、收紧追踪保护，优先保持净值曲线平滑向上，再争取超额收益。',
        'balanced_alpha': '平衡阿尔法在趋势、订单流和拥挤度间做折中，减少过度择时，追求收益与回撤的中位平衡。',
        'defensive_cash': '防守现金画像优先控制回撤，只有在信号共振较强时才提高仓位，默认降低短线翻仓频率。',
        'liquidation_reversal_v1': '清算反向 v1：当价格加速并触发同向清算压力时，执行反向开仓；压力回落或价格回归均线后离场。',
    }
    liq_profile = 'liquidation_reversal_v1'
    liq_profile_label = BACKTEST_PROFILES.get(liq_profile, {}).get('label', liq_profile)
    liq_summary: Dict[str, Any] = {
        'ok': bool(liq_bt.get('ok')),
        'profile': liq_profile,
        'profile_label': liq_profile_label,
        'description': profile_descriptions.get(liq_profile, ''),
    }
    if liq_bt.get('ok'):
        liq_summary.update({
            'return_pct': liq_bt.get('return_pct', 0.0),
            'annualized_return_pct': liq_bt.get('annualized_return_pct', 0.0),
            'max_drawdown_pct': liq_bt.get('max_drawdown_pct', 0.0),
            'rolling_drawdown': liq_bt.get('rolling_drawdown', {}),
            'trades': liq_bt.get('trades', 0),
            'win_rate_pct': liq_bt.get('win_rate_pct', 0.0),
            'profit_factor': liq_bt.get('profit_factor'),
            'sharpe_like': liq_bt.get('sharpe_like', 0.0),
            'calmar_like': liq_bt.get('calmar_like'),
            'costs': liq_bt.get('costs', {}),
            'curve': liq_bt.get('curve', []),
            'signal': _downsample_points([
                {'ts': int(to_num(c.get('ts'), 0)), 'value': int(liq_signal[i]) if i < len(liq_signal) else 0}
                for i, c in enumerate(candles)
            ], 360),
            'virtual_trade_rows': liq_trade_rows,
        })
    else:
        liq_summary['error'] = liq_bt.get('error', 'liquidation_backtest_failed')

    result = {
        'ok': True,
        'generated_at': now_ms(),
        'source': kline.get('source', {}),
        'params': {
            'instId': inst_id,
            'bar': bar,
            'bars': len(candles),
            'requestedBars': bars,
            'start': p.get('start') or (fmt_ms(start_ts) if start_ts else '-'),
            'end': p.get('end') or (fmt_ms(end_ts) if end_ts else '-'),
            'drawdownWindowBars': dd_window,
            'leverage': leverage,
            'feeBps': fee_bps,
            'slippageBps': slippage_bps,
            'barMillis': bar_ms,
            'systemThreshold': system_threshold,
            'profile': profile_name,
        },
        'range': {
            'start_ms': int(to_num(start_ts, 0)),
            'end_ms': int(to_num(end_ts, 0)),
            'start_ts': fmt_ms(start_ts) if start_ts else '-',
            'end_ts': fmt_ms(end_ts) if end_ts else '-',
            'bars': len(candles),
        },
        'system': {
            'return_pct': system_bt.get('return_pct', 0.0),
            'annualized_return_pct': system_bt.get('annualized_return_pct', 0.0),
            'max_drawdown_pct': system_bt.get('max_drawdown_pct', 0.0),
            'rolling_drawdown': system_bt.get('rolling_drawdown', {}),
            'drawdown_period': system_bt.get('drawdown_period', {}),
            'trades': system_bt.get('trades', 0),
            'win_rate_pct': system_bt.get('win_rate_pct', 0.0),
            'avg_win_pct': system_bt.get('avg_win_pct', 0.0),
            'avg_loss_pct': system_bt.get('avg_loss_pct', 0.0),
            'reward_risk_ratio': system_bt.get('reward_risk_ratio'),
            'profit_factor': system_bt.get('profit_factor'),
            'expectancy_pct': system_bt.get('expectancy_pct', 0.0),
            'sharpe_like': system_bt.get('sharpe_like', 0.0),
            'calmar_like': system_bt.get('calmar_like'),
            'costs': system_bt.get('costs', {}),
            'curve': system_bt.get('curve', []),
            'signal': _downsample_points([
                {'ts': int(to_num(c.get('ts'), 0)), 'value': int(system_signal[i]) if i < len(system_signal) else 0}
                for i, c in enumerate(candles)
            ], 360),
        },
        'benchmark': {
            'return_pct': benchmark_bt.get('return_pct', 0.0),
            'annualized_return_pct': benchmark_bt.get('annualized_return_pct', 0.0),
            'max_drawdown_pct': benchmark_bt.get('max_drawdown_pct', 0.0),
            'curve': benchmark_bt.get('curve', []),
        },
        'liquidation_only': liq_summary,
        'excess': {
            'return_pct': round(to_num(system_bt.get('return_pct'), 0.0) - to_num(benchmark_bt.get('return_pct'), 0.0), 4),
            'rolling_7d_outperform_rate_pct': rolling_7d.get('rate_pct', 0.0),
            'rolling_30d_outperform_rate_pct': rolling_30d.get('rate_pct', 0.0),
        },
        'advisor': {
            'profile': profile_name,
            'profile_label': BACKTEST_PROFILES.get(profile_name, {}).get('label', profile_name),
            'description': profile_descriptions.get(profile_name, profile_descriptions['regime_balance_v4']),
            'cluster_latest': {
                key: round(to_num((stable_pack.get('clusters') or {}).get(key, [0.0])[-1] if (stable_pack.get('clusters') or {}).get(key) else 0.0), 4)
                for key in ['benchmark_regime', 'trend_stack', 'flow_impulse', 'carry_regime', 'risk_pressure']
            },
            'max_hold_bars': stable_pack.get('max_hold_bars', {}),
        },
        'window_scan': window_scan,
        'factors': factor_results,
    }

    try:
        result.setdefault('source', {})['aux'] = (aux or {}).get('source_meta', {})
    except Exception:
        pass

    # charts for dedicated factor visualization
    xmc_factor = list(fac.get('cross_market_correlation') or [0.0] * n)
    xmc_corr = list((aux.get('series') or {}).get('cross_market_corr') or [0.0] * n)
    if len(xmc_factor) < n:
        xmc_factor.extend([xmc_factor[-1] if xmc_factor else 0.0] * (n - len(xmc_factor)))
    if len(xmc_corr) < n:
        xmc_corr.extend([xmc_corr[-1] if xmc_corr else 0.0] * (n - len(xmc_corr)))
    xmc_kline: List[Dict[str, Any]] = []
    for i, c in enumerate(candles):
        xmc_kline.append({
            'ts': int(to_num(c.get('ts'), 0)),
            'open': to_num(c.get('open'), 0.0),
            'high': to_num(c.get('high'), 0.0),
            'low': to_num(c.get('low'), 0.0),
            'close': to_num(c.get('close'), 0.0),
            'factor': round(to_num(xmc_factor[i], 0.0), 6),
            'corr': round(to_num(xmc_corr[i], 0.0), 6),
        })

    mkr_factor = list(fac.get('macro_risk_sentiment') or [0.0] * n)
    mkr_vix = list((aux.get('series') or {}).get('macro_vix') or [20.0] * n)
    mkr_dxy = list((aux.get('series') or {}).get('macro_dxy_5d_change') or [0.0] * n)
    if len(mkr_factor) < n:
        mkr_factor.extend([mkr_factor[-1] if mkr_factor else 0.0] * (n - len(mkr_factor)))
    if len(mkr_vix) < n:
        mkr_vix.extend([mkr_vix[-1] if mkr_vix else 20.0] * (n - len(mkr_vix)))
    if len(mkr_dxy) < n:
        mkr_dxy.extend([mkr_dxy[-1] if mkr_dxy else 0.0] * (n - len(mkr_dxy)))

    macro_kline: List[Dict[str, Any]] = []
    for i, c in enumerate(candles):
        macro_kline.append({
            'ts': int(to_num(c.get('ts'), 0)),
            'open': to_num(c.get('open'), 0.0),
            'high': to_num(c.get('high'), 0.0),
            'low': to_num(c.get('low'), 0.0),
            'close': to_num(c.get('close'), 0.0),
            'factor': round(to_num(mkr_factor[i], 0.0), 6),
            'vix': round(to_num(mkr_vix[i], 0.0), 6),
            'dxy_5d_change': round(to_num(mkr_dxy[i], 0.0), 6),
        })

    fng_factor = list(fac.get('fear_greed') or [0.0] * n)
    fng_value = list((aux.get('series') or {}).get('fear_greed_value') or [50.0] * n)
    fng_delta = list((aux.get('series') or {}).get('fear_greed_delta') or [0.0] * n)
    if len(fng_factor) < n:
        fng_factor.extend([fng_factor[-1] if fng_factor else 0.0] * (n - len(fng_factor)))
    if len(fng_value) < n:
        fng_value.extend([fng_value[-1] if fng_value else 50.0] * (n - len(fng_value)))
    if len(fng_delta) < n:
        fng_delta.extend([fng_delta[-1] if fng_delta else 0.0] * (n - len(fng_delta)))

    fng_kline: List[Dict[str, Any]] = []
    for i, c in enumerate(candles):
        fng_kline.append({
            'ts': int(to_num(c.get('ts'), 0)),
            'open': to_num(c.get('open'), 0.0),
            'high': to_num(c.get('high'), 0.0),
            'low': to_num(c.get('low'), 0.0),
            'close': to_num(c.get('close'), 0.0),
            'factor': round(to_num(fng_factor[i], 0.0), 6),
            'value': round(to_num(fng_value[i], 0.0), 6),
            'delta': round(to_num(fng_delta[i], 0.0), 6),
        })

    base_kline: List[Dict[str, Any]] = []
    for c in candles:
        base_kline.append({
            'ts': int(to_num(c.get('ts'), 0)),
            'open': to_num(c.get('open'), 0.0),
            'high': to_num(c.get('high'), 0.0),
            'low': to_num(c.get('low'), 0.0),
            'close': to_num(c.get('close'), 0.0),
        })

    result['charts'] = {
        'base_kline': _downsample_points(base_kline, 1200),
        'factor_series': factor_series,
        'factor_meta': factor_meta_rows,
        'cross_market_kline': _downsample_points(xmc_kline, 260),
        'cross_market_daily': _downsample_points(list(aux.get('cross_market_daily') or []), 420),
        'macro_risk_kline': _downsample_points(macro_kline, 260),
        'macro_risk_daily': _downsample_points(list(aux.get('macro_risk_daily') or []), 420),
        'fear_greed_kline': _downsample_points(fng_kline, 260),
        'fear_greed_daily': _downsample_points(list(aux.get('fear_greed_daily') or []), 420),
    }

    write_json(BACKTEST_CACHE_FILE, result)
    return result


def build_status() -> Dict[str, Any]:
    state = read_json(STATUS_FILE, {})
    trades = read_json(TRADES_FILE, [])
    decisions = read_json(DECISIONS_FILE, [])
    evolutions = read_json(EVOLUTION_FILE, [])
    cost_ledger = read_json(COST_LEDGER_FILE, [])

    if not isinstance(state, dict):
        state = {}
    if not isinstance(trades, list):
        trades = []
    if not isinstance(decisions, list):
        decisions = []
    if not isinstance(evolutions, list):
        evolutions = []
    if not isinstance(cost_ledger, list):
        cost_ledger = []

    factors_obj = state.get('factors') or {}
    factors = []
    for key, f in factors_obj.items():
        if key == 'whale_flow' or not isinstance(f, dict):
            continue
        details = f.get('details') if isinstance(f.get('details'), dict) else {}
        exp = explain(key, details)
        meta = factor_meta(key)
        guide_lines = exp.get('guide') if isinstance(exp, dict) else []
        if not isinstance(guide_lines, list):
            guide_lines = []
        runtime_lines = [describe_factor_detail(k, v) for k, v in details.items()]
        factors.append({
            'key': key,
            'name': meta.get('label') or f.get('name') or key,
            'score': round(to_num(f.get('score'), 50), 2),
            'confidence': round(to_num(f.get('confidence'), 50), 2),
            'direction': f.get('direction') or 'neutral',
            'recommendation': recommendation(str(f.get('direction') or 'neutral')),
            'logic': str((exp or {}).get('logic', '\u53c2\u4e0e\u7efc\u5408\u8bc4\u5206')),
            'story': factor_story(key, round(to_num(f.get('score'), 50), 2), round(to_num(f.get('confidence'), 50), 2), str(f.get('direction') or 'neutral'), details),
            'group': meta.get('group') or 'aux',
            'thesis': meta.get('thesis') or '',
            'metrics': (guide_lines + runtime_lines)[:28],
        })
    factors.sort(key=lambda x: x['score'], reverse=True)

    groups = state.get('factor_groups') if isinstance(state.get('factor_groups'), dict) else default_factor_groups()
    main_keys = set(groups.get('main') or [])
    aux_keys = set(groups.get('aux') or [])
    main_factors = [x for x in factors if x.get('key') in main_keys] or [x for x in factors if x.get('group') == 'main']
    aux_factors = [x for x in factors if x.get('key') in aux_keys] or [x for x in factors if x.get('group') != 'main']
    benchmark_context = state.get('benchmark_context') if isinstance(state.get('benchmark_context'), dict) else {}
    strategy_advisor = state.get('strategy_advisor') if isinstance(state.get('strategy_advisor'), dict) else {}

    # Ensure new Stock Market Pro factors are always shown
    new_factor_names = ['cross_market_correlation', 'macro_risk_sentiment', 'enhanced_technical']
    new_factors = [x for x in factors if x['key'] in new_factor_names]
    other_factors = [x for x in factors if x['key'] not in new_factor_names]
    
    # Combine: top 5 from others + all new factors (up to 12 total)
    radar_factors = other_factors[:5] + new_factors
    radar_labels = [x['name'] for x in radar_factors[:12]]
    radar_scores = [x['score'] for x in radar_factors[:12]]

    mode = load_mode()
    live_or_demo = get_okx_account('live' if mode == 'live' else 'paper')
    paper = get_paper_account()
    trader_monitor = build_trader_monitor(trades)
    trade_history = build_trade_lifecycle(trades, cost_ledger, 220)
    close_rows = [x for x in trade_history if str(x.get('status') or '') in {'close', 'partial_close'}]
    if not close_rows:
        fallback_rows = build_trade_lifecycle_from_trades(trades, 220)
        if fallback_rows:
            trade_history = fallback_rows
    today_trade_history = filter_today_trade_lifecycle(trade_history)

    processing = "\u5f85\u547d"
    msg = str(((state.get('last_execution') or {}).get('message')) or '').lower()
    if msg and msg != 'hold':
        processing = "\u5904\u7406\u4e2d"

    script_mtime = '-'
    try:
        script_mtime = fmt_ms(int(os.path.getmtime('/root/.okx-paper/ai_trader_v3_1.py') * 1000))
    except Exception:
        script_mtime = '-'

    bt_latest = read_json(BACKTEST_CACHE_FILE, {})
    bt_sys = (bt_latest or {}).get('system') if isinstance(bt_latest, dict) else {}
    bt_bm = (bt_latest or {}).get('benchmark') if isinstance(bt_latest, dict) else {}
    bt_ex = (bt_latest or {}).get('excess') if isinstance(bt_latest, dict) else {}
    bt_liq = (bt_latest or {}).get('liquidation_only') if isinstance(bt_latest, dict) else {}

    return {
        'timestamp': now_ms(),
        'mode': mode,
        'trader_status': state.get('trader_status', 'unknown'),
        'processing_state': processing,
        'next_check': state.get('next_check', ''),
        'signal': state.get('signal') or {},
        'risk': state.get('risk') or {},
        'trade_plan': state.get('trade_plan') or {},
        'active_strategy': state.get('active_strategy') or '',
        'strategy_tracks': state.get('strategy_tracks') or {},
        'adaptive': state.get('adaptive') or {},
        'paper_account': paper,
        'selected_account': live_or_demo,
        'factors': factors,
        'main_factors': main_factors,
        'aux_factors': aux_factors,
        'benchmark_context': benchmark_context,
        'strategy_advisor': strategy_advisor,
        'radar': {'labels': radar_labels, 'scores': radar_scores},
        'trade_history': trade_history,
        'trade_history_raw': list(reversed(trades[-120:])),
        'decision_history': list(reversed(decisions[-150:])),
        'evolution_history': list(reversed(evolutions[-120:])),
        'today_trade_history': today_trade_history[:120],
        'trader_runtime': trader_monitor.get('runtime', {}),
        'trader_recent_dialogue': trader_monitor.get('latest_dialogue', {}),
        'trader_events': trader_monitor.get('events', []),
        'trader_session_errors': trader_monitor.get('errors', []),
        'trader_memory_head': trader_monitor.get('memory_head', '-'),
        'backtest_latest': {
            'generated_at': (bt_latest or {}).get('generated_at'),
            'params': (bt_latest or {}).get('params') if isinstance(bt_latest, dict) else {},
            'system': {
                'return_pct': (bt_sys or {}).get('return_pct'),
                'annualized_return_pct': (bt_sys or {}).get('annualized_return_pct'),
                'max_drawdown_pct': (bt_sys or {}).get('max_drawdown_pct'),
                'trades': (bt_sys or {}).get('trades'),
                'profit_factor': (bt_sys or {}).get('profit_factor'),
            },
            'benchmark': {
                'return_pct': (bt_bm or {}).get('return_pct'),
                'annualized_return_pct': (bt_bm or {}).get('annualized_return_pct'),
                'max_drawdown_pct': (bt_bm or {}).get('max_drawdown_pct'),
            },
            'excess': {
                'return_pct': (bt_ex or {}).get('return_pct'),
                'rolling_7d_outperform_rate_pct': (bt_ex or {}).get('rolling_7d_outperform_rate_pct'),
                'rolling_30d_outperform_rate_pct': (bt_ex or {}).get('rolling_30d_outperform_rate_pct'),
            },
            'liquidation_only': {
                'ok': bool((bt_liq or {}).get('ok')),
                'profile': (bt_liq or {}).get('profile'),
                'profile_label': (bt_liq or {}).get('profile_label'),
                'return_pct': (bt_liq or {}).get('return_pct'),
                'max_drawdown_pct': (bt_liq or {}).get('max_drawdown_pct'),
                'trades': (bt_liq or {}).get('trades'),
                'virtual_trade_rows': list((bt_liq or {}).get('virtual_trade_rows') or [])[:120],
            },
            'window_scan': ((bt_latest or {}).get('window_scan') or [])[:4],
        },
        'release_notes': {
            'version': RELEASE_VERSION,
            'date': RELEASE_DATE,
            'title_zh': RELEASE_TITLE_ZH,
            'title_en': RELEASE_TITLE_EN,
            'highlights_zh': list(RELEASE_NOTES_ZH),
            'highlights_en': list(RELEASE_NOTES_EN),
            'files': list(RELEASE_FILES),
            'script_mtime': script_mtime,
        },
        'error_logs': {
            'app': tail_file(AI_LOG_FILE, 40),
            'stderr': tail_file(AI_STDERR_FILE, 40),
            'state_errors': list((state.get('last_errors') or []))[-20:],
        },
    }


def run_ai_decision_once() -> Dict[str, Any]:
    try:
        cp = subprocess.run(['python3', '/root/.okx-paper/ai_trader_v3_1.py', '--once'], capture_output=True, text=True, timeout=180)
        state = read_json(STATUS_FILE, {})
        last = (state or {}).get('last_decision') or {}
        return {
            'ok': cp.returncode == 0,
            'returncode': cp.returncode,
            'stdout': (cp.stdout or '')[-500:],
            'stderr': (cp.stderr or '')[-500:],
            'last_decision': last,
            'active_strategy': (state or {}).get('active_strategy'),
            'strategy_tracks': (state or {}).get('strategy_tracks'),
        }
    except Exception as e:
        return {'ok': False, 'error': str(e)}


HTML = """<!doctype html>
<html lang=\"zh-CN\"><head>
<meta charset=\"utf-8\" />
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />
<title>OpenClaw OKX</title>
<style>
:root{--bg:#fff;--fg:#111;--muted:#666;--bd:#ddd}
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--fg);font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Arial,sans-serif}
.wrap{max-width:none;width:100%;margin:0;padding:14px 18px}.card{border:1px solid var(--bd);border-radius:12px;padding:12px;background:#fff}
.grid{display:grid;gap:10px}.top{grid-template-columns:repeat(auto-fit,minmax(140px,1fr))}.two{grid-template-columns:2fr 1fr}.factors{grid-template-columns:repeat(auto-fit,minmax(320px,1fr))}
.muted{color:var(--muted);font-size:12px}.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap} h1{font-size:22px;margin:0 0 6px} h2{font-size:16px;margin:0 0 8px}
button{border:1px solid #bbb;background:#fff;padding:6px 10px;border-radius:8px;cursor:pointer}button.active{background:#111;color:#fff;border-color:#111} input,select{border:1px solid #bbb;background:#fff;padding:6px 8px;border-radius:8px}
table{width:100%;border-collapse:collapse;font-size:12px}th,td{border-bottom:1px solid #eee;padding:6px;text-align:left;vertical-align:top}
.tag{display:inline-block;padding:2px 8px;border:1px solid #bbb;border-radius:999px;font-size:12px}
.bull{color:#0a7d2f}.bear{color:#b42318}.neu{color:#555}
.scroll{max-height:280px;overflow:auto;border:1px solid #eee;border-radius:10px}
.event{padding:8px;border-bottom:1px solid #eee}.event .meta{font-size:12px;color:var(--muted)}.event .txt{margin-top:4px;font-size:13px}
.kv{display:grid;grid-template-columns:140px 1fr;gap:6px 10px;font-size:13px}.kv div:nth-child(odd){color:var(--muted)}
canvas{width:100%;height:320px;border:1px solid #eee;border-radius:10px}#radar{max-width:440px}
.exchart{position:relative;width:100%;height:430px;border:1px solid #1c2230;border-radius:10px;background:#0f131d;overflow:hidden}
.exchart-inner{position:absolute;inset:0}
.exchart-tooltip{position:absolute;z-index:8;left:12px;top:12px;max-width:360px;background:rgba(9,12,18,.88);color:#dfe6f4;border:1px solid rgba(112,132,170,.4);border-radius:8px;padding:8px 10px;font-size:12px;line-height:1.45;pointer-events:none;display:none;white-space:pre-line;box-shadow:0 8px 24px rgba(0,0,0,.35)}
.chart-toolbar{display:flex;gap:8px;align-items:center;justify-content:space-between;margin:2px 0 8px}
.chart-btn{border:1px solid #c9c9c9;background:#fff;padding:4px 8px;border-radius:7px;font-size:12px;cursor:pointer}
.chart-btn:hover{border-color:#888}
.chart-btn.active{background:#111;color:#fff;border-color:#111}
.exchart-empty{display:flex;align-items:center;justify-content:center;width:100%;height:100%;color:#9ca7bf;font-size:13px}
.factor-chip{display:inline-flex;align-items:center;gap:4px;border:1px solid #d2d2d2;background:#fff;border-radius:999px;padding:4px 8px;font-size:12px}
.factor-chip input{margin:0}
.factor-chip span{white-space:nowrap}
.perf-chip{display:inline-flex;align-items:center;gap:6px;border:1px solid #d2d2d2;background:#fff;border-radius:999px;padding:4px 9px;font-size:12px}
#btPerfTerminal{height:390px}
.guide-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px}
.guide-item{border:1px solid #ececec;border-radius:10px;background:#fafafa;padding:8px}
.guide-title{font-size:12px;color:#666}
.guide-value{font-size:14px;color:#111;margin-top:2px}
.guide-note{font-size:12px;color:#555;line-height:1.5}
.legend-dot{display:inline-block;width:9px;height:9px;border-radius:999px;margin-right:6px;vertical-align:middle}
.factor-state-row{display:grid;grid-template-columns:1.2fr 0.7fr 0.7fr 0.8fr;gap:6px;padding:6px 0;border-bottom:1px dashed #ececec;font-size:12px}
.factor-state-row.h{font-weight:700;color:#333;border-bottom:1px solid #ddd}
.factor-state-row:last-child{border-bottom:0}
.layout{display:grid;grid-template-columns:220px minmax(0,1fr);gap:12px;align-items:start}
.side{position:sticky;top:8px;border:1px solid #ddd;border-radius:12px;background:#fafafa;padding:8px}
.side .nav{display:block;text-decoration:none;color:#222;padding:8px 10px;border-radius:8px;margin-bottom:6px;border:1px solid transparent;font-size:13px}
.side .nav.active{background:#111;color:#fff;border-color:#111}
.view{display:none}
.view.active{display:block}
.terminal-layout{display:grid;grid-template-columns:2fr 1fr;gap:10px;margin-top:8px}
.terminal-left,.terminal-right{display:grid;gap:10px;align-content:start}
@media (max-width:1100px){.layout{grid-template-columns:1fr}.side{position:static;display:flex;gap:6px;overflow:auto;white-space:nowrap}.side .nav{margin:0}}
@media (max-width:1100px){.terminal-layout{grid-template-columns:1fr}}
@media (max-width:900px){.two,.factors{grid-template-columns:1fr}#btPerfTerminal{height:330px}}

.claw-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.claw-kv{display:grid;grid-template-columns:160px 1fr;gap:6px 10px;align-items:start}
.claw-param-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px}
.claw-param{border:1px solid #e6e6e6;border-radius:10px;padding:8px;background:#fff}
.claw-param .name{font-size:12px;color:#333;font-weight:600;margin-bottom:4px}
.claw-param .hint{font-size:11px;color:#777;margin-top:4px;line-height:1.35}
.claw-param input,.claw-param select,.claw-param textarea{width:100%;box-sizing:border-box;border:1px solid #d6d6d6;border-radius:8px;padding:6px 8px;background:#fff;font-size:12px}
.claw-param textarea{min-height:110px;resize:vertical}
.claw-result-grid{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(0,1fr);gap:10px;align-items:start;margin-top:8px}
#claw402Result,#claw402Explain{margin:0;white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.45;max-height:520px;overflow:auto;background:#fafafa;border:1px solid #eee;border-radius:10px;padding:10px}
#claw402Explain{overflow:auto}
.claw-exp-head{font-size:13px;font-weight:700;color:#222;margin-bottom:6px}
.claw-exp-row{margin:0 0 6px;line-height:1.45}
.claw-exp-key{font-weight:600;color:#222}
.claw-exp-val{color:#333}
.claw-exp-sub{font-size:11px;color:#666;line-height:1.4;margin-top:2px}
.claw-exp-sep{height:1px;background:#e7e7e7;margin:8px 0}
#claw402RecentTbl td.ok{color:#0f7a31;font-weight:700}
#claw402RecentTbl td.bad{color:#b42318;font-weight:700}
#claw402TplBtns,#claw402FavList{display:flex;gap:6px;flex-wrap:wrap;align-items:center}
.claw-chip{border:1px solid #d4d4d4;background:#fff;color:#222;border-radius:999px;padding:5px 10px;font-size:12px;cursor:pointer;line-height:1.2}
.claw-chip:hover{background:#f6f6f6}
.claw-chip.active{border-color:#111;background:#111;color:#fff}
.claw-chip.danger{border-color:#e7b8b8;color:#9f1d1d;background:#fff6f6}
@media (max-width:1200px){.claw-result-grid{grid-template-columns:1fr}}
@media (max-width:900px){.claw-grid{grid-template-columns:1fr}.claw-kv{grid-template-columns:120px 1fr}}
</style></head><body><div class=\"wrap\">
  <div class=\"row\" style=\"margin-bottom:8px\">
    <a class=\"tag\" href=\"/\">COEVO 主页</a>
    <a class=\"tag\" href=\"/okx\">OKX策略交易实例</a>
    <a class=\"tag\" href=\"/xhs\">小红书实例</a>
    <a class=\"tag\" href=\"/main\">&#20027;Agent&#23454;&#20363;</a>
  </div>
  <h1 id=\"title\"></h1>
  <div class=\"row\" style=\"margin-bottom:10px\">
    <button id=\"btnPaper\"></button>
    <button id=\"btnLive\"></button>
    <button id=\"btnAIDecision\"></button>
    <button id=\"btnLang\"></button>
    <span class=\"tag\" id=\"modeTag\">mode</span>
    <span class=\"muted\" id=\"updated\"></span>
  </div>
  <div class="layout">
    <aside class="side" id="okxSideNav">
      <a class="nav active" href="#overview" data-view="overview">&#24635;&#35272;</a>
      <a class="nav" href="#backtest" data-view="backtest">&#22238;&#27979;&#19982;&#22238;&#25764;</a>
      <a class="nav" href="#trades" data-view="trades">&#20132;&#26131;&#35760;&#24405;</a>
      <a class="nav" href="#liqfactor" data-view="liqfactor">清算因子</a>
      <a class="nav" href="#session" data-view="session">&#20250;&#35805;&#30417;&#25511;</a>
      <a class="nav" href="#claw402" data-view="claw402">Claw402 API</a>
      <a class="nav" href="#errors" data-view="errors">&#38169;&#35823;&#26085;&#24535;</a>
    </aside>
    <div>
      <section id="view-overview" class="view active">
        <div class="card" style="margin-top:0">
          <h2>&#26032;&#25163;&#24555;&#36895;&#25351;&#24341;(&#24314;&#35758;&#20808;&#30475;)</h2>
          <div id="noviceGuide" class="guide-grid"></div>
          <div id="noviceGuideNote" class="guide-note" style="margin-top:8px"></div>
          <details style="margin-top:8px">
            <summary style="cursor:pointer;color:#333">&#26415;&#35821;&#23567;&#35789;&#20856;(&#28857;&#20987;&#23637;&#24320;)</summary>
            <div id="noviceTerms" class="guide-grid" style="margin-top:8px"></div>
          </details>
        </div>
        <div class="card" style="margin-top:10px">
          <h2 id="releaseTitle"></h2>
          <div id="releaseBox" class="guide-note">-</div>
        </div>
        <div id="top" class="grid top"></div>
        <div class="grid two" style="margin-top:10px">
          <div class="card"><h2>当前策略建议</h2><div id="advisorBox" class="guide-grid"></div><div id="advisorNotes" class="guide-note" style="margin-top:8px">-</div></div>
          <div class="card"><h2>BTC基准过滤</h2><div id="benchmarkBox" class="kv"></div><div id="clusterBox" class="guide-grid" style="margin-top:8px"></div></div>
        </div>
        <div class="grid two" style="margin-top:10px">
          <div class="card"><h2 id="accountTitle"></h2><div id="acct" class="muted">loading...</div><div style="margin-top:10px;overflow:auto"><table id="balTbl"></table></div><div style="margin-top:10px;overflow:auto"><table id="fundTbl"></table></div><div style="margin-top:10px;overflow:auto"><table id="posTbl"></table></div></div>
          <div class="card"><h2 id="radarTitle"></h2><canvas id="radar" width="440" height="330"></canvas><div class="muted" id="trackInfo" style="margin-top:8px"></div></div>
        </div>
        <div class="card" style="margin-top:10px"><h2 id="factorTitle"></h2><div id="factors"></div></div>
      </section>

      <section id="view-backtest" class="view">
        <div class="card" id="backtestPanel" style="margin-top:0">
          <h2>&#30495;&#23454;K&#32447;&#22238;&#27979;&#19982;&#22238;&#25764;&#20998;&#26512;</h2>
          <div class="row" style="margin-bottom:8px">
            <span class="muted">&#20132;&#26131;&#23545;</span><input id="btInst" value="BTC-USDT-SWAP" style="width:170px" />
            <span class="muted">&#21608;&#26399;</span>
            <select id="btBar">
              <option value="5m">5m</option>
              <option value="15m">15m</option>
              <option value="1H" selected>1H</option>
              <option value="4H">4H</option>
              <option value="1D">1D</option>
            </select>
            <span class="muted">策略画像</span>
            <select id="btProfile">
              <option value="regime_balance_v4" selected>稳健多状态 v4.1</option>
              <option value="balanced_alpha">平衡阿尔法</option>
              <option value="defensive_cash">防守现金</option>
              <option value="liquidation_reversal_v1">清算反向 v1</option>
            </select>
            <span class="muted">&#24320;&#22987;</span><input id="btStart" type="datetime-local" />
            <span class="muted">&#32467;&#26463;</span><input id="btEnd" type="datetime-local" />
            <span class="muted">K&#32447;&#26465;&#25968;</span><input id="btBars" type="number" value="3000" min="300" max="12000" style="width:110px" />
            <span class="muted">&#22238;&#25764;&#31383;&#21475;(&#26465;)</span><input id="btDDWin" type="number" value="96" min="5" max="2000" style="width:110px" />
            <button id="btnBacktestRun">&#19968;&#38190;&#22238;&#27979;</button>
          </div>
          <div class="row" style="margin-bottom:8px">
            <span class="muted">&#26464;&#26438;</span><input id="btLev" type="number" value="1.02" min="0.2" max="8" step="0.01" style="width:80px" />
            <span class="muted">&#25163;&#32493;&#36153;(bps)</span><input id="btFee" type="number" value="5" min="0" max="50" step="0.1" style="width:90px" />
            <span class="muted">&#28369;&#28857;(bps)</span><input id="btSlip" type="number" value="3" min="0" max="50" step="0.1" style="width:90px" />
            <span class="tag" id="btStatus">&#26410;&#25191;&#34892;</span>
            <span class="muted" id="btSource">&#25968;&#25454;&#28304;: -</span>
          </div>
          <div class="terminal-layout">
            <div class="terminal-left">
              <div class="card">
                <h2>&#20840;&#22240;&#23376;&#21472;&#21152;K&#32447;&#22270;(&#21487;&#33258;&#30001;&#36873;&#25321;)</h2>
                <div class="chart-toolbar">
                  <div class="muted" id="btUnifiedInfo">-</div>
                  <div class="row" style="gap:6px">
                    <button class="chart-btn" id="btFactorAll">&#20840;&#36873;</button>
                    <button class="chart-btn" id="btFactorNone">&#28165;&#31354;</button>
                    <button class="chart-btn" data-reset-chart="btUnifiedKline">&#37325;&#32622;&#35270;&#22270;</button>
                  </div>
                </div>
                <div id="btFactorSelector" class="row" style="gap:6px;align-items:flex-start;flex-wrap:wrap;margin-bottom:8px"></div>
                <div id="btUnifiedLegend" class="row" style="gap:6px;align-items:flex-start;flex-wrap:wrap;margin-bottom:8px"></div>
                <div id="btUnifiedExplain" style="margin-bottom:8px"></div>
                <div id="btUnifiedGuide" class="guide-note" style="margin-bottom:8px"></div>
                <div id="btUnifiedKline" class="exchart"></div>
              </div>
              <div class="card">
                <h2>&#31995;&#32479;&#36164;&#37329;&#32456;&#31471;(&#20132;&#26131;&#25152;&#39118;&#26684;)</h2>
                <div class="chart-toolbar">
                  <div class="muted" id="btPerfInfo">-</div>
                  <div class="row" style="gap:6px">
                    <button class="chart-btn" data-perf-range="all">ALL</button>
                    <button class="chart-btn" data-perf-range="30d">30D</button>
                    <button class="chart-btn" data-perf-range="14d">14D</button>
                    <button class="chart-btn" data-perf-range="7d">7D</button>
                    <button class="chart-btn" id="btPerfReset">&#37325;&#32622;&#35270;&#22270;</button>
                  </div>
                </div>
                <div id="btPerfLegend" class="row" style="gap:6px;align-items:flex-start;flex-wrap:wrap;margin-bottom:8px"></div>
                <div id="btPerfTerminal" class="exchart"></div>
              </div>
            </div>
            <div class="terminal-right">
              <div class="card">
                <h2>&#24635;&#31995;&#32479;&#22238;&#27979;</h2>
                <div id="btSummary" class="kv"></div>
              </div>
              <div class="card">
                <h2>&#22238;&#25764;&#21306;&#38388;</h2>
                <div id="btDrawdown" class="kv"></div>
              </div>
              <div class="card"><h2>相对 BTC 现货</h2><div id="btBenchmark" class="kv"></div></div>
              <div class="card"><h2>回测策略说明</h2><div id="btAdvisor" class="guide-note">-</div></div>
            </div>
          </div>
          <div class="card" style="margin-top:8px">
            <h2>历史窗口扫描</h2>
            <div id="btWindowScan" style="overflow:auto"></div>
          </div>
          <div class="card" style="margin-top:8px">
            <h2>&#21333;&#22240;&#23376;&#22238;&#27979;&#19982;&#22238;&#25764;</h2>
            <div style="overflow:auto"><table id="btFactorTbl"></table></div>
          </div>
        </div>
      </section>

      <section id="view-trades" class="view">
        <div class="grid two" style="margin-top:0"><div class="card"><h2 id="tradeTitle"></h2><div style="overflow:auto"><table id="tradeTbl"></table></div></div><div class="card"><h2 id="evoTitle"></h2><div style="overflow:auto"><table id="evoTbl"></table></div></div></div>
        <div class="card" style="margin-top:10px">
          <h2>清算反向因子开仓记录（回测）</h2>
          <div class="guide-note" style="margin-bottom:8px">该表来自最近一次回测缓存，属于策略触发记录，不代表真实下单成交。</div>
          <div style="overflow:auto"><table id="liqBtTradeTbl"></table></div>
        </div>
        <div class="grid two" style="margin-top:10px"><div class="card"><h2>&#20170;&#26085;&#20132;&#26131;&#35760;&#24405;</h2><div style="overflow:auto"><table id="todayTradeTbl"></table></div></div><div class="card"><h2>&#20250;&#35805;&#36816;&#34892;&#20449;&#24687;</h2><div id="traderRuntime" class="kv"></div></div></div>
      </section>

      <section id="view-liqfactor" class="view">
        <div class="card" style="margin-top:0">
          <h2>清算因子总览</h2>
          <div id="liqFactorSummary" class="kv"></div>
          <div id="liqFactorExplain" class="guide-note" style="margin-top:8px">-</div>
        </div>
        <div class="grid two" style="margin-top:10px">
          <div class="card">
            <h2>清算因子真实成交记录</h2>
            <div class="guide-note" style="margin-bottom:8px">该表仅展示策略命中“清算反向因子”的真实成交流水。</div>
            <div style="overflow:auto"><table id="liqFactorRealTbl"></table></div>
          </div>
          <div class="card">
            <h2>清算因子回测触发记录</h2>
            <div class="guide-note" style="margin-bottom:8px">该表来自最近一次回测缓存，属于策略触发记录，不代表真实下单成交。</div>
            <div style="overflow:auto"><table id="liqFactorBtTbl"></table></div>
          </div>
        </div>
      </section>

      <section id="view-session" class="view">
        <div class="grid two" style="margin-top:0"><div class="card"><h2>&#26368;&#36817;&#19968;&#27425;&#23545;&#35805;&#20449;&#24687;</h2><div id="latestDialogue" class="kv"></div></div><div class="card"><h2>MEMORY &#25688;&#35201;</h2><pre id="traderMemory" style="margin:0;white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.45;max-height:280px;overflow:auto;background:#fafafa;border:1px solid #eee;border-radius:10px;padding:10px">-</pre></div></div>
        <div class="card" style="margin-top:10px"><h2>&#26368;&#36817;&#20107;&#20214;&#27969;</h2><div id="traderEvents" class="scroll"></div></div>
        <div class="card" style="margin-top:10px"><h2>&#20250;&#35805;&#38169;&#35823;&#26085;&#24535;&#25688;&#35201;</h2><pre id="traderSessionErrors" style="margin:0;white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.45;max-height:260px;overflow:auto;background:#fafafa;border:1px solid #eee;border-radius:10px;padding:10px">-</pre></div>
      </section>

      <section id="view-claw402" class="view">
        <div class="claw-grid" style="margin-top:0">
          <div class="card">
            <h2>Claw402 &#38065;&#21253;&#29366;&#24577;</h2>
            <div id="claw402WalletBox" class="claw-kv"></div>
          </div>
          <div class="card">
            <h2>&#35843;&#29992;&#32479;&#35745;</h2>
            <div id="claw402UsageBox" class="claw-kv"></div>
          </div>
        </div>
        <div class="card" style="margin-top:10px">
          <h2>Claw402 API &#35843;&#29992;&#21488;</h2>
          <div id="claw402CatalogStatus" class="guide-note">&#27491;&#22312;&#21152;&#36733; skill &#30446;&#24405;...</div>
          <div class="row" style="margin-top:8px">
            <span class="muted">&#20998;&#31867;</span>
            <select id="claw402Category" style="min-width:180px"></select>
            <span class="muted">&#25509;&#21475;</span>
            <select id="claw402Endpoint" style="min-width:320px;max-width:100%"></select>
            <button id="claw402RunBtn">&#25191;&#34892;&#35843;&#29992;</button>
            <button id="claw402RefreshBtn">&#21047;&#26032;&#20313;&#39069;/&#32479;&#35745;</button>
          </div>
          <div style="margin-top:8px">
            <div class="muted" style="margin-bottom:4px">&#24120;&#29992;&#27169;&#26495;</div>
            <div id="claw402TplBtns"></div>
          </div>
          <div style="margin-top:8px">
            <div class="row" style="gap:6px;align-items:center;margin-bottom:4px">
              <span class="muted">&#25105;&#30340;&#25910;&#34255;</span>
              <button id="claw402FavToggleBtn">&#25910;&#34255;&#24403;&#21069;&#25509;&#21475;</button>
              <button id="claw402FavClearBtn">&#28165;&#31354;&#25910;&#34255;</button>
            </div>
            <div id="claw402FavList"></div>
          </div>
          <div id="claw402ParamBox" class="claw-param-grid" style="margin-top:8px"></div>
          <div id="claw402PostWrap" style="display:none;margin-top:8px">
            <div class="muted" style="margin-bottom:4px">POST JSON &#35831;&#27714;&#20307;</div>
            <textarea id="claw402PostBody" style="width:100%;min-height:140px;border:1px solid #d6d6d6;border-radius:10px;padding:8px;box-sizing:border-box"></textarea>
          </div>
          <div id="claw402CmdPreview" class="guide-note" style="margin-top:8px">-</div>
          <div id="claw402ReqMeta" class="guide-note" style="margin-top:6px">-</div>
          <div class="claw-result-grid">
            <div>
              <div class="muted" style="margin-bottom:4px">&#21407;&#22987;&#36820;&#22238; JSON</div>
              <pre id="claw402Result">{}</pre>
            </div>
            <div>
              <div class="muted" style="margin-bottom:4px">&#20013;&#25991;&#35299;&#37322;</div>
              <div id="claw402Explain">&#31561;&#24453;&#35843;&#29992;&#21518;&#33258;&#21160;&#35299;&#35835;</div>
            </div>
          </div>
        </div>
        <div class="card" style="margin-top:10px">
          <h2>&#26368;&#36817;&#35843;&#29992;&#35760;&#24405;</h2>
          <div style="overflow:auto">
            <table id="claw402RecentTbl">
              <tr><th>&#26102;&#38388;</th><th>&#25509;&#21475;</th><th>&#32791;&#26102;(ms)</th><th>&#29366;&#24577;</th><th>&#38169;&#35823;</th></tr>
            </table>
          </div>
        </div>
      </section>

      <section id="view-errors" class="view">
        <div class="card" style="margin-top:0"><h2 id="errorTitle"></h2><pre id="errorBox" style="margin:0;white-space:pre-wrap;word-break:break-word;font-size:12px;line-height:1.45;max-height:560px;overflow:auto;background:#fafafa;border:1px solid #eee;border-radius:10px;padding:10px">-</pre></div>
      </section>
    </div>
  </div>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
<script>
let currentMode='paper';let currentLang=localStorage.getItem('lang')||'zh';
const TX={zh:{title:'OpenClaw OKX \u72ec\u7acb\u76d1\u63a7\u9762\u677f',paper:'\u6a21\u62df\u76d8',live:'\u5b9e\u76d8',ai:'AI\u51b3\u7b56',lang:'English',modePaper:'\u6a21\u62df\u76d8',modeLive:'\u5b9e\u76d8',account:'\u8d26\u6237\u6982\u89c8\uff08\u5f53\u524d\u6a21\u5f0f\uff09',radar:'\u56e0\u5b50\u7efc\u5408\u96f7\u8fbe\u56fe',factor:'\u56e0\u5b50\u660e\u7ec6',trade:'\u4ea4\u6613\u5386\u53f2',evo:'\u8fdb\u5316\u5386\u53f2',status:'\u7cfb\u7edf\u72b6\u6001',proc:'\u5904\u7406\u72b6\u6001',score:'\u7efc\u5408\u8bc4\u5206',conf:'\u7efc\u5408\u7f6e\u4fe1\u5ea6',track:'\u5f53\u524d\u7b56\u7565\u8f68\u9053',next:'next_check',tradingEq:'\u4ea4\u6613\u8d26\u6237\u6743\u76ca(USDT)',fundingEq:'\u8d44\u91d1\u8d26\u6237\u6743\u76ca(USDT)',totalEq:'\u603b\u6743\u76ca(USDT)',posVal:'\u6301\u4ed3\u4ef7\u503c(USDT)',profile:'\u914d\u7f6e',cfg:'\u914d\u7f6e\u72b6\u6001',yes:'\u5df2\u914d\u7f6e',no:'\u672a\u914d\u7f6e',direction:'\u65b9\u5411',logic:'\u903b\u8f91\u8bf4\u660e',noData:'\u65e0',intr:'\u77ed\u7ebf',sw:'\u957f\u7ebf',sel:'\u9009\u4e2d',error:'\u9519\u8bef\u65e5\u5fd7',noError:'\u6682\u65e0\u9519\u8bef'},en:{title:'OpenClaw OKX Independent Monitor',paper:'Paper',live:'Live',ai:'AI Decision',lang:'\u4e2d\u6587',modePaper:'Paper',modeLive:'Live',account:'Account Overview',radar:'Factor Radar',factor:'Factor Details',trade:'Trade History',evo:'Evolution History',status:'Trader Status',proc:'Processing',score:'Score',conf:'Confidence',track:'Active Track',next:'next_check',tradingEq:'Trading Equity (USDT)',fundingEq:'Funding Equity (USDT)',totalEq:'Total Equity (USDT)',posVal:'Position Value (USDT)',profile:'Profile',cfg:'Configured',yes:'Yes',no:'No',direction:'Direction',logic:'Logic',noData:'N/A',intr:'Intraday',sw:'Long-term',sel:'Selected',error:'Error Logs',noError:'No errors'}};
function t(k){return (TX[currentLang]||TX.zh)[k]||k}
function esc(s){return String((s===undefined||s===null)?'':s).replace(/[&<>\"]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[m]))}
function fmt(v){const n=Number(v);if(!Number.isFinite(n))return '--';if(Math.abs(n)>=1e6)return n.toLocaleString('en-US',{maximumFractionDigits:2});if(Math.abs(n)>=1)return n.toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});if(Math.abs(n)>=0.0001)return n.toFixed(6).replace(/0+$/,'').replace(/\\.$/,'');return n.toFixed(8).replace(/0+$/,'').replace(/\\.$/,'')}
function fmtTs(v){const s=String(v===undefined||v===null?'':v).trim();if(!s)return '--';const d=new Date(s);if(!Number.isNaN(d.getTime()))return d.toLocaleString('zh-CN',{hour12:false});return s.substring(0,19)}
function trackName(n){if(currentLang!=='zh')return n||'--';if(n==='intraday'||n==='short_term_intraday')return '\u77ed\u7ebf';if(n==='swing'||n==='long_term_swing')return '\u957f\u7ebf';return n||'--'}
async function setMode(mode){await fetch('/api/mode',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode})});await load()}async function runAI(){const r=await fetch('/api/ai-decision',{method:'POST'});const d=await r.json();if(d&&d.last_decision){alert((currentLang==='zh'?'AI\u51b3\u7b56: ':'AI decision: ')+(d.last_decision.direction||'hold')+' | '+(currentLang==='zh'?'\u8bc4\u5206 ':'score ')+fmt(d.last_decision.score));}await load()}
function drawRadar(labels,scores){const c=document.getElementById('radar');const ctx=c.getContext('2d');const w=c.width,h=c.height,cx=w/2,cy=h/2,r=Math.min(w,h)*0.34;ctx.clearRect(0,0,w,h);const n=Math.max(3,labels.length||0);ctx.strokeStyle='#ddd';for(let lv=1;lv<=5;lv++){const rr=r*lv/5;ctx.beginPath();for(let i=0;i<n;i++){const a=-Math.PI/2+i*2*Math.PI/n;const x=cx+rr*Math.cos(a),y=cy+rr*Math.sin(a);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y)}ctx.closePath();ctx.stroke()}for(let i=0;i<n;i++){const a=-Math.PI/2+i*2*Math.PI/n;const x=cx+r*Math.cos(a),y=cy+r*Math.sin(a);ctx.beginPath();ctx.moveTo(cx,cy);ctx.lineTo(x,y);ctx.stroke();const lx=cx+(r+14)*Math.cos(a),ly=cy+(r+14)*Math.sin(a);ctx.fillStyle='#333';ctx.font='11px sans-serif';ctx.fillText(String(labels[i]||'').slice(0,8),lx-20,ly)}ctx.beginPath();for(let i=0;i<n;i++){const s=((scores[i]===undefined||scores[i]===null)?50:scores[i])/100;const rr=r*s;const a=-Math.PI/2+i*2*Math.PI/n;const x=cx+rr*Math.cos(a),y=cy+rr*Math.sin(a);if(i===0)ctx.moveTo(x,y);else ctx.lineTo(x,y)}ctx.closePath();ctx.fillStyle='rgba(17,17,17,0.18)';ctx.strokeStyle='#111';ctx.lineWidth=2;ctx.fill();ctx.stroke();}
function applyStatic(){
  document.getElementById('title').textContent=t('title');
  document.getElementById('btnPaper').textContent=t('paper');
  document.getElementById('btnLive').textContent=t('live');
  document.getElementById('btnAIDecision').textContent=t('ai');
  document.getElementById('btnLang').textContent=t('lang');
  document.getElementById('accountTitle').textContent=t('account');
  document.getElementById('radarTitle').textContent=t('radar');
  document.getElementById('factorTitle').textContent=t('factor');
  document.getElementById('tradeTitle').textContent=t('trade');
  document.getElementById('evoTitle').textContent=t('evo');
  const et=document.getElementById('errorTitle');
  if(et)et.textContent=t('error');
  const rt=document.getElementById('releaseTitle');
  if(rt)rt.textContent=currentLang==='zh'?'版本更新说明':'Release Notes';
}
function factorStrengthLabel(score){
  const s = Number(score||0);
  const dist = Math.abs(s - 50);
  if(dist >= 25) return '强';
  if(dist >= 12) return '中';
  return '弱';
}
function factorBandLabel(score){
  const s = Number(score||0);
  if(!Number.isFinite(s)) return '未知';
  if(s >= 65) return '明显偏多';
  if(s >= 55) return '轻度偏多';
  if(s <= 35) return '明显偏空';
  if(s <= 45) return '轻度偏空';
  return '中性观望';
}
function renderNoviceGuide(d){
  const wrap=document.getElementById('noviceGuide');
  const note=document.getElementById('noviceGuideNote');
  const terms=document.getElementById('noviceTerms');
  if(!wrap||!note||!terms) return;
  const sig=d.signal||{};
  const score=Number(sig.score||0);
  const conf=Number(sig.confidence||0);
  const dir=String(sig.direction||'hold');
  const dirTxt=dir==='bullish'?'偏多':dir==='bearish'?'偏空':'观望';
  const phase = score>=65?'信号较强':(score>=55?'信号一般':'信号偏弱');
  const actionHint = score>=65?'可重点关注“是否已触发风控后的开仓条件”':(score>=55?'先观察因子是否同向增多，再决定是否参与':'以观望为主，等待因子重新聚合');
  wrap.innerHTML=[
    ['当前综合信号', `${fmt(score)} / 100（${phase}）`],
    ['当前方向结论', `${dirTxt}（不是立即下单指令）`],
    ['当前置信度', `${fmt(conf)} / 100（越高越稳定）`],
    ['建议你现在做什么', actionHint],
  ].map(x=>`<div class='guide-item'><div class='guide-title'>${esc(x[0])}</div><div class='guide-value'>${esc(x[1])}</div></div>`).join('');
  note.textContent='读面板顺序建议：先看“当前综合信号” -> 再看“因子明细里是否多数同方向” -> 最后看“交易记录是否按预期执行”。回测页的因子值是 -1~1，实时页的因子评分是 0~100，两者含义不同。';
  terms.innerHTML=[
    ['因子', '把市场某个维度（动量、资金费率、情绪等）量化成一个可比较的数值。'],
    ['综合评分(0-100)', '实时决策层的总分，50附近表示中性，远离50表示方向更明确。'],
    ['置信度(0-100)', '当前结论的稳定程度，不代表收益保证。'],
    ['回测因子值(-1~1)', '-1偏空、0中性、+1偏多；仅用于历史评估，不是直接下单按钮。'],
    ['阈值', '因子值绝对值超过阈值后，才会被当作有效信号。'],
    ['最大回撤', '历史测试里从高点到低点的最大资金回落幅度。'],
    ['胜率', '历史测试中盈利交易占比，高胜率不等于高收益。'],
    ['Sharpe-like', '收益相对波动的比值，越高通常代表收益质量越好。'],
  ].map(x=>`<div class='guide-item'><div class='guide-title'>${esc(x[0])}</div><div class='guide-note'>${esc(x[1])}</div></div>`).join('');
}
function renderReleaseNotes(d){
  const box=document.getElementById('releaseBox');
  if(!box) return;
  const rn=d.release_notes||{};
  const title=currentLang==='zh'?(rn.title_zh||'-'):(rn.title_en||'-');
  const list=(currentLang==='zh'?(rn.highlights_zh||[]):(rn.highlights_en||[])).slice(0,12);
  const files=(rn.files||[]).slice(0,6);
  const version=rn.version||'-';
  const date=rn.date||'-';
  const updated=rn.script_mtime||'-';
  const listHtml=list.length?`<ul style="margin:6px 0 8px 18px;padding:0">${list.map(x=>`<li style="margin:4px 0">${esc(x)}</li>`).join('')}</ul>`:'<div>-</div>';
  const fileHtml=files.length?files.map(x=>`<div><code>${esc(x)}</code></div>`).join(''):'<div>-</div>';
  box.innerHTML=`
    <div><b>${esc(title)}</b></div>
    <div style="margin-top:4px">${currentLang==='zh'?'版本':'Version'}: <b>${esc(version)}</b> | ${currentLang==='zh'?'发布日期':'Date'}: ${esc(date)} | ${currentLang==='zh'?'策略文件更新时间':'Strategy mtime'}: ${esc(updated)}</div>
    <div style="margin-top:6px">${currentLang==='zh'?'本次更新内容':'What changed'}:</div>
    ${listHtml}
    <div>${currentLang==='zh'?'关联文件':'Related files'}:</div>
    ${fileHtml}
  `;
}
function renderErrors(d){const box=document.getElementById('errorBox');if(!box)return;const e=d.error_logs||{};const rows=[...(e.state_errors||[]),...(e.app||[]),...(e.stderr||[])].slice(-60);box.textContent=rows.length?rows.join('\\n'):t('noError');}
async function renderLiquidationHeatmap(){
  const wrap=document.getElementById('factors');
  if(!wrap) return;
  
  // 检查是否已经有清算热力图卡片
  let liqCard=document.getElementById('liquidation-heatmap-card');
  if(!liqCard){
    // 创建新卡片
    const div=document.createElement('div');
    div.id='liquidation-heatmap-card';
    div.className='card';
    div.innerHTML='<div style="font-weight:700">清算热力图因子</div><div class="muted">正在加载清算数据...</div>';
    wrap.appendChild(div);
    liqCard=div;
  }
  
  try{
    const resp=await fetch('/api/liquidation-heatmap?_='+Date.now(),{cache:'no-store'});
    if(!resp.ok) throw new Error('http_'+resp.status);
    const data=await resp.json();
    
    if(!data.ok){
      liqCard.innerHTML='<div style="font-weight:700">清算热力图因子</div><div class="muted">加载失败: '+esc(data.error||'unknown')+'</div>';
      return;
    }
    
    const score=data.score||50;
    const dir=data.direction||'neutral';
    const conf=data.confidence||20;
    const cls=dir==='long'?'bull':(dir==='short'?'bear':'neu');
    const dirTxt=dir==='long'?'偏多':(dir==='short'?'偏空':'中性');
    
    const details=data.details||{};
    const shortZones=details.short_zones||[];
    const longZones=details.long_zones||[];
    const nearestShort=details.nearest_short||{};
    const nearestLong=details.nearest_long||{};
    
    let metrics=[];
    metrics.push('当前价格: $'+fmt(data.current_price));
    metrics.push('观察带: 上下'+(details.observation_band_pct||3)+'%');
    metrics.push('空头清算密集区: '+(nearestShort.zone_count||0)+'个');
    metrics.push('多头清算密集区: '+(nearestLong.zone_count||0)+'个');
    
    if(nearestShort.nearest){
      const ns=nearestShort.nearest;
      metrics.push('最近空头区: $'+fmt(ns.price)+' (距离 '+fmt(ns.distance_pct)+'%)');
      if(ns.cascade_risk) metrics.push('⚠️ 连环密集区风险');
    }
    
    if(nearestLong.nearest){
      const nl=nearestLong.nearest;
      metrics.push('最近多头区: $'+fmt(nl.price)+' (距离 '+fmt(nl.distance_pct)+'%)');
      if(nl.cascade_risk) metrics.push('⚠️ 连环密集区风险');
    }
    
    const scoreBand=factorBandLabel(score);
    const strength=factorStrengthLabel(score);
    
    liqCard.innerHTML=`
      <div style="font-weight:700">清算热力图因子</div>
      <div class="muted ${cls}">角色: 辅助判断因子 | 方向: ${esc(dirTxt)} | 反向交易信号</div>
      <div>评分 ${fmt(score)} / 100（${esc(scoreBand)}） | 置信度 ${fmt(conf)} / 100</div>
      <div class='guide-note' style='margin-top:4px'>基于清算密集区识别，当价格接近大规模清算时，产生反向开单信号。</div>
      <details style='margin-top:6px'>
        <summary style='cursor:pointer;color:#444'>查看清算密集区详情</summary>
        <ul>${metrics.map(x=>'<li>'+esc(x)+'</li>').join('')}</ul>
      </details>
    `;
  }catch(e){
    liqCard.innerHTML='<div style="font-weight:700">清算热力图因子</div><div class="muted">加载失败: '+esc(e.message)+'</div>';
  }
  await ensureX402LiquidationSection(liqCard);
}

let liqX402Busy=false;
let liqSectionBootstrapped=false;
let liqX402LastData=null;
const LIQ_X402_CACHE_KEY='okx_x402_heatmap_cache_v1';
const LIQ_X402_LEVELS_KEY='okx_x402_levels_v1';
const LIQ_X402_VIEW_KEY='okx_x402_heatmap_view_v1';
const LIQ_X402_VIEWPORT_KEY='okx_x402_heatmap_viewport_v1';
const LIQ_X402_LAYOUT={left:56,right:78,top:16,bottom:28};
let liqX402Viewport={loaded:false,total:0,start:0,window:0,y_zoom:1,y_offset:0,data_key:''};
function _liqX402SanitizeSymbol(v){ const s=(v||'BTCUSDT').toUpperCase().replace(/[^A-Z0-9_-]/g,''); return s||'BTCUSDT'; }
function _liqX402SanitizeExchange(v){ const s=(v||'Binance').replace(/[^A-Za-z0-9_-]/g,''); return s||'Binance'; }
function _liqX402SanitizeInterval(v){ const allowed=['15m','30m','1h','2h','4h','6h','12h','1d','3d','1w']; return allowed.includes(v)?v:'1d'; }
function _liqX402GetTotalX(data){
  const heat=(data&&data.heatmap&&typeof data.heatmap==='object')?data.heatmap:{};
  const tBins=Math.max(1, Number(heat.time_bins||0));
  const cLen=Array.isArray(data&&data.candles)?data.candles.length:0;
  return Math.max(1, tBins, cLen);
}
function _liqX402ClampViewport(v, totalX){
  const total=Math.max(1, Number(totalX||1));
  const minWindow=Math.max(8, Math.min(80, total));
  const defWindow=Math.max(minWindow, Math.min(total, Math.round(total*0.72)));
  if(!Number.isFinite(v.window) || v.window<=0){ v.window=defWindow; }
  v.window=Math.max(minWindow, Math.min(total, Number(v.window)));
  if(!Number.isFinite(v.start)){ v.start=Math.max(0, total-v.window); }
  v.start=Math.max(0, Math.min(Math.max(0,total-v.window), Number(v.start)));
  v.y_zoom=Math.max(1, Math.min(6, Number(v.y_zoom||1)));
  v.y_offset=Math.max(-0.45, Math.min(0.45, Number(v.y_offset||0)));
  v.total=total;
  return v;
}
function _liqX402SaveViewport(){
  try{
    const v=_liqX402ClampViewport(liqX402Viewport, liqX402Viewport.total||1);
    sessionStorage.setItem(LIQ_X402_VIEWPORT_KEY, JSON.stringify({
      start:v.start, window:v.window, y_zoom:v.y_zoom, y_offset:v.y_offset, data_key:v.data_key||'',
    }));
  }catch(_){ }
}
function _liqX402LoadViewport(){
  try{
    const raw=sessionStorage.getItem(LIQ_X402_VIEWPORT_KEY);
    if(raw){
      const d=JSON.parse(raw)||{};
      liqX402Viewport.start=Number(d.start||0);
      liqX402Viewport.window=Number(d.window||0);
      liqX402Viewport.y_zoom=Number(d.y_zoom||1);
      liqX402Viewport.y_offset=Number(d.y_offset||0);
      liqX402Viewport.data_key=String(d.data_key||'');
    }
  }catch(_){ }
  liqX402Viewport.loaded=true;
}
function _liqX402ResetViewport(totalX, save){
  const total=Math.max(1, Number(totalX||1));
  const minWindow=Math.max(8, Math.min(80, total));
  const window=Math.max(minWindow, Math.min(total, Math.round(total*0.72)));
  liqX402Viewport.total=total;
  liqX402Viewport.window=window;
  liqX402Viewport.start=Math.max(0, total-window);
  liqX402Viewport.y_zoom=1;
  liqX402Viewport.y_offset=0;
  _liqX402ClampViewport(liqX402Viewport, total);
  if(save!==false) _liqX402SaveViewport();
}
function _liqX402PrepareViewport(data){
  if(!liqX402Viewport.loaded) _liqX402LoadViewport();
  const totalX=_liqX402GetTotalX(data||{});
  const key=`${(data&&data.exchange)||''}:${(data&&data.symbol)||''}:${(data&&data.interval)||''}:${totalX}`;
  if(liqX402Viewport.data_key!==key){
    _liqX402ResetViewport(totalX, false);
    liqX402Viewport.data_key=key;
  }else{
    _liqX402ClampViewport(liqX402Viewport, totalX);
  }
  _liqX402SaveViewport();
  return liqX402Viewport;
}
function _liqX402DefaultViewOpts(){ return { line_strength:100, bubble_size:100, show_k:true }; }
function _liqX402LoadViewOpts(){
  try{
    const raw=localStorage.getItem(LIQ_X402_VIEW_KEY);
    if(raw){
      const d=JSON.parse(raw);
      return {
        line_strength: Math.max(20, Math.min(220, Number((d||{}).line_strength||100))),
        bubble_size: Math.max(20, Math.min(220, Number((d||{}).bubble_size||100))),
        show_k: (d||{}).show_k !== false,
      };
    }
  }catch(_){ }
  return _liqX402DefaultViewOpts();
}
function _liqX402SaveViewOpts(opts){
  try{
    localStorage.setItem(LIQ_X402_VIEW_KEY, JSON.stringify({
      line_strength: Math.max(20, Math.min(220, Number((opts||{}).line_strength||100))),
      bubble_size: Math.max(20, Math.min(220, Number((opts||{}).bubble_size||100))),
      show_k: (opts||{}).show_k !== false,
    }));
  }catch(_){ }
}
function _liqX402SaveCache(data){
  try{
    if(data && data.ok){
      sessionStorage.setItem(LIQ_X402_CACHE_KEY, JSON.stringify({saved_at: Date.now(), data}));
    }
  }catch(_){ }
}
function _liqX402LoadCache(){
  if(liqX402LastData && liqX402LastData.ok) return;
  try{
    const raw=sessionStorage.getItem(LIQ_X402_CACHE_KEY);
    if(!raw) return;
    const obj=JSON.parse(raw);
    if(obj && obj.data && obj.data.ok){
      liqX402LastData=obj.data;
    }
  }catch(_){ }
}
function _liqX402SaveLevelPrefs(sel){
  try{
    localStorage.setItem(LIQ_X402_LEVELS_KEY, JSON.stringify({
      high: !!(sel&&sel.high),
      mid: !!(sel&&sel.mid),
      low: !!(sel&&sel.low),
    }));
  }catch(_){ }
}
function _liqX402LoadLevelPrefs(){
  try{
    const raw=localStorage.getItem(LIQ_X402_LEVELS_KEY);
    if(raw){
      const d=JSON.parse(raw);
      const out={
        high: !!(d&&d.high),
        mid: !!(d&&d.mid),
        low: !!(d&&d.low),
      };
      if(out.high||out.mid||out.low) return out;
    }
  }catch(_){ }
  return {high:true,mid:true,low:true};
}
function _liqX402ApplyLevelPrefs(){
  const pref=_liqX402LoadLevelPrefs();
  const high=document.getElementById('liqX402LvHigh');
  const mid=document.getElementById('liqX402LvMid');
  const low=document.getElementById('liqX402LvLow');
  if(high) high.checked=!!pref.high;
  if(mid) mid.checked=!!pref.mid;
  if(low) low.checked=!!pref.low;
}
function _liqX402ReadViewOptsFromDom(){
  const line=document.getElementById('liqX402LineStrength');
  const bubble=document.getElementById('liqX402BubbleSize');
  const showK=document.getElementById('liqX402ShowK');
  return {
    line_strength: Math.max(20, Math.min(220, Number(line&&line.value||100))),
    bubble_size: Math.max(20, Math.min(220, Number(bubble&&bubble.value||100))),
    show_k: !!(showK ? showK.checked : true),
  };
}
function _liqX402UpdateViewOptLabels(opts){
  const lv=document.getElementById('liqX402LineStrengthVal');
  const bv=document.getElementById('liqX402BubbleSizeVal');
  if(lv) lv.textContent=`${Math.round(Number((opts||{}).line_strength||100))}%`;
  if(bv) bv.textContent=`${Math.round(Number((opts||{}).bubble_size||100))}%`;
}
function _liqX402ApplyViewOptsToDom(){
  const opts=_liqX402LoadViewOpts();
  const line=document.getElementById('liqX402LineStrength');
  const bubble=document.getElementById('liqX402BubbleSize');
  const showK=document.getElementById('liqX402ShowK');
  if(line) line.value=String(opts.line_strength);
  if(bubble) bubble.value=String(opts.bubble_size);
  if(showK) showK.checked=!!opts.show_k;
  _liqX402UpdateViewOptLabels(opts);
}
function _liqX402SelectedLevels(){
  const high=document.getElementById('liqX402LvHigh');
  const mid=document.getElementById('liqX402LvMid');
  const low=document.getElementById('liqX402LvLow');
  return {
    high: !!(high && high.checked),
    mid: !!(mid && mid.checked),
    low: !!(low && low.checked),
  };
}
function _liqX402AnyLevelSelected(sel){
  return !!(sel && (sel.high || sel.mid || sel.low));
}
function _liqX402LevelLabel(level){
  if(level==='high') return '\u9ad8\u6760\u6746';
  if(level==='mid') return '\u4e2d\u6760\u6746';
  if(level==='low') return '\u4f4e\u6760\u6746';
  return '\u5168\u90e8';
}
function _liqX402PickTopLevels(data, sel){
  const rows=[];
  const byLevel=(data && data.level_top_levels && typeof data.level_top_levels==='object') ? data.level_top_levels : {};
  const pushRows=(level)=>{
    const arr=Array.isArray(byLevel[level])?byLevel[level]:[];
    arr.forEach((x)=>{ rows.push({level, price:x.price, total_liq:x.total_liq, distance_pct:x.distance_pct}); });
  };
  if(sel.high) pushRows('high');
  if(sel.mid) pushRows('mid');
  if(sel.low) pushRows('low');
  if(rows.length===0){
    const base=Array.isArray(data && data.top_levels)?data.top_levels:[];
    base.forEach((x)=>rows.push({level:'all', price:x.price, total_liq:x.total_liq, distance_pct:x.distance_pct}));
  }
  rows.sort((a,b)=>Number(b.total_liq||0)-Number(a.total_liq||0));
  return rows.slice(0,8);
}
function _liqX402LevelAllowed(level, selected){
  if(level==='high') return !!selected.high;
  if(level==='mid') return !!selected.mid;
  if(level==='low') return !!selected.low;
  return _liqX402AnyLevelSelected(selected);
}
function _liqX402LevelTone(level, above){
  if(above){
    if(level==='high') return '91,214,255';
    if(level==='mid') return '56,189,248';
    return '45,125,255';
  }
  if(level==='high') return '255,90,94';
  if(level==='mid') return '255,130,74';
  return '255,45,126';
}
function _drawX402Heatmap(canvas, data, selected, opts){
  if(!canvas) return;
  const ctx=canvas.getContext('2d');
  const w=canvas.width, h=canvas.height;
  ctx.clearRect(0,0,w,h);
  const bg=ctx.createLinearGradient(0,0,0,h);
  bg.addColorStop(0,'#050b14');
  bg.addColorStop(1,'#060d18');
  ctx.fillStyle=bg;
  ctx.fillRect(0,0,w,h);

  const left=LIQ_X402_LAYOUT.left,right=LIQ_X402_LAYOUT.right,top=LIQ_X402_LAYOUT.top,bottom=LIQ_X402_LAYOUT.bottom;
  const pw=Math.max(1,w-left-right), ph=Math.max(1,h-top-bottom);
  const lineScale=Math.max(0.2,Math.min(2.2,Number((opts||{}).line_strength||100)/100));
  const bubbleScale=Math.max(0.2,Math.min(2.2,Number((opts||{}).bubble_size||100)/100));
  const showK=(opts||{}).show_k!==false;

  ctx.fillStyle='rgba(8,20,34,0.88)';
  ctx.fillRect(left,top,pw,ph);

  if(!_liqX402AnyLevelSelected(selected)){
    ctx.fillStyle='#9aa7ba'; ctx.font='12px sans-serif'; ctx.fillText('\u8bf7\u81f3\u5c11\u9009\u62e9\u4e00\u4e2a\u6760\u6746\u5c42\u7ea7',left+12,top+22); return;
  }
  const heat=(data&&data.heatmap&&typeof data.heatmap==='object')?data.heatmap:{};
  const interval=String((data&&data.interval)||'1d');
  const points=Array.isArray(heat.points)?heat.points:[];
  if(!points.length){
    ctx.fillStyle='#9aa7ba'; ctx.font='12px sans-serif'; ctx.fillText('\u6682\u65e0\u70ed\u529b\u56fe\u6570\u636e',left+12,top+22); return;
  }
  const tBins=Math.max(1, Number(heat.time_bins||1));
  const pBins=Math.max(1, Number(heat.price_bins||1));
  const maxV=Math.max(1, Number(heat.max_value||1));
  let pMin=Number(heat.price_min||0), pMax=Number(heat.price_max||0);
  if(!Number.isFinite(pMin)||!Number.isFinite(pMax)||pMin===pMax){ pMin=0; pMax=1; }
  if(pMin>pMax){ const t=pMin; pMin=pMax; pMax=t; }
  const vp=_liqX402PrepareViewport(data||{});
  const totalX=Math.max(1, Number(vp.total||_liqX402GetTotalX(data||{})));
  const xMapScale=(tBins>1 && totalX>1) ? ((totalX-1)/Math.max(1,tBins-1)) : 1;
  const xStart=Number(vp.start||0);
  const xWindow=Math.max(1, Number(vp.window||1));
  const xEnd=xStart+xWindow;
  const currentPrice=Number((((data||{}).summary||{}).current_price)||0);
  const bands=(data&&data.leverage_bands&&typeof data.leverage_bands==='object')?data.leverage_bands:{};
  const dominant=Array.isArray(bands.dominant_by_bin)?bands.dominant_by_bin:[];
  const cw=pw/xWindow, ch=ph/pBins;
  const baseRange=Math.max(1e-9,(pMax-pMin));
  const yZoom=Math.max(1, Math.min(6, Number(vp.y_zoom||1)));
  const viewRange=baseRange / yZoom;
  let center=((pMax+pMin)/2) + (Number(vp.y_offset||0) * baseRange);
  if(viewRange < baseRange){
    center=Math.max(pMin+viewRange*0.5, Math.min(pMax-viewRange*0.5, center));
  }
  const vMin=center-viewRange*0.5;
  const vMax=center+viewRange*0.5;
  const yByPrice=(price)=>top + ((vMax-price)/Math.max(1e-9,(vMax-vMin)))*ph;
  const rowTotals=new Array(pBins).fill(0);
  const rowXMass=new Array(pBins).fill(0);
  const rowXWeight=new Array(pBins).fill(0);

  ctx.strokeStyle='rgba(120,148,178,0.18)';
  ctx.lineWidth=1;
  for(let i=0;i<=6;i++){
    const y=top + (ph*i/6);
    ctx.beginPath(); ctx.moveTo(left,y); ctx.lineTo(left+pw,y); ctx.stroke();
    const pVal=vMax-((vMax-vMin)*i/6);
    ctx.fillStyle='#8fa2b9'; ctx.font='11px sans-serif';
    ctx.fillText(fmt(pVal), left+pw+10, y+4);
  }

  for(const row of points){
    if(!Array.isArray(row)||row.length<3) continue;
    const xBin=Number(row[0])||0;
    const x=xBin*xMapScale;
    const y=Number(row[1])||0;
    const v=Number(row[2])||0;
    if(v<=0||x<0||y<0||y>=pBins) continue;
    if(x < xStart || x > xEnd) continue;
    const level=(y>=0&&y<dominant.length)?String(dominant[y]||'all'):'all';
    if(!_liqX402LevelAllowed(level, selected)) continue;
    const ratio=Math.max(0,Math.min(1,v/maxV));
    rowTotals[y]+=v;
    rowXMass[y]+=x*v;
    rowXWeight[y]+=v;
    const priceAtBin=pMin + ((pMax-pMin)*(y/Math.max(1,pBins-1)));
    const above=currentPrice>0 ? (priceAtBin>=currentPrice) : (y>=Math.floor(pBins*0.5));
    const tone=_liqX402LevelTone(level, above);
    const alpha=Math.max(0.06, Math.min(0.95, (0.06 + Math.pow(ratio,0.58)*0.9) * lineScale));
    const ypx=top + (pBins-1-y+0.5)*ch;
    if(priceAtBin < vMin || priceAtBin > vMax) continue;
    const x0=left + (x-xStart)*cw;
    const seg=Math.max(2, cw*(1.4 + ratio*22*lineScale));
    ctx.strokeStyle=`rgba(${tone},${alpha})`;
    ctx.lineWidth=Math.max(1, ch*(0.62 + 0.55*Math.min(1,lineScale)));
    ctx.lineCap='round';
    ctx.beginPath();
    ctx.moveTo(x0,ypx);
    ctx.lineTo(Math.min(left+pw, x0+seg), ypx);
    ctx.stroke();
  }

  const profileMax=Math.max(1, ...rowTotals);
  for(let y=0;y<pBins;y++){
    const total=rowTotals[y]||0;
    if(total<=0) continue;
    const ratio=Math.max(0,Math.min(1,total/profileMax));
    const ypx=top + (pBins-1-y+0.5)*ch;
    const len=Math.max(1, 54*Math.pow(ratio,0.62));
    const priceAtBin=pMin + ((pMax-pMin)*(y/Math.max(1,pBins-1)));
    if(priceAtBin < vMin || priceAtBin > vMax) continue;
    const above=currentPrice>0 ? (priceAtBin>=currentPrice) : (y>=Math.floor(pBins*0.5));
    const tone=above?'80,189,255':'255,95,95';
    ctx.fillStyle=`rgba(${tone},${0.12 + ratio*0.55})`;
    ctx.fillRect(left+pw+10, ypx-Math.max(1,ch*0.32), len, Math.max(1,ch*0.64));
  }

  const candles=Array.isArray((data||{}).candles)?(data||{}).candles:[];
  const candleTs=Array.isArray(candles)?candles.map(c=>Number((c||{}).ts||0)).filter(x=>Number.isFinite(x)&&x>0):[];
  if(showK && candles.length>=2){
    const n=Math.min(360, candles.length);
    const arr=candles.slice(-n);
    const bodyW=Math.max(1, Math.min(8, cw*0.64));
    for(let i=0;i<arr.length;i++){
      const c=arr[i]||{};
      const op=Number(c.open||0), hi=Number(c.high||0), lo=Number(c.low||0), cl=Number(c.close||0);
      if(!(op||hi||lo||cl)) continue;
      const xRaw=(n<=1)?0:(i*(totalX-1)/Math.max(1,n-1));
      if(xRaw < xStart || xRaw > xEnd) continue;
      const x=left + (xRaw-xStart)*cw;
      if(Math.max(op,hi,lo,cl) < vMin || Math.min(op,hi,lo,cl) > vMax) continue;
      const yH=yByPrice(hi), yL=yByPrice(lo), yO=yByPrice(op), yC=yByPrice(cl);
      const up=cl>=op;
      const tone=up?'74,222,128':'251,146,60';
      ctx.strokeStyle=`rgba(${tone},0.92)`;
      ctx.lineWidth=1;
      ctx.beginPath(); ctx.moveTo(x,yH); ctx.lineTo(x,yL); ctx.stroke();
      const topBody=Math.min(yO,yC), hBody=Math.max(1,Math.abs(yC-yO));
      ctx.fillStyle=`rgba(${tone},0.7)`;
      ctx.fillRect(x-bodyW*0.5, topBody, bodyW, hBody);
    }
  }

  if(currentPrice>0 && currentPrice>=vMin && currentPrice<=vMax){
    const yCur=yByPrice(currentPrice);
    ctx.setLineDash([4,4]);
    ctx.strokeStyle='rgba(24,220,164,0.75)';
    ctx.lineWidth=1;
    ctx.beginPath(); ctx.moveTo(left,yCur); ctx.lineTo(left+pw,yCur); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle='rgba(18,197,149,0.88)';
    ctx.fillRect(left+pw+9, yCur-9, 62, 18);
    ctx.fillStyle='#e8fff7'; ctx.font='11px sans-serif';
    ctx.fillText(fmt(currentPrice), left+pw+12, yCur+4);
  }

  const hStart=Number(heat.start_ts||0), hEnd=Number(heat.end_ts||0);
  const fmtTick=(ms)=>{
    try{
      const d=new Date(Number(ms)||0);
      const mm=String(d.getMonth()+1).padStart(2,'0');
      const dd=String(d.getDate()).padStart(2,'0');
      const hh=String(d.getHours()).padStart(2,'0');
      const mi=String(d.getMinutes()).padStart(2,'0');
      return interval==='1d'||interval==='3d'||interval==='1w' ? `${mm}-${dd}` : `${mm}-${dd} ${hh}:${mi}`;
    }catch(_){ return '-'; }
  };
  ctx.fillStyle='#91a6c0';
  ctx.font='11px sans-serif';
  ctx.textAlign='center';
  for(let i=0;i<=4;i++){
    const xr=xStart + (xWindow*i/4);
    const xpx=left + (xr-xStart)*cw;
    let ts=0;
    if(candleTs.length>=2){
      const idx=Math.max(0, Math.min(candleTs.length-1, Math.round((xr/Math.max(1,totalX-1))*(candleTs.length-1))));
      ts=Number(candleTs[idx]||0);
    }else if(hStart>0 && hEnd>=hStart){
      ts=hStart + (hEnd-hStart)*(xr/Math.max(1,totalX-1));
    }
    if(ts>0) ctx.fillText(fmtTick(ts), xpx, top+ph+16);
  }
  ctx.textAlign='left';

  const tops=_liqX402PickTopLevels(data||{}, selected).slice(0,8);
  if(tops.length){
    const mx=Math.max(1,...tops.map(x=>Number(x.total_liq||0)));
    for(let i=0;i<tops.length;i++){
      const row=tops[i]||{};
      const py=Number(row.price||0);
      if(!(py>=vMin&&py<=vMax)) continue;
      const y=yByPrice(py);
      const yNorm=Math.max(0, Math.min(1, (py-pMin)/Math.max(1e-9,(pMax-pMin))));
      const yi=Math.max(0, Math.min(pBins-1, Math.round(yNorm*(pBins-1))));
      let xr = rowXWeight[yi]>0 ? (rowXMass[yi]/Math.max(1e-9,rowXWeight[yi])) : (xStart + xWindow*(0.58 + 0.38*(i/Math.max(1,tops.length-1))));
      if(!Number.isFinite(xr)) xr=xStart + xWindow*0.72;
      if(xr < xStart || xr > xEnd) continue;
      const x=left + (xr-xStart)*cw;
      const r=4 + Math.pow(Math.max(0,Number(row.total_liq||0))/mx,0.55) * 12 * bubbleScale;
      const lv=String(row.level||'all');
      const tone=lv==='high'?'107,242,160':(lv==='mid'?'87,179,255':'255,123,190');
      ctx.fillStyle=`rgba(${tone},0.18)`;
      ctx.strokeStyle=`rgba(${tone},0.92)`;
      ctx.lineWidth=1.2;
      ctx.beginPath(); ctx.arc(x,y,r,0,Math.PI*2); ctx.fill(); ctx.stroke();
    }
  }

  ctx.strokeStyle='rgba(130,156,182,0.3)';
  ctx.lineWidth=1;
  ctx.strokeRect(left,top,pw,ph);
  ctx.fillStyle='#8ea1b8';
  ctx.font='11px sans-serif';
  ctx.fillText(`${(data||{}).exchange||''} ${(data||{}).symbol||''} ${(data||{}).interval||''}  ${Math.round(xWindow)}/${Math.round(Math.max(1,_liqX402GetTotalX(data||{})))} bars`, left+6, h-8);
  ctx.fillText('\u62d6\u52a8\u79fb\u52a8 | \u6eda\u8f6e\u7f29\u653e | \u53cc\u51fb\u91cd\u7f6e', left+250, h-8);
}
function renderX402HeatmapFromState(showStatusHint){
  const statusEl=document.getElementById('liqX402Status');
  const metaEl=document.getElementById('liqX402Meta');
  const tableEl=document.getElementById('liqX402Top');
  const canvas=document.getElementById('liqX402Canvas');
  if(!statusEl||!metaEl||!tableEl||!canvas) return;
  if(!liqX402LastData || !liqX402LastData.ok){
    if(showStatusHint){ statusEl.textContent='x402 \u6682\u65e0\u53ef\u7528\u6570\u636e\uff0c\u8bf7\u5148\u70b9\u51fb\u201c\u751f\u6210\u70ed\u529b\u56fe\u201d\u3002'; }
    metaEl.textContent='';
    tableEl.innerHTML='<tr><th>\u6760\u6746\u5c42\u7ea7</th><th>\u4ef7\u683c</th><th>\u6e05\u7b97\u603b\u91cf</th><th>\u8ddd\u79bb</th></tr><tr><td colspan="4" style="text-align:center;color:#999">\u6682\u65e0\u5173\u952e\u4ef7\u4f4d</td></tr>';
    _drawX402Heatmap(canvas, null, {high:true,mid:true,low:true}, _liqX402ReadViewOptsFromDom());
    return;
  }
  const sel=_liqX402SelectedLevels();
  if(!_liqX402AnyLevelSelected(sel)){
    statusEl.textContent='\u8bf7\u81f3\u5c11\u9009\u62e9\u4e00\u4e2a\u6760\u6746\u5c42\u7ea7\u3002';
  }
  const data=liqX402LastData;
  const viewOpts=_liqX402ReadViewOptsFromDom();
  const bands=(data.leverage_bands && typeof data.leverage_bands==='object')?data.leverage_bands:{};
  _drawX402Heatmap(canvas, data, sel, viewOpts);
  const s=data.summary||{};
  const totals=(bands && bands.level_totals && typeof bands.level_totals==='object') ? bands.level_totals : {};
  const candleCount=Array.isArray(data.candles)?data.candles.length:0;
  const levelsText=['high','mid','low'].filter(k=>sel[k]).map(k=>_liqX402LevelLabel(k)).join('/');
  const totalText = (totals.high||totals.mid||totals.low)
    ? ` | H:${fmt(totals.high||0)} M:${fmt(totals.mid||0)} L:${fmt(totals.low||0)}`
    : '';
  metaEl.textContent=`\u70ed\u529b\u70b9 ${s.nonzero_points||0}/${s.raw_point_count||0} | \u5cf0\u503c ${fmt(s.max_liq_value||0)} | \u73b0\u4ef7 ${s.current_price?('$'+fmt(s.current_price)):'-'} | K\u7ebf ${candleCount} | \u5c42\u7ea7 ${levelsText||'ALL'}${totalText}`;
  const top=_liqX402PickTopLevels(data, sel);
  tableEl.innerHTML='<tr><th>\u6760\u6746\u5c42\u7ea7</th><th>\u4ef7\u683c</th><th>\u6e05\u7b97\u603b\u91cf</th><th>\u8ddd\u79bb</th></tr>' +
    (top.map(x=>`<tr><td>${esc(_liqX402LevelLabel(x.level))}</td><td>$${esc(fmt(x.price))}</td><td>${esc(fmt(x.total_liq))}</td><td>${x.distance_pct==null?'-':esc(fmt(x.distance_pct))+'%'}</td></tr>`).join('') || '<tr><td colspan="4" style="text-align:center;color:#999">\u6682\u65e0\u5173\u952e\u4ef7\u4f4d</td></tr>');
  if(showStatusHint){
    statusEl.textContent=`x402 \u70ed\u529b\u56fe\u5df2\u66f4\u65b0 @ ${new Date().toLocaleTimeString('zh-CN', { hour12: false })}`;
  }
}
function _liqX402BindLevelEvents(){
  ['liqX402LvHigh','liqX402LvMid','liqX402LvLow'].forEach((id)=>{
    const el=document.getElementById(id);
    if(el && !el.dataset.bound){
      el.dataset.bound='1';
      el.onchange=()=>{
        _liqX402SaveLevelPrefs(_liqX402SelectedLevels());
        renderX402HeatmapFromState(true);
      };
    }
  });
}
function _liqX402BindViewEvents(){
  ['liqX402LineStrength','liqX402BubbleSize','liqX402ShowK'].forEach((id)=>{
    const el=document.getElementById(id);
    if(!el || el.dataset.bound) return;
    el.dataset.bound='1';
    const handler=()=>{
      const opts=_liqX402ReadViewOptsFromDom();
      _liqX402SaveViewOpts(opts);
      _liqX402UpdateViewOptLabels(opts);
      renderX402HeatmapFromState(false);
    };
    el.oninput=handler;
    el.onchange=handler;
  });
}
function _liqX402ResetView(){
  _liqX402ResetViewport(_liqX402GetTotalX(liqX402LastData||{}), true);
  renderX402HeatmapFromState(false);
}
function _liqX402BindCanvasEvents(){
  const canvas=document.getElementById('liqX402Canvas');
  if(!canvas || canvas.dataset.bound) return;
  canvas.dataset.bound='1';
  canvas.style.cursor='grab';
  canvas.style.touchAction='none';
  const getScale=()=>{
    const rect=canvas.getBoundingClientRect();
    const sx=canvas.width/Math.max(1, rect.width||1);
    const sy=canvas.height/Math.max(1, rect.height||1);
    return {rect,sx,sy};
  };
  canvas.addEventListener('pointerdown',(ev)=>{
    if(ev.button!==0) return;
    _liqX402PrepareViewport(liqX402LastData||{});
    liqX402Viewport.dragging=true;
    liqX402Viewport.last_x=Number(ev.clientX||0);
    liqX402Viewport.last_y=Number(ev.clientY||0);
    canvas.style.cursor='grabbing';
    try{ canvas.setPointerCapture(ev.pointerId); }catch(_){ }
    ev.preventDefault();
  });
  canvas.addEventListener('pointermove',(ev)=>{
    if(!liqX402Viewport.dragging) return;
    const dxCss=Number(ev.clientX||0)-Number(liqX402Viewport.last_x||0);
    const dyCss=Number(ev.clientY||0)-Number(liqX402Viewport.last_y||0);
    liqX402Viewport.last_x=Number(ev.clientX||0);
    liqX402Viewport.last_y=Number(ev.clientY||0);
    const {sx,sy}=getScale();
    const dx=dxCss*sx;
    const dy=dyCss*sy;
    const left=LIQ_X402_LAYOUT.left,right=LIQ_X402_LAYOUT.right,top=LIQ_X402_LAYOUT.top,bottom=LIQ_X402_LAYOUT.bottom;
    const pw=Math.max(1, canvas.width-left-right), ph=Math.max(1, canvas.height-top-bottom);
    liqX402Viewport.start -= dx * (Number(liqX402Viewport.window||1) / Math.max(1,pw));
    liqX402Viewport.y_offset += dy/Math.max(1,ph) * (0.9/Math.max(1, Number(liqX402Viewport.y_zoom||1)));
    _liqX402ClampViewport(liqX402Viewport, _liqX402GetTotalX(liqX402LastData||{}));
    _liqX402SaveViewport();
    renderX402HeatmapFromState(false);
    ev.preventDefault();
  });
  const endDrag=(ev)=>{
    if(liqX402Viewport.dragging){
      liqX402Viewport.dragging=false;
      canvas.style.cursor='grab';
      _liqX402SaveViewport();
    }
    if(ev && ev.pointerId!==undefined){
      try{ canvas.releasePointerCapture(ev.pointerId); }catch(_){ }
    }
  };
  canvas.addEventListener('pointerup', endDrag);
  canvas.addEventListener('pointercancel', endDrag);
  canvas.addEventListener('wheel', (ev)=>{
    _liqX402PrepareViewport(liqX402LastData||{});
    const left=LIQ_X402_LAYOUT.left,right=LIQ_X402_LAYOUT.right,top=LIQ_X402_LAYOUT.top,bottom=LIQ_X402_LAYOUT.bottom;
    const {rect,sx}=getScale();
    const pw=Math.max(1, canvas.width-left-right), ph=Math.max(1, canvas.height-top-bottom);
    const cx=(Number(ev.clientX||0)-rect.left)*sx;
    const ratio=Math.max(0, Math.min(1, (cx-left)/Math.max(1,pw)));
    if(ev.shiftKey){
      const zin=ev.deltaY<0;
      const next=Number(liqX402Viewport.y_zoom||1)*(zin?1.12:(1/1.12));
      liqX402Viewport.y_zoom=Math.max(1, Math.min(6, next));
    }else{
      const zin=ev.deltaY<0;
      const oldWin=Math.max(1, Number(liqX402Viewport.window||1));
      const newWin=oldWin*(zin?0.88:1.14);
      const total=Math.max(1, _liqX402GetTotalX(liqX402LastData||{}));
      const minWindow=Math.max(8, Math.min(80, total));
      const clamped=Math.max(minWindow, Math.min(total, newWin));
      const anchor=Number(liqX402Viewport.start||0)+oldWin*ratio;
      liqX402Viewport.window=clamped;
      liqX402Viewport.start=anchor-clamped*ratio;
    }
    _liqX402ClampViewport(liqX402Viewport, _liqX402GetTotalX(liqX402LastData||{}));
    _liqX402SaveViewport();
    renderX402HeatmapFromState(false);
    ev.preventDefault();
  }, {passive:false});
  canvas.addEventListener('dblclick',(ev)=>{
    _liqX402ResetView();
    ev.preventDefault();
  });
}

async function runX402LiquidationHeatmap(){
  const symbolEl=document.getElementById('liqX402Symbol');
  const exchangeEl=document.getElementById('liqX402Exchange');
  const intervalEl=document.getElementById('liqX402Interval');
  const statusEl=document.getElementById('liqX402Status');
  const metaEl=document.getElementById('liqX402Meta');
  const tableEl=document.getElementById('liqX402Top');
  const canvas=document.getElementById('liqX402Canvas');
  if(!symbolEl||!exchangeEl||!intervalEl||!statusEl||!metaEl||!tableEl||!canvas) return;
  _liqX402BindLevelEvents();
  _liqX402BindViewEvents();
  const sel=_liqX402SelectedLevels();
  if(!_liqX402AnyLevelSelected(sel)){
    statusEl.textContent='\u8bf7\u81f3\u5c11\u9009\u62e9\u4e00\u4e2a\u6760\u6746\u5c42\u7ea7\u3002';
    return;
  }
  if(liqX402Busy){ statusEl.textContent='x402 \u8bf7\u6c42\u8fdb\u884c\u4e2d...'; return; }
  const symbol=_liqX402SanitizeSymbol(symbolEl.value);
  const exchange=_liqX402SanitizeExchange(exchangeEl.value);
  const interval=_liqX402SanitizeInterval(intervalEl.value);
  symbolEl.value=symbol; exchangeEl.value=exchange; intervalEl.value=interval;
  liqX402Busy=true; statusEl.textContent=`\u6b63\u5728\u52a0\u8f7d ${symbol}/${exchange}/${interval} ...`;
  try{
    const u=`/api/liquidation-heatmap-x402?symbol=${encodeURIComponent(symbol)}&exchange=${encodeURIComponent(exchange)}&interval=${encodeURIComponent(interval)}&_=${Date.now()}`;
    const resp=await fetch(u,{cache:'no-store'});
    if(!resp.ok) throw new Error('http_'+resp.status);
    const data=await resp.json();
    if(!data.ok) throw new Error(data.error||'x402_failed');
    liqX402LastData=data;
    _liqX402SaveCache(data);
    renderX402HeatmapFromState(true);
  }catch(e){
    statusEl.textContent='x402 \u8bf7\u6c42\u5931\u8d25: '+String((e&&e.message)||e||'unknown');
    if(!liqX402LastData){
      metaEl.textContent='';
      tableEl.innerHTML='<tr><th>\u6760\u6746\u5c42\u7ea7</th><th>\u4ef7\u683c</th><th>\u6e05\u7b97\u603b\u91cf</th><th>\u8ddd\u79bb</th></tr><tr><td colspan="4" style="text-align:center;color:#999">\u6682\u65e0\u5173\u952e\u4ef7\u4f4d</td></tr>';
      _drawX402Heatmap(canvas, null, {high:true,mid:true,low:true}, _liqX402ReadViewOptsFromDom());
    }
  }finally{ liqX402Busy=false; }
}

async function ensureX402LiquidationSection(liqCard){
  if(!liqCard) return;
  _liqX402LoadCache();
  let panel=document.getElementById('liqX402Panel');
  if(!panel){
    panel=document.createElement('div'); panel.id='liqX402Panel'; panel.style.marginTop='10px';
    panel.innerHTML='<div style="height:1px;background:#223146;margin:10px 0"></div>' +
      '<div style="font-weight:700;color:#dbe8ff">x402 \u6e05\u7b97\u70ed\u529b\u56fe(K\u7ebf\u53e0\u52a0)</div>' +
      '<div class="guide-note" style="margin-top:4px;color:#9fb2cc">\u4f20\u5165\u53c2\u6570\u540e\u70b9\u51fb\u201c\u751f\u6210\u70ed\u529b\u56fe\u201d\uff0c\u4f1a\u53e0\u52a0 K \u7ebf\u548c\u5206\u5c42\u6d41\u52a8\u6027\u7ebf\u6761\u3002</div>' +
      '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px;align-items:center">' +
      '<input id="liqX402Symbol" type="text" value="BTCUSDT" style="width:120px;padding:5px 8px;background:#0b1220;color:#dbe8ff;border:1px solid #334155;border-radius:6px" placeholder="BTCUSDT" />' +
      '<input id="liqX402Exchange" type="text" value="Binance" style="width:100px;padding:5px 8px;background:#0b1220;color:#dbe8ff;border:1px solid #334155;border-radius:6px" placeholder="Binance" />' +
      '<select id="liqX402Interval" style="padding:5px 8px;background:#0b1220;color:#dbe8ff;border:1px solid #334155;border-radius:6px">' +
      '<option value="15m">15m</option><option value="30m">30m</option><option value="1h">1h</option><option value="2h">2h</option><option value="4h">4h</option><option value="6h">6h</option><option value="12h">12h</option><option value="1d" selected>1d</option><option value="3d">3d</option><option value="1w">1w</option>' +
      '</select>' +
      '<label style="display:inline-flex;align-items:center;gap:4px;font-size:12px;color:#dbe8ff"><input id="liqX402LvHigh" type="checkbox" checked />\u9ad8\u6760\u6746</label>' +
      '<label style="display:inline-flex;align-items:center;gap:4px;font-size:12px;color:#dbe8ff"><input id="liqX402LvMid" type="checkbox" checked />\u4e2d\u6760\u6746</label>' +
      '<label style="display:inline-flex;align-items:center;gap:4px;font-size:12px;color:#dbe8ff"><input id="liqX402LvLow" type="checkbox" checked />\u4f4e\u6760\u6746</label>' +
      '<button id="liqX402RunBtn" class="nav" style="padding:5px 12px">\u751f\u6210\u70ed\u529b\u56fe</button>' +
      '<button id="liqX402ResetViewBtn" class="nav" style="padding:5px 12px">\u91cd\u7f6e\u89c6\u56fe</button></div>' +
      '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:6px;align-items:center;color:#c4d4ea;font-size:12px">' +
      '<label style="display:inline-flex;align-items:center;gap:4px"><input id="liqX402ShowK" type="checkbox" checked />K\u7ebf</label>' +
      '<span>\u7ebf\u6761\u5f3a\u5ea6</span><input id="liqX402LineStrength" type="range" min="20" max="220" value="100" style="width:130px" /><span id="liqX402LineStrengthVal">100%</span>' +
      '<span>\u6c14\u6ce1\u5927\u5c0f</span><input id="liqX402BubbleSize" type="range" min="20" max="220" value="100" style="width:130px" /><span id="liqX402BubbleSizeVal">100%</span>' +
      '</div>' +
      '<div id="liqX402Status" class="muted" style="margin-top:6px;color:#9fb2cc">\u5c31\u7eea\u3002</div>' +
      '<canvas id="liqX402Canvas" width="1080" height="420" style="margin-top:8px;width:100%;max-width:100%;border:1px solid #2f3e54;border-radius:8px;background:#050b14"></canvas>' +
      '<div id="liqX402Meta" class="guide-note" style="margin-top:6px;color:#9fb2cc"></div>' +
      '<table id="liqX402Top" style="margin-top:8px;background:#0a1220;color:#dbe8ff"><tr><th>\u6760\u6746\u5c42\u7ea7</th><th>\u4ef7\u683c</th><th>\u6e05\u7b97\u603b\u91cf</th><th>\u8ddd\u79bb</th></tr></table>';
    liqCard.appendChild(panel);
    const btn=document.getElementById('liqX402RunBtn');
    const resetBtn=document.getElementById('liqX402ResetViewBtn');
    if(btn) btn.onclick=()=>runX402LiquidationHeatmap();
    if(resetBtn) resetBtn.onclick=()=>_liqX402ResetView();
    _liqX402ApplyLevelPrefs();
    _liqX402ApplyViewOptsToDom();
    _liqX402BindLevelEvents();
    _liqX402BindViewEvents();
    _liqX402BindCanvasEvents();
  }
  const symbolEl=document.getElementById('liqX402Symbol');
  const exchangeEl=document.getElementById('liqX402Exchange');
  const intervalEl=document.getElementById('liqX402Interval');
  const statusEl=document.getElementById('liqX402Status');
  if(liqX402LastData && liqX402LastData.ok){
    if(symbolEl && liqX402LastData.symbol) symbolEl.value=_liqX402SanitizeSymbol(liqX402LastData.symbol);
    if(exchangeEl && liqX402LastData.exchange) exchangeEl.value=_liqX402SanitizeExchange(liqX402LastData.exchange);
    if(intervalEl && liqX402LastData.interval) intervalEl.value=_liqX402SanitizeInterval(liqX402LastData.interval);
  }
  if(liqX402LastData && liqX402LastData.ok){
    renderX402HeatmapFromState(false);
  }else if(statusEl){
    statusEl.textContent='\u8bf7\u70b9\u51fb\u201c\u751f\u6210\u70ed\u529b\u56fe\u201d\u83b7\u53d6\u6700\u65b0\u6570\u636e\u3002';
  }
  _liqX402BindCanvasEvents();
}

function renderAccount(d){const acct=document.getElementById('acct');const bal=document.getElementById('balTbl');const fund=document.getElementById('fundTbl');const pos=document.getElementById('posTbl');if(!acct||!bal||!fund||!pos){console.error('[renderAccount] 缺少 DOM 元素:',{acct:!!acct,bal:!!bal,fund:!!fund,pos:!!pos});return;}if(d.mode==='paper'){const p=d.paper_account||{};acct.innerHTML=`<div>模拟交易 | 总权益: ${fmt(p.equity)} USDT | 当前价: ${fmt(p.current_price)}</div>`;bal.innerHTML=`<tr><th>项目</th><th>数值</th></tr>`+[['初始资金',p.initial_balance],['余额',p.balance],['权益',p.equity],['已实现盈亏',p.realized_pnl],['未实现盈亏',p.unrealized_pnl],['今日盈亏',p.daily_pnl]].map(x=>`<tr><td>${esc(x[0])}</td><td>${fmt(x[1])} USDT</td></tr>`).join('');fund.innerHTML='';pos.innerHTML=`<tr><th>币对</th><th>方向</th><th>持仓量<br>(BTC)</th><th>开仓均价</th><th>最新价</th><th>持仓价值<br>(USDT)</th><th>保证金</th><th>杠杆</th><th>预估强平价</th><th>未实现盈亏</th><th>收益率</th><th>持仓时长</th><th>状态</th></tr>`+(p.positions||[]).slice(0,30).map(x=>{const upl=x.unrealizedPnl||0;const uplPct=x.unrealizedPnlPct||0;const holdHrs=x.hold_hours||0;const maxHrs=x.max_hold_hours||4;const timeout=x.timeout;const entry=x.entryPrice||0;const mark=x.markPx||0;const size=x.size||0;const lev=x.lever||1;const notional=x.notional||Math.round(size*mark*100)/100;const margin=Math.round(notional/lev*100)/100;const liqPx=lev>1?(x.side==='long'?Math.round(entry*(1-0.9/lev)*100)/100:Math.round(entry*(1+0.9/lev)*100)/100):'-';const uplCls=upl>0?'bull':upl<0?'bear':'neu';const statusCls=timeout?'bear':'neu';const statusTxt=timeout?'超时警告!':holdHrs.toFixed(1)+'h';const dirTxt=x.side==='long'?'做多':'做空';const dirCls=x.side==='long'?'bull':'bear';return`<tr><td>${esc(x.instId||'BTC-USDT-SWAP')}</td><td class='${dirCls}'>${dirTxt}</td><td>${fmt(size)}</td><td>${fmt(entry)}</td><td>${fmt(mark)}</td><td>${fmt(notional)}</td><td>${fmt(margin)}</td><td>${lev}x</td><td>${liqPx}</td><td class='${uplCls}'>${upl>=0?'+':''}${fmt(upl)} U</td><td class='${uplCls}'>${uplPct>=0?'+':''}${fmt(uplPct)}%</td><td>${holdHrs.toFixed(1)}/${maxHrs}h</td><td class='${statusCls}'>${statusTxt}</td></tr>`;}).join('')||'<tr><td colspan=\"13\" style=\"text-align:center;color:#999\">暂无持仓</td></tr>';return;}const a=d.selected_account||{};acct.innerHTML=`<div>账户: ${esc(a.profile||'--')} | 配置: ${a.configured?'是':'否'}</div><div>交易账户权益: ${fmt(a.trading_equity_usdt)} USDT | 资金账户权益: ${fmt(a.funding_equity_usdt)} USDT</div><div>总权益: ${fmt(a.total_equity_usdt)} USDT | 持仓价值: ${fmt(a.position_value_usdt)} USDT</div>`;bal.innerHTML=`<tr><th>币种</th><th>权益</th><th>可用</th><th>冻结</th><th>USD价值</th></tr>`+(a.trading_balances||[]).slice(0,50).map(x=>`<tr><td>${esc(x.ccy)}</td><td>${fmt(x.equity)}</td><td>${fmt(x.avail)}</td><td>${fmt(x.frozen)}</td><td>${fmt(x.eq_usd)}</td></tr>`).join('');fund.innerHTML=`<tr><th>币种</th><th>权益</th><th>可用</th><th>冻结</th></tr>`+(a.funding_balances||[]).slice(0,50).map(x=>`<tr><td>${esc(x.ccy)}</td><td>${fmt(x.equity)}</td><td>${fmt(x.avail)}</td><td>${fmt(x.frozen)}</td></tr>`).join('');pos.innerHTML=`<tr><th>币对</th><th>方向</th><th>持仓量</th><th>最新价</th><th>持仓价值</th><th>未实现盈亏</th><th>收益率</th><th>杠杆</th><th>强平价</th></tr>`+(a.positions||[]).slice(0,50).map(x=>`<tr><td>${esc(x.instId)}</td><td>${esc(x.side)}</td><td>${fmt(x.size)}</td><td>${fmt(x.entryPx||x.entryPrice||0)} / ${fmt(x.markPx)}</td><td>${fmt(x.notional)}</td><td>${fmt(x.upl)}</td><td>${fmt(x.uplRatioPct)}%</td><td>${fmt(x.lever)}x</td><td>${fmt(x.liqPx)}</td></tr>`).join('');}
function renderStrategyOverview(d){
  const advisor=d.strategy_advisor||{};
  const benchmark=d.benchmark_context||{};
  const clusters=advisor.cluster_scores||{};
  const stance=advisor.stance||'-';
  const advice=Array.isArray(advisor.advice)?advisor.advice:[];
  const regime=benchmark.regime==='bull'?'多头环境':benchmark.regime==='bear'?'空头环境':'中性环境';
  const preferred=advisor.preferred_decision==='long'?'优先做多':advisor.preferred_decision==='short'?'允许做空':'优先等待';
  const riskBudget=advisor.risk_budget||'-';
  const advisorBox=document.getElementById('advisorBox');
  const advisorNotes=document.getElementById('advisorNotes');
  const benchmarkBox=document.getElementById('benchmarkBox');
  const clusterBox=document.getElementById('clusterBox');
  if(advisorBox){
    advisorBox.innerHTML=[
      ['当前画像', advisor.profile||'regime_balance_v4'],
      ['当前立场', preferred],
      ['风险预算', riskBudget],
      ['策略结论', stance],
    ].map(x=>`<div class='guide-item'><div class='guide-title'>${esc(x[0])}</div><div class='guide-value'>${esc(x[1])}</div></div>`).join('');
  }
  if(advisorNotes){
    advisorNotes.innerHTML=advice.length?`<ul style="margin:0 0 0 18px;padding:0">${advice.map(x=>`<li style="margin:4px 0">${esc(x)}</li>`).join('')}</ul>`:'-';
  }
  if(benchmarkBox){
    benchmarkBox.innerHTML=[
      ['当前环境', regime],
      ['基准分数', fmt(benchmark.score)],
      ['允许做多', benchmark.allow_long?'是':'否'],
      ['允许做空', benchmark.allow_short?'是':'否'],
      ['近5日日线变化', fmt(benchmark.d1_ret_5_pct)+'%'],
    ].map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
  }
  if(clusterBox){
    clusterBox.innerHTML=[
      ['BTC基准环境', clusters.benchmark_regime],
      ['趋势结构簇', clusters.trend_stack],
      ['订单流推进簇', clusters.flow_impulse],
      ['资金费率/基差簇', clusters.carry_regime],
      ['风险压力簇', clusters.risk_pressure],
    ].map(x=>`<div class='guide-item'><div class='guide-title'>${esc(x[0])}</div><div class='guide-value'>${fmt(x[1])}</div></div>`).join('');
  }
}
function renderFactors(d){
  const wrap=document.getElementById('factors');
  if(!wrap){console.error('[renderFactors] 缺少 factors 元素');return;}
  // Preserve liquidation heatmap panel across auto refresh redraws.
  const existingLiqCard=document.getElementById('liquidation-heatmap-card');
  const shouldReattachLiq=!!(existingLiqCard && existingLiqCard.parentElement===wrap);
  if(shouldReattachLiq){
    existingLiqCard.remove();
  }
  const main=(d.main_factors||[]);
  const aux=(d.aux_factors||[]);
  function card(f){
    const cls=f.direction==='bullish'?'bull':(f.direction==='bearish'?'bear':'neu');
    const dirTxt=f.direction==='bullish'?'\u504f\u591a':(f.direction==='bearish'?'\u504f\u7a7a':'\u4e2d\u6027');
    const scoreBand=factorBandLabel(f.score);
    const strength=factorStrengthLabel(f.score);
    const newbieTxt=f.story||`新手解读：该因子当前${dirTxt}，信号强度${strength}。分数越远离50，代表这个因子的态度越明确。`;
    const metrics=(f.metrics||[]).slice(0,14);
    return `<div class='card'>
      <div style='font-weight:700'>${esc(f.name)}</div>
      <div class='muted ${cls}'>角色: ${f.group==='main'?'主参考因子':'辅助判断因子'} | 方向: ${esc(dirTxt)} | ${esc(f.recommendation||'')}</div>
      <div>评分 ${fmt(f.score)} / 100（${esc(scoreBand)}） | 置信度 ${fmt(f.confidence)} / 100</div>
      <div class='guide-note' style='margin-top:4px'>${esc(newbieTxt)}</div>
      <details style='margin-top:6px'>
        <summary style='cursor:pointer;color:#444'>查看详细计算逻辑与实时指标</summary>
        <div class='muted' style='margin-top:4px'>用途定位: ${esc(f.thesis||'')}</div>
        <div class='muted' style='margin-top:4px'>逻辑说明: ${esc(f.logic||'')}</div>
        <ul>${metrics.map(x=>`<li>${esc(x)}</li>`).join('')||'<li>暂无数据</li>'}</ul>
      </details>
    </div>`;
  }
  wrap.innerHTML=`
    <div>
      <h3 style="margin:0 0 8px">主参考因子</h3>
      <div class="guide-note" style="margin-bottom:8px">这一组直接决定是否开仓，以及是优先做多、做空还是等待。</div>
      <div class="grid factors">${main.map(card).join('')||'<div class="muted">暂无主参考因子</div>'}</div>
    </div>
    <div style="margin-top:14px">
      <h3 style="margin:0 0 8px">辅助判断因子</h3>
      <div class="guide-note" style="margin-bottom:8px">这一组主要负责过滤噪声、调整风险预算、解释为什么当前不适合出手。</div>
      <div class="grid factors">${aux.map(card).join('')||'<div class="muted">暂无辅助判断因子</div>'}</div>
    </div>
  `;
  if(shouldReattachLiq){
    wrap.appendChild(existingLiqCard);
  }
}
function renderHistory(d){
  const tradeRows=(d.trade_history||[]).slice(0,120);
  const historyHead='<tr><th>开仓时间</th><th>平仓时间</th><th>策略</th><th>开仓逻辑(因子依据)</th><th>方向</th><th>开仓价</th><th>平仓价</th><th>平仓量(BTC)</th><th>成交额(U)</th><th>杠杆</th><th>仓位详情</th><th>持仓时长(分钟)</th><th>状态</th><th>平仓原因</th><th>信号评分</th><th>信号置信度</th><th>净盈亏(U)</th><th>收益率</th></tr>';
  document.getElementById('tradeTbl').innerHTML=historyHead+tradeRows.map(x=>{
    const side=String(x.side||x.direction||'').toLowerCase();
    const dirTxt=side==='long'?'做多':side==='short'?'做空':(side||'-');
    const dirCls=side==='long'?'bull':side==='short'?'bear':'neu';
    const pnl=Number(x.net_pnl===undefined?x.pnl:x.net_pnl)||0;
    const pnlCls=pnl>0?'bull':pnl<0?'bear':'neu';
    const statusRaw=String(x.status||'');
    const statusTxt=statusRaw==='partial_close'?'部分平仓':statusRaw==='close'?'已平仓':statusRaw==='open'?'持仓中':(statusRaw||'-');
    const holdTxt=(x.hold_minutes===undefined||x.hold_minutes===null)?'--':fmt(x.hold_minutes);
    const reason=String(x.close_reason||'-');
    const entryLogic=String(x.entry_logic_zh||x.factor_basis_zh||'-');
    const posDetail=String(x.position_detail_zh||'-');
    return `<tr><td>${esc(fmtTs(x.open_ts))}</td><td>${esc(fmtTs(x.close_ts))}</td><td>${esc(x.strategy||'-')}</td><td style='min-width:320px;max-width:520px;white-space:normal;line-height:1.45'>${esc(entryLogic)}</td><td class='${dirCls}'>${dirTxt}</td><td>${fmt(x.open_price)}</td><td>${statusRaw==='open'?'--':fmt(x.close_price)}</td><td>${fmt(x.size_btc)}</td><td>${fmt(x.notional)}</td><td>${fmt(x.leverage||1)}x</td><td style='min-width:280px;max-width:460px;white-space:normal;line-height:1.45'>${esc(posDetail)}</td><td>${holdTxt}</td><td>${esc(statusTxt)}</td><td>${esc(reason)}</td><td>${fmt(x.signal_score)}</td><td>${fmt(x.signal_confidence)}</td><td class='${pnlCls}'>${pnl>=0?'+':''}${fmt(pnl)}</td><td class='${pnlCls}'>${pnl>=0?'+':''}${fmt(x.pnl_pct)}%</td></tr>`;
  }).join('');

  document.getElementById('evoTbl').innerHTML='<tr><th>时间</th><th>类型</th><th>盈亏</th><th>分析总结</th><th>参数调整</th></tr>'+(d.evolution_history||[]).slice(0,80).map(x=>{const delta=x.equity_delta||0;const deltaCls=delta>0?'bull':delta<0?'bear':'neu';const type=x.type||'';const typeTxt=type==='loss_deep_analysis'?'亏损复盘':type==='win_analysis'?'盈利分析':type==='daily_evolution'?'每日检查':type;const summary=(x.summary||'').substring(0,80);const layers=x.layers||{};const adj=[];if(layers.execution&&layers.execution.adjustments){layers.execution.adjustments.forEach(a=>adj.push(a.param+':'+a.before+'->'+a.after));}return`<tr><td>${esc(x.ts||'').substring(0,19)}</td><td>${typeTxt}</td><td class='${deltaCls}'>${delta>=0?'+':''}${fmt(delta)}</td><td style='font-size:11px;max-width:300px'>${esc(summary)}</td><td style='font-size:11px'>${esc(adj.join(', '))}</td></tr>`;}).join('');

  const liqWrap=document.getElementById('liqBtTradeTbl');
  if(liqWrap){
    const liqRows=((((d.backtest_latest||{}).liquidation_only||{}).virtual_trade_rows)||[]).slice(0,120);
    if(!liqRows.length){
      liqWrap.innerHTML='<tr><th>说明</th></tr><tr><td style="color:#888">暂无清算反向回测触发记录。请先在“回测与回撤分析”执行一次回测。</td></tr>';
    }else{
      liqWrap.innerHTML='<tr><th>开仓时间</th><th>平仓时间</th><th>方向</th><th>开仓逻辑(因子依据)</th><th>仓位详情</th><th>开仓价</th><th>平仓价</th><th>持仓时长(分钟)</th><th>状态</th><th>平仓原因</th><th>毛收益(%)</th><th>净收益(%)</th><th>估算成本(%)</th></tr>'+
        liqRows.map(x=>{const side=String(x.side||'').toLowerCase();const dirTxt=side==='long'?'做多':side==='short'?'做空':(side||'-');const dirCls=side==='long'?'bull':side==='short'?'bear':'neu';const st=String(x.status||'');const stTxt=st==='close'?'已平仓':st==='open'?'持仓中':'-';const g=Number(x.gross_pnl_pct||0);const n=Number(x.net_pnl_pct||0);const gCls=g>0?'bull':g<0?'bear':'neu';const nCls=n>0?'bull':n<0?'bear':'neu';return `<tr><td>${esc(fmtTs(x.open_ts))}</td><td>${esc(fmtTs(x.close_ts))}</td><td class='${dirCls}'>${dirTxt}</td><td style='min-width:320px;max-width:520px;white-space:normal;line-height:1.45'>${esc(String(x.entry_logic_zh||x.factor_basis_zh||'-'))}</td><td style='min-width:280px;max-width:460px;white-space:normal;line-height:1.45'>${esc(String(x.position_detail_zh||'-'))}</td><td>${fmt(x.open_price)}</td><td>${st==='open'?'--':fmt(x.close_price)}</td><td>${fmt(x.hold_minutes)}</td><td>${esc(stTxt)}</td><td>${esc(String(x.close_reason||'-'))}</td><td class='${gCls}'>${g>=0?'+':''}${fmt(g)}</td><td class='${nCls}'>${n>=0?'+':''}${fmt(n)}</td><td>${fmt(x.estimated_cost_pct)}</td></tr>`;}).join('');
    }
  }
}
function _isLiqFactorTradeRow(x){
  const strategy=String((x&&x.strategy)||'').toLowerCase();
  const logicZh=String((x&&(x.entry_logic_zh||x.factor_basis_zh||''))||'');
  const logic=logicZh.toLowerCase();
  if(strategy.includes('liquidation')||strategy.includes('liq')||strategy.indexOf('清算')>=0) return true;
  if(logic.includes('liquidation')||logicZh.indexOf('清算')>=0) return true;
  return false;
}
function renderLiquidationFactorPage(d){
  const summary=document.getElementById('liqFactorSummary');
  const explain=document.getElementById('liqFactorExplain');
  const realTbl=document.getElementById('liqFactorRealTbl');
  const btTbl=document.getElementById('liqFactorBtTbl');
  if(!summary||!explain||!realTbl||!btTbl) return;

  const bt=(((d||{}).backtest_latest||{}).liquidation_only)||{};
  const btRows=Array.isArray(bt.virtual_trade_rows)?bt.virtual_trade_rows.slice(0,120):[];
  const realAll=(Array.isArray((d||{}).trade_history)?(d||{}).trade_history:[]).filter(_isLiqFactorTradeRow);
  const realRows=realAll.slice(0,120);
  const closedRows=realAll.filter(x=>{const st=String((x&&x.status)||'').toLowerCase();return st==='close'||st==='partial_close';});
  let realNetPnl=0;
  let realWinCount=0;
  closedRows.forEach(x=>{
    const pnl=Number(x.net_pnl===undefined?x.pnl:x.net_pnl)||0;
    realNetPnl+=pnl;
    if(pnl>0) realWinCount+=1;
  });
  const realWinRate=closedRows.length?(realWinCount*100/closedRows.length):null;
  const recentTrigger=realAll.length?fmtTs(realAll[0].open_ts):'--';
  const activeTrack=String(((d||{}).active_strategy)||'-');
  const profileLabel=String(bt.profile_label||bt.profile||'-');
  summary.innerHTML=[
    ['当前执行策略轨道', activeTrack],
    ['清算因子回测画像', profileLabel],
    ['实盘记录条数', String(realAll.length)],
    ['实盘已平仓笔数', String(closedRows.length)],
    ['实盘净盈亏(U)', (realNetPnl>=0?'+':'')+fmt(realNetPnl)],
    ['实盘胜率(%)', realWinRate===null?'--':fmt(realWinRate)],
    ['最近一次因子触发开仓', recentTrigger],
    ['回测收益(%)', bt.ok?fmt(bt.return_pct):'--'],
    ['回测最大回撤(%)', bt.ok?fmt(bt.max_drawdown_pct):'--'],
    ['回测交易数', bt.ok?String(bt.trades||0):'--'],
    ['回测记录条数', String(btRows.length)],
  ].map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
  explain.textContent='本页聚焦“清算反向因子”。左表为真实成交，右表为回测触发记录。若右表有数据而左表为空，通常表示因子在回测中可触发，但当前实盘主策略尚未按该因子独立执行。';

  if(!realRows.length){
    realTbl.innerHTML='<tr><th>说明</th></tr><tr><td style="color:#888">暂无清算因子真实成交记录。</td></tr>';
  }else{
    realTbl.innerHTML='<tr><th>开仓时间</th><th>平仓时间</th><th>方向</th><th>开仓逻辑(因子依据)</th><th>仓位详情</th><th>开仓价</th><th>平仓价</th><th>持仓时长(分钟)</th><th>状态</th><th>平仓原因</th><th>净盈亏(U)</th><th>收益率(%)</th></tr>'+
      realRows.map(x=>{const side=String(x.side||x.direction||'').toLowerCase();const dirTxt=side==='long'?'做多':side==='short'?'做空':(side||'-');const dirCls=side==='long'?'bull':side==='short'?'bear':'neu';const st=String(x.status||'');const stTxt=st==='partial_close'?'部分平仓':st==='close'?'已平仓':st==='open'?'持仓中':(st||'-');const pnl=Number(x.net_pnl===undefined?x.pnl:x.net_pnl)||0;const pnlCls=pnl>0?'bull':pnl<0?'bear':'neu';const holdTxt=(x.hold_minutes===undefined||x.hold_minutes===null)?'--':fmt(x.hold_minutes);return `<tr><td>${esc(fmtTs(x.open_ts))}</td><td>${esc(fmtTs(x.close_ts))}</td><td class='${dirCls}'>${dirTxt}</td><td style='min-width:260px;max-width:420px;white-space:normal;line-height:1.45'>${esc(String(x.entry_logic_zh||x.factor_basis_zh||'-'))}</td><td style='min-width:220px;max-width:380px;white-space:normal;line-height:1.45'>${esc(String(x.position_detail_zh||'-'))}</td><td>${fmt(x.open_price)}</td><td>${st==='open'?'--':fmt(x.close_price)}</td><td>${holdTxt}</td><td>${esc(stTxt)}</td><td>${esc(String(x.close_reason||'-'))}</td><td class='${pnlCls}'>${pnl>=0?'+':''}${fmt(pnl)}</td><td class='${pnlCls}'>${pnl>=0?'+':''}${fmt(x.pnl_pct)}%</td></tr>`;}).join('');
  }

  if(!btRows.length){
    btTbl.innerHTML='<tr><th>说明</th></tr><tr><td style="color:#888">暂无清算反向回测触发记录。请先在“回测与回撤分析”执行一次回测。</td></tr>';
  }else{
    btTbl.innerHTML='<tr><th>开仓时间</th><th>平仓时间</th><th>方向</th><th>开仓逻辑(因子依据)</th><th>仓位详情</th><th>开仓价</th><th>平仓价</th><th>持仓时长(分钟)</th><th>状态</th><th>平仓原因</th><th>毛收益(%)</th><th>净收益(%)</th><th>估算成本(%)</th></tr>'+
      btRows.map(x=>{const side=String(x.side||'').toLowerCase();const dirTxt=side==='long'?'做多':side==='short'?'做空':(side||'-');const dirCls=side==='long'?'bull':side==='short'?'bear':'neu';const st=String(x.status||'');const stTxt=st==='close'?'已平仓':st==='open'?'持仓中':'-';const g=Number(x.gross_pnl_pct||0);const n=Number(x.net_pnl_pct||0);const gCls=g>0?'bull':g<0?'bear':'neu';const nCls=n>0?'bull':n<0?'bear':'neu';return `<tr><td>${esc(fmtTs(x.open_ts))}</td><td>${esc(fmtTs(x.close_ts))}</td><td class='${dirCls}'>${dirTxt}</td><td style='min-width:260px;max-width:420px;white-space:normal;line-height:1.45'>${esc(String(x.entry_logic_zh||x.factor_basis_zh||'-'))}</td><td style='min-width:220px;max-width:380px;white-space:normal;line-height:1.45'>${esc(String(x.position_detail_zh||'-'))}</td><td>${fmt(x.open_price)}</td><td>${st==='open'?'--':fmt(x.close_price)}</td><td>${fmt(x.hold_minutes)}</td><td>${esc(stTxt)}</td><td>${esc(String(x.close_reason||'-'))}</td><td class='${gCls}'>${g>=0?'+':''}${fmt(g)}</td><td class='${nCls}'>${n>=0?'+':''}${fmt(n)}</td><td>${fmt(x.estimated_cost_pct)}</td></tr>`;}).join('');
  }
}
function renderTraderMonitor(d){
  const rt=d.trader_runtime||{};
  const todayRows=(d.today_trade_history||[]).slice(0,120);
  document.getElementById('todayTradeTbl').innerHTML='<tr><th>开仓时间</th><th>平仓时间</th><th>方向</th><th>开仓逻辑(因子依据)</th><th>仓位详情</th><th>开仓价</th><th>平仓价</th><th>平仓量(BTC)</th><th>持仓时长(分钟)</th><th>净盈亏(U)</th><th>收益率</th><th>状态</th></tr>'+todayRows.map(x=>{const side=String(x.side||x.direction||'').toLowerCase();const dirTxt=side==='long'?'做多':side==='short'?'做空':(side||'-');const dirCls=side==='long'?'bull':side==='short'?'bear':'neu';const pnl=Number(x.net_pnl===undefined?x.pnl:x.net_pnl)||0;const pnlCls=pnl>0?'bull':pnl<0?'bear':'neu';const statusRaw=String(x.status||'');const statusTxt=statusRaw==='partial_close'?'部分平仓':statusRaw==='close'?'已平仓':statusRaw==='open'?'持仓中':(statusRaw||'-');const holdTxt=(x.hold_minutes===undefined||x.hold_minutes===null)?'--':fmt(x.hold_minutes);const entryLogic=String(x.entry_logic_zh||x.factor_basis_zh||'-');const posDetail=String(x.position_detail_zh||'-');return `<tr><td>${esc(fmtTs(x.open_ts))}</td><td>${esc(fmtTs(x.close_ts))}</td><td class='${dirCls}'>${dirTxt}</td><td style='min-width:300px;max-width:500px;white-space:normal;line-height:1.45'>${esc(entryLogic)}</td><td style='min-width:260px;max-width:460px;white-space:normal;line-height:1.45'>${esc(posDetail)}</td><td>${fmt(x.open_price)}</td><td>${statusRaw==='open'?'--':fmt(x.close_price)}</td><td>${fmt(x.size_btc)}</td><td>${holdTxt}</td><td class='${pnlCls}'>${pnl>=0?'+':''}${fmt(pnl)}</td><td class='${pnlCls}'>${pnl>=0?'+':''}${fmt(x.pnl_pct)}%</td><td>${esc(statusTxt)}</td></tr>`}).join('');
  document.getElementById('traderRuntime').innerHTML=[['会话ID',rt.session_id||'-'],['会话文件',rt.session_file||'-'],['上次更新',rt.updated_at||'-'],['距今分钟',rt.minutes_since_update===undefined||rt.minutes_since_update===null?'-':rt.minutes_since_update],['消息通道',rt.channel||'-'],['账号ID',rt.account_id||'-'],['目标用户',rt.to||'-']].map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
  const dlg=d.trader_recent_dialogue||{};
  const u=dlg.user||{}, a=dlg.assistant||{};
  document.getElementById('latestDialogue').innerHTML=[['最近用户消息时间',u.timestamp||'-'],['最近用户消息',u.summary||'-'],['最近助手消息时间',a.timestamp||'-'],['最近助手消息',a.summary||'-']].map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
  document.getElementById('traderMemory').textContent=d.trader_memory_head||'-';
  const ev=d.trader_events||[];
  document.getElementById('traderEvents').innerHTML=ev.length?ev.slice().reverse().map(e=>`<div class='event'><div class='meta'>${esc(e.timestamp||'-')} | ${esc(e.role||'-')}</div><div class='txt'>${esc(e.summary||'-')}</div></div>`).join(''):'<div class="event muted">暂无事件</div>';
  const errs=d.trader_session_errors||[];
  document.getElementById('traderSessionErrors').textContent=errs.length?errs.slice(-30).map(x=>`${x.timestamp} | ${x.role} | ${x.summary}`).join('\\n'):'未发现会话错误关键词';
}

const CLAW402_FAVORITES_KEY='claw402_api_favorites_v1';
const CLAW402_PARAM_META={
  symbol:{label:'\u4ea4\u6613\u5bf9',tip:'\u5982 BTCUSDT / ETHUSDT / AAPL'},
  baseCoin:{label:'\u57fa\u7840\u5e01\u79cd',tip:'\u5982 BTC / ETH'},
  exchange:{label:'\u4ea4\u6613\u6240',tip:'\u5982 Binance / OKX / Bybit'},
  interval:{label:'\u5468\u671f',tip:'\u65f6\u95f4\u7c92\u5ea6\uff0c\u5982 1h/4h/1d'},
  limit:{label:'\u6570\u91cf\u4e0a\u9650',tip:'\u8fd4\u56de\u6761\u6570'},
  duration:{label:'\u7edf\u8ba1\u7a97\u53e3',tip:'\u5982 1h / 24h / 7d'},
  type:{label:'\u7c7b\u578b',tip:'\u6309\u63a5\u53e3\u5b9a\u4e49\u586b\u5199'},
  sortBy:{label:'\u6392\u5e8f\u5b57\u6bb5',tip:'\u6309\u54ea\u4e2a\u5b57\u6bb5\u6392\u5e8f'},
  sortType:{label:'\u6392\u5e8f\u65b9\u5411',tip:'asc \u5347\u5e8f / desc \u964d\u5e8f'},
  size:{label:'\u6837\u672c\u6570',tip:'\u8fd4\u56de\u6761\u6570\u6216 K \u7ebf\u6837\u672c'},
  page:{label:'\u9875\u7801',tip:'\u4ece 1 \u5f00\u59cb'},
  pageSize:{label:'\u6bcf\u9875\u6570\u91cf',tip:'\u5206\u9875\u53c2\u6570'},
  productType:{label:'\u4ea7\u54c1\u7c7b\u578b',tip:'\u5982 SWAP / SPOT'},
  endTime:{label:'\u7ed3\u675f\u65f6\u95f4',tip:'\u53ef\u4f20\u6beb\u79d2\u65f6\u95f4\u6233'},
  exchangeType:{label:'\u4ea4\u6613\u6240\u7c7b\u578b',tip:'\u6309\u63a5\u53e3\u8981\u6c42\u586b\u5199'},
  side:{label:'\u65b9\u5411',tip:'buy/sell \u6216 long/short'},
  amount:{label:'\u91d1\u989d\u9608\u503c',tip:'\u7528\u4e8e\u8fc7\u6ee4\u5927\u989d\u6570\u636e'},
  exchanges:{label:'\u4ea4\u6613\u6240\u96c6\u5408',tip:'\u591a\u4e2a\u7528\u9017\u53f7\u5206\u9694\uff0c\u7559\u7a7a\u8868\u793a\u5168\u90e8'},
  network_slug:{label:'\u94fe\u7f51\u7edc',tip:'\u5982 base / solana / ethereum'},
  contract_address:{label:'\u5408\u7ea6\u5730\u5740',tip:'\u4ee3\u5e01\u6216\u4ea4\u6613\u5bf9\u5408\u7ea6\u5730\u5740'},
  query:{label:'\u641c\u7d22\u5173\u952e\u8bcd',tip:'\u4f8b\uff1aBTC / PEPE / AI'},
  network:{label:'\u7f51\u7edc',tip:'\u6309\u63a5\u53e3\u5b9a\u4e49\u586b\u5199'},
  id:{label:'ID \u5217\u8868',tip:'\u591a\u4e2a ID \u7528\u9017\u53f7\u5206\u9694'},
  slug:{label:'Slug \u522b\u540d',tip:'\u82f1\u6587\u9879\u76ee\u6807\u8bc6'},
  convert:{label:'\u8ba1\u4ef7\u8d27\u5e01',tip:'\u5982 USD / USDT / CNY'},
  aux:{label:'\u9644\u52a0\u5b57\u6bb5',tip:'\u6269\u5c55\u8fd4\u56de\u5185\u5bb9'},
  tag:{label:'\u6807\u7b7e',tip:'\u4f8b\uff1adefi / meme'},
  cryptocurrency_type:{label:'\u8d44\u4ea7\u7c7b\u578b',tip:'coins / tokens'},
  price_min:{label:'\u6700\u5c0f\u4ef7\u683c',tip:'\u4ef7\u683c\u4e0b\u9650'},
  price_max:{label:'\u6700\u5927\u4ef7\u683c',tip:'\u4ef7\u683c\u4e0a\u9650'},
  market_cap_min:{label:'\u6700\u5c0f\u5e02\u503c',tip:'\u5e02\u503c\u4e0b\u9650'},
  market_cap_max:{label:'\u6700\u5927\u5e02\u503c',tip:'\u5e02\u503c\u4e0a\u9650'},
  liquidity_min:{label:'\u6700\u5c0f\u6d41\u52a8\u6027',tip:'\u6d41\u52a8\u6027\u4e0b\u9650'},
  volume_24h_min:{label:'24h\u6700\u5c0f\u6210\u4ea4\u989d',tip:'24\u5c0f\u65f6\u6210\u4ea4\u989d\u4e0b\u9650'},
  feed:{label:'\u884c\u60c5\u6e90',tip:'\u5982 sip / iex'},
  currency:{label:'\u5e01\u79cd',tip:'\u7ed3\u7b97\u6216\u8ba1\u4ef7\u5e01\u79cd'},
  timeframe:{label:'K\u7ebf\u5468\u671f',tip:'\u5982 1Day / 1Hour / 5Min'},
  start:{label:'\u5f00\u59cb\u65f6\u95f4',tip:'ISO \u683c\u5f0f\u6216\u65e5\u671f'},
  end:{label:'\u7ed3\u675f\u65f6\u95f4',tip:'ISO \u683c\u5f0f\u6216\u65e5\u671f'},
  adjustment:{label:'\u590d\u6743\u65b9\u5f0f',tip:'\u5982 split / dividend / all'},
  top:{label:'Top \u6570\u91cf',tip:'\u6392\u540d\u5217\u8868\u8fd4\u56de\u6570'},
  market_type:{label:'\u5e02\u573a\u7c7b\u578b',tip:'stocks / crypto'},
  stocks_ticker:{label:'\u80a1\u7968\u4ee3\u7801',tip:'\u5982 AAPL'},
  stock_ticker:{label:'\u80a1\u7968\u4ee3\u7801',tip:'\u5982 AAPL'},
  ticker:{label:'\u4ee3\u7801',tip:'\u8d44\u4ea7\u6216\u80a1\u7968\u4ee3\u7801'},
  multiplier:{label:'\u805a\u5408\u500d\u6570',tip:'K\u7ebf\u805a\u5408\u53c2\u6570'},
  timespan:{label:'\u65f6\u95f4\u5355\u4f4d',tip:'minute / hour / day'},
  from:{label:'\u5f00\u59cb\u65e5\u671f',tip:'YYYY-MM-DD'},
  to:{label:'\u7ed3\u675f\u65e5\u671f',tip:'YYYY-MM-DD'},
  window:{label:'\u7a97\u53e3\u957f\u5ea6',tip:'\u6280\u672f\u6307\u6807\u8ba1\u7b97\u7a97\u53e3'},
  underlying_asset:{label:'\u6807\u7684\u8d44\u4ea7',tip:'\u5982 AAPL'},
  tickers:{label:'\u4ee3\u7801\u5217\u8868',tip:'\u9017\u53f7\u5206\u9694'},
  outputsize:{label:'\u8f93\u51fa\u6570\u91cf',tip:'\u8fd4\u56de\u6570\u636e\u6761\u6570'},
  time_period:{label:'\u5468\u671f\u53c2\u6570',tip:'\u5982 14 / 20'},
  list_status:{label:'\u4e0a\u5e02\u72b6\u6001',tip:'L \u4e0a\u5e02 / D \u9000\u5e02'},
  ts_code:{label:'\u8bc1\u5238\u4ee3\u7801',tip:'\u5982 000001.SZ'},
  start_date:{label:'\u5f00\u59cb\u65e5\u671f',tip:'YYYYMMDD'},
  end_date:{label:'\u7ed3\u675f\u65e5\u671f',tip:'YYYYMMDD'},
  trade_date:{label:'\u4ea4\u6613\u65e5\u671f',tip:'YYYYMMDD'},
  period:{label:'\u62a5\u544a\u671f',tip:'\u5982 20231231'},
  model:{label:'\u6a21\u578b',tip:'AI \u6a21\u578b\u540d\u79f0'},
  max_tokens:{label:'\u6700\u5927\u8f93\u51faToken',tip:'\u9650\u5236\u56de\u590d\u957f\u5ea6'},
  messages:{label:'\u6d88\u606f\u6570\u7ec4',tip:'Chat \u8bf7\u6c42\u6d88\u606f'},
  input:{label:'\u8f93\u5165\u6587\u672c',tip:'\u7528\u4e8e embedding \u6216\u68c0\u7d22'},
  prompt:{label:'\u63d0\u793a\u8bcd',tip:'\u7528\u4e8e completions'},
  suffix:{label:'\u540e\u7f00',tip:'\u8865\u5168\u540e\u7f00\u6587\u672c'},
};
const CLAW402_ENDPOINT_META={
  '/api/v1/coinank/liquidation/heat-map':'\u6e05\u7b97\u70ed\u529b\u56fe',
  '/api/v1/coinank/liquidation/liq-map':'\u6e05\u7b97\u4ef7\u4f4d\u5bc6\u96c6\u56fe',
  '/api/v1/coinank/liquidation/orders':'\u8fd1\u671f\u5f3a\u5e73\u8ba2\u5355',
  '/api/v1/coinank/funding-rate/current':'\u8d44\u91d1\u8d39\u7387\u6392\u884c',
  '/api/v1/nofx/funding-rate/top':'NoFx \u9ad8\u8d44\u91d1\u8d39\u7387',
  '/api/v1/coinank/oi/all':'\u603b\u6301\u4ed3\u91cf OI',
  '/api/v1/nofx/oi/top-ranking':'OI \u589e\u5e45\u6392\u884c',
  '/api/v1/coinank/market-order/agg-cvd':'\u805a\u5408 CVD',
  '/api/v1/coinank/price/last':'\u6700\u65b0\u4ef7\u683c',
  '/api/v1/coinank/kline/lists':'K\u7ebf\u5217\u8868',
  '/api/v1/coinank/indicator/fear-greed':'\u6050\u614c\u4e0e\u8d2a\u5a6a\u6307\u6570',
  '/api/v1/coinank/indicator/altcoin-season':'\u5c71\u5be8\u5b63\u6307\u6570',
  '/api/v1/coinank/etf/us-btc-inflow':'\u7f8e\u80a1 BTC ETF \u8d44\u91d1\u6d41',
  '/api/v1/coinank/news/list':'\u5e02\u573a\u8d44\u8baf',
  '/api/v1/nofx/netflow/top-ranking':'\u51c0\u6d41\u5165\u6392\u884c',
  '/api/v1/nofx/price/ranking':'\u6da8\u8dcc\u5e45\u6392\u884c',
};
const CLAW402_RESULT_KEY_META={
  symbol:'\u4ea4\u6613\u5bf9',
  pair:'\u4ea4\u6613\u5bf9',
  instid:'\u4ea4\u6613\u5bf9',
  exchange:'\u4ea4\u6613\u6240',
  market:'\u5e02\u573a',
  side:'\u65b9\u5411',
  direction:'\u65b9\u5411',
  price:'\u4ef7\u683c',
  last:'\u6700\u65b0\u4ef7',
  open:'\u5f00\u76d8\u4ef7',
  high:'\u6700\u9ad8\u4ef7',
  low:'\u6700\u4f4e\u4ef7',
  close:'\u6536\u76d8\u4ef7',
  bid:'\u4e70\u4e00',
  ask:'\u5356\u4e00',
  spread:'\u4ef7\u5dee',
  amount:'\u91d1\u989d',
  size:'\u6570\u91cf',
  qty:'\u6570\u91cf',
  quantity:'\u6570\u91cf',
  volume:'\u6210\u4ea4\u91cf',
  vol:'\u6210\u4ea4\u91cf',
  volume24h:'24h\u6210\u4ea4\u91cf',
  turnover:'\u6210\u4ea4\u989d',
  value:'\u6570\u503c',
  score:'\u8bc4\u5206',
  rank:'\u6392\u540d',
  index:'\u6307\u6570',
  ratio:'\u6bd4\u503c',
  rate:'\u6bd4\u7387',
  fundingrate:'\u8d44\u91d1\u8d39\u7387',
  openinterest:'\u6301\u4ed3\u91cf',
  oi:'\u6301\u4ed3\u91cf',
  changepct:'\u6da8\u8dcc\u5e45',
  percent:'\u767e\u5206\u6bd4',
  pct:'\u767e\u5206\u6bd4',
  change:'\u53d8\u52a8',
  long:'\u591a\u5934',
  short:'\u7a7a\u5934',
  timestamp:'\u65f6\u95f4\u6233',
  ts:'\u65f6\u95f4\u6233',
  time:'\u65f6\u95f4',
  date:'\u65e5\u671f',
  createdat:'\u521b\u5efa\u65f6\u95f4',
  updatedat:'\u66f4\u65b0\u65f6\u95f4',
  interval:'\u5468\u671f',
  timeframe:'\u5468\u671f',
  period:'\u5468\u671f',
  min:'\u6700\u5c0f\u503c',
  max:'\u6700\u5927\u503c',
  avg:'\u5747\u503c',
  mean:'\u5747\u503c',
  median:'\u4e2d\u4f4d\u6570',
  count:'\u6570\u91cf',
  total:'\u603b\u8ba1',
};
const CLAW402_RESULT_TOKEN_META={
  symbol:'\u4ea4\u6613\u5bf9',
  pair:'\u4ea4\u6613\u5bf9',
  exchange:'\u4ea4\u6613\u6240',
  market:'\u5e02\u573a',
  price:'\u4ef7\u683c',
  amount:'\u91d1\u989d',
  qty:'\u6570\u91cf',
  size:'\u6570\u91cf',
  volume:'\u6210\u4ea4\u91cf',
  value:'\u6570\u503c',
  funding:'\u8d44\u91d1',
  rate:'\u6bd4\u7387',
  open:'\u5f00\u76d8',
  close:'\u6536\u76d8',
  high:'\u6700\u9ad8',
  low:'\u6700\u4f4e',
  interest:'\u6301\u4ed3',
  side:'\u65b9\u5411',
  direction:'\u65b9\u5411',
  score:'\u8bc4\u5206',
  rank:'\u6392\u540d',
  change:'\u53d8\u52a8',
  pct:'\u767e\u5206\u6bd4',
  percent:'\u767e\u5206\u6bd4',
  ratio:'\u6bd4\u503c',
  time:'\u65f6\u95f4',
  timestamp:'\u65f6\u95f4\u6233',
  date:'\u65e5\u671f',
  interval:'\u5468\u671f',
  period:'\u5468\u671f',
  min:'\u6700\u5c0f',
  max:'\u6700\u5927',
  avg:'\u5747\u503c',
  total:'\u603b\u8ba1',
  count:'\u6570\u91cf',
};
const CLAW402_PRESET_TEMPLATES=[
  {id:'liq_heat',label:'\u6e05\u7b97\u70ed\u529b\u56fe(BTC)',category_key:'coinank',path:'/api/v1/coinank/liquidation/heat-map',method:'GET',params:{symbol:'BTCUSDT',exchange:'Binance',interval:'1d'}},
  {id:'liq_map',label:'\u6e05\u7b97\u4ef7\u4f4d\u5bc6\u96c6\u56fe',category_key:'coinank',path:'/api/v1/coinank/liquidation/liq-map',method:'GET',params:{symbol:'BTCUSDT',exchange:'Binance',interval:'1d'}},
  {id:'funding_top',label:'\u8d44\u91d1\u8d39\u7387\u6392\u884c',category_key:'coinank',path:'/api/v1/coinank/funding-rate/current',method:'GET',params:{type:'current'}},
  {id:'oi_rank',label:'OI \u589e\u5e45\u6392\u884c',category_key:'nofx',path:'/api/v1/nofx/oi/top-ranking',method:'GET',params:{limit:'20',duration:'1h'}},
  {id:'cvd',label:'\u805a\u5408 CVD(BTC)',category_key:'coinank',path:'/api/v1/coinank/market-order/agg-cvd',method:'GET',params:{exchanges:'',symbol:'BTCUSDT',interval:'1h',size:'24'}},
  {id:'fear_greed',label:'\u6050\u614c\u8d2a\u5a6a\u6307\u6570',category_key:'coinank',path:'/api/v1/coinank/indicator/fear-greed',method:'GET',params:{}},
  {id:'btc_price',label:'BTC \u5b9e\u65f6\u4ef7\u683c',category_key:'coinank',path:'/api/v1/coinank/price/last',method:'GET',params:{symbol:'BTCUSDT',exchange:'Binance',productType:'SWAP'}},
  {id:'news_zh',label:'\u4e2d\u6587\u5e02\u573a\u8d44\u8baf',category_key:'coinank',path:'/api/v1/coinank/news/list',method:'GET',params:{type:'2',lang:'zh',page:'1',pageSize:'10'}},
];
const claw402State={inited:false,catalog:[],categories:[],byPath:{},running:false,favorites:[]};
function _claw402PathLabel(path){
  const p=String(path||'');
  if(CLAW402_ENDPOINT_META[p]) return CLAW402_ENDPOINT_META[p];
  const arr=p.split('/').filter(Boolean);
  const last=arr.length?arr[arr.length-1]:p;
  return last.replace(/-/g,' ').replace(/_/g,' ');
}
function _claw402ParamMeta(name){
  const n=String(name||'');
  if(CLAW402_PARAM_META[n]) return CLAW402_PARAM_META[n];
  return {label:'\u53c2\u6570 '+n, tip:'\u8bf7\u6309\u63a5\u53e3\u6587\u6863\u586b\u5199'};
}
function _claw402NormKey(key){
  return String(key||'')
    .replace(/([a-z0-9])([A-Z])/g,'$1_$2')
    .replace(/[^a-zA-Z0-9]+/g,'_')
    .replace(/^_+|_+$/g,'')
    .toLowerCase();
}
function _claw402ResultKeyLabel(rawKey){
  const key=String(rawKey||'');
  const norm=_claw402NormKey(key);
  const join=norm.replace(/_/g,'');
  if(CLAW402_RESULT_KEY_META[norm]) return CLAW402_RESULT_KEY_META[norm];
  if(CLAW402_RESULT_KEY_META[join]) return CLAW402_RESULT_KEY_META[join];
  const tokens=norm.split('_').filter(Boolean);
  if(!tokens.length) return key;
  const mapped=tokens.map(t=>CLAW402_RESULT_TOKEN_META[t]||t.toUpperCase());
  const changed=tokens.some(t=>Boolean(CLAW402_RESULT_TOKEN_META[t]));
  return changed?mapped.join(' '):key;
}
function _claw402MaybeTime(value, key){
  const raw=String(value===undefined||value===null?'':value).trim();
  if(!raw) return null;
  const norm=_claw402NormKey(key||'');
  const likelyByKey=/(time|date|timestamp|ts|_at)$/.test(norm) || norm==='time' || norm==='date';
  if(typeof value==='number' && Number.isFinite(value)){
    if(value>1000000000000 && value<1000000000000000){
      const d0=new Date(value);
      if(!Number.isNaN(d0.getTime())) return d0;
    }
    if(value>1000000000 && value<20000000000){
      const d1=new Date(value*1000);
      if(!Number.isNaN(d1.getTime())) return d1;
    }
  }
  if(/^\\d{13}$/.test(raw)){
    const d2=new Date(Number(raw));
    if(!Number.isNaN(d2.getTime())) return d2;
  }
  if(/^\\d{10}$/.test(raw)){
    const d3=new Date(Number(raw)*1000);
    if(!Number.isNaN(d3.getTime())) return d3;
  }
  if(!likelyByKey) return null;
  const ms=Date.parse(raw);
  if(!Number.isFinite(ms)) return null;
  const d4=new Date(ms);
  return Number.isNaN(d4.getTime())?null:d4;
}
function _claw402FormatNumByKey(key, num){
  if(!Number.isFinite(num)) return String(num);
  const norm=_claw402NormKey(key||'');
  const abs=Math.abs(num);
  const percentLike=/(pct|percent|ratio|change_rate|funding_rate|apr|apy|rate)$/.test(norm) || /(pct|percent|ratio|funding|rate)/.test(norm);
  if(percentLike && abs<=1.5){
    const digits=abs<0.01?4:2;
    return (num*100).toFixed(digits)+'%';
  }
  if(/bps$|_bps|basis/.test(norm)){
    return num.toFixed(2)+' bps';
  }
  let digits=8;
  if(abs>=1000000) digits=2;
  else if(abs>=1000) digits=4;
  else if(abs>=1) digits=6;
  return num.toLocaleString('zh-CN',{maximumFractionDigits:digits});
}
function _claw402ExplainPrimitive(key, value){
  if(value===null||value===undefined) return '\u7a7a\u503c';
  if(typeof value==='boolean') return value?'\u662f':'\u5426';
  const dt=_claw402MaybeTime(value, key);
  if(dt) return dt.toLocaleString('zh-CN');
  if(typeof value==='number') return _claw402FormatNumByKey(key, value);
  if(typeof value==='string'){
    const s=value.trim();
    if(!s) return '\u7a7a\u5b57\u7b26\u4e32';
    if(/^-?\\d+(\\.\\d+)?$/.test(s) && s.length<24){
      const n=Number(s);
      if(Number.isFinite(n)) return _claw402FormatNumByKey(key, n);
    }
    if(s.length>280) return s.slice(0,280)+'...';
    return s;
  }
  try{return JSON.stringify(value);}catch(_){return String(value);}
}
function _claw402PickMainKeys(obj){
  const keys=Object.keys(obj||{});
  if(!keys.length) return [];
  const normalized={};
  keys.forEach(k=>{normalized[_claw402NormKey(k)]=k;});
  const pri=['symbol','pair','instid','name','exchange','market','side','direction','price','last','open','high','low','close','amount','size','qty','quantity','volume','value','turnover','funding_rate','open_interest','oi','change','change_pct','pct','ratio','score','rank','time','timestamp','date','interval','period'];
  const out=[];
  pri.forEach(k=>{
    const hit=normalized[_claw402NormKey(k)];
    if(hit && out.indexOf(hit)<0) out.push(hit);
  });
  keys.forEach(k=>{
    if(out.indexOf(k)<0 && out.length<8) out.push(k);
  });
  return out.slice(0,8);
}
function _claw402ExplainRows(root){
  const rows=[];
  let lineCount=0;
  const LIMIT=120;
  const push=(depth, key, value, subText)=>{
    if(lineCount>=LIMIT) return;
    const indent=Math.max(0, depth)*14;
    const rawKey=String(key===undefined||key===null?'':key);
    let keyHtml='';
    if(rawKey){
      const zh=_claw402ResultKeyLabel(rawKey);
      keyHtml=(zh===rawKey)
        ? `<span class="claw-exp-key">${esc(rawKey)}</span>`
        : `<span class="claw-exp-key">${esc(zh)}</span> <span class="muted">(${esc(rawKey)})</span>`;
    }
    const valText=String(value===undefined||value===null?'\u7a7a\u503c':value);
    const valHtml=`<span class="claw-exp-val">${esc(valText)}</span>`;
    const head=keyHtml?`${keyHtml}\uff1a${valHtml}`:valHtml;
    const sub=subText?`<div class="claw-exp-sub">${esc(String(subText))}</div>`:'';
    rows.push(`<div class="claw-exp-row" style="margin-left:${indent}px">${head}${sub}</div>`);
    lineCount+=1;
  };
  const walk=(key, value, depth)=>{
    if(lineCount>=LIMIT) return;
    if(Array.isArray(value)){
      const len=value.length;
      push(depth, key, `\u6570\u7ec4\uff0c\u5171 ${len} \u9879`);
      if(!len) return;
      const first=value[0];
      if(first && typeof first==='object' && !Array.isArray(first)){
        const fields=Object.keys(first);
        if(fields.length){
          push(depth+1, '', '\u793a\u4f8b\u5b57\u6bb5\uff1a'+fields.slice(0,8).map(f=>_claw402ResultKeyLabel(f)).join('\u3001'));
        }
        const sampleCount=Math.min(len,3);
        for(let i=0;i<sampleCount;i++){
          const item=value[i];
          if(item && typeof item==='object' && !Array.isArray(item)){
            const ks=_claw402PickMainKeys(item);
            const text=ks.map(k=>`${_claw402ResultKeyLabel(k)}=${_claw402ExplainPrimitive(k,item[k])}`).join('\uff0c');
            push(depth+1, '#'+String(i+1), text||'\u7ed3\u6784\u590d\u6742\uff0c\u8bf7\u53c2\u8003\u5de6\u4fa7 JSON');
          }else{
            push(depth+1, '#'+String(i+1), _claw402ExplainPrimitive(key, item));
          }
        }
        if(len>sampleCount){
          push(depth+1, '', `\u5176\u4f59 ${len-sampleCount} \u9879\u5df2\u7701\u7565`);
        }
        return;
      }
      const take=Math.min(len,6);
      const brief=value.slice(0,take).map(v=>_claw402ExplainPrimitive(key,v)).join('\u3001');
      push(depth+1, '', '\u524d '+take+' \u9879\uff1a'+brief, len>take?`\u5171 ${len} \u9879\u3002`:'');
      return;
    }
    if(value && typeof value==='object'){
      const keys=Object.keys(value);
      const keyTip=keys.slice(0,10).map(k=>_claw402ResultKeyLabel(k)).join('\u3001');
      push(depth, key, `\u5bf9\u8c61\uff0c\u5305\u542b ${keys.length} \u4e2a\u5b57\u6bb5`, keyTip?('\u5173\u952e\u5b57\u6bb5\uff1a'+keyTip):'');
      const picks=_claw402PickMainKeys(value);
      picks.forEach(k=>walk(k, value[k], depth+1));
      if(keys.length>picks.length){
        push(depth+1, '', `\u5176\u4f59 ${keys.length-picks.length} \u4e2a\u5b57\u6bb5\u5df2\u7701\u7565`);
      }
      return;
    }
    push(depth, key, _claw402ExplainPrimitive(key, value));
  };
  walk('', root, 0);
  if(lineCount>=LIMIT){
    rows.push('<div class="claw-exp-sub">\u8fd4\u56de\u5185\u5bb9\u8f83\u591a\uff0c\u5df2\u81ea\u52a8\u622a\u65ad\u3002</div>');
  }
  return rows.join('');
}
function _claw402RenderExplain(payload, path, ok, errMsg){
  const box=document.getElementById('claw402Explain');
  if(!box) return;
  const endpoint=String(path||'');
  const label=_claw402PathLabel(endpoint||'');
  const head=[
    '<div class="claw-exp-head">\u8c03\u7528\u7ed3\u679c\u4e2d\u6587\u89e3\u91ca</div>',
    `<div class="claw-exp-row"><span class="claw-exp-key">\u63a5\u53e3</span>\uff1a<span class="claw-exp-val">${esc(label||'-')}</span> <span class="muted">(${esc(endpoint||'-')})</span></div>`,
  ];
  if(!ok){
    const err=String(errMsg||'\u672a\u77e5\u9519\u8bef');
    let detail='';
    try{
      const s=JSON.stringify(payload===undefined?{}:payload);
      if(s && s!=='{}'){
        detail=s.length>320?(s.slice(0,320)+'...'):s;
      }
    }catch(_){ }
    head.push(`<div class="claw-exp-row"><span class="claw-exp-key">\u72b6\u6001</span>\uff1a<span class="claw-exp-val">\u5931\u8d25</span></div>`);
    head.push(`<div class="claw-exp-row"><span class="claw-exp-key">\u539f\u56e0</span>\uff1a<span class="claw-exp-val">${esc(err)}</span></div>`);
    if(detail){
      head.push(`<div class="claw-exp-row"><span class="claw-exp-key">\u8fd4\u56de\u6458\u8981</span>\uff1a<span class="claw-exp-val">${esc(detail)}</span></div>`);
    }
    box.innerHTML=head.join('');
    return;
  }
  let kind='\u57fa\u7840\u7c7b\u578b';
  let count='-';
  if(Array.isArray(payload)){ kind='\u6570\u7ec4'; count=String(payload.length); }
  else if(payload && typeof payload==='object'){ kind='\u5bf9\u8c61'; count=String(Object.keys(payload).length); }
  head.push(`<div class="claw-exp-row"><span class="claw-exp-key">\u6570\u636e\u7ed3\u6784</span>\uff1a<span class="claw-exp-val">${esc(kind)}</span> <span class="muted">(${esc(count)})</span></div>`);
  const body=_claw402ExplainRows(payload);
  box.innerHTML=head.join('') + '<div class="claw-exp-sep"></div>' + (body||'<div class="claw-exp-row">\u6682\u65e0\u53ef\u89e3\u8bfb\u5b57\u6bb5</div>');
}
function _claw402Endpoint(){
  const sel=document.getElementById('claw402Endpoint');
  if(!sel) return null;
  return claw402State.byPath[String(sel.value||'')]||null;
}
function _claw402LoadFavorites(){
  try{
    const raw=localStorage.getItem(CLAW402_FAVORITES_KEY)||'[]';
    const arr=JSON.parse(raw);
    if(!Array.isArray(arr)) return [];
    return arr.filter(x=>x&&x.path).slice(0,30);
  }catch(_){return [];}
}
function _claw402SaveFavorites(list){
  try{localStorage.setItem(CLAW402_FAVORITES_KEY, JSON.stringify((list||[]).slice(0,30)));}catch(_){ }
}
function _claw402FindFavoriteIndex(path){
  const p=String(path||'');
  return (claw402State.favorites||[]).findIndex(x=>String((x||{}).path||'')===p);
}
function _claw402CurrentSnapshot(){
  const ep=_claw402Endpoint();
  if(!ep) return null;
  const method=String(ep.method||'GET').toUpperCase();
  const postBodyEl=document.getElementById('claw402PostBody');
  const item={
    label:_claw402PathLabel(ep.path),
    path:String(ep.path||''),
    category_key:String(ep.category_key||''),
    method,
    params:_claw402CollectParams(),
    post_body:method==='POST'?String(postBodyEl&&postBodyEl.value||''):'',
  };
  if(item.post_body&&item.post_body.length>4000) item.post_body=item.post_body.slice(0,4000);
  return item;
}
function _claw402SyncFavoriteButton(){
  const btn=document.getElementById('claw402FavToggleBtn');
  const ep=_claw402Endpoint();
  if(!btn){return;}
  if(!ep){btn.textContent='\u6536\u85cf\u5f53\u524d\u63a5\u53e3';btn.disabled=true;return;}
  btn.disabled=false;
  const idx=_claw402FindFavoriteIndex(ep.path);
  btn.textContent=idx>=0?'\u53d6\u6d88\u6536\u85cf\u5f53\u524d\u63a5\u53e3':'\u6536\u85cf\u5f53\u524d\u63a5\u53e3';
}
function _claw402RenderFavorites(){
  const host=document.getElementById('claw402FavList');
  if(!host) return;
  const favs=claw402State.favorites||[];
  if(!favs.length){
    host.innerHTML='<span class="muted">\u6682\u65e0\u6536\u85cf</span>';
    _claw402SyncFavoriteButton();
    return;
  }
  host.innerHTML=favs.map((f,idx)=>`<button class="claw-chip" data-fav-idx="${idx}" title="${esc(f.path||'')}">${esc(f.label||_claw402PathLabel(f.path||''))}</button>`).join('');
  host.querySelectorAll('[data-fav-idx]').forEach(btn=>{
    btn.onclick=()=>{
      const i=Number(btn.getAttribute('data-fav-idx')||-1);
      if(i<0||i>=favs.length) return;
      _claw402ApplyPreset(favs[i], '\u6536\u85cf');
    };
  });
  _claw402SyncFavoriteButton();
}
function _claw402ToggleFavorite(){
  const snap=_claw402CurrentSnapshot();
  const meta=document.getElementById('claw402ReqMeta');
  if(!snap){ if(meta) meta.textContent='\u5f53\u524d\u65e0\u53ef\u6536\u85cf\u63a5\u53e3'; return; }
  const idx=_claw402FindFavoriteIndex(snap.path);
  if(idx>=0){
    claw402State.favorites.splice(idx,1);
    if(meta) meta.textContent='\u5df2\u53d6\u6d88\u6536\u85cf: '+(snap.label||snap.path);
  }else{
    claw402State.favorites.unshift(snap);
    claw402State.favorites=claw402State.favorites.slice(0,30);
    if(meta) meta.textContent='\u5df2\u6536\u85cf: '+(snap.label||snap.path);
  }
  _claw402SaveFavorites(claw402State.favorites);
  _claw402RenderFavorites();
}
function _claw402ClearFavorites(){
  claw402State.favorites=[];
  _claw402SaveFavorites(claw402State.favorites);
  _claw402RenderFavorites();
  const meta=document.getElementById('claw402ReqMeta');
  if(meta) meta.textContent='\u5df2\u6e05\u7a7a\u6240\u6709\u6536\u85cf';
}
function _claw402RenderTemplates(){
  const host=document.getElementById('claw402TplBtns');
  if(!host) return;
  host.innerHTML=CLAW402_PRESET_TEMPLATES.map((t,idx)=>`<button class="claw-chip" data-tpl-idx="${idx}" title="${esc(t.path||'')}">${esc(t.label||('T'+idx))}</button>`).join('');
  host.querySelectorAll('[data-tpl-idx]').forEach(btn=>{
    btn.onclick=()=>{
      const i=Number(btn.getAttribute('data-tpl-idx')||-1);
      if(i<0||i>=CLAW402_PRESET_TEMPLATES.length) return;
      _claw402ApplyPreset(CLAW402_PRESET_TEMPLATES[i], '\u6a21\u677f');
    };
  });
}
function _claw402SelectEndpoint(path, categoryKey){
  const cSel=document.getElementById('claw402Category');
  const eSel=document.getElementById('claw402Endpoint');
  if(!cSel||!eSel) return false;
  let cat=String(categoryKey||'');
  if(!cat){
    const ep=claw402State.byPath[String(path||'')]||{};
    cat=String(ep.category_key||'');
  }
  if(cat){
    const has=Array.from(cSel.options).some(o=>String(o.value||'')===cat);
    if(has) cSel.value=cat;
  }
  _claw402RenderEndpointOptions();
  if(!path) return false;
  let found=Array.from(eSel.options).some(o=>String(o.value||'')===String(path||''));
  if(!found){
    const ep=claw402State.byPath[String(path||'')]||{};
    const cat2=String(ep.category_key||'');
    if(cat2 && cSel.value!==cat2){
      cSel.value=cat2;
      _claw402RenderEndpointOptions();
      found=Array.from(eSel.options).some(o=>String(o.value||'')===String(path||''));
    }
  }
  if(!found) return false;
  eSel.value=String(path||'');
  _claw402RenderParams();
  return true;
}
function _claw402SetParamValues(params){
  const box=document.getElementById('claw402ParamBox');
  if(!box||!params||typeof params!=='object') return;
  Object.keys(params).forEach(k=>{
    const el=box.querySelector('[data-param="'+k.replace(/"/g,'')+'"]');
    if(el) el.value=String(params[k]===undefined||params[k]===null?'':params[k]);
  });
  _claw402UpdateCmdPreview();
}
function _claw402ApplyPreset(preset, sourceLabel){
  if(!preset) return;
  const ok=_claw402SelectEndpoint(preset.path, preset.category_key);
  if(!ok){
    const status=document.getElementById('claw402ReqMeta');
    if(status) status.textContent='\u672a\u627e\u5230\u63a5\u53e3: '+String(preset.path||'');
    return;
  }
  _claw402SetParamValues(preset.params||{});
  const ep=_claw402Endpoint();
  const postBody=document.getElementById('claw402PostBody');
  if(ep&&String(ep.method||'GET').toUpperCase()==='POST'&&postBody){
    if(String(preset.post_body||'').trim()) postBody.value=String(preset.post_body);
  }
  _claw402UpdateCmdPreview();
  const status=document.getElementById('claw402ReqMeta');
  if(status){
    const src=sourceLabel||'\u6a21\u677f';
    status.textContent='\u5df2\u5e94\u7528'+src+': '+String(preset.label||_claw402PathLabel(preset.path||''));
  }
  _claw402SyncFavoriteButton();
}
function _claw402RenderWallet(d){
  const box=document.getElementById('claw402WalletBox');
  if(!box) return;
  const bal=(d&&d.balances)||{};
  const errs=Array.isArray(d&&d.errors)?d.errors:[];
  box.innerHTML=[
    ['\u94b1\u5305\u5df2\u914d\u7f6e', d&&d.configured?'\u662f':'\u5426'],
    ['\u7f51\u5173', (d&&d.gateway)||'-'],
    ['\u94b1\u5305\u5730\u5740', (d&&d.address)||'-'],
    ['Base ETH', bal.base_eth===null||bal.base_eth===undefined?'-':fmt(bal.base_eth)],
    ['Base USDC', bal.base_usdc===null||bal.base_usdc===undefined?'-':fmt(bal.base_usdc)],
    ['\u6700\u8fd1\u5237\u65b0', (d&&d.updated_at)||'-'],
    ['\u72b6\u6001', d&&d.ok?'\u6b63\u5e38':'\u5f02\u5e38'],
    ['\u9519\u8bef', errs.length?errs.join(' | '):'-'],
  ].map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
}
function _claw402RenderUsage(d){
  const box=document.getElementById('claw402UsageBox');
  if(box){
    box.innerHTML=[
      ['\u603b\u8c03\u7528', d&&d.total_calls||0],
      ['\u6210\u529f', d&&d.success_calls||0],
      ['\u5931\u8d25', d&&d.failed_calls||0],
      ['\u6210\u529f\u7387', ((d&&d.success_rate)||0)+'%'],
      ['\u63a5\u53e3\u6570', d&&d.endpoint_count||0],
      ['\u6700\u8fd1\u8c03\u7528', (d&&d.last_call_at)||'-'],
    ].map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
  }
  const tb=document.getElementById('claw402RecentTbl');
  if(tb){
    const rows=Array.isArray(d&&d.recent_calls)?d.recent_calls:[];
    tb.innerHTML='<tr><th>\u65f6\u95f4</th><th>\u63a5\u53e3</th><th>\u8017\u65f6(ms)</th><th>\u72b6\u6001</th><th>\u9519\u8bef</th></tr>' + (rows.slice(0,24).map(r=>`<tr><td>${esc(r.time||'-')}</td><td>${esc(_claw402PathLabel(r.endpoint||''))} <span class="muted">(${esc(r.endpoint||'')})</span></td><td>${esc(r.duration_ms||0)}</td><td class="${r.ok?'ok':'bad'}">${r.ok?'\u6210\u529f':'\u5931\u8d25'}</td><td>${esc(r.error||'-')}</td></tr>`).join('')||'<tr><td colspan="5" style="text-align:center;color:#999">\u6682\u65e0\u8bb0\u5f55</td></tr>');
  }
}
async function refreshClaw402Wallet(showStatus){
  try{
    const r=await fetch('/api/claw402/wallet?_='+Date.now(),{cache:'no-store'});
    const d=r.ok?await r.json():{ok:false,error:'http_'+r.status};
    _claw402RenderWallet(d||{});
    if(showStatus){
      const meta=document.getElementById('claw402ReqMeta');
      if(meta) meta.textContent=(d&&d.ok)?'\u94b1\u5305\u72b6\u6001\u5df2\u5237\u65b0':'\u94b1\u5305\u72b6\u6001\u5237\u65b0\u5931\u8d25';
    }
  }catch(e){
    _claw402RenderWallet({ok:false,configured:false,address:'-',balances:{},errors:[String((e&&e.message)||e||'unknown')]});
  }
}
async function refreshClaw402Usage(showStatus){
  try{
    const r=await fetch('/api/claw402/usage?_='+Date.now(),{cache:'no-store'});
    const d=r.ok?await r.json():{ok:false,error:'http_'+r.status};
    _claw402RenderUsage(d||{});
    if(showStatus){
      const meta=document.getElementById('claw402ReqMeta');
      if(meta) meta.textContent=(d&&d.ok)?'\u8c03\u7528\u7edf\u8ba1\u5df2\u5237\u65b0':'\u8c03\u7528\u7edf\u8ba1\u5237\u65b0\u5931\u8d25';
    }
  }catch(e){
    _claw402RenderUsage({ok:false,recent_calls:[]});
  }
}
function _claw402RenderEndpointOptions(){
  const cSel=document.getElementById('claw402Category');
  const eSel=document.getElementById('claw402Endpoint');
  if(!cSel||!eSel) return;
  const ck=String(cSel.value||'');
  const list=(claw402State.catalog||[]).filter(ep=>!ck||String(ep.category_key||'')===ck);
  eSel.innerHTML=list.map(ep=>{
    const label=_claw402PathLabel(ep.path||'');
    return `<option value="${esc(ep.path||'')}">${esc(label)} | ${esc(ep.path||'')} [${esc(String(ep.method||'GET').toUpperCase())}]</option>`;
  }).join('');
  _claw402RenderParams();
}
function _claw402RenderParams(){
  const ep=_claw402Endpoint();
  const box=document.getElementById('claw402ParamBox');
  const postWrap=document.getElementById('claw402PostWrap');
  const postBody=document.getElementById('claw402PostBody');
  const status=document.getElementById('claw402CatalogStatus');
  if(!box||!postWrap||!postBody) return;
  if(!ep){
    box.innerHTML='<div class="muted">\u672a\u627e\u5230\u63a5\u53e3\u5b9a\u4e49</div>';
    postWrap.style.display='none';
    _claw402SyncFavoriteButton();
    return;
  }
  if(status){
    const desc=String(ep.description||'').trim();
    status.textContent=desc?('\u63a5\u53e3\u8bf4\u660e: '+desc):('\u5f53\u524d\u63a5\u53e3: '+_claw402PathLabel(ep.path||''));
  }
  const params=Array.isArray(ep.params)?ep.params:[];
  box.innerHTML=params.length?params.map(p=>{
    const n=String(p.name||'');
    const hint=String(p.hint||n||'').trim();
    const def=String(p.default===undefined||p.default===null?'':p.default);
    const meta=_claw402ParamMeta(n);
    const title=`${meta.label} (${n})`;
    const tips=[];
    if(meta.tip) tips.push(meta.tip);
    if(hint && hint!==n) tips.push('\u63a5\u53e3\u63d0\u793a: '+hint);
    const tipText=tips.join(' | ') || '\u8bf7\u8f93\u5165\u53c2\u6570\u503c';
    const opts=Array.isArray(p.options)?p.options.slice():[];
    if(def && opts.indexOf(def)<0) opts.unshift(def);
    const field=opts.length
      ? `<select data-param="${esc(n)}">${opts.map(o=>`<option value="${esc(o)}" ${String(o)===def?'selected':''}>${esc(o)}</option>`).join('')}</select>`
      : `<input data-param="${esc(n)}" value="${esc(def)}" placeholder="${esc(meta.tip||n)}" />`;
    return `<div class="claw-param"><div class="name">${esc(title)}</div>${field}<div class="hint">${esc(tipText)}</div></div>`;
  }).join(''):'<div class="muted">\u5f53\u524d\u63a5\u53e3\u65e0 query \u53c2\u6570</div>';

  postWrap.style.display=String(ep.method||'GET').toUpperCase()==='POST'?'block':'none';
  if(String(ep.method||'GET').toUpperCase()==='POST'){
    const sb=String(ep.sample_post_body||'').trim();
    if(sb){ postBody.value=sb; }
    else if(!String(postBody.value||'').trim()){ postBody.value='{"messages":[{"role":"user","content":"hello"}]}'; }
  }
  box.querySelectorAll('[data-param]').forEach(el=>{el.addEventListener('input',_claw402UpdateCmdPreview);el.addEventListener('change',_claw402UpdateCmdPreview);});
  postBody.oninput=_claw402UpdateCmdPreview;
  _claw402UpdateCmdPreview();
  _claw402SyncFavoriteButton();
}
function _claw402CollectParams(){
  const out={};
  const box=document.getElementById('claw402ParamBox');
  if(!box) return out;
  box.querySelectorAll('[data-param]').forEach(el=>{
    const k=String(el.getAttribute('data-param')||'').trim();
    const v=String((el.value===undefined||el.value===null)?'':el.value).trim();
    if(k&&v) out[k]=v;
  });
  return out;
}
function _claw402UpdateCmdPreview(){
  const ep=_claw402Endpoint();
  const box=document.getElementById('claw402CmdPreview');
  if(!box||!ep){ if(box) box.textContent='-'; return; }
  const params=_claw402CollectParams();
  const kv=Object.keys(params).sort().map(k=>`${k}=${params[k]}`);
  const method=String(ep.method||'GET').toUpperCase();
  if(method==='POST'){
    const body=document.getElementById('claw402PostBody');
    let pb=String(body&&body.value||'{}').trim();
    if(pb.length>220) pb=pb.slice(0,220)+'...';
    box.textContent='node scripts/query.mjs '+ep.path+(kv.length?('?'+new URLSearchParams(params).toString()):'')+" --post '"+pb+"'";
  }else{
    box.textContent='node scripts/query.mjs '+ep.path+(kv.length?(' '+kv.join(' ')):'');
  }
}
async function runClaw402Request(){
  if(claw402State.running) return;
  const ep=_claw402Endpoint();
  const meta=document.getElementById('claw402ReqMeta');
  const pre=document.getElementById('claw402Result');
  const exp=document.getElementById('claw402Explain');
  if(!ep||!meta||!pre) return;
  const method=String(ep.method||'GET').toUpperCase();
  const params=_claw402CollectParams();
  const postBody=document.getElementById('claw402PostBody');
  const payload={path:ep.path,method,params,post_body:method==='POST'?String(postBody&&postBody.value||'{}'):'{}'};
  claw402State.running=true;
  meta.textContent='\u8bf7\u6c42\u4e2d\uff0c\u8bf7\u7a0d\u5019...';
  if(exp){
    exp.innerHTML='<div class="claw-exp-row">\u6b63\u5728\u751f\u6210\u4e2d\u6587\u89e3\u91ca\uff0c\u8bf7\u7a0d\u5019...</div>';
  }
  try{
    const r=await fetch('/api/claw402/request',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
    const d=r.ok?await r.json():{ok:false,error:'http_'+r.status};
    if(d&&d.ok){
      meta.textContent=`\u8c03\u7528\u6210\u529f | ${_claw402PathLabel(d.path||'')} | \u8017\u65f6 ${d.duration_ms||0} ms`;
      const resp=(d.response===undefined)?{}:d.response;
      pre.textContent=JSON.stringify(resp,null,2);
      _claw402RenderExplain(resp, d.path||ep.path, true, '');
    }else{
      meta.textContent='\u8c03\u7528\u5931\u8d25: '+String((d&&d.error)||'unknown');
      pre.textContent=JSON.stringify(d||{},null,2);
      _claw402RenderExplain((d&&d.response!==undefined)?d.response:d, (d&&d.path)||ep.path, false, String((d&&d.error)||'unknown'));
    }
  }catch(e){
    const errMsg=String((e&&e.message)||e||'unknown');
    meta.textContent='\u8c03\u7528\u5f02\u5e38: '+errMsg;
    pre.textContent=String((e&&e.stack)||e||'unknown');
    _claw402RenderExplain({error:errMsg}, ep.path, false, errMsg);
  }finally{
    claw402State.running=false;
    refreshClaw402Usage(false);
    refreshClaw402Wallet(false);
  }
}
async function initClaw402Hub(){
  if(claw402State.inited) return;
  const cSel=document.getElementById('claw402Category');
  const eSel=document.getElementById('claw402Endpoint');
  const status=document.getElementById('claw402CatalogStatus');
  const runBtn=document.getElementById('claw402RunBtn');
  const rfBtn=document.getElementById('claw402RefreshBtn');
  const favBtn=document.getElementById('claw402FavToggleBtn');
  const favClrBtn=document.getElementById('claw402FavClearBtn');
  if(!cSel||!eSel||!status||!runBtn||!rfBtn||!favBtn||!favClrBtn) return;
  status.textContent='\u6b63\u5728\u8bfb\u53d6 claw402 skill \u63a5\u53e3\u76ee\u5f55...';
  try{
    const r=await fetch('/api/claw402/catalog?_='+Date.now(),{cache:'no-store'});
    const d=r.ok?await r.json():{ok:false,error:'http_'+r.status};
    if(!d||!d.ok){
      status.textContent='\u76ee\u5f55\u52a0\u8f7d\u5931\u8d25: '+String((d&&d.error)||'unknown');
      return;
    }
    claw402State.catalog=Array.isArray(d.endpoints)?d.endpoints:[];
    claw402State.categories=Array.isArray(d.categories)?d.categories:[];
    claw402State.byPath={};
    claw402State.catalog.forEach(ep=>{if(ep&&ep.path) claw402State.byPath[String(ep.path)]=ep;});

    cSel.innerHTML=claw402State.categories.map(c=>`<option value="${esc(c.key||'')}">${esc(c.label||c.key||'')} (${Number(c.count||0)})</option>`).join('');
    if(!cSel.value && claw402State.categories.length){ cSel.value=String(claw402State.categories[0].key||''); }

    cSel.onchange=_claw402RenderEndpointOptions;
    eSel.onchange=_claw402RenderParams;
    runBtn.onclick=runClaw402Request;
    rfBtn.onclick=()=>{refreshClaw402Wallet(true);refreshClaw402Usage(true);};
    favBtn.onclick=_claw402ToggleFavorite;
    favClrBtn.onclick=_claw402ClearFavorites;

    claw402State.favorites=_claw402LoadFavorites();
    _claw402RenderTemplates();
    _claw402RenderEndpointOptions();
    _claw402RenderFavorites();
    await refreshClaw402Wallet(false);
    await refreshClaw402Usage(false);

    // apply first preset for quick start
    if(CLAW402_PRESET_TEMPLATES.length){
      _claw402ApplyPreset(CLAW402_PRESET_TEMPLATES[0], '\u9ed8\u8ba4\u6a21\u677f');
    }

    status.textContent=`\u76ee\u5f55\u5df2\u52a0\u8f7d: ${claw402State.catalog.length} \u4e2a\u63a5\u53e3\uff0c${claw402State.categories.length} \u4e2a\u5206\u7c7b`;
    claw402State.inited=true;
  }catch(e){
    status.textContent='\u76ee\u5f55\u52a0\u8f7d\u5f02\u5e38: '+String((e&&e.message)||e||'unknown');
  }
}

async function load(){
  console.log('[load] 开始加载');
  try{
    const t0=Date.now();
    console.log('[load] 发送 API 请求');
    const resp=await fetch('/api/status?_='+Date.now(),{cache:'no-store'});
    console.log('[load] API 响应状态:', resp.status);
    if(!resp.ok) throw new Error('http_'+resp.status);
    const d=await resp.json();
    console.log('[load] API响应时间:',Date.now()-t0,'ms');
    console.log('[load] 数据:', d.mode, d.signal?.score);
    currentMode=d.mode||'paper';
    applyStatic();
    document.getElementById('btnPaper').className=currentMode==='paper'?'active':'';
    document.getElementById('btnLive').className=currentMode==='live'?'active':'';
    document.getElementById('modeTag').textContent=currentMode==='live'?t('modeLive'):t('modePaper');
    document.getElementById('updated').textContent=(currentLang==='zh'?'更新时间: ':'updated: ')+new Date(d.timestamp).toLocaleString('zh-CN');
    document.getElementById('top').innerHTML=[
      [t('status'),d.trader_status],
      [t('proc'),d.processing_state],
      [t('score'),((d.signal||{}).score)],
      [t('conf'),((d.signal||{}).confidence)],
      ['BTC基准分',((d.signal||{}).benchmark_score)],
      ['结构建议',((d.signal||{}).preferred_decision)],
      [t('track'),trackName(d.active_strategy||'--')],
      [t('next'),d.next_check||'--']
    ].map(x=>`<div class='card'><div class='muted'>${x[0]}</div><div>${esc(x[1])}</div></div>`).join('');
    const tr=d.strategy_tracks||{};
    document.getElementById('trackInfo').innerHTML=`<div>${t('intr')}: ${fmt(((tr.intraday||{}).score))} ${esc(((tr.intraday||{}).direction)||'--')}</div><div>${t('sw')}: ${fmt(((tr.swing||{}).score))} ${esc(((tr.swing||{}).direction)||'--')}</div><div>${t('sel')}: ${esc(trackName(((tr.selected||{}).name)||'--'))} / ${esc(((tr.selected||{}).direction)||'--')}</div>`;
    renderNoviceGuide(d);
    renderReleaseNotes(d);
    renderStrategyOverview(d);
    console.log('[load] 开始渲染账户');
    renderAccount(d);
    console.log('[load] 开始渲染因子');
    renderFactors(d);
    renderHistory(d);
    renderLiquidationFactorPage(d);
    renderTraderMonitor(d);
    renderErrors(d);
    console.log('[load] 开始绘制雷达图');
    drawRadar((d.radar||{}).labels||[],(d.radar||{}).scores||[]);
    console.log('[load] 开始渲染清算热力图');
    await renderLiquidationHeatmap();
    liqSectionBootstrapped=true;
    console.log('[load] 渲染完成，总耗时:',Date.now()-t0,'ms');
  }catch(err){
    console.error('[load] 捕获到错误:', err);
    const msg=(err&&err.message)?err.message:String(err||'unknown');
    const box=document.getElementById('errorBox');
    if(box) box.textContent='加载失败: '+msg+'\\n请检查网络';
    const acct=document.getElementById('acct');
    if(acct) acct.textContent='loading failed: '+msg;
    const top=document.getElementById('top');
    if(top&&top.innerHTML.trim()==='') top.innerHTML=`<div class='card'><div class='muted'>状态</div><div>数据加载失败</div></div>`;
    console.error('load failed',err);
  }
}

let btLast = null;
let btAutoRefreshOnce = false;
function toLocalInputValue(ms){try{const d=new Date(ms);const y=d.getFullYear();const m=String(d.getMonth()+1).padStart(2,'0');const dd=String(d.getDate()).padStart(2,'0');const hh=String(d.getHours()).padStart(2,'0');const mi=String(d.getMinutes()).padStart(2,'0');return `${y}-${m}-${dd}T${hh}:${mi}`;}catch(_){return ''}}
function lineCanvas(id, points, key, color, yAsPercent){
  const c=document.getElementById(id); if(!c) return; const ctx=c.getContext('2d'); const w=c.width,h=c.height;
  ctx.clearRect(0,0,w,h); ctx.fillStyle='#fff'; ctx.fillRect(0,0,w,h);
  if(!Array.isArray(points) || points.length<2){ctx.fillStyle='#666';ctx.font='12px sans-serif';ctx.fillText('\u6682\u65e0\u6570\u636e',12,20);return;}
  const vals=points.map(p=>Number(p[key]||0));
  let min=Math.min(...vals), max=Math.max(...vals);
  if(!Number.isFinite(min)||!Number.isFinite(max)){return;}
  if(Math.abs(max-min)<1e-9){max=min+1e-6;}
  const pad=26;
  ctx.strokeStyle='#eee'; ctx.lineWidth=1;
  for(let i=0;i<5;i++){const y=pad+(h-pad*2)*i/4; ctx.beginPath(); ctx.moveTo(pad,y); ctx.lineTo(w-pad,y); ctx.stroke();}
  ctx.strokeStyle=color||'#111'; ctx.lineWidth=2; ctx.beginPath();
  for(let i=0;i<vals.length;i++){
    const x=pad+(w-pad*2)*(i/(vals.length-1));
    const y=pad+(h-pad*2)*(1-(vals[i]-min)/(max-min));
    if(i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
  }
  ctx.stroke();
  ctx.fillStyle='#333'; ctx.font='12px sans-serif';
  const suffix=yAsPercent?'%':'';
  ctx.fillText(String(min.toFixed(2))+suffix,6,h-pad+4);
  ctx.fillText(String(max.toFixed(2))+suffix,6,pad+4);
}
const klineFactorState = {};
function _clampNum(x, lo, hi){ return Math.max(lo, Math.min(hi, x)); }
function _getKlineState(id, total){
  let s = klineFactorState[id];
  if(!s){
    s = {start:0, window:Math.max(60, Math.min(220, Math.max(60,total||220))), total:Math.max(0,total||0), listeners:false, drag:false, lastX:0, touchMode:'none', touchDist:0, points:[]};
    klineFactorState[id] = s;
  }
  if(!Number.isFinite(s.window) || s.window < 24){
    s.window = Math.max(60, Math.min(220, Math.max(60,total||220)));
  }
  const t = Math.max(0, Number(total)||0);
  const wasRight = (Number(s.start)||0) + (Number(s.window)||0) >= Math.max(0, (Number(s.total)||0) - 1);
  s.total = t;
  if(t >= 4){
    s.window = _clampNum(Math.round(s.window), 24, t);
    if(wasRight){ s.start = Math.max(0, t - s.window); }
    s.start = _clampNum(Math.round(s.start||0), 0, Math.max(0, t - s.window));
  }else{
    s.start = 0;
  }
  return s;
}
function _attachKlineInteractions(id, c){
  const s = klineFactorState[id];
  if(!s || s.listeners) return;
  const redraw = ()=>{ const st=klineFactorState[id]||{}; klineFactorCanvas(id, st.points||[]); };
  const pxToBars = (px)=>{ const st=klineFactorState[id]||{}; const w=Math.max(1, c.clientWidth||c.width||1); return Math.round((px / w) * Math.max(24, st.window||24)); };

  c.addEventListener('wheel', (ev)=>{
    ev.preventDefault();
    const st = klineFactorState[id]; if(!st || st.total < 8) return;
    const oldWindow = st.window;
    const zoomIn = ev.deltaY < 0;
    const step = Math.max(2, Math.round(oldWindow * 0.12));
    const nextWindow = _clampNum(oldWindow + (zoomIn ? -step : step), 24, st.total);
    if(nextWindow === oldWindow) return;
    const rect = c.getBoundingClientRect();
    const relX = _clampNum((ev.clientX - rect.left) / Math.max(1, rect.width), 0, 1);
    const anchor = st.start + Math.round(relX * oldWindow);
    st.window = nextWindow;
    st.start = _clampNum(anchor - Math.round(relX * nextWindow), 0, Math.max(0, st.total - st.window));
    redraw();
  }, {passive:false});

  c.addEventListener('mousedown', (ev)=>{
    const st = klineFactorState[id]; if(!st) return;
    st.drag = true;
    st.lastX = ev.clientX;
  });
  c.addEventListener('mousemove', (ev)=>{
    const st = klineFactorState[id]; if(!st || !st.drag) return;
    const dx = ev.clientX - st.lastX;
    st.lastX = ev.clientX;
    const bars = pxToBars(dx);
    if(!bars) return;
    st.start = _clampNum(st.start - bars, 0, Math.max(0, st.total - st.window));
    redraw();
  });
  c.addEventListener('mouseleave', ()=>{ const st=klineFactorState[id]; if(st) st.drag=false; });
  window.addEventListener('mouseup', ()=>{ const st=klineFactorState[id]; if(st) st.drag=false; });

  c.addEventListener('dblclick', ()=>{
    const st = klineFactorState[id]; if(!st) return;
    st.window = _clampNum(Math.max(60, Math.min(220, st.total)), 24, Math.max(24, st.total||24));
    st.start = Math.max(0, st.total - st.window);
    redraw();
  });

  c.addEventListener('touchstart', (ev)=>{
    const st = klineFactorState[id]; if(!st) return;
    if(ev.touches.length===1){
      st.touchMode='pan';
      st.lastX=ev.touches[0].clientX;
    }else if(ev.touches.length>=2){
      st.touchMode='pinch';
      const t0=ev.touches[0], t1=ev.touches[1];
      st.touchDist=Math.hypot((t1.clientX-t0.clientX),(t1.clientY-t0.clientY));
    }
  }, {passive:true});
  c.addEventListener('touchmove', (ev)=>{
    const st = klineFactorState[id]; if(!st) return;
    if(st.touchMode==='pan' && ev.touches.length===1){
      ev.preventDefault();
      const x=ev.touches[0].clientX;
      const dx=x-st.lastX;
      st.lastX=x;
      const bars=pxToBars(dx);
      if(!bars) return;
      st.start = _clampNum(st.start - bars, 0, Math.max(0, st.total - st.window));
      redraw();
      return;
    }
    if(st.touchMode==='pinch' && ev.touches.length>=2){
      ev.preventDefault();
      const t0=ev.touches[0], t1=ev.touches[1];
      const dist=Math.hypot((t1.clientX-t0.clientX),(t1.clientY-t0.clientY));
      if(!Number.isFinite(dist) || dist<=1 || !Number.isFinite(st.touchDist) || st.touchDist<=1){ st.touchDist=dist; return; }
      const ratio = st.touchDist / dist;
      const nextWindow = _clampNum(Math.round(st.window * ratio), 24, Math.max(24, st.total));
      if(nextWindow !== st.window){
        const center = st.start + Math.round(st.window/2);
        st.window = nextWindow;
        st.start = _clampNum(center - Math.round(nextWindow/2), 0, Math.max(0, st.total - st.window));
        redraw();
      }
      st.touchDist = dist;
    }
  }, {passive:false});
  c.addEventListener('touchend', ()=>{ const st=klineFactorState[id]; if(st) st.touchMode='none'; }, {passive:true});

  s.listeners = true;
}
function klineFactorCanvas(id, points){
  const c=document.getElementById(id); if(!c) return;
  const ctx=c.getContext('2d'); const w=c.width,h=c.height;
  const all = Array.isArray(points) ? points : [];
  const st = _getKlineState(id, all.length);
  st.points = all;
  _attachKlineInteractions(id, c);
  ctx.clearRect(0,0,w,h); ctx.fillStyle='#fff'; ctx.fillRect(0,0,w,h);
  if(all.length<4){ctx.fillStyle='#666';ctx.font='12px sans-serif';ctx.fillText('\u6682\u65e0\u6570\u636e',12,20);return;}

  const windowBars = _clampNum(Math.round(st.window||all.length), 24, all.length);
  st.window = windowBars;
  st.start = _clampNum(Math.round(st.start||0), 0, Math.max(0, all.length - windowBars));
  const startIdx = st.start;
  const endIdx = Math.min(all.length, startIdx + windowBars);
  const view = all.slice(startIdx, endIdx);
  if(view.length < 2){ctx.fillStyle='#666';ctx.font='12px sans-serif';ctx.fillText('\u6682\u65e0\u6570\u636e',12,20);return;}

  const padL=42,padR=16,padT=16,padB=16;
  const topH=Math.floor(h*0.66);
  const botY=topH+10;
  const botH=h-botY-padB;

  const highs=view.map(p=>Number(p.high||p.close||0)).filter(v=>Number.isFinite(v));
  const lows=view.map(p=>Number(p.low||p.close||0)).filter(v=>Number.isFinite(v));
  if(!highs.length||!lows.length){ctx.fillStyle='#666';ctx.fillText('\u4ef7\u683c\u6570\u636e\u4e3a\u7a7a',12,20);return;}
  let pMin=Math.min(...lows), pMax=Math.max(...highs);
  if(!Number.isFinite(pMin)||!Number.isFinite(pMax) || Math.abs(pMax-pMin)<1e-9){pMax=pMin+1e-6;}

  const n=view.length;
  const xAt=(i)=>padL+(w-padL-padR)*(i/(Math.max(1,n-1)));
  const yPrice=(v)=>padT+(topH-padT-8)*(1-(v-pMin)/(pMax-pMin));

  ctx.strokeStyle='#eeeeee'; ctx.lineWidth=1;
  for(let i=0;i<4;i++){
    const y=padT+(topH-padT-8)*(i/3);
    ctx.beginPath(); ctx.moveTo(padL,y); ctx.lineTo(w-padR,y); ctx.stroke();
  }

  const bodyW=Math.max(2, Math.min(8, (w-padL-padR)/Math.max(20,n)*0.7));
  for(let i=0;i<n;i++){
    const p=view[i]||{};
    const o=Number(p.open||0), h1=Number(p.high||0), l1=Number(p.low||0), cl=Number(p.close||0);
    if(![o,h1,l1,cl].every(Number.isFinite)) continue;
    const x=xAt(i);
    const yH=yPrice(h1), yL=yPrice(l1), yO=yPrice(o), yC=yPrice(cl);
    const up=cl>=o;
    ctx.strokeStyle=up?'#0a7d2f':'#b42318';
    ctx.beginPath(); ctx.moveTo(x,yH); ctx.lineTo(x,yL); ctx.stroke();
    const yTop=Math.min(yO,yC), yBot=Math.max(yO,yC);
    const bh=Math.max(1.2, yBot-yTop);
    ctx.fillStyle=up?'#0a7d2f':'#b42318';
    ctx.fillRect(x-bodyW/2,yTop,bodyW,bh);
  }

  const fMin=-1.0, fMax=1.0;
  const yF=(v)=>botY + (botH)*(1-((v-fMin)/(fMax-fMin)));
  ctx.strokeStyle='#f1f1f1';
  for(let i=0;i<3;i++){
    const y=botY + botH*(i/2);
    ctx.beginPath(); ctx.moveTo(padL,y); ctx.lineTo(w-padR,y); ctx.stroke();
  }
  ctx.strokeStyle='#cccccc'; ctx.beginPath(); ctx.moveTo(padL,yF(0)); ctx.lineTo(w-padR,yF(0)); ctx.stroke();

  ctx.strokeStyle='#111'; ctx.lineWidth=2; ctx.beginPath();
  for(let i=0;i<n;i++){
    const f=Number((view[i]||{}).factor||0);
    const x=xAt(i), y=yF(Math.max(-1,Math.min(1,Number.isFinite(f)?f:0)));
    if(i===0) ctx.moveTo(x,y); else ctx.lineTo(x,y);
  }
  ctx.stroke();

  ctx.fillStyle='#333'; ctx.font='12px sans-serif';
  ctx.fillText('\u4ef7\u683c', 8, 18);
  ctx.fillText(String(pMax.toFixed(2)), 4, padT+10);
  ctx.fillText(String(pMin.toFixed(2)), 4, topH-2);
  ctx.fillText('\u56e0\u5b50(-1~1)', 6, botY+12);
  ctx.fillText('1.00', 8, yF(1)+4);
  ctx.fillText('0.00', 8, yF(0)+4);
  ctx.fillText('-1.00', 4, yF(-1)+4);
  const lTs=Number((view[0]||{}).ts||0), rTs=Number((view[n-1]||{}).ts||0);
  const lTxt=(Number.isFinite(lTs)&&lTs>0)?new Date(lTs).toLocaleString('zh-CN',{hour12:false}):String(startIdx+1);
  const rTxt=(Number.isFinite(rTs)&&rTs>0)?new Date(rTs).toLocaleString('zh-CN',{hour12:false}):String(endIdx);
  ctx.fillText(lTxt, padL, h-2);
  const rw=ctx.measureText(rTxt).width;
  ctx.fillText(rTxt, w-padR-rw, h-2);
  const hint='\u62d6\u52a8\u5e73\u79fb | \u6eda\u8f6e/\u53cc\u6307\u7f29\u653e | \u53cc\u51fb\u590d\u4f4d';
  const hw=ctx.measureText(hint).width;
  ctx.fillStyle='#666';
  ctx.fillText(hint, Math.max(padL, w-hw-10), 14);
}

const exChartStore = {};
function _toSec(ts){
  const n = Number(ts||0);
  if(!Number.isFinite(n) || n<=0) return 0;
  return n > 1e12 ? Math.floor(n/1000) : Math.floor(n);
}
function _fmtTimeFromSec(sec){
  const ms = Number(sec||0)*1000;
  if(!Number.isFinite(ms) || ms<=0) return '-';
  return new Date(ms).toLocaleString('zh-CN',{hour12:false});
}
function _renderFallbackChart(hostId, points){
  const host = document.getElementById(hostId);
  if(!host) return;
  const cvId = hostId + '__fallback_canvas';
  host.innerHTML = `<canvas id=\"${cvId}\" width=\"1760\" height=\"300\"></canvas>`;
  klineFactorCanvas(cvId, points||[]);
}
function _createExchangeChart(hostId){
  const host = document.getElementById(hostId);
  if(!host) return null;
  host.innerHTML = '';
  const inner = document.createElement('div');
  inner.className = 'exchart-inner';
  host.appendChild(inner);
  const tip = document.createElement('div');
  tip.className = 'exchart-tooltip';
  host.appendChild(tip);
  if(!(window.LightweightCharts && window.LightweightCharts.createChart)){
    host.innerHTML = '<div class=\"exchart-empty\">图表库加载失败，自动使用简化图表</div>';
    return null;
  }
  const chart = window.LightweightCharts.createChart(inner, {
    width: Math.max(320, host.clientWidth || 900),
    height: Math.max(260, host.clientHeight || 430),
    layout: {background: {type: 'solid', color: '#0f131d'}, textColor: '#b8c3d9'},
    grid: {vertLines: {color: '#1d2433'}, horzLines: {color: '#1d2433'}},
    rightPriceScale: {borderColor: '#2a3346'},
    leftPriceScale: {visible: true, borderColor: '#2a3346'},
    timeScale: {borderColor: '#2a3346', timeVisible: true, secondsVisible: false, rightOffset: 8, barSpacing: 8},
    crosshair: {
      mode: window.LightweightCharts.CrosshairMode.Normal,
      vertLine: {labelBackgroundColor: '#25324d'},
      horzLine: {labelBackgroundColor: '#25324d'},
    },
  });
  const candle = chart.addCandlestickSeries({
    upColor: '#00c087', downColor: '#ff5b6e', borderVisible: false, wickUpColor: '#00c087', wickDownColor: '#ff5b6e',
    priceScaleId: 'right',
  });
  const factor = chart.addLineSeries({
    color: '#e6b422', lineWidth: 2, priceScaleId: 'left', lastValueVisible: true,
    crosshairMarkerVisible: true, crosshairMarkerRadius: 3,
  });
  chart.priceScale('left').applyOptions({scaleMargins: {top: 0.72, bottom: 0.02}, autoScale: true});
  chart.priceScale('right').applyOptions({scaleMargins: {top: 0.06, bottom: 0.34}, autoScale: true});
  const state = {id:hostId, host, chart, candle, factor, tip, dataByTs:new Map(), resizeObserver:null, inited:false, options:{}};
  if(window.ResizeObserver){
    state.resizeObserver = new ResizeObserver(()=>{
      if(!state.chart) return;
      state.chart.applyOptions({width: Math.max(320, host.clientWidth || 900), height: Math.max(260, host.clientHeight || 430)});
    });
    state.resizeObserver.observe(host);
  }else{
    window.addEventListener('resize', ()=>{
      if(!state.chart) return;
      state.chart.applyOptions({width: Math.max(320, host.clientWidth || 900), height: Math.max(260, host.clientHeight || 430)});
    });
  }
  chart.subscribeCrosshairMove((param)=>{
    if(!state.tip) return;
    if(!param || !param.time){
      state.tip.style.display = 'none';
      return;
    }
    const ts = Number(param.time);
    const p = state.dataByTs.get(ts);
    if(!p){
      state.tip.style.display = 'none';
      return;
    }
    const metricLabel = state.options.metricLabel || '扩展值';
    const metricKey = state.options.metricKey || '';
    const extraLabel = state.options.extraLabel || '';
    const extraKey = state.options.extraKey || '';
    const metricVal = metricKey ? fmt(p[metricKey]) : '-';
    const extraVal = extraKey ? fmt(p[extraKey]) : '';
    let txt = '时间: '+_fmtTimeFromSec(ts)+'\\n';
    txt += 'O/H/L/C: '+fmt(p.open)+' / '+fmt(p.high)+' / '+fmt(p.low)+' / '+fmt(p.close)+'\\n';
    txt += '因子值: '+fmt(p.factor);
    if(metricKey) txt += '\\n'+metricLabel+': '+metricVal;
    if(extraKey) txt += '\\n'+extraLabel+': '+extraVal;
    state.tip.textContent = txt;
    state.tip.style.display = 'block';
    const point = param.point;
    if(point && Number.isFinite(point.x) && Number.isFinite(point.y)){
      const pad = 14;
      const tipW = Math.min(360, state.tip.offsetWidth || 260);
      const tipH = state.tip.offsetHeight || 96;
      let x = point.x + 14;
      let y = point.y - tipH - 10;
      if(x + tipW > state.host.clientWidth - pad) x = state.host.clientWidth - tipW - pad;
      if(y < pad) y = point.y + 10;
      state.tip.style.left = Math.max(pad, x)+'px';
      state.tip.style.top = Math.max(pad, y)+'px';
    }else{
      state.tip.style.left = '12px';
      state.tip.style.top = '12px';
    }
  });
  exChartStore[hostId] = state;
  return state;
}
function _getExchangeChart(hostId){
  const host = document.getElementById(hostId);
  if(!host) return null;
  let st = exChartStore[hostId];
  if(st && st.host !== host) st = null;
  if(!st) st = _createExchangeChart(hostId);
  return st;
}
function resetExchangeChart(hostId){
  if(hostId === 'btUnifiedKline'){
    const u = _getUnifiedChart();
    if(u && u.chart){ u.chart.timeScale().fitContent(); }
    return;
  }
  const st = _getExchangeChart(hostId);
  if(!st || !st.chart) return;
  st.chart.timeScale().fitContent();
}
function renderExchangeKline(hostId, points, options){
  const host = document.getElementById(hostId);
  if(!host) return;
  if(!Array.isArray(points) || points.length < 3){
    host.innerHTML = '<div class=\"exchart-empty\">暂无可视化数据</div>';
    if(exChartStore[hostId]) delete exChartStore[hostId];
    return;
  }
  const st = _getExchangeChart(hostId);
  if(!st || !st.chart || !st.candle || !st.factor){
    _renderFallbackChart(hostId, points);
    return;
  }
  st.options = options || {};
  const candleData = [];
  const factorData = [];
  const map = new Map();
  for(const p of points){
    const t = _toSec(p.ts);
    if(!t) continue;
    const row = {
      time: t,
      open: Number(p.open||0),
      high: Number(p.high||0),
      low: Number(p.low||0),
      close: Number(p.close||0),
      factor: Number(p.factor||0),
    };
    if(Number.isFinite(Number(p[st.options.metricKey]))){ row[st.options.metricKey] = Number(p[st.options.metricKey]); }
    if(Number.isFinite(Number(p[st.options.extraKey]))){ row[st.options.extraKey] = Number(p[st.options.extraKey]); }
    candleData.push({time: t, open: row.open, high: row.high, low: row.low, close: row.close});
    factorData.push({time: t, value: row.factor});
    map.set(t, row);
  }
  if(candleData.length < 3){
    host.innerHTML = '<div class=\"exchart-empty\">图表数据不足</div>';
    return;
  }
  st.dataByTs = map;
  st.candle.setData(candleData);
  st.factor.setData(factorData);
  if(!st.inited){
    st.chart.timeScale().fitContent();
    st.inited = true;
  }
}

const perfTerminalState = {range:'all', chart:null};
function _createPerfTerminal(hostId){
  const host = document.getElementById(hostId);
  if(!host) return null;
  if(!(window.LightweightCharts && window.LightweightCharts.createChart)){
    host.innerHTML = '<div class="exchart-empty">chart lib missing</div>';
    return null;
  }
  host.innerHTML = '';
  const inner = document.createElement('div');
  inner.className = 'exchart-inner';
  host.appendChild(inner);
  const tip = document.createElement('div');
  tip.className = 'exchart-tooltip';
  host.appendChild(tip);
  const chart = window.LightweightCharts.createChart(inner, {
    width: Math.max(320, host.clientWidth || 900),
    height: Math.max(260, host.clientHeight || 390),
    layout: {background: {type: 'solid', color: '#0f131d'}, textColor: '#b8c3d9'},
    grid: {vertLines: {color: '#1d2433'}, horzLines: {color: '#1d2433'}},
    rightPriceScale: {borderColor: '#2a3346'},
    leftPriceScale: {visible: true, borderColor: '#2a3346'},
    timeScale: {borderColor: '#2a3346', timeVisible: true, secondsVisible: false, rightOffset: 8, barSpacing: 8},
    crosshair: {
      mode: window.LightweightCharts.CrosshairMode.Normal,
      vertLine: {labelBackgroundColor: '#25324d'},
      horzLine: {labelBackgroundColor: '#25324d'},
    },
  });
  const equity = chart.addAreaSeries({
    lineColor: '#4d8cff',
    topColor: 'rgba(77,140,255,0.40)',
    bottomColor: 'rgba(77,140,255,0.05)',
    lineWidth: 2,
    priceScaleId: 'right',
  });
  const drawdown = chart.addLineSeries({
    color: '#ff6b6b',
    lineWidth: 2,
    priceScaleId: 'left',
  });
  chart.priceScale('right').applyOptions({scaleMargins: {top: 0.08, bottom: 0.28}, autoScale: true});
  chart.priceScale('left').applyOptions({scaleMargins: {top: 0.72, bottom: 0.02}, autoScale: true});
  const st = {
    host, chart, equity, drawdown, tip,
    map: new Map(), data: [],
    range: perfTerminalState.range || 'all',
    inited: false,
    resizeObserver: null,
  };
  chart.subscribeCrosshairMove((param)=>{
    if(!param || !param.time){
      st.tip.style.display = 'none';
      return;
    }
    const p = st.map.get(Number(param.time || 0));
    if(!p){
      st.tip.style.display = 'none';
      return;
    }
    const retPct = ((Number(p.equity||1)-1)*100);
    const txt = 'time: ' + _fmtTimeFromSec(Number(param.time||0)) + '\\nequity: ' + fmt(p.equity) + '\\nreturn: ' + fmt(retPct) + '%\\ndrawdown: ' + fmt(p.drawdown_pct) + '%';
    st.tip.textContent = txt;
    st.tip.style.display = 'block';
    const pt = param.point;
    if(pt && Number.isFinite(pt.x) && Number.isFinite(pt.y)){
      const pad = 14;
      const tipW = Math.min(320, st.tip.offsetWidth || 230);
      const tipH = st.tip.offsetHeight || 90;
      let x = pt.x + 12;
      let y = pt.y - tipH - 10;
      if(x + tipW > st.host.clientWidth - pad) x = st.host.clientWidth - tipW - pad;
      if(y < pad) y = pt.y + 10;
      st.tip.style.left = Math.max(pad, x) + 'px';
      st.tip.style.top = Math.max(pad, y) + 'px';
    }
  });
  if(window.ResizeObserver){
    st.resizeObserver = new ResizeObserver(()=>{
      st.chart.applyOptions({
        width: Math.max(320, host.clientWidth || 900),
        height: Math.max(260, host.clientHeight || 390),
      });
    });
    st.resizeObserver.observe(host);
  }
  perfTerminalState.chart = st;
  return st;
}
function _getPerfTerminal(){
  const st = perfTerminalState.chart;
  if(st && st.host && document.body.contains(st.host)) return st;
  return _createPerfTerminal('btPerfTerminal');
}
function _perfDays(range){
  if(range === '7d') return 7;
  if(range === '14d') return 14;
  if(range === '30d') return 30;
  return 0;
}
function _syncPerfRangeButtons(range){
  document.querySelectorAll('[data-perf-range]').forEach(btn=>{
    const r = String(btn.getAttribute('data-perf-range') || '');
    btn.classList.toggle('active', r === range);
  });
}
function setPerfRange(range){
  const st = _getPerfTerminal();
  if(!st || !st.chart || !Array.isArray(st.data) || !st.data.length) return;
  const next = ['all','30d','14d','7d'].includes(range) ? range : 'all';
  st.range = next;
  perfTerminalState.range = next;
  _syncPerfRangeButtons(next);
  const days = _perfDays(next);
  if(!days){
    st.chart.timeScale().fitContent();
    return;
  }
  const first = Number((st.data[0]||{}).time||0);
  const last = Number((st.data[st.data.length-1]||{}).time||0);
  if(!(first>0 && last>0)) return;
  const from = Math.max(first, last - days*86400);
  st.chart.timeScale().setVisibleRange({from, to: last});
}
function renderPerformanceTerminal(curve){
  const host = document.getElementById('btPerfTerminal');
  if(!host) return;
  if(!Array.isArray(curve) || curve.length < 3){
    host.innerHTML = '<div class="exchart-empty">no curve data</div>';
    const info = document.getElementById('btPerfInfo'); if(info) info.textContent = '-';
    const legend = document.getElementById('btPerfLegend'); if(legend) legend.innerHTML = '';
    return;
  }
  const st = _getPerfTerminal();
  if(!st || !st.chart || !st.equity || !st.drawdown){
    host.innerHTML = '<div class="exchart-empty">chart unavailable</div>';
    return;
  }
  const eqData = [];
  const ddData = [];
  const map = new Map();
  for(const p of curve){
    const t = _toSec(p.ts);
    if(!t) continue;
    const e = Number(p.equity||0);
    const dd = Number(p.drawdown_pct||0);
    if(!Number.isFinite(e)) continue;
    eqData.push({time: t, value: e});
    ddData.push({time: t, value: -Math.abs(dd)});
    map.set(t, {equity:e, drawdown_pct:dd});
  }
  if(eqData.length < 3){
    host.innerHTML = '<div class="exchart-empty">curve too short</div>';
    return;
  }
  st.data = eqData;
  st.map = map;
  st.equity.setData(eqData);
  st.drawdown.setData(ddData);
  const firstEq = Number(eqData[0].value||1);
  const lastEq = Number(eqData[eqData.length-1].value||firstEq);
  const retPct = (lastEq/Math.max(1e-9,firstEq)-1)*100;
  let worstDd = 0;
  for(const x of curve){
    const d = Math.abs(Number((x||{}).drawdown_pct||0));
    if(Number.isFinite(d) && d > worstDd) worstDd = d;
  }
  const info = document.getElementById('btPerfInfo');
  if(info){
    info.textContent = 'points=' + eqData.length + ' | latest equity=' + fmt(lastEq) + ' | return=' + fmt(retPct) + '% | maxDD=' + fmt(worstDd) + '%';
  }
  const legend = document.getElementById('btPerfLegend');
  if(legend){
    legend.innerHTML =
      '<span class="perf-chip"><span class="legend-dot" style="background:#4d8cff"></span>equity ' + fmt(lastEq) + '</span>' +
      '<span class="perf-chip"><span class="legend-dot" style="background:#36b37e"></span>return ' + fmt(retPct) + '%</span>' +
      '<span class="perf-chip"><span class="legend-dot" style="background:#ff6b6b"></span>drawdown -' + fmt(worstDd) + '%</span>';
  }
  if(!st.inited){
    st.chart.timeScale().fitContent();
    st.inited = true;
  }
  setPerfRange(st.range || perfTerminalState.range || 'all');
}

const FACTOR_CN = {
  momentum:'动量', trend_ema:'EMA趋势', rsi_reversion:'RSI回归', breakout_20:'通道突破',
  volume_flow:'量价流', volatility_regime:'波动区间', asr_vc:'ASR-VC', enhanced_technical:'增强技术',
  basis_spread:'基差', funding_rate:'资金费率', oi_momentum:'OI动量', long_short_ratio:'多空账户比',
  taker_imbalance:'主动成交不平衡', order_flow_persistence:'订单流持续性',
  funding_basis_divergence:'资金费率-基差背离', liquidation_pressure:'清算压力',
  cross_market_correlation:'跨市场相关', macro_risk_sentiment:'宏观风险', fear_greed:'恐慌贪婪',
};
const FACTOR_COLORS = ['#f4c430','#58d68d','#5dade2','#f1948a','#bb8fce','#48c9b0','#f5b041','#7fb3d5','#ec7063','#73c6b6','#85c1e9','#f8c471','#7dcea0','#d2b4de','#76d7c4','#f9e79f','#f7dc6f','#a3e4d7','#f0b27a','#aed6f1'];
const unifiedFactorState = {selected:[], available:[], chartReady:false, bound:false};
function factorLabel(key){ return FACTOR_CN[key] || key; }
function factorColor(key){
  let h = 0;
  const s = String(key||'');
  for(let i=0;i<s.length;i++) h = (h * 131 + s.charCodeAt(i)) >>> 0;
  return FACTOR_COLORS[h % FACTOR_COLORS.length];
}
function _createUnifiedChart(hostId){
  const host = document.getElementById(hostId);
  if(!host) return null;
  if(!(window.LightweightCharts && window.LightweightCharts.createChart)){
    host.innerHTML = '<div class=\"exchart-empty\">图表库未加载，无法展示全因子叠加图</div>';
    return null;
  }
  host.innerHTML = '';
  const inner = document.createElement('div');
  inner.className = 'exchart-inner';
  host.appendChild(inner);
  const tip = document.createElement('div');
  tip.className = 'exchart-tooltip';
  host.appendChild(tip);
  const chart = window.LightweightCharts.createChart(inner, {
    width: Math.max(320, host.clientWidth || 900),
    height: Math.max(260, host.clientHeight || 430),
    layout: {background: {type: 'solid', color: '#0f131d'}, textColor: '#b8c3d9'},
    grid: {vertLines: {color: '#1d2433'}, horzLines: {color: '#1d2433'}},
    rightPriceScale: {borderColor: '#2a3346'},
    leftPriceScale: {visible: true, borderColor: '#2a3346'},
    timeScale: {borderColor: '#2a3346', timeVisible: true, secondsVisible: false, rightOffset: 8, barSpacing: 8},
    crosshair: {
      mode: window.LightweightCharts.CrosshairMode.Normal,
      vertLine: {labelBackgroundColor: '#25324d'},
      horzLine: {labelBackgroundColor: '#25324d'},
    },
  });
  const candle = chart.addCandlestickSeries({
    upColor: '#00c087', downColor: '#ff5b6e', borderVisible: false, wickUpColor: '#00c087', wickDownColor: '#ff5b6e',
    priceScaleId: 'right',
  });
  chart.priceScale('left').applyOptions({scaleMargins: {top: 0.72, bottom: 0.02}, autoScale: true});
  chart.priceScale('right').applyOptions({scaleMargins: {top: 0.06, bottom: 0.34}, autoScale: true});
  const st = {
    host, chart, candle, tip, lines:{}, baseByTs:new Map(), factorByTs:{}, inited:false,
    resizeObserver:null,
  };
  chart.subscribeCrosshairMove((param)=>{
    if(!param || !param.time){ st.tip.style.display='none'; return; }
    const t = Number(param.time||0);
    const base = st.baseByTs.get(t);
    if(!base){ st.tip.style.display='none'; return; }
    let txt = '时间: '+_fmtTimeFromSec(t)+'\\n';
    txt += 'O/H/L/C: '+fmt(base.open)+' / '+fmt(base.high)+' / '+fmt(base.low)+' / '+fmt(base.close);
    const picked = (unifiedFactorState.selected||[]).slice(0,10);
    for(const k of picked){
      const m = st.factorByTs[k];
      if(m && m.has(t)){
        txt += '\\n'+factorLabel(k)+': '+fmt(m.get(t));
      }
    }
    st.tip.textContent = txt;
    st.tip.style.display='block';
    const point = param.point;
    if(point && Number.isFinite(point.x) && Number.isFinite(point.y)){
      const pad = 14;
      const tipW = Math.min(360, st.tip.offsetWidth || 280);
      const tipH = st.tip.offsetHeight || 120;
      let x = point.x + 14;
      let y = point.y - tipH - 10;
      if(x + tipW > st.host.clientWidth - pad) x = st.host.clientWidth - tipW - pad;
      if(y < pad) y = point.y + 10;
      st.tip.style.left = Math.max(pad, x)+'px';
      st.tip.style.top = Math.max(pad, y)+'px';
    }else{
      st.tip.style.left='12px';
      st.tip.style.top='12px';
    }
  });
  if(window.ResizeObserver){
    st.resizeObserver = new ResizeObserver(()=>{
      st.chart.applyOptions({width: Math.max(320, host.clientWidth || 900), height: Math.max(260, host.clientHeight || 430)});
    });
    st.resizeObserver.observe(host);
  }
  return st;
}
function _getUnifiedChart(){
  if(!unifiedFactorState.chart || !unifiedFactorState.chart.host || !document.body.contains(unifiedFactorState.chart.host)){
    unifiedFactorState.chart = _createUnifiedChart('btUnifiedKline');
  }
  return unifiedFactorState.chart;
}
function _syncUnifiedSelector(available, meta){
  const wrap = document.getElementById('btFactorSelector');
  if(!wrap) return;
  const m = {};
  (meta||[]).forEach(x=>{ if(x&&x.key) m[x.key]=x; });
  const ordered = available.slice().sort((a,b)=>{
    const wa = Math.abs(Number((m[a]||{}).weight||0));
    const wb = Math.abs(Number((m[b]||{}).weight||0));
    return wb - wa;
  });
  wrap.innerHTML = ordered.map(k=>{
    const checked = (unifiedFactorState.selected||[]).includes(k) ? 'checked' : '';
    const latest = Number((m[k]||{}).latest||0);
    return `<label class=\"factor-chip\"><input type=\"checkbox\" data-factor-key=\"${esc(k)}\" ${checked}/><span>${esc(factorLabel(k))} (${fmt(latest)})</span></label>`;
  }).join('');
  wrap.querySelectorAll('input[data-factor-key]').forEach(el=>{
    el.addEventListener('change', ()=>{
      const next = [];
      wrap.querySelectorAll('input[data-factor-key]').forEach(x=>{
        if(x.checked){ next.push(x.getAttribute('data-factor-key')||''); }
      });
      unifiedFactorState.selected = next.filter(Boolean);
      if(btLast) renderUnifiedFactorChart(btLast);
    });
  });
}
function renderUnifiedFactorChart(bt){
  const charts = (bt&&bt.charts)||{};
  const base = Array.isArray(charts.base_kline) ? charts.base_kline : [];
  const factorSeries = (charts.factor_series && typeof charts.factor_series==='object') ? charts.factor_series : {};
  const meta = Array.isArray(charts.factor_meta) ? charts.factor_meta : [];
  const available = Object.keys(factorSeries);
  unifiedFactorState.available = available.slice();

  if(!available.length || base.length < 3){
    const host = document.getElementById('btUnifiedKline');
    if(host) host.innerHTML = '<div class=\"exchart-empty\">暂无全因子图数据</div>';
    const info = document.getElementById('btUnifiedInfo');
    if(info) info.textContent = '无可用因子序列';
    const legend=document.getElementById('btUnifiedLegend'); if(legend) legend.innerHTML='';
    const exp=document.getElementById('btUnifiedExplain'); if(exp) exp.innerHTML='';
    const guide=document.getElementById('btUnifiedGuide'); if(guide) guide.textContent='';
    return;
  }
  if(!(unifiedFactorState.selected||[]).some(k=>available.includes(k))){
    const sorted = meta.slice().sort((a,b)=>Math.abs(Number(b.weight||0))-Math.abs(Number(a.weight||0)));
    const defaults = sorted.slice(0,6).map(x=>String(x.key||'')).filter(k=>available.includes(k));
    unifiedFactorState.selected = defaults.length ? defaults : available.slice(0,6);
  }else{
    unifiedFactorState.selected = unifiedFactorState.selected.filter(k=>available.includes(k));
  }
  _syncUnifiedSelector(available, meta);

  const st = _getUnifiedChart();
  if(!st || !st.chart || !st.candle) return;

  const cData = [];
  const baseMap = new Map();
  for(const p of base){
    const t=_toSec(p.ts); if(!t) continue;
    const row={time:t,open:Number(p.open||0),high:Number(p.high||0),low:Number(p.low||0),close:Number(p.close||0)};
    cData.push(row);
    baseMap.set(t,row);
  }
  st.baseByTs = baseMap;
  st.candle.setData(cData);

  const selected = unifiedFactorState.selected || [];
  const selectedSet = new Set(selected);
  for(const k of Object.keys(st.lines)){
    if(!selectedSet.has(k)){
      try{ st.chart.removeSeries(st.lines[k]); }catch(_){ }
      delete st.lines[k];
      delete st.factorByTs[k];
    }
  }
  for(const k of selected){
    const src = Array.isArray(factorSeries[k]) ? factorSeries[k] : [];
    if(!st.lines[k]){
      st.lines[k] = st.chart.addLineSeries({
        color: factorColor(k), lineWidth: 2, priceScaleId: 'left',
        lastValueVisible: true, crosshairMarkerVisible: true, crosshairMarkerRadius: 2,
        title: factorLabel(k),
      });
    }
    const ld = [];
    const mp = new Map();
    for(const p of src){
      const t=_toSec(p.ts); if(!t) continue;
      const v=Number(p.value||0);
      ld.push({time:t,value:v});
      mp.set(t,v);
    }
    st.lines[k].setData(ld);
    st.factorByTs[k] = mp;
  }
  if(!st.inited){
    st.chart.timeScale().fitContent();
    st.inited = true;
  }
  const metaMap = {};
  (meta||[]).forEach(x=>{ if(x&&x.key) metaMap[x.key]=x; });
  const legend=document.getElementById('btUnifiedLegend');
  if(legend){
    legend.innerHTML=selected.map(k=>{
      const m=metaMap[k]||{};
      const latest=Number(m.latest||0);
      return `<span class=\"factor-chip\"><span class=\"legend-dot\" style=\"background:${factorColor(k)}\"></span>${esc(factorLabel(k))}: ${fmt(latest)}</span>`;
    }).join('');
  }
  const exp=document.getElementById('btUnifiedExplain');
  if(exp){
    const rows = selected.map(k=>{
      const m=metaMap[k]||{};
      const latest=Number(m.latest||0);
      const th=Math.abs(Number(m.threshold||0.14));
      const state = latest>=th ? '偏多' : (latest<=-th ? '偏空' : '中性');
      return {k, latest, th, state};
    });
    let bull=0,bear=0,neu=0;
    rows.forEach(x=>{ if(x.state==='偏多') bull++; else if(x.state==='偏空') bear++; else neu++; });
    const dominant = bull>bear?'偏多':(bear>bull?'偏空':'中性');
    exp.innerHTML = `<div class='factor-state-row h'><div>因子</div><div>当前值</div><div>阈值</div><div>状态</div></div>` +
      rows.map(x=>`<div class='factor-state-row'><div>${esc(factorLabel(x.k))}</div><div>${fmt(x.latest)}</div><div>\u00b1${fmt(x.th)}</div><div>${esc(x.state)}</div></div>`).join('') +
      `<div class='guide-note' style='margin-top:6px'>当前统计：偏多 ${bull} 个，偏空 ${bear} 个，中性 ${neu} 个，多数方向为 <b>${dominant}</b>。说明：只有当因子值超过该因子阈值时，才算“有效倾向”。</div>`;
  }
  const guide=document.getElementById('btUnifiedGuide');
  if(guide){
    guide.textContent='怎么用这张图：先勾选你要观察的因子，再看“状态”是否同向聚合；同向越多，信号通常越清晰。注意这仍是分析辅助，最终执行还要看风控和仓位规则。';
  }
  const info = document.getElementById('btUnifiedInfo');
  if(info){
    info.textContent = `已选因子 ${selected.length}/${available.length} | 可拖拽/缩放/十字光标 | ${base.length} 根K线`;
  }
}

function renderBacktest(bt){
  if(!bt||!bt.ok){
    document.getElementById('btStatus').textContent='\u56de\u6d4b\u5931\u8d25';
    return;
  }
  btLast = bt;
  const src=(bt.source||{}); const sp=(bt.params||{}); const sys=(bt.system||{}); const bm=(bt.benchmark||{}); const ex=(bt.excess||{}); const adv=(bt.advisor||{}); const liq=(bt.liquidation_only||{});
  const liqOk=!!liq.ok;
  document.getElementById('btStatus').textContent='\u6700\u8fd1\u56de\u6d4b: '+new Date(bt.generated_at||Date.now()).toLocaleString('zh-CN');
  const aux=(src.aux||{}); document.getElementById('btSource').textContent='\u6570\u636e\u6e90: '+String(src.exchange||'-')+' '+String(src.endpoint||'')+' | '+String(sp.instId||'-')+' '+String(sp.bar||'-')+' | bars='+String((bt.range||{}).bars||'-')+' | profile='+String(adv.profile_label||sp.profile||'-')+' | cross='+String(aux.cross_market_points||0)+' | macro='+String(aux.macro_risk_points||0)+' | fng='+String(aux.fear_greed_points||0);
  const summaryRows=[
    ['\u6536\u76ca\u7387(%)', fmt(sys.return_pct)],
    ['年化收益(%)', fmt(sys.annualized_return_pct)],
    ['BTC现货收益(%)', fmt(bm.return_pct)],
    ['BTC现货年化(%)', fmt(bm.annualized_return_pct)],
    ['超额收益(%)', fmt(ex.return_pct)],
    ['\u6700\u5927\u56de\u64a4(%)', fmt(sys.max_drawdown_pct)],
    ['BTC现货最大回撤(%)', fmt(bm.max_drawdown_pct)],
    ['\u6eda\u52a8\u56de\u64a4(\u5f53\u524d/\u6700\u5dee)', fmt((((sys.rolling_drawdown||{}).latest_pct)))+' / '+fmt((((sys.rolling_drawdown||{}).worst_pct)))],
    ['\u4ea4\u6613\u6570', sys.trades],
    ['\u80dc\u7387(%)', fmt(sys.win_rate_pct)],
    ['\u5e73\u5747\u76c8\u5229(%)', fmt(sys.avg_win_pct)],
    ['\u5e73\u5747\u4e8f\u635f(%)', fmt(sys.avg_loss_pct)],
    ['\u76c8\u4e8f\u6bd4(\u5e73\u5747\u76c8/\u4e8f)', fmt(sys.reward_risk_ratio)],
    ['Profit Factor', fmt(sys.profit_factor)],
    ['\u5355\u7b14\u671f\u671b(%)', fmt(sys.expectancy_pct)],
    ['Sharpe-like', fmt(sys.sharpe_like)],
    ['Calmar-like', fmt(sys.calmar_like)],
    ['\u6210\u672c(\u603b, %\u6743\u76ca)', fmt((((sys.costs||{}).total_pct_equity)))],
    ['清算反向独立收益(%)', liqOk ? fmt(liq.return_pct) : '-'],
    ['清算反向独立最大回撤(%)', liqOk ? fmt(liq.max_drawdown_pct) : '-'],
  ];
  document.getElementById('btSummary').innerHTML=summaryRows.map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
  const dp=sys.drawdown_period||{};
  document.getElementById('btDrawdown').innerHTML=[
    ['\u5cf0\u503c\u65f6\u95f4', dp.peak_ts||'-'],
    ['\u8c37\u503c\u65f6\u95f4', dp.trough_ts||'-'],
    ['\u56de\u64a4\u7a97\u53e3(\u6761)', (sys.rolling_drawdown||{}).window_bars||'-'],
    ['\u533a\u95f4\u5f00\u59cb', (bt.range||{}).start_ts||'-'],
    ['\u533a\u95f4\u7ed3\u675f', (bt.range||{}).end_ts||'-'],
  ].map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
  const benchmarkRows=[
    ['7天滚动跑赢率(%)', fmt(ex.rolling_7d_outperform_rate_pct)],
    ['30天滚动跑赢率(%)', fmt(ex.rolling_30d_outperform_rate_pct)],
    ['策略画像', adv.profile_label||sp.profile||'-'],
    ['多头最大持仓(bar)', ((adv.max_hold_bars||{}).long)||'-'],
    ['空头最大持仓(bar)', ((adv.max_hold_bars||{}).short)||'-'],
    ['清算反向交易数', liqOk ? String(liq.trades||0) : '-'],
    ['清算反向胜率(%)', liqOk ? fmt(liq.win_rate_pct) : '-'],
  ];
  document.getElementById('btBenchmark').innerHTML=benchmarkRows.map(x=>`<div>${esc(x[0])}</div><div>${esc(x[1])}</div>`).join('');
  const liqLine = liqOk
    ? `清算反向独立回测：收益 ${fmt(liq.return_pct)}%，最大回撤 ${fmt(liq.max_drawdown_pct)}%，交易数 ${esc(liq.trades||0)}。`
    : '清算反向独立回测暂不可用。';
  document.getElementById('btAdvisor').innerHTML=`<div><b>策略框架：</b>${esc(adv.description||'-')}</div><div style="margin-top:6px">当前最新簇读数：BTC基准 ${fmt(((adv.cluster_latest||{}).benchmark_regime))} / 趋势 ${fmt(((adv.cluster_latest||{}).trend_stack))} / 订单流 ${fmt(((adv.cluster_latest||{}).flow_impulse))} / 拥挤度 ${fmt(((adv.cluster_latest||{}).carry_regime))} / 风险压力 ${fmt(((adv.cluster_latest||{}).risk_pressure))}</div><div style="margin-top:6px">解读：先看状态是否允许出手，再看触发信号；避免因为单一分数波动频繁翻仓。</div><div style="margin-top:6px">${liqLine}</div>`;
  const scan=document.getElementById('btWindowScan');
  const scanRows=Array.isArray(bt.window_scan)?bt.window_scan:[];
  if(scan){
    if(!scanRows.length){
      scan.innerHTML='<div class=\"guide-note\">暂无窗口扫描数据</div>';
    }else{
      scan.innerHTML='<table><tr><th>窗口(1H bars)</th><th>收益(%)</th><th>年化(%)</th><th>BTC现货(%)</th><th>超额(%)</th><th>最大回撤(%)</th><th>Profit Factor</th><th>交易数</th></tr>'+
        scanRows.map(x=>`<tr><td>${esc(x.bars)}</td><td>${fmt(x.return_pct)}</td><td>${fmt(x.annualized_return_pct)}</td><td>${fmt(x.benchmark_return_pct)}</td><td>${fmt(x.excess_return_pct)}</td><td>${fmt(x.max_drawdown_pct)}</td><td>${fmt(x.profit_factor)}</td><td>${esc(x.trades||0)}</td></tr>`).join('')+
        '</table><div class=\"guide-note\" style=\"margin-top:8px\">看这个表时，优先看最长窗口是否仍为正收益，再看超额收益和最大回撤。如果短窗口低于 BTC，说明系统在最近顺风段更保守，但只要长窗口和回撤控制更稳，它仍可能更适合作为自动化系统。</div>';
    }
  }

  const f=(bt.factors||[]);
  document.getElementById('btFactorTbl').innerHTML='<tr><th>\u56e0\u5b50</th><th>\u5206\u7ec4</th><th>\u6743\u91cd</th><th>\u6536\u76ca\u7387(%)</th><th>\u6700\u5927\u56de\u64a4(%)</th><th>\u6eda\u52a8\u56de\u64a4\u6700\u5dee(%)</th><th>\u4ea4\u6613\u6570</th><th>\u80dc\u7387(%)</th><th>\u56e0\u5b50\u7528\u9014</th></tr>'+
    f.map(x=>`<tr><td>${esc(x.name||x.key||'-')}</td><td>${x.group==='main'?'主参考':'辅助'}</td><td>${fmt(x.weight)}</td><td>${fmt(x.return_pct)}</td><td>${fmt(x.max_drawdown_pct)}</td><td>${fmt((((x.rolling_drawdown||{}).worst_pct)))}</td><td>${esc(x.trades||0)}</td><td>${fmt(x.win_rate_pct)}</td><td>${esc(x.thesis||'-')}</td></tr>`).join('');

  const curve=(sys.curve||[]);
  renderPerformanceTerminal(curve);
  renderUnifiedFactorChart(bt);
}
async function runBacktest(){
  const payload={
    instId:(document.getElementById('btInst').value||'BTC-USDT-SWAP').trim(),
    bar:(document.getElementById('btBar').value||'1H').trim(),
    profile:(document.getElementById('btProfile').value||'regime_balance_v4').trim(),
    start:(document.getElementById('btStart').value||'').trim(),
    end:(document.getElementById('btEnd').value||'').trim(),
    bars:Number(document.getElementById('btBars').value||1500),
    drawdownWindowBars:Number(document.getElementById('btDDWin').value||96),
    leverage:Number(document.getElementById('btLev').value||1),
    feeBps:Number(document.getElementById('btFee').value||5),
    slippageBps:Number(document.getElementById('btSlip').value||3),
  };
  document.getElementById('btStatus').textContent='\u56de\u6d4b\u6267\u884c\u4e2d...';
  const r=await fetch('/api/backtest/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});
  const d=await r.json();
  if(!r.ok||!d.ok){
    document.getElementById('btStatus').textContent='\u56de\u6d4b\u5931\u8d25: '+String((d&&d.error)||('http_'+r.status));
    return;
  }
  renderBacktest(d);
}
async function loadBacktestLatest(){
  try{
    const r=await fetch('/api/backtest-latest?_='+Date.now(),{cache:'no-store'});
    if(!r.ok) return;
    const d=await r.json();
    if(!d||!d.ok) return;
    renderBacktest(d);
    const p=d.params||{};
    if(p.instId) document.getElementById('btInst').value=p.instId;
    if(p.bar) document.getElementById('btBar').value=p.bar;
    if(p.profile) document.getElementById('btProfile').value=p.profile;
    if(p.requestedBars){
      const curBars = Number(document.getElementById('btBars').value||3000);
      const reqBars = Number(p.requestedBars||0);
      if(Number.isFinite(reqBars) && reqBars > curBars){
        document.getElementById('btBars').value = reqBars;
      }
    }
    if(p.drawdownWindowBars) document.getElementById('btDDWin').value=p.drawdownWindowBars;
    if(p.leverage) document.getElementById('btLev').value=p.leverage;
    if(p.feeBps!==undefined) document.getElementById('btFee').value=p.feeBps;
    if(p.slippageBps!==undefined) document.getElementById('btSlip').value=p.slippageBps;
    const endMs = Number(((d.range||{}).end_ms)||0);
    const staleMs = Date.now() - endMs;
    if(!btAutoRefreshOnce && Number.isFinite(endMs) && endMs > 0 && staleMs > 6*3600*1000){
      btAutoRefreshOnce = true;
      document.getElementById('btStatus').textContent='检测到缓存K线过旧，正在自动刷新最新数据...';
      setTimeout(()=>{ runBacktest(); }, 120);
    }
  }catch(_){ }
}


function switchView(v){
  const all=['overview','backtest','trades','liqfactor','session','claw402','errors'];
  const target=all.includes(v)?v:'overview';
  all.forEach(id=>{
    const el=document.getElementById('view-'+id);
    if(el) el.classList.toggle('active', id===target);
  });
  const nav=document.querySelectorAll('#okxSideNav .nav');
  nav.forEach(a=>a.classList.toggle('active', (a.dataset.view||'')===target));
  try{localStorage.setItem('okx_view',target);}catch(_){ }
  try{if(location.hash !== '#'+target){history.replaceState(null,'','#'+target);}}catch(_){ }
}
function initSideNav(){
  const nav=document.querySelectorAll('#okxSideNav .nav');
  nav.forEach(a=>{
    a.addEventListener('click', (ev)=>{
      ev.preventDefault();
      switchView(a.dataset.view||'overview');
    });
  });
  let v='';
  try{v=(location.hash||'').replace('#','').trim();}catch(_){v='';}
  if(!v){
    try{v=localStorage.getItem('okx_view')||'overview';}catch(_){v='overview';}
  }
  switchView(v||'overview');
}

window.onload=()=>{
  document.getElementById('btnPaper').onclick=()=>setMode('paper');
  document.getElementById('btnLive').onclick=()=>setMode('live');
  document.getElementById('btnAIDecision').onclick=()=>runAI();
  document.getElementById('btnLang').onclick=()=>{
    currentLang=currentLang==='zh'?'en':'zh';
    localStorage.setItem('lang',currentLang);
    load();
  };
  document.getElementById('btnBacktestRun').onclick=()=>runBacktest();
  const btnAll = document.getElementById('btFactorAll');
  if(btnAll) btnAll.onclick=()=>{
    unifiedFactorState.selected = (unifiedFactorState.available||[]).slice();
    if(btLast) renderUnifiedFactorChart(btLast);
  };
  const btnNone = document.getElementById('btFactorNone');
  if(btnNone) btnNone.onclick=()=>{
    unifiedFactorState.selected = [];
    if(btLast) renderUnifiedFactorChart(btLast);
  };
  document.querySelectorAll('[data-perf-range]').forEach(btn=>{
    btn.onclick=()=>setPerfRange(String(btn.getAttribute('data-perf-range')||'all'));
  });
  const perfReset = document.getElementById('btPerfReset');
  if(perfReset) perfReset.onclick=()=>setPerfRange('all');
  _syncPerfRangeButtons(perfTerminalState.range || 'all');
  document.querySelectorAll('[data-reset-chart]').forEach(btn=>{
    btn.onclick=()=>resetExchangeChart(btn.getAttribute('data-reset-chart')||'');
  });
  initSideNav();
  initClaw402Hub();
  load();
  loadBacktestLatest();
  setInterval(load,10000);
  setInterval(()=>{if(claw402State.inited){refreshClaw402Wallet(false);refreshClaw402Usage(false);}},30000);
};
</script></body></html>
"""

def _escape_html(value: Any) -> str:
    return (
        str(value if value is not None else '')
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#39;')
    )


def _json_for_html_script(data: Any) -> str:
    payload = json.dumps(data, ensure_ascii=False)
    return payload.replace('</', '<\\/')


def _build_instance_nav_links(active_id: str = '') -> str:
    active = str(active_id or '').strip().lower()
    links = [
        '<a class="nav-link{cls}" data-role="home" data-label-zh="主页" data-label-en="Home" href="/">主页</a>'.format(
            cls=' is-active' if not active else ''
        )
    ]
    for row in list_instance_public_meta():
        iid = str(row.get('id') or '')
        if not iid:
            continue
        name_zh = _escape_html(str(row.get('name_zh') or iid))
        name_en = _escape_html(str(row.get('name_en') or name_zh))
        href = _escape_html(str(row.get('instance_path') or f'/instance/{iid}'))
        cls = ' is-active' if iid == active else ''
        links.append(
            f'<a class="nav-link{cls}" data-role="instance" data-id="{_escape_html(iid)}" '
            f'data-label-zh="{name_zh}" data-label-en="{name_en}" href="{href}">{name_zh}</a>'
        )
    return ''.join(links)


def _build_portal_cards_html(items: List[Dict[str, Any]]) -> str:
    cards: List[str] = []
    for row in items:
        iid = str(row.get('id') or '').strip().lower()
        if not iid:
            continue
        href = _escape_html(str(row.get('instance_path') or f'/instance/{iid}'))
        name_zh = _escape_html(str(row.get('name_zh') or iid))
        name_en = _escape_html(str(row.get('name_en') or name_zh))
        desc_zh = _escape_html(str(row.get('description_zh') or ''))
        desc_en = _escape_html(str(row.get('description_en') or desc_zh))
        cat_zh = _escape_html(str(row.get('category_zh') or '实例'))
        cat_en = _escape_html(str(row.get('category_en') or 'Instance'))
        cards.append(
            f'''
            <a class="instance-card" id="card-{iid}" href="{href}" data-id="{_escape_html(iid)}">
              <div class="instance-head">
                <span class="instance-cat" data-zh="{cat_zh}" data-en="{cat_en}">{cat_zh}</span>
                <span class="instance-id">{_escape_html(iid.upper())}</span>
              </div>
              <h3 class="instance-title" id="title-{iid}" data-zh="{name_zh}" data-en="{name_en}">{name_zh}</h3>
              <p class="instance-desc" id="desc-{iid}" data-zh="{desc_zh}" data-en="{desc_en}">{desc_zh}</p>
              <div class="status-pill loading" id="status-{iid}">加载中...</div>
              <div class="instance-enter" id="enter-{iid}" data-zh="进入实例" data-en="Open Instance">进入实例</div>
            </a>
            '''
        )
    return ''.join(cards)


def render_portal_home_html() -> str:
    items = list_instance_public_meta()
    nav_links = _build_instance_nav_links('')
    cards_html = _build_portal_cards_html(items)
    instances_json = _json_for_html_script(items)
    html = """<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>COEVO · 实例中控</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@500;700&family=Noto+Sans+SC:wght@400;500;700&display=swap');
:root{--bg:#f7f4ec;--ink:#111315;--muted:#5f635f;--line:#d6d0c2;--card:#ffffffcc;--ok:#1a8a4a;--warn:#b77800;--err:#b42318;--brand:#141a1a}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg);color:var(--ink);font-family:"Noto Sans SC",sans-serif}
body{min-height:100vh;overflow-x:hidden;background-image:radial-gradient(circle at 8% 12%,rgba(255,255,255,.95),rgba(255,255,255,0) 36%),radial-gradient(circle at 90% 8%,rgba(255,255,255,.75),rgba(255,255,255,0) 40%),linear-gradient(135deg,#efebe1 0%,#f9f7f2 54%,#ece7db 100%)}
.bg-shape{position:fixed;pointer-events:none;z-index:0;filter:blur(0.2px)}
.bg-a{width:42vw;height:42vw;left:-14vw;top:48vh;background:radial-gradient(circle,rgba(184,200,188,.22),rgba(184,200,188,0) 66%)}
.bg-b{width:36vw;height:36vw;right:-10vw;top:-9vw;background:radial-gradient(circle,rgba(214,190,166,.2),rgba(214,190,166,0) 66%)}
.layout{position:relative;z-index:1;display:grid;grid-template-columns:250px 1fr;min-height:100vh}
.side{padding:20px 16px;border-right:1px solid var(--line);background:linear-gradient(180deg,rgba(248,245,238,.85),rgba(248,245,238,.55));backdrop-filter:blur(8px)}
.side-brand{font-family:"Cormorant Garamond",serif;font-size:33px;letter-spacing:.2em;margin:0 0 4px;color:var(--brand)}
.side-sub{font-size:12px;color:var(--muted);margin-bottom:18px}
.nav-links{display:flex;flex-direction:column;gap:8px}
.nav-link{text-decoration:none;color:#1a1f1f;border:1px solid #d9d3c7;border-radius:12px;padding:9px 11px;background:rgba(255,255,255,.62);font-size:13px;transition:all .2s ease}
.nav-link:hover{transform:translateX(2px);background:#fff;border-color:#bdb7aa}
.nav-link.is-active{background:#1f2626;color:#fff;border-color:#1f2626}
.main{padding:20px 22px 30px}
.topbar{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
.hero-title{margin:0;font-family:"Cormorant Garamond",serif;font-size:46px;letter-spacing:.08em;line-height:.95}
.hero-desc{margin:8px 0 0;color:var(--muted);font-size:14px;max-width:860px;line-height:1.7}
.actions{display:flex;gap:8px;align-items:center}
.btn{border:1px solid #b9b4a8;background:#fff;border-radius:11px;padding:8px 14px;font-size:13px;cursor:pointer}
.btn:hover{background:#f9f9f8}
.stats{margin-top:15px;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
.stat{border:1px solid var(--line);border-radius:13px;background:var(--card);padding:11px 12px;box-shadow:0 8px 24px rgba(34,35,31,.06)}
.stat-k{font-size:12px;color:var(--muted)}
.stat-v{margin-top:5px;font-size:26px;font-weight:700;font-family:"Cormorant Garamond",serif}
.grid{margin-top:14px;display:grid;gap:12px;grid-template-columns:repeat(3,minmax(0,1fr))}
.instance-card{text-decoration:none;color:inherit;border:1px solid var(--line);border-radius:16px;background:var(--card);padding:14px;display:flex;flex-direction:column;gap:8px;box-shadow:0 10px 28px rgba(36,35,30,.08);transition:transform .2s ease,border-color .2s ease,box-shadow .2s ease}
.instance-card:hover{transform:translateY(-2px);border-color:#bfb9ab;box-shadow:0 16px 36px rgba(34,34,30,.12)}
.instance-head{display:flex;justify-content:space-between;align-items:center;gap:8px}
.instance-cat{border:1px solid #d8d2c6;border-radius:999px;padding:3px 10px;font-size:11px;background:#fff}
.instance-id{font-size:11px;color:var(--muted);letter-spacing:.12em}
.instance-title{margin:0;font-size:20px;line-height:1.2}
.instance-desc{margin:0;color:var(--muted);font-size:12px;line-height:1.65;min-height:42px}
.status-pill{border:1px solid #d7d2c5;border-radius:999px;padding:6px 10px;font-size:12px;background:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.status-pill.loading{color:#666}
.status-pill.up{color:var(--ok);border-color:#b8dec7;background:#f3fcf7}
.status-pill.degraded{color:var(--warn);border-color:#ead3a5;background:#fffbf2}
.status-pill.down{color:var(--err);border-color:#e5b9b4;background:#fff6f5}
.instance-enter{font-size:12px;color:#222;opacity:.86}
.updated{margin-top:10px;font-size:12px;color:var(--muted)}
.ai-panel{margin-top:12px;border:1px solid var(--line);border-radius:14px;background:var(--card);box-shadow:0 10px 28px rgba(36,35,30,.08);padding:12px}
.ai-head{display:flex;justify-content:space-between;align-items:center;gap:8px}
.ai-title{font-size:15px;font-weight:700}
.ai-meta{font-size:12px;color:var(--muted)}
.ai-body{margin-top:8px;border:1px solid #e4dfd2;border-radius:10px;background:#fcfcfa;padding:10px;font-size:13px;line-height:1.65;white-space:pre-wrap;min-height:80px}
.ai-trace-title{margin-top:8px;font-size:12px;color:var(--muted)}
.ai-trace{margin-top:6px;border:1px dashed #d8d3c9;border-radius:10px;background:#faf9f6;padding:8px;font-size:12px;line-height:1.55;white-space:pre-wrap;min-height:50px;max-height:220px;overflow:auto}
.ai-form{margin-top:10px;display:grid;gap:8px}
.ai-row{display:grid;grid-template-columns:120px 1fr;gap:8px;align-items:center}
.ai-row label{font-size:12px;color:var(--muted)}
.ai-row input{width:100%;border:1px solid #d4cec1;border-radius:9px;padding:8px 10px;background:#fff;font-size:12px}
.ai-actions{display:flex;gap:8px;align-items:center;justify-content:flex-end}
.ai-actions .btn{padding:7px 12px}
.ai-actions .btn.test{background:#f8f8f7}
.btn.ai{background:#1f2626;color:#fff;border-color:#1f2626}
.btn.ai:disabled{opacity:.6;cursor:default}
@media (max-width:1240px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.stats{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media (max-width:900px){.layout{grid-template-columns:1fr}.side{padding:14px 12px;border-right:0;border-bottom:1px solid var(--line)}.nav-links{flex-direction:row;overflow:auto;padding-bottom:2px}.nav-link{white-space:nowrap}.main{padding:14px 12px 20px}.hero-title{font-size:36px}.grid{grid-template-columns:1fr}.stats{grid-template-columns:repeat(2,minmax(0,1fr))}}
</style></head>
<body>
<div class="bg-shape bg-a"></div><div class="bg-shape bg-b"></div>
<div class="layout">
  <aside class="side">
    <h1 class="side-brand">COEVO</h1>
    <div class="side-sub" id="sideSub">Agent Instance Center</div>
    <div class="nav-links" id="mainNav">__NAV_LINKS__</div>
  </aside>
  <main class="main">
    <div class="topbar">
      <div>
        <h2 class="hero-title" id="heroTitle">平台实例中控</h2>
        <p class="hero-desc" id="heroDesc">统一查看 OpenClaw 子实例状态、运行摘要与健康信号，支持后续按实例注册表扩展到更多节点。</p>
      </div>
      <div class="actions">
        <button class="btn ai" id="aiBtn">AI分析</button>
        <button class="btn" id="langBtn">EN</button>
      </div>
    </div>

    <section class="stats">
      <div class="stat"><div class="stat-k" id="kTotal">实例总数</div><div class="stat-v" id="vTotal">-</div></div>
      <div class="stat"><div class="stat-k" id="kUp">健康实例</div><div class="stat-v" id="vUp">-</div></div>
      <div class="stat"><div class="stat-k" id="kDeg">降级实例</div><div class="stat-v" id="vDeg">-</div></div>
      <div class="stat"><div class="stat-k" id="kDown">异常实例</div><div class="stat-v" id="vDown">-</div></div>
    </section>

    <section class="grid" id="grid">__CARDS__</section>
    <div class="updated" id="updated">更新时间：-</div>
    <section class="ai-panel">
      <div class="ai-head">
        <div class="ai-title" id="aiTitle">AI分析</div>
        <div class="ai-meta" id="aiMeta">-</div>
      </div>
      <div class="ai-form" id="aiForm">
        <div class="ai-row">
          <label id="aiLblApiKey">API Key</label>
          <input id="aiApiKey" type="password" autocomplete="off" placeholder="sk-..." />
        </div>
        <div class="ai-row">
          <label id="aiLblApiBase">API URL</label>
          <input id="aiApiBase" type="text" autocomplete="off" placeholder="https://api.openai.com/v1" />
        </div>
        <div class="ai-row">
          <label id="aiLblModel">Model</label>
          <input id="aiModel" type="text" autocomplete="off" placeholder="gpt-4.1-mini" />
        </div>
        <div class="ai-actions">
          <button class="btn" id="aiSaveBtn">保存参数</button>
          <button class="btn test" id="aiTestBtn">测试模型</button>
        </div>
      </div>
      <div class="ai-body" id="aiBody">点击“AI分析”生成平台诊断建议。</div>
      <div class="ai-trace-title" id="aiTraceTitle">分析过程</div>
      <div class="ai-trace" id="aiTrace">暂无过程日志</div>
    </section>
  </main>
</div>
<script>
const INSTANCE_LIST = __INSTANCES_JSON__;
const TEXT = {
  zh:{
    sideSub:'Agent \u5b9e\u4f8b\u4e2d\u63a7',
    heroTitle:'\u5e73\u53f0\u5b9e\u4f8b\u4e2d\u63a7',
    heroDesc:'\u7edf\u4e00\u67e5\u770b OpenClaw \u5b50\u5b9e\u4f8b\u72b6\u6001\u3001\u8fd0\u884c\u6458\u8981\u4e0e\u5065\u5eb7\u4fe1\u53f7\uff0c\u652f\u6301\u540e\u7eed\u6309\u5b9e\u4f8b\u6ce8\u518c\u8868\u6269\u5c55\u5230\u66f4\u591a\u8282\u70b9\u3002',
    total:'\u5b9e\u4f8b\u603b\u6570',up:'\u5065\u5eb7\u5b9e\u4f8b',degraded:'\u964d\u7ea7\u5b9e\u4f8b',down:'\u5f02\u5e38\u5b9e\u4f8b',updated:'\u66f4\u65b0\u65f6\u95f4',enter:'\u8fdb\u5165\u5b9e\u4f8b',loading:'\u52a0\u8f7d\u4e2d...',
    aiBtn:'AI\u5206\u6790',aiTitle:'AI\u7f51\u7ad9\u4f53\u68c0',aiInit:'\u70b9\u51fb\u201cAI\u5206\u6790\u201d\u751f\u6210\u5e73\u53f0\u8bca\u65ad\u5efa\u8bae\u3002',aiLoading:'AI\u6b63\u5728\u5206\u6790\u4e2d...',aiUpdated:'\u5206\u6790\u65f6\u95f4',aiErrorPrefix:'\u5206\u6790\u5931\u8d25',
    aiApiKey:'API Key',aiApiBase:'API URL',aiModel:'\u6a21\u578b',aiSave:'\u4fdd\u5b58\u53c2\u6570',aiTest:'\u6d4b\u8bd5\u6a21\u578b',aiSaved:'\u53c2\u6570\u5df2\u4fdd\u5b58\u5230\u672c\u5730\u6d4f\u89c8\u5668\u3002',aiTesting:'\u6b63\u5728\u6d4b\u8bd5\u6a21\u578b\u8fde\u901a\u6027...',aiTestOk:'\u6a21\u578b\u6d4b\u8bd5\u901a\u8fc7',aiTraceTitle:'\u5206\u6790\u8fc7\u7a0b',aiTraceEmpty:'\u6682\u65e0\u8fc7\u7a0b\u65e5\u5fd7',aiTraceLoading:'\u7b49\u5f85\u6a21\u578b\u8f93\u51fa\u5206\u6790\u8fc7\u7a0b...'
  },
  en:{
    sideSub:'Agent Instance Center',heroTitle:'Instance Operations Console',heroDesc:'Unified visibility for OpenClaw sub-instances, runtime summaries, and health signals with registry-first horizontal scaling.',
    total:'Total Instances',up:'Healthy',degraded:'Degraded',down:'Down',updated:'Updated At',enter:'Open Instance',loading:'Loading...',
    aiBtn:'AI Analysis',aiTitle:'AI Site Review',aiInit:'Click "AI Analysis" to generate health review and optimization suggestions.',aiLoading:'AI is analyzing...',aiUpdated:'Analyzed At',aiErrorPrefix:'Analysis Failed',
    aiApiKey:'API Key',aiApiBase:'API URL',aiModel:'Model',aiSave:'Save Config',aiTest:'Test Model',aiSaved:'Configuration saved in browser.',aiTesting:'Testing model connectivity...',aiTestOk:'Model test passed',aiTraceTitle:'Analysis Trace',aiTraceEmpty:'No trace yet.',aiTraceLoading:'Waiting for model trace output...'
  }
};
const AI_CFG_KEY = 'coevo_ai_cfg_v1';
let lang = (localStorage.getItem('coevo_lang') || 'zh') === 'en' ? 'en' : 'zh';
let summaryMap = {};
let aiReport = null;
let aiLoading = false;
let aiTesting = false;
let aiCfg = {api_key:'', api_base:'https://api.openai.com/v1', model:'gpt-4.1-mini'};
let aiProgressTick = 0;
let aiProgressTimer = null;

function safe(v){return String(v===undefined||v===null?'':v);}

function loadAiConfig(){
  try{
    const raw = localStorage.getItem(AI_CFG_KEY) || '{}';
    const cfg = JSON.parse(raw);
    if(cfg && typeof cfg === 'object'){
      aiCfg.api_key = safe(cfg.api_key || aiCfg.api_key);
      aiCfg.api_base = safe(cfg.api_base || aiCfg.api_base);
      aiCfg.model = safe(cfg.model || aiCfg.model);
    }
  }catch(_){ }
}

function fillAiConfigInputs(){
  const k = document.getElementById('aiApiKey');
  const b = document.getElementById('aiApiBase');
  const m = document.getElementById('aiModel');
  if(k) k.value = aiCfg.api_key || '';
  if(b) b.value = aiCfg.api_base || 'https://api.openai.com/v1';
  if(m) m.value = aiCfg.model || 'gpt-4.1-mini';
}

function readAiConfigInputs(){
  const k = document.getElementById('aiApiKey');
  const b = document.getElementById('aiApiBase');
  const m = document.getElementById('aiModel');
  aiCfg.api_key = safe(k && k.value ? k.value.trim() : aiCfg.api_key || '');
  aiCfg.api_base = safe(b && b.value ? b.value.trim() : aiCfg.api_base || 'https://api.openai.com/v1');
  aiCfg.model = safe(m && m.value ? m.value.trim() : aiCfg.model || 'gpt-4.1-mini');
}

function saveAiConfig(showHint=true){
  readAiConfigInputs();
  try{
    localStorage.setItem(AI_CFG_KEY, JSON.stringify({
      api_key: aiCfg.api_key || '',
      api_base: aiCfg.api_base || 'https://api.openai.com/v1',
      model: aiCfg.model || 'gpt-4.1-mini'
    }));
  }catch(_){ }
  if(showHint){
    const t = TEXT[lang];
    aiReport = {ok:true,mode:'config_saved',analysis_text:t.aiSaved || 'Saved'};
    renderAiBox();
  }
}

function buildAiPayload(force){
  readAiConfigInputs();
  return {
    force: !!force,
    api_key: aiCfg.api_key || '',
    api_base: aiCfg.api_base || '',
    model: aiCfg.model || ''
  };
}

function startAiProgress(){
  aiProgressTick = 0;
  if(aiProgressTimer){
    clearInterval(aiProgressTimer);
    aiProgressTimer = null;
  }
  aiProgressTimer = setInterval(()=>{ aiProgressTick += 1; renderAiBox(); }, 900);
}

function stopAiProgress(){
  if(aiProgressTimer){
    clearInterval(aiProgressTimer);
    aiProgressTimer = null;
  }
}

function applyLang(){
  const t = TEXT[lang];
  document.documentElement.lang = lang === 'zh' ? 'zh-CN' : 'en';
  document.title = lang === 'zh' ? 'COEVO \u00b7 \u5b9e\u4f8b\u4e2d\u63a7' : 'COEVO \u00b7 Instance Console';
  document.getElementById('langBtn').textContent = lang === 'zh' ? 'EN' : '\u4e2d\u6587';
  document.getElementById('sideSub').textContent = t.sideSub;
  document.getElementById('heroTitle').textContent = t.heroTitle;
  document.getElementById('heroDesc').textContent = t.heroDesc;
  document.getElementById('kTotal').textContent = t.total;
  document.getElementById('kUp').textContent = t.up;
  document.getElementById('kDeg').textContent = t.degraded;
  document.getElementById('kDown').textContent = t.down;

  const aiBtn = document.getElementById('aiBtn');
  if(aiBtn) aiBtn.textContent = t.aiBtn;
  const aiTitle = document.getElementById('aiTitle');
  if(aiTitle) aiTitle.textContent = t.aiTitle;
  const aiLblApiKey = document.getElementById('aiLblApiKey');
  if(aiLblApiKey) aiLblApiKey.textContent = t.aiApiKey;
  const aiLblApiBase = document.getElementById('aiLblApiBase');
  if(aiLblApiBase) aiLblApiBase.textContent = t.aiApiBase;
  const aiLblModel = document.getElementById('aiLblModel');
  if(aiLblModel) aiLblModel.textContent = t.aiModel;
  const aiSaveBtn = document.getElementById('aiSaveBtn');
  if(aiSaveBtn) aiSaveBtn.textContent = t.aiSave;
  const aiTestBtn = document.getElementById('aiTestBtn');
  if(aiTestBtn) aiTestBtn.textContent = t.aiTest;
  const aiTraceTitle = document.getElementById('aiTraceTitle');
  if(aiTraceTitle) aiTraceTitle.textContent = t.aiTraceTitle || 'Analysis Trace';

  document.querySelectorAll('[data-label-zh]').forEach((el)=>{
    const z = el.getAttribute('data-label-zh') || '';
    const e = el.getAttribute('data-label-en') || z;
    el.textContent = lang === 'zh' ? z : e;
  });
  document.querySelectorAll('[data-zh]').forEach((el)=>{
    const z = el.getAttribute('data-zh') || '';
    const e = el.getAttribute('data-en') || z;
    el.textContent = lang === 'zh' ? z : e;
  });
  INSTANCE_LIST.forEach((meta)=>{
    const enter = document.getElementById('enter-' + meta.id);
    if(enter) enter.textContent = t.enter;
  });
  paintStatuses();
  renderAiBox();
}

function paintStatuses(){
  const t = TEXT[lang];
  INSTANCE_LIST.forEach((meta)=>{
    const row = summaryMap[meta.id];
    const el = document.getElementById('status-' + meta.id);
    if(!el) return;
    if(!row){
      el.className = 'status-pill loading';
      el.textContent = t.loading;
      return;
    }
    const level = String(row.status_level || 'down');
    const statusText = lang === 'zh' ? safe(row.status_text_zh || row.status_text_en || '-') : safe(row.status_text_en || row.status_text_zh || '-');
    const summaryText = lang === 'zh' ? safe(row.summary_zh || row.summary_en || '') : safe(row.summary_en || row.summary_zh || '');
    el.className = 'status-pill ' + (level || 'down');
    el.textContent = summaryText ? (statusText + ' \u00b7 ' + summaryText) : statusText;
  });
}

function renderAiBox(){
  const t = TEXT[lang];
  const body = document.getElementById('aiBody');
  const meta = document.getElementById('aiMeta');
  const trace = document.getElementById('aiTrace');
  if(!body || !meta || !trace) return;
  const progressSteps = [
    lang === 'zh' ? '1/3 \u91c7\u96c6\u5e73\u53f0\u72b6\u6001' : '1/3 Collecting platform status',
    lang === 'zh' ? '2/3 \u8c03\u7528\u6a21\u578b\u5206\u6790' : '2/3 Requesting model analysis',
    lang === 'zh' ? '3/3 \u751f\u6210\u8bca\u65ad\u5efa\u8bae' : '3/3 Building recommendations'
  ];
  if(aiTesting){
    body.textContent = t.aiTesting;
    meta.textContent = '...';
    trace.textContent = (t.aiTraceLoading || 'Loading trace...') + '\n' + (lang === 'zh' ? '\u6b63\u5728\u6267\u884c\u6a21\u578b\u8fde\u901a\u6027\u6d4b\u8bd5' : 'Running model connectivity test');
    return;
  }
  if(aiLoading){
    body.textContent = t.aiLoading;
    meta.textContent = '...';
    const stepText = progressSteps[Math.min(progressSteps.length - 1, Math.floor(aiProgressTick / 2))];
    trace.textContent = (t.aiTraceLoading || 'Loading trace...') + '\n' + stepText;
    return;
  }
  if(!aiReport){
    body.textContent = t.aiInit;
    meta.textContent = '-';
    trace.textContent = t.aiTraceEmpty || 'No trace yet.';
    return;
  }
  if(aiReport.mode === 'config_saved'){
    body.textContent = safe(aiReport.analysis_text || t.aiSaved);
    meta.textContent = '-';
    trace.textContent = t.aiTraceEmpty || 'No trace yet.';
    return;
  }
  let traceText = '';
  const traceRows = Array.isArray(aiReport.trace) ? aiReport.trace : [];
  if(traceRows.length > 0){
    const parts = [];
    traceRows.forEach((row)=>{
      if(!row) return;
      if(typeof row === 'string'){
        if(row.trim()) parts.push('- ' + row.trim());
        return;
      }
      const tm = safe(row.time || '');
      const step = safe(row.step || '');
      const st = safe(row.status || '');
      const detail = safe(row.detail || '');
      const left = [tm, step, st].filter(Boolean).join(' | ');
      parts.push((left ? ('- ' + left) : '-') + (detail ? (' -> ' + detail) : ''));
    });
    traceText = parts.join('\n').trim();
  }
  if(!traceText){
    const c = ((aiReport.snapshot || {}).instance_counts || {});
    if(c && (c.total !== undefined)){
      traceText = (lang === 'zh' ? '\u5b9e\u4f8b\u6c47\u603b: ' : 'Instance summary: ') + JSON.stringify(c);
    }
  }
  trace.textContent = traceText || (t.aiTraceEmpty || 'No trace yet.');
  if(aiReport.ok){
    let mainText = safe(aiReport.analysis_text || aiReport.summary || t.aiInit);
    if(aiReport.mode === 'test_model'){
      const respText = safe(aiReport.test_response_text || '');
      mainText = t.aiTestOk + '\u3002' + (respText ? (' \u8fd4\u56de: ' + respText) : '');
    }
    body.textContent = mainText;
    const at = safe(aiReport.updated_at || '-');
    const model = safe(aiReport.model || aiCfg.model || '-');
    const fromCache = aiReport.from_cache ? 'cache' : 'live';
    meta.textContent = (t.aiUpdated || 'Updated') + ': ' + at + ' | model=' + model + ' | ' + fromCache;
  }else{
    body.textContent = (t.aiErrorPrefix || 'Error') + ': ' + safe(aiReport.error || 'unknown_error');
    meta.textContent = '-';
  }
}

async function runAiAnalysis(force=false){
  aiLoading = true;
  startAiProgress();
  renderAiBox();
  try{
    const r = await fetch('/api/ai-analysis', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(buildAiPayload(force))
    });
    const d = await r.json().catch(()=>({ok:false,error:'invalid_json'}));
    aiReport = d;
  }catch(_){
    aiReport = {ok:false,error:'network_error'};
  }finally{
    aiLoading = false;
    stopAiProgress();
    renderAiBox();
  }
}

async function testAiModel(){
  aiTesting = true;
  startAiProgress();
  renderAiBox();
  try{
    const payload = buildAiPayload(true);
    const r = await fetch('/api/ai-analysis-test', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const d = await r.json().catch(()=>({ok:false,error:'invalid_json'}));
    aiReport = d;
  }catch(_){
    aiReport = {ok:false,error:'network_error'};
  }finally{
    aiTesting = false;
    stopAiProgress();
    renderAiBox();
  }
}

function paintCounts(counts){
  const c = counts || {};
  document.getElementById('vTotal').textContent = safe(c.total ?? '-');
  document.getElementById('vUp').textContent = safe(c.up ?? '-');
  document.getElementById('vDeg').textContent = safe(c.degraded ?? '-');
  document.getElementById('vDown').textContent = safe(c.down ?? '-');
}

async function refreshSummary(){
  try{
    const r = await fetch('/api/instances/summary?_=' + Date.now(), {cache:'no-store'});
    const d = r.ok ? await r.json() : {};
    const rows = Array.isArray(d.instances) ? d.instances : [];
    summaryMap = {};
    rows.forEach((row)=>{ if(row && row.id){ summaryMap[String(row.id)] = row; } });
    paintCounts(d.counts || {});
    paintStatuses();
    const ts = d.updated_at || '';
    const label = TEXT[lang].updated;
    document.getElementById('updated').textContent = label + ': ' + (ts || '-');
  }catch(_){
    paintStatuses();
  }
}

const aiBtn = document.getElementById('aiBtn');
if(aiBtn){
  aiBtn.addEventListener('click', ()=>runAiAnalysis(true));
}
const aiSaveBtn = document.getElementById('aiSaveBtn');
if(aiSaveBtn){
  aiSaveBtn.addEventListener('click', ()=>saveAiConfig(true));
}
const aiTestBtn = document.getElementById('aiTestBtn');
if(aiTestBtn){
  aiTestBtn.addEventListener('click', ()=>testAiModel());
}

document.getElementById('langBtn').addEventListener('click', ()=>{
  lang = lang === 'zh' ? 'en' : 'zh';
  localStorage.setItem('coevo_lang', lang);
  applyLang();
});

loadAiConfig();
fillAiConfigInputs();
applyLang();
renderAiBox();
refreshSummary();
setInterval(refreshSummary, 15000);
</script>
</body></html>
"""
    return (
        html.replace('__NAV_LINKS__', nav_links)
        .replace('__CARDS__', cards_html)
        .replace('__INSTANCES_JSON__', instances_json)
    )


def render_instance_wrapper_html(instance_id: str) -> str:
    item = get_instance_meta(instance_id)
    if not item:
        return '<!doctype html><html><body><h1>instance_not_found</h1></body></html>'

    pub = instance_public_meta(item)
    meta_payload = {
        'id': str(pub.get('id') or ''),
        'name_zh': str(pub.get('name_zh') or ''),
        'name_en': str(pub.get('name_en') or ''),
        'description_zh': str(pub.get('description_zh') or ''),
        'description_en': str(pub.get('description_en') or ''),
        'category_zh': str(pub.get('category_zh') or '实例'),
        'category_en': str(pub.get('category_en') or 'Instance'),
        'chat_agent': str(item.get('chat_agent') or ''),
        'chat_enabled': bool(str(item.get('chat_agent') or '').strip()),
        'raw_path': str(pub.get('raw_path') or f"/instance/{pub.get('id')}/raw"),
    }

    chat_enabled = bool(meta_payload.get('chat_enabled')) and str(meta_payload.get('chat_agent') or '') in CHAT_ALLOWED_AGENTS
    chat_agent = str(meta_payload.get('chat_agent') or '') if chat_enabled else ''

    chat_block = """
    <section class="chat" id="chatCard">
      <div class="chat-head">
        <h3 id="chatTitle">实例对话入口</h3>
        <button class="btn" id="chatClearBtn">清空</button>
      </div>
      <div class="chat-input-row">
        <input id="chatInput" autocomplete="off" />
        <button class="btn" id="chatSendBtn">发送</button>
        <button class="btn" id="chatStopBtn">停止</button>
      </div>
      <div class="chat-log" id="chatLog"></div>
    </section>
    """
    if not chat_enabled:
        chat_block = """
        <section class="chat chat-disabled" id="chatCard">
          <div id="chatDisabledText">当前实例未开放对话入口。</div>
        </section>
        """

    nav_links = _build_instance_nav_links(str(meta_payload.get('id') or ''))
    meta_json = _json_for_html_script(meta_payload)

    html = """<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>COEVO · 实例详情</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@500;700&family=Noto+Sans+SC:wght@400;500;700&display=swap');
:root{--bg:#f7f4ec;--ink:#111315;--muted:#5f635f;--line:#d6d0c2;--card:#ffffffcc;--ok:#1a8a4a;--warn:#b77800;--err:#b42318}
*{box-sizing:border-box}
html,body{margin:0;padding:0;background:var(--bg);color:var(--ink);font-family:"Noto Sans SC",sans-serif}
body{min-height:100vh;background-image:radial-gradient(circle at 10% 8%,rgba(255,255,255,.95),rgba(255,255,255,0) 34%),linear-gradient(132deg,#efebe1 0%,#f8f6f1 58%,#ece7db 100%)}
.layout{display:grid;grid-template-columns:250px 1fr;min-height:100vh}
.side{padding:20px 16px;border-right:1px solid var(--line);background:linear-gradient(180deg,rgba(248,245,238,.85),rgba(248,245,238,.55));backdrop-filter:blur(8px)}
.side-brand{font-family:"Cormorant Garamond",serif;font-size:33px;letter-spacing:.2em;margin:0 0 4px}
.side-sub{font-size:12px;color:var(--muted);margin-bottom:18px}
.nav-links{display:flex;flex-direction:column;gap:8px}
.nav-link{text-decoration:none;color:#1a1f1f;border:1px solid #d9d3c7;border-radius:12px;padding:9px 11px;background:rgba(255,255,255,.62);font-size:13px;transition:all .2s ease}
.nav-link:hover{transform:translateX(2px);background:#fff;border-color:#bdb7aa}
.nav-link.is-active{background:#1f2626;color:#fff;border-color:#1f2626}
.main{padding:20px 22px 24px}
.topbar{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:10px}
.top-title{font-family:"Cormorant Garamond",serif;font-size:32px;letter-spacing:.08em}
.actions{display:flex;gap:8px;align-items:center}
.btn,.link-btn{border:1px solid #b9b4a8;background:#fff;border-radius:11px;padding:8px 12px;font-size:13px;cursor:pointer;text-decoration:none;color:#1b1d1f}
.hero{border:1px solid var(--line);border-radius:16px;background:var(--card);padding:16px;box-shadow:0 10px 28px rgba(36,35,30,.08)}
.hero h1{margin:0;font-size:30px;line-height:1.2}
.hero p{margin:8px 0 0;color:var(--muted);font-size:13px;line-height:1.7}
.meta-chip-row{margin-top:12px;display:flex;flex-wrap:wrap;gap:8px}
.chip{border:1px solid #d9d3c7;border-radius:999px;padding:6px 10px;background:#fff;font-size:12px;max-width:100%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chip.up{color:var(--ok);border-color:#b8dec7;background:#f3fcf7}
.chip.degraded{color:var(--warn);border-color:#ead3a5;background:#fffbf2}
.chip.down{color:var(--err);border-color:#e5b9b4;background:#fff6f5}
.chat{margin-top:12px;border:1px solid var(--line);border-radius:16px;background:var(--card);padding:12px;box-shadow:0 10px 28px rgba(36,35,30,.08)}
.chat-head{display:flex;justify-content:space-between;align-items:center;gap:8px}
.chat-head h3{margin:0;font-size:15px}
.chat-input-row{margin-top:10px;display:flex;gap:8px;align-items:center}
.chat-input-row input{flex:1;border:1px solid #d0caba;border-radius:10px;padding:9px 10px;background:#fff}
.chat-log{margin-top:10px;border:1px solid #e4dfd2;border-radius:10px;background:#fcfcfa;min-height:90px;max-height:260px;overflow:auto;padding:8px;font-size:12px;line-height:1.55}
.chat-line{padding:3px 0;border-bottom:1px dashed #ece7db}
.chat-line:last-child{border-bottom:0}
.chat-line.meta{color:#5f635f}
.chat-line.user{color:#174f84}
.chat-line.error{color:#b42318}
.chat-line.thinking{color:#7f5f16}
.chat-line.tool_call{color:#175b6b}
.chat-line.tool_result{color:#476319}
.chat-disabled{font-size:13px;color:var(--muted)}
.frame{margin-top:12px;border:1px solid var(--line);border-radius:16px;background:var(--card);overflow:hidden;box-shadow:0 10px 28px rgba(36,35,30,.08)}
.frame iframe{display:block;width:100%;height:calc(100vh - 380px);min-height:620px;border:0;background:#fff}
@media (max-width:1020px){.layout{grid-template-columns:1fr}.side{padding:14px 12px;border-right:0;border-bottom:1px solid var(--line)}.nav-links{flex-direction:row;overflow:auto;padding-bottom:2px}.nav-link{white-space:nowrap}.main{padding:14px 12px 18px}.frame iframe{height:calc(100vh - 420px);min-height:560px}}
</style></head>
<body>
<div class="layout">
  <aside class="side">
    <h1 class="side-brand">COEVO</h1>
    <div class="side-sub" id="sideSub">Agent Instance Center</div>
    <div class="nav-links" id="mainNav">__NAV_LINKS__</div>
  </aside>
  <main class="main">
    <div class="topbar">
      <div class="top-title" id="topTitle">实例详情</div>
      <div class="actions">
        <a class="link-btn" id="openRawBtn" target="_blank" rel="noopener">原始面板</a>
        <button class="btn" id="langBtn">EN</button>
      </div>
    </div>

    <section class="hero">
      <h1 id="heroName">-</h1>
      <p id="heroDesc">-</p>
      <div class="meta-chip-row">
        <span class="chip" id="chipStatus">状态：加载中...</span>
        <span class="chip" id="chipSummary">摘要：-</span>
        <span class="chip" id="chipLatency">延迟：-</span>
        <span class="chip" id="chipUpdated">更新：-</span>
        <span class="chip" id="chipError">错误：-</span>
      </div>
    </section>

    __CHAT_BLOCK__

    <section class="frame">
      <iframe id="rawFrame" title="instance raw"></iframe>
    </section>
  </main>
</div>
<script>
const META = __META_JSON__;
const CHAT_ENABLED = __CHAT_ENABLED__;
const CHAT_AGENT = "__CHAT_AGENT__";
const TEXT = {
  zh:{
    sideSub:'Agent 实例中控',pageTitle:'实例详情',rawPanel:'原始面板',status:'状态',summary:'摘要',latency:'延迟',updated:'更新',error:'错误',loading:'加载中...',
    chatTitle:'实例对话入口',chatPlaceholder:'输入后回车，消息将发送到当前实例 Agent',send:'发送',stop:'停止',clear:'清空',chatDisabled:'当前实例未开放对话入口。'
  },
  en:{
    sideSub:'Agent Instance Center',pageTitle:'Instance Details',rawPanel:'Raw Panel',status:'Status',summary:'Summary',latency:'Latency',updated:'Updated',error:'Errors',loading:'Loading...',
    chatTitle:'Instance Chat',chatPlaceholder:'Press Enter to send message to the current instance agent',send:'Send',stop:'Stop',clear:'Clear',chatDisabled:'Chat is not enabled for this instance.'
  }
};
let lang = (localStorage.getItem('coevo_lang') || 'zh') === 'en' ? 'en' : 'zh';
let statusCache = null;
let errorCount = '-';
let stream = null;
function safe(v){return String(v===undefined||v===null?'':v);}
function setChip(id,label,value,level){
  const el = document.getElementById(id);
  if(!el) return;
  el.textContent = label + '：' + (value || '-');
  if(id === 'chipStatus'){ el.className = 'chip ' + (level || ''); }
}
function applyLang(){
  const t = TEXT[lang];
  const isZh = lang === 'zh';
  document.documentElement.lang = isZh ? 'zh-CN' : 'en';
  document.title = 'COEVO · ' + (isZh ? (META.name_zh || META.id) : (META.name_en || META.id));
  document.getElementById('langBtn').textContent = isZh ? 'EN' : '中文';
  document.getElementById('sideSub').textContent = t.sideSub;
  document.getElementById('topTitle').textContent = t.pageTitle;
  document.getElementById('openRawBtn').textContent = t.rawPanel;
  document.getElementById('openRawBtn').href = META.raw_path || ('/instance/' + META.id + '/raw');
  document.getElementById('heroName').textContent = isZh ? (META.name_zh || META.id) : (META.name_en || META.id);
  document.getElementById('heroDesc').textContent = isZh ? (META.description_zh || '') : (META.description_en || '');
  document.querySelectorAll('[data-label-zh]').forEach((el)=>{
    const z = el.getAttribute('data-label-zh') || '';
    const e = el.getAttribute('data-label-en') || z;
    el.textContent = isZh ? z : e;
  });
  if(CHAT_ENABLED){
    const title = document.getElementById('chatTitle');
    if(title) title.textContent = t.chatTitle;
    const input = document.getElementById('chatInput');
    if(input) input.placeholder = t.chatPlaceholder;
    const sendBtn = document.getElementById('chatSendBtn');
    if(sendBtn) sendBtn.textContent = t.send;
    const stopBtn = document.getElementById('chatStopBtn');
    if(stopBtn) stopBtn.textContent = t.stop;
    const clearBtn = document.getElementById('chatClearBtn');
    if(clearBtn) clearBtn.textContent = t.clear;
  } else {
    const dis = document.getElementById('chatDisabledText');
    if(dis) dis.textContent = t.chatDisabled;
  }
  renderStatus();
}
function renderStatus(){
  const t = TEXT[lang];
  if(!statusCache){
    setChip('chipStatus', t.status, t.loading, '');
    setChip('chipSummary', t.summary, '-', '');
    setChip('chipLatency', t.latency, '-', '');
    setChip('chipUpdated', t.updated, '-', '');
    setChip('chipError', t.error, errorCount, '');
    return;
  }
  const isZh = lang === 'zh';
  const level = safe(statusCache.status_level || 'down');
  const statusText = isZh ? safe(statusCache.status_text_zh || statusCache.status_text_en || '-') : safe(statusCache.status_text_en || statusCache.status_text_zh || '-');
  const summaryText = isZh ? safe(statusCache.summary_zh || statusCache.summary_en || '-') : safe(statusCache.summary_en || statusCache.summary_zh || '-');
  const latencyText = statusCache.latency_ms === undefined || statusCache.latency_ms === null ? '-' : (safe(statusCache.latency_ms) + 'ms');
  const updatedText = safe(statusCache.updated_at || '-');
  const errText = errorCount === '-' ? safe(statusCache.error || '-') : errorCount;
  setChip('chipStatus', t.status, statusText, level);
  setChip('chipSummary', t.summary, summaryText, '');
  setChip('chipLatency', t.latency, latencyText, '');
  setChip('chipUpdated', t.updated, updatedText, '');
  setChip('chipError', t.error, errText, '');
}
async function refreshStatus(){
  try{
    const r = await fetch('/api/instances/' + encodeURIComponent(META.id) + '/status?_=' + Date.now(), {cache:'no-store'});
    const d = r.ok ? await r.json() : {};
    statusCache = d;
  }catch(_){
    statusCache = {status_level:'down',status_text_zh:'异常',status_text_en:'Down',summary_zh:'状态读取失败',summary_en:'Status read failed',latency_ms:0,updated_at:'-',error:'status_fetch_failed'};
  }
  renderStatus();
}
async function refreshErrors(){
  if(!CHAT_ENABLED){ errorCount = '-'; renderStatus(); return; }
  try{
    const r = await fetch('/api/agent-errors?_=' + Date.now(), {cache:'no-store'});
    const d = r.ok ? await r.json() : {};
    const one = (((d || {}).agents || {})[CHAT_AGENT] || {});
    const count = Number(one.session_error_count || 0) + Number(one.gateway_error_count || 0);
    errorCount = String(count);
  }catch(_){ errorCount = '-'; }
  renderStatus();
}
function appendChat(kind, text, ts){
  const box = document.getElementById('chatLog');
  if(!box) return;
  const div = document.createElement('div');
  div.className = 'chat-line ' + String(kind || 'meta');
  const stamp = ts ? ('[' + ts + '] ') : '';
  div.textContent = stamp + text;
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}
function stopChat(){
  if(stream){
    stream.close();
    stream = null;
    appendChat('meta', '[system] stream stopped');
  }
}
function startChat(){
  if(!CHAT_ENABLED) return;
  const input = document.getElementById('chatInput');
  const q = (input && input.value ? input.value : '').trim();
  if(!q) return;
  stopChat();
  appendChat('user', q, new Date().toLocaleTimeString());
  if(input) input.value = '';
  const url = '/api/agent-chat-stream?agent=' + encodeURIComponent(CHAT_AGENT) + '&timeout=240&message=' + encodeURIComponent(q);
  stream = new EventSource(url);
  stream.addEventListener('chunk', (ev)=>{
    try{
      const d = JSON.parse(ev.data || '{}');
      let text = safe(d.text || '');
      const kind = safe(d.kind || 'meta');
      if(kind === 'thinking') text = '[thinking] ' + text;
      if(kind === 'tool_call') text = '[toolCall] ' + text;
      if(kind === 'tool_result') text = '[toolResult] ' + text;
      if(kind === 'error') text = '[error] ' + text;
      const role = safe(d.role || '');
      appendChat(kind, (role ? (role + ': ') : '') + text, d.ts || '');
    }catch(_){ }
  });
  stream.addEventListener('log', (ev)=>{
    try{const d = JSON.parse(ev.data || '{}'); if(d.text) appendChat('meta', '[cli] ' + safe(d.text));}catch(_){ }
  });
  stream.addEventListener('error', (ev)=>{
    try{const d = JSON.parse(ev.data || '{}'); appendChat('error', '[stream] ' + safe(d.error || 'error'));}catch(_){ appendChat('error', '[stream] error'); }
  });
  stream.addEventListener('done', (ev)=>{
    try{const d = JSON.parse(ev.data || '{}'); appendChat('meta', '[done] ok=' + safe(d.ok) + ' returncode=' + safe(d.returncode));}catch(_){ }
    if(stream){ stream.close(); stream = null; }
  });
}
if(CHAT_ENABLED){
  const sendBtn = document.getElementById('chatSendBtn');
  const stopBtn = document.getElementById('chatStopBtn');
  const clearBtn = document.getElementById('chatClearBtn');
  const input = document.getElementById('chatInput');
  if(sendBtn) sendBtn.addEventListener('click', startChat);
  if(stopBtn) stopBtn.addEventListener('click', stopChat);
  if(clearBtn) clearBtn.addEventListener('click', ()=>{ const box = document.getElementById('chatLog'); if(box) box.innerHTML = ''; });
  if(input) input.addEventListener('keydown', (ev)=>{ if(ev.key === 'Enter'){ startChat(); } });
}
document.getElementById('rawFrame').src = META.raw_path || ('/instance/' + META.id + '/raw');
document.getElementById('langBtn').addEventListener('click', ()=>{
  lang = lang === 'zh' ? 'en' : 'zh';
  localStorage.setItem('coevo_lang', lang);
  applyLang();
});
applyLang();
refreshStatus();
refreshErrors();
setInterval(refreshStatus, 15000);
if(CHAT_ENABLED){ setInterval(refreshErrors, 18000); }
</script>
</body></html>
"""
    return (
        html.replace('__NAV_LINKS__', nav_links)
        .replace('__META_JSON__', meta_json)
        .replace('__CHAT_BLOCK__', chat_block)
        .replace('__CHAT_ENABLED__', 'true' if chat_enabled else 'false')
        .replace('__CHAT_AGENT__', _escape_html(chat_agent))
    )

def _load_wallet_private_key() -> str:
    wallet_key = os.getenv('WALLET_PRIVATE_KEY', '').strip()
    if wallet_key:
        return wallet_key
    if os.path.exists(CLAW402_GATEWAY_CONF):
        try:
            with open(CLAW402_GATEWAY_CONF, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    row = line.strip()
                    if row.startswith('Environment=WALLET_PRIVATE_KEY='):
                        wallet_key = row.split('=', 2)[2].strip().strip('"').strip("'")
                        if wallet_key:
                            return wallet_key
        except Exception:
            return ''
    return ''


def _claw402_default_usage() -> Dict[str, Any]:
    return {
        'total_calls': 0,
        'success_calls': 0,
        'failed_calls': 0,
        'by_endpoint': {},
        'recent_calls': [],
        'last_call_at': '',
        'updated_at_ms': 0,
    }


def _claw402_read_usage() -> Dict[str, Any]:
    data = read_json(CLAW402_USAGE_FILE, {})
    if not isinstance(data, dict):
        return _claw402_default_usage()
    base = _claw402_default_usage()
    base.update({k: data.get(k, base[k]) for k in base.keys()})
    if not isinstance(base.get('by_endpoint'), dict):
        base['by_endpoint'] = {}
    if not isinstance(base.get('recent_calls'), list):
        base['recent_calls'] = []
    return base


def _claw402_update_usage(endpoint: str, ok: bool, duration_ms: int, error_msg: str = '') -> None:
    ep = str(endpoint or '').strip()[:160] or 'unknown'
    with CLAW402_USAGE_LOCK:
        data = _claw402_read_usage()
        now_iso = datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S')
        data['total_calls'] = int(data.get('total_calls') or 0) + 1
        if ok:
            data['success_calls'] = int(data.get('success_calls') or 0) + 1
        else:
            data['failed_calls'] = int(data.get('failed_calls') or 0) + 1
        data['last_call_at'] = now_iso
        data['updated_at_ms'] = now_ms()

        by_ep = data.get('by_endpoint') if isinstance(data.get('by_endpoint'), dict) else {}
        one = by_ep.get(ep) if isinstance(by_ep.get(ep), dict) else {}
        one['count'] = int(one.get('count') or 0) + 1
        if ok:
            one['success'] = int(one.get('success') or 0) + 1
        else:
            one['failed'] = int(one.get('failed') or 0) + 1
        one['last_call_at'] = now_iso
        one['last_duration_ms'] = int(duration_ms or 0)
        if error_msg:
            one['last_error'] = str(error_msg)[:220]
        by_ep[ep] = one
        data['by_endpoint'] = by_ep

        recent = data.get('recent_calls') if isinstance(data.get('recent_calls'), list) else []
        recent.insert(0, {
            'time': now_iso,
            'endpoint': ep,
            'ok': bool(ok),
            'duration_ms': int(duration_ms or 0),
            'error': str(error_msg or '')[:220],
        })
        data['recent_calls'] = recent[:80]
        write_json(CLAW402_USAGE_FILE, data)


def _claw402_usage_snapshot() -> Dict[str, Any]:
    data = _claw402_read_usage()
    total = int(data.get('total_calls') or 0)
    success = int(data.get('success_calls') or 0)
    failed = int(data.get('failed_calls') or 0)
    rate = round((success * 100.0 / total), 2) if total > 0 else 0.0
    by_ep = data.get('by_endpoint') if isinstance(data.get('by_endpoint'), dict) else {}
    top_eps = []
    for path, row in by_ep.items():
        if not isinstance(row, dict):
            continue
        c = int(row.get('count') or 0)
        s = int(row.get('success') or 0)
        f = int(row.get('failed') or 0)
        top_eps.append({
            'path': path,
            'count': c,
            'success': s,
            'failed': f,
            'success_rate': round((s * 100.0 / c), 2) if c > 0 else 0.0,
            'last_call_at': row.get('last_call_at') or '-',
            'last_duration_ms': int(row.get('last_duration_ms') or 0),
        })
    top_eps.sort(key=lambda x: x.get('count', 0), reverse=True)
    return {
        'ok': True,
        'total_calls': total,
        'success_calls': success,
        'failed_calls': failed,
        'success_rate': rate,
        'endpoint_count': len(top_eps),
        'top_endpoints': top_eps[:20],
        'recent_calls': (data.get('recent_calls') or [])[:30],
        'last_call_at': data.get('last_call_at') or '-',
        'updated_at_ms': int(data.get('updated_at_ms') or 0),
    }


def _claw402_category_meta(path: str) -> Dict[str, str]:
    p = str(path or '')
    if p.startswith('/api/v1/coinank/'):
        return {'key': 'coinank', 'label': 'CoinAnk \u884d\u751f\u54c1/\u6307\u6807'}
    if p.startswith('/api/v1/nofx/'):
        return {'key': 'nofx', 'label': 'NoFx \u6392\u884c/\u8d44\u91d1\u6d41'}
    if p.startswith('/api/v1/crypto/cmc/'):
        return {'key': 'cmc', 'label': 'CMC \u73b0\u8d27/DEX'}
    if p.startswith('/api/v1/ai/'):
        return {'key': 'ai', 'label': 'AI \u6a21\u578b'}
    if p.startswith('/api/v1/alpaca/'):
        return {'key': 'alpaca', 'label': '\u7f8e\u80a1 Alpaca'}
    if p.startswith('/api/v1/polygon/'):
        return {'key': 'polygon', 'label': '\u7f8e\u80a1 Polygon'}
    if p.startswith('/api/v1/stocks/us/'):
        return {'key': 'stocks_us', 'label': '\u7f8e\u80a1\u57fa\u7840\u6570\u636e'}
    if p.startswith('/api/v1/stocks/cn/'):
        return {'key': 'stocks_cn', 'label': 'A\u80a1\u6570\u636e'}
    if p.startswith('/api/v1/twelvedata/'):
        return {'key': 'twelvedata', 'label': '\u5916\u6c47/\u5b8f\u89c2/\u91d1\u5c5e'}
    return {'key': 'other', 'label': '\u5176\u4ed6'}


def _claw402_enum_from_hint(hint: str) -> List[str]:
    s = str(hint or '')
    m = re.search(r'\(([^)]*)\)', s)
    if not m:
        return []
    raw = m.group(1)
    vals = [x.strip() for x in re.split(r'[/,|]', raw) if x.strip()]
    dedup: List[str] = []
    seen = set()
    for v in vals:
        if v not in seen:
            seen.add(v)
            dedup.append(v)
    return dedup[:20]


def _claw402_param_default(name: str, path: str) -> str:
    n = str(name or '')
    p = str(path or '')
    mapping = {
        'symbol': 'BTCUSDT',
        'baseCoin': 'BTC',
        'exchange': 'Binance',
        'interval': '1d',
        'limit': '20',
        'size': '20',
        'page': '1',
        'pageSize': '20',
        'lang': 'zh',
        'sortType': 'desc',
        'duration': '1h',
        'productType': 'SWAP',
        'type': 'current',
        'network_slug': 'base',
        'query': 'BTC',
        'symbols': 'BTC,ETH',
        'timeframe': '1Day',
        'market_type': 'stocks',
        'ts_code': '000001.SZ',
        'trade_date': datetime.now(TZ_CN).strftime('%Y%m%d'),
        'start_date': '20240101',
        'end_date': datetime.now(TZ_CN).strftime('%Y%m%d'),
        'from': '2024-01-01',
        'to': datetime.now(TZ_CN).strftime('%Y-%m-%d'),
    }
    if n == 'symbol' and ('/stocks/us/' in p or '/alpaca/' in p or '/polygon/' in p):
        return 'AAPL'
    if n == 'symbols' and ('/alpaca/' in p):
        return 'AAPL,TSLA'
    if n == 'exchange' and '/coinank/' in p:
        return 'Binance'
    return mapping.get(n, '')


def _claw402_param_options(name: str, hint: str) -> List[str]:
    n = str(name or '')
    enum_vals = _claw402_enum_from_hint(hint)
    if enum_vals:
        return enum_vals
    presets = {
        'interval': ['15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '3d', '1w'],
        'exchange': ['Binance', 'OKX', 'Bybit', 'Deribit'],
        'sortType': ['desc', 'asc'],
        'lang': ['zh', 'en'],
        'productType': ['SWAP', 'SPOT'],
        'type': ['current', 'day', 'week', 'month', 'year'],
        'market_type': ['stocks', 'crypto'],
        'timeframe': ['1Min', '5Min', '15Min', '1Hour', '1Day'],
    }
    return presets.get(n, [])


def _claw402_extract_params(params_col: str, path: str, sample_params: Dict[str, Any]) -> List[Dict[str, Any]]:
    s = str(params_col or '').strip()
    if not s or s in {'-', '?', '--'}:
        s = ''
    s = s.replace('`', '')
    rows: List[Dict[str, Any]] = []
    used = set()
    if s:
        parts = [x.strip() for x in s.split(',') if x.strip()]
        for part in parts:
            name = re.split(r'[\s(]', part, 1)[0].strip()
            if not name or not re.fullmatch(r'[A-Za-z0-9_]+', name):
                continue
            dval = ''
            if isinstance(sample_params, dict) and name in sample_params:
                dval = str(sample_params.get(name) or '')
            if not dval:
                dval = _claw402_param_default(name, path)
            rows.append({
                'name': name,
                'hint': part,
                'default': dval,
                'options': _claw402_param_options(name, part),
            })
            used.add(name)
    if isinstance(sample_params, dict):
        for k, v in sample_params.items():
            name = str(k or '').strip()
            if not re.fullmatch(r'[A-Za-z0-9_]+', name) or name in used:
                continue
            dval = str(v or '') or _claw402_param_default(name, path)
            rows.append({
                'name': name,
                'hint': name,
                'default': dval,
                'options': _claw402_param_options(name, name),
            })
            used.add(name)
    return rows[:20]


def _claw402_parse_examples(skill_text: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for raw in skill_text.splitlines():
        line = raw.strip()
        if 'node scripts/query.mjs /api/v1/' not in line:
            continue
        m = re.search(r'node\s+scripts/query\.mjs\s+(/api/v1/[^\s]+)\s*(.*)$', line)
        if not m:
            continue
        path = m.group(1).strip()
        rest = (m.group(2) or '').strip()
        if path in out:
            continue
        item: Dict[str, Any] = {'params': {}, 'post_body': ''}
        if '--post' in rest:
            i = rest.find('--post')
            body_raw = rest[i + len('--post'):].strip()
            if body_raw.startswith("'") and body_raw.endswith("'") and len(body_raw) >= 2:
                body_raw = body_raw[1:-1]
            item['post_body'] = body_raw[:3000]
        else:
            try:
                toks = shlex.split(rest)
            except Exception:
                toks = rest.split()
            for tok in toks:
                if '=' not in tok:
                    continue
                k, v = tok.split('=', 1)
                kk = re.sub(r'[^A-Za-z0-9_]', '', k)
                if not kk:
                    continue
                item['params'][kk] = str(v)[:160]
        out[path] = item
    return out


def _claw402_catalog_payload(force_reload: bool = False) -> Dict[str, Any]:
    with CLAW402_CATALOG_LOCK:
        mtime = 0.0
        try:
            mtime = os.path.getmtime(CLAW402_SKILL_MD)
        except Exception:
            mtime = 0.0
        cache = CLAW402_CATALOG_CACHE.get('payload') if isinstance(CLAW402_CATALOG_CACHE, dict) else None
        if (not force_reload) and cache and float(CLAW402_CATALOG_CACHE.get('mtime') or 0.0) == float(mtime):
            return cache

        if not os.path.exists(CLAW402_SKILL_MD):
            payload = {'ok': False, 'error': 'skill_md_missing', 'path': CLAW402_SKILL_MD, 'categories': [], 'endpoints': []}
            CLAW402_CATALOG_CACHE['mtime'] = float(mtime)
            CLAW402_CATALOG_CACHE['payload'] = payload
            return payload

        skill_text = read_text(CLAW402_SKILL_MD, '')
        examples = _claw402_parse_examples(skill_text)

        endpoint_map: Dict[str, Dict[str, Any]] = {}
        for raw in skill_text.splitlines():
            line = raw.strip()
            if not line.startswith('|') or '`/api/v1/' not in line:
                continue
            cols = [c.strip() for c in line.split('|')[1:-1]]
            if not cols:
                continue
            path = cols[0].replace('`', '').strip()
            if not path.startswith('/api/v1/'):
                continue

            method = 'GET'
            desc = ''
            params_col = ''
            if len(cols) >= 2 and cols[1].upper() in {'GET', 'POST'}:
                method = cols[1].upper()
                if len(cols) >= 3:
                    desc = cols[2]
                if len(cols) >= 4:
                    params_col = cols[3]
            else:
                if len(cols) >= 2:
                    desc = cols[1]
                if len(cols) >= 3:
                    params_col = cols[2]

            ex = examples.get(path, {}) if isinstance(examples.get(path), dict) else {}
            sample_params = ex.get('params') if isinstance(ex.get('params'), dict) else {}
            sample_post = str(ex.get('post_body') or '')
            if sample_post and method != 'POST':
                method = 'POST'

            if path.startswith('/api/v1/ai/') and method != 'POST':
                if any(x in path for x in ['/chat', '/messages', '/embeddings', '/images', '/completions', '/mcp']):
                    method = 'POST'

            meta = _claw402_category_meta(path)
            endpoint_map[path] = {
                'path': path,
                'method': method,
                'description': desc,
                'category_key': meta['key'],
                'category_label': meta['label'],
                'params': _claw402_extract_params(params_col, path, sample_params),
                'sample_post_body': sample_post[:4000],
            }

        for path, ex in examples.items():
            if path in endpoint_map:
                continue
            meta = _claw402_category_meta(path)
            sample_params = ex.get('params') if isinstance(ex.get('params'), dict) else {}
            sample_post = str(ex.get('post_body') or '')
            method = 'POST' if sample_post else 'GET'
            endpoint_map[path] = {
                'path': path,
                'method': method,
                'description': '',
                'category_key': meta['key'],
                'category_label': meta['label'],
                'params': _claw402_extract_params('', path, sample_params),
                'sample_post_body': sample_post[:4000],
            }

        order = ['coinank', 'nofx', 'cmc', 'ai', 'alpaca', 'polygon', 'stocks_us', 'stocks_cn', 'twelvedata', 'other']
        endpoints = sorted(
            endpoint_map.values(),
            key=lambda x: (order.index(x.get('category_key')) if x.get('category_key') in order else 999, x.get('path', '')),
        )

        counts: Dict[str, int] = {}
        labels: Dict[str, str] = {}
        for ep in endpoints:
            k = str(ep.get('category_key') or 'other')
            counts[k] = counts.get(k, 0) + 1
            labels[k] = str(ep.get('category_label') or k)
        categories = []
        for k in order:
            if k in counts:
                categories.append({'key': k, 'label': labels.get(k, k), 'count': counts[k]})

        payload = {
            'ok': True,
            'source': CLAW402_SKILL_MD,
            'source_mtime': mtime,
            'generated_at': datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S'),
            'categories': categories,
            'endpoints': endpoints,
        }
        CLAW402_CATALOG_CACHE['mtime'] = float(mtime)
        CLAW402_CATALOG_CACHE['payload'] = payload
        return payload


def _claw402_rpc_call(rpc_url: str, method: str, params: List[Any]) -> Dict[str, Any]:
    try:
        r = requests.post(
            rpc_url,
            json={'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params},
            timeout=8,
            headers={'Content-Type': 'application/json'},
        )
        obj = r.json() if r.content else {}
        if isinstance(obj, dict) and obj.get('error'):
            return {'ok': False, 'error': obj.get('error')}
        return {'ok': True, 'result': (obj or {}).get('result')}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def _claw402_wallet_payload() -> Dict[str, Any]:
    key = _load_wallet_private_key()
    gateway = os.getenv('CLAW402_GATEWAY', 'https://claw402.ai')
    base = {
        'ok': False,
        'configured': bool(key),
        'gateway': gateway,
        'address': '-',
        'balances': {'base_eth': None, 'base_usdc': None},
        'errors': [],
        'updated_at': datetime.now(TZ_CN).strftime('%Y-%m-%d %H:%M:%S'),
    }
    if not key:
        base['errors'].append('wallet_private_key_missing')
        return base

    try:
        env = os.environ.copy()
        env['WALLET_PRIVATE_KEY'] = key
        cp = subprocess.run(
            [
                'node',
                '-e',
                "const { privateKeyToAccount } = require('web3-eth-accounts'); const a = privateKeyToAccount(process.env.WALLET_PRIVATE_KEY); process.stdout.write(String(a.address||''));",
            ],
            cwd=CLAW402_SKILL_DIR,
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
        )
        addr = (cp.stdout or '').strip()
        if cp.returncode != 0 or not re.fullmatch(r'0x[a-fA-F0-9]{40}', addr):
            base['errors'].append('wallet_address_derive_failed')
            return base
        base['address'] = addr
    except Exception as e:
        base['errors'].append(f'wallet_address_error:{e}')
        return base

    bal = _claw402_rpc_call(CLAW402_BASE_RPC, 'eth_getBalance', [base['address'], 'latest'])
    if bal.get('ok'):
        try:
            wei = int(str(bal.get('result') or '0x0'), 16)
            base['balances']['base_eth'] = round(wei / 1e18, 8)
        except Exception:
            base['errors'].append('eth_balance_parse_failed')
    else:
        base['errors'].append(f"eth_balance_error:{bal.get('error')}")

    try:
        addr_hex = str(base['address'])[2:].lower().rjust(64, '0')
        data = '0x70a08231' + addr_hex
        usdc = _claw402_rpc_call(CLAW402_BASE_RPC, 'eth_call', [{'to': CLAW402_BASE_USDC, 'data': data}, 'latest'])
        if usdc.get('ok'):
            raw = int(str(usdc.get('result') or '0x0'), 16)
            base['balances']['base_usdc'] = round(raw / 1e6, 6)
        else:
            base['errors'].append(f"usdc_balance_error:{usdc.get('error')}")
    except Exception as e:
        base['errors'].append(f'usdc_balance_parse_failed:{e}')

    base['ok'] = True
    return base


def _claw402_parse_query_output(stdout_text: str) -> Dict[str, Any]:
    out = str(stdout_text or '').strip()
    if not out:
        return {'ok': False, 'error': 'empty_stdout', 'response': {}}
    pos = out.find('{')
    if pos > 0:
        out = out[pos:]
    try:
        payload = json.loads(out)
        return {'ok': True, 'response': payload}
    except Exception:
        clipped = out
        truncated = False
        if len(clipped) > 280000:
            clipped = clipped[:280000]
            truncated = True
        return {'ok': True, 'response': {'raw_text': clipped, 'truncated': truncated}}


def _claw402_run_request(path: str, method: str, params: Dict[str, Any], post_body: Any) -> Dict[str, Any]:
    safe_path = str(path or '').strip()
    safe_method = str(method or 'GET').upper().strip()
    if safe_method not in {'GET', 'POST'}:
        safe_method = 'GET'
    if not re.fullmatch(r'/api/v1/[A-Za-z0-9/_-]+', safe_path):
        return {'ok': False, 'error': 'invalid_path', 'path': safe_path}

    wallet_key = _load_wallet_private_key()
    if not wallet_key:
        return {'ok': False, 'error': 'wallet_private_key_missing'}
    if not os.path.exists(CLAW402_QUERY_SCRIPT):
        return {'ok': False, 'error': 'query_script_missing', 'path': CLAW402_QUERY_SCRIPT}

    clean_params: Dict[str, str] = {}
    if isinstance(params, dict):
        for k, v in params.items():
            kk = re.sub(r'[^A-Za-z0-9_]', '', str(k or ''))
            if not kk:
                continue
            vv = str(v if v is not None else '').strip()
            if vv == '':
                continue
            clean_params[kk] = vv[:300]

    target_path = safe_path
    cmd = ['node', CLAW402_QUERY_SCRIPT]

    post_json = ''
    if safe_method == 'POST':
        if clean_params:
            target_path = safe_path + '?' + urlencode(clean_params)
        if isinstance(post_body, (dict, list)):
            post_json = json.dumps(post_body, ensure_ascii=False)
        else:
            raw_body = str(post_body or '').strip()
            if not raw_body:
                raw_body = '{}'
            try:
                json.loads(raw_body)
                post_json = raw_body
            except Exception:
                return {'ok': False, 'error': 'invalid_post_json'}
        cmd.extend([target_path, '--post', post_json])
    else:
        cmd.append(target_path)
        for k in sorted(clean_params.keys()):
            cmd.append(f'{k}={clean_params[k]}')

    preview_parts = ['node scripts/query.mjs', target_path]
    if safe_method == 'POST':
        pv = post_json
        if len(pv) > 220:
            pv = pv[:220] + '...'
        preview_parts.append("--post '" + pv + "'")
    else:
        for k in sorted(clean_params.keys()):
            preview_parts.append(f'{k}={clean_params[k]}')
    command_preview = ' '.join(preview_parts)

    env = os.environ.copy()
    env['WALLET_PRIVATE_KEY'] = wallet_key
    t0 = time.time()
    try:
        cp = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=95,
            cwd=os.path.dirname(CLAW402_QUERY_SCRIPT),
            env=env,
        )
    except Exception as e:
        duration_ms = int((time.time() - t0) * 1000)
        _claw402_update_usage(safe_path, False, duration_ms, str(e))
        return {'ok': False, 'error': 'subprocess_failed', 'detail': str(e), 'duration_ms': duration_ms}

    duration_ms = int((time.time() - t0) * 1000)
    if cp.returncode != 0:
        detail = (cp.stderr or cp.stdout or 'claw402_call_failed').strip()
        _claw402_update_usage(safe_path, False, duration_ms, detail)
        return {
            'ok': False,
            'error': 'claw402_call_failed',
            'detail': detail[:800],
            'duration_ms': duration_ms,
            'path': safe_path,
            'method': safe_method,
            'command_preview': command_preview,
        }

    parsed = _claw402_parse_query_output(cp.stdout or '')
    if not parsed.get('ok'):
        _claw402_update_usage(safe_path, False, duration_ms, str(parsed.get('error') or 'parse_failed'))
        return {
            'ok': False,
            'error': parsed.get('error') or 'parse_failed',
            'duration_ms': duration_ms,
            'path': safe_path,
            'method': safe_method,
            'command_preview': command_preview,
        }

    _claw402_update_usage(safe_path, True, duration_ms, '')
    return {
        'ok': True,
        'path': safe_path,
        'method': safe_method,
        'duration_ms': duration_ms,
        'params': clean_params,
        'command_preview': command_preview,
        'response': parsed.get('response'),
    }

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload: bytes, ctype: str = 'application/json; charset=utf-8') -> None:
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def _send_sse_headers(self) -> None:
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Connection', 'keep-alive')
        self.end_headers()

    def _send_sse_event(self, event: str, data: Dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False)
        chunk = f'event: {event}\ndata: {payload}\n\n'.encode('utf-8', errors='replace')
        self.wfile.write(chunk)
        self.wfile.flush()

    def _send_sse_ping(self) -> None:
        self.wfile.write(b': ping\n\n')
        self.wfile.flush()

    def _handle_agent_chat_stream(self, parsed) -> None:
        qs = parse_qs(parsed.query)
        agent = str((qs.get('agent') or [''])[0]).strip().lower()
        message = str((qs.get('message') or [''])[0]).strip()
        timeout_sec = int(str((qs.get('timeout') or ['180'])[0]).strip() or '180')
        timeout_sec = max(30, min(timeout_sec, 600))

        if agent not in CHAT_ALLOWED_AGENTS:
            self._send(400, json.dumps({'error': 'invalid_agent', 'allowed': sorted(CHAT_ALLOWED_AGENTS)}, ensure_ascii=False).encode('utf-8'))
            return
        if not message:
            self._send(400, json.dumps({'error': 'empty_message'}, ensure_ascii=False).encode('utf-8'))
            return

        session_file = get_agent_session_file(agent)
        offset = 0
        if session_file and os.path.isfile(session_file):
            try:
                offset = os.path.getsize(session_file)
            except Exception:
                offset = 0

        cmd = [
            'openclaw',
            'agent',
            '--agent',
            agent,
            '--session-id',
            f'agent:{agent}:main',
            '--message',
            message,
            '--timeout',
            str(timeout_sec),
        ]
        proc = None
        try:
            self._send_sse_headers()
            self._send_sse_event(
                'meta',
                {
                    'ok': True,
                    'agent': agent,
                    'message': _clip_text(message, 180),
                    'command': ' '.join(shlex.quote(x) for x in cmd),
                    'session_file': session_file or '',
                    'timestamp': now_ms(),
                },
            )
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            started = time.time()
            last_ping = 0.0
            while True:
                # Follow session stream and emit thinking/text/tool events.
                next_session_file = get_agent_session_file(agent)
                if next_session_file and next_session_file != session_file:
                    session_file = next_session_file
                    offset = 0
                    self._send_sse_event('meta', {'agent': agent, 'session_file': session_file, 'note': 'session_file_switched'})
                if session_file and os.path.isfile(session_file):
                    offset, events = read_agent_session_events_since(session_file, offset)
                    for e in events:
                        self._send_sse_event(
                            'chunk',
                            {
                                'agent': agent,
                                'ts': e.get('timestamp', ''),
                                'role': e.get('role', ''),
                                'kind': e.get('kind', 'text'),
                                'text': e.get('text', ''),
                            },
                        )

                # Emit process stdout for troubleshooting context.
                if proc and proc.stdout:
                    ready, _, _ = select.select([proc.stdout], [], [], 0)
                    while ready:
                        line = proc.stdout.readline()
                        if not line:
                            break
                        line = line.rstrip('\r\n')
                        if line:
                            self._send_sse_event('log', {'agent': agent, 'text': _clip_text(line, 360)})
                        ready, _, _ = select.select([proc.stdout], [], [], 0)

                if proc and proc.poll() is not None:
                    # Flush tail once process exits.
                    if proc.stdout:
                        for line in proc.stdout.readlines():
                            line = line.rstrip('\r\n')
                            if line:
                                self._send_sse_event('log', {'agent': agent, 'text': _clip_text(line, 360)})
                    if session_file and os.path.isfile(session_file):
                        for _ in range(4):
                            offset, events = read_agent_session_events_since(session_file, offset)
                            for e in events:
                                self._send_sse_event(
                                    'chunk',
                                    {
                                        'agent': agent,
                                        'ts': e.get('timestamp', ''),
                                        'role': e.get('role', ''),
                                        'kind': e.get('kind', 'text'),
                                        'text': e.get('text', ''),
                                    },
                                )
                            time.sleep(0.05)
                    self._send_sse_event(
                        'done',
                        {
                            'agent': agent,
                            'ok': proc.returncode == 0,
                            'returncode': proc.returncode,
                            'elapsedSec': round(time.time() - started, 2),
                        },
                    )
                    return

                now = time.time()
                if now - started > timeout_sec + 30:
                    if proc:
                        proc.kill()
                    self._send_sse_event(
                        'error',
                        {'agent': agent, 'error': 'chat_timeout', 'timeoutSec': timeout_sec},
                    )
                    self._send_sse_event(
                        'done',
                        {'agent': agent, 'ok': False, 'returncode': -9, 'elapsedSec': round(now - started, 2)},
                    )
                    return
                if now - last_ping > 12:
                    self._send_sse_ping()
                    last_ping = now
                time.sleep(0.2)
        except (BrokenPipeError, ConnectionResetError):
            if proc and proc.poll() is None:
                proc.kill()
            return
        except Exception as e:
            try:
                self._send_sse_event('error', {'agent': agent, 'error': str(e)})
                self._send_sse_event('done', {'agent': agent, 'ok': False, 'returncode': -1})
            except Exception:
                pass
        finally:
            if proc and proc.poll() is None:
                proc.kill()

    def do_HEAD(self):
        if self.path.startswith('/api/'):
            self._send(200, b'')
        else:
            self._send(200, b'', 'text/html; charset=utf-8')

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/status':
            data = build_status()
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/xhs-status':
            data = fetch_xhs_status()
            code = 200 if data.get('ok') else 502
            self._send(code, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/main-status':
            data = fetch_main_status()
            code = 200 if data.get('ok') else 502
            self._send(code, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/world-status':
            data = fetch_world_status()
            code = 200 if data.get('ok') else 502
            self._send(code, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/airdrop-status':
            data = fetch_airdrop_status()
            code = 200 if data.get('ok') else 502
            self._send(code, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/agent-errors':
            data = build_agent_error_report()
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/ai-analysis':
            qs = parse_qs(parsed.query)
            force = _to_bool((qs.get('force') or ['0'])[0], False)
            data = run_site_ai_analysis(force, {}, False)
            code = 200 if data.get('ok') else 502
            self._send(code, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/ai-analysis-test':
            data = run_site_ai_analysis(True, {}, True)
            code = 200 if data.get('ok') else 502
            self._send(code, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/agent-chat-stream':
            self._handle_agent_chat_stream(parsed)
            return
        if parsed.path == '/api/xhs-image':
            qs = parse_qs(parsed.query)
            img_path = (qs.get('path') or [''])[0]
            if not img_path:
                self._send(400, json.dumps({'error': 'missing_path'}).encode('utf-8'))
                return
            img = fetch_xhs_image(img_path)
            if not img.get('ok'):
                self._send(int(img.get('status') or 502), b'', str(img.get('ctype') or 'application/octet-stream'))
                return
            self._send(200, img.get('content') or b'', str(img.get('ctype') or 'application/octet-stream'))
            return
        if parsed.path == '/api/ai-decision':
            out = run_ai_decision_once()
            self._send(200, json.dumps(out, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/mode':
            data = {'mode': load_mode()}
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/account':
            qs = parse_qs(parsed.query)
            m = (qs.get('mode') or [load_mode()])[0]
            data = get_okx_account('live' if m == 'live' else 'paper')
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/backtest-latest':
            data = read_json(BACKTEST_CACHE_FILE, {})
            if not isinstance(data, dict) or not data:
                data = {'ok': False, 'error': 'no_backtest_cache'}
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/claw402/catalog':
            data = _claw402_catalog_payload()
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/claw402/wallet':
            data = _claw402_wallet_payload()
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/claw402/usage':
            data = _claw402_usage_snapshot()
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/liquidation-heatmap-x402':
            qs = parse_qs(parsed.query)
            symbol = re.sub(r'[^A-Z0-9_-]', '', (qs.get('symbol') or ['BTCUSDT'])[0].upper()) or 'BTCUSDT'
            exchange = re.sub(r'[^A-Za-z0-9_-]', '', (qs.get('exchange') or ['Binance'])[0]) or 'Binance'
            interval = (qs.get('interval') or ['1d'])[0]
            if interval not in {'15m', '30m', '1h', '2h', '4h', '6h', '12h', '1d', '3d', '1w'}:
                interval = '1d'
            try:
                wallet_key = os.getenv('WALLET_PRIVATE_KEY', '').strip()
                if not wallet_key:
                    cfg = '/root/.config/systemd/user/openclaw-gateway.service.d/40-claw402.conf'
                    if os.path.exists(cfg):
                        with open(cfg, 'r', encoding='utf-8') as f:
                            for line in f:
                                line = line.strip()
                                if line.startswith('Environment=WALLET_PRIVATE_KEY='):
                                    wallet_key = line.split('=', 2)[2].strip().strip('"').strip("'")
                                    break
                if not wallet_key:
                    self._send(200, json.dumps({'ok': False, 'error': 'wallet_private_key_missing'}).encode('utf-8'))
                    return

                script_path = '/root/.openclaw/workspace/skills/claw402/scripts/query.mjs'
                if not os.path.exists(script_path):
                    self._send(200, json.dumps({'ok': False, 'error': 'claw402_script_missing', 'path': script_path}, ensure_ascii=False).encode('utf-8'))
                    return

                env = os.environ.copy()
                env['WALLET_PRIVATE_KEY'] = wallet_key
                cmd = ['node', script_path, '/api/v1/coinank/liquidation/heat-map', f'exchange={exchange}', f'symbol={symbol}', f'interval={interval}']
                cp = subprocess.run(cmd, capture_output=True, text=True, timeout=60, cwd=os.path.dirname(script_path), env=env)
                if cp.returncode != 0:
                    detail = (cp.stderr or cp.stdout or 'x402_call_failed').strip()
                    self._send(200, json.dumps({'ok': False, 'error': 'x402_call_failed', 'detail': detail[:600]}, ensure_ascii=False).encode('utf-8'))
                    return

                out = (cp.stdout or '').strip()
                if not out:
                    self._send(200, json.dumps({'ok': False, 'error': 'x402_empty_stdout'}).encode('utf-8'))
                    return
                pos = out.find('{')
                if pos > 0:
                    out = out[pos:]
                payload = json.loads(out)

                core = ((payload.get('data') or {}).get('data') or {})
                liq = (core.get('liqHeatMap') or {})
                entries = liq.get('data') or []
                chart_times = liq.get('chartTimeArray') or []
                prices = liq.get('priceArray') or []
                if not entries or not chart_times or not prices:
                    self._send(200, json.dumps({'ok': False, 'error': 'x402_empty_data', 'raw_status': payload.get('status')}, ensure_ascii=False).encode('utf-8'))
                    return

                t_len = len(chart_times)
                p_len = len(prices)
                t_bins = max(1, min(120, t_len))
                p_bins = max(1, min(90, p_len))
                t_step = max(1, math.ceil(t_len / t_bins))
                p_step = max(1, math.ceil(p_len / p_bins))

                buckets = {}
                price_totals = {}
                nonzero_points = 0
                for rec in entries:
                    if not isinstance(rec, list) or len(rec) < 3:
                        continue
                    try:
                        xi = int(float(rec[0]))
                        yi = int(float(rec[1]))
                        vv = float(rec[2])
                    except Exception:
                        continue
                    if vv <= 0 or xi < 0 or yi < 0:
                        continue
                    nonzero_points += 1
                    bx = min(t_bins - 1, xi // t_step)
                    by = min(p_bins - 1, yi // p_step)
                    buckets[(bx, by)] = buckets.get((bx, by), 0.0) + vv
                    if yi < p_len:
                        try:
                            py = float(prices[yi])
                            price_totals[py] = price_totals.get(py, 0.0) + vv
                        except Exception:
                            pass

                points = [[k[0], k[1], round(v, 3)] for k, v in sorted(buckets.items(), key=lambda x: (x[0][0], x[0][1])) if v > 0]
                max_bucket = max((x[2] for x in points), default=0.0)

                current_price = 0.0
                status = read_json(STATUS_FILE, {})
                spot = status.get('spot', {}) if isinstance(status, dict) else {}
                if isinstance(spot, dict) and spot.get('price'):
                    try:
                        current_price = float(spot.get('price') or 0)
                    except Exception:
                        current_price = 0.0
                if not current_price and isinstance(status, dict):
                    ld = status.get('last_decision', {})
                    if isinstance(ld, dict) and ld.get('price'):
                        try:
                            current_price = float(ld.get('price') or 0)
                        except Exception:
                            current_price = 0.0
                if not current_price:
                    try:
                        inst = symbol.upper().replace('_', '').replace('-', '')
                        okx_inst = f"{inst[:-4]}-USDT-SWAP" if inst.endswith('USDT') and len(inst) > 4 else ''
                        r = requests.get('https://www.okx.com/api/v5/market/tickers?instType=SWAP', timeout=6)
                        d = r.json()
                        if d.get('code') == '0':
                            for t in d.get('data', []):
                                if okx_inst and t.get('instId') != okx_inst:
                                    continue
                                if okx_inst or t.get('instId') == 'BTC-USDT-SWAP':
                                    current_price = float(t.get('last', 0) or 0)
                                    break
                    except Exception:
                        pass

                candles: List[Dict[str, Any]] = []
                kline_limit = max(160, min(360, t_bins * 2))
                try:
                    rk = requests.get(
                        'https://api.binance.com/api/v3/klines',
                        params={
                            'symbol': symbol,
                            'interval': interval,
                            'limit': int(kline_limit),
                        },
                        timeout=8,
                    )
                    kd = rk.json()
                    if rk.status_code == 200 and isinstance(kd, list):
                        for row in kd:
                            if not isinstance(row, list) or len(row) < 6:
                                continue
                            try:
                                ts = int(float(row[0]))
                                op = float(row[1])
                                hi = float(row[2])
                                lo = float(row[3])
                                cl = float(row[4])
                            except Exception:
                                continue
                            candles.append(
                                {
                                    'ts': ts,
                                    'open': round(op, 8),
                                    'high': round(hi, 8),
                                    'low': round(lo, 8),
                                    'close': round(cl, 8),
                                }
                            )
                except Exception:
                    pass

                if not candles:
                    try:
                        inst = symbol.upper().replace('_', '').replace('-', '')
                        okx_inst = f"{inst[:-4]}-USDT-SWAP" if inst.endswith('USDT') and len(inst) > 4 else 'BTC-USDT-SWAP'
                        okx_bar_map = {
                            '15m': '15m',
                            '30m': '30m',
                            '1h': '1H',
                            '2h': '2H',
                            '4h': '4H',
                            '6h': '6H',
                            '12h': '12H',
                            '1d': '1D',
                            '3d': '3D',
                            '1w': '1W',
                        }
                        okx_bar = okx_bar_map.get(interval, '1D')
                        r2 = requests.get(
                            'https://www.okx.com/api/v5/market/history-candles',
                            params={
                                'instId': okx_inst,
                                'bar': okx_bar,
                                'limit': str(int(min(300, kline_limit))),
                            },
                            timeout=8,
                        )
                        d2 = r2.json()
                        rows = d2.get('data', []) if isinstance(d2, dict) else []
                        if isinstance(rows, list):
                            for row in reversed(rows):
                                if not isinstance(row, list) or len(row) < 5:
                                    continue
                                try:
                                    ts = int(float(row[0]))
                                    op = float(row[1])
                                    hi = float(row[2])
                                    lo = float(row[3])
                                    cl = float(row[4])
                                except Exception:
                                    continue
                                candles.append(
                                    {
                                        'ts': ts,
                                        'open': round(op, 8),
                                        'high': round(hi, 8),
                                        'low': round(lo, 8),
                                        'close': round(cl, 8),
                                    }
                                )
                    except Exception:
                        pass

                if not candles:
                    try:
                        by_t: Dict[int, Dict[str, float]] = {}
                        for rec in entries:
                            if not isinstance(rec, list) or len(rec) < 3:
                                continue
                            try:
                                xi = int(float(rec[0]))
                                yi = int(float(rec[1]))
                                vv = float(rec[2])
                                if xi < 0 or yi < 0 or vv <= 0 or yi >= len(prices):
                                    continue
                                py = float(prices[yi])
                            except Exception:
                                continue
                            row = by_t.get(xi)
                            if row is None:
                                row = {'sum': 0.0, 'pv': 0.0, 'hi': py, 'lo': py}
                                by_t[xi] = row
                            row['sum'] += vv
                            row['pv'] += py * vv
                            row['hi'] = max(float(row.get('hi') or py), py)
                            row['lo'] = min(float(row.get('lo') or py), py)
                        prev_close = float(current_price or 0)
                        if prev_close <= 0 and prices:
                            try:
                                prev_close = float(prices[len(prices) // 2])
                            except Exception:
                                prev_close = 0.0
                        for xi in sorted(by_t.keys()):
                            row = by_t.get(xi) or {}
                            sm = float(row.get('sum') or 0)
                            if sm <= 0:
                                continue
                            cl = float(row.get('pv') or 0) / sm
                            op = prev_close if prev_close > 0 else cl
                            hi = max(float(row.get('hi') or cl), op, cl)
                            lo = min(float(row.get('lo') or cl), op, cl)
                            ts = int(chart_times[min(max(0, xi), len(chart_times) - 1)]) if chart_times else int(time.time() * 1000)
                            candles.append(
                                {
                                    'ts': ts,
                                    'open': round(op, 8),
                                    'high': round(hi, 8),
                                    'low': round(lo, 8),
                                    'close': round(cl, 8),
                                }
                            )
                            prev_close = cl
                    except Exception:
                        candles = []

                if not current_price and candles:
                    try:
                        current_price = float((candles[-1] or {}).get('close') or 0)
                    except Exception:
                        current_price = 0.0

                top_levels = []
                for price, total in sorted(price_totals.items(), key=lambda x: x[1], reverse=True)[:10]:
                    dist = None
                    if current_price > 0:
                        dist = (price - current_price) / current_price * 100
                    top_levels.append({'price': round(price, 6), 'total_liq': round(total, 3), 'distance_pct': round(dist, 4) if isinstance(dist, float) else None})

                leverage_bands = {
                    'available': False,
                    'dominant_by_bin': ['all'] * p_bins,
                    'bin_scores': {'high': [0.0] * p_bins, 'mid': [0.0] * p_bins, 'low': [0.0] * p_bins},
                    'level_totals': {'high': 0.0, 'mid': 0.0, 'low': 0.0},
                }
                level_top_levels = {'high': [], 'mid': [], 'low': []}

                try:
                    cmd2 = ['node', script_path, '/api/v1/coinank/liquidation/liq-map', f'exchange={exchange}', f'symbol={symbol}', f'interval={interval}']
                    cp2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=60, cwd=os.path.dirname(script_path), env=env)
                    if cp2.returncode == 0 and (cp2.stdout or '').strip():
                        out2 = (cp2.stdout or '').strip()
                        pos2 = out2.find('{')
                        if pos2 > 0:
                            out2 = out2[pos2:]
                        payload2 = json.loads(out2)
                        lvl_core = ((payload2.get('data') or {}).get('data') or {})
                        lvl_prices = lvl_core.get('prices') or []
                        x25 = lvl_core.get('x25') or []
                        x30 = lvl_core.get('x30') or []
                        x40 = lvl_core.get('x40') or []
                        x50 = lvl_core.get('x50') or []
                        x60 = lvl_core.get('x60') or []
                        x70 = lvl_core.get('x70') or []
                        x80 = lvl_core.get('x80') or []
                        x90 = lvl_core.get('x90') or []
                        x100 = lvl_core.get('x100') or []
                        min_len = min(len(lvl_prices), len(x25), len(x30), len(x40), len(x50), len(x60), len(x70), len(x80), len(x90), len(x100))
                        if min_len > 0:
                            by_price: Dict[float, List[float]] = {}
                            for i in range(min_len):
                                try:
                                    price = float(lvl_prices[i])
                                    low_v = float(x25[i] or 0) + float(x30[i] or 0) + float(x40[i] or 0)
                                    mid_v = float(x50[i] or 0) + float(x60[i] or 0) + float(x70[i] or 0)
                                    high_v = float(x80[i] or 0) + float(x90[i] or 0) + float(x100[i] or 0)
                                except Exception:
                                    continue
                                key = round(price, 6)
                                prev = by_price.get(key)
                                if prev is None:
                                    by_price[key] = [high_v, mid_v, low_v]
                                else:
                                    prev[0] += high_v
                                    prev[1] += mid_v
                                    prev[2] += low_v

                            by_price_int: Dict[int, List[float]] = {}
                            for k, vals in by_price.items():
                                kk = int(round(k))
                                prev = by_price_int.get(kk)
                                if prev is None:
                                    by_price_int[kk] = [vals[0], vals[1], vals[2]]
                                else:
                                    prev[0] += vals[0]
                                    prev[1] += vals[1]
                                    prev[2] += vals[2]

                            def _scores_for_price(px: float) -> List[float]:
                                kf = round(float(px), 6)
                                row = by_price.get(kf)
                                if row is not None:
                                    return row
                                return by_price_int.get(int(round(float(px))), [0.0, 0.0, 0.0])

                            high_bins: List[float] = []
                            mid_bins: List[float] = []
                            low_bins: List[float] = []
                            dominant: List[str] = []
                            for b in range(p_bins):
                                st = b * p_step
                                ed = min(p_len, (b + 1) * p_step)
                                h_sum = 0.0
                                m_sum = 0.0
                                l_sum = 0.0
                                for yi in range(st, ed):
                                    if yi >= len(prices):
                                        continue
                                    try:
                                        pr = float(prices[yi])
                                    except Exception:
                                        continue
                                    hs, ms, ls = _scores_for_price(pr)
                                    h_sum += float(hs or 0)
                                    m_sum += float(ms or 0)
                                    l_sum += float(ls or 0)
                                high_bins.append(round(h_sum, 3))
                                mid_bins.append(round(m_sum, 3))
                                low_bins.append(round(l_sum, 3))
                                mx = max(h_sum, m_sum, l_sum)
                                if mx <= 0:
                                    dominant.append('all')
                                elif h_sum >= m_sum and h_sum >= l_sum:
                                    dominant.append('high')
                                elif m_sum >= h_sum and m_sum >= l_sum:
                                    dominant.append('mid')
                                else:
                                    dominant.append('low')

                            def _build_level_top(idx: int) -> List[Dict[str, Any]]:
                                rows: List[Dict[str, Any]] = []
                                for price, vals in by_price.items():
                                    total = float(vals[idx] or 0)
                                    if total <= 0:
                                        continue
                                    dist = None
                                    if current_price > 0:
                                        dist = (price - current_price) / current_price * 100
                                    rows.append({'price': round(price, 6), 'total_liq': round(total, 3), 'distance_pct': round(dist, 4) if isinstance(dist, float) else None})
                                rows.sort(key=lambda x: float(x.get('total_liq') or 0), reverse=True)
                                return rows[:12]

                            level_top_levels = {
                                'high': _build_level_top(0),
                                'mid': _build_level_top(1),
                                'low': _build_level_top(2),
                            }
                            leverage_bands = {
                                'available': True,
                                'dominant_by_bin': dominant,
                                'bin_scores': {'high': high_bins, 'mid': mid_bins, 'low': low_bins},
                                'level_totals': {
                                    'high': round(sum(high_bins), 3),
                                    'mid': round(sum(mid_bins), 3),
                                    'low': round(sum(low_bins), 3),
                                },
                            }
                        else:
                            leverage_bands['error'] = 'liq_map_empty'
                    elif cp2.returncode != 0:
                        leverage_bands['error'] = 'liq_map_call_failed'
                        leverage_bands['detail'] = (cp2.stderr or cp2.stdout or '')[:200]
                except Exception as le:
                    leverage_bands['error'] = f'liq_map_exception: {le}'[:220]

                result = {
                    'ok': True,
                    'provider': 'claw402',
                    'symbol': symbol,
                    'exchange': exchange,
                    'interval': interval,
                    'summary': {
                        'raw_point_count': len(entries),
                        'nonzero_points': nonzero_points,
                        'max_liq_value': float(liq.get('maxLiqValue') or 0),
                        'chart_interval': core.get('chartInterval'),
                        'current_price': current_price or None,
                    },
                    'heatmap': {
                        'time_bins': t_bins,
                        'price_bins': p_bins,
                        'points': points,
                        'max_value': max_bucket,
                        'price_min': float(min(prices)) if prices else None,
                        'price_max': float(max(prices)) if prices else None,
                        'start_ts': int(chart_times[0]) if chart_times else None,
                        'end_ts': int(chart_times[-1]) if chart_times else None,
                        'tick_size': core.get('tickSize'),
                    },
                    'top_levels': top_levels,
                    'leverage_bands': leverage_bands,
                    'level_top_levels': level_top_levels,
                    'candles': candles[-320:],
                }
                self._send(200, json.dumps(result, ensure_ascii=False).encode('utf-8'))
            except Exception as e:
                import traceback
                self._send(200, json.dumps({'ok': False, 'error': str(e), 'traceback': traceback.format_exc()}, ensure_ascii=False).encode('utf-8'))
            return

        if parsed.path == '/api/liquidation-heatmap':
            try:
                # 获取当前价格 - 从多个来源尝试
                status = read_json(STATUS_FILE, {})
                current_price = 0
                
                # 尝试从 spot 获取
                spot = status.get('spot', {})
                if isinstance(spot, dict) and spot.get('price'):
                    current_price = spot.get('price', 0)
                
                # 如果没有，从 last_decision 获取
                if not current_price:
                    last_decision = status.get('last_decision', {})
                    if isinstance(last_decision, dict):
                        current_price = last_decision.get('price', 0)
                
                # 如果还是没有，从 okx API 获取
                if not current_price:
                    try:
                        resp = requests.get('https://www.okx.com/api/v5/market/tickers?instType=SWAP', timeout=5)
                        data = resp.json()
                        if data.get('code') == '0':
                            for t in data.get('data', []):
                                if t.get('instId') == 'BTC-USDT-SWAP':
                                    current_price = float(t.get('last', 0))
                                    break
                    except:
                        pass
                
                if not current_price:
                    self._send(200, json.dumps({'ok': False, 'error': 'no_price_available'}).encode('utf-8'))
                    return
                
                # 调用清算热力图因子模块
                import sys
                sys.path.insert(0, '/root/.okx-paper')
                from liquidation_heatmap_factor import compute_liquidation_heatmap_factor
                
                result = compute_liquidation_heatmap_factor(current_price, 'BTCUSDT')
                result['ok'] = True
                result['current_price'] = current_price
                self._send(200, json.dumps(result, ensure_ascii=False).encode('utf-8'))
            except Exception as e:
                import traceback
                self._send(200, json.dumps({'ok': False, 'error': str(e), 'traceback': traceback.format_exc()}).encode('utf-8'))
            return
        if parsed.path == '/api/instances':
            data = {
                'ok': True,
                'instances': list_instance_public_meta(),
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/instances/summary':
            data = fetch_instances_summary()
            self._send(200, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        m_inst_status = re.match(r'^/api/instances/([A-Za-z0-9_-]+)/status$', parsed.path)
        if m_inst_status:
            iid = str(m_inst_status.group(1) or '').lower()
            data = fetch_instance_status(iid)
            err = str(data.get('error') or '')
            if err == 'instance_not_found':
                code = 404
            else:
                code = 200 if data.get('ok') else 502
            self._send(code, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return

        inst_route = _parse_instance_route(parsed.path)
        if inst_route:
            iid = str(inst_route.get('id') or '')
            tail = str(inst_route.get('tail') or '')
            if not get_instance_meta(iid):
                self._send(404, json.dumps({'error': 'instance_not_found', 'id': iid}).encode('utf-8'))
                return
            if not tail:
                page = render_instance_wrapper_html(iid)
                self._send(200, page.encode('utf-8'), 'text/html; charset=utf-8')
                return
            if tail == 'raw' or tail.startswith('raw/'):
                rel = _normalize_rel_path(tail[3:])
                if rel.startswith('/api/'):
                    prox = proxy_instance_api(iid, 'GET', rel, parsed.query, b'', self.headers)
                else:
                    prox = proxy_instance_raw_get(iid, rel, parsed.query, self.headers)
                self._send(int(prox.get('status') or 502), prox.get('content') or b'', str(prox.get('ctype') or 'application/octet-stream'))
                return
            if tail.startswith('api/'):
                rel = '/' + tail
                prox = proxy_instance_api(iid, 'GET', rel, parsed.query, b'', self.headers)
                self._send(int(prox.get('status') or 502), prox.get('content') or b'', str(prox.get('ctype') or 'application/octet-stream'))
                return
            self._send(404, json.dumps({'error': 'not_found'}).encode('utf-8'))
            return

        legacy_raw = _parse_legacy_raw_route(parsed.path)
        if legacy_raw:
            iid = str(legacy_raw.get('id') or '')
            rel = str(legacy_raw.get('rel_path') or '/')
            if rel.startswith('/api/'):
                prox = proxy_instance_api(iid, 'GET', rel, parsed.query, b'', self.headers)
            else:
                prox = proxy_instance_raw_get(iid, rel, parsed.query, self.headers)
            self._send(int(prox.get('status') or 502), prox.get('content') or b'', str(prox.get('ctype') or 'application/octet-stream'))
            return

        if parsed.path in {'/'}:
            self._send(200, render_portal_home_html().encode('utf-8'), 'text/html; charset=utf-8')
            return

        legacy_pages = {
            '/okx': 'okx',
            '/xhs': 'xhs',
            '/main': 'main',
            '/world': 'world',
            '/airdrop': 'airdrop',
        }
        normalized_path = parsed.path.rstrip('/') or '/'
        legacy_id = legacy_pages.get(normalized_path)
        if legacy_id:
            self._send(200, render_instance_wrapper_html(legacy_id).encode('utf-8'), 'text/html; charset=utf-8')
            return

        if parsed.path.startswith('/api/'):
            prox = proxy_world_api('GET', parsed.path, parsed.query, b'', self.headers)
            self._send(int(prox.get('status') or 502), prox.get('content') or b'', str(prox.get('ctype') or 'application/octet-stream'))
            return

        self._send(404, json.dumps({'error': 'not_found'}).encode('utf-8'))

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path in {'/api/ai-analysis', '/api/ai-analysis-test'}:
            size = int(self.headers.get('Content-Length', '0') or '0')
            raw = self.rfile.read(size) if size > 0 else b'{}'
            try:
                body = json.loads(raw.decode('utf-8'))
            except Exception:
                body = {}
            force = _to_bool((body or {}).get('force'), parsed.path == '/api/ai-analysis-test')
            opts = _extract_site_ai_options(body)
            test_model = parsed.path == '/api/ai-analysis-test'
            data = run_site_ai_analysis(force, opts, test_model)
            code = 200 if data.get('ok') else 502
            self._send(code, json.dumps(data, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/claw402/request':
            size = int(self.headers.get('Content-Length', '0') or '0')
            raw = self.rfile.read(size) if size > 0 else b'{}'
            try:
                body = json.loads(raw.decode('utf-8'))
            except Exception:
                self._send(400, json.dumps({'ok': False, 'error': 'invalid_json'}).encode('utf-8'))
                return
            path = str((body or {}).get('path') or '')
            method = str((body or {}).get('method') or 'GET')
            params = (body or {}).get('params') if isinstance((body or {}).get('params'), dict) else {}
            post_body = (body or {}).get('post_body')
            out = _claw402_run_request(path, method, params, post_body)
            self._send(200, json.dumps(out, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/ai-decision':
            out = run_ai_decision_once()
            self._send(200, json.dumps(out, ensure_ascii=False).encode('utf-8'))
            return
        if parsed.path == '/api/mode':
            size = int(self.headers.get('Content-Length', '0') or '0')
            raw = self.rfile.read(size) if size > 0 else b'{}'
            try:
                body = json.loads(raw.decode('utf-8'))
            except Exception:
                body = {}
            mode = str(body.get('mode', 'paper')).lower()
            if mode not in {'paper', 'live'}:
                self._send(400, json.dumps({'error': 'invalid_mode'}).encode('utf-8'))
                return
            save_mode(mode)
            self._send(200, json.dumps({'ok': True, 'mode': mode}).encode('utf-8'))
            return
        if parsed.path == '/api/backtest/run':
            size = int(self.headers.get('Content-Length', '0') or '0')
            raw = self.rfile.read(size) if size > 0 else b'{}'
            try:
                body = json.loads(raw.decode('utf-8'))
            except Exception:
                body = {}
            out = run_backtest_real_kline(body)
            code = 200 if out.get('ok') else 502
            self._send(code, json.dumps(out, ensure_ascii=False).encode('utf-8'))
            return
        inst_route = _parse_instance_route(parsed.path)
        if inst_route:
            iid = str(inst_route.get('id') or '')
            tail = str(inst_route.get('tail') or '')
            if not get_instance_meta(iid):
                self._send(404, json.dumps({'error': 'instance_not_found', 'id': iid}).encode('utf-8'))
                return
            size = int(self.headers.get('Content-Length', '0') or '0')
            raw = self.rfile.read(size) if size > 0 else b''
            if tail == 'raw' or tail.startswith('raw/'):
                rel = _normalize_rel_path(tail[3:])
                if not rel.startswith('/api/'):
                    self._send(405, json.dumps({'error': 'method_not_allowed'}).encode('utf-8'))
                    return
                prox = proxy_instance_api(iid, 'POST', rel, parsed.query, raw, self.headers)
                self._send(int(prox.get('status') or 502), prox.get('content') or b'', str(prox.get('ctype') or 'application/octet-stream'))
                return
            if tail.startswith('api/'):
                rel = '/' + tail
                prox = proxy_instance_api(iid, 'POST', rel, parsed.query, raw, self.headers)
                self._send(int(prox.get('status') or 502), prox.get('content') or b'', str(prox.get('ctype') or 'application/octet-stream'))
                return
            self._send(405, json.dumps({'error': 'method_not_allowed'}).encode('utf-8'))
            return

        legacy_raw = _parse_legacy_raw_route(parsed.path)
        if legacy_raw:
            rel = str(legacy_raw.get('rel_path') or '/')
            if not rel.startswith('/api/'):
                self._send(405, json.dumps({'error': 'method_not_allowed'}).encode('utf-8'))
                return
            size = int(self.headers.get('Content-Length', '0') or '0')
            raw = self.rfile.read(size) if size > 0 else b''
            prox = proxy_instance_api(str(legacy_raw.get('id') or ''), 'POST', rel, parsed.query, raw, self.headers)
            self._send(int(prox.get('status') or 502), prox.get('content') or b'', str(prox.get('ctype') or 'application/octet-stream'))
            return

        if parsed.path.startswith('/api/'):
            size = int(self.headers.get('Content-Length', '0') or '0')
            raw = self.rfile.read(size) if size > 0 else b''
            prox = proxy_world_api('POST', parsed.path, parsed.query, raw, self.headers)
            self._send(int(prox.get('status') or 502), prox.get('content') or b'', str(prox.get('ctype') or 'application/octet-stream'))
            return

        self._send(404, json.dumps({'error': 'not_found'}).encode('utf-8'))


if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(MODE_FILE):
        save_mode('paper')
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f'coevo dashboard listening on {HOST}:{PORT}', flush=True)
    server.serve_forever()
