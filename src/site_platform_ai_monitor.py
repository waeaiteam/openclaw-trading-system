#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests


def now_ms() -> int:
    return int(time.time() * 1000)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def add_trace(trace: List[Dict[str, Any]], step: str, status: str, detail: str) -> None:
    trace.append(
        {
            'time': utc_iso(),
            'step': str(step or ''),
            'status': str(status or ''),
            'detail': str(detail or '')[:400],
        }
    )


def fetch_json(base_url: str, path: str, timeout_sec: int) -> Tuple[Dict[str, Any], str]:
    url = base_url.rstrip('/') + path
    try:
        resp = requests.get(url, timeout=timeout_sec)
        if resp.status_code >= 400:
            return {}, f'http_{resp.status_code}'
        data = resp.json()
        if isinstance(data, dict):
            return data, ''
        return {}, 'invalid_json_type'
    except Exception as e:
        return {}, str(e)


def compact_snapshot(base_url: str, timeout_sec: int) -> Dict[str, Any]:
    summary, summary_err = fetch_json(base_url, '/api/instances/summary', timeout_sec)
    errors, errors_err = fetch_json(base_url, '/api/agent-errors', timeout_sec)
    okx, okx_err = fetch_json(base_url, '/api/status', timeout_sec)
    xhs, xhs_err = fetch_json(base_url, '/api/xhs-status', timeout_sec)
    main, main_err = fetch_json(base_url, '/api/main-status', timeout_sec)
    world, world_err = fetch_json(base_url, '/api/world-status', timeout_sec)
    airdrop, airdrop_err = fetch_json(base_url, '/api/airdrop-status', timeout_sec)

    rows: List[Dict[str, Any]] = []
    for one in (summary.get('instances') or []) if isinstance(summary, dict) else []:
        if not isinstance(one, dict):
            continue
        rows.append(
            {
                'id': one.get('id'),
                'status_level': one.get('status_level'),
                'status_text_zh': one.get('status_text_zh'),
                'summary_zh': one.get('summary_zh'),
                'latency_ms': one.get('latency_ms'),
                'error': one.get('error') or '',
            }
        )

    agent_rows: List[Dict[str, Any]] = []
    agents = (errors.get('agents') or {}) if isinstance(errors, dict) else {}
    if isinstance(agents, dict):
        for aid, row in agents.items():
            if not isinstance(row, dict):
                continue
            session_err = int(row.get('session_error_count') or 0)
            gateway_err = int(row.get('gateway_error_count') or 0)
            agent_rows.append(
                {
                    'id': aid,
                    'session_error_count': session_err,
                    'gateway_error_count': gateway_err,
                    'total_error_count': session_err + gateway_err,
                }
            )
    agent_rows.sort(key=lambda x: int(x.get('total_error_count') or 0), reverse=True)

    snapshot = {
        'generated_at': utc_iso(),
        'sources': {
            'instances_summary': {'ok': bool(summary), 'error': summary_err},
            'agent_errors': {'ok': bool(errors), 'error': errors_err},
            'okx_status': {'ok': bool(okx), 'error': okx_err},
            'xhs_status': {'ok': bool(xhs), 'error': xhs_err},
            'main_status': {'ok': bool(main), 'error': main_err},
            'world_status': {'ok': bool(world), 'error': world_err},
            'airdrop_status': {'ok': bool(airdrop), 'error': airdrop_err},
        },
        'instance_counts': (summary.get('counts') or {}) if isinstance(summary, dict) else {},
        'instances': rows,
        'agent_errors': agent_rows,
        'okx': {
            'mode': okx.get('mode'),
            'trader_status': okx.get('trader_status'),
            'processing_state': okx.get('processing_state'),
            'active_strategy': okx.get('active_strategy'),
            'signal_score': ((okx.get('signal') or {}).get('score') if isinstance(okx, dict) else None),
        },
        'xhs': {
            'health': ((xhs.get('health') or {}).get('status') if isinstance(xhs, dict) else None),
            'today_publish': ((xhs.get('publish') or {}).get('today_count') if isinstance(xhs, dict) else None),
            'success_publish': ((xhs.get('publish') or {}).get('success_count') if isinstance(xhs, dict) else None),
            'next_run': ((xhs.get('schedule') or {}).get('next_run_time') if isinstance(xhs, dict) else None),
        },
        'main': {
            'health': ((main.get('health') or {}).get('status') if isinstance(main, dict) else None),
            'run_state': ((main.get('runtime') or {}).get('run_state') if isinstance(main, dict) else None),
            'failed_main': ((main.get('queue') or {}).get('failed_count_main') if isinstance(main, dict) else None),
        },
        'world': {
            'status': ((world.get('health') or {}).get('status') if isinstance(world, dict) else None),
            'title': (world.get('title') if isinstance(world, dict) else None),
        },
        'airdrop': {
            'status': ((airdrop.get('health') or {}).get('status') if isinstance(airdrop, dict) else None),
            'wallet_count': ((airdrop.get('stats') or {}).get('wallet_count') if isinstance(airdrop, dict) else None),
            'project_count': ((airdrop.get('stats') or {}).get('project_count') if isinstance(airdrop, dict) else None),
            'pending_tasks': ((airdrop.get('stats') or {}).get('pending_tasks') if isinstance(airdrop, dict) else None),
        },
    }
    return snapshot


def extract_response_text(payload: Dict[str, Any]) -> str:
    out_text = payload.get('output_text')
    if isinstance(out_text, str) and out_text.strip():
        return out_text.strip()

    rows: List[str] = []
    for item in payload.get('output') or []:
        if not isinstance(item, dict):
            continue
        for content in item.get('content') or []:
            if not isinstance(content, dict):
                continue
            ctype = str(content.get('type') or '')
            if ctype in {'output_text', 'text', 'input_text'}:
                txt = str(content.get('text') or '').strip()
                if txt:
                    rows.append(txt)
    return '\n'.join(rows).strip()


def resolve_runtime_config() -> Dict[str, str]:
    api_key = str(os.getenv('SITE_AI_API_KEY') or os.getenv('OPENAI_API_KEY') or '').strip()
    model = str(os.getenv('SITE_AI_MODEL') or 'gpt-4.1-mini').strip()
    base = str(os.getenv('SITE_AI_API_BASE') or 'https://api.openai.com/v1').rstrip('/')
    return {
        'api_key': api_key,
        'model': model,
        'base': base,
    }


def _call_openai_response(api_key: str, base: str, payload: Dict[str, Any], timeout_sec: int = 90) -> Tuple[Dict[str, Any], str]:
    endpoint = base.rstrip('/') + '/responses'
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    }
    try:
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=timeout_sec)
    except Exception as e:
        return {}, f'openai_request_failed: {e}'
    if resp.status_code >= 400:
        return {}, f'openai_http_{resp.status_code}: {resp.text[:600]}'
    try:
        body = resp.json()
    except Exception:
        return {}, f'openai_invalid_json: {resp.text[:600]}'
    if not isinstance(body, dict):
        return {}, 'openai_invalid_json_type'
    return body, ''


def call_openai_model_test(runtime: Dict[str, str]) -> Dict[str, Any]:
    api_key = str(runtime.get('api_key') or '').strip()
    if not api_key:
        return {'ok': False, 'error': 'missing_openai_api_key'}

    model = str(runtime.get('model') or 'gpt-4.1-mini').strip()
    base = str(runtime.get('base') or 'https://api.openai.com/v1').rstrip('/')
    payload = {
        'model': model,
        'input': [
            {'role': 'system', 'content': [{'type': 'input_text', 'text': 'You are a concise assistant.'}]},
            {'role': 'user', 'content': [{'type': 'input_text', 'text': 'Reply exactly with: MODEL_OK'}]},
        ],
        'temperature': 0,
        'max_output_tokens': 80,
    }
    body, err = _call_openai_response(api_key, base, payload, 45)
    if err:
        return {'ok': False, 'error': err}
    text = extract_response_text(body)
    usage = body.get('usage') if isinstance(body.get('usage'), dict) else {}
    return {
        'ok': True,
        'source': 'openai_direct',
        'model': model,
        'analysis_text': f'妯″瀷杩為€氭垚鍔燂紝杩斿洖: {text or "MODEL_OK"}',
        'test_response_text': text or 'MODEL_OK',
        'usage': {
            'input_tokens': int(usage.get('input_tokens') or 0),
            'output_tokens': int(usage.get('output_tokens') or 0),
            'total_tokens': int(usage.get('total_tokens') or 0),
        },
    }


def call_openai_analysis(snapshot: Dict[str, Any], runtime: Dict[str, str]) -> Dict[str, Any]:
    api_key = str(runtime.get('api_key') or '').strip()
    if not api_key:
        return {'ok': False, 'error': 'missing_openai_api_key'}

    model = str(runtime.get('model') or 'gpt-4.1-mini').strip()
    base = str(runtime.get('base') or 'https://api.openai.com/v1').rstrip('/')

    system_prompt = (
        '\u4f60\u662f\u8d44\u6df1\u7f51\u7ad9\u8fd0\u7ef4\u4e0eSRE\u987e\u95ee\u3002'
        '\u8bf7\u53ea\u57fa\u4e8e\u8f93\u5165\u5feb\u7167\u8f93\u51fa\u4e2d\u6587\u8bca\u65ad\uff0c\u4e0d\u8981\u7f16\u9020\u6570\u636e\u3002'
        '\u8f93\u51fa\u5fc5\u987b\u5305\u542b\u56db\u6bb5\uff1a'
        '\u3010\u6570\u636e\u89c2\u5bdf\u3011'
        '\u3010\u5206\u6790\u8fc7\u7a0b\uff08\u53ef\u5c55\u793a\u7248\uff09\u3011'
        '\u3010\u4f18\u5316\u5efa\u8bae\uff08\u6309\u4f18\u5148\u7ea7\uff09\u3011'
        '\u301030\u5206\u949f\u52a8\u4f5c\u6e05\u5355\u3011'
    )
    user_prompt = (
        '\u8bf7\u5206\u6790\u8fd9\u4e2a\u5e73\u53f0\u5f53\u524d\u72b6\u6001\uff0c\u5e76\u8f93\u51fa\u7ed3\u6784\u5316\u4e2d\u6587\u5efa\u8bae\u3002\n'
        '\u8981\u6c42\uff1a\n'
        '1) \u7ed3\u8bba\u7b80\u6d01\u53ef\u6267\u884c\uff1b\n'
        '2) \u95ee\u9898\u70b9\u4e0d\u8d85\u8fc75\u6761\uff1b\n'
        '3) \u4f18\u5316\u5efa\u8bae\u4e0d\u8d85\u8fc75\u6761\uff0c\u5e76\u6807\u6ce8\u4f18\u5148\u7ea7\uff1b\n'
        '4) 30\u5206\u949f\u52a8\u4f5c\u4e0d\u8d85\u8fc74\u6761\uff1b\n'
        '5) \u5982\u679c\u67d0\u9879\u7f3a\u5c11\u6570\u636e\uff0c\u660e\u786e\u5199\u201c\u672a\u89c2\u6d4b\u5230\u201d\u3002\n\n'
        '\u5e73\u53f0\u5feb\u7167(JSON):\n'
        + json.dumps(snapshot, ensure_ascii=False, indent=2)
    )

    req_payload = {
        'model': model,
        'input': [
            {'role': 'system', 'content': [{'type': 'input_text', 'text': system_prompt}]},
            {'role': 'user', 'content': [{'type': 'input_text', 'text': user_prompt}]},
        ],
        'temperature': 0.2,
        'max_output_tokens': 1200,
    }
    body, err = _call_openai_response(api_key, base, req_payload, 90)
    if err:
        return {'ok': False, 'error': err}

    text = extract_response_text(body)
    if not text:
        return {'ok': False, 'error': 'openai_empty_output', 'detail': json.dumps(body, ensure_ascii=False)[:600]}

    usage = body.get('usage') if isinstance(body.get('usage'), dict) else {}
    return {
        'ok': True,
        'source': 'openai_direct',
        'model': model,
        'analysis_text': text,
        'usage': {
            'input_tokens': int(usage.get('input_tokens') or 0),
            'output_tokens': int(usage.get('output_tokens') or 0),
            'total_tokens': int(usage.get('total_tokens') or 0),
        },
    }

def main() -> int:
    parser = argparse.ArgumentParser(description='Platform status monitor + direct AI analysis.')
    parser.add_argument('--base-url', default='http://127.0.0.1:18091', help='dashboard base url')
    parser.add_argument('--timeout', type=int, default=12, help='per-request timeout')
    parser.add_argument('--test-model', action='store_true', help='test model connectivity only')
    parser.add_argument('--pretty', action='store_true', help='pretty print output json')
    args = parser.parse_args()

    runtime = resolve_runtime_config()
    snapshot: Dict[str, Any] = {}
    trace: List[Dict[str, Any]] = []
    key_state = 'provided' if runtime.get('api_key') else 'missing'
    add_trace(trace, 'runtime_config', 'ok', f"model={runtime.get('model')} base={runtime.get('base')} key={key_state}")
    if not args.test_model:
        t0 = time.time()
        snapshot = compact_snapshot(args.base_url, max(3, min(args.timeout, 60)))
        elapsed = round(time.time() - t0, 3)
        counts = snapshot.get('instance_counts') if isinstance(snapshot, dict) else {}
        add_trace(trace, 'collect_snapshot', 'ok', f'elapsed={elapsed}s counts={counts}')
        down_ids = []
        degraded_ids = []
        for row in snapshot.get('instances') or []:
            if not isinstance(row, dict):
                continue
            sid = str(row.get('id') or '')
            lvl = str(row.get('status_level') or '')
            if lvl == 'down':
                down_ids.append(sid)
            elif lvl == 'degraded':
                degraded_ids.append(sid)
        if down_ids:
            add_trace(trace, 'instance_health', 'warn', 'down=' + ','.join(down_ids[:10]))
        if degraded_ids:
            add_trace(trace, 'instance_health', 'warn', 'degraded=' + ','.join(degraded_ids[:10]))
        add_trace(trace, 'call_model', 'running', 'requesting AI diagnosis')
        ai = call_openai_analysis(snapshot, runtime)
        if ai.get('ok'):
            usage = ai.get('usage') if isinstance(ai.get('usage'), dict) else {}
            add_trace(trace, 'call_model', 'ok', f"tokens={usage.get('total_tokens')}")
        else:
            add_trace(trace, 'call_model', 'error', str(ai.get('error') or 'ai_call_failed'))
    else:
        add_trace(trace, 'test_model', 'running', 'checking model connectivity')
        ai = call_openai_model_test(runtime)
        if ai.get('ok'):
            usage = ai.get('usage') if isinstance(ai.get('usage'), dict) else {}
            add_trace(trace, 'test_model', 'ok', f"tokens={usage.get('total_tokens')}")
        else:
            add_trace(trace, 'test_model', 'error', str(ai.get('error') or 'test_failed'))
    out = {
        'ok': bool(ai.get('ok')),
        'source': ai.get('source') or 'openai_direct',
        'model': ai.get('model') or '',
        'mode': 'test_model' if args.test_model else 'analysis',
        'analysis_text': ai.get('analysis_text') or '',
        'test_response_text': ai.get('test_response_text') or '',
        'usage': ai.get('usage') or {},
        'error': ai.get('error') or '',
        'detail': ai.get('detail') or '',
        'snapshot': snapshot,
        'trace': trace,
        'updated_at': utc_iso(),
        'updated_at_ms': now_ms(),
    }

    if args.pretty:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print(json.dumps(out, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())

