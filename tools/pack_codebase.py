"""
tools/pack_codebase.py - Packs key codebase files into full_codebase.md
"""
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TARGET_FILES = [
    'README.md',
    'batch_writer.py',
    'config.py',
    'docs/explanations.md',
    'korean_engine.py',
    'main.py',
    'matrix_thresholds.json',
    'models.py',
    'pipeline/__init__.py',
    'pipeline/evaluator.py',
    'pipeline/pool_state.py',
    'pipeline/profile.py',
    'reports/daily/report_2026-10-18.json',
    'reports/daily/report_2026-10-18.md',
    'runtime_overrides.json',
    'screening/__init__.py',
    'screening/metrics.py',
    'screening/screener.py',
    'tests/test_adaptive_stops_and_fixes.py',
    'tests/test_pipeline_e2e.py',
    'tests/test_venues_contract_size.py',
    'tests/test_venues_streams_resilience.py',
    'tools/__init__.py',
    'tools/analyze_signals.py',
    'tools/audit_venues.py',
    'tools/calibrate_thresholds.py',
    'tools/full_daily_report.py',
    'tools/manage_pool.py',
    'tools/run_daily_pipeline.py',
    'tools/screen_universe.py',
    'tools/validate_data.py',
    'tools/verify_all.py',
    'trading_profile.json',
    'universe_live.json',
    'venues/__init__.py',
    'venues/asterdex.py',
    'venues/base.py',
    'venues/binance.py',
    'venues/bingx.py',
    'venues/bybit.py',
    'venues/gateio.py',
    'venues/htx.py',
    'venues/hyperliquid.py',
    'venues/kucoin.py',
    'venues/manager.py',
    'venues/mexc.py',
    'venues/okx.py',
    'venues/pionex.py',
    'venues/upbit.py',
    'web/__init__.py',
    'web/reports_manager.py',
    'web/server.py',
    'web/views.py'
]


def pack() -> None:
    out_lines = ['# Full Codebase: KoreanSunrise\n']
    for rel in TARGET_FILES:
        fp = os.path.join(PROJECT_ROOT, rel.replace('/', os.sep))
        if not os.path.exists(fp):
            continue
        ext = os.path.splitext(rel)[1].lstrip('.')
        if ext == 'py':
            lang = 'python'
        elif ext == 'json':
            lang = 'json'
        elif ext == 'md':
            lang = 'md'
        else:
            lang = ''
        out_lines.append(f'## {rel}\n')
        out_lines.append(f'```{lang}')
        with open(fp, 'r', encoding='utf-8', errors='replace') as f:
            out_lines.append(f.read().rstrip())
        out_lines.append('```\n')

    out_file = os.path.join(PROJECT_ROOT, 'full_codebase.md')
    with open(out_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(out_lines))
    print(f"[OK] Generated {out_file} ({len(out_lines)} blocks)")


if __name__ == '__main__':
    pack()
