"""
tools/clean_window_report.py — Analyzes only the clean window after fixes (22:19 - 00:03).
"""
import gzip
import json
import os
import sys

def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
    files = [
        r'data/signals\2026-10-20\sunrise_signals_20261020_194947_0001.jsonl.gz',
        r'data/signals\2026-10-20\sunrise_signals_20261020_201948_0002.jsonl.gz',
        r'data/signals\2026-10-20\sunrise_signals_20261020_204948_0003.jsonl.gz'
    ]

    strats = {}
    asterdex_trades = {}
    venues = {}
    maker_reasons = {}
    fade_reasons = {}

    for f in files:
        if not os.path.exists(f):
            continue
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip():
                    continue
                rec = json.loads(line)
                st = rec.get('strategy', 'lead_lag')
                ex = rec.get('follower_exchange', 'unknown')
                pnl_bps = float(rec.get('net_pnl_bps', 0.0) or 0.0)
                reason = rec.get('close_reason', 'unknown')
                
                # Per strategy
                if st not in strats:
                    strats[st] = {'n': 0, 'wins': 0, 'pnl_bps': 0.0, 'filled': 0, 'usd_pnl': 0.0}
                strats[st]['n'] += 1
                if pnl_bps > 0:
                    strats[st]['wins'] += 1
                strats[st]['pnl_bps'] += pnl_bps
                strats[st]['usd_pnl'] += (pnl_bps / 10000.0) * 10.0
                if rec.get('maker_filled'):
                    strats[st]['filled'] += 1

                if st == 'maker_void':
                    maker_reasons[reason] = maker_reasons.get(reason, 0) + 1
                elif st == 'fade_fakeout':
                    fade_reasons[reason] = fade_reasons.get(reason, 0) + 1
                
                # Per venue
                if ex not in venues:
                    venues[ex] = {'n': 0, 'pnl_bps': 0.0, 'wins': 0, 'usd_pnl': 0.0}
                venues[ex]['n'] += 1
                if pnl_bps > 0:
                    venues[ex]['wins'] += 1
                venues[ex]['pnl_bps'] += pnl_bps
                venues[ex]['usd_pnl'] += (pnl_bps / 10000.0) * 10.0
                
                # AsterDex breakdown
                if ex == 'asterdex':
                    asterdex_trades[st] = asterdex_trades.get(st, 0) + 1

    print("=" * 80)
    print("      KOREAN SUNRISE: ЧИСТЫЙ СРЕЗ ЗА 1.5 ЧАСА ПОСЛЕ ФИКСОВ (22:19 - 00:03)")
    print("=" * 80)
    
    tot_trades = sum(d['n'] for d in strats.values())
    tot_usd = sum(d['usd_pnl'] for d in strats.values())
    tot_wins = sum(d['wins'] for d in strats.values())
    tot_wr = tot_wins / tot_trades * 100 if tot_trades > 0 else 0

    print(f"ИТОГО ЗА ОКНО: Сделок = {tot_trades} | Винрейт = {tot_wr:.1f}% | Net PnL = {tot_usd:+.3f} $\n")

    print("[1] РЕЗУЛЬТАТЫ ПО СТРАТЕГИЯМ:")
    print(f"  {'Стратегия':<16} | {'Сделок':>6} | {'Винрейт':>8} | {'Net USD':>10} | {'Net bps':>10} | {'Avg bps':>8} | {'Filled':>6}")
    print("  " + "-" * 75)
    for st, d in strats.items():
        wr = d['wins'] / d['n'] * 100 if d['n'] > 0 else 0
        avg = d['pnl_bps'] / d['n'] if d['n'] > 0 else 0
        print(f"  {st:<16} | {d['n']:>6d} | {wr:>7.1f}% | {d['usd_pnl']:>+9.3f}$ | {d['pnl_bps']:>+9.1f} | {avg:>+7.1f} | {d['filled']:>6d}")

    print("\n[2] ПРИЧИНЫ ЗАКРЫТИЯ MAKER VOID (Strategy 3):")
    for r, cnt in maker_reasons.items():
        print(f"  • {r:<25}: {cnt:>4d}")

    print("\n[3] ПРИЧИНЫ ЗАКРЫТИЯ FADE (Strategy 2):")
    for r, cnt in fade_reasons.items():
        print(f"  • {r:<25}: {cnt:>4d}")

    print("\n[4] СРЕЗ ПО БИРЖАМ (Net USD):")
    print(f"  {'Биржа':<14} | {'Сделок':>6} | {'Винрейт':>8} | {'Net USD':>10} | {'Net bps':>10}")
    print("  " + "-" * 56)
    for ex, d in sorted(venues.items(), key=lambda x: x[1]['usd_pnl'], reverse=True):
        wr = d['wins'] / d['n'] * 100 if d['n'] > 0 else 0
        print(f"  {ex:<14} | {d['n']:>6d} | {wr:>7.1f}% | {d['usd_pnl']:>+9.3f}$ | {d['pnl_bps']:>+9.1f}")

    print("\n[5] СТАТУС ASTERDEX:")
    if not asterdex_trades:
        print("  ✅ AsterDex: 0 сделок! Полностью заблокирован, слив остановлен!")
    else:
        print(f"  AsterDex сделок: {asterdex_trades}")

if __name__ == "__main__":
    main()
