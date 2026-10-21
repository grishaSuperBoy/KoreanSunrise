"""
tools/calc_alpha_returns.py — Exact dollar calculation for slot sizing.
"""
import glob
import gzip
import json
import os
import sys
from datetime import datetime

def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))

    alpha_coins = {'AXS', 'ELSA', 'IOST', 'JTO', 'LIT', 'MEGA', 'O', 'PUFFER', 'TIA', 'UNI', 'VET', 'ZKP'}

    alpha_trades_24h = []
    alpha_trades_evening = []

    for f in files:
        try:
            with gzip.open(f, 'rt', encoding='utf-8') as gz:
                for line in gz:
                    rec = json.loads(line)
                    if rec.get('strategy') != 'lead_lag' and 'strategy' in rec:
                        continue
                    if rec.get('follower_exchange') == 'asterdex':
                        continue
                    c = rec.get('coin')
                    if c in alpha_coins:
                        alpha_trades_24h.append(rec)
                        ts = float(rec.get('open_ts') or rec.get('ts') or 0.0)
                        if ts > 0:
                            dt_utc = datetime.utcfromtimestamp(ts)
                            kst_h = (dt_utc.hour + 9) % 24
                            if kst_h in [19, 20, 21, 22, 23, 0]:
                                alpha_trades_evening.append(rec)
        except Exception:
            pass

    def calc(trades, slot):
        tot_bps = sum(float(r.get('net_pnl_bps', 0.0) or 0.0) for r in trades)
        tot_usd = sum((float(r.get('net_pnl_bps', 0.0) or 0.0) / 10000.0) * slot for r in trades)
        wins = sum(1 for r in trades if float(r.get('net_pnl_bps', 0.0) or 0.0) > 0)
        tp = sum(1 for r in trades if r.get('close_reason') == 'converged')
        n = len(trades)
        wr = wins / n * 100 if n > 0 else 0
        tp_wr = tp / n * 100 if n > 0 else 0
        avg_bps = tot_bps / n if n > 0 else 0
        return n, wr, tp_wr, avg_bps, tot_bps, tot_usd

    n_ev, wr_ev, tp_ev, avg_ev, bps_ev, usd_ev = calc(alpha_trades_evening, 10.0)
    n_24, wr_24, tp_24, avg_24, bps_24, usd_24 = calc(alpha_trades_24h, 10.0)

    print("=" * 80)
    print("        РАСЧЁТ ЧИСТОЙ ПРИБЫЛИ НА АЛЬФА-МОНЕТАХ (СЛОТ $10)")
    print("=" * 80)
    print(f"1. Вечерняя сессия (6 часов, 19:00 - 01:00 KST):")
    print(f"   • Сделок:           {n_ev}")
    print(f"   • Win Rate:         {wr_ev:.1f}% (Тейк-профитов TP: {tp_ev:.1f}%)")
    print(f"   • Накоплено базисов: {bps_ev:+.1f} bps (в среднем {avg_ev:+.1f} bps на сделку)")
    print(f"   • ЧИСТАЯ ПРИБЫЛЬ:   +{usd_ev:.3f} $\n")

    print(f"2. Полные сутки (24 часа, все сессии):")
    print(f"   • Сделок:           {n_24}")
    print(f"   • Win Rate:         {wr_24:.1f}% (Тейк-профитов TP: {tp_24:.1f}%)")
    print(f"   • Накоплено базисов: {bps_24:+.1f} bps (в среднем {avg_24:+.1f} bps на сделку)")
    print(f"   • ЧИСТАЯ ПРИБЫЛЬ:   +{usd_24:.3f} $\n")

    print("-" * 80)
    print("    МАСШТАБИРОВАНИЕ НА РАЗНЫЕ РАЗМЕРЫ ВХОДА (ПРИ ТОМ ЖЕ ВИНРЕЙТЕ)")
    print("-" * 80)
    print(f"  {'Размер входа (слот)':<22} | {'Вечер (6 часов)':>16} | {'Сутки (24 часа)':>16} | {'Доходность (% к слоту)'}")
    print("  " + "-" * 75)
    for slot in [10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]:
        u_ev = (bps_ev / 10000.0) * slot
        u_24 = (bps_24 / 10000.0) * slot
        pct_ev = (u_ev / slot) * 100
        pct_24 = (u_24 / slot) * 100
        print(f"  {slot:>7.0f} $ на сделку     | {u_ev:>+13.2f} $   | {u_24:>+13.2f} $   |  +{pct_24:.1f}%")

if __name__ == "__main__":
    main()
