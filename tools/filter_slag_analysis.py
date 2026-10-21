"""
tools/filter_slag_analysis.py — Quantitative breakdown of winrate after filtering slag.
"""
import glob
import gzip
import json
import os
import sys
from collections import defaultdict
from datetime import datetime

def main():
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
    prime_signals = []

    for f in files:
        try:
            with gzip.open(f, 'rt', encoding='utf-8') as gz:
                for line in gz:
                    rec = json.loads(line)
                    if rec.get('strategy') != 'lead_lag' and 'strategy' in rec:
                        continue
                    ts = float(rec.get('open_ts') or rec.get('ts') or 0.0)
                    if ts <= 0:
                        continue
                    dt_utc = datetime.utcfromtimestamp(ts)
                    kst_h = (dt_utc.hour + 9) % 24
                    if kst_h in [19, 20, 21, 22, 23, 0]:
                        prime_signals.append(rec)
        except Exception:
            pass

    print(f"Всего сигналов в вечерний прайм-тайм (19:00 - 01:00 KST): {len(prime_signals)}\n")

    def evaluate(step_name, subset):
        if not subset:
            print(f"  {step_name:<50} | Нет сделок")
            return
        n = len(subset)
        wins = sum(1 for r in subset if float(r.get('net_pnl_bps', 0.0) or 0.0) > 0)
        tp_converged = sum(1 for r in subset if r.get('close_reason') == 'converged')
        total_pnl = sum(float(r.get('net_pnl_bps', 0.0) or 0.0) for r in subset)
        avg_pnl = total_pnl / n
        wr = wins / n * 100
        tp_wr = tp_converged / n * 100
        total_usd = (total_pnl / 10000.0) * 10.0
        print(f"  {step_name:<50} | N={n:<4} | WinRate: {wr:5.1f}% | TP(Target): {tp_wr:5.1f}% | Ср. PnL: {avg_pnl:+5.1f} bps | Net PnL: {total_usd:+6.2f} $")

    print("=" * 105)
    print("                    ЭВОЛЮЦИЯ ВИНРЕЙТА: ОЧИСТКА ОТ ШЛАКА ШАГ ЗА ШАГОМ")
    print("=" * 105)

    evaluate("0. База (СО ВСЕМ ШЛАКОМ: AsterDex, шум, микро-свипы)", prime_signals)

    no_aster = [r for r in prime_signals if r.get('follower_exchange') != 'asterdex']
    evaluate("1. Убираем AsterDex (главный пылесос слива)", no_aster)

    min_swept = [r for r in no_aster if float(r.get('leader_swept_usd', 0) or 0) >= 3000.0]
    evaluate("2. + Только реальный выкуп (Swept >= $3,000)", min_swept)

    tight_foll = [r for r in min_swept if float(r.get('follower_spread_bps', 999) or 999) <= 15.0]
    evaluate("3. + Плотный стакан фолловера (Spread <= 15 bps)", tight_foll)

    solid_edge = [r for r in tight_foll if float(r.get('gross_exec_edge_bps', 0) or 0) >= 30.0]
    evaluate("4. + Уверенный эдж (Gross Edge >= 30 bps)", solid_edge)

    top_venues = ['binance', 'bybit', 'okx', 'hyperliquid', 'bingx', 'pionex']
    majors_only = [r for r in solid_edge if r.get('follower_exchange') in top_venues]
    evaluate("5. + Только ТОП-биржи (Binance, Bybit, OKX, HL, BingX)", majors_only)

    institutional = [r for r in no_aster if float(r.get('leader_swept_usd', 0) or 0) >= 10000.0 and float(r.get('follower_spread_bps', 999) or 999) <= 15.0]
    evaluate("6. ИНСТИТУЦИОНАЛЫ: Swept >= $10k & Spread <= 15 bps", institutional)

    mega_sweeps = [r for r in no_aster if float(r.get('leader_swept_usd', 0) or 0) >= 25000.0 and float(r.get('follower_spread_bps', 999) or 999) <= 20.0]
    evaluate("7. КИТЫ: Свипы >= $25,000 (пробой стакана Upbit)", mega_sweeps)

    coin_stats = defaultdict(lambda: {'n': 0, 'wins': 0, 'pnl_bps': 0.0, 'tp': 0})
    for r in no_aster:
        c = r.get('coin')
        pnl = float(r.get('net_pnl_bps', 0.0) or 0.0)
        coin_stats[c]['n'] += 1
        coin_stats[c]['pnl_bps'] += pnl
        if pnl > 0:
            coin_stats[c]['wins'] += 1
        if r.get('close_reason') == 'converged':
            coin_stats[c]['tp'] += 1

    alpha_coins = [c for c, d in coin_stats.items() if d['pnl_bps'] > 0]
    toxic_coins = [c for c, d in coin_stats.items() if d['pnl_bps'] <= 0]
    
    alpha_trades = [r for r in no_aster if r.get('coin') in alpha_coins]
    toxic_trades = [r for r in no_aster if r.get('coin') in toxic_coins]

    print("\n" + "=" * 105)
    print("                    РАЗДЕЛЕНИЕ: ЧИСТЫЙ АЛЬФА-ПУЛ VS ТОКСИЧНЫЙ ШЛАК")
    print("=" * 105)
    evaluate("1. Все сделки (без AsterDex)", no_aster)
    evaluate("2. ЧИСТЫЙ АЛЬФА-ПУЛ (15 прибыльных монет)", alpha_trades)
    evaluate("3. ТОКСИЧНЫЙ ШЛАК (CAP, ERA, GTC, GMT, FLOCK)", toxic_trades)

    print("\n" + "=" * 105)
    print("                    ТОП-10 МОНЕТ ВИННЕРОВ (РЕАЛЬНЫЙ АЛЬФА-ПУЛ)")
    print("=" * 105)
    for c, d in sorted(coin_stats.items(), key=lambda x: x[1]['pnl_bps'], reverse=True)[:10]:
        wr = d['wins'] / d['n'] * 100
        avg = d['pnl_bps'] / d['n']
        print(f"  • {c:<8} | Сделок: {d['n']:>3} | WinRate: {wr:5.1f}% | TP: {d['tp']:>2} | Ср. PnL: {avg:+5.1f} bps | Total: {d['pnl_bps']:+6.1f} bps")

    print(f"\nСписок 15 прибыльных Альфа-монет: {', '.join(sorted(alpha_coins))}")

if __name__ == "__main__":
    main()
