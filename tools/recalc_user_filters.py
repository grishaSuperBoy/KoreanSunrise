"""
tools/recalc_user_filters.py — Exact recalculation of PnL across Binance, Bybit, MEXC, Hyperliquid
Testing multiple interpretations of "стакан > 15000" and "спред < 0.7"
"""
import glob
import gzip
import json
import os
import sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

def load_signals(date_str=None):
    if date_str:
        pattern = f"data/signals/{date_str}/*.jsonl.gz"
    else:
        pattern = "data/signals/**/*.jsonl.gz"
    files = sorted(glob.glob(pattern, recursive=True))
    signals = []
    for f in files:
        try:
            with gzip.open(f, "rt", encoding="utf-8") as gz:
                for line in gz:
                    s = json.loads(line)
                    signals.append(s)
        except Exception as e:
            pass
    return signals

def run_analysis(signals, title=""):
    print(f"\n{'='*90}")
    print(f"  {title} (Total signals: {len(signals)})")
    print(f"{'='*90}")

    TARGET_VENUES = {"binance", "bybit", "mexc", "hyperliquid"}
    
    # Filter strategy 1 (Lead-Lag)
    s1 = [s for s in signals if s.get("strategy") == "lead_lag"]
    s1_target = [s for s in s1 if s.get("follower_exchange", "").lower() in TARGET_VENUES]
    
    print(f"Total Lead-Lag signals across all venues: {len(s1)}")
    print(f"Lead-Lag signals on 4 venues (Binance, Bybit, MEXC, Hyperliquid): {len(s1_target)}")
    
    def stats(subset, name):
        if not subset:
            print(f"  {name:<45} | N=0 | Нет сделок")
            return
        n = len(subset)
        wins = sum(1 for s in subset if float(s.get("net_pnl_bps", 0.0) or 0.0) > 0)
        tp = sum(1 for s in subset if s.get("close_reason") == "converged")
        net_bps = sum(float(s.get("net_pnl_bps", 0.0) or 0.0) for s in subset)
        gross_bps = sum(float(s.get("gross_exec_pnl_bps", 0.0) or s.get("net_pnl_bps", 0.0) + 10.0) for s in subset)
        avg_net = net_bps / n
        avg_gross = gross_bps / n
        wr = wins / n * 100
        tp_wr = tp / n * 100
        # PnL in USD based on standard $10 order size
        net_usd_10 = (net_bps / 10000.0) * 10.0
        # PnL in USD based on $100 order size
        net_usd_100 = (net_bps / 10000.0) * 100.0
        print(f"  {name:<45} | N={n:>4} | WR: {wr:>5.1f}% | TP: {tp_wr:>5.1f}% | Avg: {avg_net:>+5.1f} bps | Net PnL: {net_bps:>+8.1f} bps | USD($10): {net_usd_10:>+7.2f}$ | USD($100): {net_usd_100:>+7.2f}$")

    print("\n--- 1. РАЗБИВКА ПО 4 БИРЖАМ (БЕЗ ФИЛЬТРОВ, ЧИСТЫЙ LEAD-LAG) ---")
    for ex in ["binance", "bybit", "mexc", "hyperliquid"]:
        sub = [s for s in s1_target if s.get("follower_exchange", "").lower() == ex]
        stats(sub, f"Биржа: {ex.upper()}")
    stats(s1_target, "ИТОГО 4 БИРЖИ (RAW)")

    print("\n--- 2. ВАРИАНТЫ ТРАКТОВКИ 'СТАКАН > 15,000' И 'СПРЕД < 0.7' ---")
    
    # Interpretation A: Follower depth (top3_ask >= $15,000) and Spread <= 70 bps (0.7%)
    sub_a1 = [s for s in s1_target if float(s.get("follower_top3_ask_usd", 0) or 0) >= 15000.0 and float(s.get("follower_spread_bps", 999) or 999) <= 70.0]
    stats(sub_a1, "A1: Follower Top3 Ask >= $15k & Spread <= 70 bps (0.7%)")

    # Interpretation A2: Follower depth (top3_ask >= $15,000) and Spread <= 15 bps
    sub_a2 = [s for s in s1_target if float(s.get("follower_top3_ask_usd", 0) or 0) >= 15000.0 and float(s.get("follower_spread_bps", 999) or 999) <= 15.0]
    stats(sub_a2, "A2: Follower Top3 Ask >= $15k & Spread <= 15 bps (0.15%)")

    # Interpretation A3: Follower depth (top3_ask >= $15,000) and Spread <= 7.0 bps
    sub_a3 = [s for s in s1_target if float(s.get("follower_top3_ask_usd", 0) or 0) >= 15000.0 and float(s.get("follower_spread_bps", 999) or 999) <= 7.0]
    stats(sub_a3, "A3: Follower Top3 Ask >= $15k & Spread <= 7.0 bps")

    # Interpretation B1: Leader Swept (Upbit/Bithumb выкуп >= $15,000) and Spread <= 70 bps
    sub_b1 = [s for s in s1_target if float(s.get("leader_swept_usd", 0) or 0) >= 15000.0 and float(s.get("follower_spread_bps", 999) or 999) <= 70.0]
    stats(sub_b1, "B1: Leader Swept >= $15k & Spread <= 70 bps (0.7%)")

    # Interpretation B2: Leader Swept >= $15,000 and Spread <= 15 bps
    sub_b2 = [s for s in s1_target if float(s.get("leader_swept_usd", 0) or 0) >= 15000.0 and float(s.get("follower_spread_bps", 999) or 999) <= 15.0]
    stats(sub_b2, "B2: Leader Swept >= $15k & Spread <= 15 bps")

    # Interpretation C1: Either Follower Depth >= 15k OR Leader Swept >= 15k, Spread <= 70 bps
    sub_c1 = [s for s in s1_target if (float(s.get("follower_top3_ask_usd", 0) or 0) >= 15000.0 or float(s.get("leader_swept_usd", 0) or 0) >= 15000.0) and float(s.get("follower_spread_bps", 999) or 999) <= 70.0]
    stats(sub_c1, "C1: (Follower >= 15k OR Swept >= 15k) & Spread <= 70 bps")

    # Interpretation D: Coin-level filter. Calculate average/median depth and spread per coin on these 4 venues!
    # A coin qualifies if its median follower depth >= $15,000 and median spread <= 70 bps
    coin_depth = defaultdict(list)
    coin_spread = defaultdict(list)
    for s in s1_target:
        c = s.get("coin")
        coin_depth[c].append(float(s.get("follower_top3_ask_usd", 0) or 0))
        coin_spread[c].append(float(s.get("follower_spread_bps", 999) or 999))
    
    import statistics
    qualifying_coins_d = set()
    for c in coin_depth:
        med_d = statistics.median(coin_depth[c])
        med_sp = statistics.median(coin_spread[c])
        if med_d >= 15000.0 and med_sp <= 70.0:
            qualifying_coins_d.add(c)
    
    sub_d = [s for s in s1_target if s.get("coin") in qualifying_coins_d]
    stats(sub_d, f"D: Монеты с Median Depth >= $15k & Median Spread <= 0.7% ({len(qualifying_coins_d)} монет)")

    # Interpretation E: Screener volume filter (24h volume on leader/follower >= 15,000) & Spread <= 70 bps
    # Just any trade with spread <= 70 bps (0.7%) on these 4 top venues
    sub_e = [s for s in s1_target if float(s.get("follower_spread_bps", 999) or 999) <= 70.0]
    stats(sub_e, "E: Только Spread <= 70 bps (0.7%) на 4 топ-биржах")

    # Interpretation F: Spread <= 15 bps on 4 top venues
    sub_f = [s for s in s1_target if float(s.get("follower_spread_bps", 999) or 999) <= 15.0]
    stats(sub_f, "F: Spread <= 15 bps (0.15%) на 4 топ-биржах")

    # Detailed per-venue breakdown for Interpretation A1
    print("\n--- 3. ДЕТАЛЬНО ПО 4 БИРЖАМ ДЛЯ ФИЛЬТРА A1 (Depth >= 15k, Spread <= 70 bps) ---")
    for ex in ["binance", "bybit", "mexc", "hyperliquid"]:
        sub = [s for s in sub_a1 if s.get("follower_exchange", "").lower() == ex]
        stats(sub, f"Биржа: {ex.upper()}")

    # Top coins for Interpretation A1
    coin_pnl = defaultdict(lambda: {"count": 0, "wins": 0, "net": 0.0, "gross": 0.0})
    for s in sub_a1:
        c = s.get("coin")
        pnl = float(s.get("net_pnl_bps", 0.0) or 0.0)
        coin_pnl[c]["count"] += 1
        coin_pnl[c]["net"] += pnl
        if pnl > 0:
            coin_pnl[c]["wins"] += 1
    
    print("\n--- 4. ТОП-10 ПРИБЫЛЬНЫХ МОНЕТ (по фильтру A1) ---")
    for c, d in sorted(coin_pnl.items(), key=lambda x: x[1]["net"], reverse=True)[:10]:
        wr = d["wins"] / d["count"] * 100
        avg = d["net"] / d["count"]
        print(f"  • {c:<8} | Сделок: {d['count']:>3} | WinRate: {wr:5.1f}% | Avg: {avg:+5.1f} bps | Net: {d['net']:+7.1f} bps")

    print("\n--- 5. ТОП-5 МИНУСОВЫХ МОНЕТ (по фильтру A1) ---")
    for c, d in sorted(coin_pnl.items(), key=lambda x: x[1]["net"])[:5]:
        wr = d["wins"] / d["count"] * 100
        avg = d["net"] / d["count"]
        print(f"  • {c:<8} | Сделок: {d['count']:>3} | WinRate: {wr:5.1f}% | Avg: {avg:+5.1f} bps | Net: {d['net']:+7.1f} bps")

if __name__ == "__main__":
    print("=== АНАЛИЗ ДАННЫХ ЗА 2026-10-20 (ВЧЕРА) ===")
    sig_yesterday = load_signals("2026-10-20")
    run_analysis(sig_yesterday, "2026-10-20 (ВЧЕРА)")

    print("\n\n=== АНАЛИЗ ДАННЫХ ЗА 2026-10-21 (СЕГОДНЯ) ===")
    sig_today = load_signals("2026-10-21")
    run_analysis(sig_today, "2026-10-21 (СЕГОДНЯ)")

    print("\n\n=== СУММАРНЫЙ АНАЛИЗ (ОБА ДНЯ: 2026-10-20 + 2026-10-21) ===")
    sig_all = sig_yesterday + sig_today
    run_analysis(sig_all, "СУММАРНО ЗА 2 ДНЯ (20-21 ОКТЯБРЯ)")
