"""
tools/calc_korean_formula.py — Quantitative Evaluator for the Korean Whale Alpha Formula.

Formula Core:
1. Target Venues: Top-tier liquid CEXs (Binance, Bybit, MEXC, Hyperliquid).
2. Asset Selection: High-beta alts / Korean alts (excluding heavy institutional majors where HFT eliminates lag).
3. Trigger Threshold: Serious Korean whale volume swept on Upbit/Bithumb (default: >= $10k / $15k).
4. Spread Guard: Follower spread within liquid bounds (default <= 15 bps or <= 70 bps).
"""
import argparse
import glob
import gzip
import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Set, Any

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Institutional majors where global market makers arbitrage Upbit instantly (< 5ms)
DEFAULT_EXCLUDED_MAJORS: Set[str] = {
    "BTC", "ETH", "SOL", "AAVE", "UNI", "INJ", "PENDLE", "JTO", 
    "ENS", "GMT", "AXL", "DATA", "ICP", "ZRO"
}

DEFAULT_VENUES: List[str] = ["binance", "bybit", "mexc", "hyperliquid"]

def load_signals(date_str: str = None) -> List[Dict[str, Any]]:
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
        except Exception:
            pass
    return signals

def evaluate_subset(subset: List[Dict[str, Any]], label: str = "") -> Dict[str, Any]:
    n = len(subset)
    if n == 0:
        return {"n": 0, "wins": 0, "tp": 0, "net_bps": 0.0, "gross_bps": 0.0, "wr": 0.0, "tp_wr": 0.0, "avg_net": 0.0, "usd_100": 0.0}
    wins = sum(1 for s in subset if float(s.get("net_pnl_bps", 0.0) or 0.0) > 0)
    tp = sum(1 for s in subset if s.get("close_reason") == "converged")
    net_bps = sum(float(s.get("net_pnl_bps", 0.0) or 0.0) for s in subset)
    gross_bps = sum(float(s.get("gross_exec_pnl_bps", 0.0) or s.get("net_pnl_bps", 0.0) + 10.0) for s in subset)
    wr = wins / n * 100.0
    tp_wr = tp / n * 100.0
    avg_net = net_bps / n
    usd_100 = (net_bps / 10000.0) * 100.0
    return {
        "n": n, "wins": wins, "tp": tp, "net_bps": net_bps, "gross_bps": gross_bps,
        "wr": wr, "tp_wr": tp_wr, "avg_net": avg_net, "usd_100": usd_100
    }

def print_row(name: str, res: Dict[str, Any], extra: str = ""):
    if res["n"] == 0:
        print(f"  {name:<40} | N=0 | Нет сделок")
        return
    print(f"  {name:<40} | N={res['n']:>3} | WR: {res['wr']:>5.1f}% | TP: {res['tp_wr']:>5.1f}% | Net: {res['net_bps']:>+7.1f} bps | Ср: {res['avg_net']:>+5.1f}b | PnL($100): {res['usd_100']:>+6.2f}$ {extra}")

def run_formula(args):
    signals = load_signals(args.days)
    target_venues = set(v.lower().strip() for v in args.venues.split(","))
    excluded_coins = set(c.upper().strip() for c in args.exclude_majors.split(",")) if args.exclude_majors else set()

    # Filter Strategy 1 (Lead-Lag)
    s1 = [s for s in signals if s.get("strategy") == "lead_lag"]
    
    # Filter 4 venues
    s1_venues = [s for s in s1 if s.get("follower_exchange", "").lower() in target_venues]

    # Filter out heavy majors
    s1_alts = [s for s in s1_venues if s.get("coin", "").upper() not in excluded_coins]

    print("=" * 100)
    print(f"               КОРЕЙСКАЯ ФОРМУЛА: ОЦЕНКА КИТОВОЙ АЛЬФЫ (LEAD-LAG S1)")
    print(f"  Выборка файлов: {'ВСЕ ДНИ' if not args.days else args.days} | Всего Lead-Lag: {len(s1)} | На топ-биржах: {len(s1_venues)} | Альты: {len(s1_alts)}")
    print(f"  Биржи: {', '.join(sorted(target_venues))} | Фильтр спреда: <= {args.max_spread} bps")
    print("=" * 100)

    if args.grid:
        print("\n--- СЕТКА ПОРОГОВ ВЫКУПА (LEADER SWEPT USD) ---")
        grid_steps = [1000, 3000, 5000, 8000, 10000, 12000, 15000, 20000, 25000]
        for th in grid_steps:
            sub = [s for s in s1_alts if float(s.get("leader_swept_usd", 0) or 0) >= th and float(s.get("follower_spread_bps", 999) or 999) <= args.max_spread]
            res = evaluate_subset(sub)
            coins = len(set(s.get("coin") for s in sub))
            print_row(f"Выкуп >= ${th:<5} (Монет: {coins:>2})", res)
        print("-" * 100)

    # Specific threshold evaluation
    target_swept = float(args.swept)
    final_trades = [
        s for s in s1_alts 
        if float(s.get("leader_swept_usd", 0) or 0) >= target_swept 
        and float(s.get("follower_spread_bps", 999) or 999) <= args.max_spread
    ]

    print(f"\n--- ИТОГОВЫЙ РЕЗУЛЬТАТ ПРИ ВЫКУПЕ >= ${int(target_swept):,} ---")
    tot_res = evaluate_subset(final_trades)
    print_row(f"ИТОГО (Swept >= ${int(target_swept)})", tot_res)

    # By venue breakdown
    print(f"\n--- РАЗБИВКА ПО БИРЖАМ (Swept >= ${int(target_swept)}) ---")
    for ex in sorted(target_venues):
        sub_ex = [s for s in final_trades if s.get("follower_exchange", "").lower() == ex]
        res_ex = evaluate_subset(sub_ex)
        print_row(f"Биржа: {ex.upper()}", res_ex)

    # By coin breakdown
    print(f"\n--- РАЗБИВКА ПО МОНЕТАМ (Swept >= ${int(target_swept)}) ---")
    coin_groups = defaultdict(list)
    for s in final_trades:
        coin_groups[s.get("coin")].append(s)

    for c, c_trades in sorted(coin_groups.items(), key=lambda x: sum(float(s.get("net_pnl_bps", 0) or 0) for s in x[1]), reverse=True):
        c_res = evaluate_subset(c_trades)
        venues_used = ",".join(sorted(set(s.get("follower_exchange") for s in c_trades)))
        print_row(f"Монета: {c:<8}", c_res, extra=f"| Биржи: {venues_used}")

    # Detailed trade list
    if args.verbose or len(final_trades) <= 25:
        print(f"\n--- ВСЕ СДЕЛКИ СПИСКОМ (N={len(final_trades)}) ---")
        for i, s in enumerate(final_trades, 1):
            c = s.get("coin")
            ex = s.get("follower_exchange")
            sw = float(s.get("leader_swept_usd", 0))
            net = float(s.get("net_pnl_bps", 0))
            sp = float(s.get("follower_spread_bps", 0))
            rs = s.get("close_reason", "")
            dur = float(s.get("duration_sec", 0))
            print(f"  {i:>2}. {c:<6} -> {ex:<10} | Swept: ${sw:>8.1f} | Net: {net:>+6.1f} bps | Спред: {sp:>4.1f}b | Причина: {rs:<17} | Время: {dur:>5.2f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate Korean Whale Alpha Formula")
    parser.add_argument("--swept", type=float, default=15000.0, help="Min leader swept volume in USD (default: 15000)")
    parser.add_argument("--venues", type=str, default=",".join(DEFAULT_VENUES), help="Comma-separated follower venues")
    parser.add_argument("--max-spread", type=float, default=70.0, help="Max follower spread in bps (default: 70.0 bps = 0.7%)")
    parser.add_argument("--exclude-majors", type=str, default=",".join(DEFAULT_EXCLUDED_MAJORS), help="Majors to exclude")
    parser.add_argument("--days", type=str, default=None, help="Filter by day directory (e.g. 2026-10-21 or 2026-10-20)")
    parser.add_argument("--grid", action="store_true", default=True, help="Display swept threshold grid comparison")
    parser.add_argument("--verbose", action="store_true", help="Print all trades")
    args = parser.parse_args()
    run_formula(args)
