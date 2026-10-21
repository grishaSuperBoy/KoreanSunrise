"""
tools/calc_korean_formula.py — Quantitative Evaluator for the Korean Whale Alpha Formula.

Formula Core:
1. Target Venues: Top-tier liquid CEXs (Binance, Bybit, MEXC, Hyperliquid).
2. Asset Selection: High-beta alts / Korean alts (excluding heavy institutional majors where HFT eliminates lag).
3. Trigger Models:
   - Fixed Dollar Sweep: Whale volume swept on Upbit/Bithumb (default: >= $15,000).
   - Adaptive Tsunami Ratio: Ratio of Leader Swept USD to Follower Top-3 Ask Depth (default: >= 10.0x with $3,000 floor).
4. Spread Guard: Follower spread within liquid bounds (default <= 70 bps).
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

DEFAULT_VENUES: List[str] = ["binance", "bybit", "gateio", "mexc", "hyperliquid"]

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
    gross_bps = sum(float(s.get("gross_exec_pnl_bps", 0.0) or (float(s.get("net_pnl_bps", 0.0) or 0.0) + 10.0)) for s in subset)
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
        print(f"  {name:<44} | N=0 | Нет сделок")
        return
    print(f"  {name:<44} | N={res['n']:>3} | WR: {res['wr']:>5.1f}% | TP: {res['tp_wr']:>5.1f}% | Net: {res['net_bps']:>+7.1f} bps | Ср: {res['avg_net']:>+5.1f}b | PnL($100): {res['usd_100']:>+6.2f}$ {extra}")

def run_formula(args):
    signals = load_signals(args.days)
    target_venues = set(v.lower().strip() for v in args.venues.split(","))
    excluded_coins = set(c.upper().strip() for c in args.exclude_majors.split(",")) if args.exclude_majors else set()

    # Filter Strategy 1 (Lead-Lag)
    s1 = [s for s in signals if s.get("strategy") == "lead_lag"]
    
    # Enrich with tsunami ratio
    for s in s1:
        sw = float(s.get("leader_swept_usd", 0.0) or 0.0)
        top3 = float(s.get("follower_top3_ask_usd", 0.0) or 0.0)
        s["tsunami_ratio"] = sw / max(top3, 100.0)

    # Filter target venues
    s1_venues = [s for s in s1 if s.get("follower_exchange", "").lower() in target_venues]

    # Filter out heavy majors
    s1_alts = [s for s in s1_venues if s.get("coin", "").upper() not in excluded_coins]

    print("=" * 106)
    print(f"               КОРЕЙСКАЯ ФОРМУЛА: КВАНТОВЫЙ РАСЧЁТ TSUNAMI RATIO (LEAD-LAG S1)")
    print(f"  Выборка файлов: {'ВСЕ ДНИ' if not args.days else args.days} | Всего Lead-Lag: {len(s1)} | Топ-биржи: {len(s1_venues)} | Альты: {len(s1_alts)}")
    print(f"  Биржи: {', '.join(sorted(target_venues))} | Макс. спред: <= {args.max_spread} bps")
    print("=" * 106)

    if args.grid:
        print("\n--- СЕТКА 1: ФИКСИРОВАННЫЙ ВЫКУП ЛИДЕРА (LEADER SWEPT USD) ---")
        grid_steps = [1000, 3000, 5000, 8000, 10000, 12000, 15000, 20000, 25000]
        for th in grid_steps:
            sub = [
                s for s in s1_alts 
                if float(s.get("leader_swept_usd", 0) or 0) >= th 
                and float(s.get("follower_spread_bps", 999) or 999) <= args.max_spread
            ]
            res = evaluate_subset(sub)
            coins = len(set(s.get("coin") for s in sub))
            print_row(f"Фикс. выкуп >= ${th:<5} (Монет: {coins:>2})", res)

        print(f"\n--- СЕТКА 2: АДАПТИВНЫЙ TSUNAMI RATIO (ВЫКУП / СТАКАН ТОП-3) [Floor >= ${int(args.floor):,}] ---")
        ratio_steps = [1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 15.0, 20.0, 30.0, 50.0]
        for r in ratio_steps:
            sub = [
                s for s in s1_alts 
                if s["tsunami_ratio"] >= r 
                and float(s.get("leader_swept_usd", 0) or 0) >= args.floor 
                and float(s.get("follower_spread_bps", 999) or 999) <= args.max_spread
            ]
            res = evaluate_subset(sub)
            coins = len(set(s.get("coin") for s in sub))
            print_row(f"Tsunami Ratio >= {r:>4.1f}x (Монет: {coins:>2})", res)
        print("-" * 106)

    # 1. Fixed threshold evaluation
    target_swept = float(args.swept)
    fixed_trades = [
        s for s in s1_alts 
        if float(s.get("leader_swept_usd", 0) or 0) >= target_swept 
        and float(s.get("follower_spread_bps", 999) or 999) <= args.max_spread
    ]

    # 2. Adaptive Tsunami Ratio evaluation
    target_ratio = float(args.tsunami)
    adaptive_trades = [
        s for s in s1_alts 
        if s["tsunami_ratio"] >= target_ratio 
        and float(s.get("leader_swept_usd", 0) or 0) >= args.floor 
        and float(s.get("follower_spread_bps", 999) or 999) <= args.max_spread
    ]

    print("\n" + "=" * 106)
    print(f"                   СРАВНИТЕЛЬНЫЙ ИТОГ: ЭМПИРИЧЕСКИЙ vs КВАНТОВЫЙ ПОДХОД")
    print("=" * 106)
    res_fixed = evaluate_subset(fixed_trades)
    print_row(f"Эмпирический: Swept >= ${int(target_swept):,}", res_fixed)
    res_adaptive = evaluate_subset(adaptive_trades)
    print_row(f"Квантовый: Tsunami >= {target_ratio:.1f}x (Floor ${int(args.floor):,})", res_adaptive)

    # Mode selection for breakdown (default to adaptive if selected)
    active_trades = adaptive_trades if args.mode == "adaptive" else (fixed_trades if args.mode == "fixed" else adaptive_trades)
    mode_title = f"КВАНТОВЫЙ TSUNAMI RATIO >= {target_ratio:.1f}x (Floor ${int(args.floor):,})" if active_trades is adaptive_trades else f"ФИКСИРОВАННЫЙ ВЫКУП >= ${int(target_swept):,}"

    # Venue breakdown
    print(f"\n--- РАЗБИВКА ПО БИРЖАМ: {mode_title} ---")
    for ex in sorted(target_venues):
        sub_ex = [s for s in active_trades if s.get("follower_exchange", "").lower() == ex]
        res_ex = evaluate_subset(sub_ex)
        print_row(f"Биржа: {ex.upper()}", res_ex)

    # Coin breakdown
    print(f"\n--- РАЗБИВКА ПО МОНЕТАМ: {mode_title} ---")
    coin_groups = defaultdict(list)
    for s in active_trades:
        coin_groups[s.get("coin")].append(s)

    for c, c_trades in sorted(coin_groups.items(), key=lambda x: sum(float(s.get("net_pnl_bps", 0) or 0) for s in x[1]), reverse=True):
        c_res = evaluate_subset(c_trades)
        venues_used = ",".join(sorted(set(s.get("follower_exchange") for s in c_trades)))
        print_row(f"Монета: {c:<8}", c_res, extra=f"| Биржи: {venues_used}")

    # Detailed trade list
    if args.verbose or len(active_trades) <= 40:
        print(f"\n--- ВСЕ СДЕЛКИ СПИСКОМ: {mode_title} (N={len(active_trades)}) ---")
        for i, s in enumerate(active_trades, 1):
            c = s.get("coin")
            ex = s.get("follower_exchange")
            sw = float(s.get("leader_swept_usd", 0))
            top3 = float(s.get("follower_top3_ask_usd", 0))
            rat = s["tsunami_ratio"]
            net = float(s.get("net_pnl_bps", 0))
            sp = float(s.get("follower_spread_bps", 0))
            rs = s.get("close_reason", "")
            dur = float(s.get("duration_sec", 0))
            print(f"  {i:>2}. {c:<6} -> {ex:<10} | Ratio: {rat:>5.1f}x (Swept: ${sw:>7.0f} / Top3: ${top3:>5.0f}) | Net: {net:>+6.1f} bps | Спред: {sp:>4.1f}b | Причина: {rs:<17} | {dur:>5.2f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate Korean Whale Alpha Formula with Adaptive Tsunami Ratio")
    parser.add_argument("--tsunami", type=float, default=10.0, help="Min Tsunami Ratio (leader swept / follower top3 ask depth, default: 10.0x)")
    parser.add_argument("--floor", type=float, default=3000.0, help="Min leader swept USD floor for Tsunami Ratio (default: 3000.0)")
    parser.add_argument("--swept", type=float, default=15000.0, help="Fixed min leader swept volume in USD (default: 15000.0)")
    parser.add_argument("--mode", type=str, choices=["adaptive", "fixed", "all"], default="adaptive", help="Primary breakdown mode (default: adaptive)")
    parser.add_argument("--venues", type=str, default=",".join(DEFAULT_VENUES), help="Comma-separated follower venues")
    parser.add_argument("--max-spread", type=float, default=70.0, help="Max follower spread in bps (default: 70.0 bps = 0.7%)")
    parser.add_argument("--exclude-majors", type=str, default=",".join(DEFAULT_EXCLUDED_MAJORS), help="Majors to exclude")
    parser.add_argument("--days", type=str, default=None, help="Filter by day directory (e.g. 2026-10-21 or 2026-10-20)")
    parser.add_argument("--grid", action="store_true", default=True, help="Display both fixed and adaptive grid comparisons")
    parser.add_argument("--verbose", action="store_true", help="Print all trades")
    args = parser.parse_args()
    run_formula(args)
