"""
tools/full_multi_strategy_breakdown.py - Comprehensive Multi-Strategy Quantitative Analytics.
Analyzes Strategy 1 (Lead-Lag), Strategy 2 (Fade Fakeout), Strategy 3 (Maker Void),
Strategy 4 (Rubber Band Rebound), and Strategy 5 (Cross-Korean Triangle) across all exchanges,
coins, filters, and exit reasons.
"""
from __future__ import annotations

import glob
import gzip
import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def analyze_chunks(file_paths: List[str], label: str = "SESSION"):
    records_by_strat: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    total_lines = 0

    for fp in file_paths:
        try:
            with gzip.open(fp, "rt", encoding="utf-8") as gz:
                for line in gz:
                    total_lines += 1
                    try:
                        d = json.loads(line)
                        st = d.get("strategy", "lead_lag")
                        records_by_strat[st].append(d)
                    except Exception:
                        pass
        except Exception as e:
            print(f"Error reading {fp}: {e}")

    print(f"\n{'='*95}")
    print(f"  KOREAN SUNRISE: MULTI-STRATEGY QUANTITATIVE AUDIT | {label}")
    print(f"  Files scanned: {len(file_paths)} chunks | Total records parsed: {total_lines:,}")
    print(f"{'='*95}")

    for strat_name, recs in sorted(records_by_strat.items()):
        print(f"\n{'-'*95}")
        print(f"  STRATEGY: [{strat_name.upper()}] — Total Records: {len(recs):,}")
        print(f"{'-'*95}")

        # Distinguish maker resting orders from actual fills/trades
        filled_recs = []
        unfilled_recs = []
        for r in recs:
            # Maker void
            if strat_name == "maker_void":
                if r.get("maker_filled", False):
                    filled_recs.append(r)
                else:
                    unfilled_recs.append(r)
            # Rubber band
            elif strat_name == "rubber_band":
                if r.get("maker_filled", False) or r.get("order_type") == "taker_sniper":
                    filled_recs.append(r)
                else:
                    unfilled_recs.append(r)
            # Lead lag, fade, triangle (taker or executed)
            else:
                filled_recs.append(r)

        if not filled_recs and not unfilled_recs:
            continue

        print(f"  * Total Posted: {len(recs):,} | Filled/Executed: {len(filled_recs):,} | Unfilled: {len(unfilled_recs):,}")
        if recs and unfilled_recs:
            fill_rate = len(filled_recs) / len(recs) * 100.0
            print(f"  * Execution Fill Rate: {fill_rate:.1f}%")

        if not filled_recs:
            print("  (No filled trades for this strategy in this period)")
            continue

        # Performance metrics on filled trades
        pnls = [r.get("net_pnl_bps", 0.0) for r in filled_recs if r.get("net_pnl_bps") is not None]
        if not pnls:
            print("  (No PnL data available)")
            continue

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        evens = [p for p in pnls if p == 0]

        total_pnl = sum(pnls)
        avg_pnl = total_pnl / len(pnls) if pnls else 0.0
        win_rate = len(wins) / len(pnls) * 100.0 if pnls else 0.0

        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)

        durations = [r.get("duration_sec", 0.0) for r in filled_recs if r.get("duration_sec") is not None]
        avg_dur = sum(durations) / len(durations) if durations else 0.0

        print(f"  ┌────────────────────────┬─────────────┬─────────────┬─────────────┬─────────────┐")
        print(f"  │ Trades: {len(pnls):<14} │ Win Rate:   │ Total PnL:  │ Avg PnL:    │ Profit Fac: │")
        print(f"  │ Wins: {len(wins):<4} Losses: {len(losses):<4} │ {win_rate:6.1f}%    │ {total_pnl:+9.1f} b │ {avg_pnl:+7.2f} bps │ {profit_factor:7.2f}     │")
        print(f"  ├────────────────────────┴─────────────┴─────────────┴─────────────┴─────────────┤")
        print(f"  │ Avg Win: {avg_win:+6.1f} bps | Avg Loss: {avg_loss:+6.1f} bps | Avg Duration: {avg_dur:4.1f}s | Evens: {len(evens)}      │")
        print(f"  └────────────────────────────────────────────────────────────────────────────────┘")

        # Exit reasons
        reasons: Dict[str, List[float]] = defaultdict(list)
        for r in filled_recs:
            reason = r.get("close_reason", "unknown") or "unknown"
            p = r.get("net_pnl_bps", 0.0)
            reasons[reason].append(p)

        print(f"\n  Exit Reason Breakdown:")
        print(f"  {'Reason':<30} | {'Count':<7} | {'Win Rate':<9} | {'Avg PnL':<11} | {'Total PnL':<11}")
        print(f"  {'-'*30}-+-{'-'*7}-+-{'-'*9}-+-{'-'*11}-+-{'-'*11}")
        for r_name, r_pnls in sorted(reasons.items(), key=lambda x: len(x[1]), reverse=True):
            r_wins = [p for p in r_pnls if p > 0]
            r_wr = len(r_wins) / len(r_pnls) * 100.0 if r_pnls else 0.0
            r_avg = sum(r_pnls) / len(r_pnls) if r_pnls else 0.0
            r_tot = sum(r_pnls)
            print(f"  {r_name:<30} | {len(r_pnls):<7} | {r_wr:6.1f}%   | {r_avg:+8.2f} b | {r_tot:+9.1f} b")

        # By Exchange
        exchanges: Dict[str, List[float]] = defaultdict(list)
        for r in filled_recs:
            ex = r.get("follower_exchange", "unknown")
            p = r.get("net_pnl_bps", 0.0)
            exchanges[ex].append(p)

        print(f"\n  Exchange Breakdown:")
        print(f"  {'Exchange':<16} | {'Trades':<7} | {'Win Rate':<9} | {'Avg PnL':<11} | {'Total PnL':<11}")
        print(f"  {'-'*16}-+-{'-'*7}-+-{'-'*9}-+-{'-'*11}-+-{'-'*11}")
        for ex, e_pnls in sorted(exchanges.items(), key=lambda x: sum(x[1]), reverse=True):
            e_wins = [p for p in e_pnls if p > 0]
            e_wr = len(e_wins) / len(e_pnls) * 100.0 if e_pnls else 0.0
            e_avg = sum(e_pnls) / len(e_pnls) if e_pnls else 0.0
            e_tot = sum(e_pnls)
            print(f"  {ex:<16} | {len(e_pnls):<7} | {e_wr:6.1f}%   | {e_avg:+8.2f} b | {e_tot:+9.1f} b")

        # By Coin (Top Winners & Top Losers)
        coins: Dict[str, List[float]] = defaultdict(list)
        for r in filled_recs:
            c = r.get("coin", "unknown")
            p = r.get("net_pnl_bps", 0.0)
            coins[c].append(p)

        coin_stats = []
        for c, c_pnls in coins.items():
            c_wins = [p for p in c_pnls if p > 0]
            c_wr = len(c_wins) / len(c_pnls) * 100.0 if c_pnls else 0.0
            c_avg = sum(c_pnls) / len(c_pnls) if c_pnls else 0.0
            c_tot = sum(c_pnls)
            coin_stats.append((c, len(c_pnls), c_wr, c_avg, c_tot))

        coin_stats.sort(key=lambda x: x[4], reverse=True)

        print(f"\n  Top 5 Profitable Coins:")
        for c, count, wr, avg_p, tot_p in coin_stats[:5]:
            print(f"    * {c:<10} | {count:3d} trades | WR: {wr:5.1f}% | Avg: {avg_p:+6.1f} bps | Total: {tot_p:+7.1f} bps")

        print(f"  Top 5 Losing Coins:")
        for c, count, wr, avg_p, tot_p in reversed(coin_stats[-5:]):
            if tot_p < 0:
                print(f"    * {c:<10} | {count:3d} trades | WR: {wr:5.1f}% | Avg: {avg_p:+6.1f} bps | Total: {tot_p:+7.1f} bps")

        # Specific strategy inspection for S1
        if strat_name == "lead_lag":
            print(f"\n  --- Strategy 1 Individual Trades Audit ---")
            for r in filled_recs:
                tid = r.get("id", "")
                c = r.get("coin", "")
                f_ex = r.get("follower_exchange", "")
                side = r.get("side", "long")
                phi = r.get("phi", 0.0)
                base = r.get("kimchi_baseline_bps", 0.0)
                disloc = r.get("raw_dislocation_bps", 0.0)
                pnl = r.get("net_pnl_bps", 0.0)
                reason = r.get("close_reason", "")
                dur = r.get("duration_sec", 0.0)
                swept = r.get("leader_swept_usd", 0.0)
                f_depth = r.get("follower_top3_ask_usd", 0.0) if side == "long" else r.get("follower_top3_bid_usd", 0.0)
                print(f"    [{tid}] {c:<6} @ {f_ex:<10} {side:<5} | Swept=${swept:.0f} Depth=${f_depth:.0f} (Phi={phi:.1f}x) | Base={base:+.1f}b Disloc={disloc:+.1f}b | PnL={pnl:+.1f}b ({reason}) {dur:.1f}s")


if __name__ == "__main__":
    # Scan overnight session (chunks written after 2026-10-23 20:57 UTC)
    # File pattern for overnight session: sunrise_signals_20261023_182738_0001 ... _0011
    overnight_files = sorted(glob.glob("data/signals/2026-10-23/sunrise_signals_20261023_*_00*.jsonl.gz"))[-11:]
    analyze_chunks(overnight_files, label="LATEST OVERNIGHT SESSION (POST QUANT-FIX: Oct 23 21:08 - Oct 24 02:54)")

    # Scan ALL chunks from Oct 23
    all_oct23_files = sorted(glob.glob("data/signals/2026-10-23/sunrise_signals_*.jsonl.gz"))
    analyze_chunks(all_oct23_files, label="FULL DAY OCT 23 (ALL 46 CHUNKS, ~24 HOURS)")
