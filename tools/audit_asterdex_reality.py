"""
tools/audit_asterdex_reality.py — Deep Quantitative Audit of AsterDex Trades.
Recalculates all recorded AsterDex historical trades across all 3 strategies
under realistic DEX microstructure constraints:
1. Bid-ask spread drag on exit.
2. Oracle & LP quotation update latency penalty (150-500ms lag).
3. Toxicity / adverse selection on passive maker fills.
4. Counterfactual filters: Tight spreads only, High edge only, Cross-venue offset.
"""
import glob
import gzip
import json
import os
import sys

def main():
    files = sorted(glob.glob("data/signals/**/*.jsonl.gz", recursive=True))
    if not files:
        print("No signal files found in data/signals/")
        return

    ll_trades = []
    maker_trades = []
    fade_trades = []

    for f in files:
        try:
            with gzip.open(f, "rt", encoding="utf-8") as gz:
                for line in gz:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    if rec.get("follower_exchange") != "asterdex":
                        continue
                    st = rec.get("strategy", "lead_lag")
                    if st == "lead_lag":
                        ll_trades.append(rec)
                    elif st == "maker_void":
                        maker_trades.append(rec)
                    elif st == "fade_fakeout":
                        fade_trades.append(rec)
        except Exception as e:
            pass

    print("=" * 80)
    print(f"       ASTERDEX REALITY AUDIT & RE-CALCULATION REPORT")
    print(f"       Total Archives Analyzed: {len(files)} | Today: 2026-10-20")
    print("=" * 80)
    print(f"Total AsterDex Recorded Events: {len(ll_trades) + len(maker_trades) + len(fade_trades)}")
    print(f"  • Strategy 1 (Lead-Lag Taker):     {len(ll_trades)} trades")
    print(f"  • Strategy 2 (Fade Fakeout Short): {len(fade_trades)} trades")
    print(f"  • Strategy 3 (Maker Liquidity):   {len(maker_trades)} orders ({sum(1 for r in maker_trades if r.get('maker_filled'))} filled)")
    print("-" * 80)

    def print_metric(name, trades, penalty_fn=None):
        if not trades:
            print(f"  {name:<42} | No trades")
            return
        pnls = []
        wins = 0
        for r in trades:
            pnl = float(r.get("net_pnl_bps", 0.0) or 0.0)
            if penalty_fn:
                pnl -= penalty_fn(r)
            pnls.append(pnl)
            if pnl > 0:
                wins += 1
        tot = sum(pnls)
        avg = tot / len(trades)
        wr = wins / len(trades) * 100
        print(f"  {name:<42} | N={len(trades):<5} | Win: {wr:5.1f}% | Avg: {avg:+6.1f} bps | Total: {tot:+9.1f} bps")

    # -------------------------------------------------------------------------
    # 1. Strategy 1: Lead-Lag Sniping
    # -------------------------------------------------------------------------
    print("\n[1] STRATEGY 1: LEAD-LAG TAKER SNIPING")
    print_metric("A. Baseline Simulation (Raw)", ll_trades)
    print_metric("B. Reality (+15 bps lag & +exit half-spread)", ll_trades,
                 lambda r: (float(r.get("follower_spread_bps", 20.0) or 20.0) * 0.5) + 15.0)
    
    # Filter: Spread <= 10 bps
    ll_10 = [r for r in ll_trades if float(r.get("follower_spread_bps", 999) or 999) <= 10.0]
    print_metric("C. Filter: Spread <= 10 bps (Raw)", ll_10)
    print_metric("D. Filter: Spread <= 10 bps (Reality)", ll_10,
                 lambda r: (float(r.get("follower_spread_bps", 6.0) or 6.0) * 0.5) + 15.0)

    # Filter: Spread <= 6 bps & Edge >= 40 bps (Majors only: INJ, NEAR, XRP)
    ll_majors = [r for r in ll_trades if float(r.get("follower_spread_bps", 999) or 999) <= 6.0 and float(r.get("gross_exec_edge_bps", 0) or 0) >= 40.0]
    print_metric("E. Filter: Majors (Spread<=6bps & Edge>=40)", ll_majors)
    print_metric("F. Filter: Majors Reality (-18 bps penalty)", ll_majors, lambda r: 18.0)

    # -------------------------------------------------------------------------
    # 2. Strategy 2: Fade the Fakeout
    # -------------------------------------------------------------------------
    print("\n[2] STRATEGY 2: FADE THE FAKEOUT (SHORTING ON CANCEL)")
    print_metric("A. Baseline Simulation (Raw)", fade_trades)
    print_metric("B. Reality (-HalfSpread - 15 bps lag)", fade_trades,
                 lambda r: (float(r.get("follower_spread_bps", 20.0) or 20.0) * 0.5) + 15.0)

    # -------------------------------------------------------------------------
    # 3. Strategy 3: Maker Voids (Passive Limit Harvesting)
    # -------------------------------------------------------------------------
    print("\n[3] STRATEGY 3: MAKER VOIDS (PASSIVE SPREAD HARVESTING)")
    maker_filled = [r for r in maker_trades if r.get("maker_filled")]
    print_metric("A. All Filled Orders (Raw)", maker_filled)
    print_metric("B. Reality (+25 bps Adverse Selection Penalty)", maker_filled, lambda r: 25.0)

    # Split Maker by Spread:
    m_tight = [r for r in maker_filled if float(r.get("spread_open_bps", 999) or 999) <= 45.0]
    m_toxic = [r for r in maker_filled if float(r.get("spread_open_bps", 0) or 0) > 45.0]
    print_metric("C. Maker Filled on Tight Spreads (<= 45 bps)", m_tight)
    print_metric("D. Maker Filled on Toxic Spreads (> 45 bps)", m_toxic)

    print("\n" + "=" * 80)
    print("                      KEY QUANTITATIVE CONCLUSIONS")
    print("=" * 80)

    # Detailed inspection of the 4 real winners vs 41 losers
    print("\n[4] DEEP DIVE: CAN LEAD-LAG BE SAVED ON ASTERDEX?")
    for r in ll_trades:
        raw = float(r.get("net_pnl_bps", 0.0) or 0.0)
        spread = float(r.get("follower_spread_bps", 20.0) or 20.0)
        penalty = (spread * 0.5) + 15.0
        r["real_pnl"] = raw - penalty

    real_wins = [r for r in ll_trades if r["real_pnl"] > 0]
    print(f"Real Winners: {len(real_wins)} / {len(ll_trades)} ({len(real_wins)/len(ll_trades)*100:.1f}%)")
    for r in real_wins:
        print(f"  • WIN: {r.get('coin'):<6} | Raw PnL: {r.get('net_pnl_bps'):+6.2f} bps | Real PnL: {r['real_pnl']:+6.2f} bps | Spread: {r.get('follower_spread_bps'):4.1f} bps | Edge: {r.get('gross_exec_edge_bps'):5.1f} bps | Swept: ${r.get('leader_swept_usd', 0):.0f}")

    print("\n  Summary comparison with other follower venues:")
    # Compare AsterDex with Binance / Bybit in the same dataset
    all_venues = {}
    for f in files:
        try:
            with gzip.open(f, "rt", encoding="utf-8") as gz:
                for line in gz:
                    rec = json.loads(line)
                    if rec.get("strategy") == "lead_lag" or "strategy" not in rec:
                        v = rec.get("follower_exchange", "unknown")
                        if v not in all_venues:
                            all_venues[v] = {"n": 0, "pnl": 0.0, "wins": 0}
                        pnl = float(rec.get("net_pnl_bps", 0.0) or 0.0)
                        all_venues[v]["n"] += 1
                        all_venues[v]["pnl"] += pnl
                        if pnl > 0:
                            all_venues[v]["wins"] += 1
        except Exception:
            pass

    print(f"  {'VENUE':<14} | {'TRADES':<6} | {'WIN RATE':<9} | {'AVG PNL':<10} | {'TOTAL PNL'}")
    print("  " + "-" * 55)
    for v, d in sorted(all_venues.items(), key=lambda x: x[1]["pnl"], reverse=True):
        wr = d["wins"] / d["n"] * 100 if d["n"] > 0 else 0
        avg = d["pnl"] / d["n"] if d["n"] > 0 else 0
        print(f"  {v:<14} | {d['n']:<6} | {wr:6.1f}%   | {avg:+7.1f} bps | {d['pnl']:+8.1f} bps")

if __name__ == "__main__":
    main()
