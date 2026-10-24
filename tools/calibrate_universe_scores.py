"""
tools/calibrate_universe_scores.py - Empirical Backtest & Calibration of Spot-Perp Lead-Lag Scoring.

Audits all historical signals (Oct 20-23, 2026) through the new continuous
pipeline.spot_perp_dominance.score_pair_lead_lag() framework.

Verifies:
1. Does high Composite Score (>= 0.70) reliably predict positive PnL and high Win Rate?
2. How does the 0.55 - 0.70 ("HALF" size) bucket perform?
3. What is the empirical Lead Time (X seconds) where follower catches up with Upbit?
4. Rigorous venue comparison: Is OKX genuinely superior to Binance/Bybit across all days?
"""
from __future__ import annotations

import glob
import gzip
import json
import math
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import numpy as np

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipeline.spot_perp_dominance import (
    calc_cross_correlation,
    categorize_lead_lag_score,
    score_pair_lead_lag,
)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def analyze_lead_lag_path(path: List[List[float]]) -> Tuple[float, float]:
    """
    Analyzes trajectory path [[t, upbit_move_bps, follower_move_bps, pnl_bps, disloc_bps], ...]
    Returns:
        (lead_lag_corr, lead_time_sec_to_half_move)
    """
    if not path or len(path) < 5:
        return 0.0, 999.0

    upbit_moves = [row[1] for row in path]
    follower_moves = [row[2] for row in path]
    times = [row[0] for row in path]

    # Calculate lag-2 cross-correlation
    corr = calc_cross_correlation(upbit_moves, follower_moves, lag_steps=2)

    # Lead time: when did follower achieve at least 50% of Upbit's peak move?
    peak_upbit = max(abs(m) for m in upbit_moves) if upbit_moves else 0.0
    lead_time = 999.0
    if peak_upbit >= 10.0:  # meaningful move
        target_move = peak_upbit * 0.5
        for t, _, f_move, _, _ in path:
            if abs(f_move) >= target_move:
                lead_time = t
                break

    return corr, lead_time


def main():
    signal_files = sorted(glob.glob("data/signals/2026-10-*/sunrise_signals_*.jsonl.gz"))
    print(f"Scanning {len(signal_files)} signal archives...")

    all_s1 = []
    total_parsed = 0

    for fp in signal_files:
        try:
            with gzip.open(fp, "rt", encoding="utf-8") as gz:
                for line in gz:
                    total_parsed += 1
                    try:
                        r = json.loads(line)
                        if r.get("strategy") == "lead_lag":
                            all_s1.append(r)
                    except Exception:
                        pass
        except Exception as e:
            print(f"Error reading {fp}: {e}")

    print(f"Parsed {total_parsed:,} total records. Found {len(all_s1):,} Lead-Lag (S1) signals.")

    # Calibration Evaluation
    scored_signals = []
    lead_times = []

    # Historical 24h volume benchmarks by venue to estimate PVR
    venue_vol_benchmarks = {
        "binance": 12.0,  # typically 12x Upbit
        "bybit": 8.0,     # 8x
        "okx": 3.5,       # 3.5x
        "gateio": 2.5,
        "mexc": 2.0,
        "kucoin": 1.8,
        "bingx": 1.5,
        "asterdex": 0.8,
        "hyperliquid": 1.2,
    }

    for s in all_s1:
        coin = s.get("coin", "")
        venue = s.get("follower_exchange", "binance").lower()
        side = s.get("side", "long")
        pnl = float(s.get("net_pnl_bps", 0.0) or 0.0)
        swept_usd = float(s.get("leader_swept_usd", 0.0) or 0.0)
        f_depth = float(s.get("follower_top3_ask_usd" if side == "long" else "follower_top3_bid_usd", 0.0) or 500.0)
        spread_bps = float(s.get("follower_spread_bps", 10.0) or 10.0)
        sigma_bps = float(s.get("pair_sigma_bps", 30.0) or 30.0)
        recent_buys = float(s.get("leader_recent_buys_usd", 10000.0) or 10000.0)
        path = s.get("path", [])

        # Empirical metrics calculation
        corr, lead_time = analyze_lead_lag_path(path)
        if lead_time < 900.0:
            lead_times.append(lead_time)

        # SDR estimation: swept volume vs opposing follower depth
        # Coins with massive swept relative to follower depth have high spot dominance
        sdr_est = min(0.60, max(0.01, swept_usd / (swept_usd + f_depth * 10.0)))

        # PVR estimation based on venue baseline
        pvr_base = venue_vol_benchmarks.get(venue, 5.0)
        # Adjust if swept is unusually large
        pvr_est = max(0.5, pvr_base * (5000.0 / max(swept_usd, 500.0)))

        # VWAP Impact estimate: how many bps walked by swept volume
        # impact approx = spread_bps * (swept_usd / depth)
        walk_ratio = swept_usd / max(f_depth, 100.0)
        vwap_impact_bps = min(300.0, spread_bps * math.sqrt(walk_ratio))

        # Kimchi sigma in %
        kimchi_sigma_pct = sigma_bps / 100.0

        # Continuous Score
        score, comps = score_pair_lead_lag(
            sdr_24h=sdr_est,
            pvr_venue=pvr_est,
            vwap_impact_bps=vwap_impact_bps,
            kimchi_sigma_24h_pct=kimchi_sigma_pct,
            lead_lag_corr=corr if corr > 0 else 0.40,
            flow_density_15m_usd=recent_buys,
        )

        decision, size_mult = categorize_lead_lag_score(score)

        s["_score"] = score
        s["_decision"] = decision
        s["_size_mult"] = size_mult
        s["_lead_time"] = lead_time
        scored_signals.append(s)

    # Output Results
    print("\n" + "=" * 95)
    print("  COMPOSITE SCORING ENGINE: PERFORMANCE BREAKDOWN ACROSS HISTORICAL DATA")
    print("=" * 95)

    buckets = [
        ("Tier 1: FULL Size (Score >= 0.70)", [s for s in scored_signals if s["_score"] >= 0.70]),
        ("Tier 2: HALF Size (0.55 <= Score < 0.70)", [s for s in scored_signals if 0.55 <= s["_score"] < 0.70]),
        ("Tier 3: SKIP Bucket (Score < 0.55)", [s for s in scored_signals if s["_score"] < 0.55]),
    ]

    print(f"{'Категория':<42} | {'Сделок':>6} | {'Win Rate':>8} | {'Total PnL':>12} | {'Avg PnL/Tr':>11} | {'Profit Fac':>10}")
    print("-" * 95)

    for label, sigs in buckets:
        n = len(sigs)
        if n == 0:
            print(f"{label:<42} | {0:>6} |        - |            - |           - |          -")
            continue
        pnls = [float(s.get("net_pnl_bps", 0.0) or 0.0) for s in sigs]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        wr = len(wins) / n * 100.0
        tot_pnl = sum(pnls)
        avg_pnl = tot_pnl / n
        gross_w = sum(wins)
        gross_l = abs(sum(losses))
        pf = (gross_w / gross_l) if gross_l > 0 else (99.0 if gross_w > 0 else 0.0)

        print(f"{label:<42} | {n:>6} | {wr:>7.1f}% | {tot_pnl:>+11.1f}b | {avg_pnl:>+10.2f}b | {pf:>10.2f}")

    print("-" * 95)

    # Lead Time Distribution Analysis
    if lead_times:
        p25, p50, p75, p90 = np.percentile(lead_times, [25, 50, 75, 90])
        print(f"\n  [Lead Time X Empirical Distribution]:")
        print(f"  * Median (50%): {p50:.2f}s | 25th percentile: {p25:.2f}s | 75th percentile: {p75:.2f}s | 90th percentile: {p90:.2f}s")
        print(f"  * Sweet Spot (1.0s - 5.0s): {sum(1 for t in lead_times if 1.0 <= t <= 5.0) / len(lead_times) * 100:.1f}% of moves")

    # Venue Cross-Sectional Comparison
    print("\n" + "=" * 95)
    print("  VENUE CROSS-SECTIONAL AUDIT: OKX vs BINANCE vs BYBIT vs OTHERS (ALL S1 DATA)")
    print("=" * 95)
    venues = sorted(set(s.get("follower_exchange", "").lower() for s in scored_signals if s.get("follower_exchange")))
    print(f"{'Venue':<15} | {'Сделок':>6} | {'Win Rate':>8} | {'Total PnL':>12} | {'Avg PnL/Tr':>11} | {'Avg Score':>9}")
    print("-" * 95)

    for v in venues:
        v_sigs = [s for s in scored_signals if s.get("follower_exchange", "").lower() == v]
        if not v_sigs:
            continue
        n = len(v_sigs)
        pnls = [float(s.get("net_pnl_bps", 0.0) or 0.0) for s in v_sigs]
        wins = [p for p in pnls if p > 0]
        wr = len(wins) / n * 100.0
        tot_pnl = sum(pnls)
        avg_pnl = tot_pnl / n
        avg_sc = sum(s["_score"] for s in v_sigs) / n
        print(f"{v:<15} | {n:>6} | {wr:>7.1f}% | {tot_pnl:>+11.1f}b | {avg_pnl:>+10.2f}b | {avg_sc:>9.3f}")

    print("=" * 95)


if __name__ == "__main__":
    main()
