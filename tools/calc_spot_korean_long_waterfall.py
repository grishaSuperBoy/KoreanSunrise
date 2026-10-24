"""
tools/calc_spot_korean_long_waterfall.py - Sequential Waterfall Analysis:
Step 1: Spot-Dominance Filter
Step 2: Korean Long Filter Suite
"""
from __future__ import annotations

import glob
import gzip
import json
import math
import os
import sys

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pipeline.spot_perp_dominance import score_pair_lead_lag, categorize_lead_lag_score

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def main():
    files = sorted(glob.glob("data/signals/2026-10-*/sunrise_signals_*.jsonl.gz"))
    all_s1 = []
    for fp in files:
        with gzip.open(fp, "rt", encoding="utf-8") as gz:
            for line in gz:
                try:
                    r = json.loads(line)
                    if r.get("strategy") == "lead_lag":
                        all_s1.append(r)
                except Exception:
                    pass

    print(f"Total S1 Signals parsed across all 187 archives: {len(all_s1)}")

    toxic = {'CAP', 'ERA', 'GTC', 'GMT', 'FLOCK', 'DRV', 'ATH', 'SPEC', 'MASK', 'B3', 'CP', 'IRYS', 'BIRB', 'BLEND'}
    heavy = {'BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'WLD', 'NEAR', 'LINK', 'SUI', 'APT', 'UNI', 'ETC', 'AXS', 'BEAM'}

    # Step 0: Raw S1
    step0_all = all_s1

    # Step 1: Spot-Dominance Filter (Score >= 0.55)
    scored = []
    for s in all_s1:
        swept = float(s.get("leader_swept_usd", 0) or 0)
        side = s.get("side", "long")
        f_depth = float(s.get("follower_top3_ask_usd" if side == "long" else "follower_top3_bid_usd", 0) or 500)
        spread = float(s.get("follower_spread_bps", 10) or 10)
        sigma = float(s.get("pair_sigma_bps", 30) or 30)
        recent = float(s.get("leader_recent_buys_usd", 10000) or 10000)
        sdr_est = min(0.60, max(0.01, swept / (swept + f_depth * 10.0)))
        walk_ratio = swept / max(f_depth, 100.0)
        vwap_impact = min(300.0, spread * (walk_ratio ** 0.5))

        score, comps = score_pair_lead_lag(
            sdr_24h=sdr_est,
            pvr_venue=5.0,
            vwap_impact_bps=vwap_impact,
            kimchi_sigma_24h_pct=sigma / 100.0,
            lead_lag_corr=0.60,
            flow_density_15m_usd=recent,
        )
        s["_score"] = score
        if score >= 0.55:
            scored.append(s)

    step1_spot_filter = scored

    # Step 2: Korean Long Suite on top of Spot Filter
    step2_long_only = [s for s in step1_spot_filter if s.get("side", "long") == "long"]
    step3_swept_20k = [s for s in step2_long_only if float(s.get("leader_swept_usd", 0) or 0) >= 20000.0]
    step4_spread_15bps = [s for s in step3_swept_20k if float(s.get("follower_spread_bps", 999) or 999) <= 15.0]
    step5_no_asterdex = [s for s in step4_spread_15bps if s.get("follower_exchange", "").lower() != "asterdex"]
    step6_no_toxic_heavy = [s for s in step5_no_asterdex if s.get("coin") not in toxic and s.get("coin") not in heavy]
    step7_regime_no_dump = [s for s in step6_no_toxic_heavy if s.get("market_regime") not in ("FLASH_DUMP", "BEAR_DRIFT")]
    step8_full_korean_long = [s for s in step7_regime_no_dump if float(s.get("market_breadth_pct", 50.0) or 50.0) >= 50.0]

    pipeline_steps = [
        ("1. Сырой S1 (Все сигналы базы)", step0_all),
        ("2. ЭТАП А: Спот-фильтр (Score >= 0.55)", step1_spot_filter),
        ("3. ЭТАП Б: + Только LONG", step2_long_only),
        ("4.         + Swept >= $20,000", step3_swept_20k),
        ("5.         + Follower Spread <= 15 bps", step4_spread_15bps),
        ("6.         + Исключение AsterDex", step5_no_asterdex),
        ("7.         + Без токсичных токенов и мажоров", step6_no_toxic_heavy),
        ("8.         + Режим рынка: БЕЗ DUMP / BEAR", step7_regime_no_dump),
        ("9. ФИНАЛ: Спот-фильтр + Полный Корейский Лонг (Breadth >= 50%)", step8_full_korean_long),
    ]

    print("\n" + "=" * 105)
    print(f"{'ЭТАП ВОДОПАДА':<60} | {'Сделок':>6} | {'Win Rate':>8} | {'Total PnL':>12} | {'Avg PnL':>10} | {'PF':>6}")
    print("-" * 105)

    for name, sigs in pipeline_steps:
        n = len(sigs)
        if n == 0:
            print(f"{name:<60} | {0:>6} |        - |            - |          - |      -")
            continue
        pnls = [float(s.get("net_pnl_bps", 0) or 0) for s in sigs]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        wr = len(wins) / n * 100.0
        tot = sum(pnls)
        avg = tot / n
        gw = sum(wins)
        gl = abs(sum(losses))
        pf = (gw / gl) if gl > 0 else (99.0 if gw > 0 else 0.0)
        print(f"{name:<60} | {n:>6} | {wr:>7.1f}% | {tot:>+11.1f}b | {avg:>+9.2f}b | {pf:>6.2f}")

    print("=" * 105)

    # Let's also test Swept >= $10k variation
    step3_swept_10k = [s for s in step2_long_only if float(s.get("leader_swept_usd", 0) or 0) >= 10000.0]
    step4_10k = [s for s in step3_swept_10k if float(s.get("follower_spread_bps", 999) or 999) <= 15.0]
    step5_10k = [s for s in step4_10k if s.get("follower_exchange", "").lower() != "asterdex"]
    step6_10k = [s for s in step5_10k if s.get("coin") not in toxic and s.get("coin") not in heavy]
    step7_10k = [s for s in step6_10k if s.get("market_regime") not in ("FLASH_DUMP", "BEAR_DRIFT")]
    step8_10k = [s for s in step7_10k if float(s.get("market_breadth_pct", 50.0) or 50.0) >= 50.0]

    pnls_10k = [float(s.get("net_pnl_bps", 0) or 0) for s in step8_10k]
    wins_10k = [p for p in pnls_10k if p > 0]
    losses_10k = [p for p in pnls_10k if p < 0]
    wr_10k = len(wins_10k) / len(step8_10k) * 100 if step8_10k else 0
    tot_10k = sum(pnls_10k)
    avg_10k = tot_10k / len(step8_10k) if step8_10k else 0
    pf_10k = (sum(wins_10k) / abs(sum(losses_10k))) if losses_10k and sum(losses_10k) != 0 else 99.0

    print(f"\nВариант со Swept >= $10,000 (более широкий вход):")
    print(f"Сделок: {len(step8_10k)} | Win Rate: {wr_10k:.1f}% | Total PnL: {tot_10k:+.1f} bps | Avg: {avg_10k:+.2f} bps | PF: {pf_10k:.2f}")

    if step8_full_korean_long:
        print("\n" + "=" * 105)
        print(f"  ФИНАЛЬНЫЙ СПИСОК СДЕЛОК: СПОТ-ФИЛЬТР + КОРЕЙСКИЙ ЛОНГ (N={len(step8_full_korean_long)})")
        print("=" * 105)
        for i, s in enumerate(step8_full_korean_long, 1):
            coin = s.get("coin", "")
            venue = s.get("follower_exchange", "")
            score = s.get("_score", 0.0)
            swept = float(s.get("leader_swept_usd", 0) or 0)
            spread = float(s.get("follower_spread_bps", 0) or 0)
            pnl = float(s.get("net_pnl_bps", 0) or 0)
            reason = s.get("close_reason", "")
            dur = s.get("duration_sec", 0.0)
            reg = s.get("market_regime", "")
            br = s.get("market_breadth_pct", 0)
            print(f" {i:2d}. {coin:<8} @ {venue:<10} | Score={score:.3f} | Swept=${swept:>6.0f} | Spread={spread:4.1f}b | PnL={pnl:>+8.1f}b | Reason={reason:<16} | Dur={dur:4.1f}s | Reg={reg} ({br}%)")


if __name__ == "__main__":
    main()
