"""
tools/analyze_signals.py - Offline multi-strategy quantitative analyzer for Korean Sunrise signals.
Reads all compressed .jsonl.gz files from data/signals/ and produces comprehensive, actionable reports
across Strategy 1 (Lead-Lag Momentum), Strategy 2 (Fade the Fakeout), and Strategy 3 (Maker Liquidity).
"""
from __future__ import annotations

from collections import Counter, defaultdict
import glob
import gzip
import json
import os
import statistics
import sys
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DEFAULT_DATA_ROOT = os.path.join(PROJECT_ROOT, "data")


def load_signals(data_root: str = DEFAULT_DATA_ROOT) -> List[Dict[str, Any]]:
    pattern = os.path.join(data_root, "signals", "**", "*.jsonl.gz")
    files = sorted(glob.glob(pattern, recursive=True))
    signals: List[Dict[str, Any]] = []
    for fpath in files:
        try:
            with gzip.open(fpath, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            signals.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
        except Exception as e:
            print(f"[warn] Failed to read {fpath}: {e}", file=sys.stderr)
    return signals


def _mean(vals: List[float]) -> float:
    return statistics.mean(vals) if vals else 0.0


def _med(vals: List[float]) -> float:
    return statistics.median(vals) if vals else 0.0


def interpolate_markout(path: List[List[float]], target_dt: float) -> Tuple[float, float]:
    """
    Returns (f_mid_move_bps, net_exec_pnl_bps) at `target_dt` seconds from signal open.
    Supports both 5-column path [dt, up_move, f_mid_move, f_exec_pnl, rem_edge]
    and legacy 4-column path.
    """
    if not path:
        return 0.0, 0.0
    best_row = path[0]
    for row in path:
        if row[0] <= target_dt:
            best_row = row
        else:
            break
    if len(best_row) >= 5:
        return float(best_row[2]), float(best_row[3])
    return float(best_row[2]), float(best_row[2]) - 10.0


def main() -> None:
    data_root = os.getenv("DATA_ROOT", DEFAULT_DATA_ROOT)
    if not os.path.isabs(data_root):
        data_root = os.path.join(PROJECT_ROOT, data_root)

    signals = load_signals(data_root)
    if not signals:
        print(f"No signals found in '{data_root}/signals/'. Run main.py first to collect data.")
        return

    # Partition by strategy
    s1_signals = [s for s in signals if s.get("strategy") in ("lead_lag", None, "")]
    s2_signals = [s for s in signals if s.get("strategy") == "fade_fakeout"]
    s3_signals = [s for s in signals if s.get("strategy") == "maker_void"]

    print("=" * 105)
    print(f" KOREAN SUNRISE — MULTI-STRATEGY QUANTITATIVE REPORT & ATTRIBUTION (Signals: {len(signals):,})")
    print("=" * 105)

    # -------------------------------------------------------------------------
    # 1. Executive Multi-Strategy Scorecard
    # -------------------------------------------------------------------------
    def _strat_stats(rows: List[Dict[str, Any]], pos_size: float = 10.0, fee_bps: float = 10.0):
        n = len(rows)
        if n == 0:
            return {"trades": 0, "win_pct": 0.0, "mean_pnl": 0.0, "med_pnl": 0.0, "net_usd": 0.0, "pf": 0.0, "dur": 0.0}
        pnls = [float(r.get("net_pnl_bps", 0.0)) for r in rows]
        wins = sum(1 for p in pnls if p > 0.0)
        gains = sum(pos_size * (p / 1e4) for p in pnls if p > 0.0)
        losses = sum(abs(pos_size * (p / 1e4)) for p in pnls if p < 0.0)
        net_usd = sum(pos_size * (p / 1e4) for p in pnls)
        pf = round(gains / losses, 2) if losses > 0 else (99.0 if gains > 0 else 0.0)
        durs = [float(r.get("duration_sec", 0.0)) for r in rows if float(r.get("duration_sec", 0.0)) > 0]
        return {
            "trades": n,
            "win_pct": (wins / n * 100.0),
            "mean_pnl": _mean(pnls),
            "med_pnl": _med(pnls),
            "net_usd": net_usd,
            "pf": pf,
            "dur": _mean(durs),
        }

    st1 = _strat_stats(s1_signals)
    st2 = _strat_stats(s2_signals)
    s3_filled = [s for s in s3_signals if s.get("maker_filled", False)]
    st3 = _strat_stats(s3_filled)
    all_filled = s1_signals + s2_signals + s3_filled
    st_tot = _strat_stats(all_filled)

    print("\n[1] MULTI-STRATEGY CONSOLIDATED SCORECARD:")
    print(
        f"{'Strategy':<30} | {'Trades':>7} | {'Win%':>6} | {'Mean PnL':>11} | "
        f"{'Med PnL':>9} | {'Net PnL ($)':>11} | {'Profit Factor':>13} | {'Avg Dur':>8}"
    )
    print("-" * 105)
    print(
        f"{'1. Lead-Lag Momentum (Long)':<30} | {st1['trades']:>7d} | {st1['win_pct']:>5.1f}% | "
        f"{st1['mean_pnl']:>+10.2f}b | {st1['med_pnl']:>+8.2f}b | ${st1['net_usd']:>10.2f} | {st1['pf']:>13.2f} | {st1['dur']:>7.2f}s"
    )
    print(
        f"{'2. Fade the Fakeout (Short)':<30} | {st2['trades']:>7d} | {st2['win_pct']:>5.1f}% | "
        f"{st2['mean_pnl']:>+10.2f}b | {st2['med_pnl']:>+8.2f}b | ${st2['net_usd']:>10.2f} | {st2['pf']:>13.2f} | {st2['dur']:>7.2f}s"
    )
    maker_label = f"3. Maker Voids ({len(s3_filled)}/{len(s3_signals)})"
    print(
        f"{maker_label:<30} | {st3['trades']:>7d} | {st3['win_pct']:>5.1f}% | "
        f"{st3['mean_pnl']:>+10.2f}b | {st3['med_pnl']:>+8.2f}b | ${st3['net_usd']:>10.2f} | {st3['pf']:>13.2f} | {st3['dur']:>7.2f}s"
    )
    print("-" * 105)
    print(
        f"{'TOTAL MULTI-STRAT PORTFOLIO':<30} | {st_tot['trades']:>7d} | {st_tot['win_pct']:>5.1f}% | "
        f"{st_tot['mean_pnl']:>+10.2f}b | {st_tot['med_pnl']:>+8.2f}b | ${st_tot['net_usd']:>10.2f} | {st_tot['pf']:>13.2f} | {st_tot['dur']:>7.2f}s"
    )

    # -------------------------------------------------------------------------
    # 2. Strategy 1 Deep Dive: Venue Breakdown & Sub-second Markout
    # -------------------------------------------------------------------------
    if s1_signals:
        print("\n[2] STRATEGY 1: LEAD-LAG BREAKDOWN BY VENUE & TRIGGER:")
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for s in s1_signals:
            key = f"{s.get('follower_exchange', '?'):<7} | {s.get('trigger_type', '?'):<14}"
            groups[key].append(s)

        print(
            f"{'Venue   | Trigger':<24} | {'Count':>5} | {'Conv%':>5} | {'Win%':>5} | "
            f"{'MeanPnL':>8} | {'MedPnL':>7} | {'MedTop3':>8} | {'MedCap15':>8}"
        )
        print("-" * 92)
        for k, rows in sorted(groups.items()):
            n = len(rows)
            pnls = [float(r.get("net_pnl_bps", 0.0)) for r in rows]
            top3s = [float(r.get("follower_top3_ask_usd", 0.0)) for r in rows if float(r.get("follower_top3_ask_usd", 0.0)) > 0]
            caps = [
                float(r.get("stealth_capacity_usd") or (0.15 * float(r.get("follower_top3_ask_usd", 0.0))))
                for r in rows
                if float(r.get("follower_top3_ask_usd", 0.0)) > 0
            ]
            conv_pct = sum(1 for r in rows if r.get("close_reason") == "converged") / n * 100.0
            win_pct = sum(1 for p in pnls if p > 0.0) / n * 100.0
            print(
                f"{k:<24} | {n:>5d} | {conv_pct:>4.1f}% | {win_pct:>4.1f}% | "
                f"{_mean(pnls):>+7.2f} | {_med(pnls):>+6.2f} | ${_med(top3s):>7.0f} | ${_med(caps):>7.0f}"
            )

        # Markout curve
        horizons = [0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0]
        print("\n[3] STRATEGY 1: SUB-SECOND MARKOUT CURVE:")
        print(f"{'Horizon (sec)':<15} | {'Mean Mid Move (bps)':>20} | {'Mean Net Exec PnL (bps)':>24} | {'Exec Win%':>10}")
        print("-" * 78)
        for h in horizons:
            mid_moves = []
            exec_pnls = []
            for s in s1_signals:
                m_move, e_pnl = interpolate_markout(s.get("path", []), h)
                mid_moves.append(m_move)
                exec_pnls.append(e_pnl)
            w_pct = sum(1 for x in exec_pnls if x > 0.0) / len(exec_pnls) * 100.0 if exec_pnls else 0.0
            print(f"t = +{h:<10.2f}s | {_mean(mid_moves):>+19.2f} | {_mean(exec_pnls):>+23.2f} | {w_pct:>9.1f}%")

    # -------------------------------------------------------------------------
    # 3. Strategy 2 Deep Dive: Fade the Fakeout (Shorts)
    # -------------------------------------------------------------------------
    if s2_signals:
        print("\n[4] STRATEGY 2: FADE THE FAKEOUT (SHORTS) ATTRIBUTION:")
        f_reasons = defaultdict(list)
        for s in s2_signals:
            f_reasons[s.get("fade_reason", "unknown")].append(s)

        print(f"{'Fade Reason':<20} | {'Count':>5} | {'Win%':>6} | {'MeanPnL':>9} | {'MeanMFE':>9} | {'MeanMAE':>9} | {'Avg Dur':>8}")
        print("-" * 80)
        for fr, rows in sorted(f_reasons.items()):
            n = len(rows)
            pnls = [float(r.get("net_pnl_bps", 0.0)) for r in rows]
            mfes = [float(r.get("mfe_exec_pnl_bps", 0.0)) for r in rows]
            maes = [float(r.get("mae_exec_pnl_bps", 0.0)) for r in rows]
            durs = [float(r.get("duration_sec", 0.0)) for r in rows]
            win_pct = sum(1 for p in pnls if p > 0.0) / n * 100.0
            print(
                f"{fr:<20} | {n:>5d} | {win_pct:>5.1f}% | "
                f"{_mean(pnls):>+8.2f}b | {_mean(mfes):>+8.2f}b | {_mean(maes):>+8.2f}b | {_mean(durs):>7.2f}s"
            )

    # -------------------------------------------------------------------------
    # 4. Strategy 3 Deep Dive: Maker Liquidity Harvesting
    # -------------------------------------------------------------------------
    if s3_signals:
        print("\n[5] STRATEGY 3: MAKER LIQUIDITY HARVESTING:")
        fill_rate = len(s3_filled) / len(s3_signals) * 100.0 if s3_signals else 0.0
        print(f"Total Void Opportunities: {len(s3_signals)} | Filled: {len(s3_filled)} ({fill_rate:.1f}% fill rate)")
        if s3_filled:
            rebates = sum(float(r.get("maker_rebate_bps", 1.5)) for r in s3_filled)
            taker_fees = sum(float(r.get("taker_fee_bps", 5.0)) for r in s3_filled)
            sp_open = [float(r.get("spread_open_bps", 0.0)) for r in s3_filled]
            sp_close = [float(r.get("spread_close_bps", 0.0)) for r in s3_filled]
            print(f"Mean Open Spread: {_mean(sp_open):.1f} bps -> Mean Close Spread: {_mean(sp_close):.1f} bps (Compression: {_mean(sp_open) - _mean(sp_close):+.1f} bps)")
            print(f"Total Rebates Accrued: +{rebates:.1f} bps | Total Taker Exit Fees: -{taker_fees:.1f} bps")

    # -------------------------------------------------------------------------
    # 5. Cross-Strategy Coin Leaderboard
    # -------------------------------------------------------------------------
    coin_data = defaultdict(lambda: {
        "s1_pnl": 0.0, "s2_pnl": 0.0, "s3_pnl": 0.0,
        "s1_trades": 0, "s2_trades": 0, "s3_trades": 0,
        "total_trades": 0, "wins": 0, "total_pnl": 0.0
    })

    for s in s1_signals:
        c = s.get("coin", "UNKNOWN").upper()
        p = float(s.get("net_pnl_bps", 0.0))
        coin_data[c]["s1_pnl"] += p
        coin_data[c]["s1_trades"] += 1
        coin_data[c]["total_trades"] += 1
        coin_data[c]["total_pnl"] += p
        if p > 0:
            coin_data[c]["wins"] += 1

    for s in s2_signals:
        c = s.get("coin", "UNKNOWN").upper()
        p = float(s.get("net_pnl_bps", 0.0))
        coin_data[c]["s2_pnl"] += p
        coin_data[c]["s2_trades"] += 1
        coin_data[c]["total_trades"] += 1
        coin_data[c]["total_pnl"] += p
        if p > 0:
            coin_data[c]["wins"] += 1

    for s in s3_filled:
        c = s.get("coin", "UNKNOWN").upper()
        p = float(s.get("net_pnl_bps", 0.0))
        coin_data[c]["s3_pnl"] += p
        coin_data[c]["s3_trades"] += 1
        coin_data[c]["total_trades"] += 1
        coin_data[c]["total_pnl"] += p
        if p > 0:
            coin_data[c]["wins"] += 1

    ranked_coins = sorted(coin_data.items(), key=lambda x: x[1]["total_pnl"], reverse=True)

    print("\n[6] CROSS-STRATEGY COIN ATTRIBUTION MATRIX (Top 25):")
    print(
        f"{'Coin':<10} | {'Trades':>6} | {'Win%':>6} | {'S1 LeadLag':>11} | "
        f"{'S2 Fade':>10} | {'S3 Maker':>10} | {'Total PnL':>11} | {'Status':<18}"
    )
    print("-" * 96)
    for c, cd in ranked_coins[:25]:
        wr = cd["wins"] / cd["total_trades"] * 100.0 if cd["total_trades"] else 0.0
        if cd["total_pnl"] > 0 and cd["s1_pnl"] > 0:
            tag = "🚀 Momentum Alpha"
        elif cd["total_pnl"] > 0 and cd["s2_pnl"] > 0:
            tag = "🎯 Fade Goldmine"
        elif cd["total_pnl"] > 0 and cd["s3_pnl"] > 0:
            tag = "🧲 Maker Harvest"
        elif cd["total_pnl"] < -10.0:
            tag = "⚠️ Toxic Bleeder"
        else:
            tag = "Neutral"

        print(
            f"{c:<10} | {cd['total_trades']:>6d} | {wr:>5.1f}% | "
            f"{cd['s1_pnl']:>+10.1f}b | {cd['s2_pnl']:>+9.1f}b | {cd['s3_pnl']:>+9.1f}b | "
            f"{cd['total_pnl']:>+10.1f}b | {tag:<18}"
        )

    # -------------------------------------------------------------------------
    # 6. Actionable Strategy Recommendations & Blacklists
    # -------------------------------------------------------------------------
    fade_stars = [c for c, cd in ranked_coins if cd["s2_pnl"] > 5.0 and cd["s2_trades"] >= 2]
    toxic_coins = [c for c, cd in ranked_coins if cd["total_trades"] >= 3 and cd["total_pnl"] < -20.0 and cd["s2_pnl"] <= 0]

    if fade_stars:
        print("\n[⭐] HIGH CONVICTION FADE COINS (Consistently Fakeout on Upbit Sweeps):")
        print("  • " + ", ".join(fade_stars))

    if toxic_coins:
        print("\n[⛔] TOXIC COINS RECOMMENDED FOR EXCLUSION (Bleed Across All Strategies):")
        print("export EXCLUDE_COINS=\"BTC,ETH,USDT,USDC," + ",".join(sorted(toxic_coins)) + "\"")


if __name__ == "__main__":
    main()
