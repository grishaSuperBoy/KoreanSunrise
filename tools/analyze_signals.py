"""
tools/analyze_signals.py - Offline analyzer & coin filter for Korean Sunrise signals.
Reads all compressed .jsonl.gz files from data/signals/ and prints actionable reports.
"""
from __future__ import annotations

from collections import defaultdict
import glob
import gzip
import json
import os
import statistics
import sys
from typing import Any, Dict, List

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
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
                        signals.append(json.loads(line))
        except Exception as e:
            print(f"[warn] Failed to read {fpath}: {e}", file=sys.stderr)
    return signals


def _mean(vals: List[float]) -> float:
    return statistics.mean(vals) if vals else 0.0


def _med(vals: List[float]) -> float:
    return statistics.median(vals) if vals else 0.0


def interpolate_markout(path: List[List[float]], target_dt: float) -> tuple[float, float]:
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

    print("=" * 88)
    print(f" KOREAN SUNRISE — POST-TRADE SIGNAL ANALYSIS (Total Signals: {len(signals)})")
    print("=" * 88)

    # 1. Breakdown by Exchange & Trigger Type
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for s in signals:
        key = f"{s.get('follower_exchange', '?'):<7} | {s.get('trigger_type', '?'):<14}"
        groups[key].append(s)

    print("\n[1] BREAKDOWN BY FOLLOWER EXCHANGE & TRIGGER TYPE:")
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

    # 2. Sub-second Markout Curve across horizons
    horizons = [0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0]
    print("\n[2] SUB-SECOND MARKOUT CURVE (Across All Signals):")
    print(f"{'Horizon (sec)':<15} | {'Mean Mid Move (bps)':>20} | {'Mean Net Exec PnL (bps)':>24} | {'Exec Win%':>10}")
    print("-" * 78)
    for h in horizons:
        mid_moves = []
        exec_pnls = []
        for s in signals:
            m_move, e_pnl = interpolate_markout(s.get("path", []), h)
            mid_moves.append(m_move)
            exec_pnls.append(e_pnl)
        w_pct = sum(1 for x in exec_pnls if x > 0.0) / len(exec_pnls) * 100.0
        print(f"t = +{h:<10.2f}s | {_mean(mid_moves):>+19.2f} | {_mean(exec_pnls):>+23.2f} | {w_pct:>9.1f}%")

    # 3. Impact of Upbit Swept USD Volume (Filtering out retail noise)
    buckets = [
        ("< $1,000", 0.0, 1000.0),
        ("$1,000 - $5,000", 1000.0, 5000.0),
        ("$5,000 - $20,000", 5000.0, 20000.0),
        ("> $20,000", 20000.0, 1e12),
    ]
    print("\n[3] FILTER BY UPBIT SWEPT VOLUME (leader_swept_usd):")
    print(f"{'Upbit Swept USD':<20} | {'Count':>5} | {'Win%':>6} | {'MeanPnL':>8} | {'MeanMFE':>8} | {'Rev% (Fakeout)':>15}")
    print("-" * 74)
    for label, lo, hi in buckets:
        sub = [s for s in signals if lo <= float(s.get("leader_swept_usd", 0.0)) < hi]
        if not sub:
            continue
        pnls = [float(r.get("net_pnl_bps", 0.0)) for r in sub]
        mfes = [float(r.get("mfe_exec_pnl_bps", 0.0)) for r in sub]
        win_pct = sum(1 for p in pnls if p > 0.0) / len(sub) * 100.0
        rev_pct = sum(1 for r in sub if r.get("close_reason") == "reversed") / len(sub) * 100.0
        print(
            f"{label:<20} | {len(sub):>5d} | {win_pct:>5.1f}% | "
            f"{_mean(pnls):>+7.2f} | {_mean(mfes):>+7.2f} | {rev_pct:>14.1f}%"
        )

    # 4. Coin Ranking (Whitelist vs Toxic Shitcoins)
    by_coin: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for s in signals:
        by_coin[s.get("coin", "UNKNOWN")].append(s)

    coin_stats = []
    for coin, rows in by_coin.items():
        n = len(rows)
        pnls = [float(r.get("net_pnl_bps", 0.0)) for r in rows]
        mfes = [float(r.get("mfe_exec_pnl_bps", 0.0)) for r in rows]
        spreads = [float(r.get("follower_spread_bps", 0.0)) for r in rows]
        top3s = [float(r.get("follower_top3_ask_usd", 0.0)) for r in rows if float(r.get("follower_top3_ask_usd", 0.0)) > 0]
        caps = [
            float(r.get("stealth_capacity_usd") or (0.15 * float(r.get("follower_top3_ask_usd", 0.0))))
            for r in rows
            if float(r.get("follower_top3_ask_usd", 0.0)) > 0
        ]
        win_pct = sum(1 for p in pnls if p > 0.0) / n * 100.0
        mfe_pos_pct = sum(1 for m in mfes if m > 2.0) / n * 100.0
        coin_stats.append({
            "coin": coin,
            "n": n,
            "win_pct": win_pct,
            "mfe_pos_pct": mfe_pos_pct,
            "mean_pnl": _mean(pnls),
            "sum_pnl": sum(pnls),
            "med_top3": _med(top3s),
            "med_cap": _med(caps),
            "mean_mfe": _mean(mfes),
            "mean_spread": _mean(spreads),
        })

    coin_stats.sort(key=lambda x: x["mean_pnl"], reverse=True)

    print("\n[4] COIN PERFORMANCE RANKING (Sorted by Mean Realized Net PnL):")
    print(
        f"{'Coin':<10} | {'Trades':>6} | {'Win%':>6} | "
        f"{'MeanPnL':>8} | {'MedTop3':>8} | {'MedCap15':>8} | {'MeanMFE':>8} | {'F_Spread':>8}"
    )
    print("-" * 92)
    for st in coin_stats:
        print(
            f"{st['coin']:<10} | {st['n']:>6d} | {st['win_pct']:>5.1f}% | "
            f"{st['mean_pnl']:>+7.2f} | ${st['med_top3']:>7.0f} | ${st['med_cap']:>7.0f} | "
            f"{st['mean_mfe']:>+7.2f} | {st['mean_spread']:>7.2f}"
        )

    # Suggest automatic blacklist for coins with >= 3 signals and negative MFE/PnL
    toxic_coins = [
        st["coin"]
        for st in coin_stats
        if st["n"] >= 3 and st["mean_pnl"] < -3.0 and st["mean_mfe"] < 2.0
    ]
    if toxic_coins:
        print("\n[!] SUGGESTED TOXIC COINS TO ADD TO EXCLUDE_COINS:")
        print("export EXCLUDE_COINS=\"BTC,ETH,USDT,USDC," + ",".join(sorted(toxic_coins)) + "\"")


if __name__ == "__main__":
    main()
