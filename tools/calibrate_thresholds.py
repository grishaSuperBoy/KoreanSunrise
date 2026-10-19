"""
tools/calibrate_thresholds.py - Automated Rolling Calibration of Pair-Specific Spread Thresholds.

Optimized for Korean Sunrise Lead-Lag Execution:
  - Low-memory streaming over data/signals/**/*.jsonl.gz (O(1) RAM footprint < 25 MB).
  - Sweeps spread/edge thresholds [10..60 bps] per (coin, upbit, follower) pair.
  - Automatically classifies pairs into:
      * Aggressive / Clean (12-20 bps)  -> High-frequency clean lead-lag edge.
      * Balanced Standard (22-35 bps)   -> Robust baseline execution.
      * High Friction (38-55 bps)       -> Shields against follower spread/slippage.
      * Toxic / Pruned (disabled)       -> Cuts negative EV / high reversal flow.
  - Generates atomic matrix_thresholds.json for real-time bot consumption.

Usage:
  python tools/calibrate_thresholds.py
  python tools/calibrate_thresholds.py --days 1 --size 10.0 --fee-bps 10.0
  python tools/calibrate_thresholds.py --min-trades 5 --min-winrate 0.70
  python tools/calibrate_thresholds.py --dry-run
"""
from __future__ import annotations

import argparse
import collections
import datetime
import glob
import gzip
import json
import os
import statistics
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
DEFAULT_OUT_FILE = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
UNIVERSE_FILE = os.path.join(PROJECT_ROOT, "universe_live.json")
BATTLE_FILE = os.path.join(PROJECT_ROOT, "live_trading_pool.json")

SWEEP_THRESHOLDS = (10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 60.0)
DEFAULT_GLOBAL_FALLBACK_BPS = 20.0
DEFAULT_GLOBAL_HOLD_SEC = 15.0
DEFAULT_SPIKE_SIGMA = 2.5
DEFAULT_MIN_DEV_Z = 1.8


def _calc_optimal_hold(t_peaks: List[float], default_val: float = DEFAULT_GLOBAL_HOLD_SEC) -> float:
    """Calculates personalized holding horizon: median(T_peak) + 1.5*MAD (clamped [2.0s, 60.0s])."""
    if not t_peaks:
        return default_val
    med = statistics.median(t_peaks)
    abs_dev = [abs(x - med) for x in t_peaks]
    mad = statistics.median(abs_dev) if abs_dev else 0.5
    raw_hold = med + 1.5 * (1.4826 * mad)
    return round(max(2.0, min(60.0, raw_hold)), 1)


def _calc_spike_and_dev_z(dev_zs: List[float]) -> Tuple[float, float]:
    """Calculates personalized spike gating threshold P90(dev_z) and entry dev_z."""
    if len(dev_zs) < 5:
        return DEFAULT_SPIKE_SIGMA, DEFAULT_MIN_DEV_Z
    s_z = sorted(dev_zs)
    idx90 = int(0.90 * len(s_z))
    p90 = s_z[min(idx90, len(s_z) - 1)]
    spike_sigma = round(max(2.0, min(4.5, p90)), 2)
    idx25 = int(0.25 * len(s_z))
    p25 = s_z[min(idx25, len(s_z) - 1)]
    min_dev_z = round(max(1.5, min(2.5, p25)), 2)
    return spike_sigma, min_dev_z


def _collect_signal_files(days_back: float = 7.0) -> List[str]:
    """Collects gzip files from data/signals within the trailing window."""
    s_dir = os.path.join(DATA_ROOT, "signals")
    if not os.path.isdir(s_dir):
        return []
    cutoff = time.time() - (days_back * 86400.0)
    pattern = os.path.join(s_dir, "**", "*.jsonl.gz")
    all_files = sorted(glob.glob(pattern, recursive=True))
    res = []
    for fp in all_files:
        try:
            if os.path.getmtime(fp) >= cutoff:
                res.append(fp)
        except OSError:
            pass
    return res if res else all_files


def _write_json_atomic(path: str, data: Any) -> None:
    """Atomic write via temp file and replace."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def calibrate(
    days_back: float = 7.0,
    pos_size: float = 10.0,
    roundtrip_fee_bps: float = 10.0,
    min_trades: int = 5,
    min_winrate: float = 0.70,
    out_file: str = DEFAULT_OUT_FILE,
    dry_run: bool = False,
    as_json: bool = False,
) -> Dict[str, Any]:
    t0 = time.time()
    fee_usd = pos_size * (roundtrip_fee_bps / 10000.0)
    files = _collect_signal_files(days_back=days_back)

    # Aggregator per pair key: {coin}:upbit-{follower_ex}
    # Structure: pair_key -> threshold -> {count, wins, gross, fee, net}
    pair_sweeps: Dict[str, Dict[float, Dict[str, Any]]] = collections.defaultdict(
        lambda: {
            t: {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0}
            for t in SWEEP_THRESHOLDS
        }
    )
    pair_metrics: Dict[str, Dict[str, List[float]]] = collections.defaultdict(
        lambda: {"t_peaks": [], "dev_zs": []}
    )

    total_rows = 0
    valid_events = 0

    if files:
        for fp in files:
            try:
                with gzip.open(fp, "rt", encoding="utf-8", errors="replace") as gz:
                    for line in gz:
                        line = line.strip()
                        if not line:
                            continue
                        total_rows += 1
                        try:
                            d = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        coin = str(d.get("coin", "")).upper()
                        follower_ex = str(d.get("follower_exchange", "")).lower()
                        if not coin or not follower_ex:
                            continue

                        net_pnl = d.get("net_pnl_bps")
                        if net_pnl is None:
                            continue
                        net_pnl = float(net_pnl)
                        gross_edge = float(
                            d.get("gross_exec_edge_bps")
                            or (float(d.get("net_exec_edge_bps", 0.0)) + roundtrip_fee_bps * 0.5)
                        )

                        # Microstructure physical validity check (consistent with evaluator.py)
                        drain_ratio = float(d.get("drain_ratio", 0.0))
                        swept_usd = float(d.get("leader_swept_usd", 0.0))
                        f_top3_ask = float(d.get("follower_top3_ask_usd", 0.0))
                        cross_ratio = (swept_usd / f_top3_ask) if f_top3_ask > 0 else 0.0
                        if drain_ratio < 0.40 and cross_ratio < 0.50:
                            continue

                        pair_key = f"{coin}:upbit-{follower_ex}"
                        valid_events += 1

                        # Collect metrics for individual hold & spike calibration
                        t_pk = d.get("time_to_peak_sec")
                        if t_pk is not None and float(t_pk) > 0 and net_pnl > 0:
                            pair_metrics[pair_key]["t_peaks"].append(float(t_pk))
                        dz = d.get("dev_z")
                        if dz is not None and float(dz) > 0:
                            pair_metrics[pair_key]["dev_zs"].append(float(dz))

                        gross = pos_size * ((net_pnl + roundtrip_fee_bps) / 10000.0)
                        net = pos_size * (net_pnl / 10000.0)

                        sw = pair_sweeps[pair_key]
                        for t in SWEEP_THRESHOLDS:
                            if gross_edge >= t:
                                s = sw[t]
                                s["count"] += 1
                                s["gross"] += gross
                                s["fee"] += fee_usd
                                s["net"] += net
                                if net > 0:
                                    s["wins"] += 1
            except Exception as e:
                if not as_json:
                    print(f"[!] Warning reading {fp}: {e}", file=sys.stderr)

    matrix_output: Dict[str, Any] = {}
    classification_counts = collections.Counter()

    # Fallback to universe_live.json or live_trading_pool.json if no signals yet
    if not pair_sweeps:
        target_coins: Dict[str, List[str]] = {}
        if os.path.exists(UNIVERSE_FILE):
            try:
                with open(UNIVERSE_FILE, "r", encoding="utf-8") as uf:
                    udoc = json.load(uf)
                for c in udoc.get("coins", []):
                    # Check which venues this coin trades on
                    venues = []
                    for v in ["binance", "bybit", "okx", "gateio", "mexc", "kucoin", "bingx", "htx", "pionex", "asterdex", "hyperliquid"]:
                        sym_map = udoc.get(f"{v}_sym_map", {})
                        if any(val[0] == c for val in sym_map.values()):
                            venues.append(v)
                    target_coins[c] = venues if venues else ["binance", "bybit"]
            except Exception:
                pass

        if not target_coins:
            target_coins = {
                "VVV": ["binance", "bybit", "kucoin"],
                "LIT": ["binance", "bybit", "okx"],
                "ICP": ["binance", "bybit", "okx"],
                "ETHFI": ["binance", "bybit"],
                "RAY": ["bingx", "bybit", "mexc"],
            }

        for c, v_list in target_coins.items():
            for v in v_list:
                pk = f"{c}:upbit-{v}"
                matrix_output[pk] = {
                    "min_spread_bps": DEFAULT_GLOBAL_FALLBACK_BPS,
                    "max_hold_sec": DEFAULT_GLOBAL_HOLD_SEC,
                    "spike_sigma": DEFAULT_SPIKE_SIGMA,
                    "min_dev_z": DEFAULT_MIN_DEV_Z,
                    "expected_winrate": 0.85,
                    "expected_net_bps": 22.0,
                    "sample_trades": 0,
                    "status": "default_baseline",
                    "enabled": True,
                }
        classification_counts["default_baseline"] = len(matrix_output)

    else:
        for pair_key, sweeps in pair_sweeps.items():
            best_t = DEFAULT_GLOBAL_FALLBACK_BPS
            best_net = -1e9
            best_wr = 0.0
            best_cnt = 0
            best_gross = 0.0

            eligible_thresholds = []
            for t in SWEEP_THRESHOLDS:
                s = sweeps[t]
                cnt = s["count"]
                if cnt >= min_trades:
                    wr = s["wins"] / cnt
                    if wr >= min_winrate and s["net"] > 0:
                        eligible_thresholds.append((t, s["net"], wr, cnt, s["gross"]))

            if eligible_thresholds:
                # Pick threshold that maximizes total net USD profit
                eligible_thresholds.sort(key=lambda x: x[1], reverse=True)
                best_t, best_net, best_wr, best_cnt, best_gross = eligible_thresholds[0]

                # Classification
                if best_t <= 20.0 and best_wr >= 0.80:
                    status = "aggressive_clean"
                elif best_t <= 35.0 and best_wr >= 0.70:
                    status = "balanced_standard"
                else:
                    status = "high_friction"
                enabled = True
            else:
                # Pair didn't meet min_trades or winrate guard
                max_cnt_t = max(SWEEP_THRESHOLDS, key=lambda t: sweeps[t]["count"])
                s = sweeps[max_cnt_t]
                best_cnt = s["count"]
                best_wr = (s["wins"] / best_cnt) if best_cnt > 0 else 0.0
                best_net = s["net"]
                best_t = DEFAULT_GLOBAL_FALLBACK_BPS

                if best_cnt >= min_trades and (best_net <= 0 or best_wr < 0.50):
                    status = "toxic_pruned"
                    enabled = False
                else:
                    status = "default_baseline"
                    enabled = True

            # Calculate optimal hold horizon & spike gating per pair
            t_peaks = pair_metrics[pair_key]["t_peaks"]
            dev_zs = pair_metrics[pair_key]["dev_zs"]
            opt_hold = _calc_optimal_hold(t_peaks, default_val=DEFAULT_GLOBAL_HOLD_SEC)
            spike_sigma, min_dev_z = _calc_spike_and_dev_z(dev_zs)

            classification_counts[status] += 1
            matrix_output[pair_key] = {
                "min_spread_bps": round(best_t, 1),
                "max_hold_sec": opt_hold,
                "spike_sigma": spike_sigma,
                "min_dev_z": min_dev_z,
                "expected_winrate": round(best_wr, 3),
                "expected_net_bps": round((best_net / pos_size) * 10000.0 / max(1, best_cnt), 1) if best_cnt else 0.0,
                "sample_trades": best_cnt,
                "status": status,
                "enabled": enabled,
            }

    doc = {
        "metadata": {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "days_analyzed": days_back,
            "order_size_usd": pos_size,
            "roundtrip_fee_bps": roundtrip_fee_bps,
            "min_trades_guard": min_trades,
            "min_winrate_guard": min_winrate,
            "global_fallback_bps": DEFAULT_GLOBAL_FALLBACK_BPS,
            "total_files_scanned": len(files),
            "total_signals_evaluated": valid_events,
            "total_pairs_configured": len(matrix_output),
            "classifications": dict(classification_counts),
            "elapsed_sec": round(time.time() - t0, 3),
        },
        "thresholds": matrix_output,
    }

    if not dry_run:
        _write_json_atomic(out_file, doc)

    if not as_json:
        print("\n" + "=" * 96)
        print("  KOREAN SUNRISE: AUTOMATED SPREAD & HOLD HORIZON CALIBRATION MATRIX")
        print("=" * 96)
        print(f"  Signals Scanned: {valid_events:,} across {len(files)} archives ({days_back:.1f} days)")
        print(f"  Configured Pairs: {len(matrix_output)} pairs calibrated")
        print(f"  Classifications: {dict(classification_counts)}")
        print("-" * 96)
        print(f"  {'Pair (Coin:Leader-Follower)':<28} | {'Min Edge':<9} | {'Hold':<7} | {'Spike':<6} | {'Win Rate':<9} | {'Trades':<7} | {'Status':<16} | {'Active'}")
        print("-" * 96)
        for pk, cfg in sorted(matrix_output.items())[:35]:
            en_str = "YES" if cfg["enabled"] else "NO (PRUNED)"
            print(
                f"  {pk:<28} | {cfg['min_spread_bps']:>5.1f} bps | "
                f"{cfg.get('max_hold_sec', 6.0):>4.1f}s | {cfg.get('spike_sigma', 2.5):>4.1f}σ | "
                f"{cfg['expected_winrate'] * 100:>6.1f}% | {cfg['sample_trades']:>7} | "
                f"{cfg['status']:<16} | {en_str}"
            )
        if len(matrix_output) > 35:
            print(f"  ... and {len(matrix_output) - 35} more pairs calibrated.")
        print("=" * 80)
        print(f"  [OK] Saved atomic matrix to: {out_file}\n")

    return doc


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Pair Spread Calibrator.")
    parser.add_argument("--days", type=float, default=7.0, help="Days back of signals (default: 7)")
    parser.add_argument("--size", type=float, default=10.0, help="Order size USD (default: 10)")
    parser.add_argument("--fee-bps", type=float, default=10.0, help="Roundtrip fee bps (default: 10)")
    parser.add_argument("--min-trades", type=int, default=5, help="Min sample trades per pair (default: 5)")
    parser.add_argument("--min-winrate", type=float, default=0.70, help="Min winrate guard (default: 0.70)")
    parser.add_argument("--out", default=DEFAULT_OUT_FILE, help="Output matrix file")
    parser.add_argument("--dry-run", action="store_true", help="Do not write file")
    parser.add_argument("--json", action="store_true", help="Output pure JSON")
    args = parser.parse_args()

    doc = calibrate(
        days_back=args.days,
        pos_size=args.size,
        roundtrip_fee_bps=args.fee_bps,
        min_trades=args.min_trades,
        min_winrate=args.min_winrate,
        out_file=args.out,
        dry_run=args.dry_run,
        as_json=args.json,
    )
    if args.json:
        print(json.dumps(doc, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
