"""
tools/full_daily_report.py - High-speed, comprehensive quantitative report generator for Korean Sunrise.
Optimized for low-resource VPS and multi-strategy real-time production analytics.

Multi-Strategy Architecture:
  - Strategy 1: Korean Sunrise Momentum (Upbit Spot Sweep Lead-Lag -> Long Follower Perp)
  - Strategy 2: Fade the Fakeout (Short Follower Perp on MM Quote Pullbacks & False Sweeps)
  - Strategy 3: Maker Liquidity Harvesting (Passive Post-Only Liquidity on Blown Follower Spreads)
  - Cross-Strategy Portfolio Consolidation & Coin Alpha Matrix

Usage:
  python tools/full_daily_report.py
  python tools/full_daily_report.py --days 1 --size 10.0 --fee-bps 10.0
  python tools/full_daily_report.py --json
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
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports", "daily")
BATTLE_FILE = os.path.join(PROJECT_ROOT, "live_trading_pool.json")
UNIVERSE_FILE = os.path.join(PROJECT_ROOT, "universe_live.json")
THRESHOLDS_FILE = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")

from config import ALL_SUPPORTED_VENUES

SWEEP_THRESHOLDS = (10, 15, 20, 25, 30, 35, 40, 50, 60)
FUNDED_VENUES = set(ALL_SUPPORTED_VENUES)


def _collect_files(days_back: float = 1.0) -> List[str]:
    s_dir = os.path.join(DATA_ROOT, "signals")
    if not os.path.isdir(s_dir):
        return []
    pattern = os.path.join(s_dir, "**", "*.jsonl.gz")
    all_files = sorted(glob.glob(pattern, recursive=True))
    if days_back is None or days_back <= 0:
        return all_files
    cutoff = time.time() - (days_back * 86400.0)
    return [fp for fp in all_files if os.path.getmtime(fp) >= cutoff]


def _safe_pf(gains: float, losses: float) -> float:
    if losses > 0.0:
        return round(gains / losses, 2)
    return 99.0 if gains > 0.0 else 0.0


def generate_daily_report(
    days_back: float = 1.0,
    pos_size: float = 10.0,
    roundtrip_fee_bps: float = 10.0,
    as_json: bool = False,
) -> Dict[str, Any]:
    t0 = time.time()
    today_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    os.makedirs(REPORTS_DIR, exist_ok=True)

    fee_usd = pos_size * (roundtrip_fee_bps / 10000.0)
    files = _collect_files(days_back=days_back)

    # 1. Load battle coins
    battle_coins: Set[str] = set()
    if os.path.exists(BATTLE_FILE):
        try:
            with open(BATTLE_FILE, "r", encoding="utf-8") as bf:
                bdoc = json.load(bf)
                coins_map = bdoc.get("coins") or bdoc.get("battle_pool") or {}
                battle_coins = {
                    k.upper() for k, v in coins_map.items()
                    if isinstance(v, dict) and v.get("enabled", True)
                }
        except Exception:
            pass

    # 2. Load matrix thresholds
    calibrated_thresholds: Dict[str, float] = {}
    if os.path.exists(THRESHOLDS_FILE):
        try:
            with open(THRESHOLDS_FILE, "r", encoding="utf-8") as tf:
                tdoc = json.load(tf)
                for pk, cfg in tdoc.get("thresholds", {}).items():
                    if cfg.get("enabled", True):
                        calibrated_thresholds[pk] = float(cfg.get("min_spread_bps", 20.0))
        except Exception:
            pass

    # Global Telemetry Counters
    total_signals = 0
    total_bytes = sum(os.path.getsize(f) for f in files) if files else 0
    venues_seen: Set[str] = set()
    coins_seen: Set[str] = set()

    # -------------------------------------------------------------------------
    # STRATEGY 1 ACCUMULATORS: Lead-Lag Momentum
    # -------------------------------------------------------------------------
    s1_signals = 0
    s1_close_reasons = collections.Counter()
    s1_trigger_types = collections.Counter()
    s1_stop_stats = {
        "total_stops": 0,
        "invalidation": 0,
        "adverse_pnl": 0,
        "alpha_decay": 0,
        "other": 0,
        "total_stop_pnl_usd": 0.0,
    }

    def _new_bucket() -> Dict[str, Any]:
        return {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0, "gains": 0.0, "losses": 0.0}

    s1_scenarios = {
        "all_signals": _new_bucket(),
        "calibrated_threshold_only": _new_bucket(),
        "battle_coins_only": _new_bucket(),
        "funded_venues_only": _new_bucket(),
    }
    s1_sweep = {t: {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0} for t in SWEEP_THRESHOLDS}
    s1_pair_stats = collections.defaultdict(lambda: {
        "count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0,
        "top3_depths": [], "stealth_caps": []
    })
    s1_coin_stats = collections.defaultdict(lambda: {
        "count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0,
        "mfes": [], "t_peaks": [], "top3_depths": [], "stealth_caps": []
    })
    venue_capacities = collections.defaultdict(lambda: {"count": 0, "top3_depths": [], "stealth_caps": []})

    # -------------------------------------------------------------------------
    # STRATEGY 2 ACCUMULATORS: Fade the Fakeout
    # -------------------------------------------------------------------------
    s2_signals = 0
    s2_bucket = {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0, "gains": 0.0, "losses": 0.0}
    s2_fade_reasons = collections.Counter()
    s2_close_reasons = collections.Counter()
    s2_durations: List[float] = []
    s2_mfes: List[float] = []
    s2_maes: List[float] = []
    s2_coin_stats = collections.defaultdict(lambda: {"count": 0, "wins": 0, "net": 0.0, "gross": 0.0, "mfes": []})
    s2_venue_stats = collections.defaultdict(lambda: {"count": 0, "wins": 0, "net": 0.0})

    # -------------------------------------------------------------------------
    # STRATEGY 3 ACCUMULATORS: Maker Liquidity Harvesting
    # -------------------------------------------------------------------------
    s3_opportunities = 0
    s3_filled_count = 0
    s3_unfilled_count = 0
    s3_bucket = {"count": 0, "wins": 0, "gross": 0.0, "rebates": 0.0, "taker_fees": 0.0, "net": 0.0, "gains": 0.0, "losses": 0.0}
    s3_sides = collections.Counter()
    s3_close_reasons = collections.Counter()
    s3_spread_open: List[float] = []
    s3_spread_close: List[float] = []
    s3_coin_stats = collections.defaultdict(lambda: {"opps": 0, "filled": 0, "wins": 0, "net": 0.0})
    s3_venue_stats = collections.defaultdict(lambda: {"opps": 0, "filled": 0, "wins": 0, "net": 0.0})

    # -------------------------------------------------------------------------
    # CROSS-STRATEGY CONSOLIDATION
    # -------------------------------------------------------------------------
    coin_cross_stats = collections.defaultdict(lambda: {
        "s1_net": 0.0, "s2_net": 0.0, "s3_net": 0.0, "total_net": 0.0,
        "s1_trades": 0, "s2_trades": 0, "s3_trades": 0, "total_trades": 0, "wins": 0
    })
    venue_cross_stats = collections.defaultdict(lambda: {
        "s1_net": 0.0, "s2_net": 0.0, "s3_net": 0.0, "total_net": 0.0,
        "s1_trades": 0, "s2_trades": 0, "s3_trades": 0, "total_trades": 0, "wins": 0
    })

    # -------------------------------------------------------------------------
    # STREAMING INGESTION OVER FILES
    # -------------------------------------------------------------------------
    for fpath in files:
        try:
            with gzip.open(fpath, "rt", encoding="utf-8", errors="replace") as gz:
                for line in gz:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    total_signals += 1
                    coin = str(d.get("coin", "")).upper()
                    f_ex = str(d.get("follower_exchange", "")).lower()
                    if not coin or not f_ex:
                        continue

                    coins_seen.add(coin)
                    venues_seen.add(f_ex)

                    strat = d.get("strategy") or "lead_lag"
                    net_pnl_bps = d.get("net_pnl_bps")
                    if net_pnl_bps is None:
                        continue
                    net_pnl_bps = float(net_pnl_bps)

                    # =========================================================
                    # ROUTE: Strategy 1 (Lead-Lag Sweep Momentum)
                    # =========================================================
                    if strat == "lead_lag":
                        s1_signals += 1
                        reason = d.get("close_reason", "unknown")
                        s1_close_reasons[reason] += 1
                        trig = d.get("trigger_type", "unknown")
                        s1_trigger_types[trig] += 1

                        stop_hit = d.get("stop_hit", False)
                        stop_type = d.get("stop_type")
                        if not stop_hit and reason.startswith("stop_"):
                            stop_hit = True
                            stop_type = reason.replace("stop_", "")

                        gross = pos_size * ((net_pnl_bps + roundtrip_fee_bps) / 10000.0)
                        net = pos_size * (net_pnl_bps / 10000.0)
                        is_win = 1 if net > 0 else 0

                        if stop_hit:
                            s1_stop_stats["total_stops"] += 1
                            s1_stop_stats["total_stop_pnl_usd"] += net
                            st_key = str(stop_type or "other").lower()
                            if st_key in ("invalidation", "adverse_pnl", "alpha_decay"):
                                s1_stop_stats[st_key] += 1
                            else:
                                s1_stop_stats["other"] += 1

                        def _add_s1_bucket(b: Dict[str, Any]) -> None:
                            b["count"] += 1
                            b["gross"] += gross
                            b["fee"] += fee_usd
                            b["net"] += net
                            b["wins"] += is_win
                            if net > 0:
                                b["gains"] += net
                            elif net < 0:
                                b["losses"] += abs(net)

                        _add_s1_bucket(s1_scenarios["all_signals"])

                        # Cross-strategy tracking
                        cx = coin_cross_stats[coin]
                        cx["s1_net"] += net
                        cx["total_net"] += net
                        cx["s1_trades"] += 1
                        cx["total_trades"] += 1
                        cx["wins"] += is_win

                        vx = venue_cross_stats[f_ex]
                        vx["s1_net"] += net
                        vx["total_net"] += net
                        vx["s1_trades"] += 1
                        vx["total_trades"] += 1
                        vx["wins"] += is_win

                        # Microstructure physical validity check
                        drain_ratio = float(d.get("drain_ratio", 0.0))
                        swept_usd = float(d.get("leader_swept_usd", 0.0))
                        f_top3_ask = float(d.get("follower_top3_ask_usd", 0.0))
                        cross_ratio = (swept_usd / f_top3_ask) if f_top3_ask > 0 else 0.0
                        if drain_ratio < 0.30 or cross_ratio < 0.20:
                            continue

                        stealth_cap = float(d.get("stealth_capacity_usd", 0.0))
                        if stealth_cap <= 0.0 and f_top3_ask > 0.0:
                            stealth_cap = round(0.15 * f_top3_ask, 2)

                        pk = f"{coin}:upbit-{f_ex}"
                        gross_edge_bps = float(
                            d.get("gross_exec_edge_bps")
                            or (float(d.get("net_exec_edge_bps", 0.0)) + roundtrip_fee_bps * 0.5)
                        )
                        req_edge = calibrated_thresholds.get(pk, 20.0)
                        if gross_edge_bps >= req_edge:
                            _add_s1_bucket(s1_scenarios["calibrated_threshold_only"])
                        if coin in battle_coins:
                            _add_s1_bucket(s1_scenarios["battle_coins_only"])
                        if f_ex in FUNDED_VENUES:
                            _add_s1_bucket(s1_scenarios["funded_venues_only"])

                        # Threshold sweep
                        for t in SWEEP_THRESHOLDS:
                            if gross_edge_bps >= t:
                                sw = s1_sweep[t]
                                sw["count"] += 1
                                sw["gross"] += gross
                                sw["fee"] += fee_usd
                                sw["net"] += net
                                sw["wins"] += is_win

                        # Pair stats
                        ps = s1_pair_stats[pk]
                        ps["count"] += 1
                        ps["gross"] += gross
                        ps["fee"] += fee_usd
                        ps["net"] += net
                        ps["wins"] += is_win
                        if f_top3_ask > 0:
                            ps["top3_depths"].append(f_top3_ask)
                            ps["stealth_caps"].append(stealth_cap)

                        # Coin stats
                        cs = s1_coin_stats[coin]
                        cs["count"] += 1
                        cs["gross"] += gross
                        cs["fee"] += fee_usd
                        cs["net"] += net
                        cs["wins"] += is_win
                        if f_top3_ask > 0:
                            cs["top3_depths"].append(f_top3_ask)
                            cs["stealth_caps"].append(stealth_cap)
                        mfe = d.get("mfe_exec_pnl_bps")
                        if mfe is not None:
                            cs["mfes"].append(float(mfe))
                        t_pk = d.get("time_to_peak_sec")
                        if t_pk is not None:
                            cs["t_peaks"].append(float(t_pk))

                        # Venue capacity
                        if f_top3_ask > 0:
                            vc = venue_capacities[f_ex]
                            vc["count"] += 1
                            vc["top3_depths"].append(f_top3_ask)
                            vc["stealth_caps"].append(stealth_cap)

                    # =========================================================
                    # ROUTE: Strategy 2 (Fade the Fakeout)
                    # =========================================================
                    elif strat == "fade_fakeout":
                        s2_signals += 1
                        f_reason = d.get("fade_reason", "unknown")
                        c_reason = d.get("close_reason", "unknown")
                        s2_fade_reasons[f_reason] += 1
                        s2_close_reasons[c_reason] += 1

                        gross = pos_size * ((net_pnl_bps + roundtrip_fee_bps) / 10000.0)
                        net = pos_size * (net_pnl_bps / 10000.0)
                        is_win = 1 if net > 0 else 0

                        s2_bucket["count"] += 1
                        s2_bucket["gross"] += gross
                        s2_bucket["fee"] += fee_usd
                        s2_bucket["net"] += net
                        s2_bucket["wins"] += is_win
                        if net > 0:
                            s2_bucket["gains"] += net
                        elif net < 0:
                            s2_bucket["losses"] += abs(net)

                        dur = float(d.get("duration_sec", 0.0))
                        mfe = float(d.get("mfe_exec_pnl_bps", 0.0))
                        mae = float(d.get("mae_exec_pnl_bps", 0.0))
                        s2_durations.append(dur)
                        s2_mfes.append(mfe)
                        s2_maes.append(mae)

                        # Coin & Venue stats
                        c2 = s2_coin_stats[coin]
                        c2["count"] += 1
                        c2["wins"] += is_win
                        c2["gross"] += gross
                        c2["net"] += net
                        c2["mfes"].append(mfe)

                        v2 = s2_venue_stats[f_ex]
                        v2["count"] += 1
                        v2["wins"] += is_win
                        v2["net"] += net

                        # Cross-strategy tracking
                        cx = coin_cross_stats[coin]
                        cx["s2_net"] += net
                        cx["total_net"] += net
                        cx["s2_trades"] += 1
                        cx["total_trades"] += 1
                        cx["wins"] += is_win

                        vx = venue_cross_stats[f_ex]
                        vx["s2_net"] += net
                        vx["total_net"] += net
                        vx["s2_trades"] += 1
                        vx["total_trades"] += 1
                        vx["wins"] += is_win

                    # =========================================================
                    # ROUTE: Strategy 3 (Maker Liquidity Harvesting)
                    # =========================================================
                    elif strat == "maker_void":
                        s3_opportunities += 1
                        side = d.get("side", "unknown")
                        c_reason = d.get("close_reason", "unknown")
                        is_filled = bool(d.get("maker_filled", False))
                        s3_sides[side] += 1
                        s3_close_reasons[c_reason] += 1

                        rebate_bps = float(d.get("maker_rebate_bps", 1.5))
                        taker_fee_bps = float(d.get("taker_fee_bps", 5.0))
                        sp_open = float(d.get("spread_open_bps", 0.0))
                        sp_close = float(d.get("spread_close_bps", 0.0))
                        s3_spread_open.append(sp_open)
                        s3_spread_close.append(sp_close)

                        c3 = s3_coin_stats[coin]
                        c3["opps"] += 1
                        v3 = s3_venue_stats[f_ex]
                        v3["opps"] += 1

                        if is_filled:
                            s3_filled_count += 1
                            c3["filled"] += 1
                            v3["filled"] += 1

                            rebate_usd = pos_size * (rebate_bps / 10000.0)
                            taker_usd = pos_size * (taker_fee_bps / 10000.0)
                            net = pos_size * (net_pnl_bps / 10000.0)
                            gross = net - rebate_usd + taker_usd
                            is_win = 1 if net > 0 else 0

                            s3_bucket["count"] += 1
                            s3_bucket["gross"] += gross
                            s3_bucket["rebates"] += rebate_usd
                            s3_bucket["taker_fees"] += taker_usd
                            s3_bucket["net"] += net
                            s3_bucket["wins"] += is_win
                            if net > 0:
                                s3_bucket["gains"] += net
                            elif net < 0:
                                s3_bucket["losses"] += abs(net)

                            c3["wins"] += is_win
                            c3["net"] += net
                            v3["wins"] += is_win
                            v3["net"] += net

                            # Cross-strategy tracking
                            cx = coin_cross_stats[coin]
                            cx["s3_net"] += net
                            cx["total_net"] += net
                            cx["s3_trades"] += 1
                            cx["total_trades"] += 1
                            cx["wins"] += is_win

                            vx = venue_cross_stats[f_ex]
                            vx["s3_net"] += net
                            vx["total_net"] += net
                            vx["s3_trades"] += 1
                            vx["total_trades"] += 1
                            vx["wins"] += is_win
                        else:
                            s3_unfilled_count += 1

        except Exception as e:
            if not as_json:
                print(f"[!] Warning reading {fpath}: {e}", file=sys.stderr)

    # -------------------------------------------------------------------------
    # FORMATTING OUTPUT DATA STRUCTURES
    # -------------------------------------------------------------------------
    # Strategy 1 PnL summary
    s1_pnl_summary = {}
    for name, sc in s1_scenarios.items():
        cnt = sc["count"]
        gains = sc.get("gains", 0.0)
        losses = sc.get("losses", 0.0)
        s1_pnl_summary[name] = {
            "trades": cnt,
            "wins": sc["wins"],
            "winrate_pct": round((sc["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "gross_usd": round(sc["gross"], 2),
            "fee_usd": round(sc["fee"], 2),
            "net_pnl_usd": round(sc["net"], 2),
            "profit_factor": _safe_pf(gains, losses),
        }

    # Strategy 2 summary
    s2_cnt = s2_bucket["count"]
    s2_summary = {
        "trades": s2_cnt,
        "wins": s2_bucket["wins"],
        "winrate_pct": round((s2_bucket["wins"] / s2_cnt) * 100.0, 1) if s2_cnt else 0.0,
        "gross_usd": round(s2_bucket["gross"], 2),
        "fee_usd": round(s2_bucket["fee"], 2),
        "net_pnl_usd": round(s2_bucket["net"], 2),
        "profit_factor": _safe_pf(s2_bucket["gains"], s2_bucket["losses"]),
        "mean_duration_sec": round(statistics.mean(s2_durations), 2) if s2_durations else 0.0,
        "mean_mfe_bps": round(statistics.mean(s2_mfes), 2) if s2_mfes else 0.0,
        "mean_mae_bps": round(statistics.mean(s2_maes), 2) if s2_maes else 0.0,
        "fade_reasons": dict(s2_fade_reasons),
        "close_reasons": dict(s2_close_reasons),
    }

    # Strategy 3 summary
    s3_cnt = s3_bucket["count"]
    s3_fill_rate = round((s3_filled_count / s3_opportunities * 100.0), 1) if s3_opportunities else 0.0
    s3_summary = {
        "opportunities": s3_opportunities,
        "trades": s3_filled_count,
        "unfilled": s3_unfilled_count,
        "fill_rate_pct": s3_fill_rate,
        "wins": s3_bucket["wins"],
        "winrate_pct": round((s3_bucket["wins"] / s3_cnt) * 100.0, 1) if s3_cnt else 0.0,
        "gross_usd": round(s3_bucket["gross"], 2),
        "rebates_earned_usd": round(s3_bucket["rebates"], 2),
        "taker_fees_paid_usd": round(s3_bucket["taker_fees"], 2),
        "net_pnl_usd": round(s3_bucket["net"], 2),
        "profit_factor": _safe_pf(s3_bucket["gains"], s3_bucket["losses"]),
        "mean_spread_open_bps": round(statistics.mean(s3_spread_open), 1) if s3_spread_open else 0.0,
        "mean_spread_close_bps": round(statistics.mean(s3_spread_close), 1) if s3_spread_close else 0.0,
        "sides": dict(s3_sides),
        "close_reasons": dict(s3_close_reasons),
    }

    # Consolidated Portfolio
    total_trades = s1_pnl_summary["all_signals"]["trades"] + s2_cnt + s3_filled_count
    total_wins = s1_pnl_summary["all_signals"]["wins"] + s2_bucket["wins"] + s3_bucket["wins"]
    total_gross = s1_pnl_summary["all_signals"]["gross_usd"] + s2_summary["gross_usd"] + s3_summary["gross_usd"]
    total_fees = (
        s1_pnl_summary["all_signals"]["fee_usd"]
        + s2_summary["fee_usd"]
        + s3_summary["taker_fees_paid_usd"]
        - s3_summary["rebates_earned_usd"]
    )
    total_net = s1_pnl_summary["all_signals"]["net_pnl_usd"] + s2_summary["net_pnl_usd"] + s3_summary["net_pnl_usd"]
    total_gains = s1_scenarios["all_signals"]["gains"] + s2_bucket["gains"] + s3_bucket["gains"]
    total_losses = s1_scenarios["all_signals"]["losses"] + s2_bucket["losses"] + s3_bucket["losses"]

    portfolio_summary = {
        "total_signals_recorded": total_signals,
        "total_executable_trades": total_trades,
        "total_wins": total_wins,
        "winrate_pct": round((total_wins / total_trades * 100.0), 1) if total_trades else 0.0,
        "gross_usd": round(total_gross, 2),
        "fees_and_rebates_usd": round(total_fees, 2),
        "net_pnl_usd": round(total_net, 2),
        "profit_factor": _safe_pf(total_gains, total_losses),
    }

    # Format top pairs (Strategy 1)
    sorted_pairs = []
    for pk, ps in sorted(s1_pair_stats.items(), key=lambda x: x[1]["net"], reverse=True):
        cnt = ps["count"]
        med_top3 = statistics.median(ps["top3_depths"]) if ps["top3_depths"] else 0.0
        med_cap = statistics.median(ps["stealth_caps"]) if ps["stealth_caps"] else 0.0
        sorted_pairs.append({
            "pair": pk,
            "trades": cnt,
            "winrate_pct": round((ps["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "net_pnl_usd": round(ps["net"], 2),
            "gross_usd": round(ps["gross"], 2),
            "med_top3_depth_usd": round(med_top3, 1),
            "med_stealth_cap_usd": round(med_cap, 1),
        })

    # Format top coins (Strategy 1)
    sorted_s1_coins = []
    for c, cs in sorted(s1_coin_stats.items(), key=lambda x: x[1]["net"], reverse=True):
        cnt = cs["count"]
        avg_mfe = sum(cs["mfes"]) / len(cs["mfes"]) if cs["mfes"] else 0.0
        med_tpk = sorted(cs["t_peaks"])[len(cs["t_peaks"]) // 2] if cs["t_peaks"] else 0.0
        med_top3 = statistics.median(cs["top3_depths"]) if cs["top3_depths"] else 0.0
        med_cap = statistics.median(cs["stealth_caps"]) if cs["stealth_caps"] else 0.0
        sorted_s1_coins.append({
            "coin": c,
            "trades": cnt,
            "winrate_pct": round((cs["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "net_pnl_usd": round(cs["net"], 2),
            "avg_mfe_bps": round(avg_mfe, 1),
            "median_t_peak_sec": round(med_tpk, 2),
            "med_top3_depth_usd": round(med_top3, 1),
            "med_stealth_cap_usd": round(med_cap, 1),
        })

    # Cross-Strategy Coin Leaderboard
    sorted_cross_coins = []
    for c, cdata in sorted(coin_cross_stats.items(), key=lambda x: x[1]["total_net"], reverse=True):
        n_t = cdata["total_trades"]
        wr = round((cdata["wins"] / n_t * 100.0), 1) if n_t else 0.0
        # Determine best strategy for coin
        strats_pnl = [("LeadLag", cdata["s1_net"]), ("Fade", cdata["s2_net"]), ("Maker", cdata["s3_net"])]
        strats_pnl.sort(key=lambda x: x[1], reverse=True)
        best_strat = strats_pnl[0][0] if strats_pnl[0][1] > 0 else "None"

        # Classification tag
        if cdata["total_net"] > 0 and cdata["s1_net"] > 0:
            status = "🚀 Alpha Momentum"
        elif cdata["total_net"] > 0 and cdata["s2_net"] > 0:
            status = "🎯 Fakeout Goldmine"
        elif cdata["total_net"] > 0 and cdata["s3_net"] > 0:
            status = "🧲 Spread Harvester"
        elif cdata["total_net"] < -2.0:
            status = "⚠️ Toxic Bleeder"
        else:
            status = "Neutral"

        sorted_cross_coins.append({
            "coin": c,
            "total_trades": n_t,
            "winrate_pct": wr,
            "s1_net_usd": round(cdata["s1_net"], 2),
            "s2_net_usd": round(cdata["s2_net"], 2),
            "s3_net_usd": round(cdata["s3_net"], 2),
            "total_net_usd": round(cdata["total_net"], 2),
            "best_strategy": best_strat,
            "status": status,
        })

    # Format venue physical capacities
    venue_summary = []
    for vex, vdata in sorted(venue_capacities.items(), key=lambda x: len(x[1]["stealth_caps"]), reverse=True):
        if not vdata["stealth_caps"]:
            continue
        caps = sorted(vdata["stealth_caps"])
        depths = sorted(vdata["top3_depths"])
        n_c = len(caps)
        med_cap = statistics.median(caps)
        med_depth = statistics.median(depths)
        p25_cap = caps[n_c // 4]
        p75_cap = caps[(3 * n_c) // 4]
        vx = venue_cross_stats.get(vex, {})
        venue_summary.append({
            "venue": vex,
            "signals": vdata["count"],
            "total_net_usd": round(vx.get("total_net", 0.0), 2),
            "s1_trades": vx.get("s1_trades", 0),
            "s2_trades": vx.get("s2_trades", 0),
            "s3_trades": vx.get("s3_trades", 0),
            "med_top3_depth_usd": round(med_depth, 1),
            "med_stealth_cap_usd": round(med_cap, 1),
            "p25_stealth_cap_usd": round(p25_cap, 1),
            "p75_stealth_cap_usd": round(p75_cap, 1),
            "min_stealth_cap_usd": round(caps[0], 1),
            "max_stealth_cap_usd": round(caps[-1], 1),
        })

    doc = {
        "metadata": {
            "report_date": today_str,
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "days_analyzed": days_back,
            "order_size_usd": pos_size,
            "roundtrip_fee_bps": roundtrip_fee_bps,
            "total_files_scanned": len(files),
            "total_size_mb": round(total_bytes / (1024 * 1024), 2),
            "total_signals_recorded": total_signals,
            "coins_active": len(coins_seen),
            "venues_active": sorted(venues_seen),
            "elapsed_sec": round(time.time() - t0, 3),
        },
        "portfolio": portfolio_summary,
        "strategies_breakdown": {
            "lead_lag": {
                "name": "1. Korean Lead-Lag Momentum (Long)",
                "trades": s1_pnl_summary["all_signals"]["trades"],
                "wins": s1_pnl_summary["all_signals"]["wins"],
                "winrate_pct": s1_pnl_summary["all_signals"]["winrate_pct"],
                "gross_usd": s1_pnl_summary["all_signals"]["gross_usd"],
                "fee_usd": s1_pnl_summary["all_signals"]["fee_usd"],
                "net_pnl_usd": s1_pnl_summary["all_signals"]["net_pnl_usd"],
                "profit_factor": s1_pnl_summary["all_signals"]["profit_factor"],
                "trigger_types": dict(s1_trigger_types),
                "close_reasons": dict(s1_close_reasons),
                "pnl_scenarios": s1_pnl_summary,
            },
            "fade_fakeout": {
                "name": "2. Fade the Fakeout (Short)",
                **s2_summary,
            },
            "maker_void": {
                "name": "3. Maker Liquidity Harvesting (Void)",
                **s3_summary,
            },
        },
        "stop_loss_telemetry": {
            "total_stops": s1_stop_stats["total_stops"],
            "stop_rate_pct": round((s1_stop_stats["total_stops"] / s1_signals * 100.0), 1) if s1_signals else 0.0,
            "invalidation_count": s1_stop_stats["invalidation"],
            "adverse_pnl_count": s1_stop_stats["adverse_pnl"],
            "alpha_decay_count": s1_stop_stats["alpha_decay"],
            "other_count": s1_stop_stats["other"],
            "total_stop_pnl_usd": round(s1_stop_stats["total_stop_pnl_usd"], 2),
        },
        "sweep_thresholds": {
            t: {
                "trades": s1_sweep[t]["count"],
                "winrate_pct": round((s1_sweep[t]["wins"] / s1_sweep[t]["count"]) * 100.0, 1) if s1_sweep[t]["count"] else 0.0,
                "net_usd": round(s1_sweep[t]["net"], 2),
            }
            for t in SWEEP_THRESHOLDS
        },
        "cross_coins_leaderboard": sorted_cross_coins[:35],
        "top_pairs": sorted_pairs[:20],
        "top_coins": sorted_s1_coins[:20],
        "top_coins_s1": sorted_s1_coins[:20],
        "venue_capacities": venue_summary,
    }

    # Write JSON report
    json_path = os.path.join(REPORTS_DIR, f"report_{today_str}.json")
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(doc, jf, indent=2, ensure_ascii=False)

    # Render Markdown report
    md_content = _render_markdown_report(doc)
    md_path = os.path.join(REPORTS_DIR, f"report_{today_str}.md")
    with open(md_path, "w", encoding="utf-8") as mf:
        mf.write(md_content)

    if not as_json:
        print(md_content)
        print(f"\n[OK] Multi-Strategy daily report generated and saved to:")
        print(f"  • Markdown: {md_path}")
        print(f"  • JSON:     {json_path}\n")

    return doc


def _render_markdown_report(doc: Dict[str, Any]) -> str:
    m = doc["metadata"]
    port = doc["portfolio"]
    strats = doc["strategies_breakdown"]
    s1 = strats["lead_lag"]
    s2 = strats["fade_fakeout"]
    s3 = strats["maker_void"]
    sl = doc.get("stop_loss_telemetry", {})
    vc = doc.get("venue_capacities", [])
    cross_coins = doc.get("cross_coins_leaderboard", [])

    lines = [
        f"# Korean Sunrise: Суточный Квантовый Отчёт (3 Стратегии)",
        f"*Дата отчёта:* `{m['report_date']}` | *Сгенерирован:* `{m['generated_at']}` | *Окно:* `{m['days_analyzed']} дн.` | *Слот:* `${m['order_size_usd']}` | *Комиссия:* `{m['roundtrip_fee_bps']} bps`",
        "",
        "## 1. Сводная таблица по 3 стратегиям (Executive Multi-Strategy Scorecard)",
        "| Стратегия | Направление | Сделок / Сигналов | Win Rate | Gross USD | Комиссии / Rebates | Net PnL USD | Profit Factor |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
        f"| **1. Lead-Lag Momentum** | Long | {s1['trades']:,} | {s1['winrate_pct']}% | ${s1['gross_usd']:.2f} | -${s1['fee_usd']:.2f} | **${s1['net_pnl_usd']:.2f}** | {s1['profit_factor']} |",
        f"| **2. Fade the Fakeout** | Short | {s2['trades']:,} | {s2['winrate_pct']}% | ${s2['gross_usd']:.2f} | -${s2['fee_usd']:.2f} | **${s2['net_pnl_usd']:.2f}** | {s2['profit_factor']} |",
        f"| **3. Maker Liquidity** | Post-Only | {s3['trades']:,} (из {s3['opportunities']:,}) | {s3['winrate_pct']}% | ${s3['gross_usd']:.2f} | +${s3['rebates_earned_usd']:.2f} / -${s3['taker_fees_paid_usd']:.2f} | **${s3['net_pnl_usd']:.2f}** | {s3['profit_factor']} |",
        f"| **ИТОГО ПОРТФЕЛЬ** | **Multi-Strat** | **{port['total_executable_trades']:,}** | **{port['winrate_pct']}%** | **${port['gross_usd']:.2f}** | **-${port['fees_and_rebates_usd']:.2f}** | **${port['net_pnl_usd']:.2f}** | **{port['profit_factor']}** |",
        "",
        "## 2. Стратегия 1: Korean Lead-Lag Momentum (Снайпинг импульсов Upbit)",
        f"- **Всего зафиксировано сигналов:** `{s1['trades']:,}`",
        f"- **Триггеры выкупа Upbit:** `Depth Drain: {s1['trigger_types'].get('depth_drain', 0)}` | `Spread Blowout: {s1['trigger_types'].get('spread_blowout', 0)}`",
        f"- **Причины закрытия позиций:** `Converged (TP): {s1['close_reasons'].get('converged', 0)}` | `Timeout: {s1['close_reasons'].get('timeout', 0)}` | `Reversed: {s1['close_reasons'].get('reversed', 0)}` | `Stops: {sl.get('total_stops', 0)}`",
        "",
        "### PnL Сценарии исполнения Lead-Lag:",
        "| Сценарий фильтрации | Сигналов | Win Rate | Gross USD | Комиссии | Net PnL USD | Profit Factor |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]

    labels = {
        "all_signals": "Все сигналы (Raw Streaming)",
        "calibrated_threshold_only": "Калиброванный фильтр (Matrix Thresholds)",
        "battle_coins_only": "Проверенный пул (Battle Pool Only)",
        "funded_venues_only": "Фондированные биржи (Funded CEXs Only)",
    }
    for k, row in s1.get("pnl_scenarios", {}).items():
        lbl = labels.get(k, k)
        lines.append(
            f"| **{lbl}** | {row['trades']:,} | {row['winrate_pct']}% | "
            f"${row['gross_usd']:.2f} | ${row['fee_usd']:.2f} | **${row['net_pnl_usd']:.2f}** | {row['profit_factor']} |"
        )

    # Section 3: Strategy 2 - Fade the Fakeout
    lines.extend([
        "",
        "## 3. Стратегия 2: Fade the Fakeout (Шорт ложных пробоев и снятий котировок MM)",
        f"- **Исполнено шортов:** `{s2['trades']:,}` | **Win Rate:** `{s2['winrate_pct']}%` | **Net PnL:** `${s2['net_pnl_usd']:.2f}` | **Profit Factor:** `{s2['profit_factor']}`",
        f"- **Средний MFE (макс. профит):** `+{s2['mean_mfe_bps']:.1f} bps` | **Средний MAE (просадка):** `{s2['mean_mae_bps']:.1f} bps` | **Ср. удержание:** `{s2['mean_duration_sec']:.2f}s`",
        "",
        "### Разбор причин входа в Fade (False Breakout Trigger):",
        "| Причина детекции фейкаута | Описание | Сработало |",
        "| :--- | :--- | :---: |",
        f"| `mm_quote_cancel` | Маркетмейкер снял аски на Upbit без подтверждения сделками (Ghosting) | **{s2['fade_reasons'].get('mm_quote_cancel', 0)}** |",
        f"| `micro_sweep` | Микро-покупка ритейла без достаточного институционального объёма | **{s2['fade_reasons'].get('micro_sweep', 0)}** |",
        f"| `wide_spread` | Дырявый стакан Upbit со спредом > 45 bps (неликвидная ловушка) | **{s2['fade_reasons'].get('wide_spread', 0)}** |",
        "",
        "### Исходы закрытия шорт-позиций Fade:",
        f"- `Target Fade Reached (TP): {s2['close_reasons'].get('target_fade_reached', 0)}` (цена вернулась к нормальному спреду)",
        f"- `Stop Invalidation (SL): {s2['close_reasons'].get('stop_invalidation', 0)}` (импульс оказался реальным, позиция сброшена)",
        f"- `Timeout: {s2['close_reasons'].get('timeout', 0) + s2['close_reasons'].get('timeout_no_follower_data', 0)}` (закрытие по горизонту удержания)",
    ])

    # Section 4: Strategy 3 - Maker Liquidity Harvesting
    lines.extend([
        "",
        "## 4. Стратегия 3: Maker Liquidity Harvesting (Поставка ликвидности в расширенные спреды)",
        f"- **Зафиксировано аномальных спредов (Void Opps):** `{s3['opportunities']:,}`",
        f"- **Успешно исполнено мейкер-лимиток (Fills):** `{s3['trades']:,}` (**Fill Rate: {s3['fill_rate_pct']}%**)",
        f"- **Win Rate исполненных:** `{s3['winrate_pct']}%` | **Net PnL:** `${s3['net_pnl_usd']:.2f}` | **Profit Factor:** `{s3['profit_factor']}`",
        f"- **Экономика сборов:** Заработано Maker-рибейтов: `+${s3['rebates_earned_usd']:.2f}` | Уплачено Taker-комиссий выхода: `-${s3['taker_fees_paid_usd']:.2f}`",
        f"- **Сжатие спреда:** Средний спред на входе: `{s3['mean_spread_open_bps']:.1f} bps` -> Средний спред на выходе: `{s3['mean_spread_close_bps']:.1f} bps`",
        f"- **Стороны лимиток:** `Bids (выкуп дипа): {s3['sides'].get('bid', 0)}` | `Asks (продажа спайка): {s3['sides'].get('ask', 0)}`",
        f"- **Исходы ордеров:** `Spread Restored (Profit): {s3['close_reasons'].get('spread_restored', 0)}` | `Unfilled Timeout (Risk $0): {s3['close_reasons'].get('unfilled_timeout', 0)}` | `Stops: {s3['close_reasons'].get('stop_invalidation', 0)}`",
    ])

    # Section 5: Stop-Loss Protection Telemetry
    lines.extend([
        "",
        "## 5. Стоп-Лоссы и Защита Капитала (Adaptive Stop-Loss Telemetry)",
        f"- **Всего сработало стопов:** `{sl.get('total_stops', 0):,}` ({sl.get('stop_rate_pct', 0.0)}% от импульсов)",
        f"- **Model A: Microstructure Invalidation:** `{sl.get('invalidation_count', 0):,}` (мгновенный сброс при откате Upbit)",
        f"- **Model B: Hard Adverse PnL Drawdown:** `{sl.get('adverse_pnl_count', 0):,}` (жесткая отсечка токсичного дрейфа)",
        f"- **Model C: Alpha Decay Timeout:** `{sl.get('alpha_decay_count', 0):,}` (сброс по затуханию альфы при PnL <= 0)",
        f"- **Суммарный контролируемый PnL по стопам:** `${sl.get('total_stop_pnl_usd', 0.0):.2f}`",
    ])

    # Section 6: Cross-Strategy Coin Leaderboard
    lines.extend([
        "",
        "## 6. Кросс-Стратегический Рейтинг Монет (Cross-Strategy Alpha Matrix)",
        "| Монета | Сделок | Win Rate | S1 Lead-Lag | S2 Fade | S3 Maker | Общий Net PnL | Лучшая стратегия | Статус |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    if cross_coins:
        for c in cross_coins[:25]:
            lines.append(
                f"| **{c['coin']}** | {c['total_trades']} | {c['winrate_pct']}% | "
                f"${c['s1_net_usd']:.2f} | ${c['s2_net_usd']:.2f} | ${c['s3_net_usd']:.2f} | "
                f"**${c['total_net_usd']:.2f}** | `{c['best_strategy']}` | {c['status']} |"
            )
    else:
        lines.append("| *Нет накопленных данных по монетам за выбранный период* | - | - | - | - | - | - | - | - |")

    # Section 7: Venue Physical Capacity & Cross-Strategy PnL
    lines.extend([
        "",
        "## 7. Физическая ёмкость стакана и PnL по биржам (Venue Matrix)",
        "| Биржа | Сигналов | Сделок S1 | Сделок S2 | Сделок S3 | Общий Net USD | Мед. Top-3 ($) | Мед. 15% Ёмкость ($) | P25 ($) | P75 ($) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    if vc:
        for v in vc:
            lines.append(
                f"| `{v['venue']}` | {v['signals']:,} | {v.get('s1_trades', 0)} | {v.get('s2_trades', 0)} | {v.get('s3_trades', 0)} | "
                f"**${v.get('total_net_usd', 0.0):.2f}** | ${v['med_top3_depth_usd']:,.1f} | "
                f"**${v['med_stealth_cap_usd']:,.1f}** | ${v['p25_stealth_cap_usd']:,.1f} | ${v['p75_stealth_cap_usd']:,.1f} |"
            )
    else:
        lines.append("| *Нет накопленных данных по биржам* | - | - | - | - | - | - | - | - | - |")

    # Section 8: Threshold Sweep
    lines.extend([
        "",
        "## 8. Оптимизация порогов спреда Lead-Lag (Threshold Sweep)",
        "| Порог спреда (bps) | Сигналов | Win Rate | Net PnL USD |",
        "| :---: | :---: | :---: | :---: |",
    ])
    for t, sw in sorted(doc["sweep_thresholds"].items(), key=lambda x: int(x[0])):
        lines.append(f"| >={t} bps | {sw['trades']:,} | {sw['winrate_pct']}% | ${sw['net_usd']:.2f} |")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Multi-Strategy Daily Quantitative Report.")
    parser.add_argument("--days", type=float, default=1.0, help="Days back (default: 1.0)")
    parser.add_argument("--size", type=float, default=10.0, help="Order size USD (default: 10.0)")
    parser.add_argument("--fee-bps", type=float, default=10.0, help="Roundtrip fee bps (default: 10.0)")
    parser.add_argument("--json", action="store_true", help="Output JSON only")
    args = parser.parse_args()

    doc = generate_daily_report(
        days_back=args.days,
        pos_size=args.size,
        roundtrip_fee_bps=args.fee_bps,
        as_json=args.json,
    )
    if args.json:
        print(json.dumps(doc, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
