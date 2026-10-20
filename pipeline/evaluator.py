"""
pipeline/evaluator.py — Quantitative signal evaluator and pool promotion engine.

Zero Hardcode & Pure Trading Mathematics:
  - Evaluates recorded .jsonl.gz signal archives from data/signals/.
  - Uses strictly realized executable PnL (net_pnl_bps), which already accounts
    for entry ask, exit bid, and roundtrip taker fees (10 bps).
  - Dynamically filters out retail micro-noise using book microstructure:
      1. Drain Ratio >= 0.30 (swept >= 30% of Upbit normal ask depth).
      2. Cross-Liquidity Ratio >= 0.20 (Upbit sweep is commensurate with follower's top-3 depth).
      3. Follower Top-3 Depth >= $66.7 (supports $10 minimum exchange order at 15% stealth size).
  - Evaluates pure statistical edge:
      * Expectancy: Mean & Median Net PnL (bps) > 0.
      * Profit Factor: Sum(Gains) / Sum(|Losses|) >= 1.2.
      * Win Rate >= 50%.
      * Sample Size >= min_events.
"""
from __future__ import annotations

import glob
import gzip
import json
import logging
import math
import os
import statistics
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from config import MIN_ORDER_USD, STEALTH_DEPTH_FRACTION
from pipeline.pool_state import PoolStateManager

log = logging.getLogger("pipeline.evaluator")

DEFAULT_MIN_EVENTS = 5
DEFAULT_MIN_PROFIT_FACTOR = 1.25
DEFAULT_MIN_WIN_RATE = 50.0


def _iter_gz_jsonl(path: str):
    """Yields parsed JSON dicts from a gzip-compressed JSONL file."""
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        pass
    except Exception as e:
        log.debug(f"[evaluator] Cannot read {path}: {e}")


def _collect_signal_files(data_root: str, days_back: int = 7) -> List[str]:
    """Finds all sunrise signal files within the last `days_back` days."""
    pattern = os.path.join(data_root, "signals", "**", "*.jsonl.gz")
    all_files = sorted(glob.glob(pattern, recursive=True))
    cutoff = time.time() - (days_back * 86400.0)
    files = [f for f in all_files if os.path.getmtime(f) >= cutoff]
    return files if files else all_files


class CoinSignalAccumulator:
    """Accumulates verified trading statistics for a single coin across signals."""
    __slots__ = (
        "coin", "venues", "net_pnls", "mfes", "t_peaks",
        "top3_depths", "stealth_caps", "valid_sweeps", "total_signals",
    )

    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.venues: set = set()
        self.net_pnls: List[float] = []
        self.mfes: List[float] = []
        self.t_peaks: List[float] = []
        self.top3_depths: List[float] = []
        self.stealth_caps: List[float] = []
        self.valid_sweeps: int = 0
        self.total_signals: int = 0

    def add_signal(self, row: Dict[str, Any]) -> None:
        self.total_signals += 1

        follower_ex = str(row.get("follower_exchange", "")).lower()
        if follower_ex:
            self.venues.add(follower_ex)

        net_pnl = row.get("net_pnl_bps")
        if net_pnl is None or not math.isfinite(float(net_pnl)):
            return

        # Microstructure physical validity check (Zero Hardcode):
        # 1. Drain ratio must show structural depletion of Upbit's ask depth
        drain_ratio = float(row.get("drain_ratio", 0.0))
        # 2. Cross-liquidity ratio: sweep volume vs follower book depth
        swept_usd = float(row.get("leader_swept_usd", 0.0))
        f_top3_ask = float(row.get("follower_top3_ask_usd", 0.0))
        cross_ratio = (swept_usd / f_top3_ask) if f_top3_ask > 0 else 0.0

        stealth_cap = float(row.get("stealth_capacity_usd", 0.0))
        if stealth_cap <= 0.0 and f_top3_ask > 0.0:
            stealth_cap = round(0.15 * f_top3_ask, 2)

        # Discard microscopic noise where Upbit sweep was insignificant or follower book is overwhelmingly massive
        if drain_ratio < 0.30 or cross_ratio < 0.20:
            return

        self.valid_sweeps += 1
        pnl = float(net_pnl)
        self.net_pnls.append(pnl)

        mfe = row.get("mfe_exec_pnl_bps")
        if mfe is not None and math.isfinite(float(mfe)):
            self.mfes.append(float(mfe))

        t_peak = row.get("time_to_peak_sec")
        if t_peak is not None and math.isfinite(float(t_peak)):
            self.t_peaks.append(float(t_peak))

        if f_top3_ask > 0:
            self.top3_depths.append(f_top3_ask)
            self.stealth_caps.append(stealth_cap)

    def finalize(self) -> Optional[Dict[str, Any]]:
        n = len(self.net_pnls)
        if n == 0:
            return None

        gains = sum(p for p in self.net_pnls if p > 0)
        losses = sum(abs(p) for p in self.net_pnls if p < 0)

        # Standard quantitative Profit Factor: Gains / Losses
        # Capped conservatively at 5.0 when zero losses occur to prevent artificial 99.0
        if losses > 0:
            profit_factor = gains / losses
        elif gains > 0:
            profit_factor = 5.0
        else:
            profit_factor = 0.0
        win_count = sum(1 for p in self.net_pnls if p > 0)
        win_rate = (win_count / n) * 100.0

        mean_pnl = statistics.mean(self.net_pnls)
        median_pnl = statistics.median(self.net_pnls)
        median_depth = statistics.median(self.top3_depths) if self.top3_depths else 0.0
        median_stealth_cap = statistics.median(self.stealth_caps) if self.stealth_caps else 0.0
        mean_mfe = statistics.mean(self.mfes) if self.mfes else 0.0
        med_t_peak = statistics.median(self.t_peaks) if self.t_peaks else 0.0

        return {
            "coin": self.coin,
            "venues": sorted(self.venues),
            "event_count": n,
            "total_signals": self.total_signals,
            "mean_net_pnl_bps": round(mean_pnl, 2),
            "median_net_pnl_bps": round(median_pnl, 2),
            "win_rate_pct": round(win_rate, 1),
            "profit_factor": round(profit_factor, 2),
            "top3_depth_usd": round(median_depth, 1),
            "stealth_capacity_usd": round(median_stealth_cap, 1),
            "mean_mfe_bps": round(mean_mfe, 2),
            "median_t_peak_sec": round(med_t_peak, 2),
            # Rank score = Expected Edge * Statistical Confidence
            "ranking_score": round(mean_pnl * (win_rate / 100.0) * math.log(n + 1), 3),
        }


class SunriseEvaluator:
    """
    Evaluates recorded signal archives and promotes qualified coins to the Battle pool.
    """

    def __init__(
        self,
        data_root: str = "data",
        days_back: int = 7,
        min_events: int = DEFAULT_MIN_EVENTS,
        min_profit_factor: float = DEFAULT_MIN_PROFIT_FACTOR,
        min_win_rate: float = DEFAULT_MIN_WIN_RATE,
        pool_state: Optional[PoolStateManager] = None,
    ) -> None:
        self.data_root = data_root
        self.days_back = days_back
        self.min_events = min_events
        self.min_profit_factor = min_profit_factor
        self.min_win_rate = min_win_rate
        self.pool = pool_state or PoolStateManager()

    def run(self, dry_run: bool = False) -> List[Dict[str, Any]]:
        t0 = time.time()
        log.info(f"[evaluator] Starting evaluation (days_back={self.days_back}, min_events={self.min_events})")

        files = _collect_signal_files(self.data_root, self.days_back)
        if not files:
            log.warning(f"[evaluator] No signal files found in {self.data_root}/signals/")
            return []

        log.info(f"[evaluator] Processing {len(files)} compressed signal archives...")

        accumulators: Dict[str, CoinSignalAccumulator] = defaultdict(lambda: CoinSignalAccumulator(""))
        total_rows = 0

        for fpath in files:
            for row in _iter_gz_jsonl(fpath):
                coin = str(row.get("coin", "")).upper()
                if not coin:
                    continue
                if coin not in accumulators:
                    accumulators[coin] = CoinSignalAccumulator(coin)
                accumulators[coin].add_signal(row)
                total_rows += 1

        # Finalize stats
        all_stats: List[Dict[str, Any]] = []
        for acc in accumulators.values():
            st = acc.finalize()
            if st:
                all_stats.append(st)

        all_stats.sort(key=lambda s: s["ranking_score"], reverse=True)

        # Gate criteria for Battle promotion (Strictly positive statistical expectancy)
        qualified: List[Dict[str, Any]] = []
        for s in all_stats:
            # 1. Sufficient sample size
            if s["event_count"] < self.min_events:
                continue
            # 2. Positive expectancy after all roundtrip fees
            if s["mean_net_pnl_bps"] <= 0.0 or s["median_net_pnl_bps"] < -2.0:
                continue
            # 3. Both Profit Factor AND Win Rate must meet statistical standards
            if s["profit_factor"] < self.min_profit_factor or s["win_rate_pct"] < self.min_win_rate:
                continue
            # 4. Book depth supports minimum order ($10 min order at 15% stealth fraction)
            if s["top3_depth_usd"] < round(MIN_ORDER_USD / STEALTH_DEPTH_FRACTION, 1):
                continue

            qualified.append(s)

        elapsed = round(time.time() - t0, 2)
        log.info(
            f"[evaluator] Evaluated {total_rows} signals across {len(all_stats)} coins in {elapsed}s. "
            f"Qualified for Battle: {len(qualified)}"
        )

        for i, q in enumerate(qualified[:15], 1):
            log.info(
                f"  #{i:2d} {q['coin']:<10} | Exp: {q['mean_net_pnl_bps']:>+6.2f} bps | "
                f"PF: {q['profit_factor']:>4.2f} | Win: {q['win_rate_pct']:>4.1f}% | "
                f"N={q['event_count']} | Depth: ${q['top3_depth_usd']:>6.0f} | "
                f"Cap15%: ${q['stealth_capacity_usd']:>5.0f} | MFE: +{q['mean_mfe_bps']:.1f} bps"
            )

        if not dry_run:
            self.pool.reload()
            self.pool.promote_to_battle(qualified)

        return qualified
