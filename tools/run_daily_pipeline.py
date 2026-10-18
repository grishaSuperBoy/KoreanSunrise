"""
tools/run_daily_pipeline.py — Master daily lifecycle orchestrator for Korean Sunrise.

Executes the autonomous 24-hour cycle:
  [Phase 1] Universe Screening:
            Queries Upbit, Binance Futures, and Bybit Linear.
            Updates: universe_live.json (Incubator universe).
  [Phase 2] Pool Evaluation & Promotion:
            Scores recent signals using pure realized Net PnL and Profit Factor.
            Updates: live_trading_pool.json (Battle pool).
  [Phase 3] Lifecycle Summary & Actionable Pairs Report.

Usage:
  python tools/run_daily_pipeline.py
  python tools/run_daily_pipeline.py --skip-screening
  python tools/run_daily_pipeline.py --days 3
  python tools/run_daily_pipeline.py --dry-run
"""
from __future__ import annotations

import argparse
import datetime
import logging
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from pipeline.evaluator import SunriseEvaluator
from pipeline.pool_state import PoolStateManager
from pipeline.profile import TradingProfileManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("daily_pipeline")


def phase_1_screening(dry_run: bool = False) -> bool:
    log.info("=" * 80)
    log.info("[PHASE 1/3] Live Universe Screening: Upbit Spot KRW <-> Binance/Bybit USDT Perps")
    log.info("=" * 80)
    cmd = [sys.executable, os.path.join(HERE, "screen_universe.py")]
    if dry_run:
        cmd.append("--dry-run")

    try:
        res = subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)
        return res.returncode == 0
    except Exception as e:
        log.error(f"[daily_pipeline] Screening failed: {e}")
        return False


def phase_2_evaluation(days_back: int = 7, min_events: int = 5, dry_run: bool = False) -> int:
    log.info("=" * 80)
    log.info(f"[PHASE 2/3] Quantitative Pool Evaluation (days_back={days_back}, min_events={min_events})")
    log.info("=" * 80)
    data_root = os.getenv("DATA_ROOT", os.path.join(PROJECT_ROOT, "data"))
    psm = PoolStateManager()
    psm.reload()

    ev = SunriseEvaluator(
        data_root=data_root,
        days_back=days_back,
        min_events=min_events,
        pool_state=psm,
    )
    qualified = ev.run(dry_run=dry_run)
    return len(qualified)


def phase_3_summary() -> None:
    log.info("=" * 80)
    log.info("[PHASE 3/3] Daily Lifecycle Summary & Actionable Pairs")
    log.info("=" * 80)
    psm = PoolStateManager()
    tpm = TradingProfileManager()
    psm.reload()
    tpm.reload()

    snap = psm.get_pool_snapshot()
    battle_coins = psm.get_battle_coins()
    log.info(
        f"Pipeline state: Incubator: {snap['summary']['incubator']} coins | "
        f"Battle Pool: {snap['summary']['battle']} coins (Active: {snap['summary']['battle_active']})"
    )

    if battle_coins:
        log.info("Active Battle coins ready for live execution:")
        for c in battle_coins:
            coin = c["coin"]
            venues = c.get("venues", [])
            depth = c.get("top3_depth_usd", 0.0)
            sizes = []
            for v in venues:
                sz = tpm.calculate_stealth_order_size(v, depth)
                if sz:
                    sizes.append(f"{v}: ${sz:.1f}")
            log.info(
                f"  • {coin:<10} | Exp: {c.get('mean_net_pnl_bps', 0):>+6.2f} bps | "
                f"PF: {c.get('profit_factor', 0):>4.2f} | Win: {c.get('win_rate_pct', 0):>4.1f}% | "
                f"Sizes: [{' | '.join(sizes)}]"
            )
    else:
        log.info("No coins currently in Battle pool. Incubator is actively gathering signal telemetry.")
    log.info("=" * 80)


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Autonomous Daily Pipeline")
    parser.add_argument("--skip-screening", action="store_true", help="Skip REST screening of exchanges")
    parser.add_argument("--days", type=int, default=7, help="Days of logs to scan for evaluation")
    parser.add_argument("--min-events", type=int, default=5, help="Minimum events to qualify for Battle")
    parser.add_argument("--dry-run", action="store_true", help="Dry run without updating state files")
    args = parser.parse_args()

    t0 = time.time()
    log.info("=== Starting Korean Sunrise Autonomous Daily Pipeline ===")

    if not args.skip_screening:
        ok = phase_1_screening(dry_run=args.dry_run)
        if not ok:
            log.warning("Screening phase encountered errors. Continuing with existing universe...")

    qualified_count = phase_2_evaluation(days_back=args.days, min_events=args.min_events, dry_run=args.dry_run)

    phase_3_summary()

    elapsed = round(time.time() - t0, 1)
    log.info(f"=== Daily Pipeline completed in {elapsed}s (Battle Qualified: {qualified_count}) ===")


if __name__ == "__main__":
    main()
