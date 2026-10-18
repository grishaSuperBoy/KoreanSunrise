"""
tools/run_daily_pipeline.py - Master Orchestrator for the 24-Hour Autonomous Lifecycle.

Executes the 4 core daily operations sequentially:
  [Phase 1] Universe Screening (1-min candles across 24h on Upbit & 9 follower CEXs):
            Updates: universe_live.json (Incubator universe)
  [Phase 2] Pool Evaluation (Yesterday's telemetry scoring & promotion):
            Updates: live_trading_pool.json (Battle vs Incubator)
  [Phase 3] Spread Calibration (10-60 bps sweep per pair):
            Updates: matrix_thresholds.json (Dynamic execution thresholds)
  [Phase 4] Full Daily Quantitative Report:
            Updates: reports/daily/report_YYYY-MM-DD.md & .json

Usage:
  python tools/run_daily_pipeline.py
  python tools/run_daily_pipeline.py --skip-screening
  python tools/run_daily_pipeline.py --dry-run
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

UNIVERSE_FILE = os.path.join(PROJECT_ROOT, "universe_live.json")
BATTLE_FILE = os.path.join(PROJECT_ROOT, "live_trading_pool.json")
THRESHOLDS_FILE = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
LOG_FILE = os.path.join(PROJECT_ROOT, "logs", "daily_pipeline.log")

os.makedirs(os.path.join(PROJECT_ROOT, "logs"), exist_ok=True)

logger = logging.getLogger("daily_pipeline")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")

_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
logger.addHandler(_sh)


def run_pipeline(
    skip_screening: bool = False,
    skip_evaluation: bool = False,
    skip_calibration: bool = False,
    skip_report: bool = False,
    dry_run: bool = False,
) -> bool:
    t_start = time.time()
    logger.info("================================================================================")
    logger.info("  KOREAN SUNRISE: 24-HOUR MASTER AUTONOMOUS LIFECYCLE PIPELINE")
    logger.info("================================================================================")

    # -------------------------------------------------------------------------
    # PHASE 1: Universe Screening (1440 1m candles across 24h)
    # -------------------------------------------------------------------------
    if skip_screening:
        logger.info("[PHASE 1/4] Universe Screening: SKIPPED (--skip-screening)")
    else:
        logger.info("[PHASE 1/4] Universe Screening: Fetching 1440 1m candles across Upbit & 9 followers...")
        cmd_scr = [
            sys.executable,
            os.path.join(PROJECT_ROOT, "tools", "screen_universe.py"),
            "--kline-limit", "1440",
            "--workers", "8",
        ]
        if not dry_run:
            p = subprocess.run(cmd_scr, capture_output=True, text=True, encoding="utf-8", errors="replace")
            if p.returncode != 0:
                logger.error(f"[!] Phase 1 failed: {p.stderr}")
            else:
                logger.info("[OK] Phase 1 completed successfully.")

    # -------------------------------------------------------------------------
    # PHASE 2: Pool Evaluation & Promotion
    # -------------------------------------------------------------------------
    if skip_evaluation:
        logger.info("[PHASE 2/4] Pool Evaluation: SKIPPED (--skip-evaluation)")
    else:
        logger.info("[PHASE 2/4] Pool Evaluation: Scoring recorded signals and promoting to Battle...")
        from pipeline.evaluator import SunriseEvaluator
        if not dry_run:
            ev = SunriseEvaluator(data_root=os.path.join(PROJECT_ROOT, "data"))
            promoted = ev.run()
            logger.info(f"[OK] Phase 2 completed: {len(promoted)} coins promoted to Battle pool.")

    # -------------------------------------------------------------------------
    # PHASE 3: Spread Threshold Calibration
    # -------------------------------------------------------------------------
    if skip_calibration:
        logger.info("[PHASE 3/4] Spread Calibration: SKIPPED (--skip-calibration)")
    else:
        logger.info("[PHASE 3/4] Spread Calibration: Sweeping thresholds [10..60 bps] per pair...")
        cmd_cal = [
            sys.executable,
            os.path.join(PROJECT_ROOT, "tools", "calibrate_thresholds.py"),
        ]
        if not dry_run:
            p = subprocess.run(cmd_cal, capture_output=True, text=True, encoding="utf-8", errors="replace")
            if p.returncode != 0:
                logger.error(f"[!] Phase 3 failed: {p.stderr}")
            else:
                logger.info("[OK] Phase 3 completed: matrix_thresholds.json updated.")

    # -------------------------------------------------------------------------
    # PHASE 4: Full Daily Report
    # -------------------------------------------------------------------------
    if skip_report:
        logger.info("[PHASE 4/4] Daily Report: SKIPPED (--skip-report)")
    else:
        logger.info("[PHASE 4/4] Daily Report: Generating quantitative performance report...")
        cmd_rep = [
            sys.executable,
            os.path.join(PROJECT_ROOT, "tools", "full_daily_report.py"),
        ]
        if not dry_run:
            p = subprocess.run(cmd_rep, capture_output=True, text=True, encoding="utf-8", errors="replace")
            if p.returncode != 0:
                logger.error(f"[!] Phase 4 failed: {p.stderr}")
            else:
                logger.info("[OK] Phase 4 completed: report saved to reports/daily/.")

    elapsed = time.time() - t_start
    logger.info("================================================================================")
    logger.info(f"  [ALL COMPLETE] Master pipeline finished in {elapsed:.2f}s.")
    logger.info("================================================================================\n")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Master 24h Autonomous Daily Pipeline.")
    parser.add_argument("--skip-screening", action="store_true", help="Skip universe screening")
    parser.add_argument("--skip-evaluation", action="store_true", help="Skip pool evaluation")
    parser.add_argument("--skip-calibration", action="store_true", help="Skip spread calibration")
    parser.add_argument("--skip-report", action="store_true", help="Skip daily report")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing")
    args = parser.parse_args()

    run_pipeline(
        skip_screening=args.skip_screening,
        skip_evaluation=args.skip_evaluation,
        skip_calibration=args.skip_calibration,
        skip_report=args.skip_report,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
