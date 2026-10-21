"""
tools/screen_universe.py — Autonomous Two-Stage Universe Screener for Korean Sunrise.
Stage 1: 24h Ticker, volume, and namesake price sanity filter.
Stage 2: 1-minute kline correlation and Kimchi basis volatility screening.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from screening.screener import UniverseScreener

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("screen_universe")


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Two-Stage Universe Screener.")
    parser.add_argument("--out", default=os.path.join(PROJECT_ROOT, "universe_live.json"), help="Output path")
    parser.add_argument("--min-vol", type=float, default=20_000.0, help="Min 24h volume USD on both venues (default: $20K)")
    parser.add_argument("--max-price-diff", type=float, default=15.0, help="Max price diff %% (namesake filter, default: 15%%)")
    parser.add_argument("--min-corr", type=float, default=0.50, help="Min multi-scale log-returns correlation (default: 0.50)")
    parser.add_argument("--min-both-trade", type=float, default=0.05, help="Min fraction of active trading minutes (default: 0.05)")
    parser.add_argument("--min-dev-bps", type=float, default=6.0, help="Min P90 basis deviation in bps (default: 6.0 bps)")
    parser.add_argument("--venues", nargs="+", default=None, help="Specific follower venues (e.g. --venues binance)")
    parser.add_argument("--coins", nargs="+", default=None, help="Specific coins to screen (e.g. --coins ETHFI AXS SPX)")
    parser.add_argument("--kline-limit", type=int, default=1440, help="Number of 1m candles to evaluate (default: 1440 = 24h)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel worker threads (default: 8)")
    args = parser.parse_args()

    screener = UniverseScreener(
        venues=args.venues,
        coins=args.coins,
        min_vol_usd=args.min_vol,
        max_price_diff_pct=args.max_price_diff,
        min_corr=args.min_corr,
        min_both_trade=args.min_both_trade,
        min_dev_bps=args.min_dev_bps,
        kline_limit=args.kline_limit,
        output_file=args.out,
        max_workers=args.workers,
    )

    doc = screener.run()
    meta = doc.get("_meta", {})
    log.info(
        f"Screening complete: {meta.get('coins_count', 0)} qualified coins saved to {args.out} "
        f"in {meta.get('screening_elapsed_sec', 0.0)}s."
    )


if __name__ == "__main__":
    main()
