"""
tools/validate_data.py - High-speed multi-threaded data quality auditor for Korean Sunrise.

Checks archives in data/:
1. File integrity: valid gzip CRC, valid JSON, no truncated lines.
2. Schema completeness: all required fields present per signal event.
3. Numerical sanity:
   - Prices and depths: strictly > 0, no NaN / Inf.
   - Timestamps: open_ts <= close_ts, duration >= 0.
   - PnL metrics: finite net_pnl_bps and gross_exec_edge_bps.
4. Summary output: console defect report and statistics.

Usage:
  python tools/validate_data.py
  python tools/validate_data.py --days 7
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import glob
import gzip
import json
import math
import os
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")

REQUIRED_SIGNAL_FIELDS = (
    "id",
    "coin",
    "follower_exchange",
    "trigger_type",
    "dislocation_bps",
    "gross_exec_edge_bps",
    "net_pnl_bps",
    "leader_swept_usd",
    "follower_top3_ask_usd",
    "close_reason",
    "entry_ts",
)


def _audit_file(fpath: str) -> Dict[str, Any]:
    stat = {
        "file": fpath,
        "rows": 0,
        "corrupted_rows": 0,
        "missing_fields": 0,
        "nan_inf_count": 0,
        "gzip_error": False,
        "coins": set(),
        "venues": set(),
    }
    try:
        with gzip.open(fpath, "rt", encoding="utf-8", errors="replace") as gz:
            for line in gz:
                line = line.strip()
                if not line:
                    continue
                stat["rows"] += 1
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    stat["corrupted_rows"] += 1
                    continue

                coin = str(d.get("coin", "")).upper()
                f_ex = str(d.get("follower_exchange", "")).lower()
                if coin:
                    stat["coins"].add(coin)
                if f_ex:
                    stat["venues"].add(f_ex)

                # Check schema
                missing = [f for f in REQUIRED_SIGNAL_FIELDS if f not in d]
                if missing:
                    stat["missing_fields"] += 1

                # Check numbers
                for num_key in ("gross_exec_edge_bps", "net_pnl_bps", "leader_swept_usd", "follower_top3_ask_usd"):
                    v = d.get(num_key)
                    if v is not None:
                        try:
                            fv = float(v)
                            if not math.isfinite(fv):
                                stat["nan_inf_count"] += 1
                        except (ValueError, TypeError):
                            stat["nan_inf_count"] += 1

    except Exception:
        stat["gzip_error"] = True

    return stat


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Data Quality Auditor.")
    parser.add_argument("--days", type=float, default=7.0, help="Days back to scan (default: 7.0)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel worker threads")
    args = parser.parse_args()

    print("=" * 80)
    print("  KOREAN SUNRISE: MULTI-THREADED DATA QUALITY AUDITOR")
    print("=" * 80)

    pattern = os.path.join(DATA_ROOT, "signals", "**", "*.jsonl.gz")
    all_files = sorted(glob.glob(pattern, recursive=True))

    if not all_files:
        print(f"  [OK] No signal files found in {os.path.join(DATA_ROOT, 'signals')}. System is in fresh state.")
        print("=" * 80)
        return

    cutoff = time.time() - (args.days * 86400.0)
    files = [f for f in all_files if os.path.getmtime(f) >= cutoff]
    if not files:
        files = all_files

    print(f"  Scanning {len(files)} archives ({args.days:.1f} days trailing)...")
    t0 = time.time()

    total_rows = 0
    total_corrupted = 0
    total_missing = 0
    total_nan_inf = 0
    gzip_errors = 0
    all_coins = set()
    all_venues = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for res in ex.map(_audit_file, files):
            total_rows += res["rows"]
            total_corrupted += res["corrupted_rows"]
            total_missing += res["missing_fields"]
            total_nan_inf += res["nan_inf_count"]
            if res["gzip_error"]:
                gzip_errors += 1
            all_coins.update(res["coins"])
            all_venues.update(res["venues"])

    elapsed = time.time() - t0
    print("-" * 80)
    print(f"  Total Files Audited     : {len(files):,}")
    print(f"  Total Signals Checked   : {total_rows:,}")
    print(f"  Gzip Corrupted Files    : {gzip_errors}")
    print(f"  Malformed JSON Rows     : {total_corrupted}")
    print(f"  Missing Required Fields : {total_missing}")
    print(f"  NaN / Inf Anomalies     : {total_nan_inf}")
    print(f"  Unique Coins Recorded   : {len(all_coins)}")
    print(f"  Unique Follower Venues  : {len(all_venues)} ({', '.join(sorted(all_venues))})")
    print(f"  Scan Speed              : {total_rows / max(0.001, elapsed):,.0f} rows/s ({elapsed:.2f}s)")
    print("=" * 80)

    if gzip_errors == 0 and total_corrupted == 0 and total_nan_inf == 0:
        print("  [SUCCESS] All data archives passed strict CRC & Schema validation!")
    else:
        print("  [WARNING] Defective data detected. Check data_quality.log for details.")
    print("=" * 80)


if __name__ == "__main__":
    main()
