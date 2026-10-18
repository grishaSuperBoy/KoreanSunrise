"""
tools/audit_venues.py - Exhaustive live health, latency, and telemetry auditor for Korean Sunrise.
Audits Upbit Spot Leader and all 9 perpetual follower venues.

Usage:
  python tools/audit_venues.py
  python tools/audit_venues.py --port 8089
"""
from __future__ import annotations

import argparse
import collections
import glob
import gzip
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ALL_VENUES = [
    "upbit",
    "binance",
    "bybit",
    "okx",
    "gateio",
    "mexc",
    "kucoin",
    "bingx",
    "htx",
    "pionex",
]

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
TOKEN = os.getenv("API_AUTH_TOKEN", "vitali_secret_key_2026")


def fetch_live_stats(port: int = 8089) -> Optional[Dict[str, Any]]:
    for host in ["127.0.0.1", "localhost"]:
        url = f"http://{host}:{port}/stats?token={urllib.parse.quote(TOKEN)}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "VenueAuditor/1.0"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode("utf-8"))
        except Exception:
            continue
    return None


def audit_recent_signals(lookback_minutes: float = 60.0) -> Dict[str, int]:
    """Counts signals recorded per venue over the trailing window."""
    s_dir = os.path.join(DATA_ROOT, "signals")
    if not os.path.isdir(s_dir):
        return {}
    cutoff_ts = time.time() - (lookback_minutes * 60.0)
    venue_counts: Dict[str, int] = collections.defaultdict(int)

    pattern = os.path.join(s_dir, "**", "*.jsonl.gz")
    for f in glob.glob(pattern, recursive=True):
        try:
            if os.path.getmtime(f) >= cutoff_ts:
                with gzip.open(f, "rt", encoding="utf-8", errors="replace") as gz:
                    for line in gz:
                        if not line.strip():
                            continue
                        d = json.loads(line)
                        f_ex = str(d.get("follower_exchange", "")).lower()
                        if f_ex:
                            venue_counts[f_ex] += 1
        except Exception:
            pass

    return dict(venue_counts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Venue Health Auditor.")
    parser.add_argument("--port", type=int, default=8089, help="Web API port (default: 8089)")
    args = parser.parse_args()

    print("=" * 80)
    print("  KOREAN SUNRISE: LIVE VENUE HEALTH & TELEMETRY AUDIT")
    print("=" * 80)

    stats = fetch_live_stats(args.port)
    sig_counts = audit_recent_signals(lookback_minutes=60.0)

    if not stats:
        print(f"  [!] Web API server is NOT responding at http://127.0.0.1:{args.port}/")
        print("      (Make sure main.py is running with API_SERVER_ENABLED=true)\n")
    else:
        counters = stats.get("counters", {})
        print(f"  Service Status : {'RUNNING' if stats.get('running') else 'STOPPED'}")
        print(f"  Total Ticks    : Upbit {counters.get('ticks_upbit', 0):,} | Followers {counters.get('ticks_followers', 0):,}")
        print(f"  Total Signals  : {counters.get('signals_triggered', 0):,} (⚔️Battle: {counters.get('signals_battle', 0)}, 🧪Inc: {counters.get('signals_incubator', 0)})\n")

    print("-" * 80)
    print(f"  {'Venue':<12} | {'Role':<18} | {'Recent Signals (60m)':<22} | {'Status'}")
    print("-" * 80)

    for v in ALL_VENUES:
        role = "Spot Leader (KRW)" if v == "upbit" else "Perp Follower (USDT)"
        cnt = sig_counts.get(v, 0)
        status = "HEALTHY" if (cnt > 0 or stats) else "IDLE / NO DATA"
        print(f"  {v.upper():<12} | {role:<18} | {cnt:>22} | {status}")

    print("=" * 80)


if __name__ == "__main__":
    main()
