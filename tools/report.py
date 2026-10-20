"""
tools/report.py - Server-side CLI report viewer and generator for Korean Sunrise.
Instant terminal access to multi-strategy daily reports, JSON telemetry, and real-time generation.

Usage:
  python tools/report.py                 # Print the latest daily report (Markdown)
  python tools/report.py --json          # Print the latest report as JSON
  python tools/report.py --list          # List all available historical reports
  python tools/report.py --date 2026-10-20 # View report for specific date
  python tools/report.py --generate      # Generate and print a fresh report right now
  python tools/report.py --days 2        # Generate report for last N days
  python tools/report.py --analyze       # Run offline terminal signal analyzer
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import subprocess

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from web.reports_manager import list_available_reports, get_report_content, get_report_json
from tools.full_daily_report import generate_daily_report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Korean Sunrise Server CLI: View, inspect, or generate multi-strategy reports."
    )
    parser.add_argument("--date", "-d", type=str, default="latest", help="Report date (YYYY-MM-DD or 'latest')")
    parser.add_argument("--json", "-j", action="store_true", help="Print raw JSON format instead of Markdown")
    parser.add_argument("--list", "-l", action="store_true", help="List all available generated reports")
    parser.add_argument("--generate", "-g", action="store_true", help="Run full report generation on-demand")
    parser.add_argument("--days", type=float, default=1.0, help="Lookback window in days when generating (default: 1.0)")
    parser.add_argument("--size", type=float, default=10.0, help="Simulated order size USD (default: 10.0)")
    parser.add_argument("--fee-bps", type=float, default=10.0, help="Roundtrip fee in bps (default: 10.0)")
    parser.add_argument("--analyze", "-a", action="store_true", help="Launch interactive multi-strategy analyzer")

    args = parser.parse_args()

    # 1. Launch interactive analyzer
    if args.analyze:
        script = os.path.join(HERE, "analyze_signals.py")
        subprocess.run([sys.executable, script])
        return

    # 2. List available reports
    if args.list:
        cards = list_available_reports()
        if not cards:
            print("No daily reports found in 'reports/daily/'. Run 'python tools/report.py --generate' to create one.")
            return
        print("=" * 78)
        print(f" KOREAN SUNRISE — ARCHIVED DAILY REPORTS ({len(cards)} available)")
        print("=" * 78)
        print(f"{'Date':<12} | {'Trades':>7} | {'Win Rate':>8} | {'Net PnL ($)':>11} | {'File'}")
        print("-" * 78)
        for c in cards:
            print(
                f"{c['date_key']:<12} | {c['total_trades']:>7d} | {c['winrate_pct']:>7.1f}% | "
                f"${c['net_pnl_usd']:>10.2f} | {c['filename_md']}"
            )
        return

    # 3. Generate fresh report
    if args.generate:
        print(f"[*] Generating fresh Korean Sunrise report (lookback={args.days}d, size=${args.size}, fee={args.fee_bps}bps)...")
        doc = generate_daily_report(
            days_back=args.days,
            pos_size=args.size,
            roundtrip_fee_bps=args.fee_bps,
            as_json=args.json,
        )
        if args.json:
            print(json.dumps(doc, indent=2, ensure_ascii=False))
        return

    # 4. View existing report
    target = args.date
    if args.json:
        doc = get_report_json(target)
        if doc:
            print(json.dumps(doc, indent=2, ensure_ascii=False))
        else:
            print(f"[!] Report '{target}' not found. Run 'python tools/report.py --generate' to build it.", file=sys.stderr)
            sys.exit(1)
    else:
        content = get_report_content(target)
        if content:
            print(content)
        else:
            print(f"[!] Report '{target}' not found.", file=sys.stderr)
            print("[*] Generating report now...")
            generate_daily_report(days_back=1.0, pos_size=args.size, roundtrip_fee_bps=args.fee_bps, as_json=False)


if __name__ == "__main__":
    main()
