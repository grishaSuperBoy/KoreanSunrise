"""
tools/manage_pool.py — CLI management tool for Korean Sunrise pools & execution profiles.

Pool Commands:
  python tools/manage_pool.py status
  python tools/manage_pool.py list-battle
  python tools/manage_pool.py list-incubator
  python tools/manage_pool.py pause <COIN> [--reason <REASON>]
  python tools/manage_pool.py resume <COIN>
  python tools/manage_pool.py evaluate [--days <N>] [--min-events <M>] [--dry-run]
  python tools/manage_pool.py set-param [--min-order <USD>] [--stealth-frac <FRAC>] [--size-cap <USD>]

Profile Commands:
  python tools/manage_pool.py profile
  python tools/manage_pool.py pairs
  python tools/manage_pool.py set-capital <VENUE> <USD>
  python tools/manage_pool.py set-slots <N>
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
from typing import Any, Dict, List

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from pipeline.pool_state import (
    PoolStateManager,
    _read_json,
    _write_json_atomic,
    OVERRIDES_FILE,
)
from pipeline.evaluator import SunriseEvaluator
from pipeline.profile import TradingProfileManager


def cmd_status(psm: PoolStateManager, tpm: TradingProfileManager, args: argparse.Namespace) -> None:
    psm.reload()
    tpm.reload()
    snap = psm.get_pool_snapshot()
    prof = tpm.get_profile()

    if args.json:
        print(json.dumps({"pool": snap, "profile": prof}, indent=2, ensure_ascii=False))
        return

    summary = snap["summary"]
    settings = snap["settings"]
    up_ts = snap["updated_at"]
    up_str = datetime.datetime.fromtimestamp(up_ts).strftime("%Y-%m-%d %H:%M:%S") if up_ts else "never"

    print("=" * 78)
    print(" 🌅 KOREAN SUNRISE — CONTINUOUS POOL & EXECUTION STATUS")
    print("=" * 78)
    print(f" Last Updated      : {up_str}")
    print(f" Incubator Pool    : {summary['incubator']} coins (Telemetry collection)")
    print(f" Battle Pool       : {summary['battle']} coins (Active: {summary['battle_active']}, Paused: {summary['paused']})")
    print("-" * 78)
    print(" Sizing & Gate Parameters (Zero Hardcode):")
    print(f"   Min Order Size  : ${settings['min_order_usd']:.2f}")
    print(f"   Stealth Fraction: {settings['stealth_depth_frac']*100:.1f}% (of top-3 follower ask depth)")
    print(f"   Max Position Cap: ${settings['size_cap_usd']:,.2f}")
    print("-" * 78)
    caps = prof.get("capital", {}).get("per_venue_usd", {})
    cap_strs = [f"{v.capitalize()}: ${amt:.2f}" for v, amt in caps.items()]
    print(f" Active Venues     : {', '.join(prof.get('active_venues', []))}")
    print(f" Capital per Venue : {' | '.join(cap_strs)}")
    print(f" Max Active Slots  : {prof.get('max_active_slots', 3)} concurrent positions")
    print("=" * 78)

    paused = [c for c in snap["coins"].values() if not c["enabled"]]
    if paused:
        print("\nPAUSED COINS (Circuit Breaker):")
        for c in paused:
            print(f"  • {c['coin']:<12} | reason: {c.get('pause_reason') or 'unspecified'} (basket: {c['basket']})")
    print()


def cmd_list_battle(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    coins = psm.get_battle_coins()
    if args.json:
        print(json.dumps(coins, indent=2, ensure_ascii=False))
        return

    print("=" * 95)
    print(f" ⚔️ ACTIVE BATTLE POOL ({len(coins)} QUALIFIED COINS)")
    print("=" * 95)
    if not coins:
        print("  No qualified battle coins found in live_trading_pool.json.")
        print("  Run 'python tools/manage_pool.py evaluate' to score recorded signals and promote candidates.")
        print("=" * 95)
        return

    hdr = f"{'#':<3} {'Coin':<10} {'Exp PnL':>10} {'WinRate':>9} {'ProfitFact':>11} {'Depth($)':>10} {'Size($)':>9} {'Signals':>8} {'Venues':<20}"
    print(hdr)
    print("-" * 95)
    for i, c in enumerate(coins, 1):
        v_str = ",".join(c.get("venues", []))
        print(
            f"{i:<3} {c['coin']:<10} "
            f"{c.get('mean_net_pnl_bps', 0):>+9.1f}bps "
            f"{c.get('win_rate_pct', 0):>8.1f}% "
            f"{c.get('profit_factor', 0):>10.2f} "
            f"${c.get('top3_depth_usd', 0):>9,.0f} "
            f"${c.get('recommended_size_usd', 0):>8.2f} "
            f"{c.get('event_count', 0):>8} "
            f"{v_str:<20}"
        )
    print("=" * 95)


def cmd_list_incubator(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    snap = psm.get_pool_snapshot()
    incubator_coins = [c for c in snap["coins"].values() if c["basket"] == "incubator"]
    if args.json:
        print(json.dumps(incubator_coins, indent=2, ensure_ascii=False))
        return

    print("=" * 78)
    print(f" 🧪 INCUBATOR CANDIDATES ({len(incubator_coins)} COINS)")
    print("=" * 78)
    if not incubator_coins:
        print("  No incubator coins loaded.")
        print("  Run 'python tools/screen_universe.py' to generate universe_live.json.")
        print("=" * 78)
        return

    for i, c in enumerate(incubator_coins[:50], 1):
        v_str = ",".join(c.get("venues", []))
        st = "ACTIVE" if c["enabled"] else f"PAUSED ({c.get('pause_reason')})"
        print(f"  {i:3d}. {c['coin']:<10} | {st:<15} | venues: {v_str}")
    if len(incubator_coins) > 50:
        print(f"  ... and {len(incubator_coins) - 50} more coins.")
    print("=" * 78)


def cmd_pause(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.strip().upper()
    reason = args.reason or "manual CLI pause"
    psm.pause_coin(coin, reason=reason)
    print(f"✓ Paused coin: {coin} (reason: {reason})")
    print("  Persisted to runtime_overrides.json. Signals will not be actionable.")


def cmd_resume(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.strip().upper()
    psm.resume_coin(coin)
    print(f"✓ Resumed coin: {coin}")


def cmd_evaluate(args: argparse.Namespace) -> None:
    data_root = os.getenv("DATA_ROOT", os.path.join(PROJECT_ROOT, "data"))
    ev = SunriseEvaluator(
        data_root=data_root,
        days_back=args.days,
        min_events=args.min_events,
    )
    ranked = ev.run(dry_run=args.dry_run)
    if args.dry_run:
        print(f"\n[DRY-RUN] Evaluated {len(ranked)} qualifying coins without modifying live_trading_pool.json.")
    else:
        print(f"\n✓ Evaluation complete. {len(ranked)} coins updated in Battle pool.")


def cmd_profile(tpm: TradingProfileManager, args: argparse.Namespace) -> None:
    tpm.reload()
    prof = tpm.get_profile()
    if args.json:
        print(json.dumps(prof, indent=2, ensure_ascii=False))
        return

    caps = prof.get("capital", {}).get("per_venue_usd", {})
    print("=" * 65)
    print(" 💼 EXECUTION PROFILE & CAPITAL ALLOCATION")
    print("=" * 65)
    print(f" Active Venues       : {', '.join(prof.get('active_venues', []))}")
    print(f" Margin Cushion      : {prof.get('capital', {}).get('reserve_margin_frac', 0.2)*100:.0f}%")
    print(f" Max Active Slots    : {prof.get('max_active_slots', 3)}")
    print(f" Min Order Notional  : ${prof.get('min_order_usd', 10.0):.2f}")
    print(f" Max Single Position : ${prof.get('max_single_position_usd', 500.0):.2f}")
    print("-" * 65)
    print(" Deposits per Venue:")
    for v, amt in caps.items():
        print(f"   • {v:<12} : ${amt:.2f}")
    print("=" * 65)


def cmd_pairs(psm: PoolStateManager, tpm: TradingProfileManager, args: argparse.Namespace) -> None:
    psm.reload()
    tpm.reload()
    battle_coins = psm.get_battle_coins()

    print("=" * 85)
    print(" 🎯 ACTIONABLE BATTLE PAIRS MATCHED WITH ACTIVE VENUE DEPOSITS")
    print("=" * 85)
    if not battle_coins:
        print("  No active battle coins currently qualified.")
        print("=" * 85)
        return

    print(f"{'Coin':<10} | {'Venues':<18} | {'Top-3 Depth':>12} | {'Venue Size ($)':<35}")
    print("-" * 85)
    for c in battle_coins:
        coin = c["coin"]
        venues = c.get("venues", [])
        depth = c.get("top3_depth_usd", 0.0)
        size_parts = []
        for v in venues:
            sz = tpm.calculate_stealth_order_size(v, depth)
            if sz:
                size_parts.append(f"{v}: ${sz:.2f}")
        size_str = " | ".join(size_parts) if size_parts else "depth too low"
        print(f"{coin:<10} | {','.join(venues):<18} | ${depth:>10,.0f} | {size_str:<35}")
    print("=" * 85)


def cmd_set_capital(tpm: TradingProfileManager, args: argparse.Namespace) -> None:
    venue = args.venue.lower().strip()
    amount = float(args.amount)
    tpm.set_venue_capital(venue, amount)
    print(f"✓ Capital for {venue} updated to ${amount:.2f}")


def cmd_set_slots(tpm: TradingProfileManager, args: argparse.Namespace) -> None:
    slots = int(args.slots)
    tpm.set_max_slots(slots)
    print(f"✓ Max concurrent active slots updated to {slots}")


def cmd_set_param(args: argparse.Namespace) -> None:
    overrides = _read_json(OVERRIDES_FILE, default={})
    if args.min_order is not None:
        overrides["min_order_usd"] = float(args.min_order)
    if args.stealth_frac is not None:
        overrides["stealth_depth_frac"] = float(args.stealth_frac)
    if args.size_cap is not None:
        overrides["size_cap_usd"] = float(args.size_cap)
    _write_json_atomic(OVERRIDES_FILE, overrides)
    print("✓ Sizing & risk parameters updated in runtime_overrides.json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Pool & Profile CLI Manager")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    sub = parser.add_subparsers(dest="command")

    # Pool commands
    sub.add_parser("status", help="Show overview of Incubator, Battle, and sizing parameters")
    sub.add_parser("list-battle", help="List active Battle pool coins")
    sub.add_parser("list-incubator", help="List candidate Incubator coins")

    p_pause = sub.add_parser("pause", help="Pause a coin (circuit breaker)")
    p_pause.add_argument("coin", help="Base coin symbol (e.g. SAND)")
    p_pause.add_argument("--reason", default="manual CLI pause", help="Pause reason")

    p_resume = sub.add_parser("resume", help="Resume a paused coin")
    p_resume.add_argument("coin", help="Base coin symbol")

    p_eval = sub.add_parser("evaluate", help="Score recorded signals and promote to Battle")
    p_eval.add_argument("--days", type=int, default=7, help="Days of logs to scan (default: 7)")
    p_eval.add_argument("--min-events", type=int, default=5, help="Minimum qualifying events (default: 5)")
    p_eval.add_argument("--dry-run", action="store_true", help="Score without modifying live_trading_pool.json")

    p_param = sub.add_parser("set-param", help="Update sizing & risk parameters")
    p_param.add_argument("--min-order", type=float, help="Min order USD")
    p_param.add_argument("--stealth-frac", type=float, help="Stealth fraction of top-3 depth")
    p_param.add_argument("--size-cap", type=float, help="Size cap USD")

    # Profile commands
    sub.add_parser("profile", help="Show execution profile and venue deposits")
    sub.add_parser("pairs", help="List actionable battle pairs with computed order sizes")

    p_cap = sub.add_parser("set-capital", help="Set deposit capital for a venue")
    p_cap.add_argument("venue", help="Follower venue (binance, bybit)")
    p_cap.add_argument("amount", type=float, help="Capital in USD")

    p_slots = sub.add_parser("set-slots", help="Set max concurrent active position slots")
    p_slots.add_argument("slots", type=int, help="Number of slots")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return

    psm = PoolStateManager()
    tpm = TradingProfileManager()

    if args.command == "status":
        cmd_status(psm, tpm, args)
    elif args.command == "list-battle":
        cmd_list_battle(psm, args)
    elif args.command == "list-incubator":
        cmd_list_incubator(psm, args)
    elif args.command == "pause":
        cmd_pause(psm, args)
    elif args.command == "resume":
        cmd_resume(psm, args)
    elif args.command == "evaluate":
        cmd_evaluate(args)
    elif args.command == "set-param":
        cmd_set_param(args)
    elif args.command == "profile":
        cmd_profile(tpm, args)
    elif args.command == "pairs":
        cmd_pairs(psm, tpm, args)
    elif args.command == "set-capital":
        cmd_set_capital(tpm, args)
    elif args.command == "set-slots":
        cmd_set_slots(tpm, args)


if __name__ == "__main__":
    main()
