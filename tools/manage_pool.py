"""
tools/manage_pool.py - CLI tool for managing Korean Sunrise Incubator, Battle, and Execution Profiles.

Pool Commands:
  python tools/manage_pool.py status
  python tools/manage_pool.py list-battle
  python tools/manage_pool.py list-incubator
  python tools/manage_pool.py pause <COIN> [--reason <REASON>]
  python tools/manage_pool.py resume <COIN>
  python tools/manage_pool.py promote <COIN> [--reason <REASON>]
  python tools/manage_pool.py demote <COIN> [--reason <REASON>]
  python tools/manage_pool.py evaluate [--dry-run] [--days <N>]

Execution Profile Commands:
  python tools/manage_pool.py profile
  python tools/manage_pool.py set-capital <VENUE> <USD>
  python tools/manage_pool.py set-slots <N>
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from pipeline.pool_state import PoolStateManager, OVERRIDES_FILE
from pipeline.evaluator import SunriseEvaluator
from pipeline.profile import TradingProfileManager


def cmd_status(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    snap = psm.get_pool_snapshot()
    if getattr(args, "json", False):
        print(json.dumps(snap, indent=2, ensure_ascii=False))
        return

    summary = snap["summary"]
    settings = snap["settings"]
    up_ts = snap["updated_at"]
    up_str = datetime.datetime.fromtimestamp(up_ts).strftime("%Y-%m-%d %H:%M:%S") if up_ts else "never"

    print("=" * 70)
    print(" KOREAN SUNRISE CONTINUOUS TRADING POOL STATUS")
    print("=" * 70)
    print(f" Last Updated   : {up_str}")
    print(f" Incubator Coins: {summary['incubator']}")
    print(f" Battle Coins   : {summary['battle']} (Active: {summary['battle_active']}, Paused: {summary['paused']})")
    print("-" * 70)
    print(" Sizing & Risk Parameters:")
    print(f"   Min Order Size: ${settings['min_order_usd']:.2f}")
    depth_frac = settings.get("stealth_depth_frac", settings.get("max_depth_frac", 0.15))
    print(f"   Max Depth Frac: {depth_frac*100:.1f}% (top-3 depth)")
    print(f"   Size Cap      : ${settings['size_cap_usd']:,.2f}")
    print("=" * 70)


def cmd_list_battle(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    snap = psm.get_pool_snapshot()
    coins_dict = snap.get("coins", {})
    battle = [c for c in coins_dict.values() if c.get("basket") == "battle"]

    if getattr(args, "json", False):
        print(json.dumps(battle, indent=2, ensure_ascii=False))
        return

    print("=" * 80)
    print(f" BATTLE POOL COINS ({len(battle)})")
    print("=" * 80)
    if not battle:
        print(" [!] Battle pool is currently empty.")
        print("=" * 80)
        return

    print(f" {'Coin':<8} | {'Status':<8} | {'WinRate':<7} | {'Net PnL':<10} | {'Promoted At':<19} | {'Follower Venues'}")
    print("-" * 80)
    for c in battle:
        status_str = "ACTIVE" if c["enabled"] else "PAUSED"
        up_at = datetime.datetime.fromtimestamp(c["promoted_at"]).strftime("%Y-%m-%d %H:%M:%S") if c.get("promoted_at") else "-"
        venues_str = ", ".join(c.get("venues", []))
        print(
            f" {c['coin']:<8} | {status_str:<8} | "
            f"{c.get('win_rate_pct', 0):>6.1f}% | {c.get('median_net_pnl_bps', 0):>7.1f} bps | {up_at:<19} | {venues_str}"
        )
    print("=" * 80)


def cmd_list_incubator(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    snap = psm.get_pool_snapshot()
    coins_dict = snap.get("coins", {})
    incubator = [c for c in coins_dict.values() if c.get("basket") == "incubator"]

    if getattr(args, "json", False):
        print(json.dumps(incubator, indent=2, ensure_ascii=False))
        return

    print("=" * 80)
    print(f" INCUBATOR COINS ({len(incubator)})")
    print("=" * 80)
    print(f" {'Coin':<8} | {'Status':<8} | {'Venues Count':<12} | {'Follower Venues'}")
    print("-" * 80)
    for c in incubator:
        status_str = "ENABLED" if c["enabled"] else "DISABLED"
        v_list = c.get("venues", [])
        print(f" {c['coin']:<8} | {status_str:<8} | {len(v_list):>12} | {', '.join(v_list)}")
    print("=" * 80)


def cmd_pause(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.upper()
    psm.pause_coin(coin, reason=args.reason or "manual_operator_pause")
    print(f"[OK] Paused coin {coin}: {args.reason or 'manual_operator_pause'}")


def cmd_resume(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.upper()
    psm.resume_coin(coin)
    print(f"[OK] Resumed coin {coin}")


def cmd_promote(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.upper()
    ok = psm.promote_to_battle(coin, score=100.0, ev_net_bps=25.0, top3_depth_usd=500.0, win_rate_pct=85.0, min_events=0)
    if ok:
        print(f"[OK] Promoted {coin} to Battle pool.")
    else:
        print(f"[!] Failed to promote {coin}.")


def cmd_demote(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.upper()
    psm.demote_to_incubator(coin, reason=args.reason or "manual_demote")
    print(f"[OK] Demoted {coin} to Incubator pool.")


def cmd_evaluate(psm: PoolStateManager, args: argparse.Namespace) -> None:
    print(f"[*] Running quantitative evaluation across trailing {args.days} days...")
    ev = SunriseEvaluator(data_root=os.path.join(PROJECT_ROOT, "data"), pool_state=psm)
    res = ev.run()
    print(f"[OK] Evaluation complete: {len(res)} coins qualified and promoted to Battle pool.")


def cmd_profile(args: argparse.Namespace) -> None:
    tpm = TradingProfileManager()
    tpm.reload()
    prof = tpm.get_profile()
    if getattr(args, "json", False):
        print(json.dumps(prof, indent=2, ensure_ascii=False))
        return

    print("=" * 70)
    print(" TRADING PROFILE & CAPITAL ALLOCATION")
    print("=" * 70)
    caps = prof.get("capital", {}).get("per_venue_usd", {})
    reserve = prof.get("capital", {}).get("reserve_margin_frac", 0.20)
    slots = prof.get("max_active_slots", 5)
    print(f" Margin Reserve Cushion: {reserve*100:.1f}%")
    print(f" Max Concurrent Slots  : {slots}")
    print("-" * 70)
    print(f" {'Follower Venue':<20} | {'Capital USD':<15} | {'Slot Budget USD'}")
    print("-" * 70)
    total_cap = 0.0
    for v, usd in sorted(caps.items()):
        total_cap += float(usd)
        slot_b = (float(usd) / max(1, slots)) * (1.0 - reserve)
        print(f" {v.upper():<20} | ${float(usd):>12.2f}  | ${slot_b:>13.2f}")
    print("-" * 70)
    print(f" {'TOTAL CAPITAL':<20} | ${total_cap:>12.2f}")
    print("=" * 70)


def cmd_set_capital(args: argparse.Namespace) -> None:
    tpm = TradingProfileManager()
    tpm.set_venue_capital(args.venue, float(args.usd))
    print(f"[OK] Set capital for venue '{args.venue}' to ${float(args.usd):.2f}")


def cmd_set_slots(args: argparse.Namespace) -> None:
    tpm = TradingProfileManager()
    tpm.set_max_slots(int(args.slots))
    print(f"[OK] Set max active slots to {int(args.slots)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Pool & Profile CLI Manager.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # status
    p_stat = sub.add_parser("status", help="Show pool summary status")
    p_stat.add_argument("--json", action="store_true")

    # list-battle
    p_bat = sub.add_parser("list-battle", help="List battle pool coins")
    p_bat.add_argument("--json", action="store_true")

    # list-incubator
    p_inc = sub.add_parser("list-incubator", help="List incubator coins")
    p_inc.add_argument("--json", action="store_true")

    # pause
    p_pau = sub.add_parser("pause", help="Pause trading on a coin")
    p_pau.add_argument("coin", help="Coin symbol")
    p_pau.add_argument("--reason", default="manual_pause")

    # resume
    p_res = sub.add_parser("resume", help="Resume trading on a coin")
    p_res.add_argument("coin", help="Coin symbol")

    # promote
    p_pro = sub.add_parser("promote", help="Promote coin to battle")
    p_pro.add_argument("coin", help="Coin symbol")
    p_pro.add_argument("--reason", default="manual_promote")

    # demote
    p_dem = sub.add_parser("demote", help="Demote coin to incubator")
    p_dem.add_argument("coin", help="Coin symbol")
    p_dem.add_argument("--reason", default="manual_demote")

    # evaluate
    p_ev = sub.add_parser("evaluate", help="Run pool promotion evaluator")
    p_ev.add_argument("--days", type=float, default=7.0)

    # profile
    p_prof = sub.add_parser("profile", help="Show trading capital profile")
    p_prof.add_argument("--json", action="store_true")

    # set-capital
    p_scap = sub.add_parser("set-capital", help="Set capital for a venue")
    p_scap.add_argument("venue", help="Follower venue name")
    p_scap.add_argument("usd", type=float, help="Capital USD")

    # set-slots
    p_slot = sub.add_parser("set-slots", help="Set max active slots")
    p_slot.add_argument("slots", type=int, help="Max slots")

    args = parser.parse_args()

    psm = PoolStateManager()

    if args.cmd == "status":
        cmd_status(psm, args)
    elif args.cmd == "list-battle":
        cmd_list_battle(psm, args)
    elif args.cmd == "list-incubator":
        cmd_list_incubator(psm, args)
    elif args.cmd == "pause":
        cmd_pause(psm, args)
    elif args.cmd == "resume":
        cmd_resume(psm, args)
    elif args.cmd == "promote":
        cmd_promote(psm, args)
    elif args.cmd == "demote":
        cmd_demote(psm, args)
    elif args.cmd == "evaluate":
        cmd_evaluate(psm, args)
    elif args.cmd == "profile":
        cmd_profile(args)
    elif args.cmd == "set-capital":
        cmd_set_capital(args)
    elif args.cmd == "set-slots":
        cmd_set_slots(args)


if __name__ == "__main__":
    main()
