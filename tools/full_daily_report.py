"""
tools/full_daily_report.py - High-speed, low-memory quantitative report generator for Korean Sunrise.
Optimized for low-resource VPS and real-time production analysis.

Architecture & Features:
  - Zero-OOM Streaming: Uses in-flight streaming aggregation without loading full datasets in RAM.
  - Multi-Venue Lead-Lag Telemetry: Evaluates Upbit KRW Spot sweeps against all 9 follower perp venues.
  - PnL Scenarios: All Signals, Battle Coins, Calibrated Threshold Filtered, Funded Venues.
  - Generates Markdown Report (reports/daily/report_YYYY-MM-DD.md) and JSON (reports/daily/report_YYYY-MM-DD.json).

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

    # Load battle coins
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

    # Load matrix thresholds
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

    # Telemetry accumulators
    total_signals = 0
    total_bytes = sum(os.path.getsize(f) for f in files) if files else 0
    venues_seen: Set[str] = set()
    coins_seen: Set[str] = set()
    close_reasons = collections.Counter()
    trigger_types = collections.Counter()
    stop_stats = {
        "total_stops": 0,
        "invalidation": 0,
        "adverse_pnl": 0,
        "alpha_decay": 0,
        "other": 0,
        "total_stop_pnl_usd": 0.0,
    }

    def _new_bucket() -> Dict[str, Any]:
        return {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0, "gains": 0.0, "losses": 0.0}

    pnl_scenarios = {
        "all_signals": _new_bucket(),
        "calibrated_threshold_only": _new_bucket(),
        "battle_coins_only": _new_bucket(),
        "funded_venues_only": _new_bucket(),
    }

    sweep = {t: {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0} for t in SWEEP_THRESHOLDS}
    pair_stats = collections.defaultdict(lambda: {
        "count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0,
        "top3_depths": [], "stealth_caps": []
    })
    coin_stats = collections.defaultdict(lambda: {
        "count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0,
        "mfes": [], "t_peaks": [], "top3_depths": [], "stealth_caps": []
    })
    venue_capacities = collections.defaultdict(lambda: {"count": 0, "top3_depths": [], "stealth_caps": []})

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

                    reason = d.get("close_reason", "unknown")
                    close_reasons[reason] += 1
                    trig = d.get("trigger_type", "unknown")
                    trigger_types[trig] += 1

                    stop_hit = d.get("stop_hit", False)
                    stop_type = d.get("stop_type")
                    if not stop_hit and reason.startswith("stop_"):
                        stop_hit = True
                        stop_type = reason.replace("stop_", "")

                    if stop_hit:
                        stop_stats["total_stops"] += 1
                        st_key = str(stop_type or "other").lower()
                        if st_key in ("invalidation", "adverse_pnl", "alpha_decay"):
                            stop_stats[st_key] += 1
                        else:
                            stop_stats["other"] += 1

                    net_pnl_bps = d.get("net_pnl_bps")
                    if net_pnl_bps is None:
                        continue
                    net_pnl_bps = float(net_pnl_bps)
                    gross_edge_bps = float(
                        d.get("gross_exec_edge_bps")
                        or (float(d.get("net_exec_edge_bps", 0.0)) + roundtrip_fee_bps * 0.5)
                    )

                    gross = pos_size * ((net_pnl_bps + roundtrip_fee_bps) / 10000.0)
                    net = pos_size * (net_pnl_bps / 10000.0)
                    is_win = 1 if net > 0 else 0

                    if stop_hit:
                        stop_stats["total_stop_pnl_usd"] += net

                    def _add_to_bucket(b: Dict[str, Any]) -> None:
                        b["count"] += 1
                        b["gross"] += gross
                        b["fee"] += fee_usd
                        b["net"] += net
                        b["wins"] += is_win
                        if net > 0:
                            b["gains"] += net
                        elif net < 0:
                            b["losses"] += abs(net)

                    _add_to_bucket(pnl_scenarios["all_signals"])

                    # Microstructure physical validity check (consistent with evaluator.py)
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
                    req_edge = calibrated_thresholds.get(pk, 20.0)
                    if gross_edge_bps >= req_edge:
                        _add_to_bucket(pnl_scenarios["calibrated_threshold_only"])

                    if coin in battle_coins:
                        _add_to_bucket(pnl_scenarios["battle_coins_only"])

                    if f_ex in FUNDED_VENUES:
                        _add_to_bucket(pnl_scenarios["funded_venues_only"])

                    # Threshold sweep
                    for t in SWEEP_THRESHOLDS:
                        if gross_edge_bps >= t:
                            sw = sweep[t]
                            sw["count"] += 1
                            sw["gross"] += gross
                            sw["fee"] += fee_usd
                            sw["net"] += net
                            sw["wins"] += is_win

                    # Pair stats
                    ps = pair_stats[pk]
                    ps["count"] += 1
                    ps["gross"] += gross
                    ps["fee"] += fee_usd
                    ps["net"] += net
                    ps["wins"] += is_win
                    if f_top3_ask > 0:
                        ps["top3_depths"].append(f_top3_ask)
                        ps["stealth_caps"].append(stealth_cap)

                    # Coin stats
                    cs = coin_stats[coin]
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

                    # Venue capacity stats
                    if f_top3_ask > 0:
                        vc = venue_capacities[f_ex]
                        vc["count"] += 1
                        vc["top3_depths"].append(f_top3_ask)
                        vc["stealth_caps"].append(stealth_cap)

        except Exception as e:
            if not as_json:
                print(f"[!] Warning reading {fpath}: {e}", file=sys.stderr)

    # Format PnL summary
    pnl_summary = {}
    for name, sc in pnl_scenarios.items():
        cnt = sc["count"]
        gains = sc.get("gains", 0.0)
        losses = sc.get("losses", 0.0)
        pf = (gains / losses) if losses > 0 else (99.0 if gains > 0 else 0.0)
        pnl_summary[name] = {
            "trades": cnt,
            "wins": sc["wins"],
            "winrate_pct": round((sc["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "gross_usd": round(sc["gross"], 2),
            "fee_usd": round(sc["fee"], 2),
            "net_pnl_usd": round(sc["net"], 2),
            "profit_factor": round(pf, 2),
        }

    # Format top pairs
    sorted_pairs = []
    for pk, ps in sorted(pair_stats.items(), key=lambda x: x[1]["net"], reverse=True):
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

    # Format top coins
    sorted_coins = []
    for c, cs in sorted(coin_stats.items(), key=lambda x: x[1]["net"], reverse=True):
        cnt = cs["count"]
        avg_mfe = sum(cs["mfes"]) / len(cs["mfes"]) if cs["mfes"] else 0.0
        med_tpk = sorted(cs["t_peaks"])[len(cs["t_peaks"]) // 2] if cs["t_peaks"] else 0.0
        med_top3 = statistics.median(cs["top3_depths"]) if cs["top3_depths"] else 0.0
        med_cap = statistics.median(cs["stealth_caps"]) if cs["stealth_caps"] else 0.0
        sorted_coins.append({
            "coin": c,
            "trades": cnt,
            "winrate_pct": round((cs["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "net_pnl_usd": round(cs["net"], 2),
            "avg_mfe_bps": round(avg_mfe, 1),
            "median_t_peak_sec": round(med_tpk, 2),
            "med_top3_depth_usd": round(med_top3, 1),
            "med_stealth_cap_usd": round(med_cap, 1),
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
        venue_summary.append({
            "venue": vex,
            "signals": vdata["count"],
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
        "close_reasons": dict(close_reasons),
        "trigger_types": dict(trigger_types),
        "stop_loss_telemetry": {
            "total_stops": stop_stats["total_stops"],
            "stop_rate_pct": round((stop_stats["total_stops"] / total_signals * 100.0), 1) if total_signals else 0.0,
            "invalidation_count": stop_stats["invalidation"],
            "adverse_pnl_count": stop_stats["adverse_pnl"],
            "alpha_decay_count": stop_stats["alpha_decay"],
            "other_count": stop_stats["other"],
            "total_stop_pnl_usd": round(stop_stats["total_stop_pnl_usd"], 2),
        },
        "pnl_scenarios": pnl_summary,
        "venue_capacities": venue_summary,
        "sweep_thresholds": {
            t: {
                "trades": sweep[t]["count"],
                "winrate_pct": round((sweep[t]["wins"] / sweep[t]["count"]) * 100.0, 1) if sweep[t]["count"] else 0.0,
                "net_usd": round(sweep[t]["net"], 2),
            }
            for t in SWEEP_THRESHOLDS
        },
        "top_pairs": sorted_pairs[:25],
        "top_coins": sorted_coins[:25],
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
        print(f"\n[OK] Daily report generated and saved to:")
        print(f"  • Markdown: {md_path}")
        print(f"  • JSON:     {json_path}\n")

    return doc


def _render_markdown_report(doc: Dict[str, Any]) -> str:
    m = doc["metadata"]
    p = doc["pnl_scenarios"]
    vc = doc.get("venue_capacities", [])
    sl = doc.get("stop_loss_telemetry", {})
    cr = doc.get("close_reasons", {})
    lines = [
        f"# Korean Sunrise: Суточный Квантовый Отчёт ({m['report_date']})",
        f"*Сгенерирован:* `{m['generated_at']}` | *Окно:* `{m['days_analyzed']} d` | *Размер слота:* `${m['order_size_usd']}` | *Комиссия:* `{m['roundtrip_fee_bps']} bps`",
        "",
        "## 1. Сводные метрики телеметрии",
        f"- **Всего зафиксировано сигналов:** `{m['total_signals_recorded']:,}`",
        f"- **Активных монет с импульсами:** `{m['coins_active']}`",
        f"- **Фолловер-бирж активно:** `{len(m['venues_active'])}` ({', '.join(m['venues_active']) if m['venues_active'] else 'None'})",
        f"- **Архивов данных обработано:** `{m['total_files_scanned']}` (`{m['total_size_mb']} MB`)",
        f"- **Причины закрытия импульсов:** `Converged: {cr.get('converged', 0)}` | `Timeout: {cr.get('timeout', 0)}` | `Reversed: {cr.get('reversed', 0)}` | `Stops: {sl.get('total_stops', 0)}` (Inv: {sl.get('invalidation_count', 0)}, Adv: {sl.get('adverse_pnl_count', 0)}, Decay: {sl.get('alpha_decay_count', 0)})",
        f"- **Триггеры выкупа Upbit:** `Depth Drain: {doc['trigger_types'].get('depth_drain', 0)}` | `Spread Blowout: {doc['trigger_types'].get('spread_blowout', 0)}`",
        "",
        "## 2. PnL Сценарии исполнения",
        "| Сценарий | Сигналов | Win Rate | Gross USD | Комиссии | Net PnL USD | Profit Factor |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    labels = {
        "all_signals": "Все зафиксированные сигналы (Raw)",
        "calibrated_threshold_only": "С калиброванным порогом (Matrix Filter)",
        "battle_coins_only": "Только проверенные монеты (Battle Pool)",
        "funded_venues_only": "Только фондированные биржи (Funded CEXs)",
    }
    for k, row in p.items():
        lbl = labels.get(k, k)
        lines.append(
            f"| **{lbl}** | {row['trades']:,} | {row['winrate_pct']}% | "
            f"${row['gross_usd']:.2f} | ${row['fee_usd']:.2f} | **${row['net_pnl_usd']:.2f}** | {row['profit_factor']} |"
        )

    # Section 3: Stop-Loss Protection Telemetry
    lines.extend([
        "",
        "## 3. Стоп-Лоссы и Защита от Токсичного Потока (Stop-Loss Protection)",
        f"- **Всего сработало стоп-лоссов:** `{sl.get('total_stops', 0):,}` ({sl.get('stop_rate_pct', 0.0)}% от всех импульсов)",
        f"- **Model A: Microstructure Invalidation:** `{sl.get('invalidation_count', 0):,}` (мгновенный сброс при сломе/откате пампа на Upbit)",
        f"- **Model B: Hard Adverse PnL Drawdown:** `{sl.get('adverse_pnl_count', 0):,}` (жесткая отсечка при токсичном движении фолловера)",
        f"- **Model C: Alpha Decay Timeout:** `{sl.get('alpha_decay_count', 0):,}` (сброс по истечении окна передачи импульса при PnL <= 0)",
        f"- **Суммарный PnL по стопам:** `${sl.get('total_stop_pnl_usd', 0.0):.2f}` (контролируемые локальные риски вместо зависания в просадках)",
    ])

    # Section 4: Physical order book capacity
    lines.extend([
        "",
        "## 4. Физическая ёмкость стакана по биржам (Real Order Book Capacity)",
        "| Биржа | Сигналов | Мед. Top-3 стакан ($) | Мед. 15% Ёмкость ($) | P25 Ёмкость ($) | P75 Ёмкость ($) | Мин. ($) | Макс. ($) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    if vc:
        for v in vc:
            lines.append(
                f"| `{v['venue']}` | {v['signals']:,} | ${v['med_top3_depth_usd']:,.1f} | "
                f"**${v['med_stealth_cap_usd']:,.1f}** | ${v['p25_stealth_cap_usd']:,.1f} | "
                f"${v['p75_stealth_cap_usd']:,.1f} | ${v['min_stealth_cap_usd']:,.1f} | ${v['max_stealth_cap_usd']:,.1f} |"
            )
    else:
        lines.append("| *Нет накопленных данных по стаканам* | - | - | - | - | - | - | - |")

    lines.extend([
        "",
        "## 5. Оптимизация порогов спреда (Threshold Sweep)",
        "| Порог (bps) | Сигналов | Win Rate | Net PnL USD |",
        "| :---: | :---: | :---: | :---: |",
    ])
    for t, sw in sorted(doc["sweep_thresholds"].items(), key=lambda x: int(x[0])):
        lines.append(f"| >={t} bps | {sw['trades']:,} | {sw['winrate_pct']}% | ${sw['net_usd']:.2f} |")

    lines.extend([
        "",
        "## 6. Топ прибыльных связок (Upbit Leader <-> Perp Follower)",
        "| Связка (Coin:Upbit-Follower) | Сделок | Win Rate | Gross USD | Net PnL USD | Мед. Top-3 ($) | Мед. 15% Ёмкость ($) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    if doc["top_pairs"]:
        for pr in doc["top_pairs"][:15]:
            lines.append(
                f"| `{pr['pair']}` | {pr['trades']} | {pr['winrate_pct']}% | "
                f"${pr['gross_usd']:.2f} | **${pr['net_pnl_usd']:.2f}** | "
                f"${pr.get('med_top3_depth_usd', 0.0):,.1f} | **${pr.get('med_stealth_cap_usd', 0.0):,.1f}** |"
            )
    else:
        lines.append("| *Нет накопленных сделок за выбранный период* | - | - | - | - | - | - |")

    lines.extend([
        "",
        "## 7. Топ монет по эффективности опережения",
        "| Монета | Сделок | Win Rate | Net PnL USD | Мед. Top-3 ($) | Мед. 15% Ёмкость ($) | Ср. MFE (bps) | Медиана $T_{peak}$ |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    if doc["top_coins"]:
        for c in doc["top_coins"][:15]:
            lines.append(
                f"| **{c['coin']}** | {c['trades']} | {c['winrate_pct']}% | **${c['net_pnl_usd']:.2f}** | "
                f"${c.get('med_top3_depth_usd', 0.0):,.1f} | **${c.get('med_stealth_cap_usd', 0.0):,.1f}** | "
                f"+{c['avg_mfe_bps']} bps | {c['median_t_peak_sec']} s |"
            )
    else:
        lines.append("| *Нет накопленных данных по монетам* | - | - | - | - | - | - | - |")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Daily Quantitative Report.")
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
