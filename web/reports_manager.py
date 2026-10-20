"""
web/reports_manager.py - Daily reports discovery, metadata extraction, and content serving for Korean Sunrise.
"""
from __future__ import annotations

import datetime
import glob
import json
import os
import re
from typing import Any, Dict, List, Optional

import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports", "daily")

_REPORTS_CACHE: List[Dict[str, Any]] = []
_REPORTS_CACHE_TIME: float = 0.0
_REPORTS_CACHE_LOCK = threading.Lock()


def list_available_reports(force: bool = False, ttl_sec: float = 10.0) -> List[Dict[str, Any]]:
    """Discovers all archived daily reports and extracts card metrics, with in-memory caching."""
    global _REPORTS_CACHE, _REPORTS_CACHE_TIME
    now = time.time()
    with _REPORTS_CACHE_LOCK:
        if not force and _REPORTS_CACHE and (now - _REPORTS_CACHE_TIME < ttl_sec):
            return list(_REPORTS_CACHE)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_files = sorted(glob.glob(os.path.join(REPORTS_DIR, "report_*.md")), reverse=True)

    cards = []
    seen_dates = set()

    for md_path in report_files:
        bn = os.path.basename(md_path)
        m = re.search(r"(\d{4}-?\d{2}-?\d{2})", bn)
        date_raw = m.group(1) if m else "2026-10-18"
        if len(date_raw) == 8 and "-" not in date_raw:
            date_key = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:]}"
        else:
            date_key = date_raw

        if date_key in seen_dates:
            continue
        seen_dates.add(date_key)

        json_path = os.path.join(REPORTS_DIR, f"report_{date_key}.json")
        card = _build_card_from_files(date_key, md_path, json_path)
        cards.append(card)

    cards.sort(key=lambda x: x["date_key"], reverse=True)
    with _REPORTS_CACHE_LOCK:
        _REPORTS_CACHE = cards
        _REPORTS_CACHE_TIME = time.time()
    return cards


def _build_card_from_files(date_key: str, md_path: str, json_path: str) -> Dict[str, Any]:
    """Extracts summary metrics from JSON if available, or provides sensible defaults."""
    card = {
        "id": date_key,
        "date_key": date_key,
        "title": f"Суточный квантовый отчёт: {date_key}",
        "date_from": f"{date_key} 00:00:00 UTC",
        "date_to": f"{date_key} 23:59:59 UTC",
        "filename_md": os.path.basename(md_path),
        "filepath_md": md_path,
        "has_json": os.path.exists(json_path),
        "net_pnl_usd": 0.0,
        "winrate_pct": 0.0,
        "total_trades": 0,
        "total_rows": 0,
        "total_mb": 0.0,
        "venues_breakdown": [],
        "strategies_breakdown": [
            {"name": "1. Upbit Depth Drain", "winrate": 0.0, "net": 0.0},
            {"name": "2. Spread Blowout Sniping", "winrate": 0.0, "net": 0.0},
            {"name": "3. Calibrated Spread Filter", "winrate": 0.0, "net": 0.0},
        ],
    }

    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as jf:
                data = json.load(jf)
            meta = data.get("metadata", {})
            port = data.get("portfolio")
            if port:
                card["total_trades"] = port.get("total_executable_trades", 0)
                card["net_pnl_usd"] = port.get("net_pnl_usd", 0.0)
                card["winrate_pct"] = port.get("winrate_pct", 0.0)
            else:
                sc = data.get("pnl_scenarios", {})
                p_all = sc.get("calibrated_threshold_only") or sc.get("all_signals") or {}
                card["total_trades"] = p_all.get("trades", 0)
                card["net_pnl_usd"] = p_all.get("net_pnl_usd", 0.0)
                card["winrate_pct"] = p_all.get("winrate_pct", 0.0)

            card["total_rows"] = meta.get("total_signals_recorded", 0)
            card["total_mb"] = meta.get("total_size_mb", 0.0)

            # Real multi-strategy breakdown from JSON
            s_breakdown = data.get("strategies_breakdown")
            if s_breakdown:
                s1 = s_breakdown.get("lead_lag", {})
                s2 = s_breakdown.get("fade_fakeout", {})
                s3 = s_breakdown.get("maker_void", {})
                card["strategies_breakdown"] = [
                    {"name": "1. Lead-Lag Momentum", "winrate": s1.get("winrate_pct", 0.0), "net": s1.get("net_pnl_usd", 0.0), "trades": s1.get("trades", 0)},
                    {"name": "2. Fade the Fakeout", "winrate": s2.get("winrate_pct", 0.0), "net": s2.get("net_pnl_usd", 0.0), "trades": s2.get("trades", 0)},
                    {"name": "3. Maker Liquidity", "winrate": s3.get("winrate_pct", 0.0), "net": s3.get("net_pnl_usd", 0.0), "trades": s3.get("trades", 0)},
                ]

            # Top pairs
            top_pairs = data.get("top_pairs", [])
            card["venues_breakdown"] = [
                {
                    "pair": pr["pair"].replace("upbit-", "Upbit <-> "),
                    "trades": pr["trades"],
                    "net": pr["net_pnl_usd"],
                    "winrate": pr["winrate_pct"],
                }
                for pr in top_pairs[:8]
            ]
        except Exception:
            pass

    return card


def get_latest_report_date() -> Optional[str]:
    """Returns the date key of the most recent available report."""
    cards = list_available_reports()
    return cards[0]["date_key"] if cards else None


def get_report_content(report_id: str = "latest") -> Optional[str]:
    """Reads Markdown content of a specific report by date key (or 'latest')."""
    if str(report_id).lower() in ("latest", "today", ""):
        cards = list_available_reports()
        if not cards:
            return None
        safe_id = cards[0]["date_key"]
    else:
        safe_id = re.sub(r"[^0-9\-]", "", str(report_id))
    if not safe_id:
        return None
    fn = f"report_{safe_id}.md"
    fp = os.path.join(REPORTS_DIR, fn)
    if os.path.isfile(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return None
    return None


def get_report_json(report_id: str = "latest") -> Optional[Dict[str, Any]]:
    """Reads full JSON document of a specific report by date key (or 'latest')."""
    if str(report_id).lower() in ("latest", "today", ""):
        cards = list_available_reports()
        if not cards:
            return None
        safe_id = cards[0]["date_key"]
    else:
        safe_id = re.sub(r"[^0-9\-]", "", str(report_id))
    if not safe_id:
        return None
    fn = f"report_{safe_id}.json"
    fp = os.path.join(REPORTS_DIR, fn)
    if os.path.isfile(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None
