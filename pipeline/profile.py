"""
pipeline/profile.py — Trading Profile & Capital Allocation Manager for Korean Sunrise.

Responsibilities:
  - Manages trading_profile.json (capital per follower venue, margin reserve, slots).
  - Dynamically calculates executable stealth order sizes matched to the operator's
    actual available deposit on Binance and Bybit.
  - Enforces margin reserve cushions (20%) and slot diversification.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Dict, List, Optional

log = logging.getLogger("pipeline.profile")

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROFILE_FILE = os.path.join(HERE, "trading_profile.json")

DEFAULT_PROFILE: Dict[str, Any] = {
    "_meta": {
        "description": "Korean Sunrise Execution Profile: Venue deposits, margin cushions, and slot sizing",
        "version": "1.0",
    },
    "capital": {
        "per_venue_usd": {
            "binance": 100.0,
            "bybit": 100.0,
            "okx": 100.0,
            "gateio": 100.0,
            "mexc": 100.0,
            "kucoin": 100.0,
            "bingx": 100.0,
            "htx": 100.0,
            "pionex": 100.0,
        },
        "reserve_margin_frac": 0.20,  # 20% margin cushion against drawdowns
    },
    "active_venues": [
        "binance",
        "bybit",
        "okx",
        "gateio",
        "mexc",
        "kucoin",
        "bingx",
        "htx",
        "pionex",
    ],
    "max_active_slots": 5,           # Max concurrent positions
    "min_order_usd": 10.0,           # Exchange min notional
    "stealth_depth_frac": 0.15,      # Universal stealth sizing rule (15% top-3 ask depth)
    "max_single_position_usd": 500.0,# Hard ceiling per position
}


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        log.warning(f"[profile] Failed to read {path}: {e}")
        return default


def _write_json_atomic(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f"[profile] Atomic write failed for {path}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


class TradingProfileManager:
    """Manages active trading venues, deposit allocation, and position sizing."""

    def __init__(self, profile_file: str = PROFILE_FILE) -> None:
        self.profile_file = profile_file
        self._lock = threading.RLock()
        self._profile: Dict[str, Any] = {}
        self.reload()

    def reload(self) -> None:
        with self._lock:
            data = _read_json(self.profile_file)
            if not data:
                log.info(f"[profile] Initializing default trading profile at {self.profile_file}")
                data = json.loads(json.dumps(DEFAULT_PROFILE))
                _write_json_atomic(self.profile_file, data)
            self._profile = data

    def get_profile(self) -> Dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._profile))

    def update_profile(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            def _merge(src: dict, patch: dict) -> None:
                for k, v in patch.items():
                    if isinstance(v, dict) and k in src and isinstance(src[k], dict):
                        _merge(src[k], v)
                    else:
                        src[k] = v

            _merge(self._profile, updates)
            _write_json_atomic(self.profile_file, self._profile)
            return self.get_profile()

    def set_venue_capital(self, venue: str, capital_usd: float) -> None:
        with self._lock:
            venue = venue.lower().strip()
            caps = self._profile.setdefault("capital", {}).setdefault("per_venue_usd", {})
            caps[venue] = max(0.0, float(capital_usd))
            _write_json_atomic(self.profile_file, self._profile)
            log.info(f"[profile] Updated capital for {venue}: ${capital_usd:.2f}")

    def set_max_slots(self, slots: int) -> None:
        with self._lock:
            self._profile["max_active_slots"] = max(1, int(slots))
            _write_json_atomic(self.profile_file, self._profile)
            log.info(f"[profile] Updated max_active_slots: {slots}")

    def is_venue_active(self, venue: str) -> bool:
        with self._lock:
            active = [v.lower() for v in self._profile.get("active_venues", [])]
            return venue.lower() in active

    def calculate_stealth_order_size(
        self,
        venue: str,
        follower_top3_ask_usd: float,
    ) -> Optional[float]:
        """
        Dynamically calculates actionable order size in USD:
          1. Slot Budget = (Venue_Capital / Slots) * (1 - Reserve_Margin)
          2. Stealth Size = 0.15 * Top-3 Ask Depth
          3. Sized Order = min(Slot Budget, Stealth Size, Max Position Cap)
          Returns None if book depth cannot support the exchange minimum order.
        """
        with self._lock:
            venue = venue.lower()
            if not self.is_venue_active(venue):
                return None

            cap_dict = self._profile.get("capital", {}).get("per_venue_usd", {})
            venue_cap = float(cap_dict.get(venue, 50.0))
            if venue_cap <= 0:
                return None

            reserve_margin = float(self._profile.get("capital", {}).get("reserve_margin_frac", 0.20))
            slots = max(1, int(self._profile.get("max_active_slots", 3)))
            stealth_frac = float(self._profile.get("stealth_depth_frac", 0.15))
            min_order = float(self._profile.get("min_order_usd", 10.0))
            max_cap = float(self._profile.get("max_single_position_usd", 500.0))

            slot_budget = (venue_cap / slots) * (1.0 - reserve_margin)
            stealth_size = stealth_frac * follower_top3_ask_usd

            size = min(slot_budget, stealth_size, max_cap)
            if size < min_order:
                # If slot budget allows min_order and depth is within 80% of min_order requirement
                if slot_budget >= min_order and stealth_size >= (min_order * 0.8):
                    return min_order
                return None

            return round(size, 2)
