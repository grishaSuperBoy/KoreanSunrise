"""
pipeline/pool_state.py — Atomic state manager for Korean Sunrise trading pools.

Responsibilities:
  - Atomic read/write of live_trading_pool.json and runtime_overrides.json.
  - Manages two distinct baskets:
      * Incubator: Broad universe of candidate coins collecting telemetry.
      * Battle: Statistically proven coins with positive Net EV and Profit Factor.
  - Applies runtime_overrides.json (manual pauses, size caps, risk parameters).
  - Calculates dynamic position sizing via Stealth Sizing rule (15% top-3 ask depth).
  - Provides query methods for KoreanEngine and StreamManager.

File contracts:
  universe_live.json     — Screened incubator candidate pool (Upbit <-> Binance/Bybit).
  live_trading_pool.json — Battle pool promoted by SunriseEvaluator.
  runtime_overrides.json — Operator manual circuit breaker pauses & global overrides.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

log = logging.getLogger("pipeline.pool_state")

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UNIVERSE_FILE = os.path.join(HERE, "universe_live.json")
BATTLE_POOL_FILE = os.path.join(HERE, "live_trading_pool.json")
OVERRIDES_FILE = os.path.join(HERE, "runtime_overrides.json")

# Default sizing & gate rules
DEFAULT_MIN_ORDER_USD = 10.0      # Exchange minimum order size ($5-$10)
DEFAULT_STEALTH_FRAC = 0.15       # Max 15% of top-3 follower ask depth
DEFAULT_SIZE_CAP_USD = 10000.0    # Max budget cap per trade
DEFAULT_MIN_DEPTH_USD = 66.7      # Min top-3 depth ($10 / 0.15) to support minimum order


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        log.warning(f"[pool_state] Failed to read {path}: {e}")
        return default


def _write_json_atomic(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f"[pool_state] Atomic write failed for {path}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


class CoinEntry:
    """In-memory representation of a coin in either Incubator or Battle basket."""
    __slots__ = (
        "coin", "venues", "basket", "enabled", "pause_reason",
        "top3_depth_usd", "min_order_usd", "max_order_usd",
        "mean_net_pnl_bps", "median_net_pnl_bps", "win_rate_pct",
        "profit_factor", "promoted_at", "event_count",
    )

    def __init__(
        self,
        coin: str,
        venues: List[str],
        basket: str = "incubator",
        enabled: bool = True,
        pause_reason: Optional[str] = None,
        top3_depth_usd: float = 0.0,
        min_order_usd: float = DEFAULT_MIN_ORDER_USD,
        max_order_usd: float = DEFAULT_SIZE_CAP_USD,
        mean_net_pnl_bps: float = 0.0,
        median_net_pnl_bps: float = 0.0,
        win_rate_pct: float = 0.0,
        profit_factor: float = 0.0,
        promoted_at: float = 0.0,
        event_count: int = 0,
    ) -> None:
        self.coin = coin
        self.venues = venues
        self.basket = basket            # "incubator" | "battle"
        self.enabled = enabled
        self.pause_reason = pause_reason
        self.top3_depth_usd = top3_depth_usd
        self.min_order_usd = min_order_usd
        self.max_order_usd = max_order_usd
        self.mean_net_pnl_bps = mean_net_pnl_bps
        self.median_net_pnl_bps = median_net_pnl_bps
        self.win_rate_pct = win_rate_pct
        self.profit_factor = profit_factor
        self.promoted_at = promoted_at
        self.event_count = event_count

    def compute_size_usd(
        self,
        stealth_frac: float = DEFAULT_STEALTH_FRAC,
        min_order: float = DEFAULT_MIN_ORDER_USD,
        size_cap: float = DEFAULT_SIZE_CAP_USD,
    ) -> Optional[float]:
        """
        Calculates recommended execution size using Universal Stealth Sizing:
        size = min(size_cap, max(min_order, stealth_frac * top3_depth))
        Returns None if book is too shallow to support min_order safely.
        """
        if self.top3_depth_usd <= 0:
            return None
        candidate = stealth_frac * self.top3_depth_usd
        if candidate < min_order:
            return None
        return min(max(candidate, min_order), size_cap)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "coin": self.coin,
            "venues": self.venues,
            "basket": self.basket,
            "enabled": self.enabled,
            "pause_reason": self.pause_reason,
            "top3_depth_usd": round(self.top3_depth_usd, 1),
            "min_order_usd": round(self.min_order_usd, 2),
            "max_order_usd": round(self.max_order_usd, 2),
            "mean_net_pnl_bps": round(self.mean_net_pnl_bps, 2),
            "median_net_pnl_bps": round(self.median_net_pnl_bps, 2),
            "win_rate_pct": round(self.win_rate_pct, 1),
            "profit_factor": round(self.profit_factor, 2),
            "promoted_at": self.promoted_at,
            "event_count": self.event_count,
        }


class PoolStateManager:
    """
    Central authority managing Incubator candidates and Battle pool coins.
    Thread-safe for real-time reads and atomic updates.
    """

    def __init__(
        self,
        universe_file: str = UNIVERSE_FILE,
        battle_pool_file: str = BATTLE_POOL_FILE,
        overrides_file: str = OVERRIDES_FILE,
    ) -> None:
        self._universe_file = universe_file
        self._battle_pool_file = battle_pool_file
        self._overrides_file = overrides_file

        self._lock = threading.RLock()
        self._coins: Dict[str, CoinEntry] = {}
        self._overrides: Dict[str, Any] = {}
        self._universe_raw: Dict[str, Any] = {}
        self._last_reload: float = 0.0

    def reload(self) -> None:
        """Reloads all JSON configuration files and merges state atomically."""
        with self._lock:
            self._overrides = _read_json(self._overrides_file, default={})
            self._load_incubator()
            self._load_battle_pool()
            self._apply_overrides()
            self._last_reload = time.time()
            log.info(
                f"[pool_state] Reloaded: {self._count_basket('incubator')} incubator, "
                f"{self._count_basket('battle')} battle "
                f"({sum(1 for c in self._coins.values() if c.basket == 'battle' and c.enabled)} active)"
            )

    def get_all_coins(self) -> List[str]:
        """All base coins across both Incubator and Battle baskets."""
        with self._lock:
            return sorted(self._coins.keys())

    def get_raw_universe_doc(self) -> Dict[str, Any]:
        """Raw dictionary from universe_live.json."""
        with self._lock:
            return dict(self._universe_raw)

    def is_battle_coin(self, coin: str) -> bool:
        """Returns True if coin is currently active in the Battle pool."""
        with self._lock:
            entry = self._coins.get(coin.upper())
            return bool(entry and entry.basket == "battle" and entry.enabled)

    def get_coin_entry(self, coin: str) -> Optional[CoinEntry]:
        with self._lock:
            return self._coins.get(coin.upper())

    def get_battle_coins(self) -> List[Dict[str, Any]]:
        """Returns all enabled Battle coins with dynamic stealth sizing."""
        with self._lock:
            overrides = self._overrides
            stealth_frac = float(overrides.get("stealth_depth_frac", DEFAULT_STEALTH_FRAC))
            min_order = float(overrides.get("min_order_usd", DEFAULT_MIN_ORDER_USD))
            size_cap = float(overrides.get("size_cap_usd", DEFAULT_SIZE_CAP_USD))

            result = []
            for entry in sorted(self._coins.values(), key=lambda e: e.mean_net_pnl_bps, reverse=True):
                if entry.basket != "battle" or not entry.enabled:
                    continue
                size = entry.compute_size_usd(stealth_frac, min_order, size_cap)
                d = entry.to_dict()
                d["recommended_size_usd"] = round(size, 2) if size else min_order
                result.append(d)
            return result

    def get_pool_snapshot(self) -> Dict[str, Any]:
        """Full snapshot of the pool state for reporting and CLI inspection."""
        with self._lock:
            overrides = self._overrides
            return {
                "updated_at": round(self._last_reload, 3),
                "settings": {
                    "min_order_usd": float(overrides.get("min_order_usd", DEFAULT_MIN_ORDER_USD)),
                    "stealth_depth_frac": float(overrides.get("stealth_depth_frac", DEFAULT_STEALTH_FRAC)),
                    "size_cap_usd": float(overrides.get("size_cap_usd", DEFAULT_SIZE_CAP_USD)),
                    "min_depth_usd": float(overrides.get("min_depth_usd", DEFAULT_MIN_DEPTH_USD)),
                },
                "coins": {coin: entry.to_dict() for coin, entry in self._coins.items()},
                "summary": {
                    "incubator": self._count_basket("incubator"),
                    "battle": self._count_basket("battle"),
                    "battle_active": sum(1 for c in self._coins.values() if c.basket == "battle" and c.enabled),
                    "paused": sum(1 for c in self._coins.values() if not c.enabled),
                },
            }

    def pause_coin(self, coin: str, reason: str = "manual") -> bool:
        """Pauses a coin (circuit breaker) and persists to runtime_overrides.json."""
        with self._lock:
            coin = coin.upper()
            if coin in self._coins:
                self._coins[coin].enabled = False
                self._coins[coin].pause_reason = reason

            overrides = _read_json(self._overrides_file, default={})
            paused = overrides.get("paused_coins", {})
            paused[coin] = {"reason": reason, "since": time.time()}
            overrides["paused_coins"] = paused
            _write_json_atomic(self._overrides_file, overrides)
            self._overrides = overrides
            log.info(f"[pool_state] Paused coin: {coin} (reason={reason})")
            return True

    def resume_coin(self, coin: str) -> bool:
        """Resumes a paused coin."""
        with self._lock:
            coin = coin.upper()
            if coin in self._coins:
                self._coins[coin].enabled = True
                self._coins[coin].pause_reason = None

            overrides = _read_json(self._overrides_file, default={})
            paused = overrides.get("paused_coins", {})
            paused.pop(coin, None)
            overrides["paused_coins"] = paused
            _write_json_atomic(self._overrides_file, overrides)
            self._overrides = overrides
            log.info(f"[pool_state] Resumed coin: {coin}")
            return True

    def promote_to_battle(self, qualifying_stats: List[Dict[str, Any]]) -> None:
        """
        Promotes qualifying coins from Incubator to Battle based on verified
        quantitative performance. Demotes degraded coins back to Incubator.
        """
        with self._lock:
            overrides = self._overrides
            qualified_coins: Set[str] = set()

            for stat in qualifying_stats:
                coin = str(stat["coin"]).upper()
                qualified_coins.add(coin)

                entry = self._coins.get(coin)
                if entry is None:
                    entry = CoinEntry(coin=coin, venues=stat.get("venues", ["binance", "bybit"]))
                    self._coins[coin] = entry

                entry.basket = "battle"
                entry.venues = stat.get("venues", entry.venues)
                entry.top3_depth_usd = float(stat.get("top3_depth_usd", 0.0))
                entry.mean_net_pnl_bps = float(stat.get("mean_net_pnl_bps", 0.0))
                entry.median_net_pnl_bps = float(stat.get("median_net_pnl_bps", 0.0))
                entry.win_rate_pct = float(stat.get("win_rate_pct", 0.0))
                entry.profit_factor = float(stat.get("profit_factor", 0.0))
                entry.event_count = int(stat.get("event_count", 0))
                entry.promoted_at = time.time()

                if coin not in overrides.get("paused_coins", {}):
                    entry.enabled = True
                    entry.pause_reason = None

            # Demote battle coins that failed qualification this cycle
            for coin, entry in self._coins.items():
                if entry.basket == "battle" and coin not in qualified_coins:
                    entry.basket = "incubator"
                    log.info(f"[pool_state] Demoted {coin} from Battle -> Incubator (failed criteria)")

            self._persist_battle_pool()
            log.info(
                f"[pool_state] Updated Battle pool: {len(qualified_coins)} coins qualified. "
                f"Total Battle: {self._count_basket('battle')}"
            )

    # ------------------------------------------------------------------ Private
    def _load_incubator(self) -> None:
        doc = _read_json(self._universe_file, default={})
        self._universe_raw = doc
        if not doc:
            return

        coins = doc.get("coins", [])
        binance_map = doc.get("binance_sym_map", {})
        bybit_map = doc.get("bybit_sym_map", {})

        # Map coin -> active follower venues
        coin_venues: Dict[str, Set[str]] = {}
        for c in coins:
            coin_venues.setdefault(c, set())

        for raw, item in binance_map.items():
            c = item[0] if isinstance(item, (list, tuple)) else item
            coin_venues.setdefault(c, set()).add("binance")

        for raw, item in bybit_map.items():
            c = item[0] if isinstance(item, (list, tuple)) else item
            coin_venues.setdefault(c, set()).add("bybit")

        for coin, venues in coin_venues.items():
            if coin in self._coins:
                self._coins[coin].venues = sorted(venues)
            else:
                self._coins[coin] = CoinEntry(
                    coin=coin,
                    venues=sorted(venues),
                    basket="incubator",
                )

    def _load_battle_pool(self) -> None:
        doc = _read_json(self._battle_pool_file, default={})
        if not doc:
            return

        for coin, meta in doc.get("coins", {}).items():
            coin = coin.upper()
            entry = self._coins.get(coin)
            if entry is None:
                entry = CoinEntry(coin=coin, venues=meta.get("venues", []))
                self._coins[coin] = entry

            entry.basket = "battle"
            entry.venues = meta.get("venues", entry.venues)
            entry.top3_depth_usd = float(meta.get("top3_depth_usd", 0.0))
            entry.mean_net_pnl_bps = float(meta.get("mean_net_pnl_bps", 0.0))
            entry.median_net_pnl_bps = float(meta.get("median_net_pnl_bps", 0.0))
            entry.win_rate_pct = float(meta.get("win_rate_pct", 0.0))
            entry.profit_factor = float(meta.get("profit_factor", 0.0))
            entry.event_count = int(meta.get("event_count", 0))
            entry.promoted_at = float(meta.get("promoted_at", 0.0))

    def _apply_overrides(self) -> None:
        paused = self._overrides.get("paused_coins", {})
        for coin, entry in self._coins.items():
            if coin in paused:
                entry.enabled = False
                info = paused[coin]
                entry.pause_reason = info.get("reason", "manual") if isinstance(info, dict) else str(info)
            elif entry.pause_reason == "manual":
                entry.enabled = True
                entry.pause_reason = None

    def _persist_battle_pool(self) -> None:
        doc = {
            "_meta": {
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "updated_ts": round(time.time(), 3),
                "battle_count": self._count_basket("battle"),
                "incubator_count": self._count_basket("incubator"),
            },
            "coins": {
                coin: entry.to_dict()
                for coin, entry in self._coins.items()
                if entry.basket == "battle"
            },
        }
        _write_json_atomic(self._battle_pool_file, doc)

    def _count_basket(self, basket: str) -> int:
        return sum(1 for e in self._coins.values() if e.basket == basket)
