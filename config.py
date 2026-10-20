"""
config.py - Configuration & Live Universe Discovery for Korean Sunrise.
Automatically fetches all KRW coins from Upbit and matches them against
active USDT Linear Perpetuals on Binance and Bybit (including 1000x/1M prefixes).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
import os
from typing import Dict, List, Set, Tuple

log = logging.getLogger("config")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DATA_ROOT = os.getenv("DATA_ROOT", "data")
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# API Web Server & Dashboard
API_SERVER_ENABLED = os.getenv("API_SERVER_ENABLED", "true").lower() in ("true", "1", "yes")
API_SERVER_HOST = os.getenv("API_SERVER_HOST", "0.0.0.0")
API_SERVER_PORT = int(os.getenv("API_SERVER_PORT", "8089"))
API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN", "vitali_secret_key_2026")

# Exclude heavy majors & stablecoins so we focus purely on alts / shitcoins.
# (Can be overridden via env EXCLUDE_COINS="BTC,ETH,USDT,USDC")
_default_exclude = "BTC,ETH,USDT,USDC,BUSD,FDUSD,DAI,TUSD,RLUSD,PYUSD,USDE,USDD,EUR,GUSD"
EXCLUDED_COINS: Set[str] = {
    x.strip().upper()
    for x in os.getenv("EXCLUDE_COINS", _default_exclude).split(",")
    if x.strip()
}

# Known rename/alias mapping between Upbit KRW ticker and Global Perp base ticker
# Format: Upbit_Ticker -> Global_Base_Ticker
TICKER_ALIASES: Dict[str, str] = {
    # Add any ticker discrepancies if Upbit uses an old/local symbol name
    "MATIC": "POL",
}

# Core Engine Parameters
DRAIN_THRESHOLD = 0.50          # 50% of normal top-5 ask depth swept on Upbit
BLOWOUT_THRESHOLD = 2.5         # Instant spread >= 2.5x normal EWMA spread AND >= 2 ticks
MIN_EXEC_EDGE_BPS = 12.0        # Minimum net executable edge after entry taker fee
MAX_POSITION_BUDGET_USD = 10000.0
MIN_ORDER_USD = 10.0            # Unified minimum exchange order notional in USD ($10.0 universally accepted across all venues)
STEALTH_DEPTH_FRACTION = 0.15   # Max 15% of top 3 ask levels on follower order book
TRADE_CONFIRM_LOOKBACK_SEC = 2.0 # Maximum 2.0s lookback for taker trade confirmation against MM quote cancellations

# Execution Fees & Warmup Safety
TAKER_FEE_BPS = 5.0             # Single-leg VIP0 taker fee (bps) on Binance/Bybit
ROUNDTRIP_FEE_BPS = 10.0        # Entry + Exit taker fee (bps) for net realized PnL
MIN_WARMUP_SAMPLES = 30         # Minimum 1s MAD samples before firing signals
MAX_SIGNAL_HOLD_SEC = 30.0      # Maximum holding horizon before timeout closure (adaptive ceiling)
MAX_FOLLOWER_BOOK_AGE_SEC = 1.50 # Maximum 1.5s staleness for follower book snapshots — generous enough for slow alts, still blocks stale 15s+ feeds

# Adaptive Stop-Loss Parameters
STOP_INVALIDATION_BPS = 15.0    # Model A: Microstructure invalidation tolerance (leader premium breakdown)
STOP_ADVERSE_PNL_BPS = 35.0     # Model B: Hard adverse PnL drawdown stop (bps)
ALPHA_DECAY_SEC = 15.0          # Model C: Alpha decay timeout if PnL <= 0 (fallback if not in matrix)

ALL_SUPPORTED_VENUES = [
    "binance",
    "bybit",
    "okx",
    "gateio",
    "mexc",
    "kucoin",
    "bingx",
    "htx",
    "pionex",
    "asterdex",
    "hyperliquid",
]


@dataclass
class MatchedUniverse:
    """Holds the dynamically matched universe of coins across Upbit and all 11 follower perp venues."""
    # Canonical base coin list (Upbit base names, e.g. ["PEPE", "DOGE", "SAND", ...])
    coins: List[str] = field(default_factory=list)

    # Upbit mapping: KRW market code -> canonical coin (e.g. "KRW-PEPE" -> "PEPE")
    upbit_krw_to_coin: Dict[str, str] = field(default_factory=dict)

    # First-class typed dictionary for all follower venue symbol maps:
    # venue -> {raw_symbol: (canonical_coin, mult, contract_size)}
    venue_sym_maps: Dict[str, Dict[str, Tuple[str, float, float]]] = field(default_factory=dict)

    # Legacy convenience attributes for Binance & Bybit backward-compatibility
    binance_sym_map: Dict[str, Tuple[str, float]] = field(default_factory=dict)
    bybit_sym_map: Dict[str, Tuple[str, float]] = field(default_factory=dict)

    def get_sym_map(self, venue: str) -> Dict[str, Tuple[str, float, float]]:
        """Returns the symbol mapping for any supported venue with (coin, mult, c_size)."""
        v = venue.lower()
        if v in self.venue_sym_maps:
            return self.venue_sym_maps[v]
        if v == "binance":
            return {
                k: (val[0], float(val[1]), float(val[2]) if len(val) >= 3 else 1.0)
                for k, val in self.binance_sym_map.items()
            }
        if v == "bybit":
            return {
                k: (val[0], float(val[1]), float(val[2]) if len(val) >= 3 else 1.0)
                for k, val in self.bybit_sym_map.items()
            }
        return {}