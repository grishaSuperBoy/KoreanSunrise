"""
config.py - Configuration & Live Universe Discovery for Korean Sunrise.
Automatically fetches all KRW coins from Upbit and matches them against
active USDT Linear Perpetuals on Binance and Bybit (including 1000x/1M prefixes).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import os
import re
import urllib.request
from typing import Dict, List, Optional, Set, Tuple

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
STEALTH_DEPTH_FRACTION = 0.15   # Max 15% of top 3 ask levels on follower order book

# Execution Fees & Warmup Safety
TAKER_FEE_BPS = 5.0             # Single-leg VIP0 taker fee (bps) on Binance/Bybit
ROUNDTRIP_FEE_BPS = 10.0        # Entry + Exit taker fee (bps) for net realized PnL
MIN_WARMUP_SAMPLES = 30         # Minimum 1s MAD samples before firing signals
MAX_SIGNAL_HOLD_SEC = 30.0      # Maximum holding horizon before timeout closure (adaptive ceiling)
MAX_FOLLOWER_BOOK_AGE_SEC = 5.0 # Maximum staleness allowed for follower book snapshots (adaptive for alts)

# Adaptive Stop-Loss Parameters
STOP_INVALIDATION_BPS = 15.0    # Model A: Microstructure invalidation tolerance (leader premium breakdown)
STOP_ADVERSE_PNL_BPS = 35.0     # Model B: Hard adverse PnL drawdown stop (bps)
ALPHA_DECAY_SEC = 8.0           # Model C: Alpha decay timeout if PnL <= 0 (transmission failure)

# Regex to detect multiplier prefixes on Binance/Bybit perps: 1000PEPE, 1000000MOG, etc.
_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")

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


def _http_get_json(url: str, timeout: float = 12.0):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; KoreanSunrise/2.0)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _match_perp_base(perp_base: str, upbit_bases: Set[str]) -> Optional[Tuple[str, float]]:
    """
    Matches a Binance/Bybit perp base asset (e.g. 'SAND' or '1000PEPE')
    to an Upbit KRW base coin.
    Returns (upbit_coin, multiplier) or None.
    """
    # 1. Direct 1:1 match
    if perp_base in upbit_bases:
        return perp_base, 1.0

    # 2. Check reverse alias (if Upbit has old name and perp has new name)
    for up_coin, alias_target in TICKER_ALIASES.items():
        if perp_base == alias_target and up_coin in upbit_bases:
            return up_coin, 1.0

    # 3. Multiplier prefix match (e.g. 1000PEPE -> PEPE with mult=1000.0)
    m = _MULT_PREFIX_RE.match(perp_base)
    if m:
        mult_str, stripped_base = m.group(1), m.group(2)
        mult = float(mult_str)
        if stripped_base in upbit_bases:
            return stripped_base, mult
        for up_coin, alias_target in TICKER_ALIASES.items():
            if stripped_base == alias_target and up_coin in upbit_bases:
                return up_coin, mult

    return None


def discover_universe() -> MatchedUniverse:
    """
    Queries Upbit, Binance Futures, and Bybit Linear REST APIs to build
    an exact, real-time intersection of all Korean KRW altcoins/shitcoins
    and their corresponding USDT perpetual contracts.
    """
    # 1. Fetch all Upbit KRW spot markets
    upbit_data = _http_get_json("https://api.upbit.com/v1/market/all?isDetails=true")
    upbit_bases: Set[str] = set()
    for item in upbit_data:
        mkt = item.get("market", "")
        if mkt.startswith("KRW-"):
            coin = mkt.split("-", 1)[1].upper()
            if coin and coin not in EXCLUDED_COINS:
                upbit_bases.add(coin)

    if not upbit_bases:
        raise RuntimeError("Failed to discover any KRW markets from Upbit REST API")

    # 2. Fetch all active Binance USDT Linear Perpetuals
    binance_sym_map: Dict[str, Tuple[str, float]] = {}
    try:
        b_data = _http_get_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
        for s in b_data.get("symbols", []):
            if (
                s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"
                and s.get("quoteAsset") == "USDT"
            ):
                raw_sym = s["symbol"].upper()
                base_asset = s.get("baseAsset", "").upper()
                matched = _match_perp_base(base_asset, upbit_bases)
                if matched:
                    binance_sym_map[raw_sym] = matched
    except Exception as e:
        log.error(f"[discovery] Binance Futures exchangeInfo failed: {e}")

    # 3. Fetch all active Bybit USDT Linear Perpetuals
    bybit_sym_map: Dict[str, Tuple[str, float]] = {}
    try:
        cursor = ""
        while True:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
            if cursor:
                url += f"&cursor={cursor}"
            by_data = _http_get_json(url)
            res = by_data.get("result", {})
            for item in res.get("list", []):
                if (
                    item.get("status") == "Trading"
                    and item.get("quoteCoin") == "USDT"
                    and item.get("contractType") == "LinearPerpetual"
                ):
                    raw_sym = item["symbol"].upper()
                    base_coin = item.get("baseCoin", "").upper()
                    matched = _match_perp_base(base_coin, upbit_bases)
                    if matched:
                        bybit_sym_map[raw_sym] = matched
            cursor = res.get("nextPageCursor", "")
            if not cursor:
                break
    except Exception as e:
        log.error(f"[discovery] Bybit instruments-info failed: {e}")

    # 4. Keep only Upbit coins that have at least one active perp on Binance or Bybit
    matched_coins: Set[str] = set()
    for coin, _ in binance_sym_map.values():
        matched_coins.add(coin)
    for coin, _ in bybit_sym_map.values():
        matched_coins.add(coin)

    sorted_coins = sorted(matched_coins)
    upbit_krw_to_coin = {f"KRW-{c}": c for c in sorted_coins}

    log.info(
        f"[discovery] Upbit KRW alts: {len(upbit_bases)} | "
        f"Matched active alts: {len(sorted_coins)} "
        f"(Binance perps: {len(binance_sym_map)}, Bybit perps: {len(bybit_sym_map)})"
    )

    return MatchedUniverse(
        coins=sorted_coins,
        upbit_krw_to_coin=upbit_krw_to_coin,
        binance_sym_map=binance_sym_map,
        bybit_sym_map=bybit_sym_map,
    )