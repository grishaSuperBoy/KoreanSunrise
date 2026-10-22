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
SIGNAL_FLUSH_INTERVAL_SEC = float(os.getenv("SIGNAL_FLUSH_INTERVAL_SEC", "1800.0"))  # 30-min in-memory buffer flush
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# API Web Server & Dashboard
API_SERVER_ENABLED = os.getenv("API_SERVER_ENABLED", "true").lower() in ("true", "1", "yes")
API_SERVER_HOST = os.getenv("API_SERVER_HOST", "0.0.0.0")
API_SERVER_PORT = int(os.getenv("API_SERVER_PORT", "8089"))
API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN")

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
MIN_ORDER_USD = 10.0            # Default minimum order notional for alts ($10.0; major contracts BTC/SOL have higher exchange contract lot floors $150-$700)
STEALTH_DEPTH_FRACTION = 0.15   # Max 15% of top 3 ask levels on follower order book
TRADE_CONFIRM_LOOKBACK_SEC = 2.0 # Maximum 2.0s lookback for taker trade confirmation against MM quote cancellations

# Execution Fees & Warmup Safety
# NOTE: 5.0 bps single-leg / 10.0 bps roundtrip represents an idealized VIP0 taker tier.
# On illiquid altcoins, effective taker fees and adverse crossing spreads may be wider.
TAKER_FEE_BPS = 5.0             # Single-leg VIP0 taker fee (bps) on Binance/Bybit
ROUNDTRIP_FEE_BPS = 10.0        # Theoretical entry + exit taker fee (bps) for net simulated PnL
MIN_WARMUP_SAMPLES = 30         # Minimum 1s MAD samples before firing signals
MAX_SIGNAL_HOLD_SEC = 30.0      # Maximum holding horizon before timeout closure (adaptive ceiling)
MAX_FOLLOWER_BOOK_AGE_SEC = 1.50 # Maximum 1.5s staleness for follower book snapshots — generous enough for slow alts, still blocks stale 15s+ feeds

# Adaptive Stop-Loss Parameters
STOP_INVALIDATION_BPS = 15.0    # Model A: Microstructure invalidation tolerance (leader premium breakdown)
STOP_ADVERSE_PNL_BPS = 35.0     # Model B: Hard adverse PnL drawdown stop (bps)

# Bithumb Co-Leader Configuration
BITHUMB_ENABLED = os.getenv("BITHUMB_ENABLED", "true").lower() in ("true", "1", "yes")
BITHUMB_MIN_SWEPT_USD = float(os.getenv("BITHUMB_MIN_SWEPT_USD", "3000.0"))
REQUIRE_KOREAN_CONFIRMATION = os.getenv("REQUIRE_KOREAN_CONFIRMATION", "false").lower() in ("true", "1", "yes")
STRICT_KOREAN_GATE = os.getenv("STRICT_KOREAN_GATE", "false").lower() in ("true", "1", "yes")
MIN_KOREAN_CONFIRM_USD = float(os.getenv("MIN_KOREAN_CONFIRM_USD", "500.0")) # Min taker volume ($) on peer Korean exchange for volume confirmation ($500+ in production; set 0.0 for open telemetry-only research)

# Per-Strategy Risk & Exposure Limits:
# Applied to each sub-strategy (S2 Fade/Sniper and S3 Maker/Taker Voids) individually
# to prevent cross-strategy execution starvation. (Total portfolio max = 2x these limits).
MAX_STRATEGY_CONCURRENT_POSITIONS = int(os.getenv("MAX_STRATEGY_CONCURRENT_POSITIONS", os.getenv("MAX_PORTFOLIO_CONCURRENT_POSITIONS", "10")))
MAX_STRATEGY_EXPOSURE_USD = float(os.getenv("MAX_STRATEGY_EXPOSURE_USD", os.getenv("MAX_PORTFOLIO_EXPOSURE_USD", "5000.0")))
# Backward-compatibility aliases
MAX_PORTFOLIO_CONCURRENT_POSITIONS = MAX_STRATEGY_CONCURRENT_POSITIONS
MAX_PORTFOLIO_EXPOSURE_USD = MAX_STRATEGY_EXPOSURE_USD
MAKER_REBATE_BPS = float(os.getenv("MAKER_REBATE_BPS", "0.0")) # Realistic maker rebate on alts (0.0 bps default; 0% rebate on non-VIP alts)
FOLLOWER_SPREAD_WARMUP_SAMPLES = int(os.getenv("FOLLOWER_SPREAD_WARMUP_SAMPLES", "5")) # Min order book ticks required before follower spread baseline is active

# Macro Market Regime Benchmark Configuration
REGIME_BENCHMARK_EXCHANGE = os.getenv("REGIME_BENCHMARK_EXCHANGE", "binance").lower() # Benchmark venue for BTC momentum and alt breadth (avoids Kimchi contamination)

ALPHA_DECAY_SEC = 15.0          # Model C: Alpha decay timeout if PnL <= 0 (fallback if not in matrix)

# Strategy 1: Korean Momentum (Upbit Sweep Lead-Lag) + Moonbag / Runner Dual-Phase Mode
S1_RUNNER_ENABLED = os.getenv("S1_RUNNER_ENABLED", "false").lower() in ("true", "1", "yes") # OFF by default
S1_RUNNER_PORTION = float(os.getenv("S1_RUNNER_PORTION", "0.30"))                           # 30% held as trailing runner
S1_RUNNER_TRAILING_DIST_BPS = float(os.getenv("S1_RUNNER_TRAILING_DIST_BPS", "20.0"))       # 20 bps pullback from peak
S1_RUNNER_MAX_HOLD_SEC = float(os.getenv("S1_RUNNER_MAX_HOLD_SEC", "120.0"))               # 120s max runner hold

# Strategy 2: Anti-Sweep Reversal (Fade the Fakeout) / Pre-Sweep Early Long
MAX_FADE_SWEPT_USD = float(os.getenv("MAX_FADE_SWEPT_USD", "2000.0"))        # Max Upbit sweep volume ($2,000) to consider a micro-sweep
MIN_FADE_SPREAD_BPS = float(os.getenv("MIN_FADE_SPREAD_BPS", "35.0"))        # Min Upbit spread (35 bps) to confirm an illiquid gap
MAX_FADE_FOLLOWER_DEPTH_USD = float(os.getenv("MAX_FADE_FOLLOWER_DEPTH_USD", "3500.0")) # Max follower depth ($3,500) to ensure thin book
FADE_COOLDOWN_SEC = float(os.getenv("FADE_COOLDOWN_SEC", "30.0"))            # Anti-machinegun cooldown per pair (sec)
S2_INVERT_TO_LONG = os.getenv("S2_INVERT_TO_LONG", "true").lower() in ("true", "1", "yes") # True: Invert S2 short to early long sniper on MM quote cancellations

# Strategy 3: Maker Spread Harvesting (Void Hunter)
MAX_MAKER_SPREAD_BPS = float(os.getenv("MAX_MAKER_SPREAD_BPS", "120.0"))     # Max spread ceiling to reject toxic/dead DEX pools
S3_HUNTER_MODE = os.getenv("S3_HUNTER_MODE", "false").lower() in ("true", "1", "yes") # False (Default): Passive Maker Void. True: Taker mode (must use tight spread + large lag)
S3_ALIGN_TREND = os.getenv("S3_ALIGN_TREND", "true").lower() in ("true", "1", "yes") # True: align maker limit orders with Upbit directional momentum
S3_REQUIRE_UPBIT = os.getenv("S3_REQUIRE_UPBIT", "true").lower() in ("true", "1", "yes") # True: reject unanchored tokens without active Upbit/Bithumb feed

# Strategy 4: Predatory Rebound Maker (Rubber Band)
S4_RUBBER_BAND_ENABLED = os.getenv("S4_RUBBER_BAND_ENABLED", "false").lower() in ("true", "1", "yes") # OFF by default until user verification
MIN_RUBBER_BAND_DISLOCATION_BPS = float(os.getenv("MIN_RUBBER_BAND_DISLOCATION_BPS", "100.0")) # Min Upbit-Follower dislocation (+100 bps)
RUBBER_BAND_MIN_Z_SCORE = float(os.getenv("RUBBER_BAND_MIN_Z_SCORE", "3.0"))                   # Min statistical dislocation Z-score (3.0σ)
RUBBER_BAND_MAKER_DISCOUNT_BPS = float(os.getenv("RUBBER_BAND_MAKER_DISCOUNT_BPS", "25.0"))   # Post-Only Limit Bid discount below best_bid
RUBBER_BAND_MAKER_TTL_SEC = float(os.getenv("RUBBER_BAND_MAKER_TTL_SEC", "2.5"))              # Short resting order TTL (sec)
TARGET_RUBBER_BAND_BPS = float(os.getenv("TARGET_RUBBER_BAND_BPS", "35.0"))                     # Target snap-back take profit (+35 bps)
STOP_RUBBER_BAND_BPS = float(os.getenv("STOP_RUBBER_BAND_BPS", "25.0"))                         # Stop loss threshold (-25 bps)
RUBBER_BAND_MAX_HOLD_SEC = float(os.getenv("RUBBER_BAND_MAX_HOLD_SEC", "30.0"))                # Max holding time (30s)
RUBBER_BAND_ALPHA_DECAY_SEC = float(os.getenv("RUBBER_BAND_ALPHA_DECAY_SEC", "15.0"))          # Alpha decay timeout (15s)
RUBBER_BAND_COOLDOWN_SEC = float(os.getenv("RUBBER_BAND_COOLDOWN_SEC", "20.0"))                # Cooldown per pair (20s)

# Strategy 5: Cross-Korean Triangle (Bithumb Radar) [Renumbered from S7]
S5_CROSS_KOREAN_ENABLED = os.getenv("S5_CROSS_KOREAN_ENABLED", os.getenv("S7_CROSS_KOREAN_ENABLED", "false")).lower() in ("true", "1", "yes") # OFF by default
S7_CROSS_KOREAN_ENABLED = S5_CROSS_KOREAN_ENABLED                                                # Backward-compatibility alias
MIN_BITHUMB_LEAD_BPS = float(os.getenv("MIN_BITHUMB_LEAD_BPS", "25.0"))
MIN_BITHUMB_BUYS_USD = float(os.getenv("MIN_BITHUMB_BUYS_USD", "2000.0"))                       # Real trade tape confirmation (>= $2,000 in 2s)
MIN_UPBIT_CONFIRM_BPS = float(os.getenv("MIN_UPBIT_CONFIRM_BPS", "3.0"))                        # Upbit momentum confirmation (+3 bps or tape)
BITHUMB_MIN_VOLUME_SHARE = float(os.getenv("BITHUMB_MIN_VOLUME_SHARE", "0.30"))                 # Minimum Bithumb volume / Upbit volume share
EXCLUDED_TRIANGLE_COINS = {"BTC", "ETH", "SOL", "XRP", "DOGE"}                                  # Reject major coins where Bithumb cannot lead Upbit

# Archived / Deprecated Strategies (Kept for telemetry and historical replay)
S5_IMBALANCE_ENABLED = os.getenv("S5_IMBALANCE_ENABLED", "false").lower() in ("true", "1", "yes") # Retired (OBIV)
MIN_UPBIT_IMBALANCE = float(os.getenv("MIN_UPBIT_IMBALANCE", "0.60"))
MIN_DELTA_IMBALANCE_5S = float(os.getenv("MIN_DELTA_IMBALANCE_5S", "0.35"))
MAX_FOLLOWER_IMBALANCE = float(os.getenv("MAX_FOLLOWER_IMBALANCE", "0.25"))
S6_BETA_NEUTRAL_ENABLED = os.getenv("S6_BETA_NEUTRAL_ENABLED", "false").lower() in ("true", "1", "yes") # Retired (Alt+BTC Pairs)
DEFAULT_BETA_HEDGE = float(os.getenv("DEFAULT_BETA_HEDGE", "1.3"))

# AsterDex DEX Adaptation & Safety
MAX_ASTERDEX_SPREAD_BPS = float(os.getenv("MAX_ASTERDEX_SPREAD_BPS", "45.0")) # Hard spread ceiling on AsterDex (blocks 250-350 bps defensive walls)
ASTERDEX_MAKER_ENABLED = os.getenv("ASTERDEX_MAKER_ENABLED", "false").lower() in ("true", "1") # Block passive maker on DEX AMMs by default

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