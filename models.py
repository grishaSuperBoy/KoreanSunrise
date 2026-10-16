"""
models.py - Data models for Korean Sunrise lead-lag arbitrage and momentum engine.
Includes full microstructure telemetry for post-trade coin filtering and alpha analysis.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


@dataclass(slots=True)
class OrderBookDepth5:
    """
    Top-5 order book snapshot normalized to 1 single base coin in USD/USDT.
    For 1000x perpetuals, prices are already divided by 1000 and sizes multiplied by 1000.
    """
    exchange: str
    symbol: str                      # Canonical base coin (e.g. 'PEPE', 'SAND')
    raw_symbol: str                  # Venue-specific symbol (e.g. 'KRW-PEPE', '1000PEPEUSDT')
    ts: float                        # Local receipt timestamp (sec)
    exchange_ts: int                 # Exchange event timestamp (ms)
    bids: List[Tuple[float, float]]  # [(price_usd, size_coins), ...]
    asks: List[Tuple[float, float]]  # [(price_usd, size_coins), ...]

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def best_bid_usd(self) -> float:
        return self.bids[0][0] * self.bids[0][1] if self.bids else 0.0

    @property
    def best_ask_usd(self) -> float:
        return self.asks[0][0] * self.asks[0][1] if self.asks else 0.0

    @property
    def mid_price(self) -> float:
        if self.bids and self.asks:
            return (self.bids[0][0] + self.asks[0][0]) * 0.5
        return self.best_bid or self.best_ask

    @property
    def spread_bps(self) -> float:
        if self.bids and self.asks:
            mid = self.mid_price
            return (self.asks[0][0] - self.bids[0][0]) / mid * 1e4 if mid > 0 else 0.0
        return 0.0

    @property
    def depth_bid_usd(self) -> float:
        return sum(p * q for p, q in self.bids)

    @property
    def depth_ask_usd(self) -> float:
        return sum(p * q for p, q in self.asks)

    def top_n_ask_depth_usd(self, n: int = 3) -> float:
        """Sum of notional USD depth across top N ask levels."""
        return sum(p * q for p, q in self.asks[:n])

    def top_n_bid_depth_usd(self, n: int = 3) -> float:
        """Sum of notional USD depth across top N bid levels."""
        return sum(p * q for p, q in self.bids[:n])

    @property
    def imbalance_top3(self) -> float:
        """Order book imbalance across top 3 levels in [-1.0, +1.0]."""
        b = self.top_n_bid_depth_usd(3)
        a = self.top_n_ask_depth_usd(3)
        tot = b + a
        return (b - a) / tot if tot > 0 else 0.0

    @property
    def imbalance_top5(self) -> float:
        """Order book imbalance across all 5 levels in [-1.0, +1.0]."""
        b = self.depth_bid_usd
        a = self.depth_ask_usd
        tot = b + a
        return (b - a) / tot if tot > 0 else 0.0

    @property
    def network_latency_ms(self) -> float:
        """Estimated one-way transit latency from exchange engine to local host (ms)."""
        if self.exchange_ts > 0:
            return round(self.ts * 1000.0 - float(self.exchange_ts), 1)
        return 0.0


@dataclass
class SunriseSignal:
    """Lead-lag signal triggered by Korean spot buying pressure with full telemetry."""
    id: str
    coin: str                         # Canonical base coin (e.g. 'PEPE')
    leader_exchange: str              # 'upbit'
    follower_exchange: str            # 'binance' or 'bybit'
    follower_contract: str            # Exact perp contract (e.g. '1000PEPEUSDT')
    open_ts: float
    trigger_type: str                 # 'depth_drain' or 'spread_blowout'

    # Upbit (Leader) metrics at trigger
    leader_mid: float
    leader_best_ask: float
    leader_spread_bps: float
    leader_norm_spread_bps: float     # EWMA normal spread on Upbit (bps)
    leader_top5_ask_usd: float        # Current top-5 ask liquidity on Upbit ($)
    leader_norm_depth_usd: float      # EWMA normal top-5 ask liquidity on Upbit ($)
    leader_swept_usd: float           # Estimated USD volume eaten in the trigger tick ($)
    leader_imbalance_top5: float      # Upbit top-5 book imbalance [-1, +1]
    drain_ratio: float                # Swept Ask Depth / Norm Depth
    spread_blowout_ratio: float       # Instant Spread / Norm Spread

    # Follower (Perp) metrics at trigger
    follower_mid: float
    follower_best_bid: float
    follower_best_ask: float
    follower_spread_bps: float
    follower_top3_ask_usd: float
    follower_top3_bid_usd: float
    follower_imbalance_top3: float    # Follower top-3 book imbalance [-1, +1]

    # Basis, Sigma & Dislocation metrics
    kimchi_baseline_bps: float
    pair_sigma_bps: float             # Robust MAD sigma of the pair basis (bps)
    raw_dislocation_bps: float
    dev_z: float                      # Dislocation jump in robust MAD sigmas
    net_exec_edge_bps: float          # Executable edge vs follower_best_ask after entry taker fee

    # Latency & Freshness telemetry
    leader_latency_ms: float          # Upbit WS transit delay (ms)
    follower_latency_ms: float        # Follower WS transit delay (ms)
    follower_book_age_ms: float       # How old the follower snapshot was when Upbit triggered (ms)

    # Sizing (Universal stealth rule: <= 15% top-3 ask depth)
    suggested_size_usd: float

    # Outcome & Excursion tracking (filled as signal matures)
    closed: bool = False
    close_ts: Optional[float] = None
    duration_sec: Optional[float] = None
    close_reason: Optional[str] = None              # 'converged', 'reversed', 'timeout'
    captured_mid_move_bps: Optional[float] = None   # Mid-to-Mid move at close (bps)
    captured_exec_move_bps: Optional[float] = None  # Entry Ask to Exit Bid move at close (bps)
    net_pnl_bps: Optional[float] = None             # Realized PnL after spread + roundtrip fees (bps)

    # Peak excursion metrics during signal lifetime (after roundtrip fees)
    mfe_exec_pnl_bps: float = -999.0                # Max Favorable Excursion (best net exit PnL achieved)
    mae_exec_pnl_bps: float = 999.0                 # Max Adverse Excursion (worst net exit PnL seen)
    mfe_mid_move_bps: float = 0.0                   # Peak mid-price move achieved (bps)
    time_to_peak_sec: float = 0.0                   # Time (sec) from open to peak executable PnL

    # Trajectory: [[dt_sec, upbit_mid_move_bps, follower_mid_move_bps, net_exec_pnl_bps, rem_edge_bps], ...]
    path: List[List[float]] = field(default_factory=list)