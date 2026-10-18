"""
models.py - Data models for Korean Sunrise lead-lag arbitrage and momentum engine.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple


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
        return sum(p * q for p, q in self.asks[:n])

    def top_n_bid_depth_usd(self, n: int = 3) -> float:
        return sum(p * q for p, q in self.bids[:n])

    @property
    def imbalance_top3(self) -> float:
        b = self.top_n_bid_depth_usd(3)
        a = self.top_n_ask_depth_usd(3)
        tot = b + a
        return (b - a) / tot if tot > 0 else 0.0

    @property
    def imbalance_top5(self) -> float:
        b = self.depth_bid_usd
        a = self.depth_ask_usd
        tot = b + a
        return (b - a) / tot if tot > 0 else 0.0

    @property
    def network_latency_ms(self) -> float:
        if self.exchange_ts > 0:
            return round(self.ts * 1000.0 - float(self.exchange_ts), 1)
        return 0.0


@dataclass
class SunriseSignal:
    """Lead-lag signal triggered by Korean spot buying pressure with full telemetry."""
    id: str
    coin: str
    leader_exchange: str
    follower_exchange: str
    follower_contract: str
    open_ts: float
    trigger_type: str

    # Upbit (Leader) metrics at trigger
    leader_mid: float
    leader_best_ask: float
    leader_spread_bps: float
    leader_norm_spread_bps: float
    leader_top5_ask_usd: float
    leader_norm_depth_usd: float
    leader_swept_usd: float
    leader_imbalance_top5: float
    drain_ratio: float
    spread_blowout_ratio: float

    # Follower (Perp) metrics at trigger
    follower_mid: float
    follower_best_bid: float
    follower_best_ask: float
    follower_spread_bps: float
    follower_top3_ask_usd: float
    follower_top3_bid_usd: float
    follower_imbalance_top3: float

    # Basis, Sigma & Dislocation metrics
    kimchi_baseline_bps: float
    pair_sigma_bps: float
    raw_dislocation_bps: float
    dev_z: float
    gross_exec_edge_bps: float        # Executable edge before entry fee
    net_exec_edge_bps: float          # Executable edge after entry taker fee

    # Latency & Freshness telemetry
    leader_latency_ms: float
    follower_latency_ms: float
    follower_book_age_ms: float

    # Sizing
    suggested_size_usd: float

    # Pool routing
    basket: str = "incubator"
    is_actionable: bool = False

    # Outcome & Excursion tracking
    closed: bool = False
    close_ts: Optional[float] = None
    duration_sec: Optional[float] = None
    close_reason: Optional[str] = None
    captured_mid_move_bps: Optional[float] = None
    captured_exec_move_bps: Optional[float] = None
    net_pnl_bps: Optional[float] = None

    mfe_exec_pnl_bps: float = -999.0
    mae_exec_pnl_bps: float = 999.0
    mfe_mid_move_bps: float = 0.0
    time_to_peak_sec: float = 0.0

    path: List[List[float]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Compatibility aliases required by validate_data.py & analytical tools
        d["dislocation_bps"] = self.raw_dislocation_bps
        d["entry_ts"] = self.open_ts
        return d