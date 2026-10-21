"""
pipeline/market_regime.py — Real-Time Macro Market Regime & Altcoin Breadth Engine.

Provides quantitative macro market regime detection for Korean Sunrise:
1. BTC Macro Velocity & Flash Dump Circuit Breaker:
   - Tracks 30s and 300s BTC price momentum.
   - Detects acute flash crashes and enforces a cooldown lockout to protect long positions.
2. Cross-Universe Altcoin Market Breadth Index (MBI):
   - Computes rolling 60s and 300s advance/decline ratios across all active alts (300+ coins).
   - Identifies systemic market pumps (broad rally where shorts get destroyed) vs market selloffs.
3. Korean Aggregate Flow Imbalance:
   - Tracks net taker buy volume across all Upbit pairs.
4. Strategy Routing:
   - S1 (Lead-Lag Long): HARD VETO during FLASH_DUMP.
   - S2 (Fade Fakeout Short): HARD SHIELD (veto) during STRONG_PUMP.
   - Dynamic Tsunami Ratio multiplier adjustment based on regime tailwinds.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
import logging
import math
import threading
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

log = logging.getLogger("pipeline.regime")


class MarketRegime(str, Enum):
    STRONG_PUMP = "STRONG_PUMP"       # Broad market rally / pump wave
    BULL_DRIFT  = "BULL_DRIFT"        # Mild upward drift
    NEUTRAL     = "NEUTRAL"           # Idiosyncratic / choppy market
    BEAR_DRIFT  = "BEAR_DRIFT"        # Mild downward drift
    FLASH_DUMP  = "FLASH_DUMP"        # Panic selloff / liquidation cascade


@dataclass(slots=True)
class MarketRegimeSnapshot:
    regime: MarketRegime
    breadth_pct_60s: float           # % of alts with positive return over last 60s
    breadth_pct_300s: float          # % of alts with positive return over last 300s
    btc_ret_30s_bps: float           # BTC return in bps over last 30s
    btc_ret_300s_bps: float          # BTC return in bps over last 300s
    btc_dump_active: bool            # True if BTC is in active flash dump or cooldown
    korean_buy_ratio_60s: float      # Upbit taker buy / total volume over last 60s (0.0 to 1.0)
    coins_tracked: int               # Number of active coins with fresh price history
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "regime": self.regime.value,
            "breadth_pct_60s": round(self.breadth_pct_60s, 1),
            "breadth_pct_300s": round(self.breadth_pct_300s, 1),
            "btc_ret_30s_bps": round(self.btc_ret_30s_bps, 1),
            "btc_ret_300s_bps": round(self.btc_ret_300s_bps, 1),
            "btc_dump_active": self.btc_dump_active,
            "korean_buy_ratio_60s": round(self.korean_buy_ratio_60s, 2),
            "coins_tracked": self.coins_tracked,
            "timestamp": round(self.timestamp, 3),
        }


class MarketRegimeDetector:
    """
    High-frequency, thread-safe macro regime and market breadth evaluator.
    Designed for low CPU footprint and sub-millisecond query latency.
    """

    def __init__(
        self,
        btc_dump_30s_bps: float = -35.0,
        btc_dump_300s_bps: float = -80.0,
        btc_pump_300s_bps: float = 30.0,
        dump_cooldown_sec: float = 120.0,
        breadth_dump_threshold: float = 25.0,
        breadth_pump_threshold: float = 70.0,
        min_coins_for_breadth: int = 15,
        cache_ttl_sec: float = 1.0,
    ):
        self.btc_dump_30s_bps = btc_dump_30s_bps
        self.btc_dump_300s_bps = btc_dump_300s_bps
        self.btc_pump_300s_bps = btc_pump_300s_bps
        self.dump_cooldown_sec = dump_cooldown_sec
        self.breadth_dump_threshold = breadth_dump_threshold
        self.breadth_pump_threshold = breadth_pump_threshold
        self.min_coins_for_breadth = min_coins_for_breadth
        self.cache_ttl_sec = cache_ttl_sec

        self._lock = threading.Lock()

        # BTC time series buffer: deque[(timestamp, price)]
        self._btc_history: Deque[Tuple[float, float]] = deque(maxlen=1200)

        # Altcoin price history: coin -> deque[(timestamp, mid_price)]
        self._alt_history: Dict[str, Deque[Tuple[float, float]]] = {}
        self._last_alt_sample_ts: Dict[str, float] = {}

        # Korean trade flow buffer: deque[(timestamp, usd, is_buy)]
        self._korean_trades: Deque[Tuple[float, float, bool]] = deque(maxlen=5000)

        # Cooldown state
        self._dump_cooldown_until: float = 0.0

        # Snapshot cache
        self._cached_snapshot: Optional[MarketRegimeSnapshot] = None
        self._cache_ts: float = 0.0

    def on_btc_tick(self, price: float, ts: float) -> None:
        """Records a new BTC benchmark tick and evaluates instant crash triggers."""
        if price <= 0:
            return
        with self._lock:
            self._btc_history.append((ts, price))
            # Clean old BTC history > 650s
            cutoff = ts - 650.0
            while self._btc_history and self._btc_history[0][0] < cutoff:
                self._btc_history.popleft()

            # Immediate flash dump check:
            # 1. 300s cumulative breakdown: BTC dropped >= btc_dump_300s_bps (default -80.0 bps / -0.8%) over 5 minutes.
            # 2. 30s acute flash crash:
            #    - Severe shock (<= -50.0 bps): acute cascade wick, triggers unconditionally.
            #    - Moderate shock (<= btc_dump_30s_bps, e.g. -35.0 bps):
            #      If breadth is available, confirms with market breadth (breadth_300s <= 45.0% or breadth_60s <= 40.0%).
            #      If breadth has not warmed up yet (< min_coins_for_breadth), triggers safely.
            ret_30s = self._calc_btc_ret(ts, 30.0)
            ret_300s = self._calc_btc_ret(ts, 300.0)

            is_300s_dump = ret_300s <= self.btc_dump_300s_bps
            is_30s_dump = False
            if ret_30s <= -50.0:
                is_30s_dump = True
            elif ret_30s <= self.btc_dump_30s_bps:
                b60, b300, active_c = self._calc_breadth(ts)
                if active_c < self.min_coins_for_breadth or b300 <= 45.0 or b60 <= 40.0:
                    is_30s_dump = True

            if is_30s_dump or is_300s_dump:
                if ts >= self._dump_cooldown_until:
                    log.warning(
                        f"[REGIME-CIRCUIT-BREAKER] 🚨 BTC Flash Dump Triggered! "
                        f"Ret30s: {ret_30s:+.1f} bps | Ret300s: {ret_300s:+.1f} bps | "
                        f"Lockout: {self.dump_cooldown_sec:.0f}s"
                    )
                self._dump_cooldown_until = max(self._dump_cooldown_until, ts + self.dump_cooldown_sec)

    def on_alt_tick(self, coin: str, mid_price: float, ts: float) -> None:
        """
        Samples altcoin mid prices for cross-universe breadth calculation.
        Throttled to max 1 sample per second per coin to preserve memory and speed.
        """
        if mid_price <= 0 or coin.upper() == "BTC":
            return
        c = coin.upper()
        with self._lock:
            last_ts = self._last_alt_sample_ts.get(c, 0.0)
            if ts - last_ts < 1.0:
                return
            self._last_alt_sample_ts[c] = ts

            buf = self._alt_history.get(c)
            if buf is None:
                buf = self._alt_history[c] = deque(maxlen=360)
            buf.append((ts, mid_price))

            # Prune buffer entries older than 350s
            cutoff = ts - 350.0
            while buf and buf[0][0] < cutoff:
                buf.popleft()

    def on_korean_trade(self, trade_usd: float, is_taker_buy: bool, ts: float) -> None:
        """Records Korean trades to track aggregate taker buy/sell volume pressure."""
        if trade_usd <= 0:
            return
        with self._lock:
            self._korean_trades.append((ts, trade_usd, is_taker_buy))
            cutoff = ts - 120.0
            while self._korean_trades and self._korean_trades[0][0] < cutoff:
                self._korean_trades.popleft()

    def _calc_btc_ret(self, now: float, window_sec: float) -> float:
        """Helper to calculate BTC return in bps over a historical window."""
        if len(self._btc_history) < 2:
            return 0.0
        cur_px = self._btc_history[-1][1]
        target_ts = now - window_sec
        # Find closest price at or before target_ts, defaulting to oldest available tick in buffer
        past_px = self._btc_history[0][1]
        for t, p in self._btc_history:
            if t <= target_ts:
                past_px = p
            else:
                break
        if past_px <= 0:
            return 0.0
        return (cur_px - past_px) / past_px * 1e4

    def get_regime(self, now: float) -> MarketRegimeSnapshot:
        """Returns the current market regime snapshot, utilizing a 1.0s TTL cache."""
        with self._lock:
            if self._cached_snapshot is not None and (now - self._cache_ts) < self.cache_ttl_sec:
                return self._cached_snapshot

            snap = self._compute_regime_snapshot(now)
            self._cached_snapshot = snap
            self._cache_ts = now
            return snap

    def _compute_regime_snapshot(self, now: float) -> MarketRegimeSnapshot:
        """Internal uncached evaluator."""
        # 1. BTC Momentum
        btc_fresh = bool(self._btc_history and (now - self._btc_history[-1][0]) <= 20.0)
        ret_30s = self._calc_btc_ret(now, 30.0) if btc_fresh else 0.0
        ret_300s = self._calc_btc_ret(now, 300.0) if btc_fresh else 0.0
        is_dump_active = now < self._dump_cooldown_until

        # 2. Altcoin Market Breadth
        breadth_60s, breadth_300s, coins_active = self._calc_breadth(now)

        # 3. Korean Volume Flow
        buy_usd = 0.0
        total_usd = 0.0
        cutoff_60s = now - 60.0
        for t, usd, is_buy in self._korean_trades:
            if t >= cutoff_60s:
                total_usd += usd
                if is_buy:
                    buy_usd += usd
        kr_buy_ratio = (buy_usd / total_usd) if total_usd > 0 else 0.5

        # 4. Regime Classification Hierarchy:
        # A. FLASH DUMP: BTC crash in progress OR active circuit breaker cooldown OR severe alt collapse
        if is_dump_active or (coins_active >= self.min_coins_for_breadth and breadth_300s < self.breadth_dump_threshold):
            reg = MarketRegime.FLASH_DUMP

        # B. STRONG PUMP: High alt breadth (>70%) OR BTC bull impulse with majority alts green
        elif (coins_active >= self.min_coins_for_breadth and breadth_300s >= self.breadth_pump_threshold) or (
            ret_300s >= self.btc_pump_300s_bps and breadth_300s >= 55.0
        ):
            reg = MarketRegime.STRONG_PUMP

        # C. BULL DRIFT: Moderately positive market
        elif breadth_300s >= 55.0 or ret_300s >= 15.0:
            reg = MarketRegime.BULL_DRIFT

        # D. BEAR DRIFT: Moderately negative market
        elif (coins_active >= self.min_coins_for_breadth and breadth_300s <= 38.0) or ret_300s <= -20.0:
            reg = MarketRegime.BEAR_DRIFT

        # E. NEUTRAL: Choppy / idiosyncratic baseline
        else:
            reg = MarketRegime.NEUTRAL

        return MarketRegimeSnapshot(
            regime=reg,
            breadth_pct_60s=breadth_60s,
            breadth_pct_300s=breadth_300s,
            btc_ret_30s_bps=ret_30s,
            btc_ret_300s_bps=ret_300s,
            btc_dump_active=is_dump_active,
            korean_buy_ratio_60s=kr_buy_ratio,
            coins_tracked=coins_active,
            timestamp=now,
        )

    def _calc_breadth(self, now: float) -> Tuple[float, float, int]:
        """Calculates % of alts whose price is higher than 60s and 300s ago."""
        t60_target = now - 60.0
        t300_target = now - 300.0

        n_up_60s = 0
        n_up_300s = 0
        valid_coins = 0

        for coin, buf in self._alt_history.items():
            if not buf:
                continue
            cur_p = buf[-1][1]
            last_t = buf[-1][0]
            if (now - last_t) > 30.0:
                continue  # Stale feed, ignore

            # Find past price at 60s
            p60 = None
            p300 = None
            for t, p in buf:
                if t <= t60_target:
                    p60 = p
                if t <= t300_target:
                    p300 = p

            if p300 is not None:
                valid_coins += 1
                if cur_p > p300:
                    n_up_300s += 1
                if p60 is not None and cur_p > p60:
                    n_up_60s += 1

        if valid_coins < self.min_coins_for_breadth:
            # During startup / warmup, default to 50% neutral breadth
            return 50.0, 50.0, valid_coins

        b60 = (n_up_60s / valid_coins) * 100.0
        b300 = (n_up_300s / valid_coins) * 100.0
        return b60, b300, valid_coins

    def is_long_allowed(self, now: float) -> Tuple[bool, str]:
        """
        Safety gate for Strategy 1 (Lead-Lag Longs).
        Returns (False, reason) if market is in active flash dump or severe systemic collapse.
        """
        snap = self.get_regime(now)
        if snap.regime == MarketRegime.FLASH_DUMP:
            if snap.btc_dump_active:
                return False, f"BTC Flash Dump Active (30s: {snap.btc_ret_30s_bps:+.1f} bps, 300s: {snap.btc_ret_300s_bps:+.1f} bps)"
            return False, f"Severe Market Breadth Collapse ({snap.breadth_pct_300s:.1f}% positive alts)"
        return True, "OK"

    def is_short_allowed(self, now: float) -> Tuple[bool, str]:
        """
        Shield for Strategy 2 (Fade Fakeout Shorts).
        Blocks shorting into an aggressive nationwide/global market pump wave.
        """
        snap = self.get_regime(now)
        if snap.regime == MarketRegime.STRONG_PUMP:
            return False, f"Strong Market Pump in progress (Breadth: {snap.breadth_pct_300s:.1f}%, BTC: {snap.btc_ret_300s_bps:+.1f} bps)"
        return True, "OK"

    def get_tsunami_multiplier(self, now: float) -> float:
        """
        Dynamic multiplier for Tsunami Ratio threshold:
        - In STRONG_PUMP: reduces required tsunami (0.6x) because market tailwinds carry the trade.
        - In BEAR_DRIFT: tightens required tsunami (1.5x) to demand overwhelming whale volume against the trend.
        - In NEUTRAL / BULL_DRIFT: 1.0x baseline.
        """
        snap = self.get_regime(now)
        if snap.regime == MarketRegime.STRONG_PUMP:
            return 0.6
        if snap.regime == MarketRegime.BEAR_DRIFT:
            return 1.5
        return 1.0

    def get_regime_modifiers(self, side: str, now: float) -> Tuple[float, float, float]:
        """
        Calculates dynamic (edge_multiplier, drain_multiplier, size_multiplier) for S1 Lead-Lag:
        - With market regime tailwind (Long in PUMP, Short in DUMP/BEAR):
          - Lower edge requirement (0.7x - 0.8x, easier entry).
          - Lower drain threshold (0.7x - 0.8x, faster trigger).
          - Higher position size multiplier (1.3x - 1.5x).
        - Against market regime (Long in BEAR, Short in BULL):
          - Higher edge requirement (1.4x, stricter defense).
          - Higher drain requirement (1.3x, requires massive idiosyncratic sweep).
          - Reduced position size (0.5x).
        - NEUTRAL: 1.0x baseline across all parameters.
        """
        snap = self.get_regime(now)
        reg = snap.regime

        if side.lower() == "long":
            if reg == MarketRegime.STRONG_PUMP:
                return (0.7, 0.8, 1.5)
            elif reg == MarketRegime.BULL_DRIFT:
                return (0.9, 0.9, 1.2)
            elif reg == MarketRegime.BEAR_DRIFT:
                return (1.4, 1.3, 0.5)
            elif reg == MarketRegime.FLASH_DUMP:
                return (2.0, 2.0, 0.0)  # Longs are vetoed during flash dump
            else:  # NEUTRAL
                return (1.0, 1.0, 1.0)
        else:  # short
            if reg == MarketRegime.FLASH_DUMP:
                return (0.7, 0.7, 1.5)
            elif reg == MarketRegime.BEAR_DRIFT:
                return (0.8, 0.8, 1.3)
            elif reg == MarketRegime.STRONG_PUMP:
                return (2.0, 2.0, 0.0)  # Shorts are vetoed during strong pump
            elif reg == MarketRegime.BULL_DRIFT:
                return (1.4, 1.3, 0.5)
            else:  # NEUTRAL
                return (1.0, 1.0, 1.0)
