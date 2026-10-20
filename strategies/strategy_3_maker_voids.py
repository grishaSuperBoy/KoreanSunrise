"""
strategies/strategy_3_maker_voids.py - Strategy 3: Maker Liquidity Harvesting & Spread Voids.

Microstructure Physics:
When volatility or a cross-venue burst occurs, designated market makers (DMM) on follower perps
briefly pull quotes or widen spreads by 30-100 bps.
Instead of paying aggressive taker fees, this strategy posts passive Maker Post-Only limit orders
directly inside the widened spread. It captures the spread + maker fee rebates as the book normalizes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import itertools
import logging
import math
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("strategy.maker_voids")


@dataclass
class ActiveMakerVoid:
    id: str
    coin: str
    follower_exchange: str
    follower_contract: str
    side: str                          # "ask" (sell limit) or "bid" (buy limit)
    open_ts: float

    # Pricing & Order placement
    best_bid_at_open: float
    best_ask_at_open: float
    spread_open_bps: float
    target_limit_px: float             # Post-Only Limit price inside the spread
    ref_lead_mid: float
    maker_edge_bps: float
    suggested_size_usd: float

    # Execution state
    maker_filled: bool = False
    fill_ts: Optional[float] = None
    fill_px: Optional[float] = None
    ttl_sec: float = 3.0               # Max resting time before cancelling unfilled limit order
    max_hold_sec: float = 10.0          # Max hold time after fill
    maker_rebate_bps: float = 1.5      # Maker fee rebate (or 0% maker fee)
    taker_fee_bps: float = 5.0         # Realistic exit taker fee (market exit on normalization)

    # Outcome
    closed: bool = False
    close_ts: Optional[float] = None
    duration_sec: Optional[float] = None
    close_reason: Optional[str] = None
    spread_close_bps: float = 0.0
    net_pnl_bps: Optional[float] = None


class MakerVoidStrategy:
    """
    Executes and tracks passive Maker Post-Only limit orders capturing widened spreads.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        min_order_usd: float = 10.0,
        max_position_budget_usd: float = 250.0,
        stealth_depth_fraction: float = 0.15,
        min_spread_bps: float = 25.0,
        min_maker_edge_bps: float = 5.0,
        maker_rebate_bps: float = 1.5,
        taker_fee_bps: float = 5.0,
        resting_ttl_sec: float = 3.0,
        max_hold_sec: float = 10.0,
        stop_invalidation_bps: float = 15.0,
        stop_adverse_pnl_bps: float = 25.0,
        max_spread_bps: float = 120.0,
        target_pnl_bps: float = 15.0,
        cooldown_sec: float = 30.0,
        allow_asterdex_maker: bool = False,
        counters: Optional[Dict[str, int]] = None,
    ):
        self.emit = emit_callback
        self.min_order_usd = min_order_usd
        self.max_position_budget_usd = max_position_budget_usd
        self.stealth_depth_fraction = stealth_depth_fraction
        self.min_spread_bps = min_spread_bps
        self.min_maker_edge_bps = min_maker_edge_bps
        self.maker_rebate_bps = maker_rebate_bps
        self.taker_fee_bps = taker_fee_bps
        self.resting_ttl_sec = resting_ttl_sec
        self.max_hold_sec = max_hold_sec
        self.stop_invalidation_bps = stop_invalidation_bps
        self.stop_adverse_pnl_bps = stop_adverse_pnl_bps
        self.max_spread_bps = max_spread_bps
        self.target_pnl_bps = target_pnl_bps
        self.cooldown_sec = cooldown_sec
        self.allow_asterdex_maker = allow_asterdex_maker

        # (follower_ex, coin, side) -> ActiveMakerVoid
        self._open_voids: Dict[Tuple[str, str, str], ActiveMakerVoid] = {}
        self._last_void_ts: Dict[Tuple[str, str], float] = {}
        self._seq = itertools.count(1)
        self._lock = threading.RLock()
        default_counters = {
            "maker_voids_triggered": 0,
            "maker_voids_filled": 0,
            "maker_voids_restored": 0,
            "maker_voids_unfilled": 0,
            "maker_voids_stopped": 0,
        }
        if counters is not None:
            for k, v in default_counters.items():
                counters.setdefault(k, v)
            self.counters = counters
        else:
            self.counters = default_counters

    def check_void_opportunity(
        self,
        follower_ex: str,
        coin: str,
        f_snap: Any,
        upbit_snap: Optional[Any],
        now: float,
        norm_spread_bps: float = 20.0,
    ) -> Optional[ActiveMakerVoid]:
        """
        Detects if follower order book has experienced a spread blowout or liquidity void.
        """
        with self._lock:
            f_bid = float(getattr(f_snap, "best_bid", 0.0) or 0.0)
            f_ask = float(getattr(f_snap, "best_ask", 0.0) or 0.0)
            f_spread = float(getattr(f_snap, "spread_bps", 0.0) or 0.0)
            if f_bid <= 0 or f_ask <= 0:
                return None

            # Spread must be blown out (>= 25 bps AND >= 1.4x baseline spread)
            if f_spread < max(self.min_spread_bps, norm_spread_bps * 1.4):
                return None

            # AsterDex DEX safety guard:
            # Passive maker orders on DEX pools with synthetic/wide spreads face 100% adverse selection.
            # Only allow if explicitly enabled AND spread is tight (<= 45.0 bps).
            if follower_ex.lower() == "asterdex":
                if not self.allow_asterdex_maker or f_spread > 45.0:
                    return None

            # Toxic pool / broken book safety ceiling (reject DEX spreads > max_spread_bps)
            if f_spread > self.max_spread_bps:
                return None

            # Cooldown guard: at least cooldown_sec between void postings for same (follower_ex, coin)
            pair_key = (follower_ex, coin)
            if (now - self._last_void_ts.get(pair_key, 0.0)) < self.cooldown_sec:
                return None

            upbit_mid = float(getattr(upbit_snap, "mid_price", 0.0) or 0.0) if upbit_snap else 0.0
            follower_mid = float(getattr(f_snap, "mid_price", 0.0) or (0.5 * (f_bid + f_ask)))

            # Determine side and maker edge
            half_spread_edge = 0.5 * f_spread

            if upbit_mid > follower_mid and upbit_mid > 0:
                side = "ask"
                target_limit_px = f_ask - 0.5 * (f_ask - f_bid)
                maker_edge_bps = max(half_spread_edge, (upbit_mid - target_limit_px) / target_limit_px * 1e4)
            elif upbit_mid < follower_mid and upbit_mid > 0:
                side = "bid"
                target_limit_px = f_bid + 0.5 * (f_ask - f_bid)
                maker_edge_bps = max(half_spread_edge, (target_limit_px - upbit_mid) / target_limit_px * 1e4)
            else:
                side = "ask"
                target_limit_px = f_ask - 0.5 * (f_ask - f_bid)
                maker_edge_bps = half_spread_edge

            if maker_edge_bps < self.min_maker_edge_bps:
                return None

            key = (follower_ex, coin, side)
            if key in self._open_voids:
                return None

            if side == "ask" and hasattr(f_snap, "top_n_ask_depth_usd"):
                top3_depth = float(f_snap.top_n_ask_depth_usd(3))
            elif side == "bid" and hasattr(f_snap, "top_n_bid_depth_usd"):
                top3_depth = float(f_snap.top_n_bid_depth_usd(3))
            else:
                top3_depth = 100.0
            size_usd = min(self.max_position_budget_usd, max(self.min_order_usd, round(top3_depth * self.stealth_depth_fraction, 2)))

            seq_num = next(self._seq)
            void = ActiveMakerVoid(
                id=f"void_{int(now)}_{seq_num:04d}",
                coin=coin,
                follower_exchange=follower_ex,
                follower_contract=getattr(f_snap, "raw_symbol", f"{coin}USDT"),
                side=side,
                open_ts=round(now, 3),
                best_bid_at_open=round(f_bid, 8),
                best_ask_at_open=round(f_ask, 8),
                spread_open_bps=round(f_spread, 2),
                target_limit_px=round(target_limit_px, 8),
                ref_lead_mid=round(upbit_mid, 8),
                maker_edge_bps=round(maker_edge_bps, 2),
                suggested_size_usd=round(size_usd, 2),
                ttl_sec=self.resting_ttl_sec,
                max_hold_sec=self.max_hold_sec,
                maker_rebate_bps=self.maker_rebate_bps,
                taker_fee_bps=self.taker_fee_bps,
            )
            self._open_voids[key] = void
            self._last_void_ts[pair_key] = now
            self.counters["maker_voids_triggered"] += 1
            log.info(
                f"[MAKER-VOID] 🛡️ Posted Post-Only {side.upper()} on {coin} ({void.follower_contract}) @ {follower_ex} | "
                f"Limit Px: {target_limit_px} | Edge: +{maker_edge_bps:.1f} bps | Spread: {f_spread:.1f} bps"
            )
            return void

    def on_follower_tick(
        self,
        follower_ex: str,
        coin: str,
        f_snap: Any,
        now: float,
        norm_spread_bps: float = 20.0,
    ) -> None:
        """
        Monitors fill crossing and exit lifecycle for active maker void orders.
        """
        with self._lock:
            cur_bid = float(getattr(f_snap, "best_bid", 0.0) or 0.0)
            cur_ask = float(getattr(f_snap, "best_ask", 0.0) or 0.0)
            cur_spread = float(getattr(f_snap, "spread_bps", 0.0) or 0.0)
            if cur_bid <= 0 or cur_ask <= 0:
                return

            to_remove: List[Tuple[str, str, str]] = []

            for side in ("ask", "bid"):
                key = (follower_ex, coin, side)
                void = self._open_voids.get(key)
                if void is None or void.closed:
                    continue

                dt = max(0.0, now - void.open_ts)

                # Check fill crossing
                if not void.maker_filled:
                    is_filled = (
                        (side == "ask" and cur_bid >= void.target_limit_px)
                        or (side == "bid" and cur_ask <= void.target_limit_px)
                    )
                    if is_filled:
                        void.maker_filled = True
                        void.fill_ts = now
                        void.fill_px = void.target_limit_px
                        self.counters["maker_voids_filled"] += 1
                        log.info(f"[MAKER-FILL] ⚡ Order FILLED on {coin} {side.upper()} @ {void.target_limit_px} on {follower_ex}!")
                    elif dt >= void.ttl_sec:
                        # Resting timeout: cancel unfilled order cleanly without fees
                        void.closed = True
                        void.close_ts = round(now, 3)
                        void.duration_sec = round(dt, 3)
                        void.close_reason = "unfilled_timeout"
                        void.net_pnl_bps = 0.0
                        void.spread_close_bps = round(cur_spread, 2)
                        self.counters["maker_voids_unfilled"] += 1
                        self._emit_void(void)
                        to_remove.append(key)
                        continue

                # Position is FILLED: monitor exit & spread restoration
                if void.maker_filled:
                    dt_fill = max(0.0, now - (void.fill_ts or void.open_ts))
                    # If we sold as Maker Ask: profit when cur_ask is below fill_px + rebate
                    # If we bought as Maker Bid: profit when cur_bid is above fill_px + rebate
                    if side == "ask":
                        gross_pnl_bps = (void.fill_px - cur_ask) / void.fill_px * 1e4
                    else:
                        gross_pnl_bps = (cur_bid - void.fill_px) / void.fill_px * 1e4

                    # Realistic PnL model: maker rebate on entry, taker fee on exit
                    net_pnl_bps = gross_pnl_bps + void.maker_rebate_bps - void.taker_fee_bps

                    close_reason: Optional[str] = None

                    # 1. Take-Profit: Target Net PnL reached (>= target_pnl_bps) or Spread restored to normal
                    if net_pnl_bps >= self.target_pnl_bps:
                        close_reason = "target_profit"
                        self.counters["maker_voids_restored"] += 1
                    elif cur_spread <= max(15.0, norm_spread_bps * 1.15) and net_pnl_bps > 0:
                        close_reason = "restored"
                        self.counters["maker_voids_restored"] += 1
                    # 2. Hard Invalidation stop
                    elif net_pnl_bps <= -self.stop_invalidation_bps:
                        close_reason = "stop_invalidation"
                        self.counters["maker_voids_stopped"] += 1
                    # 3. Timeout
                    elif dt_fill >= void.max_hold_sec:
                        close_reason = "timeout"

                    if close_reason:
                        void.closed = True
                        void.close_ts = round(now, 3)
                        void.duration_sec = round(dt, 3)
                        void.close_reason = close_reason
                        void.net_pnl_bps = round(net_pnl_bps, 2)
                        void.spread_close_bps = round(cur_spread, 2)
                        self._emit_void(void)
                        to_remove.append(key)

            for k in to_remove:
                self._open_voids.pop(k, None)

    def sweep_timeouts(self, now: float) -> None:
        """
        Sweeps and cancels/closes expired maker void orders if stream ticks stopped arriving.
        Prevents open limit orders or filled positions from hanging indefinitely.
        """
        with self._lock:
            to_remove: List[Tuple[str, str, str]] = []
            for key, void in list(self._open_voids.items()):
                if void.closed:
                    continue

                dt = max(0.0, now - void.open_ts)
                if not void.maker_filled:
                    if dt >= void.ttl_sec:
                        # Resting timeout without fill: cancel cleanly with 0 PnL
                        void.closed = True
                        void.close_ts = round(now, 3)
                        void.duration_sec = round(dt, 3)
                        void.close_reason = "unfilled_timeout"
                        void.net_pnl_bps = 0.0
                        self.counters["maker_voids_unfilled"] += 1
                        self._emit_void(void)
                        to_remove.append(key)
                else:
                    dt_fill = max(0.0, now - (void.fill_ts or void.open_ts))
                    if dt_fill >= void.max_hold_sec:
                        # Max hold expired without subsequent follower tick: close with fee deduction
                        void.closed = True
                        void.close_ts = round(now, 3)
                        void.duration_sec = round(dt, 3)
                        void.close_reason = "timeout_no_follower_data"
                        void.net_pnl_bps = round(void.maker_rebate_bps - void.taker_fee_bps, 2)
                        self.counters["maker_voids_stopped"] += 1
                        self._emit_void(void)
                        to_remove.append(key)

            for k in to_remove:
                self._open_voids.pop(k, None)

    def _emit_void(self, void: ActiveMakerVoid) -> None:
        """Serializes and sends closed void record to batch writer callback."""
        rec = {
            "id": void.id,
            "strategy": "maker_void",
            "order_type": "maker_post_only",
            "side": void.side,
            "coin": void.coin,
            "follower_exchange": void.follower_exchange,
            "follower_contract": void.follower_contract,
            "open_ts": void.open_ts,
            "close_ts": void.close_ts,
            "duration_sec": void.duration_sec,
            "target_limit_px": void.target_limit_px,
            "maker_edge_bps": void.maker_edge_bps,
            "maker_filled": void.maker_filled,
            "spread_open_bps": void.spread_open_bps,
            "spread_close_bps": void.spread_close_bps,
            "suggested_size_usd": void.suggested_size_usd,
            "net_pnl_bps": void.net_pnl_bps,
            "close_reason": void.close_reason,
            "maker_rebate_bps": void.maker_rebate_bps,
            "taker_fee_bps": void.taker_fee_bps,
        }
        self.emit(rec)
        log.info(
            f"[MAKER-EXIT] 🏁 Void closed on {void.coin} ({void.side}) @ {void.follower_exchange} | "
            f"Filled: {void.maker_filled} | Net: {void.net_pnl_bps:+.2f} bps | Reason: {void.close_reason}"
        )
