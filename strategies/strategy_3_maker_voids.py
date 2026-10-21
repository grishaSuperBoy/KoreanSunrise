"""
strategies/strategy_3_maker_voids.py - Strategy 3: Maker Liquidity Harvesting & Spread Voids.

Microstructure Simulation:
When volatility or a cross-venue burst occurs, designated market makers (DMM) on follower perps
briefly pull quotes or widen spreads.
This paper-trading telemetry module simulates passive maker limit orders or predatory taker void sweeps
inside widened spreads to evaluate theoretical capture against synthetic order book depth.
(No live orders are placed; simulated fills and PnL are evaluated via order book snapshots).
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
    side: str                          # "ask" / "bid" (classic maker) or "buy" / "sell" (taker hunter)
    open_ts: float

    # Pricing & Order placement
    best_bid_at_open: float
    best_ask_at_open: float
    spread_open_bps: float
    target_limit_px: float             # Post-Only Limit price inside the spread or taker entry
    ref_lead_mid: float
    maker_edge_bps: float
    suggested_size_usd: float
    order_type: str = "maker_post_only" # "maker_post_only" or "taker_void_sweep"

    # Execution state
    maker_filled: bool = False
    fill_ts: Optional[float] = None
    fill_px: Optional[float] = None
    ttl_sec: float = 3.0               # Max resting time before cancelling unfilled limit order
    max_hold_sec: float = 10.0          # Max hold time after fill
    maker_rebate_bps: float = 0.0      # Realistic maker rebate on alts (0.0% by default)
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
    Simulates and tracks theoretical maker limit or taker sweep executions on widened follower spreads.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        min_order_usd: float = 10.0,
        max_position_budget_usd: float = 250.0,
        stealth_depth_fraction: float = 0.15,
        min_spread_bps: float = 25.0,
        min_maker_edge_bps: float = 5.0,
        maker_rebate_bps: float = 0.0,
        taker_fee_bps: float = 5.0,
        resting_ttl_sec: float = 3.0,
        max_hold_sec: float = 10.0,
        max_follower_book_age_sec: float = 1.5,
        stop_invalidation_bps: float = 15.0,
        stop_adverse_pnl_bps: float = 25.0,
        max_spread_bps: float = 120.0,
        target_pnl_bps: float = 15.0,
        cooldown_sec: float = 30.0,
        allow_asterdex_maker: bool = False,
        hunter_mode: bool = False,
        max_active_voids: int = 10,
        max_total_exposure_usd: float = 5000.0,
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
        self.max_follower_book_age_sec = max_follower_book_age_sec
        self.stop_invalidation_bps = stop_invalidation_bps
        self.stop_adverse_pnl_bps = stop_adverse_pnl_bps
        self.max_spread_bps = max_spread_bps
        self.target_pnl_bps = target_pnl_bps
        self.cooldown_sec = cooldown_sec
        self.allow_asterdex_maker = allow_asterdex_maker
        self.hunter_mode = hunter_mode
        self.max_active_voids = max_active_voids
        self.max_total_exposure_usd = max_total_exposure_usd

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
            "maker_voids_timeout": 0,
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
        recv_ts: Optional[float] = None,
        is_warmed_up: bool = True,
    ) -> Optional[ActiveMakerVoid]:
        """
        Detects if follower order book has experienced a spread blowout or liquidity void.
        Compares the follower's instantaneous spread against its own baseline EWMA spread (norm_spread_bps).
        """
        with self._lock:
            # 0a. Warmup Guard: skip void detection until follower spread baseline has collected warmup samples
            if not is_warmed_up:
                return None

            # 0b. Staleness Guard: reject stale book snapshots (> max_follower_book_age_sec)
            snap_ts = recv_ts if recv_ts is not None else float(getattr(f_snap, "ts", now) or now)
            if (now - snap_ts) > self.max_follower_book_age_sec:
                return None

            # 0b. Portfolio Exposure Guard: cap total active voids and exposure budget
            active_voids = [v for v in self._open_voids.values() if not v.closed]
            if len(active_voids) >= self.max_active_voids:
                return None
            total_exposure = sum(v.suggested_size_usd for v in active_voids)
            if total_exposure >= self.max_total_exposure_usd:
                return None

            f_bid = float(getattr(f_snap, "best_bid", 0.0) or 0.0)
            f_ask = float(getattr(f_snap, "best_ask", 0.0) or 0.0)
            f_spread = float(getattr(f_snap, "spread_bps", 0.0) or 0.0)
            if f_bid <= 0 or f_ask <= 0:
                return None

            # Cooldown guard: at least cooldown_sec between void postings for same (follower_ex, coin)
            pair_key = (follower_ex, coin)
            if (now - self._last_void_ts.get(pair_key, 0.0)) < self.cooldown_sec:
                return None

            upbit_mid = float(getattr(upbit_snap, "mid_price", 0.0) or 0.0) if upbit_snap else 0.0
            follower_mid = float(getattr(f_snap, "mid_price", 0.0) or (0.5 * (f_bid + f_ask)))
            half_spread_edge = 0.5 * f_spread

            if self.hunter_mode:
                # Taker Lag-Arb Trigger (Tight Spread + Large Lag):
                # Takers MUST cross the spread! Entering a wide spread produces an instant
                # structural MTM loss of (-spread - 2*fees).
                # Therefore, Taker Hunter triggers ONLY on:
                # 1. Tight spread: f_spread <= 20.0 bps (cheap to cross)
                # 2. Hard DEX ceiling: reject AsterDex spreads > 20.0 bps
                # 3. Large dislocation lag: |upbit_mid - follower_mid| / follower_mid * 1e4 >= 40.0 bps
                # 4. Net positive edge: edge > spread/2 + roundtrip_fee + 10.0 margin
                if f_spread > 20.0:
                    return None
                if follower_ex.lower() == "asterdex" and f_spread > 20.0:
                    return None
                if upbit_mid <= 0 or follower_mid <= 0:
                    return None

                dislocation_bps = (upbit_mid - follower_mid) / follower_mid * 1e4
                if abs(dislocation_bps) < 40.0:
                    return None

                roundtrip_fee = self.taker_fee_bps * 2.0
                required_edge = (f_spread / 2.0) + roundtrip_fee + 10.0

                if dislocation_bps > 0:
                    # Upbit is leading higher -> Taker BUY by sweeping ask
                    side = "buy"
                    target_limit_px = f_ask
                    maker_edge_bps = (upbit_mid - f_ask) / f_ask * 1e4
                    if maker_edge_bps < required_edge:
                        return None
                    order_type = "taker_void_sweep"
                else:
                    # Upbit is leading lower -> Taker SELL by hitting bid
                    side = "sell"
                    target_limit_px = f_bid
                    maker_edge_bps = (f_bid - upbit_mid) / upbit_mid * 1e4
                    if maker_edge_bps < required_edge:
                        return None
                    order_type = "taker_void_sweep"
            else:
                # Classic Maker Mode (Post-Only inside Widened Spreads):
                # 1. Spread must be blown out (>= 25 bps AND >= 1.4x baseline spread)
                if f_spread < max(self.min_spread_bps, norm_spread_bps * 1.4):
                    return None

                # 2. AsterDex DEX safety guard (reject synthetic/wide spreads)
                if follower_ex.lower() == "asterdex":
                    if not self.allow_asterdex_maker or f_spread > 45.0:
                        return None

                # 3. Toxic pool / broken book safety ceiling
                if f_spread > self.max_spread_bps:
                    return None

                order_type = "maker_post_only"
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

            if side in ("ask", "buy") and hasattr(f_snap, "top_n_ask_depth_usd"):
                top3_depth = float(f_snap.top_n_ask_depth_usd(3))
            elif side in ("bid", "sell") and hasattr(f_snap, "top_n_bid_depth_usd"):
                top3_depth = float(f_snap.top_n_bid_depth_usd(3))
            else:
                top3_depth = 100.0
            size_usd = min(self.max_position_budget_usd, max(self.min_order_usd, round(top3_depth * self.stealth_depth_fraction, 2)))
            if total_exposure + size_usd > self.max_total_exposure_usd:
                return None

            seq_num = next(self._seq)
            void = ActiveMakerVoid(
                id=f"void_{int(now)}_{seq_num:04d}",
                coin=coin,
                follower_exchange=follower_ex,
                follower_contract=getattr(f_snap, "raw_symbol", f"{coin}USDT"),
                side=side,
                order_type=order_type,
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
                maker_filled=True if self.hunter_mode else False,
                fill_ts=round(now, 3) if self.hunter_mode else None,
                fill_px=round(target_limit_px, 8) if self.hunter_mode else None,
            )
            self._open_voids[key] = void
            self._last_void_ts[pair_key] = now
            self.counters["maker_voids_triggered"] += 1
            if self.hunter_mode:
                self.counters["maker_voids_filled"] += 1
                log.info(
                    f"[TAKER-HUNTER] ⚡ Swept {side.upper()} on {coin} ({void.follower_contract}) @ {follower_ex} | "
                    f"Taker Px: {target_limit_px} | Edge: +{maker_edge_bps:.1f} bps | Spread: {f_spread:.1f} bps"
                )
            else:
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
        recv_ts: Optional[float] = None,
    ) -> None:
        """
        Monitors fill crossing and exit lifecycle for active maker void orders.
        """
        with self._lock:
            snap_ts = recv_ts if recv_ts is not None else float(getattr(f_snap, "ts", now) or now)
            if (now - snap_ts) > self.max_follower_book_age_sec:
                return

            cur_bid = float(getattr(f_snap, "best_bid", 0.0) or 0.0)
            cur_ask = float(getattr(f_snap, "best_ask", 0.0) or 0.0)
            cur_spread = float(getattr(f_snap, "spread_bps", 0.0) or 0.0)
            if cur_bid <= 0 or cur_ask <= 0:
                return

            to_remove: List[Tuple[str, str, str]] = []

            for key in list(self._open_voids.keys()):
                if key[0] != follower_ex or key[1] != coin:
                    continue
                void = self._open_voids.get(key)
                if void is None or void.closed:
                    continue

                side = void.side
                dt = max(0.0, now - void.open_ts)

                # Check fill crossing (for classic maker post-only)
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

                    if getattr(void, "order_type", "maker_post_only") == "taker_void_sweep":
                        # Taker Void Hunter PnL model:
                        if side == "buy":
                            gross_pnl_bps = (cur_bid - void.fill_px) / void.fill_px * 1e4
                        else:
                            gross_pnl_bps = (void.fill_px - cur_ask) / void.fill_px * 1e4
                        net_pnl_bps = gross_pnl_bps - (void.taker_fee_bps * 2.0)
                    else:
                        # Realistic Maker PnL model: maker rebate on entry, taker fee on exit
                        if side == "ask":
                            gross_pnl_bps = (void.fill_px - cur_ask) / void.fill_px * 1e4
                        else:
                            gross_pnl_bps = (cur_bid - void.fill_px) / void.fill_px * 1e4
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
                    # In taker hunter mode, account for initial entry crossing cost (spread + fees)
                    if getattr(void, "order_type", "") == "taker_void_sweep":
                        stop_thresh = -(void.spread_open_bps + (void.taker_fee_bps * 2.0) + self.stop_invalidation_bps)
                    else:
                        stop_thresh = -self.stop_invalidation_bps

                    if net_pnl_bps <= stop_thresh:
                        close_reason = "stop_invalidation"
                        self.counters["maker_voids_stopped"] += 1
                    # 3. Timeout
                    elif dt_fill >= void.max_hold_sec:
                        close_reason = "timeout"
                        self.counters["maker_voids_timeout"] += 1

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
                        self.counters["maker_voids_timeout"] += 1
                        self._emit_void(void)
                        to_remove.append(key)

            for k in to_remove:
                self._open_voids.pop(k, None)

    def _emit_void(self, void: ActiveMakerVoid) -> None:
        """Serializes and sends closed void record to batch writer callback."""
        rec = {
            "id": void.id,
            "strategy": "maker_void",
            "order_type": getattr(void, "order_type", "maker_post_only"),
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
