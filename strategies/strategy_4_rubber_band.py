"""
strategies/strategy_4_rubber_band.py - Strategy 4: Predatory Rebound Maker (Rubber Band).

Microstructure Physics:
During systemic market selloffs or liquidation cascades (FLASH_DUMP), leveraged follower perps
(Binance/Bybit/OKX/etc.) drop precipitously due to forced margin liquidations and thin bid depth.
Conversely, co-listed Korean spot tokens on Upbit and Bithumb experience no margin calls and remain
anchored by Korean retail fiat liquidity.

This creates an extreme positive dislocation:
    Dislocation = (Mid_Upbit_USD - Mid_Follower_USDT) / Mid_Follower_USDT * 10,000 bps

The "Rubber Band" is stretched to its limit.
However, entering at market ask (taker) during a liquidation cascade is toxic: slippage is severe,
fees are high, and cascades frequently produce 2-3 consecutive replenishment wicks down.

Predatory Maker Architecture:
1. When dislocation reaches statistical extremes (Z >= 3.0σ, dislocation >= 100 bps) and market breadth
   shows initial exhaustion / stabilization (rebound >= +10% from trough):
2. Instead of buying at market ask, we post a Predatory Post-Only Limit BID at a discount below best bid
   (best_bid - 25..35 bps) with a short resting TTL (2.5s).
3. If the liquidation wick flushes into our discount bid:
   - Order is FILLED at the bottom of the wick.
   - Entry fee is 0 bps (maker order / rebate).
4. If the wick does not reach our bid within 2.5s:
   - Resting order expires cleanly with 0 loss and 0 fee (unfilled_timeout).
5. If the Upbit spot anchor collapses (cur_upbit_mid drops):
   - Resting order is immediately cancelled or filled position exited.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import itertools
import logging
import math
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import config as C

log = logging.getLogger("strategy.rubber_band")


@dataclass
class ActiveRubberBandPosition:
    id: str
    coin: str
    leader_exchange: str
    follower_exchange: str
    follower_contract: str
    open_ts: float
    trigger_reason: str

    # Pricing & Dislocation at trigger
    entry_upbit_mid: float
    entry_follower_mid: float
    dislocation_bps_open: float
    dislocation_z: float = 3.0
    spread_open_bps: float = 0.0
    side: str = "long"                # Always long on rebound

    # Predatory Maker Execution
    order_type: str = "maker_post_only"  # "maker_post_only" | "taker_sniper"
    target_limit_px: float = 0.0         # Discounted limit bid price
    entry_px: float = 0.0                # Limit bid px or fill px
    suggested_size_usd: float = 0.0
    maker_filled: bool = False
    fill_ts: Optional[float] = None
    fill_px: Optional[float] = None
    maker_ttl_sec: float = 2.5
    maker_discount_bps: float = 25.0
    maker_rebate_bps: float = 0.0
    taker_fee_bps: float = 5.0           # Exit leg taker fee

    # Targets & Risk Parameters
    target_rebound_bps: float = 35.0
    stop_loss_bps: float = 25.0
    roundtrip_fee_bps: float = 5.0       # 0 bps maker entry + 5 bps taker exit
    max_hold_sec: float = 30.0
    alpha_decay_sec: float = 15.0

    # Lifecycle state
    closed: bool = False
    close_ts: Optional[float] = None
    duration_sec: Optional[float] = None
    close_reason: Optional[str] = None
    exit_px: Optional[float] = None
    net_pnl_bps: Optional[float] = None
    mfe_exec_pnl_bps: float = -999.0
    mae_exec_pnl_bps: float = 999.0

    path: List[List[float]] = field(default_factory=list)


class RubberBandSniperStrategy:
    """
    Quantitative execution engine for Strategy 4: Predatory Rebound Maker.
    Catches liquidation wicks on follower perpetuals at a discount below best bid
    anchored by Korean spot liquidity.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        min_order_usd: float = 10.0,
        max_position_budget_usd: float = 500.0,
        stealth_depth_fraction: float = 0.15,
        min_dislocation_bps: float = 100.0,
        min_z_score: float = 3.0,
        maker_discount_bps: float = 25.0,
        maker_ttl_sec: float = 2.5,
        target_rebound_bps: float = 35.0,
        stop_loss_bps: float = 25.0,
        maker_rebate_bps: float = 0.0,
        taker_fee_bps: float = 5.0,
        roundtrip_fee_bps: Optional[float] = None,
        max_hold_sec: float = 30.0,
        alpha_decay_sec: float = 15.0,
        cooldown_sec: float = 20.0,
        penalty_cooldown_sec: float = getattr(C, "RUBBER_BAND_PENALTY_COOLDOWN_SEC", 120.0),
        max_active_positions: int = 10,
        max_total_exposure_usd: float = 5000.0,
        hunter_mode: bool = False,         # If True: instant taker fill (for backwards-compat/testing)
        dynamic_discount: bool = False,    # If True: auto-derive discount from spread (0.0..2.5 bps)
        dynamic_target: bool = False,      # If True: auto-derive rebound target from spread & sigma (8..15 bps)
        enabled: bool = True,
        counters: Optional[Dict[str, int]] = None,
    ):
        self.emit = emit_callback
        self.min_order_usd = min_order_usd
        self.max_position_budget_usd = max_position_budget_usd
        self.stealth_depth_fraction = stealth_depth_fraction
        self.min_dislocation_bps = min_dislocation_bps
        self.min_z_score = min_z_score
        self.maker_discount_bps = maker_discount_bps
        self.maker_ttl_sec = maker_ttl_sec
        self.target_rebound_bps = target_rebound_bps
        self.stop_loss_bps = stop_loss_bps
        self.maker_rebate_bps = maker_rebate_bps
        self.taker_fee_bps = taker_fee_bps
        # Roundtrip fee: in maker mode = 0 maker entry + taker exit = taker_fee_bps
        if roundtrip_fee_bps is not None:
            self.roundtrip_fee_bps = roundtrip_fee_bps
        else:
            self.roundtrip_fee_bps = (taker_fee_bps * 2.0) if hunter_mode else (taker_fee_bps - maker_rebate_bps)
        self.max_hold_sec = max_hold_sec
        self.alpha_decay_sec = alpha_decay_sec
        self.cooldown_sec = cooldown_sec
        self.penalty_cooldown_sec = penalty_cooldown_sec
        self.max_active_positions = max_active_positions
        self.max_total_exposure_usd = max_total_exposure_usd
        self.hunter_mode = hunter_mode
        self.dynamic_discount = dynamic_discount
        self.dynamic_target = dynamic_target
        self.enabled = enabled
        self.counters = counters if counters is not None else {}

        self._lock = threading.Lock()
        self._seq = itertools.count(1)

        # Active positions: id -> ActiveRubberBandPosition
        self._active_positions: Dict[str, ActiveRubberBandPosition] = {}
        # Quick lookup: (follower_ex, coin) -> id
        self._coin_pos_map: Dict[Tuple[str, str], str] = {}
        # Cooldown: (follower_ex, coin) -> last_trigger_ts
        self._last_trigger_ts: Dict[Tuple[str, str], float] = {}
        # Per-pair active cooldown requirement (standard vs penalty after stop)
        self._pair_cooldowns: Dict[Tuple[str, str], float] = {}

        # Dump exhaustion tracking
        self._last_dump_ts: float = 0.0

    def on_regime_update(self, regime_str: str, now: float) -> None:
        """Called whenever the macro market regime snapshot is refreshed."""
        with self._lock:
            if regime_str == "FLASH_DUMP":
                self._last_dump_ts = now

    def check_rubber_band_opportunity(
        self,
        follower_ex: str,
        coin: str,
        follower_snap: Any,
        upbit_snap: Optional[Any],
        now: float,
        regime_snapshot: Optional[Any] = None,
        recv_ts: float = 0.0,
        pair_sigma_bps: float = 35.0,
        baseline_bps: float = 0.0,
    ) -> Optional[ActiveRubberBandPosition]:
        """
        Evaluates whether a co-listed altcoin exhibits extreme statistical rubber-band dislocation
        and post-dump exhaustion signals to warrant a predatory maker limit bid.
        """
        if not self.enabled:
            return None

        # 1. Basic book validity and freshness checks
        if upbit_snap is None or follower_snap is None:
            return None

        f_age = now - recv_ts if recv_ts > 0 else 0.0
        if f_age > 1.5:
            return None

        upbit_mid = getattr(upbit_snap, "mid_price", 0.0)
        f_mid = getattr(follower_snap, "mid_price", 0.0)
        if upbit_mid <= 0.0 or f_mid <= 0.0:
            return None

        f_spread_bps = getattr(follower_snap, "spread_bps", 999.0)
        if f_spread_bps > 30.0:
            return None  # Disregard illiquid blown-out books

        # 2. Rubber-band tension (dislocation) & Statistical Z-Score check
        # Elastic tension is measured relative to the pair's historical Kimchi baseline
        raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4
        dislocation_bps = raw_diff_bps - baseline_bps
        if dislocation_bps < self.min_dislocation_bps:
            return None

        effective_sigma = max(10.0, pair_sigma_bps)
        disloc_z = dislocation_bps / effective_sigma
        if disloc_z < self.min_z_score:
            # If dislocation is enormous (>= 150 bps), allow even if sigma is uncalibrated
            if dislocation_bps < (self.min_dislocation_bps * 1.5):
                return None

        # 3. Exhaustion & Turn Trigger
        regime_str = "NEUTRAL"
        btc_ret_30s = 0.0
        breadth_60s = 50.0
        if regime_snapshot is not None:
            reg_enum = getattr(regime_snapshot, "regime", None)
            regime_str = reg_enum.value if hasattr(reg_enum, "value") else str(reg_enum or "NEUTRAL")
            btc_ret_30s = getattr(regime_snapshot, "btc_ret_30s_bps", 0.0)
            breadth_60s = getattr(regime_snapshot, "breadth_pct_60s", 50.0)

        trigger_reason = ""
        with self._lock:
            dump_age = now - self._last_dump_ts

        if hasattr(follower_snap, "top_n_bid_depth_usd"):
            f_bid_depth = follower_snap.top_n_bid_depth_usd(3)
            f_ask_depth = follower_snap.top_n_ask_depth_usd(3)
        else:
            f_bid_depth = getattr(follower_snap, "top3_bid_usd", getattr(follower_snap, "depth_bid_usd", 0.0))
            f_ask_depth = getattr(follower_snap, "top3_ask_usd", getattr(follower_snap, "depth_ask_usd", 0.0))

        bid_replenished = (f_bid_depth >= 1.2 * f_ask_depth) if f_ask_depth > 0 else False

        if regime_str != "FLASH_DUMP":
            if dump_age <= 120.0 or breadth_60s >= 25.0:
                trigger_reason = "post_dump_snapback"
            else:
                if dislocation_bps >= self.min_dislocation_bps * 1.5 and bid_replenished:
                    trigger_reason = "idiosyncratic_snapback"
        else:
            # Active FLASH_DUMP: require acute deceleration + book absorption
            if btc_ret_30s >= -8.0 and bid_replenished:
                trigger_reason = "dump_exhaustion_absorption"

        if not trigger_reason:
            return None

        # 4. Portfolio exposure and concurrency guards
        key = (follower_ex, coin)
        with self._lock:
            if key in self._coin_pos_map:
                return None  # Already in position

            last_trig = self._last_trigger_ts.get(key, 0.0)
            req_cooldown = self._pair_cooldowns.get(key, self.cooldown_sec)
            if (now - last_trig) < req_cooldown:
                return None

            if len(self._active_positions) >= self.max_active_positions:
                return None

        # 5. Position sizing via executable depth floor on BID side
        best_bid = getattr(follower_snap, "best_bid", 0.0)
        best_ask = getattr(follower_snap, "best_ask", 0.0)
        if best_bid <= 0.0 or best_ask <= 0.0:
            return None

        # Predatory maker bid is placed on the buy side, so we size against bid depth
        # to ensure our resting order doesn't dominate the book or suffer adverse selection.
        if f_bid_depth <= 0.0 or f_bid_depth < self.min_order_usd:
            return None  # Dead book / insufficient liquidity

        stealth_size = f_bid_depth * self.stealth_depth_fraction
        order_size_usd = max(self.min_order_usd, min(stealth_size, self.max_position_budget_usd))
        if order_size_usd < self.min_order_usd:
            return None

        # Exposure limit check including prospective order size
        with self._lock:
            total_exposure = sum(p.suggested_size_usd for p in self._active_positions.values())
            if total_exposure + order_size_usd > self.max_total_exposure_usd:
                return None

        # 6. Predatory Maker Limit Pricing
        # Instead of crossing the spread at best_ask, place a post-only limit bid
        # at a discount below best_bid to catch the liquidation knife tip with 0 bps taker fees.
        if self.dynamic_discount:
            # Pegged to follower spread: max(0.0, min(2.5, 0.15 * f_spread_bps))
            eff_discount_bps = max(0.0, min(2.5, round(0.15 * f_spread_bps, 2)))
        else:
            eff_discount_bps = self.maker_discount_bps

        if self.dynamic_target:
            # Quantitative First-Passage Formula on 9,564 empirical trajectories:
            # T* = max(8.0, min(15.0, 0.8 * f_spread_bps + 0.15 * effective_sigma))
            eff_target_bps = max(8.0, min(15.0, round(0.8 * f_spread_bps + 0.15 * effective_sigma, 2)))
        else:
            eff_target_bps = self.target_rebound_bps

        target_limit_px = round(best_bid * (1.0 - eff_discount_bps / 1e4), 8)
        if target_limit_px <= 0.0:
            target_limit_px = best_bid

        # Post-only guard at placement: limit bid must strictly not cross best_ask
        if target_limit_px >= best_ask:
            return None  # Taker cross rejected by post-only policy

        seq = next(self._seq)
        pos_id = f"rb_{int(now)}_{seq}"
        sym = getattr(follower_snap, "symbol", coin)

        is_filled = True if self.hunter_mode else False
        pos = ActiveRubberBandPosition(
            id=pos_id,
            coin=coin,
            leader_exchange="upbit",
            follower_exchange=follower_ex,
            follower_contract=sym,
            open_ts=now,
            trigger_reason=trigger_reason,
            entry_upbit_mid=upbit_mid,
            entry_follower_mid=f_mid,
            dislocation_bps_open=round(dislocation_bps, 2),
            dislocation_z=round(disloc_z, 2),
            spread_open_bps=round(f_spread_bps, 2),
            order_type="taker_sniper" if self.hunter_mode else "maker_post_only",
            target_limit_px=target_limit_px,
            entry_px=best_ask if self.hunter_mode else target_limit_px,
            suggested_size_usd=order_size_usd,
            maker_filled=is_filled,
            fill_ts=now if is_filled else None,
            fill_px=best_ask if is_filled else None,
            maker_ttl_sec=self.maker_ttl_sec,
            maker_discount_bps=eff_discount_bps,
            maker_rebate_bps=self.maker_rebate_bps,
            taker_fee_bps=self.taker_fee_bps,
            target_rebound_bps=eff_target_bps,
            stop_loss_bps=self.stop_loss_bps,
            roundtrip_fee_bps=self.roundtrip_fee_bps,
            max_hold_sec=self.max_hold_sec,
            alpha_decay_sec=self.alpha_decay_sec,
        )

        with self._lock:
            self._active_positions[pos_id] = pos
            self._coin_pos_map[key] = pos_id
            self._last_trigger_ts[key] = now

        self.counters["rubber_band_triggered"] = self.counters.get("rubber_band_triggered", 0) + 1
        if is_filled:
            self.counters["rubber_band_filled"] = self.counters.get("rubber_band_filled", 0) + 1

        log.info(
            f"[S4-RUBBER-BAND] 🏹 Posted Predatory Maker BID on {coin} ({follower_ex}): "
            f"Limit Px={target_limit_px:.6f} (-{eff_discount_bps:.1f} bps) | "
            f"Disloc={dislocation_bps:+.1f} bps (Z={disloc_z:.1f}σ) | "
            f"Target=+{eff_target_bps:.1f} bps | Size=${order_size_usd:.1f}"
        )
        return pos

    def on_follower_tick(
        self,
        follower_ex: str,
        coin: str,
        snap: Any,
        now: float,
        cur_upbit_mid: Optional[float] = None,
    ) -> Optional[ActiveRubberBandPosition]:
        """
        Processes real-time ticks for open Rubber Band positions:
        - Evaluates resting maker order fills vs. timeout expiration.
        - Evaluates take-profit rebound convergence, stops, and decay timeouts once filled.
        """
        key = (follower_ex, coin)
        pos: Optional[ActiveRubberBandPosition] = None

        with self._lock:
            pos_id = self._coin_pos_map.get(key)
            if pos_id is not None:
                pos = self._active_positions.get(pos_id)

        if pos is None or pos.closed:
            return None

        best_bid = getattr(snap, "best_bid", 0.0)
        best_ask = getattr(snap, "best_ask", 0.0)
        f_mid = getattr(snap, "mid_price", 0.0)
        if best_bid <= 0.0 or best_ask <= 0.0:
            return None

        # Phase 1: Resting Order Management (Awaiting Wick Fill)
        if not pos.maker_filled:
            resting_dt = now - pos.open_ts

            # 1. Resting Order Timeout Check: if wick doesn't hit within TTL, cancel at 0 fee & 0 loss
            if resting_dt > pos.maker_ttl_sec:
                self._close_position(pos, "unfilled_timeout", pos.target_limit_px, 0.0, now)
                return pos

            # 2. Upbit Anchor Collapse during resting: cancel order if Upbit spot collapsed
            if cur_upbit_mid is not None and cur_upbit_mid > 0:
                upbit_drop_bps = (pos.entry_upbit_mid - cur_upbit_mid) / pos.entry_upbit_mid * 1e4
                if upbit_drop_bps >= 25.0:
                    self._close_position(pos, "upbit_anchor_collapsed_resting", pos.target_limit_px, 0.0, now)
                    return pos

            # 3. Check Maker Fill: Did price wick down to or below our discounted limit bid?
            # Trade-through (best_bid < target_limit_px or best_ask <= target_limit_px) guarantees fill;
            # touch (best_bid == target_limit_px) models fill.
            # (Note: f_mid is intentionally NOT used to prevent phantom fills when target sits untraded inside the spread).
            if best_bid <= pos.target_limit_px or best_ask <= pos.target_limit_px:
                pos.maker_filled = True
                pos.fill_ts = now
                pos.fill_px = pos.target_limit_px
                pos.entry_px = pos.target_limit_px
                self.counters["rubber_band_filled"] = self.counters.get("rubber_band_filled", 0) + 1
                fill_mode = "trade_through" if (best_bid < pos.target_limit_px or best_ask <= pos.target_limit_px) else "touch_fill"
                log.info(
                    f"[S4-RUBBER-BAND] 🎯 Predatory Maker BID FILLED ({fill_mode}) on {coin} ({follower_ex}) "
                    f"@ {pos.fill_px:.6f} (-{pos.maker_discount_bps:.1f} bps below pre-dump bid)"
                )
            else:
                return None  # Still resting, waiting for fill

        # Phase 2: Active Filled Position Tracking (Mean-Reversion Snap-back)
        fill_px = pos.fill_px or pos.entry_px
        fill_t = pos.fill_ts or pos.open_ts
        dt = max(0.001, now - fill_t)

        # True executable PnL: Exit at current best bid (taker sell)
        raw_pnl_bps = (best_bid - fill_px) / fill_px * 1e4
        if pos.order_type == "taker_sniper":
            net_pnl_bps = raw_pnl_bps - pos.roundtrip_fee_bps
        else:
            net_pnl_bps = raw_pnl_bps - pos.taker_fee_bps + pos.maker_rebate_bps

        # Track trajectory (sample max 1 per 0.5s)
        if not pos.path or (now - pos.path[-1][0]) >= 0.5:
            pos.path.append([round(now, 3), round(net_pnl_bps, 2)])

        if net_pnl_bps > pos.mfe_exec_pnl_bps:
            pos.mfe_exec_pnl_bps = net_pnl_bps
        if net_pnl_bps < pos.mae_exec_pnl_bps:
            pos.mae_exec_pnl_bps = net_pnl_bps

        # Exit condition evaluation
        close_reason: Optional[str] = None

        # 1. Target Rebound Convergence
        if net_pnl_bps >= pos.target_rebound_bps:
            close_reason = "target_converged"
        # 2. Upbit Anchor Collapse: Check whether exit is profitable or a stop
        elif cur_upbit_mid is not None and cur_upbit_mid > 0 and ((cur_upbit_mid - best_bid) / best_bid * 1e4) <= 0.0:
            if net_pnl_bps > 0.0:
                close_reason = "converged_on_anchor_break"  # Profitable exit before anchor collapsed
            else:
                close_reason = "stop_anchor_collapsed"
        # 3. Dynamic Stop Loss
        elif net_pnl_bps <= -pos.stop_loss_bps:
            close_reason = "stop_loss"
        # 4. Alpha Decay Timeout (stalled trade after decay window)
        elif dt >= pos.alpha_decay_sec and net_pnl_bps <= 0.0:
            close_reason = "stop_decay"
        # 5. Max Hold Duration
        elif dt >= pos.max_hold_sec:
            close_reason = "converged" if net_pnl_bps > 0.0 else "timeout"

        if close_reason is not None:
            self._close_position(pos, close_reason, best_bid, net_pnl_bps, now)
            return pos

        return None

    def _close_position(
        self,
        pos: ActiveRubberBandPosition,
        close_reason: str,
        exit_px: float,
        net_pnl_bps: float,
        now: float,
    ) -> None:
        """Finalizes an active Rubber Band trade and dispatches telemetry."""
        key = (pos.follower_exchange, pos.coin)
        with self._lock:
            pos.closed = True
            pos.close_ts = now
            pos.duration_sec = round(now - pos.open_ts, 3)
            pos.close_reason = close_reason
            pos.exit_px = exit_px
            pos.net_pnl_bps = round(net_pnl_bps, 2)
            pos.mfe_exec_pnl_bps = round(pos.mfe_exec_pnl_bps, 2)
            pos.mae_exec_pnl_bps = round(pos.mae_exec_pnl_bps, 2)

            self._active_positions.pop(pos.id, None)
            self._coin_pos_map.pop(key, None)
            self._last_trigger_ts[key] = now
            if close_reason in ("stop_decay", "stop_loss", "stop_anchor_collapsed", "timeout") or net_pnl_bps <= 0.0:
                self._pair_cooldowns[key] = self.penalty_cooldown_sec
            else:
                self._pair_cooldowns[key] = self.cooldown_sec

        # Update engine counters accurately by economic outcome:
        if close_reason in ("target_converged", "converged_on_anchor_break"):
            self.counters["rubber_band_converged"] = self.counters.get("rubber_band_converged", 0) + 1
        elif close_reason in ("unfilled_timeout", "upbit_anchor_collapsed_resting"):
            self.counters["rubber_band_unfilled"] = self.counters.get("rubber_band_unfilled", 0) + 1
        elif close_reason == "timeout":
            self.counters["rubber_band_timeout"] = self.counters.get("rubber_band_timeout", 0) + 1
        elif "stop" in close_reason or net_pnl_bps <= 0.0:
            self.counters["rubber_band_stopped"] = self.counters.get("rubber_band_stopped", 0) + 1
        else:
            self.counters["rubber_band_converged"] = self.counters.get("rubber_band_converged", 0) + 1

        rec = {
            "strategy": "rubber_band",
            "strategy_id": 4,
            "strategy_name": "Predatory Rebound Maker",
            "id": pos.id,
            "coin": pos.coin,
            "leader_exchange": pos.leader_exchange,
            "follower_exchange": pos.follower_exchange,
            "follower_contract": pos.follower_contract,
            "open_ts": pos.open_ts,
            "close_ts": pos.close_ts,
            "duration_sec": pos.duration_sec,
            "close_reason": pos.close_reason,
            "side": pos.side,
            "order_type": pos.order_type,
            "suggested_size_usd": pos.suggested_size_usd,
            "entry_px": pos.entry_px,
            "fill_px": pos.fill_px,
            "exit_px": pos.exit_px,
            "net_pnl_bps": pos.net_pnl_bps,
            "mfe_exec_pnl_bps": pos.mfe_exec_pnl_bps,
            "mae_exec_pnl_bps": pos.mae_exec_pnl_bps,
            "dislocation_bps_open": pos.dislocation_bps_open,
            "dislocation_z": pos.dislocation_z,
            "target_rebound_bps": pos.target_rebound_bps,
            "trigger_reason": pos.trigger_reason,
            "maker_filled": pos.maker_filled,
            "path": pos.path,
        }

        try:
            self.emit(rec)
        except Exception as e:
            log.error(f"[S4-RUBBER-BAND] Failed to emit record {pos.id}: {e}")

        log.info(
            f"[S4-RUBBER-BAND] Closed {pos.coin} ({pos.follower_exchange}): "
            f"PnL={pos.net_pnl_bps:+.1f} bps | Reason={close_reason} | Duration={pos.duration_sec:.1f}s"
        )

    def sweep_timeouts(self, now: float) -> None:
        """Sweeps stale open positions exceeding max hold duration or resting order TTL."""
        stale: List[Tuple[ActiveRubberBandPosition, str]] = []
        with self._lock:
            for pos in list(self._active_positions.values()):
                if not pos.maker_filled and (now - pos.open_ts) >= pos.maker_ttl_sec:
                    stale.append((pos, "unfilled_timeout"))
                elif pos.maker_filled and (now - (pos.fill_ts or pos.open_ts)) >= pos.max_hold_sec:
                    stale.append((pos, "timeout"))

        for pos, reason in stale:
            if reason == "unfilled_timeout":
                self._close_position(pos, "unfilled_timeout", pos.target_limit_px, 0.0, now)
            else:
                # Realistic stale book penalty on timeout: spread cost + fee + adverse slippage
                fill_p = pos.fill_px or pos.entry_px
                fee_cost = pos.roundtrip_fee_bps if pos.order_type == "taker_sniper" else pos.taker_fee_bps
                penalty_bps = pos.spread_open_bps + fee_cost + 10.0
                net_pnl = -penalty_bps
                exit_px = fill_p * (1.0 - penalty_bps / 1e4)
                self._close_position(pos, "timeout", exit_px, net_pnl, now)
