"""
strategies/strategy_5_cross_korean.py - Strategy 5: Cross-Korean Triangle (Bithumb Radar).

Microstructure Simulation:
Bithumb often experiences lower maker/taker fee structures and idiosyncratic speculative bursts
that lead Upbit by 300 - 1,200 ms on domestic Korean microcaps / altcoins.

Lead-Lag Cascade:
    Stage 1: Bithumb retail burst -> Bithumb mid surges above Upbit (+25..+50 bps)
             Confirmed by REAL trade tape execution (recent_bithumb_buys_usd >= $2,000)
    Stage 2: Upbit arbitrageurs & retail see the move and sweep Upbit
    Stage 3: International follower perps (Binance/Bybit/OKX) react and re-price upward

Guards against Bithumb ghost quotes:
1. Real trade tape confirmation: >= $2,000 executed buy volume in the last 2 seconds.
2. Volume-share whitelist: Bithumb 24h volume must be >= 30% of Upbit volume.
3. Major coin exclusion: BTC, ETH, SOL, XRP, DOGE are excluded (Bithumb is purely an arbitrage sink on majors).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import itertools
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("strategy.cross_korean")

DEFAULT_EXCLUDED_MAJORS: Set[str] = {"BTC", "ETH", "SOL", "XRP", "DOGE"}


@dataclass
class ActiveCrossKoreanPosition:
    id: str
    coin: str
    bithumb_exchange: str
    follower_exchange: str
    follower_contract: str
    open_ts: float

    # Pricing & Dislocation at trigger
    bithumb_mid: float
    upbit_mid: float
    follower_mid: float
    bithumb_upbit_spread_bps: float
    entry_px: float                   # Executable Ask for Follower
    suggested_size_usd: float
    spread_open_bps: float = 0.0
    side: str = "long"

    # Targets & Risk Parameters
    target_pnl_bps: float = 30.0
    stop_loss_bps: float = 20.0
    roundtrip_fee_bps: float = 10.0
    max_hold_sec: float = 20.0
    alpha_decay_sec: float = 10.0

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


class CrossKoreanTriangleStrategy:
    """
    Quantitative execution engine for Strategy 5: Cross-Korean Triangle.
    Uses early Bithumb trade tape bursts to front-run Upbit and international followers on qualifying altcoins.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        min_order_usd: float = 10.0,
        max_position_budget_usd: float = 500.0,
        stealth_depth_fraction: float = 0.15,
        min_bithumb_lead_bps: float = 25.0,
        min_bithumb_buys_usd: float = 2000.0,  # Minimum executed buy volume on Bithumb tape to reject ghost quotes
        min_volume_share: float = 0.30,       # Minimum Bithumb volume / Upbit volume share
        min_upbit_confirm_bps: float = 0.0,   # Optional Upbit momentum confirmation (e.g. +3 bps)
        excluded_coins: Optional[Set[str]] = None,
        target_pnl_bps: float = 30.0,
        stop_loss_bps: float = 20.0,
        roundtrip_fee_bps: float = 10.0,
        max_hold_sec: float = 20.0,
        alpha_decay_sec: float = 10.0,
        cooldown_sec: float = 30.0,
        max_active_positions: int = 5,
        max_total_exposure_usd: float = 3000.0,
        enabled: bool = False,                 # Disabled by default until user verification
        counters: Optional[Dict[str, int]] = None,
    ):
        self.emit = emit_callback
        self.min_order_usd = min_order_usd
        self.max_position_budget_usd = max_position_budget_usd
        self.stealth_depth_fraction = stealth_depth_fraction
        self.min_bithumb_lead_bps = min_bithumb_lead_bps
        self.min_bithumb_buys_usd = min_bithumb_buys_usd
        self.min_volume_share = min_volume_share
        self.min_upbit_confirm_bps = min_upbit_confirm_bps
        self.excluded_coins = excluded_coins if excluded_coins is not None else set(DEFAULT_EXCLUDED_MAJORS)
        self.target_pnl_bps = target_pnl_bps
        self.stop_loss_bps = stop_loss_bps
        self.roundtrip_fee_bps = roundtrip_fee_bps
        self.max_hold_sec = max_hold_sec
        self.alpha_decay_sec = alpha_decay_sec
        self.cooldown_sec = cooldown_sec
        self.max_active_positions = max_active_positions
        self.max_total_exposure_usd = max_total_exposure_usd
        self.enabled = enabled
        self.counters = counters if counters is not None else {}

        self._lock = threading.Lock()
        self._seq = itertools.count(1)

        # Active positions: id -> ActiveCrossKoreanPosition
        self._active_positions: Dict[str, ActiveCrossKoreanPosition] = {}
        # Quick lookup: (follower_ex, coin) -> id
        self._coin_pos_map: Dict[Tuple[str, str], str] = {}
        # Cooldown: (follower_ex, coin) -> last_trigger_ts
        self._last_trigger_ts: Dict[Tuple[str, str], float] = {}

    def check_triangle_opportunity(
        self,
        follower_ex: str,
        coin: str,
        bithumb_snap: Optional[Any],
        upbit_snap: Optional[Any],
        follower_snap: Optional[Any],
        now: float,
        market_regime: str = "NEUTRAL",
        recent_bithumb_buys_usd: float = 0.0,
        recv_ts: float = 0.0,
        bithumb_volume_share: Optional[float] = None,
        upbit_ret_2s_bps: float = 0.0,
        recent_upbit_buys_usd: float = 0.0,
    ) -> Optional[ActiveCrossKoreanPosition]:
        """Evaluates whether Bithumb is outrunning Upbit and follower with tape confirmation."""
        if not self.enabled:
            return None

        # 1. Exclude major coins where Bithumb cannot sustainably lead Upbit
        if coin in self.excluded_coins:
            return None

        if market_regime == "FLASH_DUMP":
            return None

        # 2. Volume-share filter: Bithumb must have sufficient relative liquidity
        if bithumb_volume_share is not None and bithumb_volume_share < self.min_volume_share:
            return None

        # 3. Real Tape Confirmation guard: Bithumb must have actual executed buy volume
        if self.min_bithumb_buys_usd > 0.0 and recent_bithumb_buys_usd < self.min_bithumb_buys_usd:
            return None

        # 4. Middle leg transmission check: Upbit must either show upward momentum or recent buy tape
        # to verify that the Bithumb burst is propagating to Upbit rather than being a dead-end fakeout.
        if self.min_upbit_confirm_bps > 0.0:
            if upbit_ret_2s_bps < self.min_upbit_confirm_bps and recent_upbit_buys_usd < 50.0:
                return None

        if bithumb_snap is None or upbit_snap is None or follower_snap is None:
            return None

        f_age = now - recv_ts if recv_ts > 0 else 0.0
        if f_age > 1.5:
            return None

        bit_mid = getattr(bithumb_snap, "mid_price", 0.0)
        upbit_mid = getattr(upbit_snap, "mid_price", 0.0)
        f_mid = getattr(follower_snap, "mid_price", 0.0)
        if bit_mid <= 0.0 or upbit_mid <= 0.0 or f_mid <= 0.0:
            return None

        f_spread_bps = getattr(follower_snap, "spread_bps", 999.0)
        if f_spread_bps > 30.0:
            return None

        # Cross-Korean Lead: Bithumb must be trading higher than Upbit
        bit_upbit_diff_bps = (bit_mid - upbit_mid) / upbit_mid * 1e4
        if bit_upbit_diff_bps < self.min_bithumb_lead_bps:
            return None

        # Follower must still lag behind Bithumb
        f_lag_bps = (bit_mid - f_mid) / f_mid * 1e4
        if f_lag_bps < (self.min_bithumb_lead_bps + self.roundtrip_fee_bps):
            return None

        key = (follower_ex, coin)
        with self._lock:
            if key in self._coin_pos_map:
                return None

            last_trig = self._last_trigger_ts.get(key, 0.0)
            if (now - last_trig) < self.cooldown_sec:
                return None

            if len(self._active_positions) >= self.max_active_positions:
                return None

        best_ask = getattr(follower_snap, "best_ask", 0.0)
        if best_ask <= 0.0:
            return None

        f_ask_depth = getattr(follower_snap, "top3_ask_usd", 0.0)
        if hasattr(follower_snap, "top_n_ask_depth_usd"):
            f_ask_depth = follower_snap.top_n_ask_depth_usd(3)

        if f_ask_depth <= 0.0 or f_ask_depth < self.min_order_usd:
            return None  # Dead book / insufficient liquidity

        stealth_size = f_ask_depth * self.stealth_depth_fraction
        order_size_usd = max(self.min_order_usd, min(stealth_size, self.max_position_budget_usd))
        if order_size_usd < self.min_order_usd:
            return None

        # Exposure limit check including prospective order size
        with self._lock:
            total_exp = sum(p.suggested_size_usd for p in self._active_positions.values())
            if total_exp + order_size_usd > self.max_total_exposure_usd:
                return None

        seq = next(self._seq)
        pos_id = f"tri_{int(now)}_{seq}"
        sym = getattr(follower_snap, "symbol", coin)

        pos = ActiveCrossKoreanPosition(
            id=pos_id,
            coin=coin,
            bithumb_exchange="bithumb",
            follower_exchange=follower_ex,
            follower_contract=sym,
            open_ts=now,
            bithumb_mid=bit_mid,
            upbit_mid=upbit_mid,
            follower_mid=f_mid,
            bithumb_upbit_spread_bps=round(bit_upbit_diff_bps, 2),
            entry_px=best_ask,
            suggested_size_usd=order_size_usd,
            spread_open_bps=f_spread_bps,
            target_pnl_bps=self.target_pnl_bps,
            stop_loss_bps=self.stop_loss_bps,
            roundtrip_fee_bps=self.roundtrip_fee_bps,
            max_hold_sec=self.max_hold_sec,
            alpha_decay_sec=self.alpha_decay_sec,
        )

        with self._lock:
            self._active_positions[pos_id] = pos
            self._coin_pos_map[key] = pos_id
            self._last_trigger_ts[key] = now

        self.counters["triangle_triggered"] = self.counters.get("triangle_triggered", 0) + 1
        log.info(
            f"[S5-TRIANGLE] 🔺 Bithumb early radar LONG on {coin} ({follower_ex}): "
            f"Bithumb-Upbit={bit_upbit_diff_bps:+.1f} bps | Target=+{self.target_pnl_bps:.1f} bps | "
            f"Size=${order_size_usd:.1f}"
        )
        return pos

    def on_follower_tick(
        self,
        follower_ex: str,
        coin: str,
        snap: Any,
        now: float,
    ) -> Optional[ActiveCrossKoreanPosition]:
        key = (follower_ex, coin)
        pos: Optional[ActiveCrossKoreanPosition] = None

        with self._lock:
            pos_id = self._coin_pos_map.get(key)
            if pos_id is not None:
                pos = self._active_positions.get(pos_id)

        if pos is None or pos.closed:
            return None

        best_bid = getattr(snap, "best_bid", 0.0)
        if best_bid <= 0.0:
            return None

        dt = max(0.001, now - pos.open_ts)
        raw_pnl_bps = (best_bid - pos.entry_px) / pos.entry_px * 1e4
        net_pnl_bps = raw_pnl_bps - pos.roundtrip_fee_bps

        if not pos.path or (now - pos.path[-1][0]) >= 0.5:
            pos.path.append([round(now, 3), round(net_pnl_bps, 2)])

        if net_pnl_bps > pos.mfe_exec_pnl_bps:
            pos.mfe_exec_pnl_bps = net_pnl_bps
        if net_pnl_bps < pos.mae_exec_pnl_bps:
            pos.mae_exec_pnl_bps = net_pnl_bps

        close_reason: Optional[str] = None
        if net_pnl_bps >= pos.target_pnl_bps:
            close_reason = "target_converged"
        elif net_pnl_bps <= -(pos.spread_open_bps * 0.5 + pos.roundtrip_fee_bps + pos.stop_loss_bps):
            close_reason = "stop_loss"
        elif dt >= pos.alpha_decay_sec and net_pnl_bps <= 0.0:
            close_reason = "stop_decay"
        elif dt >= pos.max_hold_sec:
            close_reason = "timeout"

        if close_reason is not None:
            self._close_position(pos, close_reason, best_bid, net_pnl_bps, now)
            return pos

        return None

    def _close_position(
        self,
        pos: ActiveCrossKoreanPosition,
        close_reason: str,
        exit_px: float,
        net_pnl_bps: float,
        now: float,
    ) -> None:
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

        if close_reason == "target_converged":
            self.counters["triangle_converged"] = self.counters.get("triangle_converged", 0) + 1
        elif "stop" in close_reason:
            self.counters["triangle_stopped"] = self.counters.get("triangle_stopped", 0) + 1
        elif close_reason == "timeout":
            self.counters["triangle_timeout"] = self.counters.get("triangle_timeout", 0) + 1

        rec = {
            "strategy": "cross_korean_triangle",
            "strategy_id": 5,
            "strategy_name": "Cross-Korean Triangle (Bithumb -> Upbit -> Follower)",
            "id": pos.id,
            "coin": pos.coin,
            "leader_exchange": "bithumb",
            "co_leader_exchange": "upbit",
            "follower_exchange": pos.follower_exchange,
            "follower_contract": pos.follower_contract,
            "open_ts": pos.open_ts,
            "close_ts": pos.close_ts,
            "duration_sec": pos.duration_sec,
            "close_reason": pos.close_reason,
            "side": pos.side,
            "suggested_size_usd": pos.suggested_size_usd,
            "entry_px": pos.entry_px,
            "exit_px": pos.exit_px,
            "net_pnl_bps": pos.net_pnl_bps,
            "mfe_exec_pnl_bps": pos.mfe_exec_pnl_bps,
            "mae_exec_pnl_bps": pos.mae_exec_pnl_bps,
            "bithumb_upbit_spread_bps": pos.bithumb_upbit_spread_bps,
            "target_pnl_bps": pos.target_pnl_bps,
            "path": pos.path,
        }

        try:
            self.emit(rec)
        except Exception as e:
            log.error(f"[S5-TRIANGLE] Failed to emit record {pos.id}: {e}")

        log.info(
            f"[S5-TRIANGLE] Closed {pos.coin} ({pos.follower_exchange}): "
            f"PnL={pos.net_pnl_bps:+.1f} bps | Reason={close_reason} | Duration={pos.duration_sec:.1f}s"
        )

    def sweep_timeouts(self, now: float) -> None:
        stale: List[ActiveCrossKoreanPosition] = []
        with self._lock:
            for pos in list(self._active_positions.values()):
                if (now - pos.open_ts) >= pos.max_hold_sec:
                    stale.append(pos)

        for pos in stale:
            # Realistic stale book penalty on timeout: spread cost + fee + adverse slippage
            penalty_bps = pos.spread_open_bps + pos.roundtrip_fee_bps + 10.0
            net_pnl = -penalty_bps
            exit_px = pos.entry_px * (1.0 - penalty_bps / 1e4)
            self._close_position(pos, "timeout", exit_px, net_pnl, now)
