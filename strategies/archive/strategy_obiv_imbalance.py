"""
strategies/strategy_5_imbalance.py - Strategy 5: Orderbook Imbalance Front-Runner (OBIV).

Microstructure Simulation:
Before a retail market sweep (Tsunami) occurs on Upbit, informed traders, local market-makers,
and accumulation algorithms heavily stack limit bids in the top 5 levels of the Upbit KRW order book.

Orderbook Imbalance:
    Imbalance = (Depth_Bid_Top5 - Depth_Ask_Top5) / (Depth_Bid_Top5 + Depth_Ask_Top5)

Orderbook Imbalance Velocity (OBIV):
    Delta_Imbalance_5s = Imbalance(t) - Imbalance(t - 5s)

When:
    1. Upbit Imbalance surges to >= +0.60 (bids exceed asks by > 4:1)
    2. Delta_Imbalance over last 5 seconds >= +0.35 (rapid aggressive liquidity injection)
    3. Follower perpetual book (Binance/Bybit/OKX) is still dormant/neutral (Follower Imbalance <= +0.20)

This module front-runs the anticipated Upbit market sweep by securing early long positions on
the follower perpetual contract before the blowout occurs.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import itertools
import logging
import threading
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

log = logging.getLogger("strategy.imbalance")


@dataclass
class ActiveImbalancePosition:
    id: str
    coin: str
    leader_exchange: str
    follower_exchange: str
    follower_contract: str
    open_ts: float

    # Pricing & Metrics at trigger
    entry_upbit_mid: float
    entry_follower_mid: float
    upbit_imbalance_at_open: float
    delta_imbalance_5s: float
    entry_px: float                   # Executable entry ask price
    suggested_size_usd: float
    spread_open_bps: float = 0.0
    side: str = "long"                # Imbalance front-running is directional long

    # Targets & Risk Parameters
    target_pnl_bps: float = 25.0
    stop_loss_bps: float = 18.0
    roundtrip_fee_bps: float = 10.0
    max_hold_sec: float = 15.0
    alpha_decay_sec: float = 8.0

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


class OrderbookImbalanceStrategy:
    """
    Quantitative execution engine for Strategy 5: Orderbook Imbalance Front-Runner.
    Monitors high-frequency depth skew and bid stacking on Upbit to front-run follower perps.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        min_order_usd: float = 10.0,
        max_position_budget_usd: float = 500.0,
        stealth_depth_fraction: float = 0.15,
        min_upbit_imbalance: float = 0.60,
        min_delta_imbalance_5s: float = 0.35,
        max_follower_imbalance: float = 0.25,
        target_pnl_bps: float = 25.0,
        stop_loss_bps: float = 18.0,
        roundtrip_fee_bps: float = 10.0,
        max_hold_sec: float = 15.0,
        alpha_decay_sec: float = 8.0,
        cooldown_sec: float = 30.0,
        max_active_positions: int = 10,
        max_total_exposure_usd: float = 5000.0,
        enabled: bool = False,         # Disabled by default until user verification
        counters: Optional[Dict[str, int]] = None,
    ):
        self.emit = emit_callback
        self.min_order_usd = min_order_usd
        self.max_position_budget_usd = max_position_budget_usd
        self.stealth_depth_fraction = stealth_depth_fraction
        self.min_upbit_imbalance = min_upbit_imbalance
        self.min_delta_imbalance_5s = min_delta_imbalance_5s
        self.max_follower_imbalance = max_follower_imbalance
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

        # Imbalance history buffer: coin -> deque[(timestamp, imbalance)]
        self._upbit_imbalance_history: Dict[str, Deque[Tuple[float, float]]] = {}

        # Active positions: id -> ActiveImbalancePosition
        self._active_positions: Dict[str, ActiveImbalancePosition] = {}
        # Quick lookup: (follower_ex, coin) -> id
        self._coin_pos_map: Dict[Tuple[str, str], str] = {}
        # Cooldown: (follower_ex, coin) -> last_trigger_ts
        self._last_trigger_ts: Dict[Tuple[str, str], float] = {}

    def on_upbit_book_tick(self, coin: str, snap: Any, now: float) -> None:
        """Records Upbit orderbook imbalance snapshots to track velocity (OBIV)."""
        imbalance = getattr(snap, "imbalance_top5", 0.0)
        c = coin.upper()
        with self._lock:
            buf = self._upbit_imbalance_history.get(c)
            if buf is None:
                buf = self._upbit_imbalance_history[c] = deque(maxlen=60)
            buf.append((now, imbalance))

            # Prune samples older than 20s
            cutoff = now - 20.0
            while buf and buf[0][0] < cutoff:
                buf.popleft()

    def _calc_delta_imbalance(self, coin: str, now: float, window_sec: float = 5.0) -> Tuple[float, float]:
        """Calculates current imbalance and delta over window_sec for Upbit."""
        buf = self._upbit_imbalance_history.get(coin.upper())
        if not buf or len(buf) < 2:
            return 0.0, 0.0
        # Freshness guard: Reject stale Upbit imbalance older than 1.5s
        if (now - buf[-1][0]) > 1.5:
            return 0.0, 0.0
        # Window coverage guard: Ensure buffer actually spans back to at least (now - window_sec + 0.5)
        if buf[0][0] > (now - window_sec + 0.5):
            return 0.0, 0.0

        cur_imb = buf[-1][1]
        target_t = now - window_sec
        past_imb = buf[0][1]
        for t, imb in buf:
            if t <= target_t:
                past_imb = imb
            else:
                break
        return cur_imb, (cur_imb - past_imb)

    def check_imbalance_opportunity(
        self,
        follower_ex: str,
        coin: str,
        follower_snap: Any,
        upbit_snap: Optional[Any],
        now: float,
        market_regime: str = "NEUTRAL",
        recv_ts: float = 0.0,
    ) -> Optional[ActiveImbalancePosition]:
        """Evaluates whether pre-sweep bid imbalance warrants front-running the follower perp."""
        if not self.enabled:
            return None

        # Hard veto: Do not front-run longs during active FLASH_DUMP
        if market_regime == "FLASH_DUMP":
            return None

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
            return None

        # 1. Evaluate Upbit Imbalance Velocity (OBIV)
        with self._lock:
            cur_imb, delta_imb = self._calc_delta_imbalance(coin, now, window_sec=5.0)

        # Must satisfy high positive imbalance and aggressive rate of change
        if cur_imb < self.min_upbit_imbalance or delta_imb < self.min_delta_imbalance_5s:
            return None

        # 2. Verify Follower perp is still dormant (not already blown out)
        f_imbalance = getattr(follower_snap, "imbalance_top3", getattr(follower_snap, "imbalance_top5", 0.0))
        if f_imbalance > self.max_follower_imbalance:
            return None  # Follower has already reacted / blown out

        # 3. Portfolio exposure and concurrency guards
        key = (follower_ex, coin)
        with self._lock:
            if key in self._coin_pos_map:
                return None

            last_trig = self._last_trigger_ts.get(key, 0.0)
            if (now - last_trig) < self.cooldown_sec:
                return None

            if len(self._active_positions) >= self.max_active_positions:
                return None

        # 4. Sizing via follower executable ask depth & liquidity floor
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

        # Portfolio exposure check including prospective order size
        with self._lock:
            total_exposure = sum(p.suggested_size_usd for p in self._active_positions.values())
            if total_exposure + order_size_usd > self.max_total_exposure_usd:
                return None

        seq = next(self._seq)
        pos_id = f"imb_{int(now)}_{seq}"
        sym = getattr(follower_snap, "symbol", coin)

        pos = ActiveImbalancePosition(
            id=pos_id,
            coin=coin,
            leader_exchange="upbit",
            follower_exchange=follower_ex,
            follower_contract=sym,
            open_ts=now,
            entry_upbit_mid=upbit_mid,
            entry_follower_mid=f_mid,
            upbit_imbalance_at_open=round(cur_imb, 3),
            delta_imbalance_5s=round(delta_imb, 3),
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

        self.counters["imbalance_triggered"] = self.counters.get("imbalance_triggered", 0) + 1
        log.info(
            f"[S5-IMBALANCE] ⚡ Front-running LONG on {coin} ({follower_ex}): "
            f"Upbit Imb={cur_imb:+.2f} (Δ5s={delta_imb:+.2f}) | Foll Imb={f_imbalance:+.2f} | "
            f"Target=+{self.target_pnl_bps:.1f} bps | Size=${order_size_usd:.1f}"
        )
        return pos

    def on_follower_tick(
        self,
        follower_ex: str,
        coin: str,
        snap: Any,
        now: float,
    ) -> Optional[ActiveImbalancePosition]:
        """Monitors open imbalance positions for target convergence, stop loss, and decay."""
        key = (follower_ex, coin)
        pos: Optional[ActiveImbalancePosition] = None

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

        # 1. Target Convergence
        if net_pnl_bps >= pos.target_pnl_bps:
            close_reason = "target_converged"
        # 2. Dynamic Stop Loss
        elif net_pnl_bps <= -(pos.spread_open_bps * 0.5 + pos.roundtrip_fee_bps + pos.stop_loss_bps):
            close_reason = "stop_loss"
        # 3. Alpha Decay (if no sweep occurs within decay window)
        elif dt >= pos.alpha_decay_sec and net_pnl_bps <= 0.0:
            close_reason = "stop_decay"
        # 4. Max Hold Timeout
        elif dt >= pos.max_hold_sec:
            close_reason = "timeout"

        if close_reason is not None:
            self._close_position(pos, close_reason, best_bid, net_pnl_bps, now)
            return pos

        return None

    def _close_position(
        self,
        pos: ActiveImbalancePosition,
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
            self.counters["imbalance_converged"] = self.counters.get("imbalance_converged", 0) + 1
        elif "stop" in close_reason:
            self.counters["imbalance_stopped"] = self.counters.get("imbalance_stopped", 0) + 1
        elif close_reason == "timeout":
            self.counters["imbalance_timeout"] = self.counters.get("imbalance_timeout", 0) + 1

        rec = {
            "strategy": "imbalance_front_runner",
            "strategy_id": 5,
            "strategy_name": "Orderbook Imbalance Front-Runner",
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
            "suggested_size_usd": pos.suggested_size_usd,
            "entry_px": pos.entry_px,
            "exit_px": pos.exit_px,
            "net_pnl_bps": pos.net_pnl_bps,
            "mfe_exec_pnl_bps": pos.mfe_exec_pnl_bps,
            "mae_exec_pnl_bps": pos.mae_exec_pnl_bps,
            "upbit_imbalance_at_open": pos.upbit_imbalance_at_open,
            "delta_imbalance_5s": pos.delta_imbalance_5s,
            "target_pnl_bps": pos.target_pnl_bps,
            "path": pos.path,
        }

        try:
            self.emit(rec)
        except Exception as e:
            log.error(f"[S5-IMBALANCE] Failed to emit record {pos.id}: {e}")

        log.info(
            f"[S5-IMBALANCE] Closed {pos.coin} ({pos.follower_exchange}): "
            f"PnL={pos.net_pnl_bps:+.1f} bps | Reason={close_reason} | Duration={pos.duration_sec:.1f}s"
        )

    def sweep_timeouts(self, now: float) -> None:
        stale: List[ActiveImbalancePosition] = []
        with self._lock:
            for pos in list(self._active_positions.values()):
                if (now - pos.open_ts) >= pos.max_hold_sec:
                    stale.append(pos)

        for pos in stale:
            raw_pnl = 0.0
            net_pnl = -pos.roundtrip_fee_bps
            self._close_position(pos, "timeout", pos.entry_px, net_pnl, now)
