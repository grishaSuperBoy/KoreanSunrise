"""
strategies/strategy_2_fade_fakeout.py - Strategy 2: Fade the Fakeout / Pre-Sweep Sniper.

Microstructure Simulation:
On illiquid alts and meme tokens, micro-sweeps (< $3,500 swept or MM quote pulls/cancels)
frequently fail to sustain follow-through momentum.
This paper-trading telemetry module simulates hypothetical position tracking against local
order book depth to measure transient dislocation and mean-reversion characteristics.
(No live orders are placed; simulated fills are evaluated via top-of-book VWAP).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import itertools
import logging
import math
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("strategy.fade")

# Heavy institutional majors where MMs cancel quotes rapidly at high frequency
# (Filtered out in inverted pre-sweep long mode to prevent quote-jitter spam)
JITTER_MAJORS: Set[str] = {
    "BTC", "ETH", "SOL", "DOT", "FIL", "TAO", "LIT", "RENDER",
    "AAVE", "UNI", "INJ", "PENDLE", "JTO", "ENS", "GMT", "AXL", "DATA", "ICP", "ZRO"
}


@dataclass
class ActiveFadePosition:
    id: str
    coin: str
    leader_exchange: str
    follower_exchange: str
    follower_contract: str
    open_ts: float
    fade_reason: str
    trigger_type: str
    swept_usd: float
    upbit_spread_bps: float

    # Pricing
    entry_upbit_mid: float
    entry_follower_mid: float
    entry_px: float                   # Executable entry price (VWAP ask for long, VWAP bid for short)
    suggested_size_usd: float
    side: str = "short"               # "short" (classic fade) or "long" (inverted pre-sweep sniper)

    # Parameters
    max_hold_sec: float = 7.5
    stop_invalidation_bps: float = 20.0
    target_fade_bps: float = 12.0
    roundtrip_fee_bps: float = 10.0

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


class FadeFakeoutStrategy:
    """
    Simulates and tracks hypothetical positions fading unconfirmed sweeps or sniping early momentum.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        roundtrip_fee_bps: float = 10.0,
        min_order_usd: float = 10.0,
        max_position_budget_usd: float = 500.0,
        stealth_depth_fraction: float = 0.15,
        max_hold_sec: float = 6.0,
        max_follower_book_age_sec: float = 1.5,
        stop_invalidation_bps: float = 30.0,
        target_fade_bps: float = 15.0,
        min_upbit_spread_bps: float = 35.0,
        max_fade_swept_usd: float = 2000.0,
        max_follower_depth_usd: float = 3500.0,
        cooldown_sec: float = 30.0,
        invert_to_long: bool = False,
        max_active_positions: int = 10,
        max_total_exposure_usd: float = 5000.0,
        counters: Optional[Dict[str, int]] = None,
    ):
        self.emit = emit_callback
        self.roundtrip_fee_bps = roundtrip_fee_bps
        self.min_order_usd = min_order_usd
        self.max_position_budget_usd = max_position_budget_usd
        self.stealth_depth_fraction = stealth_depth_fraction
        self.max_hold_sec = max_hold_sec
        self.max_follower_book_age_sec = max_follower_book_age_sec
        self.stop_invalidation_bps = stop_invalidation_bps
        self.target_fade_bps = target_fade_bps
        self.min_upbit_spread_bps = min_upbit_spread_bps
        self.max_fade_swept_usd = max_fade_swept_usd
        self.max_follower_depth_usd = max_follower_depth_usd
        self.cooldown_sec = cooldown_sec
        self.invert_to_long = invert_to_long
        self.max_active_positions = max_active_positions
        self.max_total_exposure_usd = max_total_exposure_usd

        self._active: Dict[str, List[ActiveFadePosition]] = {}
        self._last_fade_ts: Dict[Tuple[str, str], float] = {}
        self._seq = itertools.count(1)
        self._lock = threading.RLock()
        default_counters = {
            "fade_signals_triggered": 0,
            "fade_signals_converged": 0,
            "fade_signals_stopped": 0,
            "fade_signals_timeout": 0,
        }
        if counters is not None:
            for k, v in default_counters.items():
                counters.setdefault(k, v)
            self.counters = counters
        else:
            self.counters = default_counters

    def is_fakeout_pattern(
        self,
        is_cancel_blocked: bool,
        swept_usd: float,
        upbit_spread_bps: float,         
        korean_confirmed: bool = False,
    ) -> Tuple[bool, str]:
        """
        Detects if an observed Upbit sweep event fits the fakeout pattern.
        Microstructure Rules:
        1. Swept USD must be a micro-sweep (<= max_fade_swept_usd). NEVER fade large sweeps!
        2. Upbit spread must be wide (>= min_upbit_spread_bps), indicating an illiquid gap.
        3. Separates true MM quote cancellations from retail exhaustion.
        4. Cross-Korean Confirmation Guard: If korean_confirmed is True (both Upbit & Bithumb
           are surging together), NEVER fade — it is a genuine dual-exchange Korean pump.
        """
        # Hard gate: cross-Korean dual pump. Never short when both Korean venues are pumping!
        if korean_confirmed:
            return False, ""

        # Hard gate: volume ceiling. Large sweeps are momentum, not fakeouts.
        if swept_usd > self.max_fade_swept_usd or swept_usd <= 0.0:
            return False, ""

        # Hard gate: spread floor. Tight spreads indicate liquid majors, not illiquid alts.
        if upbit_spread_bps < self.min_upbit_spread_bps:
            return False, ""

        if is_cancel_blocked:
            return True, "mm_quote_cancel"
        return True, "micro_sweep_exhaustion"

    def on_fakeout_detected(
        self,
        coin: str,
        trigger_type: str,
        swept_usd: float,
        upbit_snap: Any,
        follower_snaps: Dict[str, Any],
        now: float,
        fade_reason: str,
        is_stable_pair_fn: Optional[Callable[[str, str, float], bool]] = None,
        recv_ts_map: Optional[Dict[Tuple[str, str], float]] = None,
    ) -> List[ActiveFadePosition]:
        """
        Opens short positions fading the fakeout across contemporaneous follower books.
        Guards against stale quotes and dislocated baselines.
        """
        with self._lock:
            # Jitter Guard: in inverted long mode, reject high-frequency majors that generate noise
            if self.invert_to_long and coin.upper() in JITTER_MAJORS:
                return []

            # 0. Portfolio Exposure Guard: cap total active positions and exposure
            current_active = sum(len([p for p in plist if not p.closed]) for plist in self._active.values())
            if current_active >= self.max_active_positions:
                return []
            current_exposure = sum(sum(p.suggested_size_usd for p in plist if not p.closed) for plist in self._active.values())
            if current_exposure >= self.max_total_exposure_usd:
                return []

            opened: List[ActiveFadePosition] = []
            upbit_mid = float(getattr(upbit_snap, "mid_price", 0.0) or 0.0)
            upbit_spread = float(getattr(upbit_snap, "spread_bps", 0.0) or 0.0)

            for follower_ex, f_snap in follower_snaps.items():
                if follower_ex == "upbit" or f_snap is None:
                    continue

                if current_active + len(opened) >= self.max_active_positions:
                    break

                # 1. Cooldown Guard: at least cooldown_sec between fades on same (coin, follower_ex)
                pair_key = (coin, follower_ex)
                if (now - self._last_fade_ts.get(pair_key, 0.0)) < self.cooldown_sec:
                    continue

                # 2. Staleness Guard: book must be fresh (< max_follower_book_age_sec)
                if recv_ts_map:
                    f_recv = recv_ts_map.get((follower_ex, coin), 0.0)
                    if (now - f_recv) > self.max_follower_book_age_sec:
                        continue

                # 3. Stability Guard: only fade when pre-existing pair baseline is stable
                if is_stable_pair_fn and not is_stable_pair_fn(coin, follower_ex, now):
                    continue

                f_bid = float(getattr(f_snap, "best_bid", 0.0) or 0.0)
                f_ask = float(getattr(f_snap, "best_ask", 0.0) or 0.0)
                f_mid = float(getattr(f_snap, "mid_price", 0.0) or (0.5 * (f_bid + f_ask)))
                if f_bid <= 0 or f_ask <= 0:
                    continue

                # Follower spread guard: reject illiquid defensive spreads (e.g. AsterDex 150-350 bps)
                f_spread_bps = float(getattr(f_snap, "spread_bps", 0.0) or ((f_ask - f_bid) / f_bid * 1e4))
                max_f_spread = 45.0 if follower_ex.lower() == "asterdex" else 80.0
                if f_spread_bps > max_f_spread:
                    continue

                # Don't open duplicate fade for same (coin, follower_ex) if already open
                existing = [p for p in self._active.get(coin, []) if p.follower_exchange == follower_ex and not p.closed]
                if existing:
                    continue

                # 4. Thin Depth Guard: Follower book must be illiquid (top3 depth <= max_follower_depth_usd)
                if self.invert_to_long:
                    top3_ask_usd = float(f_snap.top_n_ask_depth_usd(3) if hasattr(f_snap, "top_n_ask_depth_usd") else 0.0)
                    if top3_ask_usd > self.max_follower_depth_usd:
                        continue
                    stealth_size = min(self.max_position_budget_usd, round(top3_ask_usd * self.stealth_depth_fraction, 2))
                    if stealth_size < self.min_order_usd:
                        continue
                    # Inverted: Long entry at executable ask
                    entry_px = f_snap.vwap_ask_usd(stealth_size) if hasattr(f_snap, "vwap_ask_usd") else f_ask
                    if entry_px <= 0:
                        entry_px = f_ask
                    side = "long"
                else:
                    top3_bid_usd = float(f_snap.top_n_bid_depth_usd(3) if hasattr(f_snap, "top_n_bid_depth_usd") else 0.0)
                    if top3_bid_usd > self.max_follower_depth_usd:
                        continue
                    stealth_size = min(self.max_position_budget_usd, round(top3_bid_usd * self.stealth_depth_fraction, 2))
                    if stealth_size < self.min_order_usd:
                        continue
                    # Classic: Short entry at executable bid
                    entry_px = f_snap.vwap_bid_usd(stealth_size) if hasattr(f_snap, "vwap_bid_usd") else f_bid
                    if entry_px <= 0:
                        entry_px = f_bid
                    side = "short"

                # Check portfolio total exposure budget
                if current_exposure + stealth_size > self.max_total_exposure_usd:
                    continue
                current_exposure += stealth_size

                seq_num = next(self._seq)
                pos = ActiveFadePosition(
                    id=f"fade_{int(now)}_{seq_num:04d}",
                    coin=coin,
                    leader_exchange="upbit",
                    follower_exchange=follower_ex,
                    follower_contract=getattr(f_snap, "raw_symbol", f"{coin}USDT"),
                    open_ts=round(now, 3),
                    fade_reason=fade_reason,
                    trigger_type=trigger_type,
                    swept_usd=round(swept_usd, 2),
                    upbit_spread_bps=round(upbit_spread, 2),
                    entry_upbit_mid=round(upbit_mid, 8),
                    entry_follower_mid=round(f_mid, 8),
                    entry_px=round(entry_px, 8),
                    suggested_size_usd=round(stealth_size, 2),
                    side=side,
                    max_hold_sec=self.max_hold_sec,
                    stop_invalidation_bps=self.stop_invalidation_bps,
                    target_fade_bps=self.target_fade_bps,
                    roundtrip_fee_bps=self.roundtrip_fee_bps,
                    path=[[0.0, 0.0, 0.0, round(-self.roundtrip_fee_bps, 2)]],
                )
                if coin not in self._active:
                    self._active[coin] = []
                self._active[coin].append(pos)
                self._last_fade_ts[pair_key] = now
                opened.append(pos)
                self.counters["fade_signals_triggered"] += 1
                log.info(
                    f"[FADE-SHUTTER] 🎯 Fade entered on {coin} ({pos.follower_contract}) @ {follower_ex} | "
                    f"Entry: {entry_px} | Reason: {fade_reason} | Swept: ${swept_usd:.0f}"
                )

            return opened

    def on_follower_tick(
        self,
        ex: str,
        coin: str,
        f_snap: Any,
        now: float,
        cur_upbit_mid: Optional[float] = None,
    ) -> None:
        """Updates active short positions on follower price change."""
        with self._lock:
            positions = self._active.get(coin, [])
            if not positions:
                return

            cur_ask = float(getattr(f_snap, "best_ask", 0.0) or 0.0)
            cur_bid = float(getattr(f_snap, "best_bid", 0.0) or 0.0)
            cur_mid = float(getattr(f_snap, "mid_price", 0.0) or (0.5 * (cur_bid + cur_ask)))
            if cur_ask <= 0 or cur_bid <= 0:
                return

            alive: List[ActiveFadePosition] = []
            for pos in positions:
                if pos.follower_exchange != ex or pos.closed:
                    if not pos.closed:
                        alive.append(pos)
                    continue

                dt = max(0.0, now - pos.open_ts)
                lead_move = ((cur_upbit_mid - pos.entry_upbit_mid) / pos.entry_upbit_mid * 1e4) if cur_upbit_mid else 0.0
                foll_move = (cur_mid - pos.entry_follower_mid) / pos.entry_follower_mid * 1e4

                close_reason: Optional[str] = None

                if getattr(pos, "side", "short") == "long":
                    # Inverted Long PnL: exit at current bid
                    gross_pnl_bps = (cur_bid - pos.entry_px) / pos.entry_px * 1e4
                    net_pnl_bps = gross_pnl_bps - pos.roundtrip_fee_bps

                    # Excursion tracking
                    if net_pnl_bps > pos.mfe_exec_pnl_bps:
                        pos.mfe_exec_pnl_bps = net_pnl_bps
                    if net_pnl_bps < pos.mae_exec_pnl_bps:
                        pos.mae_exec_pnl_bps = net_pnl_bps

                    if len(pos.path) < 50:
                        pos.path.append([round(dt, 3), round(lead_move, 2), round(foll_move, 2), round(net_pnl_bps, 2)])

                    # Exit Evaluation
                    if net_pnl_bps >= pos.target_fade_bps or (lead_move >= 15.0 and net_pnl_bps > 0.0):
                        close_reason = "converged"
                        self.counters["fade_signals_converged"] += 1
                    elif foll_move <= -pos.stop_invalidation_bps or net_pnl_bps <= -max(pos.stop_invalidation_bps + pos.roundtrip_fee_bps + 15.0, 45.0):
                        close_reason = "stop_invalidation"
                        self.counters["fade_signals_stopped"] += 1
                    elif dt >= pos.max_hold_sec:
                        close_reason = "timeout"
                        self.counters["fade_signals_timeout"] += 1

                    if close_reason:
                        pos.closed = True
                        pos.close_ts = round(now, 3)
                        pos.duration_sec = round(dt, 3)
                        pos.close_reason = close_reason
                        pos.exit_px = round(cur_bid, 8)
                        pos.net_pnl_bps = round(net_pnl_bps, 2)
                        self._emit_position(pos)
                    else:
                        alive.append(pos)
                else:
                    # Classic Short PnL: entry - current ask (cost to buy back)
                    gross_pnl_bps = (pos.entry_px - cur_ask) / pos.entry_px * 1e4
                    net_pnl_bps = gross_pnl_bps - pos.roundtrip_fee_bps

                    # Excursion tracking
                    if net_pnl_bps > pos.mfe_exec_pnl_bps:
                        pos.mfe_exec_pnl_bps = net_pnl_bps
                    if net_pnl_bps < pos.mae_exec_pnl_bps:
                        pos.mae_exec_pnl_bps = net_pnl_bps

                    if len(pos.path) < 50:
                        pos.path.append([round(dt, 3), round(lead_move, 2), round(foll_move, 2), round(net_pnl_bps, 2)])

                    # Exit Evaluation
                    # 1. Take-Profit Convergence: Leader reverted down by >= 15 bps or Short captured >= target_fade_bps
                    if net_pnl_bps >= pos.target_fade_bps or (lead_move <= -15.0 and net_pnl_bps > 0.0):
                        close_reason = "converged"
                        self.counters["fade_signals_converged"] += 1
                    # 2. Invalidation Stop: follower price moved up against our short by >= stop_invalidation_bps or severe drawdown
                    elif foll_move >= pos.stop_invalidation_bps or net_pnl_bps <= -max(pos.stop_invalidation_bps + pos.roundtrip_fee_bps + 15.0, 45.0):
                        close_reason = "stop_invalidation"
                        self.counters["fade_signals_stopped"] += 1
                    # 3. Timeout / Decay
                    elif dt >= pos.max_hold_sec:
                        close_reason = "timeout"
                        self.counters["fade_signals_timeout"] += 1

                    if close_reason:
                        pos.closed = True
                        pos.close_ts = round(now, 3)
                        pos.duration_sec = round(dt, 3)
                        pos.close_reason = close_reason
                        pos.exit_px = round(cur_ask, 8)
                        pos.net_pnl_bps = round(net_pnl_bps, 2)
                        self._emit_position(pos)
                    else:
                        alive.append(pos)

            self._active[coin] = alive

    def on_upbit_tick(self, coin: str, upbit_snap: Any, now: float) -> None:
        """Periodic timeout sweep and leader reversion audit."""
        self._sweep_coin_timeouts(coin, now)

    def sweep_timeouts(self, now: float) -> None:
        """Sweeps and closes positions across all coins if streams stopped ticking."""
        with self._lock:
            for coin in list(self._active.keys()):
                self._sweep_coin_timeouts(coin, now)

    def _sweep_coin_timeouts(self, coin: str, now: float) -> None:
        """Closes expired fade positions without follower updates cleanly with realistic fees."""
        with self._lock:
            positions = self._active.get(coin, [])
            if not positions:
                return

            alive: List[ActiveFadePosition] = []
            for pos in positions:
                if pos.closed:
                    continue
                dt = max(0.0, now - pos.open_ts)
                # Conservative timeout: If max_hold_sec exceeded without follower updates,
                # close as timeout_no_follower_data with zero gross move minus roundtrip taker fees
                if dt >= pos.max_hold_sec:
                    pos.closed = True
                    pos.close_ts = round(now, 3)
                    pos.duration_sec = round(dt, 3)
                    pos.close_reason = "timeout_no_follower_data"
                    pos.exit_px = pos.entry_px
                    pos.net_pnl_bps = round(-pos.roundtrip_fee_bps, 2)
                    self.counters["fade_signals_timeout"] += 1
                    self._emit_position(pos)
                else:
                    alive.append(pos)
            self._active[coin] = alive

    def _emit_position(self, pos: ActiveFadePosition) -> None:
        """Serializes and sends closed fade position to batch writer callback."""
        rec = {
            "id": pos.id,
            "strategy": "fade_fakeout",
            "side": getattr(pos, "side", "short"),
            "coin": pos.coin,
            "leader_exchange": pos.leader_exchange,
            "follower_exchange": pos.follower_exchange,
            "follower_contract": pos.follower_contract,
            "open_ts": pos.open_ts,
            "close_ts": pos.close_ts,
            "duration_sec": pos.duration_sec,
            "fade_reason": pos.fade_reason,
            "trigger_type": pos.trigger_type,
            "swept_usd": pos.swept_usd,
            "upbit_spread_bps": pos.upbit_spread_bps,
            "entry_px": pos.entry_px,
            "exit_px": pos.exit_px,
            "suggested_size_usd": pos.suggested_size_usd,
            "net_pnl_bps": pos.net_pnl_bps,
            "close_reason": pos.close_reason,
            "mfe_exec_pnl_bps": round(pos.mfe_exec_pnl_bps, 2) if pos.mfe_exec_pnl_bps != -999.0 else 0.0,
            "mae_exec_pnl_bps": round(pos.mae_exec_pnl_bps, 2) if pos.mae_exec_pnl_bps != 999.0 else 0.0,
            "path": pos.path,
        }
        self.emit(rec)
        log.info(
            f"[FADE-EXIT] 🏁 {pos.coin} {getattr(pos, 'side', 'short').upper()} closed on {pos.follower_exchange} | "
            f"Net: {pos.net_pnl_bps:+.2f} bps | Reason: {pos.close_reason} | Dur: {pos.duration_sec:.2f}s"
        )
