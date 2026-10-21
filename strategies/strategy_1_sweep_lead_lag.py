"""
strategies/strategy_1_sweep_lead_lag.py - Strategy 1: Korean Sunrise Momentum (Upbit Sweep Lead-Lag).

Microstructure Physics:
South Korea's crypto market (Upbit) is isolated by strict capital controls and features high retail volume.
When an aggressive taker buy sweeps the ask side on Upbit spot, international perpetual futures
(Binance, Bybit, Pionex, Hyperliquid, BingX, KuCoin, OKX) lag by 50ms - 2.5s.
This strategy executes taker LONG on follower perps in the direction of the sweep before the global spread closes.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import logging
import math
import threading
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import config as C
from models import OrderBookDepth5, SunriseSignal

log = logging.getLogger("strategy.lead_lag")

BASELINE_TAU = 600.0        # EWMA time constant for cross-venue basis baseline (sec, 10 minutes)
DEPTH_FLOOR_USD = 5.0       # Numerical stability floor for depth normalization ($5 for low-cap alts)
BEST_ASK_FLOOR_USD = 2.5    # Numerical stability floor for best ask ($2.50 for low-cap alts)
DEV_SAMPLE_DT = 1.0         # 1-second fixed time-grid for MAD sampling


@dataclass(slots=True)
class UpbitNormState:
    """EWMA normalized depth, spread, and top-level liquidity for Upbit spot."""
    depth_ask: float
    best_ask_usd: float
    spread_bps: float
    t: float

    def __getitem__(self, key: str) -> float:
        return getattr(self, key)

    def __setitem__(self, key: str, value: float) -> None:
        setattr(self, key, value)


def evaluate_exit_reason(
    cur_net_pnl: float,
    net_exec_edge_bps: float,
    rem_edge: float,
    dt: float,
    hold_sec: float,
    pair_decay_sec: float,
    sigma: float,
    upbit_drop_bps: float,
    cur_dislocation_bps: float,
    stop_invalidation_bps: float,
    stop_adverse_pnl_bps: float,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Pure mathematical evaluator for signal exit and stop-loss conditions in strict priority:
    1. Take-Profit: 75% net edge captured or basis fully absorbed in profit.
    2. Model A: Microstructure Invalidation Stop.
    3. Model B: Hard Adverse PnL Drawdown Stop.
    4. Model C: Alpha Decay Timeout Stop.
    5. Max Hold Horizon Timeout.
    Returns (close_reason, stop_type).
    """
    # 1. Normal Take-Profit / Edge Absorption
    if cur_net_pnl >= (net_exec_edge_bps * 0.75) and cur_net_pnl > 0.0:
        return "converged", None
    if rem_edge <= 0.0 and cur_net_pnl > 0.0:
        return "converged", None

    # 2. Model A: Microstructure Invalidation Stop
    if upbit_drop_bps >= (1.5 * sigma) or cur_dislocation_bps <= -stop_invalidation_bps:
        return "stop_invalidation", "invalidation"

    # 3. Model B: Hard Adverse PnL Drawdown Stop
    adverse_limit = max(stop_adverse_pnl_bps, 2.5 * sigma)
    if cur_net_pnl <= -adverse_limit:
        return "stop_adverse_pnl", "adverse_pnl"

    # 4. Model C: Alpha Decay Timeout Stop
    if dt >= pair_decay_sec and cur_net_pnl <= 0.0:
        return "stop_decay", "alpha_decay"

    # 5. Max Hold Horizon Timeout
    if dt >= hold_sec:
        return "timeout", None

    return None, None


class PairState:
    """State of an (Upbit, Follower) pair for a single coin."""

    __slots__ = (
        "coin",
        "follower_ex",
        "warmup_samples",
        "spike_sigma",
        "baseline_bps",
        "n_obs",
        "t_last",
        "t_last_diff",
        "_initial_diffs",
        "dev_window",
        "dev_ts",
        "cached_sigma",
        "cached_sigma_ts",
        "open_signal",
        "_lock",
    )

    def __init__(
        self,
        coin: str,
        follower_ex: str,
        warmup_samples: int = C.MIN_WARMUP_SAMPLES,
        spike_sigma: float = 2.5,
    ):
        self.coin = coin
        self.follower_ex = follower_ex
        self.warmup_samples = warmup_samples
        self.spike_sigma = spike_sigma

        # Rolling cross-venue basis baseline (bps): (Mid_Upbit - Mid_Follower) / Mid_Follower * 10000
        self.baseline_bps: float = 0.0
        self.n_obs: int = 0
        self.t_last: float = 0.0
        self.t_last_diff: float = 0.0
        self._initial_diffs: List[float] = []

        # Robust MAD window sampled on fixed 1s grid
        self.dev_window: Deque[float] = deque(maxlen=300)
        self.dev_ts: float = 0.0
        self.cached_sigma: float = 10.0
        self.cached_sigma_ts: float = 0.0

        # Active open signal
        self.open_signal: Optional[SunriseSignal] = None
        self._lock = threading.Lock()

    @property
    def is_warmed_up(self) -> bool:
        """Determines if sufficient observation samples have been collected."""
        with self._lock:
            return self.n_obs >= self.warmup_samples and len(self.dev_window) >= self.warmup_samples

    def is_stable(self, now: float, current_diff_bps: Optional[float] = None) -> bool:
        """Separate gate: verifies pair baseline is not in a pre-existing severe dislocation."""
        with self._lock:
            if not (self.n_obs >= self.warmup_samples and len(self.dev_window) >= self.warmup_samples):
                return False
            if self.warmup_samples < 10 or self.n_obs < 10:
                return True
            cur_sig = self._calc_sigma_unlocked(now)
            diff = current_diff_bps if current_diff_bps is not None else self.t_last_diff
            return abs(diff - self.baseline_bps) <= max(2.5 * cur_sig, 25.0)

    def learn(self, raw_diff_bps: float, now: float) -> None:
        with self._lock:
            self.t_last_diff = raw_diff_bps
            if self.n_obs == 0:
                self.baseline_bps = raw_diff_bps
                self._initial_diffs = [raw_diff_bps]
                self.n_obs = 1
                self.t_last = now
                self.dev_ts = now
                return

            dt = max(0.0, now - self.t_last)
            self.t_last = now

            # Cold-start robustness: for the first 10 samples, anchor baseline to the median of observed diffs
            if len(self._initial_diffs) < 10:
                self._initial_diffs.append(raw_diff_bps)
                if len(self._initial_diffs) >= 3:
                    s_diffs = sorted(self._initial_diffs)
                    self.baseline_bps = s_diffs[len(s_diffs) // 2]

            deviation = raw_diff_bps - self.baseline_bps

            # Spike gating: slow down baseline adaptation during active pump
            cur_sig = self._calc_sigma_unlocked(now)
            is_warmed = self.n_obs >= self.warmup_samples and len(self.dev_window) >= self.warmup_samples
            is_spike = is_warmed and abs(deviation) > (self.spike_sigma * cur_sig)
            eff_tau = 5.0 if not is_warmed else (BASELINE_TAU * 20.0 if is_spike else BASELINE_TAU)

            a = 1.0 - math.exp(-dt / eff_tau) if eff_tau > 0 else 0.001
            self.baseline_bps += a * deviation
            self.n_obs += 1

            if now - self.dev_ts >= DEV_SAMPLE_DT:
                self.dev_ts = now
                if is_spike:
                    clamped_dev = math.copysign(min(abs(deviation), (self.spike_sigma + 1.5) * cur_sig), deviation)
                    self.dev_window.append(clamped_dev)
                else:
                    self.dev_window.append(deviation)
                self.cached_sigma_ts = 0.0

    def sigma(self, now: float) -> float:
        with self._lock:
            return self._calc_sigma_unlocked(now)

    def _calc_sigma_unlocked(self, now: float) -> float:
        if self.cached_sigma_ts and (now - self.cached_sigma_ts) < 1.0:
            return self.cached_sigma

        if len(self.dev_window) < 10:
            return 10.0

        vals = sorted(self.dev_window)
        med = vals[len(vals) // 2]
        abs_dev = sorted(abs(x - med) for x in vals)
        mad = abs_dev[len(abs_dev) // 2]
        self.cached_sigma = max(1.4826 * mad, 1.5)
        self.cached_sigma_ts = now
        return self.cached_sigma


class KoreanLeadLagStrategy:
    """
    Strategy 1: Korean Sunrise Momentum (Upbit Sweep Lead-Lag).
    Encapsulates edge evaluation, sizing, order creation, and exit lifecycle.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        min_edge_bps: float = C.MIN_EXEC_EDGE_BPS,
        max_position_budget_usd: float = C.MAX_POSITION_BUDGET_USD,
        min_order_usd: float = getattr(C, "MIN_ORDER_USD", 10.0),
        stealth_depth_fraction: float = C.STEALTH_DEPTH_FRACTION,
        roundtrip_fee_bps: float = C.ROUNDTRIP_FEE_BPS,
        max_hold_sec: float = C.MAX_SIGNAL_HOLD_SEC,
        max_follower_book_age_sec: float = getattr(C, "MAX_FOLLOWER_BOOK_AGE_SEC", 1.50),
        stop_invalidation_bps: float = getattr(C, "STOP_INVALIDATION_BPS", 15.0),
        stop_adverse_pnl_bps: float = getattr(C, "STOP_ADVERSE_PNL_BPS", 35.0),
        alpha_decay_sec: float = getattr(C, "ALPHA_DECAY_SEC", 15.0),
        pool_state: Any = None,
        profile_manager: Any = None,
    ):
        self.emit = emit_callback
        self.min_edge_bps = min_edge_bps
        self.max_position_budget_usd = max_position_budget_usd
        self.min_order_usd = min_order_usd
        self.stealth_fraction = stealth_depth_fraction
        self.roundtrip_fee_bps = roundtrip_fee_bps
        self.max_hold_sec = max_hold_sec
        self.max_follower_book_age_sec = max_follower_book_age_sec
        self.stop_invalidation_bps = stop_invalidation_bps
        self.stop_adverse_pnl_bps = stop_adverse_pnl_bps
        self.alpha_decay_sec = alpha_decay_sec
        self.pool_state = pool_state
        self.profile_manager = profile_manager

    def evaluate_entry(
        self,
        coin: str,
        follower_ex: str,
        st: PairState,
        upbit_snap: OrderBookDepth5,
        f_snap: OrderBookDepth5,
        f_book_age_ms: float,
        norm_depth: float,
        norm_spread: float,
        swept_usd: float,
        trigger_type: str,
        drain_ratio: float,
        blowout_ratio: float,
        now: float,
        recent_buys_usd: float,
        pair_cfg: Dict[str, Any],
        seq_num: int,
        pool_state: Any = None,
        profile_manager: Any = None,
    ) -> Optional[SunriseSignal]:
        """Calculates dislocation, edge, stealth sizing, and instantiates SunriseSignal if valid."""
        pool_st = pool_state if pool_state is not None else self.pool_state
        prof_mgr = profile_manager if profile_manager is not None else self.profile_manager

        sigma = st.sigma(now)
        upbit_mid = upbit_snap.mid_price
        f_mid = f_snap.mid_price
        f_bid = f_snap.best_bid
        f_ask = f_snap.best_ask

        if f_ask <= 0 or f_bid <= 0:
            return None

        # Follower spread guard: reject illiquid books where follower spread is abnormally wide
        # (Blocks 150-350 bps defensive quarantine spreads on AsterDex like TAIKO/TREE)
        f_spread_bps = float(getattr(f_snap, "spread_bps", 0.0) or ((f_ask - f_bid) / f_bid * 1e4))
        max_f_spread = getattr(C, "MAX_ASTERDEX_SPREAD_BPS", 45.0) if follower_ex.lower() == "asterdex" else 80.0
        if f_spread_bps > max_f_spread:
            return None

        # Stale quote guard: reject follower books older than max allowed age
        if f_book_age_ms > (self.max_follower_book_age_sec * 1000.0):
            return None

        raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4
        dislocation_bps = raw_diff_bps - st.baseline_bps
        if dislocation_bps <= 0:
            return None

        dev_z = dislocation_bps / max(sigma, 1.5)
        if not pair_cfg.get("enabled", True):
            return None  # Pruned pair due to negative EV or high adverse flow

        min_dev_z = float(pair_cfg.get("min_dev_z", 1.8))
        if dev_z < min_dev_z:
            return None

        pair_hold_sec = float(pair_cfg.get("max_hold_sec", self.max_hold_sec))
        top3_ask_depth_usd = f_snap.top_n_ask_depth_usd(3)
        min_depth_needed = self.min_order_usd / max(self.stealth_fraction, 0.01)
        if top3_ask_depth_usd < min_depth_needed:
            return None

        stealth_capacity_usd = round(self.stealth_fraction * top3_ask_depth_usd, 2)
        is_battle = pool_st.is_battle_coin(coin) if pool_st else False
        basket = "battle" if is_battle else "incubator"
        is_actionable = is_battle

        if prof_mgr:
            if not prof_mgr.is_venue_active(follower_ex):
                return None
            sized = prof_mgr.calculate_stealth_order_size(follower_ex, top3_ask_depth_usd)
            if is_battle and sized is None:
                return None
            suggested_size_usd = sized if sized is not None else min(
                self.max_position_budget_usd,
                stealth_capacity_usd,
            )
        else:
            suggested_size_usd = min(self.max_position_budget_usd, stealth_capacity_usd)

        if suggested_size_usd < self.min_order_usd:
            return None

        f_entry_px = f_snap.vwap_ask_usd(suggested_size_usd)
        if f_entry_px <= 0:
            return None

        f_ask_diff_bps = (upbit_mid - f_entry_px) / f_entry_px * 1e4
        gross_exec_edge_bps = f_ask_diff_bps - st.baseline_bps
        net_edge = gross_exec_edge_bps - self.roundtrip_fee_bps

        if "min_spread_bps" in pair_cfg:
            req_spread = float(pair_cfg["min_spread_bps"])
            if gross_exec_edge_bps < req_spread or net_edge <= 0.0:
                return None
        else:
            if net_edge < self.min_edge_bps:
                return None

        init_exit_px = f_snap.vwap_bid_usd(suggested_size_usd)
        init_exec_move_bps = (init_exit_px - f_entry_px) / f_entry_px * 1e4
        init_net_pnl_bps = round(init_exec_move_bps - self.roundtrip_fee_bps, 2)

        lead_ex = getattr(upbit_snap, "exchange", "upbit") or "upbit"
        sig = SunriseSignal(
            id=f"sun_{int(now)}_{seq_num:04d}",
            coin=coin,
            leader_exchange=lead_ex,
            follower_exchange=follower_ex,
            follower_contract=f_snap.raw_symbol,
            open_ts=round(now, 3),
            trigger_type=trigger_type,
            leader_mid=round(upbit_mid, 8),
            leader_best_ask=round(upbit_snap.best_ask, 8),
            leader_spread_bps=round(upbit_snap.spread_bps, 2),
            leader_norm_spread_bps=round(norm_spread, 2),
            leader_top5_ask_usd=round(upbit_snap.depth_ask_usd, 2),
            leader_norm_depth_usd=round(norm_depth, 2),
            leader_swept_usd=round(swept_usd, 2),
            leader_imbalance_top5=round(upbit_snap.imbalance_top5, 3),
            drain_ratio=round(drain_ratio, 3),
            spread_blowout_ratio=round(blowout_ratio, 2),
            follower_mid=round(f_mid, 8),
            follower_best_bid=round(f_bid, 8),
            follower_best_ask=round(f_ask, 8),
            follower_spread_bps=round(f_snap.spread_bps, 2),
            follower_top3_ask_usd=round(top3_ask_depth_usd, 2),
            follower_top3_bid_usd=round(f_snap.top_n_bid_depth_usd(3), 2),
            follower_imbalance_top3=round(f_snap.imbalance_top3, 3),
            kimchi_baseline_bps=round(st.baseline_bps, 2),
            pair_sigma_bps=round(sigma, 2),
            raw_dislocation_bps=round(dislocation_bps, 2),
            dev_z=round(dev_z, 2),
            gross_exec_edge_bps=round(gross_exec_edge_bps, 2),
            net_exec_edge_bps=round(net_edge, 2),
            leader_latency_ms=upbit_snap.network_latency_ms,
            follower_latency_ms=f_snap.network_latency_ms,
            follower_book_age_ms=round(f_book_age_ms, 1),
            suggested_size_usd=round(suggested_size_usd, 2),
            stealth_capacity_usd=round(stealth_capacity_usd, 2),
            follower_entry_px=round(f_entry_px, 8),
            leader_recent_buys_usd=round(recent_buys_usd, 2),
            basket=basket,
            is_actionable=is_actionable,
            max_hold_sec=pair_hold_sec,
            mfe_exec_pnl_bps=init_net_pnl_bps,
            mae_exec_pnl_bps=init_net_pnl_bps,
            mfe_mid_move_bps=0.0,
            time_to_peak_sec=0.0,
            path=[[0.0, 0.0, 0.0, init_net_pnl_bps, round(net_edge, 2)]],
        )
        return sig
