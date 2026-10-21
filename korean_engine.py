"""
korean_engine.py - High-frequency Korean Lead-Lag & Relative Depth Drain Engine.
Evaluates Spot Leader (Upbit) against Perpetual Followers (Binance, Bybit)
and records full microstructural telemetry for post-trade analysis.
"""
from __future__ import annotations

import itertools
import json
import logging
import math
import os
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

import config as C
from models import OrderBookDepth5, SunriseSignal
from pipeline.market_regime import MarketRegime, MarketRegimeDetector, MarketRegimeSnapshot
from strategies import (
    ActiveFadePosition,
    ActiveMakerVoid,
    FadeFakeoutStrategy,
    KoreanLeadLagStrategy,
    MakerVoidStrategy,
    PairState,
    UpbitNormState,
    evaluate_exit_reason,
)

log = logging.getLogger("engine")

DEV_SAMPLE_DT = 1.0     # Fixed 1s time grid for robust MAD window
NORM_TAU = 600.0        # EWMA time constant for Upbit depth and spread normalization (sec)
FOLLOWER_SPREAD_TAU = 120.0 # Responsive 2-minute EWMA time constant for follower spread baselines
BASELINE_TAU = 600.0    # EWMA time constant for cross-venue basis baseline (sec, 10 minutes)
DEPTH_FLOOR_USD = 5.0   # Numerical stability floor for depth normalization ($5 for low-cap alts)
BEST_ASK_FLOOR_USD = 2.5 # Numerical stability floor for best ask ($2.50 for low-cap alts)
KOREAN_LEADERS: Set[str] = {"upbit", "bithumb"}


def _extract_spread_bps(norm_obj: Any, default: float = 20.0) -> float:
    """Robust extractor for spread_bps supporting both UpbitNormState and dict structures."""
    if norm_obj is None:
        return default
    if isinstance(norm_obj, dict):
        return float(norm_obj.get("spread_bps", default))
    val = getattr(norm_obj, "spread_bps", default)
    return float(val if val is not None else default)


class FollowerSpreadNorm:
    """
    EWMA baseline for a follower venue's own spread with cold-start warmup protection.

    Cold-start robustness:
    If the first tick arrives during an active spread blowout / void, anchoring immediately
    to the first raw tick would calibrate the baseline to the anomaly.
    Instead:
    - We accumulate the first WARMUP_SAMPLES observations (default 5).
    - During warmup, baseline is anchored to the minimum observed spread.
    - `is_warmed_up` is exposed so S3 Maker/Taker can gate execution.
    """
    __slots__ = ("spread_bps", "last_ts", "n_obs", "_warmup_spreads", "warmup_samples")

    def __init__(self, warmup_samples: int = 5):
        self.spread_bps: float = 20.0
        self.last_ts: float = 0.0
        self.n_obs: int = 0
        self._warmup_spreads: List[float] = []
        self.warmup_samples: int = warmup_samples

    @property
    def is_warmed_up(self) -> bool:
        return self.n_obs >= self.warmup_samples

    def update(self, spread_bps: float, now: float, tau: float = FOLLOWER_SPREAD_TAU) -> float:
        clamped_cur = max(spread_bps, 0.5)
        self.n_obs += 1

        if self.n_obs == 1:
            self.spread_bps = clamped_cur
            self.last_ts = now
            self._warmup_spreads = [clamped_cur]
            return self.spread_bps

        dt = max(0.0, now - self.last_ts)
        self.last_ts = now

        if len(self._warmup_spreads) < self.warmup_samples:
            self._warmup_spreads.append(clamped_cur)
            # Anchor to minimum observed spread during warmup to prevent locking into a blowout
            self.spread_bps = min(self._warmup_spreads)
            return self.spread_bps

        a = 1.0 - math.exp(-dt / tau)
        clamped_update = min(clamped_cur, self.spread_bps * 5.0)
        self.spread_bps = max((1.0 - a) * self.spread_bps + a * clamped_update, 0.5)
        return self.spread_bps

    def __getitem__(self, idx: int) -> float:
        """Tuple backward-compatibility: (spread_bps, last_ts)."""
        if idx == 0:
            return self.spread_bps
        elif idx == 1:
            return self.last_ts
        raise IndexError(f"FollowerSpreadNorm index out of range: {idx}")


class KoreanEngine:
    """
    Core Lead-Lag Engine with full analytical telemetry.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        drain_threshold: float = C.DRAIN_THRESHOLD,
        blowout_threshold: float = C.BLOWOUT_THRESHOLD,
        min_edge_bps: float = C.MIN_EXEC_EDGE_BPS,
        max_position_budget_usd: float = C.MAX_POSITION_BUDGET_USD,
        min_order_usd: float = getattr(C, "MIN_ORDER_USD", 10.0),
        stealth_depth_fraction: float = C.STEALTH_DEPTH_FRACTION,
        taker_fee_bps: float = C.TAKER_FEE_BPS,
        roundtrip_fee_bps: float = C.ROUNDTRIP_FEE_BPS,
        warmup_samples: int = C.MIN_WARMUP_SAMPLES,
        max_hold_sec: float = C.MAX_SIGNAL_HOLD_SEC,
        max_follower_book_age_sec: float = getattr(C, "MAX_FOLLOWER_BOOK_AGE_SEC", 1.50),
        stop_invalidation_bps: float = getattr(C, "STOP_INVALIDATION_BPS", 15.0),
        stop_adverse_pnl_bps: float = getattr(C, "STOP_ADVERSE_PNL_BPS", 35.0),
        alpha_decay_sec: float = getattr(C, "ALPHA_DECAY_SEC", 15.0),
        trade_confirm_lookback_sec: float = getattr(C, "TRADE_CONFIRM_LOOKBACK_SEC", 2.0),
        min_trade_confirm_floor_usd: float = 5.0,
        require_trade_confirmation: bool = False,
        require_korean_confirmation: bool = getattr(C, "REQUIRE_KOREAN_CONFIRMATION", False),
        pool_state: Any = None,
        profile_manager: Any = None,
        thresholds_file: Optional[str] = None,
    ):
        self.emit = emit_callback
        self.drain_threshold = drain_threshold
        self.blowout_threshold = blowout_threshold
        self.min_edge_bps = min_edge_bps
        self.max_position_budget_usd = max_position_budget_usd
        self.min_order_usd = min_order_usd
        self.stealth_fraction = stealth_depth_fraction
        self.taker_fee_bps = taker_fee_bps
        self.roundtrip_fee_bps = roundtrip_fee_bps
        self.warmup_samples = warmup_samples
        self.max_hold_sec = max_hold_sec
        self.max_follower_book_age_sec = max_follower_book_age_sec
        self.stop_invalidation_bps = stop_invalidation_bps
        self.stop_adverse_pnl_bps = stop_adverse_pnl_bps
        self.alpha_decay_sec = alpha_decay_sec
        self.trade_confirm_lookback_sec = trade_confirm_lookback_sec
        self.min_trade_confirm_floor_usd = min_trade_confirm_floor_usd
        self.require_trade_confirmation = require_trade_confirmation
        self.require_korean_confirmation = require_korean_confirmation
        self.pool_state = pool_state
        self.profile_manager = profile_manager

        # Internal reentrant lock for thread-safe cross-thread execution
        self._engine_lock = threading.RLock()

        # (exchange, coin) -> OrderBookDepth5
        self._books: Dict[Tuple[str, str], OrderBookDepth5] = {}
        self._recv_ts: Dict[Tuple[str, str], float] = {}

        # O(1) follower lookup index: coin -> set of follower exchanges
        self._coin_followers: Dict[str, Set[str]] = defaultdict(set)

        # Upbit recent taker buys: coin -> deque of (ts, trade_usd)
        self._buys_lock = threading.Lock()
        self._upbit_recent_buys: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=200))
        self._has_upbit_trades: Dict[str, bool] = defaultdict(bool)
        self._bithumb_recent_buys: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=200))
        self._has_bithumb_trades: Dict[str, bool] = defaultdict(bool)

        # Korean leader normalisation state per coin:
        # coin -> {"depth_ask": float, "best_ask_usd": float, "spread_bps": float, "t": float}
        self._upbit_norm: Dict[str, Dict[str, float]] = {}
        self._bithumb_norm: Dict[str, Dict[str, float]] = {}

        # Follower exchange own spread baseline EWMA per (follower_ex, coin):
        # (follower_ex, coin) -> FollowerSpreadNorm
        self._follower_spread_ewma: Dict[Tuple[str, str], FollowerSpreadNorm] = {}

        # Pair states: (coin, follower_exchange) or (coin, follower_exchange, leader_exchange) -> PairState
        self._pairs: Dict[Tuple[Any, ...], PairState] = {}

        # Active signals tracking: coin -> list of active SunriseSignal
        self._active_signals: Dict[str, List[SunriseSignal]] = defaultdict(list)
        self._seq = itertools.count(1)

        # Telemetry counters
        self.counters = {
            "ticks_upbit": 0,
            "ticks_bithumb": 0,
            "ticks_followers": 0,
            "trades_upbit": 0,
            "trades_bithumb": 0,
            "cancels_blocked": 0,
            "korean_unconfirmed_blocked": 0,
            "signals_triggered": 0,
            "signals_battle": 0,
            "signals_incubator": 0,
            "signals_converged": 0,
            "signals_reversed": 0,
            "signals_timeout": 0,
            "signals_stop_loss_invalidation": 0,
            "signals_stop_loss_adverse_pnl": 0,
            "signals_stop_loss_decay": 0,
            "fade_signals_triggered": 0,
            "fade_signals_converged": 0,
            "fade_signals_stopped": 0,
            "fade_signals_timeout": 0,
            "maker_voids_triggered": 0,
            "maker_voids_filled": 0,
            "maker_voids_restored": 0,
            "maker_voids_unfilled": 0,
            "maker_voids_stopped": 0,
            "maker_voids_timeout": 0,
            "regime_dump_blocked": 0,
            "regime_pump_blocked_shorts": 0,
        }

        # Macro Market Regime & Altcoin Breadth Engine
        self.regime_detector = MarketRegimeDetector()

        # Multi-Strategy Quantitative Suite:
        # Strategy 1: Korean Sunrise Momentum (Upbit Sweep Lead-Lag)
        self.strat_lead_lag = KoreanLeadLagStrategy(
            emit_callback=self.emit,
            min_edge_bps=self.min_edge_bps,
            max_position_budget_usd=self.max_position_budget_usd,
            min_order_usd=self.min_order_usd,
            stealth_depth_fraction=self.stealth_fraction,
            roundtrip_fee_bps=self.roundtrip_fee_bps,
            max_hold_sec=self.max_hold_sec,
            max_follower_book_age_sec=self.max_follower_book_age_sec,
            stop_invalidation_bps=self.stop_invalidation_bps,
            stop_adverse_pnl_bps=self.stop_adverse_pnl_bps,
            alpha_decay_sec=self.alpha_decay_sec,
            pool_state=self.pool_state,
            profile_manager=self.profile_manager,
        )

        # Strategy 2: Pre-Sweep Early Long / Inverted Fade
        self.strat_fade = FadeFakeoutStrategy(
            emit_callback=self._on_strategy_emit,
            roundtrip_fee_bps=self.roundtrip_fee_bps,
            min_order_usd=self.min_order_usd,
            max_position_budget_usd=self.max_position_budget_usd,
            stealth_depth_fraction=self.stealth_fraction,
            max_fade_swept_usd=getattr(C, "MAX_FADE_SWEPT_USD", 2000.0),
            min_upbit_spread_bps=getattr(C, "MIN_FADE_SPREAD_BPS", 35.0),
            max_follower_depth_usd=getattr(C, "MAX_FADE_FOLLOWER_DEPTH_USD", 3500.0),
            cooldown_sec=getattr(C, "FADE_COOLDOWN_SEC", 30.0),
            invert_to_long=getattr(C, "S2_INVERT_TO_LONG", True),
            max_active_positions=getattr(C, "MAX_PORTFOLIO_CONCURRENT_POSITIONS", 10),
            max_total_exposure_usd=getattr(C, "MAX_PORTFOLIO_EXPOSURE_USD", 5000.0),
            counters=self.counters,
        )

        # Strategy 3: Toxic Taker Void Hunter / Inverted Maker
        self.strat_maker = MakerVoidStrategy(
            emit_callback=self._on_strategy_emit,
            min_order_usd=self.min_order_usd,
            max_position_budget_usd=self.max_position_budget_usd,
            stealth_depth_fraction=self.stealth_fraction,
            maker_rebate_bps=getattr(C, "MAKER_REBATE_BPS", 0.0),
            taker_fee_bps=self.taker_fee_bps,
            max_spread_bps=getattr(C, "MAX_MAKER_SPREAD_BPS", 120.0),
            allow_asterdex_maker=getattr(C, "ASTERDEX_MAKER_ENABLED", False),
            hunter_mode=getattr(C, "S3_HUNTER_MODE", True),
            max_active_voids=getattr(C, "MAX_PORTFOLIO_CONCURRENT_POSITIONS", 10),
            max_total_exposure_usd=getattr(C, "MAX_PORTFOLIO_EXPOSURE_USD", 5000.0),
            counters=self.counters,
        )

        # Pair-specific calibrated execution thresholds
        self.thresholds_file = thresholds_file or os.path.join(C.PROJECT_ROOT, "matrix_thresholds.json")
        self._thresholds: Dict[str, Any] = {}
        self._last_thresh_check: float = 0.0
        self._thresh_mtime: float = 0.0
        self.reload_thresholds(force=True)

    def _on_strategy_emit(self, rec: Dict[str, Any]) -> None:
        """Centralized emitter for multi-strategy execution before routing to writer callback."""
        try:
            self.emit(rec)
        except Exception as e:
            log.error(f"[engine] Failed to emit strategy record {rec.get('id', '')}: {e}")

    def reload_thresholds(self, force: bool = False) -> None:
        try:
            if os.path.exists(self.thresholds_file):
                mtime = os.path.getmtime(self.thresholds_file)
                if not force and mtime == self._thresh_mtime:
                    return
                with open(self.thresholds_file, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                    self._thresholds = doc.get("thresholds", {})
                self._thresh_mtime = mtime
                log.info(f"[engine] Loaded {len(self._thresholds)} calibrated pair thresholds.")
                # Update existing PairState instances with latest spike_sigma
                with self._engine_lock:
                    pair_items = list(self._pairs.items())
                for (coin, f_ex), st in pair_items:
                    cfg = self.get_pair_threshold_config(coin, f_ex)
                    new_sigma = float(cfg.get("spike_sigma", 2.5))
                    if st.spike_sigma != new_sigma:
                        st.spike_sigma = new_sigma
                        st.cached_sigma_ts = 0.0
        except Exception as e:
            log.warning(f"[engine] Failed to load {self.thresholds_file}: {e}")

    def maybe_reload_thresholds(self, now: float) -> None:
        if now - self._last_thresh_check >= 30.0:
            self._last_thresh_check = now
            try:
                self.reload_thresholds()
            except Exception as e:
                log.warning(f"[engine] Error reloading thresholds: {e}")

            if self.pool_state is not None and hasattr(self.pool_state, "reload"):
                try:
                    self.pool_state.reload()
                except Exception as e:
                    log.warning(f"[engine] Error reloading pool_state: {e}")

            if self.profile_manager is not None and hasattr(self.profile_manager, "reload"):
                try:
                    self.profile_manager.reload()
                except Exception as e:
                    log.warning(f"[engine] Error reloading profile_manager: {e}")

    def get_pair_threshold_config(self, coin: str, follower_ex: str, leader_ex: str = "upbit") -> Dict[str, Any]:
        pk = f"{coin.upper()}:{leader_ex.lower()}-{follower_ex.lower()}"
        if pk in self._thresholds:
            return self._thresholds[pk]
        pk_upbit = f"{coin.upper()}:upbit-{follower_ex.lower()}"
        return self._thresholds.get(pk_upbit, {})

    def _get_or_create_pair_state(self, coin: str, follower_ex: str, leader_ex: str = "upbit") -> PairState:
        pair_key = (coin, follower_ex) if leader_ex == "upbit" else (coin, follower_ex, leader_ex)
        with self._engine_lock:
            st = self._pairs.get(pair_key)
            if st is None:
                cfg = self.get_pair_threshold_config(coin, follower_ex, leader_ex)
                sp_sig = float(cfg.get("spike_sigma", 2.5))
                st = self._pairs[pair_key] = PairState(
                    coin, follower_ex, self.warmup_samples, spike_sigma=sp_sig
                )
            return st

    def on_book_update(self, snap: OrderBookDepth5) -> None:
        now = time.time()
        ex = snap.exchange.lower()
        coin = snap.symbol.upper()

        if coin == "BTC":
            self.regime_detector.on_btc_tick(snap.mid_price, now)
        else:
            self.regime_detector.on_alt_tick(coin, snap.mid_price, now)

        key = (ex, coin)

        with self._engine_lock:
            prev_snap = self._books.get(key)
            self._books[key] = snap
            self._recv_ts[key] = now

        if ex == "upbit":
            self.counters["ticks_upbit"] += 1
            self._eval_lead_lag(coin, snap, prev_snap, now, leader_ex="upbit")
            self._update_norm(coin, snap, now, venue="upbit")
            self._track_active_signals_for_coin(coin, now)
        elif ex == "bithumb":
            self.counters["ticks_bithumb"] += 1
            self._eval_lead_lag(coin, snap, prev_snap, now, leader_ex="bithumb")
            self._update_norm(coin, snap, now, venue="bithumb")
            self._track_active_signals_for_coin(coin, now)
        else:
            with self._engine_lock:
                self._coin_followers[coin].add(ex)
            self.counters["ticks_followers"] += 1

            # Learn Upbit baseline
            upbit_snap = self._books.get(("upbit", coin))
            if upbit_snap is not None and (now - self._recv_ts.get(("upbit", coin), 0.0)) <= 2.0:
                st = self._get_or_create_pair_state(coin, ex, leader_ex="upbit")
                if upbit_snap.mid_price > 0 and snap.mid_price > 0:
                    raw_diff_bps = (upbit_snap.mid_price - snap.mid_price) / snap.mid_price * 1e4
                    with self._engine_lock:
                        has_open = (st.open_signal is not None)
                    if not has_open:
                        st.learn(raw_diff_bps, now)

            # Learn Bithumb baseline
            bithumb_snap = self._books.get(("bithumb", coin))
            if bithumb_snap is not None and (now - self._recv_ts.get(("bithumb", coin), 0.0)) <= 2.0:
                st_b = self._get_or_create_pair_state(coin, ex, leader_ex="bithumb")
                if bithumb_snap.mid_price > 0 and snap.mid_price > 0:
                    raw_diff_bps_b = (bithumb_snap.mid_price - snap.mid_price) / snap.mid_price * 1e4
                    with self._engine_lock:
                        has_open_b = (st_b.open_signal is not None)
                    if not has_open_b:
                        st_b.learn(raw_diff_bps_b, now)

            self._track_follower_move(coin, ex, snap, now)

            # Update follower's own baseline spread EWMA (venues have different baseline spreads)
            # Protected by _engine_lock against concurrent feed updates (Item 6)
            f_key = (ex, coin)
            cur_f_spread = max(snap.spread_bps, 0.5)
            with self._engine_lock:
                f_norm = self._follower_spread_ewma.get(f_key)
                if f_norm is None:
                    warmup_n = int(getattr(C, "FOLLOWER_SPREAD_WARMUP_SAMPLES", 5))
                    f_norm = FollowerSpreadNorm(warmup_samples=warmup_n)
                    self._follower_spread_ewma[f_key] = f_norm
                f_norm_spread = f_norm.update(cur_f_spread, now)
                is_f_warmed = f_norm.is_warmed_up

            # Multi-strategy follower updates (invoked outside _engine_lock per Lock Hierarchy Contract):
            # Strategy 2: Fade the Fakeout (guarded with freshness <= 2.0s to avoid stale Upbit mid)
            upbit_age = now - self._recv_ts.get(("upbit", coin), 0.0)
            cur_upbit_mid = upbit_snap.mid_price if (upbit_snap and upbit_age <= 2.0) else None
            self.strat_fade.on_follower_tick(ex, coin, snap, now, cur_upbit_mid=cur_upbit_mid)

            # Strategy 3: Maker Liquidity Harvesting / Taker Hunter (using follower's own EWMA spread baseline and freshness check)
            f_recv = self._recv_ts.get((ex, coin), now)
            self.strat_maker.on_follower_tick(ex, coin, snap, now, norm_spread_bps=f_norm_spread, recv_ts=f_recv)
            self.strat_maker.check_void_opportunity(
                ex, coin, snap, upbit_snap, now,
                norm_spread_bps=f_norm_spread,
                recv_ts=f_recv,
                is_warmed_up=is_f_warmed,
            )

    def on_upbit_trade(self, coin: str, trade_usd: float, is_taker_buy: bool, ts: float) -> None:
        """
        Real-time trade stream ingestion from Upbit.
        Records taker buys to verify liquidity sweeps and reject MM quote cancellations.
        Thread-safe across concurrent WebSocket worker threads.
        """
        self.regime_detector.on_korean_trade(trade_usd, is_taker_buy, ts)
        c = coin.upper()
        with self._buys_lock:
            self.counters["trades_upbit"] = self.counters.get("trades_upbit", 0) + 1
            self._has_upbit_trades[c] = True
            if is_taker_buy and trade_usd > 0.0:
                self._upbit_recent_buys[c].append((ts, trade_usd))

    def on_bithumb_trade(self, coin: str, trade_usd: float, is_taker_buy: bool, ts: float) -> None:
        """
        Real-time trade stream ingestion from Bithumb.
        Records taker buys to verify Bithumb liquidity sweeps and volume explosions.
        Thread-safe across concurrent WebSocket worker threads.
        """
        self.regime_detector.on_korean_trade(trade_usd, is_taker_buy, ts)
        c = coin.upper()
        with self._buys_lock:
            self.counters["trades_bithumb"] = self.counters.get("trades_bithumb", 0) + 1
            self._has_bithumb_trades[c] = True
            if is_taker_buy and trade_usd > 0.0:
                self._bithumb_recent_buys[c].append((ts, trade_usd))

    def get_recent_upbit_buys(self, coin: str, cutoff: float) -> Tuple[float, bool]:
        """
        Thread-safe retrieval and pruning of recent taker buys for a coin on Upbit.
        Returns (recent_buys_usd, has_trades).
        Prevents race conditions (IndexError / deque mutation during iteration).
        """
        c = coin.upper()
        with self._buys_lock:
            buys_deque = self._upbit_recent_buys.get(c)
            recent_buys_usd = 0.0
            if buys_deque:
                while buys_deque and buys_deque[0][0] < cutoff:
                    buys_deque.popleft()
                recent_buys_usd = sum(b[1] for b in buys_deque)
            has_trades = self._has_upbit_trades.get(c, False)
            return recent_buys_usd, has_trades

    def get_recent_bithumb_buys(self, coin: str, cutoff: float) -> Tuple[float, bool]:
        """
        Thread-safe retrieval and pruning of recent taker buys for a coin on Bithumb.
        Returns (recent_buys_usd, has_trades).
        """
        c = coin.upper()
        with self._buys_lock:
            buys_deque = self._bithumb_recent_buys.get(c)
            recent_buys_usd = 0.0
            if buys_deque:
                while buys_deque and buys_deque[0][0] < cutoff:
                    buys_deque.popleft()
                recent_buys_usd = sum(b[1] for b in buys_deque)
            has_trades = self._has_bithumb_trades.get(c, False)
            return recent_buys_usd, has_trades

    # ----------------------------------------------------------- Korean Norm (EWMA)
    def _update_norm(self, coin: str, snap: OrderBookDepth5, now: float, venue: str = "upbit") -> None:
        norm_map = self._bithumb_norm if venue == "bithumb" else self._upbit_norm
        nrm = norm_map.get(coin)
        cur_depth = snap.depth_ask_usd
        cur_best_ask_usd = snap.best_ask_usd
        cur_spread = snap.spread_bps

        if nrm is None:
            norm_map[coin] = UpbitNormState(
                depth_ask=max(cur_depth, DEPTH_FLOOR_USD),
                best_ask_usd=max(cur_best_ask_usd, BEST_ASK_FLOOR_USD),
                spread_bps=max(cur_spread, 1.0),
                t=now,
            )
            return

        if isinstance(nrm, dict):
            nrm = norm_map[coin] = UpbitNormState(
                depth_ask=float(nrm["depth_ask"]),
                best_ask_usd=float(nrm["best_ask_usd"]),
                spread_bps=float(nrm["spread_bps"]),
                t=float(nrm["t"]),
            )

        dt = max(0.0, now - nrm.t)
        nrm.t = now
        a = 1.0 - math.exp(-dt / NORM_TAU)

        clamped_spread = min(cur_spread, nrm.spread_bps * 5.0)
        clamped_depth = min(cur_depth, nrm.depth_ask * 5.0)
        clamped_best_ask = min(cur_best_ask_usd, nrm.best_ask_usd * 5.0)

        nrm.depth_ask = max((1.0 - a) * nrm.depth_ask + a * clamped_depth, DEPTH_FLOOR_USD)
        nrm.best_ask_usd = max((1.0 - a) * nrm.best_ask_usd + a * clamped_best_ask, BEST_ASK_FLOOR_USD)
        nrm.spread_bps = max((1.0 - a) * nrm.spread_bps + a * clamped_spread, 0.5)

    def _update_upbit_norm(self, coin: str, snap: OrderBookDepth5, now: float) -> None:
        """Public / test fixture API: update Upbit normalized baseline (used in test suites)."""
        self._update_norm(coin, snap, now, venue="upbit")

    def _update_bithumb_norm(self, coin: str, snap: OrderBookDepth5, now: float) -> None:
        """Public / test fixture API: update Bithumb normalized baseline (used in test suites)."""
        self._update_norm(coin, snap, now, venue="bithumb")

    def get_norm_spread_bps(self, coin: str, venue: str = "upbit", default: float = 20.0) -> float:
        """Public inspection API: retrieves normalized spread in bps for a leader venue safely across object/dict states."""
        norm_map = self._bithumb_norm if venue.lower() == "bithumb" else self._upbit_norm
        return _extract_spread_bps(norm_map.get(coin), default=default)
    # ----------------------------------------------------------- Signal Logic
    def _eval_lead_lag(
        self,
        coin: str,
        leader_snap: OrderBookDepth5,
        prev_leader_snap: Optional[OrderBookDepth5],
        now: float,
        leader_ex: str = "upbit",
        upbit_snap: Optional[OrderBookDepth5] = None,
        prev_upbit_snap: Optional[OrderBookDepth5] = None,
    ) -> None:
        if leader_snap is None and upbit_snap is not None:
            leader_snap = upbit_snap
        if prev_leader_snap is None and prev_upbit_snap is not None:
            prev_leader_snap = prev_upbit_snap

        norm_map = self._bithumb_norm if leader_ex == "bithumb" else self._upbit_norm
        nrm = norm_map.get(coin)
        if not nrm or prev_leader_snap is None:
            return

        if (
            leader_snap.best_ask <= 0
            or leader_snap.best_bid <= 0
            or prev_leader_snap.best_ask <= 0
            or prev_leader_snap.best_bid <= 0
        ):
            return

        # Test fixture compatibility: unit tests (e.g. test_korean_engine_strategy) inject raw dict mocks into _upbit_norm
        if isinstance(nrm, dict):
            norm_depth = float(nrm.get("depth_ask", DEPTH_FLOOR_USD))
            norm_best_ask_usd = float(nrm.get("best_ask_usd", BEST_ASK_FLOOR_USD))
        else:
            norm_depth = nrm.depth_ask
            norm_best_ask_usd = nrm.best_ask_usd
        norm_spread = _extract_spread_bps(nrm, default=20.0)

        cur_depth = leader_snap.depth_ask_usd
        cur_spread = leader_snap.spread_bps

        trigger_type: Optional[str] = None
        drain_ratio = 0.0
        swept_usd = 0.0
        blowout_ratio = cur_spread / max(norm_spread, 0.5)

        ask_lifted = leader_snap.best_ask > prev_leader_snap.best_ask * (1.0 + 1e-7)
        ask_held = abs(leader_snap.best_ask - prev_leader_snap.best_ask) <= prev_leader_snap.best_ask * 1e-7
        bid_non_decreasing = leader_snap.best_bid >= prev_leader_snap.best_bid * (1.0 - 1e-7)

        # 1. True Depth Drain
        if ask_lifted and bid_non_decreasing:
            swept_top_level = prev_leader_snap.best_ask_usd
            top5_loss = max(0.0, prev_leader_snap.depth_ask_usd - cur_depth)
            swept_usd = max(top5_loss, swept_top_level)
            drain_ratio = max(
                swept_usd / max(norm_depth, DEPTH_FLOOR_USD),
                swept_top_level / max(norm_best_ask_usd * 2.0, DEPTH_FLOOR_USD),
            )
            if drain_ratio >= self.drain_threshold:
                trigger_type = "depth_drain"

        elif ask_held and bid_non_decreasing:
            best_ask_loss = prev_leader_snap.best_ask_usd - leader_snap.best_ask_usd
            top5_loss = prev_leader_snap.depth_ask_usd - cur_depth
            if best_ask_loss > 0 and top5_loss > 0:
                if best_ask_loss >= 0.5 * top5_loss:
                    swept_usd = top5_loss
                    drain_ratio = top5_loss / max(norm_depth, DEPTH_FLOOR_USD)
                    if drain_ratio >= self.drain_threshold:
                        trigger_type = "depth_drain"

        # 2. Upward Spread Blowout
        if (
            trigger_type is None
            and ask_lifted
            and bid_non_decreasing
            and blowout_ratio >= self.blowout_threshold
        ):
            trigger_type = "spread_blowout"
            swept_usd = max(prev_leader_snap.best_ask_usd, max(0.0, prev_leader_snap.depth_ask_usd - cur_depth))

        # Cancel vs. Sweep Discrimination (Anti-Ghosting Guard)
        recent_buys_usd = 0.0
        is_cancel_blocked = False
        saved_trigger_type = trigger_type
        saved_swept_usd = swept_usd
        if trigger_type is not None:
            cutoff = now - getattr(self, "trade_confirm_lookback_sec", 2.0)
            if leader_ex == "bithumb":
                recent_buys_usd, has_trades = self.get_recent_bithumb_buys(coin, cutoff)
            else:
                recent_buys_usd, has_trades = self.get_recent_upbit_buys(coin, cutoff)

            should_verify_trades = self.require_trade_confirmation or has_trades
            if should_verify_trades:
                ratio_needed = 0.25 if ask_held else 0.15
                min_buys_needed = max(getattr(self, "min_trade_confirm_floor_usd", 5.0), swept_usd * ratio_needed)

                if recent_buys_usd < min_buys_needed:
                    self.counters["cancels_blocked"] = self.counters.get("cancels_blocked", 0) + 1
                    is_cancel_blocked = True
                    log.debug(
                        f"[engine] Blocked false sweep / MM cancel on {leader_ex.capitalize()} {coin}: "
                        f"swept_usd=${swept_usd:.1f}, but recent taker buys=${recent_buys_usd:.1f} "
                        f"(needed ${min_buys_needed:.1f})"
                    )
                    trigger_type = None
                    swept_usd = 0.0
                    drain_ratio = 0.0

            if leader_ex == "bithumb" and trigger_type is not None:
                min_bithumb_swept = getattr(C, "BITHUMB_MIN_SWEPT_USD", 3000.0)
                if swept_usd < min_bithumb_swept and recent_buys_usd < min_bithumb_swept:
                    trigger_type = None

        with self._engine_lock:
            follower_exchanges = set(self._coin_followers.get(coin, set()))
            if not follower_exchanges:
                follower_exchanges = {ex for (ex, c) in list(self._books.keys()) if ex not in KOREAN_LEADERS and c == coin}
                if follower_exchanges:
                    self._coin_followers[coin] = follower_exchanges

        # Strategy 2: Check Fade the Fakeout on blocked cancels or micro-sweeps (on Upbit)
        fade_trig = trigger_type or saved_trigger_type
        fade_swept = swept_usd or saved_swept_usd
        if fade_trig is not None and leader_ex == "upbit":
            # Cross-Korean dual-pump check:
            # S2 ASYMMETRY DESIGN CONTRACT (Strict Anti-Pump Shield):
            # S2 detects fakeouts and quote cancellations to fade/sniper. To protect from
            # fading/shorting into a genuine nationwide Korean rally, S2 requires strict
            # dual confirmation (price parity AND volume burst). Only when BOTH price parity
            # AND aggressive taker volume are present on Bithumb is it considered an undeniable
            # dual-exchange pump, suppressing the fakeout fade.
            b_snap = self._books.get(("bithumb", coin))
            cutoff = now - 2.0
            recent_b_buys, _ = self.get_recent_bithumb_buys(coin, cutoff)
            cfg_confirm = getattr(C, "MIN_KOREAN_CONFIRM_USD", 500.0)
            min_confirm_usd = float(cfg_confirm if cfg_confirm is not None else 500.0)
            bithumb_price_parity = bool(b_snap and b_snap.mid_price > 0 and leader_snap.mid_price > 0 and b_snap.mid_price >= leader_snap.mid_price * 0.9995)
            vol_confirmed = (recent_b_buys >= min_confirm_usd and recent_b_buys > 0) if min_confirm_usd > 0 else (recent_b_buys > 0)
            korean_dual_pump = (bithumb_price_parity and vol_confirmed)

            # Macro Regime Shield: Block shorting fakeouts into a strong market pump wave
            allowed_short, short_reason = self.regime_detector.is_short_allowed(now)
            if not allowed_short:
                self.counters["regime_pump_blocked_shorts"] = self.counters.get("regime_pump_blocked_shorts", 0) + 1
                log.debug(f"[REGIME-SHIELD] S2 Fade Short blocked on {coin}: {short_reason}")
                is_fake = False
                fade_reason = None
            else:
                is_fake, fade_reason = self.strat_fade.is_fakeout_pattern(
                    is_cancel_blocked=is_cancel_blocked,
                    swept_usd=fade_swept,
                    upbit_spread_bps=cur_spread,
                    korean_confirmed=korean_dual_pump,
                )
            if is_fake:
                f_snaps = {f_ex: self._books.get((f_ex, coin)) for f_ex in follower_exchanges if self._books.get((f_ex, coin)) is not None}

                # LOCK HIERARCHY CONTRACT:
                # _is_pair_stable_for_fade is invoked as a predicate inside strat_fade while holding strat_fade._lock.
                # _get_or_create_pair_state acquires self._engine_lock.
                # Established lock acquisition order: strat_fade._lock -> self._engine_lock -> PairState._lock.
                # RULE: Never call strat_fade or strat_maker methods from inside a 'with self._engine_lock:'
                # block in KoreanEngine to prevent ABBA deadlocks.
                def _is_pair_stable_for_fade(c: str, f_ex: str, t: float) -> bool:
                    pair_st = self._get_or_create_pair_state(c, f_ex, leader_ex="upbit")
                    f_book = self._books.get((f_ex, c))
                    upbit_book = self._books.get(("upbit", c)) or leader_snap
                    cur_diff: Optional[float] = None
                    if f_book and upbit_book and f_book.mid_price > 0 and upbit_book.mid_price > 0:
                        cur_diff = (upbit_book.mid_price - f_book.mid_price) / f_book.mid_price * 1e4
                    return pair_st.is_stable(t, current_diff_bps=cur_diff)

                self.strat_fade.on_fakeout_detected(
                    coin=coin,
                    trigger_type=fade_trig,
                    swept_usd=fade_swept,
                    upbit_snap=leader_snap,
                    follower_snaps=f_snaps,
                    now=now,
                    fade_reason=fade_reason,
                    is_stable_pair_fn=_is_pair_stable_for_fade,
                    recv_ts_map=self._recv_ts,
                )

        for follower_ex in follower_exchanges:
            f_snap = self._books.get((follower_ex, coin))
            f_recv = self._recv_ts.get((follower_ex, coin), 0.0)
            if f_snap is None:
                continue

            book_age_sec = now - f_recv
            if book_age_sec > 2.0:
                continue

            st = self._get_or_create_pair_state(coin, follower_ex, leader_ex=leader_ex)

            lead_mid = leader_snap.mid_price
            f_mid = f_snap.mid_price
            if lead_mid <= 0 or f_mid <= 0:
                continue

            raw_diff_bps = (lead_mid - f_mid) / f_mid * 1e4

            with self._engine_lock:
                has_open = (st.open_signal is not None)

            if trigger_type and not has_open and st.is_stable(now, current_diff_bps=raw_diff_bps):
                self._check_entry(
                    coin=coin,
                    follower_ex=follower_ex,
                    st=st,
                    upbit_snap=leader_snap,
                    f_snap=f_snap,
                    f_book_age_ms=book_age_sec * 1000.0,
                    norm_depth=norm_depth,
                    norm_spread=norm_spread,
                    swept_usd=swept_usd,
                    trigger_type=trigger_type,
                    drain_ratio=drain_ratio,
                    blowout_ratio=blowout_ratio,
                    now=now,
                    recent_buys_usd=recent_buys_usd,
                    leader_ex=leader_ex,
                    leader_snap=leader_snap,
                )

            with self._engine_lock:
                has_open = (st.open_signal is not None)

            if not has_open:
                st.learn(raw_diff_bps, now)

    def _check_entry(
        self,
        coin: str,
        follower_ex: str,
        st: PairState,
        upbit_snap: Optional[OrderBookDepth5] = None,
        f_snap: Optional[OrderBookDepth5] = None,
        f_book_age_ms: float = 0.0,
        norm_depth: float = 0.0,
        norm_spread: float = 0.0,
        swept_usd: float = 0.0,
        trigger_type: str = "depth_drain",
        drain_ratio: float = 0.0,
        blowout_ratio: float = 0.0,
        now: float = 0.0,
        recent_buys_usd: float = 0.0,
        leader_ex: str = "upbit",
        leader_snap: Optional[OrderBookDepth5] = None,
    ) -> None:
        leader_snap_to_use = leader_snap if leader_snap is not None else upbit_snap
        if leader_snap_to_use is None or f_snap is None:
            return

        # Macro Market Regime & Flash Dump Circuit Breaker
        long_allowed, long_reason = self.regime_detector.is_long_allowed(now)
        if not long_allowed:
            self.counters["regime_dump_blocked"] = self.counters.get("regime_dump_blocked", 0) + 1
            log.info(f"[REGIME-VETO] S1 Long blocked on {coin} @ {follower_ex}: {long_reason}")
            return

        pair_cfg = self.get_pair_threshold_config(coin, follower_ex, leader_ex=leader_ex)
        seq_num = next(self._seq)
        sig = self.strat_lead_lag.evaluate_entry(
            coin=coin,
            follower_ex=follower_ex,
            st=st,
            upbit_snap=leader_snap_to_use,
            f_snap=f_snap,
            f_book_age_ms=f_book_age_ms,
            norm_depth=norm_depth,
            norm_spread=norm_spread,
            swept_usd=swept_usd,
            trigger_type=trigger_type,
            drain_ratio=drain_ratio,
            blowout_ratio=blowout_ratio,
            now=now,
            recent_buys_usd=recent_buys_usd,
            pair_cfg=pair_cfg,
            seq_num=seq_num,
            pool_state=self.pool_state,
            profile_manager=self.profile_manager,
        )
        if sig is None:
            return

        # Attach real-time market regime snapshot telemetry
        reg_snap = self.regime_detector.get_regime(now)
        sig.market_regime = reg_snap.regime.value
        sig.market_breadth_pct = round(reg_snap.breadth_pct_300s, 1)
        sig.btc_ret_300s_bps = round(reg_snap.btc_ret_300s_bps, 1)

        # Check cross-Korean confirmation and spread
        other_ex = "bithumb" if leader_ex == "upbit" else "upbit"
        other_snap = self._books.get((other_ex, coin))
        has_other_korean = (other_snap is not None and other_snap.mid_price > 0 and leader_snap_to_use.mid_price > 0)
        if has_other_korean:
            sig.korean_co_listed = True
            cutoff = now - 2.0
            cfg_confirm = getattr(C, "MIN_KOREAN_CONFIRM_USD", 500.0)
            min_confirm_usd = float(cfg_confirm if cfg_confirm is not None else 500.0)
            # S1 ASYMMETRY DESIGN CONTRACT (Early Breakout Leader-Follower Sniper):
            # S1 snipes global perps when Korean leaders break out.
            # Confirmation is intentionally permissive (OR): either price parity exists
            # OR taker buying is already active on the peer Korean venue, validating leadership.
            if leader_ex == "upbit":
                recent_other_buys, _ = self.get_recent_bithumb_buys(coin, cutoff)
                sig.korean_spread_bps = round((other_snap.mid_price - leader_snap_to_use.mid_price) / leader_snap_to_use.mid_price * 1e4, 1)
                parity = (other_snap.mid_price >= leader_snap_to_use.mid_price * 0.9995)
                # Volume confirmation requires non-dust flow (>= min_confirm_usd in prod, > 0 in telemetry mode)
                vol_confirmed = (recent_other_buys >= min_confirm_usd and recent_other_buys > 0) if min_confirm_usd > 0 else (recent_other_buys > 0)
                sig.korean_confirmed = parity or vol_confirmed
            else:
                recent_other_buys, _ = self.get_recent_upbit_buys(coin, cutoff)
                sig.korean_spread_bps = round((leader_snap_to_use.mid_price - other_snap.mid_price) / other_snap.mid_price * 1e4, 1)
                parity = (leader_snap_to_use.mid_price >= other_snap.mid_price * 0.9995)
                vol_confirmed = (recent_other_buys >= min_confirm_usd and recent_other_buys > 0) if min_confirm_usd > 0 else (recent_other_buys > 0)
                sig.korean_confirmed = parity or vol_confirmed

            # Cross-Korean Confirmation Gate:
            # In live Battle mode (real money execution), block unconfirmed pumps if require_korean_confirmation is on.
            # In Incubator/Telemetry mode (default), record all signals to collect empirical evidence for quantitative reports.
            req_confirm = getattr(self, "require_korean_confirmation", getattr(C, "REQUIRE_KOREAN_CONFIRMATION", False))
            strict_gate = getattr(C, "STRICT_KOREAN_GATE", False)
            if not sig.korean_confirmed and ((sig.is_actionable and req_confirm) or strict_gate):
                self.counters["korean_unconfirmed_blocked"] = self.counters.get("korean_unconfirmed_blocked", 0) + 1
                log.debug(
                    f"[engine] Blocked unconfirmed {leader_ex.capitalize()} signal on {coin}: "
                    f"peer {other_ex} did not confirm (spread={sig.korean_spread_bps:+.1f} bps, actionable={sig.is_actionable})"
                )
                return

        with self._engine_lock:
            if st.open_signal is not None:
                return
            st.open_signal = sig
            self._active_signals[coin].append(sig)

        self.counters["signals_triggered"] += 1
        dual_icon = "⚡" if sig.korean_confirmed else ""
        dual_suffix = "+DUAL" if sig.korean_confirmed else ""
        if sig.is_actionable:
            self.counters["signals_battle"] += 1
            tag = f"[SIGNAL-BATTLE ({leader_ex.upper()}{dual_suffix})] ⚔️{dual_icon}"
        else:
            self.counters["signals_incubator"] += 1
            tag = f"[SIGNAL-INCUBATOR ({leader_ex.upper()}{dual_suffix})] 🧪{dual_icon}"

        buys_str = f", Buys: ${sig.leader_recent_buys_usd:.0f}" if sig.leader_recent_buys_usd > 0 else ""
        kr_str = (
            f" | KR-Conf: {sig.korean_confirmed} ({sig.korean_spread_bps:+.1f}bps)"
            if sig.korean_co_listed
            else " | KR-Solo"
        )
        log.info(
            f"{tag} {sig.coin} ({sig.follower_contract}) | {leader_ex} -> {sig.follower_exchange} | "
            f"Trig: {sig.trigger_type} (Swept: ${sig.leader_swept_usd:.0f}{buys_str}) | "
            f"Edge: +{sig.gross_exec_edge_bps:.1f} bps | Z: {sig.dev_z:.1f}σ | "
            f"Size: ${sig.suggested_size_usd:.0f} (Cap15%: ${sig.stealth_capacity_usd:.0f}){kr_str}"
        )

    # ----------------------------------------------------------- Tracking & Closure
    def _track_active_signals_for_coin(self, coin: str, now: float) -> None:
        with self._engine_lock:
            signals = list(self._active_signals.get(coin, []))
        if not signals:
            return
        for sig in signals:
            if sig.closed:
                continue
            lead_snap = self._books.get((sig.leader_exchange, coin)) or self._books.get(("upbit", coin))
            if not lead_snap:
                continue
            f_snap = self._books.get((sig.follower_exchange, coin))
            if f_snap is not None:
                self._track_one_signal(sig, lead_snap, f_snap, now)

    def _track_follower_move(self, coin: str, follower_ex: str, f_snap: OrderBookDepth5, now: float) -> None:
        with self._engine_lock:
            signals = list(self._active_signals.get(coin, []))
        if not signals:
            return
        for sig in signals:
            if sig.follower_exchange == follower_ex and not sig.closed:
                lead_snap = self._books.get((sig.leader_exchange, coin)) or self._books.get(("upbit", coin))
                if not lead_snap:
                    continue
                self._track_one_signal(sig, lead_snap, f_snap, now)

    def _track_one_signal(
        self,
        sig: SunriseSignal,
        leader_snap: OrderBookDepth5,
        f_snap: OrderBookDepth5,
        now: float,
    ) -> None:
        with self._engine_lock:
            if sig.closed:
                return

            dt = round(now - sig.open_ts, 3)

            # 1. Mid-to-Mid move on follower and leader (bps)
            f_mid_move = (
                (f_snap.mid_price - sig.follower_mid) / sig.follower_mid * 1e4
                if sig.follower_mid > 0
                else 0.0
            )
            lead_mid_move = (
                (leader_snap.mid_price - sig.leader_mid) / sig.leader_mid * 1e4
                if sig.leader_mid > 0
                else 0.0
            )

            # 2. True executable move on follower: Entry at VWAP ask -> Exit at current VWAP bid (bps)
            entry_px = getattr(sig, "follower_entry_px", 0.0) or sig.follower_best_ask
            exit_px = f_snap.vwap_bid_usd(sig.suggested_size_usd) if sig.suggested_size_usd > 0 else f_snap.best_bid
            f_exec_move = (exit_px - entry_px) / entry_px * 1e4 if entry_px > 0 else 0.0
            cur_net_pnl = round(f_exec_move - self.roundtrip_fee_bps, 2)

            # 3. Update MFE / MAE excursion metrics
            if cur_net_pnl > sig.mfe_exec_pnl_bps:
                sig.mfe_exec_pnl_bps = cur_net_pnl
                sig.time_to_peak_sec = dt
            if cur_net_pnl < sig.mae_exec_pnl_bps:
                sig.mae_exec_pnl_bps = cur_net_pnl
            if f_mid_move > sig.mfe_mid_move_bps:
                sig.mfe_mid_move_bps = round(f_mid_move, 2)

            # 4. Remaining dislocation edge
            cur_diff_bps = (
                (leader_snap.mid_price - f_snap.mid_price) / f_snap.mid_price * 1e4
                if f_snap.mid_price > 0
                else 0.0
            )
            rem_edge = (cur_diff_bps - sig.kimchi_baseline_bps) - self.taker_fee_bps

            if (not sig.path or dt > sig.path[-1][0]) and len(sig.path) < 50:
                sig.path.append([
                    dt,
                    round(lead_mid_move, 2),
                    round(f_mid_move, 2),
                    cur_net_pnl,
                    round(rem_edge, 2),
                ])

            # Closure & Stop-Loss evaluation:
            pair_cfg = self.get_pair_threshold_config(sig.coin, sig.follower_exchange, leader_ex=sig.leader_exchange)
            hold_sec = getattr(sig, "max_hold_sec", self.max_hold_sec) or self.max_hold_sec
            default_decay = max(self.alpha_decay_sec, hold_sec * 0.5) if hold_sec > self.max_hold_sec else self.alpha_decay_sec
            pair_decay_sec = float(pair_cfg.get("alpha_decay_sec", default_decay))
            sigma = max(sig.pair_sigma_bps, 2.0)

            leader_drop_bps = (
                (sig.leader_mid - leader_snap.mid_price) / sig.leader_mid * 1e4
                if sig.leader_mid > 0
                else 0.0
            )
            cur_dislocation_bps = cur_diff_bps - sig.kimchi_baseline_bps

            close_reason, stop_type = evaluate_exit_reason(
                cur_net_pnl=cur_net_pnl,
                net_exec_edge_bps=sig.net_exec_edge_bps,
                rem_edge=rem_edge,
                dt=dt,
                hold_sec=hold_sec,
                pair_decay_sec=pair_decay_sec,
                sigma=sigma,
                upbit_drop_bps=leader_drop_bps,
                cur_dislocation_bps=cur_dislocation_bps,
                stop_invalidation_bps=self.stop_invalidation_bps,
                stop_adverse_pnl_bps=self.stop_adverse_pnl_bps,
            )

            if close_reason is not None:
                if stop_type is not None:
                    sig.stop_hit = True
                    sig.stop_type = stop_type
                    loss = abs(min(0.0, cur_net_pnl)) if stop_type == "invalidation" else abs(cur_net_pnl)
                    sig.stop_loss_bps = round(loss, 2)
                self._close_signal(sig, now, close_reason, f_mid_move, f_exec_move)

    def _close_signal(
        self,
        sig: SunriseSignal,
        now: float,
        reason: str,
        f_mid_move_bps: float,
        f_exec_move_bps: float,
    ) -> None:
        with self._engine_lock:
            if sig.closed:
                return
            sig.closed = True
            sig.close_ts = round(now, 3)
            sig.duration_sec = round(now - sig.open_ts, 3)
            sig.close_reason = reason
            sig.captured_mid_move_bps = round(f_mid_move_bps, 2)
            sig.captured_exec_move_bps = round(f_exec_move_bps, 2)
            sig.net_pnl_bps = round(f_exec_move_bps - self.roundtrip_fee_bps, 2)

            if reason == "converged":
                self.counters["signals_converged"] += 1
            elif reason == "stop_invalidation":
                self.counters["signals_stop_loss_invalidation"] += 1
            elif reason == "stop_adverse_pnl":
                self.counters["signals_stop_loss_adverse_pnl"] += 1
            elif reason == "stop_decay":
                self.counters["signals_stop_loss_decay"] += 1
            elif reason == "reversed":
                self.counters["signals_reversed"] += 1
            else:
                self.counters["signals_timeout"] += 1

            pair_key = (sig.coin, sig.follower_exchange)
            st = self._pairs.get(pair_key)
            if not st:
                st = self._pairs.get((sig.coin, sig.follower_exchange, sig.leader_exchange))
            if st and st.open_signal is not None and getattr(st.open_signal, "id", None) == sig.id:
                st.open_signal = None

            if sig.coin in self._active_signals:
                if sig in self._active_signals[sig.coin]:
                    self._active_signals[sig.coin].remove(sig)
                if not self._active_signals[sig.coin]:
                    self._active_signals.pop(sig.coin, None)

        try:
            self.emit(sig.to_dict())
        except Exception as e:
            log.error(f"[engine] Failed to emit closed signal {getattr(sig, 'id', '')}: {e}")

    def sweep_timeouts(self, now: float) -> None:
        """
        Sweeps and closes any active signals that exceeded max_hold_sec even if no new ticks arrived.
        Also sweeps Strategy 2 (fade fakeout) and Strategy 3 (maker void) timeouts.
        Prevents open positions or resting limit orders from lingering on dead streams.
        """
        with self._engine_lock:
            active_items = [(coin, list(signals)) for coin, signals in self._active_signals.items()]

        for coin, signals in active_items:
            for sig in signals:
                try:
                    hold_sec = getattr(sig, "max_hold_sec", self.max_hold_sec) or self.max_hold_sec
                    if not sig.closed and (now - sig.open_ts) >= hold_sec:
                        f_snap = self._books.get((sig.follower_exchange, coin))
                        if f_snap:
                            f_mid_move = (
                                (f_snap.mid_price - sig.follower_mid) / sig.follower_mid * 1e4
                                if sig.follower_mid > 0
                                else 0.0
                            )
                            entry_px = getattr(sig, "follower_entry_px", 0.0) or sig.follower_best_ask
                            exit_px = f_snap.vwap_bid_usd(sig.suggested_size_usd) if sig.suggested_size_usd > 0 else f_snap.best_bid
                            f_exec_move = (exit_px - entry_px) / entry_px * 1e4 if entry_px > 0 else 0.0
                        else:
                            f_mid_move = 0.0
                            f_exec_move = 0.0
                        self._close_signal(sig, now, "timeout", f_mid_move, f_exec_move)
                except Exception as e:
                    log.error(f"[engine] Error sweeping timeout for signal {getattr(sig, 'id', '')}: {e}")

        # Multi-strategy timeout sweeps:
        # Strategy 2: Fade the Fakeout (sweep expired shorts if streams disconnected)
        self.strat_fade.sweep_timeouts(now)

        # Strategy 3: Maker Liquidity Harvesting (sweep expired resting orders & filled voids)
        self.strat_maker.sweep_timeouts(now)

        # Pruning of idle follower spread EWMA baselines (Item 2: idle > 1 hour)
        with self._engine_lock:
            dead_ewma = [k for k, v in self._follower_spread_ewma.items() if (now - v.last_ts) > 3600.0]
            for k in dead_ewma:
                del self._follower_spread_ewma[k]