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
    ActiveBetaNeutralPair,
    ActiveCrossKoreanPosition,
    ActiveFadePosition,
    ActiveImbalancePosition,
    ActiveMakerVoid,
    ActiveRubberBandPosition,
    BetaNeutralStrategy,
    CrossKoreanTriangleStrategy,
    FadeFakeoutStrategy,
    KoreanLeadLagStrategy,
    MakerVoidStrategy,
    OrderbookImbalanceStrategy,
    RubberBandSniperStrategy,
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
        runner_enabled: Optional[bool] = None,
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
        self.runner_enabled = runner_enabled if runner_enabled is not None else getattr(C, "S1_RUNNER_ENABLED", True)
        self.pool_state = pool_state
        self.profile_manager = profile_manager

        # Internal reentrant lock for thread-safe cross-thread execution
        self._engine_lock = threading.RLock()

        # (exchange, coin) -> OrderBookDepth5
        self._books: Dict[Tuple[str, str], OrderBookDepth5] = {}
        self._recv_ts: Dict[Tuple[str, str], float] = {}

        # O(1) follower lookup index: coin -> set of follower exchanges
        self._coin_followers: Dict[str, Set[str]] = defaultdict(set)

        # Upbit recent taker buys/sells: coin -> deque of (ts, trade_usd)
        self._buys_lock = threading.Lock()
        self._upbit_recent_buys: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=2000))
        self._upbit_recent_sells: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=2000))
        self._has_upbit_trades: Dict[str, bool] = defaultdict(bool)
        self._bithumb_recent_buys: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=2000))
        self._bithumb_recent_sells: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=2000))
        self._has_bithumb_trades: Dict[str, bool] = defaultdict(bool)
        self._upbit_mid_history: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=500))

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
            "trades_upbit_sells": 0,
            "trades_bithumb": 0,
            "trades_bithumb_sells": 0,
            "cancels_blocked": 0,
            "cancels_blocked_sells": 0,
            "korean_unconfirmed_blocked": 0,
            "signals_triggered": 0,
            "signals_long": 0,
            "signals_short": 0,
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
            "rubber_band_triggered": 0,
            "rubber_band_converged": 0,
            "rubber_band_stopped": 0,
            "rubber_band_timeout": 0,
            "regime_dump_blocked": 0,
            "regime_pump_blocked_shorts": 0,
        }

        # Macro Market Regime & Altcoin Breadth Engine
        self.regime_detector = MarketRegimeDetector()
        self.regime_benchmark_exchange = getattr(C, "REGIME_BENCHMARK_EXCHANGE", "binance").lower()

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
            hunter_mode=getattr(C, "S3_HUNTER_MODE", False),
            align_trend=getattr(C, "S3_ALIGN_TREND", True),
            require_upbit=getattr(C, "S3_REQUIRE_UPBIT", True),
            max_active_voids=getattr(C, "MAX_PORTFOLIO_CONCURRENT_POSITIONS", 10),
            max_total_exposure_usd=getattr(C, "MAX_PORTFOLIO_EXPOSURE_USD", 5000.0),
            counters=self.counters,
        )

        # Strategy 4: Predatory Rebound Maker (Rubber Band)
        self.strat_rubber_band = RubberBandSniperStrategy(
            emit_callback=self._on_strategy_emit,
            min_order_usd=self.min_order_usd,
            max_position_budget_usd=self.max_position_budget_usd,
            stealth_depth_fraction=self.stealth_fraction,
            min_dislocation_bps=getattr(C, "MIN_RUBBER_BAND_DISLOCATION_BPS", 100.0),
            min_z_score=getattr(C, "RUBBER_BAND_MIN_Z_SCORE", 3.0),
            maker_discount_bps=getattr(C, "RUBBER_BAND_MAKER_DISCOUNT_BPS", 25.0),
            maker_ttl_sec=getattr(C, "RUBBER_BAND_MAKER_TTL_SEC", 2.5),
            target_rebound_bps=getattr(C, "TARGET_RUBBER_BAND_BPS", 35.0),
            stop_loss_bps=getattr(C, "STOP_RUBBER_BAND_BPS", 25.0),
            maker_rebate_bps=getattr(C, "MAKER_REBATE_BPS", 0.0),
            taker_fee_bps=self.taker_fee_bps,
            max_hold_sec=getattr(C, "RUBBER_BAND_MAX_HOLD_SEC", 30.0),
            alpha_decay_sec=getattr(C, "RUBBER_BAND_ALPHA_DECAY_SEC", 15.0),
            cooldown_sec=getattr(C, "RUBBER_BAND_COOLDOWN_SEC", 20.0),
            max_active_positions=getattr(C, "MAX_PORTFOLIO_CONCURRENT_POSITIONS", 10),
            max_total_exposure_usd=getattr(C, "MAX_PORTFOLIO_EXPOSURE_USD", 5000.0),
            enabled=getattr(C, "S4_RUBBER_BAND_ENABLED", False),
            counters=self.counters,
        )

        # Strategy 5: Cross-Korean Triangle (Bithumb Radar) [Renumbered from S7]
        self.strat_triangle = CrossKoreanTriangleStrategy(
            emit_callback=self._on_strategy_emit,
            min_order_usd=self.min_order_usd,
            max_position_budget_usd=self.max_position_budget_usd,
            stealth_depth_fraction=self.stealth_fraction,
            min_bithumb_lead_bps=getattr(C, "MIN_BITHUMB_LEAD_BPS", 25.0),
            min_bithumb_buys_usd=getattr(C, "MIN_BITHUMB_BUYS_USD", 2000.0),
            min_volume_share=getattr(C, "BITHUMB_MIN_VOLUME_SHARE", 0.30),
            min_upbit_confirm_bps=getattr(C, "MIN_UPBIT_CONFIRM_BPS", 3.0),
            excluded_coins=getattr(C, "EXCLUDED_TRIANGLE_COINS", {"BTC", "ETH", "SOL", "XRP", "DOGE"}),
            roundtrip_fee_bps=self.roundtrip_fee_bps,
            max_active_positions=getattr(C, "MAX_PORTFOLIO_CONCURRENT_POSITIONS", 5),
            max_total_exposure_usd=getattr(C, "MAX_PORTFOLIO_EXPOSURE_USD", 5000.0),
            enabled=getattr(C, "S5_CROSS_KOREAN_ENABLED", getattr(C, "S7_CROSS_KOREAN_ENABLED", False)),
            counters=self.counters,
        )

        # Archived Strategy S5 (Legacy OBIV Imbalance - Dormant)
        self.strat_imbalance = OrderbookImbalanceStrategy(
            emit_callback=self._on_strategy_emit,
            min_order_usd=self.min_order_usd,
            max_position_budget_usd=self.max_position_budget_usd,
            stealth_depth_fraction=self.stealth_fraction,
            min_upbit_imbalance=getattr(C, "MIN_UPBIT_IMBALANCE", 0.60),
            min_delta_imbalance_5s=getattr(C, "MIN_DELTA_IMBALANCE_5S", 0.35),
            max_follower_imbalance=getattr(C, "MAX_FOLLOWER_IMBALANCE", 0.25),
            roundtrip_fee_bps=self.roundtrip_fee_bps,
            max_active_positions=getattr(C, "MAX_PORTFOLIO_CONCURRENT_POSITIONS", 10),
            max_total_exposure_usd=getattr(C, "MAX_PORTFOLIO_EXPOSURE_USD", 5000.0),
            enabled=getattr(C, "S5_IMBALANCE_ENABLED", False),
            counters=self.counters,
        )

        # Archived Strategy S6 (Legacy Beta-Neutral Pairs - Dormant)
        self.strat_beta_neutral = BetaNeutralStrategy(
            emit_callback=self._on_strategy_emit,
            min_order_usd=self.min_order_usd,
            max_position_budget_usd=self.max_position_budget_usd,
            stealth_depth_fraction=self.stealth_fraction,
            default_beta=getattr(C, "DEFAULT_BETA_HEDGE", 1.3),
            roundtrip_fee_bps=self.roundtrip_fee_bps,
            max_active_pairs=getattr(C, "MAX_PORTFOLIO_CONCURRENT_POSITIONS", 5),
            max_total_exposure_usd=getattr(C, "MAX_PORTFOLIO_EXPOSURE_USD", 5000.0),
            enabled=getattr(C, "S6_BETA_NEUTRAL_ENABLED", False),
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

        # Macro Market Regime Benchmark:
        # Strictly feed ticks from the benchmark follower exchange (default: "binance").
        # Upbit/Bithumb MUST NEVER be ingested into the BTC velocity or alt breadth buffer
        # because KRW Kimchi premium (+150-300 bps) and multi-exchange tick interleaving
        # create synthetic price jumps of 50+ bps, falsely tripping the flash dump circuit breaker.
        if ex == self.regime_benchmark_exchange:
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
            self._upbit_mid_history[coin].append((now, snap.mid_price))
            self.strat_imbalance.on_upbit_book_tick(coin, snap, now)
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
            reg_snap = self.regime_detector.get_regime(now)
            cur_regime = reg_snap.regime.value
            self.strat_maker.check_void_opportunity(
                ex, coin, snap, upbit_snap, now,
                norm_spread_bps=f_norm_spread,
                recv_ts=f_recv,
                is_warmed_up=is_f_warmed,
                market_regime=cur_regime,
            )

            # Strategy 4: Predatory Rebound Maker (Rubber Band)
            if self.strat_rubber_band.enabled:
                self.strat_rubber_band.on_regime_update(cur_regime, now)
                self.strat_rubber_band.on_follower_tick(ex, coin, snap, now, cur_upbit_mid=cur_upbit_mid)
                pair_cfg = self.get_pair_threshold_config(coin, ex, leader_ex="upbit")
                sigma_val = float(pair_cfg.get("sigma_bps", 35.0))
                self.strat_rubber_band.check_rubber_band_opportunity(
                    follower_ex=ex,
                    coin=coin,
                    follower_snap=snap,
                    upbit_snap=upbit_snap,
                    now=now,
                    regime_snapshot=reg_snap,
                    recv_ts=f_recv,
                    pair_sigma_bps=sigma_val,
                )

            # Strategy 5: Cross-Korean Triangle (Bithumb Radar) [Renumbered from S7]
            if self.strat_triangle.enabled:
                recent_b_buys, _ = self.get_recent_bithumb_buys(coin, now - 2.0)
                recent_u_buys, _ = self.get_recent_upbit_buys(coin, now - 2.0)
                upbit_ret_2s = self.get_upbit_return_bps(coin, now, window_sec=2.0)
                vol_share = self.get_bithumb_volume_share(coin, now, window_sec=300.0)

                self.strat_triangle.on_follower_tick(ex, coin, snap, now)
                self.strat_triangle.check_triangle_opportunity(
                    follower_ex=ex,
                    coin=coin,
                    bithumb_snap=bithumb_snap,
                    upbit_snap=upbit_snap,
                    follower_snap=snap,
                    now=now,
                    market_regime=cur_regime,
                    recent_bithumb_buys_usd=recent_b_buys,
                    recv_ts=f_recv,
                    bithumb_volume_share=vol_share,
                    upbit_ret_2s_bps=upbit_ret_2s,
                    recent_upbit_buys_usd=recent_u_buys,
                )

            # Archived Strategy: Orderbook Imbalance Front-Runner (OBIV - Dormant)
            if self.strat_imbalance.enabled:
                self.strat_imbalance.on_follower_tick(ex, coin, snap, now)
                self.strat_imbalance.check_imbalance_opportunity(
                    follower_ex=ex,
                    coin=coin,
                    follower_snap=snap,
                    upbit_snap=upbit_snap,
                    now=now,
                    market_regime=cur_regime,
                    recv_ts=f_recv,
                )

            # Archived Strategy: Beta-Neutral Pairs Trading (Alt Long + BTC Short - Dormant)
            if self.strat_beta_neutral.enabled:
                btc_snap = self._books.get((self.regime_benchmark_exchange, "BTC"))
                if btc_snap is not None:
                    self.strat_beta_neutral.on_tick_update(ex, coin, snap, btc_snap, now)
                    self.strat_beta_neutral.check_pair_opportunity(
                        follower_ex=ex,
                        coin=coin,
                        follower_snap=snap,
                        upbit_snap=upbit_snap,
                        btc_snap=btc_snap,
                        now=now,
                        recv_ts=f_recv,
                    )

    def on_upbit_trade(self, coin: str, trade_usd: float, is_taker_buy: bool, ts: float) -> None:
        """
        Real-time trade stream ingestion from Upbit.
        Records taker buys/sells to verify liquidity sweeps and reject MM quote cancellations.
        Thread-safe across concurrent WebSocket worker threads.
        """
        self.regime_detector.on_korean_trade(trade_usd, is_taker_buy, ts)
        c = coin.upper()
        with self._buys_lock:
            self.counters["trades_upbit"] = self.counters.get("trades_upbit", 0) + 1
            self._has_upbit_trades[c] = True
            if is_taker_buy and trade_usd > 0.0:
                self._upbit_recent_buys[c].append((ts, trade_usd))
            elif (not is_taker_buy) and trade_usd > 0.0:
                self.counters["trades_upbit_sells"] = self.counters.get("trades_upbit_sells", 0) + 1
                self._upbit_recent_sells[c].append((ts, trade_usd))

    def on_bithumb_trade(self, coin: str, trade_usd: float, is_taker_buy: bool, ts: float) -> None:
        """
        Real-time trade stream ingestion from Bithumb.
        Records taker buys/sells to verify Bithumb liquidity sweeps and volume explosions.
        Thread-safe across concurrent WebSocket worker threads.
        """
        self.regime_detector.on_korean_trade(trade_usd, is_taker_buy, ts)
        c = coin.upper()
        with self._buys_lock:
            self.counters["trades_bithumb"] = self.counters.get("trades_bithumb", 0) + 1
            self._has_bithumb_trades[c] = True
            if is_taker_buy and trade_usd > 0.0:
                self._bithumb_recent_buys[c].append((ts, trade_usd))
            elif (not is_taker_buy) and trade_usd > 0.0:
                self.counters["trades_bithumb_sells"] = self.counters.get("trades_bithumb_sells", 0) + 1
                self._bithumb_recent_sells[c].append((ts, trade_usd))

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

    def get_recent_upbit_sells(self, coin: str, cutoff: float) -> Tuple[float, bool]:
        """
        Thread-safe retrieval and pruning of recent taker sells for a coin on Upbit.
        Returns (recent_sells_usd, has_trades).
        """
        c = coin.upper()
        with self._buys_lock:
            sells_deque = self._upbit_recent_sells.get(c)
            recent_sells_usd = 0.0
            if sells_deque:
                while sells_deque and sells_deque[0][0] < cutoff:
                    sells_deque.popleft()
                recent_sells_usd = sum(s[1] for s in sells_deque)
            has_trades = self._has_upbit_trades.get(c, False)
            return recent_sells_usd, has_trades

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

    def get_recent_bithumb_sells(self, coin: str, cutoff: float) -> Tuple[float, bool]:
        """
        Thread-safe retrieval and pruning of recent taker sells for a coin on Bithumb.
        Returns (recent_sells_usd, has_trades).
        """
        c = coin.upper()
        with self._buys_lock:
            sells_deque = self._bithumb_recent_sells.get(c)
            recent_sells_usd = 0.0
            if sells_deque:
                while sells_deque and sells_deque[0][0] < cutoff:
                    sells_deque.popleft()
                recent_sells_usd = sum(s[1] for s in sells_deque)
            has_trades = self._has_bithumb_trades.get(c, False)
            return recent_sells_usd, has_trades

    def get_upbit_return_bps(self, coin: str, now: float, window_sec: float = 2.0) -> float:
        """Returns Upbit mid-price return in bps over approximately window_sec."""
        history = self._upbit_mid_history.get(coin)
        if not history or len(history) < 2:
            return 0.0
        cur_t, cur_px = history[-1]
        if (now - cur_t) > window_sec:
            return 0.0
        target_t = now - window_sec
        past_px = history[0][1]
        for t, px in history:
            if t <= target_t:
                past_px = px
            else:
                break
        if past_px <= 0.0 or cur_px <= 0.0:
            return 0.0
        return (cur_px - past_px) / past_px * 1e4

    def get_bithumb_volume_share(self, coin: str, now: float, window_sec: float = 300.0) -> Optional[float]:
        """Calculates Bithumb's volume share vs Upbit over the rolling window."""
        b_buys, _ = self.get_recent_bithumb_buys(coin, now - window_sec)
        u_buys, _ = self.get_recent_upbit_buys(coin, now - window_sec)
        if (b_buys + u_buys) <= 0.0:
            return None
        if u_buys <= 0.0:
            return 1.0 if b_buys > 0 else None
        return round(b_buys / u_buys, 3)

    # ----------------------------------------------------------- Korean Norm (EWMA)
    def _update_norm(self, coin: str, snap: OrderBookDepth5, now: float, venue: str = "upbit") -> None:
        norm_map = self._bithumb_norm if venue == "bithumb" else self._upbit_norm
        nrm = norm_map.get(coin)
        cur_depth_ask = snap.depth_ask_usd
        cur_best_ask_usd = snap.best_ask_usd
        cur_depth_bid = snap.depth_bid_usd
        cur_best_bid_usd = snap.best_bid_usd
        cur_spread = snap.spread_bps

        if nrm is None:
            norm_map[coin] = UpbitNormState(
                depth_ask=max(cur_depth_ask, DEPTH_FLOOR_USD),
                best_ask_usd=max(cur_best_ask_usd, BEST_ASK_FLOOR_USD),
                spread_bps=max(cur_spread, 1.0),
                t=now,
                depth_bid=max(cur_depth_bid, DEPTH_FLOOR_USD),
                best_bid_usd=max(cur_best_bid_usd, BEST_ASK_FLOOR_USD),
            )
            return

        if isinstance(nrm, dict):
            nrm = norm_map[coin] = UpbitNormState(
                depth_ask=float(nrm.get("depth_ask", DEPTH_FLOOR_USD)),
                best_ask_usd=float(nrm.get("best_ask_usd", BEST_ASK_FLOOR_USD)),
                spread_bps=float(nrm.get("spread_bps", 20.0)),
                t=float(nrm.get("t", now)),
                depth_bid=float(nrm.get("depth_bid", nrm.get("depth_ask", DEPTH_FLOOR_USD))),
                best_bid_usd=float(nrm.get("best_bid_usd", nrm.get("best_ask_usd", BEST_ASK_FLOOR_USD))),
            )

        dt = max(0.0, now - nrm.t)
        nrm.t = now
        a = 1.0 - math.exp(-dt / NORM_TAU)

        clamped_spread = min(cur_spread, nrm.spread_bps * 5.0)
        clamped_depth_ask = min(cur_depth_ask, nrm.depth_ask * 5.0)
        clamped_best_ask = min(cur_best_ask_usd, nrm.best_ask_usd * 5.0)
        clamped_depth_bid = min(cur_depth_bid, (getattr(nrm, "depth_bid", 0.0) or cur_depth_bid) * 5.0)
        clamped_best_bid = min(cur_best_bid_usd, (getattr(nrm, "best_bid_usd", 0.0) or cur_best_bid_usd) * 5.0)

        nrm.depth_ask = max((1.0 - a) * nrm.depth_ask + a * clamped_depth_ask, DEPTH_FLOOR_USD)
        nrm.best_ask_usd = max((1.0 - a) * nrm.best_ask_usd + a * clamped_best_ask, BEST_ASK_FLOOR_USD)
        nrm.depth_bid = max((1.0 - a) * (getattr(nrm, "depth_bid", 0.0) or cur_depth_bid) + a * clamped_depth_bid, DEPTH_FLOOR_USD)
        nrm.best_bid_usd = max((1.0 - a) * (getattr(nrm, "best_bid_usd", 0.0) or cur_best_bid_usd) + a * clamped_best_bid, BEST_ASK_FLOOR_USD)
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

    @property
    def current_regime(self) -> str:
        """Current real-time macro market regime ('STRONG_PUMP', 'BULL_DRIFT', 'NEUTRAL', 'BEAR_DRIFT', 'FLASH_DUMP')."""
        return self.regime_detector.get_regime().regime.value
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
            norm_depth_ask = float(nrm.get("depth_ask", DEPTH_FLOOR_USD))
            norm_best_ask_usd = float(nrm.get("best_ask_usd", BEST_ASK_FLOOR_USD))
            norm_depth_bid = float(nrm.get("depth_bid", norm_depth_ask))
            norm_best_bid_usd = float(nrm.get("best_bid_usd", norm_best_ask_usd))
        else:
            norm_depth_ask = nrm.depth_ask
            norm_best_ask_usd = nrm.best_ask_usd
            norm_depth_bid = getattr(nrm, "depth_bid", 0.0) or norm_depth_ask
            norm_best_bid_usd = getattr(nrm, "best_bid_usd", 0.0) or norm_best_ask_usd
        norm_spread = _extract_spread_bps(nrm, default=20.0)

        cur_depth_ask = leader_snap.depth_ask_usd
        cur_depth_bid = leader_snap.depth_bid_usd
        cur_spread = leader_snap.spread_bps
        blowout_ratio = cur_spread / max(norm_spread, 0.5)

        long_edge_mult, long_drain_mult, long_size_mult = self.regime_detector.get_regime_modifiers("long", now)
        short_edge_mult, short_drain_mult, short_size_mult = self.regime_detector.get_regime_modifiers("short", now)

        long_trigger_type: Optional[str] = None
        long_drain_ratio = 0.0
        long_swept_usd = 0.0

        short_trigger_type: Optional[str] = None
        short_drain_ratio = 0.0
        short_swept_usd = 0.0

        ask_lifted = leader_snap.best_ask > prev_leader_snap.best_ask * (1.0 + 1e-7)
        ask_held = abs(leader_snap.best_ask - prev_leader_snap.best_ask) <= prev_leader_snap.best_ask * 1e-7
        bid_non_decreasing = leader_snap.best_bid >= prev_leader_snap.best_bid * (1.0 - 1e-7)

        bid_dropped = leader_snap.best_bid < prev_leader_snap.best_bid * (1.0 - 1e-7)
        bid_held = abs(leader_snap.best_bid - prev_leader_snap.best_bid) <= prev_leader_snap.best_bid * 1e-7
        ask_non_increasing = leader_snap.best_ask <= prev_leader_snap.best_ask * (1.0 + 1e-7)

        eff_long_drain = self.drain_threshold * long_drain_mult
        eff_short_drain = self.drain_threshold * short_drain_mult

        # 1. Long Depth Drain
        if ask_lifted and bid_non_decreasing:
            swept_top_level = prev_leader_snap.best_ask_usd
            top5_loss = max(0.0, prev_leader_snap.depth_ask_usd - cur_depth_ask)
            long_swept_usd = max(top5_loss, swept_top_level)
            long_drain_ratio = max(
                long_swept_usd / max(norm_depth_ask, DEPTH_FLOOR_USD),
                swept_top_level / max(norm_best_ask_usd * 2.0, DEPTH_FLOOR_USD),
            )
            if long_drain_ratio >= eff_long_drain:
                long_trigger_type = "depth_drain"

        elif ask_held and bid_non_decreasing:
            best_ask_loss = prev_leader_snap.best_ask_usd - leader_snap.best_ask_usd
            top5_loss = prev_leader_snap.depth_ask_usd - cur_depth_ask
            if best_ask_loss > 0 and top5_loss > 0:
                if best_ask_loss >= 0.5 * top5_loss:
                    long_swept_usd = top5_loss
                    long_drain_ratio = top5_loss / max(norm_depth_ask, DEPTH_FLOOR_USD)
                    if long_drain_ratio >= eff_long_drain:
                        long_trigger_type = "depth_drain"

        # 2. Long Upward Spread Blowout
        if (
            long_trigger_type is None
            and ask_lifted
            and bid_non_decreasing
            and blowout_ratio >= self.blowout_threshold
        ):
            long_trigger_type = "spread_blowout"
            long_swept_usd = max(prev_leader_snap.best_ask_usd, max(0.0, prev_leader_snap.depth_ask_usd - cur_depth_ask))

        # 3. Short Depth Drain
        if bid_dropped and ask_non_increasing:
            swept_top_level_bid = prev_leader_snap.best_bid_usd
            top5_loss_bid = max(0.0, prev_leader_snap.depth_bid_usd - cur_depth_bid)
            short_swept_usd = max(top5_loss_bid, swept_top_level_bid)
            short_drain_ratio = max(
                short_swept_usd / max(norm_depth_bid, DEPTH_FLOOR_USD),
                swept_top_level_bid / max(norm_best_bid_usd * 2.0, DEPTH_FLOOR_USD),
            )
            if short_drain_ratio >= eff_short_drain:
                short_trigger_type = "depth_drain"

        elif bid_held and ask_non_increasing:
            best_bid_loss = prev_leader_snap.best_bid_usd - leader_snap.best_bid_usd
            top5_loss_bid = prev_leader_snap.depth_bid_usd - cur_depth_bid
            if best_bid_loss > 0 and top5_loss_bid > 0:
                if best_bid_loss >= 0.5 * top5_loss_bid:
                    short_swept_usd = top5_loss_bid
                    short_drain_ratio = top5_loss_bid / max(norm_depth_bid, DEPTH_FLOOR_USD)
                    if short_drain_ratio >= eff_short_drain:
                        short_trigger_type = "depth_drain"

        # 4. Short Downward Spread Blowout
        if (
            short_trigger_type is None
            and bid_dropped
            and ask_non_increasing
            and blowout_ratio >= self.blowout_threshold
        ):
            short_trigger_type = "spread_blowout"
            short_swept_usd = max(prev_leader_snap.best_bid_usd, max(0.0, prev_leader_snap.depth_bid_usd - cur_depth_bid))

        # Cancel vs. Sweep Discrimination (Anti-Ghosting Guard)
        recent_buys_usd = 0.0
        is_cancel_blocked = False
        saved_trigger_type = long_trigger_type
        saved_swept_usd = long_swept_usd
        cutoff = now - getattr(self, "trade_confirm_lookback_sec", 2.0)

        if long_trigger_type is not None:
            if leader_ex == "bithumb":
                recent_buys_usd, has_trades = self.get_recent_bithumb_buys(coin, cutoff)
            else:
                recent_buys_usd, has_trades = self.get_recent_upbit_buys(coin, cutoff)

            should_verify_trades = self.require_trade_confirmation or has_trades
            if should_verify_trades:
                ratio_needed = 0.25 if ask_held else 0.15
                min_buys_needed = max(getattr(self, "min_trade_confirm_floor_usd", 5.0), long_swept_usd * ratio_needed)

                if recent_buys_usd < min_buys_needed:
                    self.counters["cancels_blocked"] = self.counters.get("cancels_blocked", 0) + 1
                    is_cancel_blocked = True
                    log.debug(
                        f"[engine] Blocked false sweep / MM cancel on {leader_ex.capitalize()} {coin}: "
                        f"swept_usd=${long_swept_usd:.1f}, but recent taker buys=${recent_buys_usd:.1f} "
                        f"(needed ${min_buys_needed:.1f})"
                    )
                    long_trigger_type = None
                    long_swept_usd = 0.0
                    long_drain_ratio = 0.0

            if leader_ex == "bithumb" and long_trigger_type is not None:
                min_bithumb_swept = getattr(C, "BITHUMB_MIN_SWEPT_USD", 3000.0)
                if long_swept_usd < min_bithumb_swept and recent_buys_usd < min_bithumb_swept:
                    long_trigger_type = None

        recent_sells_usd = 0.0
        if short_trigger_type is not None:
            if leader_ex == "bithumb":
                recent_sells_usd, has_trades_s = self.get_recent_bithumb_sells(coin, cutoff)
            else:
                recent_sells_usd, has_trades_s = self.get_recent_upbit_sells(coin, cutoff)

            should_verify_sells = self.require_trade_confirmation or has_trades_s
            if should_verify_sells:
                ratio_needed = 0.25 if bid_held else 0.15
                min_sells_needed = max(getattr(self, "min_trade_confirm_floor_usd", 5.0), short_swept_usd * ratio_needed)

                if recent_sells_usd < min_sells_needed:
                    self.counters["cancels_blocked_sells"] = self.counters.get("cancels_blocked_sells", 0) + 1
                    log.debug(
                        f"[engine] Blocked false bid sweep / MM cancel on {leader_ex.capitalize()} {coin}: "
                        f"swept_usd=${short_swept_usd:.1f}, but recent taker sells=${recent_sells_usd:.1f} "
                        f"(needed ${min_sells_needed:.1f})"
                    )
                    short_trigger_type = None
                    short_swept_usd = 0.0
                    short_drain_ratio = 0.0

            if leader_ex == "bithumb" and short_trigger_type is not None:
                min_bithumb_swept = getattr(C, "BITHUMB_MIN_SWEPT_USD", 3000.0)
                if short_swept_usd < min_bithumb_swept and recent_sells_usd < min_bithumb_swept:
                    short_trigger_type = None

        active_side = "long" if long_trigger_type else ("short" if short_trigger_type else None)
        trigger_type = long_trigger_type or short_trigger_type
        swept_usd = long_swept_usd if active_side == "long" else short_swept_usd
        drain_ratio = long_drain_ratio if active_side == "long" else short_drain_ratio
        norm_depth = norm_depth_ask if active_side == "long" else norm_depth_bid
        recent_trade_usd = recent_buys_usd if active_side == "long" else recent_sells_usd
        edge_mult = long_edge_mult if active_side == "long" else short_edge_mult
        size_mult = long_size_mult if active_side == "long" else short_size_mult

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

                cur_regime = self.regime_detector.get_regime(now).regime.value
                self.strat_fade.on_fakeout_detected(
                    coin=coin,
                    trigger_type=fade_trig,
                    swept_usd=fade_swept,
                    upbit_snap=leader_snap,
                    follower_snaps=f_snaps,
                    now=now,
                    fade_reason=fade_reason,
                    market_regime=cur_regime,
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
                    recent_buys_usd=recent_trade_usd,
                    leader_ex=leader_ex,
                    leader_snap=leader_snap,
                    side=active_side or "long",
                    edge_multiplier=edge_mult,
                    size_multiplier=size_mult,
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
        side: str = "long",
        edge_multiplier: float = 1.0,
        size_multiplier: float = 1.0,
    ) -> None:
        leader_snap_to_use = leader_snap if leader_snap is not None else upbit_snap
        if leader_snap_to_use is None or f_snap is None:
            return

        # Macro Market Regime & Flash Dump Circuit Breaker / Pump Shield
        if side == "short":
            short_allowed, short_reason = self.regime_detector.is_short_allowed(now)
            if not short_allowed:
                self.counters["regime_pump_blocked_shorts"] = self.counters.get("regime_pump_blocked_shorts", 0) + 1
                log.info(f"[REGIME-SHIELD] S1 Short blocked on {coin} @ {follower_ex}: {short_reason}")
                return
        else:
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
            side=side,
            edge_multiplier=edge_multiplier,
            size_multiplier=size_multiplier,
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
            if side == "short":
                if leader_ex == "upbit":
                    recent_other_sells, _ = self.get_recent_bithumb_sells(coin, cutoff)
                    sig.korean_spread_bps = round((other_snap.mid_price - leader_snap_to_use.mid_price) / leader_snap_to_use.mid_price * 1e4, 1)
                    parity = (other_snap.mid_price <= leader_snap_to_use.mid_price * 1.0005)
                    vol_confirmed = (recent_other_sells >= min_confirm_usd and recent_other_sells > 0) if min_confirm_usd > 0 else (recent_other_sells > 0)
                    sig.korean_confirmed = parity or vol_confirmed
                else:
                    recent_other_sells, _ = self.get_recent_upbit_sells(coin, cutoff)
                    sig.korean_spread_bps = round((leader_snap_to_use.mid_price - other_snap.mid_price) / other_snap.mid_price * 1e4, 1)
                    parity = (leader_snap_to_use.mid_price <= other_snap.mid_price * 1.0005)
                    vol_confirmed = (recent_other_sells >= min_confirm_usd and recent_other_sells > 0) if min_confirm_usd > 0 else (recent_other_sells > 0)
                    sig.korean_confirmed = parity or vol_confirmed
            else:
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
        self.counters[f"signals_{side}"] = self.counters.get(f"signals_{side}", 0) + 1
        dual_icon = "⚡" if sig.korean_confirmed else ""
        dual_suffix = "+DUAL" if sig.korean_confirmed else ""
        if sig.is_actionable:
            self.counters["signals_battle"] += 1
            tag = f"[SIGNAL-BATTLE ({leader_ex.upper()}{dual_suffix}) ({side.upper()})] ⚔️{dual_icon}"
        else:
            self.counters["signals_incubator"] += 1
            tag = f"[SIGNAL-INCUBATOR ({leader_ex.upper()}{dual_suffix}) ({side.upper()})] 🧪{dual_icon}"

        trades_label = "Buys" if side == "long" else "Sells"
        buys_str = f", {trades_label}: ${sig.leader_recent_buys_usd:.0f}" if sig.leader_recent_buys_usd > 0 else ""
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
            side = getattr(sig, "side", "long") or "long"

            # 1. Mid-to-Mid move on follower and leader (bps)
            if side == "short":
                f_mid_move = (
                    (sig.follower_mid - f_snap.mid_price) / sig.follower_mid * 1e4
                    if sig.follower_mid > 0
                    else 0.0
                )
                lead_mid_move = (
                    (sig.leader_mid - leader_snap.mid_price) / sig.leader_mid * 1e4
                    if sig.leader_mid > 0
                    else 0.0
                )
                # True executable move: Entry at VWAP bid -> Exit at current VWAP ask (buy to cover)
                entry_px = getattr(sig, "follower_entry_px", 0.0) or sig.follower_best_bid
                exit_px = f_snap.vwap_ask_usd(sig.suggested_size_usd) if sig.suggested_size_usd > 0 else f_snap.best_ask
                f_exec_move = (entry_px - exit_px) / entry_px * 1e4 if entry_px > 0 else 0.0
            else:
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
                # True executable move: Entry at VWAP ask -> Exit at current VWAP bid
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
            if side == "short":
                cur_dislocation_bps = sig.kimchi_baseline_bps - cur_diff_bps
                rem_edge = cur_dislocation_bps - self.taker_fee_bps
                leader_adverse_bps = (
                    (leader_snap.mid_price - sig.leader_mid) / sig.leader_mid * 1e4
                    if sig.leader_mid > 0
                    else 0.0
                )
            else:
                cur_dislocation_bps = cur_diff_bps - sig.kimchi_baseline_bps
                rem_edge = cur_dislocation_bps - self.taker_fee_bps
                leader_adverse_bps = (
                    (sig.leader_mid - leader_snap.mid_price) / sig.leader_mid * 1e4
                    if sig.leader_mid > 0
                    else 0.0
                )

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

            close_reason, stop_type = evaluate_exit_reason(
                cur_net_pnl=cur_net_pnl,
                net_exec_edge_bps=sig.net_exec_edge_bps,
                rem_edge=rem_edge,
                dt=dt,
                hold_sec=hold_sec,
                pair_decay_sec=pair_decay_sec,
                sigma=sigma,
                upbit_drop_bps=leader_adverse_bps,
                cur_dislocation_bps=cur_dislocation_bps,
                stop_invalidation_bps=self.stop_invalidation_bps,
                stop_adverse_pnl_bps=self.stop_adverse_pnl_bps,
            )

            # Dual-Phase Moonbag / Runner Evaluation (Strategy 1)
            if getattr(sig, "runner_active", False):
                # --- Peak & excursion tracking ---
                if cur_net_pnl > sig.runner_peak_pnl_bps:
                    sig.runner_peak_pnl_bps = cur_net_pnl
                    sig.runner_peak_ts = now
                if cur_net_pnl > sig.runner_mfe_bps:
                    sig.runner_mfe_bps = cur_net_pnl
                if cur_net_pnl < sig.runner_mae_bps:
                    sig.runner_mae_bps = cur_net_pnl

                runner_dt = now - (sig.phase1_close_ts or sig.open_ts)
                since_peak = now - (sig.runner_peak_ts or sig.open_ts)

                # --- Adaptive trailing distance ---
                trail_factor = getattr(C, "S1_RUNNER_TRAIL_FACTOR", 0.35)
                trail_floor = getattr(C, "S1_RUNNER_TRAIL_FLOOR_BPS", 8.0)
                trail_cap = getattr(C, "S1_RUNNER_TRAIL_CAP_BPS", 25.0)
                sigma_factor = getattr(C, "S1_RUNNER_TRAIL_SIGMA_FACTOR", 1.0)

                adaptive_trail = sig.runner_peak_pnl_bps * trail_factor
                sigma_floor = sigma * sigma_factor
                trail_dist = max(trail_floor, min(sigma_floor, trail_cap * 1.5), min(adaptive_trail, trail_cap))
                sig.runner_trail_dist_bps = round(trail_dist, 2)

                # --- Dynamic breakeven escalation ---
                escalate_sec = getattr(C, "S1_RUNNER_BREAKEVEN_ESCALATE_SEC", 15.0)
                lock_frac = getattr(C, "S1_RUNNER_BREAKEVEN_LOCK_FRACTION", 0.5)
                if runner_dt >= escalate_sec:
                    breakeven_floor = sig.runner_peak_pnl_bps * lock_frac
                else:
                    breakeven_floor = 0.0
                sig.runner_breakeven_floor_bps = round(breakeven_floor, 2)

                # --- Exit evaluation (priority order) ---
                runner_exit_reason: Optional[str] = None

                if cur_net_pnl <= (sig.runner_peak_pnl_bps - trail_dist):
                    runner_exit_reason = "runner_trailing_stop"
                elif cur_net_pnl < breakeven_floor:
                    runner_exit_reason = "runner_breakeven_lock"
                elif since_peak >= getattr(C, "S1_RUNNER_NO_PEAK_DECAY_SEC", 20.0):
                    runner_exit_reason = "runner_dead_stall"
                elif stop_type is not None:
                    runner_exit_reason = f"runner_{close_reason}"
                elif runner_dt >= getattr(C, "S1_RUNNER_MAX_HOLD_SEC", 120.0):
                    runner_exit_reason = "runner_timeout"

                if runner_exit_reason is not None:
                    sig.runner_pnl_bps = cur_net_pnl
                    portion = getattr(C, "S1_RUNNER_PORTION", 0.30)
                    blended_pnl = round((1.0 - portion) * sig.phase1_pnl_bps + portion * cur_net_pnl, 2)
                    self._close_signal(
                        sig, now, runner_exit_reason, f_mid_move, f_exec_move,
                        override_net_pnl=blended_pnl
                    )
                return

            if close_reason is not None:
                # Phase 1 → Phase 2 Runner transition with threshold gate (long breakout only)
                should_activate_runner = (
                    close_reason == "converged"
                    and getattr(self, "runner_enabled", getattr(C, "S1_RUNNER_ENABLED", False))
                    and not getattr(sig, "runner_active", False)
                    and getattr(sig, "side", "long") == "long"
                )
                if should_activate_runner:
                    min_phase1 = getattr(C, "S1_RUNNER_MIN_PHASE1_BPS", 20.0)
                    if cur_net_pnl >= min_phase1:
                        sig.runner_enabled = True
                        sig.runner_active = True
                        sig.phase1_pnl_bps = cur_net_pnl
                        sig.phase1_close_ts = now
                        sig.phase1_mfe_bps = cur_net_pnl
                        sig.phase1_mae_bps = cur_net_pnl
                        sig.runner_pnl_bps = cur_net_pnl
                        sig.runner_peak_pnl_bps = cur_net_pnl
                        sig.runner_peak_ts = now
                        sig.runner_mfe_bps = cur_net_pnl
                        sig.runner_mae_bps = cur_net_pnl
                        portion = getattr(C, "S1_RUNNER_PORTION", 0.30)
                        self.counters["signals_runner_activated"] = self.counters.get("signals_runner_activated", 0) + 1
                        log.info(
                            f"[S1-RUNNER] 🚀 Activated on {sig.coin} ({sig.follower_exchange}): "
                            f"Phase1 locked {(1.0-portion)*100:.0f}% @ {cur_net_pnl:+.1f} bps | "
                            f"Runner {(portion)*100:.0f}% trailing (peak={sig.runner_peak_pnl_bps:+.1f})"
                        )
                        return
                    # If phase1 too small — fall through to normal close below
                    log.debug(
                        f"[S1-RUNNER] Skipped activation on {sig.coin}: "
                        f"phase1_pnl={cur_net_pnl:+.1f} < min_phase1={min_phase1:.1f}"
                    )

                # Normal close (either not converged, or runner activation skipped)
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
        override_net_pnl: Optional[float] = None,
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
            if override_net_pnl is not None:
                sig.net_pnl_bps = round(override_net_pnl, 2)
            else:
                sig.net_pnl_bps = round(f_exec_move_bps - self.roundtrip_fee_bps, 2)

            if "runner" in reason:
                self.counters["signals_runner_closed"] = self.counters.get("signals_runner_closed", 0) + 1
                self.counters[f"signals_{reason}"] = self.counters.get(f"signals_{reason}", 0) + 1
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
                    if sig.closed:
                        continue
                    dt = now - sig.open_ts
                    is_runner = getattr(sig, "runner_active", False)
                    if is_runner:
                        hold_sec = getattr(C, "S1_RUNNER_MAX_HOLD_SEC", 120.0)
                    else:
                        hold_sec = getattr(sig, "max_hold_sec", self.max_hold_sec) or self.max_hold_sec
                    pair_cfg = self.get_pair_threshold_config(sig.coin, sig.follower_exchange, leader_ex=sig.leader_exchange)
                    default_decay = max(self.alpha_decay_sec, hold_sec * 0.5) if hold_sec > self.max_hold_sec else self.alpha_decay_sec
                    pair_decay_sec = float(pair_cfg.get("alpha_decay_sec", default_decay))

                    f_recv = self._recv_ts.get((sig.follower_exchange, coin), 0.0)
                    is_feed_stalled = (now - f_recv) > 5.0

                    should_close = False
                    close_reason = "runner_timeout" if is_runner else "timeout"
                    if is_runner:
                        runner_dt = now - (sig.phase1_close_ts or sig.open_ts)
                        since_peak = now - (sig.runner_peak_ts or sig.open_ts)
                        if runner_dt >= hold_sec:
                            should_close = True
                            close_reason = "runner_timeout"
                        elif since_peak >= getattr(C, "S1_RUNNER_NO_PEAK_DECAY_SEC", 20.0):
                            should_close = True
                            close_reason = "runner_dead_stall"
                    else:
                        if dt >= hold_sec:
                            should_close = True
                            close_reason = "timeout"
                        elif dt >= pair_decay_sec and is_feed_stalled:
                            should_close = True
                            close_reason = "stop_decay"

                    if should_close:
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

                        override_pnl = None
                        if is_runner:
                            cur_net_pnl = f_exec_move - self.roundtrip_fee_bps
                            runner_dt = now - (sig.phase1_close_ts or sig.open_ts)
                            since_peak = now - (sig.runner_peak_ts or sig.open_ts)

                            # Re-evaluate trailing on stale data (best_bid may be old, but it's our best estimate)
                            trail_factor = getattr(C, "S1_RUNNER_TRAIL_FACTOR", 0.35)
                            trail_floor = getattr(C, "S1_RUNNER_TRAIL_FLOOR_BPS", 8.0)
                            trail_cap = getattr(C, "S1_RUNNER_TRAIL_CAP_BPS", 25.0)
                            sigma_factor = getattr(C, "S1_RUNNER_TRAIL_SIGMA_FACTOR", 1.0)
                            sigma = max(sig.pair_sigma_bps, 2.0)

                            adaptive_trail = sig.runner_peak_pnl_bps * trail_factor
                            sigma_floor = sigma * sigma_factor
                            trail_dist = max(trail_floor, min(sigma_floor, trail_cap * 1.5), min(adaptive_trail, trail_cap))

                            escalate_sec = getattr(C, "S1_RUNNER_BREAKEVEN_ESCALATE_SEC", 15.0)
                            lock_frac = getattr(C, "S1_RUNNER_BREAKEVEN_LOCK_FRACTION", 0.5)
                            breakeven_floor = sig.runner_peak_pnl_bps * lock_frac if runner_dt >= escalate_sec else 0.0

                            # Determine sweep exit reason (mirror tick-path priority)
                            if cur_net_pnl <= (sig.runner_peak_pnl_bps - trail_dist):
                                close_reason = "runner_trailing_stop"
                            elif cur_net_pnl < breakeven_floor:
                                close_reason = "runner_breakeven_lock"
                            elif since_peak >= getattr(C, "S1_RUNNER_NO_PEAK_DECAY_SEC", 20.0):
                                close_reason = "runner_dead_stall"
                            elif runner_dt >= getattr(C, "S1_RUNNER_MAX_HOLD_SEC", 120.0):
                                close_reason = "runner_timeout"

                            portion = getattr(C, "S1_RUNNER_PORTION", 0.30)
                            override_pnl = round((1.0 - portion) * sig.phase1_pnl_bps + portion * cur_net_pnl, 2)

                        self._close_signal(sig, now, close_reason, f_mid_move, f_exec_move, override_net_pnl=override_pnl)
                except Exception as e:
                    log.error(f"[engine] Error sweeping timeout for signal {getattr(sig, 'id', '')}: {e}")

        # Multi-strategy timeout sweeps:
        # Strategy 2: Fade the Fakeout (sweep expired shorts if streams disconnected)
        self.strat_fade.sweep_timeouts(now)

        # Strategy 3: Maker Liquidity Harvesting (sweep expired resting orders & filled voids)
        self.strat_maker.sweep_timeouts(now)

        # Strategy 4: Predatory Rebound Maker (sweep expired resting orders & filled positions)
        self.strat_rubber_band.sweep_timeouts(now)

        # Strategy 5: Cross-Korean Triangle (sweep expired positions)
        self.strat_triangle.sweep_timeouts(now)

        # Archived Strategy S5 (Legacy OBIV Imbalance)
        self.strat_imbalance.sweep_timeouts(now)

        # Archived Strategy S6 (Legacy Beta-Neutral Pairs)
        self.strat_beta_neutral.sweep_timeouts(now)

        # Pruning of idle follower spread EWMA baselines (Item 2: idle > 1 hour)
        with self._engine_lock:
            dead_ewma = [k for k, v in self._follower_spread_ewma.items() if (now - v.last_ts) > 3600.0]
            for k in dead_ewma:
                del self._follower_spread_ewma[k]