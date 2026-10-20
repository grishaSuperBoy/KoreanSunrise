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
BASELINE_TAU = 600.0    # EWMA time constant for cross-venue basis baseline (sec, 10 minutes)
DEPTH_FLOOR_USD = 5.0   # Numerical stability floor for depth normalization ($5 for low-cap alts)
BEST_ASK_FLOOR_USD = 2.5 # Numerical stability floor for best ask ($2.50 for low-cap alts)


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

        # Upbit normalisation state per coin:
        # coin -> {"depth_ask": float, "best_ask_usd": float, "spread_bps": float, "t": float}
        self._upbit_norm: Dict[str, Dict[str, float]] = {}

        # Pair states: (coin, follower_exchange) -> PairState
        self._pairs: Dict[Tuple[str, str], PairState] = {}

        # Active signals tracking: coin -> list of active SunriseSignal
        self._active_signals: Dict[str, List[SunriseSignal]] = defaultdict(list)
        self._seq = itertools.count(1)

        # Telemetry counters
        self.counters = {
            "ticks_upbit": 0,
            "ticks_followers": 0,
            "trades_upbit": 0,
            "cancels_blocked": 0,
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
        }

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

        # Strategy 2: Fade the Fakeout (Shorting False/Unconfirmed Sweeps)
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
        )

        # Strategy 3: Maker Liquidity Harvesting (Passive Maker Post-Only on Widened Spreads)
        self.strat_maker = MakerVoidStrategy(
            emit_callback=self._on_strategy_emit,
            min_order_usd=self.min_order_usd,
            max_position_budget_usd=self.max_position_budget_usd,
            stealth_depth_fraction=self.stealth_fraction,
            taker_fee_bps=self.taker_fee_bps,
            max_spread_bps=getattr(C, "MAX_MAKER_SPREAD_BPS", 120.0),
            allow_asterdex_maker=getattr(C, "ASTERDEX_MAKER_ENABLED", False),
        )

        # Pair-specific calibrated execution thresholds
        self.thresholds_file = thresholds_file or os.path.join(C.PROJECT_ROOT, "matrix_thresholds.json")
        self._thresholds: Dict[str, Any] = {}
        self._last_thresh_check: float = 0.0
        self._thresh_mtime: float = 0.0
        self.reload_thresholds(force=True)

    def _on_strategy_emit(self, rec: Dict[str, Any]) -> None:
        """Centralized telemetry accumulator for multi-strategy execution before routing to writer."""
        strat = rec.get("strategy")
        reason = str(rec.get("close_reason", ""))
        if strat == "fade_fakeout":
            self.counters["fade_signals_triggered"] = self.counters.get("fade_signals_triggered", 0) + 1
            if reason == "target_fade_reached":
                self.counters["fade_signals_converged"] = self.counters.get("fade_signals_converged", 0) + 1
            elif "stop" in reason:
                self.counters["fade_signals_stopped"] = self.counters.get("fade_signals_stopped", 0) + 1
            elif "timeout" in reason:
                self.counters["fade_signals_timeout"] = self.counters.get("fade_signals_timeout", 0) + 1
        elif strat == "maker_void":
            self.counters["maker_voids_triggered"] = self.counters.get("maker_voids_triggered", 0) + 1
            if rec.get("maker_filled"):
                self.counters["maker_voids_filled"] = self.counters.get("maker_voids_filled", 0) + 1
            if reason == "spread_restored":
                self.counters["maker_voids_restored"] = self.counters.get("maker_voids_restored", 0) + 1
            elif "stop" in reason:
                self.counters["maker_voids_stopped"] = self.counters.get("maker_voids_stopped", 0) + 1
            elif "unfilled" in reason or "timeout" in reason:
                self.counters["maker_voids_unfilled"] = self.counters.get("maker_voids_unfilled", 0) + 1

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

    def get_pair_threshold_config(self, coin: str, follower_ex: str) -> Dict[str, Any]:
        pk = f"{coin.upper()}:upbit-{follower_ex.lower()}"
        return self._thresholds.get(pk, {})

    def _get_or_create_pair_state(self, coin: str, follower_ex: str) -> PairState:
        pair_key = (coin, follower_ex)
        with self._engine_lock:
            st = self._pairs.get(pair_key)
            if st is None:
                cfg = self.get_pair_threshold_config(coin, follower_ex)
                sp_sig = float(cfg.get("spike_sigma", 2.5))
                st = self._pairs[pair_key] = PairState(
                    coin, follower_ex, self.warmup_samples, spike_sigma=sp_sig
                )
            return st

    def on_book_update(self, snap: OrderBookDepth5) -> None:
        now = time.time()
        ex = snap.exchange.lower()
        coin = snap.symbol.upper()
        key = (ex, coin)

        with self._engine_lock:
            prev_snap = self._books.get(key)
            self._books[key] = snap
            self._recv_ts[key] = now

        if ex == "upbit":
            self.counters["ticks_upbit"] += 1
            self._eval_lead_lag(coin, snap, prev_snap, now)
            self._update_upbit_norm(coin, snap, now)
            self._track_active_signals_for_coin(coin, now)
        else:
            with self._engine_lock:
                self._coin_followers[coin].add(ex)
            self.counters["ticks_followers"] += 1
            upbit_snap = self._books.get(("upbit", coin))
            if upbit_snap is not None and (now - self._recv_ts.get(("upbit", coin), 0.0)) <= 2.0:
                st = self._get_or_create_pair_state(coin, ex)
                if upbit_snap.mid_price > 0 and snap.mid_price > 0:
                    raw_diff_bps = (upbit_snap.mid_price - snap.mid_price) / snap.mid_price * 1e4
                    with self._engine_lock:
                        has_open = (st.open_signal is not None)
                    if not has_open:
                        st.learn(raw_diff_bps, now)

            self._track_follower_move(coin, ex, snap, now)

            # Multi-strategy follower updates:
            # Strategy 2: Fade the Fakeout
            cur_upbit_mid = upbit_snap.mid_price if upbit_snap else None
            self.strat_fade.on_follower_tick(ex, coin, snap, now, cur_upbit_mid=cur_upbit_mid)

            # Strategy 3: Maker Liquidity Harvesting
            norm_spread = float(getattr(self._upbit_norm.get(coin), "spread_bps", 20.0) or 20.0)
            self.strat_maker.on_follower_tick(ex, coin, snap, now, norm_spread_bps=norm_spread)
            self.strat_maker.check_void_opportunity(ex, coin, snap, upbit_snap, now, norm_spread_bps=norm_spread)

    def on_upbit_trade(self, coin: str, trade_usd: float, is_taker_buy: bool, ts: float) -> None:
        """
        Real-time trade stream ingestion from Upbit.
        Records taker buys to verify liquidity sweeps and reject MM quote cancellations.
        Thread-safe across concurrent WebSocket worker threads.
        """
        c = coin.upper()
        with self._buys_lock:
            self.counters["trades_upbit"] = self.counters.get("trades_upbit", 0) + 1
            self._has_upbit_trades[c] = True
            if is_taker_buy and trade_usd > 0.0:
                self._upbit_recent_buys[c].append((ts, trade_usd))

    def get_recent_upbit_buys(self, coin: str, cutoff: float) -> Tuple[float, bool]:
        """
        Thread-safe retrieval and pruning of recent taker buys for a coin.
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

    # ----------------------------------------------------------- Upbit Norm
    def _update_upbit_norm(self, coin: str, snap: OrderBookDepth5, now: float) -> None:
        nrm = self._upbit_norm.get(coin)
        cur_depth = snap.depth_ask_usd
        cur_best_ask_usd = snap.best_ask_usd
        cur_spread = snap.spread_bps

        if nrm is None:
            self._upbit_norm[coin] = UpbitNormState(
                depth_ask=max(cur_depth, DEPTH_FLOOR_USD),
                best_ask_usd=max(cur_best_ask_usd, BEST_ASK_FLOOR_USD),
                spread_bps=max(cur_spread, 1.0),
                t=now,
            )
            return

        if isinstance(nrm, dict):
            nrm = self._upbit_norm[coin] = UpbitNormState(
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

    # ----------------------------------------------------------- Signal Logic
    def _eval_lead_lag(
        self,
        coin: str,
        upbit_snap: OrderBookDepth5,
        prev_upbit_snap: Optional[OrderBookDepth5],
        now: float,
    ) -> None:
        nrm = self._upbit_norm.get(coin)
        if not nrm or prev_upbit_snap is None:
            return

        if (
            upbit_snap.best_ask <= 0
            or upbit_snap.best_bid <= 0
            or prev_upbit_snap.best_ask <= 0
            or prev_upbit_snap.best_bid <= 0
        ):
            return

        norm_depth = nrm["depth_ask"]
        norm_best_ask_usd = nrm["best_ask_usd"]
        norm_spread = nrm["spread_bps"]

        cur_depth = upbit_snap.depth_ask_usd
        cur_spread = upbit_snap.spread_bps

        trigger_type: Optional[str] = None
        drain_ratio = 0.0
        swept_usd = 0.0
        blowout_ratio = cur_spread / max(norm_spread, 0.5)

        ask_lifted = upbit_snap.best_ask > prev_upbit_snap.best_ask * (1.0 + 1e-7)
        ask_held = abs(upbit_snap.best_ask - prev_upbit_snap.best_ask) <= prev_upbit_snap.best_ask * 1e-7
        bid_non_decreasing = upbit_snap.best_bid >= prev_upbit_snap.best_bid * (1.0 - 1e-7)

        # 1. True Depth Drain
        if ask_lifted and bid_non_decreasing:
            swept_top_level = prev_upbit_snap.best_ask_usd
            top5_loss = max(0.0, prev_upbit_snap.depth_ask_usd - cur_depth)
            swept_usd = max(top5_loss, swept_top_level)
            drain_ratio = max(
                swept_usd / max(norm_depth, DEPTH_FLOOR_USD),
                swept_top_level / max(norm_best_ask_usd * 2.0, DEPTH_FLOOR_USD),
            )
            if drain_ratio >= self.drain_threshold:
                trigger_type = "depth_drain"

        elif ask_held and bid_non_decreasing:
            best_ask_loss = prev_upbit_snap.best_ask_usd - upbit_snap.best_ask_usd
            top5_loss = prev_upbit_snap.depth_ask_usd - cur_depth
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
            swept_usd = max(prev_upbit_snap.best_ask_usd, max(0.0, prev_upbit_snap.depth_ask_usd - cur_depth))

        # Cancel vs. Sweep Discrimination (Anti-Ghosting Guard)
        # If Upbit trade feed is available or trade confirmation is required,
        # verify that the liquidity drop was backed by real taker buys rather than MM cancels/quote pullbacks.
        recent_buys_usd = 0.0
        is_cancel_blocked = False
        saved_trigger_type = trigger_type
        saved_swept_usd = swept_usd
        if trigger_type is not None:
            cutoff = now - getattr(self, "trade_confirm_lookback_sec", 2.0)
            recent_buys_usd, has_trades = self.get_recent_upbit_buys(coin, cutoff)

            should_verify_trades = self.require_trade_confirmation or has_trades
            if should_verify_trades:
                ratio_needed = 0.25 if ask_held else 0.15
                min_buys_needed = max(getattr(self, "min_trade_confirm_floor_usd", 5.0), swept_usd * ratio_needed)

                if recent_buys_usd < min_buys_needed:
                    self.counters["cancels_blocked"] = self.counters.get("cancels_blocked", 0) + 1
                    is_cancel_blocked = True
                    log.debug(
                        f"[engine] Blocked false sweep / MM cancel on Upbit {coin}: "
                        f"swept_usd=${swept_usd:.1f}, but recent taker buys=${recent_buys_usd:.1f} "
                        f"(needed ${min_buys_needed:.1f})"
                    )
                    trigger_type = None
                    swept_usd = 0.0
                    drain_ratio = 0.0

        with self._engine_lock:
            follower_exchanges = set(self._coin_followers.get(coin, set()))
            if not follower_exchanges:
                follower_exchanges = {ex for (ex, c) in list(self._books.keys()) if ex != "upbit" and c == coin}
                if follower_exchanges:
                    self._coin_followers[coin] = follower_exchanges

        # Strategy 2: Check Fade the Fakeout on blocked cancels or micro-sweeps
        fade_trig = trigger_type or saved_trigger_type
        fade_swept = swept_usd or saved_swept_usd
        if fade_trig is not None:
            is_fake, fade_reason = self.strat_fade.is_fakeout_pattern(
                is_cancel_blocked=is_cancel_blocked,
                swept_usd=fade_swept,
                upbit_spread_bps=cur_spread,
                recent_buys_usd=recent_buys_usd,
            )
            if is_fake:
                f_snaps = {f_ex: self._books.get((f_ex, coin)) for f_ex in follower_exchanges if self._books.get((f_ex, coin)) is not None}
                self.strat_fade.on_fakeout_detected(
                    coin=coin,
                    trigger_type=fade_trig,
                    swept_usd=fade_swept,
                    upbit_snap=upbit_snap,
                    follower_snaps=f_snaps,
                    now=now,
                    fade_reason=fade_reason,
                    is_stable_pair_fn=lambda c, f_ex, t: self._get_or_create_pair_state(c, f_ex).is_stable(t),
                    recv_ts_map=self._recv_ts,
                )

        for follower_ex in follower_exchanges:
            f_snap = self._books.get((follower_ex, coin))
            f_recv = self._recv_ts.get((follower_ex, coin), 0.0)
            if f_snap is None:
                continue

            book_age_sec = now - f_recv
            # Decouple baseline learning from entry age guard:
            # Baseline learning requires reasonably contemporaneous quotes (<= 2.0s)
            if book_age_sec > 2.0:
                continue

            st = self._get_or_create_pair_state(coin, follower_ex)

            upbit_mid = upbit_snap.mid_price
            f_mid = f_snap.mid_price
            if upbit_mid <= 0 or f_mid <= 0:
                continue

            raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4

            # Entry check delegated to _check_entry (single gatekeeper for HFT staleness)
            with self._engine_lock:
                has_open = (st.open_signal is not None)

            if trigger_type and not has_open and st.is_stable(now, current_diff_bps=raw_diff_bps):
                self._check_entry(
                    coin=coin,
                    follower_ex=follower_ex,
                    st=st,
                    upbit_snap=upbit_snap,
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
        recent_buys_usd: float = 0.0,
    ) -> None:
        pair_cfg = self.get_pair_threshold_config(coin, follower_ex)
        seq_num = next(self._seq)
        sig = self.strat_lead_lag.evaluate_entry(
            coin=coin,
            follower_ex=follower_ex,
            st=st,
            upbit_snap=upbit_snap,
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

        with self._engine_lock:
            if st.open_signal is not None:
                return
            st.open_signal = sig
            self._active_signals[coin].append(sig)

        self.counters["signals_triggered"] += 1
        if sig.is_actionable:
            self.counters["signals_battle"] += 1
            tag = "[SIGNAL-BATTLE] ⚔️"
        else:
            self.counters["signals_incubator"] += 1
            tag = "[SIGNAL-INCUBATOR] 🧪"

        buys_str = f", UpBuys: ${sig.leader_recent_buys_usd:.0f}" if sig.leader_recent_buys_usd > 0 else ""
        log.info(
            f"{tag} {sig.coin} ({sig.follower_contract}) | Upbit -> {sig.follower_exchange} | "
            f"Trig: {sig.trigger_type} (Swept: ${sig.leader_swept_usd:.0f}{buys_str}) | "
            f"Edge: +{sig.net_exec_edge_bps:.1f} bps | Z: {sig.dev_z:.1f}σ | "
            f"Size: ${sig.suggested_size_usd:.0f} (Cap15%: ${sig.stealth_capacity_usd:.0f})"
        )

    # ----------------------------------------------------------- Tracking & Closure
    def _track_active_signals_for_coin(self, coin: str, now: float) -> None:
        with self._engine_lock:
            signals = list(self._active_signals.get(coin, []))
        if not signals:
            return
        upbit_snap = self._books.get(("upbit", coin))
        if not upbit_snap:
            return
        for sig in signals:
            if sig.closed:
                continue
            f_snap = self._books.get((sig.follower_exchange, coin))
            if f_snap is not None:
                self._track_one_signal(sig, upbit_snap, f_snap, now)

    def _track_follower_move(self, coin: str, follower_ex: str, f_snap: OrderBookDepth5, now: float) -> None:
        with self._engine_lock:
            signals = list(self._active_signals.get(coin, []))
        if not signals:
            return
        upbit_snap = self._books.get(("upbit", coin))
        if not upbit_snap:
            return
        for sig in signals:
            if sig.follower_exchange == follower_ex and not sig.closed:
                self._track_one_signal(sig, upbit_snap, f_snap, now)

    def _track_one_signal(
        self,
        sig: SunriseSignal,
        upbit_snap: OrderBookDepth5,
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
            up_mid_move = (
                (upbit_snap.mid_price - sig.leader_mid) / sig.leader_mid * 1e4
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
                (upbit_snap.mid_price - f_snap.mid_price) / f_snap.mid_price * 1e4
                if f_snap.mid_price > 0
                else 0.0
            )
            rem_edge = (cur_diff_bps - sig.kimchi_baseline_bps) - self.taker_fee_bps

            if not sig.path or dt > sig.path[-1][0]:
                sig.path.append([
                    dt,
                    round(up_mid_move, 2),
                    round(f_mid_move, 2),
                    cur_net_pnl,
                    round(rem_edge, 2),
                ])

            # Closure & Stop-Loss evaluation:
            pair_cfg = self.get_pair_threshold_config(sig.coin, sig.follower_exchange)
            hold_sec = getattr(sig, "max_hold_sec", self.max_hold_sec) or self.max_hold_sec
            default_decay = max(self.alpha_decay_sec, hold_sec * 0.5) if hold_sec > self.max_hold_sec else self.alpha_decay_sec
            pair_decay_sec = float(pair_cfg.get("alpha_decay_sec", default_decay))
            sigma = max(sig.pair_sigma_bps, 2.0)

            upbit_drop_bps = (
                (sig.leader_mid - upbit_snap.mid_price) / sig.leader_mid * 1e4
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
                upbit_drop_bps=upbit_drop_bps,
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