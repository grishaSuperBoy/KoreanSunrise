"""
korean_engine.py - High-frequency Korean Lead-Lag & Relative Depth Drain Engine.
Evaluates Spot Leader (Upbit) against Perpetual Followers (Binance, Bybit)
and records full microstructural telemetry for post-trade analysis.
"""
from __future__ import annotations

import json
import logging
import math
import os
import time
from collections import defaultdict, deque
from dataclasses import asdict
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import config as C
from models import OrderBookDepth5, SunriseSignal

log = logging.getLogger("engine")

DEV_SAMPLE_DT = 1.0     # Fixed 1s time grid for robust MAD window
NORM_TAU = 600.0        # EWMA time constant for Upbit depth and spread normalization (sec)
BASELINE_TAU = 3600.0   # EWMA time constant for cross-venue basis baseline (sec)


def _base(symbol: str) -> str:
    s = symbol.upper()
    if s.startswith("KRW-"):
        return s[4:]
    for suff in ("-PERP", "_PERP", "PERP", "USDT"):
        if s.endswith(suff) and len(s) > len(suff):
            return s[: -len(suff)]
    return s


class PairState:
    """State of an (Upbit, Follower) pair for a single coin."""

    def __init__(self, coin: str, follower_ex: str, warmup_samples: int = C.MIN_WARMUP_SAMPLES):
        self.coin = coin
        self.follower_ex = follower_ex
        self.warmup_samples = warmup_samples

        # Rolling cross-venue basis baseline (bps): (Mid_Upbit - Mid_Follower) / Mid_Follower * 10000
        self.baseline_bps: float = 0.0
        self.n_obs: int = 0
        self.t_last: float = 0.0

        # Robust MAD window sampled on fixed 1s grid
        self.dev_window: Deque[float] = deque(maxlen=300)
        self.dev_ts: float = 0.0
        self.cached_sigma: float = 10.0
        self.cached_sigma_ts: float = 0.0

        # Active open signal
        self.open_signal: Optional[SunriseSignal] = None

    @property
    def is_warmed_up(self) -> bool:
        return len(self.dev_window) >= self.warmup_samples and self.n_obs >= self.warmup_samples

    def learn(self, raw_diff_bps: float, now: float) -> None:
        if self.n_obs == 0:
            self.baseline_bps = raw_diff_bps
            self.n_obs = 1
            self.t_last = now
            self.dev_ts = now
            return

        dt = max(0.0, now - self.t_last)
        self.t_last = now
        deviation = raw_diff_bps - self.baseline_bps

        # Spike gating: if a massive pump/dislocation is currently happening (> 2.5 sigma),
        # slow down baseline adaptation by 20x so the baseline doesn't chase the pump.
        cur_sig = self.sigma(now)
        is_spike = self.is_warmed_up and abs(deviation) > (2.5 * cur_sig)
        eff_tau = BASELINE_TAU * 20.0 if is_spike else BASELINE_TAU

        a = 1.0 - math.exp(-dt / eff_tau) if eff_tau > 0 else 0.001
        self.baseline_bps += a * deviation
        self.n_obs += 1

        if now - self.dev_ts >= DEV_SAMPLE_DT:
            self.dev_ts = now
            if is_spike:
                clamped_dev = math.copysign(min(abs(deviation), 4.0 * cur_sig), deviation)
                self.dev_window.append(clamped_dev)
            else:
                self.dev_window.append(deviation)
            self.cached_sigma_ts = 0.0

    def sigma(self, now: float) -> float:
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
        stealth_depth_fraction: float = C.STEALTH_DEPTH_FRACTION,
        taker_fee_bps: float = C.TAKER_FEE_BPS,
        roundtrip_fee_bps: float = C.ROUNDTRIP_FEE_BPS,
        warmup_samples: int = C.MIN_WARMUP_SAMPLES,
        max_hold_sec: float = C.MAX_SIGNAL_HOLD_SEC,
        pool_state: Any = None,
        profile_manager: Any = None,
    ):
        self.emit = emit_callback
        self.drain_threshold = drain_threshold
        self.blowout_threshold = blowout_threshold
        self.min_edge_bps = min_edge_bps
        self.max_position_budget_usd = max_position_budget_usd
        self.stealth_fraction = stealth_depth_fraction
        self.taker_fee_bps = taker_fee_bps
        self.roundtrip_fee_bps = roundtrip_fee_bps
        self.warmup_samples = warmup_samples
        self.max_hold_sec = max_hold_sec
        self.pool_state = pool_state
        self.profile_manager = profile_manager

        # (exchange, coin) -> OrderBookDepth5
        self._books: Dict[Tuple[str, str], OrderBookDepth5] = {}
        self._recv_ts: Dict[Tuple[str, str], float] = {}

        # Upbit normalisation state per coin:
        # coin -> {"depth_ask": float, "best_ask_usd": float, "spread_bps": float, "t": float}
        self._upbit_norm: Dict[str, Dict[str, float]] = {}

        # Pair states: (coin, follower_exchange) -> PairState
        self._pairs: Dict[Tuple[str, str], PairState] = {}

        # Active signals tracking: coin -> list of active SunriseSignal
        self._active_signals: Dict[str, List[SunriseSignal]] = defaultdict(list)
        self._seq = 0

        # Telemetry counters
        self.counters = {
            "ticks_upbit": 0,
            "ticks_followers": 0,
            "signals_triggered": 0,
            "signals_battle": 0,
            "signals_incubator": 0,
            "signals_converged": 0,
            "signals_reversed": 0,
            "signals_timeout": 0,
        }

        # Pair-specific calibrated execution thresholds
        self.thresholds_file = os.path.join(C.PROJECT_ROOT, "matrix_thresholds.json")
        self._thresholds: Dict[str, Any] = {}
        self.reload_thresholds()

    def reload_thresholds(self) -> None:
        try:
            if os.path.exists(self.thresholds_file):
                with open(self.thresholds_file, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                    self._thresholds = doc.get("thresholds", {})
                log.info(f"[engine] Loaded {len(self._thresholds)} calibrated pair thresholds.")
        except Exception as e:
            log.warning(f"[engine] Failed to load {self.thresholds_file}: {e}")

    def get_pair_threshold_config(self, coin: str, follower_ex: str) -> Dict[str, Any]:
        pk = f"{coin.upper()}:upbit-{follower_ex.lower()}"
        return self._thresholds.get(pk, {})

    def on_book_update(self, snap: OrderBookDepth5) -> None:
        now = time.time()
        ex = snap.exchange.lower()
        coin = _base(snap.symbol)
        key = (ex, coin)

        prev_snap = self._books.get(key)
        self._books[key] = snap
        self._recv_ts[key] = now

        if ex == "upbit":
            self.counters["ticks_upbit"] += 1
            self._eval_lead_lag(coin, snap, prev_snap, now)
            self._update_upbit_norm(coin, snap, now)
            self._track_active_signals_for_coin(coin, now)
        else:
            self.counters["ticks_followers"] += 1
            upbit_snap = self._books.get(("upbit", coin))
            if upbit_snap is not None and (now - self._recv_ts.get(("upbit", coin), 0.0)) <= 2.0:
                pair_key = (coin, ex)
                st = self._pairs.get(pair_key)
                if st is None:
                    st = self._pairs[pair_key] = PairState(coin, ex, self.warmup_samples)
                if upbit_snap.mid_price > 0 and snap.mid_price > 0:
                    raw_diff_bps = (upbit_snap.mid_price - snap.mid_price) / snap.mid_price * 1e4
                    st.learn(raw_diff_bps, now)

            self._track_follower_move(coin, ex, snap, now)

    # ----------------------------------------------------------- Upbit Norm
    def _update_upbit_norm(self, coin: str, snap: OrderBookDepth5, now: float) -> None:
        nrm = self._upbit_norm.get(coin)
        cur_depth = snap.depth_ask_usd
        cur_best_ask_usd = snap.best_ask_usd
        cur_spread = snap.spread_bps

        if nrm is None:
            self._upbit_norm[coin] = {
                "depth_ask": max(cur_depth, 100.0),
                "best_ask_usd": max(cur_best_ask_usd, 50.0),
                "spread_bps": max(cur_spread, 1.0),
                "t": now,
            }
            return

        dt = max(0.0, now - nrm["t"])
        nrm["t"] = now
        a = 1.0 - math.exp(-dt / NORM_TAU)

        clamped_spread = min(cur_spread, nrm["spread_bps"] * 5.0)
        clamped_depth = min(cur_depth, nrm["depth_ask"] * 5.0)
        clamped_best_ask = min(cur_best_ask_usd, nrm["best_ask_usd"] * 5.0)

        nrm["depth_ask"] = max((1.0 - a) * nrm["depth_ask"] + a * clamped_depth, 100.0)
        nrm["best_ask_usd"] = max((1.0 - a) * nrm["best_ask_usd"] + a * clamped_best_ask, 50.0)
        nrm["spread_bps"] = max((1.0 - a) * nrm["spread_bps"] + a * clamped_spread, 0.5)

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
                swept_usd / max(norm_depth, 100.0),
                swept_top_level / max(norm_best_ask_usd * 2.0, 100.0),
            )
            if drain_ratio >= self.drain_threshold:
                trigger_type = "depth_drain"

        elif ask_held and bid_non_decreasing:
            best_ask_loss = prev_upbit_snap.best_ask_usd - upbit_snap.best_ask_usd
            top5_loss = prev_upbit_snap.depth_ask_usd - cur_depth
            if best_ask_loss > 0 and top5_loss > 0:
                if best_ask_loss >= 0.5 * top5_loss:
                    swept_usd = top5_loss
                    drain_ratio = top5_loss / max(norm_depth, 100.0)
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

        follower_exchanges = [ex for (ex, c) in self._books.keys() if ex != "upbit" and c == coin]
        for follower_ex in follower_exchanges:
            f_snap = self._books.get((follower_ex, coin))
            f_recv = self._recv_ts.get((follower_ex, coin), 0.0)
            if f_snap is None or (now - f_recv) > 2.0:
                continue

            pair_key = (coin, follower_ex)
            st = self._pairs.get(pair_key)
            if st is None:
                st = self._pairs[pair_key] = PairState(coin, follower_ex, self.warmup_samples)

            upbit_mid = upbit_snap.mid_price
            f_mid = f_snap.mid_price
            if upbit_mid <= 0 or f_mid <= 0:
                continue

            raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4

            if trigger_type and st.open_signal is None and st.is_warmed_up:
                self._check_entry(
                    coin=coin,
                    follower_ex=follower_ex,
                    st=st,
                    upbit_snap=upbit_snap,
                    f_snap=f_snap,
                    f_book_age_ms=(now - f_recv) * 1000.0,
                    norm_depth=norm_depth,
                    norm_spread=norm_spread,
                    swept_usd=swept_usd,
                    trigger_type=trigger_type,
                    drain_ratio=drain_ratio,
                    blowout_ratio=blowout_ratio,
                    now=now,
                )

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
    ) -> None:
        sigma = st.sigma(now)
        upbit_mid = upbit_snap.mid_price
        f_mid = f_snap.mid_price
        f_bid = f_snap.best_bid
        f_ask = f_snap.best_ask

        if f_ask <= 0 or f_bid <= 0:
            return

        raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4
        dislocation_bps = raw_diff_bps - st.baseline_bps

        if dislocation_bps <= 0:
            return

        dev_z = dislocation_bps / max(sigma, 1.5)
        if dev_z < 1.8:
            return

        f_ask_diff_bps = (upbit_mid - f_ask) / f_ask * 1e4
        gross_exec_edge_bps = f_ask_diff_bps - st.baseline_bps
        net_edge = gross_exec_edge_bps - self.taker_fee_bps

        # Check pair-specific calibrated threshold
        pair_cfg = self.get_pair_threshold_config(coin, follower_ex)
        if not pair_cfg.get("enabled", True):
            return  # Pruned pair due to negative EV or high adverse flow

        req_edge = float(pair_cfg.get("min_spread_bps", self.min_edge_bps))
        if net_edge < req_edge:
            return

        if f_snap.spread_bps >= gross_exec_edge_bps * 0.8:
            return

        top3_ask_depth_usd = f_snap.top_n_ask_depth_usd(3)
        if top3_ask_depth_usd < 50.0:
            return

        # Determine pool basket and actionable status
        is_battle = self.pool_state.is_battle_coin(coin) if self.pool_state else False
        basket = "battle" if is_battle else "incubator"
        is_actionable = is_battle

        # Calculate dynamic stealth order size
        if self.profile_manager:
            sized = self.profile_manager.calculate_stealth_order_size(follower_ex, top3_ask_depth_usd)
            suggested_size_usd = sized if sized is not None else min(
                self.max_position_budget_usd,
                self.stealth_fraction * top3_ask_depth_usd,
            )
        else:
            suggested_size_usd = min(
                self.max_position_budget_usd,
                self.stealth_fraction * top3_ask_depth_usd,
            )

        # Initial executable PnL at t=0 (if bought at f_ask and immediately sold at f_bid)
        init_exec_move_bps = (f_bid - f_ask) / f_ask * 1e4
        init_net_pnl_bps = round(init_exec_move_bps - self.roundtrip_fee_bps, 2)

        self._seq += 1
        sig = SunriseSignal(
            id=f"sun_{int(now)}_{self._seq:04d}",
            coin=coin,
            leader_exchange="upbit",
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
            net_exec_edge_bps=round(net_edge, 2),
            leader_latency_ms=upbit_snap.network_latency_ms,
            follower_latency_ms=f_snap.network_latency_ms,
            follower_book_age_ms=round(f_book_age_ms, 1),
            suggested_size_usd=round(suggested_size_usd, 2),
            basket=basket,
            is_actionable=is_actionable,
            mfe_exec_pnl_bps=init_net_pnl_bps,
            mae_exec_pnl_bps=init_net_pnl_bps,
            mfe_mid_move_bps=0.0,
            time_to_peak_sec=0.0,
            path=[[0.0, 0.0, 0.0, init_net_pnl_bps, round(net_edge, 2)]],
        )

        st.open_signal = sig
        self._active_signals[coin].append(sig)
        self.counters["signals_triggered"] += 1
        if is_actionable:
            self.counters["signals_battle"] += 1
            tag = "[SIGNAL-BATTLE] ⚔️"
        else:
            self.counters["signals_incubator"] += 1
            tag = "[SIGNAL-INCUBATOR] 🧪"

        log.info(
            f"{tag} {sig.coin} ({sig.follower_contract}) | Upbit -> {sig.follower_exchange} | "
            f"Trig: {sig.trigger_type} (Swept: ${sig.leader_swept_usd:.0f}) | "
            f"Edge: +{sig.net_exec_edge_bps:.1f} bps | Z: {sig.dev_z:.1f}σ | Size: ${sig.suggested_size_usd:.0f}"
        )

    # ----------------------------------------------------------- Tracking & Closure
    def _track_active_signals_for_coin(self, coin: str, now: float) -> None:
        signals = self._active_signals.get(coin)
        if not signals:
            return
        for sig in list(signals):
            if sig.closed:
                continue
            f_snap = self._books.get((sig.follower_exchange, coin))
            if f_snap is not None:
                self._track_follower_move(coin, sig.follower_exchange, f_snap, now)

    def _track_follower_move(self, coin: str, follower_ex: str, f_snap: OrderBookDepth5, now: float) -> None:
        signals = self._active_signals.get(coin)
        if not signals:
            return

        upbit_snap = self._books.get(("upbit", coin))
        if not upbit_snap:
            return

        for sig in list(signals):
            if sig.follower_exchange != follower_ex or sig.closed:
                continue

            dt = round(now - sig.open_ts, 3)

            # 1. Mid-to-Mid move on follower and leader (bps)
            f_mid_move = (f_snap.mid_price - sig.follower_mid) / sig.follower_mid * 1e4
            up_mid_move = (upbit_snap.mid_price - sig.leader_mid) / sig.leader_mid * 1e4

            # 2. True executable move on follower: Entry at best_ask -> Exit at current best_bid (bps)
            f_exec_move = (f_snap.best_bid - sig.follower_best_ask) / sig.follower_best_ask * 1e4
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
            cur_diff_bps = (upbit_snap.mid_price - f_snap.mid_price) / f_snap.mid_price * 1e4
            rem_edge = (cur_diff_bps - sig.kimchi_baseline_bps) - self.taker_fee_bps

            if not sig.path or dt > sig.path[-1][0]:
                sig.path.append([
                    dt,
                    round(up_mid_move, 2),
                    round(f_mid_move, 2),
                    cur_net_pnl,
                    round(rem_edge, 2),
                ])

            # Closure evaluation:
            if f_mid_move >= (sig.net_exec_edge_bps * 0.8):
                self._close_signal(sig, now, "converged", f_mid_move, f_exec_move)
            elif rem_edge <= 0.0:
                if f_mid_move >= (sig.net_exec_edge_bps * 0.35) and f_mid_move > 0:
                    self._close_signal(sig, now, "converged", f_mid_move, f_exec_move)
                else:
                    self._close_signal(sig, now, "reversed", f_mid_move, f_exec_move)
            elif dt >= self.max_hold_sec:
                self._close_signal(sig, now, "timeout", f_mid_move, f_exec_move)

    def _close_signal(
        self,
        sig: SunriseSignal,
        now: float,
        reason: str,
        f_mid_move_bps: float,
        f_exec_move_bps: float,
    ) -> None:
        sig.closed = True
        sig.close_ts = round(now, 3)
        sig.duration_sec = round(now - sig.open_ts, 3)
        sig.close_reason = reason
        sig.captured_mid_move_bps = round(f_mid_move_bps, 2)
        sig.captured_exec_move_bps = round(f_exec_move_bps, 2)
        sig.net_pnl_bps = round(f_exec_move_bps - self.roundtrip_fee_bps, 2)

        if reason == "converged":
            self.counters["signals_converged"] += 1
        elif reason == "reversed":
            self.counters["signals_reversed"] += 1
        else:
            self.counters["signals_timeout"] += 1

        self.emit(asdict(sig))

        pair_key = (sig.coin, sig.follower_exchange)
        st = self._pairs.get(pair_key)
        if st and st.open_signal == sig:
            st.open_signal = None

        if sig.coin in self._active_signals and sig in self._active_signals[sig.coin]:
            self._active_signals[sig.coin].remove(sig)

    def sweep_timeouts(self, now: float) -> None:
        """
        Sweeps and closes any active signals that exceeded max_hold_sec even if no new ticks arrived.
        Prevents open signals from lingering on illiquid or inactive coins.
        """
        for coin, signals in list(self._active_signals.items()):
            for sig in list(signals):
                if not sig.closed and (now - sig.open_ts) >= self.max_hold_sec:
                    f_snap = self._books.get((sig.follower_exchange, coin))
                    if f_snap:
                        f_mid_move = (f_snap.mid_price - sig.follower_mid) / sig.follower_mid * 1e4
                        f_exec_move = (f_snap.best_bid - sig.follower_best_ask) / sig.follower_best_ask * 1e4
                    else:
                        f_mid_move = 0.0
                        f_exec_move = 0.0
                    self._close_signal(sig, now, "timeout", f_mid_move, f_exec_move)