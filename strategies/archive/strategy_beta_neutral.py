"""
strategies/strategy_6_beta_neutral.py - Strategy 6: Beta-Neutral Pairs Trading (Alt Long + BTC Short).

Microstructure Simulation:
Isolates pure Korean idiosyncratic alpha from broader cryptocurrency market risk:
    Portfolio = Long Altcoin (Follower Perp) + Short BTC (Binance Perp)
    Notional_BTC = Beta * Notional_Alt

When systemic macro risk strikes (BTC dips or shakes out), the short BTC hedge protects the
position, while the Korean retail buying pressure drives relative outperformance on the altcoin.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import itertools
import logging
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

log = logging.getLogger("strategy.beta_neutral")


@dataclass
class ActiveBetaNeutralPair:
    id: str
    coin: str
    follower_exchange: str
    follower_contract: str
    open_ts: float

    # Sizing & Prices
    alt_size_usd: float
    btc_size_usd: float
    alt_entry_px: float               # Executable Ask for Altcoin
    btc_entry_px: float               # Executable Bid for BTC hedge
    entry_upbit_mid: float
    entry_follower_mid: float
    beta: float = 1.3
    spread_open_bps: float = 0.0      # Follower spread at open to avoid spread-trap stops

    # Targets & Risk Parameters
    target_pnl_bps: float = 30.0
    stop_loss_bps: float = 20.0
    roundtrip_fee_bps: float = 12.0   # Covers two roundtrips (alt perp + btc perp)
    max_hold_sec: float = 45.0
    alpha_decay_sec: float = 20.0

    # Lifecycle state
    closed: bool = False
    close_ts: Optional[float] = None
    duration_sec: Optional[float] = None
    close_reason: Optional[str] = None
    alt_exit_px: Optional[float] = None
    btc_exit_px: Optional[float] = None
    net_pnl_bps: Optional[float] = None
    mfe_exec_pnl_bps: float = -999.0
    mae_exec_pnl_bps: float = 999.0

    path: List[List[float]] = field(default_factory=list)


class BetaNeutralStrategy:
    """
    Quantitative execution engine for Strategy 6: Beta-Neutral Pairs Trading.
    Hedges Korean momentum against macro BTC beta.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        min_order_usd: float = 10.0,
        max_position_budget_usd: float = 500.0,
        stealth_depth_fraction: float = 0.15,
        default_beta: float = 1.3,
        min_edge_bps: float = 35.0,
        target_pnl_bps: float = 30.0,
        stop_loss_bps: float = 20.0,
        roundtrip_fee_bps: float = 12.0,
        max_hold_sec: float = 45.0,
        alpha_decay_sec: float = 20.0,
        cooldown_sec: float = 30.0,
        max_active_pairs: int = 5,
        max_total_exposure_usd: float = 5000.0,
        enabled: bool = False,         # Disabled by default until user verification
        counters: Optional[Dict[str, int]] = None,
    ):
        self.emit = emit_callback
        self.min_order_usd = min_order_usd
        self.max_position_budget_usd = max_position_budget_usd
        self.stealth_depth_fraction = stealth_depth_fraction
        self.default_beta = default_beta
        self.min_edge_bps = min_edge_bps
        self.target_pnl_bps = target_pnl_bps
        self.stop_loss_bps = stop_loss_bps
        self.roundtrip_fee_bps = roundtrip_fee_bps
        self.max_hold_sec = max_hold_sec
        self.alpha_decay_sec = alpha_decay_sec
        self.cooldown_sec = cooldown_sec
        self.max_active_pairs = max_active_pairs
        self.max_total_exposure_usd = max_total_exposure_usd
        self.enabled = enabled
        self.counters = counters if counters is not None else {}

        self._lock = threading.Lock()
        self._seq = itertools.count(1)

        # Active pairs: id -> ActiveBetaNeutralPair
        self._active_pairs: Dict[str, ActiveBetaNeutralPair] = {}
        # Quick lookup: (follower_ex, coin) -> id
        self._coin_pos_map: Dict[Tuple[str, str], str] = {}
        # Cooldown: (follower_ex, coin) -> last_trigger_ts
        self._last_trigger_ts: Dict[Tuple[str, str], float] = {}

    def check_pair_opportunity(
        self,
        follower_ex: str,
        coin: str,
        follower_snap: Any,
        upbit_snap: Optional[Any],
        btc_snap: Optional[Any],
        now: float,
        beta: Optional[float] = None,
        recv_ts: float = 0.0,
    ) -> Optional[ActiveBetaNeutralPair]:
        """Evaluates entry into a beta-neutral pair (Long Alt + Short BTC)."""
        if not self.enabled:
            return None

        if coin.upper() == "BTC" or upbit_snap is None or follower_snap is None or btc_snap is None:
            return None

        f_age = now - recv_ts if recv_ts > 0 else 0.0
        if f_age > 1.5:
            return None

        upbit_mid = getattr(upbit_snap, "mid_price", 0.0)
        f_mid = getattr(follower_snap, "mid_price", 0.0)
        btc_mid = getattr(btc_snap, "mid_price", 0.0)
        if upbit_mid <= 0.0 or f_mid <= 0.0 or btc_mid <= 0.0:
            return None

        f_spread_bps = getattr(follower_snap, "spread_bps", 999.0)
        if f_spread_bps > 30.0:
            return None  # Disregard illiquid blown-out books

        # Dislocation check
        dislocation_bps = (upbit_mid - f_mid) / f_mid * 1e4
        if dislocation_bps < self.min_edge_bps:
            return None

        best_ask_alt = getattr(follower_snap, "best_ask", 0.0)
        best_bid_btc = getattr(btc_snap, "best_bid", 0.0)
        if best_ask_alt <= 0.0 or best_bid_btc <= 0.0:
            return None

        # Minimum book depth check
        f_ask_depth = getattr(follower_snap, "top3_ask_usd", 0.0)
        if hasattr(follower_snap, "top_n_ask_depth_usd"):
            f_ask_depth = follower_snap.top_n_ask_depth_usd(3)

        if f_ask_depth <= 0.0 or f_ask_depth < self.min_order_usd:
            return None  # Dead book / insufficient liquidity

        stealth_size = f_ask_depth * self.stealth_depth_fraction
        alt_size_usd = max(self.min_order_usd, min(stealth_size, self.max_position_budget_usd))
        if alt_size_usd < self.min_order_usd:
            return None

        actual_beta = beta if beta is not None else self.default_beta
        btc_size_usd = round(alt_size_usd * actual_beta, 2)
        prospective_exposure = alt_size_usd + btc_size_usd

        key = (follower_ex, coin)
        with self._lock:
            if key in self._coin_pos_map:
                return None

            last_trig = self._last_trigger_ts.get(key, 0.0)
            if (now - last_trig) < self.cooldown_sec:
                return None

            if len(self._active_pairs) >= self.max_active_pairs:
                return None

            total_exp = sum(p.alt_size_usd + p.btc_size_usd for p in self._active_pairs.values())
            if total_exp + prospective_exposure > self.max_total_exposure_usd:
                return None

        seq = next(self._seq)
        pos_id = f"bn_{int(now)}_{seq}"
        sym = getattr(follower_snap, "symbol", coin)

        pair = ActiveBetaNeutralPair(
            id=pos_id,
            coin=coin,
            follower_exchange=follower_ex,
            follower_contract=sym,
            open_ts=now,
            alt_size_usd=alt_size_usd,
            btc_size_usd=btc_size_usd,
            beta=actual_beta,
            alt_entry_px=best_ask_alt,
            btc_entry_px=best_bid_btc,
            entry_upbit_mid=upbit_mid,
            entry_follower_mid=f_mid,
            spread_open_bps=f_spread_bps,
            target_pnl_bps=self.target_pnl_bps,
            stop_loss_bps=self.stop_loss_bps,
            roundtrip_fee_bps=self.roundtrip_fee_bps,
            max_hold_sec=self.max_hold_sec,
            alpha_decay_sec=self.alpha_decay_sec,
        )

        with self._lock:
            self._active_pairs[pos_id] = pair
            self._coin_pos_map[key] = pos_id
            self._last_trigger_ts[key] = now

        self.counters["beta_neutral_triggered"] = self.counters.get("beta_neutral_triggered", 0) + 1
        log.info(
            f"[S6-BETA-NEUTRAL] ⚖️ Opened Hedged Pair: Long {coin} (${alt_size_usd:.1f}) + "
            f"Short BTC (${btc_size_usd:.1f}, β={actual_beta:.2f}) @ {follower_ex} | "
            f"Edge={dislocation_bps:+.1f} bps"
        )
        return pair

    def on_tick_update(
        self,
        follower_ex: str,
        coin: str,
        alt_snap: Any,
        btc_snap: Any,
        now: float,
    ) -> Optional[ActiveBetaNeutralPair]:
        """Evaluates net pair performance considering altcoin move and BTC short hedge move."""
        key = (follower_ex, coin)
        pair: Optional[ActiveBetaNeutralPair] = None

        with self._lock:
            pos_id = self._coin_pos_map.get(key)
            if pos_id is not None:
                pair = self._active_pairs.get(pos_id)

        if pair is None or pair.closed:
            return None

        alt_best_bid = getattr(alt_snap, "best_bid", 0.0)
        btc_best_ask = getattr(btc_snap, "best_ask", 0.0)
        if alt_best_bid <= 0.0 or btc_best_ask <= 0.0:
            return None

        dt = max(0.001, now - pair.open_ts)

        # 1. Long Altcoin Move (in bps)
        alt_move_bps = (alt_best_bid - pair.alt_entry_px) / pair.alt_entry_px * 1e4

        # 2. Short BTC Move (in bps: positive when BTC drops)
        btc_short_move_bps = (pair.btc_entry_px - btc_best_ask) / pair.btc_entry_px * 1e4

        # 3. Weighted Net Pair PnL:
        # Net Return = (Weight_Alt * Ret_Alt + Weight_BTC * Ret_BTC_Short) - Fee
        tot_cap = pair.alt_size_usd + pair.btc_size_usd
        w_alt = pair.alt_size_usd / tot_cap
        w_btc = pair.btc_size_usd / tot_cap
        net_pair_pnl_bps = (w_alt * alt_move_bps + w_btc * btc_short_move_bps) - pair.roundtrip_fee_bps

        if not pair.path or (now - pair.path[-1][0]) >= 0.5:
            pair.path.append([round(now, 3), round(net_pair_pnl_bps, 2)])

        if net_pair_pnl_bps > pair.mfe_exec_pnl_bps:
            pair.mfe_exec_pnl_bps = net_pair_pnl_bps
        if net_pair_pnl_bps < pair.mae_exec_pnl_bps:
            pair.mae_exec_pnl_bps = net_pair_pnl_bps

        close_reason: Optional[str] = None
        if net_pair_pnl_bps >= pair.target_pnl_bps:
            close_reason = "target_converged"
        elif net_pair_pnl_bps <= -(pair.spread_open_bps * 0.5 + pair.roundtrip_fee_bps + pair.stop_loss_bps):
            close_reason = "stop_loss"
        elif dt >= pair.alpha_decay_sec and net_pair_pnl_bps <= 0.0:
            close_reason = "stop_decay"
        elif dt >= pair.max_hold_sec:
            close_reason = "timeout"

        if close_reason is not None:
            self._close_pair(pair, close_reason, alt_best_bid, btc_best_ask, net_pair_pnl_bps, now)
            return pair

        return None

    def _close_pair(
        self,
        pair: ActiveBetaNeutralPair,
        close_reason: str,
        alt_exit_px: float,
        btc_exit_px: float,
        net_pnl_bps: float,
        now: float,
    ) -> None:
        key = (pair.follower_exchange, pair.coin)
        with self._lock:
            pair.closed = True
            pair.close_ts = now
            pair.duration_sec = round(now - pair.open_ts, 3)
            pair.close_reason = close_reason
            pair.alt_exit_px = alt_exit_px
            pair.btc_exit_px = btc_exit_px
            pair.net_pnl_bps = round(net_pnl_bps, 2)
            pair.mfe_exec_pnl_bps = round(pair.mfe_exec_pnl_bps, 2)
            pair.mae_exec_pnl_bps = round(pair.mae_exec_pnl_bps, 2)

            self._active_pairs.pop(pair.id, None)
            self._coin_pos_map.pop(key, None)

        if close_reason == "target_converged":
            self.counters["beta_neutral_converged"] = self.counters.get("beta_neutral_converged", 0) + 1
        elif "stop" in close_reason:
            self.counters["beta_neutral_stopped"] = self.counters.get("beta_neutral_stopped", 0) + 1
        elif close_reason == "timeout":
            self.counters["beta_neutral_timeout"] = self.counters.get("beta_neutral_timeout", 0) + 1

        rec = {
            "strategy": "beta_neutral_pairs",
            "strategy_id": 6,
            "strategy_name": "Beta-Neutral Pairs Trading",
            "id": pair.id,
            "coin": pair.coin,
            "follower_exchange": pair.follower_exchange,
            "follower_contract": pair.follower_contract,
            "open_ts": pair.open_ts,
            "close_ts": pair.close_ts,
            "duration_sec": pair.duration_sec,
            "close_reason": pair.close_reason,
            "beta": pair.beta,
            "alt_size_usd": pair.alt_size_usd,
            "btc_size_usd": pair.btc_size_usd,
            "alt_entry_px": pair.alt_entry_px,
            "alt_exit_px": pair.alt_exit_px,
            "btc_entry_px": pair.btc_entry_px,
            "btc_exit_px": pair.btc_exit_px,
            "net_pnl_bps": pair.net_pnl_bps,
            "mfe_exec_pnl_bps": pair.mfe_exec_pnl_bps,
            "mae_exec_pnl_bps": pair.mae_exec_pnl_bps,
            "target_pnl_bps": pair.target_pnl_bps,
            "path": pair.path,
        }

        try:
            self.emit(rec)
        except Exception as e:
            log.error(f"[S6-BETA-NEUTRAL] Failed to emit record {pair.id}: {e}")

        log.info(
            f"[S6-BETA-NEUTRAL] Closed {pair.coin}-BTC ({pair.follower_exchange}): "
            f"PnL={pair.net_pnl_bps:+.1f} bps | Reason={close_reason} | Duration={pair.duration_sec:.1f}s"
        )

    def sweep_timeouts(self, now: float) -> None:
        stale: List[ActiveBetaNeutralPair] = []
        with self._lock:
            for pair in list(self._active_pairs.values()):
                if (now - pair.open_ts) >= pair.max_hold_sec:
                    stale.append(pair)

        for pair in stale:
            net_pnl = -pair.roundtrip_fee_bps
            self._close_pair(pair, "timeout", pair.alt_entry_px, pair.btc_entry_px, net_pnl, now)
