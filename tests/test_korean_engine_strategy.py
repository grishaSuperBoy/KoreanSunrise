"""
tests/test_korean_engine_strategy.py - Comprehensive Unit Test Suite for KoreanEngine Strategy Logic.

Exhaustively verifies every microstructural branch and invariant:
1. depth_drain trigger: Ask lifted mode (swept top level + top 5 depletion).
2. depth_drain trigger: Ask held mode (front-of-book depletion >= 50% top 5 loss).
3. depth_drain rejection: Deep-book pullbacks (best ask loss < 50% of top 5 loss).
4. spread_blowout trigger: Upward spread widening beyond blowout threshold.
5. Take-Profit Convergence: 75% of initial net edge captured.
6. Take-Profit Convergence: Dislocation basis absorbed (rem_edge <= 0) with net profit.
7. Concurrent Multi-Venue Tracking: Independent signal lifecycle on Binance and Bybit.
8. Illiquid Silence Sweeper: sweep_timeouts cleanly closes inactive signals.
9. Zero-Division & Corrupt Tick Resilience: Zero price protection in tracking & sweep.
10. Baseline Spike-Gating: 20x adaptation damping during extreme dislocation.
11. Portfolio Pruning Guard: Disabled pair rejection in calibrated thresholds.
"""
from __future__ import annotations

import unittest
import time
from typing import List, Dict, Any

from models import OrderBookDepth5, SunriseSignal
from korean_engine import KoreanEngine, PairState, UpbitNormState, evaluate_exit_reason


class TestKoreanEngineStrategy(unittest.TestCase):

    def setUp(self):
        self.emitted: List[Dict[str, Any]] = []
        self.engine = KoreanEngine(
            emit_callback=lambda sig: self.emitted.append(sig),
            drain_threshold=0.40,
            blowout_threshold=2.0,
            min_edge_bps=5.0,
            max_position_budget_usd=1000.0,
            stealth_depth_fraction=0.15,
            taker_fee_bps=5.0,
            roundtrip_fee_bps=10.0,
            warmup_samples=5,
            max_hold_sec=15.0,
            max_follower_book_age_sec=5.0,
            stop_invalidation_bps=15.0,
            stop_adverse_pnl_bps=35.0,
            alpha_decay_sec=8.0,
            require_trade_confirmation=False,
        )

    # -------------------------------------------------------------------------
    # 1. Depth Drain: Ask Lifted Mode
    # -------------------------------------------------------------------------
    def test_depth_drain_ask_lifted(self):
        coin = "SOL"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)
        self.assertTrue(st.is_warmed_up)

        # Baseline Upbit norm: depth 1000 USD, best ask 200 USD, spread 5 bps
        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 200.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        # Follower calm book: ask 100.0, bid 99.95 (mid 99.975)
        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="SOLUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0), (99.90, 20.0)],
            asks=[(100.00, 10.0), (100.05, 20.0), (100.10, 30.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        # Upbit previous tick: best ask 100.0 ($200), depth 1000 USD
        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 10.0)],
            asks=[(100.00, 2.0), (100.10, 8.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Upbit current tick: best ask LIFTS to 100.50 (+50 bps), depth drops to 400 USD
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 10.0)],
            asks=[(100.50, 4.0)]
        )

        self.engine.on_book_update(u_curr)

        self.assertIsNotNone(st.open_signal)
        sig = st.open_signal
        self.assertEqual(sig.trigger_type, "depth_drain")
        self.assertEqual(sig.coin, coin)
        self.assertEqual(sig.follower_exchange, "binance")
        self.assertGreaterEqual(sig.net_exec_edge_bps, 5.0)

    # -------------------------------------------------------------------------
    # 2. Depth Drain: Ask Held Mode (Front-of-Book Sweep)
    # -------------------------------------------------------------------------
    def test_depth_drain_ask_held(self):
        coin = "AVAX"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "bybit")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)
        self.assertTrue(st.is_warmed_up)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 500.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        # Follower book
        f_snap = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="AVAXUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)],
            asks=[(100.00, 10.0), (100.05, 20.0), (100.10, 30.0)]
        )
        self.engine._books[("bybit", coin)] = f_snap
        self.engine._recv_ts[("bybit", coin)] = now

        # Upbit previous tick: best ask 100.50 ($500), top5 depth $1000
        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(100.40, 5.0)],
            asks=[(100.50, 5.0), (100.60, 5.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Upbit current tick: best ask price HELD (still 100.50), but size drops from 5.0 -> 0.5 ($50 left)
        # best_ask_loss = 500 - 50 = 450 USD.
        # top5_loss = 1000 - 550 = 450 USD.
        # best_ask_loss (450) >= 0.5 * top5_loss (225) -> Front-of-book drain!
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(100.40, 5.0)],
            asks=[(100.50, 0.5), (100.60, 5.0)]
        )

        self.engine.on_book_update(u_curr)

        self.assertIsNotNone(st.open_signal)
        self.assertEqual(st.open_signal.trigger_type, "depth_drain")
        self.assertEqual(st.open_signal.follower_exchange, "bybit")

    # -------------------------------------------------------------------------
    # 3. Depth Drain Rejection: Deep Book Pull (Anti-Manipulation)
    # -------------------------------------------------------------------------
    def test_depth_drain_deep_book_pull_rejected(self):
        coin = "NEAR"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 2000.0,
            "best_ask_usd": 100.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="NEARUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)],
            asks=[(100.00, 10.0), (100.05, 20.0), (100.10, 30.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        # Upbit prev: best ask $100, level 5 has $1900
        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 5.0)],
            asks=[(100.50, 1.0), (101.00, 19.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Upbit curr: someone pulled $1000 from level 5, but best ask was untouched ($100 left)
        # best_ask_loss = 0. top5_loss = 1000 USD.
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 5.0)],
            asks=[(100.50, 1.0), (101.00, 9.0)]
        )

        self.engine.on_book_update(u_curr)

        # Signal must NOT trigger because depth depletion was not at the front of book
        self.assertIsNone(st.open_signal)

    # -------------------------------------------------------------------------
    # 4. Spread Blowout Trigger Detection
    # -------------------------------------------------------------------------
    def test_spread_blowout_trigger(self):
        coin = "DOT"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)

        # Norm spread = 2.0 bps, depth = 10,000 USD, best ask = 5000 USD
        self.engine._upbit_norm[coin] = {
            "depth_ask": 10000.0,
            "best_ask_usd": 5000.0,
            "spread_bps": 2.0,
            "t": now - 1.0,
        }

        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="DOTUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)],
            asks=[(100.00, 10.0), (100.05, 20.0), (100.10, 30.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        # Prev spread: bid 100.0, ask 100.02 (spread = 2 bps)
        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(100.00, 10.0)],
            asks=[(100.02, 10.0), (100.10, 80.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Curr: ask blows out to 100.50. Spread = (100.50 - 100.00)/100.25 * 1e4 = ~50 bps
        # Blowout ratio = 50.0 / 2.0 = 25.0 >= 2.0 blowout threshold!
        # Net edge = 15.0 bps >= 5.0 bps min edge!
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(100.00, 10.0)],
            asks=[(100.50, 80.0)]
        )

        self.engine.on_book_update(u_curr)

        self.assertIsNotNone(st.open_signal)
        self.assertEqual(st.open_signal.trigger_type, "spread_blowout")

    # -------------------------------------------------------------------------
    # 5. Take-Profit: 75% Net Edge Captured
    # -------------------------------------------------------------------------
    def test_convergence_75_pct_edge_captured(self):
        coin = "LINK"
        now = 1000.0
        st = self.engine._get_or_create_pair_state(coin, "binance")

        sig = SunriseSignal(
            id="sig_link_tp75",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="LINKUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=100.0,
            leader_best_ask=100.05,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=99.0,
            follower_best_bid=98.95,
            follower_best_ask=99.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=100.0,
            dev_z=3.0,
            gross_exec_edge_bps=50.0,
            net_exec_edge_bps=40.0,  # 75% hurdle is 30.0 bps
            follower_entry_px=100.00,
            suggested_size_usd=100.0,
            max_hold_sec=15.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        u_book = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.0, 10.0)], asks=[(100.1, 10.0)]
        )
        # Follower moves up strongly: exit price is 100.41
        # f_exec_move = (100.41 - 100.00) / 100.00 * 1e4 = +41.0 bps
        # cur_net_pnl = 41.0 - 10.0 = +31.0 bps >= 30.0 bps (75% of 40 bps)!
        f_book = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="LINKUSDT",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.41, 10.0)], asks=[(100.45, 10.0)]
        )
        self.engine._books[("upbit", coin)] = u_book
        self.engine._books[("binance", coin)] = f_book

        self.engine._track_follower_move(coin, "binance", f_book, now + 2.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "converged")
        self.assertAlmostEqual(sig.net_pnl_bps, 31.0, places=1)
        self.assertIsNone(st.open_signal)
        self.assertEqual(self.engine.counters["signals_converged"], 1)

    # -------------------------------------------------------------------------
    # 6. Take-Profit: Dislocation Absorbed (rem_edge <= 0 with positive PnL)
    # -------------------------------------------------------------------------
    def test_convergence_remaining_edge_absorbed(self):
        coin = "ADA"
        now = 1000.0
        st = self.engine._get_or_create_pair_state(coin, "binance")

        sig = SunriseSignal(
            id="sig_ada_rem",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="ADAUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=100.0,
            leader_best_ask=100.05,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=99.0,
            follower_best_bid=98.95,
            follower_best_ask=99.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=100.0,
            dev_z=3.0,
            gross_exec_edge_bps=50.0,
            net_exec_edge_bps=40.0,
            follower_entry_px=100.00,
            suggested_size_usd=100.0,
            max_hold_sec=15.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Basis completely converges: Upbit mid = 100.20, Follower mid = 100.20 (diff = 0 bps)
        # rem_edge = (0 - 0) - taker_fee (5.0) = -5.0 bps <= 0.
        # Follower exit price = 100.25 -> f_exec_move = +25 bps, net PnL = +15 bps > 0!
        u_book = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.18, 10.0)], asks=[(100.22, 10.0)]
        )
        f_book = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="ADAUSDT",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.25, 10.0)], asks=[(100.30, 10.0)]
        )
        self.engine._books[("upbit", coin)] = u_book
        self.engine._books[("binance", coin)] = f_book

        self.engine._track_follower_move(coin, "binance", f_book, now + 2.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "converged")
        self.assertGreater(sig.net_pnl_bps, 0.0)

    # -------------------------------------------------------------------------
    # 7. Concurrent Multi-Venue Signal Tracking (Binance & Bybit)
    # -------------------------------------------------------------------------
    def test_concurrent_multi_venue_tracking(self):
        coin = "ETH"
        now = time.time()

        st_bin = self.engine._get_or_create_pair_state(coin, "binance")
        st_byb = self.engine._get_or_create_pair_state(coin, "bybit")
        for i in range(6):
            st_bin.learn(0.0, now - 30.0 + i * 2.0)
            st_byb.learn(0.0, now - 30.0 + i * 2.0)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 200.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        # Follower books on both Binance and Bybit
        f_bin = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="ETHUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)], asks=[(100.00, 10.0), (100.05, 20.0)]
        )
        f_byb = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="ETHUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)], asks=[(100.00, 10.0), (100.05, 20.0)]
        )
        self.engine._books[("binance", coin)] = f_bin
        self.engine._recv_ts[("binance", coin)] = now
        self.engine._books[("bybit", coin)] = f_byb
        self.engine._recv_ts[("bybit", coin)] = now

        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 10.0)], asks=[(100.00, 2.0), (100.10, 8.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Upbit sweeps ask -> triggers lead-lag
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 10.0)], asks=[(100.60, 4.0)]
        )
        self.engine.on_book_update(u_curr)

        # Both pairs must have open signals
        self.assertIsNotNone(st_bin.open_signal)
        self.assertIsNotNone(st_byb.open_signal)
        self.assertEqual(len(self.engine._active_signals[coin]), 2)

        sig_bin = st_bin.open_signal
        sig_byb = st_byb.open_signal

        # Binance moves up -> Take-Profit convergence
        f_bin_up = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="ETHUSDT",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.55, 10.0)], asks=[(100.60, 10.0)]
        )
        self.engine._books[("binance", coin)] = f_bin_up
        self.engine._track_follower_move(coin, "binance", f_bin_up, now + 2.0)

        self.assertTrue(sig_bin.closed)
        self.assertEqual(sig_bin.close_reason, "converged")
        # Bybit signal remains open!
        self.assertFalse(sig_byb.closed)
        self.assertEqual(len(self.engine._active_signals[coin]), 1)

        # Bybit stalls until alpha decay (dt >= 8.0s) with 0 PnL
        f_byb_flat = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="ETHUSDT",
            ts=now + 8.5, exchange_ts=int((now + 8.5) * 1000),
            bids=[(99.95, 10.0)], asks=[(100.00, 10.0)]
        )
        self.engine._books[("bybit", coin)] = f_byb_flat
        self.engine._track_follower_move(coin, "bybit", f_byb_flat, now + 8.5)

        self.assertTrue(sig_byb.closed)
        self.assertEqual(sig_byb.close_reason, "stop_decay")
        self.assertEqual(len(self.engine._active_signals.get(coin, [])), 0)

    # -------------------------------------------------------------------------
    # 8. Illiquid Silence Sweeper (sweep_timeouts)
    # -------------------------------------------------------------------------
    def test_timeout_sweep_on_illiquid_silence(self):
        coin = "SHITCOIN"
        now = 1000.0
        st = self.engine._get_or_create_pair_state(coin, "binance")

        sig = SunriseSignal(
            id="sig_shitcoin_to",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="SHITCOINUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=1.0,
            leader_best_ask=1.01,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=0.99,
            follower_best_bid=0.985,
            follower_best_ask=0.995,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=100.0,
            dev_z=3.0,
            gross_exec_edge_bps=50.0,
            net_exec_edge_bps=40.0,
            follower_entry_px=0.995,
            suggested_size_usd=100.0,
            max_hold_sec=15.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Zero ticks arrive. Time advances by 16.0s (past max_hold_sec = 15.0s)
        self.engine.sweep_timeouts(now + 16.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "timeout")
        self.assertIsNone(st.open_signal)
        self.assertEqual(len(self.engine._active_signals.get(coin, [])), 0)
        self.assertEqual(len(self.emitted), 1)

    # -------------------------------------------------------------------------
    # 9. Zero-Division & Corrupt Orderbook Resilience
    # -------------------------------------------------------------------------
    def test_zero_division_resilience(self):
        coin = "ZERO"
        now = 1000.0
        st = self.engine._get_or_create_pair_state(coin, "binance")

        sig = SunriseSignal(
            id="sig_zero_resilience",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="ZEROUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=0.0,         # Corrupt 0.0 mid
            leader_best_ask=0.0,
            leader_spread_bps=0.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=0.0,       # Corrupt 0.0 mid
            follower_best_bid=0.0,
            follower_best_ask=0.0,
            follower_spread_bps=0.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=0.0,
            dev_z=0.0,
            gross_exec_edge_bps=0.0,
            net_exec_edge_bps=0.0,
            follower_entry_px=0.0,  # Corrupt 0.0 entry
            suggested_size_usd=100.0,
            max_hold_sec=10.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Corrupt orderbook with 0.0 prices
        u_corrupt = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[], asks=[]
        )
        f_corrupt = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="ZEROUSDT",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[], asks=[]
        )
        self.engine._books[("upbit", coin)] = u_corrupt
        self.engine._books[("binance", coin)] = f_corrupt

        # Must not raise ZeroDivisionError!
        try:
            self.engine._track_follower_move(coin, "binance", f_corrupt, now + 1.0)
            self.engine.sweep_timeouts(now + 12.0)
        except ZeroDivisionError as e:
            self.fail(f"KoreanEngine raised ZeroDivisionError on zero prices: {e}")

        self.assertTrue(sig.closed)

    # -------------------------------------------------------------------------
    # 10. Baseline Spike-Gating Slowdown
    # -------------------------------------------------------------------------
    def test_baseline_spike_gating_slowdown(self):
        st = PairState("SOL", "binance", warmup_samples=5, spike_sigma=2.5)
        now = 1000.0

        # Warm up with regular 10.0 bps diff
        for i in range(10):
            st.learn(10.0, now + i)
        self.assertTrue(st.is_warmed_up)
        initial_baseline = st.baseline_bps
        self.assertAlmostEqual(initial_baseline, 10.0, places=1)

        # Massive pump: 1000 bps deviation (>> 2.5 * sigma)
        # Spike gating activates: eff_tau is multiplied by 20x (72,000s)
        # Baseline should barely move!
        st.learn(1010.0, now + 11.0)

        # In 1 second with 72,000s tau: a = 1 - exp(-1/72000) ~ 1.38e-5
        # Expected baseline shift: ~1000 * 1.38e-5 = ~0.014 bps!
        self.assertAlmostEqual(st.baseline_bps, initial_baseline, delta=0.5)

    # -------------------------------------------------------------------------
    # 11. Pruned / Disabled Pair Rejection
    # -------------------------------------------------------------------------
    def test_disabled_pair_rejection(self):
        coin = "BADCOIN"
        now = time.time()

        # Inject disabled pair threshold
        pk = f"{coin}:upbit-binance"
        self.engine._thresholds[pk] = {"enabled": False, "min_spread_bps": 5.0}

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 200.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol=f"{coin}USDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)], asks=[(100.00, 10.0), (100.05, 20.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 10.0)], asks=[(100.00, 2.0)]
        )
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 10.0)], asks=[(105.00, 2.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        self.engine.on_book_update(u_curr)

        # Must be rejected because pair was pruned/disabled in portfolio thresholds
        self.assertIsNone(st.open_signal)
        self.assertEqual(len(self.engine._active_signals.get(coin, [])), 0)

    # -------------------------------------------------------------------------
    # 12. Pure Exit Evaluator Parametric Verification
    # -------------------------------------------------------------------------
    def test_evaluate_exit_reason_pure_function(self):
        # 1. Take-Profit: 75% edge
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=30.0, net_exec_edge_bps=40.0, rem_edge=10.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=20.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "converged")
        self.assertIsNone(stype)

        # 2. Take-Profit: basis fully absorbed in profit
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=15.0, net_exec_edge_bps=40.0, rem_edge=-2.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=-5.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "converged")
        self.assertIsNone(stype)

        # 3. Model A Invalidation: Upbit dump >= 1.5 * sigma (4.5 bps)
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=-5.0, net_exec_edge_bps=40.0, rem_edge=20.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=5.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "stop_invalidation")
        self.assertEqual(stype, "invalidation")

        # 4. Model A Invalidation: dislocation <= -stop_invalidation_bps (-15.0 bps)
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=-5.0, net_exec_edge_bps=40.0, rem_edge=0.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=-16.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "stop_invalidation")
        self.assertEqual(stype, "invalidation")

        # 5. Model B Adverse PnL: cur_net_pnl <= -35.0 bps
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=-36.0, net_exec_edge_bps=40.0, rem_edge=20.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "stop_adverse_pnl")
        self.assertEqual(stype, "adverse_pnl")

        # 6. Model C Alpha Decay: dt >= 8.0s and PnL <= 0
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=-2.0, net_exec_edge_bps=40.0, rem_edge=10.0,
            dt=8.5, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "stop_decay")
        self.assertEqual(stype, "alpha_decay")

        # 7. Max Hold Horizon: dt >= 15.0s
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=5.0, net_exec_edge_bps=40.0, rem_edge=10.0,
            dt=15.5, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "timeout")
        self.assertIsNone(stype)

        # 8. Normal open state: no exit
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=5.0, net_exec_edge_bps=40.0, rem_edge=10.0,
            dt=3.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertIsNone(reason)
        self.assertIsNone(stype)

    # -------------------------------------------------------------------------
    # 13. UpbitNormState Slots and Dict Compatibility
    # -------------------------------------------------------------------------
    def test_upbit_norm_state_slots_and_dict_compatibility(self):
        norm = UpbitNormState(depth_ask=1200.0, best_ask_usd=300.0, spread_bps=4.5, t=100.0)
        # Attribute access
        self.assertEqual(norm.depth_ask, 1200.0)
        self.assertEqual(norm.best_ask_usd, 300.0)
        self.assertEqual(norm.spread_bps, 4.5)
        self.assertEqual(norm.t, 100.0)

        # Dict-like __getitem__ and __setitem__ compatibility
        self.assertEqual(norm["depth_ask"], 1200.0)
        norm["depth_ask"] = 1500.0
        self.assertEqual(norm.depth_ask, 1500.0)


if __name__ == "__main__":
    unittest.main()
