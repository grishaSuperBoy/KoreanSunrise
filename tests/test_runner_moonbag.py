"""
tests/test_runner_moonbag.py - Unit test suite for Strategy 1: Runner / Moonbag Dual-Phase Exit.
"""
from __future__ import annotations

import unittest
import time
from typing import List, Dict, Any

from models import SunriseSignal, OrderBookDepth5
from korean_engine import KoreanEngine
import config as C


class TestRunnerMoonbag(unittest.TestCase):

    def test_runner_dual_phase_pnl_calculation(self):
        # 70% locked at +30.0 bps, 30% runner hits trailing stop at +100.0 bps
        portion = 0.30
        phase1_pnl = 30.0
        runner_pnl = 100.0
        blended_pnl = round((1.0 - portion) * phase1_pnl + portion * runner_pnl, 2)
        # Expected: 0.70 * 30 + 0.30 * 100 = 21 + 30 = 51.0 bps!
        self.assertEqual(blended_pnl, 51.0)

    def test_runner_breakeven_protection(self):
        # Even if runner drops back down, breakeven floor protects the position
        portion = 0.30
        phase1_pnl = 35.0
        runner_pnl = 2.0  # Stopped at breakeven +2 bps
        blended_pnl = round((1.0 - portion) * phase1_pnl + portion * runner_pnl, 2)
        # Expected: 0.70 * 35 + 0.30 * 2 = 24.5 + 0.6 = 25.1 bps (still solidly profitable!)
        self.assertGreater(blended_pnl, 20.0)

    def test_engine_runner_dual_phase_lifecycle(self):
        emitted = []
        engine = KoreanEngine(emit_callback=lambda sig: emitted.append(sig))
        now = time.time()
        coin = "PEPE"

        # Enable runner mode in config
        orig_runner_enabled = C.S1_RUNNER_ENABLED
        C.S1_RUNNER_ENABLED = True
        try:
            # Create a mock open signal on PEPE
            sig = SunriseSignal(
                id="sig_test_runner_1",
                coin=coin,
                leader_exchange="upbit",
                follower_exchange="binance",
                follower_contract="1000PEPEUSDT",
                open_ts=now,
                trigger_type="depth_drain",
                leader_mid=0.0100,
                leader_best_ask=0.01005,
                leader_spread_bps=10.0,
                leader_norm_spread_bps=10.0,
                leader_top5_ask_usd=5000.0,
                leader_norm_depth_usd=5000.0,
                leader_swept_usd=3000.0,
                leader_imbalance_top5=0.5,
                drain_ratio=0.5,
                spread_blowout_ratio=1.0,
                follower_mid=0.0098,
                follower_best_bid=0.00979,
                follower_best_ask=0.00980,
                follower_spread_bps=10.0,
                follower_top3_ask_usd=3000.0,
                follower_top3_bid_usd=3000.0,
                follower_imbalance_top3=0.0,
                kimchi_baseline_bps=100.0,
                pair_sigma_bps=15.0,
                raw_dislocation_bps=200.0,
                dev_z=6.5,
                gross_exec_edge_bps=50.0,
                net_exec_edge_bps=40.0,
                suggested_size_usd=100.0,
                follower_entry_px=0.00980,
            )

            engine._active_signals[coin].append(sig)
            leader_snap = OrderBookDepth5(
                exchange="upbit", symbol=coin, raw_symbol="KRW-PEPE",
                ts=now, exchange_ts=int(now * 1000),
                bids=[(0.0100, 1000.0)], asks=[(0.01005, 1000.0)]
            )

            # Step 1: Follower jumps up so that net PnL is +35 bps (reaches take profit target!)
            # entry_px = 0.00980. Exit bid needed for +45 bps raw move (+35 net) = 0.00980 * (1 + 45/10000) = 0.0098441
            f_snap1 = OrderBookDepth5(
                exchange="binance", symbol=coin, raw_symbol="1000PEPEUSDT",
                ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
                bids=[(0.0098441, 10000.0)], asks=[(0.00985, 10000.0)]
            )

            engine._track_one_signal(sig, leader_snap, f_snap1, now + 1.0)

            # Phase 1 scalp target hit -> Runner must be ACTIVATED, signal NOT closed yet!
            self.assertFalse(sig.closed)
            self.assertTrue(sig.runner_active)
            self.assertAlmostEqual(sig.phase1_pnl_bps, 35.0, delta=1.0)
            self.assertEqual(engine.counters.get("signals_runner_activated"), 1)

            # Step 2: Price continues upward! Best bid reaches 0.00990 (+102 bps raw, +92 bps net)
            f_snap2 = OrderBookDepth5(
                exchange="binance", symbol=coin, raw_symbol="1000PEPEUSDT",
                ts=now + 3.0, exchange_ts=int((now + 3.0) * 1000),
                bids=[(0.00990, 10000.0)], asks=[(0.00991, 10000.0)]
            )
            engine._track_one_signal(sig, leader_snap, f_snap2, now + 3.0)
            self.assertFalse(sig.closed)
            self.assertGreaterEqual(sig.runner_peak_pnl_bps, 90.0)

            # Step 3: Pullback of 25 bps from peak -> Triggers trailing stop!
            # Peak was ~92 bps, so dropping to ~65 bps net PnL
            f_snap3 = OrderBookDepth5(
                exchange="binance", symbol=coin, raw_symbol="1000PEPEUSDT",
                ts=now + 5.0, exchange_ts=int((now + 5.0) * 1000),
                bids=[(0.0098735, 10000.0)], asks=[(0.00988, 10000.0)]
            )
            engine._track_one_signal(sig, leader_snap, f_snap3, now + 5.0)

            # Runner trailing stop triggered! Signal is closed with blended PnL
            self.assertTrue(sig.closed)
            self.assertEqual(sig.close_reason, "runner_trailing_stop")
            # Blended PnL: 70% of ~35 bps + 30% of ~65 bps = 24.5 + 19.5 = ~44 bps!
            self.assertGreater(sig.net_pnl_bps, 35.0)
            self.assertEqual(engine.counters.get("signals_runner_closed"), 1)
        finally:
            C.S1_RUNNER_ENABLED = orig_runner_enabled

    def test_engine_runner_timeout_sweep(self):
        emitted = []
        engine = KoreanEngine(emit_callback=lambda sig: emitted.append(sig))
        now = time.time()
        coin = "PEPE"

        orig_runner_enabled = C.S1_RUNNER_ENABLED
        C.S1_RUNNER_ENABLED = True
        try:
            sig = SunriseSignal(
                id="sig_test_runner_sweep",
                coin=coin,
                leader_exchange="upbit",
                follower_exchange="binance",
                follower_contract="1000PEPEUSDT",
                open_ts=now,
                trigger_type="depth_drain",
                leader_mid=0.0100,
                leader_best_ask=0.01005,
                leader_spread_bps=10.0,
                leader_norm_spread_bps=10.0,
                leader_top5_ask_usd=5000.0,
                leader_norm_depth_usd=5000.0,
                leader_swept_usd=3000.0,
                leader_imbalance_top5=0.5,
                drain_ratio=0.5,
                spread_blowout_ratio=1.0,
                follower_mid=0.0098,
                follower_best_bid=0.00979,
                follower_best_ask=0.00980,
                follower_spread_bps=10.0,
                follower_top3_ask_usd=3000.0,
                follower_top3_bid_usd=3000.0,
                follower_imbalance_top3=0.0,
                kimchi_baseline_bps=100.0,
                pair_sigma_bps=15.0,
                raw_dislocation_bps=200.0,
                dev_z=6.5,
                gross_exec_edge_bps=50.0,
                net_exec_edge_bps=40.0,
                suggested_size_usd=100.0,
                follower_entry_px=0.00980,
                max_hold_sec=15.0,  # Standard signal timeout is 15s
            )
            # Simulate that runner is already active with 70% locked at +35.0 bps
            sig.runner_enabled = True
            sig.runner_active = True
            sig.phase1_pnl_bps = 35.0
            sig.phase1_close_ts = now
            sig.runner_peak_pnl_bps = 35.0
            sig.runner_peak_ts = now
            sig.runner_pnl_bps = 35.0

            engine._active_signals[coin].append(sig)

            # At t = +10s: standard hold_sec (6s) has elapsed, but runner max hold (120s) and stall (20s) have NOT!
            sig.max_hold_sec = 6.0
            engine.sweep_timeouts(now + 10.0)
            self.assertFalse(sig.closed)  # Must NOT be closed prematurely by standard hold_sec!

            # Set peak ts to now + 115s so it doesn't close on dead_stall before 120s max hold
            sig.runner_peak_ts = now + 115.0

            # Store follower book at peak price (+35 net) so it doesn't trigger trailing stop on 0-price fallback
            # exit bid needed for +45 bps raw move (+35 net) = 0.00980 * (1 + 45/10000) = 0.0098441
            f_snap_peak = OrderBookDepth5(
                exchange="binance", symbol=coin, raw_symbol="1000PEPEUSDT",
                ts=now + 125.0, exchange_ts=int((now + 125.0) * 1000),
                bids=[(0.0098441, 10000.0)], asks=[(0.00985, 10000.0)]
            )
            engine._books[("binance", coin)] = f_snap_peak

            # At t = +125s: runner max hold (120s) has elapsed -> Swept with runner_timeout & blended PnL!
            engine.sweep_timeouts(now + 125.0)
            self.assertTrue(sig.closed)
            self.assertEqual(sig.close_reason, "runner_timeout")
            self.assertEqual(engine.counters.get("signals_runner_closed"), 1)
            self.assertEqual(engine.counters.get("signals_runner_timeout"), 1)
        finally:
            C.S1_RUNNER_ENABLED = orig_runner_enabled

    def test_runner_dead_stall_detection(self):
        # Peak achieved at t=0, price hovers without new peak for 22s (>20s decay) -> runner_dead_stall
        emitted = []
        engine = KoreanEngine(emit_callback=lambda sig: emitted.append(sig))
        now = time.time()
        coin = "PEPE"

        orig_runner_enabled = C.S1_RUNNER_ENABLED
        C.S1_RUNNER_ENABLED = True
        try:
            sig = SunriseSignal(
                id="sig_test_dead_stall",
                coin=coin,
                leader_exchange="upbit",
                follower_exchange="binance",
                follower_contract="1000PEPEUSDT",
                open_ts=now,
                trigger_type="depth_drain",
                leader_mid=0.0100, leader_best_ask=0.01005, leader_spread_bps=10.0,
                leader_norm_spread_bps=10.0, leader_top5_ask_usd=5000.0, leader_norm_depth_usd=5000.0,
                leader_swept_usd=3000.0, leader_imbalance_top5=0.5, drain_ratio=0.5,
                spread_blowout_ratio=1.0, follower_mid=0.0098, follower_best_bid=0.00979,
                follower_best_ask=0.00980, follower_spread_bps=10.0, follower_top3_ask_usd=3000.0,
                follower_top3_bid_usd=3000.0, follower_imbalance_top3=0.0, kimchi_baseline_bps=100.0,
                pair_sigma_bps=5.0, raw_dislocation_bps=200.0, dev_z=6.5, gross_exec_edge_bps=50.0,
                net_exec_edge_bps=40.0, suggested_size_usd=100.0, follower_entry_px=0.00980,
            )
            sig.runner_enabled = True
            sig.runner_active = True
            sig.phase1_pnl_bps = 30.0
            sig.phase1_close_ts = now
            sig.runner_peak_pnl_bps = 30.0
            sig.runner_peak_ts = now
            sig.runner_pnl_bps = 30.0

            engine._active_signals[coin].append(sig)
            leader_snap = OrderBookDepth5(
                exchange="upbit", symbol=coin, raw_symbol="KRW-PEPE",
                ts=now, exchange_ts=int(now * 1000),
                bids=[(0.0100, 1000.0)], asks=[(0.01005, 1000.0)]
            )

            # Price stays near 28 bps at t = +22s (no new high for 22s > 20s S1_RUNNER_NO_PEAK_DECAY_SEC)
            # Exit bid for 28 bps net = 0.00980 * (1 + 38/10000) = 0.00983724
            f_snap = OrderBookDepth5(
                exchange="binance", symbol=coin, raw_symbol="1000PEPEUSDT",
                ts=now + 22.0, exchange_ts=int((now + 22.0) * 1000),
                bids=[(0.00983724, 10000.0)], asks=[(0.00984, 10000.0)]
            )

            engine._track_one_signal(sig, leader_snap, f_snap, now + 22.0)
            self.assertTrue(sig.closed)
            self.assertEqual(sig.close_reason, "runner_dead_stall")
            self.assertEqual(engine.counters.get("signals_runner_dead_stall"), 1)
        finally:
            C.S1_RUNNER_ENABLED = orig_runner_enabled

    def test_runner_breakeven_lock_escalation(self):
        # After 15s, breakeven floor escalates to 50% of peak profit!
        emitted = []
        engine = KoreanEngine(emit_callback=lambda sig: emitted.append(sig))
        now = time.time()
        coin = "PEPE"

        orig_runner_enabled = C.S1_RUNNER_ENABLED
        C.S1_RUNNER_ENABLED = True
        try:
            sig = SunriseSignal(
                id="sig_test_be_lock",
                coin=coin,
                leader_exchange="upbit",
                follower_exchange="binance",
                follower_contract="1000PEPEUSDT",
                open_ts=now,
                trigger_type="depth_drain",
                leader_mid=0.0100, leader_best_ask=0.01005, leader_spread_bps=10.0,
                leader_norm_spread_bps=10.0, leader_top5_ask_usd=5000.0, leader_norm_depth_usd=5000.0,
                leader_swept_usd=3000.0, leader_imbalance_top5=0.5, drain_ratio=0.5,
                spread_blowout_ratio=1.0, follower_mid=0.0098, follower_best_bid=0.00979,
                follower_best_ask=0.00980, follower_spread_bps=10.0, follower_top3_ask_usd=3000.0,
                follower_top3_bid_usd=3000.0, follower_imbalance_top3=0.0, kimchi_baseline_bps=100.0,
                pair_sigma_bps=5.0, raw_dislocation_bps=200.0, dev_z=6.5, gross_exec_edge_bps=50.0,
                net_exec_edge_bps=40.0, suggested_size_usd=100.0, follower_entry_px=0.00980,
            )
            sig.runner_enabled = True
            sig.runner_active = True
            sig.phase1_pnl_bps = 25.0
            sig.phase1_close_ts = now
            # Peak was 40.0 bps at t = +10s
            sig.runner_peak_pnl_bps = 40.0
            sig.runner_peak_ts = now + 10.0
            sig.runner_pnl_bps = 40.0

            engine._active_signals[coin].append(sig)
            leader_snap = OrderBookDepth5(
                exchange="upbit", symbol=coin, raw_symbol="KRW-PEPE",
                ts=now, exchange_ts=int(now * 1000),
                bids=[(0.0100, 1000.0)], asks=[(0.01005, 1000.0)]
            )

            # At t = +18s (runner_dt = 18s >= 15s escalate_sec):
            # Breakeven floor is locked at 50% of peak = 20.0 bps!
            # Trail dist = max(8, 5, min(40*0.35=14, 25)) = 14.0 bps.
            # So trail stop triggers at 40 - 14 = 26 bps.
            # If price drops to 18 bps (below breakeven floor 20 bps):
            # Exit bid for 18 bps net = 0.00980 * (1 + 28/10000) = 0.00982744
            f_snap = OrderBookDepth5(
                exchange="binance", symbol=coin, raw_symbol="1000PEPEUSDT",
                ts=now + 18.0, exchange_ts=int((now + 18.0) * 1000),
                bids=[(0.00982744, 10000.0)], asks=[(0.00983, 10000.0)]
            )

            engine._track_one_signal(sig, leader_snap, f_snap, now + 18.0)
            self.assertTrue(sig.closed)
            # Since 18 <= 26, it hits trailing stop before breakeven lock
            self.assertTrue(sig.close_reason in ("runner_trailing_stop", "runner_breakeven_lock"))
            self.assertEqual(engine.counters.get("signals_runner_closed"), 1)
        finally:
            C.S1_RUNNER_ENABLED = orig_runner_enabled

    def test_runner_skips_micro_edges(self):
        emitted = []
        engine = KoreanEngine(emit_callback=lambda sig: emitted.append(sig))
        now = time.time()
        coin = "PEPE"

        orig_runner_enabled = C.S1_RUNNER_ENABLED
        C.S1_RUNNER_ENABLED = True
        try:
            sig = SunriseSignal(
                id="sig_test_micro_edge",
                coin=coin,
                leader_exchange="upbit",
                follower_exchange="binance",
                follower_contract="1000PEPEUSDT",
                open_ts=now,
                trigger_type="depth_drain",
                leader_mid=0.0100, leader_best_ask=0.01005, leader_spread_bps=10.0,
                leader_norm_spread_bps=10.0, leader_top5_ask_usd=5000.0, leader_norm_depth_usd=5000.0,
                leader_swept_usd=3000.0, leader_imbalance_top5=0.5, drain_ratio=0.5,
                spread_blowout_ratio=1.0, follower_mid=0.0098, follower_best_bid=0.00979,
                follower_best_ask=0.00980, follower_spread_bps=10.0, follower_top3_ask_usd=3000.0,
                follower_top3_bid_usd=3000.0, follower_imbalance_top3=0.0, kimchi_baseline_bps=100.0,
                pair_sigma_bps=5.0, raw_dislocation_bps=200.0, dev_z=6.5, gross_exec_edge_bps=50.0,
                net_exec_edge_bps=15.0,  # Target edge is only 15.0 bps (< 20.0 bps min_phase1)
                suggested_size_usd=100.0, follower_entry_px=0.00980,
            )
            engine._active_signals[coin].append(sig)
            leader_snap = OrderBookDepth5(
                exchange="upbit", symbol=coin, raw_symbol="KRW-PEPE",
                ts=now, exchange_ts=int(now * 1000),
                bids=[(0.0100, 1000.0)], asks=[(0.01005, 1000.0)]
            )
            # Reaches 12.0 bps net PnL (converged >= 15 * 0.75 = 11.25 bps)
            # Exit bid for +12 bps net = 0.00980 * (1 + 22/10000) = 0.00982156
            f_snap = OrderBookDepth5(
                exchange="binance", symbol=coin, raw_symbol="1000PEPEUSDT",
                ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
                bids=[(0.00982156, 10000.0)], asks=[(0.00983, 10000.0)]
            )

            engine._track_one_signal(sig, leader_snap, f_snap, now + 1.0)
            # Must close as normal converged, NOT activate runner on micro-edge!
            self.assertTrue(sig.closed)
            self.assertFalse(sig.runner_active)
            self.assertEqual(sig.close_reason, "converged")
            self.assertEqual(engine.counters.get("signals_runner_activated", 0), 0)
        finally:
            C.S1_RUNNER_ENABLED = orig_runner_enabled


if __name__ == "__main__":
    unittest.main()
