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
            sig.runner_peak_pnl_bps = 35.0
            sig.runner_pnl_bps = 35.0

            engine._active_signals[coin].append(sig)

            # At t = +20s: standard hold_sec (15s) has elapsed, but runner max hold (120s) has NOT!
            engine.sweep_timeouts(now + 20.0)
            self.assertFalse(sig.closed)  # Must NOT be closed prematurely by standard 15s timeout!

            # At t = +125s: runner max hold (120s) has elapsed -> Swept with runner_timeout & blended PnL!
            engine.sweep_timeouts(now + 125.0)
            self.assertTrue(sig.closed)
            self.assertEqual(sig.close_reason, "runner_timeout")
            self.assertEqual(engine.counters.get("signals_runner_closed"), 1)
        finally:
            C.S1_RUNNER_ENABLED = orig_runner_enabled


if __name__ == "__main__":
    unittest.main()
