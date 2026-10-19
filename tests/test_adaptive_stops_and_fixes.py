"""
tests/test_adaptive_stops_and_fixes.py - Verification suite for Korean Sunrise 7-fix overhaul:
1. Book age tolerances (5.0s entry, 15.0s baseline learning).
2. Holding horizons (30.0s engine default, 60.0s calibrator ceiling).
3. Multi-second Upbit sweep trade confirmation (6.0s window).
4. Triple Stop-Loss Models (Model A Invalidation, Model B Adverse PnL, Model C Alpha Decay).
5. Elimination of redundant spread penalty.
6. Upbit spot silence tolerance (15.0s).
7. Evaluator noise filter relaxation (0.30).
8. ReconnectPolicy and defensive order book sorting.
"""
from __future__ import annotations

import unittest
import time
from typing import Dict, Any

import config as C
from models import OrderBookDepth5, SunriseSignal, normalize_exchange_ts
from korean_engine import KoreanEngine, PairState
from venues.base import BaseVenueStream, ReconnectPolicy, TTLCache
from tools.calibrate_thresholds import _calc_optimal_hold
from pipeline.evaluator import CoinSignalAccumulator, DEFAULT_MIN_EVENTS


class TestAdaptiveStopsAndFixes(unittest.TestCase):

    def setUp(self):
        self.emitted: list = []
        self.engine = KoreanEngine(
            emit_callback=lambda s: self.emitted.append(s),
            warmup_samples=5,
            max_follower_book_age_sec=5.0,
            max_hold_sec=30.0,
            stop_invalidation_bps=15.0,
            stop_adverse_pnl_bps=35.0,
            alpha_decay_sec=8.0,
        )

    # -------------------------------------------------------------
    # Test 1: Normalize exchange ts across ns, us, ms, s
    # -------------------------------------------------------------
    def test_normalize_exchange_ts(self):
        now_ms = int(time.time() * 1000)
        # ms
        self.assertEqual(normalize_exchange_ts(now_ms), now_ms)
        # seconds -> ms
        now_s = now_ms // 1000
        self.assertEqual(normalize_exchange_ts(now_s), now_s * 1000)
        # ns -> ms
        now_ns = now_ms * 1_000_000
        self.assertEqual(normalize_exchange_ts(now_ns), now_ms)
        # us -> ms
        now_us = now_ms * 1_000
        self.assertEqual(normalize_exchange_ts(now_us), now_ms)
        # string representation
        self.assertEqual(normalize_exchange_ts(str(now_ms)), now_ms)

    # -------------------------------------------------------------
    # Test 2: ReconnectPolicy and TTLCache
    # -------------------------------------------------------------
    def test_reconnect_policy_and_ttl_cache(self):
        policy = ReconnectPolicy(base_delay=2.0, max_delay=60.0, backoff_factor=2.0)
        # Standard error
        d1 = policy.compute_delay(Exception("Connection reset"))
        self.assertEqual(d1, 2.0)
        d2 = policy.compute_delay(Exception("Connection reset"))
        self.assertEqual(d2, 4.0)

        # 429 Too Many Requests -> aggressive backoff
        d_429 = policy.compute_delay(Exception("HTTP 429 Too Many Requests"))
        self.assertGreaterEqual(d_429, 30.0)

        # Reset
        policy.reset()
        self.assertEqual(policy.compute_delay(), 2.0)

        # TTLCache
        cache = TTLCache(ttl_sec=1.0)
        cache.set("key1")
        self.assertTrue(cache.get("key1"))
        self.assertFalse(cache.get("key2"))

    # -------------------------------------------------------------
    # Test 3: Defensive sorting in BaseVenueStream._sanitise
    # -------------------------------------------------------------
    def test_defensive_orderbook_sanitise(self):
        received = []
        stream = BaseVenueStream("test_venue", ["BTC"], callback=lambda snap: received.append(snap))
        # Unsorted bids and asks
        snap = OrderBookDepth5(
            exchange="test_venue",
            symbol="BTC",
            raw_symbol="BTCUSDT",
            ts=time.time(),
            exchange_ts=int(time.time() * 1000),
            bids=[(100.0, 1.0), (102.0, 2.0), (101.0, 1.5)],  # Best bid is 102.0
            asks=[(105.0, 1.0), (103.0, 2.0), (104.0, 1.5)],  # Best ask is 103.0
        )
        stream._sanitise(snap)
        self.assertEqual(len(received), 1)
        clean = received[0]
        # Bids must be sorted descending: 102.0, 101.0, 100.0
        self.assertEqual(clean.bids[0][0], 102.0)
        # Asks must be sorted ascending: 103.0, 104.0, 105.0
        self.assertEqual(clean.asks[0][0], 103.0)

    # -------------------------------------------------------------
    # Test 4: Follower baseline learning decoupled from 5.0s entry age
    # -------------------------------------------------------------
    def test_follower_book_age_tolerance_and_learning(self):
        now = time.time()
        coin = "ALT"
        # Upbit book 1.0s ago
        u_snap = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 1.0, exchange_ts=int((now - 1.0) * 1000),
            bids=[(10.0, 100.0)], asks=[(10.1, 100.0)]
        )
        self.engine._books[("upbit", coin)] = u_snap
        self.engine._recv_ts[("upbit", coin)] = now - 1.0

        # Follower book arrived 3.5s ago (older than old 0.3s limit, but <= 5.0s)
        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol=f"{coin}USDT",
            ts=now - 3.5, exchange_ts=int((now - 3.5) * 1000),
            bids=[(9.995, 100.0)], asks=[(10.005, 100.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now - 3.5

        # Normalization and baseline state
        self.engine._upbit_norm[coin] = {
            "depth_ask": 500.0,
            "best_ask_usd": 200.0,
            "spread_bps": 10.0,
            "t": now - 1.0,
        }
        st = self.engine._get_or_create_pair_state(coin, "binance")
        # Warm it up
        for i in range(6):
            st.learn(10.0, (now - 20.0) + (i * 2.0))
        self.assertTrue(st.is_warmed_up)

        # Sweep on Upbit triggers lead-lag evaluation
        u_sweep = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(10.1, 100.0)], asks=[(10.5, 100.0)]
        )
        self.engine.on_book_update(u_sweep)

        # With 5.0s tolerance, the 3.5s follower book is accepted and signal fires!
        self.assertEqual(len(self.engine._active_signals[coin]), 1)
        sig = self.engine._active_signals[coin][0]
        self.assertEqual(sig.coin, coin)

    # -------------------------------------------------------------
    # Test 5: Model A Microstructure Invalidation Stop-Loss
    # -------------------------------------------------------------
    def test_model_a_invalidation_stop(self):
        now = 1000.0
        coin = "DOGE"
        st = self.engine._get_or_create_pair_state(coin, "binance")
        st.baseline_bps = 0.0

        sig = SunriseSignal(
            id="test_sig_inv",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="DOGEUSDT",
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
            pair_sigma_bps=5.0,
            raw_dislocation_bps=100.0,
            dev_z=3.0,
            gross_exec_edge_bps=90.0,
            net_exec_edge_bps=80.0,
            suggested_size_usd=100.0,
            max_hold_sec=30.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Upbit dumps below entry (drop > 1.5 * sigma = 7.5 bps -> e.g. 15 bps drop)
        u_dump = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[(99.80, 10.0)], asks=[(99.85, 10.0)]  # Mid = 99.825 (-17.5 bps from 100.0)
        )
        f_book = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol=f"{coin}USDT",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[(99.00, 10.0)], asks=[(99.05, 10.0)]
        )
        self.engine._books[("upbit", coin)] = u_dump
        self.engine._books[("binance", coin)] = f_book

        self.engine._track_follower_move(coin, "binance", f_book, now + 1.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "stop_invalidation")
        self.assertTrue(sig.stop_hit)
        self.assertEqual(sig.stop_type, "invalidation")
        self.assertGreater(self.engine.counters["signals_stop_loss_invalidation"], 0)

    # -------------------------------------------------------------
    # Test 6: Model B Hard Adverse PnL Drawdown Stop-Loss
    # -------------------------------------------------------------
    def test_model_b_adverse_pnl_stop(self):
        now = 1000.0
        coin = "PEPE"
        st = self.engine._get_or_create_pair_state(coin, "bybit")
        st.baseline_bps = 0.0

        sig = SunriseSignal(
            id="test_sig_adv",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="bybit",
            follower_contract="1000PEPEUSDT",
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
            follower_mid=100.0,
            follower_best_bid=99.95,
            follower_best_ask=100.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=50.0,
            dev_z=3.0,
            gross_exec_edge_bps=40.0,
            net_exec_edge_bps=30.0,
            suggested_size_usd=100.0,
            max_hold_sec=30.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        u_held = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.00, 10.0)], asks=[(100.05, 10.0)]
        )
        # Follower crashes violently: best bid drops to 99.50 (-55 bps exec move -> net PnL = -65 bps)
        f_crash = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="1000PEPEUSDT",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(99.50, 10.0)], asks=[(99.55, 10.0)]
        )
        self.engine._books[("upbit", coin)] = u_held
        self.engine._books[("bybit", coin)] = f_crash

        self.engine._track_follower_move(coin, "bybit", f_crash, now + 2.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "stop_adverse_pnl")
        self.assertTrue(sig.stop_hit)
        self.assertEqual(sig.stop_type, "adverse_pnl")
        self.assertGreater(self.engine.counters["signals_stop_loss_adverse_pnl"], 0)

    # -------------------------------------------------------------
    # Test 7: Model C Alpha Decay Timeout Stop
    # -------------------------------------------------------------
    def test_model_c_alpha_decay_stop(self):
        now = 1000.0
        coin = "SHIB"
        st = self.engine._get_or_create_pair_state(coin, "binance")
        st.baseline_bps = 0.0

        sig = SunriseSignal(
            id="test_sig_decay",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="1000SHIBUSDT",
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
            follower_mid=100.0,
            follower_best_bid=99.95,
            follower_best_ask=100.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=2.0,
            raw_dislocation_bps=50.0,
            dev_z=3.0,
            gross_exec_edge_bps=40.0,
            net_exec_edge_bps=30.0,
            suggested_size_usd=100.0,
            max_hold_sec=30.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Leader stays, follower is flat, but 8.5 seconds have passed (dt >= alpha_decay_sec=8.0)
        u_stay = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 8.5, exchange_ts=int((now + 8.5) * 1000),
            bids=[(100.00, 10.0)], asks=[(100.05, 10.0)]
        )
        f_flat = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="1000SHIBUSDT",
            ts=now + 8.5, exchange_ts=int((now + 8.5) * 1000),
            bids=[(100.02, 10.0)], asks=[(100.07, 10.0)]  # Net PnL is still -7 bps due to fees
        )
        self.engine._books[("upbit", coin)] = u_stay
        self.engine._books[("binance", coin)] = f_flat

        self.engine._track_follower_move(coin, "binance", f_flat, now + 8.5)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "stop_decay")
        self.assertTrue(sig.stop_hit)
        self.assertEqual(sig.stop_type, "alpha_decay")
        self.assertGreater(self.engine.counters["signals_stop_loss_decay"], 0)

    # -------------------------------------------------------------
    # Test 8: Holding horizon clamping in calibrator
    # -------------------------------------------------------------
    def test_optimal_hold_calibrator(self):
        # Peak at 25 seconds on a slow altcoin wave
        t_peaks = [20.0, 24.0, 26.0, 25.0, 30.0]
        optimal = _calc_optimal_hold(t_peaks)
        self.assertGreater(optimal, 20.0)
        self.assertLessEqual(optimal, 60.0)

    # -------------------------------------------------------------
    # Test 9: Evaluator noise filter relaxation to 0.30
    # -------------------------------------------------------------
    def test_evaluator_noise_filter(self):
        acc = CoinSignalAccumulator("TEST")
        # Signal with drain_ratio 0.35 and cross_ratio 0.35 (would have been dropped by 0.40/0.50)
        acc.add_signal({
            "follower_exchange": "binance",
            "net_pnl_bps": 25.0,
            "drain_ratio": 0.35,
            "leader_swept_usd": 700.0,
            "follower_top3_ask_usd": 2000.0,  # cross_ratio = 700/2000 = 0.35
            "stealth_capacity_usd": 300.0,
            "mfe_exec_pnl_bps": 30.0,
            "time_to_peak_sec": 12.0,
        })
        self.assertEqual(acc.valid_sweeps, 1)
        self.assertEqual(len(acc.net_pnls), 1)
        self.assertEqual(DEFAULT_MIN_EVENTS, 20)

    # -------------------------------------------------------------
    # Test 10: Daily reconnect jitter never produces 1s thrashing
    # -------------------------------------------------------------
    def test_daily_reconnect_jitter_no_thrash(self):
        from venues.base import _secs_until_daily_reconnect_for
        # Test for various venue names, result must always be >= 60.0
        for name in ("binance_0", "bybit", "gateio", "okx", "upbit", "hyperliquid"):
            secs = _secs_until_daily_reconnect_for(name)
            self.assertGreaterEqual(secs, 60.0, f"Jittered reconnect too small for {name}: {secs}")
            self.assertLessEqual(secs, 86400.0 + 500.0)

    # -------------------------------------------------------------
    # Test 11: Inactive venue rejected by KoreanEngine._check_entry
    # -------------------------------------------------------------
    def test_inactive_venue_rejected_by_engine(self):
        from unittest.mock import MagicMock
        mock_profile = MagicMock()
        mock_profile.is_venue_active.return_value = False  # Venue is disabled!
        self.engine.profile_manager = mock_profile

        now = time.time()
        coin = "SOL"
        st = self.engine._get_or_create_pair_state(coin, "bybit")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 100.0,
            "spread_bps": 2.0,
            "t": now - 1.0,
        }
        f_snap = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="SOLUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 10.0)],
            asks=[(100.00, 10.0), (100.05, 10.0), (100.10, 10.0)],
        )
        self.engine._books[("bybit", coin)] = f_snap
        self.engine._recv_ts[("bybit", coin)] = now

        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 5.0)],
            asks=[(100.00, 10.0), (100.05, 5.0), (100.50, 20.0)],
        )
        self.engine._books[("upbit", coin)] = u_prev

        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(100.00, 5.0)],
            asks=[(100.50, 20.0)],
        )
        self.engine.on_book_update(u_curr)

        # Signal must NOT be emitted because bybit is inactive in profile
        self.assertIsNone(st.open_signal)

    # -------------------------------------------------------------
    # Test 12: BatchWriter with deterministic now timestamp
    # -------------------------------------------------------------
    def test_batch_writer_deterministic_now(self):
        from batch_writer import BatchWriter
        import tempfile
        import shutil
        import os

        tmp_dir = tempfile.mkdtemp()
        try:
            bw = BatchWriter(data_root=tmp_dir, flush_interval_sec=1.0)
            bw.add({"sig": "test_now"})
            # Flush using a specific fixed epoch timestamp: 2026-05-15 12:00:00 UTC = 1778846400
            test_ts = 1778846400.0
            bw.flush(now=test_ts)
            expected_day_dir = os.path.join(tmp_dir, "signals", "2026-05-15")
            self.assertTrue(os.path.isdir(expected_day_dir))
            files = os.listdir(expected_day_dir)
            self.assertEqual(len(files), 1)
            self.assertTrue(files[0].startswith("sunrise_signals_20260515_120000_"))
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # -------------------------------------------------------------
    # Test 13: PoolStateManager restores stealth_capacity_usd
    # -------------------------------------------------------------
    def test_pool_state_restores_stealth_capacity(self):
        from pipeline.pool_state import PoolStateManager
        import tempfile
        import shutil
        import json
        import os

        tmp_dir = tempfile.mkdtemp()
        try:
            u_path = os.path.join(tmp_dir, "universe_live.json")
            b_path = os.path.join(tmp_dir, "battle_pool.json")
            with open(u_path, "w", encoding="utf-8") as f:
                json.dump({"pairs": {"PEPE": {"venues": ["binance"]}}}, f)
            with open(b_path, "w", encoding="utf-8") as f:
                json.dump({
                    "coins": {
                        "PEPE": {
                            "venues": ["binance"],
                            "top3_depth_usd": 10000.0,
                            "stealth_capacity_usd": 777.5,
                            "mean_net_pnl_bps": 25.0,
                        }
                    }
                }, f)
            mgr = PoolStateManager(universe_file=u_path, battle_pool_file=b_path, overrides_file=os.path.join(tmp_dir, "ov.json"))
            mgr.reload()
            entry = mgr.get_coin_entry("PEPE")
            self.assertIsNotNone(entry)
            self.assertEqual(entry.stealth_capacity_usd, 777.5)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
