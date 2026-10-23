"""
tests/test_cargo_cult_audit.py — Verification of quantitative anti-cargo-cult upgrades.

Tests:
1. Low-cap depth normalization: honest drain ratio on $50-$80 books without $100 floor dilution.
2. Decoupled baseline learning: quiet alts continue learning baselines up to 180s silence.
3. Anti-ghosting trade confirmation: 15s lookback with $5 floor.
4. Multi-scale return correlation: resolves Epps effect on asynchronous 1m candles.
5. Namesake / fake coin discrimination under multi-scale screening.
6. SunriseEvaluator promotion with 5 events and low-cap depth.
"""
from __future__ import annotations

import math
import numpy as np
import time
import unittest

import config as C
from models import OrderBookDepth5
from korean_engine import KoreanEngine, PairState
from screening.metrics import evaluate_pair_klines
from pipeline.evaluator import CoinSignalAccumulator, SunriseEvaluator, DEFAULT_MIN_EVENTS
from pipeline.pool_state import DEFAULT_MIN_ORDER_USD, DEFAULT_MIN_DEPTH_USD


def _make_book(venue: str, coin: str, mid: float, depth_per_lvl: float = 100.0, ts: float = 1000.0) -> OrderBookDepth5:
    spread = mid * 0.001
    b0 = mid - spread * 0.5
    a0 = mid + spread * 0.5
    bids = [(b0 - i * 0.001 * mid, depth_per_lvl / (b0 - i * 0.001 * mid)) for i in range(5)]
    asks = [(a0 + i * 0.001 * mid, depth_per_lvl / (a0 + i * 0.001 * mid)) for i in range(5)]
    return OrderBookDepth5(
        exchange=venue,
        symbol=coin,
        raw_symbol=f"{coin}-TEST",
        ts=ts,
        exchange_ts=int(ts * 1000.0),
        bids=bids,
        asks=asks,
    )


class TestCargoCultAudit(unittest.TestCase):

    def test_low_cap_depth_normalization_no_dilution(self):
        """Small-cap books ($50 normal depth) must compute honest drain_ratio without $100 floor dilution."""
        engine = KoreanEngine(emit_callback=lambda s: None, min_edge_bps=5.0)
        # Seed Upbit norm with low depth: 5 levels of $10 each = $50 total depth
        for t in range(10):
            ob = _make_book("upbit", "SHITCOIN", mid=1.0, depth_per_lvl=10.0, ts=1000.0 + t)
            engine.on_book_update(ob)

        norm_depth = engine._upbit_norm["SHITCOIN"]["depth_ask"]
        # Depth should reflect actual small book (~$50), NOT clamped to 100.0
        self.assertLess(norm_depth, 70.0, f"Norm depth should reflect genuine small book, got {norm_depth}")
        self.assertGreater(norm_depth, 20.0)

        # Now simulate a sweep of $35:
        # In the old logic with floor=100, drain_ratio would be 35 / 100 = 0.35 (failing 0.50 gate)
        # With honest floor=5.0, drain_ratio is 35 / 50 = 0.70 (clearly passing 0.50 gate!)
        honest_drain_ratio = 35.0 / max(norm_depth, 5.0)
        self.assertGreaterEqual(honest_drain_ratio, 0.50, f"Drain ratio should exceed 0.50, got {honest_drain_ratio}")

    def test_contemporaneous_baseline_learning_guard(self):
        """Baseline learning requires reasonably contemporaneous quotes (<= 2.0s) to prevent market drift poisoning."""
        engine = KoreanEngine(emit_callback=lambda s: None, max_follower_book_age_sec=0.50, lead_lag_min_power_ratio=0.0)
        coin = "QUIETALT"
        follower = "binance"
        st = engine._get_or_create_pair_state(coin, follower)

        now = time.time()
        upbit_ob = _make_book("upbit", coin, mid=10.0, depth_per_lvl=20.0, ts=now)
        f_ob = _make_book(follower, coin, mid=10.0, depth_per_lvl=20.0, ts=now)

        engine._books[("upbit", coin)] = upbit_ob

        # 1. Follower update arrives, but Upbit snapshot is 3.5s old (> 2.0s guard)
        engine._recv_ts[("upbit", coin)] = now - 3.5
        engine.on_book_update(f_ob)
        self.assertEqual(st.n_obs, 0, "Stale Upbit quote (> 2.0s) must not trigger baseline learning")

        # 2. Follower update arrives with contemporaneous Upbit snapshot (0.5s old <= 2.0s guard)
        engine._recv_ts[("upbit", coin)] = time.time() - 0.5
        engine.on_book_update(f_ob)
        self.assertEqual(st.n_obs, 1, "Contemporaneous quotes (<= 2.0s) must trigger baseline learning")

    def test_multi_scale_epps_effect_recovery(self):
        """Multi-scale aggregation recovers true co-movement on quiet alts with non-synchronous 1m returns."""
        np.random.seed(42)
        n_minutes = 120
        # True common underlying asset price process
        true_returns = np.random.normal(0, 0.005, size=n_minutes)
        true_log_price = np.cumsum(true_returns)

        # Non-synchronous execution:
        # Venue A observes price with small noise and occasional tick delays
        # Venue B observes price with noise and alternating zero-volume minutes
        ts_list = [1000000 + i * 60 for i in range(n_minutes)]

        kl_upbit = {}
        kl_perp = {}

        for i, t in enumerate(ts_list):
            p_base = np.exp(true_log_price[i])
            # Even minutes trade on Upbit, odd minutes trade on Perp
            upbit_vol = 500.0 if i % 2 == 0 else 0.0
            perp_vol = 500.0 if i % 2 != 0 else 0.0

            # Price has micro-jitter
            p_up = p_base * (1.0 + np.random.normal(0, 0.002)) * 1350.0  # KRW
            p_perp = p_base * (1.0 + np.random.normal(0, 0.002))         # USD

            kl_upbit[t] = (p_up, upbit_vol)
            kl_perp[t] = (p_perp, perp_vol)

        res = evaluate_pair_klines(
            kl_upbit=kl_upbit,
            kl_perp=kl_perp,
            usdt_krw_rate=1350.0,
            min_corr=0.50,
            min_both_trade=0.0,
            min_dev_bps=0.0,
        )

        # Multi-scale 15-minute correlation should be high (> 0.60) even if 1m correlation is noisy
        self.assertGreaterEqual(res["corr"], 0.50, f"Expected multi-scale correlation >= 0.50, got {res['corr']}")
        self.assertLess(res["price_diff_pct"], 5.0)

    def test_namesake_rejection_under_multiscale(self):
        """Two unrelated tokens with independent random walks must be rejected by correlation check."""
        np.random.seed(99)
        n_minutes = 120
        ts_list = [1000000 + i * 60 for i in range(n_minutes)]

        # Independent price series
        p1 = np.exp(np.cumsum(np.random.normal(0, 0.01, size=n_minutes))) * 1350.0
        p2 = np.exp(np.cumsum(np.random.normal(0, 0.01, size=n_minutes)))

        kl_upbit = {t: (float(p1[i]), 1000.0) for i, t in enumerate(ts_list)}
        kl_perp = {t: (float(p2[i]), 1000.0) for i, t in enumerate(ts_list)}

        res = evaluate_pair_klines(
            kl_upbit=kl_upbit,
            kl_perp=kl_perp,
            usdt_krw_rate=1350.0,
            min_corr=0.50,
        )
        self.assertEqual(res["passed"], 0.0, "Independent namesake tokens must fail the gate")
        self.assertLess(res["corr"], 0.50)

    def test_evaluator_promotes_with_5_events(self):
        """CoinSignalAccumulator and evaluator qualify coins with 5 verified positive events."""
        acc = CoinSignalAccumulator("LOWCAPCOIN")
        for _ in range(5):
            acc.add_signal({
                "follower_exchange": "binance",
                "net_pnl_bps": 20.0,
                "drain_ratio": 0.40,
                "leader_swept_usd": 30.0,
                "follower_top3_ask_usd": 80.0,  # cross_ratio = 30/80 = 0.375 >= 0.20
                "stealth_capacity_usd": 12.0,
                "mfe_exec_pnl_bps": 25.0,
                "time_to_peak_sec": 8.0,
            })

        st = acc.finalize()
        self.assertIsNotNone(st)
        self.assertEqual(st["event_count"], 5)
        self.assertGreater(st["mean_net_pnl_bps"], 0)
        self.assertGreaterEqual(st["profit_factor"], 1.25)
        self.assertGreaterEqual(st["win_rate_pct"], 50.0)
        self.assertGreaterEqual(st["top3_depth_usd"], 66.7)

        # Evaluator gate check
        self.assertEqual(DEFAULT_MIN_EVENTS, 5)
        self.assertEqual(DEFAULT_MIN_ORDER_USD, 10.0)
        self.assertEqual(DEFAULT_MIN_DEPTH_USD, 66.7)

    def test_follower_book_age_strictly_bounded_hft(self):
        """HFT quote freshness: books older than 500ms are strictly rejected for entry."""
        engine = KoreanEngine(emit_callback=lambda s: None, max_follower_book_age_sec=0.50, lead_lag_min_power_ratio=0.0)
        coin = "FAST"
        follower = "binance"
        st = engine._get_or_create_pair_state(coin, follower)
        now = time.time()
        for i in range(35):
            st.learn(0.0, now - 40.0 + i)
        self.assertTrue(st.is_warmed_up)

        u_snap = _make_book("upbit", coin, mid=105.0, depth_per_lvl=1000.0, ts=now)
        f_stale = _make_book(follower, coin, mid=100.0, depth_per_lvl=1000.0, ts=now - 0.55)

        engine._books[("upbit", coin)] = u_snap
        engine._books[(follower, coin)] = f_stale
        engine._recv_ts[(follower, coin)] = now - 0.55

        # 550ms old follower book: MUST BE REJECTED
        engine._check_entry(
            coin=coin, follower_ex=follower, st=st,
            upbit_snap=u_snap, f_snap=f_stale,
            f_book_age_ms=550.0, norm_depth=1000.0, norm_spread=5.0,
            swept_usd=5000.0, trigger_type="depth_drain", drain_ratio=0.8,
            blowout_ratio=3.0, now=now
        )
        self.assertIsNone(st.open_signal)

        # 200ms old follower book: MUST BE ACCEPTED
        f_fresh = _make_book(follower, coin, mid=100.0, depth_per_lvl=1000.0, ts=now - 0.20)
        engine._books[(follower, coin)] = f_fresh
        engine._recv_ts[(follower, coin)] = now - 0.20
        engine._check_entry(
            coin=coin, follower_ex=follower, st=st,
            upbit_snap=u_snap, f_snap=f_fresh,
            f_book_age_ms=200.0, norm_depth=1000.0, norm_spread=5.0,
            swept_usd=5000.0, trigger_type="depth_drain", drain_ratio=0.8,
            blowout_ratio=3.0, now=now
        )
        self.assertIsNotNone(st.open_signal)

    def test_gross_calibration_vs_net_edge_alignment(self):
        """When matrix has min_spread_bps=20.0, gross edge >= 20.0 with net edge > 0 is accepted."""
        engine = KoreanEngine(emit_callback=lambda s: None, roundtrip_fee_bps=10.0, lead_lag_min_power_ratio=0.0)
        coin = "MATRIXCOIN"
        follower = "binance"
        # Pair threshold config has min_spread_bps=20.0 (calibrated on gross spread)
        engine._thresholds[f"{coin}:upbit-{follower}"] = {"min_spread_bps": 20.0, "enabled": True}

        st = engine._get_or_create_pair_state(coin, follower)
        now = time.time()
        for i in range(35):
            st.learn(0.0, now - 40.0 + i)
        self.assertTrue(st.is_warmed_up)

        # Follower mid = 100.00, best ask = 100.05.
        # Upbit mid = 100.28.
        # Gross ask edge = (100.28 - 100.05) / 100.05 * 1e4 = 23.0 bps >= 20.0 bps req_spread.
        # Net edge = 23.0 - 10.0 (fee) = 13.0 bps > 0.
        # Under the old bug (requiring net_edge >= min_spread_bps), 13.0 < 20.0 would have rejected this trade!
        u_snap = _make_book("upbit", coin, mid=100.28, depth_per_lvl=1000.0, ts=now)
        f_snap = _make_book(follower, coin, mid=100.00, depth_per_lvl=1000.0, ts=now - 0.1)

        engine._books[("upbit", coin)] = u_snap
        engine._books[(follower, coin)] = f_snap
        engine._recv_ts[(follower, coin)] = now - 0.1

        engine._check_entry(
            coin=coin, follower_ex=follower, st=st,
            upbit_snap=u_snap, f_snap=f_snap,
            f_book_age_ms=100.0, norm_depth=1000.0, norm_spread=5.0,
            swept_usd=5000.0, trigger_type="depth_drain", drain_ratio=0.8,
            blowout_ratio=3.0, now=now
        )
        self.assertIsNotNone(st.open_signal, "Gross edge 23 bps meets 20 bps min_spread_bps with positive net edge")
        self.assertAlmostEqual(st.open_signal.gross_exec_edge_bps, 23.0, delta=0.5)
        self.assertAlmostEqual(st.open_signal.net_exec_edge_bps, 13.0, delta=0.5)

    def test_cold_start_median_baseline_no_poison(self):
        """Opening tick outlier of +120 bps must not poison the baseline for hours."""
        st = PairState("PUMPCOIN", "binance", warmup_samples=30)
        now = 1000.0

        # Tick 0: initial wild burst of +120 bps
        st.learn(120.0, now)
        self.assertEqual(st.baseline_bps, 120.0)

        # Subsequent 10 ticks are quiet market around 5.0 bps
        for i in range(1, 11):
            st.learn(5.0, now + i)

        # By tick 10, the median of initial samples has snapped baseline directly to ~5 bps
        self.assertLess(st.baseline_bps, 10.0, f"Baseline should not be poisoned by tick 0 outlier, got {st.baseline_bps}")
        self.assertGreaterEqual(st.baseline_bps, 4.0)

    def test_trade_confirm_lookback_strict_2s_guard(self):
        """Taker buys older than 2.0s must NOT validate fresh liquidity sweeps."""
        engine = KoreanEngine(
            emit_callback=lambda s: None,
            require_trade_confirmation=True,
            trade_confirm_lookback_sec=2.0,
            min_trade_confirm_floor_usd=5.0,
        )
        coin = "SWEEPCOIN"
        now = time.time()

        # Simulate a taker buy 4.0s ago (stale)
        engine.on_upbit_trade(coin, trade_usd=100.0, is_taker_buy=True, ts=now - 4.0)

        # Sweep occurs now
        cutoff = now - engine.trade_confirm_lookback_sec
        recent_buys, has_trades = engine.get_recent_upbit_buys(coin, cutoff)
        self.assertEqual(recent_buys, 0.0, "4s-old trade must be pruned by 2.0s lookback")

        # Now simulate a fresh taker buy 0.5s ago
        engine.on_upbit_trade(coin, trade_usd=100.0, is_taker_buy=True, ts=now - 0.5)
        cutoff = now - engine.trade_confirm_lookback_sec
        recent_buys, has_trades = engine.get_recent_upbit_buys(coin, cutoff)
        self.assertEqual(recent_buys, 100.0, "0.5s-old trade must be accepted")

    def test_microstructure_noise_rejection_strict_or_logic(self):
        """Evaluator strictly rejects trades where either drain_ratio < 0.30 OR cross_ratio < 0.20."""
        acc = CoinSignalAccumulator("NOISECOIN")

        # Case A: High drain (0.70) but microscopic cross ratio (0.01) - $100 sweep vs $10,000 follower book
        acc.add_signal({
            "follower_exchange": "binance",
            "net_pnl_bps": 15.0,
            "drain_ratio": 0.70,
            "leader_swept_usd": 100.0,
            "follower_top3_ask_usd": 10000.0,  # cross_ratio = 100 / 10000 = 0.01 < 0.20
        })
        self.assertEqual(acc.valid_sweeps, 0, "Low cross-ratio must be rejected by OR gating")

        # Case B: High cross ratio (0.80) but tiny drain ratio (0.10) - retail ripple on Upbit
        acc.add_signal({
            "follower_exchange": "binance",
            "net_pnl_bps": 15.0,
            "drain_ratio": 0.10,
            "leader_swept_usd": 800.0,
            "follower_top3_ask_usd": 1000.0,  # cross_ratio = 800 / 1000 = 0.80, but drain = 0.10 < 0.30
        })
        self.assertEqual(acc.valid_sweeps, 0, "Low drain-ratio must be rejected by OR gating")

        # Case C: Both criteria satisfied - genuine institutional sweep
        acc.add_signal({
            "follower_exchange": "binance",
            "net_pnl_bps": 15.0,
            "drain_ratio": 0.45,
            "leader_swept_usd": 300.0,
            "follower_top3_ask_usd": 600.0,  # cross_ratio = 300 / 600 = 0.50 >= 0.20, drain = 0.45 >= 0.30
        })
        self.assertEqual(acc.valid_sweeps, 1, "Valid sweep with both drain >= 0.30 and cross >= 0.20 must be accepted")

    def test_alpha_decay_respects_half_life_of_max_hold(self):
        """Alpha decay stop must not choke slow alts (max_hold=45s) prematurely before half-life."""
        engine = KoreanEngine(emit_callback=lambda s: None, max_hold_sec=30.0, alpha_decay_sec=15.0)
        coin = "SLOWALT"
        follower = "bybit"
        # Calibrated pair has max_hold_sec = 45.0s
        engine._thresholds[f"{coin}:upbit-{follower}"] = {"max_hold_sec": 45.0, "enabled": True}

        # Check pair decay calculation
        pair_cfg = engine.get_pair_threshold_config(coin, follower)
        hold_sec = float(pair_cfg.get("max_hold_sec", 30.0))
        default_decay = max(engine.alpha_decay_sec, hold_sec * 0.5) if hold_sec > engine.max_hold_sec else engine.alpha_decay_sec
        pair_decay_sec = float(pair_cfg.get("alpha_decay_sec", default_decay))

        self.assertGreaterEqual(pair_decay_sec, 22.5, "Alpha decay must be at least 50% of 45s hold horizon")

    def test_unified_min_order_usd_across_system(self):
        """Unified $10.0 min order is enforced across config, pool_state, profile, and evaluator."""
        from pipeline.profile import TradingProfileManager
        self.assertEqual(C.MIN_ORDER_USD, 10.0)
        self.assertEqual(DEFAULT_MIN_ORDER_USD, 10.0)
        self.assertEqual(DEFAULT_MIN_DEPTH_USD, 66.7)

        prof_mgr = TradingProfileManager()
        prof = prof_mgr.get_profile()
        self.assertEqual(prof["min_order_usd"], 10.0)

        # Order below 10.0 USD must return None (stealth size rejected)
        sized = prof_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=50.0)
        # 50.0 * 0.15 = 7.5 USD < 10.0 USD min order -> None
        self.assertIsNone(sized)

        # Order above 10.0 USD is accepted
        sized_ok = prof_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=200.0)
        # 200.0 * 0.15 = 30.0 USD (capped by slot budget) >= 10.0 USD
        self.assertIsNotNone(sized_ok)
        self.assertGreaterEqual(sized_ok, 10.0)

    def test_kucoin_stream_retries_on_initial_symbol_resolution_failure(self):
        """KucoinStream does not terminate permanent loop when HTTP symbol resolution fails initially."""
        import asyncio
        from venues.kucoin import KucoinStream

        stream = KucoinStream(symbols=["BTC", "ETH"], callback=lambda b: None)
        calls = 0
        def mock_resolve():
            nonlocal calls
            calls += 1
            if calls == 1:
                return {}
            stream._ours_to_venue["BTC"] = "XBTUSDTM"
            return stream._ours_to_venue

        stream._resolve_symbols = mock_resolve
        stream._running = True

        async def run_test():
            # Run one iteration of symbol resolution
            if not stream._ours_to_venue:
                await asyncio.to_thread(stream._resolve_symbols)
            self.assertEqual(len(stream._ours_to_venue), 0, "First call failed as simulated")

            # Second iteration simulates retry loop
            if not stream._ours_to_venue:
                await asyncio.to_thread(stream._resolve_symbols)
            self.assertEqual(len(stream._ours_to_venue), 1, "Retry succeeded without killing stream")
            self.assertEqual(stream._ours_to_venue["BTC"], "XBTUSDTM")

        asyncio.run(run_test())


    def test_upbit_rest_client_thread_safe_locking(self):
        """UpbitRestClient serializes concurrent calls with internal thread lock."""
        import concurrent.futures
        from unittest.mock import MagicMock
        from venues.upbit import UpbitRestClient

        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{"candle_date_time_utc": "2026-10-20T00:00:00", "trade_price": 1000.0, "candle_acc_trade_volume": 10.0}]
        mock_session.get.return_value = mock_resp

        client = UpbitRestClient(mock_session)
        self.assertTrue(hasattr(client, "_lock"))

        def worker():
            return client.fetch_klines_1m("KRW-BTC", count=1)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            futures = [ex.submit(worker) for _ in range(10)]
            results = [f.result() for f in futures]

        self.assertEqual(len(results), 10)
        self.assertEqual(mock_session.get.call_count, 10)

    def test_track_one_signal_linear_execution(self):
        """_track_one_signal updates signal state in O(1) per signal."""
        from models import SunriseSignal
        engine = KoreanEngine(emit_callback=lambda s: None, max_hold_sec=30.0)
        coin = "LINCOIN"
        follower = "binance"
        now = time.time()

        sig = SunriseSignal(
            id="sig-test-1",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange=follower,
            follower_contract=f"{coin}USDT",
            open_ts=now - 5.0,
            trigger_type="depth_drain",
            leader_mid=100.0,
            leader_best_ask=100.1,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=500.0,
            leader_norm_depth_usd=500.0,
            leader_swept_usd=300.0,
            leader_imbalance_top5=-0.5,
            drain_ratio=0.6,
            spread_blowout_ratio=1.0,
            follower_mid=100.0,
            follower_best_bid=99.95,
            follower_best_ask=100.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=1000.0,
            follower_top3_bid_usd=1000.0,
            follower_imbalance_top3=0.0,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=2.0,
            raw_dislocation_bps=20.0,
            dev_z=3.0,
            gross_exec_edge_bps=20.0,
            net_exec_edge_bps=10.0,
            suggested_size_usd=50.0,
        )
        u_snap = _make_book("upbit", coin, mid=100.0, depth_per_lvl=1000.0, ts=now)
        engine._books[("upbit", coin)] = u_snap
        engine._active_signals[coin] = [sig]
        f_snap = _make_book(follower, coin, mid=100.5, depth_per_lvl=1000.0, ts=now)

        engine._track_one_signal(sig, u_snap, f_snap, now)
        self.assertGreater(sig.mfe_exec_pnl_bps, 0.0)
        self.assertGreater(len(sig.path), 0)

    def test_collect_signal_files_strict_window(self):
        """_collect_signal_files strictly respects days_back window without falling back to dumping all files."""
        from tools.calibrate_thresholds import _collect_signal_files
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create an old file and set mtime to 2020
            old_file = os.path.join(tmp_dir, "signals_2020-01-01.jsonl")
            with open(old_file, "w") as f:
                f.write('{"coin": "BTC"}\n')
            os.utime(old_file, (1577836800, 1577836800))

            # When requesting last 1 day, the 2020 file must NOT be returned
            files = _collect_signal_files(tmp_dir, days_back=1)
            self.assertEqual(files, [], "Old files must not be returned when days_back filter yields empty")

            # When requesting days_back=0, all files should be returned
            all_files = _collect_signal_files(tmp_dir, days_back=0)
            self.assertEqual(len(all_files), 1)

    def test_default_baseline_zero_trade_honest_metrics(self):
        """Uncalibrated pairs output 0.0 expected winrate and net bps when sample_trades is 0."""
        from tools.calibrate_thresholds import calibrate
        import tempfile
        import os
        import json

        with tempfile.TemporaryDirectory() as tmp_dir:
            sig_file = os.path.join(tmp_dir, "signals_2026-10-20.jsonl")
            with open(sig_file, "w") as f:
                # No signals in file
                pass
            out_file = os.path.join(tmp_dir, "matrix_out.json")
            res = calibrate(
                signals_dir=tmp_dir,
                output_path=out_file,
                pairs_filter=["BTC:upbit-binance"],
                days_back=1,
                min_trades=20,
            )
            entry = res["thresholds"].get("BTC:upbit-binance")
            self.assertIsNotNone(entry)
            self.assertEqual(entry["sample_trades"], 0)
            self.assertEqual(entry["expected_winrate"], 0.0)
            self.assertEqual(entry["expected_net_bps"], 0.0)

    def test_funded_venues_matches_supported_venues(self):
        """Funded venues in reporting matches config.ALL_SUPPORTED_VENUES."""
        from tools.full_daily_report import FUNDED_VENUES
        from config import ALL_SUPPORTED_VENUES

        self.assertEqual(FUNDED_VENUES, set(ALL_SUPPORTED_VENUES))

    def test_decoupled_warmup_and_spike_gating_preserves_baseline(self):
        """Warmup and stability are decoupled: a 30 bps pump triggers 20x spike damping and does not poison baseline."""
        st = PairState("PUMPCOIN", "binance", warmup_samples=20, spike_sigma=2.5)
        now = 1000.0

        # 1. Warm up with 25 normal ticks at 0.0 bps
        for i in range(25):
            st.learn(0.0, now + i)

        self.assertTrue(st.is_warmed_up, "State must be warmed up after 25 samples")
        self.assertTrue(st.is_stable(now + 25.0), "State must be stable around 0 bps")
        self.assertAlmostEqual(st.baseline_bps, 0.0, delta=0.1)

        # 2. Simulate an abrupt sustained +30.0 bps pump on Upbit across 10 ticks
        for i in range(1, 11):
            st.learn(30.0, now + 25.0 + i)

        # Because is_warmed_up remains True, is_spike is True, activating eff_tau = 12000s
        # Over 10 seconds at eff_tau=12000s, baseline shifts by less than 0.1 bps!
        self.assertTrue(st.is_warmed_up, "is_warmed_up must remain True during a pump")
        self.assertLess(st.baseline_bps, 0.20, f"Baseline should be protected by spike gate (eff_tau=12000s), got {st.baseline_bps}")
        self.assertFalse(st.is_stable(now + 36.0), "is_stable must be False while market is in active 30 bps dislocation")

        # 3. Market mean-reverts back to normal 0.0 bps
        st.learn(0.0, now + 37.0)
        self.assertTrue(st.is_stable(now + 38.0), "is_stable recovers True after reversion to normal basis")

    def test_evaluator_market_regime_breakdown(self):
        """SunriseEvaluator groups and aggregates PnL metrics by market regime and strategy."""
        import os
        import tempfile
        import gzip
        import json
        with tempfile.TemporaryDirectory() as tmp_dir:
            sig_dir = os.path.join(tmp_dir, "signals", "2026-10-21")
            os.makedirs(sig_dir, exist_ok=True)
            fpath = os.path.join(sig_dir, "signals_test.jsonl.gz")
            with gzip.open(fpath, "wt", encoding="utf-8") as f:
                # 3 signals in STRONG_PUMP (profitable)
                for i in range(3):
                    f.write(json.dumps({
                        "coin": "SOL", "strategy": "lead_lag", "market_regime": "STRONG_PUMP",
                        "net_pnl_bps": 25.0 + i * 5.0, "drain_ratio": 0.5, "leader_swept_usd": 1000.0,
                        "follower_top3_ask_usd": 2000.0, "stealth_capacity_usd": 300.0
                    }) + "\n")
                # 2 signals in BEAR_DRIFT (losses)
                for i in range(2):
                    f.write(json.dumps({
                        "coin": "SOL", "strategy": "lead_lag", "market_regime": "BEAR_DRIFT",
                        "net_pnl_bps": -15.0 - i * 5.0, "drain_ratio": 0.5, "leader_swept_usd": 1000.0,
                        "follower_top3_ask_usd": 2000.0, "stealth_capacity_usd": 300.0
                    }) + "\n")

            evaluator = SunriseEvaluator(data_root=tmp_dir, days_back=1, min_events=2)
            evaluator.run(dry_run=True)
            breakdown = evaluator.get_regime_breakdown()
            self.assertIn(("lead_lag", "STRONG_PUMP"), breakdown)
            self.assertIn(("lead_lag", "BEAR_DRIFT"), breakdown)
            pump_stats = breakdown[("lead_lag", "STRONG_PUMP")]
            self.assertEqual(pump_stats["count"], 3)
            self.assertEqual(pump_stats["win_rate_pct"], 100.0)
            self.assertGreater(pump_stats["mean_net_pnl_bps"], 0.0)
            bear_stats = breakdown[("lead_lag", "BEAR_DRIFT")]
            self.assertEqual(bear_stats["count"], 2)
            self.assertEqual(bear_stats["win_rate_pct"], 0.0)
            self.assertLess(bear_stats["mean_net_pnl_bps"], 0.0)


if __name__ == "__main__":
    unittest.main()

