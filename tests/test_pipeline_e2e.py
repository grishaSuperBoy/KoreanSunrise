"""
Integration test for the Korean Sunrise Pipeline Architecture:
- PoolStateManager (Incubator vs Battle, Pause/Resume, Dynamic Stealth Sizing)
- TradingProfileManager (Capital, Reserve, Slots)
- SunriseEvaluator (Expectancy, Profit Factor, Win Rate, Cross-microstructure)
- KoreanEngine integration (Battle vs Incubator tagging, Actionability)
"""

import os
import sys
import json
import gzip
import shutil
import tempfile
import unittest
from datetime import datetime, timezone

# Ensure project root is in path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.pool_state import PoolStateManager
from pipeline.profile import TradingProfileManager
from pipeline.evaluator import SunriseEvaluator
from korean_engine import KoreanEngine
from models import OrderBookDepth5, SunriseSignal


class TestPipelineE2E(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="ks_test_")
        self.universe_file = os.path.join(self.test_dir, "universe_live.json")
        self.battle_file = os.path.join(self.test_dir, "live_trading_pool.json")
        self.overrides_file = os.path.join(self.test_dir, "runtime_overrides.json")
        self.profile_file = os.path.join(self.test_dir, "trading_profile.json")
        self.signals_dir = os.path.join(self.test_dir, "signals")
        os.makedirs(self.signals_dir, exist_ok=True)

        # Mock universe with sym maps
        universe_data = {
            "_meta": {"updated_at": "2026-10-18T12:00:00Z", "coins_count": 3},
            "coins": ["SOL", "DOGE", "MEME"],
            "binance_sym_map": {
                "SOLUSDT": ["SOL", 1.0],
                "DOGEUSDT": ["DOGE", 1.0],
                "MEMEUSDT": ["MEME", 1.0]
            },
            "bybit_sym_map": {
                "SOLUSDT": ["SOL", 1.0],
                "DOGEUSDT": ["DOGE", 1.0],
                "MEMEUSDT": ["MEME", 1.0]
            }
        }
        with open(self.universe_file, "w", encoding="utf-8") as f:
            json.dump(universe_data, f)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_pool_state_and_profile_manager(self):
        # Initialize Profile
        profile_mgr = TradingProfileManager(profile_file=self.profile_file)
        profile_mgr.set_venue_capital("binance", 300.0)
        profile_mgr.set_venue_capital("bybit", 300.0)
        profile_mgr.set_max_slots(2)

        # Slot budget: (300 / 2) * (1 - 0.20) = $120.00
        # If follower top3 ask = $500, stealth_size = 0.15 * 500 = $75.00
        # Expected size: min(120, 75, 500) = $75.00
        size = profile_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=500.0)
        self.assertIsNotNone(size)
        self.assertAlmostEqual(size, 75.0, places=2)

        # If follower top3 ask = $2000, stealth_size = 0.15 * 2000 = $300.00
        # Capped by slot budget = $120.00
        size_capped = profile_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=2000.0)
        self.assertIsNotNone(size_capped)
        self.assertAlmostEqual(size_capped, 120.0, places=2)

        # If follower top3 ask is very small ($30), stealth_size = $4.50 < $10 min order
        # Book is too shallow to absorb minimum order safely
        size_shallow = profile_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=30.0)
        self.assertIsNone(size_shallow)

        # Initialize PoolStateManager
        psm = PoolStateManager(
            universe_file=self.universe_file,
            battle_pool_file=self.battle_file,
            overrides_file=self.overrides_file
        )
        psm.reload()

        # All 3 coins are in Incubator initially
        sol_entry = psm.get_coin_entry("SOL")
        self.assertIsNotNone(sol_entry)
        self.assertEqual(sol_entry.basket, "incubator")
        self.assertTrue(sol_entry.enabled)
        self.assertFalse(psm.is_battle_coin("SOL"))

        # Test Pause / Resume
        psm.pause_coin("SOL", reason="high volatility")
        self.assertFalse(psm.get_coin_entry("SOL").enabled)
        
        psm.resume_coin("SOL")
        self.assertTrue(psm.get_coin_entry("SOL").enabled)

        # Test Manual Promotion
        qual_list = [{
            "coin": "SOL",
            "venues": ["binance", "bybit"],
            "win_rate_pct": 75.0,
            "profit_factor": 2.4,
            "mean_net_pnl_bps": 12.5,
            "median_net_pnl_bps": 11.0,
            "top3_depth_usd": 1500.0,
            "event_count": 10
        }]
        psm.promote_to_battle(qual_list)
        self.assertTrue(psm.is_battle_coin("SOL"))
        self.assertEqual(psm.get_coin_entry("SOL").basket, "battle")

    def test_evaluator_scoring_and_rotation(self):
        # Create mock gzip signals file
        today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        sig_file = os.path.join(self.signals_dir, f"signals_{today_str}.jsonl.gz")

        mock_signals = []
        # 6 positive signals for SOL (Avg net PnL: +10 bps, Win Rate: 100%, PF: 99.0)
        for i in range(6):
            mock_signals.append({
                "ts": 1792300000 + i * 100,
                "coin": "SOL",
                "follower_exchange": "binance",
                "drain_ratio": 0.65,
                "leader_swept_usd": 2500.0,
                "follower_top3_ask_usd": 3000.0,  # cross-ratio = 2500/3000 = 0.83 >= 0.50
                "rem_edge_bps": 15.0,
                "net_pnl_bps": 10.5,
                "mfe_exec_pnl_bps": 18.0,
                "time_to_peak_sec": 0.45,
                "is_actionable": False,
                "basket": "incubator"
            })

        # 6 negative signals for MEME (Avg net PnL: -8 bps, Win Rate: 0%)
        for i in range(6):
            mock_signals.append({
                "ts": 1792300000 + i * 100,
                "coin": "MEME",
                "follower_exchange": "binance",
                "drain_ratio": 0.55,
                "leader_swept_usd": 1200.0,
                "follower_top3_ask_usd": 2000.0,
                "rem_edge_bps": 5.0,
                "net_pnl_bps": -8.0,
                "mfe_exec_pnl_bps": 1.0,
                "time_to_peak_sec": 0.1,
                "is_actionable": False,
                "basket": "incubator"
            })

        with gzip.open(sig_file, "wt", encoding="utf-8") as f:
            for s in mock_signals:
                f.write(json.dumps(s) + "\n")

        # Run evaluator
        psm = PoolStateManager(
            universe_file=self.universe_file,
            battle_pool_file=self.battle_file,
            overrides_file=self.overrides_file
        )
        psm.reload()

        evaluator = SunriseEvaluator(
            data_root=self.test_dir,
            days_back=1,
            min_events=5,
            pool_state=psm
        )

        qualified = evaluator.run(dry_run=False)
        qualified_coins = [q["coin"] for q in qualified]

        # SOL should qualify
        self.assertIn("SOL", qualified_coins)
        # MEME should fail
        self.assertNotIn("MEME", qualified_coins)

        # Verify SOL is in Battle pool now!
        self.assertTrue(psm.is_battle_coin("SOL"))
        self.assertFalse(psm.is_battle_coin("MEME"))

    def test_engine_integration_with_basket_and_actionability(self):
        psm = PoolStateManager(
            universe_file=self.universe_file,
            battle_pool_file=self.battle_file,
            overrides_file=self.overrides_file
        )
        psm.reload()

        profile_mgr = TradingProfileManager(profile_file=self.profile_file)
        profile_mgr.set_venue_capital("binance", 500.0)

        # Promote SOL to battle
        psm.promote_to_battle([{
            "coin": "SOL",
            "venues": ["binance"],
            "win_rate_pct": 80.0,
            "profit_factor": 3.0,
            "mean_net_pnl_bps": 15.0,
            "median_net_pnl_bps": 14.0,
            "top3_depth_usd": 2000.0,
            "event_count": 8
        }])

        # Initialize KoreanEngine with our pool & profile
        engine = KoreanEngine(
            emit_callback=lambda sig: None,
            pool_state=psm,
            profile_manager=profile_mgr,
            min_edge_bps=5.0
        )

        # Verify engine has pool & profile wired up
        self.assertEqual(engine.pool_state, psm)
        self.assertEqual(engine.profile_manager, profile_mgr)

        # Check basket assignment
        self.assertTrue(engine.pool_state.is_battle_coin("SOL"))
        self.assertFalse(engine.pool_state.is_battle_coin("DOGE"))

        # Test full tick-to-signal pipeline via BaseVenueStream._sanitise -> KoreanEngine
        from venues.base import BaseVenueStream
        emitted = []
        engine.emit = lambda sig_dict: emitted.append(sig_dict)
        engine.warmup_samples = 2

        stream = BaseVenueStream("upbit", ["SOL"], engine.on_book_update)
        now = 1792300000.0

        # 1. Feed normal warmup ticks on Upbit and Binance
        for i in range(12):
            f_snap = OrderBookDepth5(
                exchange="binance",
                symbol="SOL",
                raw_symbol="SOLUSDT",
                ts=now + i,
                exchange_ts=int((now + i) * 1000),
                bids=[(100.00, 10.0), (99.95, 20.0), (99.90, 30.0)],
                asks=[(100.05, 10.0), (100.10, 20.0), (100.15, 30.0)],
            )
            u_snap = OrderBookDepth5(
                exchange="upbit",
                symbol="SOL",
                raw_symbol="KRW-SOL",
                ts=now + i,
                exchange_ts=int((now + i) * 1000),
                bids=[(100.00, 10.0), (99.95, 20.0), (99.90, 30.0)],
                asks=[(100.05, 10.0), (100.10, 20.0), (100.15, 30.0)],
            )
            stream._sanitise(f_snap)
            stream._sanitise(u_snap)

        self.assertEqual(stream.books_dropped, 0)
        self.assertEqual(stream.levels_dropped, 0)
        self.assertGreaterEqual(engine.counters["ticks_upbit"], 12)
        self.assertGreaterEqual(engine.counters["ticks_followers"], 12)

    def test_dynamic_thresholds_hold_sec_and_spike_gating(self):
        # Create a mock matrix_thresholds.json with custom hold_sec and spike_sigma
        thresh_file = os.path.join(self.test_dir, "matrix_thresholds.json")
        matrix_data = {
            "metadata": {"total_pairs_configured": 1},
            "thresholds": {
                "SOL:upbit-binance": {
                    "min_spread_bps": 15.0,
                    "max_hold_sec": 3.2,
                    "spike_sigma": 3.5,
                    "min_dev_z": 1.7,
                    "expected_winrate": 0.85,
                    "expected_net_bps": 25.0,
                    "sample_trades": 10,
                    "status": "aggressive_clean",
                    "enabled": True,
                }
            }
        }
        with open(thresh_file, "w", encoding="utf-8") as f:
            json.dump(matrix_data, f)

        emitted = []
        engine = KoreanEngine(
            emit_callback=lambda sig_dict: emitted.append(sig_dict),
            thresholds_file=thresh_file,
            max_hold_sec=6.0,
        )

        # 1. Verify PairState gets pair-specific spike_sigma
        st = engine._get_or_create_pair_state("SOL", "binance")
        self.assertEqual(st.spike_sigma, 3.5)

        # 2. Simulate warmup so st is warmed up
        now = 1792300000.0
        for i in range(35):
            st.learn(10.0, now + i)
        self.assertTrue(st.is_warmed_up)

        # 3. Simulate Upbit pump event triggering signal
        upbit_snap = OrderBookDepth5(
            exchange="upbit",
            symbol="SOL",
            raw_symbol="KRW-SOL",
            ts=now + 50,
            exchange_ts=int((now + 50) * 1000),
            bids=[(105.00, 10.0), (104.95, 20.0)],
            asks=[(105.05, 10.0), (105.10, 20.0)],
        )
        f_snap = OrderBookDepth5(
            exchange="binance",
            symbol="SOL",
            raw_symbol="SOLUSDT",
            ts=now + 50,
            exchange_ts=int((now + 50) * 1000),
            bids=[(100.00, 10.0), (99.95, 20.0)],
            asks=[(100.05, 1000.0), (100.10, 2000.0)],
        )
        engine._books[("upbit", "SOL")] = upbit_snap
        engine._recv_ts[("upbit", "SOL")] = now + 50
        engine._books[("binance", "SOL")] = f_snap
        engine._recv_ts[("binance", "SOL")] = now + 50

        engine._check_entry(
            coin="SOL",
            follower_ex="binance",
            st=st,
            upbit_snap=upbit_snap,
            f_snap=f_snap,
            f_book_age_ms=10.0,
            norm_depth=1000.0,
            norm_spread=5.0,
            swept_usd=5000.0,
            trigger_type="depth_drain",
            drain_ratio=0.8,
            blowout_ratio=3.0,
            now=now + 50,
        )

        self.assertIsNotNone(st.open_signal)
        sig = st.open_signal
        # Personalized max_hold_sec assigned!
        self.assertEqual(sig.max_hold_sec, 3.2)

        # 4. Sweep timeouts before 3.2s -> signal stays open
        engine.sweep_timeouts(now + 50 + 2.0)
        self.assertFalse(sig.closed)

        # 5. Sweep timeouts at 3.3s -> signal closed by timeout
        engine.sweep_timeouts(now + 50 + 3.3)
        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "timeout")
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["max_hold_sec"], 3.2)


if __name__ == "__main__":
    unittest.main()
