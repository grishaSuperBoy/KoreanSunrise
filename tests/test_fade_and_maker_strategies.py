"""
tests/test_fade_and_maker_strategies.py - Unit test suite for Strategy 2 (Fade Fakeout) and Strategy 3 (Maker Voids).
"""
from __future__ import annotations

import unittest
import time
import os
from typing import List, Dict, Any

from models import OrderBookDepth5
from strategies import FadeFakeoutStrategy, MakerVoidStrategy


def make_snap(ex: str, sym: str, bids: list, asks: list, now: float, raw: str = "") -> OrderBookDepth5:
    return OrderBookDepth5(
        exchange=ex,
        symbol=sym,
        raw_symbol=raw or sym,
        ts=now,
        exchange_ts=int(now * 1000),
        bids=bids,
        asks=asks,
    )


class TestFadeAndMakerStrategies(unittest.TestCase):

    def setUp(self):
        self.emitted: List[Dict[str, Any]] = []
        self.fade_strat = FadeFakeoutStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            roundtrip_fee_bps=10.0,
            min_order_usd=10.0,
            max_position_budget_usd=500.0,
            stealth_depth_fraction=0.15,
            max_hold_sec=5.0,
            stop_invalidation_bps=20.0,
            target_fade_bps=12.0,
            min_conviction_usd=3500.0,
            max_upbit_spread_bps=45.0,
        )
        self.maker_strat = MakerVoidStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            min_order_usd=10.0,
            max_position_budget_usd=250.0,
            stealth_depth_fraction=0.15,
            min_spread_bps=25.0,
            min_maker_edge_bps=5.0,
            maker_rebate_bps=1.5,
            resting_ttl_sec=2.0,
            max_hold_sec=6.0,
            stop_invalidation_bps=15.0,
        )

    # -------------------------------------------------------------------------
    # 1. Strategy 2: Fade the Fakeout (Short on MM Quote Cancel)
    # -------------------------------------------------------------------------
    def test_fade_entry_on_mm_cancel(self):
        now = time.time()
        coin = "GTC"

        upbit_snap = make_snap("upbit", "GTC", [(1.00, 1000.0)], [(1.006, 500.0)], now)
        f_snap = make_snap("binance", "GTC", [(1.00, 5000.0)], [(1.0005, 5000.0)], now, "GTCUSDT")

        opened = self.fade_strat.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=2000.0,
            upbit_snap=upbit_snap,
            follower_snaps={"binance": f_snap},
            now=now,
            fade_reason="mm_quote_cancel",
        )

        self.assertEqual(len(opened), 1)
        pos = opened[0]
        self.assertEqual(pos.coin, "GTC")
        self.assertEqual(pos.follower_exchange, "binance")
        self.assertEqual(pos.entry_px, 1.00)

        # Subsequent tick: Upbit mean-reverts down by 30 bps, follower price drops
        f_snap2 = make_snap("binance", "GTC", [(0.997, 5000.0)], [(0.9975, 5000.0)], now + 1.0, "GTCUSDT")

        self.fade_strat.on_follower_tick(
            ex="binance",
            coin="GTC",
            f_snap=f_snap2,
            now=now + 1.0,
            cur_upbit_mid=0.997,
        )

        self.assertEqual(len(self.emitted), 1)
        rec = self.emitted[0]
        self.assertEqual(rec["strategy"], "fade_fakeout")
        self.assertEqual(rec["side"], "short")
        self.assertEqual(rec["close_reason"], "converged")
        self.assertGreater(rec["net_pnl_bps"], 0.0)

    def test_fade_stop_invalidation_when_pump_continues(self):
        now = time.time()
        coin = "FLOCK"

        upbit_snap = make_snap("upbit", "FLOCK", [(0.05, 1000.0)], [(0.051, 500.0)], now)
        f_snap = make_snap("bybit", "FLOCK", [(0.05, 5000.0)], [(0.05005, 5000.0)], now, "FLOCKUSDT")

        opened = self.fade_strat.on_fakeout_detected(
            coin=coin,
            trigger_type="spread_blowout",
            swept_usd=1000.0,
            upbit_snap=upbit_snap,
            follower_snaps={"bybit": f_snap},
            now=now,
            fade_reason="micro_sweep_exhaustion",
        )
        self.assertEqual(len(opened), 1)

        # Opposite movement: Follower actually pumps +30 bps against short
        f_snap2 = make_snap("bybit", "FLOCK", [(0.0515, 5000.0)], [(0.0516, 5000.0)], now + 0.5, "FLOCKUSDT")

        self.fade_strat.on_follower_tick(
            ex="bybit",
            coin="FLOCK",
            f_snap=f_snap2,
            now=now + 0.5,
            cur_upbit_mid=0.0515,
        )

        self.assertEqual(len(self.emitted), 1)
        rec = self.emitted[0]
        self.assertEqual(rec["close_reason"], "stop_invalidation")
        self.assertLess(rec["net_pnl_bps"], 0.0)

    # -------------------------------------------------------------------------
    # 2. Strategy 3: Maker Liquidity Harvesting (Maker Post-Only in Blown Spread)
    # -------------------------------------------------------------------------
    def test_maker_void_post_only_and_fill(self):
        now = time.time()
        coin = "TIA"

        # Follower spread is blown out: bid=10.00, ask=10.06 -> 60 bps spread
        f_snap = make_snap("bingx", "TIA", [(10.00, 2000.0)], [(10.06, 2000.0)], now, "TIA-USDT")
        upbit_snap = make_snap("upbit", "TIA", [(10.05, 1000.0)], [(10.07, 1000.0)], now)

        void = self.maker_strat.check_void_opportunity(
            follower_ex="bingx",
            coin="TIA",
            f_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            norm_spread_bps=15.0,
        )

        self.assertIsNotNone(void)
        self.assertEqual(void.side, "ask")
        # Ask limit price is halfway in spread: 10.06 - 0.5 * 0.06 = 10.03
        self.assertAlmostEqual(void.target_limit_px, 10.03, places=4)
        self.assertFalse(void.maker_filled)

        # Intermediate tick: buyer crosses limit price (bid rises to 10.03 >= 10.03) -> FILLED!
        f_snap2 = make_snap("bingx", "TIA", [(10.03, 1500.0)], [(10.04, 1500.0)], now + 0.3, "TIA-USDT")
        self.maker_strat.on_follower_tick(
            follower_ex="bingx",
            coin="TIA",
            f_snap=f_snap2,
            now=now + 0.3,
            norm_spread_bps=15.0,
        )
        self.assertTrue(void.maker_filled)

        # Spread restores back to tight: bid=10.01, ask=10.02 -> 10 bps spread
        f_snap3 = make_snap("bingx", "TIA", [(10.01, 3000.0)], [(10.02, 3000.0)], now + 1.2, "TIA-USDT")
        self.maker_strat.on_follower_tick(
            follower_ex="bingx",
            coin="TIA",
            f_snap=f_snap3,
            now=now + 1.2,
            norm_spread_bps=15.0,
        )

        self.assertEqual(len(self.emitted), 1)
        rec = self.emitted[0]
        self.assertEqual(rec["strategy"], "maker_void")
        self.assertEqual(rec["order_type"], "maker_post_only")
        self.assertTrue(rec["maker_filled"])
        self.assertEqual(rec["close_reason"], "restored")
        self.assertGreater(rec["net_pnl_bps"], 0.0)

    def test_maker_void_unfilled_ttl_cancellation(self):
        now = time.time()
        coin = "O"

        f_snap = make_snap("pionex", "O", [(0.40, 2000.0)], [(0.404, 2000.0)], now, "O_USDT_PERP")

        void = self.maker_strat.check_void_opportunity(
            follower_ex="pionex",
            coin="O",
            f_snap=f_snap,
            upbit_snap=None,
            now=now,
            norm_spread_bps=15.0,
        )
        self.assertIsNotNone(void)

        # Time passes beyond TTL (2.5s > 2.0s TTL) without price touching limit
        f_snap2 = make_snap("pionex", "O", [(0.40, 2000.0)], [(0.404, 2000.0)], now + 2.5, "O_USDT_PERP")
        self.maker_strat.on_follower_tick(
            follower_ex="pionex",
            coin="O",
            f_snap=f_snap2,
            now=now + 2.5,
            norm_spread_bps=15.0,
        )

        self.assertEqual(len(self.emitted), 1)
        rec = self.emitted[0]
        self.assertEqual(rec["close_reason"], "unfilled_timeout")
        self.assertFalse(rec["maker_filled"])
        self.assertEqual(rec["net_pnl_bps"], 0.0)

    # -------------------------------------------------------------------------
    # 3. Bug Fix Verification Tests
    # -------------------------------------------------------------------------
    def test_fade_timeout_no_follower_data_penalizes_fees(self):
        """Fix 1: Timeout on Upbit tick writes -roundtrip_fee_bps and timeout_no_follower_data, never MFE."""
        now = time.time()
        coin = "TREE"

        upbit_snap = make_snap("upbit", "TREE", [(0.10, 1000.0)], [(0.101, 500.0)], now)
        f_snap = make_snap("binance", "TREE", [(0.10, 5000.0)], [(0.1001, 5000.0)], now, "TREEUSDT")

        opened = self.fade_strat.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=1000.0,
            upbit_snap=upbit_snap,
            follower_snaps={"binance": f_snap},
            now=now,
            fade_reason="micro_sweep_exhaustion",
        )
        self.assertEqual(len(opened), 1)

        # Trigger on_upbit_tick beyond 1.5x max_hold_sec (5.0s * 1.5 = 7.5s)
        self.fade_strat.on_upbit_tick(coin=coin, upbit_snap=upbit_snap, now=now + 8.0)

        self.assertEqual(len(self.emitted), 1)
        rec = self.emitted[0]
        self.assertEqual(rec["close_reason"], "timeout_no_follower_data")
        self.assertEqual(rec["net_pnl_bps"], -self.fade_strat.roundtrip_fee_bps)
        self.assertEqual(rec["exit_px"], rec["entry_px"])

    def test_fade_stale_and_unstable_guards(self):
        """Fix 5: Stale follower book (>1.5s) or unstable baseline rejects fade entry."""
        now = time.time()
        coin = "GTC"
        upbit_snap = make_snap("upbit", "GTC", [(1.00, 1000.0)], [(1.006, 500.0)], now)
        f_snap = make_snap("binance", "GTC", [(1.00, 5000.0)], [(1.0005, 5000.0)], now, "GTCUSDT")

        # Stale follower quote (received 3.0s ago)
        opened_stale = self.fade_strat.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=2000.0,
            upbit_snap=upbit_snap,
            follower_snaps={"binance": f_snap},
            now=now,
            fade_reason="mm_quote_cancel",
            recv_ts_map={("binance", coin): now - 3.0},
        )
        self.assertEqual(len(opened_stale), 0)

        # Unstable pair (is_stable_pair_fn returns False)
        opened_unstable = self.fade_strat.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=2000.0,
            upbit_snap=upbit_snap,
            follower_snaps={"binance": f_snap},
            now=now,
            fade_reason="mm_quote_cancel",
            recv_ts_map={("binance", coin): now},
            is_stable_pair_fn=lambda c, ex, t: False,
        )
        self.assertEqual(len(opened_unstable), 0)

    def test_maker_realistic_exit_taker_fee(self):
        """Fix 6: Maker position exit deducts realistic taker fee."""
        now = time.time()
        coin = "TIA"
        f_snap = make_snap("bingx", "TIA", [(10.00, 2000.0)], [(10.06, 2000.0)], now, "TIA-USDT")

        void = self.maker_strat.check_void_opportunity(
            follower_ex="bingx",
            coin=coin,
            f_snap=f_snap,
            upbit_snap=None,
            now=now,
            norm_spread_bps=15.0,
        )
        self.assertIsNotNone(void)

        # Fill order
        f_snap2 = make_snap("bingx", "TIA", [(10.03, 1500.0)], [(10.04, 1500.0)], now + 0.5, "TIA-USDT")
        self.maker_strat.on_follower_tick("bingx", coin, f_snap2, now + 0.5, norm_spread_bps=15.0)
        self.assertTrue(void.maker_filled)

        # Spread restores to normal (10 bps): bid=10.01, ask=10.02
        # Gross pnl = (10.03 - 10.02) / 10.03 * 1e4 ≈ 9.97 bps
        # Net pnl = gross + rebate (1.5) - taker_fee (5.0) = 9.97 + 1.5 - 5.0 = 6.47 bps
        f_snap3 = make_snap("bingx", "TIA", [(10.01, 3000.0)], [(10.02, 3000.0)], now + 1.0, "TIA-USDT")
        self.maker_strat.on_follower_tick("bingx", coin, f_snap3, now + 1.0, norm_spread_bps=15.0)

        self.assertEqual(len(self.emitted), 1)
        rec = self.emitted[0]
        expected_net = round(((10.03 - 10.02) / 10.03 * 1e4) + 1.5 - 5.0, 2)
        self.assertAlmostEqual(rec["net_pnl_bps"], expected_net, places=1)
        self.assertEqual(rec["taker_fee_bps"], 5.0)

    def test_maker_sweep_timeouts_cleans_hanging_orders(self):
        """Fix 4: MakerVoidStrategy.sweep_timeouts cancels resting limit orders when streams die."""
        now = time.time()
        coin = "O"
        f_snap = make_snap("pionex", "O", [(0.40, 2000.0)], [(0.404, 2000.0)], now, "O_USDT_PERP")

        void = self.maker_strat.check_void_opportunity("pionex", coin, f_snap, None, now, norm_spread_bps=15.0)
        self.assertIsNotNone(void)

        # Calling sweep_timeouts beyond TTL cancels resting order
        self.maker_strat.sweep_timeouts(now + 3.0)
        self.assertEqual(len(self.emitted), 1)
        self.assertEqual(self.emitted[0]["close_reason"], "unfilled_timeout")
        self.assertEqual(self.emitted[0]["net_pnl_bps"], 0.0)

    def test_open_signal_thread_safety_double_check(self):
        """Fix 10: PairState.open_signal is strictly guarded by _engine_lock preventing duplicate entries."""
        from korean_engine import KoreanEngine
        import threading

        engine = KoreanEngine(emit_callback=lambda s: None)
        now = time.time()
        coin = "SOL"
        st = engine._get_or_create_pair_state(coin, "binance")

        # Warm up pair state
        for _ in range(35):
            st.learn(10.0, now)

        upbit_snap = make_snap("upbit", coin, [(100.0, 5000.0)], [(101.5, 500.0)], now)
        f_snap = make_snap("binance", coin, [(100.0, 5000.0)], [(100.1, 5000.0)], now, "SOLUSDT")

        # Simulate concurrent entry attempt: 2 threads call _check_entry simultaneously
        results = []
        def attempt_entry():
            engine._check_entry(
                coin=coin,
                follower_ex="binance",
                st=st,
                upbit_snap=upbit_snap,
                f_snap=f_snap,
                f_book_age_ms=50.0,
                norm_depth=5000.0,
                norm_spread=10.0,
                swept_usd=4000.0,
                trigger_type="depth_drain",
                drain_ratio=0.8,
                blowout_ratio=1.5,
                now=now,
            )
            results.append(True)

        t1 = threading.Thread(target=attempt_entry)
        t2 = threading.Thread(target=attempt_entry)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Exactly 1 signal must be opened, not 2
        with engine._engine_lock:
            self.assertEqual(len(engine._active_signals[coin]), 1)
            self.assertIsNotNone(st.open_signal)
            self.assertEqual(st.open_signal.id, engine._active_signals[coin][0].id)

    def test_multi_strategy_reporting_and_web_cards(self):
        """Verifies multi-strategy report generation and web dashboard card parsing."""
        import tempfile
        import gzip
        import json
        from tools.full_daily_report import generate_daily_report
        from web.reports_manager import _build_card_from_files

        with tempfile.TemporaryDirectory() as tmpdir:
            sig_dir = os.path.join(tmpdir, "data", "signals", "2026-10-20")
            os.makedirs(sig_dir, exist_ok=True)
            rep_dir = os.path.join(tmpdir, "reports", "daily")
            os.makedirs(rep_dir, exist_ok=True)

            # Write mock data containing all 3 strategies
            gz_path = os.path.join(sig_dir, "signals.jsonl.gz")
            records = [
                # S1: Lead-Lag
                {
                    "strategy": "lead_lag", "coin": "BTC", "follower_exchange": "binance",
                    "net_pnl_bps": 25.0, "gross_exec_edge_bps": 35.0, "leader_swept_usd": 5000.0,
                    "follower_top3_ask_usd": 2000.0, "drain_ratio": 0.5, "close_reason": "converged",
                    "trigger_type": "depth_drain",
                },
                # S2: Fade Fakeout
                {
                    "strategy": "fade_fakeout", "coin": "TIA", "follower_exchange": "bybit",
                    "fade_reason": "mm_quote_cancel", "close_reason": "target_fade_reached",
                    "net_pnl_bps": 12.0, "duration_sec": 2.5, "mfe_exec_pnl_bps": 14.0, "mae_exec_pnl_bps": -2.0,
                },
                # S3: Maker Void
                {
                    "strategy": "maker_void", "coin": "SOL", "follower_exchange": "bingx",
                    "side": "bid", "maker_filled": True, "close_reason": "spread_restored",
                    "net_pnl_bps": 8.5, "maker_rebate_bps": 1.5, "taker_fee_bps": 5.0,
                    "spread_open_bps": 22.0, "spread_close_bps": 6.0,
                },
            ]
            with gzip.open(gz_path, "wt", encoding="utf-8") as gz:
                for r in records:
                    gz.write(json.dumps(r) + "\n")

            # Patch DATA_ROOT & REPORTS_DIR in full_daily_report
            import tools.full_daily_report as fdr
            old_data, old_rep = fdr.DATA_ROOT, fdr.REPORTS_DIR
            try:
                fdr.DATA_ROOT = os.path.join(tmpdir, "data")
                fdr.REPORTS_DIR = rep_dir

                doc = generate_daily_report(days_back=1.0, pos_size=10.0, roundtrip_fee_bps=10.0, as_json=True)

                # 1. Check all 3 strategies in document
                self.assertIn("strategies_breakdown", doc)
                sb = doc["strategies_breakdown"]
                self.assertEqual(sb["lead_lag"]["trades"], 1)
                self.assertEqual(sb["fade_fakeout"]["trades"], 1)
                self.assertEqual(sb["maker_void"]["trades"], 1)

                # 2. Check portfolio totals
                port = doc["portfolio"]
                self.assertEqual(port["total_executable_trades"], 3)
                self.assertEqual(port["total_wins"], 3)
                self.assertEqual(port["winrate_pct"], 100.0)

                # 3. Check web reports card parsing
                json_path = os.path.join(rep_dir, f"report_{doc['metadata']['report_date']}.json")
                md_path = os.path.join(rep_dir, f"report_{doc['metadata']['report_date']}.md")
                card = _build_card_from_files("2026-10-20", md_path, json_path)
                self.assertEqual(card["total_trades"], 3)
                self.assertEqual(card["winrate_pct"], 100.0)
                self.assertEqual(len(card["strategies_breakdown"]), 3)
                self.assertEqual(card["strategies_breakdown"][1]["name"], "2. Fade the Fakeout")
                self.assertEqual(card["strategies_breakdown"][1]["trades"], 1)
            finally:
                fdr.DATA_ROOT, fdr.REPORTS_DIR = old_data, old_rep


if __name__ == "__main__":
    unittest.main()
