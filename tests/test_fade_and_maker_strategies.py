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
            max_fade_swept_usd=3500.0,
            min_upbit_spread_bps=45.0,
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
        f_snap = make_snap("binance", "GTC", [(1.00, 2000.0)], [(1.0005, 2000.0)], now, "GTCUSDT")

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
        f_snap2 = make_snap("binance", "GTC", [(0.997, 2000.0)], [(0.9975, 2000.0)], now + 1.0, "GTCUSDT")

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
            is_warmed_up=True,
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
            is_warmed_up=True,
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
        f_snap = make_snap("binance", "GTC", [(1.00, 2000.0)], [(1.0005, 2000.0)], now, "GTCUSDT")

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
            is_warmed_up=True,
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

        void = self.maker_strat.check_void_opportunity("pionex", coin, f_snap, None, now, norm_spread_bps=15.0, is_warmed_up=True)
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

    # -------------------------------------------------------------------------
    # 5. Physics Calibration & Anti-Machinegun Verification Tests
    # -------------------------------------------------------------------------
    def test_fade_filters_reject_large_sweeps_and_tight_spreads(self):
        """Microstructure rule: Only micro-sweeps on wide spreads are fakeouts. Majors are rejected."""
        # 1. Large sweep ($10,000 / $33,000 like FIL or DOT) must NEVER be faded
        is_fake, _ = self.fade_strat.is_fakeout_pattern(
            is_cancel_blocked=True,
            swept_usd=10000.0,
            upbit_spread_bps=50.0,
        )
        self.assertFalse(is_fake)

        # 2. Tight spread (< 35 bps) on liquid majors must NEVER be faded
        is_fake, _ = self.fade_strat.is_fakeout_pattern(
            is_cancel_blocked=True,
            swept_usd=1000.0,
            upbit_spread_bps=15.0,
        )
        self.assertFalse(is_fake)

        # 3. Genuine micro-sweep on illiquid gap qualifies
        is_fake, reason = self.fade_strat.is_fakeout_pattern(
            is_cancel_blocked=False,
            swept_usd=1200.0,
            upbit_spread_bps=55.0,
        )
        self.assertTrue(is_fake)
        self.assertEqual(reason, "micro_sweep_exhaustion")

    def test_fade_cooldown_and_thick_depth_guard(self):
        """Guard against machine-gun spam and liquid venue thick order books."""
        now = time.time()
        coin = "TREE"
        upbit_snap = make_snap("upbit", coin, [(0.10, 1000.0)], [(0.101, 500.0)], now)

        # 1. Thick follower depth ($25,000) is rejected
        f_thick = make_snap("binance", coin, [(0.10, 250000.0)], [(0.1005, 250000.0)], now, "TREEUSDT")
        opened_thick = self.fade_strat.on_fakeout_detected(
            coin=coin, trigger_type="depth_drain", swept_usd=1000.0,
            upbit_snap=upbit_snap, follower_snaps={"binance": f_thick}, now=now,
            fade_reason="micro_sweep_exhaustion"
        )
        self.assertEqual(len(opened_thick), 0)

        # 2. Thin follower depth ($2,000) opens cleanly
        f_thin = make_snap("binance", coin, [(0.10, 20000.0)], [(0.1005, 20000.0)], now, "TREEUSDT")
        opened = self.fade_strat.on_fakeout_detected(
            coin=coin, trigger_type="depth_drain", swept_usd=1000.0,
            upbit_snap=upbit_snap, follower_snaps={"binance": f_thin}, now=now,
            fade_reason="micro_sweep_exhaustion"
        )
        self.assertEqual(len(opened), 1)

        # 3. Subsequent fade within cooldown (<30s) is blocked
        opened_cooldown = self.fade_strat.on_fakeout_detected(
            coin=coin, trigger_type="depth_drain", swept_usd=1000.0,
            upbit_snap=upbit_snap, follower_snaps={"binance": f_thin}, now=now + 5.0,
            fade_reason="micro_sweep_exhaustion"
        )
        self.assertEqual(len(opened_cooldown), 0)

    def test_maker_void_rejects_toxic_dex_spreads(self):
        """AsterDex / toxic DEX safeguard: spreads > max_spread_bps (120 bps) are rejected."""
        now = time.time()
        coin = "WAL"
        # 250 bps synthetic spread on AsterDex
        f_toxic = make_snap("asterdex", coin, [(1.00, 1000.0)], [(1.025, 1000.0)], now, "WALUSDT")

        void = self.maker_strat.check_void_opportunity(
            follower_ex="asterdex",
            coin=coin,
            f_snap=f_toxic,
            upbit_snap=None,
            now=now,
            norm_spread_bps=20.0,
        )
        self.assertIsNone(void)

    def test_asterdex_lead_lag_tight_spread_allowed_wide_rejected(self):
        """Lead-Lag Taker: AsterDex allowed on tight spreads (<=45 bps), rejected on wide defensive spreads."""
        from strategies.strategy_1_sweep_lead_lag import KoreanLeadLagStrategy, PairState
        strat1 = KoreanLeadLagStrategy(emit_callback=lambda s: None)
        now = time.time()
        coin = "GMT"
        st = PairState(coin, "asterdex", warmup_samples=10)
        for _ in range(15):
            st.learn(10.0, now)

        upbit_snap = make_snap("upbit", coin, [(1.00, 10000.0)], [(1.015, 5000.0)], now)

        # 1. Wide spread (80 bps > 45 bps) on AsterDex rejected
        f_wide = make_snap("asterdex", coin, [(1.00, 5000.0)], [(1.008, 5000.0)], now, "GMTUSDT")
        sig_wide = strat1.evaluate_entry(
            coin=coin, follower_ex="asterdex", st=st, upbit_snap=upbit_snap, f_snap=f_wide,
            f_book_age_ms=100.0, norm_depth=10000.0, norm_spread=15.0, swept_usd=5000.0,
            trigger_type="depth_drain", drain_ratio=0.6, blowout_ratio=1.5, now=now,
            recent_buys_usd=5000.0, pair_cfg={"enabled": True}, seq_num=1
        )
        self.assertIsNone(sig_wide)

        # 2. Tight spread (20 bps <= 45 bps) on AsterDex allowed
        f_tight = make_snap("asterdex", coin, [(1.00, 5000.0)], [(1.002, 5000.0)], now, "GMTUSDT")
        sig_tight = strat1.evaluate_entry(
            coin=coin, follower_ex="asterdex", st=st, upbit_snap=upbit_snap, f_snap=f_tight,
            f_book_age_ms=100.0, norm_depth=10000.0, norm_spread=15.0, swept_usd=5000.0,
            trigger_type="depth_drain", drain_ratio=0.6, blowout_ratio=1.5, now=now,
            recent_buys_usd=5000.0, pair_cfg={"enabled": True}, seq_num=2
        )
        self.assertIsNotNone(sig_tight)
        self.assertEqual(sig_tight.follower_exchange, "asterdex")

    def test_strategy_counters_shared_and_no_double_counting(self):
        shared_counters: Dict[str, int] = {}
        now = time.time()
        coin = "GTC"

        fade_strat = FadeFakeoutStrategy(
            emit_callback=lambda sig: None,
            max_fade_swept_usd=2000.0,
            min_upbit_spread_bps=35.0,
            max_follower_depth_usd=3500.0,
            counters=shared_counters,
        )
        maker_strat = MakerVoidStrategy(
            emit_callback=lambda sig: None,
            min_spread_bps=25.0,
            counters=shared_counters,
        )

        # 1. Fade signal trigger
        upbit_snap = make_snap("upbit", coin, [(1.00, 1000.0)], [(1.006, 500.0)], now)
        f_snap = make_snap("binance", coin, [(1.00, 2000.0)], [(1.0005, 2000.0)], now, "GTCUSDT")

        opened = fade_strat.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=1200.0,
            upbit_snap=upbit_snap,
            follower_snaps={"binance": f_snap},
            now=now,
            fade_reason="micro_sweep_exhaustion",
        )
        self.assertEqual(len(opened), 1)
        self.assertEqual(shared_counters["fade_signals_triggered"], 1)

        # Close fade on convergence
        f_snap_revert = make_snap("binance", coin, [(0.997, 2000.0)], [(0.9975, 2000.0)], now + 1.0, "GTCUSDT")
        fade_strat.on_follower_tick("binance", coin, f_snap_revert, now + 1.0, cur_upbit_mid=0.997)
        self.assertEqual(shared_counters["fade_signals_triggered"], 1)
        self.assertEqual(shared_counters["fade_signals_converged"], 1)

        # 2. Maker void trigger
        f_wide = make_snap("binance", coin, [(10.00, 5000.0)], [(10.06, 5000.0)], now, "GTCUSDT")
        upbit_snap2 = make_snap("upbit", coin, [(10.05, 1000.0)], [(10.07, 1000.0)], now)
        placed = maker_strat.check_void_opportunity(
            follower_ex="binance",
            coin=coin,
            f_snap=f_wide,
            upbit_snap=upbit_snap2,
            now=now,
            norm_spread_bps=15.0,
            is_warmed_up=True,
        )
        self.assertIsNotNone(placed)
        self.assertEqual(shared_counters["maker_voids_triggered"], 1)

        # Fill maker order
        f_fill = make_snap("binance", coin, [(10.03, 5000.0)], [(10.04, 5000.0)], now + 0.3, "GTCUSDT")
        maker_strat.on_follower_tick("binance", coin, f_fill, now + 0.3, norm_spread_bps=15.0)
        self.assertEqual(shared_counters["maker_voids_filled"], 1)

        # Restore spread / take profit
        f_tight = make_snap("binance", coin, [(10.01, 5000.0)], [(10.02, 5000.0)], now + 1.0, "GTCUSDT")
        maker_strat.on_follower_tick("binance", coin, f_tight, now + 1.0, norm_spread_bps=15.0)
        self.assertEqual(shared_counters["maker_voids_triggered"], 1)
        self.assertEqual(shared_counters["maker_voids_restored"], 1)

    # -------------------------------------------------------------------------
    # 5. Inverted Alpha Strategies: S2 Long Sniper & S3 Taker Void Hunter
    # -------------------------------------------------------------------------
    def test_inverted_s2_pre_sweep_long_execution(self):
        """Verifies Inverted S2 enters LONG on Upbit MM cancel and profits on follower pump."""
        now = time.time()
        coin = "GTC"
        emitted_longs = []

        long_fade = FadeFakeoutStrategy(
            emit_callback=lambda sig: emitted_longs.append(sig),
            roundtrip_fee_bps=10.0,
            invert_to_long=True,
            target_fade_bps=12.0,
            stop_invalidation_bps=20.0,
            min_upbit_spread_bps=35.0,
        )

        upbit_snap = make_snap("upbit", coin, [(1.00, 1000.0)], [(1.006, 500.0)], now)
        f_snap = make_snap("asterdex", coin, [(1.00, 2000.0)], [(1.0005, 2000.0)], now, "GTCUSDT")

        opened = long_fade.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=1500.0,
            upbit_snap=upbit_snap,
            follower_snaps={"asterdex": f_snap},
            now=now,
            fade_reason="mm_quote_cancel",
        )

        self.assertEqual(len(opened), 1)
        pos = opened[0]
        self.assertEqual(pos.side, "long")
        self.assertEqual(pos.entry_px, 1.0005) # Bought executable ask

        # Follower pumps up by 25 bps (bid moves to 1.0030)
        f_pump = make_snap("asterdex", coin, [(1.0030, 2000.0)], [(1.0035, 2000.0)], now + 0.5, "GTCUSDT")
        long_fade.on_follower_tick("asterdex", coin, f_pump, now + 0.5, cur_upbit_mid=1.0050)

        self.assertEqual(len(emitted_longs), 1)
        rec = emitted_longs[0]
        self.assertEqual(rec["side"], "long")
        self.assertEqual(rec["close_reason"], "converged")
        self.assertGreater(rec["net_pnl_bps"], 0.0)

    def test_inverted_s3_taker_void_hunter_execution(self):
        """Verifies Inverted S3 sweeps underpriced ask as an aggressive taker and captures pump."""
        now = time.time()
        coin = "TIA"
        emitted_hunter = []

        hunter_strat = MakerVoidStrategy(
            emit_callback=lambda sig: emitted_hunter.append(sig),
            min_spread_bps=25.0,
            target_pnl_bps=15.0,
            hunter_mode=True,
        )

        # Follower spread is tight (15 bps: 10.00 / 10.015), but Upbit mid is 10.08 (lag > 60 bps!)
        f_tight = make_snap("asterdex", coin, [(10.00, 3000.0)], [(10.015, 3000.0)], now, "TIA-USDT")
        upbit_snap = make_snap("upbit", coin, [(10.07, 1000.0)], [(10.09, 1000.0)], now)

        void = hunter_strat.check_void_opportunity(
            follower_ex="asterdex",
            coin=coin,
            f_snap=f_tight,
            upbit_snap=upbit_snap,
            now=now,
            norm_spread_bps=15.0,
            is_warmed_up=True,
        )

        self.assertIsNotNone(void)
        self.assertEqual(void.side, "buy")
        self.assertEqual(void.order_type, "taker_void_sweep")
        self.assertTrue(void.maker_filled) # Immediate taker execution
        self.assertEqual(void.fill_px, 10.015)

        # Follower market converges up to Upbit: bid moves to 10.08, ask to 10.09
        f_surge = make_snap("asterdex", coin, [(10.08, 5000.0)], [(10.09, 5000.0)], now + 0.8, "TIA-USDT")
        hunter_strat.on_follower_tick("asterdex", coin, f_surge, now + 0.8, norm_spread_bps=15.0)

        self.assertEqual(len(emitted_hunter), 1)
        rec = emitted_hunter[0]
        self.assertEqual(rec["order_type"], "taker_void_sweep")
        self.assertEqual(rec["side"], "buy")
        self.assertEqual(rec["close_reason"], "target_profit")
        self.assertGreater(rec["net_pnl_bps"], 0.0)

    def test_s2_long_sizing_uses_ask_depth_and_skips_thin_books(self):
        """Bug 3 & Bug 4 Verification: Long sizing consumes ask liquidity and skips thin books."""
        now = time.time()
        coin = "GTC"

        # Ask side has only $20 depth (0.20 * 100.0 = $20.0), Bid side has $500 depth (5.0 * 100.0 = $500.0)
        # stealth_depth_fraction is 0.15: 15% of $20 = $3.00, which is < min_order_usd ($10.0)
        # Previous bug: used max(ask, bid) -> 15% of $500 = $75, sizing long based on bid!
        # And if stealth < min_order, it forced $10 instead of skipping.
        thin_ask_snap = make_snap(
            "bybit", coin,
            [(100.0, 5.0)],    # $500 bid depth
            [(100.05, 0.20)],  # $20.01 ask depth
            now, "GTCUSDT"
        )
        upbit_snap = make_snap("upbit", coin, [(100.0, 10.0)], [(100.5, 10.0)], now)

        strat = FadeFakeoutStrategy(
            emit_callback=lambda s: None,
            min_order_usd=10.0,
            stealth_depth_fraction=0.15,
            invert_to_long=True,
            cooldown_sec=0.0,
        )

        opened = strat.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=500.0,
            upbit_snap=upbit_snap,
            follower_snaps={"bybit": thin_ask_snap},
            now=now,
            fade_reason="micro_sweep_exhaustion",
        )
        # Sizing 15% of $20.01 ask depth = $3 < min_order_usd ($10) -> MUST SKIP (not force $10)
        self.assertEqual(len(opened), 0)

        # Adequate ask depth ($200) -> 15% of $200 = $30 -> enters long
        adequate_ask_snap = make_snap(
            "bybit", coin,
            [(100.0, 0.5)],    # $50 bid depth
            [(100.0, 2.0)],    # $200 ask depth
            now, "GTCUSDT"
        )
        opened2 = strat.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=500.0,
            upbit_snap=upbit_snap,
            follower_snaps={"bybit": adequate_ask_snap},
            now=now,
            fade_reason="micro_sweep_exhaustion",
        )
        self.assertEqual(len(opened2), 1)
        self.assertEqual(opened2[0].side, "long")
        self.assertEqual(opened2[0].suggested_size_usd, 30.0) # Strictly 15% of ask depth!

    def test_extract_spread_bps_and_dict_compatibility(self):
        """Bug 7 Verification: _extract_spread_bps handles both dict and UpbitNormState seamlessly."""
        from korean_engine import _extract_spread_bps, UpbitNormState

        # 1. None returns default
        self.assertEqual(_extract_spread_bps(None, default=20.0), 20.0)

        # 2. Dict returns correct spread_bps
        dict_norm = {"depth_ask": 100.0, "best_ask_usd": 50.0, "spread_bps": 37.5, "t": 1000.0}
        self.assertEqual(_extract_spread_bps(dict_norm, default=20.0), 37.5)

        # 3. UpbitNormState returns correct spread_bps
        obj_norm = UpbitNormState(depth_ask=100.0, best_ask_usd=50.0, spread_bps=42.0, t=1000.0)
        self.assertEqual(_extract_spread_bps(obj_norm, default=20.0), 42.0)

    def test_follower_spread_ewma_and_stale_upbit_freshness_in_engine(self):
        """Bug 1 & Bug 2 Verification: Follower maintains own spread EWMA and S2 receives None if Upbit is stale."""
        from korean_engine import KoreanEngine
        now = time.time()
        coin = "DOGE"
        engine = KoreanEngine(emit_callback=lambda s: None)

        # 1. Bybit tick arrives with tight spread (5.0 bps)
        f_tight = make_snap("bybit", coin, [(0.1000, 1000.0)], [(0.10005, 1000.0)], now)
        engine.on_book_update(f_tight)
        self.assertIn(("bybit", coin), engine._follower_spread_ewma)
        self.assertAlmostEqual(engine._follower_spread_ewma[("bybit", coin)][0], 5.0, places=1)

        # 2. Upbit tick arrived at now - 5.0s (stale)
        upbit_snap = make_snap("upbit", coin, [(0.1000, 1000.0)], [(0.1003, 1000.0)], now - 5.0)
        engine._books[("upbit", coin)] = upbit_snap
        engine._recv_ts[("upbit", coin)] = now - 5.0

        captured_upbit_mid = []
        engine.strat_fade.on_follower_tick = lambda ex, c, s, t, cur_upbit_mid=None: captured_upbit_mid.append(cur_upbit_mid)

        engine.on_book_update(f_tight)
        self.assertEqual(len(captured_upbit_mid), 1)
        self.assertIsNone(captured_upbit_mid[0]) # Gated out due to staleness!

    def test_portfolio_caps_in_s2_and_s3(self):
        """Points 11 & 12 Verification: Portfolio caps & exposure limits in S2 and S3."""
        now = time.time()
        coin = "GTC"
        upbit_snap = make_snap("upbit", coin, [(100.0, 10.0)], [(100.5, 10.0)], now)

        # S2 with cap of 2 positions
        strat_s2 = FadeFakeoutStrategy(
            emit_callback=lambda s: None,
            min_order_usd=10.0,
            stealth_depth_fraction=0.15,
            invert_to_long=True,
            cooldown_sec=0.0,
            max_active_positions=2,
        )
        snaps = {
            "bybit": make_snap("bybit", coin, [(100.0, 1.0)], [(100.0, 2.0)], now, "GTCUSDT"),
            "binance": make_snap("binance", coin, [(100.0, 1.0)], [(100.0, 2.0)], now, "GTCUSDT"),
            "okx": make_snap("okx", coin, [(100.0, 1.0)], [(100.0, 2.0)], now, "GTCUSDT"),
        }
        opened = strat_s2.on_fakeout_detected(
            coin=coin,
            trigger_type="depth_drain",
            swept_usd=500.0,
            upbit_snap=upbit_snap,
            follower_snaps=snaps,
            now=now,
            fade_reason="micro_sweep_exhaustion",
        )
        # 3 followers supplied, but capped strictly at 2
        self.assertEqual(len(opened), 2)

        # S3 with cap of 1 void
        strat_s3 = MakerVoidStrategy(
            emit_callback=lambda s: None,
            max_active_voids=1,
            cooldown_sec=0.0,
        )
        f_snap1 = make_snap("bybit", "AAA", [(1.0, 100.0)], [(1.01, 100.0)], now) # 100 bps spread
        void1 = strat_s3.check_void_opportunity("bybit", "AAA", f_snap1, upbit_snap, now, norm_spread_bps=20.0, is_warmed_up=True)
        self.assertIsNotNone(void1)

        f_snap2 = make_snap("bybit", "BBB", [(1.0, 100.0)], [(1.01, 100.0)], now)
        void2 = strat_s3.check_void_opportunity("bybit", "BBB", f_snap2, upbit_snap, now, norm_spread_bps=20.0, is_warmed_up=True)
        # Capped at 1 active void
        self.assertIsNone(void2)

    def test_s3_staleness_guard(self):
        """Point 12 Verification: S3 rejects stale follower book snapshots (> max_follower_book_age_sec)."""
        now = time.time()
        strat_s3 = MakerVoidStrategy(emit_callback=lambda s: None, max_follower_book_age_sec=1.5)
        f_snap = make_snap("bybit", "AAA", [(1.0, 100.0)], [(1.01, 100.0)], now - 3.0) # 3.0s old

        void = strat_s3.check_void_opportunity(
            "bybit", "AAA", f_snap, None, now, norm_spread_bps=20.0, recv_ts=now - 3.0
        )
        self.assertIsNone(void)

    def test_s3_timeout_counter_separation(self):
        """Point 13 Verification: S3 increments maker_voids_timeout, not maker_voids_stopped."""
        now = time.time()
        emitted = []
        counters = {"maker_voids_timeout": 0, "maker_voids_stopped": 0}
        strat_s3 = MakerVoidStrategy(
            emit_callback=lambda s: emitted.append(s),
            max_hold_sec=5.0,
            hunter_mode=True,
            counters=counters,
        )
        # Tight spread (10 bps) with Upbit leading by 50 bps
        f_snap = make_snap("bybit", "AAA", [(1.0, 100.0)], [(1.001, 100.0)], now)
        upbit_snap = make_snap("upbit", "AAA", [(1.005, 100.0)], [(1.006, 100.0)], now)
        strat_s3.check_void_opportunity("bybit", "AAA", f_snap, upbit_snap, now, norm_spread_bps=10.0, is_warmed_up=True)

        # Trigger timeout after 6 seconds without subsequent ticks
        strat_s3.sweep_timeouts(now + 6.0)

        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["close_reason"], "timeout_no_follower_data")
        self.assertEqual(counters["maker_voids_timeout"], 1)
        self.assertEqual(counters["maker_voids_stopped"], 0) # MUST NOT be incremented!

    def test_s1_path_length_capped_at_50(self):
        """Point 14 Verification: KoreanEngine caps sig.path at 50 points to prevent unbounded memory growth."""
        from korean_engine import KoreanEngine
        from models import SunriseSignal
        now = time.time()
        coin = "GTC"
        engine = KoreanEngine(emit_callback=lambda s: None)
        st = engine._get_or_create_pair_state(coin, "bybit", leader_ex="upbit")

        upbit_t0 = make_snap("upbit", coin, [(10.0, 100.0)], [(10.01, 100.0)], now)
        f_t0 = make_snap("bybit", coin, [(10.0, 100.0)], [(10.01, 100.0)], now)
        engine._books[("upbit", coin)] = upbit_t0
        engine._books[("bybit", coin)] = f_t0
        engine._recv_ts[("upbit", coin)] = now
        engine._recv_ts[("bybit", coin)] = now

        sig = SunriseSignal(
            id="test_sig_01",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="bybit",
            follower_contract="GTCUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=10.005,
            leader_best_ask=10.01,
            leader_spread_bps=10.0,
            leader_norm_spread_bps=10.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=10.005,
            follower_best_bid=10.00,
            follower_best_ask=10.01,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=0.0,
            dev_z=1.0,
            gross_exec_edge_bps=50.0,
            net_exec_edge_bps=40.0,
            follower_entry_px=10.01,
            suggested_size_usd=100.0,
            max_hold_sec=15.0,
            path=[],
        )
        st.open_signal = sig
        engine._active_signals[coin].append(sig)

        # Simulate 80 follower ticks
        for i in range(1, 81):
            t_curr = now + i * 0.05
            f_tick = make_snap("bybit", coin, [(10.0, 100.0)], [(10.01, 100.0)], t_curr)
            engine.on_book_update(f_tick)

        self.assertLessEqual(len(sig.path), 50)

    def test_min_korean_confirm_usd_zero_telemetry_mode(self):
        """Verify that MIN_KOREAN_CONFIRM_USD=0.0 is not overridden by 500.0 fallback."""
        import config as C
        from unittest.mock import patch
        from korean_engine import KoreanEngine

        engine = KoreanEngine(emit_callback=lambda s: None)
        coin = "ZEROCOIN"
        now = time.time()
        engine._get_or_create_pair_state(coin, "bybit")

        # Provide leader and bithumb snaps
        engine._books[("upbit", coin)] = make_snap("upbit", coin, [(10.0, 100.0)], [(10.05, 100.0)], now)
        engine._recv_ts[("upbit", coin)] = now
        # Bithumb price is lower (parity is False)
        engine._books[("bithumb", coin)] = make_snap("bithumb", coin, [(9.5, 100.0)], [(9.6, 100.0)], now)
        engine._recv_ts[("bithumb", coin)] = now

        # Add a small buy trade of $5 (small trade)
        engine._bithumb_recent_buys[coin].append((now - 0.5, 5.0))
        engine._has_bithumb_trades[coin] = True

        with patch.object(C, "MIN_KOREAN_CONFIRM_USD", 0.0):
            cutoff = now - 2.0
            recent_b_buys, _ = engine.get_recent_bithumb_buys(coin, cutoff)
            self.assertEqual(recent_b_buys, 5.0)

            cfg_confirm = getattr(C, "MIN_KOREAN_CONFIRM_USD", 500.0)
            min_confirm_usd = float(cfg_confirm if cfg_confirm is not None else 500.0)
            self.assertEqual(min_confirm_usd, 0.0)

            # In 0.0 telemetry mode, any real buy (> 0) confirms volume
            vol_confirmed = (recent_b_buys >= min_confirm_usd and recent_b_buys > 0) if min_confirm_usd > 0 else (recent_b_buys > 0)
            self.assertTrue(vol_confirmed)

        # In standard production mode (500.0), $5 does NOT confirm volume
        with patch.object(C, "MIN_KOREAN_CONFIRM_USD", 500.0):
            cfg_confirm = getattr(C, "MIN_KOREAN_CONFIRM_USD", 500.0)
            min_confirm_usd = float(cfg_confirm if cfg_confirm is not None else 500.0)
            self.assertEqual(min_confirm_usd, 500.0)

            vol_confirmed = (recent_b_buys >= min_confirm_usd and recent_b_buys > 0) if min_confirm_usd > 0 else (recent_b_buys > 0)
            self.assertFalse(vol_confirmed)

    def test_follower_spread_norm_warmup_and_cold_start(self):
        """Verify FollowerSpreadNorm does not anchor to a blowout on cold-start and gates S3."""
        from korean_engine import FollowerSpreadNorm, KoreanEngine

        now = 1000.0
        norm = FollowerSpreadNorm(warmup_samples=5)
        self.assertFalse(norm.is_warmed_up)

        # Tick 1: An active blowout occurs on startup (120 bps)
        s1 = norm.update(120.0, now)
        self.assertEqual(s1, 120.0)
        self.assertFalse(norm.is_warmed_up)

        # Tick 2: Normal calm spread arrives (10 bps)
        s2 = norm.update(10.0, now + 1.0)
        # Cold start anchors to minimum observed spread during warmup, dropping to 10.0 immediately!
        self.assertEqual(s2, 10.0)
        self.assertFalse(norm.is_warmed_up)

        # Ticks 3, 4, 5
        norm.update(12.0, now + 2.0)
        norm.update(11.0, now + 3.0)
        s5 = norm.update(10.5, now + 4.0)
        self.assertTrue(norm.is_warmed_up)
        self.assertEqual(s5, 10.0)

        # Test S3 check_void_opportunity warmup gate
        maker_strat = MakerVoidStrategy(
            emit_callback=lambda sig: None,
            min_spread_bps=25.0,
        )
        snap = make_snap("bybit", "TEST", [(10.0, 100.0)], [(10.08, 100.0)], now) # 80 bps spread
        # Unwarmed -> returns None even though spread is blown out
        self.assertIsNone(maker_strat.check_void_opportunity("bybit", "TEST", snap, None, now, norm_spread_bps=10.0, is_warmed_up=False))
        # Warmed -> identifies void opportunity
        self.assertIsNotNone(maker_strat.check_void_opportunity("bybit", "TEST", snap, None, now, norm_spread_bps=10.0, is_warmed_up=True))

    def test_follower_spread_idle_pruning_in_sweep(self):
        """Verify idle follower spread EWMA entries (> 1 hour) are pruned during timeout sweeps."""
        from korean_engine import KoreanEngine, FollowerSpreadNorm

        engine = KoreanEngine(emit_callback=lambda s: None)
        now = 10000.0

        # Active entry (last updated 30s ago)
        active_norm = FollowerSpreadNorm(warmup_samples=5)
        active_norm.update(10.0, now - 30.0)
        engine._follower_spread_ewma[("bybit", "BTC")] = active_norm

        # Dead entry (last updated 2 hours ago)
        dead_norm = FollowerSpreadNorm(warmup_samples=5)
        dead_norm.update(10.0, now - 7200.0)
        engine._follower_spread_ewma[("okx", "DEADCOIN")] = dead_norm

        self.assertEqual(len(engine._follower_spread_ewma), 2)
        engine.sweep_timeouts(now)
        self.assertEqual(len(engine._follower_spread_ewma), 1)
        self.assertIn(("bybit", "BTC"), engine._follower_spread_ewma)
        self.assertNotIn(("okx", "DEADCOIN"), engine._follower_spread_ewma)

    def test_s2_regime_flash_dump_and_bear_drift_filters(self):
        """Verify S2 Market Regime Filter: VETO on FLASH_DUMP, $5k swept floor on BEAR_DRIFT."""
        now = time.time()
        fade_long_strat = FadeFakeoutStrategy(
            emit_callback=lambda sig: None,
            invert_to_long=True,
            max_follower_depth_usd=5000.0,
        )
        upbit_snap = make_snap("upbit", "ARB", [(1.000, 1000.0)], [(1.005, 1000.0)], now)
        f_snap = make_snap("bybit", "ARB", [(0.999, 1000.0)], [(1.000, 500.0)], now)

        # 1. FLASH_DUMP -> Hard Veto
        opened_dump = fade_long_strat.on_fakeout_detected(
            coin="ARB", trigger_type="depth_drain", swept_usd=8000.0,
            upbit_snap=upbit_snap, follower_snaps={"bybit": f_snap}, now=now,
            fade_reason="micro_sweep_exhaustion", market_regime="FLASH_DUMP"
        )
        self.assertEqual(len(opened_dump), 0)

        # 2. BEAR_DRIFT with swept_usd < 5000 -> Blocked
        opened_weak = fade_long_strat.on_fakeout_detected(
            coin="ARB", trigger_type="depth_drain", swept_usd=2500.0,
            upbit_snap=upbit_snap, follower_snaps={"bybit": f_snap}, now=now,
            fade_reason="micro_sweep_exhaustion", market_regime="BEAR_DRIFT"
        )
        self.assertEqual(len(opened_weak), 0)

        # 3. BEAR_DRIFT with swept_usd >= 5000 -> Allowed
        opened_strong = fade_long_strat.on_fakeout_detected(
            coin="ARB", trigger_type="depth_drain", swept_usd=6000.0,
            upbit_snap=upbit_snap, follower_snaps={"bybit": f_snap}, now=now,
            fade_reason="micro_sweep_exhaustion", market_regime="BEAR_DRIFT"
        )
        self.assertEqual(len(opened_strong), 1)
        self.assertEqual(opened_strong[0].side, "long")

    def test_s2_classic_fade_strong_pump_veto(self):
        """Verify S2 Classic Fade (Short) blocks entries during STRONG_PUMP."""
        now = time.time()
        fade_short_strat = FadeFakeoutStrategy(
            emit_callback=lambda sig: None,
            invert_to_long=False,
            max_follower_depth_usd=5000.0,
        )
        upbit_snap = make_snap("upbit", "OP", [(2.000, 1000.0)], [(2.010, 1000.0)], now)
        f_snap = make_snap("binance", "OP", [(1.995, 500.0)], [(2.000, 1000.0)], now)

        # STRONG_PUMP -> Blocked
        opened_pump = fade_short_strat.on_fakeout_detected(
            coin="OP", trigger_type="depth_drain", swept_usd=2000.0,
            upbit_snap=upbit_snap, follower_snaps={"binance": f_snap}, now=now,
            fade_reason="micro_sweep_exhaustion", market_regime="STRONG_PUMP"
        )
        self.assertEqual(len(opened_pump), 0)

        # NEUTRAL -> Allowed
        opened_neutral = fade_short_strat.on_fakeout_detected(
            coin="OP", trigger_type="depth_drain", swept_usd=2000.0,
            upbit_snap=upbit_snap, follower_snaps={"binance": f_snap}, now=now,
            fade_reason="micro_sweep_exhaustion", market_regime="NEUTRAL"
        )
        self.assertEqual(len(opened_neutral), 1)
        self.assertEqual(opened_neutral[0].side, "short")

    def test_s3_regime_flash_dump_and_directional_filters(self):
        """Verify S3 Market Regime: FLASH_DUMP veto, BEAR_DRIFT blocks buys, STRONG_PUMP blocks sells."""
        now = time.time()
        hunter = MakerVoidStrategy(
            emit_callback=lambda sig: None,
            hunter_mode=True,
            taker_fee_bps=2.0,
        )
        # Follower tight book (5 bps spread): bid 100.00, ask 100.05
        f_snap = make_snap("bybit", "SOL", [(100.00, 50.0)], [(100.05, 50.0)], now)

        # Upbit leading higher (101.00 -> +95 bps dislocation -> BUY opportunity)
        upbit_high = make_snap("upbit", "SOL", [(100.95, 50.0)], [(101.05, 50.0)], now)

        # Upbit leading lower (99.00 -> -100 bps dislocation -> SELL opportunity)
        upbit_low = make_snap("upbit", "SOL", [(98.95, 50.0)], [(99.05, 50.0)], now)

        # 1. FLASH_DUMP blocks both buy and sell
        v_dump_buy = hunter.check_void_opportunity("bybit", "SOL", f_snap, upbit_high, now, is_warmed_up=True, market_regime="FLASH_DUMP")
        v_dump_sell = hunter.check_void_opportunity("bybit", "SOL", f_snap, upbit_low, now, is_warmed_up=True, market_regime="FLASH_DUMP")
        self.assertIsNone(v_dump_buy)
        self.assertIsNone(v_dump_sell)

        # 2. BEAR_DRIFT blocks buy, but allows sell!
        v_bear_buy = hunter.check_void_opportunity("bybit", "SOL", f_snap, upbit_high, now, is_warmed_up=True, market_regime="BEAR_DRIFT")
        self.assertIsNone(v_bear_buy)

        v_bear_sell = hunter.check_void_opportunity("bybit", "SOL", f_snap, upbit_low, now, is_warmed_up=True, market_regime="BEAR_DRIFT")
        self.assertIsNotNone(v_bear_sell)
        self.assertEqual(v_bear_sell.side, "sell")

        # 3. STRONG_PUMP blocks sell, but allows buy!
        hunter._last_void_ts.clear()
        hunter._open_voids.clear()
        v_pump_sell = hunter.check_void_opportunity("bybit", "SOL", f_snap, upbit_low, now, is_warmed_up=True, market_regime="STRONG_PUMP")
        self.assertIsNone(v_pump_sell)

        v_pump_buy = hunter.check_void_opportunity("bybit", "SOL", f_snap, upbit_high, now, is_warmed_up=True, market_regime="STRONG_PUMP")
        self.assertIsNotNone(v_pump_buy)
        self.assertEqual(v_pump_buy.side, "buy")


if __name__ == "__main__":
    unittest.main()
