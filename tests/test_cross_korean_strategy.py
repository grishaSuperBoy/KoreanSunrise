"""
tests/test_cross_korean_strategy.py - Unit test suite for Strategy 5: Cross-Korean Triangle (Bithumb Radar).
"""
from __future__ import annotations

import unittest
import time
from typing import List, Dict, Any

from models import OrderBookDepth5
from strategies.strategy_5_cross_korean import ActiveCrossKoreanPosition, CrossKoreanTriangleStrategy


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


class TestCrossKoreanTriangleStrategy(unittest.TestCase):

    def setUp(self):
        self.emitted: List[Dict[str, Any]] = []
        self.counters: Dict[str, int] = {}
        self.strat = CrossKoreanTriangleStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            min_order_usd=10.0,
            max_position_budget_usd=500.0,
            stealth_depth_fraction=0.15,
            min_bithumb_lead_bps=25.0,
            min_bithumb_buys_usd=50.0,
            target_pnl_bps=30.0,
            stop_loss_bps=20.0,
            roundtrip_fee_bps=10.0,
            max_hold_sec=20.0,
            alpha_decay_sec=10.0,
            cooldown_sec=20.0,
            max_active_positions=5,
            max_total_exposure_usd=2500.0,
            enabled=True,
            counters=self.counters,
        )

    def test_triangle_entry_on_bithumb_lead(self):
        now = time.time()
        coin = "CTC"

        # Bithumb mid = 1.05, Upbit mid = 1.02 (+29.4 bps lead over Upbit)
        # Follower (Binance) mid = 1.01 (lag = +39.6 bps)
        bithumb_snap = make_snap("bithumb", "CTC", [(1.049, 5000.0)], [(1.051, 5000.0)], now)
        upbit_snap = make_snap("upbit", "CTC", [(1.019, 5000.0)], [(1.021, 5000.0)], now)
        f_snap = make_snap("binance", "CTC", [(1.009, 3000.0)], [(1.010, 3000.0)], now, "CTCUSDT")

        pos = self.strat.check_triangle_opportunity(
            follower_ex="binance",
            coin=coin,
            bithumb_snap=bithumb_snap,
            upbit_snap=upbit_snap,
            follower_snap=f_snap,
            now=now,
            market_regime="NEUTRAL",
            recent_bithumb_buys_usd=500.0,
            recv_ts=now,
        )

        self.assertIsNotNone(pos)
        self.assertEqual(pos.coin, "CTC")
        self.assertEqual(pos.side, "long")
        self.assertGreaterEqual(pos.bithumb_upbit_spread_bps, 25.0)
        self.assertEqual(self.counters.get("triangle_triggered"), 1)

    def test_triangle_target_convergence(self):
        now = time.time()
        coin = "CTC"

        bithumb_snap = make_snap("bithumb", "CTC", [(1.05, 5000.0)], [(1.051, 5000.0)], now)
        upbit_snap = make_snap("upbit", "CTC", [(1.02, 5000.0)], [(1.021, 5000.0)], now)
        f_snap = make_snap("binance", "CTC", [(1.009, 3000.0)], [(1.010, 3000.0)], now, "CTCUSDT")

        pos = self.strat.check_triangle_opportunity("binance", coin, bithumb_snap, upbit_snap, f_snap, now, recent_bithumb_buys_usd=500.0, recv_ts=now)
        self.assertIsNotNone(pos)

        # Follower catches up to Bithumb: Best bid reaches 1.015 (+49.5 bps raw move, +39.5 bps net)
        tick_snap = make_snap("binance", "CTC", [(1.015, 3000.0)], [(1.016, 3000.0)], now + 2.0, "CTCUSDT")
        closed = self.strat.on_follower_tick("binance", coin, tick_snap, now + 2.0)

        self.assertIsNotNone(closed)
        self.assertTrue(closed.closed)
        self.assertEqual(closed.close_reason, "target_converged")
        self.assertGreaterEqual(closed.net_pnl_bps, 30.0)
        self.assertEqual(self.counters.get("triangle_converged"), 1)
        self.assertEqual(len(self.emitted), 1)
        self.assertEqual(self.emitted[0]["strategy_id"], 5)
        self.assertEqual(self.emitted[0]["strategy"], "cross_korean_triangle")

    def test_triangle_rejects_ghost_quotes_without_trades(self):
        now = time.time()
        coin = "CTC"

        bithumb_snap = make_snap("bithumb", "CTC", [(1.05, 5000.0)], [(1.051, 5000.0)], now)
        upbit_snap = make_snap("upbit", "CTC", [(1.02, 5000.0)], [(1.021, 5000.0)], now)
        f_snap = make_snap("binance", "CTC", [(1.009, 3000.0)], [(1.010, 3000.0)], now, "CTCUSDT")

        # Zero recent buys on Bithumb (ghost quote) -> must be rejected
        pos = self.strat.check_triangle_opportunity(
            "binance", coin, bithumb_snap, upbit_snap, f_snap, now,
            recent_bithumb_buys_usd=0.0, recv_ts=now
        )
        self.assertIsNone(pos)

    def test_triangle_rejects_major_coins(self):
        now = time.time()
        coin = "BTC"  # Excluded major

        bithumb_snap = make_snap("bithumb", "BTC", [(85000.0, 50000.0)], [(85050.0, 50000.0)], now)
        upbit_snap = make_snap("upbit", "BTC", [(84000.0, 50000.0)], [(84050.0, 50000.0)], now)
        f_snap = make_snap("binance", "BTC", [(83900.0, 50000.0)], [(83950.0, 50000.0)], now, "BTCUSDT")

        # Even with lead and volume, majors are strictly excluded
        pos = self.strat.check_triangle_opportunity(
            "binance", coin, bithumb_snap, upbit_snap, f_snap, now,
            recent_bithumb_buys_usd=50000.0, recv_ts=now
        )
        self.assertIsNone(pos)

    def test_triangle_rejects_low_volume_share(self):
        now = time.time()
        coin = "CTC"

        bithumb_snap = make_snap("bithumb", "CTC", [(1.05, 5000.0)], [(1.051, 5000.0)], now)
        upbit_snap = make_snap("upbit", "CTC", [(1.02, 5000.0)], [(1.021, 5000.0)], now)
        f_snap = make_snap("binance", "CTC", [(1.009, 3000.0)], [(1.010, 3000.0)], now, "CTCUSDT")

        # Bithumb volume share is only 10% (< 30% min) -> must be rejected
        pos = self.strat.check_triangle_opportunity(
            "binance", coin, bithumb_snap, upbit_snap, f_snap, now,
            recent_bithumb_buys_usd=500.0, recv_ts=now, bithumb_volume_share=0.10
        )
        self.assertIsNone(pos)

    def test_triangle_timeout_penalty(self):
        now = time.time()
        coin = "CTC"

        bithumb_snap = make_snap("bithumb", "CTC", [(1.05, 5000.0)], [(1.051, 5000.0)], now)
        upbit_snap = make_snap("upbit", "CTC", [(1.02, 5000.0)], [(1.021, 5000.0)], now)
        f_snap = make_snap("binance", "CTC", [(1.009, 3000.0)], [(1.010, 3000.0)], now, "CTCUSDT")

        pos = self.strat.check_triangle_opportunity(
            "binance", coin, bithumb_snap, upbit_snap, f_snap, now,
            recent_bithumb_buys_usd=500.0, recv_ts=now
        )
        self.assertIsNotNone(pos)

        # Timeout after 25s (> 20s max_hold_sec)
        self.strat.sweep_timeouts(now + 25.0)
        self.assertTrue(pos.closed)
        self.assertEqual(pos.close_reason, "timeout")
        # Realistic penalty: spread + fee + 10 bps adverse slippage
        expected_penalty = pos.spread_open_bps + pos.roundtrip_fee_bps + 10.0
        self.assertAlmostEqual(pos.net_pnl_bps, -expected_penalty, delta=0.5)

    def test_triangle_upbit_transmission_confirmation(self):
        strat = CrossKoreanTriangleStrategy(
            emit_callback=lambda sig: None,
            min_bithumb_buys_usd=50.0,
            min_upbit_confirm_bps=3.0, # Requires Upbit to show +3 bps transmission momentum
            enabled=True,
        )
        now = time.time()
        coin = "CTC"

        bithumb_snap = make_snap("bithumb", "CTC", [(1.05, 5000.0)], [(1.051, 5000.0)], now)
        upbit_snap = make_snap("upbit", "CTC", [(1.02, 5000.0)], [(1.021, 5000.0)], now)
        f_snap = make_snap("binance", "CTC", [(1.009, 3000.0)], [(1.010, 3000.0)], now, "CTCUSDT")

        # 1. Upbit is flat (0 bps ret, 0 buys) -> Rejected!
        pos_flat = strat.check_triangle_opportunity(
            "binance", coin, bithumb_snap, upbit_snap, f_snap, now,
            recent_bithumb_buys_usd=500.0, recv_ts=now,
            upbit_ret_2s_bps=0.0, recent_upbit_buys_usd=0.0
        )
        self.assertIsNone(pos_flat)

        # 2. Upbit transmission confirmed (+5.0 bps ret) -> Accepted!
        pos_confirmed = strat.check_triangle_opportunity(
            "binance", coin, bithumb_snap, upbit_snap, f_snap, now,
            recent_bithumb_buys_usd=500.0, recv_ts=now,
            upbit_ret_2s_bps=5.0, recent_upbit_buys_usd=0.0
        )
        self.assertIsNotNone(pos_confirmed)


if __name__ == "__main__":
    unittest.main()
