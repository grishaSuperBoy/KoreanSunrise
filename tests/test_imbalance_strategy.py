"""
tests/test_imbalance_strategy.py - Unit test suite for Strategy 5: Orderbook Imbalance Front-Runner.
"""
from __future__ import annotations

import unittest
import time
from typing import List, Dict, Any

from models import OrderBookDepth5
from strategies.archive import ActiveImbalancePosition, OrderbookImbalanceStrategy


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


class TestOrderbookImbalanceStrategy(unittest.TestCase):

    def setUp(self):
        self.emitted: List[Dict[str, Any]] = []
        self.counters: Dict[str, int] = {}
        self.strat = OrderbookImbalanceStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            min_order_usd=10.0,
            max_position_budget_usd=500.0,
            stealth_depth_fraction=0.15,
            min_upbit_imbalance=0.60,
            min_delta_imbalance_5s=0.35,
            max_follower_imbalance=0.25,
            target_pnl_bps=25.0,
            stop_loss_bps=18.0,
            roundtrip_fee_bps=10.0,
            max_hold_sec=15.0,
            alpha_decay_sec=8.0,
            cooldown_sec=20.0,
            max_active_positions=5,
            max_total_exposure_usd=2500.0,
            enabled=True,
            counters=self.counters,
        )

    def test_imbalance_entry_on_bid_stacking(self):
        now = time.time()
        coin = "SEI"

        # 5 seconds ago: Balanced Upbit book (imbalance ~ 0.0)
        t_past = now - 5.0
        past_upbit = make_snap("upbit", "SEI", [(1.0, 1000.0)], [(1.002, 1000.0)], t_past)
        self.strat.on_upbit_book_tick(coin, past_upbit, t_past)

        # Now: Aggressive bid stacking on Upbit ($9,000 bids vs $1,000 asks => imbalance = 0.80)
        # Delta imbalance = 0.80 - 0.0 = +0.80 >= 0.35 threshold
        cur_upbit = make_snap("upbit", "SEI", [(1.0, 9000.0)], [(1.002, 1000.0)], now)
        self.strat.on_upbit_book_tick(coin, cur_upbit, now)

        # Follower book is still calm/neutral (imbalance <= 0.20)
        f_snap = make_snap("binance", "SEI", [(0.999, 2000.0)], [(1.000, 2000.0)], now, "SEIUSDT")

        pos = self.strat.check_imbalance_opportunity(
            follower_ex="binance",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=cur_upbit,
            now=now,
            market_regime="NEUTRAL",
            recv_ts=now,
        )

        self.assertIsNotNone(pos)
        self.assertEqual(pos.coin, "SEI")
        self.assertEqual(pos.side, "long")
        self.assertGreaterEqual(pos.upbit_imbalance_at_open, 0.60)
        self.assertGreaterEqual(pos.delta_imbalance_5s, 0.35)
        self.assertEqual(self.counters.get("imbalance_triggered"), 1)

    def test_imbalance_rejects_during_flash_dump(self):
        now = time.time()
        coin = "DOGE"

        cur_upbit = make_snap("upbit", "DOGE", [(0.12, 10000.0)], [(0.1202, 1000.0)], now)
        self.strat.on_upbit_book_tick(coin, cur_upbit, now)
        f_snap = make_snap("bybit", "DOGE", [(0.119, 2000.0)], [(0.120, 2000.0)], now, "DOGEUSDT")

        pos = self.strat.check_imbalance_opportunity(
            follower_ex="bybit",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=cur_upbit,
            now=now,
            market_regime="FLASH_DUMP",  # Must reject during systemic flash dump
            recv_ts=now,
        )

        self.assertIsNone(pos)

    def test_imbalance_target_convergence(self):
        now = time.time()
        coin = "XRP"

        # Setup entry
        self.strat.on_upbit_book_tick(coin, make_snap("upbit", "XRP", [(1.5, 1000.0)], [(1.502, 1000.0)], now - 5.0), now - 5.0)
        cur_upbit = make_snap("upbit", "XRP", [(1.5, 9000.0)], [(1.502, 1000.0)], now)
        self.strat.on_upbit_book_tick(coin, cur_upbit, now)
        f_snap = make_snap("binance", "XRP", [(1.498, 3000.0)], [(1.500, 3000.0)], now, "XRPUSDT")

        pos = self.strat.check_imbalance_opportunity("binance", coin, f_snap, cur_upbit, now, recv_ts=now)
        self.assertIsNotNone(pos)
        self.assertEqual(pos.entry_px, 1.500)

        # Price sweeps upward: Best bid jumps from 1.498 to 1.507 (+46.7 bps raw move, +36.7 bps net)
        tick_snap = make_snap("binance", "XRP", [(1.507, 3000.0)], [(1.509, 3000.0)], now + 2.0, "XRPUSDT")
        closed_pos = self.strat.on_follower_tick("binance", coin, tick_snap, now + 2.0)

        self.assertIsNotNone(closed_pos)
        self.assertTrue(closed_pos.closed)
        self.assertEqual(closed_pos.close_reason, "target_converged")
        self.assertGreaterEqual(closed_pos.net_pnl_bps, 25.0)
        self.assertEqual(self.counters.get("imbalance_converged"), 1)


if __name__ == "__main__":
    unittest.main()
