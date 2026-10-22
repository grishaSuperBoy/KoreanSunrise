"""
tests/test_beta_neutral_strategy.py - Unit test suite for Strategy 6: Beta-Neutral Pairs Trading.
"""
from __future__ import annotations

import unittest
import time
from typing import List, Dict, Any

from models import OrderBookDepth5
from strategies.archive import ActiveBetaNeutralPair, BetaNeutralStrategy


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


class TestBetaNeutralStrategy(unittest.TestCase):

    def setUp(self):
        self.emitted: List[Dict[str, Any]] = []
        self.counters: Dict[str, int] = {}
        self.strat = BetaNeutralStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            min_order_usd=10.0,
            max_position_budget_usd=500.0,
            stealth_depth_fraction=0.15,
            default_beta=1.3,
            min_edge_bps=35.0,
            target_pnl_bps=30.0,
            stop_loss_bps=20.0,
            roundtrip_fee_bps=12.0,
            max_hold_sec=45.0,
            alpha_decay_sec=20.0,
            cooldown_sec=20.0,
            max_active_pairs=5,
            max_total_exposure_usd=3000.0,
            enabled=True,
            counters=self.counters,
        )

    def test_beta_neutral_entry(self):
        now = time.time()
        coin = "SEI"

        upbit_snap = make_snap("upbit", "SEI", [(1.05, 5000.0)], [(1.06, 5000.0)], now)
        f_snap = make_snap("binance", "SEI", [(0.999, 4000.0)], [(1.000, 4000.0)], now, "SEIUSDT")
        btc_snap = make_snap("binance", "BTC", [(80000.0, 50000.0)], [(80010.0, 50000.0)], now, "BTCUSDT")

        pair = self.strat.check_pair_opportunity(
            follower_ex="binance",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            btc_snap=btc_snap,
            now=now,
            beta=1.3,
            recv_ts=now,
        )

        self.assertIsNotNone(pair)
        self.assertEqual(pair.coin, "SEI")
        self.assertEqual(pair.beta, 1.3)
        self.assertEqual(pair.alt_entry_px, 1.000)
        self.assertEqual(pair.btc_entry_px, 80000.0)
        self.assertAlmostEqual(pair.btc_size_usd, pair.alt_size_usd * 1.3, places=2)
        self.assertEqual(self.counters.get("beta_neutral_triggered"), 1)

    def test_beta_neutral_hedge_success_during_btc_dump(self):
        now = time.time()
        coin = "SEI"

        upbit_snap = make_snap("upbit", "SEI", [(1.05, 5000.0)], [(1.06, 5000.0)], now)
        f_snap = make_snap("binance", "SEI", [(0.999, 4000.0)], [(1.000, 4000.0)], now, "SEIUSDT")
        btc_snap = make_snap("binance", "BTC", [(80000.0, 50000.0)], [(80010.0, 50000.0)], now, "BTCUSDT")

        pair = self.strat.check_pair_opportunity("binance", coin, f_snap, upbit_snap, btc_snap, now, beta=1.3, recv_ts=now)
        self.assertIsNotNone(pair)

        # BTC dumps from 80,000 to 79,200 (-100.0 bps on BTC) -> Short BTC leg gains +100.0 bps
        # Altcoin holds flat or gains slightly: 1.000 -> 1.003 (+30.0 bps)
        # Weighted return: 0.435 * 30 + 0.565 * 100 - 12 fee = 13.05 + 56.5 - 12 = +57.5 bps >= 30 target!
        alt_tick = make_snap("binance", "SEI", [(1.003, 4000.0)], [(1.004, 4000.0)], now + 3.0, "SEIUSDT")
        btc_tick = make_snap("binance", "BTC", [(79190.0, 50000.0)], [(79200.0, 50000.0)], now + 3.0, "BTCUSDT")

        closed = self.strat.on_tick_update("binance", coin, alt_tick, btc_tick, now + 3.0)
        self.assertIsNotNone(closed)
        self.assertTrue(closed.closed)
        self.assertEqual(closed.close_reason, "target_converged")
        self.assertGreaterEqual(closed.net_pnl_bps, 30.0)
        self.assertEqual(self.counters.get("beta_neutral_converged"), 1)


if __name__ == "__main__":
    unittest.main()
