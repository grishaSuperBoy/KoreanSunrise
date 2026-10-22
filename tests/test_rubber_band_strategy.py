"""
tests/test_rubber_band_strategy.py - Unit test suite for Strategy 4: Rubber Band Sniper.
"""
from __future__ import annotations

import unittest
import time
from typing import List, Dict, Any

from models import OrderBookDepth5
from pipeline.market_regime import MarketRegime, MarketRegimeSnapshot
from strategies import ActiveRubberBandPosition, RubberBandSniperStrategy


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


class TestRubberBandStrategy(unittest.TestCase):

    def setUp(self):
        self.emitted: List[Dict[str, Any]] = []
        self.counters: Dict[str, int] = {}
        self.strat = RubberBandSniperStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            min_order_usd=10.0,
            max_position_budget_usd=500.0,
            stealth_depth_fraction=0.15,
            min_dislocation_bps=100.0,
            target_rebound_bps=35.0,
            stop_loss_bps=25.0,
            roundtrip_fee_bps=10.0,
            max_hold_sec=30.0,
            alpha_decay_sec=15.0,
            cooldown_sec=20.0,
            max_active_positions=5,
            max_total_exposure_usd=2500.0,
            hunter_mode=True,
            counters=self.counters,
        )

    def test_rubber_band_entry_post_dump(self):
        now = time.time()
        coin = "SEI"

        # Upbit mid = 1.05 (in USD), Follower mid = 1.00 (dislocation = +500.0 bps)
        upbit_snap = make_snap("upbit", "SEI", [(1.049, 5000.0)], [(1.051, 5000.0)], now)
        f_snap = make_snap(
            "binance", "SEI",
            [(0.9995, 3000.0), (0.9990, 2000.0)],  # Top bids = $5,000
            [(1.0005, 2000.0), (1.0010, 1000.0)],  # Top asks = $3,000
            now, "SEIUSDT"
        )

        # Notify that a dump occurred 20 seconds ago
        self.strat.on_regime_update("FLASH_DUMP", now - 20.0)

        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=35.0,
            breadth_pct_300s=20.0,
            btc_ret_30s_bps=5.0,
            btc_ret_300s_bps=-40.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.6,
            coins_tracked=50,
            timestamp=now,
        )

        pos = self.strat.check_rubber_band_opportunity(
            follower_ex="binance",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            regime_snapshot=reg_snap,
            recv_ts=now,
        )

        self.assertIsNotNone(pos)
        self.assertEqual(pos.coin, "SEI")
        self.assertEqual(pos.side, "long")
        self.assertEqual(pos.trigger_reason, "post_dump_snapback")
        self.assertAlmostEqual(pos.dislocation_bps_open, 500.0, places=1)
        self.assertEqual(self.counters.get("rubber_band_triggered"), 1)

    def test_rubber_band_entry_during_exhausting_dump(self):
        now = time.time()
        coin = "DOGE"

        upbit_snap = make_snap("upbit", "DOGE", [(0.122, 10000.0)], [(0.123, 10000.0)], now)
        f_snap = make_snap(
            "bybit", "DOGE",
            [(0.1200, 8000.0)],
            [(0.1202, 3000.0)],  # Bids heavily outweigh asks (8000 vs 3000)
            now, "DOGEUSDT"
        )

        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.FLASH_DUMP,
            breadth_pct_60s=15.0,
            breadth_pct_300s=10.0,
            btc_ret_30s_bps=-3.0,  # BTC 30s momentum has flattened >= -8.0 bps!
            btc_ret_300s_bps=-75.0,
            btc_dump_active=True,
            korean_buy_ratio_60s=0.5,
            coins_tracked=50,
            timestamp=now,
        )

        pos = self.strat.check_rubber_band_opportunity(
            follower_ex="bybit",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            regime_snapshot=reg_snap,
            recv_ts=now,
        )

        self.assertIsNotNone(pos)
        self.assertEqual(pos.trigger_reason, "dump_exhaustion_absorption")
        self.assertEqual(pos.side, "long")

    def test_rubber_band_rejects_active_knife_fall(self):
        now = time.time()
        coin = "XRP"

        upbit_snap = make_snap("upbit", "XRP", [(1.50, 10000.0)], [(1.51, 10000.0)], now)
        f_snap = make_snap("okx", "XRP", [(1.45, 2000.0)], [(1.46, 5000.0)], now, "XRPUSDT")

        # Plunging BTC crash leg (-35.0 bps over 30s)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.FLASH_DUMP,
            breadth_pct_60s=5.0,
            breadth_pct_300s=5.0,
            btc_ret_30s_bps=-35.0,
            btc_ret_300s_bps=-120.0,
            btc_dump_active=True,
            korean_buy_ratio_60s=0.2,
            coins_tracked=50,
            timestamp=now,
        )

        pos = self.strat.check_rubber_band_opportunity(
            follower_ex="okx",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            regime_snapshot=reg_snap,
            recv_ts=now,
        )

        # Must reject catching the falling knife
        self.assertIsNone(pos)

    def test_rubber_band_rejects_low_dislocation(self):
        now = time.time()
        coin = "BTC"

        # Dislocation is only ~20 bps (< 100 bps)
        upbit_snap = make_snap("upbit", "BTC", [(80010.0, 50000.0)], [(80020.0, 50000.0)], now)
        f_snap = make_snap("binance", "BTC", [(79990.0, 50000.0)], [(80000.0, 50000.0)], now, "BTCUSDT")

        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=45.0,
            breadth_pct_300s=40.0,
            btc_ret_30s_bps=0.0,
            btc_ret_300s_bps=0.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.5,
            coins_tracked=50,
            timestamp=now,
        )

        pos = self.strat.check_rubber_band_opportunity(
            follower_ex="binance",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            regime_snapshot=reg_snap,
            recv_ts=now,
        )

        self.assertIsNone(pos)

    def test_rubber_band_target_rebound_convergence(self):
        now = time.time()
        coin = "LINK"

        upbit_snap = make_snap("upbit", "LINK", [(15.5, 5000.0)], [(15.6, 5000.0)], now)
        f_snap = make_snap("binance", "LINK", [(15.0, 6000.0)], [(15.01, 3000.0)], now, "LINKUSDT")

        self.strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=30.0,
            breadth_pct_300s=20.0,
            btc_ret_30s_bps=2.0,
            btc_ret_300s_bps=-30.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.5,
            coins_tracked=50,
            timestamp=now,
        )

        pos = self.strat.check_rubber_band_opportunity(
            follower_ex="binance",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            regime_snapshot=reg_snap,
            recv_ts=now,
        )
        self.assertIsNotNone(pos)
        self.assertEqual(pos.entry_px, 15.01)

        # Price rebounds: Best bid jumps from 15.00 to 15.10 (+60 bps raw move)
        # Net PnL = +60 bps - 10 bps fee = +50 bps >= target 35 bps
        tick_snap = make_snap("binance", "LINK", [(15.10, 4000.0)], [(15.11, 4000.0)], now + 2.0, "LINKUSDT")
        closed_pos = self.strat.on_follower_tick("binance", "LINK", tick_snap, now + 2.0)

        self.assertIsNotNone(closed_pos)
        self.assertTrue(closed_pos.closed)
        self.assertEqual(closed_pos.close_reason, "target_converged")
        self.assertGreaterEqual(closed_pos.net_pnl_bps, 35.0)
        self.assertEqual(self.counters.get("rubber_band_converged"), 1)
        self.assertEqual(len(self.emitted), 1)

    def test_rubber_band_stop_loss(self):
        now = time.time()
        coin = "ADA"

        upbit_snap = make_snap("upbit", "ADA", [(0.55, 5000.0)], [(0.56, 5000.0)], now)
        f_snap = make_snap("bybit", "ADA", [(0.50, 6000.0)], [(0.501, 3000.0)], now, "ADAUSDT")

        self.strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=30.0,
            breadth_pct_300s=20.0,
            btc_ret_30s_bps=0.0,
            btc_ret_300s_bps=-30.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.5,
            coins_tracked=50,
            timestamp=now,
        )

        pos = self.strat.check_rubber_band_opportunity(
            follower_ex="bybit",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            regime_snapshot=reg_snap,
            recv_ts=now,
        )
        self.assertIsNotNone(pos)

        # Adverse move: Best bid falls to 0.4980 (approx -60 bps raw move)
        # Net PnL <= -25 bps stop loss threshold
        tick_snap = make_snap("bybit", "ADA", [(0.4980, 4000.0)], [(0.4985, 4000.0)], now + 2.0, "ADAUSDT")
        closed_pos = self.strat.on_follower_tick("bybit", "ADA", tick_snap, now + 2.0)

        self.assertIsNotNone(closed_pos)
        self.assertTrue(closed_pos.closed)
        self.assertEqual(closed_pos.close_reason, "stop_loss")
        self.assertLessEqual(closed_pos.net_pnl_bps, -25.0)
        self.assertEqual(self.counters.get("rubber_band_stopped"), 1)

    def test_rubber_band_decay_and_timeout(self):
        now = time.time()
        coin = "NEAR"

        upbit_snap = make_snap("upbit", "NEAR", [(5.5, 5000.0)], [(5.6, 5000.0)], now)
        f_snap = make_snap("okx", "NEAR", [(5.0, 6000.0)], [(5.01, 3000.0)], now, "NEARUSDT")

        self.strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=30.0,
            breadth_pct_300s=20.0,
            btc_ret_30s_bps=0.0,
            btc_ret_300s_bps=-30.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.5,
            coins_tracked=50,
            timestamp=now,
        )

        pos = self.strat.check_rubber_band_opportunity(
            follower_ex="okx",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            regime_snapshot=reg_snap,
            recv_ts=now,
        )
        self.assertIsNotNone(pos)

        # Tick at dt = 16.0s (> alpha_decay_sec 15s) with flat price (bid = 5.01, fee net PnL = -10 bps <= 0)
        flat_snap = make_snap("okx", "NEAR", [(5.01, 3000.0)], [(5.02, 3000.0)], now + 16.0, "NEARUSDT")
        closed_pos = self.strat.on_follower_tick("okx", "NEAR", flat_snap, now + 16.0)

        self.assertIsNotNone(closed_pos)
        self.assertEqual(closed_pos.close_reason, "stop_decay")

    def test_rubber_band_cooldown_and_concurrency(self):
        now = time.time()
        coin = "SUI"

        upbit_snap = make_snap("upbit", "SUI", [(2.5, 5000.0)], [(2.6, 5000.0)], now)
        f_snap = make_snap("binance", "SUI", [(2.0, 6000.0)], [(2.002, 3000.0)], now, "SUIUSDT")

        self.strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=30.0,
            breadth_pct_300s=20.0,
            btc_ret_30s_bps=0.0,
            btc_ret_300s_bps=-30.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.5,
            coins_tracked=50,
            timestamp=now,
        )

        pos1 = self.strat.check_rubber_band_opportunity(
            follower_ex="binance",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now,
            regime_snapshot=reg_snap,
            recv_ts=now,
        )
        self.assertIsNotNone(pos1)

        # Duplicate check on same pair while position open -> returns None
        pos2 = self.strat.check_rubber_band_opportunity(
            follower_ex="binance",
            coin=coin,
            follower_snap=f_snap,
            upbit_snap=upbit_snap,
            now=now + 1.0,
            regime_snapshot=reg_snap,
            recv_ts=now + 1.0,
        )
        self.assertIsNone(pos2)

    def test_predatory_maker_limit_bid_placement_and_fill(self):
        # Maker mode (hunter_mode=False default)
        maker_strat = RubberBandSniperStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            min_order_usd=10.0,
            max_position_budget_usd=500.0,
            stealth_depth_fraction=0.15,
            min_dislocation_bps=100.0,
            min_z_score=3.0,
            maker_discount_bps=25.0, # -25 bps below best_bid
            maker_ttl_sec=2.5,
            target_rebound_bps=35.0,
            stop_loss_bps=25.0,
            maker_rebate_bps=0.0,
            taker_fee_bps=5.0,
            hunter_mode=False,
            counters=self.counters,
        )

        now = time.time()
        coin = "SOL"

        # Upbit mid = 155.0, Follower best_bid = 150.0, best_ask = 150.10 (+333 bps dislocation, Z=9.5σ)
        upbit_snap = make_snap("upbit", "SOL", [(154.9, 5000.0)], [(155.1, 5000.0)], now)
        f_snap = make_snap("binance", "SOL", [(150.0, 6000.0)], [(150.10, 3000.0)], now, "SOLUSDT")

        maker_strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=35.0,
            breadth_pct_300s=25.0,
            btc_ret_30s_bps=5.0,
            btc_ret_300s_bps=-30.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.6,
            coins_tracked=50,
            timestamp=now,
        )

        pos = maker_strat.check_rubber_band_opportunity(
            "binance", coin, f_snap, upbit_snap, now, reg_snap, now, pair_sigma_bps=35.0
        )
        self.assertIsNotNone(pos)
        self.assertFalse(pos.maker_filled) # Order is RESTING
        # Expected limit price: 150.0 * (1 - 0.0025) = 149.625
        self.assertAlmostEqual(pos.target_limit_px, 149.625, places=3)

        # 1. Price is still higher (149.80) -> Order remains resting
        tick1 = make_snap("binance", "SOL", [(149.80, 5000.0)], [(149.90, 5000.0)], now + 0.5, "SOLUSDT")
        res1 = maker_strat.on_follower_tick("binance", "SOL", tick1, now + 0.5)
        self.assertIsNone(res1)
        self.assertFalse(pos.maker_filled)

        # 2. Liquidation wick flushes down to 149.60 -> FILLS predatory maker bid!
        tick2 = make_snap("binance", "SOL", [(149.60, 5000.0)], [(149.65, 5000.0)], now + 1.0, "SOLUSDT")
        res2 = maker_strat.on_follower_tick("binance", "SOL", tick2, now + 1.0)
        self.assertIsNone(res2) # Not closed yet, just filled!
        self.assertTrue(pos.maker_filled)
        self.assertAlmostEqual(pos.fill_px, 149.625, places=3)

        # 3. Violent snap-back: Best bid bounces to 150.30 (+45.1 bps raw)
        # Net PnL = 45.1 - 5.0 fee = +40.1 bps >= +35.0 bps target!
        tick3 = make_snap("binance", "SOL", [(150.30, 5000.0)], [(150.35, 5000.0)], now + 2.0, "SOLUSDT")
        closed = maker_strat.on_follower_tick("binance", "SOL", tick3, now + 2.0)
        self.assertIsNotNone(closed)
        self.assertTrue(closed.closed)
        self.assertEqual(closed.close_reason, "target_converged")
        self.assertGreaterEqual(closed.net_pnl_bps, 35.0)

    def test_predatory_maker_deep_wick_fill_not_rejected(self):
        counters = {}
        maker_strat = RubberBandSniperStrategy(
            emit_callback=lambda sig: None,
            maker_discount_bps=25.0,
            maker_ttl_sec=2.5,
            hunter_mode=False,
            counters=counters,
        )

        now = time.time()
        coin = "SOL"

        upbit_snap = make_snap("upbit", "SOL", [(155.0, 5000.0)], [(155.1, 5000.0)], now)
        f_snap = make_snap("binance", "SOL", [(150.0, 6000.0)], [(150.10, 3000.0)], now, "SOLUSDT")

        maker_strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=35.0,
            breadth_pct_300s=25.0,
            btc_ret_30s_bps=5.0,
            btc_ret_300s_bps=-30.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.6,
            coins_tracked=50,
            timestamp=now,
        )

        pos = maker_strat.check_rubber_band_opportunity(
            "binance", coin, f_snap, upbit_snap, now, reg_snap, now, pair_sigma_bps=35.0
        )
        self.assertIsNotNone(pos)
        self.assertFalse(pos.maker_filled)
        self.assertAlmostEqual(pos.target_limit_px, 149.625, places=3)

        # Deep liquidation wick: Ask plunges below our resting bid (149.50 <= 149.625)
        # This must fill the resting order (trade-through), NOT treat it as post_only_rejected!
        tick_deep = make_snap("binance", "SOL", [(149.40, 5000.0)], [(149.50, 5000.0)], now + 0.8, "SOLUSDT")
        res = maker_strat.on_follower_tick("binance", "SOL", tick_deep, now + 0.8)
        self.assertIsNone(res)  # Position remains open, now filled and actively managed
        self.assertTrue(pos.maker_filled)
        self.assertAlmostEqual(pos.fill_px, 149.625, places=3)
        self.assertEqual(counters.get("rubber_band_filled"), 1)
        self.assertEqual(counters.get("rubber_band_unfilled", 0), 0)

    def test_predatory_maker_unfilled_timeout_zero_loss(self):
        maker_strat = RubberBandSniperStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            maker_ttl_sec=2.5,
            hunter_mode=False,
            counters=self.counters,
        )

        now = time.time()
        coin = "SOL"

        upbit_snap = make_snap("upbit", "SOL", [(155.0, 5000.0)], [(155.1, 5000.0)], now)
        f_snap = make_snap("binance", "SOL", [(150.0, 6000.0)], [(150.10, 3000.0)], now, "SOLUSDT")

        maker_strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=35.0,
            breadth_pct_300s=25.0,
            btc_ret_30s_bps=5.0,
            btc_ret_300s_bps=-30.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.6,
            coins_tracked=50,
            timestamp=now,
        )

        pos = maker_strat.check_rubber_band_opportunity("binance", coin, f_snap, upbit_snap, now, reg_snap, now)
        self.assertIsNotNone(pos)
        self.assertFalse(pos.maker_filled)

        # 3.0s elapsed without fill (> 2.5s TTL) -> Cancels cleanly at 0 loss and 0 fee
        tick = make_snap("binance", "SOL", [(150.05, 5000.0)], [(150.15, 5000.0)], now + 3.0, "SOLUSDT")
        closed = maker_strat.on_follower_tick("binance", "SOL", tick, now + 3.0)
        self.assertIsNotNone(closed)
        self.assertEqual(closed.close_reason, "unfilled_timeout")
        self.assertEqual(closed.net_pnl_bps, 0.0) # 0.0 bps loss!

    def test_predatory_maker_anchor_collapse_cancellation(self):
        maker_strat = RubberBandSniperStrategy(
            emit_callback=lambda sig: self.emitted.append(sig),
            maker_ttl_sec=2.5,
            hunter_mode=False,
            counters=self.counters,
        )

        now = time.time()
        coin = "SOL"

        upbit_snap = make_snap("upbit", "SOL", [(155.0, 5000.0)], [(155.1, 5000.0)], now)
        f_snap = make_snap("binance", "SOL", [(150.0, 6000.0)], [(150.10, 3000.0)], now, "SOLUSDT")

        maker_strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL,
            breadth_pct_60s=35.0,
            breadth_pct_300s=25.0,
            btc_ret_30s_bps=5.0,
            btc_ret_300s_bps=-30.0,
            btc_dump_active=False,
            korean_buy_ratio_60s=0.6,
            coins_tracked=50,
            timestamp=now,
        )

        pos = maker_strat.check_rubber_band_opportunity("binance", coin, f_snap, upbit_snap, now, reg_snap, now)
        self.assertIsNotNone(pos)

        # While resting, Upbit crashes from 155.0 to 154.0 (> 60 bps drop) -> Abort resting order
        tick = make_snap("binance", "SOL", [(149.80, 5000.0)], [(149.90, 5000.0)], now + 0.5, "SOLUSDT")
        closed = maker_strat.on_follower_tick("binance", "SOL", tick, now + 0.5, cur_upbit_mid=154.0)
        self.assertIsNotNone(closed)
        self.assertEqual(closed.close_reason, "upbit_anchor_collapsed_resting")
        self.assertEqual(closed.net_pnl_bps, 0.0)

    def test_unfilled_timeout_single_increment_only(self):
        counters = {}
        maker_strat = RubberBandSniperStrategy(
            emit_callback=lambda sig: None,
            maker_ttl_sec=1.0,
            hunter_mode=False,
            counters=counters,
        )
        now = time.time()
        coin = "SOL"

        upbit_snap = make_snap("upbit", "SOL", [(155.0, 5000.0)], [(155.1, 5000.0)], now)
        f_snap = make_snap("binance", "SOL", [(150.0, 6000.0)], [(150.10, 3000.0)], now, "SOLUSDT")

        maker_strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL, breadth_pct_60s=35.0, breadth_pct_300s=25.0,
            btc_ret_30s_bps=5.0, btc_ret_300s_bps=-30.0, btc_dump_active=False,
            korean_buy_ratio_60s=0.6, coins_tracked=50, timestamp=now,
        )

        maker_strat.check_rubber_band_opportunity("binance", coin, f_snap, upbit_snap, now, reg_snap, now)
        # Timeout after 1.5s
        tick = make_snap("binance", "SOL", [(150.05, 5000.0)], [(150.15, 5000.0)], now + 1.5, "SOLUSDT")
        maker_strat.on_follower_tick("binance", "SOL", tick, now + 1.5)

        # Counter must be EXACTLY 1, not 2!
        self.assertEqual(counters.get("rubber_band_unfilled"), 1)

    def test_anchor_collapse_profitable_counts_as_converged(self):
        counters = {}
        maker_strat = RubberBandSniperStrategy(
            emit_callback=lambda sig: None,
            hunter_mode=True,  # Instant fill at best_ask
            counters=counters,
        )
        now = time.time()
        coin = "SOL"

        # Entered at 150.10
        upbit_snap = make_snap("upbit", "SOL", [(155.0, 5000.0)], [(155.1, 5000.0)], now)
        f_snap = make_snap("binance", "SOL", [(150.0, 6000.0)], [(150.10, 3000.0)], now, "SOLUSDT")

        maker_strat.on_regime_update("FLASH_DUMP", now - 10.0)
        reg_snap = MarketRegimeSnapshot(
            regime=MarketRegime.NEUTRAL, breadth_pct_60s=35.0, breadth_pct_300s=25.0,
            btc_ret_30s_bps=5.0, btc_ret_300s_bps=-30.0, btc_dump_active=False,
            korean_buy_ratio_60s=0.6, coins_tracked=50, timestamp=now,
        )
        pos = maker_strat.check_rubber_band_opportunity("binance", coin, f_snap, upbit_snap, now, reg_snap, now)
        self.assertIsNotNone(pos)

        # Price rebounds on follower to best_bid = 150.50 (+26.6 bps gross, +16.6 bps net)
        # But Upbit collapsed to 150.40 (below follower best_bid) -> Trigger anchor break
        tick = make_snap("binance", "SOL", [(150.50, 5000.0)], [(150.55, 5000.0)], now + 2.0, "SOLUSDT")
        closed = maker_strat.on_follower_tick("binance", "SOL", tick, now + 2.0, cur_upbit_mid=150.40)

        self.assertIsNotNone(closed)
        self.assertEqual(closed.close_reason, "converged_on_anchor_break")
        self.assertGreater(closed.net_pnl_bps, 0.0)
        # Must be counted as converged, NOT stopped!
        self.assertEqual(counters.get("rubber_band_converged"), 1)
        self.assertEqual(counters.get("rubber_band_stopped", 0), 0)


if __name__ == "__main__":
    unittest.main()
