"""
tests/test_market_regime.py — Comprehensive Unit Tests for Macro Regime & Breadth Engine.
"""
import time
import unittest

from pipeline.market_regime import MarketRegime, MarketRegimeDetector, MarketRegimeSnapshot


class TestMarketRegimeDetector(unittest.TestCase):
    def setUp(self):
        self.detector = MarketRegimeDetector(
            btc_dump_30s_bps=-20.0,
            btc_dump_300s_bps=-45.0,
            btc_pump_300s_bps=30.0,
            dump_cooldown_sec=60.0,
            breadth_dump_threshold=25.0,
            breadth_pump_threshold=70.0,
            min_coins_for_breadth=5,  # Lowered for unit test convenience
            cache_ttl_sec=0.0,        # No caching during unit tests
        )

    def test_warmup_neutral_state(self):
        now = 1000.0
        snap = self.detector.get_regime(now)
        self.assertEqual(snap.regime, MarketRegime.NEUTRAL)
        self.assertEqual(snap.breadth_pct_300s, 50.0)
        self.assertEqual(snap.btc_ret_30s_bps, 0.0)
        self.assertFalse(snap.btc_dump_active)
        allowed, reason = self.detector.is_long_allowed(now)
        self.assertTrue(allowed)
        self.assertEqual(self.detector.get_tsunami_multiplier(now), 1.0)

    def test_btc_flash_dump_circuit_breaker(self):
        t0 = 1000.0
        # Normal BTC price
        self.detector.on_btc_tick(100000.0, t0 - 40.0)
        self.detector.on_btc_tick(100000.0, t0 - 20.0)

        # Sudden flash dump: 100,000 -> 99,700 (-30 bps in 10s)
        self.detector.on_btc_tick(99700.0, t0)

        snap = self.detector.get_regime(t0)
        self.assertEqual(snap.regime, MarketRegime.FLASH_DUMP)
        self.assertTrue(snap.btc_dump_active)
        self.assertAlmostEqual(snap.btc_ret_30s_bps, -30.0, places=1)

        # Longs must be vetoed!
        allowed, reason = self.detector.is_long_allowed(t0)
        self.assertFalse(allowed)
        self.assertIn("BTC Flash Dump", reason)

        # After 30s, cooldown (60s) must still be active even if BTC bounces
        t_bounce = t0 + 30.0
        self.detector.on_btc_tick(99900.0, t_bounce)
        snap_bounce = self.detector.get_regime(t_bounce)
        self.assertEqual(snap_bounce.regime, MarketRegime.FLASH_DUMP)
        self.assertTrue(snap_bounce.btc_dump_active)
        allowed, _ = self.detector.is_long_allowed(t_bounce)
        self.assertFalse(allowed)

        # After 65s (exceeding 60s cooldown), lockout clears
        t_cleared = t0 + 65.0
        self.detector.on_btc_tick(99900.0, t_cleared)
        snap_cleared = self.detector.get_regime(t_cleared)
        self.assertFalse(snap_cleared.btc_dump_active)
        allowed, _ = self.detector.is_long_allowed(t_cleared)
        self.assertTrue(allowed)

    def test_strong_pump_and_short_shield(self):
        t0 = 1000.0
        # Simulate 10 altcoins
        coins = ["SOL", "ADA", "DOGE", "AVAX", "LINK", "DOT", "NEAR", "APT", "SUI", "RENDER"]
        
        # 300s ago: all at 10.0
        for c in coins:
            self.detector.on_alt_tick(c, 10.0, t0 - 300.0)

        # Now: 8 out of 10 coins surged to 10.5 (+5%), 2 dropped to 9.8 (-2%)
        # Breadth = 8 / 10 = 80% (>= 70% pump threshold)
        for c in coins[:8]:
            self.detector.on_alt_tick(c, 10.5, t0)
        for c in coins[8:]:
            self.detector.on_alt_tick(c, 9.8, t0)

        # BTC is calm/positive (+10 bps)
        self.detector.on_btc_tick(100000.0, t0 - 300.0)
        self.detector.on_btc_tick(100100.0, t0)

        snap = self.detector.get_regime(t0)
        self.assertEqual(snap.regime, MarketRegime.STRONG_PUMP)
        self.assertEqual(snap.breadth_pct_300s, 80.0)

        # Longs allowed and encouraged (Tsunami multiplier reduced)
        self.assertTrue(self.detector.is_long_allowed(t0)[0])
        self.assertEqual(self.detector.get_tsunami_multiplier(t0), 0.6)

        # Shorts (S2 Fade) MUST BE BLOCKED during strong pump wave
        allowed_short, short_reason = self.detector.is_short_allowed(t0)
        self.assertFalse(allowed_short)
        self.assertIn("Strong Market Pump", short_reason)

    def test_altcoin_breadth_dump_without_btc(self):
        t0 = 1000.0
        coins = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
        # 300s ago: all at 10.0
        for c in coins:
            self.detector.on_alt_tick(c, 10.0, t0 - 300.0)

        # Now: 9 out of 10 dropped to 9.0 (only 1 up -> 10% breadth < 25% dump threshold)
        for c in coins[:9]:
            self.detector.on_alt_tick(c, 9.0, t0)
        self.detector.on_alt_tick(coins[9], 10.2, t0)

        # BTC flat
        self.detector.on_btc_tick(100000.0, t0 - 300.0)
        self.detector.on_btc_tick(100000.0, t0)

        snap = self.detector.get_regime(t0)
        self.assertEqual(snap.regime, MarketRegime.FLASH_DUMP)
        self.assertEqual(snap.breadth_pct_300s, 10.0)

        # Longs vetoed due to systemic alt breadth collapse
        allowed, reason = self.detector.is_long_allowed(t0)
        self.assertFalse(allowed)
        self.assertIn("Market Breadth Collapse", reason)

    def test_korean_trade_flow_accumulation(self):
        t0 = 1000.0
        # $80k buys, $20k sells in last 60s
        self.detector.on_korean_trade(50000.0, True, t0 - 30.0)
        self.detector.on_korean_trade(30000.0, True, t0 - 15.0)
        self.detector.on_korean_trade(20000.0, False, t0 - 10.0)

        snap = self.detector.get_regime(t0)
        self.assertAlmostEqual(snap.korean_buy_ratio_60s, 0.8, places=2)

    def test_snapshot_to_dict(self):
        now = 1000.0
        snap = self.detector.get_regime(now)
        d = snap.to_dict()
        self.assertIn("regime", d)
        self.assertIn("breadth_pct_300s", d)
        self.assertIn("btc_ret_30s_bps", d)
        self.assertIn("btc_dump_active", d)


if __name__ == "__main__":
    unittest.main()
