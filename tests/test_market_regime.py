"""
tests/test_market_regime.py — Comprehensive Unit Tests for Macro Regime & Breadth Engine.
"""
import time
import unittest

from pipeline.market_regime import MarketRegime, MarketRegimeDetector, MarketRegimeSnapshot


class TestMarketRegimeDetector(unittest.TestCase):
    def setUp(self):
        self.detector = MarketRegimeDetector(
            btc_dump_30s_bps=-30.0,
            btc_dump_300s_bps=-80.0,
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

    def test_regime_modifiers_tuning(self):
        now = 1000.0
        # In NEUTRAL warmup: 1.0 baseline across all metrics
        e_l, d_l, s_l = self.detector.get_regime_modifiers("long", now)
        e_s, d_s, s_s = self.detector.get_regime_modifiers("short", now)
        self.assertEqual((e_l, d_l, s_l), (1.0, 1.0, 1.0))
        self.assertEqual((e_s, d_s, s_s), (1.0, 1.0, 1.0))

        # Trigger flash dump: 100,000 -> 99,700
        self.detector.on_btc_tick(100000.0, now - 40.0)
        self.detector.on_btc_tick(100000.0, now - 20.0)
        self.detector.on_btc_tick(99700.0, now)
        e_l, d_l, s_l = self.detector.get_regime_modifiers("long", now)
        e_s, d_s, s_s = self.detector.get_regime_modifiers("short", now)
        # In flash dump: Long size 0.0 (vetoed), Short size 1.5x (boosted)
        self.assertEqual(s_l, 0.0)
        self.assertEqual(s_s, 1.5)
        self.assertEqual(e_s, 0.7)

    def test_btc_minor_noise_does_not_trigger_dump(self):
        t0 = 2000.0
        # Normal BTC noise: -20 bps (100,000 -> 99,800)
        self.detector.on_btc_tick(100000.0, t0 - 40.0)
        self.detector.on_btc_tick(99800.0, t0)
        snap = self.detector.get_regime(t0)
        self.assertFalse(snap.btc_dump_active)
        self.assertNotEqual(snap.regime, MarketRegime.FLASH_DUMP)

    def test_btc_moderate_dump_filtered_by_bullish_breadth(self):
        t0 = 3000.0
        coins = [f"COIN_{i}" for i in range(10)]
        # 300s ago all at 10.0
        for c in coins:
            self.detector.on_alt_tick(c, 10.0, t0 - 300.0)
        # Now: 8 out of 10 coins surged (80% green breadth)
        for c in coins[:8]:
            self.detector.on_alt_tick(c, 10.5, t0)
        for c in coins[8:]:
            self.detector.on_alt_tick(c, 9.8, t0)

        # BTC flickers down by -32 bps in 30s
        self.detector.on_btc_tick(100000.0, t0 - 40.0)
        self.detector.on_btc_tick(99680.0, t0)

        snap = self.detector.get_regime(t0)
        # Bullish breadth (80%) filters out the BTC noise! Longs not vetoed!
        self.assertFalse(snap.btc_dump_active)
        self.assertNotEqual(snap.regime, MarketRegime.FLASH_DUMP)

    def test_btc_moderate_dump_confirmed_by_bearish_breadth(self):
        t0 = 4000.0
        coins = [f"COIN_{i}" for i in range(10)]
        for c in coins:
            self.detector.on_alt_tick(c, 10.0, t0 - 300.0)
        # Now: 8 out of 10 coins dropped (only 20% breadth)
        for c in coins[:2]:
            self.detector.on_alt_tick(c, 10.5, t0)
        for c in coins[2:]:
            self.detector.on_alt_tick(c, 9.5, t0)

        # BTC drops -32 bps in 30s
        self.detector.on_btc_tick(100000.0, t0 - 40.0)
        self.detector.on_btc_tick(99680.0, t0)

        snap = self.detector.get_regime(t0)
        # Bearish breadth confirms the dump!
        self.assertTrue(snap.btc_dump_active)
        self.assertEqual(snap.regime, MarketRegime.FLASH_DUMP)

    def test_btc_severe_crash_unconditional(self):
        t0 = 5000.0
        coins = [f"COIN_{i}" for i in range(10)]
        for c in coins:
            self.detector.on_alt_tick(c, 10.0, t0 - 300.0)
        # Even if alts were green:
        for c in coins:
            self.detector.on_alt_tick(c, 10.5, t0)

        # Violent BTC drop: -55 bps (100,000 -> 99,450)
        self.detector.on_btc_tick(100000.0, t0 - 40.0)
        self.detector.on_btc_tick(99450.0, t0)

        snap = self.detector.get_regime(t0)
        # Severe crash triggers unconditionally!
        self.assertTrue(snap.btc_dump_active)
        self.assertEqual(snap.regime, MarketRegime.FLASH_DUMP)


if __name__ == "__main__":
    unittest.main()
