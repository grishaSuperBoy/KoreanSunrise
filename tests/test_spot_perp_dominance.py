"""
tests/test_spot_perp_dominance.py - Unit tests for Spot-Perp Lead-Lag Scoring Engine.
"""
from __future__ import annotations

import unittest
from pipeline.spot_perp_dominance import (
    _sigmoid,
    simulate_vwap_walk_bps,
    calc_kimchi_volatility_pct,
    calc_cross_correlation,
    score_pair_lead_lag,
    categorize_lead_lag_score,
    evaluate_pair_dominance,
)


class TestSpotPerpDominance(unittest.TestCase):

    def test_sigmoid_properties(self):
        # Midpoint always yields 0.5
        self.assertAlmostEqual(_sigmoid(20.0, midpoint=20.0, steepness=1.0), 0.5, places=5)
        # Large values saturate to 1.0 and 0.0
        self.assertAlmostEqual(_sigmoid(100.0, midpoint=20.0, steepness=1.0), 1.0, places=5)
        self.assertAlmostEqual(_sigmoid(-100.0, midpoint=20.0, steepness=1.0), 0.0, places=5)

    def test_simulate_vwap_walk_basic(self):
        # Empty or invalid book returns penalty
        self.assertEqual(simulate_vwap_walk_bps([], 10000.0), 999.0)
        self.assertEqual(simulate_vwap_walk_bps([(100.0, 10.0)], -50.0), 999.0)

        # Single level with ample depth: VWAP = best price -> impact = 0.0 bps
        book = [(100.0, 1000.0)]  # $100k depth at 100.0
        impact = simulate_vwap_walk_bps(book, 10000.0)
        self.assertEqual(impact, 0.0)

        # Multi-level walk:
        # Level 1: 100.0, size 100 ($10,000)
        # Level 2: 101.0, size 100 ($10,100)
        # Target: $20,000
        # Takes: $10,000 @ 100.0 (cost $1,000,000 / 100 = 100 shares -> $10,000)
        # Takes: $10,000 @ 101.0 (shares = 10,000 / 101 = 99.0099 shares)
        # Total cost = $20,000, Total shares = 100 + 99.0099 = 199.0099
        # VWAP = $20,000 / 199.0099 = 100.4975
        # Impact bps = (100.4975 - 100.0) / 100.0 * 10,000 = ~49.75 bps
        book_2lvl = [(100.0, 100.0), (101.0, 100.0)]
        impact_2lvl = simulate_vwap_walk_bps(book_2lvl, 20000.0)
        self.assertAlmostEqual(impact_2lvl, 49.75, delta=1.0)

        # Shallow book (< 90% depth available) returns penalty
        shallow_book = [(100.0, 10.0)]  # only $1,000 depth
        self.assertEqual(simulate_vwap_walk_bps(shallow_book, 50000.0), 999.0)

    def test_calc_kimchi_volatility(self):
        # Empty or single point returns 0.0
        self.assertEqual(calc_kimchi_volatility_pct([]), 0.0)
        self.assertEqual(calc_kimchi_volatility_pct([100.0]), 0.0)

        # Flat history (0 variance)
        self.assertEqual(calc_kimchi_volatility_pct([200.0, 200.0, 200.0]), 0.0)

        # Known stddev: values 100 bps and 300 bps (mean 200 bps, diffs -100, +100)
        # var = (10000 + 10000) / 1 = 20000 -> std = 141.42 bps = 1.414%
        vol = calc_kimchi_volatility_pct([100.0, 300.0])
        self.assertAlmostEqual(vol, 1.414, places=2)

    def test_calc_cross_correlation(self):
        # Generate series where y perfectly lags x by 2 steps
        x = [1.0, 2.0, 4.0, 1.0, 3.0, 5.0, 2.0, 1.0, 4.0, 6.0, 2.0, 1.0, 3.0]
        y = [0.0, 0.0] + x[:-2]  # y is x lagged by 2 steps

        rho = calc_cross_correlation(x, y, lag_steps=2)
        self.assertAlmostEqual(rho, 1.0, places=2)

        # Insufficient data
        self.assertEqual(calc_cross_correlation([1.0, 2.0], [1.0, 2.0], lag_steps=2), 0.0)

    def test_score_pair_lead_lag_ideal_setup(self):
        # Ideal Korean Spot Leader:
        # High SDR (35%), low PVR (2.5x), sweet-spot impact (18 bps),
        # low kimchi sigma (0.8%), strong lead-lag correlation (0.75), active flow ($60k)
        score, comps = score_pair_lead_lag(
            sdr_24h=0.35,
            pvr_venue=2.5,
            vwap_impact_bps=18.0,
            kimchi_sigma_24h_pct=0.8,
            lead_lag_corr=0.75,
            liq_flow_ratio=0.02,
            flow_density_15m_usd=60000.0,
        )

        self.assertGreaterEqual(score, 0.70)
        self.assertGreater(comps["s_spot"], 0.85)
        self.assertGreater(comps["s_odr"], 0.95)
        self.assertEqual(comps["flow_mult"], 1.0)

        decision, size_mult = categorize_lead_lag_score(score)
        self.assertEqual(decision, "FULL")
        self.assertEqual(size_mult, 1.0)

    def test_score_pair_lead_lag_perp_overloaded_toxic(self):
        # Perp Overloaded Meme Coin on Binance/Bybit:
        # Negligible SDR (2%), massive PVR (45x), violent liquidations (40%),
        # volatile kimchi (4.5%), low correlation (0.15)
        score, comps = score_pair_lead_lag(
            sdr_24h=0.02,
            pvr_venue=45.0,
            vwap_impact_bps=2.0,  # Rock hard book, absorbs everything without moving
            kimchi_sigma_24h_pct=4.5,
            lead_lag_corr=0.15,
            liq_flow_ratio=0.40,
            flow_density_15m_usd=50000.0,
        )

        self.assertLess(score, 0.40)
        decision, size_mult = categorize_lead_lag_score(score)
        self.assertEqual(decision, "SKIP")
        self.assertEqual(size_mult, 0.0)

    def test_score_pair_dead_session_throttle(self):
        # Perfect 24h stats, BUT Upbit has been completely dead in last 15m ($2k flow)
        score, comps = score_pair_lead_lag(
            sdr_24h=0.40,
            pvr_venue=1.5,
            vwap_impact_bps=18.0,
            kimchi_sigma_24h_pct=0.5,
            lead_lag_corr=0.80,
            flow_density_15m_usd=2000.0,  # Dead market (10% of $20k throttle threshold)
        )

        self.assertLess(comps["flow_mult"], 0.20)
        self.assertLess(score, 0.30)
        decision, _ = categorize_lead_lag_score(score)
        self.assertEqual(decision, "SKIP")

    def test_evaluate_pair_dominance_convenience(self):
        m = evaluate_pair_dominance(
            coin="DOOD",
            venue="binance",
            sdr_24h=0.30,
            pvr_venue=3.0,
            vwap_impact_bps=22.0,
            kimchi_sigma_24h_pct=1.0,
            lead_lag_corr=0.65,
        )
        self.assertEqual(m.coin, "DOOD")
        self.assertEqual(m.venue, "binance")
        self.assertGreaterEqual(m.composite_score, 0.60)
        self.assertIn(m.decision, ("FULL", "HALF"))


if __name__ == "__main__":
    unittest.main()
