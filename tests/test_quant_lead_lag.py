"""
tests/test_quant_lead_lag.py - Unit tests for quantitative microstructure math and gating utility.
"""
import unittest

from strategies.quant_lead_lag import (
    LeadLagQuantDecision,
    check_impulse_power_ratio,
    check_information_flow_leadership,
    compute_elastic_dislocation_bps,
    compute_impulse_power_ratio,
    compute_raw_dislocation_bps,
    evaluate_lead_lag_microstructure_gate,
)


class TestQuantLeadLag(unittest.TestCase):
    """Verifies pure mathematical formulas and microstructural gate logic."""

    def test_compute_raw_dislocation_bps(self):
        # Normal upward dislocation
        self.assertAlmostEqual(compute_raw_dislocation_bps(101.0, 100.0), 100.0, places=4)
        # Normal downward dislocation
        self.assertAlmostEqual(compute_raw_dislocation_bps(99.0, 100.0), -100.0, places=4)
        # Zero / invalid price guards
        self.assertEqual(compute_raw_dislocation_bps(0.0, 100.0), 0.0)
        self.assertEqual(compute_raw_dislocation_bps(100.0, 0.0), 0.0)
        self.assertEqual(compute_raw_dislocation_bps(-5.0, 100.0), 0.0)

    def test_compute_elastic_dislocation_bps(self):
        # Long side: dislocation above baseline
        self.assertAlmostEqual(compute_elastic_dislocation_bps(150.0, 50.0, side="long"), 100.0, places=4)
        self.assertAlmostEqual(compute_elastic_dislocation_bps(20.0, 50.0, side="long"), -30.0, places=4)
        # Short side: dislocation below baseline
        self.assertAlmostEqual(compute_elastic_dislocation_bps(-50.0, 50.0, side="short"), 100.0, places=4)
        self.assertAlmostEqual(compute_elastic_dislocation_bps(60.0, 50.0, side="short"), -10.0, places=4)

    def test_compute_impulse_power_ratio(self):
        # High power sweep
        self.assertEqual(compute_impulse_power_ratio(20000.0, 4000.0), 5.0)
        # Low power sweep
        self.assertEqual(compute_impulse_power_ratio(5000.0, 15000.0), 0.33)
        # Zero / negative swept USD
        self.assertEqual(compute_impulse_power_ratio(0.0, 5000.0), 0.0)
        self.assertEqual(compute_impulse_power_ratio(-100.0, 5000.0), 0.0)
        # Opposing depth is zero or negative (dead order book guard -> returns 0.0)
        self.assertEqual(compute_impulse_power_ratio(1000.0, 0.0), 0.0)
        self.assertEqual(compute_impulse_power_ratio(1000.0, -50.0), 0.0)

    def test_check_information_flow_leadership_long(self):
        # Legitimate Korean leader: baseline is +15 bps, raw is +45 bps
        ok, reason = check_information_flow_leadership(raw_diff_bps=45.0, baseline_bps=15.0, side="long")
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

        # Inverted baseline: perps already pumped ahead (baseline = -40 bps)
        ok, reason = check_information_flow_leadership(raw_diff_bps=20.0, baseline_bps=-40.0, side="long")
        self.assertFalse(ok)
        self.assertIn("follower_already_leads_upbit", reason)

        # Instantaneous perp surge ahead of Upbit (raw_diff = -25 bps)
        ok, reason = check_information_flow_leadership(raw_diff_bps=-25.0, baseline_bps=5.0, side="long")
        self.assertFalse(ok)
        self.assertIn("follower_instantaneous_above_upbit", reason)

    def test_check_information_flow_leadership_short(self):
        # Legitimate Korean leader dumping: baseline is -10 bps, raw is -50 bps
        ok, reason = check_information_flow_leadership(raw_diff_bps=-50.0, baseline_bps=-10.0, side="short")
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

        # Inverted baseline: perps already dumped far below Upbit (baseline = +35 bps)
        ok, reason = check_information_flow_leadership(raw_diff_bps=-20.0, baseline_bps=35.0, side="short")
        self.assertFalse(ok)
        self.assertIn("follower_already_leads_dump", reason)

        # Instantaneous perp drop far below Upbit (raw_diff = +25 bps)
        ok, reason = check_information_flow_leadership(raw_diff_bps=25.0, baseline_bps=-5.0, side="short")
        self.assertFalse(ok)
        self.assertIn("follower_instantaneous_below_upbit", reason)

    def test_check_impulse_power_ratio(self):
        # Pass threshold (>= 4.0x)
        ok, phi, reason = check_impulse_power_ratio(swept_usd=20000.0, follower_opposing_depth_usd=4000.0, min_power_ratio=4.0)
        self.assertTrue(ok)
        self.assertEqual(phi, 5.0)
        self.assertEqual(reason, "ok")

        # Fail threshold (< 4.0x)
        ok, phi, reason = check_impulse_power_ratio(swept_usd=5000.0, follower_opposing_depth_usd=10000.0, min_power_ratio=4.0)
        self.assertFalse(ok)
        self.assertEqual(phi, 0.5)
        self.assertIn("insufficient_impulse_power", reason)

    def test_evaluate_lead_lag_microstructure_gate_full(self):
        # 1. Perfectly valid trade: strong Korean sweep, follower book thin, Upbit leading
        dec = evaluate_lead_lag_microstructure_gate(
            leader_mid=101.5,
            follower_mid=100.0,
            baseline_bps=20.0,
            sigma=10.0,
            swept_usd=30000.0,
            follower_opposing_depth_usd=5000.0,
            side="long",
            min_dev_z=1.8,
            min_power_ratio=4.0,
            min_lead_basis_bps=-10.0,
        )
        self.assertTrue(dec.passed)
        self.assertEqual(dec.reason, "ok")
        self.assertEqual(dec.phi, 6.0)
        self.assertGreater(dec.dev_z, 1.8)

        # 2. Gate 1 failure: negative elastic dislocation
        dec_neg = evaluate_lead_lag_microstructure_gate(
            leader_mid=100.0,
            follower_mid=100.5,
            baseline_bps=20.0,
            sigma=10.0,
            swept_usd=30000.0,
            follower_opposing_depth_usd=5000.0,
            side="long",
        )
        self.assertFalse(dec_neg.passed)
        self.assertIn("negative_elastic_dislocation", dec_neg.reason)

        # 3. Gate 2 failure: inverted basis (perps already lead pump)
        dec_inverted = evaluate_lead_lag_microstructure_gate(
            leader_mid=100.5,
            follower_mid=100.0,
            baseline_bps=-80.0,  # Perps 80 bps higher on average
            sigma=10.0,
            swept_usd=30000.0,
            follower_opposing_depth_usd=5000.0,
            side="long",
        )
        self.assertFalse(dec_inverted.passed)
        self.assertIn("follower_already_leads_upbit", dec_inverted.reason)

        # 4. Gate 3 failure: insufficient Z-score
        dec_low_z = evaluate_lead_lag_microstructure_gate(
            leader_mid=100.05,
            follower_mid=100.0,
            baseline_bps=0.0,
            sigma=15.0,  # High volatility makes 5 bps dislocation trivial (Z = 0.33)
            swept_usd=30000.0,
            follower_opposing_depth_usd=5000.0,
            side="long",
            min_dev_z=1.8,
        )
        self.assertFalse(dec_low_z.passed)
        self.assertIn("insufficient_z_score", dec_low_z.reason)

        # 5. Gate 4 failure: insufficient impulse power ratio (hitting an ask wall)
        dec_low_phi = evaluate_lead_lag_microstructure_gate(
            leader_mid=101.5,
            follower_mid=100.0,
            baseline_bps=20.0,
            sigma=10.0,
            swept_usd=6000.0,
            follower_opposing_depth_usd=30000.0,  # 30k USD ask wall, Phi = 0.2x
            side="long",
            min_power_ratio=4.0,
        )
        self.assertFalse(dec_low_phi.passed)
        self.assertIn("insufficient_impulse_power", dec_low_phi.reason)


if __name__ == "__main__":
    unittest.main()
