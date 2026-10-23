"""
strategies/quant_lead_lag.py - Quantitative Microstructure Math & Gating for Lead-Lag Transmission.
Pure, stateless, unit-testable mathematical functions governing information flow and orderbook physics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class LeadLagQuantDecision:
    """Immutable result of microstructure quant gate evaluation."""
    passed: bool
    reason: str
    phi: float
    raw_diff_bps: float
    dislocation_bps: float
    dev_z: float
    baseline_bps: float
    details: Dict[str, Any] = field(default_factory=dict)


def compute_raw_dislocation_bps(leader_mid: float, follower_mid: float) -> float:
    """
    Computes instantaneous raw price difference in basis points:
    (Leader_Mid - Follower_Mid) / Follower_Mid * 10,000.
    """
    if follower_mid <= 0.0 or leader_mid <= 0.0:
        return 0.0
    return (leader_mid - follower_mid) / follower_mid * 1e4


def compute_elastic_dislocation_bps(raw_diff_bps: float, baseline_bps: float, side: str = "long") -> float:
    """
    Computes elastic dislocation relative to 10-minute rolling Kimchi baseline.
    For Long: raw_diff_bps - baseline_bps (Upbit lifted ABOVE baseline).
    For Short: baseline_bps - raw_diff_bps (Upbit dropped BELOW baseline).
    """
    if side == "short":
        return baseline_bps - raw_diff_bps
    return raw_diff_bps - baseline_bps


def compute_impulse_power_ratio(swept_usd: float, follower_opposing_depth_usd: float) -> float:
    """
    Computes Impulse Power Ratio (Phi):
    Ratio of Korean leader sweep size to follower opposing top-3 depth.
    Phi = swept_usd / follower_opposing_depth_usd.
    A high Phi (e.g. >= 4.0x) indicates the Korean sweep overwhelms the follower book.
    If follower depth is zero or negative, or sweep is non-positive, returns 0.0.
    """
    if swept_usd <= 0.0 or follower_opposing_depth_usd <= 0.0:
        return 0.0
    return round(swept_usd / follower_opposing_depth_usd, 2)


def check_information_flow_leadership(
    raw_diff_bps: float,
    baseline_bps: float,
    side: str = "long",
    min_lead_basis_bps: float = -10.0,
) -> Tuple[bool, str]:
    """
    Validates whether the Korean leader (Upbit) is genuinely leading price discovery,
    or whether the follower (global perpetual swap) is already leading with an inverted basis.

    Physics:
    For Long:
      If baseline_bps < min_lead_basis_bps (e.g. -10 bps), global perps are already
      trading higher than Upbit (e.g. perps at +100 bps premium). Upbit buys are
      merely lagging retail FOMO catching up to an existing perp move.
      The instantaneous raw_diff_bps must also not be deep in negative territory.
    For Short:
      If baseline_bps > +min_lead_basis_bps (e.g. +10 bps), global perps are already
      trading lower than Upbit, so Upbit is lagging behind global dump.
    """
    if side == "long":
        if baseline_bps < min_lead_basis_bps:
            return False, f"follower_already_leads_upbit (baseline={baseline_bps:+.1f}b < {min_lead_basis_bps:+.1f}b)"
        if raw_diff_bps < min_lead_basis_bps:
            return False, f"follower_instantaneous_above_upbit (raw_diff={raw_diff_bps:+.1f}b < {min_lead_basis_bps:+.1f}b)"
    else:
        # Short side: Upbit is dumping below perps. Perps must not already be far below Upbit.
        max_short_lead_basis = abs(min_lead_basis_bps)
        if baseline_bps > max_short_lead_basis:
            return False, f"follower_already_leads_dump (baseline={baseline_bps:+.1f}b > {max_short_lead_basis:+.1f}b)"
        if raw_diff_bps > max_short_lead_basis:
            return False, f"follower_instantaneous_below_upbit (raw_diff={raw_diff_bps:+.1f}b > {max_short_lead_basis:+.1f}b)"

    return True, "ok"


def check_impulse_power_ratio(
    swept_usd: float,
    follower_opposing_depth_usd: float,
    min_power_ratio: float = 4.0,
) -> Tuple[bool, float, str]:
    """
    Validates whether the Korean sweep volume is sufficient to punch through
    the opposing limit orders on the follower exchange.

    If Phi < min_power_ratio, follower market makers absorb the impulse like a wall,
    causing immediate stall and stop_decay loss.
    """
    phi = compute_impulse_power_ratio(swept_usd, follower_opposing_depth_usd)
    if phi < min_power_ratio:
        return False, phi, f"insufficient_impulse_power (phi={phi:.1f}x < {min_power_ratio:.1f}x, depth=${follower_opposing_depth_usd:.0f})"
    return True, phi, "ok"


def evaluate_lead_lag_microstructure_gate(
    leader_mid: float,
    follower_mid: float,
    baseline_bps: float,
    sigma: float,
    swept_usd: float,
    follower_opposing_depth_usd: float,
    side: str = "long",
    min_dev_z: float = 1.8,
    min_power_ratio: float = 4.0,
    min_lead_basis_bps: float = -10.0,
) -> LeadLagQuantDecision:
    """
    Master microstructure gate combining:
    1. Raw and elastic dislocation calculation.
    2. Information flow direction parity check.
    3. Z-score statistical significance check.
    4. Orderbook Impulse Power Ratio (Phi) penetration check.
    """
    raw_diff = compute_raw_dislocation_bps(leader_mid, follower_mid)
    dislocation = compute_elastic_dislocation_bps(raw_diff, baseline_bps, side=side)
    dev_z = round(dislocation / max(sigma, 1.5), 2)
    phi = compute_impulse_power_ratio(swept_usd, follower_opposing_depth_usd)

    details = {
        "leader_mid": leader_mid,
        "follower_mid": follower_mid,
        "raw_diff_bps": raw_diff,
        "baseline_bps": baseline_bps,
        "dislocation_bps": dislocation,
        "sigma": sigma,
        "dev_z": dev_z,
        "swept_usd": swept_usd,
        "follower_depth_usd": follower_opposing_depth_usd,
        "phi": phi,
        "side": side,
    }

    # Gate 1: Direction of dislocation must be positive
    if dislocation <= 0.0:
        return LeadLagQuantDecision(
            passed=False,
            reason=f"negative_elastic_dislocation ({dislocation:+.1f}b <= 0)",
            phi=phi,
            raw_diff_bps=raw_diff,
            dislocation_bps=dislocation,
            dev_z=dev_z,
            baseline_bps=baseline_bps,
            details=details,
        )

    # Gate 2: Information Flow Direction Parity (Upbit must be leading, not lagging)
    lead_ok, lead_reason = check_information_flow_leadership(
        raw_diff_bps=raw_diff,
        baseline_bps=baseline_bps,
        side=side,
        min_lead_basis_bps=min_lead_basis_bps,
    )
    if not lead_ok:
        return LeadLagQuantDecision(
            passed=False,
            reason=lead_reason,
            phi=phi,
            raw_diff_bps=raw_diff,
            dislocation_bps=dislocation,
            dev_z=dev_z,
            baseline_bps=baseline_bps,
            details=details,
        )

    # Gate 3: Statistical Significance (Z-score)
    if dev_z < min_dev_z:
        return LeadLagQuantDecision(
            passed=False,
            reason=f"insufficient_z_score ({dev_z:.2f}σ < {min_dev_z:.2f}σ)",
            phi=phi,
            raw_diff_bps=raw_diff,
            dislocation_bps=dislocation,
            dev_z=dev_z,
            baseline_bps=baseline_bps,
            details=details,
        )

    # Gate 4: Impulse Power Ratio (Phi)
    phi_ok, phi_val, phi_reason = check_impulse_power_ratio(
        swept_usd=swept_usd,
        follower_opposing_depth_usd=follower_opposing_depth_usd,
        min_power_ratio=min_power_ratio,
    )
    if not phi_ok:
        return LeadLagQuantDecision(
            passed=False,
            reason=phi_reason,
            phi=phi_val,
            raw_diff_bps=raw_diff,
            dislocation_bps=dislocation,
            dev_z=dev_z,
            baseline_bps=baseline_bps,
            details=details,
        )

    return LeadLagQuantDecision(
        passed=True,
        reason="ok",
        phi=phi,
        raw_diff_bps=raw_diff,
        dislocation_bps=dislocation,
        dev_z=dev_z,
        baseline_bps=baseline_bps,
        details=details,
    )
