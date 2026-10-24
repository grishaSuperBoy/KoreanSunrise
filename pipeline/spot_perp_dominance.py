"""
pipeline/spot_perp_dominance.py - Institutional Spot-Perp Lead-Lag Scoring & Universe Selection.

Evaluates whether a coin-venue pair is structurally suitable for Korean Lead-Lag (S1)
and Predatory Rebound Maker (S4) trading, replacing arbitrary binary cutoffs with a
continuous, normalized composite score in [0.0, 1.0].

Core Mathematical Components:
1. SDR (Spot Dominance Ratio): Upbit 24h volume vs global aggregate perp volume.
2. PVR (Perp Velocity Ratio): Follower exchange perp volume vs Upbit volume, penalised by liquidation flow.
3. Orderbook Elasticity (VWAP Walk Impact): Simulates walking follower book by median Upbit sweep.
4. Kimchi Premium Volatility: Rolling stddev of kimchi basis over 24h.
5. Lead-Lag Empirical Correlation: Cross-correlation between Upbit and perp mid returns at empirical lag.
6. Session Flow Density: Activity throttle based on recent 15m Upbit flow.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _sigmoid(x: float, midpoint: float, steepness: float) -> float:
    """
    Logistic normalization of a continuous metric into [0.0, 1.0].
    Monotonically increases from 0 to 1 with center at midpoint.
    """
    try:
        z = -steepness * (x - midpoint)
        if z > 40.0:
            return 0.0
        if z < -40.0:
            return 1.0
        return 1.0 / (1.0 + math.exp(z))
    except (OverflowError, ValueError):
        return 0.0 if z > 0 else 1.0


def simulate_vwap_walk_bps(
    book_levels: Sequence[Tuple[float, float]],
    target_usd: float,
) -> float:
    """
    Simulates walking the orderbook with a target USD impulse.
    book_levels: List of (price, size) tuples ordered from best to worst.
    target_usd: Dollar amount of the sweep (e.g. $25,000 to $50,000).

    Returns:
        Impact in basis points: abs(VWAP - best_price) / best_price * 10,000.
        If the book has insufficient depth (< 90% target absorbed), returns 999.0 bps.
    """
    if not book_levels or target_usd <= 0.0:
        return 999.0

    best_p = float(book_levels[0][0])
    if best_p <= 0.0:
        return 999.0

    accum_usd = 0.0
    accum_cost = 0.0

    for level in book_levels:
        p = float(level[0])
        sz = float(level[1])
        lvl_usd = p * sz
        if lvl_usd <= 0.0:
            continue

        take_usd = min(lvl_usd, target_usd - accum_usd)
        accum_usd += take_usd
        accum_cost += take_usd * p

        if accum_usd >= target_usd:
            break

    # If the book cannot absorb at least 90% of the impulse, it's a void/toxic book
    if accum_usd < target_usd * 0.90:
        return 999.0

    vwap = accum_cost / accum_usd
    impact_bps = abs(vwap - best_p) / best_p * 1e4
    return round(impact_bps, 2)


def calc_kimchi_volatility_pct(basis_history_bps: Sequence[float]) -> float:
    """
    Computes sample standard deviation of the Kimchi basis (in %).
    basis_history_bps: Array of basis deviations in bps ((Upbit - Follower) / Follower * 10,000).
    """
    if not basis_history_bps or len(basis_history_bps) < 2:
        return 0.0

    n = len(basis_history_bps)
    mean_bps = sum(basis_history_bps) / n
    var = sum((x - mean_bps) ** 2 for x in basis_history_bps) / (n - 1)
    std_bps = math.sqrt(max(0.0, var))
    # Convert bps to percent (100 bps = 1.0%)
    return round(std_bps / 100.0, 3)


def calc_cross_correlation(
    x_series: Sequence[float],
    y_series: Sequence[float],
    lag_steps: int = 2,
) -> float:
    """
    Computes Pearson correlation between leader series x(t) and follower series y(t + lag).
    lag_steps: Positive integer representing how many steps y lags behind x.
    """
    if len(x_series) != len(y_series) or len(x_series) <= lag_steps + 5:
        return 0.0

    # Align series: x[0 : N - lag], y[lag : N]
    x_aligned = x_series[: len(x_series) - lag_steps]
    y_aligned = y_series[lag_steps:]

    n = len(x_aligned)
    if n < 5:
        return 0.0

    mean_x = sum(x_aligned) / n
    mean_y = sum(y_aligned) / n

    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_aligned, y_aligned))
    var_x = sum((x - mean_x) ** 2 for x in x_aligned)
    var_y = sum((y - mean_y) ** 2 for y in y_aligned)

    denom = math.sqrt(var_x * var_y)
    if denom <= 1e-12:
        return 0.0

    rho = cov / denom
    return round(max(-1.0, min(1.0, rho)), 3)


@dataclass(slots=True)
class PairDominanceMetrics:
    coin: str
    venue: str
    sdr_24h: float                        # Upbit 24h vol / Total perp 24h vol
    pvr_venue: float                      # Venue perp 24h vol / Upbit 24h vol
    vwap_impact_bps: float                # Simulated bps walked by median impulse
    kimchi_sigma_24h_pct: float           # Stddev of kimchi premium in %
    lead_lag_corr: float                  # Empirical correlation at optimal lag (e.g. 1-3s)
    liq_flow_ratio: float = 0.0           # Venue liquidation vol / Venue total vol
    flow_density_15m_usd: float = 50000.0 # Recent 15m Upbit flow in USD

    # Calculated components
    s_spot: float = 0.0
    s_pvr: float = 0.0
    s_odr: float = 0.0
    s_kimchi: float = 0.0
    s_lead: float = 0.0
    flow_mult: float = 1.0
    composite_score: float = 0.0
    decision: str = "SKIP"                # FULL, HALF, SKIP
    size_multiplier: float = 0.0


def score_pair_lead_lag(
    sdr_24h: float,
    pvr_venue: float,
    vwap_impact_bps: float,
    kimchi_sigma_24h_pct: float,
    lead_lag_corr: float,
    liq_flow_ratio: float = 0.0,
    flow_density_15m_usd: float = 50000.0,
) -> Tuple[float, Dict[str, float]]:
    """
    Computes continuous [0.0, 1.0] composite score evaluating suitability of a pair for Lead-Lag.

    Returns:
        (composite_score, components_breakdown)
    """
    # 1. Spot dominance (SDR):
    # Midpoint at 0.20 (20% share), steepness 15.0.
    # SDR >= 0.35 -> s_spot > 0.90; SDR <= 0.05 -> s_spot < 0.10
    s_spot = _sigmoid(sdr_24h, midpoint=0.20, steepness=15.0)

    # 2. Perp overload (PVR) penalized by liquidation flow:
    # Lower PVR is better (follower perp shouldn't dwarf Upbit spot).
    # Effective PVR amplified when liquidation flow is heavy:
    pvr_eff = pvr_venue * (1.0 + 2.0 * max(0.0, liq_flow_ratio))
    # Midpoint at 6.0x, steepness 0.25 (evaluated negatively)
    s_pvr = _sigmoid(-pvr_eff, midpoint=-6.0, steepness=0.25)

    # 3. Orderbook elasticity (ODR / VWAP Impact):
    # Sweet spot: 10 to 30 bps (ideal peak at 18 bps with width sigma=12).
    # Penalizes rock-hard books (< 4 bps: impulse absorbed, no dislocation)
    # and illiquid voids (> 80 bps: toxic slippage on our own market orders).
    if vwap_impact_bps >= 900.0:
        s_odr = 0.0
    else:
        s_odr = math.exp(-((vwap_impact_bps - 18.0) ** 2) / (2.0 * (12.0 ** 2)))

    # 4. Kimchi Basis Stability:
    # Stable basis (sigma < 1.5%) means price movements transmit cleanly.
    # Erratic basis (sigma > 3.0%) is noisy idiosyncratic flow.
    s_kimchi = _sigmoid(-kimchi_sigma_24h_pct, midpoint=-1.5, steepness=2.0)

    # 5. Lead-Lag Empirical Transmission:
    # Correlation between Upbit returns and perp returns at 1-3s lag.
    # Higher correlation = tighter coupling.
    s_lead = _sigmoid(lead_lag_corr, midpoint=0.50, steepness=8.0)

    # 6. Session Flow Density Gate:
    # If Upbit trading flow over recent 15 minutes is < $10k, market is dead/asleep.
    # Throttle between $0 and $20,000.
    flow_mult = min(1.0, max(0.0, flow_density_15m_usd / 20000.0))

    raw_score = (
        0.30 * s_spot +
        0.20 * s_pvr +
        0.20 * s_odr +
        0.15 * s_lead +
        0.15 * s_kimchi
    ) * flow_mult

    score_norm = round(max(0.0, min(1.0, raw_score)), 3)

    components = {
        "s_spot": round(s_spot, 3),
        "s_pvr": round(s_pvr, 3),
        "s_odr": round(s_odr, 3),
        "s_kimchi": round(s_kimchi, 3),
        "s_lead": round(s_lead, 3),
        "flow_mult": round(flow_mult, 3),
        "composite_score": score_norm,
    }
    return score_norm, components


def categorize_lead_lag_score(score: float) -> Tuple[str, float]:
    """
    Maps continuous score to trading decision and position sizing multiplier.

    Thresholds:
        score >= 0.70 -> ("FULL", 1.0)
        0.55 <= score < 0.70 -> ("HALF", 0.5)
        score < 0.55 -> ("SKIP", 0.0)
    """
    if score >= 0.70:
        return "FULL", 1.0
    if score >= 0.55:
        return "HALF", 0.5
    return "SKIP", 0.0


def evaluate_pair_dominance(
    coin: str,
    venue: str,
    sdr_24h: float,
    pvr_venue: float,
    vwap_impact_bps: float,
    kimchi_sigma_24h_pct: float,
    lead_lag_corr: float,
    liq_flow_ratio: float = 0.0,
    flow_density_15m_usd: float = 50000.0,
) -> PairDominanceMetrics:
    """Convenience function returning fully populated PairDominanceMetrics."""
    score, comp = score_pair_lead_lag(
        sdr_24h=sdr_24h,
        pvr_venue=pvr_venue,
        vwap_impact_bps=vwap_impact_bps,
        kimchi_sigma_24h_pct=kimchi_sigma_24h_pct,
        lead_lag_corr=lead_lag_corr,
        liq_flow_ratio=liq_flow_ratio,
        flow_density_15m_usd=flow_density_15m_usd,
    )
    decision, size_mult = categorize_lead_lag_score(score)

    return PairDominanceMetrics(
        coin=coin,
        venue=venue,
        sdr_24h=sdr_24h,
        pvr_venue=pvr_venue,
        vwap_impact_bps=vwap_impact_bps,
        kimchi_sigma_24h_pct=kimchi_sigma_24h_pct,
        lead_lag_corr=lead_lag_corr,
        liq_flow_ratio=liq_flow_ratio,
        flow_density_15m_usd=flow_density_15m_usd,
        s_spot=comp["s_spot"],
        s_pvr=comp["s_pvr"],
        s_odr=comp["s_odr"],
        s_kimchi=comp["s_kimchi"],
        s_lead=comp["s_lead"],
        flow_mult=comp["flow_mult"],
        composite_score=score,
        decision=decision,
        size_multiplier=size_mult,
    )
