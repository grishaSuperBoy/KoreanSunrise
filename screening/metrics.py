"""
screening/metrics.py — Quantitative metrics and statistical validation for candidate pairs.
Vectorized numpy math for 1-minute kline correlation, namesake detection, and basis volatility.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import numpy as np


def evaluate_pair_klines(
    kl_upbit: Dict[int, Tuple[float, float]],
    kl_perp: Dict[int, Tuple[float, float]],
    usdt_krw_rate: float,
    min_corr: float = 0.50,
    min_both_trade: float = 0.05,
    min_dev_bps: float = 6.0,
    max_price_diff_pct: float = 15.0,
    min_samples: int = 30,
) -> Dict[str, float]:
    """
    Evaluates 1-minute candle time series between Upbit (KRW) and Follower Perp (USD).

    Returns dictionary of quantitative metrics:
      - 'samples': number of overlapping minutes
      - 'price_diff_pct': percentage difference in median price (detects namesakes)
      - 'corr': correlation of minute log-returns
      - 'both_trade': fraction of minutes with volume > 0 on both venues
      - 'med_basis_bps': median basis (prevailing Kimchi Premium in bps)
      - 'p90_dev_bps': 90th percentile of |basis - med_basis| in bps
      - 'passed': 1.0 if qualifies, 0.0 otherwise
    """
    empty_res = {
        "samples": 0.0,
        "price_diff_pct": 999.0,
        "corr": 0.0,
        "both_trade": 0.0,
        "med_basis_bps": 0.0,
        "p90_dev_bps": 0.0,
        "passed": 0.0,
    }

    if usdt_krw_rate <= 0:
        return empty_res

    common_ts = sorted(set(kl_upbit.keys()) & set(kl_perp.keys()))
    if len(common_ts) < min_samples:
        return {**empty_res, "samples": float(len(common_ts))}

    # Upbit prices in USD: price_krw / usdt_krw_rate
    p_up_usd = np.array([kl_upbit[t][0] / usdt_krw_rate for t in common_ts], dtype=np.float64)
    v_up_usd = np.array([kl_upbit[t][1] / usdt_krw_rate for t in common_ts], dtype=np.float64)

    p_perp_usd = np.array([kl_perp[t][0] for t in common_ts], dtype=np.float64)
    v_perp_usd = np.array([kl_perp[t][1] for t in common_ts], dtype=np.float64)

    # Sanity: ensure all prices positive
    if np.any(p_up_usd <= 0) or np.any(p_perp_usd <= 0):
        return {**empty_res, "samples": float(len(common_ts))}

    # 1. Namesake check (Guard against different tokens sharing the same ticker)
    med_up = float(np.median(p_up_usd))
    med_perp = float(np.median(p_perp_usd))
    price_diff_pct = abs(med_up - med_perp) / max(med_up, med_perp) * 100.0

    if price_diff_pct > max_price_diff_pct:
        return {
            "samples": float(len(common_ts)),
            "price_diff_pct": round(price_diff_pct, 1),
            "corr": 0.0,
            "both_trade": 0.0,
            "med_basis_bps": 0.0,
            "p90_dev_bps": 0.0,
            "passed": 0.0,
        }

    # 2. Multi-scale correlation of log-returns (recovers true co-movement suppressed by Epps effect on alts)
    r_up_1m = np.diff(np.log(p_up_usd))
    r_perp_1m = np.diff(np.log(p_perp_usd))

    corr_1m = 0.0
    std_up_1m = float(np.std(r_up_1m))
    std_perp_1m = float(np.std(r_perp_1m))
    if std_up_1m >= 1e-9 and std_perp_1m >= 1e-9:
        c1 = float(np.corrcoef(r_up_1m, r_perp_1m)[0, 1])
        corr_1m = c1 if np.isfinite(c1) else 0.0

    # Multi-scale aggregation (e.g. 15-minute or adaptive k-step) for asynchronous trading
    corr_multi = 0.0
    k = 15 if len(p_up_usd) >= 30 else max(3, len(p_up_usd) // 3)
    if len(p_up_usd) >= 2 * k:
        p_up_sub = p_up_usd[::k]
        p_perp_sub = p_perp_usd[::k]
        r_up_multi = np.diff(np.log(p_up_sub))
        r_perp_multi = np.diff(np.log(p_perp_sub))
        if len(r_up_multi) >= 2:
            std_up_m = float(np.std(r_up_multi))
            std_perp_m = float(np.std(r_perp_multi))
            if std_up_m >= 1e-9 and std_perp_m >= 1e-9:
                cm = float(np.corrcoef(r_up_multi, r_perp_multi)[0, 1])
                corr_multi = cm if np.isfinite(cm) else 0.0

    corr = max(corr_1m, corr_multi)

    # 3. Active trading minutes on both sides
    both_trade = float(np.mean((v_up_usd > 0) & (v_perp_usd > 0)))

    # 4. Kimchi basis & spread volatility
    mid = (p_up_usd + p_perp_usd) * 0.5
    signed_basis_bps = (p_up_usd - p_perp_usd) / mid * 10000.0
    med_basis_bps = float(np.median(signed_basis_bps))
    basis_dev_bps = np.abs(signed_basis_bps - med_basis_bps)
    p90_dev_bps = float(np.percentile(basis_dev_bps, 90))

    # 5. Gate decision
    passed = 1.0 if (
        corr >= min_corr
        and both_trade >= min_both_trade
        and p90_dev_bps >= min_dev_bps
        and price_diff_pct <= max_price_diff_pct
    ) else 0.0

    return {
        "samples": float(len(common_ts)),
        "price_diff_pct": round(price_diff_pct, 2),
        "corr": round(corr, 3),
        "both_trade": round(both_trade, 3),
        "med_basis_bps": round(med_basis_bps, 1),
        "p90_dev_bps": round(p90_dev_bps, 1),
        "passed": passed,
    }
