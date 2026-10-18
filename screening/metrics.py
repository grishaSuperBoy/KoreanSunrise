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
    min_corr: float = 0.70,
    min_both_trade: float = 0.20,
    min_dev_bps: float = 10.0,
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

    # 2. Correlation of minute log-returns
    r_up = np.diff(np.log(p_up_usd))
    r_perp = np.diff(np.log(p_perp_usd))

    std_up = float(np.std(r_up))
    std_perp = float(np.std(r_perp))

    if std_up < 1e-9 or std_perp < 1e-9:
        corr = 0.0
    else:
        c_val = float(np.corrcoef(r_up, r_perp)[0, 1])
        corr = c_val if np.isfinite(c_val) else 0.0

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
