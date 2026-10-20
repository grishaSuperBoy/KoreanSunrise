"""
strategies - Microstructural quantitative trading strategies for KoreanSunrise.

Architecture:
- Strategy 1: Korean Sunrise Momentum (Upbit Sweep Lead-Lag) -> strategies/strategy_1_sweep_lead_lag.py
- Strategy 2: Fade the Fakeout (Anti-Sweep Short)            -> strategies/strategy_2_fade_fakeout.py
- Strategy 3: Maker Liquidity Harvesting (Spread Voids)      -> strategies/strategy_3_maker_voids.py
"""
from __future__ import annotations

from strategies.strategy_1_sweep_lead_lag import (
    KoreanLeadLagStrategy,
    PairState,
    UpbitNormState,
    evaluate_exit_reason,
)
from strategies.strategy_2_fade_fakeout import (
    ActiveFadePosition,
    FadeFakeoutStrategy,
)
from strategies.strategy_3_maker_voids import (
    ActiveMakerVoid,
    MakerVoidStrategy,
)

__all__ = [
    "KoreanLeadLagStrategy",
    "PairState",
    "UpbitNormState",
    "evaluate_exit_reason",
    "FadeFakeoutStrategy",
    "ActiveFadePosition",
    "MakerVoidStrategy",
    "ActiveMakerVoid",
]
