"""
strategies - Microstructural quantitative trading strategies for KoreanSunrise.

Official Active Roster (5 Strategies):
- Strategy 1: Korean Sunrise Momentum (Upbit Sweep Lead-Lag) -> strategies/strategy_1_sweep_lead_lag.py
- Strategy 2: Anti-Sweep Reversal (Fade the Fakeout)          -> strategies/strategy_2_fade_fakeout.py
- Strategy 3: Maker Spread Harvesting (Void Hunter)           -> strategies/strategy_3_maker_voids.py
- Strategy 4: Predatory Rebound Maker (Rubber Band)           -> strategies/strategy_4_rubber_band.py
- Strategy 5: Cross-Korean Triangle (Bithumb Radar)           -> strategies/strategy_5_cross_korean.py

Archived / Deprecated:
- strategies/archive/strategy_obiv_imbalance.py (OBIV L2 Imbalance - Retired)
- strategies/archive/strategy_beta_neutral.py (Alt+BTC Pairs - Retired)
"""
from __future__ import annotations

# Strategy 1: Korean Sunrise Momentum (Upbit Sweep Lead-Lag)
from strategies.strategy_1_sweep_lead_lag import (
    KoreanLeadLagStrategy,
    PairState,
    UpbitNormState,
    evaluate_exit_reason,
)

# Strategy 2: Anti-Sweep Reversal (Fade the Fakeout)
from strategies.strategy_2_fade_fakeout import (
    ActiveFadePosition,
    FadeFakeoutStrategy,
)

# Strategy 3: Maker Spread Harvesting (Void Hunter)
from strategies.strategy_3_maker_voids import (
    ActiveMakerVoid,
    MakerVoidStrategy,
)

# Strategy 4: Predatory Rebound Maker (Rubber Band)
from strategies.strategy_4_rubber_band import (
    ActiveRubberBandPosition,
    RubberBandSniperStrategy,
)

# Strategy 5: Cross-Korean Triangle (Bithumb Radar)
from strategies.strategy_5_cross_korean import (
    ActiveCrossKoreanPosition,
    CrossKoreanTriangleStrategy,
)

# Backward-compatibility imports for archived strategies
from strategies.archive import (
    ActiveBetaNeutralPair,
    ActiveImbalancePosition,
    BetaNeutralStrategy,
    OrderbookImbalanceStrategy,
)

__all__ = [
    # Active Roster
    "KoreanLeadLagStrategy",
    "PairState",
    "UpbitNormState",
    "evaluate_exit_reason",
    "FadeFakeoutStrategy",
    "ActiveFadePosition",
    "MakerVoidStrategy",
    "ActiveMakerVoid",
    "RubberBandSniperStrategy",
    "ActiveRubberBandPosition",
    "CrossKoreanTriangleStrategy",
    "ActiveCrossKoreanPosition",
    # Legacy / Archived
    "OrderbookImbalanceStrategy",
    "ActiveImbalancePosition",
    "BetaNeutralStrategy",
    "ActiveBetaNeutralPair",
]
