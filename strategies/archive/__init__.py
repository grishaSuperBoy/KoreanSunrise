"""
strategies/archive - Deprecated / Non-Viable Strategies archived from active roster.
- Orderbook Imbalance Front-Runner (OBIV): Retired due to high microsecond HFT noise and latency decay.
- Beta-Neutral Pairs Trading (Alt Long + BTC Short): Retired due to dual-leg taker fee friction and low dislocation edge.
"""
from __future__ import annotations

from strategies.archive.strategy_obiv_imbalance import (
    ActiveImbalancePosition,
    OrderbookImbalanceStrategy,
)
from strategies.archive.strategy_beta_neutral import (
    ActiveBetaNeutralPair,
    BetaNeutralStrategy,
)

__all__ = [
    "OrderbookImbalanceStrategy",
    "ActiveImbalancePosition",
    "BetaNeutralStrategy",
    "ActiveBetaNeutralPair",
]
