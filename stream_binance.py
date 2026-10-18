"""
stream_binance.py — Legacy compatibility wrapper.
All functionality is now maintained modularly in venues.binance.
"""
from venues.binance import BinanceRestClient, BinanceFuturesStream

__all__ = ["BinanceRestClient", "BinanceFuturesStream"]