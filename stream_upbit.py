"""
stream_upbit.py — Legacy compatibility wrapper.
All functionality is now maintained modularly in venues.upbit.
"""
from venues.upbit import UpbitRestClient, UpbitSpotStream

__all__ = ["UpbitRestClient", "UpbitSpotStream"]