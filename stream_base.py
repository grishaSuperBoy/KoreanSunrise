"""
stream_base.py — Legacy compatibility wrapper.
All functionality is now maintained modularly in venues.base.
"""
from venues.base import (
    BaseVenueStream,
    create_http_session,
    normalize_symbol,
    _secs_until_daily_reconnect_for,
    _reconnect_jitter_sec,
    _secs_until_daily_reconnect,
    log,
)

__all__ = [
    "BaseVenueStream",
    "create_http_session",
    "normalize_symbol",
    "_secs_until_daily_reconnect_for",
    "_reconnect_jitter_sec",
    "_secs_until_daily_reconnect",
    "log",
]