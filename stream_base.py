"""
stream_base.py - Base WebSocket feed infrastructure for Korean Sunrise.
Includes auto-reconnects, rate limits, and health telemetry.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, List, Optional, Set

from models import OrderBookDepth5

log = logging.getLogger("stream")

_DAILY_RECONNECT_HOUR = 2
_DAILY_RECONNECT_MINUTE = 23
_RECONNECT_SPREAD_SEC = 7 * 60


def _secs_until_daily_reconnect() -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(
        hour=_DAILY_RECONNECT_HOUR, minute=_DAILY_RECONNECT_MINUTE,
        second=0, microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _reconnect_jitter_sec(name: str) -> float:
    h = int(hashlib.sha256(name.encode("utf-8")).hexdigest()[:8], 16)
    span = 2 * _RECONNECT_SPREAD_SEC
    return float(h % span) - _RECONNECT_SPREAD_SEC


def _secs_until_daily_reconnect_for(name: str) -> float:
    return max(1.0, _secs_until_daily_reconnect() + _reconnect_jitter_sec(name))


class BaseVenueStream:
    """Base class for venue-specific WebSocket feeds."""

    def __init__(self, name: str, symbols: List[str], callback: Callable[[OrderBookDepth5], None]):
        self.name = name.lower()
        self.symbols = [s.upper() for s in symbols]
        self._raw_callback = callback
        self.callback = self._sanitise
        self.connected = False
        self.last_msg_ts = 0.0
        self.last_data_ts = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.subscribed: List[str] = []
        self.symbols_seen: Set[str] = set()
        self.books_dropped = 0
        self.levels_dropped = 0
        self._first_ts = 0.0

    def _note_connect(self) -> None:
        self.connected = True
        self.last_msg_ts = time.time()
        if not self._first_ts:
            self._first_ts = self.last_msg_ts

    def _note_symbol(self, sym: str) -> None:
        if sym:
            self.symbols_seen.add(str(sym).upper())
            self.last_data_ts = time.time()

    def _sanitise(self, snap: OrderBookDepth5) -> None:
        """Sanitise and filter crossed books before forwarding to engine."""
        try:
            bids = [
                (float(p), float(s))
                for p, s in snap.bids
                if float(p) > 0.0 and float(s) > 0.0
            ]
            asks = [
                (float(p), float(s))
                for p, s in snap.asks
                if float(p) > 0.0 and float(s) > 0.0
            ]
            if not bids or not asks:
                self.books_dropped += 1
                return

            # Reject inverted (crossed) books
            if bids[0][0] >= asks[0][0]:
                self.books_dropped += 1
                return

            clean_snap = OrderBookDepth5(
                exchange=snap.exchange,
                symbol=snap.symbol,
                raw_symbol=snap.raw_symbol,
                ts=snap.ts or time.time(),
                exchange_ts=snap.exchange_ts or 0,
                bids=bids[:5],
                asks=asks[:5],
            )
            self._note_symbol(clean_snap.symbol)
            self._raw_callback(clean_snap)
        except Exception:
            self.books_dropped += 1

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        task = self._task
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.connected = False

    async def _run_loop(self) -> None:
        raise NotImplementedError