"""
venues/base.py — Core base classes and utilities for venue REST and WebSocket connections.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Callable, List, Optional, Set
import requests
from requests.adapters import HTTPAdapter

from models import OrderBookDepth5

log = logging.getLogger("stream")

_DAILY_RECONNECT_HOUR = 2
_DAILY_RECONNECT_MINUTE = 23
_RECONNECT_SPREAD_SEC = 7 * 60

_SEPS = "-_/:"
_QUOTES = ("USDT", "USDC", "USD", "KRW")
_CONTRACT_SUFFIXES = ("PERP", "SWAP")


def normalize_symbol(raw: str) -> str:
    """Canonical base coin symbol normalizer."""
    s_raw = str(raw).strip()
    if len(s_raw) > 1 and s_raw[0] == "k" and s_raw[1].isupper():
        s_raw = "1000" + s_raw[1:]
    s = s_raw.upper()
    if ":" in s:
        s = s.split(":", 1)[0]
    for sep in _SEPS:
        s = s.replace(sep, "")
    for suf in _CONTRACT_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
    for q in _QUOTES:
        if s.endswith(q) and len(s) > len(q):
            s = s[:-len(q)]
            break
    for suf in _CONTRACT_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
    return s


def create_http_session(
    pool_connections: int = 25,
    pool_maxsize: int = 25,
    max_retries: int = 2,
    timeout: float = 8.0,
) -> requests.Session:
    """Creates a pre-configured requests.Session with connection pooling and headers."""
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=max_retries,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; KoreanSunrise/2.0; +https://github.com/grishaSuperBoy/KoreanSunrise)",
        "Accept": "application/json",
    })
    return session


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
                recv_ts=snap.recv_ts or time.time(),
                network_latency_ms=snap.network_latency_ms,
                bids=bids[:5],
                asks=asks[:5],
            )
            self._raw_callback(clean_snap)

        except (ValueError, TypeError, IndexError):
            self.levels_dropped += 1

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    def get_status(self) -> dict:
        return {
            "name": self.name,
            "connected": self.connected,
            "symbols_seen": len(self.symbols_seen),
            "subscribed": len(self.subscribed),
            "last_data_ago_sec": (
                round(time.time() - self.last_data_ts, 1)
                if self.last_data_ts else None
            ),
            "books_dropped": self.books_dropped,
            "levels_dropped": self.levels_dropped,
        }
