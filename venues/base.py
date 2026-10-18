"""
venues/base.py — Core base classes and utilities for venue REST and WebSocket connections.
Provides BaseVenueStream and BaseCEXStream with auto-reconnection, daily reconnect windows,
gzip decompression, level extraction helpers, and HTTP session management.
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
from requests.adapters import HTTPAdapter
import websockets

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
    pool_connections: int = 35,
    pool_maxsize: int = 35,
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.connected = False

    async def _run_loop(self) -> None:
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


class BaseCEXStream(BaseVenueStream):
    """
    Generic base class for Centralized Exchange WebSocket feeds.
    Provides auto-reconnection, heartbeat ping loops, binary gzip decompression,
    starvation detection, and level extraction helpers.
    """

    WS_URL: str = ""
    PING_INTERVAL_SEC: float = 15.0

    def __init__(
        self,
        exchange: str,
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
    ):
        super().__init__(exchange, symbols, callback)
        self.venue = exchange.lower()
        if stream_name:
            self.name = stream_name.lower()
        self.ws_url = self.WS_URL
        self.subscribed = list(self.symbols)

    @staticmethod
    def _base_coin(s: str) -> str:
        return normalize_symbol(s)

    @staticmethod
    def _extract_levels(raw_side: Any, limit: int = 5) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        for row in raw_side or ():
            if len(out) >= limit:
                break
            try:
                if isinstance(row, (list, tuple)) and len(row) >= 2:
                    p, q = float(row[0]), float(row[1])
                elif isinstance(row, dict):
                    p = float(row.get("p") or row.get("price") or row.get("px") or 0.0)
                    q = float(
                        row.get("q")
                        or row.get("s")
                        or row.get("m")
                        or row.get("size")
                        or row.get("qty")
                        or row.get("amount")
                        or row.get("v")
                        or row.get("vol")
                        or 0.0
                    )
                else:
                    continue
                if p > 0.0 and q > 0.0:
                    out.append((p, q))
            except (TypeError, ValueError, IndexError):
                continue
        return out

    def _handle_sub_reply(self, data: Any) -> None:
        """Hook to suppress subscription confirmation messages."""
        pass

    async def _send_subscriptions(self, ws: Any) -> None:
        raise NotImplementedError

    async def _send_client_ping(self, ws: Any) -> None:
        pass

    async def _handle_server_ping(self, ws: Any, raw_msg: str) -> bool:
        return False

    def _parse_message(self, raw_msg: str) -> None:
        raise NotImplementedError

    async def _ping_loop(self, ws: Any) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.PING_INTERVAL_SEC)
                await self._send_client_ping(ws)
        except Exception:
            pass

    async def _run_loop(self) -> None:
        if not self.ws_url:
            log.warning(f"[{self.name}] No WS endpoint configured.")
            return

        while self._running:
            try:
                log.info(f"[{self.name}] Connecting to {self.ws_url} ({len(self.symbols)} symbols)...")
                async with websockets.connect(
                    self.ws_url,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=2000,
                ) as ws:
                    self._note_connect()
                    log.info(f"[{self.name}] Connected! Sending subscriptions...")
                    await self._send_subscriptions(ws)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info(f"[{self.name}] Scheduled daily reconnect. Reconnecting...")
                                break

                            if not self.last_msg_ts:
                                self.last_msg_ts = time.time()
                            elif time.time() - self.last_msg_ts > 180.0:
                                log.warning(f"[{self.name}] No data for >180s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(30.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            # Automatic GZIP decompression for compressed streams (HTX, BingX)
                            if isinstance(msg, bytes):
                                try:
                                    msg = gzip.decompress(msg).decode("utf-8")
                                except Exception:
                                    msg = msg.decode("utf-8", errors="ignore")

                            if isinstance(msg, str):
                                s_clean = msg.strip().strip('"').lower()
                                if s_clean == "ping":
                                    await ws.send("pong")
                                    self.last_msg_ts = time.time()
                                    continue
                                if s_clean == "pong":
                                    self.last_msg_ts = time.time()
                                    continue
                                if "ping" in s_clean or "pong" in s_clean or '"time"' in s_clean:
                                    if await self._handle_server_ping(ws, msg):
                                        self.last_msg_ts = time.time()
                                        continue

                            self.last_msg_ts = time.time()
                            self._parse_message(msg)
                    finally:
                        ping_task.cancel()
                        self.connected = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                err_str = str(e)
                delay = 30.0 if ("403" in err_str or "11001" in err_str or "getaddrinfo" in err_str) else 3.0
                log.warning(f"[{self.name}] WS disconnected: {e}. Reconnecting in {delay}s...")
                await asyncio.sleep(delay)
