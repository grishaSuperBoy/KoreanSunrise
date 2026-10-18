"""
venues/bybit.py — Bybit Linear Perps REST Client and WebSocket L2 Stream.
Normalizes 1000x/1M perpetual contracts to single-unit prices and quantities.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5
from venues.base import (
    BaseVenueStream,
    create_http_session,
    normalize_symbol,
    _secs_until_daily_reconnect_for,
    log,
)

_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")


class BybitRestClient:
    """REST Client for Bybit Linear Perps discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.bybit.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """
        Fetches all active USDT Linear Perpetuals from Bybit.
        Returns: raw_symbol (e.g. '1000PEPEUSDT') -> (canonical_coin 'PEPE', multiplier 1000.0).
        """
        url = f"{self.BASE_URL}/v5/market/instruments-info?category=linear&status=Trading&limit=1000"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                items = data.get("result", {}).get("list", [])
                for item in items:
                    raw_sym = item.get("symbol", "").upper()
                    quote = item.get("quoteCoin", "").upper()
                    if quote != "USDT":
                        continue
                    base_coin = item.get("baseCoin", "").upper()
                    mult = 1.0
                    m = _MULT_PREFIX_RE.match(base_coin)
                    if m:
                        mult = float(m.group(1))
                        base_coin = m.group(2)
                    out[raw_sym] = (base_coin, mult)
        except Exception as e:
            log.error(f"[bybit_rest] Failed to fetch linear instruments: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24h tickers for all linear perps in a single call.
        Returns raw_symbol -> {price, vol_24h}.
        """
        url = f"{self.BASE_URL}/v5/market/tickers?category=linear"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                items = r.json().get("result", {}).get("list", [])
                for item in items:
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("USDT"):
                        continue
                    vol = float(item.get("turnover24h", 0.0) or 0.0)
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[bybit_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        symbol: str,
        limit: int = 200,
        mult: float = 1.0,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to 200 1m klines.
        Returns dict: minute_ts_sec -> (normalized_close_usd, turnover_usd).
        """
        url = f"{self.BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval=1&limit={min(limit, 1000)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                items = r.json().get("result", {}).get("list", [])
                for row in items:
                    t_sec = (int(row[0]) // 60000) * 60
                    close = float(row[4]) * inv_mult
                    turnover = float(row[6])
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, turnover)
        except Exception:
            pass
        return out


class BybitPerpStream(BaseVenueStream):
    """Subscribes to linear orderbook.50 on Bybit and emits normalized top-5 snapshots."""

    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    WATCHDOG_SECS = 30.0

    def __init__(
        self,
        sym_map: Dict[str, Tuple[str, float]],
        callback: Callable[[OrderBookDepth5], None],
    ):
        coins = sorted({coin for coin, _ in sym_map.values()})
        super().__init__("bybit", coins, callback)
        self._sym_map: Dict[str, Tuple[str, float]] = dict(sym_map)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())
        self.subscribed = list(coins)
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}

    async def _ping_loop(self, ws: Any) -> None:
        try:
            while self._running:
                await asyncio.sleep(20.0)
                await ws.send(json.dumps({"op": "ping"}))
        except Exception:
            pass

    async def _run_loop(self) -> None:
        if not self._raw_symbols:
            log.warning("[bybit] No matched symbols to subscribe.")
            return

        while self._running:
            try:
                self._books.clear()
                log.info(f"[bybit] Connecting to {self.WS_URL} ({len(self._raw_symbols)} contracts)...")
                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=4096,
                ) as ws:
                    self._note_connect()
                    log.info("[bybit] Connected! Sending subscriptions...")

                    chunk_size = 25
                    for i in range(0, len(self._raw_symbols), chunk_size):
                        chunk = self._raw_symbols[i : i + chunk_size]
                        args = [f"orderbook.50.{s}" for s in chunk]
                        await ws.send(json.dumps({"op": "subscribe", "args": args}))
                        await asyncio.sleep(0.05)

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[bybit] Scheduled daily reconnect. Reconnecting...")
                                break

                            if time.time() - self.last_msg_ts > self.WATCHDOG_SECS:
                                log.warning(f"[bybit] No messages for >{self.WATCHDOG_SECS:.0f}s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(15.0, remaining))
                            except asyncio.TimeoutError:
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
                log.warning(f"[bybit] WS disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            data = json.loads(raw_msg)
            topic = data.get("topic", "")
            if not topic.startswith("orderbook."):
                return

            d = data.get("data", {})
            raw_sym = d.get("s", "")
            if not raw_sym:
                return

            mapped = self._sym_map.get(raw_sym)
            if not mapped:
                return
            coin, mult = mapped

            mtype = data.get("type", "delta")
            if raw_sym not in self._books or mtype == "snapshot":
                self._books[raw_sym] = {"bids": {}, "asks": {}}
            book = self._books[raw_sym]
            bids_dict = book["bids"]
            asks_dict = book["asks"]

            for row in d.get("b", []):
                p, q = float(row[0]), float(row[1])
                if q <= 0.0 or p <= 0.0:
                    bids_dict.pop(p, None)
                else:
                    bids_dict[p] = q

            for row in d.get("a", []):
                p, q = float(row[0]), float(row[1])
                if q <= 0.0 or p <= 0.0:
                    asks_dict.pop(p, None)
                else:
                    asks_dict[p] = q

            if not bids_dict or not asks_dict:
                return

            sorted_bids_raw = sorted(bids_dict.items(), key=lambda x: x[0], reverse=True)
            sorted_asks_raw = sorted(asks_dict.items(), key=lambda x: x[0])

            if len(bids_dict) > 100:
                book["bids"] = dict(sorted_bids_raw[:50])
            if len(asks_dict) > 100:
                book["asks"] = dict(sorted_asks_raw[:50])

            top_bids = sorted_bids_raw[:5]
            top_asks = sorted_asks_raw[:5]
            if not top_bids or not top_asks:
                return

            if mult != 1.0:
                inv_mult = 1.0 / mult
                norm_bids = [(p * inv_mult, q * mult) for p, q in top_bids]
                norm_asks = [(p * inv_mult, q * mult) for p, q in top_asks]
            else:
                norm_bids = [(p, q) for p, q in top_bids]
                norm_asks = [(p, q) for p, q in top_asks]

            self.callback(
                OrderBookDepth5(
                    exchange="bybit",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=int(d.get("cts") or data.get("ts") or (time.time() * 1000)),
                    bids=norm_bids,
                    asks=norm_asks,
                )
            )
        except Exception:
            pass

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
