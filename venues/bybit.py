"""
venues/bybit.py — Bybit Linear Perps REST Client and WebSocket L2 Stream.
Normalizes 1000x/1M perpetual contracts to single-unit prices and quantities.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    ReconnectPolicy,
    create_http_session,
    normalize_symbol,
    parse_symbol_and_mult,
    _secs_until_daily_reconnect_for,
    log,
)


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
                    base_coin, mult = parse_symbol_and_mult(base_coin)
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
        limit: int = 1440,
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
    WATCHDOG_SECS = 90.0

    def __init__(
        self,
        sym_map: Dict[str, Tuple[str, float]],
        callback: Callable[[OrderBookDepth5], None],
    ):
        coins = sorted({v[0] for v in sym_map.values()})
        super().__init__("bybit", coins, callback)
        self._sym_map: Dict[str, Tuple[Any, ...]] = dict(sym_map)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())
        self.subscribed = list(coins)
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}
        self._lock = threading.RLock()
        self._last_u: Dict[str, int] = {}
        self._last_seq: Dict[str, int] = {}
        self._synced: Dict[str, bool] = {}

    def get_book_snapshot(self, raw_sym: str) -> Optional[Dict[str, Dict[float, float]]]:
        """Thread-safe snapshot copy of the current L2 book for raw_sym."""
        with self._lock:
            b = self._books.get(raw_sym)
            if not b:
                return None
            return {"bids": dict(b["bids"]), "asks": dict(b["asks"])}

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
                with self._lock:
                    self._books.clear()
                    self._last_u.clear()
                    self._last_seq.clear()
                    self._synced.clear()
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

                            if self._starved_since_connect(time.time()):
                                log.warning("[bybit] Connected but no data in 30s, reconnecting...")
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
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[bybit] WS disconnected: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            data = json.loads(raw_msg)
            if data.get("op") == "subscribe" or "success" in data or "ret_msg" in data:
                self._handle_sub_reply(data)

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
            coin = mapped[0]
            mult = float(mapped[1])
            c_size = float(mapped[2]) if len(mapped) >= 3 else 1.0

            mtype = data.get("type", "delta")
            u = int(d.get("u") or 0)
            seq = int(d.get("seq") or 0)

            with self._lock:
                cur_book = self._books.get(raw_sym)
                is_desynced = (self._synced.get(raw_sym) is False)
                if mtype == "snapshot" or (cur_book is None and not is_desynced):
                    self._synced[raw_sym] = True
                    self._last_u[raw_sym] = u
                    if seq > 0:
                        self._last_seq[raw_sym] = seq
                    new_bids: Dict[float, float] = {}
                    new_asks: Dict[float, float] = {}
                else:
                    if is_desynced:
                        # Waiting for a fresh snapshot after gap
                        self.seq_gaps += 1
                        return

                    last_u = self._last_u.get(raw_sym, 0)
                    if u > 0 and last_u > 0:
                        if u <= last_u:
                            self.seq_duplicates += 1
                            return
                        pu = d.get("pu")
                        if pu is not None and int(pu) != last_u:
                            self.seq_gaps += 1
                            log.warning(f"[bybit] Sequence gap on {raw_sym}: expected pu={last_u}, got {pu}. Desyncing book.")
                            self._synced[raw_sym] = False
                            self._books.pop(raw_sym, None)
                            return

                    self._last_u[raw_sym] = u
                    if seq > 0:
                        self._last_seq[raw_sym] = seq

                    new_bids = dict(cur_book.get("bids", {}))
                    new_asks = dict(cur_book.get("asks", {}))

                for row in d.get("b", []):
                    p, q = float(row[0]), float(row[1])
                    if q <= 0.0 or p <= 0.0:
                        new_bids.pop(p, None)
                    else:
                        new_bids[p] = q

                for row in d.get("a", []):
                    p, q = float(row[0]), float(row[1])
                    if q <= 0.0 or p <= 0.0:
                        new_asks.pop(p, None)
                    else:
                        new_asks[p] = q

                if not new_bids or not new_asks:
                    return

                sorted_bids_raw = sorted(new_bids.items(), key=lambda x: x[0], reverse=True)
                sorted_asks_raw = sorted(new_asks.items(), key=lambda x: x[0])

                # Memory leak / GC protection: strictly cap depth to MAX_BOOK_DEPTH (25)
                # whenever dictionary size exceeds 25, instead of letting it balloon to 100+
                MAX_BOOK_DEPTH = 25
                if len(new_bids) > MAX_BOOK_DEPTH:
                    new_bids = dict(sorted_bids_raw[:MAX_BOOK_DEPTH])
                if len(new_asks) > MAX_BOOK_DEPTH:
                    new_asks = dict(sorted_asks_raw[:MAX_BOOK_DEPTH])

                # Atomic Copy-on-Write assignment
                self._books[raw_sym] = {"bids": new_bids, "asks": new_asks}

                top_bids = sorted_bids_raw[:5]
                top_asks = sorted_asks_raw[:5]

            if not top_bids or not top_asks:
                return

            inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
            scale_qty = mult * c_size
            if inv_mult != 1.0 or scale_qty != 1.0:
                norm_bids = [(p * inv_mult, q * scale_qty) for p, q in top_bids]
                norm_asks = [(p * inv_mult, q * scale_qty) for p, q in top_asks]
            else:
                norm_bids = [(p, q) for p, q in top_bids]
                norm_asks = [(p, q) for p, q in top_asks]

            self.callback(
                OrderBookDepth5(
                    exchange="bybit",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=normalize_exchange_ts(d.get("cts") or data.get("ts")) or int(time.time() * 1000),
                    bids=norm_bids,
                    asks=norm_asks,
                )
            )
        except Exception:
            pass
