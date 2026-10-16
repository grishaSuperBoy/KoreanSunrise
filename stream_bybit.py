"""
stream_bybit.py - Bybit Linear Perps depth WebSocket client.
Normalizes 1000x/1M perpetual contracts to single-unit prices and quantities.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Tuple
import websockets

from models import OrderBookDepth5
from stream_base import (
    BaseVenueStream,
    _secs_until_daily_reconnect_for,
    log,
)


class BybitPerpStream(BaseVenueStream):
    """Subscribes to linear orderbook.50 on Bybit and emits normalized top-5 snapshots."""

    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    WATCHDOG_SECS = 30.0

    def __init__(
        self,
        sym_map: Dict[str, Tuple[str, float]],
        callback: Callable[[OrderBookDepth5], None],
    ):
        # sym_map: raw_contract (e.g. "1000PEPEUSDT") -> (canonical_coin "PEPE", multiplier 1000.0)
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

                    # Bybit V5 public linear allows up to 10 args per subscribe call reliably,
                    # or chunks up to 25 without hitting payload/rate limits.
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

            # Prune local book if delta drift accumulated > 100 levels
            if len(bids_dict) > 100:
                book["bids"] = dict(sorted_bids_raw[:50])
            if len(asks_dict) > 100:
                book["asks"] = dict(sorted_asks_raw[:50])

            top_bids = sorted_bids_raw[:5]
            top_asks = sorted_asks_raw[:5]
            if not top_bids or not top_asks:
                return

            # Normalize price and quantity if contract has a multiplier (e.g. 1000PEPEUSDT)
            if mult != 1.0:
                inv_mult = 1.0 / mult
                norm_bids = [(p * inv_mult, q * mult) for p, q in top_bids]
                norm_asks = [(p * inv_mult, q * mult) for p, q in top_asks]
            else:
                norm_bids = top_bids
                norm_asks = top_asks

            self.callback(
                OrderBookDepth5(
                    exchange="bybit",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=int(data.get("ts") or (time.time() * 1000)),
                    bids=norm_bids,
                    asks=norm_asks,
                )
            )
        except Exception:
            pass