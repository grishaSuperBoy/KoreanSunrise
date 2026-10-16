"""
stream_binance.py - Binance Futures depth5@100ms WebSocket client.
Normalizes 1000x/1M perpetual contracts to single-unit prices and quantities.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Callable, Dict, List, Set, Tuple
import websockets

from models import OrderBookDepth5
from stream_base import (
    BaseVenueStream,
    _secs_until_daily_reconnect_for,
    log,
)


class BinanceFuturesStream(BaseVenueStream):
    """Subscribes to depth5@100ms on Binance USDT Linear Perps."""

    BASE_WS_URL = "wss://fstream.binance.com/stream"
    WATCHDOG_SECS = 30.0
    MAX_STREAMS_PER_CONN = 180

    def __init__(
        self,
        sym_map: Dict[str, Tuple[str, float]],
        callback: Callable[[OrderBookDepth5], None],
    ):
        # sym_map: raw_contract (e.g. "1000PEPEUSDT") -> (canonical_coin "PEPE", multiplier 1000.0)
        coins = sorted({coin for coin, _ in sym_map.values()})
        super().__init__("binance", coins, callback)
        self._sym_map: Dict[str, Tuple[str, float]] = dict(sym_map)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())
        self.subscribed = list(coins)
        self._connected_chunks: Set[int] = set()

    async def _run_loop(self) -> None:
        if not self._raw_symbols:
            log.warning("[binance] No matched symbols to subscribe.")
            return

        chunks = [
            self._raw_symbols[i : i + self.MAX_STREAMS_PER_CONN]
            for i in range(0, len(self._raw_symbols), self.MAX_STREAMS_PER_CONN)
        ]

        if len(chunks) == 1:
            await self._run_single_chunk(chunks[0], chunk_id=0)
        else:
            tasks = [
                asyncio.create_task(self._run_single_chunk(chunk, idx))
                for idx, chunk in enumerate(chunks)
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                for t in tasks:
                    if not t.done():
                        t.cancel()

    async def _run_single_chunk(self, raw_syms: List[str], chunk_id: int) -> None:
        sub_streams = [f"{s.lower()}@depth5@100ms" for s in raw_syms]
        url = f"{self.BASE_WS_URL}?streams=" + "/".join(sub_streams)
        tag = f"binance#{chunk_id}" if chunk_id > 0 else "binance"

        while self._running:
            try:
                log.info(f"[{tag}] Connecting to stream ({len(raw_syms)} contracts)...")
                async with websockets.connect(
                    url,
                    open_timeout=20,
                    ping_interval=20,
                    ping_timeout=15,
                    max_queue=4096,
                ) as ws:
                    self._connected_chunks.add(chunk_id)
                    self._note_connect()
                    log.info(f"[{tag}] Connected! Streaming depth5@100ms...")
                    reconnect_in = _secs_until_daily_reconnect_for(f"{self.name}_{chunk_id}")
                    deadline = time.monotonic() + reconnect_in
                    chunk_last_msg_ts = time.time()

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info(f"[{tag}] Scheduled daily reconnect. Reconnecting...")
                                break

                            if time.time() - chunk_last_msg_ts > self.WATCHDOG_SECS:
                                log.warning(f"[{tag}] No messages for >{self.WATCHDOG_SECS:.0f}s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(15.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            chunk_last_msg_ts = time.time()
                            self.last_msg_ts = chunk_last_msg_ts
                            self._parse_message(msg)
                    finally:
                        self._connected_chunks.discard(chunk_id)
                        self.connected = bool(self._connected_chunks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected_chunks.discard(chunk_id)
                self.connected = bool(self._connected_chunks)
                log.warning(f"[{tag}] WS disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            payload = json.loads(raw_msg)
            data = payload.get("data", payload)
            raw_sym = data.get("s")
            if not raw_sym:
                return

            raw_sym = raw_sym.upper()
            mapped = self._sym_map.get(raw_sym)
            if not mapped:
                return
            coin, mult = mapped

            raw_bids = data.get("b", [])[:5]
            raw_asks = data.get("a", [])[:5]
            if not raw_bids or not raw_asks:
                return

            if mult != 1.0:
                inv_mult = 1.0 / mult
                bids = [(float(p) * inv_mult, float(q) * mult) for p, q in raw_bids]
                asks = [(float(p) * inv_mult, float(q) * mult) for p, q in raw_asks]
            else:
                bids = [(float(p), float(q)) for p, q in raw_bids]
                asks = [(float(p), float(q)) for p, q in raw_asks]

            self.callback(
                OrderBookDepth5(
                    exchange="binance",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=int(data.get("E") or (time.time() * 1000)),
                    bids=bids,
                    asks=asks,
                )
            )
        except Exception:
            pass