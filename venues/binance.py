"""
venues/binance.py — Binance Futures REST Client and WebSocket depth5@100ms Stream.
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

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    ReconnectPolicy,
    create_http_session,
    normalize_symbol,
    _secs_until_daily_reconnect_for,
    log,
)

_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")


class BinanceRestClient:
    """REST Client for Binance Futures discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://fapi.binance.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """
        Fetches all active USDT Linear Perpetuals from Binance.
        Returns: raw_symbol (e.g. '1000PEPEUSDT') -> (canonical_coin 'PEPE', multiplier 1000.0).
        """
        url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for s in data.get("symbols", []):
                    if (
                        s.get("contractType") == "PERPETUAL"
                        and s.get("status") == "TRADING"
                        and s.get("quoteAsset") == "USDT"
                    ):
                        raw_sym = s["symbol"].upper()
                        base_asset = s.get("baseAsset", "").upper()
                        mult = 1.0
                        m = _MULT_PREFIX_RE.match(base_asset)
                        if m:
                            mult = float(m.group(1))
                            base_asset = m.group(2)
                        out[raw_sym] = (base_asset, mult)
        except Exception as e:
            log.error(f"[binance_rest] Failed to fetch exchangeInfo: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24h ticker metrics for all symbols in a single request.
        Returns raw_symbol -> {price, vol_24h}.
        """
        url = f"{self.BASE_URL}/fapi/v1/ticker/24hr"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json():
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("USDT"):
                        continue
                    vol = float(item.get("quoteVolume", 0.0) or 0.0)
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[binance_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        symbol: str,
        limit: int = 200,
        mult: float = 1.0,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to 200 1m klines.
        Returns dict: minute_ts_sec -> (normalized_close_usd, quote_volume_usd).
        """
        url = f"{self.BASE_URL}/fapi/v1/klines?symbol={symbol}&interval=1m&limit={min(limit, 1500)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json():
                    t_sec = (int(row[0]) // 60000) * 60
                    close = float(row[4]) * inv_mult
                    quote_vol = float(row[7])
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, quote_vol)
        except Exception:
            pass
        return out


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
        coins = sorted({v[0] for v in sym_map.values()})
        super().__init__("binance", coins, callback)
        self._sym_map: Dict[str, Tuple[Any, ...]] = dict(sym_map)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())
        self.subscribed = list(coins)
        self._connected_chunks: Set[int] = set()
        self._chunk_policies: Dict[int, ReconnectPolicy] = {}

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
        policy = self._chunk_policies.setdefault(chunk_id, ReconnectPolicy(base_delay=2.0, max_delay=120.0))

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
                    policy.record_connected()
                    log.info(f"[{tag}] Connected! Streaming depth5@100ms...")
                    reconnect_in = _secs_until_daily_reconnect_for(f"{self.name}_{chunk_id}")
                    deadline = time.monotonic() + reconnect_in
                    chunk_connect_ts = time.time()
                    chunk_last_msg_ts = chunk_connect_ts
                    chunk_data_ts = 0.0

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info(f"[{tag}] Scheduled daily reconnect. Reconnecting...")
                                break

                            now = time.time()
                            if (now - chunk_connect_ts > 30.0) and (chunk_data_ts <= chunk_connect_ts):
                                log.warning(f"[{tag}] Connected but no data in 30s, reconnecting...")
                                break

                            if now - chunk_last_msg_ts > self.WATCHDOG_SECS:
                                log.warning(f"[{tag}] No messages for >{self.WATCHDOG_SECS:.0f}s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(15.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            recv_now = time.time()
                            chunk_last_msg_ts = recv_now
                            chunk_data_ts = recv_now
                            self.last_msg_ts = recv_now
                            self._parse_message(msg)
                    finally:
                        self._connected_chunks.discard(chunk_id)
                        self.connected = bool(self._connected_chunks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected_chunks.discard(chunk_id)
                self.connected = bool(self._connected_chunks)
                delay = policy.compute_delay(e)
                log.warning(
                    f"[{tag}] WS disconnected: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

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
            coin = mapped[0]
            mult = float(mapped[1])
            c_size = float(mapped[2]) if len(mapped) >= 3 else 1.0

            raw_bids = data.get("b", [])[:5]
            raw_asks = data.get("a", [])[:5]
            if not raw_bids or not raw_asks:
                return

            # Sequence gap / packet drop detection
            curr_u = int(data.get("u") or 0)
            prev_u = int(data.get("pu") or 0)
            if curr_u > 0:
                last_u = self._last_u.get(raw_sym)
                if last_u is not None and prev_u > 0:
                    if prev_u != last_u:
                        self.seq_gaps += 1
                        log.warning(
                            f"[binance] Sequence gap for {raw_sym}: expected pu={last_u}, got {prev_u} (u={curr_u}). Dropping gap tick."
                        )
                        self._last_u[raw_sym] = curr_u
                        return
                    if curr_u <= last_u:
                        self.seq_duplicates += 1
                        return
                self._last_u[raw_sym] = curr_u

            inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
            scale_qty = mult * c_size
            if inv_mult != 1.0 or scale_qty != 1.0:
                bids = [(float(p) * inv_mult, float(q) * scale_qty) for p, q in raw_bids]
                asks = [(float(p) * inv_mult, float(q) * scale_qty) for p, q in raw_asks]
            else:
                bids = [(float(p), float(q)) for p, q in raw_bids]
                asks = [(float(p), float(q)) for p, q in raw_asks]

            if coin not in self.symbols_seen:
                self._note_sub(True)
            self.callback(
                OrderBookDepth5(
                    exchange="binance",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=normalize_exchange_ts(data.get("E")) or int(time.time() * 1000),
                    bids=bids,
                    asks=asks,
                )
            )
        except Exception:
            pass
