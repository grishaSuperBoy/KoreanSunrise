"""
venues/asterdex.py — Aster DEX REST Client and WebSocket depth5@100ms Stream.
Aster DEX is a high-liquidity perpetual DEX (built on ApolloX/APX Binance Cloud protocol).
REST and WebSocket APIs are 100% Binance-Futures-compatible.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseCEXStream,
    create_http_session,
    normalize_symbol,
    parse_symbol_and_mult,
    log,
)


class AsterdexRestClient:
    """REST Client for Aster DEX linear perpetuals discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://fapi.asterdex.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float, float]]:
        """
        Fetches all active USDT Linear Perpetuals from Aster DEX.
        Returns: raw_symbol (e.g. '1000PEPEUSDT') -> (canonical_coin 'PEPE', multiplier 1000.0, contract_size 1.0).
        """
        url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
        out: Dict[str, Tuple[str, float, float]] = {}
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
                        raw_sym = s.get("symbol", "").upper()
                        base_asset = s.get("baseAsset", "").upper()
                        coin, mult = parse_symbol_and_mult(base_asset)
                        c_size = float(s.get("contractSize") or s.get("contract_size") or 1.0)
                        out[raw_sym] = (coin, mult, c_size)
        except Exception as e:
            log.warning(f"[asterdex_rest] Failed to fetch exchangeInfo: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """
        Fetches contract_size for active symbols.
        Returns raw_symbol -> contract_size float.
        """
        url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for s in data.get("symbols", []):
                    raw_sym = s.get("symbol", "").upper()
                    c_size = float(s.get("contractSize") or s.get("contract_size") or 1.0)
                    out[raw_sym] = c_size
        except Exception as e:
            log.warning(f"[asterdex_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24-hour ticker statistics for Aster DEX perps.
        Returns: raw_symbol -> {'venue_sym': sym, 'price': float, 'vol_24h': float}.
        """
        url = f"{self.BASE_URL}/fapi/v1/ticker/24hr"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=10)
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
            log.warning(f"[asterdex_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        symbol: str,
        limit: int = 200,
        mult: float = 1.0,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to limit 1m klines from Aster DEX.
        Returns: minute_ts_sec -> (normalized_close_usd, quote_volume_usd).
        """
        url = f"{self.BASE_URL}/fapi/v1/klines?symbol={symbol}&interval=1m&limit={min(limit, 1000)}"
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


class AsterdexStream(BaseCEXStream):
    """
    Subscribes to depth5@100ms on Aster DEX USDT Linear Perps via WebSocket.
    Uses Binance-compatible SUBSCRIBE method and depthUpdate stream format.
    """

    WS_URL = "wss://fstream.asterdex.com/ws"
    CHUNK_SIZE = 50

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("asterdex", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())

    async def _send_subscriptions(self, ws: Any) -> None:
        if not self._raw_symbols:
            return

        streams = [f"{s.lower()}@depth5@100ms" for s in self._raw_symbols]
        for i in range(0, len(streams), self.CHUNK_SIZE):
            chunk = streams[i : i + self.CHUNK_SIZE]
            sub_msg = {
                "method": "SUBSCRIBE",
                "params": chunk,
                "id": (i // self.CHUNK_SIZE) + 1,
            }
            await ws.send(json.dumps(sub_msg))
            await asyncio.sleep(0.05)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            payload = json.loads(raw_msg)
            if payload.get("id") is not None and "result" in payload:
                self._handle_sub_reply(payload)
                return

            data = payload.get("data", payload)
            if data.get("e") != "depthUpdate":
                return

            raw_sym = str(data.get("s", "")).upper()
            if not raw_sym:
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
                            f"[asterdex] Sequence gap for {raw_sym}: expected pu={last_u}, got {prev_u} (u={curr_u}). Dropping gap tick."
                        )
                        self._last_u[raw_sym] = curr_u
                        return
                    if curr_u <= last_u:
                        self.seq_duplicates += 1
                        return
                self._last_u[raw_sym] = curr_u

            coin, mult = self._resolve_coin_and_mult(raw_sym)
            c_size = self._get_contract_size(raw_sym)
            exchange_ts = normalize_exchange_ts(data.get("E")) or int(time.time() * 1000)

            raw_bids = data.get("b", [])
            raw_asks = data.get("a", [])

            bids = self._extract_levels(raw_bids, limit=5, mult=mult, contract_size=c_size)
            asks = self._extract_levels(raw_asks, limit=5, mult=mult, contract_size=c_size)

            if not bids or not asks:
                return

            if coin not in self.symbols_seen:
                self._note_sub(True)
            snap = OrderBookDepth5(
                exchange="asterdex",
                symbol=coin,
                raw_symbol=raw_sym,
                ts=time.time(),
                exchange_ts=exchange_ts,
                bids=bids,
                asks=asks,
            )
            self.callback(snap)
        except Exception:
            pass
