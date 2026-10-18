"""
venues/bingx.py — BingX Swap REST Client and WebSocket depth20 Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class BingXRestClient:
    """REST Client for BingX Swap discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://open-api.bingx.com/openApi/swap"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns symbol (e.g. 'BTC-USDT') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/v2/quote/contracts"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("-USDT"):
                        continue
                    base_coin = normalize_symbol(item.get("asset", "") or sym)
                    out[sym] = (base_coin, 1.0)
        except Exception as e:
            log.error(f"[bingx_rest] Failed to fetch contracts: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns symbol -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/v2/quote/ticker"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("-USDT"):
                        continue
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    vol_usd = float(item.get("volume", 0.0) or 0.0) * price
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[bingx_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        sym = symbol if "-" in symbol else f"{normalize_symbol(symbol)}-USDT"
        url = f"{self.BASE_URL}/v3/quote/klines?symbol={sym}&interval=1m&limit={min(limit, 1000)}"
        out: Dict[int, Tuple[float, float]] = {}
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = (int(row.get("time", 0)) // 60000) * 60
                    close = float(row.get("close", 0.0))
                    vol_usd = float(row.get("volume", 0.0)) * close
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class BingXStream(BaseCEXStream):
    """BingX Swap WebSocket order book client."""

    WS_URL = "wss://open-api-swap.bingx.com/swap-market"
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
    ):
        super().__init__("bingx", symbols, callback, stream_name=stream_name)

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send("ping")

    async def _send_subscriptions(self, ws: Any) -> None:
        for s in self.symbols:
            coin = self._base_coin(s)
            sub = {
                "id": f"sub_{coin}",
                "reqType": "sub",
                "dataType": f"{coin}-USDT@depth20",
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.02)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            datatype = data.get("dataType", "")
            if "@depth" not in datatype:
                return
            sym_raw = datatype.split("@")[0]
            sym = self._base_coin(sym_raw)
            b_data = data.get("data", {})
            bids = self._extract_levels(b_data.get("bids"), 5)
            asks = self._extract_levels(b_data.get("asks"), 5)
            if sym and bids and asks:
                self._note_symbol(sym)
                self.callback(OrderBookDepth5(
                    exchange="bingx",
                    symbol=sym,
                    raw_symbol=sym_raw,
                    ts=time.time(),
                    exchange_ts=int(b_data.get("ts", 0) or time.time() * 1000),
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
