"""
venues/mexc.py — MEXC Contract REST Client and WebSocket sub.depth.full Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class MEXCRestClient:
    """REST Client for MEXC Contract discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://contract.mexc.com/api/v1/contract"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float, float]]:
        """Returns symbol (e.g. 'BTC_USDT') -> (canonical_coin 'BTC', mult 1.0, contractSize)."""
        url = f"{self.BASE_URL}/detail"
        out: Dict[str, Tuple[str, float, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "")
                    if not sym.endswith("_USDT") and not sym.endswith("USDT"):
                        continue
                    base_coin = normalize_symbol(item.get("baseCoin", "") or sym)
                    cs = float(item.get("contractSize", 1.0) or 1.0)
                    out[sym] = (base_coin, 1.0, cs)
        except Exception as e:
            log.error(f"[mexc_rest] Failed to fetch contract details: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns symbol (e.g. 'BTC_USDT') -> contractSize (float)."""
        url = f"{self.BASE_URL}/detail"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("_USDT") and not sym.endswith("USDT"):
                        continue
                    cs = float(item.get("contractSize", 1.0) or 1.0)
                    out[sym] = cs
        except Exception as e:
            log.error(f"[mexc_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns symbol -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/ticker"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "")
                    if not sym.endswith("_USDT") and not sym.endswith("USDT"):
                        continue
                    vol = float(item.get("amount24", 0.0) or 0.0)
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[mexc_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        sym = symbol if "_" in symbol else f"{normalize_symbol(symbol)}_USDT"
        url = f"{self.BASE_URL}/kline/{sym}?interval=Min1"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                data = r.json().get("data", {})
                times = data.get("time", [])
                closes = data.get("close", [])
                amounts = data.get("amount", [])
                for t, c, a in zip(times[-limit:], closes[-limit:], amounts[-limit:]):
                    t_sec = (int(t) // 60) * 60
                    close = float(c) * inv_mult
                    vol_usd = float(a)
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class MEXCStream(BaseCEXStream):
    """MEXC Contract WebSocket order book client."""

    WS_URL = "wss://contract.mexc.com/edge"
    SHARD_SIZE = 80
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("mexc", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send('"Ping"')

    async def _send_subscriptions(self, ws: Any) -> None:
        if not self._contract_sizes:
            try:
                sizes = await asyncio.to_thread(MEXCRestClient().fetch_contract_sizes)
                if sizes:
                    self._contract_sizes.update(sizes)
            except Exception as e:
                log.warning(f"[mexc] Failed to auto-fetch contract sizes: {e}")

        for s in self.symbols:
            pair = s if "_" in s else f"{self._base_coin(s)}_USDT"
            sub = {
                "method": "sub.depth.full",
                "param": {"symbol": pair, "limit": 20},
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.012)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            channel = data.get("channel", "")
            if "depth" not in channel:
                return
            d = data.get("data", {})
            raw_sym = (data.get("symbol", "") or "").upper()
            if not raw_sym:
                return
            sym, mult = self._resolve_coin_and_mult(raw_sym)
            contract_size = self._get_contract_size(raw_sym)
            bids = self._extract_levels(d.get("bids"), 5, mult=mult, contract_size=contract_size)
            asks = self._extract_levels(d.get("asks"), 5, mult=mult, contract_size=contract_size)
            if bids and asks:
                self._note_symbol(sym)
                ets = normalize_exchange_ts(d.get("cts") or data.get("ts")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="mexc",
                    symbol=sym,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
