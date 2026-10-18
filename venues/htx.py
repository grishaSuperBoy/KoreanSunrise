"""
venues/htx.py — HTX (Huobi) Linear Swap REST Client and WebSocket depth.step0 Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests

from models import OrderBookDepth5
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class HTXRestClient:
    """REST Client for HTX Linear Swap discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.hbdm.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns contract_code (e.g. 'BTC-USDT') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/linear-swap-api/v1/swap_contract_info"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    code = item.get("contract_code", "").upper()
                    if not code.endswith("-USDT"):
                        continue
                    base_coin = normalize_symbol(item.get("symbol", "") or code)
                    cs = float(item.get("contract_size", 1.0) or 1.0)
                    out[code] = (base_coin, 1.0, cs)
        except Exception as e:
            log.error(f"[htx_rest] Failed to fetch contract info: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns contract_code (e.g. 'BTC-USDT') -> contract_size (float)."""
        url = f"{self.BASE_URL}/linear-swap-api/v1/swap_contract_info"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    code = item.get("contract_code", "").upper()
                    if not code.endswith("-USDT"):
                        continue
                    cs = float(item.get("contract_size", 1.0) or 1.0)
                    out[code] = cs
        except Exception as e:
            log.error(f"[htx_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns contract_code -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/linear-swap-ex/market/detail/batch_merged"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("ticks", []):
                    code = item.get("contract_code", "").upper()
                    if not code.endswith("-USDT"):
                        continue
                    price = float(item.get("close", 0.0) or 0.0)
                    vol_usd = float(item.get("trade_turnover", 0.0) or 0.0)
                    if price > 0:
                        out[code] = {"venue_sym": code, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[htx_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        code = symbol if "-" in symbol else f"{normalize_symbol(symbol)}-USDT"
        url = f"{self.BASE_URL}/linear-swap-ex/market/history/kline?contract_code={code}&period=1min&size={min(limit, 2000)}"
        out: Dict[int, Tuple[float, float]] = {}
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = int(row.get("id", 0))
                    close = float(row.get("close", 0.0))
                    vol_usd = float(row.get("trade_turnover", 0.0))
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class HTXStream(BaseCEXStream):
    """HTX Linear Swap WebSocket order book client."""

    WS_URL = "wss://api.hbdm.com/linear-swap-ws"
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("htx", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    async def _handle_server_ping(self, ws: Any, raw_msg: str) -> bool:
        try:
            d = json.loads(raw_msg)
            if isinstance(d, dict) and "ping" in d:
                await ws.send(json.dumps({"pong": d["ping"]}))
                return True
        except Exception:
            pass
        return False

    async def _send_subscriptions(self, ws: Any) -> None:
        if not self._contract_sizes:
            try:
                sizes = await asyncio.to_thread(HTXRestClient().fetch_contract_sizes)
                if sizes:
                    self._contract_sizes.update(sizes)
            except Exception as e:
                log.warning(f"[htx] Failed to auto-fetch contract sizes: {e}")

        for s in self.symbols:
            code = s if "-" in s else f"{self._base_coin(s)}-USDT"
            sub = {
                "sub": f"market.{code}.depth.step0",
                "id": f"sub_{code}",
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.015)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            ch = data.get("ch", "")
            if not ch.startswith("market.") or ".depth." not in ch:
                return
            code = ch.split(".")[1].upper()
            sym, mult = self._resolve_coin_and_mult(code)
            contract_size = self._get_contract_size(code)
            tick = data.get("tick", {})
            bids = self._extract_levels(tick.get("bids"), 5, mult=mult, contract_size=contract_size)
            asks = self._extract_levels(tick.get("asks"), 5, mult=mult, contract_size=contract_size)
            if bids and asks:
                self._note_symbol(sym)
                ets = int(data.get("ts", 0) or tick.get("ts", 0))
                self.callback(OrderBookDepth5(
                    exchange="htx",
                    symbol=sym,
                    raw_symbol=code,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
