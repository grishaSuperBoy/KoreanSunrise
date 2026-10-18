"""
venues/gateio.py — Gate.io Futures REST Client and WebSocket order_book_update Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, parse_symbol_and_mult, log


class GateioRestClient:
    """REST Client for Gate.io Futures discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://fx-api.gateio.ws/api/v4/futures/usdt"

    def __init__(self, session: Optional[requests.Session] = None):
        # Always use dedicated session so popping User-Agent does not mutate other venues' sessions
        self.session = create_http_session()
        # Gate.io API WAF rejects fake browser User-Agents with 403; default requests UA works
        self.session.headers.pop("User-Agent", None)

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns contract (e.g. 'BTC_USDT', '1000BONK_USDT') -> (canonical_coin 'BTC', mult 1.0 or 1000.0)."""
        url = f"{self.BASE_URL}/contracts"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                for item in r.json():
                    contract = item.get("name", "").upper()
                    if not contract.endswith("_USDT"):
                        continue
                    base_coin, mult = parse_symbol_and_mult(contract)
                    quanto = float(item.get("quanto_multiplier", 1.0) or 1.0)
                    out[contract] = (base_coin, mult, quanto)
        except Exception as e:
            log.error(f"[gateio_rest] Failed to fetch contracts: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns contract (e.g. 'BTC_USDT') -> quanto_multiplier (e.g. 0.0001)."""
        url = f"{self.BASE_URL}/contracts"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                for item in r.json():
                    contract = item.get("name", "").upper()
                    if not contract.endswith("_USDT"):
                        continue
                    quanto = float(item.get("quanto_multiplier", 1.0) or 1.0)
                    out[contract] = quanto
        except Exception as e:
            log.error(f"[gateio_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns contract -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/tickers"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                for item in r.json():
                    contract = item.get("contract", "")
                    if not contract.endswith("_USDT"):
                        continue
                    price = float(item.get("last", 0.0) or 0.0)
                    vol_usd = float(item.get("volume_24h_quote", 0.0) or 0.0)
                    if price > 0:
                        out[contract] = {"venue_sym": contract, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[gateio_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        contract = symbol if "_" in symbol else f"{normalize_symbol(symbol)}_USDT"
        url = f"{self.BASE_URL}/candlesticks?contract={contract}&interval=1m&limit={min(limit, 1000)}"
        out: Dict[int, Tuple[float, float]] = {}
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json():
                    t_sec = (int(row.get("t", 0)) // 60) * 60
                    close = float(row.get("c", 0.0))
                    vol_usd = float(row.get("sum", 0.0))
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class GateioStream(BaseCEXStream):
    """Gate.io Futures WebSocket order book client."""

    WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols,
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("gateio", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}

    def _to_raw_symbol(self, s: str) -> str:
        s_u = s.upper()
        if s_u.endswith("_USDT"):
            return s_u
        for raw, (coin, _) in self._sym_map.items():
            if coin.upper() == s_u:
                return raw
        return f"{s_u}_USDT"

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.ping"}))

    async def _send_subscriptions(self, ws: Any) -> None:
        self._books.clear()
        if not self._contract_sizes:
            try:
                sizes = await asyncio.to_thread(GateioRestClient().fetch_contract_sizes)
                if sizes:
                    self._contract_sizes.update(sizes)
            except Exception as e:
                log.warning(f"[gateio] Failed to auto-fetch contract sizes: {e}")

        for s in self.symbols:
            pair = self._to_raw_symbol(s)
            sub = {
                "time": int(time.time()),
                "channel": "futures.order_book_update",
                "event": "subscribe",
                "payload": [pair, "100ms", "20"],
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.01)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            channel = data.get("channel", "")
            if "order_book" not in channel:
                return
            result = data.get("result", {})
            contract = (result.get("contract", "") or result.get("s", "")).upper()
            if not contract:
                return
            sym, mult = self._resolve_coin_and_mult(contract)
            contract_size = self._get_contract_size(contract)

            event = data.get("event")
            if contract not in self._books or event == "all":
                self._books[contract] = {"bids": {}, "asks": {}}

            book = self._books[contract]
            bids_dict = book["bids"]
            asks_dict = book["asks"]

            for row in result.get("b", []):
                p, s = float(row.get("p", 0.0)), float(row.get("s", 0.0))
                q = s * contract_size
                if q <= 0.0 or p <= 0.0:
                    bids_dict.pop(p, None)
                else:
                    bids_dict[p] = q

            for row in result.get("a", []):
                p, s = float(row.get("p", 0.0)), float(row.get("s", 0.0))
                q = s * contract_size
                if q <= 0.0 or p <= 0.0:
                    asks_dict.pop(p, None)
                else:
                    asks_dict[p] = q

            if not bids_dict or not asks_dict:
                return

            sorted_bids_raw = sorted(bids_dict.items(), key=lambda x: x[0], reverse=True)
            sorted_asks_raw = sorted(asks_dict.items(), key=lambda x: x[0])

            # Prevent unbounded memory growth on volatile price moves
            if len(bids_dict) > 100:
                book["bids"] = dict(sorted_bids_raw[:50])
            if len(asks_dict) > 100:
                book["asks"] = dict(sorted_asks_raw[:50])

            if mult != 1.0:
                inv_mult = 1.0 / mult
                top_bids = [(p * inv_mult, q * mult) for p, q in sorted_bids_raw[:5]]
                top_asks = [(p * inv_mult, q * mult) for p, q in sorted_asks_raw[:5]]
            else:
                top_bids = sorted_bids_raw[:5]
                top_asks = sorted_asks_raw[:5]

            if top_bids and top_asks:
                ets = int(result.get("t") or data.get("time_ms") or ((data.get("time") or time.time()) * 1000))
                self.callback(OrderBookDepth5(
                    exchange="gateio",
                    symbol=sym,
                    raw_symbol=contract,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=top_bids,
                    asks=top_asks,
                ))
        except Exception as e:
            log.debug(f"[gateio] parse error: {e}")
