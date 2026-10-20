"""
venues/pionex.py — Pionex WebSocket DEPTH stream and REST Client for Linear Perps.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, parse_symbol_and_mult, log


class PionexRestClient:
    """REST Client for Pionex Perpetual contracts, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.pionex.com/api/v1"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns symbol (e.g. 'BTC_USDT_PERP', '1000PEPE_USDT_PERP') -> (canonical_coin 'BTC', mult 1.0 or 1000.0)."""
        url = f"{self.BASE_URL}/common/symbols?type=PERP"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for s in r.json().get("data", {}).get("symbols", []):
                    sym = s.get("symbol", "").upper()
                    if not sym.endswith("_PERP") or "USDT" not in sym:
                        continue
                    base_coin, mult = parse_symbol_and_mult(sym)
                    if mult == 1.0:
                        base_curr = str(s.get("baseCurrency", "") or s.get("baseCoin", "")).upper()
                        if base_curr:
                            b_coin, b_mult = parse_symbol_and_mult(base_curr)
                            if b_mult != 1.0:
                                base_coin, mult = b_coin, b_mult
                    out[sym] = (base_coin, mult)
        except Exception as e:
            log.error(f"[pionex_rest] Failed to fetch symbols: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns symbol -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/market/tickers?type=PERP"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for t in r.json().get("data", {}).get("tickers", []):
                    sym = t.get("symbol", "").upper()
                    if not sym.endswith("_PERP") or "USDT" not in sym:
                        continue
                    price = float(t.get("close", 0.0) or 0.0)
                    vol_usd = float(t.get("amount", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[pionex_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        sym = symbol if "_PERP" in symbol else f"{normalize_symbol(symbol)}_USDT_PERP"
        url = f"{self.BASE_URL}/market/klines?symbol={sym}&interval=1M&limit={min(limit, 500)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", {}).get("klines", []):
                    t_sec = (int(row.get("time", 0)) // 60000) * 60
                    raw_close = float(row.get("close", 0.0))
                    close = raw_close * inv_mult
                    vol_usd = float(row.get("volume", 0.0)) * raw_close
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class PionexStream(BaseCEXStream):
    """Pionex WebSocket order book client."""

    WS_URL = "wss://ws.pionex.com/wsPub"
    SHARD_SIZE = 40
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("pionex", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    def _to_raw_symbol(self, s: str) -> str:
        s_u = s.upper()
        if s_u.endswith("_PERP"):
            return s_u
        for raw, (coin, _) in self._sym_map.items():
            if coin.upper() == s_u:
                return raw
        return f"{s_u}_USDT_PERP"

    async def _send_client_ping(self, ws: Any) -> None:
        now_ms = int(time.time() * 1000)
        await ws.send(json.dumps({"op": "PING", "timestamp": now_ms}))

    async def _handle_server_ping(self, ws: Any, raw_msg: str) -> bool:
        try:
            d = json.loads(raw_msg)
            if isinstance(d, dict) and d.get("op") == "PING":
                await ws.send(json.dumps({"op": "PONG", "timestamp": d.get("timestamp", int(time.time() * 1000))}))
                return True
        except Exception:
            pass
        return False

    async def _send_subscriptions(self, ws: Any) -> None:
        for s in self.symbols:
            sym = self._to_raw_symbol(s)
            sub = {
                "op": "SUBSCRIBE",
                "topic": "DEPTH",
                "symbol": sym,
                "limit": 5,
            }
            await ws.send(json.dumps(sub))
            # Pionex rate limit is ~5 req/sec on public WS connection.
            # 250ms spacing ensures zero 1008 policy violation disconnects.
            await asyncio.sleep(0.25)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            topic = data.get("topic", "")
            if topic != "DEPTH":
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
                ets = normalize_exchange_ts(data.get("timestamp")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="pionex",
                    symbol=sym,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
