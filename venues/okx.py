"""
venues/okx.py — OKX v5 REST Client and WebSocket books5 L2 Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class OKXRestClient:
    """REST Client for OKX v5 discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://www.okx.com/api/v5"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns: instId (e.g. 'BTC-USDT-SWAP') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/public/instruments?instType=SWAP"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCcy", "").upper()
                    if settle != "USDT":
                        continue
                    inst_id = item.get("instId", "").upper()
                    base_coin = normalize_symbol(item.get("ctValCcy", "") or inst_id)
                    ct_val = float(item.get("ctVal", 1.0) or 1.0)
                    out[inst_id] = (base_coin, 1.0, ct_val)
        except Exception as e:
            log.error(f"[okx_rest] Failed to fetch instruments: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns instId (e.g. 'BTC-USDT-SWAP') -> ctVal (e.g. 0.01)."""
        url = f"{self.BASE_URL}/public/instruments?instType=SWAP"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCcy", "").upper()
                    if settle != "USDT":
                        continue
                    inst_id = item.get("instId", "").upper()
                    ct_val = float(item.get("ctVal", 1.0) or 1.0)
                    out[inst_id] = ct_val
        except Exception as e:
            log.error(f"[okx_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns instId -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/market/tickers?instType=SWAP"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    inst_id = item.get("instId", "").upper()
                    if not inst_id.endswith("-USDT-SWAP"):
                        continue
                    price = float(item.get("last", 0.0) or 0.0)
                    vol_usd = float(item.get("volCcy24h", 0.0) or 0.0)
                    if price > 0:
                        out[inst_id] = {"venue_sym": inst_id, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[okx_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        inst_id = symbol if "-SWAP" in symbol else f"{normalize_symbol(symbol)}-USDT-SWAP"
        url = f"{self.BASE_URL}/market/candles?instId={inst_id}&bar=1m&limit={min(limit, 300)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = (int(row[0]) // 60000) * 60
                    raw_close = float(row[4])
                    close = raw_close * inv_mult
                    vol_usd = float(row[7]) if len(row) > 7 else float(row[5]) * raw_close
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class OKXStream(BaseCEXStream):
    """OKX v5 WebSocket client for perpetual swap books5."""

    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("okx", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send("ping")

    async def _send_subscriptions(self, ws: Any) -> None:
        if not self._contract_sizes:
            try:
                sizes = await asyncio.to_thread(OKXRestClient().fetch_contract_sizes)
                if sizes:
                    self._contract_sizes.update(sizes)
            except Exception as e:
                log.warning(f"[okx] Failed to auto-fetch contract sizes: {e}")

        for i in range(0, len(self.symbols), 50):
            args = [
                {"channel": "books5", "instId": s if "-SWAP" in s else f"{self._base_coin(s)}-USDT-SWAP"}
                for s in self.symbols[i : i + 50]
            ]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            await asyncio.sleep(0.04)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            arg = data.get("arg", {})
            if arg.get("channel") != "books5":
                return
            raw_data = data.get("data")
            book_data = raw_data[0] if isinstance(raw_data, list) and raw_data else (raw_data if isinstance(raw_data, dict) else {})
            inst_id = arg.get("instId", "").upper()
            if not inst_id:
                return

            seq_id = int(book_data.get("seqId") or 0)
            if seq_id > 0:
                last_seq = self._last_seq.get(inst_id)
                if last_seq is not None:
                    if seq_id <= last_seq:
                        self.seq_duplicates += 1
                        return
                self._last_seq[inst_id] = seq_id

            sym, mult = self._resolve_coin_and_mult(inst_id)
            contract_size = self._get_contract_size(inst_id)
            bids = self._extract_levels(book_data.get("bids") or book_data.get("b"), 5, mult=mult, contract_size=contract_size)
            asks = self._extract_levels(book_data.get("asks") or book_data.get("a"), 5, mult=mult, contract_size=contract_size)
            if bids and asks:
                self._note_symbol(sym)
                ets = normalize_exchange_ts(book_data.get("ts") or data.get("ts")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="okx",
                    symbol=sym,
                    raw_symbol=inst_id,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
