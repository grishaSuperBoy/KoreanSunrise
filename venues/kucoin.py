"""
venues/kucoin.py — KuCoin Futures REST Client and WebSocket level2Depth5 Stream.
"""
from __future__ import annotations

import asyncio
import json
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


class KucoinRestClient:
    """REST Client for KuCoin Futures discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api-futures.kucoin.com/api/v1"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float, float]]:
        """Returns symbol (e.g. 'XBTUSDTM') -> (canonical_coin 'BTC', mult 1.0, multiplier)."""
        url = f"{self.BASE_URL}/contracts/active"
        out: Dict[str, Tuple[str, float, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCurrency", "").upper()
                    if settle != "USDT":
                        continue
                    sym = item.get("symbol", "").upper()
                    base = item.get("baseCurrency", "").upper()
                    if base == "XBT":
                        base = "BTC"
                    base_coin = normalize_symbol(base)
                    c_size = float(item.get("multiplier", 1.0) or 1.0)
                    out[sym] = (base_coin, 1.0, c_size)
        except Exception as e:
            log.error(f"[kucoin_rest] Failed to fetch contracts: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns symbol (e.g. 'XBTUSDTM') -> multiplier (e.g. 0.001)."""
        url = f"{self.BASE_URL}/contracts/active"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCurrency", "").upper()
                    if settle != "USDT":
                        continue
                    sym = item.get("symbol", "").upper()
                    mult = float(item.get("multiplier", 1.0) or 1.0)
                    out[sym] = mult
        except Exception as e:
            log.error(f"[kucoin_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns symbol -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/contracts/active"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCurrency", "").upper()
                    if settle != "USDT":
                        continue
                    sym = item.get("symbol", "").upper()
                    price = float(item.get("lastTradePrice", 0.0) or 0.0)
                    vol_usd = float(item.get("turnoverOf24h", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[kucoin_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        url = f"{self.BASE_URL}/kline/query?symbol={symbol}&granularity=1"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", [])[-limit:]:
                    t_sec = (int(row[0]) // 60000) * 60
                    raw_close = float(row[4])
                    close = raw_close * inv_mult
                    vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * raw_close
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class KucoinStream(BaseVenueStream):
    """KuCoin Futures: USDT-margined linear perpetuals WebSocket client."""

    URL = "wss://ws-api-futures.kucoin.com/"
    BULLET = "https://api-futures.kucoin.com/api/v1/bullet-public"
    CONTRACTS = "https://api-futures.kucoin.com/api/v1/contracts/active"
    TOPIC = "/contractMarket/level2Depth5:{sym}"
    PING_FALLBACK_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        sym_list = list(symbols.keys()) if isinstance(symbols, dict) else list(symbols)
        super().__init__("kucoin", sym_list, callback)
        self.subscribed = list(self.symbols)
        self._ours_to_venue: Dict[str, str] = {}
        self._venue_to_ours: Dict[str, str] = {}
        self._contract_sizes: Dict[str, float] = {k.upper(): float(v) for k, v in (contract_sizes or {}).items()}
        self._ping_sec = self.PING_FALLBACK_SEC
        self._session = create_http_session()

    def _resolve_symbols(self) -> Dict[str, str]:
        try:
            if self._session is None:
                self._session = create_http_session()
            r = self._session.get(self.CONTRACTS, timeout=8)
            if r.status_code == 200:
                for c in r.json().get("data", []):
                    sym = c.get("symbol", "").upper()
                    mult = float(c.get("multiplier", 1.0) or 1.0)
                    self._contract_sizes[sym] = mult
                    base = c.get("baseCurrency", "").upper()
                    if base == "XBT":
                        base = "BTC"
                    base_norm = normalize_symbol(base)
                    for s in self.symbols:
                        if normalize_symbol(s) == base_norm:
                            self._ours_to_venue[s] = sym
                            self._venue_to_ours[sym] = normalize_symbol(s)
        except Exception as e:
            log.warning(f"[kucoin] Failed to resolve contract symbols: {e}")
        return self._ours_to_venue

    async def stop(self) -> None:
        await super().stop()
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None

    async def _run_loop(self) -> None:
        while self._running:
            try:
                if not self._ours_to_venue:
                    await asyncio.to_thread(self._resolve_symbols)
                target_syms = list(self._ours_to_venue.values())
                if not target_syms:
                    log.warning("[kucoin] No symbols resolved for subscription, retrying...")
                    delay = self._reconnect_policy.compute_delay(Exception("kucoin symbol resolution failed"))
                    await asyncio.sleep(delay)
                    continue

                # Get dynamic bullet token using persistent session
                if self._session is None:
                    self._session = create_http_session()
                bullet_resp = await asyncio.to_thread(self._session.post, self.BULLET, timeout=10)
                data = bullet_resp.json().get("data", {})
                token = data.get("token")
                endpoints = data.get("instanceServers", [])
                if not token or not endpoints:
                    delay = self._reconnect_policy.compute_delay(Exception("kucoin bullet token failed"))
                    await asyncio.sleep(delay)
                    continue

                ws_endpoint = endpoints[0]["endpoint"]
                ping_interval = float(endpoints[0].get("pingInterval", 15000)) / 1000.0
                ws_url = f"{ws_endpoint}?token={token}&[format=json]"

                log.info(f"[kucoin] Connecting to {ws_url[:50]}... ({len(target_syms)} symbols)")
                async with websockets.connect(
                    ws_url,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=4096,
                ) as ws:
                    self._note_connect()
                    log.info("[kucoin] Connected! Subscribing...")

                    # Subscribe in chunks of 50
                    for i in range(0, len(target_syms), 50):
                        chunk = target_syms[i : i + 50]
                        topic = f"/contractMarket/level2Depth5:{','.join(chunk)}"
                        sub_msg = {
                            "id": int(time.time() * 1000),
                            "type": "subscribe",
                            "topic": topic,
                            "response": False,
                        }
                        await ws.send(json.dumps(sub_msg))
                        await asyncio.sleep(0.04)

                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    async def _ping_worker():
                        while self._running:
                            await asyncio.sleep(ping_interval)
                            await ws.send(json.dumps({"id": int(time.time() * 1000), "type": "ping"}))

                    ping_task = asyncio.create_task(_ping_worker())
                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[kucoin] Daily scheduled reconnect. Reconnecting...")
                                break

                            if self._starved_since_connect(time.time()):
                                log.warning("[kucoin] Connected but no data in 30s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(30.0, remaining))
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
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[kucoin] WS error: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            topic = data.get("topic", "")
            if not topic.startswith("/contractMarket/level2Depth5:"):
                return
            sym_raw = topic.split(":", 1)[1].upper()
            clean_raw = sym_raw[:-1] if sym_raw.endswith("M") else sym_raw
            sym = self._venue_to_ours.get(sym_raw) or normalize_symbol(clean_raw.replace("XBT", "BTC"))
            contract_size = self._contract_sizes.get(sym_raw, 1.0)
            payload = data.get("data", {})
            bids_raw = payload.get("bids", [])[:5]
            asks_raw = payload.get("asks", [])[:5]
            bids = [(float(p), float(q) * contract_size) for p, q in bids_raw if float(p) > 0 and float(q) > 0]
            asks = [(float(p), float(q) * contract_size) for p, q in asks_raw if float(p) > 0 and float(q) > 0]
            if bids and asks:
                self._note_symbol(sym)
                ets = normalize_exchange_ts(payload.get("timestamp")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="kucoin",
                    symbol=sym,
                    raw_symbol=sym_raw,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
