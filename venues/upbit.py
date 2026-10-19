"""
venues/upbit.py — Upbit REST Client and WebSocket L2 Stream (Spot Leader, South Korea).
Converts KRW prices to USD in real time using the live KRW-USDT rate.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests
import websockets

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    ReconnectPolicy,
    create_http_session,
    _secs_until_daily_reconnect_for,
    log,
)


class UpbitRestClient:
    """REST Client for Upbit market discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.upbit.com/v1"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_usdt_krw_rate(self) -> float:
        """Fetches current KRW-USDT spot mid rate."""
        url = f"{self.BASE_URL}/ticker?markets=KRW-USDT"
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                data = r.json()
                if data and len(data) > 0:
                    rate = float(data[0].get("trade_price", 0.0))
                    if rate > 0:
                        return rate
        except Exception as e:
            log.warning(f"[upbit_rest] Failed to fetch USDT-KRW rate: {e}")
        return 1350.0  # Safe fallback

    def fetch_krw_markets(self) -> Dict[str, str]:
        """
        Fetches all KRW spot markets.
        Returns dict: canonical coin symbol -> market code (e.g. 'BTC' -> 'KRW-BTC').
        """
        url = f"{self.BASE_URL}/market/all?isDetails=true"
        out: Dict[str, str] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json():
                    mkt = item.get("market", "")
                    if mkt.startswith("KRW-"):
                        coin = mkt.split("-", 1)[1].upper()
                        if coin:
                            out[coin] = mkt
        except Exception as e:
            log.error(f"[upbit_rest] Failed to fetch Upbit markets: {e}")
        return out

    def fetch_tickers_24h(
        self,
        market_codes: List[str],
        usdt_krw_rate: float,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24h tickers in batches of up to 100 markets.
        Returns canonical coin -> {price_krw, vol_krw_24h, price_usd, vol_usd_24h}.
        """
        out: Dict[str, Dict[str, Any]] = {}
        if not market_codes or usdt_krw_rate <= 0:
            return out

        chunk_size = 100
        for i in range(0, len(market_codes), chunk_size):
            chunk = market_codes[i : i + chunk_size]
            url = f"{self.BASE_URL}/ticker?markets={','.join(chunk)}"
            try:
                r = self.session.get(url, timeout=8)
                if r.status_code == 200:
                    for item in r.json():
                        mkt = item.get("market", "")
                        if not mkt.startswith("KRW-"):
                            continue
                        coin = mkt.split("-", 1)[1].upper()
                        price_krw = float(item.get("trade_price", 0.0) or 0.0)
                        vol_krw = float(item.get("acc_trade_price_24h", 0.0) or 0.0)

                        if price_krw > 0:
                            out[coin] = {
                                "market": mkt,
                                "price_krw": price_krw,
                                "vol_krw_24h": vol_krw,
                                "price_usd": price_krw / usdt_krw_rate,
                                "vol_usd_24h": vol_krw / usdt_krw_rate,
                            }
                time.sleep(0.12)  # Upbit 10 req/s rate compliance (<= 8.3 req/s safe buffer)
            except Exception as e:
                log.warning(f"[upbit_rest] Failed to fetch ticker batch ({len(chunk)}): {e}")

        return out

    def fetch_klines_1m(
        self,
        market: str,
        count: int = 200,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to `count` 1-minute candles (default 200 = ~3.3h).
        Paginates Upbit /v1/candles/minutes/1 in chunks of 200 candles using 'to' parameter if count > 200.
        Returns dict: minute_ts_sec -> (close_krw, volume_krw).
        """
        out: Dict[int, Tuple[float, float]] = {}
        to_param: Optional[str] = None
        remaining = max(1, count)

        while remaining > 0:
            batch_count = min(remaining, 200)
            url = f"{self.BASE_URL}/candles/minutes/1?market={market}&count={batch_count}"
            if to_param:
                url += f"&to={to_param}"
            try:
                r = self.session.get(url, timeout=6)
                if r.status_code != 200:
                    break
                rows = r.json()
                if not rows or not isinstance(rows, list):
                    break
                for row in rows:
                    ms = int(row.get("timestamp", 0))
                    t_sec = (ms // 60000) * 60
                    close_krw = float(row.get("trade_price", 0.0) or 0.0)
                    vol_krw = float(row.get("candle_acc_trade_price", 0.0) or 0.0)
                    if t_sec > 0 and close_krw > 0:
                        out[t_sec] = (close_krw, vol_krw)

                oldest = rows[-1].get("candle_date_time_utc")
                if not oldest or len(rows) < batch_count:
                    break
                to_param = oldest
                remaining -= len(rows)
                time.sleep(0.12)  # Upbit 10 req/s rate compliance (<= 8.3 req/s safe buffer)
            except Exception:
                break

        return out


class UpbitSpotStream(BaseVenueStream):
    """
    Upbit Spot WebSocket Client.
    Streams L2 order books for matched Korean Won pairs (KRW-<COIN>) and converts
    prices to USDT using the live KRW-USDT mid price.
    """

    WS_URL = "wss://api.upbit.com/websocket/v1"
    REST_USDT_OB = "https://api.upbit.com/v1/orderbook?markets=KRW-USDT"

    RATE_MAX_JUMP = 0.02
    RATE_JUMP_ACCEPT_AFTER = 5
    WATCHDOG_SECS = 30.0
    BACKOFF_MIN = 3.0
    BACKOFF_MAX = 60.0
    PARSE_ERR_LOG_EVERY = 100

    def __init__(
        self,
        krw_to_coin: Dict[str, str],
        callback: Callable[[OrderBookDepth5], None],
        trade_callback: Optional[Callable[[str, float, bool, float], None]] = None,
    ):
        coins = sorted(set(krw_to_coin.values()))
        super().__init__("upbit", coins, callback)
        self._krw_to_coin: Dict[str, str] = dict(krw_to_coin)
        self.subscribed: List[str] = list(coins)
        self.trade_callback = trade_callback

        self.usdt_krw_rate: float = 0.0
        self.rate_ready: bool = False
        self.rate_ts: float = 0.0
        self._rate_jump_count: int = 0
        self._parse_errors: int = 0
        self._callback_errors: int = 0

    @staticmethod
    def _http_get_json(url: str, timeout: float = 10.0):
        headers = {"User-Agent": "Mozilla/5.0 (compatible; KoreanSunrise/2.0)"}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _fetch_initial_usdt_rate(self) -> float:
        try:
            data = self._http_get_json(self.REST_USDT_OB)
            units = data[0].get("orderbook_units", [])
            if units:
                bp = float(units[0].get("bid_price", 0.0))
                ap = float(units[0].get("ask_price", 0.0))
                if bp > 0 and ap > 0:
                    return (bp + ap) * 0.5
        except Exception as e:
            log.warning(f"[upbit] Failed to fetch initial KRW-USDT orderbook rate: {e}")
        return 0.0

    def _update_rate(self, new_rate: float) -> None:
        if new_rate <= 0:
            return
        if not self.rate_ready:
            self.usdt_krw_rate = new_rate
            self.rate_ready = True
            self.rate_ts = time.time()
            self._rate_jump_count = 0
            log.info(f"[upbit] USDT/KRW mid-rate initialised: {new_rate:.2f}")
            return

        deviation = abs(new_rate - self.usdt_krw_rate) / self.usdt_krw_rate
        if deviation > self.RATE_MAX_JUMP:
            self._rate_jump_count += 1
            if self._rate_jump_count < self.RATE_JUMP_ACCEPT_AFTER:
                log.warning(
                    f"[upbit] Suspicious USDT/KRW jump {self.usdt_krw_rate:.2f} -> {new_rate:.2f} "
                    f"({deviation:.1%}), ignoring ({self._rate_jump_count}/{self.RATE_JUMP_ACCEPT_AFTER})"
                )
                return
            log.warning(f"[upbit] USDT/KRW jump persisted, accepting {new_rate:.2f}")
        self._rate_jump_count = 0
        self.usdt_krw_rate = new_rate
        self.rate_ts = time.time()

    async def _run_loop(self) -> None:
        target_codes: List[str] = ["KRW-USDT"] + sorted(self._krw_to_coin.keys())

        while self._running:
            try:
                rest_rate = await asyncio.to_thread(self._fetch_initial_usdt_rate)
                if rest_rate > 0:
                    self._update_rate(rest_rate)

                log.info(f"[upbit] Connecting to {self.WS_URL} ({len(self._krw_to_coin)} matched KRW coins)...")

                sub_msg: List[Dict[str, Any]] = [
                    {"ticket": f"upbit-sunrise-{int(time.time())}"},
                    {"type": "orderbook", "codes": target_codes},
                ]
                if self.trade_callback:
                    sub_msg.append({"type": "trade", "codes": target_codes})
                sub_msg.append({"format": "DEFAULT"})

                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=20,
                    ping_interval=20,
                    ping_timeout=15,
                    max_queue=4096,
                ) as ws:
                    self._note_connect()
                    self._note_sub(True)
                    self.last_msg_ts = time.time()
                    trades_note = " + trades" if self.trade_callback else ""
                    log.info(f"[upbit] Connected! Subscribed to {len(target_codes)} order books{trades_note} (incl. KRW-USDT).")
                    await ws.send(json.dumps(sub_msg))

                    try:
                        reconnect_in = _secs_until_daily_reconnect_for(self.name)
                        deadline = time.monotonic() + reconnect_in

                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[upbit] Scheduled daily reconnect. Reconnecting...")
                                break

                            if self._starved_since_connect(time.time()):
                                log.warning("[upbit] Connected but no data in 30s, reconnecting...")
                                break

                            if time.time() - self.last_msg_ts > self.WATCHDOG_SECS:
                                log.warning(f"[upbit] No messages for >{self.WATCHDOG_SECS:.0f}s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(15.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            self.last_msg_ts = time.time()
                            self._handle_msg(msg)
                    finally:
                        self.connected = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[upbit] WS error: {e!r}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

        self.connected = False

    def _handle_msg(self, raw: str | bytes) -> None:
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
        except Exception:
            self._parse_errors += 1
            if self._parse_errors == 1 or self._parse_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.warning(f"[upbit] parse error #{self._parse_errors}", exc_info=True)
            return

        msg_type = data.get("type")
        if msg_type == "trade":
            self._handle_trade(data)
            return

        try:
            snap = self._parse_orderbook_data(data)
        except Exception:
            self._parse_errors += 1
            if self._parse_errors == 1 or self._parse_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.warning(f"[upbit] orderbook parse error #{self._parse_errors}", exc_info=True)
            return

        if snap is None:
            return

        try:
            self.callback(snap)
        except Exception:
            self._callback_errors += 1
            if self._callback_errors == 1 or self._callback_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.error(f"[upbit] callback error #{self._callback_errors}", exc_info=True)

    def _handle_trade(self, data: Dict[str, Any]) -> None:
        if not self.trade_callback or not self.rate_ready or self.usdt_krw_rate <= 0:
            return
        code = data.get("code")
        if not code:
            return
        coin = self._krw_to_coin.get(code)
        if not coin:
            return

        trade_price = float(data.get("trade_price", 0.0) or 0.0)
        trade_volume = float(data.get("trade_volume", 0.0) or 0.0)
        if trade_price <= 0.0 or trade_volume <= 0.0:
            return

        ask_bid = str(data.get("ask_bid", "")).upper()
        # In Upbit protocol:
        # "BID" indicates taker bought from ask (market buy)
        # "ASK" indicates taker sold to bid (market sell)
        is_taker_buy = (ask_bid == "BID")
        trade_usd = (trade_price * trade_volume) / self.usdt_krw_rate
        now_ts = time.time()

        try:
            self.trade_callback(coin, trade_usd, is_taker_buy, now_ts)
        except Exception:
            self._callback_errors += 1
            if self._callback_errors == 1 or self._callback_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.error(f"[upbit] trade callback error #{self._callback_errors}", exc_info=True)

    def _parse_msg(self, raw: str | bytes) -> OrderBookDepth5 | None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        return self._parse_orderbook_data(data)

    def _parse_orderbook_data(self, data: Dict[str, Any]) -> OrderBookDepth5 | None:
        code = data.get("code")
        if not code:
            return None

        units = data.get("orderbook_units") or []
        if not units:
            return None

        if code == "KRW-USDT":
            u0 = units[0]
            bp = float(u0.get("bid_price", 0.0))
            ap = float(u0.get("ask_price", 0.0))
            if bp > 0 and ap > 0:
                self._update_rate((bp + ap) * 0.5)
            return None

        coin = self._krw_to_coin.get(code)
        if not coin or not self.rate_ready:
            return None

        inv_rate = 1.0 / self.usdt_krw_rate
        bids: List[Tuple[float, float]] = []
        asks: List[Tuple[float, float]] = []

        for u in units[:5]:
            bp, bs = float(u.get("bid_price", 0.0)), float(u.get("bid_size", 0.0))
            ap, asz = float(u.get("ask_price", 0.0)), float(u.get("ask_size", 0.0))
            if bp > 0.0 and bs > 0.0:
                bids.append((bp * inv_rate, bs))
            if ap > 0.0 and asz > 0.0:
                asks.append((ap * inv_rate, asz))

        if not bids or not asks:
            return None

        return OrderBookDepth5(
            exchange="upbit",
            symbol=coin,
            raw_symbol=code,
            ts=time.time(),
            exchange_ts=normalize_exchange_ts(data.get("timestamp")) or int(time.time() * 1000),
            bids=bids,
            asks=asks,
        )
