"""
stream_upbit.py - Direct WebSocket connection to Upbit (South Korea spot leader).
Converts KRW prices to USDT in real time using the live KRW-USDT order book mid-price.

Conversion: price_usdt = price_krw / usdt_krw_rate, where usdt_krw_rate is
the number of KRW per 1 USDT. No snapshots are published until the rate is known.
"""
from __future__ import annotations

import asyncio
import json
import time
import urllib.request
from typing import Callable, Dict, List, Tuple

import websockets

from models import OrderBookDepth5
from stream_base import (
    BaseVenueStream,
    _secs_until_daily_reconnect_for,
    log,
)


class UpbitSpotStream(BaseVenueStream):
    """
    Upbit Spot WebSocket Client.
    Streams L2 order books for matched Korean Won pairs (KRW-<COIN>) and converts
    prices to USDT using the live KRW-USDT mid price.
    """

    WS_URL = "wss://api.upbit.com/websocket/v1"
    REST_USDT_OB = "https://api.upbit.com/v1/orderbook?markets=KRW-USDT"

    # Rate sanity: reject a new rate that deviates from current rate by more
    # than 2%, unless it persists for RATE_JUMP_ACCEPT_AFTER ticks.
    RATE_MAX_JUMP = 0.02
    RATE_JUMP_ACCEPT_AFTER = 5

    # Reconnect if no message arrived for 30s
    WATCHDOG_SECS = 30.0

    BACKOFF_MIN = 3.0
    BACKOFF_MAX = 60.0

    PARSE_ERR_LOG_EVERY = 100

    def __init__(
        self,
        krw_to_coin: Dict[str, str],
        callback: Callable[[OrderBookDepth5], None],
    ):
        # Store canonical coins in BaseVenueStream
        coins = sorted(set(krw_to_coin.values()))
        super().__init__("upbit", coins, callback)
        self._krw_to_coin: Dict[str, str] = dict(krw_to_coin)
        self.subscribed: List[str] = list(coins)

        self.usdt_krw_rate: float = 0.0
        self.rate_ready: bool = False
        self.rate_ts: float = 0.0
        self._rate_jump_count: int = 0
        self._parse_errors: int = 0
        self._callback_errors: int = 0

    # ------------------------------------------------------------------ REST
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
            log.warning(f"[upbit] Failed to fetch initial KRW-USDT orderbook rate: {e} (waiting for WS tick)")
        return 0.0

    # ------------------------------------------------------------------ Rate
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

    # ------------------------------------------------------------------ Loop
    async def _run_loop(self) -> None:
        backoff = self.BACKOFF_MIN
        target_codes: List[str] = ["KRW-USDT"] + sorted(self._krw_to_coin.keys())

        while self._running:
            try:
                rest_rate = await asyncio.to_thread(self._fetch_initial_usdt_rate)
                if rest_rate > 0:
                    self._update_rate(rest_rate)

                log.info(f"[upbit] Connecting to {self.WS_URL} ({len(self._krw_to_coin)} matched KRW coins)...")

                sub_msg = [
                    {"ticket": f"upbit-sunrise-{int(time.time())}"},
                    {"type": "orderbook", "codes": target_codes},
                    {"format": "DEFAULT"},
                ]

                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=20,
                    ping_interval=20,
                    ping_timeout=15,
                    max_queue=4096,
                ) as ws:
                    self._note_connect()
                    self.last_msg_ts = time.time()
                    log.info(f"[upbit] Connected! Subscribed to {len(target_codes)} order books (incl. KRW-USDT).")
                    await ws.send(json.dumps(sub_msg))
                    backoff = self.BACKOFF_MIN

                    try:
                        reconnect_in = _secs_until_daily_reconnect_for(self.name)
                        deadline = time.monotonic() + reconnect_in

                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[upbit] Scheduled daily reconnect. Reconnecting...")
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
                log.warning(f"[upbit] WS error: {e!r}. Reconnecting in {backoff:.0f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.BACKOFF_MAX)

        self.connected = False

    # ------------------------------------------------------------------ Parsing
    def _handle_msg(self, raw: str | bytes) -> None:
        try:
            snap = self._parse_msg(raw)
        except Exception:
            self._parse_errors += 1
            if self._parse_errors == 1 or self._parse_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.warning(f"[upbit] parse error #{self._parse_errors}", exc_info=True)
            return

        if snap is None:
            return

        try:
            self.callback(snap)
        except Exception:
            self._callback_errors += 1
            if self._callback_errors == 1 or self._callback_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.error(f"[upbit] callback error #{self._callback_errors}", exc_info=True)

    def _parse_msg(self, raw: str | bytes) -> OrderBookDepth5 | None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)

        code = data.get("code")
        if not code:
            return None

        units = data.get("orderbook_units") or []
        if not units:
            return None

        # Live KRW-USDT conversion rate from top-of-book mid-price
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
            exchange_ts=int(data.get("timestamp", 0)),
            bids=bids,
            asks=asks,
        )