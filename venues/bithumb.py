"""
venues/bithumb.py — Bithumb REST Client and WebSocket L2 Stream (South Korea Spot Co-Leader).
Streams L2 order book depth and real-time execution trades for Bithumb KRW pairs.
Converts KRW prices to USD in real time using the live USDT/KRW rate.
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    create_http_session,
    _secs_until_daily_reconnect_for,
    log,
)


class BithumbRestClient:
    """REST Client for Bithumb market discovery, 24h tickers, and orderbook snapshots."""

    BASE_URL = "https://api.bithumb.com/public"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()
        self._lock = threading.Lock()

    def fetch_krw_markets(self) -> Dict[str, str]:
        """
        Fetches all KRW spot markets.
        Returns dict: canonical coin symbol -> market code (e.g. 'BTC' -> 'BTC_KRW').
        """
        url = f"{self.BASE_URL}/ticker/ALL_KRW"
        out: Dict[str, str] = {}
        try:
            with self._lock:
                r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                data = r.json().get("data", {})
                for coin in data.keys():
                    if coin != "date" and coin.isalnum():
                        c_upper = coin.upper()
                        out[c_upper] = f"{c_upper}_KRW"
        except Exception as e:
            log.error(f"[bithumb_rest] Failed to fetch Bithumb markets: {e}")
        return out

    def fetch_tickers_24h(
        self,
        usdt_krw_rate: float = 1350.0,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24-hour ticker statistics for all Bithumb KRW markets, normalized to USD.
        Returns dict: coin -> {'price': float, 'vol_24h': float, 'price_krw': float}.
        """
        url = f"{self.BASE_URL}/ticker/ALL_KRW"
        out: Dict[str, Dict[str, Any]] = {}
        rate = usdt_krw_rate if usdt_krw_rate > 0 else 1350.0
        try:
            with self._lock:
                r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                data = r.json().get("data", {})
                for coin, stats in data.items():
                    if coin == "date" or not isinstance(stats, dict):
                        continue
                    c_upper = coin.upper()
                    try:
                        px_krw = float(stats.get("closing_price", 0.0) or 0.0)
                        vol_krw = float(stats.get("acc_trade_value_24H", 0.0) or 0.0)
                        if px_krw > 0:
                            out[c_upper] = {
                                "price": round(px_krw / rate, 8),
                                "vol_24h": round(vol_krw / rate, 2),
                                "price_krw": px_krw,
                            }
                    except (ValueError, TypeError):
                        continue
        except Exception as e:
            log.error(f"[bithumb_rest] Failed to fetch Bithumb 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        market: str,
        count: int = 200,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches 1-minute candles for Bithumb KRW pair (e.g. 'BTC_KRW' or 'BTC').
        Returns dict: minute_ts_sec -> (close_krw, volume_krw).
        """
        sym = market.replace("-", "_").upper()
        if not sym.endswith("_KRW"):
            sym = f"{sym}_KRW"
        url = f"{self.BASE_URL}/candlestick/{sym}/1m"
        out: Dict[int, Tuple[float, float]] = {}
        try:
            with self._lock:
                r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                rows = r.json().get("data", [])
                if isinstance(rows, list):
                    # Bithumb format: [time_ms, open, close, high, low, volume]
                    target_rows = rows[-count:] if count > 0 else rows
                    for row in target_rows:
                        t_sec = (int(row[0]) // 60000) * 60
                        c_krw = float(row[2])
                        v_coins = float(row[5])
                        v_krw = v_coins * c_krw
                        if t_sec > 0 and c_krw > 0:
                            out[t_sec] = (c_krw, v_krw)
        except Exception as e:
            log.warning(f"[bithumb_rest] Failed to fetch klines for {sym}: {e}")
        return out


class BithumbSpotStream(BaseVenueStream):
    """
    Bithumb Spot WebSocket Client.
    Streams L2 order books and trade executions for matched Korean Won pairs (<COIN>_KRW).
    Maintains an active L2 book and converts KRW prices to USD via live USDT/KRW rate.
    """

    WS_URL = "wss://pubwss.bithumb.com/pub/ws"
    WATCHDOG_SECS = 60.0

    def __init__(
        self,
        coins: List[str],
        callback: Callable[[OrderBookDepth5], None],
        trade_callback: Optional[Callable[[str, float, bool, float], None]] = None,
        initial_usdt_krw_rate: float = 1350.0,
        rate_provider: Optional[Callable[[], float]] = None,
    ):
        canon_coins = sorted(set(c.upper() for c in coins))
        super().__init__("bithumb", canon_coins, callback)
        self.trade_callback = trade_callback
        self.rate_provider = rate_provider
        self.usdt_krw_rate = initial_usdt_krw_rate if initial_usdt_krw_rate > 0 else 1350.0
        self.rate_ready = True
        self.subscribed = [f"{c}_KRW" for c in canon_coins]

        # In-memory L2 books: symbol_krw -> {price_krw: qty_coins}
        self._bids: Dict[str, Dict[float, float]] = {s: {} for s in self.subscribed}
        self._asks: Dict[str, Dict[float, float]] = {s: {} for s in self.subscribed}
        self._book_lock = threading.Lock()

    @property
    def current_rate(self) -> float:
        """Returns the freshest USDT/KRW rate, querying the provider if available."""
        if self.rate_provider:
            try:
                r = self.rate_provider()
                if r > 0:
                    self.usdt_krw_rate = r
                    return r
            except Exception:
                pass
        return self.usdt_krw_rate if self.usdt_krw_rate > 0 else 1350.0

    def set_usdt_krw_rate(self, new_rate: float) -> None:
        """Dynamically updates the KRW-USDT conversion rate from external feed."""
        if new_rate > 0:
            self.usdt_krw_rate = new_rate
            self.rate_ready = True

    async def _run_loop(self) -> None:
        """Main connection and message routing loop with automatic backoff reconnection."""
        while self._running:
            try:
                log.info(f"[bithumb] Connecting to {self.WS_URL} ({len(self.subscribed)} symbols)...")
                daily_budget = _secs_until_daily_reconnect_for("bithumb")

                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=15,
                    ping_interval=None,
                    ping_timeout=None,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._note_connect()
                    log.info(f"[bithumb] Connected! Subscribing to orderbookdepth and transaction...")

                    # 1. Subscribe to orderbook depth
                    sub_ob = {
                        "type": "orderbookdepth",
                        "symbols": self.subscribed,
                    }
                    await ws.send(json.dumps(sub_ob))

                    # 2. Subscribe to transaction feed if trade_callback provided
                    if self.trade_callback:
                        sub_tx = {
                            "type": "transaction",
                            "symbols": self.subscribed,
                        }
                        await ws.send(json.dumps(sub_tx))

                    start_time = time.time()

                    while self._running:
                        # Check daily maintenance reconnect
                        if (time.time() - start_time) >= daily_budget:
                            log.info("[bithumb] Daily planned reconnect window reached.")
                            break

                        try:
                            raw_msg = await asyncio.wait_for(ws.recv(), timeout=self.WATCHDOG_SECS)
                        except asyncio.TimeoutError:
                            log.warning(f"[bithumb] Watchdog: No data received for {self.WATCHDOG_SECS}s, reconnecting...")
                            break

                        if not raw_msg:
                            continue

                        now = time.time()
                        self.last_msg_ts = now

                        try:
                            msg = json.loads(raw_msg)
                        except Exception:
                            continue

                        # Handle Bithumb payloads
                        mtype = msg.get("type")
                        if mtype == "orderbookdepth":
                            self._handle_depth_update(msg, now)
                        elif mtype == "transaction":
                            self._handle_trade_update(msg, now)
                        elif "status" in msg:
                            resmsg = msg.get("resmsg", "")
                            if msg.get("status") == "0000":
                                self.sub_accepted += 1
                            else:
                                self.sub_rejected += 1
                                self.sub_errors.append(resmsg)
                                log.warning(f"[bithumb] Subscription status: {msg}")

            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                self.connected = False
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(f"[bithumb] WS disconnected: {e}. Reconnecting in {delay:.1f}s...")
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                self.connected = False
                break
            except Exception as e:
                self.connected = False
                delay = self._reconnect_policy.compute_delay(e)
                log.error(f"[bithumb] Unexpected WS error: {e}. Reconnecting in {delay:.1f}s...", exc_info=True)
                await asyncio.sleep(delay)

    def _handle_depth_update(self, msg: Dict[str, Any], now: float) -> None:
        """Processes incremental L2 order book updates and emits OrderBookDepth5."""
        content = msg.get("content", {})
        dt_raw = content.get("datetime")
        exchange_ts = normalize_exchange_ts(dt_raw)

        rate = self.current_rate
        if rate <= 0:
            return

        symbols_updated: Set[str] = set()

        with self._book_lock:
            for item in content.get("list", []):
                sym = item.get("symbol", "").upper()
                if not sym or sym not in self._bids:
                    continue

                otype = item.get("orderType")
                try:
                    px_krw = float(item.get("price", 0.0) or 0.0)
                    qty = float(item.get("quantity", 0.0) or 0.0)
                except (ValueError, TypeError):
                    continue

                if px_krw <= 0:
                    continue

                target = self._bids[sym] if otype == "bid" else self._asks[sym]
                if qty <= 0:
                    target.pop(px_krw, None)
                else:
                    target[px_krw] = qty

                symbols_updated.add(sym)

            for sym in symbols_updated:
                bids_dict = self._bids[sym]
                asks_dict = self._asks[sym]
                if not bids_dict or not asks_dict:
                    continue

                # Top-5 sorted
                sorted_bids = sorted(bids_dict.items(), key=lambda x: x[0], reverse=True)[:5]
                sorted_asks = sorted(asks_dict.items(), key=lambda x: x[0])[:5]

                if not sorted_bids or not sorted_asks:
                    continue

                # Best bid must be strictly less than best ask
                if sorted_bids[0][0] >= sorted_asks[0][0]:
                    # Crossed book transient anomaly
                    continue

                coin = sym.split("_")[0]
                self._note_symbol(coin)

                # Convert KRW to USD
                bids_usd = [(round(p / rate, 8), round(q, 8)) for p, q in sorted_bids]
                asks_usd = [(round(p / rate, 8), round(q, 8)) for p, q in sorted_asks]

                snap = OrderBookDepth5(
                    exchange="bithumb",
                    symbol=coin,
                    raw_symbol=sym,
                    ts=now,
                    exchange_ts=exchange_ts,
                    bids=bids_usd,
                    asks=asks_usd,
                )
                self.callback(snap)

    def _handle_trade_update(self, msg: Dict[str, Any], now: float) -> None:
        """Processes real-time execution trades and routes to trade_callback."""
        if not self.trade_callback:
            return

        rate = self.current_rate
        if rate <= 0:
            return

        content = msg.get("content", {})
        for item in content.get("list", []):
            sym = item.get("symbol", "").upper()
            coin = sym.split("_")[0]
            try:
                px_krw = float(item.get("contPrice", 0.0) or 0.0)
                qty = float(item.get("contQty", 0.0) or 0.0)
            except (ValueError, TypeError):
                continue

            if px_krw <= 0 or qty <= 0:
                continue

            # buySellGb: '1' -> sell, '2' -> buy
            buy_sell = str(item.get("buySellGb", ""))
            is_buy = (buy_sell == "2")

            px_usd = round(px_krw / rate, 8)
            vol_usd = round((px_krw * qty) / rate, 2)

            try:
                self.trade_callback(coin, vol_usd, is_buy, now)
            except Exception as e:
                log.error(f"[bithumb] Error in trade_callback: {e}")
