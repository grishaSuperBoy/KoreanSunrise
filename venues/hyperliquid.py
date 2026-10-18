"""
venues/hyperliquid.py — Hyperliquid REST Client and WebSocket l2Book Stream.
Connects directly to Hyperliquid L1/L2 matching engine.
Supports single-unit normalization for 'k...' 1000x multiplier assets (e.g. kPEPE, kBONK).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5
from venues.base import (
    BaseVenueStream,
    create_http_session,
    normalize_symbol,
    parse_symbol_and_mult,
    _secs_until_daily_reconnect_for,
    log,
)


class HyperliquidRestClient:
    """REST Client for Hyperliquid perpetuals discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.hyperliquid.xyz/info"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float, float]]:
        """
        Fetches all active perpetual contracts from Hyperliquid.
        Returns: raw_symbol (e.g. 'SOLUSDT', '1000PEPEUSDT') -> (canonical_coin 'SOL', multiplier 1.0, contract_size 1.0).
        """
        out: Dict[str, Tuple[str, float, float]] = {}
        try:
            r = self.session.post(
                self.BASE_URL,
                json={"type": "metaAndAssetCtxs"},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                universe = data[0].get("universe", []) if isinstance(data, list) and len(data) > 0 else []
                for asset in universe:
                    hl_name = asset.get("name", "")
                    if not hl_name:
                        continue
                    coin, mult = parse_symbol_and_mult(hl_name)
                    if mult != 1.0:
                        raw_sym = f"{int(mult)}{coin}USDT"
                    else:
                        raw_sym = f"{coin}USDT"
                    out[raw_sym] = (coin, mult, 1.0)
        except Exception as e:
            log.warning(f"[hyperliquid_rest] Failed to fetch meta: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Fetches contract sizes for Hyperliquid perpetual contracts (defaults to 1.0)."""
        perps = self.fetch_linear_perpetuals()
        return {raw_sym: c_size for raw_sym, (_, _, c_size) in perps.items()}

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24-hour ticker statistics for Hyperliquid perps.
        Returns: raw_symbol -> {'venue_sym': raw_sym, 'price': float, 'vol_24h': float}.
        """
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.post(
                self.BASE_URL,
                json={"type": "metaAndAssetCtxs"},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and len(data) >= 2:
                    universe = data[0].get("universe", [])
                    ctxs = data[1]
                    for idx, asset in enumerate(universe):
                        hl_name = asset.get("name", "")
                        if not hl_name or idx >= len(ctxs):
                            continue
                        ctx = ctxs[idx]
                        coin, mult = parse_symbol_and_mult(hl_name)
                        if mult != 1.0:
                            raw_sym = f"{int(mult)}{coin}USDT"
                        else:
                            raw_sym = f"{coin}USDT"

                        price = float(ctx.get("midPx") or ctx.get("markPx") or 0.0)
                        vol = float(ctx.get("dayNtlVlm") or 0.0)
                        if price > 0:
                            out[raw_sym] = {"venue_sym": raw_sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[hyperliquid_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        symbol: str,
        limit: int = 200,
        mult: float = 1.0,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to limit 1m klines from Hyperliquid via candleSnapshot.
        Returns: minute_ts_sec -> (normalized_close_usd, quote_volume_usd).
        """
        out: Dict[int, Tuple[float, float]] = {}
        coin, s_mult = parse_symbol_and_mult(symbol)
        effective_mult = mult if mult != 1.0 else s_mult
        if effective_mult == 1000.0:
            hl_coin = "k" + coin
        elif effective_mult == 1_000_000.0:
            hl_coin = "m" + coin
        elif effective_mult == 1_000_000_000.0:
            hl_coin = "b" + coin
        else:
            hl_coin = coin

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (min(limit, 500) * 60 * 1000)
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": hl_coin,
                "interval": "1m",
                "startTime": start_ms,
                "endTime": now_ms,
            },
        }
        inv_mult = (1.0 / effective_mult) if effective_mult != 1.0 else 1.0
        try:
            r = self.session.post(self.BASE_URL, json=payload, timeout=6)
            if r.status_code == 200:
                candles = r.json()
                if isinstance(candles, list):
                    for c in candles:
                        t_sec = int(c.get("t", 0)) // 1000
                        close = float(c.get("c", 0.0)) * inv_mult
                        vol = float(c.get("v", 0.0)) * close * effective_mult
                        if t_sec > 0 and close > 0:
                            out[t_sec] = (close, vol)
        except Exception:
            pass
        return out


class HyperliquidStream(BaseVenueStream):
    """
    Direct WebSocket connection to Hyperliquid l2Book order book feed.
    Emits standardized OrderBookDepth5 events to the trading engine.
    """

    WS_URL = "wss://api.hyperliquid.xyz/ws"
    PING_INTERVAL_SEC = 20.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        self._contract_sizes: Dict[str, float] = {
            k.upper(): float(v) for k, v in (contract_sizes or {}).items()
        }
        if isinstance(symbols, dict):
            self._sym_map: Dict[str, Tuple[str, float, float]] = {}
            for k, v in symbols.items():
                k_u = str(k).upper()
                if isinstance(v, (list, tuple)):
                    coin = str(v[0])
                    mult = float(v[1]) if len(v) > 1 else 1.0
                    c_size = float(v[2]) if len(v) >= 3 else self._contract_sizes.get(k_u, 1.0)
                    self._sym_map[k_u] = (coin, mult, c_size)
                    self._contract_sizes[k_u] = c_size
                else:
                    coin, mult = parse_symbol_and_mult(k_u)
                    c_size = self._contract_sizes.get(k_u, 1.0)
                    self._sym_map[k_u] = (coin, mult, c_size)
            coins = sorted({c for c, _, _ in self._sym_map.values()})
        else:
            raw_list = [str(s).upper() for s in symbols]
            self._sym_map = {}
            for s in raw_list:
                coin, mult = parse_symbol_and_mult(s)
                c_size = self._contract_sizes.get(s, 1.0)
                self._sym_map[s] = (coin, mult, c_size)
            coins = sorted({c for c, _, _ in self._sym_map.values()})

        super().__init__("hyperliquid", coins, callback)
        self.subscribed = list(coins)

    def _resolve_hl_coin(self, raw_sym: str, coin: str, mult: float) -> str:
        if mult == 1000.0 or raw_sym.startswith("1000"):
            return "k" + coin
        if mult == 1_000_000.0 or raw_sym.startswith("1M") or raw_sym.startswith("1000000"):
            return "m" + coin
        if mult == 1_000_000_000.0 or raw_sym.startswith("1B") or raw_sym.startswith("1000000000"):
            return "b" + coin
        return coin

    async def _run_loop(self) -> None:
        while self._running:
            try:
                log.info(f"[hyperliquid] Connecting to {self.WS_URL} ({len(self.symbols)} coins)...")
                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=2000,
                ) as ws:
                    self._note_connect()
                    log.info("[hyperliquid] Connected! Subscribing to l2Book...")

                    # Subscribe to all configured coins
                    seen_hl: Set[str] = set()
                    for raw_sym, (coin, mult, c_size) in self._sym_map.items():
                        hl_coin = self._resolve_hl_coin(raw_sym, coin, mult)
                        if hl_coin in seen_hl:
                            continue
                        seen_hl.add(hl_coin)
                        sub_msg = {
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": hl_coin},
                        }
                        await ws.send(json.dumps(sub_msg))
                        await asyncio.sleep(0.02)

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[hyperliquid] Scheduled daily reconnect. Reconnecting...")
                                break

                            if not self.last_msg_ts:
                                self.last_msg_ts = time.time()
                            elif time.time() - self.last_msg_ts > 60.0:
                                log.warning("[hyperliquid] No data for >60s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(20.0, remaining))
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
                log.warning(f"[hyperliquid] WS disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    async def _ping_loop(self, ws: Any) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.PING_INTERVAL_SEC)
                await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            pass

    def _parse_message(self, raw_msg: str) -> None:
        try:
            payload = json.loads(raw_msg)
            ch = payload.get("channel")
            if ch != "l2Book":
                return

            data = payload.get("data", {})
            hl_coin = data.get("coin", "")
            if not hl_coin:
                return

            coin, mult = parse_symbol_and_mult(hl_coin)
            if mult != 1.0:
                raw_sym = f"{int(mult)}{coin}USDT"
            else:
                raw_sym = f"{coin}USDT"

            mapped = self._sym_map.get(raw_sym)
            if not mapped:
                for sym_key, info in self._sym_map.items():
                    if info[0] == coin:
                        mapped = info
                        break

            c_size = mapped[2] if mapped and len(mapped) >= 3 else self._contract_sizes.get(raw_sym, 1.0)

            levels = data.get("levels", [[], []])
            raw_bids = levels[0] if len(levels) > 0 else []
            raw_asks = levels[1] if len(levels) > 1 else []

            inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
            scale_qty = mult * c_size

            bids: List[Tuple[float, float]] = []
            for item in raw_bids[:5]:
                p = float(item.get("px", 0.0) or 0.0)
                q = float(item.get("sz", 0.0) or 0.0)
                if p > 0.0 and q > 0.0:
                    bids.append((p * inv_mult, q * scale_qty))

            asks: List[Tuple[float, float]] = []
            for item in raw_asks[:5]:
                p = float(item.get("px", 0.0) or 0.0)
                q = float(item.get("sz", 0.0) or 0.0)
                if p > 0.0 and q > 0.0:
                    asks.append((p * inv_mult, q * scale_qty))

            if not bids or not asks:
                return

            snap = OrderBookDepth5(
                exchange="hyperliquid",
                symbol=coin,
                raw_symbol=raw_sym,
                ts=time.time(),
                exchange_ts=int(data.get("time", 0)),
                bids=bids,
                asks=asks,
            )
            self.callback(snap)
        except Exception:
            pass
