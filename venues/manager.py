"""
venues/manager.py — Dynamic WebSocket stream coordinator for Korean Sunrise.
Manages Upbit spot leader feed and all active perpetual follower feeds:
Binance, Bybit, OKX, Gate.io, MEXC, KuCoin, BingX, HTX, Pionex.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from config import MatchedUniverse
from models import OrderBookDepth5
from venues.base import BaseVenueStream
from venues.asterdex import AsterdexStream
from venues.bingx import BingXStream
from venues.binance import BinanceFuturesStream
from venues.bithumb import BithumbRestClient, BithumbSpotStream
from venues.bybit import BybitPerpStream
from venues.gateio import GateioStream
from venues.htx import HTXStream
from venues.hyperliquid import HyperliquidStream
from venues.kucoin import KucoinStream
from venues.mexc import MEXCStream
from venues.okx import OKXStream
from venues.pionex import PionexStream
from venues.upbit import UpbitSpotStream

log = logging.getLogger("venues.manager")


class StreamManager:
    """Manages connections to Upbit and all configured follower exchanges."""

    def __init__(
        self,
        universe: MatchedUniverse,
        callback: Callable[[OrderBookDepth5], None],
        trade_callback: Optional[Callable[[str, float, bool, float], None]] = None,
        active_venues: Optional[List[str]] = None,
        bithumb_trade_callback: Optional[Callable[[str, float, bool, float], None]] = None,
    ):
        self.universe = universe
        self.callback = callback
        self.trade_callback = trade_callback
        self.bithumb_trade_callback = bithumb_trade_callback
        self.streams: Dict[str, BaseVenueStream] = {}

        allowed_venues = [v.lower() for v in active_venues] if active_venues else None

        # 1. Leader stream: Upbit (Spot KRW)
        if universe.upbit_krw_to_coin:
            def _sync_to_bithumb(rate: float) -> None:
                b_stream = self.streams.get("bithumb")
                if isinstance(b_stream, BithumbSpotStream) and rate > 0:
                    b_stream.set_usdt_krw_rate(rate)

            self.streams["upbit"] = UpbitSpotStream(
                universe.upbit_krw_to_coin,
                callback=callback,
                trade_callback=trade_callback,
                on_rate_update=_sync_to_bithumb,
            )

        # 1b. Co-Leader stream: Bithumb (Spot KRW)
        import config as C
        bithumb_enabled = getattr(C, "BITHUMB_ENABLED", True)

        if bithumb_enabled and universe.coins:
            b_cb = bithumb_trade_callback or trade_callback
            try:
                init_rate = self.get_usdt_krw_rate() or 1400.0
                raw_doc = getattr(universe, "_raw_doc", {}) or {}
                bt_coins = [
                    coin for coin, p in raw_doc.get("pairs", {}).items()
                    if p.get("bithumb_symbol")
                ] or universe.coins
                self.streams["bithumb"] = BithumbSpotStream(
                    coins=bt_coins,
                    callback=callback,
                    trade_callback=b_cb,
                    initial_usdt_krw_rate=init_rate,
                    rate_provider=self.get_usdt_krw_rate,
                )
                log.info(f"[stream_manager] Registered co-leader feed for BITHUMB ({len(bt_coins)} coins)")
            except Exception as e:
                log.warning(f"[stream_manager] Failed to initialize Bithumb feed: {e}")

        # 2. Dynamic Follower streams
        allowed_venues = [v.lower() for v in active_venues] if active_venues else None

        # Binance
        if allowed_venues is None or "binance" in allowed_venues:
            b_map = universe.get_sym_map("binance") or universe.binance_sym_map
            if b_map:
                self.streams["binance"] = BinanceFuturesStream(b_map, callback)

        # Bybit
        if allowed_venues is None or "bybit" in allowed_venues:
            by_map = universe.get_sym_map("bybit") or universe.bybit_sym_map
            if by_map:
                self.streams["bybit"] = BybitPerpStream(by_map, callback)

        # Map of additional follower stream classes that take (symbols, callback)
        cex_classes = {
            "okx": OKXStream,
            "gateio": GateioStream,
            "mexc": MEXCStream,
            "kucoin": KucoinStream,
            "bingx": BingXStream,
            "htx": HTXStream,
            "pionex": PionexStream,
            "asterdex": AsterdexStream,
            "hyperliquid": HyperliquidStream,
        }

        # Check universe for other venue sym maps
        raw_doc = getattr(universe, "_raw_doc", {}) or {}
        for v_name, cls in cex_classes.items():
            if allowed_venues is not None and v_name not in allowed_venues:
                continue

            v_sym_map = universe.get_sym_map(v_name)
            if not v_sym_map:
                v_sym_map = raw_doc.get(f"{v_name}_sym_map", {})

            if v_sym_map:
                norm_map = {
                    k: (v[0], float(v[1]), float(v[2])) if len(v) >= 3 else (v[0], float(v[1]), 1.0)
                    for k, v in v_sym_map.items()
                }
                try:
                    self.streams[v_name] = cls(norm_map, callback)
                    log.info(f"[stream_manager] Registered follower feed for {v_name.upper()} ({len(norm_map)} contracts)")
                except Exception as e:
                    log.warning(f"[stream_manager] Failed to initialize {v_name} feed: {e}")
            else:
                v_coins = [
                    coin for coin, p in raw_doc.get("pairs", {}).items()
                    if v_name in p.get("venues", [])
                ]
                if v_coins:
                    try:
                        self.streams[v_name] = cls(v_coins, callback)
                        log.info(f"[stream_manager] Registered follower feed for {v_name.upper()} ({len(v_coins)} coins)")
                    except Exception as e:
                        log.warning(f"[stream_manager] Failed to initialize {v_name} feed: {e}")

    async def start_all(self) -> None:
        log.info(
            f"Starting {len(self.streams)} feeds: {', '.join(s.upper() for s in self.streams.keys())}..."
        )
        for name, stream in self.streams.items():
            try:
                await stream.start()
            except Exception as e:
                log.error(f"[stream_manager] Error starting {name} feed: {e}")

    async def stop_all(self) -> None:
        log.info("Stopping all venue streams...")
        for name, stream in self.streams.items():
            try:
                await stream.stop()
            except Exception as e:
                log.error(f"[stream_manager] Error stopping {name} feed: {e}")

    def get_usdt_krw_rate(self) -> float:
        """Returns the real-time USDT/KRW conversion rate from Upbit feed, keeping Bithumb synced."""
        upbit_stream = self.streams.get("upbit")
        if isinstance(upbit_stream, UpbitSpotStream) and upbit_stream.rate_ready:
            rate = round(upbit_stream.usdt_krw_rate, 2)
            bithumb_stream = self.streams.get("bithumb")
            if isinstance(bithumb_stream, BithumbSpotStream) and rate > 0:
                bithumb_stream.set_usdt_krw_rate(rate)
            return rate
        return 0.0

    def get_status(self) -> Dict[str, Dict[str, Any]]:
        status: Dict[str, Dict[str, Any]] = {}
        for name, s in self.streams.items():
            info: Dict[str, Any] = {
                "connected": s.connected,
                "symbols_seen": len(s.symbols_seen),
                "subscribed": len(s.subscribed),
                "sub_accepted": getattr(s, "sub_accepted", 0),
                "sub_rejected": getattr(s, "sub_rejected", 0),
                "sub_errors": list(getattr(s, "sub_errors", [])),
                "books_dropped": s.books_dropped,
                "seq_gaps": getattr(s, "seq_gaps", 0),
                "seq_duplicates": getattr(s, "seq_duplicates", 0),
            }
            if isinstance(s, (UpbitSpotStream, BithumbSpotStream)):
                info["usdt_krw_rate"] = round(s.usdt_krw_rate, 2) if getattr(s, "rate_ready", False) else 0.0
            status[name] = info
        return status
