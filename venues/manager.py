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
from venues.bingx import BingXStream
from venues.binance import BinanceFuturesStream
from venues.bybit import BybitPerpStream
from venues.gateio import GateioStream
from venues.htx import HTXStream
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
        active_venues: Optional[List[str]] = None,
    ):
        self.universe = universe
        self.callback = callback
        self.streams: Dict[str, BaseVenueStream] = {}

        # 1. Leader stream: Upbit (Spot KRW)
        if universe.upbit_krw_to_coin:
            self.streams["upbit"] = UpbitSpotStream(universe.upbit_krw_to_coin, callback)

        # 2. Dynamic Follower streams
        allowed_venues = [v.lower() for v in active_venues] if active_venues else None

        # Binance
        if universe.binance_sym_map and (allowed_venues is None or "binance" in allowed_venues):
            self.streams["binance"] = BinanceFuturesStream(universe.binance_sym_map, callback)

        # Bybit
        if universe.bybit_sym_map and (allowed_venues is None or "bybit" in allowed_venues):
            self.streams["bybit"] = BybitPerpStream(universe.bybit_sym_map, callback)

        # Map of additional CEX stream classes that take (symbols, callback)
        cex_classes = {
            "okx": OKXStream,
            "gateio": GateioStream,
            "mexc": MEXCStream,
            "kucoin": KucoinStream,
            "bingx": BingXStream,
            "htx": HTXStream,
            "pionex": PionexStream,
        }

        # Check universe for other venue sym maps
        raw_doc = getattr(universe, "_raw_doc", {}) or {}
        for v_name, cls in cex_classes.items():
            if allowed_venues is not None and v_name not in allowed_venues:
                continue

            v_sym_map = raw_doc.get(f"{v_name}_sym_map", {})
            if v_sym_map:
                norm_map = {k: (v[0], float(v[1])) for k, v in v_sym_map.items()}
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
        """Returns the real-time USDT/KRW conversion rate from Upbit feed."""
        upbit_stream = self.streams.get("upbit")
        if isinstance(upbit_stream, UpbitSpotStream) and upbit_stream.rate_ready:
            return round(upbit_stream.usdt_krw_rate, 2)
        return 0.0

    def get_status(self) -> Dict[str, Dict[str, Any]]:
        status: Dict[str, Dict[str, Any]] = {}
        for name, s in self.streams.items():
            info: Dict[str, Any] = {
                "connected": s.connected,
                "symbols_seen": len(s.symbols_seen),
                "subscribed": len(s.subscribed),
                "books_dropped": s.books_dropped,
            }
            if isinstance(s, UpbitSpotStream):
                info["usdt_krw_rate"] = round(s.usdt_krw_rate, 2) if s.rate_ready else 0.0
            status[name] = info
        return status
