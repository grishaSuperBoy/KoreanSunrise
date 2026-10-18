"""
stream_manager.py - Coordinates WebSocket feeds across Upbit, Binance, and Bybit.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict

from config import MatchedUniverse
from models import OrderBookDepth5
from venues.base import BaseVenueStream
from venues.binance import BinanceFuturesStream
from venues.bybit import BybitPerpStream
from venues.upbit import UpbitSpotStream

log = logging.getLogger("stream_manager")


class StreamManager:
    """Manages connections to Upbit, Binance Futures, and Bybit Linear."""

    def __init__(
        self,
        universe: MatchedUniverse,
        callback: Callable[[OrderBookDepth5], None],
    ):
        self.universe = universe
        self.callback = callback
        self.streams: Dict[str, BaseVenueStream] = {}

        # 1. Upbit spot stream (KRW-COIN -> COIN)
        self.streams["upbit"] = UpbitSpotStream(universe.upbit_krw_to_coin, callback)

        # 2. Binance USDT perps (RAW_SYM -> (COIN, MULTIPLIER))
        self.streams["binance"] = BinanceFuturesStream(universe.binance_sym_map, callback)

        # 3. Bybit USDT perps (RAW_SYM -> (COIN, MULTIPLIER))
        self.streams["bybit"] = BybitPerpStream(universe.bybit_sym_map, callback)

    async def start_all(self) -> None:
        log.info(
            f"Starting streams for {len(self.universe.coins)} matched coins across "
            f"Upbit ({len(self.universe.upbit_krw_to_coin)}), "
            f"Binance ({len(self.universe.binance_sym_map)}), and "
            f"Bybit ({len(self.universe.bybit_sym_map)})..."
        )
        for stream in self.streams.values():
            await stream.start()

    async def stop_all(self) -> None:
        log.info("Stopping all streams...")
        for stream in self.streams.values():
            await stream.stop()

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