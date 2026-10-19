"""
main.py - Main entry point for Korean Sunrise Lead-Lag & Momentum Sniping Service.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from typing import Optional

from batch_writer import BatchWriter
import config as C
from korean_engine import KoreanEngine
from pipeline.pool_state import PoolStateManager
from pipeline.profile import TradingProfileManager
from venues import StreamManager
from web import APIServer

LOG_FMT = "%(asctime)s | %(levelname)-7s | %(name)-14s | %(message)s"
LOG_DIR = os.path.join(C.PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "korean_sunrise.log")

_handlers: list[logging.Handler] = [logging.StreamHandler()]
try:
    _handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))
except Exception:
    pass

logging.basicConfig(
    level=getattr(logging, C.LOG_LEVEL.upper(), logging.INFO),
    format=LOG_FMT,
    handlers=_handlers,
)
log = logging.getLogger("main")


class KoreanSunriseService:
    def __init__(self):
        self.writer = BatchWriter(data_root=C.DATA_ROOT)
        self.pool_state = PoolStateManager()
        self.pool_state.reload()
        self.profile_manager = TradingProfileManager()
        self.profile_manager.reload()

        self.engine = KoreanEngine(
            emit_callback=self.writer.add,
            drain_threshold=C.DRAIN_THRESHOLD,
            blowout_threshold=C.BLOWOUT_THRESHOLD,
            min_edge_bps=C.MIN_EXEC_EDGE_BPS,
            max_position_budget_usd=C.MAX_POSITION_BUDGET_USD,
            min_order_usd=getattr(C, "MIN_ORDER_USD", 10.0),
            stealth_depth_fraction=C.STEALTH_DEPTH_FRACTION,
            taker_fee_bps=C.TAKER_FEE_BPS,
            roundtrip_fee_bps=C.ROUNDTRIP_FEE_BPS,
            warmup_samples=C.MIN_WARMUP_SAMPLES,
            max_hold_sec=C.MAX_SIGNAL_HOLD_SEC,
            require_trade_confirmation=True,
            pool_state=self.pool_state,
            profile_manager=self.profile_manager,
        )
        self.universe: Optional[C.MatchedUniverse] = None
        self.stream_manager: Optional[StreamManager] = None
        self.api_server: Optional[APIServer] = None
        if getattr(C, "API_SERVER_ENABLED", True):
            self.api_server = APIServer(
                service=self,
                host=getattr(C, "API_SERVER_HOST", "0.0.0.0"),
                port=getattr(C, "API_SERVER_PORT", 8089),
                token=getattr(C, "API_AUTH_TOKEN", "vitali_secret_key_2026"),
            )
        self.running = False
        self._shutdown_event = asyncio.Event()

    @staticmethod
    def _parse_universe_doc(doc: Dict[str, Any]) -> C.MatchedUniverse:
        coins = doc.get("coins", [])
        upbit_krw_to_coin = doc.get("upbit_krw_to_coin", {})
        venue_sym_maps: Dict[str, Dict[str, Tuple[str, float, float]]] = {}
        for k, v in doc.items():
            if k.endswith("_sym_map") and isinstance(v, dict):
                v_name = k.replace("_sym_map", "").lower()
                venue_sym_maps[v_name] = {
                    raw_sym: (
                        str(tup[0]),
                        float(tup[1]),
                        float(tup[2]) if len(tup) >= 3 else 1.0,
                    )
                    for raw_sym, tup in v.items()
                }

        binance_sym_map = {k: (v[0], v[1]) for k, v in venue_sym_maps.get("binance", {}).items()}
        bybit_sym_map = {k: (v[0], v[1]) for k, v in venue_sym_maps.get("bybit", {}).items()}

        univ = C.MatchedUniverse(
            coins=coins,
            upbit_krw_to_coin=upbit_krw_to_coin,
            venue_sym_maps=venue_sym_maps,
            binance_sym_map=binance_sym_map,
            bybit_sym_map=bybit_sym_map,
        )
        univ._raw_doc = doc
        return univ

    async def run(self) -> None:
        self.running = True
        if self.api_server:
            self.api_server.start()
        univ_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universe_live.json")

        if os.path.exists(univ_file):
            try:
                with open(univ_file, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                self.universe = self._parse_universe_doc(doc)
                log.info(f"=== Loaded {len(self.universe.coins)} incubator coins across {len(self.universe.venue_sym_maps)} venues from universe_live.json ===")
            except Exception as e:
                log.warning(f"Could not parse universe_live.json: {e}")
                self.universe = None

        if self.universe is None or not self.universe.coins:
            log.info("=== universe_live.json not found or empty: Running multi-venue UniverseScreener ===")
            from screening.screener import UniverseScreener
            screener = UniverseScreener(output_file=univ_file)
            doc = await asyncio.to_thread(screener.run)
            self.universe = self._parse_universe_doc(doc)

        self.pool_state.reload()

        if not self.universe or not self.universe.coins:
            raise RuntimeError("No overlapping coins discovered between Upbit and Perp exchanges.")

        log.info(f"=== Starting Korean Sunrise for {len(self.universe.coins)} matched target coins ===")
        active_venues = self.profile_manager.get_profile().get("active_venues")
        self.stream_manager = StreamManager(
            universe=self.universe,
            callback=self.engine.on_book_update,
            trade_callback=self.engine.on_upbit_trade,
            active_venues=active_venues,
        )
        await self.stream_manager.start_all()

        last_heartbeat = time.time()
        last_reload = time.time()

        try:
            while not self._shutdown_event.is_set():
                now = time.time()
                try:
                    self.engine.sweep_timeouts(now)
                except Exception as e:
                    log.error(f"[main] sweep_timeouts error: {e}")
                await asyncio.to_thread(self.writer.maybe_flush, now)

                if now - last_reload >= 30.0:
                    last_reload = now
                    await asyncio.to_thread(self.engine.maybe_reload_thresholds, now)

                if now - last_heartbeat >= 15.0:
                    self._log_heartbeat()
                    last_heartbeat = now

                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

        except asyncio.CancelledError:
            log.info("Received CancelledError")
        finally:
            await self.shutdown()

    def _log_heartbeat(self) -> None:
        if not self.stream_manager:
            return
        c = self.engine.counters
        status = self.stream_manager.get_status()

        parts = []
        krw_rate = 0.0
        for k, v in status.items():
            st_label = "OK" if v["connected"] else "DISC"
            parts.append(f"{k}: {st_label} ({v['symbols_seen']}/{v['subscribed']})")
            if "usdt_krw_rate" in v:
                krw_rate = v["usdt_krw_rate"]

        conn_str = " | ".join(parts)
        snap = self.pool_state.get_pool_snapshot()
        summary = snap["summary"]
        log.info(
            f"[HEARTBEAT] KRW/USDT: {krw_rate:.1f} | "
            f"Pool: Inc {summary['incubator']} | Battle {summary['battle']} (Active: {summary['battle_active']}) | "
            f"Ticks: Up {c['ticks_upbit']} (Trades: {c.get('trades_upbit', 0)}) / Foll {c['ticks_followers']} | "
            f"Cancels Blocked: {c.get('cancels_blocked', 0)} | "
            f"Sigs: {c['signals_triggered']} (⚔️Battle: {c.get('signals_battle', 0)}, 🧪Inc: {c.get('signals_incubator', 0)}) | "
            f"Streams: [{conn_str}]"
        )

    async def shutdown(self) -> None:
        if not self.running:
            return
        self.running = False
        log.info("Gracefully shutting down Korean Sunrise...")
        if self.stream_manager is not None:
            await self.stream_manager.stop_all()
        if self.api_server is not None:
            self.api_server.stop()
        await asyncio.to_thread(self.writer.flush)
        log.info("Korean Sunrise stopped cleanly.")


async def main_async() -> None:
    svc = KoreanSunriseService()

    def _sig_handler():
        log.info("Shutdown signal received")
        svc._shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass

    await svc.run()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()