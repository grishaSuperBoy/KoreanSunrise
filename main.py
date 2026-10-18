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
from stream_manager import StreamManager
from web import APIServer

LOG_FMT = "%(asctime)s | %(levelname)-7s | %(name)-14s | %(message)s"
logging.basicConfig(level=getattr(logging, C.LOG_LEVEL.upper(), logging.INFO), format=LOG_FMT)
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
            stealth_depth_fraction=C.STEALTH_DEPTH_FRACTION,
            taker_fee_bps=C.TAKER_FEE_BPS,
            roundtrip_fee_bps=C.ROUNDTRIP_FEE_BPS,
            warmup_samples=C.MIN_WARMUP_SAMPLES,
            max_hold_sec=C.MAX_SIGNAL_HOLD_SEC,
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

    async def run(self) -> None:
        self.running = True
        if self.api_server:
            self.api_server.start()
        univ_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universe_live.json")

        if os.path.exists(univ_file):
            try:
                with open(univ_file, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                coins = doc.get("coins", [])
                upbit_krw_to_coin = doc.get("upbit_krw_to_coin", {})
                binance_sym_map = {k: tuple(v) for k, v in doc.get("binance_sym_map", {}).items()}
                bybit_sym_map = {k: tuple(v) for k, v in doc.get("bybit_sym_map", {}).items()}
                self.universe = C.MatchedUniverse(
                    coins=coins,
                    upbit_krw_to_coin=upbit_krw_to_coin,
                    binance_sym_map=binance_sym_map,
                    bybit_sym_map=bybit_sym_map,
                )
                self.universe._raw_doc = doc
                log.info(f"=== Loaded {len(coins)} incubator coins from universe_live.json ===")
            except Exception as e:
                log.warning(f"Could not parse universe_live.json: {e}")
                self.universe = None

        if self.universe is None or not self.universe.coins:
            log.info("=== Discovering overlapping Upbit KRW alts & Binance/Bybit Perps ===")
            self.universe = await asyncio.to_thread(C.discover_universe)
            try:
                doc = {
                    "_meta": {
                        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "updated_ts": round(time.time(), 3),
                        "coins_count": len(self.universe.coins),
                    },
                    "coins": self.universe.coins,
                    "upbit_krw_to_coin": self.universe.upbit_krw_to_coin,
                    "binance_sym_map": self.universe.binance_sym_map,
                    "bybit_sym_map": self.universe.bybit_sym_map,
                }
                with open(univ_file + ".tmp", "w", encoding="utf-8") as f:
                    json.dump(doc, f, indent=2, ensure_ascii=False)
                os.replace(univ_file + ".tmp", univ_file)
            except Exception as e:
                log.warning(f"Could not persist universe_live.json: {e}")

        self.pool_state.reload()

        if not self.universe or not self.universe.coins:
            raise RuntimeError("No overlapping coins discovered between Upbit and Perp exchanges.")

        log.info(f"=== Starting Korean Sunrise for {len(self.universe.coins)} matched target coins ===")
        self.stream_manager = StreamManager(
            universe=self.universe,
            callback=self.engine.on_book_update,
        )
        await self.stream_manager.start_all()

        last_heartbeat = time.time()

        try:
            while not self._shutdown_event.is_set():
                now = time.time()
                self.engine.sweep_timeouts(now)
                self.writer.maybe_flush(now)

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
            f"Ticks: Up {c['ticks_upbit']} / Foll {c['ticks_followers']} | "
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
        self.writer.flush()
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