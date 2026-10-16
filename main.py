"""
main.py - Main entry point for Korean Sunrise Lead-Lag & Momentum Sniping Service.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Optional

from batch_writer import BatchWriter
import config as C
from korean_engine import KoreanEngine
from stream_manager import StreamManager

LOG_FMT = "%(asctime)s | %(levelname)-7s | %(name)-14s | %(message)s"
logging.basicConfig(level=getattr(logging, C.LOG_LEVEL.upper(), logging.INFO), format=LOG_FMT)
log = logging.getLogger("main")


class KoreanSunriseService:
    def __init__(self):
        self.writer = BatchWriter(data_root=C.DATA_ROOT)
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
        )
        self.universe: Optional[C.MatchedUniverse] = None
        self.stream_manager: Optional[StreamManager] = None
        self.running = False
        self._shutdown_event = asyncio.Event()

    async def run(self) -> None:
        self.running = True
        log.info("=== Discovering overlapping Upbit KRW alts & Binance/Bybit Perps ===")
        self.universe = await asyncio.to_thread(C.discover_universe)

        if not self.universe.coins:
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
        log.info(
            f"[HEARTBEAT] KRW/USDT: {krw_rate:.1f} | "
            f"Upbit Ticks: {c['ticks_upbit']} | Follower Ticks: {c['ticks_followers']} | "
            f"Signals: {c['signals_triggered']} "
            f"(Conv: {c['signals_converged']}, Rev: {c['signals_reversed']}, TO: {c['signals_timeout']}) | "
            f"Written: {self.writer.total_written} | Streams: [{conn_str}]"
        )

    async def shutdown(self) -> None:
        if not self.running:
            return
        self.running = False
        log.info("Gracefully shutting down Korean Sunrise...")
        if self.stream_manager is not None:
            await self.stream_manager.stop_all()
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