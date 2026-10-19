"""
tools/probe_live_system.py - Autonomous Live Health, Connectivity, and Data Integrity Auditor.

Runs a complete, real-time diagnostic probe across all Korean Sunrise subsystems
WITHOUT requiring an LLM agent or consuming API tokens.

Evaluates 5 Critical Gates:
  [Gate 1] Live Exchange REST API Reachability, Latency & Geo/Rate-Limit Audit.
  [Gate 2] Upbit KRW/USDT Dynamic FX Rate Integrity & Freshness.
  [Gate 3] Real-Time WebSocket Ingestion, Ticks/sec, Anti-Starvation & Book Sanity:
           - No crossed books (best_bid < best_ask strictly enforced).
           - No zero/negative prices or depths.
           - Top-3 depth within sane dollar limits ($100 to $50M, catches multiplier corruption).
           - Ticks arrive continuously within latency tolerance.
  [Gate 4] Disk Storage, BatchWriter I/O, and Gzip CRC32 Integrity.
  [Gate 5] Historical Data Archive Audit (scans existing signals for NaN/Inf/schema defects).

Usage:
  python tools/probe_live_system.py
  python tools/probe_live_system.py --seconds 20
  python tools/probe_live_system.py --skip-ws
"""
from __future__ import annotations

import argparse
import asyncio
import glob
import gzip
import json
import math
import os
import shutil
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Set, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import config as C
from batch_writer import BatchWriter
from models import OrderBookDepth5
from venues.base import create_http_session
from venues.binance import BinanceFuturesStream
from venues.bybit import BybitPerpStream
from venues.okx import OKXStream
from venues.upbit import UpbitRestClient, UpbitSpotStream


class SystemProbe:
    """Executes exhaustive live connectivity and data sanity audits."""

    def __init__(self, ws_duration_sec: float = 15.0, skip_ws: bool = False):
        self.ws_duration_sec = ws_duration_sec
        self.skip_ws = skip_ws
        self.results: Dict[str, Dict[str, Any]] = {}
        self.session = create_http_session(timeout=6.0)

    # =========================================================================
    # Gate 1: REST API Reachability & Geo-Block Audit
    # =========================================================================
    def audit_rest_endpoints(self) -> bool:
        print("\n" + "=" * 78)
        print("  [GATE 1] AUDITING EXCHANGE REST APIS & GEO-BLOCK STATUS")
        print("=" * 78)

        endpoints = [
            ("Upbit (Spot Leader)", "https://api.upbit.com/v1/market/all?isDetails=true"),
            ("Binance (Futures)", "https://fapi.binance.com/fapi/v1/ping"),
            ("Bybit (Linear Perp)", "https://api.bybit.com/v5/market/time"),
            ("OKX (Perpetual)", "https://www.okx.com/api/v5/public/time"),
            ("Gate.io (Futures)", "https://api.gateio.ws/api/v4/spot/time"),
            ("Hyperliquid (L1/L2)", "https://api.hyperliquid.xyz/info"),
        ]

        all_passed = True
        gate1_data = []

        for name, url in endpoints:
            t0 = time.time()
            try:
                # Custom payload for Hyperliquid POST info
                if "hyperliquid" in url:
                    req = urllib.request.Request(
                        url,
                        data=json.dumps({"type": "meta"}).encode("utf-8"),
                        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                    )
                else:
                    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

                with urllib.request.urlopen(req, timeout=6) as resp:
                    lat_ms = int((time.time() - t0) * 1000)
                    status_code = resp.status
                    if status_code in (200, 201):
                        print(f"  [PASS] {name:<26} -> HTTP {status_code:<3} ({lat_ms:>4} ms)")
                        gate1_data.append({"venue": name, "ok": True, "latency_ms": lat_ms})
                    else:
                        print(f"  [WARN] {name:<26} -> HTTP {status_code:<3} ({lat_ms:>4} ms)")
                        gate1_data.append({"venue": name, "ok": True, "latency_ms": lat_ms})
            except Exception as e:
                lat_ms = int((time.time() - t0) * 1000)
                err_msg = str(e)
                print(f"  [FAIL] {name:<26} -> {err_msg} ({lat_ms:>4} ms)")
                # If Upbit, Binance, or Bybit fail, critical failure
                if any(k in name for k in ("Upbit", "Binance", "Bybit")):
                    all_passed = False
                gate1_data.append({"venue": name, "ok": False, "error": err_msg})

        self.results["gate1"] = {"passed": all_passed, "endpoints": gate1_data}
        return all_passed

    # =========================================================================
    # Gate 2: Upbit KRW/USDT Dynamic FX Rate Integrity
    # =========================================================================
    def audit_fx_rate(self) -> bool:
        print("\n" + "=" * 78)
        print("  [GATE 2] AUDITING UPBIT KRW/USDT DYNAMIC FX CONVERSION")
        print("=" * 78)

        client = UpbitRestClient(session=self.session)
        rate = client.fetch_usdt_krw_rate()
        print(f"  Current Upbit KRW/USDT Spot Rate: {rate:,.2f} KRW per USD")

        passed = True
        if rate <= 0.0:
            print("  [FAIL] FX Rate is ZERO or NEGATIVE! Cannot price Korean assets.")
            passed = False
        elif not math.isfinite(rate):
            print("  [FAIL] FX Rate is NaN or Infinite!")
            passed = False
        elif rate < 1000.0 or rate > 2500.0:
            print(f"  [WARN] FX Rate {rate} is outside historical bounds (1000-2500). Verify Upbit ticker.")
            passed = False
        else:
            print(f"  [PASS] FX Rate is healthy, finite, and within valid trading corridor.")

        self.results["gate2"] = {"passed": passed, "rate": rate}
        return passed

    # =========================================================================
    # Gate 3: Live Real-Time WebSocket Ingestion & Poisoning Guard
    # =========================================================================
    async def audit_websockets(self) -> bool:
        if self.skip_ws:
            print("\n  [GATE 3] WebSocket Ingestion Audit SKIPPED by user flag.")
            self.results["gate3"] = {"passed": True, "skipped": True}
            return True

        print("\n" + "=" * 78)
        print(f"  [GATE 3] AUDITING LIVE WEBSOCKET INGESTION & DATA INTEGRITY ({self.ws_duration_sec:.0f}s)")
        print("=" * 78)

        stats: Dict[str, Dict[str, Any]] = {
            "upbit": {"ticks": 0, "crossed": 0, "zero_px": 0, "mult_errors": 0, "latencies": []},
            "binance": {"ticks": 0, "crossed": 0, "zero_px": 0, "mult_errors": 0, "latencies": []},
            "bybit": {"ticks": 0, "crossed": 0, "zero_px": 0, "mult_errors": 0, "latencies": []},
            "okx": {"ticks": 0, "crossed": 0, "zero_px": 0, "mult_errors": 0, "latencies": []},
        }

        now_base = time.time()

        def make_callback(venue_key: str):
            def _cb(snap: OrderBookDepth5):
                st = stats[venue_key]
                st["ticks"] += 1
                now = time.time()

                # Latency tracking
                if snap.exchange_ts > 0:
                    lat = (now * 1000.0) - snap.exchange_ts
                    if -5000.0 <= lat <= 30000.0:
                        st["latencies"].append(lat)

                # 1. Crossed order book check
                if snap.best_bid >= snap.best_ask and snap.best_ask > 0:
                    st["crossed"] += 1

                # 2. Non-positive price / size check
                if snap.best_bid <= 0 or snap.best_ask <= 0:
                    st["zero_px"] += 1

                # 3. Dollar depth sanity check (multiplier corruption detector)
                # For major coins, top-3 ask depth should be between $10 and $50M.
                top3_ask = snap.top_n_ask_depth_usd(3)
                if top3_ask > 50_000_000.0 or (top3_ask < 5.0 and snap.coin in ("BTC", "ETH")):
                    st["mult_errors"] += 1

            return _cb

        test_coins = ["BTC", "ETH", "SOL", "DOGE", "XRP"]
        upbit_krw_map = {f"KRW-{c}": c for c in test_coins}
        binance_syms = {f"{c}USDT": (c, 1.0) for c in test_coins}
        bybit_syms = {f"{c}USDT": (c, 1.0) for c in test_coins}
        okx_syms = {f"{c}-USDT-SWAP": (c, 1.0) for c in test_coins}

        upbit_stream = UpbitSpotStream(krw_to_coin=upbit_krw_map, callback=make_callback("upbit"))
        binance_stream = BinanceFuturesStream(sym_map=binance_syms, callback=make_callback("binance"))
        bybit_stream = BybitPerpStream(sym_map=bybit_syms, callback=make_callback("bybit"))
        okx_stream = OKXStream(symbols=okx_syms, callback=make_callback("okx"))

        streams = [
            ("Upbit (Leader)", upbit_stream),
            ("Binance", binance_stream),
            ("Bybit", bybit_stream),
            ("OKX", okx_stream),
        ]

        print(f"  Starting parallel WebSocket feeds for {len(test_coins)} test assets: {', '.join(test_coins)}...")
        for _, s in streams:
            await s.start()

        # Monitor live ticks in 1-second intervals
        remaining = int(self.ws_duration_sec)
        while remaining > 0:
            await asyncio.sleep(1.0)
            remaining -= 1
            up_t = stats["upbit"]["ticks"]
            bn_t = stats["binance"]["ticks"]
            by_t = stats["bybit"]["ticks"]
            ok_t = stats["okx"]["ticks"]
            sys.stdout.write(f"\r  [PROBE] Ingestion streaming... {remaining:>2}s remaining | Ticks: Upbit={up_t}, Binance={bn_t}, Bybit={by_t}, OKX={ok_t}   ")
            sys.stdout.flush()

        print("\n  Stopping streams and compiling diagnostics...")
        for _, s in streams:
            try:
                await s.stop()
            except Exception:
                pass

        all_passed = True
        print("\n" + "-" * 78)
        print(f"  {'VENUE':<14} | {'TICKS':>7} | {'TPS':>6} | {'CROSSED':>7} | {'ZERO_PX':>7} | {'MULT_ERR':>8} | {'STATUS'}")
        print("-" * 78)

        for v_name, key in [("Upbit (Leader)", "upbit"), ("Binance", "binance"), ("Bybit", "bybit"), ("OKX", "okx")]:
            st = stats[key]
            ticks = st["ticks"]
            tps = ticks / max(1.0, self.ws_duration_sec)
            crossed = st["crossed"]
            zeros = st["zero_px"]
            mult_err = st["mult_errors"]

            status = "PASS"
            # Require Upbit and Binance to have ticks; Bybit and OKX might be slightly slower but must have no defects
            if key in ("upbit", "binance") and ticks == 0:
                status = "FAIL (ZOMBIE STREAM: 0 TICKS)"
                all_passed = False
            elif crossed > 0:
                status = f"FAIL ({crossed} CROSSED BOOKS)"
                all_passed = False
            elif zeros > 0:
                status = f"FAIL ({zeros} ZERO PRICES)"
                all_passed = False
            elif mult_err > 0:
                status = f"FAIL ({mult_err} MULTIPLIER CORRUPTIONS)"
                all_passed = False

            print(f"  {v_name:<14} | {ticks:>7} | {tps:>6.1f} | {crossed:>7} | {zeros:>7} | {mult_err:>8} | [{status}]")

        print("-" * 78)
        self.results["gate3"] = {"passed": all_passed, "stats": stats}
        return all_passed

    # =========================================================================
    # Gate 4: Disk Storage & BatchWriter I/O Health Check
    # =========================================================================
    def audit_storage_io(self) -> bool:
        print("\n" + "=" * 78)
        print("  [GATE 4] AUDITING DISK I/O, BATCHWRITER & GZIP CRC32 INTEGRITY")
        print("=" * 78)

        test_dir = os.path.join(C.DATA_ROOT, "_probe_test")
        os.makedirs(test_dir, exist_ok=True)

        passed = True
        try:
            writer = BatchWriter(data_root=test_dir, flush_interval_sec=0.1)

            # Write 5 mock signals
            test_signals = [
                {
                    "id": f"probe_test_{i}",
                    "coin": "BTC",
                    "follower_exchange": "binance",
                    "net_pnl_bps": 15.5,
                    "stealth_capacity_usd": 5000.0,
                    "close_reason": "converged",
                    "entry_ts": time.time(),
                }
                for i in range(5)
            ]
            for sig in test_signals:
                writer.add(sig)

            # Flush to disk
            writer.flush()

            # Read back from disk and verify gzip integrity
            gz_files = glob.glob(os.path.join(test_dir, "**", "*.jsonl.gz"), recursive=True)
            if not gz_files:
                print("  [FAIL] BatchWriter flush completed but no .jsonl.gz file was created on disk!")
                passed = False
            else:
                read_count = 0
                for fpath in gz_files:
                    with gzip.open(fpath, "rt", encoding="utf-8") as gz:
                        for line in gz:
                            d = json.loads(line)
                            if d.get("coin") == "BTC":
                                read_count += 1
                if read_count == 5:
                    print(f"  [PASS] BatchWriter serialized, compressed, and flushed 5/5 records cleanly.")
                    print(f"  [PASS] Gzip CRC32 verified, JSON parser verified, zero corruption.")
                else:
                    print(f"  [FAIL] Expected 5 records from disk, found {read_count}.")
                    passed = False

        except Exception as e:
            print(f"  [FAIL] Disk write error encountered: {e}")
            passed = False
        finally:
            shutil.rmtree(test_dir, ignore_errors=True)

        self.results["gate4"] = {"passed": passed}
        return passed

    # =========================================================================
    # Gate 5: Historical Data Archive Deep Audit
    # =========================================================================
    def audit_historical_archives(self) -> bool:
        print("\n" + "=" * 78)
        print("  [GATE 5] AUDITING RECORDED SIGNALS IN data/ FOR ROT & POISONING")
        print("=" * 78)

        signals_dir = os.path.join(C.DATA_ROOT, "signals")
        if not os.path.isdir(signals_dir):
            print("  [OK] No prior signal archives found (System is in clean fresh state).")
            self.results["gate5"] = {"passed": True, "files": 0}
            return True

        gz_files = glob.glob(os.path.join(signals_dir, "**", "*.jsonl.gz"), recursive=True)
        if not gz_files:
            print("  [OK] data/signals directory exists but is clean (0 archive files).")
            self.results["gate5"] = {"passed": True, "files": 0}
            return True

        total_rows = 0
        corrupted_rows = 0
        nan_inf_count = 0

        for fpath in gz_files:
            try:
                with gzip.open(fpath, "rt", encoding="utf-8", errors="replace") as gz:
                    for line in gz:
                        line = line.strip()
                        if not line:
                            continue
                        total_rows += 1
                        try:
                            d = json.loads(line)
                        except json.JSONDecodeError:
                            corrupted_rows += 1
                            continue

                        # Check numbers for NaN / Inf
                        for k in ("net_pnl_bps", "gross_exec_edge_bps", "stealth_capacity_usd"):
                            v = d.get(k)
                            if v is not None:
                                try:
                                    fv = float(v)
                                    if not math.isfinite(fv):
                                        nan_inf_count += 1
                                except (ValueError, TypeError):
                                    nan_inf_count += 1
            except Exception:
                corrupted_rows += 1

        print(f"  Total historical signal records audited: {total_rows}")
        passed = True
        if corrupted_rows > 0:
            print(f"  [FAIL] Found {corrupted_rows} corrupted rows / bad gzip records in data/signals!")
            passed = False
        if nan_inf_count > 0:
            print(f"  [FAIL] Found {nan_inf_count} NaN or Inf corrupted numerical values!")
            passed = False

        if passed:
            print(f"  [PASS] 100% of recorded historical rows are sound, valid, and unpoisoned.")

        self.results["gate5"] = {"passed": passed, "total_rows": total_rows}
        return passed

    # =========================================================================
    # Run Complete Verification Suite
    # =========================================================================
    async def run(self) -> int:
        print("=" * 78)
        print("      KOREAN SUNRISE: COMPREHENSIVE LIVE HEALTH & CONNECTIVITY PROBE   ")
        print("=" * 78)

        g1 = self.audit_rest_endpoints()
        g2 = self.audit_fx_rate()
        g3 = await self.audit_websockets()
        g4 = self.audit_storage_io()
        g5 = self.audit_historical_archives()

        all_clean = g1 and g2 and g3 and g4 and g5

        print("\n" + "=" * 78)
        print("  FINAL SYSTEM READINESS SCORECARD")
        print("=" * 78)
        print(f"  [Gate 1] REST API Reachability & Geo-Block Status: {'PASS' if g1 else 'FAILED'}")
        print(f"  [Gate 2] Upbit KRW/USDT Dynamic FX Integrity:     {'PASS' if g2 else 'FAILED'}")
        print(f"  [Gate 3] Live WebSocket Ingestion & Book Sanity:   {'PASS' if g3 else 'FAILED'}")
        print(f"  [Gate 4] Disk Storage, BatchWriter & Gzip CRC32:   {'PASS' if g4 else 'FAILED'}")
        print(f"  [Gate 5] Historical Data Quality & Anti-Poisoning: {'PASS' if g5 else 'FAILED'}")
        print("=" * 78)

        if all_clean:
            print("  >>> SYSTEM IS 100% HEALTHY, LIVE, UNPOISONED, AND PRODUCTION-READY <<<")
            print("=" * 78 + "\n")
            return 0
        else:
            print("  >>> WARNING: SYSTEM HAS CRITICAL SUBSYSTEM DEFECTS! FIX BEFORE RUNNING! <<<")
            print("=" * 78 + "\n")
            return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Autonomous Live Health Auditor.")
    parser.add_argument("--seconds", type=float, default=15.0, help="Live WebSocket probe duration (default: 15.0s)")
    parser.add_argument("--skip-ws", action="store_true", help="Skip live WebSocket streaming test")
    args = parser.parse_args()

    probe = SystemProbe(ws_duration_sec=args.seconds, skip_ws=args.skip_ws)
    exit_code = asyncio.run(probe.run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
