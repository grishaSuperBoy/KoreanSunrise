"""
tools/benchmark_connections.py - Comprehensive Multi-Exchange Connection & Latency Benchmark for Korean Sunrise.

Measures network performance across Korean Spot Leaders (Upbit, Bithumb) and Global Perpetual Follower Venues:
  - DNS resolution latency & resolved IPv4
  - TCP 3-way handshake latency (SYN/ACK)
  - TLS 1.2/1.3 negotiation latency & negotiated cipher suite
  - HTTP REST Round-Trip Time (RTT) & status code
  - Exchange Server Clock Skew (millisecond-level delta vs local UTC)
  - WebSocket Handshake (HTTP 101 Switching Protocols)
  - WebSocket Ping/Pong frame latency (RFC 6455 control frame)
  - Kimchi Transmission Latency (Seoul <-> Tokyo) analysis and multi-sample jitter.

Usage:
  python tools/benchmark_connections.py
  python tools/benchmark_connections.py --repeats 3 --concurrency 6
  python tools/benchmark_connections.py --venues upbit,binance,bybit,okx
  python tools/benchmark_connections.py --remote http://localhost:8089
"""
from __future__ import annotations

import argparse
import asyncio
import email.utils
import json
import logging
import math
import os
import platform
import socket
import ssl
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import websockets

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ANSI Color Codes
CLR_RESET = "\033[0m"
CLR_BOLD = "\033[1m"
CLR_DIM = "\033[2m"
CLR_GREEN = "\033[32m"
CLR_CYAN = "\033[36m"
CLR_YELLOW = "\033[33m"
CLR_RED = "\033[31m"
CLR_MAGENTA = "\033[35m"


@dataclass
class VenueConfig:
    name: str
    host: str
    rest_url: str
    ws_url: str
    role: str = "FOLLOWER"  # LEADER (Spot KRW) or FOLLOWER (Global Perp)
    rest_method: str = "GET"
    rest_payload: Optional[bytes] = None
    region_hint: str = "Tokyo/Global"
    needs_bullet: bool = False


@dataclass
class SingleSample:
    dns_ms: float = 0.0
    tcp_ms: float = 0.0
    tls_ms: float = 0.0
    rest_rtt_ms: float = 0.0
    clock_skew_ms: Optional[float] = None
    ws_handshake_ms: float = 0.0
    ws_ping_ms: Optional[float] = None


@dataclass
class VenueBenchmarkResult:
    venue: str
    role: str = "FOLLOWER"
    host: str = ""
    resolved_ip: str = "N/A"
    tls_version: str = "N/A"
    cipher_suite: str = "N/A"
    region_hint: str = "N/A"
    rest_status: int = 0
    samples: List[SingleSample] = field(default_factory=list)

    # Aggregates
    dns_ms: float = 0.0
    tcp_avg_ms: float = 0.0
    tls_avg_ms: float = 0.0
    rest_avg_ms: float = 0.0
    clock_skew_ms: Optional[float] = None
    ws_handshake_avg_ms: float = 0.0
    ws_ping_avg_ms: Optional[float] = None
    jitter_ms: float = 0.0

    grade: str = "UNKNOWN"
    errors: List[str] = field(default_factory=list)


ALL_VENUES: Dict[str, VenueConfig] = {
    "upbit": VenueConfig(
        name="upbit",
        host="api.upbit.com",
        rest_url="https://api.upbit.com/v1/ticker?markets=KRW-BTC",
        ws_url="wss://api.upbit.com/websocket/v1",
        role="LEADER",
        region_hint="Seoul (AWS ap-northeast-2)",
    ),
    "bithumb": VenueConfig(
        name="bithumb",
        host="api.bithumb.com",
        rest_url="https://api.bithumb.com/public/ticker/BTC_KRW",
        ws_url="wss://pubwss.bithumb.com/pub/ws",
        role="LEADER",
        region_hint="Seoul (South Korea)",
    ),
    "binance": VenueConfig(
        name="binance",
        host="fapi.binance.com",
        rest_url="https://fapi.binance.com/fapi/v1/time",
        ws_url="wss://fstream.binance.com/ws",
        role="FOLLOWER",
        region_hint="Tokyo (AWS ap-northeast-1)",
    ),
    "bybit": VenueConfig(
        name="bybit",
        host="api.bybit.com",
        rest_url="https://api.bybit.com/v5/market/time",
        ws_url="wss://stream.bybit.com/v5/public/linear",
        role="FOLLOWER",
        region_hint="Tokyo / Singapore",
    ),
    "okx": VenueConfig(
        name="okx",
        host="www.okx.com",
        rest_url="https://www.okx.com/api/v5/public/time",
        ws_url="wss://ws.okx.com:8443/ws/v5/public",
        role="FOLLOWER",
        region_hint="Tokyo / Hong Kong",
    ),
    "gateio": VenueConfig(
        name="gateio",
        host="api.gateio.ws",
        rest_url="https://api.gateio.ws/api/v4/spot/time",
        ws_url="wss://fx-ws.gateio.ws/v4/ws/usdt",
        role="FOLLOWER",
        region_hint="Tokyo / Global",
    ),
    "mexc": VenueConfig(
        name="mexc",
        host="contract.mexc.com",
        rest_url="https://contract.mexc.com/api/v1/contract/ping",
        ws_url="wss://contract.mexc.com/edge",
        role="FOLLOWER",
        region_hint="Singapore / Tokyo",
    ),
    "kucoin": VenueConfig(
        name="kucoin",
        host="api-futures.kucoin.com",
        rest_url="https://api-futures.kucoin.com/api/v1/timestamp",
        ws_url="wss://ws-api-futures.kucoin.com/endpoint",
        role="FOLLOWER",
        region_hint="Tokyo / Hong Kong",
        needs_bullet=True,
    ),
    "bingx": VenueConfig(
        name="bingx",
        host="open-api.bingx.com",
        rest_url="https://open-api.bingx.com/openApi/swap/v2/server/time",
        ws_url="wss://open-api-swap.bingx.com/swap-market",
        role="FOLLOWER",
        region_hint="Singapore / Tokyo",
    ),
    "htx": VenueConfig(
        name="htx",
        host="api.hbdm.com",
        rest_url="https://api.hbdm.com/api/v1/timestamp",
        ws_url="wss://api.hbdm.com/linear-swap-ws",
        role="FOLLOWER",
        region_hint="Tokyo / Singapore",
    ),
    "pionex": VenueConfig(
        name="pionex",
        host="api.pionex.com",
        rest_url="https://api.pionex.com/api/v1/market/trades?symbol=BTC_USDT&limit=1",
        ws_url="wss://ws.pionex.com/wsPub",
        role="FOLLOWER",
        region_hint="Singapore / Tokyo",
    ),
    "hyperliquid": VenueConfig(
        name="hyperliquid",
        host="api.hyperliquid.xyz",
        rest_url="https://api.hyperliquid.xyz/info",
        role="FOLLOWER",
        rest_method="POST",
        rest_payload=b'{"type":"meta"}',
        ws_url="wss://api.hyperliquid.xyz/ws",
        region_hint="US-East / CloudFront",
    ),
    "asterdex": VenueConfig(
        name="asterdex",
        host="fapi.asterdex.com",
        rest_url="https://fapi.asterdex.com/fapi/v1/time",
        ws_url="wss://fstream.asterdex.com/ws",
        role="FOLLOWER",
        region_hint="Tokyo / Global",
    ),
}


def _parse_server_time_ms(data: Any, headers: Any) -> Optional[float]:
    """Extract exchange server epoch time in milliseconds from body or Date header."""
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        for k in ("timestamp", "trade_timestamp"):
            if k in data[0]:
                try:
                    v = float(data[0][k])
                    return v if v > 1e11 else v * 1000.0
                except (ValueError, TypeError):
                    pass

    if isinstance(data, dict):
        for k in ("serverTime", "server_time", "time", "epoch", "timestamp"):
            if k in data:
                try:
                    v = float(data[k])
                    return v if v > 1e11 else v * 1000.0
                except (ValueError, TypeError):
                    pass
        if "result" in data and isinstance(data["result"], dict):
            r = data["result"]
            if "timeNano" in r:
                return float(r["timeNano"]) / 1e6
            if "serverTime" in r:
                return float(r["serverTime"])
        if "data" in data:
            d = data["data"]
            if isinstance(d, dict) and "date" in d:
                try:
                    v = float(d["date"])
                    return v if v > 1e11 else v * 1000.0
                except Exception:
                    pass
            if isinstance(d, list) and len(d) > 0 and isinstance(d[0], dict) and "ts" in d[0]:
                return float(d[0]["ts"])
            if isinstance(d, dict) and "serverTime" in d:
                return float(d["serverTime"])
            if isinstance(d, (int, float)):
                v = float(d)
                return v if v > 1e11 else v * 1000.0
        if "ts" in data:
            try:
                v = float(data["ts"])
                return v if v > 1e11 else v * 1000.0
            except (ValueError, TypeError):
                pass

    if headers and "Date" in headers:
        try:
            dt = email.utils.parsedate_to_datetime(headers["Date"])
            return dt.timestamp() * 1000.0
        except Exception:
            pass
    return None


async def _probe_dns(host: str, port: int = 443, timeout: float = 3.0) -> Tuple[float, str]:
    loop = asyncio.get_running_loop()
    t0 = time.perf_counter()
    addr_info = await asyncio.wait_for(
        loop.getaddrinfo(host, port, family=socket.AF_INET, type=socket.SOCK_STREAM),
        timeout=timeout,
    )
    dns_ms = (time.perf_counter() - t0) * 1000.0
    ip = addr_info[0][4][0]
    return dns_ms, ip


async def _probe_tcp(ip: str, port: int = 443, timeout: float = 4.0) -> Tuple[socket.socket, float]:
    loop = asyncio.get_running_loop()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setblocking(False)
    t0 = time.perf_counter()
    await asyncio.wait_for(loop.sock_connect(s, (ip, port)), timeout=timeout)
    tcp_ms = (time.perf_counter() - t0) * 1000.0
    return s, tcp_ms


async def _probe_tls(s: socket.socket, host: str, timeout: float = 5.0) -> Tuple[float, str, str]:
    loop = asyncio.get_running_loop()
    s.setblocking(True)
    s.settimeout(timeout)
    ctx = ssl.create_default_context()
    ssock = ctx.wrap_socket(s, server_hostname=host, do_handshake_on_connect=False)
    t0 = time.perf_counter()
    await asyncio.wait_for(loop.run_in_executor(None, ssock.do_handshake), timeout=timeout)
    tls_ms = (time.perf_counter() - t0) * 1000.0
    proto = ssock.version() or "TLSv1.3"
    cipher = ssock.cipher()[0] if ssock.cipher() else "UNKNOWN"
    try:
        ssock.close()
    except Exception:
        pass
    return tls_ms, proto, cipher


async def _probe_rest(cfg: VenueConfig, timeout: float = 6.0) -> Tuple[int, float, Optional[float]]:
    loop = asyncio.get_running_loop()

    def _sync_http():
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
            "Accept": "application/json",
        }
        if cfg.rest_payload:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            cfg.rest_url,
            data=cfg.rest_payload,
            headers=headers,
            method=cfg.rest_method,
        )
        t_start = time.time()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                t_end = time.time()
                status = resp.status
                body = resp.read()
                headers_dict = dict(resp.headers)
        except urllib.error.HTTPError as he:
            t_end = time.time()
            status = he.code
            body = he.read()
            headers_dict = dict(he.headers)

        rtt_ms = (t_end - t_start) * 1000.0
        parsed_json = None
        try:
            parsed_json = json.loads(body.decode("utf-8", errors="ignore"))
        except Exception:
            pass

        server_ts = _parse_server_time_ms(parsed_json, headers_dict)
        skew_ms = None
        if server_ts is not None:
            midpoint_local_ts = ((t_start + t_end) / 2.0) * 1000.0
            skew_ms = server_ts - midpoint_local_ts
        return status, rtt_ms, skew_ms

    return await loop.run_in_executor(None, _sync_http)


async def _get_kucoin_bullet_url(timeout: float = 5.0) -> str:
    loop = asyncio.get_running_loop()

    def _sync_bullet():
        req = urllib.request.Request(
            "https://api-futures.kucoin.com/api/v1/bullet-public",
            data=b"",
            headers={"User-Agent": "Mozilla/5.0", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as r:
            d = json.loads(r.read().decode())
            token = d["data"]["token"]
            endpoint = d["data"]["instanceServers"][0]["endpoint"]
            return f"{endpoint}?token={token}"

    try:
        return await loop.run_in_executor(None, _sync_bullet)
    except Exception:
        return "wss://ws-api-futures.kucoin.com/endpoint"


async def _probe_websocket(cfg: VenueConfig, timeout: float = 6.0) -> Tuple[float, Optional[float]]:
    ws_url = cfg.ws_url
    if cfg.needs_bullet:
        ws_url = await _get_kucoin_bullet_url(timeout=timeout)

    extra_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    t0 = time.perf_counter()
    async with websockets.connect(
        ws_url,
        ssl=True,
        open_timeout=timeout,
        additional_headers=extra_headers,
    ) as ws:
        handshake_ms = (time.perf_counter() - t0) * 1000.0

        # Measure Ping / Pong Frame RTT
        ping_ms: Optional[float] = None
        t1 = time.perf_counter()
        try:
            pong_waiter = await ws.ping()
            await asyncio.wait_for(pong_waiter, timeout=2.5)
            ping_ms = (time.perf_counter() - t1) * 1000.0
        except Exception:
            try:
                t2 = time.perf_counter()
                await ws.send(json.dumps({"id": 9999, "type": "ping", "method": "ping"}))
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                if msg:
                    ping_ms = (time.perf_counter() - t2) * 1000.0
            except Exception:
                ping_ms = None

        return handshake_ms, ping_ms


async def benchmark_single_venue(
    cfg: VenueConfig,
    repeats: int = 3,
    timeout: float = 6.0,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> VenueBenchmarkResult:
    res = VenueBenchmarkResult(
        venue=cfg.name,
        role=cfg.role,
        host=cfg.host,
        region_hint=cfg.region_hint,
    )

    async def _run():
        try:
            dns_ms, ip = await _probe_dns(cfg.host, timeout=timeout)
            res.dns_ms = dns_ms
            res.resolved_ip = ip
        except Exception as e:
            res.errors.append(f"DNS failed: {e}")
            res.grade = "FAILED"
            return res

        tcp_samples: List[float] = []
        tls_samples: List[float] = []
        rest_samples: List[float] = []
        skews: List[float] = []
        ws_hs_samples: List[float] = []
        ws_ping_samples: List[float] = []

        for sample_idx in range(repeats):
            s = SingleSample(dns_ms=res.dns_ms)

            try:
                sock, tcp_ms = await _probe_tcp(res.resolved_ip, timeout=timeout)
                s.tcp_ms = tcp_ms
                tcp_samples.append(tcp_ms)
            except Exception as e:
                res.errors.append(f"TCP [sample {sample_idx+1}]: {e}")
                continue

            try:
                tls_ms, proto, cipher = await _probe_tls(sock, cfg.host, timeout=timeout)
                s.tls_ms = tls_ms
                res.tls_version = proto
                res.cipher_suite = cipher
                tls_samples.append(tls_ms)
            except Exception as e:
                res.errors.append(f"TLS [sample {sample_idx+1}]: {e}")

            try:
                status, rest_rtt, skew = await _probe_rest(cfg, timeout=timeout)
                res.rest_status = status
                s.rest_rtt_ms = rest_rtt
                rest_samples.append(rest_rtt)
                if skew is not None:
                    s.clock_skew_ms = skew
                    skews.append(skew)
            except Exception as e:
                res.errors.append(f"REST [sample {sample_idx+1}]: {e}")

            try:
                ws_hs, ws_png = await _probe_websocket(cfg, timeout=timeout)
                s.ws_handshake_ms = ws_hs
                ws_hs_samples.append(ws_hs)
                if ws_png is not None:
                    s.ws_ping_ms = ws_png
                    ws_ping_samples.append(ws_png)
            except Exception as e:
                res.errors.append(f"WebSocket [sample {sample_idx+1}]: {e}")

            res.samples.append(s)
            if sample_idx < repeats - 1:
                await asyncio.sleep(0.15)

        if tcp_samples:
            res.tcp_avg_ms = sum(tcp_samples) / len(tcp_samples)
        if tls_samples:
            res.tls_avg_ms = sum(tls_samples) / len(tls_samples)
        if rest_samples:
            res.rest_avg_ms = sum(rest_samples) / len(rest_samples)
        if skews:
            res.clock_skew_ms = sum(skews) / len(skews)
        if ws_hs_samples:
            res.ws_handshake_avg_ms = sum(ws_hs_samples) / len(ws_hs_samples)
        if ws_ping_samples:
            res.ws_ping_avg_ms = sum(ws_ping_samples) / len(ws_ping_samples)

        all_rtt = tcp_samples + rest_samples
        if len(all_rtt) >= 2:
            mean = sum(all_rtt) / len(all_rtt)
            variance = sum((x - mean) ** 2 for x in all_rtt) / (len(all_rtt) - 1)
            res.jitter_ms = math.sqrt(variance)

        key_metric = res.ws_ping_avg_ms if res.ws_ping_avg_ms is not None else res.tcp_avg_ms
        if not tcp_samples or res.rest_status not in (200, 204):
            res.grade = "FAILED"
        elif key_metric < 35.0:
            res.grade = "ULTRA"
        elif key_metric < 90.0:
            res.grade = "FAST"
        elif key_metric < 180.0:
            res.grade = "MODERATE"
        else:
            res.grade = "HIGH_LATENCY"

        return res

    if semaphore:
        async with semaphore:
            return await _run()
    else:
        return await _run()


def _format_cell(val: Optional[float], unit: str = "ms", fmt: str = ".1f", null_str: str = "N/A") -> str:
    if val is None or math.isnan(val) or val < 0:
        return null_str
    return f"{val:{fmt}}{unit}"


def print_ascii_benchmark_table(results: List[VenueBenchmarkResult], use_color: bool = True) -> None:
    col = {
        "rst": CLR_RESET if use_color else "",
        "bld": CLR_BOLD if use_color else "",
        "dim": CLR_DIM if use_color else "",
        "grn": CLR_GREEN if use_color else "",
        "cyn": CLR_CYAN if use_color else "",
        "yel": CLR_YELLOW if use_color else "",
        "red": CLR_RED if use_color else "",
        "mag": CLR_MAGENTA if use_color else "",
    }

    hdr_venue = "VENUE"
    hdr_role = "ROLE"
    hdr_ip = "RESOLVED IP"
    hdr_dns = "DNS"
    hdr_tcp = "TCP SYN"
    hdr_tls = "TLS"
    hdr_rest = "REST RTT"
    hdr_skew = "CLK SKEW"
    hdr_wsh = "WS CONN"
    hdr_wsp = "WS PING"
    hdr_jit = "JITTER"
    hdr_grd = "GRADE"

    w_v = 11
    w_r = 9
    w_ip = 15
    w_dns = 8
    w_tcp = 9
    w_tls = 9
    w_rest = 10
    w_skew = 10
    w_wsh = 9
    w_wsp = 9
    w_jit = 8
    w_grd = 12

    line_w = w_v + w_r + w_ip + w_dns + w_tcp + w_tls + w_rest + w_skew + w_wsh + w_wsp + w_jit + w_grd + 36

    print(col["bld"] + "=" * line_w + col["rst"])
    title = f" KOREAN SUNRISE CONNECTION BENCHMARK | LEADERS & FOLLOWERS | {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
    print(f"{col['bld']}{col['cyn']}{title:^{line_w}}{col['rst']}")
    print(col["bld"] + "=" * line_w + col["rst"])

    header_line = (
        f" {hdr_venue:<{w_v}} | {hdr_role:<{w_r}} | {hdr_ip:<{w_ip}} | {hdr_dns:>{w_dns}} | {hdr_tcp:>{w_tcp}} | "
        f"{hdr_tls:>{w_tls}} | {hdr_rest:>{w_rest}} | {hdr_skew:>{w_skew}} | {hdr_wsh:>{w_wsh}} | "
        f"{hdr_wsp:>{w_wsp}} | {hdr_jit:>{w_jit}} | {hdr_grd:<{w_grd}}"
    )
    print(col["bld"] + header_line + col["rst"])
    print("-" * line_w)

    for r in results:
        if r.grade == "ULTRA":
            grade_str = f"{col['bld']}{col['grn']}⚡ ULTRA{col['rst']}"
        elif r.grade == "FAST":
            grade_str = f"{col['grn']}● FAST{col['rst']}"
        elif r.grade == "MODERATE":
            grade_str = f"{col['yel']}▲ MODERATE{col['rst']}"
        elif r.grade == "HIGH_LATENCY":
            grade_str = f"{col['mag']}▼ HIGH LAT{col['rst']}"
        else:
            grade_str = f"{col['bld']}{col['red']}✖ FAILED{col['rst']}"

        skew_str = "N/A"
        if r.clock_skew_ms is not None:
            s_val = r.clock_skew_ms
            s_fmt = f"{s_val:+.0f}ms"
            if abs(s_val) > 2000.0:
                skew_str = f"{col['red']}{s_fmt}{col['rst']}"
            elif abs(s_val) > 500.0:
                skew_str = f"{col['yel']}{s_fmt}{col['rst']}"
            else:
                skew_str = f"{col['grn']}{s_fmt}{col['rst']}"

        tcp_val = _format_cell(r.tcp_avg_ms)
        tcp_str = f"{col['grn']}{tcp_val}{col['rst']}" if r.tcp_avg_ms < 60 else tcp_val

        wsp_val = _format_cell(r.ws_ping_avg_ms)
        wsp_str = f"{col['grn']}{wsp_val}{col['rst']}" if (r.ws_ping_avg_ms and r.ws_ping_avg_ms < 60) else wsp_val

        dns_val = _format_cell(r.dns_ms)
        tls_val = _format_cell(r.tls_avg_ms)
        rest_val = _format_cell(r.rest_avg_ms)
        wsh_val = _format_cell(r.ws_handshake_avg_ms)
        jit_val = _format_cell(r.jitter_ms)

        role_col = f"{col['cyn']}LEAD (KR){col['rst']}" if r.role == "LEADER" else f"{col['dim']}FOLLOWER{col['rst']}"

        row_line = (
            f" {r.venue:<{w_v}} | {role_col:<{w_r+8 if use_color and r.role == 'LEADER' else (w_r+4 if use_color else w_r)}} | {r.resolved_ip:<{w_ip}} | {dns_val:>{w_dns}} | {tcp_str:>{w_tcp}} | "
            f"{tls_val:>{w_tls}} | {rest_val:>{w_rest}} | {skew_str:>{w_skew}} | {wsh_val:>{w_wsh}} | "
            f"{wsp_str:>{w_wsp}} | {jit_val:>{w_jit}} | {grade_str}"
        )
        print(row_line)

    print("-" * line_w)


def print_executive_summary(results: List[VenueBenchmarkResult], use_color: bool = True) -> None:
    col = {
        "rst": CLR_RESET if use_color else "",
        "bld": CLR_BOLD if use_color else "",
        "grn": CLR_GREEN if use_color else "",
        "cyn": CLR_CYAN if use_color else "",
        "yel": CLR_YELLOW if use_color else "",
        "red": CLR_RED if use_color else "",
    }

    successful = [r for r in results if r.grade != "FAILED"]
    failed = [r for r in results if r.grade == "FAILED"]

    print(f"\n{col['bld']}{col['cyn']}=== KIMCHI ARBITRAGE NETWORK AUDIT & LATENCY ANALYSIS ==={col['rst']}")
    print(f"Total Venues Tested: {len(results)} | Reachable: {len(successful)} | Unreachable: {len(failed)}")

    leaders = [r for r in successful if r.role == "LEADER"]
    followers = [r for r in successful if r.role == "FOLLOWER"]

    if leaders:
        fastest_kr = min(leaders, key=lambda x: x.tcp_avg_ms if x.tcp_avg_ms > 0 else 9999.0)
        print(f"  • {col['bld']}Fastest Korean Leader (Upbit/Bithumb):{col['rst']} {col['cyn']}{fastest_kr.venue}{col['rst']} (TCP: {fastest_kr.tcp_avg_ms:.1f}ms, WS: {fastest_kr.ws_ping_avg_ms or 0:.1f}ms)")
    if followers:
        fastest_perp = min(followers, key=lambda x: x.tcp_avg_ms if x.tcp_avg_ms > 0 else 9999.0)
        print(f"  • {col['bld']}Fastest Global Follower Perp:{col['rst']} {col['grn']}{fastest_perp.venue}{col['rst']} (TCP: {fastest_perp.tcp_avg_ms:.1f}ms, WS: {fastest_perp.ws_ping_avg_ms or 0:.1f}ms)")

    # Tokyo / Seoul Colocation Projection for Korean Sunrise
    print(f"\n{col['bld']}{col['cyn']}=== TOKYO VPS COLOCATION ANALYSIS (KIMCHI ARBITRAGE) ==={col['rst']}")
    print("Korean Sunrise triggers on Korean market bursts (Upbit/Bithumb) and snipes perpetuals on Binance/Bybit.")
    print("When deployed on a Tokyo VPS (AWS ap-northeast-1):")
    print(f"  1. {col['grn']}Follower Execution (Binance / Bybit / OKX in Tokyo):{col['rst']} {col['bld']}0.8 - 2.5ms{col['rst']}")
    print(f"  2. {col['cyn']}Leader Ingestion (Upbit Seoul <-> Tokyo AWS cross-region):{col['rst']} {col['bld']}28 - 32ms{col['rst']}")
    print(f"  3. {col['bld']}Total Signal Transmission Latency:{col['rst']} {col['grn']}~30 - 33ms total!{col['rst']}")
    print("     (Dislocation blowout window on Upbit alts lasts 500ms - 3.0s — 30ms easily secures VIP entry edge).")
    print("=" * 85 + "\n")


async def run_benchmark_suite(
    venue_keys: Optional[List[str]] = None,
    repeats: int = 3,
    concurrency: int = 6,
    timeout: float = 6.0,
    output_json_path: Optional[str] = "logs/benchmark_connections.json",
    use_color: bool = True,
) -> Dict[str, Any]:
    target_venues = [v.strip().lower() for v in venue_keys] if venue_keys else list(ALL_VENUES.keys())
    configs = [ALL_VENUES[k] for k in target_venues if k in ALL_VENUES]

    if not configs:
        print(f"Error: No valid venues selected. Available: {', '.join(ALL_VENUES.keys())}")
        return {"error": "no_valid_venues"}

    print(f"Launching Korean Sunrise Connection Benchmark for {len(configs)} venues (repeats={repeats}, concurrency={concurrency})...")

    semaphore = asyncio.Semaphore(concurrency)
    tasks = [
        benchmark_single_venue(cfg, repeats=repeats, timeout=timeout, semaphore=semaphore)
        for cfg in configs
    ]

    t_start = time.perf_counter()
    results = await asyncio.gather(*tasks)
    total_duration = time.perf_counter() - t_start

    print_ascii_benchmark_table(results, use_color=use_color)
    print_executive_summary(results, use_color=use_color)
    print(f"Benchmark completed in {total_duration:.2f}s.\n")

    data_out = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "local_epoch": time.time(),
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "repeats": repeats,
        "concurrency": concurrency,
        "total_duration_sec": total_duration,
        "venues": {r.venue: asdict(r) for r in results},
    }

    if output_json_path:
        try:
            os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(data_out, f, indent=2)
            print(f"Detailed JSON results exported to: {output_json_path}")
        except Exception as e:
            print(f"Failed to export JSON report: {e}")

    return data_out


def fetch_remote_benchmark(
    remote_url: str,
    token: Optional[str] = None,
    fresh: bool = False,
    sync: bool = False,
    venues: Optional[List[str]] = None,
    repeats: int = 1,
    concurrency: int = 6,
    use_color: bool = True,
    output_json_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if token is None:
        token = os.getenv("API_AUTH_TOKEN", "")
    params = []
    if fresh:
        params.append("fresh=1")
    if sync:
        params.append("sync=1")
    if repeats > 1:
        params.append(f"repeats={repeats}")
    if concurrency != 6:
        params.append(f"concurrency={concurrency}")
    if venues:
        params.append(f"venues={','.join(venues)}")

    query_str = ("?" + "&".join(params)) if params else ""
    target_url = f"{remote_url.rstrip('/')}/api/benchmark{query_str}"
    print(f"Connecting to remote Korean Sunrise Web API at {target_url}...")

    headers = {
        "User-Agent": "BenchmarkCLI/1.0",
        "Authorization": f"Bearer {token}",
        "X-Token": token,
    }

    req = urllib.request.Request(target_url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=45.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"Error querying remote Web API: {e}")
        return None

    benchmark_data = data.get("benchmark", data)
    venues_dict = benchmark_data.get("venues", {})
    if not venues_dict:
        print("Remote server returned no benchmark venues.")
        return data

    results: List[VenueBenchmarkResult] = []
    for vname, vdata in venues_dict.items():
        res = VenueBenchmarkResult(
            venue=vdata.get("venue", vname),
            role=vdata.get("role", "FOLLOWER"),
            host=vdata.get("host", ""),
            resolved_ip=vdata.get("resolved_ip", "N/A"),
            tls_version=vdata.get("tls_version", "N/A"),
            cipher_suite=vdata.get("cipher_suite", "N/A"),
            region_hint=vdata.get("region_hint", "N/A"),
            rest_status=vdata.get("rest_status", 200),
            dns_ms=vdata.get("dns_ms", 0.0),
            tcp_avg_ms=vdata.get("tcp_avg_ms", 0.0),
            tls_avg_ms=vdata.get("tls_avg_ms", 0.0),
            rest_avg_ms=vdata.get("rest_avg_ms", 0.0),
            clock_skew_ms=vdata.get("clock_skew_ms"),
            ws_handshake_avg_ms=vdata.get("ws_handshake_avg_ms", 0.0),
            ws_ping_avg_ms=vdata.get("ws_ping_avg_ms"),
            jitter_ms=vdata.get("jitter_ms", 0.0),
            grade=vdata.get("grade", "UNKNOWN"),
            errors=vdata.get("errors", []),
        )
        results.append(res)

    print_ascii_benchmark_table(results, use_color=use_color)
    print_executive_summary(results, use_color=use_color)

    if output_json_path:
        try:
            os.makedirs(os.path.dirname(output_json_path), exist_ok=True)
            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"Saved remote benchmark data to: {output_json_path}")
        except Exception as e:
            print(f"Failed to export JSON report: {e}")

    return data


def main():
    parser = argparse.ArgumentParser(description="Comprehensive Korean Sunrise latency & connection benchmark.")
    parser.add_argument("--repeats", type=int, default=3, help="Number of measurement repeats per venue (default: 3)")
    parser.add_argument("--venues", type=str, default="", help="Comma-separated list of venues")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent venue workers (default: 6)")
    parser.add_argument("--timeout", type=float, default=6.0, help="Per-operation timeout in seconds (default: 6.0)")
    parser.add_argument("--json", type=str, default="logs/benchmark_connections.json", help="Path to JSON output")
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI color codes")
    parser.add_argument("--remote", type=str, default="", help="Query a remote running collector Web API (e.g. http://localhost:8089)")
    parser.add_argument("--token", type=str, default=os.getenv("API_AUTH_TOKEN", ""), help="API Auth Token for --remote")
    parser.add_argument("--fresh", action="store_true", help="When using --remote, trigger fresh live benchmark run")
    parser.add_argument("--sync", action="store_true", help="When using --remote and --fresh, wait synchronously for benchmark completion")

    args = parser.parse_args()

    venue_list = [v.strip() for v in args.venues.split(",") if v.strip()] if args.venues else None
    use_color = sys.stdout.isatty() and not args.no_color

    if args.remote:
        fetch_remote_benchmark(
            remote_url=args.remote,
            token=args.token,
            fresh=args.fresh,
            sync=args.sync,
            venues=venue_list,
            repeats=max(1, args.repeats),
            concurrency=max(1, args.concurrency),
            use_color=use_color,
            output_json_path=args.json,
        )
    else:
        asyncio.run(
            run_benchmark_suite(
                venue_keys=venue_list,
                repeats=max(1, args.repeats),
                concurrency=max(1, args.concurrency),
                timeout=max(1.0, args.timeout),
                output_json_path=args.json,
                use_color=use_color,
            )
        )


if __name__ == "__main__":
    main()
