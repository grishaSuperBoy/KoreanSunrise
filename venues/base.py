"""
venues/base.py — Core base classes and utilities for venue REST and WebSocket connections.
Provides BaseVenueStream and BaseCEXStream with auto-reconnection, daily reconnect windows,
gzip decompression, level extraction helpers, and HTTP session management.
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
from requests.adapters import HTTPAdapter
import websockets

from models import OrderBookDepth5, normalize_exchange_ts

log = logging.getLogger("stream")

_DAILY_RECONNECT_HOUR = 2
_DAILY_RECONNECT_MINUTE = 23
_RECONNECT_SPREAD_SEC = 7 * 60

_SEPS = "-_/:"
_QUOTES = ("USDT", "USDC", "USD", "KRW")
_CONTRACT_SUFFIXES = ("PERP", "SWAP")


import re

_KNOWN_BASE_TOKENS = {
    # Genuine tokens starting with K, M, B that must not be stripped as metric multiplier prefixes
    "KAS", "KAVA", "KLAY", "KDA", "KSM", "KEY", "KNC", "KP3R", "KMD", "KUB",
    "MKR", "MANA", "MATIC", "MEME", "MINA", "MOVE", "ME", "MOVR", "MDT", "MAGIC", "MASK", "MBL", "MTL", "MBOX", "MYRO", "MOG", "MELANIA",
    "BTC", "BCH", "BNB", "BOME", "BONK", "BLUR", "BAT", "BICO", "BAND", "BADGER", "BEL", "BETA", "BAKE", "BEAM", "BSV", "BTT", "BIGTIME",
}

_SCALE_PREFIX_MAP: List[Tuple[re.Pattern, float]] = [
    (re.compile(r"^1000000000([A-Z0-9]+)$"), 1_000_000_000.0),
    (re.compile(r"^1B([A-Z0-9]+)$"), 1_000_000_000.0),
    (re.compile(r"^1000000([A-Z0-9]+)$"), 1_000_000.0),
    (re.compile(r"^1M([A-Z0-9]+)$"), 1_000_000.0),
    (re.compile(r"^100000([A-Z0-9]+)$"), 100_000.0),
    (re.compile(r"^10000([A-Z0-9]+)$"), 10_000.0),
    (re.compile(r"^1000([A-Z0-9]+)$"), 1_000.0),
    (re.compile(r"^1K([A-Z0-9]+)$"), 1_000.0),
    (re.compile(r"^100([A-Z0-9]+)$"), 100.0),
]


def parse_symbol_and_mult(raw: str) -> Tuple[str, float]:
    """
    Returns (canonical_base_coin, multiplier)
    e.g. '1000PEPE-USDT' -> ('PEPE', 1000.0)
         '1MPEPE_USDT'   -> ('PEPE', 1000000.0)
         'kPEPE'         -> ('PEPE', 1000.0)
         'kSHIB'         -> ('SHIB', 1000.0)
         'mPEPE'         -> ('PEPE', 1000000.0)
         'bLUNC'         -> ('LUNC', 1000000000.0)
         'KASUSDT'       -> ('KAS', 1.0)
    """
    s_raw = str(raw).strip()
    if not s_raw:
        return "", 1.0

    # Hyperliquid / DEX single-letter prefix convention ('k' for 1000, 'm' for 1M, 'b' for 1B)
    prefix_mult = 1.0
    if len(s_raw) > 2 and s_raw[0] in ("k", "m", "b") and s_raw[1].isupper():
        if s_raw.upper() not in _KNOWN_BASE_TOKENS:
            p_char = s_raw[0]
            if p_char == "k":
                prefix_mult = 1000.0
            elif p_char == "m":
                prefix_mult = 1_000_000.0
            elif p_char == "b":
                prefix_mult = 1_000_000_000.0
            s_raw = s_raw[1:]

    s = s_raw.upper()
    if ":" in s:
        s = s.split(":", 1)[0]
    for sep in _SEPS:
        s = s.replace(sep, "")
    for suf in _CONTRACT_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
    for q in _QUOTES:
        if s.endswith(q) and len(s) > len(q):
            s = s[:-len(q)]
            break
    for suf in _CONTRACT_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
    if s == "XBT":
        s = "BTC"

    if prefix_mult != 1.0:
        return s, prefix_mult

    # Check numeric and metric prefix multipliers (1000, 1M, 1B, 1000000, etc.)
    for pattern, mult in _SCALE_PREFIX_MAP:
        m = pattern.match(s)
        if m:
            candidate_coin = m.group(1)
            if candidate_coin:
                return candidate_coin, mult

    return s, 1.0


def normalize_symbol(raw: str) -> str:
    """Canonical base coin symbol normalizer (strips 1000x prefix and quote)."""
    coin, _ = parse_symbol_and_mult(raw)
    return coin


def create_http_session(
    pool_connections: int = 35,
    pool_maxsize: int = 35,
    max_retries: int = 2,
    timeout: float = 8.0,
) -> requests.Session:
    """Creates a pre-configured requests.Session with connection pooling and headers."""
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=max_retries,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    return session


def _secs_until_daily_reconnect() -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(
        hour=_DAILY_RECONNECT_HOUR, minute=_DAILY_RECONNECT_MINUTE,
        second=0, microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _reconnect_jitter_sec(name: str) -> float:
    h = int(hashlib.sha256(name.encode("utf-8")).hexdigest()[:8], 16)
    span = 2 * _RECONNECT_SPREAD_SEC
    return float(h % span) - _RECONNECT_SPREAD_SEC


def _secs_until_daily_reconnect_for(name: str) -> float:
    now = datetime.now(timezone.utc)
    base_target = now.replace(
        hour=_DAILY_RECONNECT_HOUR, minute=_DAILY_RECONNECT_MINUTE,
        second=0, microsecond=0,
    )
    jitter = _reconnect_jitter_sec(name)
    target = base_target + timedelta(seconds=jitter)
    if target <= now:
        target += timedelta(days=1)
    return max(60.0, (target - now).total_seconds())


class ReconnectPolicy:
    """
    Unified exponential backoff and rate-limit mitigation policy for all stream connections.
    Eliminates fragile hardcoded sleeps and protects against exchange IP bans.
    """
    def __init__(
        self,
        base_delay: float = 2.0,
        max_delay: float = 120.0,
        factor: float = 2.0,
        healthy_threshold_sec: float = 15.0,
        use_jitter: bool = False,
        **kwargs: Any,
    ):
        if "backoff_factor" in kwargs:
            factor = float(kwargs["backoff_factor"])
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.factor = factor
        self.backoff_factor = factor  # alias for backward compatibility
        self.healthy_threshold_sec = healthy_threshold_sec
        self.use_jitter = use_jitter
        self.consecutive_failures = 0
        self._last_connected_ts = 0.0

    def record_connected(self) -> None:
        self._last_connected_ts = time.time()

    def record_disconnected(self) -> None:
        now = time.time()
        # If the stream was connected and healthy for >= healthy_threshold_sec,
        # reset consecutive failures so normal periodic reconnects start from base_delay.
        if self._last_connected_ts > 0.0 and (now - self._last_connected_ts >= self.healthy_threshold_sec):
            self.consecutive_failures = 0

    def compute_delay(self, error: Any = None, use_jitter: Optional[bool] = None) -> float:
        self.record_disconnected()
        self.consecutive_failures += 1

        # Exponential backoff: base_delay * (factor ** min(attempts, 6))
        exp_delay = min(self.max_delay, self.base_delay * (self.factor ** min(self.consecutive_failures - 1, 6)))
        should_jitter = self.use_jitter if use_jitter is None else use_jitter
        if should_jitter:
            # Predictable jitter based on attempt count (0.2s - 0.9s)
            jitter = 0.2 + ((self.consecutive_failures * 17) % 70) / 100.0
            delay = exp_delay + jitter
        else:
            delay = exp_delay

        err_str = str(error or "").lower()
        # Rate limit (HTTP 429 / 418 / "rate" / "too many requests") -> minimum 60.0s cool-down
        if "429" in err_str or "418" in err_str or "rate" in err_str or "too many" in err_str:
            delay = max(delay, 60.0)
        # Cloudflare / DNS resolution / Block (HTTP 403 / 11001 / getaddrinfo) -> minimum 30.0s cool-down
        elif "403" in err_str or "11001" in err_str or "getaddrinfo" in err_str:
            delay = max(delay, 30.0)

        return min(self.max_delay, delay)

    def reset(self) -> None:
        self.consecutive_failures = 0
        self._last_connected_ts = 0.0


class TTLCache:
    """
    Lightweight in-memory TTL dictionary cache for deduplicating high-frequency
    events and tracking subscription confirmations without unbounded memory growth.
    """
    def __init__(self, ttl_sec: float = 60.0):
        self.ttl_sec = ttl_sec
        self._store: Dict[Any, float] = {}

    def get(self, key: Any) -> bool:
        now = time.time()
        exp = self._store.get(key)
        if exp is None:
            return False
        if now > exp:
            del self._store[key]
            return False
        return True

    def set(self, key: Any) -> None:
        now = time.time()
        self._store[key] = now + self.ttl_sec
        if len(self._store) > 10000:
            self.cleanup()

    def cleanup(self) -> None:
        now = time.time()
        self._store = {k: exp for k, exp in self._store.items() if exp > now}


class BaseVenueStream:
    """Base class for venue-specific WebSocket feeds."""

    def __init__(self, name: str, symbols: List[str], callback: Callable[[OrderBookDepth5], None]):
        self.name = name.lower()
        self.symbols = [s.upper() for s in symbols]
        self._raw_callback = callback
        self.callback = self._sanitise
        self.connected = False
        self.last_msg_ts = 0.0
        self.last_data_ts = 0.0
        self._connect_ts = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.subscribed: List[str] = []
        self.sub_accepted: int = 0
        self.sub_rejected: int = 0
        self.sub_errors: List[str] = []
        self.symbols_seen: Set[str] = set()
        self.sym_first_ts: Dict[str, float] = {}
        self.books_dropped = 0
        self.levels_dropped = 0
        self.seq_gaps = 0
        self.seq_duplicates = 0
        self._last_u: Dict[str, int] = {}
        self._last_seq: Dict[str, int] = {}
        self._first_ts = 0.0
        self._reconnect_policy = ReconnectPolicy(base_delay=2.0, max_delay=120.0)

    def _note_connect(self) -> None:
        self.connected = True
        self.last_msg_ts = time.time()
        self._connect_ts = self.last_msg_ts
        if not self._first_ts:
            self._first_ts = self.last_msg_ts
        self._reconnect_policy.record_connected()

    def _note_symbol(self, sym: str) -> None:
        if not sym:
            return
        s = sym.upper()
        self.last_data_ts = time.time()
        if s not in self.symbols_seen:
            self.symbols_seen.add(s)
            if not self._first_ts:
                self._first_ts = time.time()
            self.sym_first_ts[s] = self._first_ts

    def _note_sub(self, ok: bool, msg: str = "") -> None:
        if ok:
            self.sub_accepted += 1
        else:
            self.sub_rejected += 1
            if msg:
                self.sub_errors.append(msg[:200])
                if len(self.sub_errors) > 20:
                    del self.sub_errors[:-20]

    def _starved_since_connect(self, now: float, grace_sec: float = 30.0) -> bool:
        cts = getattr(self, "_connect_ts", 0.0) or 0.0
        if cts <= 0:
            return False
        last_data = getattr(self, "last_data_ts", 0.0) or 0.0
        return (now - cts > grace_sec) and (last_data <= cts)

    @staticmethod
    def _sub_reply_text(data: Dict[str, Any]) -> str:
        for k in ("ret_msg", "msg", "message", "err_msg", "detail", "error"):
            v = data.get(k)
            if isinstance(v, str) and v:
                return v
            if isinstance(v, dict):
                return json.dumps(v, ensure_ascii=False)[:200]
        return ""

    def _handle_sub_reply(self, data: Dict[str, Any]) -> None:
        try:
            if not isinstance(data, dict):
                return
            ev = str(data.get("event") or "").lower()
            action = str(data.get("action") or "").lower()
            msg_type = str(data.get("type") or "").lower()
            req_id = str(data.get("id") or "")

            if data.get("op") in ("ping", "pong") or str(data.get("ret_msg", "")).strip().lower() in ("pong", "ping"):
                return

            if data.get("error") not in (None, {}, "", []):
                self._note_sub(False, self._sub_reply_text(data) or json.dumps(data.get("error"))[:200])
                return

            if data.get("success") is not None:
                if data["success"]:
                    self._note_sub(True)
                else:
                    self._note_sub(False, self._sub_reply_text(data) or "refused")
                return

            if data.get("op") == "error":
                self._note_sub(False, self._sub_reply_text(data) or "error")
                return

            # Covers Toobit ("event": "sub"), OKX ("event": "subscribe"), LBank ("action": "subscribe")
            if ev in ("sub", "subscribe", "unsubscribe") or action in ("sub", "subscribe"):
                self._note_sub(True)
                return
            if ev == "error":
                self._note_sub(False, self._sub_reply_text(data) or str(data.get("code", "")))
                return

            # KuCoin subscription acknowledgements
            if msg_type in ("ack", "welcome"):
                self._note_sub(True)
                return

            # XT / KuCoin batch response IDs
            if req_id and (req_id.startswith("xt_") or req_id.startswith("kc_") or req_id.startswith("sub_")):
                if "error" not in data and data.get("code") in (None, 0, "0", "00000", 200, "200"):
                    self._note_sub(True)
                    return

            if "code" in data and not any(k in data for k in ("data", "dataType", "topic", "channel", "table")):
                if str(data["code"]).strip() in ("0", "00000", "200000", "200"):
                    self._note_sub(True)
                else:
                    self._note_sub(False, self._sub_reply_text(data) or str(data["code"]))
                return

            ch = str(data.get("channel", ""))
            if ch.startswith("rs."):
                reason = str(data.get("data", ""))
                if ch == "rs.error" or reason not in ("success", ""):
                    self._note_sub(False, reason or ch)
                else:
                    self._note_sub(True)
                return

            if "result" in data and data.get("result") is None:
                self._note_sub(True)
                return
        except Exception:
            pass

    def _sanitise(self, snap: OrderBookDepth5) -> None:
        """Sanitise and filter crossed books before forwarding to engine."""
        try:
            bids = [
                (float(p), float(s))
                for p, s in snap.bids
                if float(p) > 0.0 and float(s) > 0.0
            ]
            asks = [
                (float(p), float(s))
                for p, s in snap.asks
                if float(p) > 0.0 and float(s) > 0.0
            ]
            if not bids or not asks:
                self.books_dropped += 1
                return

            # Defensively sort order book levels: bids descending, asks ascending
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])

            # Reject inverted (crossed) books
            if bids[0][0] >= asks[0][0]:
                self.books_dropped += 1
                return

            now = time.time()
            self._note_symbol(snap.symbol)

            clean_snap = OrderBookDepth5(
                exchange=snap.exchange,
                symbol=snap.symbol,
                raw_symbol=snap.raw_symbol,
                ts=snap.ts or now,
                exchange_ts=normalize_exchange_ts(snap.exchange_ts) or int(now * 1000),
                bids=bids[:5],
                asks=asks[:5],
            )
            self._raw_callback(clean_snap)

        except (ValueError, TypeError, IndexError) as e:
            self.levels_dropped += 1
            log.debug(f"[{self.name}] Sanitise drop: {e}")

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.connected = False

    async def _run_loop(self) -> None:
        raise NotImplementedError

    def get_status(self) -> dict:
        return {
            "name": self.name,
            "connected": self.connected,
            "symbols_seen": len(self.symbols_seen),
            "subscribed": len(self.subscribed),
            "sub_accepted": self.sub_accepted,
            "sub_rejected": self.sub_rejected,
            "sub_errors": list(self.sub_errors),
            "last_data_ago_sec": (
                round(time.time() - self.last_data_ts, 1)
                if self.last_data_ts else None
            ),
            "books_dropped": self.books_dropped,
            "levels_dropped": self.levels_dropped,
            "seq_gaps": self.seq_gaps,
            "seq_duplicates": self.seq_duplicates,
        }


class BaseCEXStream(BaseVenueStream):
    """
    Generic base class for Centralized Exchange WebSocket feeds.
    Provides auto-reconnection, heartbeat ping loops, binary gzip decompression,
    starvation detection, and level extraction helpers.
    """

    WS_URL: str = ""
    PING_INTERVAL_SEC: float = 15.0

    def __init__(
        self,
        exchange: str,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        self._contract_sizes: Dict[str, float] = {
            k.upper(): float(v) for k, v in (contract_sizes or {}).items()
        }
        if isinstance(symbols, dict):
            self._sym_map: Dict[str, Tuple[str, float]] = {}
            for k, v in symbols.items():
                k_u = str(k).upper()
                if isinstance(v, (list, tuple)):
                    coin = str(v[0])
                    mult = float(v[1]) if len(v) > 1 else 1.0
                    self._sym_map[k_u] = (coin, mult)
                    if len(v) >= 3 and k_u not in self._contract_sizes:
                        self._contract_sizes[k_u] = float(v[2])
                else:
                    self._sym_map[k_u] = parse_symbol_and_mult(k_u)
            raw_list = list(symbols.keys())
            coins = sorted({c for c, _ in self._sym_map.values()})
        else:
            raw_list = [str(s).upper() for s in symbols]
            self._sym_map = {s: parse_symbol_and_mult(s) for s in raw_list}
            coins = sorted({c for c, _ in self._sym_map.values()})

        super().__init__(exchange, raw_list, callback)
        self.venue = exchange.lower()
        if stream_name:
            self.name = stream_name.lower()
        self.ws_url = self.WS_URL
        self.subscribed = coins  # Canonical coins for accurate heartbeat X/Y counter

    def _resolve_coin_and_mult(self, raw_sym: str) -> Tuple[str, float]:
        raw_u = raw_sym.upper()
        if raw_u in self._sym_map:
            return self._sym_map[raw_u]
        return parse_symbol_and_mult(raw_u)

    def _get_contract_size(self, raw_sym: str) -> float:
        raw_u = raw_sym.upper()
        return self._contract_sizes.get(raw_u, 1.0)

    @staticmethod
    def _base_coin(s: str) -> str:
        return normalize_symbol(s)

    def _extract_levels(
        self,
        raw_side: Any,
        limit: int = 5,
        mult: float = 1.0,
        contract_size: float = 1.0,
    ) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        for row in raw_side or ():
            if len(out) >= limit:
                break
            try:
                if isinstance(row, (list, tuple)) and len(row) >= 2:
                    p, q = float(row[0]), float(row[1])
                elif isinstance(row, dict):
                    p = float(row.get("p") or row.get("price") or row.get("px") or 0.0)
                    q = float(
                        row.get("q")
                        or row.get("s")
                        or row.get("m")
                        or row.get("size")
                        or row.get("qty")
                        or row.get("amount")
                        or row.get("v")
                        or row.get("vol")
                        or 0.0
                    )
                else:
                    continue
                if p > 0.0 and q > 0.0:
                    real_p = p * inv_mult
                    real_q = q * contract_size * mult
                    out.append((real_p, real_q))
            except (TypeError, ValueError, IndexError):
                continue
        return out

    async def _send_subscriptions(self, ws: Any) -> None:
        raise NotImplementedError

    async def _send_client_ping(self, ws: Any) -> None:
        pass

    async def _handle_server_ping(self, ws: Any, raw_msg: str) -> bool:
        return False

    def _parse_message(self, raw_msg: str) -> None:
        raise NotImplementedError

    async def _ping_loop(self, ws: Any) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.PING_INTERVAL_SEC)
                await self._send_client_ping(ws)
        except Exception:
            pass

    async def _run_loop(self) -> None:
        if not self.ws_url:
            log.warning(f"[{self.name}] No WS endpoint configured.")
            return

        while self._running:
            try:
                log.info(f"[{self.name}] Connecting to {self.ws_url} ({len(self.symbols)} symbols)...")
                async with websockets.connect(
                    self.ws_url,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=2000,
                ) as ws:
                    self._note_connect()
                    log.info(f"[{self.name}] Connected! Sending subscriptions...")
                    await self._send_subscriptions(ws)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info(f"[{self.name}] Scheduled daily reconnect. Reconnecting...")
                                break

                            if self._starved_since_connect(time.time()):
                                log.warning(f"[{self.name}] Connected but no data in 30s, reconnecting...")
                                break

                            if not self.last_msg_ts:
                                self.last_msg_ts = time.time()
                            elif time.time() - self.last_msg_ts > 180.0:
                                log.warning(f"[{self.name}] No data for >180s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(30.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            # Automatic GZIP decompression for compressed streams (HTX, BingX)
                            if isinstance(msg, bytes):
                                try:
                                    msg = gzip.decompress(msg).decode("utf-8")
                                except Exception:
                                    msg = msg.decode("utf-8", errors="ignore")

                            if isinstance(msg, str):
                                s_clean = msg.strip().strip('"').lower()
                                if s_clean == "ping":
                                    await ws.send("pong")
                                    self.last_msg_ts = time.time()
                                    continue
                                if s_clean == "pong":
                                    self.last_msg_ts = time.time()
                                    continue
                                if "ping" in s_clean or "pong" in s_clean or '"time"' in s_clean:
                                    if await self._handle_server_ping(ws, msg):
                                        self.last_msg_ts = time.time()
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
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[{self.name}] WS disconnected: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)
