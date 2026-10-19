"""
tests/test_venues_streams_resilience.py - Deep Invariant and Resilience Tests for Venue Streams.

Verifies:
1. ReconnectPolicy: exponential backoff progression, jitter, 429 rate limit cooldown,
   403/DNS cooldown, healthy connection reset (>15s), and fast-disconnect persistence.
2. Starvation Detection: _starved_since_connect accurately catches silent zombie sockets.
3. Subscription Telemetry: _note_sub and _handle_sub_reply across OKX, Bybit, KuCoin, Gate.io, MEXC.
4. Timestamp Contracts: normalize_exchange_ts handles ns/us/ms/s, float, string, and invalid data.
5. End-to-End Parsing: All 11 venue streams (Binance, Bybit, Upbit, Hyperliquid, KuCoin, OKX, Gateio,
   MEXC, BingX, HTX, Pionex, Asterdex) emit clean, normalized OrderBookDepth5 with contract size adjustments.
6. StreamManager: Telemetry rollup with sub_accepted, sub_rejected, sub_errors across all streams.
"""
from __future__ import annotations

import json
import time
import unittest
from typing import Any, Dict, List

from config import MatchedUniverse
from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseVenueStream, BaseCEXStream, ReconnectPolicy
from venues.manager import StreamManager
from venues.binance import BinanceFuturesStream
from venues.bybit import BybitPerpStream
from venues.upbit import UpbitSpotStream
from venues.hyperliquid import HyperliquidStream
from venues.kucoin import KucoinStream
from venues.okx import OKXStream
from venues.gateio import GateioStream
from venues.mexc import MEXCStream
from venues.bingx import BingXStream
from venues.htx import HTXStream
from venues.pionex import PionexStream
from venues.asterdex import AsterdexStream


class TestReconnectPolicyResilience(unittest.TestCase):
    """Tests exponential backoff, jitter, rate limits, and healthy threshold resets."""

    def test_exponential_progression_deterministic(self):
        policy = ReconnectPolicy(base_delay=2.0, max_delay=120.0, factor=2.0, use_jitter=False)
        expected = [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 120.0, 120.0]
        actual = [policy.compute_delay() for _ in range(len(expected))]
        self.assertEqual(actual, expected)

    def test_jitter_variation(self):
        policy = ReconnectPolicy(base_delay=2.0, max_delay=120.0, factor=2.0, use_jitter=True)
        delays = [policy.compute_delay() for _ in range(5)]
        # Delay must strictly exceed pure exponential progression due to positive jitter
        pure_exp = [2.0, 4.0, 8.0, 16.0, 32.0]
        for act, exp in zip(delays, pure_exp):
            self.assertGreater(act, exp)
            self.assertLessEqual(act, exp + 1.0)

    def test_rate_limit_429_cooldown(self):
        policy = ReconnectPolicy(base_delay=2.0, max_delay=120.0, factor=2.0)
        delay_429 = policy.compute_delay(Exception("HTTP 429 Too Many Requests"))
        self.assertGreaterEqual(delay_429, 60.0)

        delay_rate = policy.compute_delay(Exception("rate limit exceeded"))
        self.assertGreaterEqual(delay_rate, 60.0)

    def test_forbidden_403_and_dns_cooldown(self):
        policy = ReconnectPolicy(base_delay=2.0, max_delay=120.0, factor=2.0)
        delay_403 = policy.compute_delay(Exception("HTTP 403 Forbidden Cloudflare"))
        self.assertGreaterEqual(delay_403, 30.0)

        delay_dns = policy.compute_delay(Exception("[Errno 11001] getaddrinfo failed"))
        self.assertGreaterEqual(delay_dns, 30.0)

    def test_healthy_connection_reset(self):
        policy = ReconnectPolicy(base_delay=2.0, max_delay=120.0, factor=2.0, healthy_threshold_sec=15.0, use_jitter=False)
        # Fail 1 -> 2.0
        self.assertEqual(policy.compute_delay(), 2.0)
        # Fail 2 -> 4.0
        self.assertEqual(policy.compute_delay(), 4.0)

        # Connect and stay connected for 20s (exceeds 15s healthy threshold)
        t0 = time.time() - 20.0
        policy._last_connected_ts = t0
        # Disconnect after healthy connection -> consecutive_failures resets to 0, next delay starts at base 2.0
        next_delay = policy.compute_delay()
        self.assertEqual(next_delay, 2.0)

    def test_fast_disconnect_preserves_backoff(self):
        policy = ReconnectPolicy(base_delay=2.0, max_delay=120.0, factor=2.0, healthy_threshold_sec=15.0, use_jitter=False)
        # Fail 1 -> 2.0
        self.assertEqual(policy.compute_delay(), 2.0)

        # Flapping connection: only stays up for 3 seconds (< 15s threshold)
        t0 = time.time() - 3.0
        policy._last_connected_ts = t0
        # Disconnect -> consecutive failures preserved and incremented, next delay is 4.0
        next_delay = policy.compute_delay()
        self.assertEqual(next_delay, 4.0)


class TestStarvationDetection(unittest.TestCase):
    """Tests starvation detection logic for silent zombie sockets."""

    def test_starvation_lifecycle(self):
        stream = BaseVenueStream("test", ["BTC"], callback=lambda s: None)
        now = time.time()

        # Not yet connected
        self.assertFalse(stream._starved_since_connect(now))

        # Freshly connected
        stream._note_connect()
        # 10s after connect with no data: not starved yet (grace is 30s)
        self.assertFalse(stream._starved_since_connect(stream._connect_ts + 10.0))

        # 35s after connect with NO data: starved!
        self.assertTrue(stream._starved_since_connect(stream._connect_ts + 35.0))

        # Data arrives at connect + 15s
        stream.last_data_ts = stream._connect_ts + 15.0
        # At connect + 35s: data was received after connect -> NOT starved
        self.assertFalse(stream._starved_since_connect(stream._connect_ts + 35.0))


class TestSubscriptionTelemetry(unittest.TestCase):
    """Tests subscription ACK tracking and payload parsing across venues."""

    def test_note_sub_accounting(self):
        stream = BaseVenueStream("test", ["BTC"], callback=lambda s: None)
        self.assertEqual(stream.sub_accepted, 0)
        self.assertEqual(stream.sub_rejected, 0)

        stream._note_sub(True)
        stream._note_sub(True)
        self.assertEqual(stream.sub_accepted, 2)

        stream._note_sub(False, "Symbol not supported")
        self.assertEqual(stream.sub_rejected, 1)
        self.assertEqual(stream.sub_errors, ["Symbol not supported"])

    def test_handle_sub_reply_venues(self):
        stream = BaseVenueStream("test", ["BTC"], callback=lambda s: None)

        # OKX
        stream._handle_sub_reply({"event": "subscribe", "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"}})
        self.assertEqual(stream.sub_accepted, 1)

        # Bybit
        stream._handle_sub_reply({"op": "subscribe", "success": True, "ret_msg": ""})
        self.assertEqual(stream.sub_accepted, 2)

        # KuCoin Ack
        stream._handle_sub_reply({"id": "kc_100", "type": "ack"})
        self.assertEqual(stream.sub_accepted, 3)

        # KuCoin Welcome
        stream._handle_sub_reply({"id": "1", "type": "welcome"})
        self.assertEqual(stream.sub_accepted, 4)

        # Toobit / XT
        stream._handle_sub_reply({"event": "sub"})
        self.assertEqual(stream.sub_accepted, 5)

        # Sub rejection / error
        stream._handle_sub_reply({"event": "error", "code": "51000", "msg": "Invalid instrument"})
        self.assertEqual(stream.sub_rejected, 1)

        # Bybit failure
        stream._handle_sub_reply({"op": "subscribe", "success": False, "ret_msg": "Too many topics"})
        self.assertEqual(stream.sub_rejected, 2)

        status = stream.get_status()
        self.assertEqual(status["sub_accepted"], 5)
        self.assertEqual(status["sub_rejected"], 2)
        self.assertEqual(len(status["sub_errors"]), 2)


class TestTimestampNormalizationContracts(unittest.TestCase):
    """Tests normalize_exchange_ts handles all timestamp unit scales."""

    def test_ns_us_ms_s_conversions(self):
        t_ms = 1710000000123
        t_ns = t_ms * 1_000_000
        t_us = t_ms * 1_000
        t_s = 1710000000

        self.assertEqual(normalize_exchange_ts(t_ns), t_ms)
        self.assertEqual(normalize_exchange_ts(t_us), t_ms)
        self.assertEqual(normalize_exchange_ts(t_ms), t_ms)
        self.assertEqual(normalize_exchange_ts(t_s), t_s * 1000)

    def test_string_and_float_conversions(self):
        self.assertEqual(normalize_exchange_ts("1710000000123"), 1710000000123)
        self.assertEqual(normalize_exchange_ts(1710000000.5), 1710000000500)
        self.assertEqual(normalize_exchange_ts("1710000000.5"), 1710000000500)

    def test_invalid_inputs(self):
        self.assertEqual(normalize_exchange_ts(None), 0)
        self.assertEqual(normalize_exchange_ts(""), 0)
        self.assertEqual(normalize_exchange_ts(0), 0)
        self.assertEqual(normalize_exchange_ts(-500), 0)
        self.assertEqual(normalize_exchange_ts("invalid_ts"), 0)


class TestAllVenueStreamsParsing(unittest.TestCase):
    """Tests parsing and emission across all 11 supported venue stream implementations."""

    def test_binance_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTCUSDT": ("BTC", 1.0, 1.0), "1000PEPEUSDT": ("PEPE", 1000.0, 1.0)}
        stream = BinanceFuturesStream(sym_map, callback=lambda snap: received.append(snap))

        payload = json.dumps({
            "data": {
                "s": "1000PEPEUSDT",
                "E": 1710000000123,
                "u": 100,
                "pu": 99,
                "b": [["0.01000", "50000"]],
                "a": [["0.01005", "40000"]],
            }
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        snap = received[0]
        self.assertEqual(snap.exchange, "binance")
        self.assertEqual(snap.symbol, "PEPE")
        self.assertEqual(snap.exchange_ts, 1710000000123)
        # Price scaled by 1/1000, size scaled by 1000
        self.assertAlmostEqual(snap.bids[0][0], 0.00001, places=7)
        self.assertAlmostEqual(snap.bids[0][1], 50000000.0, places=2)

    def test_hyperliquid_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTCUSDT": ("BTC", 1.0, 1.0), "kPEPEUSDT": ("PEPE", 1000.0, 1.0)}
        stream = HyperliquidStream(sym_map, callback=lambda snap: received.append(snap))

        # Sub confirm
        stream._parse_message(json.dumps({"channel": "subscriptionResponse", "data": {}}))
        self.assertEqual(stream.sub_accepted, 1)

        # L2 Book
        payload = json.dumps({
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "time": 1710000000456,
                "levels": [
                    [{"px": "65000.0", "sz": "1.25"}],
                    [{"px": "65010.0", "sz": "2.50"}],
                ],
            },
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        snap = received[0]
        self.assertEqual(snap.exchange, "hyperliquid")
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.exchange_ts, 1710000000456)
        self.assertEqual(snap.bids[0][0], 65000.0)

    def test_upbit_parsing_with_fx_conversion(self):
        received: List[OrderBookDepth5] = []
        trades: List[tuple] = []
        krw_map = {"KRW-BTC": "BTC"}
        stream = UpbitSpotStream(
            krw_map,
            callback=lambda snap: received.append(snap),
            trade_callback=lambda coin, usd, buy, ts: trades.append((coin, usd, buy, ts)),
        )

        # 1. Provide live KRW-USDT rate (1400 KRW = 1 USD)
        rate_msg = json.dumps({
            "code": "KRW-USDT",
            "orderbook_units": [{"bid_price": 1400.0, "ask_price": 1400.0, "bid_size": 100, "ask_size": 100}],
        })
        stream._handle_msg(rate_msg)
        self.assertTrue(stream.rate_ready)
        self.assertEqual(stream.usdt_krw_rate, 1400.0)

        # 2. Provide BTC KRW order book
        ob_msg = json.dumps({
            "code": "KRW-BTC",
            "timestamp": 1710000000789,
            "orderbook_units": [{"bid_price": 70000000.0, "ask_price": 70014000.0, "bid_size": 1.5, "ask_size": 2.0}],
        })
        stream._handle_msg(ob_msg)
        self.assertEqual(len(received), 1)
        snap = received[0]
        self.assertEqual(snap.exchange, "upbit")
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.exchange_ts, 1710000000789)
        # 70,000,000 / 1400 = 50,000 USD
        self.assertAlmostEqual(snap.bids[0][0], 50000.0, places=2)

    def test_bybit_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTCUSDT": ("BTC", 1.0, 1.0)}
        stream = BybitPerpStream(sym_map, callback=lambda snap: received.append(snap))

        # Sub confirm
        stream._parse_message(json.dumps({"op": "subscribe", "success": True}))
        self.assertEqual(stream.sub_accepted, 1)

        # Snapshot
        payload = json.dumps({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1710000000999,
            "data": {
                "s": "BTCUSDT",
                "u": 1,
                "seq": 1,
                "b": [["60000", "0.5"]],
                "a": [["60010", "0.8"]],
            },
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].exchange_ts, 1710000000999)

    def test_kucoin_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTC": ("BTC", 1.0, 0.001)}
        stream = KucoinStream(sym_map, callback=lambda snap: received.append(snap))
        stream._venue_to_ours["XBTUSDTM"] = "BTC"
        stream._contract_sizes["XBTUSDTM"] = 0.001

        # Sub ack
        stream._parse_message(json.dumps({"id": "sub_1", "type": "ack"}))
        self.assertEqual(stream.sub_accepted, 1)

        payload = json.dumps({
            "topic": "/contractMarket/level2Depth5:XBTUSDTM",
            "data": {
                "timestamp": 1710000000222,
                "bids": [["60000", "1500"]],
                "asks": [["60010", "2000"]],
            },
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        snap = received[0]
        self.assertEqual(snap.exchange_ts, 1710000000222)
        # Contract multiplier applied: 1500 * 0.001 = 1.5
        self.assertAlmostEqual(snap.bids[0][1], 1.5, places=3)

    def test_okx_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTC-USDT-SWAP": ("BTC", 1.0, 1.0)}
        stream = OKXStream(sym_map, callback=lambda snap: received.append(snap))

        payload = json.dumps({
            "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
            "data": [{"ts": "1710000000333", "bids": [["60000", "1.0"]], "asks": [["60010", "1.0"]]}],
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].exchange_ts, 1710000000333)

    def test_gateio_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTC_USDT": ("BTC", 1.0, 1.0)}
        stream = GateioStream(sym_map, callback=lambda snap: received.append(snap))

        payload = json.dumps({
            "time": 1710000000,
            "channel": "futures.order_book_update",
            "event": "all",
            "result": {
                "t": 1710000000444,
                "contract": "BTC_USDT",
                "b": [{"p": "60000", "s": "2.0"}],
                "a": [{"p": "60010", "s": "2.0"}],
            },
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].exchange_ts, 1710000000444)

    def test_mexc_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTC_USDT": ("BTC", 1.0, 1.0)}
        stream = MEXCStream(sym_map, callback=lambda snap: received.append(snap))

        payload = json.dumps({
            "channel": "push.depth",
            "symbol": "BTC_USDT",
            "data": {
                "cts": 1710000000555,
                "bids": [{"p": "60000", "v": "1.0"}],
                "asks": [{"p": "60010", "v": "1.0"}],
            },
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].exchange_ts, 1710000000555)

    def test_bingx_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTC-USDT": ("BTC", 1.0, 1.0)}
        stream = BingXStream(sym_map, callback=lambda snap: received.append(snap))

        payload = json.dumps({
            "dataType": "BTC-USDT@depth20",
            "data": {
                "ts": 1710000000666,
                "bids": [["60000", "1.0"]],
                "asks": [["60010", "1.0"]],
            },
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].exchange_ts, 1710000000666)

    def test_htx_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTC-USDT": ("BTC", 1.0, 1.0)}
        stream = HTXStream(sym_map, callback=lambda snap: received.append(snap))

        payload = json.dumps({
            "ch": "market.BTC-USDT.depth.step0",
            "ts": 1710000000777,
            "tick": {
                "bids": [[60000.0, 1.0]],
                "asks": [[60010.0, 1.0]],
            },
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].exchange_ts, 1710000000777)

    def test_pionex_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTC_USDT": ("BTC", 1.0, 1.0)}
        stream = PionexStream(sym_map, callback=lambda snap: received.append(snap))

        payload = json.dumps({
            "topic": "DEPTH",
            "symbol": "BTC_USDT",
            "timestamp": 1710000000888,
            "data": {
                "bids": [["60000", "1.0"]],
                "asks": [["60010", "1.0"]],
            },
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].exchange_ts, 1710000000888)

    def test_asterdex_parsing(self):
        received: List[OrderBookDepth5] = []
        sym_map = {"BTCUSDT": ("BTC", 1.0, 1.0)}
        stream = AsterdexStream(sym_map, callback=lambda snap: received.append(snap))

        # Sub confirm
        stream._parse_message(json.dumps({"id": 1, "result": None}))
        self.assertEqual(stream.sub_accepted, 1)

        payload = json.dumps({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "E": 1710000000999,
            "u": 10,
            "pu": 9,
            "b": [["60000", "1.0"]],
            "a": [["60010", "1.0"]],
        })
        stream._parse_message(payload)
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].exchange_ts, 1710000000999)


class TestStreamManagerStatus(unittest.TestCase):
    """Tests StreamManager initialization and telemetry summary."""

    def test_manager_status_contains_sub_telemetry(self):
        mock_universe = MatchedUniverse(
            coins=["BTC"],
            upbit_krw_to_coin={"KRW-BTC": "BTC"},
            binance_sym_map={"BTCUSDT": ("BTC", 1.0)},
            bybit_sym_map={"BTCUSDT": ("BTC", 1.0)},
        )
        mgr = StreamManager(mock_universe, callback=lambda s: None, active_venues=["binance", "bybit"])
        status = mgr.get_status()

        self.assertIn("upbit", status)
        self.assertIn("binance", status)
        self.assertIn("bybit", status)
        for v_name, v_status in status.items():
            self.assertIn("sub_accepted", v_status)
            self.assertIn("sub_rejected", v_status)
            self.assertIn("sub_errors", v_status)
            self.assertIn("connected", v_status)


if __name__ == "__main__":
    unittest.main()
