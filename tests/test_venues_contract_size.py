"""
tests/test_venues_contract_size.py — Unit tests verifying contract size scaling
(quanto_multiplier, ctVal, multiplier, contractSize) across perpetual follower venues:
Gate.io, OKX, KuCoin, MEXC, HTX.
"""
from __future__ import annotations

import json
import time
import unittest
from typing import List

from models import OrderBookDepth5
from venues.base import BaseCEXStream
from venues.bingx import BingXStream
from venues.gateio import GateioStream
from venues.htx import HTXStream
from venues.kucoin import KucoinStream
from venues.mexc import MEXCStream
from venues.okx import OKXStream
from venues.pionex import PionexStream
from venues.asterdex import AsterdexStream, AsterdexRestClient
from venues.hyperliquid import HyperliquidStream, HyperliquidRestClient


class TestVenuesContractSize(unittest.TestCase):
    """Verifies that contract lot sizes correctly scale orderbook quantities to base coin units."""

    def test_base_cex_extract_levels(self):
        """Test _extract_levels with combinations of contract_size and mult."""
        received: List[OrderBookDepth5] = []
        stream = BaseCEXStream("test", ["BTC-USDT"], received.append)

        # 1. Default (no contract_size scaling, mult=1.0)
        raw = [["80000.0", "10.0"]]
        levels = stream._extract_levels(raw, limit=5, mult=1.0, contract_size=1.0)
        self.assertEqual(levels, [(80000.0, 10.0)])

        # 2. Contract size scaling only (e.g. OKX BTC: 1 contract = 0.01 BTC)
        levels = stream._extract_levels(raw, limit=5, mult=1.0, contract_size=0.01)
        self.assertEqual(levels, [(80000.0, 0.1)])

        # 3. 1000x mult prefix only (e.g. Binance 1000PEPE)
        pepe_raw = [["0.015", "100.0"]]  # price for 1000, qty in thousands
        levels = stream._extract_levels(pepe_raw, limit=5, mult=1000.0, contract_size=1.0)
        self.assertAlmostEqual(levels[0][0], 0.000015)
        self.assertAlmostEqual(levels[0][1], 100000.0)

        # 4. Both contract_size and mult (e.g. OKX 1000PEPE: ctVal=10 units of 1000PEPE)
        levels = stream._extract_levels(pepe_raw, limit=5, mult=1000.0, contract_size=10.0)
        self.assertAlmostEqual(levels[0][0], 0.000015)
        self.assertAlmostEqual(levels[0][1], 1000000.0)

    def test_gateio_quanto_multiplier(self):
        """Gate.io: quanto_multiplier scales 's' from contracts to coins."""
        snaps: List[OrderBookDepth5] = []
        contract_sizes = {
            "BTC_USDT": 0.0001,
            "PEPE_USDT": 10_000_000.0,
        }
        stream = GateioStream(
            ["BTC_USDT", "PEPE_USDT"],
            snaps.append,
            contract_sizes=contract_sizes,
        )

        # 1. BTC_USDT (quanto = 0.0001)
        msg_btc = json.dumps({
            "channel": "futures.order_book_update",
            "event": "all",
            "result": {
                "contract": "BTC_USDT",
                "t": 1700000000000,
                "b": [{"p": "80000.0", "s": 500}],   # 500 contracts = 0.05 BTC
                "a": [{"p": "80001.0", "s": 1000}],  # 1000 contracts = 0.1 BTC
            }
        })
        stream._parse_message(msg_btc)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.bids[0], (80000.0, 0.05))
        self.assertEqual(snap.asks[0], (80001.0, 0.1))
        # USD Depth: 80001.0 * 0.1 = $8000.10 (NOT $80,001,000!)
        self.assertAlmostEqual(snap.depth_ask_usd, 8000.10, places=2)

        # 2. PEPE_USDT (quanto = 10,000,000)
        snaps.clear()
        msg_pepe = json.dumps({
            "channel": "futures.order_book_update",
            "event": "all",
            "result": {
                "contract": "PEPE_USDT",
                "t": 1700000000000,
                "b": [{"p": "0.000004", "s": 5}],    # 5 contracts = 50,000,000 PEPE
                "a": [{"p": "0.0000041", "s": 10}],  # 10 contracts = 100,000,000 PEPE
            }
        })
        stream._parse_message(msg_pepe)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "PEPE")
        self.assertEqual(snap.bids[0], (0.000004, 50_000_000.0))
        self.assertEqual(snap.asks[0], (0.0000041, 100_000_000.0))
        # USD Depth: 0.0000041 * 100,000,000 = $410.0 (NOT $0.000041!)
        self.assertAlmostEqual(snap.depth_ask_usd, 410.0, places=2)

    def test_okx_ct_val(self):
        """OKX: ctVal scales 'sz' from contracts to coins."""
        snaps: List[OrderBookDepth5] = []
        contract_sizes = {
            "BTC-USDT-SWAP": 0.01,
            "1000PEPE-USDT-SWAP": 10.0,
        }
        sym_map = {
            "BTC-USDT-SWAP": ("BTC", 1.0, 0.01),
            "1000PEPE-USDT-SWAP": ("PEPE", 1000.0, 10.0),
        }
        stream = OKXStream(sym_map, snaps.append, contract_sizes=contract_sizes)

        # 1. BTC-USDT-SWAP (ctVal = 0.01)
        msg_btc = json.dumps({
            "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "ts": "1700000000000",
                "bids": [["80000.0", "20", "0", "1"]],  # 20 contracts = 0.2 BTC
                "asks": [["80002.0", "50", "0", "1"]],  # 50 contracts = 0.5 BTC
            }]
        })
        stream._parse_message(msg_btc)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.bids[0], (80000.0, 0.2))
        self.assertEqual(snap.asks[0], (80002.0, 0.5))
        self.assertAlmostEqual(snap.depth_ask_usd, 80002.0 * 0.5, places=2)

        # 2. 1000PEPE-USDT-SWAP (mult = 1000, ctVal = 10)
        snaps.clear()
        msg_pepe = json.dumps({
            "arg": {"channel": "books5", "instId": "1000PEPE-USDT-SWAP"},
            "data": [{
                "ts": "1700000000000",
                "bids": [["0.004", "15", "0", "1"]],   # px=0.000004, 15*10*1000 = 150,000 PEPE
                "asks": [["0.0042", "30", "0", "1"]],  # px=0.0000042, 30*10*1000 = 300,000 PEPE
            }]
        })
        stream._parse_message(msg_pepe)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "PEPE")
        self.assertAlmostEqual(snap.bids[0][0], 0.000004)
        self.assertAlmostEqual(snap.bids[0][1], 150_000.0)
        self.assertAlmostEqual(snap.asks[0][0], 0.0000042)
        self.assertAlmostEqual(snap.asks[0][1], 300_000.0)
        self.assertAlmostEqual(snap.depth_ask_usd, 0.0000042 * 300_000.0, places=2)

    def test_kucoin_multiplier(self):
        """KuCoin: multiplier scales 'size' from contracts to coins."""
        snaps: List[OrderBookDepth5] = []
        contract_sizes = {
            "XBTUSDTM": 0.001,
            "PEPEUSDTM": 520_000.0,
        }
        stream = KucoinStream(["XBTUSDTM", "PEPEUSDTM"], snaps.append, contract_sizes=contract_sizes)

        # 1. XBTUSDTM (multiplier = 0.001)
        msg_btc = json.dumps({
            "topic": "/contractMarket/level2Depth5:XBTUSDTM",
            "data": {
                "timestamp": 1700000000000,
                "bids": [["80000.0", "50"]],  # 50 contracts = 0.05 BTC
                "asks": [["80001.0", "80"]],  # 80 contracts = 0.08 BTC
            }
        })
        stream._parse_message(msg_btc)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.bids[0], (80000.0, 0.05))
        self.assertEqual(snap.asks[0], (80001.0, 0.08))
        self.assertAlmostEqual(snap.depth_ask_usd, 80001.0 * 0.08, places=2)

        # 2. PEPEUSDTM (multiplier = 520,000)
        snaps.clear()
        msg_pepe = json.dumps({
            "topic": "/contractMarket/level2Depth5:PEPEUSDTM",
            "data": {
                "timestamp": 1700000000000,
                "bids": [["0.0000038", "10"]],  # 10 contracts = 5,200,000 PEPE
                "asks": [["0.0000039", "25"]],  # 25 contracts = 13,000,000 PEPE
            }
        })
        stream._parse_message(msg_pepe)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "PEPE")
        self.assertEqual(snap.bids[0], (0.0000038, 5_200_000.0))
        self.assertEqual(snap.asks[0], (0.0000039, 13_000_000.0))
        self.assertAlmostEqual(snap.depth_ask_usd, 0.0000039 * 13_000_000.0, places=2)

    def test_mexc_contract_size(self):
        """MEXC: contractSize scales 'vol' from contracts to coins."""
        snaps: List[OrderBookDepth5] = []
        contract_sizes = {
            "BTC_USDT": 0.0001,
            "PEPE_USDT": 10_000_000.0,
        }
        stream = MEXCStream(["BTC_USDT", "PEPE_USDT"], snaps.append, contract_sizes=contract_sizes)

        # 1. BTC_USDT (contractSize = 0.0001)
        msg_btc = json.dumps({
            "channel": "push.depth",
            "symbol": "BTC_USDT",
            "data": {
                "cts": 1700000000000,
                "bids": [["80000.0", "300", "1"]],  # 300 contracts = 0.03 BTC
                "asks": [["80002.0", "600", "1"]],  # 600 contracts = 0.06 BTC
            }
        })
        stream._parse_message(msg_btc)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "BTC")
        self.assertAlmostEqual(snap.bids[0][0], 80000.0)
        self.assertAlmostEqual(snap.bids[0][1], 0.03)
        self.assertAlmostEqual(snap.asks[0][0], 80002.0)
        self.assertAlmostEqual(snap.asks[0][1], 0.06)
        self.assertAlmostEqual(snap.depth_ask_usd, 80002.0 * 0.06, places=2)

        # 2. PEPE_USDT (contractSize = 10,000,000)
        snaps.clear()
        msg_pepe = json.dumps({
            "channel": "push.depth",
            "symbol": "PEPE_USDT",
            "data": {
                "cts": 1700000000000,
                "bids": [["0.00000385", "4", "1"]],  # 4 contracts = 40,000,000 PEPE
                "asks": [["0.00000388", "8", "1"]],  # 8 contracts = 80,000,000 PEPE
            }
        })
        stream._parse_message(msg_pepe)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "PEPE")
        self.assertEqual(snap.bids[0], (0.00000385, 40_000_000.0))
        self.assertEqual(snap.asks[0], (0.00000388, 80_000_000.0))
        self.assertAlmostEqual(snap.depth_ask_usd, 0.00000388 * 80_000_000.0, places=2)

    def test_htx_contract_size(self):
        """HTX: contract_size scales 'amount' from contracts to coins."""
        snaps: List[OrderBookDepth5] = []
        contract_sizes = {
            "BTC-USDT": 0.001,
            "DOGE-USDT": 100.0,
        }
        stream = HTXStream(["BTC-USDT", "DOGE-USDT"], snaps.append, contract_sizes=contract_sizes)

        # 1. BTC-USDT (contract_size = 0.001)
        msg_btc = json.dumps({
            "ch": "market.BTC-USDT.depth.step0",
            "ts": 1700000000000,
            "tick": {
                "bids": [[80000.0, 70]],  # 70 contracts = 0.07 BTC
                "asks": [[80001.0, 90]],  # 90 contracts = 0.09 BTC
            }
        })
        stream._parse_message(msg_btc)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.bids[0], (80000.0, 0.07))
        self.assertEqual(snap.asks[0], (80001.0, 0.09))
        self.assertAlmostEqual(snap.depth_ask_usd, 80001.0 * 0.09, places=2)

        # 2. DOGE-USDT (contract_size = 100.0)
        snaps.clear()
        msg_doge = json.dumps({
            "ch": "market.DOGE-USDT.depth.step0",
            "ts": 1700000000000,
            "tick": {
                "bids": [[0.078, 50]],   # 50 contracts = 5000 DOGE
                "asks": [[0.0782, 100]],  # 100 contracts = 10000 DOGE
            }
        })
        stream._parse_message(msg_doge)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "DOGE")
        self.assertEqual(snap.bids[0], (0.078, 5000.0))
        self.assertEqual(snap.asks[0], (0.0782, 10000.0))
        self.assertAlmostEqual(snap.depth_ask_usd, 0.0782 * 10000.0, places=2)

    def test_engine_integration_depth_accuracy(self):
        """Demonstrates that KoreanEngine receives exact USD depth instead of 10x-10000x distortion."""
        from korean_engine import KoreanEngine
        from pipeline.pool_state import PoolStateManager

        signals: List[dict] = []
        engine = KoreanEngine(
            emit_callback=signals.append,
            min_edge_bps=10.0,
            taker_fee_bps=5.0,
            pool_state=PoolStateManager(),
        )

        # Upbit USDT/KRW rate snapshot (e.g. 1350)
        upbit_rate_book = OrderBookDepth5(
            exchange="upbit",
            symbol="USDT",
            raw_symbol="KRW-USDT",
            ts=time.time(),
            exchange_ts=int(time.time() * 1000),
            bids=[(1350.0, 10000.0)],
            asks=[(1351.0, 10000.0)],
        )
        engine.on_book_update(upbit_rate_book)

        # 1. Gate.io DOGE with quanto_multiplier = 10.0
        gate_snaps: List[OrderBookDepth5] = []
        gate_stream = GateioStream(
            ["DOGE_USDT"],
            gate_snaps.append,
            contract_sizes={"DOGE_USDT": 10.0},
        )
        gate_msg_doge = json.dumps({
            "channel": "futures.order_book_update",
            "event": "all",
            "result": {
                "contract": "DOGE_USDT",
                "t": 1700000000000,
                "b": [{"p": "0.0800", "s": 500}],   # 500 contracts * 10 = 5000 DOGE ($400)
                "a": [{"p": "0.0801", "s": 1000}],  # 1000 contracts * 10 = 10000 DOGE ($801)
            }
        })
        gate_stream._parse_message(gate_msg_doge)
        self.assertEqual(len(gate_snaps), 1)
        doge_snap = gate_snaps[0]

        # Engine receives the book
        engine.on_book_update(doge_snap)

        saved_doge_book = engine._books.get(("gateio", "DOGE"))
        self.assertIsNotNone(saved_doge_book)
        # Top 1 ask depth USD must be exactly 0.0801 * 10000 DOGE = $801.0
        self.assertAlmostEqual(saved_doge_book.top_n_ask_depth_usd(1), 801.0, places=2)
        # Without quanto_multiplier fix, depth would be 0.0801 * 1000 = $80.1 (10x too small!)
        self.assertNotEqual(saved_doge_book.top_n_ask_depth_usd(1), 80.1)

        # 2. Gate.io BTC with quanto_multiplier = 0.0001
        gate_snaps.clear()
        gate_stream_btc = GateioStream(
            ["BTC_USDT"],
            gate_snaps.append,
            contract_sizes={"BTC_USDT": 0.0001},
        )
        gate_msg_btc = json.dumps({
            "channel": "futures.order_book_update",
            "event": "all",
            "result": {
                "contract": "BTC_USDT",
                "t": 1700000000000,
                "b": [{"p": "80000.0", "s": 50}],  # 50 contracts * 0.0001 = 0.005 BTC ($400)
                "a": [{"p": "80002.0", "s": 100}], # 100 contracts * 0.0001 = 0.01 BTC ($800.02)
            }
        })
        gate_stream_btc._parse_message(gate_msg_btc)
        self.assertEqual(len(gate_snaps), 1)
        btc_snap = gate_snaps[0]

        engine.on_book_update(btc_snap)
        saved_btc_book = engine._books.get(("gateio", "BTC"))
        self.assertIsNotNone(saved_btc_book)
        # Top 1 ask depth USD must be exactly 80002.0 * 0.01 BTC = $800.02
        self.assertAlmostEqual(saved_btc_book.top_n_ask_depth_usd(1), 800.02, places=2)
        # Without quanto_multiplier fix, depth would be 80002.0 * 100 = $8,000,200 (10,000x too large!)
        self.assertNotEqual(saved_btc_book.top_n_ask_depth_usd(1), 8_000_200.0)

    def test_bingx_1000x_contracts(self):
        """BingX: 1000x contracts subscribe with raw symbol and scale price/qty by mult."""
        snaps: List[OrderBookDepth5] = []
        sym_map = {
            "1000PEPE-USDT": ("PEPE", 1000.0),
        }
        stream = BingXStream(sym_map, snaps.append)

        # 1. Reverse symbol resolution ensures subscription matches exchange symbol
        self.assertEqual(stream._to_raw_symbol("PEPE"), "1000PEPE-USDT")
        self.assertEqual(stream._to_raw_symbol("1000PEPE-USDT"), "1000PEPE-USDT")

        # 2. Parsing scales price down by 1000, qty up by 1000
        msg_pepe = json.dumps({
            "code": 0,
            "dataType": "1000PEPE-USDT@depth20",
            "ts": 1700000000000,
            "data": {
                "bids": [["0.0038", "100.0"]],
                "asks": [["0.0039", "200.0"]],
            }
        })
        stream._parse_message(msg_pepe)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "PEPE")
        self.assertEqual(snap.raw_symbol, "1000PEPE-USDT")
        # Price scaled: 0.0038 / 1000 = 0.0000038
        self.assertAlmostEqual(snap.bids[0][0], 0.0000038)
        # Quantity scaled: 100.0 * 1000 = 100,000.0
        self.assertAlmostEqual(snap.bids[0][1], 100_000.0)
        # Depth in USD: 0.0039 * 200 = 0.78 USD
        self.assertAlmostEqual(snap.depth_ask_usd, 0.78, places=4)

    def test_gateio_1000x_contracts(self):
        """Gate.io: 1000x ticker prefix scales price/qty alongside quanto_multiplier."""
        snaps: List[OrderBookDepth5] = []
        sym_map = {
            "1000BONK_USDT": ("BONK", 1000.0),
        }
        # quanto = 100 (1 contract = 100 units of 1000BONK = 100,000 BONK)
        contract_sizes = {"1000BONK_USDT": 100.0}
        stream = GateioStream(sym_map, snaps.append, contract_sizes=contract_sizes)

        self.assertEqual(stream._to_raw_symbol("BONK"), "1000BONK_USDT")

        msg = json.dumps({
            "channel": "futures.order_book_update",
            "event": "all",
            "result": {
                "contract": "1000BONK_USDT",
                "t": 1700000000000,
                "b": [{"p": "0.0108", "s": 50}],   # 50 contracts * 100 quanto * 1000 mult = 5,000,000 BONK
                "a": [{"p": "0.0109", "s": 100}],  # 100 contracts * 100 quanto * 1000 mult = 10,000,000 BONK
            }
        })
        stream._parse_message(msg)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "BONK")
        # Price: 0.0108 / 1000 = 0.0000108
        self.assertAlmostEqual(snap.bids[0][0], 0.0000108)
        # Qty: 50 * 100 * 1000 = 5,000,000
        self.assertAlmostEqual(snap.bids[0][1], 5_000_000.0)
        # USD Depth: 0.0109 * 100 * 100 = 109.0 USD
        self.assertAlmostEqual(snap.depth_ask_usd, 109.0, places=2)

    def test_pionex_1000x_contracts(self):
        """Pionex: 1000x contracts resolve base coin, scale price down and qty up."""
        snaps: List[OrderBookDepth5] = []
        sym_map = {
            "1000PEPE_USDT_PERP": ("PEPE", 1000.0),
        }
        stream = PionexStream(sym_map, snaps.append)

        self.assertEqual(stream._to_raw_symbol("PEPE"), "1000PEPE_USDT_PERP")

        msg = json.dumps({
            "topic": "DEPTH",
            "symbol": "1000PEPE_USDT_PERP",
            "timestamp": 1700000000000,
            "data": {
                "bids": [["0.00385", "50"]],
                "asks": [["0.00388", "100"]],
            }
        })
        stream._parse_message(msg)
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.symbol, "PEPE")
        self.assertEqual(snap.raw_symbol, "1000PEPE_USDT_PERP")
        self.assertAlmostEqual(snap.bids[0][0], 0.00000385)
        self.assertAlmostEqual(snap.bids[0][1], 50_000.0)
        self.assertAlmostEqual(snap.asks[0][0], 0.00000388)
        self.assertAlmostEqual(snap.asks[0][1], 100_000.0)
        self.assertAlmostEqual(snap.depth_ask_usd, 0.00388 * 100, places=4)

    def test_gateio_session_isolation_user_agent(self):
        """GateioRestClient must not mutate the shared session's User-Agent."""
        from venues.base import create_http_session
        from venues.gateio import GateioRestClient

        shared_session = create_http_session()
        original_ua = shared_session.headers.get("User-Agent")
        self.assertIsNotNone(original_ua)

        # Initialize GateioRestClient passing shared_session
        client = GateioRestClient(shared_session)

        # Gate.io's session must not have User-Agent (for its WAF requirements)
        self.assertNotIn("User-Agent", client.session.headers)

        # Shared session must RETAIN its User-Agent for Binance/Bybit/Upbit/OKX/etc.
        self.assertEqual(shared_session.headers.get("User-Agent"), original_ua)

    def test_gateio_timestamp_parsing_fallbacks(self):
        """GateioStream must safely parse timestamps even if time is null or time_ms is present."""
        snaps: List[OrderBookDepth5] = []
        stream = GateioStream(["BTC_USDT"], snaps.append)

        # 1. Message with result.t present and time is null (would crash old code with None * 1000!)
        msg1 = json.dumps({
            "channel": "futures.order_book_update",
            "event": "update",
            "time": None,
            "result": {
                "s": "BTC_USDT",
                "t": 1700000000123,
                "b": [{"p": "80000.0", "s": "1.0"}],
                "a": [{"p": "80001.0", "s": "1.0"}],
            }
        })
        stream._parse_message(msg1)
        self.assertEqual(len(snaps), 1)
        self.assertEqual(snaps[0].exchange_ts, 1700000000123)

        # 2. Message with time_ms present, result.t absent, time is null
        snaps.clear()
        msg2 = json.dumps({
            "channel": "futures.order_book_update",
            "event": "update",
            "time": None,
            "time_ms": 1700000000456,
            "result": {
                "s": "BTC_USDT",
                "b": [{"p": "80000.0", "s": "1.0"}],
                "a": [{"p": "80001.0", "s": "1.0"}],
            }
        })
        stream._parse_message(msg2)
        self.assertEqual(len(snaps), 1)
        self.assertEqual(snaps[0].exchange_ts, 1700000000456)

    def test_asterdex_stream_and_multiplier(self):
        """AsterdexStream: parses Binance-format depth5 and handles 1000x prefixes."""
        snaps: List[OrderBookDepth5] = []
        stream = AsterdexStream(["BTCUSDT", "1000PEPEUSDT"], snaps.append)

        # 1. Standard BTCUSDT stream message
        msg_btc = json.dumps({
            "stream": "btcusdt@depth5@100ms",
            "data": {
                "e": "depthUpdate",
                "E": 1700000000100,
                "s": "BTCUSDT",
                "b": [["85000.0", "1.5"], ["84999.0", "2.0"]],
                "a": [["85001.0", "0.8"], ["85002.0", "1.2"]],
            }
        })
        stream._parse_message(msg_btc)
        self.assertEqual(len(snaps), 1)
        snap = snaps[-1]
        self.assertEqual(snap.exchange, "asterdex")
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.raw_symbol, "BTCUSDT")
        self.assertEqual(snap.bids[0], (85000.0, 1.5))
        self.assertEqual(snap.asks[0], (85001.0, 0.8))
        self.assertEqual(snap.exchange_ts, 1700000000100)

        # 2. 1000PEPEUSDT unwrapped message
        msg_pepe = json.dumps({
            "e": "depthUpdate",
            "E": 1700000000200,
            "s": "1000PEPEUSDT",
            "b": [["0.015", "100.0"]],
            "a": [["0.016", "200.0"]],
        })
        stream._parse_message(msg_pepe)
        self.assertEqual(len(snaps), 2)
        pepe_snap = snaps[-1]
        self.assertEqual(pepe_snap.exchange, "asterdex")
        self.assertEqual(pepe_snap.symbol, "PEPE")
        self.assertEqual(pepe_snap.raw_symbol, "1000PEPEUSDT")
        # 0.015 / 1000 = 0.000015, 100.0 * 1000 = 100000.0
        self.assertAlmostEqual(pepe_snap.bids[0][0], 0.000015)
        self.assertAlmostEqual(pepe_snap.bids[0][1], 100000.0)
        self.assertAlmostEqual(pepe_snap.asks[0][0], 0.000016)
        self.assertAlmostEqual(pepe_snap.asks[0][1], 200000.0)

    def test_asterdex_rest_discovery(self):
        """AsterdexRestClient: parses Binance-format exchangeInfo."""
        from unittest.mock import MagicMock
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "symbols": [
                {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "1000PEPEUSDT", "baseAsset": "1000PEPE", "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING"},
                {"symbol": "ETHUSDC", "baseAsset": "ETH", "quoteAsset": "USDC", "contractType": "PERPETUAL", "status": "TRADING"},
            ]
        }
        mock_session.get.return_value = mock_resp

        client = AsterdexRestClient(session=mock_session)
        perps = client.fetch_linear_perpetuals()
        self.assertEqual(perps.get("BTCUSDT"), ("BTC", 1.0, 1.0))
        self.assertEqual(perps.get("1000PEPEUSDT"), ("PEPE", 1000.0, 1.0))
        self.assertNotIn("ETHUSDC", perps)

        sizes = client.fetch_contract_sizes()
        self.assertEqual(sizes.get("BTCUSDT"), 1.0)

    def test_asterdex_stream_with_contract_size(self):
        """AsterdexStream: passes contract_size to _extract_levels."""
        snaps: List[OrderBookDepth5] = []
        sym_map = {
            "BTCUSDT": ("BTC", 1.0, 0.1),
            "1000PEPEUSDT": ("PEPE", 1000.0, 2.0),
        }
        stream = AsterdexStream(sym_map, snaps.append)

        # 1. BTC with contract_size = 0.1
        msg = json.dumps({
            "e": "depthUpdate",
            "E": 1700000000000,
            "s": "BTCUSDT",
            "b": [["80000.0", "50.0"]],  # 50 contracts * 0.1 = 5.0 BTC
            "a": [["80001.0", "100.0"]], # 100 contracts * 0.1 = 10.0 BTC
        })
        stream._parse_message(msg)
        self.assertEqual(len(snaps), 1)
        snap = snaps[-1]
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.bids[0], (80000.0, 5.0))
        self.assertEqual(snap.asks[0], (80001.0, 10.0))

        # 2. 1000PEPE with mult = 1000 and contract_size = 2.0
        msg_pepe = json.dumps({
            "e": "depthUpdate",
            "E": 1700000000100,
            "s": "1000PEPEUSDT",
            "b": [["0.012", "10.0"]],  # price 0.000012, qty = 10.0 * 2.0 * 1000 = 20,000 PEPE
            "a": [["0.013", "20.0"]],  # price 0.000013, qty = 20.0 * 2.0 * 1000 = 40,000 PEPE
        })
        stream._parse_message(msg_pepe)
        self.assertEqual(len(snaps), 2)
        pepe_snap = snaps[-1]
        self.assertEqual(pepe_snap.symbol, "PEPE")
        self.assertAlmostEqual(pepe_snap.bids[0][0], 0.000012)
        self.assertAlmostEqual(pepe_snap.bids[0][1], 20000.0)
        self.assertAlmostEqual(pepe_snap.asks[0][0], 0.000013)
        self.assertAlmostEqual(pepe_snap.asks[0][1], 40000.0)

    def test_hyperliquid_stream_and_k_multiplier(self):
        """HyperliquidStream: parses l2Book levels and handles k-prefixed 1000x coins."""
        snaps: List[OrderBookDepth5] = []
        stream = HyperliquidStream(["SOL", "kBONK"], snaps.append)

        # 1. Standard coin SOL
        msg_sol = json.dumps({
            "channel": "l2Book",
            "data": {
                "coin": "SOL",
                "time": 1700000000300,
                "levels": [
                    [{"px": "150.0", "sz": "10.0", "n": 1}, {"px": "149.9", "sz": "5.0", "n": 2}],
                    [{"px": "150.1", "sz": "12.0", "n": 1}, {"px": "150.2", "sz": "8.0", "n": 3}],
                ]
            }
        })
        stream._parse_message(msg_sol)
        self.assertEqual(len(snaps), 1)
        snap = snaps[-1]
        self.assertEqual(snap.exchange, "hyperliquid")
        self.assertEqual(snap.symbol, "SOL")
        self.assertEqual(snap.raw_symbol, "SOLUSDT")
        self.assertEqual(snap.bids[0], (150.0, 10.0))
        self.assertEqual(snap.asks[0], (150.1, 12.0))
        self.assertEqual(snap.exchange_ts, 1700000000300)

        # 2. k-prefixed coin kBONK (multiplier = 1000.0)
        msg_bonk = json.dumps({
            "channel": "l2Book",
            "data": {
                "coin": "kBONK",
                "time": 1700000000400,
                "levels": [
                    [{"px": "0.025", "sz": "500.0", "n": 1}],
                    [{"px": "0.026", "sz": "600.0", "n": 2}],
                ]
            }
        })
        stream._parse_message(msg_bonk)
        self.assertEqual(len(snaps), 2)
        bonk_snap = snaps[-1]
        self.assertEqual(bonk_snap.exchange, "hyperliquid")
        self.assertEqual(bonk_snap.symbol, "BONK")
        self.assertEqual(bonk_snap.raw_symbol, "1000BONKUSDT")
        # 0.025 / 1000 = 0.000025, 500.0 * 1000 = 500000.0
        self.assertAlmostEqual(bonk_snap.bids[0][0], 0.000025)
        self.assertAlmostEqual(bonk_snap.bids[0][1], 500000.0)
        self.assertAlmostEqual(bonk_snap.asks[0][0], 0.000026)
        self.assertAlmostEqual(bonk_snap.asks[0][1], 600000.0)

    def test_hyperliquid_stream_with_contract_size_and_multiscale(self):
        """HyperliquidStream: supports custom contract_size and multi-scale prefixes (m, b)."""
        snaps: List[OrderBookDepth5] = []
        sym_map = {
            "SOLUSDT": ("SOL", 1.0, 0.5),
            "1000000PEPEUSDT": ("PEPE", 1_000_000.0, 1.0),
            "1000000000LUNCUSDT": ("LUNC", 1_000_000_000.0, 1.0),
        }
        stream = HyperliquidStream(sym_map, snaps.append)

        # 1. SOL with contract_size = 0.5
        msg_sol = json.dumps({
            "channel": "l2Book",
            "data": {
                "coin": "SOL",
                "time": 1700000000500,
                "levels": [
                    [{"px": "160.0", "sz": "20.0"}],
                    [{"px": "160.2", "sz": "40.0"}],
                ]
            }
        })
        stream._parse_message(msg_sol)
        self.assertEqual(len(snaps), 1)
        snap = snaps[-1]
        self.assertEqual(snap.symbol, "SOL")
        self.assertEqual(snap.bids[0], (160.0, 10.0))  # 20 * 0.5 = 10.0
        self.assertEqual(snap.asks[0], (160.2, 20.0))  # 40 * 0.5 = 20.0

        # 2. mPEPE (1M scale)
        msg_mpepe = json.dumps({
            "channel": "l2Book",
            "data": {
                "coin": "mPEPE",
                "time": 1700000000600,
                "levels": [
                    [{"px": "10.0", "sz": "5.0"}],  # price per 1M = $10 -> single price $0.00001
                    [{"px": "10.1", "sz": "8.0"}],
                ]
            }
        })
        stream._parse_message(msg_mpepe)
        self.assertEqual(len(snaps), 2)
        m_snap = snaps[-1]
        self.assertEqual(m_snap.symbol, "PEPE")
        self.assertAlmostEqual(m_snap.bids[0][0], 0.00001)
        self.assertAlmostEqual(m_snap.bids[0][1], 5_000_000.0)

    def test_hyperliquid_rest_discovery(self):
        """HyperliquidRestClient: parses metaAndAssetCtxs with k-multipliers."""
        from unittest.mock import MagicMock
        mock_session = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [
            {
                "universe": [
                    {"name": "BTC", "szDecimals": 4},
                    {"name": "kSHIB", "szDecimals": 0},
                ]
            },
            [
                {"midPx": "80000.0", "dayNtlVlm": "50000000.0"},
                {"midPx": "0.025", "dayNtlVlm": "1200000.0"},
            ]
        ]
        mock_session.post.return_value = mock_resp

        client = HyperliquidRestClient(session=mock_session)
        perps = client.fetch_linear_perpetuals()
        self.assertEqual(perps.get("BTCUSDT"), ("BTC", 1.0, 1.0))
        self.assertEqual(perps.get("1000SHIBUSDT"), ("SHIB", 1000.0, 1.0))

        sizes = client.fetch_contract_sizes()
        self.assertEqual(sizes.get("BTCUSDT"), 1.0)
        self.assertEqual(sizes.get("1000SHIBUSDT"), 1.0)

        ticks = client.fetch_tickers_24h()
        self.assertEqual(ticks["BTCUSDT"]["vol_24h"], 50000000.0)
        self.assertEqual(ticks["BTCUSDT"]["price"], 80000.0)
        self.assertEqual(ticks["1000SHIBUSDT"]["vol_24h"], 1200000.0)
        self.assertEqual(ticks["1000SHIBUSDT"]["price"], 0.025)

    def test_symbol_multiplier_normalization(self):
        """venues/base.py: parse_symbol_and_mult handles multi-scale and preserves genuine tokens."""
        from venues.base import parse_symbol_and_mult
        cases = [
            ("1000PEPE-USDT", ("PEPE", 1000.0)),
            ("1MPEPE_USDT", ("PEPE", 1_000_000.0)),
            ("1BPEPEUSDT", ("PEPE", 1_000_000_000.0)),
            ("1000000PEPEUSDT", ("PEPE", 1_000_000.0)),
            ("1000000000LUNCUSDT", ("LUNC", 1_000_000_000.0)),
            ("kPEPE", ("PEPE", 1000.0)),
            ("kSHIB", ("SHIB", 1000.0)),
            ("mPEPE", ("PEPE", 1_000_000.0)),
            ("bLUNC", ("LUNC", 1_000_000_000.0)),
            ("KASUSDT", ("KAS", 1.0)),
            ("MKRUSDT", ("MKR", 1.0)),
            ("BTC-USDT-SWAP", ("BTC", 1.0)),
            ("BONKUSDT", ("BONK", 1.0)),
            ("1000BONKUSDT", ("BONK", 1000.0)),
            ("1KPEPE", ("PEPE", 1000.0)),
        ]
        for raw, expected in cases:
            res = parse_symbol_and_mult(raw)
            self.assertEqual(res, expected, f"Failed for {raw}: got {res}, expected {expected}")

    def test_bybit_thread_safe_copy_on_write(self):
        """BybitPerpStream: Copy-on-Write updates ensure thread-safe book snapshots without corrupt dict states."""
        from venues.bybit import BybitPerpStream
        snaps: List[OrderBookDepth5] = []
        sym_map = {"BTCUSDT": ("BTC", 1.0, 1.0)}
        stream = BybitPerpStream(sym_map, snaps.append)

        # 1. Snapshot message
        snap_msg = json.dumps({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1700000000000,
            "data": {
                "s": "BTCUSDT",
                "b": [["80000.0", "1.0"], ["79990.0", "2.0"]],
                "a": [["80010.0", "1.5"], ["80020.0", "2.5"]],
            }
        })
        stream._parse_message(snap_msg)
        book = stream.get_book_snapshot("BTCUSDT")
        self.assertIsNotNone(book)
        self.assertEqual(book["bids"][80000.0], 1.0)
        self.assertEqual(book["asks"][80010.0], 1.5)

        # 2. Delta update (removes 80000.0 bid, adds 80005.0)
        delta_msg = json.dumps({
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "ts": 1700000000100,
            "data": {
                "s": "BTCUSDT",
                "b": [["80000.0", "0.0"], ["80005.0", "3.0"]],
                "a": [["80010.0", "4.0"]],
            }
        })
        stream._parse_message(delta_msg)
        book2 = stream.get_book_snapshot("BTCUSDT")
        self.assertNotIn(80000.0, book2["bids"])
        self.assertEqual(book2["bids"][80005.0], 3.0)
        self.assertEqual(book2["asks"][80010.0], 4.0)
        # Previous snapshot reference remains unmodified (immutability)
        self.assertEqual(book["bids"][80000.0], 1.0)

    def test_bybit_sequence_gap_detection(self):
        """BybitPerpStream: sequence gaps in pu/u or out-of-order deltas invalidate the book."""
        from venues.bybit import BybitPerpStream
        snaps: List[OrderBookDepth5] = []
        sym_map = {"BTCUSDT": ("BTC", 1.0, 1.0)}
        stream = BybitPerpStream(sym_map, snaps.append)

        # 1. Snapshot message with u = 100
        snap_msg = json.dumps({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1700000000000,
            "data": {
                "s": "BTCUSDT",
                "u": 100,
                "seq": 1000,
                "b": [["80000.0", "1.0"]],
                "a": [["80010.0", "1.0"]],
            }
        })
        stream._parse_message(snap_msg)
        self.assertIsNotNone(stream.get_book_snapshot("BTCUSDT"))
        self.assertEqual(stream.seq_gaps, 0)

        # 2. Sequential delta (pu = 100, u = 101) -> Valid
        delta_valid = json.dumps({
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "ts": 1700000000100,
            "data": {
                "s": "BTCUSDT",
                "pu": 100,
                "u": 101,
                "b": [["80005.0", "2.0"]],
                "a": [],
            }
        })
        stream._parse_message(delta_valid)
        self.assertIsNotNone(stream.get_book_snapshot("BTCUSDT"))
        self.assertEqual(stream.seq_gaps, 0)

        # 3. Gap delta (pu = 105, expected 101) -> Sequence Gap! Book evicted!
        delta_gap = json.dumps({
            "topic": "orderbook.50.BTCUSDT",
            "type": "delta",
            "ts": 1700000000200,
            "data": {
                "s": "BTCUSDT",
                "pu": 105,
                "u": 106,
                "b": [["80006.0", "2.0"]],
                "a": [],
            }
        })
        stream._parse_message(delta_gap)
        self.assertEqual(stream.seq_gaps, 1)
        # Corrupt book evicted from memory
        self.assertIsNone(stream.get_book_snapshot("BTCUSDT"))

        # 4. Subsequent delta without a snapshot is dropped
        stream._parse_message(delta_valid)
        self.assertEqual(stream.seq_gaps, 2)
        self.assertIsNone(stream.get_book_snapshot("BTCUSDT"))

    def test_bybit_memory_capping_under_spam(self):
        """BybitPerpStream: MAX_BOOK_DEPTH strictly limits book size to 25 levels during spam."""
        from venues.bybit import BybitPerpStream
        snaps: List[OrderBookDepth5] = []
        sym_map = {"BTCUSDT": ("BTC", 1.0, 1.0)}
        stream = BybitPerpStream(sym_map, snaps.append)

        # Generate snapshot with 50 distinct levels
        bids_spam = [[str(80000 - i * 10), "1.0"] for i in range(50)]
        asks_spam = [[str(80010 + i * 10), "1.0"] for i in range(50)]
        snap_msg = json.dumps({
            "topic": "orderbook.50.BTCUSDT",
            "type": "snapshot",
            "ts": 1700000000000,
            "data": {
                "s": "BTCUSDT",
                "u": 1,
                "b": bids_spam,
                "a": asks_spam,
            }
        })
        stream._parse_message(snap_msg)
        book = stream.get_book_snapshot("BTCUSDT")
        self.assertIsNotNone(book)
        # Memory capped to 25 levels max, not allowed to grow to 50 or 100+
        self.assertLessEqual(len(book["bids"]), 25)
        self.assertLessEqual(len(book["asks"]), 25)

    def test_gateio_sequence_gap_detection(self):
        """GateioStream: sequence gap in id invalidates local book cache."""
        snaps: List[OrderBookDepth5] = []
        stream = GateioStream(["BTC_USDT"], snaps.append)

        # 1. Snapshot with id = 10
        snap_msg = json.dumps({
            "channel": "futures.order_book_update",
            "event": "all",
            "result": {
                "contract": "BTC_USDT",
                "id": 10,
                "b": [{"p": "80000.0", "s": "1.0"}],
                "a": [{"p": "80001.0", "s": "1.0"}],
            }
        })
        stream._parse_message(snap_msg)
        self.assertIsNotNone(stream.get_book_snapshot("BTC_USDT"))
        self.assertEqual(stream.seq_gaps, 0)

        # 2. Sequential update (id = 11) -> Valid
        up_valid = json.dumps({
            "channel": "futures.order_book_update",
            "event": "update",
            "result": {
                "contract": "BTC_USDT",
                "id": 11,
                "b": [{"p": "80000.5", "s": "2.0"}],
                "a": [],
            }
        })
        stream._parse_message(up_valid)
        self.assertIsNotNone(stream.get_book_snapshot("BTC_USDT"))
        self.assertEqual(stream.seq_gaps, 0)

        # 3. Gap update (id = 15, expected 12) -> Sequence gap! Book evicted!
        up_gap = json.dumps({
            "channel": "futures.order_book_update",
            "event": "update",
            "result": {
                "contract": "BTC_USDT",
                "id": 15,
                "b": [{"p": "80000.8", "s": "2.0"}],
                "a": [],
            }
        })
        stream._parse_message(up_gap)
        self.assertEqual(stream.seq_gaps, 1)
        self.assertIsNone(stream.get_book_snapshot("BTC_USDT"))

    def test_gateio_memory_capping_under_spam(self):
        """GateioStream: MAX_BOOK_DEPTH bounds dictionary to 25 levels max."""
        snaps: List[OrderBookDepth5] = []
        stream = GateioStream(["BTC_USDT"], snaps.append)

        bids_spam = [{"p": str(80000 - i * 5), "s": "1.0"} for i in range(40)]
        asks_spam = [{"p": str(80005 + i * 5), "s": "1.0"} for i in range(40)]
        msg = json.dumps({
            "channel": "futures.order_book_update",
            "event": "all",
            "result": {
                "contract": "BTC_USDT",
                "id": 1,
                "b": bids_spam,
                "a": asks_spam,
            }
        })
        stream._parse_message(msg)
        book = stream.get_book_snapshot("BTC_USDT")
        self.assertIsNotNone(book)
        self.assertLessEqual(len(book["bids"]), 25)
        self.assertLessEqual(len(book["asks"]), 25)

    def test_binance_and_asterdex_sequence_gap_detection(self):
        """BinanceFuturesStream and AsterdexStream: verify pu == last_u continuity."""
        from venues.binance import BinanceFuturesStream
        snaps_binance: List[OrderBookDepth5] = []
        stream_b = BinanceFuturesStream({"BTCUSDT": ("BTC", 1.0, 1.0)}, snaps_binance.append)

        # 1. Initial tick u = 100
        msg1 = json.dumps({
            "s": "BTCUSDT",
            "u": 100,
            "b": [["80000.0", "1.0"]],
            "a": [["80001.0", "1.0"]],
        })
        stream_b._parse_message(msg1)
        self.assertEqual(len(snaps_binance), 1)

        # 2. Sequential tick (pu = 100, u = 105)
        msg2 = json.dumps({
            "s": "BTCUSDT",
            "pu": 100,
            "u": 105,
            "b": [["80000.0", "1.0"]],
            "a": [["80001.0", "1.0"]],
        })
        stream_b._parse_message(msg2)
        self.assertEqual(len(snaps_binance), 2)
        self.assertEqual(stream_b.seq_gaps, 0)

        # 3. Gap tick (pu = 110, expected 105) -> Gap detected, tick dropped!
        msg3 = json.dumps({
            "s": "BTCUSDT",
            "pu": 110,
            "u": 115,
            "b": [["80000.0", "1.0"]],
            "a": [["80001.0", "1.0"]],
        })
        stream_b._parse_message(msg3)
        self.assertEqual(len(snaps_binance), 2)  # Not increased
        self.assertEqual(stream_b.seq_gaps, 1)

        # 4. Aster DEX gap verification
        snaps_aster: List[OrderBookDepth5] = []
        stream_a = AsterdexStream({"BTCUSDT": ("BTC", 1.0, 1.0)}, snaps_aster.append)

        msg_a1 = json.dumps({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "u": 500,
            "b": [["80000.0", "1.0"]],
            "a": [["80001.0", "1.0"]],
        })
        stream_a._parse_message(msg_a1)
        self.assertEqual(len(snaps_aster), 1)

        # Gap tick in Aster DEX
        msg_a2 = json.dumps({
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "pu": 520,
            "u": 525,
            "b": [["80000.0", "1.0"]],
            "a": [["80001.0", "1.0"]],
        })
        stream_a._parse_message(msg_a2)
        self.assertEqual(len(snaps_aster), 1)  # Dropped
        self.assertEqual(stream_a.seq_gaps, 1)

    def test_okx_seq_id_dedup(self):
        """OKXStream: drops duplicate or backward seqId messages."""
        snaps: List[OrderBookDepth5] = []
        stream = OKXStream({"BTC-USDT-SWAP": ("BTC", 1.0, 1.0)}, snaps.append)

        msg1 = json.dumps({
            "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "seqId": 1000,
                "bids": [["80000.0", "1.0", "0", "1"]],
                "asks": [["80001.0", "1.0", "0", "1"]],
            }]
        })
        stream._parse_message(msg1)
        self.assertEqual(len(snaps), 1)

        # Duplicate / backward seqId 999
        msg2 = json.dumps({
            "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
            "data": [{
                "seqId": 999,
                "bids": [["80000.0", "1.0", "0", "1"]],
                "asks": [["80001.0", "1.0", "0", "1"]],
            }]
        })
        stream._parse_message(msg2)
        self.assertEqual(len(snaps), 1)
        self.assertEqual(stream.seq_duplicates, 1)


if __name__ == "__main__":
    unittest.main()


