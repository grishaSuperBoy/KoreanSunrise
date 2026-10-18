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


if __name__ == "__main__":
    unittest.main()


