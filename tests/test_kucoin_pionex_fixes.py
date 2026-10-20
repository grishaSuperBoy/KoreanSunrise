"""
tests/test_kucoin_pionex_fixes.py - Comprehensive Unit Tests for KuCoin symbol resolution,
Pionex WS rate limiting pacing, and 30-minute BatchWriter memory buffering.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

from batch_writer import BatchWriter
import config as C
from models import OrderBookDepth5
from venues.kucoin import KucoinStream
from venues.pionex import PionexStream


class TestKucoinSymbolResolution(unittest.TestCase):
    """Deep verification of KuCoin symbol mapping, contract resolution, and depth parsing."""

    def setUp(self):
        self.mock_contracts_response = {
            "code": "200000",
            "data": [
                {"symbol": "XBTUSDTM", "baseCurrency": "XBT", "multiplier": 0.001},
                {"symbol": "ETHFIUSDTM", "baseCurrency": "ETHFI", "multiplier": 1.0},
                {"symbol": "AXSUSDTM", "baseCurrency": "AXS", "multiplier": 0.1},
                {"symbol": "SOLUSDTM", "baseCurrency": "SOL", "multiplier": 1.0},
                {"symbol": "DOGEUSDTM", "baseCurrency": "DOGE", "multiplier": 10.0},
            ]
        }

    def test_init_with_dict_symbols(self):
        """Verify initialization with a venue symbol map preserves contracts and sizes."""
        sym_map = {
            "ETHFIUSDTM": ("ETHFI", 1.0, 1.0),
            "AXSUSDTM": ("AXS", 1.0, 0.1),
        }
        stream = KucoinStream(sym_map, lambda x: None)
        self.assertIn("ETHFIUSDTM", stream._sym_map)
        self.assertIn("AXSUSDTM", stream._sym_map)
        self.assertEqual(stream._sym_map["AXSUSDTM"], ("AXS", 1.0, 0.1))
        self.assertEqual(stream._contract_sizes.get("AXSUSDTM"), 0.1)

    def test_resolve_symbols_with_venue_symbols_dict(self):
        """CRITICAL: Test that passing venue symbols (ETHFIUSDTM) properly resolves all targets."""
        sym_map = {
            "ETHFIUSDTM": ("ETHFI", 1.0, 1.0),
            "AXSUSDTM": ("AXS", 1.0, 0.1),
            "XBTUSDTM": ("BTC", 1.0, 0.001),
        }
        stream = KucoinStream(sym_map, lambda x: None)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self.mock_contracts_response

        with patch.object(stream._session, "get", return_value=mock_resp):
            resolved = stream._resolve_symbols()

        # Must resolve all 3 requested contracts
        self.assertEqual(len(resolved), 3)
        self.assertEqual(resolved["ETHFIUSDTM"], "ETHFIUSDTM")
        self.assertEqual(resolved["AXSUSDTM"], "AXSUSDTM")
        self.assertEqual(resolved["XBTUSDTM"], "XBTUSDTM")

        # Reverse map to canonical coins must be accurate
        self.assertEqual(stream._venue_to_ours["ETHFIUSDTM"], "ETHFI")
        self.assertEqual(stream._venue_to_ours["AXSUSDTM"], "AXS")
        self.assertEqual(stream._venue_to_ours["XBTUSDTM"], "BTC")
        self.assertEqual(stream._contract_sizes["AXSUSDTM"], 0.1)

    def test_resolve_symbols_with_base_coin_list(self):
        """Test backward compatibility when symbols is a list of bare coin names."""
        coins = ["ETHFI", "AXS", "BTC"]
        stream = KucoinStream(coins, lambda x: None)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self.mock_contracts_response

        with patch.object(stream._session, "get", return_value=mock_resp):
            resolved = stream._resolve_symbols()

        self.assertEqual(len(resolved), 3)
        self.assertEqual(resolved["ETHFI"], "ETHFIUSDTM")
        self.assertEqual(resolved["AXS"], "AXSUSDTM")
        self.assertEqual(resolved["BTC"], "XBTUSDTM")

        self.assertEqual(stream._venue_to_ours["ETHFIUSDTM"], "ETHFI")
        self.assertEqual(stream._venue_to_ours["AXSUSDTM"], "AXS")
        self.assertEqual(stream._venue_to_ours["XBTUSDTM"], "BTC")

    def test_parse_depth5_message_and_canonical_coin(self):
        """Test incoming depth update correctly produces OrderBookDepth5 with canonical coin."""
        received = []
        sym_map = {"AXSUSDTM": ("AXS", 1.0, 0.1)}
        stream = KucoinStream(sym_map, callback=received.append)
        stream._venue_to_ours["AXSUSDTM"] = "AXS"
        stream._contract_sizes["AXSUSDTM"] = 0.1

        raw_msg = json.dumps({
            "type": "message",
            "topic": "/contractMarket/level2Depth5:AXSUSDTM",
            "subject": "level2",
            "data": {
                "bids": [["5.50", "100"], ["5.49", "200"]],
                "asks": [["5.52", "150"], ["5.53", "300"]],
                "timestamp": 1700000000000
            }
        })

        stream._parse_message(raw_msg)
        self.assertEqual(len(received), 1)
        snap: OrderBookDepth5 = received[0]
        self.assertEqual(snap.exchange, "kucoin")
        self.assertEqual(snap.symbol, "AXS")
        self.assertEqual(snap.raw_symbol, "AXSUSDTM")

        # Multiplier of 0.1 applied: 100 contracts * 0.1 = 10.0 tokens
        self.assertEqual(snap.bids[0], (5.50, 10.0))
        self.assertEqual(snap.asks[0], (5.52, 15.0))


class TestPionexRateLimitingPacing(unittest.TestCase):
    """Verification of Pionex subscription spacing to avoid 1008 policy violation rate limit."""

    def test_subscription_sleep_pacing_at_least_200ms(self):
        """Ensure subscription loop sleeps >= 200ms between symbols to protect against 1008."""
        syms = ["BTC_USDT_PERP", "ETH_USDT_PERP", "SOL_USDT_PERP"]
        stream = PionexStream(syms, lambda x: None)

        sent_times = []

        class MockWS:
            async def send(self, data):
                sent_times.append(time.monotonic())

        async def _run():
            ws = MockWS()
            await stream._send_subscriptions(ws)

        asyncio.run(_run())

        self.assertEqual(len(sent_times), 3)
        diff1 = sent_times[1] - sent_times[0]
        diff2 = sent_times[2] - sent_times[1]
        self.assertGreaterEqual(diff1, 0.20, f"Pionex pacing too fast: {diff1:.3f}s")
        self.assertGreaterEqual(diff2, 0.20, f"Pionex pacing too fast: {diff2:.3f}s")

    def test_pionex_symbol_transformation(self):
        """Test _to_raw_symbol handles canonical coins, maps, and existing PERP suffixes."""
        sym_map = {"BTC_USDT_PERP": ("BTC", 1.0), "ETH_USDT_PERP": ("ETH", 1.0)}
        stream = PionexStream(sym_map, lambda x: None)
        self.assertEqual(stream._to_raw_symbol("BTC"), "BTC_USDT_PERP")
        self.assertEqual(stream._to_raw_symbol("ETH"), "ETH_USDT_PERP")
        self.assertEqual(stream._to_raw_symbol("SOL_USDT_PERP"), "SOL_USDT_PERP")
        self.assertEqual(stream._to_raw_symbol("DOGE"), "DOGE_USDT_PERP")


class TestBatchWriter30MinuteBuffering(unittest.TestCase):
    """Verification that BatchWriter buffers in RAM and only writes to disk every 30 minutes."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_default_flush_interval_is_1800s(self):
        writer = BatchWriter(data_root=self.tmp_dir)
        self.assertEqual(writer.flush_interval_sec, 1800.0)

    def test_in_memory_buffering_before_30_minutes(self):
        """Signals must remain in RAM without touching disk prior to 30 minutes."""
        writer = BatchWriter(data_root=self.tmp_dir, flush_interval_sec=1800.0)
        start_ts = 1000000.0
        writer._last_flush = start_ts

        # Add 5 signals
        for i in range(5):
            writer.add({"id": f"sig_{i}", "coin": "BTC", "edge": 25.0})

        self.assertEqual(writer.rows_count, 5)

        # At 15 minutes (900s), maybe_flush must NOT flush
        writer.maybe_flush(now=start_ts + 900.0)
        self.assertEqual(writer.rows_count, 5)
        self.assertEqual(writer.total_written, 0)

        # Check signals folder on disk is empty
        sig_dir = os.path.join(self.tmp_dir, "signals")
        files = os.listdir(sig_dir)
        self.assertEqual(len(files), 0)

    def test_flushes_at_or_after_1800_seconds(self):
        """Signals must flush cleanly to compressed .jsonl.gz when 1800s expires."""
        writer = BatchWriter(data_root=self.tmp_dir, flush_interval_sec=1800.0)
        start_ts = 1000000.0
        writer._last_flush = start_ts

        writer.add({"id": "sig_1", "coin": "PUFFER", "edge": 30.0})
        writer.add({"id": "sig_2", "coin": "TIA", "edge": 35.0})

        # At 30 minutes + 1s (1801.0s), maybe_flush MUST flush
        writer.maybe_flush(now=start_ts + 1801.0)
        self.assertEqual(writer.rows_count, 0)
        self.assertEqual(writer.total_written, 2)

        # Check compressed file exists on disk
        sig_dir = os.path.join(self.tmp_dir, "signals")
        all_files = []
        for root, _, fs in os.walk(sig_dir):
            for f in fs:
                if f.endswith(".jsonl.gz"):
                    all_files.append(os.path.join(root, f))

        self.assertEqual(len(all_files), 1)


if __name__ == "__main__":
    unittest.main()
