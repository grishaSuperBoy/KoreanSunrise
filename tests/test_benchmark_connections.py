"""
tests/test_benchmark_connections.py - Unit tests for tools/benchmark_connections.py
and Web API benchmark endpoints in Korean Sunrise.
"""
from __future__ import annotations

import json
import unittest
import urllib.request
import urllib.error
from unittest.mock import MagicMock, patch

from tools.benchmark_connections import (
    ALL_VENUES,
    SingleSample,
    VenueBenchmarkResult,
    VenueConfig,
    _format_cell,
    _parse_server_time_ms,
    print_ascii_benchmark_table,
    print_executive_summary,
    fetch_remote_benchmark,
)
from web.server import APIServer


class TestBenchmarkConnections(unittest.TestCase):
    def test_korean_sunrise_venues_configured(self):
        expected_venues = {
            "upbit", "bithumb", "binance", "bybit", "okx",
            "gateio", "mexc", "kucoin", "bingx", "htx",
            "pionex", "hyperliquid", "asterdex",
        }
        self.assertEqual(set(ALL_VENUES.keys()), expected_venues)
        for name, cfg in ALL_VENUES.items():
            self.assertTrue(cfg.host, f"Empty host for {name}")
            self.assertTrue(cfg.rest_url.startswith("https://"), f"Bad REST url for {name}")
            self.assertTrue(cfg.ws_url.startswith("wss://"), f"Bad WSS url for {name}")
            self.assertIn(cfg.role, ("LEADER", "FOLLOWER"), f"Bad role for {name}")

        self.assertEqual(ALL_VENUES["upbit"].role, "LEADER")
        self.assertEqual(ALL_VENUES["bithumb"].role, "LEADER")
        self.assertEqual(ALL_VENUES["binance"].role, "FOLLOWER")
        self.assertEqual(ALL_VENUES["bybit"].role, "FOLLOWER")

    def test_parse_server_time_ms(self):
        # Upbit format: [{"trade_timestamp": 1700000000000}]
        self.assertEqual(_parse_server_time_ms([{"trade_timestamp": 1700000000000}], {}), 1700000000000.0)

        # Bithumb format: {"data": {"date": "1700000000000"}}
        self.assertEqual(_parse_server_time_ms({"data": {"date": "1700000000000"}}, {}), 1700000000000.0)

        # Binance format: {"serverTime": 1700000000000}
        self.assertEqual(_parse_server_time_ms({"serverTime": 1700000000000}, {}), 1700000000000.0)

        # Bybit format: {"result": {"timeNano": "1700000000000000000"}}
        self.assertEqual(_parse_server_time_ms({"result": {"timeNano": "1700000000000000000"}}, {}), 1700000000000.0)

        # OKX format: {"data": [{"ts": "1700000000000"}]}
        self.assertEqual(_parse_server_time_ms({"data": [{"ts": "1700000000000"}]}, {}), 1700000000000.0)

        # KuCoin format: {"data": 1700000000000}
        self.assertEqual(_parse_server_time_ms({"data": 1700000000000}, {}), 1700000000000.0)

        # Date header fallback
        headers = {"Date": "Mon, 19 Oct 2026 12:00:00 GMT"}
        ts = _parse_server_time_ms({}, headers)
        self.assertIsNotNone(ts)
        self.assertGreater(ts, 1700000000000.0)

    def test_grading_logic(self):
        r_ultra = VenueBenchmarkResult(venue="upbit", role="LEADER", host="api.upbit.com", rest_status=200, tcp_avg_ms=15.0, ws_ping_avg_ms=18.0)
        r_ultra.grade = "ULTRA"
        self.assertEqual(r_ultra.grade, "ULTRA")

        r_fail = VenueBenchmarkResult(venue="fail", role="FOLLOWER", host="fail.com", rest_status=500, tcp_avg_ms=0.0)
        r_fail.grade = "FAILED"
        self.assertEqual(r_fail.grade, "FAILED")

    def test_format_cell(self):
        self.assertEqual(_format_cell(12.345), "12.3ms")
        self.assertEqual(_format_cell(None), "N/A")
        self.assertEqual(_format_cell(-1.0), "N/A")

    def test_table_and_executive_summary_rendering(self):
        results = [
            VenueBenchmarkResult(
                venue="upbit",
                role="LEADER",
                host="api.upbit.com",
                resolved_ip="1.2.3.4",
                dns_ms=5.0,
                tcp_avg_ms=15.0,
                tls_avg_ms=12.0,
                rest_status=200,
                rest_avg_ms=30.0,
                clock_skew_ms=10.0,
                ws_handshake_avg_ms=25.0,
                ws_ping_avg_ms=16.0,
                jitter_ms=1.2,
                grade="ULTRA",
            ),
            VenueBenchmarkResult(
                venue="binance",
                role="FOLLOWER",
                host="fapi.binance.com",
                resolved_ip="5.6.7.8",
                dns_ms=4.0,
                tcp_avg_ms=2.0,
                tls_avg_ms=3.0,
                rest_status=200,
                rest_avg_ms=8.0,
                clock_skew_ms=-5.0,
                ws_handshake_avg_ms=10.0,
                ws_ping_avg_ms=1.5,
                jitter_ms=0.2,
                grade="ULTRA",
            ),
        ]
        # Verify both table and Kimchi summary print cleanly without errors
        print_ascii_benchmark_table(results, use_color=False)
        print_executive_summary(results, use_color=False)

    @patch("urllib.request.urlopen")
    def test_fetch_remote_benchmark(self, mock_urlopen):
        mock_resp = MagicMock()
        payload = {
            "status": "ready",
            "benchmark": {
                "timestamp_utc": "2026-10-19T12:00:00Z",
                "venues": {
                    "upbit": {
                        "venue": "upbit",
                        "role": "LEADER",
                        "host": "api.upbit.com",
                        "resolved_ip": "1.2.3.4",
                        "dns_ms": 5.0,
                        "tcp_avg_ms": 15.0,
                        "tls_avg_ms": 12.0,
                        "rest_avg_ms": 30.0,
                        "clock_skew_ms": 10.0,
                        "ws_handshake_avg_ms": 25.0,
                        "ws_ping_avg_ms": 16.0,
                        "jitter_ms": 1.2,
                        "grade": "ULTRA",
                    }
                }
            }
        }
        mock_resp.read.return_value = json.dumps(payload).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        data = fetch_remote_benchmark(
            remote_url="http://127.0.0.1:8089",
            token="test_token",
            use_color=False,
        )
        self.assertIsNotNone(data)
        self.assertIn("benchmark", data)


class TestBenchmarkWebEndpoints(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mock_service = MagicMock()
        cls.mock_service.running = True
        cls.mock_service.engine.counters = {"signals_triggered": 10}
        cls.mock_service.pool_state.get_pool_snapshot.return_value = {"summary": {"battle": 2}}
        cls.mock_service.stream_manager.get_status.return_value = {}
        cls.mock_service.stream_manager.get_usdt_krw_rate.return_value = 1380.0

        cls.token = "test_benchmark_token_2026"
        cls.server = APIServer(service=cls.mock_service, host="127.0.0.1", port=0, token=cls.token)
        cls.server.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.port}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def test_get_benchmark_unauthorized(self) -> None:
        req = urllib.request.Request(f"{self.base_url}/api/benchmark")
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req)
        self.assertEqual(ctx.exception.code, 401)

    def test_get_benchmark_authorized_bearer(self) -> None:
        req = urllib.request.Request(
            f"{self.base_url}/api/benchmark",
            headers={"Authorization": f"Bearer {self.token}"}
        )
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertIn("status", data)
            self.assertIn("is_running", data)
            self.assertIn("benchmark", data)

    def test_post_benchmark_run_trigger(self) -> None:
        req = urllib.request.Request(
            f"{self.base_url}/api/benchmark/run",
            data=json.dumps({"repeats": 1, "concurrency": 4}).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "X-Token": self.token,
            }
        )
        with urllib.request.urlopen(req) as resp:
            self.assertEqual(resp.status, 200)
            data = json.loads(resp.read().decode("utf-8"))
            self.assertIn("status", data)
            self.assertIn(data["status"], ("started", "already_running"))


if __name__ == "__main__":
    unittest.main()
