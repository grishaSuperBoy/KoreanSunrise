"""
tests/test_bithumb_venue.py — Comprehensive unit tests for Bithumb REST and WebSocket stream.
"""
import time
import unittest
from unittest.mock import MagicMock, patch

from models import OrderBookDepth5
from venues.bithumb import BithumbRestClient, BithumbSpotStream


class TestBithumbVenue(unittest.TestCase):

    def test_bithumb_rest_fetch_krw_markets(self):
        client = BithumbRestClient()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "0000",
            "data": {
                "BTC": {"closing_price": "100000000"},
                "ETH": {"closing_price": "3000000"},
                "date": "1729450000000",
            }
        }
        with patch.object(client.session, "get", return_value=mock_resp):
            markets = client.fetch_krw_markets()
            self.assertEqual(len(markets), 2)
            self.assertEqual(markets["BTC"], "BTC_KRW")
            self.assertEqual(markets["ETH"], "ETH_KRW")
            self.assertNotIn("date", markets)

    def test_bithumb_rest_fetch_tickers_24h(self):
        client = BithumbRestClient()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "status": "0000",
            "data": {
                "BTC": {
                    "closing_price": "140000000",
                    "acc_trade_value_24H": "28000000000",
                },
                "date": "1729450000000",
            }
        }
        with patch.object(client.session, "get", return_value=mock_resp):
            tickers = client.fetch_tickers_24h(usdt_krw_rate=1400.0)
            self.assertIn("BTC", tickers)
            self.assertAlmostEqual(tickers["BTC"]["price"], 100000.0)
            self.assertAlmostEqual(tickers["BTC"]["vol_24h"], 20000000.0)

    def test_bithumb_stream_orderbook_depth_and_conversion(self):
        snaps = []
        stream = BithumbSpotStream(
            coins=["BTC"],
            callback=lambda s: snaps.append(s),
            initial_usdt_krw_rate=1400.0,
        )

        now = time.time()
        # Feed initial orderbook
        msg = {
            "type": "orderbookdepth",
            "content": {
                "list": [
                    {"orderType": "bid", "price": "140000000", "quantity": "1.5", "symbol": "BTC_KRW"},
                    {"orderType": "bid", "price": "139000000", "quantity": "2.0", "symbol": "BTC_KRW"},
                    {"orderType": "ask", "price": "141000000", "quantity": "1.0", "symbol": "BTC_KRW"},
                    {"orderType": "ask", "price": "142000000", "quantity": "3.0", "symbol": "BTC_KRW"},
                ],
                "datetime": "1729450000000000",
            }
        }

        stream._handle_depth_update(msg, now)
        self.assertEqual(len(snaps), 1)
        snap: OrderBookDepth5 = snaps[0]
        self.assertEqual(snap.exchange, "bithumb")
        self.assertEqual(snap.symbol, "BTC")
        self.assertEqual(snap.raw_symbol, "BTC_KRW")

        # Bids converted to USD: 140,000,000 / 1400 = 100,000.0
        self.assertAlmostEqual(snap.best_bid, 100000.0)
        # Asks converted to USD: 141,000,000 / 1400 = 100,714.28571429
        self.assertAlmostEqual(snap.best_ask, 100714.28571429)

        # Update orderbook: cancel best bid (qty = 0)
        cancel_msg = {
            "type": "orderbookdepth",
            "content": {
                "list": [
                    {"orderType": "bid", "price": "140000000", "quantity": "0", "symbol": "BTC_KRW"},
                ],
                "datetime": "1729450000100000",
            }
        }
        stream._handle_depth_update(cancel_msg, now + 0.1)
        self.assertEqual(len(snaps), 2)
        snap2 = snaps[1]
        # New best bid is 139,000,000 / 1400 = 99,285.71428571
        self.assertAlmostEqual(snap2.best_bid, 99285.71428571)

    def test_bithumb_stream_trades(self):
        trades = []
        stream = BithumbSpotStream(
            coins=["BTC"],
            callback=lambda s: None,
            trade_callback=lambda coin, vol, is_buy, ts: trades.append((coin, vol, is_buy, ts)),
            initial_usdt_krw_rate=1400.0,
        )

        now = time.time()
        trade_msg = {
            "type": "transaction",
            "content": {
                "list": [
                    {
                        "symbol": "BTC_KRW",
                        "contPrice": "140000000",
                        "contQty": "0.5",
                        "buySellGb": "2",  # buy
                    },
                    {
                        "symbol": "BTC_KRW",
                        "contPrice": "139500000",
                        "contQty": "1.0",
                        "buySellGb": "1",  # sell
                    }
                ]
            }
        }
        stream._handle_trade_update(trade_msg, now)
        self.assertEqual(len(trades), 2)
        coin1, vol1, is_buy1, ts1 = trades[0]
        self.assertEqual(coin1, "BTC")
        self.assertTrue(is_buy1)
        self.assertAlmostEqual(vol1, 50000.0)
        self.assertAlmostEqual(ts1, now)

        coin2, vol2, is_buy2, ts2 = trades[1]
        self.assertEqual(coin2, "BTC")
        self.assertFalse(is_buy2)
        self.assertAlmostEqual(vol2, 139500000.0 / 1400.0, places=2)


if __name__ == "__main__":
    unittest.main()
