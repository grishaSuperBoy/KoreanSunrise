"""
tests/test_bithumb_lead_lag.py — Unit tests for Bithumb Co-Leader integration:
1. Bithumb anti-ghosting blocks false quote pullbacks.
2. Bithumb depth drain sweep triggers lead-lag signal to follower (Binance/Bybit).
3. Cross-Korean confirmation and spread telemetry (Upbit + Bithumb).
"""
import time
import unittest

from models import OrderBookDepth5
from korean_engine import KoreanEngine


def make_snap(exchange: str, symbol: str, bids, asks, ts: float, raw_symbol: str = "") -> OrderBookDepth5:
    return OrderBookDepth5(
        exchange=exchange,
        symbol=symbol,
        raw_symbol=raw_symbol or f"{symbol}_USDT",
        ts=ts,
        exchange_ts=int(ts * 1000),
        bids=bids,
        asks=asks,
    )


class TestBithumbLeadLagIntegration(unittest.TestCase):

    def setUp(self):
        self.emitted = []
        self.engine = KoreanEngine(
            emit_callback=lambda s: self.emitted.append(s),
            require_trade_confirmation=True,
            drain_threshold=0.30,
        )

    def test_bithumb_anti_ghosting_and_real_sweep(self):
        """Verifies that Bithumb blocks unbacked cancels and fires signals on verified sweeps."""
        coin = "BTC"
        t0 = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance", leader_ex="bithumb")
        st.warmup_samples = 1
        st.dev_window.append(0.0)
        st.n_obs = 10
        st.baseline_bps = 0.0

        # Follower orderbook (calm, ready to be sniped)
        f_snap = make_snap(
            "binance", coin,
            bids=[(99000.0, 1.0)],
            asks=[(99100.0, 1.0), (99200.0, 1.0), (99300.0, 1.0)],
            ts=t0,
            raw_symbol="BTCUSDT",
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = t0

        # Bithumb baseline book with heavy $10,000 ask plate at 105,000
        bithumb_t0 = make_snap(
            "bithumb", coin,
            bids=[(104900.0, 1.0)],
            asks=[(105000.0, 0.0952), (105500.0, 0.5)],
            ts=t0 - 0.1,
            raw_symbol="BTC_KRW",
        )
        self.engine._books[("bithumb", coin)] = bithumb_t0
        self.engine._recv_ts[("bithumb", coin)] = t0 - 0.1
        self.engine._update_bithumb_norm(coin, bithumb_t0, t0 - 0.1)

        # 1. MM cancels the 105,000 ask plate with ZERO trades
        t_cancel = t0 + 0.05
        bithumb_cancel = make_snap(
            "bithumb", coin,
            bids=[(104900.0, 1.0)],
            asks=[(105500.0, 0.5)],
            ts=t_cancel,
            raw_symbol="BTC_KRW",
        )
        self.engine.on_book_update(bithumb_cancel)

        # Blocked by anti-ghosting guard!
        self.assertEqual(self.engine.counters["cancels_blocked"], 1)
        self.assertIsNone(st.open_signal)
        self.assertEqual(self.engine.counters["signals_triggered"], 0)

        # 2. Real aggressive buyer sweeps the 105,000 ask plate with $10,000 taker buy
        st.baseline_bps = 0.0
        t_sweep = t0 + 0.15
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = t_sweep
        self.engine._books[("bithumb", coin)] = bithumb_t0
        self.engine._recv_ts[("bithumb", coin)] = t_sweep - 0.05

        self.engine.on_bithumb_trade(coin, trade_usd=10000.0, is_taker_buy=True, ts=t_sweep)
        self.assertEqual(self.engine.counters["trades_bithumb"], 1)

        bithumb_swept = make_snap(
            "bithumb", coin,
            bids=[(104900.0, 1.0)],
            asks=[(105500.0, 0.5)],
            ts=t_sweep,
            raw_symbol="BTC_KRW",
        )
        self.engine.on_book_update(bithumb_swept)

        # Confirmed! Signal fires with Bithumb as leader
        self.assertEqual(self.engine.counters["cancels_blocked"], 1)  # unchanged
        self.assertEqual(self.engine.counters["signals_triggered"], 1)
        self.assertIsNotNone(st.open_signal)
        self.assertEqual(st.open_signal.leader_exchange, "bithumb")
        self.assertEqual(st.open_signal.follower_exchange, "binance")
        self.assertEqual(st.open_signal.leader_recent_buys_usd, 10000.0)

    def test_cross_korean_confirmation_telemetry(self):
        """Verifies that Korean co-leader spread and confirmation are tracked on Upbit sweep."""
        coin = "BTC"
        t0 = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance", leader_ex="upbit")
        st.warmup_samples = 1
        st.dev_window.append(0.0)
        st.n_obs = 10
        st.baseline_bps = 0.0

        # Follower orderbook
        f_snap = make_snap(
            "binance", coin,
            bids=[(99000.0, 1.0)],
            asks=[(99100.0, 1.0), (99200.0, 1.0)],
            ts=t0,
            raw_symbol="BTCUSDT",
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = t0

        # Bithumb already higher (co-leader confirming)
        b_snap = make_snap(
            "bithumb", coin,
            bids=[(105300.0, 1.0)],
            asks=[(105400.0, 1.0)],
            ts=t0,
            raw_symbol="BTC_KRW",
        )
        self.engine._books[("bithumb", coin)] = b_snap
        self.engine._recv_ts[("bithumb", coin)] = t0

        # Upbit baseline book
        upbit_t0 = make_snap(
            "upbit", coin,
            bids=[(104900.0, 1.0)],
            asks=[(105000.0, 0.0952), (105500.0, 0.5)],
            ts=t0 - 0.1,
            raw_symbol="KRW-BTC",
        )
        self.engine._books[("upbit", coin)] = upbit_t0
        self.engine._recv_ts[("upbit", coin)] = t0 - 0.1
        self.engine._update_upbit_norm(coin, upbit_t0, t0 - 0.1)

        # Real Upbit taker trade arrives
        t_sweep = t0 + 0.1
        self.engine.on_upbit_trade(coin, trade_usd=10000.0, is_taker_buy=True, ts=t_sweep)

        # Upbit orderbook sweep arrives
        upbit_swept = make_snap(
            "upbit", coin,
            bids=[(104900.0, 1.0)],
            asks=[(105500.0, 0.5)],
            ts=t_sweep,
            raw_symbol="KRW-BTC",
        )
        self.engine.on_book_update(upbit_swept)

        sig = st.open_signal
        self.assertIsNotNone(sig)
        self.assertEqual(sig.leader_exchange, "upbit")
        self.assertTrue(sig.korean_confirmed)
        self.assertNotEqual(sig.korean_spread_bps, 0.0)

    def test_unconfirmed_korean_signal_telemetry_and_gate(self):
        """Verifies that unconfirmed Upbit sweep records telemetry, and is blocked when gate is enabled."""
        coin = "ETH"
        t0 = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance", leader_ex="upbit")
        st.warmup_samples = 1
        st.dev_window.append(0.0)
        st.n_obs = 10
        st.baseline_bps = 0.0

        # Follower orderbook
        f_snap = make_snap(
            "binance", coin,
            bids=[(3000.0, 10.0)],
            asks=[(3005.0, 10.0)],
            ts=t0,
            raw_symbol="ETHUSDT",
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = t0

        # Bithumb is LAGGING / flat at 3100 (while Upbit sweeps to 3300)
        b_snap = make_snap(
            "bithumb", coin,
            bids=[(3090.0, 5.0)],
            asks=[(3100.0, 5.0)],
            ts=t0,
            raw_symbol="ETH_KRW",
        )
        self.engine._books[("bithumb", coin)] = b_snap
        self.engine._recv_ts[("bithumb", coin)] = t0

        # Upbit baseline book
        upbit_t0 = make_snap(
            "upbit", coin,
            bids=[(3100.0, 5.0)],
            asks=[(3110.0, 1.0), (3300.0, 10.0)],
            ts=t0 - 0.1,
            raw_symbol="KRW-ETH",
        )
        self.engine._books[("upbit", coin)] = upbit_t0
        self.engine._recv_ts[("upbit", coin)] = t0 - 0.1
        self.engine._update_upbit_norm(coin, upbit_t0, t0 - 0.1)

        # Real Upbit taker trade
        t_sweep = t0 + 0.1
        self.engine.on_upbit_trade(coin, trade_usd=10000.0, is_taker_buy=True, ts=t_sweep)

        # 1. Telemetry mode (require_korean_confirmation=False): Records signal with korean_confirmed=False
        self.engine.require_korean_confirmation = False
        upbit_swept = make_snap(
            "upbit", coin,
            bids=[(3100.0, 5.0)],
            asks=[(3300.0, 10.0)],
            ts=t_sweep,
            raw_symbol="KRW-ETH",
        )
        self.engine.on_book_update(upbit_swept)

        sig = st.open_signal
        self.assertIsNotNone(sig)
        self.assertTrue(sig.korean_co_listed)
        self.assertFalse(sig.korean_confirmed)
        self.assertLess(sig.korean_spread_bps, 0.0)

        # 2. Strict / Battle mode: Gate blocks unconfirmed signal
        st.open_signal = None
        self.engine.require_korean_confirmation = True
        import config as C
        C.STRICT_KOREAN_GATE = True
        try:
            self.engine._books[("upbit", coin)] = upbit_t0
            self.engine.on_upbit_trade(coin, trade_usd=10000.0, is_taker_buy=True, ts=t_sweep + 0.1)
            upbit_swept2 = make_snap(
                "upbit", coin,
                bids=[(3100.0, 5.0)],
                asks=[(3300.0, 10.0)],
                ts=t_sweep + 0.1,
                raw_symbol="KRW-ETH",
            )
            self.engine.on_book_update(upbit_swept2)
            self.assertIsNone(st.open_signal)
            self.assertEqual(self.engine.counters["korean_unconfirmed_blocked"], 1)
        finally:
            C.STRICT_KOREAN_GATE = False

    def test_fade_aborted_when_korean_confirmed(self):
        """Verifies that Strategy 2 (Fade) aborts shorting if both Upbit and Bithumb confirm the pump."""
        # 1. When korean_confirmed=True (both Korean exchanges pump), fade must return False
        is_fake, reason = self.engine.strat_fade.is_fakeout_pattern(
            is_cancel_blocked=False,
            swept_usd=1500.0,
            upbit_spread_bps=50.0,
            korean_confirmed=True,
        )
        self.assertFalse(is_fake)
        self.assertEqual(reason, "")

        # 2. When korean_confirmed=False (only Upbit flickers, Bithumb silent), fade is permitted
        is_fake, reason = self.engine.strat_fade.is_fakeout_pattern(
            is_cancel_blocked=False,
            swept_usd=1500.0,
            upbit_spread_bps=50.0,
            korean_confirmed=False,
        )
        self.assertTrue(is_fake)
        self.assertEqual(reason, "micro_sweep_exhaustion")


if __name__ == "__main__":
    unittest.main()
