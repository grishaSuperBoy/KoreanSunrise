"""
screening/screener.py — Two-Stage Universe Screener and Validator for Korean Sunrise.
Stage 1: Fast 24h ticker, volume, and namesake price sanity filter.
Stage 2: Deep 1-minute kline correlation and Kimchi basis volatility screening.
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from config import EXCLUDED_COINS, TICKER_ALIASES
from screening.metrics import evaluate_pair_klines
from venues.base import create_http_session
from venues.binance import BinanceRestClient
from venues.bybit import BybitRestClient
from venues.upbit import UpbitRestClient

log = logging.getLogger("screening")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_OUTPUT_FILE = os.path.join(PROJECT_ROOT, "universe_live.json")

# Default Screening Thresholds
DEFAULT_MIN_VOL_USD = 100_000.0         # $100K 24h volume on Upbit & Perp
DEFAULT_MAX_PRICE_DIFF_PCT = 15.0       # Max 15% price diff (eliminates namesake tokens)
DEFAULT_MIN_CORR = 0.65                 # Min correlation of minute log-returns
DEFAULT_MIN_BOTH_TRADE = 0.15           # Min 15% of minutes traded on both sides
DEFAULT_MIN_DEV_BPS = 8.0               # Min 8.0 bps P90 spread volatility
DEFAULT_KLINE_LIMIT = 200               # 200 1-minute candles (~3.3 hours)


class UniverseScreener:
    """
    Two-stage quantitative screener that filters Upbit KRW alts and Perp followers.
    """

    def __init__(
        self,
        min_vol_usd: float = DEFAULT_MIN_VOL_USD,
        max_price_diff_pct: float = DEFAULT_MAX_PRICE_DIFF_PCT,
        min_corr: float = DEFAULT_MIN_CORR,
        min_both_trade: float = DEFAULT_MIN_BOTH_TRADE,
        min_dev_bps: float = DEFAULT_MIN_DEV_BPS,
        kline_limit: int = DEFAULT_KLINE_LIMIT,
        output_file: str = DEFAULT_OUTPUT_FILE,
        max_workers: int = 8,
    ) -> None:
        self.min_vol_usd = min_vol_usd
        self.max_price_diff_pct = max_price_diff_pct
        self.min_corr = min_corr
        self.min_both_trade = min_both_trade
        self.min_dev_bps = min_dev_bps
        self.kline_limit = kline_limit
        self.output_file = output_file
        self.max_workers = max_workers

        self.session = create_http_session()
        self.upbit = UpbitRestClient(self.session)
        self.binance = BinanceRestClient(self.session)
        self.bybit = BybitRestClient(self.session)

    def run(self) -> Dict[str, Any]:
        t0 = time.time()
        log.info("=== Starting Two-Stage Korean Sunrise Universe Screener ===")

        # ------------------------------------------------------------- Stage 1
        log.info("[Stage 1/2] Fetching 24h market tickers & exchange metadata...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            f_rate = ex.submit(self.upbit.fetch_usdt_krw_rate)
            f_up_mkts = ex.submit(self.upbit.fetch_krw_markets)
            f_bin_perps = ex.submit(self.binance.fetch_linear_perpetuals)
            f_byb_perps = ex.submit(self.bybit.fetch_linear_perpetuals)
            f_bin_ticks = ex.submit(self.binance.fetch_tickers_24h)
            f_byb_ticks = ex.submit(self.bybit.fetch_tickers_24h)

            usdt_krw_rate = f_rate.result()
            up_mkts = f_up_mkts.result()
            bin_perps = f_bin_perps.result()
            byb_perps = f_byb_perps.result()
            bin_ticks = f_bin_ticks.result()
            byb_ticks = f_byb_ticks.result()

        log.info(f"[Stage 1/2] USDT/KRW Rate: {usdt_krw_rate:.2f} | Upbit KRW Markets: {len(up_mkts)}")

        # Fetch Upbit 24h tickers in fast batches
        up_ticks = self.upbit.fetch_tickers_24h(list(up_mkts.values()), usdt_krw_rate)
        log.info(f"[Stage 1/2] Fetched 24h tickers for {len(up_ticks)} Upbit coins")

        # Invert perp mappings: base_coin -> (raw_sym, mult)
        bin_base_map: Dict[str, Tuple[str, float]] = {}
        for raw, (base, mult) in bin_perps.items():
            bin_base_map[base] = (raw, mult)

        byb_base_map: Dict[str, Tuple[str, float]] = {}
        for raw, (base, mult) in byb_perps.items():
            byb_base_map[base] = (raw, mult)

        # Stage 1 Matching and Fast Volume + Price Discrepancy Filter
        candidate_coins: Dict[str, Dict[str, Any]] = {}
        rejected_namesakes = 0
        rejected_volume = 0

        for coin, mkt_code in up_mkts.items():
            if coin in EXCLUDED_COINS:
                continue

            # Check matching aliases if coin not directly in perp base map
            matched_bin = bin_base_map.get(coin)
            if not matched_bin and coin in TICKER_ALIASES:
                matched_bin = bin_base_map.get(TICKER_ALIASES[coin])

            matched_byb = byb_base_map.get(coin)
            if not matched_byb and coin in TICKER_ALIASES:
                matched_byb = byb_base_map.get(TICKER_ALIASES[coin])

            if not matched_bin and not matched_byb:
                continue

            up_tick = up_ticks.get(coin)
            if not up_tick:
                continue

            up_vol = up_tick["vol_usd_24h"]
            up_px = up_tick["price_usd"]

            # Filter 1: Upbit volume
            if up_vol < self.min_vol_usd:
                rejected_volume += 1
                continue

            # Check venues that pass volume & price sanity
            valid_venues: List[str] = []
            bin_meta = None
            byb_meta = None

            if matched_bin:
                raw_b, mult_b = matched_bin
                t_b = bin_ticks.get(raw_b)
                if t_b and t_b["vol_24h"] >= self.min_vol_usd:
                    px_b = t_b["price"] / mult_b
                    diff_pct = abs(up_px - px_b) / max(up_px, px_b) * 100.0
                    if diff_pct <= self.max_price_diff_pct:
                        valid_venues.append("binance")
                        bin_meta = (raw_b, mult_b, px_b, t_b["vol_24h"])
                    else:
                        rejected_namesakes += 1
                        log.debug(f"[sanity] Rejected namesake {coin} on Binance (price diff {diff_pct:.1f}%)")

            if matched_byb:
                raw_by, mult_by = matched_byb
                t_by = byb_ticks.get(raw_by)
                if t_by and t_by["vol_24h"] >= self.min_vol_usd:
                    px_by = t_by["price"] / mult_by
                    diff_pct = abs(up_px - px_by) / max(up_px, px_by) * 100.0
                    if diff_pct <= self.max_price_diff_pct:
                        valid_venues.append("bybit")
                        byb_meta = (raw_by, mult_by, px_by, t_by["vol_24h"])
                    else:
                        rejected_namesakes += 1
                        log.debug(f"[sanity] Rejected namesake {coin} on Bybit (price diff {diff_pct:.1f}%)")

            if valid_venues:
                candidate_coins[coin] = {
                    "coin": coin,
                    "market": mkt_code,
                    "up_px": up_px,
                    "up_vol": up_vol,
                    "venues": valid_venues,
                    "bin_meta": bin_meta,
                    "byb_meta": byb_meta,
                }

        log.info(
            f"[Stage 1/2] Complete: {len(candidate_coins)} candidates survived "
            f"(Rejected: {rejected_volume} low volume, {rejected_namesakes} namesakes/price diffs)"
        )

        # ------------------------------------------------------------- Stage 2
        log.info(f"[Stage 2/2] Fetching {self.kline_limit} 1m candles for deep correlation screening...")

        def _evaluate_worker(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            c = item["coin"]
            mkt = item["market"]
            kl_up = self.upbit.fetch_klines_1m(mkt, count=self.kline_limit)
            if len(kl_up) < 30:
                return None

            best_res: Optional[Dict[str, Any]] = None
            confirmed_venues: List[str] = []

            if "binance" in item["venues"] and item["bin_meta"]:
                raw_b, mult_b, _, _ = item["bin_meta"]
                kl_b = self.binance.fetch_klines_1m(raw_b, limit=self.kline_limit, mult=mult_b)
                res_b = evaluate_pair_klines(
                    kl_up, kl_b, usdt_krw_rate,
                    min_corr=self.min_corr,
                    min_both_trade=self.min_both_trade,
                    min_dev_bps=self.min_dev_bps,
                    max_price_diff_pct=self.max_price_diff_pct,
                )
                if res_b["passed"] > 0:
                    confirmed_venues.append("binance")
                    best_res = res_b

            if "bybit" in item["venues"] and item["byb_meta"]:
                raw_by, mult_by, _, _ = item["byb_meta"]
                kl_by = self.bybit.fetch_klines_1m(raw_by, limit=self.kline_limit, mult=mult_by)
                res_by = evaluate_pair_klines(
                    kl_up, kl_by, usdt_krw_rate,
                    min_corr=self.min_corr,
                    min_both_trade=self.min_both_trade,
                    min_dev_bps=self.min_dev_bps,
                    max_price_diff_pct=self.max_price_diff_pct,
                )
                if res_by["passed"] > 0:
                    confirmed_venues.append("bybit")
                    if best_res is None or res_by["corr"] > best_res["corr"]:
                        best_res = res_by

            if confirmed_venues and best_res:
                return {
                    "coin": c,
                    "market": mkt,
                    "venues": confirmed_venues,
                    "metrics": best_res,
                    "bin_meta": item["bin_meta"],
                    "byb_meta": item["byb_meta"],
                }
            return None

        vetted_coins: List[Dict[str, Any]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = [ex.submit(_evaluate_worker, it) for it in candidate_coins.values()]
            for f in concurrent.futures.as_completed(futures):
                try:
                    res = f.result()
                    if res:
                        vetted_coins.append(res)
                except Exception as e:
                    log.debug(f"[screening] Worker error: {e}")

        # Sort by correlation and volume
        vetted_coins.sort(key=lambda x: x["metrics"]["corr"], reverse=True)

        elapsed = round(time.time() - t0, 2)
        log.info(
            f"[Stage 2/2] Complete in {elapsed}s: {len(vetted_coins)} coins qualified for live universe."
        )

        for i, q in enumerate(vetted_coins[:20], 1):
            m = q["metrics"]
            v_str = ",".join(q["venues"])
            log.info(
                f"  #{i:2d} {q['coin']:<10} | Corr: {m['corr']:>5.3f} | "
                f"Both: {m['both_trade']:>4.1%} | P90 Dev: {m['p90_dev_bps']:>5.1f} bps | "
                f"Diff: {m['price_diff_pct']:>4.1f}% | [{v_str}]"
            )

        # ------------------------------------------------------------- Persist
        final_doc = self._build_universe_doc(vetted_coins, usdt_krw_rate, elapsed)
        self._save_atomic(final_doc)

        return final_doc

    def _build_universe_doc(
        self,
        vetted_coins: List[Dict[str, Any]],
        usdt_krw_rate: float,
        elapsed_sec: float,
    ) -> Dict[str, Any]:
        coins_list = [c["coin"] for c in vetted_coins]
        upbit_krw_to_coin: Dict[str, str] = {c["market"]: c["coin"] for c in vetted_coins}
        binance_sym_map: Dict[str, Tuple[str, float]] = {}
        bybit_sym_map: Dict[str, Tuple[str, float]] = {}
        pairs_meta: Dict[str, Any] = {}

        for c in vetted_coins:
            coin = c["coin"]
            bm = c.get("bin_meta")
            bym = c.get("byb_meta")
            bin_raw = bm[0] if bm else None
            byb_raw = bym[0] if bym else None

            if "binance" in c["venues"] and bm:
                binance_sym_map[bm[0]] = (coin, bm[1])
            if "bybit" in c["venues"] and bym:
                bybit_sym_map[bym[0]] = (coin, bym[1])

            pairs_meta[coin] = {
                "coin": coin,
                "upbit_symbol": c["market"],
                "binance_symbol": bin_raw,
                "bybit_symbol": byb_raw,
                "venues": c["venues"],
                "corr": c["metrics"]["corr"],
                "p90_dev_bps": c["metrics"]["p90_dev_bps"],
                "both_trade": c["metrics"]["both_trade"],
            }

        return {
            "_meta": {
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "updated_ts": round(time.time(), 3),
                "usdt_krw_rate": round(usdt_krw_rate, 2),
                "coins_count": len(coins_list),
                "binance_count": len(binance_sym_map),
                "bybit_count": len(bybit_sym_map),
                "screening_elapsed_sec": elapsed_sec,
            },
            "coins": coins_list,
            "upbit_krw_to_coin": upbit_krw_to_coin,
            "binance_sym_map": binance_sym_map,
            "bybit_sym_map": bybit_sym_map,
            "pairs": pairs_meta,
        }

    def _save_atomic(self, doc: Dict[str, Any]) -> None:
        tmp = self.output_file + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(doc, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.output_file)
            log.info(f"Saved verified live universe to {self.output_file}")
        except Exception as e:
            log.error(f"Failed to write live universe to {self.output_file}: {e}")
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
