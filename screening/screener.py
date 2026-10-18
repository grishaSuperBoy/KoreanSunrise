"""
screening/screener.py — Multi-Venue Two-Stage Universe Screener and Validator for Korean Sunrise.
Stage 1: Fast 24h ticker, volume, and namesake price sanity filter across all venues.
Stage 2: Deep 1-minute kline correlation and Kimchi basis volatility screening.
Supports all 9 major perpetual follower exchanges:
  Binance, Bybit, OKX, Gate.io, MEXC, KuCoin, BingX, HTX, Pionex.
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from config import EXCLUDED_COINS, TICKER_ALIASES
from screening.metrics import evaluate_pair_klines
from venues.base import create_http_session
from venues.asterdex import AsterdexRestClient
from venues.bingx import BingXRestClient
from venues.binance import BinanceRestClient
from venues.bybit import BybitRestClient
from venues.gateio import GateioRestClient
from venues.htx import HTXRestClient
from venues.hyperliquid import HyperliquidRestClient
from venues.kucoin import KucoinRestClient
from venues.mexc import MEXCRestClient
from venues.okx import OKXRestClient
from venues.pionex import PionexRestClient
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
DEFAULT_KLINE_LIMIT = 200               # 200 1-minute candles (~3.3h, uniform across all follower venues)

CLIENT_FACTORIES: Dict[str, Any] = {
    "binance": BinanceRestClient,
    "bybit": BybitRestClient,
    "okx": OKXRestClient,
    "gateio": GateioRestClient,
    "mexc": MEXCRestClient,
    "kucoin": KucoinRestClient,
    "bingx": BingXRestClient,
    "htx": HTXRestClient,
    "pionex": PionexRestClient,
    "asterdex": AsterdexRestClient,
    "hyperliquid": HyperliquidRestClient,
}


class UniverseScreener:
    """
    Two-stage quantitative screener that filters Upbit KRW spot leader alts
    against multiple perpetual follower exchanges.
    """

    def __init__(
        self,
        venues: Optional[List[str]] = None,
        coins: Optional[List[str]] = None,
        min_vol_usd: float = DEFAULT_MIN_VOL_USD,
        max_price_diff_pct: float = DEFAULT_MAX_PRICE_DIFF_PCT,
        min_corr: float = DEFAULT_MIN_CORR,
        min_both_trade: float = DEFAULT_MIN_BOTH_TRADE,
        min_dev_bps: float = DEFAULT_MIN_DEV_BPS,
        kline_limit: int = DEFAULT_KLINE_LIMIT,
        output_file: str = DEFAULT_OUTPUT_FILE,
        max_workers: int = 10,
    ) -> None:
        self.target_venues = [v.lower() for v in (venues or list(CLIENT_FACTORIES.keys())) if v.lower() in CLIENT_FACTORIES]
        self.target_coins = [c.upper() for c in coins] if coins else None
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
        self.clients = {v: CLIENT_FACTORIES[v](self.session) for v in self.target_venues}

    def run(self) -> Dict[str, Any]:
        t0 = time.time()
        log.info(f"=== Starting Multi-Venue Screener ({len(self.target_venues)} follower venues) ===")
        log.info(f"Active follower venues: {', '.join(self.target_venues)}")

        # ------------------------------------------------------------- Stage 1
        log.info("[Stage 1/2] Fetching 24h market tickers & exchange metadata across venues...")
        
        # Parallel fetch for all venue instruments and 24h tickers
        venue_perps: Dict[str, Dict[str, Tuple[str, float]]] = {}
        venue_ticks: Dict[str, Dict[str, Dict[str, Any]]] = {}

        def _fetch_venue_metadata(venue: str):
            client = self.clients[venue]
            perps = client.fetch_linear_perpetuals()
            ticks = client.fetch_tickers_24h()
            return venue, perps, ticks

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(4, len(self.target_venues) + 1)) as ex:
            f_rate = ex.submit(self.upbit.fetch_usdt_krw_rate)
            f_up_mkts = ex.submit(self.upbit.fetch_krw_markets)
            v_futures = [ex.submit(_fetch_venue_metadata, v) for v in self.target_venues]

            usdt_krw_rate = f_rate.result()
            up_mkts = f_up_mkts.result()
            for fut in concurrent.futures.as_completed(v_futures):
                try:
                    v, p, tk = fut.result()
                    venue_perps[v] = p
                    venue_ticks[v] = tk
                    log.info(f"  • {v.upper():<8}: {len(p)} contracts | {len(tk)} 24h tickers")
                except Exception as e:
                    log.warning(f"  • Venue fetch failed: {e}")

        log.info(f"[Stage 1/2] USDT/KRW Rate: {usdt_krw_rate:.2f} | Upbit KRW Markets: {len(up_mkts)}")

        # Batch fetch Upbit 24h tickers
        up_ticks = self.upbit.fetch_tickers_24h(list(up_mkts.values()), usdt_krw_rate)
        log.info(f"[Stage 1/2] Upbit 24h tickers fetched: {len(up_ticks)}")

        # Build reverse base map for each venue: canonical_base -> (raw_sym, mult, c_size)
        venue_base_maps: Dict[str, Dict[str, Tuple[str, float, float]]] = {}
        for v, perps in venue_perps.items():
            b_map: Dict[str, Tuple[str, float, float]] = {}
            for raw, val in perps.items():
                base = val[0]
                mult = float(val[1]) if len(val) > 1 else 1.0
                c_size = float(val[2]) if len(val) > 2 else 1.0
                b_map[base] = (raw, mult, c_size)
            venue_base_maps[v] = b_map

        # Stage 1 Matching and Fast Volume + Namesake Price Sanity Filter
        candidate_coins: Dict[str, Dict[str, Any]] = {}
        rejected_namesakes = 0
        rejected_volume = 0

        for coin, mkt_code in up_mkts.items():
            if self.target_coins and coin not in self.target_coins:
                continue
            if coin in EXCLUDED_COINS:
                continue

            up_tick = up_ticks.get(coin)
            if not up_tick:
                continue

            up_vol = up_tick["vol_usd_24h"]
            up_px = up_tick["price_usd"]

            if up_vol < self.min_vol_usd:
                rejected_volume += 1
                continue

            matched_venues_meta: Dict[str, Tuple[str, float, float, float, float]] = {}

            for v in self.target_venues:
                b_map = venue_base_maps.get(v, {})
                matched = b_map.get(coin)
                if not matched and coin in TICKER_ALIASES:
                    matched = b_map.get(TICKER_ALIASES[coin])

                if not matched:
                    continue

                raw_sym, mult, c_size = matched
                t_info = venue_ticks.get(v, {}).get(raw_sym)
                if not t_info:
                    continue

                vol_v = t_info.get("vol_24h", 0.0)
                px_v = (t_info.get("price", 0.0) or 0.0) / mult

                if vol_v < self.min_vol_usd or px_v <= 0:
                    continue

                diff_pct = abs(up_px - px_v) / max(up_px, px_v) * 100.0
                if diff_pct <= self.max_price_diff_pct:
                    matched_venues_meta[v] = (raw_sym, mult, px_v, vol_v, c_size)
                else:
                    rejected_namesakes += 1
                    log.debug(f"[sanity] Rejected namesake {coin} on {v} (diff {diff_pct:.1f}%)")

            if matched_venues_meta:
                candidate_coins[coin] = {
                    "coin": coin,
                    "market": mkt_code,
                    "up_px": up_px,
                    "up_vol": up_vol,
                    "matched_venues": matched_venues_meta,
                }

        log.info(
            f"[Stage 1/2] Complete: {len(candidate_coins)} candidate coins survived "
            f"(Rejected: {rejected_volume} low volume, {rejected_namesakes} namesakes/price diffs)"
        )

        # ------------------------------------------------------------- Stage 2
        log.info(f"[Stage 2/2] Fetching {self.kline_limit} 1m candles for deep correlation screening...")

        def _evaluate_coin_worker(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            c = item["coin"]
            mkt = item["market"]
            kl_up = self.upbit.fetch_klines_1m(mkt, count=self.kline_limit)
            if len(kl_up) < 30:
                return None

            confirmed_venues: Dict[str, Dict[str, Any]] = {}
            best_res: Optional[Dict[str, Any]] = None

            for v, (raw_sym, mult, _, _, c_size) in item["matched_venues"].items():
                client = self.clients.get(v)
                if not client:
                    continue
                kl_v = client.fetch_klines_1m(raw_sym, limit=self.kline_limit, mult=mult)
                if len(kl_v) < 30:
                    continue

                res = evaluate_pair_klines(
                    kl_up, kl_v, usdt_krw_rate,
                    min_corr=self.min_corr,
                    min_both_trade=self.min_both_trade,
                    min_dev_bps=self.min_dev_bps,
                    max_price_diff_pct=self.max_price_diff_pct,
                )

                if res["passed"] > 0:
                    confirmed_venues[v] = {
                        "raw_sym": raw_sym,
                        "mult": mult,
                        "c_size": c_size,
                        "metrics": res,
                    }
                    if best_res is None or res["corr"] > best_res["corr"]:
                        best_res = res

            if confirmed_venues and best_res:
                return {
                    "coin": c,
                    "market": mkt,
                    "venues": sorted(confirmed_venues.keys()),
                    "venue_details": confirmed_venues,
                    "best_metrics": best_res,
                }
            return None

        vetted_coins: List[Dict[str, Any]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = [ex.submit(_evaluate_coin_worker, it) for it in candidate_coins.values()]
            for f in concurrent.futures.as_completed(futures):
                try:
                    res = f.result()
                    if res:
                        vetted_coins.append(res)
                except Exception as e:
                    log.debug(f"[screening] Worker error: {e}")

        # Sort by best correlation
        vetted_coins.sort(key=lambda x: x["best_metrics"]["corr"], reverse=True)

        elapsed = round(time.time() - t0, 2)
        log.info(
            f"[Stage 2/2] Complete in {elapsed}s: {len(vetted_coins)} coins qualified across all venues."
        )

        for i, q in enumerate(vetted_coins[:25], 1):
            m = q["best_metrics"]
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

        # Build sym_map per venue
        sym_maps: Dict[str, Dict[str, Tuple[str, float, float]]] = {v: {} for v in self.target_venues}
        pairs_meta: Dict[str, Any] = {}

        for c in vetted_coins:
            coin = c["coin"]
            v_details = c.get("venue_details", {})
            v_list = c.get("venues", [])

            for v in v_list:
                dt = v_details.get(v)
                if dt:
                    raw_sym = dt["raw_sym"]
                    mult = dt["mult"]
                    c_size = dt.get("c_size", 1.0)
                    sym_maps[v][raw_sym] = (coin, mult, c_size)

            pairs_meta[coin] = {
                "coin": coin,
                "upbit_symbol": c["market"],
                "venues": v_list,
                "best_corr": c["best_metrics"]["corr"],
                "best_p90_dev_bps": c["best_metrics"]["p90_dev_bps"],
                "venue_symbols": {v: v_details[v]["raw_sym"] for v in v_list if v in v_details},
            }

        doc = {
            "_meta": {
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "updated_ts": round(time.time(), 3),
                "usdt_krw_rate": round(usdt_krw_rate, 2),
                "coins_count": len(coins_list),
                "active_venues": self.target_venues,
                "screening_elapsed_sec": elapsed_sec,
            },
            "coins": coins_list,
            "upbit_krw_to_coin": upbit_krw_to_coin,
            "pairs": pairs_meta,
        }

        # Store individual {venue}_sym_map entries for backward compatibility and direct lookup
        for v in self.target_venues:
            doc[f"{v}_sym_map"] = sym_maps[v]
            doc["_meta"][f"{v}_count"] = len(sym_maps[v])

        return doc

    def _save_atomic(self, doc: Dict[str, Any]) -> None:
        tmp = self.output_file + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(doc, f, indent=2, ensure_ascii=False)
            os.replace(tmp, self.output_file)
            log.info(f"Saved verified multi-venue universe to {self.output_file}")
        except Exception as e:
            log.error(f"Failed to write live universe to {self.output_file}: {e}")
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass
