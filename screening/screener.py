"""
screening/screener.py — Multi-Venue Two-Stage Universe Screener and Validator for Korean Sunrise.
Stage 1: Fast 24h ticker, volume, and namesake price sanity filter across Upbit, Bithumb & follower perps.
Stage 2: Deep 1-minute kline correlation (up to 1440 candles = 24h) and Kimchi basis volatility screening.
Supports Dual-Korean confirmation (Upbit + Bithumb verification boost) and Bithumb-exclusive coins.
Follower venues: Binance, Bybit, OKX, Gate.io, MEXC, KuCoin, BingX, HTX, Pionex, AsterDex, Hyperliquid.
"""
from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from config import EXCLUDED_COINS, TICKER_ALIASES
from screening.metrics import evaluate_pair_klines
from venues.base import create_http_session
from venues.asterdex import AsterdexRestClient
from venues.bithumb import BithumbRestClient
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
DEFAULT_MIN_VOL_USD = 20_000.0          # $20K 24h volume on Korean spot & Perp (accessible for alts)
DEFAULT_MAX_PRICE_DIFF_PCT = 15.0       # Max 15% price diff (eliminates namesake tokens)
DEFAULT_MIN_CORR = 0.50                 # Min correlation of multi-scale log-returns
DEFAULT_MIN_BOTH_TRADE = 0.05           # Min 5% of minutes traded on both sides (handles slow alts)
DEFAULT_MIN_DEV_BPS = 6.0               # Min 6.0 bps P90 spread volatility
DEFAULT_KLINE_LIMIT = 200               # 200 1-minute candles baseline (CLI defaults to 1440 = 24h)

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
    Two-stage quantitative screener that filters Korean spot leader alts (Upbit & Bithumb)
    against multiple perpetual follower exchanges, applying Dual-Korean verification.
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
        self.bithumb = BithumbRestClient(self.session)
        self.clients = {v: CLIENT_FACTORIES[v](self.session) for v in self.target_venues}

    def run(self) -> Dict[str, Any]:
        t0 = time.time()
        log.info(f"=== Starting Multi-Venue Screener ({len(self.target_venues)} follower venues) ===")
        log.info(f"Spot Leaders: UPBIT + BITHUMB | Followers: {', '.join(self.target_venues)}")
        log.info(f"Target candle depth: {self.kline_limit} 1m candles ({round(self.kline_limit / 60, 1)}h window)")

        # ------------------------------------------------------------- Stage 1
        log.info("[Stage 1/2] Fetching 24h market tickers & exchange metadata across all venues...")
        
        venue_perps: Dict[str, Dict[str, Tuple[Any, ...]]] = {}
        venue_ticks: Dict[str, Dict[str, Dict[str, Any]]] = {}

        def _fetch_venue_metadata(venue: str):
            client = self.clients[venue]
            perps = client.fetch_linear_perpetuals()
            ticks = client.fetch_tickers_24h()
            return venue, perps, ticks

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(4, len(self.target_venues) + 3)) as ex:
            f_rate = ex.submit(self.upbit.fetch_usdt_krw_rate)
            f_up_mkts = ex.submit(self.upbit.fetch_krw_markets)
            f_bt_mkts = ex.submit(self.bithumb.fetch_krw_markets)
            v_futures = [ex.submit(_fetch_venue_metadata, v) for v in self.target_venues]

            usdt_krw_rate = f_rate.result()
            up_mkts = f_up_mkts.result()
            bt_mkts = f_bt_mkts.result()
            for fut in concurrent.futures.as_completed(v_futures):
                try:
                    v, p, tk = fut.result()
                    venue_perps[v] = p
                    venue_ticks[v] = tk
                    log.info(f"  • {v.upper():<8}: {len(p)} contracts | {len(tk)} 24h tickers")
                except Exception as e:
                    log.warning(f"  • Venue fetch failed: {e}")

        log.info(
            f"[Stage 1/2] USDT/KRW Rate: {usdt_krw_rate:.2f} | "
            f"Spot Markets: Upbit={len(up_mkts)}, Bithumb={len(bt_mkts)}"
        )

        # Batch fetch spot 24h tickers
        up_ticks = self.upbit.fetch_tickers_24h(list(up_mkts.values()), usdt_krw_rate)
        bt_ticks = self.bithumb.fetch_tickers_24h(usdt_krw_rate)
        log.info(f"[Stage 1/2] 24h Tickers: Upbit={len(up_ticks)}, Bithumb={len(bt_ticks)}")

        # Build reverse base map for each venue: canonical_base -> (raw_sym, mult, c_size)
        venue_base_maps: Dict[str, Dict[str, Tuple[str, float, float]]] = {}
        for v, perps in venue_perps.items():
            b_map: Dict[str, Tuple[str, float, float]] = {}
            for raw, val in perps.items():
                base = val[0]
                mult = float(val[1]) if len(val) > 1 else 1.0
                c_size = float(val[2]) if len(val) > 2 else 1.0
                if base not in b_map or mult == 1.0:
                    b_map[base] = (raw, mult, c_size)
            venue_base_maps[v] = b_map

        # Stage 1 Matching and Fast Volume + Namesake Price Sanity Filter
        candidate_coins: Dict[str, Dict[str, Any]] = {}
        rejected_namesakes = 0
        rejected_volume = 0

        all_korean_coins = sorted(set(up_mkts.keys()) | set(bt_mkts.keys()))

        for coin in all_korean_coins:
            if self.target_coins and coin not in self.target_coins:
                continue
            if coin in EXCLUDED_COINS:
                continue

            has_up = coin in up_mkts
            has_bt = coin in bt_mkts
            is_co_listed = (has_up and has_bt)

            up_tick = up_ticks.get(coin) if has_up else None
            bt_tick = bt_ticks.get(coin) if has_bt else None

            up_vol = float(up_tick.get("vol_usd_24h", 0.0)) if up_tick else 0.0
            up_px = float(up_tick.get("price_usd", 0.0)) if up_tick else 0.0

            bt_vol = float(bt_tick.get("vol_24h", 0.0)) if bt_tick else 0.0
            bt_px = float(bt_tick.get("price", 0.0)) if bt_tick else 0.0

            max_spot_vol = max(up_vol, bt_vol)
            if max_spot_vol < self.min_vol_usd:
                rejected_volume += 1
                continue

            # Reference spot price for namesake filtering
            ref_px = up_px if up_px > 0 else bt_px
            if ref_px <= 0:
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

                diff_pct = abs(ref_px - px_v) / max(ref_px, px_v) * 100.0
                if diff_pct <= self.max_price_diff_pct:
                    matched_venues_meta[v] = (raw_sym, mult, px_v, vol_v, c_size)
                else:
                    rejected_namesakes += 1
                    log.debug(f"[sanity] Rejected namesake {coin} on {v} (diff {diff_pct:.1f}%)")

            if matched_venues_meta:
                candidate_coins[coin] = {
                    "coin": coin,
                    "has_upbit": has_up,
                    "has_bithumb": has_bt,
                    "is_co_listed": is_co_listed,
                    "upbit_market": up_mkts.get(coin),
                    "bithumb_market": bt_mkts.get(coin),
                    "up_px": up_px,
                    "up_vol": up_vol,
                    "bt_px": bt_px,
                    "bt_vol": bt_vol,
                    "ref_px": ref_px,
                    "matched_venues": matched_venues_meta,
                    "matched_venues_meta": matched_venues_meta,
                }

        co_listed_candidates = sum(1 for c in candidate_coins.values() if c["is_co_listed"])
        bithumb_exclusives = sum(1 for c in candidate_coins.values() if c["has_bithumb"] and not c["has_upbit"])
        log.info(
            f"[Stage 1/2] Complete: {len(candidate_coins)} candidate coins survived "
            f"({co_listed_candidates} Dual-Listed, {bithumb_exclusives} Bithumb-Exclusive | "
            f"Rejected: {rejected_volume} low vol, {rejected_namesakes} namesakes)"
        )

        # ------------------------------------------------------------- Stage 2
        log.info(
            f"[Stage 2/2] Fetching {self.kline_limit} 1m candles for deep correlation & basis screening..."
        )

        def _evaluate_coin_worker(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            c = item["coin"]
            has_up = item["has_upbit"]
            has_bt = item["has_bithumb"]
            is_co_listed = item["is_co_listed"]

            kl_up: Dict[int, Tuple[float, float]] = {}
            kl_bt: Dict[int, Tuple[float, float]] = {}

            if has_up and item.get("upbit_market"):
                kl_up = self.upbit.fetch_klines_1m(item["upbit_market"], count=self.kline_limit)
            if has_bt and item.get("bithumb_market"):
                # Bithumb returns up to 200 recent candles
                kl_bt = self.bithumb.fetch_klines_1m(item["bithumb_market"], count=min(self.kline_limit, 200))

            if len(kl_up) < 30 and len(kl_bt) < 30:
                return None

            confirmed_venues: Dict[str, Dict[str, Any]] = {}
            best_res: Optional[Dict[str, Any]] = None
            coin_dual_confirmed = False

            venues_map = item.get("matched_venues_meta") or item.get("matched_venues", {})
            for v, meta in venues_map.items():
                if len(meta) == 5:
                    raw_sym, mult, _, _, c_size = meta
                elif len(meta) == 3:
                    raw_sym, mult, c_size = meta
                elif len(meta) == 2:
                    raw_sym, mult = meta
                    c_size = 1.0
                else:
                    continue
                client = self.clients.get(v)
                if not client:
                    continue
                kl_v = client.fetch_klines_1m(raw_sym, limit=self.kline_limit, mult=mult)
                if len(kl_v) < 30:
                    continue

                res_up: Optional[Dict[str, float]] = None
                res_bt: Optional[Dict[str, float]] = None

                if len(kl_up) >= 30:
                    res_up = evaluate_pair_klines(
                        kl_up, kl_v, usdt_krw_rate,
                        min_corr=self.min_corr,
                        min_both_trade=self.min_both_trade,
                        min_dev_bps=self.min_dev_bps,
                        max_price_diff_pct=self.max_price_diff_pct,
                    )

                if len(kl_bt) >= 30:
                    res_bt = evaluate_pair_klines(
                        kl_bt, kl_v, usdt_krw_rate,
                        min_corr=self.min_corr,
                        min_both_trade=self.min_both_trade,
                        min_dev_bps=self.min_dev_bps,
                        max_price_diff_pct=self.max_price_diff_pct,
                    )

                # DUAL-KOREAN CHECK (Проверяется дважды / усиление сигнала)
                is_up_passed = bool(res_up and res_up["passed"] > 0)
                is_bt_passed = bool(res_bt and res_bt["passed"] > 0)

                qual_res: Optional[Dict[str, float]] = None
                pair_dual = False

                if is_co_listed and is_up_passed and is_bt_passed:
                    # Double Korean Confirmation (Dual-Confirmed!)
                    pair_dual = True
                    coin_dual_confirmed = True
                    # Take the higher correlation for ranking, mark dual flag
                    qual_res = dict(res_up if res_up["corr"] >= res_bt["corr"] else res_bt)
                    qual_res["dual_confirmed"] = 1.0
                elif is_up_passed and res_up:
                    qual_res = dict(res_up)
                    qual_res["dual_confirmed"] = 0.0
                elif is_bt_passed and res_bt:
                    qual_res = dict(res_bt)
                    qual_res["dual_confirmed"] = 0.0

                if qual_res and qual_res.get("passed", 0.0) > 0:
                    confirmed_venues[v] = {
                        "raw_sym": raw_sym,
                        "mult": mult,
                        "c_size": c_size,
                        "metrics": qual_res,
                        "dual_confirmed": pair_dual,
                    }
                    if best_res is None or qual_res["corr"] > best_res["corr"]:
                        best_res = qual_res

            if confirmed_venues and best_res:
                return {
                    "coin": c,
                    "has_upbit": has_up,
                    "has_bithumb": has_bt,
                    "is_co_listed": is_co_listed,
                    "dual_confirmed": coin_dual_confirmed,
                    "upbit_market": item.get("upbit_market"),
                    "bithumb_market": item.get("bithumb_market"),
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

        # Sort: Dual-Korean confirmed first, then by best correlation descending
        vetted_coins.sort(
            key=lambda x: (x.get("dual_confirmed", False), x["best_metrics"]["corr"]),
            reverse=True,
        )

        elapsed = round(time.time() - t0, 2)
        dual_count = sum(1 for c in vetted_coins if c.get("dual_confirmed"))
        bt_excl_count = sum(1 for c in vetted_coins if c.get("has_bithumb") and not c.get("has_upbit"))

        log.info(
            f"[Stage 2/2] Complete in {elapsed}s: {len(vetted_coins)} coins qualified across all venues! "
            f"({dual_count} Dual-Korean Confirmed, {bt_excl_count} Bithumb-Exclusive)"
        )

        for i, q in enumerate(vetted_coins[:30], 1):
            m = q["best_metrics"]
            v_str = ",".join(q["venues"])
            dual_tag = "⚡ [DUAL-KR]" if q.get("dual_confirmed") else ("🇰🇷 [BT-EXCL]" if not q.get("has_upbit") else "   [UP-SOLO]")
            log.info(
                f"  #{i:2d} {q['coin']:<8} {dual_tag} | Corr: {m['corr']:>5.3f} | "
                f"Both: {m['both_trade']:>4.1%} | P90 Dev: {m['p90_dev_bps']:>5.1f} bps | [{v_str}]"
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
        upbit_krw_to_coin: Dict[str, str] = {
            c["upbit_market"]: c["coin"] for c in vetted_coins if c.get("upbit_market")
        }
        bithumb_krw_to_coin: Dict[str, str] = {
            c["bithumb_market"]: c["coin"] for c in vetted_coins if c.get("bithumb_market")
        }

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
                "upbit_symbol": c.get("upbit_market"),
                "bithumb_symbol": c.get("bithumb_market"),
                "korean_co_listed": c.get("is_co_listed", False),
                "dual_korean_confirmed": c.get("dual_confirmed", False),
                "venues": v_list,
                "best_corr": c["best_metrics"]["corr"],
                "best_p90_dev_bps": c["best_metrics"]["p90_dev_bps"],
                "venue_symbols": {v: v_details[v]["raw_sym"] for v in v_list if v in v_details},
            }

        dual_count = sum(1 for c in vetted_coins if c.get("dual_confirmed"))
        bt_excl_count = sum(1 for c in vetted_coins if c.get("has_bithumb") and not c.get("has_upbit"))

        doc = {
            "_meta": {
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "updated_ts": round(time.time(), 3),
                "usdt_krw_rate": round(usdt_krw_rate, 2),
                "coins_count": len(coins_list),
                "dual_korean_count": dual_count,
                "upbit_count": len(upbit_krw_to_coin),
                "bithumb_count": len(bithumb_krw_to_coin),
                "bithumb_exclusive_count": bt_excl_count,
                "active_venues": self.target_venues,
                "kline_limit": self.kline_limit,
                "screening_elapsed_sec": elapsed_sec,
            },
            "coins": coins_list,
            "upbit_krw_to_coin": upbit_krw_to_coin,
            "bithumb_krw_to_coin": bithumb_krw_to_coin,
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
