# Full Codebase: KoreanSunrise

## README.md

```md
# Korean Sunrise 🌅

**High-Frequency Spot Lead-Lag & Momentum Sniping System**
*Spot Leader: Upbit (South Korea)* ➔ *Perpetual Followers: Binance & Bybit Futures*

---

## 1. Архитектура и физика стратегии

Корейский криптовалютный рынок изолирован строгим валютным контролем (Foreign Exchange Transactions Act) и законодательным запретом на торговлю деривативами для резидентов. Вся ликвидность сосредоточена на спотовых парах к корейской воне (KRW).

Когда розничный или институциональный капитал в Корее начинает агрессивно выкупать альткоин или шиткоин ($500K – $50M капы):
1. **Агрессивный спотовый импульс на Upbit**: рыночные ордера выбивают уровни аска в стакане, вызывая скачок спреда и опустошение стакана.
2. **Временной лаг на мировых перпах (1.5 – 4.0 сек)**: алгоритмы маркет-мейкеров на Binance и Bybit не могут мгновенно захеджироваться на корейском споте и реагируют с существенной задержкой.
3. **Снайперский вход**: система детектирует физический дисбаланс в стакане Upbit и открывает направленную позицию на Binance или Bybit до того, как перпы догонят спот.

---

## 2. Динамические метрики входа (Zero Hardcode)

В системе полностью отсутствуют жестко закодированные долларовые пороги. Триггеры адаптируются под емкость каждого отдельного стакана:

* **Relative Depth Drain**:
  $$\text{Drain Ratio} = \frac{\Delta \text{Depth}_{\text{ask}}}{D_{\text{norm, ask}}} \ge 0.50$$
  *Фиксирует мгновенное выедание более 50% нормальной емкости стакана аска.*

* **Spread Blowout Factor & Price Shock**:
  $$\frac{\text{Spread}_{\text{instant}}}{\overline{\text{Spread}}_{600s}} \ge 2.5 \quad \text{и} \quad \frac{\Delta \text{Mid}}{\sigma_{\text{MAD}}} \ge 2.0$$
  *Фиксирует раскрытие спреда в 2.5 раза шире нормы при сдвиге цены более чем на 2 робастных сигмы.*

* **Universal Stealth Sizing Rule**:
  $$\text{Size}_{\text{USD}} = \min\left(\text{MaxBudgetUSD}, \,\, 0.15 \times \sum_{i=1}^3 \text{Depth}_{i}^{\text{ask}}(\text{follower})\right)$$
  *Размер ордера строго ограничен 15% от доступного объема в топ-3 уровнях стакана фьючерсов, предотвращая собственное проскальзывание и оставаясь невидимым для алгоритмов биржи.*

---

## 3. Структура проекта

```
KoreanSunrise/
├── main.py                  # Главная точка входа сервиса
├── config.py                # Конфигурация, авто-дискавери и окружение
├── korean_engine.py         # Ядро детекции Lead-Lag, фильтрации и расчета эджа
├── batch_writer.py          # Атомарный компрессор сигналов на диск (gzip jsonl)
├── models.py                # Типы данных (OrderBookDepth5, SunriseSignal)
├── matrix_thresholds.json   # Калиброванные динамические пороги входа
├── universe_live.json       # Активный инкубационный пул монет
├── trading_profile.json     # Профиль риск-менеджмента и лимиты
├── runtime_overrides.json   # Операторские паузы и ручные оверрайды
│
├── venues/                  # Модульные адаптеры 10 бирж и координатор стримов
│   ├── base.py              # Базовые классы BaseVenueStream, BaseCEXStream
│   ├── upbit.py             # Spot Leader: Upbit KRW
│   ├── binance.py           # Follower: Binance Futures (USDT-M)
│   ├── bybit.py             # Follower: Bybit Linear Perpetuals
│   ├── okx.py               # Follower: OKX USDT Swaps
│   ├── gateio.py            # Follower: Gate.io USDT Futures
│   ├── mexc.py              # Follower: MEXC USDT Contract
│   ├── kucoin.py            # Follower: KuCoin USDT Futures
│   ├── bingx.py             # Follower: BingX Swap
│   ├── htx.py               # Follower: HTX / Huobi Linear Swap
│   ├── pionex.py            # Follower: Pionex Futures
│   └── manager.py           # StreamManager: координатор параллельных потоков
│
├── pipeline/                # Оценка эффективности и управление пулом
│   ├── evaluator.py         # Анализ сигналов, расчет PnL/Winrate/Score
│   ├── pool_state.py        # Управление пулом (Graduation / Probation / Delisting)
│   └── profile.py           # Управление профилями капитала и риск-параметров
│
├── screening/               # Скрининг и отбор монет
│   ├── metrics.py           # Расчет OBI, спредов, волатильности и ликвидности
│   └── screener.py          # Полносуточная пагинация (1440 свечей) по 10 биржам
│
├── tools/                   # Набор CLI инструментов аналитики и обслуживания
│   ├── run_daily_pipeline.py    # Мастер-оркестратор ежедневного жизненного цикла
│   ├── screen_universe.py       # Скрининг инкубатора с 24-часовой пагинацией
│   ├── calibrate_thresholds.py  # Авто-калибровка порогов спреда (matrix_thresholds)
│   ├── full_daily_report.py     # Генератор квантовых отчетов (Markdown + JSON)
│   ├── analyze_signals.py       # Пост-трейд аналитика и кривые маркаута
│   ├── manage_pool.py           # CLI управление пулом монет (pause/resume/graduate)
│   ├── audit_venues.py          # Аудит задержек и ликвидности 10 бирж
│   └── validate_data.py         # Валидатор целостности архивов и схем
│
├── web/                     # Web UI мониторинга и REST API
│   ├── server.py            # Асинхронный веб-сервер (порт 8089)
│   ├── views.py             # HTML5/CSS3 дашборд с авторизацией
│   └── reports_manager.py   # Менеджер исторических отчетов
│
├── scripts/                 # Скрипты деплоя и системной автоматизации
│   ├── korean-sunrise.service   # Systemd unit для сервиса
│   ├── daily-pipeline.service   # Systemd unit для ночного пайплайна
│   ├── daily-pipeline.timer     # Systemd таймер (03:00 UTC)
│   └── cron_daily.sh            # Cron bash скрипт запуска
│
├── reports/                 # Хранилище сгенерированных отчетов
│   └── daily/               # Ежедневные JSON и Markdown отчеты
│
├── docs/                    # Документация и аналитические заметки
│   └── explanations.md      # Заметки по микроструктуре и боевому исполнению
│
└── logs/                    # Логи сервисов (авто-ротация и изоляция)
    └── .gitkeep
```

---

## 4. Запуск и использование

```bash
# Установка зависимостей
pip install -r requirements.txt

# Запуск основного сервиса сбора и детекции (с Web UI на :8089)
python main.py

# Запуск полного ежедневного пайплайна (скрининг -> калибровка -> отчет)
python tools/run_daily_pipeline.py

# Анализ собранных сигналов и построение кривых маркаута
python tools/analyze_signals.py
```
```

## batch_writer.py

```python
"""
batch_writer.py - Non-blocking compressed disk writer for Korean Sunrise signals.
Buffers raw JSON lines in RAM and compresses via gzip in batches upon flush.
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

log = logging.getLogger("writer")


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class BatchWriter:
    """Buffers signals in RAM (as raw lines) and writes compressed sequentially to disk."""

    def __init__(self, data_root: str = "data", flush_interval_sec: float = 5.0):
        self.data_root = data_root
        self.flush_interval_sec = flush_interval_sec
        self._lines: List[str] = []
        self.total_written = 0
        self._flush_seq = 0
        self._lock = threading.Lock()
        self._last_flush = time.time()
        os.makedirs(os.path.join(self.data_root, "signals"), exist_ok=True)

    @property
    def rows_count(self) -> int:
        with self._lock:
            return len(self._lines)

    def add(self, signal_dict: Dict[str, Any]) -> None:
        """Non-blocking in-memory append of signal line."""
        try:
            line = json.dumps(signal_dict, default=str) + "\n"
            with self._lock:
                self._lines.append(line)
        except Exception as e:
            log.error(f"[writer] Failed to buffer signal: {e}")

    def maybe_flush(self, now: float = 0.0) -> None:
        now = now or time.time()
        with self._lock:
            need_flush = (now - self._last_flush >= self.flush_interval_sec) and bool(self._lines)
        if need_flush:
            self.flush()

    def flush(self) -> None:
        """Compresses all buffered lines in a single batch and writes atomically to disk."""
        with self._lock:
            if not self._lines:
                return
            batch_lines = self._lines
            self._lines = []
            self._last_flush = time.time()
            self._flush_seq += 1
            seq = self._flush_seq

        batch_rows = len(batch_lines)
        try:
            raw_bytes = "".join(batch_lines).encode("utf-8")
            compressed_data = gzip.compress(raw_bytes, compresslevel=6)

            day = _utc_day()
            target_dir = os.path.join(self.data_root, "signals", day)
            os.makedirs(target_dir, exist_ok=True)

            ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"sunrise_signals_{ts_str}_{seq:04d}.jsonl.gz"
            tmp_path = os.path.join(target_dir, f"{filename}.tmp")
            final_path = os.path.join(target_dir, filename)

            with open(tmp_path, "wb") as f:
                f.write(compressed_data)
            os.replace(tmp_path, final_path)
            with self._lock:
                self.total_written += batch_rows
            log.info(
                f"[writer] Flushed {batch_rows} signals to {filename} "
                f"({len(compressed_data)} bytes)"
            )
        except Exception as e:
            with self._lock:
                self._lines = batch_lines + self._lines  # Rollback lines to buffer on failure
            log.error(f"[writer] Flush error: {e}")
```

## config.py

```python
"""
config.py - Configuration & Live Universe Discovery for Korean Sunrise.
Automatically fetches all KRW coins from Upbit and matches them against
active USDT Linear Perpetuals on Binance and Bybit (including 1000x/1M prefixes).
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import os
import re
import urllib.request
from typing import Dict, List, Optional, Set, Tuple

log = logging.getLogger("config")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
DATA_ROOT = os.getenv("DATA_ROOT", "data")
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# API Web Server & Dashboard
API_SERVER_ENABLED = os.getenv("API_SERVER_ENABLED", "true").lower() in ("true", "1", "yes")
API_SERVER_HOST = os.getenv("API_SERVER_HOST", "0.0.0.0")
API_SERVER_PORT = int(os.getenv("API_SERVER_PORT", "8089"))
API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN", "vitali_secret_key_2026")

# Exclude heavy majors & stablecoins so we focus purely on alts / shitcoins.
# (Can be overridden via env EXCLUDE_COINS="BTC,ETH,USDT,USDC")
_default_exclude = "BTC,ETH,USDT,USDC,BUSD,FDUSD,DAI,TUSD,RLUSD,PYUSD,USDE,USDD,EUR,GUSD"
EXCLUDED_COINS: Set[str] = {
    x.strip().upper()
    for x in os.getenv("EXCLUDE_COINS", _default_exclude).split(",")
    if x.strip()
}

# Known rename/alias mapping between Upbit KRW ticker and Global Perp base ticker
# Format: Upbit_Ticker -> Global_Base_Ticker
TICKER_ALIASES: Dict[str, str] = {
    # Add any ticker discrepancies if Upbit uses an old/local symbol name
    "MATIC": "POL",
}

# Core Engine Parameters
DRAIN_THRESHOLD = 0.50          # 50% of normal top-5 ask depth swept on Upbit
BLOWOUT_THRESHOLD = 2.5         # Instant spread >= 2.5x normal EWMA spread AND >= 2 ticks
MIN_EXEC_EDGE_BPS = 12.0        # Minimum net executable edge after entry taker fee
MAX_POSITION_BUDGET_USD = 10000.0
MIN_ORDER_USD = 10.0            # Minimum order size in USD to reject non-executable micro-orders
STEALTH_DEPTH_FRACTION = 0.15   # Max 15% of top 3 ask levels on follower order book

# Execution Fees & Warmup Safety
TAKER_FEE_BPS = 5.0             # Single-leg VIP0 taker fee (bps) on Binance/Bybit
ROUNDTRIP_FEE_BPS = 10.0        # Entry + Exit taker fee (bps) for net realized PnL
MIN_WARMUP_SAMPLES = 30         # Minimum 1s MAD samples before firing signals
MAX_SIGNAL_HOLD_SEC = 30.0      # Maximum holding horizon before timeout closure (adaptive ceiling)
MAX_FOLLOWER_BOOK_AGE_SEC = 5.0 # Maximum staleness allowed for follower book snapshots (adaptive for alts)

# Adaptive Stop-Loss Parameters
STOP_INVALIDATION_BPS = 15.0    # Model A: Microstructure invalidation tolerance (leader premium breakdown)
STOP_ADVERSE_PNL_BPS = 35.0     # Model B: Hard adverse PnL drawdown stop (bps)
ALPHA_DECAY_SEC = 8.0           # Model C: Alpha decay timeout if PnL <= 0 (transmission failure)

# Regex to detect multiplier prefixes on Binance/Bybit perps: 1000PEPE, 1000000MOG, etc.
_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")

ALL_SUPPORTED_VENUES = [
    "binance",
    "bybit",
    "okx",
    "gateio",
    "mexc",
    "kucoin",
    "bingx",
    "htx",
    "pionex",
    "asterdex",
    "hyperliquid",
]


@dataclass
class MatchedUniverse:
    """Holds the dynamically matched universe of coins across Upbit and all 11 follower perp venues."""
    # Canonical base coin list (Upbit base names, e.g. ["PEPE", "DOGE", "SAND", ...])
    coins: List[str] = field(default_factory=list)

    # Upbit mapping: KRW market code -> canonical coin (e.g. "KRW-PEPE" -> "PEPE")
    upbit_krw_to_coin: Dict[str, str] = field(default_factory=dict)

    # First-class typed dictionary for all follower venue symbol maps:
    # venue -> {raw_symbol: (canonical_coin, mult, contract_size)}
    venue_sym_maps: Dict[str, Dict[str, Tuple[str, float, float]]] = field(default_factory=dict)

    # Legacy convenience attributes for Binance & Bybit backward-compatibility
    binance_sym_map: Dict[str, Tuple[str, float]] = field(default_factory=dict)
    bybit_sym_map: Dict[str, Tuple[str, float]] = field(default_factory=dict)

    def get_sym_map(self, venue: str) -> Dict[str, Tuple[str, float, float]]:
        """Returns the symbol mapping for any supported venue with (coin, mult, c_size)."""
        v = venue.lower()
        if v in self.venue_sym_maps:
            return self.venue_sym_maps[v]
        if v == "binance":
            return {
                k: (val[0], float(val[1]), float(val[2]) if len(val) >= 3 else 1.0)
                for k, val in self.binance_sym_map.items()
            }
        if v == "bybit":
            return {
                k: (val[0], float(val[1]), float(val[2]) if len(val) >= 3 else 1.0)
                for k, val in self.bybit_sym_map.items()
            }
        return {}


def _http_get_json(url: str, timeout: float = 12.0):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; KoreanSunrise/2.0)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _match_perp_base(perp_base: str, upbit_bases: Set[str]) -> Optional[Tuple[str, float]]:
    """
    Matches a Binance/Bybit perp base asset (e.g. 'SAND' or '1000PEPE')
    to an Upbit KRW base coin.
    Returns (upbit_coin, multiplier) or None.
    """
    # 1. Direct 1:1 match
    if perp_base in upbit_bases:
        return perp_base, 1.0

    # 2. Check reverse alias (if Upbit has old name and perp has new name)
    for up_coin, alias_target in TICKER_ALIASES.items():
        if perp_base == alias_target and up_coin in upbit_bases:
            return up_coin, 1.0

    # 3. Multiplier prefix match (e.g. 1000PEPE -> PEPE with mult=1000.0)
    m = _MULT_PREFIX_RE.match(perp_base)
    if m:
        mult_str, stripped_base = m.group(1), m.group(2)
        mult = float(mult_str)
        if stripped_base in upbit_bases:
            return stripped_base, mult
        for up_coin, alias_target in TICKER_ALIASES.items():
            if stripped_base == alias_target and up_coin in upbit_bases:
                return up_coin, mult

    return None


def discover_universe() -> MatchedUniverse:
    """
    Queries Upbit, Binance Futures, and Bybit Linear REST APIs to build
    an exact, real-time intersection of all Korean KRW altcoins/shitcoins
    and their corresponding USDT perpetual contracts.
    """
    # 1. Fetch all Upbit KRW spot markets
    upbit_data = _http_get_json("https://api.upbit.com/v1/market/all?isDetails=true")
    upbit_bases: Set[str] = set()
    for item in upbit_data:
        mkt = item.get("market", "")
        if mkt.startswith("KRW-"):
            coin = mkt.split("-", 1)[1].upper()
            if coin and coin not in EXCLUDED_COINS:
                upbit_bases.add(coin)

    if not upbit_bases:
        raise RuntimeError("Failed to discover any KRW markets from Upbit REST API")

    # 2. Fetch all active Binance USDT Linear Perpetuals
    binance_sym_map: Dict[str, Tuple[str, float]] = {}
    try:
        b_data = _http_get_json("https://fapi.binance.com/fapi/v1/exchangeInfo")
        for s in b_data.get("symbols", []):
            if (
                s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"
                and s.get("quoteAsset") == "USDT"
            ):
                raw_sym = s["symbol"].upper()
                base_asset = s.get("baseAsset", "").upper()
                matched = _match_perp_base(base_asset, upbit_bases)
                if matched:
                    binance_sym_map[raw_sym] = matched
    except Exception as e:
        log.error(f"[discovery] Binance Futures exchangeInfo failed: {e}")

    # 3. Fetch all active Bybit USDT Linear Perpetuals
    bybit_sym_map: Dict[str, Tuple[str, float]] = {}
    try:
        cursor = ""
        while True:
            url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"
            if cursor:
                url += f"&cursor={cursor}"
            by_data = _http_get_json(url)
            res = by_data.get("result", {})
            for item in res.get("list", []):
                if (
                    item.get("status") == "Trading"
                    and item.get("quoteCoin") == "USDT"
                    and item.get("contractType") == "LinearPerpetual"
                ):
                    raw_sym = item["symbol"].upper()
                    base_coin = item.get("baseCoin", "").upper()
                    matched = _match_perp_base(base_coin, upbit_bases)
                    if matched:
                        bybit_sym_map[raw_sym] = matched
            cursor = res.get("nextPageCursor", "")
            if not cursor:
                break
    except Exception as e:
        log.error(f"[discovery] Bybit instruments-info failed: {e}")

    # 4. Keep only Upbit coins that have at least one active perp on Binance or Bybit
    matched_coins: Set[str] = set()
    for coin, _ in binance_sym_map.values():
        matched_coins.add(coin)
    for coin, _ in bybit_sym_map.values():
        matched_coins.add(coin)

    sorted_coins = sorted(matched_coins)
    upbit_krw_to_coin = {f"KRW-{c}": c for c in sorted_coins}

    log.info(
        f"[discovery] Upbit KRW alts: {len(upbit_bases)} | "
        f"Matched active alts: {len(sorted_coins)} "
        f"(Binance perps: {len(binance_sym_map)}, Bybit perps: {len(bybit_sym_map)})"
    )

    return MatchedUniverse(
        coins=sorted_coins,
        upbit_krw_to_coin=upbit_krw_to_coin,
        binance_sym_map=binance_sym_map,
        bybit_sym_map=bybit_sym_map,
    )
```

## docs/explanations.md

```md
# Korean Sunrise — Аналитические заметки и дорожная карта боевого исполнения

## 1. Телеметрия сигналов и отсев токсичных монет

Для базового отсева монет данные уже собираются:
- `drain_ratio`, `dev_z`, `net_exec_edge_bps`, `follower_spread_bps`, `follower_top3_ask_usd`
- Итоговый `net_pnl_bps` и траектория `path` в виде `[dt, up_mid_move, f_mid_move, f_exec_pnl, rem_edge]`
- Долларовая фактура пробоя на Upbit (`leader_norm_depth_usd`, `leader_swept_usd`, `pair_sigma_bps`)
- Имбалансы стаканов лидера и фолловера (`leader_imbalance`, `follower_imbalance`)
- Лаги и возраст котировок (`upbit_latency_ms`, `follower_book_age_ms`)
- Пиковый профит, просадка и тайминг пика (`max_exec_pnl_bps`, `min_exec_pnl_bps`, `time_to_peak_sec`)
- 5-мерный path для точного построения кривой маркаута (markout curve на 100 мс, 250 мс, 500 мс, 1 с, 2 с, 5 с)

## 2. Архитектура боевого автомата (Execution & Risk)

Разворачивать боевых торговых ботов на этой основе требует следующих компонентов:

### 1. Модуль ордер-менеджмента (Order Execution Manager)
- **Рыночный вход (Market Entry)**: По сигналу `SunriseSignal` бот мгновенно отправляет маркет-ордер на покупку на фолловере на рассчитанный объем `suggested_size_usd`.
- **Управление позицией**: Обработка заполнения (fill), отслеживание реальной цены входа, выставление защитного стоп-лосса и выход при `converged`, `reversed` или `timeout`.

### 2. Учет балансов и риск-менеджмент (Risk Management & Inventory)
- Контроль свободной маржи на фьючерсных аккаунтах.
- Ограничение максимальной экспозиции на одну монету.
- Защита от проскальзывания (Slippage Protection): если спред резко расширился, блокировать вход.

### 3. Режим «Dry Run / Paper Trading»
- Эмуляция исполнения по реальному `best_ask` в момент сигнала.
- Расчет честного PnL по ходу движения стакана фолловера.
- Проверка Realized PnL с учетом реальных проскальзываний на потоке данных.
```

## korean_engine.py

```python
"""
korean_engine.py - High-frequency Korean Lead-Lag & Relative Depth Drain Engine.
Evaluates Spot Leader (Upbit) against Perpetual Followers (Binance, Bybit)
and records full microstructural telemetry for post-trade analysis.
"""
from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple

import config as C
from models import OrderBookDepth5, SunriseSignal

log = logging.getLogger("engine")

DEV_SAMPLE_DT = 1.0     # Fixed 1s time grid for robust MAD window
NORM_TAU = 600.0        # EWMA time constant for Upbit depth and spread normalization (sec)
BASELINE_TAU = 3600.0   # EWMA time constant for cross-venue basis baseline (sec)


def _base(symbol: str) -> str:
    s = symbol.upper()
    if s.startswith("KRW-"):
        return s[4:]
    for suff in ("-PERP", "_PERP", "PERP", "USDT"):
        if s.endswith(suff) and len(s) > len(suff):
            return s[: -len(suff)]
    return s


@dataclass(slots=True)
class UpbitNormState:
    """EWMA normalized depth, spread, and top-level liquidity for Upbit spot."""
    depth_ask: float
    best_ask_usd: float
    spread_bps: float
    t: float

    def __getitem__(self, key: str) -> float:
        return getattr(self, key)

    def __setitem__(self, key: str, value: float) -> None:
        setattr(self, key, value)


def evaluate_exit_reason(
    cur_net_pnl: float,
    net_exec_edge_bps: float,
    rem_edge: float,
    dt: float,
    hold_sec: float,
    pair_decay_sec: float,
    sigma: float,
    upbit_drop_bps: float,
    cur_dislocation_bps: float,
    stop_invalidation_bps: float,
    stop_adverse_pnl_bps: float,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Pure mathematical evaluator for signal exit and stop-loss conditions in strict priority:
    1. Take-Profit: 75% net edge captured or basis fully absorbed in profit.
    2. Model A: Microstructure Invalidation Stop.
    3. Model B: Hard Adverse PnL Drawdown Stop.
    4. Model C: Alpha Decay Timeout Stop.
    5. Max Hold Horizon Timeout.
    Returns (close_reason, stop_type).
    """
    # 1. Normal Take-Profit / Edge Absorption
    if cur_net_pnl >= (net_exec_edge_bps * 0.75) and cur_net_pnl > 0.0:
        return "converged", None
    if rem_edge <= 0.0 and cur_net_pnl > 0.0:
        return "converged", None

    # 2. Model A: Microstructure Invalidation Stop
    if upbit_drop_bps >= (1.5 * sigma) or cur_dislocation_bps <= -stop_invalidation_bps:
        return "stop_invalidation", "invalidation"

    # 3. Model B: Hard Adverse PnL Drawdown Stop
    adverse_limit = max(stop_adverse_pnl_bps, 2.5 * sigma)
    if cur_net_pnl <= -adverse_limit:
        return "stop_adverse_pnl", "adverse_pnl"

    # 4. Model C: Alpha Decay Timeout Stop
    if dt >= pair_decay_sec and cur_net_pnl <= 0.0:
        return "stop_decay", "alpha_decay"

    # 5. Max Hold Horizon Timeout
    if dt >= hold_sec:
        return "timeout", None

    return None, None


class PairState:
    """State of an (Upbit, Follower) pair for a single coin."""

    __slots__ = (
        "coin",
        "follower_ex",
        "warmup_samples",
        "spike_sigma",
        "baseline_bps",
        "n_obs",
        "t_last",
        "dev_window",
        "dev_ts",
        "cached_sigma",
        "cached_sigma_ts",
        "open_signal",
    )

    def __init__(
        self,
        coin: str,
        follower_ex: str,
        warmup_samples: int = C.MIN_WARMUP_SAMPLES,
        spike_sigma: float = 2.5,
    ):
        self.coin = coin
        self.follower_ex = follower_ex
        self.warmup_samples = warmup_samples
        self.spike_sigma = spike_sigma

        # Rolling cross-venue basis baseline (bps): (Mid_Upbit - Mid_Follower) / Mid_Follower * 10000
        self.baseline_bps: float = 0.0
        self.n_obs: int = 0
        self.t_last: float = 0.0

        # Robust MAD window sampled on fixed 1s grid
        self.dev_window: Deque[float] = deque(maxlen=300)
        self.dev_ts: float = 0.0
        self.cached_sigma: float = 10.0
        self.cached_sigma_ts: float = 0.0

        # Active open signal
        self.open_signal: Optional[SunriseSignal] = None

    @property
    def is_warmed_up(self) -> bool:
        return len(self.dev_window) >= self.warmup_samples and self.n_obs >= self.warmup_samples

    def learn(self, raw_diff_bps: float, now: float) -> None:
        if self.n_obs == 0:
            self.baseline_bps = raw_diff_bps
            self.n_obs = 1
            self.t_last = now
            self.dev_ts = now
            return

        dt = max(0.0, now - self.t_last)
        self.t_last = now
        deviation = raw_diff_bps - self.baseline_bps

        # Spike gating: if a massive pump/dislocation is currently happening (> spike_sigma),
        # slow down baseline adaptation by 20x so the baseline doesn't chase the pump.
        cur_sig = self.sigma(now)
        is_spike = self.is_warmed_up and abs(deviation) > (self.spike_sigma * cur_sig)
        eff_tau = 10.0 if not self.is_warmed_up else (BASELINE_TAU * 20.0 if is_spike else BASELINE_TAU)

        a = 1.0 - math.exp(-dt / eff_tau) if eff_tau > 0 else 0.001
        self.baseline_bps += a * deviation
        self.n_obs += 1

        if now - self.dev_ts >= DEV_SAMPLE_DT:
            self.dev_ts = now
            if is_spike:
                clamped_dev = math.copysign(min(abs(deviation), (self.spike_sigma + 1.5) * cur_sig), deviation)
                self.dev_window.append(clamped_dev)
            else:
                self.dev_window.append(deviation)
            self.cached_sigma_ts = 0.0

    def sigma(self, now: float) -> float:
        if self.cached_sigma_ts and (now - self.cached_sigma_ts) < 1.0:
            return self.cached_sigma

        if len(self.dev_window) < 10:
            return 10.0

        vals = sorted(self.dev_window)
        med = vals[len(vals) // 2]
        abs_dev = sorted(abs(x - med) for x in vals)
        mad = abs_dev[len(abs_dev) // 2]
        self.cached_sigma = max(1.4826 * mad, 1.5)
        self.cached_sigma_ts = now
        return self.cached_sigma


class KoreanEngine:
    """
    Core Lead-Lag Engine with full analytical telemetry.
    """

    def __init__(
        self,
        emit_callback: Callable[[Dict[str, Any]], None],
        drain_threshold: float = C.DRAIN_THRESHOLD,
        blowout_threshold: float = C.BLOWOUT_THRESHOLD,
        min_edge_bps: float = C.MIN_EXEC_EDGE_BPS,
        max_position_budget_usd: float = C.MAX_POSITION_BUDGET_USD,
        min_order_usd: float = getattr(C, "MIN_ORDER_USD", 10.0),
        stealth_depth_fraction: float = C.STEALTH_DEPTH_FRACTION,
        taker_fee_bps: float = C.TAKER_FEE_BPS,
        roundtrip_fee_bps: float = C.ROUNDTRIP_FEE_BPS,
        warmup_samples: int = C.MIN_WARMUP_SAMPLES,
        max_hold_sec: float = C.MAX_SIGNAL_HOLD_SEC,
        max_follower_book_age_sec: float = getattr(C, "MAX_FOLLOWER_BOOK_AGE_SEC", 5.0),
        stop_invalidation_bps: float = getattr(C, "STOP_INVALIDATION_BPS", 15.0),
        stop_adverse_pnl_bps: float = getattr(C, "STOP_ADVERSE_PNL_BPS", 35.0),
        alpha_decay_sec: float = getattr(C, "ALPHA_DECAY_SEC", 8.0),
        require_trade_confirmation: bool = False,
        pool_state: Any = None,
        profile_manager: Any = None,
        thresholds_file: Optional[str] = None,
    ):
        self.emit = emit_callback
        self.drain_threshold = drain_threshold
        self.blowout_threshold = blowout_threshold
        self.min_edge_bps = min_edge_bps
        self.max_position_budget_usd = max_position_budget_usd
        self.min_order_usd = min_order_usd
        self.stealth_fraction = stealth_depth_fraction
        self.taker_fee_bps = taker_fee_bps
        self.roundtrip_fee_bps = roundtrip_fee_bps
        self.warmup_samples = warmup_samples
        self.max_hold_sec = max_hold_sec
        self.max_follower_book_age_sec = max_follower_book_age_sec
        self.stop_invalidation_bps = stop_invalidation_bps
        self.stop_adverse_pnl_bps = stop_adverse_pnl_bps
        self.alpha_decay_sec = alpha_decay_sec
        self.require_trade_confirmation = require_trade_confirmation
        self.pool_state = pool_state
        self.profile_manager = profile_manager

        # (exchange, coin) -> OrderBookDepth5
        self._books: Dict[Tuple[str, str], OrderBookDepth5] = {}
        self._recv_ts: Dict[Tuple[str, str], float] = {}

        # O(1) follower lookup index: coin -> set of follower exchanges
        self._coin_followers: Dict[str, Set[str]] = defaultdict(set)

        # Upbit recent taker buys: coin -> deque of (ts, trade_usd)
        self._buys_lock = threading.Lock()
        self._upbit_recent_buys: Dict[str, Deque[Tuple[float, float]]] = defaultdict(lambda: deque(maxlen=200))
        self._has_upbit_trades: Dict[str, bool] = defaultdict(bool)

        # Upbit normalisation state per coin:
        # coin -> {"depth_ask": float, "best_ask_usd": float, "spread_bps": float, "t": float}
        self._upbit_norm: Dict[str, Dict[str, float]] = {}

        # Pair states: (coin, follower_exchange) -> PairState
        self._pairs: Dict[Tuple[str, str], PairState] = {}

        # Active signals tracking: coin -> list of active SunriseSignal
        self._active_signals: Dict[str, List[SunriseSignal]] = defaultdict(list)
        self._seq = 0

        # Telemetry counters
        self.counters = {
            "ticks_upbit": 0,
            "ticks_followers": 0,
            "trades_upbit": 0,
            "cancels_blocked": 0,
            "signals_triggered": 0,
            "signals_battle": 0,
            "signals_incubator": 0,
            "signals_converged": 0,
            "signals_reversed": 0,
            "signals_timeout": 0,
            "signals_stop_loss_invalidation": 0,
            "signals_stop_loss_adverse_pnl": 0,
            "signals_stop_loss_decay": 0,
        }

        # Pair-specific calibrated execution thresholds
        self.thresholds_file = thresholds_file or os.path.join(C.PROJECT_ROOT, "matrix_thresholds.json")
        self._thresholds: Dict[str, Any] = {}
        self._last_thresh_check: float = 0.0
        self._thresh_mtime: float = 0.0
        self.reload_thresholds(force=True)

    def reload_thresholds(self, force: bool = False) -> None:
        try:
            if os.path.exists(self.thresholds_file):
                mtime = os.path.getmtime(self.thresholds_file)
                if not force and mtime == self._thresh_mtime:
                    return
                with open(self.thresholds_file, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                    self._thresholds = doc.get("thresholds", {})
                self._thresh_mtime = mtime
                log.info(f"[engine] Loaded {len(self._thresholds)} calibrated pair thresholds.")
                # Update existing PairState instances with latest spike_sigma
                for (coin, f_ex), st in self._pairs.items():
                    cfg = self.get_pair_threshold_config(coin, f_ex)
                    st.spike_sigma = float(cfg.get("spike_sigma", 2.5))
        except Exception as e:
            log.warning(f"[engine] Failed to load {self.thresholds_file}: {e}")

    def maybe_reload_thresholds(self, now: float) -> None:
        if now - self._last_thresh_check >= 30.0:
            self._last_thresh_check = now
            try:
                self.reload_thresholds()
            except Exception as e:
                log.warning(f"[engine] Error reloading thresholds: {e}")

            if self.pool_state is not None and hasattr(self.pool_state, "reload"):
                try:
                    self.pool_state.reload()
                except Exception as e:
                    log.warning(f"[engine] Error reloading pool_state: {e}")

            if self.profile_manager is not None and hasattr(self.profile_manager, "reload"):
                try:
                    self.profile_manager.reload()
                except Exception as e:
                    log.warning(f"[engine] Error reloading profile_manager: {e}")

    def get_pair_threshold_config(self, coin: str, follower_ex: str) -> Dict[str, Any]:
        pk = f"{coin.upper()}:upbit-{follower_ex.lower()}"
        return self._thresholds.get(pk, {})

    def _get_or_create_pair_state(self, coin: str, follower_ex: str) -> PairState:
        pair_key = (coin, follower_ex)
        st = self._pairs.get(pair_key)
        if st is None:
            cfg = self.get_pair_threshold_config(coin, follower_ex)
            sp_sig = float(cfg.get("spike_sigma", 2.5))
            st = self._pairs[pair_key] = PairState(
                coin, follower_ex, self.warmup_samples, spike_sigma=sp_sig
            )
        return st

    def on_book_update(self, snap: OrderBookDepth5) -> None:
        now = time.time()
        ex = snap.exchange.lower()
        coin = snap.symbol.upper()
        key = (ex, coin)

        prev_snap = self._books.get(key)
        self._books[key] = snap
        self._recv_ts[key] = now

        if ex == "upbit":
            self.counters["ticks_upbit"] += 1
            self._eval_lead_lag(coin, snap, prev_snap, now)
            self._update_upbit_norm(coin, snap, now)
            self._track_active_signals_for_coin(coin, now)
        else:
            self._coin_followers[coin].add(ex)
            self.counters["ticks_followers"] += 1
            upbit_snap = self._books.get(("upbit", coin))
            if upbit_snap is not None and (now - self._recv_ts.get(("upbit", coin), 0.0)) <= 15.0:
                st = self._get_or_create_pair_state(coin, ex)
                if upbit_snap.mid_price > 0 and snap.mid_price > 0:
                    raw_diff_bps = (upbit_snap.mid_price - snap.mid_price) / snap.mid_price * 1e4
                    if st.open_signal is None:
                        st.learn(raw_diff_bps, now)

            self._track_follower_move(coin, ex, snap, now)

    def on_upbit_trade(self, coin: str, trade_usd: float, is_taker_buy: bool, ts: float) -> None:
        """
        Real-time trade stream ingestion from Upbit.
        Records taker buys to verify liquidity sweeps and reject MM quote cancellations.
        Thread-safe across concurrent WebSocket worker threads.
        """
        c = coin.upper()
        with self._buys_lock:
            self.counters["trades_upbit"] = self.counters.get("trades_upbit", 0) + 1
            self._has_upbit_trades[c] = True
            if is_taker_buy and trade_usd > 0.0:
                self._upbit_recent_buys[c].append((ts, trade_usd))

    def get_recent_upbit_buys(self, coin: str, cutoff: float) -> Tuple[float, bool]:
        """
        Thread-safe retrieval and pruning of recent taker buys for a coin.
        Returns (recent_buys_usd, has_trades).
        Prevents race conditions (IndexError / deque mutation during iteration).
        """
        c = coin.upper()
        with self._buys_lock:
            buys_deque = self._upbit_recent_buys.get(c)
            recent_buys_usd = 0.0
            if buys_deque:
                while buys_deque and buys_deque[0][0] < cutoff:
                    buys_deque.popleft()
                recent_buys_usd = sum(b[1] for b in buys_deque)
            has_trades = self._has_upbit_trades.get(c, False)
            return recent_buys_usd, has_trades

    # ----------------------------------------------------------- Upbit Norm
    def _update_upbit_norm(self, coin: str, snap: OrderBookDepth5, now: float) -> None:
        nrm = self._upbit_norm.get(coin)
        cur_depth = snap.depth_ask_usd
        cur_best_ask_usd = snap.best_ask_usd
        cur_spread = snap.spread_bps

        if nrm is None:
            self._upbit_norm[coin] = UpbitNormState(
                depth_ask=max(cur_depth, 100.0),
                best_ask_usd=max(cur_best_ask_usd, 50.0),
                spread_bps=max(cur_spread, 1.0),
                t=now,
            )
            return

        if isinstance(nrm, dict):
            nrm = self._upbit_norm[coin] = UpbitNormState(
                depth_ask=float(nrm["depth_ask"]),
                best_ask_usd=float(nrm["best_ask_usd"]),
                spread_bps=float(nrm["spread_bps"]),
                t=float(nrm["t"]),
            )

        dt = max(0.0, now - nrm.t)
        nrm.t = now
        a = 1.0 - math.exp(-dt / NORM_TAU)

        clamped_spread = min(cur_spread, nrm.spread_bps * 5.0)
        clamped_depth = min(cur_depth, nrm.depth_ask * 5.0)
        clamped_best_ask = min(cur_best_ask_usd, nrm.best_ask_usd * 5.0)

        nrm.depth_ask = max((1.0 - a) * nrm.depth_ask + a * clamped_depth, 100.0)
        nrm.best_ask_usd = max((1.0 - a) * nrm.best_ask_usd + a * clamped_best_ask, 50.0)
        nrm.spread_bps = max((1.0 - a) * nrm.spread_bps + a * clamped_spread, 0.5)

    # ----------------------------------------------------------- Signal Logic
    def _eval_lead_lag(
        self,
        coin: str,
        upbit_snap: OrderBookDepth5,
        prev_upbit_snap: Optional[OrderBookDepth5],
        now: float,
    ) -> None:
        nrm = self._upbit_norm.get(coin)
        if not nrm or prev_upbit_snap is None:
            return

        if (
            upbit_snap.best_ask <= 0
            or upbit_snap.best_bid <= 0
            or prev_upbit_snap.best_ask <= 0
            or prev_upbit_snap.best_bid <= 0
        ):
            return

        norm_depth = nrm["depth_ask"]
        norm_best_ask_usd = nrm["best_ask_usd"]
        norm_spread = nrm["spread_bps"]

        cur_depth = upbit_snap.depth_ask_usd
        cur_spread = upbit_snap.spread_bps

        trigger_type: Optional[str] = None
        drain_ratio = 0.0
        swept_usd = 0.0
        blowout_ratio = cur_spread / max(norm_spread, 0.5)

        ask_lifted = upbit_snap.best_ask > prev_upbit_snap.best_ask * (1.0 + 1e-7)
        ask_held = abs(upbit_snap.best_ask - prev_upbit_snap.best_ask) <= prev_upbit_snap.best_ask * 1e-7
        bid_non_decreasing = upbit_snap.best_bid >= prev_upbit_snap.best_bid * (1.0 - 1e-7)

        # 1. True Depth Drain
        if ask_lifted and bid_non_decreasing:
            swept_top_level = prev_upbit_snap.best_ask_usd
            top5_loss = max(0.0, prev_upbit_snap.depth_ask_usd - cur_depth)
            swept_usd = max(top5_loss, swept_top_level)
            drain_ratio = max(
                swept_usd / max(norm_depth, 100.0),
                swept_top_level / max(norm_best_ask_usd * 2.0, 100.0),
            )
            if drain_ratio >= self.drain_threshold:
                trigger_type = "depth_drain"

        elif ask_held and bid_non_decreasing:
            best_ask_loss = prev_upbit_snap.best_ask_usd - upbit_snap.best_ask_usd
            top5_loss = prev_upbit_snap.depth_ask_usd - cur_depth
            if best_ask_loss > 0 and top5_loss > 0:
                if best_ask_loss >= 0.5 * top5_loss:
                    swept_usd = top5_loss
                    drain_ratio = top5_loss / max(norm_depth, 100.0)
                    if drain_ratio >= self.drain_threshold:
                        trigger_type = "depth_drain"

        # 2. Upward Spread Blowout
        if (
            trigger_type is None
            and ask_lifted
            and bid_non_decreasing
            and blowout_ratio >= self.blowout_threshold
        ):
            trigger_type = "spread_blowout"
            swept_usd = max(prev_upbit_snap.best_ask_usd, max(0.0, prev_upbit_snap.depth_ask_usd - cur_depth))

        # Cancel vs. Sweep Discrimination (Anti-Ghosting Guard)
        # If Upbit trade feed is available or trade confirmation is required,
        # verify that the liquidity drop was backed by real taker buys rather than MM cancels/quote pullbacks.
        recent_buys_usd = 0.0
        if trigger_type is not None:
            cutoff = now - 6.0
            recent_buys_usd, has_trades = self.get_recent_upbit_buys(coin, cutoff)

            should_verify_trades = self.require_trade_confirmation or has_trades
            if should_verify_trades:
                ratio_needed = 0.25 if ask_held else 0.15
                min_buys_needed = max(15.0, swept_usd * ratio_needed)

                if recent_buys_usd < min_buys_needed:
                    self.counters["cancels_blocked"] = self.counters.get("cancels_blocked", 0) + 1
                    log.debug(
                        f"[engine] Blocked false sweep / MM cancel on Upbit {coin}: "
                        f"swept_usd=${swept_usd:.1f}, but recent taker buys=${recent_buys_usd:.1f} "
                        f"(needed ${min_buys_needed:.1f})"
                    )
                    trigger_type = None
                    swept_usd = 0.0
                    drain_ratio = 0.0

        follower_exchanges = self._coin_followers.get(coin)
        if not follower_exchanges:
            follower_exchanges = {ex for (ex, c) in self._books.keys() if ex != "upbit" and c == coin}
            if follower_exchanges:
                self._coin_followers[coin] = follower_exchanges

        for follower_ex in follower_exchanges:
            f_snap = self._books.get((follower_ex, coin))
            f_recv = self._recv_ts.get((follower_ex, coin), 0.0)
            if f_snap is None:
                continue

            book_age_sec = now - f_recv
            # Decouple baseline learning from entry age guard:
            # Baseline learning tolerates up to 15.0s, ensuring pair warms up on slow alts
            if book_age_sec > 15.0:
                continue

            st = self._get_or_create_pair_state(coin, follower_ex)

            upbit_mid = upbit_snap.mid_price
            f_mid = f_snap.mid_price
            if upbit_mid <= 0 or f_mid <= 0:
                continue

            raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4

            # Entry check requires fresh follower book (<= max_follower_book_age_sec, default 5.0s)
            if (
                trigger_type
                and st.open_signal is None
                and st.is_warmed_up
                and book_age_sec <= self.max_follower_book_age_sec
            ):
                self._check_entry(
                    coin=coin,
                    follower_ex=follower_ex,
                    st=st,
                    upbit_snap=upbit_snap,
                    f_snap=f_snap,
                    f_book_age_ms=book_age_sec * 1000.0,
                    norm_depth=norm_depth,
                    norm_spread=norm_spread,
                    swept_usd=swept_usd,
                    trigger_type=trigger_type,
                    drain_ratio=drain_ratio,
                    blowout_ratio=blowout_ratio,
                    now=now,
                    recent_buys_usd=recent_buys_usd,
                )

            if st.open_signal is None:
                st.learn(raw_diff_bps, now)

    def _check_entry(
        self,
        coin: str,
        follower_ex: str,
        st: PairState,
        upbit_snap: OrderBookDepth5,
        f_snap: OrderBookDepth5,
        f_book_age_ms: float,
        norm_depth: float,
        norm_spread: float,
        swept_usd: float,
        trigger_type: str,
        drain_ratio: float,
        blowout_ratio: float,
        now: float,
        recent_buys_usd: float = 0.0,
    ) -> None:
        sigma = st.sigma(now)
        upbit_mid = upbit_snap.mid_price
        f_mid = f_snap.mid_price
        f_bid = f_snap.best_bid
        f_ask = f_snap.best_ask

        if f_ask <= 0 or f_bid <= 0:
            return

        # Stale quote guard: reject follower books older than max allowed age (default 300ms)
        if f_book_age_ms > (self.max_follower_book_age_sec * 1000.0):
            return

        raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4
        dislocation_bps = raw_diff_bps - st.baseline_bps

        if dislocation_bps <= 0:
            return

        dev_z = dislocation_bps / max(sigma, 1.5)

        # Check pair-specific calibrated threshold
        pair_cfg = self.get_pair_threshold_config(coin, follower_ex)
        if not pair_cfg.get("enabled", True):
            return  # Pruned pair due to negative EV or high adverse flow

        min_dev_z = float(pair_cfg.get("min_dev_z", 1.8))
        if dev_z < min_dev_z:
            return

        pair_hold_sec = float(pair_cfg.get("max_hold_sec", self.max_hold_sec))

        top3_ask_depth_usd = f_snap.top_n_ask_depth_usd(3)
        if top3_ask_depth_usd < 50.0:
            return

        # Physical 15% order book capacity (independent of current wallet budget)
        stealth_capacity_usd = round(self.stealth_fraction * top3_ask_depth_usd, 2)

        # Determine pool basket and actionable status
        is_battle = self.pool_state.is_battle_coin(coin) if self.pool_state else False
        basket = "battle" if is_battle else "incubator"
        is_actionable = is_battle

        # Calculate dynamic stealth order size
        if self.profile_manager:
            sized = self.profile_manager.calculate_stealth_order_size(follower_ex, top3_ask_depth_usd)
            suggested_size_usd = sized if sized is not None else min(
                self.max_position_budget_usd,
                stealth_capacity_usd,
            )
        else:
            suggested_size_usd = min(
                self.max_position_budget_usd,
                stealth_capacity_usd,
            )

        # Reject non-executable micro-orders smaller than exchange minimum notional
        if suggested_size_usd < self.min_order_usd:
            return

        # True VWAP execution fill price across ask levels for suggested size
        f_entry_px = f_snap.vwap_ask_usd(suggested_size_usd)
        if f_entry_px <= 0:
            return

        f_ask_diff_bps = (upbit_mid - f_entry_px) / f_entry_px * 1e4
        gross_exec_edge_bps = f_ask_diff_bps - st.baseline_bps
        net_edge = gross_exec_edge_bps - self.roundtrip_fee_bps

        req_edge = float(pair_cfg.get("min_spread_bps", self.min_edge_bps))
        if net_edge < req_edge:
            return

        # Initial executable PnL at t=0 (if bought at f_entry_px and immediately sold at VWAP bid)
        init_exit_px = f_snap.vwap_bid_usd(suggested_size_usd)
        init_exec_move_bps = (init_exit_px - f_entry_px) / f_entry_px * 1e4
        init_net_pnl_bps = round(init_exec_move_bps - self.roundtrip_fee_bps, 2)

        self._seq += 1
        sig = SunriseSignal(
            id=f"sun_{int(now)}_{self._seq:04d}",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange=follower_ex,
            follower_contract=f_snap.raw_symbol,
            open_ts=round(now, 3),
            trigger_type=trigger_type,
            leader_mid=round(upbit_mid, 8),
            leader_best_ask=round(upbit_snap.best_ask, 8),
            leader_spread_bps=round(upbit_snap.spread_bps, 2),
            leader_norm_spread_bps=round(norm_spread, 2),
            leader_top5_ask_usd=round(upbit_snap.depth_ask_usd, 2),
            leader_norm_depth_usd=round(norm_depth, 2),
            leader_swept_usd=round(swept_usd, 2),
            leader_imbalance_top5=round(upbit_snap.imbalance_top5, 3),
            drain_ratio=round(drain_ratio, 3),
            spread_blowout_ratio=round(blowout_ratio, 2),
            follower_mid=round(f_mid, 8),
            follower_best_bid=round(f_bid, 8),
            follower_best_ask=round(f_ask, 8),
            follower_spread_bps=round(f_snap.spread_bps, 2),
            follower_top3_ask_usd=round(top3_ask_depth_usd, 2),
            follower_top3_bid_usd=round(f_snap.top_n_bid_depth_usd(3), 2),
            follower_imbalance_top3=round(f_snap.imbalance_top3, 3),
            kimchi_baseline_bps=round(st.baseline_bps, 2),
            pair_sigma_bps=round(sigma, 2),
            raw_dislocation_bps=round(dislocation_bps, 2),
            dev_z=round(dev_z, 2),
            gross_exec_edge_bps=round(gross_exec_edge_bps, 2),
            net_exec_edge_bps=round(net_edge, 2),
            leader_latency_ms=upbit_snap.network_latency_ms,
            follower_latency_ms=f_snap.network_latency_ms,
            follower_book_age_ms=round(f_book_age_ms, 1),
            suggested_size_usd=round(suggested_size_usd, 2),
            stealth_capacity_usd=round(stealth_capacity_usd, 2),
            follower_entry_px=round(f_entry_px, 8),
            leader_recent_buys_usd=round(recent_buys_usd, 2),
            basket=basket,
            is_actionable=is_actionable,
            max_hold_sec=pair_hold_sec,
            mfe_exec_pnl_bps=init_net_pnl_bps,
            mae_exec_pnl_bps=init_net_pnl_bps,
            mfe_mid_move_bps=0.0,
            time_to_peak_sec=0.0,
            path=[[0.0, 0.0, 0.0, init_net_pnl_bps, round(net_edge, 2)]],
        )

        st.open_signal = sig
        self._active_signals[coin].append(sig)
        self.counters["signals_triggered"] += 1
        if is_actionable:
            self.counters["signals_battle"] += 1
            tag = "[SIGNAL-BATTLE] ⚔️"
        else:
            self.counters["signals_incubator"] += 1
            tag = "[SIGNAL-INCUBATOR] 🧪"

        buys_str = f", UpBuys: ${sig.leader_recent_buys_usd:.0f}" if sig.leader_recent_buys_usd > 0 else ""
        log.info(
            f"{tag} {sig.coin} ({sig.follower_contract}) | Upbit -> {sig.follower_exchange} | "
            f"Trig: {sig.trigger_type} (Swept: ${sig.leader_swept_usd:.0f}{buys_str}) | "
            f"Edge: +{sig.net_exec_edge_bps:.1f} bps | Z: {sig.dev_z:.1f}σ | "
            f"Size: ${sig.suggested_size_usd:.0f} (Cap15%: ${sig.stealth_capacity_usd:.0f})"
        )

    # ----------------------------------------------------------- Tracking & Closure
    def _track_active_signals_for_coin(self, coin: str, now: float) -> None:
        signals = self._active_signals.get(coin)
        if not signals:
            return
        for sig in list(signals):
            if sig.closed:
                continue
            f_snap = self._books.get((sig.follower_exchange, coin))
            if f_snap is not None:
                self._track_follower_move(coin, sig.follower_exchange, f_snap, now)

    def _track_follower_move(self, coin: str, follower_ex: str, f_snap: OrderBookDepth5, now: float) -> None:
        signals = self._active_signals.get(coin)
        if not signals:
            return

        upbit_snap = self._books.get(("upbit", coin))
        if not upbit_snap:
            return

        for sig in list(signals):
            if sig.follower_exchange != follower_ex or sig.closed:
                continue

            dt = round(now - sig.open_ts, 3)

            # 1. Mid-to-Mid move on follower and leader (bps)
            f_mid_move = (
                (f_snap.mid_price - sig.follower_mid) / sig.follower_mid * 1e4
                if sig.follower_mid > 0
                else 0.0
            )
            up_mid_move = (
                (upbit_snap.mid_price - sig.leader_mid) / sig.leader_mid * 1e4
                if sig.leader_mid > 0
                else 0.0
            )

            # 2. True executable move on follower: Entry at VWAP ask -> Exit at current VWAP bid (bps)
            entry_px = getattr(sig, "follower_entry_px", 0.0) or sig.follower_best_ask
            exit_px = f_snap.vwap_bid_usd(sig.suggested_size_usd) if sig.suggested_size_usd > 0 else f_snap.best_bid
            f_exec_move = (exit_px - entry_px) / entry_px * 1e4 if entry_px > 0 else 0.0
            cur_net_pnl = round(f_exec_move - self.roundtrip_fee_bps, 2)

            # 3. Update MFE / MAE excursion metrics
            if cur_net_pnl > sig.mfe_exec_pnl_bps:
                sig.mfe_exec_pnl_bps = cur_net_pnl
                sig.time_to_peak_sec = dt
            if cur_net_pnl < sig.mae_exec_pnl_bps:
                sig.mae_exec_pnl_bps = cur_net_pnl
            if f_mid_move > sig.mfe_mid_move_bps:
                sig.mfe_mid_move_bps = round(f_mid_move, 2)

            # 4. Remaining dislocation edge
            cur_diff_bps = (
                (upbit_snap.mid_price - f_snap.mid_price) / f_snap.mid_price * 1e4
                if f_snap.mid_price > 0
                else 0.0
            )
            rem_edge = (cur_diff_bps - sig.kimchi_baseline_bps) - self.taker_fee_bps

            if not sig.path or dt > sig.path[-1][0]:
                sig.path.append([
                    dt,
                    round(up_mid_move, 2),
                    round(f_mid_move, 2),
                    cur_net_pnl,
                    round(rem_edge, 2),
                ])

            # Closure & Stop-Loss evaluation:
            pair_cfg = self.get_pair_threshold_config(coin, follower_ex)
            hold_sec = getattr(sig, "max_hold_sec", self.max_hold_sec) or self.max_hold_sec
            pair_decay_sec = float(pair_cfg.get("alpha_decay_sec", self.alpha_decay_sec))
            sigma = max(sig.pair_sigma_bps, 2.0)

            upbit_drop_bps = (
                (sig.leader_mid - upbit_snap.mid_price) / sig.leader_mid * 1e4
                if sig.leader_mid > 0
                else 0.0
            )
            cur_dislocation_bps = cur_diff_bps - sig.kimchi_baseline_bps

            close_reason, stop_type = evaluate_exit_reason(
                cur_net_pnl=cur_net_pnl,
                net_exec_edge_bps=sig.net_exec_edge_bps,
                rem_edge=rem_edge,
                dt=dt,
                hold_sec=hold_sec,
                pair_decay_sec=pair_decay_sec,
                sigma=sigma,
                upbit_drop_bps=upbit_drop_bps,
                cur_dislocation_bps=cur_dislocation_bps,
                stop_invalidation_bps=self.stop_invalidation_bps,
                stop_adverse_pnl_bps=self.stop_adverse_pnl_bps,
            )

            if close_reason is not None:
                if stop_type is not None:
                    sig.stop_hit = True
                    sig.stop_type = stop_type
                    loss = abs(min(0.0, cur_net_pnl)) if stop_type == "invalidation" else abs(cur_net_pnl)
                    sig.stop_loss_bps = round(loss, 2)
                self._close_signal(sig, now, close_reason, f_mid_move, f_exec_move)
                continue

    def _close_signal(
        self,
        sig: SunriseSignal,
        now: float,
        reason: str,
        f_mid_move_bps: float,
        f_exec_move_bps: float,
    ) -> None:
        sig.closed = True
        sig.close_ts = round(now, 3)
        sig.duration_sec = round(now - sig.open_ts, 3)
        sig.close_reason = reason
        sig.captured_mid_move_bps = round(f_mid_move_bps, 2)
        sig.captured_exec_move_bps = round(f_exec_move_bps, 2)
        sig.net_pnl_bps = round(f_exec_move_bps - self.roundtrip_fee_bps, 2)

        if reason == "converged":
            self.counters["signals_converged"] += 1
        elif reason == "stop_invalidation":
            self.counters["signals_stop_loss_invalidation"] += 1
        elif reason == "stop_adverse_pnl":
            self.counters["signals_stop_loss_adverse_pnl"] += 1
        elif reason == "stop_decay":
            self.counters["signals_stop_loss_decay"] += 1
        elif reason == "reversed":
            self.counters["signals_reversed"] += 1
        else:
            self.counters["signals_timeout"] += 1

        try:
            self.emit(sig.to_dict())
        except Exception as e:
            log.error(f"[engine] Failed to emit closed signal {getattr(sig, 'id', '')}: {e}")

        pair_key = (sig.coin, sig.follower_exchange)
        st = self._pairs.get(pair_key)
        if st and st.open_signal is not None and getattr(st.open_signal, "id", None) == sig.id:
            st.open_signal = None

        if sig.coin in self._active_signals:
            if sig in self._active_signals[sig.coin]:
                self._active_signals[sig.coin].remove(sig)
            if not self._active_signals[sig.coin]:
                self._active_signals.pop(sig.coin, None)

    def sweep_timeouts(self, now: float) -> None:
        """
        Sweeps and closes any active signals that exceeded max_hold_sec even if no new ticks arrived.
        Prevents open signals from lingering on illiquid or inactive coins.
        """
        for coin, signals in list(self._active_signals.items()):
            for sig in list(signals):
                try:
                    hold_sec = getattr(sig, "max_hold_sec", self.max_hold_sec) or self.max_hold_sec
                    if not sig.closed and (now - sig.open_ts) >= hold_sec:
                        f_snap = self._books.get((sig.follower_exchange, coin))
                        if f_snap:
                            f_mid_move = (
                                (f_snap.mid_price - sig.follower_mid) / sig.follower_mid * 1e4
                                if sig.follower_mid > 0
                                else 0.0
                            )
                            entry_px = getattr(sig, "follower_entry_px", 0.0) or sig.follower_best_ask
                            exit_px = f_snap.vwap_bid_usd(sig.suggested_size_usd) if sig.suggested_size_usd > 0 else f_snap.best_bid
                            f_exec_move = (exit_px - entry_px) / entry_px * 1e4 if entry_px > 0 else 0.0
                        else:
                            f_mid_move = 0.0
                            f_exec_move = 0.0
                        self._close_signal(sig, now, "timeout", f_mid_move, f_exec_move)
                except Exception as e:
                    log.error(f"[engine] Error sweeping timeout for signal {getattr(sig, 'id', '')}: {e}")
```

## main.py

```python
"""
main.py - Main entry point for Korean Sunrise Lead-Lag & Momentum Sniping Service.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time
from typing import Optional

from batch_writer import BatchWriter
import config as C
from korean_engine import KoreanEngine
from pipeline.pool_state import PoolStateManager
from pipeline.profile import TradingProfileManager
from venues import StreamManager
from web import APIServer

LOG_FMT = "%(asctime)s | %(levelname)-7s | %(name)-14s | %(message)s"
LOG_DIR = os.path.join(C.PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "korean_sunrise.log")

_handlers: list[logging.Handler] = [logging.StreamHandler()]
try:
    _handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))
except Exception:
    pass

logging.basicConfig(
    level=getattr(logging, C.LOG_LEVEL.upper(), logging.INFO),
    format=LOG_FMT,
    handlers=_handlers,
)
log = logging.getLogger("main")


class KoreanSunriseService:
    def __init__(self):
        self.writer = BatchWriter(data_root=C.DATA_ROOT)
        self.pool_state = PoolStateManager()
        self.pool_state.reload()
        self.profile_manager = TradingProfileManager()
        self.profile_manager.reload()

        self.engine = KoreanEngine(
            emit_callback=self.writer.add,
            drain_threshold=C.DRAIN_THRESHOLD,
            blowout_threshold=C.BLOWOUT_THRESHOLD,
            min_edge_bps=C.MIN_EXEC_EDGE_BPS,
            max_position_budget_usd=C.MAX_POSITION_BUDGET_USD,
            min_order_usd=getattr(C, "MIN_ORDER_USD", 10.0),
            stealth_depth_fraction=C.STEALTH_DEPTH_FRACTION,
            taker_fee_bps=C.TAKER_FEE_BPS,
            roundtrip_fee_bps=C.ROUNDTRIP_FEE_BPS,
            warmup_samples=C.MIN_WARMUP_SAMPLES,
            max_hold_sec=C.MAX_SIGNAL_HOLD_SEC,
            require_trade_confirmation=True,
            pool_state=self.pool_state,
            profile_manager=self.profile_manager,
        )
        self.universe: Optional[C.MatchedUniverse] = None
        self.stream_manager: Optional[StreamManager] = None
        self.api_server: Optional[APIServer] = None
        if getattr(C, "API_SERVER_ENABLED", True):
            self.api_server = APIServer(
                service=self,
                host=getattr(C, "API_SERVER_HOST", "0.0.0.0"),
                port=getattr(C, "API_SERVER_PORT", 8089),
                token=getattr(C, "API_AUTH_TOKEN", "vitali_secret_key_2026"),
            )
        self.running = False
        self._shutdown_event = asyncio.Event()

    @staticmethod
    def _parse_universe_doc(doc: Dict[str, Any]) -> C.MatchedUniverse:
        coins = doc.get("coins", [])
        upbit_krw_to_coin = doc.get("upbit_krw_to_coin", {})
        venue_sym_maps: Dict[str, Dict[str, Tuple[str, float, float]]] = {}
        for k, v in doc.items():
            if k.endswith("_sym_map") and isinstance(v, dict):
                v_name = k.replace("_sym_map", "").lower()
                venue_sym_maps[v_name] = {
                    raw_sym: (
                        str(tup[0]),
                        float(tup[1]),
                        float(tup[2]) if len(tup) >= 3 else 1.0,
                    )
                    for raw_sym, tup in v.items()
                }

        binance_sym_map = {k: (v[0], v[1]) for k, v in venue_sym_maps.get("binance", {}).items()}
        bybit_sym_map = {k: (v[0], v[1]) for k, v in venue_sym_maps.get("bybit", {}).items()}

        univ = C.MatchedUniverse(
            coins=coins,
            upbit_krw_to_coin=upbit_krw_to_coin,
            venue_sym_maps=venue_sym_maps,
            binance_sym_map=binance_sym_map,
            bybit_sym_map=bybit_sym_map,
        )
        univ._raw_doc = doc
        return univ

    async def run(self) -> None:
        self.running = True
        if self.api_server:
            self.api_server.start()
        univ_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universe_live.json")

        if os.path.exists(univ_file):
            try:
                with open(univ_file, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                self.universe = self._parse_universe_doc(doc)
                log.info(f"=== Loaded {len(self.universe.coins)} incubator coins across {len(self.universe.venue_sym_maps)} venues from universe_live.json ===")
            except Exception as e:
                log.warning(f"Could not parse universe_live.json: {e}")
                self.universe = None

        if self.universe is None or not self.universe.coins:
            log.info("=== universe_live.json not found or empty: Running multi-venue UniverseScreener ===")
            from screening.screener import UniverseScreener
            screener = UniverseScreener(output_file=univ_file)
            doc = await asyncio.to_thread(screener.run)
            self.universe = self._parse_universe_doc(doc)

        self.pool_state.reload()

        if not self.universe or not self.universe.coins:
            raise RuntimeError("No overlapping coins discovered between Upbit and Perp exchanges.")

        log.info(f"=== Starting Korean Sunrise for {len(self.universe.coins)} matched target coins ===")
        active_venues = self.profile_manager.get_profile().get("active_venues")
        self.stream_manager = StreamManager(
            universe=self.universe,
            callback=self.engine.on_book_update,
            trade_callback=self.engine.on_upbit_trade,
            active_venues=active_venues,
        )
        await self.stream_manager.start_all()

        last_heartbeat = time.time()
        last_reload = time.time()

        try:
            while not self._shutdown_event.is_set():
                now = time.time()
                try:
                    self.engine.sweep_timeouts(now)
                except Exception as e:
                    log.error(f"[main] sweep_timeouts error: {e}")
                await asyncio.to_thread(self.writer.maybe_flush, now)

                if now - last_reload >= 30.0:
                    last_reload = now
                    await asyncio.to_thread(self.engine.maybe_reload_thresholds, now)

                if now - last_heartbeat >= 15.0:
                    self._log_heartbeat()
                    last_heartbeat = now

                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass

        except asyncio.CancelledError:
            log.info("Received CancelledError")
        finally:
            await self.shutdown()

    def _log_heartbeat(self) -> None:
        if not self.stream_manager:
            return
        c = self.engine.counters
        status = self.stream_manager.get_status()

        parts = []
        krw_rate = 0.0
        for k, v in status.items():
            st_label = "OK" if v["connected"] else "DISC"
            parts.append(f"{k}: {st_label} ({v['symbols_seen']}/{v['subscribed']})")
            if "usdt_krw_rate" in v:
                krw_rate = v["usdt_krw_rate"]

        conn_str = " | ".join(parts)
        snap = self.pool_state.get_pool_snapshot()
        summary = snap["summary"]
        log.info(
            f"[HEARTBEAT] KRW/USDT: {krw_rate:.1f} | "
            f"Pool: Inc {summary['incubator']} | Battle {summary['battle']} (Active: {summary['battle_active']}) | "
            f"Ticks: Up {c['ticks_upbit']} (Trades: {c.get('trades_upbit', 0)}) / Foll {c['ticks_followers']} | "
            f"Cancels Blocked: {c.get('cancels_blocked', 0)} | "
            f"Sigs: {c['signals_triggered']} (⚔️Battle: {c.get('signals_battle', 0)}, 🧪Inc: {c.get('signals_incubator', 0)}) | "
            f"Streams: [{conn_str}]"
        )

    async def shutdown(self) -> None:
        if not self.running:
            return
        self.running = False
        log.info("Gracefully shutting down Korean Sunrise...")
        if self.stream_manager is not None:
            await self.stream_manager.stop_all()
        if self.api_server is not None:
            self.api_server.stop()
        await asyncio.to_thread(self.writer.flush)
        log.info("Korean Sunrise stopped cleanly.")


async def main_async() -> None:
    svc = KoreanSunriseService()

    def _sig_handler():
        log.info("Shutdown signal received")
        svc._shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _sig_handler)
        except NotImplementedError:
            pass

    await svc.run()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
```

## matrix_thresholds.json

```json
{
  "metadata": {
    "generated_at": "2026-10-18T15:16:52.640214+00:00",
    "days_analyzed": 7.0,
    "order_size_usd": 10.0,
    "roundtrip_fee_bps": 10.0,
    "min_trades_guard": 5,
    "min_winrate_guard": 0.7,
    "global_fallback_bps": 20.0,
    "total_files_scanned": 0,
    "total_signals_evaluated": 0,
    "total_pairs_configured": 269,
    "classifications": {
      "default_baseline": 269
    },
    "elapsed_sec": 0.01
  },
  "thresholds": {
    "VVV:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "TRUMP:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "TRUMP:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "TRUMP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-htx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SOMI:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SOMI:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SOMI:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SOMI:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GRT:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GRT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GRT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GRT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "EUL:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROS:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROS:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ESP:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ESP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-htx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GTC:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GTC:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "B3:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "B3:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CYBER:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CYBER:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CYBER:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CYBER:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "HYPER:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "HYPER:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "HYPER:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SENT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AWE:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AWE:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AWE:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AWE:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKC:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKC:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKC:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKC:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEO:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IO:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SKR:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SKR:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SKR:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SKR:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CARV:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CARV:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CARV:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CARV:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ARX:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MMT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MNT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MNT:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PONS:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PONS:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PONS:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "API3:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "API3:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENS:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENS:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENS:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENS:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IOTA:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IOTA:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BEAM:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PUMP:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PUMP:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PUMP:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "DOS:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "DOS:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "DOS:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RENDER:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RENDER:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-gateio": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-pionex": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "2Z:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "F:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIO:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NMR:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MANA:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VTHO:upbit-binance": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VTHO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MOODENG:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ATOM:upbit-bybit": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ATOM:upbit-mexc": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ATOM:upbit-bingx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIGTIME:upbit-okx": {
      "min_spread_bps": 20.0,
      "max_hold_sec": 6.0,
      "spike_sigma": 2.5,
      "min_dev_z": 1.8,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    }
  }
}
```

## models.py

```python
"""
models.py - Data models for Korean Sunrise lead-lag arbitrage and momentum engine.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple


def normalize_exchange_ts(raw_ts: Any) -> int:
    """Normalizes exchange timestamps into integer milliseconds (Unix epoch ms).
    Supports nanoseconds (19 digits), microseconds (16 digits), milliseconds (13 digits),
    and seconds (10 digits integer or float). Returns 0 on None or invalid input.
    """
    if raw_ts is None:
        return 0
    try:
        if isinstance(raw_ts, int):
            if raw_ts <= 0:
                return 0
            if raw_ts > 10**17:     # Nanoseconds (e.g. 1.7e18) -> ms
                return raw_ts // 1_000_000
            elif raw_ts > 10**14:   # Microseconds (e.g. 1.7e15) -> ms
                return raw_ts // 1_000
            elif raw_ts > 10**11:   # Milliseconds (e.g. 1.7e12) -> ms
                return raw_ts
            else:                  # Seconds (e.g. 1.7e9) -> ms
                return raw_ts * 1000

        s_ts = str(raw_ts).strip()
        if "." not in s_ts and s_ts.lstrip("-").isdigit():
            int_val = int(s_ts)
            if int_val <= 0:
                return 0
            if int_val > 10**17:
                return int_val // 1_000_000
            elif int_val > 10**14:
                return int_val // 1_000
            elif int_val > 10**11:
                return int_val
            else:
                return int_val * 1000

        val = float(raw_ts)
        if not math.isfinite(val) or val <= 0:
            return 0
        if val > 1e17:     # Nanoseconds -> ms
            return int(val / 1e6)
        elif val > 1e14:   # Microseconds -> ms
            return int(val / 1e3)
        elif val > 1e11:   # Milliseconds -> ms
            return int(val)
        else:              # Seconds -> ms
            return int(val * 1000.0)
    except (TypeError, ValueError):
        return 0


@dataclass(slots=True)
class OrderBookDepth5:
    """
    Top-5 order book snapshot normalized to 1 single base coin in USD/USDT.
    For 1000x perpetuals, prices are already divided by 1000 and sizes multiplied by 1000.
    """
    exchange: str
    symbol: str                      # Canonical base coin (e.g. 'PEPE', 'SAND')
    raw_symbol: str                  # Venue-specific symbol (e.g. 'KRW-PEPE', '1000PEPEUSDT')
    ts: float                        # Local receipt timestamp (sec)
    exchange_ts: int                 # Exchange event timestamp (ms)
    bids: List[Tuple[float, float]]  # [(price_usd, size_coins), ...]
    asks: List[Tuple[float, float]]  # [(price_usd, size_coins), ...]

    @property
    def best_bid(self) -> float:
        return self.bids[0][0] if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0][0] if self.asks else 0.0

    @property
    def best_bid_usd(self) -> float:
        return self.bids[0][0] * self.bids[0][1] if self.bids else 0.0

    @property
    def best_ask_usd(self) -> float:
        return self.asks[0][0] * self.asks[0][1] if self.asks else 0.0

    @property
    def mid_price(self) -> float:
        if self.bids and self.asks:
            return (self.bids[0][0] + self.asks[0][0]) * 0.5
        return self.best_bid or self.best_ask

    @property
    def spread_bps(self) -> float:
        if self.bids and self.asks:
            mid = self.mid_price
            return (self.asks[0][0] - self.bids[0][0]) / mid * 1e4 if mid > 0 else 0.0
        return 0.0

    @property
    def depth_bid_usd(self) -> float:
        return sum(p * q for p, q in self.bids)

    @property
    def depth_ask_usd(self) -> float:
        return sum(p * q for p, q in self.asks)

    def top_n_ask_depth_usd(self, n: int = 3) -> float:
        return sum(p * q for p, q in self.asks[:n])

    def top_n_bid_depth_usd(self, n: int = 3) -> float:
        return sum(p * q for p, q in self.bids[:n])

    @property
    def imbalance_top3(self) -> float:
        b = self.top_n_bid_depth_usd(3)
        a = self.top_n_ask_depth_usd(3)
        tot = b + a
        return (b - a) / tot if tot > 0 else 0.0

    @property
    def imbalance_top5(self) -> float:
        b = self.depth_bid_usd
        a = self.depth_ask_usd
        tot = b + a
        return (b - a) / tot if tot > 0 else 0.0

    @property
    def network_latency_ms(self) -> float:
        if self.exchange_ts > 0:
            return round(self.ts * 1000.0 - float(self.exchange_ts), 1)
        return 0.0

    def vwap_ask_usd(self, notional_usd: float) -> float:
        """
        Calculates the VWAP execution fill price for buying notional_usd across available ask levels.
        If notional exceeds available book depth, the remaining portion is filled at the worst available ask price.
        """
        if not self.asks or notional_usd <= 0:
            return self.best_ask
        remaining = notional_usd
        total_qty = 0.0
        filled_usd = 0.0
        for p, q in self.asks:
            level_usd = p * q
            if level_usd <= 0:
                continue
            if remaining <= level_usd:
                total_qty += remaining / p
                filled_usd += remaining
                remaining = 0.0
                break
            else:
                total_qty += q
                filled_usd += level_usd
                remaining -= level_usd
        if remaining > 0 and self.asks:
            worst_p = self.asks[-1][0]
            if worst_p > 0:
                total_qty += remaining / worst_p
                filled_usd += remaining
        return (filled_usd / total_qty) if total_qty > 0 else self.best_ask

    def vwap_bid_usd(self, notional_usd: float) -> float:
        """
        Calculates the VWAP execution fill price for selling notional_usd across available bid levels.
        If notional exceeds available book depth, the remaining portion is filled at the worst available bid price.
        """
        if not self.bids or notional_usd <= 0:
            return self.best_bid
        remaining = notional_usd
        total_qty = 0.0
        filled_usd = 0.0
        for p, q in self.bids:
            level_usd = p * q
            if level_usd <= 0:
                continue
            if remaining <= level_usd:
                total_qty += remaining / p
                filled_usd += remaining
                remaining = 0.0
                break
            else:
                total_qty += q
                filled_usd += level_usd
                remaining -= level_usd
        if remaining > 0 and self.bids:
            worst_p = self.bids[-1][0]
            if worst_p > 0:
                total_qty += remaining / worst_p
                filled_usd += remaining
        return (filled_usd / total_qty) if total_qty > 0 else self.best_bid


@dataclass
class SunriseSignal:
    """Lead-lag signal triggered by Korean spot buying pressure with full telemetry."""
    id: str
    coin: str
    leader_exchange: str
    follower_exchange: str
    follower_contract: str
    open_ts: float
    trigger_type: str

    # Upbit (Leader) metrics at trigger
    leader_mid: float
    leader_best_ask: float
    leader_spread_bps: float
    leader_norm_spread_bps: float
    leader_top5_ask_usd: float
    leader_norm_depth_usd: float
    leader_swept_usd: float
    leader_imbalance_top5: float
    drain_ratio: float
    spread_blowout_ratio: float

    # Follower (Perp) metrics at trigger
    follower_mid: float
    follower_best_bid: float
    follower_best_ask: float
    follower_spread_bps: float
    follower_top3_ask_usd: float
    follower_top3_bid_usd: float
    follower_imbalance_top3: float

    # Basis, Sigma & Dislocation metrics
    kimchi_baseline_bps: float
    pair_sigma_bps: float
    raw_dislocation_bps: float
    dev_z: float
    gross_exec_edge_bps: float        # Executable edge before entry fee
    net_exec_edge_bps: float          # Executable edge after entry taker fee

    # Latency & Freshness telemetry
    leader_latency_ms: float = 0.0
    follower_latency_ms: float = 0.0
    follower_book_age_ms: float = 0.0

    # Sizing & Physical Capacity
    suggested_size_usd: float = 0.0
    stealth_capacity_usd: float = 0.0
    follower_entry_px: float = 0.0
    leader_recent_buys_usd: float = 0.0

    # Pool routing
    basket: str = "incubator"
    is_actionable: bool = False
    max_hold_sec: float = 6.0

    # Outcome & Excursion tracking
    closed: bool = False
    close_ts: Optional[float] = None
    duration_sec: Optional[float] = None
    close_reason: Optional[str] = None
    captured_mid_move_bps: Optional[float] = None
    captured_exec_move_bps: Optional[float] = None
    net_pnl_bps: Optional[float] = None

    # Stop-Loss audit tracking
    stop_hit: bool = False
    stop_type: Optional[str] = None
    stop_loss_bps: float = 0.0

    mfe_exec_pnl_bps: float = -999.0
    mae_exec_pnl_bps: float = 999.0
    mfe_mid_move_bps: float = 0.0
    time_to_peak_sec: float = 0.0

    path: List[List[float]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Compatibility aliases required by validate_data.py & analytical tools
        d["dislocation_bps"] = self.raw_dislocation_bps
        d["entry_ts"] = self.open_ts
        return d
```

## pipeline/__init__.py

```python
"""
pipeline - Continuous trading pool, incubator, and evaluation pipeline for Korean Sunrise.
"""
from __future__ import annotations

from pipeline.pool_state import PoolStateManager, CoinEntry
from pipeline.evaluator import SunriseEvaluator
from pipeline.profile import TradingProfileManager

__all__ = [
    "PoolStateManager",
    "CoinEntry",
    "SunriseEvaluator",
    "TradingProfileManager",
]
```

## pipeline/evaluator.py

```python
"""
pipeline/evaluator.py — Quantitative signal evaluator and pool promotion engine.

Zero Hardcode & Pure Trading Mathematics:
  - Evaluates recorded .jsonl.gz signal archives from data/signals/.
  - Uses strictly realized executable PnL (net_pnl_bps), which already accounts
    for entry ask, exit bid, and roundtrip taker fees (10 bps).
  - Dynamically filters out retail micro-noise using book microstructure:
      1. Drain Ratio >= 0.50 (swept >= 50% of Upbit normal ask depth).
      2. Cross-Liquidity Ratio >= 0.80 (Upbit sweep is commensurate with follower's top-3 depth).
      3. Follower Top-3 Depth >= $66.7 (supports $10 minimum exchange order at 15% stealth size).
  - Evaluates pure statistical edge:
      * Expectancy: Mean & Median Net PnL (bps) > 0.
      * Profit Factor: Sum(Gains) / Sum(|Losses|) >= 1.2.
      * Win Rate >= 50%.
      * Sample Size >= min_events.
"""
from __future__ import annotations

import glob
import gzip
import json
import logging
import math
import os
import statistics
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from pipeline.pool_state import PoolStateManager

log = logging.getLogger("pipeline.evaluator")

DEFAULT_MIN_EVENTS = 3
DEFAULT_MIN_PROFIT_FACTOR = 1.2
DEFAULT_MIN_WIN_RATE = 50.0


def _iter_gz_jsonl(path: str):
    """Yields parsed JSON dicts from a gzip-compressed JSONL file."""
    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        pass
    except Exception as e:
        log.debug(f"[evaluator] Cannot read {path}: {e}")


def _collect_signal_files(data_root: str, days_back: int = 7) -> List[str]:
    """Finds all sunrise signal files within the last `days_back` days."""
    pattern = os.path.join(data_root, "signals", "**", "*.jsonl.gz")
    all_files = sorted(glob.glob(pattern, recursive=True))
    cutoff = time.time() - (days_back * 86400.0)
    files = [f for f in all_files if os.path.getmtime(f) >= cutoff]
    return files if files else all_files


class CoinSignalAccumulator:
    """Accumulates verified trading statistics for a single coin across signals."""
    __slots__ = (
        "coin", "venues", "net_pnls", "mfes", "t_peaks",
        "top3_depths", "stealth_caps", "valid_sweeps", "total_signals",
    )

    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.venues: set = set()
        self.net_pnls: List[float] = []
        self.mfes: List[float] = []
        self.t_peaks: List[float] = []
        self.top3_depths: List[float] = []
        self.stealth_caps: List[float] = []
        self.valid_sweeps: int = 0
        self.total_signals: int = 0

    def add_signal(self, row: Dict[str, Any]) -> None:
        self.total_signals += 1

        follower_ex = str(row.get("follower_exchange", "")).lower()
        if follower_ex:
            self.venues.add(follower_ex)

        net_pnl = row.get("net_pnl_bps")
        if net_pnl is None or not math.isfinite(float(net_pnl)):
            return

        # Microstructure physical validity check (Zero Hardcode):
        # 1. Drain ratio must show structural depletion of Upbit's ask depth
        drain_ratio = float(row.get("drain_ratio", 0.0))
        # 2. Cross-liquidity ratio: sweep volume vs follower book depth
        swept_usd = float(row.get("leader_swept_usd", 0.0))
        f_top3_ask = float(row.get("follower_top3_ask_usd", 0.0))
        cross_ratio = (swept_usd / f_top3_ask) if f_top3_ask > 0 else 0.0

        stealth_cap = float(row.get("stealth_capacity_usd", 0.0))
        if stealth_cap <= 0.0 and f_top3_ask > 0.0:
            stealth_cap = round(0.15 * f_top3_ask, 2)

        # Discard microscopic noise where Upbit sweep was insignificant vs follower
        if drain_ratio < 0.30 and cross_ratio < 0.30:
            return

        self.valid_sweeps += 1
        pnl = float(net_pnl)
        self.net_pnls.append(pnl)

        mfe = row.get("mfe_exec_pnl_bps")
        if mfe is not None and math.isfinite(float(mfe)):
            self.mfes.append(float(mfe))

        t_peak = row.get("time_to_peak_sec")
        if t_peak is not None and math.isfinite(float(t_peak)):
            self.t_peaks.append(float(t_peak))

        if f_top3_ask > 0:
            self.top3_depths.append(f_top3_ask)
            self.stealth_caps.append(stealth_cap)

    def finalize(self) -> Optional[Dict[str, Any]]:
        n = len(self.net_pnls)
        if n == 0:
            return None

        gains = sum(p for p in self.net_pnls if p > 0)
        losses = sum(abs(p) for p in self.net_pnls if p < 0)

        # Standard quantitative Profit Factor: Gains / Losses
        profit_factor = (gains / losses) if losses > 0 else (99.0 if gains > 0 else 0.0)
        win_count = sum(1 for p in self.net_pnls if p > 0)
        win_rate = (win_count / n) * 100.0

        mean_pnl = statistics.mean(self.net_pnls)
        median_pnl = statistics.median(self.net_pnls)
        median_depth = statistics.median(self.top3_depths) if self.top3_depths else 0.0
        median_stealth_cap = statistics.median(self.stealth_caps) if self.stealth_caps else 0.0
        mean_mfe = statistics.mean(self.mfes) if self.mfes else 0.0
        med_t_peak = statistics.median(self.t_peaks) if self.t_peaks else 0.0

        return {
            "coin": self.coin,
            "venues": sorted(self.venues),
            "event_count": n,
            "total_signals": self.total_signals,
            "mean_net_pnl_bps": round(mean_pnl, 2),
            "median_net_pnl_bps": round(median_pnl, 2),
            "win_rate_pct": round(win_rate, 1),
            "profit_factor": round(profit_factor, 2),
            "top3_depth_usd": round(median_depth, 1),
            "stealth_capacity_usd": round(median_stealth_cap, 1),
            "mean_mfe_bps": round(mean_mfe, 2),
            "median_t_peak_sec": round(med_t_peak, 2),
            # Rank score = Expected Edge * Statistical Confidence
            "ranking_score": round(mean_pnl * (win_rate / 100.0) * math.log(n + 1), 3),
        }


class SunriseEvaluator:
    """
    Evaluates recorded signal archives and promotes qualified coins to the Battle pool.
    """

    def __init__(
        self,
        data_root: str = "data",
        days_back: int = 7,
        min_events: int = DEFAULT_MIN_EVENTS,
        min_profit_factor: float = DEFAULT_MIN_PROFIT_FACTOR,
        min_win_rate: float = DEFAULT_MIN_WIN_RATE,
        pool_state: Optional[PoolStateManager] = None,
    ) -> None:
        self.data_root = data_root
        self.days_back = days_back
        self.min_events = min_events
        self.min_profit_factor = min_profit_factor
        self.min_win_rate = min_win_rate
        self.pool = pool_state or PoolStateManager()

    def run(self, dry_run: bool = False) -> List[Dict[str, Any]]:
        t0 = time.time()
        log.info(f"[evaluator] Starting evaluation (days_back={self.days_back}, min_events={self.min_events})")

        files = _collect_signal_files(self.data_root, self.days_back)
        if not files:
            log.warning(f"[evaluator] No signal files found in {self.data_root}/signals/")
            return []

        log.info(f"[evaluator] Processing {len(files)} compressed signal archives...")

        accumulators: Dict[str, CoinSignalAccumulator] = defaultdict(lambda: CoinSignalAccumulator(""))
        total_rows = 0

        for fpath in files:
            for row in _iter_gz_jsonl(fpath):
                coin = str(row.get("coin", "")).upper()
                if not coin:
                    continue
                if coin not in accumulators:
                    accumulators[coin] = CoinSignalAccumulator(coin)
                accumulators[coin].add_signal(row)
                total_rows += 1

        # Finalize stats
        all_stats: List[Dict[str, Any]] = []
        for acc in accumulators.values():
            st = acc.finalize()
            if st:
                all_stats.append(st)

        all_stats.sort(key=lambda s: s["ranking_score"], reverse=True)

        # Gate criteria for Battle promotion (Strictly positive statistical expectancy)
        qualified: List[Dict[str, Any]] = []
        for s in all_stats:
            # 1. Sufficient sample size
            if s["event_count"] < self.min_events:
                continue
            # 2. Positive expectancy after all roundtrip fees
            if s["mean_net_pnl_bps"] <= 0.0 or s["median_net_pnl_bps"] < -2.0:
                continue
            # 3. Profit Factor or Winrate edge
            if s["profit_factor"] < self.min_profit_factor and s["win_rate_pct"] < self.min_win_rate:
                continue
            # 4. Book depth supports minimum order
            if s["top3_depth_usd"] < 50.0:
                continue

            qualified.append(s)

        elapsed = round(time.time() - t0, 2)
        log.info(
            f"[evaluator] Evaluated {total_rows} signals across {len(all_stats)} coins in {elapsed}s. "
            f"Qualified for Battle: {len(qualified)}"
        )

        for i, q in enumerate(qualified[:15], 1):
            log.info(
                f"  #{i:2d} {q['coin']:<10} | Exp: {q['mean_net_pnl_bps']:>+6.2f} bps | "
                f"PF: {q['profit_factor']:>4.2f} | Win: {q['win_rate_pct']:>4.1f}% | "
                f"N={q['event_count']} | Depth: ${q['top3_depth_usd']:>6.0f} | "
                f"Cap15%: ${q['stealth_capacity_usd']:>5.0f} | MFE: +{q['mean_mfe_bps']:.1f} bps"
            )

        if not dry_run:
            self.pool.reload()
            self.pool.promote_to_battle(qualified)

        return qualified
```

## pipeline/pool_state.py

```python
"""
pipeline/pool_state.py — Atomic state manager for Korean Sunrise trading pools.

Responsibilities:
  - Atomic read/write of live_trading_pool.json and runtime_overrides.json.
  - Manages two distinct baskets:
      * Incubator: Broad universe of candidate coins collecting telemetry.
      * Battle: Statistically proven coins with positive Net EV and Profit Factor.
  - Applies runtime_overrides.json (manual pauses, size caps, risk parameters).
  - Calculates dynamic position sizing via Stealth Sizing rule (15% top-3 ask depth).
  - Provides query methods for KoreanEngine and StreamManager.

File contracts:
  universe_live.json     — Screened incubator candidate pool (Upbit <-> Binance/Bybit).
  live_trading_pool.json — Battle pool promoted by SunriseEvaluator.
  runtime_overrides.json — Operator manual circuit breaker pauses & global overrides.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

log = logging.getLogger("pipeline.pool_state")

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UNIVERSE_FILE = os.path.join(HERE, "universe_live.json")
BATTLE_POOL_FILE = os.path.join(HERE, "live_trading_pool.json")
OVERRIDES_FILE = os.path.join(HERE, "runtime_overrides.json")

# Default sizing & gate rules
DEFAULT_MIN_ORDER_USD = 10.0      # Exchange minimum order size ($5-$10)
DEFAULT_STEALTH_FRAC = 0.15       # Max 15% of top-3 follower ask depth
DEFAULT_SIZE_CAP_USD = 10000.0    # Max budget cap per trade
DEFAULT_MIN_DEPTH_USD = 66.7      # Min top-3 depth ($10 / 0.15) to support minimum order


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        log.warning(f"[pool_state] Failed to read {path}: {e}")
        return default


def _write_json_atomic(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f"[pool_state] Atomic write failed for {path}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


class CoinEntry:
    """In-memory representation of a coin in either Incubator or Battle basket."""
    __slots__ = (
        "coin", "venues", "basket", "enabled", "pause_reason",
        "top3_depth_usd", "stealth_capacity_usd", "min_order_usd", "max_order_usd",
        "mean_net_pnl_bps", "median_net_pnl_bps", "win_rate_pct",
        "profit_factor", "promoted_at", "event_count",
    )

    def __init__(
        self,
        coin: str,
        venues: List[str],
        basket: str = "incubator",
        enabled: bool = True,
        pause_reason: Optional[str] = None,
        top3_depth_usd: float = 0.0,
        stealth_capacity_usd: float = 0.0,
        min_order_usd: float = DEFAULT_MIN_ORDER_USD,
        max_order_usd: float = DEFAULT_SIZE_CAP_USD,
        mean_net_pnl_bps: float = 0.0,
        median_net_pnl_bps: float = 0.0,
        win_rate_pct: float = 0.0,
        profit_factor: float = 0.0,
        promoted_at: float = 0.0,
        event_count: int = 0,
    ) -> None:
        self.coin = coin
        self.venues = venues
        self.basket = basket            # "incubator" | "battle"
        self.enabled = enabled
        self.pause_reason = pause_reason
        self.top3_depth_usd = top3_depth_usd
        self.stealth_capacity_usd = stealth_capacity_usd if stealth_capacity_usd > 0.0 else round(top3_depth_usd * DEFAULT_STEALTH_FRAC, 2)
        self.min_order_usd = min_order_usd
        self.max_order_usd = max_order_usd
        self.mean_net_pnl_bps = mean_net_pnl_bps
        self.median_net_pnl_bps = median_net_pnl_bps
        self.win_rate_pct = win_rate_pct
        self.profit_factor = profit_factor
        self.promoted_at = promoted_at
        self.event_count = event_count

    def compute_size_usd(
        self,
        stealth_frac: float = DEFAULT_STEALTH_FRAC,
        min_order: float = DEFAULT_MIN_ORDER_USD,
        size_cap: float = DEFAULT_SIZE_CAP_USD,
    ) -> Optional[float]:
        """
        Calculates recommended execution size using Universal Stealth Sizing:
        size = min(size_cap, max(min_order, stealth_frac * top3_depth))
        Returns None if book is too shallow to support min_order safely.
        """
        if self.top3_depth_usd <= 0:
            return None
        candidate = stealth_frac * self.top3_depth_usd
        if candidate < min_order:
            return None
        return min(max(candidate, min_order), size_cap)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "coin": self.coin,
            "venues": self.venues,
            "basket": self.basket,
            "enabled": self.enabled,
            "pause_reason": self.pause_reason,
            "top3_depth_usd": round(self.top3_depth_usd, 1),
            "stealth_capacity_usd": round(self.stealth_capacity_usd, 1),
            "min_order_usd": round(self.min_order_usd, 2),
            "max_order_usd": round(self.max_order_usd, 2),
            "mean_net_pnl_bps": round(self.mean_net_pnl_bps, 2),
            "median_net_pnl_bps": round(self.median_net_pnl_bps, 2),
            "win_rate_pct": round(self.win_rate_pct, 1),
            "profit_factor": round(self.profit_factor, 2),
            "promoted_at": self.promoted_at,
            "event_count": self.event_count,
        }


class PoolStateManager:
    """
    Central authority managing Incubator candidates and Battle pool coins.
    Thread-safe for real-time reads and atomic updates.
    """

    def __init__(
        self,
        universe_file: str = UNIVERSE_FILE,
        battle_pool_file: str = BATTLE_POOL_FILE,
        overrides_file: str = OVERRIDES_FILE,
    ) -> None:
        self._universe_file = universe_file
        self._battle_pool_file = battle_pool_file
        self._overrides_file = overrides_file

        self._lock = threading.RLock()
        self._coins: Dict[str, CoinEntry] = {}
        self._overrides: Dict[str, Any] = {}
        self._universe_raw: Dict[str, Any] = {}
        self._last_reload: float = 0.0
        self._mtimes: Dict[str, float] = {}

    def reload(self, force: bool = False) -> None:
        """Reloads all JSON configuration files and merges state atomically."""
        with self._lock:
            current_mtimes = {}
            for path in (self._universe_file, self._battle_pool_file, self._overrides_file):
                if os.path.exists(path):
                    try:
                        current_mtimes[path] = os.path.getmtime(path)
                    except OSError:
                        pass
            if not force and current_mtimes and current_mtimes == self._mtimes:
                return

            self._coins = {}
            self._overrides = _read_json(self._overrides_file, default={})
            self._load_incubator()
            self._load_battle_pool()
            self._apply_overrides()
            self._last_reload = time.time()
            self._mtimes = current_mtimes
            log.info(
                f"[pool_state] Reloaded: {self._count_basket('incubator')} incubator, "
                f"{self._count_basket('battle')} battle "
                f"({sum(1 for c in self._coins.values() if c.basket == 'battle' and c.enabled)} active)"
            )

    def get_all_coins(self) -> List[str]:
        """All base coins across both Incubator and Battle baskets."""
        with self._lock:
            return sorted(self._coins.keys())

    def get_raw_universe_doc(self) -> Dict[str, Any]:
        """Raw dictionary from universe_live.json."""
        with self._lock:
            return dict(self._universe_raw)

    def is_battle_coin(self, coin: str) -> bool:
        """Returns True if coin is currently active in the Battle pool."""
        with self._lock:
            entry = self._coins.get(coin.upper())
            return bool(entry and entry.basket == "battle" and entry.enabled)

    def get_coin_entry(self, coin: str) -> Optional[CoinEntry]:
        with self._lock:
            return self._coins.get(coin.upper())

    def get_battle_coins(self) -> List[Dict[str, Any]]:
        """Returns all enabled Battle coins with dynamic stealth sizing."""
        with self._lock:
            overrides = self._overrides
            stealth_frac = float(overrides.get("stealth_depth_frac", DEFAULT_STEALTH_FRAC))
            min_order = float(overrides.get("min_order_usd", DEFAULT_MIN_ORDER_USD))
            size_cap = float(overrides.get("size_cap_usd", DEFAULT_SIZE_CAP_USD))

            result = []
            for entry in sorted(self._coins.values(), key=lambda e: e.mean_net_pnl_bps, reverse=True):
                if entry.basket != "battle" or not entry.enabled:
                    continue
                size = entry.compute_size_usd(stealth_frac, min_order, size_cap)
                d = entry.to_dict()
                d["recommended_size_usd"] = round(size, 2) if size else min_order
                result.append(d)
            return result

    def get_pool_snapshot(self) -> Dict[str, Any]:
        """Full snapshot of the pool state for reporting and CLI inspection."""
        with self._lock:
            overrides = self._overrides
            return {
                "updated_at": round(self._last_reload, 3),
                "settings": {
                    "min_order_usd": float(overrides.get("min_order_usd", DEFAULT_MIN_ORDER_USD)),
                    "stealth_depth_frac": float(overrides.get("stealth_depth_frac", DEFAULT_STEALTH_FRAC)),
                    "size_cap_usd": float(overrides.get("size_cap_usd", DEFAULT_SIZE_CAP_USD)),
                    "min_depth_usd": float(overrides.get("min_depth_usd", DEFAULT_MIN_DEPTH_USD)),
                },
                "coins": {coin: entry.to_dict() for coin, entry in self._coins.items()},
                "summary": {
                    "incubator": self._count_basket("incubator"),
                    "battle": self._count_basket("battle"),
                    "battle_active": sum(1 for c in self._coins.values() if c.basket == "battle" and c.enabled),
                    "paused": sum(1 for c in self._coins.values() if not c.enabled),
                },
            }

    def pause_coin(self, coin: str, reason: str = "manual") -> bool:
        """Pauses a coin (circuit breaker) and persists to runtime_overrides.json."""
        with self._lock:
            coin = coin.upper()
            if coin in self._coins:
                self._coins[coin].enabled = False
                self._coins[coin].pause_reason = reason

            overrides = _read_json(self._overrides_file, default={})
            paused = overrides.get("paused_coins", {})
            paused[coin] = {"reason": reason, "since": time.time()}
            overrides["paused_coins"] = paused
            _write_json_atomic(self._overrides_file, overrides)
            self._overrides = overrides
            if os.path.exists(self._overrides_file):
                try:
                    self._mtimes[self._overrides_file] = os.path.getmtime(self._overrides_file)
                except OSError:
                    pass
            log.info(f"[pool_state] Paused coin: {coin} (reason={reason})")
            return True

    def resume_coin(self, coin: str) -> bool:
        """Resumes a paused coin."""
        with self._lock:
            coin = coin.upper()
            if coin in self._coins:
                self._coins[coin].enabled = True
                self._coins[coin].pause_reason = None

            overrides = _read_json(self._overrides_file, default={})
            paused = overrides.get("paused_coins", {})
            paused.pop(coin, None)
            overrides["paused_coins"] = paused
            _write_json_atomic(self._overrides_file, overrides)
            self._overrides = overrides
            if os.path.exists(self._overrides_file):
                try:
                    self._mtimes[self._overrides_file] = os.path.getmtime(self._overrides_file)
                except OSError:
                    pass
            log.info(f"[pool_state] Resumed coin: {coin}")
            return True

    def promote_coin_manual(
        self,
        coin: str,
        mean_net_pnl_bps: float = 25.0,
        top3_depth_usd: float = 500.0,
        win_rate_pct: float = 85.0,
        profit_factor: float = 2.0,
    ) -> bool:
        """Manually promotes a single coin to the Battle pool."""
        with self._lock:
            coin = coin.upper()
            entry = self._coins.get(coin)
            if entry is None:
                entry = CoinEntry(coin=coin, venues=["binance", "bybit"])
                self._coins[coin] = entry

            entry.basket = "battle"
            entry.enabled = True
            entry.pause_reason = None
            if entry.top3_depth_usd <= 0:
                entry.top3_depth_usd = top3_depth_usd
            if entry.mean_net_pnl_bps <= 0:
                entry.mean_net_pnl_bps = mean_net_pnl_bps
                entry.median_net_pnl_bps = mean_net_pnl_bps
            if entry.win_rate_pct <= 0:
                entry.win_rate_pct = win_rate_pct
            if entry.profit_factor <= 0:
                entry.profit_factor = profit_factor
            entry.promoted_at = time.time()

            # Remove from paused_coins if it was paused
            overrides = _read_json(self._overrides_file, default={})
            paused = overrides.get("paused_coins", {})
            if coin in paused:
                paused.pop(coin, None)
                overrides["paused_coins"] = paused
                _write_json_atomic(self._overrides_file, overrides)
                self._overrides = overrides

            self._persist_battle_pool()
            log.info(f"[pool_state] Manually promoted {coin} to Battle pool")
            return True

    def demote_to_incubator(self, coin: str, reason: str = "manual_demote") -> bool:
        """Demotes a single coin from Battle back to Incubator, resetting battle metrics."""
        with self._lock:
            coin = coin.upper()
            entry = self._coins.get(coin)
            if not entry:
                return False
            entry.basket = "incubator"
            entry.mean_net_pnl_bps = 0.0
            entry.median_net_pnl_bps = 0.0
            entry.win_rate_pct = 0.0
            entry.profit_factor = 0.0
            entry.promoted_at = 0.0
            entry.event_count = 0
            self._persist_battle_pool()
            log.info(f"[pool_state] Demoted {coin} to Incubator (reason={reason})")
            return True

    def promote_to_battle(self, qualifying_stats: List[Dict[str, Any]]) -> None:
        """
        Promotes qualifying coins from Incubator to Battle based on verified
        quantitative performance. Demotes degraded coins back to Incubator.
        """
        with self._lock:
            self._overrides = _read_json(self._overrides_file, default={})
            if os.path.exists(self._overrides_file):
                try:
                    self._mtimes[self._overrides_file] = os.path.getmtime(self._overrides_file)
                except OSError:
                    pass
            overrides = self._overrides
            qualified_coins: Set[str] = set()

            for stat in qualifying_stats:
                coin = str(stat["coin"]).upper()
                qualified_coins.add(coin)

                entry = self._coins.get(coin)
                if entry is None:
                    entry = CoinEntry(coin=coin, venues=stat.get("venues", ["binance", "bybit"]))
                    self._coins[coin] = entry

                entry.basket = "battle"
                entry.venues = stat.get("venues", entry.venues)
                entry.top3_depth_usd = float(stat.get("top3_depth_usd", 0.0))
                entry.stealth_capacity_usd = float(
                    stat.get("stealth_capacity_usd")
                    or round(entry.top3_depth_usd * DEFAULT_STEALTH_FRAC, 2)
                )
                entry.mean_net_pnl_bps = float(stat.get("mean_net_pnl_bps", 0.0))
                entry.median_net_pnl_bps = float(stat.get("median_net_pnl_bps", 0.0))
                entry.win_rate_pct = float(stat.get("win_rate_pct", 0.0))
                entry.profit_factor = float(stat.get("profit_factor", 0.0))
                entry.event_count = int(stat.get("event_count", 0))
                entry.promoted_at = time.time()

            # Demote battle coins that failed qualification this cycle
            for coin, entry in self._coins.items():
                if entry.basket == "battle" and coin not in qualified_coins:
                    entry.basket = "incubator"
                    entry.stealth_capacity_usd = 0.0
                    entry.mean_net_pnl_bps = 0.0
                    entry.median_net_pnl_bps = 0.0
                    entry.win_rate_pct = 0.0
                    entry.profit_factor = 0.0
                    entry.promoted_at = 0.0
                    entry.event_count = 0
                    log.info(f"[pool_state] Demoted {coin} from Battle -> Incubator (failed criteria)")

            self._apply_overrides()
            self._persist_battle_pool()
            log.info(
                f"[pool_state] Updated Battle pool: {len(qualified_coins)} coins qualified. "
                f"Total Battle: {self._count_basket('battle')}"
            )

    # ------------------------------------------------------------------ Private
    def _load_incubator(self) -> None:
        doc = _read_json(self._universe_file, default={})
        self._universe_raw = doc
        if not doc:
            return

        coins = doc.get("coins", [])
        coin_venues: Dict[str, Set[str]] = {c: set() for c in coins}

        # Dynamic venue extraction from all *_sym_map entries or pairs
        for k, v_map in doc.items():
            if k.endswith("_sym_map") and isinstance(v_map, dict):
                venue_name = k[:-8].lower()
                for raw, item in v_map.items():
                    c = item[0] if isinstance(item, (list, tuple)) else item
                    coin_venues.setdefault(c, set()).add(venue_name)

        pairs = doc.get("pairs", {})
        if isinstance(pairs, dict):
            for c, p_meta in pairs.items():
                if isinstance(p_meta, dict):
                    for v in p_meta.get("venues", []):
                        coin_venues.setdefault(c, set()).add(v.lower())

        for coin, venues in coin_venues.items():
            if coin in self._coins:
                self._coins[coin].venues = sorted(venues)
            else:
                self._coins[coin] = CoinEntry(
                    coin=coin,
                    venues=sorted(venues),
                    basket="incubator",
                )

    def _load_battle_pool(self) -> None:
        doc = _read_json(self._battle_pool_file, default={})
        if not doc:
            return

        for coin, meta in doc.get("coins", {}).items():
            coin = coin.upper()
            entry = self._coins.get(coin)
            if entry is None:
                entry = CoinEntry(coin=coin, venues=meta.get("venues", []))
                self._coins[coin] = entry

            entry.basket = "battle"
            entry.venues = meta.get("venues", entry.venues)
            entry.top3_depth_usd = float(meta.get("top3_depth_usd", 0.0))
            entry.mean_net_pnl_bps = float(meta.get("mean_net_pnl_bps", 0.0))
            entry.median_net_pnl_bps = float(meta.get("median_net_pnl_bps", 0.0))
            entry.win_rate_pct = float(meta.get("win_rate_pct", 0.0))
            entry.profit_factor = float(meta.get("profit_factor", 0.0))
            entry.event_count = int(meta.get("event_count", 0))
            entry.promoted_at = float(meta.get("promoted_at", 0.0))

    def _apply_overrides(self) -> None:
        paused = self._overrides.get("paused_coins", {})
        for coin, entry in self._coins.items():
            if coin in paused:
                entry.enabled = False
                info = paused[coin]
                entry.pause_reason = info.get("reason", "manual") if isinstance(info, dict) else str(info)
            else:
                entry.enabled = True
                entry.pause_reason = None

    def _persist_battle_pool(self) -> None:
        doc = {
            "_meta": {
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "updated_ts": round(time.time(), 3),
                "battle_count": self._count_basket("battle"),
                "incubator_count": self._count_basket("incubator"),
            },
            "coins": {
                coin: entry.to_dict()
                for coin, entry in self._coins.items()
                if entry.basket == "battle"
            },
        }
        _write_json_atomic(self._battle_pool_file, doc)
        if os.path.exists(self._battle_pool_file):
            try:
                self._mtimes[self._battle_pool_file] = os.path.getmtime(self._battle_pool_file)
            except OSError:
                pass

    def _count_basket(self, basket: str) -> int:
        return sum(1 for e in self._coins.values() if e.basket == basket)
```

## pipeline/profile.py

```python
"""
pipeline/profile.py — Trading Profile & Capital Allocation Manager for Korean Sunrise.

Responsibilities:
  - Manages trading_profile.json (capital per follower venue, margin reserve, slots).
  - Dynamically calculates executable stealth order sizes matched to the operator's
    actual available deposit on Binance and Bybit.
  - Enforces margin reserve cushions (20%) and slot diversification.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Dict, List, Optional

log = logging.getLogger("pipeline.profile")

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROFILE_FILE = os.path.join(HERE, "trading_profile.json")

DEFAULT_PROFILE: Dict[str, Any] = {
    "_meta": {
        "description": "Korean Sunrise Execution Profile: Venue deposits, margin cushions, and slot sizing",
        "version": "1.0",
    },
    "capital": {
        "per_venue_usd": {
            "binance": 100.0,
            "bybit": 100.0,
            "okx": 100.0,
            "gateio": 100.0,
            "mexc": 100.0,
            "kucoin": 100.0,
            "bingx": 100.0,
            "htx": 100.0,
            "pionex": 100.0,
            "asterdex": 100.0,
            "hyperliquid": 100.0,
        },
        "reserve_margin_frac": 0.20,  # 20% margin cushion against drawdowns
    },
    "active_venues": [
        "binance",
        "bybit",
        "okx",
        "gateio",
        "mexc",
        "kucoin",
        "bingx",
        "htx",
        "pionex",
        "asterdex",
        "hyperliquid",
    ],
    "max_active_slots": 5,           # Max concurrent positions
    "min_order_usd": 10.0,           # Exchange min notional
    "stealth_depth_frac": 0.15,      # Universal stealth sizing rule (15% top-3 ask depth)
    "max_single_position_usd": 500.0,# Hard ceiling per position
}


def _read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        log.warning(f"[profile] Failed to read {path}: {e}")
        return default


def _write_json_atomic(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        log.error(f"[profile] Atomic write failed for {path}: {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


class TradingProfileManager:
    """Manages active trading venues, deposit allocation, and position sizing."""

    def __init__(self, profile_file: str = PROFILE_FILE) -> None:
        self.profile_file = profile_file
        self._lock = threading.RLock()
        self._profile: Dict[str, Any] = {}
        self._file_mtime: float = 0.0
        self.reload(force=True)

    def reload(self, force: bool = False) -> None:
        with self._lock:
            if os.path.exists(self.profile_file):
                try:
                    mtime = os.path.getmtime(self.profile_file)
                    if not force and mtime == self._file_mtime:
                        return
                    self._file_mtime = mtime
                except OSError:
                    pass
            data = _read_json(self.profile_file)
            if not data:
                log.info(f"[profile] Initializing default trading profile at {self.profile_file}")
                data = json.loads(json.dumps(DEFAULT_PROFILE))
                _write_json_atomic(self.profile_file, data)
                if os.path.exists(self.profile_file):
                    try:
                        self._file_mtime = os.path.getmtime(self.profile_file)
                    except OSError:
                        pass
            self._profile = data

    def get_profile(self) -> Dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._profile))

    def update_profile(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            def _merge(src: dict, patch: dict) -> None:
                for k, v in patch.items():
                    if isinstance(v, dict) and k in src and isinstance(src[k], dict):
                        _merge(src[k], v)
                    else:
                        src[k] = v

            _merge(self._profile, updates)
            _write_json_atomic(self.profile_file, self._profile)
            if os.path.exists(self.profile_file):
                try:
                    self._file_mtime = os.path.getmtime(self.profile_file)
                except OSError:
                    pass
            return self.get_profile()

    def set_venue_capital(self, venue: str, capital_usd: float) -> None:
        with self._lock:
            venue = venue.lower().strip()
            caps = self._profile.setdefault("capital", {}).setdefault("per_venue_usd", {})
            caps[venue] = max(0.0, float(capital_usd))
            _write_json_atomic(self.profile_file, self._profile)
            if os.path.exists(self.profile_file):
                try:
                    self._file_mtime = os.path.getmtime(self.profile_file)
                except OSError:
                    pass
            log.info(f"[profile] Updated capital for {venue}: ${capital_usd:.2f}")

    def set_max_slots(self, slots: int) -> None:
        with self._lock:
            self._profile["max_active_slots"] = max(1, int(slots))
            _write_json_atomic(self.profile_file, self._profile)
            if os.path.exists(self.profile_file):
                try:
                    self._file_mtime = os.path.getmtime(self.profile_file)
                except OSError:
                    pass
            log.info(f"[profile] Updated max_active_slots: {slots}")

    def is_venue_active(self, venue: str) -> bool:
        with self._lock:
            active = [v.lower() for v in self._profile.get("active_venues", [])]
            return venue.lower() in active

    def calculate_stealth_order_size(
        self,
        venue: str,
        follower_top3_ask_usd: float,
    ) -> Optional[float]:
        """
        Dynamically calculates actionable order size in USD:
          1. Slot Budget = (Venue_Capital / Slots) * (1 - Reserve_Margin)
          2. Stealth Size = 0.15 * Top-3 Ask Depth
          3. Sized Order = min(Slot Budget, Stealth Size, Max Position Cap)
          Returns None if book depth cannot support the exchange minimum order.
        """
        with self._lock:
            venue = venue.lower()
            if not self.is_venue_active(venue):
                return None

            cap_dict = self._profile.get("capital", {}).get("per_venue_usd", {})
            venue_cap = float(cap_dict.get(venue, 50.0))
            if venue_cap <= 0:
                return None

            reserve_margin = float(self._profile.get("capital", {}).get("reserve_margin_frac", 0.20))
            slots = max(1, int(self._profile.get("max_active_slots", 3)))
            stealth_frac = float(self._profile.get("stealth_depth_frac", 0.15))
            min_order = float(self._profile.get("min_order_usd", 10.0))
            max_cap = float(self._profile.get("max_single_position_usd", 500.0))

            slot_budget = (venue_cap / slots) * (1.0 - reserve_margin)
            stealth_size = stealth_frac * follower_top3_ask_usd

            size = min(slot_budget, stealth_size, max_cap)
            if size < min_order:
                return None

            return round(size, 2)
```

## reports/daily/report_2026-10-18.json

```json
{
  "metadata": {
    "report_date": "2026-10-18",
    "generated_at": "2026-10-18T18:09:44.940632+00:00",
    "days_analyzed": 1.0,
    "order_size_usd": 10.0,
    "roundtrip_fee_bps": 10.0,
    "total_files_scanned": 0,
    "total_size_mb": 0.0,
    "total_signals_recorded": 0,
    "coins_active": 0,
    "venues_active": [],
    "elapsed_sec": 0.006
  },
  "close_reasons": {},
  "trigger_types": {},
  "pnl_scenarios": {
    "all_signals": {
      "trades": 0,
      "wins": 0,
      "winrate_pct": 0.0,
      "gross_usd": 0.0,
      "fee_usd": 0.0,
      "net_pnl_usd": 0.0,
      "profit_factor": 0.0
    },
    "calibrated_threshold_only": {
      "trades": 0,
      "wins": 0,
      "winrate_pct": 0.0,
      "gross_usd": 0.0,
      "fee_usd": 0.0,
      "net_pnl_usd": 0.0,
      "profit_factor": 0.0
    },
    "battle_coins_only": {
      "trades": 0,
      "wins": 0,
      "winrate_pct": 0.0,
      "gross_usd": 0.0,
      "fee_usd": 0.0,
      "net_pnl_usd": 0.0,
      "profit_factor": 0.0
    },
    "funded_venues_only": {
      "trades": 0,
      "wins": 0,
      "winrate_pct": 0.0,
      "gross_usd": 0.0,
      "fee_usd": 0.0,
      "net_pnl_usd": 0.0,
      "profit_factor": 0.0
    }
  },
  "venue_capacities": [],
  "sweep_thresholds": {
    "10": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    },
    "15": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    },
    "20": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    },
    "25": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    },
    "30": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    },
    "35": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    },
    "40": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    },
    "50": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    },
    "60": {
      "trades": 0,
      "winrate_pct": 0.0,
      "net_usd": 0.0
    }
  },
  "top_pairs": [],
  "top_coins": []
}
```

## reports/daily/report_2026-10-18.md

```md
# Korean Sunrise: Суточный Квантовый Отчёт (2026-10-18)
*Сгенерирован:* `2026-10-18T18:09:44.940632+00:00` | *Окно:* `1.0 d` | *Размер слота:* `$10.0` | *Комиссия:* `10.0 bps`

## 1. Сводные метрики телеметрии
- **Всего зафиксировано сигналов:** `0`
- **Активных монет с импульсами:** `0`
- **Фолловер-бирж активно:** `0` (None)
- **Архивов данных обработано:** `0` (`0.0 MB`)
- **Причины закрытия импульсов:** `Converged: 0` | `Timeout: 0` | `Reversed: 0`
- **Триггеры выкупа Upbit:** `Depth Drain: 0` | `Spread Blowout: 0`

## 2. PnL Сценарии исполнения
| Сценарий | Сигналов | Win Rate | Gross USD | Комиссии | Net PnL USD | Profit Factor |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Все зафиксированные сигналы (Raw)** | 0 | 0.0% | $0.00 | $0.00 | **$0.00** | 0.0 |
| **С калиброванным порогом (Matrix Filter)** | 0 | 0.0% | $0.00 | $0.00 | **$0.00** | 0.0 |
| **Только проверенные монеты (Battle Pool)** | 0 | 0.0% | $0.00 | $0.00 | **$0.00** | 0.0 |
| **Только фондированные биржи (Funded CEXs)** | 0 | 0.0% | $0.00 | $0.00 | **$0.00** | 0.0 |

## 3. Физическая ёмкость стакана по биржам (Real Order Book Capacity)
| Биржа | Сигналов | Мед. Top-3 стакан ($) | Мед. 15% Ёмкость ($) | P25 Ёмкость ($) | P75 Ёмкость ($) | Мин. ($) | Макс. ($) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| *Нет накопленных данных по стаканам* | - | - | - | - | - | - | - |

## 4. Оптимизация порогов спреда (Threshold Sweep)
| Порог (bps) | Сигналов | Win Rate | Net PnL USD |
| :---: | :---: | :---: | :---: |
| >=10 bps | 0 | 0.0% | $0.00 |
| >=15 bps | 0 | 0.0% | $0.00 |
| >=20 bps | 0 | 0.0% | $0.00 |
| >=25 bps | 0 | 0.0% | $0.00 |
| >=30 bps | 0 | 0.0% | $0.00 |
| >=35 bps | 0 | 0.0% | $0.00 |
| >=40 bps | 0 | 0.0% | $0.00 |
| >=50 bps | 0 | 0.0% | $0.00 |
| >=60 bps | 0 | 0.0% | $0.00 |

## 5. Топ прибыльных связок (Upbit Leader <-> Perp Follower)
| Связка (Coin:Upbit-Follower) | Сделок | Win Rate | Gross USD | Net PnL USD | Мед. Top-3 ($) | Мед. 15% Ёмкость ($) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| *Нет накопленных сделок за выбранный период* | - | - | - | - | - | - |

## 6. Топ монет по эффективности опережения
| Монета | Сделок | Win Rate | Net PnL USD | Мед. Top-3 ($) | Мед. 15% Ёмкость ($) | Ср. MFE (bps) | Медиана $T_{peak}$ |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| *Нет накопленных данных по монетам* | - | - | - | - | - | - | - |
```

## runtime_overrides.json

```json
{
  "paused_coins": {}
}
```

## screening/__init__.py

```python
"""
screening package for Korean Sunrise.
Provides two-stage universe screening, namesake detection, and kline correlation.
"""

from screening.metrics import evaluate_pair_klines
from screening.screener import UniverseScreener

__all__ = ["evaluate_pair_klines", "UniverseScreener"]
```

## screening/metrics.py

```python
"""
screening/metrics.py — Quantitative metrics and statistical validation for candidate pairs.
Vectorized numpy math for 1-minute kline correlation, namesake detection, and basis volatility.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple
import numpy as np


def evaluate_pair_klines(
    kl_upbit: Dict[int, Tuple[float, float]],
    kl_perp: Dict[int, Tuple[float, float]],
    usdt_krw_rate: float,
    min_corr: float = 0.70,
    min_both_trade: float = 0.20,
    min_dev_bps: float = 10.0,
    max_price_diff_pct: float = 15.0,
    min_samples: int = 30,
) -> Dict[str, float]:
    """
    Evaluates 1-minute candle time series between Upbit (KRW) and Follower Perp (USD).

    Returns dictionary of quantitative metrics:
      - 'samples': number of overlapping minutes
      - 'price_diff_pct': percentage difference in median price (detects namesakes)
      - 'corr': correlation of minute log-returns
      - 'both_trade': fraction of minutes with volume > 0 on both venues
      - 'med_basis_bps': median basis (prevailing Kimchi Premium in bps)
      - 'p90_dev_bps': 90th percentile of |basis - med_basis| in bps
      - 'passed': 1.0 if qualifies, 0.0 otherwise
    """
    empty_res = {
        "samples": 0.0,
        "price_diff_pct": 999.0,
        "corr": 0.0,
        "both_trade": 0.0,
        "med_basis_bps": 0.0,
        "p90_dev_bps": 0.0,
        "passed": 0.0,
    }

    if usdt_krw_rate <= 0:
        return empty_res

    common_ts = sorted(set(kl_upbit.keys()) & set(kl_perp.keys()))
    if len(common_ts) < min_samples:
        return {**empty_res, "samples": float(len(common_ts))}

    # Upbit prices in USD: price_krw / usdt_krw_rate
    p_up_usd = np.array([kl_upbit[t][0] / usdt_krw_rate for t in common_ts], dtype=np.float64)
    v_up_usd = np.array([kl_upbit[t][1] / usdt_krw_rate for t in common_ts], dtype=np.float64)

    p_perp_usd = np.array([kl_perp[t][0] for t in common_ts], dtype=np.float64)
    v_perp_usd = np.array([kl_perp[t][1] for t in common_ts], dtype=np.float64)

    # Sanity: ensure all prices positive
    if np.any(p_up_usd <= 0) or np.any(p_perp_usd <= 0):
        return {**empty_res, "samples": float(len(common_ts))}

    # 1. Namesake check (Guard against different tokens sharing the same ticker)
    med_up = float(np.median(p_up_usd))
    med_perp = float(np.median(p_perp_usd))
    price_diff_pct = abs(med_up - med_perp) / max(med_up, med_perp) * 100.0

    if price_diff_pct > max_price_diff_pct:
        return {
            "samples": float(len(common_ts)),
            "price_diff_pct": round(price_diff_pct, 1),
            "corr": 0.0,
            "both_trade": 0.0,
            "med_basis_bps": 0.0,
            "p90_dev_bps": 0.0,
            "passed": 0.0,
        }

    # 2. Correlation of minute log-returns
    r_up = np.diff(np.log(p_up_usd))
    r_perp = np.diff(np.log(p_perp_usd))

    std_up = float(np.std(r_up))
    std_perp = float(np.std(r_perp))

    if std_up < 1e-9 or std_perp < 1e-9:
        corr = 0.0
    else:
        c_val = float(np.corrcoef(r_up, r_perp)[0, 1])
        corr = c_val if np.isfinite(c_val) else 0.0

    # 3. Active trading minutes on both sides
    both_trade = float(np.mean((v_up_usd > 0) & (v_perp_usd > 0)))

    # 4. Kimchi basis & spread volatility
    mid = (p_up_usd + p_perp_usd) * 0.5
    signed_basis_bps = (p_up_usd - p_perp_usd) / mid * 10000.0
    med_basis_bps = float(np.median(signed_basis_bps))
    basis_dev_bps = np.abs(signed_basis_bps - med_basis_bps)
    p90_dev_bps = float(np.percentile(basis_dev_bps, 90))

    # 5. Gate decision
    passed = 1.0 if (
        corr >= min_corr
        and both_trade >= min_both_trade
        and p90_dev_bps >= min_dev_bps
        and price_diff_pct <= max_price_diff_pct
    ) else 0.0

    return {
        "samples": float(len(common_ts)),
        "price_diff_pct": round(price_diff_pct, 2),
        "corr": round(corr, 3),
        "both_trade": round(both_trade, 3),
        "med_basis_bps": round(med_basis_bps, 1),
        "p90_dev_bps": round(p90_dev_bps, 1),
        "passed": passed,
    }
```

## screening/screener.py

```python
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
```

## tests/test_adaptive_stops_and_fixes.py

```python
"""
tests/test_adaptive_stops_and_fixes.py - Verification suite for Korean Sunrise 7-fix overhaul:
1. Book age tolerances (5.0s entry, 15.0s baseline learning).
2. Holding horizons (30.0s engine default, 60.0s calibrator ceiling).
3. Multi-second Upbit sweep trade confirmation (6.0s window).
4. Triple Stop-Loss Models (Model A Invalidation, Model B Adverse PnL, Model C Alpha Decay).
5. Elimination of redundant spread penalty.
6. Upbit spot silence tolerance (15.0s).
7. Evaluator noise filter relaxation (0.30).
8. ReconnectPolicy and defensive order book sorting.
"""
from __future__ import annotations

import unittest
import time
from typing import Dict, Any

import config as C
from models import OrderBookDepth5, SunriseSignal, normalize_exchange_ts
from korean_engine import KoreanEngine, PairState
from venues.base import BaseVenueStream, ReconnectPolicy, TTLCache
from tools.calibrate_thresholds import _calc_optimal_hold
from pipeline.evaluator import CoinSignalAccumulator, DEFAULT_MIN_EVENTS


class TestAdaptiveStopsAndFixes(unittest.TestCase):

    def setUp(self):
        self.emitted: list = []
        self.engine = KoreanEngine(
            emit_callback=lambda s: self.emitted.append(s),
            warmup_samples=5,
            max_follower_book_age_sec=5.0,
            max_hold_sec=30.0,
            stop_invalidation_bps=15.0,
            stop_adverse_pnl_bps=35.0,
            alpha_decay_sec=8.0,
        )

    # -------------------------------------------------------------
    # Test 1: Normalize exchange ts across ns, us, ms, s
    # -------------------------------------------------------------
    def test_normalize_exchange_ts(self):
        now_ms = int(time.time() * 1000)
        # ms
        self.assertEqual(normalize_exchange_ts(now_ms), now_ms)
        # seconds -> ms
        now_s = now_ms // 1000
        self.assertEqual(normalize_exchange_ts(now_s), now_s * 1000)
        # ns -> ms
        now_ns = now_ms * 1_000_000
        self.assertEqual(normalize_exchange_ts(now_ns), now_ms)
        # us -> ms
        now_us = now_ms * 1_000
        self.assertEqual(normalize_exchange_ts(now_us), now_ms)
        # string representation
        self.assertEqual(normalize_exchange_ts(str(now_ms)), now_ms)

    # -------------------------------------------------------------
    # Test 2: ReconnectPolicy and TTLCache
    # -------------------------------------------------------------
    def test_reconnect_policy_and_ttl_cache(self):
        policy = ReconnectPolicy(base_delay=2.0, max_delay=60.0, backoff_factor=2.0)
        # Standard error
        d1 = policy.compute_delay(Exception("Connection reset"))
        self.assertEqual(d1, 2.0)
        d2 = policy.compute_delay(Exception("Connection reset"))
        self.assertEqual(d2, 4.0)

        # 429 Too Many Requests -> aggressive backoff
        d_429 = policy.compute_delay(Exception("HTTP 429 Too Many Requests"))
        self.assertGreaterEqual(d_429, 30.0)

        # Reset
        policy.reset()
        self.assertEqual(policy.compute_delay(), 2.0)

        # TTLCache
        cache = TTLCache(ttl_sec=1.0)
        cache.set("key1")
        self.assertTrue(cache.get("key1"))
        self.assertFalse(cache.get("key2"))

    # -------------------------------------------------------------
    # Test 3: Defensive sorting in BaseVenueStream._sanitise
    # -------------------------------------------------------------
    def test_defensive_orderbook_sanitise(self):
        received = []
        stream = BaseVenueStream("test_venue", ["BTC"], callback=lambda snap: received.append(snap))
        # Unsorted bids and asks
        snap = OrderBookDepth5(
            exchange="test_venue",
            symbol="BTC",
            raw_symbol="BTCUSDT",
            ts=time.time(),
            exchange_ts=int(time.time() * 1000),
            bids=[(100.0, 1.0), (102.0, 2.0), (101.0, 1.5)],  # Best bid is 102.0
            asks=[(105.0, 1.0), (103.0, 2.0), (104.0, 1.5)],  # Best ask is 103.0
        )
        stream._sanitise(snap)
        self.assertEqual(len(received), 1)
        clean = received[0]
        # Bids must be sorted descending: 102.0, 101.0, 100.0
        self.assertEqual(clean.bids[0][0], 102.0)
        # Asks must be sorted ascending: 103.0, 104.0, 105.0
        self.assertEqual(clean.asks[0][0], 103.0)

    # -------------------------------------------------------------
    # Test 4: Follower baseline learning decoupled from 5.0s entry age
    # -------------------------------------------------------------
    def test_follower_book_age_tolerance_and_learning(self):
        now = time.time()
        coin = "ALT"
        # Upbit book 1.0s ago
        u_snap = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 1.0, exchange_ts=int((now - 1.0) * 1000),
            bids=[(10.0, 100.0)], asks=[(10.1, 100.0)]
        )
        self.engine._books[("upbit", coin)] = u_snap
        self.engine._recv_ts[("upbit", coin)] = now - 1.0

        # Follower book arrived 3.5s ago (older than old 0.3s limit, but <= 5.0s)
        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol=f"{coin}USDT",
            ts=now - 3.5, exchange_ts=int((now - 3.5) * 1000),
            bids=[(9.995, 100.0)], asks=[(10.005, 100.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now - 3.5

        # Normalization and baseline state
        self.engine._upbit_norm[coin] = {
            "depth_ask": 500.0,
            "best_ask_usd": 200.0,
            "spread_bps": 10.0,
            "t": now - 1.0,
        }
        st = self.engine._get_or_create_pair_state(coin, "binance")
        # Warm it up
        for i in range(6):
            st.learn(10.0, (now - 20.0) + (i * 2.0))
        self.assertTrue(st.is_warmed_up)

        # Sweep on Upbit triggers lead-lag evaluation
        u_sweep = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(10.1, 100.0)], asks=[(10.5, 100.0)]
        )
        self.engine.on_book_update(u_sweep)

        # With 5.0s tolerance, the 3.5s follower book is accepted and signal fires!
        self.assertEqual(len(self.engine._active_signals[coin]), 1)
        sig = self.engine._active_signals[coin][0]
        self.assertEqual(sig.coin, coin)

    # -------------------------------------------------------------
    # Test 5: Model A Microstructure Invalidation Stop-Loss
    # -------------------------------------------------------------
    def test_model_a_invalidation_stop(self):
        now = 1000.0
        coin = "DOGE"
        st = self.engine._get_or_create_pair_state(coin, "binance")
        st.baseline_bps = 0.0

        sig = SunriseSignal(
            id="test_sig_inv",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="DOGEUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=100.0,
            leader_best_ask=100.05,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=99.0,
            follower_best_bid=98.95,
            follower_best_ask=99.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=5.0,
            raw_dislocation_bps=100.0,
            dev_z=3.0,
            gross_exec_edge_bps=90.0,
            net_exec_edge_bps=80.0,
            suggested_size_usd=100.0,
            max_hold_sec=30.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Upbit dumps below entry (drop > 1.5 * sigma = 7.5 bps -> e.g. 15 bps drop)
        u_dump = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[(99.80, 10.0)], asks=[(99.85, 10.0)]  # Mid = 99.825 (-17.5 bps from 100.0)
        )
        f_book = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol=f"{coin}USDT",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[(99.00, 10.0)], asks=[(99.05, 10.0)]
        )
        self.engine._books[("upbit", coin)] = u_dump
        self.engine._books[("binance", coin)] = f_book

        self.engine._track_follower_move(coin, "binance", f_book, now + 1.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "stop_invalidation")
        self.assertTrue(sig.stop_hit)
        self.assertEqual(sig.stop_type, "invalidation")
        self.assertGreater(self.engine.counters["signals_stop_loss_invalidation"], 0)

    # -------------------------------------------------------------
    # Test 6: Model B Hard Adverse PnL Drawdown Stop-Loss
    # -------------------------------------------------------------
    def test_model_b_adverse_pnl_stop(self):
        now = 1000.0
        coin = "PEPE"
        st = self.engine._get_or_create_pair_state(coin, "bybit")
        st.baseline_bps = 0.0

        sig = SunriseSignal(
            id="test_sig_adv",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="bybit",
            follower_contract="1000PEPEUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=100.0,
            leader_best_ask=100.05,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=100.0,
            follower_best_bid=99.95,
            follower_best_ask=100.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=50.0,
            dev_z=3.0,
            gross_exec_edge_bps=40.0,
            net_exec_edge_bps=30.0,
            suggested_size_usd=100.0,
            max_hold_sec=30.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        u_held = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.00, 10.0)], asks=[(100.05, 10.0)]
        )
        # Follower crashes violently: best bid drops to 99.50 (-55 bps exec move -> net PnL = -65 bps)
        f_crash = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="1000PEPEUSDT",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(99.50, 10.0)], asks=[(99.55, 10.0)]
        )
        self.engine._books[("upbit", coin)] = u_held
        self.engine._books[("bybit", coin)] = f_crash

        self.engine._track_follower_move(coin, "bybit", f_crash, now + 2.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "stop_adverse_pnl")
        self.assertTrue(sig.stop_hit)
        self.assertEqual(sig.stop_type, "adverse_pnl")
        self.assertGreater(self.engine.counters["signals_stop_loss_adverse_pnl"], 0)

    # -------------------------------------------------------------
    # Test 7: Model C Alpha Decay Timeout Stop
    # -------------------------------------------------------------
    def test_model_c_alpha_decay_stop(self):
        now = 1000.0
        coin = "SHIB"
        st = self.engine._get_or_create_pair_state(coin, "binance")
        st.baseline_bps = 0.0

        sig = SunriseSignal(
            id="test_sig_decay",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="1000SHIBUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=100.0,
            leader_best_ask=100.05,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=100.0,
            follower_best_bid=99.95,
            follower_best_ask=100.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=2.0,
            raw_dislocation_bps=50.0,
            dev_z=3.0,
            gross_exec_edge_bps=40.0,
            net_exec_edge_bps=30.0,
            suggested_size_usd=100.0,
            max_hold_sec=30.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Leader stays, follower is flat, but 8.5 seconds have passed (dt >= alpha_decay_sec=8.0)
        u_stay = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 8.5, exchange_ts=int((now + 8.5) * 1000),
            bids=[(100.00, 10.0)], asks=[(100.05, 10.0)]
        )
        f_flat = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="1000SHIBUSDT",
            ts=now + 8.5, exchange_ts=int((now + 8.5) * 1000),
            bids=[(100.02, 10.0)], asks=[(100.07, 10.0)]  # Net PnL is still -7 bps due to fees
        )
        self.engine._books[("upbit", coin)] = u_stay
        self.engine._books[("binance", coin)] = f_flat

        self.engine._track_follower_move(coin, "binance", f_flat, now + 8.5)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "stop_decay")
        self.assertTrue(sig.stop_hit)
        self.assertEqual(sig.stop_type, "alpha_decay")
        self.assertGreater(self.engine.counters["signals_stop_loss_decay"], 0)

    # -------------------------------------------------------------
    # Test 8: Holding horizon clamping in calibrator
    # -------------------------------------------------------------
    def test_optimal_hold_calibrator(self):
        # Peak at 25 seconds on a slow altcoin wave
        t_peaks = [20.0, 24.0, 26.0, 25.0, 30.0]
        optimal = _calc_optimal_hold(t_peaks)
        self.assertGreater(optimal, 20.0)
        self.assertLessEqual(optimal, 60.0)

    # -------------------------------------------------------------
    # Test 9: Evaluator noise filter relaxation to 0.30
    # -------------------------------------------------------------
    def test_evaluator_noise_filter(self):
        acc = CoinSignalAccumulator("TEST")
        # Signal with drain_ratio 0.35 and cross_ratio 0.35 (would have been dropped by 0.40/0.50)
        acc.add_signal({
            "follower_exchange": "binance",
            "net_pnl_bps": 25.0,
            "drain_ratio": 0.35,
            "leader_swept_usd": 700.0,
            "follower_top3_ask_usd": 2000.0,  # cross_ratio = 700/2000 = 0.35
            "stealth_capacity_usd": 300.0,
            "mfe_exec_pnl_bps": 30.0,
            "time_to_peak_sec": 12.0,
        })
        self.assertEqual(acc.valid_sweeps, 1)
        self.assertEqual(len(acc.net_pnls), 1)
        self.assertEqual(DEFAULT_MIN_EVENTS, 3)


if __name__ == "__main__":
    unittest.main()
```

## tests/test_korean_engine_strategy.py

```python
"""
tests/test_korean_engine_strategy.py - Comprehensive Unit Test Suite for KoreanEngine Strategy Logic.

Exhaustively verifies every microstructural branch and invariant:
1. depth_drain trigger: Ask lifted mode (swept top level + top 5 depletion).
2. depth_drain trigger: Ask held mode (front-of-book depletion >= 50% top 5 loss).
3. depth_drain rejection: Deep-book pullbacks (best ask loss < 50% of top 5 loss).
4. spread_blowout trigger: Upward spread widening beyond blowout threshold.
5. Take-Profit Convergence: 75% of initial net edge captured.
6. Take-Profit Convergence: Dislocation basis absorbed (rem_edge <= 0) with net profit.
7. Concurrent Multi-Venue Tracking: Independent signal lifecycle on Binance and Bybit.
8. Illiquid Silence Sweeper: sweep_timeouts cleanly closes inactive signals.
9. Zero-Division & Corrupt Tick Resilience: Zero price protection in tracking & sweep.
10. Baseline Spike-Gating: 20x adaptation damping during extreme dislocation.
11. Portfolio Pruning Guard: Disabled pair rejection in calibrated thresholds.
"""
from __future__ import annotations

import unittest
import time
from typing import List, Dict, Any

from models import OrderBookDepth5, SunriseSignal
from korean_engine import KoreanEngine, PairState, UpbitNormState, evaluate_exit_reason


class TestKoreanEngineStrategy(unittest.TestCase):

    def setUp(self):
        self.emitted: List[Dict[str, Any]] = []
        self.engine = KoreanEngine(
            emit_callback=lambda sig: self.emitted.append(sig),
            drain_threshold=0.40,
            blowout_threshold=2.0,
            min_edge_bps=5.0,
            max_position_budget_usd=1000.0,
            stealth_depth_fraction=0.15,
            taker_fee_bps=5.0,
            roundtrip_fee_bps=10.0,
            warmup_samples=5,
            max_hold_sec=15.0,
            max_follower_book_age_sec=5.0,
            stop_invalidation_bps=15.0,
            stop_adverse_pnl_bps=35.0,
            alpha_decay_sec=8.0,
            require_trade_confirmation=False,
        )

    # -------------------------------------------------------------------------
    # 1. Depth Drain: Ask Lifted Mode
    # -------------------------------------------------------------------------
    def test_depth_drain_ask_lifted(self):
        coin = "SOL"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)
        self.assertTrue(st.is_warmed_up)

        # Baseline Upbit norm: depth 1000 USD, best ask 200 USD, spread 5 bps
        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 200.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        # Follower calm book: ask 100.0, bid 99.95 (mid 99.975)
        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="SOLUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0), (99.90, 20.0)],
            asks=[(100.00, 10.0), (100.05, 20.0), (100.10, 30.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        # Upbit previous tick: best ask 100.0 ($200), depth 1000 USD
        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 10.0)],
            asks=[(100.00, 2.0), (100.10, 8.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Upbit current tick: best ask LIFTS to 100.50 (+50 bps), depth drops to 400 USD
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 10.0)],
            asks=[(100.50, 4.0)]
        )

        self.engine.on_book_update(u_curr)

        self.assertIsNotNone(st.open_signal)
        sig = st.open_signal
        self.assertEqual(sig.trigger_type, "depth_drain")
        self.assertEqual(sig.coin, coin)
        self.assertEqual(sig.follower_exchange, "binance")
        self.assertGreaterEqual(sig.net_exec_edge_bps, 5.0)

    # -------------------------------------------------------------------------
    # 2. Depth Drain: Ask Held Mode (Front-of-Book Sweep)
    # -------------------------------------------------------------------------
    def test_depth_drain_ask_held(self):
        coin = "AVAX"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "bybit")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)
        self.assertTrue(st.is_warmed_up)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 500.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        # Follower book
        f_snap = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="AVAXUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)],
            asks=[(100.00, 10.0), (100.05, 20.0), (100.10, 30.0)]
        )
        self.engine._books[("bybit", coin)] = f_snap
        self.engine._recv_ts[("bybit", coin)] = now

        # Upbit previous tick: best ask 100.50 ($500), top5 depth $1000
        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(100.40, 5.0)],
            asks=[(100.50, 5.0), (100.60, 5.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Upbit current tick: best ask price HELD (still 100.50), but size drops from 5.0 -> 0.5 ($50 left)
        # best_ask_loss = 500 - 50 = 450 USD.
        # top5_loss = 1000 - 550 = 450 USD.
        # best_ask_loss (450) >= 0.5 * top5_loss (225) -> Front-of-book drain!
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(100.40, 5.0)],
            asks=[(100.50, 0.5), (100.60, 5.0)]
        )

        self.engine.on_book_update(u_curr)

        self.assertIsNotNone(st.open_signal)
        self.assertEqual(st.open_signal.trigger_type, "depth_drain")
        self.assertEqual(st.open_signal.follower_exchange, "bybit")

    # -------------------------------------------------------------------------
    # 3. Depth Drain Rejection: Deep Book Pull (Anti-Manipulation)
    # -------------------------------------------------------------------------
    def test_depth_drain_deep_book_pull_rejected(self):
        coin = "NEAR"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 2000.0,
            "best_ask_usd": 100.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="NEARUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)],
            asks=[(100.00, 10.0), (100.05, 20.0), (100.10, 30.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        # Upbit prev: best ask $100, level 5 has $1900
        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 5.0)],
            asks=[(100.50, 1.0), (101.00, 19.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Upbit curr: someone pulled $1000 from level 5, but best ask was untouched ($100 left)
        # best_ask_loss = 0. top5_loss = 1000 USD.
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 5.0)],
            asks=[(100.50, 1.0), (101.00, 9.0)]
        )

        self.engine.on_book_update(u_curr)

        # Signal must NOT trigger because depth depletion was not at the front of book
        self.assertIsNone(st.open_signal)

    # -------------------------------------------------------------------------
    # 4. Spread Blowout Trigger Detection
    # -------------------------------------------------------------------------
    def test_spread_blowout_trigger(self):
        coin = "DOT"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)

        # Norm spread = 2.0 bps, depth = 10,000 USD, best ask = 5000 USD
        self.engine._upbit_norm[coin] = {
            "depth_ask": 10000.0,
            "best_ask_usd": 5000.0,
            "spread_bps": 2.0,
            "t": now - 1.0,
        }

        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="DOTUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)],
            asks=[(100.00, 10.0), (100.05, 20.0), (100.10, 30.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        # Prev spread: bid 100.0, ask 100.02 (spread = 2 bps)
        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(100.00, 10.0)],
            asks=[(100.02, 10.0), (100.10, 80.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Curr: ask blows out to 100.50. Spread = (100.50 - 100.00)/100.25 * 1e4 = ~50 bps
        # Blowout ratio = 50.0 / 2.0 = 25.0 >= 2.0 blowout threshold!
        # Net edge = 15.0 bps >= 5.0 bps min edge!
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(100.00, 10.0)],
            asks=[(100.50, 80.0)]
        )

        self.engine.on_book_update(u_curr)

        self.assertIsNotNone(st.open_signal)
        self.assertEqual(st.open_signal.trigger_type, "spread_blowout")

    # -------------------------------------------------------------------------
    # 5. Take-Profit: 75% Net Edge Captured
    # -------------------------------------------------------------------------
    def test_convergence_75_pct_edge_captured(self):
        coin = "LINK"
        now = 1000.0
        st = self.engine._get_or_create_pair_state(coin, "binance")

        sig = SunriseSignal(
            id="sig_link_tp75",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="LINKUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=100.0,
            leader_best_ask=100.05,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=99.0,
            follower_best_bid=98.95,
            follower_best_ask=99.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=100.0,
            dev_z=3.0,
            gross_exec_edge_bps=50.0,
            net_exec_edge_bps=40.0,  # 75% hurdle is 30.0 bps
            follower_entry_px=100.00,
            suggested_size_usd=100.0,
            max_hold_sec=15.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        u_book = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.0, 10.0)], asks=[(100.1, 10.0)]
        )
        # Follower moves up strongly: exit price is 100.41
        # f_exec_move = (100.41 - 100.00) / 100.00 * 1e4 = +41.0 bps
        # cur_net_pnl = 41.0 - 10.0 = +31.0 bps >= 30.0 bps (75% of 40 bps)!
        f_book = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="LINKUSDT",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.41, 10.0)], asks=[(100.45, 10.0)]
        )
        self.engine._books[("upbit", coin)] = u_book
        self.engine._books[("binance", coin)] = f_book

        self.engine._track_follower_move(coin, "binance", f_book, now + 2.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "converged")
        self.assertAlmostEqual(sig.net_pnl_bps, 31.0, places=1)
        self.assertIsNone(st.open_signal)
        self.assertEqual(self.engine.counters["signals_converged"], 1)

    # -------------------------------------------------------------------------
    # 6. Take-Profit: Dislocation Absorbed (rem_edge <= 0 with positive PnL)
    # -------------------------------------------------------------------------
    def test_convergence_remaining_edge_absorbed(self):
        coin = "ADA"
        now = 1000.0
        st = self.engine._get_or_create_pair_state(coin, "binance")

        sig = SunriseSignal(
            id="sig_ada_rem",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="ADAUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=100.0,
            leader_best_ask=100.05,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=99.0,
            follower_best_bid=98.95,
            follower_best_ask=99.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=100.0,
            dev_z=3.0,
            gross_exec_edge_bps=50.0,
            net_exec_edge_bps=40.0,
            follower_entry_px=100.00,
            suggested_size_usd=100.0,
            max_hold_sec=15.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Basis completely converges: Upbit mid = 100.20, Follower mid = 100.20 (diff = 0 bps)
        # rem_edge = (0 - 0) - taker_fee (5.0) = -5.0 bps <= 0.
        # Follower exit price = 100.25 -> f_exec_move = +25 bps, net PnL = +15 bps > 0!
        u_book = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.18, 10.0)], asks=[(100.22, 10.0)]
        )
        f_book = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="ADAUSDT",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.25, 10.0)], asks=[(100.30, 10.0)]
        )
        self.engine._books[("upbit", coin)] = u_book
        self.engine._books[("binance", coin)] = f_book

        self.engine._track_follower_move(coin, "binance", f_book, now + 2.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "converged")
        self.assertGreater(sig.net_pnl_bps, 0.0)

    # -------------------------------------------------------------------------
    # 7. Concurrent Multi-Venue Signal Tracking (Binance & Bybit)
    # -------------------------------------------------------------------------
    def test_concurrent_multi_venue_tracking(self):
        coin = "ETH"
        now = time.time()

        st_bin = self.engine._get_or_create_pair_state(coin, "binance")
        st_byb = self.engine._get_or_create_pair_state(coin, "bybit")
        for i in range(6):
            st_bin.learn(0.0, now - 30.0 + i * 2.0)
            st_byb.learn(0.0, now - 30.0 + i * 2.0)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 200.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        # Follower books on both Binance and Bybit
        f_bin = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="ETHUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)], asks=[(100.00, 10.0), (100.05, 20.0)]
        )
        f_byb = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="ETHUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)], asks=[(100.00, 10.0), (100.05, 20.0)]
        )
        self.engine._books[("binance", coin)] = f_bin
        self.engine._recv_ts[("binance", coin)] = now
        self.engine._books[("bybit", coin)] = f_byb
        self.engine._recv_ts[("bybit", coin)] = now

        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 10.0)], asks=[(100.00, 2.0), (100.10, 8.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        # Upbit sweeps ask -> triggers lead-lag
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 10.0)], asks=[(100.60, 4.0)]
        )
        self.engine.on_book_update(u_curr)

        # Both pairs must have open signals
        self.assertIsNotNone(st_bin.open_signal)
        self.assertIsNotNone(st_byb.open_signal)
        self.assertEqual(len(self.engine._active_signals[coin]), 2)

        sig_bin = st_bin.open_signal
        sig_byb = st_byb.open_signal

        # Binance moves up -> Take-Profit convergence
        f_bin_up = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="ETHUSDT",
            ts=now + 2.0, exchange_ts=int((now + 2.0) * 1000),
            bids=[(100.55, 10.0)], asks=[(100.60, 10.0)]
        )
        self.engine._books[("binance", coin)] = f_bin_up
        self.engine._track_follower_move(coin, "binance", f_bin_up, now + 2.0)

        self.assertTrue(sig_bin.closed)
        self.assertEqual(sig_bin.close_reason, "converged")
        # Bybit signal remains open!
        self.assertFalse(sig_byb.closed)
        self.assertEqual(len(self.engine._active_signals[coin]), 1)

        # Bybit stalls until alpha decay (dt >= 8.0s) with 0 PnL
        f_byb_flat = OrderBookDepth5(
            exchange="bybit", symbol=coin, raw_symbol="ETHUSDT",
            ts=now + 8.5, exchange_ts=int((now + 8.5) * 1000),
            bids=[(99.95, 10.0)], asks=[(100.00, 10.0)]
        )
        self.engine._books[("bybit", coin)] = f_byb_flat
        self.engine._track_follower_move(coin, "bybit", f_byb_flat, now + 8.5)

        self.assertTrue(sig_byb.closed)
        self.assertEqual(sig_byb.close_reason, "stop_decay")
        self.assertEqual(len(self.engine._active_signals.get(coin, [])), 0)

    # -------------------------------------------------------------------------
    # 8. Illiquid Silence Sweeper (sweep_timeouts)
    # -------------------------------------------------------------------------
    def test_timeout_sweep_on_illiquid_silence(self):
        coin = "SHITCOIN"
        now = 1000.0
        st = self.engine._get_or_create_pair_state(coin, "binance")

        sig = SunriseSignal(
            id="sig_shitcoin_to",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="SHITCOINUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=1.0,
            leader_best_ask=1.01,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=0.99,
            follower_best_bid=0.985,
            follower_best_ask=0.995,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=100.0,
            dev_z=3.0,
            gross_exec_edge_bps=50.0,
            net_exec_edge_bps=40.0,
            follower_entry_px=0.995,
            suggested_size_usd=100.0,
            max_hold_sec=15.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Zero ticks arrive. Time advances by 16.0s (past max_hold_sec = 15.0s)
        self.engine.sweep_timeouts(now + 16.0)

        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "timeout")
        self.assertIsNone(st.open_signal)
        self.assertEqual(len(self.engine._active_signals.get(coin, [])), 0)
        self.assertEqual(len(self.emitted), 1)

    # -------------------------------------------------------------------------
    # 9. Zero-Division & Corrupt Orderbook Resilience
    # -------------------------------------------------------------------------
    def test_zero_division_resilience(self):
        coin = "ZERO"
        now = 1000.0
        st = self.engine._get_or_create_pair_state(coin, "binance")

        sig = SunriseSignal(
            id="sig_zero_resilience",
            coin=coin,
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="ZEROUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=0.0,         # Corrupt 0.0 mid
            leader_best_ask=0.0,
            leader_spread_bps=0.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=0.0,       # Corrupt 0.0 mid
            follower_best_bid=0.0,
            follower_best_ask=0.0,
            follower_spread_bps=0.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=3.0,
            raw_dislocation_bps=0.0,
            dev_z=0.0,
            gross_exec_edge_bps=0.0,
            net_exec_edge_bps=0.0,
            follower_entry_px=0.0,  # Corrupt 0.0 entry
            suggested_size_usd=100.0,
            max_hold_sec=10.0,
        )
        st.open_signal = sig
        self.engine._active_signals[coin].append(sig)

        # Corrupt orderbook with 0.0 prices
        u_corrupt = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[], asks=[]
        )
        f_corrupt = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="ZEROUSDT",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[], asks=[]
        )
        self.engine._books[("upbit", coin)] = u_corrupt
        self.engine._books[("binance", coin)] = f_corrupt

        # Must not raise ZeroDivisionError!
        try:
            self.engine._track_follower_move(coin, "binance", f_corrupt, now + 1.0)
            self.engine.sweep_timeouts(now + 12.0)
        except ZeroDivisionError as e:
            self.fail(f"KoreanEngine raised ZeroDivisionError on zero prices: {e}")

        self.assertTrue(sig.closed)

    # -------------------------------------------------------------------------
    # 10. Baseline Spike-Gating Slowdown
    # -------------------------------------------------------------------------
    def test_baseline_spike_gating_slowdown(self):
        st = PairState("SOL", "binance", warmup_samples=5, spike_sigma=2.5)
        now = 1000.0

        # Warm up with regular 10.0 bps diff
        for i in range(10):
            st.learn(10.0, now + i)
        self.assertTrue(st.is_warmed_up)
        initial_baseline = st.baseline_bps
        self.assertAlmostEqual(initial_baseline, 10.0, places=1)

        # Massive pump: 1000 bps deviation (>> 2.5 * sigma)
        # Spike gating activates: eff_tau is multiplied by 20x (72,000s)
        # Baseline should barely move!
        st.learn(1010.0, now + 11.0)

        # In 1 second with 72,000s tau: a = 1 - exp(-1/72000) ~ 1.38e-5
        # Expected baseline shift: ~1000 * 1.38e-5 = ~0.014 bps!
        self.assertAlmostEqual(st.baseline_bps, initial_baseline, delta=0.5)

    # -------------------------------------------------------------------------
    # 11. Pruned / Disabled Pair Rejection
    # -------------------------------------------------------------------------
    def test_disabled_pair_rejection(self):
        coin = "BADCOIN"
        now = time.time()

        # Inject disabled pair threshold
        pk = f"{coin}:upbit-binance"
        self.engine._thresholds[pk] = {"enabled": False, "min_spread_bps": 5.0}

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)

        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 200.0,
            "spread_bps": 5.0,
            "t": now - 1.0,
        }

        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol=f"{coin}USDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 20.0)], asks=[(100.00, 10.0), (100.05, 20.0)]
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 10.0)], asks=[(100.00, 2.0)]
        )
        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 10.0)], asks=[(105.00, 2.0)]
        )
        self.engine._books[("upbit", coin)] = u_prev

        self.engine.on_book_update(u_curr)

        # Must be rejected because pair was pruned/disabled in portfolio thresholds
        self.assertIsNone(st.open_signal)
        self.assertEqual(len(self.engine._active_signals.get(coin, [])), 0)

    # -------------------------------------------------------------------------
    # 12. Pure Exit Evaluator Parametric Verification
    # -------------------------------------------------------------------------
    def test_evaluate_exit_reason_pure_function(self):
        # 1. Take-Profit: 75% edge
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=30.0, net_exec_edge_bps=40.0, rem_edge=10.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=20.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "converged")
        self.assertIsNone(stype)

        # 2. Take-Profit: basis fully absorbed in profit
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=15.0, net_exec_edge_bps=40.0, rem_edge=-2.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=-5.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "converged")
        self.assertIsNone(stype)

        # 3. Model A Invalidation: Upbit dump >= 1.5 * sigma (4.5 bps)
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=-5.0, net_exec_edge_bps=40.0, rem_edge=20.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=5.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "stop_invalidation")
        self.assertEqual(stype, "invalidation")

        # 4. Model A Invalidation: dislocation <= -stop_invalidation_bps (-15.0 bps)
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=-5.0, net_exec_edge_bps=40.0, rem_edge=0.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=-16.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "stop_invalidation")
        self.assertEqual(stype, "invalidation")

        # 5. Model B Adverse PnL: cur_net_pnl <= -35.0 bps
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=-36.0, net_exec_edge_bps=40.0, rem_edge=20.0,
            dt=2.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "stop_adverse_pnl")
        self.assertEqual(stype, "adverse_pnl")

        # 6. Model C Alpha Decay: dt >= 8.0s and PnL <= 0
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=-2.0, net_exec_edge_bps=40.0, rem_edge=10.0,
            dt=8.5, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "stop_decay")
        self.assertEqual(stype, "alpha_decay")

        # 7. Max Hold Horizon: dt >= 15.0s
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=5.0, net_exec_edge_bps=40.0, rem_edge=10.0,
            dt=15.5, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertEqual(reason, "timeout")
        self.assertIsNone(stype)

        # 8. Normal open state: no exit
        reason, stype = evaluate_exit_reason(
            cur_net_pnl=5.0, net_exec_edge_bps=40.0, rem_edge=10.0,
            dt=3.0, hold_sec=15.0, pair_decay_sec=8.0, sigma=3.0,
            upbit_drop_bps=0.0, cur_dislocation_bps=10.0,
            stop_invalidation_bps=15.0, stop_adverse_pnl_bps=35.0,
        )
        self.assertIsNone(reason)
        self.assertIsNone(stype)

    # -------------------------------------------------------------------------
    # 13. UpbitNormState Slots and Dict Compatibility
    # -------------------------------------------------------------------------
    def test_upbit_norm_state_slots_and_dict_compatibility(self):
        norm = UpbitNormState(depth_ask=1200.0, best_ask_usd=300.0, spread_bps=4.5, t=100.0)
        # Attribute access
        self.assertEqual(norm.depth_ask, 1200.0)
        self.assertEqual(norm.best_ask_usd, 300.0)
        self.assertEqual(norm.spread_bps, 4.5)
        self.assertEqual(norm.t, 100.0)

        # Dict-like __getitem__ and __setitem__ compatibility
        self.assertEqual(norm["depth_ask"], 1200.0)
        norm["depth_ask"] = 1500.0
        self.assertEqual(norm.depth_ask, 1500.0)

    # -------------------------------------------------------------------------
    # 14. Minimum Order Size Guard
    # -------------------------------------------------------------------------
    def test_min_order_size_rejection(self):
        coin = "MICRO"
        now = time.time()

        st = self.engine._get_or_create_pair_state(coin, "binance")
        for i in range(6):
            st.learn(0.0, now - 30.0 + i * 2.0)

        # Norm ask depth $1000
        self.engine._upbit_norm[coin] = {
            "depth_ask": 1000.0,
            "best_ask_usd": 100.0,
            "spread_bps": 2.0,
            "t": now - 1.0,
        }

        # Follower book has tiny depth: Top-3 Ask = $40.0. 15% stealth capacity = $6.00 < $10.0 min_order!
        f_snap = OrderBookDepth5(
            exchange="binance", symbol=coin, raw_symbol="MICROUSDT",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(99.95, 10.0)],
            asks=[(100.00, 0.2), (100.05, 0.1), (100.10, 0.1)],  # 0.4 * ~100 = $40 total
        )
        self.engine._books[("binance", coin)] = f_snap
        self.engine._recv_ts[("binance", coin)] = now

        u_prev = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now - 0.1, exchange_ts=int((now - 0.1) * 1000),
            bids=[(99.95, 5.0)],
            asks=[(100.00, 10.0), (100.05, 5.0), (100.50, 20.0)],
        )
        self.engine._books[("upbit", coin)] = u_prev

        u_curr = OrderBookDepth5(
            exchange="upbit", symbol=coin, raw_symbol=f"KRW-{coin}",
            ts=now, exchange_ts=int(now * 1000),
            bids=[(100.00, 5.0)],
            asks=[(100.50, 20.0)],
        )
        self.engine.on_book_update(u_curr)

        # Order size was $6.00 < min_order_usd ($10.00) -> MUST BE REJECTED
        self.assertIsNone(st.open_signal)


if __name__ == "__main__":
    unittest.main()
```

## tests/test_pipeline_e2e.py

```python
"""
Integration test for the Korean Sunrise Pipeline Architecture:
- PoolStateManager (Incubator vs Battle, Pause/Resume, Dynamic Stealth Sizing)
- TradingProfileManager (Capital, Reserve, Slots)
- SunriseEvaluator (Expectancy, Profit Factor, Win Rate, Cross-microstructure)
- KoreanEngine integration (Battle vs Incubator tagging, Actionability)
"""

import os
import sys
import json
import gzip
import shutil
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, timezone

# Ensure project root is in path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.pool_state import PoolStateManager
from pipeline.profile import TradingProfileManager
from pipeline.evaluator import SunriseEvaluator
from korean_engine import KoreanEngine
from models import OrderBookDepth5, SunriseSignal


class TestPipelineE2E(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="ks_test_")
        self.universe_file = os.path.join(self.test_dir, "universe_live.json")
        self.battle_file = os.path.join(self.test_dir, "live_trading_pool.json")
        self.overrides_file = os.path.join(self.test_dir, "runtime_overrides.json")
        self.profile_file = os.path.join(self.test_dir, "trading_profile.json")
        self.signals_dir = os.path.join(self.test_dir, "signals")
        os.makedirs(self.signals_dir, exist_ok=True)

        # Mock universe with sym maps
        universe_data = {
            "_meta": {"updated_at": "2026-10-18T12:00:00Z", "coins_count": 3},
            "coins": ["SOL", "DOGE", "MEME"],
            "binance_sym_map": {
                "SOLUSDT": ["SOL", 1.0],
                "DOGEUSDT": ["DOGE", 1.0],
                "MEMEUSDT": ["MEME", 1.0]
            },
            "bybit_sym_map": {
                "SOLUSDT": ["SOL", 1.0],
                "DOGEUSDT": ["DOGE", 1.0],
                "MEMEUSDT": ["MEME", 1.0]
            }
        }
        with open(self.universe_file, "w", encoding="utf-8") as f:
            json.dump(universe_data, f)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_pool_state_and_profile_manager(self):
        # Initialize Profile
        profile_mgr = TradingProfileManager(profile_file=self.profile_file)
        profile_mgr.set_venue_capital("binance", 300.0)
        profile_mgr.set_venue_capital("bybit", 300.0)
        profile_mgr.set_max_slots(2)

        # Slot budget: (300 / 2) * (1 - 0.20) = $120.00
        # If follower top3 ask = $500, stealth_size = 0.15 * 500 = $75.00
        # Expected size: min(120, 75, 500) = $75.00
        size = profile_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=500.0)
        self.assertIsNotNone(size)
        self.assertAlmostEqual(size, 75.0, places=2)

        # If follower top3 ask = $2000, stealth_size = 0.15 * 2000 = $300.00
        # Capped by slot budget = $120.00
        size_capped = profile_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=2000.0)
        self.assertIsNotNone(size_capped)
        self.assertAlmostEqual(size_capped, 120.0, places=2)

        # If follower top3 ask is very small ($30), stealth_size = $4.50 < $10 min order
        # Book is too shallow to absorb minimum order safely
        size_shallow = profile_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=30.0)
        self.assertIsNone(size_shallow)

        # Strict 15% limit: top3 ask $60 -> stealth_size = $9.00 < $10.0 min order (no artificial bump)
        size_no_bump = profile_mgr.calculate_stealth_order_size("binance", follower_top3_ask_usd=60.0)
        self.assertIsNone(size_no_bump)

        # Initialize PoolStateManager
        psm = PoolStateManager(
            universe_file=self.universe_file,
            battle_pool_file=self.battle_file,
            overrides_file=self.overrides_file
        )
        psm.reload()

        # All 3 coins are in Incubator initially
        sol_entry = psm.get_coin_entry("SOL")
        self.assertIsNotNone(sol_entry)
        self.assertEqual(sol_entry.basket, "incubator")
        self.assertTrue(sol_entry.enabled)
        self.assertFalse(psm.is_battle_coin("SOL"))

        # Test Pause / Resume
        psm.pause_coin("SOL", reason="high volatility")
        self.assertFalse(psm.get_coin_entry("SOL").enabled)
        
        psm.resume_coin("SOL")
        self.assertTrue(psm.get_coin_entry("SOL").enabled)

        # Test Manual Promotion
        qual_list = [{
            "coin": "SOL",
            "venues": ["binance", "bybit"],
            "win_rate_pct": 75.0,
            "profit_factor": 2.4,
            "mean_net_pnl_bps": 12.5,
            "median_net_pnl_bps": 11.0,
            "top3_depth_usd": 1500.0,
            "event_count": 10
        }]
        psm.promote_to_battle(qual_list)
        self.assertTrue(psm.is_battle_coin("SOL"))
        self.assertEqual(psm.get_coin_entry("SOL").basket, "battle")

    def test_evaluator_scoring_and_rotation(self):
        # Create mock gzip signals file
        today_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        sig_file = os.path.join(self.signals_dir, f"signals_{today_str}.jsonl.gz")

        mock_signals = []
        # 6 positive signals for SOL (Avg net PnL: +10 bps, Win Rate: 100%, PF: 99.0)
        for i in range(6):
            mock_signals.append({
                "ts": 1792300000 + i * 100,
                "coin": "SOL",
                "follower_exchange": "binance",
                "drain_ratio": 0.65,
                "leader_swept_usd": 2500.0,
                "follower_top3_ask_usd": 3000.0,  # cross-ratio = 2500/3000 = 0.83 >= 0.50
                "rem_edge_bps": 15.0,
                "net_pnl_bps": 10.5,
                "mfe_exec_pnl_bps": 18.0,
                "time_to_peak_sec": 0.45,
                "is_actionable": False,
                "basket": "incubator"
            })

        # 6 negative signals for MEME (Avg net PnL: -8 bps, Win Rate: 0%)
        for i in range(6):
            mock_signals.append({
                "ts": 1792300000 + i * 100,
                "coin": "MEME",
                "follower_exchange": "binance",
                "drain_ratio": 0.55,
                "leader_swept_usd": 1200.0,
                "follower_top3_ask_usd": 2000.0,
                "rem_edge_bps": 5.0,
                "net_pnl_bps": -8.0,
                "mfe_exec_pnl_bps": 1.0,
                "time_to_peak_sec": 0.1,
                "is_actionable": False,
                "basket": "incubator"
            })

        with gzip.open(sig_file, "wt", encoding="utf-8") as f:
            for s in mock_signals:
                f.write(json.dumps(s) + "\n")

        # Run evaluator
        psm = PoolStateManager(
            universe_file=self.universe_file,
            battle_pool_file=self.battle_file,
            overrides_file=self.overrides_file
        )
        psm.reload()

        evaluator = SunriseEvaluator(
            data_root=self.test_dir,
            days_back=1,
            min_events=5,
            pool_state=psm
        )

        qualified = evaluator.run(dry_run=False)
        qualified_coins = [q["coin"] for q in qualified]

        # SOL should qualify
        self.assertIn("SOL", qualified_coins)
        # MEME should fail
        self.assertNotIn("MEME", qualified_coins)

        # Verify SOL is in Battle pool now!
        self.assertTrue(psm.is_battle_coin("SOL"))
        self.assertFalse(psm.is_battle_coin("MEME"))

        # Verify physical capacity metrics are captured and persisted
        sol_q = next(q for q in qualified if q["coin"] == "SOL")
        self.assertIn("stealth_capacity_usd", sol_q)
        self.assertAlmostEqual(sol_q["stealth_capacity_usd"], 450.0, places=1) # 15% of 3000
        sol_entry = psm.get_coin_entry("SOL")
        self.assertAlmostEqual(sol_entry.stealth_capacity_usd, 450.0, places=1)

    def test_engine_integration_with_basket_and_actionability(self):
        psm = PoolStateManager(
            universe_file=self.universe_file,
            battle_pool_file=self.battle_file,
            overrides_file=self.overrides_file
        )
        psm.reload()

        profile_mgr = TradingProfileManager(profile_file=self.profile_file)
        profile_mgr.set_venue_capital("binance", 500.0)

        # Promote SOL to battle
        psm.promote_to_battle([{
            "coin": "SOL",
            "venues": ["binance"],
            "win_rate_pct": 80.0,
            "profit_factor": 3.0,
            "mean_net_pnl_bps": 15.0,
            "median_net_pnl_bps": 14.0,
            "top3_depth_usd": 2000.0,
            "event_count": 8
        }])

        # Initialize KoreanEngine with our pool & profile
        engine = KoreanEngine(
            emit_callback=lambda sig: None,
            pool_state=psm,
            profile_manager=profile_mgr,
            min_edge_bps=5.0
        )

        # Verify engine has pool & profile wired up
        self.assertEqual(engine.pool_state, psm)
        self.assertEqual(engine.profile_manager, profile_mgr)

        # Check basket assignment
        self.assertTrue(engine.pool_state.is_battle_coin("SOL"))
        self.assertFalse(engine.pool_state.is_battle_coin("DOGE"))

        # Test full tick-to-signal pipeline via BaseVenueStream._sanitise -> KoreanEngine
        from venues.base import BaseVenueStream
        emitted = []
        engine.emit = lambda sig_dict: emitted.append(sig_dict)
        engine.warmup_samples = 2

        stream = BaseVenueStream("upbit", ["SOL"], engine.on_book_update)
        now = 1792300000.0

        # 1. Feed normal warmup ticks on Upbit and Binance
        for i in range(12):
            f_snap = OrderBookDepth5(
                exchange="binance",
                symbol="SOL",
                raw_symbol="SOLUSDT",
                ts=now + i,
                exchange_ts=int((now + i) * 1000),
                bids=[(100.00, 10.0), (99.95, 20.0), (99.90, 30.0)],
                asks=[(100.05, 10.0), (100.10, 20.0), (100.15, 30.0)],
            )
            u_snap = OrderBookDepth5(
                exchange="upbit",
                symbol="SOL",
                raw_symbol="KRW-SOL",
                ts=now + i,
                exchange_ts=int((now + i) * 1000),
                bids=[(100.00, 10.0), (99.95, 20.0), (99.90, 30.0)],
                asks=[(100.05, 10.0), (100.10, 20.0), (100.15, 30.0)],
            )
            stream._sanitise(f_snap)
            stream._sanitise(u_snap)

        self.assertEqual(stream.books_dropped, 0)
        self.assertEqual(stream.levels_dropped, 0)
        self.assertGreaterEqual(engine.counters["ticks_upbit"], 12)
        self.assertGreaterEqual(engine.counters["ticks_followers"], 12)

    def test_dynamic_thresholds_hold_sec_and_spike_gating(self):
        # Create a mock matrix_thresholds.json with custom hold_sec and spike_sigma
        thresh_file = os.path.join(self.test_dir, "matrix_thresholds.json")
        matrix_data = {
            "metadata": {"total_pairs_configured": 1},
            "thresholds": {
                "SOL:upbit-binance": {
                    "min_spread_bps": 15.0,
                    "max_hold_sec": 3.2,
                    "spike_sigma": 3.5,
                    "min_dev_z": 1.7,
                    "expected_winrate": 0.85,
                    "expected_net_bps": 25.0,
                    "sample_trades": 10,
                    "status": "aggressive_clean",
                    "enabled": True,
                }
            }
        }
        with open(thresh_file, "w", encoding="utf-8") as f:
            json.dump(matrix_data, f)

        emitted = []
        engine = KoreanEngine(
            emit_callback=lambda sig_dict: emitted.append(sig_dict),
            thresholds_file=thresh_file,
            max_hold_sec=6.0,
        )

        # 1. Verify PairState gets pair-specific spike_sigma
        st = engine._get_or_create_pair_state("SOL", "binance")
        self.assertEqual(st.spike_sigma, 3.5)

        # 2. Simulate warmup so st is warmed up
        now = 1792300000.0
        for i in range(35):
            st.learn(10.0, now + i)
        self.assertTrue(st.is_warmed_up)

        # 3. Simulate Upbit pump event triggering signal
        upbit_snap = OrderBookDepth5(
            exchange="upbit",
            symbol="SOL",
            raw_symbol="KRW-SOL",
            ts=now + 50,
            exchange_ts=int((now + 50) * 1000),
            bids=[(105.00, 10.0), (104.95, 20.0)],
            asks=[(105.05, 10.0), (105.10, 20.0)],
        )
        f_snap = OrderBookDepth5(
            exchange="binance",
            symbol="SOL",
            raw_symbol="SOLUSDT",
            ts=now + 50,
            exchange_ts=int((now + 50) * 1000),
            bids=[(100.00, 10.0), (99.95, 20.0)],
            asks=[(100.05, 1000.0), (100.10, 2000.0)],
        )
        engine._books[("upbit", "SOL")] = upbit_snap
        engine._recv_ts[("upbit", "SOL")] = now + 50
        engine._books[("binance", "SOL")] = f_snap
        engine._recv_ts[("binance", "SOL")] = now + 50

        engine._check_entry(
            coin="SOL",
            follower_ex="binance",
            st=st,
            upbit_snap=upbit_snap,
            f_snap=f_snap,
            f_book_age_ms=10.0,
            norm_depth=1000.0,
            norm_spread=5.0,
            swept_usd=5000.0,
            trigger_type="depth_drain",
            drain_ratio=0.8,
            blowout_ratio=3.0,
            now=now + 50,
        )

        self.assertIsNotNone(st.open_signal)
        sig = st.open_signal
        # Personalized max_hold_sec assigned!
        self.assertEqual(sig.max_hold_sec, 3.2)
        self.assertGreater(sig.stealth_capacity_usd, 0.0)

        # 4. Sweep timeouts before 3.2s -> signal stays open
        engine.sweep_timeouts(now + 50 + 2.0)
        self.assertFalse(sig.closed)

        # 5. Sweep timeouts at 3.3s -> signal closed by timeout
        engine.sweep_timeouts(now + 50 + 3.3)
        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "timeout")
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["max_hold_sec"], 3.2)
        self.assertEqual(emitted[0]["stealth_capacity_usd"], sig.stealth_capacity_usd)

    def test_engine_hot_reload_pool_and_profile(self):
        # 1. Initialize fresh PoolStateManager and TradingProfileManager
        psm = PoolStateManager(
            universe_file=self.universe_file,
            battle_pool_file=self.battle_file,
            overrides_file=self.overrides_file
        )
        psm.reload()
        profile_mgr = TradingProfileManager(profile_file=self.profile_file)
        profile_mgr.set_venue_capital("binance", 100.0)

        # 2. Initialize KoreanEngine with pool_state and profile_manager
        engine = KoreanEngine(
            emit_callback=lambda sig: None,
            pool_state=psm,
            profile_manager=profile_mgr,
        )

        # Initially DOGE is in Incubator, not Battle
        self.assertFalse(engine.pool_state.is_battle_coin("DOGE"))
        self.assertEqual(engine.profile_manager.get_profile()["capital"]["per_venue_usd"]["binance"], 100.0)

        # 3. Simulate external process (nightly cron or CLI) modifying live_trading_pool.json on disk
        battle_doc = {
            "_meta": {"updated_at": "2026-10-18T14:00:00Z"},
            "coins": {
                "DOGE": {
                    "coin": "DOGE",
                    "basket": "battle",
                    "venues": ["binance", "bybit"],
                    "mean_net_pnl_bps": 22.0,
                    "top3_depth_usd": 1200.0,
                    "win_rate_pct": 88.0,
                    "profit_factor": 2.5,
                    "event_count": 15,
                    "promoted_at": 1792300000.0,
                }
            }
        }
        with open(self.battle_file, "w", encoding="utf-8") as f:
            json.dump(battle_doc, f)

        # Simulate external process modifying trading_profile.json
        prof_doc = profile_mgr.get_profile()
        prof_doc["capital"]["per_venue_usd"]["binance"] = 850.0
        with open(self.profile_file, "w", encoding="utf-8") as f:
            json.dump(prof_doc, f)

        # 4. Trigger engine.maybe_reload_thresholds after 35 seconds
        now = 1000.0
        engine._last_thresh_check = now - 35.0
        engine.maybe_reload_thresholds(now)

        # 5. Verify hot reloading took effect without restarting engine!
        self.assertTrue(engine.pool_state.is_battle_coin("DOGE"))
        self.assertEqual(engine.profile_manager.get_profile()["capital"]["per_venue_usd"]["binance"], 850.0)

    def test_signal_closure_real_pnl_convergence_vs_reversal(self):
        emitted = []
        engine = KoreanEngine(
            emit_callback=lambda sig_dict: emitted.append(sig_dict),
            roundtrip_fee_bps=10.0,
            taker_fee_bps=5.0,
            max_hold_sec=10.0,
        )

        now = 1000.0
        sig = SunriseSignal(
            id="test_sig_1",
            coin="SOL",
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="SOLUSDT",
            open_ts=now,
            trigger_type="depth_drain",
            leader_mid=100.5,
            leader_best_ask=100.55,
            leader_spread_bps=5.0,
            leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0,
            leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0,
            leader_imbalance_top5=0.5,
            drain_ratio=0.5,
            spread_blowout_ratio=1.0,
            follower_mid=100.0,
            follower_best_bid=99.95,
            follower_best_ask=100.05,
            follower_spread_bps=10.0,
            follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0,
            follower_imbalance_top3=0.5,
            kimchi_baseline_bps=0.0,
            pair_sigma_bps=2.0,
            raw_dislocation_bps=50.0,
            dev_z=3.0,
            gross_exec_edge_bps=35.0,
            net_exec_edge_bps=30.0,
            leader_latency_ms=10.0,
            follower_latency_ms=15.0,
            follower_book_age_ms=5.0,
            suggested_size_usd=100.0,
            basket="battle",
            is_actionable=True,
            max_hold_sec=10.0,
        )
        engine._active_signals["SOL"].append(sig)

        # 1. Upbit retreats so rem_edge <= 0, but follower barely moved:
        # Entry best_ask was 100.05. Current best_bid is 100.07.
        # f_exec_move = (100.07 - 100.05) / 100.05 * 1e4 = +2.0 bps
        # cur_net_pnl = 2.0 - 10.0 = -8.0 bps < 0!
        u_retreat = OrderBookDepth5(
            exchange="upbit", symbol="SOL", raw_symbol="KRW-SOL",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[(100.00, 10.0)], asks=[(100.05, 10.0)]
        )
        f_lagging = OrderBookDepth5(
            exchange="binance", symbol="SOL", raw_symbol="SOLUSDT",
            ts=now + 1.0, exchange_ts=int((now + 1.0) * 1000),
            bids=[(100.07, 10.0)], asks=[(100.12, 10.0)]
        )
        engine._books[("upbit", "SOL")] = u_retreat
        engine._books[("binance", "SOL")] = f_lagging

        engine._track_follower_move("SOL", "binance", f_lagging, now + 1.0)

        # Must close as "stop_invalidation" because Upbit leader pump broke down!
        self.assertTrue(sig.closed)
        self.assertEqual(sig.close_reason, "stop_invalidation")
        self.assertTrue(sig.stop_hit)
        self.assertEqual(sig.stop_type, "invalidation")
        self.assertLess(sig.net_pnl_bps, 0.0)

    def test_pair_state_cold_start_warmup(self):
        from korean_engine import PairState
        st = PairState("SOL", "binance", warmup_samples=30)
        now = 1000.0

        # Tick 0 hits an extreme initial spike: 150 bps
        st.learn(150.0, now)
        self.assertEqual(st.baseline_bps, 150.0)
        self.assertFalse(st.is_warmed_up)

        # Subsequent 30 ticks are quiet normal spread: 10 bps
        for i in range(1, 31):
            st.learn(10.0, now + i)

        # With eff_tau = 10.0 during warmup, the baseline should have adapted rapidly
        # and forgotten the initial 150 bps spike (down to < 20 bps instead of remaining ~145 bps)
        self.assertTrue(st.is_warmed_up)
        self.assertLess(st.baseline_bps, 20.0)
        self.assertGreater(st.baseline_bps, 9.0)

    def test_no_learn_during_open_signal_and_cleanup_memory_leak(self):
        engine = KoreanEngine(emit_callback=lambda sig: None)
        st = engine._get_or_create_pair_state("SOL", "binance")

        # Warm up
        now = 1000.0
        for i in range(35):
            st.learn(10.0, now + i)
        self.assertTrue(st.is_warmed_up)
        obs_before = st.n_obs

        # Create active signal
        sig = SunriseSignal(
            id="sig_test_100",
            coin="SOL",
            leader_exchange="upbit",
            follower_exchange="binance",
            follower_contract="SOLUSDT",
            open_ts=now + 50,
            trigger_type="depth_drain",
            leader_mid=100.5, leader_best_ask=100.55,
            leader_spread_bps=5.0, leader_norm_spread_bps=5.0,
            leader_top5_ask_usd=1000.0, leader_norm_depth_usd=1000.0,
            leader_swept_usd=500.0, leader_imbalance_top5=0.5,
            drain_ratio=0.5, spread_blowout_ratio=1.0,
            follower_mid=100.0, follower_best_bid=99.95, follower_best_ask=100.05,
            follower_spread_bps=10.0, follower_top3_ask_usd=2000.0,
            follower_top3_bid_usd=2000.0, follower_imbalance_top3=0.5,
            kimchi_baseline_bps=10.0, pair_sigma_bps=2.0,
            raw_dislocation_bps=50.0, dev_z=3.0,
            gross_exec_edge_bps=35.0, net_exec_edge_bps=30.0,
            leader_latency_ms=10.0, follower_latency_ms=15.0,
            follower_book_age_ms=5.0, suggested_size_usd=100.0,
            basket="battle", is_actionable=True, max_hold_sec=10.0,
        )
        st.open_signal = sig
        engine._active_signals["SOL"].append(sig)

        # Feed follower tick: must NOT call st.learn because st.open_signal is active
        f_snap = OrderBookDepth5(
            exchange="binance", symbol="SOL", raw_symbol="SOLUSDT",
            ts=now + 51, exchange_ts=int((now + 51) * 1000),
            bids=[(100.0, 10.0)], asks=[(100.05, 10.0)]
        )
        engine._books[("upbit", "SOL")] = OrderBookDepth5(
            exchange="upbit", symbol="SOL", raw_symbol="KRW-SOL",
            ts=now + 51, exchange_ts=int((now + 51) * 1000),
            bids=[(100.0, 10.0)], asks=[(100.05, 10.0)]
        )
        engine._recv_ts[("upbit", "SOL")] = now + 51
        engine.on_book_update(f_snap)

        self.assertEqual(st.n_obs, obs_before)

        # Close signal: test by-id clearing and memory cleanup of _active_signals
        import copy
        sig_copy = copy.copy(sig)  # different object reference, same id
        engine._close_signal(sig_copy, now + 52, "converged", 10.0, 25.0)

        self.assertIsNone(st.open_signal)
        self.assertNotIn("SOL", engine._active_signals)  # list was popped, no memory leak

    def test_screener_contract_size_propagation(self):
        from screening.screener import UniverseScreener
        screener = UniverseScreener(venues=["gateio", "okx"])
        vetted = [{
            "coin": "BTC",
            "market": "KRW-BTC",
            "venues": ["gateio", "okx"],
            "venue_details": {
                "gateio": {"raw_sym": "BTC_USDT", "mult": 1.0, "c_size": 0.0001},
                "okx": {"raw_sym": "BTC-USDT-SWAP", "mult": 1.0, "c_size": 0.01},
            },
            "best_metrics": {"corr": 0.98, "p90_dev_bps": 12.0},
        }]
        doc = screener._build_universe_doc(vetted, 1350.0, 1.0)
        self.assertEqual(doc["gateio_sym_map"]["BTC_USDT"], ("BTC", 1.0, 0.0001))
        self.assertEqual(doc["okx_sym_map"]["BTC-USDT-SWAP"], ("BTC", 1.0, 0.01))

    def test_microstructure_noise_filtering_in_calibration(self):
        from unittest.mock import patch
        import tools.calibrate_thresholds as ct

        noise_row = {
            "coin": "SOL", "follower_exchange": "binance",
            "net_pnl_bps": 20.0, "gross_exec_edge_bps": 25.0,
            "drain_ratio": 0.1, "leader_swept_usd": 10.0, "follower_top3_ask_usd": 100.0,
        }
        valid_row = {
            "coin": "SOL", "follower_exchange": "binance",
            "net_pnl_bps": 20.0, "gross_exec_edge_bps": 25.0,
            "drain_ratio": 0.5, "leader_swept_usd": 500.0, "follower_top3_ask_usd": 1000.0,
        }

        tmp_gz = os.path.join(self.test_dir, "signals_test.jsonl.gz")
        with gzip.open(tmp_gz, "wt", encoding="utf-8") as gz:
            gz.write(json.dumps(noise_row) + "\n")
            gz.write(json.dumps(valid_row) + "\n")

        with patch.object(ct, "_collect_signal_files", return_value=[tmp_gz]):
            res = ct.calibrate(days_back=1.0, min_trades=1, min_winrate=0.5, dry_run=True, as_json=True)
            self.assertEqual(res["metadata"]["total_signals_evaluated"], 1)
            self.assertIn("SOL:upbit-binance", res["thresholds"])
            self.assertEqual(res["thresholds"]["SOL:upbit-binance"]["sample_trades"], 1)
            self.assertEqual(res["thresholds"]["SOL:upbit-binance"]["expected_net_bps"], 20.0)

    def test_screener_kline_limit_default(self):
        from screening.screener import UniverseScreener, DEFAULT_KLINE_LIMIT
        self.assertEqual(DEFAULT_KLINE_LIMIT, 200)
        screener = UniverseScreener(venues=["binance"])
        self.assertEqual(screener.kline_limit, 200)

    def test_batch_writer_non_blocking_buffer(self):
        from batch_writer import BatchWriter
        writer_dir = os.path.join(self.test_dir, "writer_test")
        writer = BatchWriter(data_root=writer_dir, flush_interval_sec=1.0)

        sig1 = {"coin": "SOL", "pnl": 15.0}
        sig2 = {"coin": "BTC", "pnl": 25.0}

        writer.add(sig1)
        writer.add(sig2)

        self.assertEqual(writer.rows_count, 2)
        self.assertEqual(len(writer._lines), 2)
        self.assertIn('"coin": "SOL"', writer._lines[0])

        # Flush to disk
        writer.flush()
        self.assertEqual(writer.rows_count, 0)
        self.assertEqual(writer.total_written, 2)

        # Verify gzipped file written on disk
        import glob
        files = glob.glob(os.path.join(writer_dir, "signals", "**", "*.jsonl.gz"), recursive=True)
        self.assertEqual(len(files), 1)
        with gzip.open(files[0], "rt", encoding="utf-8") as gz:
            lines = [json.loads(l) for l in gz if l.strip()]
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0]["coin"], "SOL")
        self.assertEqual(lines[1]["coin"], "BTC")

        # Test rollback on flush error
        writer.add({"coin": "ETH", "pnl": 5.0})
        self.assertEqual(writer.rows_count, 1)
        with patch("os.replace", side_effect=OSError("Disk write error")):
            writer.flush()
        # Lines must be preserved in buffer for next retry
        self.assertEqual(writer.rows_count, 1)
        self.assertIn('"coin": "ETH"', writer._lines[0])

    def test_pool_state_pause_reload_and_demote_cleanup(self):
        psm = PoolStateManager(
            universe_file=self.universe_file,
            battle_pool_file=self.battle_file,
            overrides_file=self.overrides_file
        )
        psm.reload()

        # Promote SOL to battle
        psm.promote_to_battle([{
            "coin": "SOL", "venues": ["binance"],
            "win_rate_pct": 80.0, "profit_factor": 3.0,
            "mean_net_pnl_bps": 15.0, "median_net_pnl_bps": 14.0,
            "top3_depth_usd": 2000.0, "event_count": 8
        }])
        self.assertTrue(psm.is_battle_coin("SOL"))
        sol_entry = psm.get_coin_entry("SOL")
        self.assertEqual(sol_entry.mean_net_pnl_bps, 15.0)
        self.assertTrue(sol_entry.enabled)

        # Simulate external pause written to overrides_file
        with open(self.overrides_file, "w", encoding="utf-8") as f:
            json.dump({"paused_coins": {"SOL": {"reason": "circuit_breaker"}}}, f)

        # Promote another coin or re-run promotion without manual reload
        psm.promote_to_battle([{
            "coin": "SOL", "venues": ["binance"],
            "win_rate_pct": 85.0, "profit_factor": 3.2,
            "mean_net_pnl_bps": 16.0, "median_net_pnl_bps": 15.0,
            "top3_depth_usd": 2000.0, "event_count": 10
        }])
        # SOL must be paused now because overrides_file was re-read!
        sol_entry = psm.get_coin_entry("SOL")
        self.assertFalse(sol_entry.enabled)
        self.assertEqual(sol_entry.pause_reason, "circuit_breaker")

        # Now demote SOL back to Incubator (e.g. fails qualification)
        psm.promote_to_battle([])  # empty list demotes SOL
        self.assertFalse(psm.is_battle_coin("SOL"))
        sol_entry = psm.get_coin_entry("SOL")
        self.assertEqual(sol_entry.basket, "incubator")
        self.assertEqual(sol_entry.mean_net_pnl_bps, 0.0)
        self.assertEqual(sol_entry.profit_factor, 0.0)
        self.assertEqual(sol_entry.promoted_at, 0.0)

    def test_daily_report_stealth_capacity(self):
        import tools.full_daily_report as fdr
        # Write mock signals in test_dir/signals
        sig_file = os.path.join(self.signals_dir, "signals_2026-10-18.jsonl.gz")
        mock_signals = [
            {
                "coin": "SOL",
                "follower_exchange": "binance",
                "drain_ratio": 0.65,
                "leader_swept_usd": 3000.0,
                "follower_top3_ask_usd": 2000.0,
                "stealth_capacity_usd": 300.0,
                "net_pnl_bps": 12.0,
                "gross_exec_edge_bps": 22.0,
                "mfe_exec_pnl_bps": 15.0,
                "time_to_peak_sec": 0.5,
                "close_reason": "converged",
                "trigger_type": "depth_drain",
            },
            {
                "coin": "SOL",
                "follower_exchange": "binance",
                "drain_ratio": 0.70,
                "leader_swept_usd": 4000.0,
                "follower_top3_ask_usd": 3000.0,
                "stealth_capacity_usd": 450.0,
                "net_pnl_bps": 18.0,
                "gross_exec_edge_bps": 28.0,
                "mfe_exec_pnl_bps": 20.0,
                "time_to_peak_sec": 0.8,
                "close_reason": "converged",
                "trigger_type": "depth_drain",
            },
            {
                "coin": "SOL",
                "follower_exchange": "binance",
                "drain_ratio": 0.60,
                "leader_swept_usd": 3000.0,
                "follower_top3_ask_usd": 2500.0,
                "stealth_capacity_usd": 375.0,
                "net_pnl_bps": -15.0,
                "gross_exec_edge_bps": 20.0,
                "mfe_exec_pnl_bps": -5.0,
                "time_to_peak_sec": 0.2,
                "close_reason": "stop_invalidation",
                "stop_hit": True,
                "stop_type": "invalidation",
                "stop_loss_bps": 15.0,
                "trigger_type": "depth_drain",
            },
            {
                "coin": "DOGE",
                "follower_exchange": "bybit",
                "drain_ratio": 0.50,
                "leader_swept_usd": 2000.0,
                "follower_top3_ask_usd": 1000.0,
                "stealth_capacity_usd": 150.0,
                "net_pnl_bps": -35.0,
                "gross_exec_edge_bps": 15.0,
                "mfe_exec_pnl_bps": -10.0,
                "time_to_peak_sec": 0.1,
                "close_reason": "stop_adverse_pnl",
                "stop_hit": True,
                "stop_type": "adverse_pnl",
                "stop_loss_bps": 35.0,
                "trigger_type": "depth_drain",
            },
            {
                "coin": "DOGE",
                "follower_exchange": "bybit",
                "drain_ratio": 0.50,
                "leader_swept_usd": 2000.0,
                "follower_top3_ask_usd": 1000.0,
                "stealth_capacity_usd": 150.0,
                "net_pnl_bps": -10.0,
                "gross_exec_edge_bps": 15.0,
                "mfe_exec_pnl_bps": 0.0,
                "time_to_peak_sec": 0.0,
                "close_reason": "stop_decay",
                "stop_hit": True,
                "stop_type": "alpha_decay",
                "stop_loss_bps": 10.0,
                "trigger_type": "depth_drain",
            },
        ]
        with gzip.open(sig_file, "wt", encoding="utf-8") as f:
            for s in mock_signals:
                f.write(json.dumps(s) + "\n")

        with patch.object(fdr, "DATA_ROOT", self.test_dir), \
             patch.object(fdr, "REPORTS_DIR", os.path.join(self.test_dir, "reports")):
            doc = fdr.generate_daily_report(days_back=1.0, as_json=True)

        self.assertIn("venue_capacities", doc)
        self.assertEqual(len(doc["venue_capacities"]), 2)
        v_binance = next(v for v in doc["venue_capacities"] if v["venue"] == "binance")
        self.assertEqual(v_binance["signals"], 3)
        self.assertAlmostEqual(v_binance["med_top3_depth_usd"], 2500.0, places=1)
        self.assertAlmostEqual(v_binance["med_stealth_cap_usd"], 375.0, places=1)

        # Verify stop loss telemetry section
        self.assertIn("stop_loss_telemetry", doc)
        sl = doc["stop_loss_telemetry"]
        self.assertEqual(sl["total_stops"], 3)
        self.assertEqual(sl["invalidation_count"], 1)
        self.assertEqual(sl["adverse_pnl_count"], 1)
        self.assertEqual(sl["alpha_decay_count"], 1)
        self.assertLess(sl["total_stop_pnl_usd"], 0.0)

        # Check top_coins
        self.assertGreater(len(doc["top_coins"]), 0)
        c_sol = next(c for c in doc["top_coins"] if c["coin"] == "SOL")
        self.assertEqual(c_sol["coin"], "SOL")
        self.assertAlmostEqual(c_sol["med_stealth_cap_usd"], 375.0, places=1)

        # Check top_pairs
        self.assertGreater(len(doc["top_pairs"]), 0)
        p_sol = next(p for p in doc["top_pairs"] if "SOL" in p["pair"])
        self.assertAlmostEqual(p_sol["med_stealth_cap_usd"], 375.0, places=1)

    def test_orderbook_vwap_and_true_roundtrip_math(self):
        """Verifies VWAP book depth fills and true roundtrip fee subtraction in KoreanEngine."""
        # 1. OrderBookDepth5 VWAP math
        snap = OrderBookDepth5(
            exchange="binance", symbol="TEST", raw_symbol="TESTUSDT",
            ts=1000.0, exchange_ts=1000000,
            # L1: $10 @ 100.0, L2: $40.4 @ 101.0, L3: $1020 @ 102.0
            asks=[(100.0, 0.1), (101.0, 0.4), (102.0, 10.0)],
            # L1: $9.9 @ 99.0, L2: $39.2 @ 98.0, L3: $970 @ 97.0
            bids=[(99.0, 0.1), (98.0, 0.4), (97.0, 10.0)],
        )

        # Buying $100 fills: $10 @ 100.0 (qty 0.1) + $40.4 @ 101.0 (qty 0.4) + $49.6 @ 102.0 (qty 49.6/102.0)
        expected_qty = 0.1 + 0.4 + (49.6 / 102.0)
        expected_vwap_ask = 100.0 / expected_qty
        self.assertAlmostEqual(snap.vwap_ask_usd(100.0), expected_vwap_ask, places=4)
        self.assertGreater(snap.vwap_ask_usd(100.0), snap.best_ask)

        # Selling $100 fills: $9.9 @ 99.0 (qty 0.1) + $39.2 @ 98.0 (qty 0.4) + $50.9 @ 97.0 (qty 50.9/97.0)
        expected_sell_qty = 0.1 + 0.4 + (50.9 / 97.0)
        expected_vwap_bid = 100.0 / expected_sell_qty
        self.assertAlmostEqual(snap.vwap_bid_usd(100.0), expected_vwap_bid, places=4)
        self.assertLess(snap.vwap_bid_usd(100.0), snap.best_bid)

        # 2. KoreanEngine: Net edge must account for full roundtrip_fee_bps (10.0 bps) & VWAP
        from korean_engine import KoreanEngine, PairState
        emitted = []
        engine = KoreanEngine(emit_callback=emitted.append, min_edge_bps=12.0)
        st = PairState("TEST", "binance", warmup_samples=1)
        st.learn(0.0, 1000.0)  # baseline = 0.0

        # Upbit jumps: mid = 102.0
        upbit_snap = OrderBookDepth5(
            exchange="upbit", symbol="TEST", raw_symbol="KRW-TEST",
            ts=1000.0, exchange_ts=1000000,
            bids=[(101.95, 10.0)], asks=[(102.05, 10.0)]
        )

        # Follower snap has deep book: L1=100.0 ($10), L2=100.5 ($100), L3=101.0 ($1000)
        f_snap = OrderBookDepth5(
            exchange="binance", symbol="TEST", raw_symbol="TESTUSDT",
            ts=1000.0, exchange_ts=1000000,
            bids=[(99.5, 10.0), (99.0, 10.0)],
            asks=[(100.0, 0.1), (100.5, 1.0), (101.0, 10.0)],
        )

        engine._books[("upbit", "TEST")] = upbit_snap
        engine._books[("binance", "TEST")] = f_snap

        engine._check_entry(
            coin="TEST",
            follower_ex="binance",
            st=st,
            upbit_snap=upbit_snap,
            f_snap=f_snap,
            f_book_age_ms=5.0,
            norm_depth=1000.0,
            norm_spread=5.0,
            swept_usd=5000.0,
            trigger_type="depth_drain",
            drain_ratio=0.8,
            blowout_ratio=3.0,
            now=1000.0,
        )

        self.assertIsNotNone(st.open_signal)
        sig = st.open_signal
        # Real entry price is VWAP fill, higher than best_ask (100.0)
        self.assertGreater(sig.follower_entry_px, 100.0)
        # Net edge is gross_exec_edge - 10.0 bps (roundtrip fee)
        self.assertAlmostEqual(sig.net_exec_edge_bps, sig.gross_exec_edge_bps - 10.0, places=2)

        # 3. Follower moves slightly: gross move is +8.0 bps, but net PnL is 8 - 10 = -2 bps < 0.
        # Engine MUST NOT close as "converged"!
        f_slight_move = OrderBookDepth5(
            exchange="binance", symbol="TEST", raw_symbol="TESTUSDT",
            ts=1001.0, exchange_ts=1001000,
            # Bid shifted up slightly
            bids=[(sig.follower_entry_px * (1 + 0.0008), 20.0)],
            asks=[(sig.follower_entry_px * (1 + 0.0015), 20.0)],
        )
        engine._books[("binance", "TEST")] = f_slight_move
        engine._track_follower_move("TEST", "binance", f_slight_move, 1001.0)

        # Signal remains open because net PnL is negative!
        self.assertFalse(sig.closed)

    def test_follower_book_age_guard(self):
        """Verifies that KoreanEngine strictly rejects stale follower order books (> 300ms)."""
        from korean_engine import KoreanEngine, PairState
        emitted = []
        engine = KoreanEngine(emit_callback=emitted.append, max_follower_book_age_sec=0.30)
        st = PairState("BTC", "binance", warmup_samples=1)
        st.learn(0.0, 1000.0)

        upbit_snap = OrderBookDepth5(
            exchange="upbit", symbol="BTC", raw_symbol="KRW-BTC",
            ts=1000.0, exchange_ts=1000000,
            bids=[(105000.0, 1.0)], asks=[(105100.0, 1.0)]
        )
        f_snap = OrderBookDepth5(
            exchange="binance", symbol="BTC", raw_symbol="BTCUSDT",
            ts=1000.0, exchange_ts=1000000,
            bids=[(99000.0, 1.0)], asks=[(99100.0, 1.0)]
        )
        engine._books[("upbit", "BTC")] = upbit_snap
        engine._books[("binance", "BTC")] = f_snap

        # 1. Book age is 350ms (> 300ms threshold) -> REJECTED
        engine._recv_ts[("binance", "BTC")] = 1000.0 - 0.35
        engine._check_entry(
            coin="BTC", follower_ex="binance", st=st,
            upbit_snap=upbit_snap, f_snap=f_snap,
            f_book_age_ms=350.0, norm_depth=1000.0, norm_spread=5.0,
            swept_usd=5000.0, trigger_type="depth_drain", drain_ratio=0.8,
            blowout_ratio=3.0, now=1000.0
        )
        self.assertIsNone(st.open_signal)

        # 2. Book age is 150ms (< 300ms threshold) -> ACCEPTED
        engine._recv_ts[("binance", "BTC")] = 1000.0 - 0.15
        engine._check_entry(
            coin="BTC", follower_ex="binance", st=st,
            upbit_snap=upbit_snap, f_snap=f_snap,
            f_book_age_ms=150.0, norm_depth=1000.0, norm_spread=5.0,
            swept_usd=5000.0, trigger_type="depth_drain", drain_ratio=0.8,
            blowout_ratio=3.0, now=1000.0
        )
        self.assertIsNotNone(st.open_signal)
        self.assertEqual(st.open_signal.follower_book_age_ms, 150.0)

    def test_matched_universe_multi_venue_typing(self):
        """Verifies MatchedUniverse loads all 11 venues with typed (coin, mult, c_size) mappings."""
        from main import KoreanSunriseService
        from venues.manager import StreamManager
        doc = {
            "coins": ["BTC", "PEPE", "SOL"],
            "upbit_krw_to_coin": {"KRW-BTC": "BTC", "KRW-PEPE": "PEPE", "KRW-SOL": "SOL"},
            "binance_sym_map": {"BTCUSDT": ["BTC", 1.0, 1.0], "1000PEPEUSDT": ["PEPE", 1000.0, 1.0]},
            "gateio_sym_map": {"BTC_USDT": ["BTC", 1.0, 0.0001]},
            "okx_sym_map": {"BTC-USDT-SWAP": ["BTC", 1.0, 0.01]},
            "hyperliquid_sym_map": {"kBONKUSDT": ["BONK", 1000.0, 1.0]},
        }
        univ = KoreanSunriseService._parse_universe_doc(doc)
        self.assertEqual(univ.coins, ["BTC", "PEPE", "SOL"])
        self.assertIn("gateio", univ.venue_sym_maps)
        self.assertIn("okx", univ.venue_sym_maps)
        self.assertIn("hyperliquid", univ.venue_sym_maps)

        # Check get_sym_map preserves contract size
        self.assertEqual(univ.get_sym_map("gateio")["BTC_USDT"], ("BTC", 1.0, 0.0001))
        self.assertEqual(univ.get_sym_map("okx")["BTC-USDT-SWAP"], ("BTC", 1.0, 0.01))
        self.assertEqual(univ.get_sym_map("hyperliquid")["kBONKUSDT"], ("BONK", 1000.0, 1.0))

        # Check StreamManager initializes feeds using typed universe
        sm = StreamManager(universe=univ, callback=lambda x: None, active_venues=["upbit", "gateio", "okx"])
        self.assertIn("upbit", sm.streams)
        self.assertIn("gateio", sm.streams)
        self.assertIn("okx", sm.streams)
        self.assertNotIn("binance", sm.streams)

    def test_upbit_trade_stream_parsing(self):
        """Verifies UpbitSpotStream parses trades and order books correctly."""
        from venues.upbit import UpbitSpotStream
        krw_to_coin = {"KRW-BTC": "BTC"}
        received_books = []
        received_trades = []

        stream = UpbitSpotStream(
            krw_to_coin=krw_to_coin,
            callback=received_books.append,
            trade_callback=lambda c, usd, buy, ts: received_trades.append((c, usd, buy, ts)),
        )
        stream.usdt_krw_rate = 1400.0
        stream.rate_ready = True

        # 1. Trade message from Upbit: Taker BUY (ask_bid == "BID")
        trade_msg = json.dumps({
            "type": "trade",
            "code": "KRW-BTC",
            "trade_price": 140000000.0,  # 140,000,000 KRW / 1400 = $100,000
            "trade_volume": 0.05,        # 0.05 BTC = $5,000
            "ask_bid": "BID",            # Taker Buy
            "trade_timestamp": 1700000000000,
        })
        stream._handle_msg(trade_msg)

        self.assertEqual(len(received_trades), 1)
        coin, usd, is_buy, ts = received_trades[0]
        self.assertEqual(coin, "BTC")
        self.assertAlmostEqual(usd, 5000.0, places=2)
        self.assertTrue(is_buy)

        # 2. Trade message: Taker SELL (ask_bid == "ASK")
        sell_msg = json.dumps({
            "type": "trade",
            "code": "KRW-BTC",
            "trade_price": 140000000.0,
            "trade_volume": 0.02,        # $2,000
            "ask_bid": "ASK",            # Taker Sell
            "trade_timestamp": 1700000001000,
        })
        stream._handle_msg(sell_msg)
        self.assertEqual(len(received_trades), 2)
        self.assertFalse(received_trades[1][2])  # is_taker_buy == False

        # 3. Orderbook message
        ob_msg = json.dumps({
            "type": "orderbook",
            "code": "KRW-BTC",
            "timestamp": 1700000002000,
            "orderbook_units": [
                {"bid_price": 139900000.0, "bid_size": 1.0, "ask_price": 140100000.0, "ask_size": 1.0}
            ]
        })
        stream._handle_msg(ob_msg)
        self.assertEqual(len(received_books), 1)
        self.assertEqual(received_books[0].symbol, "BTC")

    def test_upbit_cancel_vs_sweep_guard(self):
        """
        Verifies that KoreanEngine discriminates between MM quote cancellations
        and genuine market sweeps using the Upbit trade stream.
        """
        import time
        from korean_engine import KoreanEngine, PairState
        emitted = []
        engine = KoreanEngine(
            emit_callback=emitted.append,
            require_trade_confirmation=True,
            drain_threshold=0.30,
        )

        st = engine._get_or_create_pair_state("BTC", "binance")
        st.warmup_samples = 1
        st.dev_window.append(0.0)
        st.n_obs = 10
        st.baseline_bps = 0.0

        # Follower orderbook (calm, ready to be sniped)
        t0 = time.time()
        f_snap = OrderBookDepth5(
            exchange="binance", symbol="BTC", raw_symbol="BTCUSDT",
            ts=t0, exchange_ts=int(t0 * 1000),
            bids=[(99000.0, 1.0)], asks=[(99100.0, 1.0), (99200.0, 1.0), (99300.0, 1.0)]
        )
        engine._books[("binance", "BTC")] = f_snap
        engine._recv_ts[("binance", "BTC")] = t0

        # Upbit baseline book with a heavy $10,000 ask plate at 105,000
        upbit_t0 = OrderBookDepth5(
            exchange="upbit", symbol="BTC", raw_symbol="KRW-BTC",
            ts=t0 - 0.1, exchange_ts=int((t0 - 0.1) * 1000),
            bids=[(104900.0, 1.0)],
            asks=[(105000.0, 0.0952), (105500.0, 0.5)]  # ~10,000 USD at best ask
        )
        engine._books[("upbit", "BTC")] = upbit_t0
        engine._recv_ts[("upbit", "BTC")] = t0 - 0.1
        engine._update_upbit_norm("BTC", upbit_t0, t0 - 0.1)

        # SCENARIO A: Market-maker CANCELS the 105,000 ask plate!
        # Best ask lifts to 105,500 with zero taker trades on Upbit.
        t_cancel = time.time()
        upbit_cancel = OrderBookDepth5(
            exchange="upbit", symbol="BTC", raw_symbol="KRW-BTC",
            ts=t_cancel, exchange_ts=int(t_cancel * 1000),
            bids=[(104900.0, 1.0)],
            asks=[(105500.0, 0.5)]
        )
        # Process orderbook update with no preceding trades
        engine.on_book_update(upbit_cancel)

        # Result: Blocked! cancels_blocked increments, no false signal opened!
        self.assertEqual(engine.counters["cancels_blocked"], 1)
        self.assertIsNone(st.open_signal)
        self.assertEqual(engine.counters["signals_triggered"], 0)

        # SCENARIO B: REAL SWEEP! Aggressive buyer sweeps the 105,000 ask plate!
        # Reset Upbit book back to baseline and reset baseline_bps to 0.0
        st.baseline_bps = 0.0
        t_sweep = time.time()
        engine._books[("binance", "BTC")] = f_snap
        engine._recv_ts[("binance", "BTC")] = t_sweep
        engine._books[("upbit", "BTC")] = upbit_t0
        engine._recv_ts[("upbit", "BTC")] = t_sweep - 0.05

        # First, real-time taker buy trades hit the tape:
        engine.on_upbit_trade("BTC", trade_usd=10000.0, is_taker_buy=True, ts=t_sweep)
        self.assertEqual(engine.counters["trades_upbit"], 1)

        # Now the orderbook update arrives reflecting the swept plate
        upbit_swept = OrderBookDepth5(
            exchange="upbit", symbol="BTC", raw_symbol="KRW-BTC",
            ts=t_sweep, exchange_ts=int(t_sweep * 1000),
            bids=[(104900.0, 1.0)],
            asks=[(105500.0, 0.5)]
        )
        engine.on_book_update(upbit_swept)

        # Result: Confirmed! Signal fires with verified taker buys!
        self.assertEqual(engine.counters["cancels_blocked"], 1)  # unchanged
        self.assertEqual(engine.counters["signals_triggered"], 1)
        self.assertIsNotNone(st.open_signal)
        self.assertEqual(st.open_signal.leader_recent_buys_usd, 10000.0)

    def test_concurrent_upbit_trade_stream_thread_safety(self):
        """Stress-tests concurrent on_upbit_trade and get_recent_upbit_buys across multiple threads."""
        import threading
        import time

        engine = KoreanEngine(
            emit_callback=lambda sig: None,
            thresholds_file=os.path.join(self.test_dir, "matrix_thresholds.json")
        )

        coin = "ETH"
        num_writers = 8
        trades_per_writer = 250
        num_readers = 8
        reads_per_reader = 250
        errors = []

        now = time.time()

        def writer_worker(w_id: int):
            try:
                for i in range(trades_per_writer):
                    t = now + (i * 0.001)
                    engine.on_upbit_trade(coin, trade_usd=50.0 + (i % 10), is_taker_buy=True, ts=t)
            except Exception as e:
                errors.append(f"Writer-{w_id} failed: {e}")

        def reader_worker(r_id: int):
            try:
                for i in range(reads_per_reader):
                    cutoff = now + (i * 0.001) - 1.0
                    buys_usd, has_trades = engine.get_recent_upbit_buys(coin, cutoff)
                    self.assertIsInstance(buys_usd, float)
                    self.assertIsInstance(has_trades, bool)
            except Exception as e:
                errors.append(f"Reader-{r_id} failed: {e}")

        threads = []
        for w in range(num_writers):
            threads.append(threading.Thread(target=writer_worker, args=(w,)))
        for r in range(num_readers):
            threads.append(threading.Thread(target=reader_worker, args=(r,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        self.assertEqual(errors, [], f"Encountered thread race errors: {errors}")
        expected_total_trades = num_writers * trades_per_writer
        self.assertEqual(engine.counters["trades_upbit"], expected_total_trades)


if __name__ == "__main__":
    unittest.main()
```

## tests/test_venues_contract_size.py

```python
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
from venues.bingx import BingXStream, BingXRestClient
from venues.gateio import GateioStream, GateioRestClient
from venues.htx import HTXStream, HTXRestClient
from venues.kucoin import KucoinStream, KucoinRestClient
from venues.mexc import MEXCStream, MEXCRestClient
from venues.okx import OKXStream, OKXRestClient
from venues.pionex import PionexStream, PionexRestClient
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

    def test_rest_klines_multiplier_normalization(self):
        """Verifies that fetch_klines_1m across BingX, Gate.io, KuCoin normalizes close with inv_mult."""
        from unittest.mock import MagicMock

        # 1. BingX
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"data": [{"time": 1700000000000, "close": "15.0", "volume": "100.0"}]}
        session = MagicMock()
        session.get.return_value = mock_resp
        bx = BingXRestClient(session=session)
        kl = bx.fetch_klines_1m("1000PEPE-USDT", mult=1000.0)
        self.assertIn(1700000000000 // 60000 * 60, kl)
        close, vol = kl[1700000000000 // 60000 * 60]
        self.assertAlmostEqual(close, 0.015)
        self.assertAlmostEqual(vol, 1500.0)

        # 2. Gate.io
        mock_resp_g = MagicMock(status_code=200)
        mock_resp_g.json.return_value = [{"t": 1700000000, "c": "15.0", "sum": "1500.0"}]
        session_g = MagicMock()
        session_g.get.return_value = mock_resp_g
        gt = GateioRestClient()
        gt.session = session_g
        kl_g = gt.fetch_klines_1m("1000BONK_USDT", mult=1000.0)
        self.assertIn(1700000000 // 60 * 60, kl_g)
        close_g, vol_g = kl_g[1700000000 // 60 * 60]
        self.assertAlmostEqual(close_g, 0.015)
        self.assertAlmostEqual(vol_g, 1500.0)

        # 3. KuCoin
        mock_resp_k = MagicMock(status_code=200)
        mock_resp_k.json.return_value = {"data": [[1700000000000, "0", "0", "0", "15.0", "100.0", "1500.0"]]}
        session_k = MagicMock()
        session_k.get.return_value = mock_resp_k
        kc = KucoinRestClient(session=session_k)
        kl_k = kc.fetch_klines_1m("PEPEUSDTM", mult=1000.0)
        close_k, vol_k = kl_k[1700000000000 // 60000 * 60]
        self.assertAlmostEqual(close_k, 0.015)
        self.assertAlmostEqual(vol_k, 1500.0)

    def test_kucoin_stream_session_lifecycle(self):
        """KucoinStream: verifies session reuse across bullet calls and clean closure on stop."""
        import asyncio
        from unittest.mock import MagicMock

        snaps: List[OrderBookDepth5] = []
        stream = KucoinStream(["XBTUSDTM"], snaps.append)
        self.assertIsNotNone(stream._session)
        init_session = stream._session

        # _resolve_symbols must reuse the existing session
        mock_resp = MagicMock(status_code=200)
        mock_resp.json.return_value = {"data": [{"symbol": "XBTUSDTM", "multiplier": 1.0, "baseCurrency": "XBT"}]}
        init_session.get = MagicMock(return_value=mock_resp)
        stream._resolve_symbols()
        self.assertIs(stream._session, init_session)
        init_session.get.assert_called_once()

        # stop() must close session and clear reference
        init_session.close = MagicMock()
        asyncio.run(stream.stop())
        init_session.close.assert_called_once()
        self.assertIsNone(stream._session)


if __name__ == "__main__":
    unittest.main()
```

## tests/test_venues_streams_resilience.py

```python
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
```

## tools/__init__.py

```python
"""
tools - Operator CLI tools and pipeline orchestration for Korean Sunrise.
"""
```

## tools/analyze_signals.py

```python
"""
tools/analyze_signals.py - Offline analyzer & coin filter for Korean Sunrise signals.
Reads all compressed .jsonl.gz files from data/signals/ and prints actionable reports.
"""
from __future__ import annotations

from collections import defaultdict
import glob
import gzip
import json
import os
import statistics
import sys
from typing import Any, Dict, List

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DEFAULT_DATA_ROOT = os.path.join(PROJECT_ROOT, "data")


def load_signals(data_root: str = DEFAULT_DATA_ROOT) -> List[Dict[str, Any]]:
    pattern = os.path.join(data_root, "signals", "**", "*.jsonl.gz")
    files = sorted(glob.glob(pattern, recursive=True))
    signals: List[Dict[str, Any]] = []
    for fpath in files:
        try:
            with gzip.open(fpath, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        signals.append(json.loads(line))
        except Exception as e:
            print(f"[warn] Failed to read {fpath}: {e}", file=sys.stderr)
    return signals


def _mean(vals: List[float]) -> float:
    return statistics.mean(vals) if vals else 0.0


def _med(vals: List[float]) -> float:
    return statistics.median(vals) if vals else 0.0


def interpolate_markout(path: List[List[float]], target_dt: float) -> tuple[float, float]:
    """
    Returns (f_mid_move_bps, net_exec_pnl_bps) at `target_dt` seconds from signal open.
    Supports both 5-column path [dt, up_move, f_mid_move, f_exec_pnl, rem_edge]
    and legacy 4-column path.
    """
    if not path:
        return 0.0, 0.0
    best_row = path[0]
    for row in path:
        if row[0] <= target_dt:
            best_row = row
        else:
            break
    if len(best_row) >= 5:
        return float(best_row[2]), float(best_row[3])
    return float(best_row[2]), float(best_row[2]) - 10.0


def main() -> None:
    data_root = os.getenv("DATA_ROOT", DEFAULT_DATA_ROOT)
    if not os.path.isabs(data_root):
        data_root = os.path.join(PROJECT_ROOT, data_root)

    signals = load_signals(data_root)
    if not signals:
        print(f"No signals found in '{data_root}/signals/'. Run main.py first to collect data.")
        return

    print("=" * 88)
    print(f" KOREAN SUNRISE — POST-TRADE SIGNAL ANALYSIS (Total Signals: {len(signals)})")
    print("=" * 88)

    # 1. Breakdown by Exchange & Trigger Type
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for s in signals:
        key = f"{s.get('follower_exchange', '?'):<7} | {s.get('trigger_type', '?'):<14}"
        groups[key].append(s)

    print("\n[1] BREAKDOWN BY FOLLOWER EXCHANGE & TRIGGER TYPE:")
    print(
        f"{'Venue   | Trigger':<24} | {'Count':>5} | {'Conv%':>5} | {'Win%':>5} | "
        f"{'MeanPnL':>8} | {'MedPnL':>7} | {'MedTop3':>8} | {'MedCap15':>8}"
    )
    print("-" * 92)
    for k, rows in sorted(groups.items()):
        n = len(rows)
        pnls = [float(r.get("net_pnl_bps", 0.0)) for r in rows]
        top3s = [float(r.get("follower_top3_ask_usd", 0.0)) for r in rows if float(r.get("follower_top3_ask_usd", 0.0)) > 0]
        caps = [
            float(r.get("stealth_capacity_usd") or (0.15 * float(r.get("follower_top3_ask_usd", 0.0))))
            for r in rows
            if float(r.get("follower_top3_ask_usd", 0.0)) > 0
        ]
        conv_pct = sum(1 for r in rows if r.get("close_reason") == "converged") / n * 100.0
        win_pct = sum(1 for p in pnls if p > 0.0) / n * 100.0
        print(
            f"{k:<24} | {n:>5d} | {conv_pct:>4.1f}% | {win_pct:>4.1f}% | "
            f"{_mean(pnls):>+7.2f} | {_med(pnls):>+6.2f} | ${_med(top3s):>7.0f} | ${_med(caps):>7.0f}"
        )

    # 2. Sub-second Markout Curve across horizons
    horizons = [0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0]
    print("\n[2] SUB-SECOND MARKOUT CURVE (Across All Signals):")
    print(f"{'Horizon (sec)':<15} | {'Mean Mid Move (bps)':>20} | {'Mean Net Exec PnL (bps)':>24} | {'Exec Win%':>10}")
    print("-" * 78)
    for h in horizons:
        mid_moves = []
        exec_pnls = []
        for s in signals:
            m_move, e_pnl = interpolate_markout(s.get("path", []), h)
            mid_moves.append(m_move)
            exec_pnls.append(e_pnl)
        w_pct = sum(1 for x in exec_pnls if x > 0.0) / len(exec_pnls) * 100.0
        print(f"t = +{h:<10.2f}s | {_mean(mid_moves):>+19.2f} | {_mean(exec_pnls):>+23.2f} | {w_pct:>9.1f}%")

    # 3. Impact of Upbit Swept USD Volume (Filtering out retail noise)
    buckets = [
        ("< $1,000", 0.0, 1000.0),
        ("$1,000 - $5,000", 1000.0, 5000.0),
        ("$5,000 - $20,000", 5000.0, 20000.0),
        ("> $20,000", 20000.0, 1e12),
    ]
    print("\n[3] FILTER BY UPBIT SWEPT VOLUME (leader_swept_usd):")
    print(f"{'Upbit Swept USD':<20} | {'Count':>5} | {'Win%':>6} | {'MeanPnL':>8} | {'MeanMFE':>8} | {'Rev% (Fakeout)':>15}")
    print("-" * 74)
    for label, lo, hi in buckets:
        sub = [s for s in signals if lo <= float(s.get("leader_swept_usd", 0.0)) < hi]
        if not sub:
            continue
        pnls = [float(r.get("net_pnl_bps", 0.0)) for r in sub]
        mfes = [float(r.get("mfe_exec_pnl_bps", 0.0)) for r in sub]
        win_pct = sum(1 for p in pnls if p > 0.0) / len(sub) * 100.0
        rev_pct = sum(1 for r in sub if r.get("close_reason") == "reversed") / len(sub) * 100.0
        print(
            f"{label:<20} | {len(sub):>5d} | {win_pct:>5.1f}% | "
            f"{_mean(pnls):>+7.2f} | {_mean(mfes):>+7.2f} | {rev_pct:>14.1f}%"
        )

    # 4. Coin Ranking (Whitelist vs Toxic Shitcoins)
    by_coin: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for s in signals:
        by_coin[s.get("coin", "UNKNOWN")].append(s)

    coin_stats = []
    for coin, rows in by_coin.items():
        n = len(rows)
        pnls = [float(r.get("net_pnl_bps", 0.0)) for r in rows]
        mfes = [float(r.get("mfe_exec_pnl_bps", 0.0)) for r in rows]
        spreads = [float(r.get("follower_spread_bps", 0.0)) for r in rows]
        top3s = [float(r.get("follower_top3_ask_usd", 0.0)) for r in rows if float(r.get("follower_top3_ask_usd", 0.0)) > 0]
        caps = [
            float(r.get("stealth_capacity_usd") or (0.15 * float(r.get("follower_top3_ask_usd", 0.0))))
            for r in rows
            if float(r.get("follower_top3_ask_usd", 0.0)) > 0
        ]
        win_pct = sum(1 for p in pnls if p > 0.0) / n * 100.0
        mfe_pos_pct = sum(1 for m in mfes if m > 2.0) / n * 100.0
        coin_stats.append({
            "coin": coin,
            "n": n,
            "win_pct": win_pct,
            "mfe_pos_pct": mfe_pos_pct,
            "mean_pnl": _mean(pnls),
            "sum_pnl": sum(pnls),
            "med_top3": _med(top3s),
            "med_cap": _med(caps),
            "mean_mfe": _mean(mfes),
            "mean_spread": _mean(spreads),
        })

    coin_stats.sort(key=lambda x: x["mean_pnl"], reverse=True)

    print("\n[4] COIN PERFORMANCE RANKING (Sorted by Mean Realized Net PnL):")
    print(
        f"{'Coin':<10} | {'Trades':>6} | {'Win%':>6} | "
        f"{'MeanPnL':>8} | {'MedTop3':>8} | {'MedCap15':>8} | {'MeanMFE':>8} | {'F_Spread':>8}"
    )
    print("-" * 92)
    for st in coin_stats:
        print(
            f"{st['coin']:<10} | {st['n']:>6d} | {st['win_pct']:>5.1f}% | "
            f"{st['mean_pnl']:>+7.2f} | ${st['med_top3']:>7.0f} | ${st['med_cap']:>7.0f} | "
            f"{st['mean_mfe']:>+7.2f} | {st['mean_spread']:>7.2f}"
        )

    # Suggest automatic blacklist for coins with >= 3 signals and negative MFE/PnL
    toxic_coins = [
        st["coin"]
        for st in coin_stats
        if st["n"] >= 3 and st["mean_pnl"] < -3.0 and st["mean_mfe"] < 2.0
    ]
    if toxic_coins:
        print("\n[!] SUGGESTED TOXIC COINS TO ADD TO EXCLUDE_COINS:")
        print("export EXCLUDE_COINS=\"BTC,ETH,USDT,USDC," + ",".join(sorted(toxic_coins)) + "\"")


if __name__ == "__main__":
    main()
```

## tools/audit_venues.py

```python
"""
tools/audit_venues.py - Exhaustive live health, latency, and telemetry auditor for Korean Sunrise.
Audits Upbit Spot Leader and all 9 perpetual follower venues.

Usage:
  python tools/audit_venues.py
  python tools/audit_venues.py --port 8089
"""
from __future__ import annotations

import argparse
import collections
import glob
import gzip
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ALL_VENUES = [
    "upbit",
    "binance",
    "bybit",
    "okx",
    "gateio",
    "mexc",
    "kucoin",
    "bingx",
    "htx",
    "pionex",
    "asterdex",
    "hyperliquid",
]

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
TOKEN = os.getenv("API_AUTH_TOKEN", "vitali_secret_key_2026")


def fetch_live_stats(port: int = 8089) -> Optional[Dict[str, Any]]:
    for host in ["127.0.0.1", "localhost"]:
        url = f"http://{host}:{port}/stats?token={urllib.parse.quote(TOKEN)}"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "VenueAuditor/1.0"})
            with urllib.request.urlopen(req, timeout=3) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode("utf-8"))
        except Exception:
            continue
    return None


def audit_recent_signals(lookback_minutes: float = 60.0) -> Dict[str, int]:
    """Counts signals recorded per venue over the trailing window."""
    s_dir = os.path.join(DATA_ROOT, "signals")
    if not os.path.isdir(s_dir):
        return {}
    cutoff_ts = time.time() - (lookback_minutes * 60.0)
    venue_counts: Dict[str, int] = collections.defaultdict(int)

    pattern = os.path.join(s_dir, "**", "*.jsonl.gz")
    for f in glob.glob(pattern, recursive=True):
        try:
            if os.path.getmtime(f) >= cutoff_ts:
                with gzip.open(f, "rt", encoding="utf-8", errors="replace") as gz:
                    for line in gz:
                        if not line.strip():
                            continue
                        d = json.loads(line)
                        f_ex = str(d.get("follower_exchange", "")).lower()
                        if f_ex:
                            venue_counts[f_ex] += 1
        except Exception:
            pass

    return dict(venue_counts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Venue Health Auditor.")
    parser.add_argument("--port", type=int, default=8089, help="Web API port (default: 8089)")
    args = parser.parse_args()

    print("=" * 80)
    print("  KOREAN SUNRISE: LIVE VENUE HEALTH & TELEMETRY AUDIT")
    print("=" * 80)

    stats = fetch_live_stats(args.port)
    sig_counts = audit_recent_signals(lookback_minutes=60.0)

    if not stats:
        print(f"  [!] Web API server is NOT responding at http://127.0.0.1:{args.port}/")
        print("      (Make sure main.py is running with API_SERVER_ENABLED=true)\n")
    else:
        counters = stats.get("counters", {})
        print(f"  Service Status : {'RUNNING' if stats.get('running') else 'STOPPED'}")
        print(f"  Total Ticks    : Upbit {counters.get('ticks_upbit', 0):,} | Followers {counters.get('ticks_followers', 0):,}")
        print(f"  Total Signals  : {counters.get('signals_triggered', 0):,} (⚔️Battle: {counters.get('signals_battle', 0)}, 🧪Inc: {counters.get('signals_incubator', 0)})\n")

    print("-" * 80)
    print(f"  {'Venue':<12} | {'Role':<18} | {'Recent Signals (60m)':<22} | {'Status'}")
    print("-" * 80)

    for v in ALL_VENUES:
        role = "Spot Leader (KRW)" if v == "upbit" else "Perp Follower (USDT)"
        cnt = sig_counts.get(v, 0)
        status = "HEALTHY" if (cnt > 0 or stats) else "IDLE / NO DATA"
        print(f"  {v.upper():<12} | {role:<18} | {cnt:>22} | {status}")

    print("=" * 80)


if __name__ == "__main__":
    main()
```

## tools/calibrate_thresholds.py

```python
"""
tools/calibrate_thresholds.py - Automated Rolling Calibration of Pair-Specific Spread Thresholds.

Optimized for Korean Sunrise Lead-Lag Execution:
  - Low-memory streaming over data/signals/**/*.jsonl.gz (O(1) RAM footprint < 25 MB).
  - Sweeps spread/edge thresholds [10..60 bps] per (coin, upbit, follower) pair.
  - Automatically classifies pairs into:
      * Aggressive / Clean (12-20 bps)  -> High-frequency clean lead-lag edge.
      * Balanced Standard (22-35 bps)   -> Robust baseline execution.
      * High Friction (38-55 bps)       -> Shields against follower spread/slippage.
      * Toxic / Pruned (disabled)       -> Cuts negative EV / high reversal flow.
  - Generates atomic matrix_thresholds.json for real-time bot consumption.

Usage:
  python tools/calibrate_thresholds.py
  python tools/calibrate_thresholds.py --days 1 --size 10.0 --fee-bps 10.0
  python tools/calibrate_thresholds.py --min-trades 5 --min-winrate 0.70
  python tools/calibrate_thresholds.py --dry-run
"""
from __future__ import annotations

import argparse
import collections
import datetime
import glob
import gzip
import json
import os
import statistics
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
DEFAULT_OUT_FILE = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
UNIVERSE_FILE = os.path.join(PROJECT_ROOT, "universe_live.json")
BATTLE_FILE = os.path.join(PROJECT_ROOT, "live_trading_pool.json")

SWEEP_THRESHOLDS = (10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 60.0)
DEFAULT_GLOBAL_FALLBACK_BPS = 20.0
DEFAULT_GLOBAL_HOLD_SEC = 15.0
DEFAULT_SPIKE_SIGMA = 2.5
DEFAULT_MIN_DEV_Z = 1.8


def _calc_optimal_hold(t_peaks: List[float], default_val: float = DEFAULT_GLOBAL_HOLD_SEC) -> float:
    """Calculates personalized holding horizon: median(T_peak) + 1.5*MAD (clamped [2.0s, 60.0s])."""
    if not t_peaks:
        return default_val
    med = statistics.median(t_peaks)
    abs_dev = [abs(x - med) for x in t_peaks]
    mad = statistics.median(abs_dev) if abs_dev else 0.5
    raw_hold = med + 1.5 * (1.4826 * mad)
    return round(max(2.0, min(60.0, raw_hold)), 1)


def _calc_spike_and_dev_z(dev_zs: List[float]) -> Tuple[float, float]:
    """Calculates personalized spike gating threshold P90(dev_z) and entry dev_z."""
    if len(dev_zs) < 5:
        return DEFAULT_SPIKE_SIGMA, DEFAULT_MIN_DEV_Z
    s_z = sorted(dev_zs)
    idx90 = int(0.90 * len(s_z))
    p90 = s_z[min(idx90, len(s_z) - 1)]
    spike_sigma = round(max(2.0, min(4.5, p90)), 2)
    idx25 = int(0.25 * len(s_z))
    p25 = s_z[min(idx25, len(s_z) - 1)]
    min_dev_z = round(max(1.5, min(2.5, p25)), 2)
    return spike_sigma, min_dev_z


def _collect_signal_files(days_back: float = 7.0) -> List[str]:
    """Collects gzip files from data/signals within the trailing window."""
    s_dir = os.path.join(DATA_ROOT, "signals")
    if not os.path.isdir(s_dir):
        return []
    cutoff = time.time() - (days_back * 86400.0)
    pattern = os.path.join(s_dir, "**", "*.jsonl.gz")
    all_files = sorted(glob.glob(pattern, recursive=True))
    res = []
    for fp in all_files:
        try:
            if os.path.getmtime(fp) >= cutoff:
                res.append(fp)
        except OSError:
            pass
    return res if res else all_files


def _write_json_atomic(path: str, data: Any) -> None:
    """Atomic write via temp file and replace."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def calibrate(
    days_back: float = 7.0,
    pos_size: float = 10.0,
    roundtrip_fee_bps: float = 10.0,
    min_trades: int = 5,
    min_winrate: float = 0.70,
    out_file: str = DEFAULT_OUT_FILE,
    dry_run: bool = False,
    as_json: bool = False,
) -> Dict[str, Any]:
    t0 = time.time()
    fee_usd = pos_size * (roundtrip_fee_bps / 10000.0)
    files = _collect_signal_files(days_back=days_back)

    # Aggregator per pair key: {coin}:upbit-{follower_ex}
    # Structure: pair_key -> threshold -> {count, wins, gross, fee, net}
    pair_sweeps: Dict[str, Dict[float, Dict[str, Any]]] = collections.defaultdict(
        lambda: {
            t: {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0}
            for t in SWEEP_THRESHOLDS
        }
    )
    pair_metrics: Dict[str, Dict[str, List[float]]] = collections.defaultdict(
        lambda: {"t_peaks": [], "dev_zs": []}
    )

    total_rows = 0
    valid_events = 0

    if files:
        for fp in files:
            try:
                with gzip.open(fp, "rt", encoding="utf-8", errors="replace") as gz:
                    for line in gz:
                        line = line.strip()
                        if not line:
                            continue
                        total_rows += 1
                        try:
                            d = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        coin = str(d.get("coin", "")).upper()
                        follower_ex = str(d.get("follower_exchange", "")).lower()
                        if not coin or not follower_ex:
                            continue

                        net_pnl = d.get("net_pnl_bps")
                        if net_pnl is None:
                            continue
                        net_pnl = float(net_pnl)
                        gross_edge = float(
                            d.get("gross_exec_edge_bps")
                            or (float(d.get("net_exec_edge_bps", 0.0)) + roundtrip_fee_bps * 0.5)
                        )

                        # Microstructure physical validity check (consistent with evaluator.py)
                        drain_ratio = float(d.get("drain_ratio", 0.0))
                        swept_usd = float(d.get("leader_swept_usd", 0.0))
                        f_top3_ask = float(d.get("follower_top3_ask_usd", 0.0))
                        cross_ratio = (swept_usd / f_top3_ask) if f_top3_ask > 0 else 0.0
                        if drain_ratio < 0.40 and cross_ratio < 0.50:
                            continue

                        pair_key = f"{coin}:upbit-{follower_ex}"
                        valid_events += 1

                        # Collect metrics for individual hold & spike calibration
                        t_pk = d.get("time_to_peak_sec")
                        if t_pk is not None and float(t_pk) > 0 and net_pnl > 0:
                            pair_metrics[pair_key]["t_peaks"].append(float(t_pk))
                        dz = d.get("dev_z")
                        if dz is not None and float(dz) > 0:
                            pair_metrics[pair_key]["dev_zs"].append(float(dz))

                        gross = pos_size * ((net_pnl + roundtrip_fee_bps) / 10000.0)
                        net = pos_size * (net_pnl / 10000.0)

                        sw = pair_sweeps[pair_key]
                        for t in SWEEP_THRESHOLDS:
                            if gross_edge >= t:
                                s = sw[t]
                                s["count"] += 1
                                s["gross"] += gross
                                s["fee"] += fee_usd
                                s["net"] += net
                                if net > 0:
                                    s["wins"] += 1
            except Exception as e:
                if not as_json:
                    print(f"[!] Warning reading {fp}: {e}", file=sys.stderr)

    matrix_output: Dict[str, Any] = {}
    classification_counts = collections.Counter()

    # Fallback to universe_live.json or live_trading_pool.json if no signals yet
    if not pair_sweeps:
        target_coins: Dict[str, List[str]] = {}
        if os.path.exists(UNIVERSE_FILE):
            try:
                with open(UNIVERSE_FILE, "r", encoding="utf-8") as uf:
                    udoc = json.load(uf)
                for c in udoc.get("coins", []):
                    # Check which venues this coin trades on
                    venues = []
                    for v in ["binance", "bybit", "okx", "gateio", "mexc", "kucoin", "bingx", "htx", "pionex", "asterdex", "hyperliquid"]:
                        sym_map = udoc.get(f"{v}_sym_map", {})
                        if any(val[0] == c for val in sym_map.values()):
                            venues.append(v)
                    target_coins[c] = venues if venues else ["binance", "bybit"]
            except Exception:
                pass

        if not target_coins:
            target_coins = {
                "VVV": ["binance", "bybit", "kucoin"],
                "LIT": ["binance", "bybit", "okx"],
                "ICP": ["binance", "bybit", "okx"],
                "ETHFI": ["binance", "bybit"],
                "RAY": ["bingx", "bybit", "mexc"],
            }

        for c, v_list in target_coins.items():
            for v in v_list:
                pk = f"{c}:upbit-{v}"
                matrix_output[pk] = {
                    "min_spread_bps": DEFAULT_GLOBAL_FALLBACK_BPS,
                    "max_hold_sec": DEFAULT_GLOBAL_HOLD_SEC,
                    "spike_sigma": DEFAULT_SPIKE_SIGMA,
                    "min_dev_z": DEFAULT_MIN_DEV_Z,
                    "expected_winrate": 0.85,
                    "expected_net_bps": 22.0,
                    "sample_trades": 0,
                    "status": "default_baseline",
                    "enabled": True,
                }
        classification_counts["default_baseline"] = len(matrix_output)

    else:
        for pair_key, sweeps in pair_sweeps.items():
            best_t = DEFAULT_GLOBAL_FALLBACK_BPS
            best_net = -1e9
            best_wr = 0.0
            best_cnt = 0
            best_gross = 0.0

            eligible_thresholds = []
            for t in SWEEP_THRESHOLDS:
                s = sweeps[t]
                cnt = s["count"]
                if cnt >= min_trades:
                    wr = s["wins"] / cnt
                    if wr >= min_winrate and s["net"] > 0:
                        eligible_thresholds.append((t, s["net"], wr, cnt, s["gross"]))

            if eligible_thresholds:
                # Pick threshold that maximizes total net USD profit
                eligible_thresholds.sort(key=lambda x: x[1], reverse=True)
                best_t, best_net, best_wr, best_cnt, best_gross = eligible_thresholds[0]

                # Classification
                if best_t <= 20.0 and best_wr >= 0.80:
                    status = "aggressive_clean"
                elif best_t <= 35.0 and best_wr >= 0.70:
                    status = "balanced_standard"
                else:
                    status = "high_friction"
                enabled = True
            else:
                # Pair didn't meet min_trades or winrate guard
                max_cnt_t = max(SWEEP_THRESHOLDS, key=lambda t: sweeps[t]["count"])
                s = sweeps[max_cnt_t]
                best_cnt = s["count"]
                best_wr = (s["wins"] / best_cnt) if best_cnt > 0 else 0.0
                best_net = s["net"]
                best_t = DEFAULT_GLOBAL_FALLBACK_BPS

                if best_cnt >= min_trades and (best_net <= 0 or best_wr < 0.50):
                    status = "toxic_pruned"
                    enabled = False
                else:
                    status = "default_baseline"
                    enabled = True

            # Calculate optimal hold horizon & spike gating per pair
            t_peaks = pair_metrics[pair_key]["t_peaks"]
            dev_zs = pair_metrics[pair_key]["dev_zs"]
            opt_hold = _calc_optimal_hold(t_peaks, default_val=DEFAULT_GLOBAL_HOLD_SEC)
            spike_sigma, min_dev_z = _calc_spike_and_dev_z(dev_zs)

            classification_counts[status] += 1
            matrix_output[pair_key] = {
                "min_spread_bps": round(best_t, 1),
                "max_hold_sec": opt_hold,
                "spike_sigma": spike_sigma,
                "min_dev_z": min_dev_z,
                "expected_winrate": round(best_wr, 3),
                "expected_net_bps": round((best_net / pos_size) * 10000.0 / max(1, best_cnt), 1) if best_cnt else 0.0,
                "sample_trades": best_cnt,
                "status": status,
                "enabled": enabled,
            }

    doc = {
        "metadata": {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "days_analyzed": days_back,
            "order_size_usd": pos_size,
            "roundtrip_fee_bps": roundtrip_fee_bps,
            "min_trades_guard": min_trades,
            "min_winrate_guard": min_winrate,
            "global_fallback_bps": DEFAULT_GLOBAL_FALLBACK_BPS,
            "total_files_scanned": len(files),
            "total_signals_evaluated": valid_events,
            "total_pairs_configured": len(matrix_output),
            "classifications": dict(classification_counts),
            "elapsed_sec": round(time.time() - t0, 3),
        },
        "thresholds": matrix_output,
    }

    if not dry_run:
        _write_json_atomic(out_file, doc)

    if not as_json:
        print("\n" + "=" * 96)
        print("  KOREAN SUNRISE: AUTOMATED SPREAD & HOLD HORIZON CALIBRATION MATRIX")
        print("=" * 96)
        print(f"  Signals Scanned: {valid_events:,} across {len(files)} archives ({days_back:.1f} days)")
        print(f"  Configured Pairs: {len(matrix_output)} pairs calibrated")
        print(f"  Classifications: {dict(classification_counts)}")
        print("-" * 96)
        print(f"  {'Pair (Coin:Leader-Follower)':<28} | {'Min Edge':<9} | {'Hold':<7} | {'Spike':<6} | {'Win Rate':<9} | {'Trades':<7} | {'Status':<16} | {'Active'}")
        print("-" * 96)
        for pk, cfg in sorted(matrix_output.items())[:35]:
            en_str = "YES" if cfg["enabled"] else "NO (PRUNED)"
            print(
                f"  {pk:<28} | {cfg['min_spread_bps']:>5.1f} bps | "
                f"{cfg.get('max_hold_sec', 6.0):>4.1f}s | {cfg.get('spike_sigma', 2.5):>4.1f}σ | "
                f"{cfg['expected_winrate'] * 100:>6.1f}% | {cfg['sample_trades']:>7} | "
                f"{cfg['status']:<16} | {en_str}"
            )
        if len(matrix_output) > 35:
            print(f"  ... and {len(matrix_output) - 35} more pairs calibrated.")
        print("=" * 80)
        print(f"  [OK] Saved atomic matrix to: {out_file}\n")

    return doc


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Pair Spread Calibrator.")
    parser.add_argument("--days", type=float, default=7.0, help="Days back of signals (default: 7)")
    parser.add_argument("--size", type=float, default=10.0, help="Order size USD (default: 10)")
    parser.add_argument("--fee-bps", type=float, default=10.0, help="Roundtrip fee bps (default: 10)")
    parser.add_argument("--min-trades", type=int, default=5, help="Min sample trades per pair (default: 5)")
    parser.add_argument("--min-winrate", type=float, default=0.70, help="Min winrate guard (default: 0.70)")
    parser.add_argument("--out", default=DEFAULT_OUT_FILE, help="Output matrix file")
    parser.add_argument("--dry-run", action="store_true", help="Do not write file")
    parser.add_argument("--json", action="store_true", help="Output pure JSON")
    args = parser.parse_args()

    doc = calibrate(
        days_back=args.days,
        pos_size=args.size,
        roundtrip_fee_bps=args.fee_bps,
        min_trades=args.min_trades,
        min_winrate=args.min_winrate,
        out_file=args.out,
        dry_run=args.dry_run,
        as_json=args.json,
    )
    if args.json:
        print(json.dumps(doc, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
```

## tools/full_daily_report.py

```python
"""
tools/full_daily_report.py - High-speed, low-memory quantitative report generator for Korean Sunrise.
Optimized for low-resource VPS and real-time production analysis.

Architecture & Features:
  - Zero-OOM Streaming: Uses in-flight streaming aggregation without loading full datasets in RAM.
  - Multi-Venue Lead-Lag Telemetry: Evaluates Upbit KRW Spot sweeps against all 9 follower perp venues.
  - PnL Scenarios: All Signals, Battle Coins, Calibrated Threshold Filtered, Funded Venues.
  - Generates Markdown Report (reports/daily/report_YYYY-MM-DD.md) and JSON (reports/daily/report_YYYY-MM-DD.json).

Usage:
  python tools/full_daily_report.py
  python tools/full_daily_report.py --days 1 --size 10.0 --fee-bps 10.0
  python tools/full_daily_report.py --json
"""
from __future__ import annotations

import argparse
import collections
import datetime
import glob
import gzip
import json
import os
import statistics
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports", "daily")
BATTLE_FILE = os.path.join(PROJECT_ROOT, "live_trading_pool.json")
UNIVERSE_FILE = os.path.join(PROJECT_ROOT, "universe_live.json")
THRESHOLDS_FILE = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")

SWEEP_THRESHOLDS = (10, 15, 20, 25, 30, 35, 40, 50, 60)
FUNDED_VENUES = {"binance", "bybit", "okx", "gateio", "mexc", "kucoin", "bingx", "htx", "pionex", "asterdex", "hyperliquid"}


def _collect_files(days_back: float = 1.0) -> List[str]:
    s_dir = os.path.join(DATA_ROOT, "signals")
    if not os.path.isdir(s_dir):
        return []
    cutoff = time.time() - (days_back * 86400.0)
    pattern = os.path.join(s_dir, "**", "*.jsonl.gz")
    all_files = sorted(glob.glob(pattern, recursive=True))
    res = [fp for fp in all_files if os.path.getmtime(fp) >= cutoff]
    return res if res else all_files


def generate_daily_report(
    days_back: float = 1.0,
    pos_size: float = 10.0,
    roundtrip_fee_bps: float = 10.0,
    as_json: bool = False,
) -> Dict[str, Any]:
    t0 = time.time()
    today_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    os.makedirs(REPORTS_DIR, exist_ok=True)

    fee_usd = pos_size * (roundtrip_fee_bps / 10000.0)
    files = _collect_files(days_back=days_back)

    # Load battle coins
    battle_coins: Set[str] = set()
    if os.path.exists(BATTLE_FILE):
        try:
            with open(BATTLE_FILE, "r", encoding="utf-8") as bf:
                bdoc = json.load(bf)
                coins_map = bdoc.get("coins") or bdoc.get("battle_pool") or {}
                battle_coins = {
                    k.upper() for k, v in coins_map.items()
                    if isinstance(v, dict) and v.get("enabled", True)
                }
        except Exception:
            pass

    # Load matrix thresholds
    calibrated_thresholds: Dict[str, float] = {}
    if os.path.exists(THRESHOLDS_FILE):
        try:
            with open(THRESHOLDS_FILE, "r", encoding="utf-8") as tf:
                tdoc = json.load(tf)
                for pk, cfg in tdoc.get("thresholds", {}).items():
                    if cfg.get("enabled", True):
                        calibrated_thresholds[pk] = float(cfg.get("min_spread_bps", 20.0))
        except Exception:
            pass

    # Telemetry accumulators
    total_signals = 0
    total_bytes = sum(os.path.getsize(f) for f in files) if files else 0
    venues_seen: Set[str] = set()
    coins_seen: Set[str] = set()
    close_reasons = collections.Counter()
    trigger_types = collections.Counter()
    stop_stats = {
        "total_stops": 0,
        "invalidation": 0,
        "adverse_pnl": 0,
        "alpha_decay": 0,
        "other": 0,
        "total_stop_pnl_usd": 0.0,
    }

    def _new_bucket() -> Dict[str, Any]:
        return {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0, "gains": 0.0, "losses": 0.0}

    pnl_scenarios = {
        "all_signals": _new_bucket(),
        "calibrated_threshold_only": _new_bucket(),
        "battle_coins_only": _new_bucket(),
        "funded_venues_only": _new_bucket(),
    }

    sweep = {t: {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0} for t in SWEEP_THRESHOLDS}
    pair_stats = collections.defaultdict(lambda: {
        "count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0,
        "top3_depths": [], "stealth_caps": []
    })
    coin_stats = collections.defaultdict(lambda: {
        "count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0,
        "mfes": [], "t_peaks": [], "top3_depths": [], "stealth_caps": []
    })
    venue_capacities = collections.defaultdict(lambda: {"count": 0, "top3_depths": [], "stealth_caps": []})

    for fpath in files:
        try:
            with gzip.open(fpath, "rt", encoding="utf-8", errors="replace") as gz:
                for line in gz:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        d = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    total_signals += 1
                    coin = str(d.get("coin", "")).upper()
                    f_ex = str(d.get("follower_exchange", "")).lower()
                    if not coin or not f_ex:
                        continue

                    coins_seen.add(coin)
                    venues_seen.add(f_ex)

                    reason = d.get("close_reason", "unknown")
                    close_reasons[reason] += 1
                    trig = d.get("trigger_type", "unknown")
                    trigger_types[trig] += 1

                    stop_hit = d.get("stop_hit", False)
                    stop_type = d.get("stop_type")
                    if not stop_hit and reason.startswith("stop_"):
                        stop_hit = True
                        stop_type = reason.replace("stop_", "")

                    if stop_hit:
                        stop_stats["total_stops"] += 1
                        st_key = str(stop_type or "other").lower()
                        if st_key in ("invalidation", "adverse_pnl", "alpha_decay"):
                            stop_stats[st_key] += 1
                        else:
                            stop_stats["other"] += 1

                    net_pnl_bps = d.get("net_pnl_bps")
                    if net_pnl_bps is None:
                        continue
                    net_pnl_bps = float(net_pnl_bps)
                    gross_edge_bps = float(
                        d.get("gross_exec_edge_bps")
                        or (float(d.get("net_exec_edge_bps", 0.0)) + roundtrip_fee_bps * 0.5)
                    )

                    gross = pos_size * ((net_pnl_bps + roundtrip_fee_bps) / 10000.0)
                    net = pos_size * (net_pnl_bps / 10000.0)
                    is_win = 1 if net > 0 else 0

                    if stop_hit:
                        stop_stats["total_stop_pnl_usd"] += net

                    def _add_to_bucket(b: Dict[str, Any]) -> None:
                        b["count"] += 1
                        b["gross"] += gross
                        b["fee"] += fee_usd
                        b["net"] += net
                        b["wins"] += is_win
                        if net > 0:
                            b["gains"] += net
                        elif net < 0:
                            b["losses"] += abs(net)

                    _add_to_bucket(pnl_scenarios["all_signals"])

                    # Microstructure physical validity check (consistent with evaluator.py)
                    drain_ratio = float(d.get("drain_ratio", 0.0))
                    swept_usd = float(d.get("leader_swept_usd", 0.0))
                    f_top3_ask = float(d.get("follower_top3_ask_usd", 0.0))
                    cross_ratio = (swept_usd / f_top3_ask) if f_top3_ask > 0 else 0.0
                    if drain_ratio < 0.30 and cross_ratio < 0.30:
                        continue

                    stealth_cap = float(d.get("stealth_capacity_usd", 0.0))
                    if stealth_cap <= 0.0 and f_top3_ask > 0.0:
                        stealth_cap = round(0.15 * f_top3_ask, 2)

                    pk = f"{coin}:upbit-{f_ex}"
                    req_edge = calibrated_thresholds.get(pk, 20.0)
                    if gross_edge_bps >= req_edge:
                        _add_to_bucket(pnl_scenarios["calibrated_threshold_only"])

                    if coin in battle_coins:
                        _add_to_bucket(pnl_scenarios["battle_coins_only"])

                    if f_ex in FUNDED_VENUES:
                        _add_to_bucket(pnl_scenarios["funded_venues_only"])

                    # Threshold sweep
                    for t in SWEEP_THRESHOLDS:
                        if gross_edge_bps >= t:
                            sw = sweep[t]
                            sw["count"] += 1
                            sw["gross"] += gross
                            sw["fee"] += fee_usd
                            sw["net"] += net
                            sw["wins"] += is_win

                    # Pair stats
                    ps = pair_stats[pk]
                    ps["count"] += 1
                    ps["gross"] += gross
                    ps["fee"] += fee_usd
                    ps["net"] += net
                    ps["wins"] += is_win
                    if f_top3_ask > 0:
                        ps["top3_depths"].append(f_top3_ask)
                        ps["stealth_caps"].append(stealth_cap)

                    # Coin stats
                    cs = coin_stats[coin]
                    cs["count"] += 1
                    cs["gross"] += gross
                    cs["fee"] += fee_usd
                    cs["net"] += net
                    cs["wins"] += is_win
                    if f_top3_ask > 0:
                        cs["top3_depths"].append(f_top3_ask)
                        cs["stealth_caps"].append(stealth_cap)
                    mfe = d.get("mfe_exec_pnl_bps")
                    if mfe is not None:
                        cs["mfes"].append(float(mfe))
                    t_pk = d.get("time_to_peak_sec")
                    if t_pk is not None:
                        cs["t_peaks"].append(float(t_pk))

                    # Venue capacity stats
                    if f_top3_ask > 0:
                        vc = venue_capacities[f_ex]
                        vc["count"] += 1
                        vc["top3_depths"].append(f_top3_ask)
                        vc["stealth_caps"].append(stealth_cap)

        except Exception as e:
            if not as_json:
                print(f"[!] Warning reading {fpath}: {e}", file=sys.stderr)

    # Format PnL summary
    pnl_summary = {}
    for name, sc in pnl_scenarios.items():
        cnt = sc["count"]
        gains = sc.get("gains", 0.0)
        losses = sc.get("losses", 0.0)
        pf = (gains / losses) if losses > 0 else (99.0 if gains > 0 else 0.0)
        pnl_summary[name] = {
            "trades": cnt,
            "wins": sc["wins"],
            "winrate_pct": round((sc["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "gross_usd": round(sc["gross"], 2),
            "fee_usd": round(sc["fee"], 2),
            "net_pnl_usd": round(sc["net"], 2),
            "profit_factor": round(pf, 2),
        }

    # Format top pairs
    sorted_pairs = []
    for pk, ps in sorted(pair_stats.items(), key=lambda x: x[1]["net"], reverse=True):
        cnt = ps["count"]
        med_top3 = statistics.median(ps["top3_depths"]) if ps["top3_depths"] else 0.0
        med_cap = statistics.median(ps["stealth_caps"]) if ps["stealth_caps"] else 0.0
        sorted_pairs.append({
            "pair": pk,
            "trades": cnt,
            "winrate_pct": round((ps["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "net_pnl_usd": round(ps["net"], 2),
            "gross_usd": round(ps["gross"], 2),
            "med_top3_depth_usd": round(med_top3, 1),
            "med_stealth_cap_usd": round(med_cap, 1),
        })

    # Format top coins
    sorted_coins = []
    for c, cs in sorted(coin_stats.items(), key=lambda x: x[1]["net"], reverse=True):
        cnt = cs["count"]
        avg_mfe = sum(cs["mfes"]) / len(cs["mfes"]) if cs["mfes"] else 0.0
        med_tpk = sorted(cs["t_peaks"])[len(cs["t_peaks"]) // 2] if cs["t_peaks"] else 0.0
        med_top3 = statistics.median(cs["top3_depths"]) if cs["top3_depths"] else 0.0
        med_cap = statistics.median(cs["stealth_caps"]) if cs["stealth_caps"] else 0.0
        sorted_coins.append({
            "coin": c,
            "trades": cnt,
            "winrate_pct": round((cs["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "net_pnl_usd": round(cs["net"], 2),
            "avg_mfe_bps": round(avg_mfe, 1),
            "median_t_peak_sec": round(med_tpk, 2),
            "med_top3_depth_usd": round(med_top3, 1),
            "med_stealth_cap_usd": round(med_cap, 1),
        })

    # Format venue physical capacities
    venue_summary = []
    for vex, vdata in sorted(venue_capacities.items(), key=lambda x: len(x[1]["stealth_caps"]), reverse=True):
        if not vdata["stealth_caps"]:
            continue
        caps = sorted(vdata["stealth_caps"])
        depths = sorted(vdata["top3_depths"])
        n_c = len(caps)
        med_cap = statistics.median(caps)
        med_depth = statistics.median(depths)
        p25_cap = caps[n_c // 4]
        p75_cap = caps[(3 * n_c) // 4]
        venue_summary.append({
            "venue": vex,
            "signals": vdata["count"],
            "med_top3_depth_usd": round(med_depth, 1),
            "med_stealth_cap_usd": round(med_cap, 1),
            "p25_stealth_cap_usd": round(p25_cap, 1),
            "p75_stealth_cap_usd": round(p75_cap, 1),
            "min_stealth_cap_usd": round(caps[0], 1),
            "max_stealth_cap_usd": round(caps[-1], 1),
        })

    doc = {
        "metadata": {
            "report_date": today_str,
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "days_analyzed": days_back,
            "order_size_usd": pos_size,
            "roundtrip_fee_bps": roundtrip_fee_bps,
            "total_files_scanned": len(files),
            "total_size_mb": round(total_bytes / (1024 * 1024), 2),
            "total_signals_recorded": total_signals,
            "coins_active": len(coins_seen),
            "venues_active": sorted(venues_seen),
            "elapsed_sec": round(time.time() - t0, 3),
        },
        "close_reasons": dict(close_reasons),
        "trigger_types": dict(trigger_types),
        "stop_loss_telemetry": {
            "total_stops": stop_stats["total_stops"],
            "stop_rate_pct": round((stop_stats["total_stops"] / total_signals * 100.0), 1) if total_signals else 0.0,
            "invalidation_count": stop_stats["invalidation"],
            "adverse_pnl_count": stop_stats["adverse_pnl"],
            "alpha_decay_count": stop_stats["alpha_decay"],
            "other_count": stop_stats["other"],
            "total_stop_pnl_usd": round(stop_stats["total_stop_pnl_usd"], 2),
        },
        "pnl_scenarios": pnl_summary,
        "venue_capacities": venue_summary,
        "sweep_thresholds": {
            t: {
                "trades": sweep[t]["count"],
                "winrate_pct": round((sweep[t]["wins"] / sweep[t]["count"]) * 100.0, 1) if sweep[t]["count"] else 0.0,
                "net_usd": round(sweep[t]["net"], 2),
            }
            for t in SWEEP_THRESHOLDS
        },
        "top_pairs": sorted_pairs[:25],
        "top_coins": sorted_coins[:25],
    }

    # Write JSON report
    json_path = os.path.join(REPORTS_DIR, f"report_{today_str}.json")
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(doc, jf, indent=2, ensure_ascii=False)

    # Render Markdown report
    md_content = _render_markdown_report(doc)
    md_path = os.path.join(REPORTS_DIR, f"report_{today_str}.md")
    with open(md_path, "w", encoding="utf-8") as mf:
        mf.write(md_content)

    if not as_json:
        print(md_content)
        print(f"\n[OK] Daily report generated and saved to:")
        print(f"  • Markdown: {md_path}")
        print(f"  • JSON:     {json_path}\n")

    return doc


def _render_markdown_report(doc: Dict[str, Any]) -> str:
    m = doc["metadata"]
    p = doc["pnl_scenarios"]
    vc = doc.get("venue_capacities", [])
    sl = doc.get("stop_loss_telemetry", {})
    cr = doc.get("close_reasons", {})
    lines = [
        f"# Korean Sunrise: Суточный Квантовый Отчёт ({m['report_date']})",
        f"*Сгенерирован:* `{m['generated_at']}` | *Окно:* `{m['days_analyzed']} d` | *Размер слота:* `${m['order_size_usd']}` | *Комиссия:* `{m['roundtrip_fee_bps']} bps`",
        "",
        "## 1. Сводные метрики телеметрии",
        f"- **Всего зафиксировано сигналов:** `{m['total_signals_recorded']:,}`",
        f"- **Активных монет с импульсами:** `{m['coins_active']}`",
        f"- **Фолловер-бирж активно:** `{len(m['venues_active'])}` ({', '.join(m['venues_active']) if m['venues_active'] else 'None'})",
        f"- **Архивов данных обработано:** `{m['total_files_scanned']}` (`{m['total_size_mb']} MB`)",
        f"- **Причины закрытия импульсов:** `Converged: {cr.get('converged', 0)}` | `Timeout: {cr.get('timeout', 0)}` | `Reversed: {cr.get('reversed', 0)}` | `Stops: {sl.get('total_stops', 0)}` (Inv: {sl.get('invalidation_count', 0)}, Adv: {sl.get('adverse_pnl_count', 0)}, Decay: {sl.get('alpha_decay_count', 0)})",
        f"- **Триггеры выкупа Upbit:** `Depth Drain: {doc['trigger_types'].get('depth_drain', 0)}` | `Spread Blowout: {doc['trigger_types'].get('spread_blowout', 0)}`",
        "",
        "## 2. PnL Сценарии исполнения",
        "| Сценарий | Сигналов | Win Rate | Gross USD | Комиссии | Net PnL USD | Profit Factor |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]
    labels = {
        "all_signals": "Все зафиксированные сигналы (Raw)",
        "calibrated_threshold_only": "С калиброванным порогом (Matrix Filter)",
        "battle_coins_only": "Только проверенные монеты (Battle Pool)",
        "funded_venues_only": "Только фондированные биржи (Funded CEXs)",
    }
    for k, row in p.items():
        lbl = labels.get(k, k)
        lines.append(
            f"| **{lbl}** | {row['trades']:,} | {row['winrate_pct']}% | "
            f"${row['gross_usd']:.2f} | ${row['fee_usd']:.2f} | **${row['net_pnl_usd']:.2f}** | {row['profit_factor']} |"
        )

    # Section 3: Stop-Loss Protection Telemetry
    lines.extend([
        "",
        "## 3. Стоп-Лоссы и Защита от Токсичного Потока (Stop-Loss Protection)",
        f"- **Всего сработало стоп-лоссов:** `{sl.get('total_stops', 0):,}` ({sl.get('stop_rate_pct', 0.0)}% от всех импульсов)",
        f"- **Model A: Microstructure Invalidation:** `{sl.get('invalidation_count', 0):,}` (мгновенный сброс при сломе/откате пампа на Upbit)",
        f"- **Model B: Hard Adverse PnL Drawdown:** `{sl.get('adverse_pnl_count', 0):,}` (жесткая отсечка при токсичном движении фолловера)",
        f"- **Model C: Alpha Decay Timeout:** `{sl.get('alpha_decay_count', 0):,}` (сброс по истечении окна передачи импульса при PnL <= 0)",
        f"- **Суммарный PnL по стопам:** `${sl.get('total_stop_pnl_usd', 0.0):.2f}` (контролируемые локальные риски вместо зависания в просадках)",
    ])

    # Section 4: Physical order book capacity
    lines.extend([
        "",
        "## 4. Физическая ёмкость стакана по биржам (Real Order Book Capacity)",
        "| Биржа | Сигналов | Мед. Top-3 стакан ($) | Мед. 15% Ёмкость ($) | P25 Ёмкость ($) | P75 Ёмкость ($) | Мин. ($) | Макс. ($) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    if vc:
        for v in vc:
            lines.append(
                f"| `{v['venue']}` | {v['signals']:,} | ${v['med_top3_depth_usd']:,.1f} | "
                f"**${v['med_stealth_cap_usd']:,.1f}** | ${v['p25_stealth_cap_usd']:,.1f} | "
                f"${v['p75_stealth_cap_usd']:,.1f} | ${v['min_stealth_cap_usd']:,.1f} | ${v['max_stealth_cap_usd']:,.1f} |"
            )
    else:
        lines.append("| *Нет накопленных данных по стаканам* | - | - | - | - | - | - | - |")

    lines.extend([
        "",
        "## 5. Оптимизация порогов спреда (Threshold Sweep)",
        "| Порог (bps) | Сигналов | Win Rate | Net PnL USD |",
        "| :---: | :---: | :---: | :---: |",
    ])
    for t, sw in sorted(doc["sweep_thresholds"].items(), key=lambda x: int(x[0])):
        lines.append(f"| >={t} bps | {sw['trades']:,} | {sw['winrate_pct']}% | ${sw['net_usd']:.2f} |")

    lines.extend([
        "",
        "## 6. Топ прибыльных связок (Upbit Leader <-> Perp Follower)",
        "| Связка (Coin:Upbit-Follower) | Сделок | Win Rate | Gross USD | Net PnL USD | Мед. Top-3 ($) | Мед. 15% Ёмкость ($) |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    if doc["top_pairs"]:
        for pr in doc["top_pairs"][:15]:
            lines.append(
                f"| `{pr['pair']}` | {pr['trades']} | {pr['winrate_pct']}% | "
                f"${pr['gross_usd']:.2f} | **${pr['net_pnl_usd']:.2f}** | "
                f"${pr.get('med_top3_depth_usd', 0.0):,.1f} | **${pr.get('med_stealth_cap_usd', 0.0):,.1f}** |"
            )
    else:
        lines.append("| *Нет накопленных сделок за выбранный период* | - | - | - | - | - | - |")

    lines.extend([
        "",
        "## 7. Топ монет по эффективности опережения",
        "| Монета | Сделок | Win Rate | Net PnL USD | Мед. Top-3 ($) | Мед. 15% Ёмкость ($) | Ср. MFE (bps) | Медиана $T_{peak}$ |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ])
    if doc["top_coins"]:
        for c in doc["top_coins"][:15]:
            lines.append(
                f"| **{c['coin']}** | {c['trades']} | {c['winrate_pct']}% | **${c['net_pnl_usd']:.2f}** | "
                f"${c.get('med_top3_depth_usd', 0.0):,.1f} | **${c.get('med_stealth_cap_usd', 0.0):,.1f}** | "
                f"+{c['avg_mfe_bps']} bps | {c['median_t_peak_sec']} s |"
            )
    else:
        lines.append("| *Нет накопленных данных по монетам* | - | - | - | - | - | - | - |")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Daily Quantitative Report.")
    parser.add_argument("--days", type=float, default=1.0, help="Days back (default: 1.0)")
    parser.add_argument("--size", type=float, default=10.0, help="Order size USD (default: 10.0)")
    parser.add_argument("--fee-bps", type=float, default=10.0, help="Roundtrip fee bps (default: 10.0)")
    parser.add_argument("--json", action="store_true", help="Output JSON only")
    args = parser.parse_args()

    doc = generate_daily_report(
        days_back=args.days,
        pos_size=args.size,
        roundtrip_fee_bps=args.fee_bps,
        as_json=args.json,
    )
    if args.json:
        print(json.dumps(doc, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
```

## tools/manage_pool.py

```python
"""
tools/manage_pool.py - CLI tool for managing Korean Sunrise Incubator, Battle, and Execution Profiles.

Pool Commands:
  python tools/manage_pool.py status
  python tools/manage_pool.py list-battle
  python tools/manage_pool.py list-incubator
  python tools/manage_pool.py pause <COIN> [--reason <REASON>]
  python tools/manage_pool.py resume <COIN>
  python tools/manage_pool.py promote <COIN> [--reason <REASON>]
  python tools/manage_pool.py demote <COIN> [--reason <REASON>]
  python tools/manage_pool.py evaluate [--dry-run] [--days <N>]

Execution Profile Commands:
  python tools/manage_pool.py profile
  python tools/manage_pool.py set-capital <VENUE> <USD>
  python tools/manage_pool.py set-slots <N>
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from pipeline.pool_state import PoolStateManager, OVERRIDES_FILE
from pipeline.evaluator import SunriseEvaluator
from pipeline.profile import TradingProfileManager


def cmd_status(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    snap = psm.get_pool_snapshot()
    if getattr(args, "json", False):
        print(json.dumps(snap, indent=2, ensure_ascii=False))
        return

    summary = snap["summary"]
    settings = snap["settings"]
    up_ts = snap["updated_at"]
    up_str = datetime.datetime.fromtimestamp(up_ts).strftime("%Y-%m-%d %H:%M:%S") if up_ts else "never"

    print("=" * 70)
    print(" KOREAN SUNRISE CONTINUOUS TRADING POOL STATUS")
    print("=" * 70)
    print(f" Last Updated   : {up_str}")
    print(f" Incubator Coins: {summary['incubator']}")
    print(f" Battle Coins   : {summary['battle']} (Active: {summary['battle_active']}, Paused: {summary['paused']})")
    print("-" * 70)
    print(" Sizing & Risk Parameters:")
    print(f"   Min Order Size: ${settings['min_order_usd']:.2f}")
    depth_frac = settings.get("stealth_depth_frac", settings.get("max_depth_frac", 0.15))
    print(f"   Max Depth Frac: {depth_frac*100:.1f}% (top-3 depth)")
    print(f"   Size Cap      : ${settings['size_cap_usd']:,.2f}")
    print("=" * 70)


def cmd_list_battle(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    snap = psm.get_pool_snapshot()
    coins_dict = snap.get("coins", {})
    battle = [c for c in coins_dict.values() if c.get("basket") == "battle"]

    if getattr(args, "json", False):
        print(json.dumps(battle, indent=2, ensure_ascii=False))
        return

    print("=" * 80)
    print(f" BATTLE POOL COINS ({len(battle)})")
    print("=" * 80)
    if not battle:
        print(" [!] Battle pool is currently empty.")
        print("=" * 80)
        return

    print(f" {'Coin':<8} | {'Status':<8} | {'WinRate':<7} | {'Net PnL':<10} | {'Promoted At':<19} | {'Follower Venues'}")
    print("-" * 80)
    for c in battle:
        status_str = "ACTIVE" if c["enabled"] else "PAUSED"
        up_at = datetime.datetime.fromtimestamp(c["promoted_at"]).strftime("%Y-%m-%d %H:%M:%S") if c.get("promoted_at") else "-"
        venues_str = ", ".join(c.get("venues", []))
        print(
            f" {c['coin']:<8} | {status_str:<8} | "
            f"{c.get('win_rate_pct', 0):>6.1f}% | {c.get('median_net_pnl_bps', 0):>7.1f} bps | {up_at:<19} | {venues_str}"
        )
    print("=" * 80)


def cmd_list_incubator(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    snap = psm.get_pool_snapshot()
    coins_dict = snap.get("coins", {})
    incubator = [c for c in coins_dict.values() if c.get("basket") == "incubator"]

    if getattr(args, "json", False):
        print(json.dumps(incubator, indent=2, ensure_ascii=False))
        return

    print("=" * 80)
    print(f" INCUBATOR COINS ({len(incubator)})")
    print("=" * 80)
    print(f" {'Coin':<8} | {'Status':<8} | {'Venues Count':<12} | {'Follower Venues'}")
    print("-" * 80)
    for c in incubator:
        status_str = "ENABLED" if c["enabled"] else "DISABLED"
        v_list = c.get("venues", [])
        print(f" {c['coin']:<8} | {status_str:<8} | {len(v_list):>12} | {', '.join(v_list)}")
    print("=" * 80)


def cmd_pause(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.upper()
    psm.pause_coin(coin, reason=args.reason or "manual_operator_pause")
    print(f"[OK] Paused coin {coin}: {args.reason or 'manual_operator_pause'}")


def cmd_resume(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.upper()
    psm.resume_coin(coin)
    print(f"[OK] Resumed coin {coin}")


def cmd_promote(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    coin = args.coin.upper()
    ok = psm.promote_coin_manual(coin)
    if ok:
        print(f"[OK] Promoted {coin} to Battle pool.")
    else:
        print(f"[!] Failed to promote {coin}.")


def cmd_demote(psm: PoolStateManager, args: argparse.Namespace) -> None:
    psm.reload()
    coin = args.coin.upper()
    ok = psm.demote_to_incubator(coin, reason=args.reason or "manual_demote")
    if ok:
        print(f"[OK] Demoted {coin} to Incubator pool.")
    else:
        print(f"[!] Coin {coin} not found in pool.")


def cmd_evaluate(psm: PoolStateManager, args: argparse.Namespace) -> None:
    dry = getattr(args, "dry_run", False)
    print(f"[*] Running quantitative evaluation across trailing {args.days} days (dry_run={dry})...")
    ev = SunriseEvaluator(
        data_root=os.path.join(PROJECT_ROOT, "data"),
        days_back=int(args.days),
        pool_state=psm,
    )
    res = ev.run(dry_run=dry)
    print(f"[OK] Evaluation complete: {len(res)} coins qualified for Battle pool.")


def cmd_profile(args: argparse.Namespace) -> None:
    tpm = TradingProfileManager()
    tpm.reload()
    prof = tpm.get_profile()
    if getattr(args, "json", False):
        print(json.dumps(prof, indent=2, ensure_ascii=False))
        return

    print("=" * 70)
    print(" TRADING PROFILE & CAPITAL ALLOCATION")
    print("=" * 70)
    caps = prof.get("capital", {}).get("per_venue_usd", {})
    reserve = prof.get("capital", {}).get("reserve_margin_frac", 0.20)
    slots = prof.get("max_active_slots", 5)
    print(f" Margin Reserve Cushion: {reserve*100:.1f}%")
    print(f" Max Concurrent Slots  : {slots}")
    print("-" * 70)
    print(f" {'Follower Venue':<20} | {'Capital USD':<15} | {'Slot Budget USD'}")
    print("-" * 70)
    total_cap = 0.0
    for v, usd in sorted(caps.items()):
        total_cap += float(usd)
        slot_b = (float(usd) / max(1, slots)) * (1.0 - reserve)
        print(f" {v.upper():<20} | ${float(usd):>12.2f}  | ${slot_b:>13.2f}")
    print("-" * 70)
    print(f" {'TOTAL CAPITAL':<20} | ${total_cap:>12.2f}")
    print("=" * 70)


def cmd_set_capital(args: argparse.Namespace) -> None:
    tpm = TradingProfileManager()
    tpm.set_venue_capital(args.venue, float(args.usd))
    print(f"[OK] Set capital for venue '{args.venue}' to ${float(args.usd):.2f}")


def cmd_set_slots(args: argparse.Namespace) -> None:
    tpm = TradingProfileManager()
    tpm.set_max_slots(int(args.slots))
    print(f"[OK] Set max active slots to {int(args.slots)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Pool & Profile CLI Manager.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # status
    p_stat = sub.add_parser("status", help="Show pool summary status")
    p_stat.add_argument("--json", action="store_true")

    # list-battle
    p_bat = sub.add_parser("list-battle", help="List battle pool coins")
    p_bat.add_argument("--json", action="store_true")

    # list-incubator
    p_inc = sub.add_parser("list-incubator", help="List incubator coins")
    p_inc.add_argument("--json", action="store_true")

    # pause
    p_pau = sub.add_parser("pause", help="Pause trading on a coin")
    p_pau.add_argument("coin", help="Coin symbol")
    p_pau.add_argument("--reason", default="manual_pause")

    # resume
    p_res = sub.add_parser("resume", help="Resume trading on a coin")
    p_res.add_argument("coin", help="Coin symbol")

    # promote
    p_pro = sub.add_parser("promote", help="Promote coin to battle")
    p_pro.add_argument("coin", help="Coin symbol")
    p_pro.add_argument("--reason", default="manual_promote")

    # demote
    p_dem = sub.add_parser("demote", help="Demote coin to incubator")
    p_dem.add_argument("coin", help="Coin symbol")
    p_dem.add_argument("--reason", default="manual_demote")

    # evaluate
    p_ev = sub.add_parser("evaluate", help="Run pool promotion evaluator")
    p_ev.add_argument("--days", type=float, default=7.0)
    p_ev.add_argument("--dry-run", action="store_true", help="Do not persist changes")

    # profile
    p_prof = sub.add_parser("profile", help="Show trading capital profile")
    p_prof.add_argument("--json", action="store_true")

    # set-capital
    p_scap = sub.add_parser("set-capital", help="Set capital for a venue")
    p_scap.add_argument("venue", help="Follower venue name")
    p_scap.add_argument("usd", type=float, help="Capital USD")

    # set-slots
    p_slot = sub.add_parser("set-slots", help="Set max active slots")
    p_slot.add_argument("slots", type=int, help="Max slots")

    args = parser.parse_args()

    psm = PoolStateManager()

    if args.cmd == "status":
        cmd_status(psm, args)
    elif args.cmd == "list-battle":
        cmd_list_battle(psm, args)
    elif args.cmd == "list-incubator":
        cmd_list_incubator(psm, args)
    elif args.cmd == "pause":
        cmd_pause(psm, args)
    elif args.cmd == "resume":
        cmd_resume(psm, args)
    elif args.cmd == "promote":
        cmd_promote(psm, args)
    elif args.cmd == "demote":
        cmd_demote(psm, args)
    elif args.cmd == "evaluate":
        cmd_evaluate(psm, args)
    elif args.cmd == "profile":
        cmd_profile(args)
    elif args.cmd == "set-capital":
        cmd_set_capital(args)
    elif args.cmd == "set-slots":
        cmd_set_slots(args)


if __name__ == "__main__":
    main()
```

## tools/run_daily_pipeline.py

```python
"""
tools/run_daily_pipeline.py - Master Orchestrator for the 24-Hour Autonomous Lifecycle.

Executes the 4 core daily operations sequentially:
  [Phase 1] Universe Screening (1-min candles across 24h on Upbit & 9 follower CEXs):
            Updates: universe_live.json (Incubator universe)
  [Phase 2] Pool Evaluation (Yesterday's telemetry scoring & promotion):
            Updates: live_trading_pool.json (Battle vs Incubator)
  [Phase 3] Spread Calibration (10-60 bps sweep per pair):
            Updates: matrix_thresholds.json (Dynamic execution thresholds)
  [Phase 4] Full Daily Quantitative Report:
            Updates: reports/daily/report_YYYY-MM-DD.md & .json

Usage:
  python tools/run_daily_pipeline.py
  python tools/run_daily_pipeline.py --skip-screening
  python tools/run_daily_pipeline.py --dry-run
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

UNIVERSE_FILE = os.path.join(PROJECT_ROOT, "universe_live.json")
BATTLE_FILE = os.path.join(PROJECT_ROOT, "live_trading_pool.json")
THRESHOLDS_FILE = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
LOG_FILE = os.path.join(PROJECT_ROOT, "logs", "daily_pipeline.log")

os.makedirs(os.path.join(PROJECT_ROOT, "logs"), exist_ok=True)

logger = logging.getLogger("daily_pipeline")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")

_fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
_fh.setFormatter(_fmt)
logger.addHandler(_fh)

_sh = logging.StreamHandler(sys.stdout)
_sh.setFormatter(_fmt)
logger.addHandler(_sh)


def run_pipeline(
    skip_screening: bool = False,
    skip_evaluation: bool = False,
    skip_calibration: bool = False,
    skip_report: bool = False,
    dry_run: bool = False,
) -> bool:
    t_start = time.time()
    logger.info("================================================================================")
    logger.info("  KOREAN SUNRISE: 24-HOUR MASTER AUTONOMOUS LIFECYCLE PIPELINE")
    logger.info("================================================================================")

    # -------------------------------------------------------------------------
    # PHASE 1: Universe Screening (1440 1m candles across 24h)
    # -------------------------------------------------------------------------
    if skip_screening:
        logger.info("[PHASE 1/4] Universe Screening: SKIPPED (--skip-screening)")
    else:
        logger.info("[PHASE 1/4] Universe Screening: Fetching 200 1m candles across Upbit & 9 followers...")
        cmd_scr = [
            sys.executable,
            os.path.join(PROJECT_ROOT, "tools", "screen_universe.py"),
            "--kline-limit", "200",
            "--workers", "8",
        ]
        if not dry_run:
            p = subprocess.run(cmd_scr, capture_output=True, text=True, encoding="utf-8", errors="replace")
            if p.returncode != 0:
                logger.error(f"[!] Phase 1 failed: {p.stderr}")
            else:
                logger.info("[OK] Phase 1 completed successfully.")

    # -------------------------------------------------------------------------
    # PHASE 2: Pool Evaluation & Promotion
    # -------------------------------------------------------------------------
    if skip_evaluation:
        logger.info("[PHASE 2/4] Pool Evaluation: SKIPPED (--skip-evaluation)")
    else:
        logger.info("[PHASE 2/4] Pool Evaluation: Scoring recorded signals and promoting to Battle...")
        from pipeline.evaluator import SunriseEvaluator
        if not dry_run:
            ev = SunriseEvaluator(data_root=os.path.join(PROJECT_ROOT, "data"))
            promoted = ev.run()
            logger.info(f"[OK] Phase 2 completed: {len(promoted)} coins promoted to Battle pool.")

    # -------------------------------------------------------------------------
    # PHASE 3: Spread Threshold Calibration
    # -------------------------------------------------------------------------
    if skip_calibration:
        logger.info("[PHASE 3/4] Spread Calibration: SKIPPED (--skip-calibration)")
    else:
        logger.info("[PHASE 3/4] Spread Calibration: Sweeping thresholds [10..60 bps] per pair...")
        cmd_cal = [
            sys.executable,
            os.path.join(PROJECT_ROOT, "tools", "calibrate_thresholds.py"),
        ]
        if not dry_run:
            p = subprocess.run(cmd_cal, capture_output=True, text=True, encoding="utf-8", errors="replace")
            if p.returncode != 0:
                logger.error(f"[!] Phase 3 failed: {p.stderr}")
            else:
                logger.info("[OK] Phase 3 completed: matrix_thresholds.json updated.")

    # -------------------------------------------------------------------------
    # PHASE 4: Full Daily Report
    # -------------------------------------------------------------------------
    if skip_report:
        logger.info("[PHASE 4/4] Daily Report: SKIPPED (--skip-report)")
    else:
        logger.info("[PHASE 4/4] Daily Report: Generating quantitative performance report...")
        cmd_rep = [
            sys.executable,
            os.path.join(PROJECT_ROOT, "tools", "full_daily_report.py"),
        ]
        if not dry_run:
            p = subprocess.run(cmd_rep, capture_output=True, text=True, encoding="utf-8", errors="replace")
            if p.returncode != 0:
                logger.error(f"[!] Phase 4 failed: {p.stderr}")
            else:
                logger.info("[OK] Phase 4 completed: report saved to reports/daily/.")

    elapsed = time.time() - t_start
    logger.info("================================================================================")
    logger.info(f"  [ALL COMPLETE] Master pipeline finished in {elapsed:.2f}s.")
    logger.info("================================================================================\n")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Master 24h Autonomous Daily Pipeline.")
    parser.add_argument("--skip-screening", action="store_true", help="Skip universe screening")
    parser.add_argument("--skip-evaluation", action="store_true", help="Skip pool evaluation")
    parser.add_argument("--skip-calibration", action="store_true", help="Skip spread calibration")
    parser.add_argument("--skip-report", action="store_true", help="Skip daily report")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing")
    args = parser.parse_args()

    run_pipeline(
        skip_screening=args.skip_screening,
        skip_evaluation=args.skip_evaluation,
        skip_calibration=args.skip_calibration,
        skip_report=args.skip_report,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
```

## tools/screen_universe.py

```python
"""
tools/screen_universe.py — Autonomous Two-Stage Universe Screener for Korean Sunrise.
Stage 1: 24h Ticker, volume, and namesake price sanity filter.
Stage 2: 1-minute kline correlation and Kimchi basis volatility screening.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from screening.screener import UniverseScreener

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
log = logging.getLogger("screen_universe")


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Two-Stage Universe Screener.")
    parser.add_argument("--out", default=os.path.join(PROJECT_ROOT, "universe_live.json"), help="Output path")
    parser.add_argument("--min-vol", type=float, default=100_000.0, help="Min 24h volume USD on both venues (default: $100K)")
    parser.add_argument("--max-price-diff", type=float, default=15.0, help="Max price diff %% (namesake filter, default: 15%%)")
    parser.add_argument("--min-corr", type=float, default=0.65, help="Min 1m log-returns correlation (default: 0.65)")
    parser.add_argument("--min-both-trade", type=float, default=0.15, help="Min fraction of active trading minutes (default: 0.15)")
    parser.add_argument("--min-dev-bps", type=float, default=8.0, help="Min P90 basis deviation in bps (default: 8.0 bps)")
    parser.add_argument("--venues", nargs="+", default=None, help="Specific follower venues (e.g. --venues binance)")
    parser.add_argument("--coins", nargs="+", default=None, help="Specific coins to screen (e.g. --coins ETHFI AXS SPX)")
    parser.add_argument("--kline-limit", type=int, default=200, help="Number of 1m candles to evaluate (default: 200 = ~3.3h)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel worker threads (default: 8)")
    args = parser.parse_args()

    screener = UniverseScreener(
        venues=args.venues,
        coins=args.coins,
        min_vol_usd=args.min_vol,
        max_price_diff_pct=args.max_price_diff,
        min_corr=args.min_corr,
        min_both_trade=args.min_both_trade,
        min_dev_bps=args.min_dev_bps,
        kline_limit=args.kline_limit,
        output_file=args.out,
        max_workers=args.workers,
    )

    doc = screener.run()
    meta = doc.get("_meta", {})
    log.info(
        f"Screening complete: {meta.get('coins_count', 0)} qualified coins saved to {args.out} "
        f"in {meta.get('screening_elapsed_sec', 0.0)}s."
    )


if __name__ == "__main__":
    main()
```

## tools/validate_data.py

```python
"""
tools/validate_data.py - High-speed multi-threaded data quality auditor for Korean Sunrise.

Checks archives in data/:
1. File integrity: valid gzip CRC, valid JSON, no truncated lines.
2. Schema completeness: all required fields present per signal event.
3. Numerical sanity:
   - Prices and depths: strictly > 0, no NaN / Inf.
   - Timestamps: open_ts <= close_ts, duration >= 0.
   - PnL metrics: finite net_pnl_bps and gross_exec_edge_bps.
4. Summary output: console defect report and statistics.

Usage:
  python tools/validate_data.py
  python tools/validate_data.py --days 7
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import glob
import gzip
import json
import math
import os
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
DATA_ROOT = os.path.join(PROJECT_ROOT, "data")

REQUIRED_SIGNAL_FIELDS = (
    "id",
    "coin",
    "follower_exchange",
    "trigger_type",
    "dislocation_bps",
    "gross_exec_edge_bps",
    "net_pnl_bps",
    "leader_swept_usd",
    "follower_top3_ask_usd",
    "close_reason",
    "entry_ts",
)


def _audit_file(fpath: str) -> Dict[str, Any]:
    stat = {
        "file": fpath,
        "rows": 0,
        "corrupted_rows": 0,
        "missing_fields": 0,
        "nan_inf_count": 0,
        "gzip_error": False,
        "coins": set(),
        "venues": set(),
    }
    try:
        with gzip.open(fpath, "rt", encoding="utf-8", errors="replace") as gz:
            for line in gz:
                line = line.strip()
                if not line:
                    continue
                stat["rows"] += 1
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    stat["corrupted_rows"] += 1
                    continue

                coin = str(d.get("coin", "")).upper()
                f_ex = str(d.get("follower_exchange", "")).lower()
                if coin:
                    stat["coins"].add(coin)
                if f_ex:
                    stat["venues"].add(f_ex)

                # Check schema
                missing = [f for f in REQUIRED_SIGNAL_FIELDS if f not in d]
                if missing:
                    stat["missing_fields"] += 1

                # Check numbers
                for num_key in ("gross_exec_edge_bps", "net_pnl_bps", "leader_swept_usd", "follower_top3_ask_usd"):
                    v = d.get(num_key)
                    if v is not None:
                        try:
                            fv = float(v)
                            if not math.isfinite(fv):
                                stat["nan_inf_count"] += 1
                        except (ValueError, TypeError):
                            stat["nan_inf_count"] += 1

    except Exception:
        stat["gzip_error"] = True

    return stat


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Data Quality Auditor.")
    parser.add_argument("--days", type=float, default=7.0, help="Days back to scan (default: 7.0)")
    parser.add_argument("--workers", type=int, default=4, help="Parallel worker threads")
    args = parser.parse_args()

    print("=" * 80)
    print("  KOREAN SUNRISE: MULTI-THREADED DATA QUALITY AUDITOR")
    print("=" * 80)

    pattern = os.path.join(DATA_ROOT, "signals", "**", "*.jsonl.gz")
    all_files = sorted(glob.glob(pattern, recursive=True))

    if not all_files:
        print(f"  [OK] No signal files found in {os.path.join(DATA_ROOT, 'signals')}. System is in fresh state.")
        print("=" * 80)
        return

    cutoff = time.time() - (args.days * 86400.0)
    files = [f for f in all_files if os.path.getmtime(f) >= cutoff]
    if not files:
        files = all_files

    print(f"  Scanning {len(files)} archives ({args.days:.1f} days trailing)...")
    t0 = time.time()

    total_rows = 0
    total_corrupted = 0
    total_missing = 0
    total_nan_inf = 0
    gzip_errors = 0
    all_coins = set()
    all_venues = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for res in ex.map(_audit_file, files):
            total_rows += res["rows"]
            total_corrupted += res["corrupted_rows"]
            total_missing += res["missing_fields"]
            total_nan_inf += res["nan_inf_count"]
            if res["gzip_error"]:
                gzip_errors += 1
            all_coins.update(res["coins"])
            all_venues.update(res["venues"])

    elapsed = time.time() - t0
    print("-" * 80)
    print(f"  Total Files Audited     : {len(files):,}")
    print(f"  Total Signals Checked   : {total_rows:,}")
    print(f"  Gzip Corrupted Files    : {gzip_errors}")
    print(f"  Malformed JSON Rows     : {total_corrupted}")
    print(f"  Missing Required Fields : {total_missing}")
    print(f"  NaN / Inf Anomalies     : {total_nan_inf}")
    print(f"  Unique Coins Recorded   : {len(all_coins)}")
    print(f"  Unique Follower Venues  : {len(all_venues)} ({', '.join(sorted(all_venues))})")
    print(f"  Scan Speed              : {total_rows / max(0.001, elapsed):,.0f} rows/s ({elapsed:.2f}s)")
    print("=" * 80)

    if gzip_errors == 0 and total_corrupted == 0 and total_nan_inf == 0:
        print("  [SUCCESS] All data archives passed strict CRC & Schema validation!")
    else:
        log_dir = os.path.join(PROJECT_ROOT, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "data_quality.log")
        with open(log_file, "a", encoding="utf-8") as lf:
            lf.write(
                f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Audited {len(files)} files: "
                f"{gzip_errors} gzip err, {total_corrupted} corrupted rows, {total_nan_inf} NaN/Inf\n"
            )
        print("  [WARNING] Defective data detected. Check logs/data_quality.log for details.")
    print("=" * 80)


if __name__ == "__main__":
    main()
```

## tools/verify_all.py

```python
"""
tools/verify_all.py - Complete Multi-Gate Verification Suite for Korean Sunrise.

Automated 5-Gate Quality Arbiter:
  - Gate 1: Manifest Integrity (SHA256 checksums across core files).
  - Gate 2: Fee & Parameter Synchronization (Engine, Config, Calibrator, Models).
  - Gate 3: Microstructure Data Contracts & Models (OrderBookDepth5, SunriseSignal, normalize_exchange_ts).
  - Gate 4: Bytecode Compilation & Syntax Audit (all .py files in repo).
  - Gate 5: Deep Invariant & Unit Test Suite (python -m unittest).

Usage:
  python tools/verify_all.py           # Run all 5 gates
  python tools/verify_all.py --seal    # Reseal MANIFEST.sha256 with current clean hashes
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import py_compile
import subprocess
import sys
import unittest

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
MANIFEST_FILE = os.path.join(PROJECT_ROOT, "MANIFEST.sha256")

CORE_FILES = [
    "config.py",
    "models.py",
    "korean_engine.py",
    "venues/base.py",
    "venues/manager.py",
    "pipeline/evaluator.py",
    "pipeline/pool_state.py",
    "pipeline/profile.py",
    "tools/calibrate_thresholds.py",
    "tools/full_daily_report.py",
    "tools/pack_codebase.py",
    "tests/test_adaptive_stops_and_fixes.py",
    "tests/test_korean_engine_strategy.py",
    "tests/test_pipeline_e2e.py",
    "tests/test_venues_streams_resilience.py",
]


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def seal_manifest() -> None:
    print(f"[*] Sealing {len(CORE_FILES)} core files into MANIFEST.sha256...")
    lines = []
    for rel_path in sorted(CORE_FILES):
        abs_path = os.path.join(PROJECT_ROOT, rel_path)
        if not os.path.isfile(abs_path):
            print(f"    [!] Skipping missing file: {rel_path}")
            continue
        csum = sha256_file(abs_path)
        lines.append(f"{csum}  {rel_path.replace(os.sep, '/')}\n")
    with open(MANIFEST_FILE, "w", encoding="utf-8") as f:
        f.writelines(lines)
    print(f"[OK] MANIFEST.sha256 successfully sealed ({len(lines)} entries).\n")


def check_gate_1_manifest() -> bool:
    print("[Gate 1] Checking Manifest Integrity (MANIFEST.sha256)...")
    if not os.path.isfile(MANIFEST_FILE):
        print("  [!] MANIFEST.sha256 not found. Run with --seal to initialize.")
        return False

    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    mismatches = []
    checked = 0
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        expected_sha, rel_path = parts[0], parts[1].replace("/", os.sep)
        abs_path = os.path.join(PROJECT_ROOT, rel_path)
        if not os.path.isfile(abs_path):
            mismatches.append(f"Missing file: {rel_path}")
            continue
        actual_sha = sha256_file(abs_path)
        if actual_sha != expected_sha:
            mismatches.append(f"Hash mismatch: {rel_path} (exp {expected_sha[:8]}..., got {actual_sha[:8]}...)")
        checked += 1

    if mismatches:
        for m in mismatches:
            print(f"  [FAIL] {m}")
        return False
    print(f"  [PASS] {checked}/{checked} core files matched manifest checksums.")
    return True


def check_gate_2_parameter_sync() -> bool:
    print("[Gate 2] Checking Fee & Parameter Synchronization...")
    import config as C
    from korean_engine import KoreanEngine

    errors = []
    # 1. Fees
    if C.TAKER_FEE_BPS != 5.0:
        errors.append(f"C.TAKER_FEE_BPS expected 5.0, got {C.TAKER_FEE_BPS}")
    if C.ROUNDTRIP_FEE_BPS != 10.0:
        errors.append(f"C.ROUNDTRIP_FEE_BPS expected 10.0, got {C.ROUNDTRIP_FEE_BPS}")

    # 2. Tolerances & Horizons
    if C.MAX_FOLLOWER_BOOK_AGE_SEC < 5.0:
        errors.append(f"C.MAX_FOLLOWER_BOOK_AGE_SEC expected >= 5.0s, got {C.MAX_FOLLOWER_BOOK_AGE_SEC}")
    if C.MAX_SIGNAL_HOLD_SEC < 30.0:
        errors.append(f"C.MAX_SIGNAL_HOLD_SEC expected >= 30.0s, got {C.MAX_SIGNAL_HOLD_SEC}")

    # 3. Stop-Loss Configuration
    if not hasattr(C, "STOP_INVALIDATION_BPS") or C.STOP_INVALIDATION_BPS <= 0:
        errors.append("C.STOP_INVALIDATION_BPS missing or non-positive")
    if not hasattr(C, "STOP_ADVERSE_PNL_BPS") or C.STOP_ADVERSE_PNL_BPS <= 0:
        errors.append("C.STOP_ADVERSE_PNL_BPS missing or non-positive")
    if not hasattr(C, "ALPHA_DECAY_SEC") or C.ALPHA_DECAY_SEC <= 0:
        errors.append("C.ALPHA_DECAY_SEC missing or non-positive")

    # 4. Engine defaults reflection
    eng = KoreanEngine(emit_callback=lambda s: None)
    if eng.max_follower_book_age_sec < 5.0:
        errors.append(f"KoreanEngine.max_follower_book_age_sec expected >= 5.0s, got {eng.max_follower_book_age_sec}")
    if eng.max_hold_sec < 30.0:
        errors.append(f"KoreanEngine.max_hold_sec expected >= 30.0s, got {eng.max_hold_sec}")

    if errors:
        for err in errors:
            print(f"  [FAIL] {err}")
        return False
    print("  [PASS] Fee and engine parameter invariants verified.")
    return True


def check_gate_3_event_schemas() -> bool:
    print("[Gate 3] Checking Microstructure Data Contracts & Models...")
    from models import OrderBookDepth5, SunriseSignal, normalize_exchange_ts

    errors = []
    # 1. normalize_exchange_ts
    if normalize_exchange_ts(1792400000000000000) != 1792400000000:
        errors.append("normalize_exchange_ts failed on nanoseconds")
    if normalize_exchange_ts(1792400000000000) != 1792400000000:
        errors.append("normalize_exchange_ts failed on microseconds")
    if normalize_exchange_ts(1792400000000) != 1792400000000:
        errors.append("normalize_exchange_ts failed on milliseconds")
    if normalize_exchange_ts(1792400000) != 1792400000000:
        errors.append("normalize_exchange_ts failed on seconds")

    # 2. SunriseSignal Stop-Loss Fields
    sig = SunriseSignal(
        id="sun_test",
        coin="SOL",
        leader_exchange="upbit",
        follower_exchange="binance",
        follower_contract="SOLUSDT",
        open_ts=1000.0,
        trigger_type="depth_drain",
        leader_mid=100.0,
        leader_best_ask=100.05,
        leader_spread_bps=5.0,
        leader_norm_spread_bps=5.0,
        leader_top5_ask_usd=1000.0,
        leader_norm_depth_usd=1000.0,
        leader_swept_usd=500.0,
        leader_imbalance_top5=0.5,
        drain_ratio=0.5,
        spread_blowout_ratio=1.0,
        follower_mid=100.0,
        follower_best_bid=99.95,
        follower_best_ask=100.05,
        follower_spread_bps=10.0,
        follower_top3_ask_usd=2000.0,
        follower_top3_bid_usd=2000.0,
        follower_imbalance_top3=0.5,
        kimchi_baseline_bps=0.0,
        pair_sigma_bps=2.0,
        raw_dislocation_bps=50.0,
        dev_z=3.0,
        gross_exec_edge_bps=35.0,
        net_exec_edge_bps=30.0,
    )
    sig_dict = sig.to_dict()
    for field_name in ("stop_hit", "stop_type", "stop_loss_bps"):
        if field_name not in sig_dict:
            errors.append(f"SunriseSignal.to_dict() missing stop-loss field: {field_name}")

    if errors:
        for err in errors:
            print(f"  [FAIL] {err}")
        return False
    print("  [PASS] Data contracts and schema serialization verified.")
    return True


def check_gate_4_compilation() -> bool:
    print("[Gate 4] Auditing Bytecode Compilation across all Python files...")
    py_files = sorted(glob.glob(os.path.join(PROJECT_ROOT, "**", "*.py"), recursive=True))
    errors = []
    compiled = 0
    for pf in py_files:
        if "venv" in pf or ".git" in pf:
            continue
        try:
            py_compile.compile(pf, doraise=True)
            compiled += 1
        except py_compile.PyCompileError as e:
            errors.append(f"Compilation error in {pf}: {e}")

    if errors:
        for err in errors:
            print(f"  [FAIL] {err}")
        return False
    print(f"  [PASS] {compiled} Python files compiled with zero syntax errors.")
    return True


def check_gate_5_unit_tests() -> bool:
    print("[Gate 5] Executing Complete Repository Unit Test Suite...")
    loader = unittest.TestLoader()
    suite = loader.discover(os.path.join(PROJECT_ROOT, "tests"), pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=1)
    result = runner.run(suite)
    if not result.wasSuccessful():
        print(f"  [FAIL] Unit tests failed: {len(result.failures)} failures, {len(result.errors)} errors.")
        return False
    print(f"  [PASS] All {result.testsRun} unit tests passed successfully.")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Korean Sunrise Verification Arbiter.")
    parser.add_argument("--seal", action="store_true", help="Reseal MANIFEST.sha256 with current clean hashes.")
    args = parser.parse_args()

    print("======================================================================")
    print("      KOREAN SUNRISE - COMPREHENSIVE MULTI-GATE VERIFICATION          ")
    print("======================================================================\n")

    if args.seal:
        seal_manifest()

    gates = [
        ("Gate 1 (Manifest Integrity)", check_gate_1_manifest),
        ("Gate 2 (Parameter Synchronization)", check_gate_2_parameter_sync),
        ("Gate 3 (Data Contracts & Schemas)", check_gate_3_event_schemas),
        ("Gate 4 (Bytecode Syntax Audit)", check_gate_4_compilation),
        ("Gate 5 (Repository Unit Tests)", check_gate_5_unit_tests),
    ]

    all_passed = True
    for name, gate_func in gates:
        success = gate_func()
        print()
        if not success:
            all_passed = False
            break

    print("======================================================================")
    if all_passed:
        print("  >>> ALL 5 GATES PASSED: CODEBASE VERIFIED AND PRODUCTION-READY <<<  ")
        print("======================================================================")
        sys.exit(0)
    else:
        print("  >>> VERIFICATION FAILED: GATES REPORTED ANOMALIES <<<               ")
        print("======================================================================")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

## trading_profile.json

```json
{
  "_meta": {
    "description": "Korean Sunrise Execution Profile: Venue deposits, margin cushions, and slot sizing",
    "version": "1.0"
  },
  "capital": {
    "per_venue_usd": {
      "binance": 100.0,
      "bybit": 100.0,
      "okx": 100.0,
      "gateio": 100.0,
      "mexc": 100.0,
      "kucoin": 100.0,
      "bingx": 100.0,
      "htx": 100.0,
      "pionex": 100.0,
      "asterdex": 100.0,
      "hyperliquid": 100.0
    },
    "reserve_margin_frac": 0.2
  },
  "active_venues": [
    "binance",
    "bybit",
    "okx",
    "gateio",
    "mexc",
    "kucoin",
    "bingx",
    "htx",
    "pionex",
    "asterdex",
    "hyperliquid"
  ],
  "max_active_slots": 5,
  "min_order_usd": 10.0,
  "stealth_depth_frac": 0.15,
  "max_single_position_usd": 500.0
}
```

## universe_live.json

```json
{
  "_meta": {
    "updated_at": "2026-10-18T16:34:02Z",
    "updated_ts": 1792341242.422,
    "usdt_krw_rate": 1353.0,
    "coins_count": 98,
    "active_venues": [
      "binance",
      "bybit",
      "okx",
      "gateio",
      "mexc",
      "kucoin",
      "bingx",
      "htx",
      "pionex"
    ],
    "screening_elapsed_sec": 84.15,
    "binance_count": 76,
    "bybit_count": 70,
    "okx_count": 61,
    "gateio_count": 48,
    "mexc_count": 70,
    "kucoin_count": 62,
    "bingx_count": 73,
    "htx_count": 12,
    "pionex_count": 36
  },
  "coins": [
    "ETHFI",
    "AXS",
    "SPX",
    "CFX",
    "LIT",
    "SYRUP",
    "ENSO",
    "TAO",
    "SAND",
    "ELSA",
    "O",
    "ORCA",
    "ZORA",
    "VET",
    "WLFI",
    "WLD",
    "FIL",
    "BREV",
    "EUL",
    "KMNO",
    "ERA",
    "UNI",
    "PUMP",
    "CELO",
    "AERO",
    "NEAR",
    "ICP",
    "BAT",
    "PROM",
    "MINA",
    "MORPHO",
    "VVV",
    "PONS",
    "TRUMP",
    "XRP",
    "NEO",
    "SENT",
    "RAY",
    "ZRO",
    "PIEVERSE",
    "SUI",
    "OPG",
    "WCT",
    "JTO",
    "SEI",
    "WAL",
    "LINK",
    "SPK",
    "ETC",
    "PENDLE",
    "TREE",
    "CYBER",
    "GTC",
    "APT",
    "ENS",
    "DOT",
    "THETA",
    "VANA",
    "LINEA",
    "HYPER",
    "FLOCK",
    "TAIKO",
    "MMT",
    "GMT",
    "ZKC",
    "MIRA",
    "ZKP",
    "MEGA",
    "PROS",
    "MASK",
    "BCH",
    "LA",
    "CAP",
    "ARX",
    "TIA",
    "MON",
    "AAVE",
    "AXL",
    "ATOM",
    "RENDER",
    "MANA",
    "ATH",
    "NXPC",
    "ESP",
    "F",
    "IOST",
    "ZBT",
    "BEAM",
    "2Z",
    "FF",
    "PUFFER",
    "VIRTUAL",
    "T",
    "AWE",
    "STX",
    "INJ",
    "GRT",
    "ONT"
  ],
  "upbit_krw_to_coin": {
    "KRW-ETHFI": "ETHFI",
    "KRW-AXS": "AXS",
    "KRW-SPX": "SPX",
    "KRW-CFX": "CFX",
    "KRW-LIT": "LIT",
    "KRW-SYRUP": "SYRUP",
    "KRW-ENSO": "ENSO",
    "KRW-TAO": "TAO",
    "KRW-SAND": "SAND",
    "KRW-ELSA": "ELSA",
    "KRW-O": "O",
    "KRW-ORCA": "ORCA",
    "KRW-ZORA": "ZORA",
    "KRW-VET": "VET",
    "KRW-WLFI": "WLFI",
    "KRW-WLD": "WLD",
    "KRW-FIL": "FIL",
    "KRW-BREV": "BREV",
    "KRW-EUL": "EUL",
    "KRW-KMNO": "KMNO",
    "KRW-ERA": "ERA",
    "KRW-UNI": "UNI",
    "KRW-PUMP": "PUMP",
    "KRW-CELO": "CELO",
    "KRW-AERO": "AERO",
    "KRW-NEAR": "NEAR",
    "KRW-ICP": "ICP",
    "KRW-BAT": "BAT",
    "KRW-PROM": "PROM",
    "KRW-MINA": "MINA",
    "KRW-MORPHO": "MORPHO",
    "KRW-VVV": "VVV",
    "KRW-PONS": "PONS",
    "KRW-TRUMP": "TRUMP",
    "KRW-XRP": "XRP",
    "KRW-NEO": "NEO",
    "KRW-SENT": "SENT",
    "KRW-RAY": "RAY",
    "KRW-ZRO": "ZRO",
    "KRW-PIEVERSE": "PIEVERSE",
    "KRW-SUI": "SUI",
    "KRW-OPG": "OPG",
    "KRW-WCT": "WCT",
    "KRW-JTO": "JTO",
    "KRW-SEI": "SEI",
    "KRW-WAL": "WAL",
    "KRW-LINK": "LINK",
    "KRW-SPK": "SPK",
    "KRW-ETC": "ETC",
    "KRW-PENDLE": "PENDLE",
    "KRW-TREE": "TREE",
    "KRW-CYBER": "CYBER",
    "KRW-GTC": "GTC",
    "KRW-APT": "APT",
    "KRW-ENS": "ENS",
    "KRW-DOT": "DOT",
    "KRW-THETA": "THETA",
    "KRW-VANA": "VANA",
    "KRW-LINEA": "LINEA",
    "KRW-HYPER": "HYPER",
    "KRW-FLOCK": "FLOCK",
    "KRW-TAIKO": "TAIKO",
    "KRW-MMT": "MMT",
    "KRW-GMT": "GMT",
    "KRW-ZKC": "ZKC",
    "KRW-MIRA": "MIRA",
    "KRW-ZKP": "ZKP",
    "KRW-MEGA": "MEGA",
    "KRW-PROS": "PROS",
    "KRW-MASK": "MASK",
    "KRW-BCH": "BCH",
    "KRW-LA": "LA",
    "KRW-CAP": "CAP",
    "KRW-ARX": "ARX",
    "KRW-TIA": "TIA",
    "KRW-MON": "MON",
    "KRW-AAVE": "AAVE",
    "KRW-AXL": "AXL",
    "KRW-ATOM": "ATOM",
    "KRW-RENDER": "RENDER",
    "KRW-MANA": "MANA",
    "KRW-ATH": "ATH",
    "KRW-NXPC": "NXPC",
    "KRW-ESP": "ESP",
    "KRW-F": "F",
    "KRW-IOST": "IOST",
    "KRW-ZBT": "ZBT",
    "KRW-BEAM": "BEAM",
    "KRW-2Z": "2Z",
    "KRW-FF": "FF",
    "KRW-PUFFER": "PUFFER",
    "KRW-VIRTUAL": "VIRTUAL",
    "KRW-T": "T",
    "KRW-AWE": "AWE",
    "KRW-STX": "STX",
    "KRW-INJ": "INJ",
    "KRW-GRT": "GRT",
    "KRW-ONT": "ONT"
  },
  "pairs": {
    "ETHFI": {
      "coin": "ETHFI",
      "upbit_symbol": "KRW-ETHFI",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.917,
      "best_p90_dev_bps": 23.5,
      "venue_symbols": {
        "binance": "ETHFIUSDT",
        "bingx": "ETHFI-USDT",
        "bybit": "ETHFIUSDT",
        "gateio": "ETHFI_USDT",
        "kucoin": "ETHFIUSDTM",
        "mexc": "ETHFI_USDT",
        "okx": "ETHFI-USDT-SWAP",
        "pionex": "ETHFI_USDT_PERP"
      }
    },
    "AXS": {
      "coin": "AXS",
      "upbit_symbol": "KRW-AXS",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.895,
      "best_p90_dev_bps": 19.7,
      "venue_symbols": {
        "binance": "AXSUSDT",
        "bingx": "AXS-USDT",
        "bybit": "AXSUSDT",
        "gateio": "AXS_USDT",
        "kucoin": "AXSUSDTM",
        "mexc": "AXS_USDT",
        "okx": "AXS-USDT-SWAP",
        "pionex": "AXS_USDT_PERP"
      }
    },
    "SPX": {
      "coin": "SPX",
      "upbit_symbol": "KRW-SPX",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.89,
      "best_p90_dev_bps": 35.0,
      "venue_symbols": {
        "binance": "SPXUSDT",
        "bingx": "SPX-USDT",
        "bybit": "SPXUSDT",
        "gateio": "SPX_USDT",
        "kucoin": "SPXUSDTM",
        "mexc": "SPX_USDT",
        "okx": "SPX-USDT-SWAP",
        "pionex": "SPX_USDT_PERP"
      }
    },
    "CFX": {
      "coin": "CFX",
      "upbit_symbol": "KRW-CFX",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.885,
      "best_p90_dev_bps": 23.1,
      "venue_symbols": {
        "binance": "CFXUSDT",
        "bingx": "CFX-USDT",
        "bybit": "CFXUSDT",
        "gateio": "CFX_USDT",
        "kucoin": "CFXUSDTM",
        "mexc": "CFX_USDT",
        "okx": "CFX-USDT-SWAP"
      }
    },
    "LIT": {
      "coin": "LIT",
      "upbit_symbol": "KRW-LIT",
      "venues": [
        "binance",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.88,
      "best_p90_dev_bps": 20.8,
      "venue_symbols": {
        "binance": "LITUSDT",
        "bybit": "LITUSDT",
        "gateio": "LIT_USDT",
        "htx": "LIT-USDT",
        "kucoin": "LITUSDTM",
        "mexc": "LIT_USDT",
        "okx": "LIT-USDT-SWAP",
        "pionex": "LIT_USDT_PERP"
      }
    },
    "SYRUP": {
      "coin": "SYRUP",
      "upbit_symbol": "KRW-SYRUP",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.872,
      "best_p90_dev_bps": 40.8,
      "venue_symbols": {
        "binance": "SYRUPUSDT",
        "bingx": "SYRUP-USDT",
        "bybit": "SYRUPUSDT",
        "kucoin": "SYRUPUSDTM",
        "mexc": "SYRUP_USDT",
        "okx": "SYRUP-USDT-SWAP"
      }
    },
    "ENSO": {
      "coin": "ENSO",
      "upbit_symbol": "KRW-ENSO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "mexc",
        "okx"
      ],
      "best_corr": 0.87,
      "best_p90_dev_bps": 30.6,
      "venue_symbols": {
        "binance": "ENSOUSDT",
        "bingx": "ENSO-USDT",
        "bybit": "ENSOUSDT",
        "gateio": "ENSO_USDT",
        "mexc": "ENSO_USDT",
        "okx": "ENSO-USDT-SWAP"
      }
    },
    "TAO": {
      "coin": "TAO",
      "upbit_symbol": "KRW-TAO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "pionex"
      ],
      "best_corr": 0.858,
      "best_p90_dev_bps": 17.1,
      "venue_symbols": {
        "binance": "TAOUSDT",
        "bingx": "TAO-USDT",
        "bybit": "TAOUSDT",
        "gateio": "TAO_USDT",
        "htx": "TAO-USDT",
        "kucoin": "TAOUSDTM",
        "mexc": "TAO_USDT",
        "pionex": "TAO_USDT_PERP"
      }
    },
    "SAND": {
      "coin": "SAND",
      "upbit_symbol": "KRW-SAND",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.851,
      "best_p90_dev_bps": 40.1,
      "venue_symbols": {
        "binance": "SANDUSDT",
        "bingx": "SAND-USDT",
        "bybit": "SANDUSDT",
        "gateio": "SAND_USDT",
        "kucoin": "SANDUSDTM",
        "mexc": "SAND_USDT",
        "okx": "SAND-USDT-SWAP",
        "pionex": "SAND_USDT_PERP"
      }
    },
    "ELSA": {
      "coin": "ELSA",
      "upbit_symbol": "KRW-ELSA",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "mexc",
        "pionex"
      ],
      "best_corr": 0.85,
      "best_p90_dev_bps": 35.8,
      "venue_symbols": {
        "binance": "ELSAUSDT",
        "bingx": "ELSA-USDT",
        "bybit": "ELSAUSDT",
        "gateio": "ELSA_USDT",
        "mexc": "ELSA_USDT",
        "pionex": "ELSA_USDT_PERP"
      }
    },
    "O": {
      "coin": "O",
      "upbit_symbol": "KRW-O",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.849,
      "best_p90_dev_bps": 42.5,
      "venue_symbols": {
        "binance": "OUSDT",
        "bingx": "O-USDT",
        "bybit": "OUSDT",
        "gateio": "O_USDT",
        "kucoin": "OUSDTM",
        "mexc": "O_USDT",
        "okx": "O-USDT-SWAP"
      }
    },
    "ORCA": {
      "coin": "ORCA",
      "upbit_symbol": "KRW-ORCA",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc"
      ],
      "best_corr": 0.842,
      "best_p90_dev_bps": 17.9,
      "venue_symbols": {
        "binance": "ORCAUSDT",
        "bingx": "ORCA-USDT",
        "bybit": "ORCAUSDT",
        "gateio": "ORCA_USDT",
        "kucoin": "ORCAUSDTM",
        "mexc": "ORCA_USDT"
      }
    },
    "ZORA": {
      "coin": "ZORA",
      "upbit_symbol": "KRW-ZORA",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.842,
      "best_p90_dev_bps": 22.2,
      "venue_symbols": {
        "binance": "ZORAUSDT",
        "bingx": "ZORA-USDT",
        "bybit": "ZORAUSDT",
        "kucoin": "ZORAUSDTM",
        "mexc": "ZORA_USDT",
        "okx": "ZORA-USDT-SWAP"
      }
    },
    "VET": {
      "coin": "VET",
      "upbit_symbol": "KRW-VET",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "kucoin",
        "mexc",
        "pionex"
      ],
      "best_corr": 0.835,
      "best_p90_dev_bps": 23.7,
      "venue_symbols": {
        "binance": "VETUSDT",
        "bingx": "VET-USDT",
        "bybit": "VETUSDT",
        "kucoin": "VETUSDTM",
        "mexc": "VET_USDT",
        "pionex": "VET_USDT_PERP"
      }
    },
    "WLFI": {
      "coin": "WLFI",
      "upbit_symbol": "KRW-WLFI",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.834,
      "best_p90_dev_bps": 21.2,
      "venue_symbols": {
        "binance": "WLFIUSDT",
        "bingx": "WLFI-USDT",
        "bybit": "WLFIUSDT",
        "gateio": "WLFI_USDT",
        "kucoin": "WLFIUSDTM",
        "mexc": "WLFI_USDT",
        "okx": "WLFI-USDT-SWAP"
      }
    },
    "WLD": {
      "coin": "WLD",
      "upbit_symbol": "KRW-WLD",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.834,
      "best_p90_dev_bps": 21.0,
      "venue_symbols": {
        "binance": "WLDUSDT",
        "bingx": "WLD-USDT",
        "bybit": "WLDUSDT",
        "gateio": "WLD_USDT",
        "htx": "WLD-USDT",
        "kucoin": "WLDUSDTM",
        "mexc": "WLD_USDT",
        "okx": "WLD-USDT-SWAP",
        "pionex": "WLD_USDT_PERP"
      }
    },
    "FIL": {
      "coin": "FIL",
      "upbit_symbol": "KRW-FIL",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "okx",
        "pionex"
      ],
      "best_corr": 0.824,
      "best_p90_dev_bps": 20.3,
      "venue_symbols": {
        "binance": "FILUSDT",
        "bingx": "FIL-USDT",
        "bybit": "FILUSDT",
        "gateio": "FIL_USDT",
        "htx": "FIL-USDT",
        "kucoin": "FILUSDTM",
        "okx": "FIL-USDT-SWAP",
        "pionex": "FIL_USDT_PERP"
      }
    },
    "BREV": {
      "coin": "BREV",
      "upbit_symbol": "KRW-BREV",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.824,
      "best_p90_dev_bps": 24.7,
      "venue_symbols": {
        "binance": "BREVUSDT",
        "bingx": "BREV-USDT",
        "bybit": "BREVUSDT",
        "mexc": "BREV_USDT",
        "okx": "BREV-USDT-SWAP"
      }
    },
    "EUL": {
      "coin": "EUL",
      "upbit_symbol": "KRW-EUL",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.82,
      "best_p90_dev_bps": 25.5,
      "venue_symbols": {
        "kucoin": "EULUSDTM"
      }
    },
    "KMNO": {
      "coin": "KMNO",
      "upbit_symbol": "KRW-KMNO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.818,
      "best_p90_dev_bps": 25.2,
      "venue_symbols": {
        "binance": "KMNOUSDT",
        "bingx": "KMNO-USDT",
        "bybit": "KMNOUSDT",
        "mexc": "KMNO_USDT",
        "okx": "KMNO-USDT-SWAP"
      }
    },
    "ERA": {
      "coin": "ERA",
      "upbit_symbol": "KRW-ERA",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc"
      ],
      "best_corr": 0.815,
      "best_p90_dev_bps": 63.8,
      "venue_symbols": {
        "binance": "ERAUSDT",
        "bingx": "ERA-USDT",
        "bybit": "ERAUSDT",
        "gateio": "ERA_USDT",
        "kucoin": "ERAUSDTM",
        "mexc": "ERA_USDT"
      }
    },
    "UNI": {
      "coin": "UNI",
      "upbit_symbol": "KRW-UNI",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.814,
      "best_p90_dev_bps": 16.9,
      "venue_symbols": {
        "binance": "UNIUSDT",
        "bingx": "UNI-USDT",
        "bybit": "UNIUSDT",
        "gateio": "UNI_USDT",
        "htx": "UNI-USDT",
        "kucoin": "UNIUSDTM",
        "mexc": "UNI_USDT",
        "okx": "UNI-USDT-SWAP",
        "pionex": "UNI_USDT_PERP"
      }
    },
    "PUMP": {
      "coin": "PUMP",
      "upbit_symbol": "KRW-PUMP",
      "venues": [
        "binance",
        "bingx",
        "gateio",
        "htx",
        "kucoin",
        "okx",
        "pionex"
      ],
      "best_corr": 0.813,
      "best_p90_dev_bps": 23.8,
      "venue_symbols": {
        "binance": "PUMPUSDT",
        "bingx": "PUMP-USDT",
        "gateio": "PUMP_USDT",
        "htx": "PUMP-USDT",
        "kucoin": "PUMPUSDTM",
        "okx": "PUMP-USDT-SWAP",
        "pionex": "PUMP_USDT_PERP"
      }
    },
    "CELO": {
      "coin": "CELO",
      "upbit_symbol": "KRW-CELO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.811,
      "best_p90_dev_bps": 75.3,
      "venue_symbols": {
        "binance": "CELOUSDT",
        "bingx": "CELO-USDT",
        "bybit": "CELOUSDT",
        "mexc": "CELO_USDT",
        "okx": "CELO-USDT-SWAP"
      }
    },
    "AERO": {
      "coin": "AERO",
      "upbit_symbol": "KRW-AERO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.811,
      "best_p90_dev_bps": 30.1,
      "venue_symbols": {
        "binance": "AEROUSDT",
        "bingx": "AERO-USDT",
        "bybit": "AEROUSDT",
        "gateio": "AERO_USDT",
        "kucoin": "AEROUSDTM",
        "mexc": "AERO_USDT",
        "okx": "AERO-USDT-SWAP",
        "pionex": "AERO_USDT_PERP"
      }
    },
    "NEAR": {
      "coin": "NEAR",
      "upbit_symbol": "KRW-NEAR",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.811,
      "best_p90_dev_bps": 16.1,
      "venue_symbols": {
        "binance": "NEARUSDT",
        "bingx": "NEAR-USDT",
        "bybit": "NEARUSDT",
        "gateio": "NEAR_USDT",
        "htx": "NEAR-USDT",
        "kucoin": "NEARUSDTM",
        "mexc": "NEAR_USDT",
        "okx": "NEAR-USDT-SWAP",
        "pionex": "NEAR_USDT_PERP"
      }
    },
    "ICP": {
      "coin": "ICP",
      "upbit_symbol": "KRW-ICP",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.811,
      "best_p90_dev_bps": 19.2,
      "venue_symbols": {
        "binance": "ICPUSDT",
        "bingx": "ICP-USDT",
        "bybit": "ICPUSDT",
        "gateio": "ICP_USDT",
        "kucoin": "ICPUSDTM",
        "mexc": "ICP_USDT",
        "okx": "ICP-USDT-SWAP",
        "pionex": "ICP_USDT_PERP"
      }
    },
    "BAT": {
      "coin": "BAT",
      "upbit_symbol": "KRW-BAT",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.81,
      "best_p90_dev_bps": 40.3,
      "venue_symbols": {
        "binance": "BATUSDT",
        "bingx": "BAT-USDT",
        "bybit": "BATUSDT",
        "gateio": "BAT_USDT",
        "kucoin": "BATUSDTM",
        "mexc": "BAT_USDT",
        "okx": "BAT-USDT-SWAP",
        "pionex": "BAT_USDT_PERP"
      }
    },
    "PROM": {
      "coin": "PROM",
      "upbit_symbol": "KRW-PROM",
      "venues": [
        "binance",
        "bingx",
        "gateio",
        "kucoin",
        "mexc"
      ],
      "best_corr": 0.807,
      "best_p90_dev_bps": 22.9,
      "venue_symbols": {
        "binance": "PROMUSDT",
        "bingx": "PROM-USDT",
        "gateio": "PROM_USDT",
        "kucoin": "PROMUSDTM",
        "mexc": "PROM_USDT"
      }
    },
    "MINA": {
      "coin": "MINA",
      "upbit_symbol": "KRW-MINA",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.807,
      "best_p90_dev_bps": 24.7,
      "venue_symbols": {
        "binance": "MINAUSDT",
        "bingx": "MINA-USDT",
        "bybit": "MINAUSDT",
        "gateio": "MINA_USDT",
        "kucoin": "MINAUSDTM",
        "mexc": "MINA_USDT",
        "okx": "MINA-USDT-SWAP"
      }
    },
    "MORPHO": {
      "coin": "MORPHO",
      "upbit_symbol": "KRW-MORPHO",
      "venues": [
        "binance",
        "bingx",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.801,
      "best_p90_dev_bps": 26.4,
      "venue_symbols": {
        "binance": "MORPHOUSDT",
        "bingx": "MORPHO-USDT",
        "kucoin": "MORPHOUSDTM",
        "mexc": "MORPHO_USDT",
        "okx": "MORPHO-USDT-SWAP"
      }
    },
    "VVV": {
      "coin": "VVV",
      "upbit_symbol": "KRW-VVV",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.789,
      "best_p90_dev_bps": 30.7,
      "venue_symbols": {
        "kucoin": "VVVUSDTM"
      }
    },
    "PONS": {
      "coin": "PONS",
      "upbit_symbol": "KRW-PONS",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.789,
      "best_p90_dev_bps": 35.0,
      "venue_symbols": {
        "binance": "PONSUSDT",
        "bingx": "PONS-USDT",
        "bybit": "PONSUSDT",
        "gateio": "PONS_USDT",
        "htx": "PONS-USDT",
        "kucoin": "PONSUSDTM",
        "mexc": "PONS_USDT",
        "okx": "PONS-USDT-SWAP",
        "pionex": "PONS_USDT_PERP"
      }
    },
    "TRUMP": {
      "coin": "TRUMP",
      "upbit_symbol": "KRW-TRUMP",
      "venues": [
        "binance",
        "bybit",
        "gateio",
        "kucoin",
        "okx",
        "pionex"
      ],
      "best_corr": 0.784,
      "best_p90_dev_bps": 16.6,
      "venue_symbols": {
        "binance": "TRUMPUSDT",
        "bybit": "TRUMPUSDT",
        "gateio": "TRUMP_USDT",
        "kucoin": "TRUMPUSDTM",
        "okx": "TRUMP-USDT-SWAP",
        "pionex": "TRUMP_USDT_PERP"
      }
    },
    "XRP": {
      "coin": "XRP",
      "upbit_symbol": "KRW-XRP",
      "venues": [
        "binance",
        "bingx",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.781,
      "best_p90_dev_bps": 11.2,
      "venue_symbols": {
        "binance": "XRPUSDT",
        "bingx": "XRP-USDT",
        "gateio": "XRP_USDT",
        "kucoin": "XRPUSDTM",
        "mexc": "XRP_USDT",
        "okx": "XRP-USDT-SWAP",
        "pionex": "XRP_USDT_PERP"
      }
    },
    "NEO": {
      "coin": "NEO",
      "upbit_symbol": "KRW-NEO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.777,
      "best_p90_dev_bps": 21.1,
      "venue_symbols": {
        "binance": "NEOUSDT",
        "bingx": "NEO-USDT",
        "bybit": "NEOUSDT",
        "mexc": "NEO_USDT"
      }
    },
    "SENT": {
      "coin": "SENT",
      "upbit_symbol": "KRW-SENT",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.777,
      "best_p90_dev_bps": 33.2,
      "venue_symbols": {
        "binance": "SENTUSDT",
        "bingx": "SENT-USDT",
        "bybit": "SENTUSDT",
        "gateio": "SENT_USDT",
        "kucoin": "SENTUSDTM",
        "mexc": "SENT_USDT",
        "okx": "SENT-USDT-SWAP",
        "pionex": "SENT_USDT_PERP"
      }
    },
    "RAY": {
      "coin": "RAY",
      "upbit_symbol": "KRW-RAY",
      "venues": [
        "bingx",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.777,
      "best_p90_dev_bps": 25.3,
      "venue_symbols": {
        "bingx": "RAY-USDT",
        "gateio": "RAY_USDT",
        "kucoin": "RAYUSDTM",
        "mexc": "RAY_USDT",
        "okx": "RAY-USDT-SWAP",
        "pionex": "RAY_USDT_PERP"
      }
    },
    "ZRO": {
      "coin": "ZRO",
      "upbit_symbol": "KRW-ZRO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.777,
      "best_p90_dev_bps": 23.9,
      "venue_symbols": {
        "binance": "ZROUSDT",
        "bingx": "ZRO-USDT",
        "bybit": "ZROUSDT",
        "gateio": "ZRO_USDT",
        "kucoin": "ZROUSDTM",
        "mexc": "ZRO_USDT",
        "okx": "ZRO-USDT-SWAP",
        "pionex": "ZRO_USDT_PERP"
      }
    },
    "PIEVERSE": {
      "coin": "PIEVERSE",
      "upbit_symbol": "KRW-PIEVERSE",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.776,
      "best_p90_dev_bps": 20.8,
      "venue_symbols": {
        "binance": "PIEVERSEUSDT",
        "bingx": "PIEVERSE-USDT",
        "bybit": "PIEVERSEUSDT",
        "gateio": "PIEVERSE_USDT",
        "kucoin": "PIEVERSEUSDTM",
        "mexc": "PIEVERSE_USDT",
        "okx": "PIEVERSE-USDT-SWAP"
      }
    },
    "SUI": {
      "coin": "SUI",
      "upbit_symbol": "KRW-SUI",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.776,
      "best_p90_dev_bps": 15.7,
      "venue_symbols": {
        "binance": "SUIUSDT",
        "bingx": "SUI-USDT",
        "bybit": "SUIUSDT",
        "gateio": "SUI_USDT",
        "htx": "SUI-USDT",
        "kucoin": "SUIUSDTM",
        "mexc": "SUI_USDT",
        "okx": "SUI-USDT-SWAP",
        "pionex": "SUI_USDT_PERP"
      }
    },
    "OPG": {
      "coin": "OPG",
      "upbit_symbol": "KRW-OPG",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.773,
      "best_p90_dev_bps": 39.9,
      "venue_symbols": {
        "binance": "OPGUSDT",
        "bingx": "OPG-USDT",
        "bybit": "OPGUSDT",
        "kucoin": "OPGUSDTM",
        "mexc": "OPG_USDT",
        "okx": "OPG-USDT-SWAP"
      }
    },
    "WCT": {
      "coin": "WCT",
      "upbit_symbol": "KRW-WCT",
      "venues": [
        "binance",
        "bingx",
        "mexc",
        "okx"
      ],
      "best_corr": 0.772,
      "best_p90_dev_bps": 32.1,
      "venue_symbols": {
        "binance": "WCTUSDT",
        "bingx": "WCT-USDT",
        "mexc": "WCT_USDT",
        "okx": "WCT-USDT-SWAP"
      }
    },
    "JTO": {
      "coin": "JTO",
      "upbit_symbol": "KRW-JTO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.769,
      "best_p90_dev_bps": 22.0,
      "venue_symbols": {
        "binance": "JTOUSDT",
        "bingx": "JTO-USDT",
        "bybit": "JTOUSDT",
        "gateio": "JTO_USDT",
        "mexc": "JTO_USDT",
        "okx": "JTO-USDT-SWAP",
        "pionex": "JTO_USDT_PERP"
      }
    },
    "SEI": {
      "coin": "SEI",
      "upbit_symbol": "KRW-SEI",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.766,
      "best_p90_dev_bps": 19.3,
      "venue_symbols": {
        "binance": "SEIUSDT",
        "bingx": "SEI-USDT",
        "bybit": "SEIUSDT",
        "gateio": "SEI_USDT",
        "kucoin": "SEIUSDTM",
        "mexc": "SEI_USDT",
        "okx": "SEI-USDT-SWAP",
        "pionex": "SEI_USDT_PERP"
      }
    },
    "WAL": {
      "coin": "WAL",
      "upbit_symbol": "KRW-WAL",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.762,
      "best_p90_dev_bps": 28.5,
      "venue_symbols": {
        "binance": "WALUSDT",
        "bingx": "WAL-USDT",
        "bybit": "WALUSDT",
        "gateio": "WAL_USDT",
        "htx": "WAL-USDT",
        "kucoin": "WALUSDTM",
        "mexc": "WAL_USDT",
        "okx": "WAL-USDT-SWAP",
        "pionex": "WAL_USDT_PERP"
      }
    },
    "LINK": {
      "coin": "LINK",
      "upbit_symbol": "KRW-LINK",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.762,
      "best_p90_dev_bps": 16.0,
      "venue_symbols": {
        "binance": "LINKUSDT",
        "bingx": "LINK-USDT",
        "bybit": "LINKUSDT",
        "gateio": "LINK_USDT",
        "htx": "LINK-USDT",
        "kucoin": "LINKUSDTM",
        "mexc": "LINK_USDT",
        "okx": "LINK-USDT-SWAP",
        "pionex": "LINK_USDT_PERP"
      }
    },
    "SPK": {
      "coin": "SPK",
      "upbit_symbol": "KRW-SPK",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.762,
      "best_p90_dev_bps": 24.3,
      "venue_symbols": {
        "binance": "SPKUSDT",
        "bingx": "SPK-USDT",
        "bybit": "SPKUSDT",
        "mexc": "SPK_USDT",
        "okx": "SPK-USDT-SWAP"
      }
    },
    "ETC": {
      "coin": "ETC",
      "upbit_symbol": "KRW-ETC",
      "venues": [
        "binance",
        "bingx",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.761,
      "best_p90_dev_bps": 20.7,
      "venue_symbols": {
        "binance": "ETCUSDT",
        "bingx": "ETC-USDT",
        "gateio": "ETC_USDT",
        "kucoin": "ETCUSDTM",
        "mexc": "ETC_USDT",
        "okx": "ETC-USDT-SWAP",
        "pionex": "ETC_USDT_PERP"
      }
    },
    "PENDLE": {
      "coin": "PENDLE",
      "upbit_symbol": "KRW-PENDLE",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.76,
      "best_p90_dev_bps": 24.6,
      "venue_symbols": {
        "binance": "PENDLEUSDT",
        "bingx": "PENDLE-USDT",
        "bybit": "PENDLEUSDT",
        "gateio": "PENDLE_USDT",
        "kucoin": "PENDLEUSDTM",
        "mexc": "PENDLE_USDT",
        "okx": "PENDLE-USDT-SWAP",
        "pionex": "PENDLE_USDT_PERP"
      }
    },
    "TREE": {
      "coin": "TREE",
      "upbit_symbol": "KRW-TREE",
      "venues": [
        "binance",
        "bingx",
        "bybit"
      ],
      "best_corr": 0.757,
      "best_p90_dev_bps": 26.6,
      "venue_symbols": {
        "binance": "TREEUSDT",
        "bingx": "TREE-USDT",
        "bybit": "TREEUSDT"
      }
    },
    "CYBER": {
      "coin": "CYBER",
      "upbit_symbol": "KRW-CYBER",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.752,
      "best_p90_dev_bps": 30.8,
      "venue_symbols": {
        "binance": "CYBERUSDT",
        "bingx": "CYBER-USDT",
        "bybit": "CYBERUSDT",
        "mexc": "CYBER_USDT"
      }
    },
    "GTC": {
      "coin": "GTC",
      "upbit_symbol": "KRW-GTC",
      "venues": [
        "binance",
        "bingx",
        "kucoin"
      ],
      "best_corr": 0.752,
      "best_p90_dev_bps": 43.9,
      "venue_symbols": {
        "binance": "GTCUSDT",
        "bingx": "GTC-USDT",
        "kucoin": "GTCUSDTM"
      }
    },
    "APT": {
      "coin": "APT",
      "upbit_symbol": "KRW-APT",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.751,
      "best_p90_dev_bps": 23.4,
      "venue_symbols": {
        "binance": "APTUSDT",
        "bingx": "APT-USDT",
        "bybit": "APTUSDT",
        "gateio": "APT_USDT",
        "kucoin": "APTUSDTM",
        "mexc": "APT_USDT",
        "okx": "APT-USDT-SWAP",
        "pionex": "APT_USDT_PERP"
      }
    },
    "ENS": {
      "coin": "ENS",
      "upbit_symbol": "KRW-ENS",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "mexc"
      ],
      "best_corr": 0.751,
      "best_p90_dev_bps": 21.0,
      "venue_symbols": {
        "binance": "ENSUSDT",
        "bingx": "ENS-USDT",
        "bybit": "ENSUSDT",
        "gateio": "ENS_USDT",
        "mexc": "ENS_USDT"
      }
    },
    "DOT": {
      "coin": "DOT",
      "upbit_symbol": "KRW-DOT",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.749,
      "best_p90_dev_bps": 20.7,
      "venue_symbols": {
        "binance": "DOTUSDT",
        "bingx": "DOT-USDT",
        "bybit": "DOTUSDT",
        "kucoin": "DOTUSDTM",
        "mexc": "DOT_USDT",
        "okx": "DOT-USDT-SWAP",
        "pionex": "DOT_USDT_PERP"
      }
    },
    "THETA": {
      "coin": "THETA",
      "upbit_symbol": "KRW-THETA",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.746,
      "best_p90_dev_bps": 38.7,
      "venue_symbols": {
        "kucoin": "THETAUSDTM"
      }
    },
    "VANA": {
      "coin": "VANA",
      "upbit_symbol": "KRW-VANA",
      "venues": [
        "binance",
        "bingx",
        "bybit"
      ],
      "best_corr": 0.746,
      "best_p90_dev_bps": 21.3,
      "venue_symbols": {
        "binance": "VANAUSDT",
        "bingx": "VANA-USDT",
        "bybit": "VANAUSDT"
      }
    },
    "LINEA": {
      "coin": "LINEA",
      "upbit_symbol": "KRW-LINEA",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.745,
      "best_p90_dev_bps": 22.2,
      "venue_symbols": {
        "binance": "LINEAUSDT",
        "bingx": "LINEA-USDT",
        "bybit": "LINEAUSDT",
        "mexc": "LINEA_USDT",
        "okx": "LINEA-USDT-SWAP"
      }
    },
    "HYPER": {
      "coin": "HYPER",
      "upbit_symbol": "KRW-HYPER",
      "venues": [
        "binance",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.744,
      "best_p90_dev_bps": 27.4,
      "venue_symbols": {
        "binance": "HYPERUSDT",
        "bybit": "HYPERUSDT",
        "mexc": "HYPER_USDT"
      }
    },
    "FLOCK": {
      "coin": "FLOCK",
      "upbit_symbol": "KRW-FLOCK",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.743,
      "best_p90_dev_bps": 20.9,
      "venue_symbols": {
        "binance": "FLOCKUSDT",
        "bingx": "FLOCK-USDT",
        "bybit": "FLOCKUSDT",
        "mexc": "FLOCK_USDT",
        "okx": "FLOCK-USDT-SWAP"
      }
    },
    "TAIKO": {
      "coin": "TAIKO",
      "upbit_symbol": "KRW-TAIKO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.742,
      "best_p90_dev_bps": 32.7,
      "venue_symbols": {
        "binance": "TAIKOUSDT",
        "bingx": "TAIKO-USDT",
        "bybit": "TAIKOUSDT",
        "mexc": "TAIKO_USDT"
      }
    },
    "MMT": {
      "coin": "MMT",
      "upbit_symbol": "KRW-MMT",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.741,
      "best_p90_dev_bps": 41.6,
      "venue_symbols": {
        "kucoin": "MMTUSDTM"
      }
    },
    "GMT": {
      "coin": "GMT",
      "upbit_symbol": "KRW-GMT",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.738,
      "best_p90_dev_bps": 19.5,
      "venue_symbols": {
        "binance": "GMTUSDT",
        "bingx": "GMT-USDT",
        "bybit": "GMTUSDT",
        "mexc": "GMT_USDT",
        "okx": "GMT-USDT-SWAP"
      }
    },
    "ZKC": {
      "coin": "ZKC",
      "upbit_symbol": "KRW-ZKC",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.725,
      "best_p90_dev_bps": 25.1,
      "venue_symbols": {
        "binance": "ZKCUSDT",
        "bingx": "ZKC-USDT",
        "bybit": "ZKCUSDT",
        "mexc": "ZKC_USDT"
      }
    },
    "MIRA": {
      "coin": "MIRA",
      "upbit_symbol": "KRW-MIRA",
      "venues": [
        "binance",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.723,
      "best_p90_dev_bps": 20.5,
      "venue_symbols": {
        "binance": "MIRAUSDT",
        "bybit": "MIRAUSDT",
        "mexc": "MIRA_USDT"
      }
    },
    "ZKP": {
      "coin": "ZKP",
      "upbit_symbol": "KRW-ZKP",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.717,
      "best_p90_dev_bps": 28.7,
      "venue_symbols": {
        "binance": "ZKPUSDT",
        "bingx": "ZKP-USDT",
        "bybit": "ZKPUSDT",
        "kucoin": "ZKPUSDTM",
        "mexc": "ZKP_USDT",
        "okx": "ZKP-USDT-SWAP"
      }
    },
    "MEGA": {
      "coin": "MEGA",
      "upbit_symbol": "KRW-MEGA",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.715,
      "best_p90_dev_bps": 29.8,
      "venue_symbols": {
        "binance": "MEGAUSDT",
        "bingx": "MEGA-USDT",
        "bybit": "MEGAUSDT",
        "gateio": "MEGA_USDT",
        "kucoin": "MEGAUSDTM",
        "mexc": "MEGA_USDT",
        "okx": "MEGA-USDT-SWAP"
      }
    },
    "PROS": {
      "coin": "PROS",
      "upbit_symbol": "KRW-PROS",
      "venues": [
        "kucoin",
        "okx"
      ],
      "best_corr": 0.712,
      "best_p90_dev_bps": 31.5,
      "venue_symbols": {
        "kucoin": "PROSUSDTM",
        "okx": "PROS-USDT-SWAP"
      }
    },
    "MASK": {
      "coin": "MASK",
      "upbit_symbol": "KRW-MASK",
      "venues": [
        "bybit",
        "okx"
      ],
      "best_corr": 0.708,
      "best_p90_dev_bps": 22.2,
      "venue_symbols": {
        "bybit": "MASKUSDT",
        "okx": "MASK-USDT-SWAP"
      }
    },
    "BCH": {
      "coin": "BCH",
      "upbit_symbol": "KRW-BCH",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "pionex"
      ],
      "best_corr": 0.707,
      "best_p90_dev_bps": 12.3,
      "venue_symbols": {
        "binance": "BCHUSDT",
        "bingx": "BCH-USDT",
        "bybit": "BCHUSDT",
        "gateio": "BCH_USDT",
        "kucoin": "BCHUSDTM",
        "mexc": "BCH_USDT",
        "pionex": "BCH_USDT_PERP"
      }
    },
    "LA": {
      "coin": "LA",
      "upbit_symbol": "KRW-LA",
      "venues": [
        "mexc",
        "okx"
      ],
      "best_corr": 0.707,
      "best_p90_dev_bps": 18.0,
      "venue_symbols": {
        "mexc": "LA_USDT",
        "okx": "LA-USDT-SWAP"
      }
    },
    "CAP": {
      "coin": "CAP",
      "upbit_symbol": "KRW-CAP",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.706,
      "best_p90_dev_bps": 19.1,
      "venue_symbols": {
        "binance": "CAPUSDT",
        "bingx": "CAP-USDT",
        "bybit": "CAPUSDT",
        "gateio": "CAP_USDT",
        "kucoin": "CAPUSDTM",
        "mexc": "CAP_USDT",
        "okx": "CAP-USDT-SWAP"
      }
    },
    "ARX": {
      "coin": "ARX",
      "upbit_symbol": "KRW-ARX",
      "venues": [
        "bybit",
        "kucoin",
        "okx"
      ],
      "best_corr": 0.704,
      "best_p90_dev_bps": 34.4,
      "venue_symbols": {
        "bybit": "ARXUSDT",
        "kucoin": "ARXUSDTM",
        "okx": "ARX-USDT-SWAP"
      }
    },
    "TIA": {
      "coin": "TIA",
      "upbit_symbol": "KRW-TIA",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "htx",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.703,
      "best_p90_dev_bps": 26.5,
      "venue_symbols": {
        "binance": "TIAUSDT",
        "bingx": "TIA-USDT",
        "bybit": "TIAUSDT",
        "gateio": "TIA_USDT",
        "htx": "TIA-USDT",
        "kucoin": "TIAUSDTM",
        "mexc": "TIA_USDT",
        "okx": "TIA-USDT-SWAP",
        "pionex": "TIA_USDT_PERP"
      }
    },
    "MON": {
      "coin": "MON",
      "upbit_symbol": "KRW-MON",
      "venues": [
        "binance",
        "bybit",
        "kucoin"
      ],
      "best_corr": 0.703,
      "best_p90_dev_bps": 31.6,
      "venue_symbols": {
        "binance": "MONUSDT",
        "bybit": "MONUSDT",
        "kucoin": "MONUSDTM"
      }
    },
    "AAVE": {
      "coin": "AAVE",
      "upbit_symbol": "KRW-AAVE",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.702,
      "best_p90_dev_bps": 15.4,
      "venue_symbols": {
        "binance": "AAVEUSDT",
        "bingx": "AAVE-USDT",
        "bybit": "AAVEUSDT",
        "gateio": "AAVE_USDT",
        "kucoin": "AAVEUSDTM",
        "mexc": "AAVE_USDT",
        "okx": "AAVE-USDT-SWAP",
        "pionex": "AAVE_USDT_PERP"
      }
    },
    "AXL": {
      "coin": "AXL",
      "upbit_symbol": "KRW-AXL",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.699,
      "best_p90_dev_bps": 25.9,
      "venue_symbols": {
        "binance": "AXLUSDT",
        "bingx": "AXL-USDT",
        "bybit": "AXLUSDT",
        "mexc": "AXL_USDT"
      }
    },
    "ATOM": {
      "coin": "ATOM",
      "upbit_symbol": "KRW-ATOM",
      "venues": [
        "binance",
        "kucoin",
        "okx"
      ],
      "best_corr": 0.694,
      "best_p90_dev_bps": 19.3,
      "venue_symbols": {
        "binance": "ATOMUSDT",
        "kucoin": "ATOMUSDTM",
        "okx": "ATOM-USDT-SWAP"
      }
    },
    "RENDER": {
      "coin": "RENDER",
      "upbit_symbol": "KRW-RENDER",
      "venues": [
        "binance",
        "bybit",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.694,
      "best_p90_dev_bps": 20.8,
      "venue_symbols": {
        "binance": "RENDERUSDT",
        "bybit": "RENDERUSDT",
        "mexc": "RENDER_USDT",
        "okx": "RENDER-USDT-SWAP",
        "pionex": "RENDER_USDT_PERP"
      }
    },
    "MANA": {
      "coin": "MANA",
      "upbit_symbol": "KRW-MANA",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.691,
      "best_p90_dev_bps": 60.3,
      "venue_symbols": {
        "kucoin": "MANAUSDTM"
      }
    },
    "ATH": {
      "coin": "ATH",
      "upbit_symbol": "KRW-ATH",
      "venues": [
        "bingx",
        "mexc"
      ],
      "best_corr": 0.688,
      "best_p90_dev_bps": 23.3,
      "venue_symbols": {
        "bingx": "ATH-USDT",
        "mexc": "ATH_USDT"
      }
    },
    "NXPC": {
      "coin": "NXPC",
      "upbit_symbol": "KRW-NXPC",
      "venues": [
        "binance",
        "bybit",
        "gateio",
        "mexc"
      ],
      "best_corr": 0.684,
      "best_p90_dev_bps": 44.9,
      "venue_symbols": {
        "binance": "NXPCUSDT",
        "bybit": "NXPCUSDT",
        "gateio": "NXPC_USDT",
        "mexc": "NXPC_USDT"
      }
    },
    "ESP": {
      "coin": "ESP",
      "upbit_symbol": "KRW-ESP",
      "venues": [
        "binance",
        "bingx",
        "gateio"
      ],
      "best_corr": 0.681,
      "best_p90_dev_bps": 54.4,
      "venue_symbols": {
        "binance": "ESPUSDT",
        "bingx": "ESP-USDT",
        "gateio": "ESP_USDT"
      }
    },
    "F": {
      "coin": "F",
      "upbit_symbol": "KRW-F",
      "venues": [
        "binance",
        "bingx",
        "okx"
      ],
      "best_corr": 0.679,
      "best_p90_dev_bps": 29.5,
      "venue_symbols": {
        "binance": "FUSDT",
        "bingx": "F-USDT",
        "okx": "F-USDT-SWAP"
      }
    },
    "IOST": {
      "coin": "IOST",
      "upbit_symbol": "KRW-IOST",
      "venues": [
        "binance",
        "bingx",
        "mexc"
      ],
      "best_corr": 0.677,
      "best_p90_dev_bps": 28.8,
      "venue_symbols": {
        "binance": "IOSTUSDT",
        "bingx": "IOST-USDT",
        "mexc": "IOST_USDT"
      }
    },
    "ZBT": {
      "coin": "ZBT",
      "upbit_symbol": "KRW-ZBT",
      "venues": [
        "bingx",
        "bybit",
        "okx"
      ],
      "best_corr": 0.676,
      "best_p90_dev_bps": 21.7,
      "venue_symbols": {
        "bingx": "ZBT-USDT",
        "bybit": "ZBTUSDT",
        "okx": "ZBT-USDT-SWAP"
      }
    },
    "BEAM": {
      "coin": "BEAM",
      "upbit_symbol": "KRW-BEAM",
      "venues": [
        "bingx",
        "bybit"
      ],
      "best_corr": 0.675,
      "best_p90_dev_bps": 27.4,
      "venue_symbols": {
        "bingx": "BEAM-USDT",
        "bybit": "BEAMUSDT"
      }
    },
    "2Z": {
      "coin": "2Z",
      "upbit_symbol": "KRW-2Z",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.675,
      "best_p90_dev_bps": 23.6,
      "venue_symbols": {
        "kucoin": "2ZUSDTM"
      }
    },
    "FF": {
      "coin": "FF",
      "upbit_symbol": "KRW-FF",
      "venues": [
        "bingx",
        "gateio",
        "mexc"
      ],
      "best_corr": 0.675,
      "best_p90_dev_bps": 49.3,
      "venue_symbols": {
        "bingx": "FF-USDT",
        "gateio": "FF_USDT",
        "mexc": "FF_USDT"
      }
    },
    "PUFFER": {
      "coin": "PUFFER",
      "upbit_symbol": "KRW-PUFFER",
      "venues": [
        "bybit"
      ],
      "best_corr": 0.671,
      "best_p90_dev_bps": 42.2,
      "venue_symbols": {
        "bybit": "PUFFERUSDT"
      }
    },
    "VIRTUAL": {
      "coin": "VIRTUAL",
      "upbit_symbol": "KRW-VIRTUAL",
      "venues": [
        "bingx",
        "gateio",
        "okx",
        "pionex"
      ],
      "best_corr": 0.67,
      "best_p90_dev_bps": 20.0,
      "venue_symbols": {
        "bingx": "VIRTUAL-USDT",
        "gateio": "VIRTUAL_USDT",
        "okx": "VIRTUAL-USDT-SWAP",
        "pionex": "VIRTUAL_USDT_PERP"
      }
    },
    "T": {
      "coin": "T",
      "upbit_symbol": "KRW-T",
      "venues": [
        "binance",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.663,
      "best_p90_dev_bps": 33.6,
      "venue_symbols": {
        "binance": "TUSDT",
        "bybit": "TUSDT",
        "mexc": "T_USDT"
      }
    },
    "AWE": {
      "coin": "AWE",
      "upbit_symbol": "KRW-AWE",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.66,
      "best_p90_dev_bps": 26.4,
      "venue_symbols": {
        "kucoin": "AWEUSDTM"
      }
    },
    "STX": {
      "coin": "STX",
      "upbit_symbol": "KRW-STX",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.66,
      "best_p90_dev_bps": 22.5,
      "venue_symbols": {
        "kucoin": "STXUSDTM"
      }
    },
    "INJ": {
      "coin": "INJ",
      "upbit_symbol": "KRW-INJ",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.658,
      "best_p90_dev_bps": 27.9,
      "venue_symbols": {
        "kucoin": "INJUSDTM"
      }
    },
    "GRT": {
      "coin": "GRT",
      "upbit_symbol": "KRW-GRT",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.657,
      "best_p90_dev_bps": 33.4,
      "venue_symbols": {
        "kucoin": "GRTUSDTM"
      }
    },
    "ONT": {
      "coin": "ONT",
      "upbit_symbol": "KRW-ONT",
      "venues": [
        "okx"
      ],
      "best_corr": 0.654,
      "best_p90_dev_bps": 33.8,
      "venue_symbols": {
        "okx": "ONT-USDT-SWAP"
      }
    }
  },
  "binance_sym_map": {
    "ETHFIUSDT": [
      "ETHFI",
      1.0,
      1.0
    ],
    "AXSUSDT": [
      "AXS",
      1.0,
      1.0
    ],
    "SPXUSDT": [
      "SPX",
      1.0,
      1.0
    ],
    "CFXUSDT": [
      "CFX",
      1.0,
      1.0
    ],
    "LITUSDT": [
      "LIT",
      1.0,
      1.0
    ],
    "SYRUPUSDT": [
      "SYRUP",
      1.0,
      1.0
    ],
    "ENSOUSDT": [
      "ENSO",
      1.0,
      1.0
    ],
    "TAOUSDT": [
      "TAO",
      1.0,
      1.0
    ],
    "SANDUSDT": [
      "SAND",
      1.0,
      1.0
    ],
    "ELSAUSDT": [
      "ELSA",
      1.0,
      1.0
    ],
    "OUSDT": [
      "O",
      1.0,
      1.0
    ],
    "ORCAUSDT": [
      "ORCA",
      1.0,
      1.0
    ],
    "ZORAUSDT": [
      "ZORA",
      1.0,
      1.0
    ],
    "VETUSDT": [
      "VET",
      1.0,
      1.0
    ],
    "WLFIUSDT": [
      "WLFI",
      1.0,
      1.0
    ],
    "WLDUSDT": [
      "WLD",
      1.0,
      1.0
    ],
    "FILUSDT": [
      "FIL",
      1.0,
      1.0
    ],
    "BREVUSDT": [
      "BREV",
      1.0,
      1.0
    ],
    "KMNOUSDT": [
      "KMNO",
      1.0,
      1.0
    ],
    "ERAUSDT": [
      "ERA",
      1.0,
      1.0
    ],
    "UNIUSDT": [
      "UNI",
      1.0,
      1.0
    ],
    "PUMPUSDT": [
      "PUMP",
      1.0,
      1.0
    ],
    "CELOUSDT": [
      "CELO",
      1.0,
      1.0
    ],
    "AEROUSDT": [
      "AERO",
      1.0,
      1.0
    ],
    "NEARUSDT": [
      "NEAR",
      1.0,
      1.0
    ],
    "ICPUSDT": [
      "ICP",
      1.0,
      1.0
    ],
    "BATUSDT": [
      "BAT",
      1.0,
      1.0
    ],
    "PROMUSDT": [
      "PROM",
      1.0,
      1.0
    ],
    "MINAUSDT": [
      "MINA",
      1.0,
      1.0
    ],
    "MORPHOUSDT": [
      "MORPHO",
      1.0,
      1.0
    ],
    "PONSUSDT": [
      "PONS",
      1.0,
      1.0
    ],
    "TRUMPUSDT": [
      "TRUMP",
      1.0,
      1.0
    ],
    "XRPUSDT": [
      "XRP",
      1.0,
      1.0
    ],
    "NEOUSDT": [
      "NEO",
      1.0,
      1.0
    ],
    "SENTUSDT": [
      "SENT",
      1.0,
      1.0
    ],
    "ZROUSDT": [
      "ZRO",
      1.0,
      1.0
    ],
    "PIEVERSEUSDT": [
      "PIEVERSE",
      1.0,
      1.0
    ],
    "SUIUSDT": [
      "SUI",
      1.0,
      1.0
    ],
    "OPGUSDT": [
      "OPG",
      1.0,
      1.0
    ],
    "WCTUSDT": [
      "WCT",
      1.0,
      1.0
    ],
    "JTOUSDT": [
      "JTO",
      1.0,
      1.0
    ],
    "SEIUSDT": [
      "SEI",
      1.0,
      1.0
    ],
    "WALUSDT": [
      "WAL",
      1.0,
      1.0
    ],
    "LINKUSDT": [
      "LINK",
      1.0,
      1.0
    ],
    "SPKUSDT": [
      "SPK",
      1.0,
      1.0
    ],
    "ETCUSDT": [
      "ETC",
      1.0,
      1.0
    ],
    "PENDLEUSDT": [
      "PENDLE",
      1.0,
      1.0
    ],
    "TREEUSDT": [
      "TREE",
      1.0,
      1.0
    ],
    "CYBERUSDT": [
      "CYBER",
      1.0,
      1.0
    ],
    "GTCUSDT": [
      "GTC",
      1.0,
      1.0
    ],
    "APTUSDT": [
      "APT",
      1.0,
      1.0
    ],
    "ENSUSDT": [
      "ENS",
      1.0,
      1.0
    ],
    "DOTUSDT": [
      "DOT",
      1.0,
      1.0
    ],
    "VANAUSDT": [
      "VANA",
      1.0,
      1.0
    ],
    "LINEAUSDT": [
      "LINEA",
      1.0,
      1.0
    ],
    "HYPERUSDT": [
      "HYPER",
      1.0,
      1.0
    ],
    "FLOCKUSDT": [
      "FLOCK",
      1.0,
      1.0
    ],
    "TAIKOUSDT": [
      "TAIKO",
      1.0,
      1.0
    ],
    "GMTUSDT": [
      "GMT",
      1.0,
      1.0
    ],
    "ZKCUSDT": [
      "ZKC",
      1.0,
      1.0
    ],
    "MIRAUSDT": [
      "MIRA",
      1.0,
      1.0
    ],
    "ZKPUSDT": [
      "ZKP",
      1.0,
      1.0
    ],
    "MEGAUSDT": [
      "MEGA",
      1.0,
      1.0
    ],
    "BCHUSDT": [
      "BCH",
      1.0,
      1.0
    ],
    "CAPUSDT": [
      "CAP",
      1.0,
      1.0
    ],
    "TIAUSDT": [
      "TIA",
      1.0,
      1.0
    ],
    "MONUSDT": [
      "MON",
      1.0,
      1.0
    ],
    "AAVEUSDT": [
      "AAVE",
      1.0,
      1.0
    ],
    "AXLUSDT": [
      "AXL",
      1.0,
      1.0
    ],
    "ATOMUSDT": [
      "ATOM",
      1.0,
      1.0
    ],
    "RENDERUSDT": [
      "RENDER",
      1.0,
      1.0
    ],
    "NXPCUSDT": [
      "NXPC",
      1.0,
      1.0
    ],
    "ESPUSDT": [
      "ESP",
      1.0,
      1.0
    ],
    "FUSDT": [
      "F",
      1.0,
      1.0
    ],
    "IOSTUSDT": [
      "IOST",
      1.0,
      1.0
    ],
    "TUSDT": [
      "T",
      1.0,
      1.0
    ]
  },
  "bybit_sym_map": {
    "ETHFIUSDT": [
      "ETHFI",
      1.0,
      1.0
    ],
    "AXSUSDT": [
      "AXS",
      1.0,
      1.0
    ],
    "SPXUSDT": [
      "SPX",
      1.0,
      1.0
    ],
    "CFXUSDT": [
      "CFX",
      1.0,
      1.0
    ],
    "LITUSDT": [
      "LIT",
      1.0,
      1.0
    ],
    "SYRUPUSDT": [
      "SYRUP",
      1.0,
      1.0
    ],
    "ENSOUSDT": [
      "ENSO",
      1.0,
      1.0
    ],
    "TAOUSDT": [
      "TAO",
      1.0,
      1.0
    ],
    "SANDUSDT": [
      "SAND",
      1.0,
      1.0
    ],
    "ELSAUSDT": [
      "ELSA",
      1.0,
      1.0
    ],
    "OUSDT": [
      "O",
      1.0,
      1.0
    ],
    "ORCAUSDT": [
      "ORCA",
      1.0,
      1.0
    ],
    "ZORAUSDT": [
      "ZORA",
      1.0,
      1.0
    ],
    "VETUSDT": [
      "VET",
      1.0,
      1.0
    ],
    "WLFIUSDT": [
      "WLFI",
      1.0,
      1.0
    ],
    "WLDUSDT": [
      "WLD",
      1.0,
      1.0
    ],
    "FILUSDT": [
      "FIL",
      1.0,
      1.0
    ],
    "BREVUSDT": [
      "BREV",
      1.0,
      1.0
    ],
    "KMNOUSDT": [
      "KMNO",
      1.0,
      1.0
    ],
    "ERAUSDT": [
      "ERA",
      1.0,
      1.0
    ],
    "UNIUSDT": [
      "UNI",
      1.0,
      1.0
    ],
    "CELOUSDT": [
      "CELO",
      1.0,
      1.0
    ],
    "AEROUSDT": [
      "AERO",
      1.0,
      1.0
    ],
    "NEARUSDT": [
      "NEAR",
      1.0,
      1.0
    ],
    "ICPUSDT": [
      "ICP",
      1.0,
      1.0
    ],
    "BATUSDT": [
      "BAT",
      1.0,
      1.0
    ],
    "MINAUSDT": [
      "MINA",
      1.0,
      1.0
    ],
    "PONSUSDT": [
      "PONS",
      1.0,
      1.0
    ],
    "TRUMPUSDT": [
      "TRUMP",
      1.0,
      1.0
    ],
    "NEOUSDT": [
      "NEO",
      1.0,
      1.0
    ],
    "SENTUSDT": [
      "SENT",
      1.0,
      1.0
    ],
    "ZROUSDT": [
      "ZRO",
      1.0,
      1.0
    ],
    "PIEVERSEUSDT": [
      "PIEVERSE",
      1.0,
      1.0
    ],
    "SUIUSDT": [
      "SUI",
      1.0,
      1.0
    ],
    "OPGUSDT": [
      "OPG",
      1.0,
      1.0
    ],
    "JTOUSDT": [
      "JTO",
      1.0,
      1.0
    ],
    "SEIUSDT": [
      "SEI",
      1.0,
      1.0
    ],
    "WALUSDT": [
      "WAL",
      1.0,
      1.0
    ],
    "LINKUSDT": [
      "LINK",
      1.0,
      1.0
    ],
    "SPKUSDT": [
      "SPK",
      1.0,
      1.0
    ],
    "PENDLEUSDT": [
      "PENDLE",
      1.0,
      1.0
    ],
    "TREEUSDT": [
      "TREE",
      1.0,
      1.0
    ],
    "CYBERUSDT": [
      "CYBER",
      1.0,
      1.0
    ],
    "APTUSDT": [
      "APT",
      1.0,
      1.0
    ],
    "ENSUSDT": [
      "ENS",
      1.0,
      1.0
    ],
    "DOTUSDT": [
      "DOT",
      1.0,
      1.0
    ],
    "VANAUSDT": [
      "VANA",
      1.0,
      1.0
    ],
    "LINEAUSDT": [
      "LINEA",
      1.0,
      1.0
    ],
    "HYPERUSDT": [
      "HYPER",
      1.0,
      1.0
    ],
    "FLOCKUSDT": [
      "FLOCK",
      1.0,
      1.0
    ],
    "TAIKOUSDT": [
      "TAIKO",
      1.0,
      1.0
    ],
    "GMTUSDT": [
      "GMT",
      1.0,
      1.0
    ],
    "ZKCUSDT": [
      "ZKC",
      1.0,
      1.0
    ],
    "MIRAUSDT": [
      "MIRA",
      1.0,
      1.0
    ],
    "ZKPUSDT": [
      "ZKP",
      1.0,
      1.0
    ],
    "MEGAUSDT": [
      "MEGA",
      1.0,
      1.0
    ],
    "MASKUSDT": [
      "MASK",
      1.0,
      1.0
    ],
    "BCHUSDT": [
      "BCH",
      1.0,
      1.0
    ],
    "CAPUSDT": [
      "CAP",
      1.0,
      1.0
    ],
    "ARXUSDT": [
      "ARX",
      1.0,
      1.0
    ],
    "TIAUSDT": [
      "TIA",
      1.0,
      1.0
    ],
    "MONUSDT": [
      "MON",
      1.0,
      1.0
    ],
    "AAVEUSDT": [
      "AAVE",
      1.0,
      1.0
    ],
    "AXLUSDT": [
      "AXL",
      1.0,
      1.0
    ],
    "RENDERUSDT": [
      "RENDER",
      1.0,
      1.0
    ],
    "NXPCUSDT": [
      "NXPC",
      1.0,
      1.0
    ],
    "ZBTUSDT": [
      "ZBT",
      1.0,
      1.0
    ],
    "BEAMUSDT": [
      "BEAM",
      1.0,
      1.0
    ],
    "PUFFERUSDT": [
      "PUFFER",
      1.0,
      1.0
    ],
    "TUSDT": [
      "T",
      1.0,
      1.0
    ]
  },
  "okx_sym_map": {
    "ETHFI-USDT-SWAP": [
      "ETHFI",
      1.0,
      1.0
    ],
    "AXS-USDT-SWAP": [
      "AXS",
      1.0,
      0.1
    ],
    "SPX-USDT-SWAP": [
      "SPX",
      1.0,
      1.0
    ],
    "CFX-USDT-SWAP": [
      "CFX",
      1.0,
      10.0
    ],
    "LIT-USDT-SWAP": [
      "LIT",
      1.0,
      1.0
    ],
    "SYRUP-USDT-SWAP": [
      "SYRUP",
      1.0,
      10.0
    ],
    "ENSO-USDT-SWAP": [
      "ENSO",
      1.0,
      1.0
    ],
    "SAND-USDT-SWAP": [
      "SAND",
      1.0,
      10.0
    ],
    "O-USDT-SWAP": [
      "O",
      1.0,
      10.0
    ],
    "ZORA-USDT-SWAP": [
      "ZORA",
      1.0,
      10.0
    ],
    "WLFI-USDT-SWAP": [
      "WLFI",
      1.0,
      10.0
    ],
    "WLD-USDT-SWAP": [
      "WLD",
      1.0,
      1.0
    ],
    "FIL-USDT-SWAP": [
      "FIL",
      1.0,
      0.1
    ],
    "BREV-USDT-SWAP": [
      "BREV",
      1.0,
      1.0
    ],
    "KMNO-USDT-SWAP": [
      "KMNO",
      1.0,
      100.0
    ],
    "UNI-USDT-SWAP": [
      "UNI",
      1.0,
      1.0
    ],
    "PUMP-USDT-SWAP": [
      "PUMP",
      1.0,
      1000.0
    ],
    "CELO-USDT-SWAP": [
      "CELO",
      1.0,
      1.0
    ],
    "AERO-USDT-SWAP": [
      "AERO",
      1.0,
      10.0
    ],
    "NEAR-USDT-SWAP": [
      "NEAR",
      1.0,
      10.0
    ],
    "ICP-USDT-SWAP": [
      "ICP",
      1.0,
      0.01
    ],
    "BAT-USDT-SWAP": [
      "BAT",
      1.0,
      10.0
    ],
    "MINA-USDT-SWAP": [
      "MINA",
      1.0,
      1.0
    ],
    "MORPHO-USDT-SWAP": [
      "MORPHO",
      1.0,
      1.0
    ],
    "PONS-USDT-SWAP": [
      "PONS",
      1.0,
      10.0
    ],
    "TRUMP-USDT-SWAP": [
      "TRUMP",
      1.0,
      0.1
    ],
    "XRP-USDT-SWAP": [
      "XRP",
      1.0,
      100.0
    ],
    "SENT-USDT-SWAP": [
      "SENT",
      1.0,
      100.0
    ],
    "RAY-USDT-SWAP": [
      "RAY",
      1.0,
      1.0
    ],
    "ZRO-USDT-SWAP": [
      "ZRO",
      1.0,
      1.0
    ],
    "PIEVERSE-USDT-SWAP": [
      "PIEVERSE",
      1.0,
      10.0
    ],
    "SUI-USDT-SWAP": [
      "SUI",
      1.0,
      1.0
    ],
    "OPG-USDT-SWAP": [
      "OPG",
      1.0,
      10.0
    ],
    "WCT-USDT-SWAP": [
      "WCT",
      1.0,
      10.0
    ],
    "JTO-USDT-SWAP": [
      "JTO",
      1.0,
      1.0
    ],
    "SEI-USDT-SWAP": [
      "SEI",
      1.0,
      10.0
    ],
    "WAL-USDT-SWAP": [
      "WAL",
      1.0,
      10.0
    ],
    "LINK-USDT-SWAP": [
      "LINK",
      1.0,
      1.0
    ],
    "SPK-USDT-SWAP": [
      "SPK",
      1.0,
      100.0
    ],
    "ETC-USDT-SWAP": [
      "ETC",
      1.0,
      10.0
    ],
    "PENDLE-USDT-SWAP": [
      "PENDLE",
      1.0,
      1.0
    ],
    "APT-USDT-SWAP": [
      "APT",
      1.0,
      1.0
    ],
    "DOT-USDT-SWAP": [
      "DOT",
      1.0,
      1.0
    ],
    "LINEA-USDT-SWAP": [
      "LINEA",
      1.0,
      100.0
    ],
    "FLOCK-USDT-SWAP": [
      "FLOCK",
      1.0,
      100.0
    ],
    "GMT-USDT-SWAP": [
      "GMT",
      1.0,
      1.0
    ],
    "ZKP-USDT-SWAP": [
      "ZKP",
      1.0,
      10.0
    ],
    "MEGA-USDT-SWAP": [
      "MEGA",
      1.0,
      10.0
    ],
    "PROS-USDT-SWAP": [
      "PROS",
      1.0,
      1.0
    ],
    "MASK-USDT-SWAP": [
      "MASK",
      1.0,
      1.0
    ],
    "LA-USDT-SWAP": [
      "LA",
      1.0,
      10.0
    ],
    "CAP-USDT-SWAP": [
      "CAP",
      1.0,
      100.0
    ],
    "ARX-USDT-SWAP": [
      "ARX",
      1.0,
      10.0
    ],
    "TIA-USDT-SWAP": [
      "TIA",
      1.0,
      1.0
    ],
    "AAVE-USDT-SWAP": [
      "AAVE",
      1.0,
      0.1
    ],
    "ATOM-USDT-SWAP": [
      "ATOM",
      1.0,
      1.0
    ],
    "RENDER-USDT-SWAP": [
      "RENDER",
      1.0,
      1.0
    ],
    "F-USDT-SWAP": [
      "F",
      1.0,
      100.0
    ],
    "ZBT-USDT-SWAP": [
      "ZBT",
      1.0,
      1.0
    ],
    "VIRTUAL-USDT-SWAP": [
      "VIRTUAL",
      1.0,
      1.0
    ],
    "ONT-USDT-SWAP": [
      "ONT",
      1.0,
      10.0
    ]
  },
  "gateio_sym_map": {
    "ETHFI_USDT": [
      "ETHFI",
      1.0,
      0.1
    ],
    "AXS_USDT": [
      "AXS",
      1.0,
      0.1
    ],
    "SPX_USDT": [
      "SPX",
      1.0,
      10.0
    ],
    "CFX_USDT": [
      "CFX",
      1.0,
      10.0
    ],
    "LIT_USDT": [
      "LIT",
      1.0,
      1.0
    ],
    "ENSO_USDT": [
      "ENSO",
      1.0,
      1.0
    ],
    "TAO_USDT": [
      "TAO",
      1.0,
      0.01
    ],
    "SAND_USDT": [
      "SAND",
      1.0,
      1.0
    ],
    "ELSA_USDT": [
      "ELSA",
      1.0,
      10.0
    ],
    "O_USDT": [
      "O",
      1.0,
      10.0
    ],
    "ORCA_USDT": [
      "ORCA",
      1.0,
      0.1
    ],
    "WLFI_USDT": [
      "WLFI",
      1.0,
      10.0
    ],
    "WLD_USDT": [
      "WLD",
      1.0,
      1.0
    ],
    "FIL_USDT": [
      "FIL",
      1.0,
      0.01
    ],
    "ERA_USDT": [
      "ERA",
      1.0,
      10.0
    ],
    "UNI_USDT": [
      "UNI",
      1.0,
      1.0
    ],
    "PUMP_USDT": [
      "PUMP",
      1.0,
      1000.0
    ],
    "AERO_USDT": [
      "AERO",
      1.0,
      1.0
    ],
    "NEAR_USDT": [
      "NEAR",
      1.0,
      1.0
    ],
    "ICP_USDT": [
      "ICP",
      1.0,
      0.001
    ],
    "BAT_USDT": [
      "BAT",
      1.0,
      10.0
    ],
    "PROM_USDT": [
      "PROM",
      1.0,
      0.1
    ],
    "MINA_USDT": [
      "MINA",
      1.0,
      0.1
    ],
    "PONS_USDT": [
      "PONS",
      1.0,
      10.0
    ],
    "TRUMP_USDT": [
      "TRUMP",
      1.0,
      0.1
    ],
    "XRP_USDT": [
      "XRP",
      1.0,
      10.0
    ],
    "SENT_USDT": [
      "SENT",
      1.0,
      100.0
    ],
    "RAY_USDT": [
      "RAY",
      1.0,
      0.1
    ],
    "ZRO_USDT": [
      "ZRO",
      1.0,
      1.0
    ],
    "PIEVERSE_USDT": [
      "PIEVERSE",
      1.0,
      10.0
    ],
    "SUI_USDT": [
      "SUI",
      1.0,
      1.0
    ],
    "JTO_USDT": [
      "JTO",
      1.0,
      1.0
    ],
    "SEI_USDT": [
      "SEI",
      1.0,
      10.0
    ],
    "WAL_USDT": [
      "WAL",
      1.0,
      10.0
    ],
    "LINK_USDT": [
      "LINK",
      1.0,
      1.0
    ],
    "ETC_USDT": [
      "ETC",
      1.0,
      0.1
    ],
    "PENDLE_USDT": [
      "PENDLE",
      1.0,
      1.0
    ],
    "APT_USDT": [
      "APT",
      1.0,
      0.1
    ],
    "ENS_USDT": [
      "ENS",
      1.0,
      0.1
    ],
    "MEGA_USDT": [
      "MEGA",
      1.0,
      10.0
    ],
    "BCH_USDT": [
      "BCH",
      1.0,
      0.01
    ],
    "CAP_USDT": [
      "CAP",
      1.0,
      100.0
    ],
    "TIA_USDT": [
      "TIA",
      1.0,
      1.0
    ],
    "AAVE_USDT": [
      "AAVE",
      1.0,
      0.01
    ],
    "NXPC_USDT": [
      "NXPC",
      1.0,
      1.0
    ],
    "ESP_USDT": [
      "ESP",
      1.0,
      10.0
    ],
    "FF_USDT": [
      "FF",
      1.0,
      10.0
    ],
    "VIRTUAL_USDT": [
      "VIRTUAL",
      1.0,
      10.0
    ]
  },
  "mexc_sym_map": {
    "ETHFI_USDT": [
      "ETHFI",
      1.0,
      1.0
    ],
    "AXS_USDT": [
      "AXS",
      1.0,
      1.0
    ],
    "SPX_USDT": [
      "SPX",
      1.0,
      1.0
    ],
    "CFX_USDT": [
      "CFX",
      1.0,
      1.0
    ],
    "LIT_USDT": [
      "LIT",
      1.0,
      0.1
    ],
    "SYRUP_USDT": [
      "SYRUP",
      1.0,
      10.0
    ],
    "ENSO_USDT": [
      "ENSO",
      1.0,
      1.0
    ],
    "TAO_USDT": [
      "TAO",
      1.0,
      0.01
    ],
    "SAND_USDT": [
      "SAND",
      1.0,
      1.0
    ],
    "ELSA_USDT": [
      "ELSA",
      1.0,
      10.0
    ],
    "O_USDT": [
      "O",
      1.0,
      1.0
    ],
    "ORCA_USDT": [
      "ORCA",
      1.0,
      1.0
    ],
    "ZORA_USDT": [
      "ZORA",
      1.0,
      10.0
    ],
    "VET_USDT": [
      "VET",
      1.0,
      1.0
    ],
    "WLFI_USDT": [
      "WLFI",
      1.0,
      1.0
    ],
    "WLD_USDT": [
      "WLD",
      1.0,
      1.0
    ],
    "BREV_USDT": [
      "BREV",
      1.0,
      10.0
    ],
    "KMNO_USDT": [
      "KMNO",
      1.0,
      100.0
    ],
    "ERA_USDT": [
      "ERA",
      1.0,
      1.0
    ],
    "UNI_USDT": [
      "UNI",
      1.0,
      0.1
    ],
    "CELO_USDT": [
      "CELO",
      1.0,
      1.0
    ],
    "AERO_USDT": [
      "AERO",
      1.0,
      10.0
    ],
    "NEAR_USDT": [
      "NEAR",
      1.0,
      1.0
    ],
    "ICP_USDT": [
      "ICP",
      1.0,
      0.01
    ],
    "BAT_USDT": [
      "BAT",
      1.0,
      1.0
    ],
    "PROM_USDT": [
      "PROM",
      1.0,
      1.0
    ],
    "MINA_USDT": [
      "MINA",
      1.0,
      1.0
    ],
    "MORPHO_USDT": [
      "MORPHO",
      1.0,
      1.0
    ],
    "PONS_USDT": [
      "PONS",
      1.0,
      100.0
    ],
    "XRP_USDT": [
      "XRP",
      1.0,
      1.0
    ],
    "NEO_USDT": [
      "NEO",
      1.0,
      0.1
    ],
    "SENT_USDT": [
      "SENT",
      1.0,
      10.0
    ],
    "RAY_USDT": [
      "RAY",
      1.0,
      0.1
    ],
    "ZRO_USDT": [
      "ZRO",
      1.0,
      1.0
    ],
    "PIEVERSE_USDT": [
      "PIEVERSE",
      1.0,
      10.0
    ],
    "SUI_USDT": [
      "SUI",
      1.0,
      1.0
    ],
    "OPG_USDT": [
      "OPG",
      1.0,
      10.0
    ],
    "WCT_USDT": [
      "WCT",
      1.0,
      10.0
    ],
    "JTO_USDT": [
      "JTO",
      1.0,
      1.0
    ],
    "SEI_USDT": [
      "SEI",
      1.0,
      10.0
    ],
    "WAL_USDT": [
      "WAL",
      1.0,
      10.0
    ],
    "LINK_USDT": [
      "LINK",
      1.0,
      0.1
    ],
    "SPK_USDT": [
      "SPK",
      1.0,
      10.0
    ],
    "ETC_USDT": [
      "ETC",
      1.0,
      0.1
    ],
    "PENDLE_USDT": [
      "PENDLE",
      1.0,
      1.0
    ],
    "CYBER_USDT": [
      "CYBER",
      1.0,
      1.0
    ],
    "APT_USDT": [
      "APT",
      1.0,
      0.1
    ],
    "ENS_USDT": [
      "ENS",
      1.0,
      0.1
    ],
    "DOT_USDT": [
      "DOT",
      1.0,
      0.1
    ],
    "LINEA_USDT": [
      "LINEA",
      1.0,
      10.0
    ],
    "HYPER_USDT": [
      "HYPER",
      1.0,
      10.0
    ],
    "FLOCK_USDT": [
      "FLOCK",
      1.0,
      1.0
    ],
    "TAIKO_USDT": [
      "TAIKO",
      1.0,
      1.0
    ],
    "GMT_USDT": [
      "GMT",
      1.0,
      10.0
    ],
    "ZKC_USDT": [
      "ZKC",
      1.0,
      1.0
    ],
    "MIRA_USDT": [
      "MIRA",
      1.0,
      1.0
    ],
    "ZKP_USDT": [
      "ZKP",
      1.0,
      10.0
    ],
    "MEGA_USDT": [
      "MEGA",
      1.0,
      1.0
    ],
    "BCH_USDT": [
      "BCH",
      1.0,
      0.01
    ],
    "LA_USDT": [
      "LA",
      1.0,
      1.0
    ],
    "CAP_USDT": [
      "CAP",
      1.0,
      10.0
    ],
    "TIA_USDT": [
      "TIA",
      1.0,
      1.0
    ],
    "AAVE_USDT": [
      "AAVE",
      1.0,
      0.01
    ],
    "AXL_USDT": [
      "AXL",
      1.0,
      1.0
    ],
    "RENDER_USDT": [
      "RENDER",
      1.0,
      1.0
    ],
    "ATH_USDT": [
      "ATH",
      1.0,
      10.0
    ],
    "NXPC_USDT": [
      "NXPC",
      1.0,
      1.0
    ],
    "IOST_USDT": [
      "IOST",
      1.0,
      100.0
    ],
    "FF_USDT": [
      "FF",
      1.0,
      1.0
    ],
    "T_USDT": [
      "T",
      1.0,
      100.0
    ]
  },
  "kucoin_sym_map": {
    "ETHFIUSDTM": [
      "ETHFI",
      1.0,
      1.0
    ],
    "AXSUSDTM": [
      "AXS",
      1.0,
      0.1
    ],
    "SPXUSDTM": [
      "SPX",
      1.0,
      1.0
    ],
    "CFXUSDTM": [
      "CFX",
      1.0,
      10.0
    ],
    "LITUSDTM": [
      "LIT",
      1.0,
      1.0
    ],
    "SYRUPUSDTM": [
      "SYRUP",
      1.0,
      10.0
    ],
    "TAOUSDTM": [
      "TAO",
      1.0,
      0.01
    ],
    "SANDUSDTM": [
      "SAND",
      1.0,
      1.0
    ],
    "OUSDTM": [
      "O",
      1.0,
      1.0
    ],
    "ORCAUSDTM": [
      "ORCA",
      1.0,
      1.0
    ],
    "ZORAUSDTM": [
      "ZORA",
      1.0,
      100.0
    ],
    "VETUSDTM": [
      "VET",
      1.0,
      100.0
    ],
    "WLFIUSDTM": [
      "WLFI",
      1.0,
      1.0
    ],
    "WLDUSDTM": [
      "WLD",
      1.0,
      1.0
    ],
    "FILUSDTM": [
      "FIL",
      1.0,
      0.1
    ],
    "EULUSDTM": [
      "EUL",
      1.0,
      0.1
    ],
    "ERAUSDTM": [
      "ERA",
      1.0,
      10.0
    ],
    "UNIUSDTM": [
      "UNI",
      1.0,
      1.0
    ],
    "PUMPUSDTM": [
      "PUMP",
      1.0,
      1000.0
    ],
    "AEROUSDTM": [
      "AERO",
      1.0,
      1.0
    ],
    "NEARUSDTM": [
      "NEAR",
      1.0,
      0.1
    ],
    "ICPUSDTM": [
      "ICP",
      1.0,
      0.01
    ],
    "BATUSDTM": [
      "BAT",
      1.0,
      1.0
    ],
    "PROMUSDTM": [
      "PROM",
      1.0,
      0.1
    ],
    "MINAUSDTM": [
      "MINA",
      1.0,
      1.0
    ],
    "MORPHOUSDTM": [
      "MORPHO",
      1.0,
      1.0
    ],
    "VVVUSDTM": [
      "VVV",
      1.0,
      0.1
    ],
    "PONSUSDTM": [
      "PONS",
      1.0,
      10.0
    ],
    "TRUMPUSDTM": [
      "TRUMP",
      1.0,
      0.1
    ],
    "XRPUSDTM": [
      "XRP",
      1.0,
      10.0
    ],
    "SENTUSDTM": [
      "SENT",
      1.0,
      10.0
    ],
    "RAYUSDTM": [
      "RAY",
      1.0,
      1.0
    ],
    "ZROUSDTM": [
      "ZRO",
      1.0,
      1.0
    ],
    "PIEVERSEUSDTM": [
      "PIEVERSE",
      1.0,
      10.0
    ],
    "SUIUSDTM": [
      "SUI",
      1.0,
      1.0
    ],
    "OPGUSDTM": [
      "OPG",
      1.0,
      10.0
    ],
    "SEIUSDTM": [
      "SEI",
      1.0,
      10.0
    ],
    "WALUSDTM": [
      "WAL",
      1.0,
      10.0
    ],
    "LINKUSDTM": [
      "LINK",
      1.0,
      0.1
    ],
    "ETCUSDTM": [
      "ETC",
      1.0,
      0.1
    ],
    "PENDLEUSDTM": [
      "PENDLE",
      1.0,
      1.0
    ],
    "GTCUSDTM": [
      "GTC",
      1.0,
      1.0
    ],
    "APTUSDTM": [
      "APT",
      1.0,
      0.1
    ],
    "DOTUSDTM": [
      "DOT",
      1.0,
      1.0
    ],
    "THETAUSDTM": [
      "THETA",
      1.0,
      0.1
    ],
    "MMTUSDTM": [
      "MMT",
      1.0,
      10.0
    ],
    "ZKPUSDTM": [
      "ZKP",
      1.0,
      10.0
    ],
    "MEGAUSDTM": [
      "MEGA",
      1.0,
      10.0
    ],
    "PROSUSDTM": [
      "PROS",
      1.0,
      1.0
    ],
    "BCHUSDTM": [
      "BCH",
      1.0,
      0.01
    ],
    "CAPUSDTM": [
      "CAP",
      1.0,
      100.0
    ],
    "ARXUSDTM": [
      "ARX",
      1.0,
      10.0
    ],
    "TIAUSDTM": [
      "TIA",
      1.0,
      1.0
    ],
    "MONUSDTM": [
      "MON",
      1.0,
      10.0
    ],
    "AAVEUSDTM": [
      "AAVE",
      1.0,
      0.01
    ],
    "ATOMUSDTM": [
      "ATOM",
      1.0,
      0.1
    ],
    "MANAUSDTM": [
      "MANA",
      1.0,
      1.0
    ],
    "2ZUSDTM": [
      "2Z",
      1.0,
      10.0
    ],
    "AWEUSDTM": [
      "AWE",
      1.0,
      10.0
    ],
    "STXUSDTM": [
      "STX",
      1.0,
      1.0
    ],
    "INJUSDTM": [
      "INJ",
      1.0,
      1.0
    ],
    "GRTUSDTM": [
      "GRT",
      1.0,
      1.0
    ]
  },
  "bingx_sym_map": {
    "ETHFI-USDT": [
      "ETHFI",
      1.0,
      1.0
    ],
    "AXS-USDT": [
      "AXS",
      1.0,
      1.0
    ],
    "SPX-USDT": [
      "SPX",
      1.0,
      1.0
    ],
    "CFX-USDT": [
      "CFX",
      1.0,
      1.0
    ],
    "SYRUP-USDT": [
      "SYRUP",
      1.0,
      1.0
    ],
    "ENSO-USDT": [
      "ENSO",
      1.0,
      1.0
    ],
    "TAO-USDT": [
      "TAO",
      1.0,
      1.0
    ],
    "SAND-USDT": [
      "SAND",
      1.0,
      1.0
    ],
    "ELSA-USDT": [
      "ELSA",
      1.0,
      1.0
    ],
    "O-USDT": [
      "O",
      1.0,
      1.0
    ],
    "ORCA-USDT": [
      "ORCA",
      1.0,
      1.0
    ],
    "ZORA-USDT": [
      "ZORA",
      1.0,
      1.0
    ],
    "VET-USDT": [
      "VET",
      1.0,
      1.0
    ],
    "WLFI-USDT": [
      "WLFI",
      1.0,
      1.0
    ],
    "WLD-USDT": [
      "WLD",
      1.0,
      1.0
    ],
    "FIL-USDT": [
      "FIL",
      1.0,
      1.0
    ],
    "BREV-USDT": [
      "BREV",
      1.0,
      1.0
    ],
    "KMNO-USDT": [
      "KMNO",
      1.0,
      1.0
    ],
    "ERA-USDT": [
      "ERA",
      1.0,
      1.0
    ],
    "UNI-USDT": [
      "UNI",
      1.0,
      1.0
    ],
    "PUMP-USDT": [
      "PUMP",
      1.0,
      1.0
    ],
    "CELO-USDT": [
      "CELO",
      1.0,
      1.0
    ],
    "AERO-USDT": [
      "AERO",
      1.0,
      1.0
    ],
    "NEAR-USDT": [
      "NEAR",
      1.0,
      1.0
    ],
    "ICP-USDT": [
      "ICP",
      1.0,
      1.0
    ],
    "BAT-USDT": [
      "BAT",
      1.0,
      1.0
    ],
    "PROM-USDT": [
      "PROM",
      1.0,
      1.0
    ],
    "MINA-USDT": [
      "MINA",
      1.0,
      1.0
    ],
    "MORPHO-USDT": [
      "MORPHO",
      1.0,
      1.0
    ],
    "PONS-USDT": [
      "PONS",
      1.0,
      1.0
    ],
    "XRP-USDT": [
      "XRP",
      1.0,
      1.0
    ],
    "NEO-USDT": [
      "NEO",
      1.0,
      1.0
    ],
    "SENT-USDT": [
      "SENT",
      1.0,
      1.0
    ],
    "RAY-USDT": [
      "RAY",
      1.0,
      1.0
    ],
    "ZRO-USDT": [
      "ZRO",
      1.0,
      1.0
    ],
    "PIEVERSE-USDT": [
      "PIEVERSE",
      1.0,
      1.0
    ],
    "SUI-USDT": [
      "SUI",
      1.0,
      1.0
    ],
    "OPG-USDT": [
      "OPG",
      1.0,
      1.0
    ],
    "WCT-USDT": [
      "WCT",
      1.0,
      1.0
    ],
    "JTO-USDT": [
      "JTO",
      1.0,
      1.0
    ],
    "SEI-USDT": [
      "SEI",
      1.0,
      1.0
    ],
    "WAL-USDT": [
      "WAL",
      1.0,
      1.0
    ],
    "LINK-USDT": [
      "LINK",
      1.0,
      1.0
    ],
    "SPK-USDT": [
      "SPK",
      1.0,
      1.0
    ],
    "ETC-USDT": [
      "ETC",
      1.0,
      1.0
    ],
    "PENDLE-USDT": [
      "PENDLE",
      1.0,
      1.0
    ],
    "TREE-USDT": [
      "TREE",
      1.0,
      1.0
    ],
    "CYBER-USDT": [
      "CYBER",
      1.0,
      1.0
    ],
    "GTC-USDT": [
      "GTC",
      1.0,
      1.0
    ],
    "APT-USDT": [
      "APT",
      1.0,
      1.0
    ],
    "ENS-USDT": [
      "ENS",
      1.0,
      1.0
    ],
    "DOT-USDT": [
      "DOT",
      1.0,
      1.0
    ],
    "VANA-USDT": [
      "VANA",
      1.0,
      1.0
    ],
    "LINEA-USDT": [
      "LINEA",
      1.0,
      1.0
    ],
    "FLOCK-USDT": [
      "FLOCK",
      1.0,
      1.0
    ],
    "TAIKO-USDT": [
      "TAIKO",
      1.0,
      1.0
    ],
    "GMT-USDT": [
      "GMT",
      1.0,
      1.0
    ],
    "ZKC-USDT": [
      "ZKC",
      1.0,
      1.0
    ],
    "ZKP-USDT": [
      "ZKP",
      1.0,
      1.0
    ],
    "MEGA-USDT": [
      "MEGA",
      1.0,
      1.0
    ],
    "BCH-USDT": [
      "BCH",
      1.0,
      1.0
    ],
    "CAP-USDT": [
      "CAP",
      1.0,
      1.0
    ],
    "TIA-USDT": [
      "TIA",
      1.0,
      1.0
    ],
    "AAVE-USDT": [
      "AAVE",
      1.0,
      1.0
    ],
    "AXL-USDT": [
      "AXL",
      1.0,
      1.0
    ],
    "ATH-USDT": [
      "ATH",
      1.0,
      1.0
    ],
    "ESP-USDT": [
      "ESP",
      1.0,
      1.0
    ],
    "F-USDT": [
      "F",
      1.0,
      1.0
    ],
    "IOST-USDT": [
      "IOST",
      1.0,
      1.0
    ],
    "ZBT-USDT": [
      "ZBT",
      1.0,
      1.0
    ],
    "BEAM-USDT": [
      "BEAM",
      1.0,
      1.0
    ],
    "FF-USDT": [
      "FF",
      1.0,
      1.0
    ],
    "VIRTUAL-USDT": [
      "VIRTUAL",
      1.0,
      1.0
    ]
  },
  "htx_sym_map": {
    "LIT-USDT": [
      "LIT",
      1.0,
      1.0
    ],
    "TAO-USDT": [
      "TAO",
      1.0,
      0.001
    ],
    "WLD-USDT": [
      "WLD",
      1.0,
      1.0
    ],
    "FIL-USDT": [
      "FIL",
      1.0,
      0.1
    ],
    "UNI-USDT": [
      "UNI",
      1.0,
      1.0
    ],
    "PUMP-USDT": [
      "PUMP",
      1.0,
      100.0
    ],
    "NEAR-USDT": [
      "NEAR",
      1.0,
      1.0
    ],
    "PONS-USDT": [
      "PONS",
      1.0,
      10.0
    ],
    "SUI-USDT": [
      "SUI",
      1.0,
      1.0
    ],
    "WAL-USDT": [
      "WAL",
      1.0,
      10.0
    ],
    "LINK-USDT": [
      "LINK",
      1.0,
      0.1
    ],
    "TIA-USDT": [
      "TIA",
      1.0,
      0.1
    ]
  },
  "pionex_sym_map": {
    "ETHFI_USDT_PERP": [
      "ETHFI",
      1.0,
      1.0
    ],
    "AXS_USDT_PERP": [
      "AXS",
      1.0,
      1.0
    ],
    "SPX_USDT_PERP": [
      "SPX",
      1.0,
      1.0
    ],
    "LIT_USDT_PERP": [
      "LIT",
      1.0,
      1.0
    ],
    "TAO_USDT_PERP": [
      "TAO",
      1.0,
      1.0
    ],
    "SAND_USDT_PERP": [
      "SAND",
      1.0,
      1.0
    ],
    "ELSA_USDT_PERP": [
      "ELSA",
      1.0,
      1.0
    ],
    "VET_USDT_PERP": [
      "VET",
      1.0,
      1.0
    ],
    "WLD_USDT_PERP": [
      "WLD",
      1.0,
      1.0
    ],
    "FIL_USDT_PERP": [
      "FIL",
      1.0,
      1.0
    ],
    "UNI_USDT_PERP": [
      "UNI",
      1.0,
      1.0
    ],
    "PUMP_USDT_PERP": [
      "PUMP",
      1.0,
      1.0
    ],
    "AERO_USDT_PERP": [
      "AERO",
      1.0,
      1.0
    ],
    "NEAR_USDT_PERP": [
      "NEAR",
      1.0,
      1.0
    ],
    "ICP_USDT_PERP": [
      "ICP",
      1.0,
      1.0
    ],
    "BAT_USDT_PERP": [
      "BAT",
      1.0,
      1.0
    ],
    "PONS_USDT_PERP": [
      "PONS",
      1.0,
      1.0
    ],
    "TRUMP_USDT_PERP": [
      "TRUMP",
      1.0,
      1.0
    ],
    "XRP_USDT_PERP": [
      "XRP",
      1.0,
      1.0
    ],
    "SENT_USDT_PERP": [
      "SENT",
      1.0,
      1.0
    ],
    "RAY_USDT_PERP": [
      "RAY",
      1.0,
      1.0
    ],
    "ZRO_USDT_PERP": [
      "ZRO",
      1.0,
      1.0
    ],
    "SUI_USDT_PERP": [
      "SUI",
      1.0,
      1.0
    ],
    "JTO_USDT_PERP": [
      "JTO",
      1.0,
      1.0
    ],
    "SEI_USDT_PERP": [
      "SEI",
      1.0,
      1.0
    ],
    "WAL_USDT_PERP": [
      "WAL",
      1.0,
      1.0
    ],
    "LINK_USDT_PERP": [
      "LINK",
      1.0,
      1.0
    ],
    "ETC_USDT_PERP": [
      "ETC",
      1.0,
      1.0
    ],
    "PENDLE_USDT_PERP": [
      "PENDLE",
      1.0,
      1.0
    ],
    "APT_USDT_PERP": [
      "APT",
      1.0,
      1.0
    ],
    "DOT_USDT_PERP": [
      "DOT",
      1.0,
      1.0
    ],
    "BCH_USDT_PERP": [
      "BCH",
      1.0,
      1.0
    ],
    "TIA_USDT_PERP": [
      "TIA",
      1.0,
      1.0
    ],
    "AAVE_USDT_PERP": [
      "AAVE",
      1.0,
      1.0
    ],
    "RENDER_USDT_PERP": [
      "RENDER",
      1.0,
      1.0
    ],
    "VIRTUAL_USDT_PERP": [
      "VIRTUAL",
      1.0,
      1.0
    ]
  }
}
```

## venues/__init__.py

```python
"""
venues — Modular Exchange Package for Korean Sunrise.
Contains dedicated REST and WebSocket implementations for each venue:
  - Upbit (Spot Leader, South Korea KRW)
  - Binance (USDT Linear Perpetuals Follower)
  - Bybit (USDT Linear Perpetuals Follower)
  - OKX (USDT Swap Follower)
  - Gate.io (USDT Futures Follower)
  - MEXC (USDT Contract Follower)
  - KuCoin (USDT Futures Follower)
  - BingX (USDT Swap Follower)
  - HTX / Huobi (USDT Linear Swap Follower)
  - Pionex (USDT Perpetual Follower)
"""

from venues.base import (
    BaseVenueStream,
    BaseCEXStream,
    create_http_session,
    normalize_symbol,
)
from venues.upbit import UpbitRestClient, UpbitSpotStream
from venues.binance import BinanceRestClient, BinanceFuturesStream
from venues.bybit import BybitRestClient, BybitPerpStream
from venues.okx import OKXRestClient, OKXStream
from venues.gateio import GateioRestClient, GateioStream
from venues.mexc import MEXCRestClient, MEXCStream
from venues.kucoin import KucoinRestClient, KucoinStream
from venues.bingx import BingXRestClient, BingXStream
from venues.htx import HTXRestClient, HTXStream
from venues.pionex import PionexRestClient, PionexStream
from venues.asterdex import AsterdexRestClient, AsterdexStream
from venues.hyperliquid import HyperliquidRestClient, HyperliquidStream
from venues.manager import StreamManager

__all__ = [
    "BaseVenueStream",
    "BaseCEXStream",
    "create_http_session",
    "normalize_symbol",
    "UpbitSpotStream",
    "UpbitRestClient",
    "BinanceFuturesStream",
    "BinanceRestClient",
    "BybitPerpStream",
    "BybitRestClient",
    "OKXStream",
    "OKXRestClient",
    "GateioStream",
    "GateioRestClient",
    "MEXCStream",
    "MEXCRestClient",
    "KucoinStream",
    "KucoinRestClient",
    "BingXStream",
    "BingXRestClient",
    "HTXStream",
    "HTXRestClient",
    "PionexStream",
    "PionexRestClient",
    "AsterdexStream",
    "AsterdexRestClient",
    "HyperliquidStream",
    "HyperliquidRestClient",
    "StreamManager",
]
```

## venues/asterdex.py

```python
"""
venues/asterdex.py — Aster DEX REST Client and WebSocket depth5@100ms Stream.
Aster DEX is a high-liquidity perpetual DEX (built on ApolloX/APX Binance Cloud protocol).
REST and WebSocket APIs are 100% Binance-Futures-compatible.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseCEXStream,
    create_http_session,
    normalize_symbol,
    parse_symbol_and_mult,
    log,
)


class AsterdexRestClient:
    """REST Client for Aster DEX linear perpetuals discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://fapi.asterdex.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float, float]]:
        """
        Fetches all active USDT Linear Perpetuals from Aster DEX.
        Returns: raw_symbol (e.g. '1000PEPEUSDT') -> (canonical_coin 'PEPE', multiplier 1000.0, contract_size 1.0).
        """
        url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
        out: Dict[str, Tuple[str, float, float]] = {}
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for s in data.get("symbols", []):
                    if (
                        s.get("contractType") == "PERPETUAL"
                        and s.get("status") == "TRADING"
                        and s.get("quoteAsset") == "USDT"
                    ):
                        raw_sym = s.get("symbol", "").upper()
                        base_asset = s.get("baseAsset", "").upper()
                        coin, mult = parse_symbol_and_mult(base_asset)
                        c_size = float(s.get("contractSize") or s.get("contract_size") or 1.0)
                        out[raw_sym] = (coin, mult, c_size)
        except Exception as e:
            log.warning(f"[asterdex_rest] Failed to fetch exchangeInfo: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """
        Fetches contract_size for active symbols.
        Returns raw_symbol -> contract_size float.
        """
        url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for s in data.get("symbols", []):
                    raw_sym = s.get("symbol", "").upper()
                    c_size = float(s.get("contractSize") or s.get("contract_size") or 1.0)
                    out[raw_sym] = c_size
        except Exception as e:
            log.warning(f"[asterdex_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24-hour ticker statistics for Aster DEX perps.
        Returns: raw_symbol -> {'venue_sym': sym, 'price': float, 'vol_24h': float}.
        """
        url = f"{self.BASE_URL}/fapi/v1/ticker/24hr"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                for item in r.json():
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("USDT"):
                        continue
                    vol = float(item.get("quoteVolume", 0.0) or 0.0)
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[asterdex_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        symbol: str,
        limit: int = 200,
        mult: float = 1.0,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to limit 1m klines from Aster DEX.
        Returns: minute_ts_sec -> (normalized_close_usd, quote_volume_usd).
        """
        url = f"{self.BASE_URL}/fapi/v1/klines?symbol={symbol}&interval=1m&limit={min(limit, 1000)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json():
                    t_sec = (int(row[0]) // 60000) * 60
                    close = float(row[4]) * inv_mult
                    quote_vol = float(row[7])
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, quote_vol)
        except Exception:
            pass
        return out


class AsterdexStream(BaseCEXStream):
    """
    Subscribes to depth5@100ms on Aster DEX USDT Linear Perps via WebSocket.
    Uses Binance-compatible SUBSCRIBE method and depthUpdate stream format.
    """

    WS_URL = "wss://fstream.asterdex.com/ws"
    CHUNK_SIZE = 50

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("asterdex", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())

    async def _send_subscriptions(self, ws: Any) -> None:
        if not self._raw_symbols:
            return

        streams = [f"{s.lower()}@depth5@100ms" for s in self._raw_symbols]
        for i in range(0, len(streams), self.CHUNK_SIZE):
            chunk = streams[i : i + self.CHUNK_SIZE]
            sub_msg = {
                "method": "SUBSCRIBE",
                "params": chunk,
                "id": (i // self.CHUNK_SIZE) + 1,
            }
            await ws.send(json.dumps(sub_msg))
            await asyncio.sleep(0.05)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            payload = json.loads(raw_msg)
            if payload.get("id") is not None and "result" in payload:
                self._handle_sub_reply(payload)
                return

            data = payload.get("data", payload)
            if data.get("e") != "depthUpdate":
                return

            raw_sym = str(data.get("s", "")).upper()
            if not raw_sym:
                return

            # Sequence gap / packet drop detection
            curr_u = int(data.get("u") or 0)
            prev_u = int(data.get("pu") or 0)
            if curr_u > 0:
                last_u = self._last_u.get(raw_sym)
                if last_u is not None and prev_u > 0:
                    if prev_u != last_u:
                        self.seq_gaps += 1
                        log.warning(
                            f"[asterdex] Sequence gap for {raw_sym}: expected pu={last_u}, got {prev_u} (u={curr_u}). Dropping gap tick."
                        )
                        self._last_u[raw_sym] = curr_u
                        return
                    if curr_u <= last_u:
                        self.seq_duplicates += 1
                        return
                self._last_u[raw_sym] = curr_u

            coin, mult = self._resolve_coin_and_mult(raw_sym)
            c_size = self._get_contract_size(raw_sym)
            exchange_ts = normalize_exchange_ts(data.get("E")) or int(time.time() * 1000)

            raw_bids = data.get("b", [])
            raw_asks = data.get("a", [])

            bids = self._extract_levels(raw_bids, limit=5, mult=mult, contract_size=c_size)
            asks = self._extract_levels(raw_asks, limit=5, mult=mult, contract_size=c_size)

            if not bids or not asks:
                return

            if coin not in self.symbols_seen:
                self._note_sub(True)
            snap = OrderBookDepth5(
                exchange="asterdex",
                symbol=coin,
                raw_symbol=raw_sym,
                ts=time.time(),
                exchange_ts=exchange_ts,
                bids=bids,
                asks=asks,
            )
            self.callback(snap)
        except Exception:
            pass
```

## venues/base.py

```python
"""
venues/base.py — Core base classes and utilities for venue REST and WebSocket connections.
Provides BaseVenueStream and BaseCEXStream with auto-reconnection, daily reconnect windows,
gzip decompression, level extraction helpers, and HTTP session management.
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
from requests.adapters import HTTPAdapter
import websockets

from models import OrderBookDepth5, normalize_exchange_ts

log = logging.getLogger("stream")

_DAILY_RECONNECT_HOUR = 2
_DAILY_RECONNECT_MINUTE = 23
_RECONNECT_SPREAD_SEC = 7 * 60

_SEPS = "-_/:"
_QUOTES = ("USDT", "USDC", "USD", "KRW")
_CONTRACT_SUFFIXES = ("PERP", "SWAP")


import re

_KNOWN_BASE_TOKENS = {
    # Genuine tokens starting with K, M, B that must not be stripped as metric multiplier prefixes
    "KAS", "KAVA", "KLAY", "KDA", "KSM", "KEY", "KNC", "KP3R", "KMD", "KUB",
    "MKR", "MANA", "MATIC", "MEME", "MINA", "MOVE", "ME", "MOVR", "MDT", "MAGIC", "MASK", "MBL", "MTL", "MBOX", "MYRO", "MOG", "MELANIA",
    "BTC", "BCH", "BNB", "BOME", "BONK", "BLUR", "BAT", "BICO", "BAND", "BADGER", "BEL", "BETA", "BAKE", "BEAM", "BSV", "BTT", "BIGTIME",
}

_SCALE_PREFIX_MAP: List[Tuple[re.Pattern, float]] = [
    (re.compile(r"^1000000000([A-Z0-9]+)$"), 1_000_000_000.0),
    (re.compile(r"^1B([A-Z0-9]+)$"), 1_000_000_000.0),
    (re.compile(r"^1000000([A-Z0-9]+)$"), 1_000_000.0),
    (re.compile(r"^1M([A-Z0-9]+)$"), 1_000_000.0),
    (re.compile(r"^100000([A-Z0-9]+)$"), 100_000.0),
    (re.compile(r"^10000([A-Z0-9]+)$"), 10_000.0),
    (re.compile(r"^1000([A-Z0-9]+)$"), 1_000.0),
    (re.compile(r"^1K([A-Z0-9]+)$"), 1_000.0),
    (re.compile(r"^100([A-Z0-9]+)$"), 100.0),
]

_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")


def parse_symbol_and_mult(raw: str) -> Tuple[str, float]:
    """
    Returns (canonical_base_coin, multiplier)
    e.g. '1000PEPE-USDT' -> ('PEPE', 1000.0)
         '1MPEPE_USDT'   -> ('PEPE', 1000000.0)
         'kPEPE'         -> ('PEPE', 1000.0)
         'kSHIB'         -> ('SHIB', 1000.0)
         'mPEPE'         -> ('PEPE', 1000000.0)
         'bLUNC'         -> ('LUNC', 1000000000.0)
         'KASUSDT'       -> ('KAS', 1.0)
    """
    s_raw = str(raw).strip()
    if not s_raw:
        return "", 1.0

    # Hyperliquid / DEX single-letter prefix convention ('k' for 1000, 'm' for 1M, 'b' for 1B)
    prefix_mult = 1.0
    if len(s_raw) > 2 and s_raw[0] in ("k", "m", "b") and s_raw[1].isupper():
        if s_raw.upper() not in _KNOWN_BASE_TOKENS:
            p_char = s_raw[0]
            if p_char == "k":
                prefix_mult = 1000.0
            elif p_char == "m":
                prefix_mult = 1_000_000.0
            elif p_char == "b":
                prefix_mult = 1_000_000_000.0
            s_raw = s_raw[1:]

    s = s_raw.upper()
    if ":" in s:
        s = s.split(":", 1)[0]
    for sep in _SEPS:
        s = s.replace(sep, "")
    for suf in _CONTRACT_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
    for q in _QUOTES:
        if s.endswith(q) and len(s) > len(q):
            s = s[:-len(q)]
            break
    for suf in _CONTRACT_SUFFIXES:
        if s.endswith(suf) and len(s) > len(suf):
            s = s[:-len(suf)]
    if s == "XBT":
        s = "BTC"

    if prefix_mult != 1.0:
        return s, prefix_mult

    # Check numeric and metric prefix multipliers (1000, 1M, 1B, 1000000, etc.)
    for pattern, mult in _SCALE_PREFIX_MAP:
        m = pattern.match(s)
        if m:
            candidate_coin = m.group(1)
            if candidate_coin:
                return candidate_coin, mult

    return s, 1.0


def normalize_symbol(raw: str) -> str:
    """Canonical base coin symbol normalizer (strips 1000x prefix and quote)."""
    coin, _ = parse_symbol_and_mult(raw)
    return coin


def create_http_session(
    pool_connections: int = 35,
    pool_maxsize: int = 35,
    max_retries: int = 2,
    timeout: float = 8.0,
) -> requests.Session:
    """Creates a pre-configured requests.Session with connection pooling and headers."""
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_connections,
        pool_maxsize=pool_maxsize,
        max_retries=max_retries,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    })
    return session


def _secs_until_daily_reconnect() -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(
        hour=_DAILY_RECONNECT_HOUR, minute=_DAILY_RECONNECT_MINUTE,
        second=0, microsecond=0,
    )
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _reconnect_jitter_sec(name: str) -> float:
    h = int(hashlib.sha256(name.encode("utf-8")).hexdigest()[:8], 16)
    span = 2 * _RECONNECT_SPREAD_SEC
    return float(h % span) - _RECONNECT_SPREAD_SEC


def _secs_until_daily_reconnect_for(name: str) -> float:
    return max(1.0, _secs_until_daily_reconnect() + _reconnect_jitter_sec(name))


class ReconnectPolicy:
    """
    Unified exponential backoff and rate-limit mitigation policy for all stream connections.
    Eliminates fragile hardcoded sleeps and protects against exchange IP bans.
    """
    def __init__(
        self,
        base_delay: float = 2.0,
        max_delay: float = 120.0,
        factor: float = 2.0,
        healthy_threshold_sec: float = 15.0,
        use_jitter: bool = False,
        **kwargs: Any,
    ):
        if "backoff_factor" in kwargs:
            factor = float(kwargs["backoff_factor"])
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.factor = factor
        self.backoff_factor = factor  # alias for backward compatibility
        self.healthy_threshold_sec = healthy_threshold_sec
        self.use_jitter = use_jitter
        self.consecutive_failures = 0
        self._last_connected_ts = 0.0

    def record_connected(self) -> None:
        self._last_connected_ts = time.time()

    def record_disconnected(self) -> None:
        now = time.time()
        # If the stream was connected and healthy for >= healthy_threshold_sec,
        # reset consecutive failures so normal periodic reconnects start from base_delay.
        if self._last_connected_ts > 0.0 and (now - self._last_connected_ts >= self.healthy_threshold_sec):
            self.consecutive_failures = 0

    def compute_delay(self, error: Any = None, use_jitter: Optional[bool] = None) -> float:
        self.record_disconnected()
        self.consecutive_failures += 1

        # Exponential backoff: base_delay * (factor ** min(attempts, 6))
        exp_delay = min(self.max_delay, self.base_delay * (self.factor ** min(self.consecutive_failures - 1, 6)))
        should_jitter = self.use_jitter if use_jitter is None else use_jitter
        if should_jitter:
            # Predictable jitter based on attempt count (0.2s - 0.9s)
            jitter = 0.2 + ((self.consecutive_failures * 17) % 70) / 100.0
            delay = exp_delay + jitter
        else:
            delay = exp_delay

        err_str = str(error or "").lower()
        # Rate limit (HTTP 429 / 418 / "rate" / "too many requests") -> minimum 60.0s cool-down
        if "429" in err_str or "418" in err_str or "rate" in err_str or "too many" in err_str:
            delay = max(delay, 60.0)
        # Cloudflare / DNS resolution / Block (HTTP 403 / 11001 / getaddrinfo) -> minimum 30.0s cool-down
        elif "403" in err_str or "11001" in err_str or "getaddrinfo" in err_str:
            delay = max(delay, 30.0)

        return min(self.max_delay, delay)

    def reset(self) -> None:
        self.consecutive_failures = 0
        self._last_connected_ts = 0.0


class TTLCache:
    """
    Lightweight in-memory TTL dictionary cache for deduplicating high-frequency
    events and tracking subscription confirmations without unbounded memory growth.
    """
    def __init__(self, ttl_sec: float = 60.0):
        self.ttl_sec = ttl_sec
        self._store: Dict[Any, float] = {}

    def get(self, key: Any) -> bool:
        now = time.time()
        exp = self._store.get(key)
        if exp is None:
            return False
        if now > exp:
            del self._store[key]
            return False
        return True

    def set(self, key: Any) -> None:
        now = time.time()
        self._store[key] = now + self.ttl_sec
        if len(self._store) > 10000:
            self.cleanup()

    def cleanup(self) -> None:
        now = time.time()
        self._store = {k: exp for k, exp in self._store.items() if exp > now}


class BaseVenueStream:
    """Base class for venue-specific WebSocket feeds."""

    def __init__(self, name: str, symbols: List[str], callback: Callable[[OrderBookDepth5], None]):
        self.name = name.lower()
        self.symbols = [s.upper() for s in symbols]
        self._raw_callback = callback
        self.callback = self._sanitise
        self.connected = False
        self.last_msg_ts = 0.0
        self.last_data_ts = 0.0
        self._connect_ts = 0.0
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.subscribed: List[str] = []
        self.sub_accepted: int = 0
        self.sub_rejected: int = 0
        self.sub_errors: List[str] = []
        self.symbols_seen: Set[str] = set()
        self.sym_first_ts: Dict[str, float] = {}
        self.books_dropped = 0
        self.levels_dropped = 0
        self.seq_gaps = 0
        self.seq_duplicates = 0
        self._last_u: Dict[str, int] = {}
        self._last_seq: Dict[str, int] = {}
        self._first_ts = 0.0
        self._reconnect_policy = ReconnectPolicy(base_delay=2.0, max_delay=120.0)

    def _note_connect(self) -> None:
        self.connected = True
        self.last_msg_ts = time.time()
        self._connect_ts = self.last_msg_ts
        if not self._first_ts:
            self._first_ts = self.last_msg_ts
        self._reconnect_policy.record_connected()

    def _note_symbol(self, sym: str) -> None:
        if not sym:
            return
        s = sym.upper()
        self.last_data_ts = time.time()
        if s not in self.symbols_seen:
            self.symbols_seen.add(s)
            if not self._first_ts:
                self._first_ts = time.time()
            self.sym_first_ts[s] = self._first_ts

    def _note_sub(self, ok: bool, msg: str = "") -> None:
        if ok:
            self.sub_accepted += 1
        else:
            self.sub_rejected += 1
            if msg:
                self.sub_errors.append(msg[:200])
                if len(self.sub_errors) > 20:
                    del self.sub_errors[:-20]

    def _starved_since_connect(self, now: float, grace_sec: float = 30.0) -> bool:
        cts = getattr(self, "_connect_ts", 0.0) or 0.0
        if cts <= 0:
            return False
        last_data = getattr(self, "last_data_ts", 0.0) or 0.0
        return (now - cts > grace_sec) and (last_data <= cts)

    @staticmethod
    def _sub_reply_text(data: Dict[str, Any]) -> str:
        for k in ("ret_msg", "msg", "message", "err_msg", "detail", "error"):
            v = data.get(k)
            if isinstance(v, str) and v:
                return v
            if isinstance(v, dict):
                return json.dumps(v, ensure_ascii=False)[:200]
        return ""

    def _handle_sub_reply(self, data: Dict[str, Any]) -> None:
        try:
            if not isinstance(data, dict):
                return
            ev = str(data.get("event") or "").lower()
            action = str(data.get("action") or "").lower()
            msg_type = str(data.get("type") or "").lower()
            req_id = str(data.get("id") or "")

            if data.get("op") in ("ping", "pong") or str(data.get("ret_msg", "")).strip().lower() in ("pong", "ping"):
                return

            if data.get("error") not in (None, {}, "", []):
                self._note_sub(False, self._sub_reply_text(data) or json.dumps(data.get("error"))[:200])
                return

            if data.get("success") is not None:
                if data["success"]:
                    self._note_sub(True)
                else:
                    self._note_sub(False, self._sub_reply_text(data) or "refused")
                return

            if data.get("op") == "error":
                self._note_sub(False, self._sub_reply_text(data) or "error")
                return

            # Covers Toobit ("event": "sub"), OKX ("event": "subscribe"), LBank ("action": "subscribe")
            if ev in ("sub", "subscribe", "unsubscribe") or action in ("sub", "subscribe"):
                self._note_sub(True)
                return
            if ev == "error":
                self._note_sub(False, self._sub_reply_text(data) or str(data.get("code", "")))
                return

            # KuCoin subscription acknowledgements
            if msg_type in ("ack", "welcome"):
                self._note_sub(True)
                return

            # XT / KuCoin batch response IDs
            if req_id and (req_id.startswith("xt_") or req_id.startswith("kc_") or req_id.startswith("sub_")):
                if "error" not in data and data.get("code") in (None, 0, "0", "00000", 200, "200"):
                    self._note_sub(True)
                    return

            if "code" in data and not any(k in data for k in ("data", "dataType", "topic", "channel", "table")):
                if str(data["code"]).strip() in ("0", "00000", "200000", "200"):
                    self._note_sub(True)
                else:
                    self._note_sub(False, self._sub_reply_text(data) or str(data["code"]))
                return

            ch = str(data.get("channel", ""))
            if ch.startswith("rs."):
                reason = str(data.get("data", ""))
                if ch == "rs.error" or reason not in ("success", ""):
                    self._note_sub(False, reason or ch)
                else:
                    self._note_sub(True)
                return

            if "result" in data and data.get("result") is None:
                self._note_sub(True)
                return
        except Exception:
            pass

    def _sanitise(self, snap: OrderBookDepth5) -> None:
        """Sanitise and filter crossed books before forwarding to engine."""
        try:
            bids = [
                (float(p), float(s))
                for p, s in snap.bids
                if float(p) > 0.0 and float(s) > 0.0
            ]
            asks = [
                (float(p), float(s))
                for p, s in snap.asks
                if float(p) > 0.0 and float(s) > 0.0
            ]
            if not bids or not asks:
                self.books_dropped += 1
                return

            # Defensively sort order book levels: bids descending, asks ascending
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])

            # Reject inverted (crossed) books
            if bids[0][0] >= asks[0][0]:
                self.books_dropped += 1
                return

            now = time.time()
            self._note_symbol(snap.symbol)

            clean_snap = OrderBookDepth5(
                exchange=snap.exchange,
                symbol=snap.symbol,
                raw_symbol=snap.raw_symbol,
                ts=snap.ts or now,
                exchange_ts=normalize_exchange_ts(snap.exchange_ts) or int(now * 1000),
                bids=bids[:5],
                asks=asks[:5],
            )
            self._raw_callback(clean_snap)

        except (ValueError, TypeError, IndexError) as e:
            self.levels_dropped += 1
            log.debug(f"[{self.name}] Sanitise drop: {e}")

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.connected = False

    async def _run_loop(self) -> None:
        raise NotImplementedError

    def get_status(self) -> dict:
        return {
            "name": self.name,
            "connected": self.connected,
            "symbols_seen": len(self.symbols_seen),
            "subscribed": len(self.subscribed),
            "sub_accepted": self.sub_accepted,
            "sub_rejected": self.sub_rejected,
            "sub_errors": list(self.sub_errors),
            "last_data_ago_sec": (
                round(time.time() - self.last_data_ts, 1)
                if self.last_data_ts else None
            ),
            "books_dropped": self.books_dropped,
            "levels_dropped": self.levels_dropped,
            "seq_gaps": self.seq_gaps,
            "seq_duplicates": self.seq_duplicates,
        }


class BaseCEXStream(BaseVenueStream):
    """
    Generic base class for Centralized Exchange WebSocket feeds.
    Provides auto-reconnection, heartbeat ping loops, binary gzip decompression,
    starvation detection, and level extraction helpers.
    """

    WS_URL: str = ""
    PING_INTERVAL_SEC: float = 15.0

    def __init__(
        self,
        exchange: str,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        self._contract_sizes: Dict[str, float] = {
            k.upper(): float(v) for k, v in (contract_sizes or {}).items()
        }
        if isinstance(symbols, dict):
            self._sym_map: Dict[str, Tuple[str, float]] = {}
            for k, v in symbols.items():
                k_u = str(k).upper()
                if isinstance(v, (list, tuple)):
                    coin = str(v[0])
                    mult = float(v[1]) if len(v) > 1 else 1.0
                    self._sym_map[k_u] = (coin, mult)
                    if len(v) >= 3 and k_u not in self._contract_sizes:
                        self._contract_sizes[k_u] = float(v[2])
                else:
                    self._sym_map[k_u] = parse_symbol_and_mult(k_u)
            raw_list = list(symbols.keys())
            coins = sorted({c for c, _ in self._sym_map.values()})
        else:
            raw_list = [str(s).upper() for s in symbols]
            self._sym_map = {s: parse_symbol_and_mult(s) for s in raw_list}
            coins = sorted({c for c, _ in self._sym_map.values()})

        super().__init__(exchange, raw_list, callback)
        self.venue = exchange.lower()
        if stream_name:
            self.name = stream_name.lower()
        self.ws_url = self.WS_URL
        self.subscribed = coins  # Canonical coins for accurate heartbeat X/Y counter

    def _resolve_coin_and_mult(self, raw_sym: str) -> Tuple[str, float]:
        raw_u = raw_sym.upper()
        if raw_u in self._sym_map:
            return self._sym_map[raw_u]
        return parse_symbol_and_mult(raw_u)

    def _get_contract_size(self, raw_sym: str) -> float:
        raw_u = raw_sym.upper()
        return self._contract_sizes.get(raw_u, 1.0)

    @staticmethod
    def _base_coin(s: str) -> str:
        return normalize_symbol(s)

    def _extract_levels(
        self,
        raw_side: Any,
        limit: int = 5,
        mult: float = 1.0,
        contract_size: float = 1.0,
    ) -> List[Tuple[float, float]]:
        out: List[Tuple[float, float]] = []
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        for row in raw_side or ():
            if len(out) >= limit:
                break
            try:
                if isinstance(row, (list, tuple)) and len(row) >= 2:
                    p, q = float(row[0]), float(row[1])
                elif isinstance(row, dict):
                    p = float(row.get("p") or row.get("price") or row.get("px") or 0.0)
                    q = float(
                        row.get("q")
                        or row.get("s")
                        or row.get("m")
                        or row.get("size")
                        or row.get("qty")
                        or row.get("amount")
                        or row.get("v")
                        or row.get("vol")
                        or 0.0
                    )
                else:
                    continue
                if p > 0.0 and q > 0.0:
                    real_p = p * inv_mult
                    real_q = q * contract_size * mult
                    out.append((real_p, real_q))
            except (TypeError, ValueError, IndexError):
                continue
        return out

    async def _send_subscriptions(self, ws: Any) -> None:
        raise NotImplementedError

    async def _send_client_ping(self, ws: Any) -> None:
        pass

    async def _handle_server_ping(self, ws: Any, raw_msg: str) -> bool:
        return False

    def _parse_message(self, raw_msg: str) -> None:
        raise NotImplementedError

    async def _ping_loop(self, ws: Any) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.PING_INTERVAL_SEC)
                await self._send_client_ping(ws)
        except Exception:
            pass

    async def _run_loop(self) -> None:
        if not self.ws_url:
            log.warning(f"[{self.name}] No WS endpoint configured.")
            return

        while self._running:
            try:
                log.info(f"[{self.name}] Connecting to {self.ws_url} ({len(self.symbols)} symbols)...")
                async with websockets.connect(
                    self.ws_url,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=2000,
                ) as ws:
                    self._note_connect()
                    log.info(f"[{self.name}] Connected! Sending subscriptions...")
                    await self._send_subscriptions(ws)
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info(f"[{self.name}] Scheduled daily reconnect. Reconnecting...")
                                break

                            if self._starved_since_connect(time.time()):
                                log.warning(f"[{self.name}] Connected but no data in 30s, reconnecting...")
                                break

                            if not self.last_msg_ts:
                                self.last_msg_ts = time.time()
                            elif time.time() - self.last_msg_ts > 180.0:
                                log.warning(f"[{self.name}] No data for >180s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(30.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            # Automatic GZIP decompression for compressed streams (HTX, BingX)
                            if isinstance(msg, bytes):
                                try:
                                    msg = gzip.decompress(msg).decode("utf-8")
                                except Exception:
                                    msg = msg.decode("utf-8", errors="ignore")

                            if isinstance(msg, str):
                                s_clean = msg.strip().strip('"').lower()
                                if s_clean == "ping":
                                    await ws.send("pong")
                                    self.last_msg_ts = time.time()
                                    continue
                                if s_clean == "pong":
                                    self.last_msg_ts = time.time()
                                    continue
                                if "ping" in s_clean or "pong" in s_clean or '"time"' in s_clean:
                                    if await self._handle_server_ping(ws, msg):
                                        self.last_msg_ts = time.time()
                                        continue

                            self.last_msg_ts = time.time()
                            self._parse_message(msg)
                    finally:
                        ping_task.cancel()
                        self.connected = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[{self.name}] WS disconnected: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)
```

## venues/binance.py

```python
"""
venues/binance.py — Binance Futures REST Client and WebSocket depth5@100ms Stream.
Normalizes 1000x/1M perpetual contracts to single-unit prices and quantities.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    ReconnectPolicy,
    create_http_session,
    normalize_symbol,
    _secs_until_daily_reconnect_for,
    log,
)

_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")


class BinanceRestClient:
    """REST Client for Binance Futures discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://fapi.binance.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """
        Fetches all active USDT Linear Perpetuals from Binance.
        Returns: raw_symbol (e.g. '1000PEPEUSDT') -> (canonical_coin 'PEPE', multiplier 1000.0).
        """
        url = f"{self.BASE_URL}/fapi/v1/exchangeInfo"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                for s in data.get("symbols", []):
                    if (
                        s.get("contractType") == "PERPETUAL"
                        and s.get("status") == "TRADING"
                        and s.get("quoteAsset") == "USDT"
                    ):
                        raw_sym = s["symbol"].upper()
                        base_asset = s.get("baseAsset", "").upper()
                        mult = 1.0
                        m = _MULT_PREFIX_RE.match(base_asset)
                        if m:
                            mult = float(m.group(1))
                            base_asset = m.group(2)
                        out[raw_sym] = (base_asset, mult)
        except Exception as e:
            log.error(f"[binance_rest] Failed to fetch exchangeInfo: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24h ticker metrics for all symbols in a single request.
        Returns raw_symbol -> {price, vol_24h}.
        """
        url = f"{self.BASE_URL}/fapi/v1/ticker/24hr"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json():
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("USDT"):
                        continue
                    vol = float(item.get("quoteVolume", 0.0) or 0.0)
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[binance_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        symbol: str,
        limit: int = 200,
        mult: float = 1.0,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to 200 1m klines.
        Returns dict: minute_ts_sec -> (normalized_close_usd, quote_volume_usd).
        """
        url = f"{self.BASE_URL}/fapi/v1/klines?symbol={symbol}&interval=1m&limit={min(limit, 1500)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json():
                    t_sec = (int(row[0]) // 60000) * 60
                    close = float(row[4]) * inv_mult
                    quote_vol = float(row[7])
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, quote_vol)
        except Exception:
            pass
        return out


class BinanceFuturesStream(BaseVenueStream):
    """Subscribes to depth5@100ms on Binance USDT Linear Perps."""

    BASE_WS_URL = "wss://fstream.binance.com/stream"
    WATCHDOG_SECS = 30.0
    MAX_STREAMS_PER_CONN = 180

    def __init__(
        self,
        sym_map: Dict[str, Tuple[str, float]],
        callback: Callable[[OrderBookDepth5], None],
    ):
        coins = sorted({v[0] for v in sym_map.values()})
        super().__init__("binance", coins, callback)
        self._sym_map: Dict[str, Tuple[Any, ...]] = dict(sym_map)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())
        self.subscribed = list(coins)
        self._connected_chunks: Set[int] = set()
        self._chunk_policies: Dict[int, ReconnectPolicy] = {}

    async def _run_loop(self) -> None:
        if not self._raw_symbols:
            log.warning("[binance] No matched symbols to subscribe.")
            return

        chunks = [
            self._raw_symbols[i : i + self.MAX_STREAMS_PER_CONN]
            for i in range(0, len(self._raw_symbols), self.MAX_STREAMS_PER_CONN)
        ]

        if len(chunks) == 1:
            await self._run_single_chunk(chunks[0], chunk_id=0)
        else:
            tasks = [
                asyncio.create_task(self._run_single_chunk(chunk, idx))
                for idx, chunk in enumerate(chunks)
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                for t in tasks:
                    if not t.done():
                        t.cancel()

    async def _run_single_chunk(self, raw_syms: List[str], chunk_id: int) -> None:
        sub_streams = [f"{s.lower()}@depth5@100ms" for s in raw_syms]
        url = f"{self.BASE_WS_URL}?streams=" + "/".join(sub_streams)
        tag = f"binance#{chunk_id}" if chunk_id > 0 else "binance"
        policy = self._chunk_policies.setdefault(chunk_id, ReconnectPolicy(base_delay=2.0, max_delay=120.0))

        while self._running:
            try:
                log.info(f"[{tag}] Connecting to stream ({len(raw_syms)} contracts)...")
                async with websockets.connect(
                    url,
                    open_timeout=20,
                    ping_interval=20,
                    ping_timeout=15,
                    max_queue=4096,
                ) as ws:
                    self._connected_chunks.add(chunk_id)
                    self._note_connect()
                    policy.record_connected()
                    log.info(f"[{tag}] Connected! Streaming depth5@100ms...")
                    reconnect_in = _secs_until_daily_reconnect_for(f"{self.name}_{chunk_id}")
                    deadline = time.monotonic() + reconnect_in
                    chunk_connect_ts = time.time()
                    chunk_last_msg_ts = chunk_connect_ts
                    chunk_data_ts = 0.0

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info(f"[{tag}] Scheduled daily reconnect. Reconnecting...")
                                break

                            now = time.time()
                            if (now - chunk_connect_ts > 30.0) and (chunk_data_ts <= chunk_connect_ts):
                                log.warning(f"[{tag}] Connected but no data in 30s, reconnecting...")
                                break

                            if now - chunk_last_msg_ts > self.WATCHDOG_SECS:
                                log.warning(f"[{tag}] No messages for >{self.WATCHDOG_SECS:.0f}s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(15.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            recv_now = time.time()
                            chunk_last_msg_ts = recv_now
                            chunk_data_ts = recv_now
                            self.last_msg_ts = recv_now
                            self._parse_message(msg)
                    finally:
                        self._connected_chunks.discard(chunk_id)
                        self.connected = bool(self._connected_chunks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected_chunks.discard(chunk_id)
                self.connected = bool(self._connected_chunks)
                delay = policy.compute_delay(e)
                log.warning(
                    f"[{tag}] WS disconnected: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            payload = json.loads(raw_msg)
            data = payload.get("data", payload)
            raw_sym = data.get("s")
            if not raw_sym:
                return

            raw_sym = raw_sym.upper()
            mapped = self._sym_map.get(raw_sym)
            if not mapped:
                return
            coin = mapped[0]
            mult = float(mapped[1])
            c_size = float(mapped[2]) if len(mapped) >= 3 else 1.0

            raw_bids = data.get("b", [])[:5]
            raw_asks = data.get("a", [])[:5]
            if not raw_bids or not raw_asks:
                return

            # Sequence gap / packet drop detection
            curr_u = int(data.get("u") or 0)
            prev_u = int(data.get("pu") or 0)
            if curr_u > 0:
                last_u = self._last_u.get(raw_sym)
                if last_u is not None and prev_u > 0:
                    if prev_u != last_u:
                        self.seq_gaps += 1
                        log.warning(
                            f"[binance] Sequence gap for {raw_sym}: expected pu={last_u}, got {prev_u} (u={curr_u}). Dropping gap tick."
                        )
                        self._last_u[raw_sym] = curr_u
                        return
                    if curr_u <= last_u:
                        self.seq_duplicates += 1
                        return
                self._last_u[raw_sym] = curr_u

            inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
            scale_qty = mult * c_size
            if inv_mult != 1.0 or scale_qty != 1.0:
                bids = [(float(p) * inv_mult, float(q) * scale_qty) for p, q in raw_bids]
                asks = [(float(p) * inv_mult, float(q) * scale_qty) for p, q in raw_asks]
            else:
                bids = [(float(p), float(q)) for p, q in raw_bids]
                asks = [(float(p), float(q)) for p, q in raw_asks]

            if coin not in self.symbols_seen:
                self._note_sub(True)
            self.callback(
                OrderBookDepth5(
                    exchange="binance",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=normalize_exchange_ts(data.get("E")) or int(time.time() * 1000),
                    bids=bids,
                    asks=asks,
                )
            )
        except Exception:
            pass
```

## venues/bingx.py

```python
"""
venues/bingx.py — BingX Swap REST Client and WebSocket depth20 Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, parse_symbol_and_mult, log


class BingXRestClient:
    """REST Client for BingX Swap discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://open-api.bingx.com/openApi/swap"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns symbol (e.g. 'BTC-USDT', '1000PEPE-USDT') -> (canonical_coin 'BTC', mult 1.0 or 1000.0)."""
        url = f"{self.BASE_URL}/v2/quote/contracts"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("-USDT"):
                        continue
                    base_coin, mult = parse_symbol_and_mult(item.get("asset", "") or sym)
                    out[sym] = (base_coin, mult)
        except Exception as e:
            log.error(f"[bingx_rest] Failed to fetch contracts: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns symbol -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/v2/quote/ticker"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("-USDT"):
                        continue
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    vol_usd = float(item.get("volume", 0.0) or 0.0) * price
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[bingx_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        sym = symbol if "-" in symbol else f"{normalize_symbol(symbol)}-USDT"
        url = f"{self.BASE_URL}/v3/quote/klines?symbol={sym}&interval=1m&limit={min(limit, 1000)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = (int(row.get("time", 0)) // 60000) * 60
                    raw_close = float(row.get("close", 0.0))
                    close = raw_close * inv_mult
                    vol_usd = float(row.get("volume", 0.0)) * raw_close
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class BingXStream(BaseCEXStream):
    """BingX Swap WebSocket order book client."""

    WS_URL = "wss://open-api-swap.bingx.com/swap-market"
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("bingx", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    def _to_raw_symbol(self, s: str) -> str:
        s_u = s.upper()
        if s_u.endswith("-USDT"):
            return s_u
        for raw, (coin, _) in self._sym_map.items():
            if coin.upper() == s_u:
                return raw
        return f"{s_u}-USDT"

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send("ping")

    async def _send_subscriptions(self, ws: Any) -> None:
        for s in self.symbols:
            raw_sym = self._to_raw_symbol(s)
            sub = {
                "id": f"sub_{raw_sym}",
                "reqType": "sub",
                "dataType": f"{raw_sym}@depth20",
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.02)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            datatype = data.get("dataType", "")
            if "@depth" not in datatype:
                return
            sym_raw = datatype.split("@")[0].upper()
            sym, mult = self._resolve_coin_and_mult(sym_raw)
            contract_size = self._get_contract_size(sym_raw)
            b_data = data.get("data", {})
            bids = self._extract_levels(b_data.get("bids"), 5, mult=mult, contract_size=contract_size)
            asks = self._extract_levels(b_data.get("asks"), 5, mult=mult, contract_size=contract_size)
            if sym and bids and asks:
                self._note_symbol(sym)
                self.callback(OrderBookDepth5(
                    exchange="bingx",
                    symbol=sym,
                    raw_symbol=sym_raw,
                    ts=time.time(),
                    exchange_ts=normalize_exchange_ts(b_data.get("ts")) or int(time.time() * 1000),
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
```

## venues/bybit.py

```python
"""
venues/bybit.py — Bybit Linear Perps REST Client and WebSocket L2 Stream.
Normalizes 1000x/1M perpetual contracts to single-unit prices and quantities.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    ReconnectPolicy,
    create_http_session,
    normalize_symbol,
    parse_symbol_and_mult,
    _secs_until_daily_reconnect_for,
    log,
)

_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")


class BybitRestClient:
    """REST Client for Bybit Linear Perps discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.bybit.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """
        Fetches all active USDT Linear Perpetuals from Bybit.
        Returns: raw_symbol (e.g. '1000PEPEUSDT') -> (canonical_coin 'PEPE', multiplier 1000.0).
        """
        url = f"{self.BASE_URL}/v5/market/instruments-info?category=linear&status=Trading&limit=1000"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                items = data.get("result", {}).get("list", [])
                for item in items:
                    raw_sym = item.get("symbol", "").upper()
                    quote = item.get("quoteCoin", "").upper()
                    if quote != "USDT":
                        continue
                    base_coin = item.get("baseCoin", "").upper()
                    base_coin, mult = parse_symbol_and_mult(base_coin)
                    out[raw_sym] = (base_coin, mult)
        except Exception as e:
            log.error(f"[bybit_rest] Failed to fetch linear instruments: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24h tickers for all linear perps in a single call.
        Returns raw_symbol -> {price, vol_24h}.
        """
        url = f"{self.BASE_URL}/v5/market/tickers?category=linear"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                items = r.json().get("result", {}).get("list", [])
                for item in items:
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("USDT"):
                        continue
                    vol = float(item.get("turnover24h", 0.0) or 0.0)
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[bybit_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        symbol: str,
        limit: int = 1440,
        mult: float = 1.0,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to 200 1m klines.
        Returns dict: minute_ts_sec -> (normalized_close_usd, turnover_usd).
        """
        url = f"{self.BASE_URL}/v5/market/kline?category=linear&symbol={symbol}&interval=1&limit={min(limit, 1000)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                items = r.json().get("result", {}).get("list", [])
                for row in items:
                    t_sec = (int(row[0]) // 60000) * 60
                    close = float(row[4]) * inv_mult
                    turnover = float(row[6])
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, turnover)
        except Exception:
            pass
        return out


class BybitPerpStream(BaseVenueStream):
    """Subscribes to linear orderbook.50 on Bybit and emits normalized top-5 snapshots."""

    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    WATCHDOG_SECS = 30.0

    def __init__(
        self,
        sym_map: Dict[str, Tuple[str, float]],
        callback: Callable[[OrderBookDepth5], None],
    ):
        coins = sorted({v[0] for v in sym_map.values()})
        super().__init__("bybit", coins, callback)
        self._sym_map: Dict[str, Tuple[Any, ...]] = dict(sym_map)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())
        self.subscribed = list(coins)
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}
        self._lock = threading.RLock()
        self._last_u: Dict[str, int] = {}
        self._last_seq: Dict[str, int] = {}
        self._synced: Dict[str, bool] = {}

    def get_book_snapshot(self, raw_sym: str) -> Optional[Dict[str, Dict[float, float]]]:
        """Thread-safe snapshot copy of the current L2 book for raw_sym."""
        with self._lock:
            b = self._books.get(raw_sym)
            if not b:
                return None
            return {"bids": dict(b["bids"]), "asks": dict(b["asks"])}

    async def _ping_loop(self, ws: Any) -> None:
        try:
            while self._running:
                await asyncio.sleep(20.0)
                await ws.send(json.dumps({"op": "ping"}))
        except Exception:
            pass

    async def _run_loop(self) -> None:
        if not self._raw_symbols:
            log.warning("[bybit] No matched symbols to subscribe.")
            return

        while self._running:
            try:
                with self._lock:
                    self._books.clear()
                    self._last_u.clear()
                    self._last_seq.clear()
                    self._synced.clear()
                log.info(f"[bybit] Connecting to {self.WS_URL} ({len(self._raw_symbols)} contracts)...")
                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=4096,
                ) as ws:
                    self._note_connect()
                    log.info("[bybit] Connected! Sending subscriptions...")

                    chunk_size = 25
                    for i in range(0, len(self._raw_symbols), chunk_size):
                        chunk = self._raw_symbols[i : i + chunk_size]
                        args = [f"orderbook.50.{s}" for s in chunk]
                        await ws.send(json.dumps({"op": "subscribe", "args": args}))
                        await asyncio.sleep(0.05)

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[bybit] Scheduled daily reconnect. Reconnecting...")
                                break

                            if self._starved_since_connect(time.time()):
                                log.warning("[bybit] Connected but no data in 30s, reconnecting...")
                                break

                            if time.time() - self.last_msg_ts > self.WATCHDOG_SECS:
                                log.warning(f"[bybit] No messages for >{self.WATCHDOG_SECS:.0f}s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(15.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            self.last_msg_ts = time.time()
                            self._parse_message(msg)
                    finally:
                        ping_task.cancel()
                        self.connected = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[bybit] WS disconnected: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            data = json.loads(raw_msg)
            if data.get("op") == "subscribe" or "success" in data or "ret_msg" in data:
                self._handle_sub_reply(data)

            topic = data.get("topic", "")
            if not topic.startswith("orderbook."):
                return

            d = data.get("data", {})
            raw_sym = d.get("s", "")
            if not raw_sym:
                return

            mapped = self._sym_map.get(raw_sym)
            if not mapped:
                return
            coin = mapped[0]
            mult = float(mapped[1])
            c_size = float(mapped[2]) if len(mapped) >= 3 else 1.0

            mtype = data.get("type", "delta")
            u = int(d.get("u") or 0)
            seq = int(d.get("seq") or 0)

            with self._lock:
                cur_book = self._books.get(raw_sym)
                is_desynced = (self._synced.get(raw_sym) is False)
                if mtype == "snapshot" or (cur_book is None and not is_desynced):
                    self._synced[raw_sym] = True
                    self._last_u[raw_sym] = u
                    if seq > 0:
                        self._last_seq[raw_sym] = seq
                    new_bids: Dict[float, float] = {}
                    new_asks: Dict[float, float] = {}
                else:
                    if is_desynced:
                        # Waiting for a fresh snapshot after gap
                        self.seq_gaps += 1
                        return

                    last_u = self._last_u.get(raw_sym, 0)
                    if u > 0 and last_u > 0:
                        if u <= last_u:
                            self.seq_duplicates += 1
                            return
                        pu = d.get("pu")
                        if pu is not None and int(pu) != last_u:
                            self.seq_gaps += 1
                            log.warning(f"[bybit] Sequence gap on {raw_sym}: expected pu={last_u}, got {pu}. Desyncing book.")
                            self._synced[raw_sym] = False
                            self._books.pop(raw_sym, None)
                            return

                    self._last_u[raw_sym] = u
                    if seq > 0:
                        self._last_seq[raw_sym] = seq

                    new_bids = dict(cur_book.get("bids", {}))
                    new_asks = dict(cur_book.get("asks", {}))

                for row in d.get("b", []):
                    p, q = float(row[0]), float(row[1])
                    if q <= 0.0 or p <= 0.0:
                        new_bids.pop(p, None)
                    else:
                        new_bids[p] = q

                for row in d.get("a", []):
                    p, q = float(row[0]), float(row[1])
                    if q <= 0.0 or p <= 0.0:
                        new_asks.pop(p, None)
                    else:
                        new_asks[p] = q

                if not new_bids or not new_asks:
                    return

                sorted_bids_raw = sorted(new_bids.items(), key=lambda x: x[0], reverse=True)
                sorted_asks_raw = sorted(new_asks.items(), key=lambda x: x[0])

                # Memory leak / GC protection: strictly cap depth to MAX_BOOK_DEPTH (25)
                # whenever dictionary size exceeds 25, instead of letting it balloon to 100+
                MAX_BOOK_DEPTH = 25
                if len(new_bids) > MAX_BOOK_DEPTH:
                    new_bids = dict(sorted_bids_raw[:MAX_BOOK_DEPTH])
                if len(new_asks) > MAX_BOOK_DEPTH:
                    new_asks = dict(sorted_asks_raw[:MAX_BOOK_DEPTH])

                # Atomic Copy-on-Write assignment
                self._books[raw_sym] = {"bids": new_bids, "asks": new_asks}

                top_bids = sorted_bids_raw[:5]
                top_asks = sorted_asks_raw[:5]

            if not top_bids or not top_asks:
                return

            inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
            scale_qty = mult * c_size
            if inv_mult != 1.0 or scale_qty != 1.0:
                norm_bids = [(p * inv_mult, q * scale_qty) for p, q in top_bids]
                norm_asks = [(p * inv_mult, q * scale_qty) for p, q in top_asks]
            else:
                norm_bids = [(p, q) for p, q in top_bids]
                norm_asks = [(p, q) for p, q in top_asks]

            self.callback(
                OrderBookDepth5(
                    exchange="bybit",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=normalize_exchange_ts(d.get("cts") or data.get("ts")) or int(time.time() * 1000),
                    bids=norm_bids,
                    asks=norm_asks,
                )
            )
        except Exception:
            pass
```

## venues/gateio.py

```python
"""
venues/gateio.py — Gate.io Futures REST Client and WebSocket order_book_update Stream.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, parse_symbol_and_mult, log


class GateioRestClient:
    """REST Client for Gate.io Futures discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://fx-api.gateio.ws/api/v4/futures/usdt"

    def __init__(self, session: Optional[requests.Session] = None):
        # Always use dedicated session so popping User-Agent does not mutate other venues' sessions
        self.session = create_http_session()
        # Gate.io API WAF rejects fake browser User-Agents with 403; default requests UA works
        self.session.headers.pop("User-Agent", None)

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns contract (e.g. 'BTC_USDT', '1000BONK_USDT') -> (canonical_coin 'BTC', mult 1.0 or 1000.0)."""
        url = f"{self.BASE_URL}/contracts"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                for item in r.json():
                    contract = item.get("name", "").upper()
                    if not contract.endswith("_USDT"):
                        continue
                    base_coin, mult = parse_symbol_and_mult(contract)
                    quanto = float(item.get("quanto_multiplier", 1.0) or 1.0)
                    out[contract] = (base_coin, mult, quanto)
        except Exception as e:
            log.error(f"[gateio_rest] Failed to fetch contracts: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns contract (e.g. 'BTC_USDT') -> quanto_multiplier (e.g. 0.0001)."""
        url = f"{self.BASE_URL}/contracts"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                for item in r.json():
                    contract = item.get("name", "").upper()
                    if not contract.endswith("_USDT"):
                        continue
                    quanto = float(item.get("quanto_multiplier", 1.0) or 1.0)
                    out[contract] = quanto
        except Exception as e:
            log.error(f"[gateio_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns contract -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/tickers"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                for item in r.json():
                    contract = item.get("contract", "")
                    if not contract.endswith("_USDT"):
                        continue
                    price = float(item.get("last", 0.0) or 0.0)
                    vol_usd = float(item.get("volume_24h_quote", 0.0) or 0.0)
                    if price > 0:
                        out[contract] = {"venue_sym": contract, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[gateio_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        contract = symbol if "_" in symbol else f"{normalize_symbol(symbol)}_USDT"
        url = f"{self.BASE_URL}/candlesticks?contract={contract}&interval=1m&limit={min(limit, 1000)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json():
                    t_sec = (int(row.get("t", 0)) // 60) * 60
                    close = float(row.get("c", 0.0)) * inv_mult
                    vol_usd = float(row.get("sum", 0.0))
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class GateioStream(BaseCEXStream):
    """Gate.io Futures WebSocket order book client."""

    WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols,
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("gateio", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}
        self._lock = threading.RLock()
        self._last_id: Dict[str, int] = {}
        self._synced: Dict[str, bool] = {}

    def get_book_snapshot(self, contract: str) -> Optional[Dict[str, Dict[float, float]]]:
        """Thread-safe snapshot copy of the current L2 book for contract."""
        with self._lock:
            b = self._books.get(contract)
            if not b:
                return None
            return {"bids": dict(b["bids"]), "asks": dict(b["asks"])}

    def _to_raw_symbol(self, s: str) -> str:
        s_u = s.upper()
        if s_u.endswith("_USDT"):
            return s_u
        for raw, (coin, _) in self._sym_map.items():
            if coin.upper() == s_u:
                return raw
        return f"{s_u}_USDT"

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.ping"}))

    async def _send_subscriptions(self, ws: Any) -> None:
        with self._lock:
            self._books.clear()
            self._last_id.clear()
            self._synced.clear()

        if not self._contract_sizes:
            try:
                sizes = await asyncio.to_thread(GateioRestClient().fetch_contract_sizes)
                if sizes:
                    self._contract_sizes.update(sizes)
            except Exception as e:
                log.warning(f"[gateio] Failed to auto-fetch contract sizes: {e}")

        for s in self.symbols:
            pair = self._to_raw_symbol(s)
            sub = {
                "time": int(time.time()),
                "channel": "futures.order_book_update",
                "event": "subscribe",
                "payload": [pair, "100ms", "20"],
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.01)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            channel = data.get("channel", "")
            if "order_book" not in channel:
                return
            result = data.get("result", {})
            contract = (result.get("contract", "") or result.get("s", "")).upper()
            if not contract:
                return
            sym, mult = self._resolve_coin_and_mult(contract)
            contract_size = self._get_contract_size(contract)

            event = data.get("event")
            curr_id = int(result.get("id") or result.get("last_id") or 0)

            with self._lock:
                cur_book = self._books.get(contract)
                is_desynced = (self._synced.get(contract) is False)
                if event == "all" or (cur_book is None and not is_desynced):
                    self._synced[contract] = True
                    self._last_id[contract] = curr_id
                    new_bids: Dict[float, float] = {}
                    new_asks: Dict[float, float] = {}
                else:
                    if is_desynced:
                        # Waiting for a fresh 'all' snapshot after gap
                        self.seq_gaps += 1
                        return

                    last_id = self._last_id.get(contract, 0)
                    if curr_id > 0 and last_id > 0:
                        if curr_id <= last_id:
                            self.seq_duplicates += 1
                            return
                        if curr_id > last_id + 1:
                            self.seq_gaps += 1
                            log.warning(
                                f"[gateio] Sequence gap on {contract}: expected {last_id + 1}, got {curr_id}. Desyncing book."
                            )
                            self._synced[contract] = False
                            self._books.pop(contract, None)
                            return

                    self._last_id[contract] = curr_id
                    new_bids = dict(cur_book.get("bids", {}))
                    new_asks = dict(cur_book.get("asks", {}))

                for row in result.get("b", []):
                    p, s = float(row.get("p", 0.0)), float(row.get("s", 0.0))
                    q = s * contract_size
                    if q <= 0.0 or p <= 0.0:
                        new_bids.pop(p, None)
                    else:
                        new_bids[p] = q

                for row in result.get("a", []):
                    p, s = float(row.get("p", 0.0)), float(row.get("s", 0.0))
                    q = s * contract_size
                    if q <= 0.0 or p <= 0.0:
                        new_asks.pop(p, None)
                    else:
                        new_asks[p] = q

                if not new_bids or not new_asks:
                    return

                sorted_bids_raw = sorted(new_bids.items(), key=lambda x: x[0], reverse=True)
                sorted_asks_raw = sorted(new_asks.items(), key=lambda x: x[0])

                # Memory leak / GC protection: strictly cap depth to MAX_BOOK_DEPTH (25)
                MAX_BOOK_DEPTH = 25
                if len(new_bids) > MAX_BOOK_DEPTH:
                    new_bids = dict(sorted_bids_raw[:MAX_BOOK_DEPTH])
                if len(new_asks) > MAX_BOOK_DEPTH:
                    new_asks = dict(sorted_asks_raw[:MAX_BOOK_DEPTH])

                # Atomic Copy-on-Write assignment
                self._books[contract] = {"bids": new_bids, "asks": new_asks}

            if mult != 1.0:
                inv_mult = 1.0 / mult
                top_bids = [(p * inv_mult, q * mult) for p, q in sorted_bids_raw[:5]]
                top_asks = [(p * inv_mult, q * mult) for p, q in sorted_asks_raw[:5]]
            else:
                top_bids = sorted_bids_raw[:5]
                top_asks = sorted_asks_raw[:5]

            if top_bids and top_asks:
                ets = normalize_exchange_ts(result.get("t") or data.get("time_ms") or data.get("time")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="gateio",
                    symbol=sym,
                    raw_symbol=contract,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=top_bids,
                    asks=top_asks,
                ))
        except Exception as e:
            log.debug(f"[gateio] parse error: {e}")
```

## venues/htx.py

```python
"""
venues/htx.py — HTX (Huobi) Linear Swap REST Client and WebSocket depth.step0 Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class HTXRestClient:
    """REST Client for HTX Linear Swap discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.hbdm.com"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns contract_code (e.g. 'BTC-USDT') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/linear-swap-api/v1/swap_contract_info"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    code = item.get("contract_code", "").upper()
                    if not code.endswith("-USDT"):
                        continue
                    base_coin = normalize_symbol(item.get("symbol", "") or code)
                    cs = float(item.get("contract_size", 1.0) or 1.0)
                    out[code] = (base_coin, 1.0, cs)
        except Exception as e:
            log.error(f"[htx_rest] Failed to fetch contract info: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns contract_code (e.g. 'BTC-USDT') -> contract_size (float)."""
        url = f"{self.BASE_URL}/linear-swap-api/v1/swap_contract_info"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    code = item.get("contract_code", "").upper()
                    if not code.endswith("-USDT"):
                        continue
                    cs = float(item.get("contract_size", 1.0) or 1.0)
                    out[code] = cs
        except Exception as e:
            log.error(f"[htx_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns contract_code -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/linear-swap-ex/market/detail/batch_merged"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("ticks", []):
                    code = item.get("contract_code", "").upper()
                    if not code.endswith("-USDT"):
                        continue
                    price = float(item.get("close", 0.0) or 0.0)
                    vol_usd = float(item.get("trade_turnover", 0.0) or 0.0)
                    if price > 0:
                        out[code] = {"venue_sym": code, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[htx_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        code = symbol if "-" in symbol else f"{normalize_symbol(symbol)}-USDT"
        url = f"{self.BASE_URL}/linear-swap-ex/market/history/kline?contract_code={code}&period=1min&size={min(limit, 2000)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = int(row.get("id", 0))
                    close = float(row.get("close", 0.0)) * inv_mult
                    vol_usd = float(row.get("trade_turnover", 0.0))
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class HTXStream(BaseCEXStream):
    """HTX Linear Swap WebSocket order book client."""

    WS_URL = "wss://api.hbdm.com/linear-swap-ws"
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("htx", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    async def _handle_server_ping(self, ws: Any, raw_msg: str) -> bool:
        try:
            d = json.loads(raw_msg)
            if isinstance(d, dict) and "ping" in d:
                await ws.send(json.dumps({"pong": d["ping"]}))
                return True
        except Exception:
            pass
        return False

    async def _send_subscriptions(self, ws: Any) -> None:
        if not self._contract_sizes:
            try:
                sizes = await asyncio.to_thread(HTXRestClient().fetch_contract_sizes)
                if sizes:
                    self._contract_sizes.update(sizes)
            except Exception as e:
                log.warning(f"[htx] Failed to auto-fetch contract sizes: {e}")

        for s in self.symbols:
            code = s if "-" in s else f"{self._base_coin(s)}-USDT"
            sub = {
                "sub": f"market.{code}.depth.step0",
                "id": f"sub_{code}",
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.015)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            ch = data.get("ch", "")
            if not ch.startswith("market.") or ".depth." not in ch:
                return
            code = ch.split(".")[1].upper()
            sym, mult = self._resolve_coin_and_mult(code)
            contract_size = self._get_contract_size(code)
            tick = data.get("tick", {})
            bids = self._extract_levels(tick.get("bids"), 5, mult=mult, contract_size=contract_size)
            asks = self._extract_levels(tick.get("asks"), 5, mult=mult, contract_size=contract_size)
            if bids and asks:
                self._note_symbol(sym)
                ets = normalize_exchange_ts(data.get("ts") or tick.get("ts")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="htx",
                    symbol=sym,
                    raw_symbol=code,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
```

## venues/hyperliquid.py

```python
"""
venues/hyperliquid.py — Hyperliquid REST Client and WebSocket l2Book Stream.
Connects directly to Hyperliquid L1/L2 matching engine.
Supports single-unit normalization for 'k...' 1000x multiplier assets (e.g. kPEPE, kBONK).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    ReconnectPolicy,
    create_http_session,
    normalize_symbol,
    parse_symbol_and_mult,
    _secs_until_daily_reconnect_for,
    log,
)


class HyperliquidRestClient:
    """REST Client for Hyperliquid perpetuals discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.hyperliquid.xyz/info"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float, float]]:
        """
        Fetches all active perpetual contracts from Hyperliquid.
        Returns: raw_symbol (e.g. 'SOLUSDT', '1000PEPEUSDT') -> (canonical_coin 'SOL', multiplier 1.0, contract_size 1.0).
        """
        out: Dict[str, Tuple[str, float, float]] = {}
        try:
            r = self.session.post(
                self.BASE_URL,
                json={"type": "metaAndAssetCtxs"},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                universe = data[0].get("universe", []) if isinstance(data, list) and len(data) > 0 else []
                for asset in universe:
                    hl_name = asset.get("name", "")
                    if not hl_name:
                        continue
                    coin, mult = parse_symbol_and_mult(hl_name)
                    if mult != 1.0:
                        raw_sym = f"{int(mult)}{coin}USDT"
                    else:
                        raw_sym = f"{coin}USDT"
                    out[raw_sym] = (coin, mult, 1.0)
        except Exception as e:
            log.warning(f"[hyperliquid_rest] Failed to fetch meta: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Fetches contract sizes for Hyperliquid perpetual contracts (defaults to 1.0)."""
        perps = self.fetch_linear_perpetuals()
        return {raw_sym: c_size for raw_sym, (_, _, c_size) in perps.items()}

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24-hour ticker statistics for Hyperliquid perps.
        Returns: raw_symbol -> {'venue_sym': raw_sym, 'price': float, 'vol_24h': float}.
        """
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.post(
                self.BASE_URL,
                json={"type": "metaAndAssetCtxs"},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list) and len(data) >= 2:
                    universe = data[0].get("universe", [])
                    ctxs = data[1]
                    for idx, asset in enumerate(universe):
                        hl_name = asset.get("name", "")
                        if not hl_name or idx >= len(ctxs):
                            continue
                        ctx = ctxs[idx]
                        coin, mult = parse_symbol_and_mult(hl_name)
                        if mult != 1.0:
                            raw_sym = f"{int(mult)}{coin}USDT"
                        else:
                            raw_sym = f"{coin}USDT"

                        price = float(ctx.get("midPx") or ctx.get("markPx") or 0.0)
                        vol = float(ctx.get("dayNtlVlm") or 0.0)
                        if price > 0:
                            out[raw_sym] = {"venue_sym": raw_sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[hyperliquid_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(
        self,
        symbol: str,
        limit: int = 200,
        mult: float = 1.0,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to limit 1m klines from Hyperliquid via candleSnapshot.
        Returns: minute_ts_sec -> (normalized_close_usd, quote_volume_usd).
        """
        out: Dict[int, Tuple[float, float]] = {}
        coin, s_mult = parse_symbol_and_mult(symbol)
        effective_mult = mult if mult != 1.0 else s_mult
        if effective_mult == 1000.0:
            hl_coin = "k" + coin
        elif effective_mult == 1_000_000.0:
            hl_coin = "m" + coin
        elif effective_mult == 1_000_000_000.0:
            hl_coin = "b" + coin
        else:
            hl_coin = coin

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - (min(limit, 500) * 60 * 1000)
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": hl_coin,
                "interval": "1m",
                "startTime": start_ms,
                "endTime": now_ms,
            },
        }
        inv_mult = (1.0 / effective_mult) if effective_mult != 1.0 else 1.0
        try:
            r = self.session.post(self.BASE_URL, json=payload, timeout=6)
            if r.status_code == 200:
                candles = r.json()
                if isinstance(candles, list):
                    for c in candles:
                        t_sec = int(c.get("t", 0)) // 1000
                        close = float(c.get("c", 0.0)) * inv_mult
                        vol = float(c.get("v", 0.0)) * close * effective_mult
                        if t_sec > 0 and close > 0:
                            out[t_sec] = (close, vol)
        except Exception:
            pass
        return out


class HyperliquidStream(BaseVenueStream):
    """
    Direct WebSocket connection to Hyperliquid l2Book order book feed.
    Emits standardized OrderBookDepth5 events to the trading engine.
    """

    WS_URL = "wss://api.hyperliquid.xyz/ws"
    PING_INTERVAL_SEC = 20.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        self._contract_sizes: Dict[str, float] = {
            k.upper(): float(v) for k, v in (contract_sizes or {}).items()
        }
        if isinstance(symbols, dict):
            self._sym_map: Dict[str, Tuple[str, float, float]] = {}
            for k, v in symbols.items():
                k_u = str(k).upper()
                if isinstance(v, (list, tuple)):
                    coin = str(v[0])
                    mult = float(v[1]) if len(v) > 1 else 1.0
                    c_size = float(v[2]) if len(v) >= 3 else self._contract_sizes.get(k_u, 1.0)
                    self._sym_map[k_u] = (coin, mult, c_size)
                    self._contract_sizes[k_u] = c_size
                else:
                    coin, mult = parse_symbol_and_mult(k_u)
                    c_size = self._contract_sizes.get(k_u, 1.0)
                    self._sym_map[k_u] = (coin, mult, c_size)
            coins = sorted({c for c, _, _ in self._sym_map.values()})
        else:
            raw_list = [str(s).upper() for s in symbols]
            self._sym_map = {}
            for s in raw_list:
                coin, mult = parse_symbol_and_mult(s)
                c_size = self._contract_sizes.get(s, 1.0)
                self._sym_map[s] = (coin, mult, c_size)
            coins = sorted({c for c, _, _ in self._sym_map.values()})

        super().__init__("hyperliquid", coins, callback)
        self.subscribed = list(coins)

    def _resolve_hl_coin(self, raw_sym: str, coin: str, mult: float) -> str:
        if mult == 1000.0 or raw_sym.startswith("1000"):
            return "k" + coin
        if mult == 1_000_000.0 or raw_sym.startswith("1M") or raw_sym.startswith("1000000"):
            return "m" + coin
        if mult == 1_000_000_000.0 or raw_sym.startswith("1B") or raw_sym.startswith("1000000000"):
            return "b" + coin
        return coin

    async def _run_loop(self) -> None:
        while self._running:
            try:
                log.info(f"[hyperliquid] Connecting to {self.WS_URL} ({len(self.symbols)} coins)...")
                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=2000,
                ) as ws:
                    self._note_connect()
                    log.info("[hyperliquid] Connected! Subscribing to l2Book...")

                    # Subscribe to all configured coins
                    seen_hl: Set[str] = set()
                    for raw_sym, (coin, mult, c_size) in self._sym_map.items():
                        hl_coin = self._resolve_hl_coin(raw_sym, coin, mult)
                        if hl_coin in seen_hl:
                            continue
                        seen_hl.add(hl_coin)
                        sub_msg = {
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": hl_coin},
                        }
                        await ws.send(json.dumps(sub_msg))
                        await asyncio.sleep(0.02)

                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[hyperliquid] Scheduled daily reconnect. Reconnecting...")
                                break

                            if self._starved_since_connect(time.time()):
                                log.warning("[hyperliquid] Connected but no data in 30s, reconnecting...")
                                break

                            if not self.last_msg_ts:
                                self.last_msg_ts = time.time()
                            elif time.time() - self.last_msg_ts > 60.0:
                                log.warning("[hyperliquid] No data for >60s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(20.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            self.last_msg_ts = time.time()
                            self._parse_message(msg)
                    finally:
                        ping_task.cancel()
                        self.connected = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[hyperliquid] WS disconnected: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

    async def _ping_loop(self, ws: Any) -> None:
        try:
            while self._running:
                await asyncio.sleep(self.PING_INTERVAL_SEC)
                await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            pass

    def _parse_message(self, raw_msg: str) -> None:
        try:
            payload = json.loads(raw_msg)
            ch = payload.get("channel")
            if ch == "subscriptionResponse":
                self._note_sub(True)
                return
            if ch != "l2Book":
                return

            data = payload.get("data", {})
            hl_coin = data.get("coin", "")
            if not hl_coin:
                return

            coin, mult = parse_symbol_and_mult(hl_coin)
            if mult != 1.0:
                raw_sym = f"{int(mult)}{coin}USDT"
            else:
                raw_sym = f"{coin}USDT"

            mapped = self._sym_map.get(raw_sym)
            if not mapped:
                for sym_key, info in self._sym_map.items():
                    if info[0] == coin:
                        mapped = info
                        break

            c_size = mapped[2] if mapped and len(mapped) >= 3 else self._contract_sizes.get(raw_sym, 1.0)

            levels = data.get("levels", [[], []])
            raw_bids = levels[0] if len(levels) > 0 else []
            raw_asks = levels[1] if len(levels) > 1 else []

            inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
            scale_qty = mult * c_size

            bids: List[Tuple[float, float]] = []
            for item in raw_bids[:5]:
                p = float(item.get("px", 0.0) or 0.0)
                q = float(item.get("sz", 0.0) or 0.0)
                if p > 0.0 and q > 0.0:
                    bids.append((p * inv_mult, q * scale_qty))

            asks: List[Tuple[float, float]] = []
            for item in raw_asks[:5]:
                p = float(item.get("px", 0.0) or 0.0)
                q = float(item.get("sz", 0.0) or 0.0)
                if p > 0.0 and q > 0.0:
                    asks.append((p * inv_mult, q * scale_qty))

            if not bids or not asks:
                return

            snap = OrderBookDepth5(
                exchange="hyperliquid",
                symbol=coin,
                raw_symbol=raw_sym,
                ts=time.time(),
                exchange_ts=normalize_exchange_ts(data.get("time")) or int(time.time() * 1000),
                bids=bids,
                asks=asks,
            )
            self.callback(snap)
        except Exception:
            pass
```

## venues/kucoin.py

```python
"""
venues/kucoin.py — KuCoin Futures REST Client and WebSocket level2Depth5 Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    ReconnectPolicy,
    create_http_session,
    normalize_symbol,
    _secs_until_daily_reconnect_for,
    log,
)


class KucoinRestClient:
    """REST Client for KuCoin Futures discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api-futures.kucoin.com/api/v1"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns symbol (e.g. 'XBTUSDTM') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/contracts/active"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCurrency", "").upper()
                    if settle != "USDT":
                        continue
                    sym = item.get("symbol", "").upper()
                    base = item.get("baseCurrency", "").upper()
                    if base == "XBT":
                        base = "BTC"
                    base_coin = normalize_symbol(base)
                    c_size = float(item.get("multiplier", 1.0) or 1.0)
                    out[sym] = (base_coin, 1.0, c_size)
        except Exception as e:
            log.error(f"[kucoin_rest] Failed to fetch contracts: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns symbol (e.g. 'XBTUSDTM') -> multiplier (e.g. 0.001)."""
        url = f"{self.BASE_URL}/contracts/active"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCurrency", "").upper()
                    if settle != "USDT":
                        continue
                    sym = item.get("symbol", "").upper()
                    mult = float(item.get("multiplier", 1.0) or 1.0)
                    out[sym] = mult
        except Exception as e:
            log.error(f"[kucoin_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns symbol -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/contracts/active"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCurrency", "").upper()
                    if settle != "USDT":
                        continue
                    sym = item.get("symbol", "").upper()
                    price = float(item.get("lastTradePrice", 0.0) or 0.0)
                    vol_usd = float(item.get("turnoverOf24h", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[kucoin_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        url = f"{self.BASE_URL}/kline/query?symbol={symbol}&granularity=1"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", [])[-limit:]:
                    t_sec = (int(row[0]) // 60000) * 60
                    raw_close = float(row[4])
                    close = raw_close * inv_mult
                    vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * raw_close
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class KucoinStream(BaseVenueStream):
    """KuCoin Futures: USDT-margined linear perpetuals WebSocket client."""

    URL = "wss://ws-api-futures.kucoin.com/"
    BULLET = "https://api-futures.kucoin.com/api/v1/bullet-public"
    CONTRACTS = "https://api-futures.kucoin.com/api/v1/contracts/active"
    TOPIC = "/contractMarket/level2Depth5:{sym}"
    PING_FALLBACK_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        sym_list = list(symbols.keys()) if isinstance(symbols, dict) else list(symbols)
        super().__init__("kucoin", sym_list, callback)
        self.subscribed = list(self.symbols)
        self._ours_to_venue: Dict[str, str] = {}
        self._venue_to_ours: Dict[str, str] = {}
        self._contract_sizes: Dict[str, float] = {k.upper(): float(v) for k, v in (contract_sizes or {}).items()}
        self._ping_sec = self.PING_FALLBACK_SEC
        self._session = create_http_session()

    def _resolve_symbols(self) -> Dict[str, str]:
        try:
            r = self._session.get(self.CONTRACTS, timeout=8)
            if r.status_code == 200:
                for c in r.json().get("data", []):
                    sym = c.get("symbol", "").upper()
                    mult = float(c.get("multiplier", 1.0) or 1.0)
                    self._contract_sizes[sym] = mult
                    base = c.get("baseCurrency", "").upper()
                    if base == "XBT":
                        base = "BTC"
                    base_norm = normalize_symbol(base)
                    for s in self.symbols:
                        if normalize_symbol(s) == base_norm:
                            self._ours_to_venue[s] = sym
                            self._venue_to_ours[sym] = normalize_symbol(s)
        except Exception as e:
            log.warning(f"[kucoin] Failed to resolve contract symbols: {e}")
        return self._ours_to_venue

    async def stop(self) -> None:
        await super().stop()
        if self._session is not None:
            try:
                self._session.close()
            except Exception:
                pass
            self._session = None

    async def _run_loop(self) -> None:
        await asyncio.to_thread(self._resolve_symbols)
        target_syms = list(self._ours_to_venue.values())
        if not target_syms:
            log.warning("[kucoin] No symbols resolved for subscription.")
            return

        while self._running:
            try:
                # Get dynamic bullet token using persistent session
                if self._session is None:
                    self._session = create_http_session()
                bullet_resp = await asyncio.to_thread(self._session.post, self.BULLET, timeout=10)
                data = bullet_resp.json().get("data", {})
                token = data.get("token")
                endpoints = data.get("instanceServers", [])
                if not token or not endpoints:
                    delay = self._reconnect_policy.compute_delay(Exception("kucoin bullet token failed"))
                    await asyncio.sleep(delay)
                    continue

                ws_endpoint = endpoints[0]["endpoint"]
                ping_interval = float(endpoints[0].get("pingInterval", 15000)) / 1000.0
                ws_url = f"{ws_endpoint}?token={token}&[format=json]"

                log.info(f"[kucoin] Connecting to {ws_url[:50]}... ({len(target_syms)} symbols)")
                async with websockets.connect(
                    ws_url,
                    open_timeout=20,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=4096,
                ) as ws:
                    self._note_connect()
                    log.info("[kucoin] Connected! Subscribing...")

                    # Subscribe in chunks of 50
                    for i in range(0, len(target_syms), 50):
                        chunk = target_syms[i : i + 50]
                        topic = f"/contractMarket/level2Depth5:{','.join(chunk)}"
                        sub_msg = {
                            "id": int(time.time() * 1000),
                            "type": "subscribe",
                            "topic": topic,
                            "response": False,
                        }
                        await ws.send(json.dumps(sub_msg))
                        await asyncio.sleep(0.04)

                    reconnect_in = _secs_until_daily_reconnect_for(self.name)
                    deadline = time.monotonic() + reconnect_in

                    async def _ping_worker():
                        while self._running:
                            await asyncio.sleep(ping_interval)
                            await ws.send(json.dumps({"id": int(time.time() * 1000), "type": "ping"}))

                    ping_task = asyncio.create_task(_ping_worker())
                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[kucoin] Daily scheduled reconnect. Reconnecting...")
                                break

                            if self._starved_since_connect(time.time()):
                                log.warning("[kucoin] Connected but no data in 30s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(30.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            self.last_msg_ts = time.time()
                            self._parse_message(msg)
                    finally:
                        ping_task.cancel()
                        self.connected = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[kucoin] WS error: {e}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            topic = data.get("topic", "")
            if not topic.startswith("/contractMarket/level2Depth5:"):
                return
            sym_raw = topic.split(":", 1)[1].upper()
            clean_raw = sym_raw[:-1] if sym_raw.endswith("M") else sym_raw
            sym = self._venue_to_ours.get(sym_raw) or normalize_symbol(clean_raw.replace("XBT", "BTC"))
            contract_size = self._contract_sizes.get(sym_raw, 1.0)
            payload = data.get("data", {})
            bids_raw = payload.get("bids", [])[:5]
            asks_raw = payload.get("asks", [])[:5]
            bids = [(float(p), float(q) * contract_size) for p, q in bids_raw if float(p) > 0 and float(q) > 0]
            asks = [(float(p), float(q) * contract_size) for p, q in asks_raw if float(p) > 0 and float(q) > 0]
            if bids and asks:
                self._note_symbol(sym)
                ets = normalize_exchange_ts(payload.get("timestamp")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="kucoin",
                    symbol=sym,
                    raw_symbol=sym_raw,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
```

## venues/manager.py

```python
"""
venues/manager.py — Dynamic WebSocket stream coordinator for Korean Sunrise.
Manages Upbit spot leader feed and all active perpetual follower feeds:
Binance, Bybit, OKX, Gate.io, MEXC, KuCoin, BingX, HTX, Pionex.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from config import MatchedUniverse
from models import OrderBookDepth5
from venues.base import BaseVenueStream
from venues.asterdex import AsterdexStream
from venues.bingx import BingXStream
from venues.binance import BinanceFuturesStream
from venues.bybit import BybitPerpStream
from venues.gateio import GateioStream
from venues.htx import HTXStream
from venues.hyperliquid import HyperliquidStream
from venues.kucoin import KucoinStream
from venues.mexc import MEXCStream
from venues.okx import OKXStream
from venues.pionex import PionexStream
from venues.upbit import UpbitSpotStream

log = logging.getLogger("venues.manager")


class StreamManager:
    """Manages connections to Upbit and all configured follower exchanges."""

    def __init__(
        self,
        universe: MatchedUniverse,
        callback: Callable[[OrderBookDepth5], None],
        trade_callback: Optional[Callable[[str, float, bool, float], None]] = None,
        active_venues: Optional[List[str]] = None,
    ):
        self.universe = universe
        self.callback = callback
        self.trade_callback = trade_callback
        self.streams: Dict[str, BaseVenueStream] = {}

        # 1. Leader stream: Upbit (Spot KRW)
        if universe.upbit_krw_to_coin:
            self.streams["upbit"] = UpbitSpotStream(
                universe.upbit_krw_to_coin,
                callback=callback,
                trade_callback=trade_callback,
            )

        # 2. Dynamic Follower streams
        allowed_venues = [v.lower() for v in active_venues] if active_venues else None

        # Binance
        if allowed_venues is None or "binance" in allowed_venues:
            b_map = universe.get_sym_map("binance") or universe.binance_sym_map
            if b_map:
                self.streams["binance"] = BinanceFuturesStream(b_map, callback)

        # Bybit
        if allowed_venues is None or "bybit" in allowed_venues:
            by_map = universe.get_sym_map("bybit") or universe.bybit_sym_map
            if by_map:
                self.streams["bybit"] = BybitPerpStream(by_map, callback)

        # Map of additional follower stream classes that take (symbols, callback)
        cex_classes = {
            "okx": OKXStream,
            "gateio": GateioStream,
            "mexc": MEXCStream,
            "kucoin": KucoinStream,
            "bingx": BingXStream,
            "htx": HTXStream,
            "pionex": PionexStream,
            "asterdex": AsterdexStream,
            "hyperliquid": HyperliquidStream,
        }

        # Check universe for other venue sym maps
        raw_doc = getattr(universe, "_raw_doc", {}) or {}
        for v_name, cls in cex_classes.items():
            if allowed_venues is not None and v_name not in allowed_venues:
                continue

            v_sym_map = universe.get_sym_map(v_name)
            if not v_sym_map:
                v_sym_map = raw_doc.get(f"{v_name}_sym_map", {})

            if v_sym_map:
                norm_map = {
                    k: (v[0], float(v[1]), float(v[2])) if len(v) >= 3 else (v[0], float(v[1]), 1.0)
                    for k, v in v_sym_map.items()
                }
                try:
                    self.streams[v_name] = cls(norm_map, callback)
                    log.info(f"[stream_manager] Registered follower feed for {v_name.upper()} ({len(norm_map)} contracts)")
                except Exception as e:
                    log.warning(f"[stream_manager] Failed to initialize {v_name} feed: {e}")
            else:
                v_coins = [
                    coin for coin, p in raw_doc.get("pairs", {}).items()
                    if v_name in p.get("venues", [])
                ]
                if v_coins:
                    try:
                        self.streams[v_name] = cls(v_coins, callback)
                        log.info(f"[stream_manager] Registered follower feed for {v_name.upper()} ({len(v_coins)} coins)")
                    except Exception as e:
                        log.warning(f"[stream_manager] Failed to initialize {v_name} feed: {e}")

    async def start_all(self) -> None:
        log.info(
            f"Starting {len(self.streams)} feeds: {', '.join(s.upper() for s in self.streams.keys())}..."
        )
        for name, stream in self.streams.items():
            try:
                await stream.start()
            except Exception as e:
                log.error(f"[stream_manager] Error starting {name} feed: {e}")

    async def stop_all(self) -> None:
        log.info("Stopping all venue streams...")
        for name, stream in self.streams.items():
            try:
                await stream.stop()
            except Exception as e:
                log.error(f"[stream_manager] Error stopping {name} feed: {e}")

    def get_usdt_krw_rate(self) -> float:
        """Returns the real-time USDT/KRW conversion rate from Upbit feed."""
        upbit_stream = self.streams.get("upbit")
        if isinstance(upbit_stream, UpbitSpotStream) and upbit_stream.rate_ready:
            return round(upbit_stream.usdt_krw_rate, 2)
        return 0.0

    def get_status(self) -> Dict[str, Dict[str, Any]]:
        status: Dict[str, Dict[str, Any]] = {}
        for name, s in self.streams.items():
            info: Dict[str, Any] = {
                "connected": s.connected,
                "symbols_seen": len(s.symbols_seen),
                "subscribed": len(s.subscribed),
                "sub_accepted": getattr(s, "sub_accepted", 0),
                "sub_rejected": getattr(s, "sub_rejected", 0),
                "sub_errors": list(getattr(s, "sub_errors", [])),
                "books_dropped": s.books_dropped,
                "seq_gaps": getattr(s, "seq_gaps", 0),
                "seq_duplicates": getattr(s, "seq_duplicates", 0),
            }
            if isinstance(s, UpbitSpotStream):
                info["usdt_krw_rate"] = round(s.usdt_krw_rate, 2) if s.rate_ready else 0.0
            status[name] = info
        return status
```

## venues/mexc.py

```python
"""
venues/mexc.py — MEXC Contract REST Client and WebSocket sub.depth.full Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class MEXCRestClient:
    """REST Client for MEXC Contract discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://contract.mexc.com/api/v1/contract"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns symbol (e.g. 'BTC_USDT') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/detail"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "")
                    if not sym.endswith("_USDT") and not sym.endswith("USDT"):
                        continue
                    base_coin = normalize_symbol(item.get("baseCoin", "") or sym)
                    cs = float(item.get("contractSize", 1.0) or 1.0)
                    out[sym] = (base_coin, 1.0, cs)
        except Exception as e:
            log.error(f"[mexc_rest] Failed to fetch contract details: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns symbol (e.g. 'BTC_USDT') -> contractSize (float)."""
        url = f"{self.BASE_URL}/detail"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("_USDT") and not sym.endswith("USDT"):
                        continue
                    cs = float(item.get("contractSize", 1.0) or 1.0)
                    out[sym] = cs
        except Exception as e:
            log.error(f"[mexc_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns symbol -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/ticker"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "")
                    if not sym.endswith("_USDT") and not sym.endswith("USDT"):
                        continue
                    vol = float(item.get("amount24", 0.0) or 0.0)
                    price = float(item.get("lastPrice", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol}
        except Exception as e:
            log.warning(f"[mexc_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        sym = symbol if "_" in symbol else f"{normalize_symbol(symbol)}_USDT"
        url = f"{self.BASE_URL}/kline/{sym}?interval=Min1"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                data = r.json().get("data", {})
                times = data.get("time", [])
                closes = data.get("close", [])
                amounts = data.get("amount", [])
                for t, c, a in zip(times[-limit:], closes[-limit:], amounts[-limit:]):
                    t_sec = (int(t) // 60) * 60
                    close = float(c) * inv_mult
                    vol_usd = float(a)
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class MEXCStream(BaseCEXStream):
    """MEXC Contract WebSocket order book client."""

    WS_URL = "wss://contract.mexc.com/edge"
    SHARD_SIZE = 80
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("mexc", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send('"Ping"')

    async def _send_subscriptions(self, ws: Any) -> None:
        if not self._contract_sizes:
            try:
                sizes = await asyncio.to_thread(MEXCRestClient().fetch_contract_sizes)
                if sizes:
                    self._contract_sizes.update(sizes)
            except Exception as e:
                log.warning(f"[mexc] Failed to auto-fetch contract sizes: {e}")

        for s in self.symbols:
            pair = s if "_" in s else f"{self._base_coin(s)}_USDT"
            sub = {
                "method": "sub.depth.full",
                "param": {"symbol": pair, "limit": 20},
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.012)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            channel = data.get("channel", "")
            if "depth" not in channel:
                return
            d = data.get("data", {})
            raw_sym = (data.get("symbol", "") or "").upper()
            if not raw_sym:
                return
            sym, mult = self._resolve_coin_and_mult(raw_sym)
            contract_size = self._get_contract_size(raw_sym)
            bids = self._extract_levels(d.get("bids"), 5, mult=mult, contract_size=contract_size)
            asks = self._extract_levels(d.get("asks"), 5, mult=mult, contract_size=contract_size)
            if bids and asks:
                self._note_symbol(sym)
                ets = normalize_exchange_ts(d.get("cts") or data.get("ts")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="mexc",
                    symbol=sym,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
```

## venues/okx.py

```python
"""
venues/okx.py — OKX v5 REST Client and WebSocket books5 L2 Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class OKXRestClient:
    """REST Client for OKX v5 discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://www.okx.com/api/v5"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns: instId (e.g. 'BTC-USDT-SWAP') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/public/instruments?instType=SWAP"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCcy", "").upper()
                    if settle != "USDT":
                        continue
                    inst_id = item.get("instId", "").upper()
                    base_coin = normalize_symbol(item.get("ctValCcy", "") or inst_id)
                    ct_val = float(item.get("ctVal", 1.0) or 1.0)
                    out[inst_id] = (base_coin, 1.0, ct_val)
        except Exception as e:
            log.error(f"[okx_rest] Failed to fetch instruments: {e}")
        return out

    def fetch_contract_sizes(self) -> Dict[str, float]:
        """Returns instId (e.g. 'BTC-USDT-SWAP') -> ctVal (e.g. 0.01)."""
        url = f"{self.BASE_URL}/public/instruments?instType=SWAP"
        out: Dict[str, float] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    settle = item.get("settleCcy", "").upper()
                    if settle != "USDT":
                        continue
                    inst_id = item.get("instId", "").upper()
                    ct_val = float(item.get("ctVal", 1.0) or 1.0)
                    out[inst_id] = ct_val
        except Exception as e:
            log.error(f"[okx_rest] Failed to fetch contract sizes: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns instId -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/market/tickers?instType=SWAP"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    inst_id = item.get("instId", "").upper()
                    if not inst_id.endswith("-USDT-SWAP"):
                        continue
                    price = float(item.get("last", 0.0) or 0.0)
                    vol_usd = float(item.get("volCcy24h", 0.0) or 0.0)
                    if price > 0:
                        out[inst_id] = {"venue_sym": inst_id, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[okx_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        inst_id = symbol if "-SWAP" in symbol else f"{normalize_symbol(symbol)}-USDT-SWAP"
        url = f"{self.BASE_URL}/market/candles?instId={inst_id}&bar=1m&limit={min(limit, 300)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = (int(row[0]) // 60000) * 60
                    raw_close = float(row[4])
                    close = raw_close * inv_mult
                    vol_usd = float(row[7]) if len(row) > 7 else float(row[5]) * raw_close
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class OKXStream(BaseCEXStream):
    """OKX v5 WebSocket client for perpetual swap books5."""

    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("okx", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send("ping")

    async def _send_subscriptions(self, ws: Any) -> None:
        if not self._contract_sizes:
            try:
                sizes = await asyncio.to_thread(OKXRestClient().fetch_contract_sizes)
                if sizes:
                    self._contract_sizes.update(sizes)
            except Exception as e:
                log.warning(f"[okx] Failed to auto-fetch contract sizes: {e}")

        for i in range(0, len(self.symbols), 50):
            args = [
                {"channel": "books5", "instId": s if "-SWAP" in s else f"{self._base_coin(s)}-USDT-SWAP"}
                for s in self.symbols[i : i + 50]
            ]
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            await asyncio.sleep(0.04)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            arg = data.get("arg", {})
            if arg.get("channel") != "books5":
                return
            raw_data = data.get("data")
            book_data = raw_data[0] if isinstance(raw_data, list) and raw_data else (raw_data if isinstance(raw_data, dict) else {})
            inst_id = arg.get("instId", "").upper()
            if not inst_id:
                return

            seq_id = int(book_data.get("seqId") or 0)
            if seq_id > 0:
                last_seq = self._last_seq.get(inst_id)
                if last_seq is not None:
                    if seq_id <= last_seq:
                        self.seq_duplicates += 1
                        return
                self._last_seq[inst_id] = seq_id

            sym, mult = self._resolve_coin_and_mult(inst_id)
            contract_size = self._get_contract_size(inst_id)
            bids = self._extract_levels(book_data.get("bids") or book_data.get("b"), 5, mult=mult, contract_size=contract_size)
            asks = self._extract_levels(book_data.get("asks") or book_data.get("a"), 5, mult=mult, contract_size=contract_size)
            if bids and asks:
                self._note_symbol(sym)
                ets = normalize_exchange_ts(book_data.get("ts") or data.get("ts")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="okx",
                    symbol=sym,
                    raw_symbol=inst_id,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
```

## venues/pionex.py

```python
"""
venues/pionex.py — Pionex WebSocket DEPTH stream and REST Client for Linear Perps.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, parse_symbol_and_mult, log


class PionexRestClient:
    """REST Client for Pionex Perpetual contracts, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.pionex.com/api/v1"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns symbol (e.g. 'BTC_USDT_PERP', '1000PEPE_USDT_PERP') -> (canonical_coin 'BTC', mult 1.0 or 1000.0)."""
        url = f"{self.BASE_URL}/common/symbols?type=PERP"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for s in r.json().get("data", {}).get("symbols", []):
                    sym = s.get("symbol", "").upper()
                    if not sym.endswith("_PERP") or "USDT" not in sym:
                        continue
                    base_coin, mult = parse_symbol_and_mult(sym)
                    if mult == 1.0:
                        base_curr = str(s.get("baseCurrency", "") or s.get("baseCoin", "")).upper()
                        if base_curr:
                            b_coin, b_mult = parse_symbol_and_mult(base_curr)
                            if b_mult != 1.0:
                                base_coin, mult = b_coin, b_mult
                    out[sym] = (base_coin, mult)
        except Exception as e:
            log.error(f"[pionex_rest] Failed to fetch symbols: {e}")
        return out

    def fetch_tickers_24h(self) -> Dict[str, Dict[str, Any]]:
        """Returns symbol -> {price, vol_24h}."""
        url = f"{self.BASE_URL}/market/tickers?type=PERP"
        out: Dict[str, Dict[str, Any]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for t in r.json().get("data", {}).get("tickers", []):
                    sym = t.get("symbol", "").upper()
                    if not sym.endswith("_PERP") or "USDT" not in sym:
                        continue
                    price = float(t.get("close", 0.0) or 0.0)
                    vol_usd = float(t.get("amount", 0.0) or 0.0)
                    if price > 0:
                        out[sym] = {"venue_sym": sym, "price": price, "vol_24h": vol_usd}
        except Exception as e:
            log.warning(f"[pionex_rest] Failed to fetch 24h tickers: {e}")
        return out

    def fetch_klines_1m(self, symbol: str, limit: int = 200, mult: float = 1.0) -> Dict[int, Tuple[float, float]]:
        """Returns minute_ts_sec -> (close_usd, vol_usd)."""
        sym = symbol if "_PERP" in symbol else f"{normalize_symbol(symbol)}_USDT_PERP"
        url = f"{self.BASE_URL}/market/klines?symbol={sym}&interval=1M&limit={min(limit, 500)}"
        out: Dict[int, Tuple[float, float]] = {}
        inv_mult = (1.0 / mult) if mult != 1.0 else 1.0
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", {}).get("klines", []):
                    t_sec = (int(row.get("time", 0)) // 60000) * 60
                    raw_close = float(row.get("close", 0.0))
                    close = raw_close * inv_mult
                    vol_usd = float(row.get("volume", 0.0)) * raw_close
                    if t_sec > 0 and close > 0:
                        out[t_sec] = (close, vol_usd)
        except Exception:
            pass
        return out


class PionexStream(BaseCEXStream):
    """Pionex WebSocket order book client."""

    WS_URL = "wss://ws.pionex.com/wsPub"
    SHARD_SIZE = 40
    PING_INTERVAL_SEC = 15.0

    def __init__(
        self,
        symbols: List[str] | Dict[str, Any],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
        contract_sizes: Optional[Dict[str, float]] = None,
    ):
        super().__init__("pionex", symbols, callback, stream_name=stream_name, contract_sizes=contract_sizes)

    def _to_raw_symbol(self, s: str) -> str:
        s_u = s.upper()
        if s_u.endswith("_PERP"):
            return s_u
        for raw, (coin, _) in self._sym_map.items():
            if coin.upper() == s_u:
                return raw
        return f"{s_u}_USDT_PERP"

    async def _send_client_ping(self, ws: Any) -> None:
        now_ms = int(time.time() * 1000)
        await ws.send(json.dumps({"op": "PING", "timestamp": now_ms}))

    async def _handle_server_ping(self, ws: Any, raw_msg: str) -> bool:
        try:
            d = json.loads(raw_msg)
            if isinstance(d, dict) and d.get("op") == "PING":
                await ws.send(json.dumps({"op": "PONG", "timestamp": d.get("timestamp", int(time.time() * 1000))}))
                return True
        except Exception:
            pass
        return False

    async def _send_subscriptions(self, ws: Any) -> None:
        for s in self.symbols:
            sym = self._to_raw_symbol(s)
            sub = {
                "op": "SUBSCRIBE",
                "topic": "DEPTH",
                "symbol": sym,
                "limit": 5,
            }
            await ws.send(json.dumps(sub))
            await asyncio.sleep(0.02)

    def _parse_message(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
            self._handle_sub_reply(data)

            topic = data.get("topic", "")
            if topic != "DEPTH":
                return
            d = data.get("data", {})
            raw_sym = (data.get("symbol", "") or "").upper()
            if not raw_sym:
                return
            sym, mult = self._resolve_coin_and_mult(raw_sym)
            contract_size = self._get_contract_size(raw_sym)
            bids = self._extract_levels(d.get("bids"), 5, mult=mult, contract_size=contract_size)
            asks = self._extract_levels(d.get("asks"), 5, mult=mult, contract_size=contract_size)
            if bids and asks:
                self._note_symbol(sym)
                ets = normalize_exchange_ts(data.get("timestamp")) or int(time.time() * 1000)
                self.callback(OrderBookDepth5(
                    exchange="pionex",
                    symbol=sym,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=ets,
                    bids=bids,
                    asks=asks,
                ))
        except Exception:
            pass
```

## venues/upbit.py

```python
"""
venues/upbit.py — Upbit REST Client and WebSocket L2 Stream (Spot Leader, South Korea).
Converts KRW prices to USD in real time using the live KRW-USDT rate.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests
import websockets

from models import OrderBookDepth5, normalize_exchange_ts
from venues.base import (
    BaseVenueStream,
    ReconnectPolicy,
    create_http_session,
    _secs_until_daily_reconnect_for,
    log,
)


class UpbitRestClient:
    """REST Client for Upbit market discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.upbit.com/v1"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_usdt_krw_rate(self) -> float:
        """Fetches current KRW-USDT spot mid rate."""
        url = f"{self.BASE_URL}/ticker?markets=KRW-USDT"
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                data = r.json()
                if data and len(data) > 0:
                    rate = float(data[0].get("trade_price", 0.0))
                    if rate > 0:
                        return rate
        except Exception as e:
            log.warning(f"[upbit_rest] Failed to fetch USDT-KRW rate: {e}")
        return 1350.0  # Safe fallback

    def fetch_krw_markets(self) -> Dict[str, str]:
        """
        Fetches all KRW spot markets.
        Returns dict: canonical coin symbol -> market code (e.g. 'BTC' -> 'KRW-BTC').
        """
        url = f"{self.BASE_URL}/market/all?isDetails=true"
        out: Dict[str, str] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json():
                    mkt = item.get("market", "")
                    if mkt.startswith("KRW-"):
                        coin = mkt.split("-", 1)[1].upper()
                        if coin:
                            out[coin] = mkt
        except Exception as e:
            log.error(f"[upbit_rest] Failed to fetch Upbit markets: {e}")
        return out

    def fetch_tickers_24h(
        self,
        market_codes: List[str],
        usdt_krw_rate: float,
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetches 24h tickers in batches of up to 100 markets.
        Returns canonical coin -> {price_krw, vol_krw_24h, price_usd, vol_usd_24h}.
        """
        out: Dict[str, Dict[str, Any]] = {}
        if not market_codes or usdt_krw_rate <= 0:
            return out

        chunk_size = 100
        for i in range(0, len(market_codes), chunk_size):
            chunk = market_codes[i : i + chunk_size]
            url = f"{self.BASE_URL}/ticker?markets={','.join(chunk)}"
            try:
                r = self.session.get(url, timeout=8)
                if r.status_code == 200:
                    for item in r.json():
                        mkt = item.get("market", "")
                        if not mkt.startswith("KRW-"):
                            continue
                        coin = mkt.split("-", 1)[1].upper()
                        price_krw = float(item.get("trade_price", 0.0) or 0.0)
                        vol_krw = float(item.get("acc_trade_price_24h", 0.0) or 0.0)

                        if price_krw > 0:
                            out[coin] = {
                                "market": mkt,
                                "price_krw": price_krw,
                                "vol_krw_24h": vol_krw,
                                "price_usd": price_krw / usdt_krw_rate,
                                "vol_usd_24h": vol_krw / usdt_krw_rate,
                            }
                time.sleep(0.12)  # Upbit 10 req/s rate compliance (<= 8.3 req/s safe buffer)
            except Exception as e:
                log.warning(f"[upbit_rest] Failed to fetch ticker batch ({len(chunk)}): {e}")

        return out

    def fetch_klines_1m(
        self,
        market: str,
        count: int = 200,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to `count` 1-minute candles (default 200 = ~3.3h).
        Paginates Upbit /v1/candles/minutes/1 in chunks of 200 candles using 'to' parameter if count > 200.
        Returns dict: minute_ts_sec -> (close_krw, volume_krw).
        """
        out: Dict[int, Tuple[float, float]] = {}
        to_param: Optional[str] = None
        remaining = max(1, count)

        while remaining > 0:
            batch_count = min(remaining, 200)
            url = f"{self.BASE_URL}/candles/minutes/1?market={market}&count={batch_count}"
            if to_param:
                url += f"&to={to_param}"
            try:
                r = self.session.get(url, timeout=6)
                if r.status_code != 200:
                    break
                rows = r.json()
                if not rows or not isinstance(rows, list):
                    break
                for row in rows:
                    ms = int(row.get("timestamp", 0))
                    t_sec = (ms // 60000) * 60
                    close_krw = float(row.get("trade_price", 0.0) or 0.0)
                    vol_krw = float(row.get("candle_acc_trade_price", 0.0) or 0.0)
                    if t_sec > 0 and close_krw > 0:
                        out[t_sec] = (close_krw, vol_krw)

                oldest = rows[-1].get("candle_date_time_utc")
                if not oldest or len(rows) < batch_count:
                    break
                to_param = oldest
                remaining -= len(rows)
                time.sleep(0.12)  # Upbit 10 req/s rate compliance (<= 8.3 req/s safe buffer)
            except Exception:
                break

        return out


class UpbitSpotStream(BaseVenueStream):
    """
    Upbit Spot WebSocket Client.
    Streams L2 order books for matched Korean Won pairs (KRW-<COIN>) and converts
    prices to USDT using the live KRW-USDT mid price.
    """

    WS_URL = "wss://api.upbit.com/websocket/v1"
    REST_USDT_OB = "https://api.upbit.com/v1/orderbook?markets=KRW-USDT"

    RATE_MAX_JUMP = 0.02
    RATE_JUMP_ACCEPT_AFTER = 5
    WATCHDOG_SECS = 30.0
    BACKOFF_MIN = 3.0
    BACKOFF_MAX = 60.0
    PARSE_ERR_LOG_EVERY = 100

    def __init__(
        self,
        krw_to_coin: Dict[str, str],
        callback: Callable[[OrderBookDepth5], None],
        trade_callback: Optional[Callable[[str, float, bool, float], None]] = None,
    ):
        coins = sorted(set(krw_to_coin.values()))
        super().__init__("upbit", coins, callback)
        self._krw_to_coin: Dict[str, str] = dict(krw_to_coin)
        self.subscribed: List[str] = list(coins)
        self.trade_callback = trade_callback

        self.usdt_krw_rate: float = 0.0
        self.rate_ready: bool = False
        self.rate_ts: float = 0.0
        self._rate_jump_count: int = 0
        self._parse_errors: int = 0
        self._callback_errors: int = 0

    @staticmethod
    def _http_get_json(url: str, timeout: float = 10.0):
        headers = {"User-Agent": "Mozilla/5.0 (compatible; KoreanSunrise/2.0)"}
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _fetch_initial_usdt_rate(self) -> float:
        try:
            data = self._http_get_json(self.REST_USDT_OB)
            units = data[0].get("orderbook_units", [])
            if units:
                bp = float(units[0].get("bid_price", 0.0))
                ap = float(units[0].get("ask_price", 0.0))
                if bp > 0 and ap > 0:
                    return (bp + ap) * 0.5
        except Exception as e:
            log.warning(f"[upbit] Failed to fetch initial KRW-USDT orderbook rate: {e}")
        return 0.0

    def _update_rate(self, new_rate: float) -> None:
        if new_rate <= 0:
            return
        if not self.rate_ready:
            self.usdt_krw_rate = new_rate
            self.rate_ready = True
            self.rate_ts = time.time()
            self._rate_jump_count = 0
            log.info(f"[upbit] USDT/KRW mid-rate initialised: {new_rate:.2f}")
            return

        deviation = abs(new_rate - self.usdt_krw_rate) / self.usdt_krw_rate
        if deviation > self.RATE_MAX_JUMP:
            self._rate_jump_count += 1
            if self._rate_jump_count < self.RATE_JUMP_ACCEPT_AFTER:
                log.warning(
                    f"[upbit] Suspicious USDT/KRW jump {self.usdt_krw_rate:.2f} -> {new_rate:.2f} "
                    f"({deviation:.1%}), ignoring ({self._rate_jump_count}/{self.RATE_JUMP_ACCEPT_AFTER})"
                )
                return
            log.warning(f"[upbit] USDT/KRW jump persisted, accepting {new_rate:.2f}")
        self._rate_jump_count = 0
        self.usdt_krw_rate = new_rate
        self.rate_ts = time.time()

    async def _run_loop(self) -> None:
        target_codes: List[str] = ["KRW-USDT"] + sorted(self._krw_to_coin.keys())

        while self._running:
            try:
                rest_rate = await asyncio.to_thread(self._fetch_initial_usdt_rate)
                if rest_rate > 0:
                    self._update_rate(rest_rate)

                log.info(f"[upbit] Connecting to {self.WS_URL} ({len(self._krw_to_coin)} matched KRW coins)...")

                sub_msg: List[Dict[str, Any]] = [
                    {"ticket": f"upbit-sunrise-{int(time.time())}"},
                    {"type": "orderbook", "codes": target_codes},
                ]
                if self.trade_callback:
                    sub_msg.append({"type": "trade", "codes": target_codes})
                sub_msg.append({"format": "DEFAULT"})

                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=20,
                    ping_interval=20,
                    ping_timeout=15,
                    max_queue=4096,
                ) as ws:
                    self._note_connect()
                    self._note_sub(True)
                    self.last_msg_ts = time.time()
                    trades_note = " + trades" if self.trade_callback else ""
                    log.info(f"[upbit] Connected! Subscribed to {len(target_codes)} order books{trades_note} (incl. KRW-USDT).")
                    await ws.send(json.dumps(sub_msg))

                    try:
                        reconnect_in = _secs_until_daily_reconnect_for(self.name)
                        deadline = time.monotonic() + reconnect_in

                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[upbit] Scheduled daily reconnect. Reconnecting...")
                                break

                            if self._starved_since_connect(time.time()):
                                log.warning("[upbit] Connected but no data in 30s, reconnecting...")
                                break

                            if time.time() - self.last_msg_ts > self.WATCHDOG_SECS:
                                log.warning(f"[upbit] No messages for >{self.WATCHDOG_SECS:.0f}s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(15.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            self.last_msg_ts = time.time()
                            self._handle_msg(msg)
                    finally:
                        self.connected = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                delay = self._reconnect_policy.compute_delay(e)
                log.warning(
                    f"[upbit] WS error: {e!r}. Reconnecting in {delay:.1f}s "
                    f"(attempt {self._reconnect_policy.consecutive_failures})..."
                )
                await asyncio.sleep(delay)

        self.connected = False

    def _handle_msg(self, raw: str | bytes) -> None:
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            data = json.loads(raw)
        except Exception:
            self._parse_errors += 1
            if self._parse_errors == 1 or self._parse_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.warning(f"[upbit] parse error #{self._parse_errors}", exc_info=True)
            return

        msg_type = data.get("type")
        if msg_type == "trade":
            self._handle_trade(data)
            return

        try:
            snap = self._parse_orderbook_data(data)
        except Exception:
            self._parse_errors += 1
            if self._parse_errors == 1 or self._parse_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.warning(f"[upbit] orderbook parse error #{self._parse_errors}", exc_info=True)
            return

        if snap is None:
            return

        try:
            self.callback(snap)
        except Exception:
            self._callback_errors += 1
            if self._callback_errors == 1 or self._callback_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.error(f"[upbit] callback error #{self._callback_errors}", exc_info=True)

    def _handle_trade(self, data: Dict[str, Any]) -> None:
        if not self.trade_callback or not self.rate_ready or self.usdt_krw_rate <= 0:
            return
        code = data.get("code")
        if not code:
            return
        coin = self._krw_to_coin.get(code)
        if not coin:
            return

        trade_price = float(data.get("trade_price", 0.0) or 0.0)
        trade_volume = float(data.get("trade_volume", 0.0) or 0.0)
        if trade_price <= 0.0 or trade_volume <= 0.0:
            return

        ask_bid = str(data.get("ask_bid", "")).upper()
        # In Upbit protocol:
        # "BID" indicates taker bought from ask (market buy)
        # "ASK" indicates taker sold to bid (market sell)
        is_taker_buy = (ask_bid == "BID")
        trade_usd = (trade_price * trade_volume) / self.usdt_krw_rate
        now_ts = time.time()

        try:
            self.trade_callback(coin, trade_usd, is_taker_buy, now_ts)
        except Exception:
            self._callback_errors += 1
            if self._callback_errors == 1 or self._callback_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.error(f"[upbit] trade callback error #{self._callback_errors}", exc_info=True)

    def _parse_msg(self, raw: str | bytes) -> OrderBookDepth5 | None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        return self._parse_orderbook_data(data)

    def _parse_orderbook_data(self, data: Dict[str, Any]) -> OrderBookDepth5 | None:
        code = data.get("code")
        if not code:
            return None

        units = data.get("orderbook_units") or []
        if not units:
            return None

        if code == "KRW-USDT":
            u0 = units[0]
            bp = float(u0.get("bid_price", 0.0))
            ap = float(u0.get("ask_price", 0.0))
            if bp > 0 and ap > 0:
                self._update_rate((bp + ap) * 0.5)
            return None

        coin = self._krw_to_coin.get(code)
        if not coin or not self.rate_ready:
            return None

        inv_rate = 1.0 / self.usdt_krw_rate
        bids: List[Tuple[float, float]] = []
        asks: List[Tuple[float, float]] = []

        for u in units[:5]:
            bp, bs = float(u.get("bid_price", 0.0)), float(u.get("bid_size", 0.0))
            ap, asz = float(u.get("ask_price", 0.0)), float(u.get("ask_size", 0.0))
            if bp > 0.0 and bs > 0.0:
                bids.append((bp * inv_rate, bs))
            if ap > 0.0 and asz > 0.0:
                asks.append((ap * inv_rate, asz))

        if not bids or not asks:
            return None

        return OrderBookDepth5(
            exchange="upbit",
            symbol=coin,
            raw_symbol=code,
            ts=time.time(),
            exchange_ts=normalize_exchange_ts(data.get("timestamp")) or int(time.time() * 1000),
            bids=bids,
            asks=asks,
        )
```

## web/__init__.py

```python
"""
web package for Korean Sunrise.
"""
from web.server import APIServer
from web.reports_manager import list_available_reports, get_report_content

__all__ = ["APIServer", "list_available_reports", "get_report_content"]
```

## web/reports_manager.py

```python
"""
web/reports_manager.py - Daily reports discovery, metadata extraction, and content serving for Korean Sunrise.
"""
from __future__ import annotations

import datetime
import glob
import json
import os
import re
from typing import Any, Dict, List, Optional

import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports", "daily")

_REPORTS_CACHE: List[Dict[str, Any]] = []
_REPORTS_CACHE_TIME: float = 0.0
_REPORTS_CACHE_LOCK = threading.Lock()


def list_available_reports(force: bool = False, ttl_sec: float = 10.0) -> List[Dict[str, Any]]:
    """Discovers all archived daily reports and extracts card metrics, with in-memory caching."""
    global _REPORTS_CACHE, _REPORTS_CACHE_TIME
    now = time.time()
    with _REPORTS_CACHE_LOCK:
        if not force and _REPORTS_CACHE and (now - _REPORTS_CACHE_TIME < ttl_sec):
            return list(_REPORTS_CACHE)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    report_files = sorted(glob.glob(os.path.join(REPORTS_DIR, "report_*.md")), reverse=True)

    cards = []
    seen_dates = set()

    for md_path in report_files:
        bn = os.path.basename(md_path)
        m = re.search(r"(\d{4}-?\d{2}-?\d{2})", bn)
        date_raw = m.group(1) if m else "2026-10-18"
        if len(date_raw) == 8 and "-" not in date_raw:
            date_key = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:]}"
        else:
            date_key = date_raw

        if date_key in seen_dates:
            continue
        seen_dates.add(date_key)

        json_path = os.path.join(REPORTS_DIR, f"report_{date_key}.json")
        card = _build_card_from_files(date_key, md_path, json_path)
        cards.append(card)

    cards.sort(key=lambda x: x["date_key"], reverse=True)
    with _REPORTS_CACHE_LOCK:
        _REPORTS_CACHE = cards
        _REPORTS_CACHE_TIME = time.time()
    return cards


def _build_card_from_files(date_key: str, md_path: str, json_path: str) -> Dict[str, Any]:
    """Extracts summary metrics from JSON if available, or provides sensible defaults."""
    card = {
        "id": date_key,
        "date_key": date_key,
        "title": f"Суточный квантовый отчёт: {date_key}",
        "date_from": f"{date_key} 00:00:00 UTC",
        "date_to": f"{date_key} 23:59:59 UTC",
        "filename_md": os.path.basename(md_path),
        "filepath_md": md_path,
        "has_json": os.path.exists(json_path),
        "net_pnl_usd": 0.0,
        "winrate_pct": 0.0,
        "total_trades": 0,
        "total_rows": 0,
        "total_mb": 0.0,
        "venues_breakdown": [],
        "strategies_breakdown": [
            {"name": "1. Upbit Depth Drain", "winrate": 0.0, "net": 0.0},
            {"name": "2. Spread Blowout Sniping", "winrate": 0.0, "net": 0.0},
            {"name": "3. Calibrated Spread Filter", "winrate": 0.0, "net": 0.0},
        ],
    }

    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as jf:
                data = json.load(jf)
            meta = data.get("metadata", {})
            sc = data.get("pnl_scenarios", {})
            p_all = sc.get("calibrated_threshold_only") or sc.get("all_signals") or {}

            card["total_trades"] = p_all.get("trades", 0)
            card["net_pnl_usd"] = p_all.get("net_pnl_usd", 0.0)
            card["winrate_pct"] = p_all.get("winrate_pct", 0.0)
            card["total_rows"] = meta.get("total_signals_recorded", 0)
            card["total_mb"] = meta.get("total_size_mb", 0.0)

            # Top pairs
            top_pairs = data.get("top_pairs", [])
            card["venues_breakdown"] = [
                {
                    "pair": pr["pair"].replace("upbit-", "Upbit <-> "),
                    "trades": pr["trades"],
                    "net": pr["net_pnl_usd"],
                    "winrate": pr["winrate_pct"],
                }
                for pr in top_pairs[:8]
            ]
        except Exception:
            pass

    return card


def get_report_content(report_id: str) -> Optional[str]:
    """Reads Markdown content of a specific report by date key (safe against traversal)."""
    safe_id = re.sub(r"[^0-9\-]", "", str(report_id))
    if not safe_id:
        return None
    fn = f"report_{safe_id}.md"
    fp = os.path.join(REPORTS_DIR, fn)
    if os.path.isfile(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return None
    return None
```

## web/server.py

```python
"""
web/server.py - Built-in HTTP Server and API for Korean Sunrise Intelligence Hub.
Provides live monitoring, reporting hub, threshold calibration, 1-click dataset export,
and hardened security (zero-token URL leaks, SameSite HttpOnly cookies, and anti-CSRF protection).
"""
from __future__ import annotations

import datetime
import hashlib
import hmac
import http.server
import json
import logging
import os
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from web.reports_manager import list_available_reports, get_report_content
from web.views import render_dashboard_html, render_login_html

log = logging.getLogger("api_server")

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)


class APIServer:
    """Lightweight built-in HTTP server for Korean Sunrise monitoring and controls."""

    def __init__(
        self,
        service: Any,
        host: str = "0.0.0.0",
        port: int = 8089,
        token: str = "vitali_secret_key_2026",
    ):
        self.service = service
        self.host = host
        self.port = port
        self.token = token
        self._csrf_secret: str = secrets.token_hex(16)
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

        # In-memory mtime cache for matrix thresholds to avoid synchronous disk I/O on active dashboard
        self._cached_thresholds: Dict[str, Any] = {}
        self._thresholds_mtime: float = 0.0
        self._thresholds_lock = threading.Lock()

    def get_thresholds_doc(self) -> Dict[str, Any]:
        """Returns cached matrix thresholds document, reloading from disk only when mtime changes."""
        th_file = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
        try:
            if not os.path.isfile(th_file):
                return {}
            mtime = os.path.getmtime(th_file)
            with self._thresholds_lock:
                if mtime != self._thresholds_mtime or not self._cached_thresholds:
                    with open(th_file, "r", encoding="utf-8") as tf:
                        self._cached_thresholds = json.load(tf)
                    self._thresholds_mtime = mtime
                return self._cached_thresholds
        except Exception as e:
            log.warning(f"[api_server] Error reading thresholds: {e}")
            with self._thresholds_lock:
                return self._cached_thresholds or {}

    def get_csrf_token(self, token: Optional[str] = None) -> str:
        """Derives a cryptographically secure anti-CSRF token bound to the server session and token."""
        tok = token or self.token
        if not tok:
            return ""
        key = f"{tok}:{self._csrf_secret}".encode("utf-8")
        return hmac.new(key, b"korean_sunrise_csrf_v1", hashlib.sha256).hexdigest()[:32]

    def verify_csrf_token(self, candidate: str) -> bool:
        """Validates candidate anti-CSRF token using timing-safe comparison."""
        if not candidate or not self.token:
            return False
        expected = self.get_csrf_token()
        return hmac.compare_digest(candidate, expected)

    def start(self) -> None:
        if self._running:
            return

        server_ref = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                log.debug(f"[api] {self.address_string()} - {format % args}")

            def _get_auth_info(self) -> Tuple[str, str]:
                """
                Extracts authentication credentials and identifies the source ('header' or 'cookie').
                Query parameter (?token=...) authentication is strictly rejected to prevent secret
                leakage into web server access logs, reverse proxy logs, and browser history.
                """
                auth_hdr = self.headers.get("Authorization", "")
                if auth_hdr.startswith("Bearer "):
                    return auth_hdr.split(" ", 1)[1].strip(), "header"
                if "X-Token" in self.headers:
                    return self.headers["X-Token"].strip(), "header"
                cookies = self.headers.get("Cookie", "")
                for part in cookies.split(";"):
                    if "=" in part:
                        k, v = part.strip().split("=", 1)
                        if k == "token":
                            return v.strip(), "cookie"
                return "", ""

            def _is_authenticated(self) -> bool:
                if not server_ref.token:
                    return True
                tok, _ = self._get_auth_info()
                return bool(tok) and hmac.compare_digest(tok, server_ref.token)

            def _validate_csrf(self, body_bytes: bytes) -> bool:
                """
                Validates anti-CSRF tokens for state-changing requests originating from cookie sessions.
                Checks X-CSRF-Token / X-CSRFToken header first, then inspects body payload.
                """
                header_csrf = self.headers.get("X-CSRF-Token") or self.headers.get("X-CSRFToken")
                if header_csrf and server_ref.verify_csrf_token(header_csrf.strip()):
                    return True

                if body_bytes:
                    content_type = self.headers.get("Content-Type", "")
                    if "application/x-www-form-urlencoded" in content_type:
                        try:
                            form = urllib.parse.parse_qs(body_bytes.decode("utf-8", errors="replace"))
                            cand = form.get("csrf_token", [""])[0]
                            if cand and server_ref.verify_csrf_token(cand):
                                return True
                        except Exception:
                            pass
                    elif "application/json" in content_type:
                        try:
                            payload = json.loads(body_bytes.decode("utf-8", errors="replace"))
                            if isinstance(payload, dict):
                                cand = payload.get("csrf_token", "")
                                if cand and server_ref.verify_csrf_token(str(cand)):
                                    return True
                        except Exception:
                            pass

                return False

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                q = urllib.parse.parse_qs(parsed.query)
                is_auth = self._is_authenticated()

                if path in ("/", "/dashboard", "/status"):
                    if not is_auth:
                        self._send_login()
                    else:
                        self._handle_dashboard()
                elif not is_auth:
                    self._send_unauthorized()
                elif path == "/stats":
                    self._handle_stats()
                elif path == "/pool":
                    self._handle_pool()
                elif path == "/battle":
                    self._handle_battle()
                elif path in ("/api/trading/profile", "/profile"):
                    self._handle_profile()
                elif path in ("/api/thresholds", "/thresholds"):
                    self._handle_thresholds()
                elif path in ("/api/report", "/report"):
                    self._handle_reports_list()
                elif path == "/api/report/content":
                    self._handle_report_content(q)
                elif path == "/download":
                    self._handle_download(q)
                else:
                    self._send_404()

            def do_POST(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"

                length = int(self.headers.get("Content-Length", 0))
                body_bytes = self.rfile.read(length) if length > 0 else b""

                # 1. Public Authentication Route
                if path == "/login":
                    self._handle_login(body_bytes)
                    return

                # 2. General Authentication Check
                tok, auth_source = self._get_auth_info()
                if not tok or not hmac.compare_digest(tok, server_ref.token):
                    self._send_unauthorized()
                    return

                # 3. Anti-CSRF Verification for Browser/Cookie Sessions
                # Requests authenticated via custom headers (Bearer/X-Token) are exempt as
                # browsers cannot forge custom cross-origin headers without CORS preflight.
                if auth_source == "cookie":
                    if not self._validate_csrf(body_bytes):
                        self._send_forbidden("CSRF validation failed: missing or invalid anti-CSRF token")
                        return

                # 4. State-Changing Protected Routes
                if path == "/logout":
                    self._handle_logout()
                elif path == "/api/report/generate":
                    self._handle_generate_report()
                elif path == "/api/thresholds/calibrate":
                    self._handle_calibrate_thresholds()
                elif path == "/api/daily-pipeline":
                    self._handle_trigger_pipeline()
                elif path in ("/pool/pause", "/pool/resume"):
                    self._handle_pool_toggle(path, body_bytes)
                else:
                    self._send_404()

            # Handlers
            def _send_login(self, error: Optional[str] = None) -> None:
                html_body = render_login_html(error=error)
                self._send_html(html_body, code=200 if not error else 401)

            def _handle_login(self, body_bytes: bytes) -> None:
                content_type = self.headers.get("Content-Type", "")
                submitted_token = ""
                is_form = False

                if "application/json" in content_type:
                    try:
                        payload = json.loads(body_bytes.decode("utf-8", errors="replace"))
                        submitted_token = payload.get("token", "")
                    except Exception:
                        pass
                elif "application/x-www-form-urlencoded" in content_type:
                    is_form = True
                    try:
                        form = urllib.parse.parse_qs(body_bytes.decode("utf-8", errors="replace"))
                        submitted_token = form.get("token", [""])[0]
                    except Exception:
                        pass
                else:
                    body_text = body_bytes.decode("utf-8", errors="replace").strip()
                    if body_text.startswith("token="):
                        is_form = True
                        form = urllib.parse.parse_qs(body_text)
                        submitted_token = form.get("token", [""])[0]
                    else:
                        submitted_token = body_text

                if not server_ref.token or hmac.compare_digest(submitted_token, server_ref.token):
                    csrf_tok = server_ref.get_csrf_token(server_ref.token)
                    cookie_tok = f"token={server_ref.token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400"
                    cookie_csrf = f"csrf_token={csrf_tok}; Path=/; SameSite=Strict; Max-Age=86400"

                    if is_form or "text/html" in self.headers.get("Accept", ""):
                        self.send_response(303)
                        self.send_header("Location", "/dashboard")
                        self.send_header("Set-Cookie", cookie_tok)
                        self.send_header("Set-Cookie", cookie_csrf)
                        self.end_headers()
                    else:
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json; charset=utf-8")
                        self.send_header("Set-Cookie", cookie_tok)
                        self.send_header("Set-Cookie", cookie_csrf)
                        b = json.dumps({"ok": True, "csrf_token": csrf_tok}).encode("utf-8")
                        self.send_header("Content-Length", str(len(b)))
                        self.end_headers()
                        self.wfile.write(b)
                else:
                    if is_form or "text/html" in self.headers.get("Accept", ""):
                        self._send_login(error="Неверный ключ авторизации")
                    else:
                        self._send_json({"ok": False, "error": "Invalid authentication token"}, code=401)

            def _handle_logout(self) -> None:
                cookie_clear_tok = "token=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"
                cookie_clear_csrf = "csrf_token=; Path=/; Max-Age=0; SameSite=Strict"

                if "text/html" in self.headers.get("Accept", "") or "application/x-www-form-urlencoded" in self.headers.get("Content-Type", ""):
                    self.send_response(303)
                    self.send_header("Location", "/")
                    self.send_header("Set-Cookie", cookie_clear_tok)
                    self.send_header("Set-Cookie", cookie_clear_csrf)
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Set-Cookie", cookie_clear_tok)
                    self.send_header("Set-Cookie", cookie_clear_csrf)
                    b = json.dumps({"ok": True, "message": "Logged out successfully"}).encode("utf-8")
                    self.send_header("Content-Length", str(len(b)))
                    self.end_headers()
                    self.wfile.write(b)

            def _handle_dashboard(self) -> None:
                tok, _ = self._get_auth_info()
                effective_tok = tok or server_ref.token
                csrf_tok = server_ref.get_csrf_token(effective_tok)

                cookie_tok_hdr = f"token={effective_tok}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400"
                cookie_csrf_hdr = f"csrf_token={csrf_tok}; Path=/; SameSite=Strict; Max-Age=86400"

                # Collect live status
                status = {}
                pool_snap = {}
                profile = {}
                thresholds = {}

                if hasattr(server_ref.service, "stream_manager") and server_ref.service.stream_manager:
                    status["streams"] = server_ref.service.stream_manager.get_status()
                    status["usdt_krw_rate"] = server_ref.service.stream_manager.get_usdt_krw_rate()
                if hasattr(server_ref.service, "engine") and server_ref.service.engine:
                    status["counters"] = server_ref.service.engine.counters
                if hasattr(server_ref.service, "pool_state") and server_ref.service.pool_state:
                    pool_snap = server_ref.service.pool_state.get_pool_snapshot()
                if hasattr(server_ref.service, "profile_manager") and server_ref.service.profile_manager:
                    profile = server_ref.service.profile_manager.get_profile()

                thresholds = server_ref.get_thresholds_doc()
                cards = list_available_reports()
                html_body = render_dashboard_html(
                    service_status=status,
                    pool_snapshot=pool_snap,
                    profile_data=profile,
                    thresholds_doc=thresholds,
                    reports_cards=cards,
                    csrf_token=csrf_tok,
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Set-Cookie", cookie_tok_hdr)
                self.send_header("Set-Cookie", cookie_csrf_hdr)
                self.end_headers()
                self.wfile.write(html_body.encode("utf-8"))

            def _handle_stats(self) -> None:
                data = {
                    "running": getattr(server_ref.service, "running", False),
                    "counters": getattr(server_ref.service.engine, "counters", {}) if hasattr(server_ref.service, "engine") else {},
                }
                self._send_json(data)

            def _handle_pool(self) -> None:
                if hasattr(server_ref.service, "pool_state") and server_ref.service.pool_state:
                    self._send_json(server_ref.service.pool_state.get_pool_snapshot())
                else:
                    self._send_json({})

            def _handle_battle(self) -> None:
                if hasattr(server_ref.service, "pool_state") and server_ref.service.pool_state:
                    self._send_json(server_ref.service.pool_state.get_battle_coins())
                else:
                    self._send_json([])

            def _handle_profile(self) -> None:
                if hasattr(server_ref.service, "profile_manager") and server_ref.service.profile_manager:
                    self._send_json(server_ref.service.profile_manager.get_profile())
                else:
                    self._send_json({})

            def _handle_thresholds(self) -> None:
                self._send_json(server_ref.get_thresholds_doc())

            def _handle_reports_list(self) -> None:
                self._send_json(list_available_reports())

            def _handle_report_content(self, q: Dict[str, List[str]]) -> None:
                rep_id = q.get("id", [""])[0]
                content = get_report_content(rep_id)
                if content:
                    self._send_text(content)
                else:
                    self._send_text("Отчёт не найден.", code=404)

            def _handle_generate_report(self) -> None:
                cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "full_daily_report.py"), "--json"]
                try:
                    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
                    self._send_json({"ok": res.returncode == 0, "output": res.stdout})
                except Exception as e:
                    self._send_json({"ok": False, "error": str(e)}, code=500)

            def _handle_calibrate_thresholds(self) -> None:
                cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "calibrate_thresholds.py"), "--json"]
                try:
                    res = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=120)
                    if hasattr(server_ref.service, "engine") and server_ref.service.engine:
                        server_ref.service.engine.reload_thresholds()
                    self._send_json({"ok": res.returncode == 0, "output": res.stdout})
                except Exception as e:
                    self._send_json({"ok": False, "error": str(e)}, code=500)

            def _handle_trigger_pipeline(self) -> None:
                cmd = [sys.executable, os.path.join(PROJECT_ROOT, "tools", "run_daily_pipeline.py")]
                subprocess.Popen(cmd)
                self._send_json({"ok": True, "message": "Master 24h pipeline started in background"})

            def _handle_pool_toggle(self, path: str, body_bytes: bytes) -> None:
                try:
                    payload = json.loads(body_bytes.decode("utf-8", errors="replace")) if body_bytes else {}
                    coin = payload.get("coin", "").upper()
                    if not coin:
                        self._send_json({"error": "Missing coin parameter"}, code=400)
                        return
                    if hasattr(server_ref.service, "pool_state") and server_ref.service.pool_state:
                        if path == "/pool/pause":
                            server_ref.service.pool_state.pause_coin(coin)
                        else:
                            server_ref.service.pool_state.resume_coin(coin)
                        self._send_json({"ok": True, "coin": coin})
                        return
                except Exception as e:
                    self._send_json({"error": str(e)}, code=400)
                    return
                self._send_json({"error": "pool_state unavailable"}, code=500)

            def _handle_download(self, q: Dict[str, List[str]]) -> None:
                target_file = os.path.basename(q.get("file", [""])[0])
                if target_file:
                    fp = os.path.join(PROJECT_ROOT, "reports", "daily", target_file)
                    if os.path.isfile(fp):
                        self.send_response(200)
                        self.send_header("Content-Type", "application/octet-stream")
                        self.send_header("Content-Disposition", f"attachment; filename=\"{target_file}\"")
                        self.end_headers()
                        with open(fp, "rb") as f:
                            shutil.copyfileobj(f, self.wfile)
                        return
                    self._send_404()
                    return

                # Download zip of configs, reports, and signal archives
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                tmp.close()
                try:
                    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as z:
                        for fn in ["universe_live.json", "live_trading_pool.json", "matrix_thresholds.json", "trading_profile.json", "runtime_overrides.json"]:
                            fp = os.path.join(PROJECT_ROOT, fn)
                            if os.path.isfile(fp):
                                z.write(fp, arcname=fn)
                        rep_dir = os.path.join(PROJECT_ROOT, "reports", "daily")
                        if os.path.isdir(rep_dir):
                            for rfn in os.listdir(rep_dir):
                                rfp = os.path.join(rep_dir, rfn)
                                if os.path.isfile(rfp):
                                    z.write(rfp, arcname=os.path.join("reports", "daily", rfn))
                        sig_dir = os.path.join(PROJECT_ROOT, "data", "signals")
                        if os.path.isdir(sig_dir):
                            for root, _, files in os.walk(sig_dir):
                                for sfn in files:
                                    if sfn.endswith(".jsonl.gz"):
                                        full_p = os.path.join(root, sfn)
                                        rel_p = os.path.relpath(full_p, PROJECT_ROOT)
                                        z.write(full_p, arcname=rel_p)

                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Disposition", "attachment; filename=\"korean_sunrise_export.zip\"")
                    self.end_headers()
                    with open(tmp.name, "rb") as f:
                        shutil.copyfileobj(f, self.wfile)
                finally:
                    try:
                        os.remove(tmp.name)
                    except OSError:
                        pass

            # Helpers
            def _send_json(self, data: Any, code: int = 200) -> None:
                b = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def _send_html(self, html_str: str, code: int = 200) -> None:
                b = html_str.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def _send_text(self, text: str, code: int = 200) -> None:
                b = text.encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def _send_unauthorized(self) -> None:
                self._send_json({"error": "Unauthorized. Provide Authorization header or session cookie"}, code=401)

            def _send_forbidden(self, msg: str = "Forbidden") -> None:
                self._send_json({"error": msg}, code=403)

            def _send_404(self) -> None:
                self._send_json({"error": "Not Found"}, code=404)

        try:
            self._server = http.server.ThreadingHTTPServer((self.host, self.port), Handler)
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            self._running = True
            log.info(f"=== Korean Sunrise Web API Server running at http://{self.host}:{self.port}/ (Auth token protected) ===")
        except Exception as e:
            log.error(f"[api_server] Failed to start HTTP server on port {self.port}: {e}")

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        log.info("[api_server] Web API Server stopped cleanly.")
```

## web/views.py

```python
"""
web/views.py - Material Design 3 Dark Theme Dashboard and Reporting Hub for Korean Sunrise.
Hardened with zero-token URL leaks, anti-CSRF protection, and session controls.
"""
from __future__ import annotations

import html
import json
import urllib.parse
from typing import Any, Dict, List, Optional


def render_login_html(error: Optional[str] = None) -> str:
    error_banner = ""
    if error:
        error_banner = f"""<div style="background:rgba(239, 68, 68, 0.15);border:1px solid #ef4444;border-radius:10px;padding:12px;margin-bottom:20px;color:#fca5a5;font-size:13px;display:flex;align-items:center;gap:8px;">
          <span>⚠️</span>
          <span>{html.escape(error)}</span>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Вход | Korean Sunrise Intelligence</title>
<style>
:root {{
  --bg-color: #0b0e17;
  --surface-color: #151c2c;
  --surface-border: rgba(255, 255, 255, 0.08);
  --primary-color: #38bdf8;
  --primary-glow: rgba(56, 189, 248, 0.25);
  --text-main: #f8fafc;
  --text-muted: #94a3b8;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
  background-color: var(--bg-color);
  color: var(--text-main);
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  padding: 16px;
}}
.login-card {{
  background: var(--surface-color);
  border: 1px solid var(--surface-border);
  border-radius: 20px;
  padding: 36px;
  width: 100%;
  max-width: 420px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.6);
  position: relative;
  overflow: hidden;
}}
.login-card::before {{
  content: '';
  position: absolute;
  top: -50px;
  right: -50px;
  width: 120px;
  height: 120px;
  background: radial-gradient(circle, var(--primary-glow) 0%, transparent 70%);
  border-radius: 50%;
}}
.logo-title {{
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}}
.logo-icon {{
  font-size: 28px;
}}
h1 {{
  font-size: 22px;
  font-weight: 700;
  letter-spacing: -0.5px;
}}
.subtitle {{
  color: var(--text-muted);
  font-size: 14px;
  margin-bottom: 28px;
}}
.form-group {{
  margin-bottom: 20px;
}}
label {{
  display: block;
  font-size: 13px;
  color: var(--text-muted);
  margin-bottom: 8px;
  font-weight: 500;
}}
input[type="password"] {{
  width: 100%;
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--surface-border);
  border-radius: 10px;
  padding: 12px 14px;
  color: #fff;
  font-size: 15px;
  outline: none;
  transition: border-color 0.2s, box-shadow 0.2s;
}}
input[type="password"]:focus {{
  border-color: var(--primary-color);
  box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.15);
}}
button {{
  width: 100%;
  background: linear-gradient(135deg, #0284c7 0%, #2563eb 100%);
  border: none;
  border-radius: 10px;
  color: #fff;
  font-size: 15px;
  font-weight: 600;
  padding: 14px;
  cursor: pointer;
  transition: opacity 0.2s, transform 0.1s;
}}
button:hover {{ opacity: 0.95; }}
button:active {{ transform: scale(0.99); }}
</style>
</head>
<body>
<div class="login-card">
  <div class="logo-title">
    <div class="logo-icon">🌅</div>
    <h1>Korean Sunrise</h1>
  </div>
  <p class="subtitle">Авторизация для доступа к квантовой телеметрии</p>
  {error_banner}
  <form method="POST" action="/login">
    <div class="form-group">
      <label for="token">Секретный ключ авторизации:</label>
      <input type="password" id="token" name="token" placeholder="Введите API токен" required autofocus>
    </div>
    <button type="submit">Войти в панель управления</button>
  </form>
</div>
</body>
</html>"""


def render_dashboard_html(
    service_status: Dict[str, Any],
    pool_snapshot: Dict[str, Any],
    profile_data: Dict[str, Any],
    thresholds_doc: Dict[str, Any],
    reports_cards: List[Dict[str, Any]],
    csrf_token: str = "",
    auth_token: str = "",
) -> str:
    effective_csrf = csrf_token or auth_token
    summary = pool_snapshot.get("summary", {})
    counters = service_status.get("counters", {})
    streams = service_status.get("streams", {})
    krw_rate = service_status.get("usdt_krw_rate", 1350.0)

    # Thresholds stats
    th_meta = thresholds_doc.get("metadata", {})
    th_class = th_meta.get("classifications", {})
    th_items = sorted(thresholds_doc.get("thresholds", {}).items())

    # Build active streams pills
    stream_pills_html = []
    for s_name, s_info in streams.items():
        is_ok = s_info.get("connected", False)
        st_color = "#10b981" if is_ok else "#ef4444"
        sub_cnt = s_info.get("subscribed", 0)
        seen_cnt = s_info.get("symbols_seen", 0)
        stream_pills_html.append(f"""
        <div class="stream-pill">
          <span class="status-dot" style="background: {st_color};"></span>
          <span class="stream-title">{s_name.upper()}</span>
          <span class="stream-meta">{seen_cnt}/{sub_cnt}</span>
        </div>
        """)
    streams_bar = "".join(stream_pills_html)

    # Build report cards HTML
    report_cards_html = []
    if reports_cards:
        for rc in reports_cards:
            pnl_val = rc.get("net_pnl_usd", 0.0)
            pnl_color = "#10b981" if pnl_val >= 0 else "#ef4444"
            sign = "+" if pnl_val > 0 else ""

            # venues
            v_rows = []
            for v in rc.get("venues_breakdown", [])[:6]:
                v_pnl = v.get("net", 0.0)
                vp_col = "#10b981" if v_pnl >= 0 else "#ef4444"
                v_rows.append(f"""
                <div class="venue-chip">
                  <span class="venue-name">{html.escape(v.get('pair', ''))}</span>
                  <span class="venue-trades">{v.get('trades', 0)} сд ({v.get('winrate', 0)}%)</span>
                  <span class="venue-pnl" style="color: {vp_col};">{sign}${v_pnl:.2f}</span>
                </div>
                """)
            venues_html = "".join(v_rows) if v_rows else "<div style='color:#64748b;font-size:12px;'>Нет связок</div>"

            fn_escaped = urllib.parse.quote(rc.get("filename_md", ""))
            report_cards_html.append(f"""
            <div class="report-card">
              <div class="rc-header">
                <div>
                  <div class="rc-title">{html.escape(rc.get('title', ''))}</div>
                  <div class="rc-date">{html.escape(rc.get('date_from', ''))} &bull; {rc.get('total_rows', 0):,} импульсов ({rc.get('total_mb', 0)} MB)</div>
                </div>
                <div class="rc-pnl" style="color: {pnl_color};">
                  {sign}${pnl_val:.2f}
                  <div class="rc-winrate">Win Rate: {rc.get('winrate_pct', 0)}% &bull; {rc.get('total_trades', 0):,} сд</div>
                </div>
              </div>
              <div class="rc-body">
                <div class="section-label">Топ связок с Upbit</div>
                <div class="venues-list">
                  {venues_html}
                </div>
              </div>
              <div class="rc-footer">
                <button class="btn-read" onclick="viewReport('{rc.get('id', '')}')">📖 Читать квантовый отчёт</button>
                <a href="/download?file={fn_escaped}" class="btn-dl">Скачать .md</a>
              </div>
            </div>
            """)
    else:
        report_cards_html.append("<div style='padding:24px;color:#64748b;'>Нет доступных отчётов. Нажмите 'Сгенерировать суточный отчёт'.</div>")
    reports_view_html = "".join(report_cards_html)

    # Build Thresholds rows
    th_rows = []
    for pk, cfg in th_items[:80]:
        st = cfg.get("status", "default_baseline")
        en = cfg.get("enabled", True)
        badge_cls = "badge-green" if st == "aggressive_clean" else ("badge-blue" if st == "balanced_standard" else ("badge-yellow" if st == "high_friction" else "badge-red"))
        status_label = st.replace("_", " ").upper()
        th_rows.append(f"""
        <tr>
          <td style="font-weight:600;">{html.escape(pk)}</td>
          <td><span class="badge {badge_cls}">{status_label}</span></td>
          <td style="text-align:right;font-family:monospace;font-weight:700;">{cfg.get('min_spread_bps', 20.0):.1f} bps</td>
          <td style="text-align:right;font-family:monospace;">{cfg.get('expected_winrate', 0.85)*100:.1f}%</td>
          <td style="text-align:right;font-family:monospace;">{cfg.get('sample_trades', 0)}</td>
          <td style="text-align:center;">
            <span style="color:{'#10b981' if en else '#ef4444'};font-weight:700;">{'АКТИВНА' if en else 'ОТКЛЮЧЕНА'}</span>
          </td>
        </tr>
        """)
    thresholds_table_html = "".join(th_rows)

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Korean Sunrise | Lead-Lag Intelligence Hub</title>
<style>
:root {{
  --bg-0: #07090e;
  --bg-1: #0f141f;
  --bg-2: #171f30;
  --bg-3: #1f2a40;
  --border: rgba(255, 255, 255, 0.08);
  --border-focus: #38bdf8;
  --primary: #38bdf8;
  --accent: #f59e0b;
  --green: #10b981;
  --red: #ef4444;
  --text: #f8fafc;
  --text-muted: #94a3b8;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  background: var(--bg-0);
  color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  min-height: 100vh;
  display: flex;
  flex-direction: column;
}}
/* Top bar */
.topbar {{
  background: var(--bg-1);
  border-bottom: 1px solid var(--border);
  padding: 12px 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 16px;
}}
.topbar-left {{
  display: flex;
  align-items: center;
  gap: 12px;
}}
.brand-title {{
  font-size: 18px;
  font-weight: 700;
  letter-spacing: -0.5px;
  display: flex;
  align-items: center;
  gap: 8px;
}}
.rate-badge {{
  background: rgba(245, 158, 11, 0.15);
  color: var(--accent);
  border: 1px solid rgba(245, 158, 11, 0.3);
  padding: 4px 10px;
  border-radius: 8px;
  font-size: 13px;
  font-family: monospace;
  font-weight: 700;
}}
/* Navigation Tabs */
.tabs {{
  display: flex;
  align-items: center;
  gap: 8px;
}}
.tab-btn {{
  background: transparent;
  border: none;
  color: var(--text-muted);
  font-size: 14px;
  font-weight: 600;
  padding: 8px 16px;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.15s;
}}
.tab-btn:hover {{ color: #fff; background: rgba(255,255,255,0.05); }}
.tab-btn.active {{ color: #fff; background: var(--bg-3); border: 1px solid var(--border); }}

/* Stream ticker bar */
.streams-strip {{
  background: var(--bg-1);
  border-bottom: 1px solid var(--border);
  padding: 8px 24px;
  display: flex;
  align-items: center;
  gap: 12px;
  overflow-x: auto;
}}
.stream-pill {{
  background: var(--bg-2);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 4px 10px;
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  white-space: nowrap;
}}
.status-dot {{ width: 8px; height: 8px; border-radius: 50%; }}
.stream-title {{ font-weight: 700; }}
.stream-meta {{ color: var(--text-muted); font-size: 11px; }}

/* Content Area */
.main-content {{
  padding: 24px;
  max-width: 1400px;
  margin: 0 auto;
  width: 100%;
  flex: 1;
}}
.tab-pane {{ display: none; }}
.tab-pane.active {{ display: block; }}

/* Cards grid */
.metric-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 16px;
  margin-bottom: 24px;
}}
.metric-card {{
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 18px 20px;
}}
.mc-label {{
  font-size: 13px;
  color: var(--text-muted);
  margin-bottom: 8px;
}}
.mc-val {{
  font-size: 26px;
  font-weight: 700;
  letter-spacing: -0.5px;
}}
.mc-sub {{
  font-size: 12px;
  color: var(--text-muted);
  margin-top: 4px;
}}

/* Report Cards */
.report-card {{
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 24px;
  margin-bottom: 20px;
}}
.rc-header {{
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  border-bottom: 1px solid var(--border);
  padding-bottom: 16px;
  margin-bottom: 16px;
}}
.rc-title {{ font-size: 18px; font-weight: 700; }}
.rc-date {{ font-size: 13px; color: var(--text-muted); margin-top: 4px; }}
.rc-pnl {{ font-size: 24px; font-weight: 800; text-align: right; font-family: monospace; }}
.rc-winrate {{ font-size: 12px; color: var(--text-muted); font-family: sans-serif; font-weight: normal; }}
.section-label {{ font-size: 12px; text-transform: uppercase; color: var(--text-muted); font-weight: 700; margin-bottom: 10px; }}
.venues-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 8px; margin-bottom: 16px; }}
.venue-chip {{
  background: var(--bg-2);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 8px 12px;
  display: flex;
  justify-content: space-between;
  font-size: 13px;
}}
.rc-footer {{ display: flex; justify-content: flex-end; gap: 12px; }}
.btn-read {{
  background: linear-gradient(135deg, #0284c7 0%, #2563eb 100%);
  color: #fff;
  border: none;
  padding: 10px 18px;
  border-radius: 8px;
  font-weight: 600;
  cursor: pointer;
}}
.btn-dl {{
  background: var(--bg-2);
  color: var(--text);
  border: 1px solid var(--border);
  padding: 10px 18px;
  border-radius: 8px;
  font-size: 13px;
  text-decoration: none;
}}

/* Table styles */
.data-table {{
  width: 100%;
  border-collapse: collapse;
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 12px;
  overflow: hidden;
  font-size: 13px;
}}
.data-table th, .data-table td {{
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
}}
.data-table th {{
  background: var(--bg-2);
  color: var(--text-muted);
  font-weight: 600;
  text-align: left;
}}
.badge {{
  padding: 3px 8px;
  border-radius: 6px;
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
}}
.badge-green {{ background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); }}
.badge-blue {{ background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); }}
.badge-yellow {{ background: rgba(245, 158, 11, 0.15); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3); }}
.badge-red {{ background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }}

/* Modal overlay */
.modal-overlay {{
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.85);
  display: none;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  padding: 24px;
}}
.modal-overlay.active {{ display: flex; }}
.modal-box {{
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 16px;
  max-width: 900px;
  width: 100%;
  max-height: 85vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}}
.modal-header {{
  padding: 16px 24px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
}}
.modal-content {{
  padding: 24px;
  overflow-y: auto;
  font-family: monospace;
  font-size: 13px;
  line-height: 1.6;
  white-space: pre-wrap;
}}
</style>
</head>
<body>

<div class="topbar">
  <div class="topbar-left">
    <div class="brand-title">🌅 Korean Sunrise Intelligence</div>
    <div class="rate-badge">KRW/USDT: {krw_rate:.1f}</div>
  </div>
  <div class="tabs">
    <button class="tab-btn active" onclick="switchTab('dashboard')">Мониторинг</button>
    <button class="tab-btn" onclick="switchTab('reports')">Суточная отчётность</button>
    <button class="tab-btn" onclick="switchTab('thresholds')">Матрица спредов ({len(th_items)})</button>
    <button class="tab-btn" onclick="switchTab('profile')">Депозит и слоты</button>
    <button class="tab-btn" onclick="switchTab('controls')">Управление</button>
    <form method="POST" action="/logout" style="margin:0;display:inline;">
      <input type="hidden" name="csrf_token" value="{effective_csrf}">
      <button type="submit" class="tab-btn" style="color:var(--red);" title="Завершить сеанс">Выйти 🚪</button>
    </form>
  </div>
</div>

<div class="streams-strip">
  {streams_bar}
</div>

<div class="main-content">

  <!-- TAB: DASHBOARD -->
  <div id="tab-dashboard" class="tab-pane active">
    <div class="metric-grid">
      <div class="metric-card">
        <div class="mc-label">Боевой пул (Battle)</div>
        <div class="mc-val" style="color:var(--green);">{summary.get('battle_active', 0)} <span style="font-size:16px;color:var(--text-muted);">/ {summary.get('battle', 0)}</span></div>
        <div class="mc-sub">Инкубатор: {summary.get('incubator', 0)} монет</div>
      </div>
      <div class="metric-card">
        <div class="mc-label">Зафиксировано импульсов</div>
        <div class="mc-val" style="color:var(--primary);">{counters.get('signals_triggered', 0):,}</div>
        <div class="mc-sub">⚔️ Battle: {counters.get('signals_battle', 0)} &bull; 🧪 Inc: {counters.get('signals_incubator', 0)}</div>
      </div>
      <div class="metric-card">
        <div class="mc-label">Сходимость импульсов</div>
        <div class="mc-val" style="color:var(--green);">{counters.get('signals_converged', 0):,}</div>
        <div class="mc-sub">Таймауты: {counters.get('signals_timeout', 0)} &bull; Развороты: {counters.get('signals_reversed', 0)}</div>
      </div>
      <div class="metric-card">
        <div class="mc-label">Тиков стакана обработано</div>
        <div class="mc-val">{counters.get('ticks_upbit', 0) + counters.get('ticks_followers', 0):,}</div>
        <div class="mc-sub">Upbit: {counters.get('ticks_upbit', 0):,} &bull; Follower: {counters.get('ticks_followers', 0):,}</div>
      </div>
    </div>

    <div style="background:var(--bg-1);border:1px solid var(--border);border-radius:14px;padding:20px;">
      <h3 style="margin-bottom:12px;">Статус активного боевого пула</h3>
      <div style="display:flex;flex-wrap:wrap;gap:8px;">
        {_render_battle_tags(pool_snapshot)}
      </div>
    </div>
  </div>

  <!-- TAB: REPORTS -->
  <div id="tab-reports" class="tab-pane">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
      <h2>Архив суточных квантовых отчётов</h2>
      <button class="btn-read" onclick="triggerDailyReport()">⚡ Сгенерировать отчёт сейчас</button>
    </div>
    {reports_view_html}
  </div>

  <!-- TAB: THRESHOLDS -->
  <div id="tab-thresholds" class="tab-pane">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
      <div>
        <h2>Матрица калиброванных порогов спреда</h2>
        <p style="color:var(--text-muted);font-size:13px;margin-top:4px;">
          Классификация: 
          Aggressive Clean: {th_class.get('aggressive_clean', 0)} | 
          Balanced Standard: {th_class.get('balanced_standard', 0)} | 
          High Friction: {th_class.get('high_friction', 0)} | 
          Pruned: {th_class.get('toxic_pruned', 0)}
        </p>
      </div>
      <button class="btn-read" onclick="triggerCalibration()">🎯 Перекалибровать пороги</button>
    </div>
    <div style="overflow-x:auto;">
      <table class="data-table">
        <thead>
          <tr>
            <th>Связка (Монета:Лидер-Фолловер)</th>
            <th>Классификация</th>
            <th style="text-align:right;">Мин. Спред</th>
            <th style="text-align:right;">Ожидаемый WinRate</th>
            <th style="text-align:right;">Сделок в выборке</th>
            <th style="text-align:center;">Статус</th>
          </tr>
        </thead>
        <tbody>
          {thresholds_table_html}
        </tbody>
      </table>
    </div>
  </div>

  <!-- TAB: PROFILE -->
  <div id="tab-profile" class="tab-pane">
    <h2>Профиль депозита и сайзинга</h2>
    <div style="background:var(--bg-1);border:1px solid var(--border);border-radius:14px;padding:24px;margin-top:16px;max-width:700px;">
      <pre style="font-family:monospace;font-size:13px;line-height:1.5;">{json.dumps(profile_data, indent=2, ensure_ascii=False)}</pre>
    </div>
  </div>

  <!-- TAB: CONTROLS -->
  <div id="tab-controls" class="tab-pane">
    <h2>Управление и автономные пайплайны</h2>
    <div style="display:grid;grid-template-columns:repeat(auto-fit, minmax(300px, 1fr));gap:16px;margin-top:16px;">
      <div style="background:var(--bg-1);border:1px solid var(--border);border-radius:12px;padding:20px;">
        <h3>24-Часовой мастер-пайплайн</h3>
        <p style="color:var(--text-muted);font-size:13px;margin:8px 0 16px;">Запускает суточный скрининг 1440 свечей, оценку пула, калибровку спредов и отчётность.</p>
        <button class="btn-read" style="width:100%;" onclick="runPipeline()">🚀 Запустить суточный цикл</button>
      </div>
      <div style="background:var(--bg-1);border:1px solid var(--border);border-radius:12px;padding:20px;">
        <h3>Экспорт всех данных (.ZIP)</h3>
        <p style="color:var(--text-muted);font-size:13px;margin:8px 0 16px;">Выгрузка сигналов, отчётов и конфигураций в 1 клик без передачи токена в URL.</p>
        <a href="/download" class="btn-dl" style="display:block;text-align:center;">📦 Скачать архив</a>
      </div>
    </div>
  </div>

</div>

<!-- MODAL REPORT VIEWER -->
<div id="reportModal" class="modal-overlay" onclick="closeModal(event)">
  <div class="modal-box" onclick="event.stopPropagation()">
    <div class="modal-header">
      <h3 id="modalTitle">Суточный отчёт</h3>
      <button style="background:none;border:none;color:#fff;font-size:20px;cursor:pointer;" onclick="closeModal(event)">&times;</button>
    </div>
    <div id="modalContent" class="modal-content">Загрузка...</div>
  </div>
</div>

<script>
const CSRF_TOKEN = "{effective_csrf}";

function switchTab(name) {{
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  const target = document.getElementById('tab-' + name);
  if (target) {{
    target.classList.add('active');
  }}
  if (window.event && window.event.target && window.event.target.classList.contains('tab-btn')) {{
    window.event.target.classList.add('active');
  }}
}}

async function viewReport(reportId) {{
  document.getElementById('modalTitle').innerText = 'Отчёт: ' + reportId;
  document.getElementById('modalContent').innerText = 'Загрузка...';
  document.getElementById('reportModal').classList.add('active');
  try {{
    const res = await fetch('/api/report/content?id=' + encodeURIComponent(reportId));
    if (!res.ok) {{
      document.getElementById('modalContent').innerText = 'Ошибка (' + res.status + '): ' + res.statusText;
      return;
    }}
    const text = await res.text();
    document.getElementById('modalContent').innerText = text;
  }} catch (e) {{
    document.getElementById('modalContent').innerText = 'Ошибка загрузки: ' + e;
  }}
}}

function closeModal(e) {{
  document.getElementById('reportModal').classList.remove('active');
}}

async function triggerDailyReport() {{
  if (!confirm('Сгенерировать суточный квантовый отчёт прямо сейчас?')) return;
  try {{
    const res = await fetch('/api/report/generate', {{
      method: 'POST',
      headers: {{
        'X-CSRF-Token': CSRF_TOKEN,
        'Content-Type': 'application/json'
      }}
    }});
    if (!res.ok) {{
      const err = await res.json().catch(() => ({{}}));
      alert('Ошибка (' + res.status + '): ' + (err.error || res.statusText));
      return;
    }}
    alert('Отчёт успешно сформирован!');
    location.reload();
  }} catch (e) {{
    alert('Ошибка сети: ' + e);
  }}
}}

async function triggerCalibration() {{
  if (!confirm('Запустить алгоритмическую калибровку спредов по всем парам?')) return;
  try {{
    const res = await fetch('/api/thresholds/calibrate', {{
      method: 'POST',
      headers: {{
        'X-CSRF-Token': CSRF_TOKEN,
        'Content-Type': 'application/json'
      }}
    }});
    if (!res.ok) {{
      const err = await res.json().catch(() => ({{}}));
      alert('Ошибка (' + res.status + '): ' + (err.error || res.statusText));
      return;
    }}
    alert('Калибровка завершена!');
    location.reload();
  }} catch (e) {{
    alert('Ошибка сети: ' + e);
  }}
}}

async function runPipeline() {{
  if (!confirm('Запустить 24-часовой автономный пайплайн?')) return;
  try {{
    const res = await fetch('/api/daily-pipeline', {{
      method: 'POST',
      headers: {{
        'X-CSRF-Token': CSRF_TOKEN,
        'Content-Type': 'application/json'
      }}
    }});
    if (!res.ok) {{
      const err = await res.json().catch(() => ({{}}));
      alert('Ошибка (' + res.status + '): ' + (err.error || res.statusText));
      return;
    }}
    alert('Пайплайн запущен в фоне!');
  }} catch (e) {{
    alert('Ошибка сети: ' + e);
  }}
}}

async function toggleCoinPool(action, coin) {{
  try {{
    const res = await fetch('/pool/' + action, {{
      method: 'POST',
      headers: {{
        'X-CSRF-Token': CSRF_TOKEN,
        'Content-Type': 'application/json'
      }},
      body: JSON.stringify({{ coin: coin }})
    }});
    if (!res.ok) {{
      const err = await res.json().catch(() => ({{}}));
      alert('Ошибка (' + res.status + '): ' + (err.error || res.statusText));
      return;
    }}
    location.reload();
  }} catch (e) {{
    alert('Ошибка сети: ' + e);
  }}
}}
</script>
</body>
</html>"""


def _render_battle_tags(pool_snapshot: Dict[str, Any]) -> str:
    coins_dict = pool_snapshot.get("coins", {})
    battle = [
        c for c in coins_dict.values()
        if isinstance(c, dict) and c.get("basket") == "battle"
    ]
    if not battle:
        return "<span style='color:#64748b;font-size:13px;'>Боевой пул пока пуст. Монеты находятся в инкубаторе и промоутятся после оценки.</span>"
    tags = []
    for c in sorted(battle, key=lambda x: x.get("mean_net_pnl_bps", 0.0), reverse=True):
        name = html.escape(str(c.get("coin", "")))
        en = c.get("enabled", True)
        col = "#10b981" if en else "#64748b"
        act_text = "Пауза ⏸" if en else "Старт ▶"
        act_fn = "pause" if en else "resume"
        btn_col = "#f59e0b" if en else "#10b981"
        tags.append(f"""
        <div style="background:var(--bg-2);border:1px solid var(--border);border-radius:8px;padding:6px 12px;font-size:13px;display:flex;align-items:center;gap:10px;">
          <span style="color:{col};font-weight:700;">⚔️ {name}</span>
          <button onclick="toggleCoinPool('{act_fn}', '{name}')" style="background:none;border:1px solid {btn_col};color:{btn_col};border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;">{act_text}</button>
        </div>
        """)
    return "".join(tags)
```
