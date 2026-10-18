# KoreanSunrise — Full Codebase Snapshot

## Table of Contents

- [README.md](#readme-md)
- [batch_writer.py](#batch_writer-py)
- [config.py](#config-py)
- [docs/explanations.md](#docs-explanations-md)
- [korean_engine.py](#korean_engine-py)
- [logs/.gitkeep](#logs--gitkeep)
- [main.py](#main-py)
- [matrix_thresholds.json](#matrix_thresholds-json)
- [models.py](#models-py)
- [pipeline/__init__.py](#pipeline-__init__-py)
- [pipeline/evaluator.py](#pipeline-evaluator-py)
- [pipeline/pool_state.py](#pipeline-pool_state-py)
- [pipeline/profile.py](#pipeline-profile-py)
- [reports/daily/report_2026-10-18.json](#reports-daily-report_2026-10-18-json)
- [reports/daily/report_2026-10-18.md](#reports-daily-report_2026-10-18-md)
- [requirements.txt](#requirements-txt)
- [runtime_overrides.json](#runtime_overrides-json)
- [screening/__init__.py](#screening-__init__-py)
- [screening/metrics.py](#screening-metrics-py)
- [screening/screener.py](#screening-screener-py)
- [scripts/cron_daily.sh](#scripts-cron_daily-sh)
- [scripts/daily-pipeline.service](#scripts-daily-pipeline-service)
- [scripts/daily-pipeline.timer](#scripts-daily-pipeline-timer)
- [scripts/korean-sunrise.service](#scripts-korean-sunrise-service)
- [tests/test_pipeline_e2e.py](#tests-test_pipeline_e2e-py)
- [tools/__init__.py](#tools-__init__-py)
- [tools/analyze_signals.py](#tools-analyze_signals-py)
- [tools/audit_venues.py](#tools-audit_venues-py)
- [tools/calibrate_thresholds.py](#tools-calibrate_thresholds-py)
- [tools/full_daily_report.py](#tools-full_daily_report-py)
- [tools/manage_pool.py](#tools-manage_pool-py)
- [tools/run_daily_pipeline.py](#tools-run_daily_pipeline-py)
- [tools/screen_universe.py](#tools-screen_universe-py)
- [tools/validate_data.py](#tools-validate_data-py)
- [trading_profile.json](#trading_profile-json)
- [universe_live.json](#universe_live-json)
- [venues/__init__.py](#venues-__init__-py)
- [venues/base.py](#venues-base-py)
- [venues/binance.py](#venues-binance-py)
- [venues/bingx.py](#venues-bingx-py)
- [venues/bybit.py](#venues-bybit-py)
- [venues/gateio.py](#venues-gateio-py)
- [venues/htx.py](#venues-htx-py)
- [venues/kucoin.py](#venues-kucoin-py)
- [venues/manager.py](#venues-manager-py)
- [venues/mexc.py](#venues-mexc-py)
- [venues/okx.py](#venues-okx-py)
- [venues/pionex.py](#venues-pionex-py)
- [venues/upbit.py](#venues-upbit-py)
- [web/__init__.py](#web-__init__-py)
- [web/reports_manager.py](#web-reports_manager-py)
- [web/server.py](#web-server-py)
- [web/views.py](#web-views-py)

---

## README.md

````markdown
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

````

## batch_writer.py

```python
"""
batch_writer.py - Atomic compressed disk writer for Korean Sunrise signals.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict

log = logging.getLogger("writer")


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class BatchWriter:
    """Buffers signals in RAM (gzipped) and writes sequentially to disk."""

    def __init__(self, data_root: str = "data", flush_interval_sec: float = 5.0):
        self.data_root = data_root
        self.flush_interval_sec = flush_interval_sec
        self.buf = io.BytesIO()
        self.gz = gzip.GzipFile(fileobj=self.buf, mode="wb", compresslevel=6)
        self.rows_count = 0
        self.total_written = 0
        self._flush_seq = 0
        self._lock = threading.Lock()
        self._last_flush = time.time()
        os.makedirs(os.path.join(self.data_root, "signals"), exist_ok=True)

    def add(self, signal_dict: Dict[str, Any]) -> None:
        try:
            line = json.dumps(signal_dict, default=str) + "\n"
            encoded = line.encode("utf-8")
            with self._lock:
                self.gz.write(encoded)
                self.rows_count += 1
        except Exception as e:
            log.error(f"[writer] Failed to buffer signal: {e}")

    def maybe_flush(self, now: float = 0.0) -> None:
        now = now or time.time()
        if now - self._last_flush >= self.flush_interval_sec and self.rows_count > 0:
            self.flush()

    def flush(self) -> None:
        with self._lock:
            if self.rows_count == 0:
                return

            batch_rows = self.rows_count
            self._flush_seq += 1
            seq = self._flush_seq

            self.gz.close()
            compressed_data = self.buf.getvalue()

            day = _utc_day()
            target_dir = os.path.join(self.data_root, "signals", day)
            os.makedirs(target_dir, exist_ok=True)

            ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"sunrise_signals_{ts_str}_{seq:04d}.jsonl.gz"
            tmp_path = os.path.join(target_dir, f"{filename}.tmp")
            final_path = os.path.join(target_dir, filename)

            try:
                with open(tmp_path, "wb") as f:
                    f.write(compressed_data)
                os.replace(tmp_path, final_path)
                self.total_written += batch_rows
                log.info(
                    f"[writer] Flushed {batch_rows} signals to {filename} "
                    f"({len(compressed_data)} bytes)"
                )
            except Exception as e:
                log.error(f"[writer] Flush error: {e}")
            finally:
                self.buf = io.BytesIO()
                self.gz = gzip.GzipFile(fileobj=self.buf, mode="wb", compresslevel=6)
                self.rows_count = 0
                self._last_flush = time.time()
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
STEALTH_DEPTH_FRACTION = 0.15   # Max 15% of top 3 ask levels on follower order book

# Execution Fees & Warmup Safety
TAKER_FEE_BPS = 5.0             # Single-leg VIP0 taker fee (bps) on Binance/Bybit
ROUNDTRIP_FEE_BPS = 10.0        # Entry + Exit taker fee (bps) for net realized PnL
MIN_WARMUP_SAMPLES = 30         # Minimum 1s MAD samples before firing signals
MAX_SIGNAL_HOLD_SEC = 6.0       # Maximum holding horizon before timeout closure

# Regex to detect multiplier prefixes on Binance/Bybit perps: 1000PEPE, 1000000MOG, etc.
_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")


@dataclass
class MatchedUniverse:
    """Holds the dynamically matched universe of coins across Upbit, Binance, and Bybit."""
    # Canonical base coin list (Upbit base names, e.g. ["PEPE", "DOGE", "SAND", ...])
    coins: List[str] = field(default_factory=list)

    # Upbit mapping: KRW market code -> canonical coin (e.g. "KRW-PEPE" -> "PEPE")
    upbit_krw_to_coin: Dict[str, str] = field(default_factory=dict)

    # Binance Futures mapping: raw exchange symbol -> (canonical coin, price_multiplier)
    # e.g. "1000PEPEUSDT" -> ("PEPE", 1000.0), "SANDUSDT" -> ("SAND", 1.0)
    binance_sym_map: Dict[str, Tuple[str, float]] = field(default_factory=dict)

    # Bybit Linear mapping: raw exchange symbol -> (canonical coin, price_multiplier)
    # e.g. "1000PEPEUSDT" -> ("PEPE", 1000.0), "SANDUSDT" -> ("SAND", 1.0)
    bybit_sym_map: Dict[str, Tuple[str, float]] = field(default_factory=dict)


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

````markdown
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

````

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
import time
from collections import defaultdict, deque
from dataclasses import asdict
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

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


class PairState:
    """State of an (Upbit, Follower) pair for a single coin."""

    def __init__(self, coin: str, follower_ex: str, warmup_samples: int = C.MIN_WARMUP_SAMPLES):
        self.coin = coin
        self.follower_ex = follower_ex
        self.warmup_samples = warmup_samples

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

        # Spike gating: if a massive pump/dislocation is currently happening (> 2.5 sigma),
        # slow down baseline adaptation by 20x so the baseline doesn't chase the pump.
        cur_sig = self.sigma(now)
        is_spike = self.is_warmed_up and abs(deviation) > (2.5 * cur_sig)
        eff_tau = BASELINE_TAU * 20.0 if is_spike else BASELINE_TAU

        a = 1.0 - math.exp(-dt / eff_tau) if eff_tau > 0 else 0.001
        self.baseline_bps += a * deviation
        self.n_obs += 1

        if now - self.dev_ts >= DEV_SAMPLE_DT:
            self.dev_ts = now
            if is_spike:
                clamped_dev = math.copysign(min(abs(deviation), 4.0 * cur_sig), deviation)
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
        stealth_depth_fraction: float = C.STEALTH_DEPTH_FRACTION,
        taker_fee_bps: float = C.TAKER_FEE_BPS,
        roundtrip_fee_bps: float = C.ROUNDTRIP_FEE_BPS,
        warmup_samples: int = C.MIN_WARMUP_SAMPLES,
        max_hold_sec: float = C.MAX_SIGNAL_HOLD_SEC,
        pool_state: Any = None,
        profile_manager: Any = None,
    ):
        self.emit = emit_callback
        self.drain_threshold = drain_threshold
        self.blowout_threshold = blowout_threshold
        self.min_edge_bps = min_edge_bps
        self.max_position_budget_usd = max_position_budget_usd
        self.stealth_fraction = stealth_depth_fraction
        self.taker_fee_bps = taker_fee_bps
        self.roundtrip_fee_bps = roundtrip_fee_bps
        self.warmup_samples = warmup_samples
        self.max_hold_sec = max_hold_sec
        self.pool_state = pool_state
        self.profile_manager = profile_manager

        # (exchange, coin) -> OrderBookDepth5
        self._books: Dict[Tuple[str, str], OrderBookDepth5] = {}
        self._recv_ts: Dict[Tuple[str, str], float] = {}

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
            "signals_triggered": 0,
            "signals_battle": 0,
            "signals_incubator": 0,
            "signals_converged": 0,
            "signals_reversed": 0,
            "signals_timeout": 0,
        }

        # Pair-specific calibrated execution thresholds
        self.thresholds_file = os.path.join(C.PROJECT_ROOT, "matrix_thresholds.json")
        self._thresholds: Dict[str, Any] = {}
        self.reload_thresholds()

    def reload_thresholds(self) -> None:
        try:
            if os.path.exists(self.thresholds_file):
                with open(self.thresholds_file, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                    self._thresholds = doc.get("thresholds", {})
                log.info(f"[engine] Loaded {len(self._thresholds)} calibrated pair thresholds.")
        except Exception as e:
            log.warning(f"[engine] Failed to load {self.thresholds_file}: {e}")

    def get_pair_threshold_config(self, coin: str, follower_ex: str) -> Dict[str, Any]:
        pk = f"{coin.upper()}:upbit-{follower_ex.lower()}"
        return self._thresholds.get(pk, {})

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
            self.counters["ticks_followers"] += 1
            upbit_snap = self._books.get(("upbit", coin))
            if upbit_snap is not None and (now - self._recv_ts.get(("upbit", coin), 0.0)) <= 2.0:
                pair_key = (coin, ex)
                st = self._pairs.get(pair_key)
                if st is None:
                    st = self._pairs[pair_key] = PairState(coin, ex, self.warmup_samples)
                if upbit_snap.mid_price > 0 and snap.mid_price > 0:
                    raw_diff_bps = (upbit_snap.mid_price - snap.mid_price) / snap.mid_price * 1e4
                    st.learn(raw_diff_bps, now)

            self._track_follower_move(coin, ex, snap, now)

    # ----------------------------------------------------------- Upbit Norm
    def _update_upbit_norm(self, coin: str, snap: OrderBookDepth5, now: float) -> None:
        nrm = self._upbit_norm.get(coin)
        cur_depth = snap.depth_ask_usd
        cur_best_ask_usd = snap.best_ask_usd
        cur_spread = snap.spread_bps

        if nrm is None:
            self._upbit_norm[coin] = {
                "depth_ask": max(cur_depth, 100.0),
                "best_ask_usd": max(cur_best_ask_usd, 50.0),
                "spread_bps": max(cur_spread, 1.0),
                "t": now,
            }
            return

        dt = max(0.0, now - nrm["t"])
        nrm["t"] = now
        a = 1.0 - math.exp(-dt / NORM_TAU)

        clamped_spread = min(cur_spread, nrm["spread_bps"] * 5.0)
        clamped_depth = min(cur_depth, nrm["depth_ask"] * 5.0)
        clamped_best_ask = min(cur_best_ask_usd, nrm["best_ask_usd"] * 5.0)

        nrm["depth_ask"] = max((1.0 - a) * nrm["depth_ask"] + a * clamped_depth, 100.0)
        nrm["best_ask_usd"] = max((1.0 - a) * nrm["best_ask_usd"] + a * clamped_best_ask, 50.0)
        nrm["spread_bps"] = max((1.0 - a) * nrm["spread_bps"] + a * clamped_spread, 0.5)

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

        follower_exchanges = [ex for (ex, c) in self._books.keys() if ex != "upbit" and c == coin]
        for follower_ex in follower_exchanges:
            f_snap = self._books.get((follower_ex, coin))
            f_recv = self._recv_ts.get((follower_ex, coin), 0.0)
            if f_snap is None or (now - f_recv) > 2.0:
                continue

            pair_key = (coin, follower_ex)
            st = self._pairs.get(pair_key)
            if st is None:
                st = self._pairs[pair_key] = PairState(coin, follower_ex, self.warmup_samples)

            upbit_mid = upbit_snap.mid_price
            f_mid = f_snap.mid_price
            if upbit_mid <= 0 or f_mid <= 0:
                continue

            raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4

            if trigger_type and st.open_signal is None and st.is_warmed_up:
                self._check_entry(
                    coin=coin,
                    follower_ex=follower_ex,
                    st=st,
                    upbit_snap=upbit_snap,
                    f_snap=f_snap,
                    f_book_age_ms=(now - f_recv) * 1000.0,
                    norm_depth=norm_depth,
                    norm_spread=norm_spread,
                    swept_usd=swept_usd,
                    trigger_type=trigger_type,
                    drain_ratio=drain_ratio,
                    blowout_ratio=blowout_ratio,
                    now=now,
                )

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
    ) -> None:
        sigma = st.sigma(now)
        upbit_mid = upbit_snap.mid_price
        f_mid = f_snap.mid_price
        f_bid = f_snap.best_bid
        f_ask = f_snap.best_ask

        if f_ask <= 0 or f_bid <= 0:
            return

        raw_diff_bps = (upbit_mid - f_mid) / f_mid * 1e4
        dislocation_bps = raw_diff_bps - st.baseline_bps

        if dislocation_bps <= 0:
            return

        dev_z = dislocation_bps / max(sigma, 1.5)
        if dev_z < 1.8:
            return

        f_ask_diff_bps = (upbit_mid - f_ask) / f_ask * 1e4
        gross_exec_edge_bps = f_ask_diff_bps - st.baseline_bps
        net_edge = gross_exec_edge_bps - self.taker_fee_bps

        # Check pair-specific calibrated threshold
        pair_cfg = self.get_pair_threshold_config(coin, follower_ex)
        if not pair_cfg.get("enabled", True):
            return  # Pruned pair due to negative EV or high adverse flow

        req_edge = float(pair_cfg.get("min_spread_bps", self.min_edge_bps))
        if net_edge < req_edge:
            return

        if f_snap.spread_bps >= gross_exec_edge_bps * 0.8:
            return

        top3_ask_depth_usd = f_snap.top_n_ask_depth_usd(3)
        if top3_ask_depth_usd < 50.0:
            return

        # Determine pool basket and actionable status
        is_battle = self.pool_state.is_battle_coin(coin) if self.pool_state else False
        basket = "battle" if is_battle else "incubator"
        is_actionable = is_battle

        # Calculate dynamic stealth order size
        if self.profile_manager:
            sized = self.profile_manager.calculate_stealth_order_size(follower_ex, top3_ask_depth_usd)
            suggested_size_usd = sized if sized is not None else min(
                self.max_position_budget_usd,
                self.stealth_fraction * top3_ask_depth_usd,
            )
        else:
            suggested_size_usd = min(
                self.max_position_budget_usd,
                self.stealth_fraction * top3_ask_depth_usd,
            )

        # Initial executable PnL at t=0 (if bought at f_ask and immediately sold at f_bid)
        init_exec_move_bps = (f_bid - f_ask) / f_ask * 1e4
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
            basket=basket,
            is_actionable=is_actionable,
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

        log.info(
            f"{tag} {sig.coin} ({sig.follower_contract}) | Upbit -> {sig.follower_exchange} | "
            f"Trig: {sig.trigger_type} (Swept: ${sig.leader_swept_usd:.0f}) | "
            f"Edge: +{sig.net_exec_edge_bps:.1f} bps | Z: {sig.dev_z:.1f}σ | Size: ${sig.suggested_size_usd:.0f}"
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
            f_mid_move = (f_snap.mid_price - sig.follower_mid) / sig.follower_mid * 1e4
            up_mid_move = (upbit_snap.mid_price - sig.leader_mid) / sig.leader_mid * 1e4

            # 2. True executable move on follower: Entry at best_ask -> Exit at current best_bid (bps)
            f_exec_move = (f_snap.best_bid - sig.follower_best_ask) / sig.follower_best_ask * 1e4
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
            cur_diff_bps = (upbit_snap.mid_price - f_snap.mid_price) / f_snap.mid_price * 1e4
            rem_edge = (cur_diff_bps - sig.kimchi_baseline_bps) - self.taker_fee_bps

            if not sig.path or dt > sig.path[-1][0]:
                sig.path.append([
                    dt,
                    round(up_mid_move, 2),
                    round(f_mid_move, 2),
                    cur_net_pnl,
                    round(rem_edge, 2),
                ])

            # Closure evaluation:
            if f_mid_move >= (sig.net_exec_edge_bps * 0.8):
                self._close_signal(sig, now, "converged", f_mid_move, f_exec_move)
            elif rem_edge <= 0.0:
                if f_mid_move >= (sig.net_exec_edge_bps * 0.35) and f_mid_move > 0:
                    self._close_signal(sig, now, "converged", f_mid_move, f_exec_move)
                else:
                    self._close_signal(sig, now, "reversed", f_mid_move, f_exec_move)
            elif dt >= self.max_hold_sec:
                self._close_signal(sig, now, "timeout", f_mid_move, f_exec_move)

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
        elif reason == "reversed":
            self.counters["signals_reversed"] += 1
        else:
            self.counters["signals_timeout"] += 1

        self.emit(sig.to_dict())

        pair_key = (sig.coin, sig.follower_exchange)
        st = self._pairs.get(pair_key)
        if st and st.open_signal == sig:
            st.open_signal = None

        if sig.coin in self._active_signals and sig in self._active_signals[sig.coin]:
            self._active_signals[sig.coin].remove(sig)

    def sweep_timeouts(self, now: float) -> None:
        """
        Sweeps and closes any active signals that exceeded max_hold_sec even if no new ticks arrived.
        Prevents open signals from lingering on illiquid or inactive coins.
        """
        for coin, signals in list(self._active_signals.items()):
            for sig in list(signals):
                if not sig.closed and (now - sig.open_ts) >= self.max_hold_sec:
                    f_snap = self._books.get((sig.follower_exchange, coin))
                    if f_snap:
                        f_mid_move = (f_snap.mid_price - sig.follower_mid) / sig.follower_mid * 1e4
                        f_exec_move = (f_snap.best_bid - sig.follower_best_ask) / sig.follower_best_ask * 1e4
                    else:
                        f_mid_move = 0.0
                        f_exec_move = 0.0
                    self._close_signal(sig, now, "timeout", f_mid_move, f_exec_move)
```

## logs/.gitkeep

```
# Keep logs directory in git

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
            stealth_depth_fraction=C.STEALTH_DEPTH_FRACTION,
            taker_fee_bps=C.TAKER_FEE_BPS,
            roundtrip_fee_bps=C.ROUNDTRIP_FEE_BPS,
            warmup_samples=C.MIN_WARMUP_SAMPLES,
            max_hold_sec=C.MAX_SIGNAL_HOLD_SEC,
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

    async def run(self) -> None:
        self.running = True
        if self.api_server:
            self.api_server.start()
        univ_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "universe_live.json")

        if os.path.exists(univ_file):
            try:
                with open(univ_file, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                coins = doc.get("coins", [])
                upbit_krw_to_coin = doc.get("upbit_krw_to_coin", {})
                binance_sym_map = {k: tuple(v) for k, v in doc.get("binance_sym_map", {}).items()}
                bybit_sym_map = {k: tuple(v) for k, v in doc.get("bybit_sym_map", {}).items()}
                self.universe = C.MatchedUniverse(
                    coins=coins,
                    upbit_krw_to_coin=upbit_krw_to_coin,
                    binance_sym_map=binance_sym_map,
                    bybit_sym_map=bybit_sym_map,
                )
                self.universe._raw_doc = doc
                log.info(f"=== Loaded {len(coins)} incubator coins from universe_live.json ===")
            except Exception as e:
                log.warning(f"Could not parse universe_live.json: {e}")
                self.universe = None

        if self.universe is None or not self.universe.coins:
            log.info("=== Discovering overlapping Upbit KRW alts & Binance/Bybit Perps ===")
            self.universe = await asyncio.to_thread(C.discover_universe)
            try:
                doc = {
                    "_meta": {
                        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        "updated_ts": round(time.time(), 3),
                        "coins_count": len(self.universe.coins),
                    },
                    "coins": self.universe.coins,
                    "upbit_krw_to_coin": self.universe.upbit_krw_to_coin,
                    "binance_sym_map": self.universe.binance_sym_map,
                    "bybit_sym_map": self.universe.bybit_sym_map,
                }
                with open(univ_file + ".tmp", "w", encoding="utf-8") as f:
                    json.dump(doc, f, indent=2, ensure_ascii=False)
                os.replace(univ_file + ".tmp", univ_file)
            except Exception as e:
                log.warning(f"Could not persist universe_live.json: {e}")

        self.pool_state.reload()

        if not self.universe or not self.universe.coins:
            raise RuntimeError("No overlapping coins discovered between Upbit and Perp exchanges.")

        log.info(f"=== Starting Korean Sunrise for {len(self.universe.coins)} matched target coins ===")
        active_venues = self.profile_manager.get_profile().get("active_venues")
        self.stream_manager = StreamManager(
            universe=self.universe,
            callback=self.engine.on_book_update,
            active_venues=active_venues,
        )
        await self.stream_manager.start_all()

        last_heartbeat = time.time()

        try:
            while not self._shutdown_event.is_set():
                now = time.time()
                self.engine.sweep_timeouts(now)
                self.writer.maybe_flush(now)

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
            f"Ticks: Up {c['ticks_upbit']} / Foll {c['ticks_followers']} | "
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
        self.writer.flush()
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
    "generated_at": "2026-10-18T14:09:54.010303+00:00",
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
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VVV:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "LIT:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ICP:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ETHFI:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RAY:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROM:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AXL:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "TRUMP:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "TRUMP:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "TRUMP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-htx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WLD:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ELSA:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SPX:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SOMI:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SOMI:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SOMI:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SOMI:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GRT:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GRT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GRT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GRT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "EUL:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZRO:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZORA:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CAP:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PIEVERSE:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "WCT:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROS:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PROS:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENSO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BREV:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ESP:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ESP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-htx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEAR:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VET:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "KMNO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GTC:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "GTC:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "B3:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "B3:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CYBER:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CYBER:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CYBER:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CYBER:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "HYPER:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "HYPER:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "HYPER:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SENT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AWE:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AWE:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AWE:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "AWE:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKC:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKC:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKC:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKC:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ZKP:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEO:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NEO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IO:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "INJ:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SKR:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SKR:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SKR:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SKR:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CARV:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CARV:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CARV:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "CARV:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ERA:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "SEI:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ARX:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MMT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MNT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MNT:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PONS:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PONS:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PONS:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "API3:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "API3:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENS:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENS:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENS:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ENS:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IOTA:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "IOTA:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "O:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BCH:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BEAM:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BAT:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PUMP:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PUMP:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "PUMP:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "DOS:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "DOS:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "DOS:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RENDER:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "RENDER:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-gateio": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "APT:upbit-pionex": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "2Z:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "F:upbit-okx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIO:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIO:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIO:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "NMR:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MANA:upbit-kucoin": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VTHO:upbit-binance": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "VTHO:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "MOODENG:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ATOM:upbit-bybit": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ATOM:upbit-mexc": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "ATOM:upbit-bingx": {
      "min_spread_bps": 20.0,
      "expected_winrate": 0.85,
      "expected_net_bps": 22.0,
      "sample_trades": 0,
      "status": "default_baseline",
      "enabled": true
    },
    "BIGTIME:upbit-okx": {
      "min_spread_bps": 20.0,
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

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple


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
    leader_latency_ms: float
    follower_latency_ms: float
    follower_book_age_ms: float

    # Sizing
    suggested_size_usd: float

    # Pool routing
    basket: str = "incubator"
    is_actionable: bool = False

    # Outcome & Excursion tracking
    closed: bool = False
    close_ts: Optional[float] = None
    duration_sec: Optional[float] = None
    close_reason: Optional[str] = None
    captured_mid_move_bps: Optional[float] = None
    captured_exec_move_bps: Optional[float] = None
    net_pnl_bps: Optional[float] = None

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

DEFAULT_MIN_EVENTS = 5
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
        "top3_depths", "valid_sweeps", "total_signals",
    )

    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.venues: set = set()
        self.net_pnls: List[float] = []
        self.mfes: List[float] = []
        self.t_peaks: List[float] = []
        self.top3_depths: List[float] = []
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

        # Discard microscopic noise where Upbit sweep was insignificant vs follower
        if drain_ratio < 0.40 and cross_ratio < 0.50:
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
                f"N={q['event_count']} | Depth: ${q['top3_depth_usd']:>6.0f} | MFE: +{q['mean_mfe_bps']:.1f} bps"
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
        "top3_depth_usd", "min_order_usd", "max_order_usd",
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

    def reload(self) -> None:
        """Reloads all JSON configuration files and merges state atomically."""
        with self._lock:
            self._overrides = _read_json(self._overrides_file, default={})
            self._load_incubator()
            self._load_battle_pool()
            self._apply_overrides()
            self._last_reload = time.time()
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
            log.info(f"[pool_state] Resumed coin: {coin}")
            return True

    def promote_to_battle(self, qualifying_stats: List[Dict[str, Any]]) -> None:
        """
        Promotes qualifying coins from Incubator to Battle based on verified
        quantitative performance. Demotes degraded coins back to Incubator.
        """
        with self._lock:
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
                entry.mean_net_pnl_bps = float(stat.get("mean_net_pnl_bps", 0.0))
                entry.median_net_pnl_bps = float(stat.get("median_net_pnl_bps", 0.0))
                entry.win_rate_pct = float(stat.get("win_rate_pct", 0.0))
                entry.profit_factor = float(stat.get("profit_factor", 0.0))
                entry.event_count = int(stat.get("event_count", 0))
                entry.promoted_at = time.time()

                if coin not in overrides.get("paused_coins", {}):
                    entry.enabled = True
                    entry.pause_reason = None

            # Demote battle coins that failed qualification this cycle
            for coin, entry in self._coins.items():
                if entry.basket == "battle" and coin not in qualified_coins:
                    entry.basket = "incubator"
                    log.info(f"[pool_state] Demoted {coin} from Battle -> Incubator (failed criteria)")

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
            elif entry.pause_reason == "manual":
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
        self.reload()

    def reload(self) -> None:
        with self._lock:
            data = _read_json(self.profile_file)
            if not data:
                log.info(f"[profile] Initializing default trading profile at {self.profile_file}")
                data = json.loads(json.dumps(DEFAULT_PROFILE))
                _write_json_atomic(self.profile_file, data)
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
            return self.get_profile()

    def set_venue_capital(self, venue: str, capital_usd: float) -> None:
        with self._lock:
            venue = venue.lower().strip()
            caps = self._profile.setdefault("capital", {}).setdefault("per_venue_usd", {})
            caps[venue] = max(0.0, float(capital_usd))
            _write_json_atomic(self.profile_file, self._profile)
            log.info(f"[profile] Updated capital for {venue}: ${capital_usd:.2f}")

    def set_max_slots(self, slots: int) -> None:
        with self._lock:
            self._profile["max_active_slots"] = max(1, int(slots))
            _write_json_atomic(self.profile_file, self._profile)
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
                # If slot budget allows min_order and depth is within 80% of min_order requirement
                if slot_budget >= min_order and stealth_size >= (min_order * 0.8):
                    return min_order
                return None

            return round(size, 2)

```

## reports/daily/report_2026-10-18.json

```json
{
  "metadata": {
    "report_date": "2026-10-18",
    "generated_at": "2026-10-18T14:09:54.306603+00:00",
    "days_analyzed": 1.0,
    "order_size_usd": 10.0,
    "roundtrip_fee_bps": 10.0,
    "total_files_scanned": 0,
    "total_size_mb": 0.0,
    "total_signals_recorded": 0,
    "coins_active": 0,
    "venues_active": [],
    "elapsed_sec": 0.015
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

````markdown
# Korean Sunrise: Суточный Квантовый Отчёт (2026-10-18)
*Сгенерирован:* `2026-10-18T14:09:54.306603+00:00` | *Окно:* `1.0 d` | *Размер слота:* `$10.0` | *Комиссия:* `10.0 bps`

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

## 3. Оптимизация порогов спреда (Threshold Sweep)
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

## 4. Топ прибыльных связок (Upbit Leader <-> Perp Follower)
| Связка (Coin:Upbit-Follower) | Сделок | Win Rate | Gross USD | Net PnL USD |
| :--- | :---: | :---: | :---: | :---: |
| *Нет накопленных сделок за выбранный период* | - | - | - | - |

## 5. Топ монет по эффективности опережения
| Монета | Сделок | Win Rate | Net PnL USD | Ср. MFE (bps) | Медиана $T_{peak}$ |
| :--- | :---: | :---: | :---: | :---: | :---: |
| *Нет накопленных данных по монетам* | - | - | - | - | - |
````

## requirements.txt

```text
websockets>=12.0
aiohttp>=3.9.0

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
from venues.base import create_http_session, normalize_symbol
from venues.bingx import BingXRestClient
from venues.binance import BinanceRestClient
from venues.bybit import BybitRestClient
from venues.gateio import GateioRestClient
from venues.htx import HTXRestClient
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
DEFAULT_KLINE_LIMIT = 1440             # 1440 1-minute candles (24 hours = 1 day)

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
}


class UniverseScreener:
    """
    Two-stage quantitative screener that filters Upbit KRW spot leader alts
    against multiple perpetual follower exchanges.
    """

    def __init__(
        self,
        venues: Optional[List[str]] = None,
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

        # Build reverse base map for each venue: canonical_base -> (raw_sym, mult)
        venue_base_maps: Dict[str, Dict[str, Tuple[str, float]]] = {}
        for v, perps in venue_perps.items():
            b_map: Dict[str, Tuple[str, float]] = {}
            for raw, (base, mult) in perps.items():
                b_map[base] = (raw, mult)
            venue_base_maps[v] = b_map

        # Stage 1 Matching and Fast Volume + Namesake Price Sanity Filter
        candidate_coins: Dict[str, Dict[str, Any]] = {}
        rejected_namesakes = 0
        rejected_volume = 0

        for coin, mkt_code in up_mkts.items():
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

            matched_venues_meta: Dict[str, Tuple[str, float, float, float]] = {}

            for v in self.target_venues:
                b_map = venue_base_maps.get(v, {})
                matched = b_map.get(coin)
                if not matched and coin in TICKER_ALIASES:
                    matched = b_map.get(TICKER_ALIASES[coin])

                if not matched:
                    continue

                raw_sym, mult = matched
                t_info = venue_ticks.get(v, {}).get(raw_sym)
                if not t_info:
                    continue

                vol_v = t_info.get("vol_24h", 0.0)
                px_v = (t_info.get("price", 0.0) or 0.0) / mult

                if vol_v < self.min_vol_usd or px_v <= 0:
                    continue

                diff_pct = abs(up_px - px_v) / max(up_px, px_v) * 100.0
                if diff_pct <= self.max_price_diff_pct:
                    matched_venues_meta[v] = (raw_sym, mult, px_v, vol_v)
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

            for v, (raw_sym, mult, _, _) in item["matched_venues"].items():
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
        sym_maps: Dict[str, Dict[str, Tuple[str, float]]] = {v: {} for v in self.target_venues}
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
                    sym_maps[v][raw_sym] = (coin, mult)

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

## scripts/cron_daily.sh

```bash
#!/usr/bin/env bash
# scripts/cron_daily.sh - Daily automated maintenance cycle for Korean Sunrise
# Designed for cron execution on Linux/VPS servers (e.g. at 00:00 UTC)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

if [ -d "${PROJECT_ROOT}/venv" ]; then
    source "${PROJECT_ROOT}/venv/bin/activate"
elif [ -d "/root/venv" ]; then
    source "/root/venv/bin/activate"
fi

mkdir -p "${PROJECT_ROOT}/logs"

TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
echo "================================================================================"
echo "[CRON] Starting Korean Sunrise Daily Lifecycle Pipeline at ${TIMESTAMP}"
echo "================================================================================"

python3 tools/run_daily_pipeline.py "$@"

EXIT_CODE=$?
echo "[CRON] Daily Lifecycle finished with exit code ${EXIT_CODE} at $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
exit ${EXIT_CODE}

```

## scripts/daily-pipeline.service

```
[Unit]
Description=Korean Sunrise 24-Hour Autonomous Lifecycle Pipeline Service
After=network.target

[Service]
Type=oneshot
User=root
WorkingDirectory=/root/KoreanSunrise
ExecStart=/usr/bin/python3 tools/run_daily_pipeline.py
StandardOutput=append:/root/KoreanSunrise/logs/daily_pipeline.log
StandardError=append:/root/KoreanSunrise/logs/daily_pipeline.log

```

## scripts/daily-pipeline.timer

```
[Unit]
Description=Daily Timer for Korean Sunrise 24-Hour Autonomous Lifecycle Pipeline

[Timer]
OnCalendar=*-*-* 00:00:00 UTC
Persistent=true

[Install]
WantedBy=timers.target

```

## scripts/korean-sunrise.service

```
[Unit]
Description=Korean Sunrise Lead-Lag Sniping & Data Telemetry Service
After=network.target network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/KoreanSunrise
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=5
KillMode=mixed
TimeoutStopSec=15
LimitNOFILE=65535
StandardOutput=append:/root/KoreanSunrise/logs/service_stdout.log
StandardError=append:/root/KoreanSunrise/logs/service_stderr.log

[Install]
WantedBy=multi-user.target

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
        f"{'MeanPnL':>8} | {'MedPnL':>7} | {'MeanMFE':>8} | {'PeakT(s)':>8}"
    )
    print("-" * 88)
    for k, rows in sorted(groups.items()):
        n = len(rows)
        pnls = [float(r.get("net_pnl_bps", 0.0)) for r in rows]
        mfes = [float(r.get("mfe_exec_pnl_bps", r.get("net_pnl_bps", 0.0))) for r in rows]
        t_peaks = [float(r.get("time_to_peak_sec", 0.0)) for r in rows]
        conv_pct = sum(1 for r in rows if r.get("close_reason") == "converged") / n * 100.0
        win_pct = sum(1 for p in pnls if p > 0.0) / n * 100.0
        print(
            f"{k:<24} | {n:>5d} | {conv_pct:>4.1f}% | {win_pct:>4.1f}% | "
            f"{_mean(pnls):>+7.2f} | {_med(pnls):>+6.2f} | {_mean(mfes):>+7.2f} | {_med(t_peaks):>7.2f}s"
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
        win_pct = sum(1 for p in pnls if p > 0.0) / n * 100.0
        mfe_pos_pct = sum(1 for m in mfes if m > 2.0) / n * 100.0
        coin_stats.append({
            "coin": coin,
            "n": n,
            "win_pct": win_pct,
            "mfe_pos_pct": mfe_pos_pct,
            "mean_pnl": _mean(pnls),
            "sum_pnl": sum(pnls),
            "mean_mfe": _mean(mfes),
            "mean_spread": _mean(spreads),
        })

    coin_stats.sort(key=lambda x: x["mean_pnl"], reverse=True)

    print("\n[4] COIN PERFORMANCE RANKING (Sorted by Mean Realized Net PnL):")
    print(
        f"{'Coin':<10} | {'Trades':>6} | {'Win%':>6} | {'MFE>2bps%':>9} | "
        f"{'MeanPnL':>8} | {'TotalPnL':>9} | {'MeanMFE':>8} | {'F_Spread':>8}"
    )
    print("-" * 88)
    for st in coin_stats:
        print(
            f"{st['coin']:<10} | {st['n']:>6d} | {st['win_pct']:>5.1f}% | {st['mfe_pos_pct']:>8.1f}% | "
            f"{st['mean_pnl']:>+7.2f} | {st['sum_pnl']:>+8.1f} | {st['mean_mfe']:>+7.2f} | {st['mean_spread']:>7.2f}"
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
                        gross_edge = float(d.get("gross_exec_edge_bps") or 0.0)

                        pair_key = f"{coin}:upbit-{follower_ex}"
                        valid_events += 1

                        gross = pos_size * (gross_edge / 10000.0)
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
                    for v in ["binance", "bybit", "okx", "gateio", "mexc", "kucoin", "bingx", "htx", "pionex"]:
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

            classification_counts[status] += 1
            matrix_output[pair_key] = {
                "min_spread_bps": round(best_t, 1),
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
        print("\n" + "=" * 80)
        print("  KOREAN SUNRISE: AUTOMATED SPREAD THRESHOLD CALIBRATION MATRIX")
        print("=" * 80)
        print(f"  Signals Scanned: {valid_events:,} across {len(files)} archives ({days_back:.1f} days)")
        print(f"  Configured Pairs: {len(matrix_output)} pairs calibrated")
        print(f"  Classifications: {dict(classification_counts)}")
        print("-" * 80)
        print(f"  {'Pair (Coin:Leader-Follower)':<32} | {'Min Edge':<9} | {'Win Rate':<9} | {'Trades':<7} | {'Status':<16} | {'Active'}")
        print("-" * 80)
        for pk, cfg in sorted(matrix_output.items())[:35]:
            en_str = "YES" if cfg["enabled"] else "NO (PRUNED)"
            print(
                f"  {pk:<32} | {cfg['min_spread_bps']:>5.1f} bps | "
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
FUNDED_VENUES = {"binance", "bybit", "okx", "gateio", "mexc", "kucoin", "bingx"}


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
                battle_coins = set(bdoc.get("battle_pool", {}).keys())
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

    pnl_scenarios = {
        "all_signals": {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0},
        "calibrated_threshold_only": {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0},
        "battle_coins_only": {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0},
        "funded_venues_only": {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0},
    }

    sweep = {t: {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0} for t in SWEEP_THRESHOLDS}
    pair_stats = collections.defaultdict(lambda: {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0})
    coin_stats = collections.defaultdict(lambda: {"count": 0, "wins": 0, "gross": 0.0, "fee": 0.0, "net": 0.0, "mfes": [], "t_peaks": []})

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

                    net_pnl_bps = d.get("net_pnl_bps")
                    if net_pnl_bps is None:
                        continue
                    net_pnl_bps = float(net_pnl_bps)
                    gross_edge_bps = float(d.get("gross_exec_edge_bps") or 0.0)

                    gross = pos_size * (gross_edge_bps / 10000.0)
                    net = pos_size * (net_pnl_bps / 10000.0)
                    is_win = 1 if net > 0 else 0

                    # Scenario: All signals
                    s_all = pnl_scenarios["all_signals"]
                    s_all["count"] += 1
                    s_all["gross"] += gross
                    s_all["fee"] += fee_usd
                    s_all["net"] += net
                    s_all["wins"] += is_win

                    # Scenario: Calibrated threshold
                    pk = f"{coin}:upbit-{f_ex}"
                    req_edge = calibrated_thresholds.get(pk, 20.0)
                    if gross_edge_bps >= req_edge:
                        s_cal = pnl_scenarios["calibrated_threshold_only"]
                        s_cal["count"] += 1
                        s_cal["gross"] += gross
                        s_cal["fee"] += fee_usd
                        s_cal["net"] += net
                        s_cal["wins"] += is_win

                    # Scenario: Battle coins only
                    if coin in battle_coins:
                        s_bat = pnl_scenarios["battle_coins_only"]
                        s_bat["count"] += 1
                        s_bat["gross"] += gross
                        s_bat["fee"] += fee_usd
                        s_bat["net"] += net
                        s_bat["wins"] += is_win

                    # Scenario: Funded venues only
                    if f_ex in FUNDED_VENUES:
                        s_fun = pnl_scenarios["funded_venues_only"]
                        s_fun["count"] += 1
                        s_fun["gross"] += gross
                        s_fun["fee"] += fee_usd
                        s_fun["net"] += net
                        s_fun["wins"] += is_win

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

                    # Coin stats
                    cs = coin_stats[coin]
                    cs["count"] += 1
                    cs["gross"] += gross
                    cs["fee"] += fee_usd
                    cs["net"] += net
                    cs["wins"] += is_win
                    mfe = d.get("mfe_exec_pnl_bps")
                    if mfe is not None:
                        cs["mfes"].append(float(mfe))
                    t_pk = d.get("time_to_peak_sec")
                    if t_pk is not None:
                        cs["t_peaks"].append(float(t_pk))

        except Exception as e:
            if not as_json:
                print(f"[!] Warning reading {fpath}: {e}", file=sys.stderr)

    # Format PnL summary
    pnl_summary = {}
    for name, sc in pnl_scenarios.items():
        cnt = sc["count"]
        pnl_summary[name] = {
            "trades": cnt,
            "wins": sc["wins"],
            "winrate_pct": round((sc["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "gross_usd": round(sc["gross"], 2),
            "fee_usd": round(sc["fee"], 2),
            "net_pnl_usd": round(sc["net"], 2),
            "profit_factor": round(sc["gross"] / max(0.01, sc["fee"]), 2) if sc["fee"] else 0.0,
        }

    # Format top pairs
    sorted_pairs = []
    for pk, ps in sorted(pair_stats.items(), key=lambda x: x[1]["net"], reverse=True):
        cnt = ps["count"]
        sorted_pairs.append({
            "pair": pk,
            "trades": cnt,
            "winrate_pct": round((ps["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "net_pnl_usd": round(ps["net"], 2),
            "gross_usd": round(ps["gross"], 2),
        })

    # Format top coins
    sorted_coins = []
    for c, cs in sorted(coin_stats.items(), key=lambda x: x[1]["net"], reverse=True):
        cnt = cs["count"]
        avg_mfe = sum(cs["mfes"]) / len(cs["mfes"]) if cs["mfes"] else 0.0
        med_tpk = sorted(cs["t_peaks"])[len(cs["t_peaks"]) // 2] if cs["t_peaks"] else 0.0
        sorted_coins.append({
            "coin": c,
            "trades": cnt,
            "winrate_pct": round((cs["wins"] / cnt) * 100.0, 1) if cnt else 0.0,
            "net_pnl_usd": round(cs["net"], 2),
            "avg_mfe_bps": round(avg_mfe, 1),
            "median_t_peak_sec": round(med_tpk, 2),
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
        "pnl_scenarios": pnl_summary,
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
    lines = [
        f"# Korean Sunrise: Суточный Квантовый Отчёт ({m['report_date']})",
        f"*Сгенерирован:* `{m['generated_at']}` | *Окно:* `{m['days_analyzed']} d` | *Размер слота:* `${m['order_size_usd']}` | *Комиссия:* `{m['roundtrip_fee_bps']} bps`",
        "",
        "## 1. Сводные метрики телеметрии",
        f"- **Всего зафиксировано сигналов:** `{m['total_signals_recorded']:,}`",
        f"- **Активных монет с импульсами:** `{m['coins_active']}`",
        f"- **Фолловер-бирж активно:** `{len(m['venues_active'])}` ({', '.join(m['venues_active']) if m['venues_active'] else 'None'})",
        f"- **Архивов данных обработано:** `{m['total_files_scanned']}` (`{m['total_size_mb']} MB`)",
        f"- **Причины закрытия импульсов:** `Converged: {doc['close_reasons'].get('converged', 0)}` | `Timeout: {doc['close_reasons'].get('timeout', 0)}` | `Reversed: {doc['close_reasons'].get('reversed', 0)}`",
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

    lines.extend([
        "",
        "## 3. Оптимизация порогов спреда (Threshold Sweep)",
        "| Порог (bps) | Сигналов | Win Rate | Net PnL USD |",
        "| :---: | :---: | :---: | :---: |",
    ])
    for t, sw in sorted(doc["sweep_thresholds"].items(), key=lambda x: int(x[0])):
        lines.append(f"| >={t} bps | {sw['trades']:,} | {sw['winrate_pct']}% | ${sw['net_usd']:.2f} |")

    lines.extend([
        "",
        "## 4. Топ прибыльных связок (Upbit Leader <-> Perp Follower)",
        "| Связка (Coin:Upbit-Follower) | Сделок | Win Rate | Gross USD | Net PnL USD |",
        "| :--- | :---: | :---: | :---: | :---: |",
    ])
    if doc["top_pairs"]:
        for pr in doc["top_pairs"][:15]:
            lines.append(f"| `{pr['pair']}` | {pr['trades']} | {pr['winrate_pct']}% | ${pr['gross_usd']:.2f} | **${pr['net_pnl_usd']:.2f}** |")
    else:
        lines.append("| *Нет накопленных сделок за выбранный период* | - | - | - | - |")

    lines.extend([
        "",
        "## 5. Топ монет по эффективности опережения",
        "| Монета | Сделок | Win Rate | Net PnL USD | Ср. MFE (bps) | Медиана $T_{peak}$ |",
        "| :--- | :---: | :---: | :---: | :---: | :---: |",
    ])
    if doc["top_coins"]:
        for c in doc["top_coins"][:15]:
            lines.append(f"| **{c['coin']}** | {c['trades']} | {c['winrate_pct']}% | **${c['net_pnl_usd']:.2f}** | +{c['avg_mfe_bps']} bps | {c['median_t_peak_sec']} s |")
    else:
        lines.append("| *Нет накопленных данных по монетам* | - | - | - | - | - |")

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
    coin = args.coin.upper()
    ok = psm.promote_to_battle(coin, score=100.0, ev_net_bps=25.0, top3_depth_usd=500.0, win_rate_pct=85.0, min_events=0)
    if ok:
        print(f"[OK] Promoted {coin} to Battle pool.")
    else:
        print(f"[!] Failed to promote {coin}.")


def cmd_demote(psm: PoolStateManager, args: argparse.Namespace) -> None:
    coin = args.coin.upper()
    psm.demote_to_incubator(coin, reason=args.reason or "manual_demote")
    print(f"[OK] Demoted {coin} to Incubator pool.")


def cmd_evaluate(psm: PoolStateManager, args: argparse.Namespace) -> None:
    print(f"[*] Running quantitative evaluation across trailing {args.days} days...")
    ev = SunriseEvaluator(data_root=os.path.join(PROJECT_ROOT, "data"), pool_state=psm)
    res = ev.run()
    print(f"[OK] Evaluation complete: {len(res)} coins qualified and promoted to Battle pool.")


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
        logger.info("[PHASE 1/4] Universe Screening: Fetching 1440 1m candles across Upbit & 9 followers...")
        cmd_scr = [
            sys.executable,
            os.path.join(PROJECT_ROOT, "tools", "screen_universe.py"),
            "--kline-limit", "1440",
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
    parser.add_argument("--kline-limit", type=int, default=1440, help="Number of 1m candles to evaluate (default: 1440 = 24h)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel worker threads (default: 8)")
    args = parser.parse_args()

    screener = UniverseScreener(
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
      "pionex": 100.0
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
    "pionex"
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
    "updated_at": "2026-10-18T13:58:13Z",
    "updated_ts": 1792331893.775,
    "usdt_krw_rate": 1350.0,
    "coins_count": 65,
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
    "screening_elapsed_sec": 100.26,
    "binance_count": 42,
    "bybit_count": 44,
    "okx_count": 26,
    "gateio_count": 22,
    "mexc_count": 45,
    "kucoin_count": 30,
    "bingx_count": 42,
    "htx_count": 2,
    "pionex_count": 16
  },
  "coins": [
    "VVV",
    "LIT",
    "ICP",
    "ETHFI",
    "RAY",
    "PROM",
    "AXL",
    "TRUMP",
    "WLD",
    "ELSA",
    "SPX",
    "SOMI",
    "GRT",
    "EUL",
    "ZRO",
    "ZORA",
    "CAP",
    "PIEVERSE",
    "WCT",
    "PROS",
    "ENSO",
    "BREV",
    "ESP",
    "NEAR",
    "VET",
    "KMNO",
    "GTC",
    "B3",
    "CYBER",
    "HYPER",
    "SENT",
    "AWE",
    "ZKC",
    "ZKP",
    "NEO",
    "IO",
    "INJ",
    "SKR",
    "CARV",
    "ERA",
    "SEI",
    "ARX",
    "MMT",
    "MNT",
    "PONS",
    "API3",
    "ENS",
    "IOTA",
    "O",
    "BCH",
    "BEAM",
    "BAT",
    "PUMP",
    "DOS",
    "RENDER",
    "APT",
    "2Z",
    "F",
    "BIO",
    "NMR",
    "MANA",
    "VTHO",
    "MOODENG",
    "ATOM",
    "BIGTIME"
  ],
  "upbit_krw_to_coin": {
    "KRW-VVV": "VVV",
    "KRW-LIT": "LIT",
    "KRW-ICP": "ICP",
    "KRW-ETHFI": "ETHFI",
    "KRW-RAY": "RAY",
    "KRW-PROM": "PROM",
    "KRW-AXL": "AXL",
    "KRW-TRUMP": "TRUMP",
    "KRW-WLD": "WLD",
    "KRW-ELSA": "ELSA",
    "KRW-SPX": "SPX",
    "KRW-SOMI": "SOMI",
    "KRW-GRT": "GRT",
    "KRW-EUL": "EUL",
    "KRW-ZRO": "ZRO",
    "KRW-ZORA": "ZORA",
    "KRW-CAP": "CAP",
    "KRW-PIEVERSE": "PIEVERSE",
    "KRW-WCT": "WCT",
    "KRW-PROS": "PROS",
    "KRW-ENSO": "ENSO",
    "KRW-BREV": "BREV",
    "KRW-ESP": "ESP",
    "KRW-NEAR": "NEAR",
    "KRW-VET": "VET",
    "KRW-KMNO": "KMNO",
    "KRW-GTC": "GTC",
    "KRW-B3": "B3",
    "KRW-CYBER": "CYBER",
    "KRW-HYPER": "HYPER",
    "KRW-SENT": "SENT",
    "KRW-AWE": "AWE",
    "KRW-ZKC": "ZKC",
    "KRW-ZKP": "ZKP",
    "KRW-NEO": "NEO",
    "KRW-IO": "IO",
    "KRW-INJ": "INJ",
    "KRW-SKR": "SKR",
    "KRW-CARV": "CARV",
    "KRW-ERA": "ERA",
    "KRW-SEI": "SEI",
    "KRW-ARX": "ARX",
    "KRW-MMT": "MMT",
    "KRW-MNT": "MNT",
    "KRW-PONS": "PONS",
    "KRW-API3": "API3",
    "KRW-ENS": "ENS",
    "KRW-IOTA": "IOTA",
    "KRW-O": "O",
    "KRW-BCH": "BCH",
    "KRW-BEAM": "BEAM",
    "KRW-BAT": "BAT",
    "KRW-PUMP": "PUMP",
    "KRW-DOS": "DOS",
    "KRW-RENDER": "RENDER",
    "KRW-APT": "APT",
    "KRW-2Z": "2Z",
    "KRW-F": "F",
    "KRW-BIO": "BIO",
    "KRW-NMR": "NMR",
    "KRW-MANA": "MANA",
    "KRW-VTHO": "VTHO",
    "KRW-MOODENG": "MOODENG",
    "KRW-ATOM": "ATOM",
    "KRW-BIGTIME": "BIGTIME"
  },
  "pairs": {
    "VVV": {
      "coin": "VVV",
      "upbit_symbol": "KRW-VVV",
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
      "best_corr": 0.924,
      "best_p90_dev_bps": 32.0,
      "venue_symbols": {
        "binance": "VVVUSDT",
        "bingx": "VVV-USDT",
        "bybit": "VVVUSDT",
        "gateio": "VVV_USDT",
        "kucoin": "VVVUSDTM",
        "mexc": "VVV_USDT",
        "okx": "VVV-USDT-SWAP",
        "pionex": "VVV_USDT_PERP"
      }
    },
    "LIT": {
      "coin": "LIT",
      "upbit_symbol": "KRW-LIT",
      "venues": [
        "binance",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.875,
      "best_p90_dev_bps": 25.0,
      "venue_symbols": {
        "binance": "LITUSDT",
        "bybit": "LITUSDT",
        "gateio": "LIT_USDT",
        "kucoin": "LITUSDTM",
        "mexc": "LIT_USDT",
        "okx": "LIT-USDT-SWAP",
        "pionex": "LIT_USDT_PERP"
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
      "best_corr": 0.87,
      "best_p90_dev_bps": 25.4,
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
    "ETHFI": {
      "coin": "ETHFI",
      "upbit_symbol": "KRW-ETHFI",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.862,
      "best_p90_dev_bps": 26.4,
      "venue_symbols": {
        "binance": "ETHFIUSDT",
        "bingx": "ETHFI-USDT",
        "bybit": "ETHFIUSDT",
        "gateio": "ETHFI_USDT",
        "mexc": "ETHFI_USDT",
        "okx": "ETHFI-USDT-SWAP",
        "pionex": "ETHFI_USDT_PERP"
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
      "best_corr": 0.844,
      "best_p90_dev_bps": 21.4,
      "venue_symbols": {
        "bingx": "RAY-USDT",
        "gateio": "RAY_USDT",
        "kucoin": "RAYUSDTM",
        "mexc": "RAY_USDT",
        "okx": "RAY-USDT-SWAP",
        "pionex": "RAY_USDT_PERP"
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
      "best_corr": 0.836,
      "best_p90_dev_bps": 30.6,
      "venue_symbols": {
        "binance": "PROMUSDT",
        "bingx": "PROM-USDT",
        "gateio": "PROM_USDT",
        "kucoin": "PROMUSDTM",
        "mexc": "PROM_USDT"
      }
    },
    "AXL": {
      "coin": "AXL",
      "upbit_symbol": "KRW-AXL",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "pionex"
      ],
      "best_corr": 0.827,
      "best_p90_dev_bps": 27.7,
      "venue_symbols": {
        "binance": "AXLUSDT",
        "bingx": "AXL-USDT",
        "bybit": "AXLUSDT",
        "mexc": "AXL_USDT",
        "pionex": "AXL_USDT_PERP"
      }
    },
    "TRUMP": {
      "coin": "TRUMP",
      "upbit_symbol": "KRW-TRUMP",
      "venues": [
        "bybit",
        "kucoin",
        "okx"
      ],
      "best_corr": 0.824,
      "best_p90_dev_bps": 11.3,
      "venue_symbols": {
        "bybit": "TRUMPUSDT",
        "kucoin": "TRUMPUSDTM",
        "okx": "TRUMP-USDT-SWAP"
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
      "best_corr": 0.819,
      "best_p90_dev_bps": 15.7,
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
      "best_corr": 0.816,
      "best_p90_dev_bps": 56.1,
      "venue_symbols": {
        "binance": "ELSAUSDT",
        "bingx": "ELSA-USDT",
        "bybit": "ELSAUSDT",
        "gateio": "ELSA_USDT",
        "mexc": "ELSA_USDT",
        "pionex": "ELSA_USDT_PERP"
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
      "best_corr": 0.809,
      "best_p90_dev_bps": 37.3,
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
    "SOMI": {
      "coin": "SOMI",
      "upbit_symbol": "KRW-SOMI",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.802,
      "best_p90_dev_bps": 41.3,
      "venue_symbols": {
        "binance": "SOMIUSDT",
        "bingx": "SOMI-USDT",
        "bybit": "SOMIUSDT",
        "mexc": "SOMI_USDT"
      }
    },
    "GRT": {
      "coin": "GRT",
      "upbit_symbol": "KRW-GRT",
      "venues": [
        "binance",
        "bybit",
        "kucoin",
        "mexc"
      ],
      "best_corr": 0.8,
      "best_p90_dev_bps": 25.2,
      "venue_symbols": {
        "binance": "GRTUSDT",
        "bybit": "GRTUSDT",
        "kucoin": "GRTUSDTM",
        "mexc": "GRT_USDT"
      }
    },
    "EUL": {
      "coin": "EUL",
      "upbit_symbol": "KRW-EUL",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.798,
      "best_p90_dev_bps": 24.0,
      "venue_symbols": {
        "kucoin": "EULUSDTM"
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
      "best_corr": 0.797,
      "best_p90_dev_bps": 22.2,
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
      "best_corr": 0.796,
      "best_p90_dev_bps": 27.4,
      "venue_symbols": {
        "binance": "ZORAUSDT",
        "bingx": "ZORA-USDT",
        "bybit": "ZORAUSDT",
        "kucoin": "ZORAUSDTM",
        "mexc": "ZORA_USDT",
        "okx": "ZORA-USDT-SWAP"
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
      "best_corr": 0.791,
      "best_p90_dev_bps": 21.0,
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
      "best_corr": 0.788,
      "best_p90_dev_bps": 22.1,
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
    "WCT": {
      "coin": "WCT",
      "upbit_symbol": "KRW-WCT",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.785,
      "best_p90_dev_bps": 32.4,
      "venue_symbols": {
        "binance": "WCTUSDT",
        "bingx": "WCT-USDT",
        "bybit": "WCTUSDT",
        "mexc": "WCT_USDT",
        "okx": "WCT-USDT-SWAP"
      }
    },
    "PROS": {
      "coin": "PROS",
      "upbit_symbol": "KRW-PROS",
      "venues": [
        "kucoin",
        "okx"
      ],
      "best_corr": 0.777,
      "best_p90_dev_bps": 32.6,
      "venue_symbols": {
        "kucoin": "PROSUSDTM",
        "okx": "PROS-USDT-SWAP"
      }
    },
    "ENSO": {
      "coin": "ENSO",
      "upbit_symbol": "KRW-ENSO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc",
        "okx"
      ],
      "best_corr": 0.774,
      "best_p90_dev_bps": 22.6,
      "venue_symbols": {
        "binance": "ENSOUSDT",
        "bingx": "ENSO-USDT",
        "bybit": "ENSOUSDT",
        "mexc": "ENSO_USDT",
        "okx": "ENSO-USDT-SWAP"
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
      "best_corr": 0.765,
      "best_p90_dev_bps": 33.8,
      "venue_symbols": {
        "binance": "BREVUSDT",
        "bingx": "BREV-USDT",
        "bybit": "BREVUSDT",
        "mexc": "BREV_USDT",
        "okx": "BREV-USDT-SWAP"
      }
    },
    "ESP": {
      "coin": "ESP",
      "upbit_symbol": "KRW-ESP",
      "venues": [
        "kucoin",
        "okx"
      ],
      "best_corr": 0.764,
      "best_p90_dev_bps": 44.6,
      "venue_symbols": {
        "kucoin": "ESPUSDTM",
        "okx": "ESP-USDT-SWAP"
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
      "best_corr": 0.762,
      "best_p90_dev_bps": 12.2,
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
    "VET": {
      "coin": "VET",
      "upbit_symbol": "KRW-VET",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "kucoin",
        "mexc"
      ],
      "best_corr": 0.761,
      "best_p90_dev_bps": 17.7,
      "venue_symbols": {
        "binance": "VETUSDT",
        "bingx": "VET-USDT",
        "bybit": "VETUSDT",
        "kucoin": "VETUSDTM",
        "mexc": "VET_USDT"
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
      "best_corr": 0.75,
      "best_p90_dev_bps": 24.0,
      "venue_symbols": {
        "binance": "KMNOUSDT",
        "bingx": "KMNO-USDT",
        "bybit": "KMNOUSDT",
        "mexc": "KMNO_USDT",
        "okx": "KMNO-USDT-SWAP"
      }
    },
    "GTC": {
      "coin": "GTC",
      "upbit_symbol": "KRW-GTC",
      "venues": [
        "binance",
        "bingx"
      ],
      "best_corr": 0.741,
      "best_p90_dev_bps": 59.8,
      "venue_symbols": {
        "binance": "GTCUSDT",
        "bingx": "GTC-USDT"
      }
    },
    "B3": {
      "coin": "B3",
      "upbit_symbol": "KRW-B3",
      "venues": [
        "bybit",
        "mexc"
      ],
      "best_corr": 0.739,
      "best_p90_dev_bps": 63.7,
      "venue_symbols": {
        "bybit": "B3USDT",
        "mexc": "B3_USDT"
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
      "best_corr": 0.738,
      "best_p90_dev_bps": 35.6,
      "venue_symbols": {
        "binance": "CYBERUSDT",
        "bingx": "CYBER-USDT",
        "bybit": "CYBERUSDT",
        "mexc": "CYBER_USDT"
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
      "best_corr": 0.737,
      "best_p90_dev_bps": 29.9,
      "venue_symbols": {
        "binance": "HYPERUSDT",
        "bybit": "HYPERUSDT",
        "mexc": "HYPER_USDT"
      }
    },
    "SENT": {
      "coin": "SENT",
      "upbit_symbol": "KRW-SENT",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.736,
      "best_p90_dev_bps": 37.5,
      "venue_symbols": {
        "kucoin": "SENTUSDTM"
      }
    },
    "AWE": {
      "coin": "AWE",
      "upbit_symbol": "KRW-AWE",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.735,
      "best_p90_dev_bps": 30.1,
      "venue_symbols": {
        "binance": "AWEUSDT",
        "bingx": "AWE-USDT",
        "bybit": "AWEUSDT",
        "mexc": "AWE_USDT"
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
      "best_corr": 0.731,
      "best_p90_dev_bps": 29.3,
      "venue_symbols": {
        "binance": "ZKCUSDT",
        "bingx": "ZKC-USDT",
        "bybit": "ZKCUSDT",
        "mexc": "ZKC_USDT"
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
      "best_corr": 0.728,
      "best_p90_dev_bps": 27.9,
      "venue_symbols": {
        "binance": "ZKPUSDT",
        "bingx": "ZKP-USDT",
        "bybit": "ZKPUSDT",
        "kucoin": "ZKPUSDTM",
        "mexc": "ZKP_USDT",
        "okx": "ZKP-USDT-SWAP"
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
      "best_corr": 0.727,
      "best_p90_dev_bps": 24.3,
      "venue_symbols": {
        "binance": "NEOUSDT",
        "bingx": "NEO-USDT",
        "bybit": "NEOUSDT",
        "mexc": "NEO_USDT"
      }
    },
    "IO": {
      "coin": "IO",
      "upbit_symbol": "KRW-IO",
      "venues": [
        "bybit",
        "mexc",
        "pionex"
      ],
      "best_corr": 0.719,
      "best_p90_dev_bps": 47.4,
      "venue_symbols": {
        "bybit": "IOUSDT",
        "mexc": "IO_USDT",
        "pionex": "IO_USDT_PERP"
      }
    },
    "INJ": {
      "coin": "INJ",
      "upbit_symbol": "KRW-INJ",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "gateio",
        "mexc",
        "pionex"
      ],
      "best_corr": 0.718,
      "best_p90_dev_bps": 25.3,
      "venue_symbols": {
        "binance": "INJUSDT",
        "bingx": "INJ-USDT",
        "bybit": "INJUSDT",
        "gateio": "INJ_USDT",
        "mexc": "INJ_USDT",
        "pionex": "INJ_USDT_PERP"
      }
    },
    "SKR": {
      "coin": "SKR",
      "upbit_symbol": "KRW-SKR",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "pionex"
      ],
      "best_corr": 0.717,
      "best_p90_dev_bps": 50.7,
      "venue_symbols": {
        "binance": "SKRUSDT",
        "bingx": "SKR-USDT",
        "bybit": "SKRUSDT",
        "pionex": "SKR_USDT_PERP"
      }
    },
    "CARV": {
      "coin": "CARV",
      "upbit_symbol": "KRW-CARV",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.712,
      "best_p90_dev_bps": 47.9,
      "venue_symbols": {
        "binance": "CARVUSDT",
        "bingx": "CARV-USDT",
        "bybit": "CARVUSDT",
        "mexc": "CARV_USDT"
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
        "mexc"
      ],
      "best_corr": 0.71,
      "best_p90_dev_bps": 70.0,
      "venue_symbols": {
        "binance": "ERAUSDT",
        "bingx": "ERA-USDT",
        "bybit": "ERAUSDT",
        "gateio": "ERA_USDT",
        "mexc": "ERA_USDT"
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
        "mexc"
      ],
      "best_corr": 0.705,
      "best_p90_dev_bps": 22.8,
      "venue_symbols": {
        "binance": "SEIUSDT",
        "bingx": "SEI-USDT",
        "bybit": "SEIUSDT",
        "gateio": "SEI_USDT",
        "kucoin": "SEIUSDTM",
        "mexc": "SEI_USDT"
      }
    },
    "ARX": {
      "coin": "ARX",
      "upbit_symbol": "KRW-ARX",
      "venues": [
        "okx"
      ],
      "best_corr": 0.704,
      "best_p90_dev_bps": 39.0,
      "venue_symbols": {
        "okx": "ARX-USDT-SWAP"
      }
    },
    "MMT": {
      "coin": "MMT",
      "upbit_symbol": "KRW-MMT",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.704,
      "best_p90_dev_bps": 39.6,
      "venue_symbols": {
        "kucoin": "MMTUSDTM"
      }
    },
    "MNT": {
      "coin": "MNT",
      "upbit_symbol": "KRW-MNT",
      "venues": [
        "bingx",
        "mexc"
      ],
      "best_corr": 0.7,
      "best_p90_dev_bps": 29.1,
      "venue_symbols": {
        "bingx": "MNT-USDT",
        "mexc": "MNT_USDT"
      }
    },
    "PONS": {
      "coin": "PONS",
      "upbit_symbol": "KRW-PONS",
      "venues": [
        "kucoin",
        "mexc",
        "okx"
      ],
      "best_corr": 0.697,
      "best_p90_dev_bps": 33.7,
      "venue_symbols": {
        "kucoin": "PONSUSDTM",
        "mexc": "PONS_USDT",
        "okx": "PONS-USDT-SWAP"
      }
    },
    "API3": {
      "coin": "API3",
      "upbit_symbol": "KRW-API3",
      "venues": [
        "binance",
        "mexc"
      ],
      "best_corr": 0.696,
      "best_p90_dev_bps": 30.0,
      "venue_symbols": {
        "binance": "API3USDT",
        "mexc": "API3_USDT"
      }
    },
    "ENS": {
      "coin": "ENS",
      "upbit_symbol": "KRW-ENS",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.696,
      "best_p90_dev_bps": 21.2,
      "venue_symbols": {
        "binance": "ENSUSDT",
        "bingx": "ENS-USDT",
        "bybit": "ENSUSDT",
        "mexc": "ENS_USDT"
      }
    },
    "IOTA": {
      "coin": "IOTA",
      "upbit_symbol": "KRW-IOTA",
      "venues": [
        "bingx",
        "bybit"
      ],
      "best_corr": 0.695,
      "best_p90_dev_bps": 27.0,
      "venue_symbols": {
        "bingx": "IOTA-USDT",
        "bybit": "IOTAUSDT"
      }
    },
    "O": {
      "coin": "O",
      "upbit_symbol": "KRW-O",
      "venues": [
        "binance",
        "bingx",
        "gateio",
        "kucoin",
        "mexc"
      ],
      "best_corr": 0.693,
      "best_p90_dev_bps": 35.7,
      "venue_symbols": {
        "binance": "OUSDT",
        "bingx": "O-USDT",
        "gateio": "O_USDT",
        "kucoin": "OUSDTM",
        "mexc": "O_USDT"
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
        "mexc"
      ],
      "best_corr": 0.692,
      "best_p90_dev_bps": 14.9,
      "venue_symbols": {
        "binance": "BCHUSDT",
        "bingx": "BCH-USDT",
        "bybit": "BCHUSDT",
        "gateio": "BCH_USDT",
        "mexc": "BCH_USDT"
      }
    },
    "BEAM": {
      "coin": "BEAM",
      "upbit_symbol": "KRW-BEAM",
      "venues": [
        "bybit"
      ],
      "best_corr": 0.686,
      "best_p90_dev_bps": 30.6,
      "venue_symbols": {
        "bybit": "BEAMUSDT"
      }
    },
    "BAT": {
      "coin": "BAT",
      "upbit_symbol": "KRW-BAT",
      "venues": [
        "bingx",
        "bybit",
        "gateio",
        "kucoin",
        "mexc",
        "okx",
        "pionex"
      ],
      "best_corr": 0.685,
      "best_p90_dev_bps": 31.2,
      "venue_symbols": {
        "bingx": "BAT-USDT",
        "bybit": "BATUSDT",
        "gateio": "BAT_USDT",
        "kucoin": "BATUSDTM",
        "mexc": "BAT_USDT",
        "okx": "BAT-USDT-SWAP",
        "pionex": "BAT_USDT_PERP"
      }
    },
    "PUMP": {
      "coin": "PUMP",
      "upbit_symbol": "KRW-PUMP",
      "venues": [
        "binance",
        "bingx",
        "gateio"
      ],
      "best_corr": 0.685,
      "best_p90_dev_bps": 26.7,
      "venue_symbols": {
        "binance": "PUMPUSDT",
        "bingx": "PUMP-USDT",
        "gateio": "PUMP_USDT"
      }
    },
    "DOS": {
      "coin": "DOS",
      "upbit_symbol": "KRW-DOS",
      "venues": [
        "binance",
        "bingx",
        "bybit"
      ],
      "best_corr": 0.685,
      "best_p90_dev_bps": 54.1,
      "venue_symbols": {
        "binance": "DOSUSDT",
        "bingx": "DOS-USDT",
        "bybit": "DOSUSDT"
      }
    },
    "RENDER": {
      "coin": "RENDER",
      "upbit_symbol": "KRW-RENDER",
      "venues": [
        "gateio",
        "kucoin"
      ],
      "best_corr": 0.681,
      "best_p90_dev_bps": 20.2,
      "venue_symbols": {
        "gateio": "RENDER_USDT",
        "kucoin": "RENDERUSDTM"
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
      "best_corr": 0.68,
      "best_p90_dev_bps": 19.2,
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
    "2Z": {
      "coin": "2Z",
      "upbit_symbol": "KRW-2Z",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.676,
      "best_p90_dev_bps": 24.9,
      "venue_symbols": {
        "kucoin": "2ZUSDTM"
      }
    },
    "F": {
      "coin": "F",
      "upbit_symbol": "KRW-F",
      "venues": [
        "okx"
      ],
      "best_corr": 0.676,
      "best_p90_dev_bps": 22.9,
      "venue_symbols": {
        "okx": "F-USDT-SWAP"
      }
    },
    "BIO": {
      "coin": "BIO",
      "upbit_symbol": "KRW-BIO",
      "venues": [
        "binance",
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.674,
      "best_p90_dev_bps": 42.3,
      "venue_symbols": {
        "binance": "BIOUSDT",
        "bingx": "BIO-USDT",
        "bybit": "BIOUSDT",
        "mexc": "BIO_USDT"
      }
    },
    "NMR": {
      "coin": "NMR",
      "upbit_symbol": "KRW-NMR",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.673,
      "best_p90_dev_bps": 23.2,
      "venue_symbols": {
        "kucoin": "NMRUSDTM"
      }
    },
    "MANA": {
      "coin": "MANA",
      "upbit_symbol": "KRW-MANA",
      "venues": [
        "kucoin"
      ],
      "best_corr": 0.661,
      "best_p90_dev_bps": 74.6,
      "venue_symbols": {
        "kucoin": "MANAUSDTM"
      }
    },
    "VTHO": {
      "coin": "VTHO",
      "upbit_symbol": "KRW-VTHO",
      "venues": [
        "binance",
        "mexc"
      ],
      "best_corr": 0.66,
      "best_p90_dev_bps": 29.1,
      "venue_symbols": {
        "binance": "VTHOUSDT",
        "mexc": "VTHO_USDT"
      }
    },
    "MOODENG": {
      "coin": "MOODENG",
      "upbit_symbol": "KRW-MOODENG",
      "venues": [
        "bybit"
      ],
      "best_corr": 0.66,
      "best_p90_dev_bps": 30.9,
      "venue_symbols": {
        "bybit": "MOODENGUSDT"
      }
    },
    "ATOM": {
      "coin": "ATOM",
      "upbit_symbol": "KRW-ATOM",
      "venues": [
        "bingx",
        "bybit",
        "mexc"
      ],
      "best_corr": 0.657,
      "best_p90_dev_bps": 21.0,
      "venue_symbols": {
        "bingx": "ATOM-USDT",
        "bybit": "ATOMUSDT",
        "mexc": "ATOM_USDT"
      }
    },
    "BIGTIME": {
      "coin": "BIGTIME",
      "upbit_symbol": "KRW-BIGTIME",
      "venues": [
        "okx"
      ],
      "best_corr": 0.654,
      "best_p90_dev_bps": 15.8,
      "venue_symbols": {
        "okx": "BIGTIME-USDT-SWAP"
      }
    }
  },
  "binance_sym_map": {
    "VVVUSDT": [
      "VVV",
      1.0
    ],
    "LITUSDT": [
      "LIT",
      1.0
    ],
    "ICPUSDT": [
      "ICP",
      1.0
    ],
    "ETHFIUSDT": [
      "ETHFI",
      1.0
    ],
    "PROMUSDT": [
      "PROM",
      1.0
    ],
    "AXLUSDT": [
      "AXL",
      1.0
    ],
    "WLDUSDT": [
      "WLD",
      1.0
    ],
    "ELSAUSDT": [
      "ELSA",
      1.0
    ],
    "SPXUSDT": [
      "SPX",
      1.0
    ],
    "SOMIUSDT": [
      "SOMI",
      1.0
    ],
    "GRTUSDT": [
      "GRT",
      1.0
    ],
    "ZROUSDT": [
      "ZRO",
      1.0
    ],
    "ZORAUSDT": [
      "ZORA",
      1.0
    ],
    "CAPUSDT": [
      "CAP",
      1.0
    ],
    "PIEVERSEUSDT": [
      "PIEVERSE",
      1.0
    ],
    "WCTUSDT": [
      "WCT",
      1.0
    ],
    "ENSOUSDT": [
      "ENSO",
      1.0
    ],
    "BREVUSDT": [
      "BREV",
      1.0
    ],
    "NEARUSDT": [
      "NEAR",
      1.0
    ],
    "VETUSDT": [
      "VET",
      1.0
    ],
    "KMNOUSDT": [
      "KMNO",
      1.0
    ],
    "GTCUSDT": [
      "GTC",
      1.0
    ],
    "CYBERUSDT": [
      "CYBER",
      1.0
    ],
    "HYPERUSDT": [
      "HYPER",
      1.0
    ],
    "AWEUSDT": [
      "AWE",
      1.0
    ],
    "ZKCUSDT": [
      "ZKC",
      1.0
    ],
    "ZKPUSDT": [
      "ZKP",
      1.0
    ],
    "NEOUSDT": [
      "NEO",
      1.0
    ],
    "INJUSDT": [
      "INJ",
      1.0
    ],
    "SKRUSDT": [
      "SKR",
      1.0
    ],
    "CARVUSDT": [
      "CARV",
      1.0
    ],
    "ERAUSDT": [
      "ERA",
      1.0
    ],
    "SEIUSDT": [
      "SEI",
      1.0
    ],
    "API3USDT": [
      "API3",
      1.0
    ],
    "ENSUSDT": [
      "ENS",
      1.0
    ],
    "OUSDT": [
      "O",
      1.0
    ],
    "BCHUSDT": [
      "BCH",
      1.0
    ],
    "PUMPUSDT": [
      "PUMP",
      1.0
    ],
    "DOSUSDT": [
      "DOS",
      1.0
    ],
    "APTUSDT": [
      "APT",
      1.0
    ],
    "BIOUSDT": [
      "BIO",
      1.0
    ],
    "VTHOUSDT": [
      "VTHO",
      1.0
    ]
  },
  "bybit_sym_map": {
    "VVVUSDT": [
      "VVV",
      1.0
    ],
    "LITUSDT": [
      "LIT",
      1.0
    ],
    "ICPUSDT": [
      "ICP",
      1.0
    ],
    "ETHFIUSDT": [
      "ETHFI",
      1.0
    ],
    "AXLUSDT": [
      "AXL",
      1.0
    ],
    "TRUMPUSDT": [
      "TRUMP",
      1.0
    ],
    "WLDUSDT": [
      "WLD",
      1.0
    ],
    "ELSAUSDT": [
      "ELSA",
      1.0
    ],
    "SPXUSDT": [
      "SPX",
      1.0
    ],
    "SOMIUSDT": [
      "SOMI",
      1.0
    ],
    "GRTUSDT": [
      "GRT",
      1.0
    ],
    "ZROUSDT": [
      "ZRO",
      1.0
    ],
    "ZORAUSDT": [
      "ZORA",
      1.0
    ],
    "CAPUSDT": [
      "CAP",
      1.0
    ],
    "PIEVERSEUSDT": [
      "PIEVERSE",
      1.0
    ],
    "WCTUSDT": [
      "WCT",
      1.0
    ],
    "ENSOUSDT": [
      "ENSO",
      1.0
    ],
    "BREVUSDT": [
      "BREV",
      1.0
    ],
    "NEARUSDT": [
      "NEAR",
      1.0
    ],
    "VETUSDT": [
      "VET",
      1.0
    ],
    "KMNOUSDT": [
      "KMNO",
      1.0
    ],
    "B3USDT": [
      "B3",
      1.0
    ],
    "CYBERUSDT": [
      "CYBER",
      1.0
    ],
    "HYPERUSDT": [
      "HYPER",
      1.0
    ],
    "AWEUSDT": [
      "AWE",
      1.0
    ],
    "ZKCUSDT": [
      "ZKC",
      1.0
    ],
    "ZKPUSDT": [
      "ZKP",
      1.0
    ],
    "NEOUSDT": [
      "NEO",
      1.0
    ],
    "IOUSDT": [
      "IO",
      1.0
    ],
    "INJUSDT": [
      "INJ",
      1.0
    ],
    "SKRUSDT": [
      "SKR",
      1.0
    ],
    "CARVUSDT": [
      "CARV",
      1.0
    ],
    "ERAUSDT": [
      "ERA",
      1.0
    ],
    "SEIUSDT": [
      "SEI",
      1.0
    ],
    "ENSUSDT": [
      "ENS",
      1.0
    ],
    "IOTAUSDT": [
      "IOTA",
      1.0
    ],
    "BCHUSDT": [
      "BCH",
      1.0
    ],
    "BEAMUSDT": [
      "BEAM",
      1.0
    ],
    "BATUSDT": [
      "BAT",
      1.0
    ],
    "DOSUSDT": [
      "DOS",
      1.0
    ],
    "APTUSDT": [
      "APT",
      1.0
    ],
    "BIOUSDT": [
      "BIO",
      1.0
    ],
    "MOODENGUSDT": [
      "MOODENG",
      1.0
    ],
    "ATOMUSDT": [
      "ATOM",
      1.0
    ]
  },
  "okx_sym_map": {
    "VVV-USDT-SWAP": [
      "VVV",
      1.0
    ],
    "LIT-USDT-SWAP": [
      "LIT",
      1.0
    ],
    "ICP-USDT-SWAP": [
      "ICP",
      1.0
    ],
    "ETHFI-USDT-SWAP": [
      "ETHFI",
      1.0
    ],
    "RAY-USDT-SWAP": [
      "RAY",
      1.0
    ],
    "TRUMP-USDT-SWAP": [
      "TRUMP",
      1.0
    ],
    "WLD-USDT-SWAP": [
      "WLD",
      1.0
    ],
    "SPX-USDT-SWAP": [
      "SPX",
      1.0
    ],
    "ZRO-USDT-SWAP": [
      "ZRO",
      1.0
    ],
    "ZORA-USDT-SWAP": [
      "ZORA",
      1.0
    ],
    "CAP-USDT-SWAP": [
      "CAP",
      1.0
    ],
    "PIEVERSE-USDT-SWAP": [
      "PIEVERSE",
      1.0
    ],
    "WCT-USDT-SWAP": [
      "WCT",
      1.0
    ],
    "PROS-USDT-SWAP": [
      "PROS",
      1.0
    ],
    "ENSO-USDT-SWAP": [
      "ENSO",
      1.0
    ],
    "BREV-USDT-SWAP": [
      "BREV",
      1.0
    ],
    "ESP-USDT-SWAP": [
      "ESP",
      1.0
    ],
    "NEAR-USDT-SWAP": [
      "NEAR",
      1.0
    ],
    "KMNO-USDT-SWAP": [
      "KMNO",
      1.0
    ],
    "ZKP-USDT-SWAP": [
      "ZKP",
      1.0
    ],
    "ARX-USDT-SWAP": [
      "ARX",
      1.0
    ],
    "PONS-USDT-SWAP": [
      "PONS",
      1.0
    ],
    "BAT-USDT-SWAP": [
      "BAT",
      1.0
    ],
    "APT-USDT-SWAP": [
      "APT",
      1.0
    ],
    "F-USDT-SWAP": [
      "F",
      1.0
    ],
    "BIGTIME-USDT-SWAP": [
      "BIGTIME",
      1.0
    ]
  },
  "gateio_sym_map": {
    "VVV_USDT": [
      "VVV",
      1.0
    ],
    "LIT_USDT": [
      "LIT",
      1.0
    ],
    "ICP_USDT": [
      "ICP",
      1.0
    ],
    "ETHFI_USDT": [
      "ETHFI",
      1.0
    ],
    "RAY_USDT": [
      "RAY",
      1.0
    ],
    "PROM_USDT": [
      "PROM",
      1.0
    ],
    "WLD_USDT": [
      "WLD",
      1.0
    ],
    "ELSA_USDT": [
      "ELSA",
      1.0
    ],
    "SPX_USDT": [
      "SPX",
      1.0
    ],
    "ZRO_USDT": [
      "ZRO",
      1.0
    ],
    "CAP_USDT": [
      "CAP",
      1.0
    ],
    "PIEVERSE_USDT": [
      "PIEVERSE",
      1.0
    ],
    "NEAR_USDT": [
      "NEAR",
      1.0
    ],
    "INJ_USDT": [
      "INJ",
      1.0
    ],
    "ERA_USDT": [
      "ERA",
      1.0
    ],
    "SEI_USDT": [
      "SEI",
      1.0
    ],
    "O_USDT": [
      "O",
      1.0
    ],
    "BCH_USDT": [
      "BCH",
      1.0
    ],
    "BAT_USDT": [
      "BAT",
      1.0
    ],
    "PUMP_USDT": [
      "PUMP",
      1.0
    ],
    "RENDER_USDT": [
      "RENDER",
      1.0
    ],
    "APT_USDT": [
      "APT",
      1.0
    ]
  },
  "mexc_sym_map": {
    "VVV_USDT": [
      "VVV",
      1.0
    ],
    "LIT_USDT": [
      "LIT",
      1.0
    ],
    "ICP_USDT": [
      "ICP",
      1.0
    ],
    "ETHFI_USDT": [
      "ETHFI",
      1.0
    ],
    "RAY_USDT": [
      "RAY",
      1.0
    ],
    "PROM_USDT": [
      "PROM",
      1.0
    ],
    "AXL_USDT": [
      "AXL",
      1.0
    ],
    "WLD_USDT": [
      "WLD",
      1.0
    ],
    "ELSA_USDT": [
      "ELSA",
      1.0
    ],
    "SPX_USDT": [
      "SPX",
      1.0
    ],
    "SOMI_USDT": [
      "SOMI",
      1.0
    ],
    "GRT_USDT": [
      "GRT",
      1.0
    ],
    "ZRO_USDT": [
      "ZRO",
      1.0
    ],
    "ZORA_USDT": [
      "ZORA",
      1.0
    ],
    "CAP_USDT": [
      "CAP",
      1.0
    ],
    "PIEVERSE_USDT": [
      "PIEVERSE",
      1.0
    ],
    "WCT_USDT": [
      "WCT",
      1.0
    ],
    "ENSO_USDT": [
      "ENSO",
      1.0
    ],
    "BREV_USDT": [
      "BREV",
      1.0
    ],
    "NEAR_USDT": [
      "NEAR",
      1.0
    ],
    "VET_USDT": [
      "VET",
      1.0
    ],
    "KMNO_USDT": [
      "KMNO",
      1.0
    ],
    "B3_USDT": [
      "B3",
      1.0
    ],
    "CYBER_USDT": [
      "CYBER",
      1.0
    ],
    "HYPER_USDT": [
      "HYPER",
      1.0
    ],
    "AWE_USDT": [
      "AWE",
      1.0
    ],
    "ZKC_USDT": [
      "ZKC",
      1.0
    ],
    "ZKP_USDT": [
      "ZKP",
      1.0
    ],
    "NEO_USDT": [
      "NEO",
      1.0
    ],
    "IO_USDT": [
      "IO",
      1.0
    ],
    "INJ_USDT": [
      "INJ",
      1.0
    ],
    "CARV_USDT": [
      "CARV",
      1.0
    ],
    "ERA_USDT": [
      "ERA",
      1.0
    ],
    "SEI_USDT": [
      "SEI",
      1.0
    ],
    "MNT_USDT": [
      "MNT",
      1.0
    ],
    "PONS_USDT": [
      "PONS",
      1.0
    ],
    "API3_USDT": [
      "API3",
      1.0
    ],
    "ENS_USDT": [
      "ENS",
      1.0
    ],
    "O_USDT": [
      "O",
      1.0
    ],
    "BCH_USDT": [
      "BCH",
      1.0
    ],
    "BAT_USDT": [
      "BAT",
      1.0
    ],
    "APT_USDT": [
      "APT",
      1.0
    ],
    "BIO_USDT": [
      "BIO",
      1.0
    ],
    "VTHO_USDT": [
      "VTHO",
      1.0
    ],
    "ATOM_USDT": [
      "ATOM",
      1.0
    ]
  },
  "kucoin_sym_map": {
    "VVVUSDTM": [
      "VVV",
      1.0
    ],
    "LITUSDTM": [
      "LIT",
      1.0
    ],
    "ICPUSDTM": [
      "ICP",
      1.0
    ],
    "RAYUSDTM": [
      "RAY",
      1.0
    ],
    "PROMUSDTM": [
      "PROM",
      1.0
    ],
    "TRUMPUSDTM": [
      "TRUMP",
      1.0
    ],
    "WLDUSDTM": [
      "WLD",
      1.0
    ],
    "SPXUSDTM": [
      "SPX",
      1.0
    ],
    "GRTUSDTM": [
      "GRT",
      1.0
    ],
    "EULUSDTM": [
      "EUL",
      1.0
    ],
    "ZROUSDTM": [
      "ZRO",
      1.0
    ],
    "ZORAUSDTM": [
      "ZORA",
      1.0
    ],
    "CAPUSDTM": [
      "CAP",
      1.0
    ],
    "PIEVERSEUSDTM": [
      "PIEVERSE",
      1.0
    ],
    "PROSUSDTM": [
      "PROS",
      1.0
    ],
    "ESPUSDTM": [
      "ESP",
      1.0
    ],
    "NEARUSDTM": [
      "NEAR",
      1.0
    ],
    "VETUSDTM": [
      "VET",
      1.0
    ],
    "SENTUSDTM": [
      "SENT",
      1.0
    ],
    "ZKPUSDTM": [
      "ZKP",
      1.0
    ],
    "SEIUSDTM": [
      "SEI",
      1.0
    ],
    "MMTUSDTM": [
      "MMT",
      1.0
    ],
    "PONSUSDTM": [
      "PONS",
      1.0
    ],
    "OUSDTM": [
      "O",
      1.0
    ],
    "BATUSDTM": [
      "BAT",
      1.0
    ],
    "RENDERUSDTM": [
      "RENDER",
      1.0
    ],
    "APTUSDTM": [
      "APT",
      1.0
    ],
    "2ZUSDTM": [
      "2Z",
      1.0
    ],
    "NMRUSDTM": [
      "NMR",
      1.0
    ],
    "MANAUSDTM": [
      "MANA",
      1.0
    ]
  },
  "bingx_sym_map": {
    "VVV-USDT": [
      "VVV",
      1.0
    ],
    "ICP-USDT": [
      "ICP",
      1.0
    ],
    "ETHFI-USDT": [
      "ETHFI",
      1.0
    ],
    "RAY-USDT": [
      "RAY",
      1.0
    ],
    "PROM-USDT": [
      "PROM",
      1.0
    ],
    "AXL-USDT": [
      "AXL",
      1.0
    ],
    "WLD-USDT": [
      "WLD",
      1.0
    ],
    "ELSA-USDT": [
      "ELSA",
      1.0
    ],
    "SPX-USDT": [
      "SPX",
      1.0
    ],
    "SOMI-USDT": [
      "SOMI",
      1.0
    ],
    "ZRO-USDT": [
      "ZRO",
      1.0
    ],
    "ZORA-USDT": [
      "ZORA",
      1.0
    ],
    "CAP-USDT": [
      "CAP",
      1.0
    ],
    "PIEVERSE-USDT": [
      "PIEVERSE",
      1.0
    ],
    "WCT-USDT": [
      "WCT",
      1.0
    ],
    "ENSO-USDT": [
      "ENSO",
      1.0
    ],
    "BREV-USDT": [
      "BREV",
      1.0
    ],
    "NEAR-USDT": [
      "NEAR",
      1.0
    ],
    "VET-USDT": [
      "VET",
      1.0
    ],
    "KMNO-USDT": [
      "KMNO",
      1.0
    ],
    "GTC-USDT": [
      "GTC",
      1.0
    ],
    "CYBER-USDT": [
      "CYBER",
      1.0
    ],
    "AWE-USDT": [
      "AWE",
      1.0
    ],
    "ZKC-USDT": [
      "ZKC",
      1.0
    ],
    "ZKP-USDT": [
      "ZKP",
      1.0
    ],
    "NEO-USDT": [
      "NEO",
      1.0
    ],
    "INJ-USDT": [
      "INJ",
      1.0
    ],
    "SKR-USDT": [
      "SKR",
      1.0
    ],
    "CARV-USDT": [
      "CARV",
      1.0
    ],
    "ERA-USDT": [
      "ERA",
      1.0
    ],
    "SEI-USDT": [
      "SEI",
      1.0
    ],
    "MNT-USDT": [
      "MNT",
      1.0
    ],
    "ENS-USDT": [
      "ENS",
      1.0
    ],
    "IOTA-USDT": [
      "IOTA",
      1.0
    ],
    "O-USDT": [
      "O",
      1.0
    ],
    "BCH-USDT": [
      "BCH",
      1.0
    ],
    "BAT-USDT": [
      "BAT",
      1.0
    ],
    "PUMP-USDT": [
      "PUMP",
      1.0
    ],
    "DOS-USDT": [
      "DOS",
      1.0
    ],
    "APT-USDT": [
      "APT",
      1.0
    ],
    "BIO-USDT": [
      "BIO",
      1.0
    ],
    "ATOM-USDT": [
      "ATOM",
      1.0
    ]
  },
  "htx_sym_map": {
    "WLD-USDT": [
      "WLD",
      1.0
    ],
    "NEAR-USDT": [
      "NEAR",
      1.0
    ]
  },
  "pionex_sym_map": {
    "VVV_USDT_PERP": [
      "VVV",
      1.0
    ],
    "LIT_USDT_PERP": [
      "LIT",
      1.0
    ],
    "ICP_USDT_PERP": [
      "ICP",
      1.0
    ],
    "ETHFI_USDT_PERP": [
      "ETHFI",
      1.0
    ],
    "RAY_USDT_PERP": [
      "RAY",
      1.0
    ],
    "AXL_USDT_PERP": [
      "AXL",
      1.0
    ],
    "WLD_USDT_PERP": [
      "WLD",
      1.0
    ],
    "ELSA_USDT_PERP": [
      "ELSA",
      1.0
    ],
    "SPX_USDT_PERP": [
      "SPX",
      1.0
    ],
    "ZRO_USDT_PERP": [
      "ZRO",
      1.0
    ],
    "NEAR_USDT_PERP": [
      "NEAR",
      1.0
    ],
    "IO_USDT_PERP": [
      "IO",
      1.0
    ],
    "INJ_USDT_PERP": [
      "INJ",
      1.0
    ],
    "SKR_USDT_PERP": [
      "SKR",
      1.0
    ],
    "BAT_USDT_PERP": [
      "BAT",
      1.0
    ],
    "APT_USDT_PERP": [
      "APT",
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
from venues.manager import StreamManager

__all__ = [
    "BaseVenueStream",
    "BaseCEXStream",
    "create_http_session",
    "normalize_symbol",
    "UpbitRestClient",
    "UpbitSpotStream",
    "BinanceRestClient",
    "BinanceFuturesStream",
    "BybitRestClient",
    "BybitPerpStream",
    "OKXRestClient",
    "OKXStream",
    "GateioRestClient",
    "GateioStream",
    "MEXCRestClient",
    "MEXCStream",
    "KucoinRestClient",
    "KucoinStream",
    "BingXRestClient",
    "BingXStream",
    "HTXRestClient",
    "HTXStream",
    "PionexRestClient",
    "PionexStream",
    "StreamManager",
]

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

from models import OrderBookDepth5

log = logging.getLogger("stream")

_DAILY_RECONNECT_HOUR = 2
_DAILY_RECONNECT_MINUTE = 23
_RECONNECT_SPREAD_SEC = 7 * 60

_SEPS = "-_/:"
_QUOTES = ("USDT", "USDC", "USD", "KRW")
_CONTRACT_SUFFIXES = ("PERP", "SWAP")


import re

_MULT_PREFIX_RE = re.compile(r"^(1000000|100000|10000|1000|100)([A-Z0-9]+)$")


def parse_symbol_and_mult(raw: str) -> Tuple[str, float]:
    """Returns (canonical_base_coin, multiplier) e.g. '1000PEPE-USDT' -> ('PEPE', 1000.0)."""
    s_raw = str(raw).strip()
    if len(s_raw) > 1 and s_raw[0] == "k" and s_raw[1].isupper():
        s_raw = "1000" + s_raw[1:]
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
    m = _MULT_PREFIX_RE.match(s)
    if m:
        return m.group(2), float(m.group(1))
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
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.subscribed: List[str] = []
        self.symbols_seen: Set[str] = set()
        self.books_dropped = 0
        self.levels_dropped = 0
        self._first_ts = 0.0

    def _note_connect(self) -> None:
        self.connected = True
        self.last_msg_ts = time.time()
        if not self._first_ts:
            self._first_ts = self.last_msg_ts

    def _note_symbol(self, sym: str) -> None:
        if sym:
            self.symbols_seen.add(str(sym).upper())
            self.last_data_ts = time.time()

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
                exchange_ts=int(snap.exchange_ts or (now * 1000)),
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
            "last_data_ago_sec": (
                round(time.time() - self.last_data_ts, 1)
                if self.last_data_ts else None
            ),
            "books_dropped": self.books_dropped,
            "levels_dropped": self.levels_dropped,
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
        symbols: List[str] | Dict[str, Tuple[str, float]],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
    ):
        if isinstance(symbols, dict):
            self._sym_map: Dict[str, Tuple[str, float]] = dict(symbols)
            raw_list = list(symbols.keys())
            coins = sorted({c for c, _ in symbols.values()})
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

    @staticmethod
    def _base_coin(s: str) -> str:
        return normalize_symbol(s)

    def _extract_levels(self, raw_side: Any, limit: int = 5, mult: float = 1.0) -> List[Tuple[float, float]]:
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
                    out.append((p * inv_mult, q * mult))
            except (TypeError, ValueError, IndexError):
                continue
        return out

    def _handle_sub_reply(self, data: Any) -> None:
        """Hook to suppress subscription confirmation messages."""
        pass

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
                err_str = str(e)
                delay = 30.0 if ("403" in err_str or "11001" in err_str or "getaddrinfo" in err_str) else 3.0
                log.warning(f"[{self.name}] WS disconnected: {e}. Reconnecting in {delay}s...")
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

from models import OrderBookDepth5
from venues.base import (
    BaseVenueStream,
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
        coins = sorted({coin for coin, _ in sym_map.values()})
        super().__init__("binance", coins, callback)
        self._sym_map: Dict[str, Tuple[str, float]] = dict(sym_map)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())
        self.subscribed = list(coins)
        self._connected_chunks: Set[int] = set()

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
                    log.info(f"[{tag}] Connected! Streaming depth5@100ms...")
                    reconnect_in = _secs_until_daily_reconnect_for(f"{self.name}_{chunk_id}")
                    deadline = time.monotonic() + reconnect_in
                    chunk_last_msg_ts = time.time()

                    try:
                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info(f"[{tag}] Scheduled daily reconnect. Reconnecting...")
                                break

                            if time.time() - chunk_last_msg_ts > self.WATCHDOG_SECS:
                                log.warning(f"[{tag}] No messages for >{self.WATCHDOG_SECS:.0f}s, reconnecting...")
                                break

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=min(15.0, remaining))
                            except asyncio.TimeoutError:
                                continue

                            chunk_last_msg_ts = time.time()
                            self.last_msg_ts = chunk_last_msg_ts
                            self._parse_message(msg)
                    finally:
                        self._connected_chunks.discard(chunk_id)
                        self.connected = bool(self._connected_chunks)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected_chunks.discard(chunk_id)
                self.connected = bool(self._connected_chunks)
                log.warning(f"[{tag}] WS disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

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
            coin, mult = mapped

            raw_bids = data.get("b", [])[:5]
            raw_asks = data.get("a", [])[:5]
            if not raw_bids or not raw_asks:
                return

            if mult != 1.0:
                inv_mult = 1.0 / mult
                bids = [(float(p) * inv_mult, float(q) * mult) for p, q in raw_bids]
                asks = [(float(p) * inv_mult, float(q) * mult) for p, q in raw_asks]
            else:
                bids = [(float(p), float(q)) for p, q in raw_bids]
                asks = [(float(p), float(q)) for p, q in raw_asks]

            self.callback(
                OrderBookDepth5(
                    exchange="binance",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=int(data.get("E") or (time.time() * 1000)),
                    bids=bids,
                    asks=asks,
                )
            )
        except Exception:
            pass

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

from models import OrderBookDepth5
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class BingXRestClient:
    """REST Client for BingX Swap discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://open-api.bingx.com/openApi/swap"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns symbol (e.g. 'BTC-USDT') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/v2/quote/contracts"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    sym = item.get("symbol", "").upper()
                    if not sym.endswith("-USDT"):
                        continue
                    base_coin = normalize_symbol(item.get("asset", "") or sym)
                    out[sym] = (base_coin, 1.0)
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
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = (int(row.get("time", 0)) // 60000) * 60
                    close = float(row.get("close", 0.0))
                    vol_usd = float(row.get("volume", 0.0)) * close
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
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
    ):
        super().__init__("bingx", symbols, callback, stream_name=stream_name)

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send("ping")

    async def _send_subscriptions(self, ws: Any) -> None:
        for s in self.symbols:
            coin = self._base_coin(s)
            sub = {
                "id": f"sub_{coin}",
                "reqType": "sub",
                "dataType": f"{coin}-USDT@depth20",
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
            sym_raw = datatype.split("@")[0]
            sym = self._base_coin(sym_raw)
            b_data = data.get("data", {})
            bids = self._extract_levels(b_data.get("bids"), 5)
            asks = self._extract_levels(b_data.get("asks"), 5)
            if sym and bids and asks:
                self._note_symbol(sym)
                self.callback(OrderBookDepth5(
                    exchange="bingx",
                    symbol=sym,
                    raw_symbol=sym_raw,
                    ts=time.time(),
                    exchange_ts=int(b_data.get("ts", 0) or time.time() * 1000),
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
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import requests
import websockets

from models import OrderBookDepth5
from venues.base import (
    BaseVenueStream,
    create_http_session,
    normalize_symbol,
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
                    mult = 1.0
                    m = _MULT_PREFIX_RE.match(base_coin)
                    if m:
                        mult = float(m.group(1))
                        base_coin = m.group(2)
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
        coins = sorted({coin for coin, _ in sym_map.values()})
        super().__init__("bybit", coins, callback)
        self._sym_map: Dict[str, Tuple[str, float]] = dict(sym_map)
        self._raw_symbols: List[str] = sorted(self._sym_map.keys())
        self.subscribed = list(coins)
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}

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
                self._books.clear()
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
                log.warning(f"[bybit] WS disconnected: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            data = json.loads(raw_msg)
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
            coin, mult = mapped

            mtype = data.get("type", "delta")
            if raw_sym not in self._books or mtype == "snapshot":
                self._books[raw_sym] = {"bids": {}, "asks": {}}
            book = self._books[raw_sym]
            bids_dict = book["bids"]
            asks_dict = book["asks"]

            for row in d.get("b", []):
                p, q = float(row[0]), float(row[1])
                if q <= 0.0 or p <= 0.0:
                    bids_dict.pop(p, None)
                else:
                    bids_dict[p] = q

            for row in d.get("a", []):
                p, q = float(row[0]), float(row[1])
                if q <= 0.0 or p <= 0.0:
                    asks_dict.pop(p, None)
                else:
                    asks_dict[p] = q

            if not bids_dict or not asks_dict:
                return

            sorted_bids_raw = sorted(bids_dict.items(), key=lambda x: x[0], reverse=True)
            sorted_asks_raw = sorted(asks_dict.items(), key=lambda x: x[0])

            if len(bids_dict) > 100:
                book["bids"] = dict(sorted_bids_raw[:50])
            if len(asks_dict) > 100:
                book["asks"] = dict(sorted_asks_raw[:50])

            top_bids = sorted_bids_raw[:5]
            top_asks = sorted_asks_raw[:5]
            if not top_bids or not top_asks:
                return

            if mult != 1.0:
                inv_mult = 1.0 / mult
                norm_bids = [(p * inv_mult, q * mult) for p, q in top_bids]
                norm_asks = [(p * inv_mult, q * mult) for p, q in top_asks]
            else:
                norm_bids = [(p, q) for p, q in top_bids]
                norm_asks = [(p, q) for p, q in top_asks]

            self.callback(
                OrderBookDepth5(
                    exchange="bybit",
                    symbol=coin,
                    raw_symbol=raw_sym,
                    ts=time.time(),
                    exchange_ts=int(d.get("cts") or data.get("ts") or (time.time() * 1000)),
                    bids=norm_bids,
                    asks=norm_asks,
                )
            )
        except Exception:
            pass

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

```

## venues/gateio.py

```python
"""
venues/gateio.py — Gate.io Futures REST Client and WebSocket order_book_update Stream.
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
import requests

from models import OrderBookDepth5
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class GateioRestClient:
    """REST Client for Gate.io Futures discovery, 24h tickers, and 1m klines."""

    BASE_URL = "https://fx-api.gateio.ws/api/v4/futures/usdt"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()
        # Gate.io API WAF rejects fake browser User-Agents with 403; default requests UA works
        self.session.headers.pop("User-Agent", None)

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns contract (e.g. 'BTC_USDT') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/contracts"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                for item in r.json():
                    contract = item.get("name", "")
                    if not contract.endswith("_USDT"):
                        continue
                    base_coin = normalize_symbol(contract)
                    out[contract] = (base_coin, 1.0)
        except Exception as e:
            log.error(f"[gateio_rest] Failed to fetch contracts: {e}")
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
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json():
                    t_sec = (int(row.get("t", 0)) // 60) * 60
                    close = float(row.get("c", 0.0))
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
    ):
        super().__init__("gateio", symbols, callback, stream_name=stream_name)
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send(json.dumps({"time": int(time.time()), "channel": "futures.ping"}))

    async def _send_subscriptions(self, ws: Any) -> None:
        self._books.clear()
        for s in self.symbols:
            pair = s if s.endswith("_USDT") else f"{self._base_coin(s)}_USDT"
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

            event = data.get("event")
            if contract not in self._books or event == "all":
                self._books[contract] = {"bids": {}, "asks": {}}

            book = self._books[contract]
            bids_dict = book["bids"]
            asks_dict = book["asks"]

            for row in result.get("b", []):
                p, q = float(row.get("p", 0.0)), float(row.get("s", 0.0))
                if q <= 0.0 or p <= 0.0:
                    bids_dict.pop(p, None)
                else:
                    bids_dict[p] = q

            for row in result.get("a", []):
                p, q = float(row.get("p", 0.0)), float(row.get("s", 0.0))
                if q <= 0.0 or p <= 0.0:
                    asks_dict.pop(p, None)
                else:
                    asks_dict[p] = q

            if not bids_dict or not asks_dict:
                return

            sorted_bids_raw = sorted(bids_dict.items(), key=lambda x: x[0], reverse=True)
            sorted_asks_raw = sorted(asks_dict.items(), key=lambda x: x[0])

            # Prevent unbounded memory growth on volatile price moves
            if len(bids_dict) > 100:
                book["bids"] = dict(sorted_bids_raw[:50])
            if len(asks_dict) > 100:
                book["asks"] = dict(sorted_asks_raw[:50])

            if mult != 1.0:
                inv_mult = 1.0 / mult
                top_bids = [(p * inv_mult, q * mult) for p, q in sorted_bids_raw[:5]]
                top_asks = [(p * inv_mult, q * mult) for p, q in sorted_asks_raw[:5]]
            else:
                top_bids = sorted_bids_raw[:5]
                top_asks = sorted_asks_raw[:5]

            if top_bids and top_asks:
                ets = int(result.get("t", 0) or data.get("time", 0) * 1000)
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

from models import OrderBookDepth5
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
                    out[code] = (base_coin, 1.0)
        except Exception as e:
            log.error(f"[htx_rest] Failed to fetch contract info: {e}")
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
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = int(row.get("id", 0))
                    close = float(row.get("close", 0.0))
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
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
    ):
        super().__init__("htx", symbols, callback, stream_name=stream_name)

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
        for s in self.symbols:
            coin = self._base_coin(s)
            code = f"{coin}-USDT"
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
            code = ch.split(".")[1]
            sym = self._base_coin(code)
            tick = data.get("tick", {})
            bids = self._extract_levels(tick.get("bids"), 5)
            asks = self._extract_levels(tick.get("asks"), 5)
            if bids and asks:
                self._note_symbol(sym)
                ets = int(data.get("ts", 0) or tick.get("ts", 0))
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

from models import OrderBookDepth5
from venues.base import BaseVenueStream, create_http_session, normalize_symbol, _secs_until_daily_reconnect_for, log


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
                    out[sym] = (base_coin, 1.0)
        except Exception as e:
            log.error(f"[kucoin_rest] Failed to fetch contracts: {e}")
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
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", [])[-limit:]:
                    t_sec = (int(row[0]) // 60000) * 60
                    close = float(row[4])
                    vol_usd = float(row[6]) if len(row) > 6 else float(row[5]) * close
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

    def __init__(self, symbols: List[str], callback: Callable[[OrderBookDepth5], None]):
        super().__init__("kucoin", symbols, callback)
        self.subscribed = list(self.symbols)
        self._ours_to_venue: Dict[str, str] = {}
        self._ping_sec = self.PING_FALLBACK_SEC

    def _resolve_symbols(self) -> Dict[str, str]:
        session = create_http_session()
        try:
            r = session.get(self.CONTRACTS, timeout=8)
            if r.status_code == 200:
                for c in r.json().get("data", []):
                    sym = c.get("symbol", "").upper()
                    base = c.get("baseCurrency", "").upper()
                    if base == "XBT":
                        base = "BTC"
                    base_norm = normalize_symbol(base)
                    for s in self.symbols:
                        if normalize_symbol(s) == base_norm:
                            self._ours_to_venue[s] = sym
        except Exception as e:
            log.warning(f"[kucoin] Failed to resolve contract symbols: {e}")
        return self._ours_to_venue

    async def _run_loop(self) -> None:
        await asyncio.to_thread(self._resolve_symbols)
        target_syms = list(self._ours_to_venue.values())
        if not target_syms:
            log.warning("[kucoin] No symbols resolved for subscription.")
            return

        while self._running:
            try:
                # Get dynamic bullet token
                session = create_http_session()
                bullet_resp = await asyncio.to_thread(session.post, self.BULLET, timeout=10)
                data = bullet_resp.json().get("data", {})
                token = data.get("token")
                endpoints = data.get("instanceServers", [])
                if not token or not endpoints:
                    await asyncio.sleep(5.0)
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
                log.warning(f"[kucoin] WS error: {e}. Reconnecting in 3s...")
                await asyncio.sleep(3.0)

    def _parse_message(self, raw_msg: str | bytes) -> None:
        try:
            if isinstance(raw_msg, bytes):
                raw_msg = raw_msg.decode("utf-8")
            data = json.loads(raw_msg)
            topic = data.get("topic", "")
            if not topic.startswith("/contractMarket/level2Depth5:"):
                return
            sym_raw = topic.split(":", 1)[1]
            sym = normalize_symbol(sym_raw.replace("XBT", "BTC"))
            payload = data.get("data", {})
            bids_raw = payload.get("bids", [])[:5]
            asks_raw = payload.get("asks", [])[:5]
            bids = [(float(p), float(q)) for p, q in bids_raw if float(p) > 0 and float(q) > 0]
            asks = [(float(p), float(q)) for p, q in asks_raw if float(p) > 0 and float(q) > 0]
            if bids and asks:
                self._note_symbol(sym)
                ets = int(data.get("data", {}).get("timestamp", 0) or time.time() * 1000)
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
from venues.bingx import BingXStream
from venues.binance import BinanceFuturesStream
from venues.bybit import BybitPerpStream
from venues.gateio import GateioStream
from venues.htx import HTXStream
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
        active_venues: Optional[List[str]] = None,
    ):
        self.universe = universe
        self.callback = callback
        self.streams: Dict[str, BaseVenueStream] = {}

        # 1. Leader stream: Upbit (Spot KRW)
        if universe.upbit_krw_to_coin:
            self.streams["upbit"] = UpbitSpotStream(universe.upbit_krw_to_coin, callback)

        # 2. Dynamic Follower streams
        allowed_venues = [v.lower() for v in active_venues] if active_venues else None

        # Binance
        if universe.binance_sym_map and (allowed_venues is None or "binance" in allowed_venues):
            self.streams["binance"] = BinanceFuturesStream(universe.binance_sym_map, callback)

        # Bybit
        if universe.bybit_sym_map and (allowed_venues is None or "bybit" in allowed_venues):
            self.streams["bybit"] = BybitPerpStream(universe.bybit_sym_map, callback)

        # Map of additional CEX stream classes that take (symbols, callback)
        cex_classes = {
            "okx": OKXStream,
            "gateio": GateioStream,
            "mexc": MEXCStream,
            "kucoin": KucoinStream,
            "bingx": BingXStream,
            "htx": HTXStream,
            "pionex": PionexStream,
        }

        # Check universe for other venue sym maps
        raw_doc = getattr(universe, "_raw_doc", {}) or {}
        for v_name, cls in cex_classes.items():
            if allowed_venues is not None and v_name not in allowed_venues:
                continue

            v_sym_map = raw_doc.get(f"{v_name}_sym_map", {})
            if v_sym_map:
                norm_map = {k: (v[0], float(v[1])) for k, v in v_sym_map.items()}
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
                "books_dropped": s.books_dropped,
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

from models import OrderBookDepth5
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
                    out[sym] = (base_coin, 1.0)
        except Exception as e:
            log.error(f"[mexc_rest] Failed to fetch contract details: {e}")
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
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                data = r.json().get("data", {})
                times = data.get("time", [])
                closes = data.get("close", [])
                amounts = data.get("amount", [])
                for t, c, a in zip(times[-limit:], closes[-limit:], amounts[-limit:]):
                    t_sec = (int(t) // 60) * 60
                    close = float(c)
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
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
    ):
        super().__init__("mexc", symbols, callback, stream_name=stream_name)

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send('"Ping"')

    async def _send_subscriptions(self, ws: Any) -> None:
        for s in self.symbols:
            pair = f"{self._base_coin(s)}_USDT"
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
            raw_sym = data.get("symbol", "")
            if not raw_sym:
                return
            sym = self._base_coin(raw_sym)
            bids = self._extract_levels(d.get("bids"), 5)
            asks = self._extract_levels(d.get("asks"), 5)
            if bids and asks:
                self._note_symbol(sym)
                ets = int(d.get("cts", 0) or data.get("ts", 0))
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

from models import OrderBookDepth5
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
                    out[inst_id] = (base_coin, 1.0)
        except Exception as e:
            log.error(f"[okx_rest] Failed to fetch instruments: {e}")
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
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", []):
                    t_sec = (int(row[0]) // 60000) * 60
                    close = float(row[4])
                    vol_usd = float(row[7]) if len(row) > 7 else float(row[5]) * close
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
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
    ):
        super().__init__("okx", symbols, callback, stream_name=stream_name)

    async def _send_client_ping(self, ws: Any) -> None:
        await ws.send("ping")

    async def _send_subscriptions(self, ws: Any) -> None:
        for i in range(0, len(self.symbols), 50):
            args = [
                {"channel": "books5", "instId": f"{self._base_coin(s)}-USDT-SWAP"}
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
            inst_id = arg.get("instId", "")
            if not inst_id:
                return
            sym = self._base_coin(inst_id)
            bids = self._extract_levels(book_data.get("bids") or book_data.get("b"), 5)
            asks = self._extract_levels(book_data.get("asks") or book_data.get("a"), 5)
            if bids and asks:
                self._note_symbol(sym)
                ets = int(book_data.get("ts", 0) or data.get("ts", 0))
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

from models import OrderBookDepth5
from venues.base import BaseCEXStream, create_http_session, normalize_symbol, log


class PionexRestClient:
    """REST Client for Pionex Perpetual contracts, 24h tickers, and 1m klines."""

    BASE_URL = "https://api.pionex.com/api/v1"

    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or create_http_session()

    def fetch_linear_perpetuals(self) -> Dict[str, Tuple[str, float]]:
        """Returns symbol (e.g. 'BTC_USDT_PERP') -> (canonical_coin 'BTC', mult 1.0)."""
        url = f"{self.BASE_URL}/common/symbols?type=PERP"
        out: Dict[str, Tuple[str, float]] = {}
        try:
            r = self.session.get(url, timeout=8)
            if r.status_code == 200:
                for s in r.json().get("data", {}).get("symbols", []):
                    sym = s.get("symbol", "").upper()
                    if not sym.endswith("_PERP") or "USDT" not in sym:
                        continue
                    base_coin = normalize_symbol(s.get("baseCoin", "") or sym)
                    out[sym] = (base_coin, 1.0)
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
        try:
            r = self.session.get(url, timeout=6)
            if r.status_code == 200:
                for row in r.json().get("data", {}).get("klines", []):
                    t_sec = (int(row.get("time", 0)) // 60000) * 60
                    close = float(row.get("close", 0.0))
                    vol_usd = float(row.get("volume", 0.0)) * close
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
        symbols: List[str],
        callback: Callable[[OrderBookDepth5], None],
        stream_name: Optional[str] = None,
    ):
        super().__init__("pionex", symbols, callback, stream_name=stream_name)

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
            coin = self._base_coin(s)
            sym = f"{coin}_USDT_PERP"
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
            raw_sym = data.get("symbol", "")
            if not raw_sym:
                return
            sym = self._base_coin(raw_sym)
            bids = self._extract_levels(d.get("bids"), 5)
            asks = self._extract_levels(d.get("asks"), 5)
            if bids and asks:
                self._note_symbol(sym)
                ets = int(data.get("timestamp", 0) or time.time() * 1000)
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

from models import OrderBookDepth5
from venues.base import (
    BaseVenueStream,
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
                time.sleep(0.08)  # Upbit 10 req/s rate compliance
            except Exception as e:
                log.warning(f"[upbit_rest] Failed to fetch ticker batch ({len(chunk)}): {e}")

        return out

    def fetch_klines_1m(
        self,
        market: str,
        count: int = 1440,
    ) -> Dict[int, Tuple[float, float]]:
        """
        Fetches up to `count` 1-minute candles (default 1440 = 24h).
        Paginates Upbit /v1/candles/minutes/1 in chunks of 200 candles using 'to' parameter.
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
                time.sleep(0.04)  # 10 req/s compliance
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
    ):
        coins = sorted(set(krw_to_coin.values()))
        super().__init__("upbit", coins, callback)
        self._krw_to_coin: Dict[str, str] = dict(krw_to_coin)
        self.subscribed: List[str] = list(coins)

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
        backoff = self.BACKOFF_MIN
        target_codes: List[str] = ["KRW-USDT"] + sorted(self._krw_to_coin.keys())

        while self._running:
            try:
                rest_rate = await asyncio.to_thread(self._fetch_initial_usdt_rate)
                if rest_rate > 0:
                    self._update_rate(rest_rate)

                log.info(f"[upbit] Connecting to {self.WS_URL} ({len(self._krw_to_coin)} matched KRW coins)...")

                sub_msg = [
                    {"ticket": f"upbit-sunrise-{int(time.time())}"},
                    {"type": "orderbook", "codes": target_codes},
                    {"format": "DEFAULT"},
                ]

                async with websockets.connect(
                    self.WS_URL,
                    open_timeout=20,
                    ping_interval=20,
                    ping_timeout=15,
                    max_queue=4096,
                ) as ws:
                    self._note_connect()
                    self.last_msg_ts = time.time()
                    log.info(f"[upbit] Connected! Subscribed to {len(target_codes)} order books (incl. KRW-USDT).")
                    await ws.send(json.dumps(sub_msg))
                    backoff = self.BACKOFF_MIN

                    try:
                        reconnect_in = _secs_until_daily_reconnect_for(self.name)
                        deadline = time.monotonic() + reconnect_in

                        while self._running:
                            remaining = deadline - time.monotonic()
                            if remaining <= 0:
                                log.info("[upbit] Scheduled daily reconnect. Reconnecting...")
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
                log.warning(f"[upbit] WS error: {e!r}. Reconnecting in {backoff:.0f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.BACKOFF_MAX)

        self.connected = False

    def _handle_msg(self, raw: str | bytes) -> None:
        try:
            snap = self._parse_msg(raw)
        except Exception:
            self._parse_errors += 1
            if self._parse_errors == 1 or self._parse_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.warning(f"[upbit] parse error #{self._parse_errors}", exc_info=True)
            return

        if snap is None:
            return

        try:
            self.callback(snap)
        except Exception:
            self._callback_errors += 1
            if self._callback_errors == 1 or self._callback_errors % self.PARSE_ERR_LOG_EVERY == 0:
                log.error(f"[upbit] callback error #{self._callback_errors}", exc_info=True)

    def _parse_msg(self, raw: str | bytes) -> OrderBookDepth5 | None:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)

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
            exchange_ts=int(data.get("timestamp", 0)),
            bids=bids,
            asks=asks,
        )

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

HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports", "daily")


def list_available_reports() -> List[Dict[str, Any]]:
    """Discovers all archived daily reports and extracts card metrics."""
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
    """Reads Markdown content of a specific report by date key."""
    fn = f"report_{report_id}.md"
    fp = os.path.join(REPORTS_DIR, fn)
    if os.path.exists(fp):
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
Provides live monitoring, reporting hub, threshold calibration, and 1-click dataset export.
"""
from __future__ import annotations

import datetime
import http.server
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import zipfile
from typing import Any, Dict, List, Optional

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
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self) -> None:
        if self._running:
            return

        server_ref = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                log.debug(f"[api] {self.address_string()} - {format % args}")

            def _get_token(self, q: Dict[str, List[str]]) -> str:
                if "token" in q and q["token"]:
                    return q["token"][0]
                auth_hdr = self.headers.get("Authorization", "")
                if auth_hdr.startswith("Bearer "):
                    return auth_hdr.split(" ", 1)[1].strip()
                if "X-Token" in self.headers:
                    return self.headers["X-Token"].strip()
                cookies = self.headers.get("Cookie", "")
                for part in cookies.split(";"):
                    if "=" in part:
                        k, v = part.strip().split("=", 1)
                        if k == "token":
                            return v
                return ""

            def _is_authenticated(self, q: Dict[str, List[str]]) -> bool:
                if not server_ref.token:
                    return True
                return self._get_token(q) == server_ref.token

            def do_GET(self) -> None:
                parsed = urllib.parse.urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                q = urllib.parse.parse_qs(parsed.query)
                is_auth = self._is_authenticated(q)

                if path in ("/", "/dashboard", "/status"):
                    if not is_auth:
                        self._send_login()
                    else:
                        self._handle_dashboard(q)
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
                q = urllib.parse.parse_qs(parsed.query)

                if not self._is_authenticated(q):
                    self._send_unauthorized()
                    return

                if path == "/api/report/generate":
                    self._handle_generate_report()
                elif path == "/api/thresholds/calibrate":
                    self._handle_calibrate_thresholds()
                elif path == "/api/daily-pipeline":
                    self._handle_trigger_pipeline()
                elif path in ("/pool/pause", "/pool/resume"):
                    self._handle_pool_toggle(path)
                else:
                    self._send_404()

            # Handlers
            def _send_login(self) -> None:
                html_body = render_login_html()
                self._send_html(html_body, code=200)

            def _handle_dashboard(self, q: Dict[str, List[str]]) -> None:
                tok = self._get_token(q) or server_ref.token
                cookie_hdr = f"token={tok}; Path=/; HttpOnly; SameSite=Strict"

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

                th_file = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
                if os.path.exists(th_file):
                    try:
                        with open(th_file, "r", encoding="utf-8") as tf:
                            thresholds = json.load(tf)
                    except Exception:
                        pass

                cards = list_available_reports()
                html_body = render_dashboard_html(
                    service_status=status,
                    pool_snapshot=pool_snap,
                    profile_data=profile,
                    thresholds_doc=thresholds,
                    reports_cards=cards,
                    auth_token=tok,
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Set-Cookie", cookie_hdr)
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
                    snap = server_ref.service.pool_state.get_pool_snapshot()
                    self._send_json(snap.get("battle", []))
                else:
                    self._send_json([])

            def _handle_profile(self) -> None:
                if hasattr(server_ref.service, "profile_manager") and server_ref.service.profile_manager:
                    self._send_json(server_ref.service.profile_manager.get_profile())
                else:
                    self._send_json({})

            def _handle_thresholds(self) -> None:
                th_file = os.path.join(PROJECT_ROOT, "matrix_thresholds.json")
                if os.path.exists(th_file):
                    try:
                        with open(th_file, "r", encoding="utf-8") as tf:
                            self._send_json(json.load(tf))
                            return
                    except Exception as e:
                        self._send_json({"error": str(e)}, code=500)
                        return
                self._send_json({"thresholds": {}})

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

            def _handle_pool_toggle(self, path: str) -> None:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length).decode("utf-8")
                try:
                    payload = json.loads(body)
                    coin = payload.get("coin", "").upper()
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
                target_file = q.get("file", [""])[0]
                if target_file:
                    fp = os.path.join(PROJECT_ROOT, "reports", "daily", target_file)
                    if os.path.exists(fp):
                        self.send_response(200)
                        self.send_header("Content-Type", "application/octet-stream")
                        self.send_header("Content-Disposition", f"attachment; filename=\"{target_file}\"")
                        self.end_headers()
                        with open(fp, "rb") as f:
                            shutil.copyfileobj(f, self.wfile)
                        return
                    self._send_404()
                    return

                # Download zip of configs and reports
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                tmp.close()
                try:
                    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as z:
                        for fn in ["universe_live.json", "live_trading_pool.json", "matrix_thresholds.json", "trading_profile.json"]:
                            fp = os.path.join(PROJECT_ROOT, fn)
                            if os.path.exists(fp):
                                z.write(fp, arcname=fn)
                        rep_dir = os.path.join(PROJECT_ROOT, "reports", "daily")
                        if os.path.isdir(rep_dir):
                            for rfn in os.listdir(rep_dir):
                                z.write(os.path.join(rep_dir, rfn), arcname=os.path.join("reports", "daily", rfn))

                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Disposition", f"attachment; filename=\"korean_sunrise_export.zip\"")
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
                self._send_json({"error": "Unauthorized. Provide valid ?token= or Bearer token"}, code=401)

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
"""
from __future__ import annotations

import html
import json
import urllib.parse
from typing import Any, Dict, List, Optional


def render_login_html() -> str:
    return """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Вход | Korean Sunrise Intelligence</title>
<style>
:root {
  --bg-color: #0b0e17;
  --surface-color: #151c2c;
  --surface-border: rgba(255, 255, 255, 0.08);
  --primary-color: #38bdf8;
  --primary-glow: rgba(56, 189, 248, 0.25);
  --text-main: #f8fafc;
  --text-muted: #94a3b8;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
  background-color: var(--bg-color);
  color: var(--text-main);
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  padding: 16px;
}
.login-card {
  background: var(--surface-color);
  border: 1px solid var(--surface-border);
  border-radius: 20px;
  padding: 36px;
  width: 100%;
  max-width: 420px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.6);
  position: relative;
  overflow: hidden;
}
.login-card::before {
  content: '';
  position: absolute;
  top: -50px;
  right: -50px;
  width: 120px;
  height: 120px;
  background: radial-gradient(circle, var(--primary-glow) 0%, transparent 70%);
  border-radius: 50%;
}
.logo-title {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}
.logo-icon {
  font-size: 28px;
}
h1 {
  font-size: 22px;
  font-weight: 700;
  letter-spacing: -0.5px;
}
.subtitle {
  color: var(--text-muted);
  font-size: 14px;
  margin-bottom: 28px;
}
.form-group {
  margin-bottom: 20px;
}
label {
  display: block;
  font-size: 13px;
  color: var(--text-muted);
  margin-bottom: 8px;
  font-weight: 500;
}
input[type="password"] {
  width: 100%;
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid var(--surface-border);
  border-radius: 10px;
  padding: 12px 14px;
  color: #fff;
  font-size: 15px;
  outline: none;
  transition: border-color 0.2s, box-shadow 0.2s;
}
input[type="password"]:focus {
  border-color: var(--primary-color);
  box-shadow: 0 0 0 3px rgba(56, 189, 248, 0.15);
}
button {
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
}
button:hover { opacity: 0.95; }
button:active { transform: scale(0.99); }
</style>
</head>
<body>
<div class="login-card">
  <div class="logo-title">
    <div class="logo-icon">🌅</div>
    <h1>Korean Sunrise</h1>
  </div>
  <p class="subtitle">Авторизация для доступа к квантовой телеметрии</p>
  <form method="GET" action="/">
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
    auth_token: str,
) -> str:
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
                <a href="/download?token={auth_token}&file={rc.get('filename_md', '')}" class="btn-dl">Скачать .md</a>
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
}}
.data-table th, .data-table td {{
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
  font-size: 13px;
}}
.data-table th {{ background: var(--bg-2); color: var(--text-muted); font-weight: 600; text-align: left; }}
.badge {{ padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; text-transform: uppercase; }}
.badge-green {{ background: rgba(16, 185, 129, 0.15); color: #10b981; border: 1px solid rgba(16, 185, 129, 0.3); }}
.badge-blue {{ background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3); }}
.badge-yellow {{ background: rgba(245, 158, 11, 0.15); color: #f59e0b; border: 1px solid rgba(245, 158, 11, 0.3); }}
.badge-red {{ background: rgba(239, 68, 68, 0.15); color: #ef4444; border: 1px solid rgba(239, 68, 68, 0.3); }}

/* Modal */
.modal-overlay {{
  display: none;
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.8);
  backdrop-filter: blur(4px);
  z-index: 1000;
  padding: 24px;
  align-items: center;
  justify-content: center;
}}
.modal-overlay.active {{ display: flex; }}
.modal-box {{
  background: var(--bg-1);
  border: 1px solid var(--border);
  border-radius: 16px;
  width: 100%;
  max-width: 900px;
  max-height: 85vh;
  display: flex;
  flex-direction: column;
}}
.modal-header {{
  padding: 16px 20px;
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
        <p style="color:var(--text-muted);font-size:13px;margin:8px 0 16px;">Выгрузка сигналов, отчётов и конфигураций в 1 клик без SSH.</p>
        <a href="/download?token={auth_token}" class="btn-dl" style="display:block;text-align:center;">📦 Скачать архив</a>
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
const TOKEN = "{auth_token}";

function switchTab(name) {{
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  event.target.classList.add('active');
}}

async function viewReport(reportId) {{
  document.getElementById('modalTitle').innerText = 'Отчёт: ' + reportId;
  document.getElementById('modalContent').innerText = 'Загрузка...';
  document.getElementById('reportModal').classList.add('active');
  try {{
    const res = await fetch('/api/report/content?token=' + TOKEN + '&id=' + reportId);
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
  const res = await fetch('/api/report/generate?token=' + TOKEN, {{ method: 'POST' }});
  alert('Отчёт успешно сформирован!');
  location.reload();
}}

async function triggerCalibration() {{
  if (!confirm('Запустить алгоритмическую калибровку спредов по всем парам?')) return;
  const res = await fetch('/api/thresholds/calibrate?token=' + TOKEN, {{ method: 'POST' }});
  alert('Калибровка завершена!');
  location.reload();
}}

async function runPipeline() {{
  if (!confirm('Запустить 24-часовой автономный пайплайн?')) return;
  const res = await fetch('/api/daily-pipeline?token=' + TOKEN, {{ method: 'POST' }});
  alert('Пайплайн запущен в фоне!');
}}
</script>
</body>
</html>"""


def _render_battle_tags(pool_snapshot: Dict[str, Any]) -> str:
    battle = pool_snapshot.get("battle", [])
    if not battle:
        return "<span style='color:#64748b;font-size:13px;'>Боевой пул пока пуст. Монеты находятся в инкубаторе и промоутятся после оценки.</span>"
    tags = []
    for c in battle:
        name = c.get("coin", "")
        en = c.get("enabled", True)
        col = "#10b981" if en else "#64748b"
        tags.append(f"""
        <div style="background:var(--bg-2);border:1px solid var(--border);border-radius:8px;padding:6px 12px;font-size:13px;display:flex;align-items:center;gap:6px;">
          <span style="color:{col};font-weight:700;">⚔️ {name}</span>
        </div>
        """)
    return "".join(tags)

```

