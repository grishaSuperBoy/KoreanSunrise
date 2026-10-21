# Korean Sunrise 🌅

**High-Frequency Spot Lead-Lag & Momentum Sniping System**
*Spot Leaders: Upbit & Bithumb (South Korea)* ➔ *Perpetual Followers: 11 Global Venues (Binance, Bybit, OKX, BingX, MEXC, Gate.io, KuCoin, HTX, Pionex, Hyperliquid, AsterDex)*

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

---

## 5. Шпаргалка по отчётности (3 Стратегии)

### Через консоль на сервере (CLI):
```bash
python report.py          # Посмотреть последний квантовый отчёт прямо в терминале (Markdown)
python report.py --json   # Вывести последний отчёт в JSON (для пайплайнов / jq)
python report.py --list   # Список всех суточных отчётов с Win Rate и PnL
python report.py -g       # Пересчитать и обновить отчёт прямо сейчас (на лету)
python report.py -g --days 2  # Сгенерировать отчёт за последние 2 дня
python report.py --date 2026-10-20  # Посмотреть отчёт за конкретную дату
python report.py --analyze    # Запустить консольный анализатор с матрицей монет и маркаутом
```

### Через веб эндпоинты (REST API):
* `http://127.0.0.1:8089/api/report/latest` — полный JSON последнего отчёта (все 3 стратегии, портфель, матрица монет)
* `http://127.0.0.1:8089/api/report/content?id=latest` — отчёт в Markdown с форматированными таблицами
* `http://127.0.0.1:8089/api/report/json?date=YYYY-MM-DD` — JSON отчёта за конкретную дату
* `http://127.0.0.1:8089/api/report` — список всех доступных архивных отчётов с карточками
* `http://127.0.0.1:8089/` — интерактивный веб-дашборд со вкладкой «Суточная отчётность»

---

## 6. ⚡ КРИТИЧЕСКИ ВАЖНО: Развёртывание на боевом VPS (Токио / Сеул) и NTP-Синхронизация

> [!CAUTION]
> **ОБЯЗАТЕЛЬНО К ПРОЧТЕНИЮ ПЕРЕД БОЕВЫМ ДЕПЛОЕМ!**  
> В квантовом арбитраже корейского лид-лага физический лаг между Сеулом (Upbit / Bithumb) и Токио (Binance / Bybit / OKX) составляет всего **28–32 мс**, а внутри Сеула — **0.5–2.0 мс**.  
> Если системные часы на VPS рассинхронизированы хотя бы на **15–50 мс**, телеметрия задержек (`leader_latency_ms`) и тайминги выкупа стакана будут давать фатальную погрешность.

### 1. Выбор локации сервера (Colocation)
* **AWS ap-northeast-1 (Tokyo):** Рекомендуемая локация. Пинг до основных фьючерсных бирж (Binance, Bybit, OKX): `0.8 - 2.5 мс`, пинг до Upbit/Bithumb: `28 - 32 мс`.
* **AWS ap-northeast-2 (Seoul):** Альтернативная локация для сверхбыстрого инжеста Upbit/Bithumb (`0.5 - 2.0 мс`). Пинг до Токио: `28 - 33 мс`.

### 2. Обязательная настройка Chrony (Sub-Millisecond Clock Sync)
Стандартный NTP-демон `systemd-timesyncd` на Ubuntu/Debian недостаточен для HFT и может давать дрейф в десятки миллисекунд. На боевом Linux VPS необходимо установить и активировать **`chrony`**:

```bash
# 1. Установка chrony
sudo apt update && sudo apt install -y chrony

# 2. Добавление быстрых азиатских серверов в /etc/chrony/chrony.conf:
# Рекомендуется добавить в начало секции серверов:
# server time.google.com iburst minpoll 4 maxpoll 4
# server time.cloudflare.com iburst minpoll 4 maxpoll 4
# server kr.pool.ntp.org iburst minpoll 4 maxpoll 4
# server jp.pool.ntp.org iburst minpoll 4 maxpoll 4

# 3. Запуск и автозагрузка службы
sudo systemctl enable --now chrony
sudo systemctl restart chrony

# 4. Проверка точности синхронизации (Clock Skew)
chronyc tracking
chronyc sources -v
```
**Критерий готовности:** `System time` в выводе `chronyc tracking` должен быть в пределах **±0.000500 секунд (менее 0.5 мс)**.

### 3. Специфика часовых поясов Bithumb (KST vs UTC)
* **Стаканы Bithumb (`orderbookdepth`):** Bithumb передаёт поле `datetime` в виде POSIX-эпохи в микросекундах (`"1792542995509097"`).
* **Сделки Bithumb (`transaction`):** Bithumb передаёт поле `'contDtm'` в виде строки **без таймзоны в корейском времени KST (UTC+9)** (например, `"2026-10-21 09:37:26.883114"`).
* Функция `normalize_exchange_ts` в `models.py` принудительно парсит наивные строки Bithumb как `UTC+9` и преобразует их в строгие миллисекунды UTC POSIX. Поэтому при точной настройке `chrony` на сервере расчёт сетевой задержки (`leader_latency_ms`) работает идеально независимо от того, в какой таймзоне запущен VPS.

### 4. Встроенный сетевой бенчмарк
Проверить качество соединения и рассинхронизацию часов (`CLK SKEW`) со всеми 13 площадками можно одной командой:
```bash
python tools/benchmark_connections.py
```
Команда выполнит TCP-синхронизацию, замерит DNS, RTT, пинг WebSocket и выведет статус по шкале ⚡ ULTRA / FAST / FAIR.

---

## 7. 🟡 Важные нюансы и ловушки микроструктуры при анализе отчётов

При чтении ежедневных отчётов (`python tools/full_daily_report.py`) и логов обратите внимание на две критические ловушки:

### 1. "Токсичность" Bithumb-Exclusive монет ($N=99$)
* **Проблема:** Монеты, которые торгуются только на Bithumb (отсутствуют на Upbit), обладают меньшей ликвидностью и тонкими стаканами. Снос на \$3,000 на мелком альте может составлять 10–20% всей книги заявок, провоцируя ложные выносы и последующий мгновенный откат (mean-reversion / adverse selection).
* **Как отслеживать:** В ежедневном отчёте смотрим секцию **«2.1 Кросс-Корейская аналитика»**, когорту `Single-Listed (Bithumb Solo)`.
* **Как калибровать:** Если винрейт этой когорты окажется ниже 50%, увеличьте порог входа в `.env`:
  ```bash
  BITHUMB_MIN_SWEPT_USD=5000.0   # или 10000.0 для отсечения розничного шума
  ```

### 2. Пылевые сделки и ложные подтверждения (`MIN_KOREAN_CONFIRM_USD`)
* **Проблема:** В режиме сбора телеметрии подтверждением пампа считается любая покупка на второй бирже (`recent_other_buys > 0.0`), чтобы не потерять ни одной сделки из-за микро-лага. Но в боевом режиме (`Battle Mode`) микро-покупка на \$5–10 на Bithumb не должна подтверждать институциональный снос Upbit на \$50,000 (риск "пылевого спуфинга").
* **Как калибровать:** Для перехода к боевой торговле на реальные деньги задайте минимальный долларовый порог подтверждающего тейкера в `.env`:
  ```bash
  MIN_KOREAN_CONFIRM_USD=500.0   # подтверждение только реальными покупками от $500
  ```



