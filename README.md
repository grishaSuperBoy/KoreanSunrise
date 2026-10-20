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

