# Korean Sunrise: Шпаргалка по отчётности (3 Стратегии) 📊

Быстрый доступ ко всей квантовой аналитике по трём стратегиям:
1. **Lead-Lag Momentum** (Long фолловера на импульсе Upbit)
2. **Fade the Fakeout** (Short фолловера на ложных пробоях и снятии котировок MM)
3. **Maker Liquidity Harvesting** (Post-Only лимитки на расширенных спредах)

---

## 1. Через консоль на сервере (Server CLI)

Выполняется из корня проекта:

```bash
# Посмотреть последний квантовый отчёт прямо в терминале (Markdown таблицы)
python report.py

# Вывести последний отчёт в сыром JSON (для скриптов, пайплайнов, jq)
python report.py --json

# Список всех сгенерированных суточных отчётов с Win Rate и PnL
python report.py --list

# Пересчитать и обновить отчёт прямо сейчас (на лету за последние 24 часа)
python report.py -g

# Сгенерировать отчёт за произвольное окно (например, 2 или 7 дней)
python report.py -g --days 2

# Посмотреть отчёт за конкретную историческую дату
python report.py --date 2026-10-20

# Запустить интерактивный консольный анализатор (маркаут, стаканы, матрица монет)
python report.py --analyze
```

---

## 2. Через веб-эндпоинты (REST API / Web UI)

Порт по умолчанию: `:8089` (защищён токеном авторизации или веб-сессией).

### Основные URL:
* `http://127.0.0.1:8089/api/report/latest` — полный структурированный JSON последнего отчёта (Executive Scorecard, 3 стратегии, портфель, PnL-сценарии, кросс-стратегическая матрица монет).
* `http://127.0.0.1:8089/api/report/content?id=latest` — отчёт в Markdown с отформатированными таблицами (для просмотра через curl или браузер).
* `http://127.0.0.1:8089/api/report/markdown` — синоним `/api/report/content`.
* `http://127.0.0.1:8089/api/report/json?date=YYYY-MM-DD` — JSON-отчёт за конкретную дату (`date=2026-10-20` или `id=latest`).
* `http://127.0.0.1:8089/api/report` — список всех доступных архивных отчётов с карточками и статистикой по стратегиям.
* `http://127.0.0.1:8089/` — интерактивный веб-дашборд со вкладкой **«Суточная отчётность»** (все карточки обновляются в реальном времени).

### Примеры запросов через curl:
```bash
# Получить JSON последнего отчёта:
curl -H "Authorization: Bearer <AUTH_TOKEN>" http://127.0.0.1:8089/api/report/latest

# Распечатать Markdown в терминал:
curl -H "Authorization: Bearer <AUTH_TOKEN>" http://127.0.0.1:8089/api/report/content?id=latest

# Инициировать пересчёт отчёта через POST:
curl -X POST -H "Authorization: Bearer <AUTH_TOKEN>" http://127.0.0.1:8089/api/report/generate
```

---

## 3. Где хранятся сгенерированные файлы:

* **Markdown-версии**: `reports/daily/report_YYYY-MM-DD.md`
* **JSON-версии**: `reports/daily/report_YYYY-MM-DD.json`
* **Сырые сигналы**: `data/signals/YYYY-MM-DD/sunrise_signals_*.jsonl.gz`
