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
├── models.py            # Типы данных (OrderBookDepth5, SunriseSignal)
├── stream_base.py       # Базовая логика WebSocket (реконнекты, watchdog, jitter)
├── stream_upbit.py      # Upbit WebSocket (живая конвертация KRW->USDT, фильтр прыжков курса 2%)
├── stream_binance.py    # Binance Futures WebSocket (depth5@100ms)
├── stream_bybit.py      # Bybit Linear Perps WebSocket (orderbook.50)
├── stream_manager.py    # Менеджер параллельных стримов
├── korean_engine.py     # Ядро детекции Lead-Lag, фильтрации и расчета эджа
├── batch_writer.py      # Атомарный компрессор сигналов на диск (gzip jsonl)
├── config.py            # Конфигурация пула монет и порогов
└── main.py              # Точка входа сервиса
```

---

## 4. Запуск

```bash
pip install -r requirements.txt
python main.py
```
