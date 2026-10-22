import glob, gzip, json, sys
from collections import defaultdict
import numpy as np

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

def run():
    # Load all lead-lag signals across 20, 21, 22
    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
    signals = []
    for f in files:
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    if data.get('strategy') == 'lead_lag' or ('gross_exec_edge_bps' in data and 'strategy' not in data):
                        signals.append(data)
                except Exception: pass

    print(f"Total Lead-Lag signals across all 3 days: {len(signals)}")
    if not signals:
        return

    def evaluate(name, subset):
        if not subset:
            print(f"{name:<55} | N=0 | Нет сделок")
            return
        n = len(subset)
        wins = sum(1 for s in subset if float(s.get('net_pnl_bps', 0.0) or 0.0) > 0)
        tp = sum(1 for s in subset if s.get('close_reason') == 'converged')
        pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in subset]
        avg_pnl = np.mean(pnls)
        total_pnl = np.sum(pnls)
        wr = wins / n * 100.0
        tp_wr = tp / n * 100.0
        # Calculate USD based on $100 order size
        usd_100 = (total_pnl / 1e4) * 100.0
        print(f"{name:<55} | N={n:>5} | WinRate: {wr:5.1f}% | TP: {tp_wr:4.1f}% | Avg: {avg_pnl:+6.2f}b | Total: {total_pnl:+8.1f}b | USD($100): {usd_100:+8.2f}$")

    print("\n" + "="*115)
    print("               ПОШАГОВАЯ ФИЛЬТРАЦИЯ СТРАТЕГИИ 1: ОТ КАЛА К РЕАЛЬНОМУ ПЛЮСУ")
    print("="*115)

    evaluate("0. БАЗА (ВСЕ сигналы S1 как есть, со всем шлаком)", signals)

    # 1. Выкидываем шорты
    longs_only = [s for s in signals if s.get('side', 'long') == 'long']
    evaluate("1. УБИРАЕМ ШОРТЫ (только Лонги)", longs_only)

    # 2. Выкидываем AsterDex
    no_aster = [s for s in longs_only if s.get('follower_exchange', '').lower() != 'asterdex']
    evaluate("2. + УБИРАЕМ ASTERDEX (псевдо-DEX с дырявым стаканом)", no_aster)

    # 3. Фильтр режима: НИКАКИХ лонгов в дамп (FLASH_DUMP, BEAR_DRIFT, breadth < 50%)
    green_market = [
        s for s in no_aster
        if s.get('market_regime') in ('STRONG_PUMP', 'BULL_DRIFT')
        or float(s.get('market_breadth_pct', 50.0) or 50.0) >= 50.0
    ]
    evaluate("3. + РЕЖИМ РЫНКА: только зеленый рынок (Breadth >= 50%)", green_market)

    # 4. Фильтр Цунами / Сила свипа на лидере (Swept >= $3,000)
    tsunami_3k = [s for s in green_market if float(s.get('leader_swept_usd', 0) or 0) >= 3000.0]
    evaluate("4. + ЦУНАМИ (Swept на лидере >= $3,000)", tsunami_3k)

    # 5. Плотный стакан фолловера (Spread <= 15 bps)
    tight_spread = [s for s in tsunami_3k if float(s.get('follower_spread_bps', 999) or 999) <= 15.0]
    evaluate("5. + ПЛОТНЫЙ СПРЕД ФОЛЛОВЕРА (Spread <= 15 bps)", tight_spread)

    # 6. Глубина стакана фолловера (Top3 Ask >= $15,000)
    deep_book = [s for s in tight_spread if float(s.get('follower_top3_ask_usd', 0) or 0) >= 15000.0]
    evaluate("6. + ГЛУБОКИЙ СТАКАН (Top3 Ask >= $15,000)", deep_book)

    # 7. Фильтр токсичных монет (убираем шлак: CAP, ERA, GTC, GMT, FLOCK, DRV)
    toxic_blacklist = {'CAP', 'ERA', 'GTC', 'GMT', 'FLOCK', 'DRV', 'ATH', 'SPEC'}
    no_toxic = [s for s in deep_book if s.get('coin') not in toxic_blacklist]
    evaluate("7. + ЧИСТКА ТОКСИЧНЫХ МОНЕТ (без CAP/ERA/GTC/DRV)", no_toxic)

    # 8. Институциональное Цунами: Swept >= $10,000 на плотном стакане
    inst_tsunami = [s for s in green_market if float(s.get('leader_swept_usd', 0) or 0) >= 10000.0 and float(s.get('follower_spread_bps', 999) or 999) <= 15.0 and s.get('coin') not in toxic_blacklist]
    evaluate("8. КИТ-ЦУНАМИ: Swept >= $10k & Spread <= 15 bps & no toxic", inst_tsunami)

    print("\n" + "="*115)
    print("                    ТО ЖЕ САМОЕ ТОЛЬКО ЗА СЕГОДНЯ (2026-10-22)")
    print("="*115)
    today_sigs = [s for s in signals if str(s.get('open_ts', ''))[:5] in ('17926', '17925')] # today's signals
    evaluate("0. База сегодня (все 397 сигналов)", today_sigs)
    today_longs = [s for s in today_sigs if s.get('side', 'long') == 'long']
    evaluate("1. Только лонги сегодня", today_longs)
    today_green = [s for s in today_longs if s.get('market_regime') in ('STRONG_PUMP', 'BULL_DRIFT') or float(s.get('market_breadth_pct', 50.0) or 50.0) >= 50.0]
    evaluate("2. + Только зеленый рынок сегодня", today_green)
    today_clean = [s for s in today_green if float(s.get('follower_spread_bps', 999) or 999) <= 15.0 and s.get('coin') not in toxic_blacklist]
    evaluate("3. + Плотный стакан & без токсика сегодня", today_clean)
    today_tsunami = [s for s in today_clean if float(s.get('leader_swept_usd', 0) or 0) >= 3000.0]
    evaluate("4. + Цунами (Swept >= $3k) сегодня", today_tsunami)

if __name__ == '__main__':
    run()
