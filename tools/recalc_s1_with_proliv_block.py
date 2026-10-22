import sys
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
import glob, gzip, json
import numpy as np

def run():
    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
    signals = []
    for f in files:
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    # Strategy 1 Lead-Lag signals
                    if data.get('strategy') == 'lead_lag' or ('gross_exec_edge_bps' in data and 'strategy' not in data):
                        signals.append(data)
                except Exception: pass

    print(f"Загружено сырых сигналов Стратегии 1 по всей базе: {len(signals)}")
    if not signals:
        return

    def is_proliv(s):
        regime = s.get('market_regime', '')
        breadth = float(s.get('market_breadth_pct', 50.0) or 50.0)
        btc_ret = float(s.get('btc_ret_300s_bps', 0.0) or 0.0)
        # Условие пользователя: FLASH_DUMP, BEAR_DRIFT или Breadth < 50%
        return regime in ('FLASH_DUMP', 'BEAR_DRIFT') or breadth < 50.0 or btc_ret <= -20.0

    def stats(subset):
        if not subset:
            return 0, 0.0, 0.0, 0.0, 0.0, 0.0
        n = len(subset)
        wins = sum(1 for s in subset if float(s.get('net_pnl_bps', 0.0) or 0.0) > 0)
        tp = sum(1 for s in subset if s.get('close_reason') == 'converged')
        pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in subset]
        avg_pnl = float(np.mean(pnls))
        total_pnl = float(np.sum(pnls))
        wr = wins / n * 100.0
        tp_wr = tp / n * 100.0
        usd = (total_pnl / 1e4) * 100.0
        return n, wr, tp_wr, avg_pnl, total_pnl, usd

    def row(name, subset):
        n, wr, tp_wr, avg_pnl, total_pnl, usd = stats(subset)
        if n == 0:
            print(f"{name:<60} | N=0    | Нет сделок")
            return
        print(f"{name:<60} | N={n:>5} | WinRate: {wr:5.1f}% | TP: {tp_wr:4.1f}% | Avg: {avg_pnl:+6.2f}b | Total: {total_pnl:+9.1f}b | USD($100): {usd:+8.2f}$")

    print("\n" + "="*125)
    print("                    ПЕРЕСЧЕТ СТРАТЕГИИ 1 ПО СЫРЫМ ДАННЫМ (ВСЕ 3 ДНЯ НАБЛЮДЕНИЙ)")
    print("="*125)

    row("0. БАЗА: Все сигналы S1 как есть (без фильтра пролива)", signals)

    # Разделяем на тех, кто попал в пролив и тех, кто остался
    proliv_sigs = [s for s in signals if is_proliv(s)]
    non_proliv_sigs = [s for s in signals if not is_proliv(s)]

    print("-" * 125)
    row("  [ОТСЕКАЕМ] Что глушится в пролив (FLASH_DUMP / BEAR_DRIFT / <50%)", proliv_sigs)
    row("1. [РЕЗУЛЬТАТ] S1 с глушением пролива (только живой рынок >=50%)", non_proliv_sigs)
    print("-" * 125)

    # Смотрим состав оставшихся (лонги vs шорты)
    non_proliv_shorts = [s for s in non_proliv_sigs if s.get('side') == 'short']
    non_proliv_longs = [s for s in non_proliv_sigs if s.get('side', 'long') == 'long']
    row("   - из них Шорты на растущем/нейтральном рынке", non_proliv_shorts)
    row("2. - из них ТОЛЬКО ЛОНГИ (шорты в S1 заблокированы)", non_proliv_longs)
    print("-" * 125)

    # Убираем AsterDex
    no_aster = [s for s in non_proliv_longs if s.get('follower_exchange', '').lower() != 'asterdex']
    row("3. + Убираем AsterDex (нереалистичные дырявые пулы)", no_aster)

    # Проверяем разные пороги объема кита (Swept)
    swept_3k = [s for s in no_aster if float(s.get('leader_swept_usd', 0) or 0) >= 3000.0]
    swept_5k = [s for s in no_aster if float(s.get('leader_swept_usd', 0) or 0) >= 5000.0]
    swept_10k = [s for s in no_aster if float(s.get('leader_swept_usd', 0) or 0) >= 10000.0]

    row("4a. + Порог кита: Swept >= $3,000", swept_3k)
    row("4b. + Порог кита: Swept >= $5,000", swept_5k)
    row("4c. + Порог кита: Swept >= $10,000", swept_10k)
    print("-" * 125)

    # Добавляем фильтр спреда фолловера
    tight_spread_5k = [s for s in swept_5k if float(s.get('follower_spread_bps', 999) or 999) <= 15.0]
    tight_spread_10k = [s for s in swept_10k if float(s.get('follower_spread_bps', 999) or 999) <= 15.0]
    row("5a. + Спред фолловера <= 15 bps (при Swept >= $5k)", tight_spread_5k)
    row("5b. + Спред фолловера <= 15 bps (при Swept >= $10k)", tight_spread_10k)
    print("-" * 125)

    # Черный список токсичных монет
    toxic = {'CAP', 'ERA', 'GTC', 'GMT', 'FLOCK', 'DRV', 'ATH', 'SPEC', 'MASK', 'B3', 'CP'}
    clean_5k = [s for s in tight_spread_5k if s.get('coin') not in toxic]
    clean_10k = [s for s in tight_spread_10k if s.get('coin') not in toxic]
    row("6a. + Без токсичных монет (CAP/ERA/GTC/DRV/ATH) (Swept >= $5k)", clean_5k)
    row("6b. + Без токсичных монет (CAP/ERA/GTC/DRV/ATH) (Swept >= $10k)", clean_10k)

    print("\n" + "="*125)
    print("                     РАЗБИВКА ТОЛЬКО ЗА ПОСЛЕДНИЕ 24 ЧАСА (2026-10-22)")
    print("="*125)
    # Filter for today by timestamp or date
    today_sigs = [s for s in signals if str(s.get('open_ts', ''))[:5] in ('17926', '17925')]
    today_proliv = [s for s in today_sigs if is_proliv(s)]
    today_non_proliv = [s for s in today_sigs if not is_proliv(s)]
    row("Сегодня: БАЗА (сырые сигналы)", today_sigs)
    row("Сегодня: Отсекаем пролив", today_proliv)
    row("Сегодня: Оставшиеся без пролива", today_non_proliv)
    today_longs = [s for s in today_non_proliv if s.get('side', 'long') == 'long' and s.get('follower_exchange', '').lower() != 'asterdex']
    row("Сегодня: Только лонги (без AsterDex)", today_longs)
    today_clean_5k = [s for s in today_longs if float(s.get('leader_swept_usd', 0) or 0) >= 5000.0 and float(s.get('follower_spread_bps', 999) or 999) <= 15.0 and s.get('coin') not in toxic]
    today_clean_10k = [s for s in today_longs if float(s.get('leader_swept_usd', 0) or 0) >= 10000.0 and float(s.get('follower_spread_bps', 999) or 999) <= 15.0 and s.get('coin') not in toxic]
    row("Сегодня: Итог с фильтрами (Swept >= $5k, Spread <= 15b)", today_clean_5k)
    row("Сегодня: Итог с фильтрами (Swept >= $10k, Spread <= 15b)", today_clean_10k)

if __name__ == '__main__':
    run()
