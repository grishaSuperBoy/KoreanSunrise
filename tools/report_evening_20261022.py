import sys
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
import glob, gzip, json
from collections import defaultdict, Counter
import numpy as np

def run():
    # Load all signals from today's session (2026-10-22)
    files = sorted(glob.glob('data/signals/2026-10-22/*.jsonl.gz'))
    print(f"=== ОБРАБОТКА ДАННЫХ ЗА 2026-10-22: НАЙДЕНО {len(files)} АРХИВОВ СИГНАЛОВ ===")
    
    # Files after daemon launch with S4 and S5 (after 14:00)
    session_files = [f for f in files if any(k in f for k in [
        '_143449_', '_150449_', '_153449_', '_160449_', '_163450_',
        '_170451_', '_173452_', '_180452_', '_183453_', '_190454_', '_193455_'
    ])]
    print(f"Из них в активной сессии S4/S5 (17:34 - 23:00): {len(session_files)} файлов")

    all_signals = []
    session_signals = []
    for f in files:
        is_sess = f in session_files
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    s = json.loads(line)
                    all_signals.append(s)
                    if is_sess:
                        session_signals.append(s)
                except Exception: pass

    print(f"Всего сигналов за сегодня: {len(all_signals)}")
    print(f"Всего сигналов в сессии S4/S5: {len(session_signals)}")

    # Strategy breakdown for the active session
    strat_groups = defaultdict(list)
    for s in session_signals:
        strat = s.get('strategy')
        if not strat:
            if 'gross_exec_edge_bps' in s:
                strat = 'lead_lag'
            elif s.get('type') == 'rubber_band':
                strat = 'rubber_band'
            elif s.get('type') == 'triangle':
                strat = 'cross_korean_triangle'
            else:
                strat = 'other'
        strat_groups[strat].append(s)

    print("\n" + "="*115)
    print("                     СВОДНЫЙ ОТЧЕТ ПО ВСЕМ СТРАТЕГИЯМ В АКТИВНОЙ СЕССИИ")
    print("="*115)
    header = f"{'Стратегия':<28} | {'Всего':>6} | {'Исполнено/Закрыто':>17} | {'WinRate':>7} | {'Ср. PnL (bps)':>13} | {'Total PnL (bps)':>15} | {'USD($100)':>10}"
    print(header)
    print("-" * 115)

    def print_strat_row(name, sigs, only_filled=False):
        if not sigs:
            print(f"{name:<28} | {0:>6} | {0:>17} |    -    |             - |               - |          -")
            return
        
        # In maker strategies (S3, S4), many orders expire unfilled.
        # Let's count filled / closed with PnL vs total posted
        if only_filled:
            closed = [s for s in sigs if s.get('filled') is True or s.get('close_reason') in ('target_converged', 'converged', 'stop_loss', 'stop_decay', 'stop_invalidation', 'stop_adverse_pnl')]
        else:
            closed = [s for s in sigs if s.get('close_reason') != 'unfilled_timeout']
            if not closed:
                closed = sigs

        total_posted = len(sigs)
        n_closed = len(closed)
        if n_closed == 0:
            print(f"{name:<28} | {total_posted:>6} | {0:>17} |    -    |         0.00b |            0.0b |      0.00$")
            return

        wins = sum(1 for s in closed if float(s.get('net_pnl_bps', 0.0) or 0.0) > 0)
        pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in closed]
        avg_pnl = float(np.mean(pnls))
        total_pnl = float(np.sum(pnls))
        wr = (wins / n_closed * 100.0) if n_closed else 0.0
        usd = (total_pnl / 1e4) * 100.0
        print(f"{name:<28} | {total_posted:>6} | {n_closed:>17} | {wr:>6.1f}% | {avg_pnl:>+12.2f}b | {total_pnl:>+14.1f}b | {usd:>+9.2f}$")

    print_strat_row("S4: Rubber Band (Predatory)", strat_groups['rubber_band'], only_filled=True)
    print_strat_row("S5: Cross-Korean (Triangle)", strat_groups['cross_korean_triangle'])
    print_strat_row("S3: Maker Voids (Harvesting)", strat_groups['maker_void'], only_filled=True)
    print_strat_row("S2: Fade Fakeout (Sniper)", strat_groups['fade_fakeout'])
    print_strat_row("S1: Lead-Lag (Все сырые)", strat_groups['lead_lag'])

    # Deep dive into S4 (Rubber Band)
    s4_all = [s for s in all_signals if s.get('strategy') == 'rubber_band' or s.get('type') == 'rubber_band']
    print("\n" + "="*115)
    print(f"          ДЕТАЛИЗАЦИЯ СТРАТЕГИИ 4 (RUBBER BAND): ВСЕГО СИГНАЛОВ={len(s4_all)}")
    print("="*115)
    s4_filled = [s for s in s4_all if s.get('filled') is True or s.get('close_reason') in ('target_converged', 'converged', 'stop_loss', 'stop_decay')]
    s4_tp = [s for s in s4_all if s.get('close_reason') in ('target_converged', 'converged')]
    s4_sl = [s for s in s4_all if s.get('close_reason') == 'stop_loss']
    s4_decay = [s for s in s4_all if s.get('close_reason') == 'stop_decay']
    s4_unfilled = [s for s in s4_all if s.get('close_reason') == 'unfilled_timeout']

    print(f"  • Выставлено лимиток на дне (Maker Bids): {len(s4_all)}")
    print(f"  • Снято по таймауту (цена не достала со скидкой): {len(s4_unfilled)} ({len(s4_unfilled)/max(len(s4_all),1)*100:.1f}%)")
    print(f"  • НАЛИЛО (Filled Maker): {len(s4_filled)}")
    if s4_filled:
        pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in s4_filled]
        print(f"    - Take-Profit (🎯 Цель отскока): {len(s4_tp)} ({len(s4_tp)/len(s4_filled)*100:.1f}%)")
        print(f"    - Stop-Loss (Шип пошел глубже): {len(s4_sl)} ({len(s4_sl)/len(s4_filled)*100:.1f}%)")
        print(f"    - Stop-Decay (Зависло в нуле): {len(s4_decay)} ({len(s4_decay)/len(s4_filled)*100:.1f}%)")
        print(f"    - Средний PnL на исполненную сделку: {np.mean(pnls):+.2f} bps")
        print(f"    - Совокупный PnL исполненных сделок: {np.sum(pnls):+.1f} bps")
        
        # Best coins on S4
        coin_pnl = defaultdict(float)
        for s in s4_filled:
            coin_pnl[s.get('coin')] += float(s.get('net_pnl_bps', 0.0) or 0.0)
        print("\n  Топ прибыльных монет на отскоке S4:")
        for c, p in sorted(coin_pnl.items(), key=lambda x: x[1], reverse=True)[:7]:
            print(f"    • {c:<8}: {p:+.1f} bps")

    # Deep dive into S5 (Cross-Korean Triangle)
    s5_all = [s for s in all_signals if s.get('strategy') == 'cross_korean_triangle' or s.get('type') == 'triangle']
    print("\n" + "="*115)
    print(f"          ДЕТАЛИЗАЦИЯ СТРАТЕГИИ 5 (CROSS-KOREAN TRIANGLE): ВСЕГО СИГНАЛОВ={len(s5_all)}")
    print("="*115)
    s5_tp = [s for s in s5_all if s.get('close_reason') in ('target_converged', 'converged')]
    s5_sl = [s for s in s5_all if s.get('close_reason') in ('stop_loss', 'stop_adverse_pnl')]
    s5_decay = [s for s in s5_all if s.get('close_reason') in ('stop_decay', 'timeout')]
    print(f"  • Сигналов треугольника Bithumb-Upbit: {len(s5_all)}")
    print(f"  • Сработало тейк-профитов (🎯): {len(s5_tp)}")
    if s5_all:
        s5_pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in s5_all]
        s5_wins = sum(1 for p in s5_pnls if p > 0)
        print(f"  • Win Rate: {s5_wins/len(s5_all)*100:.1f}%")
        print(f"  • Средний PnL: {np.mean(s5_pnls):+.2f} bps")
        print(f"  • Совокупный PnL: {np.sum(s5_pnls):+.1f} bps")
        print("\n  Список всех сигналов S5:")
        for s in s5_all:
            c = s.get('coin')
            f_ex = s.get('follower_exchange')
            lead = float(s.get('bithumb_lead_bps', 0) or 0)
            target = float(s.get('target_bps', 0) or 0)
            pnl = float(s.get('net_pnl_bps', 0) or 0)
            cr = s.get('close_reason')
            dur = float(s.get('duration_sec', 0) or 0)
            print(f"    • {c:<8} @ {f_ex:<8} | Lead: {lead:>+6.1f}b | Target: {target:>4.1f}b | PnL: {pnl:>+6.1f}b | Dur: {dur:>4.1f}s | {cr}")

    # Deep dive into S1 under Korean Long Filters
    print("\n" + "="*115)
    print("          РЕЗУЛЬТАТ СТРАТЕГИИ 1 С ФИЛЬТРОМ 'КОРЕЙСКИЙ ЛОНГ' ЗА СЕГОДНЯ")
    print("="*115)
    s1_all = [s for s in all_signals if s.get('strategy') == 'lead_lag' or ('gross_exec_edge_bps' in s and 'strategy' not in s)]
    toxic = {'CAP', 'ERA', 'GTC', 'GMT', 'FLOCK', 'DRV', 'ATH', 'SPEC', 'MASK', 'B3', 'CP'}
    heavy = {'BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'WLD', 'NEAR', 'LINK', 'SUI', 'APT', 'UNI', 'ETC', 'AXS', 'BEAM'}
    
    s1_filtered = [
        s for s in s1_all
        if s.get('side', 'long') == 'long'
        and float(s.get('leader_swept_usd', 0) or 0) >= 20000.0
        and float(s.get('follower_spread_bps', 999) or 999) <= 15.0
        and s.get('follower_exchange', '').lower() != 'asterdex'
        and s.get('coin') not in toxic
        and s.get('coin') not in heavy
        and s.get('market_regime') not in ('FLASH_DUMP', 'BEAR_DRIFT')
        and float(s.get('market_breadth_pct', 50.0) or 50.0) >= 50.0
    ]
    
    print(f"  • Все сырые сигналы S1 за сегодня: N={len(s1_all)} | Total PnL={sum(float(s.get('net_pnl_bps',0) or 0) for s in s1_all):+.1f} bps")
    if s1_filtered:
        n = len(s1_filtered)
        wins = sum(1 for s in s1_filtered if float(s.get('net_pnl_bps',0) or 0) > 0)
        pnls = [float(s.get('net_pnl_bps',0) or 0) for s in s1_filtered]
        print(f"  • Профильтрованный 'Корейский Лонг' ($20k+, альты, плотный спред, зеленый рынок):")
        print(f"    - Сделок: {n}")
        print(f"    - Win Rate: {wins/n*100:.1f}%")
        print(f"    - Средний PnL: {np.mean(pnls):+.2f} bps")
        print(f"    - Совокупный PnL: {np.sum(pnls):+.1f} bps")

if __name__ == '__main__':
    run()
