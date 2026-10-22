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
                    if data.get('strategy') == 'lead_lag' or ('gross_exec_edge_bps' in data and 'strategy' not in data):
                        signals.append(data)
                except Exception: pass

    # Filter down only to DUMP regimes: FLASH_DUMP and BEAR_DRIFT
    dump_sigs = [
        s for s in signals
        if s.get('market_regime') in ('FLASH_DUMP', 'BEAR_DRIFT')
        or float(s.get('market_breadth_pct', 50.0) or 50.0) < 40.0
        or float(s.get('btc_ret_300s_bps', 0.0) or 0.0) <= -20.0
    ]

    print(f"Total signals during DUMP/PROLIV regimes: {len(dump_sigs)}")

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
        usd_100 = (total_pnl / 1e4) * 100.0
        print(f"{name:<55} | N={n:>4} | WinRate: {wr:5.1f}% | TP: {tp_wr:4.1f}% | Avg: {avg_pnl:+6.2f}b | Total: {total_pnl:+8.1f}b | USD($100): {usd_100:+8.2f}$")

    print("\n" + "="*115)
    print("                     РАБОТА В ПРОЛИВ (DUMP / CASCADE): ЧТО РЕАЛЬНО РАБОТАЕТ?")
    print("="*115)

    evaluate("0. Все S1 сигналы в пролив (сырые, шорты + лонги)", dump_sigs)

    dump_shorts = [s for s in dump_sigs if s.get('side') == 'short']
    dump_longs = [s for s in dump_sigs if s.get('side', 'long') == 'long']
    evaluate("1. Шорты в пролив (сырые)", dump_shorts)
    evaluate("2. Лонги в пролив (сырые)", dump_longs)

    # Let's test Tsunami / Heavy filters on SHORTS in proliv:
    short_tsunami = [
        s for s in dump_shorts
        if float(s.get('leader_swept_usd', 0) or 0) >= 5000.0
        and float(s.get('follower_spread_bps', 999) or 999) <= 15.0
    ]
    evaluate("3. Шорты с фильтром (Swept >= $5k, Spread <= 15 bps)", short_tsunami)

    short_whale = [
        s for s in dump_shorts
        if float(s.get('leader_swept_usd', 0) or 0) >= 10000.0
        and float(s.get('follower_spread_bps', 999) or 999) <= 15.0
    ]
    evaluate("4. Кит-шорты в пролив (Swept >= $10k, Spread <= 15 bps)", short_whale)

    # Now let's test LONGS in proliv: IDIOSYNCRATIC PUMP during dump!
    # A coin that Korean whales aggressively buy EVEN WHEN MARKET DUMPS!
    long_whale = [
        s for s in dump_longs
        if float(s.get('leader_swept_usd', 0) or 0) >= 10000.0
        and float(s.get('follower_spread_bps', 999) or 999) <= 15.0
    ]
    evaluate("5. Кит-лонги в пролив (Идиосинкратический памп Swept >= $10k)", long_whale)

    long_tsunami_20k = [
        s for s in dump_longs
        if float(s.get('leader_swept_usd', 0) or 0) >= 20000.0
        and float(s.get('follower_spread_bps', 999) or 999) <= 15.0
    ]
    evaluate("6. Супер-кит лонги в пролив (Swept >= $20,000)", long_tsunami_20k)

    # Inspect any winning signals in dump
    win_dumps = [s for s in dump_sigs if float(s.get('net_pnl_bps', 0.0) or 0.0) > 0.0]
    print(f"\nВсего прибыльных сделок в пролив: {len(win_dumps)} из {len(dump_sigs)}")
    for s in win_dumps[:10]:
        side = (s.get('side') or 'long').upper()
        print(f"  • {s.get('coin')} ({side}) @ {s.get('follower_exchange')}: Leader={s.get('leader_exchange')} | Swept=${float(s.get('leader_swept_usd', 0)):.0f} | Edge={s.get('gross_exec_edge_bps')}b | NetPnL={s.get('net_pnl_bps'):+.1f}b | Reason={s.get('close_reason')}")

    print("\n" + "="*115)
    print("           ПОЧЕМУ ШОРТЫ И ЛОНГИ В ПРОЛИВ НА S1 ТЕРЯЮТ ДЕНЬГИ? АНАТОМИЯ ДВИЖЕНИЯ")
    print("="*115)
    
    # Check follower price movement after entry
    foll_moves = [float(s.get('foll_move_bps', 0.0) or 0.0) for s in dump_shorts]
    stops = sum(1 for s in dump_shorts if s.get('close_reason') == 'stop_decay')
    hard_sl = sum(1 for s in dump_shorts if s.get('close_reason') == 'hard_sl')
    print(f"Шорты в пролив: закрыто по stop_decay (таймаут 15с без движения): {stops} ({stops/len(dump_shorts)*100:.1f}%)")
    print(f"Шорты в пролив: закрыто по hard_sl (отскок против позиции): {hard_sl} ({hard_sl/len(dump_shorts)*100:.1f}%)")
    print(f"Шорты в пролив: среднее движение Follower за 15с: {np.mean(foll_moves):+.2f} bps")

    # Now let's examine S4 (Rubber Band) signals from all files
    s4_signals = []
    for f in files:
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    if data.get('strategy') == 'rubber_band' or data.get('type') == 'rubber_band':
                        s4_signals.append(data)
                except Exception: pass
    
    print(f"\nВсего сигналов по Стратегии 4 (Rubber Band) в базе: {len(s4_signals)}")
    if s4_signals:
        s4_dumps = [
            s for s in s4_signals
            if s.get('market_regime') in ('FLASH_DUMP', 'BEAR_DRIFT')
            or float(s.get('market_breadth_pct', 50.0) or 50.0) < 40.0
            or float(s.get('btc_ret_300s_bps', 0.0) or 0.0) <= -20.0
        ]
        evaluate("Стратегия 4 (Rubber Band Maker) в пролив", s4_dumps)
        evaluate("Стратегия 4 (все режимы)", s4_signals)

if __name__ == '__main__':
    run()
