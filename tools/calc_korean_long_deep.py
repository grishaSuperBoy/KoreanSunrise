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

    print(f"Всего сигналов S1 в базе: {len(signals)}")
    longs = [s for s in signals if s.get('side', 'long') == 'long']
    print(f"Всего лонгов S1: {len(longs)}")

    def eval_subset(name, subset):
        if not subset:
            print(f"{name:<62} | N=0    | Нет сделок")
            return
        n = len(subset)
        wins = sum(1 for s in subset if float(s.get('net_pnl_bps', 0.0) or 0.0) > 0)
        tp = sum(1 for s in subset if s.get('close_reason') == 'converged')
        pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in subset]
        avg_pnl = float(np.mean(pnls))
        total_pnl = float(np.sum(pnls))
        wr = wins / n * 100.0
        tp_wr = tp / n * 100.0
        usd = (total_pnl / 1e4) * 100.0
        mfes = [float(s.get('mfe_exec_pnl_bps', 0.0) or 0.0) for s in subset]
        avg_mfe = float(np.mean(mfes)) if mfes else 0.0
        print(f"{name:<62} | N={n:>5} | WR:{wr:5.1f}% | TP:{tp_wr:4.1f}% | Avg:{avg_pnl:+6.2f}b | Total:{total_pnl:+9.1f}b | MFE:{avg_mfe:+5.1f}b | USD($100):{usd:+8.2f}$")

    print("\n" + "="*135)
    print("                    1. РАЗБИВКА ПО ПОРОГАМ ЦУНАМИ (LEADER SWEPT USD) НА ЛОНГАХ")
    print("="*135)
    eval_subset("Все лонги (без фильтров)", longs)
    
    # Swept thresholds
    for thr in [1000, 2000, 3000, 5000, 10000, 15000, 20000, 25000, 30000, 50000]:
        sub = [s for s in longs if float(s.get('leader_swept_usd', 0) or 0) >= thr]
        eval_subset(f"Цунами Swept >= ${thr:,}", sub)

    print("\n" + "="*135)
    print("          2. КОМБИНАЦИЯ: ЦУНАМИ + СПРЕД ФОЛЛОВЕРА (Spread <= 15 bps)")
    print("="*135)
    tight = [s for s in longs if float(s.get('follower_spread_bps', 999) or 999) <= 15.0]
    eval_subset("Все лонги со спредом <= 15 bps", tight)
    for thr in [3000, 5000, 10000, 15000, 20000, 25000, 30000, 50000]:
        sub = [s for s in tight if float(s.get('leader_swept_usd', 0) or 0) >= thr]
        eval_subset(f"Спред <= 15 bps & Swept >= ${thr:,}", sub)

    print("\n" + "="*135)
    print("          3. ЧТО ЕЩЁ? ДОБАВЛЯЕМ ОСТАЛЬНЫЕ ФИЛЬТРЫ К (Swept >= $20k & Spread <= 15 bps)")
    print("="*135)
    base_20k = [s for s in tight if float(s.get('leader_swept_usd', 0) or 0) >= 20000.0]
    eval_subset("База 20k: Swept >= $20k & Spread <= 15 bps", base_20k)

    # Filter A: No AsterDex
    no_aster_20k = [s for s in base_20k if s.get('follower_exchange', '').lower() != 'asterdex']
    eval_subset("  + Без AsterDex", no_aster_20k)

    # Filter B: No Proliv (Breadth >= 50% & not in FLASH_DUMP / BEAR_DRIFT)
    no_proliv_20k = [
        s for s in no_aster_20k
        if s.get('market_regime') not in ('FLASH_DUMP', 'BEAR_DRIFT')
        and float(s.get('market_breadth_pct', 50.0) or 50.0) >= 50.0
        and float(s.get('btc_ret_300s_bps', 0.0) or 0.0) > -20.0
    ]
    eval_subset("  + Без пролива (Рынок зеленый >= 50%)", no_proliv_20k)

    # Filter C: Toxic coin blacklist
    toxic = {'CAP', 'ERA', 'GTC', 'GMT', 'FLOCK', 'DRV', 'ATH', 'SPEC', 'MASK', 'B3', 'CP'}
    no_toxic_20k = [s for s in no_proliv_20k if s.get('coin') not in toxic]
    eval_subset("  + Без токсичных монет (CAP/ERA/GTC/DRV/ATH/MASK)", no_toxic_20k)

    # Filter D: Drain ratio >= 0.70 (вынос более 70% стакана Upbit)
    drain_70 = [s for s in no_toxic_20k if float(s.get('drain_ratio', 0) or 0) >= 0.70]
    eval_subset("  + Вынос стакана Upbit Drain >= 70%", drain_70)

    # Filter E: Follower Depth >= $10k (Top3 Ask)
    deep_foll = [s for s in no_toxic_20k if float(s.get('follower_top3_ask_usd', 0) or 0) >= 10000.0]
    eval_subset("  + Глубина фолловера Top3 Ask >= $10k", deep_foll)

    # Filter F: Korean Co-confirmed (Bithumb or Upbit confirmation)
    kr_conf = [s for s in no_toxic_20k if s.get('korean_confirmed') is True]
    eval_subset("  + Корейское подтверждение (korean_confirmed == True)", kr_conf)

    # Filter G: By Venue breakdown for Swept >= $20k
    print("\n" + "="*135)
    print("          4. РАЗБИВКА ПО БИРЖАМ ФОЛЛОВЕРА ДЛЯ (Swept >= $20k & Spread <= 15 bps)")
    print("="*135)
    venues = sorted(set(s.get('follower_exchange', '') for s in no_toxic_20k))
    for v in venues:
        v_sub = [s for s in no_toxic_20k if s.get('follower_exchange') == v]
        eval_subset(f"Биржа: {v.upper()}", v_sub)

    # List the actual trades in Swept >= $20k
    print("\n" + "="*135)
    print("          5. СПИСОК ВСЕХ СДЕЛОК (Swept >= $20,000 & Spread <= 15 bps & Без токсика)")
    print("="*135)
    for s in no_toxic_20k[:25]:
        coin = s.get('coin')
        f_ex = s.get('follower_exchange')
        swept = float(s.get('leader_swept_usd', 0))
        sp = float(s.get('follower_spread_bps', 0))
        edge = float(s.get('gross_exec_edge_bps', 0))
        net = float(s.get('net_pnl_bps', 0))
        mfe = float(s.get('mfe_exec_pnl_bps', 0))
        reason = s.get('close_reason')
        print(f"  • {coin:<6} @ {f_ex:<8} | Swept=${swept:>7,.0f} | Sp={sp:4.1f}b | Edge={edge:5.1f}b | NetPnL={net:+6.1f}b | MFE={mfe:+5.1f}b | Reason={reason}")

if __name__ == '__main__':
    run()
