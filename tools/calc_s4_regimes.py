import sys
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
import glob, gzip, json
import numpy as np

def run():
    files = sorted(glob.glob('data/signals/2026-10-22/*.jsonl.gz'))[-11:]
    s4_trades = []
    for f in files:
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    s = json.loads(line)
                    if s.get('strategy') == 'rubber_band' and s.get('close_reason') in ('target_converged', 'stop_loss', 'stop_decay', 'timeout'):
                        s4_trades.append(s)
                except Exception: pass

    print(f"Всего закрытых сделок S4 в выборке: {len(s4_trades)}")

    # Sort by open_ts
    s4_trades.sort(key=lambda x: x.get('open_ts', 0))
    ts_min = s4_trades[0]['open_ts']
    ts_max = s4_trades[-1]['open_ts']
    print(f"Диапазон времени: {ts_min} ... {ts_max} (дельта {(ts_max - ts_min)/3600:.1f} часов)")

    # Split into 5 time buckets of 1 hour each
    n_buckets = 5
    bucket_size = (ts_max - ts_min) / n_buckets
    for b in range(n_buckets):
        b_start = ts_min + b * bucket_size
        b_end = b_start + bucket_size
        sub = [s for s in s4_trades if b_start <= s['open_ts'] < b_end]
        if not sub: continue
        pnls = [float(s['net_pnl_bps']) for s in sub]
        wins = sum(1 for p in pnls if p > 0)
        tp = sum(1 for s in sub if s['close_reason'] == 'target_converged')
        print(f"Час {b+1}: N={len(sub):>3} | WR={wins/len(sub)*100:5.1f}% | TP={tp/len(sub)*100:4.1f}% | Avg={np.mean(pnls):+6.2f}b | Total={np.sum(pnls):+8.1f}b")

    # Let's inspect the coins: which coins won, which coins lost?
    coin_stats = {}
    for s in s4_trades:
        c = s.get('coin')
        if c not in coin_stats:
            coin_stats[c] = []
        coin_stats[c].append(float(s['net_pnl_bps']))

    print("\n=== ТОП 10 САМЫХ ПРИБЫЛЬНЫХ МОНЕТ S4 ===")
    for c, pnls in sorted(coin_stats.items(), key=lambda x: sum(x[1]), reverse=True)[:10]:
        wins = sum(1 for p in pnls if p > 0)
        print(f"  {c:<8}: N={len(pnls):>2} | WR={wins/len(pnls)*100:5.1f}% | Avg={np.mean(pnls):+6.2f}b | Total={sum(pnls):+7.1f}b")

    print("\n=== ТОП 10 САМЫХ УБЫТОЧНЫХ МОНЕТ S4 ===")
    for c, pnls in sorted(coin_stats.items(), key=lambda x: sum(x[1]))[:10]:
        wins = sum(1 for p in pnls if p > 0)
        print(f"  {c:<8}: N={len(pnls):>2} | WR={wins/len(pnls)*100:5.1f}% | Avg={np.mean(pnls):+6.2f}b | Total={sum(pnls):+7.1f}b")

if __name__ == '__main__':
    run()
