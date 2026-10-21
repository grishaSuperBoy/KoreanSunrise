import glob
import gzip
import json
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from collections import defaultdict

files = sorted(glob.glob('data/signals/2026-10-20/*.jsonl.gz'))
print(f"Total files for 2026-10-20: {len(files)}")

s1_venues = defaultdict(lambda: {'count': 0, 'wins': 0, 'gross': 0.0, 'net': 0.0})
s1_coins = defaultdict(lambda: {'count': 0, 'wins': 0, 'gross': 0.0, 'net': 0.0})
s1_pairs = defaultdict(lambda: {'count': 0, 'wins': 0, 'gross': 0.0, 'net': 0.0})
s1_total = 0

for f in files:
    with gzip.open(f, 'rt', encoding='utf-8') as gz:
        for line in gz:
            s = json.loads(line)
            if s.get('strategy') == 'lead_lag':
                s1_total += 1
                ex = s.get('follower_exchange')
                coin = s.get('coin')
                pair = f"{coin}:{ex}"
                net = s.get('net_pnl_bps', 0.0)
                gross = s.get('gross_exec_pnl_bps', net + 10.0)

                v = s1_venues[ex]
                v['count'] += 1
                v['net'] += net
                v['gross'] += gross
                if net > 0:
                    v['wins'] += 1

                c = s1_coins[coin]
                c['count'] += 1
                c['net'] += net
                c['gross'] += gross
                if net > 0:
                    c['wins'] += 1

                p = s1_pairs[pair]
                p['count'] += 1
                p['net'] += net
                p['gross'] += gross
                if net > 0:
                    p['wins'] += 1

print(f"Total S1 Lead-Lag trades on 2026-10-20: {s1_total}")

print("\n=== 2026-10-20: S1 LEAD-LAG ПО БИРЖАМ ===")
for ex, v in sorted(s1_venues.items(), key=lambda x: x[1]['gross'], reverse=True):
    wr = v['wins'] / v['count'] * 100
    avg_net = v['net'] / v['count']
    avg_gross = v['gross'] / v['count']
    print(f"Биржа: {ex:<12} | Сделок: {v['count']:>3} | WinRate: {wr:>5.1f}% | Gross PnL: {v['gross']:>+7.1f} bps (Ср: {avg_gross:>+5.1f}b) | Net PnL: {v['net']:>+7.1f} bps (Ср: {avg_net:>+5.1f}b)")

print("\n=== 2026-10-20: ТОП ПЛЮСОВЫХ СВЯЗОК В S1 LEAD-LAG ===")
pos_pairs = sorted([p for p in s1_pairs.items() if p[1]['net'] > 0], key=lambda x: x[1]['net'], reverse=True)
for pair, p in pos_pairs[:15]:
    wr = p['wins'] / p['count'] * 100
    avg_net = p['net'] / p['count']
    avg_gross = p['gross'] / p['count']
    print(f"Связка: {pair:<18} | Сделок: {p['count']:>2} | WinRate: {wr:>5.1f}% | Net PnL: {p['net']:>+6.1f} bps (Ср: {avg_net:>+5.1f}b) | Gross: {p['gross']:>+6.1f} bps")

print("\n=== 2026-10-20: ТОП МИНУСОВЫХ СВЯЗОК В S1 LEAD-LAG ===")
neg_pairs = sorted([p for p in s1_pairs.items() if p[1]['net'] < 0], key=lambda x: x[1]['net'])
for pair, p in neg_pairs[:15]:
    wr = p['wins'] / p['count'] * 100
    avg_net = p['net'] / p['count']
    avg_gross = p['gross'] / p['count']
    print(f"Связка: {pair:<18} | Сделок: {p['count']:>2} | WinRate: {wr:>5.1f}% | Net PnL: {p['net']:>+6.1f} bps (Ср: {avg_net:>+5.1f}b) | Gross: {p['gross']:>+6.1f} bps")
