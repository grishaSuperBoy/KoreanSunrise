import glob
import gzip
import json
import os
import sys

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from collections import defaultdict

files = sorted(glob.glob('data/signals/2026-10-21/*.jsonl.gz'))
signals = []
for f in files:
    with gzip.open(f, 'rt', encoding='utf-8') as gz:
        for line in gz:
            try:
                signals.append(json.loads(line))
            except Exception:
                pass

print(f"Total signals loaded for 2026-10-21: {len(signals)}")

# Group by strategy
strats = defaultdict(list)
for s in signals:
    strats[s.get('strategy', 'unknown')].append(s)

for st_name, st_sigs in strats.items():
    print(f"Strategy: {st_name} -> {len(st_sigs)} records")

# 1. Strategy 1 (Lead-Lag)
s1 = strats.get('lead_lag', [])
print(f"\n==========================================")
print(f"  STRATEGY 1: LEAD-LAG ({len(s1)} signals)")
print(f"==========================================")

pair_stats = defaultdict(lambda: {'count': 0, 'wins': 0, 'pnl_sum': 0.0, 'gross_sum': 0.0, 'mfe_sum': 0.0, 'mae_sum': 0.0})
bithumb_cohort = defaultdict(lambda: {'count': 0, 'wins': 0, 'pnl_sum': 0.0, 'gross_sum': 0.0})

for s in s1:
    coin = s.get('coin', '')
    ex = s.get('follower_exchange', '')
    pair = f"{coin} -> {ex}"
    net = s.get('net_pnl_bps', 0.0)
    gross = s.get('gross_exec_pnl_bps', s.get('net_pnl_bps', 0.0) + 10.0)
    mfe = s.get('mfe_exec_pnl_bps', 0.0)
    mae = s.get('mae_exec_pnl_bps', 0.0)

    st = pair_stats[pair]
    st['count'] += 1
    if net > 0:
        st['wins'] += 1
    st['pnl_sum'] += net
    st['gross_sum'] += gross
    st['mfe_sum'] += mfe
    st['mae_sum'] += mae

    # Check Bithumb tags
    co_listed = s.get('korean_co_listed', False)
    confirmed = s.get('korean_confirmed', False)
    leader_ex = s.get('leader_exchange', 'upbit')

    if leader_ex == 'bithumb':
        cohort = "Bithumb Lead (Excl/Initiator)"
    elif confirmed:
        cohort = "Dual-Pump (Bithumb Confirmed)"
    elif co_listed:
        cohort = "Dislocated (Bithumb Unconfirmed)"
    else:
        cohort = "Single-Listed (Upbit Only)"

    bc = bithumb_cohort[cohort]
    bc['count'] += 1
    if net > 0:
        bc['wins'] += 1
    bc['pnl_sum'] += net
    bc['gross_sum'] += gross

# Sort pairs
sorted_pairs = sorted(pair_stats.items(), key=lambda x: x[1]['pnl_sum'], reverse=True)

pos_pairs = [p for p in sorted_pairs if p[1]['pnl_sum'] > 0]
neg_pairs = [p for p in sorted_pairs if p[1]['pnl_sum'] < 0]
zero_pairs = [p for p in sorted_pairs if p[1]['pnl_sum'] == 0]

print(f"\nPairs breakdown: {len(pos_pairs)} profitable, {len(neg_pairs)} unprofitable, {len(zero_pairs)} breakeven")

print("\n--- ТОП ПЛЮСОВЫХ СВЯЗОК (S1 Lead-Lag) ---")
for p, st in pos_pairs[:20]:
    c = st['count']
    wr = st['wins'] / c * 100
    mean_net = st['pnl_sum'] / c
    mean_gross = st['gross_sum'] / c
    print(f"{p:<22} | Сделок: {c:>2} | WinRate: {wr:>5.1f}% | Net PnL: {st['pnl_sum']:>+6.1f} bps | Средний Net: {mean_net:>+5.1f} bps (Gross: {mean_gross:>+5.1f} bps)")

print("\n--- ТОП МИНУСОВЫХ СВЯЗОК (S1 Lead-Lag) ---")
for p, st in sorted(neg_pairs, key=lambda x: x[1]['pnl_sum'])[:20]:
    c = st['count']
    wr = st['wins'] / c * 100
    mean_net = st['pnl_sum'] / c
    mean_gross = st['gross_sum'] / c
    print(f"{p:<22} | Сделок: {c:>2} | WinRate: {wr:>5.1f}% | Net PnL: {st['pnl_sum']:>+6.1f} bps | Средний Net: {mean_net:>+5.1f} bps (Gross: {mean_gross:>+5.1f} bps)")

print("\n--- РАСПРЕДЕЛЕНИЕ ПО КОГОРТАМ BITHUMB ---")
for cohort, bc in sorted(bithumb_cohort.items(), key=lambda x: x[1]['count'], reverse=True):
    c = bc['count']
    wr = bc['wins'] / c * 100
    net_avg = bc['pnl_sum'] / c
    gross_avg = bc['gross_sum'] / c
    print(f"{cohort:<35} | Сделок: {c:>3} | WinRate: {wr:>5.1f}% | Total Net: {bc['pnl_sum']:>+7.1f} bps | Ср. Net: {net_avg:>+5.1f} bps (Gross: {gross_avg:>+5.1f} bps)")

# 2. Strategy 3 (Maker Voids) breakdown by venue
s3 = strats.get('maker_void', [])
print(f"\n==========================================")
print(f"  STRATEGY 3: MAKER VOIDS ({len(s3)} signals)")
print(f"==========================================")
s3_venues = defaultdict(lambda: {'count': 0, 'filled': 0, 'wins': 0, 'pnl_sum': 0.0})
for s in s3:
    ex = s.get('follower_exchange', 'unknown')
    filled = s.get('maker_filled', False)
    net = s.get('net_pnl_bps', 0.0)
    sv = s3_venues[ex]
    sv['count'] += 1
    if filled:
        sv['filled'] += 1
        sv['pnl_sum'] += net
        if net > 0:
            sv['wins'] += 1

print("Разбивка Maker Voids по биржам:")
for ex, sv in sorted(s3_venues.items(), key=lambda x: x[1]['pnl_sum']):
    f_rate = (sv['filled'] / sv['count'] * 100) if sv['count'] else 0
    w_rate = (sv['wins'] / sv['filled'] * 100) if sv['filled'] else 0
    print(f"Биржа: {ex:<12} | Сигналов: {sv['count']:>5} | Исполнений: {sv['filled']:>4} ({f_rate:>4.1f}%) | WinRate налитых: {w_rate:>4.1f}% | Net PnL: {sv['pnl_sum']:>+8.1f} bps")

# 3. Strategy 2 (Fade Fakeout) breakdown
s2 = strats.get('fade_fakeout', [])
print(f"\n==========================================")
print(f"  STRATEGY 2: FADE THE FAKEOUT ({len(s2)} signals)")
print(f"==========================================")
s2_reasons = defaultdict(lambda: {'count': 0, 'wins': 0, 'pnl_sum': 0.0})
for s in s2:
    r = s.get('fade_reason', s.get('trigger_type', 'unknown'))
    net = s.get('net_pnl_bps', 0.0)
    sr = s2_reasons[r]
    sr['count'] += 1
    sr['pnl_sum'] += net
    if net > 0:
        sr['wins'] += 1

for r, sr in s2_reasons.items():
    wr = (sr['wins'] / sr['count'] * 100) if sr['count'] else 0
    print(f"Причина: {r:<25} | Сделок: {sr['count']:>5} | WinRate: {wr:>4.1f}% | Total Net: {sr['pnl_sum']:>+8.1f} bps")
