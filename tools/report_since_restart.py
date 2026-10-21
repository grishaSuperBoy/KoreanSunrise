import glob, gzip, json, sys
from collections import defaultdict, Counter

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

files = sorted(glob.glob('data/signals/2026-10-21/*.jsonl.gz'))[-4:]
print(f"=== FILES ANALYZED (SINCE RESTART ~14:04) ===")
for f in files:
    print(" ", f)

strat_stats = defaultdict(lambda: {
    'count': 0, 'wins': 0, 'gross_bps': 0.0, 'net_bps': 0.0,
    'reasons': Counter(), 'venues': Counter(), 'coins': Counter(), 'venue_pnl': Counter()
})

all_s1 = []
for f in files:
    with gzip.open(f, 'rt', encoding='utf-8') as gz:
        for line in gz:
            s = json.loads(line)
            st = s.get('strategy', 'lead_lag')
            net = float(s.get('net_pnl_bps', 0) or 0)
            ex = s.get('follower_exchange', 'unknown').lower()
            coin = s.get('coin', 'unknown').upper()
            cr = s.get('close_reason', 'unknown')
            
            d = strat_stats[st]
            d['count'] += 1
            if net > 0:
                d['wins'] += 1
            d['net_bps'] += net
            d['reasons'][cr] += 1
            d['venues'][ex] += 1
            d['venue_pnl'][ex] += net
            d['coins'][coin] += net
            if st == 'lead_lag':
                all_s1.append(s)

print('\n' + '='*88)
print('         EXECUTIVE SCORECARD ПОСЛЕ ПЕРЕЗАПУСКА (14:04 - 17:54) | 4 ЧАНКА')
print('='*88)
header = f"{'Стратегия':<18} | {'Сделок':>6} | {'WinRate':>7} | {'Net Bps':>10} | {'Ср. Net bps':>11} | {'Net USD ($10)':>13}"
print(header)
print('-' * 88)
tot_trades = sum(d['count'] for d in strat_stats.values())
tot_net_bps = sum(d['net_bps'] for d in strat_stats.values())
tot_wins = sum(d['wins'] for d in strat_stats.values())

for st in ['lead_lag', 'fade_fakeout', 'maker_void']:
    if st not in strat_stats: continue
    d = strat_stats[st]
    c = d['count']
    wr = d['wins'] / c * 100 if c else 0
    net = d['net_bps']
    avg = net / c if c else 0
    usd10 = (net / 10000.0) * 10.0
    st_name = {
        'lead_lag': '1. Lead-Lag',
        'fade_fakeout': '2. Fade Inverted',
        'maker_void': '3. Maker Voids'
    }.get(st, st)
    print(f"{st_name:<18} | {c:>6} | {wr:>6.1f}% | {net:>+10.1f} | {avg:>+11.2f} | {usd10:>+12.2f}$")

print('-' * 88)
tot_wr = tot_wins / tot_trades * 100 if tot_trades else 0
tot_usd10 = (tot_net_bps / 10000.0) * 10.0
print(f"{'ИТОГО ПОРТФЕЛЬ':<18} | {tot_trades:>6} | {tot_wr:>6.1f}% | {tot_net_bps:>+10.1f} | {tot_net_bps/tot_trades:>+11.2f} | {tot_usd10:>+12.2f}$")

print('\n' + '='*88)
print('            СТРАТЕГИЯ 1 (LEAD-LAG): ДЕТАЛИЗАЦИЯ И РАЗБИВКА ПО ВЕНЬЮ')
print('='*88)
d1 = strat_stats['lead_lag']
for ex, pnl in sorted(d1['venue_pnl'].items(), key=lambda x: x[1], reverse=True):
    cnt = d1['venues'][ex]
    wr = sum(1 for s in all_s1 if s.get('follower_exchange','').lower() == ex and float(s.get('net_pnl_bps',0) or 0) > 0) / cnt * 100 if cnt else 0
    print(f"  Биржа: {ex:<12} | Сделок: {cnt:>3} | WR: {wr:>5.1f}% | Net: {pnl:>+8.1f} bps | Ср: {pnl/cnt:>+6.1f} bps")

print('\n--- ТОП МОНЕТЫ S1 LEAD-LAG ---')
for coin, pnl in sorted(d1['coins'].items(), key=lambda x: x[1], reverse=True)[:5]:
    cnt = sum(1 for s in all_s1 if s.get('coin','').upper() == coin)
    print(f"  {coin:<8} | Сделок: {cnt:>2} | Net PnL: {pnl:>+8.1f} bps")

print('\n' + '='*88)
print('            СТРАТЕГИЯ 2 (FADE INVERTED LONG): ПРИЧИНЫ ЗАКРЫТИЯ')
print('='*88)
d2 = strat_stats['fade_fakeout']
for r, cnt in d2['reasons'].most_common():
    print(f"  {r:<28}: {cnt:>5} ({cnt/d2['count']*100:>5.1f}%)")

print('\n' + '='*88)
print('            СТРАТЕГИЯ 3 (MAKER VOIDS): ИСХОДЫ ЛИМИТНЫХ ОРДЕРОВ')
print('='*88)
d3 = strat_stats['maker_void']
for r, cnt in d3['reasons'].most_common():
    print(f"  {r:<28}: {cnt:>5} ({cnt/d3['count']*100:>5.1f}%)")

print('\n' + '='*88)
print('         СТРАТЕГИЯ 1: СРАВНЕНИЕ TSUNAMI RATIO ПОСЛЕ ПЕРЕЗАПУСКА')
print('='*88)
targets = {'binance', 'bybit', 'mexc', 'hyperliquid'}
excl = {'BTC', 'ETH', 'SOL', 'AAVE', 'UNI', 'INJ', 'PENDLE', 'JTO', 'ENS', 'GMT', 'AXL', 'DATA', 'ICP', 'ZRO'}
s1_clean = [s for s in all_s1 if s.get('follower_exchange','').lower() in targets and s.get('coin','').upper() not in excl and float(s.get('follower_spread_bps',999) or 999) <= 70.0]

for min_swept, r in [(15000, 0.0), (3000, 5.0), (3000, 10.0), (3000, 20.0)]:
    if r == 0.0:
        sub = [s for s in s1_clean if float(s.get('leader_swept_usd',0) or 0) >= min_swept]
        name = f"Фикс. выкуп >= ${min_swept:,}"
    else:
        sub = [s for s in s1_clean if float(s.get('leader_swept_usd',0) or 0) >= min_swept and (float(s.get('leader_swept_usd',0) or 0) / max(float(s.get('follower_top3_ask_usd',0) or 0), 100.0)) >= r]
        name = f"Tsunami >= {r:.1f}x (Floor ${min_swept:,})"
    c = len(sub)
    if c == 0: continue
    w = sum(1 for s in sub if float(s.get('net_pnl_bps',0) or 0) > 0)
    net = sum(float(s.get('net_pnl_bps',0) or 0) for s in sub)
    wr = w / c * 100
    avg = net / c
    print(f"  {name:<38} | N={c:>2} | WR: {wr:>5.1f}% | Net: {net:>+7.1f} bps | Ср: {avg:>+5.1f} bps")

