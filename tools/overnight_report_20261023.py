"""
overnight_report_20261023.py - Comprehensive analysis of Korean Sunrise overnight execution (Oct 22 20:21 UTC - Oct 23 07:55 UTC, ~11.5 hours)
Exact session files: 23 chunks (0001 to 0023).
"""
import glob
import gzip
import json
import os
import sys
from collections import Counter, defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

files_22 = [
    'data/signals/2026-10-22/sunrise_signals_20261022_205141_0001.jsonl.gz',
    'data/signals/2026-10-22/sunrise_signals_20261022_212142_0002.jsonl.gz',
    'data/signals/2026-10-22/sunrise_signals_20261022_215143_0003.jsonl.gz',
    'data/signals/2026-10-22/sunrise_signals_20261022_222143_0004.jsonl.gz',
    'data/signals/2026-10-22/sunrise_signals_20261022_225144_0005.jsonl.gz',
    'data/signals/2026-10-22/sunrise_signals_20261022_232144_0006.jsonl.gz',
    'data/signals/2026-10-22/sunrise_signals_20261022_235145_0007.jsonl.gz'
]
files_23 = sorted(glob.glob('data/signals/2026-10-23/*.jsonl.gz'))
all_files = [f for f in files_22 if os.path.exists(f)] + files_23

print(f"=== ANALYZING OVERNIGHT SESSION (23 CHUNKS, ~11.5 HOURS) ===")
print(f"Files: {len(all_files)} chunks from {os.path.basename(all_files[0])} to {os.path.basename(all_files[-1])}")

strat_counts = Counter()
strat_signals = defaultdict(list)

for f in all_files:
    try:
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip():
                    continue
                s = json.loads(line)
                st = s.get('strategy', 'lead_lag')
                strat_counts[st] += 1
                strat_signals[st].append(s)
    except Exception as e:
        print(f"Error reading {f}: {e}")

# ==========================================
# 1. EXECUTIVE SCORECARD
# ==========================================
print("\n" + "=" * 94)
print("              EXECUTIVE SCORECARD: OVERNIGHT 11.5h EXECUTION (ALL STRATEGIES)")
print("=" * 94)
hdr = f"{'Стратегия':<22} | {'Сигналов':>8} | {'Fills':>6} | {'FillRate':>8} | {'WinRate':>7} | {'Net Bps':>10} | {'Ср Net/Tr':>10} | {'Net $ ($10)':>11}"
print(hdr)
print("-" * 94)

tot_trades = 0
tot_wins = 0
tot_net_bps = 0.0

for st in ['lead_lag', 'fade_fakeout', 'maker_void', 'rubber_band', 'cross_korean_triangle']:
    sigs = strat_signals.get(st, [])
    n_sigs = len(sigs)
    if n_sigs == 0:
        continue
    
    if st in ['maker_void', 'rubber_band']:
        filled = [s for s in sigs if s.get('maker_filled') is True]
    else:
        filled = sigs
    
    n_filled = len(filled)
    fill_rate = (n_filled / n_sigs * 100.0) if n_sigs else 0.0
    
    wins = [s for s in filled if float(s.get('net_pnl_bps', 0) or 0) > 0]
    n_wins = len(wins)
    win_rate = (n_wins / n_filled * 100.0) if n_filled else 0.0
    
    net_bps = sum(float(s.get('net_pnl_bps', 0) or 0) for s in filled)
    avg_net = (net_bps / n_filled) if n_filled else 0.0
    usd10 = (net_bps / 10000.0) * 10.0
    
    tot_trades += n_filled
    tot_wins += n_wins
    tot_net_bps += net_bps

    st_title = {
        'lead_lag': 'S1: Lead-Lag Taker',
        'fade_fakeout': 'S2: Fade Fakeout',
        'maker_void': 'S3: Maker Voids',
        'rubber_band': 'S4: Rubber Band (Pred)',
        'cross_korean_triangle': 'S5: Cross-Korean Tri'
    }.get(st, st)
    
    print(f"{st_title:<22} | {n_sigs:>8} | {n_filled:>6} | {fill_rate:>7.1f}% | {win_rate:>6.1f}% | {net_bps:>+10.1f} | {avg_net:>+10.2f} | {usd10:>+10.2f}$")

print("-" * 94)
tot_wr = (tot_wins / tot_trades * 100.0) if tot_trades else 0.0
tot_usd10 = (tot_net_bps / 10000.0) * 10.0
print(f"{'ИТОГО ПО ВСЕМ СДЕЛКАМ':<22} | {'-':>8} | {tot_trades:>6} | {'-':>8} | {tot_wr:>6.1f}% | {tot_net_bps:>+10.1f} | {tot_net_bps/tot_trades:>+10.2f} | {tot_usd10:>+10.2f}$")

# ==========================================
# 2. STRATEGY 4: RUBBER BAND IN-DEPTH AUDIT
# ==========================================
print("\n" + "=" * 94)
print("             STRATEGY 4: PREDATORY RUBBER BAND POST-FIX VERIFICATION")
print("=" * 94)
s4_all = strat_signals.get('rubber_band', [])
s4_filled = [s for s in s4_all if s.get('maker_filled') is True]

print(f"Total S4 Orders Posted: {len(s4_all)}")
print(f"Total S4 Fills:         {len(s4_filled)} ({len(s4_filled)/len(s4_all)*100:.2f}%)")

if s4_filled:
    s4_wins = [s for s in s4_filled if float(s.get('net_pnl_bps', 0) or 0) > 0]
    s4_net = sum(float(s.get('net_pnl_bps', 0) or 0) for s in s4_filled)
    print(f"S4 Win Rate:            {len(s4_wins)}/{len(s4_filled)} ({len(s4_wins)/len(s4_filled)*100:.1f}%)")
    print(f"S4 Total Net PnL:       {s4_net:+.1f} bps")
    print(f"S4 Avg Net per Fill:    {s4_net/len(s4_filled):+.2f} bps")

    # Exit reasons
    reasons = Counter(s.get('close_reason', 'unknown') for s in s4_filled)
    print("\n--- S4 Exit Reasons (Fills only) ---")
    for r, cnt in reasons.most_common():
        sub = [s for s in s4_filled if s.get('close_reason') == r]
        sub_net = sum(float(s.get('net_pnl_bps', 0) or 0) for s in sub)
        sub_avg = sub_net / cnt if cnt else 0
        print(f"  {r:<24}: {cnt:>4} ({cnt/len(s4_filled)*100:>5.1f}%) | Net: {sub_net:>+8.1f} bps | Avg: {sub_avg:>+6.2f} bps")

    # Top coins
    coin_counts = Counter(s.get('coin', 'unknown') for s in s4_filled)
    coin_pnl = Counter()
    for s in s4_filled:
        c = s.get('coin', 'unknown')
        coin_pnl[c] += float(s.get('net_pnl_bps', 0) or 0)

    print("\n--- S4 Top Coins Traded (Fills) ---")
    for c, cnt in coin_counts.most_common(12):
        print(f"  {c:<10}: {cnt:>3} fills | Net PnL: {coin_pnl[c]:>+8.1f} bps | Avg: {coin_pnl[c]/cnt:>+6.2f} bps")

    print("\n--- S4 Specific Audit: Prior Bleeders (ALT, IRYS, ZIL) ---")
    for b_coin in ['ALT', 'IRYS', 'ZIL']:
        b_cnt = coin_counts.get(b_coin, 0)
        b_posted = sum(1 for s in s4_all if s.get('coin') == b_coin)
        b_pnl = coin_pnl.get(b_coin, 0.0)
        print(f"  {b_coin:<6}: Posted={b_posted:>4} | Filled={b_cnt:>3} | Net PnL={b_pnl:>+8.1f} bps | Avg={b_pnl/b_cnt if b_cnt else 0:>+6.2f} bps")

    print("\n--- S4 Profitable Coins (Net > 0) ---")
    green_coins = [(c, coin_pnl[c], coin_counts[c]) for c in coin_pnl if coin_pnl[c] > 0]
    green_coins.sort(key=lambda x: x[1], reverse=True)
    for c, pnl, cnt in green_coins:
        print(f"  {c:<10}: {cnt:>3} fills | Net PnL: {pnl:>+8.1f} bps | Avg: {pnl/cnt:>+6.2f} bps")

    # Venue breakdown
    venue_counts = Counter(s.get('follower_exchange', 'unknown') for s in s4_filled)
    venue_pnl = Counter()
    for s in s4_filled:
        ex = s.get('follower_exchange', 'unknown')
        venue_pnl[ex] += float(s.get('net_pnl_bps', 0) or 0)

    print("\n--- S4 Venue Performance (Fills) ---")
    for ex, cnt in venue_counts.most_common():
        ex_wins = sum(1 for s in s4_filled if s.get('follower_exchange') == ex and float(s.get('net_pnl_bps', 0) or 0) > 0)
        wr = ex_wins / cnt * 100 if cnt else 0
        print(f"  {ex:<14}: {cnt:>3} fills | WR: {wr:>5.1f}% | Net: {venue_pnl[ex]:>+8.1f} bps | Avg: {venue_pnl[ex]/cnt:>+6.2f} bps")

# ==========================================
# 3. STRATEGY 1: LEAD-LAG IN-DEPTH & FILTERS
# ==========================================
print("\n" + "=" * 94)
print("             STRATEGY 1: LEAD-LAG DUMP FILTER & 'KOREAN LONG' PERFORMANCE")
print("=" * 94)
s1_all = strat_signals.get('lead_lag', [])
print(f"Total Raw S1 Signals: {len(s1_all)}")

korean_longs = []
dumps = []
other_longs = []

MAJORS = {'BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'ADA', 'AVAX', 'LINK', 'SUI', 'APT', 'NEAR', 'WLD', 'PEPE', 'SHIB'}

for s in s1_all:
    coin = s.get('coin', '').upper()
    swept = float(s.get('leader_swept_usd', 0) or 0)
    spread = float(s.get('follower_spread_bps', 0) or s.get('leader_spread_bps', 0) or 0)
    drain = float(s.get('drain_ratio', 0) or 0)
    
    is_major = coin in MAJORS
    is_sweep = swept >= 20000.0
    tight_spread = spread <= 15.0
    
    if is_sweep and tight_spread and not is_major:
        korean_longs.append(s)
    else:
        other_longs.append(s)

print(f"Filtered 'Корейский Лонг' Trades: {len(korean_longs)}")
if korean_longs:
    kl_wins = [s for s in korean_longs if float(s.get('net_pnl_bps', 0) or 0) > 0]
    kl_net = sum(float(s.get('net_pnl_bps', 0) or 0) for s in korean_longs)
    print(f"  Win Rate:     {len(kl_wins)}/{len(korean_longs)} ({len(kl_wins)/len(korean_longs)*100:.1f}%)")
    print(f"  Total Net:    {kl_net:+.1f} bps")
    print(f"  Avg per trade:{kl_net/len(korean_longs):+.2f} bps")
    print("  Trades list:")
    for s in korean_longs:
        print(f"    {s.get('coin'):<8} @ {s.get('follower_exchange'):<10} | Swept: ${float(s.get('leader_swept_usd',0)):>7.0f} | Net: {float(s.get('net_pnl_bps',0)):>+6.1f} bps | Close: {s.get('close_reason')}")

print(f"\nRemaining S1 Signals (Unfiltered): {len(other_longs)}")
if other_longs:
    ol_wins = [s for s in other_longs if float(s.get('net_pnl_bps', 0) or 0) > 0]
    ol_net = sum(float(s.get('net_pnl_bps', 0) or 0) for s in other_longs)
    print(f"  Win Rate:     {len(ol_wins)}/{len(other_longs)} ({len(ol_wins)/len(other_longs)*100:.1f}%)")
    print(f"  Total Net:    {ol_net:+.1f} bps")
    print(f"  Avg per trade:{ol_net/len(other_longs):+.2f} bps")

# ==========================================
# 4. STRATEGY 5: CROSS-KOREAN TRIANGLE AUDIT
# ==========================================
print("\n" + "=" * 94)
print("             STRATEGY 5: CROSS-KOREAN TRIANGLE (BITHUMB -> UPBIT -> FOLLOWER)")
print("=" * 94)
s5_all = strat_signals.get('cross_korean_triangle', [])
print(f"Total S5 Signals: {len(s5_all)}")

if s5_all:
    s5_wins = [s for s in s5_all if float(s.get('net_pnl_bps', 0) or 0) > 0]
    s5_net = sum(float(s.get('net_pnl_bps', 0) or 0) for s in s5_all)
    print(f"S5 Win Rate:      {len(s5_wins)}/{len(s5_all)} ({len(s5_wins)/len(s5_all)*100:.1f}%)")
    print(f"S5 Total Net PnL: {s5_net:+.1f} bps")
    print(f"S5 Avg Net/Tr:    {s5_net/len(s5_all):+.2f} bps")
    
    s5_coins = Counter(s.get('coin') for s in s5_all)
    s5_cpnl = Counter()
    for s in s5_all:
        s5_cpnl[s.get('coin')] += float(s.get('net_pnl_bps', 0) or 0)
    print("\n--- S5 Coins ---")
    for c, cnt in s5_coins.most_common():
        print(f"  {c:<8}: {cnt:>2} trades | Net: {s5_cpnl[c]:>+7.1f} bps | Avg: {s5_cpnl[c]/cnt:>+6.1f} bps")

    s5_exs = Counter(s.get('follower_exchange') for s in s5_all)
    s5_epnl = Counter()
    for s in s5_all:
        s5_epnl[s.get('follower_exchange')] += float(s.get('net_pnl_bps', 0) or 0)
    print("\n--- S5 Venues ---")
    for ex, cnt in s5_exs.most_common():
        print(f"  {ex:<12}: {cnt:>2} trades | Net: {s5_epnl[ex]:>+7.1f} bps | Avg: {s5_epnl[ex]/cnt:>+6.2f} bps")

    s5_reasons = Counter(s.get('close_reason') for s in s5_all)
    print("\n--- S5 Close Reasons ---")
    for r, cnt in s5_reasons.most_common():
        print(f"  {r:<20}: {cnt:>2} ({cnt/len(s5_all)*100:.1f}%)")

print("=" * 94)
