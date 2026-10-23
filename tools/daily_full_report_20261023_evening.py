"""
daily_full_report_20261023_evening.py - Complete, rigorous report across the entire 18.5-hour session (Oct 22 20:21 UTC - Oct 23 14:52 UTC, Chunks 0001-0037).
Covers all 5 strategies with exact metrics, venue breakdowns, and regime shifts.
"""
import glob
import gzip
import json
import os
import sys
from collections import Counter, defaultdict
import numpy as np

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Gather all files belonging to task-23718 (chunks 0001 to 0037)
files_22 = [f"data/signals/2026-10-22/sunrise_signals_20261022_{t}.jsonl.gz" for t in [
    "205141_0001", "212142_0002", "215143_0003", "222143_0004", "225144_0005", "232144_0006", "235145_0007"
]]
files_23 = sorted(glob.glob("data/signals/2026-10-23/*.jsonl.gz"))
all_session_files = [f for f in files_22 if os.path.exists(f)] + files_23

print(f"=== FULL SESSION AUDIT: 18.5 HOURS (OCT 22 20:21 UTC -> OCT 23 14:52 UTC) ===")
print(f"Total Session Files: {len(all_session_files)} chunks from {os.path.basename(all_session_files[0])} to {os.path.basename(all_session_files[-1])}")

strat_signals = defaultdict(list)
raw_signal_count = Counter()

for f in all_session_files:
    try:
        with gzip.open(f, "rt", encoding="utf-8") as gz:
            for line in gz:
                if not line.strip():
                    continue
                s = json.loads(line)
                st = s.get("strategy")
                if not st and "gross_exec_edge_bps" in s:
                    st = "lead_lag"
                st = st or "unknown"
                raw_signal_count[st] += 1
                strat_signals[st].append(s)
    except Exception as e:
        print(f"Error reading {f}: {e}")

print("\n--- RAW SIGNAL VOLUMES (18.5 HOURS) ---")
for st, cnt in raw_signal_count.most_common():
    print(f"  {st:<24}: {cnt:>6} signals")

# ==========================================
# 1. EXECUTIVE SCORECARD
# ==========================================
print("\n" + "=" * 98)
print("             EXECUTIVE SCORECARD: 18.5h CONTINUOUS EXECUTION (ALL 5 STRATEGIES)")
print("=" * 98)
hdr = f"{'Стратегия':<24} | {'Сигналов':>8} | {'Fills':>6} | {'FillRate':>8} | {'WinRate':>7} | {'Net Bps':>10} | {'Ср Net/Tr':>10} | {'Net $ ($10k)':>12}"
print(hdr)
print("-" * 98)

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
    
    pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in filled]
    n_wins = sum(1 for p in pnls if p > 0)
    win_rate = (n_wins / n_filled * 100.0) if n_filled else 0.0
    
    net_bps = sum(pnls)
    avg_net = (net_bps / n_filled) if n_filled else 0.0
    usd10k = (net_bps / 10000.0) * 10000.0  # $10k notional position
    
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
    
    print(f"{st_title:<24} | {n_sigs:>8} | {n_filled:>6} | {fill_rate:>7.1f}% | {win_rate:>6.1f}% | {net_bps:>+10.1f} | {avg_net:>+10.2f} | {usd10k:>+11.2f}$")

print("-" * 98)
tot_wr = (tot_wins / tot_trades * 100.0) if tot_trades else 0.0
tot_usd10k = (tot_net_bps / 10000.0) * 10000.0
print(f"{'ИТОГО ПО ВСЕМ СДЕЛКАМ':<24} | {'-':>8} | {tot_trades:>6} | {'-':>8} | {tot_wr:>6.1f}% | {tot_net_bps:>+10.1f} | {tot_net_bps/tot_trades:>+10.2f} | {tot_usd10k:>+11.2f}$")

# ==========================================
# 2. STRATEGY 4: PREDATORY RUBBER BAND AUDIT
# ==========================================
print("\n" + "=" * 98)
print("             STRATEGY 4: PREDATORY RUBBER BAND MAKER (IN-DEPTH AUDIT)")
print("=" * 98)
s4_all = strat_signals.get('rubber_band', [])
s4_filled = [s for s in s4_all if s.get('maker_filled') is True]

print(f"Total S4 Orders Posted: {len(s4_all)}")
print(f"Total S4 Fills:         {len(s4_filled)} (Fill Rate: {len(s4_filled)/len(s4_all)*100:.2f}%)")

if s4_filled:
    s4_pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in s4_filled]
    s4_wins = sum(1 for p in s4_pnls if p > 0)
    s4_net = sum(s4_pnls)
    print(f"S4 Win Rate:            {s4_wins}/{len(s4_filled)} ({s4_wins/len(s4_filled)*100:.1f}%)")
    print(f"S4 Total Net PnL:       {s4_net:+.1f} bps")
    print(f"S4 Avg Net per Fill:    {s4_net/len(s4_filled):+.2f} bps")

    reasons = Counter(s.get('close_reason', 'unknown') for s in s4_filled)
    print("\n--- S4 Exit Reasons (Fills) ---")
    for r, cnt in reasons.most_common():
        sub = [s for s in s4_filled if s.get('close_reason') == r]
        sub_net = sum(float(s.get('net_pnl_bps', 0.0) or 0.0) for s in sub)
        sub_avg = sub_net / cnt if cnt else 0.0
        print(f"  {r:<26}: {cnt:>4} ({cnt/len(s4_filled)*100:>5.1f}%) | Net: {sub_net:>+8.1f} bps | Avg: {sub_avg:>+6.2f} bps")

    # Venue breakdown
    venue_stats = defaultdict(lambda: {'n': 0, 'wins': 0, 'net': 0.0, 'tp': 0, 'sl': 0, 'decay': 0})
    for s in s4_filled:
        ex = s.get('follower_exchange', 'unknown')
        pnl = float(s.get('net_pnl_bps', 0.0) or 0.0)
        cr = s.get('close_reason', '')
        venue_stats[ex]['n'] += 1
        if pnl > 0:
            venue_stats[ex]['wins'] += 1
        venue_stats[ex]['net'] += pnl
        if 'converged' in cr or 'target' in cr:
            venue_stats[ex]['tp'] += 1
        elif 'loss' in cr:
            venue_stats[ex]['sl'] += 1
        elif 'decay' in cr:
            venue_stats[ex]['decay'] += 1

    print("\n--- S4 Performance by Venue (Fills) ---")
    print(f"  {'Биржа':<12} | {'Fills':>5} | {'WinRate':>7} | {'TP':>4} | {'SL':>4} | {'Decay':>5} | {'Net Bps':>9} | {'Ср Net/Tr':>9}")
    print("  " + "-" * 72)
    for ex, st in sorted(venue_stats.items(), key=lambda x: x[1]['net'], reverse=True):
        n = st['n']
        w = st['wins']
        net = st['net']
        avg = net / n
        wr = w / n * 100
        print(f"  {ex:<12} | {n:>5} | {wr:>6.1f}% | {st['tp']:>4} | {st['sl']:>4} | {st['decay']:>5} | {net:>+9.1f} | {avg:>+9.2f}")

    # Top coins in S4
    coin_pnl = Counter()
    coin_fills = Counter()
    for s in s4_filled:
        c = s.get('coin', 'unknown')
        coin_pnl[c] += float(s.get('net_pnl_bps', 0.0) or 0.0)
        coin_fills[c] += 1

    print("\n--- S4 Top Profitable Coins ---")
    green = [(c, coin_pnl[c], coin_fills[c]) for c in coin_pnl if coin_pnl[c] > 0]
    green.sort(key=lambda x: x[1], reverse=True)
    for c, p, cnt in green[:10]:
        print(f"  {c:<10}: {cnt:>3} fills | Net: {p:>+8.1f} bps | Avg: {p/cnt:>+6.2f} bps")

    print("\n--- S4 Worst Losing Coins ---")
    red = [(c, coin_pnl[c], coin_fills[c]) for c in coin_pnl if coin_pnl[c] <= 0]
    red.sort(key=lambda x: x[1])
    for c, p, cnt in red[:10]:
        print(f"  {c:<10}: {cnt:>3} fills | Net: {p:>+8.1f} bps | Avg: {p/cnt:>+6.2f} bps")

# ==========================================
# 3. STRATEGY 1: LEAD-LAG & KOREAN LONG AUDIT
# ==========================================
print("\n" + "=" * 98)
print("             STRATEGY 1: LEAD-LAG TAKER & KOREAN LONG (IN-DEPTH AUDIT)")
print("=" * 98)
s1_all = strat_signals.get('lead_lag', [])
print(f"Total S1 Signals (18.5h): {len(s1_all)}")

s1_pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in s1_all]
s1_wins = sum(1 for p in s1_pnls if p > 0)
print(f"Raw S1 All Signals:  {len(s1_all)} trades | WR: {s1_wins/len(s1_all)*100:.1f}% | Net: {sum(s1_pnls):+.1f} bps | Avg: {np.mean(s1_pnls):+.2f} bps")

# Filters
toxic = {'CAP', 'ERA', 'GTC', 'GMT', 'FLOCK', 'DRV', 'ATH', 'SPEC', 'MASK', 'B3', 'CP'}
heavy = {'BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'WLD', 'NEAR', 'LINK', 'SUI', 'APT', 'UNI', 'ETC', 'AXS', 'BEAM'}

kl_session = [
    s for s in s1_all
    if s.get('side', 'long') == 'long'
    and float(s.get('leader_swept_usd', 0) or 0) >= 20000.0
    and float(s.get('follower_spread_bps', 999) or 999) <= 15.0
    and s.get('follower_exchange', '').lower() != 'asterdex'
    and s.get('coin') not in toxic
    and s.get('coin') not in heavy
]

kl_clean = [
    s for s in kl_session
    if s.get('market_regime') not in ('FLASH_DUMP', 'BEAR_DRIFT')
    and float(s.get('market_breadth_pct', 50.0) or 50.0) >= 50.0
]

print(f"\nFiltered 'Корейский Лонг' ($20k+ sweep, tight spread, no toxic/majors): {len(kl_session)} trades")
if kl_session:
    kl_p = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in kl_session]
    kl_w = sum(1 for p in kl_p if p > 0)
    print(f"  Win Rate:     {kl_w/len(kl_session)*100:.1f}% ({kl_w}/{len(kl_session)})")
    print(f"  Total Net:    {sum(kl_p):+.1f} bps")
    print(f"  Avg Net / Tr: {np.mean(kl_p):+.2f} bps")

print(f"\nClean 'Корейский Лонг' (+ Regime: No Dumps, Breadth >= 50%): {len(kl_clean)} trades")
if kl_clean:
    kc_p = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in kl_clean]
    kc_w = sum(1 for p in kc_p if p > 0)
    print(f"  Win Rate:     {kc_w/len(kl_clean)*100:.1f}% ({kc_w}/{len(kl_clean)})")
    print(f"  Total Net:    {sum(kc_p):+.1f} bps")
    print(f"  Avg Net / Tr: {np.mean(kc_p):+.2f} bps")
    print("  Список сделок Clean KL:")
    for s in kl_clean:
        c = s.get('coin')
        ex = s.get('follower_exchange')
        pnl = float(s.get('net_pnl_bps', 0.0) or 0.0)
        sw = float(s.get('leader_swept_usd', 0) or 0)
        cr = s.get('close_reason')
        br = s.get('market_breadth_pct')
        print(f"    {c:<8} @ {ex:<10} | Swept=${sw:>6.0f} | Net={pnl:>+6.1f} bps | {cr:<18} | Breadth={br}%")

# ==========================================
# 4. STRATEGY 5: CROSS-KOREAN TRIANGLE AUDIT
# ==========================================
print("\n" + "=" * 98)
print("             STRATEGY 5: CROSS-KOREAN TRIANGLE (BITHUMB -> UPBIT -> FOLLOWER)")
print("=" * 98)
s5_all = strat_signals.get('cross_korean_triangle', [])
print(f"Total S5 Signals (18.5h): {len(s5_all)}")
if s5_all:
    s5_p = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in s5_all]
    s5_w = sum(1 for p in s5_p if p > 0)
    print(f"S5 Win Rate:      {s5_w/len(s5_all)*100:.1f}% ({s5_w}/{len(s5_all)})")
    print(f"S5 Total Net PnL: {sum(s5_p):+.1f} bps")
    print(f"S5 Avg Net / Tr:  {np.mean(s5_p):+.2f} bps")

    s5_exs = defaultdict(lambda: {'n': 0, 'wins': 0, 'net': 0.0})
    for s in s5_all:
        ex = s.get('follower_exchange', 'unknown')
        pnl = float(s.get('net_pnl_bps', 0.0) or 0.0)
        s5_exs[ex]['n'] += 1
        if pnl > 0: s5_exs[ex]['wins'] += 1
        s5_exs[ex]['net'] += pnl
    print("\n--- S5 Performance by Venue ---")
    for ex, st in sorted(s5_exs.items(), key=lambda x: x[1]['net'], reverse=True):
        n = st['n']
        w = st['wins']
        net = st['net']
        print(f"  {ex:<12}: {n:>2} trades | WR: {w/n*100:>5.1f}% | Net: {net:>+7.1f} bps | Avg: {net/n:>+6.1f} bps")

# ==========================================
# 5. STRATEGY 2 & 3 SUMMARY
# ==========================================
print("\n" + "=" * 98)
print("             STRATEGY 2 (FADE) & STRATEGY 3 (MAKER VOIDS)")
print("=" * 98)
s2_all = strat_signals.get('fade_fakeout', [])
s3_all = strat_signals.get('maker_void', [])
s3_filled = [s for s in s3_all if s.get('maker_filled') is True]

s2_p = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in s2_all]
s2_w = sum(1 for p in s2_p if p > 0)
print(f"S2 Fade Fakeout: {len(s2_all)} trades | WR: {s2_w/len(s2_all)*100:.1f}% | Net: {sum(s2_p):+.1f} bps | Avg: {np.mean(s2_p):+.2f} bps")

s3_p = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in s3_filled]
s3_w = sum(1 for p in s3_p if p > 0)
print(f"S3 Maker Voids:  {len(s3_all)} orders | {len(s3_filled)} fills ({len(s3_filled)/len(s3_all)*100:.1f}%) | WR: {s3_w/len(s3_filled)*100:.1f}% | Net: {sum(s3_p):+.1f} bps | Avg: {np.mean(s3_p):+.2f} bps")

print("=" * 98)
