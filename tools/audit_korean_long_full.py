"""
audit_korean_long_full.py - Comprehensive, rigorous audit of Korean Long strategy across all signals (Oct 20-23, 2026)
Uses the exact criteria from tools/report_evening_20261022.py and evaluates every single parameter transparently.
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

all_files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
print(f"=== AUDITING KOREAN LONG ACROSS ALL {len(all_files)} SIGNAL ARCHIVES ===")

all_s1 = []
files_by_day = defaultdict(list)
for f in all_files:
    day = f.split('signals')[1].replace('\\', '/').split('/')[1]
    files_by_day[day].append(f)

for f in all_files:
    day = f.split('signals')[1].replace('\\', '/').split('/')[1]
    try:
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip():
                    continue
                s = json.loads(line)
                strat = s.get('strategy')
                if not strat and 'gross_exec_edge_bps' in s:
                    strat = 'lead_lag'
                if strat == 'lead_lag':
                    s['_day'] = day
                    s['_file'] = os.path.basename(f)
                    all_s1.append(s)
    except Exception as e:
        print(f"Error reading {f}: {e}")

print(f"Total S1 (Lead-Lag) Signals in database: {len(all_s1)}")
for d in sorted(files_by_day.keys()):
    day_sigs = [s for s in all_s1 if s['_day'] == d]
    print(f"  {d}: {len(day_sigs)} signals")

# Baseline raw S1
raw_pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in all_s1]
raw_wins = sum(1 for p in raw_pnls if p > 0)
print("\n" + "=" * 90)
print(f"1. RAW S1 (ALL SIGNALS, NO FILTERS AT ALL): N={len(all_s1)}")
print(f"   Win Rate:     {raw_wins/len(all_s1)*100:.1f}% ({raw_wins}/{len(all_s1)})")
print(f"   Total Net:    {sum(raw_pnls):+.1f} bps")
print(f"   Avg Net / Tr: {np.mean(raw_pnls):+.2f} bps")
print("=" * 90)

# Exact filters from report_evening_20261022.py
toxic = {'CAP', 'ERA', 'GTC', 'GMT', 'FLOCK', 'DRV', 'ATH', 'SPEC', 'MASK', 'B3', 'CP'}
heavy = {'BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'WLD', 'NEAR', 'LINK', 'SUI', 'APT', 'UNI', 'ETC', 'AXS', 'BEAM'}

# Let's test step-by-step filter waterfall:
step1_long_only = [s for s in all_s1 if s.get('side', 'long') == 'long']
step2_swept_20k = [s for s in step1_long_only if float(s.get('leader_swept_usd', 0) or 0) >= 20000.0]
step3_tight_spread = [s for s in step2_swept_20k if float(s.get('follower_spread_bps', 999) or 999) <= 15.0]
step4_no_asterdex = [s for s in step3_tight_spread if s.get('follower_exchange', '').lower() != 'asterdex']
step5_no_toxic_heavy = [s for s in step4_no_asterdex if s.get('coin') not in toxic and s.get('coin') not in heavy]
step6_regime_no_dump = [s for s in step5_no_toxic_heavy if s.get('market_regime') not in ('FLASH_DUMP', 'BEAR_DRIFT')]
step7_full_korean_long = [s for s in step6_regime_no_dump if float(s.get('market_breadth_pct', 50.0) or 50.0) >= 50.0]

steps = [
    ("Raw All S1", all_s1),
    ("Step 1: Long side only", step1_long_only),
    ("Step 2: Swept >= $20,000", step2_swept_20k),
    ("Step 3: Follower spread <= 15 bps", step3_tight_spread),
    ("Step 4: Exclude Asterdex DEX", step4_no_asterdex),
    ("Step 5: Exclude Heavy Majors & Toxic", step5_no_toxic_heavy),
    ("Step 6: Exclude FLASH_DUMP & BEAR_DRIFT", step6_regime_no_dump),
    ("Step 7: Full 'Корейский Лонг' (Breadth >= 50%)", step7_full_korean_long),
]

print("\n" + "=" * 90)
print("                    FILTER WATERFALL: EXACT STEP-BY-STEP AUDIT")
print("=" * 90)
print(f"{'Фильтр':<45} | {'Сделок':>6} | {'WinRate':>7} | {'Net Bps':>10} | {'Ср Net/Tr':>10}")
print("-" * 90)
for name, sigs in steps:
    n = len(sigs)
    if n == 0:
        print(f"{name:<45} | {0:>6} |       - |          - |          -")
        continue
    pnls = [float(s.get('net_pnl_bps', 0.0) or 0.0) for s in sigs]
    w = sum(1 for p in pnls if p > 0)
    wr = w / n * 100
    tot = sum(pnls)
    avg = np.mean(pnls)
    print(f"{name:<45} | {n:>6} | {wr:>6.1f}% | {tot:>+10.1f} | {avg:>+10.2f}")

print("-" * 90)

# Deep dive into Step 7: ALL trades that ever passed Full Korean Long
print("\n" + "=" * 90)
print(f"      ПОЛНЫЙ СПИСОК ВСЕХ СДЕЛОК STEP 7 ('КОРЕЙСКИЙ ЛОНГ' BREADTH >= 50%): N={len(step7_full_korean_long)}")
print("=" * 90)
if step7_full_korean_long:
    for i, s in enumerate(step7_full_korean_long):
        day = s['_day']
        c = s.get('coin')
        ex = s.get('follower_exchange')
        sw = float(s.get('leader_swept_usd', 0) or 0)
        sp = float(s.get('follower_spread_bps', 0) or 0)
        pnl = float(s.get('net_pnl_bps', 0) or 0)
        cr = s.get('close_reason')
        dur = float(s.get('duration_sec', 0) or 0)
        reg = s.get('market_regime')
        br = s.get('market_breadth_pct')
        print(f"{i+1:>2}. [{day}] {c:<6} @ {ex:<10} | Swept=${sw:>6.0f} | Spread={sp:>4.1f}b | Net={pnl:>+6.1f}b | Dur={dur:>4.1f}s | {cr:<18} | Reg={reg} (Br={br}%)")
else:
    print("  Нет сделок, прошедших ВСЕ фильтры одновременно!")

# Deep dive into Step 5 (Swept >= 20k, spread <= 15, no asterdex, no heavy/toxic, before regime filter)
print("\n" + "=" * 90)
print(f"      ПОЛНЫЙ СПИСОК СДЕЛОК STEP 5 (ДО ФИЛЬТРА РЕЖИМА РЫНКА): N={len(step5_no_toxic_heavy)}")
print("=" * 90)
for i, s in enumerate(step5_no_toxic_heavy):
    day = s['_day']
    c = s.get('coin')
    ex = s.get('follower_exchange')
    sw = float(s.get('leader_swept_usd', 0) or 0)
    sp = float(s.get('follower_spread_bps', 0) or 0)
    pnl = float(s.get('net_pnl_bps', 0) or 0)
    cr = s.get('close_reason')
    dur = float(s.get('duration_sec', 0) or 0)
    reg = s.get('market_regime')
    br = s.get('market_breadth_pct')
    print(f"{i+1:>2}. [{day}] {c:<6} @ {ex:<10} | Swept=${sw:>6.0f} | Spread={sp:>4.1f}b | Net={pnl:>+6.1f}b | Dur={dur:>4.1f}s | {cr:<18} | Reg={reg} (Br={br}%)")

print("=" * 90)
