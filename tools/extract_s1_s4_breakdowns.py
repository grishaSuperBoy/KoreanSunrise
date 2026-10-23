"""
extract_s1_s4_breakdowns.py - Compute precise PnL, Win/Loss, Earned vs Lost, by Exchange and by Coin
for Strategy 1 (Raw & Filtered Korean Long) and Strategy 4 (Rubber Band Fills).
"""
import glob, gzip, json, os, sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Files of the 18.5h session (chunks 0001 to 0037)
files_22 = [f"data/signals/2026-10-22/sunrise_signals_20261022_{t}.jsonl.gz" for t in [
    "205141_0001", "212142_0002", "215143_0003", "222143_0004", "225144_0005", "232144_0006", "235145_0007"
]]
files_23 = sorted(glob.glob("data/signals/2026-10-23/*.jsonl.gz"))
session_files = [f for f in files_22 if os.path.exists(f)] + files_23

print(f"Loading {len(session_files)} session files (18.5h)...")

s1_raw = []
s4_fills = []

for f in session_files:
    try:
        with gzip.open(f, "rt", encoding="utf-8") as gz:
            for line in gz:
                if not line.strip(): continue
                s = json.loads(line)
                st = s.get("strategy")
                if not st and "gross_exec_edge_bps" in s:
                    st = "lead_lag"
                if st == "lead_lag":
                    s1_raw.append(s)
                elif st == "rubber_band" and s.get("maker_filled") is True:
                    s4_fills.append(s)
    except Exception as e:
        print(f"Error {f}: {e}")

print(f"Loaded: S1 Raw = {len(s1_raw)} trades | S4 Fills = {len(s4_fills)} trades\n")

# Filtered Korean Long criteria from doc.md
toxic = {'CAP', 'ERA', 'GTC', 'GMT', 'FLOCK', 'DRV', 'ATH', 'SPEC', 'MASK', 'B3', 'CP'}
heavy = {'BTC', 'ETH', 'SOL', 'XRP', 'DOGE', 'WLD', 'NEAR', 'LINK', 'SUI', 'APT', 'UNI', 'ETC', 'AXS', 'BEAM'}

s1_kl = [
    s for s in s1_raw
    if s.get('side', 'long') == 'long'
    and float(s.get('leader_swept_usd', 0) or 0) >= 20000.0
    and float(s.get('follower_spread_bps', 999) or 999) <= 15.0
    and s.get('follower_exchange', '').lower() != 'asterdex'
    and s.get('coin') not in toxic
    and s.get('coin') not in heavy
]

def analyze_dataset(trades, notional=1000.0):
    """Computes stats by exchange and by coin."""
    by_ex = defaultdict(lambda: {'n': 0, 'wins': 0, 'losses': 0, 'earned_bps': 0.0, 'lost_bps': 0.0, 'net_bps': 0.0})
    by_coin = defaultdict(lambda: {'n': 0, 'wins': 0, 'losses': 0, 'earned_bps': 0.0, 'lost_bps': 0.0, 'net_bps': 0.0})
    
    tot_earned = 0.0
    tot_lost = 0.0
    tot_net = 0.0
    tot_wins = 0
    tot_losses = 0

    for s in trades:
        ex = s.get('follower_exchange', 'unknown').lower()
        coin = s.get('coin', 'unknown').upper()
        pnl = float(s.get('net_pnl_bps', 0.0) or 0.0)
        
        # Totals
        tot_net += pnl
        if pnl > 0:
            tot_wins += 1
            tot_earned += pnl
        else:
            tot_losses += 1
            tot_lost += abs(pnl)
            
        # Exchange
        d_ex = by_ex[ex]
        d_ex['n'] += 1
        d_ex['net_bps'] += pnl
        if pnl > 0:
            d_ex['wins'] += 1
            d_ex['earned_bps'] += pnl
        else:
            d_ex['losses'] += 1
            d_ex['lost_bps'] += abs(pnl)
            
        # Coin
        d_c = by_coin[coin]
        d_c['n'] += 1
        d_c['net_bps'] += pnl
        if pnl > 0:
            d_c['wins'] += 1
            d_c['earned_bps'] += pnl
        else:
            d_c['losses'] += 1
            d_c['lost_bps'] += abs(pnl)

    return {
        'total': {'n': len(trades), 'wins': tot_wins, 'losses': tot_losses, 'earned_bps': tot_earned, 'lost_bps': tot_lost, 'net_bps': tot_net},
        'by_ex': by_ex,
        'by_coin': by_coin
    }

def print_table(title, data, notional=1000.0):
    print("=" * 110)
    print(f" {title.upper()} (РАСЧЕТ НА САЙЗ ${notional:.0f} НА СДЕЛКУ)")
    print("=" * 110)
    
    tot = data['total']
    n = tot['n']
    wr = (tot['wins'] / n * 100) if n else 0
    tot_usd = (tot['net_bps'] / 10000.0) * notional * n
    earned_usd = (tot['earned_bps'] / 10000.0) * notional
    lost_usd = (tot['lost_bps'] / 10000.0) * notional
    net_usd = (tot['net_bps'] / 10000.0) * notional
    
    print(f"ИТОГО: Сделок={n} | Плюс={tot['wins']} | Минус={tot['losses']} | WinRate={wr:.1f}%")
    print(f"       Заработано: +{tot['earned_bps']:.1f} bps (+${earned_usd:.2f}) | Потеряно: -{tot['lost_bps']:.1f} bps (-${lost_usd:.2f}) | Чистый Net: {tot['net_bps']:+.1f} bps ({net_usd:+.2f}$)")
    print("-" * 110)
    
    # Table by Exchange
    print("РАЗБИВКА ПО БИРЖАМ:")
    print(f"{'Биржа':<12} | {'Сделок':>6} | {'Win':>4} | {'Loss':>4} | {'WR %':>6} | {'Заработано (bps)':>17} | {'Потеряно (bps)':>15} | {'Net (bps)':>10} | {'Net ($)':>10}")
    print("-" * 110)
    for ex, st in sorted(data['by_ex'].items(), key=lambda x: x[1]['net_bps'], reverse=True):
        ex_n = st['n']
        ex_wr = (st['wins'] / ex_n * 100) if ex_n else 0
        ex_usd = (st['net_bps'] / 10000.0) * notional
        print(f"{ex:<12} | {ex_n:>6} | {st['wins']:>4} | {st['losses']:>4} | {ex_wr:>5.1f}% | {st['earned_bps']:>+17.1f} | {-st['lost_bps']:>15.1f} | {st['net_bps']:>+10.1f} | {ex_usd:>+9.2f}$")

    print("-" * 110)
    # Table by Coin
    print("РАЗБИВКА ПО МОНЕТАМ:")
    print(f"{'Монета':<10} | {'Сделок':>6} | {'Win':>4} | {'Loss':>4} | {'WR %':>6} | {'Заработано (bps)':>17} | {'Потеряно (bps)':>15} | {'Net (bps)':>10} | {'Net ($)':>10}")
    print("-" * 110)
    for coin, st in sorted(data['by_coin'].items(), key=lambda x: x[1]['net_bps'], reverse=True):
        c_n = st['n']
        c_wr = (st['wins'] / c_n * 100) if c_n else 0
        c_usd = (st['net_bps'] / 10000.0) * notional
        print(f"{coin:<10} | {c_n:>6} | {st['wins']:>4} | {st['losses']:>4} | {c_wr:>5.1f}% | {st['earned_bps']:>+17.1f} | {-st['lost_bps']:>15.1f} | {st['net_bps']:>+10.1f} | {c_usd:>+9.2f}$")
    print("\n")

# Run analysis
res_s1_raw = analyze_dataset(s1_raw, notional=1000.0)
res_s1_kl = analyze_dataset(s1_kl, notional=1000.0)
res_s4 = analyze_dataset(s4_fills, notional=1000.0)

print_table("СТРАТЕГИЯ 1: ВСЕ СЫРЫЕ СДЕЛКИ LEAD-LAG (18.5 ЧАСОВ)", res_s1_raw)
print_table("СТРАТЕГИЯ 1: КОРЕЙСКИЙ ЛОНГ (ФИЛЬТР $20K+, АЛЬТЫ, СПРЕД <= 15)", res_s1_kl)
print_table("СТРАТЕГИЯ 4: PREDATORY RUBBER BAND (ИСПОЛНЕННЫЕ МЕЙКЕР-ОРДЕРА)", res_s4)
