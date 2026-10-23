import glob, gzip, json, os, sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

files_22 = [f"data/signals/2026-10-22/sunrise_signals_20261022_{t}.jsonl.gz" for t in [
    "205141_0001", "212142_0002", "215143_0003", "222143_0004", "225144_0005", "232144_0006", "235145_0007"
]]
files_23 = sorted(glob.glob("data/signals/2026-10-23/*.jsonl.gz"))
session_files = [f for f in files_22 if os.path.exists(f)] + files_23

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
    except:
        pass

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

def get_stats(trades):
    by_ex = defaultdict(lambda: {'n': 0, 'w': 0, 'l': 0, 'earned': 0.0, 'lost': 0.0, 'net': 0.0})
    by_c = defaultdict(lambda: {'n': 0, 'w': 0, 'l': 0, 'earned': 0.0, 'lost': 0.0, 'net': 0.0})
    for s in trades:
        ex = s.get('follower_exchange', 'unknown').lower()
        c = s.get('coin', 'unknown').upper()
        p = float(s.get('net_pnl_bps', 0.0) or 0.0)
        for d, k in [(by_ex, ex), (by_c, c)]:
            d[k]['n'] += 1
            d[k]['net'] += p
            if p > 0:
                d[k]['w'] += 1
                d[k]['earned'] += p
            else:
                d[k]['l'] += 1
                d[k]['lost'] += abs(p)
    return by_ex, by_c

# S1 Stats
s1_raw_ex, s1_raw_c = get_stats(s1_raw)
s1_kl_ex, s1_kl_c = get_stats(s1_kl)
s4_ex, s4_c = get_stats(s4_fills)

print("=== S4 BY EXCHANGE ===")
for ex, st in sorted(s4_ex.items(), key=lambda x: x[1]['net'], reverse=True):
    wr = st['w'] / st['n'] * 100 if st['n'] else 0
    print(f"EX: {ex:<12} | N={st['n']:>3} | W={st['w']:>3} | L={st['l']:>3} | WR={wr:>5.1f}% | Earned=+{st['earned']:>7.1f}b | Lost=-{st['lost']:>7.1f}b | Net={st['net']:>+8.1f}b")

print("\n=== S4 TOP 8 WINNING COINS ===")
for c, st in sorted(s4_c.items(), key=lambda x: x[1]['net'], reverse=True)[:8]:
    wr = st['w'] / st['n'] * 100 if st['n'] else 0
    print(f"COIN: {c:<8} | N={st['n']:>3} | W={st['w']:>2} | L={st['l']:>2} | WR={wr:>5.1f}% | Earned=+{st['earned']:>6.1f}b | Lost=-{st['lost']:>6.1f}b | Net={st['net']:>+7.1f}b")

print("\n=== S4 TOP 8 LOSING COINS ===")
for c, st in sorted(s4_c.items(), key=lambda x: x[1]['net'])[:8]:
    wr = st['w'] / st['n'] * 100 if st['n'] else 0
    print(f"COIN: {c:<8} | N={st['n']:>3} | W={st['w']:>2} | L={st['l']:>2} | WR={wr:>5.1f}% | Earned=+{st['earned']:>6.1f}b | Lost=-{st['lost']:>6.1f}b | Net={st['net']:>+7.1f}b")
