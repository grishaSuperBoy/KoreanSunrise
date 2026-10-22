import sys
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
import glob, gzip, json

files = sorted(glob.glob('data/signals/2026-10-22/*.jsonl.gz'))[-11:]
alt_trades = []
for f in files:
    with gzip.open(f, 'rt', encoding='utf-8') as gz:
        for line in gz:
            if not line.strip(): continue
            try:
                s = json.loads(line)
                if s.get('strategy') == 'rubber_band' and s.get('coin') == 'ALT' and s.get('close_reason') != 'unfilled_timeout':
                    alt_trades.append(s)
            except Exception: pass

print(f"Всего исполненных сделок по ALT: {len(alt_trades)}")
for s in alt_trades[:15]:
    ex = s.get('follower_exchange')
    dis = s.get('dislocation_bps_open')
    z = s.get('dislocation_z')
    pnl = s.get('net_pnl_bps')
    dur = s.get('duration_sec')
    cr = s.get('close_reason')
    print(f"  ALT @ {ex:<12} | disloc={dis:>6.1f}b | Z={z:>4.1f} | PnL={pnl:>+6.1f}b | Dur={dur:>4.1f}s | {cr}")
