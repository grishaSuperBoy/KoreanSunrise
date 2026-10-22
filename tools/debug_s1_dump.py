import glob, gzip, json
from collections import defaultdict
import numpy as np

def run():
    files = sorted(glob.glob('data/signals/2026-10-22/*.jsonl.gz'))
    signals = []
    for f in files:
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    if data.get('strategy') == 'lead_lag':
                        signals.append(data)
                except Exception: pass

    breakdown = defaultdict(lambda: {'long': 0, 'short': 0, 'long_pnl': [], 'short_pnl': []})
    for s in signals:
        reg = s.get('market_regime', 'unknown')
        side = s.get('side', 'unknown')
        pnl = s.get('net_pnl_bps', 0.0)
        breakdown[reg][side] += 1
        breakdown[reg][f'{side}_pnl'].append(pnl)

    print(f"\nTotal S1 Lead-Lag signals today: {len(signals)}")
    print(f"{'Regime':<15} | {'Long Count':<10} | {'Long AvgPnL':<12} | {'Short Count':<11} | {'Short AvgPnL':<12}")
    print("-" * 70)
    for reg, d in breakdown.items():
        l_count = d['long']
        s_count = d['short']
        l_pnl = np.mean(d['long_pnl']) if d['long_pnl'] else 0.0
        s_pnl = np.mean(d['short_pnl']) if d['short_pnl'] else 0.0
        print(f"{reg:<15} | {l_count:<10} | {l_pnl:+10.2f}b | {s_count:<11} | {s_pnl:+11.2f}b")

    # Let's inspect 5 sample Longs in FLASH_DUMP
    dump_longs = [s for s in signals if s.get('market_regime') == 'FLASH_DUMP' and s.get('side') == 'long']
    print(f"\n--- SAMPLE LONGS IN FLASH_DUMP (N={len(dump_longs)}) ---")
    for s in dump_longs[:5]:
        print(f"ID: {s.get('id')} | Coin: {s.get('coin')} | Ex: {s.get('follower_exchange')} | Open: {s.get('open_ts')} | GrossEdge: {s.get('gross_exec_edge_bps')} | NetPnL: {s.get('net_pnl_bps')} | CloseReason: {s.get('close_reason')} | Breadth: {s.get('market_breadth_pct')}% | BTC_ret: {s.get('btc_ret_300s_bps')}bps")

    # Let's inspect 5 sample Shorts in FLASH_DUMP
    dump_shorts = [s for s in signals if s.get('market_regime') == 'FLASH_DUMP' and s.get('side') == 'short']
    print(f"\n--- SAMPLE SHORTS IN FLASH_DUMP (N={len(dump_shorts)}) ---")
    for s in dump_shorts[:5]:
        print(f"ID: {s.get('id')} | Coin: {s.get('coin')} | Ex: {s.get('follower_exchange')} | Open: {s.get('open_ts')} | GrossEdge: {s.get('gross_exec_edge_bps')} | NetPnL: {s.get('net_pnl_bps')} | CloseReason: {s.get('close_reason')} | Breadth: {s.get('market_breadth_pct')}% | BTC_ret: {s.get('btc_ret_300s_bps')}bps")

if __name__ == '__main__':
    run()
