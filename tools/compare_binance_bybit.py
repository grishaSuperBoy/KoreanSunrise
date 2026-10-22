import sys
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
import glob, gzip, json

def run():
    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
    signals = []
    for f in files:
        with gzip.open(f, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    d = json.loads(line)
                    if (d.get('strategy') == 'lead_lag' or ('gross_exec_edge_bps' in d and 'strategy' not in d)) and d.get('side', 'long') == 'long':
                        if float(d.get('leader_swept_usd', 0) or 0) >= 20000.0 and float(d.get('follower_spread_bps', 999) or 999) <= 15.0:
                            signals.append(d)
                except Exception: pass

    binance_sigs = [s for s in signals if s.get('follower_exchange') == 'binance']
    bybit_sigs = [s for s in signals if s.get('follower_exchange') == 'bybit']

    print(f"=== BINANCE (N={len(binance_sigs)}) ===")
    for s in binance_sigs:
        coin = s.get('coin')
        swept = float(s.get('leader_swept_usd', 0))
        edge = float(s.get('gross_exec_edge_bps', 0))
        net = float(s.get('net_pnl_bps', 0))
        mfe = float(s.get('mfe_exec_pnl_bps', 0))
        dur = float(s.get('duration_sec', 0))
        ttp = float(s.get('time_to_peak_sec', 0))
        age = float(s.get('follower_book_age_ms', 0) or 0)
        f_lat = float(s.get('follower_latency_ms', 0) or 0)
        reason = s.get('close_reason')
        print(f"{coin:<6} | Swept=${swept:>7,.0f} | Edge={edge:5.1f}b | Net={net:+6.1f}b | MFE={mfe:+5.1f}b | Dur={dur:4.1f}s | TTP={ttp:4.1f}s | Lat={f_lat:4.1f}ms | Age={age:4.0f}ms | {reason}")

    print(f"\n=== BYBIT (N={len(bybit_sigs)}) ===")
    for s in bybit_sigs:
        coin = s.get('coin')
        swept = float(s.get('leader_swept_usd', 0))
        edge = float(s.get('gross_exec_edge_bps', 0))
        net = float(s.get('net_pnl_bps', 0))
        mfe = float(s.get('mfe_exec_pnl_bps', 0))
        dur = float(s.get('duration_sec', 0))
        ttp = float(s.get('time_to_peak_sec', 0))
        age = float(s.get('follower_book_age_ms', 0) or 0)
        f_lat = float(s.get('follower_latency_ms', 0) or 0)
        reason = s.get('close_reason')
        print(f"{coin:<6} | Swept=${swept:>7,.0f} | Edge={edge:5.1f}b | Net={net:+6.1f}b | MFE={mfe:+5.1f}b | Dur={dur:4.1f}s | TTP={ttp:4.1f}s | Lat={f_lat:4.1f}ms | Age={age:4.0f}ms | {reason}")

    print("\n=== ДЕТАЛЬНОЕ СРАВНЕНИЕ ОДНОВРЕМЕННЫХ СИГНАЛОВ: ENSO & IRYS ===")
    for target in ['ENSO', 'IRYS', 'DOOD']:
        print(f"\n--- {target} ---")
        target_sigs = [s for s in signals if s.get('coin') == target and float(s.get('leader_swept_usd', 0) or 0) >= 30000]
        for s in target_sigs:
            ex = s.get('follower_exchange')
            ts = s.get('open_ts')
            net = float(s.get('net_pnl_bps', 0))
            mfe = float(s.get('mfe_exec_pnl_bps', 0))
            dur = float(s.get('duration_sec', 0))
            age = float(s.get('follower_book_age_ms', 0) or 0)
            reason = s.get('close_reason')
            entry_px = s.get('follower_entry_px')
            edge = s.get('gross_exec_edge_bps')
            print(f"  {ex:<10} | ts={ts} | entry={entry_px} | Edge={edge}b | Net={net:+6.1f}b | MFE={mfe:+5.1f}b | Dur={dur:4.1f}s | Age={age:4.0f}ms | {reason}")

if __name__ == '__main__':
    run()
