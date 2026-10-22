import glob
import gzip
import json
import math
from collections import defaultdict

def run():
    # Load all signals from 2026-10-20, 2026-10-21, 2026-10-22 that have path
    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
    print(f"Scanning {len(files)} files...")
    
    signals = []
    for fpath in files:
        with gzip.open(fpath, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    path = data.get('path', [])
                    if path and len(path) >= 3:
                        if isinstance(path[0], list) and len(path[0]) >= 4:
                            signals.append({
                                'id': data.get('id'),
                                'coin': data.get('coin'),
                                'ex': data.get('follower_exchange'),
                                'spread': data.get('follower_spread_bps', 10.0),
                                'disloc': data.get('dislocation_bps', data.get('raw_dislocation_bps', 0.0)),
                                'regime': data.get('market_regime', 'NEUTRAL'),
                                'breadth': data.get('market_breadth_pct', 50.0),
                                'sigma': data.get('pair_sigma_bps', 25.0),
                                'path': path
                            })
                except Exception:
                    pass

    print(f"Loaded {len(signals)} valid trajectory signals with full paths.")
    if not signals:
        return

    # Check path length and time range
    durations = [s['path'][-1][0] - s['path'][0][0] for s in signals]
    print(f"Trajectory durations: min={min(durations):.1f}s, med={sorted(durations)[len(durations)//2]:.1f}s, max={max(durations):.1f}s")

    fee = 5.0
    stop_loss = 25.0
    ttl = 2.5
    max_hold = 30.0

    deltas = [0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 10.0, 15.0, 25.0]
    targets = [4.0, 6.0, 8.0, 10.0, 12.0, 15.0, 20.0, 25.0, 35.0]

    grid = {}
    for d in deltas:
        for t in targets:
            grid[(d, t)] = {'fills': 0, 'wins': 0, 'losses': 0, 'timeouts': 0, 'sum_pnl': 0.0, 'pnls': []}

    total_sigs = len(signals)

    for sig in signals:
        path = sig['path']
        ttl_points = [p for p in path if p[0] <= ttl]
        if not ttl_points:
            ttl_points = [path[0]]

        # min_move_in_ttl is the lowest follower mid movement in the first 2.5s
        min_move_in_ttl = min(p[2] for p in ttl_points)

        for d in deltas:
            # Did the predatory limit bid fill?
            # If d == 0: placed at best bid, filled on any touch/downtick (min_move <= 0)
            # If d > 0: placed at d bps discount below best bid, filled if price flushed down by >= d bps
            filled = (min_move_in_ttl <= -d)
            if not filled:
                continue

            # Find fill index
            fill_idx = 0
            for idx, p in enumerate(path):
                if p[0] <= ttl and p[2] <= -d:
                    fill_idx = idx
                    break
            
            fill_t = path[fill_idx][0]
            fill_px_rel = -d # entry executed at -d bps discount

            sub_path = path[fill_idx:]
            
            for t in targets:
                g = grid[(d, t)]
                g['fills'] += 1
                
                outcome = None
                pnl = 0.0
                for p in sub_path:
                    dt_hold = p[0] - fill_t
                    rel_move = p[2] - fill_px_rel
                    cur_net = rel_move - fee

                    if cur_net >= t:
                        outcome = 'win'
                        pnl = t
                        break
                    elif cur_net <= -stop_loss:
                        outcome = 'loss'
                        pnl = -stop_loss
                        break
                    elif dt_hold >= max_hold:
                        outcome = 'timeout'
                        pnl = cur_net
                        break

                if outcome is None:
                    p_last = sub_path[-1]
                    cur_net = (p_last[2] - fill_px_rel) - fee
                    outcome = 'timeout'
                    pnl = cur_net

                if outcome == 'win':
                    g['wins'] += 1
                elif outcome == 'loss':
                    g['losses'] += 1
                else:
                    g['timeouts'] += 1
                g['sum_pnl'] += pnl
                g['pnls'].append(pnl)

    print(f"\n=== QUANTITATIVE GRID SEARCH: S4 RUBBER BAND (N={total_sigs} signals) ===")
    print(f"Fee={fee} bps | Stop={stop_loss} bps | TTL={ttl}s | MaxHold={max_hold}s\n")
    print(f"{'Discount':>10} | {'Target':>8} | {'FillRate':>9} | {'Fills':>6} | {'WinRate':>8} | {'AvgPnL':>10} | {'EV/Signal':>10}")
    print("-" * 75)

    best_ev = -999.0
    best_combo = None

    for d in deltas:
        for t in targets:
            g = grid[(d, t)]
            fills = g['fills']
            fill_rate = fills / total_sigs * 100.0
            if fills > 0:
                win_rate = g['wins'] / fills * 100.0
                avg_pnl = g['sum_pnl'] / fills
                ev_per_signal = g['sum_pnl'] / total_sigs
            else:
                win_rate = 0.0
                avg_pnl = 0.0
                ev_per_signal = 0.0

            if ev_per_signal > best_ev:
                best_ev = ev_per_signal
                best_combo = (d, t, fill_rate, win_rate, avg_pnl, ev_per_signal)

            print(f"{d:10.1f} | {t:8.1f} | {fill_rate:8.1f}% | {fills:6d} | {win_rate:7.1f}% | {avg_pnl:+9.2f}b | {ev_per_signal:+9.2f}b")

    print("=" * 75)
    print(f"\nOPTIMAL (Maximum EV per Opportunity):")
    print(f"Discount d* = {best_combo[0]:.1f} bps")
    print(f"Target   T* = {best_combo[1]:.1f} bps")
    print(f"Fill Rate   = {best_combo[2]:.1f}%")
    print(f"Win Rate    = {best_combo[3]:.1f}%")
    print(f"Avg Trade   = {best_combo[4]:+.2f} bps")
    print(f"EV / Signal = {best_combo[5]:+.2f} bps")

if __name__ == '__main__':
    run()
