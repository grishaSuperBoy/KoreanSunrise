import numpy as np
import glob, gzip, json

def run():
    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
    data_list = []
    for fpath in files:
        with gzip.open(fpath, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    d = json.loads(line)
                    path = d.get('path', [])
                    if path and len(path) >= 5 and isinstance(path[0], list) and len(path[0]) >= 4:
                        moves_2_5s = [p[2] for p in path if p[0] <= 2.5]
                        moves_15s = [p[2] for p in path if p[0] <= 15.0]
                        if moves_2_5s and moves_15s:
                            data_list.append({
                                'min_2_5s': min(moves_2_5s),
                                'path': path
                            })
                except Exception: pass

    print(f"Trajectories: {len(data_list)}")

    fee_maker = 0.0 # Maker exit (0 fee)
    stop = 25.0
    max_hold = 20.0

    print(f"\n{'Discount':>10} | {'Target':>8} | {'FillRate':>9} | {'HitTarget':>10} | {'StopLoss':>9} | {'AvgPnL/Fill':>12} | {'EV/Signal':>10}")
    print("-" * 75)

    for d_val in [0.0, 1.0, 2.0, 3.0]:
        for t_val in [3.0, 4.0, 6.0, 8.0, 10.0]:
            fills = 0
            hits = 0
            stops = 0
            pnls = []

            for item in data_list:
                if item['min_2_5s'] > -d_val: continue
                fills += 1
                path = item['path']
                fill_idx = 0
                for idx, p in enumerate(path):
                    if p[0] <= 2.5 and p[2] <= -d_val:
                        fill_idx = idx
                        break
                fill_t = path[fill_idx][0]
                fill_px_rel = -d_val

                outcome = 'timeout'
                trade_pnl = 0.0
                for p in path[fill_idx:]:
                    dt_hold = p[0] - fill_t
                    rel_move = p[2] - fill_px_rel
                    net = rel_move - fee_maker # 0 fee
                    if net >= t_val:
                        outcome = 'target'
                        trade_pnl = t_val
                        break
                    elif net <= -stop:
                        outcome = 'stop'
                        trade_pnl = -stop
                        break
                    elif dt_hold >= max_hold:
                        outcome = 'timeout'
                        trade_pnl = net
                        break

                if outcome == 'timeout':
                    last_net = (path[-1][2] - fill_px_rel) - fee_maker
                    trade_pnl = max(-stop, min(t_val, last_net))

                if outcome == 'target': hits += 1
                elif outcome == 'stop': stops += 1
                pnls.append(trade_pnl)

            fill_rate = fills / len(data_list) * 100.0
            hit_pct = hits / fills * 100.0 if fills > 0 else 0.0
            stop_pct = stops / fills * 100.0 if fills > 0 else 0.0
            avg_pnl = np.mean(pnls) if pnls else 0.0
            ev_sig = np.sum(pnls) / len(data_list)

            print(f"{d_val:10.1f} | {t_val:8.1f} | {fill_rate:8.1f}% | {hit_pct:9.1f}% | {stop_pct:8.1f}% | {avg_pnl:+11.2f}b | {ev_sig:+9.2f}b")

if __name__ == '__main__':
    run()
