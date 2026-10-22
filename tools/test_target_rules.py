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
                        moves_15s = [p[2] for p in path if p[0] <= 15.0]
                        if moves_15s:
                            data_list.append({
                                'spread': d.get('follower_spread_bps', 10.0),
                                'disloc': d.get('dislocation_bps', d.get('raw_dislocation_bps', 0.0)),
                                'sigma': d.get('pair_sigma_bps', 25.0),
                                'mfe': max(moves_15s),
                                'mae': min(moves_15s)
                            })
                except Exception:
                    pass

    print(f"Loaded {len(data_list)} valid trajectories.")

    # Compare static targets vs dynamic target rules
    # Let Fee = 5.0 bps (taker exit), Stop = 25.0 bps
    fee = 5.0
    stop = 25.0

    rules = [
        ("Static 8.0 bps", lambda d: 8.0),
        ("Static 10.0 bps", lambda d: 10.0),
        ("Static 12.0 bps", lambda d: 12.0),
        ("Static 15.0 bps", lambda d: 15.0),
        ("Static 35.0 bps (Old)", lambda d: 35.0),
        ("Spread-based: 1.0 * Spread", lambda d: max(6.0, min(20.0, 1.0 * d['spread']))),
        ("Spread-based: 1.2 * Spread", lambda d: max(6.0, min(25.0, 1.2 * d['spread']))),
        ("Spread + Fee: Spread + 5 bps", lambda d: max(8.0, min(25.0, d['spread'] + fee))),
        ("Spread + 0.2*Sigma", lambda d: max(8.0, min(25.0, 0.8 * d['spread'] + 0.2 * d['sigma']))),
        ("Elastic Disloc: 0.05 * Disloc + 0.5*Spread", lambda d: max(8.0, min(25.0, 0.05 * d['disloc'] + 0.5 * d['spread']))),
    ]

    print("\n--- PERFORMANCE OF TARGET RULES (Exit Fee = 5 bps, Stop = 25 bps) ---")
    print(f"{'Rule Name':<42} | {'Avg Target':>10} | {'Hit Rate':>8} | {'Stop Rate':>9} | {'EV (bps)':>10}")
    print("-" * 88)

    for name, fn in rules:
        hits = 0
        stops = 0
        timeouts = 0
        pnls = []
        targets_eval = []
        for d in data_list:
            t = fn(d)
            targets_eval.append(t)
            mfe = d['mfe']
            mae = d['mae']
            # If hit target
            if mfe >= t:
                hits += 1
                pnls.append(t - fee)
            elif mae <= -stop:
                stops += 1
                pnls.append(-stop - fee)
            else:
                timeouts += 1
                # close at last mfe/move minus fee
                pnls.append(max(-stop, min(t, mfe)) - fee)

        avg_t = np.mean(targets_eval)
        hit_rate = hits / len(data_list) * 100.0
        stop_rate = stops / len(data_list) * 100.0
        ev = np.mean(pnls)
        print(f"{name:<42} | {avg_t:9.1f}b | {hit_rate:7.1f}% | {stop_rate:8.1f}% | {ev:+9.2f}b")

if __name__ == '__main__':
    run()
