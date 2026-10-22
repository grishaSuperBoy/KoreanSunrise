import numpy as np
import glob, gzip, json

def run():
    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
    mfes_15s = []
    for fpath in files:
        with gzip.open(fpath, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    path = data.get('path', [])
                    if path and len(path) >= 5 and isinstance(path[0], list) and len(path[0]) >= 4:
                        moves = [p[2] for p in path if p[0] <= 15.0]
                        if moves: mfes_15s.append(max(moves))
                except Exception:
                    pass

    mfes = np.array(mfes_15s)
    print(f"Dataset Size N = {len(mfes)}")

    print("\n--- QUANTITATIVE TARGET OPTIMIZATION: Taker Exit (Exit Fee = 5.0 bps) ---")
    print(f"{'Target T (bps)':>14} | {'P(Hit T)':>10} | {'Net Profit if Hit':>18} | {'EV (bps)':>10}")
    print("-" * 60)
    best_ev_taker = -999.0
    best_t_taker = None
    for t in np.arange(5.5, 25.5, 0.5):
        p_hit = np.mean(mfes >= t)
        net_win = t - 5.0
        ev = p_hit * net_win
        if ev > best_ev_taker:
            best_ev_taker = ev
            best_t_taker = t
        if round(t, 1) in [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 20.0, 25.0]:
            print(f"{t:14.1f} | {p_hit*100:9.1f}% | {net_win:+17.1f}b | {ev:+9.2f}b")

    print(f"\nOptimal Target (Taker Exit): T* = {best_t_taker:.1f} bps (Max EV = {best_ev_taker:.2f} bps, P(Hit)={np.mean(mfes >= best_t_taker)*100:.1f}%)")

    print("\n--- QUANTITATIVE TARGET OPTIMIZATION: Maker Exit (Exit Fee = 0.0 bps) ---")
    print(f"{'Target T (bps)':>14} | {'P(Hit T)':>10} | {'Net Profit if Hit':>18} | {'EV (bps)':>10}")
    print("-" * 60)
    best_ev_maker = -999.0
    best_t_maker = None
    for t in np.arange(1.0, 20.5, 0.5):
        p_hit = np.mean(mfes >= t)
        net_win = t - 0.0
        ev = p_hit * net_win
        if ev > best_ev_maker:
            best_ev_maker = ev
            best_t_maker = t
        if round(t, 1) in [2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 10.0, 12.0, 15.0]:
            print(f"{t:14.1f} | {p_hit*100:9.1f}% | {net_win:+17.1f}b | {ev:+9.2f}b")

    print(f"\nOptimal Target (Maker Exit): T* = {best_t_maker:.1f} bps (Max EV = {best_ev_maker:.2f} bps, P(Hit)={np.mean(mfes >= best_t_maker)*100:.1f}%)")

if __name__ == '__main__':
    run()
