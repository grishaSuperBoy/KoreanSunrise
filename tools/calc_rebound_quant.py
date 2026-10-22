import glob
import gzip
import json
import math
import numpy as np

def run():
    files = sorted(glob.glob('data/signals/**/*.jsonl.gz', recursive=True))
    print(f"Reading {len(files)} files for MFE and Rebound distribution...")
    
    signals = []
    for fpath in files:
        with gzip.open(fpath, 'rt', encoding='utf-8') as gz:
            for line in gz:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    path = data.get('path', [])
                    if path and len(path) >= 5 and isinstance(path[0], list) and len(path[0]) >= 4:
                        signals.append(data)
                except Exception:
                    pass

    print(f"Total analyzed signals with valid trajectory: {len(signals)}")
    if not signals:
        return

    # Extract metrics for each signal:
    # 1. Follower Spread
    # 2. Dislocation
    # 3. Pair Sigma
    # 4. MFE within 5s, 10s, 30s
    # 5. MAE (downward adverse move) within 5s, 10s, 30s
    # 6. Rebound as fraction of dislocation
    
    mfes_5s = []
    mfes_15s = []
    mfes_30s = []
    maes_5s = []
    rebound_ratios = []
    spreads = []
    sigmas = []
    dislocs = []

    for d in signals:
        path = d['path']
        spread = d.get('follower_spread_bps', 10.0)
        disloc = d.get('dislocation_bps', d.get('raw_dislocation_bps', 0.0))
        sigma = d.get('pair_sigma_bps', 25.0)

        # path points: [dt, lead_mid_move, f_mid_move, cur_net_pnl, rem_edge]
        # f_mid_move is the follower price movement in bps since trigger
        moves_5s = [p[2] for p in path if p[0] <= 5.0]
        moves_15s = [p[2] for p in path if p[0] <= 15.0]
        moves_30s = [p[2] for p in path if p[0] <= 30.0]

        if not moves_5s: moves_5s = [path[0][2]]
        if not moves_15s: moves_15s = moves_5s
        if not moves_30s: moves_30s = moves_15s

        mfe_5 = max(moves_5s)
        mfe_15 = max(moves_15s)
        mfe_30 = max(moves_30s)
        mae_5 = min(moves_5s)

        mfes_5s.append(mfe_5)
        mfes_15s.append(mfe_15)
        mfes_30s.append(mfe_30)
        maes_5s.append(mae_5)
        spreads.append(spread)
        sigmas.append(sigma)
        dislocs.append(disloc)

        if disloc > 10.0:
            rebound_ratios.append(mfe_15 / disloc)

    def stats(arr, name):
        arr = np.array(arr)
        p25 = np.percentile(arr, 25)
        p50 = np.percentile(arr, 50)
        p75 = np.percentile(arr, 75)
        p90 = np.percentile(arr, 90)
        p95 = np.percentile(arr, 95)
        print(f"{name:<25}: mean={np.mean(arr):+6.2f} | p25={p25:+6.2f} | p50(med)={p50:+6.2f} | p75={p75:+6.2f} | p90={p90:+6.2f} | p95={p95:+6.2f}")

    print("\n--- EMPIRICAL PRICE DISPLACEMENT DISTRIBUTIONS (in bps) ---")
    stats(mfes_5s, "Max Upward Move (5s)")
    stats(mfes_15s, "Max Upward Move (15s)")
    stats(mfes_30s, "Max Upward Move (30s)")
    stats(maes_5s, "Max Downward Wick (5s)")
    stats(spreads, "Follower Spread")
    stats(sigmas, "Pair Sigma")
    if rebound_ratios:
        stats(rebound_ratios, "Rebound / Disloc Ratio")

    # Now let us compute correlation between Rebound (MFE) and:
    # 1. Spread: MFE ~ alpha * Spread
    # 2. Sigma: MFE ~ beta * Sigma
    # 3. Dislocation: MFE ~ gamma * Dislocation
    print("\n--- REGRESSION ANALYSIS: WHAT PREDICTS THE REBOUND? ---")
    X_spread = np.array(spreads)
    X_sigma = np.array(sigmas)
    X_disloc = np.array(dislocs)
    Y_mfe15 = np.array(mfes_15s)

    corr_spread = np.corrcoef(X_spread, Y_mfe15)[0, 1]
    corr_sigma = np.corrcoef(X_sigma, Y_mfe15)[0, 1]
    corr_disloc = np.corrcoef(X_disloc, Y_mfe15)[0, 1]
    print(f"Correlation(MFE_15s, Follower_Spread) = {corr_spread:.3f}")
    print(f"Correlation(MFE_15s, Pair_Sigma)     = {corr_sigma:.3f}")
    print(f"Correlation(MFE_15s, Dislocation)    = {corr_disloc:.3f}")

    # Multiple Linear Regression: MFE = a * Spread + b * Sigma + c
    A = np.vstack([X_spread, X_sigma, np.ones(len(X_spread))]).T
    coeffs, residuals, rank, s = np.linalg.lstsq(A, Y_mfe15, rcond=None)
    a, b, c = coeffs
    print(f"\nFitted Model for Expected Rebound MFE_15s:")
    print(f"  E[Rebound_bps] = {a:.3f} * Spread + {b:.3f} * Sigma + {c:.3f}")

    # Now evaluate P(Rebound >= Target) across various target levels
    print("\n--- PROBABILITY OF REACHING TARGET T (Cumulative Distribution) ---")
    targets = [3.0, 5.0, 6.0, 8.0, 10.0, 12.0, 15.0, 20.0, 25.0, 35.0]
    for t in targets:
        p_5s = np.mean(np.array(mfes_5s) >= t) * 100.0
        p_15s = np.mean(np.array(mfes_15s) >= t) * 100.0
        p_30s = np.mean(np.array(mfes_30s) >= t) * 100.0
        print(f"Target >= {t:4.1f} bps:  Hit in 5s = {p_5s:5.1f}% | Hit in 15s = {p_15s:5.1f}% | Hit in 30s = {p_30s:5.1f}%")

if __name__ == '__main__':
    run()
