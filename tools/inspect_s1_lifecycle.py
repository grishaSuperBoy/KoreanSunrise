import glob, gzip, json
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

    print(f"Total S1 signals: {len(signals)}")
    durations = [s.get('duration_sec', 0.0) for s in signals if s.get('duration_sec') is not None]
    mfes = [s.get('mfe_exec_pnl_bps', 0.0) for s in signals if s.get('mfe_exec_pnl_bps') is not None]
    maes = [s.get('mae_exec_pnl_bps', 0.0) for s in signals if s.get('mae_exec_pnl_bps') is not None]
    gross_edges = [s.get('gross_exec_edge_bps', 0.0) for s in signals if s.get('gross_exec_edge_bps') is not None]
    net_edges = [s.get('net_exec_edge_bps', 0.0) for s in signals if s.get('net_exec_edge_bps') is not None]
    follower_spreads = [s.get('follower_spread_bps', 0.0) for s in signals if s.get('follower_spread_bps') is not None]

    print(f"Duration sec: mean={np.mean(durations):.1f}s, med={np.median(durations):.1f}s, p90={np.percentile(durations, 90):.1f}s")
    print(f"Gross Edge:   mean={np.mean(gross_edges):.1f}b, med={np.median(gross_edges):.1f}b, p90={np.percentile(gross_edges, 90):.1f}b")
    print(f"Net Edge:     mean={np.mean(net_edges):.1f}b, med={np.median(net_edges):.1f}b, p90={np.percentile(net_edges, 90):.1f}b")
    print(f"Follower Spd: mean={np.mean(follower_spreads):.1f}b, med={np.median(follower_spreads):.1f}b, p90={np.percentile(follower_spreads, 90):.1f}b")
    print(f"MFE (peak):   mean={np.mean(mfes):.1f}b, med={np.median(mfes):.1f}b, p90={np.percentile(mfes, 90):.1f}b")
    print(f"MAE (trough): mean={np.mean(maes):.1f}b, med={np.median(maes):.1f}b, p90={np.percentile(maes, 90):.1f}b")

    # How many signals EVER reached cur_net_pnl > 0?
    ever_positive = sum(1 for m in mfes if m > 0.0)
    print(f"\nSignals that EVER reached net profit > 0: {ever_positive} / {len(signals)} ({ever_positive/len(signals)*100:.1f}%)")

    # Path inspection for 3 typical signals
    print("\n--- SAMPLE TRAJECTORIES OF S1 SIGNALS ---")
    for s in signals[:3]:
        print(f"\nID: {s.get('id')} | Coin: {s.get('coin')} | Side: {s.get('side')} | Spread: {s.get('follower_spread_bps')}b | GrossEdge: {s.get('gross_exec_edge_bps')}b | NetEdge: {s.get('net_exec_edge_bps')}b | CloseReason: {s.get('close_reason')}")
        path = s.get('path', [])
        # path: [dt, lead_mid_move, f_mid_move, cur_net_pnl, rem_edge]
        print(f"  Path ({len(path)} points):")
        for p in path[:8]:
            print(f"    t={p[0]:.2f}s | LeadMove={p[1]:+.1f}b | FollMove={p[2]:+.1f}b | NetPnL={p[3]:+.1f}b | RemEdge={p[4]:+.1f}b")

if __name__ == '__main__':
    run()
