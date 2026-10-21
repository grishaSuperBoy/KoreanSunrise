import glob
import gzip
import json
import sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

files = sorted(glob.glob("data/signals/**/*.jsonl.gz", recursive=True))

s3_signals = []
for f in files:
    with gzip.open(f, "rt", encoding="utf-8") as gz:
        for line in gz:
            s = json.loads(line)
            if s.get("strategy") == "maker_void":
                s3_signals.append(s)

print(f"Total S3 Maker signals: {len(s3_signals)}")

# Filled vs unfulfilled
filled = [s for s in s3_signals if s.get("maker_filled", False)]
print(f"Filled: {len(filled)} ({len(filled)/len(s3_signals)*100:.1f}%)")

venue_filled = defaultdict(lambda: {"count": 0, "wins": 0, "net": 0.0, "sides": defaultdict(int)})
for s in filled:
    ex = s.get("follower_exchange", "").lower()
    net = float(s.get("net_pnl_bps", 0) or 0)
    side = s.get("side", "")
    vf = venue_filled[ex]
    vf["count"] += 1
    vf["net"] += net
    vf["sides"][side] += 1
    if net > 0:
        vf["wins"] += 1

print("\n--- S3 FILLED TRADES ПО БИРЖАМ ---")
for ex, vf in sorted(venue_filled.items(), key=lambda x: x[1]["net"]):
    c = vf["count"]
    wr = vf["wins"] / c * 100
    avg = vf["net"] / c
    print(f"  Биржа: {ex:<12} | Filled: {c:>3} | WinRate: {wr:>5.1f}% | Net: {vf['net']:>+7.1f} bps (Ср: {avg:>+5.1f}b) | Sides: {dict(vf['sides'])}")
