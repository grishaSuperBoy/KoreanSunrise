"""
Testing Inverted Strategy 3: Taker Front-Runner / Toxic Flow Following
"""
import glob
import gzip
import json
import sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROUNDTRIP_FEE_BPS = 10.0

files = sorted(glob.glob("data/signals/**/*.jsonl.gz", recursive=True))

s3_filled = []
for f in files:
    with gzip.open(f, "rt", encoding="utf-8") as gz:
        for line in gz:
            s = json.loads(line)
            if s.get("strategy") == "maker_void" and s.get("maker_filled", False):
                s3_filled.append(s)

print(f"Total S3 filled trades: {len(s3_filled)}")

# In S3, when our limit order was an "ask" (we sold):
# We sold at target_limit_px, and then the market exploded up against us!
# If we were a TAKER in the direction of the blowout:
# When side == 'ask' (blown-out ask liquidity being devoured by a buyer):
# The buyer made a killing!
# Let's calculate the gross move that destroyed our maker order:
for s in s3_filled:
    side = s.get("side", "")
    old_net = float(s.get("net_pnl_bps", 0) or 0)
    rebate = float(s.get("maker_rebate_bps", 1.5))
    taker_fee = float(s.get("taker_fee_bps", 5.0))
    # old_net = move - taker_fee + rebate (if loss, move was negative for maker)
    # The adverse price movement against maker was:
    loss_bps = -(old_net - rebate + taker_fee)
    s["toxic_flow_bps"] = loss_bps

print("\n--- СКОЛЬКО ЗАРАБОТАЛ ТОКСИЧНЫЙ ТЕЙКЕР, КОТОРЫЙ СЖИРАЛ НАШИ ОРДЕРА В S3 ---")
venue_toxic = defaultdict(list)
for s in s3_filled:
    venue_toxic[s.get("follower_exchange", "").lower()].append(s)

for ex, trades in sorted(venue_toxic.items(), key=lambda x: len(x[1]), reverse=True):
    n = len(trades)
    moves = [s["toxic_flow_bps"] for s in trades]
    total_flow = sum(moves)
    avg_flow = total_flow / n
    # If a taker took this position and paid 10 bps fee:
    net_taker = sum(m - ROUNDTRIP_FEE_BPS for m in moves)
    wr_taker = sum(1 for m in moves if (m - ROUNDTRIP_FEE_BPS) > 0) / n * 100.0
    print(f"  Биржа: {ex:<12} | Сделок: {n:>4} | Taker WinRate: {wr_taker:>5.1f}% | Ср. вынос: {avg_flow:>+6.1f} bps | Net Taker: {net_taker:>+9.1f} bps")
