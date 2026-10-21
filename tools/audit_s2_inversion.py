import glob
import gzip
import json
import sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

files = sorted(glob.glob("data/signals/**/*.jsonl.gz", recursive=True))

s2_signals = []
for f in files:
    with gzip.open(f, "rt", encoding="utf-8") as gz:
        for line in gz:
            s = json.loads(line)
            if s.get("strategy") == "fade_fakeout":
                s2_signals.append(s)

print(f"Total S2 signals: {len(s2_signals)}")

# Let's inspect the reasons for entry in S2
reasons = defaultdict(int)
for s in s2_signals:
    reasons[s.get("fade_reason", "unknown")] += 1

print("\nS2 Fade Reasons:")
for r, count in sorted(reasons.items(), key=lambda x: x[1], reverse=True):
    print(f"  • {r}: {count}")

# Look at follower price move when S2 triggered
# If side was 'short', entry_px and exit_px:
# price_change_bps = ((exit_px - entry_px) / entry_px) * 10000
# For a short, if price went UP, price_change_bps > 0
# Let's analyze price move across venues!

venue_moves = defaultdict(lambda: {"count": 0, "up_count": 0, "avg_move_bps": 0.0, "moves": []})

for s in s2_signals:
    entry = float(s.get("entry_px", 0) or 0)
    exit_px = float(s.get("exit_px", 0) or 0)
    ex = s.get("follower_exchange", "").lower()
    if entry > 0 and exit_px > 0:
        # What direction did the follower actually move?
        move_bps = ((exit_px - entry) / entry) * 10000.0
        vm = venue_moves[ex]
        vm["count"] += 1
        vm["moves"].append(move_bps)
        if move_bps > 0:
            vm["up_count"] += 1

print("\n--- РЕАЛЬНОЕ ДВИЖЕНИЕ ЦЕНЫ ФОЛЛОВЕРА ПОСЛЕ СИГНАЛА S2 ---")
for ex, vm in sorted(venue_moves.items(), key=lambda x: len(x[1]["moves"]), reverse=True):
    c = vm["count"]
    up_pct = vm["up_count"] / c * 100.0
    import statistics
    med_move = statistics.median(vm["moves"])
    mean_move = statistics.mean(vm["moves"])
    print(f"  Биржа: {ex:<12} | Сигналов: {c:>5} | Движение ВВЕРХ: {up_pct:>5.1f}% | Ср. движение: {mean_move:>+6.1f} bps | Медиана: {med_move:>+6.1f} bps")

# What if we look at Binance and Bybit on S2?
top_venues = {"binance", "bybit", "mexc", "hyperliquid"}
s2_top = [s for s in s2_signals if s.get("follower_exchange", "").lower() in top_venues]
print(f"\nS2 на топ-биржах (Binance, Bybit, MEXC, Hyperliquid): {len(s2_top)} сигналов")

# Top venues move
top_moves = [((float(s.get("exit_px", 0)) - float(s.get("entry_px", 0))) / float(s.get("entry_px", 0))) * 10000.0 for s in s2_top if float(s.get("entry_px", 0)) > 0]
if top_moves:
    up_top = sum(1 for m in top_moves if m > 0)
    print(f"  Движение ВВЕРХ на топ-биржах: {up_top / len(top_moves) * 100:.1f}%")
    print(f"  Средний рост: {statistics.mean(top_moves):+.1f} bps | Медиана: {statistics.median(top_moves):+.1f} bps")
