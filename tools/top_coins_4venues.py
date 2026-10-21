import glob
import gzip
import json
import sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TARGET_VENUES = {"binance", "bybit", "mexc", "hyperliquid"}

files = sorted(glob.glob("data/signals/**/*.jsonl.gz", recursive=True))
coin_stats = defaultdict(lambda: {"count": 0, "wins": 0, "tp": 0, "net": 0.0, "gross": 0.0, "venues": set()})

for f in files:
    with gzip.open(f, "rt", encoding="utf-8") as gz:
        for line in gz:
            s = json.loads(line)
            if s.get("strategy") == "lead_lag":
                ex = s.get("follower_exchange", "").lower()
                if ex in TARGET_VENUES:
                    c = s.get("coin")
                    net = float(s.get("net_pnl_bps", 0.0) or 0.0)
                    gross = float(s.get("gross_exec_pnl_bps", 0.0) or net + 10.0)
                    cs = coin_stats[c]
                    cs["count"] += 1
                    cs["net"] += net
                    cs["gross"] += gross
                    cs["venues"].add(ex)
                    if net > 0:
                        cs["wins"] += 1
                    if s.get("close_reason") == "converged":
                        cs["tp"] += 1

print(f"Всего уникальных монет на 4 топ-биржах: {len(coin_stats)}")
print("\n=== ТОП ПРИБЫЛЬНЫХ МОНЕТ НА 4 БИРЖАХ (Binance, Bybit, MEXC, Hyperliquid) ===")
pos = sorted([c for c in coin_stats.items() if c[1]["net"] > 0], key=lambda x: x[1]["net"], reverse=True)
for c, d in pos:
    wr = d["wins"] / d["count"] * 100
    tp = d["tp"] / d["count"] * 100
    avg = d["net"] / d["count"]
    v_str = ",".join(sorted(d["venues"]))
    print(f"  • {c:<10} | Сделок: {d['count']:>2} | WR: {wr:>5.1f}% | TP: {tp:>5.1f}% | Net: {d['net']:>+7.1f} bps (Ср: {avg:>+5.1f}b) | Биржи: {v_str}")

print("\n=== ТОП УБЫТОЧНЫХ МОНЕТ НА 4 БИРЖАХ (Binance, Bybit, MEXC, Hyperliquid) ===")
neg = sorted([c for c in coin_stats.items() if c[1]["net"] < 0], key=lambda x: x[1]["net"])
for c, d in neg:
    wr = d["wins"] / d["count"] * 100
    tp = d["tp"] / d["count"] * 100
    avg = d["net"] / d["count"]
    v_str = ",".join(sorted(d["venues"]))
    print(f"  • {c:<10} | Сделок: {d['count']:>2} | WR: {wr:>5.1f}% | TP: {tp:>5.1f}% | Net: {d['net']:>+7.1f} bps (Ср: {avg:>+5.1f}b) | Биржи: {v_str}")
