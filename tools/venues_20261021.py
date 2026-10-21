import glob
import gzip
import json
import sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

files = sorted(glob.glob("data/signals/2026-10-21/*.jsonl.gz"))
venues = defaultdict(lambda: {"count": 0, "wins": 0, "tp": 0, "net": 0.0, "gross": 0.0})

for f in files:
    with gzip.open(f, "rt", encoding="utf-8") as gz:
        for line in gz:
            s = json.loads(line)
            if s.get("strategy") == "lead_lag":
                ex = s.get("follower_exchange", "").lower()
                net = float(s.get("net_pnl_bps", 0.0) or 0.0)
                gross = float(s.get("gross_exec_pnl_bps", 0.0) or net + 10.0)
                v = venues[ex]
                v["count"] += 1
                v["net"] += net
                v["gross"] += gross
                if net > 0:
                    v["wins"] += 1
                if s.get("close_reason") == "converged":
                    v["tp"] += 1

print("=== 2026-10-21 (СЕГОДНЯ): S1 LEAD-LAG ПО ВСЕМ БИРЖАМ ===")
for ex, v in sorted(venues.items(), key=lambda x: x[1]["net"], reverse=True):
    wr = v["wins"] / v["count"] * 100
    tp = v["tp"] / v["count"] * 100
    avg = v["net"] / v["count"]
    print(f"Биржа: {ex:<12} | Сделок: {v['count']:>3} | WinRate: {wr:>5.1f}% | TP: {tp:>5.1f}% | Net: {v['net']:>+7.1f} bps (Ср: {avg:>+5.1f}b)")

# Top 4 venues sum
top4 = ["binance", "bybit", "mexc", "hyperliquid"]
top4_v = [v for ex, v in venues.items() if ex in top4]
t4_count = sum(v["count"] for ex, v in venues.items() if ex in top4)
t4_wins = sum(v["wins"] for ex, v in venues.items() if ex in top4)
t4_tp = sum(v["tp"] for ex, v in venues.items() if ex in top4)
t4_net = sum(v["net"] for ex, v in venues.items() if ex in top4)
t4_avg = t4_net / t4_count if t4_count else 0
print(f"\nИТОГО ТОП-4 (Binance, Bybit, MEXC, Hyperliquid) ЗА 2026-10-21:")
print(f"Сделок: {t4_count} | WinRate: {t4_wins/t4_count*100:5.1f}% | TP: {t4_tp/t4_count*100:5.1f}% | Net: {t4_net:+7.1f} bps (Ср: {t4_avg:+5.1f}b)")
