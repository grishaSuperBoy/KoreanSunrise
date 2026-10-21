"""
Testing filtered Inverted S2 (Early Long on Quote Pull / Micro-Breakout)
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

s2_signals = []
for f in files:
    with gzip.open(f, "rt", encoding="utf-8") as gz:
        for line in gz:
            s = json.loads(line)
            if s.get("strategy") == "fade_fakeout":
                entry = float(s.get("entry_px", 0) or 0)
                exit_px = float(s.get("exit_px", 0) or 0)
                if entry > 0 and exit_px > 0:
                    gross = ((exit_px - entry) / entry) * 10000.0
                    net = gross - ROUNDTRIP_FEE_BPS
                    s["gross_long_bps"] = gross
                    s["net_long_bps"] = net
                    s2_signals.append(s)

print(f"Total valid S2 signals: {len(s2_signals)}")

# High-frequency quote-jitter majors to exclude
JITTER_MAJORS = {
    "BTC", "ETH", "SOL", "DOT", "FIL", "TAO", "LIT", "RENDER", 
    "AAVE", "UNI", "INJ", "PENDLE", "JTO", "ENS", "GMT", "AXL", "DATA", "ICP", "ZRO"
}

alts_s2 = [s for s in s2_signals if s.get("coin", "").upper() not in JITTER_MAJORS]
print(f"S2 signals on clean Alts (excluding high-frequency jitter coins): {len(alts_s2)}")

def rep(subset, label):
    if not subset:
        print(f"  {label:<50} | N=0")
        return
    n = len(subset)
    wins = sum(1 for s in subset if s["net_long_bps"] > 0)
    total_net = sum(s["net_long_bps"] for s in subset)
    total_gross = sum(s["gross_long_bps"] for s in subset)
    avg_net = total_net / n
    avg_gross = total_gross / n
    wr = wins / n * 100.0
    usd_100 = (total_net / 10000.0) * 100.0
    print(f"  {label:<50} | N={n:>5} | WR(Net>0): {wr:>5.1f}% | Gross: {total_gross:>+8.1f}b (Ср: {avg_gross:>+5.1f}b) | Net: {total_net:>+8.1f}b (Ср: {avg_net:>+5.1f}b) | PnL($100): {usd_100:>+7.2f}$")

print("\n--- РЕЗУЛЬТАТЫ ПО АЛЬТАМ ДЛЯ РАЗНЫХ БИРЖ ---")
for ex in ["asterdex", "htx", "kucoin", "gateio", "binance", "bybit", "mexc"]:
    sub = [s for s in alts_s2 if s.get("follower_exchange", "").lower() == ex]
    rep(sub, f"Биржа: {ex.upper()}")

print("\n--- ГРАДАЦИЯ ПО ВЫКУПУ НА ЧИСТЫХ АЛЬТАХ ---")
for th in [0, 1000, 3000, 5000, 10000]:
    sub = [s for s in alts_s2 if float(s.get("swept_usd", 0) or 0) >= th]
    rep(sub, f"Альты + Swept >= ${th:<5}")

# What if swept >= 3000 on AsterDex, HTX, KuCoin?
sub_super = [s for s in alts_s2 if s.get("follower_exchange", "").lower() in {"asterdex", "htx", "kucoin"} and float(s.get("swept_usd", 0) or 0) >= 3000]
rep(sub_super, "СУПЕР-ПУЛ: AsterDex/HTX/KuCoin + Swept >= $3k")
