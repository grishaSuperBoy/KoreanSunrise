import glob
import gzip
import json
import sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TARGET_VENUES = {"binance", "bybit", "mexc", "hyperliquid"}

files = sorted(glob.glob("data/signals/**/*.jsonl.gz", recursive=True))
signals = []
for f in files:
    with gzip.open(f, "rt", encoding="utf-8") as gz:
        for line in gz:
            s = json.loads(line)
            if s.get("strategy") == "lead_lag":
                if s.get("follower_exchange", "").lower() in TARGET_VENUES:
                    signals.append(s)

print(f"Total S1 trades on 4 venues: {len(signals)}")

# Categorize coins:
# Well-known large caps / DeFi blue chips vs Korean speculative alts
MAJORS = {"BTC", "ETH", "SOL", "AAVE", "UNI", "INJ", "PENDLE", "JTO", "ENS", "GMT", "AXL", "DATA", "ICP", "ZRO"}

majors_trades = [s for s in signals if s.get("coin") in MAJORS]
alts_trades = [s for s in signals if s.get("coin") not in MAJORS]

def stats(subset, name):
    if not subset:
        print(f"  {name:<50} | N=0")
        return
    n = len(subset)
    wins = sum(1 for s in subset if float(s.get("net_pnl_bps", 0.0) or 0.0) > 0)
    tp = sum(1 for s in subset if s.get("close_reason") == "converged")
    net = sum(float(s.get("net_pnl_bps", 0.0) or 0.0) for s in subset)
    gross = sum(float(s.get("gross_exec_pnl_bps", 0.0) or s.get("net_pnl_bps", 0.0) + 10.0) for s in subset)
    coins = len(set(s.get("coin") for s in subset))
    print(f"  {name:<50} | N={n:>3} | Монет: {coins:>2} | WR: {wins/n*100:>5.1f}% | TP: {tp/n*100:>5.1f}% | Net: {net:>+7.1f} bps (Ср: {net/n:>+5.1f}b)")

print("\n--- СРАВНЕНИЕ: ТЯЖЕЛЫЕ МАЖОРЫ VS КОРЕЙСКИЕ АЛЬТКОИНЫ ---")
stats(majors_trades, "1. 'Серьезные' монеты / Мажоры (AAVE, UNI, INJ, JTO...)")
stats(alts_trades, "2. Корейские Альты / Щиткоины (DOOD, BIRB, CAP, WCT...)")

print("\n--- ГРАДАЦИЯ ПО РАЗМЕРУ ВЫКУПА (LEADER SWEPT USD) ---")
swept_micro = [s for s in signals if float(s.get("leader_swept_usd", 0) or 0) < 3000.0]
swept_mid = [s for s in signals if 3000.0 <= float(s.get("leader_swept_usd", 0) or 0) < 10000.0]
swept_whale = [s for s in signals if float(s.get("leader_swept_usd", 0) or 0) >= 10000.0]

stats(swept_micro, "A. Микро-шум: Выкуп < $3,000")
stats(swept_mid, "B. Средний выкуп: $3,000 - $10,000")
stats(swept_whale, "C. Серьезный выкуп (КИТЫ): >= $10,000")

print("\n--- ДЕТАЛЬНО: АЛЬТЫ С РАЗНЫМ РАЗМЕРОМ ВЫКУПА ---")
alts_micro = [s for s in alts_trades if float(s.get("leader_swept_usd", 0) or 0) < 3000.0]
alts_mid = [s for s in alts_trades if 3000.0 <= float(s.get("leader_swept_usd", 0) or 0) < 10000.0]
alts_whale = [s for s in alts_trades if float(s.get("leader_swept_usd", 0) or 0) >= 10000.0]

stats(alts_micro, "Альты + Выкуп < $3,000")
stats(alts_mid, "Альты + Выкуп $3,000 - $10,000")
stats(alts_whale, "Альты + Выкуп >= $10,000 (КИТЫ НА АЛЬТАХ)")

print("\n--- ДЕТАЛЬНО: МАЖОРЫ С РАЗНЫМ РАЗМЕРОМ ВЫКУПА ---")
majors_micro = [s for s in majors_trades if float(s.get("leader_swept_usd", 0) or 0) < 3000.0]
majors_mid = [s for s in majors_trades if 3000.0 <= float(s.get("leader_swept_usd", 0) or 0) < 10000.0]
majors_whale = [s for s in majors_trades if float(s.get("leader_swept_usd", 0) or 0) >= 10000.0]

stats(majors_micro, "Мажоры + Выкуп < $3,000")
stats(majors_mid, "Мажоры + Выкуп $3,000 - $10,000")
stats(majors_whale, "Мажоры + Выкуп >= $10,000")
