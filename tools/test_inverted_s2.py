"""
tools/test_inverted_s2.py — Quantitative Backtest of Inverting Strategy 2 (Short -> Long)
Testing: When Upbit MM cancels quotes (mm_quote_cancel) or micro-sweeps occur,
what is the exact WinRate and PnL if we buy LONG instead of SHORT?
"""
import glob
import gzip
import json
import os
import sys
from collections import defaultdict
import statistics

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TARGET_VENUES = {"binance", "bybit", "mexc", "hyperliquid"}
MAJORS = {"BTC", "ETH", "SOL", "AAVE", "UNI", "INJ", "PENDLE", "JTO", "ENS", "GMT", "AXL", "DATA", "ICP", "ZRO"}
ROUNDTRIP_FEE_BPS = 10.0  # 5 bps open + 5 bps close taker fee

files = sorted(glob.glob("data/signals/**/*.jsonl.gz", recursive=True))

s2_signals = []
for f in files:
    with gzip.open(f, "rt", encoding="utf-8") as gz:
        for line in gz:
            s = json.loads(line)
            if s.get("strategy") == "fade_fakeout":
                s2_signals.append(s)

print(f"Всего загружено сигналов S2 (Fade Fakeout): {len(s2_signals)}")

# Calculate Inverted Long PnL for each signal
# In S2, side was 'short', so:
# entry_px was the price at which the short was opened (selling price).
# exit_px was the price at which the short was closed (buying back).
# For a LONG:
# We BUY at entry_px and SELL at exit_px.
# gross_long_bps = ((exit_px - entry_px) / entry_px) * 10000.0
# net_long_bps = gross_long_bps - ROUNDTRIP_FEE_BPS

for s in s2_signals:
    entry = float(s.get("entry_px", 0) or 0)
    exit_px = float(s.get("exit_px", 0) or 0)
    if entry > 0 and exit_px > 0:
        gross_long = ((exit_px - entry) / entry) * 10000.0
        net_long = gross_long - ROUNDTRIP_FEE_BPS
        s["gross_long_bps"] = gross_long
        s["net_long_bps"] = net_long
    else:
        # Fallback if entry/exit missing
        old_net = float(s.get("net_pnl_bps", 0) or 0)
        # old_net = -gross - fee => gross = -(old_net + fee)
        # long_net = gross - fee = -old_net - 2*fee
        s["gross_long_bps"] = -(old_net + ROUNDTRIP_FEE_BPS)
        s["net_long_bps"] = s["gross_long_bps"] - ROUNDTRIP_FEE_BPS

def rep(subset, label):
    if not subset:
        print(f"  {label:<50} | N=0 | Нет данных")
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

print("\n" + "="*95)
print("             РЕЗУЛЬТАТЫ ИНВЕРСИИ S2 (SHORT -> LONG) ПО ВСЕМ БИРЖАМ")
print("="*95)

rep(s2_signals, "1. ВСЕ СИГНАЛЫ S2 (Инвертированы в LONG)")

# Filter by venues
top4 = [s for s in s2_signals if s.get("follower_exchange", "").lower() in TARGET_VENUES]
rep(top4, "2. ТОЛЬКО 4 ТОП-БИРЖИ (Binance, Bybit, MEXC, Hyperliquid)")

# Exclude heavy majors
top4_alts = [s for s in top4 if s.get("coin", "").upper() not in MAJORS]
rep(top4_alts, "3. ТОП-4 БИРЖИ + ТОЛЬКО АЛЬТКОИНЫ")

print("\n--- РАЗБИВКА ИНВЕРТИРОВАННОГО S2 ПО БИРЖАМ (ВСЕ СИГНАЛЫ) ---")
venue_groups = defaultdict(list)
for s in s2_signals:
    venue_groups[s.get("follower_exchange", "").lower()].append(s)

for ex, sub in sorted(venue_groups.items(), key=lambda x: sum(s["net_long_bps"] for s in x[1]), reverse=True):
    rep(sub, f"Биржа: {ex.upper()}")

print("\n--- ГРАДАЦИЯ ИНВЕРТИРОВАННОГО S2 ПО РАЗМЕРУ ВЫКУПА (НА 4 ТОП-БИРЖАХ, АЛЬТЫ) ---")
for swept_th in [0, 1000, 3000, 5000, 10000, 15000]:
    sub = [s for s in top4_alts if float(s.get("swept_usd", 0) or 0) >= swept_th]
    rep(sub, f"Выкуп >= ${swept_th:<5}")

# Breakdown by reason
print("\n--- РАЗБИВКА ПО ПРИЧИНЕ СИГНАЛА (Fade Reason) НА ТОП-4 БИРЖАХ ---")
reasons = defaultdict(list)
for s in top4_alts:
    reasons[s.get("fade_reason", "unknown")].append(s)

for r, sub in sorted(reasons.items(), key=lambda x: len(x[1]), reverse=True):
    rep(sub, f"Причина: {r}")

# Check coins
print("\n--- ТОП-10 ПРИБЫЛЬНЫХ МОНЕТ В ИНВЕРТИРОВАННОМ S2 (НА ТОП-4 БИРЖАХ) ---")
coin_groups = defaultdict(list)
for s in top4_alts:
    coin_groups[s.get("coin")].append(s)

for c, sub in sorted(coin_groups.items(), key=lambda x: sum(s["net_long_bps"] for s in x[1]), reverse=True)[:10]:
    rep(sub, f"Монета: {c}")

print("\n--- ТОП-5 УБЫТОЧНЫХ МОНЕТ В ИНВЕРТИРОВАННОМ S2 (НА ТОП-4 БИРЖАХ) ---")
for c, sub in sorted(coin_groups.items(), key=lambda x: sum(s["net_long_bps"] for s in x[1]))[:5]:
    rep(sub, f"Монета: {c}")
