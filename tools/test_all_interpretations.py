import glob
import gzip
import json
import sys
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TARGET_VENUES = {"binance", "bybit", "mexc", "hyperliquid"}

for day, title in [("2026-10-20", "2026-10-20 (ВЧЕРА)"), ("2026-10-21", "2026-10-21 (СЕГОДНЯ)"), (None, "ОБА ДНЯ ВМЕСТЕ (20-21 ОКТЯБРЯ)")]:
    pattern = f"data/signals/{day}/*.jsonl.gz" if day else "data/signals/**/*.jsonl.gz"
    files = sorted(glob.glob(pattern, recursive=True))
    
    signals = []
    for f in files:
        with gzip.open(f, "rt", encoding="utf-8") as gz:
            for line in gz:
                s = json.loads(line)
                if s.get("strategy") == "lead_lag":
                    if s.get("follower_exchange", "").lower() in TARGET_VENUES:
                        signals.append(s)

    print(f"\n{'='*80}")
    print(f"=== {title}: Сделок на 4 биржах: {len(signals)} ===")
    print(f"{'='*80}")

    def rep(name, subset):
        if not subset:
            print(f"  {name:<55} | N=0")
            return
        n = len(subset)
        wins = sum(1 for s in subset if float(s.get("net_pnl_bps", 0.0) or 0.0) > 0)
        tp = sum(1 for s in subset if s.get("close_reason") == "converged")
        net = sum(float(s.get("net_pnl_bps", 0.0) or 0.0) for s in subset)
        gross = sum(float(s.get("gross_exec_pnl_bps", 0.0) or s.get("net_pnl_bps", 0.0) + 10.0) for s in subset)
        coins = len(set(s.get("coin") for s in subset))
        print(f"  {name:<55} | N={n:>3} | Монет: {coins:>2} | WR: {wins/n*100:>5.1f}% | TP: {tp/n*100:>5.1f}% | Net: {net:>+7.1f} bps (Ср: {net/n:>+5.1f}b)")

    rep("1. Без фильтров (все сделки на 4 биржах)", signals)
    
    # 2. follower spread < 70 bps (0.7%)
    sp70 = [s for s in signals if float(s.get("follower_spread_bps", 999) or 999) < 70.0]
    rep("2. Спред фолловера < 70 bps (0.7%)", sp70)

    # 3. follower spread < 15 bps (0.15%)
    sp15 = [s for s in signals if float(s.get("follower_spread_bps", 999) or 999) < 15.0]
    rep("3. Спред фолловера < 15 bps (0.15%)", sp15)

    # 4. Follower стакан (top3_ask) >= 15000 & spread < 70 bps
    f_depth15 = [s for s in sp70 if float(s.get("follower_top3_ask_usd", 0) or 0) >= 15000.0]
    rep("4. Стакан фолловера >= $15,000 & спред < 0.7%", f_depth15)

    # 5. Leader стакан (top5_ask) >= 15000 & spread < 70 bps
    l_depth15 = [s for s in sp70 if float(s.get("leader_top5_ask_usd", 0) or 0) >= 15000.0]
    rep("5. Стакан лидера (Upbit/Bithumb) >= $15,000 & спред < 0.7%", l_depth15)

    # 6. Leader swept (объем выкупа) >= 15000 & spread < 70 bps
    l_swept15 = [s for s in sp70 if float(s.get("leader_swept_usd", 0) or 0) >= 15000.0]
    rep("6. Объем выкупа (Leader Swept) >= $15,000 & спред < 0.7%", l_swept15)

    # 7. Leader swept >= 3000 & spread < 15 bps
    l_swept3 = [s for s in sp15 if float(s.get("leader_swept_usd", 0) or 0) >= 3000.0]
    rep("7. Как считали в filter_slag: Swept >= $3k & спред <= 15 bps", l_swept3)

    # 8. Leader swept >= 10000 & spread < 15 bps
    l_swept10 = [s for s in sp15 if float(s.get("leader_swept_usd", 0) or 0) >= 10000.0]
    rep("8. Как считали в filter_slag: Swept >= $10k & спред <= 15 bps", l_swept10)
