"""
Режим MODE=collect: только наблюдение за стаканом, без единого реального
или виртуального ордера. Задача — за короткое время (по умолчанию 60 минут,
см. COLLECT_MINUTES в .env) посмотреть, как реально ходит стакан по ETH и BTC
на Lighter: какие стенки появляются, насколько они крупные, как часто их
"сносит", какой обычно перекос bid/ask.

Пишет:
  logs/collect_<symbol>_<timestamp>.csv   - снапшоты стакана (raw)
  logs/walls_<symbol>_<timestamp>.csv     - все замеченные стенки (для разбора руками)
И печатает сводку в конце.
"""
import asyncio
import csv
import logging
import os
import time
from collections import defaultdict

from config import CFG
from exchange_client import ExchangeClient
from market_data import MarketData

log = logging.getLogger("collector")

os.makedirs("logs", exist_ok=True)


async def run_collector():
    exchange = ExchangeClient()
    markets = await exchange.resolve_markets()
    md = MarketData(exchange, markets)
    await md.start()

    ts_tag = time.strftime("%Y%m%d_%H%M%S")
    snap_files = {}
    wall_files = {}
    snap_writers = {}
    wall_writers = {}
    for sym in markets:
        sf = open(f"logs/collect_{sym}_{ts_tag}.csv", "w", newline="")
        wf = open(f"logs/walls_{sym}_{ts_tag}.csv", "w", newline="")
        snap_files[sym] = sf
        wall_files[sym] = wf
        snap_writers[sym] = csv.writer(sf)
        snap_writers[sym].writerow(["ts", "best_bid", "best_ask", "mid", "imbalance",
                                     "bid_vol_usd", "ask_vol_usd", "n_bid_walls", "n_ask_walls"])
        wall_writers[sym] = csv.writer(wf)
        wall_writers[sym].writerow(["ts", "symbol", "side", "price", "size", "usd", "distance_pct"])

    stats = defaultdict(lambda: {"snapshots": 0, "max_wall_usd": 0.0, "wall_count": 0,
                                  "imbalance_sum": 0.0, "biggest_wall": None})

    end_at = time.time() + CFG.collect_minutes * 60
    log.info("Сбор данных запущен: %s, %.0f минут(ы). Символы: %s",
              time.strftime("%Y-%m-%d %H:%M:%S"), CFG.collect_minutes, ", ".join(markets.keys()))

    try:
        while time.time() < end_at:
            timeout = max(0.1, end_at - time.time())
            try:
                snap = await asyncio.wait_for(md.events.get(), timeout=timeout)
            except asyncio.TimeoutError:
                break

            s = stats[snap.symbol]
            s["snapshots"] += 1
            s["imbalance_sum"] += snap.imbalance

            snap_writers[snap.symbol].writerow([
                f"{snap.ts:.3f}", snap.best_bid, snap.best_ask, f"{snap.mid:.4f}",
                f"{snap.imbalance:.3f}", f"{snap.bid_volume_near:.0f}", f"{snap.ask_volume_near:.0f}",
                len(snap.bid_walls), len(snap.ask_walls),
            ])

            for w in (*snap.bid_walls, *snap.ask_walls):
                wall_writers[snap.symbol].writerow(
                    [f"{w.first_seen:.3f}", snap.symbol, w.side, w.price, w.size, f"{w.usd:.0f}", f"{w.distance_pct:.3f}"]
                )
                s["wall_count"] += 1
                if w.usd > s["max_wall_usd"]:
                    s["max_wall_usd"] = w.usd
                    s["biggest_wall"] = (w.side, w.price, w.usd)

            remaining = end_at - time.time()
            if s["snapshots"] % 200 == 0:
                log.info("[%s] снапшотов=%d, mid=%.2f, imbalance=%.2f, осталось %.0f мин",
                          snap.symbol, s["snapshots"], snap.mid, snap.imbalance, remaining / 60)
    finally:
        await md.stop()
        for sym in markets:
            snap_files[sym].close()
            wall_files[sym].close()
        await exchange.close()

    print("\n=== ИТОГИ СБОРА ДАННЫХ ===")
    for sym, s in stats.items():
        avg_imb = s["imbalance_sum"] / s["snapshots"] if s["snapshots"] else 0
        print(f"\n{sym}:")
        print(f"  снапшотов стакана: {s['snapshots']}")
        print(f"  замечено стенок (>= {CFG.wall_min_usd:.0f} USD): {s['wall_count']}")
        print(f"  средний имбаланс bid/(bid+ask): {avg_imb:.2f}")
        if s["biggest_wall"]:
            side, price, usd = s["biggest_wall"]
            print(f"  крупнейшая стенка: {side} @ {price} на {usd:.0f} USD")
        print(f"  файлы: logs/collect_{sym}_{ts_tag}.csv, logs/walls_{sym}_{ts_tag}.csv")

    print("\nДальше: посмотри CSV глазами (или в Excel/Pandas) — сходится ли частота и размер "
          "стенок с порогами WALL_MIN_USD/IMBALANCE_THRESHOLD в .env. При необходимости поправь "
          "пороги и запусти MODE=paper для проверки логики входов/выходов без риска денег.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(run_collector())
