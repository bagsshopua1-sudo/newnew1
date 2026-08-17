"""
Источник сигнальных данных - публичный стакан Binance Futures (без ключей,
без аккаунта - это открытые market-data эндпоинты, доступные вообще всем).

Причина: у Lighter (и любого молодого перп-DEX) собственный стакан тонкий,
крупные заявки там могут не отражать реальный интерес. Цена Lighter всё
равно следует за Binance через арбитраж - значит настоящие крупные заявки
и реальное давление объёма честнее видно там, где ликвидность глубокая,
то есть на Binance. Сигнальный движок (стены/абсорбция/дисбаланс) теперь
работает по этому стакану, а исполнение (лимитки, 0 комиссий) остаётся
на Lighter - см. bot.py: сигнал берёт направление/тип с Binance, но цену
входа/стопа - с текущего стакана Lighter (basis-проверка перед входом).

Технически - обычный REST-поллинг GET /fapi/v1/depth раз в BINANCE_POLL_INTERVAL_SEC
секунд с лимитом BINANCE_DEPTH_LIMIT уровней, а не WebSocket diff-стрим с
синхронизацией по sequence-номерам (U/u/pu) - тот вариант быстрее (сотни мс),
но сильно сложнее в реализации и не даёт выигрыша для этой задачи: стенки
живут секундами, не миллисекундами (min_wall_age_sec в signals.py = 3с).
"""
import asyncio
import logging
import time
from typing import Dict, Optional

import aiohttp

from config import CFG
from exchange_client import MarketInfo
from market_data import BookSnapshot, analyze_book

log = logging.getLogger("binance_feed")

BASE_URL = "https://fapi.binance.com/fapi/v1/depth"


def to_binance_symbol(symbol: str) -> str:
    return f"{symbol.upper()}USDT"


class BinanceFeed:
    def __init__(self, markets: Dict[str, MarketInfo], on_prolonged_outage=None):
        self.markets = markets  # наш symbol ("ETH") -> MarketInfo (для market_id в снепшоте)
        self.events: "asyncio.Queue[BookSnapshot]" = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self.on_prolonged_outage = on_prolonged_outage
        self._outage_notified = False

    async def start(self):
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._run())
        return self._task

    async def _run(self):
        consecutive_failures = 0
        backoff = CFG.binance_poll_interval_sec
        try:
            while True:
                cycle_start = time.monotonic()
                any_ok = False
                for symbol, market in self.markets.items():
                    try:
                        snap = await self._fetch_one(symbol, market)
                        if snap is not None:
                            any_ok = True
                            try:
                                self.events.put_nowait(snap)
                            except asyncio.QueueFull:
                                pass
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        log.warning("[%s] Binance depth: запрос не удался: %s", symbol, e)

                if any_ok:
                    if consecutive_failures:
                        log.info("Binance depth: связь восстановлена")
                    consecutive_failures = 0
                    self._outage_notified = False
                    backoff = CFG.binance_poll_interval_sec
                else:
                    consecutive_failures += 1
                    log.warning("Binance depth: ни один символ не получен (попытка %d)", consecutive_failures)
                    if consecutive_failures >= 5 and not self._outage_notified and self.on_prolonged_outage:
                        self._outage_notified = True
                        asyncio.create_task(self.on_prolonged_outage("binance_feed_disconnected"))
                    backoff = min(backoff * 1.5, 30)

                elapsed = time.monotonic() - cycle_start
                await asyncio.sleep(max(0.0, backoff - elapsed) if consecutive_failures else
                                     max(0.0, CFG.binance_poll_interval_sec - elapsed))
        except asyncio.CancelledError:
            pass
        finally:
            if self._session:
                await self._session.close()

    async def _fetch_one(self, symbol: str, market: MarketInfo) -> Optional[BookSnapshot]:
        params = {"symbol": to_binance_symbol(symbol), "limit": CFG.binance_depth_limit}
        async with self._session.get(BASE_URL, params=params,
                                      timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
            data = await resp.json()

        raw_bids = data.get("bids", [])
        raw_asks = data.get("asks", [])
        bids = [(float(p), float(q)) for p, q in raw_bids]
        asks = [(float(p), float(q)) for p, q in raw_asks]

        return analyze_book(symbol, market.market_index, bids, asks,
                             CFG.binance_wall_min_usd, CFG.wall_max_distance_pct)

    async def stop(self):
        if self._task:
            self._task.cancel()
