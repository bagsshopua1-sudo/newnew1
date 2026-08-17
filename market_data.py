"""
Слой рыночных данных: подписка на стакан (order book) Lighter через WebSocket,
хранение текущего состояния по каждому рынку, расчёт крупных лимиток ("стенок")
и дисбаланса объёма bid/ask. Публикует события в asyncio.Queue для сигнального движка.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import lighter
import websockets.exceptions

from config import CFG
from exchange_client import MarketInfo

log = logging.getLogger("market_data")


@dataclass
class Wall:
    price: float
    size: float
    usd: float
    side: str  # "bid" | "ask"
    distance_pct: float
    first_seen: float = field(default_factory=time.time)


@dataclass
class BookSnapshot:
    market_id: str
    symbol: str
    best_bid: float
    best_ask: float
    mid: float
    bid_walls: List[Wall]
    ask_walls: List[Wall]
    bid_volume_near: float
    ask_volume_near: float
    imbalance: float  # 0..1, доля бидов в объёме bid+ask вблизи мида
    ts: float = field(default_factory=time.time)


class MarketData:
    def __init__(self, exchange, markets: Dict[str, MarketInfo], on_prolonged_outage=None):
        self.exchange = exchange
        self.markets = markets  # symbol -> MarketInfo
        self.by_market_id = {str(m.market_index): m for m in markets.values()}
        self.events: "asyncio.Queue[BookSnapshot]" = asyncio.Queue()
        self._ws: Optional[lighter.WsClient] = None
        self._task: Optional[asyncio.Task] = None
        # для детекции "стенка отодвигается" (спуфинг)
        self._prev_walls: Dict[str, Dict[float, Wall]] = {}
        # колбэк на затяжной обрыв связи (несколько неудачных переподключений подряд)
        self.on_prolonged_outage = on_prolonged_outage
        self._outage_notified = False

    # ------------------------------------------------------------------ #

    def _on_update(self, market_id: str, book: dict):
        market = self.by_market_id.get(str(market_id))
        if market is None:
            return
        snap = self._analyze(market, book)
        if snap is not None:
            try:
                self.events.put_nowait(snap)
            except asyncio.QueueFull:
                pass

    def _analyze(self, market: MarketInfo, book: dict) -> Optional[BookSnapshot]:
        bids = sorted(book.get("bids", []), key=lambda o: -float(o["price"]))
        asks = sorted(book.get("asks", []), key=lambda o: float(o["price"]))
        if not bids or not asks:
            return None

        best_bid = float(bids[0]["price"])
        best_ask = float(asks[0]["price"])
        mid = (best_bid + best_ask) / 2

        max_dist = CFG.wall_max_distance_pct / 100.0

        def find_walls(levels, side):
            walls = []
            for lvl in levels:
                price = float(lvl["price"])
                size = float(lvl["size"])
                usd = price * size
                dist = abs(price - mid) / mid
                if usd >= CFG.wall_min_usd and dist <= max_dist:
                    walls.append(Wall(price=price, size=size, usd=usd, side=side, distance_pct=dist * 100))
            return walls

        bid_walls = find_walls(bids, "bid")
        ask_walls = find_walls(asks, "ask")

        # объём вблизи мида (в пределах той же дистанции) для расчёта дисбаланса
        near_bids = [float(o["size"]) * float(o["price"]) for o in bids
                     if abs(float(o["price"]) - mid) / mid <= max_dist]
        near_asks = [float(o["size"]) * float(o["price"]) for o in asks
                     if abs(float(o["price"]) - mid) / mid <= max_dist]
        bid_vol = sum(near_bids)
        ask_vol = sum(near_asks)
        total = bid_vol + ask_vol
        imbalance = (bid_vol / total) if total > 0 else 0.5

        return BookSnapshot(
            market_id=str(market.market_index),
            symbol=market.symbol,
            best_bid=best_bid,
            best_ask=best_ask,
            mid=mid,
            bid_walls=bid_walls,
            ask_walls=ask_walls,
            bid_volume_near=bid_vol,
            ask_volume_near=ask_vol,
            imbalance=imbalance,
        )

    # ------------------------------------------------------------------ #

    async def start(self):
        market_ids = [m.market_index for m in self.markets.values()]
        # У Lighter WS-эндпоинт требует явного параметра encoding=json в query-строке -
        # без него сервер отклоняет handshake (HTTP 400). Тот же приём использует
        # встроенный lighter.PaperClient при подключении к стакану.
        raw_ws_url = self.exchange.endpoint.ws_url
        sep = "&" if "?" in raw_ws_url else "?"
        ws_url = f"{raw_ws_url}{sep}encoding=json"
        self._ws = lighter.WsClient(
            ws_url=ws_url,
            order_book_ids=market_ids,
            account_ids=[],
            on_order_book_update=self._on_update,
        )
        self._task = asyncio.create_task(self._run_with_reconnect())
        return self._task

    async def _run_with_reconnect(self):
        backoff = 1
        consecutive_failures = 0
        while True:
            try:
                log.info("Подключение к WS стакана Lighter (%s)...", self.exchange.endpoint.ws_url)
                await self._ws.run_async()
            except asyncio.CancelledError:
                raise
            except websockets.exceptions.InvalidStatus as e:
                consecutive_failures += 1
                resp = e.response
                body = resp.body.decode("utf-8", errors="replace")[:500] if resp.body else "(пусто)"
                headers = dict(resp.headers)
                log.warning("WS ОТКЛОНЁН сервером: HTTP %d %s | заголовки=%s | тело=%s",
                            resp.status_code, resp.reason_phrase, headers, body)
                log.warning("Переподключение через %ss (попытка %d)", backoff, consecutive_failures)
                if consecutive_failures >= 5 and not self._outage_notified and self.on_prolonged_outage:
                    self._outage_notified = True
                    asyncio.create_task(self.on_prolonged_outage("ws_disconnected"))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            except Exception as e:
                consecutive_failures += 1
                log.warning("WS соединение оборвалось (%s), переподключение через %ss (попытка %d)",
                            e, backoff, consecutive_failures)
                if consecutive_failures >= 5 and not self._outage_notified and self.on_prolonged_outage:
                    self._outage_notified = True
                    asyncio.create_task(self.on_prolonged_outage("ws_disconnected"))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            backoff = 1
            consecutive_failures = 0
            self._outage_notified = False

    async def stop(self):
        if self._task:
            self._task.cancel()
