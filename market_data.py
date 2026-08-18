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
import lighter.ws_client as _lighter_ws_client
import websockets.exceptions

from config import CFG
from exchange_client import MarketInfo

log = logging.getLogger("market_data")

# lighter.WsClient.run_async() вызывает connect_async(url) без параметров,
# из-за чего действуют дефолты библиотеки websockets: ping_interval=20,
# ping_timeout=20 - клиент сам шлёт служебные WS-ping-фреймы (протокольный
# уровень) и обрывает соединение, если за 20с не получил pong. Сервер Lighter
# на такие фреймы, судя по продовым логам, не отвечает - у него свой heartbeat
# на уровне JSON-сообщений ({"type":"ping"} -> клиент отвечает {"type":"pong"},
# см. WsClient.on_message_async, это уже отдельно и корректно обрабатывается).
# В итоге реальное соединение обрывалось каждые ~2 минуты ошибкой "keepalive
# ping timeout" и стакан Lighter почти никогда не оставался живым дольше пары
# минут. Патчим connect_async, чтобы отключить именно протокольный пинг -
# ping_interval=None - и полагаться на JSON-heartbeat, который сервер
# действительно поддерживает.
_orig_lighter_connect_async = _lighter_ws_client.connect_async


def _patched_connect_async(url, **kwargs):
    kwargs.setdefault("ping_interval", None)
    return _orig_lighter_connect_async(url, **kwargs)


_lighter_ws_client.connect_async = _patched_connect_async


@dataclass
class Wall:
    price: float
    size: float
    usd: float
    side: str  # "bid" | "ask"
    distance_pct: float
    # Сколько USD стоит ЗА этой стенкой (глубже в стакане, в сторону от mid),
    # в пределах CFG.wall_backup_range_pct от её цены - т.е. есть ли у стенки
    # "подложка", или она одиночная и за ней почти пусто. Крупная стенка без
    # подложки может не удержать цену, если через неё реально начнут идти
    # (см. signals.py, где это используется как фильтр для ABSORPTION-сигналов).
    backup_usd: float = 0.0
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


def analyze_book(symbol: str, market_id: str, bids: List[tuple], asks: List[tuple],
                  wall_min_usd: float, wall_max_distance_pct: float,
                  wall_backup_range_pct: float = 0.3) -> Optional[BookSnapshot]:
    """
    Общий анализ стакана (стенки + дисбаланс) - не привязан к конкретной бирже.
    bids/asks - списки (price: float, size: float), в любом порядке (сортируем сами).
    Используется и для стакана Lighter (исполнение), и для стакана Binance (сигнал).
    """
    if not bids or not asks:
        return None
    bids = sorted(bids, key=lambda x: -x[0])
    asks = sorted(asks, key=lambda x: x[0])

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2
    if mid <= 0:
        return None

    max_dist = wall_max_distance_pct / 100.0
    backup_range = wall_backup_range_pct / 100.0

    def find_walls(levels, side):
        walls = []
        for idx, (price, size) in enumerate(levels):
            usd = price * size
            dist = abs(price - mid) / mid
            if usd >= wall_min_usd and dist <= max_dist:
                # Подложка за стенкой: levels отсортирован по удалению от mid
                # (bids - по убыванию цены, asks - по возрастанию), поэтому всё,
                # что идёт ПОСЛЕ текущего уровня в списке, глубже в стакане -
                # то есть именно "за" этой стенкой, а не перед ней.
                backup_usd = 0.0
                for p2, s2 in levels[idx + 1:]:
                    if abs(p2 - price) / price > backup_range:
                        break
                    backup_usd += p2 * s2
                walls.append(Wall(price=price, size=size, usd=usd, side=side,
                                   distance_pct=dist * 100, backup_usd=backup_usd))
        return walls

    bid_walls = find_walls(bids, "bid")
    ask_walls = find_walls(asks, "ask")

    near_bids = [price * size for price, size in bids if abs(price - mid) / mid <= max_dist]
    near_asks = [price * size for price, size in asks if abs(price - mid) / mid <= max_dist]
    bid_vol = sum(near_bids)
    ask_vol = sum(near_asks)
    total = bid_vol + ask_vol
    imbalance = (bid_vol / total) if total > 0 else 0.5

    return BookSnapshot(
        market_id=str(market_id),
        symbol=symbol,
        best_bid=best_bid,
        best_ask=best_ask,
        mid=mid,
        bid_walls=bid_walls,
        ask_walls=ask_walls,
        bid_volume_near=bid_vol,
        ask_volume_near=ask_vol,
        imbalance=imbalance,
    )


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
        bids = [(float(o["price"]), float(o["size"])) for o in book.get("bids", [])]
        asks = [(float(o["price"]), float(o["size"])) for o in book.get("asks", [])]
        return analyze_book(market.symbol, str(market.market_index), bids, asks,
                             CFG.wall_min_usd, CFG.wall_max_distance_pct, CFG.wall_backup_range_pct)

    # ------------------------------------------------------------------ #

    async def _diagnose_cloudfront_block(self, ws_url: str):
        """Разовая диагностика: обычным HTTPS GET смотрим, что реально отвечает
        CloudFront на этот адрес - тело JSON-ошибки скажет причину блокировки
        (WAF/geo/IP-репутация и т.д.), которую сам вебсокет-хендшейк не показывает."""
        import aiohttp
        http_url = ws_url.replace("wss://", "https://").replace("ws://", "http://")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(http_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    log.warning("ДИАГНОСТИКА CloudFront: обычный GET на %s -> HTTP %d, тело: %s",
                                http_url, resp.status, text[:1000])
        except Exception as e:
            log.warning("ДИАГНОСТИКА CloudFront: GET-запрос не удался: %s", e)

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
        # Разовая диагностика в фоне: обычный HTTPS GET на тот же URL покажет
        # тело JSON-ошибки CloudFront (сам WS-хендшейк тело не отдаёт).
        asyncio.create_task(self._diagnose_cloudfront_block(ws_url))
        self._task = asyncio.create_task(self._run_with_reconnect())
        return self._task

    async def _run_with_reconnect(self):
        backoff = 1
        consecutive_failures = 0
        while True:
            connected_at = time.time()
            try:
                log.info("Подключение к WS стакана Lighter (%s)...", self.exchange.endpoint.ws_url)
                await self._ws.run_async()
            except asyncio.CancelledError:
                raise
            except (websockets.exceptions.InvalidStatus, websockets.exceptions.InvalidStatusCode) as e:
                consecutive_failures += 1
                # lighter.WsClient использует старое (legacy) API websockets, которое кидает
                # InvalidStatusCode (только status_code + headers, без тела ответа); новое API
                # кидает InvalidStatus (есть ещё и e.response.body) - обрабатываем оба варианта.
                if hasattr(e, "response"):
                    resp = e.response
                    body = resp.body.decode("utf-8", errors="replace")[:500] if resp.body else "(пусто)"
                    headers = dict(resp.headers)
                    log.warning("WS ОТКЛОНЁН сервером: HTTP %d | заголовки=%s | тело=%s",
                                resp.status_code, headers, body)
                else:
                    log.warning("WS ОТКЛОНЁН сервером: HTTP %d | заголовки=%s",
                                e.status_code, dict(e.headers))
                log.warning("Переподключение через %ss (попытка %d)", backoff, consecutive_failures)
                if consecutive_failures >= 5 and not self._outage_notified and self.on_prolonged_outage:
                    self._outage_notified = True
                    asyncio.create_task(self.on_prolonged_outage("ws_disconnected"))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            except Exception as e:
                # Найдено в проде 18.08: сервер Lighter сам обрывает соединение
                # каждые ~3-4 минуты ("received 1000 (OK)...i/o timeout; no close
                # frame sent") - это регулярная, рутинная ротация со стороны
                # сервера/инфраструктуры, а не реальный сбой связи. Проблема была
                # в том, что run_async() у этой библиотеки ВСЕГДА завершается
                # исключением при разрыве (даже при штатном закрытии), а строки
                # сброса backoff=1/consecutive_failures=0 ниже выполняются только
                # если run_async() вернулся БЕЗ исключения - то есть на практике
                # они не выполнялись никогда. В итоге backoff только рос и
                # навсегда упирался в потолок (30s) после первых же 5 разрывов -
                # каждое из ~20+ переподключений за сессию вслепую ждало по 30
                # секунд без реальных проблем со связью, отсюда регулярные "дыры"
                # в цене исполнения Lighter. Если сессия прожила достаточно долго
                # (>60с) перед разрывом - это и есть штатная ротация, а не серия
                # сбоев - сбрасываем backoff, чтобы переподключаться почти сразу.
                session_lifetime = time.time() - connected_at
                if session_lifetime > 60:
                    backoff = 1
                    consecutive_failures = 0
                consecutive_failures += 1
                log.warning("WS соединение оборвалось (%s) после %.0fs на связи, переподключение через %ss (попытка %d)",
                            e, session_lifetime, backoff, consecutive_failures)
                if consecutive_failures >= 5 and not self._outage_notified and self.on_prolonged_outage:
                    self._outage_notified = True
                    asyncio.create_task(self.on_prolonged_outage("ws_disconnected"))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5)
                continue
            backoff = 1
            consecutive_failures = 0
            self._outage_notified = False

    async def stop(self):
        if self._task:
            self._task.cancel()
