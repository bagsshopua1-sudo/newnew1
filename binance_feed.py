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

Технически - WebSocket "partial book depth" стрим (<symbol>@depth<N>@<speed>),
а не REST-поллинг. Причина смены: обычный REST GET /fapi/v1/depth на IP
Render довольно быстро словил бан от Binance (HTTP 418 "Way too many
requests" - вероятно, IP общий с другими клиентами Render и лимит
исчерпывается не только нашими запросами). WS market-data стримы не
тарифицируются по этому же лимиту и как раз для этого предназначены -
сообщение самого Binance в теле 418-ошибки: "Please use the websocket
for live updates to avoid bans". Partial-depth стрим - НЕ diff-поток:
каждое сообщение уже готовый топ-N снепшот, синхронизация по
sequence-номерам (U/u/pu) не нужна.
"""
import asyncio
import json
import logging
import time
from typing import Dict, Optional

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # pragma: no cover - совместимость со старым websockets<13
    from websockets.client import connect as ws_connect

from config import CFG
from exchange_client import MarketInfo
from market_data import BookSnapshot, analyze_book

log = logging.getLogger("binance_feed")

WS_BASE = "wss://fstream.binance.com/stream"


def to_stream_name(symbol: str) -> str:
    return f"{symbol.lower()}usdt@depth{CFG.binance_ws_depth_levels}@{CFG.binance_ws_speed_ms}ms"


class BinanceFeed:
    def __init__(self, markets: Dict[str, MarketInfo], on_prolonged_outage=None):
        self.markets = markets  # наш symbol ("ETH") -> MarketInfo
        self.stream_to_symbol = {to_stream_name(sym): sym for sym in markets}
        self.events: "asyncio.Queue[BookSnapshot]" = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self.on_prolonged_outage = on_prolonged_outage
        self._outage_notified = False
        self._warned_bad_format = False

    async def start(self):
        self._task = asyncio.create_task(self._run_with_reconnect())
        return self._task

    def _url(self) -> str:
        streams = "/".join(self.stream_to_symbol.keys())
        return f"{WS_BASE}?streams={streams}"

    async def _run_with_reconnect(self):
        backoff = 1
        consecutive_failures = 0
        url = self._url()
        while True:
            try:
                log.info("Подключение к WS стакана Binance (%s)...", url)
                async with ws_connect(url) as ws:
                    backoff = 1
                    consecutive_failures = 0
                    self._outage_notified = False
                    async for raw in ws:
                        self._handle_message(raw)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                consecutive_failures += 1
                log.warning("Binance WS оборвался (%s), переподключение через %ss (попытка %d)",
                            e, backoff, consecutive_failures)
                if consecutive_failures >= 5 and not self._outage_notified and self.on_prolonged_outage:
                    self._outage_notified = True
                    asyncio.create_task(self.on_prolonged_outage("binance_feed_disconnected"))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _handle_message(self, raw):
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        stream = msg.get("stream")
        data = msg.get("data") or msg  # на всякий случай - вдруг придёт не в конверте combined-stream
        symbol = self.stream_to_symbol.get(stream)
        if symbol is None:
            return
        market = self.markets[symbol]

        # Формат полей документирован для diff-потока Futures ("b"/"a"); у
        # partial-depth потока предполагается тот же конверт, но на случай
        # расхождения пробуем и альтернативные имена ("bids"/"asks", как у Spot).
        raw_bids = data.get("b") or data.get("bids")
        raw_asks = data.get("a") or data.get("asks")
        if not raw_bids or not raw_asks:
            if not self._warned_bad_format:
                self._warned_bad_format = True
                log.warning("Binance WS: не нашёл bids/asks в сообщении, ключи=%s, сырое=%s",
                            list(data.keys()), str(data)[:500])
            return

        try:
            bids = [(float(p), float(q)) for p, q in raw_bids]
            asks = [(float(p), float(q)) for p, q in raw_asks]
        except (TypeError, ValueError) as e:
            log.warning("Binance WS: не смог распарсить bids/asks (%s): %s", e, str(data)[:300])
            return

        snap = analyze_book(symbol, market.market_index, bids, asks,
                             CFG.binance_wall_min_usd, CFG.wall_max_distance_pct, CFG.wall_backup_range_pct)
        if snap is not None:
            try:
                self.events.put_nowait(snap)
            except asyncio.QueueFull:
                pass

    async def stop(self):
        if self._task:
            self._task.cancel()
