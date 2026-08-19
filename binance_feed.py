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

ВАЖНО (19.08, второй раунд диагностики "ни одной сделки не открылось"):
раньше поток исполненных сделок (aggTrade, для executed_buy/executed_sell в
классификации SPOOF/ABSORPTION/BREAKOUT) жил в binance_trades.py и открывал
СВОЁ ОТДЕЛЬНОЕ WS-соединение к тому же fstream.binance.com. Эмпирически (по
логам Render) выяснилось: то соединение стабильно проходило handshake, но
после этого НИ РАЗУ не получало ни одного сообщения - при этом ЭТОТ поток
(стакана), подключающийся ПЕРВЫМ, продолжал получать данные без сбоев. Дело
оказалось не в имени стрима (проверяли и "aggTrade", и "aggtrade" - не
помогло), а в том, что это было ВТОРОЕ одновременное соединение с одного и
того же (уже раз забаненного по HTTP 418, то есть помеченного Binance) IP
Render - похоже, для такого IP Binance пускает второе соединение, но реально
не шлёт по нему данные. Поэтому теперь ОБА стрима (депт + aggTrade) идут
через ОДНО общее WS-соединение, объявленное здесь - это и устраняет саму
причину (нет второго соединения), и по построению не может размножить
проблему бана лимитов сильнее, чем раньше.
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


def to_depth_stream_name(symbol: str) -> str:
    return f"{symbol.lower()}usdt@depth{CFG.binance_ws_depth_levels}@{CFG.binance_ws_speed_ms}ms"


def to_trade_stream_name(symbol: str) -> str:
    # Регистр важен на практике (см. binance_trades.py, история диагностики
    # 19.08) - используем нижний регистр, как и у depth-стрима выше.
    return f"{symbol.lower()}usdt@aggtrade"


class BinanceFeed:
    def __init__(self, markets: Dict[str, MarketInfo], on_prolonged_outage=None, trade_feed=None):
        self.markets = markets  # наш symbol ("ETH") -> MarketInfo
        depth_map = {to_depth_stream_name(sym): sym for sym in markets}
        trade_map = {to_trade_stream_name(sym): sym for sym in markets}
        self.stream_to_symbol = {**depth_map, **trade_map}
        # Резервный индекс по полю "s" самого payload'а (например "BTCUSDT",
        # Binance всегда отдаёт его в верхнем регистре и в depth-, и в
        # trade-сообщениях) - на случай расхождения регистра/формата "stream".
        self._binance_symbol_to_symbol = {f"{sym.upper()}USDT": sym for sym in markets}
        # trade_feed: BinanceTradeFeed (см. binance_trades.py) - пассивное
        # хранилище тейпа сделок, которое наполняет ЭТОТ класс (единое
        # соединение, см. модульный docstring выше). None - если поток
        # исполненного объёма не нужен (например MODE=collect).
        self.trade_feed = trade_feed
        self.events: "asyncio.Queue[BookSnapshot]" = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self.on_prolonged_outage = on_prolonged_outage
        self._outage_notified = False
        self._warned_bad_format = False
        self._unmatched_count = 0
        self._last_unmatched_warn = 0.0

    async def start(self):
        self._task = asyncio.create_task(self._run_with_reconnect())
        return self._task

    def _url(self) -> str:
        streams = "/".join(self.stream_to_symbol.keys())
        return f"{WS_BASE}?streams={streams}"

    async def _run_with_reconnect(self):
        # См. модульный docstring - читаем сообщения вручную через recv() с
        # таймаутом (не "async for raw in ws:"), чтобы (1) отдельно залогировать
        # реальное завершение handshake, (2) иметь периодический heartbeat с
        # фактическим счётчиком сырых сообщений по типам (depth/trade), и (3)
        # принудительно переподключаться, если за 20с не пришло вообще ничего -
        # для BTC/ETH это ненормально и само по себе диагностически значимо.
        backoff = 1
        consecutive_failures = 0
        url = self._url()
        while True:
            try:
                log.info("Подключение к WS Binance (%s)...", url)
                async with ws_connect(url) as ws:
                    backoff = 1
                    consecutive_failures = 0
                    self._outage_notified = False
                    log.info("binance_feed: WS-соединение установлено, жду сообщения...")
                    last_heartbeat = time.time()
                    depth_count = 0
                    trade_count = 0
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=20.0)
                        except asyncio.TimeoutError:
                            log.warning("binance_feed: НИ ОДНОГО сырого сообщения от Binance за "
                                        "последние 20с на установленном соединении - переподключаюсь")
                            break
                        kind = self._handle_message(raw)
                        if kind == "depth":
                            depth_count += 1
                        elif kind == "trade":
                            trade_count += 1
                        now_ = time.time()
                        if now_ - last_heartbeat >= 30:
                            log.info("binance_feed: heartbeat - depth=%d trade=%d сырых сообщений "
                                      "за последние ~30с", depth_count, trade_count)
                            last_heartbeat = now_
                            depth_count = 0
                            trade_count = 0
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

    def _resolve_symbol(self, stream, data) -> Optional[str]:
        symbol = self.stream_to_symbol.get(stream)
        if symbol is None and stream:
            symbol = self.stream_to_symbol.get(stream.lower())
        if symbol is None and isinstance(data, dict):
            raw_symbol = data.get("s")
            if raw_symbol:
                symbol = self._binance_symbol_to_symbol.get(str(raw_symbol).upper())
        return symbol

    def _handle_message(self, raw) -> Optional[str]:
        """Возвращает "depth"/"trade"/None (тип обработанного сообщения) -
        используется только для heartbeat-счётчиков в _run_with_reconnect."""
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return None
        stream = msg.get("stream")
        data = msg.get("data") or msg  # на всякий случай - вдруг придёт не в конверте combined-stream
        if not isinstance(data, dict):
            return None
        symbol = self._resolve_symbol(stream, data)
        if symbol is None:
            self._unmatched_count += 1
            now_ = time.time()
            if now_ - self._last_unmatched_warn > 60:
                self._last_unmatched_warn = now_
                log.warning("binance_feed: не удалось сопоставить сообщение с символом (stream=%r, "
                            "data.s=%r) - пропущено %d сообщений за последнюю минуту",
                            stream, data.get("s"), self._unmatched_count)
                self._unmatched_count = 0
            return None

        # Разделяем по форме payload'а, а не по имени стрима - aggTrade и
        # partial-depth payload'ы имеют непересекающиеся наборы полей, так что
        # это надёжно независимо от того, что реально пришло в "stream".
        if "p" in data and "q" in data and "m" in data:
            if self.trade_feed is not None:
                self.trade_feed.ingest_trade(symbol, data)
            return "trade"

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
            return None

        try:
            bids = [(float(p), float(q)) for p, q in raw_bids]
            asks = [(float(p), float(q)) for p, q in raw_asks]
        except (TypeError, ValueError) as e:
            log.warning("Binance WS: не смог распарсить bids/asks (%s): %s", e, str(data)[:300])
            return None

        snap = analyze_book(symbol, market.market_index, bids, asks,
                             CFG.binance_wall_min_usd, CFG.wall_max_distance_pct, CFG.wall_backup_range_pct)
        if snap is not None:
            try:
                self.events.put_nowait(snap)
            except asyncio.QueueFull:
                pass
        return "depth"

    async def stop(self):
        if self._task:
            self._task.cancel()
