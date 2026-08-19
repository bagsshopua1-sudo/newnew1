"""
Поток РЕАЛЬНЫХ исполненных сделок с Binance Futures (aggTrade) - в отличие от
partial-depth снепшотов стакана (binance_feed.py), которые показывают только
displayed size на уровне, это даёт фактический объём, который через этот
уровень прошёл. Нужен, чтобы отличать "стенка просто стоит нетронутая" от
"через стенку реально идёт агрессивный объём, но она восполняется" (iceberg /
настоящее поглощение) - см. обсуждение с пользователем: размер стенки сам по
себе почти ничего не говорит, важнее executed_volume и то, восстанавливается
ли displayed size после fills.

Публичный стрим, без ключей/аккаунта - как и binance_feed.py.
"""
import asyncio
import json
import logging
import time
from collections import deque
from typing import Dict, Optional

try:
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # pragma: no cover - совместимость со старым websockets<13
    from websockets.client import connect as ws_connect

from exchange_client import MarketInfo

log = logging.getLogger("binance_trades")

WS_BASE = "wss://fstream.binance.com/stream"
TAPE_KEEP_SEC = 30.0  # сколько секунд сделок держим в памяти на символ


def to_stream_name(symbol: str) -> str:
    # НАЙДЕНО ЭМПИРИЧЕСКИ 19.08 (не по документации, а по факту с прод-логов
    # Render): при имени стрима "aggTrade" (с большой T, как показано в
    # официальных доках Binance) WS-хендшейк проходит успешно ("соединение
    # установлено"), но за установленным соединением НИ РАЗУ не пришло ни
    # одного сырого сообщения - ни одного - несмотря на то что BTC/ETH на
    # Binance Futures торгуются по несколько сделок в секунду. Соседний поток
    # стакана (binance_feed.py, тот же хост, тот же комбинированный стрим-URL,
    # но имя стрима полностью в нижнем регистре - "depth20@100ms") при этом
    # получает данные без единой проблемы. Единственное структурное отличие -
    # регистр имени стрима. Вывод: Binance Futures WS реально матчит имя
    # стрима только при полном нижнем регистре ("aggtrade"), молча принимая
    # подписку на несуществующий (из-за регистра) стрим без единой ошибки.
    return f"{symbol.lower()}usdt@aggtrade"


class BinanceTradeFeed:
    def __init__(self, markets: Dict[str, MarketInfo], on_prolonged_outage=None):
        self.markets = markets
        self.stream_to_symbol = {to_stream_name(sym): sym for sym in markets}
        # Резервный индекс по полю "s" самого payload'а aggTrade (например
        # "BTCUSDT" - Binance всегда отдаёт его в верхнем регистре в самих
        # данных сделки, независимо от того, как регистрозависимо (или нет)
        # эхается обратно поле "stream" в конверте комбинированного стрима).
        # ДОБАВЛЕНО 19.08: диагностика показала, что executed_buy/executed_sell
        # были ВСЕГДА равны 0 на каждом WALL_CANDIDATE, включая логи ДО
        # сегодняшнего рефакторинга - т.е. matching по msg["stream"] молча не
        # срабатывал (return None -> сообщение отбрасывалось без единой
        # ошибки/предупреждения в логах) с самого начала. Теперь matching
        # идёт в 3 попытки: точное совпадение stream -> stream.lower() ->
        # data["s"].upper(), так что бот больше не зависит от того, какой
        # регистр Binance реально использует в конверте.
        self._binance_symbol_to_symbol = {f"{sym.upper()}USDT": sym for sym in markets}
        # (ts, price, usd, is_buy_aggressor) - deque в порядке возрастания ts
        self._tape: Dict[str, deque] = {sym: deque() for sym in markets}
        self._task: Optional[asyncio.Task] = None
        self.on_prolonged_outage = on_prolonged_outage
        self._outage_notified = False
        self._first_trade_logged: set = set()
        self._unmatched_count = 0
        self._last_unmatched_warn = 0.0

    async def start(self):
        self._task = asyncio.create_task(self._run_with_reconnect())
        return self._task

    def _url(self) -> str:
        streams = "/".join(self.stream_to_symbol.keys())
        return f"{WS_BASE}?streams={streams}"

    async def _run_with_reconnect(self):
        # ВАЖНО (диагностика 19.08): раньше здесь был "async for raw in ws:",
        # который не даёт способа отличить "соединение реально висит без
        # единого сообщения" от "просто пока тихо" - в логах была только ОДНА
        # строка "Подключение к WS..." и потом полная тишина часами, без
        # единой ошибки/переподключения, при том что реальный тейп сделок
        # (executed_buy/executed_sell) всегда оставался 0. Теперь читаем
        # сообщения вручную через recv() с таймаутом - это даёт (1) отдельное
        # подтверждение, что handshake реально завершился, (2) периодический
        # heartbeat с фактическим счётчиком сырых сообщений, и (3)
        # принудительный реконнект, если за 20с не пришло вообще ничего -
        # такого тайм-аута быть не должно в норме (BTC/ETH aggTrade идут по
        # несколько раз в секунду), так что его срабатывание само по себе
        # диагностически значимо.
        backoff = 1
        consecutive_failures = 0
        url = self._url()
        while True:
            try:
                log.info("Подключение к WS сделок Binance (%s)...", url)
                async with ws_connect(url) as ws:
                    backoff = 1
                    consecutive_failures = 0
                    self._outage_notified = False
                    log.info("binance_trades: WS-соединение установлено, жду сделки...")
                    last_heartbeat = time.time()
                    msgs_since_heartbeat = 0
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=20.0)
                        except asyncio.TimeoutError:
                            log.warning("binance_trades: НИ ОДНОГО сырого сообщения от Binance за "
                                        "последние 20с на установленном соединении - это ненормально "
                                        "для BTC/ETH aggTrade (обычно несколько сообщений в секунду), "
                                        "похоже на тихое зависание - принудительно переподключаюсь")
                            break
                        msgs_since_heartbeat += 1
                        self._handle_message(raw)
                        now_ = time.time()
                        if now_ - last_heartbeat >= 30:
                            log.info("binance_trades: heartbeat - %d сырых сообщений за последние "
                                      "~30с, размеры тейпов: %s", msgs_since_heartbeat,
                                      {sym: len(t) for sym, t in self._tape.items()})
                            last_heartbeat = now_
                            msgs_since_heartbeat = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                consecutive_failures += 1
                log.warning("Binance trade WS оборвался (%s), переподключение через %ss (попытка %d)",
                            e, backoff, consecutive_failures)
                if consecutive_failures >= 5 and not self._outage_notified and self.on_prolonged_outage:
                    self._outage_notified = True
                    asyncio.create_task(self.on_prolonged_outage("binance_trades_disconnected"))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _handle_message(self, raw):
        try:
            msg = json.loads(raw)
        except (ValueError, TypeError):
            return
        stream = msg.get("stream")
        data = msg.get("data") or msg
        # Попытка 1: точное совпадение "stream" с тем, что мы сформировали в
        # to_stream_name() (например "btcusdt@aggTrade").
        symbol = self.stream_to_symbol.get(stream)
        # Попытка 2: то же самое, но в нижнем регистре - на случай, если
        # Binance эхает "stream" не в том регистре, в котором мы его
        # запросили (наблюдаемый на практике источник бага: matching молча
        # не срабатывал ни разу, объём всегда был 0).
        if symbol is None and stream:
            symbol = self.stream_to_symbol.get(stream.lower())
        # Попытка 3: резервный путь через поле "s" самих данных сделки
        # (например "BTCUSDT") - не зависит от регистра "stream" вообще и
        # срабатывает даже если формат конверта комбинированного стрима
        # окажется иным, чем мы предполагаем.
        if symbol is None:
            raw_symbol = data.get("s") if isinstance(data, dict) else None
            if raw_symbol:
                symbol = self._binance_symbol_to_symbol.get(str(raw_symbol).upper())
        if symbol is None:
            self._unmatched_count += 1
            now_ = time.time()
            if now_ - self._last_unmatched_warn > 60:
                self._last_unmatched_warn = now_
                log.warning("Не удалось сопоставить сделку Binance с символом (stream=%r, "
                            "data.s=%r) - пропущено %d сообщений за последнюю минуту",
                            stream, data.get("s") if isinstance(data, dict) else None,
                            self._unmatched_count)
                self._unmatched_count = 0
            return
        try:
            price = float(data["p"])
            qty = float(data["q"])
            is_buyer_maker = bool(data["m"])
        except (KeyError, TypeError, ValueError):
            return
        # m=True -> покупатель был мейкером -> агрессором была ПРОДАЖА (taker sell).
        # Нас интересует сторона агрессора, а не мейкера.
        is_buy_aggressor = not is_buyer_maker
        now = time.time()
        if symbol not in self._first_trade_logged:
            self._first_trade_logged.add(symbol)
            log.info("binance_trades: первая исполненная сделка получена для %s (цена=%.2f) - "
                      "тейп реально наполняется", symbol, price)
        tape = self._tape.setdefault(symbol, deque())
        tape.append((now, price, price * qty, is_buy_aggressor))
        cutoff = now - TAPE_KEEP_SEC
        while tape and tape[0][0] < cutoff:
            tape.popleft()

    def executed_usd_near(self, symbol: str, price: float, distance_pct: float, lookback_sec: float) -> dict:
        """
        Сколько реально прошло агрессивных сделок (в USD) рядом с price за
        последние lookback_sec секунд, отдельно buy/sell - это и есть
        "исполненный объём у стенки", в отличие от displayed size в стакане.
        """
        tape = self._tape.get(symbol)
        if not tape:
            return {"buy_usd": 0.0, "sell_usd": 0.0, "total_usd": 0.0}
        now = time.time()
        cutoff = now - min(lookback_sec, TAPE_KEEP_SEC)
        max_dist = price * distance_pct / 100.0
        buy_usd = 0.0
        sell_usd = 0.0
        for ts, p, usd, is_buy in reversed(tape):
            if ts < cutoff:
                break
            if abs(p - price) > max_dist:
                continue
            if is_buy:
                buy_usd += usd
            else:
                sell_usd += usd
        return {"buy_usd": buy_usd, "sell_usd": sell_usd, "total_usd": buy_usd + sell_usd}

    def executed_usd_trend(self, symbol: str, price: float, distance_pct: float,
                            lookback_sec: float = 8.0, buckets: int = 4) -> list:
        """
        То же самое, что executed_usd_near(), но БЕЗ схлопывания в одно число -
        разбивает lookback_sec на `buckets` равных временных отрезков (от
        старого к новому) и считает buy/sell USD в каждом отдельно. Один
        суммарный snapshot не отличает "объём идёт с нарастанием" (агрессор
        давит всё сильнее) от "был всплеск в начале окна и всё затихло" -
        ровно то отличие, которое нужно, чтобы поймать давление, которое
        "начинает ослабевать" (см. AUDIT_2026-08-18.md, этап 3 - критерий
        ABSORPTION) или, наоборот, continuation flow после пробоя (этап 4).
        Добавлено 18.08 (аудит стратегии, этап 1.4).
        """
        empty = [{"buy_usd": 0.0, "sell_usd": 0.0} for _ in range(max(buckets, 1))]
        tape = self._tape.get(symbol)
        if not tape or buckets < 1:
            return empty
        now = time.time()
        window = min(lookback_sec, TAPE_KEEP_SEC)
        start = now - window
        bucket_width = window / buckets
        max_dist = price * distance_pct / 100.0
        result = [{"buy_usd": 0.0, "sell_usd": 0.0} for _ in range(buckets)]
        for ts, p, usd, is_buy in tape:
            if ts < start:
                continue
            if abs(p - price) > max_dist:
                continue
            idx = int((ts - start) / bucket_width)
            idx = min(max(idx, 0), buckets - 1)
            if is_buy:
                result[idx]["buy_usd"] += usd
            else:
                result[idx]["sell_usd"] += usd
        return result

    async def stop(self):
        if self._task:
            self._task.cancel()
