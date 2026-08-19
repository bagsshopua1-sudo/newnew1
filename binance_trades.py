"""
Тейп РЕАЛЬНЫХ исполненных сделок с Binance Futures (aggTrade) - в отличие от
partial-depth снепшотов стакана (binance_feed.py), которые показывают только
displayed size на уровне, это даёт фактический объём, который через этот
уровень прошёл. Нужен, чтобы отличать "стенка просто стоит нетронутая" от
"через стенку реально идёт агрессивный объём, но она восполняется" (iceberg /
настоящее поглощение) - см. обсуждение с пользователем: размер стенки сам по
себе почти ничего не говорит, важнее executed_volume и то, восстанавливается
ли displayed size после fills.

ВАЖНО (19.08, второй раунд диагностики): раньше этот файл сам открывал
ОТДЕЛЬНОЕ WS-соединение к Binance (независимо от binance_feed.py). Сначала
подозревали баг в регистре имени стрима ("aggTrade" vs "aggtrade") - это
было исправлено, но проблема осталась той же: соединение устанавливалось
("WS-соединение установлено"), но НИ ОДНОГО сырого сообщения так и не
приходило, при этом соседний поток стакана (та же цель, тот же IP,
подключённый РАНЬШЕ) продолжал получать данные без единой проблемы. Это
указывает на то, что дело не в имени стрима, а в самом факте ВТОРОГО
одновременного WS-подключения к fstream.binance.com с одного и того же IP -
см. комментарий в binance_feed.py про то, что IP Render уже словил бан
(HTTP 418) на REST-запросах раньше, то есть IP общий/уже помеченный
Binance. Похоже, что для такого IP Binance пускает (handshake проходит), но
реально не шлёт данные по любому НЕ первому одновременному соединению.

Поэтому эта функциональность (хранение тейпа + запросы по нему) ОТДЕЛЕНА от
сетевого кода: класс ниже больше не открывает своё WS-соединение - его
наполняет ЕДИНОЕ соединение из binance_feed.py (там же теперь идёт и подписка
на aggTrade-стримы, в том же самом сокете, что и стакан). Публичный API
(executed_usd_near/executed_usd_trend) не изменился - signals.py/
order_manager.py используют этот объект точно так же, как раньше.
"""
import logging
import time
from collections import deque
from typing import Dict

from exchange_client import MarketInfo

log = logging.getLogger("binance_trades")

TAPE_KEEP_SEC = 30.0  # сколько секунд сделок держим в памяти на символ


class BinanceTradeFeed:
    """Пассивное хранилище тейпа сделок. Наполняется извне (см.
    binance_feed.py::_handle_message, ветка aggTrade) через ingest_trade()."""

    def __init__(self, markets: Dict[str, MarketInfo]):
        self.markets = markets
        # (ts, price, usd, is_buy_aggressor) - deque в порядке возрастания ts
        self._tape: Dict[str, deque] = {sym: deque() for sym in markets}
        self._first_trade_logged: set = set()

    def ingest_trade(self, symbol: str, data: dict) -> bool:
        """Разбирает payload одной aggTrade-сделки Binance и добавляет её в
        тейп символа. Возвращает True, если сделка успешно разобрана и
        добавлена (используется вызывающим кодом только для диагностики/
        подсчёта, на логику не влияет)."""
        try:
            price = float(data["p"])
            qty = float(data["q"])
            is_buyer_maker = bool(data["m"])
        except (KeyError, TypeError, ValueError):
            return False
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
        return True

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
