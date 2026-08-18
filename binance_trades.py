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
    return f"{symbol.lower()}usdt@aggTrade"


class BinanceTradeFeed:
    def __init__(self, markets: Dict[str, MarketInfo], on_prolonged_outage=None):
        self.markets = markets
        self.stream_to_symbol = {to_stream_name(sym): sym for sym in markets}
        # (ts, price, usd, is_buy_aggressor) - deque в порядке возрастания ts
        self._tape: Dict[str, deque] = {sym: deque() for sym in markets}
        self._task: Optional[asyncio.Task] = None
        self.on_prolonged_outage = on_prolonged_outage
        self._outage_notified = False

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
                log.info("Подключение к WS сделок Binance (%s)...", url)
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
        symbol = self.stream_to_symbol.get(stream)
        if symbol is None:
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

    async def stop(self):
        if self._task:
            self._task.cancel()
