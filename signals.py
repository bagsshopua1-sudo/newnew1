"""
Сигнальный движок: превращает поток снапшотов стакана (market_data.BookSnapshot)
в торговые сигналы long/short.

Два паттерна:
  - ABSORPTION (поглощение): цена подходит к крупной стенке, стенка держится
    (не тает и не убегает) несколько снапшотов подряд, цена стопорится рядом ->
    сигнал в сторону ОТ стенки (фейд).
  - BREAKOUT (пробой): стенка, которая долго стояла, резко исчезает
    (съедена агрессивным потоком, а не просто отодвинулась) -> сигнал ПО тренду.

Фильтр спуфинга: если стенка исчезла, а цена к ней даже не приближалась
(дистанция почти не менялась) — это, скорее всего, снятая "фейковая" заявка,
такой уход стенки сигналом не считается.
"""
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional

from config import CFG
from market_data import BookSnapshot, Wall
from trend_filter import TrendFilter

log = logging.getLogger("signals")

PRICE_BUCKET_DECIMALS = 2  # группировка уровней стакана в "стенки" для трекинга во времени


@dataclass
class Signal:
    symbol: str
    side: str  # "long" | "short"
    signal_type: str  # "absorption" | "breakout"
    reference_price: float  # цена стенки, от которой сигнал
    mid: float
    confidence: float  # 0..1
    ts: float


@dataclass
class _TrackedWall:
    wall: Wall
    first_seen: float
    last_seen: float
    max_usd: float
    stall_count: int = 0  # сколько снапшотов подряд цена "топчется" рядом со стенкой


class SignalEngine:
    def __init__(self, symbol: str, history_len: int = 30, trend_filter: TrendFilter = None):
        self.symbol = symbol
        self.history: Deque[BookSnapshot] = deque(maxlen=history_len)
        self.tracked: Dict[float, _TrackedWall] = {}  # bucketed price -> _TrackedWall
        self.min_wall_age_sec = 3.0  # стенка должна простоять хотя бы столько, чтобы считаться "реальной"
        self.trend_filter = trend_filter or TrendFilter()
        self.last_trend_state = None

    @staticmethod
    def _bucket(price: float) -> float:
        return round(price, PRICE_BUCKET_DECIMALS)

    def on_snapshot(self, snap: BookSnapshot) -> Optional[Signal]:
        self.history.append(snap)
        now = snap.ts

        trend_state = self.trend_filter.update(snap.mid)
        self.last_trend_state = trend_state
        if trend_state.high_volatility:
            # аномальный всплеск волатильности - стакан ведёт себя нештатно, сигналы не генерируем
            return None

        seen_now = {}
        for w in (*snap.bid_walls, *snap.ask_walls):
            key = self._bucket(w.price)
            seen_now[key] = w

        breakout_signal = None

        # обновляем/детектируем исчезновение стенок
        for key, tw in list(self.tracked.items()):
            if key in seen_now:
                w = seen_now[key]
                tw.wall = w
                tw.last_seen = now
                tw.max_usd = max(tw.max_usd, w.usd)
                # "топчется" рядом: mid почти не двигается последние снапшоты
                if len(self.history) >= 3:
                    mids = [s.mid for s in list(self.history)[-3:]]
                    stalled = (max(mids) - min(mids)) / snap.mid < 0.0006  # ~0.06%
                    if stalled and w.distance_pct < CFG.wall_max_distance_pct:
                        tw.stall_count += 1
                    else:
                        tw.stall_count = 0
            else:
                # стенка пропала: проверяем спуфинг vs реальное поглощение/пробой
                age = tw.last_seen - tw.first_seen
                was_close = tw.wall.distance_pct < CFG.wall_max_distance_pct
                price_crossed = (
                    (tw.wall.side == "ask" and snap.mid >= tw.wall.price) or
                    (tw.wall.side == "bid" and snap.mid <= tw.wall.price)
                )
                if age >= self.min_wall_age_sec and was_close and price_crossed:
                    # реальный пробой: стенка простояла, была близко и цена через неё прошла.
                    # Пробой ПО тренду не фильтруем - фильтр только против контр-трендовых входов.
                    side = "long" if tw.wall.side == "ask" else "short"
                    breakout_signal = Signal(
                        symbol=self.symbol,
                        side=side,
                        signal_type="breakout",
                        reference_price=tw.wall.price,
                        mid=snap.mid,
                        confidence=self._confidence(snap, side, boost=0.15),
                        ts=now,
                    )
                    log.info("[%s] BREAKOUT %s у %.2f (стенка стояла %.1fs, %.0f USD)",
                              self.symbol, side.upper(), tw.wall.price, age, tw.max_usd)
                # иначе — похоже на спуфинг (отодвинули заявку) или стенка была далеко: игнор
                del self.tracked[key]

        for key, w in seen_now.items():
            if key not in self.tracked:
                self.tracked[key] = _TrackedWall(wall=w, first_seen=now, last_seen=now, max_usd=w.usd)

        if breakout_signal:
            return breakout_signal

        # ABSORPTION: ищем стенку, простоявшую достаточно и с накопленным stall_count
        for tw in self.tracked.values():
            age = now - tw.first_seen
            if age >= self.min_wall_age_sec and tw.stall_count >= 3:
                side = "short" if tw.wall.side == "ask" else "long"
                if not TrendFilter.allows_fade(side, trend_state):
                    continue  # не фейдим против сильного тренда
                # Стенка формально крупная, но если сразу за ней (глубже в
                # стакане) почти нет объёма - это одиночная заявка без
                # реальной поддержки, и она может не удержать цену. Фейдим
                # только стенки с достаточной "подложкой" позади.
                min_backup = tw.wall.usd * CFG.wall_backup_min_ratio
                if tw.wall.backup_usd < min_backup:
                    log.info("[%s] ABSORPTION %s у %.2f ПРОПУЩЕН: тонкая подложка "
                              "(%.0f USD за стенкой, нужно >= %.0f)",
                              self.symbol, side.upper(), tw.wall.price, tw.wall.backup_usd, min_backup)
                    tw.stall_count = 0
                    continue
                sig = Signal(
                    symbol=self.symbol,
                    side=side,
                    signal_type="absorption",
                    reference_price=tw.wall.price,
                    mid=snap.mid,
                    confidence=self._confidence(snap, side),
                    ts=now,
                )
                log.info("[%s] ABSORPTION %s у %.2f (стенка стоит %.1fs, %.0f USD, stall=%d)",
                          self.symbol, side.upper(), tw.wall.price, age, tw.max_usd, tw.stall_count)
                tw.stall_count = 0  # чтобы не спамить сигналами каждую секунду
                return sig

        return None

    def _confidence(self, snap: BookSnapshot, side: str, boost: float = 0.0) -> float:
        # дисбаланс объёма как усиливающий/ослабляющий фактор уверенности
        imb = snap.imbalance if side == "long" else (1 - snap.imbalance)
        base = 0.5
        if imb >= CFG.imbalance_threshold:
            base += 0.25
        return min(1.0, base + boost)
